# External Tables

Antfly integrates with PostgreSQL as an external data source in two complementary modes. **Foreign tables** enable read-only, query-time federated access -- joining Antfly search results with relational data without ingesting it. **External table replication** goes further, streaming INSERT/UPDATE/DELETE changes from PostgreSQL into Antfly tables via CDC (logical replication with pglogrepl), providing durable, position-tracked change capture that eliminates the need for external sync daemons.

---

## Foreign Tables: PostgreSQL as a Federated Data Source

### Context

Antfly's query system is shard-based: queries fan out to Antfly storage shards via `RemoteIndex` HTTP proxies, then results are merged. The join system (`src/metadata/join/`) already supports cross-table joins by routing through `runQuery()`. We want to extend this so that a query can target an external PostgreSQL database instead of Antfly shards. This enables joining Antfly search results with structured relational data (customer records, product catalogs, etc.) without ingesting that data into Antfly.

**Scope**: Read-only, filters + joins only (no full-text/semantic search on foreign tables), keystore-based connection strings.

### Approach (v1: Query-Time Only)

**No persistence.** Foreign source configuration is provided inline in the `QueryRequest` via a `foreign_sources` map. Nothing is stored in the table manager or on `store.Table`. The DSN can reference `${secret:pg_dsn}` which resolves from the existing keystore.

Intercept at the `runQuery()` level in `src/metadata/api.go`. When `queryReq.ForeignSources` contains a mapping for the target table name, route to `runForeignQuery()` instead of the shard-based path. Because the join system's `tableApiQuerier.QueryTable()` calls `runQuery()` internally, joins between Antfly tables and foreign Postgres tables work by propagating `foreign_sources` through the `tableApiQuerier`.

All foreign-table-specific logic lives in a dedicated `foreign` package and `api_foreign_table.go` handler.

### Changes

#### 1. Update OpenAPI spec

**File**: `src/metadata/api.yaml`

- Add `ForeignSource` and `ForeignColumn` schemas under `components/schemas`
- Add optional `foreign_sources` field to `QueryRequest` -- map of table name to `ForeignSource`
- Run `make generate` to regenerate `api.gen.go` and SDKs

No changes to `CreateTableRequest`, `Table` response, `store.Table`, or the table manager.

#### 2. New package: `src/metadata/foreign/`

##### Two-layer abstraction (`datasource.go` + `sql_datasource.go`)

Top-level `DataSource` interface is backend-agnostic (returns rows, knows nothing about SQL). Below it, `SQLDataSource` handles SQL databases via `database/sql`, delegating dialect differences to a `Dialect` interface. Non-SQL backends would implement `DataSource` directly.

```
DataSource (top-level interface, returns []map[string]any)
├── SQLDataSource (shared SQL layer using database/sql + Dialect)
│   ├── PostgresDialect
│   ├── [future] MySQLDialect, BigQueryDialect, DatabricksDialect
└── [future] RESTDataSource, MongoDataSource, etc.
```

**`datasource.go`** - Top-level interface + registry:

```go
type DataSource interface {
    Query(ctx context.Context, params *QueryParams) (*QueryResult, error)
    Statistics(ctx context.Context) (rowCount int64, sizeBytes int64, err error)
    Close()
}

type QueryParams struct {
    Fields      []string
    FilterQuery json.RawMessage // Bleve DSL, translated by the DataSource
    OrderBy     map[string]bool
    Limit       int
    Offset      int
    Columns     []ForeignColumn // Known columns for validation
}

type QueryResult struct {
    Rows  []map[string]any
    Total int
}

type ForeignColumn struct {
    Name     string
    Type     string
    Nullable bool
}
```

A `DataSourceFactory` registry maps source type string -> factory function.

**`sql_datasource.go`** - Shared SQL implementation using `database/sql`:

```go
type Dialect interface {
    DriverName() string
    QuoteIdentifier(name string) string
    Placeholder(n int) string
    DiscoverColumns(ctx, db, table) ([]ForeignColumn, error)
    TableStatistics(ctx, db, table) (int64, int64, error)
    MapError(err error) (int, string)
}
```

`SQLDataSource` holds a `*sql.DB` and a `Dialect`. Its `Query()` method:
1. Translates `FilterQuery` (Bleve DSL) to SQL WHERE via shared `filter.go`
2. Builds SELECT using `Dialect.QuoteIdentifier()` and `Dialect.Placeholder()`
3. Executes via `*sql.DB`
4. Scans into `[]map[string]any`

##### Other files

- **`pool.go`** - `PoolManager` struct. Keyed by resolved DSN, caches `DataSource` instances. Resolves `${secret:...}` via `secrets.GetGlobalResolver().Resolve()`. Lazy creation, thread-safe via `sync.RWMutex`.

- **`filter.go`** - Bleve DSL to SQL WHERE translation (used by `SQLDataSource`). Takes a `PlaceholderFunc` and list of known columns. Handles:
  - Term queries -> `field = $N`
  - Range queries -> `field >= $N AND field <= $M`
  - Boolean (conjuncts/disjuncts/must_not) -> AND/OR/NOT
  - Query string `{"query": "field:value"}` -> `field = $N`
  - Disjunction of terms (from join LookupKeys) -> `field IN ($N, $M, ...)`
  - Match all -> empty WHERE
  - Unsupported types -> descriptive error
  - Field names validated against known columns to prevent SQL injection

- **`postgres.go`** - Postgres `Dialect` implementation. Registers factory via `init()`. Uses `pgx/v5/stdlib` as `database/sql` driver. `QuoteIdentifier` (double-quote), `Placeholder` (`$N`), `DiscoverColumns` (via `information_schema.columns`), `TableStatistics` (via `pg_stat_user_tables`), `MapError` (via `pgconn.PgError` codes).

#### 3. Foreign query handler

**New file**: `src/metadata/api_foreign_table.go`

- `runForeignQuery(ctx, queryReq, source *ForeignSource) QueryResult` -- rejects unsupported operations (semantic_search, full_text_search, graph_searches, aggregations, reranker) with 400 errors, gets DataSource from pool, translates filter_query, executes SQL, converts rows to `[]QueryHit`

#### 4. Minimal changes to existing `api.go`

**File**: `src/metadata/api.go`

Two small changes:

1. Add `foreignPool *foreign.PoolManager` field to `TableApi` struct (~line 79), initialize in `NewTableApi()` (~line 175)
2. In `runQuery()` (~line 3232), after validation but before shard lookup, check if `queryReq.ForeignSources` contains the target table name. If so, delegate to `runForeignQuery()`.

No changes to `CreateTable`, `BatchWrite`, or any write endpoints.

#### 5. Join integration: propagate foreign_sources

**File**: `src/metadata/api_join.go`

- Add `foreignSources` field to `tableApiQuerier` struct
- In `QueryTable()` and `LookupKeys()`, set `queryReq.ForeignSources` from the stored field before calling `runQuery()`
- In `executeJoin()`, pass `queryReq.ForeignSources` when constructing `tableApiQuerier`
- In `getTableStatistics()` and `tableApiQuerier.GetTableStatistics()`, check `foreignSources` for the table and query `pg_stat_user_tables` instead of aggregating shard stats

#### 6. Add `pgx/v5` dependency

```bash
go get github.com/jackc/pgx/v5
```

### Key Design Decisions

- **Query-time config, no persistence** -- foreign source config lives in `QueryRequest.ForeignSources`, not on `store.Table`. No table creation, no write guards. DSN secrets resolve from the existing keystore via `${secret:...}`.
- **Interception at `runQuery()`** rather than implementing a fake `Index` -- cleaner because external databases don't fit the shard/index model
- **Dialect interface** -- abstracts database-specific concerns (placeholders, quoting, error mapping, schema discovery) so adding MySQL/BigQuery/Databricks is just implementing the interface
- **Filter translation is strict** -- only supported Bleve query types are translated; unsupported types return errors rather than silently degrading
- **Column validation** -- field names in filter queries are validated against known columns to prevent SQL injection through field names
- **All values parameterized** -- placeholder style determined by dialect, never string interpolation
- **Connection pools are lazy and cached** -- one pool per resolved DSN, created on first query

### Files Modified/Created

| File | Action |
|------|--------|
| `go.mod` | Add `pgx/v5` |
| `src/metadata/api.yaml` | Add ForeignSource/ForeignColumn schemas, `foreign_sources` on QueryRequest |
| `src/metadata/api.gen.go` | Regenerated |
| `src/metadata/api.go` | Add foreignPool field, init in NewTableApi, routing check in runQuery |
| `src/metadata/api_foreign_table.go` | **New** - runForeignQuery |
| `src/metadata/api_join.go` | Propagate foreign_sources through tableApiQuerier |
| `src/metadata/foreign/datasource.go` | **New** - DataSource interface, QueryParams, QueryResult, ForeignColumn, factory registry |
| `src/metadata/foreign/sql_datasource.go` | **New** - SQLDataSource (database/sql), Dialect interface, SQL query building |
| `src/metadata/foreign/pool.go` | **New** - PoolManager (caches DataSource instances, secret resolution) |
| `src/metadata/foreign/filter.go` | **New** - Bleve DSL to SQL WHERE translation |
| `src/metadata/foreign/postgres.go` | **New** - Postgres Dialect implementation (pgx/v5/stdlib) |
| `src/metadata/foreign/filter_test.go` | **New** - unit tests for filter translation |

### Edge Cases

- **Query string format from joins**: `tableApiQuerier.LookupKeys()` (api_join.go:332) builds `{"query": "field:val1 OR field:val2 OR ..."}` -- the filter translator must parse this Bleve query string format and translate the disjunction to `field IN ($1, $2, ...)`
- **Column discovery**: When `ForeignSource.Columns` is empty, auto-discover via `information_schema.columns` before executing the query
- **Pool eviction**: Pools are keyed by resolved DSN. Since there's no table lifecycle (no drop), pool entries are evicted by LRU/TTL only.

### Verification

1. **Unit tests**: `GOEXPERIMENT=simd go test ./src/metadata/foreign/...` -- filter translation, pool management
2. **Build**: `make build` -- verify everything compiles
3. **E2E test** (manual or scripted with a local Postgres):
   - Create a Postgres table with test data
   - `POST /api/v1/tables/anything/_query` with `foreign_sources` and `filter_query` -- verify results
   - `POST /api/v1/tables/products/_query` with `join.right_table: "pg_customers"` and `foreign_sources: {"pg_customers": {...}}` -- verify join works
   - `POST /api/v1/tables/anything/_query` with `foreign_sources` and `semantic_search` -- verify 400 rejection

---

## External Table Replication: CDC via pglogrepl

### Context

With foreign table support for query-time PostgreSQL access in place, the next step is built-in CDC: streaming INSERT/UPDATE/DELETE from PostgreSQL into Antfly tables via logical replication. This eliminates the need for external sync daemons (like the `examples/postgres-sync` LISTEN/NOTIFY pattern) and provides durable, position-tracked change capture that doesn't miss events.

Key design goals:
- **Multi-table support**: Multiple PG tables can feed into a single Antfly table (e.g., `users` + `scores` -> Antfly `users`)
- **Transform-based writes**: Uses configurable `on_update`/`on_delete` transform templates with `{{column}}` references to PG row data, enabling `$set`/`$unset`/`$merge` and more
- **Flexible event handling**: `on_update` controls INSERT/UPDATE behavior, `on_delete` controls DELETE behavior -- both default to sensible auto-derived ops when omitted
- **Index control**: Handled by the existing table schema system (`x-antfly-types`)
- **Optional slot/publication names**: Auto-derived by default, but users can specify them for managed platforms (Supabase, Neon)

### New Dependency

Add `github.com/jackc/pglogrepl` to `go.mod` (pgx/v5 already present at v5.8.0).

### Files Created

#### 1. `src/metadata/foreign/replication.go` -- ReplicationWorker

Core CDC worker that manages a single PostgreSQL logical replication stream for one replication source feeding an Antfly table.

**Key types:**
```go
type ReplicationConfig struct {
    TableName       string // target Antfly table
    SlotName        string // PG replication slot (auto-derived or user-specified)
    PublicationName string // PG publication (auto-derived or user-specified)
    DSN             string // resolved connection string
    PostgresTable   string // source PG table
    KeyTemplate     string // "id" or "{{tenant_id}}:{{user_id}}"
    OnUpdate        []ReplicationTransformOp // nil = auto $set all columns
    OnDelete        []ReplicationTransformOp // nil = auto $unset the $set paths from OnUpdate
}

// ReplicationTransformOp defines a single transform operation with {{column}} references.
// Values containing {{column_name}} are resolved against the decoded PG row.
// Values containing {{column.key}} navigate into decoded JSONB maps.
// Literal values (no {{}}) are used as-is.
type ReplicationTransformOp struct {
    Op    string `json:"op"`    // "$set", "$unset", "$merge", "$delete_document", etc.
    Path  string `json:"path"`  // Antfly document field path (for $set, $unset)
    Value any    `json:"value"` // literal value, or "{{column}}" template reference
}

type LSNStore interface {
    LoadLSN(ctx context.Context, slotName string) (pglogrepl.LSN, error)
    SaveLSN(ctx context.Context, slotName string, lsn pglogrepl.LSN) error
}

type ReplicationWorker struct { ... }
```

**Supported transform ops:**
- `$set` -- set a field to a value: `{"op": "$set", "path": "email", "value": "{{user_email}}"}`
- `$unset` -- remove a field: `{"op": "$unset", "path": "email"}`
- `$merge` -- merge a JSONB column's keys as top-level fields: `{"op": "$merge", "value": "{{metadata}}"}`
- `$delete_document` -- delete the entire Antfly document (for on_delete only): `{"op": "$delete_document"}`
- All existing Antfly transform ops (`$inc`, `$push`, `$pull`, etc.) are also supported with literal values

**Lifecycle:**
- `Run(ctx)` -- outer loop with exponential backoff reconnection (1s->30s, jittered). Retries on transient errors (network, PG restart). Stops on permanent errors (auth failure, missing table) or context cancellation (leadership lost).
- `runOnce(ctx)` -- single session: connect with `replication=database`, `ensurePublication()` (idempotent, handles pre-existing), `ensureReplicationSlot()` (idempotent), load persisted LSN, `StartReplication()`, enter `receiveLoop()`.
- `receiveLoop(ctx, conn, lsn)` -- reads WAL messages with 3s deadline, sends standby status every 10s. Dispatches pgoutput v2 messages:
  - `RelationMessageV2` -> cache in `w.relations` map (OID -> column metadata)
  - `InsertMessageV2` / `UpdateMessageV2` -> decode tuple -> resolve `on_update` transforms against row data -> forward as Antfly transforms with `upsert=true`. If `on_update` is nil, auto-generate `$set` for every column.
  - `DeleteMessageV2` -> decode old tuple -> extract key -> resolve `on_delete` transforms against row data. If `on_delete` is nil, auto-derive `$unset` ops from `on_update`'s `$set` paths. Special: `$delete_document` op calls `deleteFunc` to remove entire doc.
  - `CommitMessage` -> `lsnStore.SaveLSN(slot, commitLSN)` -- checkpoint for restart recovery
  - `TruncateMessageV2` -> log warning, skip

**Tuple decoding:** `tupleToMap()` uses `pgtype.Map.TypeForOID()` for type-aware decoding (int->int64, float->float64, jsonb->map, timestamp->time.Time, etc.). Falls back to string for unknown OIDs. Handles null (`'n'`) and unchanged TOAST (`'u'`).

**Transform resolution:** After `tupleToMap()` produces the raw decoded row, `resolveTransforms(ops, row)` evaluates each op's `Value` field:
1. **Column references**: `"{{column}}"` -> replaced with the decoded column value (int, float, string, map, etc.)
2. **Nested JSONB references**: `"{{column.key}}"` -> navigates into a decoded JSONB map to extract a sub-key
3. **Literal values**: Values without `{{}}` are used as-is (e.g., `true`, `42`, `"active"`)
4. **$merge resolution**: The `$merge` op's value must resolve to a `map[string]any` (typically from a JSONB column). Its keys become individual `$set` ops merged into the output.
5. **$delete_document**: No value resolution needed -- signals full document deletion.

Transform resolution runs before key extraction, so `key_template` references work against the raw row columns.

**Write path -- on_update / on_delete:**
- INSERT/UPDATE: Resolve `on_update` transforms against the decoded row. If `on_update` is nil, auto-generate `$set` for every column in the relation (passthrough mode). Send resolved ops as Antfly transforms with `upsert=true`.
- DELETE: Resolve `on_delete` transforms against the decoded old tuple. If `on_delete` is nil, auto-derive `$unset` ops from the `$set` paths in `on_update` (removes only this source's fields, safe for multi-source). If `on_delete` contains `$delete_document`, call `deleteFunc` instead of transforms.

**Key extraction:** `extractKey()` evaluates `KeyTemplate` against the raw decoded row (before transform resolution). If the template contains no `{{}}`, it's treated as a plain column name and the value is formatted via `fmt.Sprintf("%v", val)`. If it contains `{{col}}` references, each is substituted with the column value. Uses `strings.NewReplacer` for simple `{{col}}` -> value substitution (no full template engine needed).

**Index control:** Handled entirely by the table's existing schema system. Users create a table with both `replication_sources` and `schema` with `document_schemas` using `x-antfly-types` annotations. The replication pipeline shapes the data; the schema controls what gets indexed.

**Helpers:** `SlotName(tableName, pgTable)` -> `"antfly_" + sanitized` (max 63 chars). `PublicationName(tableName, pgTable)` -> `"antfly_pub_" + sanitized`. Both accept user overrides via config.

#### 2. `src/metadata/foreign/replication_manager.go` -- ReplicationManager + LSN Store

Orchestrates CDC workers for all tables with replication sources. Runs on metadata leader only.

**Interfaces (to avoid circular imports with `metadata` package):**
```go
type MetadataTransformer interface {
    ForwardTransform(ctx context.Context, tableName, key string, ops []*db.TransformOp, upsert bool) error
}
type MetadataKV interface {
    Get(ctx context.Context, key []byte) ([]byte, io.Closer, error)
    Batch(ctx context.Context, writes [][2][]byte, deletes [][]byte) error
}
type TableLister interface {
    TablesMap() (map[string]*store.Table, error)
}
```

**ReplicationManager.Run(ctx):**
1. Calls `tables.TablesMap()` to find tables with `ReplicationSources` (non-empty)
2. For each source in each table: resolves DSN secrets, creates a `ReplicationWorker`, starts in errgroup
3. Blocks on `errgroup.Wait()` -- returns when ctx cancelled (leadership lost)
4. Individual worker errors are logged but don't propagate (workers retry independently)

**raftLSNStore:** Persists LSN checkpoints in metadata Raft KV at key `cdc:lsn:<slotName>`. Value is LSN string (e.g. `"0/16B3748"`). Written on every `CommitMessage`. Read on worker startup. Replicated via Raft -- survives leader failover.

#### 3. `src/metadata/replication_adapter.go` -- MetadataTransformer bridge

Implements `foreign.MetadataTransformer` on `*MetadataStore`:
- `ForwardTransform(ctx, tableName, key, ops, upsert)` -> finds shard via `table.FindShardForKey(key)`, builds a `*db.Transform` with the ops and upsert flag, calls existing `ms.forwardBatchToShard(ctx, shardID, nil, nil, []*db.Transform{transform}, db.Op_PROPOSE)` at `shard_routing.go:336`

The existing `forwardBatchToShard` already accepts transforms alongside writes/deletes and has built-in retry with exponential backoff (10 retries, 100ms->1s).

#### 4. `src/metadata/foreign/replication_test.go` -- Unit tests

Tests for: `SlotName`/`PublicationName` derivation, `tupleToMap` with various PG types (null, TOAST, basic types, unknown OID), `extractKey` (plain column, template with `{{}}`, missing column, nil value), `raftLSNStore` round-trip with mock `MetadataKV`, `isPermanentReplicationError` classification, `resolveTransforms` (`{{column}}` substitution, `{{col.key}}` JSONB navigation, literal values, `$merge` expansion, `$delete_document` passthrough), `autoGenerateOnUpdate` (all columns -> `$set` ops), `autoGenerateOnDelete` (derive `$unset` from `on_update` `$set` paths).

### Files Modified

#### 5. `src/metadata/api.yaml` -- OpenAPI schema

Add to `ForeignSource` (after `columns`):
```yaml
        replicate:
          type: boolean
          default: false
          description: |
            Enable CDC from this PostgreSQL table via logical replication.
            Requires wal_level=logical on the PostgreSQL source.
        key_template:
          type: string
          default: "id"
          description: |
            Template for constructing the Antfly document key from PG columns.
            A plain string (e.g., "id") uses that column's value directly.
            Use {{column}} syntax for composite keys: "{{tenant_id}}:{{user_id}}".
            Column references are resolved after field_mappings renaming.
        slot_name:
          type: string
          description: |
            PostgreSQL replication slot name. If omitted, auto-derived from
            the Antfly table and PG table names. Specify this when using
            pre-created slots (e.g., on Supabase or Neon).
        publication_name:
          type: string
          description: |
            PostgreSQL publication name. If omitted, auto-derived and created
            automatically. Specify this when using pre-created publications.
        on_update:
          type: array
          items:
            $ref: "#/components/schemas/ReplicationTransformOp"
          description: |
            Transform operations applied on INSERT/UPDATE events. Values can
            reference PG columns via {{column}} syntax. If omitted, auto-generates
            $set for every column (passthrough mode).
          example:
            - { op: "$set", path: "email", value: "{{user_email}}" }
            - { op: "$set", path: "score", value: "{{score}}" }
            - { op: "$merge", value: "{{metadata}}" }
            - { op: "$set", path: "active", value: true }
        on_delete:
          type: array
          items:
            $ref: "#/components/schemas/ReplicationTransformOp"
          description: |
            Transform operations applied on DELETE events. If omitted, auto-derives
            $unset ops from on_update's $set paths (safe for multi-source).
            Use $delete_document op to delete the entire Antfly document.
          example:
            - { op: "$set", path: "active", value: false }
```

Add new schema `ReplicationTransformOp`:
```yaml
    ReplicationTransformOp:
      type: object
      required:
        - op
      properties:
        op:
          type: string
          description: |
            Transform operation. Standard ops: $set, $unset, $inc, $push, $pull,
            $addToSet, $pop, $mul, $min, $max, $currentDate, $rename.
            Replication-specific: $merge (flatten JSONB into top-level fields),
            $delete_document (delete entire Antfly doc, on_delete only).
          example: "$set"
        path:
          type: string
          description: Antfly document field path. Required for $set, $unset, etc.
          example: "email"
        value:
          description: |
            Value for the operation. Can be a literal (string, number, boolean)
            or a {{column}} reference to a PG column value. Use {{col.key}} to
            navigate into decoded JSONB columns.
          example: "{{user_email}}"
```

Change `CreateTableRequest` to use `replication_sources` (plural, array):
```yaml
        replication_sources:
          type: array
          items:
            $ref: "#/components/schemas/ForeignSource"
          description: |
            PostgreSQL CDC replication sources. Multiple sources can feed into
            this table (e.g., users table + scores table -> single Antfly table).
            Each source must have replicate: true. Configure on_update/on_delete
            transforms to control how PG events map to Antfly document operations.
```

Add same field to `Table` response schema. Then run `make generate` to regenerate `api.gen.go` and SDKs.

#### 6. `src/store/table.go:21` -- Add ReplicationSourceConfig

Add to `Table` struct:
```go
ReplicationSources []ReplicationSourceConfig `json:"replication_sources,omitempty"`
```

New types:
```go
type ReplicationSourceConfig struct {
    Type            string                   `json:"type"`
    DSN             string                   `json:"dsn"`
    PostgresTable   string                   `json:"postgres_table"`
    KeyTemplate     string                   `json:"key_template"`
    SlotName        string                   `json:"slot_name,omitempty"`
    PublicationName string                   `json:"publication_name,omitempty"`
    OnUpdate        []ReplicationTransformOp `json:"on_update,omitempty"`  // nil = auto $set all columns
    OnDelete        []ReplicationTransformOp `json:"on_delete,omitempty"`  // nil = auto $unset from on_update paths
}

type ReplicationTransformOp struct {
    Op    string `json:"op"`              // "$set", "$unset", "$merge", "$delete_document", etc.
    Path  string `json:"path,omitempty"`  // field path (not needed for $merge, $delete_document)
    Value any    `json:"value,omitempty"` // literal or "{{column}}" reference
}
```

#### 7. `src/tablemgr/table.go` -- Wire through TableConfig

Add `ReplicationSources []store.ReplicationSourceConfig` to `TableConfig` struct. Wire it through `CreateTable` into the `store.Table`.

#### 8. `src/metadata/api.go` -- CreateTable handler (~line 356)

Map `create.ReplicationSources` from the generated API type to `[]store.ReplicationSourceConfig`, filtering to those with `Replicate == true`.

#### 9. `src/metadata/metadata.go:31` -- Add replManager field

Add `replManager *foreign.ReplicationManager` to `MetadataStore` struct.

#### 10. `src/metadata/runner.go:176-305` -- Start CDC in leader factory

Create `ReplicationManager` with `ln` (as MetadataTransformer), `metadataStore` (as MetadataKV), `tm` (as TableLister). Start `replManager.Run(ctx)` in a goroutine inside the `reconciler` function, alongside `ln.reconcileShards(ctx)`.

### Key Design Properties

- **At-least-once delivery**: LSN checkpointed on CommitMessage. Transforms are idempotent (`$set` same value = no-op, `$unset` already-absent field = no-op).
- **Multi-table safe**: `on_update` `$set` ops merge fields. Auto-derived `on_delete` only `$unset`s this source's `$set` paths. Multiple sources can write to the same document without conflict.
- **Flexible event handling**: `on_update`/`on_delete` with `{{column}}` references give full control over how PG events map to Antfly transforms. Omit both for simple passthrough.
- **Leader-only**: Worker lifecycle tied to leader context. On failover, new leader reads LSN from Raft KV and resumes.
- **No new external dependencies**: Uses existing pgx driver + pglogrepl (same author). No message broker needed.
- **Platform-friendly**: Auto-creates publication/slot, but accepts pre-existing ones via `slot_name`/`publication_name` for managed PostgreSQL (Supabase, Neon).
- **Standby keepalive**: Every 10s, well under PG's default 60s `wal_sender_timeout`.
- **Reconnection**: Exponential backoff 1s->30s with jitter. Permanent errors (auth, missing DB) stop the worker.

### Verification

1. **Build**: `GOEXPERIMENT=simd go build ./...`
2. **Unit tests**: `GOEXPERIMENT=simd go test ./src/metadata/foreign/...`
3. **Manual single-source test** (requires PostgreSQL with `wal_level=logical`):
   - Create PG table: `CREATE TABLE products (id TEXT PRIMARY KEY, name TEXT, price FLOAT, metadata JSONB, internal_notes TEXT)`
   - Start Antfly: `go run ./cmd/antfly swarm`
   - Create table with replication + on_update transforms:
     ```bash
     curl -X POST localhost:8080/api/v1/tables/products -d '{
       "replication_sources": [{
         "type": "postgres",
         "dsn": "postgres://user:pass@localhost:5432/mydb",
         "postgres_table": "products",
         "replicate": true,
         "key_template": "id",
         "on_update": [
           {"op": "$set", "path": "id", "value": "{{id}}"},
           {"op": "$set", "path": "title", "value": "{{name}}"},
           {"op": "$set", "path": "price", "value": "{{price}}"},
           {"op": "$merge", "value": "{{metadata}}"}
         ]
       }],
       "schema": {
         "document_schemas": {
           "default": {
             "schema": {
               "properties": {
                 "title": {"type": "string", "x-antfly-types": ["text"]},
                 "price": {"type": "number", "x-antfly-types": ["numeric"]}
               }
             }
           }
         }
       }
     }'
     ```
   - INSERT a PG row: `INSERT INTO products VALUES ('p1', 'Widget', 9.99, '{"color":"red"}', 'secret')`
   - Verify Antfly doc has fields: `id`, `title` (renamed from `name`), `price`, `color` (merged from JSONB `metadata`), but NOT `internal_notes` or `metadata`
4. **Manual multi-source test**:
   - Create two PG tables: `users (id TEXT PK, name TEXT)` and `scores (user_id TEXT PK, score INT)`
   - Create Antfly table with two replication sources:
     - Source 1: `key_template: "id"`, `on_update: [{"op":"$set","path":"name","value":"{{name}}"}]`
     - Source 2: `key_template: "user_id"`, `on_update: [{"op":"$set","path":"score","value":"{{score}}"}]`
   - INSERT into both PG tables with same key
   - Verify Antfly doc has fields from both sources merged (`name` + `score`)
   - DELETE from `scores` -> on_delete auto-derived -> `$unset score` -> verify score removed but name remains
5. **Manual soft-delete test**:
   - Create source with `on_delete: [{"op":"$set","path":"active","value":false}]`
   - DELETE a PG row -> verify Antfly doc gets `active: false` instead of field removal
6. **E2E** (future, gated by `RUN_CDC_TESTS=true`): Full lifecycle test with testcontainers PostgreSQL
