# Lake Query Mode

Antfly relational mode makes typed rows first-class while keeping JSON as a
document-backed column type. Lake query mode extends that contract to files
owned by users in object storage: Parquet datasets, Iceberg tables, and later
Lance-style datasets can be queried through the same relational row-plan API
without first copying every row into Antfly.

The goal is not to become a generic batch SQL engine. The goal is to make
Antfly an efficient serving, indexing, caching, and query-planning layer over
immutable lake files:

- Read committed table snapshots from object storage.
- Push predicates and projections into file, row-group, page, and column reads.
- Reuse Antfly relational rows plans, joins, aggregates, windows, and SQL
  lowering.
- Build Antfly-native full-text, vector, sparse, graph, and algebraic sidecar
  indexes over lake rows.
- Materialize only hot rows, hot projections, and derived summaries when the
  workload proves they are worth owning inside Antfly.

## Relationship To Arrow, Parquet, Iceberg, And Lance

These are related, but they are not one layer:

- **Arrow** is the in-memory columnar representation. It is the natural shape
  for execution batches and vectorized operators.
- **Parquet** is the object-storage file representation. It stores columns in
  row groups and column chunks, with footer metadata and optional statistics
  that make pruning possible.
- **Iceberg** is the table metadata and snapshot layer. It describes which data
  files belong to a table snapshot, their partitions, delete files, schemas,
  sequence numbers, and file-level statistics.
- **Lance** is closer to a table/file format optimized for random access,
  vectors, and fast point/nearest-neighbor reads. It belongs behind the same
  external row-source interface, but it is not required for the first Parquet
  path.

Antfly should use Arrow-like batches internally where it helps execution, but
the public contract should remain the existing typed relational plan surface.

## Fit With Relational Mode

Lake query mode should be a row source for relational plans, not a separate
query system.

`RELATIONAL.md` defines relational mode as a storage mode where schema is
required, typed cells are first-class, and the relational base store is the
source of truth. Lake query mode has the same typed row semantics, but the base
store is external and snapshot-addressed:

- For ordinary relational tables, committed rows live in Antfly's relational
  base-store keyspace.
- For lake tables, committed rows live in external immutable files and Antfly
  stores catalog bindings, snapshot metadata, pruning metadata, caches, and
  derived sidecar indexes.

The existing `rows:query`, `rows:aggregate`, `rows:window`, `rows:join`, and
`rows:lateral` APIs should work over both sources. SQL lowering should produce
the same typed request shapes. The executor chooses a local relational scan or
an external lake scan based on the table binding.

## Public Contract

There are two reasonable catalog shapes. The second is preferable because it
keeps relational table semantics intact.

Option A: add a storage mode:

```json
{
  "storage_mode": "external_parquet",
  "external_source": {
    "format": "parquet",
    "uri": "s3://bucket/events/",
    "snapshot": { "mode": "etag_manifest" }
  }
}
```

Option B: keep `storage_mode: "relational"` and add a base-source binding:

```json
{
  "storage_mode": "relational",
  "base_source": {
    "kind": "external",
    "format": "iceberg",
    "uri": "s3://bucket/warehouse/events",
    "snapshot": { "mode": "iceberg_current" }
  },
  "default_type": "event",
  "enforce_types": true,
  "document_schemas": {
    "event": {
      "schema": {
        "type": "object",
        "properties": {
          "tenant_id": { "type": "keyword" },
          "event_id": { "type": "keyword" },
          "created_at": { "type": "datetime" },
          "amount": { "type": "numeric" },
          "attrs": { "type": "json" }
        },
        "required": ["tenant_id", "event_id"],
        "additionalProperties": false
      }
    }
  },
  "primary_key": { "columns": ["tenant_id", "event_id"] }
}
```

`base_source.kind = "external"` means:

- Row writes through Antfly are rejected unless an explicit materialized-write
  mode is configured.
- Row identity is derived from the declared primary key or, if no primary key is
  declared, from an internal stable external row reference.
- Schema validation binds external columns to Antfly relational column types.
- JSON columns are allowed and use the same document-subtree indexing semantics
  as relational JSON columns.

## External Row Identity

Every external row needs a durable row reference, even when the user does not
declare a primary key:

```text
(table_id, snapshot_id, file_id, row_group_ordinal, row_ordinal)
```

For raw Parquet prefixes, `snapshot_id` can be a stable digest of object keys,
sizes, ETags/version IDs, and selected schema metadata. For Iceberg, it should
be the Iceberg snapshot id. For Lance, it should be the dataset version.

Antfly should store sidecar index postings against this external row reference.
When a query returns rows, the executor can either emit public primary-key
identity or, for catalog/debug paths, expose the external row reference as
`physical_key`.

Deletes and updates from external table formats are snapshot concerns:

- Raw Parquet prefix mode has append/replace semantics based on object changes.
- Iceberg mode honors position deletes and equality deletes before producing
  visible rows.
- Antfly sidecar indexes are versioned by snapshot id and garbage-collected
  after snapshot retention allows it.

## Catalog And Metadata Stored In Antfly

Antfly should not copy all lake data by default. It should store compact
metadata that lets it avoid reading most data.

Required catalog state:

- Source binding: URI, format, credential reference, snapshot mode, table id.
- Schema binding: external physical schema to Antfly relational type mapping.
- Snapshot record: snapshot id, parent snapshot id, observed object versions,
  created time, schema id, partition spec id.
- File inventory: file id, object key, size, ETag/version id, content type,
  partition values, row count.
- Row-group metadata: row count, byte ranges, column chunk offsets, column
  encodings, compressed/uncompressed sizes.
- Statistics: min/max/null counts, distinct counts when available, dictionary
  values when bounded, bloom/page-index metadata when available.
- Delete metadata for Iceberg: position delete files, equality delete
  predicates, sequence numbers, and applicability to data files.
- Sidecar index lifecycle: built snapshot id, source file ids, generation,
  freshness, rebuild/reconcile status.

This belongs in Antfly's metadata/catalog path, not in the user bucket. Optional
sidecar files can be written to Antfly-owned object storage when metadata is too
large for the main catalog.

## Planner

The planner should split each typed row plan into three classes of work:

1. Source pushdown:
   - projection columns
   - partition predicates
   - file-level and row-group min/max pruning
   - dictionary pruning
   - page-index pruning when available
   - limit/order shortcuts when layout permits

2. Antfly sidecar access:
   - full-text candidate row refs
   - dense/sparse vector candidate row refs
   - graph traversals over external row refs
   - algebraic materialized aggregates
   - adaptive expression/materialization cache hits

3. Residual execution:
   - expression predicates
   - JSON path predicates that cannot be pushed into Parquet metadata
   - post-join predicates
   - final projection, ordering, aggregation, windows, joins, and lateral
     correlation

For simple selective queries, sidecar indexes should produce candidate row refs
first, then the scanner fetches only the projected Parquet columns for those
row refs. For broad analytical scans, file and row-group pruning should feed
large column batches directly into aggregate/window operators.

## Execution Interface

The first implementation can adapt external rows into JSON rows and reuse the
existing row-plan executors. That is the safest MVP because the validation,
projection, expression, aggregate, join, and CTE contracts already exist.

The efficient implementation needs a typed column-batch interface:

```zig
pub const ExternalRowBatch = struct {
    snapshot_id: []const u8,
    row_refs: []const ExternalRowRef,
    columns: []const ColumnVector,
    row_count: u32,
};

pub const ExternalRowScanner = struct {
    pub fn next(self: *@This(), alloc: Allocator) !?ExternalRowBatch;
};
```

The row-plan executor should be refactored around a `RowSource` abstraction:

- `RelationalStoreRowSource` scans Antfly relational base rows.
- `JsonMaterializedRowSource` scans CTE/materialized JSON rows.
- `ExternalParquetRowSource` scans projected Parquet batches.
- `ExternalIcebergRowSource` expands Iceberg snapshot metadata and delegates
  visible data-file reads to Parquet scanners.
- `ExternalLanceRowSource` can later use Lance-native random access and vector
  indexes.

JSON should become a boundary format, not the internal hot path. Expressions
that already operate on typed relational values should evaluate directly over
column vectors where possible, falling back to row materialization only for
operators that need it.

## Object Storage Reads

Antfly already has an object storage abstraction for S3, GCS, filesystem, and
memory clients. Lake query mode should use that abstraction rather than binding
the scanner directly to one cloud provider.

Efficient Parquet reads need:

- footer range reads from the tail of each object
- coalesced column-chunk range reads
- bounded concurrency per bucket/prefix
- retry and checksum policy
- local cache keys that include bucket, object key, version/ETag, byte range,
  compression codec, and decoded column id
- admission control so broad scans cannot evict serving-critical vector/text
  index pages

The scanner should prefer range reads over whole-object reads. Whole-object
reads are only reasonable for small files or local filesystem tests.

## Derived Indexes Over Lake Rows

The most valuable Antfly-specific feature is sidecar indexing.

Full-text indexes:

- Extract configured text columns from Parquet batches.
- Index terms against external row refs.
- Store generation metadata keyed by snapshot id and source file ids.
- Rebuild incrementally by comparing snapshot file inventories.

Dense and sparse vector indexes:

- Support embedding columns already present in Parquet.
- Support generated embeddings from text/json columns through existing
  enrichment machinery.
- Store vector ids deterministically from external row refs, similar to stable
  vector ids for documents.
- Let ANN search produce external row refs, then fetch projected columns from
  Parquet.

Graph indexes:

- Treat edge-list Parquet datasets as external graph sources.
- Store graph edge postings against external row refs or materialized endpoint
  ids.
- Use row-source scans to hydrate attributes only after graph traversal.

Algebraic indexes:

- Use Parquet/Iceberg stats for coarse estimates.
- Build materialized folds over external rows for common group-by/filter shapes.
- Keep materializations snapshot-versioned.
- Reuse adaptive recommendation logic to decide whether a materialization saves
  enough scan work to justify sidecar storage.

## Caching And Materialization

There should be three levels of caching:

1. Metadata cache:
   - Iceberg metadata JSON/Avro manifests
   - Parquet footers
   - row-group stats
   - delete-file applicability

2. Data cache:
   - compressed byte ranges
   - decoded column pages
   - projected row batches for hot query shapes

3. Antfly materialization:
   - sidecar text/vector/graph/algebraic indexes
   - hot projection tables inside Antfly relational storage
   - aggregate/materialized expression rows

Materialized relational copies should be optional and workload-driven. A
materialized table can accelerate hot operational queries, but it changes the
cost model and freshness contract. The default lake path should query files in
place.

## Consistency And Freshness

Each query should bind to a snapshot before planning:

- Iceberg: bind to an Iceberg snapshot id.
- Raw Parquet prefix: bind to an Antfly-generated snapshot digest from listed
  object versions.
- File URI tests: bind to file size, mtime, and content digest where practical.

The query result should carry snapshot metadata for debugging and cache
correctness. Sidecar indexes are valid only when their snapshot id or source
file generation covers the query snapshot. If an index is stale, the planner can
either:

- fail closed when the query explicitly requires indexed freshness,
- ignore the stale index and scan files,
- or serve from an older snapshot only when the request asks for that snapshot.

No query should silently mix sidecar candidates from one snapshot with data
files from another.

## Security And Credentials

Lake bindings should never store raw cloud secrets in table schema JSON.

Use a credential reference:

```json
{
  "base_source": {
    "kind": "external",
    "format": "iceberg",
    "uri": "s3://bucket/warehouse/events",
    "credentials": { "ref": "aws-prod-analytics-readonly" }
  }
}
```

The execution layer resolves the credential reference through Antfly secret
management and hands a scoped object-storage client to the scanner. Row-level
authorization filters still need to be pushed into the base row source before
projection, aggregation, joining, lateral correlation, CTE materialization, or
windowing, matching the relational row-plan contract.

## Write Semantics

MVP lake tables are read-only.

Future write modes can be explicit:

- `read_only`: Antfly rejects row writes.
- `materialized_overlay`: Antfly stores inserts/updates/deletes in a relational
  overlay and query execution merges overlay rows with external snapshot rows.
- `iceberg_writer`: Antfly commits new Parquet files and Iceberg metadata
  updates using optimistic table commits.

The read-only path should land first. Overlay and Iceberg writer modes create
transaction, conflict, compaction, and ownership questions that are separate
from efficient querying.

## MVP

The smallest useful path:

1. Add an external base-source binding to relational table schema metadata.
2. Implement raw Parquet prefix snapshots over `file://` and object storage.
3. Read Parquet footers and cache file/row-group/column statistics.
4. Support `rows:query` with projection, scalar filters, limit, and simple
   ordering.
5. Support `rows:aggregate` for `count`, `sum`, `min`, `max`, and `avg` over
   projected scalar columns.
6. Adapt batches to JSON rows initially so existing validation and execution
   paths are reused.
7. Add tests over filesystem-backed Parquet fixtures.

That MVP proves the catalog, snapshot, pruning, and row-plan integration.

## Efficient Version

After the MVP:

1. Introduce a typed `RowSource` and `ColumnVector` execution path.
2. Push filters into row-group/page/dictionary pruning before data reads.
3. Coalesce range reads across requested columns.
4. Add footer and decoded-page caches.
5. Add Iceberg snapshot and manifest support.
6. Add sidecar full-text and vector indexes over external row refs.
7. Add algebraic materialization over external rows.
8. Add adaptive recommendations that decide between scanning lake files,
   using sidecar indexes, or materializing hot projections.

## Open Questions

- Should the catalog model expose lake tables as `storage_mode: relational`
  with `base_source`, or as separate storage modes such as `external_parquet`
  and `external_iceberg`?
- How much of the SQL surface should be accepted before the typed
  column-vector executor exists?
- Should raw Parquet prefix mode support object deletion as snapshot deletion,
  or require append-only conventions for predictable freshness?
- What is the first-class cache eviction policy when lake scans compete with
  document/vector serving workloads?
- How should external row refs be exposed in public APIs without making them a
  durable application identity?
- Should Antfly write sidecar metadata only into Antfly storage, or optionally
  into a user-visible `_antfly/` prefix near the dataset?

## Direction

Build lake query mode as an external relational row source first, not as an
import pipeline. Importing Parquet into Antfly relational tables remains useful,
but the differentiated path is querying object-store data in place while Antfly
adds serving-grade indexing, caching, semantic search, graph traversal, and
adaptive algebraic materialization.
