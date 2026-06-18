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

## Alignment With Lakehouse RT And LTAP

The lakehouse industry is moving toward storage-layer unification: one governed
copy of data in open table formats, with specialized engines for transactional
writes, analytical scans, and low-latency serving. Databricks' Lakehouse//RT and
LTAP framing is a useful reference point for Antfly's long-term direction:

- Lakehouse//RT maps to Antfly's external relational row source: low-latency
  query directly over governed lake tables without copying data into a separate
  serving database as the source of truth.
- LTAP maps to a later write-path direction: operational writes can eventually
  land in open table formats instead of requiring ETL from an operational store
  into a lake.
- The shared idea is storage-layer unification, not one engine for every
  workload. Different engines can remain best suited for row transactions,
  vector/search serving, graph traversal, and analytical scans as long as they
  share one authoritative table snapshot model.

Antfly should not start by promising arbitrary warehouse-platform completeness,
but it should own more than a passive sidecar role. Its LSM storage, relational
SQL lowering, full-text/vector/sparse/graph segments, serverless
manifest/artifact model, and algebraic indexes already cover much of the
infrastructure needed for adaptive analytical serving. The right positioning is
therefore:

> Antfly is the adaptive analytical serving, retrieval, indexing, and agent
> context layer over native Antfly rows and open lake tables.

That means Antfly should own SQL as a serving/query contract, algebraic
materialization as a latency accelerator, hybrid retrieval over live governed
data, and serverless indexed execution over immutable object-storage fragments.
It should defer generic warehouse platform promises such as arbitrary
multi-terabyte ad hoc shuffle execution, Spark-style transformation pipelines,
full BI warehouse compatibility, and full Iceberg compaction/vacuum/layout
management until the lake-native serving path proves the necessary execution
and operational machinery.

The "one copy" claim needs precise language. Lake tables should have one
authoritative base copy in Iceberg/Parquet/Lance or native Antfly relational
storage. Antfly may still maintain derived sidecars: text indexes, vector
indexes, graph indexes, aggregate materializations, decoded-page caches, and hot
projection caches. Those are not source-of-truth copies when they are
snapshot-keyed, freshness-checked, rebuildable, and garbage-collected with the
same snapshot-retention rules as the base table.

## Fit With Serverless Runtime

The serverless runtime is the right physical substrate for lake query mode. It
already has the object-store, immutable-manifest, artifact, range-read, cache,
background-build, and query-session machinery that lake serving needs. The main
difference is source ownership: current serverless publication builds Antfly
artifacts from Antfly WAL/document mutations, while lake mode binds to
user-owned external table snapshots and treats Antfly artifacts as derived
sidecars.

The mapping is direct:

- A lake table snapshot or Iceberg snapshot maps to a serverless manifest
  version.
- Parquet files, row groups, delete files, and table metadata map to
  manifest-attached external source metadata.
- Antfly text, vector, sparse, graph, and algebraic accelerators map to
  serverless artifacts.
- Query execution pins one manifest version before reading artifacts, matching
  the lake requirement that each query bind one immutable snapshot.
- Artifact range reads and cache blocks map to Parquet footer, column-chunk,
  page-index, and decoded-column cache reads.
- Serverless build and impact planning map to sidecar freshness, rebuild,
  reuse, and garbage-collection decisions.

Lake mode should not force Parquet data files into the existing document
artifact model. Instead, add an external base-source entry to the manifest:

```zig
pub const ExternalBaseSource = struct {
    format: enum { parquet_prefix, iceberg, lance },
    source_uri: []const u8,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    file_inventory_artifact: ?[]const u8 = null,
    row_group_metadata_artifact: ?[]const u8 = null,
    delete_metadata_artifact: ?[]const u8 = null,
};
```

The source metadata identifies the authoritative external rows. Serverless
artifacts then remain rebuildable sidecars over those rows. A query over an
external table opens the pinned manifest, resolves the external base source,
uses object-store range reads to scan Parquet/Iceberg metadata and projected
columns, and optionally intersects those rows with sidecar index candidates.

The incremental serverless path is:

1. Store external source bindings in table/catalog definitions.
2. Attach external snapshot metadata to published manifests.
3. Add file inventory and row-group metadata artifacts.
4. Add a Parquet footer scanner over existing object-store range reads.
5. Add `ExternalParquetRowSource` behind the relational row-plan executor.
6. Build existing text/vector/sparse/graph sidecars from external row refs.
7. Add Iceberg manifest and delete-file support.
8. Extend cache classification for lake metadata, decoded column pages, and
   broad-scan scratch so serving-critical index blocks stay protected.

This keeps the logical design in relational/lake terms while reusing the
serverless runtime as the physical serving layer.

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

Lake tables should remain logical relational tables. External Parquet, Iceberg,
and Lance datasets are physical base-source adapters, not separate storage
modes. The catalog should therefore keep `storage_mode: "relational"` and add a
base-source binding:

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

SQL remains syntax sugar over the same typed row-plan API. Long term, Antfly
should accept broad SQL syntax only when it lowers to typed row plans plus known
source capabilities. Queries that cannot lower into that contract should fail
closed or route through an explicit experimental/batch path. The durable engine
contract is the typed plan, not backend SQL text.

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
Public identity should be the declared primary key whenever one exists. External
row references are physical identities for sidecar indexes, cache keys, repair,
explain plans, cursors, and catalog/debug APIs. They may be exposed as
`physical_key`, but they should not be promoted as stable application identity:
compaction, rewrite, and table-format maintenance can move rows even when the
logical primary key is unchanged.

Deletes and updates from external table formats are snapshot concerns:

- Raw Parquet prefix mode is a convenience mode with append/replace semantics
  based on object changes.
- Iceberg mode honors position deletes and equality deletes before producing
  visible rows.
- Antfly sidecar indexes are versioned by snapshot id and garbage-collected
  after snapshot retention allows it.

Production users who need deletes, schema evolution, time travel, or strong
snapshot correctness should use Iceberg. Raw Parquet prefixes are useful for
tests, simple append-only datasets, and low-friction adoption, but Iceberg is
the long-term durable table abstraction.

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

This belongs in Antfly's metadata/catalog path by default, not in the user
bucket. Large sidecar files should be written to Antfly-owned object storage.
An optional user-visible `_antfly/` sidecar prefix can be added later for
portable rebuilds, offline inspection, cross-cluster sharing, and disaster
recovery, but it should be an explicit table option because it mutates the
user's dataset location and changes the security model.

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

## Antfly-Owned Lake-Native Scaffold

The Antfly-owned path should be a first-class native lake fragment stack, not
"Iceberg but ours" and not an extension of the current document segment alone.
The north star is:

```text
LSM hot mutable layer
  +
Antfly serverless immutable fragments
  +
Antfly sidecar indexes/materializations
  +
one RowSource/SQL contract
```

The 10-step Antfly-owned implementation plan:

1. Add a shared `RowSource` layer.

   Create `storage/rowsource/` with shared `SnapshotRef`, `RowRef`,
   `ColumnVector`, and `ColumnBatch` types, plus initial adapters for JSON rows,
   relational-store rows, serverless fragments, external Parquet, external
   Iceberg, and external Lance. The first implementation can still adapt
   `ColumnBatch` to JSON rows for the existing executor, but new lake/serverless
   code should target typed batches.

2. Define Antfly native lake fragments.

   Add `serverless/row_fragment/` with `types.zig`, `codec.zig`, `writer.zig`,
   `reader.zig`, and `mod.zig`. The first format should be an internal,
   versioned artifact codec with a header, schema fingerprint, row count, row-ref
   directory, column directory, column chunks, and optional min/max/null and
   bounded dictionary stats. Optimize for stable row refs, range reads,
   projection, point hydration, JSON subtree facts, vector payload colocation,
   and algebraic fold inputs.

3. Extend serverless manifest types.

   Add artifact/source kinds such as `row_fragment`, `row_fragment_stats`,
   `algebraic_segment`, and `external_base_source`. A manifest should describe
   both the authoritative row source and derived sidecars, not only published
   search artifacts. The source side should distinguish Antfly serverless
   fragments, LSM overlays, external Parquet, external Iceberg, and external
   Lance.

4. Build a fragment publisher from LSM/relational rows.

   Add `serverless/build/row_fragments.zig` to scan relational base rows or
   collected row preimages, project declared relational columns, encode row
   fragments, emit fragment stats, preserve stable row refs, write artifacts,
   and attach them to the next manifest. This is the standalone Antfly lake path:
   hot LSM/relational data can be published into immutable object-storage
   fragments owned by Antfly.

5. Add `ServerlessFragmentRowSource`.

   Add a row-source implementation that reads pinned manifest row fragments and
   produces `ColumnBatch`. This is the bridge from serverless artifacts to SQL
   row plans and should share the same snapshot binding as existing query
   sessions.

6. Move algebraic materialization into serverless artifacts.

   Add `serverless/algebraic_segment/` with codec, builder, and reader support
   for group-by fold materializations, expression fold materializations,
   adaptive recommendations, external-row-ref keyed aggregates, and
   serverless-fragment keyed aggregates. Repeated analytical serving workloads
   should move from scanning row fragments to reading fold artifacts.

7. Treat current search segments as sidecars over `RowSource`.

   Full-text, vector, sparse, and graph segments should declare source snapshot
   id, source schema fingerprint, source row-ref kind, source column bindings,
   and index config hash. The same sidecar model should work over document rows,
   relational rows, serverless row fragments, Iceberg rows, and Lance rows.

8. Add adaptive promotion policy.

   Add policy for row-fragment publication, hot projection promotion, algebraic
   materialization, and optional external-scan-to-fragment promotion. This turns
   adaptive ownership into concrete behavior: external rows can stay external
   until repeated workload observations justify Antfly-owned fragments or
   materialized folds.

9. Keep Iceberg and Lance as row sources, not the core.

   `ExternalIcebergRowSource`, `ExternalLanceRowSource`,
   `ExternalParquetRowSource`, `ServerlessFragmentRowSource`, and
   `RelationalStoreRowSource` should all feed the same `ColumnBatch` contract.
   Antfly-owned storage should not be coupled to Iceberg's protocol, and Iceberg
   should not block Antfly-native fragments.

10. Stage the implementation as owned milestones.

   Land the pieces in this order:

   - `rowsource/types.zig` with the stable `SnapshotRef`, `RowRef`,
     `ColumnVector`, and `ColumnBatch` contract.
   - JSON and relational adapters so the existing row-query executor can run
     unchanged while the column-batch executor matures.
   - `serverless/row_fragment` codec, reader, writer, and format tests.
   - A relational-to-row-fragment publisher that turns Antfly-owned LSM and
     relational rows into immutable serverless fragments.
   - A `ServerlessFragmentRowSource` query path that proves SQL/row plans can
     read Antfly-owned object-storage fragments.
   - Sidecar text, vector, sparse, and graph indexes keyed by serverless row
     refs and pinned source snapshots.
   - One algebraic segment for a simple group-by aggregate, then expression
     folds and adaptive aggregate recommendations.
   - External Parquet and Iceberg row sources behind the same `RowSource`
     contract.
   - Adaptive promotion from external scans to Antfly row fragments, hot
     projections, or materialized folds.
   - Operational hardening: manifest compatibility, snapshot garbage
     collection, explain plans, rebuild tooling, and cache accounting.

The owned milestones matter because they let Antfly become useful even before
the full external lake ecosystem is complete. Steps 1 through 7 create a
standalone Antfly lake-native serving path over Antfly-owned immutable
fragments. Steps 8 through 10 make that path interoperable with Iceberg,
Parquet, and Lance-style data without making those protocols the center of
Antfly's storage design.

## Implementation Tracker

The current branch has the Antfly-owned scaffold in place: shared `RowSource`
types, local/external batch adapters, serverless row fragments and stats,
manifest base-source/artifact kinds, row-fragment publication, a
`ServerlessFragmentRowSource`, algebraic group/expression materializations,
sidecar source declarations, adaptive promotion recommendations, and scaffold
operations for compatibility, GC, explain, rebuild, and cache accounting.

That scaffold is intentionally not yet the real lake query engine. The
remaining work is the production path that turns the scaffold into efficient
queries over user-owned object-storage files:

| Track | Status | Next Proof |
| --- | --- | --- |
| Real Parquet scanner | Partially implemented for the first flat column paths. Parquet footer preflight parsing now validates trailing `PAR1` trailers and plans exact footer metadata ranges in `serverless/query/lake_parquet_footer.zig`; compact-Thrift footer metadata parsing in `serverless/query/lake_parquet_metadata.zig` extracts row groups and column chunks, can enrich one or more raw inventory files with parsed footers, and feeds projected scan planning. `serverless/query/lake_parquet_page.zig` parses data page and dictionary page headers, scans PLAIN i64/byte-array column chunks across pages, decodes required dictionary-encoded i64 pages through Parquet's RLE/bit-packed hybrid index stream, supports uncompressed and Snappy page payloads for the current flat scanners, and has a first flat optional i64 DataPageV2 hybrid definition-level path that produces `NullBitmap` values. `serverless/query/lake_parquet_rowgroup.zig` assembles required PLAIN i64, optional PLAIN i64, and required dictionary i64 row groups into `ColumnBatch` values with stable external row refs; its supported-i64 row-group path now dispatches PLAIN vs dictionary decoding and uncompressed vs Snappy payload decoding from footer-enriched column-chunk metadata, validates supported projected row groups at planning time, can discover those row groups by reading object-tail footers through `ObjectRangeReader`, coalesces adjacent projected column chunk reads before dispatch, can reuse cache-keyed object ranges across repeated footer/chunk scans through `ObjectRangeCache`, then feeds both preloaded and object-range-backed `RowSource` adapters into `lake_rows` scans. Zstd/gzip codecs, repetition levels/nested data, nullable dictionary columns, broader nullable column types, and public `rows:query` routing are still missing. | Filesystem-backed Parquet fixture can run projection, scalar predicate, row-group pruning, dictionary/plain mixed column chunks, Snappy-compressed i64 pages, and stable external row refs through `rows:query`. |
| Object-store range I/O | Range planning, footer-tail reads, version-aware cache keys, cache lanes, coalescing rules, URI-to-object-ref parsing, and inventory-to-column-chunk read planning exist in `serverless/query/lake_range_io.zig`. Deterministic raw Parquet prefix snapshot planning exists in `serverless/external_source/object_snapshot.zig`, including direct object-storage listing through Antfly's object-storage abstraction to pin a prefix inventory from object keys, sizes, and ETags/version IDs. `serverless/query/lake_parquet_rowgroup.zig` can now read object-tail footer probes, fetch exact footer metadata ranges when the probe is partial, enrich raw file inventories from parsed footers, plan supported projected i64 row groups, coalesce adjacent column chunks into fewer physical reads, read those ranges through an `ObjectRangeReader` seam, reuse cache-keyed footer/metadata/chunk bytes through `ObjectRangeCache`, and decode supported uncompressed or Snappy-compressed i64 encodings from inventory metadata. `serverless/query/lake_object_reader.zig` wraps Antfly's object-storage client as the Parquet range-reader adapter used by production catalog/query routing. Persistent cache admission/eviction and object-store retry/checksum policy are still missing. | Scanner reads footers and projected column chunk ranges through Antfly's object-storage abstraction with cache keys including object version/ETag and byte range, then reuses cached metadata/ranges across scans. |
| Catalog/schema lake binding | External table binding contract exists in `serverless/external_source/catalog_binding.zig` with URI, format, credential ref, snapshot mode, schema fingerprint, read-only MVP policy, source-kind mapping, manifest base-source conversion, and runtime-schema conversion. Relational table schemas now accept `base_source`/`external_base_source`, validate that external base sources only attach to relational tables, carry the binding through runtime schema derivation, and persist it in the serialized storage schema. `serverless/build/external_source_manifest.zig` can now validate a catalog binding against a discovered inventory, pin `.current` bindings to the inventory snapshot id, reject stale explicit snapshot bindings, and record the pinned snapshot plus inventory artifact in the serverless manifest base source. Raw Parquet prefix bindings can now discover a pinned file inventory from a scoped object-storage client, and `api/table_reads.zig` can turn that discovered inventory plus client into an owned lake `TableReadSource`. It also has an opened-object-store wrapper and an `ExternalLakeRoutingTableReadSource` resolver seam so catalog routing can hand off a resolved Antfly object-store client and let the lake source own the store, inventory, and pinned scanner together. Service-level credential-ref lookup and automatic installation of that resolver around public table reads are still missing. | Public catalog/query routing loads the table schema binding, resolves credentials to a scoped object-storage client, opens the object store through the resolver, instantiates the opened lake source, publishes or records the pinned manifest, and routes `rows:query` against that pinned binding. |
| Relational `rows:query` integration | Scaffold query helpers exist, and `serverless/query/lake_parquet_rowgroup.zig` now exposes `querySupportedI64ObjectRangeRowsAlloc`: a narrow external Parquet rows-query bridge that validates an optional external binding against a pinned inventory, expands scan columns to include unprojected scalar predicate columns, scans object-range-backed row groups, and returns projected `lake_rows` results with limit and simple equality predicate support. `serverless/query/lake_rows.zig` now tracks total matched rows before limit, and `api/relational_rows.zig` can lower the supported public `rows:query` subset to a lake scan request and format projected lake rows as the normal public `RelationalRowsQueryResult`. `api/table_reads.zig` now detects bound `external_base_source` schemas for typed row-query plans, dispatches them through a `lake_rows_scan` hook instead of local owner-row scans, provides a pinned external lake scanner helper that validates the runtime binding against a pinned inventory before invoking the object-range Parquet scanner, includes an object-storage-backed pinned lake `TableReadSource` helper for production callers that already resolved a pinned inventory and scoped object-storage client, adds an opened-store ownership wrapper, and adds a routing wrapper that diverts external-bound row-query and aggregate plans through the lake resolver while forwarding ordinary tables to the base source. Broader row-plan features such as ordering, expression predicates, JSON predicates, joins, windows, and full typed-result adaptation still need integration. Projected scan planning in `serverless/query/lake_scan_plan.zig` validates external table bindings against pinned inventories and turns projected columns into logical and coalesced physical object range reads. | Install the external-lake routing wrapper in the public API/server construction path with real credential resolution, then broaden support to ordering, expression predicates, JSON predicates, joins, and windows. |
| Relational aggregate integration | Algebraic artifact scaffolding exists, and `api/relational_rows.zig` now has a narrow scan-first bridge that lowers supported global `rows:aggregate` requests to lake scans and folds projected lake rows back into the normal public `RelationalRowsAggregateResult` for `count`, `sum`, `min`, `max`, and `avg`. `api/table_reads.zig` dispatches bound external-base-source aggregate plans through the same lake scan hook, so the public typed aggregate envelope can return the normal aggregate response shape for the supported global aggregate subset, and the pinned scanner helper reuses the same inventory validation/object-range scanner path. Grouped aggregates, aggregate filters, expression aggregates, typed column-batch execution, full production catalog/object-store source resolution, and automatic selection of algebraic materializations are not yet production. | Connect pinned manifest scans to the aggregate table-read hook through the pinned scanner helper, then promote repeated aggregate shapes into algebraic materializations when available. |
| Iceberg reader | External Iceberg source identity exists. Iceberg table metadata JSON planning in `serverless/external_source/iceberg_metadata.zig` resolves a current/requested snapshot to a manifest-list URI and schema fingerprint; Avro manifest-list/manifest decoding, data-file expansion, and delete handling are not implemented. | Iceberg snapshot binding reads metadata/manifests, expands data files, applies position/equality deletes, and preserves schema/snapshot correctness. |
| Sidecar builders over real lake rows | Binding/declaration layer exists. Builders still need to consume real Parquet/Iceberg batches. | Full-text, dense vector, sparse vector, and graph sidecars build from external row refs and can produce candidate row refs that hydrate projected lake columns. |
| Freshness and consistency enforcement | Snapshot binding rules are designed; scaffold validation exists for declared artifacts and batches. | Query planning pins one source snapshot, rejects or ignores stale sidecars by policy, and never mixes candidate row refs from one snapshot with data files from another. |
| Cache isolation | Cache accounting classes exist, and the Parquet object-range scanner now has a cache-keyed `ObjectRangeCache` seam for footer, metadata, and coalesced column-chunk bytes. Runtime integration with persistent cache storage, cache-class admission, and eviction policy is not complete. | Metadata/footer, decoded data/page, broad-scan scratch, and serving-critical sidecar caches have separate admission/eviction policy. |
| Production operations | Scaffold compatibility, GC, explain, rebuild, cache accounting, and richer external inventory metadata for object versions/row-group ranges/column chunks exist. | Operator workflows rebuild sidecars from snapshot inventories, garbage-collect retained snapshots safely, expose explain output for lake scans/sidecars, and cover failure modes with fixtures. |

This tracker is the line between "scaffold complete" and "lake engine
complete." The scaffold proves Antfly's owned shape; the engine is complete
only when external Parquet/Iceberg/Lance tables can be queried through the
public relational APIs with snapshot correctness, object-store-efficient reads,
sidecar acceleration, cache isolation, and operational rebuild/GC behavior.

The concrete work left for the data-lake path is therefore:

1. Finish public catalog routing: install the external-lake routing source in
   the API/server construction path, load an external table schema, resolve the
   credential reference, open a scoped object store, pin the source snapshot,
   instantiate the opened lake `TableReadSource`, and run the existing
   `rows:query`/`rows:aggregate` routing through it.
2. Broaden the Parquet reader beyond the first flat i64/byte-array paths:
   nullable dictionary columns, more scalar types, timestamps/decimals, nested
   repetition levels, additional compression codecs, and page-index pruning.
3. Move the hot path from JSON adaptation toward typed `ColumnBatch` execution
   so projection, predicates, aggregates, joins, windows, and sidecar hydration
   do not repeatedly materialize rows.
4. Implement Iceberg metadata and manifest expansion: metadata JSON refresh,
   Avro manifest-list and manifest decoding, data-file selection, partition
   pruning, position deletes, equality deletes, and schema evolution checks.
5. Build sidecars over real external row refs: full-text, vector, sparse,
   graph, and algebraic builders that consume pinned Parquet/Iceberg batches and
   publish snapshot-keyed artifacts.
6. Add production freshness and cache policy: reject stale sidecars when needed,
   never mix snapshots, isolate metadata/footer cache from decoded broad-scan
   scratch, and add retry/checksum behavior for object-store range reads.
7. Add operator workflows: explain plans for lake scans and sidecar hits,
   rebuild/reconcile commands, snapshot retention and garbage collection, cache
   accounting, fixture coverage, and compatibility checks for manifest/artifact
   versions.

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

Cache eviction should be workload-aware rather than one shared object cache.
Serving-critical document, text, and vector index pages must have a protected
class. Lake metadata/footer cache should have a separate class. Decoded lake
column pages and broad-scan scratch data should be admitted conservatively and
evicted before serving-critical state. A cheap broad lake scan must not evict
the hot retrieval indexes that make Antfly useful as a low-latency serving
system.

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
- `lake_native_relational`: Antfly accepts relational writes through a hot
  row/cache layer, then columnizes and commits those writes into Iceberg/Parquet
  as the authoritative storage representation.

The read-only path should land first. Overlay and Iceberg writer modes create
transaction, conflict, compaction, and ownership questions that are separate
from efficient querying.
`lake_native_relational` is the LTAP-like end state and should stay behind the
query/indexing work until Antfly has proven snapshot binding, sidecar freshness,
cache isolation, and Iceberg commit correctness.

## MVP

The smallest useful path:

1. Add an external base-source binding to relational table schema metadata.
2. Implement raw Parquet prefix snapshots over `file://` and object storage as
   a convenience source mode.
3. Read Parquet footers and cache file/row-group/column statistics.
4. Support `rows:query` with projection, scalar filters, limit, and simple
   ordering.
5. Support `rows:aggregate` for `count`, `sum`, `min`, `max`, and `avg` over
   projected scalar columns.
6. Adapt batches to JSON rows initially so existing validation and execution
   paths are reused.
7. Add tests over filesystem-backed Parquet fixtures.

That MVP proves the catalog, snapshot, pruning, and row-plan integration.
Iceberg should follow soon after the MVP because it is the production-grade
snapshot and table-evolution contract.

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

## Long-Term Vision

Antfly should become an adaptive analytical serving and lake-native execution
layer for operational and lake data, with one typed relational query contract
across native Antfly rows, document-backed JSON, serverless Antfly fragments,
and external immutable files.

The durable product direction is adaptive ownership:

- Cold and broad analytical data stays in Parquet/Iceberg.
- Hot metadata, footers, and row-group statistics stay in Antfly cache/catalog.
- Hot filters and projections become cached column batches or optional
  materialized relational projections.
- Hot search, vector, sparse, graph, and algebraic access paths become
  Antfly-native sidecar indexes over external row refs.
- Hot aggregates become algebraic materializations.
- Truly operational subsets can be promoted into native relational Antfly
  tables when users need Antfly to own write serving and transaction semantics.
- Repeated analytical serving workloads can be promoted into Antfly serverless
  fragments and materialized folds rather than repeatedly scanning external lake
  files.

This is not limited to "index beside someone else's lake." The long-term shape
is one `RowSource` contract over several physical bases:

- LSM-backed native relational rows for hot mutable serving.
- Serverless Antfly segments for immutable object-storage serving.
- Iceberg/Parquet external tables for existing enterprise lakes.
- Lance-style external tables for vector-native lake data.
- Future Antfly-native lake fragments optimized for stable row refs, range
  reads, JSON subtree facts, vector payloads, and algebraic folds.

Build lake query mode as an external relational row source first, not as an
import pipeline. Importing Parquet into Antfly relational tables remains useful,
but the differentiated path is querying object-store data in place while Antfly
selectively owns the access paths and fragments that need serving-grade
latency. Over time, that can cover a meaningful subset of warehouse-shaped
workloads, especially repeated agent and application queries, without making
arbitrary warehouse replacement the day-one product promise.
