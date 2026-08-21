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
  manifest-attached external source metadata. The current scaffold can publish
  the pinned inventory as an `external_base_source` artifact and compose an
  attached artifact set plus lake base-source descriptor. Manifest v12 can now
  persist that descriptor through the normal manifest store. Live table
  publication now carries explicit pinned external bindings from table schema
  metadata into published manifests and can apply a resolved external-source
  inventory publication plan before the manifest is written. Dynamic `.current`
  bindings can now flow through the object-store discovery resolver so raw
  Parquet prefixes and Iceberg table roots are pinned into a durable inventory
  artifact before manifest publication.
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
2. Attach external snapshot metadata to published manifests. The scaffold now
   has an attach helper that appends external inventory artifacts to a cloned
   publication artifact set and compatibility-checks the owned base-source
   descriptor. Manifest v12 now stores that descriptor directly on published
   generations, and the serverless build layer now copies explicit pinned
   external base-source descriptors from table definitions into the live
   manifest builders. It can also apply a resolved external-source inventory
   publication plan to the manifest before publish. The publication resolver can
   now discover `.current` raw Parquet and Iceberg bindings, encode the pinned
   inventory with footer-derived row-group metadata, and pass that resolved plan
   into the live builder; Iceberg data files are stat-pinned to provider
   ETag/version identity before the inventory artifact is written.
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

The existing `rows/query`, `rows/aggregate`, `rows/window`, `rows/join`, and
`rows/lateral` APIs should work over both sources. SQL lowering should produce
the same typed request shapes. The executor chooses a local relational scan or
an external lake scan based on the table binding.

## Public Contract

Lake tables should remain logical relational tables. External Parquet, Iceberg,
and Lance datasets are physical base-source adapters, not separate storage
modes. The catalog should therefore keep `storage_mode: "relational"` and add a
base-source binding:

```json
{
  "version": 1,
  "storage_mode": "relational",
  "base_source": {
    "kind": "external",
    "table_id": "events",
    "format": "iceberg",
    "uri": "s3://bucket/warehouse/events",
    "snapshot": "current",
    "schema_fingerprint": "schema-v1",
    "write_policy": "read_only"
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

The runtime binding is intentionally explicit: `table_id` names the logical
external table, `schema_fingerprint` pins the external-to-Antfly type binding,
and `snapshot: "current"` means the catalog resolver must discover and publish a
durable pinned snapshot before query execution. Explicit historical snapshots
should use the same catalog shape with a concrete snapshot id rather than a
provider-specific SQL string:

```json
{
  "snapshot": { "mode": "snapshot_id", "id": "iceberg-123" }
}
```

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
- Iceberg mode honors position deletes and the first flat-scalar equality
  delete path before producing visible rows.
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
  predicates, equality field ids, sequence numbers, and applicability to data
  files.
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
row refs. The first concrete bridges for this now exist for text, sparse, dense
vector, and graph sidecars: `serverless/query/lake_sidecar_candidates.zig`
searches existing text, sparse, vector, and graph segment payloads, treats hit
document ids or graph node ids as encoded external row-ref keys, decodes them
through the source-binding inverse codec, and validates them against the
declared sidecar snapshot before handing them to lake hydration. It can also
load declared text, sparse, vector, and graph sidecar payloads through the
serverless artifact-store interface, select explicit sidecar names, and fail
closed when an implicit request would be ambiguous. For broad
analytical scans, file and row-group pruning should feed large column batches
directly into aggregate/window operators.

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

The Antfly-owned implementation shape is best understood as a staged scaffold
plus remaining hardening. The current branch has landed the first version of
most scaffold pieces; the long-term shape keeps these boundaries even as the
implementations mature:

1. Shared `RowSource` layer.

   Landed as `storage/rowsource/` with shared `SnapshotRef`, `RowRef`,
   `ColumnVector`, and `ColumnBatch` types plus local/external adapters. The
   long-term work is to move more relational execution directly onto typed
   batches instead of repeatedly adapting batches back through JSON rows.

2. Antfly native lake fragments.

   Landed as `serverless/row_fragment/` with a versioned artifact codec, reader,
   writer, stats, and RowSource adapter. Keep the format optimized for stable row
   refs, range reads, projection, point hydration, JSON subtree facts,
   first-class vector payload colocation, and algebraic fold inputs.

3. Serverless manifest source and artifact types.

   Landed artifact/source kinds include `row_fragment`, `row_fragment_stats`,
   `algebraic_segment`, and `external_base_source`. A manifest should continue to
   describe both the authoritative row source and derived sidecars, not only
   published search artifacts. The source side should distinguish Antfly
   serverless fragments, LSM overlays, external Parquet, external Iceberg, and
   external Lance.

4. Fragment publisher from LSM/relational rows.

   First publisher support exists for scanning relational base rows or collected
   row preimages, projecting declared relational columns, encoding row fragments,
   emitting fragment stats, preserving stable row refs, writing artifacts, and
   attaching them to the next manifest. This is the standalone Antfly lake path:
   hot LSM/relational data can be published into immutable object-storage
   fragments owned by Antfly.

5. `ServerlessFragmentRowSource`.

   First support exists for reading pinned manifest row fragments and producing
   `ColumnBatch`. This remains the bridge from serverless artifacts to SQL row
   plans and should share the same snapshot binding as existing query sessions.

6. Algebraic materialization in serverless artifacts.

   Landed as `serverless/algebraic_segment/` with codec, builder, and reader
   support for group-by fold materializations and global expression fold
   materializations. Repeated analytical serving workloads should move from
   scanning row fragments to reading fold artifacts. Remaining algebraic work is
   broader planner-derived shapes such as joins, multi-axis folds, HLL
   cardinalities, histograms, and ranges.

7. Search segments as sidecars over `RowSource`.

   Full-text, vector, sparse, and graph segments now have the first
   RowSource-backed sidecar builders and source declarations. They should
   continue to declare source snapshot id, source schema fingerprint, source
   row-ref kind, source column bindings, and index config hash. The same sidecar
   model should work over document rows, relational rows, serverless row
   fragments, Iceberg rows, and Lance rows. Sidecar builds and shared replay now
   enforce cumulative input, output, retained-item, and replay ceilings plus a
   hard live-allocation working-set ceiling. A configured allocation denial is
   surfaced as the stable lake build/replay budget error instead of an ambiguous
   process-level out-of-memory failure.

8. Adaptive promotion policy.

   First recommendation scaffolds exist for row-fragment publication, hot
   projection promotion, algebraic materialization, and optional
   external-scan-to-fragment promotion. The long-term behavior is adaptive
   ownership: external rows can stay external until repeated workload
   observations justify Antfly-owned fragments or materialized folds.

9. Keep Iceberg and Lance as row sources, not the core.

   `ExternalIcebergRowSource`, `ExternalLanceRowSource`,
   `ExternalParquetRowSource`, `ServerlessFragmentRowSource`, and
   `RelationalStoreRowSource` should all feed the same `ColumnBatch` contract.
   Antfly-owned storage should not be coupled to Iceberg's protocol, and Iceberg
   should not block Antfly-native fragments.

10. Keep the owned milestones ordered.

   The first pieces are landed, but the order still matters for future work:

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

### Extraction Boundary

This tracker records the implementation state of the combined mega branch so
that later extraction PRs do not lose design or completed work. It is not a
claim that every integration named below is compiled in this PR. In particular,
references to `api/table_reads.zig`, `api/http_server.zig`, typed relational row
HTTP/SQL lowering, and catalog/namespace routing describe the mega-branch
prototype. Those public adapters depend on the separately extracted relational
row-query and catalog surfaces and belong in the final lake integration layer;
copying them here would re-import those unrelated stacks and defeat the PR
restructure.

This PR owns the standalone row-source, manifest/publication, object-store,
Parquet/Iceberg scan, cache, sidecar, rebuild, GC, and explain machinery. Its
production boundary is the typed resolver and `RowSource` interfaces. Public
table routing must be ported only after the relational row API and catalog
routing prerequisites land, and should then consume these interfaces without
forking the lake implementation.

The current branch has the Antfly-owned scaffold in place: shared `RowSource`
types, local/external batch adapters, serverless row fragments and stats with
`vector_f32` payload colocation, manifest base-source/artifact kinds,
row-fragment publication, a `ServerlessFragmentRowSource`, algebraic
group/expression materializations, typed sidecar source declarations with
pinned base-source validation, adaptive promotion recommendations, and
scaffold operations for compatibility, GC, explain, rebuild, and cache
accounting.

That scaffold is intentionally not yet the real lake query engine. The
remaining work is the production path that turns the scaffold into efficient
queries over user-owned object-storage files:

| Track | Status | Next Proof |
| --- | --- | --- |
| Real Parquet scanner | Partially implemented for the first flat column paths. Parquet footer preflight parsing now validates trailing `PAR1` trailers and plans exact footer metadata ranges in `serverless/query/lake_parquet_footer.zig`; compact-Thrift footer metadata parsing in `serverless/query/lake_parquet_metadata.zig` extracts row groups, column chunks, physical types, fixed-width type lengths, flat schema repetition metadata, converted-type timestamp annotations, logical-type timestamp annotations, converted-type decimal precision/scale annotations, INT32/INT64 min/max statistics, byte-array/fixed-length-byte-array min/max statistics, boolean min/max statistics, and FLOAT/DOUBLE min/max statistics promoted into f64 inventory bounds, can enrich one or more raw inventory files with parsed footers, derives nullable and logical column-chunk metadata from footer schema elements, and feeds projected scan planning. `serverless/query/lake_parquet_page.zig` parses data page and dictionary page headers, scans PLAIN i64/i32/f32-as-f64/f64/boolean/byte-array/fixed-length-byte-array/INT96 timestamp column chunks across pages, decodes required dictionary-encoded i64, i32, f32-as-f64, f64, byte-array, and fixed-length-byte-array pages through Parquet's RLE/bit-packed hybrid index stream, supports optional dictionary-encoded i64/i32/f32-as-f64/f64/byte-array/fixed-length-byte-array DataPageV2 columns with hybrid definition levels, supports uncompressed, Snappy, gzip, and Zstd page payloads for the current flat scanners, and has flat optional i64/i32/f32-as-f64/f64/boolean/byte-array/fixed-length-byte-array/INT96 timestamp DataPageV2 hybrid definition-level paths that produce `NullBitmap` values. `serverless/query/lake_parquet_rowgroup.zig` assembles required/optional PLAIN i64, required/optional dictionary i64, required/optional PLAIN/dictionary INT64 TIMESTAMP_MILLIS/TIMESTAMP_MICROS/TIMESTAMP_NANOS columns scaled to Antfly nanosecond i64 cells, required/optional PLAIN/dictionary DECIMAL INT32/INT64/BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY columns whose unscaled values fit in i64 promoted to Antfly f64 cells with footer/inventory scale metadata, required/optional PLAIN/dictionary i32 promoted to Antfly i64 cells, required/optional PLAIN INT96 timestamps as Antfly i64 nanosecond cells, required/optional PLAIN/dictionary FLOAT columns promoted to Antfly f64 cells, required/optional PLAIN/dictionary DOUBLE columns as Antfly f64 cells, required/optional PLAIN BOOLEAN cells, and required/optional PLAIN/dictionary byte-array row groups into `ColumnBatch` values with stable external row refs; its supported row-group path now dispatches int64, int64 logical timestamps, int32/int64/binary/fixed logical decimals, int32, int96, float, double, boolean, and byte-array physical types, nullable PLAIN/dictionary metadata for those flat physical types where implemented, PLAIN vs dictionary decoding for supported physical types, and uncompressed/Snappy/gzip/Zstd payload decoding from footer-enriched column-chunk metadata, validates supported projected row groups at planning time, prunes `eq_i64`, `eq_f64`, `eq_bytes`, and `eq_bool` row groups from footer/inventory min/max statistics before object range reads, can discover those row groups by reading object-tail footers through `ObjectRangeReader`, coalesces adjacent projected column chunk reads before dispatch, can reuse cache-keyed object ranges across repeated footer/chunk scans through `ObjectRangeCache`, then feeds both preloaded and object-range-backed `RowSource` adapters into `lake_rows` scans and the first expression-aggregate fast path. The external-source inventory codec now persists physical type, fixed-width type length, logical type, decimal precision/scale, INT32/INT64, byte-array, boolean, and f64 min/max statistics, and a nullable bit for column chunks so generated inventory artifacts can carry the scanner dispatch contract. The public API server now installs the external-lake routing wrapper for supported public `rows/query` and `rows/aggregate` plans, including a `lake_rows_expression_aggregates` hook for eligible global scalar aggregates before falling back to scan-backed aggregation. Public row-query and aggregate lowering now maps floating numeric equality literals to `eq_f64`, and the lake row matcher plus row-group pruner bridge exact numeric equality across i64 and f64 column/stat representations without losing pruning. Count-only aggregates deliberately stay on the scan path until metadata-only counts have the same freshness and delete semantics as row scanning. Configured resolver route tests now cover both S3- and GCS-shaped source URIs through the public rows path, and default credentialed filesystem route tests now write minimal valid two-column Parquet objects, lazily discover row-group metadata from object footers, scan projected and unprojected predicate i64 columns through object-storage range reads, and return the normal public `rows/query` response. Nested/repeated data, exact decimal cells, larger-than-i64 binary decimals, page-index pruning, metadata-only counts, and credentialed S3/GCS provider route fixtures are still missing. | Filesystem-backed Parquet fixture can run projection, scalar predicate, min/max row-group pruning for i64, f64, byte-array, and boolean equality predicates, dictionary/plain mixed column chunks, nullable PLAIN/dictionary i64/i32/f32/f64/byte-array columns, required/optional PLAIN BOOLEAN columns, required/optional PLAIN INT32/INT64/BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY logical decimal columns whose unscaled values fit in i64, required/optional PLAIN INT64 logical timestamp columns, required/optional PLAIN INT96 timestamp columns, footer-derived flat nullable/logical/schema type-length/statistics metadata including FLOAT/DOUBLE bounds, required int32, byte-array, public floating numeric predicates, eligible global expression aggregates, Snappy/gzip/Zstd-compressed i64 pages, and stable external row refs through `rows/query`. |
| Object-store range I/O | Range planning, footer-tail reads, version-aware cache keys, cache lanes, coalescing rules, URI-to-object-ref parsing, and inventory-to-column-chunk read planning exist in `serverless/query/lake_range_io.zig`. It now also plans full-object Iceberg manifest-list and data/delete-manifest metadata reads from S3, GCS, filesystem, and internal `object://` URIs with snapshot/schema/sequence-derived synthetic metadata version ids, stable cache keys, and metadata cache-lane classification. `serverless/query/lake_iceberg_snapshot.zig` now uses Antfly's object-storage abstraction to stat and range-read Iceberg table metadata, manifest-list Avro, and data-manifest Avro objects, reuses those versioned metadata/manifest ranges through the same `ObjectRangeCache.readAlloc` path used by Parquet footer and chunk reads, applies metadata-lane hit/miss/reject accounting and cache admission policy to Iceberg metadata/delete ranges, then hands decoded files to the Iceberg inventory planner. `api/table_reads.zig` resolves an Iceberg table-root URI only through the authoritative `metadata/version-hint.text` pointer (or an explicitly supplied metadata JSON URI), deliberately rejecting metadata-directory guessing because orphaned writer files are not committed table state, then reads the Iceberg snapshot inventory through that object-store path, and hand the resulting `.iceberg` inventory to the same Parquet footer enrichment/scanner wrapper used by raw Parquet prefixes. Deterministic raw Parquet prefix snapshot planning exists in `serverless/external_source/object_snapshot.zig`, including direct object-storage listing through Antfly's object-storage abstraction to pin a prefix inventory from object keys, sizes, and ETags/version IDs. Snapshot inventory now separates the catalog source URI used for binding validation from the physical object URI base used for range reads, so filesystem-backed opened stores can validate against `file://...` while scanning internal `object://bucket/key` references through the same object-storage reader. `serverless/query/lake_parquet_rowgroup.zig` can now read object-tail footer probes, fetch exact footer metadata ranges when the probe is partial, enrich raw file inventories from parsed footers, plan supported projected int64, int32, double, and byte-array row groups, coalesce adjacent projected column chunk reads into fewer physical reads, carry the pinned ETag/version from `RangeRead` through the production object-storage adapter, reuse cache-keyed footer/metadata/chunk bytes through `ObjectRangeCache`, optionally back that cache with `PersistentObjectRangeCache` disk entries keyed by the exact versioned range key, fail closed on short or overlong object range responses, reject corrupted in-memory cache entries whose stored byte length no longer matches the planned range or whose stored bytes fail the cache-entry SHA-256 digest, ignore corrupted persistent cache entries whose embedded versioned range key, stored length, or payload digest does not match the requested range, and decode supported uncompressed, Snappy-compressed, gzip-compressed, or Zstd-compressed flat column chunks from inventory metadata. `serverless/query/lake_object_reader.zig` wraps Antfly's object-storage client as the Parquet range-reader adapter used by production catalog/query routing, issues planned range reads with object-store `If-Match`/version options so stale inventories cannot silently read a newer object body, validates returned ETag/version metadata against the planned object identity when providers surface it, compares provider/local full-object checksums when a planned read covers the complete object, compares range-response checksums when object-store metadata explicitly marks the checksum as response-body scoped, and has bounded retry for transient object-store read failures while leaving stale identity, missing object, and invalid range errors fail-closed. Full catalog protocol resolution beyond object-store table-root metadata discovery is still missing. | Scanner reads footers, projected column chunk ranges, and Iceberg metadata/manifests through Antfly's object-storage abstraction with cache keys and conditional reads that include object version/ETag and byte range, then reuses cached metadata/ranges across scans and source opens through validated persistent range-cache entries when configured. |
| Catalog/schema lake binding | External table binding contract exists in `serverless/external_source/catalog_binding.zig` with URI, format, credential ref, snapshot mode, schema fingerprint, read-only MVP policy, source-kind mapping, manifest base-source conversion, and runtime-schema conversion. Relational table schemas now accept `base_source`/`external_base_source`, validate that external base sources only attach to relational tables, carry the binding through runtime schema derivation, and persist it in the serialized storage schema. `serverless/build/external_source_manifest.zig` can now validate a catalog binding against a discovered inventory, pin `.current` bindings to the inventory snapshot id, reject stale explicit snapshot bindings, and record the pinned snapshot plus inventory artifact in the serverless manifest base source. It also has an attached-artifact helper that clones an existing publication artifact set, appends the external inventory artifact refs, clones the owned base-source descriptor, and runs the lake compatibility check against the combined artifact set. `serverless/build/external_source_publish.zig` now encodes a pinned Parquet/Iceberg inventory, writes it through the serverless artifact store as an `external_base_source` artifact, and returns the manifest plan that points the external base source at that durable inventory artifact. `serverless/build/external_source_plan_resolver.zig` now provides the generic production publication adapter: given an opened Parquet/Iceberg object-store resolver, it discovers a pinned inventory from raw Parquet listings or Iceberg metadata/manifests, pins Iceberg data-file object identity to provider ETag/version metadata, streams sequence- and partition-scoped position/equality deletes under configurable resource ceilings, persists the resulting compact sorted deletion sets with the inventory, and writes that inventory through the artifact store, and returns the external-source manifest plan. `serverless/configured_object_store_support.zig` now centralizes credentialed object-store opening for external table bindings, including `node_config.connections` entries with `kind = external_io`, `lake_read` capability, secret-store-resolved S3 fields, secret-store-resolved GCS bearer-token `Authorization` headers, bucket/prefix scope checks, and deterministic credentialed S3/GCS connection-shape tests that open scoped lake bindings without bucket-creation side effects; public query routing and serverless publication both call through that shared policy. External binding opens are read-only by default: credential-free and credentialed file/S3/GCS bindings no longer create buckets or perform bucket creation as an open-time side effect, while Antfly-owned internal object-store lanes keep their existing create-if-missing behavior. `serverless/manifest/types.zig` and `serverless/manifest/codec.zig` now carry an optional manifest `base_source` through manifest v12 encode/decode, and `serverless/build/external_source_publication.zig` can apply an external-source plan to an owned manifest by appending the inventory artifacts, setting the base-source descriptor, and validating the resulting lake manifest before publication. Live table publication now parses table schema metadata for explicit pinned external base sources and carries those descriptors through `TablePublicationPlan` into normal mutation, rebased, and compacted manifest builders. `TablePublicationPlan` can also carry a resolved external-source inventory publication plan; the live builder applies that plan before storing the manifest, so a `.current` binding that has already been discovered and pinned can publish the inventory artifact and manifest base-source pointer atomically with the generation. `CatalogService` now exposes an `ExternalSourcePlanResolver` seam and passes current external table bindings through it while building table publication plans, so production catalog/runtime code can resolve object-storage state before publication without teaching the builder how to open lake credentials. The serverless bootstrap stack installs a config-aware publication resolver by default; credential-free bindings still open directly, while credentialed filesystem bindings can now pin `.current` external Parquet inventories into durable manifest state through the same configured connection policy used by public reads. Raw Parquet prefix bindings can now discover a pinned file inventory from a scoped object-storage client, and Iceberg bindings can now resolve the scoped table root to Iceberg metadata/manifests, pin the current or requested snapshot, stat-pin active data files to provider object identity, preserve Iceberg data sequence numbers separately, and produce a `.iceberg` file inventory for the shared scanner. `api/table_reads.zig` can turn discovered Parquet or Iceberg inventories plus an object-storage client into an owned lake `TableReadSource` with an `ExternalLakePinnedSourceState` summary for source URI, snapshot id, schema fingerprint, file count, row-group count, row count, and byte count. It also has an opened-object-store wrapper and an `ExternalLakeRoutingTableReadSource` resolver seam so catalog routing can hand off a resolved Antfly object-store client and let the lake source own the store, inventory, pinned source state, and pinned scanner together. `ApiHttpServerConfig.external_lake_object_store_resolver` now lets the public API server install that resolver around supported table reads, `GET /tables/{table}/rows/source` exposes the resolved external lake pinned source state without running a row scan, `GET /tables/{table}/rows/explain` resolves the same pinned source before returning a lake explain plan and physical read summary, and `POST /tables/{table}/rows/explain` lowers supported row-query and aggregate envelopes to their lake scan shape before returning the explain plan plus physical read counts/bytes. For raw inventories without row-group metadata, shaped `POST` explains now include the footer discovery probes and the post-discovery column-chunk reads needed by the effective scan; empty source explains remain footer-probe summaries until a projection is known. The default resolver opens credential-free URIs directly, or resolves catalog credential refs through the shared configured object-store policy before handing a scoped object-store client to the scanner. Object-storage lake scanners now lazily enrich raw object-listing and Iceberg file inventories from Parquet footers at scan time when row-group metadata is not yet present, including the default credentialed filesystem route for Parquet. Live publication still needs richer end-to-end credentialed S3/GCS real-Parquet route coverage and real cloud Iceberg route coverage. | Public catalog/query routing records durable pinned snapshot/manifest state and proves S3/GCS credentialed reads with real Parquet and Iceberg fixtures. |
| Relational `rows/query` integration | Scaffold query helpers exist, and `serverless/query/lake_parquet_rowgroup.zig` now exposes `querySupportedI64ObjectRangeRowsAlloc`: a narrow external Parquet rows-query bridge that validates an optional external binding against a pinned inventory, expands scan columns to include unprojected scalar predicate columns, scans object-range-backed row groups, and returns projected `lake_rows` results with limit and simple equality predicate support. `serverless/query/lake_rows.zig` now tracks total matched rows before limit, filters supplied deleted row refs before group-by aggregation, predicate evaluation, limits, direct hydration, and sidecar candidate hydration, and `api/relational_rows.zig` can lower the supported public `rows/query` subset to a lake scan request and format projected lake rows as the normal public `RelationalRowsQueryResult`. The lake bridge now also supports field-only ordering, numeric expression ordering, `distinct_on`/`distinct_on_expressions` after ordering, `doc_key_range` filtering over projected primary-key columns or relational row refs, offset/limit pagination, scalar OR/NOT predicate groups, structured access OR/NOT predicate groups, expression OR/NOT predicate groups, scalar `expression_where` predicates, expression-array predicates, array predicates, scalar `in`/`not_in` predicates, JSON containment/path predicates, JSON extraction projections, array-length projections, coalesce projections, field-alias projections, text-pattern predicates, and numeric expression projections as bridge paths: ordered queries, distinct queries, doc-key range filters, and residual predicates disable scanner-side limit, include unprojected sort/distinct/primary-key/predicate/expression columns in the lake projection, sort, deduplicate, or filter projected lake rows before public JSON encoding, then apply offset/limit without leaking helper columns into the response. Scalar residual `where` predicates, scalar OR/NOT groups, scalar `in`/`not_in` membership predicates, keyword/text pattern predicates, JSON containment/path predicates, JSON extraction projections, array-length projections, coalesce projections, and field-alias projections now evaluate directly over projected lake cells before JSON row reconstruction; numeric field/value `expression_where`, `expression_any`, `expression_not`, and `distinct_on_expressions` residuals do the same, and expression-only numeric residual queries skip reconstruction entirely. `api/table_reads.zig` now detects bound `external_base_source` schemas for typed row-query plans, dispatches them through a `lake_rows_scan` hook instead of local owner-row scans, provides a pinned external lake scanner helper that validates the runtime binding against a pinned inventory before invoking the object-range Parquet scanner, includes an object-storage-backed pinned lake `TableReadSource` helper for production callers that already resolved a pinned inventory and scoped object-storage client, adds an opened-store ownership wrapper, and adds a routing wrapper that diverts external-bound row-query and aggregate plans through the lake resolver while forwarding ordinary tables to the base source. `api/http_server.zig` now uses that wrapper for public `rows/query` and `rows/aggregate`, including route-level regression tests that prove external-bound row queries open through both a configured resolver and the default `node_config.connections` credential-ref resolver instead of the base table reader; both paths now have successful valid-Parquet coverage for projected output, an unprojected equality predicate, and configured-route ordering over an unprojected scalar sort key through the public response encoder. Broader row-plan features such as joins, windows, full typed expression execution, and full typed-result adaptation still need integration. Projected scan planning in `serverless/query/lake_scan_plan.zig` validates external table bindings against pinned inventories and turns projected columns into logical and coalesced physical object range reads. | Add credentialed S3/GCS successful Parquet route tests, then broaden support to joins and windows. |
| Relational aggregate integration | Algebraic artifact scaffolding exists, and `api/relational_rows.zig` now has a narrow scan-first bridge that lowers supported `rows/aggregate` requests to lake scans and folds projected lake rows back into the normal public `RelationalRowsAggregateResult` for `count`, `sum`, `min`, `max`, and `avg`. The bridge now supports global aggregates, field `group_by`, and expression `group_expressions` over scanned scalar columns: group keys are included in the lake projection, folded through the existing expression evaluator where needed, and emitted through the normal aggregate response shape. Field-only scalar aggregate `filter` predicates, scalar `filter_in`/`not_in` membership predicates, keyword/text `filter_text_patterns`, JSON containment/path filters, scalar expression filters, array filter families, and expression-array containment filters are evaluated after scan, so filtered and unfiltered aggregates can share the same projected lake batch while helper filter columns stay out of the public aggregate rows. Simple aggregate expressions now work for the supported aggregate ops as bridge paths: scan planning collects referenced row fields into the lake projection, evaluates numeric field/value arithmetic, unary numeric functions, and null propagation directly over projected lake cells where possible, then falls back to the existing rows expression evaluator for unsupported expression shapes. Numeric expression filters now use the same typed cell path for `IS NULL`, `IS NOT NULL`, equality/inequality, range comparisons, and null-safe distinct comparisons before falling back to JSON row reconstruction for broader expression shapes. Aggregate output shaping now mirrors the normal relational path for the supported bridge subset: HAVING predicates, HAVING expression groups, output ordering, offset, and limit are applied over encoded aggregate rows while `total_groups` reports the post-HAVING group count before pagination. `api/table_reads.zig` dispatches bound external-base-source aggregate plans through the same lake scan hook, so the public typed aggregate envelope can return the normal aggregate response shape for the supported aggregate subset, and the pinned scanner helper reuses the same inventory validation/object-range scanner path. The configured public route test now proves source text-pattern filtered aggregates backed by text sidecar candidates, grouped filtered, membership-filtered, and expression aggregates over a real external Parquet object through the resolver-backed routing path. Broader typed column-batch execution for complex row-query residual families, grouping/output expressions, full production catalog/object-store source resolution, and automatic selection of algebraic materializations are not yet production. | Broaden typed expression execution into complex row-query residual families and group/output expressions, then promote repeated aggregate shapes into algebraic materializations when available. |
| Iceberg reader | External Iceberg source identity exists. Iceberg table metadata JSON planning in `serverless/external_source/iceberg_metadata.zig` resolves the current snapshot or a requested historical snapshot listed in the metadata file to a manifest-list URI and schema fingerprint; non-current requested snapshots must carry a snapshot schema id so Antfly does not accidentally use the table's current schema for historical data. When metadata carries the Iceberg `schemas` list, it now validates that the pinned schema id exists, rejects malformed/duplicate top-level fields, rejects top-level `struct`, `list`, and `map` fields until the scanner carries Iceberg field IDs through Parquet footer decoding, fingerprints the schema definition body so same-id schema changes do not reuse stale sidecars, and retains top-level field-id-to-column-name mappings for delete planning. `serverless/external_source/iceberg_avro.zig` now decodes uncompressed, Avro `deflate`, Avro `snappy`, and Avro `zstandard` OCF manifest-list files with schema-driven primitive and nullable fields into validated manifest-list entries, including data-vs-delete manifest content, sequence numbers, file counts, and row counts; snappy block CRC32 checksums are validated before decoded rows are trusted. It also decodes uncompressed, Avro `deflate`, Avro `snappy`, and Avro `zstandard` OCF data manifest entries into validated Parquet data/delete-file descriptors with status, snapshot id, data/file sequence numbers, content kind, file path, file format, record count, file size, simple string partition record field counts and values, and equality delete field ids while recursively skipping nested Iceberg record, map, array, bytes, fixed, enum, and nullable fields not yet used by Antfly. `serverless/external_source/types.zig` and `serverless/external_source/codec.zig` now persist file-level partition spec ids, partition field counts, and partition values in external-source inventory v14 so published Iceberg inventory artifacts carry durable pruning metadata. `serverless/external_source/iceberg_inventory.zig` now turns active decoded Parquet data-file entries into `.iceberg` external-source inventories with deterministic ordering, snapshot-derived synthetic file version ids, file paths, file sizes, row counts, partition values, and empty row-group metadata ready for the existing Parquet footer discovery/enrichment path; it can also expand a pinned metadata plan plus decoded manifest list and decoded data manifests into a validated snapshot inventory, checks manifest-list summary counts when present, rejects missing/duplicate manifests, excludes deleted data files, ignores delete-manifest entries whose summary proves there are no active added/existing delete files, and still fails closed when active delete files are present. `serverless/query/lake_range_io.zig` now has the range planning contract for reading Iceberg manifest lists and data/delete manifests through the same metadata cache lane and snapshot-versioned cache key format as Parquet footer metadata, `serverless/query/lake_iceberg_snapshot.zig` can now stat/range-read metadata and manifest objects from object storage, verify data/delete-manifest object lengths against manifest-list declarations, reuse versioned metadata/manifest ranges through the shared object-range cache seam, decode data manifests, read and validate delete manifest metadata through the delete cache lane, expose a structured active position/equality delete-file plan, map decoded position-delete rows to Antfly external row refs when pinned row-group metadata is available, map flat-scalar equality delete rows to Antfly external row refs using resolved Iceberg field ids, grouped tuple-key indexes, data-sequence applicability, and exact partition-spec/tuple scope, ignore inactive delete manifests, and return a pinned `.iceberg` inventory for snapshots that only require data manifests or inactive delete metadata. `serverless/query/lake_rows.zig` now has the common row-delete application primitive for scans and sidecar hydration, plus scan-result adapters that convert projected Iceberg position-delete `file_path`/`pos` rows and equality-delete tuple matches into deleted row refs. Active position-delete files and the first flat-scalar active equality-delete files can now be read as Parquet through the object-range scanner, converted into external row refs, and passed through the common `lake_rows` delete filter during opened Iceberg source scans. `api/table_reads.zig` routes Iceberg external table bindings through that reader for table-root object-store sources. `serverless/query/lake_parquet_rowgroup.zig` now prunes Iceberg files before row-group planning when simple equality predicates contradict persisted partition values for bytes, parseable i64, or canonical boolean values, while keeping files when partition values are absent or cannot be safely interpreted. Non-string partition transforms, richer partition predicate families, nested/complex equality-delete columns, field-ID-aware schema evolution for rename/reorder/nested read compatibility, real cloud Iceberg route fixtures, and catalog protocol clients are still missing. | Iceberg snapshot binding reads metadata/manifests, expands data files, applies position/equality deletes, and preserves schema/snapshot correctness. |
| Sidecar builders over real lake rows | Binding/declaration layer exists, and the first RowSource-backed sidecar publishers now exist. `serverless/segment/source_binding.zig` can now attach optional source column kinds to sidecar bindings, validate batches against those typed bindings, and preserve the typed metadata through sidecar clone/rebuild paths. `serverless/build/lake_sidecar_text.zig` validates pinned text bindings, consumes bytes/JSON text columns, encodes the existing Antfly text segment format with external row-ref document ids, writes through the serverless artifact store, and returns declared sidecar artifacts with store-backed artifact metadata. `serverless/build/lake_sidecar_sparse.zig` does the same for sparse weighted-feature columns encoded as JSON objects, sparse projection documents, or arrays of `{term, weight}` records. `serverless/build/lake_sidecar_vector.zig` consumes decoded `vector_f32` columns or JSON/bytes embedding values, builds the existing clustered Antfly vector segment format with external row-ref document ids, preserves the configured distance metric/build policy, publishes through the serverless artifact store, and fails closed on stale source snapshots or inconsistent dimensions. `serverless/build/lake_sidecar_graph.zig` consumes JSON/bytes graph-edge columns, uses external row-ref keys as lake node ids, writes the existing Antfly graph segment format with out/in adjacencies, publishes through the serverless artifact store, and fails closed on stale source snapshots. `serverless/build/lake_sidecar_algebraic.zig` now folds group-by and expression `count`/`sum_i64`/`min_i64`/`max_i64`/`avg_i64` materializations across all pinned `RowSource` batches into existing algebraic segment artifacts, publishes through the serverless artifact store, and fails closed on stale source snapshots. `serverless/build/lake_rebuild.zig` can now derive desired full-text, dense vector, sparse, graph, single-column algebraic group-by, and global algebraic expression-fold lake sidecars from table/index metadata plus a pinned lake source snapshot, attach the algebraic builder's concrete `bytes`/`i64` source column-kind contract to those bindings, rebuild stale folds whose typed bindings do not match, then feed those desired artifacts into the existing operation planner. Richer algebraic shapes such as joins, multi-axis folds, HLL cardinalities, histograms, and ranges still need the broader algebraic planner path. | Full-text, dense vector, sparse vector, graph, and algebraic sidecars build from external row refs, publish snapshot-keyed artifacts through the serverless artifact store, and produce candidate row refs or materialized folds that hydrate projected lake columns. |
| Freshness and consistency enforcement | Snapshot binding rules are designed; scaffold validation exists for declared artifacts and batches. `serverless/segment/sidecar_manifest.zig` now validates a declared sidecar manifest against a pinned lake base source, requiring the sidecar source kind, snapshot id, and schema fingerprint to match the base source before it can be treated as fresh. `serverless/query/lake_explain.zig` can accept declared sidecars in the explain request, validates those declarations against the pinned base source, rejects stale sidecars by default, reports selected/stale-ignored/not-requested sidecar counts when stale sidecars are explicitly ignored for scan fallback, and now reports candidate-set/hydration accounting for supplied sidecar row refs: supplied sets/refs, usable sets/refs, the effective intersected refs that hydration would use, selected sidecars without candidates, ignored stale/not-requested candidates, missing declarations, and whether sidecar hydration is possible or has an empty intersection. `serverless/segment/source_binding.zig` now validates sidecar candidate row refs against the binding that produced them, including external source id and snapshot id, and `serverless/query/lake_rows.zig` exposes binding-aware hydration that validates candidates and every hydrated batch against the same source snapshot before returning projected rows. The sidecar binding layer now also has a strict inverse codec for `rowRefKeyAlloc`, so text/vector/sparse/graph segment doc IDs that were built from external row refs can be decoded back into owned `RowRef` values and bulk-validated against the declared sidecar binding before they are used as lake hydration candidates. It also now has a conservative sidecar-aware scan helper: fresh selected sidecar candidate refs hydrate projected lake columns, stale ignored sidecars fall back to the normal RowSource scan, required sidecars fail closed when no usable candidate set is present, and missing hydrated candidates are treated as a sidecar/source mismatch instead of silently dropping rows. `serverless/query/lake_sidecar_selection.zig` now provides the query-time selection policy: requested sidecars fail closed on stale snapshot/schema by default, can explicitly ignore stale artifacts to fall back to scanning, and can require requested sidecars when scan fallback is not allowed. `serverless/query/lake_rows.zig` can now derive requested sidecars from candidate sets and run automatic sidecar-aware scans: when candidates exist it requests those sidecar names and applies the freshness policy; multiple selected candidate-producing sidecars are intersected before hydration; when no candidates exist it performs a plain scan without reporting misleading sidecar selections. The Parquet object-range rows scanner and pinned external lake scanner now accept optional sidecar declarations, desired sidecars, selection policy, and candidate row-ref sets, and route bound external scans through the automatic sidecar-aware helper. `ExternalObjectStorageLakeRowsSourceOptions` and `ApiHttpServerConfig.external_lake_sidecar_context` can now carry those sidecar declarations, desired sidecars, stale policy, and candidate sets through the production public external-lake routing wrapper; public `rows/explain` derives matching sidecar artifact refs from the declarations and reports the same sidecar selection/candidate accounting that the scanner will use. `serverless/query/lake_sidecar_candidates.zig` now has concrete text, sparse, dense vector, and graph candidate producers: they search declared sidecar payloads with the existing segment query paths or by loading declared segment artifacts from a serverless `ArtifactStore`, decode hit doc ids or graph node ids back into external row refs, reject stale snapshot/source mismatches, fail closed on ambiguous implicit sidecar selection, and return `lake_rows.SidecarCandidateSet`-compatible owned sets. It also has a combined candidate-plan executor that can run text, sparse, vector, and graph sidecar plans together, intersect duplicate same-sidecar predicates, and emit one scanner-ready candidate bundle for hydration. Public external-lake row-query, `rows/aggregate` source-filter, and `rows/explain` routing can now use an `ApiHttpServerConfig.external_lake_artifact_store` handle to invoke safe text candidate producers for positive exact, case-insensitive exact, simple single-token trailing-prefix, and split multi-token trailing-prefix text-pattern filters when the field maps to exactly one declared text sidecar, including conjunctive multi-field text filters and same-sidecar text-only OR branches, pass generated candidate row refs into the pinned scanner, and report candidate accounting in explain. More complex SQL `LIKE` patterns still fall back to scans so candidate production cannot under-select matching rows. Remaining work is to broaden automatic producer planning to sparse, dense vector, graph, aggregate materialization, and richer text/query operator families. | Query execution integrates automatic sidecar selection, pins one source snapshot end to end, rejects or ignores stale sidecars by policy, and never mixes candidate row refs from one snapshot with data files from another. |
| Cache isolation | Cache accounting classes exist, and the Parquet object-range scanner now has a cache-keyed `ObjectRangeCache` seam for footer, metadata, and coalesced column-chunk bytes. `ObjectRangeCache` now records per-lane hit/miss/stored/evicted/rejected byte counts from `RangeRead.cacheLane()` and supports optional per-lane byte admission limits with lane-local eviction, so compressed broad-scan ranges can churn under their own cap without evicting metadata/footer ranges. The cache policy now also supports a total byte cap plus protected lanes: broad-scan scratch can be rejected or evict its own lane, while protected serving-sidecar ranges remain resident unless another protected/sidecar admission needs space. `PersistentObjectRangeCache` now provides an optional durable backing store for exact range keys; validated persistent hits count as cache hits, each disk record embeds the exact versioned range key plus a SHA-256 payload digest, corrupted or mismatched disk entries are ignored and refreshed from object storage, and admitted fetched ranges are written back best-effort. `ObjectRangeCache.initWithLakeServingDefaults` and the serverless `initLakeParquetServingObjectRangeCache` export now provide a named serving policy that protects metadata and serving sidecars and caps broad-scan scratch at one eighth of the total budget. `api/table_reads.zig` can now instantiate and own that serving cache plus an optional persistent backing from external lake source options, wiring the same cache through Iceberg metadata/manifest reads, Parquet footer discovery, and row scans for opened object-store lake sources. `ApiHttpServerConfig.external_lake_serving_cache_max_bytes` and `external_lake_persistent_cache_root_dir` now give public routes a conservative 64 MiB per-open serving-cache budget by default, while still letting deployments override the budget and durable cache directory through both row-plan routing and `GET /rows/source`. `serverless/query/lake_explain.zig` can now include runtime object-range cache stats by lane, so operators can see metadata, compressed-range, decoded-column, projected-batch, serving-sidecar, and broad-scan scratch hits, misses, stored bytes, evictions, and rejected bytes alongside manifest cache-budget pressure. Antfly now validates embedded range keys, payload digests, returned ETag/version identity, full-object provider checksums, and explicit response-body range checksums before admitting or returning bytes. | Metadata/footer, decoded data/page, broad-scan scratch, and serving-critical sidecar caches have separate admission/eviction policy and optional validated persistent storage. |
| Production operations | Scaffold compatibility, GC, explain, rebuild, cache accounting, and richer external inventory metadata for object versions/row-group ranges/column chunks exist. `serverless/query/lake_explain.zig` validates base-source artifacts and sidecar declarations, reports sidecar selection outcomes, sidecar candidate-set/hydration accounting including intersected candidate cardinality, promotion recommendations, manifest artifact counts/bytes, embeds lake cache accounting with pinned/payload/total byte totals plus budget pressure flags, and can attach runtime object-range cache lane accounting for operator diagnostics. `api/http_server.zig` now exposes the first real operator read endpoint for this path: `GET /tables/{table}/rows/explain` resolves the configured external lake binding, pins the source snapshot through the same object-store resolver as public row reads, and returns the lake explain plan plus range-cache diagnostics and physical read summary without issuing a row scan; `POST /tables/{table}/rows/explain` also parses supported row-query and aggregate plans, applies row-filter pushdown, lowers them to `LakeRowsScanRequest`, validates the pinned inventory through `planProjectedLakeScanAlloc`, and reports the effective scan projected-column count, predicate pushdown, scanner limit, explain operation, logical/physical read counts, footer-probe counts, column-chunk read counts, and logical/physical bytes. For raw prefix inventories that have not yet published row-group metadata, shaped `POST` explain now plans the footer probes, discovers row-group metadata without scanning row data, and reports the resulting coalesced column-chunk reads for the effective physical column set, including unprojected predicate columns. Empty source explains still report only the required footer probes until a projection is known; configured public read explain can now surface declared sidecar selection and candidate accounting from the external-lake sidecar context, and supported text-pattern query and aggregate-source explains can now produce fresh text sidecar candidates from the configured artifact store before reporting hydration accounting. `serverless/build/lake_rebuild.zig` now goes beyond dry-run reuse/rebuild/drop decisions and emits executable rebuild operations with cloned source bindings, artifact kinds, existing artifact ids for reuse/drop, typed build specs, and concrete builder families for full-text, dense vector, sparse vector, graph, algebraic group-by, and algebraic expression sidecars; ambiguous or underspecified algebraic rebuilds fail closed. It can derive desired full-text/vector/sparse/graph sidecars and the supported algebraic materialization subset directly from table/index metadata and a pinned source snapshot, and it now has a resolved external-source helper that validates a manifest base source against a decoded inventory before deriving desired artifacts with the inventory `source_id` used by external row refs. It also has a first executor boundary: a `RowSourceProvider` opens a fresh pinned source for each rebuild operation, the executor dispatches to the existing RowSource-backed sidecar publishers, writes rebuilt declarations through the serverless artifact store, returns declared artifacts for rebuilt sidecars, and records reuse/drop decisions without deleting dropped artifact bytes during execution. Executor results can now be reconciled back into a validated next sidecar manifest: rebuilt declarations are copied from execution output, reused declarations are carried forward from the published artifact set, and dropped declarations are omitted. Dropped artifact bytes should be deleted only after the reconciled manifest is durably published, through a post-publish cleanup helper that verifies the executed drop still matches the planned artifact id. A first resolved-external-source reconcile helper now chains desired-artifact derivation, existing sidecar declaration adaptation, operation planning, execution, and manifest reconciliation for operator code that already resolved the pinned inventory and can provide a pinned `RowSourceProvider`. | Operator workflows add broader algebraic planner-derived build specs for joins/multi-axis/HLL/histogram/range shapes, expose reconcile/rebuild through real commands/controllers, garbage-collect retained snapshots safely after publication, wire concrete query operators into sidecar candidate production and public explain, and cover failure modes with fixtures. |

Algebraic materialized group-by and global expression-fold execution now follow
the same freshness rule: any caller that supplies a materialized algebraic
reader must also supply the required source kind/id/snapshot/schema contract.
`serverless/query/lake_rows.zig` rejects materialized readers without that
contract, only serves a fold whose embedded source matches it, and falls back to
a RowSource scan for stale or cross-source folds.

This tracker is the line between "scaffold complete" and "lake engine
complete." The scaffold proves Antfly's owned shape; the engine is complete
only when external Parquet/Iceberg/Lance tables can be queried through the
public relational APIs with snapshot correctness, object-store-efficient reads,
sidecar acceleration, cache isolation, and operational rebuild/GC behavior.

Freshly published Parquet and Iceberg inventories are expected to be
footer-enriched before their inventory artifact is written. Scan-time footer
discovery remains as compatibility and explain behavior for older or manually
constructed inventories that contain file identities but no row-group metadata.
Publication also fails closed on malformed Parquet footers before writing the
inventory artifact, so a catalog `.current` pin cannot advance to a durable
Antfly manifest unless every discovered data file satisfies the scanner's
footer contract.

The concrete work left for the data-lake path is therefore:

1. Finish production public catalog routing: harden the installed
   external-lake routing source on the public `rows/query`/`rows/aggregate`
   path with real-provider S3/GCS credential fixtures, harden credentialed
   publication for cloud source shapes, and add live-provider fixture-backed
   route coverage for cloud provider connection shapes.
2. Broaden the Parquet reader beyond the first flat i64/i32/f32/f64/boolean/
   byte-array, INT32/INT64/BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY logical decimal,
   INT64 logical timestamp, and INT96 timestamp paths: exact decimal cells,
   larger-than-i64 decimal values, nested repetition levels, additional
   compression codecs where needed, and page-index pruning.
3. Move the hot path from JSON adaptation toward typed `ColumnBatch` execution
   so projection, predicates, ordering, expression aggregates, joins, windows,
   and sidecar hydration do not repeatedly materialize rows.
4. Finish Iceberg delete and evolution correctness: metadata JSON refresh,
   Avro manifest-list and manifest decoding, data-file selection, partition
   pruning, structured delete-file planning, and active position-delete Parquet
   scanning now exist for the first path. Position-delete files are scanned with
   projected `file_path`/`pos` columns, converted through the scan-result
   row-ref adapter, and passed through the common `lake_rows` delete filter.
   Equality-delete manifest entries now preserve validated `equality_ids`,
   Iceberg metadata planning retains top-level field-id-to-column-name mappings
   when schema JSON is available, structured delete plans carry resolved
   equality column names, and the object-storage delete reader can scan
   equality-delete Parquet files plus pinned data files, apply data-sequence
   applicability, and pass matched deleted row refs through the common
   `lake_rows` delete filter for the supported flat scalar cell types. Equality
   delete matching now also lazily discovers data-file footers when the Iceberg
   manifest inventory has no row-group metadata yet, then rebinds matched row
   refs back to the caller-owned pinned inventory before returning them.
   Iceberg data-file sequence numbers are now carried as structured inventory
   metadata and persisted by the external-source inventory codec, so equality
   delete applicability no longer depends on parsing synthetic object-version
   strings. Opened object-store Iceberg sources stat active data files before
   scanning and replace manifest-synthetic file versions with provider
   ETag/version identity while preserving those Iceberg sequence numbers. The
   Iceberg schema planning now also fails closed when the pinned schema carries
   top-level `struct`, `list`, or `map` fields, because Antfly's current scan
   contract still resolves Parquet columns by flat names rather than Iceberg
   field IDs. Parquet footer discovery and the external-source inventory codec
   now preserve optional Parquet schema field IDs on column chunks. Lazy footer
   discovery/enrichment accepts Iceberg inventories as well as raw Parquet
   inventories, carries those footer-derived field IDs into the enriched
   inventory, and Iceberg object-range planning rejects projected chunks that
   lack those IDs so name-only Parquet metadata is not treated as
   schema-evolution-safe. The remaining work is to broaden equality deletes to
   nested/complex fields, tighten real-provider version/ETag fixtures, and use
   field IDs for actual rename/reorder/nested read compatibility instead of only
   enforcing their presence.
5. Complete sidecar builders over real external row refs: full-text, dense
   vector, sparse, graph, algebraic group-by, and algebraic expression-fold
   paths now consume pinned `RowSource` batches and publish declared sidecar
   artifacts with external row-ref doc ids or materialized folds through the
   serverless artifact store. Rebuild planning now emits executable builder
   operations for those sidecar families, table/index metadata can derive
   desired full-text/vector/sparse/graph sidecars plus supported algebraic
   `count`/`sum`/`min`/`max`/`avg` materializations for a pinned source snapshot, the
   first executor can run those operations against a supplied pinned
   `RowSourceProvider` and artifact store, executor results can reconcile
   rebuild/reuse/drop outcomes into a validated next sidecar manifest, and a
   resolved external-source helper now validates manifest base-source metadata
   against a decoded inventory before using the inventory identity for external
   row-ref-compatible rebuild bindings. A resolved external reconcile helper
   now chains derivation, current sidecar declaration adaptation, operation
   planning, execution, and manifest reconciliation for operator code that
   already resolved the pinned inventory. Remaining work is to add broader
   algebraic planner-derived build specs and invoke that workflow from real
   reconcile/operator commands or controllers.
6. Finish production freshness and cache policy: the text, sparse, dense
   vector, and graph sidecar paths now have concrete candidate producers that
   search segment payloads, can load declared sidecars through the serverless
   artifact-store interface, convert hit doc IDs or graph node IDs through the
   validated sidecar row-ref decoder, and fail closed when implicit sidecar
   selection would be ambiguous. The public external-lake row-query and
   `rows/aggregate`/`rows/explain` paths can now invoke the first
   artifact-store-backed text candidate producer for safe positive exact,
   case-insensitive exact, simple single-token trailing-prefix, and split
   multi-token trailing-prefix text-pattern filters over
   row-query predicates and aggregate source predicates when the field maps to
   exactly one declared text sidecar. It can also accelerate conservative
   text-only OR branches by unioning candidates when every branch resolves to
   the same single declared text sidecar, then feed generated candidate sets
   into the pinned scanner/explain context. Remaining work is to broaden automatic producer
   planning to sparse, dense vector, graph, aggregate materialization, and
   richer text/query operator families, tune deployment-specific values above
   the conservative public serving-cache default, and compare provider-native
   cloud-provider mappings for response-body checksum scope when they surface
   range-scoped checksum headers in addition to whole-object checksums.
7. Add operator workflows: rebuild/reconcile commands, snapshot retention and
   garbage collection, cache accounting, fixture coverage, and compatibility
   checks for manifest/artifact versions.

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

## Landed Baseline

The smallest useful path is now the baseline the production work builds from:

1. External base-source bindings attach to relational table schema metadata.
2. Raw Parquet prefix snapshots work over `file://` and object storage as a
   convenience source mode.
3. Parquet footer discovery reads and persists file, row-group, column, and
   statistics metadata.
4. `rows/query` supports projection, scalar filters, limit, simple ordering, and
   broader residual row-plan features through the lake bridge.
5. `rows/aggregate` supports scan-backed scalar aggregates and the first
   expression-aggregate fast path over typed lake batches for eligible global
   `sum`, `min`, `max`, and `avg` requests, with count-only requests staying on
   the scan path until metadata-only counts land.
6. JSON adaptation still exists as the compatibility boundary for broad row-plan
   execution, while new lake/serverless paths should move toward typed
   `ColumnBatch` execution.
7. Filesystem-backed Parquet fixtures cover the current scanner and public route
   contracts.

That baseline proves the catalog, snapshot, pruning, and row-plan integration.
The remaining work is not a second MVP; it is hardening the lake engine for real
provider routes, richer Parquet/Iceberg semantics, typed execution, sidecar
automation, and operator workflows.

## Efficient Version

The efficient long-term version should continue to:

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
