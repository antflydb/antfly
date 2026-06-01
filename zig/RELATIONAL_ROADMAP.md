# Relational One-Store Roadmap

This roadmap tracks the remaining work to make relational mode use one physical
typed store for the base table and columnar query path, rather than the current
interim shape:

- document-mode tables: JSON KV value plus derived indexes, unchanged;
- relational tables today: authoritative typed-row KV value plus derived
  typed-doc-value sections in search segments;
- relational target: one synchronous typed column store is the relational base
  table, and query/index consumers read from that same committed representation.

The goal is not to change the public relational contract. The change is an
internal storage cutover: keep `storage_mode = "relational"`, closed schemas,
JSON reconstruction, predicate pushdown, joins, algebraic facts, transforms, and
point reads, but remove the remaining physical duplication between the KV typed
row and segment typed columns.

## Why this is plausible

Antfly already has two-phase commit (2PC) and participant-resolution machinery.
That means the hard part is not inventing a new commit protocol from scratch.
The hard part is making relational typed storage a first-class synchronous
transaction participant with the same read-after-commit, recovery, split/merge,
and replay behavior that the current KV document value provides.

Search segments are not enough by themselves because they are derived artifacts
with async/batched construction semantics. A transform or point lookup
immediately after commit must see the just-written row. The one-store design
therefore needs a synchronous relational base-store layer, not simply "read
from existing search segments."

## Target Architecture

For relational tables, the base document participant becomes a typed row/column
participant:

1. Writes project incoming JSON through the runtime schema into typed cells.
2. The typed cells are prepared and committed through the same transaction
   boundary as the rest of the batch.
3. `DB.get`, transforms, lookup, vector `include_stored`, enrichment source
   reads, backfill, search projection, and shard movement reconstruct JSON from
   the committed typed cells.
4. Predicate scans and aggregations read the same committed typed cells, not a
   second copy in derived segment doc-values.
5. Document-mode tables continue to use the existing JSON KV value and derived
   indexes.

The relational base store should retain the properties that made the current
typed-row KV safe:

- self-describing enough for generic readers to avoid live-schema dependence at
  recovery/split seams;
- one atomic row/document unit for point lookups and transforms;
- canonical JSON reconstruction, not byte-exact original JSON;
- vector-field stripping semantics identical to document mode;
- nullable columns omitted from reconstruction when absent;
- `json` columns preserved as embedded subtrees.

## Proposed Storage Shape

Build a `relational_store` layer beside the current document KV path. It should
start as row-addressable and column-readable:

- **Row key:** existing document key / document identity namespace.
- **Row payload:** the current `relational_row_codec` cells, but stored under a
  relational participant API rather than as the generic document KV value.
- **Column access:** maintain column-organized blocks or memtables as part of
  the same participant so scans do not need derived segment doc-values.
- **Commit metadata:** row version / generation, delete marker, and enough
  range ownership metadata to participate in split/merge/replay.

The first implementation can keep a row-packed on-disk encoding internally while
exposing a column scan API. That lets the PR remove the second logical storage
copy first, then optimize physical column layout later. The important boundary
is that derived search segments stop being the authoritative column source for
relational reads.

## Implementation Phases

### Phase 1 - Transaction Participant Contract

Define the relational base-store participant interface:

- `prepareUpsert(table, doc_key, typed_row, txn_id)`
- `prepareDelete(table, doc_key, txn_id)`
- `commit(txn_id, commit_version)`
- `abort(txn_id)`
- `get(doc_key, read_version)`
- `scanColumn(column, bounds, read_version)`
- `scanRows(projection, filter, read_version)`
- replay/recover prepared transactions on open

Wire it into the existing 2PC participant-resolution path instead of treating
relational document storage as an opaque `store_value` write.

Acceptance:

- prepared relational rows survive crash/reopen and resolve correctly;
- abort leaves no visible row;
- commit is visible to `DB.get` and query readers at the expected read version;
- document-mode storage behavior is unchanged.

### Phase 2 - Write Path Cutover

Move relational writes from generic document KV values to the relational
participant:

- keep using `schema_capability.projectRelationalRowAlloc` and
  `relational_row_codec` for projection and canonical reconstruction;
- stop writing a relational `AROW` value as the generic document `store_value`;
- write typed cells through the relational participant in the same batch/2PC
  flow;
- keep original JSON only as transient write input for initial derived index
  projection during the foreground write.

Acceptance:

- relational `DB.get` after commit reconstructs from the relational participant;
- transforms read typed-row materialized JSON, apply changes, and write back a
  new typed row;
- deletes remove the relational row and all query-visible state;
- vector-field stripping still matches document-mode semantics.

### Phase 3 - Read Seams

Move every relational document-value reader to the relational participant:

- `DB.get` / lookup;
- `stageTransform` read-modify-write;
- vector and dense `include_stored`;
- enrichment `source_field` fast path and full-document source reads;
- derived replay collectors and index backfill;
- shard split/merge readers;
- serverless/materialized publication readers if they touch relational rows.

The existing `materializeDocumentValueAlloc` seam should remain for compatibility
while the cutover is staged, but relational code should prefer a typed row from
the participant over a generic KV value.

Acceptance:

- a test matrix proves every reader works with no generic relational KV value;
- no code path silently falls back to a missing JSON blob for relational tables;
- document-mode pass-through behavior remains unchanged.

### Phase 4 - Query Planner and Scan Source

Replace relational predicate/projection scans over derived segment
`typed_doc_values` with scans over the relational base store:

- route relational column filters to `relational_store.scanColumn`;
- route projection/stored-data reconstruction to row/projection reads from the
  same participant;
- keep full-text/inverted indexes only for text search behavior that is not a
  base-table column scan;
- keep algebraic/graph/vector indexes as derived indexes, but make their
  backfill/replay source the relational participant.

Acceptance:

- numeric/date/bool/geo/keyword relational predicates read base-store columns;
- projection and `include_stored` do not read segment stored-doc bodies or
  duplicated segment typed columns;
- query results match the current relational behavior across MVCC/read
  generations.

### Phase 5 - Derived Segment Simplification

Once the base store serves relational predicate scans, remove duplicated
relational reconstruction columns from derived full-text segments:

- keep only sections needed for full-text term search, doc identity, scoring,
  and index-local metadata;
- stop writing relational full-column `typed_doc_values` sections into search
  segments when the relational base store can serve the same scans;
- keep segment manifests only where a derived artifact still needs
  schema-less reconstruction, or remove them if no reader needs them.

Acceptance:

- relational segment size drops for scalar-heavy tables;
- compaction/merge/split no longer carries duplicated relational column bodies;
- text search still returns correct stored data by consulting the base store.

### Phase 6 - Recovery, Split, Merge, and Backfill

Make range movement and recovery treat the relational base store as table data:

- split copies/moves relational rows and column blocks for the owned key range;
- merge combines relational base-store ranges without relying on search segment
  reconstruction;
- replay rebuilds derived indexes from the relational base store;
- repair/backfill can regenerate full-text, algebraic, graph, dense, and sparse
  indexes from the committed typed rows.

Acceptance:

- crash tests cover prepared, committed, aborted, split, and merge states;
- derived indexes can be dropped and rebuilt from relational base rows;
- range ownership fencing prevents stale relational rows from serving reads.

### Phase 7 - Compatibility and Migration

This PR can avoid legacy relational on-disk migration if relational mode has not
shipped, but the code should still handle mixed local state during development:

- detect old relational `AROW` generic KV values and migrate them into the
  relational participant on open or first write;
- reject mixed base-store/old-KV states that cannot be made safe;
- preserve document-mode KV values exactly.

Acceptance:

- clean upgrade path for existing PR test data;
- no accidental interpretation of document-mode JSON blobs as relational rows;
- explicit version/capability marker for one-store relational tables.

## Code Areas

Likely touch points:

- `storage/db/db.zig` - batch write, transform, lookup, transaction plumbing,
  replay/backfill seams.
- `storage/db/document_mapper.zig` - typed row projection/reconstruction,
  materialization seam, source-field fast path.
- `storage/db/algebraic/relational_row_codec.zig` - row codec remains the
  canonical JSON formatter unless the new participant introduces a lower-level
  cell encoding.
- `storage/db/catalog/index_manager.zig` - split/merge/rebuild, derived index
  backfill sources, relational query routing.
- `storage/db/query/search_exec.zig` - relational projection and filter source.
- `section/typed_doc_values.zig` - reusable column block readers/writers if the
  relational participant stores column blocks directly.
- `storage/db/maintenance/transaction_runtime.zig` and
  `transaction_resolution.zig` - participant recovery and resolution.
- `schema/mod.zig`, `schema_capability.zig` - runtime relational column catalog
  and schema evolution constraints.

## Test Matrix

Minimum coverage before merging the one-store implementation:

- write -> `DB.get` -> query stored data for all scalar types plus `json`;
- transform read-modify-write on every scalar class and nullable column;
- delete and overwrite remove old column values from scans;
- exact read-after-commit under the existing transaction/2PC path;
- abort and crash recovery of prepared relational writes;
- replay/backfill derived text/algebraic/graph/vector/sparse indexes from typed
  base rows;
- split and merge preserve relational base rows and column scans;
- document-mode tables continue to store/retrieve JSON blobs unchanged;
- mixed text search plus relational predicate filters;
- serverless publication/query path if relational tables are exposed there.

## Rollback Points

Keep the current typed-row KV path behind a short-lived internal switch until
the one-store tests are green:

1. New relational participant present but unused.
2. Writes dual-populate participant and typed-row KV; reads still use KV.
3. Reads prefer participant with KV fallback.
4. Remove fallback once recovery/split/query tests pass.
5. Stop writing generic relational KV values.
6. Stop writing duplicated relational segment typed columns.

The final state should remove the switch and make the relational participant the
only physical base store for relational tables.
