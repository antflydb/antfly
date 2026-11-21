# Lite full-text index storage

Status: in progress (branch `perf/lwa-mp-corruption`).

This document governs how a Lite full-text index keeps its durable state and how
a write changes it. It replaces the model where an index's in-memory writer state
is the truth and its durable form is rewritten from that memory. The target model
is the one SQLite FTS5 uses: the index is ordinary keyed records in the database
file, read and written inside the writer's own transaction. In-memory structures
are caches only. The other index types follow the same pattern later (see
"Other index types").

## Why

Two problems share one cause.

**Correctness.** With several processes writing at `sync_level=full_index`, a
full-text index can keep an overwritten document's old token searchable. The
SQLite comparison benchmark (`scripts/bench/lite_sqlite_comparison.py`, 8 worker
processes, indexed) hits it in most runs under ReleaseFast. The inline write path
(`mutation_materialization.zig` `prepareInlineTextPublication`) finds the old
version by scanning this process's in-memory segments, sets the tombstone in
memory, and persists the segment's whole deletion bitmap
(`persistent.zig` `PreparedDeleteByIds.mutationsAlloc`). Segment ids come from an
in-memory counter. Nothing re-checks that memory against the file at commit: any
window where a process's serving root is stale becomes a lost delete, a dropped
tombstone from another process, or a reused segment id. Each such window has
been patched separately in the past; the class remains.

**Performance.** The same design makes every overwrite O(documents in the index)
(`index.zig` `deleteAllByIdsTracked` decodes every stored id), rewrites whole
bitmaps per write, holds every segment twice in memory (catalog mirror and
writer), and forces a full index reload whenever another process commits to the
index. It also keeps the per-index guard and the writer lease across the fsync,
so indexed writes never form groups.

## Durable layout

All records live in the index's native-catalog scope inside the `.aflite` file,
in the same durability domain as the primary rows. Segment ids are big-endian u64.

| Record | Key | Value | Mutable |
|---|---|---|---|
| Segment | `segments/<seg>` | AFSM segment bytes | no |
| Segment range | `meta/segment_range:<seg>` | min/max doc key | no |
| Active marker | `meta/active_segment:<seg>` | `{1}` | added/removed only |
| Tombstone container | `tomb/<seg><high16>` | one roaring container | yes |
| Doc map | `docmap/<doc key>` | `<seg><local id u32>` | yes |
| Segment counter | `meta/next_seg_id` | u64 | yes |
| Format | `meta/text_storage_format` | u32 | at upgrade |

Rules:

- A segment and its range are written once, when the segment id is allocated,
  and deleted only when the segment is retired. Caches keyed by segment id never
  need invalidating.
- Tombstones are stored per roaring container (`high16` = local id >> 16). A
  write that deletes one document rewrites one small container, never the whole
  bitmap.
- The doc map names each live document's current `(segment, local id)`. It is the
  only way a write finds an old version; nothing scans segments.
- `meta/next_seg_id` is read and advanced inside the transaction that publishes
  the segment, so two processes can never reuse an id.
- The doc map is kept out of the catalog mirror (it is read only through
  transactions), so the mirror does not hold every document key.

## Reading at the transaction tip

A write must read the doc map, tombstone containers and the counter as of its
own transaction: the committed state at its checkpoint plus anything it staged
earlier. Lite's write transaction today can stage index-catalog records
(`Txn.putIndexCatalogBatch`) but cannot read them back. Add:

- `lite/docstore.zig` `Txn.getIndexCatalog(scope, namespace, key)`: the staged
  `index_catalog_pending` entries (latest wins, deletes hide) over the committed
  index-catalog root at the transaction's checkpoint
  (`NativeFile.getIndexCatalogRecordAtCheckpointAlloc`).
- `commit_participant.View.getIndexCatalog`, bridged like `putIndexCatalog`, so
  an index's commit participant reads at the tip.

Writer authority (the cross-process writer lease plus the in-process writer slot)
guarantees the checkpoint is the file's tip when the transaction opens. Members of
one group commit must also see each other's staged index records; until the
prepare/stage/install split lands, the per-index apply guard serializes writes to
one index, which preserves that.

## Write protocol

For a write that changes documents in a full-text index:

1. **Prepare (no locks).** Tokenize the new versions and build the level-0
   segment bytes for this batch.
2. **Stage (inside the write transaction, at the tip).**
   - For each changed or deleted document key: read `docmap/<key>`. If present,
     read `tomb/<seg><high16>`, set the bit, and stage the container.
   - Read `meta/next_seg_id`, take the next id, stage the counter.
   - Stage the segment, its range and its active marker.
   - For each new version, stage `docmap/<key>` = (new segment, local id). For a
     deleted document, stage a delete of `docmap/<key>`.
   - Stage the index's progress record.
   Every value staged here comes from the transaction's own reads or from this
   batch's input. Nothing comes from process memory.
3. **Commit.** The index records ride in the same native commit as the primary
   rows: one redo record, one fsync.
4. **Install (after durability, memory only).** Update the in-memory caches in
   commit order: add the segment to the writer snapshot, apply the tombstone bits.
   Queries see a delete only from here on, never before it is durable. A failed
   install drops the cache; it is rebuilt lazily from the durable records, never
   treated as authoritative.

## Caches and cross-process refresh

The in-memory writer snapshot (segment list, per-segment deletion bitmaps) is a
cache of the durable records. After another process commits to the index, a
process refreshes it incrementally: re-read the active markers and the tombstone
containers of known segments, and load new segments by id. It never reloads the
whole scope and never needs a full handle reopen for this. A per-scope version
published in the shared lock region tells a process cheaply that an index moved
(planned with the shared-memory refresh work).

## Merges

A merge reads its input segments, builds output segments, and publishes in its own
transaction at the tip:

- Validate that every input's active marker is still present.
- Read every input's tombstone containers at the tip; a document deleted since
  the merge began is dropped from the output's doc map and tombstoned in the
  output, using the merge's source map.
- Rewrite `docmap/<key>` for every live document to its output location, and
  only if the doc map still names the input location (a newer version wins).
- Allocate output ids from `meta/next_seg_id`; retire inputs (segments, ranges,
  markers, tombstone containers).

There are no in-memory deletion deltas and no "deleted count changed, discard the
merge" check: a concurrent delete is carried forward from the durable records.

## Upgrade

Indexes written before this format have whole-bitmap `deletions/<seg>` records and
no doc map. On first open by a writer, a one-time upgrade in one transaction:
splits each bitmap into containers, builds the doc map from the active segments'
stored ids (skipping tombstoned ones; the newest segment wins for a repeated key),
writes `meta/next_seg_id` as the maximum of the stored value and the highest
active id + 1, and stamps `meta/text_storage_format`. A reader opening an
un-upgraded index still uses the old records.

## Crash and consistency invariants

- An acknowledged write is durable together with every index record it changed:
  they share one native commit.
- Every value a write stages comes from that transaction's reads at the tip or
  from its own input. Process memory never reaches disk.
- Install is memory-only and idempotent; a crash before install is repaired by
  the lazy reload.
- Merges publish against the tip and never drop a concurrent delete.
- Segment ids are never reused, across processes and across crashes.

## Other index types

The same principle applies to the other native-catalog indexes, in later stages:
sparse vectors (level-0 sparse segments, doc map and tombstones, background
merges into chunked postings), dense vectors (a per-index delta of appended
vectors merged into the HBC tree in the background), graph (drop the
authoritative mirror) and algebraic (HLL sketches split into register blocks).

## Implementation stages

1. Read surface: `Txn.getIndexCatalog` and `View.getIndexCatalog`, with tests
   for staged-over-committed reads and deletes.
2. Durable records and the transactional delete: doc map, tombstone containers,
   counter key; the inline path stages from transaction reads; install applies
   tombstones after durability. Upgrade of existing indexes.
3. Worker (derived replay) path on the same records.
4. Merges validate and carry deletes at the tip; remove the in-memory deletion
   deltas.
5. Incremental cross-process refresh; remove the full reopen after a foreign
   index commit.
6. Prepare/stage/install split; no index guard or writer lease across the fsync.

Each stage keeps the full-text crash sweeps, the multi-process test
`capi native separate writer processes keep full-text deletes at full_index`, and
the indexed 8-worker SQLite comparison (zero lost deletes, zero stalls) passing.
