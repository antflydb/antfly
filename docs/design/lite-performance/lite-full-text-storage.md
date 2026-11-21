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

## Versions

Every indexed copy of a document carries a **version**, a u64 unique to that
copy:

    version = (replay sequence of the write) << 20 | (copy's index in its batch)

Replay sequences are unique per batch and increase in commit order across
processes (the journal refuses an append below its committed cut), so
versions order copies of one key by when they were written. A version never
changes: a merge copies it into its output unchanged.

Liveness is a property of the version, not of where the copy sits. A copy is
live exactly when its version is the current version of its key and that
version is not dead. Nothing about a document's liveness is stored per
segment.

The engine's `DocOrdinal` (doc_identity.zig) is not used here. An ordinal
names a document, not a version: an overwrite keeps it. Its state row's
`created_generation` drives snapshot visibility, so it cannot record each
overwrite, and a new document's ordinal is not readable while its batch is
still being prepared.

## Durable layout

All records live in the index's native-catalog scope inside the `.aflite` file,
in the same durability domain as the primary rows. Segment ids are big-endian u64.

| Record | Key | Value | Mutable |
|---|---|---|---|
| Segment | `segments/<seg>` | AFSM segment bytes, with a version column | no |
| Segment range | `meta/segment_range:<seg>` | min/max doc key | no |
| Active marker | `meta/active_segment:<seg>` | `{1}` | added/removed only |
| Current version | `docmap/<doc key>` | version u64, high bit set for a delete | yes |
| Dead versions | `dead/<version >> 32>` | roaring bitmap of `version & 0xffffffff` | yes |
| Segment counter | `meta/next_seg_id` | u64 | yes |
| Format | `meta/text_storage_format` | u32 = 2 | at upgrade |

Rules:

- A segment and its range are written once, when the segment id is allocated,
  and deleted only when the segment is retired. Caches keyed by segment id never
  need invalidating.
- `docmap/<key>` is the key's current version (last writer wins by version).
  A delete leaves a marker holding the delete's version, so a copy that
  arrives later from an older write is born dead.
- `dead/` holds every version that was current and has been superseded or
  deleted, until a merge drops the last copy of it.
- A merge writes neither: versions do not move, so the doc map and the dead
  set are independent of segment layout.
- Records are read and rewritten at the tip, so a stale process cannot drop
  another's changes. `meta/next_seg_id` is read and advanced in the
  publishing transaction, so two processes never reuse an id.
- The doc map is kept out of the catalog mirror (`native_catalog_backend`
  `tip_namespace`); the dead set is small and mirrored.

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

Every publisher (inline write, replay worker, rebuild page, split handoff)
applies one rule per document copy it publishes, at the tip:

1. Read `docmap/<key>`: the current version `cur` (or none).
2. If the new copy's version `v` is newer than `cur`: add `cur` to the dead
   set (unless it is a delete marker) and set `docmap/<key> = v`.
3. Otherwise the copy is stale: add `v` to the dead set (born dead).

A delete at version `d` does the same with a delete marker as `v`. Equal
replay sequences are copies from one batch; the later batch index wins.

For the inline path, at `sync_level=full_index`:

1. **Prepare (no locks).** Tokenize the new versions and build the level-0
   segment bytes with their version column.
2. **Stage (inside the write transaction, at the tip).** Apply the rule above
   for every changed or deleted key; take the next segment id from
   `meta/next_seg_id`; stage the segment, its range and its active marker.
   Every value staged comes from the transaction's own reads or this batch's
   input; nothing comes from process memory.
3. **Commit.** The index records ride in the same native commit as the primary
   rows: one redo record, one fsync.
4. **Install (after durability, memory only).** Add the segment to the writer
   snapshot and mark the newly dead versions in the in-memory deletion bitmaps.
   Queries see a delete only from here on, never before it is durable.

Background publishers stage the same decisions through a resolver (below),
which runs inside their commit at the tip.

## Caches and cross-process refresh

The writer snapshot's per-segment deletion bitmaps are derived: a local doc is
deleted when its version is in the dead set. A process builds them when it
loads a segment, from the segment's version column and the dead set (a few
records), and keeps a per-segment sorted version index so that installing a
commit can find the copy a dead version names. After another process commits
to the index, a process reloads the dead set and loads new segments by id; it
never reloads segments it already holds. A per-scope version published in the
shared lock region tells a process cheaply that an index moved (planned with
the shared-memory refresh work).

## Merges

A merge builds its output segments from its inputs off the write path, dropping
copies whose versions are dead in its snapshot and carrying every other
version through unchanged. Its commit stages a resolver
(`native_catalog_backend.Resolver`) as its first mutation; the Lite sink runs
it while applying the commit, at the tip, under writer authority. The resolver
refuses the merge (`error.IndexScopeChanged`) when an input is no longer
active, because another process merged it away. Otherwise it removes the
versions the merge dropped from the dead set (no other segment can hold them:
versions are unique to a copy) and raises the segment counter past the
outputs. It writes nothing per live document.

A copy that became dead after the merge read its inputs is still dead in the
output: its version is in the dead set, so the output's derived bitmap marks
it. Writes never invalidate a merge.

Merge output ids are reserved at the tip, in a small commit of their own,
before the merge publishes (`reserveSegmentIdRange`). Every other background
counter write raises the counter at the tip and never lowers it
(`SegmentCounterFloor`).

## Upgrade

An index whose `meta/text_storage_format` is not 2 (no version columns, or the
location-valued doc map of format 1) is rebuilt from the primary rows on its
first writable open. The format has not been released, so there is no in-place
migration.

## Crash and consistency invariants

- An acknowledged write is durable together with every index record it changed:
  they share one native commit.
- Every value a write stages comes from that transaction's reads at the tip or
  from its own input. Process memory never reaches disk.
- Install is memory-only and idempotent; a crash before install is repaired by
  the reload.
- A version is current for at most one key, and a copy is live only while its
  version is current: merges, replays and rebuilds cannot resurrect an old
  version or hide the newest.
- Segment ids are never reused, across processes and across crashes.

## Other index types

The same principle applies to the other native-catalog indexes, in later stages:
sparse vectors (level-0 sparse segments, doc map and tombstones, background
merges into chunked postings), dense vectors (a per-index delta of appended
vectors merged into the HBC tree in the background), graph (drop the
authoritative mirror) and algebraic (HLL sketches split into register blocks).

## Implementation stages

1. Done. Read surface: `Txn.getIndexCatalogAlloc` and
   `View.getIndexCatalogAlloc`.
2. Done (format 1, superseded). Location-valued doc map, tombstones at the
   tip, tip-resolved merges and segment ids.
3. Segments carry a version column; merges carry it through.
4. Format 2 write protocol: version-valued doc map and dead set, for every
   publisher; derived deletion bitmaps on load and install.
5. Merges drop dead versions and prune them from the dead set; no per-document
   merge writes. Rebuild on open for older formats.
6. Incremental cross-process refresh; remove the full reopen after a foreign
   index commit.
7. Prepare/stage/install split; no index guard or writer lease across the fsync.

Each stage keeps the full-text crash sweeps, the multi-process test
`capi native separate writer processes keep full-text deletes at full_index`, and
the indexed 8-worker SQLite comparison (zero lost deletes, zero stalls) passing.
