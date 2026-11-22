# Lite index write path

Status: in progress (branch `perf/lwa-mp-corruption`, PR #1775).

This document governs how every Lite index (full-text, sparse vector, dense
vector, graph, algebraic) takes a write, makes it durable, makes it visible,
and folds it into its long-lived structure, in one process and across
processes. It extends `lite-full-text-storage.md`, which defines per-copy
versions, the version-valued doc map and the dead set; those carry over to
every index type here.

The model is SQLite FTS5's: a write's index changes are computed once by the
writer and stored as a small immutable record in the same transaction as the
row; readers in any process read those records from the file; bounded merge
work folds them into larger structures. Nothing is tokenized, embedded or
decoded twice, and nothing a write stages comes from process memory.

## Why

Measured with `scripts/bench/lite_sqlite_comparison.py` (8 workers, ReleaseFast):
unindexed writes run at 5-6k/s in both thread and process mode; with one
full-text index, threads run at 1.2-1.5k/s and processes at 150-550/s.
SQLite FTS5 on the same workload is far ahead. The causes, from a survey of
every index type's write path:

- **Index locks across the fsync.** Every inline publication (text, sparse,
  dense, graph) holds its index's `apply_mutex` from prepare, across the
  durability wait, through install; at `full_text`/`full_index` the
  cross-process writer lease is held through the fsync and the visibility
  wait too (`mutation_commit.zig` `commits_after_wait`). Indexed writes to one
  index therefore never share a group commit. Dense additionally holds HBC
  `published_snapshot_mu` exclusively across the fsync, blocking searches.
- **Read-modify-write of shared records per write.** Text rewrites a dead-set
  bucket and the segment counter; sparse rewrites each touched term's last
  posting chunk and term metadata; dense rewrites the whole leaf node; graph
  rewrites global counters and a read-generation key; algebraic rewrites
  aggregate rows, whole HLL sketches and an `applied_generation` key. Each is
  serialized, and several are hot keys every writer contends on.
- **A full reopen per foreign commit.** `lite_connection.zig` `refreshImpl`
  reopens the runtime whenever another process committed; every index is then
  rebuilt from the catalog (the native-catalog backend mirrors whole scopes in
  memory: HBC nodes, sparse postings, graph adjacency). In process mode this
  dominates.
- **One segment, and later one merge, per write**, with merge backpressure as
  a timeout that writers can hit.

## Model

### Versions and liveness (every index type)

Every indexed item carries a version (`DocVersion`: replay sequence << 24 |
position), unique to that copy. An item is a text or sparse document copy, a
dense vector, a graph edge, or an algebraic fact. Each index scope keeps:

- `docmap/<item key>`: the key's current version (a delete records a delete
  marker at its version);
- `dead/<version >> 32>`: roaring bitmap of superseded and deleted versions,
  pruned as merges drop the copies.

Every publisher applies one rule at the tip, last writer wins by version
(`persistent.zig` `resolveVersionsAlloc`). Item keys are per type: the
document key (text, sparse), the vector member key (dense), the edge key
(graph), the fact key (algebraic).

### Level-0 records

A transaction's changes to one index are one immutable record:

| Index | Level-0 record | Base structure it folds into |
|---|---|---|
| full-text | small AFSM segment with a version column | AFSM segments (merged by tier) |
| sparse | sparse segment (doc → weighted terms, version column) | chunked postings |
| dense | vector batch: (version, member key, vector) | HBC tree |
| graph | edge batch: (version, edge key, endpoints, properties), both directions | forward and reverse adjacency |
| algebraic | fact batch with signed deltas and HLL register maxima | aggregate rows and sketches |

Key: `l0/<be64 first version>` in the index's native-catalog scope. Value:
the encoded batch. Written once, deleted when folded. No marker, range,
counter or per-index hot key is written per transaction.

### Write protocol (inline, `full_text`/`full_index`)

1. **Prepare (no locks).** Build each index's level-0 record from the batch:
   tokenize, embed (or take authored vectors), extract edges and facts.
2. **Stage.** Stage the level-0 records and one resolver per index into the
   write's transaction. The resolver runs in the group apply
   (`PrimaryCommitContext.apply`), at the tip, under writer authority: it
   applies the version rule to the doc map and dead set, so members of one
   group commit decide in apply order. No index lock is taken.
3. **Commit.** One native commit with the primary rows; group-committed with
   other writers. The writer lease is released before the durability wait at
   every sync level, as it already is for `write`.
4. **Install (after durability, memory only).** Add the level-0 record to the
   index's in-memory snapshot and mark newly dead versions. Install is
   idempotent and order-insensitive: versions, not install order, decide.

`sync_level=write`: the replay worker builds the same level-0 records from
the journal later, many transactions per record, and publishes them with the
same resolver. `full_index` and `write` differ only in who builds the record
and when.

### Reads

A query reads the index's base structure plus its level-0 records, each
filtered by the derived deletion state (dead versions). Dense brute-forces its
level-0 vectors and merges the result with the HBC search; graph unions
level-0 edges with adjacency; algebraic adds level-0 deltas to the folded
aggregates.

### Flushing and merging

One flusher per database at a time (the maintenance turn) folds level-0
records into the base structure and deletes them in the same commit; for text
and sparse this is a merge into a level-1 segment. Work is bounded per turn,
in the manner of FTS5's `automerge`: a turn folds at most a fixed number of
records or bytes, so no writer waits on a merge and there is no backpressure
timeout. The flusher also:

- prunes dead versions whose copies it dropped (they lived only in its
  inputs);
- drops delete markers older than every index's applied cut, when no older
  copy can still arrive;
- folds counters (graph, sparse) and HLL registers (algebraic) that writes no
  longer rewrite.

### Cross-process refresh

Before serving, a process catches its shared redo overlay up with the log
and asks what other writers changed since its runtime last caught up
(`lite_connection.zig` `refreshInPlace`). Every overlay version carries the
appending instance's origin (`RedoCommitState.origin`; 0 for a version
replayed from another process's record), so the runtime's own commits are
excluded (`SharedRedoOverlay.changedKeysAfterAlloc`). It refreshes in place
when every other change is one it reads through the file anyway or can
reload:

- metadata root: artifact leases and writer bookkeeping only;
- documents root: rows, identities, the replay journal and per-write
  metadata (apply progress, document revisions, range counters), under any
  table's `\x02db/<namespace>\x00` prefix. Schema, catalogs, table settings
  and SQL markers are not routine;
- index root: native-catalog records (`\x00ncat/<scope>\x00...`) of loaded
  versioned full-text indexes, each reloaded by `PersistentIndex.refreshFromTip`:
  the mirror is reloaded, segments and level-0 records another process
  published or merged away are added or dropped in one snapshot
  publication, and every segment's deletions are derived from the dead set.

Anything else reopens the runtime, as do a fold or reclaim past the
runtime's sequence (records may not be in the overlay), outcome
uncertainty, live borrows, and an index with inline publications in flight
(their install could re-add a level-0 record the refresh already loaded or
that another process merged away). Other index kinds join as they move to
level-0 records.

## Crash and consistency invariants

- A write's index records share its native commit; an acknowledged write is
  durable with them.
- Everything a write stages comes from its own input or its transaction's
  reads at the tip; process memory never reaches disk.
- Install and refresh are memory-only and idempotent; a crash before install
  is repaired by the next refresh.
- A version is current for at most one key; an item is live only while its
  version is current. Flushes, replays and rebuilds cannot resurrect an old
  version or hide the newest.
- Segment, level-0 and vector ids are never reused across processes.

## Stages

1. Done (`lite-full-text-storage.md` stages 1-5): versions, doc map and dead
   set for full-text; tip-resolved background publishers; merges without
   per-document writes.
2. Inline decisions in the group apply: the Lite `Txn` runs resolvers in
   `PrimaryCommitContext.apply`; no index lock or writer lease across the
   fsync. All inline publications. Done for full-text
   (`mutation_materialization.zig` `InlineTextPublication`): the inline
   segment is a level-0 record, its resolver stages in the write's
   transaction, the index's apply guard is released once staged and
   retaken for the memory-only install, and the writer lease is released
   before the durability wait when no index needs a synchronous publish.
   Replay windows stamp each key with its last record's sequence. Reads no
   longer wait on capacity or reuse proofs (they admit writes only).
3. Level-0 records for every index type, read by queries, folded by a bounded
   flusher; per-transaction hot-key writes removed. Full-text level-0
   records are folded by ordinary merges, which check each input at the
   tip (`l0/` present) rather than in this process's mirror.
4. Incremental cross-process refresh. Done for document rows and full-text
   indexes; other index kinds still reopen.
5. Delete-marker cleanup and the remaining per-type folds.

Each stage keeps the crash sweeps, the multi-process capi tests and the
8-worker SQLite comparison (no stale results, no stalls, no busy failures)
passing.
