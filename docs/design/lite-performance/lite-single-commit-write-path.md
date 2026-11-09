# Lite single-commit write path

Status: in progress on `perf/lite-write-amplification` (base `origin/main` 6c0b004e95).

Goal: close the small-write gap with SQLite + FTS5. Measured on a related branch with 1 KB documents, one document per transaction:

| Workload | Antfly Lite (1 / 8 writers) | SQLite (1 / 8 writers) |
|---|---|---|
| Primary only | 743 / 473 writes/s | 17,630 / 17,255 writes/s |
| Full-text visible before ack | 102 / 43 writes/s | 11,349 / 10,522 writes/s |

Per update on the calling thread:

| Measure | Primary only | Indexed |
|---|---|---|
| Syncs | 3.0 (SQLite about 1) | 36.4 |
| Bytes submitted to write | about 60 KB (SQLite 4 KB) | about 393 KB (SQLite 27 KB) |

Paths below are relative to `zig/pkg/antfly-embedded/src/`. Line numbers are as of 6c0b004e95.

## 1. Why a write costs so much today

**Native commit: 3 fsyncs.** In `storage/lite/native.zig`, `commitTransactionWithDurability` (11692) syncs three times:

1. The data pages (11730).
2. The inactive recovery slot (11797).
3. The 1-byte `active_checkpoint` hint (11800).

Sync 3 is redundant. `selectActiveCheckpoint` (12785) picks the valid slot with the highest `commit_sequence`, and `headerChecksum` excludes the hint byte. Sync 1 exists only because a slot cannot authenticate the pages it references: pages have no commit identity and there is no manifest. Both 72-byte slots also share header sector 0. `std.Io.File.sync` is a plain `fsync`.

**Primary-only acknowledgement** (`capi/db.zig` → `mutation_commit.batchInternalPreparedOwned` → `docstore.putBatchWithReplayAndParticipant` → lite `Txn.commit` → `submitScopedMutation` → `applyMutationGroup` → native commit):

- The writer slot (lite/docstore.zig 2945-2990), the DB apply lock (mutation_commit.zig 953 to 3107/3167) and the per-call cross-process path lease are all held across the fsync. The existing 64-request group commit in `submitScopedMutation` therefore never forms, and throughput falls as writers are added.
- Every commit wakes `Store.maintenanceLoop` (2856-2861, 2973) and `Owner.notifyMutation` (capi/lite_connection.zig:1310). The owner turn then takes the path lease, refreshes, loads the catalog and checks replay, even when nothing consumes the write.
- `writeReplayEntries` (lite/docstore.zig:4618) writes the all-lane plus every hinted lane for every write.

**Indexed acknowledgement: about 12 native commits.** `lite/backend.zig` `configureDbOpenOptions` (267-311) runs every text index as an LSM, with its own WAL, on a virtual filesystem inside the native file. Each virtual-filesystem call in `lite/index_storage.zig` (306/311/325/333/491/497/735-750) is its own durable native commit. Per update:

| Concern | Site | Syncs |
|---|---|---|
| Primary | above | 3 |
| Segment install | `PersistentIndex.indexSegmentFromSinkBuilder` 2249 → `finishSource` | 3 |
| Artifact lease | `artifact_source.Registry.open` 304 | 3 |
| Text main metadata | `MainTxn.commit` 1226 → LSM WAL append | 3-6 |
| Projection progress | db.zig 61670 / mutation_commit.zig 5057; `checkpointLsmWalForManagedIndex` 9597; `publishFullTextReplaySeal` 9723; apply_state | 6-9 |
| Status snapshot | `saveIndexStatusSnapshots` db.zig:37231 | 3 |
| Journal retirement | `truncateReplayLane` (lite/docstore.zig:4727), one commit per lane | 6-15 |
| Segment cleanup | `deleteSegmentFile` / retirement markers | about 0.4 |

The root cause is nested durability domains plus one commit per concern. Copy-on-write is not the cause.

## 2. Pager/WAL decision: fix the native copy-on-write protocol

Existing implementations:

| Implementation | Location |
|---|---|
| Generic logical WAL with group commit | `storage/wal.zig` |
| LSM physical WAL | `storage/lsm_backend/wal.zig` |
| Raft replica WAL | `antfly/src/raft/storage/wal_replica_state.zig` |
| Kernel WAL owner | `antfly/src/storage/kernel_wal_owner.zig` |
| Serverless object-store WAL | `antfly/src/serverless/wal/*` |
| Hot standby replay | `antfly/src/storage/hot_standby/*` |

All of them are logical or LSM record logs, in a directory or an object store. None is a pager. Adopting one would:

- break the single-file `.aflite` format;
- add a second durability domain and a checkpointer, which is the same layering mistake as today;
- force a rewrite of what Lite's immutable-page model currently provides: cross-process reader pinning, artifact leases, allocator_v4 proofs, vacuum and backups.

The costs above are protocol and layering choices. A self-validating single-fsync copy-on-write commit costs one sync per group, the same as a WAL, and needs no checkpointer.

What to reuse from `storage/wal.zig` is the coordinator design: leader drains the queue, an adaptive window only while an fsync is in flight, per-request results, and its sim-fixture crash tests.

Rejected: an in-file redo log with lazy checkpoints. Every reader process would need a log-tail overlay. Revisit only if the bytes gate below is missed.

## 3. Target architecture

### 3.1 Native commit protocol v8 (format bump 7 → 8; v7 files are refused)

**Slots:**

- Two dedicated 4 KiB slot pages, so tearing one slot can't damage the other.
- No hint byte.

**Pages:**

- Each page header carries a `commit_sequence` covered by the page CRC.

**Manifest:**

- Each slot gains a commit manifest: `manifest_page`, `manifest_crc`, `manifest_page_count` and `presynced`.
- The manifest lists every page the commit wrote, for commits up to `max_single_sync_pages` (default 256).
- Larger commits use data fsync followed by slot fsync, and set `presynced`.

**Small commits:**

1. Write the pages, the manifest and the slot.
2. One `syncData`.
3. Publish.

**Recovery:**

- Pick the highest valid slot.
- If it isn't `presynced`, check every manifest page's `commit_sequence` and CRC.
- On failure, fall back to the other slot and truncate the file to its `page_count`.

**Allocator reuse:**

- A page may be reused only if it is free in the last durable checkpoint and its retirement epoch is ≤ the durable epoch.
- This makes it safe to stage the next group while the current group's fsync runs.

**`syncData` primitive** (in antfly_platform):

- Linux: `fdatasync`.
- macOS: `fsync`, with an optional `F_FULLFSYNC` durability policy.
- The file grows in preallocated extents.

### 3.2 Group commit with durability tickets

**API:** `Store.stage(plan) !Ticket`, `Ticket.wait()`, `Store.durableSequence()`.

**Groups:**

- At most two groups are in flight: group G is in `syncData` while G+1 accumulates in memory.
- If no fsync is in flight, the group closes at once, so a single writer pays no extra latency.
- A per-member savepoint lets one failing member roll back alone.
- If the group fsync fails, every member gets `OutcomeUnknown` and the store enters reconciliation.

**Visibility:**

- Reader publication happens only at the durable checkpoint: `notifyPublication`, `publishReadCheckpointAssumeLocked` and `ReaderCoordinator.published`.

**Locks and leases:**

- The writer slot, the DB apply lock and the capi connection gate are released before `Ticket.wait`.
- Threads in the same process join an in-process writer lane that holds the cross-process path lease. The lane hands the lease off only once its members are durable, or after a fairness quantum.

**Backends:**

- The hosted LSM backend implements `commitStaged` as an already-durable ticket.

### 3.2a Durability budget: one fsync per group, background rides along

Lite has a single durability domain: the native file. Primary rows, journal records, index publications, consumer progress, the credit ledger and durable obligations all change through native transactions. Nothing keeps its own WAL.

**Who pays for an fsync.** A foreground group pays exactly one `syncData` for all of its members. The only other durability barriers are:

- close;
- cross-process path-lease handoff, so the next owner sees a durable tip;
- page reuse that needs the durable epoch to advance;
- an idle-flush timer or undurable-bytes threshold, so ride-along work becomes durable when no foreground writer comes.

**Ride-along members.** Background work joins the group-commit queue as ride-along members: `Store.stageRideAlong(plan)`, committed with `durability = false`. It never closes a group and never forces a sync. The next foreground group's fsync makes it durable, because the next durable copy-on-write checkpoint includes every earlier transaction. This covers retirement, reclamation, merges, catch-up progress and status.

Each kind of ride-along work is safe to lose in a crash because it can be recomputed:

| Work | Why losing it is safe |
|---|---|
| Retirement | Redone from the durable progress watermark. Its ledger release is in the same transaction. |
| Reclamation | A freed page is reusable only at the durable epoch. |
| Merge | Redone. Readers see the inputs until the merged output is durable. |
| Catch-up progress | Replay restarts from durable progress, and the work is idempotent. |
| Status | Derived from live state. |
| Enrichment checkpoint | The media-pass cursor/page records are private worker preparation, never chunk artifacts; a restart re-derives a bounded amount of recompute from the last durable cursor instead of inferring coverage from a partial artifact set. |
| Recovery-agenda acknowledgement | An un-acknowledged marker just gets re-scanned and re-acknowledged on the next recovery turn. |

**Credits.** A credit release takes effect for producers only once `durableSequence()` covers the transaction that released it. So credits still return only after deletion is durable, with no dedicated fsync.

**Journal.** A consumer whose publication rides in the acknowledgement's commit (a synchronous `full_text` write) needs no journal record for that write.

**Implemented.** The journal/replay payload's physical write (`replay_append`) is still built early in `mutation_commit.zig` (`replay_payload`, used unmodified by the replication-mirror and transaction-replication outboxes, which need every hint regardless of this node's own inline-publication state), but the suppression decision runs right after every `InlineTextPublication`/`InlineGraphPublication`/`InlineSparsePublication`/`InlineDensePublication`/`InlineAlgebraicPublication` has resolved which indexes it actually serviced -- never pre-guessed from the gate conditions alone, since a publication that failed to prepare or lost its `commit_participants` slot still needs its hint kept. A kind's hint is dropped only when every target of that kind in the write was serviced inline (text/graph/sparse/dense's own blocks already strip their handled name(s) out of `sync_targets.all_indexes`/`full_text_indexes` on success, so "nothing of that kind remains" is the test; algebraic's block does not strip, so its remaining count subtracts the handled count back out). A mixed batch (two indexes of the same kind, only one inline-handled) keeps the hint for the other. Only the local journal write is ever repointed at the filtered re-encoding; `replay_payload` itself, and so replication, is untouched. Per-hint lanes already skip writing when no consumer is registered for that hint at all (`active_consumer_hint_mask`, §3.6); the "all" lane does not.

**No decode-then-re-encode.** The first cut of this decoded `replay_payload` (the bytes `encodeRecord` had just produced) only to filter `target_hints` and re-encode -- real duplicate allocation/parsing work on a path meant to save bytes. The in-memory `Record` struct `prepareDirectChangeRecord` builds is instead kept alive (an outer `maybe_record`, owned by a function-level `defer deinitRecord`) past its original block scope, and the suppression block copies it, swaps `target_hints` for the filtered set, and calls `encodeRecord` once -- no decode. The one case this cannot do safely is the `transferred_group_changed`/`deleted` merge (a resolver-review/transfer feature, independent of sync level), which already re-encodes `replay_payload` from its own decoded copy; `record_matches_replay_payload` tracks that and the suppression block falls back to decode/filter/re-encode only there, correctness over cleverness for a path inline publication cannot reach anyway (it requires `materialized_derived_batch`, which the `transferred_group`/thin-replay/group-source-command paths never populate). Measured via `zig build lite-write-amplification-cpu`'s `full-text` mode (the one CPU-bench mode that reaches inline publication, and so suppression, on every write): 1382.84 allocations/write, 6637.5µs/write CPU, against `primary-only`'s 885.41 allocations/write baseline (no indexing, no suppression at all) -- the ~497 extra allocations/write are full-text's whole inline-publication machinery (segment building, catalog mutations, tokenization, projection tracking), not isolable to just the one extra `encodeRecord` call suppression adds on top of it; no pre-suppression baseline exists for this (brand new) CPU-bench tool to A/B against directly. Reasoned, not measured in isolation: one `encodeRecord` call over already-live, already-small (one document's worth) string slices, with no decode and no re-parsing, costs at most a handful of allocations -- noise against ~1383 allocations/write already spent on everything else full-text's inline path does.

**Crash invariant.** Ride-along work may be lost at a crash. Acknowledged work may not, and credits may not leak.

**Prefix-ordered durability.** A durable copy-on-write checkpoint makes every earlier transaction durable, in apply order. After recovery, the set of applied transactions is always a prefix of apply order. This is what makes ride-along safe: retirement applied after the progress it depends on is lost or kept together with that progress.

**Group membership, not standalone commits.** On Lite, `BatchOptions.durability = .ride_along` maps to `Store.stageRideAlong`. The plan joins the next foreground group's native transaction, so one copy-on-write publication (one path copy, one allocator-ledger write, one slot write) covers the whole group. Standalone non-durable commits would still pay those bytes on every commit. Only the idle flush commits ride-along work without a foreground group.

**Manifest coverage.** A durable commit's manifest covers every page written since the last durable checkpoint, not just the current transaction's pages. Otherwise a torn page from an earlier non-durable transaction would pass verification.

**Hosted follow-up (not in this change).** The hosted LSM backend treats `.ride_along` as durable. The equivalent there is a WAL append that doesn't wait for a sync and becomes durable with the next group sync in `storage/wal.zig`.

### 3.3 One physical commit for a small indexed write

**`CommitPlan`** is a typed list of native operations in one native transaction:

- documents;
- the metadata catalog;
- index-catalog records;
- the journal append;
- consumer progress;
- credit-ledger deltas.

`commit_participant.zig` is generalized into typed plan contributors.

**Text index storage on Lite:**

- A `.native_catalog` main backend replaces the LSM-on-virtual-filesystem.
- `PersistentIndex.MainTxn` becomes a view onto the plan's index-catalog root.

**Small indexed write:**

1. Tokenize and build the posting delta while preparing, outside locks.
2. The group leader builds one segment per group.
3. The plan carries:
   - the inline segment record;
   - `saveSegmentRange` and `updateActiveSegments`;
   - the projection seal and the consumer progress record;
   - the primary rows and the journal record.
4. One `syncData`.
5. Install the in-memory snapshot.
6. Acknowledge.

**Artifacts:**

- Small segments need no artifact lease.
- Artifacts larger than `artifact_inline_limit` (about 256 KiB) keep bounded unlinked staging and are published by `putIndexCatalogRecordFromFile` inside the group transaction.
- A lease is created only when a cross-process mmap is actually requested.

**Activation and catch-up:**

- Index activation is a plan contributor.
- The derived executor skips catch-up for sequences that already have progress.
- Non-owner writes and writes with a sync level below `full_text` keep journal catch-up.

### 3.3a Vector and graph indexes

Dense vectors (HBC), sparse vectors and graph indexes follow the same design as full-text:

- **Storage.** Their Lite storage moves off LSM-on-virtual-filesystem onto native-catalog records (`lite/backend.zig` `dense_storage_backend`, `sparse_backend`, `graph_reverse_backend`). A publication is one native commit. Small artifacts are stored inline, large ones use bounded staging inside the same transaction, and there is no artifact lease unless a cross-process mmap is requested.
- **Background work.** Maintenance (HBC rebuilds, compaction, graph metrics, reverse-index upkeep, catch-up progress) rides along and never pays its own fsync.
- **Inline publication.** When a write supplies its own vectors or edges and its sync level covers the index, the publication rides in the primary write's transaction through the same contributor list as full-text.
- **Generated embeddings.** Writes whose vectors must be generated stay asynchronous through enrichment. Inference never runs inside a commit.

### 3.3b Algebraic indexes

Algebraic indexes (`storage/db/algebraic/`) are built differently. They write facts straight into the primary DocStore rather than into a nested LSM, so the fix is the commit pattern, not a backend swap:

- **Synchronous apply.** An algebraic apply for a synchronous write becomes a `commit_participant` contributor (`InlineAlgebraicPublication`, `mutation_materialization.zig`). Its facts and progress land in the primary write's own native transaction via `Index.applyBatchWithOptionsView` against the host's `commit_participant.View`.
- **Background work.** HLL maintenance, adaptive backfill and dematerialization, and join-fact upkeep become demand-driven ride-along commits.

**Measured.** `zig build lite-write-amplification`'s `algebraic`/`algebraic_8_writers` modes (one document per transaction, acked at `full_index`, against a representative group-by + HLL-cardinality config) reach 1.0 durable commits/write and 0 background syncs/update, for both 1 and 8 writers. Two fixes were needed beyond the inline-publication wiring itself:

- `InlineAlgebraicPublication`'s gate required `!executor.hasWorkers()`, copied from full-text's own gate before its "Phase 2 round 2" fix removed the same mistake. The default configuration registers a derived worker for every managed index, so this gate blocked inline publication on every write (3.0 commits/write: the slow path's own standalone commit). `Index.applyBatchWithOptionsView` and the worker's own `applyBatchWritesLocked` already serialize on the same `Index.lockWrites()` mutex, so the extra gate was unneeded; dropped, matching text/graph/sparse/dense.
- With the gate gone, the facts landed inline but the background worker's `applied_sequence` bookkeeping was never advanced to match -- only text/graph/sparse/dense called `Executor.advanceAppliedSequenceIfContiguous` after `install()`. Every later write's notify cycle found the algebraic worker still behind target and ran a redundant catch-up turn that persisted its own applied-sequence advancement as a second durable commit (2.0, not yet 1.0). Algebraic's `install()` loop now calls `advanceAppliedSequenceIfContiguous` too.

The 14 adaptive/join/bulk-finish commit sites that still opened their own `beginWriteTxn` (bulk-ingest session finish, materialized-expression-delta rebuild, and the whole adaptive observation/candidate/progress/backfill/dematerialization family) are now `beginWriteBatchWithOptions(.{ .durability = .ride_along })`, tagged `.algebraic_adaptive` for the harness. `applyBatchWritesLocked`'s own two foreground commits -- the actual durability boundary when a write cannot be inline-published -- are unchanged.

**Measured, not yet at target: the overwrite scenario.** `measureSingleWriterAlgebraicOverwrite` (a small 8-key cycle, so most writes overwrite an existing key's group-by value, dirtying the HLL sketch on nearly every write) measures this path end to end: ~1.9 durable commits/write and ~0.94 background syncs/update for a single writer, ~1.5/~0.50 for 8 concurrent writers (`measureConcurrentAlgebraicOverwrite`) -- neither reaches 1.0/0.

**Root-caused, and it is not the byte threshold.** A `Store.test_ride_along_hook` logs every `stageRideAlong` call's own `estimated_bytes` and the backlog (`ride_along_pending_bytes`) already pending before it. In both modes: every call measures ~16.9KB (one HLL register array at the default precision=14, `2^14` one-byte registers -- not oversized, and `max_hll_maintenance_rows_per_tick` capped at 8 made no difference, ruling out "the rebuild scans too many rows" as the earlier working theory had it), `pending_before` is 0 every single time, and zero calls ever approach the 1 MiB threshold on their own. `stageRideAlong`'s own byte-threshold self-drain check structurally cannot be what is firing.

The actual mechanism is `rideAlongFlushLoop`'s **idle-flush timer** (`ride_along_idle_ns`, default 50ms): it force-drains whatever is pending once that long has elapsed since the last ride-along enqueue. This harness's own writes run ~250-300ms apart in a Debug build (allocator-tracing overhead, not the commit path itself), so the 50ms timer wins the race against the next foreground write arriving to drain the HLL job for free on nearly every dirtying write -- exactly the documented safety-net behavior above ("so ride-along work becomes durable when no foreground writer comes"), just triggered far more often here than realistic traffic would, because this harness's writes are so far apart relative to that window. 8 concurrent writers help (1.5 vs 1.9) but the harness's own `Barrier` still synchronizes every writer to step together each round, leaving round-to-round gaps rather than continuously staggered traffic, so it does not reach 0 either.

**Not fixed at the source, deliberately.** `ride_along_idle_ns` is one constant shared by every ride-along user in `docstore.zig` -- graph, relational jobs, resolution, enrichment, algebraic, not just HLL maintenance -- so tuning it is a cross-cutting durability-latency decision, not a local fix to this one path. Reported as a diagnostic (`write_amplification_bench_test.zig`'s own print at both overwrite modes has the numbers), not a hard gate, pending that decision.

**Bytes by record kind.** A second hook (`test_commit_key_hook`) classifies every committed key by prefix. Plain `algebraic` mode (insert-only): 50.0 keys/write, of which `algebraic_fact_bytes` (`\x00\x00__algebraic__:` keys) is 19,159 bytes/write against only 1,522 bytes/write of primary document content -- the algebraic facts, not the document itself, dominate the *logical* payload. This logical total (~21.3KB/write) is far below the ~147KB/write the I/O counters measure for this mode, confirming most of that gap is native-file page-level copy-on-write overhead (whole-page rewrites, manifest/slot/allocator-ledger bytes), not any single oversized logical record -- a different kind of amplification than "which record kind," unaddressed by this change.

### 3.3c Relational, transactional and auxiliary write surfaces

The rule applies to every durable effect of a write, not only to search indexes. Each of the following lands in the primary write's transaction, either folded into its batch or added as a `commit_participant` contributor:

- relational secondary-index entries;
- unique-constraint and foreign-key records;
- relational columns;
- SQL DML;
- multi-document transaction resolution;
- TTL schedules;
- durable outbox, replication and CDC rows;
- child-range records;
- recovery-agenda obligations.

Validation runs under the apply lock, so there is never a window in which a child row is durable while its parent check doesn't hold.

The following background work rides along and is demand-driven and owner-only:

- relational index maintenance, sweeps and GC;
- constraint jobs;
- integrity activation backfill and generation retirement;
- TTL expiry;
- outbox publication and trimming;
- enrichment bookkeeping;
- promotions;
- split progress.

### 3.4 Progress separated from status

**Progress:**

- One authoritative progress record per consumer, `\x00\x00progress/<consumer>` (v2).
- It merges the applied sequence, the projection seal and the dense checkpoint.
- It is written only in the plan that publishes the consumer's effects.

**Status:**

- `IndexStatusSnapshot` is derived from live state.
- It is persisted at most once per `status_persist_interval` (5 s), at close, or piggybacked on another plan.

**Enrichment runtime status:**

- `enrichment_state.saveRuntimeStatus` splits into durable retry and terminal fields, written on change inside the producer's plan, and in-memory counters.

### 3.5 Journal retirement off the acknowledgement path

**Foreground callers only enqueue a watermark:**

- mutation_materialization.zig:3436;
- the derived executor's `truncate_fn`;
- mutation_commit.zig:5125 and 5178;
- the `truncateReplayJournalIfSafe` callers in db.zig.

**Owner service:**

- Retirement is an owner-only service inside the existing owner maintenance turn (path-lease holder or Raft leader).
- Each turn is one native transaction across all lanes, bounded by keys and bytes.
- `retireReplaySlice` replaces `truncateReplay` / `truncateReplayLane`.

**Credits and the reserve:**

- The append's plan charges a journal credit ledger; the retirement transaction releases it.
- Producers are admitted up to `capacity − progress_reserve`; progress and retirement plans may use the reserve.
- Explicit drains loop: consume → commit progress → retire → release.

**Lanes:**

- Hinted-lane references are written only for hints that have a registered consumer.

**Native page reclamation:**

- `reclaimRetirementPressure` moves out of `applyMutationGroup` into the owner turn, except under hard pressure.

### 3.6 No-consumer path

**Consumer registry:**

- A durable consumer registry in the metadata catalog covers indexes, generated enrichments, resolvers and promotions, the replication outbox, hot standby, and repair pins.

**Journal:**

- With no matching consumer, no journal record is written. `replay_meta_next_sequence` still advances, and consumers added later backfill from primary.

**Wakeups:**

- Commits return a `Demand` mask. `notifyMutation` becomes `notifyDemand(mask)` and wakes only when the mask is non-zero.
- Allocator certificates run at close or past a size threshold, not on every write.
- The owner loop waits without a timeout when idle.

**Durable obligations:**

- Outbox, TTL and repair obligations go into `lite/recovery_agenda.zig` atomically, in the plan that creates them.
- The agenda is read on open and on lease acquisition.

## 4. Workstreams

| WS | Scope | Main files | Depends on |
|---|---|---|---|
| WS0 | Sync, byte and operation counters; SQLite comparison; baseline | leaf IO hooks, `scripts/bench`, this directory | - |
| WS1 | Native commit protocol v8: (a) drop the hint sync; (b) page sequence + manifest + slot pages; (c) `syncData` and preallocation; (d) durable-epoch reuse | `lite/native.zig` commit/header/slot/page encode, `lite/allocator_v4.zig`, antfly_platform | - |
| WS2 | Ticketed group-commit queue and savepoints; visibility on durability | `lite/docstore.zig` 1419-3400, `TransactionWrites` in native.zig | WS1a (pipelining needs WS1d) |
| WS3 | Release the apply lock before the wait; capi writer lane | `db/mutation_commit.zig` 953-3290, `storage/docstore.zig` `commitStaged`, `capi/db.zig`, `capi/lite_connection.zig` gate/acquire | WS2 |
| WS4 | One physical commit for small indexed writes; `.native_catalog` text backend | `persistent.zig`, `lite/index_storage.zig`, `lite/backend.zig`, `lite/artifact_source.zig`, `commit_participant.zig`, `index_manager.zig` 27599-28000, mutation_materialization.zig 3386-3460 | WS2, WS6 |
| WS5 | Owner-queued retirement, journal v2, credit ledger, consumer-gated lanes | `lite/docstore.zig` 4524-4800, `storage/docstore.zig` 2855-2860, mutation_commit.zig 5125-5190, db.zig truncate callers, `derived_executor.zig`, `replay_*`, `internal_keys.zig` | - |
| WS6 | Progress v2 and derived status | `derived/apply_state.zig`; db.zig 37231-37300, 55782-55830, 61336-61760; index_manager.zig 9597 and 9723; `enrichment_state.zig` | - |
| WS7 | Demand-driven scheduling and consumer registry | capi/lite_connection.zig maintenance loop, capi/db.zig notify sites, lite/docstore.zig 1989-2160, `recovery_agenda.zig` | final wiring after WS2 and WS5 |
| WS8 | Crash qualification harness | new test files, sim IO | - |

Landing order:

- Phase A: WS0, WS1, WS5, WS6, WS7, WS8.
- Phase B: WS2.
- Phase C: WS3 and WS4.

## 5. Acceptance gates (1 KB documents, one document per transaction)

Throughput gates compare against SQLite in the same run of `scripts/bench/lite_sqlite_comparison.py` on the same quiet host, with a ReleaseFast `libantfly`. Sync and byte gates are build-independent and checked in Debug.

| Metric | Baseline | Target |
|---|---|---|
| Primary calling-path syncs/update | 3.0 | ≤ 1.0 with 1 writer; ≤ 0.25 with 8 |
| Primary bytes/update | about 60 KB | ≤ 40 KB with 1 writer; ≤ 12 KB with 8 |
| Primary throughput | 743 / 473 writes/s | ≥ SQLite WAL/FULL on the same host (17,630 / 17,255 on wolfspider); 8 writers above 1 writer |
| Indexed calling-path syncs/update | 36.4 | ≤ 1.0 with 1 writer; ≤ 0.3 with 8 |
| Indexed bytes/update | about 393 KB | ≤ 64 KB |
| Indexed throughput | 102 / 43 writes/s | ≥ SQLite + FTS5 on the same host (11,349 / 10,522 on wolfspider); 8 writers above 1 writer |
| Primary-only background work | 418 turns / 1,612 leases / 418 refreshes | 0 / 0 / 0 |

## 6. Required crash and recovery tests

1. A torn slot falls back to the previous checkpoint.
2. When the slot is durable but its pages were dropped, the manifest check fails and recovery falls back.
3. Crash at every syscall of a group, including group G+1 staging during G's fsync.
4. Indexed acknowledgement crash sweep: every acknowledged document is in primary and searchable without replay.
5. A crash during large-artifact staging leaves no catalog reference and no leaked space.
6. A crash during retirement leaves credits and deletes atomic, and the retired sequence stays ≤ the minimum consumer progress.
7. An explicit drain at journal capacity completes.
8. Outbox, TTL and repair obligations survive a crash with zero wakeups per write.
9. Multiple processes: after the lane owner gets `kill -9`, readers never see non-durable state.
10. An injected fsync error sends `OutcomeUnknown` to every member and triggers reconciliation.
11. Non-owner processes never retire, merge or reconcile.
12. A v8 build refuses a v7 file.

## 7. Status: commit-efficiency verification (`perf/lwa-commit-eff2`, Debug harness)

Lands `b60ca652ab` (ride-along commits join the next group instead of
leading their own, via `Store.stageRideAlong`) and `37a00f96fd` (native
commit manifest format v9: a run table of zero or more `(range_start,
count, crc[])` entries instead of one assumed-contiguous run, so a window
spanning several `flushTransactionWrites`/`persistAllocator` cycles no
longer falls back to the two-sync presynced path). Base `3d3c1f7e56`,
merged onto `da5239f5cd` with no conflicts.

**Debug harness (`zig build lite-write-amplification`, 128 updates,
1 writer unless noted), before (HEAD `da5239f5cd`) vs. after:**

| Mode | syncs/update before → after | durable commits/write | writes/s before → after |
|---|---|---|---|
| Primary only | 2.0 → **1.0** | 1.0 | 8.1 → 8.1 |
| Full-text visible (indexed) | 2.03 → **1.02** | 1.03 | 4.7 → 4.8 |
| Graph visible | 2.0 → **1.0** | 1.0 | 6.3 → 5.4 |
| Dense vector visible | 2.0 → **1.0** | 1.0 | 4.5 → 4.4 |
| Sparse vector visible | 2.0 → **1.0** | 1.0 | 0.7 → 0.6 (unchanged; §7.3) |
| Relational foreign-key child insert | 29.9 → **15.1** | 16.3 | 1.0 → 1.0 |

The second sync (the v8 manifest-contiguity fallback) is gone in every
mode: calling-thread syncs/update is ~1.0 everywhere (full-text/graph's
1.02-1.03 is real multi-commit overhead from unrelated bookkeeping, not a
second sync). Sparse vector throughput is unchanged -- its cost is a
separate, still-open problem (§7.3), not a sync-count problem.

### 7.1 A latent, now-fixed ride-along hang

`Store.ensureRideAlongFlushStarted` used to `tryLockStoreExclusive` to
start the one-time idle-flush background task, skipping silently on
contention (including the entire window a concurrent commit holds that
lock for, `commit_in_sync != 0`) on the stated assumption that "the next
`stageRideAlong` call gets another chance." Before `b60ca652ab`, nothing
depended on that assumption: ride-along commits led (and closed) their
own group immediately, so the idle timer was never the only thing that
could resolve a pending ride-along ticket. After `b60ca652ab` (and this
change's own `native_catalog_backend.zig` fix below), a ride-along ticket
with no concurrent foreground write and no later `stageRideAlong` call
depends entirely on that timer; losing the race to arm it left the ticket
-- and `Store.close()`'s eventual `forceDrainDurable()` wait on it --
blocked for however long until *something* drove the queue, which could
be never. Reproduced as a genuine hang (zero CPU progress, not just slow)
in `crash sweep: sustained full_index writes survive a crash during
ride-along merge and retirement`, sampled to a `tryLock` loop stuck
waiting on a `commit_in_sync`-held `store.mutex`. Fixed by guarding the
one-time task spawn with a dedicated, essentially-never-contended lock
(`ride_along_flush_start_mutex`) instead of the broad store-exclusive one,
so arming can no longer be skipped by ordinary commit traffic. The same
crash-sweep test passed twice in a row after the fix (previously hung for
40+ minutes under load without producing a result).

### 7.2 WS4 for background index maintenance

`native_catalog_sink.Sink` gained `submitDeferred`/`Pending`
(`storage/native_catalog_backend.zig`, `storage/lite/native_catalog_sink.zig`):
`NamespaceTxn.commit()` with `durable = false` now stages via
`Store.stageRideAlong` and returns immediately -- the backend's in-memory
mirror is updated synchronously regardless, per the existing
`applyExternallyCommittedMutations` contract -- instead of calling
`Store.submitIndexMutation`, which always led (and closed) its own
one-member group even when non-durable. `Backend.pending_ride_along`
collects the resulting `Pending`s and drains them opportunistically: free,
because a durable commit on the same backend already swept them into its
own group by the time its `submit` returns; never on a count/byte
threshold of its own, which regressed the `graph metric-build` test below
by reintroducing a bounded form of the same per-pass idle wait this
exists to remove. Verified via
`storage.lite.docstore.test.lite graph metric-build background passes
ride along: N passes plus one foreground write is one sync`: 11
worker/coordinator passes, 0 syncs for those passes, confirmed via a
direct harness run (`metric-build passes=11, syncs for those
passes=0`) -- previously 11 standalone non-durable commits.

**Still open:** three of the `WS4-for-graph`/`WS4-for-dense`
`commits_for_this_write <= 6` regression tests failed under this session's
extreme host contention (load average repeatedly 20-70 on 14 cores) with
counts above 6, where they passed before this change. The working
hypothesis is that the 50 ms idle-flush timer did not get scheduled before
the tests' own setup-to-measurement window elapsed in wall time, so
ride-along backlog from index setup that would normally have settled
beforehand landed inside the measured write's own commit count instead.
Not reproduced as a hang (all three completed and reported a concrete,
too-high count), and not attempted to fix blind under the same contention
that may be causing it; needs a clean re-run on a quiet host before
deciding whether the bound needs tightening, loosening, or a structural
fix (e.g. making index-open/seed settling wait on `Backend.
pending_ride_along`, not just on application-level idle). The `<= 6`
bounds are left unchanged rather than tightened, since the only values
observed in this session were either unchanged (full-text, which passed)
or anomalously high (graph/dense, under contention) -- neither supports a
lower number yet.

### 7.3 Sparse vector per-write cost: term-meta/term-range merged

`SparseIndex.installInlineSparsePublication` → `native_catalog_backend.
Backend.applyExternallyCommittedMutations` → persistent-treap `put` per
mutation is architecturally proportional to the write's own size: with
`sparse_terms = 32` (the harness's dimension count) and every term
brand-new (the harness's deterministic token ids never repeat across the
128 documents it writes), each write used to durably touch roughly 2
(forward/reverse) + 5 per term (chunk, chunk-range-meta, term-meta,
term-catalog entry, term-range-meta) + 2 (counters) ≈ 164 keys, each a
`mem_ordered.Tree.put` -- an O(log n) persistent-treap path copy, each
node copy its own `allocator.create` plus three `allocator.dupe`s.

A term's meta (max_weight, chunk_count) and its doc-id range (min/max
across chunks) were two separate records even though every call site
reads or writes them as a pair; merged into one (§7.3's own commit,
`perf(sparse): merge term meta + term range into one record per term`).
For the inline/incremental path specifically, an insert-only touch also
no longer defers its range update at all -- it was always correct to
extend the range immediately (there is no cheaper "batch it" version of
an extend-only update), unlike a delete, which still defers to a full
chunk rescan since only that can correctly *shrink* a range. New term
steady-state: 5 keys/term → 4 (chunk, chunk-range-meta, the combined
term record, term-catalog once); a re-touched term: 4 → 2.

Debug harness result (same host, same load regime as the rest of this
table, so treat the absolute numbers as noisy -- `primary-only` moved
too, from this session's own load swings -- but the sparse-vs-everything-
else *ratio* is the real signal): sparse-vector-visible went from
0.6-0.7 writes/s to 1.0-1.1, while primary-only/dense/graph were all
simultaneously *lower* than their own §7's figures under this run's
heavier load. Sparse moving up while its neighbors moved down under the
same contention is not a load artifact.

The chunk + chunk-range-meta pair (also always co-accessed, and the
larger remaining write-count lever) is deliberately NOT merged:
`encodeChunk`'s own payload is a documented "compatible Go v1 payload"
wire format, and the pair's readers extend into segment split/handoff
classification (`handoffTermPostings`/`pruneTermPostings`/
`accumulateSplitPlanningStats`) and a direct query-time chunk scan --
materially larger, riskier blast radius than the meta+range pair
actually cut. Left for a follow-up with more verification budget; the
file's own `WriteProfile` counters are still only wired for the
bulk-append path, not the incremental one `stageIncrementalMutations`
uses -- extending them there first would turn the next attempt's sizing
from an estimate into a measurement before cutting anything.

Verification: `zig build sparse-test` -- 84/84 pass (0 leaked),
including a new regression test for the one correctness-sensitive
interaction this change introduces (a term touched by both an insert
and a delete in the same deferred batch must not have the insert's
immediate write and the delete's deferred recompute clobber each
other) and that the range shrinks correctly across a close/reopen.

### 7.4 Full crash sweep family: green

All 23 `crash sweep`/`crash sweep exhaustive` tests in
`crash_consistency_test.zig` pass (0 leaked) on top of every fix in
this section (§7.1's idle-flush fix, §7.2's background-commit batching,
§7.3's sparse term-record merge), including `crash sweep: sustained
full_index writes survive a crash during ride-along merge and
retirement` -- the one test perf/lwa-postmerge owns and is separately
fixing for its known intermittent `AcknowledgedWriteLost` signature.
It passed cleanly here; that satisfies this session's gate (everything
else green, and this one either green or failing with its own known
signature, never a hang or a new failure mode) without this session
needing to reproduce or touch its bug.

### 7.5 Columnar maintenance scheduler's own background commits: ride-along

The write-amplification bench's "relational write surface" report showed
every relational mode still paying a non-zero `background_syncs_per_update`
(baseline ~1.6-1.9 `durable_commits_per_write`). A bounded stack-trace
diagnostic in `write_amplification_trace.zig`'s `recordSync()` (test-only,
since removed) traced every occurrence to `relational_columns.zig`'s
columnar-index maintenance scheduler -- `selectReady`, `compact`, `prune`,
`drainCleanup`, `drainCleanupWithLimit`, `drainGarbage` and `drainRetired` --
each opening its own standalone write transaction and forcing its own fsync
for scheduling/GC bookkeeping over the *derived* columnar index (which byte
range is deferred and until when, a build-slot claim/counter, the dirty-
journal pagination cursor, retired-root GC), never the primary row data.

All of it is already redoable through recovery paths the scheduler already
has for a canceled/interrupted pass: an abandoned `building_key` gets
reclaimed as garbage on the next turn, and GC progress is its own durable
cursor (deleting a record/page over again, next pass, is idempotent). That
makes every one of those sites a ride-along candidate under §3.2a. Converted
them all to `beginWriteBatchWithOptions(.{.durability = .ride_along})`;
`sameValue`/`deleteIfPresent`/`clearPage`/`scheduleMaintenance`/`retireBlock`/
`bootstrapBoundary`/`Cleanup.publish` (`relational_columns.zig`) and
`Prepared.apply` (`column_payloads.zig`) were widened to `anytype` so they
work over both the real `Txn` and the ride-along `Batch` type.

Measured `durable_commits_per_write` dropped to ~1.3-1.6 across the
relational modes; the bench's regression gates tightened from 2.5 to 2.0
accordingly (`write_amplification_bench_test.zig`). The residual is not a
bug: `docstore.zig`'s `rideAlongFlushLoop` (the idle-flush timer that bounds
how long a ride-along write can sit undurable when no foreground commit
comes along to carry it) still fires periodically, now tagged
`.ride_along_flush` in `write_amplification_trace.zig` instead of falling
into the unattributed `.other` bucket. `relational foreign-key` is
unaffected (still ~15.5-15.7, unchanged) -- it still spans two physical
tables through the full cross-database two-phase protocol; folding its
`.attach` bookkeeping into the child's own fused commit is the multi-table
coordinator described as follow-up work below, not done in this pass.

**Follow-up: the FK multi-table coordinator.** `capi/sql.zig`'s
`eligibleForLocalDecision` excludes any multi-table request, and
`mutation_commit.zig`'s `prepareLocalIntegrityDecision` refuses a `.foreign`
generation command as defense-in-depth
(`if (generation.definition.kind != .unique) return error.InvalidIntegrityOperation;`)
even if something upstream routed one in. Each SQL table opens its own `*DB`
(own `core.apply_mutex`) via `capi/tables.zig`'s `get(handle, name)`, all
against the same `.aflite` file; `relational_integrity_contract.zig`'s
claim/reference keys (`Address.key`) are addressed by `routing = hash(
generation, tuple_digest)`, not by table id, so they live in a namespace
shared by every table participating in one FK's `generation` -- the parent's
`.attach`/`.detach` record can fold into the child's own native batch via
`BatchExecutionOptions.extra_store_writes` exactly like a UNIQUE claim does
today, with no separate commit against the parent's own store. What is
missing is the lock discipline: a fused child insert/update needs the
parent's row to stay stable while it claims/releases a reference, and a
parent delete needs no concurrent child claim while it checks for zero
remaining references, so both operations must hold *both* tables'
`apply_mutex` for the duration of touching that shared state -- in one
fixed global order (ascending `core.identity_namespace.table_id`) to rule
out a deadlock between a child-insert-then-parent-check order on one thread
and a parent-delete-then-child-check order on another. That requires a new
entry point upstream of `commitCoordinatedLocal` that resolves every
participant table via `tables.zig`'s `get`, sorts them by table id, and
acquires their `apply_mutex`es in that order before the normal single-table
batch logic runs -- not a change to `prepareLocalIntegrityDecision` in
place, since by the time that runs today the caller already holds only its
own table's lock. Not implemented this pass: a cross-table lock-ordering
primitive is exactly the kind of thing worth its own dedicated, heavily
stress-tested session rather than folding in under time pressure --
the concurrency test matrix it needs (parent-delete racing child-insert
across N threads with no orphan/lost-reject/deadlock, plus a two-table
mutual-FK lock-order test) is substantial in its own right.
