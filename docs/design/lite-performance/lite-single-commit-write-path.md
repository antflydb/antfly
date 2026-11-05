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

| Metric | Baseline | Target |
|---|---|---|
| Primary calling-path syncs/update | 3.0 | ≤ 1.0 with 1 writer; ≤ 0.25 with 8 |
| Primary bytes/update | about 60 KB | ≤ 40 KB with 1 writer; ≤ 12 KB with 8 |
| Primary throughput | 743 / 473 writes/s | ≥ 8k with 1 writer; 8 writers ≥ 1.5× one writer |
| Indexed calling-path syncs/update | 36.4 | ≤ 1.0 with 1 writer; ≤ 0.3 with 8 |
| Indexed bytes/update | about 393 KB | ≤ 64 KB |
| Indexed throughput | 102 / 43 writes/s | ≥ 1,000 (stretch ≥ 4,000), rising with writers |
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
