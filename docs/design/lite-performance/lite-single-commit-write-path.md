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

**One journal entry per commit, shared by every lane.** The CPU agent's `document_index` key trace found a primary-only write (no managed indexes at all) still appending a `replayEntryKey` copy and a paired `replayLatestSequenceKey` marker per active consumer hint -- five of each in that fixture, dominating the per-commit page count (6.8 pages/write). `writeReplayEntries` (`storage/lite/docstore.zig`) now writes exactly one physical entry per commit, under the `replay_all_kind` lane; it no longer fans the payload out (filtered per hint, via `encodeReplayPayloadForHint`) into a separate key per active hint, and no longer writes a per-hint `replayLatestSequenceKey` at all. A hint-specific lane read (`forEachReplayLaneFrom(hint_ordinal, ...)`) now always resolves to `forEachReplayLane`'s filtering wrapper around the single "all" lane: it scans that lane by sequence, checks the entry's hint bitmask cheaply (`encodedRecordHintMask`, no decode, before deciding whether to do the real decode+filter work), and only for a match does it decode, project through the same `encodeReplayPayloadForHint` that used to run at write time, and hand the caller byte-identical output to before -- now computed lazily on the entries a catch-up scan actually visits, not on every commit regardless of whether anything ever reads that lane. `storage/docstore.zig`'s `latestReplaySequenceForOrdinal` (the cross-backend debt/target-sequence probe) falls back to the single producer head sequence (`lastReplaySequence`) when the per-hint marker key is absent, which is now always true for Lite; a backend that still writes the per-hint marker (unaffected -- this change is Lite-only) keeps its exact answer. The credit ledger charges exactly one record per append now (previously an upper bound across every active lane); retirement's own per-lane deletion loop is unaffected (it already deletes by physical key range per lane and simply finds nothing in a hint lane going forward). Two existing lite-side tests asserted the old per-lane physical shape directly (`expectLedgerMatchesScanForTest` scanning every hint lane for its own copy; a retirement-clamp test reading a per-hint key's raw bytes) and were updated to the new one-entry reality rather than reflecting a now-nonexistent write.

**`applied_sequence` means "complete through N", not "touched N".** A consumer's applied watermark advancing to N asserts that every record up to N is accounted for -- either materialized (relevant, and applied) or verified irrelevant (scanned under this consumer's hint and found not to apply). The shared lane makes the second case common: a consumer with no relevant record anywhere in a range still has its own records-not-mine genuinely scanned and hint-filtered (`replay_hint_filter_skips`) before `canAdvanceDerivedToTargetAfterReplayAsync` credits the range, exactly like the dense certified-empty generation (`ensureDensePostingCoverageByName`) credits a generation with zero embeddings once its artifact counter is proven to match. A verified-empty advance is therefore not vacuous and not a bug: it never advances past the extent that was actually scanned, and an index that is gated or blocked (never attempted, not merely found irrelevant) still correctly stays put -- see `db.zig`'s `"db replay respects per-index applied watermarks"` (verified-empty, allowed to advance) against `"db source-only batch retains a certified empty dense generation"` (gated, must not advance).

**Co-located per-commit metadata keys.** `GlobalReplayKind.next_sequence` moved from ordinal `0x02` to `0x58`, immediately next to `outcome_receipt` (`0x55`), `journal_format_version` (`0x56`) and `journal_credit_ledger` (`0x57`) in `storage/internal_keys.zig`. These are the per-commit metadata keys written on (or near) every append -- the producer's head sequence, the exported-write receipt, and the credit ledger -- and previously sorted far apart from each other, interleaved with dozens of other, far less frequently written, replay-metadata key families (`artifact_presence`, `terminal_failure_*`, `graph_contender`, etc.). Four consecutive ordinals guarantee the same B-tree leaf regardless of which of those other families a given store happens to have populated.

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
into the unattributed `.other` bucket. `relational foreign-key` was
unaffected by this pass (still ~15.5-15.7) -- it still spanned two
physical tables through the full cross-database two-phase protocol;
folding its `.attach` bookkeeping into the child's own fused commit is
the multi-table coordinator §7.6 below implements.

### 7.6 FK multi-table coordinator: implemented

Builds on §7.5's follow-up. `capi/sql.zig`'s `eligibleForLocalDecision`
excluded any multi-table request, and `mutation_commit.zig`'s
`prepareLocalIntegrityDecision` refused a `.foreign_key` generation
command as defense-in-depth even if something upstream routed one in.
Each SQL table opens its own `*DB` (own `core.apply_mutex`) via
`capi/tables.zig`'s `get(handle, name)`, all against the same `.aflite`
file.

**Physical layout, corrected from §7.5's own assumption.**
`relational_integrity_contract.zig`'s claim/reference keys are addressed
by `hash(generation, tuple_digest)` with no table id in the *address*,
but every key -- including these -- still passes through
`docstore.zig`'s `Txn.prefixedKey`, which unconditionally prepends the
owning table's own `"\x02db/" ++ namespace ++ "\x00"` key prefix
(`storage/lite/backend.zig`'s `configureDbOpenOptionsForNamespace`).
`api/relational_integrity_commit.zig`'s `Plan.expand` always attributes
the `.attach`/`.detach` command to the **parent's own**
`TableCommitRequest`, so in the working two-phase path it is validated
and written through the parent's own `*DB`/`core.store`, landing under
the parent's physical prefix -- never the child's. So §7.5's "fold the
`.attach` into the child's own native batch via `extra_store_writes`"
idea does not work: that would write the Reference under the *child's*
prefix, where the parent's own later reference scans (at DELETE time)
would never find it. The achievable design keeps the write split across
two physical stores and instead shares one fsync between them.

**Lock discipline.** `capi/sql.zig`'s `commitCoordinatedFusedMultiTable`
is a new entry point upstream of `commitCoordinatedLocal`. For a
prepared plan with exactly one table carrying real row writes/deletes
(the *leader*, e.g. a child's own INSERT) and every other participant
carrying only `integrity_commands` (a *follower*, e.g. the parent's
`.attach`), it resolves every participant's `*DB` via `tables.zig`'s
`get`, and -- only when every follower's `table_id` is smaller than the
leader's -- locks each follower's `core.apply_mutex` directly, in
ascending `table_id` order, before anything else runs. The leader's own
lock is never touched directly: `commitCoordinatedLocal` acquires and
releases it internally, unmodified, strictly after (and so strictly
above, in table-id terms) every follower lock this call already holds.
That keeps the full lock-acquisition sequence for this call -- every
follower ascending, then the leader internally, last and largest -- in
the same global ascending order a concurrent parent-side DELETE's own
single-table `commitCoordinatedLocal` call follows (RESTRICT's
"zero remaining references" check only ever touches the parent's own
prefix, so it needs no cross-table lock at all, just its own
`apply_mutex` -- already the correct serialization point against a
concurrent attach, since both operations' writes land under that same
prefix). A direction whose leader's `table_id` is *not* the largest
(the other side of a mutual-FK pair, where each table is a parent to
the other's child) falls back to the existing cross-database two-phase
`sql_commit.zig` coordinator, which already serializes through
`handle.sql_commit_mutex` -- never attempted through this lock-ordering
scheme, so it can never invert it.

**Commit sharing.** Each follower's validated effects are written via
a ride-along commit (`BatchOptions.durability = .ride_along`) against
that follower's own `core.store` -- all tables of one Lite single-file
backend share one underlying `docstore.Store`, so the entry is swept
into the very next foreground commit on *any* table of that store,
normally the leader's own commit immediately following (lite-single-
commit-write-path.md §3.2a/§7.2's existing pattern, not a new commit
primitive).

**The ambiguous-outcome bug this needed fixing.** The first version
reverted every already-staged follower write (a synchronous,
foreground compensating commit restoring prior bytes, or deleting a
key that did not exist before) whenever the leader's call returned
*any* error. Crash-sweep testing (`crash sweep: relational foreign-key
child insert`, new in `crash_consistency_capi_test.zig`) caught this:
`commitCoordinatedLocal` can fail with `error.SqlMutationOutcomeUnknown`
-- an injected crash during checkpoint publication is the textbook
case -- meaning the leader's row write *may already be durable*.
Reverting on that ambiguous outcome durably erased the parent's
Reference via the revert's own synchronous commit while the child row
turned out to be genuinely durable: a parent DELETE then incorrectly
succeeded despite a surviving child reference -- exactly the orphan
this coordinator exists to prevent. Fixed by checking
`dependencies.sql_errors.isUnknownOutcome(err)`: only a *definite*
leader failure triggers the revert; an ambiguous one leaves every
follower write exactly as staged and propagates the same uncertainty
outward, matching `commitCoordinatedLocal`'s own existing
ambiguous-outcome contract. (A narrower residual gap remains
undefended: a crash during the revert's *own* commit, after a
definite leader failure, could still leave a follower's stale
Reference pointing at a child row that never came to exist -- a
dangling reference, not an orphan. Not exercised by the current
crash-sweep scenario and not closed in this pass.)

**Measured** (`zig build lite-write-amplification`'s relational write
surface report, 128 updates): `relational foreign-key child
insert/update` dropped from ~15.5-16 to ~1.9 durable commits/write with
1 writer, ~2.3 with 8. The residual above 1.0 is the same
`ride_along_flush` idle-timer dynamic §3.3b documents for the
algebraic overwrite case: the follower's ride-along write usually, but
not always, gets swept into the exact same fsync as the leader's own
foreground commit; when it loses that race it becomes durable on the
next foreground commit (on any table of the store) or the 50ms
idle-flush timer instead, costing close to a second commit. Gates
tightened accordingly in `write_amplification_bench_test.zig`:
`foreign_key.durableCommitsPerWrite()` from 20.0 to 3.0, plus a new
`foreign_key_concurrent` gate at 3.5 (no gate existed for the
concurrent mode before). Closing the remaining gap to ~1.0 needs the
same cross-cutting ride-along-timing work §3.3b/§7.5 already flagged
as follow-up, not a change specific to FK.

**Tests.** `write_amplification_bench_test.zig` gained two concurrency
regressions alongside the existing UNIQUE-race/missing-parent-race
pair: a parent DELETE racing a child INSERT against the same parent
(asserts the no-orphan invariant against final durable state, not
per-worker outcome codes -- a DELETE matching zero rows still reports
`.ok` under standard SQL semantics, so more than one racing DELETE can
legitimately "win"), and a two-table mutual-FK pair with concurrent
writers inserting in both directions (only one direction's leader
qualifies for the fused path; the other takes the two-phase fallback;
both must complete without deadlock). `crash_consistency_capi_test.zig`
gained the FK child-insert crash-sim scenario described above. All
pre-existing suites stayed green throughout:
`capi-contract-test -Dtest-filter="relational"` (414 tests),
`-Dtest-filter="capi SQL outcome"` (26 tests),
`-Dtest-filter="crash sweep exhaustive"` (7 tests), and
`antfly-storage-test -Dtest-filter="crash sweep"` (23 tests) -- all
0 failed, 0 leaked.

## 8. Cross-process coordination: shared lock region

Status: design landing on `perf/lwa-cpu`, in reviewable steps (§8.9),
each gated green before the next starts. Supersedes `storage/lite/
inode_lock.zig`'s process-wide flock registry from the immediately
preceding round of this same effort (per-inode `Entry`, parked kernel
locks, a `writer_queue.zig`-style ticket for cross-process handoff):
that round cut per-write `flock()`/`open()` to zero by replacing a
wall-clock fairness quantum with event-driven release, but the event
itself was still a `pread` on a ticket-queue counter file, and the
file-based reader-fence/epoch machinery (`reader_epochs.zig`,
`ReaderCoordinator`) it left untouched is the larger remaining cost
(open, stat and scan a namespace directory per reader admission).
This section designs the replacement for all of it: one mapped region
per database, modeled on SQLite's `-shm` WAL-index and LMDB's
`lock.mdb`, where the steady state -- no contention, no crash -- never
calls `open`, `flock`, or issues a read syscall for coordination at
all, and the contended state hands a commit off to a waiting process
in about one commit, not a fairness quantum or a `pread`-sampled
timer.

### 8.1 The shared region: `<db>.aflite-lock`

One file next to the database, memory-mapped `MAP_SHARED` by every
process with the database open, created and version-checked under a
one-time `flock` (the only `flock` this design still issues, and only
at open, not per commit):

```
offset  bytes  field
0       8      magic ("AFLLOCK1")
8       4      format_version (bump refuses an older/newer mapper, same
               policy as native.zig's own checkpoint format bump)
12      4      header_checksum (CRC32C of bytes 16..header_end, so a
               torn header write is detected the same way a torn
               native.zig slot is)
16      8      writer_word (§8.2)
24      8      writer_ticket_tail (§8.2's FIFO counter)
32      N*32   reader slots (§8.3), N = max_readers (configurable at
               create time, stored in the header; a region created by
               an older binary with fewer slots is grown -- see
               "region growth" below, not rewritten in place)
...     M*1    process liveness bytes, one per slot (§8.4)
```

Opened lazily, the first time a `NativeFile` needs cross-process
coordination for a given inode (mirroring `inode_lock.Entry`'s own
open-on-first-use): `open`/`create` the file, `flock(.exclusive)`,
check magic/version/checksum, initialize if absent or rebuild if the
checksum fails (a corrupt region is never trusted, only replaced --
same posture as `native.zig`'s own slot/manifest recovery), `flock`
unlock, `mmap(MAP_SHARED, PROT_READ | PROT_WRITE)`, keep the mapping
for the `NativeFile`'s (and every sibling same-inode `NativeFile`'s,
via the in-process registry, §8.5) lifetime. Every field after the
header is touched only through atomics on the mapping -- no further
`read`/`write`/`pread` once mapped, by construction, not by a
best-effort fast path with a slow fallback hiding behind it.

**Region growth.** `max_readers` is fixed at creation; a process that
needs more slots than the region has (more concurrent readers than any
opener has ever configured) remaps a larger region under the same
one-time `flock`, writes the new size into the header, and every other
process notices the size change on its own next `fstat`-guarded
lazy-remap check (done at the same cadence as the mmap-failure
fallback probe below, not per commit). This is the one path that still
touches the filesystem after the initial open, and only on the
cold/rare growth transition, not on an ordinary command.

### 8.2 Writer lock: CAS, FIFO ticket, futex handoff

`writer_word` (offset 16, 8 bytes) is `0` when free, else the holder's
`(pid, generation)` packed into the word (`generation` so a dead
holder's stale word is distinguishable from a live one of the same
reused pid, together with the liveness check in §8.4 -- never a PID
reuse check alone).

- **Uncontended acquire/release**: `cmpxchg(writer_word, 0, mine)` to
  acquire, plain atomic store of `0` to release. No syscall either way
  -- this is the whole point, and it is the path every warm write
  takes.
- **Contended acquire**: CAS fails. The waiter takes a ticket from
  `writer_ticket_tail` (atomic fetch-add, in the region, no file) and
  parks on a cross-process futex at a byte derived from its ticket
  slot:
  - macOS/Darwin: `__ulock_wait`/`__ulock_wake` with
    `UL_COMPARE_AND_WAIT_SHARED` (the shared-memory variant; private
    `UL_COMPARE_AND_WAIT` only works within one process's address
    space, the wrong primitive for a `MAP_SHARED` region across
    processes).
  - Linux: `futex(2)` without `FUTEX_PRIVATE_FLAG` (the flag that
    restricts a futex to one process's address space; cross-process
    futexes on a shared mapping must omit it).
  - Both are declared in `antfly_platform`'s existing per-OS syscall
    surface (same module that already wraps `flock`/`fcntl` for this
    codebase), not re-declared locally.
- **Handoff**: the releaser, instead of a plain store when
  `writer_ticket_tail` shows a waiter behind it, stores the next
  ticket's holder into `writer_word` and wakes that specific futex
  word. FIFO because the ticket counter orders waiters exactly as
  `writer_queue.zig`'s file-based ticket does today, without a
  directory, a per-ticket file, or a tail `flock`.
- **No timer anywhere in this path.** The 1-second fairness quantum
  this section replaces, and the quantum-free but still-polled design
  one round before it, both exist only because a kernel `flock` cannot
  tell its holder "someone is waiting" without the holder asking. A
  futex can: `__ulock_wake`/`futex(..., FUTEX_WAKE, ...)` only returns
  once a waiter is actually woken, so the releaser's cost is paid
  exactly when, and only when, a waiter exists.

### 8.3 Reader table: fixed slots, in-memory oldest-pin scan

Each of the `max_readers` 32-byte slots (offset 32 onward) holds:

```
offset  bytes  field
0       4      owner pid (0 = free slot)
4       8      generation (packed (pid, open-generation) pair, to
               detect a slot a dead reader never cleared being reused
               by a new process with the same pid)
12      8      pinned_sequence (the commit_sequence this reader will
               not let the writer reclaim pages behind; monotonic,
               written once at pin time, cleared to 0 on unpin)
20      8      reserved (padding to 32 bytes; room for a second pin --
               metadata vs. data, matching `minimum_reader_sequence`
               vs. `minimum_metadata_reader_sequence` in native.zig
               today -- without another format bump)
```

A reader claims a free slot (the first `owner == 0` it finds via CAS,
same uncontended-fast-path shape as the writer word -- claiming is
rare enough per-reader-lifetime that a short linear scan under
contention is acceptable, unlike the per-commit writer path) once per
`NativeFile`/pin lifetime, writes its `pinned_sequence`, and clears the
slot (`owner = 0`) on unpin. A writer computing its reuse frontier
(`scanReaderEpochsCached`'s replacement) scans the fixed slot array --
bounded by `max_readers`, entirely in mapped memory, no directory
walk, no namespace file, no per-reader marker file -- and takes the
minimum `pinned_sequence` over slots with a live `owner` (live checked
per §8.4, not just `owner != 0`, so a dead reader's never-cleared pin
cannot block reclamation forever). This directly replaces
`reader_epochs.zig`'s namespace-directory scan and
`ReaderCoordinator`'s in-memory mirror of the same file-based state:
once this lands, both describe a reader population a *different*
process can also see without reading either's files, because that
population now lives in the one region every process maps.

### 8.4 Liveness: per-slot flock byte range, no PID-reuse assumption

Every process, on first touching the region, opens (if not already
open) one dedicated fd on `<db>.aflite-lock` and takes a lifetime
`flock` on the byte range belonging to its own writer-word holder slot
*or* reader slot (`fcntl(F_SETLK)` byte-range locking -- `flock`'s own
whole-file granularity cannot do this, so liveness uses `fcntl` range
locks specifically, a different primitive from the registry's
whole-file `flock`; both operate on the same fd class and are declared
side by side in `antfly_platform`). The kernel drops this lock
unconditionally on process death (`exit`, `kill -9`, OOM-kill), with
no dependency on the process having run any cleanup code.

A process that finds `writer_word != 0` or a reader slot's `owner !=
0` but needs to know whether that holder is *actually* still alive
(the futex wait timed out with no wake, or a reader-frontier scan
needs to decide whether a never-cleared pin is real) takes a
non-blocking `fcntl(F_SETLK)` probe on that specific byte range:
success means the recorded holder is dead (the kernel already dropped
its lock), failure means it is genuinely still running. This is the
only liveness signal; there is no PID-reuse heuristic anywhere in this
design, matching `writer_queue.zig`'s own existing rule 5 but applied
per-byte-range instead of per-ticket-file.

**Recovery on a dead writer.** A process that proves (via the
byte-range probe above) that the `writer_word` holder is dead must run
the same crash recovery `native.zig` already performs on open
(`recoverActiveCheckpoint`, slot/manifest validation) before clearing
`writer_word` and taking the lock itself -- a holder that died
mid-commit may have left the data file in a state only that recovery
path already knows how to validate. This is existing code reused, not
new recovery logic: the shared region only decides *who* clears the
word and *when* (after recovery succeeds, not before), it does not
reimplement what recovery checks.

**Recovery on a dead reader.** Simpler: a dead reader's pin is just
cleared (`owner = 0`) by whoever's frontier scan proves it dead via
the byte-range probe; nothing the reader held needs validating, since
a reader never mutates the file.

### 8.5 In-process mapping: one region, one slot, every `NativeFile`

`inode_lock.zig`'s registry already solves "every `NativeFile`/`Store`
on the same inode in this process shares one thing" for the flock
case; the shared region replaces *what* is shared, not the sharing
mechanism. The process-wide registry keyed by inode (`inode_lock.open`/
`unref`, reference-counted, rebound across
`replaceWithPreparedGeneration`'s generation swap) keeps its exact
shape: `Entry` gains the mapped region pointer and this process's
writer-holder/reader-slot index in place of the kernel fd/`Io.File`
state it holds today. Every `NativeFile` on the same inode -- same
`Store`, or an `externally_locked` sibling -- resolves to the same
`Entry`, hence the same mapping and the same slot, exactly as it
resolves to the same kernel-lock `Entry` today. A `Handle`'s capture of
"the exact `Entry` it was granted against, independent of a concurrent
rebind" (`inode_lock.zig`'s module doc) carries over unchanged.

### 8.6 Fallback: flock-based path, detected at open

mmap or either OS's shared futex primitive can be unavailable: a
non-local filesystem that does not support `MAP_SHARED` coherency
across the machines involved (network filesystems, which `native.zig`
already treats specially elsewhere), a sandboxed or restricted
environment that denies the syscall, or (for a future, currently
unsupported target) a platform with neither primitive declared in
`antfly_platform`. Detected once, at the same first-open point that
creates or opens the region: an `mmap` failure, or a one-time
self-test futex wait/wake pair that does not behave as expected, flips
a per-`Entry` flag and falls back to exactly the flock-based path this
section replaces -- not a partial mix of the two within one `Entry`'s
lifetime, so a `Handle`'s release path never has to ask "which mode
was this acquired under" beyond that one flag. This is the same
posture item 8.1 took for mmap itself: a fallback that is fully one
thing or fully the other, never a best-effort layer silently degrading
field by field.

### 8.7 Migration: what this replaces, and deletes

Every one of these moves onto the shared region; none is kept running
alongside it once its migration step lands, per the explicit
instruction to delete rather than dual-run:

| Replaced | File | Becomes |
|---|---|---|
| Writer lane admission gate | `capi/lite_connection.zig` `WriterLane` | §8.2 writer word + futex |
| Path write gate | `capi/lite_connection.zig` `PathWriterLock`/`PathGate`, `othersWaiting(AcrossProcesses)` | §8.2 (FIFO ticket already visible in the region; no separate signal needed) |
| CLH ticket queue | `storage/lite/writer_queue.zig` (`Queue`/`Turn`/`Watch`, the `<sidecar>.q/` directory) | §8.2's in-region ticket counter |
| Per-inode flock registry | `storage/lite/inode_lock.zig` | §8.5 (the `Entry`/registry *shape* survives; its kernel-lock payload does not) |
| Epoch-reuse lock | `native.zig` `acquireEpochReuseLock`/`beginReaderWindow`/`resumeReuseIfUnblocked` | §8.2 writer word (reuse no longer needs its own lock once there is one writer word per inode, held for the commit's duration) |
| Reader fences | `native.zig` `acquireReaderFence`/`releaseReaderFence` | §8.3 reader slot pin/unpin |
| Reader epoch namespace | `storage/lite/reader_epochs.zig` (namespace directory, per-reader marker files, `identity` file) | §8.3 (deleted entirely; also removes the `identity`-file-escapes-to-cwd bug class `native.zig`'s own `bug2` regression test and `reader_coordinator.zig`'s cross-root marker-sweep test guard against, since there is no longer a namespace directory to escape) |
| `ReaderCoordinator`'s in-memory mirror | `storage/lite/reader_coordinator.zig` | §8.3 (the region *is* the shared state; no process-local mirror of it is needed) |
| Vacuum's data-rewrite lock | `native.zig` `acquireDataRewriteLock`, `vacuumWithCancel` | §8.2 writer word (vacuum is a writer like any other for this purpose) |
| Backup/restore staging locks | `storage/lite/restore_staging.zig`, `artifact_source.zig`'s publication guard | §8.2/§8.4 (a backup reader is a reader slot; restore's exclusive staging window is a writer-word hold) |
| Snapshot path | `native.zig` `captureFullReadSnapshot`, `captureAllocatorVerificationSnapshot` | §8.3 (a snapshot is a pinned reader slot with no file-based fence) |

Each row lands as its own commit per §8.9; a row is deleted (not
feature-flagged or kept as a fallback branch) in the same commit that
migrates its callers, so the tree never carries two live
implementations of the same mechanism.

### 8.8 Tests

- **Two- and N-process contention.** Fork `N` real child processes (as
  `writer_queue.zig`'s own multi-process tests already do via
  `std.c.fork`/`waitpid`) contending for the writer word; assert FIFO
  grant order (ticket order == grant order) and that each handoff
  completes within about one commit's wall time, not a fairness
  quantum, with no process spinning or polling in between (a passing
  run with zero `EAGAIN`-driven retries on the futex wait, not just a
  bounded latency).
- **`kill -9` of the writer mid-commit.** A child acquires the writer
  word, is killed before releasing; the parent's byte-range liveness
  probe must detect the dead holder, run crash recovery, and hand the
  lock to a queued waiter -- no hang, and the recovered file passes
  `check()`.
- **`kill -9` of a reader holding a pin.** A child pins a reader slot,
  is killed; a concurrent writer's reuse-frontier scan must notice the
  dead slot (byte-range probe) and not be blocked from reclaiming
  pages the dead reader was pinning.
- **`kill -9` of a waiter.** A child takes a ticket and starts
  futex-waiting, is killed before being granted; the next legitimate
  handoff must skip it without hanging (mirrors `writer_queue.zig`'s
  existing dead-waiter rule, re-verified against the region's own
  ticket/futex implementation, not reused by just calling the old
  file-based code).
- **Region corruption / version mismatch.** A truncated, zeroed, or
  version-bumped region must be detected (checksum or magic/version
  fails) and rebuilt, not trusted or silently misread; a process
  opening mid-rebuild must not observe a torn intermediate state
  (bounded by the same one-time open `flock` §8.1 already takes).
- **Fallback mode.** With mmap or the futex self-test forced to fail
  (a test-only hook, not a real platform gap), every suite above still
  passes through the flock-based fallback path, including the
  multi-process and crash tests -- the fallback is not allowed to be
  the untested half of this design.
- **Existing suites stay green**: the multi-process, writer-lane,
  path-lease and admission suites, the full crash sweep (durability
  and stale-base assertions must not fire), vacuum, and backup/restore
  tests.
- **CPU gate.** `lite-write-amplification-cpu`: 0 `open`, 0 `flock`,
  and now 0 extra `pread` per warm write (uncontended steady state),
  *and* a reported lock-syscall count per contended handoff (the futex
  wait/wake pair itself, and the byte-range probe when liveness is
  checked) -- the new number this design adds to the table, since
  "zero per warm write" no longer fully describes cost once a
  contended handoff has its own, now directly measurable, syscall
  price.

### 8.9 Delivery plan: reviewable steps

Each step lands as its own commit, gated green (the suites in §8.8
that already apply to it) before the next starts:

1. The region (§8.1) and the writer lock (§8.2): format, create/open/
   version-check, CAS acquire/release, FIFO ticket, futex wait/wake on
   both platforms, the fallback flag (§8.6) wired but with the reader
   table and liveness still pending. `inode_lock.zig`'s existing
   kernel-lock path keeps running underneath until a later step
   retires it, so this step is additive, not yet a deletion.
2. The reader table (§8.3): slot claim/pin/unpin, the writer's
   oldest-pin scan. `reader_epochs.zig`/`ReaderCoordinator` keep
   running in parallel one more step, so the two reuse-frontier
   answers can be cross-checked against each other before the old one
   is deleted.
3. Liveness and recovery (§8.4): the byte-range locks, the dead-writer
   recovery handoff, the dead-reader pin clear.
4. Migration and deletion (§8.7), one replaced mechanism at a time in
   the table's own order, each its own commit deleting that row's old
   implementation in the same commit that switches its callers over --
   never a commit that only adds the new caller and leaves the old
   mechanism reachable.

## 9. Log-structured commit: redo log and batched checkpoints

Status: design, landing on `perf/lwa-wal`. §9.1-9.2 (the log format,
writer and reader) are additive per §9.9's build order; nothing in
native.zig's or docstore.zig's current commit path changes until step
(c). Coordinate with the two in-flight shared-lock-region efforts
before (c): `perf/lwa-cpu`'s `PathWriterLock`/§8.2 writer-lock
migration and `perf/lwa-lock-readers`' reader fences/epochs (§8.3/§8.4)
both touch `shared_lock_region.zig`, which §9.6 below also extends.

### 9.0 Why revisit the rejected log, now

§2 rejected an in-file redo log, on the grounds that the v8/v9
manifest-based copy-on-write protocol "costs one sync per group, the
same as a WAL, and needs no checkpointer," and reserved revisiting it
"only if the bytes gate below is missed." §7 landed that protocol:
calling-thread syncs/update is ~1.0 in every mode. It did not close
the bytes gate. Measured on the Debug CPU harness (`zig build
lite-write-amplification-cpu`, `write_amplification_cpu_bench_test.zig`),
a primary-only write still costs one fsync but ~12.7 pwrite calls
(~63 KB) and ~10 pread, against §5's target of ≤40 KB/write at 1 writer
and ≤12 KB at 8 (current measured baseline there is "about 60 KB," not
materially improved), and against SQLite's 1-2 WAL frames per commit.
Sync count stopped being the bottleneck; page-level copy-on-write
amplification is now the whole gap. Per commit, today's path rewrites:

- the `document_index` root-to-leaf path (~6.8 pages, via `IndexEditor`,
  native.zig:3146-3558, called from e.g. native.zig:8223-8252,
  9573-9644);
- the allocator_v4 ledger delta page (`State.persist`,
  `allocator_v4.zig:919`, which calls `writeDelta`/`appendDelta`,
  `allocator_v4.zig:1076-1126`, one pwrite per commit even though the
  in-memory ledger already holds the authoritative state -- see §9.4a);
- the v9 commit manifest and the checkpoint slot
  (`tryBuildCommitManifest`, native.zig:13736-13791;
  `publishCheckpoint`, native.zig:13820-13913);
- and it re-reads free-map pages every commit because they are
  deliberately never cached (`admitPageToCache`, native.zig:7854:
  `if (page.len <= 4 or page[4] == @backingInt(PageKind.free_map))
  return false;`; comment at native.zig:7878-7880: "Free-map pages are
  rewritten every commit and read once, so caching them buys nothing").

None of this shrinks by batching fsyncs further; it shrinks only by
batching *page rewrites* across many commits, which needs a log in
front of the pager so a hot page is written once per checkpoint
instead of once per commit. That is this section's subject.

### 9.1 Log placement: a region inside `.aflite`, not a sibling file

**Decision: in-file.** The log lives in a dedicated, append-only page
range inside the single `.aflite` file, addressed from the checkpoint
slot the same way the v9 manifest already is (two new slot fields,
`log_head_page` and `log_tail_offset`, next to `manifest_page` --
native.zig:4121-4146). Log pages are ordinary pages from the same
`allocator_v4`/`PageAllocator` address space; a checkpoint that
supersedes them retires them back to the free list through the
existing retirement path (`State.retire`/`complete`,
allocator_v4.zig:516/553), not a separately managed region with its
own growth/shrink bookkeeping.

**Why not a sibling file**, as `lsm_backend/wal.zig` (the closest
existing reusable implementation -- see §9.2) does with its 64 MiB
rotated segments under a `wal/` directory: that would repeat a
mistake §8's own history already paid down once. The round before §8
moved from `inode_lock.zig`'s per-inode flock registry and
`writer_queue.zig`'s per-ticket-file directory to one mapped region
specifically to cut `open`/`flock`/directory-scan cost to zero in the
steady state (§8's own intro: "a namespace directory per reader
admission" was the thing worth removing). A sibling WAL file
reintroduces exactly that shape -- a second file whose lifecycle
(create, grow, rename, truncate, vacuum, backup/restore, delete) must
track the primary file's, a second `open`/`fstat` on every cold start,
and a second thing for backup/restore (`restore_staging.zig`,
`artifact_source.zig`) and vacuum (`vacuumWithCancel`) to reason about
-- breaking the single-file `.aflite` property §2 names as a hard cost
of any such design. The log this section adds is also small (one
group's redo records, truncated behind each checkpoint, never kept
for point-in-time recovery) and short-lived compared to SQLite's own
WAL or `lsm_backend/wal.zig`'s segments, so the in-file region's extra
page-boundary bookkeeping is worth paying once to avoid a second file.
Revisit a sibling file only if in-file page-boundary handling turns
out to cost more than it saves (unlikely: the record format in §9.2
already tolerates spanning a page boundary the same way any other
multi-page native.zig write does).

### 9.2 Record format: a sequence-numbered, checksummed redo record

Modeled directly on `lsm_backend/wal.zig`'s record shape
(`wal.zig:40-43`: `magic:u32, version:u16, header_len:u16,
payload_len:u32, crc32:u32`, CRC over payload only) and its
torn-tail-vs-corrupt distinction (§9.5), with one field added for
redo's own needs -- the commit sequence is folded into the header
rather than left to the payload, so a reader can validate sequence
continuity without decoding the payload at all:

```
offset  bytes  field
0       4      magic ("ARDO")
4       2      log_format_version
6       2      header_len (future-proofing, as lsm_backend/wal.zig does)
8       4      payload_len
12      8      commit_sequence (same LSN already in use: native.zig's
               `activeCheckpoint().commit_sequence`, docstore.zig's
               `MutationRequest.committed_sequence` and
               `Store.durableSequence()` -- a redo record's sequence
               number IS the commit it belongs to, not a second,
               log-private counter)
20      4      crc32 (`antfly_hash.Crc32.hash(payload)`, payload only --
               same choice as lsm_backend/wal.zig, so a record that is
               merely short (expected after a crash) can be told apart
               from one that is complete-length but wrong (real
               corruption), per §9.5)
24      -      payload
```

`payload` is one encoded `RedoGroup`: an entry count, then one
`RedoEntry` per contribution the group's `commit_participant.List`
(commit_participant.zig:277-324) staged this commit -- primary
document mutations, index-catalog records
(`View.putIndexCatalog`), replay/progress markers
(`View.putReplay`/`deleteReplay`), and, new in this design, allocator
deltas (§9.4a). Each `RedoEntry` is self-describing (kind tag +
length), so the checkpointer (§9.4) can dispatch by kind without
decoding entries it doesn't own, matching `commit_participant.zig`'s
existing allow-listed-key-kind posture (commit_participant.zig:35-50).

`log_format_version` is independent of native.zig's `format_version`
(=5) and `indexed_format_version` (=9, native.zig:1101-1102); see §9.5
for the on-disk format bump this design needs overall.

**Module boundary.** This format, its encoder, and its reader are a
new, small, standalone module (`storage/lite/redo_log.zig`) with two
dependencies: `antfly_hash.Crc32` (`zig/lib/hash/src/mod.zig`, reused
unchanged -- no new checksum algorithm) and `std.Io` for the actual
`pwrite`/`fsync` calls once wired into native.zig. It has no
dependency on `NativeFile`, `PageAllocator`, or `docstore.Store`,
so it is unit-testable with `zig test` directly (step (b) below),
and is reviewable independent of the integration in step (c).

### 9.3 Writer: one group, one append, one fsync -- same durability
boundary, different payload

The group-commit machinery in `docstore.zig` does not change shape.
`applyMutationGroup` (docstore.zig:3732) still opens one native
transaction, runs every member's `apply` under its own savepoint, and
still ends in the single durability call that is today's fsync point
(`commitTransactionWithDurability`, docstore.zig:3812 ->
native.zig:13587). What changes is what that call does for its
members' writes:

1. Every `commit_participant.Participant.stage` call appends a
   `RedoEntry` to an in-memory `RedoGroup` builder instead of driving
   `IndexEditor`/`PageAllocator` to copy B-tree path pages immediately.
2. `flushTransactionWrites`'s role shrinks to: encode the `RedoGroup`,
   append it as one `RedoRecord` to the in-file log region (via
   `redo_log.Writer.append`, one or more `pwrite`s only if the record
   spans a page boundary -- the same "preallocated extents" growth
   native.zig already does, no new primitive).
3. One `syncIfRequired`/`syncData` call -- the durability barrier does
   not move; this is still the commit's only fsync.
4. Publish: advance the in-process overlay's visible tail to this
   record's `commit_sequence` (§9.3a) and the cross-process WAL-index
   tail in the shared lock region (§9.3b), in place of today's
   `publishCheckpoint` slot write.

Ride-along and prefix-ordered durability (§3.2a) carry over unchanged,
because both are properties of the queue and of "the next durability
barrier covers every earlier append" -- true of a redo log exactly as
it is true of a copy-on-write checkpoint. `stageRideAlong`
(docstore.zig:3344) still appends with `durable=false`;
`applyMutationGroup`'s `durable = durable or item.durable` fold
(docstore.zig:3750-3756) still decides whether this append's fsync
runs; a ride-along member's record sits in the log, unsynced, until
some later group's `durable=true` member forces the sync that covers
it -- recomputable work stays recomputable, nothing here changes which
work is safe to lose (the table in §3.2a is unchanged). Prefix order
is still structural (strict FIFO drain, one leader at a time,
`drainMutationBatchLocked`, docstore.zig:3515) and is now also literal:
the log is physically a sequence, so "the applied set after recovery is
a prefix of apply order" becomes "recovery replays a prefix of the
log, stopping at the first invalid record" -- the same guarantee,
restated one layer lower, in §9.5.

### 9.3a In-process overlay: readers see committed-but-unchecked state

A reader on the same `NativeFile`/`Store` must see every commit whose
group has appended and (if durable) synced its redo record, even
before a checkpoint has turned that record into tree pages. This is
an in-memory structure, not a second index: a small ordered map from
page id (or document-index key range, for the common small-group case)
to the pending `RedoEntry` that last touched it, keyed by
`commit_sequence`, consulted by `IndexEditor`'s read path before
falling through to a checkpointed page. It is populated at step 4 above
(the same moment `publishCheckpoint` used to flip `active_checkpoint`)
and trimmed by the checkpointer (§9.4) once a batch of records has been
turned into durable pages -- never trimmed by a reader, so a reader
holding an old pin (`ReadPin.sequence`, docstore.zig:1488-1493) still
sees the overlay entries its pin requires, exactly as it would still
see un-reclaimed pages today under the existing epoch/reuse-frontier
rule (§3.1's "a page may be reused only if it is free in the last
durable checkpoint and its retirement epoch is ≤ the durable epoch").

### 9.3b Cross-process WAL index: `shared_lock_region.zig` (landed, build step (g))

A reader in a different process cannot see this process's in-memory
overlay, so it needs an equivalent signal in the one thing every
process already maps: `shared_lock_region.zig`'s shared region
(`<db>.aflite-lock`, mmap'd `MAP_SHARED`). As implemented, this is a
single consistent triple, not the two independently-read watermarks
originally sketched above (the durable-only vs. tail-visibility
distinction turned out not to matter in practice: every publish in
this design runs strictly after the durable sync, never before, so
there is only ever one watermark worth publishing):

```
log_checkpoint_sequence: Value(u64)  -- "C", the on-disk checkpoint
                                         sequence this log is currently
                                         relative to (what a fresh
                                         reopen's redo_log.replay would
                                         use as start_after_sequence).
                                         Advances only when a real
                                         checkpoint lands
                                         (checkpointRedoLog), never on
                                         an ordinary commit.
log_durable_sequence: Value(u64)     -- the highest commit_sequence
                                         durably appended (synced).
log_durable_offset: Value(u64)       -- byte offset, relative to the
                                         log's own base_offset (read
                                         directly from the data file's
                                         own header -- never duplicated
                                         here), of the durable end of
                                         the log.
log_generation: Value(u32)           -- a seqlock word guarding the
                                         three fields above as one
                                         consistent snapshot: bumped
                                         odd before the three plain
                                         stores, even after. A reader
                                         retries (no syscall, a bounded
                                         spin) whenever it observes an
                                         odd generation or a generation
                                         that changed between its two
                                         reads of it.
```

Growing `Header` from 64 to 96 bytes bumped `format_version` 2 -> 3
(same "an old binary's region is simply rebuilt" policy every prior
version bump already used; none of the new fields are covered by
`header_checksum`, which only ever covered the *immutable* identity
span, so this didn't change that function at all). `Region.publishLogState`/
`Region.readLogState` are the writer-side publish and reader-side read,
both pure atomic stores/loads into the already-mapped region -- no new
syscall on the write path, exactly as required.

**The hook.** `NativeFile.publishRedoLogState` reads `self.redo`'s
current state and publishes it; called from exactly one place on the
write path, `publishCheckpoint`'s redo branch, immediately after
`redo.durable_sequence = checkpoint.commit_sequence` -- i.e. strictly
after the append's own `sync()` already returned, never before, so a
cross-process reader can never observe a record that isn't durable
(the crash-sim scenario this guards: "crash between the log fsync and
the watermark store," §9.7 point 3). Ride-along/deferred appends
elsewhere in `commitTransactionWithDurability` are unsynced and
correctly never call this hook at all -- their own eventual
`durable=true` commit's call covers them, same as every other
ride-along guarantee in this design. `checkpointRedoLog` also calls it
once, at the end, after deciding whether reclaim could proceed (below).

**Interaction with reader pins.** §8.3's reader-slot table
(`ReaderSlot.pinned_sequence`) is the mechanism: a cross-process
reader's `refreshCrossProcessRedoOverlay` claims a slot
(`claimReaderSlot`) once, lazily, and pins it *before* reading anything
new (`pinReader(slot, its own already-applied durable_sequence)`),
reads the published triple, applies whatever is new via
`redo_log.replay` + `redo_overlay.SharedRedoOverlay.decodeLogicalMutationsPayload`/
`decodeRedoGroupPayload` (under a new `inode_lock.withRedoOverlayLock`,
since those two decoders are deliberately unlocked and this call
reaches them *after* the overlay already exists, unlike
`ensureRedoOverlay`'s own first-population case), then advances its pin
to match. `checkpointRedoLog`'s own log-prefix reclaim (§9.4 point 4)
is gated on `minimumPinnedSequence()`: if a live cross-process reader
is pinned below the checkpoint that just landed, the physical
zero/reset of the log's stale prefix is deferred (not skipped forever)
-- the real checkpoint still lands durably either way, only space
reclamation waits. The writer keeps appending past the un-reclaimed
prefix instead of resetting over it, which only works because
`redo_log.replay`'s superseded-record handling (§9.2's module,
generalized for this) tolerates a run of superseded records (including
internal gaps -- see below) followed by genuinely new ones, not just a
superseded run with nothing after it.

**The consumed-but-unlogged sequence gap.** A real checkpoint commit
(`checkpointRedoLog`'s own `checkpointing = true` branch) consumes one
`commit_sequence` for itself without ever appending a log record for
it (that branch disables the whole redo-append path for its one
commit, going through the ordinary non-redo publish instead). Under
the original always-reset design this was invisible (the log reset to
empty immediately after, so the next record was simply the first thing
in a fresh region). Once reclaim can be *deferred*, this gap becomes a
real, recoverable artifact in the log's own bytes: `redo_log.replay`
enforces strict `+1` continuity among genuinely new records and at the
very first record when nothing precedes it, but *not* among superseded
records (safe, since a gap there is unobservable -- nothing superseded
is ever delivered) and only requires a transition-point record to be
*greater* than whatever was last scanned (tolerating however many
checkpoints' worth of consumed-but-unlogged sequences accumulated
while reclaim stayed deferred). `checkpointRedoLog` mirrors this on the
writer's own live `redo.writer`: when deferring, it advances
`redo.writer`'s `last_sequence` to the new checkpoint's own
`commit_sequence` (via `Writer.initAt` with the *same*, unreset `tail`)
so the next ordinary commit's own append does not fail the writer's
own continuity check.

**Re-open semantics.** If the published `log_checkpoint_sequence` has
advanced past what a cross-process reader's own `redo.checkpoint_sequence`
reflects, the real on-disk checkpoint slot (roots, page_count,
manifest -- not just a sequence number) has moved, and the reader must
re-read it before anything else: `refreshCrossProcessRedoOverlay`
reuses `loadCheckpointSlots`/`recoverActiveCheckpoint`/
`adoptIndexPublicationHeader`, the exact same mechanism
`refreshIfWriterBaseStale` already uses for the in-process stale-writer
case, just triggered by the region's published watermark instead of
`inode_lock.committedSequence`. Either way (checkpoint moved or not),
once the overlay is caught up to the newly-read log bytes, the
reader's own `self.header.checkpoints[active].commit_sequence` is
bumped to the new `durable_sequence` -- the same "provisional, no real
pages touched" trick an ordinary ongoing redo commit already applies
to its own writer -- so `redoOverlayEntry`'s `checkpoint.commit_sequence`
bound actually sees what this refresh just applied.

### 9.3c Scan and iterator paths: merged overlay cursors

Point gets alone are not enough: every range scan, prefix scan, or
ordered cursor over a redo-log file's metadata/index/documents roots
must also merge the in-process overlay, in the same key order, gated
on the same pinned-checkpoint check `redoOverlayEntry` already uses. A
scan that misses an overlay put, or returns an overlay-deleted key, is
a correctness bug, not a performance gap.

`OverlayScanState` (native.zig, nested in `NativeFile`) builds, once
per cursor construction, a sorted snapshot of one root's
`logical_overlay` keys restricted to the scan's prefix (borrowed key
slices, no copies -- safe because nothing mutates the map for a
cursor's lifetime: no commit runs concurrently with a cursor's use on
the same handle). `CatalogCursor.next()`, `LiveRecordCursor.next()`,
`hasLiveDocumentOutsidePrefix`, and
`snapshotDocumentsWithPrefixAtCheckpointAlloc` each run the same
merge-join: compare the overlay's next key against the tree cursor's
next key, take the smaller, and on a tie let the overlay (always the
newer state) supersede the tree's stale entry, consuming both.
`metadataCatalogCursor`/`indexCatalogCursor`/
`indexCatalogDirectoryCursor` all build on `catalogCursor`, so they
inherit this for free.

Two bugs surfaced building this, both caught and fixed with regression
tests, not just by inspection:

1. **Lost tree-side lookahead on ownership transfer.** When a
   merge-join holds the tree cursor's already-fetched candidate across
   loop iterations (so a tie, or an out-of-order overlay key, doesn't
   silently skip it), the candidate's owned key must be detached from
   the lookahead slot *before* the next fallible call (typically
   `cursor.next()`), not after. `CatalogCursor`/`LiveRecordCursor` null
   their lookahead field immediately after reading it, before doing
   anything fallible, which is correct.
   `snapshotDocumentsWithPrefixAtCheckpointAlloc`'s equivalent
   lookahead did not: it mutated a *local copy* of the entry rather
   than the actual `pending_tree` variable the function's own
   top-level `errdefer` reads, so a failure in the immediately
   following `cursor.next()` call double-freed a key already
   transferred into (and counted in) the result list. Fixed at both of
   that function's ownership-transfer sites.
2. **`fetchPut` does not update the key on a match.**
   `RedoCommitState.appendLogicalMutation` used
   `logical_overlay[root].fetchPut(allocator, owned_key, decoded)` to
   both insert and replace entries, and freed the returned `old.key`
   on a match. `std.HashMapUnmanaged.fetchPutContext` only overwrites
   `value_ptr.*`, never `key_ptr.*`, when it finds an existing match --
   so `old.key` *is* the key still live in the map, and freeing it
   corrupts the overlay the moment the same key is written a second
   time within one uncheckpointed generation (an ordinary
   put-then-overwrite or put-then-delete, not an edge case). Fixed by
   switching to `getOrPut` and only storing the new key when there
   wasn't already one to keep, matching the stdlib's own idiom for
   this situation. This was a live memory-corruption bug that predates
   the scan work; it would have corrupted the first production
   workload that updated the same document twice before a checkpoint.

New tests (native.zig): `"lite redo log document scan merges the
overlay across put, overwrite, delete and a checkpoint boundary"`,
`"...catalog cursor merges the overlay..."`, `"...vacuum captures
uncheckpointed overlay and demotes the file rather than corrupting
it"`, `"...openIndexValue (artifact lease resolution) sees an
uncheckpointed overlay entry"`.

### 9.3d Per-subsystem redo-log audit

For every subsystem that touches native.zig's three roots, the
decision is one of: it already reads/writes exclusively through the
overlay-aware point-get/cursor family above; it must force a
checkpoint first; or it is explicitly out of scope today. Audited:

- **Full-text, dense (HBC), sparse, graph** (`backend.zig`'s
  `.native_catalog` registrations, `native_catalog_sink.zig`,
  `native_catalog_backend.zig`): all four route through
  `native_catalog_backend.Backend`, which keeps its own in-process
  ordered-tree mirror, seeded once at `Backend.init()` via the
  overlay-aware `indexCatalogCursor`, and thereafter commits through
  `putIndexCatalogBatch` -- the same `TransactionWrites`/
  `logical_overlay` staging path as every other root. **Works through
  the merged paths already.** `index_storage.zig`'s virtual-filesystem
  reads for these four delegate to the already-patched
  `getIndexCatalogRecord*`/`indexCatalogCursor`/
  `indexCatalogDirectoryCursor` family.
- **Algebraic indexes** (`storage/db/algebraic/`): no direct
  `native.zig` access at all -- durable state goes through DocStore's
  own primary `RuntimeStore`/`Txn` path (the `.documents`/metadata
  point-get and scan family). **Works through the merged paths
  already.**
- **Artifact leases** (`artifact_source.zig`): writes stage ordinary
  catalog mutations (already diverted automatically). Reads resolved
  their external-value pointer through `NativeFile.openIndexValue`,
  which called `lookupCatalogPage` directly with no overlay check -- a
  real, live gap (`Registry.open`/`Capture.apply` call it on every
  lease, not dead code). **Fixed**: `openIndexValue` now checks
  `redoOverlayEntry(.index, ...)` first, matching its sibling
  functions. This also forecloses a second, currently-unreachable use
  of the same gap in `persistent.zig`'s mmap `SegmentFileStore` path
  (dead today because Lite always configures `main_backend =
  .native_catalog` for text/dense/sparse/graph, but would have been
  live if that ever changes).
- **Vacuum**: `copyVacuumCatalogRecords`/`copyVacuumDocumentRecords`
  already scan via the overlay-aware `LiveRecordCursor`, so the
  compacted file's *content* correctly includes uncheckpointed overlay
  state without forcing a checkpoint first. But `prepareVacuum`'s
  compacted header never reserved a redo-log region, and
  `replaceWithPreparedGeneration` never reset the live handle's
  `self.redo`/`self.manifest_commits_enabled` to match -- so vacuuming
  a redo-log file left `self.redo.writer` pointing at the old file's
  now-swapped-away fd and stale byte offsets, and the next commit
  would have diverted into a log region that no longer existed,
  corrupting the compacted file. **Fixed, by explicit demotion**:
  vacuum now clears `self.redo` (freeing the overlay, safe because
  every entry already reached the compacted file through the scan
  above) and restores `self.manifest_commits_enabled = true` after a
  successful publish. **Decision**: vacuum intentionally demotes a
  redo-log file to an ordinary (non-redo) file rather than preserving
  or re-reserving a log region in the rebuilt layout -- re-enabling
  redo-log mode after a vacuum requires an explicit, separate
  `enableRedoLog` call (not implemented). This is a known, deliberate
  limitation, not a correctness gap. Regression test: `"...vacuum
  captures uncheckpointed overlay and demotes the file rather than
  corrupting it"`.
- **Backup and restore**: the seal mechanism
  (`native_backup_seal.zig`, `native_backup.zig`,
  `backup_bundle_io.zig`/`backup_bundle.zig`) is byte/file-level
  (hardlinks plus bounded copies), opaque to page/catalog structure
  and to "redo" entirely. Since the log region lives inside the single
  file's own bytes, a byte-level copy captures it along with
  everything else, and `openWithRuntime`/`openWithIo` unconditionally
  run `recoverRedoLog` whenever the decoded header carries a non-zero
  `redo_log_capacity`, regardless of how the file arrived at that
  path. **Works by construction**, not via the overlay-aware paths --
  nothing to force-checkpoint for correctness. Open, not yet resolved:
  whether the backup-pin/seal quiesce point freezes writers before its
  stat+copy or merely races a live writer the same way crash recovery
  does (if the latter, no worse than what `recoverRedoLog` already
  tolerates, but unverified for this specific case); and whether to
  offer an optional forced checkpoint before sealing purely to make the
  captured image replay-free for tooling that doesn't understand the
  redo-log wire format (an optimization, not a requirement).
- **Split**: Lite's native single-file store has no shard/range split
  machinery of its own; the `split`-named B-tree-node-overflow handling
  inside `IndexEditor` is an unrelated concept. Real range/shard split
  lives in `storage/lsm_backend.zig`, a separate SST-based engine that
  never constructs a `NativeFile`. **Not applicable.**
- **`check()`**: the existing `checkpointPrefixSize`/`truncated_file`
  guard (page_count vs. physical file size) already catches, bluntly,
  any uncheckpointed commit that allocated new pages (external-value
  chains, the one page-shaped exception the diversion still makes) --
  a known, already-documented limitation. Newly found: `liveStats()`
  (feeding `live_file_count`/`live_bytes`/`compact_size`/
  `reclaimable_bytes` into `CheckReport`) *is* overlay-aware via
  `LiveRecordCursor`, while `record_count` comes from a raw,
  non-overlay-aware tree walk -- so a redo-log commit that only
  overwrites/deletes existing keys (no new pages, so the truncation
  guard never fires) produces an internally inconsistent
  `CheckReport` (`record_count` stale, `live_bytes`/`live_file_count`
  current) rather than a hard failure. **Documented, not yet fixed**:
  `check()` needs its own explicit decision -- skip/flag when
  `self.redo != null` and the overlay is non-empty, or make
  `record_count` overlay-aware too -- before redo-log becomes the
  default; tracked as follow-up for step 3.

This resolves point 2 in full except for the two explicitly open items
above (backup/restore quiesce-ordering verification, and `check()`'s
`record_count`/`liveStats` inconsistency), both documented decisions to
revisit, not blockers left unexamined. `check()`'s inconsistency is
fixed as part of point 3 below (`NativeFile.redoOverlayEntryCount`,
added into `record_count` in both the indexed and non-indexed branches
of `checkAtFileSizeWithCancel`).

### 9.3e A second occurrence of the same key-ownership bug, in replay

`appendLogicalMutation`'s `fetchPut`-vs-`getOrPut` fix (§9.3c) has a
sibling: `decodeLogicalMutationsPayload`, the function `recoverRedoLog`
uses to rebuild `logical_overlay` from the log on open, had the
identical bug (`fetchPut`, then freeing its returned "old" key as if
it were a separate allocation from the map's own live key). It did not
surface until point 3's work below actually exercised recovery against
a log whose replayed range covers more than one record for the same
key -- an ordinary put-then-overwrite or put-then-delete before the
next checkpoint, same trigger as the original bug, just on the replay
path instead of the live-commit path. Found via `docstore.zig`'s own
`performVacuum`, which opens a fresh read-only `NativeFile` on the same
path (triggering `recoverRedoLog` independently of the live handle);
closing it then double-freed. Fixed the same way: `getOrPut`, storing
the new key only when there wasn't already one to keep.

### 9.4 Background checkpointer: batched, owner-only, ride-along

The checkpointer turns a batch of log records into tree pages,
amortized across many commits instead of once per commit. It runs
only on the leader/owner (the existing rule: "Only the active Raft
leader may run reconciliation and background mutation work," and
Lite's single-process equivalent, the path-lease/writer-word holder),
using the same ride-along and maintenance machinery §3.2a/§3.5 already
built for exactly this kind of demand-driven, recomputable-if-lost
background work:

1. Triggered by the existing owner maintenance turn (the services
   `serviceRetirement` and friends already run from,
   docstore.zig:4126) or an idle-flush/byte-threshold backstop
   matching `rideAlongFlushLoop`'s existing ones (docstore.zig:3453,
   3361) -- not a new timer primitive.
2. Reads the unchecked prefix of the log (from the last checkpoint's
   covered sequence to `log_durable_sequence`), applies each
   `RedoEntry` to the real page structures (`IndexEditor`,
   `PageAllocator`, allocator_v4 -- the same code paths that run
   inline today, just invoked from the checkpointer instead of from
   `flushTransactionWrites`), and batches writes so a page touched by
   many records in the window is written once, not once per record --
   this is the entire point: the ~6.8 document_index pages a hot write
   path revisits commit after commit become one write per checkpoint
   window instead of one per commit.
3. Commits the resulting pages as a ride-along group
   (`Store.stageRideAlong`, docstore.zig:3344) -- the checkpoint itself
   never pays its own fsync; it rides the next foreground group's
   durability barrier (or the idle-flush timer, same backstop as every
   other ride-along user). If no foreground write ever comes, the
   existing idle-flush timer still drains it, per §3.2a's table: lost
   checkpoint work is always safe to lose, because the log records it
   was built from are still there to replay.
4. Truncates (or recycles, via the ordinary page-retirement path,
   `State.retire`, allocator_v4.zig:516) the log prefix the checkpoint
   now covers, but **only** once (a) the checkpoint commit itself is
   durable and (b) `minimumPinnedSequence()` (§9.3b) is past that
   prefix -- a reader pinned to an older sequence must still be able to
   read the log records the checkpoint consumed, because it may be
   reading through the overlay rather than the checkpointed pages.

This is a direct application of §3.2a's "ride-along members are safe
to lose because they're recomputable" rule to a new kind of background
work: a checkpoint pass that crashes mid-way is simply redone from the
log on the next pass, same posture as retirement, reclamation, and
merge today.

#### 9.4a Allocator state: already resident, delta moves into the log

Point 4 of this design ("allocator_v4 and free-space state become
resident in memory") turns out to already be mostly true one layer
down: `allocator_v4.State` (allocator_v4.zig:136-148) -- `counts`
(the ledger), the hierarchical `bitmap` (the free-map, rebuilt from
`counts` in memory, never itself read from or written to disk,
`setBitmap`, allocator_v4.zig:441), and `pending` (the retirement
queue) -- is already a full in-memory mirror, loaded once
(`State.load`, allocator_v4.zig:748) and mutated directly by
`retain`/`release`/`retire`/`complete`. The actual per-commit cost is
`persist()` (allocator_v4.zig:919) choosing, every commit, to append a
`"L4DL"` delta page via its own dedicated `pwrite`
(`writeDelta`/`appendDelta`/`flushDelta`, allocator_v4.zig:1076-1126)
rather than folding that delta into whatever else the commit is
already writing durably.

Under this design, that delta becomes one more `RedoEntry` kind in the
commit's single redo-log append (§9.2), and `persist()`'s own page
write is skipped whenever log-based commit is active for this file.
`checkpointStep`/`checkpointDue` (allocator_v4.zig:942/950) -- today a
foreground-callable full-ledger checkpoint -- become something the
background checkpointer (§9.4) calls instead of something a commit
calls inline. No change to `State`'s in-memory shape or its public
`retain`/`release`/`allocate` API (§ survey Part A.4) is needed; only
where `persist()`'s bytes land changes.

#### 9.4b Step 4, as implemented: riding the existing idle-flush timer

The synchronous 25%-remaining check inside `commitTransactionWithDurability`
(`maybeAutoCheckpointRedoLog`) is kept exactly as designed above --
backpressure for a writer that outruns everything else, never the
primary trigger. The primary trigger is now `docstore.zig`'s
`rideAlongFlushLoop`: the same background thread/timer §3.2a's
ride-along idle-flush barrier already runs (wakes every
`ride_along_idle_ns`, default 50ms), extended with a second,
independent check (`maybeCheckpointRedoLogOnIdleTurn`) on every wake,
regardless of whether the ride-along branch found anything to flush.
Reusing this loop rather than adding a new thread/timer was deliberate:
it already exists, already wakes on a sensible cadence, and already
has the right shape (a periodic tick that's free to be a no-op).

Two triggers, both checked on every wake:

- **Idle**: any non-empty `logical_overlay` (any root) reaching this
  point has gone at least one full `ride_along_idle_ns` tick with
  nothing else pressing enough to wake the loop sooner, so checkpointing
  it now is cheap and bounds the staleness a reopen would have to replay
  to at most one tick.
- **Threshold**: the log's own byte capacity past the halfway point
  (`redo.writer.remaining() < redo.writer.capacity / 2`), checked
  independently of the overlay's *entry* count -- a commit's external-
  value pages consume log bytes without necessarily adding many logical
  entries, so the two signals can disagree, and either one alone
  triggers the same checkpoint.

Starting the loop: `ensureRideAlongFlushStarted` used to run lazily,
only on a file's first `stageRideAlong` call -- a workload that never
rides along (every commit durable and synchronous) would never start
it, leaving only the 25%-remaining backpressure path ever checkpointing.
The fix is **not** to start it eagerly inside `createWithOptions`/
`openWithOptions`: both return `Store` by value, so a thread spawned
there against `&result` captures a pointer to a stack temporary that
goes stale the instant the caller's own copy of the returned value
settles at a different address (found by reproducing it: a real crash,
`@cmpxchgStrong` reading a corrupted `Io.Event` from freed/reused
memory, the first time a test exercised this against a stable
non-redo-log baseline). Instead, `applyMutationGroup` -- always called
through the caller's own long-lived `*Store`, never a temporary --
starts the loop lazily on a file's first real commit, for a redo-log
file specifically. A file with zero commits ever has nothing to
checkpoint regardless, so this loses nothing a workload would need.

### 9.5 Recovery: last durable checkpoint plus a validated log prefix

Recovery becomes: open the on-disk checkpoint (still the v8/v9 slot
mechanism's job -- `recoverActiveCheckpoint`, native.zig:15357-15420,
unchanged in its own slot/manifest validation), then replay the log
from that checkpoint's covered sequence forward, applying each
`RedoRecord` in order, until the first invalid one. "Invalid" is two
different failure modes, deliberately told apart the way
`lsm_backend/wal.zig` already does (wal.zig:1217/1230 vs 1310-1313):

- **Torn tail** (expected after a crash): fewer bytes remain than the
  next record's own length claims, or there simply are no more bytes.
  Benign; stop here, this is the valid prefix.
- **Corrupt record** (not expected, a real bug or media fault): a
  complete-length record whose `crc32` doesn't match its payload, or
  whose `commit_sequence` is not exactly one greater than the previous
  valid record's (sequence continuity, not just "increasing" --
  skipped sequences mean a record silently vanished, which must be
  treated as corruption, not as an acceptable gap). Hard stop at the
  record before it; a build running under the crash-sim harness (§9.8)
  must never advance past this point by accident.

This replaces the v9 manifest's role (`tryBuildCommitManifest`,
`verifyManifest`, native.zig:13736-13791/15280-15337) for the pages a
commit touched *through the log*: the manifest's "list every page
this commit wrote, verify each one's `commit_sequence` and CRC" is
exactly what sequence-continuity-plus-per-record-CRC on the log already
gives for free, one record at a time, without needing a separate
after-the-fact index of which pages belong to which commit. The
checkpoint slot mechanism itself (two dedicated pages, no hint byte,
§3.1) is unchanged -- it is still what anchors "where does log replay
start from."

**Format version bump.** This is a commit-protocol change, so it bumps
the existing version gate the same way v7->v8 and v8->v9 did: a new
`log_format_version` bump refuses an older build's file outright (no
migration -- Antfly is unreleased), checked at the same `decodeHeader`
site (native.zig:14902) alongside `format_version`/`indexed_format_version`.
A v10 file additionally carries `log_head_page`/`log_tail_offset` in
the checkpoint slot (§9.1); a reader built before this change has no
way to interpret those fields and must refuse the file rather than
silently skip the log, matching the existing "a v8 build refuses a v7
file" test (§6, item 12) -- this design adds the equivalent "a
pre-log build refuses a log-format file" test.

### 9.6 mmap read path: evaluated, deferred to step (h)

LMDB-style read-only `mmap` of the data file, serving verified pages
straight from the OS page cache with no `pread`, no copy, and no
repeated CRC, is attractive for the same reason §8 already leans on
`mmap` for the lock region: it is the one mechanism that gets "no
syscall in the steady state" for reads as well as for coordination.
It fits this design cleanly in one respect: copy-on-write already
guarantees a page is never mutated in place once published (only
retired and later reused after the reuse-frontier clears), which is
the one safety property that makes a long-lived read-only mapping of
*checkpointed* pages sound -- a reader's mapping can never observe a
page changing under it, only a page becoming unreachable (still
mapped, but no longer reachable from any root a live reader cares
about) until the mapping is remapped at the next checkpoint it
consults.

It is not free of open questions this design does not resolve yet:
file growth under a live mapping (remapping on growth, same cadence
as §8.1's "region growth" cold path, or a reservation large enough
that growth never needs remapping), whether `msync`/`madvise` hints
are worth the complexity on both target OSes, and how it interacts
with the in-process overlay (§9.3a) for the hottest, not-yet-
checkpointed pages, which an mmap of the checkpointed file obviously
cannot serve. **Recommendation: defer to build step (h)**, after
(b)-(g) land and are gated green. It is a read-path optimization
orthogonal to the commit-path rewrite this section is mainly about,
with its own risk surface, and the CPU-harness pread target (~0,
per the task's gates) is reachable through the overlay and the page
cache alone for the write-path benchmark that gate actually measures
-- mmap only helps a read-heavy workload this change's gates don't
cover. Revisit then with real numbers from (b)-(g), the same posture
§2 and §9.0 already modeled for the log itself.

### 9.7 Crash-sim scenarios this design adds

On top of the 23 existing `crash sweep` scenarios (§7.4,
`crash_consistency_test.zig`), using the same `SimIo`/`CrashPlan`
machinery (`crash_sim_io.zig:84-101`) and the same two permanent
assertions (`assertAcknowledgedWriteIsDurable`,
crash_consistency_test.zig:3210-3221; `assertFreshOnDiskValueContains`,
crash_consistency_test.zig:3235-3263), which must keep not firing:

1. Crash mid-append, before the record's CRC/length bytes land --
   recovery must treat it as a torn tail, not corruption (§9.5).
2. Crash after a complete record lands but before the fsync that
   should cover it -- recovery must not replay it (it was never
   durable), matching `assertAcknowledgedWriteIsDurable`'s own
   "ack implies durable" contract: such a record must not correspond
   to anything the store ever acknowledged.
3. Crash between the log fsync and the cross-process WAL-index
   watermark store (§9.3b) -- a concurrent reader must never observe
   `log_durable_sequence` running ahead of what the log itself proves
   durable after recovery (ordering, not just presence, of the two
   writes).
4. Crash mid-checkpoint (during step 2 or 3 of §9.4) -- the next
   checkpoint pass must redo the same window from the log without
   double-applying or skipping any record (idempotent replay).
5. Crash between a checkpoint becoming durable and the log-truncation
   step (§9.4 item 4) -- recovery must tolerate log records that are
   already reflected in the checkpoint (replay is idempotent, so
   redundant replay is correct, just wasted work, never a correctness
   bug).
6. Crash with a reader pinned mid-log (§9.3a/§9.3b) -- the checkpointer
   must not have truncated anything that reader's pin still needs;
   crash recovery in a *different* process must still honor that
   reader's `pinned_sequence` on reopen.
7. A sustained-writer scenario mirroring §7.1/§7.4's "sustained
   full_index writes survive a crash during ride-along merge and
   retirement," with checkpoint ride-along commits substituted for
   merge/retirement, to catch the same class of idle-flush-timer
   starvation bug §7.1 found, in the new code path.

### 9.6a ReadGeneration visibility, part 1: a versioned overlay (in-process)

A `ReadGeneration` (docstore.zig) wraps a *second*, independently-opened
`NativeFile` that builds its own `RedoCommitState` via its own
`recoverRedoLog` and is never refreshed afterward -- `observeReadCheckpoint`
only updates the wrapper's tracked `CheckpointSlot`, never that second
instance's internal header/overlay. A key written after the generation
opened is invisible through it even though it is definitely live
through the writer's own `self.file` (confirmed by direct reproduction:
`generation.file`'s own checkpoint sequence stays frozen at 0 while the
live writer's reaches 1, and `redoOverlayEntry`'s pinned-checkpoint
guard -- correctly, by its own contract -- then refuses to show the
generation anything). This is not specific to WS5/replay-lanes; it is
general to every read that goes through a `ReadGeneration` on a
redo-log file once more than one write happens without a generation
rotation.

The fix, per the coordinator's design (SQLite's WAL read-mark model): a
versioned overlay, shared per inode per process (owned by
`inode_lock.Entry`, next to `shared_lock_region.Region` -- the same
"one registry entry, one resource, shared by every same-process
opener" shape that field already establishes), where each entry
carries the `commit_sequence` that wrote it and superseded versions
are kept exactly as long as some reader might still need them. A
read generation's view becomes `(checkpoint C, read mark M)`: tree
pages as of `C`, overlay versions with `sequence <= M` (newest such
version per key), with `observeReadCheckpoint`/generation refresh
advancing `M` without reopening.

**Landed so far: the versioned-chain primitive and its single-instance,
point-in-time-correct resolution -- not yet the cross-instance sharing
that actually fixes `ReadGeneration`.** `LogicalOverlayEntry` became
`VersionedOverlayEntry` (`native.zig`): each entry now carries its own
`sequence` and an `older` pointer to the version it superseded, so
`logical_overlay`'s map always points at a key's *newest* version, and
`VersionedOverlayEntry.resolve(at_or_before)` walks the chain to find
the newest version whose `sequence <= at_or_before` -- `null` only
when even the oldest version postdates the query, meaning the real
tree (which had not yet had this key diverted away from it, at that
point) is the correct fallback. `redoOverlayEntry` dropped its old
"must equal the live checkpoint exactly" guard in favor of this walk,
so it is now correct for an arbitrary pinned `CheckpointSlot`, not
just the current one -- proven directly by a new test, "lite redo log
versioned overlay lets a pinned older checkpoint see its own value
after a newer overwrite" (put v1, checkpoint; put v2; read at the
*old* checkpoint sees v1, at the *current* one sees v2, and a
checkpoint from before the key existed at all correctly falls through
rather than finding anything). `appendLogicalMutation` and the replay
decoder (`decodeLogicalMutationsPayload`, now taking the record's own
`commit_sequence`) both push a new version onto the existing chain
instead of replacing it in place. `checkpointRedoLog` still discards
the *entire* chain for every key once its checkpoint lands (not
per-version reclaim yet): every version up to that checkpoint is
subsumed by its own copy-on-write pages, which already give any older
pin the same point-in-time correctness the chain gave it, by the
ordinary mechanism every non-redo-log pin has always used.

Found and fixed one bug landing this (worth recording precisely,
since it cost real debugging time): tagging a version with the
transaction's final `commit_sequence` cannot read
`self.activeCheckpoint().commit_sequence + 1` from inside
`flushTransactionWrites` -- `stageMutation` provisionally increments
`self.header.checkpoints[active].commit_sequence` on *every* staged
mutation (bookkeeping for undo/savepoints, "collapsed to one revision"
at actual commit time), so `self.header` reflects however many
mutations this transaction has staged so far, not the one true final
sequence. The correct source is `self.transaction_header` (the
snapshot taken once at `beginTransaction`, which `stageMutation` never
touches): `self.transaction_header.?.checkpoints[...].commit_sequence
+ 1`.

**Landed since (§9.6b)**: the `inode_lock.Entry`-resident shared
structure itself, per-version reclaim, and a separate, general
in-process reader-pin registry that closes a related but distinct
gap in the page-reuse frontier. See §9.6b for the full account.

### 9.6b Cross-instance sharing, per-version reclaim, and the in-process reader-pin registry

Landed: `redo_overlay.zig` (new, standalone module -- no `native.zig`/
`inode_lock.zig` dependency, avoiding the import cycle `inode_lock.zig`
holding one by value would otherwise create) holds `VersionedOverlayEntry`
(moved here unchanged) and a new `SharedRedoOverlay`: three roots'
worth of versioned maps, a page-shaped-write map (external value
chains only), a `durable_checkpoint_sequence`, and a `pins` multiset
(see below). `inode_lock.Entry` gained a `redo_overlay: ?SharedRedoOverlay`
field, next to `region`, and `ensureRedoOverlay(entry, io, ctx, populate)`:
lazily populates it once per `Entry` (so every `NativeFile` on this
inode -- the live writer and every `ReadGeneration.file` alike --
reference the *same* instance), running `populate` only for the
genuine first opener. `RedoCommitState` (`native.zig`) now holds
`shared: *SharedRedoOverlay` instead of its own `logical_overlay`/
`overlay` maps; every read/write path that touched those (point reads,
`checkpointRedoLog`'s fold/clear, `OverlayScanState`, `readBackingPages`'
page-overlay check, `hasLiveDocumentOutsidePrefix`) now goes through
`SharedRedoOverlay`'s own methods instead.

`recoverRedoLog` is two-pass to make this safe: a cheap, metadata-only
replay runs for *every* opener (using the function's own allocator) to
get `outcome`/the final `CompactCheckpoint` -- every opener needs this
for its own `redo.writer`/`self.header` regardless of who populates the
shared overlay. `ensureRedoOverlay`'s own `populate` callback (invoked
only for a genuine first opener) then runs a second, full replay
directly into the shared structure, using `std.heap.page_allocator`
unconditionally -- the structure outlives, and is shared across,
whichever instance populated it, so it cannot be tied to any one
caller's own allocator (the same rule `inode_lock.zig`'s own doc
comment already establishes for `region`). A naive "replay once with
the caller's allocator, then hand the result to the shared structure"
alternative is unsafe for exactly this reason (allocator-ownership
mismatch across instances) and was deliberately rejected.

`resolveAlloc` (renamed from `resolve`) returns an owned copy of the
resolved value, not a borrowed pointer into the chain: once the
structure is shared across threads/instances, a reference held across
the method's own `unlock` is a real use-after-free risk (a concurrent
`reclaim` could free the exact node just resolved). Every caller in
`native.zig` (`redoOverlayEntry` and its ten-odd call sites) was
updated to free the returned value after use; `LiveRecordCursor.next`
(whose own contract says a returned slice "remain[s] valid until the
next call to `next()`") holds the value in a new `overlay_value` field
instead of freeing it immediately, to match that contract.

**Per-version reclaim** (`SharedRedoOverlay.reclaim`): a version is
dropped once it is both covered by a durable checkpoint
(`durable_checkpoint_sequence`) and strictly older than every live
pin. `checkpointRedoLog` calls it (with the checkpoint that just
landed, which covers everything it folded by construction) instead of
unconditionally clearing every map, since a reader pinned to an older
checkpoint may still need a version this checkpoint's own build also
folded into real pages. When no pin is live at all, a key's chain
collapsing to a single checkpoint-covered version is dropped
*entirely* (the key itself removed, not just trimmed to one node):
nothing could resolve to it instead of the real tree, which already
has the same value as of the fold. (First landed without this last
step -- the chain-trim-to-one-node-but-never-remove-the-key version
left `count()` permanently non-zero after a checkpoint, breaking an
existing integration test that asserted the overlay goes empty with no
readers around; fixed by collecting keys to drop during the map
iteration and removing them afterward, since this map's iterator does
not tolerate a concurrent `fetchRemove` of the entry it is visiting.)

**Pin tracking, a scoped decision**: `SharedRedoOverlay.pin`/`unpin`
are a new, dedicated, in-process-only multiset on the structure
itself, *not* routed through the existing `minimumPinnedSequence`/
`shared_lock_region.zig`/`ReaderCoordinator` cross-process machinery.
Reasoning: that machinery exists to let *coordinated* readers be seen
cross-process; the overlay itself is never visible cross-process at
all (it is a pure in-process cache of logged-but-not-yet-checkpointed
mutations), so only the in-process half is ever relevant to it, and a
narrower, dedicated tracker is safer to land under time pressure than
restructuring code several other subsystems depend on. Wired into
docstore.zig's `pinInitializedReadGeneration`/`releaseReadGeneration`
(the same two functions already pin/unpin the general reader-pin
registry below), guarded by `generation.file.redo != null`.

**A separate, related gap, found while investigating a reported
32-thread/maintenance data-corruption bug** (which turned out to have
an unrelated root cause -- a stale writer instance -- fixed by the
writer-side agent itself): `NativeFile.reuseFrontier`/
`metadataReuseFrontier` already combine four inputs end to end
(checkpoint floors, `epoch_window` from `region.minimumPinnedSequence`/
`minimumMetadataPinnedSequence`, `minimum_reader_sequence` -- itself a
region scan plus the `ReaderCoordinator`'s own in-process inode-only
fence list -- and `Store.reader_frontier`, the `ReadGeneration`/
`ReadPin` mechanism's direct atomic, independent of `region`). Every
reader category traced by direct code reading (ordinary `Txn` reads/
writes via `ReadPin`→`reader_frontier`, artifact leases via either
`ReaderCoordinator.registerFull` or catalog-reachability,
`forkBorrowedRead`'s documented parent-pin lifetime contract) turned
out to already be covered by one of these. One genuine, if narrower,
gap remained: a reader that opens its *own*, plain, uncoordinated
`NativeFile` directly -- `captureFullReadSnapshot`'s own reader
(`coordinated = self.reader_coordinator != null and self.durable_header
== null and !self.read_only`, false in the common case of no
`reader_coordinator` configured at all, which is also vacuum's own
verification snapshot's path, since `performVacuum` calls exactly this)
-- never claims a `region` slot and is a *different* `NativeFile`
instance than whichever one `Store.reader_frontier` happens to be wired
into, so none of the four existing terms reach it.

Fixed with a second new addition to `inode_lock.Entry`:
`reader_pins: std.ArrayListUnmanaged(ReaderPin)` (a `{ data: ?u64,
metadata: ?u64 }` multiset, mirroring `region`'s own data/metadata
split), `pinReader`/`unpinReader`, and `minimumReaderSequence`/
`minimumMetadataReaderSequence` (the in-process-registry's own minimum
alone -- callers combine it with `region`'s answer via `@min`, matching
every other term already in `reuseFrontier`). `metadataReuseFrontier`/
`reuseFrontier` each gained one more `@min` term reading this through
`self.inode_lock_ref`. `captureFullReadSnapshot` pins *before* opening
its own reader, using `self`'s own currently-known checkpoint sequence
as the guard value: whatever the new reader actually resolves to a
moment later can only be the same sequence or later (checkpoints never
move backward), so this is always a safe, if momentarily conservative,
lower bound -- closing the window a concurrent frontier computation
could otherwise run through between "the checkpoint the new reader
will depend on becomes real" and "a pin exists to protect it." The pin
is released in `close()`, before `inode_lock.unref` (which can be the
call that frees the `Entry` this pin lives on). Also wired into
docstore.zig's `pinInitializedReadGeneration`/`releaseReadGeneration`
(redundant with `reader_frontier` for that specific category, but
gives every category one single mechanism to reason about going
forward). A per-inode `last_committed_sequence: std.atomic.Value(u64)`
field was reserved next to `reader_pins` for the writer-side agent's
own stale-writer-instance detection (a different, unrelated fix); not
populated or read by this module.

New test: "lite uncoordinated snapshot reader pins its checkpoint and
survives page reuse" (`native.zig`) -- opens a plain snapshot with no
`reader_coordinator` configured anywhere, confirms its pin registers,
drives many durable overwrites and a bounded number of reclamation
attempts while the snapshot stays open (`reusable_pages` stays `0`
throughout -- note this needs a *bounded* retry loop, not a
`retirementNeedsService()`-driven one, since an externally-pinned
backlog never resolves on its own the way crossing a durability
barrier does), confirms the snapshot still reads its original,
untouched page correctly, then closes it and confirms the same backlog
drains and pages become reusable.

`ReadGeneration` itself is now fixed: WS5's `error.ReplayIndexUnavailable`
failure and the general class it represented depended on exactly this
cross-instance sharing landing.

### 9.6c Point 3 continued: the log-full/OutcomeUnknown cluster

Investigating item 3 (re-running `storage.lite.docstore.test` with the
default temporarily flipped, reverted before committing -- the real
flip is still item 4, gated on (2)) found 6 failures, not the
`error.ReplayIndexUnavailable` case this section originally expected:
five surfaced as `error.OutcomeUnknown` from `redo_log.zig`'s
`error.LogFull` inside `appendRecord`, reached via `publishCheckpoint`/
`publishBareCommit`/`putDocumentBatch`/`putDocument` -- several
existing reclamation/budget tests call `store.file.putDocument`
directly in a loop, bypassing `Store` (and so its background idle-timer
checkpoint trigger) entirely. Root cause: `maybeAutoCheckpointRedoLog`'s
25%-remaining check is reactive, run *after* a commit's own
`appendRecord` already succeeded, to decide whether the *next* commit
needs a checkpoint first -- it cannot protect the commit that is
currently failing.

Fixed (design rule: a log-full condition must be decided before
anything is appended, never surfaced as `OutcomeUnknown`, since it is
knowable in advance): a new `RedoCommitState.pendingRecordLen()`
returns the exact byte length `appendRecord` would write right now
(framing included); both call sites (`publishCheckpoint`'s durable
branch, and `commitTransactionWithDurability`'s ride-along/deferred
branch) check it against `redo.writer.remaining()` *before* calling
`appendRecord` at all. If it doesn't fit, they call `checkpointRedoLog()`
and then return immediately, skipping the append (and the now-stale
`checkpoint`/`next` value's own publication) entirely -- never a
partial append. This is safe specifically because the current commit's
own mutations are already staged in `redo.shared` by the time either
call site runs (`flushTransactionWrites`'s diversion loop always runs
first), so `checkpointRedoLog`'s fold necessarily covers them too:
there is nothing left to log once it returns. If `checkpointRedoLog`
itself fails (e.g. no disk space to allocate the real pages it needs),
that error propagates unchanged -- `redo.shared`/`redo.writer` are
untouched by a failed checkpoint attempt, so the log stays intact and
the caller sees a clean, pre-decision error rather than a torn append,
matching what these tests' own names already expect (deferral, retry,
resume when capacity returns). `checkpointRedoLog`'s signature changed
from an inferred `!void` to an explicit `anyerror!void` to break a
dependency loop Zig's error-set inference otherwise reports once
`commitTransactionWithDurability` calls it directly (it already called
it transitively, through `maybeAutoCheckpointRedoLog`, without issue --
the loop only appears once the call is unconditional on this new path).

Confirmed fixed (3 of the 5): "reclamation writer busy retries gate
image copies until publication is available", "reclamation low disk
defers rewriting and resumes when capacity returns", "reclamation
issue1431 canceled automatic rewrites retry without file growth" all
pass now.

A deeper look at the remaining two found a real, separate bug, not an
unaudited test assumption after all: diagnosed by instrumenting
`commitTransactionWithDurability`/`publishCheckpoint`/`checkpointRedoLog`
directly (temporary `std.debug.print` tracing, removed once the root
cause was confirmed) against "reclamation retries reuse estimates and
reassess changed roots when admitted" in isolation.

**Root cause**: `PageWriteBatch.flushCount` diverts *every* page a
redo-log file writes into `redo.pending`/`redo.shared.pages` whenever
`self.redo != null and !checkpointing` -- not just external value
chain pages, despite that function's own comment assuming so (true
only for a *diverting* transactional commit, whose B-tree roots never
become real pages at all; false for a **bare** commit, which always
builds real document-index/free-map/namespace-directory pages
directly, redo-log or not, and so always diverts them here). A bare
commit never calls `appendLogicalMutation` (confirmed by reading
`putDocumentBatch`'s own branches), so `redo.shared`'s three logical
maps stay empty for it -- meaning `checkpointRedoLog`'s own
precondition (`has_logical_work`, checking only those three maps) is
`false`, and it no-ops, forever, no matter how many times it's called.
The bare commit's own already-fully-computed `next`/`checkpoint`
(correct `document_index_root_page` and all) was never examined by
`checkpointRedoLog` at all -- it operates on a wholly separate
computation, over logical heads, that has nothing to do with a bare
commit's own tree rebuild.

My first version of the LogFull fix (`checkpointRedoLog()` then
`return`) made this far worse: when the preemptive check triggered for
exactly this case, it called the perpetually-no-op `checkpointRedoLog`
and then `return`ed, **discarding the bare commit's own `checkpoint`
entirely** -- the real pages were parked in the overlay, the header
was never updated to reference them, and the commit silently vanished
(no error returned to the caller). Confirmed by direct tracing: across
5 "doc"="changed" bare writes, `self.header`'s own `commit_sequence`
stayed frozen at its pre-loop value every single time, while
`redo.pending`'s staged bytes (freshly re-added each iteration, ~16 KB
-- the diverted document-index/free-map pages) cycled between that
value and zero (cleared by the fix's own cleanup, then immediately
repopulated by the next bare commit's own, equally-discarded write).

**Fix, in three parts**:
1. `checkpointRedoLog`'s own precondition is still logical-only
   (correct in isolation: a bare commit's page-shaped divert has no
   logical counterpart to fold, so there is nothing *for it* to do),
   but it now also explicitly flushes every still-staged page
   (`SharedRedoOverlay.takeAllPagesAlloc`, new) directly to its real
   location before the logical fold -- previously, a page referenced
   by a folded head's `external_value_root_page` was never re-derived
   or re-written by that fold either, so `maybeClearPages`'s own "a
   checkpoint has already folded them all into real, physical pages"
   assumption was never actually made true by anything; now it is.
2. Both preemptive-check call sites (`publishCheckpoint`'s durable
   branch, `commitTransactionWithDurability`'s ride-along branch) now
   branch on `redo.logical_pending.items.len != 0`: non-zero means this
   commit diverted logical content, genuinely covered by
   `checkpointRedoLog`'s fold (unchanged from the first version of
   this fix); zero means page-shaped only (every bare commit, by
   construction) -- flush `redo.shared`'s staged pages directly via
   `takeAllPagesAlloc` + `writePositionalAll`, clear `redo.pending`,
   and *fall through* to the normal append/publish logic instead of
   returning, so `checkpoint`/`next` -- already correct -- gets
   published exactly as an undiverted commit's own would.
3. A new `NativeFile.committedSequence()` (delegated to by `Store.
   durableSequence`, which had the same `activeCheckpoint()`-alone gap
   for the legacy ride-along case already): the logical commit
   sequence, which actually advances on every durable commit --
   `redo.durable_sequence` in redo mode, `durable_header`-aware
   otherwise. "reclamation retries reuse estimates..." now asserts
   against this instead of `activeCheckpoint()` directly (which a
   redo-log commit that only logs, without ever running
   `checkpointRedoLog`, correctly never moves).

Both "stale quota estimates expire after roots shrink" and "retries
reuse estimates and reassess changed roots when admitted" pass with
just this fix -- the budget/physical-size-timing theory originally
written here for the first was wrong; it was the same page-loss bug,
not a separate quota-estimator gap, so no quota/budget-accounting
change was needed after all.

Only "reclamation budget rejects a mutation before exhausting
capacity"'s `error.InvalidNativeFreeMap` (expected
`error.LiteStorageBudgetExceeded`) remains, unchanged by this fix --
most likely free-map handling under redo plus a budget, overlapping
with (2)'s indexed-reclamation work rather than this cluster.

Verified the fix itself does not regress already-landed work:
`-Dtest-filter="redo"` (18/18) and `-Dtest-filter="reuse"` (235/235)
both still pass with the default back to legacy.

### 9.7a Point 3: threading the option, and holding back the default

The option is threaded the whole way: `capi/db.zig`'s
`HostLiteOpenOptions`/`LiteResolvedOpenOptions` (`use_redo_log: ?bool`,
plus `page_reuse: ?bool` since redo-log and indexed reclamation remain
mutually exclusive) -> `backend.zig`'s `CreateOptions` ->
`docstore.zig`'s `CreateOptions.use_redo_log` -> `NativeFile.createWithRedoLog`/
the new owned-io counterpart `createWithRedoLogOwnedIo` (needed because
`docstore.Store.createWithOptions` has an `opts.io == null` path
`createWithRedoLog` itself can't serve -- it requires a caller-supplied
`std.Io`). `null` (unset) and explicit `false` both mean legacy;
explicit `true` alongside `page_reuse` (which defaults to `true`) is a
caller error, not a silent downgrade. Default capacity:
`NativeFile.default_redo_log_capacity_bytes` (8 MiB) -- no prior
sizing precedent existed in this codebase to inherit; this is a fresh,
deliberately generous choice, not a measured optimum.

**The default itself is not flipped to redo-log for every new file**,
despite that being point 3's original ask. Flipping it and running the
existing suite surfaced 13 failures in `storage.lite.docstore.test`'s
reclamation/replay/maintenance tests alone (page-growth-based
assessment heuristics, vacuum/replay-cleanup code that assumes a
commit's pages are already physically on disk by the time retirement
or replay logic looks at them, allocation-failure-injection tests whose
expected failure point shifts with the allocation pattern) -- none of
which §9.3d's audit covered, because they are not "does this subsystem
read/write through the overlay-aware paths" questions at all, but
"does this subsystem's own timing/sizing assumptions survive a commit
that defers its physical page build" questions, a different audit this
pass did not have time to run to completion. Opt-in (`use_redo_log =
true`) is fully supported, covered by the same native.zig/crash-sim
tests as the legacy path, and already demonstrated end-to-end through
the real capi seam by the harness mode below. Only the production
*default* stays legacy until that second audit happens; flipping it
blind was exactly the risk this whole revision's review process (the
logical-mutation rewrite, the overlay/scan merge-join, two
`fetchPut`-vs-`getOrPut` memory-corruption bugs) was already right to
be cautious about once.

The CPU harness (`write_amplification_cpu_bench_test.zig`) gained a
`redo: bool` parameter threaded through `Fixture.open`/
`measureSingleWriter`/`measureConcurrent`, with every pre-existing call
site passing `false` explicitly (so the long-calibrated "ceilings" test
keeps measuring exactly what it always measured, unaffected by the
option existing at all) and a new `"lite write amplification cpu redo
vs legacy"` test measuring both side by side through the real capi
seam. Measured end-to-end (updates=128, text_bytes=1024): legacy
pwrite/write=12.41, pread/write=9.69, syscalls/write=23.09; redo
pwrite/write=2.07, pread/write=0.43, syscalls/write=3.52 -- the same
order-of-magnitude win native.zig's own direct measurement showed,
confirmed surviving the full capi -> backend.zig -> docstore.zig ->
native.zig path, not just the native.zig layer.

**Update:** the default was flipped after all, once the page-shaped
commit fallback (§9.6c) and `NativeFile.committedSequence()` landed and
the reclamation/replay/maintenance failures above were run down and
fixed. `use_redo_log` now defaults to `true` for newly-created files
(`docstore.zig`'s `Store.createWithOptions`); every intermediate layer
listed above still just threads the `?bool` through unchanged.

### 9.7b A self-deadlock in the idle-turn checkpoint error path

With redo as the default, a crash-sweep run appeared to hang at
roughly 10% of one core for over an hour -- slow enough to suspect host
contention (other sessions were loading the same build host) or one of
three latency hypotheses (the ride-along bounded wait, a checkpointer
firing on every commit, or the page-shaped fallback's small pwrites).
`sample`-ing the stuck process twice, minutes apart, showed the
identical stack both times: one thread in
`Store.recordMaintenanceError` -> `PublicationMutex.lock`, reached from
`Store.maybeCheckpointRedoLogOnIdleTurn` <- `Store.rideAlongFlushLoop`,
while the main thread sat in the ordinary commit path
(`Txn.commit` -> ... -> `Store.applyMutationGroup`) waiting on the same
mutex. Not slowness -- a self-deadlock.

`maybeCheckpointRedoLogOnIdleTurn` runs its whole body under
`lockStoreExclusive` (`self.mutex`) and, on a `checkpointRedoLog`
failure (the close-induced `error.OutcomeUnknown` from §9.6c's cluster
reaches this path occasionally), called `recordMaintenanceError` --
which locks that same `self.mutex` again, from the same thread.
`PublicationMutex` is a cooperative, ticketed yield-spin
(`platform_sync.lockYielding`), not a hard OS mutex, so this never
crashed outright: the nested call's own freshly-drawn ticket could
never become "next" while the ticket ahead of it -- this same call's
own, still-unreleased hold -- never advanced either. It just spun
forever, burning a steady trickle of CPU on retry/yield syscalls, which
is exactly what looked like "waiting, not computing" rather than a
hang.

Fix: split `recordMaintenanceError` into a thin `lockStore` wrapper
plus `recordMaintenanceErrorAssumeLocked` (the actual body, for a
caller that already holds `self.mutex`), and have
`maybeCheckpointRedoLogOnIdleTurn` call the `AssumeLocked` form
directly. Audited every other `lockStore`/`lockStoreExclusive`/
`tryLockStoreExclusive` call site in `docstore.zig` for the same
"calls something that re-locks while already holding the lock"
pattern; this was the only instance.

General guard against the whole bug class, not just this one call site:
in Debug/safe builds (`std.debug.runtime_safety`, covering both
`.debug` and `.safe`), `PublicationMutex` now records its owning thread
(`owner: std.atomic.Value(u64)`, a sentinel `maxInt(u64)` standing in
for "unowned" since `std.atomic.Value` cannot wrap a non-pointer
optional) and panics immediately via `assertNotOwnedByCurrentThread()`
if `lock()` is re-entered by that same thread, instead of spinning
forever. Zero cost in release builds (the field is `void` and every
check compiles out).

Regression test: a new `NativeFile.test_fail_checkpoint_redo_log`
failpoint (same disarm-on-first-use pattern as
`test_fail_checkpoint_publication`) fails the next `checkpointRedoLog`
call outright; "lite idle-turn checkpoint failure records the error
without deadlocking the store mutex" forces that failure on the
idle-turn path and asserts, each under its own bounded deadline (never
`.join()`-ing a thread that might still be hung, matching "lite closing
a store with only ride-along work (no foreground write) does not
hang"'s own pattern): the maintenance error is recorded
(`status.last_error`/`.reason`/`.state`), a subsequent commit still
succeeds, and `close()` still returns, all within a few seconds.

Landed as its own commit, separate from the default flip, so the two
can be merged independently.

### 9.7c ReadView: one read primitive

Referred to as "§9.8" in the task that asked for it; filed here instead
to avoid renumbering the existing §9.8 Gates section below, which
callers already cite by number.

#### Why: the overlay-blind-read bug class, found three times

The default flip (§9.7a) surfaced the same bug shape at three different
layers, fixed one at a time as each was found rather than once at the
root:

1. **`docstore.Cursor`** (3715ef1be0/a80c694997): `Txn.openCursor()`'s
   cursor merged only `pending_tree` (a write transaction's own
   in-flight, uncommitted writes), never `SharedRedoOverlay`. Every
   range/prefix scan through the generic `Txn` API -- SQL range reads,
   posting-list walks, replay-journal/derived-index catch-up,
   reclamation and pooling scans -- was affected. Fixed with a
   three-way merge inside `Cursor` itself (`pending_tree` over the
   overlay over the B-tree).
2. **`NativeFile.getDocumentsAtCheckpointWithState`** (304859e1f3): the
   batch document read's own tree walk
   (`readBatchAtCheckpoint`/`resolveBatchReferences`/
   `readDocumentBatchWithPath`) has no overlay knowledge at all --
   unlike the single-key `getDocumentAtCheckpointAlloc`, which checks
   `redoOverlayEntry` first. This is what derived catch-up's own
   document read (`document_collectors.zig`'s `txn.getManySorted`)
   hit: the replay-journal scan (already fixed, via `Cursor`) found a
   new entry, but the document read that follows to materialize it
   could not see the document, surfacing as `error
   .ReplayDocumentNotVisible` escalating into crash-sweep failures
   under the true default. Fixed with an overlay pre-pass ahead of the
   existing tree-based batch path.
3. **`NativeFile.readBackingPages`'s read-ahead window** (the storage
   agent's parallel find, f90ed03487 on `perf/lwa-redo-eos`): the
   multi-page read-ahead window's overlay check covered only the exact
   page being requested, not the rest of the window, so a diverted page
   past the first one in a read-ahead span was read raw, straight past
   EOF (`error.EndOfStream`).

Three call sites, three independent fixes, same root cause each time:
*some* read path forgot to consult `SharedRedoOverlay`, because nothing
makes forgetting a compile error. The redirect behind this section: stop
finding these one at a time. Put the merge in exactly one primitive,
make every reader go through it, and make the raw B-tree accessor hard
to reach any other way.

#### The primitive

```zig
/// The one way to read a redo-log-capable NativeFile: a checkpoint C
/// (the physical B-tree root this view's tree-side reads are pinned to)
/// paired with an overlay read mark M (the logical commit sequence this
/// view's overlay-side reads are pinned to). Captured together, at the
/// same moment, because under redo mode they are not the same number:
/// an ordinary commit advances M (NativeFile.committedSequence's own
/// doc comment) without moving C at all until the next
/// checkpointRedoLog. A ReadView over the *current* generation has C
/// lag M; a ReadView pinned to an older, already-checkpointed
/// generation has C == M (a real checkpoint publish is exactly the
/// moment the two numbers coincide).
pub const ReadView = struct {
    file: *NativeFile,
    checkpoint: CheckpointSlot, // C
    mark: u64,                  // M
    pinned: bool = false,

    pub fn open(file: *NativeFile, io: Io) !ReadView { ... } // current generation: C = file.activeCheckpoint(), M = file.committedSequence()
    pub fn openAt(file: *NativeFile, io: Io, checkpoint: CheckpointSlot) !ReadView { ... } // older generation: C == M == checkpoint.commit_sequence
    pub fn release(self: *ReadView, io: Io) void { ... }

    pub fn getDocumentAlloc(self: *const ReadView, allocator: Allocator, key: []const u8) !?[]u8;
    pub fn getDocumentsAlloc(self: *const ReadView, allocator: Allocator, temporary: Allocator, keys: []const []const u8, buffers: ?[]const []u8, values: []?[]const u8, state: *DocumentBatchReadState) !void;
    pub fn openCursor(self: *const ReadView, prefix: []const u8) !MergedCursor; // docstore.Cursor's three-way merge, moved down here
    pub fn openIndexValue(self: *const ReadView, allocator: Allocator, key: []const u8) !IndexValue;
    pub fn readIndexValueInto(self: *const ReadView, value: IndexValue, offset: u64, out: []u8) !void;
};
```

Every one of the three fixes above is `ReadView`-shaped already --
`redoOverlayEntry(root, checkpoint, key)`'s own version resolution
(`SharedRedoOverlay.resolveAlloc(..., at_or_before)`) is exactly "resolve
at mark M", just currently called with `checkpoint.commit_sequence`
(C) standing in for M at most call sites, which only happens to be
correct when C and M coincide (an older, already-checkpointed
generation, or legacy/non-redo files, where the distinction never
existed). `ReadView.open`'s whole job is computing the *other* case
correctly once, in one place, instead of at every call site that reads.

`docstore.Cursor`/`Txn` and every read API (`get`, `getManySorted`,
`openCursor`, `openKeyCursor`, `openIndexValue`) become thin wrappers
that open a `ReadView` and delegate. The per-site `OverlayScanState`
merges this branch already added (`Cursor`'s own three-way merge,
`getDocumentsAtCheckpointWithState`'s pre-pass) move down into
`ReadView`'s own implementation and collapse into one merge instead of
two.

#### The raw tree becomes a distinct, deliberately-named type

`DocumentIndexCursor`/`CatalogCursor` (native.zig's raw B-tree walkers,
with no overlay knowledge at all) stay exactly as they are internally,
but under a name that says so: `CheckpointedTree`, `pub` only within
native.zig (or exported under a name no other file would reach for by
accident), used only by `checkpointRedoLog`'s own fold, vacuum, and
recovery -- the three callers that correctly want the *physical* tree
on purpose, never the live overlay. Every other reader goes through
`ReadView`, which is the only type with both a `CheckpointedTree`
*and* overlay access. A new read added anywhere else that reaches for
`CheckpointedTree` directly either doesn't compile (if it stays
non-`pub`) or is an obvious, greppable code-review smell (if a name
change is all the guard this gets in a first pass).

#### Recovery before any read

`NativeFile.openWithIo`'s redo-log path (`recoverRedoLog`) already
replays the valid log prefix into the shared overlay before returning
a handle (§9.5) -- this was already correct; `ReadView` does not change
`open()`'s own ordering, it just makes every caller's *first* read
after open go through the same pinned (C, M) pair instead of each
computing its own, inconsistently. Derived workers and catch-up pin a
`ReadView` the normal way (`ReadView.open`, after the file is already
open and recovered), so a worker that starts catching up immediately
after a crash-reopen sees the just-recovered records the same way an
ordinary live reader sees just-committed ones.

#### Retention: pins already exist, nothing new needed

Agreed directly with the storage agent (owns this half, as part of its
`EndOfStream` fix): retention already works through the existing
reader-pin machinery, no new mechanism required.
`SharedRedoOverlay.pin`/`maybeClearPages` already keep a staged overlay
page alive while any pin covers it, and `inode_lock.pinReader` already
feeds `NativeFile.reuseFrontier`, which already gates `drainRetirement`
and reclamation -- confirmed by their own new test, "a pin survives
checkpoint and reclaim: a superseded multi-page external value stays
readable."

`ReadView.open`/`release` pin with exactly the same two calls
`Store.pinInitializedReadGeneration`/`releaseReadGeneration` already
make, in the same order: pin the overlay mark M first
(`redo.shared.pin(allocator, io, M)`), then the checkpoint C
(`inode_lock.pinReader(entry, io, .{ .data = C.commit_sequence,
.metadata = C.commit_sequence })`); unpin in reverse on release. Once
`ReadView` exists, `ReadGeneration` itself becomes either `ReadView`
directly or a thin wrapper over it, so there is exactly one pin type
in the file instead of two parallel ones.

The storage agent's `readBackingPages` read-ahead fix (f90ed03487,
`perf/lwa-redo-eos`, being refined to keep the read-ahead window) lives
in the page-read layer below `ReadView` -- `ReadView`'s own value-chain
reads call through it, but the fix itself doesn't overlap this design.
Merge that branch once it's refined.

#### Delivery order and status

Per the task's own ordering ("start with derived catch-up's document
reads, then batch reads, then everything else"), delivered as targeted
fixes on the existing primitives first (this subsection's own earlier
revision), then the `ReadView` type itself on `perf/lwa-readview`:

- Done earlier: the three fixes listed at the top of this section
  (`Cursor`'s merge, `getDocumentsAtCheckpointWithState`'s pre-pass,
  `readBackingPages`'s read-ahead window).
- Done on `perf/lwa-readview`: `NativeFile.readMark(checkpoint)` /
  `redoOverlayEntryAt(root, mark, key)` (cherry-picked from
  `perf/lwa-wal`'s `467c37d8c1`) -- the actual (C, M) split
  `redoOverlayEntry` needed: every caller used to resolve the overlay at
  `checkpoint.commit_sequence` directly, correct only when the physical
  checkpoint and the logical commit sequence coincide, wrong for the
  live, current generation under redo mode. `readMark` picks
  `committedSequence()` when `checkpoint` is the file's own current one,
  `checkpoint.commit_sequence` otherwise (an already-checkpointed older
  generation, where the two already coincide by construction).
- Done: the `ReadView` type itself (`native.zig`) -- `open`/`openAt`/
  `borrowed` constructors (the first two pin with the exact two calls
  `pinInitializedReadGeneration` makes, in the same order; `borrowed`
  describes a view over a pin some longer-lived caller already holds,
  e.g. a `Txn`'s own `ReadPin`), and read methods (point get, batch get,
  index-value family, metadata/index catalog cursors, and a document
  tree-cursor-plus-overlay-scan pair for `docstore.Cursor`'s own merge)
  delegating to the existing checkpoint-taking primitives with `mark`
  now computed via `readMark` rather than reused from `checkpoint`
  directly.
- Done: `CheckpointedTree` rename (from `DocumentIndexCursor`) with a
  doc comment marking it as intended only for `checkpointRedoLog`'s
  fold, vacuum, recovery, and `ReadView`'s own internals -- kept `pub`
  (not a hard compile-time restriction yet: Zig's privacy model means a
  non-`pub` type's name cannot appear in another file's own field
  declarations at all, which `docstore.Cursor` needs for its tree-side
  field, so fully hiding it would have required the full `MergedCursor`
  abstraction below instead of a rename; deferred, see below). `Txn`'s
  own direct `CatalogCursor`/`OverlayScanState` construction is gone --
  `openCursor`/`get`/`getManySorted`/`valueIdentity`/`valueFingerprint`/
  `openIndexValue`/`readIndexValueInto`/`checksumIndexValue`/
  `readAuthenticatedIndexValue`/`copyIndexValueTo` all build a borrowed
  `ReadView` over the `Txn`'s own already-pinned generation/checkpoint
  and call through it.
- Done: `OverlayScanState.initForPrefix`'s "must equal the live
  checkpoint exactly" gate, removed (now an alias for
  `initForPrefixUnchecked`) -- found it silently emptying the overlay
  for `index_storage.zig`'s directory listing and
  `native_catalog_sink.zig`'s `loadAllAlloc` (the dense/sparse/graph/
  full-text native-catalog seed this section's own §9.3d audit had
  marked safe), both reached through a `ReadGeneration`'s separately-
  opened file with a tracked-but-genuinely-current checkpoint -- the
  exact same bug class as `docstore.Cursor`'s original one, just not
  yet found there. Per-key resolution was already checkpoint-
  independent; only enumeration needed widening.
- Done: `readBatchAtCheckpoint` runs one overlay pre-pass internally,
  shared by every batch read it serves -- plain value gets
  (`getDocumentsAtCheckpointWithState`, now a thin delegate) and every
  score-batch variant (`visitScoresAtCheckpointWithState`,
  `readDecodedScoresAtCheckpointWithState`,
  `visitCompleteScoresAtCheckpointWithState`,
  `visitPreparedScoresAtCheckpointWithState`/`visitOwnedScoresAtCheckpointWithState`)
  alike. The score-reading family had no overlay knowledge at all
  before this (a graph-metric score document logged but not yet
  folded by `checkpointRedoLog` was invisible to a score-batch read
  even though a plain `get()` on the same key found it immediately);
  `visitCanonicalScores`'s own separate empty-tree-only shortcut
  (which bypassed `readBatchAtCheckpoint` entirely) is removed for the
  same reason. Gated on the documents-root overlay actually being
  non-empty (`SharedRedoOverlay.count`), confirmed necessary against a
  real regression: several memory-budget/OOM-injection tests assert
  an exact allocation count for this function and broke without the
  gate, on a file that had `use_redo_log` on but nothing ever
  diverted into the overlay during the assertion in question.
- Done: `native.MergedCursor` -- the two-way (checkpointed tree,
  redo-log overlay) comparison/tie-breaking/lookahead-buffering loop
  extracted (not reinvented) from `docstore.Cursor`'s own original
  three-way merge's "redo vs disk" half, behind the single constructor
  `ReadView.openMergedDocumentCursor`. `CheckpointedTree` is now
  genuinely non-`pub` -- `docstore.Cursor` holds a `MergedCursor`
  instead (its own keys-only/length-only short-circuits still apply to
  `MergedCursor.Entry.disk`'s raw, unresolved `DocumentIndexEntry`
  exactly as before), and `Cursor.resolveMerged` is now a true two-way
  merge (`pending_tree` vs whatever `MergedCursor` returns). Building
  this surfaced a real bug in the process: `MergedCursor.advance`'s
  overlay branch treated a `resolveAlloc` miss as corruption
  (`error.InvalidNativeRedoRecord`), the same (equally wrong) pattern
  `CatalogCursor.next`/`nextRecordAlloc` and `LiveRecordCursor.next`
  still have -- correct only when every overlay-enumerated key's
  version chain predates the view's own mark, which fails for a
  `ReadView` pinned to an older snapshot when the overlay (gate-free
  since the fix above) enumerates a key created only after that
  snapshot. Fixed by resolving the overlay candidate *before* decoding
  whether to consume a tied disk candidate, so a phantom overlay entry
  never discards disk's own correct answer for that view; caught by
  "lite key-only cursor preserves prefix overlay bounds and pinned
  snapshots" (a pinned read opened before a later write adds a
  brand-new key under the same prefix).
- Done: `CatalogCursor.next`/`nextRecordAlloc` and
  `LiveRecordCursor.next`'s identical resolveAlloc-miss-as-error
  pattern, fixed the same way (resolve before deciding whether to
  consume a tied tree-side candidate; re-stash it, never free it, on
  the phantom path). Found genuinely reachable, not just theoretical:
  "lite online vacuum publishes while group-commit mutations keep
  landing" was already crashing on this exact path
  (`LiveRecordCursor.next`, via `performVacuum`'s own `liveStats` call
  on a vacuum snapshot pinned before a concurrent write landed) --
  this fix replaces the crash with the intended skip-not-error
  behavior (that test still fails afterward on a separate, deeper
  vacuum/catch-up timing assertion, confirmed present with the same
  crash before this fix too -- not caused or resolved by it). Test:
  "lite redo log catalog cursor pinned to an older mark skips a key
  created only after it, instead of erroring" (native.zig).
- Not done: `ReadGeneration` has not been collapsed onto `ReadView`;
  it keeps its own pin lifecycle (`ReadPin` linked list, `references`
  counting, retirement, the kernel-level reader fence for
  `externally_locked` stores, and the `reader_frontier` atomic
  `Store.reuseFrontier` reads as a fast path), with `Txn.readView()`
  building a `borrowed` `ReadView` over it per call. Assessed, not
  attempted: each individual pin *could* become its own
  `ReadView.openAt` (the underlying `SharedRedoOverlay.pin`/
  `inode_lock.pinReader` registries are already multisets, not
  reference counts, so one entry per `Txn` is already how they behave
  today), but `Store`'s own `read_mutex`-protected "oldest live pin"
  bookkeeping, generation retirement (swapping the underlying file
  after e.g. a vacuum while old readers finish), and the kernel-fence
  machinery are `Store`-level concerns with no `ReadView`-shaped
  equivalent -- collapsing them would mean removing `Store.
  read_generation`/`ReadPin`/`pinInitializedReadGeneration`/
  `releaseReadGeneration`/`retireReadGeneration` outright and
  rerouting every call site (`openReadWithPrefix`, `openWriteWithPrefix`,
  `openWriteYieldingWithPrefix`, `observeReadCheckpoint`, ...) plus
  reworking `reuseFrontier` to drop `reader_frontier`'s own fast path
  in favor of `inode_lock.minimumReaderSequence` alone -- a store-wide
  architectural change, not a cheap one. Left as the next step, not
  attempted this pass.
- Not done: the reclaim agent's planned redo on/off differential oracle
  against the `ReadView` API.
- Tests added: "lite derived catch-up sees a document..." (earlier
  revision, `crash_consistency_test.zig`); "lite ReadView: get, batch
  get, and a catalog range scan across put, overwrite, delete, a
  checkpoint, and a reopen" (`native.zig`) -- exercises `ReadView.open`
  directly across the overlay/tree boundary a checkpoint creates and
  across a close+reopen (recovery must replay the log before any
  `ReadView` can pin it); "native catalog sink: a mid-stream index
  admission is visible to `loadAllAlloc` before any checkpoint"
  (`native_catalog_sink.zig`) -- the exact seam every full-text/dense/
  sparse/graph native-catalog managed index seeds through and writes
  through, exercising "admit a record mid-stream, no checkpoint" at
  that layer directly, since the real end-to-end `db.addIndex` path
  currently fails on its first call for an unrelated, separately-owned
  write-path bug (a `LogFull`/`OutcomeUnknown` case in index creation's
  own `deleteTree`, traced but not fixed by this branch -- see the
  `perf/lwa-readview` report); "lite redo log score-batch read sees a
  graph-metric score document logged before any checkpoint"
  (`native.zig`) -- put/overwrite/delete/insert across a checkpoint
  boundary, through `getScoresAtCheckpointWithState` and
  `readDecodedScoresAtCheckpointWithState`, confirming the
  `readBatchAtCheckpoint` overlay pre-pass fix above directly.

### 9.7d Retirement signal and accounting under redo (`perf/lwa-redo-retire`)

`NativeFile.retirementNeedsService` loads the on-disk allocator ledger
and asks it "is there eligible retirement work" -- correct under the
legacy commit path, blind under redo: a superseded overlay version
(§9.3a) is retirement debt (an old external-value chain included) that
only `checkpointRedoLog`'s next fold turns into a real, ledger-tracked
`Retirement`. "The overlay has anything at all" was tried as the
signal and reverted: it fires for a plain, never-overwritten bulk
insert too, which supersedes nothing, and broke 13 other reclamation/
retirement tests on contact.

**The signal.** `SharedRedoOverlay.pending_retirement_versions`
(`redo_overlay.zig`) is an O(1) counter, not a scan: `appendVersion`
already does a `getOrPut` per commit to chain a new version onto its
key's existing head (§9.3c's own description of this structure); when
that lookup finds an existing head (`gop.found_existing`), the head
just superseded is one more version a fold would discover and retire,
so the counter increments there, for free. `reclaim`'s existing walk
(dropping versions a floor advance makes unreachable) already visits
exactly the nodes it frees; decrementing there, from `destroyChain`'s
now-`usize` return, keeps the counter an accurate live count with no
second pass. `NativeFile.redoCheckpointWouldRetire` reads it (one lock,
one load) and `retirementNeedsServiceCoreWithCancel` treats a nonzero
count as unconditionally true, the same tier as the existing
`checkpointDue`/`budgetedCheckpointDue` checks: there is no epoch/
reader-fence question to ask about debt that has no `Retirement` item
yet.

**Acting on it safely.** `reclaimPagesImpl`'s existing ledger-only body
(free-map read, `drainRetirement`, a new allocator checkpoint) turns
out to be unsafe to run at all while any page-shaped write (an
external-value chain) is still staged: that content was written to a
real page id without ever touching the ledger (`checkpointRedoLog`'s
own "page-shaped exception" comment), so the ledger's free map does
not know those pages are occupied -- reproduced directly: running this
function's existing body against a redo file with one staged chain
reused that chain's own page id for the new free-map-root page,
corrupting it (`check()`'s `unexpected_page_kind` afterward). The fix
gates the whole body on `SharedRedoOverlay.pagesEmpty`: if anything
page-shaped is still staged, fold first (`checkpointRedoLog`) when
capacity pressure justifies the fold's own real, syncing commit (the
same `reserve_retirement_capacity`/`max_file_bytes` gate
`budgetedCheckpointDue` already uses), otherwise skip the body this
call (the debt is real but the next idle-flush/threshold checkpoint
will fold it for free; forcing a sync here for an unbudgeted ride-along
caller is not worth it).

**Scope deliberately not covered.** A *first-ever* diverted write for a
key already live in the real, on-disk tree (overwrite or delete, never
previously touched in this uncheckpointed window) also creates debt
the fold will retire, but detecting it would need a real tree lookup
per write -- the O(1)-per-commit constraint rules it out, and neither
target test needs it (both start from a key that does not exist on
disk yet). Left for whoever revisits this once a cheap "does this key
already resolve to something real" signal exists elsewhere.

**A second, independent regression found and contained, not fixed.**
Wiring the signal into `Store.startMaintenanceAssumeLocked` (which
decides whether to spin up a *background* maintenance thread) newly
started that thread for a retirement reason under redo for the first
time ever -- previously impossible, since redo debt alone used to make
this whole check always false. Doing so exposed a pre-existing
concurrency bug in the background loop's interaction with a live redo
commit: four crash-sweep/close+reopen regression tests (full-text
index creation/retirement-turn combinations) started failing with
`OutcomeUnknown`, reproduced as present with *only* the signal change
and *no* other part of this patch, i.e. not caused by the
`reclaimPagesImpl` fix above. This overlaps the write-serialization
race already being worked on separately (`perf/lwa-writer-race`,
`publishCheckpoint` and the commit path), so it was not chased further
here. Contained by splitting the check:
`retirementNeedsServiceLedgerOnlyWithCancel` (ledger-only, the
pre-existing behavior, unaffected by any of this) is what
`startMaintenanceAssumeLocked` calls; every other caller -- explicit
test assertions, `maintainOnceWithCancel`'s own foreground-driven
service call, `reclaimRetirementPressure`'s admission check, all
direct/synchronous callers that already control their own timing --
keeps the full, redo-aware `retirementNeedsServiceWithCancel`. Revisit
once the writer-race fix lands: the split may no longer be needed.

**Of the two target tests, one still fails for an unrelated, confirmed
pre-existing reason; the regression tests above are now gated, not
broken.**

- "lite reclamation ride-along passes force no sync before the next
  durable write": the signal and sync-count assertions now pass. The
  final `checkWithCancel().valid` assertion still fails
  (`unexpected_page_kind` before `reclaimPagesImpl`'s own gate above,
  `truncated_file` after it): `checkpointPrefixSize`
  (`page_count * page_size`) has no redo awareness at all, and a
  diverting commit advances `page_count` to hand the *next* diverting
  commit a fresh page id without the file ever being physically grown
  to match (that growth is deferred to the next fold, same as the
  page's own bytes). Reproduced with zero retirement/maintenance calls
  involved at all (eight plain `write.commit()`s, then `check()`
  immediately): a write/fold-path gap, not a retirement/accounting one
  -- likely `perf/lwa-redo-data`'s territory.
- "lite reclamation capacity reserve drains wide value graphs": still
  fails, but earlier and for a different reason than before this fix
  (confirmed by instrumentation, not inferred): at the point it fails,
  `pending_retirement_versions` is genuinely `0` -- the test's 256 KiB
  value exceeds the redo log's own region, scaled down to a quarter of
  the configured budget (§9.7a's own "floored at 64 KiB" comment), so
  the write never diverts into the overlay at all; there is no debt yet
  for the new signal to see. The failure is the pre-existing, legacy-
  shared `budgetedCheckpointDue` ledger-delta threshold not clearing
  with the margin it has under legacy, because the log reservation
  eats into the same budget (confirmed: the identical test passes
  under `use_redo_log = false`, where that reservation does not exist,
  and never even reaches the failing retry branch). A budget/threshold-
  tuning interaction with the log's own capacity reservation, not an
  overlay-debt-visibility problem.

Gate (`--test-filter "reclamation" --test-filter "retirement"
--test-filter "ride-along" --test-filter "ride along" --test-filter
"reuse" --test-filter "redo" --test-filter "prepared"
--skip-test-filter "WS4-for-dense repair-issue ride-along"`, the last
one a confirmed-unrelated pre-existing hang reproduced independently
of this branch): 199 tests, 183 passed / 15 failed before, 185 passed
/ 13 failed after -- every test that passed before still passes; two
of the pre-existing 15 failures ("lite allocator v4 releasing the last
old reader starts idle retirement", "crash sweep: sustained
full_index writes survive a crash during ride-along merge and
retirement") now pass as a side effect; the two target tests remain
among the 13 for the independent reasons above.

### 9.7e Transaction isolation and the large-transaction fallback (`perf/lwa-txn-overlay`)

Two gaps in the redo log's transaction model, found via
`storage.lite.index_storage.test.lite subtree deletion rolls back
earlier private batches on failure` and `...bounds heap across
private batches and preserves snapshots` (`deleteTree`'s subtree walk:
one transaction, many 64-key private flushes).

**Bug 1: aborted/rolled-back writes stayed visible.**
`appendLogicalMutation` pushed each mutation's new version onto the
*shared* `SharedRedoOverlay` (§9.6a/§9.6b) the moment a private batch
flushed -- before the transaction committed, and with nothing to undo
it on `abortTransaction`/`rollbackTransactionSavepoint`. Under the
single-writer model this is invisible to a reader *while* the
transaction is still open (no other commit can have advanced
`committedSequence()` past the still-uncommitted mutation's intended
final sequence yet), but once the transaction aborts, `self.header`'s
sequence counter is never consumed -- the *next* transaction computes
and publishes that exact same sequence number. From that point on, any
reader resolving at or past it finds the abandoned version, because it
was never removed.

Fixed with an undo log, not a change to *when* the push happens
(`RedoCommitState.overlay_undo`): `appendLogicalMutation`/
`appendPending` still push into `shared` immediately (preserving
intra-transaction read-your-own-write for a native-level, direct
`NativeFile` caller like `deleteTree`'s `CatalogDeleteBatch` -- it has
no `docstore.Txn.pending_tree` of its own to fall back on, unlike an
ordinary document write), but now also record enough to find and
remove exactly that version/page again
(`SharedRedoOverlay.removeLastVersion`/`discardPage`, safe under the
single-writer model: nothing else can have appended for the same key
while this transaction is still open, so the current head is always
the node to remove). `abortTransaction` undoes the whole list;
`rollbackTransactionSavepoint` undoes back to a savepoint mark
(`TransactionSavepoint` gained `overlay_undo_mark`, plus
`logical_pending_mark`/`pending_mark` so the eventual commit's own log
record never encodes a mutation the savepoint discarded) --
independent of `writes`'s own generation check, because a private
flush (unlike a legacy page-shaped one) never touches `self.header`'s
root pointers, so the existing checkpoint-restore cannot undo it the
way it undoes a flushed legacy batch. `abortTransaction` also now
clears `logical_pending`/`pending` outright: previously neither was
reset on abort, so a failed transaction's partial record bytes would
have bled into whatever the next transaction logs.

A new test, "lite redo log abort and savepoint rollback discard
overlay versions a private flush already pushed into the shared
overlay" (`native.zig`), covers both paths directly: stage, force a
private flush, abort (or roll back to a savepoint taken before a
second flush); a concurrent `ReadView`, a later transaction, and a
fresh reopen must all see the writes as absent.

**Bug 2: unbounded per-transaction buffering.** `logical_pending`
(the byte buffer `appendRecord` eventually logs) and the shared
overlay's own per-key versions both accumulate for the *whole*
transaction, cleared only at actual commit. `deleteTree`'s subtree
walk is one transaction spanning many private flushes, so a
16,384-small-key delete diverts every key, never converting any of
them to real pages until a checkpoint -- several MB against a 512 KB
test budget.

Fixed with a size-triggered fallback, not a change to the log/overlay
format: `RedoCommitState.large_txn_fallback_bytes` (default 16 KiB --
`default_large_txn_fallback_bytes`) bounds `logical_pending.items.len
+ pending.items.len`. `flushTransactionWrites`'s `diverting` decision
(and `transactionWrites`'s own up-front copy of the same condition,
which must agree for a given private batch -- see that call site's own
comment) adds `!redo.overLargeTransactionThreshold()`: once a
transaction's own buffered record has already reached the threshold,
every *later* private batch in that same transaction takes the
ordinary, non-diverting commit path instead -- the same bounded-per-
batch cost (editor scratch released each flush) the pre-redo-log
design always had, confirmed by this session's own instrumentation
(not merely assumed): the memory-bound test passes at a 4 KiB
threshold, and still passes at 16 KiB, but fails again at 64 KiB,
because the per-key overhead this bound has to cover is larger than
`logical_pending` bytes alone -- the shared overlay keeps its own copy
of the key (inside `VersionedOverlayEntry`) and `overlay_undo` keeps a
*third* copy for undo-tracking (Bug 1's own fix), so a diverted key's
real footprint is a small multiple of what `logical_pending` alone
measures. 16 KiB was chosen with that multiple in mind, not just
re-derived from the 4 KiB passing value, to leave a small transaction
some of the redo log's own I/O savings rather than falling back
almost immediately.

Earlier-diverted batches and later-physical batches coexist correctly
in one transaction: the diverted batches' content is already in
`shared` (publish/undo per Bug 1's fix above) and encoded into
`logical_pending`; the physical batches build real pages and advance
`self.header.checkpoints[active]`'s root pointers directly, exactly as
a non-redo commit does. The final commit's one `appendRecord` call
already carries the *current* `CompactCheckpoint` (reflecting
whatever the physical batches built) alongside the still-small
logical payload, so no separate reconciliation step is needed --
the record format already supported one commit mixing a page-shaped
exception (external value chains) with logical mutations; this
reuses exactly that, just for ordinary B-tree mutations instead.

Explicitly not used for this: `checkpointRedoLog`'s own fold. It opens
and commits its own native transaction (`self.beginTransaction()`/
`self.commitTransactionWithDurability`), which only runs when no
transaction is already open -- `commitTransactionWithDurability`
clears `self.transaction_header` *before* either of its own
`checkpointRedoLogNested()` call sites (§9.6c) run, which is exactly
why those are safe; calling it from inside `flushTransactionWrites`,
mid-transaction, is not available the same way, so the large-
transaction fallback is a per-batch commit-path choice instead of a
mid-transaction fold.

Verified: both named tests pass 3/3 (both their multi-case variants:
4,096/16,384 small keys and 4,096 large keys for the memory-bound
test); the new overlay-undo test passes 3/3; `--test-filter "redo"`,
`"savepoint"`, `"abort"`, `"rollback"`, `"index storage"`, `"subtree"`,
and the redo differential oracle all stay green (see this branch's own
report for exact counts).

### 9.8 Gates (from step (c) on)

Unchanged from the task's own list, restated for traceability:
`antfly-storage-test -Dtest-filter="crash sweep" -- --test-filter
"crash sweep" --allow-empty-test-filter` (assertions must not fire);
full `zig build lite-storage-test`; `capi-contract-test
-Dtest-filter="capi SQL outcome"` and `-Dtest-filter="relational"`;
`zig build lite-write-amplification` (durable commits/write,
syncs/update); the CPU harness (`lite-write-amplification-cpu`) at
pwrite ≤2/write, durable bytes/write about the record size, pread ~0.

### 9.9 Build order

(a) this section; (b) `storage/lite/redo_log.zig` -- format, writer,
reader, unit tests, torn-tail and corruption tests, no integration;
(c) the overlay (§9.3a) and log-based commit integration in
native.zig/docstore.zig behind the §9.5 format bump -- sequenced after
telling the two concurrent shared-lock-region efforts, since (c) reads
(but, until (g), does not yet write) `shared_lock_region.zig`; (d) the
background checkpointer (§9.4) and log truncation; (e) the resident
allocator delta (§9.4a); (f) recovery (§9.5) plus the crash-sim
scenarios in §9.7; (g) the cross-process WAL index (§9.3b) in
`shared_lock_region.zig`, landing after §8's steps 1-3; (h) the mmap
read path (§9.6), optional, evaluated first against real numbers from
(b)-(g).
