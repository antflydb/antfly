# Dense Indexing Lifecycle: Implementation Log

> Summary of a verbatim log. Full text: [docs/design/dense-indexing/history/lifecycle-implementation-log.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/dense-indexing/history/lifecycle-implementation-log.md). Originally relocated verbatim from zig/DENSE_INDEXING_LIFECYCLE.md (several excerpts at commit 3ce5aff4ea) on 2026-10-12; durable decisions were folded into [DENSE_INDEXING_LIFECYCLE.md](../../../../zig/DENSE_INDEXING_LIFECYCLE.md) before the move.

The log held the former implementation-status bullets, physical-versioning fix narratives, native generation lifecycle hardening with the r128 qualification, the completed implementation phases (0, 1, 2, 4, 6), and the former rollout order. At the time of the move the core implementation was complete and the single-run one-million-document qualification passed. Repeated baseline/candidate performance runs and the full deterministic fault matrix were still rollout gates. Phase 3 (performance qualification) and Phase 5 (repair scheduling and production UX) stayed in the design doc because their gates were still open.

## Decisions (as implemented at the time)

- **Ordinary dense replay uses reopenable streaming sessions**, not bulk-publication state. Streaming-ingest qualifications at 1,000,000, 100,000, and 50,000 documents confirmed that normal writes never enter bulk publication or repair.
- **`IncompleteBulkPublish` is quarantined and repaired** through a durable shadow-generation state machine that resumes after restart. Explicit generation rebuilds for dense, sparse, graph, and full-text indexes use the same intent and post-pointer reconciliation. Automatic and operator repair share one `BackendRuntime` maintenance owner.
- **The durable source counter is the authoritative cardinality invariant** for artifact-backed dense coverage. An absent counter is bootstrap debt, recovered from a stable snapshot plus a signed concurrent-write delta, with a per-attempt nonce. A surplus, a missing counter, a config mismatch, or an allowlisted structural failure fails the index closed and triggers a shadow rebuild. A deficit after replay convergence is filled in place. Key-set correctness comes from building an empty shadow only from the snapshot plus fenced replay. A per-write cryptographic digest "is not justified unless fault injection demonstrates an equal-count substitution" that bypasses the backend's own integrity checks.
- **One transaction per artifact mutation.** Document deletion, TTL cleanup, and remote document-child batches commit artifact writes, the journal record, and the counter mutation together in one primary-store transaction. Batches are normalized to their final state before the delta is computed.
- **Scheduler shape.** The debt-only fair queue has O(1) enqueue and removal and a 32-entry round-robin window. Lost wakeups are recovered by an adaptive 16-to-256-group cursor window, not by a full-node sweep. Pending outcomes are re-enqueued before the cursor advances. Retry backoff uses a separate consecutive-failure streak, so a long repair cannot jump straight to the ten-minute maximum. Unknown leadership keeps debt queued.
- **Activation.** Activation is atomic and adopts the already-open shadow runtime. It retains the previous root until the new clean checkpoint is published, and shares one absolute activation deadline. A root-generation rollover replaces the replica repair identity and all pointer-selected debts in one checkpoint.
- **Generation completion and source-artifact debt are separate outcomes.** A committed replacement completes the intent even if artifact-repair issues remain. Forced completion is persisted against the API job identity before the intent is removed, so a crash cannot build a second generation.
- **Resource ownership.** `ResourceManager` owns cache policy and disk-growth claims, both kept out of index configuration. Disk claims are rechecked at bounded publication and catch-up boundaries, and materialized candidate bytes are reconciled so they are never double-charged. Durable retry deadlines are realtime and get translated once into monotonic time; the two clock domains are never compared.
- **Write admission.** Ordinary `.write` traffic uses 200/100 pending-sequence hysteresis. Managed-Raft writes are gated on the confirmed leader before proposal; committed apply always bypasses the gate.
- **Public repair status stays compact.** Malformed durable state maps to `failed`. Malformed API repair-job records are moved into a bounded forensic quarantine and cannot block the API server.
- **Physical versioning (fresh v2 construction).** The shared exact-vector base is established before capture. The first managed index publishes a real empty generation at source sequence zero. Backfill materializes the same index-managed artifact as foreground writes. Stable-tip publication compacts the captured native delta instead of rescanning. Regressions require zero primary vector snapshot builds.
- **Orphan cleanup treats canonical `ACTIVE_ROOT` pointer targets as live.** The construction marker protects a root before pointer publication, and the durable pointer protects it afterward. This came from a managed corruption/recreate E2E failure in which a catalog-lagging cleanup worker could delete a newly published generation.
- **Native generation lifecycle hardening (post-r126 review).**
  - Backup manifest v5 authenticates both the snapshot path and the runtime `install_path`.
  - Snapshot admission takes file-descriptor leases on committed WAL prefixes. Fence work drops from O(committed WAL bytes) to descriptor acquisition.
  - Startup and publication run a filename-only inventory reconcile against `CURRENT`.
  - Native authority requires an explicit transition capability. It is granted only to an inactive candidate or an already-selected v2 generation.
  - Authenticated native restore is rehomed into a deterministic v2 generation, publishing the pointer last.
  - Compatibility LSM files are cleanup debt, removed only after native authority and the capability floor are proven.

## Replaced or rejected approaches

- **Long-lived HBC bulk sessions for ordinary replay**, replaced by streaming sessions. The old helper that deleted artifacts in a separate transaction was removed.
- **Substituting a separately committed status snapshot as the correctness target** was rejected; only the durable counter is authoritative.
- **A periodic full-node sweep for lost wakeups** was replaced by the bounded adaptive cursor window.
- **Closing and reopening a large index under the write fence at activation** was replaced by adopting the open shadow runtime.
- **Deriving orphan liveness from one manager's in-memory catalog** was unsafe under catalog lag.
- **Benchmark-only workarounds for lifecycle gaps** were rejected in favor of the durable shape.

## Key measurements

| Run | Result |
|---|---|
| r128 50K public-API lifecycle (post-merge, heavy host contention) | recall 0.9876 live, cold-reopened, and warm-reopened; all 50,000 vectors covered; no capture, generation, recovery, or cleanup errors; restart RSS peak 350.8 MB; restart physical-footprint ledger 124.2 MB |
| r128 timing (not timing evidence) | insert 207.68 seconds and profiled server query time 24.60 ms, inflated by two unrelated CPU-saturating Zig test jobs |
| r126 uncontended (applicable performance baseline) | 30.09-second lifecycle, 3.23 ms mean server time |

## Completed implementation phases (acceptance criteria)

The phases describe dependencies, not release order. The work was split into three workstreams: automatic restart repair, streaming replay, and generation publication.

- **Phase 0: invariants.** Every raw `bulk_ingest_session_depth` decision has an owner and a recovery rationale. A normal non-session HBC batch is proven reopenable. The idempotency audit covered insert, overwrite, move, delete, and coverage.
- **Phase 1: streaming contract.** Adds `WriteSessionKind` and capability predicates. Normal catch-up never writes `__bulk_publish_state`. Every committed streaming batch is reopenable. Relaxed/`none` durability never advances a durable watermark before an explicit sync succeeds.
- **Phase 2: crash/idempotency qualification.** No duplicates or lost deletes after restart. Recall matches a clean ingest. No streaming interruption yields `IncompleteBulkPublish`.
- **Phase 4: durable automatic repair core.** Repairs an `IncompleteBulkPublish` index without wipe/re-ingest. The quarantined root stays untouched until a validated replacement is active. Unknown failures stay fail-closed. A candidate from a different local root can never resume.
- **Phase 6: shadow generation publication.** The previous generation stays searchable only when it is proven complete, for example in an explicit operator rebuild. Incomplete candidates never activate. Retired generations are garbage-collected safely.

## Former rollout order

- **Streaming replay:** tests and local benchmarks, then a fault-injection canary with forced termination, then a staged binary rollout, then removal of unreachable bulk-session plumbing.
- **Automatic repair:** classification and status only, then operator-triggered execution, then automatic execution in fault-injection and canary environments, then automatic by default with concurrency one. Higher concurrency comes only after multi-group resource qualification.
