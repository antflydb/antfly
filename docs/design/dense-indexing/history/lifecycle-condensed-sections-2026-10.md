# Dense Indexing Lifecycle: Sections Before the October 2026 Condensation

> Summary of a verbatim log. Full text: [docs/design/dense-indexing/history/lifecycle-condensed-sections-2026-10.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/dense-indexing/history/lifecycle-condensed-sections-2026-10.md). Originally relocated verbatim from `zig/DENSE_INDEXING_LIFECYCLE.md` (eight sections at commit c6c0ba51cd) on 2026-10-12; durable decisions were folded into [DENSE_INDEXING_LIFECYCLE.md](../../../../zig/DENSE_INDEXING_LIFECYCLE.md) before the move.

These were the long-form originals of sections that the design doc now states as compact rules and tables. Where they differ, the design doc and the code win. The verbatim text is useful mainly for the rationale behind each rule.

## Problem and Terminology (originally lines 52–119)

The asynchronous dense worker used to open an HBC bulk-ingest session while catching up normal writes. That durably wrote `__bulk_publish_state = incomplete`, so a process interruption left the index in `IncompleteBulkPublish` and quarantined it until explicit reconstruction. Source data was never lost; the problem was the write-session contract. Streaming sessions remove the ordinary-write cause, and automatic repair handles the remaining legitimate incomplete publications. The design separates three terms:

- **Bulk-optimized batch:** one bounded apply call using grouped routing, coalesced leaf mutation, and similar optimizations. It needs no index-wide marker.
- **Streaming replay session:** a sequence of independently valid batches, recovered by replaying from the last durable applied sequence.
- **Bulk publication session:** may defer structural state across transactions. It is reserved for import, rebuild, or true bulk construction, and needs a marker plus reconstruction, or isolation in an inactive shadow generation.

## HBC session capabilities (originally lines 265–337)

- **Capability predicates.** HBC tracks `write_session_depth` and `write_session_kind`. Safety decisions use capability predicates (`crossBatchPublicationActive`, `mustPublishMetadataPerBatch`, and others). No safety decision outside session begin/finish code may depend on the raw depth.
- **Behavior per session kind.** Node-key staging and deferred quantized rebuilds or leaf splits apply only during cross-batch publication. Reopen-critical metadata and Flat-RaBitQ node keys publish at every streaming batch finish.
- **Streaming finish and abort.** Streaming finish never clears a marker because it never creates one. Abort neither rolls back committed batches nor advances the applied sequence.
- **API mirror.** `IndexManager` exposes the same split by index name. `BatchOptions.mode = .bulk_ingest` only selects batch algorithms and does not imply an unsafe publication window.

## Recovery owner (originally lines 485–509)

Managed startup catch-up owns automatic restart repair. It already runs in the background, selects locally owned groups, serializes through the write owner, and applies bounded backoff.

The HTTP repair-job store was rejected as the correctness mechanism for two reasons: it would add a second ownership and scheduling system, and its table-scoped job shape does not identify the locally owned group. It remains the operator-facing mechanism. It also durably retains unfinished cancellation traversals, which the API maintenance supervisor resumes after restart. Automatic and operator repair use the same DB repair engine and durable intent.

## Disk-capacity ownership and admission (originally lines 829–894)

- **Ownership.** Disk admission belongs to `ResourceManager`, not the executor. A capacity source supplies a physical-domain identity plus an optional observation. Provisioned storage uses a live `statvfs` probe that also feeds store heartbeats.
- **Immutable source.** The source cannot change after runtime composition; installing a different source is rejected.
- **Planned bytes.** `planned_disk_bytes` is an estimate. Each attempt reserves only the remaining growth and keeps a safety floor. Unknown capacity is accounting-only, and known-but-stale capacity fails closed.
- **Revalidation.** Claims are revalidated with an O(1) refresh at every bounded boundary. The complete shadow is measured and the claim shrunk only when the conservative claim would deny progress, which avoids repeated recursive walks. Denial metrics count only real stops. Ordinary ingest and query never measure.
- **Scope of claims.** Claims coordinate DBs that share a manager, not independent processes. Production must enforce one writer process per capacity domain or use a quota-backed source; this is part of the storage backend contract.

## Repair engine and scheduler (originally lines 1046–1207)

- **Discovery versus execution.** Cheap discovery (`discoverRecoverableStartupIndexFailures`) does no corpus scan, so one broken group cannot head-of-line block the others. A node-local executor runs the shared `advanceIndexRepairIntent` state machine. Notifications are hints, and durable intents are rescanned.
- **Initial limits.**
  - One active reconstruction per node and one per group.
  - Bounded round-robin selection across groups.
  - Admission through `ResourceManager`, with a disk plan plus a process-local reservation; free-space preflight alone is not admission.
  - A 15-second scheduler budget, checked only after a resource-sized batch.
  - Unbounded per-group concurrency is never allowed.
- **Queue.** Known debt sits in an intrusive linked hash queue with O(1) enqueue and removal and at most 32 entries per pass. Named-index operations keep the group queued until an aggregate audit checks every index in it.
- **Retry.** State-machine failures persist their deadline in the intent. Failures outside it use jittered backoff from 30 seconds through 10 minutes in monotonic time, and park only the affected group.
- **Lost-wakeup fallback.** The fallback window adapts between 16 and 256 routes per pass, targeting a 30-minute full rotation. Beyond the supported envelope it reports a rotation-SLO violation. It is a safety audit, not the primary discovery path.
- **Ownership decisions are tri-state.** `attempt`, `retry_unknown`, and `skip_nonlocal`; only a known voter set without the local node removes debt.
- **Projected completion is omitted** until measured throughput makes it defensible.

## Automatic repair test matrix (numbered form) (originally lines 1426–1513)

The original was a 26-item deterministic matrix. It covered:

- crash points around intent persistence, snapshot construction, readiness, and the pointer swap, including each state transition exercised twice
- the missing-counter snapshot-plus-delta bootstrap with concurrent insert, replace, and delete, through the normal, TTL, and remote-child paths
- `UnsupportedVersion` staying terminal, while the allowlisted `Corrupted`, `NotFound`, and `FileNotFound` cases rebuild
- missing or corrupt artifacts, a full filesystem, and malformed intents
- leadership transfer, identity and root changes, HA promotion, move/split, and restore
- the truncation race on the pinned snapshot, and relaxed/`none` durability including LMDB `no_sync`
- convergence under sustained writes, dropped notifications, and cancel versus pause (only cancel permits automatic retry; pause survives restart)
- budget exhaustion with `dense_repair_backpressure` and `Retry-After`, size-aware fairness, and soft-budget deferral
- leader-only 429 admission under hard replay pressure
- a 100,000-document interrupted external-vector ingest, and a run of at least one million documents with repeated forced termination

## V1 mirror versus native-v2 authority (originally lines 1714–1787)

- **Authority by storage mode.** A v1 index stays LSM-authoritative and may only publish an immutable posting mirror. Only a capability-authorized candidate or an index selected by the v2 pointer may route mutations through the native posting WAL.
- **Naming.** "Posting sidecar" is ambiguous. Future API cleanup should use `native_posting_store` for v2 and reserve `sidecar` for the v1 mirror.
- **Rollback is capture-local.** A capture restores natively only if it owns a pinned immutable base. A base-less candidate or a v1 mirror performs the real LSM inverse, because skipping it could leave a phantom derived vector. Native-store enablement returns a runtime authorization error instead of a `std.debug.assert`, so the fence holds in `ReleaseFast`.
- **Artifact identity is a set.** A single-source index owns its configured `embedding_name`, falling back to the index name. A multi-source index owns its set of families. Readiness sums the per-source certificates.
- **Orphan deletion is reauthorized.** Cleanup re-reads all canonical pointers immediately before deleting each candidate, because the initial inventory is only a filter. A hook test publishes and clears the marker between inventory and deletion.

## Rollout counters (originally lines 1920–1944)

Streaming replay is enabled unconditionally, with no public or resource toggle; rollback means deploying the prior binary. The original listed counters for:

- streaming and bulk session lifecycles
- reapplied batches and quarantines
- repair attempts, queue depth and age, and replay-pin and snapshot ages
- candidate bytes, phase durations, and convergence lag and activation pause
- backpressure rejections, and finish latency and maximum replay lag
