# VOPR Sections Before the October 2026 Condensation

> Summary of a verbatim log. Full text: [docs/design/vopr/history/condensed-sections-2026-10.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/vopr/history/condensed-sections-2026-10.md). Originally relocated verbatim from `zig/VOPR.md` (lines 823–893, 1033–1075, 1299–1447, 1530–1598, 1627–1868, 2159–2264 at commit c6c0ba51cd) on 2026-10-12.

The long-form walkthroughs that [VOPR.md](../../../../zig/VOPR.md) now states as compact rules, tables, and pointers: the distributed coverage boundary, the defect-class list, the scheduled-soak operations narrative, the verification audit, per-seam extension walkthroughs, and the long-form ongoing roadmap. Where this text and VOPR.md differ, VOPR.md and the code win. Related narratives live in [full-cluster-scenario-narratives.md](full-cluster-scenario-narratives.md#timer-and-deadline-defect-narrative-originally-lines-15791607), [follow-ups-2026-09.md](follow-ups-2026-09.md), and [status-history.md](status-history.md#ongoing-roadmap-narrative-relocated-from-voprmd).

## Decisions and standing rules

- `full-cluster-vopr-test` is one deployment-shaped in-process campaign (metadata quorum, restartable hosted data nodes, multiple public clients, co-scheduled serverless fixture, node-local roots, three distinct resource owners, real Raft `httpx` wire hop, one shared `VoprIo`).
- Transaction creation and recovery must use the same `BackendRuntime` realtime clock — borrowing worker I/O alone is insufficient, since a host-clock recovery pass can expire a fresh virtual-time transaction between begin and prepare (`transaction-runtime-regression-test`).
- The serverless object catalog and metadata placement catalog stay separate domains; joining them would invent an ownership relationship Antfly does not have.
- Make routed write/read, Raft, split/merge, and worker internals finer scheduler-visible transitions only where production exposes safe suspension points; never grow simulation-only business logic or a container/hypervisor clone.
- Lifecycle seams observe persistence from the production apply watermark rather than suspending inside Raft storage ownership; they preserve the single production writer and its lease ownership.
- Direct server TLS fails closed; the supported production boundary is TLS termination at a reverse proxy or load balancer.
- PostgreSQL execution deadlines keep their host-monotonic clock contract while replication runners use borrowed `std.Io` for persisted wall-clock timestamps.
- Provider boundaries: VOPR owns the deterministic application boundary, not a substitute for model execution, GPU kernels, or libpq internals, which stay in differential/integration tests.
- Object-backed WAL append accepts a durable caller-supplied operation identity: retry after ambiguous timeout returns the original LSN, conflicting reuse fails closed, and stores that cannot uphold this reject idempotent append rather than degrading silently.
- Serverless maintenance uses a durable object-store work lease with monotonic fencing tokens; released leases remain as expired records so tokens never move backwards; progress CAS stays the final durable visibility boundary.
- "Finished" is never implicit: a label applies only to a narrowly stated integrated contract whose named focused gate passed at the cited checkpoint. A changed tree must rerun the gate; documentation is not evidence. A regressed row is downgraded or repaired. Antithesis parity is not claimed.
- A suite counts as integrated only while its scenario module is forced into test discovery, its command runs at least one passing exact-replay test, and it remains a `vopr-test` dependency.
- Before source-to-source cache transfer / stale-cache pruning become replayable, give each owner a stable semantic lock key or route both through one coordinator; do not simply reverse lock order.
- Live mixed-version operation waits for two runnable compatible binaries and an explicit upgrade contract; unmodified container runs are differential, never exact replay.

## Defect classes found

Executor and clock escapes; scheduler-identity aliasing (process-global counters / raw pointers); shallow replay comparison (ID, byte count, or enabled-set only); cancellation ordering (parent unwinding before children, retry loops swallowing cancellation); teardown and restart races; write-outcome misclassification (ambiguous vs definite, untyped retryable errors); ownership misrouting; replication and merge/split protocol gaps (sparse watermarks, unprojected checkpoints, rolled-back state mistaken for progress); authorization boundary gaps; resource-accounting blind spots; fault-injection fidelity (too coarse or too narrow); leaks on error/cancellation paths; cross-boundary encoding and ABI mismatches.

## Scheduled retained-corpus soaks

| Parameter | Value (as recorded) |
| --- | --- |
| Schedule | [zig-vopr-soak.yml](../../../../.github/workflows/zig-vopr-soak.yml), daily 10:00 UTC plus manual dispatch; one ReleaseSafe runner |
| Shards | two each of `standby`, `raft`, `distributed-data`, `standby-scaling`; shard 0 bounded-fair, shard 1 adversarial |
| Per-shard history budgets | 1000, 1000, 12, 2 |
| Bounded-fair window | oldest overdue alternative serviced after 256 choices |
| Adversarial delay | selected ready actor delayed for 128 choices, then fair |
| Corpus validation budget | eight million recorded transitions; 110-minute wall-clock |
| Retained working corpus | up to 128 clean traces (standby/Raft), eight (distributed data), two (standby/scaling), plus smallest representative per failure fingerprint |
| Campaign subprocess timeout | 230 minutes, TERM then kill after 30-second grace |
| Artifact retention | 90 days |
| Qualification cleanup budgets | 1, 2, 4, 8, 16, 32 and 8,192 transitions (`scripts/ci/zig_vopr_qualification.py`) |

- `production-e2e` job: storage-owner publication tests repeated 20 times, 200 public restore cases, 200 multi-node Autograph cases (50 original + 50 data-restart per normal and 256-FD profile), 100 large-catalog isolation cases, 200 aggregation/savepoint-transform cases (15-second per-request deadline). These native stress tests do not provide VOPR schedule replay.
- Corpus caches are keyed by executable SHA-256; a different runner starts a fresh corpus instead of treating an old layout as a replay divergence. Fiber callsite identities are scoped to a pinned executable, so keep the original executable for older traces.
- Scheduled campaigns use `--defer-diagnostics` so one finding cannot consume the whole budget before reports publish; reduction runs separately via `vopr recipe`.
- An exhausted history is a failing soak (`transition-budget-exhausted`), not by itself a diagnosis of starvation or deadlock; a partial history cannot pass cleanup merely by not completing. Only a completed validation manifest can publish a working corpus.
- Full operational qualification requires two successful default-budget Linux workflow runs, the second with `require_seed=true` and nonzero consumed seeds per shard; a local gate alone is not full-soak evidence. Scheduling a workflow does not prove a nightly budget completed — use `run.json`, `results.json`, and corpus `index.json`.

## Implemented extension seams (as of the move)

Each seam had a focused exact-replay gate: DataServer/Raft lifecycle microsteps (routing through response-ack, split/merge transition identities); HTTP lifecycle and backpressure (partial writes, pipelining, half-close, reset vs FIN); background runtime lifecycle (`DurableJobLane`, DataServer services moved off private native threads); `replication-backfill-vopr-test` (each history restarts from durable status and exact-replays twenty times); `supervision-vopr-test`; `auth-lifecycle-vopr-test` (`UserManager` borrows `std.Io`, no hidden `Threaded` executors); serverless object-store protocols over `ScriptedFaultClient`; `serverless-workflow-vopr-test`; `db-index-race-vopr-test` (replaced thread-timing regressions with production-safe boundaries); admission and resource pressure; `provider-boundary-vopr-test`; `composed-query-vopr-test`.

## Ongoing roadmap (long form, at the move)

1. Deepen the promoted full-cluster reconfiguration seam: complete the managed-index lifecycle and packet-level replay stabilization; cancellation under storage/disk faults and simultaneous process loss; disjoint donor/receiver replica sets; snapshot-install rehydration.
2. Deepen public distributed operations: right/nested/foreign and multi-range-left joins; global queries under the same fail-closed rule; introduce a partial-response schema only if product semantics require it.
3. Compose independently proven fault domains, including co-locating standby and the data plane.
4. Broaden workloads: row-level tenant scoping, identity mutation races, remote generation/reranking adapters, remote-content live-secret rotation mid-request.
5. Operationalize tooling: scheduled-run evidence, quarantine review, notifications, dashboards; adopt the registered-deployment composer beyond full-cluster.
6. Expand the determinism audit toward full transitive call-graph coverage; optional/default `Threaded` owners must take the scenario's borrowed `std.Io` before joining exact replay.
7. Keep distributed fidelity explicit; add a federated agent/broker only for a demonstrated separate-address-space or mixed-binary requirement; no hosted graphical debugger or hypervisor clone for nominal parity.
