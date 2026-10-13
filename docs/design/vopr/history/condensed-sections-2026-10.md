# VOPR Sections Before the October 2026 Condensation

> Relocated verbatim from `zig/VOPR.md` (lines 823–893, 1033–1075, 1299–1447, 1530–1598, 1627–1868, 2159–2264 at commit c6c0ba51cd) on 2026-10-12. Durable decisions were folded into [VOPR.md](../../../../zig/VOPR.md) before the move.

These are the explanatory walkthroughs and long-form prose that VOPR.md now states as compact rules, tables, and pointers. Where this text and VOPR.md differ, VOPR.md and the code win.

## Distributed Coverage Boundary (prose) (originally lines 823–893)

#### Distributed Coverage Boundary

The suites above prove distributed semantics at complementary production
boundaries; they are not merely single-node unit models. They already exercise
multiple logical nodes, clients, stores, transports, consensus participants,
failure domains, and recovery owners under one exact-replay scheduler.

`full-cluster-vopr-test` now forms one deployment-shaped in-process campaign
containing all of the following at once:

- a metadata quorum and multiple independently restartable hosted data nodes;
- multiple public API clients with concurrent write, read, query, cross-node
  routing, two-table isolation, and a cross-range graph workload;
- a co-scheduled serverless build/enrichment/publication fixture and production
  serverless HTTP listener over the exact object-backed catalog mutated by the
  worker; forward-only v50 adds its stale-generation/progress-conflict mode
  beside live production metadata, DataServers, and public clients;
- node-local storage roots and modeled devices plus distinct production
  resource managers injected into each node's DB and API paths;
- production public HTTP and a real Raft `httpx` wire hop after deterministic
  link-fault policy; and
- cluster-wide oracles spanning acknowledged durability, quorum and fencing
  safety, route/topology consistency, publication visibility, eventual
  convergence, and cleanup.

The campaign uses production metadata, public API, and serverless HTTP
owners/listeners,
real metadata/Raft paths, node-local roots and modeled devices, three distinct
resource owners, and one shared `VoprIo`; clean, metadata-partition,
node-restart, in-flight graph-leader restart, in-flight graph range-merge churn,
in-flight graph-transport failure/recovery, partial-HTTP-write, and aggregate
node-memory denial/recovery modes exact replay in the hosted campaign. After a
clean worker completes, a production `ServerlessHttpClient` lists `docs` and
queries version 3 through the real serverless handler and `httpx` listener.
Transaction creation and recovery use the same `BackendRuntime` realtime
clock. Borrowing worker I/O alone is insufficient: a host-clock recovery pass
can expire a fresh virtual-time transaction between begin and prepare.
`transaction-runtime-regression-test` checks fresh and expired transactions
through both write-source configurations, along with bounded stateless retry
behavior and preservation of conditional conflicts and unknown outcomes.
The resource-pressure recording and exact replay remain in
`full-cluster-vopr-test` as a separately selectable test.
`vopr-runtime-regression-test` groups the runtime ownership, clock, snapshot,
and cluster-replay regressions and accepts runtime `--test-filter` arguments.
Forward-only v50 replaces the older standalone stale-generation cluster mode:
the production-owner campaign queries version 4 and requires the authoritative
document after a losing publication CAS and stale-derived-record rejection.
The public DataServer client also writes a graph whose two edges cross the
table's range boundary, waits for full-index acknowledgement, and traverses
both hops through the production public-query parser, range planner, internal
HTTP expansion protocol, shard fanout, and canonical response decoder.
Raft time and delivery eligibility remain explicit modeled rounds, but
each successful delivered frame crosses the production binary codec, fault
router, `IoHttpExecutor`, VOPR socket, httpx listener, and production Raft HTTP
handler. The serverless object catalog and metadata placement catalog are
separate production domains; joining them would invent an ownership
relationship that Antfly does not have. The current limits are instead that standby
and data-plane scenarios remain independently composed rather than co-resident
production services. Routed write/read, Raft, split/merge, and worker internals
should become finer scheduler-visible transitions only where production
exposes safe suspension points. This must not grow simulation-only business
logic or a container/hypervisor clone. The hosted data-node rig is not a
production `DataServer`: ordinary writes and structural merge steps do not
cross `DataServer` data Raft in that composition yet. Outside it,
`data-server-vopr-test` composes three production `DataServer` owners, two
three-replica groups, real public HTTP and Raft listeners, routed forwarding,
leader transfer, owner restart, every-replica transition/watermark convergence,
document equality, exact terminal retry, and actor-owned teardown on one
`VoprIo`. Every incomplete public graph, join, or global-query operation in
the production-owner modes must fail closed without partial rows; each faulted
history heals and proves a complete fresh request.

## Defects Found (class list) (originally lines 1033–1075)

### Defects Found

VOPR has caught real defects across these classes:

- Executor and clock escapes, where production code silently constructs a
  native thread, a threaded I/O runtime, or reads the host wall clock instead
  of using the caller's borrowed deterministic runtime.
- Scheduler-identity aliasing, where task, futex, socket, listener, and packet
  IDs derived from process-global counters or raw pointers could alias across
  unrelated operations and defeat exact replay.
- Shallow replay comparison, where replay checked only an ID, a byte count, or
  an enabled-set match instead of the actual selected transition and payload
  content, letting real differences pass silently.
- Cancellation-ordering bugs, where a canceled parent task could unwind before
  its children finished, or a retry loop re-caught and discarded its own
  cancellation instead of propagating it.
- Teardown and restart races, where resources were destroyed out of lifecycle
  order, uninitialized or poisoned memory was read during early teardown, or
  an owner's identity was not preserved across reconstruction.
- Write-outcome misclassification, where ambiguous and definite write
  failures were conflated, or resource/leadership/topology errors collapsed
  into an unretryable failure instead of a typed retryable one.
- Ownership misrouting, where a request reached a non-owning replica or node
  and either failed outright or was silently executed against the wrong local
  store.
- Replication and merge/split protocol gaps, where sparse sequence
  watermarks, unprojected checkpoints, or rolled-back/unbootstrapped state
  could be mistaken for real progress, risking a non-idempotent replay.
- Authorization boundary gaps, where fail-closed credentials were missing
  from a fixture, or a permission change was not enforced against work
  already admitted into a request.
- Resource-accounting blind spots, where memory, disk, or connection usage
  was invisible to the shared resource manager, or a capacity source leaked
  real host state into modeled behavior.
- Fault-injection fidelity bugs, where an injected fault was too coarse
  (crossing unrelated protocols) or too narrow (missing fragmented writes, an
  accidentally local "remote" target) to exercise the intended boundary.
- Ownership and leak bugs on error and cancellation paths, where allocated
  results or handles were not released when a request was canceled or a
  lifecycle hook failed.
- Cross-boundary encoding and ABI mismatches, where independent compilation
  units, internal handlers, or clients disagreed on wire encoding or
  error-domain translation for the same logical value.

## Scheduled retained-corpus soaks (operations narrative) (originally lines 1299–1447)

#### Scheduled retained-corpus soaks

[zig-vopr-soak.yml](../../../../.github/workflows/zig-vopr-soak.yml) runs daily at 10:00
UTC once merged into the default branch, and supports manual dispatch. It
builds one ReleaseSafe runner, then runs two shards each of `standby`, `raft`,
`distributed-data`, and `standby-scaling`. Per-shard history budgets are 1000,
1000, 12, and 2 respectively; the dispatch input can override them. Each shard
uses one worker and records its exact seed, revision, command, initial corpus,
and completion status in `run.json`, including the executable SHA-256 and
restored trace digests. Shard 0 uses bounded-fair mutation and shard 1 uses
adversarial mutation. Initial histories use the scenario's baseline generator
(cooperative scheduling for standby/scaling). `--exploration-policy cooperative`
selects the cooperative mutation suffix explicitly for comparisons.

The parallel `production-e2e` job builds and retains one production executable,
checks its SHA-256 before and after testing, repeats the compiled storage-owner
publication tests 20 times, and runs 200 public restore cases. It also runs
200 multi-node Autograph resolution/promotion/hydration cases: 50 original and
50 data-restart cases per normal and 256-FD profile, with two workers. Restart
cases reopen every data node after the initial document commit and require
promotion without a new write to wake recovered owners.

The same job runs 100 large-catalog control/diagnostic isolation cases (50 per
profile), plus 200 concurrent aggregation/savepoint-transform cases (50 of each
per profile). Catalog fixtures use production Raft/control cadence and retain
HTTP rejection details without retrying failed mutations. The aggregation case
retains its exact-result assertions and 15-second per-request deadline.

`scripts/ci/zig-e2e-autograph-soak.sh`, `scripts/ci/zig-e2e-catalog-soak.sh`, and
`scripts/ci/zig-e2e-query-transaction-soak.sh` expose these profiles locally. Set
`ANTFLY_E2E_REGRESSION_REPORT_DIR` to a fresh directory and optionally override
workers/repetitions. These native stress tests complement deterministic runtime
and Raft tests; they do not provide VOPR schedule replay. Session unit tests
separately force foreground/recovery overlap and cancellable execution handoff.
Aggregation unit tests advance the primary generation after result capture and
require exact results from the captured rows. They also require one read lease
across selection and collection, with Raft admission before writer exclusion.
The `production-e2e-soak` artifact retains the executable, logs, exact per-case
JUnit reports, and failed server roots. Missing, skipped, failed, or incorrectly
counted cases cannot qualify a run. PR-only qualification skips these full
production soaks.

A bounded-fair suffix randomizes runnable work, ages continuously enabled
alternatives, and services the oldest overdue alternative after a 256-choice
window. An adversarial suffix first prefers time advancement and delays a
selected ready actor for 128 choices, then restores fair scheduling so the
scenario can demonstrate recovery. Exact replay uses the recorded choices.
Each `*.schedule.json` records the mutation point, replacement, suffix seed,
policy bounds and parent digest, or both parents and the selected splice point.
Scheduling provenance follows retained traces into the next campaign.

Scheduled campaigns use `--defer-diagnostics`: every finding still retains its
trace, flight recording, and aggregate summary, but automatic reduction and
counterfactual searches run separately through `vopr recipe`. This keeps one
production finding from consuming the entire scheduled budget before reports
are published.

The workflow caches the built runner by source revision and target, then keys
each scenario's working corpus by that executable's SHA-256. Unchanged
revisions reuse the exact runner, including its fiber identities; a different
runner starts a fresh corpus rather than treating an old executable layout as
a new replay divergence. Older artifacts remain available for diagnosis with
their retained executable.

The workflow restores the last compatible scenario corpus, copies it into a
fresh run directory, and uploads reports, traces, logs, and diagnostics even
when the campaign fails. A separate job replays and merges the uploaded shard
corpora, deduplicates them, and saves a bounded working corpus for the next run.
The CLI chooses its compatibility authority by exact replay with the current
runner, preferring fresh histories. It deduplicates bytes before validation and
replays each unique compatible candidate once, streaming retained bytes to disk
before opening the next candidate. `validation.json` identifies the current
input and cumulative replay/byte cost. The default validation budget is eight
million recorded transitions (`corpus-merge --max-replay-transitions`); the
wrapper also enforces an independent 110-minute wall-clock budget.
Duplicate-only campaigns can use a replayed seed; divergent candidates remain
inputs for quarantine instead of aborting the merge before valid histories are
retained. If no candidate replays, the job
fails with an authority-selection log and leaves the uploaded shard traces
available for diagnosis without publishing a working corpus.
The working corpus keeps up to 128 clean traces for standby/Raft, eight for
distributed data, and two for production standby/scaling, plus the smallest retained
representative of every distinct failure fingerprint. `retention.json` records
that selection; the full merged corpus remains in the uploaded artifact.
Incompatible versions are quarantined for review; unexpected replay errors or
replay divergence fail the run and preserve evidence. A still-reproducing
finding in the initial corpus also fails the campaign. Campaign timeouts leave
room for artifact upload; interrupted runs never acquire a stale success report.
The wrapper gives campaign subprocesses 230 minutes, sends TERM on cancellation
or timeout, then kills the process group after a 30-second grace period and
reaps the child. Each history writes a small phase report, and its generated
trace is saved atomically before exact replay starts. Small reports and logs
upload separately before large traces.
Only a completed validation manifest can publish a working corpus.

An exhausted history remains a failing soak. standby/scaling records it explicitly
as `transition-budget-exhausted`; this does not by itself diagnose starvation
or a production deadlock. Its cleanup property runs after cancellation and
owner release and checks remaining tasks, file handles, sockets, queued
executor work and transport closes. A partial history can no longer pass
cleanup merely because it did not complete. Cutoff logs name the active
operation and wait owner, plus task dependencies and actual sleep deadlines
with their clock domains. `campaign --scenario standby-scaling --transitions N`
can target startup and intermediate cancellation boundaries explicitly.

`scripts/ci/zig_vopr_qualification.py` runs a small real campaign, validates and
retains it, copies the corpus into a new run, and requires the second campaign
to consume compatible entries. It also checks duplicates, incompatible and
malformed traces, replay divergence, rejection without a valid authority,
validation-budget exhaustion and exact replay of cleanup at transition budgets
1, 2, 4, 8, 16, 32 and 8,192. The larger cut cancels active Raft/HTTP owners,
timers and external waits as well as the early deployment-admission boundaries.
This gate runs in PR CI and before scheduled soaks. Full operational
qualification requires two successful default-budget Linux workflow runs; the
second dispatch must set `require_seed=true` and report nonzero consumed seeds
for every shard. A local gate alone is not full-soak evidence.

The same CI gate runs the production Raft transport tests and the determinism
audit.

> **Relocated:** The timer/deadline defect narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/full-cluster-scenario-narratives.md](full-cluster-scenario-narratives.md#timer-and-deadline-defect-narrative-originally-lines-15791607). The resulting borrowed-clock rules are under [`VoprIo` Runtime](../../../../zig/VOPR.md#voprio-runtime).

The executable, run and merged-corpus artifacts are retained for 90 days.
Fiber callsite identities are scoped to a pinned executable layout; keep the
original executable when investigating older traces. The diagnostic dispatch
accepts `diagnostic_run`, `diagnostic_shard` and `diagnostic_trace`, downloads
that run's executable and trace, and captures native Linux operation boundaries
and suspended owner stacks without rebuilding the runner. Version 2 is inspected
before finalization releases owners; older executables are inspected at deinit.
Its output is
diagnostic replay evidence, not a new soak qualification.

The cache is an acceleration/resumption mechanism and can be evicted; download the retained
corpus artifact to resume manually after eviction. Scheduling a workflow does
not establish that a nightly budget has completed: use its uploaded `run.json`,
`results.json`, and corpus `index.json` as evidence. Replaying a single trace
requires only that trace and a compatible runner; repeating corpus-guided
search requires the initial corpus, seed, budget, and runner revision.

```sh
zig build vopr-build -Doptimize=safe -j1
python3 ../scripts/ci/zig_vopr_soak.py --binary zig-out/bin/vopr run \
  --scenario standby-scaling --seed 0xa17f5500 --histories 2 \
  --corpus /tmp/previous-vopr-corpus --output /tmp/new-vopr-run
python3 ../scripts/ci/zig_vopr_soak.py --binary zig-out/bin/vopr merge \
  --inputs /tmp/new-vopr-run --output /tmp/merged-vopr-corpus
```

> **Relocated:** The soak-investigation defect narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/follow-ups-2026-09.md](follow-ups-2026-09.md). Durable invariants from it are in the Raft and Metadata and Distributed Data sections above.

## Verification Audit and Meaning of "Finished" (prose) (originally lines 1530–1598)

#### Verification Audit and Meaning of "Finished"

> **Relocated:** The dated 2026-08-26 verification-audit narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/status-history.md](status-history.md).

Production-owner gates, budgets, and the aggregate
`production-cluster-vopr-test` are listed once in
[Full-Cluster Scenario Versions](../../../../zig/VOPR.md#full-cluster-scenario-versions).
`production-cluster-vopr-smoke-test` (v11 plus v12's bounded cutoff) is part of
ordinary `vopr-test`; the deeper histories are named tiers so a
multi-gigabyte, tens-of-minutes witness stays out of the default edit loop
without treating an early cutoff as completion evidence.

> **Relocated:** The per-version gate-tier narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/full-cluster-scenario-narratives.md](full-cluster-scenario-narratives.md#production-owner-gate-tiers-by-version-originally-lines-17291808).

> **Relocated:** The dated checkpoint-recap narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/status-history.md](status-history.md).

Completion labels in the tables below use the three-level scale defined
under Conformance Status. A row may narrow an **Integrated** claim with a
scope qualifier such as "foundation" (the reusable engine/runtime capability
and its focused gate) or "at the named seam" (the listed production path and
fault modes only, excluding residual work named in the same row). **Partially
integrated** is the Executable-foundation level: focused production seams
exist but are not yet composed through the whole public/deployment path.
**Ongoing** and **conditional** are not completion claims.

Therefore the features labeled integrated below are implemented to their stated
boundaries, but the complete roadmap is not finished. In particular, local
run/index/report tooling and scheduled corpus-retaining campaigns are implemented,
while completed scheduled runs, notifications, and dashboards remain operational
follow-up; distributed VOPR is an
implemented runtime foundation with incomplete composition breadth; and the
event-query layer, saved cross-run set algebra, validation/counting commands,
and bounded live stream are implemented while routine quarantine review and
broader operational integration remain future work.

The word **fully** is consequently never implicit. The conformance audit is:

| Claim class | Audit result | Explicit exclusion |
| --- | --- | --- |
| Named VOPR engine/tooling features | **Implemented at the registered in-process `std.Io` boundary.** `vopr-engine-test` and the registered-source `vopr-determinism-audit` pass at this checkpoint | The audit manifest is not a transitive production call-graph proof; arbitrary guest-kernel RNG/syscall interception, uninstrumented native libraries, and separate process address spaces are also excluded |
| Rows labeled integrated | **Implemented for the production seam, schedules, properties, and exact-replay gate named in that row** | Residual work stated in the row and combinations with other independently tested domains |
| Rows labeled partially integrated | **Not complete end to end** | Promotion requires the remaining public/deployment composition and its replay gate |
| Local results/index/corpus tooling | **Implemented as repository-owned commands, formats, and a scheduled sharded workflow with replay-validated corpus retention** | Completed scheduled-run evidence, notifications, dashboards, and routine quarantine review |
| Antithesis parity | **Not claimed** | Hosted orchestration/UI, deterministic execution of arbitrary containers or kernels, and operational service parity |

This is the answer to “have we fully implemented what we call finished?”: only
for a narrowly stated integrated contract whose named focused gate has passed
at the cited checkpoint. No such label applies to the overall roadmap,
Antithesis product parity, residual work named beside a contract, or any
extension beyond the named green seams. A changed tree must rerun the
named gate before carrying the claim forward; documentation is not evidence. A
broader sentence must not erase those boundaries.

> **Relocated:** The dated v13-v18 defect-and-repair narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/status-history.md](status-history.md).

Standby, per-group Raft, LSM/WAL/LMDB/persistent/index-manager/DB-split, metadata
distributed data, the deployment-shaped full cluster, and the newer P0/P1/P2
boundary suites are implemented at the exact production seams and modes stated
in their conformance rows. “Integrated” is not upgraded to “fully implemented
everywhere,” and a row whose focused or aggregate gate regresses must be
downgraded or repaired rather than defended by this document.

That does not make the roadmap empty or make the system equivalent to the
Antithesis hypervisor. Items marked **ongoing** or **conditional**, and residual
boundaries named in an integrated row, are not finished; live mixed-binary
operation is not modeled; and arbitrary unmodified sidecars or process address
spaces remain differential/integration concerns.

> **Relocated:** The superseded v13–v18 completion recap that previously lived here is preserved verbatim in [docs/design/vopr/history/full-cluster-scenario-narratives.md](full-cluster-scenario-narratives.md#v13v18-completion-recap-originally-lines-18581890).

## Implemented Extension Seams (per-seam walkthroughs) (originally lines 1627–1868)

#### Implemented Extension Seams

The following seams are already implemented. They remain documented here so
new product work extends the same ownership and scheduling contracts instead of
introducing native-only alternatives.

##### Deeper DataServer and Raft Microsteps

The production DataServer, public listener, health request, metadata executor,
and lifecycle safepoints run on borrowed `VoprIo`. A production-neutral
DataServer lifecycle seam now exposes routing, remote forwarding, proposal
acceptance, persistence observation, apply confirmation, visibility
confirmation, and response-ack readiness with stable group/table/log
identities. Persistence is observed safely from the production apply watermark,
which Raft cannot publish before Ready storage completes, rather than adding a
suspension inside Raft storage ownership. Data-Raft proposal deadlines,
capability probes, campaigns, forwarding retries, apply waits, and replicated
timestamps use the borrowed `std.Io` clocks and sleep rather than host time.
A focused production composition now advances two local Raft groups while the
real shard-operation adapter performs merge accept/catch-up/rollback/retry/
finalize and observes the durable result. Split and merge prepare,
copy, cutover, and rollback completions carry stable transition identities;
synchronous paths reach them after transition locks and writer leases are
released, durable split-copy jobs explicitly release their per-source lane
before suspending, and finalize/rollback paths expose writer-handoff and
transition-runtime cleanup boundaries only after those scoped leases close.
Single-table, routed multi-query, and global multi-query result assembly now
crosses a production-neutral API seam carrying stable operation/table identity
and response size after read/storage leases release. Independent Raft campaigns
continue to expose the lower-level persistence/apply ordering. These seams
preserve the single production writer and its lease ownership; extend them when
new routed operations add distinct durable or result-assembly boundaries.

##### HTTP Lifecycle and Backpressure

The common listener and executors run on `VoprIo`; tests cover normal,
single-byte partial-write, deadline-first cancellation, chunked request bodies,
chunked streaming responses, keep-alive reuse, pipelining, half-close ordering,
accept-versus-shutdown, bounded connection/request admission, minimum socket
capacity, descriptor reuse, and overload recovery. Hard disconnect now has a
backend-neutral probe at the httpx handler boundary: native runtimes retain the
shared descriptor observer, while `VoprIo` models reset separately from FIN and
from full-close read abandonment. A write-half FIN preserves the response path;
a full close or reset cancels an active handler even with unread pipelined
input. Direct server TLS
configuration fails closed before runtime or socket admission; the supported
production boundary is explicit TLS termination at a reverse proxy or load
balancer. Extend this suite when httpx gains a production server-side TLS
implementation or another transport backend.

##### Background Runtime Lifecycle

`DurableJobLane` and both production/VOPR implementations now share
pause/resume/drain/close/reopen semantics; tests include admission while paused,
committed-job drain, close, reopen, wrapper relocation, nested-owner teardown,
and exact cleanup. `VoprIo` now composes the Antfly-independent narrow executor
into the same scheduler as its `std.Io` fibers, sockets, and virtual time, so
the Antfly adapter does not require a second simulated runtime.
Transaction recovery, TTL, enrichment, text merge, sparse compaction,
resolution, promotion, LSM maintenance, quarantine retry, and repair now
retain the backend-neutral `std.Io` borrowed from `BackendRuntime`; their
production passes and lifecycle controls execute on `VoprIo`. DataServer
provisioned warmup, startup catch-up, replica-root refresh, local/runtime status
refresh, and auto-bulk finish work now use one shared durable owner instead of
private native threads. Their run/deinit callbacks clear active state on normal
completion, submission failure, cancellation, and DataServer teardown; file
probes inside these services use the runtime's borrowed `std.Io`. Derived-index
execution was already owned by the DB background runtime. The focused
DataServer campaign proves both scheduler execution and queued cancellation;
extend the owner only when another DataServer-native service is introduced.

##### Replication Backfill and Rebalancing

The production snapshot and streaming runners now expose a neutral lifecycle
hook at provider preparation, apply, durable checkpoint, cutover, polling, and
failure-persistence boundaries. Both runners use their borrowed `std.Io` for
persisted wall-clock timestamps, while PostgreSQL execution deadlines retain
their existing host-monotonic clock contract. The Antfly VOPR adapter derives
stable phase, table, source, offset, authority, and checkpoint identities
without importing VOPR into the metadata kernel.

The `replication-backfill-vopr-test` gate runs the production runners through
clean snapshot-to-stream cutover plus source crash, target crash, cancellation,
stale work ownership, target-topology change, source-schema change, and stream
crash after apply but before checkpoint. Every history restarts from the
durable production status record and exact-replays twenty times. Properties
prove that a checkpoint never outruns applied data, repeated work is logically
idempotent, stale ownership is rejected, snapshot and stream data are not lost,
and every interrupted attempt recovers. The topology mode is not a renamed
fixture target or injected callback error: it creates two prepared exact-
cutover snapshots, changes the byte-exact source catalog after target apply,
requires `ReplicationSourceConfigChanged` at durable offset 0, rotates to a
distinct nonzero authority, retires the predecessor, and replays once. In
full-cluster v48, a deployment adapter sends those source and status
transitions through the live metadata quorum while target application crosses
public HTTP and DataServer Raft.

##### Standalone and Serverless Supervision

The serverless maintenance manager no longer owns a private native run-loop
thread. Production bootstrap lends its `std.Io`; the manager owns a
`std.Io.Future`, cancels and joins it during shutdown, and publishes the first
maintenance failure instead of discarding it. `serverless_main` now routes that
failure through the shared production `RuntimeSupervisor`, alongside public and
health listener failures. This keeps listener, maintenance, and process
cancellation under one owner and makes the loop runnable on `VoprIo`.

The production supervisor also accepts a borrowed `std.Io` for startup-deadline
checks and supports a fully stopped in-process restart without weakening the
executor-independent hard process watchdog. The `supervision-vopr-test` gate
exact-replays clean startup, partial-startup rollback, shutdown during startup,
child-service failure, coordinated shutdown, virtual watchdog expiry, and
restart. It proves readiness is published only after all children start, first
failure cancels the process, rollback and shutdown release every child, and a
new generation can become ready before its own coordinated teardown.

##### User and Authentication Lifecycle

`UserManager` now borrows `std.Io` for password salts, API-key identity and
secret generation, realtime expiry checks, and its mutation/seed-capture
mutex. Production standalone, metadata, and data roles pass their process
runtime explicitly; the manager no longer creates hidden `Threaded` executors.
Production-neutral lifecycle events identify user persistence/publication,
password persistence/publication, API-key persistence/publication/revocation,
permission changes, and row-filter changes.

The `auth-lifecycle-vopr-test` gate runs the real manager and stores through
password rotation, API-key rotation, permission and row-filter changes,
revocation with an already materialized reader, durable reload, and an injected
crash between user persistence and policy publication. A separate fiber
schedule holds the real seed-capture lease, forces a password mutation to park
on the production `std.Io.Mutex`, and proves it cannot finish before capture
releases the lease. Every lifecycle history exact-replays with deterministic
randomness and time.

##### Serverless Object-Store Protocols

Real WAL, catalog, manifest, artifact, and progress-store operations now run
over the reusable `ScriptedFaultClient`, covering partial committed transfer,
delayed visibility, duplicate completion, timeout-after-commit, cancellation,
retry, publication, reconciliation, and client crash. Object-backed WAL append
now also accepts a durable caller-supplied operation identity: retry after an
ambiguous timeout returns the original LSN, conflicting reuse fails closed,
and the identity survives read and truncation. Stores that cannot uphold the
contract reject idempotent append instead of silently degrading it. The full
standby seed backup/restore workflow uses the same provider: a committed chunk with
a lost response is reconciled by a restarted publisher, repeated publication
selects the same generation, cancellation before restore staging is harmless,
and retry downloads and verifies the complete chunked artifact. Extend these
protocol campaigns when new production object-store consumers are introduced.

##### Complete Serverless Workflow

Serverless maintenance now uses a durable object-store work lease with retained
monotonic fencing tokens, conditional acquire/renew/release, explicit expiry,
timeout-after-commit reconciliation, and publication guards checked at the
builder and compactor head CAS. A released lease remains as an expired record
so tokens cannot move backwards. A long-running worker may renew the exact
owner/token at cutover when nobody took over; once another worker advances the
token, the stale worker fails closed even if it already produced artifacts and
a manifest. Production bootstrap enables the shared lease lane by default with
a per-process identity generated from its borrowed `std.Io`.

`BackgroundPublisher` no longer owns a native thread: it borrows `std.Io`, owns
one Future, reports its first failure, and cooperatively joins on shutdown.
`ManagedRuntime` applies the same lease to publication and compaction, records
claim conflicts and takeovers, and preserves progress CAS as the final durable
visibility boundary.

The `serverless-workflow-vopr-test` gate composes the real WAL, builder,
artifacts, manifests, catalog, progress store, runtime, compactor, and query
session over independently faultable object-store lanes. It exact-replays clean
execution, duplicate workers, expired-lease takeover with stale publication
fencing, ambiguous head publication, cancellation before head publication,
retryable artifact failure, crash after committed manifest, and ambiguous
compaction publication. Every history restarts the production runtime from
durable state and proves both documents are visible from the compacted catalog
head.

##### DB and Index Request Races

The `db-index-race-vopr-test` gate replaces thread-timing regressions with
production-safe operation boundaries and nonblocking protocol microsteps. It
exact-replays both durable managed-admission linearizations (materialize then
delete, and delete then materialize) and proves they converge without an
orphaned repair intent. The dense published-reader/catalog-writer campaign
drives the real lock-free admission word one transition at a time: a reader
registered before closure keeps the writer undrained until release, while a
reader arriving after closure is fenced to the locked path before catalog
deletion.

The same gate runs the production text-merge admission queue on borrowed
`VoprIo`. It proves that an index-local segment waiter does not block an
independent index, older same-index work retains weighted FIFO priority,
cancellation removes its waiter without poisoning later admission, and runtime
shutdown wakes a blocked producer with the shutdown outcome. These histories
exercise the real queue, permits, futex wakeups, catalog admission atomics, DB
deletion, and durable repair cleanup; they do not mechanically reproduce the
old native test threads or suspend while holding an apply/structural mutex.

##### Admission and Resource Pressure

The production resource manager now runs under replayable contention schedules
that prove hard-limit denial, idempotent release, accounting, and capacity
recovery. A composed production request acquires foreground admission, a
multi-slice batch, and scratch memory; cancellation at each admitted edge
returns every reservation. Priority campaigns prove that background soft
pressure does not block unrelated foreground work and that bounded oversized
single-work admission provides exactly one minimum-progress grant while
rejecting a concurrent contender. `VoprIo` independently enforces task, CPU,
allocator, file, socket, storage, and queue limits, and the DataServer covers
socket admission. Continue composing these policies into deeper DataServer
request microsteps as those seams are added.

##### Provider Boundaries

`provider-boundary-vopr-test` executes the real `ManagedEmbedder` local-provider
boundary and the real PostgreSQL `RuntimeSource`/`QueryExecutor` boundary around
deterministic response adapters. Its exact-replay histories cover valid,
partial, malformed, timed-out, cancelled, transient, and retry-then-success
responses. The suite proves dense batch cardinality/dimension/finite-value
validation, local error normalization, PostgreSQL SQL construction and
cancellation propagation, and zero leaked foreground admission on every return
path. `RequestAdmission.Lease` makes that ownership single-release and explicit
for production callbacks.

Actual model execution, GPU kernels, and libpq internals remain in their
existing differential and integration tests. This is intentional: VOPR owns
the deterministic application boundary, not a substitute implementation of a
provider runtime.

##### Composed Query Lifecycle

`composed-query-vopr-test` treats text, vector, and graph completion as
independent stable transitions, then executes the production distributed
`mergeSearchResults` and graph-union implementation at the global publication
boundary. It exact-replays every component completion ordering, graph partial
failure followed by retry, early and late cancellation, admission pressure,
capacity release, and final reassembly. Properties prove that no partial or
cancelled result is published, the canonical text/vector/graph set survives
every assembly ordering, graph results remain attached, and final admission
ownership is released.

## Ongoing Roadmap (long form) (originally lines 2159–2264)

#### Ongoing Roadmap

> **Relocated:** The version-by-version roadmap narrative that previously lived here (445 lines) is
> preserved verbatim in [docs/design/vopr/history/status-history.md](status-history.md#ongoing-roadmap-narrative-relocated-from-voprmd).
> Durable decisions from it are folded into the compact roadmap below. The former "shortest current
> summary" and "detailed backlog" lists narrated the same seams in the same order, so they are merged
> into one list here.

1. **Deepen the promoted full-cluster reconfiguration seam.** The seam exact-replays a three-owner
   merge-to-split reconfiguration behind the real metadata quorum, three `DataServer`/data-Raft
   owners, public clients, two tables, and the serverless catalog, with predecessor-fenced sparse
   deltas, durable destination ranges, and restart-safe reconciliation. Composed on that owner set: a
   depth-two public graph, owner transport/process/short-write faults, memory pressure, listener
   connection-limit denial, hydration/durable-shuffle cancellation, partition-worker failover,
   distributed joins under cancellation/authorization/transport faults, a shared query-cache domain,
   replication under schema/leader/lease faults, and a bounded managed-index lifecycle.
   - Complete the managed-index lifecycle: provider retry, coverage/replay publication, owner
     reconstruction, all-node semantic recovery, and packet-level replay stabilization.
   - Add cancellation under storage/disk faults and simultaneous process loss before cancellation
     drain; cover the remaining topology, restart, short-write, and socket-pressure breadth.
   - Remove the co-location assumption (test disjoint donor/receiver replica sets); page retained
     delete-history replay within a resource budget; inject partitions.
   - Prove snapshot install rehydrates each live DB owner's derived graph/index state and the Raft
     projection.

2. **Deepen public distributed operations and cross-range graph composition.** The public request
   path composes cross-owner joins during active splits, finalizer takeover, document hydration with
   cancellation, permission revocation mid-request, split publication behind a retained snapshot,
   transport-outage and durable-shuffle cancellation, partition-worker failover, process
   destruction/reconstruction, an ordered two-table global-query baseline, and the first managed-index
   progressive-readiness lifecycle — all with typed no-partial failure and exact recovery.
   - Add cancellation under storage/disk faults combined with process loss and other combinations;
     extend joins to right/nested/foreign and multi-range-left shapes; add global queries under the
     same fail-closed rule with broader topology and process-loss histories.
   - Introduce an explicit partial-response schema only if product semantics ever require it.
   - Complete and stabilize the managed-index pending-to-ready path, then extend it across
     atomic-vs-progressive policy, rate-limit/disk/cancellation overlap, alias fencing, and
     publication-gap crashes.
   - Generalize the synchronous applied-index/derived-visibility read barrier to every production
     strong-read owner with a distinct blocking boundary; every incomplete fanout must keep failing
     closed as this expands.

3. **Compose independently proven fault domains, including standby and the data plane.** Already composed:
   link-plus-resource overlap under one quiet-suffix oracle, selected-node disk-capacity
   denial/healing at the persistent-cache reservation consumer, reversible operation costs as a fault
   distinct from CPU-work exhaustion, a shared query-cache domain, replication under
   schema/target/session/lease/checkpoint faults, generation-cutover CAS races, and two-tenant
   concurrent work with exact cross-tenant denial evidence.
   - Co-locate standby and the data plane; add managed-index and graph/query disk-pressure combinations;
     broaden listener-pressure targets to disk-capacity and wider socket surfaces.
   - Extend the fault algebra across storage, restart/process, serverless lease/object-store, and
     multi-owner faults; overlap operation-specific slowdown with cross-domain ownership and public
     deadlines.
   - Broaden fault combinations at the already-complete named seams (metadata/placement Raft wire hop,
     serverless public catalog path, memory-pressure recovery, quiet cluster-wide suffix) rather than
     rebuilding them.

4. **Broaden workloads at those same seams.** Two disjoint authenticated table identities already run
   concurrent authorized work with exact bidirectional denied-read, denied-write, and absence
   evidence.
   - Add row-level tenant scoping and identity mutation races, concurrent range-split/replicated-merge
     routing changes, and fairness between clients and background workers.
   - Extend per-node disk-capacity and socket interference beyond current memory/listener pressure.
   - Add remote generation/reranking adapters, metadata-admin mutations, MCP/A2A orchestration,
     cloud-auth refresh/signing, and bounded extension invocation.
   - Close the remote-content live-secret boundary: resolve preserved credential/header references at
     the actual request, rotate mid-request, and prove coherent per-request generations across
     retry/cancellation/refresh/crash-reopen.

5. **Operationalize and maintain the self-contained platform tooling.** The command composer,
   determinism audit, phased health adapters, recorder/event queries, debug recipes, results/index
   APIs, corpus merge, and injected-bug benchmarks are implemented and produce recurrence/rarity
   evidence. A suite counts as integrated only while its scenario module is forced into test
   discovery, its command runs at least one passing exact-replay test, and it remains a `vopr-test`
   dependency.
   - Gather evidence from the scheduled sharded campaigns and replay-validated corpus retention; add
     quarantine review, usage indexing, notifications, dashboards, and search-quality regression
     tracking; wire recurrence/rarity reports into nightly retention.
   - Adopt the registered-deployment composer beyond full-cluster in the standby, data-plane,
     distributed-transaction, and serverless suites so node identity, readiness, fault scope, and
     quiet-suffix obligations stay uniform.

6. **Expand the determinism audit toward full transitive coverage.** The explicit source-manifest gate
   is real and green, but transitive coverage of the production call graph reachable from every
   borrowed-`std.Io` scenario seam is not complete. Optional/default `Threaded` ownership (A2A/MCP
   orchestration, cloud-auth and object-store constructors, remote-provider adapters) must receive the
   scenario's borrowed `std.Io` before joining an exact-replay composition; native fallbacks remain
   physical differential paths, not VOPR evidence. Source-to-source cache transfer and stale-cache
   pruning still use address-ordered dual locking in production paths the current topology does not
   exercise.
   - Expand toward the transitive call graph continuously as sources are exported; fail closed on
     pointer-derived ordering/identity, host clocks, native threads/I/O, filesystem escapes, native
     libraries, and unordered iteration unless a narrow reviewed exception applies; preserve
     Threaded/physical-backend differential tests to detect simulator drift.
   - Before the cache-transfer/pruning paths become replayable, give each owner a stable semantic lock
     key or route both operations through one coordinator; do not simply reverse lock order.

7. **Keep distributed fidelity explicit.** The integrated in-process multi-node mode remains the
   vehicle for production owners that borrow `std.Io`; unmodified container runs are classified as
   differential, never exact replay.
   - Add live rolling mixed-version operation only once two runnable compatible binaries and an
     explicit upgrade contract exist; current artifact-compatibility and golden-reader campaigns are
     the prerequisite, not a claim of live mixed-binary execution.
   - Add a federated agent/broker only for a demonstrated separate-address-space or mixed-binary
     requirement (see Conditional Work above for the other gated additions). Do not build a hosted
     graphical debugger or a deterministic-hypervisor clone for nominal parity alone.
