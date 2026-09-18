# Workload scheduling implementation record

Design: [WORKLOAD_SCHEDULING.md](WORKLOAD_SCHEDULING.md).

The design was committed first as `74820f0ec2`. Implementation is in progress.
The current implementation provides fixed foreground admission with opt-in
bounded waiting, shared dense driver/helper scheduling, and bounded SDK read
retries. Dense native reads have one audited suspension boundary, and an opt-in
join-row worker has durable attempt ownership. Coordinator attempt dispatch
remains disabled. These stages do not complete the operator scheduling design
or qualify new defaults.

## Implemented admission contract

`common/workload_admission.zig` owns an allocation-free intrusive FIFO, active
operation count, queued count/bytes, and retained request/allocation reservations. A grant
removes queue accounting while preserving the request's byte reservation until
its lease releases. Cancellation rejoins the queue lock before retiring waiter
storage; a concurrent grant returns its execution and byte reservations exactly
once. Waiters use the caller's runtime and deadline clock. Queue reductions
retire newest excess waiters, preserve the oldest requests, and never revoke
running work. Closing admission rejects new work and retires waiters while active
leases remain owned by their callers.

All operations admitted here retain coarse operation leases. There is no claim
that these counts measure busy CPU threads. Payload reservations cover request
bodies and a conservative metadata allowance. When a retained-byte ceiling is
configured, tracked query decoding/planning and output allocations additionally
reserve their actual size before allocating. Other execution allocations still
depend on their storage resource limits; this is not a process-wide memory cap.

`common/workload_allocator.zig` supplies heap-stable reference-counted allocation
owners. Request completion releases execution capacity while buffered output
retains its allocation charge through response drain. A detached memory account
allows outstanding responses to retire after server destruction without touching
the former admission controller. The compiled API bridge retains both the
foreign response and its allocator descriptors instead of copying the body.
Serverless adapters transfer the buffers and their owner together. Large HTTP/1
responses send directly from their retained body instead of building a second
full response buffer. Transport/header metadata and unconverted streaming paths
remain outside this allocation owner.

Serverless query sessions now receive the admitted request allocator explicitly
for manifests and read buffers, while shared caches, metrics, and lease-cache
synchronization retain their runtime owner. A live query runtime is not copied
to change its allocator. The regression executes a real manifest/fragment query,
checks retained output ownership, and rejects runtime allocation growth at the
configured ceiling. This extends coverage beyond response-adapter fixtures.

Tracked allocation exhaustion unwinds without waiting while holding a partial
bundle. Output or execution failures report `execution_started=true`, preserving
write ambiguity and preventing automatic retries. Small structured error
responses use the underlying allocator so an exhausted query budget does not
prevent rejection. Actual backing-allocation failure remains distinct from a
configured resource ceiling.

The REST/httpx, alternate-listener API-kernel paths, MCP, query builder, A2A,
extension-host query/write calls, and serverless query/write handlers share this
owner at their existing admission boundaries. Legacy nonwaiting callers cannot
jump ahead of queued work. Metadata/data teardown can close admission across the
compiled API boundary. The current API ABI is 23, storage-owner ABI is 63, and
native runtime ABI is 8; these include admission diagnostics, dense I/O context,
executor capabilities, and worker configuration. Incompatible layouts are
rejected. No inference-provider admission or transaction durability contract
is replaced by this queue.

Public document lookups (`lookupKey` and its database/namespace alias) and MCP
`get_document` now share the query gate. They previously bypassed it. This closes
a data-read admission gap using the existing configured query capacity (32 by
default), so concurrent lookups can now receive 429 without opting into waiting.
No separate lookup gate or new default capacity was introduced. HTTP lookup
projections/results and the MCP application result use the query allocation
owner. Incoming request cancellation/deadlines reach storage lookup options and
readiness retries; a late result is freed before returning timeout/cancellation.
The final MCP protocol envelope retains its existing adapter ownership. The
serverless route inventory has no document-lookup endpoint; its existing query
routes already use query admission. Metadata, readiness, metrics, and control
routes retain their existing bypass policy. This behavior change still requires
release qualification; it is not evidence that all overload timeouts are fixed.

Query and write classes have independent fixed count/byte budgets. This preserves
their existing isolation and does not introduce borrowing between them. Configured
and effective queue bounds, queue residence histograms, active/queued counts,
retained bytes, expirations, cancellations, and draining are observable through
the shared admission metrics. Explicit zero execution capacity retains its legacy
unlimited meaning; explicit byte ceilings still apply to contextual leases.

The shared metrics exporter includes a process-local policy generation, active
plus queued operation count, and bounded rejection reasons. Execution capacity,
queue count, queue bytes, total retained bytes, oversized requests, wait expiry,
draining, and policy reductions remain distinguishable. Allocation-attempt
denials have a separate counter: a failed allocator growth is not another
rejected request. Providers that expose only legacy statistics report diagnostics
as unavailable rather than implying zero pressure. These are engine contracts
for Cloud observability; no Cloud dashboard or automatic scaling is implemented.

The Go, TypeScript, Python, and Rust SDKs have optional shared client pools. They bound active operations and
waiting, observe context cancellation before dispatch, and hold a slot through
response EOF/Close. They preserve unknown write outcomes and do not add automatic
write retries. TypeScript shares pools across database/inference clients; Python
shares a FIFO across synchronous threads and generated asyncio calls. Python's
canceled stream cleanup retains a slot until its separately owned cleanup task
finishes. Rust's `PooledClient` constructs the request future after admission and
retains capacity in an `Admitted<T>` wrapper through stream ownership.

Go, TypeScript, Python, and Rust provide opt-in bounded read retries. Only known
query POST routes with replayable request bodies and an explicit HTTP 429
`instance_busy` / `admission` / `execution_started=false` response qualify.
Transport failures, writes, ambiguous outcomes, and delivered successful streams
do not retry. Attempts release pool capacity before bounded backoff, respect
Retry-After or decline the retry, and preserve cancellation and the original
deadline. Rust exposes a query-request wrapper; arbitrary generated operation
closures are not automatically retried. Python synchronous transport timeouts
remain per-I/O limits, and streaming callers must bound subsequent consumption.

Public query timeouts are captured before admission and retained through catalog
binding, execution, and readiness retries. Native and runtime clock domains are
translated by remaining duration without extending the deadline. NDJSON batches
use one submission time and the shortest explicit timeout; later lines cannot
restart the budget. Deadline extraction skips unrelated JSON fields rather than
allocating a full vector/document tree before admission.
Serverless execution carries the captured deadline through a scoped cancellation
token. HTTP token adapters preserve fallible checkpoints and their timeout cause.

## Execution ownership core and dense integration

`common/workload_resources.zig` implements typed, generation-checked leases over
a fixed metadata pool. Atomic count/byte/working-set grants preserve protected
floors for each lane. Queue, runnable, retained state, resume queue, local I/O,
remote attempts, and recovery credits have distinct ownership. A request cannot
release live children; detached state transfers ownership without changing global
charges. Policy reductions preserve live reservations while rejecting growth.

`common/workload_scheduler.zig` supplies weighted service accounting corrected by
measured work, bounded resource-fit backfill, and deterministic large-request
barriers. Timer polling does not earn service credit. Existing continuations
reuse their reserved metadata and state when queueing for resumption, so full
start queues cannot prevent their progress. They precede new starts in their lane.
Grant/cancel races restore retained state to its caller, and resume cannot reduce
the charge below the state still owned. Transfers reject active execution or
queued ownership so scheduling identity cannot change underneath a waiter.

These modules provide tested foundations. Their credit ledger is not a replacement
for resource-manager allocation reservations, worker fencing, or durable write
recovery. The opt-in dense binding uses the scheduler for HBC drivers and vector
read helpers. Broader operator integration must supply audited completion bundles
and separately charged actual allocations.

`admission.dense_execution` activates a heap-stable ledger and scheduler in the
actual storage ResourceManager, including the compiled storage-owner boundary.
Its explicit runnable/outstanding/queue/wait limits preserve the legacy path when
omitted. Drivers and helpers each hold owned leases; helpers never wait and
release their permits at worker completion. Original query deadlines reach dense
queues and work through a scoped cancellation adapter. A driver retains its coarse
permit through unaudited scan/rerank waits and helper joins. This does not claim
CPU-only accounting or protected-lane latency isolation.
`antfly_dense_execution_*` metrics expose configured limits and current ownership
from the actual storage owner. Optional `max_working_bytes` bounds tracked exact
dense working memory.

`max_suspended_io` additionally opts native immutable exact-vector positional
reads into suspension. The HBC admission owner is borrowed explicitly through
the search transaction and callback scratch. After any preceding helper wave
joins, the serial read retains its request, scoped destination/read arena, and
a bounded local-I/O lease while releasing runnable ownership. It reacquires
runnable ownership through the scheduler before decoding or continuing. The
original deadline/cancellation still applies; a failed resume unwinds retained
state without releasing a nonexistent runnable lease. An unavailable I/O credit
keeps the caller coarse. The synchronous operating-system read is not preemptible.

Existing enclosing HBC/source snapshot scopes still require caller-thread
affinity. The originating Threaded executor proves that capability, and the
versioned runtime bridge preserves it across the compiled storage boundary.
Unknown or migrating providers retain coarse ownership. Tests cover both
capability outcomes in the compiled storage owner. Portable suspension still
requires refactoring those existing scopes. Projection/residual helper work,
mapped-view shortcuts, and other unaudited paths remain coarse; this is one
production boundary, not general cooperative execution.

`storage/workload_memory.zig` connects an allocation owner to both the class
ledger and `ResourceManager`. It reserves both without waiting or invoking
reclaimers while holding a partial bundle, rolls back failed grants/allocations,
and preserves pinned minimum completion memory across suspend/resume. Actual
freeing precedes returning byte credits. Exact dense scoring installs a scoped
owner for candidate sets, metadata, vector scratch, and batch buffers, with
allocation failures mapped back to structured resource rejection. Escaping search
results retain their existing allocator owner. Cached HBC scratch needs a durable
pool owner before it can be charged here; it is not attributed to a temporary
request owner. Opted-in native exact reads use a scoped read arena rather than
borrowing pooled scratch. Their actual arena allocations are charged without
also charging the same bytes as an estimated native-I/O workspace.

`common/workload_attempts.zig` models bounded coordinator attempt ownership,
pre-reserved reconciliation capacity, deadline-preserving retransmission, and
destination isolation. A timeout does not release uncertain work. Authenticated
terminal/quiescence evidence reconciles once; delayed evidence cannot reopen a
fenced generation. Restart begins with dispatch disabled until each destination
acknowledges the previous generation is fenced and quiescent. Production
coordinator wiring, trusted membership/incarnation discovery, and complete
shutdown/restart reconciliation remain necessary before enabling it. The module
does not claim that an ordinary HTTP error proves remote retirement.

The authentication prerequisite signs bounded, issuer/domain-separated attempt
and terminal/fence frames. It verifies the active plus rotation key, binds the
request content/route and canonical authenticated node identity, and projects
verified identity into the internal operation context. An opt-in client accepts
only matching signed terminal evidence; HTTP success without it remains
`AttemptOutcomeUncertain`. Existing requests omit this protocol. Authentication
alone does not establish quiescence.

`admission.remote_attempt_worker` installs a durable deduplication and
generation-close owner for authenticated internal `join-rows` requests only.
It requires the durable API session backend, a runtime-supplied node identity,
and internal service authentication. Records bind attempt identity to request
content. Active duplicates do not execute; terminal duplicates return evidence
of completion without replaying a saved result. Workers preserve the earlier
incoming deadline and local `max_run_ms` ceiling. Deadline expiry requests
cancellation but never proves quiescence. Terminal evidence is signed only after
the actual handler unwinds and its terminal state is durable.

Generation closure is persisted before cancellation is signaled, and fence
evidence waits for local work to unwind and terminal state to persist. Count and
byte ceilings include outstanding/uncertain attempts, terminal tombstones, and
fences. Restart retains unknown prior-incarnation work and its reservations;
capacity reduction cannot erase it. The v2 journal stores individual attempts,
transactional count/byte accounting, and per-coordinator closure keys. Begin,
finish, duplicate lookup, and usage use a fixed number of point operations;
only startup reconstruction and generation fencing scan bounded records.
Startup refuses a legacy v1 snapshot with `WorkerJournalMigrationRequired`;
an offline migration must fence earlier writers and preserve their obligations.
Performance qualification and coordinator reconciliation must precede rollout.
The setting does not enable coordinator dispatch or automatic remote retirement.

The existing distributed join RPCs now emit `budget_version=1` and validate an
explicit version on workers. Workers capture their receive deadline before JSON
decoding and keep the earlier caller deadline through typed dispatch. Legacy
requests without a version remain accepted. This fixes deadline renewal; it does
not enable the remote-attempt ownership model.

Stable transaction sessions support optional `transaction_sessions.max_recovery_count`
and `max_recovery_bytes`. Before marking commit execution started, the durable
store scans authoritative session records inside its serialized write transaction.
It reserves record capacity for each pending recovery obligation, including
records predating the recovery index. A full budget rejects before participant
preparation; an already pending transaction may complete under reduced limits.
Restart derives ownership from durable records, and terminal acknowledgement
retires the obligation. These bounds cover durable session records, not all
decoded 2PC memory or stateless-write recovery.

Background status refresh now treats `ConcurrencyUnavailable` from the finite
durable executor as a deferred submission rather than a fatal data-control
error. A 100 ms retry allowance retains dirty/forced wakes and clears failed
active ownership. Adjacent startup catch-up, index repair, and provisioning
also retry capacity failures; permanent submission errors still propagate.
Local-group status refresh frees a rejected captured snapshot exactly once and
uses its existing cached/Raft-only fallback while waiting. The new submission
deferral counter distinguishes this pressure from failed refresh execution.
Control/Raft executors remain separate, but this fix does not establish
process-wide protected floors or maintenance fairness under sustained saturation.

## Configuration

The canonical schema exposes `admission.query.waiting` and
`admission.write.waiting`. Both default to fail-fast behavior. An illustrative
fixed policy is:

```yaml
admission:
  query:
    max_concurrent_requests: 32
    waiting:
      max_queued_requests: 64
      max_queued_bytes: 16777216
      max_retained_bytes: 33554432
      max_wait_ms: 100
```

These values are examples, not qualified defaults. Positive wait ceilings require
positive queue count, queue bytes, and retained-byte bounds. Queue bytes cannot
exceed retained bytes. Waits are additionally limited by the original request
deadline; maximum configurable waiting is 60 seconds. A request larger than its
entire retained-byte envelope fails immediately without retry guidance. Queueing
does not raise a transport's independent connection or request-task limit.

## Operator and ownership audit

| Execution path | Current owner and boundaries | Requirement before fine-grained scheduling |
| --- | --- | --- |
| Public queries, document lookups, and writes | Admission lease held around the existing synchronous operation, including joins of its helpers; lookups share query capacity | Split runnable, retained state, and request lifetime at verified quiescent boundaries |
| Query decoding/planning | Foreground body reservations plus tracked public single/NDJSON query and serverless query allocations; some catalog/auth/session resolution precedes the grant | Complete all frontend coverage and introduce separately protected planning capacity |
| Dense rerank and helpers | Shared scheduler owns drivers/helpers; exact workspaces and opted-in native read arenas own actual bytes; serial immutable pread suspends only with proven executor affinity | Audit remaining boundaries; remove enclosing thread-affine scopes for portable suspension; add durable ownership for cached HBC scratch |
| Vector, text, graph, aggregation | Existing cancellation/work budgets and storage resource reservations; no scheduler continuation contract established | Inventory maximum nonyielding intervals, resumable state, and minimum completion resources; remain in the general lane until verified |
| Scan/stream output | Tracked buffered bodies retain actual allocation charges through transport drain, including API-kernel and serverless handoffs; streaming snapshots remain storage/transport-owned | Complete streaming retained-state ownership and resume only through admission |
| Remote coordinator/worker tasks | Versioned remaining budgets; authenticated opt-in join-row worker persists deduplication, terminal state, and generation closure; coordinator is still disabled | Wire coordinator ownership, membership/incarnation discovery, destination uncertainty, restart reconciliation, and durable-storage qualification |
| Transaction commits | Stable sessions reserve bounded durable recovery-record capacity before prepare; existing decisions, fencing, and `PendingSessionRecovery` remain authoritative | Extend coverage to stateless writes and decoded mandatory-completion resources |
| Background/control/recovery | Existing dedicated runtime owners; status/maintenance submission pressure retries without terminating control | Prove process-wide protected count/byte/progress floors and sustained-load fairness across foreground and background work |

No operator is admitted to a new protected bounded-plan execution lane by this
change. None releases execution on an unverified suspend boundary. Existing
committed-write recovery is preserved; client cancellation must not be used as
evidence that a write was rolled back or that remote work quiesced.

## Validation

```sh
cd zig
zig build antfly-common-config-test -j2
zig build antfly-workload-admission-test -j2
zig build runtime-io-abi-test antfly-storage-owner-test -Dstorage-owner-test-filter='dense execution policy' -j2
zig build antfly-data-runtime-test -j2 -- --test-filter 'data runtime status refresh retries bounded executor pressure without losing wakes'
cd ../go/pkg/sdk
go test -race ./...
cd ../../../ts
pnpm --filter @antfly/sdk test
pnpm --filter @antfly/sdk typecheck
cd ../py/packages/sdk
uv run pytest
uv run pyright
cd ../../../rs
cargo test -p antfly-sdk
```

The dedicated Zig target covers FIFO pressure, count/byte bounds, deadline/grant
races with VOPR time, cancellation/grant races, policy reductions, draining,
compiled API ownership, legacy MCP responses, and serverless behavior. Controller
burst tests cover C1/5/10/20/30/40/60/80 with a fixed execution capacity of 32.
Allocation regressions cover failed growth, policy reductions, response lifetime
beyond execution/context/server destruction, concurrent teardown, both sides of
the API allocator boundary, and serverless adapters. Real exact-scoring tests
observe memory and runnable ownership during execution and verify complete
retirement. A gated real positional read observes retained bytes/outstanding/I/O
ownership with runnable ownership released, another request progressing, and
deadline/cancellation cleanup. A full-handle test verifies suspended owners can
resume without fresh metadata capacity. Executor archive and compiled-owner
tests verify that only proven affinity enables this path. The real HTTP C80
fixture verifies 32 admitted plus 48 queued operations; its missing-table
responses test transport admission, not database query throughput.
The C80 lookup variant holds 32 active and 16 queued reads and rejects the
remaining 32, then verifies all request/output charges retire. Lookup fixtures
also cover both public aliases, MCP application results, cancellation, original
deadlines, and output lifetime after execution/context retirement.
Network tests need permission to bind local listening sockets.

The focused data-runtime gate covers actual exhaustion of an eight-slot durable
executor, preserved wakes, bounded retry timing, permanent-error propagation,
captured-snapshot failure cleanup, and refresh completion after capacity returns.
Worker tests exercise real authenticated join-row dispatch, durable duplicates,
generation closure, failure to persist terminal evidence, reopen, and reduced
capacity. Point-journal regressions also verify legacy-format refusal and that
only actual previous-executor completion can reconcile its retained uncertainty.
These tests do not constitute distributed coordinator qualification.

Native Debug smoke exposed both a fatal background-capacity escape and a query
allocator ownership mismatch. The control-capacity regression is fixed and its
focused gate passed. The standalone allocator investigation and native rerun
remain in progress; no successful current native smoke or performance result is
claimed here.

## Remaining design phases

| Design phase | Current status |
| --- | --- |
| Ownership/progress prerequisite | Typed ledger, scheduler, allocator, and attempt state models are implemented and tested; complete operator inventory and process-wide progress proof remain open |
| Phase 1 | Fixed foreground waiting, contextual allocation ownership, deadlines, overload diagnostics, and SDK contracts are integrated on the paths above; full ingress/planning/output coverage and protected floors remain open |
| Phase 2 | Dense driver/helper ownership, exact working bytes, one pinned-executor I/O boundary, session recovery bounds, and opt-in durable join-row workers are integrated; full operator scheduling, fairness, distributed coordinator ownership, and all write recovery remain open |
| Phase 3 | SDK pools/retries, engine diagnostics, and qualification tooling exist; native smoke completion, optimized release qualification, Cloud integration, sizing/defaults, and adaptive policy remain open |

- Audit the tested ownership foundations against real continuation, remote
  attempt, mandatory recovery, and demotion paths; extend deterministic coverage
  for the integration boundaries.
- Complete Phase 1 ingress/planning/output byte ownership, process-wide protected
  floors, and all SDK/Cloud error contracts. Byte accounting now covers foreground
  request reservations and the tracked query/output paths above; remaining
  allocations must acquire an appropriate durable or scoped owner.
- Complete Phase 2 beyond the dense boundary: operator suspend/resume and
  completion bundles, production resource-fit backfill and measured service
  debt, bounded/general lane isolation, demotion transfers, broader helper and
  shard fan-out accounting, coordinator uncertainty fencing/reconciliation,
  streaming retained state, and all irrevocable-write recovery handoffs. The
  scheduler model, dense binding, and opt-in worker are partial integrations.
- Complete Phase 3 Cloud diagnostics
  and policy integration, adaptive control behind an explicit mode, and deployment
  qualification. Apply the numerical acceptance thresholds already recorded in
  the qualification matrix and retain both open-loop and closed-loop results
  before changing defaults.

Passing queue correctness tests does not satisfy the full design's release gates.
Automatic/adaptive modes and new default waiting policies remain unavailable.
The [qualification matrix](WORKLOAD_SCHEDULING_QUALIFICATION.md) records actual
Cloud package sizes and numerical release thresholds selected before measurement.
Debug is the normal development correctness gate; final performance runs use
the shipped optimization mode. Qualification uses direct processes and containers
with explicit Cloud resource limits. Deployment integration tooling is separate
work.

The [fixed-policy operations guide](WORKLOAD_SCHEDULING_OPERATIONS.md) describes
configuration, diagnostics, timeout/retry behavior, and qualification boundaries.
