# Workload scheduling implementation record

Design: [WORKLOAD_SCHEDULING.md](WORKLOAD_SCHEDULING.md).

The design was committed first as `74820f0ec2`. Implementation is in progress.
The current implementation provides fixed foreground admission with opt-in
bounded waiting, shared read driver/helper scheduling, and bounded SDK read
retries. Audited dense I/O and streamed scans can suspend with prepaid state;
narrow LMDB existence probes have an opt-in protected partition. Opt-in internal read workers and coordinator dispatch have durable attempt
ownership; local response-loss, restart, and destination-isolation scenarios now pass. These stages do not complete the operator scheduling design
or qualify new defaults.

## Review order

The commits preserve implementation stages and subsequent integration fixes.
Review the final tree with those fixes applied; intermediate commits are not
independently qualified release artifacts. The following groups identify the
main contracts and representative commits, rather than an exhaustive history.

| Stage | Contract to review | Main commits |
| --- | --- | --- |
| Design and scope | Ownership model, fixed-policy rollout, local qualification; deployment tooling deferred | `74820f0ec2`, `64f1afbb37` |
| Foreground and SDK admission | Bounded FIFO, original deadlines, optional shared pools, explicit safe read retries | `c1f58483f4`, `ee9e19c705`, `d88ab51e6b`, `ea4d822345`, `dc1d530f0a`, `10a7369392`, `56bba4933f` |
| Execution and memory | Typed ownership, shared dense driver/helper capacity, actual working bytes, audited native read suspension | `44021169fe`, `1ec9843299`, `3cc6c9b647`, `849d822623`, `508311b930`, `9c687e2670` |
| Request/output lifetime | Tracked allocation ownership through kernel, HTTP, and serverless handoffs; lookup admission | `1b527c5129`, `42b8c19983`, `fe9ede7c7c`, `afe3c72ae3`, `803b886294`, `85c22d2c8b`, `3d9b3aaa1b`, `8213869f68`, `9920f56295`, `18af5c041a`, `288746b7c5`, `95aba99ad4` |
| Recovery and remote workers | Reserve durable obligations before prepare; authenticate attempts; persist opt-in worker deduplication and generation closure | `341f08a8b5`, `43ed43cabd`, `0d07db7932`, `b11a01530b`, `7fc30a22ef` |
| API replay and storage recovery | Prepaid replay; two protected memory lanes; durable table recovery credits and guarded legacy completion | `de95b61f4f`, `bd0b9f5585`, `38bfa197af` |
| Retained joins and distributed integration | Bounded persistent join state, allocation-failure cleanup, protocol-3 coordinator/worker ownership, protected control ingress, compiled runtime configuration | `48baab7337`, `63f2eb025f` |
| Native integration corrections | Table recovery-policy persistence and decoder activation; error-safe read handles and reverse replay visitors across compiled boundaries | `c10e602778`, `b6dfbb8409`, `9c834c7d45` |
| Local fault and policy plans | Advertised transport faults, signed fixture evidence, separate actual candidate policies and scaled correctness topology | `c6122532ea`, `1018036e1d`, `121d96fbf3` |
| Runtime pressure | Defer background submissions on finite executor pressure; preserve admission failures across the compiled callback ABI | `9511272560`, `2be51d61ad` |
| Evidence and operations | Retained native/container harness, vector calibration, numerical Cloud gates, explicit failure outcomes and telemetry validation | `ebe085a24d`, `0f9a1a05f2`, `dff1a3d9f7`, `0c9395fc32`, `fc2d5878ec`, `f550aefaf0`, `811956a541`, `c6ad0692ec`, `488637eb6b` |

The [validation record](WORKLOAD_SCHEDULING_VALIDATION.md) identifies exact tested
source revisions and failures. The remaining-phase table below distinguishes
implemented mechanisms from unfinished integration. In particular, fixed-policy
waiting and dense scheduling remain opt-in, and coordinator dispatch requires explicit configuration and compatible peers. Document lookups now joining the existing query gate is an intentional
default behavior change that reviewers should assess explicitly.

## Follow-up implementation stages

These review stages subdivide the broader design phases below. Stage 1 closes
frontend ownership; stage 2 extends read execution. They do not imply completion
of distributed coordinator recovery or release qualification.

The stage 1 follow-up adds:

- Actual HTTP body-buffer capacity accounting, including simultaneous old/new
  buffers during growth, and response retention through native/linked drain.
- Cancellation and deadline checks during streaming, stalled H2 progress limits,
  committed-stream reset handling, and transport-neutral stream commitment.
- An optional ingress request/byte envelope with a nonborrowable empty-probe
  partition. Accepted request counts survive handler completion and server
  teardown until the exported response retires. Query/write allocations retain
  both the class and ingress ancestors, including offload forks.
- Explicit allocators for authentication, catalog parsing, query planning,
  retrieval, MCP protocol envelopes, and serverless request handling. Persistent
  catalog caches keep their separately bounded runtime allocator.
- A separate retained MCP/A2A/transaction-session budget. Stable transaction starts
  prepay completion memory, startup restores outstanding recovery reservations,
  and retries/expiry cannot discard uncertain execution on lease loss or an
  unavailable coordinator.

The combined stage 1 gate passed 137 tests with zero leaks; one optional real
Wasmtime test was skipped there and passed in its separate engine gate. The
validation record lists focused checks and production build evidence separately.
Storage, inference, connection framing, Wasm runtime, and durable job memory retain their own owners; frontend allocation
accounting is not a process RSS cap. Existing public query/write concurrency
limits remain coarse execution admission, while ingress counts include drain.

## Stage 2: shared read execution

Stage 2 extends the fixed scheduler under opt-in `admission.read_execution`.
It is mutually exclusive with `dense_execution`; both remain disabled by default.
Stage 2 passed the combined gate (156 tests, one optional skip, zero leaks),
the five compiled storage-owner checks, and a clean production Debug build.
The validation document records exact source and binary provenance separately
from release qualification. Review stage 2 in these slices:

| Commit | Change |
| --- | --- |
| `57100f3898` | Prepaid transition capacity and atomic demotion |
| `3a026c60c5` | Absolute HTTP stream deadlines |
| `0969910a0f` | Shared native reads, protected probes, configuration and diagnostics |
| `31d8ed49c1` | Serverless query and joined-helper scheduling |
| `27146c033f` | Prepaid scan state, admitted resume and compiled deadline transport |

- Native DB lookup, scan, text/vector/composed search, aggregation, graph reads,
  and query preflight acquire shared general execution before storage locks.
  Nested dense work borrows that exact request owner; returned request metadata
  cannot retain a pointer to its stack lease. Optional dense helpers acquire
  independent nonwaiting permits or run inline.
- Serverless query handlers use a shared read runtime. Graph column/range helpers
  acquire before spawning, run inline if no permit fits, and retire only inside
  the joined worker. Serverless rejects the provisioned-only protected-probe and
  scan-suspension settings.
- Caller-thread CPU is measured in 100 microsecond units only when the runtime
  proves thread affinity. Helpers begin measurement on their executing thread.
  Migrating or unknown runtimes retain a conservative service estimate.
- An opt-in protected partition serves only a capped-key, empty-projection local
  LMDB existence probe. It validates at most 4 KiB of JSON in fixed 64 KiB scratch,
  checks JSON storage and disabled TTL, and prepays a transition ticket before
  execution. Scratch also reserves the node memory budget. Wider or incompatible
  rows close all probes/schema views before demoting to general execution and
  restarting the read. Ordinary field projections and arbitrary `limit: 1`
  queries do not qualify. These are storage execution floors; foreground ingress
  and request admission retain their separate limits.
- Demotion atomically transfers the request and quiescent children, carries
  measured service debt, and preserves global byte charges. Prepaid transition
  count/byte/metadata guarantees cannot be consumed by other work. A parked
  demotion resumes only with a complete general grant; its 100 ms residence
  ceiling cannot extend the original deadline. Live helpers/I/O forbid transfer.
- LMDB NDJSON scan output can suspend at the synchronous sink boundary with explicit
  prepaid scanner memory, snapshot lifetime, and a deadline-capable sink. It
  resumes through the same scheduler and original deadline. Buffered collectors
  and other snapshot backends (including LSM) or unproven runtimes remain coarse.
  Snapshot/cursor cleanup precedes release
  of saved-state credits. The reservation bounds owned scanner buffers and
  logical pins, not physical MVCC pages retained by concurrent writes.
- HTTP/1 and HTTP/2 writes honor min-only absolute output deadlines. HTTP/2
  flow-control and writer-lock waiting cannot renew that deadline; stream-local
  timeouts preserve other streams. A partial physical frame failure closes the
  connection to prevent subsequent framing corruption. Compiled callbacks
  subtract elapsed transit time instead of renewing the remaining budget.

General text, graph, and aggregation operations still hold a coarse execution
lease through their nonyielding regions. This stage does not assert arbitrary
operator preemption, process-wide RSS limits, distributed fan-out ownership, or
release performance qualification.

### API recovery follow-up

Started durable transaction sessions now retain an exclusive prepaid replay
workspace as well as completion-record capacity. The reservation covers decoding,
cloning, distributed table views, and replacement-record overlap. Startup restores
it before ordinary sessions can consume that capacity. Allocation failure while
preparing replay keeps the original durable obligation for another pass; it no
longer removes the transaction. The allocator failure sweep also exposed and
fixed a batch-parser key leak when value serialization fails.

The API replay checkpoint passed 190 tests, with one optional skip, zero
failures/leaks, and six expected error logs. The subsequent integrated recovery
and coordination checkpoint passed 206 tests with one optional skip and the same
zero-failure/leak result. Production transport fault tests, backend completion
credits, sustained-load progress, and release performance qualification remain
separate work.

### Backend completion boundary still to implement

The protected row/metadata lanes above reserve preparation and recovery-record
work. They do not reserve the full physical persistent LSM write.
`CompletionCredit` (`f8c71099cd`) is now connected to an internal, memory-only
sealed point-batch path (`013b533924`). Preparation charges incoming and
copy-on-write allocations before allocation; publication requires the original
pinned root and performs no allocation. Exact ordinary-allocation accounting
avoids counting prepaid bytes twice, and a retained allocator owner keeps bytes
charged through the final reader. Stale tickets retire historical roots in
bounded slices outside the writer lock. Debug and ReleaseSafe each passed 27
focused tests. See [the backend stage contract](WORKLOAD_LSM_COMPLETION.md).

This entry point rejects persistent storage before reserving capacity. WAL,
manifest and flush capacity still need prepaid ownership, with explicit handling
of uncertain storage I/O. The internal point-batch path therefore does not yet
promise mandatory transaction completion or activate a public transaction ticket.

A public completion ticket must be stored atomically with intents and the
prepared vote, bound to transaction revision, schema, backend namespace and a
sealed physical write plan. The plan must include identity, timestamp, replay
and derived-index writes. Startup must reconstruct its reservations before
foreground admission; replicas must not reject a committed prepare because
ordinary local capacity is exhausted. Unsupported plans must be rejected before
the prepared vote. These are implementation requirements, not guarantees of the
current opt-in recovery policy.

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
full response buffer. Request context and response header allocations use the
ingress owner when enabled. Connection framing and HPACK remain under their
separate transport bounds.

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
compiled API boundary. The current API ABI is 30, storage-owner ABI is 68, and
native runtime ABI is 11; these include admission diagnostics, dense I/O context,
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
The final MCP protocol envelope and agent output retain their request/class
owner through response drain. The serverless route inventory has no document
lookup endpoint; its existing query routes use query admission. Substantive
serverless metadata and publication work now enters query/write admission.
Only empty health/readiness probes use the ingress control partition. This
behavior change still requires release qualification; it is not evidence that
all overload timeouts are fixed.

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
as unavailable rather than implying zero pressure. Standalone and data-node rendering carry
the full snapshot through the versioned API kernel boundary; compile-time field
checks prevent future additions from silently disappearing in the projection.
These are engine contracts
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

`common/workload_attempts.zig` models bounded coordinator ownership. The opt-in
production coordinator persists ownership before sending a request and retains
uncertain sends across timeouts and restarts. Trusted membership maps each
endpoint to its node identity. Signed discovery binds a nonce to the worker's
durable namespace and monotonically increasing incarnation. The synchronous
compiled coordinator port copies response buffers into the caller's allocator
before returning; it does not expose Zig runtime objects across the ABI.

`admission.remote_attempt_worker` covers the audited internal lookup, query,
preflight, scan, vector, graph, text-statistics, algebraic-partial, and join routes.
It requires a durable API session backend, node identity, and internal service
authentication. Protocol 3 binds attempt identity, namespace/incarnation,
request content, route, and canonical authenticated node identity. Older signed
protocols are refused; unsigned legacy traffic remains outside this opt-in path.
Active duplicates do not execute. Terminal duplicates return evidence without
replaying a saved result. The worker preserves the earlier caller deadline and
local run ceiling. Cancellation requests termination; only handler unwind and a
durable terminal record permit signed terminal evidence.

Generation closure is durable before cancellation. A fence is issued only after
covered work has quiesced. Count and byte ceilings include attempts, tombstones,
and closure records. A capacity rejection persists a rejection floor before
signing proof that the rejected attempt cannot execute later. Safe acknowledgments
advance only beneath the first still-owned attempt, preserving delayed sends.
Coordinator journal version 2 and worker journal version 3 reject incompatible
prior journals rather than silently discarding their obligations.

Native restart can retire prior local incarnations only after exclusive ownership
of the same durable journal root is established and retained through teardown.
A new namespace, copied root, or elapsed deadline is not proof of old-worker
quiescence. Capacity-pressure reconciliation uses exact signed status and fence
evidence; missing evidence remains charged. Destination records with no debt can
be reclaimed at coordinator generation advance. Coordinator dispatch remains
disabled by default and unsupported serverless configurations are rejected.

Authenticated, bounded recovery control requests have a separate optional ingress
count/byte reserve. This lane serves attempt discovery/status/fencing and
transaction resolve/status/ack operations, survives foreground drain, and rejects
oversized or ambiguously framed bodies before protected admission. Ordinary
health probes keep their separate reserve. Join job maps, partial results, and
cached responses retain a stable owner under the session byte ceiling.

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
retires the obligation. API replay also prepays its bounded decoding workspace. Storage tables can opt
into immutable `storage.transaction_recovery` count/byte/per-transaction limits
at creation. Disabled tables preserve the previous catalog and JSON encoding.
Activated tables durably account metadata and intent obligations in the same
serialized transaction as their state changes. Compatible upgraded peers are
required; this is not an existing-table or rolling-activation migration.

`admission.transaction_completion_bytes` reserves two exclusive scratch lanes
against the host preparation budget before foreground service: one for recovery
metadata and one for applying rows. Separate lanes permit same-node resolution
without reacquiring the metadata lane. Busy or undersized completion resources
retain the durable obligation for retry; they cannot convert a committed Raft
operation into an aborted write. Protected lanes apply to tables with the
replicated recovery policy; legacy-table completion keeps its previous allocation
path so enabling a small node reserve cannot newly strand old committed intents. Logical recovery credits and these decoder/batch
reservations do not bound every index/backend allocation or process RSS. Complete
stateless coordinator/RPC ownership and worst-case index preparation remain audit
and qualification work. In particular, row completion still enters ordinary LSM
write-buffer admission, and some backend/HA buffers retain their DB allocator.
The current saturation proof covers preparation and metadata; it does not prove
completion with exhausted LSM capacity. A follow-up must prepay backend credits
or establish bounded drain progress without bypassing those limits. Corrupt or oversized historical metadata can require
operator repair or a larger reserve; a failed decode does not erase the debt.

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
| Query decoding/planning | Foreground body reservations plus tracked public single/NDJSON query and serverless query allocations; authentication/catalog/planning use ingress ownership before the class grant | Introduce separately protected execution for verified bounded planning |
| Dense rerank and helpers | Shared scheduler owns drivers/helpers; exact workspaces and opted-in native read arenas own actual bytes; serial immutable pread suspends only with proven executor affinity | Audit remaining boundaries; remove enclosing thread-affine scopes for portable suspension; add durable ownership for cached HBC scratch |
| Vector, text, graph, aggregation | Shared coarse general execution plus existing cancellation/work budgets and storage memory reservations | Audit further resumable operator state before adding suspension boundaries |
| Scan/stream output | Tracked response drain plus opt-in prepaid NDJSON scanner state, absolute snapshot lifetime, and admitted resume on proven runtimes | Extend verified suspension to other backends and operators; MVCC pages retain storage ownership |
| Remote coordinator/worker tasks | Opt-in durable coordinator and protocol-3 worker; membership identity, signed discovery, uncertainty accounting, generation closure, and same-root restart proof | Qualify real transport loss/restarts and complete shutdown/fan-out audits |
| Transaction commits | API replay workspace, durable table recovery credits, and separate protected metadata/row scratch lanes; existing decisions remain authoritative | Complete stateless coordinator/RPC and worst-case index/backend preparation ownership; qualify crash recovery |
| Background/control/recovery | Existing dedicated runtime owners; status/maintenance submission pressure retries without terminating control | Prove process-wide protected count/byte/progress floors and sustained-load fairness across foreground and background work |

Only the verified existence-probe path above enters protected read execution.
No operator releases execution on an unverified suspend boundary. Existing
committed-write recovery is preserved; client cancellation must not be used as
evidence that a write was rolled back or that remote work quiesced.

## Validation

```sh
cd zig
zig build antfly-common-config-test -j2
zig build antfly-workload-admission-test -j2
zig build runtime-callback-abi-test -j2
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

Native Debug experiments exposed query allocator mismatches, a fatal
background-capacity escape, and dense admission errors lost across the runtime
callback boundary. Corrective stages include focused ownership, executor, and
independently compiled callback regressions. The
[validation record](WORKLOAD_SCHEDULING_VALIDATION.md) preserves the failed
experiments and subsequent checks; none establishes optimized performance or
release qualification.

## Additional control and write recovery stages

The data/API transport now carries opt-in ingress capacity through its request
and connection limits, without multiplying upload buffering. Native C40/C80
correctness passed on frozen `3eea19e027`: all successful reads returned the exact
sentinel, excess work received structured 429, API probes progressed, and tracked
query ownership drained after healing. See the validation record for the frozen
binary and receipt boundaries; this is not throughput qualification.

HTTP request tasks now support nonborrowable control/recovery partitions inside
the listener total. The API uses one bounded framing/authentication classifier
for direct and compiled dispatch (API ABI 30). Recovery privilege requires a
signed internal-service credential; ordinary URL matching is insufficient.
H1 transport rejection has an allocation-free structured 429 response, while
H2 retains REFUSED_STREAM semantics. Task reservations do not reserve connection
slots or establish a process-wide execution/progress guarantee.

Ordinary LSM append failures now distinguish pre-I/O preparation from uncertain
storage effects. Uncertainty fences mutation and preserves manifest debt.
Writable reopen checkpoints valid recovered state and repairs a torn WAL before
new writes; read-only open leaves it unchanged. This fixes a reproduced case
where a post-reopen acknowledged write made the next reopen fail.

The [internal LSM completion stages](WORKLOAD_LSM_COMPLETION.md) now include a
native one-shot point-batch helper that seals memory, WAL encoding, descriptor
ownership and accounting before append/publication. It remains internal and
performs ordinary admission before sealing. It is not a prepare-now/commit-later
transaction guarantee: exact physical plans, durable certificates, pre-admitted
SST/manifest completion, restart restoration and production transaction wiring
remain required.

## Remaining design phases

| Design phase | Current status |
| --- | --- |
| Ownership/progress prerequisite | Typed ledger, scheduler, allocator, and attempt state models are implemented and tested; complete operator inventory and process-wide progress proof remain open |
| Phase 1 | Fixed foreground waiting, contextual allocation ownership, deadlines, overload diagnostics, and SDK contracts are integrated on the paths above; frontend ingress/planning/output ownership and empty-probe floors are integrated; process-wide execution/cleanup floors remain open |
| Phase 2 | Dense driver/helper ownership, exact working bytes, one pinned-executor I/O boundary, session recovery bounds, and opt-in durable join-row workers are integrated; shared coarse reads, protected existence probes, measured pinned work, demotion and scan suspension are integrated; opt-in coordinator ownership passes local response-loss, worker/API restart, and destination-isolation qualification; protected recovery mechanisms and broader fault coverage remain partial; further cooperative operators and complete write/index memory proof remain open |
| Phase 3 | SDK pools/retries, engine diagnostics, and qualification tooling exist; native pressure qualification, optimized release qualification, Cloud integration, sizing/defaults, and adaptive policy remain open |

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
