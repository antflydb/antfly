# Runnable workload scheduling

Status: opt-in scheduling and independent execution scopes are implemented for
vector kernels, spill sort/merge, grouped aggregate, partitioned join and capable
ordered scan cursors. Focused ordered-scan tests pass on native and pinned
Nix/glibc Linux; persistent lake cache wait tests and existing cache regressions
pass on native. Independent lake metadata clients and nested prefetch ownership
are under qualification. Full API/pgwire/routing gates, remaining backend
ownership/waits, resource/performance qualification and default selection remain
required.
The whole delivery goal remains safe sharing across
many connections by default, with fairness and bounded waiting, while preserving
session semantics, correctness, durability and control progress under overload.

## Ownership and transitions

Operation admission, runnable permits, retained memory and durable completion are
independent owners. An operation stays admitted and retains its memory charges
while it relinquishes runnable permits for resource or output waiting. A scheduler
entry is caller-owned, stays at one address, and outlives notifications and all
workers that reference it. It registers exactly once and retires through finish.

| From | Transition | Runnable ownership | Outstanding ownership |
| --- | --- | --- | --- |
| Fresh | Submit | Grant or bounded queue | Reserve count and metadata bytes |
| Queued | Grant | Acquire configured slot demand | Preserve |
| Running | Yield | Release, requeue, reacquire | Preserve |
| Running | Resource/output block | Release | Preserve |
| Blocked | Unblock | Queue or grant | Preserve; no new reservation |
| Any registered state | Finish/cancel/expired wait | Release if held | Release once |
| Queued/blocked | Shutdown | Stop and notify owner | Retain until owner joins and finishes |
| Running | Shutdown | Owner joins; no forced resource revocation | Retain until finish |

The entry bound includes queued, running and blocked operations. This ensures a
resuming operation does not need an unreserved queue allocation. Metadata bytes
are a caller-declared outstanding bound, not a total-process memory measurement.
Backing heaps, retained pages, thread stacks and backend allocations keep their
existing resource owners. Entry options preserve the original clock, deadline
and cancellation token across yields and waits. Queue time starts at enqueue,
not when the caller first waits. A grant racing an expired queue cannot authorize
execution: wait rechecks expiry and returns every reservation.

## Dispatch policy

Query, write, inference and background classes use deficit round robin. Weights
allocate runnable slot service, and a job's declared slot demand consumes credit.
Empty classes do not accumulate credit. Slots and outstanding operations are
finite. Control slots are reserved within the total slot envelope and are not
lent to ordinary work. Identifying protected controls remains an authenticated,
grammar-verified responsibility of the adapter, not a label clients can select.

Control also has protected outstanding metadata: `control_bytes` is reserved
inside `max_bytes`, and up to four entries (at most half `max_operations`) are
reserved inside the operation bound. Filling ordinary outstanding limits cannot
consume those reservations. An authenticated completion drain can promote its
already parked entry to the control class without reserving new metadata; its
original metadata charge remains ordinary until retirement. This changes where
the entry runs, not who owns its retained state or durable obligations.

Telemetry separates configured capacity, active slots, outstanding owners,
declared metadata bytes, queue/resource/output waiting, grants by class and
cooperative yields. Protected metadata counters describe the original reservation,
so promoting a parked ordinary owner does not fabricate a control metadata charge.
Disabled scheduling reports zero capacity. The primitive snapshot crosses the
versioned API bridge; both data-node and standalone metrics paths use the same
formatter. Focused snapshot/formatting tests pass, while full runtime endpoint
qualification remains pending with protocol verification.

Small jobs can backfill behind a head whose demand cannot fit. Every such grant
increments the blocked head's bypass count. At the configured bound, the oldest
blocked head becomes a barrier: ordinary work drains until that head can run.
Control work remains independent. A large job must fit its class's total capacity
or submission rejects it atomically. This protects against an unfulfillable
barrier and starvation from a continuing stream of smaller jobs.

The core schedules explicit declared slot demand. Its stable, single-worker
execution capsule yields after `checkpoint_budget` checkpoints or `quantum_ms`
elapsed time, whichever comes first at a checkpoint. These are cooperative
limits: an uninterrupted backend call can exceed the quantum. Real execution
cost estimates, expensive-query demotion and per-tenant weights remain subsequent
integration work; the current tests do not prove those.

The opt-in SQL adapter owns one capsule per operation. HTTP and pgwire reads use
the node's shared scheduler; pgwire page streams park between pulls and retain
their operation, snapshot and memory owners. Pure CPU kernels and exclusive
operator files use independent child executions reserved before publication and
joined before metadata/result retirement. Ordered scan providers explicitly bind
exclusive child cursor/reader state; providers lacking that ownership capability
continue in the coordinator. Each producer uses its own queue, spill and provider
wait callbacks. Nested prefetch requires its own reader and execution receiver.
A denied worker activation must close its streaming queue so the consumer can
join it. Original authority is rechecked before exposing results. Shared mutable
API adapters and policy caches remain coordinator-owned. These focused contracts
are not yet a claim of complete runtime/default qualification.

No adapter may label a whole backend call resource-blocked just because it may
perform I/O: it can also execute substantial CPU work. Releasing its runnable
permit requires a genuine wait boundary and reacquisition before execution
resumes. Private spill positional reads and writes now use that boundary, keeping
stable buffers and their quota owners across the wait. Reacquisition runs on I/O
failure as well as success; backend checkpoints run before successful data is
decoded. Ten stalled read/write schedules pass on both platforms, including
cancellation, expiry, shutdown and I/O errors. Private directory/file creation also suspends runnable ownership. Owned I/O
results are discarded if capacity or authority cannot be reacquired, with cleanup
independent of a new grant. The full SQL corpus passes all 387 tests on both
platforms; the [creation receipt](qualification/workload-pooling/receipts/2026-10-07/spill-creation-waits/README.md)
records seven handle-retirement schedules. The creation hooks also pass the full native API pooling target. Full Linux API
qualification of creation hooks, general storage/RPC waits, cleanup execution
ownership and parallel child owners remain outstanding.

## Read authority after resumption

Scheduled HTTP SELECT/EXPLAIN and retained PG SELECT operations capture a bounded
read guard before their first source read. Each successful runnable grant advances
an execution generation; yields and resource/output resumes invalidate the prior
check. Results also force validation before handoff, including when no yield
occurred. The guard owns at most 64 immutable logical/physical bindings, schema
versions, catalog revisions, effective user filters and native RLS serving
generations. A failed check cannot mark a generation valid.

Validation resolves the pinned catalog generation and serving policy, then reads
current managed user/API-key policy under one mutation lock. Current permission
must intersect admitted permission; an opened query cannot acquire a different
filter or interpretation of native protected roles/metadata. Account replacement
and API-key expiry/deletion stop the read. Pending policy installation can retain
prior serving authority; serving transitions or changed generations stop it.
Logical-to-physical projection retains explicit null filter overrides. This is
read-only integration; mutation execution and accepted durable completion keep
their existing ownership paths.

The [read-resume receipt](qualification/workload-pooling/receipts/2026-10-07/read-resume-fences/README.md)
records 22 coordinator schedules, three embedded ownership/fault fixtures, and
full native API qualification. Fresh externally issued credential leases remain
required. Validation itself can perform catalog I/O, so future storage/RPC wait
hooks must reacquire capacity before decoding and avoid recursively validating a
guard already in validation. A permit may not remain borrowed across the wait,
and a successful wait may not publish an owned result after failed reacquisition.
General backend calls must retain CPU ownership for their non-waiting work.

## Checked native wait callbacks

`common/execution_wait.zig` supplies borrowed begin/end callbacks through the
checked native callback ABI. Storage archives can release and reacquire a
coordinator's execution owner without interpreting compilation-local Zig error
integers. Scheduling error details are append-only and preserve prior numeric
IDs and timeout/unavailable/retryable behavior. Owned successful I/O results are
discarded through the original owner when resumption fails.

The [native wait receipt](qualification/workload-pooling/receipts/2026-10-07/storage-wait-boundary/README.md)
records three wait fixtures and all 25 error ABI fixtures passing on both
platforms, including a separately compiled callback owner and explicit rejection
of incompatible layouts. The qualifier verifies that the foreign dispatcher is
used, rather than taking the local-call shortcut. This establishes the interface;
SQL scan options now carry a borrowed request-local execution wait to read-index
barriers. The DataServer releases capacity only around its real owner-lock and
quorum/apply polling sleeps; registration, probing and retransmission retain CPU
ownership. Each resume uses the original entry and deadline and validates the
read fence before continuing. Group barrier preparation stays serial for this
single-worker owner, and retained remote cursors discard the borrowed callback.
The [read-index receipt](qualification/workload-pooling/receipts/2026-10-07/read-index-waits/README.md)
records 400 SQL/read-gate contracts passing on both platforms and checked callback
qualification. Actual DataServer failure schedules, retained-owner callback
retirement, complete production/API qualification and real quorum/apply delay
schedules remain separate gates. Snapshot bootstrap, general storage and RPC
waits still require propagation. Parallel children cannot share this single-worker execution owner. Accepted
durable completion retains its own owner and cannot depend on a fresh grant.

## Integration order and completion requirements

1. Own a scheduler on the execution node and validate its opt-in canonical
   configuration. Preserve fixed operation admission and a rollback path.
2. Bind both HTTP and pgwire read execution to that owner. Introduce bounded
   checkpoint quanta in scans/operators, and release runnable ownership around
   real backend waits and slow output without moving retained-state ownership.
3. Verify mixed inexpensive/expensive reads, slow consumers, cancellation,
   suspension, restart and protected controls through real protocol fixtures.
4. Extend scheduling to writes, inference, background work and fan-out. Track
   remote attempts and uncertain results at their execution owner. An accepted
   durable decision keeps its completion obligations when its foreground entry
   stops; do not add suspension points that can strand accepted writes.
5. Run declared main/fixed/scheduler qualification sweeps and resource-exhaustion
   schedules. Derive and publish automatic sizing and bounded-wait defaults only
   from that complete behavior. Adaptive tuning follows reliable fixed telemetry.

`common/runnable_scheduler.zig` implements the state machine without heap
allocation. Its tests cover weighted busy classes, bounded large-job backfill,
protected slots, blocked ownership, resumption at full outstanding bounds,
yielding, grant/cancel and grant/expiry races, real queue timeouts, invalid
policies/demands, and shutdown accounting. It is exported by request admission
and included in the pooling test selection. These tests establish core policy
and ownership, not protocol integration or default qualification.


SQL preparation now acquires runnable capacity before parsing, plan-cache access
and Describe in both HTTP and pgwire. It releases that grant before coarse
execution admission; protected controls use reserved capacity. Pgwire read setup
uses a stable request owner before setup allocations, and ordinary Describe
also acquires execution capacity. The mixed protocol fixture verifies queued
preparation makes no new catalog calls while ROLLBACK progresses. All 75 selected
native API contracts pass, with no skips/leaks and all 50 build steps successful.
The [preparation receipt](qualification/workload-pooling/receipts/2026-10-07/sql-preparation/README.md)
records source hashes and complete output. Linux API and complete production
qualification remain pending. Parser quanta, exact preparation wait suspension,
serialization ownership and broader execution classes remain separate work;
this phase does not enable scheduler defaults.


## Ordinary SQL write ownership (native and Linux API qualified)

HTTP and pgwire ordinary SQL execution now acquire the write class after
preparation and coarse operation admission, before execution setup. HTTP
session-setting commands acquire query or write capacity according to their
existing read/write classification. Grammar-verified terminal controls retain
the reserved control class. Describe remains query work. Disabled scheduling
retains the existing behavior.

A scheduled ordinary write keeps its grant until the execution worker completes,
including any accepted durable completion. Its adapter does not receive the
read-only checkpoint/park callbacks. Serial execution disables unscheduled CPU
children while retaining native spill I/O without wait suspension. This prevents
new scheduling boundaries from abandoning a proposal or requiring a fresh grant
to finish it. It does not yet release runnable capacity during write I/O or
preempt mutation CPU work; those require a separately owned durable completion
phase and independently admitted child execution.

The mixed protocol fixture now exercises explicit read-committed BEGIN READ ONLY
and protected ROLLBACK
through both HTTP and pgwire, requires two additional write-class grants, and
checks zero remaining operations, slots and retained metadata. BEGIN mutates
session state even in a read-only transaction. Its unsupported default-serializable
BEGIN case also requires the unchanged 501/0A000 diagnostic and retirement of
its write grant. The existing native row-mutation
fixture also runs with scheduling enabled; successful INSERT/RETURNING requires
one write-class grant and zero remaining scheduler charges per statement. Its
joined UPDATE and default-normalization cases exercise the serial native backend.
Authenticated pgwire INSERT/RETURNING and DELETE/RETURNING also require a write
grant and zero remaining charges, and remove their probe row before later cases.
These fixtures do not prove a stalled accepted row-mutation schedule. All 75
selected native API contracts and 50 build steps pass with no skips or leaks.
The [write receipt](qualification/workload-pooling/receipts/2026-10-07/write-runnable/README.md)
preserves the initial fixture failure and corrected complete run. GNU AArch64
also passes all 75 API contracts and 47 build steps with the pinned Nix/glibc
compiler and default build budget. Both exact artifacts pass again under two
CPUs and 2 GiB without extra swap, with required counts/names, matching hashes,
zero exits and no skips/leaks. Complete production qualification of this write
slice remains pending; automatic defaults remain disabled.
