# Runnable workload scheduling

Status: scheduler core, opt-in HTTP/pgwire SQL read integration, and private
spill read/write suspension pass native and Nix/glibc Linux API qualification.
Shared managed-policy/catalog resume fences pass focused checks on both platforms
and both expanded API suites, with exact Linux artifacts rerun at 2 GiB.
Runtime metrics endpoint checks, other backend waits, parallel child ownership,
remote-attempt accounting, qualification and default selection remain required.
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
their operation, snapshot and memory owners. Scheduled SQL currently uses serial
backend execution because parallel children need independent runnable ownership
before sharing an adapter. Serial execution retains a separate spill I/O
capability: sorting, grouping and window continuations still spool to disk, while
all spill CPU submissions pass through a manager gate that declines parallel
work. The six-case continuation fixture passes in both serial and parallel modes
on macOS and Nix/glibc GNU Linux. This temporary restriction must be resolved
before selecting defaults. Ordinary mutations retain their existing admission path;
only already verified protected controls use reserved runnable capacity.

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
