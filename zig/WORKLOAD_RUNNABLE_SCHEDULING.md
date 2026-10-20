# Runnable workload scheduling

Status: automatic scheduling and finite query waiting are now the canonical
startup defaults. Explicit `admission.runnable: null` disables scheduling and
the implicit queue; an explicit empty query `waiting` object selects fail-fast
admission. Merged-main configuration and transport-envelope checks pass;
serverless default integration passes 36 tests plus nine companions with
localhost access. Independent execution
scopes are implemented for
vector kernels, spill sort/merge, grouped aggregate, partitioned join and capable
ordered scan cursors. Focused ordered-scan tests pass on native and pinned
Nix/glibc Linux; persistent lake cache wait tests and existing cache regressions
pass on native. Independent lake metadata clients and nested prefetch ownership
are under qualification. Focused API/pgwire/routing correctness, remaining backend
ownership/waits, resource exhaustion checks and remaining default integration
checks remain required.
The user waived the full qualification sweep; it is not a delivery gate.
The whole delivery goal remains safe sharing across
many connections by default, with fairness and bounded waiting, while preserving
session semantics, correctness, durability and control progress under overload.

## Replicated required BEGIN (2026-10-19)

The new required-BEGIN command preserves preparation mode through internal batch
encoding, the Raft envelope and server ordered apply. Its mode, requirement and
receipt commit atomically; durable replay bypasses fresh catalog admission while
new entries still validate the file witness. Stripped phases and missing/unknown
modes fail closed. Native ABI is 38, engine descriptor 25, storage owner ABI 101
and required-preparation file floor 6.

Seven focused native targets and 25 package consumer checks pass at
`0964f20fb0187ae1708bcaa859afeaa83ff13a05`. Production proposals remain rejected
before a barrier or entry until all-member migration and producer readiness are
proved. Required transport, PREPARE/seal providers and real distributed failure
checks remain open; see the implementation ledger for exact evidence.

## Retained preparation agreements (2026-10-19)

Commit `bb9088a5ef0e84a0dc3c9c8e89b86f5d39a8916e` stores the original
preparation mode atomically with required BEGIN in a fixed 54-byte v7 transaction
record. Legacy records keep their previous encoding. Resolution deletes effect
and requirement sidecars while preserving the agreed mode in the existing history
record; no extra history key or later allocation growth is needed. Existing retry
retention, participant acknowledgements and expiry cleanup govern its lifetime.
A typed status read validates unresolved sidecars and returns durable contract
evidence after resolution. Legacy records never acquire this evidence from a
sidecar alone. Missing or conflicting evidence cannot authorize commit, and abort
can still retire damaged preparation metadata.

Permanent reader floor and catalog envelope 6 fence this record format, including
when the last sidecar retires. Native ABI is 37; storage owner ABI remains 101.
Files at reader 5 remain readable, but active old preparation metadata must be
resolved or explicitly migrated before enabling new required BEGIN. Peer readiness
also requires reader floor, permanent baseline and envelope at least 6.

The six-target `native-retained-contract-v3-verified.json` receipt covers all
prepared-effect codec/storage cases, 22 required-preparation DB cases, coordinator
and participant fanout, inference checkpoints and independent native ownership.
New cases check both modes through persistent resolution/reopen/retry/expiry,
reader-5 migration refusal, damaged evidence, legacy refusal and every original
admission rejection. Existing allocation-failure coverage now checks atomic mode
publication too. The immutable package passes 25 ReleaseSafe consumer checks.
These are focused checks; full Linux qualification remains waived.

Production activation remains pending. Next are strict RPC and replicated
BEGIN/PREPARE/seal adapters, real participant providers and eligible producers,
ordered-catalog/import/downgrade migration and distributed fault/restart tests.
The retained-history storage prerequisite is implemented; it does not establish
end-to-end provider readiness. Completion resource bundles, future replay/output
credit and the remaining original scheduler scope remain open.

## Required-preparation coordinator foundation (2026-10-19)

Commit `3542715d2d636ccf0a97497a0f46992a22a1a57a` adds an explicit opt-in
coordinator policy. It negotiates every participant before BEGIN, uses only
requirement-aware BEGIN/PREPARE callbacks and validates every seal before the
first commit decision. Seals bind transaction, participant, pinned route and exact
preparation mode. Original execution authority, deadlines and cancellation reach
those callbacks; deliberate recovery detaches the original execution authority.
The mode remains independent of later acknowledgement levels. Matching retained
commits can resume without fresh readiness, aborts remain terminal, and bare legacy
status cannot establish a preparation contract, including after ambiguous BEGIN.

Shared ATPW v1 framing uses fixed binary headers and checksums, bounded payloads
and original admission checkpoints. Codec support alone grants no readiness:
capabilities also require durable BEGIN/seal/recovery, retained contract evidence,
reader floors and completed catalog/import migration. Native ABI 36 fences the
new coordinator callback and option layouts; storage owner ABI remains 101.

The five-target native receipt `native-prepared-coordinator-v6-verified.json`
records coordinator model tests, all 20 required-preparation storage cases,
participant fanout, inference checkpoints and independent native ownership.
Twelve new cases cover ordering, failures before decision, every readiness bit,
retained retries, authority, fixed framing, corruption/truncation and allocation
failure. The immutable source package passes 25 ReleaseSafe consumer checks.
These are focused checks; full Linux qualification remains waived.

This is a coordinator and codec foundation. Production participant providers do
not advertise readiness, existing entry points do not select the new policy, and
the binary wrapper is not yet wired into HTTP or replicated batches. Next are
the production transport and storage integration described above: strict
RPC/Raft BEGIN/PREPARE/seal adapters and eligible predecision producers, followed
by ordered-catalog/import/downgrade migration and real distributed restart and
failure tests. Future replay/output credit, minimum completion resource bundles
and the remaining scheduler design stay open.

## Held-cursor completion after the main merge (2026-10-19)

Internal `completion_drain` authority follows a retained pgwire page through both
its delivery worker and runnable CPU admission. COMMIT can therefore materialize
an existing held cursor while ordinary execution is saturated, even though the
retained statement text is SELECT. Fresh queries and mixed statements still use
ordinary capacity. The real HTTP/pgwire/H2 integration and imported-executor routing
regression pass on the current main merge
`75c470fc1953918e422a6e13dd4d24675cef5f7c`, retaining the delivery fix from
`737098cfd7e24b7265c5e586676ada70f20adf7e`. The fixture funds the default worker-stack floors and installs both runtime and SQL
memory parents from the same storage manager, as production does. Remaining storage
credit is exhausted during the test; no lane ceiling is reduced. That main merge used native ABI 34
and storage owner ABI 101; the delivery fix changed no ABI.
See `native-latest-main-v1-verified.json` in the implementation ledger's receipt
directory for the 16-target scope and unchanged exported-package proof.

## Bounded required-BEGIN file eligibility (2026-10-19)

The future required-preparation path no longer reparses the full resolver catalog
while holding apply. It reads the permanent reader floors through the original
control, bounds scalar parsing and checks a validated store/revision/import witness.
Catalog commit attempts, including ambiguous outcomes, and physical generation
adoption invalidate old authority; ordinary writes and aborted staging do not.
Publication or successful load establishes the next witness. Active writers,
revision saturation, incomplete loads and equal revisions from another store fail
closed. BEGIN does not launch unbounded catalog repair as a fallback.

Native ABI 35 fences the new transaction and publication-stamp layouts; storage
owner ABI remains 101. The ten-target native check passes at
`6683d34283f1edfb5e8c70b8651352d9aba229a4`, including 20 required-preparation
cases and the existing actual HTTP/pgwire saturation regression. This completes the
local catalog-check step. The newer opt-in coordinator foundation above adds
negotiation and participant sealing; production adapters and the remaining
minimum-completion resource policy stay open.

## Read execution lanes

The operation classes remain query/write/inference/background/control. Read
execution now has separate general, analytical and trusted bounded lanes. HTTP
SQL and both pgwire execution paths classify compiler-owned plans: unbounded
scans, joins, grouping, ordering and unknown expression work are analytical;
other unverified plans remain general. Classification parks and reenters the same
root, retaining its original deadline, cancellation, operation and input charges.
It creates no replacement query or snapshot.

Automatic startup reserves `bounded_read_slots` ordinary CPUs (default one) and
one quarter of ordinary scheduling entries and caller-declared metadata for
bounded reads when the envelope leaves at least one general CPU and eight
entries. One bounded CPU is always a nonborrowable floor. Small envelopes use
one ordinary policy without a low-latency overlap guarantee. Fixed policies
default to no isolation. `read_isolation: false` explicitly disables the reserve;
`true` requires enough CPU/registry capacity, including managed cleanup. General
writes and background teams use the remaining general capacity.

This is a scheduling reservation, not a bound on all backend allocations. No
production SQL operator currently qualifies for the bounded lane: even constant
queries pass through catalog/provider setup. Trusted bounded entries prohibit
input growth, child fanout and mutation promotion. The verified operator
inventory, physical worker/application-memory reserves, measured work debt,
backend/application-owner integration for demotion, safe unused-capacity
borrowing and production lane integration under overload remain required.

Lane metrics expose effective capacity, parked ownership and CPU grants through
the versioned runtime snapshot. API kernel ABI 48 rejects older layout contracts;
the latest expansion carries synchronous opaque HTTP controls.
The focused read-policy and canonical configuration checks cover the implemented
subset; historical mixed-protocol/session receipts precede these changes.

### Weighted team assembly across isolated lanes

A general-lane team that has paid its deficit share can hold that round open
while its running peers relinquish fragmented capacity. The decision uses the
team's admitted static lane capacity, not the currently occupied capacity it
needs to drain. Otherwise one-slot arrivals can steal every fragment until the
large-request starvation backstop overrides weighting. Independent bounded CPU
continues through its own existing deficit/debt accounting during this drain.
No reserved CPU is borrowed by that path.

Focused schedules cover two-slot write, inference and background teams against
general and analytical reads, with bounded CPU idle and busy. They verify the
combined query-class share, independent bounded progress and original queued
cancellation/deadline retirement. This is slot/quantum accounting; operator-wide
non-yielding bounds and production mixed-load qualification remain separate.

### Reclaimable CPU borrowing

The experimental core permits a trusted prepared single-worker stage to enable a
CPU loan only from capacity above its nonborrowable bounded-read CPU floor.
`bounded_read_slots: 1` is the default and permits no loans. An explicitly larger
reserve can lend at most one idle CPU, inside the unchanged total slot ceiling.
Its verified non-yielding interval must fit the configured quantum;
ordinary unverified stages remain ineligible. Loans retain the original general
operation, heap, metadata, deadline and cancellation. They lend no memory,
registry, health/control, cleanup or durable-completion authority.

For an already-running validated stage, enabling eligibility can transfer its
live grant to elastic reserve and dispatch queued general work without suspending
the stage or refreshing its authority/quantum. If released capacity cannot fit a
queued team, the scheduler immediately converts the loan back. The native captured
SQL-policy fixture proves a copied numeric addition on that unchanged grant,
ordinary-CPU return and successful validation before output, plus catalog/policy/
credential-change refusal. The current fixture reserves both numeric scratch and
a fixed policy-validation buffer inside finite foreground memory, including
uncached schema derivation and live credential snapshots. Future policy growth
can refuse within that buffer before output. Initial capture, transport and
native stacks remain outside this focused request-heap proof.

A bounded waiter prevents further loans and requests the current one back at its
next safe checkpoint. The borrower relinquishes CPU and requeues the same owner;
this return does not wait for the normal quantum/checkpoint counter. A free base
general slot can instead take over the existing live grant without a deadline or
quantum refresh. Explicit disable reacquires ordinary CPU before an unqualified
phase; resource/output parking clears eligibility. Fanout, input growth and
mutation-completion promotion are refused while eligibility is active. Terminal
foreground causes return CPU while preserving the resource owner through cleanup.

Focused native fixtures cover reclamation, phase/cleanup transitions, bounded
CPU/control progress, unchanged fixed heap ownership and existing class fairness.
Runnable ABI 14 carries an optional checked CPU-loan capability. Local read
adapters revalidate authority after a disabling CPU wait, and root adapters do
not retire their ingress owner. Runtime/API telemetry and Prometheus now expose
coherent active/grant/return counters without duplicate CPU or memory charges.
API ABI 46 fences the enlarged telemetry layout. Production operator
qualification/integration remains open. No public SQL path is
currently eligible; this mechanism alone supplies no production latency or
whole-resource borrowing guarantee.

The core now separates the hard floor from lendable excess. Focused native core
fixtures prove immediate bounded-floor admission while a borrower continues to
run without a checkpoint, default-floor refusal to lend, and elastic reserves
of two through four CPUs across bounded occupancy. Explicit reserve sizing must
leave general, health and managed cleanup capacity; automatic sizing disables
optional isolation when it cannot fit. Coherent metrics report reserve, hard
floor and loan capacity without extending runtime/API extern layouts. This
proves CPU admission ownership, not a dedicated OS core or end-to-end latency.
The shared local-worker validation hook now ends the eligible stage and
reacquires ordinary CPU before full policy validation. A checked native adapter
regression proves no policy call while queued for ordinary CPU and unchanged
root lifetime through success or revocation. Ordinary checks do not refresh
the quantum when lending is inactive. SQL's custom provider now carries the same
checked capability and returns CPU at the start of actual catalog validation,
before allocation or IO. Five native SQL cases cover the boundary, four original
failure schedules, checked stage transitions and existing scalar lifetime polls.
Native captured-policy resumption on a prepared addition now passes. Production
operator elapsed-time proof, minimum policy/continuation resource bundles and
actual compiled API integration remain gates before lending is enabled.

Live SQL/pgwire policy snapshots now use caller-owned memory all the way through
Casbin role search, permissions, filters and API-key intersection. Pgwire's
short-lived verifier snapshot also uses its caller allocator. Failure-injection
coverage denies manager allocations and checks every caller allocation failure;
seventeen UserManager, four Casbin and ten SQL/pgwire cases pass. A
fixed policy-working-set reservation now bounds experimental captured-stage
validation without consuming control memory. Complete profile/resource bounds
and measured stage intervals are still required before production loans.

### Prepared numeric continuations

The coordinator's fixed 64-row numeric scratch now carries its next instruction
across scheduler waits. One math step evaluates at most 64 cells and checks at
most 64 integer widths; the final output step visits at most 64 roots. The
coordinator checkpoints between steps and before publication, retaining the same
scratch, operation, deadline and read-policy owner. Pure math steps call no
allocator, input provider, IO or scheduling hook. Failed steps and repreparation
cannot expose partial or previous output.

Twenty-two native numeric/SQL cases and six API cases pass, including exact
64-instruction progress without provider/allocation access and queued write-class
and protected control grants before chunk publication. Existing cancellation,
policy, finite scratch, scalar and ordered-stream cases remain covered. These
128-cell-step boundaries are a work-count bound; measured time and production
CPU lending qualification remain open.

### Bounded demotion and service debt

A demotable read reserves a count/byte transition credit inside the bounded
scheduling partition before initial admission. Its fixed caller-declared
continuation footprint and finite cleanup authority are established before any
operator resource can be published. These credits reserve capacity; they are not
additional live operations or duplicate retained bytes.

At a verified yield boundary with all children joined, the root relinquishes
bounded CPU and moves its live scheduling count/bytes to the reserved transition
owner. If general capacity is full, it cannot execute or reenter the protected
lane. It waits FIFO among transition owners for at most its declared transition
interval and retains its original deadline and cancellation. Once general
count/byte capacity exists, the same root transfers there atomically, releases
its transition credit and reenters through the original checked CPU queue.
Timeout, cancellation and shutdown stop foreground execution while the retained
owner survives through independent cleanup and owning finish.

Claimed runnable time is sampled at checkpoints and before explicit parking;
resource/output waits are excluded by resampling at successful reentry. Work
exceeding the trusted estimate triggers demotion at a checkpoint. The scheduler
reconciles measured quantum units against bounded service already charged,
retains at most 64 slot-credit units of excess debt, and repays them through
deficit scheduling before granting general CPU. This is not hardware CPU-time
measurement or preemption of unchecked operators.

The core currently supports fixed-footprint, single-worker reads. Input growth,
fanout and mutation promotion remain prohibited. It transfers scheduling
metadata/CPU ownership; actual application heaps, snapshots and remote attempts
still need matching operator-specific transfer and cleanup capabilities before
production bounded eligibility can be enabled.

### Physical read allocation owners

`common/read_memory.zig` provides stable per-read allocation groups over finite
general, bounded and transition heap partitions. The node envelope is reserved
once at pool creation and released after every group and allocation retires.
Groups reserve transition capacity before the first allocation attempt. Allocation
headers preserve the original group through compatible allocator views, resize,
remap and free; transfers move charges without copying or relocating buffers.
Transition rejects allocation growth, so required cleanup scratch is prepared
before publication. Active physical allocator operations prevent transfer.

`api/runnable_read_resources.zig` couples a group's transfer to the original
scheduler entry. Trusted, allocation-free accounting callbacks use scheduler ->
resource lock order and perform no IO or scheduler reentry. Metadata destination
capacity is checked before the physical participant; general CPU cannot be
published until physical capacity is also available and the transfer commits.
Failed startup releases both reservations. Expiry retains both owners through
independent cleanup and final physical retirement.

Focused fixtures verify real buffer addresses/content, independent metadata and
heap refusal, original deadline, automatic checkpoint demotion, compatible-view
retirement, alignment/resize/remap rollback, allocator-operation races and node
accounting without duplicate live charges. This provides the reusable owner;
production operator-wide allocation routing, physical worker and snapshot/attempt
ownership still need integration before eligibility.

Production startup now publishes this pool for enabled isolation in API owners
and managed serverless stacks. A quarter of the original foreground allowance
is carved out: half serves demoted read groups, and bounded/transition groups
each receive one quarter. The ordinary allowance shrinks by exactly that total.
Controls and retained state keep their separate envelopes. API/node accounting
pins this capacity once; serverless enforces it within the existing request
ceiling. Borrowed node owners use their already frozen policy. Disabled isolation
publishes no pool. General frontend/body quotas are derived from the remaining
ordinary/control allowance. Internal workload memory stats include the pool;
complete shared endpoint metric projection remains unfinished.

### Physical SQL worker isolation

The node SQL worker factory exposes a shared `bounded_read` role with capacity
one. It has a distinct executor from foreground, control and cleanup and remains
charged against the dedicated-worker ceiling through its last physical join.
With the default ceiling of 32, execution/control/cleanup/bounded capacities are
12/5/8/1. API startup of supported foreground SQL reserves control and the bounded
role before publishing its execution pool when read isolation is enabled.
Consumers share these node pools; connections do not allocate their own pools.
An insufficient configured queue envelope refuses publication.

Focused worker/API tests cover full foreground/cleanup saturation, 32 consumers,
protected IO progress, failed publication/retry and final retirement. Actual
compiled API role integration remains pending. Reserving this executor does not
qualify a production operator or prove its private stack/heap/work bounds; stage
wiring and those resource proofs remain necessary.

### Operator eligibility inventory

| Path | Established execution boundary | Retained continuation | Remaining bounded-lane proof | Current policy |
| --- | --- | --- | --- | --- |
| SQL preparation/cache (`api/sql_preparation.zig`) | Controlled compilation and cache waits retain original authority; verified waits lend CPU | Request capsule and plan lease | Maximum non-yielding work, fixed continuation footprint and private worker/heap floor | General |
| Constant SQL (`api/sql_execution.zig`) | Existing read fences and catalog/provider setup | Adapter, plan, parameters and result owner | Catalog/provider setup and all required allocations lack a verified bounded work/transfer contract | General |
| SQL scans (`sql/read_stream.zig`) | Controlled scan callbacks and the worker-ready barrier lend/reacquire CPU | Snapshot, pages and physically joined worker children | Backend non-yielding maximum and a verified transition/cleanup bound | Unbounded plans analytical; remaining unverified plans general |
| Typed SQL vector expressions (`sql/typed_kernel.zig`, `sql/vector_eval.zig`) | Prepared numeric chunks run on the original coordinator with node-charged fixed scratch and owner checkpoints; serial coordinator scope is separate from parallel authority | Immutable program, input page, result and fixed prepared scratch | Qualify non-yielding time and complete continuation/worker bounds; bind checked prepared CPU phases and protected-arrival proof; exclude preparation/fallback from borrowing | Ordinary general/analytical execution; no production borrowing or bounded eligibility |
| SQL grouping/join/order | Existing cooperative operator scopes and resource/output wait owners | Aggregate/join/sort state and separately owned spill resources | Complete continuation accounting, measured debt and bounded demotion transfer | Analytical |
| JSON/graph query ingress | Shared query root and existing resource owners | Backend-specific query and child/result owners | Trusted plan classification and each operator's maximum non-yielding/continuation bounds | General |

No row currently establishes production bounded eligibility. These are source
boundaries and missing predicates, not a replacement for matching production
fixtures. The minimum cleanup/transition allocation for each demotable operator
must be verified before it can enter the bounded lane.

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

Managed server startup establishes a dedicated cleanup CPU slot within `slots`,
independent of `control_slots`. Set `cleanup_slots` to one explicitly, or leave
it at zero for startup to reserve one before publishing work. The public policy
requires at least three total slots and four operation entries, and the combined
CPU reserves must leave at least one ordinary slot. Startup also checks that the
control metadata budget can hold its actual cleanup owners. It never expands the
configured total or changes an active policy. Generic native scheduler consumers
can retain the zero-cleanup shared-control policy. These rules apply to fixed and automatic managed scheduling. Remaining
production integration still requires focused verification.

Explicit `admission.runnable.capacity_mode: "automatic"` resolves capacity once
at startup using the platform CPU and memory-envelope probes. `slots` (default
8) is an upper bound. Affinity and visible cgroup quotas determine whole-worker
parallelism, with an ordinary/health/cleanup floor of three slots for the default
reserves. Fractional quotas retain one ordinary worker; the protected lanes can
share OS CPU time and do not promise dedicated cores. The configured ceiling
must accommodate every reserve plus one ordinary worker. `max_bytes` (default
8 MiB) is an upper bound on scheduling metadata; automatic mode also limits it
to 1/32 of the visible host/container memory envelope. DATA and standalone also
clamp that envelope to a stricter effective CLI/environment process budget before
creating the shared node owner. Resolution produces a fixed effective policy;
consumers never independently re-probe or shrink an already published owner.
Automatic DATA and standalone admission also cap request and retained-state pools
to the smaller of their configured limits and 1/8 of that same envelope. Existing
API transport/control and outbound control reservations are established from
those effective limits before ordinary allocations can exhaust node accounting.
Fixed policy retains its declared pool limits. Pools below the supported 1 MiB
minimum reject automatic startup.
The implicit managed policy reserves 1 MiB of its metadata ceiling for control.
This includes persistent sender/reconciliation metadata and their maximum polling
scratch while leaving protected cleanup and health headroom. An explicit fixed
scheduler retains its configured reserve; remote protocol bootstrap checks that
both consumers fit before durable enrollment and never enlarges a live policy.
An envelope too small for
the configured control-byte reserve fails startup. Missing envelope information
retains the finite configured byte ceiling. This does not cap storage, model or
total process heap. Weights, operation bounds, quantum and finite wait limits
remain configured values; effective slot and byte bounds are reported by the
existing runnable telemetry. This is static initial sizing, with no live policy
resizing. An explicit runnable object with omitted `capacity_mode` remains
`"fixed"`. Omitting `runnable` selects automatic startup sizing; explicit `null`
disables it. Omitting query waiting with scheduling enabled selects eight queued
requests, 8 MiB of retained input and a 250 ms maximum wait. An explicit waiting
object uses its property defaults (all zero for an empty object).

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
5. Verify focused main/fixed/scheduler correctness and resource-exhaustion
   schedules. Derive and publish automatic sizing and bounded-wait defaults from
   verified behavior. The full qualification sweep is waived. Adaptive tuning
   follows reliable fixed telemetry.

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


### Serial lake receiver ownership (under qualification)

Coordinator reads now use a provider capability distinct from an exclusively
owned scan child. Lake aliases clone mutable version evidence, delete state and
reader receivers while borrowing the statement's pinned inventory. Streaming,
materialized, COUNT and relation scan pulls attach the current execution only
for that operation. Before returning, the provider physically joins speculative
work and removes the borrowed execution; a retained portal cursor or column page
therefore cannot keep a pointer to a previous Execute call. A final checkpoint
checks the original result authority after joining. Row pages acquired before a
failed final checkpoint are discarded by their owner.

Row-group lookahead uses the request execution family with its own reader and
physical wait boundary, while the legacy scheduler remains for unscheduled
calls. Cancellation polling checks lifetime rather than a borrowed parent CPU
checkpoint. Existing range prefetch and row-group lookahead both retire before
a coordinator attachment is removed.

Qualification includes interleaved alias row/column pulls using different call
executions, lazy Iceberg metadata isolation, control progress while physical
reads are parked, and actual streaming, COUNT and repeated-alias SQL paths.
This does not yet qualify statement capture/provider opening or split planning
under the root execution, index/artifact readers, or the broad overload and
durability/default-sizing gates. Those remain full-design requirements.


### Lake split planning (under qualification)

Ordered and associative split calls temporarily attach the coordinator's
execution. Physical footer discovery uses the exact Store wait. Multi-file
planners reserve children in that request family, use independent readers and
version state, join the entire wave before admitting its plans, and retain the
original cancellation/deadline. Registry exhaustion falls back to the existing
coordinator without promoting the work to control capacity or a legacy pool.

Returned scan cursors get independent Store wrappers containing their original
lifetime evidence. They cannot retain the planning call's CPU receiver after the
coordinator detaches. Denied final authority closes every returned child and its
owned cursor array before reporting the failure. Native qualification covers
one ordinary slot, actual footer child dispatch, full-registry fallback,
cancellation/deadline after payload acquisition, ordered lazy ranges, control
progress, and scanning after the planning execution finishes.

Statement capture, object-provider initialization, index/artifact readers and
actual row-group lookahead remain separate ownership qualification requirements,
as do the full overload, durability and default-sizing proofs.


### Lake statement capture (under qualification)

Native lake statement and aggregate-source capture use a temporary query
execution for inventory listing, Iceberg metadata and related provider reads.
The exact Store operation suspends CPU while preserving the original token and
deadline. A successful pinned source removes the execution before returning;
it retains its original lifetime evidence and can survive the call's finish.
Denied resume discards owned metadata and closes the acquired provider store.

Qualification covers physical inventory-list CPU suspension, protected control
progress, cancellation/expiry after an owned listing is acquired, independent
source-lifetime rejection before provider opening, source access after call
retirement, initializer failure cleanup, and unchanged unscheduled behavior.
Provider initialization retains CPU for validation and credential parsing; its
physical secret/file/credential waits need explicit context support rather than
parking the entire initializer. Index/artifact reads, actual row-group lookahead
and all global overload/durability/default-sizing proofs remain requirements.


### Physical read-pool telemetry

The API kernel statistics ABI (version 31) carries a layout-only read-memory
snapshot. Standalone and DATA health metrics expose `antfly_read_memory_*`:
configured reservation, per-lane byte/group capacity and live byte/group usage,
transition capacity credits and refused allocator operations. Lane labels are
fixed to general, bounded and transition. Disabled pools report disabled and
zero capacity. The reservation is already inside the request ceiling; transition
credits are capacity promises and are not additional live allocation.

The pool snapshot holds the pool lock for live counters. Capacity and the pinned
reservation are immutable after startup. Managed serverless JSON metrics have
not yet gained this snapshot. These metrics do not establish operator bounded
eligibility, physical worker isolation or remote-attempt quiescence.


### Pgwire portal allocation routing

When read isolation publishes a pool, retained pgwire SELECT portals allocate
the native pull stream and their capsule arena from a stable general-lane
allocation group. The native stream keeps its existing per-stream budget inside
that group. General and analytical scheduling classification remains unchanged;
this routing does not establish bounded eligibility. Pool/group exhaustion
refuses startup rather than falling back to uncharged allocation.

Returned page arenas retain the stream's group through the existing resource
borrow and independent cleanup supervisor. Physical stream teardown waits for
all returned pages; the final capsule destruction frees its arena before
releasing the group and connection lifetime fence. Failed opens and materialized
fallback release unpublished groups. Non-isolated policy keeps the original
allocator. Authentication, catalog-owned identity, session state, page wrappers
and materialized results still have their separate existing budgets. HTTP SQL
execution allocation routing remains unfinished.


### HTTP and materialized pgwire execution allocation routing

HTTP SQL reads now create a general allocation group after coarse admission.
Only native read execution uses it; preparation, authentication, controls,
writes and encoded response storage retain their existing budgets. The stable
worker destroys native results and ends its read fence before releasing the
group, then parks the original root for transport handoff. Encoded output has
its independent response/transport ownership.

Materialized pgwire reads allocate a stable result owner before native execution.
It owns the general group and, after successful publication, the native result.
Wire result release destroys that result before returning group capacity. Error
unwinding follows the same ordering. Native per-query budgets remain nested
inside the pool. The feature does not classify these reads as bounded or route
catalog/session allocations into protected pools.


### Remote-attempt accounting and generation gate core

`common/remote_attempts.zig` supplies coordinator and worker state machines over
finite caller-owned arrays. The caller must account for that registry storage.
Coordinator reservations charge count and declared bytes against aggregate and
per-destination limits before transport publication. A failed transport converts
the same record to uncertainty without allocation or releasing its charges.
Unpublished reservations can abort; published attempts require verified terminal
quiescence or a matching authenticated worker fence. Duplicate/late responses
cannot recreate ownership or release a reused slot. Retries use distinct attempt
sequences under the same logical operation identity.

The worker gate deduplicates within a bounded sequence window. Advancing the
window never evicts active work; discarded old IDs are rejected rather than
executed again. Fencing first rejects delayed old-generation dispatch, then waits
for all old physical work to finish and for the integrating owner to persist the
new floor before acknowledging it. A freshly reconstructed gate cannot reopen its
restored generation. A fresh coordinator remains closed until all prior
configured destinations supply fence proofs; runtime generation changes preserve
known old charges until each destination reconciles.

Seven unit tests establish these local state transitions, including exact-once
retirement, identity conflicts and sequence exhaustion. This core is not connected
to production dispatch. Authenticated versioned wire context, durable floor
storage, membership/incarnation authority, bounded coordinator registry ownership,
physical cleanup callbacks and real partition/restart verification are required
before enabling it. Passing a generation number to the core is not itself proof
of persistence, fencing or remote quiescence.


### Durable worker generation floors

`common/remote_fence_store.zig` provides a fixed-capacity worker-local registry.
It uses an exclusive file lock, a versioned/checksummed fixed-size record image,
file sync, atomic rename and directory sync. Normal open rejects missing, corrupt,
capacity-mismatched or wrong-incarnation state. First creation is explicit and
must follow authoritative incarnation initialization in an already durable
private directory. Registry capacity never evicts old coordinator identities.

The store's fence wrapper closes the worker gate before writing its new floor.
Only successful durable publication plus physical old-attempt quiescence permits
an acknowledgement. Publication errors poison cached state until reopen, including
errors after rename; no later request can rely on the uncertain cache. Reopen
restores a closed gate that requires a strictly newer generation. Allocation
failure releases constructor ownership without retaining the exclusive lock.

Four real-filesystem fixtures and the seven core tests pass, including injected
file/directory sync failures and native close/reopen. This is not a hard crash or
power-loss proof. Production wiring still needs authenticated wire requests,
authoritative membership/incarnation ownership, bounded registry memory charges
and recovery of physical external children before gate acknowledgement. The
store alone cannot prove that remote child work from a prior process stopped.


### Signed versioned attempt context

`common/remote_attempt_wire.zig` defines a fixed-size v1 context containing exact
coordinator/generation/logical-operation/attempt identities, worker incarnation,
request binding, declared byte charge and remaining duration. Unknown versions,
flags and operation shapes fail closed. The duration becomes a finite deadline
on the receiver's local clock; it is never interpreted as a sender timestamp.
Network-delay uncertainty remains, and elapsed time is not quiescence evidence.

Optional internal service signing embeds the encoded context inside signed JWT
metadata and requires the exact coordinator subject. The API authentication
helper first verifies the service credential, then checks subject, destination,
operation kind and a length-framed method/path/body digest. User credentials,
unsigned hints and legacy credentials without context cannot authorize this
protocol. Existing ordinary service credentials remain supported by their old
paths. Worker duplicate records also bind the original request digest, preventing
an attempt ID from being reused for different content.

Six wire/authentication tests pass, including real signature tampering and
allocation-failure sweeps through both service and ordinary user credential
construction. These exposed and fixed lost ownership when a permission or row
filter was created before a fallible append, and partial identity construction
cleanup. The ledger/store suite passes twelve tests on matching core sources.
Production routes do not yet consume this context. Membership authorization,
registry startup/accounting, physical work/deadline ownership and authenticated
status/fence/dispatch handlers still require integration and distributed tests.


### Authoritative remote membership admission

`common/remote_membership.zig` derives coordinator identity from the durable
metadata incarnation and registered node ID. New reads require active registered
coordinator/worker nodes and a matching store node, physical root and process
reporter incarnation. Status/fence reconciliation remains eligible during known
drain/finalization states. Changed roots, unknown members and health observations
never retire old attempt charges or establish termination.

The API adapter obtains the exact owned linearizable snapshot and requires its
free capability. It does not fall back to cached, observational or ordinary admin
views. The snapshot lookup and return handoff preserve cancellation and the
earlier original/verified deadline. Authentication now captures platform
monotonic receipt time internally, including verification work, so callers cannot
mix clock domains or refresh the received budget.

Ten focused tests pass, including source-boundary cancellation/free counters,
missing-capability rejection, earlier deadlines, drain reconciliation, ambiguous
membership, signed context and credential allocation failures. This checks one
admission cut. Worker registry ownership, handler/client integration and resume
revalidation remain open. Removal of a coordinator requires prior reconciliation
or separately authorized recovery; absence from membership is never a fence.


### Bounded remote worker ownership registry

`common/remote_worker.zig` composes the attempt gate, durable fence store and
validated membership bindings. Startup allocates bounded coordinator, duplicate
window and active-attempt arrays through the caller's accounted allocator. Ready
coordinator records are never evicted based on health. Global/per-coordinator
count and declared-byte allowances are independent of actual application heap
budgets; routing those allocations remains an integration requirement.

A read lease can retain bounded physical resource references before publishing
pages, snapshots, output buffers or child tasks. Logical close stops publication
but keeps status active and charges held until every reference releases AFTER
physical destruction/join. Fencing closes the old generation, signals cancellation
and persists its floor, yet cannot acknowledge while any old resource remains.
Duplicate requests cannot extend the original deadline. Resume-policy failures
preserve the first cause and retain ownership through cleanup.

Capacity/deadline rejection after identity registration creates a terminal
record; later duplicates cannot start when capacity recovers. Unregistered/window
rejections are not terminal proof. Startup is closed until a borrowed bootstrap
authority confirms prior external work is reconciled/fenced with publication
stopped. Status remains unknown before that barrier. The integrating supervisor
must provide the real recovery authority and join all callbacks before teardown.

The component target passes 22 tests, including five registry fixtures with a
real retained buffer, pending fence, final zero charges, independent coordinator
progress, rejection tombstones, failed resume and allocation rollback. Production
bootstrap, handler/client wiring, actual physical-reference attachment, CPU/heap
routing and real distributed recovery verification remain unfinished.


### Composed API receiver helpers

The API owner now composes verified service context, linearizable membership and
worker registry operations through `beginRemoteRead`, `fenceRemoteReads` and
`remoteReadStatus`. These helpers use the registry's own local identity and fixed
operation kind. They preserve the original cancellation and earlier request
budget; the worker lease exposes that exact captured deadline for backend context.
Status/fence credential construction and context parsing use the protected API
allocator. Existing ordinary service authentication retains its previous allocator.

The native API fixture signs real credentials and exercises this composition
with a durable registry and authoritative-snapshot callback. With ordinary API
memory fully occupied, signed status stays active and fencing stays pending while
a real output buffer retains a resource reference. Physical buffer release then
allows fence completion and zero registry charges. The original shorter request
deadline survives into the lease. Ten API tests and 22 component tests pass.

These are owner-side helpers, not registered HTTP routes. Runtime bootstrap,
opaque-owner integration, production read/output reference attachment, client
protocol, authenticated terminal/fence response proofs and real distributed
recovery/overload verification remain requirements. The fixture's metadata source
models the linearizable callback contract; it does not establish a Raft barrier
or independent physical control-executor progress under CPU/storage overload.


### Root-signed remote quiescence replies

`common/remote_attempt_reply.zig` adds a fixed-size, versioned Ed25519 reply
protocol. Replies bind the exact status/fence request, cluster/node/store/root
and reporter incarnation, outcome and observed generation. Remaining duration is
excluded so a late verified reply can reconcile uncertainty after caller expiry.
The expected public key must come from authoritative pinned root enrollment;
response fields, health observations and self-advertised replacements cannot
supply that trust. The existing physical-root key enrollment is resolved through owned linearizable metadata; runtime bootstrap must still install that enrolled signer.

The signer obtains status and generation atomically from the worker gate, and
signs fence acknowledgments only after durable fencing and physical quiescence.
Pending fencing cannot produce an acknowledgment. Verification is inseparable
from the coordinator reconciliation helper. Unknown/active replies retain charges;
terminal evidence releases only the exact attempt. A higher fenced generation
blocks reservations and previously reserved dispatches while preserving other
destinations' uncertainty. Generation advancement must exceed both the current
and greatest observed generation; delayed lower evidence cannot close a newer
one. Persisted generation allocation remains a bootstrap requirement.

`signedRemoteReadStatus` and `signedFenceRemoteReads` compose existing API
service authentication, linearizable membership and this signer. The API fixture
verifies actual signed active/fence/fenced replies while ordinary API memory is
full and a retained output buffer prevents fencing until physical release.
The standalone reply target passes 27 tests, including imported ownership,
durable-store, membership and wire tests. Five reply fixtures cover forged
keys/signatures/identities, request/version mismatch, duplicates, stale fences,
late replies and retention of another destination's uncertainty.

These are tested owner helpers, not enabled production HTTP routes or a deployed
client protocol. Runtime/opaque-owner bootstrap, trusted key enrollment, durable
coordinator generation allocation, attachment of every physical read consumer,
external-work recovery and real partition/restart/overload integration remain
open. Full qualification remains waived and the full design remains unfinished.

Both current-source suites pass (27 component and ten API tests). Evidence and
remaining scope are pinned in
`remote-attempt-root-signed-replies-main-7aa-oct13-application.json`.


### Durable outgoing epochs and destination history

The bounded remote store now persists outgoing coordinator state alongside
incoming worker floors. `startCoordinator` durably advances beyond the last
issued/observed generation before returning a closed ledger. Restart must supply
every previously published destination in its fence barrier. History is bounded
and never automatically evicted when routing, membership or health changes.
A full history fails publication closed until authoritative retirement is added.

`prepareDispatch` records the destination durably before marking a reservation
dispatched. It rechecks ledger generation after I/O without holding the ledger
mutex over filesystem work. Failure leaves a reservation locally abortable;
wire publication is permitted only after success. Verified reply reconciliation
now requires this store: destination validation precedes durable observation of
the reply's generation, then the ledger applies the result. A failed observation
keeps uncertainty charged and poisons the store until reopen. Accepted higher
floors therefore survive restart and cannot be reused by outgoing startup.

The unreleased state image is version 2. The existing basename is retained;
older/incompatible images fail closed rather than being silently recreated or
interpreted as empty. Generation and destination history use the same exclusive
owner, checksummed atomic image, file sync and directory sync as worker floors.
The additional fixed history array uses the supplied accounted allocator.

The current component suite passes 31 tests. New cases cover reopen with an
uncertain destination, refusal to omit its fence barrier, pending old worker
execution, delayed old dispatch, history/epoch exhaustion, file-sync and
post-rename directory-sync faults, and accepted signed generation persistence.
These exercise real files and injected I/O faults, not actual machine power loss.
Bootstrap must use this startup/publication path; these helpers do not yet
install that path in the production node or provide key enrollment, prior
external-work recovery, authoritative destination retirement, or registered
read/status/fence routes. Durable I/O still needs the production CPU-lending
context. Full qualification remains waived; the full design remains unfinished.

Both final-source suites pass (31 component and ten API tests). Exact receipts
and scope are recorded in
`remote-coordinator-durable-startup-main-7aa-oct13-application.json`.


### Registered status/fence HTTP routes and enrolled root authority

The data HTTP surface registers `POST /internal/v1/attempt/status` and
`POST /internal/v1/attempt/fence`, with matching regenerated internal OpenAPI
routes. A node must install stable borrowed registry/signer owners through
`remote_attempt_control`; unconfigured service returns 503. This registration
does not enable remote read dispatch or bypass bootstrap recovery.

Ingress verifies the exact signed service context once through protected API
memory, retains its captured finite deadline in context-owned state, and passes
that deadline through authoritative membership and signing. Migration mode never
admits unsigned control evidence. Control requests accept only the canonical
body `{}`, identity encoding and no query parameters. Fixed route classification
selects protected transport capacity, and handlers use bounded control admission.
The fixed-size signed response is copied into response-owned storage before the
handler returns; pending fencing produces 409 without any signed acknowledgment.

Legacy internal RPC routes reject attempt-bearing signed credentials until a
physical ownership adapter is installed. This guard parses metadata structurally
so escaped JSON keys cannot bypass it. Ordinary service credentials retain their
legacy behavior. Registering control endpoints therefore cannot accidentally
make untracked legacy reads eligible for terminal/fence evidence.

The existing data runtime already enrolls `replica_root_public_key` immutably for
each physical root through linearizable metadata. Reply authority now resolves
that enrolled key from the same owned snapshot capability, with exact metadata,
node, store, root and reporter identity checks. Observational/planning snapshots,
missing/ambiguous identity and unenrolled keys fail closed; health/draining does
not authorize replacing a root key. `Signer.initEnrolled` accepts the existing
`storage_db_root_signing_identity.State` only when its root and derived key match
that authority. No parallel enrollment mechanism or destructive permission is
introduced. Runtime bootstrap still needs to load and install these stable owners.

The current native component suite passes 34 tests. Twelve API tests pass with
real TCP control calls over the production control registration and middleware,
including ordinary API heap exhaustion, retained output/pending fencing, final
signed evidence, migration/body rejection, and the legacy RPC guard. The same
compiled binary that encountered sandbox listener `Unexpected` passed unchanged
with localhost networking, with zero leaks. The focused listener registers the
control/capability routes rather than unrelated public execution handlers.
The metadata callback models an authoritative cut; it is not a real Raft barrier.
OpenAPI generation and consistency checks pass. Exact source, binary, log and
artifact hashes are recorded in
`remote-control-http-enrolled-authority-main-7aa-oct13-application.json`.

Node/opaque-owner bootstrap with actual prior-process recovery, Native I/O CPU
lending, production read/child/snapshot/output ownership and outbound client
integration remain open. So do authoritative destination retirement and real
distributed partition/restart/global-overload verification. Full qualification
remains waived and the full design remains unfinished.


### Durable outbound dispatch and control reconciliation client

`api/remote_attempt_client.zig` composes the existing fixed ledger, durable store,
service credentials and enrolled reply authority. It borrows explicit read and
protected control allocators/executors and creates no per-attempt reconciliation
task. Targets are immutable owner-supplied enrollment bindings. Owned-read
activation defaults disabled; control-route availability and health cannot enable
it. Resolved target URLs must stay within the configured node and internal path.

A dispatch reserves its attempt before constructing a body/path-bound credential.
All fallible local request preparation precedes durable destination recording and
transport publication. Before publication, allocation/cancellation/target errors
abort only the unpublished reservation. After publication, transport failures
return the exact uncertain ID; every HTTP response remains charged through body
destruction until a verified status/fence reply reconciles it. Existing caller
credentials are replaced, the earlier original deadline is preserved, and buffered
read responses are bounded by the declared allowance. Executors must join the
physical call and disable redirects; the standard internal executor does so.

Control requests use the independent allocator/executor, bounded reply size and
canonical versioned status/fence endpoints. Pending fencing or transport/HTTP
errors never release allowance. A valid reply must match the pinned root key,
request and destination, and persist observed generations before ledger mutation.
`Store.advanceCoordinator` now persists a runtime epoch before advancing the
existing ledger, retaining prior uncertainty while destinations fence. A failed
publication poisons the store; reopening cannot reuse the uncertain epoch.

The common component suite passes 35 tests, including runtime advancement with
pre-rename file-sync and post-rename directory-sync failures. Thirteen API/client
tests pass on the unchanged compiled binary with localhost access, with zero
leaks. The client fixture sends real TCP status/fence calls through production
control registration and middleware. It fills ordinary API memory, retains a
real output buffer after a scripted transport reset, rejects a wrong reply key,
keeps a fence pending until physical release, and retains an HTTP 503 charge until
signed terminal proof. Allocation-failure sweeps cover request preparation and
scripted output cleanup; pre-publication cancellation and invalid targets leave
no charge. Evidence is pinned in
`remote-client-dispatch-reconcile-main-7aa-oct13-application.json`.

The fixture's read executor composes real service verification and worker
admission, but remains scripted: this is not a registered production read RPC or
native storage/SQL execution proof. Production adoption must carry ownership
through all child RPCs, snapshots and retained outputs before enabling reads.
Node/opaque-owner bootstrap, actual prior-process external recovery, a bounded
operational reconciliation loop, authoritative destination retirement and durable
I/O CPU lending remain open. Full qualification remains waived; real distributed
partition/restart/global-overload proof and the full design remain unfinished.


### Bounded reconciliation passes and higher-floor startup recovery

The outbound client now exposes a caller-driven `reconcilePass` with a retained
round-robin cursor. Its fixed ring covers destination fences, generation probes,
and published attempt status. Unpublished reservations are never polled. Each
pass visits a ring position at most once and obeys explicit scan/RPC budgets;
the cursor advances before I/O, including failures. Per-call deadlines are clipped
to the original pass deadline and cancellation reaches the transport. No task,
queue or selection allocation is created for each attempt. A node-owned periodic
scheduler must still invoke passes through its protected execution resources.

A destination with an older coordinator epoch may reject fencing without
returning a signed acknowledgment. Separate status probes use a sequence value
that `Coordinator.reserve` can never allocate, so they cannot alias or retire a
real attempt. A verified higher generation is persisted and reported as requiring
explicit owner advancement. The pass stops until that transition occurs; it does
not spin through fresh epochs or discard uncertainty based on an HTTP error.

Generation advancement is now safe during partial startup. All acknowledgments
from the previous epoch are invalidated and the full destination barrier repeats.
It cannot open by combining an earlier peer's acknowledgment with a later epoch's
remaining replies. Runtime advancement continues retaining published charges.

The native component suite passes 36 tests. Thirteen API/client tests pass on the
unchanged compiled binary with localhost access, without leaks. The TCP fixture
exercises active, pending-fence and terminal passes while ordinary API memory is
full. It also combines a scripted unavailable destination with a real TCP peer
whose durable worker floor is higher than the restarted coordinator's epoch.
Repeated failed calls do not monopolize the cursor: the healthy peer's signed
probe is verified, the floor persists, the owner advances to generation 6, and
that peer completes its fence. Dispatch stays closed while the unavailable peer
remains unresolved. Cancellation and deadline exhaustion perform no RPC, and
every pass respects its scan/call ceiling. Scope, source and binary evidence are
pinned in `remote-reconcile-bounded-pass-main-7aa-oct13-application.json`.

These budgets bound requested work and transport waits; they do not preempt a
physical kernel-I/O join or filesystem sync. Node-owned periodic scheduling,
protected CPU/I/O integration, production read RPC/child/snapshot/output adoption,
actual external-work recovery and authoritative destination retirement remain
open. The fixture is not a real distributed restart or Raft proof. Full
qualification remains waived and the full design remains unfinished.


### Scoped control CPU, closed generations and explicit abandonment

The shared runtime now owns the checked execution-wait boundary; embedded
callers retain a compatibility alias. Durable fence-store operations take an
optional per-call wait scope. Contended mutex acquisition and filesystem calls
lend CPU while preserving the operation's metadata/retained ownership. Encoding
and hashing run with CPU held. Failed CPU reentry discards an acquired mutex or
new file handle under its original owner; partial durable publication poisons
the store and cannot produce an acknowledgment. Bootstrap create/open still use
the existing unscoped path.

Authenticated control ingress owns a heap-stable control execution frame from
receipt through transport cleanup. Admission, credential verification,
membership, signing and durable publication share the original deadline and
cancellation. File waits park the frame as resource-blocked; response output
parks it as output-blocked. Outbound callers may borrow their current control
frame through `Client.control_wait`; no mutable global wait is installed on the
store. This does not yet supply CPU lending for every metadata provider's I/O.

A cold worker registry can restore a closed generation equal to the client's
current generation. Its signed `closed` outcome forbids further dispatch and
requests explicit durable advancement, retaining every uncertain charge.
Unknown status is insufficient for that decision. Closed status is also
insufficient for physical retirement; recovery and a new-generation fence must
finish before the old allowance can retire.

The operation owner can explicitly `Client.abandon(id)` after it stops publishing
uses of an attempt, including retained snapshots. The existing fixed reconciliation
ring then sends signed, request-bound cancellation to `/internal/v1/attempt/cancel`.
Transport errors and response destruction never implicitly abandon an attempt.
The protected route verifies the same enrolled identity, linearizable membership,
original deadline and fixed body as status/fence. An active attempt receives a
cancellation signal but retains all resource references and allowance until its
physical owner closes and its children, buffers and snapshots retire.

Cancellation arriving before dispatch records a terminal tombstone in the bounded
duplicate window. It cannot evict active work, execute a request, cancel a generation
probe, or reset an epoch. Subsequent delayed dispatch is suppressed. Existing
read-body bindings remain enforced; only a cancellation-before-dispatch tombstone
has no read-body binding, since it permits no execution. These tombstones need not
be individually persisted: every restored gate is closed and cannot accept its old
epoch, even after the external recovery check succeeds. A cancellation against a
cold registry cannot turn lost volatile state into terminal evidence. The client
retains uncertainty until separate signed terminal or quiescent fence proof.

Production read activation remains disabled. Node/opaque-owner bootstrap, real
prior-process external recovery, node-owned periodic scheduling, integration of
abandonment with actual read/child/snapshot/output owners, authoritative permanent
loss/retirement, all metadata I/O lending, and real distributed partition/restart
proof remain open. The broader scheduler still needs its remaining physical
worker/operator bounds, production bounded-read eligibility, capacity borrowing,
managed metrics and whole-default-scope verification. Full qualification remains
waived; the full design is unfinished.

Verification: 42 native component tests, 39 shared runtime/ABI tests and 15
API/client/TCP tests pass without leaks. The TCP tests use the unchanged compiled
binary with localhost access. They exercise cancellation while ordinary API
memory is full and output remains physically held, cancellation before worker
admission, delayed dispatch suppression, and closed equal-generation recovery.
Core tests cover denied CPU reentry, handle/mutex discard, uncertain publication,
duplicate-window limits, original request bindings and recovery requirements.
OpenAPI regeneration/check and formatting/diff checks pass. Source/binary evidence
is pinned in `remote-scoped-control-cancel-main-2c8240-oct13-application.json`.
The recovery fixture recreates a registry against a durable store; it is not an
actual prior-process external-work or distributed restart proof.


### API-owned periodic reconciliation and joined shutdown

`api/remote_reconciliation.zig` now supplies one periodic consumer per live
coordinator, retaining its fair cursor between bounded passes. A ledger binding
rejects duplicate supervisors. Policy bounds interval, pass duration, per-RPC
duration, call count and scan count. A pass may persist at most one generation
advance; later passes repeat destination fencing while old published charges
remain owned. Idle polling holds no runnable root or CPU grant.

Trusted API-kernel bootstrap installs it with `startRemoteReconciliation` before
request publication. The API first establishes protected control workers, then
reserves an independent physical worker for the consumer. Controller state and
imported worker receiver metadata use the protected control allocator. Each pass
owns a control execution root with an original finite deadline and shutdown
cancellation; its scoped wait adapter lends CPU around actual file, mutex and
transport waits. Its metadata demand leaves another control operation and half
the control byte reserve available. The supplied executor still must physically
join requests and obey its independently accounted transport limits.

`stopRemoteReconciliation` cancels and physically joins before releasing the
ledger binding or worker lease. API destruction invokes it before closing the
scheduler or freeing request/control memory. Borrowed ledgers, stores, enrolled
targets and executors must remain valid until that join. An allocation failure
before publication unwinds the binding. Stopping never releases uncertain remote
allowance; an installed replacement resumes reconciliation using signed proof.

The final 16 API/client tests pass on the unchanged binary with localhost access,
without leaks. The new lifecycle fixture uses the imported worker factory, fills
ordinary API memory, occupies ordinary CPU, stalls cancellation at a real wait,
and admits separate health-control work. Stop observes shutdown cancellation and
physical return while keeping the 512-byte uncertainty charged. Restart succeeds
while ordinary memory remains full; a signed higher worker floor causes exactly
one durable advance to generation 4 and a later automatic fence. Allocation-denial
rollback and duplicate consumers are also checked. The 42-test component target
remains green (final run cached). Evidence is pinned in
`remote-periodic-control-owner-main-2c8240-oct14-application.json`.

This supplies the joined API-owned periodic runtime, not its production durable
bootstrap caller. The lifecycle fixture's transport is scripted; the existing
TCP fixtures separately verify the actual control routes. Node/opaque-owner
initialization and installation, actual external-work recovery, physical read/
child/snapshot/output adoption, protocol rollout and real partition/restart proof
remain open. The broader scheduler gaps listed above remain open too. Full
qualification remains waived and the full design is unfinished.


### Kernel-owned remote protocol bootstrap

`api/remote_attempt_owner.zig` owns the durable store, inbound registry, outbound
ledger, copied target URLs/authorities and separate read/control transports at a
stable API-kernel address. Bootstrap accepts value descriptors through a guarded
function-table capability; it never imports foreign ledgers, mutexes, signers or
executor pointers. Statistics return values without exposing owned pointers.

The API resolves local and target enrollment from one owned linearizable metadata
snapshot, loads the existing physical root signing checkpoint, and checks that
key against enrollment. It never creates a replacement root/key. Explicit first
initialization and normal reopening are separate modes: opening missing state
fails; initialization cannot overwrite existing floors. The trusted node caller
must separately establish authoritative first-use permission before choosing
initialization. No production caller infers it from a missing file.

The completed owner is published atomically for control ingress, permitting
installation after enrollment while listeners are already up. The API owns the
periodic consumer and joins it before freeing state. Inbound admission is closed
before teardown, and physical read resources must retire before registry
allocation is destroyed. Constructor allocations use accounted API memory;
transport initialization now has a fallible path with cleanup for partial setup.

The DataServer control round now attempts a rate-limited reopen after node/root
enrollment, using the 256-coordinator persistent profile. It checks for an existing
checkpoint before fetching metadata, builds bounded target descriptors from an
owned linearizable snapshot, and copies them through the API boundary. It includes
draining/unhealthy enrolled roots; it does not retire history based on health or
silently omit an old root from the startup barrier. Missing state remains inactive
and cannot authorize initialization. Actual non-test node/opaque-API code passes
semantic analysis without emitting a storage binary; a live node restart has not
been qualified.

Two owner tests pass on the unchanged compiled binary with localhost access.
They cover direct and opaque bootstrap, the default 256-coordinator profile,
missing-state refusal, duplicate initialization, increasing restart epochs,
enrollment mismatch, descriptor copying, constructor allocation-failure sweeps
and transport allocation rollback, without leaks. A newly created native fixture
supplies its fresh recovery proof and completes signed fencing through the owned
control executor and real TCP receiver sharing one protected CPU slot. The
transport caps its nested request/connect/watchdog tasks at eight workers
and one active pooled connection; a two-worker limit cannot accommodate
those nested tasks. The 16 API/client/TCP tests, 42 component tests (final target
cached) and ABI-prefix test pass as well. Source/binary evidence is pinned in
`remote-owned-bootstrap-main-2c8240-oct14-application.json`.

Bootstrap does not bypass external recovery and does not activate owned reads.
Authoritative first-use initialization/rollout, prior-process external recovery,
production read/child/snapshot/output adapters, peer-binding refresh across
reporter changes, authoritative permanent-loss handling and real distributed
restart proof remain outstanding. The broader scheduler gaps listed above also
remain open. Full qualification remains waived and the full design is unfinished.


### Permanent remote initialization authority (2026-10-14)

Metadata now stores a permanent first-use grant for each enrolled physical root,
independently of administrative retirement enrollment. Root-signed preparation
reserves an immutable initialization ID; activation advances that same ID and
cannot be downgraded by delayed preparation. Both preflight and Raft apply check
the metadata realm, registered root/key and current reporter. Store removal and
re-registration retain the grant, as do metadata restart and snapshot restore.
The new transition requires coordinated decoder version 34 before proposal.

Preparation, activation and signed status use the existing internal catalog route
without setting-administrator authority. The handler requires a host-verified
service token even in migration mode; root signatures provide store authority.
Uncertain commit outcomes remain uncertain and can be resolved through status.
Six targeted tests pass, with no leaks, across 51 build steps. Source and binary
evidence is recorded in `remote-initialization-grant-main-2c8240-oct14-application.json`.

This completes the metadata grant mechanism only. Journal ID binding, locked
first-use validation, durable publication history, activation before exposing
control ingress and automatic node initialization still need implementation.
External-work recovery and production remote reads remain closed. The full
design remains unfinished; full qualification remains waived.


### Journal initialization binding and publication history (2026-10-14)

The private journal now uses format v3 (same basename), persisting its metadata
initialization ID and a sticky publication flag. `openAuthorized` obtains fresh
authority only after acquiring the exclusive journal lock. Prepared grants permit
creation or retry of an unused matching checkpoint; active grants only permit
reopening that exact checkpoint. Missing active, mismatched, unbound and legacy
journals fail closed. No automatic legacy migration or replacement is performed.

Inbound worker admission persists the publication flag before gate admission or
physical ownership. Outbound dispatch persists it with destination history before
wire publication. File/directory sync failures prevent admission and poison the
owner until reopen. Epoch changes and terminal outcomes never clear the flag;
load rejects an unused flag combined with published destination history.
The scoped read-admission helper carries the existing CPU wait boundary through
journal I/O. The TCP read fixture retains its query root while lending CPU during
the first journal sync; its original cancellation and deadline checks remain.

All 46 component tests pass (the final identical component target is cached).
The unchanged final API binary passes all 16 tests with localhost access, with no
leaks. The sandbox run passed 14 and failed the two TCP fixtures; its earlier
localhost rerun exposed an unscoped fixture sync, now corrected and verified.
Evidence is pinned in `remote-journal-initialization-binding-main-2c8240-oct14-application.json`.

The API bootstrap still uses the explicit low-level open/create path; it has not
yet been switched to this metadata-authorized opener. Automatic node first use,
metadata activation before exposing ingress, and unused-journal recovery proof
consumption are next. Production remote reads and prior external-work recovery
remain closed. The full design remains unfinished; full qualification is waived.


### Automatic metadata-authorized node bootstrap (2026-10-14)

The node control round now permits first use without a pre-existing journal.
The API loads the physical root signing identity, resolves the registered local
and destination keys, and opens the journal through `openAuthorized`. While the
journal lock is held, a signed linearizable status reads existing authority; only
missing authority proceeds to coordinated preparation. A random candidate never
replaces an existing ID. All metadata calls retain the original bootstrap budget.

The owner constructs durable state and bounded transports, then activates the
exact journal ID in metadata before starting reconciliation or exposing control
ingress. A lost activation response requires a fresh matching active status;
blocked/ambiguous activation destroys unpublished state and leaves retryable
prepared journal state. Caller-selected open/initialize mode was removed from the
bootstrap descriptor; API ABI v32 rejects older layouts before reading them.

Only a matching, active journal whose sticky flag proves no work was ever
published can complete recovery automatically. The recovery callback reads that
durable evidence under the journal mutex while ingress is still unpublished.
Once publication is recorded, later processes remain closed pending genuine
external-work recovery. Missing active journals, old/unbound state and mismatched
root enrollment remain fail-closed. Production owned reads are still disabled.

Ten metadata/handshake/journal tests, two direct/opaque owner and transport
allocation tests, 16 API/TCP tests and the ABI-prefix test pass without leaks.
The component target is cached (46 tests previously passed). The actual non-test
node/opaque bridge passes semantic analysis in 45 build steps; this is not a live
cluster restart test. The owner and API binaries pass unchanged with localhost
access after their sandbox-blocked TCP fixtures. Source/binary receipts are in
`remote-authorized-bootstrap-main-2c8240-oct14-application.json`.

Remaining work includes recovery after published external work, production
read/child/snapshot/output ownership adapters, peer binding refresh, permanent
loss handling, distributed overload/restart proof and the broader scheduler gaps
listed above. The full design remains unfinished; full qualification is waived.


### Physical parent retention across remote uncertainty (2026-10-14)

The fixed coordinator outbox now accepts a move-only physical parent reference
before wire publication. The worker lease adapter obtains an additional resource
reference; no per-child reconciliation allocation is introduced. Retained client
dispatch transfers the reference into the reserved slot. Failure before transfer
leaves ownership with the caller; unpublished rollback after transfer releases it.
A published response, timeout, cancellation request, abandonment or new epoch
does not release the parent. Signed terminal/fence reconciliation retires only
the matching attempt/destination.

Retirement marks the slot before calling its parent release callback outside the
ledger mutex. The slot, attempt count and byte charge remain occupied throughout
the callback, preventing capacity reuse before physical release. Reentrant status,
duplicate terminal calls and late transport completion cannot release twice or
revive retirement. API owner destruction rejects remaining parent references
rather than dropping them with uncertain ledger state. Retention contexts must
remain stable until release and must not borrow a returning request's stack.

Fifty component tests and 16 API/TCP tests pass without leaks. They include closing
an inbound parent while its outbound descendant remains uncertain, fence refusal
until the descendant retires, epoch/destination isolation, failed transfer,
unpublished cleanup, and capacity retained through reentrant release. The TCP
allocation-failure sweep uses retained dispatch and verifies one release after
rollback or signed terminal reconciliation. The final binary passes unchanged
with localhost access after two sandbox TCP failures. Evidence is recorded in
`remote-parent-retention-main-2c8240-oct14-application.json`.

Production query fanout, snapshots and response buffers still need adapters to
use these retained dispatches and accounted physical owners. Cyclic descendant
recovery and distributed shutdown require their full protocol; these tests do not
prove them. Production owned reads remain disabled and the full design remains
unfinished. Full qualification remains waived.


### Shared physical ownership for internal group queries (2026-10-14)

With runnable scheduling enabled, the production internal group-query route now
runs planning, schema routing, storage and encoding on the bounded backend
executor. It joins that physical future before handing off the response. The
query receives one general runnable root and passes its team through planning
into storage/graph controls. The worker lends CPU through existing wait/child
capabilities, then parks for output before waking the network waiter. Explicitly
disabled scheduling preserves the legacy handler; unavailable configured workers
reject rather than silently execute unbounded work on the network handler.

A stable owner funds request work through the general read allocation group when
the configured read pool exists (otherwise the bounded server heap). The response
retains the same group after worker completion. HTTP response destruction frees
its body before releasing the owner, heap group, runnable metadata and node pin.
Output backpressure consumes finite operation/heap capacity without keeping CPU.
This does not assert bounds for every storage-private/operator allocation.

Body timeouts are anchored at handler receipt, including time already spent
waiting. After bounded decoding, the owned root can only tighten that deadline
before publishing children. Schema routing now accepts the request allocator and
execution context: selected index names must be freed by the same group, and
metadata routing retains original execution authority. Query response formatting,
identity-read-generation headers, catalog fences and read-index consistency are
preserved. Memory pressure maps to availability rejection.

Five owner/production-handler tests, 98 scheduler tests (71 local plus 27 API),
and 143 query-contract tests pass without leaks. Coverage includes schema-selected
index allocation, physical snapshot scratch, control progress during CPU waits,
response lifetime, cancellation/shutdown cleanup, expired deadlines, allocation
failure, and bounded outstanding slow responses. Source and binary evidence is
in `internal-query-physical-owner-main-2c8240-oct14-application.json`.

This production route still uses the existing distributed transport. It does not
yet adopt the owned remote attempt/retained-parent protocol across remote query,
snapshot and child RPCs; that protocol remains unactivated for production reads.
Prior-process/cyclic recovery, complete private operator bounds and the other
full-design gaps remain open. Full qualification remains waived.


### Remote lease adapter for physical query owners (2026-10-14)

The API now exposes receipt-time read authentication/membership resolution
separately from later worker admission, preserving the verified deadline while
waiting for CPU. The query-owner adapter can adopt that verified lease before
query allocations or snapshots exist. Admission persists publication history
through its existing CPU wait boundary. The signed byte allowance caps the
owner and its query-allocation budget, including the bounded fallback allocator
when the read pool is absent. Duplicate/terminal admission never creates another
physical lease. This adapter is not a newly activated production owned-read RPC.

A composite descendant hold now retains worker authority, the query heap group
and runnable metadata together. Response retirement frees output first, detaches
the borrowed HTTP cancellation callback under a lifetime mutex, and latches an
owned stop signal. Descendants may outlive that HTTP context without reading it.
The final worker lease closes only after buffers, runnable metadata, the owner
allocation and node pin have retired. Additional snapshot/cursor ownership still
requires its own physical adapter; retaining an allocator does not free resources
owned elsewhere.

Outbox retention carries owned atomic cancellation signals and a fixed native
monotonic deadline. Reconciliation reads those while the slot pins their owner;
no borrowed request callback runs under the ledger mutex. Parent closure,
remote-worker cancellation or parent expiry select exact child cancellation but
retain charges until trustworthy physical terminal/fence evidence. Publication
rechecks parent cancellation after durable preparation. A query cannot tighten
its deadline after publishing remote child authority; routing/snapshot deadlines
are tightened before query work starts.

Eight query-owner tests, 52 component tests and 16 API/TCP tests pass without
leaks. The owner tests simulate HTTP-context destruction while a descendant stays
uncertain, enforce heap allowances in pooled and fallback modes, and check
active/duplicate/terminal admission. Components cover owned cancellation, expiry,
and cancellation during durable preparation. The unchanged API binary passes
with localhost access after two sandbox TCP failures. Evidence is recorded in
`internal-query-remote-owner-main-2c8240-oct14-application.json`.

Production owned-read registration/activation, outbound distributed query
adoption, membership revalidation on resume, complete snapshot/child adapters,
cyclic/prior-process recovery and the remaining full-design gaps are unfinished.
The owned-read capability remains disabled. Full qualification remains waived.


### Authoritative membership checks on physical CPU resume (2026-10-14)

Remote query owners now require fresh membership validation at admission and
whenever a root, child or grandchild receives CPU after a quantum or I/O wait.
Each worker keeps independent validation state. Metadata calls lend CPU through
the same execution; their synchronous metadata-only children do not recursively
acquire the same proof. Validation preserves the original receipt deadline and
rechecks caller CPU-return restrictions after metadata I/O.

The API reads membership and the pinned reply public key from one owned
linearizable snapshot, with no observational fallback. Rejection stops runnable
execution and latches lease cancellation while retaining heap, worker and child
charges until physical cleanup. Seven append-only ABI details preserve membership
failure identities. Standalone borrowing test modules now import the platform
capacity dependency explicitly.

Scoped checks pass: 13 owner tests, 98 scheduler tests, 15 standalone borrowing,
child, fanout and ABI tests, 31 error-ABI tests, and 16 signed-RPC tests. The RPC
binary required localhost access after two sandbox TCP failures; the unchanged
binary passes all 16. Source hashes and logs are recorded in
`internal-query-membership-resume-main-2c8240-oct14-application.json`.

Production owned-read RPC activation, outgoing distributed adoption, complete
snapshot/cursor adapters, cyclic/prior-process recovery, private operator bounds
and the other full-design gaps remain unfinished. The owned-read capability is
still disabled. Full qualification remains waived.


### Signed attempts enter the buffered group-query worker (2026-10-14)

The existing group-local buffered query route now accepts AFAT read credentials
through the shared service-auth ingress boundary. It captures the receipt clock
before authentication/body reads, verifies the exact method/path/body, and passes
the same deadline into CPU admission. Unsupported read routes remain closed to
attempt credentials. AFAT v1 does not bind routing headers or query parameters,
so these overrides and non-identity content encodings are rejected for signed
reads. Legacy service-authenticated queries preserve their existing behavior.

The worker bounds authoritative membership scratch before acquiring its lease.
The signed allowance covers the owner and retained input before the remaining
query heap budget; metadata scratch must be freed before physical admission.
Preparing a different receipt cannot refresh that deadline or budget. Worker
admission precedes semantic planning, backend snapshot work and query execution.
Active duplicates and terminal replays return 409 without another backend query.
The admitted lease and query heap remain charged through buffered output
retirement. Encoding checkpoints now use the same membership-aware borrowed
execution as backend work.

Fifteen owner/handler tests and 16 signed-RPC tests pass without leaks. Handler
coverage includes membership I/O and control progress, snapshot scratch, retained
output, duplicate/terminal replay, altered input, unbound routing overrides,
unsupported routes, wrong credential kind, expired ingress/admission, unavailable
membership and insufficient signed memory. Separate pooled/fallback checks prove
bounded authority scratch and retained-input accounting. The unchanged RPC binary
passes with localhost access after two sandbox TCP failures. Final-source hashes
and logs are in `internal-query-signed-ingress-main-2c8240-oct14-application.json`.

This completes the signed ingress adapter for buffered group-local queries.
Outbound distributed query adoption and capability activation remain unfinished;
production targets still do not advertise owned reads. Complete physical storage
snapshot/cursor and remote child adapters, cyclic/prior-process recovery, private
operator bounds and the other full-design gaps remain open. Full qualification
remains waived.


### Owned group-query client and signed routing envelope (2026-10-14)

An API client adapter now sends buffered group queries through retained remote
attempt dispatch. The signed body contains an explicit version-1 envelope with
the query JSON, prepared-routing metadata and catalog route fence. Ingress decodes
this envelope with the admitted query allocator; unsigned routing headers remain
rejected. Prepared routing requires a fence and preserves the coordinator's
selected index through the routed table callback. Unknown/missing versions,
duplicate/unknown fields and empty queries fail closed. Legacy query bodies keep
their existing shape on the legacy path.

The adapter requires a trusted retained parent and an explicitly activated target.
Publication transfers that parent into the bounded outbox. Lost replies, non-200
responses and missing/invalid read-generation headers return uncertainty with the
original attempt identity. Even successful delivery does not release remote
credits: retirement still requires independently verified terminal/fence evidence.
Dispatch takes the earliest deadline from the retained parent, HTTP cancellation
and caller; an expired parent cannot publish and a later HTTP deadline cannot
extend it. Read buffers use the caller's allocator; control remains independently
accounted by the supplied protocol client. No legacy retry/fallback is introduced.

Response retirement now travels with moved query buffers from HTTP through the
client and native table QueryResponse. Buffers are freed before releasing their
physical owner. This also fixes existing public/group/graph client buffer moves
which previously could release that owner too early. Responses copied into a
different caller allocator can retire their original owner after the copy.

Nineteen owner/client/handler tests, 143 query-contract tests and 16 signed-RPC
tests pass without leaks. Coverage includes body-bound prepared index/fence
propagation, uncertainty across success/loss/HTTP or decode errors, parent deadline
composition, rejection before publication, and exact response-owner retirement.
The unchanged RPC binary passes with localhost access after two sandbox TCP
failures. Final-source hashes and logs are recorded in
`internal-query-owned-client-main-2c8240-oct14-application.json`.

The production distributed query router still needs trusted retained-parent
authority carried through its request contexts before using this adapter.
Production capability advertisement remains disabled. Complete storage snapshot,
cursor and remote-child adoption, cyclic/prior-process recovery, peer refresh,
private operator bounds and the other full-design requirements remain unfinished.
Full qualification remains waived.


### Retained authority through native buffered query routing (2026-10-14)

RequestContext and SearchRequest now carry a trusted native dispatch capability
for buffered group queries. Planning preserves it; query serialization excludes
it. The HTTP routing wrapper resolves the destination's catalog fence before
invoking the capability and clearing it from the transport request. Plain buffered
and streaming transports reject unconsumed capabilities before sending or changing
not-sent evidence. Vector-worker dispatch through this capability stays closed.

A query owner can bind a trusted protocol client. The buffered group-query handler
installs it only when owned state is recovered and at least one target has been
explicitly activated. Production targets remain unactivated. The callback matches
an unambiguous enrolled endpoint, signs the body-bound query/routing envelope,
and moves a parent hold into the outbox. Local holds retain the heap, scheduler
entry and node pin; inbound holds additionally retain the worker lease. Parent
cancellation and the fixed deadline propagate to reconciliation without releasing
uncertain credits. The child-publication marker is atomic for independent workers.

The byte charge is a fixed peer share (configured peer bytes divided by peer
attempt capacity). Idle-credit borrowing is not implemented here. A finite original
parent deadline is required. Oversized envelopes fail before publication. A lost,
non-200 or malformed reply remains uncertain; successful HTTP delivery also keeps
the outbox charge until independently verified retirement. Validated signed route
fences receive the normal acknowledgement. Absolute query deadlines now reach
transport cancellation metadata even when no cancellation token is present.
RemoteOutcomeUncertain has an append-only unavailable ABI detail.

Twenty-two owner/routing/handler tests, 143 query-contract tests, 32 error-ABI tests
and 16 signed-RPC tests pass without leaks. The owner tests cover successful, lost
and malformed replies for local and inbound parents, retain charges/heap after
root retirement, and release them only after modeled verified terminal evidence.
Routing checks prove prepared fence propagation, serialization exclusion, shorter
deadline preservation and rejection before unowned buffered/streaming sends.
The unchanged RPC binary passes with localhost access after two sandbox TCP
failures. Final-source evidence is recorded in
`internal-query-retained-routing-main-2c8240-oct14-application.json`.

This wires buffered internal group-query roots. Public query/SQL roots and other
remote RPC families still need physical ownership adoption. Complete storage
snapshot/cursor adapters, cyclic/prior-process recovery, peer refresh, private
operator bounds and the other full-design requirements remain unfinished.
Capability activation remains disabled; full qualification remains waived.


### Retained public HTTP query owners (2026-10-14)

Public table/global HTTP queries now allocate their response owner from the stable
server allocator and pin the runnable node. The owner can be retained by remote
attempts after HTTP context retirement. Retirement latches an owned cancellation
signal and detaches the borrowed listener callback under a mutex; reconciliation
never reads a destroyed HTTP context. The original execution and node pin retire
only after the final remote parent reference. Admission metadata accounts for the
actual enlarged owner without reducing the existing reservation floor.

Internal and public owners share one retained dispatch adapter for routing,
fixed peer byte shares, deadline composition, signed envelopes and uncertainty.
Ordinary public single-query and NDJSON planning preserve the trusted capability.
HTTP ingress binds it only when recovered owned state has an explicitly activated
target; production capability activation remains disabled. Legacy native callers
keep their existing entry points. Query allocations still use the existing global
request envelope; full per-query public heap/private operator adoption is open.

The public error chain preserves remote uncertainty as retryable unavailability
(503), without locally reexecuting the backend query. Two byte-only result
adapters now copy JSON into the caller-owned allocator before releasing a physical
result owner; returning only its bytes must not drop its retirement callback.
NDJSON retains its existing per-line authorization and result assembly.

Twenty-three core/public-planning tests, three HTTP response-owner tests, 143
query-contract tests and 16 signed-RPC tests pass. The HTTP lifetime test destroys
its context while a remote reply stays uncertain, checks that old cancellation
callbacks are detached, verifies the retained node cannot deinitialize, and
releases the original execution/node only after modeled verified terminal evidence.
The public planning test checks single/NDJSON capability propagation, one backend
call per result, 503 uncertainty and exactly-once result-owner retirement.
The unchanged RPC binary passes with localhost access after two sandbox TCP
failures. Final-source evidence is in
`public-query-retained-owner-main-2c8240-oct14-application.json`.

The public query test compiles the execution path and now has a 14 GiB compiler
allowance (serial build budget 16 GiB); an earlier annotation check observed an
11.60 GB peak. This is scoped native validation, not full qualification.
SQL/non-HTTP roots, joins/composed queries and other remote RPC families still
need adoption. Full storage snapshot/cursor ownership, cyclic/prior-process
recovery, peer refresh, private operator bounds and the remaining design gaps are
unfinished. Capability activation stays disabled; full qualification stays waived.


### Composed and retrieval execution propagation (2026-10-14)

Admitted global queries now recognize source-bearing composition, reuse the
original execution and retain its deadline and trusted remote dispatch capability.
Saved-source catalog reads and each leaf catalog/snapshot context use that same
execution. Retrieval planning and reconstructed retrieval runner contexts preserve
the capability for owned and borrowed executions. Cursor artifact adapters check
the original execution before and after their calls and free returned bytes if
the final check rejects them.

Twenty-five owner/routing tests and thirteen composition contract tests pass with
no leaks. Direct and saved-source ingress fixtures verify one execution, bounded
deadline and capability through the first leaf snapshot, where modeled remote
uncertainty returns 503. They do not prove successful physical storage or cursor
ownership. Composition contracts cover ordering, aggregation, overlay identities,
continuation, exact exhaustion and bounded candidate windows. A first composition
run correctly rejected zero selected tests; an explicit discovery import fixes
that selection. The only change after the core pass is this test-only import.
Evidence: `composed-query-retained-owner-main-2c8240-oct14-application.json`.

Complete physical snapshot/cursor retention and CPU loans across artifact I/O
remain open, along with SQL/non-HTTP roots, other remote RPC families, recovery,
peer refresh, private operator bounds, public per-query heaps and the remaining
full-design requirements. Owned-read capability activation remains disabled;
full qualification remains waived.


### Composed cursor provider reads (2026-10-14)

Composed cursor loading now uses the request-owned verified artifact read context.
Only provider reads park the original borrowed execution; checksum verification
and cursor JSON decoding run after resume. Original deadline and lifetime checks
remain active during the wait. Cursor parsers now release their JSON parse trees
and return independently caller-owned payload bytes.

A reopened filesystem cursor regression exposed a shared artifact error-path bug:
a failed resume attempted a CPU checkpoint while still parked and masked the
original error. Verified bounded/stat/range read error paths now check lifetime
without attempting CPU work. Acquired buffers are freed before failure returns.
The regression uses the leak-checking allocator through successful and rejected
resumes. All 14 composition/cursor tests and 25 owner/routing tests pass. The
artifact-read target passes its 32 native and 13 local tests (including discovery
anchors), covering actual scheduler CPU release, control progress, cancellation,
expiry, closure, corrupt results and provider errors. Final source hashes match
all three receipts; see `composed-cursor-read-wait-main-2c8240-oct14-application.json`.

This does not complete artifact I/O adoption: uploads and store opening still
need provider-level wait boundaries. Full snapshot/cursor resource retention,
other RPC and SQL roots, recovery, peer refresh, private operator bounds, public
per-query heaps and the remaining full design requirements remain unfinished.
Owned-read activation stays disabled; full qualification stays waived.


### Composed cursor upload waits (2026-10-14)

Request-local cursor artifact stores now borrow the original query deadline,
lifetime, CPU checkpoint and wait boundary. Durable filesystem discovery
registration and object-provider puts release runnable capacity while retaining
the original execution. Content hashing, serialization, identity checks and
result publication remain outside those waits. Conditional put conflicts verify
the existing object with separate metadata/range provider waits; digest updates
run after resume. Typed cancellation preserves deadline errors through the
provider's boolean transport callback.

The cursor fixture saves and restores through a reopened filesystem store and
rejects failed resume without leaking. The provider fixture checks parked PUT,
HEAD and GET calls, verifies a conditional retry, and rejects resume after actual
publication while preserving the durable object for subsequent authenticated
retry. Fifteen composition/cursor/provider tests and 25 owner/routing tests pass.
The artifact-read target also passes 32 native and 13 local tests (including
discovery anchors). All three receipts match final source hashes and report zero
leaks; evidence is in `composed-cursor-upload-main-2c8240-oct14-application.json`.

Store opening and native snapshot publication still need provider wait adoption.
Complete snapshot/cursor retention, remaining SQL/RPC roots, recovery after
publication, peer refresh, private operator bounds, public per-query heaps and
the remaining full design requirements are unfinished. Owned-read activation
stays disabled; full qualification stays waived.


### Request-owned store opening and snapshot descriptors (2026-10-14)

Composed leaf snapshots and cursor save/load now open native artifact stores
with the original deadline, lifetime, checkpoint and wait context. Filesystem
provider initialization and bucket discovery/provisioning relinquish runnable
capacity. Failed resume destroys an acquired filesystem client. Configured S3
and GCS constructors receive the same context; existing create-if-missing and
require-existing authority remains enforced. Artifact-wrapper bucket validation
also uses the context. The opened request-local store retains it for uploads.

Native snapshot owner-discovery registration now prepares JSON before the
provider wait and resumes before accepting success or a conditional conflict.
Descriptor publication uses the already-bound store upload context. A filesystem
regression publishes and reloads a native descriptor and checks its exact range
namespace. It covers descriptor publication, not complete remote physical
snapshot generation ownership. Provider initialization remains synchronous;
cooperative interruption inside that operation is separate unfinished work.

Five named store-opening contracts pass (six raw tests including discovery),
including real scheduler control progress while the query is parked and retained
bucket state after rejected resume. Existing filesystem/GCS/S3/borrowed-client
allocation-failure cleanup contracts pass. Fifteen composition/cursor/provider
contracts, 25 owner/routing tests, and 32 native plus 13 local artifact tests
(including discovery anchors) pass without leaks on identical final source.
Evidence: `composed-store-snapshot-wait-main-2c8240-oct14-application.json`.
No live cloud-provider integration is claimed.

Complete physical snapshot/cursor retention and remaining SQL/RPC root adoption,
recovery after publication, peer refresh, private operator bounds, public
per-query heaps and the other full design requirements remain unfinished.
Owned-read activation stays disabled; full qualification stays waived.


### Physical retained-read admission lifetime (2026-10-14)

Retained read registry slots and per-principal counts now stay occupied until
snapshot/cursor/fence destruction returns. Revocation marks a single retiring
owner under the lock, invokes mandatory cleanup outside it, and frees the slot
only afterward. Concurrent/reentrant expiry, close and disconnect skip resources
already retiring, including cancellation pointers the destructor may have freed.
Shutdown closes admission before cancellation and cleanup; its existing
stop-ingress/join-request precondition remains required.

Capture, immutable snapshot and cursor factories now register an uninitialized
cancellation capsule and take an exclusive registry borrow before native backend
creation. Initialization fills optional physical handles while that borrow pins
the capsule. Publication validates lifetime, and failure/null/revocation closes
the reservation and initialized handles before capacity becomes reusable. The
small capsule allocation precedes insertion; native backend work never does.

Twelve registry/owner/RPC/client tests pass without leaks. New tests hold a real
concurrent destructor open, reject slot reuse until it returns, exercise every
cleanup entry point with and without active borrowers, free cancellation storage
before reentrant revocation, and reject admission during shutdown callbacks.
Factory tests observe full principal capacity inside native callbacks, verify
saturated requests never invoke them, and verify failed/missing/revoked creation
returns capacity. Exact typed-page and ambiguous-replay contracts remain green.
Evidence: `retained-read-physical-retirement-main-2c8240-oct14-application.json`.

This fixes physical count admission and destruction ordering. Complete retained
byte accounting and scheduler/signed remote-parent adoption for these resources,
full physical generation lifetime proof, remaining SQL/RPC families, recovery,
peer refresh, private operator bounds and other design requirements remain open.
Owned-read activation stays disabled; full qualification stays waived.


### Retained-read allocator provenance (2026-10-14)

The production retained-read runtime used the foreground request allocator.
It now uses the server retained-state allocator, so its registry, cancellation
capsules and API allocations made by snapshot/cursor callbacks charge the
existing bounded persistent-state envelope rather than consuming request
capacity across idle periods. The registry and allocator still live until
server teardown has joined the runtime and destroyed its resources.

Thirteen focused tests pass without leaks. The production-server regression
fills the foreground heap before runtime creation, allocates capture/snapshot/
cursor handles through native callbacks, releases the capture and snapshot,
and reads the delayed cursor. It then fills retained memory, verifies new
captures fail before invoking the backend, reads the existing cursor, and
allocates protected control memory. Physical cleanup returns handle bytes to
baseline, recovery admits another capture, and runtime teardown returns its
registry/owner bytes. The fixture models physical handles with real tracked
allocations; it does not measure all engine-owned pages or DB snapshot pins.
Evidence: `retained-read-state-memory-main-2c8240-oct14-application.json`.

These allocations use the existing global retained-state byte budget. Complete
per-resource/pin accounting, scheduler/signed remote-parent adoption, remaining
SQL/RPC paths, physical generation lifetime, recovery, peer refresh, private
operator bounds and the other full design requirements remain unfinished.
Owned-read activation stays disabled; full qualification stays waived.


### Retained-read RPC execution admission (2026-10-14)

Retained-read HTTP work now runs on admitted SQL workers with an execution
deadline anchored before handler authentication/parsing and worker queuing.
Ordinary operations use query capacity; close/cancel use protected workers, CPU
and allocation capacity. Small cleanup requests qualify through fixed-stack
bounded body classification for existing protected HTTP transport and service
credential verification. Other operations cannot claim that reserve. Service
identity and catalog scope checks remain mandatory.

The admitted owner retains encoded response bytes and the original operation
while output is parked; response retirement frees bytes before releasing the
execution. RPC context checks fence native entry and response encoding, and
admit point probes receive original cancellation/deadline. Cancellation after a
cursor page advances poisons that cursor, preserving the existing no-replay
contract. Resource leases remain independently retained in the registry.

Eighteen retained-read tests, 25 internal query owner tests and 16 signed-wire
tests pass without leaks. The authenticated handler test verifies close under
simultaneous foreground heap and query-slot saturation; other tests verify
bounded admission failure before backend entry, output retention, malformed
control exclusion and canceled-page retirement. Two sandbox TCP tests pass when
the exact unchanged wire binary receives localhost access. All receipts match
final source; see `retained-read-scheduler-main-2c8240-oct14-application.json`.

Legacy capture/cursor callbacks still hold coarse admission: per-call I/O loans
and complete original context propagation inside those callbacks remain open.
Request parsing is separately bounded before worker admission. These RPCs still
lack signed remote-attempt parent/lease adoption and are not owned-read routes.
Full physical pin accounting, other SQL/RPC paths, recovery, peer refresh,
private operator bounds and remaining design requirements remain unfinished.
Owned-read activation stays disabled; full qualification stays waived.


### Retained relational page admission (2026-10-14)

Retained-read page RPCs now lend their original execution context to the native
relational reader through a checked optional view callback. The reader scopes
that borrow to the synchronous page call, checks row/condition progress and
passes it to physical cursor and point reads. Storage I/O releases CPU through
the existing execution-wait boundary; CPU must be reacquired before rows return.
Failed or canceled page calls discard output and poison the retained cursor.
The borrow clears on every exit; the next call uses its own execution context.
The private API kernel ABI is version 33 for the appended view callback.

The cold native test writes and flushes real relational rows, reopens storage,
verifies protected control progress during physical I/O, rejects failed resume
and continues a healthy cursor under a second execution after poisoning the
first caller's test context. Nineteen retained-read tests pass, including checked
callback forwarding; 23 lower-level storage tests and 16 local plus five API
read-policy tests pass without skips or leaks. The production semantic compile
check passes all 45 steps. The native page suite also includes two discovery
anchors. Receipts match current source; see
`retained-cursor-page-admission-main-2c8240-oct14-application.json`.

The checked dispatcher test does not prove separately compiled archive
integration. Legacy document/other view callbacks retain coarse admission.
Capture/snapshot creation, normalize/range context propagation, signed
retained-read parent adoption, full physical pin accounting, other SQL/RPC
roots, recovery, peer refresh and private operator bounds remain unfinished.
Owned-read activation stays disabled; full qualification stays waived.


### Retained cursor creation admission and native provider adoption (2026-10-14)

Fence and immutable-snapshot cursor opens now accept an optional checked
per-call admission callback. Retained RPC opens pass the original execution's
context separately from ScanOptions: the latter still contains only the
registry-owned cancellation capsule and resource lease. The creation borrow
passes through routed owner wrappers into native relational session planning
and never enters the returned reader. Direct metadata reads and bounded
cardinality cursors use physical admission; cancellation before publication
closes any returned native view and releases the pending registry slot.

Inspection also found that the storage-owner provider still exposed the legacy
page callback after the preceding API adapter changes. Both ordinary and pinned
relational provider views now expose the admitted page callback. API kernel ABI
34 and storage owner ABI 91 invalidate older callback layouts.

Two named cold-storage tests pass, plus two discovery anchors. Creation reads
a flushed range metadata record, allows protected control work during I/O,
unwinds rejected resume and verifies a returned reader never calls a poisoned
opening context. Twenty retained-read tests pass, including required admitted
open dispatch and both fence/snapshot rejection cleanup with successful
subsequent admission. Sixteen local plus five API read-policy tests pass. A new
45-step semantic check analyzes actual storage provider creation, snapshot and
page callbacks without linking a storage binary. It is not an executed archive
integration test. Receipts match current source; see
`retained-cursor-creation-admission-main-2c8240-oct14-application.json`.

Index readiness/ownership metadata, range-proof capture and publication locks
still hold coarse CPU admission. Initial capture/snapshot callbacks, document
views, normalize/range context propagation and signed retained parent adoption
remain incomplete. Full physical pin accounting, other SQL/RPC roots, recovery,
peer refresh, private operator bounds and other full design requirements remain
unfinished. Owned-read activation stays disabled; full qualification stays waived.


### Retained document page admission (2026-10-14)

Native document sessions now scope original execution admission to each page.
Owner-prefix traversal uses admitted physical cursor operations, including
artifact-only owners; timestamp/TTL point reads use the same port. Per-row and
condition checks observe the caller's cancellation/deadline. Failed page calls
discard output, poison continuation and clear the borrow after physical cursor
cleanup. Successful later calls use independent contexts. Session creation also
accepts a separate synchronous admission borrow, never stored with its retained
resource lease or cancellation capsule. API and storage-owner document views
now expose the existing admitted page callback; no ABI layout changed.

Three named native cursor tests pass plus two discovery anchors. The new cold
document test verifies control progress during physical reads, skips an orphan
owner, checks timestamp and document values, and observes original snapshot
contents after a concurrent write before a subsequent page. Poisoned prior-call
contexts are never called; rejected CPU resume returns no page and makes the
session reject continuation. The test follows valid short/time-bounded pages
rather than requiring a full first page. Twenty retained-read tests and 23
lower-level storage tests pass without skips or leaks. Both production API and
storage-provider semantic checks pass all 45 steps. Receipts match final source;
see `retained-document-admission-main-2c8240-oct14-application.json`. These compile
checks do not prove executed separately compiled archive integration.

Publication locks, range-proof capture and index readiness/ownership metadata
still hold coarse CPU admission. Initial capture/snapshot callbacks, normalize/
range propagation and signed retained-parent adoption remain incomplete. Full
physical pin accounting, remaining SQL/RPC roots, recovery, peer refresh, private
operator bounds and other full design requirements remain unfinished. Owned-read
activation stays disabled; full qualification stays waived.


### Retained metadata physical admission (2026-10-14)

Range-proof capture and index ownership/readiness now use a scoped metadata
transaction adapter during retained cursor creation. The adapter lends original
admission to physical point reads and copies returned values into a temporary
arena so later backend gets cannot invalidate earlier metadata. It neither
retains the caller's context in the cursor nor changes transaction identity.
The same range-generation and index-readiness algorithms operate on the adapter;
no alternate proof format, ownership hash or readiness rule was introduced.
Document creation and relational primary/exact-index range capture use it, as
do explicit and automatic relational index planning. No ABI layout changed.

Five named native tests pass plus two discovery anchors. Cold native tests cover
primary generations, exact index spans and index readiness, protected control
progress during I/O, and failed CPU resume. A primary proof remains at its
original generation after a concurrent update. A reused-buffer fixture verifies
stable metadata values and cancellation after partial proof capture with no
output or leaks. Twenty retained-read tests pass; both production API and native
provider semantic checks pass all 45 steps. The first production checks caught
a DB-local module alias mismatch, now fixed; failed receipts remain preserved.
Final-source evidence is recorded in
`retained-metadata-admission-main-2c8240-oct14-application.json`. Compile checks
do not prove executed separately compiled archive integration.

Publication locks and initial capture/snapshot callbacks still need complete
original context propagation and physical wait treatment. Normalize/range RPC
context and signed retained-parent adoption remain incomplete. Full physical
pin accounting, remaining SQL/RPC roots, recovery, peer refresh, private
operator bounds and other full design requirements remain unfinished. Owned-read
activation stays disabled; full qualification stays waived.


### Capture context and publication-lock waiting (2026-10-14)

Retained capture and immutable-snapshot RPCs now pass a separate synchronous
admission borrow through the table router, kernel-owner wrapper and native
storage provider. The returned resources retain only their independently owned
registry cancellation capsules and leases. Late rejection destroys the returned
fence/snapshot before its pending admission slot is retired. Native capture
remains fail-fast when mutation admission or unresolved intents make it busy;
it does not wait for transaction resolution under a capture fence.

The intent probe captures its read transaction under CPU admission, then lends
CPU only during admitted cursor I/O. The test initially hit the existing
retained-capture ExecutionWaitUnsupported guard; that guard remains intact.
Complete policy/import dependency acquisition still uses coarse CPU admission.
Publication read-lock acquisition for admitted snapshot/cursor creation now
retries with bounded progress waits. Each wait releases CPU while retaining the
original operation and restores the original phase before rechecking lifetime.
Cancellation after acquiring a shared lock releases it before returning. Worker
Io is borrowed only through the synchronous RPC call. API ABI 35 and storage
owner ABI 92 reject older callback layouts.

Seven named native tests pass plus two discovery anchors. New cases cover
publication-lock success, cancellation, rejected resume and cancellation just
after lock acquisition, with control progress and no leaked lock ownership.
Cold native probes preserve true/false intent uncertainty and reject failed
resume. Twenty-one retained-read tests pass, including actual per-call progress
waits, separate request/resource cancellation, rejected capture/snapshot cleanup
and successful reuse of admission. Sixteen local plus five API policy tests
pass, and both production semantic checks pass all 45 steps. Receipts match
final source; see `retained-capture-admission-main-2c8240-oct14-application.json`.
These compile checks do not prove executed separate-archive integration.

Retained dependency capture, post-lock portable-runtime metadata reads and
nested route/quorum validation still require further original-context and wait
adoption. Normalize/range RPC context and signed retained-parent adoption remain
incomplete. Full physical pin accounting, remaining SQL/RPC roots, recovery,
peer refresh, private operator bounds and other full design requirements remain
unfinished. Owned-read activation stays disabled; full qualification stays waived.


### Normalization and retained proof response admission (2026-10-14)

Retained normalization and range-proof RPCs now lend original admission through
the checked view boundary, routed-owner wrappers and both native providers.
Document and relational normalizers check the current caller between rows and
before returning; retained session cancellation and deadlines stay independent.
Range-proof retrieval checks before copying and before publication. Every borrow
clears on success or failure. These pure operations neither advance nor poison
the page cursor when the caller is rejected. API ABI 36 and storage owner ABI 93
cover the added optional callbacks.

Document normalization previously leaked partial rows and row-local JSON-null
names on errors. It now tracks initialized output, keeps borrowed null-name
scratch separate from owned output, and frees keys, JSON bytes, null names and
the outer slice on rejection. A dedicated normalized-output destructor also
discards fully returned output if the caller fails the view's final checkpoint.

Eight named native tests pass plus two discovery anchors. The normalization
fixture exhausts allocation failures for both native normalizers, preserves
JSON-null provenance, cancels between rows and after all rows, rejects late
proof-copy publication, and reuses sessions under fresh callers after poisoning
prior test contexts. The document fixture supplies its declared SQL JSON column
contract directly; it does not test public-validator column derivation. The
first relational fixture omitted JSON-column flags and failed InvalidSchema;
that fixture was corrected and the failed receipt preserved. Twenty-one
retained-read tests pass, including checked normalize/proof callback dispatch,
late rejection cleanup and unchanged page sequence. Sixteen local plus five API
policy tests pass. Both production semantic checks pass all 45 steps. Receipts
match final source; see
`retained-normalization-admission-main-2c8240-oct14-application.json`. Compile
checks do not prove executed separately compiled archive integration.

Nested route/quorum validation and protected dependency capture still require
original-context and physical-wait adoption. Signed retained-parent adoption,
full physical pin accounting, remaining SQL/RPC roots, recovery, peer refresh,
private operator bounds and other full design requirements remain unfinished.
Owned-read activation stays disabled; full qualification stays waived.

The remote retained-read client view still exposes legacy callbacks; per-call
transport admission adoption remains separate outstanding work.


### Remote retained-client call admission (2026-10-14)

Remote retained-read views now expose per-call admission for pages, proof
responses and normalization. Existing fence/snapshot handles also expose
admitted snapshot and delayed cursor creation callbacks. Client objects retain
no caller port: synchronous HTTP requests borrow its cancellation and physical
I/O wait boundary, while encoding and decoding keep CPU admission. The original
caller deadline is combined with the resource deadline and the remaining HTTP
budget is computed after serialization. Response publication checks original
lifetime and CPU admission; rejected pages cannot be replayed.

The shared read port now has separately checked lifetime and deadline callbacks.
Lifetime polling is valid while CPU is parked. An incomplete port that can
release CPU but has no lifetime callback fails before transport publication.
The cancellation bridge atomically preserves its first original error, so
transport translation cannot replace deadline/cancellation/shutdown identity.
API ABI 37 and storage owner ABI 94 cover the protocol change; native runtime
ABI stays 8 and the port verifies its vtable layout contract before field access.

Twenty-two retained-read tests pass. The new transport-stage fixture uses a real
scheduler and proves control progress, non-CPU lifetime polling while parked,
shorter deadline/header forwarding, cancellation/expiry/resume rejection, no
replay, fresh execution continuation and pre-transport rejection of an incomplete
port. It is not live-network or separate-archive integration evidence. Eight
native named tests plus two anchors, 23 storage tests, 16 local plus five API
policy tests and both 45-step production semantic checks pass. Final-source
receipts are in `retained-client-admission-main-2c8240-oct14-application.json`.
The two initial test/compile errors remain preserved with their receipts.

Initial coordinator admit/capture and legacy validate callbacks remain on the
prior transport contract; legacy callers still need per-call port adoption.
Nested route/quorum validation, protected dependency capture, signed retained
parent adoption, full physical pin accounting, remaining SQL/RPC roots, recovery,
peer refresh, private operator bounds and other full design requirements remain
unfinished. Owned-read activation stays disabled; full qualification stays waived.


### SQL cursor page and normalization admission (2026-10-14)

The SQL native cursor adapter now lends current execution admission to each
view page call. It reuses SQL's existing read-fence-aware park/resume hooks, so
physical native or remote waits can release CPU and revalidate authority before
returning rows. The borrow is stack-scoped to the pull; projection work checks
admission between rows and before publication. Failed pulls become terminal,
preventing a retry from silently skipping an advanced page. SQL normalization
also uses the admitted callbacks. Non-CPU lifetime checks observe the active
read or write execution; protected cleanup retains its independent authority.

Inspection of pgwire OwnedRead confirms its original runnable is initialized
at its final address and parked/entered between fetches. The new port follows
that existing owner and does not retain a per-call stack bridge in the cursor.
The focused new cursor test checks control progress, parked lifetime polling,
progress waits, independent subsequent execution, cancellation, expiry, rejected
resume and late rejection. Its empty read guard isolates execution mechanics;
separate authority regressions cover live policy/catalog checks. Two startup
tests plus that cursor test pass, as do three local/four native authority tests
and all 22 retained-read tests.

The production SQL ingress check exposed an existing response-copy error-set
mismatch. Copy allocation failure now maps to the existing InternalFailure
contract while the response retirement defer still runs. All 44 semantic compile
steps pass. The native HTTP/pgwire integration test compiled in 18 minutes at
9 GiB peak. Its sandbox run failed Unexpected; the exact unchanged binary passes
with local system/socket access, without skips or leaks. It reports six passing
HTTP/pgwire preparation-fairness cases with zero retained operations/bytes,
164 public array expression cases and 16 internal-array query cases. Both runs
are preserved. Final-source receipts are recorded in
`sql-retained-cursor-admission-main-2c8240-oct14-application.json`. No ABI layout
changed in this step: API ABI remains 37 and storage owner ABI 94.

Initial coordinator admit/capture, grouped proof/validation propagation,
protected dependency capture and SQL lake-I/O paths remain incomplete. Signed
retained-parent authority, full physical pin accounting, other SQL/non-HTTP
roots, recovery, peer refresh, private operator bounds, idle borrowing, managed
metrics and other full design requirements remain unfinished. Owned-read
activation stays disabled; full qualification stays waived.


### Statement capture and proof admission (2026-10-14)

Ordinary SQL statement construction now carries the original execution through
coordinator capture, cursor opening, native proofs and statement proof copies.
Initial remote admit/capture use the checked per-call transport adapter, including
park-safe cancellation and the shorter original deadline. Cohort retry releases
all frozen fences before waiting, without reacquiring a fresh statement read cut
or weakening read-index consistency. CPU-only preparation and validation retain
admission; only actual provider waits may release it. Handles keep owned state,
not borrowed call context. API ABI 38 covers the appended callbacks; storage owner
ABI stays 94.

Two focused consumer tests pass. They cover real scheduler/control progress with
original operation/memory charges, rejection during capture/open/proof waits,
routing/resource cleanup, shared capture across aliases, exact proof contents,
and partial-cohort release when the second owner is busy. This invokes the real
coordinator through checked callbacks with fixture owners. It does not prove live
distributed execution or the native storage archive. Twenty-two retained-read
tests, 21 policy tests and 44 production ingress compile steps also pass. The
application receipt verifies the only subsequent change to those latter inputs
is the two-owner test state reset/assertion, and preserves earlier failed or
interrupted runs: `coordinator-statement-admission-main-2c8240-oct14-application.json`.

Dynamic statement snapshots and delayed guarded opens remain legacy. Nested
route/quorum/read-index/validate paths, protected dependency capture and SQL lake
I/O still need admission. Signed retained-parent authority, complete physical pin
accounting, remaining roots/RPCs, recovery, peer refresh, private operator bounds,
idle borrowing, managed metrics and the other full-design requirements remain
open. Owned-read activation stays disabled; full qualification remains waived.


### Dynamic snapshots retain state while calls borrow execution (2026-10-14)

Dynamic snapshot creation and delayed guarded opens now carry the original SQL
execution into the available Bound/Provisioned callbacks. Initial remote
admit/capture, immutable owner snapshots, cursor opening and proof reads use
per-call admission. Capture retries discard the whole frozen cohort before
waiting. A later call may borrow another execution while retaining the same
snapshot capability; independent resource lifetime and policy checks still apply.
No port is saved in the returned snapshot. SQL activation replacement preserves
the same execution and its existing before-observation restriction. Guarded
publication checks admission after owner-proof validation and frees rejected
views/proofs. API ABI 39 protects the new callback layout; storage ABI stays 94.

Three focused consumer tests pass, including a seven-mode real-scheduler fixture
for capture/snapshot/open/proof waits, protected control progress, 17-byte root
charges, caller-context retirement, cancellation/expiry/resume rejection, and
resource cleanup. A two-owner dynamic retry checks release of an already-held
first fence before waiting. Fixture capability reuse is not native MVCC or live
network proof. Seven SQL authority tests, 21 read policy tests and 44 production
SQL ingress compile steps pass with final source hashes. The failed missing-
incarnation fixture run is preserved; its authenticated proof requirement was
kept intact. Evidence is recorded in
`dynamic-statement-admission-main-2c8240-oct14-application.json`.

Nested route/quorum/read-index/validate adoption, protected dependency capture,
SQL lake I/O, other roots/RPC families, signed parent lifetimes, physical pin
accounting, recovery, peer refresh, private operator bounds, safe idle borrowing,
managed metrics and the rest of the original design remain unfinished. Owned-
read activation stays disabled and full qualification stays waived.


### Owner validation keeps original execution authority (2026-10-14)

The coordinator's final fence validation now borrows the original execution
through a checked callback. Remote validation passes original lifetime/deadline
and physical transport wait controls to the retained client. Worker validation
borrows the existing registry capture and original request team; it creates no
replacement root. Snapshot creation uses the same admitted final validation.
Kernel-owner and native provider callbacks forward/check the port synchronously.
API ABI 40 and storage owner ABI 95 protect the new fence vtable layout.

Route and frozen-proof checks still execute with CPU admission. Their nested
providers need physical wait adoption; the new callback does not authorize
parking an entire coarse validation body. The new worker fixture proves control
progress at a real scheduler wait, retained operation/17-byte charge, deadline
forwarding, cancellation/rejected resumption and physical close of the retained
capture. The remote transport success fixture now covers validation, and the
coordinator fixture tests rejected final validation after captured snapshots.
All 23 retained-read tests, three coordinator tests and 21 policy tests pass.
The 44-step ingress and 45-step native provider semantic checks also pass on the
same source inputs. This does not establish live distributed/archive integration.
Evidence is in `retained-validation-admission-main-2c8240-oct14-application.json`.

Nested route/read-index/frozen-proof physical waits, protected dependency
capture, SQL lake I/O, other roots/RPCs, signed parent authority, full physical
pin accounting, recovery, peer refresh, private operator bounds, idle borrowing,
managed metrics and other original design requirements remain unfinished.
Owned-read activation stays disabled; full qualification stays waived.


### SQL lake providers borrow original execution (2026-10-14)

SQL's lake context now carries original Adapter lifetime, deadlines/cancellation,
read-fence checkpoints and physical waits. Additional checkpoints survive cursor
withExecution adaptation. Contextual native artifact stores and retained-cut
loads preserve the existing capability identity/expiry rules. Catalog/WAL reads,
snapshot pins and their constructors receive the same context. SQL cut binding
checks original admission after token encoding and before session publication.

Physical object calls release CPU only around provider methods and discard owned
results after rejected resumption. After successful resumption even I/O errors
must pass CPU/authority checks before being classified as absence. Encoding,
parsing and validation remain admitted CPU work. Provider cancellation may poll
lifetime while parked; legacy mutation callbacks retain their prior CPU checks.
REST GET/HEAD borrow read waits, while mutation ownership remains on its existing
path. API/storage ABI layouts stay at 40/95.

Six focused tests pass, including 17 real filesystem/scheduler cases, retained
capability reopen/schema/expiry checks, snapshot retirement and three WAL
correctness regressions. They prove protected control progress, retained root
operation/17-byte charge, cancellation/expiry/rejected return, durable retention
pin after rejected PUT and authority rejection after a missing WAL response.
Seven SQL authority tests, 21 policy tests and the 44-step production ingress
semantic check also pass on final inputs. These tests do not establish complete
SQL endpoint transactions or live REST/cloud behavior. Receipts, source hashes,
failed/intermediate runs and interruption audit are recorded in
`sql-lake-admission-main-2c8240-oct14-application.json`.

Route/read-index/frozen-proof physical waits, protected dependencies, remaining
lake mutation/providers, other roots/RPC families, signed parent lifetimes,
physical pin accounting, recovery, peer refresh, private operator bounds, idle
borrowing, managed metrics and the rest of the original design remain unfinished.
Owned-read activation remains disabled; full qualification remains waived.


### Frozen quorum waits release CPU at the owner timer (2026-10-14)

Frozen-proof validation now accepts a checked original admission borrow. The node
provider clamps its request to the original deadline, preserves the 250 ms safety
ceiling and captured applied-index upper bound, and releases CPU only around its
existing one-millisecond owner-Io sleep. Registration, request/proof work and mutex
try-locks stay admitted. It does not wait for frozen apply progress. Original
registration defers retire pending state after rejected return or source failure.

Retained fences and delayed snapshots forward the port, including owner-local
guarded opens/proofs. Legacy validation stays on CPU and uses checked dispatch.
The callback layout is protected by API ABI 41 and storage owner ABI 96. A
production semantic export now reaches the actual DataServer barrier factory and
its callback instead of analyzing only a runtime-supplied interface pointer.

Thirteen Raft/read-gate tests plus one discovery anchor pass. Six new fixture
modes cover real scheduler/control progress, original operation/17-byte retention,
deadline clamping, newer quorum index/changed term rejection, cancellation/expiry/
shutdown, registration cleanup and legacy CPU ownership. This exercises the actual
shared timer primitive and tracker, not a running DataServer quorum loop. All 23
retained-read tests, 21 policy tests, 44 ingress compile steps and 45 node compile
steps pass on matching inputs. Evidence and limits are in
`frozen-quorum-admission-main-2c8240-oct14-application.json`.

Route/read-index nested provider waits, protected dependencies, retained snapshot
authority after later Raft progress/failure, durable lake mutation waits, remaining
roots/RPCs, signed parent authority, physical pins, recovery, peer refresh, private
operator bounds, idle borrowing, managed metrics and other original requirements
remain open. Owned-read activation remains disabled; full qualification stays waived.


### Retained snapshot authority after Raft progress (2026-10-14)

Delayed kernel-owner snapshots previously reran capture-time frozen validation:
any newer quorum index rejected the original immutable cut. The short capture
still keeps that upper bound and all-owner final validation under mutation
fences. A separate checked retained-snapshot callback now verifies the original
Raft incarnation and term, local leadership, a fresh quorum, and no regression
below the captured applied index. Newer commits do not recapture or replace the
native snapshot. Fresh catalog identity checks still reject route/owner changes.

The node reuses its bounded ReadIndex registration loop and owner-Io wait,
preserving cancellation, original admission/deadline clamping, the independent
250 ms ceiling, protected progress and deferred registration cleanup. Missing
provider support falls back to conservative frozen validation. The new callback
layout is protected by API ABI 43 and storage owner ABI 98. The production node
semantic root explicitly instantiates both capture and retained callbacks.

The focused routed target passes two tests plus six discovery anchors. Fourteen
Raft/read-gate tests cover 22 capture/retained cases, including later commits,
term/incarnation/leadership loss, applied-state regression, unavailable quorum,
cancellation, expiry and shutdown. Three coordinator tests, 23 retained-read RPC
tests, 21 policy tests, the 45-step node check and 44-step ingress check also pass.
Three fixture failures are preserved. Auxiliary receipts precede only the fixture
progress stub/cancellation-expectation corrections; exact reverse hashes prove
those differences are confined to the new test. Production inputs are unchanged.

Verification and precise limits are recorded in
`retained-snapshot-authority-main-4838edd-oct14-application.json`. The focused
routed test invokes the actual delayed-open authority method with no native
handle; it does not prove physical row immutability or live multi-node failures.
The Raft fixture uses the actual proof validator, tracker, scheduler and timer,
with simulated quorum delivery. Full qualification remains waived. Broader
snapshot/distributed-failure qualification remains open, along with protected
dependency capture, remaining provider/root/RPC adoption, durable lake mutation
waits, signed parent lifetimes, full physical pin accounting, external recovery,
peer refresh, bounded private operators, idle borrowing, managed metrics and the
other requirements of the original design. Owned-read activation stays disabled.


### Payload-policy and primary dependency capture (2026-10-14)

Budgeted snapshot capture and admitted probe/current-scan capture no longer hold
the payload-policy mutex during backend acquisition. A short policy capture owns
a retained payload-store session, then releases its mutex. After capturing the
primary view, the caller reacquires the policy mutex and checks a process-local,
monotonic generation. Every policy publication advances that generation, including
A -> B -> A transitions. Exhaustion rejects capture instead of wrapping. A changed
policy discards the primary view, payload session and import-reader ownership
before retrying through the original progress capability. Failed acquisition or
late authority rejection retires the same resources. No call port is retained.

Eleven native tests pass (nine named plus two anchors). The new fixture executes
24 cases across snapshot, probe and current-scan captures: stable policy, changed
policy, ABA, backend failure, late authority rejection, canceled retry, generation
exhaustion and import recovery. Backend capture observes the policy mutex free.
The real memory backend's immutable snapshot excludes the first, discarded cut:
a policy change plus intervening write yields only the second capture. Injected
retry waits verify no policy/session/import ownership, protected control progress,
and original operation/17-byte retention. Payload retain/release uses a fixture;
this is not a live vector-migration or physical import-publication qualification.
Twenty-one policy tests, 45 provider compile steps and 44 ingress compile steps
pass on final hashes. The initial fixture type error is preserved in
`payload-policy-capture-main-4838edd-oct14-application.json`.

The import-reader lease still spans backend capture and returned transaction
lifetime. Its publisher's wait protocol is unfinished, so the existing
ExecutionWaitUnsupported guard stays intact. This stage removes a nested policy
mutex dependency; it does not claim complete CPU-wait adoption for native capture.
Unbudgeted legacy paths remain unchanged. Callback layouts stay at API ABI 43 and
storage owner ABI 98. Protected dependency capture, other providers/roots/RPCs,
durable mutation waits, retained-parent authority, full physical pin accounting,
external recovery, peer refresh, private operator bounds, idle borrowing, managed
metrics and the rest of the original design remain unfinished. Owned-read
activation stays disabled; full qualification remains waived.


### Portable import publisher wait admission (2026-10-14)

DocStore publication now has an explicit pre-commit mutation-admission borrow.
The owner sets its exclusion bit before draining readers and lends CPU only
around its original backend-Io sleep. The lifetime callback preserves original
cancellation/deadline/shutdown authority without translating clocks. Repeated
CAS contention checks the same lifetime. A failed wait, rejected resumption,
I/O cancellation or late authority failure atomically clears only this publisher's
bit, preserving every live reader count. A competing publisher never owns that
rollback. Successful acquisition retains the drained fence for publication.
Legacy unscheduled calls retain their existing Io path.

Directory import/prepared adoption and Lite import/prepared publication expose
controlled variants and forward the same acquisition through apply-lock waits,
reader drain and the final check before publication. There are no added caller
admission checks after irreversible adoption: existing metadata refresh, fence
release, activation and durability-unknown reporting keep their completion order.
No borrowed control is stored in a DB or returned resource. Callback layouts
stay at API ABI 43 and storage owner ABI 98.

Twelve native tests pass, including ten publisher cases and the 24 policy-capture
cases from the preceding stage. New cases verify original operation/17-byte
retention, protected control progress, before/after-claim cancellation, deadline
and shutdown rejection, failed begin/resume, I/O cancellation, rejection after
readers drain, coarse CPU retention, competing ownership and later fence reuse.
Twenty-one policy tests, 45 provider steps and 44 ingress steps pass on final
source and external input hashes.

Evidence and exact limits are in
`portable-publication-main-4838edd-oct14-application.json`. The native fixture
uses actual memory-backend read leases, publication atomics and scheduler waits,
with injected lifecycle failures and reader retirement. It does not execute a
physical file adoption or prove complete import durability. The semantic provider
root forces actual directory and Lite controlled call chains. Initial fixture
compilation failure and intermediate runs are preserved.

This supplies the publisher wait protocol; it does not enable native capture CPU
lending. Capture's ExecutionWaitUnsupported guard remains until caller adoption
and the remaining backend/protected dependencies are qualified. Staging decode,
construction, activation and durable completion remain on their coarse paths.
Scheduled import roots must explicitly pass their original mutation controls;
legacy/C API entrypoints currently pass none. Other providers/roots/RPCs, signed
retained-parent authority, complete physical pin accounting, recovery and peer
refresh, bounded private operators, idle borrowing, managed metrics and other
original design requirements remain open. Owned-read activation stays disabled;
full qualification remains waived.


### Native capture checkpoints outside backend ownership (2026-10-14)

Cooperative checkpoints can return CPU even when no physical wait is requested.
The mutex helper now uses a declared non-suspending lifetime check immediately
after acquisition; legacy budgets without that method retain their previous
checkpoint contract. Native snapshot, probe and current-scan acquisition separate
locked capture from publication. Their owned backend sections check only lifetime;
the final cooperative checkpoint runs after unlocking. Rejection there aborts the
complete captured view before returning it. Existing pin and failure cleanup stay
under their original physical ownership.

Read-version preparation uses lifetime checks at its backend-owned entry and
projection publication. Capability discovery now validates the port contract and
inspects wait support without executing a cooperative checkpoint. It grants no
execution authority. Capture-time cursor metadata receives the original control
instead of silently using the legacy preparation path. The existing unsupported
wait guard in read-version preparation remains, as does DocStore's capture guard.
No callback layout changed; API/storage owner versions stay at 43/98.

The new native fixture covers 48 combinations of snapshot/probe/current capture,
mutable/flushed storage, wait capability presence and success/contention/locked
rejection/return rejection. It configures the actual scheduler to yield at every
cooperative checkpoint and rejects checkpoints observed under its backend mutex.
Contended acquisition lends CPU for protected control, retaining the original
operation/17 bytes. Rejected captures leave no active backend readers or lock
ownership. It uses actual LSM operations and MemoryStorage, with deterministic
contention and rejection injection; it does not prove threaded contention, all
backend paths, or complete enabled capture support.

Thirteen native tests, 23 retained-read RPC tests, 21 policy tests, both 45-step
native provider checks and the 44-step ingress check pass. The auxiliary checks
precede only the fixture lifetime callback addition; reversing exactly that method
reproduces their source hashes. All production inputs are unchanged.

Evidence and exact verification scope are recorded in
`capture-lock-checkpoints-main-4838edd-oct14-application.json`. Initial cursor
fixtures lacked a non-suspending lifetime callback despite advertising execution
waits; their corrected bridge checks the original Execution lifetime. The initial
failed run is preserved. Native capture remains guarded until the remaining
protected dependencies, coarse/legacy lock holders and caller adoption are
qualified. Copy/rotation/build work still needs complete non-yielding bounds.
Other roots/providers/RPCs, signed parent lifetimes, full physical pin accounting,
durable completion, recovery, peer refresh, idle borrowing, managed metrics and
other original design requirements remain unfinished. Owned-read activation
stays disabled; full qualification remains waived.


### Run-version build admission without a held service mutex (2026-10-14)

Run-version preparation no longer holds a build mutex across cooperative work.
A backend-mutex-protected preparing flag selects the producer. Callers able to
lend execution coalesce through their original progress capability, with lifetime
checks before and after waiting and mandatory backend reentry on failure. A
canceled waiter does not clear another caller's producer ownership. Legacy and
coarse callers instead build independently reserved candidates; blocking their
CPU behind a suspended producer would reintroduce the dependency cycle.

Projection work remains outside the backend mutex and borrows the original
admission. Publication checks lifetime under the mutex, publishes at most once,
and discards a losing candidate's arrays without destroying the pinned directory.
The caller's epoch reference and backend reader ownership protect the original
cut across newer directory publication. All paths clear owned producer state and
retire temporary reservations. No new root, detached producer or retained call
port is introduced. Legacy races may perform extra metadata work and use extra
temporary memory; the existing per-candidate resource reservation still applies.
The run-version ExecutionWaitUnsupported guard is removed. DocStore's broader
capture guard remains. Callback layouts stay at API/storage owner ABI 43/98.

Eight deterministic native cases cover success, canceled/expired coalesced peer,
legacy and coarse publication winners, publication of a newer epoch, producer
cancellation and allocation failure. They use actual LSM/MemoryStorage,
ResourceManager reservations and scheduler owners, checking old-directory
identity, winner pointer preservation, reader/state cleanup, failed allocation
reservation retirement and original operation/byte retention. A separate two-thread
case exercises real coalescing through one query CPU slot, with both original
operations and 40 retained bytes while parked, one published build, no active
readers and final scheduler retirement. The fixture's initial self-type compile
error and intermediate runs are preserved.

Fifteen native tests, 23 retained-read RPC tests, 21 policy tests, the 45-step
native provider check and 44-step ingress check pass. Auxiliary receipts precede
only the appended two-thread helper/test; stripping those exact additions
reproduces their recorded hashes. All production inputs are unchanged.

Evidence and final-input checks are recorded in
`run-version-admission-main-4838edd-oct14-application.json`. This is focused native
metadata/concurrency verification, not full qualification or physical pin-byte
accounting. DocStore capture eligibility, remaining service dependencies and caller
adoption, non-yielding copy/rotation/operator bounds, other roots/providers/RPCs,
signed retained-parent authority, durable mutation completion, full pin accounting,
external recovery, peer refresh, idle borrowing, managed metrics and other
original design requirements remain unfinished. Owned-read activation stays
disabled; full qualification remains waived.


### Pre-decision snapshot admission avoids CPU capacity cycles (2026-10-14)

Foreground batch admission now explicitly selects a snapshot-acquisition policy
that rejects contention when its original controls cannot lend CPU. Retaining
policy/replication service leases can suppress CPU transfer; waiting for a capture
while occupying that CPU can otherwise prevent the capture's original continuation
from resuming. The new pre-decision helper returns Backpressured in that case,
checks original lifetime before admission and after successful acquisition, and
releases a newly acquired lease on late rejection. Lendable callers keep the
controlled wait, and existing mutation/capture-owned retains bypass reacquisition.

Only batchWithAdmissionControlsAndRootContext selects the new internal option.
The option defaults off for replay, restore, resolution and other mandatory paths;
the existing acquireMutationControlledIo contract remains available. No accepted
completion or post-primary-retirement behavior is changed. The new internal
options layout is protected by API ABI 44 and storage owner ABI 99. Error identity
uses the existing retryable Backpressured status.

The focused scheduler fixture covers eight cases: busy rejection, lendable wait,
original cancellation/expiry/shutdown causes, late-acquisition rejection, retained
capture borrow and unchanged mandatory acquisition. It uses a real capture gate
and one ordinary CPU slot, preserving two original operations/40 bytes during a
wait and proving the original reader can resume after the non-lending writer
returns. The physical bound-batch regression now checks busy rejection under a
held capture, scoped failure before effects, resource cleanup and healthy retry
under the same original owner/cancellation state after releasing the capture.
Sixteen native tests, 23 retained-read RPC tests, 21 policy tests, the 45-step
provider check and 44-step ingress check pass. The focused physical DB regression
also passes with no skips or leaks and proves healthy retry after release. Its
cold compile took about nine minutes and peaked at 6,574,096,384 bytes, slightly
above the initial 6 GiB scheduling declaration. The corrected 7 GiB declaration
passes cleanly with the same cached binary; no implementation code changed.
Auxiliary receipts precede only this scheduling-hint adjustment, verified by
exact reverse hashes. This is not a full durability or restart qualification.

Results and exact limits are recorded in
`snapshot-predecision-main-4838edd-oct14-application.json`.

This closes the foreground batch snapshot-acquisition cycle, not every wait in
the capture/mutation graph. Mandatory completion, other mutation roots, apply
waits under retained holders, DocStore capture eligibility, import reader policy,
non-yielding bounds, remaining providers/RPCs, signed retained-parent authority,
complete physical pin accounting, recovery/peer refresh, idle borrowing, managed
metrics and other original requirements still need completion and qualification.
Owned-read activation stays disabled; full qualification remains waived.


### Pre-decision apply admission avoids CPU capacity cycles (2026-10-14)

Controlled foreground batches, BEGIN and unreplicated intent preparation now
reject apply contention with retryable Backpressured when their original controls
cannot lend CPU. Snapshot, policy or replication leases may suppress CPU transfer;
a writer must not occupy a suspended apply holder's only resume capacity while
waiting for that holder. lockApplyPreDecision checks the original lifetime before
admission, again on busy rejection and after acquisition. Late lifetime or portable
activation failure releases the newly acquired apply lock. Actual try-admission
attempts retain exclusive call/contended metrics without inventing a wait duration.

The batch option is now named avoid_admission_capacity_cycle and covers both
snapshot and apply acquisition. Only the controlled foreground batch entrypoint
sets it. Controlled BEGIN and unreplicated prepare select the pre-decision helper
explicitly. Lendable acquisition, legacy calls, replicated prepare, replay,
restore and mandatory resolution retain their existing acquisition/completion
policy. API ABI 45 and storage owner ABI 100 protect the changed private options
contract. No deadline, root, cancellation authority or completion owner is renewed.

A real scheduler with one ordinary slot and an actual ApplyRwLock exercises ten
scenarios: busy rejection, lendable wait, injected original cancellation/expiry/
shutdown errors, late lifetime rejection, late activation closure, cancellation
racing busy admission, unchanged mandatory acquisition and legacy acquisition.
It retains two original operations and 40 bytes during the controlled wait,
permits protected control progress, lets the original reader resume after writer
retirement, and retires all roots/bytes and lock waiters. The mandatory fixture
simulates external holder progress; it is not proof of the entire mandatory
completion dependency graph.

All 17 focused native admission tests and three selected physical DB tests pass.
The physical tests cover snapshot-held foreground batch admission and apply-held
batch/BEGIN/prepare admission, original deadline rejection, unchanged stored state,
released policy/snapshot leases, healthy retries after release, and mandatory abort
of the prepared intents. They report zero skips, failures or leaks. The physical
cold compile completed in 543 seconds within its 8 GiB reservation. The 23 RPC and
21 policy tests, 45-step provider check and 44-step SQL ingress check also pass.
All six receipts match the exact final source/external/log hashes, without source
normalization. Evidence is recorded in
`apply-predecision-main-c102cbe-oct14-application.json`, based on main c102cbeac5
merged into the existing canonical branch. This is focused native macOS evidence,
not Linux or full durability/restart qualification.

This closes explicit foreground pre-decision apply contention, not the complete
service dependency graph. Mandatory/coarse mutation waits and other roots,
DocStore retained-capture eligibility/import ownership, bounded non-yielding work,
remaining providers/RPCs, durable mutation waits, signed retained-parent authority,
complete physical pin accounting, external recovery and peer refresh, idle
borrowing, managed metrics and other original design requirements remain open.
Owned-read activation stays disabled; full qualification remains waived.


### Pre-decision global mutation admission preserves capture progress (2026-10-14)

Controlled foreground batch, BEGIN and unreplicated prepare now select explicit
pre-decision process-wide mutation/capture admission. When the original controls
cannot lend CPU, contention returns retryable Backpressured before this operation
retains a physical mutation reader. This extends the earlier snapshot/apply policy
to the shared global barrier: retaining policy or snapshot leases must not occupy
a suspended capture holder's only resume capacity.

The runtime adds separate pre-decision helpers for a fresh explicit root, a retired
root's phase, and compatibility nesting. Lendable calls retain their original
controlled wait. Held explicit roots borrow behind closed capture admission rather
than reacquiring their own barrier; child rejection preserves the parent and rolls
back its borrower. Fresh roots remain independent of unrelated same-thread legacy
nesting. Rejected acquisition retires its physical reader without renewing the
root address, barrier identity, deadline or cancellation authority. Rootless legacy
nesting semantics remain unchanged.

Only the foreground batch option selects the new replication helper. BEGIN and
unreplicated prepare select it explicitly. Startup recovery, enclosing reader
reacquisition, mandatory resolution/replay, and null-control legacy calls keep the
existing acquisition policy. This change does not qualify those other waits. The
runtime delta consists only of three helper methods/comments: exact removal
reproduces the previously verified source, with every prior declaration and layout
field unchanged. Existing API ABI 45 and storage owner ABI 100 remain valid.

Eighteen focused admission tests pass, including 22 parameterized global-barrier
cases using a real scheduler and MutationBarrier: rootless and explicit-root
acquisition, original injected cancellation/expiry/shutdown causes, late rejection,
busy cancellation races, parent borrows behind a closed gate, compatibility nesting,
legacy/mandatory acquisition and cleanup. They preserve two original operations
and 40 retained bytes, permit protected control progress and let the original reader
resume after the rejected writer retires. The mandatory case simulates external
holder progress; it is not full mandatory liveness qualification.

The 12 existing mutation-root tests also pass, covering cross-worker release,
writer intent, explicit nesting, phase retirement/reentry, outbox debt and rollback.
Four selected physical DB tests pass with no skips/failures/leaks. The new ordinary
batch test configures the actual shared barrier through a metadata mirror, rejects
capture contention before row effects, releases policy/native leases, preserves the
retired explicit root, and commits successfully through that same root after release.
It intentionally does not exercise metadata publication or external replication.
The physical BEGIN/prepare cases cover apply contention; their new global-barrier
selection uses the same checked helper but is not separately exercised physically.
The cold physical compile/run completed in 494 seconds within its 8 GiB reservation.
The 23 retained-read RPC and 21 policy tests, 45-step provider check and 44-step SQL
ingress check also pass. All seven successful receipts match the exact final source,
external inputs and log hashes; the earlier native preflight is retained separately.
Evidence is recorded in `replication-predecision-main-c102cbe-oct14-application.json`.

This is focused native macOS verification, not Linux or full distributed durability/
restart qualification. Startup/preflight recovery, advisory memory pressure, other
coarse/mandatory service waits and mutation roots, DocStore retained-capture/import
eligibility, remaining providers/RPCs, signed retained-parent authority, full physical
pin accounting, bounded non-yielding work, durable mutation waits, external recovery,
peer refresh, idle borrowing, managed metrics and other original design requirements
remain unfinished. Owned-read activation stays disabled; full qualification remains
waived. The overall design remains in progress.


### Pre-decision pressure admission preserves producer progress (2026-10-14)

The existing controlled foreground batch option now selects explicit advisory
pressure admission before the primary mutation reader. A non-lending caller
returns retryable Backpressured for hard throttle pressure or parent/child ledger
contention, retaining the original lifetime cause and leaving another producer's
charges untouched. Normal pressure remains immediately admissible. Non-lending
soft pressure skips advisory pacing and proceeds to the unchanged allocation/WAL
hard guard; it does not grant an allocation or bypass a quota. Impossible projected
footprints and explicit reject_work retain ResourceBudgetExceeded.

The new IntentCollection binding advertises only the original permitted physical
wait boundary and provides a non-yielding lifetime callback. An explicitly retired
root may preserve lending; a held root suppresses it. Lendable hard pressure waits
and soft ten-tick pacing use the original owner Io, CPU continuation and authority.
No new reservation, root, deadline, cancellation or allocator authority is created.
The generic pressure helper, existing binding, mandatory/replay/resolution and
null-control legacy behavior keep their prior policy. Only the explicit foreground
batch option selects this change. ABI source/layout declarations and private
IntentCollection fields are unchanged; API ABI 45 and storage owner ABI 100 remain
valid.

Nineteen focused native admission tests pass. The new test exercises 26 real
ResourceManager/scheduler scenarios with held/retired mutation roots: normal, soft
and hard pressure; lendable hard/soft waits; parent/child ledger contention;
injected original cancellation/expiry/shutdown; rejection under the acquired child
ledger; impossible footprints and reject_work. It preserves two original operations
and 40 bytes while parked, permits protected control progress, resumes the same
original producer to retire its ledger/charge, and ends with zero scheduler and
parent/child ledger ownership. A real reservation over the slice hard limit still
fails after non-lending soft admission. The original 114 service-lock regressions
also pass, including generic pressure and mutation admission behavior.

Five selected physical DB tests pass with zero skips/failures/leaks. The added
case reports another producer's resident state through the actual manager: hard
pressure rejects before row effects, preserves that charge and releases native
policy/snapshot scopes; soft pressure permits a small write within the hard guard;
the same healthy original controls retry successfully after pressure clears. These
are modeled producer ledger reports, not proof of complete physical allocation
accounting. The cold physical compile/run completed in 524 seconds under its
8 GiB reservation without a memory warning. The 23 retained-read RPC and 21 policy
tests, 45-step provider check and 44-step SQL ingress check also pass. Seven final
receipts match exact source, external input and log hashes. Evidence and unchanged
ABI/field checks are recorded in
`pressure-predecision-main-c102cbe-oct14-application.json`.

Startup recovery remains a separate dependency problem. Returning Backpressured
for every pending startup flag would be insufficient: the fenced startup scan
clears that flag, whereas the dedicated background retry owner's drain calls the
outbox flush and does not perform that transition. Its recovery/CPU ownership must
be qualified before changing this protocol; this increment leaves it unchanged.

This is focused native macOS verification, not Linux or full durability/restart
qualification. Earlier preparation/accounting contention, startup/preflight
recovery, other mandatory/coarse service waits and mutation roots, DocStore
retained-capture/import eligibility, remaining providers/RPCs, signed retained-parent
authority, complete physical pin accounting, bounded non-yielding work, durable
mutation waits, external recovery, peer refresh, idle borrowing, managed metrics
and other original design requirements remain unfinished. Owned-read activation
stays disabled; full qualification remains waived and the full design is active.


### Foreground startup admission separates recovery from policy ownership (2026-10-14)

Controlled foreground batches now perform startup admission before acquiring
this invocation's policy lease, then recheck after service acquisition. This
allows the original permitted CPU boundary to lend while waiting for the startup
flush fence before policy ownership. An explicitly held root still suppresses
lending. The later recheck rejects raced pending work rather than waiting under
the newly acquired policy/snapshot holders. Mandatory/replay/resolution and
null-control legacy calls retain their existing startup protocol.

A non-lending entry now tries the flush/apply fences and performs a real outbox
presence probe. The probe inspects the three legacy keys and first v2-prefix
entry without copying or decoding an outbox payload. Only a successfully fenced
empty snapshot permits clearing optimistic startup, policy and primary-outbox
hints. Busy flush/apply/backend/policy gates and scoped failures leave startup/
policy flags intact. Existing or malformed records remain obligations. Discovered
pending work and append debt conservatively retain the maybe-bit and hint the
existing dedicated recovery owner only after probe-owned fences retire. The probe
does not deliver, acknowledge or delete an obligation, mint a root, renew a deadline
or grant new mutation authority. Legacy acquisition/recovery bodies are unchanged.

Twenty focused native tests pass, including 19 new real DocStore/scheduler startup
scenarios: idle/empty, all four contended gates, legacy/v2 records, append hints,
stale primary hints, initial and late original rejection, held parents and lending
before policy. Both original operations retain 40 bytes while parked; protected
control runs, the original producer resumes, same-scope retry succeeds after the
holder releases, and hint callbacks assert probe fences have retired. The existing
startup target passes 7 tests and outbox target 14 tests. The 21 policy and 23 RPC
tests, 45-step provider check and 44-step ingress check also pass.

Eight physical DB tests pass with no skips/failures/leaks. They verify immediate
non-lending startup contention rejection, independent original scoped failures,
healthy retry after fence release, and an actual one-ordinary-slot scheduler wait
with zero policy readers, protected control progress and the original producer's
resume. Opaque legacy and v2 records survive rejected writes, close/reopen and a
second rejection; no new user row appears. These fixtures prove preservation and
fail-closed classification, not valid WAL matching, delivery or acknowledgment.
Movable DB fixtures do not start resident recovery workers.

The first cold physical compile peaked at 11,644,633,088 bytes, above the initial
10 GiB reservation, while all eight runtime tests passed. The artifact declaration
was corrected to 12 GiB; its local helper needs 14 GiB, so the runner uses a 16 GiB
build scheduling budget. The final clean run uses the identical cached binary.
Both intermediate scheduling results are retained. Auxiliary source differences
are only the scheduling hint: exact reversal from 12 to 10 GiB reproduces every
recorded hash. Implementation, external input and log hashes are verified across
eight final receipts. ABI/dependency sources are unchanged; API ABI 45 and storage
owner ABI 100 remain valid. Evidence and exact limits are recorded in
`startup-predecision-main-c102cbe-oct14-application.json`.

This closes a foreground startup entry dependency, not the full recovery graph.
Lendable inner recovery retains coarse phases. Cold I/O, provider decoding and
non-yielding seek work beneath the presence probe are not proved constant-time
or fully cooperative. Background/recovery root adoption, accepted mutation waits,
retained-capture/import eligibility, other roots/providers/RPCs, signed retained-
parent authority, complete physical pin accounting, bounded non-yielding work,
external recovery, peer refresh, idle borrowing, managed metrics and other original
requirements remain unfinished. Owned-read activation stays disabled. Full
qualification remains waived; this is focused native macOS evidence and the full
design remains active.


### Latest main merge and startup validation limit (2026-10-14)

Merged `origin/main` at `ef7a5e3542` in `874c63642b` on the same
`codex/workload-scheduling` branch. Warming retains its original Pass deadline,
cancellation checks and physical callback join alongside upstream atomic progress
persistence and native-generation reuse. Composed-query profiling and the original
admission context both survive the HTTP integration. Recovery stash
`e0e29653ec1488013700b6de2a28c7dd033fde17` remains available. Restored WIP is unstaged;
175 tracked files outside the six incoming-main overlaps are byte-identical, and
all original untracked files survive (one active build receipt finished normally).

Post-merge native macOS checks pass: 27 maintenance/warming runtime tests and all
45 retained-read-provider compile steps, with source, external-input and log hashes
verified. `merge-main-ef7a5e-oct14-application.json` records the integration.

The earlier eight-test startup result above predates the read-only ordering guard
and subsequent test-only bounded healthy retries. That guard preserves ReadOnly
rejection with pending startup hints and opaque outbox records. Its latest
pre-merge physical run (`startup-predecision-readonly-batch-main-c102cbe-oct14-native-v4`)
passed seven of eight tests; BEGIN/prepare returned `ExecutionWaitUnsupported`.
This failure remains unresolved, so the current startup changes are not fully
validated. The attempted final read-only application audit did not pass and is not
claimed as completed evidence. Full qualification remains waived; the full design
and owned-read activation remain unfinished.


### Controlled transaction capture preserves original progress (2026-10-14)

The pre-merge BEGIN/prepare `ExecutionWaitUnsupported` failure is now resolved.
BEGIN's read adapter and `MutationAdmissionTrace` previously discarded progress
callbacks, so a contended inner capture could report unsupported waiting. The
trace also hid lending metadata. It now forwards original progress, lifetime,
deadline and lending capability, retains the first rejection, and validates the
incoming contract before reading capability metadata. This preserves DocStore's
existing retained-capture suspension guard rather than bypassing it. No ABI field
or layout changes are introduced; API ABI 45 and storage owner ABI 100 remain.

Controlled BEGIN and pre-decision intent collection now use the existing
pre-decision progress policy while apply/policy service ownership is retained.
Inner capture contention returns `Backpressured` and unwinds those holders; the
caller can retry with its original owner after the producer resumes. No new
execution root or deadline is created. Null legacy controls and mandatory
resolution/acknowledgment paths retain their existing behavior.

Twenty-two focused native tests pass, including two new tests exercising exact
sticky failures, successful progress, lifetime-only checks, original deadlines,
invalid contracts and the retained-capture lending guard. Actual memBackend,
DocStore and TxnManager BEGIN/prepare operations contend on the payload-policy
mutex with one ordinary CPU slot. Producer/background and writer/write retain
40 bytes; protected control runs, separate scoped errors stay rejected, and the
original producer resumes after the writer parks. The healthy scope retries;
import-reader and scheduler byte counts retire to zero.

All eight physical DB tests pass without skips, failures or leaks. The BEGIN
fixture now holds the inner payload-policy mutex and checks retryable rejection,
zero CPU transitions, policy-reader retirement, reacquirable apply and absence of
a rejected BEGIN before retry. The full target also verifies startup lifetime,
read-only rejection precedence, opaque legacy/v2 outbox preservation across reopen,
pre-policy lending, snapshot/apply/global admission and pressure hard guards.
The cold physical compile took about 12 minutes with reported MaxRSS 7G, within
its existing 12 GiB artifact reservation and 16 GiB runner scheduling budget.
The persisted transaction target passes five tests plus its six-test local helper,
covering rejected BEGIN and accepted PREPARE/resolution/coordinator state across
WAL replay. Provider and HTTP ingress compile checks pass 45 and 44 steps.

`mutation-progress-main-ef7a5e-oct14-application.json` verifies exact source,
external-input and log hashes for all five final receipts; no source normalization
is used. The earlier pre-merge failure and initial test API spelling compile error
remain recorded. This supersedes the unresolved-validation limitation in the
preceding merge note, while preserving that historical result.

This closes controlled transaction-record/intent capture admission. Optimistic
preparation, inner coarse recovery, import/publication dependency eligibility,
broader root adoption and other original-design requirements remain open.
Owned-read activation remains disabled and full qualification remains waived.
This evidence is native macOS; the full goal remains active.


### Run-source opens preserve producer capacity and independent authority (2026-10-14)

Run-source cache misses now release the original execution capacity during leased
source opening and restore it before cache publication. Failed CPU reentry or
late original lifetime rejection closes a successfully opened source before
returning. The original scope also wins over provider errors after completion;
no new root, cancellation source or deadline is created.

Only a caller advertising its original execution-wait boundary coalesces with a
matching in-flight producer, and the source epoch must still match. Legacy and
non-lending callers open independently, avoiding an in-flight producer dependency
while retaining CPU. Concurrent opens install one cache owner per path/epoch;
redundant candidates close outside the source-cache mutex. An expired cache winner
cannot replace a valid candidate. A waiter observes its own scope after waking;
a failed producer causes at most one independent attempt with the waiter's
original admission, rather than propagating foreign cancellation or joining an
unbounded retry chain. The now-unused private completion Io/event is removed.
Public ABI layouts are unchanged (API 45, storage owner 100).

Twenty-four focused native tests and twenty-five storage admission tests pass
without skips, failures or leaks, alongside the 45-step provider check. Twelve
new scenarios use actual OS threads and a real single-ordinary-slot scheduler.
Legacy/coarse peers finish before producer release; qualified peers coalesce;
epoch changes prevent stale joins. Foreign producer cancellation and I/O failure,
persistent provider failure, rejected wait begin, failed CPU reentry and late
original rejection preserve authority and ownership. Background/write phases,
protected control progress, 40 retained bytes during overlap, one cache winner,
exact source close counts and complete flight/operation/byte retirement are checked.

Three native Lite-provider scenarios additionally verify real artifact marker/pin
cleanup after rejected resume and late cancellation, and healthy reopening under
a fresh scope. An actively held source survives deletion/rewrite activity, its
original scope retiring and both Lite/backend close; no retired scope callback is
used by that physical read. This does not prove complete per-query pin accounting.
The eight-waiter storage regression now declares explicit lending eligibility,
checks one shared initial producer and exactly one independent retry per waiter
after failure. Existing allocation-failure, decoder and native FD-budget checks
remain green. The initial outdated-fixture timeout is preserved as evidence.
`source-open-admission-main-ef7a5e-oct14-application.json` verifies exact source,
external-input and log hashes for all three final receipts without normalization.

This removes a run-source producer dependency; DocStore's retained-capture lending
guard remains. Coarse/null portable publishers, provider-internal gates,
non-yielding metadata/decoding, deeper recovery and the broader capture/publication
graph still need qualification. Checking the original scope after a provider
returns does not prove arbitrary physical opens return within that deadline.
Other original-design requirements remain open, owned-read activation stays
disabled and full qualification remains waived. Evidence is native macOS.


### Controlled portable publication drains outside apply and policy (2026-10-14)

Directory and Lite prepared-generation publication now share explicit
pre-publication admission. Controlled callers retire the prepared raw-policy
lease before apply waits and re-enter current raw policy under apply. A publisher
claims reader exclusion before dropping apply/policy to drain existing readers
through its original CPU boundary. New readers and competing publishers remain
excluded; an existing reader can resume without finding the publisher occupying
its CPU or apply dependency. Non-lending controlled contention is retryable
`Backpressured`. Initial rejection leaves the caller's pre-existing prepared lease
for its normal retirement; helper-acquired ownership retires on later failures.

After draining, the publisher withdraws exclusion, reacquires apply and rechecks
current policy and the complete existing pristine-target predicate. It also
compares a deep-owned namespace/range witness: a namespace or in-place boundary
change cannot silently rebase the same attempt. Allocation failure and bounded
copy/comparison lifetime checks unwind without publication. The no-reader branch
retains the final recovery-required/lifetime check. Each failure cancels only its
own exclusion bit and preserves remaining readers. No privileged read bypass,
execution owner, deadline or mutation authority is introduced. Null-control
legacy ordering is retained; public ABI layouts remain API 45/storage owner 100.

Twenty-five focused native tests pass without skips/failures/leaks. Twenty-four
new publication scenarios use real DocStore/memBackend, apply and policy gates
and a one-ordinary-slot scheduler. They verify both service leases retire before
wait, original background/write producer resume, protected control, 40 retained
bytes during overlap, target/policy/destination revalidation, both range-allocation
failure points, original scoped failures, canceled Io, competing-bit preservation
and recovery-required races including an initially empty reader count. Success
returns a drained publication fence plus apply/raw-policy ownership; all final
policy-reader, scheduler operation and input-byte counts retire to zero.

Actual directory/Lite publication and retained-read provider checks both pass
45 compile steps. Exact reversal of only the two DB admission blocks reproduces
the preceding DB source hash, proving the full pristine predicate and irreversible
publication, metadata, activation and durability-unknown tails are unchanged.
`publication-dependencies-main-ef7a5e-oct14-application.json` and its preserved patch
record three exact-source/external-input/log-verified final receipts. An initial
cache-directory disappearance interrupted qualification; its incomplete receipt
remains recorded. Subsequent cold rebuilds and final checks completed normally.

This is focused helper/runtime and actual factory compile evidence, not new full
end-to-end or distributed import-under-overload qualification. The pristine-target
validator and post-publication activation/completion work remain coarse. Null/
coarse publisher adoption, other capture/import/provider dependencies and the
remaining original design still need work. DocStore retained-capture lending
eligibility remains guarded, owned-read activation stays disabled, and full
qualification remains waived. Evidence is native macOS; the full goal stays active.

Cache GC reached terminal status 0 after the final checks; shared uv pruning
timed out on another process's cache lock. Zig/Rust cleanup and subsequent Node
cache checks completed. The canonical GC log and this limitation are recorded in
the application receipt; final source/external/log hashes were reverified.


### Bounded pristine publication target probes (main 7cf87a, October 14)

The shared DB pristine-target predicate now uses cached catalog eligibility,
point-presence probes and physical prefix seeks instead of cloning index,
enrichment and resolver configs, listing transactions or snapshotting/scanning
identity history. Identity inspection copies at most five metadata roots and
200 value bytes; a mapping, state, chunk or unknown identity record rejects the
target immediately. Existing identity validation and statistics run over a
private bounded raw metadata projection. Raw writes preserve malformed source
bytes for validation without invoking normal mutation side effects. Unsupported
or oversized identity roots conservatively reject the target. Reconcile intent
retains priority, and final bulk/projection lease checks remain fresh under apply.
Controlled calls preserve the original lifetime/progress while refusing CPU
lending under apply/policy ownership.

Failure injection exposed a shared erased-cursor bug: callback NotFound was
normalized to exhaustion. A synchronous shared rejection trace now distinguishes
original callback/wait failures from provider exhaustion for admitted seek/next,
forwards capability metadata and always rejoins an already-begun wait. Mutation
admission uses the same trace. No authority is retained by the cursor.

Native macOS evidence: 27 runtime tests pass with zero skips, failures or leaks;
publication and retained-read provider checks each pass 45 compile steps. Cases
include all 32 supported metadata-presence combinations, malformed/oversized
roots, catalog/user/transaction/schema/reconcile rejection, original errors at
every healthy-path checkpoint, backend contention, protected control progress,
10,000 disqualifying history rows, allocation failures and 48 seek/next scope
failure/exhaustion/resume combinations. The final test name is included by the
build filter. Source, external-input and log hashes were verified.

Reversing only the pristine predicate/call changes and incoming main DB patch
reproduces the preceding publication DB source exactly, leaving irreversible
publication/metadata/activation/durability tails unchanged by this component.
`pristine-probe-main-7cf87a-oct14-application.json` and
`pristine-probe-db-tail-audit.json` record the evidence and preliminary failures.

This bounds logical inspection and private copies, not physical provider
capture/seek work. Post-publication activation/completion, broader capture and
legacy dependencies, retained authority/distributed failures and the remaining
original design still need implementation and evidence. Owned-read activation
remains disabled. Full qualification remains waived; the full goal stays active.


### Point lookup admission and original readiness scopes (main 7cf87a, October 14)

Public lookupKey and lookupNamespaceTableDocument now share coarse query
admission. One permit spans both aliases and remains owned through physical
response retirement, including slow or failed output. Response retirement chains
an existing owner before freeing its own metadata and releasing query capacity.
Authentication precedes admission. Projection, version headers, row-policy,
routing, consistency and lookup result construction remain byte-identical after
normalizing the extracted function's authentication-pointer parameter. A final
original-scope check rejects late cancellation before response publication.

Admitted lookup readiness retries release only CPU during their actual retry
sleep, after the failed attempt's service leases retire. CPU return uses the same
original team, current class, deadlines, cancellation and retained metadata. An
atomic first-rejection record preserves fallible scope errors across legacy
boolean cancellation, provider errors, absence and returned data. Rejected owned
results are discarded. Mandatory checked rejoin still occurs after sleep or
scope failure. The local readiness ceiling stops sleeps/new attempts while the
original caller deadline governs CPU return; readiness expiry does not invent a
caller timeout or refresh a budget.

The final native macOS point-lookup target passes 7 tests and 51 build steps with
zero skips, failures or leaks. Cases include 20 public alias/failure schedules,
36 admitted source rejection/retry schedules, 33 generic wait schedules, an
existing-owner retirement chain and an expired-local-ceiling/live-caller check.
They cover foreground memory exhaustion, original/late cancellation, original
error identity (including NotFound and retry-class errors), producer/control
progress, rejected resumption, data discard, class retention and final counters.
The policy/ABI target additionally passes 21 tests across 51 build steps. Source
sets, source/external-input hashes and log hashes are verified in
`point-lookup-main-7cf87a-oct14-application.json`.

Point providers still use legacy capture/get paths. This point-lookup component
left local decoder flight and scratch waits unaudited; the decoder component
below addresses those waits. Provider-level admission and the remaining generation
proofs are required before point lookups can safely hold runnable CPU throughout
native storage execution. This component therefore establishes coarse operation
ownership and admitted readiness waiting; full runnable point execution, internal
lookup RPC/MCP adoption, recovery workers, post-publication completion and the
remaining original design stay open. No full protocol, distributed or performance
qualification is claimed. Full qualification stays waived and the goal is active.

Cache cleanup reached terminal status 0. pnpm pruning reported EPERM on a cache
file outside the writable workspace; the remaining phases completed. The
canonical GC log and partial-prune limitation are recorded in the application
receipt. Final source/external-input/log hashes were reverified after cleanup.


## Local decoder dependency admission — October 14, main f56cf1f4c7

Production local reads now use fallible bounded workspace admission even without
an explicit read port. A legacy or non-lending caller decodes independently and
returns `Backpressured` when scratch is busy, rather than holding the last ordinary
CPU while a parked producer owns scratch. Explicit unadmitted compatibility
acquisitions remain for existing fixtures. Four workspaces, sixteen flight records,
byte admission, scratch recycling and oversized-row admission alone are preserved.
Only explicitly lendable original scopes join flights or wait for their capacity.
Flight path allocation and final destruction occur outside the coordination mutex;
reserved keys remain unpublished until allocation and original-scope checks pass.

Flight identity includes a process-local immutable run generation, path/run id,
physical offset/length, logical length, compression, checksum and cache-admission
intent. Static issuer identity distinguishes separately compiled owners; sequences
never wrap. Zero identities bypass sharing. Snapshot clones preserve identity, and
logical revisions take it from their pinned physical source rather than stale plan
metadata. These tokens do not replace retained run/source pins or prove the entire
cache-generation and physical-byte accounting graph.

A failed peer flight is released before one independent decode under the original
scope; the retry never re-enters flight election. Followers do not inherit another
operation's cancellation, deadline or provider failure. Own scope rejection still
wins. Public document lookup maps decoder backpressure to 503 with Retry-After: 1
through both aliases, preserving output-retirement query ownership.

Native macOS validation: 12 focused decoder tests, 32 physical LSM/Lite tests,
7 point-lookup tests, and a 51-step retained-read provider compile check pass with
zero test skips, failures or leaks. Ten two-thread/sole-ordinary-slot schedules
verify producer/control progress, retained metadata and final retirement; tests
also cover exact decode-contract matching, allocation/checkpoint failures, identity
revisions, coarse cursor saturation and retry. Eight physical readers perform one
successful decode, or eight physical attempts when the leader and all independent
retries fail. The HTTP fixtures cover 22 alias/failure schedules.

`local-decoder-main-f56cf1-oct14-application.json` records exact inputs, logs and
validation boundaries. HTTP ran before a final run-revision identity correction
and its test, plus a compatibility-method comment; those are rechecked by the final
decoder/storage/provider targets. HTTP implementation and tests did not change after
that successful run. No broader default activation is claimed: remaining DocStore
capture dependencies, provider/root adoption, cache/pin-generation accounting,
mandatory completion/recovery and distributed/default qualification stay open.
The full original design and goal remain active; full qualification remains waived.

Decoder-component cache cleanup reached terminal status 0. pnpm pruning reported
EPERM outside the writable workspace; other phases completed. Source/external
inputs and log hashes were reverified after cleanup, including the explicitly
recorded HTTP snapshot differences. The application receipt includes the GC log.


## Capture dependency admission — October 14, main f56cf1f4c7

DocStore's begin-with-admission entrypoints no longer blanket-reject original
lendable scopes. Generic-budget entry validates the erased lifetime contract
before policy ownership, and portForBudget preserves an existing port verbatim
through capture, physical-batch and cursor adapters. This fixes a real boundary
loss: re-erasing a Port as a generic budget had silently omitted its execution
wait capability. Invalid original contracts remain invalid; no fresh operation,
CPU class, deadline, cancellation authority or retained-metadata root is created.
Incomplete lending budgets reject before any provider lock or read ownership.

Admitted Lite capture now allocates one pin before locking and attempts its
coordination locks without waiting. Contention retires all generation, owner,
reader and writer locks before invoking the original progress callback. A coarse
caller returns Backpressured. Cold generation initialization uses an already-held
read mutex rather than recursively acquiring it; admitted kernel reader fencing
uses tryLock and retries only after provider locks retire. Late rejection after
pinning aborts exactly once. MemBackend's acquired check and the shared admission
lock helper use non-yielding lifetime checks under their mutex/lock. Legacy Lite
physical-I/O paths and the synchronous cold-file construction path are not claimed
to have adopted general execution-loan transport.

Legacy portable publication tries apply ownership and rejects live-reader drain
contention instead of consuming the CPU those readers need. Coarse direct drain
calls do the same and withdraw only their own publication bit on failure. The
existing controlled publisher still retires policy/apply and lends its original
CPU, rechecking destination and pristine state after waiting. No irreversible DB
publication/activation body changed; the source audit verifies the DB diff is only
a test explicitly supplying wait authority and the helper diff is only its legacy
pre-decision apply branch.

Native macOS checks pass: 5 focused capture tests, 27 retained-capture regressions,
32 physical storage tests, and both 51-step provider compile checks. The focused
matrix includes 49 two-thread/sole-ordinary-slot interactions across MemBackend,
LSM and Lite-supported captures; 25 Lite generation/owner/reader/writer/inode lock
schedules; 3 late physical-pin rejections; existing publication cancellation and
recovery checks; exact port forwarding; acquired-lock no-yield checks; and early
incomplete-budget rejection. Producer/control progress, original reentry, retained
metadata and final reader/kernel-pin/operation counters are checked. All runtime
suites report zero skips, failures and leaks. Exact source/external-input/log
receipts and the production-source audit are indexed by
capture-dependency-main-f56cf1-oct14-application.json.

This removes a DocStore-level prerequisite; it does not prove the complete caller
dependency graph or activate end-to-end runnable defaults. DB-level statement cuts
and provider capture still retain apply/policy/snapshot service ownership, and
must retire it before waits or explicitly suppress lending and retry safely.
Artifact reconstruction, remaining provider/RPC families, full query heap and pin
accounting, mandatory completion/recovery, distributed failure and default-load
proofs also remain. The original design and goal are active; full qualification
remains waived by the user.

Capture-component cache cleanup reached terminal status 0. pnpm pruning reported
EPERM outside the writable workspace; other phases completed. All final compiled
source, external-input and log hashes were reverified after cleanup. The
application receipt records the partial-prune limitation and canonical GC log.


### October 14: statement capture retains CPU until service fences retire

On merged main `5fdb3ea392` (branch merge `e5498c83f0`), synchronous
`RetainedDependency` admission preserves the original lifetime, absolute clock
and first rejection while refusing CPU handoff and resource waits. Availability
returns `Backpressured`; it does not poison the original scope. The borrow never
escapes into a returned transaction, view or snapshot and creates no new root.

Primary/replay intent probes, DB statement cuts, apply-held document/relational
snapshot capture, single-owner multi-view capture, and shared fence acquisition/validate/open/
snapshot wrappers now carry that restricted borrow. Snapshot and maintenance
marker reads use admitted storage operations rather than legacy capture calls.
The coordinator can release complete partial fence/view sets before waiting and
retrying with its original authority. Immutable delayed snapshot opens and row
pages retain their ordinary lending capability after service fences retire.
Schema, range, principal and policy epoch validation remain required.

Pending policy publication still progresses on later replica admission: an
admitted reader tries the exclusive apply lock and returns pressure if busy.
Its existing durable policy decision/completion tail is unchanged and receives
no new foreground rejection after publication. This tail still contains coarse
storage operations; this change does not qualify all policy publication I/O or
close the full protected dependency graph.

Focused evidence is recorded in
`statement-fences-main-5fdb3ea3-oct14-application.json` under the cumulative
integration receipts. Native tests cover real Lite intent presence/absence,
busy capture retirement and retry, primary/replay exclusion, rejection at each
warm lifetime check (including NotFound), pin cleanup, sticky original failures,
and shared fence result retirement exactly once. The cold LSM intent regression
now requires zero CPU checkpoints/loans while fenced. Provider and physical DB
semantic checks cover integration; they are not runtime qualification of a
complete multi-owner DB deployment. Full qualification remains waived.

Remaining work includes coordinator checkpoints outside these wrappers and
retained-RPC owner callbacks while other fences remain held, plus bounded
policy/publication physical I/O and remaining
provider/RPC admission families, protected recovery and mandatory completion,
full query heap and physical pin accounting, and distributed/default-overload
proofs. The original whole-design goal remains active; these focused results
are not evidence that runnable defaults are safe across every path.

Statement-capture verification completed: 8 native capture tests (0 skips,
failures or leaks), 27 retained-read tests, 51 provider compile steps including
the actual BoundTableReadSource statement/dynamic-cut constructors, and 49
physical DB semantic steps. The final native implementation/test inputs match;
only the separately validated semantic-provider root was expanded afterward.
The application receipt records exact input deltas. Cache cleanup finished with
status 0 and no partial-prune failures. Changes remain uncommitted and unpushed.


### October 14: coordinator cohorts and retained RPC checkpoints

After the local statement-capture component, both production provisioned
coordinators now use lifetime checks between participant acquisitions and a
retained-dependency borrow for frozen view/cut/proof construction. Null,
ReadUnavailable and Backpressured acquisition outcomes retire the complete
partial cohort before the original progress wait. Successful capture validates
all owners, retires every local fence handle, and only then runs the ordinary
CPU checkpoint. Failure there still retires all new cuts/views and route
activity; the captured visibility cut and range/principal checks are unchanged.

Retained RPC owner capture/validate/snapshot/open calls use the same restricted
synchronous borrow. RPC entry and response encoding for those operations now
check retained lifetime without CPU handoff. The fallible cancellation contract
is authoritative (`ensureExecutionActive`), so one-shot scope errors such as
NotFound survive instead of being normalized into Canceled. Pending registry
entries still own cancellation capsules and admission through physical cleanup;
returned resources retain no request-stack admission port.

The client uses lifetime checks for capture-bearing protocol work. Its actual
transport may obtain the original explicit I/O wait boundary after a lifetime
check without first executing an ordinary CPU checkpoint. Restricted ports
still expose no boundary. This inspection creates no new root, clock, class,
heap authority or wait capability; cancellation polling, checked transport
rejoin, bounded timeouts, ACK checks and the no-replay rule remain required.
Immutable snapshot paging retains ordinary lending behavior.

Focused native evidence: 3 production coordinator tests (10 dynamic scenarios,
including late cancellation after fence retirement, plus two-owner C=1 retry),
21 retained owner/RPC/client/registry tests including 16 late-resource rejection
cases and scope rejection at every validation/encoding check, 8 actual storage
capture tests, and 51 physical provider compile steps. No selected test skipped,
failed or leaked in the final runs. Details and exact input deltas are in
`coordinator-fences-main-5fdb3ea3-oct14-application.json` under cumulative receipts.
The first RPC run exposed two fixtures requiring unsafe fenced loans; those
fixtures now assert the retained contract, while page/network I/O loan coverage
remains in place. These are focused proofs, not full deployment qualification.

Remaining: prove the complete retained network producer/control dependency
graph and acknowledged remote cleanup through root/physical charge retirement;
local handle deinit does not prove an unreachable peer has physically released
its fence. Coarse policy-publication I/O, protected recovery and durable
completion ownership, other ingress/provider families, full query/pin accounting,
and distributed/default-overload proofs remain. No default activation changed;
`owned_reads_v1` remains false. Full qualification remains waived; the original
whole-design goal is still active. Changes remain uncommitted and unpushed.

Coordinator/RPC cache cleanup reached terminal status 0. uv pruning timed out
after 5 seconds on a shared cache lock; other phases completed (198.7 GB free).
The application receipt records this partial cleanup and verifies evidence log
hashes and final source deltas.


### October 14: exact-token physical retirement evidence

The retained registry now distinguishes revocation from physical retirement.
`closeAcknowledged` returns pending while a borrower or an out-of-lock destructor
still owns the resource; principal/global registry quota stays charged. It can
report retired only after that exact issued token disappears in the same
process incarnation. Unissued/future tokens and changed process identities fail
closed. Slot reuse cannot revive an issued sequence or revoke its replacement.
The production server initializes the registry with a secure random incarnation.

Cleanup RPC replies carry versioned evidence bound to that process and token.
Connection cancellation reports revoked, never terminal retirement: an in-flight
creation can arrive after the scoped scan. No tombstone/timeout shortcut was
introduced. Normal read response JSON is byte-identical to the prior shape;
only cleanup replies add evidence. Read decoders reject cleanup evidence as a
row/cut/proof/normalization result.

The client `closeAcknowledged()` requires both the routing ACK and exact,
well-formed cleanup evidence. Pending, lost/malformed replies, missing evidence,
wrong token/process/connection/version, and process restart cannot authorize
retirement. Only exact idempotent cleanup can be repeated; capture/open/page
replay stays forbidden. Evidence is per token, not a signed parent/generation
fence or proof that sibling/child capabilities have retired.

Legacy view destruction still performs best-effort cleanup and drops its local
client handle. It does NOT implement the node-owned sender recovery transfer or
retire signed owned-read root charges. `owned_reads_v1` remains false, and no
activation flag changed. The receiver registry's physical quota retention and
the new truthful protocol evidence are qualified here; complete distributed
root/physical-byte accounting is not.

Next required integration: reserve a bounded node-owned retirement obligation
before publishing retained work, retain its stable endpoint/authority and
parent accounting through pending/error outcomes, and join cleanup or transfer
it explicitly to protected recovery before stopping the foreground root. Unknown
creations need the signed published-attempt/generation fence; a connection
revocation ACK, elapsed lease or new peer process is insufficient. Complete
network dependency proofs, durable completion/recovery, remaining ingress and
provider adoption, full query/pin accounting and default-overload evidence also
remain under the original design.

Focused final evidence: 24 native retained RPC/client/owner/registry tests,
including a real concurrent blocked destructor, quota/slot reuse, late creation
after cancellation, lost close response, restart and malformed-ACK cases; and
51 physical provider compile steps. No selected test skipped, failed or leaked.
The application receipt is `retirement-evidence-main-5fdb3ea3-oct14-application.json`
under the cumulative integration receipts, with input/log hashes and component
diffs. Full qualification remains waived. Work remains uncommitted and unpushed;
the whole-design goal remains active.

Retirement-evidence cache cleanup finished with status 0 (186.5 GB free). No partial-prune failures were reported. Final component inputs and evidence log hashes were reverified.


### October 14: bounded node-owned retained-read retirement

`retained_read_retirement.zig` provides a bounded node ledger and one protected
consumer, with no per-capability worker. Reservations bound item/count/total
metadata bytes and retain the complete original execution/allocator/transport
parent chain before publication. Parent callbacks must charge the actual
execution and own stable references; they are not request-stack admission ports.
Setup failure rolls back, shutdown closes publication, and transfer needs no
new allocation or worker. A fair cursor attempts at most one queued close and
one recovery observation per finite control pass, lending CPU through actual
transport I/O. Slot quota and parent charges survive physical resource free and
parent-pin release. Shutdown cannot destroy an undrained owner.

`Client.captureOwned` integrates this ledger with capture, snapshot and cursor
creation. Each child reserves its own obligation before invoking the executor;
returned handles store stable metadata on the node allocator. Close transfers
ownership instead of synchronously waiting or freeing an unresolved client.
Cleanup uses its own bounded scratch allocator and finite control authority,
not the foreground allocator, cancellation or expired deadline. Exact-token
pending/error results keep charges. A changed process enters needs_fence; only
re-observing authoritative retirement can clear it. Genuine unknown creations
remain charged and require signed recovery integration, not connection cancel
or timeout. A valid creation ACK's fixed token metadata is recorded before a
late original scope error, so that case can close its known capability without
exposing query output or replaying the creation.

Evidence: 26 native owner/client/RPC/registry tests (0 skips/failures/leaks), plus
51 physical provider compile steps. New cases exercise the actual scheduler at
C=1, capture/snapshot/cursor ownership, foreground arena destruction, lost close
replies, healthy control during I/O, capacity rejection before executor entry,
fair unavailable-peer selection, reentrant construction/shutdown, stale tickets,
late cancellation after acknowledged creation, and an original stopped root
remaining charged through process mismatch and exact old-process retirement.

This is the callable managed path, not default adoption: production provisioned
and SQL adapters still use the legacy capture entry point. No server bootstrap
installs this new runtime yet. Next: supply stable charged parent bindings from
those owners, persist prepublication obligations and connect unknown-creation /
process-generation states to the existing signed recovery protocol, then prove
shutdown/restart/partition ownership before activation. Node baseline allocator
charging and complete parent-chain lifetime must be verified at each binding.
`owned_reads_v1` remains false. The original complete design, including remaining
provider families, durable completion, full query/pin accounting and overload
proofs, remains active. Full qualification remains waived.

Exact component inputs, patch and final checks are recorded in
`node-retirement-main-5fdb3ea3-oct14-application.json` under cumulative receipts.
Changes remain uncommitted and unpushed.

Node-retirement cache cleanup reached terminal status 0 (199.1 GB free). uv
pruning timed out after 5 seconds on an unrelated shared-cache lock; other
phases completed. Final source, external-input and evidence-log hashes were
reverified; the application receipt records the partial-prune limitation.

### Owned read retirement binding through admission (after main fc4f2402)

The admission ABI now exposes an optional typed retirement binding. Generic
budget adapters, retained-dependency restrictions and rejection traces forward
it synchronously, validating the original lifetime and ABI before copying the
stable registry/parent capability. The restricted port still withholds its CPU
wait boundary. A rejection propagates; it never selects legacy capture.

The bounded retirement registry and its binding/resource types now live in
embedded common code; the API module retains the independent node consumer.
`Client.captureWithAdmission`, used by the production provisioned coordinators,
selects managed capture when that capability is supplied. `execution.Adapter`
can carry it through its SQL admission borrow. This adds no request-stack Port
to retained resources and does not allocate a worker per resource.

Evidence: 26 owner/client/RPC/registry tests plus three statement admission
tests passed, with no skips, failures or leaks; native provider and production
pgwire type checks passed (51 and 50 build steps). Managed-client cases now
enter through the admission path and both restriction/trace wrappers. They
reject invalid ABI and canceled scopes before RPC or parent charges, retain
original charges through lost close replies, and physically destroy the
foreground arena/poison its scope before independent cleanup. Both ordinary
and late-cancellation cases retain the existing scheduler/control checks.

Not default activation: SQL OwnedRead does not yet install a stable parent
binding, and the server does not bootstrap this sender retirement runtime.
Next, restructure OwnedRead setup failure and cleanup completion so the original
execution/allocator/listener/transport chain survives every outstanding pin,
then wire node bootstrap/shutdown. Signed unknown-creation/generation recovery
and baseline allocator charging remain required before activation. In
particular, publishing a cleanup reservation only after Stream.open succeeds
cannot protect remote work published during a failed open; and the cleanup
runtime, its executor/transport, and scheduler storage must remain alive while
obligations remain.
These lifecycle gaps must be resolved, not hidden by a null-binding fallback.
`owned_reads_v1` remains false. The full design remains incomplete, with all
previous provider, durable completion, accounting and overload requirements.
Full qualification is still waived. Changes are uncommitted and unpushed.

Exact five-file delta and source/check hashes are recorded by
`owned-read-binding-main-fc4f2402-oct15-application.json` in cumulative receipts.

### SQL setup ownership and cumulative read retirement charges (main fc4f2402)

OwnedRead now transfers plan, admission and parent lease to its stable capsule
and publishes the reserved cleanup supervisor before beginning read capture.
All later setup errors use retained cleanup; a missing/declined stream no longer
implies that no remote resource was published. Native teardown closes a stream
only if open actually returned one. Transient receipt-budget borrows stay shared
through synchronous setup (preserving fallback's original wait deadline), then
are copied/cleared before asynchronous retirement or successful delivery.

The SQL capsule provides a typed retirement Parent that pins its complete
retained lease and charges each obligation to the original query root. Native
cleanup/future join and remote retirement are separate fences. Last native join
parks CPU; operation/admission/bytes remain until every remote parent pin has
retired. The last fence claims owning finish exactly once under a metadata lock.
A declined stream observes that finish for at most 100 ms and the original
request deadline before handing off to materialized fallback. If ownership is
still pending, it returns capacity pressure and retains the supervisor/pins.
Cancellation/timeout never authorize physical release. The observer has its own
capsule reference, and consumes setup rollback exactly once on either exit.

`retainReadBytes` adds actual obligation bytes without lending CPU, queuing,
changing class or refreshing foreground/cleanup clocks. It can follow cleanup
reservation and updates that reservation's retained byte count. Ordinary and
bounded partitions remain hard limits, including the verified bounded
continuation ceiling; control/write/cleanup/demotion growth is refused. Charges
are cumulative until owning finish. This fixes the earlier managed-client test
fixture's use of growInputBytes (a total resize, not an incremental charge).
The strengthened real-RPC test now checks the sum of all obligation charges.
This does not claim complete query heap or physical-generation accounting.

Evidence: 87 standalone scheduler tests, 26 remote owner/client/RPC/registry
tests and four pgwire tests passed, with no skips/failures/leaks in final runs.
The new pgwire test exercises six deterministic lifecycle scenarios through
real SQL stream binding, the published cleanup task/reaper and SQL parent
callbacks: failure during capture, late setup failure after a native cursor
opens, ACK before/after native join, clean fail-fast fallback, and bounded
fallback refusal with a still-owned remote obligation. The provider's remote
publication/ACK is fault-injected in this SQL test; actual transport evidence
is covered separately by the client/RPC suite, not a live cluster proof.
Production pgwire type checking passed 50 steps. The broader read-lane suite
also passed 73 local and 27 API tests before the final fallback refinement;
its exact source delta is recorded. The standalone scheduler target's missing
platform import was fixed, and its final cache replay matches the tested core.

Not default activation: node sender runtime/bootstrap and installing this
parent into the SQL adapter remain outstanding. Durable unknown-creation and
process-generation reconciliation, allocator baseline charging, worker/transport
lifetime and joined shutdown still require implementation/proof. Correction to
the preceding shutdown concern: protected cleanup roots can be admitted after
ordinary scheduler closure; the scheduler storage and independent executor/
transport must remain alive while obligations remain. `owned_reads_v1` remains
false. All earlier full-design requirements remain active, including non-SQL
families, durable completion, full query/pin accounting and overload proofs.
Full qualification remains waived. Changes are uncommitted and unpushed.

Exact four-file delta, failed intermediate checks and final source/log evidence:
`sql-owned-setup-main-fc4f2402-oct15-application.json` in cumulative receipts.

### Pending node retirement joins preserve the consumer (main fc4f2402)

A refused Runtime.join previously stopped the node consumer before reporting
CleanupPending. It now closes admission but retains the running consumer,
allocator, scheduler and I/O parents until obligations drain. A later exact ACK
or reconciliation outcome can release the same owned slots without another
worker or a replacement query scope. waitDrainedUntil provides bounded,
cancelable observation; observer expiry/cancellation never stops that worker,
releases charges or authorizes destruction. Final join/destruction still require
fencing all foreground Binding borrowers as well as draining ticket ownership.

Evidence: 27 native owner/client/RPC/registry tests passed, with no skips,
failures or leaks. The new node lifecycle test uses a charged allocator and a
real one-worker executor, rejects startup under allocation/concurrency pressure,
keeps baseline and resource bytes/pins across pending join and canceled/expired
observers, proves worker continuation and C=1 health progress, and drains queued
and needs_fence resources after ordinary scheduler closure. Reconciliation
outcomes are injected in this node test, not signed generation proofs. The real
RPC managed-client cases now also retain original cumulative root charges and
poisoned foreground lifetime through refused node join/drain observation, then
drain on actual validated close acknowledgements.

This fixes a prerequisite to production node teardown, not default activation.
Actual server startup/worker-lease wiring, charged baseline installation, SQL
binding installation and signed durable unknown-creation/process recovery are
still outstanding. No worker stop may be treated as a physical peer fence.
owned_reads_v1 remains false; all original full-design requirements remain
active. Full qualification remains waived. Changes are uncommitted/unpushed.
Exact two-file delta and hashes: node-retirement-drain-main-fc4f2402-oct15-application.json.

### Current production owner audit and fixed metadata baseline (October 16)

Current source supersedes the earlier statements that sender bootstrap, worker
leases and SQL binding installation are absent. `DataServer` bootstraps enrolled
store roots and refreshes peers; `ApiHttpServer.bootstrapRemoteAttempts` creates
the protocol state, reconciliation consumer and read-retirement consumer before
atomic publication. Both consumers hold independent SQL worker leases and node
references. Their runtimes, control gate, endpoint views/source and capability
cache reserve persistent control baselines. Pgwire and HTTP SQL setup install the
node retirement registrar and pass its binding to the native adapter.

The audit found a remaining fixed metadata gap: `remote_attempt_owner.State`,
outbound slot/destination arrays, worker registry arrays and durable fence-store
arrays/serialization buffer used the partitioned allocator but were absent from
the scheduler's idle baseline charge. The owner now reserves their computed byte
sum before its first allocation or journal initialization, retains it throughout
its initialized lifetime and releases it after destruction. The complete node
capacity preflight includes this reservation before enrollment. Endpoint and
control-gate baselines stay separately owned; this reservation covers no dynamic
transport payloads or complete per-query heaps.

The complete default reservation initially failed its new envelope regression:
the prior one-MiB control ceiling could not cover all fixed node metadata inside
the protected baseline quarter. The managed automatic default now has a sixteen-
MiB scheduling-metadata ceiling and an eight-MiB control ceiling. Its eight MiB
of foreground-plus-bounded metadata exceeds the previous seven MiB. Automatic
startup still caps total scheduling metadata to 1/32 of the process envelope;
control is capped at half the resulting metadata budget, with a minimum of the
generic 64-KiB control reserve (or a smaller explicitly selected ceiling). The
existing 64-MiB process scenario therefore still resolves to two MiB total and
one MiB control. Fixed policies retain their exact declared reservation and are
never re-probed. A small envelope that cannot fit the full protocol baseline
continues to refuse bootstrap, rather than silently dropping charges.

`remote-owner-baseline-native-v5` passes all 82 focused build steps on unchanged
native/schema inputs: four named baseline contracts, nine protected control/TCP
contracts, and 110 scheduler/configuration protocol tests. The baseline contracts
exercise refusal before allocation or journal callbacks, initial allocation
rollback, late activation rollback, exact initialized backing-array byte counts,
healthy lifetime charging and final release. The default unit suite now includes
them. The opaque API owner consumer compile check passes; this is not execution
of a newly compiled production API image. Formatting and whitespace checks pass.
`remote-owner-baseline-verified.json` binds current inputs/logs and all thirteen
named baseline/control cases, the protocol-test aggregate, and the earlier v3
failing default-envelope receipt. The first run's invalid fixture envelope was
corrected in v2; v3 exposed the genuine default-budget gap; v4 named a nonexistent
scheduler target and ran no tests; v5 uses the existing cleanup-budget target.
No ABI, worker-wire or OpenAPI layout changed. Dynamic transport heaps and full
default process profiles remain outside this proof.

The current production recovery gap is narrower and concrete: an unused journal
can complete worker recovery, while a journal that previously published remote
work remains closed. There is no production completion path for that latter
case in the current caller inventory. Recovery must prove physical quiescence,
including prior process/external ownership, before enabling it. Production
capability families still default to false; toggling those bits alone would not
satisfy this requirement. Signed discovery and SQL binding code already exist.
Remaining resource accounting, full query/operator/front-end adoption, durable
mutation completion, adaptive policy and the original overload invariants remain
in scope. Full heavy qualification remains waived; these scoped checks do not
qualify the complete design or justify default activation.

### Signed joined-checkpoint recovery (October 16)

Production now has a clean-restart completion path for a previously published
journal. After the supervisor has joined foreground borrowers and periodic
consumers, State destruction requires no retained query parents, no endpoint
leases, an empty inbound registry/control set, and joined HTTP transports. It
can write a clean receipt only if inbound recovery was already complete and
outbound attempts, startup fences and queued work have drained. The registry's
physical reference-count contract and executor in-flight join precede receipt
publication. A newly empty recovery-blocked registry cannot manufacture proof.

The fixed 208-byte `remote-quiescence.v1` receipt is signed by the enrolled root
key and binds metadata realm, node/store/root identity, the prior reporter,
initialization ID and the complete durable journal checksum. It is written to a
pending file, synced, renamed and directory-synced while the exclusive journal
lease remains held. Startup verifies it against authoritative root identity and
the exact loaded checkpoint before advancing the outgoing generation. That
durable advance invalidates the old receipt. A later reporter on the same root
can use the receipt, but another checkpoint/root/initialization/key cannot.
Publication history remains monotonic and old worker generations stay closed.
Every outgoing destination still requires its normal fresh signed fence.

All fallible owner heap/executor setup now precedes the epoch advance, so an
allocation refusal preserves valid evidence for retry. A crash or later
activation failure after the durable advance still consumes the receipt and
requires additional physical recovery evidence. The receipt shares the fence journal's durability
and no-rollback assumptions; it does not authorize importing private authority
state into another root. Missing, malformed, stale,
incorrectly signed or unreadable receipts provide no authority. If a complete
signed receipt survives an uncertain final directory sync, it is still evidence
of the already-joined checkpoint; no work can follow publication by that owner.
Old images ignore the sidecar and retain their previous fail-closed behavior.
The fence journal, wire protocol and API layouts are unchanged.

`remote-owner-recovery-native-v5` passes all 82 focused build steps on unchanged
native/schema inputs: nine named owner/baseline/recovery contracts, nine protected
control/TCP contracts and eight fresh configuration tests. The standalone
scheduler target is cached success, not a fresh 110-case claim. The independent
`remote-owner-recovery-verified.json` checks all eighteen named cases, current
hashes, default-unit ownership and the unchanged scheduler source against its
prior passing receipt. The opaque owner consumer compile check passes; a new
production API image was not executed. Formatting and whitespace checks pass.

The owner tests use real signed checkpoint files, actual State/transport
initialization and destruction, successive reporter incarnations and durable
journal reopens. They inject remote fence/terminal ACKs; existing control/TCP
contracts cover authenticated evidence, not a live multi-process restart here.
Tests preserve publication history and closed worker floors, reject stale receipt
reuse and recovery manufacture, retain uncertainty without a receipt, handle
receipt-write failure and preserve proof across late allocation refusal. Initial
v1 missed required fixture authentication fields; v2 expected not-ready instead
of the existing stronger fenced-generation refusal; v3 passed that sequence; v4
adds allocation-safe ordering; v5 makes reconciliation and endpoint-borrower
teardown guards unconditional across optimization modes. A separate v3-source
direct execution reran the nine control cases; it is historical evidence, not
the final v5 input proof.

This supersedes the prior absence-of-any-completion-path statement for clean
shutdown. Crash/permanent-loss/external-work recovery, signed generation/process
reconciliation, complete heap/pin accounting, all remaining frontends/operators,
durable mutation completion, adaptive policy and the original default overload
proofs remain outstanding. Capability families remain disabled by default. Full
heavy qualification is still waived; the full design goal stays active.


### Inherited inference-worker generation lease (October 16)

On Linux/macOS, the real inference-worker spawn now acquires a nonblocking
exclusive file lease in the node data directory before spawning and explicitly
inherits that already-locked descriptor across exec. A replacement refuses
startup while an orphan retains the descriptor, including before its startup
acknowledgement. Neither a stale PID nor elapsed time authorizes replacement.
The parent closes its descriptor only after reap and resource retirement; it
never explicitly unlocks the shared open-file lock. Partial spawn/reader/RPC
initialization failures release the lease after child cleanup. Closing the
parent descriptor alone cannot release a living child's lease. Other platforms
retain their previous unqualified behavior.

`antfly-process-generation-fence-test` is included in the default Linux/macOS
unit target. Its four real subprocess cases cover parent SIGKILL before/after
startup acknowledgement, parent descriptor close while the child lives, and
idempotent close/reacquisition. The seven named worker/callback contracts cover
refusal before spawn, release after partial-startup faults, resource retirement,
streaming ABI, deadlines and physical H2 credit cancellation. The explicit
runtime filters reject empty discovery; the target roots directly at `worker.zig`.

`process-generation-native-v7` passes 28/28 build steps with unchanged native,
Python and schema inputs. All eleven cases executed afresh on macOS; native
artifacts and the API consumer compile check were cached from the same-input
v6 build. The v6 local TCP cases were sandbox-refused; v7 reran with local network
access. Earlier receipts preserve a build API typo, fixture EOF/sentinel-free
mistakes and the discovery correction, rather than treating those as passes.
The verifier binds exact case names and current source/log/runner hashes.

This is a physical worker-generation overlap fence. It is not yet complete
node/root-bound recovery evidence: descendant descriptor retention, all external
operations, signed generation reconciliation and crash/permanent-loss handling
remain to be established. It does not authorize `Registry.completeRecovery` for
an uncertain journal, change capability defaults, qualify Linux/current production
images, or complete the broader heap/pin/operator/adaptive/default-overload scope.


### Worker fatal boundaries bypass libc teardown (October 16)

The descendant audit found that RPC parent EOF and uncertain resource ownership
still used libc `exit`, which can wait forever on a native atexit/stdio lock.
Both production paths now use the shared `worker_lifetime` immediate-exit
callbacks. Resource-owner loss no longer signals a cooperative endpoint event
before terminating; kernel pipe closure wakes the parent, whose existing reap
still precedes reservation retirement. The owner-loss callback runs on the
independent RPC reader, so a blocked entry/native thread cannot prevent it.

Five new actual subprocess contracts include a negative control that enters a
wedged C exit handler, parent EOF, malformed RPC input, fatal resource-owner
loss and SIGKILL of the parent holding the worker's private pipe. The latter
requires the inherited file lease to become acquirable; PID absence and timeout
are not positive quiescence evidence. Fixtures install a six-second emergency
exit to bound interrupted negative tests. They execute the same production
lifetime callback through the real RPC reader, without initializing full model
backends; the resource-loss case directly invokes the production fatal helper.

`worker-fatal-lifetime-native-v2` passes 30/30 focused build steps: seven native
worker/callback contracts and nine Python subprocess cases, all freshly run on
macOS. The API consumer compile check is cached success, not a new production
image execution. Its verifier binds native/Python/schema/log/runner hashes and
checks that the production worker uses both shared fatal callbacks. Initial v1
passed all subprocess cases but exposed a Zig import-name collision in the worker;
v2 fixes it. These cases remain in the default Linux/macOS unit target.

Search results for inference `spawn` calls include test servers and standalone
compiler/benchmark tools; they do not prove worker-reachable descendants. The
shared AWS credential library can run the vendor CLI, and finetune tools can
launch arbitrary executables. Complete reachability, descendant retention and
external driver-operation retirement are still unproven. No whole-node crash
recovery authority, capability activation, Linux/current image qualification or
full-design completion follows from this worker fatal-path correction.


### Detached worker payloads retain shared capacity (October 16)

A host RPC response previously released endpoint byte credit on detach even
though the host still retained its bytes. Each production worker Client now
owns a ref-counted payload budget shared across its endpoint generations.
Receive allocation reserves shared credit before allocating; allocation failure
rolls it back. Detach releases only endpoint-local credit. The payload retains
its shared reservation until physical free and can outlive the Client anchor.
Atomic reservation avoids callbacks or I/O under the RPC mutex. Existing
ceilings remain 256 MiB for bulk and a separate 4 MiB small-message allowance;
resource-control frames retain their independent path. This closes the detach
escape without introducing waiters or increasing the previous per-endpoint
limits. The shared budget spans one Client's generations, not arbitrary new
Clients or every heap allocation in a node.

Worker HTTP replies now transfer the original response slab into a response
retirement owner rather than copying the body into an uncharged buffer. The
production HTTP response handle calls Response.deinit on destruction, releasing
the original credit. Body replacement conservatively retains the slab until
response destruction. Owner-allocation failure leaves payload ownership with
the caller. No public ABI, worker wire or HTTP response layout changed.

Four RPC regressions prove retained credit across actual pipe endpoint teardown
and replacement, small-response progress with bulk full, reclamation only after
old-result free, payload survival after the Client anchor retires, allocation
rollback and a single ceiling across eight concurrent endpoint allocators. Two
HTTP ownership regressions prove original body-pointer identity, charged lifetime
through response retirement and failure rollback. All thirty-four RPC/progress
contracts, nine worker/HTTP contracts and nine subprocess cases freshly pass on
macOS in `worker-payload-credit-native-v3` (32/32 build steps). The verifier binds
all fifty-two exact case names plus native/Python/schema/log/runner hashes and
production connections. The API consumer compile check is cached success; no
new production image or Linux execution is claimed. These tests are selected
by the default Linux/macOS unit target.

Initial v1 exposed a pre-existing 100 ms header-setup timing race in a streaming
fixture. The fixture now tightens its original two-second deadline after header
acknowledgement, before the physical H2 credit wait; it never extends the original
bound. V2 caught an erroneous second Response.deinit in the new fixture; v3 fixes
that fixture ownership. Production output/timeout semantics remain covered by
the same named deadline/cancellation contracts.

This is transport-slab accounting and HTTP body ownership. Provider JSON/numeric
copies, parser/container overhead, child/backend heaps and complete query/node
accounting remain open. The route audit has not established descendant/external
operation quiescence or root-bound crash-recovery authority. Capability families
remain disabled; all original operator/frontend/durable/adaptive/default-overload
requirements remain active. Full heavyweight qualification remains waived.


### Budgeted provider result conversion and ABI lifetime (October 16)

The worker Client's shared payload budget now supplies a freeing allocator for
provider-derived memory. It reserves bulk credit before each backing allocation
and retains the budget until that allocation is physically freed. Reallocations
must admit the old and new buffers together; backing refusal rolls credit back.
Small decoded rows and metadata use bulk credit, preserving the separate ordinary
small-message allowance. No new queue or quota increase is introduced.

Worker provider request/result arenas, JSON output copies, decoded f32 rows,
row-slice arrays, numeric ABI descriptors, response-state objects and capability
parse/rewrite temporaries now use this allocator. ProviderResult carries its
allocator with its owned bytes/rows. Production publication consumes ownership
on every path, preserves the allocator in the opaque response handle and releases
all derived credit through linkedInferenceDestroyProviderResponse. A response
can outlive the Client anchor while its allocations keep the budget alive.
Capability rewriting frees the original only after replacement publication is
ready. Quota or backing-heap refusal at this worker-provider boundary becomes
retryable capacity; invalid data and cancellation preserve their own causes.
Late cancellation after numeric descriptor allocation refuses publication and
unwinds rows/descriptors. JSON rewriting and numeric preparation check the
original controls again before publication. Native in-process provider output
allocation otherwise retains its prior allocator behavior.

`worker-provider-credit-native-v3` passes 33/33 focused steps and all sixty-three
fresh macOS cases: thirty-six RPC/progress contracts, eighteen worker/provider/
HTTP/borrowed-execution contracts and nine subprocess cases, with no skips,
failures or leaks. The production-entry semantic object exports a call to the
real linkedInferenceInvokeProvider, forcing analysis of the provider path rather
than just its helper types. It compiles freshly; it is not a linked production
image or a model-execution proof. The API consumer check remains cached success.
The verifier binds exact source-derived inventories and current native/Python/
schema/log/runner hashes plus production allocator/entry-point connections.
All cases and the semantic check remain in the default Linux/macOS unit target.

Tests exercise retained JSON pointer identity through the real ABI handle,
numeric row values and descriptors, caller-copy survival after handle destruction,
quota refusal halfway through row decoding, malformed numeric cleanup, publication
allocation refusal, cancellation, response survival after anchor retirement,
capability constraints and every backing-allocation failure in the capability
rewrite path. Allocator regressions preserve control progress with bulk full,
rollback real backing refusal and enforce reallocation peak. V1 establishes the
initial provider/allocator cases, v2 adds capability failure coverage and production
semantic analysis, and v3 adds the late-cancellation publication guard.

This extends one Client's transport budget to its host-side provider conversions;
it does not yet charge all node/child/backend/query heaps to the scheduler's
node pool. Other RPC/header/container allocations, fixed owner metadata, all
in-process provider heaps, every frontend/operator and node-wide lifetime sharing
still require work. Whole-node crash/permanent-loss/external-operation recovery,
durable completion, adaptive policy and original default overload proofs remain
open. Public ABI/wire layouts and capability defaults are unchanged. No Linux or
current production-image qualification is claimed; heavy qualification remains
waived and the full goal stays active.


### Worker host bytes join the node memory owner (October 16)

Production Client configuration now requires the node's transient-host memory
hook. One stable identity per Client budget represents the combined live
transport slabs and provider-derived allocations. Every growth is admitted to
the existing ResourceManager inference-scratch slice and aggregate host ledger
before backing allocation; backing refusal rolls both ledgers back. The manager's
installed parent participates in the same admission, so sibling storage, query
and model reservations cannot each claim independent node headroom. No admission
lease or registry entry is created per decoded row.

The Client budget retains a separate node capability until its last allocation
is physically freed, including after Client destruction. Physical free precedes
node-credit return. Validated shrink remains possible after limits tighten.
Admission uses a nonwaiting attempt on its per-Client accounting gate; contention
refuses the allocation without changing node ownership. Retirement conservatively
retains credit until the existing synchronous node ledger accepts the decrease;
an accounting-mismatch refusal is fatal rather than silently losing debt. Node
callbacks must be synchronous ledger operations and must not reenter the RPC
endpoint or Client budget. This does not prove a hard return bound for every
upstream ledger lock or callback.

The internal inference-host ABI is now 38. ResourceBudget has an optional typed
transient-host tail; server worker configuration requires it, and the outer ABI
size/version gate rejects older layouts. In-process/Lite configuration retains
its previous allocator behavior. API ABI 48, local-inference ABI 4 and worker
wire version 11 are unchanged. Standalone installs the real hook and refuses
resource-owner destruction while transient bytes or retained contexts remain.

`worker-node-memory-native-v4` passes all 82 focused build steps and seventy-two
fresh macOS cases: thirty-six RPC/progress, nineteen worker/provider/ABI/control,
seven real node-ledger contracts, nine subprocess cases and one production
standalone callback/owner-lifetime contract. Node cases use actual ResourceManager
reservations and a real shared parent to test query/model/storage competition,
concurrent Clients, stale identities, backing OOM rollback, tightened limits,
anchor retirement and nonwaiting contention refusal. The final standalone case
executes the actual C callback and InferenceResourceBudgetOwner teardown, rather
than only a parallel fixture. The provider semantic object compiles freshly;
the API consumer check remains cached success. No linked production image or
model execution is claimed. Both focused targets are in default native unit
selection; the verifier binds exact names and native/Python/schema/log/runner
hashes, ABI 38 and production wiring.

V1 exposed a fixture's nonexistent query slice; V2 passes the real node cases
using the dense-search working set. V3's standalone test reused a module whose
composition links full storage/inference archives; the build planner refused
those declared 20–28 GiB steps under the focused 16 GiB limit before qualification
ran. V4 uses the existing unlinked standalone control root, executes the same
production callback and adds immediate contention refusal. Heavy qualification
remains waived; this change does not request a larger builder.

This admits the implemented parent-side transport and provider allocations to
the node ledger. Child RPC/general heaps, backend/process reservations not yet
covered by their existing contracts, remaining RPC/header/container/fixed owner
metadata and complete frontend/operator/query accounting still require work.
Whole-node crash/permanent-loss/external-operation proof, durable completion,
adaptive policy and the original default fairness/overload profiles remain
outstanding. Capability defaults stay disabled. No Linux/current production-image
qualification or full-design completion follows from the focused tests.

### Child RPC slabs and request heaps join the node owner (October 16)

The parent now reserves node memory for each incoming child payload before
publishing its offer. A fixed 256-entry table retains that grant through response
delivery, cancellation and uncertain transport failure. The child reports
retirement only after physically freeing the receive slab. Unknown retirement
IDs close the transport without refunding unrelated credit. Confirmed rejection before allocation
can refund its receive grant; EOF alone cannot. The parent's physical
kill-and-reap fence releases any remaining grants.

Child request/create arenas, serialization and generated output slabs now use a
retained allocator backed by dedicated resource RPC. Its growth is admitted by
the actual node ledger before backing allocation, using one stable identity per
worker generation. Free releases node credit after physical allocator free.
Accepted credit survives a lost reply until reap. Invalid replies, uncertain
transport outcomes and refused retirement exit immediately, bypassing libc
teardown; the surviving parent retains responsibility for physical quiescence.
The same parent apply/retire functions serve production and the real-ledger test.

Growth parks an existing borrowed execution scope and rejoins it before backing
allocation. Mandatory free does not depend on reacquiring ordinary execution
capacity. Resource replies and payload-retirement notifications use the existing
dedicated control path. Allocation/free that can perform resource RPC runs
outside the endpoint reader mutex, so the reader can route its replies.

Private worker framing advances to AFW5 and worker wire version 12. C ABI versions
remain API 48, inference-host 38 and local-inference 4. Old worker processes fail
the framing/version gate rather than silently omitting retirement accounting.

`worker-child-memory-native-v5` passes all 82 focused build steps. The verifier
matches seventy-eight named cases: thirty-nine RPC/progress, twenty
worker/provider/ABI/control, eight actual node-ledger, ten subprocess lifecycle
and one production standalone callback/owner-lifetime case. It binds 4,614 native
and Python inputs, 32 schema inputs, the runner and log hashes. The new actual
node-ledger test exercises the same parent helper as production, dedicated child
heap RPC, query/storage pressure, capacity refusal/retry and retained response
retirement. Existing rejection and mid-transfer cancellation fixtures now enable
peer grants and assert refund while preserving nested-call progress. Additional
cases gate physical receive free, reject unknown receipts, bound zero-byte grant
metadata and inject a malformed heap reply into a real subprocess with wedged
libc teardown. The production child entry point is semantically compiled; full
model execution and a current linked production image remain unqualified.

V3's first shared-helper extraction caught two value-versus-pointer compilation
errors. V4 corrected them; V5 extends rejection/cancellation coverage. Earlier
receipts remain historical. No failure, skip or leak remains in the final cases.
These changes cover child receive slabs and the selected handler-owned heaps.
Reader/control bookkeeping, remaining RPC/header/container and fixed owner
metadata, backend/process reservations outside existing contracts and complete
frontend/operator/query accounting still need work. Whole-node crash and
external-operation quiescence, durable completion, adaptive policy and the
original default fairness/overload profiles remain outstanding. Full heavyweight
qualification remains waived; current Linux/production images and full-design
completion are not claimed.

### Worker fixed control state and startup admission (October 16)

Resource-frame dispatch now reuses 128 inline task slots per endpoint instead of
allocating a task for each callback. A slot stays occupied through callback and
physical reply write, then becomes available under the endpoint mutex. Duplicate
IDs and slot exhaustion retain the existing fail-and-reap behavior. Ordinary
payload quotas and endpoint allocator exhaustion cannot consume these slots.
Runtime task/stack creation still has its independent failure contract.

The parent serializes resource parsing under its existing resource mutex using
8 KiB of fixed scratch stored in the Worker. Resource request and operation JSON
parsing no longer require the parent heap. The one-KiB resource-frame limit still
applies; parsing failure fences the worker before the resource operation runs.
Existing lease-map growth and new cache-observation metadata can still allocate
before their respective mutations and remain separate work.

Before allocating or spawning a generation, Client reserves the platform-specific
sum of `@sizeOf(Worker)`, `@sizeOf(Child)` and the child's PayloadBudget from the
same authoritative node transient-memory hook. This covers the newly fixed
resource task tables and parent parser scratch along with those objects' other
inline fields. The identity lives in the Client, outside the generation being
allocated. Refusal occurs before Worker allocation. Failed startup unwinds the
physical endpoint/process and object first, then returns the reservation. Normal
generation replacement likewise joins/reaps and destroys the old Worker before
returning its baseline; a disconnected endpoint cannot make it reusable early.
This is accounted fixed-state memory, not a whole-process RSS bound.

`worker-control-memory-native-v2` passes all 82 focused build steps, with
80 named case results: forty RPC/progress, twenty-one worker/provider/control,
eight actual node-ledger, ten subprocess lifecycle and one production standalone
callback/owner-lifetime case. The verifier checks exact names and binds 4,614
native/Python files, 32 schema files and runner/log hashes. New cases deny every
endpoint allocation while repeated resource replies still complete, and reject
startup before backing allocation then verify rollback after backing OOM. The
actual Worker resource parser also runs with its owner/endpoint allocator denied.
The partial-startup process fixture verifies fixed credit is still held when
childKill begins and is zero after each joined failed-startup path. V1 caught a
fixture's omitted node hook; V2 adds the hook and physical-cleanup assertions.
The production child entry point is semantically compiled; unchanged node-owner
artifacts may be cached, and no current linked model/production-image run is
claimed. API 48, inference-host 38,
local-inference 4, worker wire 12 and AFW5 framing are unchanged. No new worker
message or external ABI layout is needed. Remaining Client/configuration bytes,
dynamic maps and observations, runtime task/stack/allocator overhead, native
backend heaps and complete query/operator accounting are not covered by this
baseline. Whole-node physical recovery, durable completion, adaptive policy and
the original default fairness/overload profiles remain outstanding. Full
heavyweight qualification stays waived; current Linux/production images and the
full original design remain unqualified.

### Worker lease and cache metadata ownership (October 16)

Each generation now owns a separate PayloadBudget with a 1 MiB derived-allocation
ceiling and no ordinary small-message allowance. It retains the node context and
uses the same authoritative transient-host adjustment hook before backing
allocation. Lease-map storage, observation-pointer arrays and stable observation
objects allocate and free through this budget. Ordinary payloads cannot consume
its private allowance; both allowances still enforce the same aggregate node
limit. Existing map/list capacity remains charged until physical container free,
including after its last entry is removed. Reallocation charges the simultaneous
old and new allocations. This is a separate ceiling, not an extra node envelope
or a promise of fresh allocations after aggregate node exhaustion.

Lease tracking capacity is acquired before reserving a model admission lease.
Metadata quota/backing refusal returns explicit temporary capacity failure
without calling reserve. New cache observation capacity and its object are owned
before the upstream callback. The object is published only after callback
success; denial frees the unpublished object, leaving only accounted reusable
container capacity. Retirement and reaped cleanup use the original allocator and
need no fresh metadata allocation. Startup baseline now includes two
PayloadBudget objects: the child request heap and parent resource metadata.
Metadata context release and physical object destruction precede baseline return.

Cache refusal semantics are distinct. Tokenizer growth has a refusal path before
publication. Prompt-cache updates report memory already retained through a void
observer; ignoring a refusal would let the child continue with unreported bytes.
The production child now takes its immediate fatal ownership boundary for any
refused prompt-cache observation, and for a refused shrink of either cache type.
The parent joins/reaps before returning its surviving recorded reservations.
This closes continued execution after an observation failure; it does not turn
observational backend caches into strict reserve-before-allocation accounting.

`worker-metadata-memory-native-v3` passes all 82 focused build steps, with 83
named results: forty RPC/progress, twenty-three worker/provider/control, eight
actual node-ledger, eleven subprocess lifecycle and one production standalone
callback/owner-lifetime case. Its verifier binds exact names, 4,614 native/Python
files, 32 schema inputs and runner/log hashes. Metadata checks refuse before
model/cache callbacks, retry after capacity returns, avoid object debt on repeated
callback denial, and release under exhausted metadata/node/ordinary quotas. The
cache-policy case checks growth versus retirement for both cache types. A real
subprocess sends a prompt-cache observation across the resource pipe, receives a
valid capacity denial with stdin still open, then invokes the exact production
fatal-observation helper. It exits with status 1 without entering the deliberately
wedged libc atexit handler. This tests the failure boundary, not full model/cache
execution. V1 establishes metadata behavior, V2 adds the cache-refusal policy,
and V3 shares the production helper with the subprocess proof. Provider/child
entry semantic compilation passes; unchanged node-owner artifacts can be cached.
No current linked production/model-image run is claimed. API 48, inference-host 38, local-inference
4, worker wire 12 and AFW5 framing are unchanged. Client/configuration memory,
remaining endpoint maps/messages, runtime task/stack/allocator overhead, native
backend heaps, complete query/operator/pin accounting and the elapsed cost of
observation lookup remain separate work. Whole-node crash/permanent-loss and
external-operation proof, durable completion, adaptive policy and original
default fairness/overload profiles remain outstanding. Full heavyweight
qualification stays waived; current Linux/linked production-image qualification
and completion of the original design are not claimed.

### Worker RPC transport metadata ownership (October 16)

RPC pending/active/offer/incoming maps, Request objects and queued Message
containers now use an explicit transport metadata allocator. This allocator is
separate from resource lease/cache tracking and from payload bytes. Allocation
provenance is preserved through removal, cancellation, reader shutdown and
endpoint destruction; reusable container capacity stays charged until physical
free. Empty events consume metadata credit even when their payload uses zero
bytes. The existing 1,024-message queue ceiling now has a checked finish guard
as well as offer admission, rather than relying solely on a debug assertion.

The parent's generation owns a separate 1 MiB transport PayloadBudget, using the
node transient-host hook before backing allocations. The child uses a local
2 MiB transport PayloadBudget without synchronous peer callbacks. Parent startup
precharges that entire child allowance to the same node ledger before spawning;
it remains held through child reap and endpoint/object destruction. The reader
therefore never needs to issue a resource RPC to allocate bookkeeping needed for
routing resource replies. Growth of incoming-map storage is admitted before the
receiver allocates or accepts a slab; refusal rejects the offer without creating
a retirement receipt. Caller-side map refusal returns temporary capacity before
publication. Dedicated resource tasks/replies still use their existing fixed
storage and remain independent of ordinary transport metadata exhaustion.

The baseline is now `@sizeOf(Worker) + @sizeOf(Child) +
4 * @sizeOf(PayloadBudget) + child_metadata_limit`: child request and transport
budgets plus parent resource and transport budgets, their inline owners and the
precharged child transport allowance. Parent dynamic allocations remain charged
by actual live bytes; the child allowance is conservatively reserved in full.
This is a logical owned-memory reservation, not a whole-process resident bound.

Private worker wire advances to 13 and framing to AFW6 so older child processes
cannot silently bypass the reader metadata ceiling. API 48, inference-host 38
and local-inference 4 remain unchanged.

`worker-transport-memory-native-v4` passes all 82 focused build steps, with 85
named results: forty-two RPC/progress, twenty-three worker/provider/control,
eight actual node-ledger, eleven subprocess lifecycle and one production
standalone callback/owner-lifetime case. The verifier binds exact names, 4,614
native/Python inputs, 32 schema inputs and runner/log hashes. New pipe tests
refuse caller metadata before dispatch, reject receiver metadata before slab
allocation, retry after limits recover, retain physical container charges until
endpoint destruction, and fill all 1,024 empty-event slots while dedicated
resource replies still complete. Parent metadata quotas are not consumed by
those zero payload bytes; the queue's real container storage is charged.

V1 caught the startup fixture's single-identity mock, V2 caught its Zig field
ordering, and V3 corrects both. V3 also observed one existing streaming-deadline
case timing out. The same binary passed that case in about 114 ms in isolation;
V4 passes the full suite on identical native/schema inputs without changing any
deadline. The timeout's cause remains unproven and requires control-timing
investigation before default qualification. The child/provider entry compiled
freshly in V3 and is cached success in V4; the verifier checks identical inputs
and the fresh compilation evidence. API consumer and unchanged node-owner
artifacts may be cached. No linked production/model image is executed. Remaining Client/configuration/environment
storage, caller-owned Pending/Offer stack frames, runtime task/stack/allocator
overhead, native backend heaps and complete query/operator/pin accounting still
need work. Observation lookup/queue draining elapsed bounds, whole-node physical
recovery and external-operation proof, durable completion, adaptive policy and
the original default fairness/overload profiles remain outstanding. Full
heavyweight qualification remains waived; current Linux/linked production-image
qualification and completion of the original design are not claimed.

### Acknowledged sink errors survive request cleanup cancellation (October 16)

The previously intermittent streaming-deadline failure is reproduced and fixed.
Before the fix, repeated execution of the unchanged test failed on repetition 7.
A deterministic actual-pipe regression then forced request cancellation before
its sink acknowledgment arrived: the child returned Cancelled instead of the
parent's acknowledged Timeout. The request cleanup cancellation could overwrite
the typed sink outcome, so the child callback never reported its expected timeout.

The child still waits for the parent's physical acknowledgment before returning.
Once that acknowledgment exists, timeout or denial takes precedence over generic
request cancellation. An independently recorded non-cancellation child failure,
including its original deadline, remains authoritative. Successful acknowledgment
still rechecks both the event control and request cancellation; it cannot resume
canceled computation. No deadline, waiting limit, wire format or ABI changed.

The deterministic regression fails against the old loop with expected outcome 1
(timeout) versus actual outcome 3 (cancellation). After the fix it covers timeout,
denial and successful-sink cancellation orderings, with no child computation
resumed. `worker-event-cause-native-v2` passes all 82 focused build steps and 86
named results: forty-three RPC/progress, twenty-three worker/provider/control,
eight node-ledger, eleven subprocess and one production standalone owner case.
The original physical H2-credit deadline case passes 100 consecutive repetitions;
the deterministic acknowledgment regression also passes 100. Repeat receipts bind
the binaries and unchanged native/schema inputs, while the verifier binds exact
case names, 4,614 native/Python files, 32 schema files and the red/green/repeat
logs and runner hashes. The provider/child entry compiles freshly; the API consumer
check remains compile-only cached success. These are macOS focused checks, not
Linux, linked-model execution or full default-load qualification.

This resolves the reproduced cleanup-cancellation cause of the preceding timeout
report; it does not prove every control schedule. Remaining Client/configuration
and marshaling heap ownership, runtime task/stack overhead, native backend and
query/operator/pin accounting, complete physical crash/permanent-loss recovery,
durable completion, adaptive policy and the original frontend/default fairness
profiles remain in scope. Full heavyweight qualification remains waived and the
full design goal remains active.

### Parent worker HTTP marshaling and response descriptors (October 16)

Parent worker HTTP dispatch now requires a configured node budget before reading
or allocating request metadata. The host's target/request/header/parameter copies
and borrowed-executor/context allocations use the Client result allocator. The
worker's request arena, header/parameter descriptors, JSON marshaling and reply
parsing use the same allocator. Event decoding uses a freeing allocator rather
than accumulating every frame in a long-lived arena. Controlled child event JSON
now uses the endpoint compute allocator, matching uncontrolled event serialization
and entering the child heap's node admission path before backing allocation.

Returned parent worker HTTP response headers and the response retirement owner
also allocate from that budget. The owner retains allocator lifetime even for a
static error body after all headers are removed. A received body keeps its
original slab; the owner holds that payload through response destruction. The
production HTTP ABI publisher now shares a testable consuming function: response
state and header-view descriptors use the selected allocator, and the public HTTP
response destructor frees them after response cleanup. No response handle is
published until all allocations succeed. Worker-path allocation refusal becomes
temporary capacity at the HTTP boundary; other causes retain their identity.
Provider marshaling now uses the same configured-budget preflight.

`worker-http-memory-native-v2` passes all 82 focused build steps and 89 named
results: forty-three RPC/progress, twenty-six worker/provider/control, eight
actual node-ledger, eleven subprocess and one production standalone owner case.
The verifier checks exact case names and binds 4,614 native/Python inputs, 32
schema files and runner/log hashes. The worker test inventory is compared as an
exact named set because additional HTTP body analysis changes import discovery
order. Explicit semantic entry checks compile the production HTTP, provider and
child entry points; unchanged node/API artifacts can be cached.

New pipe checks deny every Client raw allocation, reject marshaling at zero quota
before dispatch, retry with capacity, preserve decoded headers/body and retire the
endpoint and Client budget anchor while the response remains alive. They then
exercise header replacement, the exact production ABI publisher and its public
destructor, with all bytes/context edges returned. The same case clears all
headers of a static-body error response after anchor retirement and uses its
allocator before destruction. A second contract rejects unconfigured HTTP and
provider marshaling before reading an undefined request. A third denies controlled
event serialization at zero compute quota, then observes charged JSON while the
physical acknowledged callback runs and verifies retry on the healthy transport.
V1 covers marshaling/ABI lifetime; V2 adds the static-body lifetime schedule and
explicit HTTP entry semantic compilation. No ABI or worker-wire change is needed.

Lazy body bytes remain borrowed from their existing caller/transport owner; this
is not a complete execution working-set reservation. Child HTTP route/backend and
in-process/Lite scratch still use their separate allocation contracts and need
audit. Client/create/configuration/environment storage, runtime task/stack and
allocator overhead, native backend and query/operator/pin ownership, complete
physical crash/permanent-loss recovery, durable completion, adaptive policy and
the original default fairness/overload profiles remain in scope. No current
linked production/model image or Linux envelope is qualified here. Full
heavyweight qualification remains waived; the full design goal remains active.

### Managed child HTTP context and publication scratch (October 16)

The supervised child now installs its existing node-backed request heap as the
linked host state's HTTP allocator immediately after creation and before configure
or HTTP dispatch. Host HTTP reconstruction selects the parent Client allocator
for a parent worker state, the installed HTTP allocator for a child state, or its
previous allocator when neither is present. Target/request/header/parameter copies,
route context allocations made through its supplied allocator and HTTP ABI
response state/views therefore share the child's existing transient-host ledger.
The state/model allocator and model residency reservations are unchanged; this
avoids replacing the model-weight allocator with request-heap accounting.

Managed child HTTP allocation refusal maps to temporary capacity, preserving other
error causes. Publication still consumes every response on failure, exposes no
handle before descriptors are ready, and releases response-owned memory before
its final descriptor allocation. Published response allocations retain their
budget independently of the initial scratch-owner anchor. The child heap and its
resource RPC context remain alive for the process generation, and the existing
physical reap fence owns uncertainty at process loss.

`worker-child-http-memory-native-v1` passes all 82 focused build steps and 90
named results: forty-three RPC/progress, twenty-seven worker/provider/control,
eight node-ledger, eleven subprocess and one production standalone owner case.
The verifier binds exact named inventories, 4,614 native/Python files, 32 schema
inputs and runner/log hashes. The production HTTP/provider/child semantic entry
checks compile freshly; unchanged node/API artifacts may be cached.

The new model-free route executes the exact public HTTP ABI entry and destructor
with the legacy state allocator configured to refuse every allocation. It checks
request headers, body, route parameters and decoded query data using the supplied
scratch allocator. Zero quota refuses before the handler. Tightening quota after
the handler builds its response refuses descriptor publication and frees all
scratch without a handle. Cancellation after dispatch likewise cleans up before
publication. Successful dispatch returns status/body/headers; the response remains
valid after owner-anchor retirement and its public destructor returns all tracked
bytes/context edges. A real model is not loaded by this test.

The installed allocator applies to HTTP context/provided-allocator allocations;
it does not intercept backend code that uses independent allocations. Direct
in-process/Lite states retain their existing contracts and remain a separate
accounting audit. Client/bootstrap/configuration memory, runtime task/stack and
allocator overhead, complete backend/query/operator/pin ownership, physical
crash/permanent-loss and external-operation reconciliation, durable completion,
adaptive policy and original default fairness/overload profiles remain in scope.
No ABI or private worker-wire layout changed. Linux/current linked production
images and full default-load qualification are not claimed. Full heavyweight
qualification stays waived; the original design goal remains active.

### Dispatch scan bound and whole-design audit (October 16)

The initial bounded selector checked the class head plus at most eight younger
entries in both its eligibility probe and deficit-round search. The continuation
change below splits this into two independently bounded windows. The separate bypass-count
limit and paid-team drain behavior remain in force. A deeper fitting operation
waits until earlier entries retire into the bounded window. Shared and isolated
lane contracts verify protected reads, controls and dedicated finite cleanup
progress. `scheduler-lookahead-native-v3` passes all 103 named contracts (94
scheduler and nine admission), with current native/C/schema input hashes. No ABI
or config field changed.

The whole original 731-line design is audited in the implementation record's
"Whole-design coverage audit and scheduler lookahead" section. It keeps atomic
CPU/memory/I/O bundles, age barriers, general measured service, physical recovery,
durable backlog transfer, adaptive execution and full frontend/default behavior
explicitly open. Automatic startup sizing is not closed-loop adaptation. Full
heavyweight qualification remains waived; the broader goal remains active.


Prepared memory batch admission now preinstalls ownership metadata before a
nonblocking, allocation-free all-slices/aggregate/ancestor acquisition attempt.
This is a resource-manager component, not an enabled CPU/memory/I/O scheduler
bundle. Continuation progress must be established independently of the bounded
fresh-start lookahead before integration. See the October 16 prepared-memory
entry in `WORKLOAD_POOLING_IMPLEMENTATION.md` for evidence and remaining scope.


### Independent continuation candidate windows (October 16)

Each CPU class now has separate intrusive queues for fresh starts and retained
continuations. Both eligibility and deficit selection merge the two windows by
enqueue sequence. Each window visits its head plus at most eight younger entries,
so a class visits at most eighteen candidates per pass regardless of queue size.
Both queues share the existing class credit, bypass policy, CPU partitions,
operation count, retained-byte accounting, cancellation and original deadline.
A continuation can be discovered behind a full fresh-start window without an
unconditional priority over older fitting starts. Conversely, fresh starts remain
visible when the continuation window is full of CPU demands that do not fit.

First grants, parked-owner reentry, and cleanup transfer select continuation
queues. Reserved children already owning external resources also enter through
the continuation queue on their first CPU return. Queue-head checks for CPU loans,
class weights, barriers, control/cleanup dispatch and unlinking include both
populations. Internal scheduler/entry metadata changes; public runtime ABI and
configuration layouts do not change. This enables continuation visibility for
existing scheduler users, but does not yet establish resource-aware barrier
progress or an atomic CPU/memory/I/O grant.

The current-source evidence is `scheduler-continuations-native-v3` and
`prepared-memory-continuations-native-v1`: 107 scheduler/admission cases and ten
prepared-memory cases were run directly and their exact names matched source
declarations. The memory build reused its unaffected cached binary; direct
execution provides fresh case evidence. The verifier binds both receipts to
4,685 identical native/schema input hashes. See the implementation record for
coverage and the remaining full-design requirements.


### Native resource-aware grants (October 16)

Trusted owning executors can now attach a prepared non-CPU resource participant
before initial admission or at a parked incremental boundary. A non-retaining
probe precedes an all-or-nothing acquisition at the paid, CPU-fitting grant point.
Pending resource barriers allow existing continuations to retire ownership while
restricting new starts; unknown ancestor headroom cannot suppress that progress.
CPU-only team drain semantics and shared class credits remain covered by the
existing 107 scheduler/admission contracts. The bounded-read progress path uses
both bounded windows when its head waits for resources.

The concrete native adapter atomically grants memory and dense scan bandwidth
before CPU publication, preserves legacy bandwidth FIFO, and retains its leases
through CPU parking until the caller joins physical users. Its memory identity
fences a duplicate bandwidth refund. Execution capsule entry/incremental requests,
original timeout/cancellation, shared ancestors, protected cleanup after close and
isolated competing barriers are covered by thirteen new contracts plus three
existing bandwidth contracts. Direct execution also reran the ten prepared-memory
cases: 133 exact named cases pass on 4,687 current native/schema inputs.

This native component does not enable bundle admission for existing production
operators or opaque borrowed executors. Actual allocator credit consumption,
private-owner forwarding, other I/O families, measured service, age barriers and
the broader original design remain open. See the implementation record's native
joint-grant entry and current receipt/verifier for precise scope.


### Consuming granted memory in physical allocators (October 17)

The native bundle now provides per-slice allocation views sharing its granted
credit. Physical allocation/growth is checked before backing calls, backing OOM
rolls counters back, and realloc counts old plus new until physical free. This
uses the existing reservation and does not reserve twice. Views reject ungranted
slices, copied quota owners and identities from earlier requests at reused
storage. Retirement fences allocation and retains the strict memory/bandwidth
leases while buffers or backing allocations remain in flight; a backing allocation
completing after close is freed before pointer publication.

Six actual-buffer contracts supplement the native joint-grant suite, including
physical callback observations and deterministic threaded retirement with a
competing requester. All 139 named scheduler/admission, joint/bandwidth/allocator
and prepared-memory cases pass in direct current-source runs. See the October 17
implementation record and `runnable-bundle-allocator-verified.json` for evidence.
Actual production operator wiring and opaque-owner forwarding remain next; this
does not reassign HBC's retained cache buffers to an operation or close the broader
original design/qualification requirements.


### Resource forwarding across opaque owners (October 17)

Runnable ABI 15 now carries versioned resource descriptors through initial API
owner start and parked Owner/Borrowed requests. Native read bridges and local read/
child receivers snapshot the callback values under the same original admitted
operation; no transient descriptor-frame pointer or fresh admission is retained.
Reserved children may attach before first CPU activation. Missing capability,
retained-policy failure, shutdown, old/short prefixes and invalid integer callback
results fail closed. Accounting mismatch retains its stable internal status across
the ABI. Native initial start requires an advertised resource capability.

Current evidence is 266 passing direct case executions (252 unique names) across
eight focused binaries plus the API-owner export semantic compile check, bound to
4,690 native/schema input hashes and the original full design. The implementation
record and `runnable-resource-forwarding-verified.json` give exact inventories and
scope. This enables native owner/read/child forwarding; actual production operator
and allocator wiring, other resource owners, age protection, measured service and
the remainder of the full design are still open.

### Production SQL numeric scratch with the original CPU continuation

The SQL coordinator now prepares an optional fixed scratch participant, parks its
existing root, and reenters through a CPU-plus-resource grant. The paid acquisition
attempt uses bounded API-partition/node accounting; refusal selects the valid
serial alternative with zero incremental scratch. Physical allocation consumes
reserved credit directly, and buffer free precedes credit release. Participant
metadata and preparation/coercion allocations use the existing bounded allocator.

The native synchronous transition detaches temporary callback metadata under the
scheduler lock on every return. Policy/resource refusal retains the original root
for cleanup, with no callback remaining to its retired stage. Queue checks use
request lifetime only; the running coordinator validates full original read policy
before arithmetic. Borrowed-owner temporary transitions require a detachable
capability and currently refuse before parking. Control and retained phases also
refuse before parking. This does not enable CPU loans or dense I/O charges.

API control ABI is now 49 for the expanded accounting callback layout; runnable
ABI 15 is unchanged. Receipt `sql-scratch-bundle-verified.json` binds 163 case
executions (158 explicitly named and five import checks), all 4,690 current
native/schema inputs, source snapshots and original design identity. This covers
the fixed numeric buffer; remaining operators/resources, measured service/age
policy, adaptive defaults and distributed/durable ownership remain incomplete.

### Original-clock aging and paid continuation progress

Blocked heads use both the existing bypass-count limit and an age threshold of
`min(25 ms, max_wait_ms / 4)`. The owning waiter's existing bounded poll validates
original authority and samples its clock outside the scheduler lock. It records
age protection for dispatch; each actual enqueue resets that flag, while probes
and notifications preserve queue age. Barrier order uses enqueue sequence across
unrelated clock origins. Fitting requests retain ordinary weighted selection.

A paid large admission retains its deficit round and reserved credit while CPU
capacity drains. A bounded progress cursor admits retained continuations using
the same class credits and debt; work in the other protected read partition
retains its reserve. Direct barrier grants spend their class credit, and an
eligible barrier repays retained debt before CPU acquisition. Physical resources
remain with their actual owners until buffers and work are retired.

Final receipt `scheduler-age-verified.json` verifies 173 case executions (168
named plus five import checks), exact source inventories, all 4,690 native/schema
inputs and the original full-design identity. Existing team fairness, CPU loans,
SQL numeric scratch, native forwarding and joint-resource regressions pass.
General measured service, complete operator/resource coverage, adaptive execution
sizing and production default qualification remain open under the original scope.

### Native estimated and measured lease service

Every managed ordinary CPU grant receives a bounded slot-quantum estimate. Its
successful wait acknowledges active service, and owning yield/park/resize/finish
and phase transfer settle observed elapsed time at the original slot width and
class. Host clocks are sampled outside owner locks. Parked, queued and wake time
is excluded; unusable samples conservatively retain their estimate.

Class corrections retain fractional refunds and overrun debt across roots and
reclassification. Refunds replenish the same deficit round, including cooperative
yields; debt is repaid by future ordinary work. Both signed correction and credit
arithmetic are bounded, with native counters for clipping and unobserved work.
Paid-team reserved credit, protected progress and existing bounded-read demotion
debt preserve their separate contracts. Completion/cleanup clock changes settle
the foreground observation first.

Current proof `measured-service-verified.json` binds 182 executions (177 named
plus five import checks), exact source inventories, current 4,690 native/schema
inputs and the original design. The measured-share test exercises both fresh and
continuously yielding roots at 0.25 and 4 quanta of actual work. SQL peer progress
is checked after actual simulated quantum exhaustion. Opaque-owner metrics,
complete phase/operator sampling, calibrated production fairness/default profiles
and runtime adaptation remain required under the full original scope.

### Coherent versioned service snapshots across the API boundary

API ABI 50 carries a versioned service payload within runnable/workload stats.
The native snapshot captures policy, ownership and all service counters under
one lock, then projects the signed ledger into unsigned debt/rebate gauges.
The fixed six-class C payload includes version, size, model and availability.
The opaque getter checks table/call/output contracts and service compatibility;
older contexts cannot read or overwrite the expanded output.

Metrics publish availability and quantum plus eight bounded six-class service
families. Unsupported telemetry produces an availability-zero sample without
usage series. Malformed payloads fail before snapshot rendering. Availability
advertises format support; unobserved/clipped signals and phase coverage still
matter before controller use. Native slot-time is an active execution-lease
observation rather than a physical CPU utilization or billing measurement.

`service-telemetry-verified.json` binds 211 executions covering 200 distinct source
cases (11 intentional core/API overlaps), all 4,690 native/schema inputs, exact
inventories, source snapshots and original design identity. Actual API exports,
old/truncated boundary refusal, the opaque getter and nonzero metric families
pass along with scheduler/SQL/resource regressions. Remaining worker/phase
coverage and fixed-mode profiles precede runtime adaptive policy work.

### Prepared-transaction file eligibility

The explicit local activation method permanently publishes reader floor and
catalog envelope version 5 before DB-owned required BEGIN can create a record.
The gate reads durable eligibility with the original admission control and
preserves rejections even when a metadata helper normally treats NotFound as
absence. Opaque/orphan preparation metadata and ordered catalog authority require
migration; failed activation leaves the file unchanged. Publication reuses the
atomic catalog upgrade and its permanent replacement baseline. Native ABI 33 and
storage owner ABI 101 cover this API addition. Local activation does not enable
normal distributed BEGIN/PREPARE, negotiate peers or reserve future completion.
See `native-prepared-file-gate-v2-verified.json` for exact qualification scope.
