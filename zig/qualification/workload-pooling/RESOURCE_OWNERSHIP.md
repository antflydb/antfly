# Pooling resource ownership and acquisition order

This ledger describes the native standalone/data-server integration. Embedded,
serverless and caller-injected owners retain their own contracts. A managed byte
ceiling is a reservation limit, not a resident-set ceiling.

## Memory owners

| Owner | Parent / accounting | Ceiling and release boundary |
| --- | --- | --- |
| Storage caches, search/apply working sets | Storage-owner ResourceManager | Detected/clamped memory envelope plus existing per-slice limits; actual reservation/observer retirement |
| Model/tokenizer/KV/scratch reservations | Local ResourceManager borrowing the linked storage context's node memory owner | Existing per-slice limits plus shared parent ceiling; allocation-free callbacks charge growth once and return actual releases |
| API foreground scratch, plans, retained readers and transaction registry | Storage-owner ResourceManager external bytes | `admission.max_request_memory_bytes` less control reserve; provenance headers/alignment included; resize/free returns storage-owner charge |
| API control scratch and SQL retirement | Pinned storage-owner ResourceManager external reservation | Reserve `min(16 MiB, request ceiling / 8)` before publishing API; foreground cannot borrow it; release only after API workers, cursors and stores join |
| API sessions, jobs, caches and owned durable session heap | Storage-owner ResourceManager external bytes | `admission.max_retained_memory_bytes`; stable allocator until actual destruction |
| HTTP decoded context and buffered output | API foreground/control allocator; nested listener application quota | Held through response writer and cleanup; count once in storage-owner totals, despite appearing in both listener and API counters |
| Native pgwire ordinary protocol/session and listener-runtime heap | Bulk partition of pinned public transport allocation; nested connection/listener/retained quotas | Shares HTTP transport's reservation; native session/listener joins precede allocator destruction; adapter execution state retains its API allocation owner |
| Native pgwire bounded frames and command/error acknowledgements | Per-connection control partition backed by the public parser allocator | 32 KiB reserved within the original connection ceiling, with frame selection capped at 8 KiB; verified simple and extended controls execute through API control memory; retained prepared statements and portals retain their original owner |
| SQL plan cache | Nested API foreground allocation | Existing 32 MiB cache cap; do not add again to storage-owner totals |
| HTTP raw/transformed body storage | Bulk partition of pinned node transport reservation | `min(64 MiB, API request ceiling)` total reservation includes the parser partition; body/task/connection limits remain nested; body allocations retain their original allocator until destruction |
| HTTP headers/parser/router/runtime heap and bounded control bodies | Parser partition of the same transport reservation | `min(8 MiB, transport reservation / 8)`; ordinary bodies cannot consume it; bounded declared-length control candidates get an independent body quota; typed dispatch still verifies the operation |
| Distributed-read reusable HTTP client heap | DataServer service foreground allocator, charged to node external bytes | `min(64 MiB, API retained ceiling / 4)`; client pool, waiter and forwarding-task limits remain nested; clients and streams join before allocator destruction |
| Data mutation forwarding reusable HTTP client heap | Pinned DataServer service control allocator | `min(4 MiB, API request ceiling / 8)` pinned before API publication; isolated from distributed-read allocations; includes fresh forwarding as well as completion and does not promise progress if this partition itself is full |
| Outbound returned bodies and streaming sinks | Caller-supplied allocation/output owner | Buffered response ceilings and stream backpressure remain independent; API callers charge their envelopes; the service client heap does not include caller-owned output |
| Distributed join/graph and model/probe helper child arenas | Caller-supplied allocation owner through a synchronized allocator | 64 MiB local ceiling per fan-out group, nested within the caller ceiling; all child tasks join before arenas and the allocator owner retire; response copies remain caller-owned |
| Caller-supplied stores and backend allocations ignoring supplied allocators | Caller/backend owner | Cannot infer limits from API configuration; caller must report its independent budget |
| Native C/C++ model scratch, allocator metadata, OS threads/stacks/TLS, mappings and page cache | Native/kernel/backend owner | Not comprehensively intercepted; measure RSS/threads/FDs and audit backend-specific reservations |

In linked native composition the callback crosses the storage context ABI and
charges the real storage-kernel manager; in direct composition it charges the
local manager. The local storage/inference manager borrows the linked context's
memory and pressure owner before publication. Child strict growth must reserve
parent bytes before mutation; observation-only growth still reports already
existing allocations, including over-budget usage. Child slice and external
releases wake the shared pressure event, as do releases from other owners. Child
shutdown returns remaining observer charges after checking strict leases. The
parent context outlives API/transport allocations and child-manager destruction.
A reclaim-capable child may ask the parent for bounded sibling-cache eviction
outside the child accounting locks. Non-reclaiming commit fences never make this
call, and parent eviction cannot bypass child ceilings.
Root aggregate memory already includes child usage, so health reporting counts
that aggregate once. Per-slice reporting combines distinct owner contributions.
This coordinates managed storage, inference, API, public HTTP and DataServer
outbound client heaps. Native scratch, OS memory and independently supplied
backend heaps still require measurement and their own ownership contracts.
Helpers without a supplied network I/O owner retain their bounded native
threaded fallback; its threads and stacks are outside the caller heap quota.

`ResourceManager.memory.external_bytes` includes pinned control/transport
reservations, child-manager usage and foreground/persistent API charges. It is a subset of
`memory.used_bytes`, not an additional quantity. API control used bytes are a
subset of its pinned reservation. Transport used bytes are likewise a subset of
its pinned reservation, with no second charge at allocation. Pgwire protocol
retained bytes are a subset of pgwire allocation bytes, themselves a subset of
shared transport used bytes. Adapter-owned retained execution objects keep their
API persistent quota. Reservations can
be returned on backing-allocation failure without changing any live object.
The external callbacks are allocation-free, do not invoke reclaimers, and must
outlive the allocator owners. The manager asserts that external charges are zero
at final destruction.

## Acquisition and progress

1. Bound transport connections, descriptors, raw bodies and application scopes
   before decoding or publishing a handler. Authentication precedes policy changes.
2. Preparation and fresh plan-cache fills finish before foreground admission
   waiting. A waiter retains only its caller-owned entry, input, original deadline
   and already reserved task; it cannot reserve a backend execution lease twice.
3. Execution admission precedes new downstream work. Each allocation first
   reserves its local API quota and then the storage-owner quota; denial rolls back the
   local reservation before returning OOM. Neither lock calls into an allocator.
4. Storage/model admission may reject a fresh operation after query admission.
   The operation then retires its lease; it does not hold it while retrying new
   mutations. Accepted durable work retains its own completion contract.
5. Verified transaction controls bypass fresh-query/write/preparation/cache-fill
   admission, use independent bounded workers and the pinned control allocator.
   Managed API initialization acquires the control workers before publication.
   HTTP reserves protected tasks under both fail-fast and waiting policies;
   fail-fast reserves them within its existing total task ceiling.
   Cleanup frees through allocation provenance. It cannot erase an already
   started durable decision.
6. Closing transports joins writers/sessions before API allocators disappear;
   API joins accepted jobs and releases persistent state before returning pinned
   control capacity. The storage-owner manager is destroyed last.

The transport reservation now partitions parser/control allocation from ordinary
raw body allocation. Native H1/H2 socket tests deny every ordinary body allocation
and verify bounded rollback/status progress; H2 also verifies that an OOM response
leaves the connection usable. Unknown-length and oversized candidate bodies use
the ordinary partition. Pgwire fixtures deny ordinary frame/execution allocation
and verify that bounded COMMIT and ROLLBACK frames and their acknowledgements
remain usable, while an ordinary SELECT still fails. Candidate frame size alone
does not authorize control execution: the backend grammar classifier verifies the
statement. Parser exhaustion and physical mutating storage completion remain
separate schedules; the API reserve alone cannot establish those guarantees.

The native socket fixture now exhausts the real DB ResourceManager alongside
API foreground allocations and foreground SQL workers, then verifies held-cursor
COMMIT and failed-transaction ROLLBACK. This proves those local storage schedules.
It does not prove physical WAL/Raft/LSM completion through arbitrary distributed
locks or remote-owner failures. New storage work required during COMMIT can still
fail under its own downstream ceiling; do not turn that failure into a claim of
replay-safe mutation rejection.

## Runnable execution and resumed reads

The opt-in runnable scheduler is separate from coarse operation admission. A
scheduled SQL read retains its operation, snapshot, output and memory owners
while it gives up a runnable slot during cooperative yields, private positional
spill I/O and retained PG output waits. Slot reacquisition preserves the original
statement deadline. Ordinary SQL writes retain a write-class grant through worker
completion, as described below; accepted durable work keeps its existing completion
owner and does not borrow read-only suspension callbacks.

Scheduled HTTP SELECT/EXPLAIN and retained PG SELECT streams keep at most 64
owned table authority pins. Before continuing after a new runnable grant, and
before handing off results, they validate catalog identity/schema/revision,
native serving policy generation, and current managed user/API-key permission
and effective row-filter scope. Current authority cannot broaden the admitted
query. Protected policies also retain the admitted role/metadata interpretation.
A pending installation can retain the previous serving generation; a serving
transition or changed generation stops the read. Failed validation releases
through the original operation owners and never becomes a successful cached
check. [Qualification evidence](receipts/2026-10-07/read-resume-fences/README.md)
records the covered schedules and platform results.

Private spill reads/writes and directory/file creation currently release runnable
capacity. Owned creation results are discarded if reacquisition or the subsequent
authority checkpoint fails; mandatory cleanup does not need a new grant. The
[creation qualification](receipts/2026-10-07/spill-creation-waits/README.md) covers
real handle retirement on both platforms. SQL scan preparation now passes a
borrowed wait to read-index barriers; the DataServer parks only for its actual
owner-lock and quorum/apply polling sleeps, and resumes the same execution owner
before probing or consuming completion. Failed resume retires the read-index
registration. Group admission is serial while borrowing this single-worker
owner, and remote registry cursors clear the borrowed wait. The
[read-index receipt](receipts/2026-10-07/read-index-waits/README.md) distinguishes
contract evidence from pending actual DataServer/quorum and whole production
qualification. Snapshot bootstrap, general storage/RPC waits and cleanup
execution ownership still need suspension boundaries. Authority validation
itself performs catalog I/O: extending these boundaries must avoid recursively
validating a guard already in validation, and must reacquire execution capacity
before parsing or consuming returned data. Preparation, serialization, parallel
CPU children, externally issued credential refresh, non-SQL workloads and remote
attempt completion still need their own owners and qualification. These read
fences do not qualify automatic defaults or the full scheduler design.

## Default release gates

Automatic query waiting/sizing remains unqualified until all of these have
receipts: Linux GNU signal-stack ownership, fixed small and large container
profiles, slow-reader and mixed open-loop pressure, main-versus-candidate latency
and throughput, and protected distributed completion. A larger independently
owned allocation or native-thread cost must not silently enlarge a process budget.
The manifests record enforced cgroup CPU/memory/PID limits, not just YAML intent.
Static-musl retains the original Zig signal-stack implementation and needs its
own profile.


Scheduled ordinary SQL writes retain a write-class grant from execution setup
through worker completion, including accepted durable work. The existing
operation lease, transaction/session owners and outcome/recovery obligations
keep their original lifetimes. Read-only suspension callbacks are withheld from
ordinary writes; their native SQL backend runs serial CPU work and retains
spill I/O. Write waits currently continue holding runnable capacity. Releasing
that capacity requires a verified durable completion owner that can finish
without request cancellation or a new foreground grant. Qualification of this
write integration passes all 75 selected API contracts on native macOS and
pinned Nix/glibc GNU AArch64 with zero skips or leaks. Both exact Linux artifacts
also pass under two CPUs and 2 GiB without extra swap. Complete production
qualification and stalled accepted mutation schedules remain pending.

Scheduled SQL decision-provider batches also execute serially on their parent
worker until provider children acquire independently owned execution entries.
This confines provider frontend CPU to the existing grant; it does not schedule
model-server/backend inference or release the grant during provider waits.
Unscheduled decision batches retain their existing parallel behavior.
Scheduled read runtimes borrow the adapter's execution checkpoint through the
provider call. Preflight, provider invocation/completion and result normalization
reach that checkpoint, preserving the original deadline and revalidating read
authority after a cooperative yield. A borrowed parent always prevents task
fan-out, including when a caller leaves the parallel default enabled. The parent
and its adapter outlive evaluation. Ordinary writes do not borrow this read
checkpoint; their accepted completion obligations remain unchanged.

## Shared execution owner startup and lifetime

Opt-in standalone creates an opaque API-kernel-owned `NodeOwner` before storage
runtime construction. Checked create/close/destroy exports keep its scheduler
and native errors inside the API archive. API, inference and both local and
compiled-context storage runtimes retain checked ports to the same owner.
Storage releases its lease after executor lanes, recurring registrations and
durable jobs join. Standalone closes the owner before normal consumer shutdown
and destroys it after all retained consumers retire. Focused maintenance checks
cover runtime retention and physical lane drain; complete compiled-context and
production startup/shutdown qualification remain pending.

Recurring storage callbacks still require explicit CPU stage admission and
mandatory holder reentry. Distributed DataServer startup must also receive the
node owner; its existing work-cost port is only pacing/accounting authority.
Default scheduling remains disabled until these paths and the full release
gates are qualified.

Checked archive callbacks carry stable request-owned execution capsules.
Independently admitted inference requests use the inference class. An embedded provider invoked synchronously by an already scheduled SQL
worker must inherit that worker's grant rather than acquire a second ordinary
slot: with one ordinary slot, nested acquisition would wait on its own parent.
Inherited grants remain confined to that worker; concurrent children need their
own entries and the parent must release execution capacity before joining them.
Shutdown closes the node owner before joining its consumers, while accepted
durable completion retains an independently valid completion contract. API
destruction alone must not invalidate a borrowed node owner. These lifetimes and
the mixed-class schedules need qualification before default enablement.


## Qualified per-write socket controls

The isolated socket component borrows original cancellation/deadline controls
through one output operation without replacing a connection-wide request deadline.
Native and fallback backpressure tests passed on macOS and pinned Nix/glibc Linux;
see [the receipts](receipts/2026-10-07/controlled-output-socket/README.md).
An application cancellation monitor reports the original cause before runtime
recancel, avoiding a panic when the executing task itself was not cancelled.

HTTP streaming headers/chunks, H2 lock and flow-control waits, and the inference
output adapter still need this mechanism connected to their original controls.
Partial frame transmission needs explicit transport retirement. These socket
receipts do not qualify those integrations, physical scheduler ownership, or
pooling defaults.


## Original HTTP output controls

Controlled HTTP/1 framing and HTTP/2 encoding, frame, lock and flow-credit waits
preserve original request fences. Uncertain output retires the connection under
cancellation protection. The focused library gate passes on native and pinned
Nix/glibc Linux; see [HTTP output receipts](receipts/2026-10-07/controlled-http-output/README.md).
The layout-only runtime callbacks retain those controls and terminal status
classes on both platforms; see [bridge receipts](receipts/2026-10-07/controlled-runtime-http-bridge/README.md).
Standalone production wiring still needs archive qualification. Parking a whole
output callback leaves formatter CPU uncharged; physical IO-only parking remains
required before enabling pooling by default.


Optional caller-thread output wait hooks now bracket socket writes, contended
lock sleeps and HTTP/2 credit waits. Formatter CPU between waits remains outside
those hooks. Both platform library gates pass; see
[wait ownership receipts](receipts/2026-10-07/output-wait-ownership/README.md).
The hooks still require checked archive integration with actual owner capsules;
whole-callback parking has not yet been removed in production.


Controlled HTTP/2 now finishes its atomic frame/header block and releases the
connection mutex before weighted execution reentry. Published headers remain
marked on failed resume; late mapped failures reset the started stream. Native
and pinned Nix/glibc library and supplemental real-owner/TCP gates pass; see
[HTTP/2 unlock/reentry receipts](receipts/2026-10-07/h2-unlock-reentry/README.md).
The supplemental ABI 33 production worker integration remains unqualified by
these focused gates and queued for full archive checks.


HTTP/2 cancellation reset now has a separate 250ms transport-cleanup budget for
connection mutex acquisition and its fixed frame. Cleanup protects against task
cancellation, retires the socket if it cannot safely reset, and avoids unlocked
stream mutation on mutex denial. Native and pinned Nix/glibc GNU HTTP gates pass;
see [bounded reset receipts](receipts/2026-10-07/h2-reset-cleanup/README.md).
This is fixed-size transport housekeeping, not an independently admitted control
CPU root. Production integration and full default-enablement gates remain pending.


Denied execution reentry and cooperative yielded waits now stop runnable work
without releasing published capsule operation/byte reservations. The original
owner releases them after consumer cleanup/reaping. Native and pinned Nix/glibc
scheduler, broker/owner and actual subprocess-to-stalled-TCP gates pass; see
[retained resume/reap receipts](receipts/2026-10-07/retained-resume-reap/README.md).
The supplemental child uses production RPC and actual OS reaping; production
Worker.Client/model startup, current whole API/archive, stuck-reap quarantine and
full default-enablement qualification remain required.


Stopped published capsules retain their original failure through later checkpoint,
park and enter attempts, including a cleared cancellation token and subsequent
shutdown. Their reservations remain held until owning finish. Both scheduler and
real owner/subprocess gates pass on native and pinned Nix/glibc GNU; see
[stopped-cause receipts](receipts/2026-10-07/stopped-capsule-causes/README.md).
Current full API/production qualification and default enablement remain pending.


Caller-owned worker output errors preserve original scheduler/fence causes across
coarse callback statuses; attempted header publication prevents worker fallback.
HTTP overload mapping now includes scheduler denials and emits retryable 503.
Native and pinned Nix/glibc helper and HTTP gates pass; see
[worker output error receipts](receipts/2026-10-07/worker-output-errors/README.md).
Actual worker integration remains supplemental pending full production/API gates.
Protected initial error output/control CPU and default enablement remain required.


Initial HTTP error responses now share a separate 250ms total cleanup budget
across locks, credit and socket writes, with task cancellation blocked for that
bounded attempt. H1 uses a fixed local buffer. H2 handlers release the frame lock
while waiting for credit; pump rejections reset only the stream on credit denial.
Native and pinned Nix/glibc pass all server tests and selected transport contracts;
see [error cleanup receipts](receipts/2026-10-07/error-output-budget/README.md).
Independent control CPU admission outside frame locks and full production/API,
whole-node/default qualification remain required.


Initial runnable acquisition now honors fail-fast allow_wait policy, while
published reentry retains bounded fair waiting. Checked runnable ABI version 2
forwards that policy and rejects legacy/malformed inputs. Native and pinned
Nix/glibc scheduler, ABI and real-owner/subprocess gates pass; see
[fail-fast acquisition receipts](receipts/2026-10-07/fail-fast-runnable-acquisition/README.md).
Independent transport cleanup wiring and its own bounded count/byte subquota
must still preserve health/control headroom. Full default-enablement proof is pending.


The earlier physical output-wait checkpoint now has terminal native and pinned
Nix/glibc full production archive/provider gates, with all 27 exact source hashes
verified; see [production pair](receipts/2026-10-07/output-wait-production-pair/README.md).
This predates the later ownership/cleanup/cause fixes and runnable ABI 2; it does
not qualify the current revision or default enablement. Current gates remain queued.


Checked runnable ABI 3 bounds transport cleanup to one retained operation and
half the shared control byte reserve, including actual capsule metadata. Tiny
policies keep their sole control operation for health. The gate remains owned
through parked/stopped cleanup until finish; failed starts unwind it. Native and
pinned Nix/glibc ABI and real-owner gates pass; see
[cleanup subquota receipts](receipts/2026-10-07/cleanup-subquota/README.md).
Transport callback wiring outside connection locks and full production/default
qualification remain required.


Initial error responses and late resets now borrow independent shared protected
cleanup execution before formatting/connection locks. Physical waits park CPU;
reentry follows frame unlock, and failed output retires the socket before owning
finish. Real public H1/H2 listeners exercise the actual checked owner under full
ordinary CPU demand, with health and H2 PING progress during zero-credit waits.
New H2 streams now honor peer initial send windows. Native and pinned Nix/glibc
transport and real-owner gates pass; see
[owned HTTP cleanup receipts](receipts/2026-10-07/owned-http-cleanup/README.md).
Full production wiring remains supplemental. Other pump/control paths, accepted
durable completion, quarantine, whole-node/default proof remain required.


A running write retains CPU/operation/byte charges through foreground cancellation
and scheduler shutdown until owning finish, with health headroom before close.
Native and pinned Nix/glibc pass the explicit lifecycle regression; see
[write completion baseline and acceptance audit](receipts/2026-10-07/durable-write-retention/README.md).
Accepted-write parking remains unimplemented: ordinary parked entries cannot
resume after close. SQL durable sealed plans and storage decisions require
distinct handoffs. Native/hosted transaction participant fanout currently has
a bounded task count but no scheduler execution port; accounting those tasks
is part of the broader completion integration, not covered by serial SQL work.


A retained running execution can now resize its CPU team demand. Growth releases
its old CPU grant and queues the full demand fairly without reacquiring operation
or byte admission; shrinking returns excess CPU immediately. Denied reentry
retains ownership to finish. Native and pinned Nix/glibc pass all 33 scheduler/
admission contracts, including stale queue-timeout regression evidence; see
[CPU team reservation receipts](receipts/2026-10-07/runnable-team-reservation/README.md).
Checked ABI exposure, actual transaction/numerical child integration and child
heap accounting are still required. This is not accepted completion authority.


Checked runnable ABI 4 exposes optional CPU team resizing on the existing API
capsule, with no new operation/byte/lease admission and single-slot cleanup
enforcement. Header-first foreign readers reject old/truncated layouts before
copying callback tails. Native and pinned Nix/glibc ABI and real-owner/listener/
worker gates pass; see
[checked CPU team receipts](receipts/2026-10-07/runnable-team-abi/README.md).
The two production inference call-site updates remain supplemental pending full
archive qualification. SQL fenced adapters and actual transaction/numerical
children still require integration; accepted completion authority is separate.


Invocation-owned Team now reserves coordinator/child CPU on one existing checked
capsule, provides an owned child allocator, and retains its grant through joined
completion. Joined attempts may reuse the same original fences; active reuse
and post-shutdown reservation are rejected. Native and pinned Nix/glibc owner
gates pass; see
[participant execution team receipts](receipts/2026-10-07/participant-execution-team/README.md).
The coordinator fanout/allocator proposal is supplemental: syntax checks pass,
but coordinator type checking and runtime gates remain pending. Team forwarding
from request ingress/table-write contracts is not yet wired. Accepted completion
I/O parking and whole-node/default qualification remain required.


Private table-write callbacks now accept an explicit retained execution team,
with fail-closed support checks and no legacy fallback for supplied capabilities.
Two named direct/compiled callback regressions plus their discovery test pass on
native and pinned Nix/glibc; see
[table-write execution contract receipts](receipts/2026-10-07/table-write-execution-contract/README.md).
The focused contract root must import the source directly: the initial consumer
module roots discovered neither named regression. SQL ingress/request-context,
serial bound storage, provisioned/hosted storage and coordinator forwarding are
supplemental pending full archive and transaction qualification. Protected COMMIT
keeps its single control slot. Accepted completion parking and default/whole-node
proof remain outstanding.


The SQL write capability factory now enforces ordinary/control root identity
and prevents any control borrower widening beyond one slot. Its wait/checkpoint
hooks remain unavailable until accepted completion authority exists. Four real
scheduler/bounded allocator regressions pass on native and pinned Nix/glibc; see
[SQL write team receipts](receipts/2026-10-08/sql-write-team/README.md).
SQL HTTP/pgwire call sites remain supplemental. The proposal narrows team creation
to mutations/COMMIT and repairs scheduled legacy API constructors which formerly
had no bounded request owner. The real constructor regression and production
archive/coordinator gates remain pending. Whole-node/default proof is incomplete.


The scheduler now reserves/activates an independent bounded completion phase
before possible durable acceptance on an already retained write/control root.
Active completion retains its CPU partition and operation/bytes through waits,
foreground cancellation and admission close; denied completion reentry retains
stopped ownership until consumer joins/transfer and owning finish. Native and
pinned Nix/glibc pass 38 scheduler/admission and 30 real-owner/SQL/worker/cleanup
contracts per platform; see
[completion reservation receipts](receipts/2026-10-08/completion-reservation/README.md).
No production consumer activates it yet. Independent completion task/Io ownership,
pre-seal/participant acceptance wiring, checked ABI exposure, physical WAL/Raft
waits and authoritative durable recovery transfer still require implementation
and qualification. Write suspension remains withheld; pooling stays disabled.


Initial Scheduler.wait can no longer retire an active completion's live consumers;
it routes active completion to retained wait. The reproduced premature-finish
failure is preserved. All 40 scheduler/admission and 30 real-owner/SQL/worker/cleanup
regressions pass on native and pinned Nix/glibc; see
[completion wait retention receipts](receipts/2026-10-08/completion-wait-retention/README.md).
A real bounded two-task fixture cancels/joins foreground, closes admission, and
resumes completion in its independent task cancellation scope, including joined
cleanup after completion cancellation before storage work. This proves task/root
lifetime mechanics, not physical durability or node-wide thread bounds. No
production consumer activates completion; checked exposure, pre-acceptance task
reservation, physical WAL/Raft waits and durable recovery transfer remain required.


Runnable ABI 5 now exposes checked optional completion reservation/activation.
The API owner keeps completion Io/clock receivers at stable capsule addresses;
original fences govern handoff, cleanup roots cannot claim completion authority,
and no fresh admission is granted. The 38 direct ABI/runtime contracts and 28
executed owner/worker/cleanup contracts pass on native and pinned Nix/glibc.
Four SQL-team contracts also execute on Linux; the native SQL target is cached. See
[completion capability receipts](receipts/2026-10-08/completion-capability/README.md).
Independent foreign Io dispatch/full archive coverage remains pending. Production
pre-acceptance task/resource reservation, storage wait integration, ambiguous
outcome joins and authoritative recovery transfer remain unimplemented. No
production consumer activates completion; write suspension/default enablement
remain withheld.


Preauthorized completion tasks now reserve bounded metadata and actual concurrent
capacity behind a closed gate before work can begin. Activation releases the gate,
and a protected foreground join prevents actual cancellation from propagating
into the accepted task. A preserved negative regression reproduces lost callback
work without that protection. All 34 task/SQL-team/owner/worker/cleanup contracts
execute and pass on native and pinned Nix/glibc; see
[completion task receipts](receipts/2026-10-08/completion-task/README.md).
The caller retains its original root/resources through joined deinit. SQL seal
and physical participant acceptance are not wired to this primitive yet. Executor
internal allocation accounting, physical wait fences and authoritative recovery
transfer remain required; no production completion activation or default enablement.


A stable local SQL completion bridge now connects existing write/control
Executions to completion reservation/activation callbacks. It retains Io/clock
receivers through owning finish and rejects suspension until activation. All 37
SQL-team/task/owner/worker/cleanup contracts execute and pass on native and pinned
Nix/glibc; see
[local completion bridge receipts](receipts/2026-10-08/local-completion-bridge/README.md).
HTTP/pgwire bridge construction and SQL-adapter forwarding remain supplemental
pending full API/archive proof. SQL sealing/participant acceptance has not moved
to the completion task. Finite physical storage waits, recovery transfer and
whole-node/default qualification remain incomplete; no production activation.


The older GNU production gate ended with a signal-KILL compiler failure on its
monolithic pooling test root; dependent jobs did not launch. The current gate now
proposes four serial compiler batches preserving all 31 pooling filters, disjoint
first-match ownership and combined inventory auditing. A dedicated participant-team
gate makes that actual regression's result explicit without replacing the broad
transaction gate. Native/pinned Nix/glibc build graph checks and ten existing audit
helper tests pass; see
[qualification batch receipts](receipts/2026-10-08/qualification-batches/README.md).
Static source selection is preserved, but compiled inventories, actual batch tests
and compiler-memory reduction remain unverified. Runtime limits are unchanged.
Full production, storage deadline/handoff and default qualification remain pending.


BEGIN/PREPARE callback wrappers now reject legacy fallback when an explicitly
supplied pre-decision deadline/clock/token/route/restore context cannot be carried.
The preserved negative test reproduced legacy success with a supplied deadline.
All 41 focused boundary/task/SQL-team/owner/worker/cleanup contracts execute and
pass on native and pinned Nix/glibc; see
[pre-decision fence receipts](receipts/2026-10-08/predecision-fences/README.md).
Empty contexts preserve legacy behavior; capable callbacks preserve context and
exact cancellation/checkpoint errors. Shared completion deadlines across physical
coordinator/storage waits, pre-seal task handoff and durable recovery transfer
remain unfinished. This is callback proof, not default or physical wait proof.


The pending physical-cache accounting proposal admits the LSM shard-array
allocation against the private startup manager before allocator invocation. A
movable reservation identity keeps that charge through cache teardown. Four
focused metadata fixtures pass on native and pinned Nix/glibc Linux, including
real two-ancestor attachment rejection/retry and allocator-observed ancestor
charges at physical shard free; see
[metadata parent receipts](receipts/2026-10-08/cache-metadata-parent-proposal/README.md).
Process-parent installation happens after local allocation and can reject
publication. This proves eventual parent accounting and physical-free ordering,
not process-parent preauthorization before allocation. Dynamic maps, allocator
overhead, transient decode allocations and retained read-cleanup tasks remain
unbounded by this proof. The full production chain remains pending and defaults
remain unqualified.


A pending trusted scheduler cleanup phase now reserves a finite independent
fence while foreground execution is running and retains the same operation,
bytes and CPU partition. It can activate after foreground cancellation, denied
reentry or owner close, then uses the existing fair queue and bounded wait.
Failed cleanup reentry keeps physical ownership charged until explicit finish
after joined cleanup or authoritative transfer. Unused durable completion can
be reserved alongside cleanup; cleanup consumes that inactive authority and
cannot replace an active durable completion. Native and pinned Nix/glibc each
pass all 45 scheduler/admission tests, including five new lifecycle tests; see
[read cleanup phase receipts](receipts/2026-10-08/read-cleanup-phase-proposal/README.md).
This is a local scheduler primitive. ABI/task admission, real cache/load/pin
cleanup callbacks, finite physical waits and fallback transfer remain unwired.
It does not make production read cancellation or shutdown bounded yet.


A pending cleanup task now reserves metadata and actual concurrent execution
before physical resource publication. Its child activates the finite cleanup
fence after foreground expiry/cancellation/close, and a protected parent join
preserves original cancellation without forwarding it into cleanup. Runnable
ABI version 6 adds checked cleanup reserve/activate callbacks and distinct
capsule-owned executor/clock receivers. Three cleanup-task fixtures and the
three existing completion-task fixtures pass on native and pinned Nix/glibc;
see [cleanup task receipts](receipts/2026-10-08/read-cleanup-task-proposal/README.md).
ABI readers reject old/truncated headers and absent capability before allocation.
Task activation or fair reentry failure still requires caller-owned physical
join or authoritative transfer before execution finish. Actual cache/load/pin
integration, finite storage waits, SQL cleanup bridge forwarding and recovery
transfer remain required. Full production/default qualification remains pending.


A pending production loader change reserves one of 32 fixed cleanup slots per
LSM cache shard before publishing an owned claim. All three physical loader
paths transfer their joined registration/key ownership into that slot on exit,
without a lock, heap allocation/free, budget callback or wake syscall. Cache
startup accounting includes the inline slots through the actual shard size.
New loads and independent admitted passes retire transferred identities; stale
cleanup cannot remove a replacement claim. Overload rejects before publication.
Legacy duplicate waiters use a bounded condition polling interval so transfer
racing a wait does not require a wake syscall on the returning read.

Native and pinned Nix/glibc pass 67 cache tests, 11 physical storage tests, four
metadata tests and three cleanup-task tests; see
[load cleanup transfer receipts](receipts/2026-10-08/load-cleanup-transfer-proposal/README.md).
The cold-read fixture now proves cache ownership for every deferred registration,
retirement under an independent scope without touching the failed foreground
budget, and correct retry results. Cache teardown requires all active claims
joined/transferred before freeing the charged shard owner. Slot count is bounded;
variable key bytes and pending-map allocation overhead are not yet admitted as
part of the complete process memory budget. Pin/decode/allocator/native-I/O
cleanup, retained task integration and whole-node/default proof remain pending.


The pending loader registry now stores at most 64 entries inline in each cache
shard. It allocates no hash buckets or growth scratch; all table storage is
covered by the actual fixed-shard startup reservation. Both legacy and owned
loads reject a full registry before copying/publishing a new key. Native and
pinned Nix/glibc focused gates pass 68 cache, 11 storage, four metadata and
three cleanup-task tests; see
[fixed pending registry receipts](receipts/2026-10-08/pending-registry-bound-proposal/README.md).
The new fixture fills the registry with legacy loads, proves one allocation per
key and zero registry allocations, checks both admission paths refuse overload
before allocator invocation, and verifies removal/reuse without stale removal.
Variable copied-key bytes still require admission; entry maps, allocator/native
waits, pin cleanup and full node/default qualification remain incomplete.


Managed owned loader claims now reserve both copied path payloads before
allocation, using stable per-slot observer identities. Failed allocation leaves
its credit in the cache; transferred cleanup frees physical keys before releasing
capacity and retains busy parent debt under independent finite cleanup. Parent
waits run outside pending registry locks. Managed cold admission drains all
shards before requesting fresh key bytes, allowing completed keys elsewhere to
return capacity. See [owned key admission receipts](receipts/2026-10-08/loader-key-admission-proposal/README.md).

This admits requested owned-key payload bytes only. Legacy unowned keys,
allocator and observer-ledger overhead, large string work, entry maps, pins,
transient decode and native I/O still need complete ownership/admission. Manager
replacement requires joined/transferred loader claims; quiescent teardown uses
mandatory legacy accounting. Full production/default qualification remains
pending; focused receipt versions identify their exact verification scope.


The three legacy backend state/raw/index loader call sites now use owned
claims and transfer joined cleanup on exit, admitting their managed key bytes
before copies. Native and pinned Nix/glibc focused gates pass 72 cache, 12
storage, four metadata and three cleanup-task tests; the new real storage
fixture verifies rejection before missing-path reads, no publication on failure,
correct persisted data after retry, and exact cleanup ownership followed by
independent collection. See [backend loader receipts](receipts/2026-10-08/backend-loader-ownership-proposal/README.md).

Backend methods still use null controls and legacy storage/decode paths. Whole
table and nonshared index runtime fallbacks need original-scope propagation
through those methods. Local path/handle allocations, pins, allocator/native-I/O
work, complete cleanup/recovery and full production/default qualification remain
incomplete. The legacy registry APIs remain for compatibility/tests.


Whole-table and nonshared-index fallback reads now pass the original port into
backend cache lookup, key admission, raw/index publication and decoding. Owned
whole-table decoding checks metadata and compressed materialization at block
boundaries. No request port is retained in local caches. Native and pinned
Nix/glibc pass 72 cache, 13 storage, four metadata and three cleanup-task tests;
the new 600-row compressed fixture sweeps sticky cancellation with and without
shared caching, verifies no rejected local publication, independent collection
and correct retry data. See [fallback read scope receipts](receipts/2026-10-08/backend-read-scope-proposal/README.md).

Native I/O is still checked around existing calls. Individual decompression,
large copies, allocator work, local cache memory, pin retirement and complete
cleanup/recovery require further bounds. Full production/default qualification
remains incomplete.


The running predecessor full DataRuntime gate exposed an HA response invalid
free: the fixture helper returned client-allocator-owned bytes but cleanup used
the server Partitioned allocator. Thirteen mismatched fixture cleanups are
corrected; all 15 direct helper response declarations now explicitly free with
std.testing.allocator. See [HTTP fixture ownership receipts](receipts/2026-10-08/http-fixture-ownership-proposal/README.md).
Source audit and formatting pass; the actual HA fixture runtime verification
remains pending in the successor DataRuntime/full gates. Production allocator
checks and control reservations remain intact. Full/default qualification is
still unproven.


Production failed-admission pin rollback now transfers physically retired byte
accounting to an atomic cache-owned mailbox. The existing charge remains until
an independent finite pass or nonwaiting reclaim adopts and reconciles it;
failed retirement no longer acquires cache/child/parent accounting locks after
physical freeing. PendingRelease can transfer completed accounting infallibly
while refusing a token that still owns a live pin. Native and pinned Nix/glibc
pass 75 cache, 13 storage, four metadata and three cleanup-task tests; see
[retired pin transfer receipts](receipts/2026-10-08/retired-pin-transfer-proposal/README.md).

The production fixture proves transfer with all accounting locks held, retained
charge through expiry and independent exact parent release. Reclaim reports no
capacity while parent is busy and preserves useful entries; two transferred
charges reconcile exactly on retry. Physical shard locking/free, transient
cleanup, live-pin transfer, full retained task adoption/recovery and default
qualification remain incomplete. The HA allocator repair is still awaiting its
actual full DataRuntime regression; source fixes are not a passing gate.


Failed retained Handle admission now transfers the physical pin through fixed
atomic counters, without locks, allocator calls, hashing, budget callbacks or
wake syscalls. Charged entries remain alive under cache ownership. Independent
finite collection advances a persistent cursor across at most 64 entries per
shard acquisition, repairs it on removal and restores exact pending ownership
if ledger admission fails before physical destruction. Fresh controlled lookup
and growth can collect earlier transfers. Native and pinned Nix/glibc pass 79
cache, 13 storage, four metadata and three cleanup-task tests; see
[deferred pin receipts](receipts/2026-10-08/deferred-pin-cleanup-proposal/README.md).

Held-lock/no-allocation handoff, cursor expiry/removal, in-progress publication
and child-ledger failure/retry fixtures pass. Nonwaiting reclaim still needs to
consume physical deferred pins so other memory owners can recover before a
new cache lookup. Transient/success-path release, collector child-ledger/allocator
work, retained task adoption/recovery, native I/O and whole-node/default proof
remain incomplete. Parent debt reconciliation runs outside shard locks.


The registered memory reclaimer now collects transferred physical pins before
idle eviction, sharing the global 64-inspection/eight-retirement bounds. It
tries every lock without waiting, restores exact pin ownership on child-ledger
denial, and reports only parent-reconciled bytes. Busy parent debt stops further
destruction. This closes the previous pressure-recovery gap before cache lookup.
Native and pinned Nix/glibc pass 83 cache, 13 storage, four metadata and three
cleanup-task tests; real sibling admission, held locks/no allocation, burst
limits and cursor progress are covered in
[deferred pin reclaim receipts](receipts/2026-10-08/deferred-pin-reclaim-proposal/README.md).

Individual allocator/free/hash work, transient/success-path release, complete
task/native-I/O/recovery ownership, whole-node memory/control/performance and
default qualification remain incomplete. The actual HA fixture repair still
requires its running DataRuntime/full qualification.

Admitted LSM cursors remember a boolean ownership mode after each scoped operation exits, without retaining the operation port. Returning retained index/data pins on close, run switching, block replacement and prefetch cleanup transfers them atomically to the charged cache owner. Temporary raw/index handles in admitted decode paths do likewise. See `receipts/2026-10-08/cursor-pin-return-proposal`; transient destruction, cursor allocation/reservation cleanup and native I/O are still outside this guarantee.

Controlled transient cache values now require the attached resource manager to admit their full estimated bytes, including policy bypasses and oversized entries. The last physical handle destroys them once and transfers still charged retirement debt without ledger waits. Controlled transient handles require the cache and manager to outlive their physical ownership. See `receipts/2026-10-08/transient-byte-admission-proposal`; input buffer allocation, native I/O, allocator destruction and reservation rollback are not yet fully bounded.

Duplicate cache publication destroys the discarded physical entry and value before releasing its incoming reservation. Admitted publication transfers the charged retirement debt atomically, so held cache/child/parent ledger locks cannot block that retirement under the shard. Foreground pressure checks may still wait through the original scope after the shard is released. See `receipts/2026-10-08/duplicate-retirement-proposal`; this does not bound physical allocator destruction or allocation-error reservation rollback.

Cache publication constructors own both input value and admitted reservation from entry, including the first checkpoint failure. Complete bytes are admitted before entry/path metadata allocation. Error guards unlock the shard and destroy descriptor/path/input before atomically transferring charged retirement debt; they never borrow the failed foreground scope or wait on its ledger locks. See `receipts/2026-10-08/publication-rollback-proposal`. Caller input allocation/native I/O and physical allocator deinit/free latency remain unqualified.

Admitted transient handles now reserve one of 32 inline physical cleanup slots per shard before publication. Last return transfers the physical body with atomics, retaining byte charge and cache lifetime; it does not destroy it on the foreground task. A fresh scoped collector or nonwaiting pressure reclaimer owns destruction and debt transfer. The latter shares 64 inspections/eight retirements across transients and retained cache entries. See `receipts/2026-10-08/transient-cleanup-owner-proposal`; final focused native and pinned Nix/glibc Linux qualification both pass (91 cache, 14 storage, four metadata and three cleanup-task tests); the initial unavailable-engine failure remains preserved. Physical allocator destruction, prepublication input rollback and native input preadmission remain incomplete.


## Dedicated cleanup CPU proposal

The native scheduler accepts `cleanup_slots: 1` as an explicit reserve within
`slots`. Health/control keeps `control_slots`; ordinary query, write, inference
and background teams share `slots - control_slots - cleanup_slots`. A valid
policy retains at least one ordinary slot. The dedicated cleanup queue admits
only trusted protected cleanup roots, and parking, failed entry, team resizing
and owning finish return capacity to that same partition. The existing global
control operation and byte quotas still cover both kinds of control work.
Public control CPU gauges include both reserves and their live grants.

A configuration with `slots: 4`, `control_slots: 1`, `cleanup_slots: 1` allows a
two-slot ordinary team, one physical cleanup and one health operation at once.
The cleanup may hold its slot through an uninterruptible allocator callback;
this does not loan its CPU or return live memory credit. Explicit zero retains
the earlier shared-control CPU policy and does not qualify health progress
through a stalled destructor. Managed cleanup startup now establishes the dedicated reserve before its
worker and listeners are published. A zero reserve is upgraded only on a
quiescent scheduler, without changing total slots or health capacity; fewer
than two ordinary slots before adoption rejects startup. An active zero-reserve
policy also rejects startup. An explicit existing reserve is reused without
resizing. Generic standalone schedulers may retain zero without a managed
consumer. Metrics capture effective policy and live counters under one lock.
Automatic default sizing, public schema and whole API startup qualification
still require their release gates. Whole Linux resource, protocol, durability and
performance/default gates remain required.


## Shared SQL executor roles

Execution, protected control and cleanup executor reservations belong to the
backend runtime. API instances borrow those role executors and report their
actual capacities; aggregate worker accounting charges each role once. Each
borrower joins its own submissions and stable imported receiver before release.
The last borrower physically joins the executor outside the role mutex. Its
separate runtime gate remains held until join and role bookkeeping finish. A
retiring role rejects new acquisition rather than publishing a second executor.
Independent completion, cleanup reaper and resource-manager worker leases remain
separate and consume the remaining runtime worker budget. Per-API preparation,
query, write and session ceilings remain per-API; the node runnable owner is the
shared CPU/operation policy where configured. This executor ledger does not
qualify automatic defaults or complete background CPU/wait-holder adoption.

Shared SQL role snapshots distinguish retained executor capacity from consumers.
A role remains retained throughout the final physical join and pool bookkeeping;
zero consumers alone does not establish retirement. Metrics must not interpret
these counts as runnable CPU grants or query queue depth. SQL consumers still join
their own submitted tasks before releasing a shared lease. The saturation fixture
holds nineteen actual callbacks across execution and cleanup, admits control work,
and joins all callbacks before any corresponding consumer is retired.

Pre-decision replication startup recovery borrows the original batch lifetime.
Flush/apply contention must check it without dropping retained service authority.
Rejection cannot clear startup/policy flags or consume undelivered outbox keys.
After a recovery delivery succeeds, its acknowledgement remains mandatory; the
next entry may observe rejection. Provider waits and page materialization still
need physical lifetime/bounded-work coverage. Legacy recovery wrappers and durable
completion owners are preserved. Focused startup-helper fixtures use real service
locks but stand-in recovery callbacks; physical DB tests remain a separate gate.

Controlled startup outbox reads borrow the original read port only through the
synchronous DocStore call. Partial singleton/page buffers, cursor and snapshot
references retire before rejection returns. No page retains the port. Physical
backend/payload reader gate rejection must preserve the caller's existing lock
and all pending keys. Payload copying checks lifetime between 64 KiB chunks;
allocation, snapshot construction, tree comparisons and foreign I/O are not
qualified by that quantum. The memory-store capability supports admitted snapshot,
point-batch and cursor boundaries without an uncontrolled fallback. Focused
fixtures use the actual memory backend; DB/provider/LSM integration remains open.

A read-budget restriction during checked CPU return observes original lifetime
without a running CPU checkpoint. Its temporary request controls suppress the
CPU borrow; the stored original borrow and all parent resource authority remain
retained. The same lifetime check is valid before queue admission and after grant.
Missing lifetime capability fails before parking. Rejected resumption retains the
original cause, phase, operation and bytes until physical users join and their
owner finishes. Running checkpoints still charge ordinary CPU quanta.

### Original lifetime during durable outbox recovery

Startup recovery now uses the retained original request admission for reading and
copying its pending page, validating each persisted CRC in at most 64 KiB updates,
and acquiring the replication transition and log fences. Rejection before
publication returns the original error and releases only locks acquired by that
operation. It does not delete or acknowledge a pending effect. The three legacy
singleton effects and mutation-specific records follow the same admission path.

Entering the publisher's recovery callback starts mandatory completion work.
Request rejection cannot replace the publisher's durability or authority result;
transition release, remote completion, final authority recheck, and subsequent
storage acknowledgement retain their existing ordering. The admitted acquisition
checks cover contention and successful acquisition before publication. They do
not make publisher callbacks or final completion waits deadline bounded.

The focused owner verifies real engine locks with a stand-in publisher, exact
original rejection, retry, and mandatory completion after publication. Physical
DB/mirror integration remains a separate gate. Recovery still retains its service
leases and CPU capacity: adopting the whole holder dependency graph and mandatory
CPU reentry is required before these waits may loan CPU or qualify fair defaults.

### Retained serial cleanup CPU

When managed cleanup CPU is configured, activating a preauthorized cleanup phase
uses the same reserved CPU partition as independent cleanup roots. The original
operation, retained bytes and policy class remain charged to their original
owner. After foreground children are fenced and joined, activation retires the
old running CPU team and admits one serial cleanup slot. Further team growth is
rejected by the cleanup capacity bound. Generic callers with no configured
cleanup partition retain their existing scheduling behavior.

Retained cleanup competes fairly with other cleanup work and retains its one-shot
physical deadline while queued. Saturating ordinary CPU cannot prevent its grant
or consume health CPU. Rejection while waiting retains the operation and bytes
until owning finish after physical cleanup or authoritative transfer. Focused
scheduler/native-owner tests establish these transitions; physical API/storage,
service-holder CPU loans, runtime pressure and automatic defaults remain separate
qualification requirements.

### Cleanup activation reports its admitted CPU demand

Runnable ABI 13 changes cleanup activation to return its actual admitted slot
count after a successful transfer. `Borrowed.activateCleanup` updates that borrow
only on checked success with a nonzero demand; the owning cleanup task then
passes the updated value to the physical cleanup callback. Local cleanup, opaque
owner capsules and SQL adapters report the scheduler entry's demand. Managed
cleanup reports one slot even when foreground work retained a wider team;
generic schedulers without a dedicated cleanup lane report their actual existing
team width.

All previous foreground borrowers must be fenced and physically joined before
activation, as before resizing a team. Copies do not track subsequent changes
and convey no permission to run old foreground children in the cleanup phase.
Version 12 callbacks have the same pointer-sized layout but a different calling
contract, so the version check rejects them before dispatch. Activation failure,
malformed success and unsupported callbacks never publish a replacement width.
Neither observation nor task completion retires the operation, original policy
class, retained bytes or physical resources.

Focused native verification passed 92 tests: actual reserved cleanup tasks over
both local and opaque three-slot foreground teams, isolated and generic cleanup,
original deadline expiry, scheduler closure, callback demand, growth rejection
and joined accounting. ABI tests cover legacy rejection before dispatch, failed
activation, zero-width success and successful one- and three-slot reports.
Full SQL/API/storage integration, holder CPU transfers, bounded outbound
lifetimes, runtime pressure, performance and default sizing remain separate
qualification gates.

### HTTP/2 health probes share one physical deadline

The HTTP client captures one monotonic deadline for each health probe before
acquiring the connection write mutex. Lock admission, PING output, TLS flushing
and the ACK wait consume that same deadline. An expired or failed probe shuts
down the connection and wakes send-window waiters; it does not wait for an
unacquired write mutex or retire pooled entry ownership. The connection's
existing receive/probe joins still govern physical destruction.

The probe temporarily installs its finite wire control on the TLS output while
holding the write mutex and restores the prior control before releasing it.
No stack deadline receiver is retained by the pooled connection. ACK observation
uses an absolute timestamp and is checked again against the original deadline.
This closes a path where lock and wire waits were unlimited and the ACK wait
started a fresh budget afterward. It supplies a transport deadline, not runnable
CPU authority for the connection's background tasks.

Physical loopback fixtures cover a healthy wire PING/ACK, held write mutex,
saturated TCP output, missing ACK, stale ready event and zero budget. A bounded
observer retains and joins the probe task; on a regression it releases the
injected holder before joining. Actual TLS health-probe failure and connection
background CPU ownership still need whole-lifecycle qualification, along with
storage birth-root integration, holder transfers and default sizing.

### Actual TLS health-probe qualification

The health-probe fixture now negotiates real TLS 1.2 and TLS 1.3 with a local
Python/OpenSSL peer, validates the encrypted PING and ACK exchange, withholds
ACKs, and stops reading to force encrypted-output backpressure. The probe runs
on an owned task with a bounded observer and is physically joined before the
TLS session, HTTP/2 state or socket can be destroyed. The peer has finite
accept/handshake/read timeouts and is joined or killed before fixture teardown.

A sentinel prior TLS output control rejects any accidental invocation. The
probe must override it only within its locked write/flush phase, then restore
all prior control fields. Shared TLS reader controls and transport scope remain
unchanged. Failed probes retire the connection and wake send-window waiters;
no stream token or entry ownership is freed merely because a probe completes.
The fixture does not qualify node CPU authority for these background fibers,
storage birth roots, holder CPU transfers, runtime resource limits or defaults.

The TLS fixtures require Python/OpenSSL with TLS 1.2 and TLS 1.3 support. Set
`ANTFLY_TEST_TLS_PYTHON` to its executable when the default Python uses an older
SSL library. Missing Python is a reported skip; unsupported TLS is a fixture
failure, never passing qualification. Receipts record the selected interpreter.

The real peer exposed an HTTP/2 buffered-reader defect: `std.Io.Reader.readVec`
can return zero after filling its own buffer, especially for decrypted TLS
records. Treating that as EOF prematurely discarded healthy encrypted frames.
The HTTP/2 reader adapter now uses `readSliceShort`, whose zero result represents
actual end of stream while buffered progress is consumed before returning.
The regression also checks true truncated frame payloads are rejected and freed.

### Runnable failure identities at the compiled storage boundary

The cumulative physical HA seed test exposed `SchedulerClosed` being converted
into `StorageKernelFailure` by the status-only storage owner maintenance call.
The runtime failure registry now declares the runnable admission and transition
errors explicitly. Failure ABI 62 assigns these unpublished pooling identities
an append-only range beginning at `0x10000`, preserving every existing main
status value. Both status-only calls and storage/local-query failure envelopes
retain the exact scheduler cause. The physical HA seed fixture also checks
queue pressure, runnable wait timeout and unsupported execution wait, with a
successful retry through the same owner after each rejection.

Native and pinned Nix glibc Zig Linux registry checks cover every registered
identity, inverse mapping, envelope validation, and the released main status
fingerprint. These focused checks do not prove the separately compiled HA seed
fixture has passed; that fixture remains part of cumulative integration. Holder
CPU loans and mandatory reentry still need implementation before background
callbacks may safely share the node CPU budget.

### Node execution owner at background runtime birth

Standalone now creates its node execution owner before opening storage contexts
or constructing the local backend runtime. Both receive the same checked
runnable port. Runtime-services request version 2 carries that optional port
into the compiled storage context; the provider validates and copies it before
constructing its runtime. BackendRuntime retains the owner before any work can
start, unwinds that lease on construction failure, and releases it only after
all executor lanes, maintenance registrations and durable jobs have joined.
The runtime exposes a synchronous borrowed port for explicitly adopted CPU
stages; the enclosing runtime lifetime must cover those consumers.

Native maintenance qualification covers actual lane-draining teardown, copied
port lifetime, exact shutdown rejection, malformed owner versions and allocation
rollback. A compiled context fixture additionally checks checked transport,
retention, rejection and destruction, but remains pending full integration.
The old runtime-services request version is rejected before the appended port
is inspected. Whole standalone startup and shutdown remain integration gates.

Existing recurring callbacks still need stage-level CPU admission, bounded
physical waits and mandatory holder reentry. Creating their shared owner at
birth is a prerequisite; it does not grant them CPU or qualify default sizing.

### Explicit runnable recurring passes

MaintenanceScheduler retains the runtime's shared node execution port and offers
`registerRunnableClass` for callbacks that have adopted runnable ownership.
Each admitted pass starts in the node's background CPU class with one slot,
charged inputs and adapter metadata, and one finite deadline. Initial CPU
pressure does not execute the callback or occupy a native runnable queue; the
bounded physical registration retries later. The callback receives the same
opaque borrow for checkpoints, actual wait parking and checked reentry. A final
checkpoint prevents a swallowed reentry failure from becoming a successful
scheduling result. Admission, rejection and pass failure counters are exposed
in scheduler snapshots.

Closing registration admission cancels runnable pass authority while preserving
its operation, bytes and node lease until the physical callback joins. Scheduler
destruction releases its consumer lease after all registrations, callbacks and
the coordinator retire. A callback must join or authoritatively transfer every
published resource and child before returning. This wrapper conveys no write
authority or automatic service-holder cleanup. Callbacks retaining service
leases need preauthorized mandatory reentry and cleanup before CPU transfer.
Legacy callbacks keep their existing registration API until that graph is
adopted; production producer, derived, propagation and maintenance migration is
still required.

Focused native checks exercise real API-owned opaque capsules with one ordinary
CPU slot: background admission rejection, independent health progress, retained
bytes during waits, queued reentry behind a foreground peer, original deadline,
owner closure, authority rejection, swallowed failures and physical callback
shutdown. These directly linked adapters do not qualify independently compiled
storage archives or automatic defaults. The existing physical SQL cleanup
saturation fixtures now assert the node's eight cleanup workers, five controls,
eleven execution workers and separate reaper rather than the old per-API pool;
their updated 24/25-worker accounting and full saturation/release paths remain
pending cumulative integration.
