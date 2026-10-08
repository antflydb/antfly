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

## Shared execution owner still required

Opt-in standalone now creates an opaque API-kernel-owned `NodeOwner` whose
lifetime outlives individual API borrowers. Checked create/close/destroy exports
keep its scheduler and native errors inside the API archive. API destruction
releases its borrow after worker joins; standalone closes the node owner before
transport/provider joins and destroys it after consumers retire. The native API
gate qualifies the borrower and factory contracts; complete production startup
and shutdown of this new wiring remain pending. Inference and maintenance do
not yet acquire this shared owner. Standalone already
shares managed memory with inference through `inference_bridge.ResourceBudget`,
but that ABI carries no execution grant. Inference HTTP dispatch and embedded
provider invocation live in the separate inference archive
(`pkg/inference/src/host/host.zig`). Its request-count gate is independent of
runnable capacity. DataServer's work-cost port is a pacing/accounting hook, not
an execution owner.

The next integration needs checked archive callbacks for stable request-owned
execution capsules, borrowing the new node owner across inference and background
consumer lifetimes. Independently admitted inference requests use the inference
class. An embedded provider invoked synchronously by an already scheduled SQL
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
