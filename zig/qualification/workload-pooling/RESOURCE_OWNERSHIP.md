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
| Experimental SQL policy-validation reservation | Node API foreground allocator, alongside prepared numeric scratch | Caller-declared 1 byte through 1 MiB plus provenance overhead; preflight uses the same fixed buffer and uncached schema derivation, retains it across CPU waits, resets only after nested validation arenas retire, frees at read-fence retirement; overflow refuses without control-memory fallback |
| SQL coordinator batched group-ID vector | API foreground fixed scratch credit admitted with the original CPU continuation | Exact `row_count * sizeof(usize)` plus charged ticket metadata; one physical vector allocation, freed before returning credit; denial/backing OOM selects the existing ordered row path before group mutation; parallel reducers keep independent child ownership |
| SQL plan cache | Nested API foreground allocation | Existing 32 MiB cache cap; do not add again to storage-owner totals |
| HTTP raw/transformed body storage | Bulk partition of pinned node transport reservation | `min(64 MiB, API request ceiling)` total reservation includes the parser partition; body/task/connection limits remain nested; body allocations retain their original allocator until destruction |
| HTTP headers/parser/router/runtime heap and bounded control bodies | Parser partition of the same transport reservation | `min(8 MiB, transport reservation / 8)`; ordinary bodies cannot consume it; bounded declared-length control candidates get an independent body quota; typed dispatch still verifies the operation |
| Distributed-read reusable HTTP client heap | DataServer service foreground allocator, charged to node external bytes | `min(64 MiB, API retained ceiling / 4)`; client pool, waiter and forwarding-task limits remain nested; clients and streams join before allocator destruction |
| Data mutation forwarding reusable HTTP client heap | Pinned DataServer service control allocator | `min(4 MiB, API request ceiling / 8)` pinned before API publication; isolated from distributed-read allocations; includes fresh forwarding as well as completion and does not promise progress if this partition itself is full |
| Outbound returned bodies and streaming sinks | Caller-supplied allocation/output owner | Buffered response ceilings and stream backpressure remain independent; API callers charge their envelopes; the service client heap does not include caller-owned output |
| Distributed join/graph and model/probe helper child arenas | Caller-supplied allocation owner through a synchronized allocator | 64 MiB local ceiling per fan-out group, nested within the caller ceiling; all child tasks join before arenas and the allocator owner retire; response copies remain caller-owned |
| Caller-supplied stores and backend allocations ignoring supplied allocators | Caller/backend owner | Cannot infer limits from API configuration; caller must report its independent budget |
| Worker parent receive slabs, provider-derived allocations and precharged child receive slabs | Client PayloadBudget backed by the node ResourceManager transient-host hook | Shared private 256 MiB bulk/derived ceiling plus 4 MiB small-message ceiling; child receive grants capped at 256 records and retained until physical-free receipt or kill-and-reap; parent returned payloads retain credit through actual destruction |
| Worker child request/create arenas, serialization and generated output slabs | Child retained allocator using dedicated resource RPC and one parent-owned identity per generation | Private child heap ceiling plus the same authoritative node aggregate; reserve before backing allocation, return after free; uncertain ownership ends the child and keeps parent credit until reap |
| Worker generation inline control/task state and parent parser scratch | Client-owned identity charged through the node transient-memory hook before allocation/spawn | `@sizeOf(Worker) + @sizeOf(Child) + 4 * @sizeOf(PayloadBudget) + child_metadata_limit`; includes fixed 128 resource-task slots per endpoint and serialized 8 KiB parent parser; return only after endpoint/process join and physical Worker destruction |
| Worker lease map and cache-observation containers/objects | Separate generation PayloadBudget backed by the node transient-memory hook | 1 MiB derived-allocation ceiling; growth before backing allocation, old/new overlap charged; physical free returns credit, reaped cleanup releases all metadata before budget/context destruction |
| Worker RPC maps, Request objects and queued Message containers | Parent separate node-accounted transport budget; child local budget precharged in node generation baseline | Parent 1 MiB live-byte ceiling, child 2 MiB precharged ceiling; count bound also applies to empty events; capacity remains owned until actual container free, child allowance until physical generation destruction |
| Parent worker HTTP marshaling, response headers/retirement owner and HTTP ABI state/views | Client result allocator backed by the node transient-memory hook | Shares the actual live-byte result ceiling; refusal before new allocation, retained response allocator and original body slab survive endpoint/Client-anchor retirement until public response destruction |
| Managed child HTTP context, provided route scratch and ABI response descriptors | Child request heap with dedicated transient-host resource RPC | Same child live-byte heap and actual node aggregate; allocation before mutation/publication, cleanup on denial/cancellation, response references retain the owner until destruction; independent backend allocation contracts remain separate |
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

## SQL foreground and independent remote retirement

The following table describes the current implementation. Remote-only uncertainty
does not own the HTTP input/context or pgwire session allocator, SQL admission,
or foreground cancellation source. Local native scans and callbacks must retire
before their foreground owner can release admission. An idle live cursor still
retains its separately accounted local state.

| Integrated owner | Execution and waiting | State retained and release boundary |
| --- | --- | --- |
| HTTP `SQLJob` | Its adapter's original runnable entry and synchronous admission port; eligible waits return runnable capacity through that port | Input capsule, identity, local results and read guard remain local. Result/fence cleanup precedes dispatcher teardown and `Owner.finishForeground`; remote cleanup borrows none of the job afterward |
| Pgwire `OwnedRead` | The original query entry parks between pull/output work; cleanup has separately reserved authority | Plan, session/input lifetime, stream and native scans remain owned until local cleanup physically joins. Local retirement can then release query admission even while independent remote references remain |
| Remote-read operation `Owner` | No ordinary query runnable or admission lease; node cleanup uses its bounded control consumer | Reserves a control metadata baseline before allocation and acquires a node pin before publication. Owns transferred endpoint bindings/references. Foreground finish seals new acquisition; the last registry/outbox reference releases endpoints, allocation, baseline and node pin |
| Coordinator credit waiter | Borrows the original synchronous execution-wait boundary, deadline and cancellation; the ledger creates no task | Caller-owned intrusive metadata remains valid through `endWait`. Queue metadata is count-bounded; summed requested allowance is tracked separately from issued attempt credits. Grant or cancellation removes queue membership exactly once; waiting publishes no attempt ID |
| Coordinator remote attempt | Data transport completion can leave the attempt uncertain; protected reconciliation remains independent | Destination/global count and byte allowance, exact durable identity and retained parent survive local transport failure. Only verified terminal/fence evidence releases them; timeout, capabilities and route/token acknowledgements do not prove physical retirement |
| Receiver retained capture/snapshot/cursor | Native work uses admitted callbacks; a resource borrow prevents destruction during a callback | Registry scope, lease and worker retention remain charged through actual native destruction. Cancellation/expiry revokes access and defers destruction until outstanding borrows return; a lost page reply cannot authorize cursor replay |

Attempt byte credits bound remote obligations; they are not a measurement of
resident heap. Physical allocations keep their original allocator/accounting
provenance. Endpoint and registry metadata also have their own bounded owners;
neither a counter decrease nor a client-visible error can substitute for their
destructors or required joins.

The dispatcher borrows foreground state only during synchronous dispatch. Its
retained descriptors copy the stable node protocol and independent parent.
`finishForeground` sets the remote owner's detached signal only after local
callbacks have joined; already published obligations keep their references until
authenticated retirement. Partial statement captures must release all held
fences before retrying prepublication backpressure. Published uncertain creation
and page advancement are not replayable capacity failures.

Evidence is deliberately scoped:

- [Independent owner/pgwire unit run](receipts/2026-10-10/child-outbox-capture-retirement-cumulative-integration/independent-read-owner-main-a26a5af7-native-v2.log)
  covers both remote-retirement orders, setup failure and exact local/node charge retirement.
- [HTTP and pgwire TCP-loss run](receipts/2026-10-10/child-outbox-capture-retirement-cumulative-integration/sql-http-independent-owner-main-a26a5af7-native-v2.log)
  passes ten selected tests with no leaks, including direct HTTP-worker and real
  incoming HTTP transport entry, local query progress during lost remote admit
  replies, and exact drain after replies resume. The receipt identifies the
  tested source snapshot; subsequent changes require their own verification.
  Its fixed nonreplicated authority does not qualify Raft quorum behavior;
  HTTP disconnect cancellation is disabled.
- Native three-peer isolation passed all eleven selected tests under
  `sql-healthy-third-destination-main-a900887124-native-v1`. Seven full unpaced
  repeats passed, then a fixture capability probe failed with
  `OutboundCapacityUnavailable`. A native trace located the final signed probe
  contending with the background consumer's one control connection. Test-only
  probes now retry only this capacity refusal under the same original deadline;
  fixture errors restore replies and allow bounded exact drain before teardown.
  The corrected fixture passed in the expanded native run; repeated full-suite
  success remains pending.
- Shared-credit fairness is now applied and passes all 22 standalone coordinator
  tests. Native capture/open/page reply-loss cases passed in expanded native v1,
  which passed fourteen of fifteen selected cases with zero skips/leaks. Its
  recovery-local fixture failed because a catalog-only router cannot prove local
  hosting. That fixture now binds its actual native owner and fixed authority to
  an explicit local router; production routing is unchanged. SQL compilation
  passes 20/20 steps and pgwire compilation passes 50/50. Native
  `sql-fair-credit-phase-recovery-main-a900887124-native-v2` reruns all fifteen
  cases; no complete or repeated success for this integration is claimed yet.

The separately compiled API image, broader operator families and production
defaults remain unqualified. The user waived full qualification; that does not
turn these focused fixtures into evidence for properties they do not exercise.

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
mandatory holder reentry. Opt-in distributed CLI startup now creates the node
owner before the host backend runtime and compiled storage context, passes that
same owner handle to the API, and fences it before listener/provider shutdown.
Its work-cost port remains pacing/accounting authority. Independently constructed
DataServers still require an explicitly supplied shared runtime/context; complete
distributed executable and startup/shutdown qualification remain pending.
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

### Distributed CLI execution owner birth

The distributed process creates one API-kernel `NodeRunnableOwner` from its
configured runnable policy. The host `BackendRuntimeHandle` and compiled storage
context retain the checked execution port before metadata/API startup; the API
receives the owner handle rather than constructing a private scheduler. The
DataServer borrows the host runtime, so its own cleanup cannot destroy the node.
Startup unwind fences admission before DataServer destruction. Serving shutdown
also fences before health/listener and provider joins, while Raft progress and
control capacity remain alive. Context and host runtime drain before final node
destruction. The disabled-policy path keeps a null owner and ordinary runtime.

Formatting, AST and whitespace checks cover this source change. Focused capsule,
maintenance-lifetime and shared SQL-worker regressions do not execute distributed
CLI startup. The exact descendant executable, linked archive, startup failure
and physical shutdown gates remain required before this wiring or defaults are
qualified. Recurring callback CPU adoption remains a separate incomplete gate.

### Shared recurring owner shutdown joins

Graph cleanup and quarantine recovery now publish one terminal join event after
the detached scheduler registration has physically joined. A second concurrent
shutdown caller waits for that event rather than treating the empty registration
as completed retirement. Admission remains permanently fenced, and the joining
caller releases the lifecycle mutex before waiting so callbacks can complete
their final ownership handshake. No-work/manual-runtime shutdown also publishes
the event before unlocking admission.

Two physical recurring callback fixtures hold the callback open while two stop
callers run concurrently. They require both callers to remain pending, scheduler
active accounting to remain retained, and both callers to finish only after the
callback resumes, takes its lifecycle lock and physically joins. These lifetime
checks are prerequisites for shared CPU adoption; neither callback has yet been
migrated to explicit runnable stage authority. Defaults and full integration
remain unqualified.

### Bounded recurring registrations

The recurring scheduler now bounds registration ownership independently of
active CPU and I/O callbacks. Its default ceiling is 4096; backend runtime
configuration can supply a smaller or larger finite registration limit, and
zero is rejected before runtime allocation or node-owner retention. All classes
and both legacy and explicitly runnable callbacks share that ceiling. A full
registry rejects with ResourceBudgetExceeded before allocating a task packet or
growing the registry. Pending installers and detached callbacks retain their
reservation until allocation rollback or physical join and task destruction.
Scheduler destruction waits for those in-flight owners as well as active work.
Allocation runs outside the scheduler mutex to preserve reclaimer progress.

Snapshots expose admitted registrations, all physically owned registrations,
the ceiling and capacity rejection count. This bounds scheduler packet and
registry metadata; it does not account callback inputs, durable owner queues or
all process memory. Public operator sizing, production callback CPU stages and
the complete overload/performance/default gates remain required.

Physical fixtures require that detaching a stalled callback does not admit a
replacement, that capacity becomes reusable only after its join, and that task
and registry allocation failures restore the reservation without leaks. A
runtime fixture verifies the birth configuration is actually applied.


### Fanout CPU accounting under protected control

Scheduler `runnable_slots` includes ordinary and protected control grants.
Hosted fanout checks ordinary occupancy after subtracting `control_slots`,
while checking protected occupancy against its own configured reserve. A
peer's protected callback may overlap this child's ordinary grant at C1.
The previous total-versus-ordinary assertion rejected that valid overlap.

A deterministic production capsule/transport-boundary fixture reserves both
children before parking the coordinator, parks one child, then holds protected
control while the second owns the sole ordinary CPU. It restores each child's
original query phase without retiring the result capsules, checks unchanged
retained bytes including capsule metadata, and retires both before restoring
the coordinator. The actual hosted-table-read fixture remains subject to the
cumulative independently compiled consumer gate. This fixture correction does
not qualify default pooling or the wider overload/performance matrix.


### Native checkpoint warming shutdown

The native warming owner introduced by main now publishes one shared physical
join event. The first stop caller permanently fences admission, detaches the
registration and cancels/joins it outside the lifecycle lock. Every later stop
caller waits for the same event; detachment never authorizes freeing the DB
while a warming turn still borrows it. Empty owners publish the join immediately.

A physical recurring fixture holds the warming turn open through two stop
callers, verifies neither returns early, takes the lifecycle lock from the
retiring callback, observes the original cancellation token, and verifies the
registration's active accounting retires after both callers join. Existing
warming cadence/retry checks run in the same focused maintenance gate. Actual
DB shutdown and archive integration remain cumulative gates; warming still
requires CPU-stage adoption and finite provider work before default qualification.


### Original native warming pass deadline

A production warming turn now borrows one Pass created before callback entry,
with a ten-second monotonic deadline and the owner's original cancellation.
State preparation and capture consume that budget. Capture uses the earlier
of its own request/expiry limit and the pass deadline; repository warming no
longer starts a new ten-second budget after capture. Stage fences reject expired
publication work, and the outer callback fence preserves DeadlineExceeded when
boolean provider cancellation reports Canceled or a provider swallows expiry.

The focused fixture exercises expiry after preparation, provider cancellation,
swallowed expiry, successful publication, and rejection before callback entry.
Physical callback shutdown and previous fanout regressions remain in the same
gate. This does not prove that every filesystem syscall or provider can be
physically interrupted; actual DB capture/archive integration remains pending.
Warming still uses legacy CPU scheduling until its capture/publication holder
graph adopts mandatory reentry and bounded CPU stages. Defaults stay unqualified.


## Duration coverage audit (2026-10-16)

These are component defaults in the current sources. Listener owners can supply
overrides; the values do not establish one end-to-end application deadline.

| Owner / stage | Component default or contract | Scope still requiring proof or integration |
| --- | --- | --- |
| HTTP header receive | 30 seconds in `httpx.ServerConfig` | Header receipt only |
| HTTP body receive | 120 seconds after headers | Body receipt only; no application execution budget implied |
| HTTP response write | 30 seconds per socket write | No total streaming-duration ceiling implied |
| Pgwire startup / idle connection | 5 seconds / 60 seconds | Session lifetime remains distinct from query lifetime |
| Pgwire idle transaction | 30 seconds | Existing transaction/cursor retirement ownership still applies |
| Pgwire statement | 30-second server limit; a shorter SQL setting tightens it | SQL timeout and hard transport cutoff remain separate; zero does not disable the server ceiling |
| Public HTTP request/stream processing | Canonical `admission.max_request_duration_ms` defaults to 300,000 ms; public transport retains tighter operator caps | H1 ingress/body/dispatch/output and H2 framing/handler/output paths have focused evidence. Opaque controls interrupt H1 reads/writes and H2 mailbox/credit waits. Native-clock projection and worker cancellation pass pipe/H2, production-callback and sixteen-case compiled integration checks. The subsequent clock intersection fix has owning-module evidence; the compiled integration predates that fix |
| Common application context | `RequestContext.deadline_ns` defaults to null | Other ingresses/owners must still establish their finite operation ceilings |
| HTTP search body timeout | Missing or null `timeout_ms` yields no body-derived deadline | A supplied original deadline is intersected, never extended; the body helper supplies no default ceiling |
| Shared owned Go SDK client | One-second local queue ceiling and 90-second operation ceiling, starting before admission | Shorter caller deadlines win; supplied HTTP clients retain their timeout policy; body retirement still owns admission |
| Default owned internal Go admin client | 90-second operation ceiling, including queue time | Explicit shared/supplied clients retain caller timeout policy |

The HTTP phase defaults are declared in
[the transport configuration](../../lib/httpx/src/server/server.zig).
Pgwire limits and its separate statement deadline are in
[the protocol owner](../../pkg/antfly/src/pgwire/protocol.zig).
The optional application deadline is in
[RequestContext](../../pkg/antfly-embedded/src/api/operation.zig);
the HTTP body timeout helper is in
[the query contract](../../pkg/antfly-embedded/src/api/query_contract.zig).

Before qualifying the full defaults or retained-state borrowing, complete the
ordinary HTTP request/stream lifetime policy across planning, admission,
execution and delivery. Keep one original deadline, preserve stricter caller
budgets, and separate accepted durable work's bounded recovery ownership.
Transport phase timeouts and client queue limits cannot substitute for that
application policy. This audit records source contracts; it does not qualify
every deployed listener or backend timeout path.

The five HTTP ingress budget helpers now intersect every narrower forwarded
budget with an existing application deadline while preserving its clock. They
continue to validate malformed metadata even when an earlier deadline is set.
The host also transfers the original deadline across the checked API boundary
for linked dispatch and internal-service authorization (introduced in API ABI 47;
the current HTTP callback layout requires API ABI 48). Focused
clock, layout, direct dispatch and compilation-analysis checks pass; actual
separately compiled runtime integration now passes all sixteen named API/pgwire
cases through API ABI 48. The subsequent native deadline conversion/intersection
follow-up has focused evidence and is not covered by that compiled profile.

The remaining server lifetime implementation must establish the finite ceiling
before expensive work and retain it through physical response writer retirement.
Buffered HTTP/2 delivery now uses the controlled header/data path with a
stream-local ceiling. A connection socket deadline would incorrectly couple
unrelated streams. H2 header-fragment ingress now has physical stall evidence.
Synchronous opaque body/stream callbacks now carry borrowed checks and remaining
budgets, with scoped controls restored before return. All 83 scoped build steps,
14 lifetime cases, 15 adapter cases and 1,000 targeted pipeline repetitions pass;
overlapping selections are not independent counts. Explicit native-clock inference
projection and worker propagation now pass 104 scoped build steps, including
real pipe cancellation during H2 credit waits and production streaming callbacks.
CPU wait hooks are installed before worker lazy-body reads; callback borrows remain
held through acknowledgment or fencing. Fresh separately compiled API/pgwire
integration passes all sixteen named cases for that profile. The subsequent
clock fix passes 38 owning-inference-module cases and sixteen lifetime cases;
older host-root aggregates did not discover the two new inference cases and
cannot qualify them. Durable work already accepted by
storage needs its independent completion owner after the caller's wait expires.


Worker progress now has one atomic reporter gate per live RPC Request, a
100 ms minimum reporting interval and fixed 2 KiB JSON encoding scratch with
256/32-byte UTF-8 model/backend prefixes. Dropped advisory reports retain no
queued payload. Gate contention returns immediately and does not park the
native execution loan. The final worker-progress-native-v4 receipt passes all
27 scoped build steps, including the actual production callback over pipes,
RPC cancellation, and physical H1/H2 owner checks; its verifier rehashes current
source/schema inputs and exact named cases. This bounds child encoding and
reporter multiplicity only: the first transport offer can still wait, and parent
progress still reaches the enrichment coordinator's uncancelable mutex and
activity hook. Independently charged bounded control callbacks, their elapsed
profiles and full resource/default qualification remain required.


The enrichment reporter's coordinator-mutex/activity-hook wait described above
is now removed from that callback. A fixed per-runtime mailbox copies one
advisory sample (256/32-byte label prefixes; native size asserted <=512 bytes).
One try-lock attempt admits a publisher or drops its report, with no allocator,
observer invocation, task spawn or native-loan checkpoint. Status readers and
ordinary owning activity boundaries consume without waiting for a publisher.
Lifecycle generations invalidate queued records at completion/publication/clear
and after joined stop, before restart or detached reload. The actual stop
regression failed before its fix and passes afterward; a held-coordinator-mutex
production test proves the reporter returns while that mutex remains held and
that hooks run only later at the ordinary owning boundary. Current
`enrichment-progress-native-v2` passes 70 focused build steps, including seven
mailbox-suite bodies, 21 visibility-wait cases and existing physical worker
checks; the independent verifier rehashes current inputs and named inventories.
These scopes overlap. Prompt cached notifications still need their own bounded,
admitted owner with observer detach/join and downstream publication ownership.
Neither that control CPU stage nor the first ordinary worker offer/pipe wait,
Linux/default envelopes or current compiled API image are qualified here.


The existing data runtime status owner now consumes progress through shared
background admission, with one original native two-second cooperative deadline
recorded before durable queuing. Coalesced wakes preserve it. Failure retains
level-triggered debt and imposes a one-second retry delay, including durable
queue rejection; publication-mutex rechecks prevent stale wakes bypassing retry
or shutdown fences. Active batch/phase telemetry keeps the existing one-second
poller live after the replay target catches up. Source audit corrected the
prior overly strong claim that cached progress updates only at ordinary
lifecycle boundaries: pending-work polling already existed. No subscriber
callback is retained by the reporting mailbox and no notification thread is
added. The queued job uses its published backend and retains its execution
capsule until physical observations/cleanup return; health keeps the protected
control lane. Current status-refresh-admission-native-v7 passes 68 scoped build
steps with exact named-case verification: three real scheduler contracts, five
DataServer contracts among twelve bodies, and the existing mailbox suite. The
concurrent-wake case failed before the fix and passes for retry/shutdown modes
afterward. Cached visibility-target success is not a fresh 21-case run. The
verifier rehashes current native/schema inputs and both pass/failure logs.
Only pass metadata plus its capsule is newly charged. Deep metadata/DB/lock
waits, snapshot/catalog private heaps, incremental large-catalog resumption,
actual compiled-image/provider progress, Linux/default envelopes and all
broader design requirements remain unqualified. The cooperative deadline is
not a proven hard two-second return bound.


Status table paging now retains one sweep cursor across admitted jobs and limits
attempted tables to eight per pass. Each collected table has a scoped owner until
publication consumes it; failure before publication deinitializes all nested
statuses. Presence pruning is separate from payload replacement, preserving
unsampled tables. Page publication inherits premetadata lifecycle/target fences,
including rejection of competing cold-entry initialization. Current
status-refresh-paging-native-v5 passes 87 steps; its verifier checks twelve new
named paging cases, current native/schema hashes and actual cache/data owner
inventories. The visibility target remains cached success. Catalog preparation, token/pruning allocations and
within-table group collection remain proportional to catalog/table size and are
not newly covered by the pass metadata charge or a hard elapsed bound.


Status observation is now resumable within a table: at most eight group attempts
per admitted pass, with a fixed upper-ID fence and attempt-before-observation
cursor. Each payload is scoped until its partial publication returns; unsampled
siblings retain their facts. Group absence retirement destroys at most eight
payloads per call outside the cache mutex, preserves post-snapshot publishers,
and rejects older callback generations after retirement. New read mirrors cannot
retain deleted groups across empty-table or rebuild-failure paths. The original
execution capsule remains charged until the synchronous work physically returns.
Current status-refresh-groups-native-v2 passes all 87 scoped build steps; its
verifier checks sixteen named paging contracts and current native/schema hashes.
The visibility target remains cached success. Catalog/presence allocations,
per-status index payloads, candidate scans, read-view reconstruction and authority
reductions still need bounded stages and explicit heap accounting. These count
bounds do not prove a hard elapsed or memory envelope; the full goal stays active.


Read-group retirement now detaches fixed immutable-position slots and releases
at most eight read-payload references using stack scratch. It no longer clones
all sibling statuses or rebuilds the full view during pruning. Readers pin each
live payload under the read mutex before cloning; concurrent retirement before
pinning omits the group, while retirement after pinning preserves the reader's
owned payload until completion. Single and partial publication can refill an
existing empty slot. New IDs and lifecycle replacement still build topology.
Slot/index allocation remains at that topology's capacity; deep retired payloads
are released outside both cache mutexes, and a completely empty view yields no
snapshot. Current status-refresh-read-retirement-native-v2 passes all 87 scoped
steps; the verifier checks eighteen named paging contracts plus the existing
read-payload ownership case and current native/schema/log hashes. Actual threaded
snapshot races, next-allocation failure and sibling pointer preservation are
covered. This is not a proof of bounded catalog/presence scans, reader work,
new-ID topology construction, authority reductions, per-payload bytes or physical
return time. Original production owner wiring and broader full-design scope stay
active; no Linux/default/current-compiled-image qualification follows.


Remote owner fixed metadata now has its own persistent scheduler baseline:
State plus outbound slot/destination arrays, inbound coordinator/duplicate/entry
arrays and durable fence-store arrays/serialization buffer. Endpoint, capability
and gate baselines remain separately owned. Capacity is checked before durable
enrollment, and State reserves before allocation or journal callbacks; successful
owners retain the charge until physical destruction. Initial allocation and late
activation failure roll back every reservation. Tests compare reserved bytes with
the actual initialized array lengths and element sizes. Idle owner metadata uses
no operation or runnable slot. Current-source audit corrected earlier claims that
worker leases and SQL bindings were absent; those production paths already exist.

The newly charged default-node regression exposed insufficient control metadata.
Automatic managed defaults now cap scheduling metadata at 16 MiB/control at 8 MiB,
with automatic control capped at half the process-clamped metadata envelope and
the generic 64-KiB minimum (or smaller explicit ceiling). Fixed policies remain
exact. The existing 64-MiB process case stays at 2 MiB/1 MiB. Small nodes can still
refuse complete protocol bootstrap; no reservation is silently omitted. Current
remote-owner-baseline-native-v5 passes 82 steps, thirteen named baseline/control
cases and 110 scheduler/configuration protocol tests; the independent verifier
binds current hashes and the earlier failing default-budget receipt. The opaque
consumer compile check passes; no current compiled-image, Linux or full default
process profile is claimed. Previously published journal/process recovery remains
closed pending physical-quiescence proof, and capability families remain disabled
by default. Dynamic transport/per-query heaps and the full original scope remain
outstanding.


Clean restart can now use a root-signed fixed 208-byte joined-checkpoint sidecar.
The producer follows joined inbound references, query parents, control callers,
endpoint leases and both HTTP executors; outstanding/queued outbound work or
startup fences suppress it. Recovery-blocked empty registries cannot produce it.
The signature binds exact journal digest and initialization plus stable enrolled
identity and prior reporter. The exclusive journal lease protects matching;
startup's durable outgoing epoch advance invalidates the previous receipt while
preserving publication history and requiring normal destination fences. Heap and
executor setup precede consumption, so late allocation refusal is retryable.
No task, unbounded payload or per-connection receipt owner is introduced.

Current remote-owner-recovery-native-v5 passes 82 steps, eighteen named owner/
recovery/control cases and eight fresh config tests. The scheduler target is
cached success; its source matches the earlier passing baseline receipt. The
verifier binds current native/schema/log hashes and exact names. Restart tests
use real filesystem/signatures/owner lifetimes but inject remote ACKs, so they do
not establish a live cluster or separate-process crash proof. Missing/stale/bad
receipts and uncertainty keep recovery closed. A crash or activation failure
after the epoch advance still needs additional evidence. No Linux/current API
image/default capability qualification or full-design completion is claimed.

The reconciliation-bound and endpoint-reference teardown guards are unconditional
in the receipt producer, including builds that omit debug assertions. This does
not qualify a ReleaseFast production image or change supervisor join obligations.


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

## Production SQL numeric coordinator scratch (2026-10-17)

`sql_execution.Adapter` owns optional fixed-stage metadata in the foreground
partition through scheduler detachment. The original runnable root retains its
operation, byte reservation, deadline, cancellation and read guard. A paid CPU
reentry selects fixed scratch or the serial zero-increment alternative using a
bounded local CAS and bounded node reservation. The linked node parent is captured
during setup; dispatch does not resolve a context, reclaim or allocate.

`allocation_budget.Fixed` owns the admitted byte credit and one physical buffer.
Its allocator uses the foreground partition's raw backing, so it cannot charge
that payload twice. The buffer is freed before `Fixed.deinit` returns the node and
partition charge. Participant metadata uses ordinary foreground accounting and
is freed last. Stage preparation/coercion and returned SQL values keep their
existing allocator contracts; this receipt does not establish complete ownership
for every one of those allocations. Persistent HBC buffers are not rebound to an
operation-scoped owner.

Cancellation/expiry after acquisition, policy/resource refusal, node contention,
partition pressure, missing bounded callbacks and physical OOM are exercised by
`sql-scratch-bundle-verified.json`. It verifies 163 case executions, exact current
inputs and API 49 layout tests. The production API owner and DataServer callback
semantic checks pass. No full production image/default-load or heavyweight Linux
qualification is claimed; remaining allocation/resource families and the original
731-line design remain in scope.

## Aged admission barriers and physical continuation progress (2026-10-17)

The original runnable owner retains every operation, metadata, deadline and
cancellation charge while a large admission drains. Its queue-age protection is
observed by the owning wait poll outside scheduler locks. Admission barriers hold
no partial memory or I/O bundle and spend the same class deficit as other grants.
Retained continuations keep bounded discovery and weighted progress while the
team's earned admission credit stays reserved.

The new physical test parks a continuation with 60 bytes of memory credit, two
I/O units and a live eight-byte buffer. A younger three-CPU request needs the
remaining 40 memory bytes and eight I/O units atomically. Once aged, its barrier
blocks fresh starts but lets the physical owner resume. The buffer is freed and
its credit retired before the large bundle acquires memory, I/O and CPU. Original
cancellation/expiry and protected health/cleanup contracts retain their owners.

`scheduler-age-verified.json` binds the current 173 executions, source-derived
case inventories, source snapshots, 4,690 native/schema inputs and original
design hash. Broader resource families, general measured service, distributed and
durable retirement and production default profiles remain in scope. C ABI and
configuration layouts are unchanged by this scheduler policy.

## Measured ordinary CPU-lease service (2026-10-17)

The stable owning execution acknowledges active service after successful wait.
It samples its original clock outside scheduler locks at each CPU return and
before slot-width or clock-domain changes. The original class owns the estimate
and observed slot-time even through a CPU loan or a later class transition.
Physical resources and retained metadata keep their existing join/retirement
owners. No new allocation or root is needed for service accounting.

Six fixed signed class ledgers retain bounded debt and fractional rebates beyond
individual root retirement. Credits and corrections remain capped, counters
saturate and unobserved clocks keep conservative estimates. Team shrink preserves
one continuous lease charge; growth closes the old grant and starts a new one.
Paid team credit remains protected while existing continuations retire. Accepted
completion and cleanup preserve their original independent progress contracts,
and their new clock domains cannot contaminate foreground measurements.

The current `measured-service-verified.json` verifies 182 executions against exact
sources and the original design. Native `serviceStats()` is currently an internal
observation interface; opaque projection, complete operator/phase coverage and
production default/overload profiles remain open. Slot-time describes occupied
execution authority and is not a physical CPU utilization or billing measure.
All original resource, distributed, durable, SDK and Cloud obligations remain.

## API-owned service telemetry snapshot (2026-10-17)

The API-owned scheduler copies policy, ownership counters and service accounting
under one owner lock. Projection and metric formatting occur after releasing that
lock. The C snapshot has no borrowed resources: version/size/model/availability,
fixed counter arrays and unsigned debt/rebate gauges fully own the observed value.
It conveys neither execution authority nor fresh deadlines.

API ABI 50 and existing typed output contracts protect the expanded runnable and
workload layouts. A checked opaque read rejects old/short owners before callback
and validates the service header and bounds. Missing telemetry remains explicitly
unavailable and emits no fabricated usage samples. Labels remain fixed server
classes. Native slot-time does not establish physical CPU billing or full phase
coverage, and separately owned workers remain an audit item.

The service baseline `service-telemetry-verified.json` records 211 executions / 200 distinct
source cases, exact source/log/binary inventories and the original full design.
The actual C export, opaque getter and all nonzero accounting metric families
pass. Fixed-policy profiles, remaining owner/phase/resource coverage, adaptive
policy and the broader distributed/durable/SDK/Cloud requirements remain open.


## Grouping execution control and remaining aggregate resource work

The main native aggregate runtime forwards its existing coordinator scope and,
where there is no coordinator, its lifetime-only scalar control into `Grouped`.
The coordinator callback retains full read-resume policy validation and may yield
its original CPU lease. The scalar callback retains its existing authority; the
operator does not grant it permission to yield or perform provider I/O. Child
reducers do not inherit the parent's mutable execution capsule.

Controlled batch hashes, raw text memo hashing/equality, dictionary-hit polls,
row hashes and retained-key equality now carry this owner. Text memo growth
rehashes fixed-width integer keys rather than entire unpolled strings. Numeric,
array and JSON comparison/hash work keeps the original per-value limits and
hashes; the shared parent supplies checkpoints without adding a query-wide work
limit. JSON object-key lookups prehash cooperatively before entering a read-only
map probe, propagate equality callback failures after stopping that probe, and
do not introduce fallible callbacks into map mutation.

The preceding hash-control baseline, `grouped-work-control-verified.json`,
records 89 executions / 88 distinct source cases and exact snapshots. Its eight
grouping-work contracts cover wide-key cancellation, allocation-fault unwind,
legacy hash/work equivalence and queued-write progress. The baseline protocol
receipt `grouped-work-control-protocol-verified.json` passed its configured
HTTP/pgwire composition. Both are historical after the retained-key edits below;
neither is presented as a current-source protocol qualification. Current focused
coverage includes those eight grouping-work contracts again.

The follow-up retained-key component now controls the in-memory main group's
admission lookups, complex byte estimates, key dictionary lookup/insertion and
payload copying. Its preceding baseline evidence is `grouped-retained-key-control-verified.json`:
105 direct executions / 104 distinct source cases (99 named plus five import
checks), all 4,692 inputs and eleven source snapshots. Text dictionaries use
fixed-width raw-hash heads and separately owned collision links, including
cooperative exact equality; map growth invokes no fallible key callbacks. Copy
work polls every 4,096 bytes, and stored-array estimates include the same physical
payload/view metadata as the legacy counter. Dictionary sharing and the existing
high-cardinality flat representation remain intact. The API owner checks both
estimation cancellation before any reservation and cancellation after a vector
has physically allocated, under the same root/deadline/retained-state owner.

The preceding aggregate-state baseline is
`grouped-aggregate-state-control-verified.json`: 122 direct executions / 121
distinct source cases (116 named plus five import checks), all 4,692 inputs,
five binary/log inventories and fourteen source snapshots. Main scalar/vector
state updates, DISTINCT/pattern values, extrema comparisons/copies, numeric
preparation/merge/snapshot/finalization and global four-lane reductions carry
the same work owner. Numeric reducer work allowances and cache lifetime remain
unchanged. All fallible numeric preparation precedes live bucket mutation;
the bounded commit region retains CPU and has no new fallible callback.

A failed group cannot publish row, batch, typed or encoded partial results or
export into spill, and cannot be merged as a valid source. Merge errors retire
an incomplete destination. Partial-read errors retire the cursor's group,
preventing a cleared test cancellation from exposing a later partial after a
failed read. The original first error still returns to the request owner.

This does not qualify every grouping phase. The worst-case latency of bounded
non-suspending numeric commits (at most the existing one-MiB state bound),
standard-vector growth, durable partial encode/decode, spill/merge I/O, output
phases, other operator creation paths and independently owned workers remain
audit items. Generic typed-store batch import keeps its existing null control;
its compatibility and allocation cleanup are regression-tested, not credited
as new original-owner propagation. All required memory/I/O bundles, physical
distributed recovery, durable ownership, SDK coverage, Cloud, adaptive policy
and representative default profiles remain within the original full design.


The retained-key estimate/copy change above includes
`Grouped.canRetainAdditional -> Store.appendBytesControlled` and complex
aggregate input byte estimates. The previously recorded raw dictionary lookup
and `datumBytes` traversal gaps in that main in-memory admission path are closed;
aggregate-state work and resource bundles remain separate requirements.


## Optional grouping batch working set

Main grouping now reserves one optional buffer for group IDs, Wyhash states,
nullable results and a fixed-capacity memo index. With a coordinator source,
that entire buffer is acquired with the original paid CPU continuation and
consumes one reserved physical allocation. Its storage is freed before its
reservation returns. Already supplied hashes need only the ID portion. Batches
above the native 4,096-row accelerator domain take the existing serial path.

Text and dictionary-ID memo entries share a preallocated open-addressed index,
with complete controlled equality on collisions and per-column generations.
At most one unique entry per input row and at least twice as many slots avoid
growth or a full-table probe. The accelerator has no memo heap allocation.
Decoded cells and row views remain in their separate required query budget.

Only denial of the optional reservation or failure of its one physical buffer
selects serial fallback. Work, policy, cancellation, deadline and decoding
errors propagate; OutOfMemory from a callback is not reclassified as scratch
denial. Current focused evidence is grouped-batch-bundle-verified.json:
126 executions / 125 distinct source cases (120 named and five import checks),
all 4,692 inputs and fifteen source snapshots. Tests include maximum memo size,
dictionary-column isolation, exact null/numeric hashes, one-shot physical
denial, original callback/decoder OOM, large-batch serial behavior and unchanged
ordinals. The real API owner contract checks the entire buffer's physical
retirement under the original root/deadline in all seven modes for both input
forms. Current HTTP/pgwire integration also passes against these combined
changes; grouped-batch-bundle-protocol-verified.json binds the exact binary,
one registered case and all 4,692 source inputs. Its six preparation schedules
retire all operations/bytes and let the cheap peer complete first.

This is the optional batch accelerator's working set. Required row/decoded/
retained state, all I/O families, HBC, independent workers, opaque images,
default profiles and the other original requirements keep their separate
contracts and open audit items.


Explicit read-group and transition-reservation exhaustion now produces
ReadMemoryCapacityUnavailable, independently mapped to SQLSTATE 53300 / HTTP
503. Failed reservations leave pool counters unchanged; release permits a
subsequent reservation. The updated unpublished runnable-read owner regression
verifies complete cleanup on transition refusal. Physical backing allocation
errors retain their identity. read-memory-capacity-verified.json covers 17
focused cases and all 4,692 inputs. Allocator-interface quota provenance and
a real saturated frontend round trip remain open; prior whole-native receipts
are historical after this update.


Required regex lane objects now use MemoryBudget.create, which returns this
allocation attempt's physical or quota failure through known nested budgets.
Physical failure after recovered quota refusal stays OutOfMemory at this
creation boundary. Generic allocator calls keep their existing contract. Failed
backing/parent admission returns previously reserved child credits; successful
objects retire through the normal budget allocator. No failure history or
shared mutable error slot supplies the classification.

checked-memory-cause-verified.json covers 138 distinct source cases, including
six budget and four regex-owner cases with credit rollback/reuse, concurrent
workers and allocation-failure cleanup. It binds all 4,692 inputs and seventeen
snapshots. This is required regex lane creation, with focused SQL/grouping and
API-owner regressions; it does not complete generic quota provenance, native
matcher allocation translation, frontend overload round trips or the other
original resource/default/durable requirements. Earlier whole-native receipts
are historical after the three source edits.


Configured worker launch JSON, data path, environment strings/map and parse
scratch now use the node-admitted Client retained allocator. Environment parsing
waits for configure; all retained views publish together after allocation, then
bootstrap copies retire physically. A failed publication cannot start a worker.
Configured bytes survive worker generation retirement and are freed at Client
destruction; detached outputs independently retain their budget/context after
that destruction. worker-configuration-memory-verified.json binds 82 native and
11 subprocess cases against all 4,692 inputs. Initial bootstrap Client/capsule
allocations before configure, fixed Client/budget objects, stack/task overhead
and remaining worker/backend domains stay open. No full production/default-load
qualification or full-design completion is claimed.


Worker Client bootstrap now reserves each allocation, including its owner/Client/
payload-budget metadata, through the existing process memory parent before
backing allocation or launch copying. Configured payload backing and object
allocation have separate owners so the node and bootstrap do not charge the same
physical data twice. Physical metadata destruction precedes release of its last
node context. Retained outputs preserve their metadata allocator until final free.
Internal inference ABI 39 adds the checked bootstrap-parent create tail; the
parent is borrowed through all retained results and never serialized into IPC.

worker-bootstrap-memory-verified.json covers 85 native plus 11 subprocess named
cases, all 4,693 inputs and seventeen snapshots. Exact real parent/facade counters
verify metadata baseline plus data versus data alone, anchor retirement, and zero
after final free. Parent selection and old create-layout rejection are checked;
complete production/legacy/WASM startup images are not qualified. Other host
startup heaps, stacks/tasks/allocator/backend overhead and the original broader
resource/durability/default/adaptive requirements stay open. Earlier configured
metadata evidence is historical after this bootstrap change.


Parent-backed inference-host creation now uses the process parent's metadata
allocator for owned settings, paths, preload records and the host state object.
Supervised-child creation uses its existing admitted RPC heap. Backend/model and
routing backing stay in their separate original contracts. Parsed settings and
preload/path strings own their input values; the child launch parsing arena
retires after initialization while retained settings remain valid and charged.
Host state and linked resource-context metadata retire physically before their
last node capability edge. Unparented in-process creation remains an open gap.

Current host-creation memory/contract receipts cover 114 distinct native source
cases plus eleven subprocess cases (146 raw native executions; 32 duplicated
across compositions), all 4,693 inputs and seventeen snapshots. Tests include
metadata allocation-failure cleanup, caller-buffer mutation, actual model-free
creation/destruction and actual-pipe child initialization with an empty launch
arena followed by zero charged metadata. Existing placement/preload and ABI
regressions pass separately. Backend/model/routing/stack/task overhead, other
original resource families and complete default/profile qualification remain
open; no full startup/model image or full-design completion is claimed.


### Embedded inference startup parent — 2026-10-17

The shared `LiteInferenceResourceOwner` now owns a process-envelope-sized
`ResourceManager` before embedded host creation. The host borrows its memory
parent for admitted creation metadata and retains its capability on configure.
The service's base reference protects error unwinding and normal host teardown;
retained capability references protect later resource-context retirement. The
parent is independent of any database generation, matching shared service
ownership across cloned handles and retired snapshots. Final owner release
validates that all child metadata credit has retired before freeing the manager.

This covers the embedded caller's previous null-parent host metadata path.
The owner/manager's own fixed allocation, outer IO/options copies, model/cache
admission, backend allocations, and a common ceiling across storage or independent
services remain open. The model/cache resource callbacks are still permissive.
No public or internal ABI version changes accompany this implementation.

Current evidence: `embedded-creation-memory-verified.json`, final v5 build log,
exact direct executable lists/results and 4,693-source-input manifest. All 104
named cases pass (93 native, 11 subprocess), including four new owner/startup
cases and real model-free creation/configuration/destruction through the native
function table. The companion inventory selects zero matching cases and is
explicitly excluded from inline-codegen startup qualification. Full production
images, real models, Linux/WASM execution and the original full design remain
unqualified. See the implementation document for the retained failed attempts,
corrected 12 GiB test-compiler scheduling estimates and broader remaining scope.


### Embedded admitted models and buffers — 2026-10-17

The service-owned manager now accounts startup metadata alongside admitted
model/KV/scratch leases, tokenizer growth and transient buffers. Exact batch
leases reside in 256 fixed slots; monotonically issued tokens are never reused,
including on integer exhaustion. Preparing/retiring slots stay occupied while
manager operations run outside the lease mutex. Stale releases and invalid
retention cannot subtract a successor's reservation. Three fixed observer banks
provide 256 slots each, validate identity/previous totals and bound manager
ledger cardinality. Reduction and release allocate no new registry records.
Logical backend metrics and physical host charge follow standalone's platform
policy. The checked batch entry point distinguishes physical allocation failure
from capacity pressure while the existing classified API preserves its policy.

Tokenizer/transient growth refuses pressure before allocation. Accepted prompt
observations record existing physical bytes even above the ceiling; prompt
preallocation admission and failed-observation physical reconciliation remain
open. The fixed owner and ledger overheads remain outside these byte counters.
A common ceiling across storage and independent services, broader backend/heap
ownership and all other original resource families still require work.

`embedded-admission-verified.json` binds 104 native + 11 subprocess named cases,
19 snapshots and the current 4,693-input manifest, including concurrent model
admission, fault cleanup and real detached `PayloadBudget` ownership after both
original anchors retire. The production native table's model-free startup still
runs. The companion inventory is empty and adds no inline-codegen qualification.
This is focused macOS evidence; real models, production images, Linux/WASM,
representative defaults and full-design completion remain unqualified.


### Physical prompt-cache host ownership — 2026-10-17

Parented inference hosts now construct portable prompt-cache allocations with
the existing admitted bootstrap allocator; supervised children use their
parent-admitted heap. ModelManager supplies that allocator through its canonical
cache factory. CPU pool buffers, cache metadata and retained prefix storage
therefore require admission before allocation and retain credits until physical
free. Logical eviction does not refund pinned buffers or physical free-list
storage. Unparented legacy construction still falls back to its base allocator.
CPU logical observations are suppressed where physical allocation is admitted;
Metal/CUDA observations remain enabled because device buffers bypass this
allocator. Cache live-byte metrics still describe logical estimates.

Pool acquisition now publishes IDs and both dequantization scratch buffers only
after successful allocation. Retirement metadata is reserved before publishing
a block, allowing final-reference release without allocation under pressure.
Failed sequence-table publication returns the newly acquired block. Metal/CUDA
hook metadata uses its construction allocator for both allocation and teardown;
the Metal ownership fixture exercises host allocations without touching a GPU.
The child heap uses mandatory parent-node admission instead of inheriting the
RPC payload's unrelated 256 MiB cap. The transport cap remains unchanged. A
300 MiB request is denied by the real parent handler before backing allocation;
this does not qualify a real large-cache execution.

Internal host ABI 40 fences older cores/workers lacking this physical ownership
contract. Existing worker launch validation rejects bridge 39 before reading
launch fields. Public API 50, native 8, runnable 15, local 4, storage 100,
MemoryParent 3 and worker wire 13/AFW6 remain unchanged. Historical ABI 39
receipts describe earlier snapshots.

Current evidence: prompt-cache-physical-verified.json binds 33 source snapshots,
the unchanged 4,693-input native manifest and exact direct test inventories:
37 host/worker + 43 RPC/progress + 24 node-memory + 2 standalone + 89 pure KV
cases = 195 native, plus 11 subprocess cases = 206 distinct checks. The final
v5 producer completed all 86 steps. Earlier attempts are retained; v4 corrected
GPU observer selection and made the Metal fixture explicitly reachable from
the host test root. V5 adds the version fence. The companion inventory selects
zero cases and provides no inline-codegen startup qualification. Cache cleanup's
complete log was inspected and no cleanup process remains; its original exit
status was not recovered. Post-cleanup verification rechecks native binaries,
logs and sources plus existing Go (15 top-level/24 subcases, 75 inputs) and
TypeScript (116 cases, 46 inputs) receipt integrity, without rerunning SDK tests.

GPU device preallocation/physical retirement and observation-failure recovery
remain open. Active generation KV reservations can conservatively overlap cache
physical charges; credit transfer is not implemented. Cold free-list trimming
and reclaim remain open, so retained physical storage can keep capacity occupied
until reuse or teardown. Allocator quota versus physical-OOM provenance remains
incomplete. Common Lite/storage/service ceilings, fixed control/IO and other
backend heaps, original SQL operator/IO ownership, distributed physical-failure
recovery, durable handoff, SDK composition, Cloud policy, representative defaults
and adaptive control still require work under the original 731-line design.
Real models, device execution, Linux/WASM, large caches and production startup
images are not qualified by these focused checks. The full design is incomplete.


### Cold CPU prompt-cache backing reclamation — 2026-10-17

Owner-side configure now trims unused native CPU pool backing toward the cache
share after eviction; disabling a cache clears logical entries even with a
nonzero configured byte target and trims unused backing to zero. Paired manager
and storage-runtime pools share that target. Attached-prefix retirement also
trims newly freed backing. Foreign scheduleConfigure only publishes a target;
physical reclamation waits for the owning lane. Referenced buffers remain live.

The pool preserves stable slot IDs and retirement metadata. Trim frees K/V
payloads on the free list, and frees idle read scratch when no backed blocks
remain. Reacquiring an empty slot stages K/V and scratch under the admitted
allocator before removing the free ID or publishing its reference. Allocation
failure restores the same empty slot and zero leaked bytes. Resident free-list
reuse still needs no data allocation. liveBlocks counts slots, including trimmed
ones; free-slot headroom does not bypass physical backing admission. Accelerator
pools are excluded from this trimming path. Internal host ABI 40 and all public,
native and worker-wire versions remain unchanged.

prompt-cache-reclaim-verified.json binds 199 native + 11 subprocess = 210 named
checks, 33 snapshots and the unchanged 4,693-input manifest. Four new pure cases
cover pinned IDs/read data, trim targets, each f32/f16 reacquisition failure,
accelerator exclusion and all three cache modes with paired runtimes. The
production host factory case observes exact parent-credit retirement, refusal
under exhausted capacity, and successful reacquisition after restoring capacity.
V1 retained two sandbox socket failures; both passed the same-binary localhost
recheck. V2 uses localhost access and completes all 86 steps. All source/helper
inputs stayed frozen for each entire build. Exact direct inventories also pass
without skips/leaks. The local companion selects zero cases and adds no
inline-codegen qualification. Earlier whole-native receipts are historical
after these three source changes.

This reclaims unused CPU payloads at owner boundaries. Idle-owner pressure recall,
metadata compaction, GPU physical admission/retirement and generation-to-cache
credit transfer remain open. Pinned bytes may exceed an eviction target while
remaining admitted; trim scans run on the owning lane and their worst latency
is not qualified here. Original shared-process/storage ceilings, other heap and
SQL/IO ownership, distributed physical-failure recovery, durable handoff, SDK
composition, Cloud policy, representative defaults and adaptive control remain
under the original 731-line design. Real models/devices, production images,
Linux/WASM and large-cache execution are not qualified by these focused tests.

Cache cleanup exited zero; its complete log was inspected. The post-cleanup
receipt rechecks current native sources/artifacts and prior Go/TypeScript
receipt integrity without rerunning SDK tests. The full design remains active.

During verification origin/main advanced to 5bbf0234f8 (#1168). Merge
5d79057cd7 integrates its five release/packaging files without conflicts.
The 4,693-input native manifest still verifies exactly; the changed release
policy test and packaging-license fixture test both pass after the merge.
Scheduling implementation changes remain uncommitted on the same branch.


### CUDA cache buffer admission and physical retirement — 2026-10-17

ModelManager's canonical prompt-cache factory now owns a device-budget capability
from its existing shared resource domain. Each buffer credit independently
retains that domain and an exact GPU KV AdmissionLease. Credit records use the
cache's allocator, so parented/supervised host bookkeeping is admitted too.
Closing the cache drops its capability after storage/manager teardown; outstanding
buffer credits keep the domain and external parent capability alive independently.

CUDA cache K/V and block-table allocations reserve the full requested new buffer
size before calling the existing context-local allocator/driver. Old buffers
remain charged during growth. Driver/context/local-limit failures roll back the
new credit. Full allocation descriptors carry credits through deferred retirement,
independent of logical view length. Successful cuMemFree releases the credit;
failed context switches or frees preserve the descriptor and charge for retry.
The deferred queue compacts failed entries instead of dropping them. Credited
KV buffers bypass generic scratch reuse so that cached scratch cannot strand KV
credits; existing stream-completion handling governs deferred physical frees.
CUDA logical cache observations are suppressed where this physical factory
budget and admitted host allocator are present. Metal retains its observation.

Internal inference bridge 41 fences older workers/cores lacking this ownership
contract. The existing launch bridge-version check rejects bridge 40 before
reading launch fields. Worker wire 13/AFW6, API 50, native 8, runnable 15,
local 4, storage 100 and MemoryParent 3 remain unchanged.

cuda-credit-verified.json binds 205 native + 11 subprocess = 216 named cases,
41 snapshots and the unchanged 4,695-input manifest. Both CUDA-enabled full
helpers finish all 90 steps with frozen inputs throughout their lifetimes.
V2 includes CUDA observer suppression and the generic scratch-cache bypass.
Exact direct inventories pass: 38 host/worker, 43 RPC/progress, 24 node-memory,
2 standalone, 93 KV, and 5 CUDA allocation/driver cases. The new mock-driver
cases execute production DeviceBuffer allocation/free, checking growth-peak
refusal before a driver call, capability/descriptor transfer, logical shrink,
driver/context/local-capacity rollback, failed retirement and successful retry.
Four injected error logs are required and matched under strict error accounting.
The factory case checks real parent-admitted record allocation/rollback; a new
host case keeps a device credit across cache and host destruction and verifies
final credit, metadata and capability retirement to zero. The local companion
again selects zero cases and adds no inline-codegen execution qualification.

These tests use a mock CUDA driver; real CUDA hook/device execution, model loads,
large caches, Linux/WASM and production images remain unqualified. Requested
buffer bytes are accounted; driver/library-private allocations and allocator
rounding require separate headroom. Metal physical allocation/retirement,
CUDA cold-pool reclamation and generation-to-cache reservation transfer remain
open. Generation estimates can still conservatively overlap physical cache
credits. Failed frees retain capacity, but permanent device/context-loss
reconciliation is not completed: teardown can leave uncertain credits retained
without a later retry owner. Idle-owner recall, common Lite/storage/service
ceilings, fixed control/IO and other backend heaps, original SQL/IO ownership,
distributed physical-failure recovery, durable handoff, SDK composition, Cloud
policy, representative defaults and adaptive control remain under the original
731-line design. This component does not complete that design.

Cache cleanup exited zero and its complete log was inspected. The post-cleanup
receipt revalidates all current native source/artifact hashes and existing Go
(15 top-level/24 subcases, 75 inputs) and TypeScript (116 cases, 46 inputs)
receipt integrity, without rerunning SDK tests. All implementation changes
remain uncommitted on the existing branch; the full design remains active.

### 2026-10-17 merged main baseline

The existing branch/worktree includes main `acf50f2f60` via `ee4c92b561`.
`origin-main-merge-verified.json` records the focused cumulative run: 206 native
cases, 11 process checks, 91 successful build steps, and 5,035 native/schema
inputs. Main's Metal lifetime observation remains wrapped around newly credited
KV allocations, so telemetry and physical admission retain separate associated
owners. The merge preserves request controls for model-aware media/source
routing. The SDK reader failure path now releases its source reader lock even
when no pull occurred; 537 TypeScript and 137 Python cases pass. Metal prewarm
binding, publication cancellation cleanup, reservation transfer and device-loss
reconciliation remain open. Full GNU qualification is waived; no real GPU or
whole-design completion claim is made.


### 2026-10-17 latest main and publication/prewarm capability ownership

The existing branch includes main `d4d3a4a6ea` via `7b19a3b514`; all saved
implementation entries and archived symlink targets were restored. Prompt-cache
construction owns its budget before publication refusal can occur. GPT Metal
sessions own a metadata-only device budget before post-construction prewarm;
new/recreated providers install it before their KV/frame work. Same-identity
binding allocates nothing; contention and foreign identity refuse before
publication. The session retains the capability across provider recreation and
releases it only after actual session metadata free. Parented Metal logical
observations defer to those physical cache credits.

The corrected direct session teardown test initializes the real prefetch queue;
its earlier sampled uninitialized-fixture wait is preserved as an interrupted
run, not a pass. The final verified receipt records 209 native cases and 11
process checks, 91/91 and 15/15 successful build steps, 5,035 native/schema
inputs, and one selected teardown test. Its full terminal log was reviewed.
Python and TypeScript media streams now enter their existing client admission
owners; source close/cancel must finish before releasing stream capacity.
Python passes 164 focused checks after main's fixture changes. The unchanged
TypeScript sources retain their verified 543 passing/one skipped result.
`origin-main-oct17-post-gc-verified.json` records cleanup and final source,
artifact, SDK and saved-work integrity.

This does not qualify real GPU execution, earlier constructor-time JIT,
reservation-to-physical transfer, device-loss reconciliation, all resource
families or SDK deadline/retry composition. The original SQL/IO, distributed
physical failure/restart, bounded durable backlog, representative defaults,
adaptive and Cloud obligations remain active. Full GNU qualification is waived;
the full design is not complete.


### 2026-10-17 constructor capability and failure ownership

Managed primary Metal loads borrow a temporary owned device-budget capability
only through construction. The architecture session retains it before JIT
preparation. The raw-resource provider initializer owns both raw resources on
entry, installs its budget before JIT initialization, and rolls back failures.
Provider recreation keeps the same identity and allocation-free rebinding.
Architecture rollback may precede prefetch setup; stopping an uninitialized queue
is now safe. Actual session destruction is checked in both queue states and
releases the capability only after session metadata free.

`metal-constructor-budget-v1-verified.json` records 210 native cases and 11 process
checks, 91/91 and 15/15 build steps, Metal/CUDA-enabled options, 5,035 fixed input
hashes, 16 source snapshots and five tested artifacts. The complete 542-line terminal
log was inspected. The corresponding post-GC receipt revalidates this exact
seven-file native change and existing 1,846-input SDK proof; no SDK tests were
rerun by this component. No implementation commit or push was made.

The JIT allocation audit preserves the remaining gap: raw runtime creation and
JIT fixture/timing buffers still have direct host/device allocation paths. Their
individual size bounds do not replace shared admission. They require proper
scratch ownership and reservation transfer, not KV relabeling or duplicate
charges. Real GPU execution is unqualified.

The durability source audit confirms existing reserved completion, derived replay
retention before commit, allocation-free ownership transfer, pre-decision pending
outbox probes, and 16-entry recovery paging. Per-transaction size limits and
conditional retained-source quotas do not by themselves prove aggregate
coordinator/participant recovery capacity or restart coverage. Preserve those
mechanisms while auditing the remaining paths. All original distributed, SQL/IO,
resource-family, default-policy, adaptive and Cloud obligations remain active;
the full design is not complete. Full GNU qualification remains waived.


### 2026-10-17 zero-byte admission owner exclusion

Admission controller ownership now includes a checked live-lease count independent
of bytes. Zero acquisitions, reductions to zero and pending external callbacks
keep the original resource owner configured until physical token retirement.
Refusal removes the provisional lease; byte reductions do not. Repeated release
cannot retire a successor owner's token. No new allocation or callback under the
ledger lock is introduced.

Two original cases reproduced the bug before the fix; all 65 admission-memory
cases pass afterwards. The cumulative receipt records 275 native cases and 11
process checks, with three successful build summaries (91/91, 15/15, 15/15).
The complete 635-line log and focused logs were reviewed after termination.
The post-GC receipt revalidates 5,035 input hashes, 17 snapshots, six artifacts,
the single-file change, exact red/green source evidence, enabled Metal/CUDA
options and the unchanged SDK receipt. No SDK retest, implementation commit or
push occurred. Full GNU qualification is waived; real GPU execution is unqualified.

The transfer audit locates the next integration at batch reservations and the
bridge/worker token protocol. Single-slice transfer and zero-batch identity
preparation already exist. Handoff must preserve aggregate ownership, live-memory
pressure accounting, failure refund, owner closure and escaped-buffer retirement.
The standalone token registry still lacks Lite's fixed-slot/nonwrapping proof.
All JIT scratch, common resource-family, SQL/IO, distributed recovery/restart,
default-policy, adaptive and Cloud obligations remain active; the original full
design is incomplete.


### Atomic batch-credit partition

Storage `BatchReservation.transferCreditTo` partitions existing logical-family
and physical-host credit without returning capacity to the shared pool. Both
owners must already have registered, live, non-prepared identities; descriptors
must match both ledger entries. All vector and host arithmetic is validated
before either owner changes. Empty destinations retain live identity ownership
until explicit release, and empty moves still reject invalid owners. The method
allocates nothing and changes no aggregate usage, peaks, ancestor credit or
pressure epochs.

`atomic-batch-transfer-v1{,-verified}.json` records 37 passing native cases:
14 prepared-batch/accounting and 23 runnable-bundle cases, including four new
transfer tests. Sources were frozen for the build and all 5,035 input hashes
remained stable. The initial test-only cleanup crash is retained separately;
zero-byte destination identities must be explicitly retired. No bridge/worker
transfer ABI, physical allocation refund, source-pool closure or escaped-buffer
integration is proven by these tests. The original full design remains open.

Required cache GC completed successfully. The post-GC receipt revalidates all
5,035 native/schema/dependency hashes, both tested artifacts, archived source and
runner, receipt/log hashes, latest main ancestry and clean merge/diff checks.


### Inference opaque credit handoff (physical funding still open)

Inference leases now transfer all six admitted families through retained host
ABI 43 and worker wire 14 to the actual Lite and standalone batch owners.
Both tokens must already be registered; validation and checked arithmetic precede
external mutation, with no controller mutex held across the callback. Aggregate
policy usage, live-pressure capacity and lease counts remain unchanged. Pending
host bytes move independently from settled residency, and later retain settles
only the retained pending portion. Explicit refusal leaves both owners intact;
uncertain worker replies retain both tracked identities until joined cleanup.
Standalone token IDs now close permanently on exhaustion instead of wrapping.
Its dynamically allocated control metadata still needs a bound.

`inference-credit-transfer-v4{,-verified,-post-gc-verified}.json` records 320
passing native cases and 11 process checks on merge HEAD `11b616a833`, with all
5,035 native/schema/dependency inputs unchanged. The eight new cases cover
controller accounting, denial, incompatible owners, mixed pending/settled retain,
actual Lite/standalone roots, retained host context, and worker token tracking.
Worker reply loss here is a unit simulation, not a new physical-process proof.
Two earlier fixture compile failures and a sandbox-only localhost failure are
retained. Both socket-dependent cases passed with loopback access; the complete
cached v4 run then passed with that access. Metal/CUDA options came from the
actual v3 compile and are bound to the identical v4 host binary and inputs.
Real GPU execution remains unqualified. Required cache GC completed; all source
snapshots, tested artifacts, logs, source hashes and main ancestry were rechecked.
The unchanged SDK evidence was revalidated without rerunning SDK tests.

This supersedes the older audit's missing opaque-transfer and wrapping-namespace
findings. Physical buffers still acquire fresh credit: prepaid pool partition,
allocation-failure refund, pool closure, escaped-buffer retirement, typed JIT
scratch and incremental cache admission remain open. The current durable-session
audit finds existing finite count/record-size admission and bounded recovery
pages; shared-writer bounds, all durable obligation families and restart IO still
need coverage. The concurrent BEGIN map-publication capacity race is resolved by the
subsequent component below. The original full design remains incomplete, and no
implementation commit or push has occurred.


### Durable session publication reservations

Every new session-map insertion now preserves capacity promised to pending
BEGINs. BEGIN prepares space for all existing provisional owners plus itself
before durable persistence; lazy loading and ownership adoption preserve those
slots when publishing their own entries. Checked count arithmetic refuses
unrepresentable requests, and session-limit checks avoid overflowing the sum.
BEGIN's final publication still allocates nothing after persistence.

Three deterministic public-operation interleavings reproduced lost publication
capacity before the fix: overlapping BEGIN, lazy durable load, and ownership
adoption. The fixture pauses BEGIN in its durable allocator, observes capacity
lost to the nested operation, and aborts before an unsafe putAssumeCapacity.
After repair each case also verifies successful durable publication. This is an
in-process deterministic interleaving proof, not a new distributed load test.

`durable-session-publication-cumulative-v2{,-verified}.json` records 34 passing
cases (32 transaction cases and two SQL/configuration cases), 59/59 build steps,
no skips/leaks, and 5,035 unchanged native/schema/dependency inputs. It preserves
the original three failing cases and the five-case focused green run. The wider
first run exposed an existing lease fixture's one-nanosecond sleep versus
millisecond expiry assumption. The fixture now checks nondecreasing expiry on
savepoint creation and exact advancement through the public maintenance method
with an explicit timestamp. That earlier failure and exact source are archived.

Relative to the earlier 320-native/11-process inference receipt, only the session
source and test build graph changed. Inference implementation sources, its tested
artifacts and the 1,846 SDK inputs were revalidated; those suites were not rerun
for this session-only repair. This fixes local publication safety, not the open
shared-writer quota, participant/outbox/restart or full-design obligations. The
older durable-session source audit is historical at this point. Physical buffer
funding/refunds, owner closure, JIT scratch admission and the remaining original
731-line design remain open. Implementation remains uncommitted and unpushed.

Required cache GC completed with exit 0. The session post-GC receipt revalidates
all 5,035 current input hashes, both source snapshots, both tested artifacts,
receipt/log hashes and merge/diff checks.


### Prepaid KV funding for primary and component Metal sessions

The managed loaders now partition resident backend KV credit into a session-owned
pool before entering the backend constructor. Physical buffers receive separate,
already-registered leases. A reservation spends available prepaid credit first
and newly admits only the uncovered increment. Returning a confirmed-free or
failed-allocation credit replenishes the pool; if the owner refuses that refund,
the confirmed-free credit retires directly and subsequent growth must admit the
missing capacity again. No unowned token is abandoned.

Both primary and lazy component constructors use the same budget helper. The
prompt cache clones the exact constructor-installed session capability, so it
cannot replace the provider's funding identity with a process-wide capability.
The source weight lease retains the resident vector with partitioned KV removed;
run admission continues to see the original total residency, including the pool.
KV and scratch now carry distinct typed identities even if their opaque context
pointer matches. KV ports/providers reject a scratch capability.

Session close releases unused available credit after actual session metadata is
freed. Outstanding physical credits retain their pool and resource domain until
confirmed retirement, independently of the weight lease and budget capabilities.
A four-thread regression races physical retirement and capability release against
closure and verifies exact final reconciliation. The Objective-C factory fixture
also covers allocation failure and an aliased buffer surviving owner/domain close;
it uses CPU objects and performs no GPU work. Unsupported non-Metal builds skip
that fixture explicitly instead of linking unavailable Objective-C symbols.

Pool preparation originally propagated pressure before a valid preferred-backend
fallback could run. A regression reproduced that failure by refusing only the
empty destination identity while allowing model-weight admission. Both loaders
now preserve the preparation error and continue through allowed backend
candidates. The unchanged test passes for primary and component loading after the
fix. Earlier compile and fixture corrections remain archived separately and are
not product red/green evidence.

`prepaid-device-funding-cumulative-v2{,-verified}.json` records 329 distinct native
cases: 292 individually printed and 37 standard-runner cases, plus 11 process
checks. All three commands exited zero (97/97, 15/15 and 15/15 build steps), with
all 5,035 native/schema/dependency inputs unchanged. Ten focused integration cases
cover seven pool/fallback cases and the three existing constructor/teardown
contracts. The final memory command selects only its 69 memory cases; the earlier
prototype's repeated filters are recorded as duplicate executions, not additional
distinct coverage. Twenty-five source snapshots and six tested artifacts are
archived. Actual host compiler 34508 supplied the enabled Metal/CUDA options,
bound to this run's input digest. The unchanged 1,846 SDK inputs and existing SDK
receipts were revalidated without rerunning SDK tests.

The seven-source `shared-control-metadata-source-audit.json` identifies the next
shared gap. Common identity ledgers bound tombstone churn but have no internal
live-metadata byte/count policy; the default allocator is not an admission
allocator. Standalone lease/free-list and observer storage also grows dynamically.
Lite's fixed wrapper slots and existing admitted reclaimer allocations remain
real guarantees, but do not establish the missing common bound. A complete fix
must include provisional, prepared and cached metadata, growth peaks, protected
completion/recovery capacity within the same process envelope, and move-safe
startup ownership. The common numeric identity issuer also needs a nonwrapping
retirement guarantee. This audit is source evidence; those changes are not yet
implemented or runtime-qualified.

Request-scoped incremental funding composition, CUDA reservation handoff, typed
JIT scratch and raw constructor host admission, exact GPU allocation granularity,
all remaining SQL/IO/output and durable/restart families, representative default
profiles, adaptive and Cloud policy remain open. Full GNU qualification remains
waived and real GPU execution remains unqualified. The original 731-line design
is incomplete. Implementation remains uncommitted and unpushed on the existing
worktree/branch, with requested main merge `11b616a833` retained.

Required cache GC completed with exit 0. The post-GC receipt revalidates all
5,035 current inputs, 25 source snapshots, six tested artifacts, SDK source/log
proofs, the seven-source next-step audit, and merge/index/diff checks.

### Shared identity table storage and bounded retirement

Reservation, prepared/active batch, and observer identity tables now admit their
backing allocation requests through the common manager and every installed
memory parent. A finite 64 MiB default cap applies to their combined storage even
when the aggregate budget is unlimited. These bytes also consume the existing
aggregate envelope; the cap adds no extra physical quota. Growth admits the
complete new table while the old table remains charged, and removing logical
identities leaves retained table storage charged until physical free. Manager
snapshots distinguish active table bytes, the cap, and retirement debt.

The allocator context exists only for each unmanaged-map operation, so startup
moves retain no allocator pointer into a former manager address. Late parent
installation includes existing startup charges. Single, oversized, batch, and
partial reservation paths preserve metadata admission when committing payload
usage. Partial fallback computes available payload after table admission.
Checked payload retirement guards the table/debt floor. Checked batch admission
continues to distinguish backing allocator OOM from a quota refusal.

Bounded rollback and rehash retirement originally invoked the legacy parent
release callback. Three regressions reproduced that behavior and bypass of a busy
parent's non-waiting refusal. Bounded retirement now uses only the non-waiting release port.
After physical free, an unavailable parent leaves exact credit debt in fixed
manager storage. New metadata allocation first settles existing debt, and the
independent payload-retirement consumer can settle it under its original cleanup
checkpoint even when no payload registry entry remains. Ordinary teardown settles
remaining debt before closing the parent. The unchanged three failing cases pass
after the fix. Two earlier regressions similarly establish actual prepared-table
charging and refusal before backing allocation/publication. An initial empty
filtered run and later fixture/compile corrections are explicitly excluded from
product red/green evidence.

`shared-control-metadata-cumulative-v1{,-verified}.json` records 429 distinct native
cases and 11 process checks. There are 457 native executions: 406 individually
printed cases plus 51 standard-runner executions, including 28 intentional repeat
executions rather than additional distinct coverage. The complete common module
passes 114 cases, including 13 new metadata cases. All three commands exited zero
(103/103, 15/15, and 15/15 build steps); all 5,036 captured native/schema/dependency
inputs remained unchanged. Twenty-seven source snapshots and six tested artifacts
are preserved. Actual worker compiler 23019 supplied enabled Metal/CUDA options
bound to this run's source digest. The existing 1,846 SDK inputs and source/log
proofs remain unchanged; SDK tests were not rerun.

Synthetic payload-limit fixtures now provision and count their actual table
storage before publishing work or parent ports. Lite's fixed-capacity wrapper
fixtures provision all 256 permitted batch/observer owners. Raw metadata refusal,
identity OOM, production startup, and backing allocator failure tests retain their
specific failure paths; none disables metadata accounting. This qualifies the
common module and the cumulative ownership suites, not every repository suite.
The earlier seven-source audit remains historical evidence rather than a current
unchanged-source assertion.

This component does not complete the shared control-metadata design. Standalone
provisional lease objects, cached free-list records and wrapper observer/token
maps still need the common bound. Capacity-domain/replay metadata, protected
completion/recovery floors within the same process envelope, nonwrapping common
numeric identities, and backing allocator/page rounding and owner-header storage
remain open. Existing independently admitted reclaimer storage was not charged a
second time. Request-specific GPU funding, CUDA handoff, constructor/JIT scratch,
remaining SQL/IO/output and durable/restart families, measured default overload
profiles, adaptive and Cloud policy also remain open. Full GNU qualification
remains waived and real GPU execution remains unqualified. The original 731-line
design is incomplete. Changes remain uncommitted and unpushed on the single
existing worktree/branch/PR, retaining merge `11b616a833` of requested main
`e37f9d3fd9`.

Required cache GC completed with exit 0 and removed 0.0 GiB of Zig/Rust output,
leaving 104.5 GiB free. The post-GC record revalidates all 5,036 inputs, 27 source
snapshots, six tested artifacts, SDK proofs, original design, and merge/index/diff
checks. The complete native log contains only the expected injected CUDA errors.

### Shared standalone control-object admission and spare-cache recovery

Standalone lease records now join the common metadata byte cap before backing
allocation, including records not yet published as tokens. Active-lease and
prompt/tokenizer wrapper maps use the same admission path as their backing
storage grows. Cached spare records and retained map capacity remain charged;
removing a logical token or observer does not return still-retained storage.
Identity tables and these control objects compete for the same existing 64 MiB
cap and aggregate/parent envelope. Their combined growth peak cannot bypass
admission. Table bytes and provisional/retained control-object bytes remain
separate in snapshots, since a prepared request's permanent table floor must not
include control objects another owner can retire.

The operation-local allocator context preserves typed quota refusal separately
from backing OOM. Its backing allocator runs outside the accounting mutex, with
credit held before allocation and through physical free. Unmanaged containers
retain no pointer to the stack context; freeing after startup relocation uses a
fresh context at the current manager address. Closing a manager with live control
objects is rejected. Callers supply a raw lifetime backing allocator rather than
an allocator already charging this manager or an ancestor. Standalone admission
also uses checked batch reservation so identity-table backing OOM survives the
public status conversion without publishing a token.

The lease pool now keeps at most 64 spare records and destroys excess records
outside its pool mutex. This is an additional idle-cache limit; all active,
provisional, cached and map storage still shares the byte cap. An 80-record burst
regression fills a real aggregate envelope and proves recycling returns physical
capacity for subsequent work. A controlled unlimited-cache variant fails that
unchanged test, while the restored bounded implementation passes. This avoids
retaining a concurrency burst's entire record pool until service shutdown.

Three initial regressions reproduced missing provisional/cached charges, cap
bypass before record allocation, and missing wrapper-map storage. Their unchanged
test bodies pass with the shared allocator. The additional checks cover real
concurrent provisional allocation, common admission before backing callbacks,
physical-free ordering, object/table cap competition in both directions, typed
OOM/parent refusal, wrapper old/new-table overlap and refusal atomicity, and
startup relocation followed by repeated late parent installation. Legitimate
wrapper metadata pressure is refused without reporting accounting corruption.
Synthetic payload-only fixtures explicitly provision/count their own actual
metadata before publishing work; raw allocation-failure tests retain their
specific failure paths.

`standalone-control-metadata-cumulative-v1{,-verified}.json` records 440 distinct
native cases and 11 process checks. Reported executions comprise 417 individually
printed cases and 55 standard-runner executions, with 32 intentional repeats
rather than additional distinct coverage. The complete common module passes 118
cases. Eleven new cases cover control-object and standalone behavior. All three
commands exited zero (103/103, 15/15 and 15/15 build steps), and all 5,036 captured
native/schema/dependency inputs remained unchanged. All 406 previously printed
cases remain selected. Twenty-eight source snapshots and six tested artifacts
are preserved. Actual worker compiler 81949 supplied enabled Metal/CUDA options
bound to this run's input digest. Existing native/local bridge comparisons keep
their previous selection; the new standalone cases exercise the direct native
variant. The existing 1,846 SDK inputs and source/log proofs remain unchanged;
SDK tests were not rerun. Fixture and compile corrections remain separate from
the four new product red cases; the temporary unlimited-cache variant was
restored to the verified bounded implementation before cumulative qualification.

Protected essential/completion/recovery metadata floors and parent role forwarding
inside the same physical and metadata envelopes are still open. Capacity-domain
and replay-tracker metadata, owner/manager headers, allocator rounding and retained
allocator pages, and nonwrapping common numeric identities remain outside this
component's proof. Existing independently admitted reclaimer storage must not be
charged twice. Observation-only producers still need verified metadata admission
before retaining physical objects. The broader constructor/JIT, GPU handoff and
request funding, SQL/IO/output, durable/restart, measured default profiles,
adaptive and Cloud obligations remain open. Full GNU qualification remains
waived; real GPU execution remains unqualified. The original 731-line design and
full goal remain incomplete. Changes remain uncommitted and unpushed on the same
worktree/branch/PR with requested main merge `11b616a833` retained.

Required cache GC completed with exit 0, removing 0.0 GiB of Zig/Rust output and
leaving 106.0 GiB free. Post-GC verification rechecks all 5,036 inputs, 28 source
snapshots, six tested artifacts, SDK evidence, original design, red sources and
merge/index/diff state. The complete native log contains only expected injected
CUDA error diagnostics.


### Private API cleanup memory inside the existing process reserve

The API's existing prepaid control budget now has separate ordinary-control and
cleanup partitions. Cleanup receives `control_bytes / 2`; ordinary controls retain
the remainder, where `control_bytes = min(16 MiB, request_memory_bytes / 8)`.
The original foreground/read allocation and total process control/transport pin
remain unchanged. Cleanup allocations do not take a second charge against the
node. Both partitions must physically retire before the API releases that pin.

Resource-manager cleanup, read-retirement and reconciliation runtime heaps use
the private partition. Imported cleanup-worker receivers and shared control/cleanup
lane receivers use it too. Ordinary HTTP/pgwire controls, remote owner state and
per-request endpoint bindings retain their ordinary allocation paths; they cannot
claim this reserve by sending a class hint. Allocation provenance routes resize,
remap and free back to the original partition across allocator views. Public HTTP
budgets exclude cleanup capacity, including API configurations without a scheduler.

Workload memory snapshots expose `cleanup` separately. The existing three-part
kernel ABI aggregates ordinary-control and cleanup statistics into its control
entry; its peak is a conservative sum of partition peaks. The API's private runtime
and receiver floor is implemented, while producer-side worker/IO allocations,
remote journal/owner metadata and other completion/resume paths still need their
own verified minimum-capacity treatment. This is not proof of every control path
progressing under overload, nor completion of the original full design.

Evidence is in the receipt directory
`receipts/2026-10-10/child-outbox-capture-retirement-cumulative-integration/`:

- `cleanup-memory-isolation-red-v2.json` records the real `OutOfMemory` regression;
  its test body is unchanged in the final source. The test fills foreground,
  ordinary-control, retained-state and real 32 MiB node budgets, then starts and
  joins actual cleanup consumers with native and imported worker leases. Legacy
  reclaimer metadata is admitted by a real bootstrap start/stop before pressure;
  adoption under an already full node budget is not claimed. `red-v1` had an
  invalid cleanup-lane fixture and is not product-regression evidence.
- The existing pending-retirement test now holds ordinary-control memory full
  throughout. It separately fills cleanup memory to check unpublished-start
  rollback, frees only that pressure, then verifies retirement and physical joins.
  Startup allocation-failure, read-pool envelope, transport and cross-view allocator
  tests also pass. Development `green-v1` exposed two assertion/advertisement
  failures and an undersized compiler-memory estimate; `green-v2` exposed one
  remaining old two-part budget assertion. Those failures are retained.
- `cleanup-memory-isolation-green-v3.json` records three terminal-zero commands:
  a fresh 67/67-step build of the cleanup, allocation-budget and kernel-boundary
  targets, followed by actual reruns of two bootstrap-owner and 34 remote-wire
  cases. The custom runners emit 46 successful case executions, 45 distinct cases;
  the five standard allocator cases are cached from an unchanged allocator source
  and add four distinct cases. The verified scope is 49 cases, not 49 fresh runs.
- Remote binaries were compiled in `green-v2`, where their targets passed. Before
  reuse, the runner proves that every captured input is unchanged except one
  assertion inside the unselected borrowed-policy test, and that replacing that
  exact line reconstructs the archived source byte for byte. Production code and
  selected remote tests are unchanged. Both binaries are hash-recorded and rerun;
  recompilation against the final raw test-file bytes is not claimed.
- `cleanup-memory-isolation-green-v3-verified.json` verifies all 5,044 native/schema
  inputs, five source snapshots, three tested custom-runner artifact hashes,
  unchanged red-test body, original design, and merge/index/diff state. The prior
  440-case component and SDK suites are not rerun on this source set. Real GPU and
  full GNU qualification are not claimed; the user's GNU qualification waiver
  remains in effect. Tests use macOS Zig 0.17.0; Linux retains pinned Nix glibc Zig.

The same worktree/branch/PR retains requested main merge `1ca87ec309` with
`edac99e820` as an ancestor. Changes remain uncommitted and unpushed. Native children
terminated before required cache GC, which exited 0, removed 0.1 GiB and left
83.5 GiB free. The post-GC receipt rechecks the captured inputs, source snapshots,
artifacts, source-proof chain, logs, documents and original design. Broader shared
metadata floors, all-stage ownership, representative mixed overload/recovery and
the remaining original-design requirements remain open.


### Caller-funded worker producers and preallocated journal publication

The private kernel ABI is now revision 51. Worker acquisition requires a valid
allocator callback table; missing funding and older factory versions fail before
worker publication. The borrower creates a stable allocator carrier before calling
the producer, which copies the C-layout callback table into its owned wrapper.
Moving the returned lease cannot leave an executor referencing a stack allocator.

Exclusive workers allocate their executor and Threaded heap metadata from that
caller funding. `BackendRuntime.WorkerLease` records the actual allocator selected
at creation and releases the executor with it after the physical worker join.
The producer wrapper survives that join, then frees through an independent copy
of the allocator table. The borrower retires its receiver and carrier last. API
resource cleanup, read-retirement and reconciliation use this funded native path;
imported counterparts use the same required allocator contract. Allocation failures
retain `OutOfMemory`, while worker-capacity failures retain their capacity status.

Shared-role borrower wrappers also use their own caller funding. The shared
executor remains node-owned, so releasing its first borrower cannot invalidate a
peer's allocator. A protected node bank for shared executors and their later task
allocations is still required. The SQL completion runtime and its reaper still
use foreground allocation paths and need integration with verified completion
capacity. Neither all bridge-frame/OS-stack accounting nor full control-progress
coverage is claimed by this component.

The journal audit found an existing protection: `remote_fence_store.Store` owns
its floor/destination tables and write buffer at initialization. A new test denies
all subsequent store-allocator allocations, persists a fence while old work is
pending, retires that work, and verifies the durable generation after reopening.
This closes the suspected *store-allocator* publication gap without adding another
reserve. It does not prove all filesystem/executor metadata is protected, or that
late remote-owner bootstrap succeeds under ordinary-control exhaustion.

Evidence under
`receipts/2026-10-10/child-outbox-capture-retirement-cumulative-integration/`:

- `worker-producer-funding-red-v1.json` records the unchanged regression failing
  with `SqlWriteCapacityUnavailable` when the runtime allocator rejects further
  allocations despite available caller memory. Four new worker tests cover that
  path, native funding, peer lifetime independence, construction rollback and
  missing/invalid allocator contracts. Existing shared-role saturation/shutdown
  tests are now included in the worker-capability target.
- `worker-producer-funding-integration-v3.json` records all six selected targets
  succeeding, 93/93 build steps, and 104 reported case executions. There are 102
  distinct test labels, including nine declaration-reachability cases; this is
  the selected component scope, not the full design's qualification. All 5,046
  captured native/schema inputs remain unchanged during the run.
- The independent provider archive uses the real runtime and worker factory. Its
  borrower forces C executor dispatch, disables the provider's runtime allocator,
  executes a task and checks caller memory and worker reservations return to zero
  only after release physically joins the executor. No producer runtime or Zig
  allocator layout is interpreted by its separately compiled borrower.
- `worker-producer-funding-integration-v3-verified.json` verifies nine source
  snapshots, six actual test-program hashes, the exact unchanged red-test body,
  private ABI revision, merge/index/diff state and the original design. The final
  run recompiles its changed sources; previous component artifacts are not reused
  across the ABI change. Source snapshots include unchanged root-build context.
- Earlier integration attempts hit declared full-provider build requirements of
  20–28 GiB above the 16 GiB allowance. Those attempts did not execute their tests
  and are not qualification evidence. The final independent-archive fixture uses
  control-side dependencies; full storage/API provider linkage is not claimed.
  The first fixture compile had a pointer-comparison type error, corrected before
  the final run. Full GNU qualification remains waived; SDK and real GPU suites
  are not rerun, nor is the earlier 440-case cumulative component on this source set.

The same canonical worktree, branch and PR retain main merge `1ca87ec309` with
`edac99e820` as an ancestor. Changes remain uncommitted and unpushed. All owned
native children terminated before cache GC, which exited 0, removed 0.0 GiB and
left 49.2 GiB free, preserving other active worktrees' caches. Post-GC verification
rechecks source inventory/hashes, snapshots, tested artifacts, logs, documents,
red evidence and merge state. The full original goal remains incomplete; shared
node executor funding, SQL completion/resume minimums, broader metadata ownership,
remaining resource stages and representative mixed overload/recovery remain open.

### SQL completion runtime and reaper funding

For APIs with a request-memory envelope, `sqlCleanupOwner` now funds its runtime,
registry slots and gated task nodes from the private cleanup floor, and obtains
the independent reaper through the native/imported caller-funded cleanup worker
path. The existing process pin and envelope sizes are unchanged. Setup refuses
before resource publication if the floor or actual workers cannot be admitted.
The reservation retains its callback parents through physical future join,
resource retirement and metadata free. Late runtime allocation failure releases
the already admitted reaper lease before returning.

The new real scheduler/supervisor regression passes in both native and imported
modes with foreground, control, retained-state and node budgets exhausted. It
reserves before publication, then exhausts private cleanup memory too and proves
publication, start, transfer and physical retirement need no new ownership
allocation. Its exact body failed before the routing fix and remains unchanged.

The `sql-completion-memory-*` receipts in the cumulative-integration directory
verify 38 distinct cases (eleven memory, eight worker and nineteen lifecycle),
5,046 native/schema inputs, thirteen source snapshots and five tested executables.
Green-v1 retains an overall failure in the updated rollback fixture's lightweight
constructor; green-v2 fixes that fixture and passes the worker suite, with the
lifecycle result cached. The memory executables were replayed after proving the
only source difference was that unselected fixture constructor. Their replay is
not reported as a fresh compilation. The implementation document records details.

Shared node executors still use the node allocator, not an API borrower's floor.
Their own protected bank remains open and must outlive every borrower and physical
pool join, then release before the storage parent closes. Direct constructors
without an envelope retain their injected allocator contract. This component does
not establish complete bridge-frame/OS-stack accounting, all resume minimums,
full provider linkage, real GPU qualification or the complete original design.
Merge `e60eaaf971` contains main `0f85c7907f`; work remains on the same canonical
branch/worktree/PR and is uncommitted/unpushed.

After all owned native processes terminated, `make gc` exited 0, removed 0.0 GiB
and left 22.6 GiB free, skipping other active worktrees. The post-GC receipt
revalidates source, snapshot, executable, document and evidence hashes.

### Node-owned prepaid SQL executor heaps

`BackendRuntime` now owns a stable shared-SQL memory bank, charged against its
borrowed process parent before allocating bank metadata. Execution, control,
cleanup and bounded-read roles each have a separate 1 MiB default floor. Real
executor objects and allocations made through their `Io.Threaded` allocators
spend these floors; configured exhaustion never falls back to ordinary memory.
Backing allocation failure rolls local credit back, physical free precedes credit
return, and reallocation admits the old+new peak. The node parent holds the entire
prepaid bank, including metadata, until physical bank destruction completes.

The shared-SQL gate retains that bank through construction attempts, live peers,
callbacks, executor joins and final pool bookkeeping. Closing this child prevents
new shared capabilities while leaving other runtime lanes usable. Its memory
parent may then close before the remaining runtime. The bank never borrows the
first API instance's allocator. Standalone and data entrypoints install it before
consumer publication; data shutdown closes it before its compiled storage parent,
and legacy data provisioned storage joins the same early process root.

`node-shared-worker-memory-integration-v5` and its verified receipt cover four new
bank behavior cases, existing multi-consumer worker cases and both actual startup
entrypoints in compiled and legacy profiles. The 30-case scope includes nine
reachability cases and three lock tests cached from actual v4 execution; eighteen
worker behavior cases execute in v5. Semantic checks do not execute entrypoints
or link full providers. The implementation document records the three integration
fixes found by those checks and the 5,048-input/fifteen-snapshot/three-program
verification scope. ABI 51 is unchanged.

The 1 MiB role defaults are bounded heap floors, not completed profiles or OS-stack
bounds. Runtimes without a parent retain their caller-owned allocator contract.
Other node lanes/construction metadata, complete bridge/frame/stack ownership,
resume minima and the full original overload/durability/SDK qualification remain
open. No full provider-link, real GPU or complete-design claim is made; full GNU
qualification remains waived. The same canonical branch/worktree/PR retains
merge `e60eaaf971`, with implementation changes uncommitted/unpushed.

All owned native builds terminated before cache GC, which exited 0, removed
77.1 GiB and left 91.6 GiB free. Post-GC verification rechecks the source corpus,
source/program/document snapshots, evidence hashes and merge/index state.


### Physical worker termination fence (private kernel ABI 52)

BackendRuntime-owned Threaded pools now retain bounded thread handles and perform
actual joins before returning worker capacity or freeing executor ownership.
Waiting for worker bodies alone did not cover pthread TLS destructors; an exact
unchanged negative-control test fails on the old detached backend and passes with
the new policy. Async, concurrent and both group spawn paths participate. Closing
rejects concurrent submissions, executes async submissions inline and clears the
exiting worker TLS pointer, preventing a new thread from escaping the drain.

A generated worktree-local standard-library overlay implements this opt-in policy
without changing installed/Nix toolchain files. Both owned executor constructors
require it, and build composition propagates the library through archive owners
and consumers. Public Zig consumers inherit `antfly-lite`'s library path and build
dependency; the README documents this requirement. The source package carries the
generator and preserves non-Mach-O artifact compatibility. Private kernel ABI 52
rejects factories with the previous retirement contract; public C/SDK and generic
runtime-native ABIs remain unchanged.

`physical-worker-retirement-integration-v4` passes 82 build steps: 20 worker
behavior cases, nine declaration cases, one funding archive case and 19 cleanup
lifecycle cases. Both startup profiles and API ownership pass semantic checks.
The final development archive passes the fetched consumer's 25-step semantic
check; seven packaging and two generator Python tests pass. See the implementation
document and verified receipts for hashes, exact scope and retained failed runs.

Physical join does not establish stack unmapping or bound libc/allocator caches.
All stack/guard/TLS accounting, other runtime lane and constructor metadata,
complete resume minima, mixed overload/recovery and the broader original design
remain incomplete. No full provider-link or full GNU qualification is claimed;
the latter remains waived. Changes remain on the single existing branch/worktree/
PR, uncommitted and unpushed.

Owned native and fetched-consumer builds terminated before `make gc`, which
exited 0, removed 0.1 GiB and left 96.9 GiB free. Post-GC receipts recheck the
5,054-input corpus, four tested programs, generated-library and unchanged
installed-library hashes, source/package/consumer/document evidence and merge
state.


### Guarded pthread mappings and prepaid shared-role stack capacity

BackendRuntime-owned pthread executors now supply their own page mappings to
`pthread_attr_setstack`, retaining each with its handle until actual thread exit.
Both end guards are protected; payload rounding and total size use checked
arithmetic. Pthread entry-wrapper metadata uses the executor's allocator while
preserving standard Thread entry/signal-stack setup. Spawn refusal unwinds both
metadata and mappings. The generated cache-local library patches both Thread and
Threaded and hashes both source pairs; installed/Nix libraries remain unchanged.

Parent-backed shared SQL banks precharge separate stack ceilings for execution,
control, cleanup and bounded read, in addition to existing task-heap floors. Each
allocation spends only its role segment, uses exact whole pages without excess
alignment, and unmaps before returning segment credit. Bank lifetime retains the
full parent pin through all shared borrowers and physical joins. Existing default
role ceilings total 26 stacks, or 416 MiB of payload plus guards, four 1 MiB heap
floors and metadata. Undersized parents are explicitly refused; no automatic
reduction of the existing 16 MiB stack size is claimed. Other owned runtime lanes
use explicit page-mapping ownership but remain outside this node-parent ledger.

`owned-stack-mappings-integration-v7` passes 82 build steps. Its scope is 23 worker
behavior cases (three new), nine reachability cases, one archive case and nineteen
cleanup cases cached from actual v4 execution: 52 distinct cases, not 52 new or
fresh behavior tests. Compiled/legacy startup and API owner checks are semantic.
The actual worker address, Darwin guard permissions and whole-range unmap while
credit is still held are checked, including blocked TLS destruction and spawn
rollback. Native/imported execution-stack exhaustion preserves the other roles.
An initially invalid Darwin mincore assumption was corrected using a direct probe
and Mach VM-region queries; failed native receipts remain available.

The fetched development source package passes 25 semantic-check steps. Seven
packaging and three generator Python tests pass. Private kernel ABI remains 52;
public C/SDK and generic runtime-native ABIs are unchanged. Detailed source,
program, generated-library, package and original-design hashes are in verified
receipts. Full GNU qualification remains waived; Linux runtime execution, full
provider linkage and full consumer runtime tests are not claimed.

Libc dynamic TLS/descriptors, allocator rounding and caches, all other node-parent
lane/constructor/bridge/frame charges, non-pthread profiles, complete resume
minima and the remaining overload/durability/SDK design still require work. The
original goal remains active and incomplete on the single existing worktree/
branch/PR; changes remain uncommitted and unpushed.

All owned native/export/consumer commands terminated before cache GC, which
exited 0 and left 95.7 GiB free. Post-GC verification rechecks source, tested
programs, both generated and installed library files, package/consumer and
document/evidence hashes, original design and merge state.


### Common ancestor at host/storage birth (runtime-services ABI 3)

Standalone/data startup now creates one stable host-owned memory root before
compiled storage and host runtime construction. Storage resource managers join it
before fixed cache admission, and context headers precharge requested bytes before
physical allocation. Rollback/destruction frees physical ownership before returning
header or resource credit. Host shared SQL banks borrow this root directly; its
lifetime exceeds both storage context and remaining host teardown.

Private runtime-services ABI 3 appends an optional checked, copied parent table.
Request versions 1/2 and truncated/old parent headers are rejected before tail
access. Storage-owner ABI 100, API kernel ABI 52 and public C/SDK interfaces remain
unchanged. Only the language-neutral parent table crosses the storage boundary.
Inference/API keep nearest-storage-child reclamation hooks; child accounting and
pressure notifications reach the common ancestor. Host process totals/peaks come
from that root through a host-only DataServer borrow, while storage slices and
context-local metrics retain their own attribution.

`common-process-memory-root-integration-v5` passes 94 build steps. Its verified
scope is 52 distinct cases: 23 worker behaviors, ten standalone bootstrap/transient/
control-metadata behaviors, ten cache/bootstrap/telemetry behaviors (six new), and
nine reachability cases. The ten metadata cases are cached from v4 with identical
current inputs. A real cache entry is reclaimed and new work admitted while a host
sibling still holds ancestor pressure. Old cache tests were corrected to include
charged identity metadata instead of asserting payload-only totals.

The actual storage context constructor/destructor and both compiled/legacy startup
bodies are semantically checked, not full provider runtime tests. Verification
binds 5,056 inputs, thirty source snapshots and six exact program hashes, original
design and merge/index state. Published embedded source inputs remain unchanged;
no new package/SDK qualification is claimed. Full GNU qualification remains waived.

Remaining runtime lane/construction/bridge/frame charges, libc TLS/descriptors and
allocator rounding, full startup/resume profiles and the broader original mixed
load/durability/SDK design remain incomplete. All changes remain uncommitted on the
single existing worktree/branch/PR.

After all owned native runs terminated, cache GC exited 0 and left 96.5 GiB free.
Post-GC receipts recheck exact source, snapshots, six program hashes, unchanged
published/library inputs, document/evidence hashes, original design and merge state.

### Service and SQL worker capacity ownership

Dedicated service workers and shared SQL roles now have separate reservation
classes. The default ceilings are 32 each; SQL execution/control/cleanup/bounded
read retain 12/5/8/1 slots collectively across consumers. Both declared envelopes
count toward the per-runtime profile (284 default, ceiling 288); each individual
executor still respects the pinned Threaded join-handle maximum of 256.

The physical WorkerLease records its class. Destruction joins and unmaps before
returning aggregate then class credit; unsuccessful construction returns both.
Shared borrowers retain their existing independently funded wrappers and owner
close gates. The SQL memory Bank uses the SQL envelope when prepaying its stack
floors. This does not fund other service stacks or libc TLS against that Bank.

Private API kernel ABI 53 publishes and checks both limits. Service reapers use
exclusive capacity; SQL roles use shared capacity. Telemetry retains aggregate
reservations and publishes the separate class capacities/usage so SQL does not
appear to exceed the dedicated-service maximum. Process-wide ownership and
broader default qualification remain incomplete.

### Runtime metadata and owned IO mapping authority

`storage/runtime_memory.Owner` is a stable, node-funded allocator context.
Standalone/data supply the common early root; compiled storage supplies its
stable resource manager (forwarding to the common ancestor when present).
Requested heap sizes and complete owned IO stack mappings charge before physical
allocation and release after physical free. Each mapping uses exact pages and
rejects unsupported alignment. Constructor/backing failure returns all charges;
reallocation admits the physical overlap. The separately charged runtime handle
outlives the allocator context and releases its header credit after destruction.

Shared SQL banks keep their prepaid authority and original backing; caller-funded
worker metadata keeps its caller allocator. Those allocations are not charged
a second time by the runtime heap wrapper. Ordinary exclusive-worker mappings
and the primary/lazy Threaded lane mappings now use the runtime page authority.
Physical joins keep the context and charges alive through pthread TLS retirement.
All runtime metadata/executor owners retire before its context is destroyed.

`runtime-lane-memory-integration-v2` provides live root-pressure, rollback,
reallocation, page, physical lane and TLS-retirement evidence, plus startup and
actual storage constructor semantic checks. Its independent funding archive
executes successfully; the new Apache development package passes fetched public
consumer analysis. No full provider/server startup runtime is claimed. PDF maps
and scratch, private embedded/native-storage executors, libc TLS/descriptors,
allocator rounding/retention and broader progress floors remain open.

### PDF stack and scratch mapping retirement

The node PDF lane now supplies runtime-backed metadata, exact guarded pthread
stacks and page-rounded scratch mappings. Its existing worker/queue, logical
scratch and per-worker retention limits are preserved. Retained arenas keep
root charges between batches. Physical thread joins, including TLS destructors,
precede arena teardown and stack unmapping; physical frees precede root credit.
A partially constructed lane joins earlier workers and returns all ownership
when a later worker cannot be admitted.

`pdf-node-memory-integration-v4` passes all 114 build steps, including the live
page/overlap, partial spawn, exhausted-root scratch/retry and TLS-retirement
regressions, plus existing affinity and scratch-budget tests. The new public
Apache source package passes fetched consumer analysis. Full server/provider
runtime and Linux mixed-load qualification are not claimed. Private embedded/
native-storage IO, separate libc thread state and allocator rounding/retention
remain outside this completed PDF binding.

### Native storage states and private manual helper lifetimes

A runtime's native pool now retains each live native state and path-lock owner.
Close rejects new owners and waits for the last retained owner to release before
runtime IO or the borrowed parent can be destroyed. Each state/path lock has a
stable funded allocator, actual joinable workers and owned guarded mappings.
Range futures, descriptor permits and explicit leases keep state ownership after
the public storage handle closes. Physical thread joins, including TLS cleanup,
precede allocator destruction, pool release and parent credit return.

Manual owned-IO helpers allocate from their runtime authority and retire their
auxiliary IO before that authority. Default owned DB runtimes and the private
externally locked C API helper use an early resource manager with the existing
automatic profile; it outlives runtime cleanup. Constructor rollback has the same
ordering. Read/result buffers retain their existing caller allocator contracts.

`private-native-io-integration-v4` passes 114 build steps with seven new live
lifetime/failure/file/retirement cases, 42 capability and 41 foundation cases.
The actual private C API constructor and close branch are semantically analyzed;
full C API execution/provider linking is not claimed. The corrected dev.2 package
passes fetched public analysis. Other Lite file/artifact/bridge/restore IO and
process-wide embedded composition remain open, along with libc and allocator
physical overhead. This does not complete the original full design.

### Stable Lite IO and managed manager references

Native file and artifact registry executors now have stable heap-owned Threaded
state, guarded mappings and actual join ownership. A supplied resource manager
funds IO metadata and mappings. File/cache data allocations retain their original
budgets. Convenience-created managers use shared ownership, and IO destruction
returns physical allocations/charges before dropping the manager reference.
Borrowed-IO files can retain managed budgets without owning the caller's IO.

File close joins before retiring the descriptor/path/cache state. Registry close
joins before freeing registry metadata and finally its IO/budget reference.
Retained artifact sources therefore survive backend and public manager-owner
close with live accounting. Private C API file/backend/runtime/DB share the same
owned manager; native backend defaults use automatic bounded profiles.

`lite-owned-io-integration-v4` passes 124 build steps, including five new lifetime,
allocation failure, retained byte and TLS/descriptor cases plus 66 existing Lite
lifecycle cases. The local test compile's measured 8.24 GB peak is covered by a
10 GiB authored reservation; the final run has no transient RSS diagnostic. The
public source package passes fetched consumer analysis. Full production runtime/
Linux mixed-load qualification remains unclaimed; bridge/restore executors,
external provider and multi-DB process lifetimes, libc and allocator overhead,
and the broader original design still need work.


### Restore and bridge executor ownership

| Owner | Authority and lifetime | Retirement |
| --- | --- | --- |
| Native path lock | Stable OwnedIo; optional manager funds metadata and guarded maps; managed lifetime retained | Physical join, synchronous native descriptor close, owner free, last budget reference release |
| Development bridge | Stable bounded executor; optional manager retained; payload map still uses the original allocator | Join before map/path/lock destruction, then destroy IO and release budget |
| Restore preparation | Owned bounded executor or synchronous borrowed std.Io; format dispatch, source files, manifests, directory copies and remote-store setup reuse the authority | Location/client and file cleanup finish before owned IO joins and budget release; borrowed IO remains caller-owned |
| Portable disposable generation | Borrows live file IO and destination manager through all staging and publication cleanup | Close prepared DB/backend before workspace cleanup; adoption swaps descriptors and preserves live executor |
| Normal table restore replacement | Transfers owned runtime and owned manager together; borrowed managers remain borrowed; reopens under the original manager | Reattach both owners on success/recovery; failed reopen joins runtime before releasing manager |

`restore-owned-io-green-v4-verified.json` records the 74-step native integration,
92 distinct custom cases, exact source/program/archive hashes and 25-step fetched
consumer semantic check. V1 preserves the cleanup-lifetime ABRT discovered by
the normal backup roundtrip; V2 preserves the field-location compile error in
the fix; V3 and V4 are green before/after the final backup authority propagation.
Recovery-branch ownership is implemented; this receipt does not claim exhaustive
fault injection of every reopen failure or a full provider/server runtime test.
The original full design remains incomplete; full qualification remains waived.


### Metadata executor and bootstrap memory

| Owner | Authority | Retirement |
| --- | --- | --- |
| Metadata CLI node root | Frozen canonical process override or cgroup/host resolution; declared before setup IO, compiled storage and host runtime | Outlives every child executor/storage cleanup |
| Metadata setup IO | Stable joinable OwnedIo; node-funded heap and exact guarded mappings; existing 1 MiB stack payload retained | Secret/audio/storage users finish before actual IO join, metadata free and parent credit return |
| Compiled metadata storage context | Common node parent supplied in context-create runtime services | Context destroys owned children before node root teardown |
| Metadata default service runtime | Stable managed manager plus handle in runtime_resources.Owner; optional caller-supplied parent/local ceiling | Handle joins physical workers and native pool borrowers before manager release |
| Metadata borrowed runtime | Embedding host retains lifetime and budgeting authority | Service quiesces its own users; service close does not destroy the host runtime |

Native tests cover owner movement, real worker stack charges, shared-parent
exhaustion with prepaid SQL-control progress, actual induced allocation failures,
refusal before server publication, owned server close, borrowed-runtime reuse and
existing WAL/multi-node/compaction construction. The allocator harness maps a
std.Io concurrency-refusal error to OutOfMemory only after checking that the
failing allocator actually induced an allocation failure. Other refusals fail.

`metadata-runtime-memory-green-v6-verified.json` binds the clean 92-step result,
71 distinct custom cases, exact 5,062-input inventory, seven executable hashes,
six run histories and fetched-package proof. V5's final actual status is zero;
its transient compile RSS diagnostic is retained. The authored lifecycle compile
reservation is now 18 GiB, above the measured 13,489,651,712-byte peak with more
than 25% headroom. No RSS check or physical retirement rule was removed.

This does not claim complete metadata catalog/WAL/media accounting, general
service/control/recovery memory floors, the metadata CLI's frozen scheduler
wiring, or actual compiled-provider/CLI execution. Full original-design
completion remains false and full qualification remains waived.


### Native control executor reserve and shared metadata scheduler

| Owner | Authority | Retirement |
| --- | --- | --- |
| Funded native control executor | Prepaid private 1 MiB heap plus all configured guarded worker mappings; default eight workers, unchanged 16 MiB stack payload | Parent retains the full ceiling through task completion, actual worker/TLS join, mapping release and runtime memory-owner destruction |
| Control heap children | Local bounded admission before backing allocation; no fallback to ordinary parent capacity | Physical free before local credit return; private ceiling stays charged |
| Existing aggregate heap/maps statistics | Combined atomic ledger of general and actual control allocations | Peaks describe simultaneous use; reserve is separately reported |
| Metadata CLI admission owner | One common startup policy resolution and one node scheduler; borrowed by API and backend/storage | Admission closes before service join; owner destroyed after runtime/storage consumers |

The focused tests cover cold publication of the full default control lane under
exhausted ordinary memory, private-heap refusal/recovery and blocked physical
TLS retirement. Metadata construction proves a backend grant is visible through
the API controller, plus default bounded waiting and fixed/disabled/fail-fast
policy preservation. Actual CLI/provider execution and mixed-load qualification
are not claimed.

The control reserve is an executor guarantee. General service dependencies,
recovery entry/queue/future/payload memory and Raft transport buffers/maps still
need explicit completion floors. Publication/activation supervisors retain durable
retry intent but ordinary-memory allocation can delay admission. Runtime restart
Work currently uses page_allocator. These paths must be included before claiming
progress under complete node-memory exhaustion. Full design completion remains
false and full qualification remains waived.

`native-control-floor-green-v2-verified.json` binds the clean 92-step native
result, 76 distinct cases, seven program hashes, all 5,063 unchanged inputs,
the retained failed-fixture history, source snapshots and fetched development
package proof (25/25 semantic steps). Source/program/evidence hashes are checked
again after terminal cache cleanup in the companion post-GC receipt.


### Native Raft executor reserves and host refusal

| Owner | Authority | Retirement |
| --- | --- | --- |
| Native Raft inbound executor | Separate prepaid 1 MiB heap and all configured guarded mappings; default 32 workers with unchanged 16 MiB stack payload | Full reserve remains charged through worker/TLS join and final runtime memory-owner destruction |
| Native Raft outbound executor | Independent heap and mapping reserve with the same default ceiling; no borrowing from inbound/control/ordinary memory | Same physical retirement barrier; local heap credit returns only after backing free |
| Aggregate memory statistics | Existing combined heap/map ledger includes actual Raft allocations; separate direction metrics report reserves and refusals | Reserved ceilings and actual live mappings remain distinct |
| Native HTTP host setup | Requires successful runtime lane publication; custom request executors cannot bypass required outbound IO | Constructor error unwinds service leases; no independent native fallback on lane refusal |

`native-raft-floor-green-v3-verified.json` binds actual exit zero, 104/104 build
steps, 121 distinct cases, ten program hashes, all 5,063 unchanged inputs, retained
run histories/source snapshots and the fetched development consumer (25/25
semantic steps). New cases run all 64 default Raft workers after exhausting
ordinary memory, test independent heap refusal/retry with control progress,
block real TLS destruction in both directions and test startup funding before
memory-owner allocation. Host cases cover both directions and custom/default
executors with service-lease rollback. V1 preserves two fixture failures. V2
passes all functional cases but records a 12,272,205,824-byte compiler peak above
its default 10 GiB claim. V3 gives the Raft runtime compile an authored 16 GiB
reservation with more than 25% headroom and reruns cleanly; memory checks remain.

The native/default profile requires its larger complete floor before publication.
Small non-Raft fixtures explicitly select one worker per protected direction;
they are not proof of the 32+32 default under a 128 MiB parent. Parent/backing
allocator identity and physical heap/TLS accounting remain open.

This does not fund the per-sender isolated HTTP executors, transport queues and
buffers, provider/descriptor/persistence dependencies or durable completion
payloads. Compiled storage and the host still own separate native runtimes; the
runtime-services interface shares a memory parent and runnable owner, not full
execution services. Startup must fund the sum until a handoff that preserves semantics
shares them. Recovery also needs a bounded funded path for debt already persisted
before startup, not only tickets prepared before new irreversible decisions.
See the latest implementation entry for source boundaries. Full design
completion remains false; full qualification remains waived.


### Shared node execution scope and final context retirement (2026-10-18)

The three native node startup paths supply one execution-services offer to
compiled storage. Execution capacity and SQL/PDF/maintenance ownership stay
with the node; storage retains local storage/policy metadata and scoped native
capabilities. The scope owns its durable IDs and recurring registrations and
cannot close peers' work. Shared storage does not become an inline/manual
runtime merely because its I/O is received across a compilation boundary.

A runtime proxy physically returns its own metadata and parent charge before
releasing its execution scope. A storage context holds an additional guard
through child-manager and bootstrap-header destruction, including constructor
rollback. This keeps the producer alive until every context charge is returned.
Lite file create/reopen borrows that runtime's filesystem I/O. Worker and SQL
leases retain local lifetime gates while joining the actual producer resource;
control/Raft token metadata is funded by the protected origin heap.

See the implementation entry and `receipts/2026-10-10/child-outbox-capture-retirement-cumulative-integration/native-execution-services-v16-verified.json`
for native/source-package evidence. Actual context shutdown, failure rollback,
shared limits, async durable draining and Lite commit/reopen are exercised;
live CLI/cluster qualification is not. Full transport dependency funding,
recovery-debt dispatch, physical rounding and the rest of the original design
remain open.


### Helper request ownership and main integration (2026-10-18)

Helper-created storage contexts now borrow node execution through a stable
synchronous request builder. Supplied memory parents and runnable owners are
copied; a zero helper limit inherits the parent's limit without a second host
probe. DataServer helper-owned runtimes and SQL banks charge an explicitly
supplied process root. A metadata owner borrows the process context when
available and attaches it after local settings, preserving its preconfigured
status and caller-owned lifetime.

Checked SQL allocations across main's reclaiming scratch wrappers preserve
both ancestor policy refusal and genuine allocator OOM, without consulting a
remembered exhaustion flag for mandatory creation. The two scratch tests check
failure rollback, later successful allocation and exact release.

See the implementation entry and `receipts/2026-10-10/child-outbox-capture-retirement-cumulative-integration/native-execution-main-v3-verified.json`
for the 117-step focused result and current development archive. The separate
`main-integration-and-wip-preservation.json` records preserved WIP and main's
release-policy files. Current proof does not establish full lazy JSON/range
admission coverage, funded no-parent helper roots, the complete transport
resource graph, recovery-debt dispatch or physical rounding. Full goal remains
incomplete; full qualification was waived.

### Owned data-helper memory root (2026-10-18)

The shared helper owner now lives in `storage/helper_runtime_resources.zig`.
Metadata retains its existing API through a re-export. DataServer eager and
lazy helper-created runtimes own this same manager/runtime pair, propagate the
configured limit, and use the owned manager as the process root when none was
supplied. Storage contexts and service consumers close before the owned runtime;
the owner joins runtime workers before releasing the manager. Caller-owned
process roots and runtimes remain borrowed.

Regression scope includes moving the owner, failed construction, shared-parent
exhaustion, a refused one-byte no-parent envelope, prepaid SQL control progress
under an exhausted owned envelope, and an actual storage context borrowing the
helper's execution and memory authority. Final-charge inspection must occur
after the helper's physical runtime join because shared IO workers can retain
lazy allocations after a storage context closes. Full DataServer helper
lifecycle execution remains unverified by these focused targets.

Focused qualification `native-helper-root-v4` completed with 71/71 build steps,
7/7 storage-context runtime cases and 6/6 helper-memory cases passing. Both
startup storage profiles were semantically checked. The initial context case
incorrectly expected lazy worker charges to retire before runtime close; the
current case inspects the retained ledger after the actual runtime join and
finds only the ledger's own shared-owner charge. All 5,072 recorded native
inputs stayed unchanged during the successful run. Full qualification remains
waived, and the full design remains incomplete.

### Native JSON producer leases and work authority (2026-10-18)

`Store.JsonLease.initControlled` borrows the work authority of the synchronous
retention operation. The lease contains source identities only through the
current immutable producer batch. Retained `Store`, JSON pool entries and
output views own their payload bytes and retain no lease control. Hashing,
content comparison, byte copying, dictionary import and fallback append polls
can propagate the owner's exact error before publishing retained state.

Pipe leases select `producer_execution`; hash joins select their explicit child
execution before a coordinator callback. Top-K receives statement control.
Coordinator relation callbacks do not turn byte chunks into logical row visits.
Temporary map adapters can signal a failed comparison as a provisional match,
but callers reject the captured error before reading or publishing that match.
Canceled pooled copies free unpublished bytes and owner headers.

The current native receipt `native-json-lease-v5` records 97 selected SQL cases
and 22/22 successful build steps. Its cancellation, retry, collision, allocator
fault, shared-body retirement and held-output lifetime cases cover retention.
They do not prove that lazy JSON/cache decode, cache-lock waits, projection
cell access or value-range reads already cooperate with their consumer. Those
paths and the remaining transport/durable ownership requirements remain open.

The final development export `0.1.0-json-lease-dev.2` also passed all 25 fetched
consumer semantic build steps. Its immutable archive and source manifest bind
the five changed SQL modules to the native-tested working tree. The consumer
uses the fetched archive, with local copies of the two pinned third-party
source mirrors for offline dependency resolution; it does not use monorepo
paths for the embedded package. These are development artifacts, not a release
or full qualification.

### Lazy JSON consumer authority and optional cache tokens (2026-10-18)

Controlled retained/column/batch access borrows the current consumer's work
control only through the synchronous call. Projected and selected readers
forward that borrow; no cache entry or held value owns a producer execution.
Text scanning checks 4 KiB refills and produces owned tokens. Frozen decoding
checks nodes and leaf copies while retaining native scalar tag identity.

Cache metadata mutexes are released before decode or a checkpoint. Decode and
encoded-text allocation use an optional pool/cache token. Contenders decline
that token and use admitted consumer scratch or decline the encoded capability,
so they cannot hold the CPU waiting for a token owner to resume. Token cleanup
and metadata publication do not yield. Cancellation reclaims partial cache
allocations and permits a later consumer to retry. Snapshotting immutable
payload fields under the metadata lock avoids racing text publication.

`native-json-consumer-v3` records 105 passing selected SQL cases, 22/22 build
steps and unchanged 5,073 inputs. Its new cancellation, retry, mutex ownership,
reentrant scratch, projection forwarding, opaque-reader error, Unicode/refill,
validation and allocator-fault cases cover this interface. Opaque readers need
an explicit controlled callback; their existing scalar callback cannot be
replaced merely because encoded bytes are available.

Strict DOM growth/key-hash work bounds, frozen encoding, all opaque reader
adapters, pgwire scalar-consumer admission, projection coercion and value-range
reads remain open, alongside the original transport and durable ownership gaps.

Development export `0.1.0-json-consumer-dev.1` passed all 25 fetched-consumer
semantic build steps. Archive/source-manifest checks bind the current public
modules, including the new Apache streaming reader, to the native-tested tree.
The consumer uses the fetched package and local copies of the two pinned
third-party source mirrors, with no embedded-package monorepo dependency.

### Pgwire active consumer ownership (2026-10-18)

Read-row delivery borrows its result, allocator and frame buffer synchronously
through `Backend.deliver_row`. The native adapter uses the node's shared SQL
executor and a separate consumer runnable. Credential and cell processing occur
inside it; both its completion event and physical future join before transport
can publish the encoded frame. The producer's parked runnable is not reused.
The result's existing read/memory lease is borrowed; delivery cannot reacquire
a second foreground read admission and create a dependency on itself.

`Result.cellControlled`/`Cells.read_controlled` forward only the current call's
control. Native page access retains its original statement deadline and page
lifetime; its static page conversion helper remains page-only. Result views and
spooled cursor views borrow ownership and do not release the original result.
The controlled writer preserves the exact checkpoint error. Failed encoding
publishes no partial DataRow. Existing committed completion remains independent
of this fresh read-delivery admission.

The final native receipt `native-pgwire-consumer-v5` records 75/75 successful
steps and 14 delivery/native-cell, 7 cursor/protocol and 3 cancellation cases.
The real production adapter runs a shared worker under one ordinary CPU while
the only foreground read lease stays retained. Producer/consumer separation,
pre-publication cancellation, socket ordering, cursor position/namespace and
unknown-commit cases supplement that path. Per-row scheduling overhead,
held-cursor materialization and remaining codec/opaque-reader work still need
qualification alongside the original transport and durable ownership scope.

The development export `0.1.0-pgwire-consumer-dev.1` also passed all 25 fetched
consumer semantic steps. Its manifest binds current Apache modules and the
public read-control alias to the native-tested tree. The fetched package uses
local copies of the two pinned third-party mirrors for offline dependencies.
This is development evidence, not full qualification or release promotion.


### Spill consumer control and cache publication (2026-10-18)

Spill columnar cells, owned-block payload/key batches, shared join bodies,
gathered payload pages, merge rows, resident sort views and leased sort pages
now forward the current consumer's borrowed read control through scalar
materialization. Existing validation, SQL NULL/JSON null distinctions, native
numeric tags, dictionary identity and immutable page leases remain authoritative.
The block/cache stores no producer or consumer scheduling authority.

JSON decoding runs outside the cache metadata mutex in a separately owned,
stable arena. Publication installs a complete value under the mutex. Concurrent
or reentrant legacy readers may publish first; a losing or cancelled candidate
releases its arena and header. Warm reads reuse the published tree. Raw backing
allocator operations are serialized separately; control callbacks never run
under either mutex. Cache retention reports live backing allocations, including
independent payload arenas and transient fills. Text encoding also publishes
outside the metadata mutex and releases its temporary encoder buffer.

`native-spill-consumer-v4` passed all 22 build steps and 120 selected SQL cases
with no skips, failures or leaks. Three direct runtime cases additionally prove
cancelled-fill cleanup/retry under all allocation failures, fresh consumer
control on cached payload/key reads, reentrant legacy publication during a
controlled decode, and cancellation of a cache-lock waiter without releasing
another owner's lock. The reentrant case proves lock placement; it does not
qualify concurrent-fill throughput. Development export
`0.1.0-spill-consumer-dev.2` passed all 25 fetched-consumer semantic steps.

This closes scalar control forwarding for the spill adapters above. Frozen
encoding, strict bounds on DOM growth/key hashing, projection/coercion and other
opaque readers, held-cursor materialization, delivery batching/throughput,
transport dependency progress, durable completion/debt admission and remaining
memory ownership/full-design requirements are still open. Full qualification
remains waived; the overall scheduling goal remains incomplete.


### Frozen encoding and held-cursor consumer work (2026-10-18)

Frozen JSON now offers controlled writer/text entry points. The validated tree
walker polls the current consumer per node and chunks escaping/copying of
strings, object keys and number lexemes at 4096 input bytes. Default Unicode
pass-through preserves UTF-8 sequences even across chunk boundaries. Legacy
entry points retain their wire representation. PGWire forwards its active
consumer through this walker and preserves cancellation, quota and writer
failure identities in both text JSON and binary JSONB formats without an extra
DOM or text allocation.

Cursor fill/held-cursor precommit spooling now runs each immutable page through
the existing synchronous delivery consumer hook. The producer page stays leased
until that consumer retires, before the page is released or COMMIT executes.
Spool copies use the current control for source cells, metadata/text/frozen
bytes, recursive JSON copies, typed-array validation/clone and numeric
validation/clone. Pages publish only after complete validation/copy plus a final
control check; failure retires the unpublished arena/header. No control is
stored in the spool or its typed values. Legacy backends retain the direct
completion path. This introduces no new fallible step after durable commit.

`native-frozen-cursor-consumer-v2` passed 89/89 build steps: 121 selected SQL
cases, 15 API/native-cell cases (including the production delivery adapter),
16 protocol/spool cases and 3 cancellation cases. The subsequent v3 run added
only a controlled-codec test to values.zig, leaving production code byte-for-byte
unchanged; it passed 74/74 steps, 121 SQL cases and 17 protocol/spool cases.
New cases cover large keys/strings/number lexemes, exact legacy bytes and UTF-8
boundaries, cancelled spool rollback/retry under allocation failures, copied
native values after source mutation, and consumer retirement before page release
and commit (including cancellation before COMMIT reaches the backend). Existing
spool, held-cursor position/namespace, failed-fetch, quota, null and ownership
regressions also pass. Previously unselected frozen-spool fixtures now use the
budget's atomic read API. Both source catalogs export the existing Apache work
helper; the generated control catalog check passes.

Development export `0.1.0-cursor-consumer-dev.1` passed all 25 fetched-consumer
semantic steps. This is component evidence, not full qualification. Full
qualification remains waived. Optional encoded-cache callbacks, scalar/array
codec preflight (particularly binary arrays), ordinary DOM serialization,
strict DOM growth/key hashing/allocator-copy bounds and throughput/batching
remain open. The original transport dependency progress, durable completion
resource/debt admission, memory-root/physical accounting and broader
scheduler/remote/SDK requirements remain open; the whole goal is incomplete.


### Shared Raft outbound task capacity (2026-10-18)

Default HttpHost clients now borrow the enclosing runtime's cancellation-safe
Raft network view. Each sender retains a private HTTP client/connection cache,
while its nested tasks use the same bounded I/O owner as snapshot traffic.
Proxy runtimes use the origin's generic I/O view and do not create a private
native outbound executor. The explicit no-runtime fallback shares one native
pool, capped at 32 workers, among that host's clients. Independent fallback
hosts still require a common runtime to share a process-wide owner.

Before any attempt submits a task or transmits bytes, admission reserves the
request/connect/socket deadline graph (six workers). The default runtime's
32-worker lane therefore admits at most five complete attempts, shared across
clients and execution-service scopes. Admission also conservatively accounts
for still-busy tasks whose completion has been observed before physical worker
retirement. It fails fast into the existing bounded sender queues; this adds no
unbounded waiting queue. Runtime stats expose active attempts and the configured
request capacity. Raft capacity does not consume the separate control lane.

The common execution-services offer is now version 2: version 1 lent a Raft I/O
view without guaranteeing complete-attempt admission and is rejected before
native dispatch. Native ABI 9 and private RuntimeRequest ABI 4 remain unchanged.
The host retains its runtime through client shutdown, metadata retirement and
worker joins. Attempt permits use fixed, checked inline storage rather than a
new foreground heap allocation. Producer-funded Raft token metadata retires
before its final native permit is released.

`native-raft-transport-pool-v4` passed all 84 build steps: 28 private transport
cases, six embedded listener/executor cases, thirteen cross-archive execution-service
cases and eight storage-context cases, plus compiled/legacy startup analysis.
New checks cover shared-client cancellation on real sockets while a sibling
completes 20 fresh connections; borrowed-client admission before task creation;
per-sender clients with no private native pool; proxy/origin host wiring and
admission with its local metadata allocator denied; cross-client global Raft
saturation with independent control progress; and real outstanding tasks that
reduce safe admission headroom. Source-catalog, error-registry and formatting
checks pass. Development export `0.1.0-raft-pool-dev.1` passes all 25 immutable
fetched-consumer semantic steps.

This closes the default Raft sender executor escape from the shared worker
ceiling. It does not prove whole-transport descriptor/buffer/provider funding,
traffic-class fairness, every explicit custom/manual composition, or all TLS
CPU/physical accounting. Protected durable precommit completion-resource
admission and funded persisted-debt dispatch still need implementation and
qualification, alongside the remaining memory-root/physical, codec, remote/SDK
and original scheduler requirements. Full qualification remains waived; the
full design goal remains incomplete.


### Reserved finite-step recovery dispatch (2026-10-18)

Registered native owners now have an allocation-free FIFO recovery queue. A
`Job` with class `.recovery` uses its prepaid owner slot instead of allocating
an `Entry`, growing the ordinary job array, or starting an executor future.
One step may run and one successor may wait per owner. A full pending slot
refuses admission with caller payload ownership preserved. Successors join the
tail after completion, preventing a hot owner from repeatedly overtaking peers.
Pause/close fences new admission without discarding accepted steps. Payload
retirement precedes owner-idle publication.

Two recovery workers and their futures are constructed before the native
runtime is returned. Their executor metadata and stacks use the protected
control memory bank; the existing eight-worker control lane remains available.
The aggregate profile is now **286 under 288**, including these two workers;
custom profiles must include that fixed reservation. Runtime proxies forward
the new class to the origin and create no duplicate recovery pool. Manual
runtimes retain inline semantics. Execution-services Offer ABI is now **3**;
older offers are rejected before dispatch. Native ABI 9 and private RuntimeRequest
ABI 4 are unchanged.

`native-recovery-dispatch-v2` passed 81/81 build steps: 20 recovery/owner-lifecycle
cases, 14 cross-archive cases, 8 storage-context cases, and 6 metadata-memory
cases (48 functional cases). Nine source-companion anchors are recorded
separately. The new tests cover 257-owner FIFO selection, a hot owner's bounded
successor, pause/close retention, ordinary allocator refusal with general
workers saturated, and actual cross-archive execution under ordinary parent
exhaustion. The archive and context binaries also passed direct runtime checks.
The development source export `0.1.0-recovery-dispatch-dev.1` passed the fetched
consumer's 25 semantic build checks. These are not 25 runtime tests. V1's only
failure was a test-inventory filter; V2 corrects it. An initial consumer setup
race was corrected before the successful consumer run.

This is dispatcher infrastructure, **not completed durable recovery**. Existing
publication and activation consumers have not been switched yet: activation
currently sleeps/retries within a long-lived callback. Publication already reads
a bounded page but still has blocking lock/completion dependencies. Convert
activation to finite passes and resolve publication completion before migrating.
Do not use recovery callbacks to sleep between attempts, wait for another
recovery owner, or perform recursive owner joins. Precommit completion
reservations, protected callback dependency budgets, queued native recovery
shutdown/failure-injection coverage, and the rest of the original design remain
open. The full qualification waiver remains in force.


### Activation recovery yields workers between attempts (2026-10-18)

Portable activation now submits `.recovery` jobs to the origin runtime's reserved
finite-step dispatcher. The owner retains the activation failure streak and
monotonic retry deadline between jobs. One job performs at most one attempt;
the existing owner maintenance probe schedules a due successor. Initial jitter,
exponential backoff and its 30-second cap remain. Single-flight contention does
not increment the activation failure streak; success resets the retry deadline.
No sleeping retry loop retains a recovery worker. Stop sets the permanent stop
flag and disarms the probe under the lifecycle mutex, serializing that decision
with the finishing job's rearm. The final lock handshake remains the lifetime
barrier. Existing DB identity, fencing and durable activation state are preserved.

`native-activation-steps-v1` passed **79/79 build steps** and **44 functional
cases**: 22 recovery/owner cases, 8 real DB activation/recovery cases, and 14
cross-archive cases. The archive binary also passed a direct runtime invocation.
The two new owner tests use an injected clock: 24 pending retry timers leave
workers available, and due work still executes under ordinary parent memory
exhaustion. They check preserved failure backoff, early-notification coalescing,
contention and successful retirement. Existing real DB regressions cover failed
submission and automatic rearm, claimed-probe close races, restart, final stop
handshake, degraded partial runtime startup, and foreground fail-fast behavior.
The development export `0.1.0-activation-steps-dev.1` passed all 25 fetched-consumer
semantic build checks. There were no native failures, skips or leaks; the partial
activation regression recorded its one explicitly expected error log.

Correction to the preceding dispatcher audit: publication recovery already reads
**16 v2 records plus three legacy singleton records per page**, in both controlled
and ordinary paths. Its policy driver makes at most two drain calls per job.
There is no unbounded record scan to replace here. Payload byte size, flush/apply/
mutation/transition/log lock waits, and `Publisher.complete` durability/authority
completion still require a bounded, funded handoff before publication can move
to these two reserved workers. Publication remains on its existing path.

This change bounds activation attempts per dispatch and releases workers during
backoff. An attempt still loads the catalog and may join existing workers; full
catalog batching, dependency funding and bounded callback duration are unproven.
Precommit completion reservation at every irrevocable write boundary, complete
persisted-debt progress, native queued recovery shutdown/failure injection, and
the remaining transport, memory, codec, fairness, remote, SDK, adaptive and Cloud
requirements remain open. Native ABI 9, private RuntimeRequest ABI 4 and
execution-services Offer ABI 3 remain unchanged. Full qualification is waived;
the original design is not complete.


### Explicit publisher completion polling (2026-10-18)

`Publisher` now carries an optional `poll_completion` capability, separate from
its blocking `complete` contract. It returns false while durability is pending,
true only for the publisher's allowed acknowledgement outcome, or the original
policy/error cause. Missing capability returns `ExecutionWaitUnsupported`; it
never falls back to a blocking callback. Mirror equality includes this capability.
The added binding field changes the native layout, so **native ABI is now 10**.
Older layouts are rejected before decoding callbacks or publisher payloads.
Private RuntimeRequest ABI 4 and execution-services Offer ABI 3 are unchanged.

`ReplicationDeferredCommitGates.pollForDurabilityAndAuthority` observes the gates
without entering their blocking completion callback. Once all are ready, it
tries the final transition fence and checks current write authority under that
fence. A contended fence remains pending. Fencing/rejection remains an error;
pending never authorizes acknowledgement or deleting the durable obligation.
`RecoveryContext.completion = .poll` selects this path, retaining the same
namespace, original recovery LSN and payload. Unsupported polling is rejected
before invoking recovery publication. The existing default remains `.wait`.

The standard hot-standby binding supplies polling with the existing sync policy
and telemetry. Pending block policy yields false; fail-closed rejects; degraded
acknowledgement remains permitted only when that policy already permits it.
The poll callback never calls `sync_wait_fn`; foreground completion continues to
use the existing wait hook and final authority contract.

`native-publication-poll-v5` passed **97/97 build steps** and **62 functional
cases**: 9 public completion/authority, 2 private publisher-policy, 22 recovery
owner/lifecycle, 15 cross-archive service, 8 storage-context, and 6 executor-archive
cases. Nine source-companion anchors are separate. Archive, context and executor
binaries also passed direct runtime checks. The new archive case forces named
callback dispatch, observes pending and ready, preserves `SyncPolicyUnsatisfied`,
refuses absent polling, and rejects an ABI 9 publisher header before touching a
poisoned payload pointer. The generic C status fixture was corrected to preserve
publisher-domain failures by name, matching production callback transport.

The executor ABI fixture now uses the consumer dependency profile in both
archives. Its unchanged six cases still exercise cancellation, groups, error
translation, real batched file IO and retained immutable-source ownership,
without building an unrelated 24-GiB-declared storage kernel. V1 found an error
value/assertion mistake in the test; V2 stopped at that unrelated build-memory
declaration; V3 exposed the fixture's lossy generic error status; V4 passed;
V5 verifies the final symmetric dependency setup. The development export
`0.1.0-publication-poll-dev.1` passed 25 fetched-consumer semantic build checks.

This is the completion capability and storage helper needed for finite
publication recovery. **The background publication driver has not switched to
polling or to the reserved recovery lane yet.** Its flush/apply/mutation/log
acquisition must be bounded, and payload bytes plus filesystem/provider/allocator
dependencies must be funded before that migration. Actual persisted-outbox
pending/acknowledgement/restart integration, precommit completion reservations,
activation catalog batching, queued recovery shutdown/failure injection, and all
remaining original design requirements remain open. Full qualification remains
waived; these focused results do not establish full-design completion.


### Publication retry driver now yields pending acknowledgements (2026-10-18)

The resident DB publication retry callback now selects
`flushDurableReplicationOutboxesStep` for configurations whose publishers expose
completion polling, including the standard HA bindings. The step reads the same
bounded page (16 v2 obligations plus three legacy singletons), matches/publishes
the original obligation, polls its durability policy, and retains the exact key
until acknowledgement and write authority permit cleanup. Foreground commit and
startup callers keep the existing `.wait` path.

Polling recovery tries the flush, apply, mutation/capture, transition and local
publication-log service fences once. Contention yields `CommitCompletionPending`
without publishing through a held service fence or dropping an intent. The same
nonwaiting mutation/apply acquisition applies to acknowledged-key cleanup.
`RootSharedLease.tryBorrowPhase` opens a retired phase only if capture admission
is available; borrowing an already-held phase never reacquires behind a writer.
Opened phases retire after their borrower releases. This is a recovery operation,
not a call to a pre-decision admission helper after an irrevocable decision.

The retry policy treats pending completion and these contention outcomes as
normal progress: a 250-ms retry deadline, no genuine-failure increment, no failure
report, and no exponential error backoff. Genuine delivery errors retain their
existing behavior. Legacy custom publishers without the polling capability keep
their original ordinary-maintenance completion contract. An opted-in polling
attempt itself never falls back to blocking completion.

`native-publication-step-v2` passed **89/89 build steps** and **54 functional
cases**: 12 public completion/retry, 2 private publisher-policy, 3 real DB recovery,
22 recovery owner/lifecycle and 15 cross-archive cases. The archive binary also
passed a direct runtime invocation. New tests prove pending/fenced outbox bytes
survive orderly DB close/reopen; the actual resident retry owner eventually
clears the key after acknowledgement; normal pending does not increase failures;
flush/apply/capture/transition contention retains the key before publication; and
legacy custom completion still works. The helper tests cover retired-root and
transition/log contention without leaking a capture reader. The existing
mutation-scoped cleanup and mandatory-completion admission tests also pass.
V1 detected a redundant checkpoint on the old waiting path; V2 restricts that
checkpoint to polling and preserves the original four-check sequence. The failed
V1 build was intentionally stopped while it compiled duplicate cases; both owned
processes were confirmed gone before source edits. V2 uses precise test selectors.

The current development export is `0.1.0-publication-step-dev.2`; all 25 fetched
consumer semantic checks passed. Dev1 predates the checkpoint correction and is
not current qualification. Native ABI 10, private RuntimeRequest ABI 4 and
execution-services Offer ABI 3 are unchanged.

**Publication still uses ordinary maintenance dispatch.** The named service
fences now yield, but WAL matching may still scan/block, storage transactions
retain their internal waits, and payload bytes plus filesystem/provider/allocator
resources are not yet funded for progress under ordinary exhaustion. Resolve
those dependencies before moving publication onto the two reserved recovery
workers. Legacy custom bounded contracts, real default HA-log dedup/restart under
overload, post-ACK cleanup contention/shutdown fault coverage, precommit resource
reservation, activation batching, remaining memory roots/codecs/transports and
full fairness/remote/SDK/adaptive/Cloud requirements remain open. Orderly reopen
with a test publisher is not a proof of power-loss or the complete HA protocol.
Full qualification remains waived; the original design remains incomplete.


### WAL recovery matching borrows retained records (2026-10-18)

Native `WAL.withEntryAt` now verifies and visits one exact entry while its read
transaction owns the bytes. It does not duplicate the payload into a caller
allocator. The callback is synchronous, cannot retain the slices, and must not
reenter a write while the native read transaction is held. Every callback/error
exit aborts the read transaction. The compiled WAL client provides the same
borrow through the existing owner-buffer read ABI, retiring that buffer after
the callback without allocating a second consumer payload copy. Missing entries,
malformed replies, callback errors and read failures retire the result buffer.

`ReplicationLog.withEntryAt` decodes a borrowed `EntryView`, preserving outer WAL
and logical envelope/checksum validation, exact LSN and predecessor checks.
`Primary.findMatchingRecordFrom` uses that visitor instead of allocating/freeing
an owned record for every inspected LSN. Kind, shard, table and payload equality
remain unchanged, and a missing retained LSN still yields `WalNoLongerRetained`
instead of skipping a gap and authorizing a duplicate append. Owned and borrowed
exact-entry validation share an overflow-safe predecessor check, returning
`RecordPreviousLsnMismatch` for a maximum predecessor value instead of trapping.
Other streaming-reader bounds remain separate work.

`native-wal-borrow-match-v1` passed **89/89 build steps** and **43 functional
cases**: 1 native WAL borrow, 12 private replication-log/primary, 12 public
completion/retry, 2 private publisher-policy, 15 archive-service, and 1 compiled
WAL client-buffer case. Both archive binaries also passed direct runtime checks.
The real primary test searches multiple retained records with its allocator
refusing every allocation; namespace/kind mismatches, missing values, and a
truncated retention prefix retain their expected outcomes. New borrow tests
verify callback rejection, outer/inner corruption, overflow-safe predecessor
validation, and owner-buffer lifetime across every tested reply/error branch.
The client fixture uses the existing C buffer ABI in two independently compiled
archives, without requiring the full physical storage-kernel qualification.
The export `0.1.0-wal-borrow-dev.1` passed 25 fetched-consumer semantic build checks.
Native ABI 10, private RuntimeRequest ABI 4, execution-services Offer ABI 3 and
storage-owner ABI 100 are unchanged.

This removes the primary's per-record payload copy and the compiled client's
second copy. It does **not** prove allocation-free matching: transaction snapshots
and compiled-owner result buffers still have their own allocation dependencies.
The search can still inspect arbitrarily many records/bytes and CRC work is not
yet bounded by a recovery work authority. Those resources, outbox payload/store
writes, and filesystem/provider dependencies still need funding before publication
can move from ordinary maintenance to the reserved recovery workers. Precommit
completion reservation and all other original-design requirements remain open;
full qualification remains waived and the design is incomplete.

### Latest main integration and spill creation progress (2026-10-18)

The canonical branch now includes origin/main at
`f91b7d0d16900133a310db5a888678ec126b8012`, merged in
`65b569cf7b39c4d8f54d6082336f24af868f8dc8`. The existing implementation work
remains in the same worktree and branch. The merge preserves main's lake
navigation, prepared text, typed numeric and partition refinements together
with the pooling branch's execution, cancellation and ownership controls.
Lazy source capture uses temporary execution authority and detaches it before
retaining the source. Scoped grouping keeps its existing coordinator scratch
path; unscoped grouping retains the new typed census and merge path.

Integration testing exposed a spill creation lock cycle. The shared accounting
mutex was held across filesystem I/O and CPU reentry. A peer could own the only
ordinary query slot while spinning on accounting, preventing the creator from
resuming and releasing that mutex. Directory initialization now has a separate
mutex. File creation reserves its slot and sequence under a short accounting
lock, releases that lock before suspension, and rolls back failed creation after
any acquired handle has retired. This also makes in-flight file creation count
against the existing 64-file ceiling.

A new regression forces metadata and protected control work to progress at the
suspended creation boundary, for both a cold directory and an already-open
directory. It verifies pre-admitted file quota and complete retirement. The
existing compaction test uses its original 250 ms waiting policy; the temporary
larger test budget and diagnostic instrumentation were removed. Twenty repeated
runs of the focused spill matrix passed, with the slowest below 75 ms.

The current combined receipt is `native-main-lake-merge-v6`: 102/102 build
steps, 98 functional cases, and a production SQL semantic check. The
`native-main-lake-spill-lock-v1` receipt additionally covers the existing
seven-scenario failed-resume/handle-retirement test, for 99 distinct functional
cases in total. Eleven module discovery anchors and repeated executions are
counted separately. All cases completed without failures, skips or leaks.
The current export `0.1.0-main-lake-merge-dev.4` passed 25 fetched-consumer
semantic build checks; all 465 changed Zig files passed AST checks. Native ABI
10, private RuntimeRequest ABI 4, execution-services Offer ABI 3 and
storage-owner ABI 100 are unchanged.

This closes the observed accounting/I/O lock cycle. It does not complete
filesystem, snapshot, payload or provider funding throughout recovery. Bounded
WAL record/byte/CRC work, complete outbox dependency funding before reserved
dispatch, precommit completion capacity, actual HA crash/overload/shutdown
coverage, activation batching, and the other original design requirements
remain open. Full qualification remains waived; the full design is incomplete.

### Admitted WAL recovery preserves lifetime through bounded byte work (2026-10-18)

Admitted durable-outbox recovery now reaches a distinct controlled publisher
callback, the primary matching loop, replication envelope decoding, and the
native or compiled-owner WAL read. Outer WAL CRC, replication payload CRC,
compiled-owner buffer copying, and exact payload comparison check the retained
original lifetime between at most 64 KiB of byte work. Every record boundary
also checks it. These checks preserve the original cancellation, deadline,
scheduler-close or catalog-generation error; the compiled client retains that
identity across the coarse C status boundary. Borrowed records still avoid a
second consumer payload copy.

Publication locks and borrowed read snapshots remain retained dependencies.
The recovery entrypoint and nested visitors restrict their admission ports to
lifetime checks, with no CPU checkpoint or wait callback. The physical owner
frees a partially copied result on rejection, including a failure after the copy
callback returned. The client destroys a received owner buffer on every exit,
including populated failure replies. Snapshot creation/get and the allocator
call are still physical operations without complete admission or parent funding;
byte quanta do not establish an end-to-end elapsed-time or memory bound.

Controlled capability refusal occurs before publication locks and publisher
telemetry. A missing capability never falls back to the unchecked matching
contract. After an actual append, request rejection cannot replace mandatory
completion or final authority checking. The existing retention-gap fail-closed
rule, kind/table/shard filters, exact payload matching and persisted formats are
preserved. Legacy materializing iteration, encoded-byte counting and retained-age
scanning now share the overflow-safe predecessor validation already used by
exact reads; all five entry/scan APIs reject an overflowing predecessor with a
typed corruption error.

The changed publisher vtable requires **native ABI 11**. The two-archive test
rejects ABI 10 before dereferencing the supplied invalid payload. The WAL owner
adds `antfly_storage_wal_read_with_admission`; existing C layouts and storage
owner ABI 100 are unchanged. Private request ABI 4 and service offer ABI 3 are
unchanged.

`native-controlled-wal-v5` passes **102/102 build steps and 67 distinct functional
cases**: 17 native WAL/replication/primary cases, 15 storage/runtime publisher
cases, 17 pgwire consumer cases, 16 cross-archive service cases and 2 compiled WAL
client cases. No module-discovery anchors are counted. Both compiled consumer
programs also pass direct execution. The production SQL target is semantic
compilation, not runtime qualification. All 19 changed Zig files pass AST checks.
The exported `0.1.0-controlled-wal-dev.2` package passes its 25-step fetched
consumer semantic check; its source manifest is checked against the current
worktree. This phase's receipt and verifier retain the precise source hashes.

**The default background step is not yet converted.**
`flushDurableReplicationOutboxesStep` currently calls recovery with null controls;
its `IntentCollection` consequently has no admission port. That path needs an
owned shutdown lifetime plus a persistent record/byte scan cursor that can return
pending after retiring all publication/snapshot dependencies. The metadata HA
adapters likewise still use the legacy synchronous matcher. The admitted path
added here is a prerequisite, not proof of bounded default recovery scheduling.
Full namespace/provider/owner-buffer funding, precommit mandatory completion
capacity, real default HA crash/overload/shutdown tests, activation batching and
all other original-design gaps remain open. Publication remains on ordinary
maintenance capacity until its entire progress dependency graph is funded. The
user waived full qualification; the full design remains incomplete. The work
stays uncommitted on the canonical branch/worktree after main merge `65b569cf`.

### Background publication uses its joined owner shutdown lifetime (2026-10-18)

The default `flushDurableReplicationOutboxesStep` now supplies owned shutdown
authority instead of null admission controls. Its synchronous
`BackgroundPublication` scope reads both `async_context.background_closing` and
`publication_recovery.stopping`. It rejects closing before publication fences
and remains live through pending-record reads, checksums, publisher recovery and
native WAL matching. The resident retry owner joins its jobs before the DB and
its shutdown flags retire. The scope uses the existing worker stack and retains
no frontend request or reset deadline. Foreground recovery keeps its original
caller controls.

The scope cannot transfer CPU or wait for another execution while publication
fences are retained. Existing admitted WAL CRC, record CRC, owner-copy and exact
comparison quanta therefore observe shutdown on the actual default background
path. A pre-append rejection preserves the durable intent. Once publication has
crossed its irreversible boundary, completion and final authority retain their
existing mandatory contract. Legacy custom publishers without completion polling
continue through their explicit ordinary-maintenance contract; controlled calls
still refuse a missing controlled-recovery capability.

The real DB suite now verifies DB shutdown before publication, retry-owner
shutdown before publication, resident close during a controlled matching
callback, physical callback retirement before DB close returns, and subsequent
reopen/replay of the exact persisted intent. The concurrent-close case uses a
cooperative fixture publisher with a finite test-only guard. Separately, a
private integration case opens a real DB, native HA primary/WAL and slot store.
An observing publisher proxy delegates matching and policy polling to the actual
HA adapter and flips the DB owner's shutdown flag at every observed matching
checkpoint. Every cancellation preserves the exact intent and WAL LSN 1. Real
standby acknowledgement then permits cleanup without a duplicate WAL append.
This proves the native matching/acknowledgement path through the production
background entrypoint; it is not crash, distributed overload or compiled-owner
shutdown qualification.

`native-background-wal-v2` passes **102/102 build steps and 56 distinct functional
cases**: 17 WAL/replication/primary, 16 storage/runtime publisher, 5 real DB recovery,
16 cross-archive service and 2 compiled WAL client cases. No discovery anchors
are counted. Both compiled consumer programs also pass direct execution. The
production SQL target is semantic compilation. All five changed Zig files pass
AST checks. The `0.1.0-background-wal-dev.1` export passes its 25-step fetched
consumer semantic check and current-source manifest verification. Native ABI 11,
private request ABI 4, service offer ABI 3 and storage owner ABI 100 apply.

The prior phase's null-background-control gap is closed. **Resumable scan work
is still incomplete.** A correct cursor needs stable outbox identity,
destructive-WAL revision validation, bounded record/byte/CRC state, and an atomic
append check of the completed absence witness. An LSN-only cursor can miss a
record replaced by truncation/reappend; a hard limit that restarts the same scan
can prevent progress. Snapshot creation/get, physical providers, owner-buffer
allocation, append work, full outbox precommit completion capacity, metadata HA
adapters and the other original-design requirements remain open. Actual
publication stays on ordinary maintenance until its entire progress dependency
graph is funded. Full qualification remains waived by the user, and the full
design remains incomplete. Implementation work is uncommitted on the canonical
branch/worktree after main merge `65b569cf`.

### WAL recovery consumes an atomic absence witness (2026-10-18)

Controlled publisher recovery, including the default background path with its
owned shutdown lifetime, now captures an ephemeral position for one open WAL.
The position identifies its in-process source owner, open instance, destructive
revision and next LSN. A completed absence scan authorizes only a conditional
append that checks the entire position under WAL append serialization. Another
writer, prefix deletion, suffix rewind/reappend or a changed owner invalidates
that authorization. Busy and changed outcomes retain the durable intent and
return ordinary pending progress without publication or failure telemetry.
Retry can reuse the matching record appended by the competing publisher.

Position identities are volatile and must never survive a process lifetime.
Owner and destructive-revision exhaustion fail closed without reusing a valid
identity. The additive C owner operations preserve storage owner ABI 100 and
validate request versions, reserved fields, flags, positions and returned LSNs.
Checked client relays preserve the original rejection cause, and successful
conditional publication performs no postcommit request-lifetime checkpoint.
Native ABI 11, private request ABI 4 and service offer ABI 3 are unchanged.

Prefix truncation now shares writer serialization. Empty-log bootstrap also
uses the canonical WAL operation instead of externally changing its append
cursor. Conditional append and empty-log bootstrap retain the advanced cursor
when completion fails after a successful commit. A transaction commit error can instead have an ambiguous durable
outcome: the open WAL is quarantined against subsequent writes until reopen
reconstructs its durable cursor. Rejected precommit work aborts its transaction;
physical commit counters advance only when a commit was actually attempted.
Canonical WAL APIs participate in this protocol; direct raw backend mutation
is outside the position contract.

The focused runtime checks cover destructive tail ABA, prefix deletion, foreign
and reopened owners, no-op truncation, busy coordination, original lifetime
rejection at every precommit boundary, competing conditional writers, owner and
revision exhaustion, bootstrap completion errors, and injected transaction
failures both before and after physical apply. The publication integration test
injects a competing append after absence and before conditional serialization,
checks pending without a duplicate append or inflated failure telemetry, then
retries against the existing record. Existing DB close/reopen and actual native
HA matching/standby acknowledgement regressions remain included.

`native-wal-position-v1` passes 102/102 build steps and establishes 90 distinct
functional cases:
48 WAL/replication/primary/owner, 17 storage/runtime publisher, 5 DB recovery,
16 cross-archive service and 4 compiled WAL client cases. No discovery anchors
are counted. Both compiled programs also pass direct execution. The production
SQL target passes semantic compilation. All 16 changed Zig files pass AST
checks. The `0.1.0-wal-position-dev.1` export passes its fetched consumer's 25
compilation steps, with all 4,408 current source manifest entries verified.

This closes the atomic absence-to-append prerequisite. **The scan is still not
resumable or bounded in total records/bytes.** The next cursor must preserve
stable outbox identity and incremental record/CRC/equality progress, use bounded
raw WAL windows and validate destructive revision. Current try-only coordination
does not establish fairness under continuous writes. Namespace snapshots/get,
physical provider and buffer/append funding, precommit mandatory completion
capacity, metadata HA adapters, actual crash/overload/shutdown qualification and
the other original-design requirements remain open. Publication remains on
ordinary maintenance until its entire progress graph is funded. Full
qualification is waived, and the full design remains incomplete. Implementation
changes remain uncommitted on the single canonical branch/worktree after main
merge `65b569cf`.

### Idempotent WAL completion retires only live transactions (2026-10-18)

`WAL.appendIdempotent` now scopes its transaction abort handler to the period
before commit consumes that transaction. Physical sync and completion follow
outside that scope. A completion error after commit preserves the advanced
append cursor and durable idempotency receipt, allowing a retry to return the
existing LSN without appending or waiting for completion again. A conflicting
digest remains an error. The test-only corruption hook uses the same scoped
ownership pattern before its postcommit sync.

A new on-disk regression injects the first completion failure, retries the same
key, rejects a conflicting digest, appends another idempotent and ordinary
record, then closes and reopens the WAL. It verifies both durable receipts,
exact payloads and the next append cursor. `native-wal-idempotent-v1` passes
82/82 build steps and 69 distinct functional cases: 33 public WAL/related,
16 private WAL/replication/owner, 16 cross-archive service and 4 compiled WAL
client cases. Both compiled programs pass direct execution. The production SQL
target passes semantic compilation and the changed WAL source passes AST checks.
The preceding 90-case phase and package export retain their recorded source
identities; this focused correction does not refresh their source snapshots.

This repairs a concrete ownership hazard in durable retry handling. Bounded
resumable recovery scanning, complete snapshot/provider/buffer/append admission,
precommit mandatory completion reservations and the remaining original design
requirements are still unfinished. Publication remains ordinary maintenance
until its entire progress graph is funded. Full qualification remains waived.
The implementation is uncommitted on the canonical branch/worktree.

### WAL recovery carries original controls into namespace reads (2026-10-18)

Controlled WAL reads now use explicit namespace snapshot and point-lookup
capabilities. A missing controlled capability fails closed rather than silently
calling an unadmitted operation. The LSM namespace provider carries the original
lifetime into backend mutex admission, source capture, read-view preparation and
its existing controlled point-read pipeline. Controls remain synchronous borrows;
returned transactions never retain the caller's port. Rejection after capture
retires the transaction before returning it to the caller.

Namespace and bound-store source capture now restrict checks to lifetime while
the backend mutex is held. They can use the original wait authority before
acquisition and checkpoint after releasing that mutex. Tests reject any CPU
checkpoint made under the backend mutex. The WAL retains its publication/snapshot
dependency restriction throughout. Internal backend contention returns pending
publication progress without a duplicate append or inflated failure telemetry;
an original caller rejection retains its cause.

Checked namespace callbacks use `read_admission.ErrorRelay` to preserve the
original control error even when it is absent from the runtime error catalog.
The relay records that error, transports a catalogued cancellation across the
checked boundary and restores the recorded cause before publishing a returned
owner. Lifetime, deadline, retirement, remote executor and explicit execution-wait
capabilities remain synchronous borrows. A returned capture is aborted if a
provider ignored a rejected callback. Native ABI **12** rejects older namespace
callback layouts; private request ABI 4, service offer ABI 3 and logical C storage
owner ABI 100 remain unchanged. The archive fixture checks the immediately
preceding native version before dereferencing an invalid payload pointer.

`native-namespace-read-v1` and its private ABI-fixture supplement establish
**94 distinct functional cases** with 102/102 main build steps: 52
WAL/replication/namespace/owner, 17 storage/runtime publisher, 5 DB recovery,
16 cross-archive service and 4 compiled WAL client cases. The focused run passes
72 cases and 82/82 steps. Both compiled programs also pass direct execution.
Production SQL passes semantic compilation. Nine changed Zig files pass AST
checks. The `0.1.0-namespace-read-dev.1` export passes 25 fetched-consumer
compilation steps and verification of all 4,408 current public source entries.
The ABI supplement changes only its private test fixture; the main integration
receipt retains its original source snapshot and the supplement records that
single input overlay explicitly.

**This is not a bounded raw-window implementation.** Namespace point lookup
still resolves complete values, and source capture/provider/buffer/append work
still needs its complete physical resource graph. A resumable record/byte/CRC
cursor must preserve stable outbox identity and consume the existing WAL
owner/revision/atomic-append guard. Full precommit mandatory completion capacity,
metadata HA adapters, default HA crash/overload/shutdown qualification and the
other original-design requirements remain open. Publication remains ordinary
maintenance until its entire progress graph is funded. Full qualification remains
waived, and the full design remains incomplete. Work remains uncommitted on the
single canonical branch/worktree.

### V2 outbox recovery resumes whole-record progress (2026-10-18)

Modern persisted v2 outbox recovery now scans at most **16 complete WAL records**
per admitted attempt. A pending attempt releases its snapshots, publication
fences and worker before retrying. Its progress lives in one fixed cursor in the
DB's `LocalExecutionState`, shared by immediate and prepared mutation receivers.
Independent DB owners do not overwrite each other's progress on a shared Primary.
The cursor owns no payload, request control, snapshot or provider allocation; the
publisher borrows it only during its synchronous recovery callback. Its mutex is
separate from resettable scalar state and is never copied while live.

The existing immutable v2 key supplies the copied 128-bit identity. Progress also
checks the original from-LSN, record kind, payload length, namespace, Primary
identity and ephemeral WAL origin/owner/destructive revision. Fresh payload
buffers can therefore resume the same immutable intent. Append-only tail growth
preserves verified progress; truncation, replacement and reopen invalidate it.
A completed absence scan still requires the existing atomic conditional append.
A verified match caches only its LSN so retries waiting for standby ACK need not
repeat the scan; every retry checks its current lifetime and WAL history before
using that match. Original rejection errors remain authoritative. Pending scans
and backend contention preserve the persisted intent without failure telemetry.

Native ABI **13** rejects the older publisher callback layout before using its
payload. Private request ABI 4, service offer ABI 3 and logical C storage owner
ABI 100 remain unchanged. The persisted AHO1 outbox encoding is unchanged; the
new identity and cursor are in-memory callback metadata. Legacy singleton and
transaction adapters without immutable v2 identity retain their prior synchronous
scan path; modern identity without its cursor fails closed.

The focused integration receipt `native-record-cursor-v3` establishes **101
functional cases** and 102/102 build steps: 39 public WAL/namespace/codec,
19 private WAL/Primary/native-owner, 18 publication, 5 DB recovery,
16 cross-archive service and 4 compiled WAL-client cases. New tests exercise
alternating independent owners, fresh payload allocations, growing tails,
intent/namespace replacement, destructive tail ABA, prefix deletion, corruption,
atomic absence consumption, lock contention and original rejection. The actual
DB background path resumes through three pending quanta, discovers an existing
record, preserves the exact persisted intent while awaiting a real standby ACK,
and clears it without appending a duplicate after ACK. Existing golden and
corrupt AHO1 codec fixtures pass. Both compiled programs also pass direct
execution, production SQL passes semantic compilation, and nine changed Zig
files pass AST checks. The `0.1.0-record-cursor-dev.2` export passes all 25 fetched
consumer build steps and verification of all 4,408 current public source entries.

**The limit counts records, not bytes.** A single point lookup can still decode
a complete compressed block and value, and CRC/equality work remains whole-record.
Outbox selection/copying and snapshot/provider/buffer/append work still require
complete physical funding. Within-DB fairness under continuous insertion of
lower-sorted outbox keys is not established by independent-owner tests. Bounded
byte/CRC continuations, precommit mandatory completion reservations, legacy
metadata HA adapters, default HA crash/overload/shutdown proof and the remaining
original design remain unfinished. Publication remains ordinary maintenance.
Full qualification remains waived; the full design remains incomplete. Work is
uncommitted on the single canonical branch/worktree.

### HA WAL allocations follow the owning node budget (2026-10-18)

The native WAL now accepts an explicit allocator and retains it through close.
Its ordered store, namespace wrapper, large encoded-entry buffers, idempotency
keys/receipts and truncation lists use that allocator. The existing allocator-free
open remains compatible. HA Primary, slot, fence, standby receive/progress,
lifecycle-ledger and identity-reader entry points now propagate their caller's
allocator. Standalone HA owners share one thread-safe, callback-free allocator
charged to the existing node root; they retire before that allocator and root.
This does not introduce another process ceiling.

The compiled-storage client has an additive allocator-aware WAL open operation.
Its allocator bridge lives in caller-funded stable storage, and the physical
owner copies the language-neutral descriptor into its stable handle before
creating allocator adapters. Moving the returned client WAL cannot leave a
callback pointing into an expired stack frame. Open failures unwind the bridge,
handle and path; close retires the WAL before releasing either bridge. The old
logical open request and native callback layouts are unchanged: native ABI 13,
logical C storage-owner ABI 100, memory-allocator ABI 1, private request ABI 4 and
service-offer ABI 3 remain current.

LSM point and replay scratch now preserve explicit callback-free budgeted roots
and local runtime-memory ABI adapters instead of selecting libc. Budgeted result
arenas are unwrapped so decoder/key scratch can be reclaimed independently of
result storage. The WAL's native-storage object and its owned pool also use the
backend allocator. These paths do not introduce reclaim callbacks under storage
locks. Unrecognized legacy allocators retain their previous scratch policy.

`native-wal-root-v3` passes **128 distinct functional cases** and 89/89 build
steps: 44 public WAL/namespace/codec/allocator, 64 private WAL/HA/owner,
16 cross-archive service and 4 compiled WAL-client cases. Eight new cases cover
storage-root propagation, large-envelope refusal before store invocation,
every allocation failure during in-memory open, independent arena scratch
retirement, node-budget refusal across ABI adapters, stable opaque open/append/
close ownership, unfunded Primary refusal and allocator inheritance across the
other HA log owners. Existing slot persistence, fencing, standby replay/retry,
recovery cursor and idempotent completion regressions pass. Both compiled test
programs also pass direct execution. Production SQL and actual standalone/data/
metadata startup bodies pass semantic checks, including both compiled and native
storage profiles. Nineteen changed Zig files pass AST checks. The
`0.1.0-wal-root-dev.2` export matches all 4,408 current public source entries and
passes all 25 fetched-consumer build steps.

This phase does not refresh the preceding publication/DB integration receipt;
its 101-case result retains its recorded source identity. Startup compilation is
not an actual server-start or overload qualification. Complete physical funding
still requires compiled read-response buffers, shared FD/lock registries, other
entry points and a double-charge audit of nested resource managers. Reclaiming
allocator wrappers still need a callback-free funded scratch capability.
Byte/CRC-resumable compressed reads, the entire outbox progress graph and
precommit mandatory completion capacity remain unfinished, along with the other
original-design requirements. Publication remains ordinary maintenance. Full
qualification remains waived and the full design remains incomplete. Work is
uncommitted on the single canonical branch/worktree.

### Compiled WAL replies and parser storage use the caller root (2026-10-18)

Allocator-open WAL clients now provide a synchronous allocation descriptor for
read and scan replies. The physical provider allocates reply bytes and scan
encoding storage through that descriptor, without retaining it. The caller owns
the returned bytes and releases them through its originating allocator. Legacy
read/scan operations and their C buffer destructor retain their existing
contract; the new operations are additive at logical C storage-owner ABI 100.
Native ABI 13, memory-allocator ABI 1, private request ABI 4 and service offer
ABI 3 remain unchanged.

Client reply/page tokens copy the allocator value before foreign dispatch.
Their cleanup needs neither the WAL handle nor its allocator bridge. The
original allocation context must still outlive the invocation and byte
retirement, as for other caller-owned buffers. A visitor may close the opaque
WAL and still inspect its detached reply. Scan visitors returning stop can do
the same: the returned wire bytes and parser metadata retire independently of
the closed WAL. Rooted streaming parsers no longer use `page_allocator` for
their entry metadata.

Cleanup is installed before dispatch and covers rejection before or after the
foreign call, populated error replies, malformed found/done flags, corrupt scan
encoding, missing entries, visitor rejection and allocation failure. Original
lifetime errors retain their identity across the coarse C status boundary. No
response, parser or returned token retains a read-control port or snapshot.
The actual provider closes its snapshot before invoking the consumer visitor.

`native-wal-reply-v1` passes **133 distinct functional cases** and 89/89 build
steps: 44 public WAL/namespace/codec/allocator, 66 private WAL/HA/owner,
16 cross-archive service and 7 independently compiled WAL-client cases. Two new
actual-provider cases close the WAL from read/scan visitors, verify surviving
reply bytes and then verify full tracked allocation retirement. The controlled
read also refuses an allocation without leaving a live reader. Three new
compiled-provider cases exercise malformed and populated-failure replies,
original rejection at every checkpoint, all allocation failures through reply
encoding/copy/parser storage, and visitor-driven close. The two compiled test
programs pass direct execution. Production SQL and actual startup bodies pass
semantic compilation in both native and compiled storage profiles. Nine changed
Zig files pass AST checks. The `0.1.0-wal-reply-dev.1` export matches all 4,408
current public source entries and passes 25 fetched-consumer build steps.

This funds complete reply values; it does not introduce byte-resumable lookup,
codec, checksum or equality work. A scan still permits an oversized first record
under its existing paging contract. Legacy opens retain their old response
allocation policy. Shared FD/lock registries, other entry points, callback-free
funded scratch for reclaiming wrappers and nested-budget double-charge checks
remain open. The complete outbox resource graph, precommit mandatory completion
capacity, selection fairness and all other original-design scope remain
unfinished. Earlier publication/DB receipts retain their own source identities.
Publication remains ordinary maintenance. Full qualification remains waived,
and the full design remains incomplete. Work remains uncommitted on the single
canonical branch/worktree.

### Budgeted scratch suppresses owner reclamation per operation (2026-10-18)

`BudgetedAllocator.threadSafeNoReclaimAllocator()` supplies a stable view of the
same owner, reservation ledger and physical backing allocator. Its allocation,
resize and remap operations suppress that budget's reclamation hooks without
mutating `reclaim_on_denial`. Ordinary and failure-tracking views keep their
existing policy. Views can resize, remap and free each other's allocations;
they share the same synchronization and live-byte accounting. The view creates
no wrapper allocation, additional process ceiling or separate reservation.

The locked allocation helpers now select reclamation policy per call. Ordinary
reclamation still runs after releasing the allocator mutex and retries once.
The no-reclaim view fails admission without entering those hooks. Its backing
allocator must obey the storage dependency's lock contract too; this capability
does not rewrite arbitrary backing callbacks. `fromAllocator` recognizes the
new view and continues to unwrap known arena wrappers to the physical owner.

LSM point and WAL replay scratch now select this view for local registered
budgeted owners, including owners configured to reclaim. These owners no longer
escape their ceiling through libc scratch allocation. Result arenas remain
independent of scratch retirement. Opaque memory adapters retain the policy
chosen at their origin; they are not inspected or rewritten as foreign budget
objects. The existing ABI scratch tests now supply an explicit no-reclaim view
from a reclaiming owner.

`native-scratch-root-v2` passes **145 distinct functional cases** and 89/89 build
steps: 56 public WAL/namespace/codec/resource/allocator, 66 private WAL/HA/owner,
16 cross-archive service and 7 compiled WAL-client cases. The new mixed-view
case holds a dependency mutex while allocation, resize and remap growth are
refused with zero reclaim calls. It verifies shrink/growth accounting, shared
ownership and unchanged policy, then proves ordinary resize, remap and
allocation can each reclaim after the mutex is released. Its reclaimer checks
that neither the allocator nor dependency mutex is held. Existing budget
concurrency, credit-transfer, node/slice refusal and operation-local failure
receipts also pass. The two LSM scratch cases now exercise reclaiming owners.
The new test is registered in the ordinary resource-budget suite; this phase
executes it through the focused WAL/resource target rather than the full suite.

Both compiled test programs pass direct execution. Production SQL and actual
startup bodies pass semantic checks in native and compiled storage profiles.
Four changed Zig files pass AST checks. The `0.1.0-scratch-root-dev.1` export
matches all 4,408 current public source entries and passes all 25 fetched-consumer
build steps. The last source change only registers the test in the private build
catalog; the exported public sources and consumer result remain current.
Native ABI 13, logical C storage-owner ABI 100, memory-allocator ABI 1, private
request ABI 4 and service-offer ABI 3 remain unchanged.

Full resource funding still requires shared FD/lock registries, remaining entry
points and an audit of nested budgets and backing/opaque allocator contracts.
Byte/CRC-resumable compressed reads, precommit mandatory completion capacity,
the complete outbox progress graph, selection fairness and the other original
design scope remain unfinished. Earlier publication/DB receipts retain their
own source identities. Publication remains ordinary maintenance. Full
qualification remains waived and the full design remains incomplete. Changes
remain uncommitted on the single canonical branch/worktree.

## Byte decode continuation ownership (2026-10-18)

The Snappy cursor owns only scalar offsets, partial headers and copy metadata;
the raw/Snappy table cursor additionally owns incremental CRC state. Neither
retains a control, reader, allocator or buffer. Each invocation borrows the same
immutable input contents and the preserved decoded prefix, with addresses free
to change. Output remains unpublished until completion. Readers own physical
admission and window bounds; the streaming-view decoder reuses its existing
8 KiB cache. Caller allocations fund the full output.

The table allocation wrapper checks CRC and size before allocating output and
retires that allocation on decode or original-authority rejection. Its generic
block contract does not replace the runtime's separate raw-block fast path or
remove the LSM's whole-compressed-input allocation. Prefix reconstruction still
uses its existing owner. Synchronous wrappers do not release workers between
steps. An owned immutable source lease, funded physical windows and operation
close/join semantics are required before WAL recovery can return workers during
physical decoding. `native-byte-decode-v4` supplies scoped codec evidence through
196 functional cases; complete physical funding and recovery remain unfinished.

## Immutable-run streaming input ownership (2026-10-18)

Eligible raw/Snappy local block reads keep one RunRangeReader through the
operation. Leased providers own a single active source use; native pathname
reads borrow the enclosing immutable run read version. Backend, storage, path
and version must outlive the reader. No original port escapes a read call.
Replaceable CURRENT/WAL paths are excluded by this contract.

Raw output is read and checksummed in 64 KiB chunks with no extra input buffer.
Snappy owns an at-most-64-KiB window from the existing local workspace and a
separately funded full output. CRC/header validation precedes Snappy output
allocation. The source stays bound across both passes; second-pass CRC verifies
the actual input decoded before output publication. A failed refill invalidates
cache metadata before calling the provider. Scratch budget reflects the fixed
window; output and source reservations retain their existing owners. Per-read
physical requests are at most 64 KiB; one codec step can cross two input windows.

Leased physical reads park/reenter only the original execution's permitted wait
boundary. Rejected begin performs no read, rejected reentry cannot publish, and
retained-dependency controls do not lend CPU. Output, workspace and source-use
retirement need no fresh execution grant. The 199-case `native-stream-input-v5`
receipt verifies these scopes. Full prefix/provider coverage and an owned WAL
read operation that survives returning the worker are still required.

## Movable block read operation ownership (2026-10-18)

OwnedRunBlockRead keeps its source use, owned path, workspace/window and partial
output between finite calls. Its fixed scalar metadata contains no current
control or self-relative reader adapter. Init and step borrow the original
control; a later step supplies a fresh borrow. The existing bounded workspace
pool limits concurrently pending block owners, and scratch/result accounting
retain their respective existing parents.

Ready releases source and scratch immediately. Verified output then belongs only
to the supplied allocator and transfers once through takeOutput; publication
still requires caller authority. Failure immediately frees partial output and
working resources and retains only its sticky error. Abandonment/deinit never
asks for a new grant. With an independently valid output allocator, ready/failed
state cleanup and verified output access survive backend destruction.

Pending state does not own a namespace snapshot or backend lifetime. The caller
must retain an immutable run read version and keep backend/storage alive through
retirement; an owned path alone cannot bind pathname generations. Namespace/WAL
operation registration, close/join cleanup and the remaining resource graph are
still required before the synchronous get path can return workers between calls.
Decode/copy/checksum steps use at most 64 KiB input/output quanta and fixed-size
provider requests; output allocation work remains separately unqualified. The
201-case native-owned-block-v3 receipt establishes only these scoped properties.

## Owned namespace lookup and close ordering (2026-10-18)

OwnedNamespaceValueRead owns its NamespaceReadTxn snapshot, namespace/key copies,
run cursor/index, pending block operation and partial result. Caller stack keys
and per-step controls do not escape. Mutable, immutable and in-memory-run values
are copied through finite quanta; path-backed raw/Snappy values can transfer an
interior logical span together with their verified owned block allocation.
The original result allocator must outlive the transferred OwnedNamespaceValue.

Pending operations hold the existing namespace reader pin. Backend.close first
marks closing, refuses new captures and waits for pins; operation retirement
releases block/workspace, index and owned metadata before aborting its snapshot
as its final backend-dependent action. Ready/failure/deinit need no fresh grant.
Concurrent close is verified with pending mutable copying and a live decoded
block workspace. A WAL/job close registry still must call/join this retirement
before destroying its backend or bridge; that registry is not implemented here.

The directory cursor carries a 64-credit per-step search limit without retaining
caller borrows; legacy full-traversal callers retain their behavior. Snapshot
capture, cold index I/O, entry search, allocation and prefix reconstruction still
need broader continuation/funding qualification. Native-owned-value-v2's 205
cases establish the scoped native ownership semantics, not complete WAL recovery
or production default-overload qualification.

## Native WAL owned-read registry and caller-owned handles (2026-10-19)

The optional erased namespace lookup capability returns a caller-funded boxed
operation. Original controls remain synchronous borrows and rejection relays
retire ignored-rejection results. Detached value allocation keeps its original
allocator independently of store wrappers. Private native ABI 14 guards the new
vtable; public C owner ABI 100 does not change.

Native WAL keeps sixteen embedded operation slots. Start reservations and
retirement counters protect store/backend lifetime while callbacks and cleanup
run outside the registry lock. Per-operation locks join steps with close; an
atomic closing flag stops new steps after a close claim. Caller destruction
releases a claimed slot to close before freeing its independent metadata.
WAL close/abandonment retires snapshots and result allocations, detaches handles,
and joins captures/retirements before backend or bridge destruction. Closed
handles use no WAL pointer and can be destroyed later if their allocator context
remains valid. Parent entry points still require caller lifetime coordination;
caller handle destruction ends its lifetime for subsequent calls.

Outer WAL CRC advances through byte quanta. Verified data is delivered only by
bounded copies into caller storage, never a raw provider span across close.
The 210-case native-wal-owned-v4 receipt covers registry capacity/reuse, allocator
failure cleanup, checked original errors, framing and a three-thread step/close/
destroy race. C operation handles/bridges and Primary byte recovery are not yet
wired. Metadata, prefix, provider and complete resource/mandatory-completion
funding work remain open; this is no full-design/default-overload claim.


## Internal C-facing owned WAL reads (2026-10-19)

`native-wal-owned-c-v3` extends the preceding native-only operation across the
compiled owner boundary. Additive ABI-100 begin, step, chunk and destroy requests
validate version, reserved fields, quantum/window lengths, pointer alignment and
required capabilities. Steps accept 1..65536 bytes; chunk copies accept at most
65536 caller-funded bytes. No result span or control port survives a call.
Request output bytes are usable only after a successful return, since the final
original authority check can reject a copy already performed.

The owner copies each read's memory descriptor into stable heap metadata before
using its self-relative standard allocator. The client similarly allocates a
stable, independently rooted operation bridge. Neither uses the WAL's open
bridge as its lifetime owner. WAL close retires provider snapshots/results and
joins in-flight native steps before destroying storage; small read metadata may
remain until explicit destroy. Destroy releases native metadata before its
memory descriptor and the client bridge. No new execution grant is required.
The caller allocator context must outlive the read handle. Calls on one client
read handle must be serialized; concurrent WAL close is supported. Cleanup does
not promise safe concurrent raw-handle reuse after caller destruction.

Original per-call authority is relayed across the C status boundary, including
custom errors and a provider that ignores rejection and returns a populated
owner. Returned failure owners are retired. Terminal failures retire the
provider operation immediately and remain sticky in the client; invalid caller
arguments and transient backpressure remain retryable. Malformed ready/chunk
flags, reserved fields, overlarge lengths and missing forward progress fail
closed and retire the operation. The shared status registry adds distinct
`Backpressured` and `TransactionClosed` identities in the pooling range, keeping
all existing values unchanged; its lossless/unique inverse audit passes.

Verification: 190 distinct custom functional cases (121 public, 69 private),
16 cross-archive service cases and nine compiled-client cases total 215.
All 89 build steps pass, including production SQL and both node startup
profiles. Eleven changed Zig files pass formatting/AST checks; all 5116 source
inputs remain unchanged through the native run. The three new owner cases cover
16 pending reads, refusal and slot cleanup, fresh step scopes, large checksum
and chunk progress, root retirement across WAL close, every allocation failure,
every begin/step/chunk authority rejection and validation before pointer use.
Two new separately compiled-client cases cover malformed replies and ignored
original rejections with exactly-once retirement. All 4408 current public package
entries and 25 fetched-consumer compilation steps pass. Receipts are under
`receipts/2026-10-10/child-outbox-capture-retirement-cumulative-integration/`.

Primary still uses its existing whole-record recovery path; it does not yet
retain these owned operations between attempts. The next integration needs a
funded bounded inner-record CRC/header/equality cursor, fresh expected-payload
borrows, stable intent/history identity, reset/close retirement and the existing
conditional absence-append guard. Cold snapshot metadata, index loading/search,
allocation CPU cost, prefix codecs and arbitrary provider physical work remain
outside the complete byte-bound claim. Complete physical roots, nested-budget
and opaque/backing allocator audits, precommit completion reservations, within-DB
outbox fairness, metadata HA adapters, default HA fault/restart/overload/shutdown
qualification, activation batching and all remaining original design scope are
still required. Full Linux qualification remains explicitly waived. This phase
is uncommitted on the single canonical branch/worktree/PR.

## Owned byte recovery and prefix block continuations (2026-10-19)

Modern v2 outbox recovery now retains a funded owned WAL read between attempts.
Each DB initializes its recovery cursor with its stable runtime allocator. The
cursor owns a provider continuation and a C-callable, grant-free destructor;
private native ABI 15 rejects the preceding scalar-only cursor layout. The
provider owns a 4096-byte heap window and scalar inner-record checksum/equality
state. Neither expected payload nor execution control is retained. Each Primary matching attempt
performs at most 16 storage/codec work units and returns pending with publication
fences released. Candidate reads are not restarted merely to yield. Existing
immutable intent, namespace, Primary identity, WAL origin/owner/destructive
revision, growing-tail and final conditional absence-append guards still apply.

The inner replication cursor consumes each input window once, validates header
and payload checksums, exact envelope length, kind/codec and WAL continuity, and
compares against a fresh expected-payload borrow. It preserves corruption-error
precedence and validates mismatching records before advancing. A terminal
failure retires the owned read. Provider backpressure retires a potentially
sticky failed lower handle and retries the current record while preserving
completed-record progress. Original authority rejection takes precedence.

A changed intent/history releases the previous continuation. Successful immutable
key deletion retires its matching cursor after releasing apply; an authoritative
empty outbox collection resets a leftover cursor. DB shutdown/failed-open cleanup
joins publication jobs and resets the cursor before destroying storage or its
allocator. A closed/reopened Primary can safely retire detached old read metadata
because the read root belongs to the DB/caller rather than the Primary's open
bridge. Synchronous callers sharing one cursor remain serialized by its mutex.

Default truncate/reopen exposed prefix-compressed table blocks, so owned block
reads now support `.prefix` and `.prefix_snappy` as well as raw and Snappy.
Prefix expansion resumes fixed header/restart/entry parsing, namespace/suffix/
value copying and shared-key copying with scalar positions, spending no more
than the requested input/output quantum per decoder call. Prefix variants own a
whole encoded buffer and optional Snappy intermediate through the caller root;
the working-set coordinator includes both in its bound. Encoded reads and CRC
advance in bounded ranges before decoding. Snappy's intermediate length is
validated against `2 * logical_length + 32` before allocation. Allocations use
separate steps; ready, failed and abandoned owners retire all intermediate
buffers and source leases without a fresh grant. Raw/Snappy's existing streamed
input paths remain in use. Prefix variants therefore have bounded continuation
work while retaining a funded whole encoded buffer; this is a distinct memory
profile from streamed raw/Snappy input.

The focused native run and separately compiled control-side Primary/physical-WAL
fixture pass. The physical fixture compiles in test mode, exports the actual C
WAL API, and has no runner entrypoint. Shared native C/Metal inputs remain at the
link boundary using the existing split-native-sources helper. It exercises
large-record progress, fresh scopes/expected borrows, close/reopen, original
custom rejection and independent root retirement. Native cases additionally
cover default prefix truncate/tail ABA, every allocation failure, partial inner
checksum rejection, transient sticky backpressure, checksum/error precedence,
shared keys and corrupt restarts. Block ownership tests cover all four formats
with leased/direct physical sources, source errors, original rejection,
abandonment, fresh finite scheduler executions and control work between steps.
Actual DB/publication tests cover mandatory standby acknowledgement, every
shutdown checkpoint across multiple byte attempts, intent deletion, empty
collection reconciliation and closing with a pending read.

Final `native-primary-byte-v2` verification: 220 distinct custom cases
(124 public WAL/runtime, 72 private WAL/HA, 12 public publication, seven private
publication and five actual DB cases), plus 16 current archive-service, nine
borrowed-client and one compiled-Primary cases total 246. All 125 build steps
pass, including production SQL and both node startup profiles. All 5119 source
inputs remain unchanged; 14 changed Zig files pass AST/format checks. Every one
of the 4408 public package entries matches current source, and the fetched
consumer passes 25 compilation steps. Current archive/borrowed-client programs
were compiled from the preceding eight-target invocation and reused by the final
run's confirmed cached compilation; all three current programs were executed
directly after the final run. Their source, binary and log hashes are recorded.
Receipts are under
`receipts/2026-10-10/child-outbox-capture-retirement-cumulative-integration/`.

This completes the modern v2 read/CRC/equality continuation integration, not the
full design. DB intent enumeration, whole outbox-value materialization and AHO1 CRC, cold
metadata capture/index loading and searches, allocator CPU cost, arbitrary provider physical work, complete root/FD/lock funding and backing/
opaque/nested-budget audits still need bounded contracts and evidence. Precommit
mandatory completion reservations, within-DB key-selection fairness under lower
key insertion churn, metadata HA legacy adapters, default HA fault/restart/
overload/shutdown qualification, activation batching and the remaining original
scheduler/transport/operator/SDK/Cloud/adaptive scope remain unfinished. Legacy
matching and ordinary synchronous prefix/get paths retain their previous APIs.
The opaque lower-storage error catalog also needs an audit:
`CorruptInput`, `InvalidTableFile` and `TableBlockChecksumMismatch` currently map
to generic `StorageKernelFailure`; those paths fail closed but lose their exact
codec diagnostic identity across the C owner boundary. Caller authority errors
and inner-record decoding retain their originating identities.
Full Linux qualification remains explicitly waived. Work remains uncommitted on
the single canonical worktree/branch/PR.


## Owned WAL lower codec error identities (2026-10-19)

`native-codec-errors-v2` adds three pooling-range C failure identities:
`CorruptInput` (`0x1000A`), `InvalidTableFile` (`0x1000B`) and
`TableBlockChecksumMismatch` (`0x1000C`). These expected storage/codec failures
previously reached the opaque client as `StorageKernelFailure`. The owner now
preserves each distinct cause through its existing status conversion, and the
client retires failed provider resources immediately while retaining the exact
sticky cause. Subsequent reads and destruction require no fresh authority.
Original caller rejection remains authoritative. Unregistered provider defects
continue to use the existing internal-failure sentinel.

The new test exercises a real native WAL owner with an erased failing lookup,
the C owned-read exports and the opaque client. Every cause retires exactly
once, leaves the registry reusable, frees all caller-root allocations, and
performs no WAL append. Cancellation before redispatch still returns the
original caller error. The full status inverse/uniqueness audit passes. Exact
source comparison with the preceding package proves that all existing numeric
statuses and mappings remain unchanged; production owned-read implementations
and native layouts are unchanged. The private owner change adds the test only.

Verification: 197 distinct native WAL/runtime/HA cases (124 public, 73 private),
16 archive-service, nine borrowed-client and one separately compiled Primary
cases total 223. All 105 selected build steps pass, including production SQL and
both node startup profiles. All 5119 source inputs remain unchanged; the three
changed Zig files pass AST and formatting checks. All 4408 public package
entries match current source and the fetched consumer passes 25 compilation
steps. The current archive and borrowed-client compilations are cached from the
preceding invocation; all three current programs are executed directly with
recorded binary/log hashes. The preceding 246-case DB/publication integration
is retained as historical evidence. Those cases were not repeated for additive
error mappings with unchanged layouts and operation implementations.

This closes the three identified lower-codec diagnostic gaps. Broader provider
error audits, owned/funded intent enumeration/value/AHO1 CRC capture, complete
physical metadata/index/search/allocator/provider bounds and root accounting,
precommit completion reservations before irreversible writes, within-DB key
fairness, metadata HA legacy adapters, default HA fault/restart/overload/shutdown
qualification, activation batching and remaining original design scope are
unfinished. Native ABI 15, C owner ABI 100, private request ABI 4, services offer
ABI 3, memory ABI 1 and durable formats are unchanged. Full Linux qualification
remains explicitly waived. Changes remain uncommitted on the single canonical
worktree/branch/PR.


## Known-key intent capture ownership (2026-10-19)

The `native-owned-intent-v2` receipt verifies optional single-namespace owned
lookup at native ABI 16. C owner ABI 100, private request ABI 4 and offer ABI 3
are unchanged. This is a native capability foundation; production DB recovery
and its compiled C storage operations do not yet use it.

| Resource | Owner and release boundary |
| --- | --- |
| Single-namespace operation box, copied key/namespace and value bytes | Independent caller allocator; owning checked dispatch; failed start or step retires the owner without a fresh grant |
| Native lookup snapshot and immutable source leases | Existing LSM owned namespace reader; retire before backend teardown |
| Portable-import reader exclusion | DocStore owned internal reader; held through lookup, released on ready or every failure; pending owner must retire before DocStore close |
| Detached intent value | Caller allocator; survives DocStore/backend close; ownership transfers exactly once and payload views cannot outlive it |
| AHO1 verification state | Inline scalar header/trailer/CRC/offset state; fresh immutable bytes and control borrowed per step; no parent store reference after lookup detachment |

Each AHO1 work unit inspects at most its 1..65536 byte quantum. This does not
bound snapshot metadata capture, cold indexes/search, allocations or opaque
provider work. The original authority remains authoritative across the erased
Store boundary, and missing capabilities reject rather than fall back to a full
get. Native tests cover cancellation after partial copying/checksum progress,
allocation refusal, portable-import exclusion, corrupt/missing values and source
close before checksum completion. The 229 focused cases, 105 native build steps
and 25 fetched consumer steps pass. Complete physical funding, production intent
selection/capture integration, DB teardown joins, legacy version guards,
precommit mandatory completion capacity and the broader original design remain
open. Full Linux qualification remains explicitly waived.


## DB-owned background intent capture (2026-10-19)

The `native-production-intent-v2` receipt verifies the actual native background
polling path at native ABI 17. The new DB local-state field owns either one
pending intent reader or one verified value. Both use the stable DB runtime
allocator, independently of Primary's owned WAL matching state. The read holds
portable-import exclusion only until its source bytes detach; verified bytes
remain funded and owned while completion is pending. No attempt control, caller
payload view or task-local allocator survives a poll.

Capture's mutex spans the synchronous publication borrow. Recovery unlocks it
before clearing; clear unlocks apply before retiring matching Primary and capture
owners. Empty reconciliation retires both; joined DB close resets capture
before core/backend/allocator teardown. Failed lookup/checksum/publication work
releases capture state without a fresh grant, while pending completion preserves
it. A fixed-size resume key owns no resource lease. Source, namespace, root
generation and immutable key presence are validated before publication, in
addition to Primary's existing history and authority guards.

At most sixteen capture/checksum units with quantum 4096 run per background
poll. This is not a total physical IO or CPU bound: encoded key selection can
still decode full persisted blocks, and cold snapshot/index/search work and
allocator CPU are synchronous. Blocking foreground collection and legacy
singletons keep their existing full-page path. The new owner neither reserves
mandatory completion capacity before commit nor caps all durable obligations.
Full physical accounting, within-DB fairness and remaining default HA evidence
remain open. All 257 focused cases, eight selected native targets and the
25-step fetched consumer pass; full Linux qualification remains waived.


## Import-reader resumption before apply (2026-10-19; v3)

`native-production-intent-v3` verifies 258 functional cases, including eight
actual DB cases, at native ABI 17. An existing immutable intent capture may
advance before acquiring apply. This lets its source/import reader retire even
when a publisher holds apply while awaiting readers. The attempt still owns the
flush fence and uses a try-only capture mutex. Pre- and post-apply work share
sixteen units of quantum 4096; no attempt borrow survives a step.

No new selection or publication runs without apply and current key/namespace/
root-generation validation. Once lookup bytes detach, remaining checksum work
has no parent-store lease. Blocking empty reconciliation also retires an earlier
polling capture, in addition to polling empty scans, public clear and joined DB
close. A real DB test holds apply plus import exclusion while polling to reader
retirement, then verifies completion and the blocking-empty cleanup scenario.

This establishes progress across those reader/apply fences. Complete scheduler
capacity graphs, legacy no-control-port contention policy, physical key/index/
allocator/provider bounds, foreground/legacy continuations, full funding,
mandatory precommit completion capacity and remaining original qualification
remain open. Full Linux qualification remains waived.


## Legacy publication reader-contention refusal (2026-10-19)

The explicit reader check introduced in this receipt duplicated lower-level
behavior: null-control draining already returns `Backpressured`. That duplicate
has been removed. The regression records existing refusal semantics and does
not demonstrate a newly fixed deadlock. ABI 18 key-selection changes are verified separately in
`native-key-selection-v2`.

`native-legacy-import-v1` verifies the public prepared portable-adoption path
without execution-wait controls. It tries apply, checks the pristine target,
claims publication exclusion and rejects outstanding readers with backpressure.
The helper cancels its own exclusion bit and releases apply on refusal; it never
withdraws another publisher's bit. No reader drain waits while this caller may
hold the only ordinary execution slot. Caller-owned CPU and raw-policy leases
remain the caller's responsibility. When no reader remains, exclusion makes the
final operational/readiness check nonwaiting and admission transfers the fences
to the existing publication scope.

An actual DB regression holds the scheduler's sole ordinary slot and independent
health capacity. Rejection preserves the admitted reader and avoids publication;
finishing the caller's CPU permits reader resumption, and a retry reaches the
callback. Deliberate callback failure releases the transferred fences with no
generation/namespace change. Existing controlled reader-drain and read-exclusion
tests remain green. Eleven actual DB cases, 72 native build steps and 25 fetched
consumer steps pass. Native ABI 17/C100/private4/memory1/offer3 are unchanged;
the controlled admission branch is byte-identical.

The complete legacy resource graph, metadata/pristine-probe and publication-claim
CPU/IO bounds, physical funding, precommit capacity, fairness and the rest of the
original design remain open. This receipt does not replace the preceding named
258-case background-capture evidence or the waived full Linux qualification.


## Native key-selection ownership (2026-10-19)

`native-key-selection-v2` retains one caller-funded snapshot across legacy probes,
modern intent-key selection and point-value handoff. Its portable-import reader
transfers to the point reader without reopening a snapshot. Empty/legacy scalar
results and final presence validation retire the reader before reacquiring apply.
Pending lookup/cursor state can resume before apply; each new poll supplies fresh
controls. Original cancellation/errors, allocation refusal, clear and joined close
retire owners. Copying seek inputs before cleanup permits borrowed result reuse.

Discovery, row probing, block decoding, key/value copying and checksum work share
sixteen units at quantum 4096 per poll. Native ABI 18 covers the new capability
and retained state; public C100/private4/memory1/offer3 remain unchanged. Import
publication revisions protect same-identity imports, mutation revisions protect
stale empty results and cached presence, and progressing validation is not reset
by ordinary writes. These checks do not establish a complete physical byte/CPU
bound for cold snapshot/index metadata, search comparisons or allocation.

The complete physical resource graph, foreground/legacy continuations, mandatory
precommit capacity, durable obligations, fairness and remaining original scope
are still open. Full Linux qualification remains explicitly waived.


## Cold-index ownership and bounded metadata steps (2026-10-19)

`native-cold-index-v2` gives both owned namespace readers a caller-funded index
continuation. One source spans footer and metadata reads; each read and metadata
CRC update consumes at most quantum. Fixed footer validation uses 48 bytes.
Metadata parsing and offset validation resume between finite steps; variable
bound comparisons use chunks. One owned raw buffer backs all key/filter spans,
with separate funded offset and block arrays. Partial retirement frees those
allocations without per-block cleanup loops. Completed indices are independent
of the backend; pending owners join retirement before snapshot/backend close.

Progressing capture work requeues through the existing bounded job queue. A
nonblocking progress probe preserves generation observation even on lock
contention, and requeues only when work advanced and remains runnable. Ack/fence
waits, unchanged progress and queue refusal keep delayed retry; close prevents
resubmission. The original five-second resident DB recovery test is unchanged.

The default native background recovery budget remains sixteen units at quantum
4096, shared across discovery, index loading, block/value capture, checksum and
presence validation. Native ABI 20 protects the larger continuation states and retry progress ports;
C100/private4/memory1/offer3 and durable formats remain stable. Source replacement,
mid-read provider/original refusal, allocation failure and malformed metadata
have focused evidence. The complete snapshot/search/allocator/provider CPU and
physical memory/resource graphs remain unqualified; retained raw metadata,
including ignored legacy sections, remains charged to the caller. Foreground/
legacy continuations, mandatory precommit capacity, full fairness and remaining
original scope are open. Full Linux qualification remains explicitly waived.


## Provider/WAL progress observation (2026-10-19)

`native-wal-progress-v2` gives recovery cursors an atomic progress generation and
mutex-protected runnable bit. Primary marks pending CPU continuations and retires
readiness on waits, errors, completion and destruction. Probes never wait, retain
no grant/control and cannot authorize publication. DB observes capture and
provider independently without nested locks. Native ABI 21 protects the larger
cursor and progress-return layouts; C100/private4/memory1/offer3 remain stable.

Queue resubmission requires a changed token and remaining runnable work from
that same source. Stale readiness from another stage cannot remove an ACK/fence
wait. Queue refusal and close retain their existing barriers. The resident real
DB/Primary fixture verifies multi-megabyte matching within five seconds, exact
intent retention during missing ACK, reader retirement, and post-ACK deletion
without duplicate append. This proves the named continuation path, not complete
resource/fairness/CPU or default HA qualification. Full original scope and the
explicit full Linux qualification waiver remain in force.


## Publication dispatch uses prepaid recovery resources (2026-10-19)

`native-publication-reserve-v1` routes cooperative publication steps through the
intrusive per-owner slot allocated at registration and runtime recovery workers
allocated at construction. It avoids ordinary job-header/future allocation after
durable publication debt exists. One current step and its bounded successor use
the existing driver and close barriers. Native ABI 22 protects new owner/port
fields; C100/private4/memory1/offer3 remain stable.

Each job's class fixes its callback. A finite DB step refuses legacy fallback
before full-page/AHO1 materialization and keeps noncooperative publishers on
ordinary capacity. Legacy handoff cannot use the reserved queue to evade ordinary
allocation pressure. Existing progress tokens, ACK/fence waiting, worker joins
and durable handoff remain in force. Parent-exhaustion and real DB refusal tests
cover this route. Snapshot/provider/allocator CPU, full completion/backlog/funding
and fairness guarantees remain incomplete; this is not proof of the full design.
Full Linux qualification remains explicitly waived.


### Durable ordinary-publication backlog admission (native ABI 23)

Ordinary HA publication now has a per-store durable limit of 4,096 obligations
and 256 MiB of encoded obligation keys and values. DocStore captures changes in
constant space, checks the final atomic batch before primary commit, and writes
an HPB1 aggregate and fixed-size hashed-key size sidecars in that same commit.
Sidecar overhead is bounded separately by the record ceiling. Replacing a record
or deleting and inserting in one batch reuses its credit. A refused batch cannot
commit its source mutations. Abort and an uncertain backend outcome cannot retire
a credit separately from its obligation.

A backend-owned immediate gate prevents overlapping native namespace views from
losing aggregate updates; unrelated writes remain concurrent. It returns
Backpressured instead of parking a worker. Custom single-writer backends use an
owner-local gate. Older roots with outboxes but no aggregate refuse growth until
recovery drains the existing obligations; four keys-only seeks establish the
empty boundary without scanning legacy values. Portable metadata includes neither
the aggregate nor the size sidecars. Native root copies retain their own physical
records. Direct raw backend mutations must preserve the accounting contract.

The six new regressions cover the real count ceiling and atomic source refusal,
byte and arithmetic boundaries, legacy draining, abort and durable reopen,
overlapping namespace views, and backend errors both before and after actual
commit. Uncertain creation and deletion fixtures forbid primary outbox value
reads: cleanup uses the eight-byte size sidecar. The first validation build was
stopped for a confirmed full-value reread regression; its owned-process identity,
termination, and absence are recorded in native-publication-backlog-v1-canceled.json.
The v2 production suites and v3 admission suite passed 290 functional cases.
V2 completed 129/135 native steps, with only the new boundary test failing to
compile because of an implicit integer type. V3 corrects that expression and
passes 60/60 focused native steps. Reversing exactly one expression inside
that previously unselected test reconstructs the v2 source byte for byte; no
production code differs. The v3 build's 5,120 source inputs are exactly current.
The current archive's 4,409 public files match source, and its fetched consumer
passes 25/25 semantic checks. See
`native-publication-backlog-v3-verified.json` and its post-GC receipt in this receipt directory.

This bounds ordinary HA publication debt, not every durable obligation family.
Transaction-specific HA outboxes need publication credit reserved during prepare;
adding refusal only at resolution would be after an irrevocable commit decision.
They remain outside this limit. Node-wide debt policy, configuration and age
metrics, minimum funded completion memory/CPU/I/O/dependencies, bounded cleanup
metadata I/O and index CPU, mixed-version/downgrade accounting, and the full
physical resource graph remain open. The earlier reserved-dispatch proof remains
a dispatch proof, not proof of real database completion under exhausted memory.
The original scheduler design remains incomplete; full Linux qualification stays
waived, and Linux builds must use the pinned Nix glibc Zig toolchain.


### Prepaid local publication envelopes (native ABI 24)

Mandatory batch and replay publication now acquire the native publisher's owned
envelope before the local primary commit. The provider allocates its state and
exact record buffer, copies the payload, and computes its checksum in 4,096-byte
construction quanta under the original lifetime. A present provider's preparation
error refuses the local commit; it cannot fall back to allocating publication.
After commit, sealing writes only the fixed 100-byte header with the publication
LSN. It neither allocates the envelope nor copies or checksums the payload again.
Preparing and abandoning a token consumes no LSN. The token owns its payload,
retains the allocating allocator, permits one publication attempt, and rejects a
changed primary identity. It does not retain the construction Port or cancellation.
Existing log serialization, failure telemetry, deferred acknowledgements and
post-append authority checks remain in the commit owner.

The original lifetime and signed row-policy expiry are checked again after this
preparation, immediately before the local decision. A real DB fixture expires
its lease inside publisher preparation and verifies source-row refusal, no HA
WAL advance, and retirement of the unused envelope. The same fixture proves
that preparation OOM refuses the source, then proves that a successful source
commit can publish its prepaid native envelope while Primary.alloc refuses new
allocations. Its WAL/backend allocators remain funded: this is an envelope-funding
proof, not an exhausted-parent completion proof. Other regressions verify exact
wire bytes, independent payload ownership, every construction refusal quantum,
no LSN on abandonment, stale-identity/single-use rejection, completion after
construction cancellation, and original rejection/cleanup through a foreign
named callback even when a provider ignores rejection or returns a later error.

The complete current source passed 296 functional cases and 135/135
native build steps, including the six durable-backlog regressions. All 5,120
native source inputs are exactly current, with no source-delta exceptions. The
current exported package has 4,409 matching public source files and passes 25/25
fetched-consumer semantic checks. See `native-publication-envelope-v3-verified.json` and its post-GC
receipt under the cumulative integration receipts.

This funds a known local publication envelope. It does not yet prepay WAL/backend
append resources, recovery working memory or every CPU/I/O dependency. Schema,
row-policy and other publication families still need the new preparation path.
Transaction publication credit must be reserved during prepare, before a global
commit decision; local preparation during resolution does not establish that
property. Legacy providers without the new capability retain their existing
publication contract. Whole-node debt policy, the physical resource graph,
bounded end-to-end recovery CPU, default HA qualification and all remaining
requirements of the original scheduler design stay open. Full Linux qualification
remains waived; Linux builds still require the pinned Nix glibc Zig toolchain.


### Metadata and maintenance publication funding (native ABI 25)

Typed and JSON schema setters now prepare mandatory native publication before
constructing and committing the new schema. Row-policy publication prepares its
record before changing the runtime admission gate or committing its preparing
marker. Both policy commit branches consume that same prepared token. The shared
PrimaryReplicationEffect owner also prepares its native frame before committing
physical maintenance effects and their outbox, and retains it through the local
publication attempt. These paths use the same fixed-header publication and owned
payload/checksum contract introduced in ABI 24; wire bytes remain unchanged.

A row-policy outbox is mandatory even when HA acknowledgement policy is async.
Its append failure now propagates through strict metadata publication, reports
DurabilityOutcomeUnknown and retains the exact durable intent. It cannot be
mistaken for optional async delivery and cleared without an append. Legacy
providers without prepare use the same strict fallback for policy delivery;
ordinary optional async schema publication retains its previous failure policy.

The real native DB fixture covers preparation OOM and prepaid publication with
Primary.alloc denied for both schema setters. It verifies the old durable/runtime
schema and LSN on refusal, then observes the new schema during publication. The
policy integration fixture proves native preparation OOM leaves the policy gate
disabled and pending marker absent, then injects an async postcommit append error
and verifies a row-policy intent remains until native recovery publishes it. The
existing replica replay, exact Raft receipt and restart checks still run. The
graph-TTL integration fixture injects frame-preparation OOM before retirement and
verifies the source graph and HA LSN remain unchanged, then exercises the existing
postcommit acknowledgement failure and idempotent recovery. A separate strict
async test covers both legacy and prepared failures and caller-owned retirement.

Qualification covers 299 functional cases: 271 regression cases from the
142/144-step native run, 2 integration cases from the focused 54/54-step run,
and 26 directly executed provider/client cases. The broad run's sole failed
target was metadata integration. Subsequent fault execution exposed cleanup of
an already committed transaction; both policy commit paths now retire their
abort handlers before HA publication. Exact source reversal bounds the
intervening production delta to those transaction cleanup guards, and the
fixture delta to its injected telemetry callback and successor failure checks.
The 271 regression cases and 26 provider/client cases use ReleaseSafe; the two
focused integration cases use Debug and cover both committed-transaction error
paths. All three current semantic preflights use ReleaseSafe. All 5,120 focused-run
source inputs are exactly current. The current archive's
4,409 public files match source, and its fetched consumer passes 25/25 semantic
checks. See `native-metadata-preparation-debug-v4-verified.json` and its post-GC receipt under the cumulative
integration receipts.

This extends local envelope funding; it does not reserve transaction publication
credit before a global decision, or fund every WAL/backend/recovery working set,
CPU quantum or I/O dependency. Other lifecycle/import paths that publish through
durable recovery still require that resource work. Whole-node backlog policy,
remaining physical accounting and the original scheduler requirements remain
open. Full Linux qualification remains waived; Linux builds must use pinned Nix
glibc Zig. The full design is not complete.


### Transaction batch publication credits at PREPARE (native ABI 26)

A transaction with write intents now reserves its future HA batch outbox in the
same backend batch as its intents and prepared vote. Its durable credit uses the
existing per-DocStore publication pool (4,096 records and 256 MiB), alongside
ordinary outboxes. It reserves one record and an encoded payload bound of six
times the intent-admission bytes plus 64 KiB of fixed envelope allowance. JSON
strings expand at most six bytes per byte; binary metadata byte arrays expand
at most four. The existing admission cost includes per-intent overhead, key
copies, logical values and prepared rows. Tests exercise actual native encoders
with control characters, binary metadata, maximal fixed fields and overflow.
This grant bounds durable batch debt; it is not preallocated encoder or WAL RAM.

The credit is appended before new intents and votes without moving the complete
mutation array. A failed prepare atomically rolls back its credit, vote and
intents. Replacement prepares reuse their existing record. Resolution retains
the credit when it materializes a batch outbox and never requests another debt
grant; delivery acknowledges and deletes the outbox and credit atomically.
Abort and resolution without a batch outbox release the credit. Already-resolved
retries keep their existing delivery ownership and do not need a cold outbox
probe merely to decide whether this resolution creates an obligation.

A durable HTC1 marker fences migration. Before the first tracked prepare, three
keys-only seeks must prove that legacy intents, legacy batch outboxes and stray
credit records are absent. Legacy globally committed transactions can still
materialize and drain their old batch outboxes without obtaining a new grant.
New transaction prepares remain refused until that old debt drains. Credit
updates share the existing native backend publication gate and the same atomic
aggregate/size-sidecar mechanism as ordinary debt. Raw backend mutation can
bypass DocStore admission and is outside this guarantee.

The broader transaction sweep exposed insufficient streaming decoder scratch
funding. With a retained arena and a backing allocator unable to resize in place,
a valid 64 KiB read can require a 196,670-byte backing allocation while the old
logical reservation allowed only 131,136 bytes. The standalone pinned-toolchain
reproducer confirms that refusal. Streaming block readers now fund backing growth
for their two scratch allocations (path and input window), including retained
nodes, before decoding. The bound follows the pinned arena's 1.5x node growth;
output and prefix buffers retain their separate working-space accounting. The
new native regression checks non-resizing backing, allocation failures, physical
charges and retirement. The near-16-MiB prepared transaction can again resolve
across LSM restart at retained-effects quota. The older coalescing test now uses
ReadVersion.preparing and a suspended-lifetime capability, preserving its pinned
layout and expiry assertions rather than referring to removed builder mutexes.

Qualification covers 142 distinct functional cases, with 119
custom-runner executions (the focused three credit cases also run in the broader
transaction suite) and 26 directly executed provider/client cases. Native Debug
build steps passed 161/161, with all 5,120 inputs exactly current. Additional
ReleaseSafe module analysis covers transaction and decoder test bodies using the
current generated build options. The fetched current public archive matches
4,409 source files and passes 25/25 independent ReleaseSafe consumer checks.
The initial active caches disappeared during qualification; fresh caches and
hash-matching pinned dependencies were restored and the final run was rebuilt.
See `native-transaction-publication-credit-v3-verified.json` and its post-GC receipt.

This completes durable batch credit for these prepare/resolve paths. Derived
transaction replay credits, whole-node debt policy and metrics, all minimum
working RAM/CPU/I/O/dependency bundles, import/downgrade accounting, the remaining
physical graph and default HA qualification remain open. The two-allocation arena
bound must be revisited if streaming scratch allocations or the pinned allocator
change. Full Linux qualification remains waived; Linux builds must use pinned
Nix glibc Zig. The original scheduler design and overall goal remain incomplete.


### Prepared WAL encoding before local commit (native ABI 27)

Mandatory prepared HA publication now owns both its replication frame and its
outer WAL encoding before the local source decision. The native WAL grant
captures its allocator, stable WAL identity, immutable tail and checksum state.
Construction copies and checksums payloads in 4 KiB original-control quanta;
refusal or allocation failure retires all partially acquired ownership. The
compiled client restores the original refusal even if a provider ignores it,
and destroys any handle returned alongside that refusal. No request control is
retained by the completed grant. The stable WAL and allocator descriptor must
outlive the grant, and publication must join before destruction.

Publication patches a bounded prefix and uses a precomputed IEEE CRC shift
matrix to combine its checksum with the saved tail checksum. It performs no
payload copy, payload checksum scan or WAL encoding allocation. It enters the
same group-commit coordinator as ordinary appends, preserving the wire format,
LSN assignment and durability completion path. Preparation and abandonment spend
no LSN; each token allows one publication attempt. The HA and WAL buffers remain
separately owned and charged; this change does not consolidate them. Storage
owner ABI 100 gains additive prepare/publish/destroy entry points; native ABI
27 fences the updated runtime contract.

Regression coverage compares concatenated CRCs across empty, boundary and large
partitions, refuses each construction checkpoint and allocation, verifies
immutable ownership and single use, and mixes a prepared large entry with an
ordinary entry in one real backend transaction. Native publication succeeds
with the WAL encoding allocator denied while the ordinary large append fails.
The real compiled owner checks allocation cleanup, abandoned grants and
interleaved LSN assignment. The compiled client checks ignored original refusal,
returned-handle retirement and detachment from the original control after prep.
Both compiled boundary tests are included in the normal build target filters.

Qualification: 293 distinct functional cases, including
28 directly executed standard provider/client cases, with native Debug
steps 144/144 and all 5,120 inputs exactly current.
The exported current archive matches 4,409 public files and passes 25/25
independent ReleaseSafe consumer checks. The first native run omitted the two
new client cases because of narrow existing filters; the final run corrects
those filters and includes both. The compiled Primary fixture also gained its
existing controlled-read export, which the new payload readback requires. See `native-wal-prepaid-encoding-v3-verified.json` and its post-GC
receipt for exact input hashes and test evidence.

This funds encoding memory and payload CPU before local commit. Backend
transaction/LSM working memory, I/O admission and other completion dependencies
remain separately acquired. It does not complete the minimum resource bundle,
transaction derived-replay credit at PREPARE, other write-family migration,
whole-node debt policy, physical resource accounting or default HA qualification.
Replay credit needs a stable bound for generated artifacts and graph-neighbor
fanout; the retained direct-vector bound alone is insufficient. Full Linux
qualification remains waived; Linux builds use pinned Nix glibc Zig. The original
scheduler design and overall goal remain incomplete.


### Exact, controlled replay-envelope encoding

CJ2 replay records now have an allocation-free exact wire-size calculation that
checks list and key lengths against the format's u32 limits and checks aggregate
size arithmetic. Encoding acquires one exact output buffer before copying;
legacy geometric growth and its extra live backing allocations are removed.
Original controls are checked during size traversal, every 64 metadata entries,
at 4 KiB copy boundaries, after allocation and before returning the buffer.
An original refusal takes precedence over a later allocator failure, and any
partial buffer is freed. Both thin and materialized mutation replay encoders use
the same original control as prepared HA publication. Physical apply fences
retain their runnable admission; this adds bounded lifetime checks, not runnable
suspension while holding those fences. The runtime ABI remains 27 and CJ2 bytes
remain unchanged.

The new normal target `docstore-replay-encoding-test` includes three regressions:
all field masks and copy boundaries match the former encoder in an exactly sized
fixed buffer; every construction checkpoint and allocation failure unwinds; and
oversized wire lengths fail before allocation or key access. Existing replay,
transaction transform/full-text visibility, graph materialization, publication,
metadata and transaction HA/partial-ACK cases also run. Qualification passed
76 distinct functional cases and 99/99 native Debug build steps,
with all 5,120 source/schema inputs exactly current. The
current public archive matches 4,409 files and passes 25/25 independent
ReleaseSafe consumer checks. See `native-replay-exact-encoding-v1-verified.json` and its post-GC receipt.

This provides an authoritative size for an assembled immutable replay record.
PREPARE still needs a stable bound on its future artifact-key set, including
background artifact growth and graph endpoint notifications. Transaction replay
outboxes remain outside the batch-credit guarantee; adding fresh admission at
resolution would not fund an already globally committed obligation. Key-set
assembly, backend completion RAM/CPU/I/O bundles, whole-node policy, the remaining
physical resource graph and full original scheduler scope remain incomplete.
Full Linux qualification remains waived; Linux builds use pinned Nix glibc Zig.

### Physical artifact replay inventory (native ABI 28)

DocStore now maintains per-document physical derived-key counts and key bytes for
the families classified by artifact_footprint. Ordinary transactions, batches and
append writers coalesce the touched keys. Commit compares actual staged key
presence with committed presence through native metadata probes, then stages exact
counter deltas in the same physical transaction as the artifacts. Artifact values
are not copied to compute these facts. Counter metadata is staged after the bulk
append phase, avoiding a counter write that drains the append arena per artifact.
The shared native publication mutation gate serializes inventory/debt RMW across
namespace views and releases on commit or abort. Footprint control writes share
that gate; ignored allocation/storage failures poison inventory publication.
Missing deletes and supported bulk fallback retain their prior behavior.

A checksummed local marker binds the inventory generation to artifact-footprint
epochs. A certified empty root starts complete, where absent counters mean zero.
Existing roots start partial, where absent counters mean unknown. Older writers
and import invalidation advance footprint epochs without refreshing the marker,
invalidating the totals. Reinitialization uses a new generation so old counters
cannot revive. Reserved inventory facts cannot be overwritten by ordinary writes.
Clear preserves the local generation fence and retires quarantined old counters;
logical and portable staging imports exclude donor inventory metadata.

Reconciliation walks one document's physical keys without artifact values, in
pages of at most 128 keys or a 64 KiB key-byte target and a 2 ms time target.
The first key can exceed the byte target, up to the existing 1 MiB transfer-key
ceiling. Underlying physical opens/searches remain synchronous. Cursor copy and
checksum work check original controls every 4 KiB. Document revision, generation
and progress CAS fence publication. Same-document mutation restarts its scan;
unrelated documents retain progress. No cursor or read snapshot survives a page.
Native ABI 28 fences the updated capture layout; other owner/request/offer ABIs
retain their existing versions.

Seven new regressions cover coalescing and abandonment through all writer forms,
legacy page restart and unrelated-document progress, two-view gate contention,
ignored allocation failure, actual WAL reopen/old-writer/import epoch rejection,
clear/reuse, checksums, arithmetic, large cursors and owned-key allocation cleanup.
A late append Unsupported regression first reproduced a phantom key count. The
corrected observer compares actual committed/staged membership rather than the
requested operation, preserving both ignored rejection and successful fallback.
Existing logical graph snapshot restore and portable graph restore also run.
Qualification passed 85 distinct functional cases, including
28 direct provider/client cases, and 152/152 native Debug steps.
The corrected public archive matches 4,410 source files and passes 25/25
independent ReleaseSafe consumer checks. The first package export exposed an incorrect SPDX header on the new file; it
was corrected to Apache-2.0 before this final native and package qualification.
Every native source/schema input is exactly current, with no source exceptions. See
`native-artifact-replay-inventory-v2-verified.json` and its post-GC receipt.

These are facts about current physical keys, not a bound on every future
transaction effect. PREPARE replay credit and atomic background-credit adjustment
remain open. Full-sync resolution can generate provider-dependent artifacts;
prepared generated effects or a correct owned postdecision protocol is also
required. Catalog changes that alter possible effects need compatible pinning.
The minimum completion RAM/CPU/I/O bundle, full capture CPU accounting, whole-node
policy, remaining physical graph and original scheduler scope remain incomplete.
Full Linux qualification remains waived; Linux builds use pinned Nix glibc Zig.

### Prepared transaction effect decoding

ATPE v1 preparation frames have a 64 MiB encoded limit and a 256 MiB decoded
limit. Before one decode allocation, a validation pass checks the entire frame,
all tags/counts, identity and decode bytes including per-slice alignment slack.
A fixed-buffer allocator then builds the plan inside that slab. Decoded owns it;
all plan slices borrow it independently of the input frame. Do not run the
existing PrecomputedGeneratedBatch per-field deinit on these slices. The future
resolution adapter must retain the slab owner through commit or explicitly
clone into an independently funded ownership form. Original controls are borrowed
only during traversal, copying and hashing; no callback is retained by Decoded.

The codec has four focused regressions and a fixed wire fixture. Qualification
also runs nine existing transaction batch-credit/backlog cases. There is no
PREPARE persistence, durable reservation or recovery-lifecycle guarantee from
this codec alone. See `native-prepared-transaction-effects-v5-verified.json` and
[the protocol](../../WORKLOAD_SCHEDULING.md#transaction-completion-before-the-commit-decision).

### Durable prepared-effect retention owner

A sealed ATPE frame is one DocStore publication obligation under the existing
4,096-record/256 MiB aggregate. Its actual key/frame bytes share physical commit
with size metadata and aggregate debt. Idempotent seals do not add credit.
Abort and explicit accepted-plan consumption retire the frame with its charge;
HA delivery credits have their own retirement boundary. Native reopen and quota
regressions exercise the actual DocStore capture, not a raw-backend substitute.

PreparedEffects owns the decoded slab through materialization. Existing effect
per-field deinit cannot free these slices. The trusted DB adapter must preserve
this owner or clone into separately funded storage, stage every required effect,
validate/retain the exact catalog generations and include table/shard identity in
its catalog digest. Caller apply ownership and retained non-yielding controls
remain required; these methods do not grant an independent execution scope.

Frame admission does not fund future generated output, replay, working memory,
provider roots or physical completion. Raw writes, restore/import/downgrade
eligibility and production prepare negotiation remain open. Six lifecycle tests, 69 strict transaction-manager cases and focused HA/inventory
and metadata cases cover this storage layer. Plain effect data lives outside
physical execution owners; native ABI 29 fences those canonical type identities.
The final qualification passed 100 distinct custom-runner cases plus 28 compiled
owner/client boundary tests. See `native-prepared-effect-seal-v5-verified.json`.


### Durable BEGIN preparation requirement

A required-BEGIN marker owns one record and its actual key/40-byte value in the
shared DocStore publication pool. Transaction creation and ordered receipt share
its atomic batch; quota or allocation rejection cannot leave a legacy transaction
without the requirement. Same-mode retries preserve the existing charge, and
later mode changes or upgrades from legacy BEGIN are refused. Accepted-plan
consumption and authoritative abort retire this owner atomically.

Committed requirements and frames survive generic recovery even with no row
intents or a stale resolved flag. The generic path records deferred work and
continues unrelated legacy recovery. It does not supply a plan-aware materializer
or a completion resource bundle. Native ABI 30 enforces the storage commit gate;
production wire activation, peer migration and restart/import policy remain open.

Qualification: 75 strict manager cases, 109 distinct custom-runner cases in the
combined focused suite and 28 owner/client boundary tests passed. All 156 native
build steps and the independent package's 25 ReleaseSafe compile checks passed.
See `native-required-preparation-v3-verified.json`. Full Linux qualification
remains waived; the full design is incomplete.


### Accepted-plan materialization ownership

DB resolution retains a decoded PreparedEffects owner through the atomic batch.
cloneForMaterialization first checks encoded and decoded bounds, then copies all
seven effect families to the caller's preparation allocator with ordinary
per-field ownership. Allocation and original-control failure clean partial copies;
the decoded slab is released separately. Native ABI 31 fences the new borrowed
accepted-owner field in TransactionResolution.

The mutation pipeline bypasses fresh synchronous producers, materializes accepted
artifact/replay/coverage effects, verifies the frame/revision and current exact
catalog plus physical namespace, and retires frame/requirement debt with the
physical commit. A terminal decision with zero row intents does not authorize
skipping accepted work. DB recovery uses the same path; generic manager recovery
continues to defer it. Catalog mismatch or corrupt preparation remains deferred
without erasing obligations or blocking unrelated legacy cleanup.

Current catalog validation is not durable generation retention. Materialization
scratch and future replay/output/publication grants still require the predecision
minimum completion bundle. Normal wire activation and peer/import/downgrade policy
remain pending; the full design is incomplete.

Qualification passed 77 strict manager cases, 118 distinct custom-runner cases
in the combined suite, 28 owner/client boundary tests and 156/156 native build
steps. The archive matches 4,412 current public sources; its independent ReleaseSafe
consumer passed 25 compile checks. See
`native-accepted-materialization-v2-verified.json`. Full Linux qualification
remains waived.


### Sealed-frame catalog lease

A durable accepted frame pins the current serving index, enrichment and resolver
catalog bytes/generations. One keys-only metadata seek establishes presence,
including opaque/orphan frames; no frame payload is hydrated. DB metadata probes
cannot iterate, so the observation uses a short current-scan lease under apply.
The existing charged frame owns this policy obligation through atomic retirement.

Serving catalog admission checks before dependency quiescence and rechecks under
apply; persistence also fences changes before a reconciliation allowance. Identical
publication and unchanged refresh remain legal. Exact ordered alignment validates
current durable and in-memory definitions/load/repair state and preserves its
context authorization. Mutable noncatalog progress can still drain. Final DB
sealing checks the actual catalog/namespace so stale speculative work creates no
pin. Native ABI 32 fences older native owners that ignore this policy.

The scope is serving catalog ownership. Raw imports/downgrade policy, distributed
protocol activation and predecision producer/completion reservations remain open.
The full design is incomplete.

Qualification passed 78 strict manager cases, 121 distinct custom-runner cases
in the combined focused suite, 28 owner/client boundary tests and all 156 native
build steps. The archive matches 4,412 current public sources; its independent
ReleaseSafe consumer passed 25 compile checks. See
`native-prepared-catalog-lease-v1-verified.json`. Full Linux qualification is waived.

### Artifact generation descriptor sharing

A writable Lite artifact registry owns one locked marker inode. Each artifact
keeps its original 32-hex lease key and marker name; those marker names are hard
links to the registry's locked inode. Independent sweep descriptors still fail
to claim live aliases. The registry anchor uses a separate owner prefix, and
bounded marker sweeping removes unlocked anchors after a crash. Closing one
artifact removes its own name without unlocking another artifact's lease.

Internal readers share one reference-counted data descriptor per Store generation.
Each NativeFile view retains its own decoded header, checkpoint and caches. Only
internal read-only opens accept a shared handle. Registry cache ownership ends
at generation retirement or Store detach; live artifact views retain independent
references through their physical close. Fenced external/read-only Store readers
keep their existing independent lock ownership.

The registry's I/O and resource manager outlive its last view. Callback detachment
and joining precede cache-owner retirement, and generation adoption fences new
opens before releasing the old generation's cached handle. Existing retired-inode
accounting still transfers between live states until the last one closes. Warm
logical source caches retain their existing admission and eviction behavior.

The updated native CLI passes the original macOS file-engine overload/restart
profile twice on the same binary. Each run completes 1,840 offered operations,
recovers descriptors from 23 to 23 against the unchanged allowance of eight,
and verifies exact HTTP/pgwire postimages across joined shutdown/reopen. This
closes that profile's descriptor-recovery gate. It does not qualify other process
envelopes or complete the full design. Receipts are
`generation-handle-pool-native-file-v1.json` and `-v2.json`.


### Graph preparation shares the existing execution owner

Graph publication now checkpoints its original execution during topology census,
CSR construction, endpoint and dictionary sorting, projection, metric output
planning, and encoding. The page-root publication callers carry this control
through selected topology preparation. Artifact page reads retain the existing
read context; provider and numerical worker cancellation tokens remain passive.
Interruptible in-place sorting adds no source-wide scratch allocation. Refusal
unwinds caller-owned output without publishing partial orderings.

Run `zig build antfly-graph-preparation-test -Dcuda=true -Doptimize=Debug -j1`
from `zig/` using the supported platform toolchain. This focused production root
includes graph publication, numerical team ownership, projection and output
admission, selective topology, reuse, warm starts, and allocation failure cases.
A 32,768-node / 131,072-edge public payload build queues a query behind its sole
ordinary CPU slot, yields to that query, and produces the same encoded metric
as serial execution. Cancellation, deadline, and lease-loss refusal preserve
the existing execution owner; paged topology preparation refuses every actual
checkpoint with the leak-checking allocator.

This is deterministic integration with the native scheduler and production graph
functions. It does not establish deployed HTTP latency, cloud-provider throughput,
or mapped-page-fault qualification. Model/framework/download preparation and the
remaining cross-engine qualification remain part of the broader objective. The
existing full GNU qualification waiver remains in effect.
