# VOPR Status History

> Relocated verbatim from `zig/VOPR.md` (lines 3–541 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`VOPR.md`](../../../zig/VOPR.md). Durable decisions from this log were folded into that document before the move.

Status (2026-08-31): the common VOPR engine and deterministic `std.Io` runtime
are integrated with independent metadata, transaction, Raft, storage, HA,
data-plane, derived-workflow, backup/restore, and clock-fault scenarios. The
production DataServer now serves public HTTP on borrowed `VoprIo`. Focused
compositions run the merge accept/catch-up/rollback/retry/finalize actions first
through one owner and two local groups, then chain replicated merge and split
across three production owners and three three-replica groups over the lifetime
of that same scheduler. The distributed history uses real public HTTP/Raft
listeners, routed merge forwarding, leader transfer, split
prepare/bootstrap/catch-up/finalize, a post-bootstrap public write, one owner
restart after cutover, exact routed terminal retry, every-replica range,
document, transition, and watermark convergence, and a fresh-state replay of
the recorded actor/time schedule. The current checkpoint repaired the sparse
split-delta sequence defect with predecessor-fenced Raft-index watermarks and a
durable version-4 activation barrier; it also fixed destination-range
projection and post-restart source-range widening found by the same history.
The focused record/replay gate is green again. Clock-only stutter is
normalized at the explicit physical-LSM differential boundary; no different
actor may execute, and a recorded actor that does not become ready within the
bound fails replay closed.
Background-owner lifecycle, serverless object-store faults,
resource admission, datagrams, corpus quarantine, multiverse artifacts, and a
scriptable/interactive debugger are executable. Replication backfill,
supervision, authentication, complete serverless orchestration, DB/index races,
query-embedding caching, provider boundaries, generation/reranking chains, and
composed and distributed query execution have focused exact-replay suites, as
do persistent Parquet cache, provisioning/startup, external-lake,
media-provider, and upgrade/compatibility boundaries. The deployment-shaped
campaign now carries serialized Raft frames through the replayable fault
router and real `httpx` client/server sockets on `VoprIo`, with distinct
production `ResourceManager` owners per logical node. One full-cluster mode
fills every node envelope through exactly-once production reservations,
requires a public write to be denied, releases the competing owners, and then
requires the same write and lookup to recover; another pauses a public graph
request between rounds and restarts its next-range leader; an eighth cuts the
real internal fanout transport after round one, requires a fail-closed typed
retryable response with no graph payload, heals the link, and requires a full
retry through the same coordinator. A ninth mode starts the same public graph
request, performs a cross-root range merge through the production merge
coordinator using the actual donor and receiver leader writers, requires
topology-retry exhaustion to return that typed 503 without partial graph data,
finalizes the merge, and requires the complete graph from the recovered route.
That v9 full-cluster mode does not route the transition through production
`DataServer` owners or replicate its structural actions through every receiver
replica. The current green focused multi-owner composition closes that production
action/proposal/apply, merge-to-split composition, ordinary-write delta replay,
every-replica convergence, failover, and restart seam. Production-owned v12
drives the real-metadata active transition through those owners and exact-
replays the complete split. Production-owned v13 separately exact-replays a
static-topology depth-two public graph across the real `DataServer` owners. It
routes public reads to the current group owner and fences every strong graph
phase on both an applied Raft ReadState and local full-index visibility.
Production-owned v14 now composes those repaired paths in one history: it
starts the same public depth-two graph during a durable nonterminal v12 split,
allows only a complete result or a typed fail-closed 409/503, completes
cutover/publication, and then requires complete public traversals across the
post-split three-range topology. Its ReleaseSafe exact-replay gate passes
15/15. Production-owned v15 adds one real transport-fault composition to that
same history. It selects a public coordinator that owns neither graph range,
cuts only the next owner's `/graph-expand` request stream after depth one,
requires the coordinator to observe the transport failure and return the typed
retryable 503 without graph data, heals before the public response, completes
the split, and requires the full post-cutover traversal. Its 450,000-transition
ReleaseSafe record and fresh-world exact replay pass 15/15. The reusable
`VoprIo` fault is endpoint-, direction-, and semantic-byte-stream scoped, so it
does not partition data-Raft or split-control requests sharing the listener.
Production-owned v16 adds the first real process-incarnation fault to the same
active-split graph history. It stops the selected next-range production
`DataServer` and its public/data-Raft listeners after depth one, preserves its
durable root and stable advertised ports, requires a typed retryable 503 with
no partial graph payload, reconstructs the owner on the same `VoprIo`, heals
Raft leadership, completes split cutover, and requires the full post-cutover
traversal. Its 650,000-transition ReleaseSafe record and fresh-world exact
replay pass 15/15 with cleanup and leak checks. Production-owned v17 adds a
recoverable short-write composition without
turning it into an outage. After depth one, `VoprIo` limits exactly one
coordinator-to-next-owner `/graph-expand` client write to one byte, requires
the production HTTP stack to resume the stream, and proves the in-flight graph
remains complete while split cutover and post-cutover traversal finish. The
fault is endpoint-, direction-, semantic-stream-, and deployment-link scoped;
an exact application counter prevents a vacuous pass. Its 500,000-transition
ReleaseSafe record and fresh-world exact replay pass 15/15. Production-owned
v18 adds memory-pressure denial and recovery across all three real `DataServer`
resource owners during the same active split and in-flight graph. It
distinguishes pre-proposal 503 from post-proposal outcome-unknown 409, requires
a read-before-retry decision for the fixed-ID write, releases the pressure,
and requires split publication plus post-cutover document and graph visibility.
Its 550,000-transition ReleaseSafe record/fresh-world replay gate is the
promotion criterion; the current-tree gate passes 15/15 with properties,
cleanup, and leak checks. Production-listener socket pressure is promoted by
v22 below. Disk-capacity pressure, broader socket-pressure overlap, pressure with
link/process/storage faults, broader short-write and restart targets, disjoint
placement, derived-state equality, bounded retained history, and snapshot-
install rehydration remain future work.
Production-owned v19 adds the first public distributed-join composition on
those same owners. Two left rows reference documents placed in two independently
owned right-table ranges; the public inner `_id` join must return exactly both
rows, report distributed execution over both groups, and never accept a
successful partial response. The history repeats that oracle before a split,
while the split is durably nonterminal, and after topology publication. It also
preserves typed 409/503 retry semantics for ownership churn and makes ordinary
strong derived queries use the same ReadIndex-plus-full-index barrier as graph
phases. This is a deliberately narrow join claim: durable shuffle
partial-worker recovery, cancellation, authorization changes,
right/nested/foreign joins, multi-range left inputs, and global-query
orchestration remain roadmap work.
Production-owned v20 adds one durable-shuffle recovery shape. A 64-row join is
forced through shuffle execution with a runtime durable store on every
production owner. The first finalizer persists the complete result and then
fails before acknowledging it; a different owner acquires the shared lease,
imports the cached result, and completes without repeating finalized work. An
exact two-attempt ledger, imported-owner/cache evidence, 64 joined rows, and an
injected-fault counter prevent a vacuous pass. Its 300,000-transition
ReleaseSafe record and fresh-world exact replay pass 15/15 with cleanup and
leak checks. This promotes finalizer takeover after an ambiguous persisted
completion, not arbitrary partition-worker recovery, public cancellation,
overlapping owner failure, or every join form.
Production-owned v21 adds the first production-owner overlapping-fault shape.
During the active split it first proves the v18 all-owner resource-admission
contract, including typed pre-proposal denial or explicit outcome-unknown plus
read-before-retry recovery. At the graph's depth-one lifecycle boundary it
saturates all three real `DataServer` resource managers again and cuts the
selected next-owner `/graph-expand` endpoint while that pressure remains
active. The public request must return a typed no-partial 503, both faults are
then healed, and the resource probe, split, depth-two graph, post-cutover read,
quiet suffix, and cleanup must all complete. A dedicated active-overlap witness
prevents the network and resource faults from passing merely because they were
registered in the same manifest. Its 600,000-transition ReleaseSafe record and
fresh-world replay pass 15/15. This promotes one link-plus-memory overlap; v21
itself does not promote disk-capacity or listener-socket pressure,
storage/process overlap, multiple failed owners, or the
broader fault matrix.
Production-owned v22 adds endpoint-stable, reversible socket-admission pressure
at one selected real `DataServer` public listener during the active split. A
fresh non-pooled production HTTP client must fail with the exact
`ProcessFdQuotaExceeded` resource error while the public handler ingress count
remains unchanged; established Raft/control connections and other listeners
stay available. The limit survives listener identity replacement, healing it
lets a second fresh client read the same document, and the in-flight graph,
split publication, post-cutover traversal, quiet suffix, cleanup, and
fresh-world exact replay must all complete. Its 500,000-transition named deep
gate passes 15/15. This promotes one selected-listener socket-denial/recovery
seam, not disk-capacity pressure, all listener classes, or socket overlap with
link/storage/process/restart faults.
Production-owned v23 composes reversible logical service rates across the real
three-`DataServer` deployment, its distributed graph coordinator, and the
production serverless workflow on the same `VoprIo` clock. A node-wide
two-times slowdown is active during DataServer bootstrap/Raft progress and
serverless publication/compaction, then heals before the public graph workload.
Typed adapters prove exact slowed cost before healing, exact baseline cost
after healing, continued DataServer and graph work, zero active effects, full
public visibility, quiet cleanup, and fresh-world exact replay. Its dedicated
90,000-transition gate passes 15/15. This is the first full-cluster service-rate
composition. Forward-only v42 adds a clean production replication snapshot-to-stream
backfill whose target crosses public HTTP, routing, leader forwarding,
`DataServer`, data Raft, and index visibility on the same slowed/healed node.
Forward-only v43 interrupts that production history after the first accepted
snapshot batch, changes the source schema, resumes from durable status with one
exact duplicate target batch, and completes CDC plus every cluster oracle.
Forward-only v44 instead destroys the exact target leader process before the
next snapshot batch, requires a stopped-endpoint failure plus one bounded
pooled-client reconnect, reconstructs the stable node/store and both listeners,
proves the first replicated row survived locally and the rebound public
endpoint serves durable data, then resumes to exact all-node visibility.
Forward-only v45 fails the first actual provider query after its durable
preparation status, closes that owned source session, creates a strictly newer
session, and resumes without adding or duplicating target batches.
Forward-only v46 revokes the work lease after the first snapshot checkpoint is
durable, requires `CdcWorkLeaseLost`, replaces the source session, and resumes
without replaying that committed target batch. Additional source-crash and
cancellation timings remain future breadth. Forward-only v47 revokes ownership
after target apply but before checkpoint publication, proves the stale owner
cannot advance durable offset 0, and requires one exact idempotent target
replay. Forward-only v48 promotes the metadata source-
catalog boundary itself: the source configuration changes through metadata
Raft after target apply, authority A is rejected before checkpoint publication,
and authority B atomically claims against and retires A before one exact
idempotent replay. Additional topology timing, metadata leadership loss, and
cross-domain overlap, plus other target-crash timings, remain future breadth.
Forward-only v49 replaces the earlier adjacent v41 cache composition with the
cache actually owned by node 1's live production `ApiHttpServer`. Under the
shared node slowdown, a same-key waiter crosses the real coalescing ledger and
expires at its logical deadline while one producer remains in flight. After
healing, a retained hit succeeds; the ordinary public workload then reaches
its durable completion fence, node 1's exact `DataServer` process is destroyed
and reconstructed from stable roots, the replacement cache is proven empty,
and the same key recomputes exactly once before a retained hit. The long-lived
public client must absorb exactly one stale pooled-connection failure and then
read a pre-restart durable document through the rebound endpoint. Cache
topology/link/storage/resource overlaps remain future breadth.
Production-owned v24 promotes document hydration through the real public graph
request and production `DataServer` owners. The request traverses two ranges,
asks for selected document fields, and requires the public response to contain
exactly the expected nodes and hydrated titles. Lifecycle evidence proves one
hydration fanout starts and completes, rather than accepting documents already
present in the traversal result. Its dedicated 90,000-transition gate passes
15/15 with fresh-world exact replay, cleanup, and leak checks. This promotes
public production-owner hydration only. At that checkpoint public cancellation,
authorization mutation, stale-generation rejection, retry exhaustion, and
their fault compositions remained future work.
Production-owned v25 promotes public request cancellation after multi-owner
hydration tasks are scheduled. A production-neutral lease-free lifecycle hook
waits for the listener's real cancellation token; the public client's
`std.Io.Future` is canceled, HTTPX interrupts the socket, the canceled request
must not publish hydration completion, and an unmodified retry must be the sole
completed hydration with exact documents. Its dedicated 110,000-transition
gate passes 15/15 with fresh-world exact replay, cleanup, and leak checks. This
promotes one cancellation/recovery shape only; authorization mutation,
stale-generation rejection, retry exhaustion, and cancellation under the
broader fault algebra remain future work.
Production-owned v27 directly replaces the earlier between-request
authorization mode with in-flight revocation through the same public graph and
real `DataServer` owners; VOPR has no compatibility alias for the retired mode,
property identity, trace revision, or build target. All public setup traffic
authenticates through one production `UserManager`; a canonical source edge
targets the independently owned tenant table. After edge expansion has reached
the foreign target, a production-neutral `target_authorization_started`
lifecycle boundary revokes that table's read policy inside the live request.
Foreign-table authorization intersects the credential scope captured at
admission with the user's current policy: a later grant cannot broaden the
request, while a revoke takes effect before hydration. The history requires an
exact empty 200 with no target key or document leak, restores the policy, and
requires the exact target table, key, and hydrated title on the next unmodified
request. Its dedicated 120,000-transition Debug and ReleaseSafe gate passes
15/15 with fresh-world exact replay, cleanup, and leak checks. This promotes one
in-flight permission-revocation/recovery shape. At that checkpoint,
stale-generation rejection, retry exhaustion, and cancellation under the
broader fault algebra remained future work.
Production-owned v28 promotes stale source-generation rejection and bounded
topology-retry exhaustion through that same public graph path and the real
`DataServer` owners. After the coordinator has acquired the stamped source
snapshot, a production-neutral lifecycle boundary publishes a real metadata
and data-plane split, waits for the destination owner, and refreshes the
production catalogs before graph execution resumes. Both permitted attempts
must reject the retained group set with `TopologyChanged`; the public response
must be the typed 503 `distributed_query_unavailable` without any target key,
title, or partial graph data. A fresh request must then read the post-split
document and reproduce the exact hydrated traversal. V28 directly introduces
its mode, property, trace revision, and build target without aliases for an
earlier VOPR surface. Its dedicated 340,000-transition gate covers this one
stale-snapshot/retry-exhaustion shape and passes 15/15 in ReleaseSafe with
fresh-world exact replay, cleanup, and leak checks; cancellation combined with
the broader fault algebra remains future work.
Production-owned v29 promotes one concrete cancellation-under-fault shape
through the same public graph path. Before the workload starts, the deployment
manifest freezes the actual coordinator-to-target-owner link. At the first
`hydration_fanout_started` boundary, after production shard tasks have been
scheduled, the lifecycle hook installs an endpoint- and payload-scoped outage
for `/graph-hydrate`. A monotonic `VoprIo` witness must prove that real
production hydration traffic reached the failed boundary before the public
client cancels its `std.Io.Future`. The listener-owned cancellation token must
be observed, the canceled request must publish no hydration completion, the
outage must heal, and one fresh request must return the exact hydrated graph.
V29 directly introduces its mode, property, trace revision, and build target;
there are no legacy aliases, readers, or migration paths for this new VOPR
surface. Its dedicated 140,000-transition ReleaseSafe gate passes 15/15 with
fresh-world exact replay, cleanup, and leak checks. This closes the scoped
transport-outage composition only; cancellation under resource, storage,
process, restart, and multi-fault combinations remains future breadth.
Production-owned v30 promotes public durable-shuffle cancellation at an actual
partition-worker boundary. A 64-row public join starts through production HTTP,
the first internal partition worker reports its nonzero durable job and owner,
and a lease-free lifecycle hook waits on that worker request's transport-owned
semantic cancellation token. The public client then cancels its
`std.Io.Future`; the started worker must not report completion. A fresh request
must complete the exact 64-row distributed shuffle with no finalizer retry, and
the terminal evidence requires exactly one more worker start than completion:
the canceled worker, while every clean-retry worker drains. The campaign found
and repaired a production boundary where internal join operations checked the
request context but executed finalize, rows, unmatched, and partition work with
the process-scoped `JoinContext`, dropping request cancellation and deadline.
V30 binds both capabilities into every internal operation and maps the join
engine's cancellation result into the transport-neutral API contract. Its mode,
property, trace revision, and build target are forward-only with no aliases,
readers, or migration paths. Its dedicated 360,000-transition record and
fresh-world exact replay pass 15/15 in Debug and ReleaseSafe. This promotes one
clean cancellation/recovery shape, not partition-worker crash recovery,
cancellation combined with another fault, authorization or generation
mutation, or the remaining join forms.
Production-owned v31 promotes one partition-worker failover through the real
durable-shuffle protocol. At the first `partition_worker_started` boundary the
history records the nonzero durable job, partition, and worker group and fails
that worker before right-row collection or result publication. The production
shuffle engine must record the failed attempt, retry the same partition on a
different group, and return all 64 rows. The public profile must report exactly
one worker retry, a failed-then-successful pair for the same partition and the
observed groups, no failed later persisted attempts, and one successful
finalizer without finalizer retry. Lifecycle evidence additionally requires
more worker starts than completions, so no failed worker is mistaken for a
published result. V31's mode, property, trace revision, and build target are
forward-only, with no aliases, legacy readers, or migration paths. This
scenario's dedicated 360,000-transition record and fresh-world exact replay
pass 15/15 in Debug and ReleaseSafe. This promotes protocol-level partition
failover, not an actual DataServer process crash/reconstruction, retry
exhaustion, or failure combined with another fault domain.
Production-owned v32 promotes the next join lifecycle boundary into an actual
partition-owner process destruction and reconstruction. The scenario chooses
the non-hosting public coordinator before dispatch, then identifies the exact
serving process at the production `partition_worker_started` boundary so a
leadership change during fixture publication cannot stale the modeled fault
domain. It destroys that `DataServer` together with its public and Raft
listeners, preserves its durable roots and stable node/store identity, fences
every remaining attempt in the original operation, and requires an exact typed
503 `distributed_query_unavailable` response containing no hits or joined row
data. The same partition must be selected on a different group and process
before the stopped owner is rebuilt. Reconstruction rotates the reporter
incarnation, republishes the rebound endpoints, restores stable leadership and
leader-correct routing for every initial group, and then permits an identical
fresh 64-row public shuffle plus a direct read through the rebuilt endpoint.
The property requires nonzero job/partition ownership, an observed down and
reconstructed process, different failed/recovery groups and nodes, no partial
initial result, exact fresh recovery, and more worker starts than completions.
V32 directly introduces its mode, property, trace revision, and build target;
there are no aliases, compatibility readers, migration paths, or legacy VOPR
formats because VOPR is new code. Its dedicated 420,000-transition record and
fresh-world exact replay pass 15/15 in Debug and ReleaseSafe.
Production-owned v33 composes durable-join retry exhaustion with two genuinely
active fault domains. The public coordinator owns the first exact-group worker
locally; at its real `partition_worker_started` boundary the campaign saturates
every production DataServer memory envelope and fails that operation with
typed resource exhaustion. In the same window it cuts only the registered
coordinator-to-alternate-owner `/join-partition` semantic stream. The property
requires a nonzero durable job, distinct first/retry groups and processes, one
or more entered workers but zero completions in the failed operation, a
monotonically matched network outage while all resource envelopes remain full,
an exact retryable 503 with no hit or joined-row data, independent healing of
both domains, and a complete identical 64-row request afterward. The history
also proved that remote exact-group 503/500 responses must never be converted
to a null route and executed against a foreign local DB; internal join
ownership, resource, and HTTP status classes now remain typed through the
client and coordinator. V33 directly introduces its mode, property, trace
revision, and build target without compatibility aliases or readers. Its
dedicated 420,000-transition record and fresh-world exact replay pass 15/15 in
Debug and ReleaseSafe. Authorization/generation mutation, broader join forms,
global-query composition, and cancellation under other fault combinations
remain future work; v34 promotes the resource-plus-link cancellation shape.
Production-owned v34 composes durable-join cancellation with the same two real
fault families without reusing v33's retry-exhaustion oracle. The public
coordinator is selected as the live leader for the alternate worker group, so
the first partition dispatch must cross the registered coordinator-to-primary-
owner `/join-partition` stream and match a scoped outage. The alternate local
worker then enters the production `partition_worker_started` boundary with a
nonzero job and the listener-owned cancellation token. At that boundary the
campaign fills every DataServer memory envelope, proves the network failure
already occurred, and parks the worker until the public future is canceled.
The property requires distinct primary/alternate groups and processes,
simultaneous network and resource evidence, no completion from the canceled
worker, cancellation observed through the semantic token, independent healing
of both domains, exactly one terminal start without a completion, and a
complete identical 64-row request afterward. V34 directly introduces its mode,
property, trace revision, observations, and build target; it adds no aliases,
legacy readers, or migration paths. Its dedicated 420,000-transition record and
fresh-world exact replay pass 15/15 in Debug and ReleaseSafe. Cancellation
followed by process destruction/restart is promoted by v35; storage faults,
disk pressure, simultaneous process loss before cancellation drain, and other
fault combinations,
authorization/generation mutation, broader join forms, and global-query
composition remain future work.
Production-owned v35 composes the established durable-worker cancellation
boundary with real destruction and reconstruction of the exact worker owner.
At `partition_worker_started` the history captures the nonzero job, partition,
group, and serving process, registers that process's deployment node-pause
fault, and cancels the public future. It requires exactly one worker start and
zero completion from the canceled operation. Only after that cancellation
terminal does a separate production restart owner tear down the selected
`DataServer` and both stable-port listeners, rebuild the same node/store
identity with a fresh incarnation, restore leadership and routing, and release
the fault. The property then requires an identical complete 64-row durable
join, a direct successful read through the rebuilt endpoint, all three live
hosts, cleanup, and exact replay. V35 directly bumps the scenario ABI and adds
its mode, property, trace revision, observations, and build target; it has no
aliases, legacy readers, or migration paths. Its dedicated 420,000-transition
record and fresh-world exact replay pass 15/15 in Debug and ReleaseSafe.
Cancellation under storage faults, disk pressure, simultaneous process loss
before cancellation drain, and other fault combinations remains future work.
Full-cluster v11 now retires the hosted data owners, keeps the real metadata
quorum authoritative, starts three production `DataServer` owners with caller-
owned public and data-Raft HTTP transports, publishes their endpoints through
metadata, and elects all three data-group leaders over real `httpx`/`VoprIo`
frames. It completes public writes and reads for two tables, observes the
serverless public catalog, cleans up, and exact-replays within 30,000
transitions. Model v5 made the formerly unstable schedule diagnostic enough to
identify three production integration defects: a native mutex wait that pinned
the single borrowed-I/O scheduler, missing internal-service credentials that
caused fail-closed middleware to reject every forwarded request, and JWT
signing through host realtime that changed packet contents across replay. The
scheduler-safe retry seam, production fixture identity, and shared transport
clock authority repair those defects; the v11 smoke gate now passes 30/30. The
v12 extension installs the
same production `HostedShardOperationAdapter` used by `MetadataServer` and
routes metadata split actions to the real DataServers. Subsequent work removed
serial control/Raft starvation, fixed cancellation and owner-drain defects,
made DataServer and remote-metadata decisions borrow the `VoprIo` clock, and
reached split finalize, publication, the post-split read, and a complete
recorded history. Parent/resource-scoped task identity
removed the earlier choice-36,298 listener-owner swap. Subsequent
deep runs exposed and fixed three more replay-identity leaks: global socket
allocation order, equal-length packet contents hidden behind a byte count, and
host filesystem capacity serialized into DataServer status HTTP. Virtual-OS
model v5 scopes fibers by parent and ASLR-independent callsite, binds accepted
connections to the logical client owner plus first semantic stream payload,
migrates parked socket resources before delivery, and rejects selected-
transition metadata or payload-digest differences immediately even when a
stable ID matches. Model v6 additionally separates a task's immutable resource-
creation owner from its scheduler identity: an outbound retry can no longer
become the child of its previous socket after that task parks for a response.
The production control owner now publishes one explicit
semaphore completion per requested round. The old 166-versus-165 control-round
divergence is closed. The remaining status/request ordering escape was traced
to transition retry jitter seeded from host randomness outside `VoprIo`.
Managed services now preserve a configured deterministic salt across service
replacement. A 60,000-transition prefix exact-replays through the formerly
divergent region, and the complete 320,000-transition v12 ReleaseSafe gate now
passes 15/15 with active split, publication, post-split read, fresh-world exact
replay, properties, cleanup, and leak checks. Canonical comparison walks wire
records directly, avoiding the former multi-gigabyte whole-trace render tail.
The 2,000-transition early-cancellation gate remains useful lifecycle evidence,
but the complete deep gate is the promotion evidence. Full-cluster v9 remains
the green nine-fault hosted campaign.
The reusable deployment composer registers role dependencies, instances,
directional links, process/storage/resource domains, fault scopes, and
quiet-suffix evidence;
full-cluster v9 is its first production-shaped consumer. The same campaign now
serves the worker-owned object catalog through the production serverless HTTP
handler, queries the published version through the real public client, and
executes a depth-two graph traversal across two table ranges through public
HTTP, production planning, shard fanout, and response assembly.
Routed data,
split/merge, query assembly, and DataServer-owned
maintenance services expose production-safe scheduler boundaries. Campaigns
export unified reduction/causal/counterfactual debug recipes, bounded flight
recordings, stable JSON/static reports, and explicit quarantine manifests; the
virtual filesystem models persistent sector corruption and torn
synchronization. Distributed VOPR is first-class across metadata, Raft, HA,
transactions, the data plane, distributed graph queries, and a deployment-
shaped full-cluster composition. Remaining gaps are targeted workload breadth,
finer safe suspension points, and native differential fidelity.

Verification audit (2026-08-28): full-cluster v9 passes all nine recorded
histories and their exact replays, including leak and strict error-log checks.
The Raft transport, determinism, serverless-workflow, focused distributed-query,
and graph lifecycle gates also pass at this checkpoint. The preceding
merge/data-Raft checkpoint passed `lib-data-storage-test` 67/67 and
`antfly-data-runtime-test` 125/125 with no leaks. It includes production-envelope
replay, pre-covering-receiver bootstrap evidence, finalize/reopen persistence,
the version-3 capability barrier, rejection of merge controls before durable
activation, source fencing and receiver-checkpoint snapshot transfer, and
identical apply results across three replica stores. This proves the production
protocol and projection seams. A focused `data-server-vopr-test` history
additionally drives the real `DataServer` adapter, two local data-Raft groups,
source fencing, receiver checkpoints, document copy, finalization, rollback,
and observation using only the borrowed `VoprIo` clock and scheduler. A second
history now chains merge into a newly admitted split generation across three
owners, uses three three-replica groups over its lifetime, writes through the
real public HTTP API after split bootstrap, changes leaders before catch-up,
finalizes, restarts one owner, proves source/destination range and document
equality plus every-replica transition/watermark convergence, and repeats from
fresh durable roots under the recorded schedule. The isolated
`data-server-transition-vopr-test` gate, including its inline-failure ownership
regression, passes at the current checkpoint. A current exact-filter run of the
same three-owner composition passes record and fresh-state replay with no leaks;
the protocol-version, parser, apply-store range, and destination DB regressions
also pass. The complete 126-test runtime shard has not yet been rerun after
these repairs, so it is not cited as a fresh aggregate result. This focused
transition claim remains distinct from v9's hosted graph/fault rig. The v11
production-owner baseline now converges metadata/store endpoints, elects its
production data-Raft leaders, completes public and serverless work, cleans up,
and passes record plus fresh-state replay: its current ReleaseSafe smoke gate is
30/30. The standalone
HTTP-client suite, immediate shutdown-wake regression, `vopr-engine-test`,
`vopr-determinism-audit`, serverless workflow, focused transition suites, and
v12 early bounded-lifecycle gate passed at their cited checkpoints. The active-
split v12 extension passed its complete 320,000-transition ReleaseSafe deep
gate 15/15 after deterministic retry-jitter ownership and bounded canonical
comparison repairs. Production graph v13, graph-during-split v14, graph-
transport-during-split v15, graph-owner-restart-during-split v16, and scoped
partial-write-during-split v17 and resource-pressure-during-split v18 pass
their ReleaseSafe 15/15 gates,
including the left-to-right-to-left traversal, fresh-world exact replay,
properties, cleanup, and leak checks. The supporting ReleaseSafe table-read
shard passes 61/61, including barrier-before-admission and strict no-stale-
fallback regressions; the focused matching-ReadState apply tracker passes 1/1.
The v12 test completes in 43 minutes
with an 8 GB test-process peak; its compile peaked at 11 GB.
After the later routed-local ownership and admission repair, the current tree's
61-test table-read gate, matching-ReadState regression, reusable engine, and
determinism audit pass, and v13's complete 15/15 graph gate passed with those
repairs. A dedicated v12 deep rerun on 2026-08-27 was externally terminated
with SIGTERM while executing bootstrap/replay, without an assertion or replay-
divergence report; its cited 15/15 result remains the earlier checkpoint. V14
subsequently ran the same current-tree production active-split path plus the
new in-flight and post-split graph obligations and passed its complete 15/15
gate. V14 therefore freshly revalidates the shared active-split/graph seam,
while the dedicated v12 seed remains a historical result rather than a newly
repeated command result. V15 then ran a 450,000-transition production-owner
transport-cut history through record and fresh-world exact replay and passed
15/15 with typed fail-closed evidence, post-heal completion, cleanup, and leak
checks. V16 then ran the corresponding 650,000-transition production-owner
restart history: it stops and reconstructs the selected remote `DataServer`
and both service listeners at stable advertised endpoints, proves fail-closed
in-flight behavior, recovers the real Raft groups, completes cutover, and
passes record plus fresh-world exact replay 15/15. The focused ReadState-only
Ready regression and the 16-test `VoprIo` network shard also pass ReleaseSafe;
they preserve two defects exposed by this campaign.
V17 then ran the 500,000-transition production-owner short-write history and
passed record plus fresh-world exact replay 15/15. Its property requires the
scoped fault to apply exactly once, the in-flight graph to return a complete
200 rather than fail closed, split cutover and post-cutover graph traversal to
complete, and cleanup/leak checks to remain green. The reusable engine and
registered-source determinism gates pass after this addition.
V18 then ran the 550,000-transition production-owner memory-pressure history
and passed record plus fresh-world exact replay 15/15. It saturates all three
real resource managers during an active split and graph, requires safe
pre-proposal or outcome-unknown write classification with read-before-retry,
then proves resource, document, graph, split, cleanup, and leak recovery. The
history exposed and repaired transient Raft-apply classification, public
409/503/504 propagation, and a host-time public-query retry escape.
Trace/observation compaction and CI tiering
remain operational follow-up; neither is permission to weaken the completion
or replay oracle. The
focused distributed availability-normalization test passes with no leaks. Its
broader API HTTP gate passed 48/49; the unrelated 128-abandoned-query admission
test missed its minimum-rejection threshold, so that aggregate is not cited as
green here. The generated public
OpenAPI contract and Go SDK include the typed
`distributed_query_unavailable` retry classification; its focused SDK test and
generated package test pass. A fresh `zig build -j1 vopr-test` aggregate was also
attempted, but it is not currently green: several untouched startup,
configuration, upgrade, storage, and VOPR CLI test binaries exit after their
expected diagnostic output without a Zig failure stack. The legacy automatic
split/merge filter failures reproduce on the untouched checkpoint. These are
separate repair items; this document does not cite the current aggregate run as
fresh proof. The word **integrated** below remains an executed claim at the
production seam and modes named in its row, not a claim that every wider
deployment composition or the present aggregate health is finished.

Scope: Zig Antfly simulation, VOPR, modeled-storage, and deterministic chaos
testing. This is the living design and operating policy. Historical phase
progress remains available in git history.

## Verification Audit Narrative (originally `zig/VOPR.md` lines 3884–3923)

> Relocated verbatim from `zig/VOPR.md` (lines 3884–3923 at commit 271838a195) on 2026-09-16 during the documentation cleanup.

The 2026-08-26 design audit forced every exported scenario module into test
discovery and gave each integrated row an executing focused test, exact replay,
and an aggregate dependency. The last green nine-fault distributed checkpoint
directly reran the Raft transport, determinism, serverless-workflow, and full-
cluster v9 gates successfully. The focused three-owner merge-to-split gate and
its protocol/parser/range regressions are also green. The table-write cache-
lifecycle shard passes 98/98, including the stable cache-role lock-order
regression.

The static production-owner checkpoint is now **green at its stated v11
seam**. Model v5 retains callsite-scoped logical task identities and scopes a
semantic same-listener connection by both its logical client owner and first
payload; two clients sending identical requests can no longer exchange
connection and packet identities. A one-request/one-completion semaphore
handshake also makes each production control round an explicit scheduler
barrier. Those changes supersede the old choice-36,457, 166-versus-165 control-
round diagnosis.

The next v11 trace initially looked like a post-acceptance Raft liveness
failure, but lifecycle counters showed zero proposals accepted. Live sampling
then found a native mutex wait pinning the one borrowed-I/O scheduler; after
that was made retryable, fail-closed internal authentication exposed missing
fixture credentials; after credentials were installed, packet digests exposed
JWT signing from host realtime. The production fixes preserve native blocking
semantics, preserve fail-closed authentication, and give signing plus
verification the owning I/O clock authority. The workload still refuses to
retry acknowledged writes and retries only known-idempotent fixed-ID upserts
after bounded reads cannot resolve an explicitly ambiguous outcome.

The ReleaseSafe v11 smoke result is now 30/30: record, fresh-state exact replay,
final properties, cleanup, and strict error-log validation all pass. V12 still
exact-replays its expected early 2,000-transition cutoff with clean unwind, but
that gate does not exercise the active split. The later readiness mismatch was
transition retry jitter seeded from host randomness: record and replay retried
bootstrap status at different logical times even though their `VoprIo` choices
were otherwise exact. Managed services now preserve a configured deterministic
salt across replacement, and the full-cluster fixture supplies a stable
per-node salt. A 60,000-transition diagnostic exact-replays through the old
choice region, and the complete 320,000-transition v12 gate passes 15/15. The
extension is integrated at the active-split seam stated here.

## Verification Audit Narrative (originally `zig/VOPR.md` lines 4006–4032)

> Relocated verbatim from `zig/VOPR.md` (lines 4006–4032 at commit 271838a195) on 2026-09-16 during the documentation cleanup.

At this checkpoint the focused model-v6 task and network suites pass 27/27,
the reusable engine test passes 144/144, and the registered-source
`vopr-determinism-audit` passes 13/13. The bounded v12
subprocess passes 15/15 and the combined
`production-cluster-vopr-smoke-test` passes 30/30 at model v6. The complete
`production-cluster-vopr-deep-test` passed 15/15 at its cited checkpoint:
active split record, fresh-world exact replay, final properties, cleanup, and
leak checks all passed. The current-tree v13 graph, v14 graph/split, and v15
graph/split/transport gates each pass 15/15 with the same complete oracle. V16
also passes 15/15 at 650,000 transitions and adds stable-endpoint teardown,
reconstruction, Raft recovery, fail-closed graph evidence, and post-cutover
completion. The bounded result remains lifecycle-only evidence; v16 is the
fresh combined active-split, production-owner graph, and real owner-restart
completion evidence. V17's current-tree gate passes 15/15 at 500,000
transitions and adds exactly-once short-write application, transparent stream
resumption, complete in-flight graph assembly, and post-cutover completion
under record and fresh-world replay. V18's current-tree gate passes 15/15 at
550,000 transitions and adds all-three-owner memory denial, explicit
pre-proposal versus outcome-unknown evidence, read-before-retry safety,
production-cadence recovery, post-split document visibility, and complete graph
execution under record and fresh-world replay.

The full-cluster v9 checkpoint additionally passed all nine recorded histories
and their clean-world exact replays, plus the focused distributed-query, graph
snapshot/lifecycle, and determinism-audit gates.

## Verification Audit Narrative (originally `zig/VOPR.md` lines 4070–4106)

> Relocated verbatim from `zig/VOPR.md` (lines 4070–4106 at commit 271838a195) on 2026-09-16 during the documentation cleanup.

The v13 production-owner graph started as the deliberately unpromoted
left-to-right-to-left experiment and its follow-up ownership audit exposed six
production defects. An
explicit remote-metadata refresh reused the ordinary one-second cache rather
than crossing a freshness boundary. A public read arriving at a non-owner
attempted the local RawNode and failed `UnknownGroup`; the earlier no-op lease
requester had hidden that routing error. The initial routing adapter then
resolved remote ownership correctly but turned a local route into an unmanaged
direct DB open, bypassing the Provisioned resident/admission owner. The managed
ReadIndex requester only enqueued a request instead of waiting for the matching
ReadState to be applied. Finally, a successful HTTP 200 could contain only the
first graph hop because the selected local replica's derived graph index lagged
its applied base state. The follow-up audit then found group-local helpers that
unconditionally reported outer read admission even when no preparation owner
existed, preventing the resident owner from self-admitting and allowing
algebraic coordinator code to borrow DB/index ownership directly. The repairs
invalidate both cached metadata artifacts on explicit refresh, adapt public
Provisioned operations to current-owner routing while preserving the local
group owner, propagate the actual admission state, use a catalog-only
algebraic planner with admitted shard callbacks, track each ReadIndex through
matching local apply, forbid strong distributed graph reads from falling back
to stale, and wait for full-index visibility before resident read admission.
The 61-test ReleaseSafe table-read gate, focused ReadState regression, and v13
ReleaseSafe exact-replay gate prove this static-topology seam. V14 additionally
proves a public graph request starts during a durable nonterminal active split,
never publishes a successful partial traversal, and completes against the
post-cutover topology. V15 cuts the real next-owner graph stream during that
split, requires a typed no-partial 503, heals, and completes the post-cutover
traversal. V9's restart, topology, and partial-write breadth is not yet all
present on production owners. V16 adds one exact remote-owner
stop/reconstruct cycle at stable public/Raft endpoints during that split and
proves the in-flight request fails closed before the recovered traversal. V17
adds one exact recoverable short write on the registered coordinator-to-owner
link and requires the in-flight request to remain complete rather than merely
fail closed. V18 applies memory pressure to every production owner, preserves
safe ambiguous-write handling, and requires the split, document, and graph to
recover under exact replay.

## Conclusion Recap (originally `zig/VOPR.md` lines 5368–5426)

> Relocated verbatim from `zig/VOPR.md` (lines 5368–5426 at commit 271838a195) on 2026-09-16 during the documentation cleanup.

At the current checkpoint, a
focused three-owner production DataServer history proved routed replicated merge execution followed by
replicated split bootstrap, a post-bootstrap public write, delta catch-up,
cutover, every-replica range/document convergence, leader transfer, owner
restart, and routed terminal retry under record and fresh-state replay. The
history repaired sparse Raft-index delta fencing, destination range projection,
and restart-safe terminal range authority behind a durable v4 protocol barrier.
Full-cluster v11 composes those owners with the real metadata quorum, public
two-table I/O, and serverless catalog. Logical-owner/content connection
identity and the explicit control-round handshake close the former replay
divergence; scheduler-safe resident-open contention, configured fail-closed
internal identity, and executor-owned authentication time close the apparent
write-liveness failure. Its model-v6 ReleaseSafe smoke gate now passes 30/30.
V12 completes the metadata-driven split, cutover, and post-split public read.
Model v6 removes one general outbound-retry identity chain, and stable per-node
retry jitter closes the remaining host-entropy escape. Its complete
320,000-transition ReleaseSafe deep gate passes 15/15 with fresh-state exact
replay, final properties, cleanup, and leak checks at its cited checkpoint.
V13 runs the depth-two public graph through production `DataServer` owners and
passes 15/15 with the same complete oracle. That history found and repaired cached-refresh,
non-owner public routing, asynchronous ReadIndex, stale-fallback, and derived-
index visibility defects. V14 now overlaps that graph with the active split,
requires complete-or-fail-closed behavior while the transition is nonterminal,
requires complete traversals after post-cutover publication, and passes its
current-tree 15/15 gate. V15 adds one scoped real next-owner graph-transport
failure during that split, requires the typed no-partial 503, heals, and passes
its 450,000-transition record and fresh-world exact replay 15/15. V16 adds a
real next-owner process-incarnation fault: it stops the production DataServer
and public/Raft listeners, preserves durable storage and stable advertised
ports, fails the in-flight graph closed, reconstructs ownership, restores Raft
leadership, finishes cutover, and passes its 650,000-transition record and
fresh-world replay 15/15. V17 applies exactly one one-byte short write to the
semantic-stream-selected next-owner graph request, proves transparent stream
resumption and a complete in-flight result, finishes cutover, and passes its
500,000-transition record and fresh-world replay 15/15. V18 saturates all three
production resource managers during that same graph/split, safely classifies
the ambiguous fixed-ID write, uses read-before-retry, restores pressure, and
passes its 550,000-transition record/fresh-world replay 15/15 after proving
document, graph, split, cleanup, and leak recovery. It does not yet cover
arbitrary coordinator, metadata-owner, multi-owner, or overlapping restart
combinations, remaining topology breadth, managed-index and graph/query
disk-pressure combinations, broader
socket-pressure targets/overlap, or broader
request/response/Raft short-write surfaces. Continue
by extending coverage through disjoint placement, bounded transfer, partitions,
and projection/DB/derived-state snapshot recovery; then by running public graph
requests under those replicated topology transitions, cancellation, authorization, and
hydration faults; distributed joins/global queries under real worker failure;
co-resident HA/data-plane owners; row-level tenant scoping and identity
mutation races; resource interference;
and eventually live mixed-version operation—without
requiring a new scheduler, virtual network, replay format, or container
hypervisor. The hard
Antithesis-class local tooling is integrated; the persistent local cross-run
index, filtered retroactive debug
pipeline, and multi-bug search-quality regression corpus are integrated. The
next operational work is to retain and merge nightly corpora, publish the
existing search-quality measurements, and extend scenarios when new
production consumers expose safe ownership boundaries.

## Ongoing Roadmap narrative (relocated from VOPR.md)

> Relocated verbatim from `zig/VOPR.md` (lines 2475-2919 at commit 271838a195, the body of the `### Ongoing Roadmap` section which spanned lines 2473-2920) on 2026-09-16 during the documentation cleanup.

The shortest current summary is:

1. **Deepen the promoted full-cluster reconfiguration seam.** The focused
   three-owner merge-to-split seam now exact-replays with predecessor-fenced sparse deltas,
   durable destination ranges, and restart-safe terminal reconciliation. V11
   instantiates the real metadata quorum, three `DataServer`/data-Raft owners,
   public clients, two tables, and serverless catalog in one deployment. After
   fixing borrowed-scheduler mutex blocking, fail-closed internal-service
   fixture identity, and host-clock JWT signing, its model-v6 smoke gate passes
   30/30. V12 adds hosted-adapter prepare/bootstrap, terminal cutover,
   publication, and the post-split public read. After retry jitter became a
   stable per-node input, its complete 320,000-transition deep gate passes
   15/15 with fresh-state exact replay at its cited checkpoint. V13 exact-
   replays a depth-two public graph on those production owners after repairing
   public owner routing and combined Raft/derived-index visibility. V14 now
   composes the graph and split and passes its current-tree 15/15 complete
   gate. V15 adds one scoped next-owner graph-transport failure during that
   split and passes its 450,000-transition record and fresh-world replay 15/15.
   V16 adds real next-owner DataServer and listener teardown/reconstruction at
   stable service ports during that split and passes its 650,000-transition
   record and fresh-world replay 15/15. V17 adds one exactly observed one-byte
   next-owner request write, transparent stream resumption, and complete graph
   assembly; its 500,000-transition record and fresh-world replay pass 15/15.
   V18 adds all-three-owner memory denial during that same graph/split, safe
   409/503 outcome classification and read-before-retry, then full recovery; its
   550,000-transition record and fresh-world replay pass 15/15. V19 adds the
   narrow public distributed join described below. V20 adds a 64-row durable
   shuffle whose first finalizer persists and fails before acknowledgement;
   another owner imports the cached result, and the 300,000-transition record
   and fresh-world replay pass 15/15. V21 then overlaps all-three-owner memory
   pressure with the selected next-owner graph link cut at the depth-one
   lifecycle boundary, requires a typed no-partial response, heals both, and
   completes resource, graph, split, quiet-suffix, and cleanup oracles; its
   600,000-transition record and fresh-world replay pass 15/15. V22 adds an
   endpoint-stable zero connection limit at one selected production listener,
   proves exact pre-ingress `ProcessFdQuotaExceeded`, heals it, and completes a
   fresh lookup plus graph/split recovery; its 500,000-transition record and
   fresh-world replay pass 15/15. V24 sends a public graph traversal with
   `include_documents`, validates exact node IDs and selected title fields,
   proves exactly one production hydration start/fanout/completion lifecycle,
   and passes its 90,000-transition record and fresh-world replay 15/15. V25
   cancels the public client's `std.Io.Future` after multi-owner hydration tasks
   are scheduled, requires listener-visible cancellation and no hydration
   completion from that request, then proves one exact unmodified retry; its
   110,000-transition record and fresh-world replay pass 15/15. Forward-only
   v29 freezes the actual coordinator-to-target link, waits until real
   `/graph-hydrate` traffic matches its scoped outage, cancels the public future,
   requires no canceled hydration completion, heals, and proves exact recovery;
   its 140,000-transition ReleaseSafe record and fresh-world replay pass 15/15.
   Forward-only v30 starts a public 64-row durable shuffle, parks its first
   production partition worker on the request's real cancellation token,
   cancels the public future, requires that worker not to complete, and proves
   exact recovery with one canceled start plus fully drained retry workers in a
   360,000-transition record/fresh-world gate.
   Forward-only v31 fails the first durable partition worker before row
   collection or publication, requires the same partition to complete on a
   different production group, and validates the exact one-retry worker ledger,
   one successful no-retry finalizer, and all 64 rows in a 360,000-transition
   record/fresh-world gate.
   Forward-only v32 destroys and reconstructs the exact serving process,
   rejects the original operation without partial rows, and proves an
   identical fresh join through restored routing. Forward-only v33 exhausts
   that join under simultaneous all-owner memory pressure and one matched
   exact-group link cut, then heals both domains and recovers. Forward-only v34
   matches the remote link first, cancels the alternate worker while all-owner
   memory pressure remains active, prevents canceled-worker completion, heals
   both domains independently, and recovers exactly.
   Forward-only v35 cancels the selected worker without completion, then
   destroys and reconstructs that exact production owner and proves an
   identical join plus direct rebuilt-endpoint read.
   Forward-only v36 establishes the exact ordered two-table public global-query
   baseline. Forward-only v37 cancels after the first production result is
   assembled, requires typed cancellation, handler drain, and no second
   canceled-request result, then proves exact recovery.
   Forward-only v38 revokes live authority for the second table after that
   first result, requires an exact 403 with no protected payload, restores
   policy, and proves the exact ordered recovery response.
   Forward-only v39 cuts the registered coordinator-to-tenant-owner query
   stream after that result, requires one semantic fault match and the exact
   retryable 503 without partial output, heals, and proves exact recovery.
   Forward-only v40 destroys that exact tenant-owner process after the first
   result, requires the same exact no-partial 503, reconstructs its stable
   DataServer and public/Raft listeners, requires the rebound endpoint to serve
   the durable tenant document directly, and proves exact recovery.
   Forward-only v49 binds node 1's actual `ApiHttpServer` cache to the same
   shared-node service-rate model as DataServer, graph, and serverless work. It
   requires one exact slowed producer/coalesced-wait flight with logical
   deadline expiry, one retained hit after healing, then exact DataServer
   reconstruction, an empty replacement cache, one recomputation, one retained
   hit, one stale pooled reconnect, and a durable rebound read. Four owned
   results, exact pre/post-restart byte ledgers, zero in-flight work/effects,
   cluster visibility, cleanup, and fresh-world replay remain required in the
   120,000-transition Debug and ReleaseSafe gates.
   Forward-only v42 composes the production replication runner with public
   routing, DataServer Raft, and index visibility. It proves exact slowed then
   healed snapshot/stream costs, three accepted batches, visibility through
   every public coordinator, cleanup, and fresh-world replay in 160,000-
   transition Debug and ReleaseSafe gates.
   Forward-only v43 interrupts after that first accepted batch, changes the
   source schema, resumes from durable status with exactly one duplicate batch,
   and completes CDC plus every cluster oracle in 180,000-transition Debug and
   ReleaseSafe gates.
   Forward-only v44 destroys and reconstructs the current target leader before
   the next batch, requires the stopped-endpoint and bounded pooled-reconnect
   failures, then proves exactly three successes plus durable local, direct
   public, and all-coordinator recovery in 220,000-transition Debug and
   ReleaseSafe gates.
   Forward-only v45 fails the first actual provider query after durable
   preparation, closes that owned session, resumes through a strictly newer
   session, and preserves exactly three target successes plus every cluster
   oracle in 220,000-transition Debug and ReleaseSafe gates.
   Forward-only v46 revokes the lease after durable snapshot offset 1,
   requires exact `CdcWorkLeaseLost` plus sequential session replacement, and
   resumes without a duplicate target batch while production charging remains
   active in 220,000-transition Debug and ReleaseSafe gates.
   Forward-only v47 loses ownership after target apply but before checkpoint
   publication, requires durable offset 0 plus one exact idempotent replay,
   and completes every cluster oracle with four target successes in 240,000-
   transition Debug and ReleaseSafe gates.
   Forward-only v48 publishes exact source config v1 and v2 plus both
   authority claims through metadata Raft, rejects authority A with
   `ReplicationSourceConfigChanged` before offset 1 publication, retires A
   under authority B, and completes every cluster oracle with four target
   successes in 260,000-transition Debug and ReleaseSafe gates.
   Forward-only v52 drives one selected DataServer capacity source to zero at
   the real persistent-cache reservation consumer, proves exact denial and
   public-read continuity, heals the same volume, and proves one reservation,
   durable write, release, and cross-node recovery. Forward-only v53 currently
   exact-replays only the bounded public managed-index pending/reconciliation
   lifecycle. Next complete its provider retry, coherent coverage/replay
   publication, durable owner reconstruction, all-node semantic recovery, and
   packet-level replay stabilization; then add cancellation under storage faults, graph/query
   and managed-index disk pressure, simultaneous
   process loss before cancellation drain, and other fault combinations, plus the
   remaining topology breadth, broader disk/socket/short-write
   surfaces, and coordinator, metadata, multi-owner, storage, process, and
   restart overlap variants; then disjoint placement, retained-history pressure,
   snapshot/derived-state recovery, and partitions.
2. **Deepen public distributed operations.** V19 adds a public inner `_id`
   join over two independently owned right groups before, during, and after an
   active split, with exact no-partial evidence and typed ownership retry. V20
   adds finalizer takeover after ambiguous persisted completion. V24 adds
   public production-owner document hydration; v25 adds one in-flight public
   cancellation and recovery shape; forward-only v27 adds authenticated
   cross-table permission revocation inside a live request plus concealment,
   restoration, and exact recovery; forward-only v28 publishes a real split
   behind the retained source snapshot, exhausts both topology attempts with a
   typed no-partial 503, and proves a fresh exact recovery; forward-only v29
   proves cancellation while real outstanding hydration is blocked by one
   scoped transport outage, followed by healing and exact recovery;
   forward-only v30 proves public durable-shuffle cancellation reaches one
   outstanding partition worker and that a clean retry drains; forward-only
   v31 proves one pre-publication worker failure retries the same partition on
   a different production group with an exact ledger and complete result;
   forward-only v32 destroys and reconstructs the exact serving process,
   rejects the original operation without partial rows, and proves an
   identical fresh join through restored routing; forward-only v33 exhausts
   the original join while real all-owner memory saturation overlaps one
   matched exact-group link cut, then heals both domains and completes an
   identical request; forward-only v34 matches the remote link first, cancels
   the alternate worker while real all-owner memory saturation remains active,
   prevents canceled-worker completion, heals both domains, and completes an
   identical request; forward-only v35 cancels the selected worker without
   completion, destroys and reconstructs its exact owner, and proves public
   plus direct-endpoint recovery. Add cancellation under storage faults, disk
   pressure, simultaneous process loss before cancellation drain, and other fault combinations;
   right/nested/foreign and multi-range-left joins; additional
   overlapping-owner fault shapes, and global-query topology, storage,
   resource, coordinator or metadata process loss, multi-process loss, and
   broader transport/overlap-fault histories;
   every incomplete fanout must continue to fail closed. The former partial-
   200 production traversal now has focused regressions and a green exact-
   replay gate; keep that invariant while adding topology and transport faults.
   V53 now begins the first managed-index progressive-readiness lifecycle with
   public mutation, pending evidence, and production reconciliation. Complete
   and exact-replay the retryable provider recovery, coherent coverage/replay
   watermarks, owner reconstruction, and semantic reads from every node before
   promoting it. Then extend it with atomic-versus-progressive policy,
   rate-limit/disk/cancellation overlap, alias fencing, and crashes inside each
   durable-generation/catalog/readiness publication gap.
   Generalize v13's synchronous applied-index/derived-visibility barrier across
   every production strong-read owner. The type system now prevents generic
   managed-Raft initiation from being passed as a read barrier, replicated
   DataServer startup fails closed until its applied-state barrier is wired,
   and direct non-Raft state is explicitly marked already safe. The three-owner
   DataServer history now covers follower routing, leader change, timeout,
   cancellation, group retirement, and derived-state visibility. Audit every
   remaining callback implementation and add this matrix only where the owner
   has a distinct blocking or visibility boundary.
3. **Compose independently proven fault domains.** V21 promotes the first
   production-owner link-plus-resource overlap under one quiet-suffix oracle.
   V52 adds one selected-node disk-capacity denial/healing path at the real
   persistent-cache reservation consumer. Co-locate HA and the data plane,
   add managed-index and graph/query disk-pressure combinations, broaden the
   v22 listener-pressure targets, and extend that algebra across storage,
   restart/process, serverless lease/object-store, and multi-owner faults.
4. **Broaden workloads at those same seams.** V51 adds two disjoint
   authenticated table identities with concurrent authorized work and exact
   bidirectional denied-read, denied-write, and absence evidence. Next add
   row-level tenant scoping and identity mutation races,
   remote-content secret rotation at actual request use, remote generation and
   reranking adapters, production metadata-admin mutations, MCP/A2A
   orchestration, cloud-auth refresh/signing, bounded extension invocation, and
   client/background fairness.
5. **Operationalize the self-contained platform.** Gather completed-run evidence
   from the scheduled sharded campaigns and replay-validated corpus retention;
   add routine quarantine review, usage indexing, notifications, dashboards,
   and search-quality regression tracking.
6. **Keep distributed fidelity explicit.** Continue using the integrated
   in-process multi-node mode for production owners that borrow `std.Io`. Add a
   repository-owned federated agent/broker only when separate-address-space or
   live mixed-version behavior is the requirement; classify unmodified
   container runs as differential, never exact replay. Keep compiler coverage,
   datagrams, server TLS, and guest-kernel interception conditional, and do not
   build a hosted UI or deterministic-hypervisor clone merely for nominal
   parity.

The detailed backlog behind that summary is:

1. Deepen the now-green P0 full-cluster active-transition path. The current
   green focused histories cover rollback and fresh retry through one owner,
   then networked forwarding, leader transfer, one
   owner restart, terminal retry, every-replica transition/watermark
   convergence, and document equality across three owners and replicated
   groups on one `VoprIo`. The merge protocol provides the durable v3 capability
   barrier, source prepare/finalize fencing, receiver checkpoints,
   catalog-independent replay identity, Raft-mediated copy, snapshot-carried
   controls, replicated observation, and actor-owned teardown. The same
   focused history now admits a new split generation, bootstraps it, performs a
   public post-bootstrap write, replays that delta after leader changes,
   finalizes cutover, restarts one owner, and verifies both ranges and all
   documents before fresh-state replay. The metadata harness now has a focused,
   regression-tested external-data-plane mode that keeps metadata replicas live
   while refusing to instantiate projected data placements as shadow hosted
   replicas. V11 puts those production owners behind that handoff, publishes
   their endpoints through the quorum, and elects every initial data-group
   leader over production HTTP. Model v5 and a one-request/one-completion
   control handshake remove the earlier task/socket/content/capacity and
   status-round replay drift. Lifecycle evidence then showed that the apparent
   Raft failure occurred before proposal acceptance: a native mutex wait pinned
   the borrowed scheduler, missing internal credentials triggered fail-closed
   503s, and host realtime escaped into signed packet contents. Those three
   defects are fixed, the no-blind-retry contract remains intact, and v11 now
   passes 30/30. V12's production `HostedShardOperationAdapter` reaches
   finalized and published split state plus the post-split read. Model v6 fixes
   the first socket-derived retry-owner chain; stable per-node transition retry
   jitter closes the later host-entropy escape. The complete 320,000-transition
   ReleaseSafe gate passed 15/15 under record and fresh-state replay at its
   cited checkpoint. V14 now keeps the production-owner graph and serverless
   clients co-scheduled with that transition and passes 15/15 on the current
   tree. V15 adds a real remote-owner graph-transport cut during that split,
   proves typed fail-closed recovery, and passes 15/15 at 450,000 transitions.
   V16 proves the same fail-closed/recovery contract while tearing down and
   reconstructing the selected production owner and both stable-port listeners;
   its 650,000-transition gate passes 15/15. V17 proves the production HTTP
   client/server pair resumes a one-byte scoped request write without losing or
   duplicating graph semantics and passes 15/15 at 500,000 transitions. V18
   proves that Raft apply, public point reads, public graph queries, and split
   control recover after all production memory envelopes are saturated and
   passes 15/15 at 550,000 transitions. V22 then denies every new connection
   at one exact public listener, requires a fresh production HTTP client to see
   `ProcessFdQuotaExceeded` without handler ingress, heals the limit, and proves
   fresh-client, graph, split, and cleanup recovery at 500,000 transitions.
   V52 separately proves one selected-node persistent-cache capacity denial,
   healing, and public-read continuity. Next add broader restart, short-write,
   and socket-pressure targets/overlaps, remaining topology modes,
   managed-index and graph/query disk-pressure combinations, and
   overlapping process/link/storage/resource faults, and
   remove the current co-location assumption with
   disjoint donor/receiver replica sets, page
   retained delete-history replay within an explicit resource budget, inject
   partitions, and prove snapshot install rehydrates each live DB owner and its
   derived graph/index state as well as the Raft projection. Those additions
   broaden the promoted seam; they are not prerequisites for its current
   active-transition claim.
2. Deepen the public-HTTP/cross-range graph composition. Its public request now
   covers parsing, planning, two-range expansion fanout, depth-two assembly, and
   an actual second-range leader restart between completed rounds. A separate
   exact-replayed mode now interrupts the real internal transport after round
   one, proves that the ordinary success schema is never used for an incomplete
   graph, returns the typed retryable
   `distributed_query_unavailable` 503, heals the fault, and requires a complete
   retry. A ninth mode now performs a production-coordinator merge across the
   actual donor/receiver leader roots, requires topology retry exhaustion to
   fail closed, finalizes the merge, and requires a complete recovered graph.
   V13 routes the same complete depth-two shape across production
   `DataServer`/data-Raft owners and exact-replays the repaired ReadState/full-
   index visibility contract. V14 composes that seam with item 1's active
   split and proves complete-or-fail-closed behavior in flight plus complete
   traversal after cutover. V15 now adds one scoped remote-owner transport
   failure during the active split, a no-partial typed 503, healing, and a
   complete post-cutover traversal. V16 substitutes a real stable-endpoint
   remote-owner restart, reconstructs DataServer/public/Raft ownership, and
   reaches the same terminal traversal. V17 substitutes one scoped short write
   and requires the in-flight graph itself to remain complete. V18 overlaps
   all-owner memory denial/recovery and requires both in-flight and post-cutover
   graph completion. V19 then runs the public two-right-owner join before,
   during, and after the split and requires exact two-row/profile evidence.
   V20 forces a 64-row durable shuffle, fails the first finalizer after result
   persistence, and proves a different owner imports that cached result and
   completes with an exact two-attempt ledger. V21 overlaps all-owner memory
   pressure with the selected second-hop graph link cut, proves simultaneous
   activation and a typed no-partial response, heals both, and completes the
   resource, graph, split, and cleanup oracles. V22 separately adds exact
   selected-listener new-connection denial before handler ingress, healing, and
   fresh-client recovery while the active split remains nonterminal.
   V52 separately proves selected-node persistent-cache disk denial/healing.
   Next add broader restart/short-write/socket targets and overlaps,
   remaining topology modes, managed-index and graph/query disk-pressure
   combinations, and storage/process/restart
   overlaps. V24/v25 now compose public document hydration and one clean
   cancellation/recovery shape; forward-only v27 composes permission revocation
   at the live foreign-table authorization boundary and exact recovery; v28
   composes retained-source-snapshot rejection and bounded retry exhaustion
   across real split publication; forward-only v29 composes cancellation with
   one observed scoped transport outage; forward-only v30 composes public
   durable-shuffle cancellation with one outstanding partition worker and a
   clean retry; forward-only v31 composes a pre-publication partition-worker
   failure with exact same-partition failover to another production group;
   forward-only v32 composes actual serving-process destruction,
   reconstruction, typed no-partial rejection, and complete fresh recovery;
   forward-only v33 composes real all-owner resource saturation with one
   matched exact-group link cut, typed no-partial retry exhaustion,
   independent healing, and complete fresh recovery; forward-only v34 matches
   that remote fault before canceling the alternate worker under all-owner
   memory pressure, prevents canceled-worker completion, heals independently,
   and recovers exactly.
   Forward-only v35 cancels a selected worker, then destroys and reconstructs
   that exact production owner before proving public and direct-endpoint
   recovery.
   Next compose cancellation with storage faults, disk pressure, simultaneous
   process loss before cancellation drain, and other fault combinations.
   Extend v20/v30/v31/v32/v33/v34/v35 across right/nested/foreign joins, multi-range
   left inputs, cancellation combined with remaining faults, and additional
   overlapping owner faults; add global queries
   with the same fail-closed publication rule. Introduce an explicit
   partial-response schema only if product semantics ever require partial
   results. V53 currently proves a bounded public managed-index pending and
   reconciliation lifecycle. Complete and stabilize its pending-to-ready,
   post-publication reconstruction, and all-node query path first; then extend
   that history across atomic and progressive policy, rate-limit and disk
   pressure, cancellation, alias fencing, and crashes between generation
   durability, catalog publication, and readiness reporting.
3. Co-locate production HA and data-plane owners, extend the integrated
   node-memory and selected-listener denial/recovery modes to disk-capacity
   pressure and broader socket targets, and combine
   directional link, storage-crash, restart, serverless lease/object-store,
   and pressure faults. V23 now composes reversible DataServer work,
   distributed-graph, and serverless operation costs in the real cluster as a
   distinct fault from the CPU-work exhaustion budget. Forward-only v49
   composes query-cache request, producer, coalesced-wait, deadline, retained-
   hit, owner-reconstruction, recomputation, reconnect, and durable-read
   evidence on node 1's actual public `ApiHttpServer`/DataServer domain before
   and after healing. Forward-only v42
   composes clean replication snapshot/stream work against that production
   cluster domain. Forward-only v43 composes schema-change interruption and
   exact duplicate resume against the same owners. Forward-only v44 composes
   target-owner reconstruction and long-lived-client reconnect against those
   owners. Forward-only v45 composes provider-query failure, exact session
   replacement, and durable resume without extra target work. Forward-only v46
   composes durable-checkpoint lease cancellation with delegated charging and
   no duplicate target work. Forward-only v47 composes apply-to-checkpoint
   ownership revalidation, stale-offset fencing, and one idempotent replay.
   Forward-only v48 composes metadata-Raft source publication, exact authority
   replacement and predecessor retirement with that same charged path.
   Forward-only v50 composes candidate-manifest durability, an authoritative
   generation cutover, the losing object-backed progress CAS, stale-derived
   rejection, and public version-4 visibility with the ordinary production
   public/DataServer/Raft owners active on the same `VoprIo`.
   Forward-only v51 composes two valid disjoint table identities with
   concurrent cross-node authorized work, exact bidirectional 403 read/write
   denials, and exact owning-identity 404 absence checks on the same production
   public/DataServer/Raft owners.
   Next overlap
   operation-specific slowdown
   with cross-domain ownership, public deadlines, and the existing
   link/storage/resource/restart faults. The real
   metadata/placement Raft wire hop, serverless public catalog path, memory-
   pressure recovery, and quiet cluster-wide suffix are complete at their
   named seams; broaden their fault combinations after item 1 instead of
   rebuilding them.
4. Extend full-cluster workload dimensions instead of multiplying suites:
   v51 now supplies authenticated table-level multi-tenancy in addition to
   two-table isolation; next add row-level tenant scoping and identity mutation
   races, concurrent range split and replicated merge routing changes,
   explicit per-node disk-capacity and broader socket interference in addition
   to current memory and selected-listener pressure, and fairness between
   clients and background workers.
5. Close the remote-content live-secret use boundary: resolve preserved
   credential and header references at the actual scraping/object request,
   rotate during an in-flight request, and prove coherent per-request
   generations across retry, cancellation, refresh, and crash/reopen.
6. Maintain all focused integrated suites as production seams evolve. A gate
   is only "integrated" when the scenario module is forced into test discovery,
   its focused command executes at least one matching test, exact replay passes,
   and the command remains a dependency of `vopr-test`.
7. Adopt the integrated registered-deployment composer beyond full-cluster in
   the HA, data-plane, distributed-transaction, and serverless suites so node
   identity, readiness, fault scope, local storage/resource ownership, and
   quiet-suffix obligations remain uniform as those compositions converge.
8. Maintain the command composer, determinism audit, phased health adapters,
   recorder/event queries, debug recipes, results/index APIs, corpus merge, and
   injected-bug benchmarks. Wire their already implemented artifacts and
   recurrence/rarity reports into nightly retention and dashboards.
9. Expand the determinism audit from an explicit source manifest toward the
   transitive production call graph reachable from every borrowed-`std.Io`
   scenario seam. Fail closed on pointer-derived ordering or identity, host
   clocks, native threads or I/O, filesystem escapes, native libraries, and
   unordered iteration unless a narrow reviewed differential exception applies.
   Add a manifest entry whenever a replayable source is exported, audit newly
   reachable callees continuously, and preserve Threaded/physical-backend
   differential tests to detect simulator drift. The current manifest gate is
   real and green; transitive coverage is not yet complete. Treat optional or
   default `Threaded` ownership as an explicit boundary: A2A/MCP orchestration,
   cloud-auth and object-store constructors, and remote-provider adapters must
   receive the scenario's borrowed `std.Io` before they join an exact-replay
   composition. Their convenient native fallbacks remain physical differential
   paths and are not VOPR evidence. In particular,
   source-to-source cache transfer and stale-cache pruning still use address-
   ordered dual locking in production paths not exercised by the current v12
   topology. Before those paths become replayable scenario transitions, give
   each owner a stable semantic lock key or route both operations through one
   coordinator; do not simply reverse a lock order and reintroduce deadlock.
10. Add live rolling mixed-version cluster operation only after the repository
   has two runnable compatible binaries and an explicit upgrade contract.
   Current artifact compatibility and golden-reader campaigns remain the
   deterministic prerequisite, not a claim of live mixed-binary execution.
11. Add the federated VOPR agent/broker only for a demonstrated separate-
   address-space or mixed-binary requirement. Add datagram, server TLS, guest-
   kernel interception, or compiler-guided campaigns only when the conditional
   prerequisites above become real. A hosted graphical debugger and a
   container/hypervisor clone remain non-goals.

The defects already found—lifetime errors, listener shutdown races, provider
publication and teardown races, ignored generation timeouts, narrowed HTTP
cancellation, query-cache cancellation, malformed reranker acceptance,
host-only retry sleep, hidden Threaded I/O, virtual socket accounting, FIN
ordering, teardown deadlocks, TLS fail-open behavior,
ReleaseSafe fiber identity corruption, lost socket data, stolen wakes, stale
return-by-value provider pointers, cross-allocator capacity ownership, graph
wire-contract mismatch, unconditional graph hydration, split-action lane
double-release, and host-clock leakage—
demonstrate that extending VOPR across
remaining orchestration boundaries is likely to pay off. See
[Defects Found](#defects-found) for the complete inventory.
