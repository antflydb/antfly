# VOPR: Deterministic Autonomous Testing for Antfly

Status (2026-08-28): the common VOPR engine and deterministic `std.Io` runtime
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
`lib-data-runtime-test` 125/125 with no leaks. It includes production-envelope
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

## Purpose

VOPR is Antfly's self-contained deterministic simulation platform. It explores
workload, scheduling, fault, and parameter choices; runs real state machines
against modeled operating-system services; evaluates named properties; retains
interesting histories; and produces exact replay, reduction, formal-trace, and
debugging artifacts.

The reusable engine is application independent and lives in `lib/vopr`.
Antfly scenarios, fixtures, audits, command policy, and production adapters
live under `pkg/antfly/src`. Production components reach the simulator through
narrow interfaces, primarily `std.Io`; they never import the explorer.

The goal is not to reproduce a general-purpose hypervisor. VOPR virtualizes the
nondeterminism Antfly and registered in-process dependencies actually use. A
native process or real-network run remains a differential compatibility layer,
not a prerequisite for deterministic search. This differs deliberately from
Antithesis's ability to run arbitrary containerized services inside a
deterministic machine: VOPR obtains deeper application scheduling and durable-
state visibility by requiring production code to cross explicit `std.Io` and
ownership seams. A deployment-shaped `full-cluster-vopr-test` now composes a
three-node metadata quorum, two-placement hosted data ranges on node-local
replica roots, two tables, three production public API HTTP listeners, four
concurrent clients, and a serverless workflow fixture with its own production
public catalog listener on one
`VoprIo`. The worker publishes into the same object-backed catalog served by
that listener; the serverless object catalog and metadata placement catalog
remain intentionally distinct production domains. Remaining distributed work
no longer starts with that substitution: the production-owned v12 history now
joins the real metadata quorum, three `DataServer`/data-Raft owners, public
clients, two tables, serverless catalog, and a metadata-driven active split on
one `VoprIo`, and its complete deep gate exact-replays. V14 now puts the v13
graph under that split and exact-replays public work before, during, and after
cutover. The next step is to put v9's nine-fault breadth on those production
owners, then add co-resident HA, deeper fault overlap, joins/global queries,
and workload breadth. This is a composition/fidelity gap, not a missing
deterministic-distributed foundation.

## Conformance Status

Completion claims use three levels:

- **Integrated** means a production or reusable path is exercised, exact replay
  is verified, and an explicit build/CI gate owns the contract. Fast gates are
  included by `vopr-test`; production-sized histories may use a named deep tier
  rather than silently turning the fast aggregate into a multi-gigabyte,
  half-hour job.
- **Executable foundation** means the reusable mechanism and focused tests or
  command exist, but campaign-wide adoption, scenario snapshots, or operational
  policy remains incomplete.
- **Operational follow-up** means correctness code exists and the remaining work
  is CI retention, corpus breadth, dashboards, or search-quality measurement.

An executable unit test alone is not called fully implemented.

This document does not use **integrated** to mean feature-complete for every
possible deployment. It means the stated production seam and modes meet the
definition above. Rows explicitly name residual boundaries when a broader
phrase such as "full cluster," "distributed query," or "provider" could imply
more. In particular, VOPR does not yet run arbitrary unmodified binaries,
sidecars, or live mixed-version clusters, and the full-cluster campaign does
not yet co-reside every independently tested HA/data-plane owner.

### Completion-Claim Audit

The implementation is not the complete roadmap. Claims are valid only at the
following boundaries:

| Claim family | Audit result | Important exclusion |
| --- | --- | --- |
| Reusable VOPR engine, `VoprIo`, replay, reduction, properties, saved cross-run event-set queries, bounded live event streaming, flight recording, local reports, debug recipes, fault/service-rate algebra, and search-quality fixtures | Implemented and exercised by the focused engine/meta gates named below; production query-cache operations, DataServer Raft and LSM-maintenance turns, distributed graph fanout, replication snapshot/stream steps, and serverless publish/compaction rounds are reviewed service-rate charge seams. V23 composes DataServer, graph, and serverless charging in one production-owner deployment history; v42 adds clean snapshot/stream work on the real public/DataServer/Raft path; v43 composes schema-change interruption, durable resume, and exact duplicate application; v44 composes target-owner restart and bounded client reconnect; v45 composes provider-session failure and replacement; v46 composes durable-checkpoint lease cancellation; v47 composes stale-owner rejection in the apply-to-checkpoint gap; v48 composes exact-cutover source-catalog and authority rotation through metadata Raft; v49 charges the actual node-owned `ApiHttpServer` cache and composes a logical deadline, exact owner reconstruction, empty-cache proof, recomputation, pooled reconnect, and durable read without disabling the other charged owners | Nightly sharding, retention, review, notifications, and broader production adoption and cross-domain combinations of service-rate charging are operational or ongoing work |
| Metadata, Raft, HA, transaction, data-plane, storage, backfill, supervision, authentication, serverless, cache, provider, generation/reranking, and query suites | Implemented at each row's named production seam and fault vocabulary | The suites are not all co-resident in one deployment history |
| Distributed graph | Focused production coordinator paths, the public hosted-source composition, v13's static production-owner graph, v14's production-owner graph during active split, v15's fail-closed owner-transport cut, v16's fail-closed remote-owner restart, v17's exactly observed recoverable next-owner short write, v18's three-owner memory denial/recovery, v21's simultaneous selected-link/all-owner-memory failure, v22's selected-listener socket denial/recovery during that split, v24's public production-owner document hydration, v25's public in-flight hydration cancellation/recovery, v27's public cross-table in-flight permission revocation/conceal/restore, v28's public stale-source-snapshot rejection/bounded retry exhaustion, and v29's cancellation with real outstanding hydration under a scoped transport outage are implemented | V9's remaining topology breadth, disk-capacity pressure, broader socket-pressure overlaps, broader partial-write surfaces, and storage/process/restart overlaps are not yet on the production owners; cancellation under resource/storage/process/restart and multi-fault combinations and global-query fault/recovery breadth are not complete. V19's narrow distributed join is audited separately below |
| Distributed join | V19 implements one public inner `_id` join with two left rows and two independently owned right ranges, exact no-partial response validation, typed ownership retry, and before/active-split/post-publication observations. V20 forces a 64-row durable shuffle, fails the first finalizer after result persistence, and proves another owner imports the cached result and completes with an exact two-attempt ledger. Forward-only v30 cancels a public durable shuffle only after an internal partition worker starts with a real request token, requires that worker not to complete, and proves an exact clean retry with terminal worker accounting. Forward-only v31 injects one pre-publication partition-worker failure and proves exact same-partition failover to a different group with a one-retry ledger and all 64 rows. Forward-only v32 destroys the exact process that starts the partition, requires typed fail-closed exhaustion without partial rows, reconstructs its stable identity and endpoints, and proves an identical fresh 64-row join plus a direct rebuilt-endpoint read. Forward-only v33 exhausts the original operation while real resource saturation overlaps a matched exact-group network cut, requires typed no-partial rejection, heals both domains independently, and proves an identical complete retry. Forward-only v34 first matches a remote worker-link outage, then cancels the alternate worker while every production memory envelope is full, requires zero canceled-worker completion, heals both domains, and proves an identical complete retry. Forward-only v35 cancels at the real worker boundary, then destroys and reconstructs that exact production owner before proving an identical join and direct rebuilt-endpoint read | Cancellation under storage faults, disk pressure, simultaneous process loss before cancellation drain, or other fault combinations; authorization and generation mutation; right/nested/foreign joins; multi-range left inputs; overlapping owner faults beyond the v33/v34 resource-plus-link shapes; and global-query topology/storage/resource, coordinator or metadata process loss, multi-process loss, and broader transport/overlap composition beyond v40 is not complete |
| Full-cluster v9 | The documented metadata/placement Raft, hosted data roots, public/serverless HTTP, graph-fanout, resource, merge-coordinator, replay, and cleanup behaviors are implemented | It is not yet a cluster of production `DataServer` owners. Data writes and merge structural actions are not proven through replicated DataServer apply on every replica |
| Full-cluster v11-v49 production owners | **Integrated at the explicitly named production-owner seams.** V11 joins the real metadata quorum, three production `DataServer`/data-Raft owners, real HTTP/Raft, two-table public clients, and the serverless catalog on one `VoprIo`. V12-v22 promote active split, graph, transport, restart, short-write, resource, join, overlapping-fault, and socket-pressure seams. V23-v40 add the cited shared costs, hydration, cancellation, authorization, stale topology, durable workers, reconstruction, and global-query recovery histories. V42 borrows the production replication runner into that cluster and proves clean snapshot-to-stream work through alternating public coordinators, routing, leader forwarding, DataServer Raft, and index visibility. Forward-only v43 changes the source schema after the first accepted snapshot batch, requires the first attempt to fail, resumes from durable status, accepts exactly one duplicate batch, and completes streaming with every document visible from every public node. Forward-only v44 destroys the current target leader before the next batch, requires the exact stopped-endpoint and bounded pooled-reconnect failures, reconstructs its stable identity/listeners, proves local durable-row recovery and a direct rebound public read, and resumes to three exact successes plus all-node visibility. Forward-only v45 fails the first provider query after durable preparation, closes that exact session, opens a newer session, and resumes with three target attempts and three successes. Forward-only v46 revokes the work lease after durable snapshot offset 1, requires exact source-session replacement, and resumes with no duplicate target batch. Forward-only v47 revokes ownership after target apply but before checkpoint publication, keeps durable offset 0, and requires one exact idempotent replay for four target successes. Forward-only v48 changes the exact-cutover source catalog through metadata Raft after the first target apply, rejects authority A before offset 1 publication, atomically claims authority B against A, retires A, and completes after one idempotent replay. Forward-only v49 replaces the earlier adjacent cache fixture with node 1's actual `ApiHttpServer` cache, requires exact slowed coalesced deadline expiry, retained-hit behavior, process reconstruction, an empty replacement cache, one recomputation, one bounded stale pooled-connection failure, and a durable rebound read. The cited v42-v49 complete gates use record plus fresh-world replay, named properties, cleanup, and leak checks; their 160,000-, 180,000-, 220,000-, 220,000-, 220,000-, 240,000-, 260,000-, and 120,000-transition Debug and ReleaseSafe gates pass 15/15. The focused LSM gate remains the exact LSM-maintenance witness | Remaining breadth includes v9 topology combinations; disk-capacity and broader socket/short-write surfaces; cache topology/link/storage/resource overlap; additional replication source-crash/cancellation and topology-change timings, metadata leadership loss during cutover, additional target-crash timings, and overlapping-fault variants in this deployment; cancellation under storage/process and richer multi-fault combinations; arbitrary coordinator/metadata or multi-owner loss; disjoint placement; retained-history pressure; snapshot/derived-state rehydration; HA co-residency; broader join/global-query forms; and richer storage/process/link/resource overlap |
| Replicated data-Raft merge/split protocols | **Integrated focused seam.** The current multi-owner checkpoint implements merge v3 capability/barrier activation, split-delta predecessor fencing behind durable protocol v4, source fencing, receiver checkpoints, catalog-independent replay identity, copied-document proposals, snapshot-carried controls, replicated observation, merge-to-split, post-bootstrap write, sparse delta catch-up, cutover, restart, routed terminal retry, and every-replica range/document/transition/watermark convergence. Its record and fresh-state replay pass | Disjoint replica sets, retained-history pressure, derived graph/index equivalence, snapshot-install rehydration, and co-resident HA/data-plane/serverless faults remain unproven |
| Antithesis-style distributed execution | Registered in-process node/process/storage/resource/link domains and exact replay are implemented | Arbitrary separate-address-space binaries, sidecars, DNS, kernels, and live mixed binaries require the conditional federated-agent or native differential modes described below |

Therefore “integrated” must never be shortened in release notes or reviews to
“all planned VOPR work is fully implemented.” In particular, a test using
`ApiHttpServer`, `HostedProvisionedTableWriteSource`, and a node-local replica
root is not evidence that the `DataServer` owner, its data-Raft proposal/apply
state machine, or follower recovery participated.

| Capability | Implementation evidence | Verification |
| --- | --- | --- |
| Stable structured choices and exact clean-world replay | `lib/vopr/src/choice.zig`, `runner.zig`, `replay.zig`, canonical `vopr-trace-v1`; a mismatch reports its byte offset and bounded first differing JSON lines without weakening byte equality | `zig build vopr-engine-test` |
| One-transition scheduling and typed termination | `scheduler.zig`, `scenario.zig`, `outcome.zig` | `zig build vopr-engine-test` |
| Antfly-independent runtime boundary | `runtime.zig`, `sim_runtime.zig`; `VoprIo` composes the narrow atomic executor into its scheduler; Antfly `DurableJobLane` adapter | `zig build vopr-engine-test vopr-runtime-test` |
| Deterministic `std.Io` tasks and synchronization | `vopr_io.zig`, `vopr_io_task.zig` | `zig build vopr-engine-test` in Debug and ReleaseSafe |
| Typed strong-read capabilities | `raft/read_gate.zig` separates enqueue-only `ReadIndexRequester` from synchronous `ReadSafetyBarrier`; managed Raft services expose only initiation, while public table-read sources accept only the barrier type. DataServer implements the barrier with canonical matching-group ReadState plus applied-index completion, starts replicated sources with a fail-closed unavailable barrier, and installs `alreadyReadSafeBarrier` only after explicitly selecting direct non-Raft ownership. The three-owner production history exact-replays follower success/typed stale-leader rejection, old-to-new leader transfer and retry, logical timeout, cancellation cleanup, state-machine group retirement, and graph/full-index visibility. The former readable-lease API, no-op name, service adapters, and metrics were renamed or deleted directly; there are no compatibility aliases | `zig build raft-test root-test lib-data-runtime-test` |
| Modeled files, durability, persistent sector corruption, torn synchronization, streams, datagrams, processes, global quotas, and endpoint-stable reversible listener connection limits | `vopr_io_file.zig`, `vopr_io_net.zig`, `vopr_io_process.zig`; virtual streams distinguish an ordered write-half FIN from full peer read abandonment and hard reset. Both FIN and full-close control remain ordered behind prior payload, while only read abandonment/reset satisfies HTTPX's production-neutral H1 disconnect probe. The forward-only API is named for peer abandonment rather than generic disconnect, full close has its own stable transition identity, and this semantic change directly advances the virtual-OS replay model to v7. Endpoint/payload outage matching exposes a monotonic post-heal witness so scenarios can prove production traffic crossed an armed boundary | `zig build vopr-engine-test data-server-vopr-test production-cluster-graph-cancellation-vopr-test production-cluster-graph-cancellation-transport-fault-vopr-test` |
| Stable optional safepoints | `vopr_io_instrumentation.zig` | `zig build vopr-engine-test` |
| Clocks, timers, storage completions, and lifecycle faults | `time.zig`, `clock_fault.zig`, `fault.zig`, storage `sim_runtime.zig` | `zig build vopr-engine-test storage-vopr-runtime-test` |
| Properties, observations, semantic coverage, cross-revision corpus quarantine, property history, and guided search | `property.zig`, `observation.zig`, `coverage.zig`, `corpus.zig`, `explorer.zig` | `zig build vopr-engine-test vopr-benchmark` |
| Integrated retroactive flight recording and fielded temporal event queries | `flight_recorder.zig`, `event_query.zig`, `debug_recipe.zig`; bounded recordings own structured fields and verbose text outside canonical bytes, support conjunctive field/text filters and before/after windows, and are populated directly by runner-backed and custom metadata/domain replay paths. Every retained/failing campaign writes `.flight.json`, while every debug recipe packages a filtered reduced-replay window | `zig build vopr-engine-test vopr-meta-test`; `vopr events` and `vopr recipe` are argument-taking commands, not standalone test gates |
| Saved cross-run event sets, validation, counting, and live streams | `event_set.zig` validates a versioned forward-only query DAG and evaluates selection, union/intersection/difference/complement, distinct/first/last moment, previous/next, and bounded sequence operations across canonical histories. Its tests execute every operator and reject forward references, malformed operators, duplicate names, v0 formats, and the former ad-hoc selector shape. `vopr events` accepts only this saved-plan format and exact-replayed traces, with repeated `--trace`, `--validate`, and `--count`. Forward-only `vopr-event-stream-v2` serializes into a caller-owned fixed-slot SPSC queue: publication cannot allocate, block, or invoke external code, while one consumer may drain concurrently. Release/acquire publication prevents partial records; power-of-two capacity and wrapping monotonic positions preserve bounded operation across counter rollover. Drop-newest backpressure, oversize records, publication after close, delivery, and sink failures have separate atomically sampled saturating counters. Failed delivery retains the oldest record for retry, and close still permits draining. Tests cover invalid capacity, oversize rejection, concurrent producer/consumer accounting, retry, close, and pressure; runner pressure leaves canonical output byte-identical to an unobserved run. Custom observers remain an expert synchronous interface and must not block or panic | `zig build vopr-engine-test vopr-meta-test` |
| Reversible node and operation service rates | `service_rate.zig` registers unambiguous stable node/operation identities, composes fully checked node/operation costs, charges logical time through borrowed `std.Io`, and heals individual effects by stable fault ID. Accounting distinguishes calls, work units, and logical nanoseconds. Six production-neutral boundaries are integrated: query cache, DataServer Raft and LSM maintenance, distributed graph, replication snapshot/stream, and serverless workflow work. Focused histories prove exact slowed/healed behavior. Full-cluster v23 installs one shared model into DataServer, graph, and serverless owners; v42 adds production replication work on the public/DataServer/Raft path; v43 proves pre-heal snapshot work survives a schema-change interruption and exact duplicate resume; v44 proves the same charged runner survives target-owner reconstruction and bounded reconnect; v45 proves it survives source-session failure and replacement; v46 proves outer lease cancellation composes with delegated charging; v47 adds ownership revalidation between target apply and checkpoint publication; v48 preserves charging while source-catalog and exact-cutover authority transitions cross metadata Raft; v49 installs the query-cache port on the actual node-owned `ApiHttpServer` across deadline expiry, healing, owner reconstruction, and exact recomputation | `zig build vopr-engine-test query-embedding-cache-vopr-test lib-data-runtime-test data-server-vopr-test distributed-query-vopr-test replication-backfill-vopr-test serverless-workflow-vopr-test production-cluster-service-rate-vopr-test production-cluster-query-cache-deadline-restart-vopr-test production-cluster-replication-backfill-vopr-test production-cluster-replication-schema-change-vopr-test production-cluster-replication-owner-restart-vopr-test production-cluster-replication-source-crash-vopr-test production-cluster-replication-cancellation-vopr-test production-cluster-replication-stale-owner-vopr-test production-cluster-replication-topology-change-vopr-test`; cache topology/link/storage/resource overlap and remaining replication fault variants in the deployment remain roadmap breadth |
| Integrated per-history and aggregate run/results API with phased health evidence | `runner.zig`, `report.zig`, `health.zig`, `vopr_io.zig`, `vopr-results`; every runner history samples continuous/recovery/final health without changing canonical trace bytes, exact replay rematerializes the evidence, `VoprIo.healthSnapshot` supplies task/descriptor/storage data, and mature P0/P1 adapters add domain progress, recovery, consistency, allocator/crash classification, and cleanup | `zig build vopr-engine-test vopr-contract-test vopr-registry-test vopr-results` |
| Integrated persistent local run/results index and usage query API | `run_index.zig`, `vopr-index`; atomically persisted `vopr-run-index-v1` projects per-history and aggregate results into canonical run, revision, property, fingerprint, corpus/quarantine, artifact, and budget records. CLI predicates and `vopr-run-index-query-v1` cover every dimension, and the same query renders a static local HTML summary | `zig build vopr-engine-test vopr-meta-test vopr-index` |
| Automatic debug recipes and deterministic corpus merging | `debug_recipe.zig`, callback-based `reducer.zig`, `corpus.zig`; `vopr-recipe`, `vopr-corpus-merge` | `zig build vopr-engine-test vopr-meta-test` |
| Integrated fault composition, structured-choice auditing, and search-quality regression corpus | `fault.zig`, `fault_vopr_io.zig`, `choice.zig`, `explorer.zig`, `benchmark.zig`; precedence drives real `VoprIo` effects in the Parquet-cache suite. Three distinct scheduling, durability, and cancellation defects run under random, guided, spliced, starvation, and checkpoint-assisted policies with replay-before-retention, recurrence, Wilson confidence, logical-cost, and minimal-output evidence | `zig build vopr-engine-test parquet-cache-vopr-test vopr-benchmark` |
| Integrated command-template composition and registered-source entropy audit | `command.zig` implements first/parallel/serial/singleton/anytime/eventually/finally roles with compatibility, exclusion, fault, and quiescence policies; `determinism.zig` combines immediate-choice and borrowed-I/O entropy evidence; Antfly `vopr/determinism_audit.zig` covers every source explicitly registered in its manifest, including every exported VOPR scenario and both legacy metadata replay regions | `zig build vopr-engine-test vopr-determinism-audit`; transitive production-call-graph coverage remains roadmap work |
| Registered deployment topology and quiet suffix | `lib/vopr/src/deployment.zig` validates role dependencies, node/instance identity, directional links, disjoint process/storage/resource domains, typed fault compatibility, readiness, measured resource policy, and per-node quiet acknowledgments. Full-cluster v9 registers four owners, seven role instances, six directional links, and every scenario fault before requiring cluster-wide quiescence | `zig build vopr-engine-test full-cluster-vopr-test` |
| Replay-before-retention campaigns, deterministic workers, bounded counterfactual graphs, and quarantine manifests/raw artifacts | Antfly `vopr/cli.zig` | `zig build vopr-meta-test` |
| Same-fingerprint reduction and reviewed promotion | `reducer.zig`, `fixture.zig`, `vopr-reduce`, `vopr-promote`; scenario ABI changes invalidate old artifacts instead of invoking a migration path | `zig build vopr-engine-test vopr-meta-test` |
| TLA+ export | transaction and Raft `vopr-tla` dispatch | `zig build transaction-vopr-test vopr-meta-test` |
| Ranked and explicitly budgeted counterfactual causality, persistent multiverse identities, clean-replay branching, and scriptable/interactive debug sessions | `causal.zig`, `multiverse.zig`, `debugger.zig`, `collector.zig`, `vopr-debug` | `zig build vopr-engine-test vopr-meta-test` |
| Metadata and acknowledged distributed-data durability | real metadata/Raft paths plus modeled storage | `zig build lib-metadata-vopr-test lib-metadata-vopr-data-test` |
| Per-group Raft scheduling | real `RawNode` message, persist, apply, restart, partition, proposal, and compaction choices | `zig build raft-vopr-test` |
| Storage differential and real-backend campaigns | WAL, LMDB, LSM, persistent index, index manager, and DB split | `zig build storage-vopr-test` |
| HA lifecycle | replication, fencing, promotion, retention, restart, and rejoin | `zig build ha-vopr-test ha-chaos-test` |
| Independent application domains | distributed transaction, data plane, derived workflow, backup/restore, and clock faults | their five focused `*-vopr-test` gates |
| Production public HTTP on deterministic I/O | `vopr/data_server.zig`, `vopr/http_lifecycle.zig`, borrowed `HttpRuntime` and `BackendRuntime` lanes, transport-neutral metadata executor; chunked upload, keep-alive pipeline, streaming response, and half-close | `zig build data-server-vopr-test` |
| Production DataServer replicated merge/split seam | `data/runtime.zig`; the focused rollback/fresh-retry history uses one owner and two groups, while `data-server-transition-vopr-test` chains merge into split across three real `DataServer` owners and three replicated groups over time. It uses public HTTP/Raft listeners, routed merge actions, leader transfer, a public post-bootstrap delta write, replicated bootstrap/catch-up/finalize, owner restart, catalog-independent replay, exact routed terminal retry, every-replica range/transition/watermark convergence, document equality, actor-owned teardown, and fresh-root replay of the recorded actor/time schedule on one `VoprIo`. Clock-only stutter is normalized at the explicit physical LSM differential boundary; no different actor may execute, and a recorded actor that does not become ready within the bound is replay divergence. A regression preserves exactly-once split-action lane release when an inline durable job fails | `zig build data-server-transition-vopr-test`; broader `data-server-vopr-test lib-data-runtime-test lib-data-storage-test` gates remain required before release |
| Production background ownership and admission | `background_runtime.zig`, `vopr_durable_job_lane.zig`; transaction recovery, TTL, enrichment, text merge, sparse compaction, resolution, promotion, LSM maintenance, quarantine retry, repair, DataServer warmup/catch-up/root/status refresh, and auto-bulk finish work on borrowed `std.Io`/shared owners; `vopr/admission.zig` | `zig build storage-vopr-runtime-test vopr-runtime-test data-server-vopr-test admission-vopr-test` |
| Real serverless object-store protocols under deterministic provider faults | `objectstore/scripted_fault.zig`, Antfly `vopr/object_store.zig` | `zig build lib-objectstore-test serverless-object-store-vopr-test` |
| P0/P1 orchestration boundaries | Antfly `vopr/replication_backfill.zig`, `supervision.zig`, `auth_lifecycle.zig`, `serverless_workflow.zig`, `db_index_races.zig` | their focused `*-vopr-test` gates |
| Cold configuration and extension lifecycle | Antfly `vopr/config_extension_lifecycle.zig`; production secret store, remote-content publisher, extension administration/catalog, package scanner, and Wasmtime artifact loader all borrow the same `std.Io` | `zig build config-extension-lifecycle-vopr-test` |
| Embedded, C API, and Lite lifecycle | Antfly `vopr/embedded_lite_lifecycle.zig` and `vopr/capi_lite_lifecycle.zig`; native Lite, docstore/index storage, Embedded DB, opaque C API handles, and portable restore borrow one caller-owned `std.Io` and `BackendRuntime` across open, close, callback, cancellation, activation, crash, and reopen boundaries | `zig build embedded-lite-lifecycle-vopr-test` |
| Cross-service resource pressure | Antfly `vopr/resource_pressure.zig`; one ResourceManager and VoprIo envelope spans production request leases, a real Lite-backed DB write/read, durable-job ownership, ManagedEmbedder provider cancellation, persistent lake-cache queue memory and disk growth, plus task/file/socket quotas | `zig build admission-vopr-test resource-budget-test` |
| Provider and composed-query boundaries | Antfly `vopr/provider_boundaries.zig`, `composed_query.zig`; real ManagedEmbedder, PostgreSQL Source, distributed merge, and graph-union seams | `zig build provider-boundary-vopr-test composed-query-vopr-test` |
| Query embedding cache | Antfly `vopr/query_embedding_cache.zig`; production cache miss coalescing, cancellation, deadline, admission, TTL, LRU, pinned eviction, cleanup, and production-neutral service-rate charging on one `VoprIo`. V2 composes a node-wide slowdown with a hit-copy slowdown, crosses a real request deadline, heals each fault independently, resumes hits, and verifies exact logical usage | `zig build query-embedding-cache-vopr-test` |
| Generation and reranking chains | Antfly `vopr/generation_reranking.zig`; production generation fallback/retry with borrowed `std.Io`, remote OpenAI-to-Antfly fallback, request-scoped remote-to-local generation and reranking replacement, malformed generation, truncated reranking, logical timeout, in-flight cancellation, local/remote routing, exact result validation, and cleanup in one composed trace. The production Antfly generator now honors its caller-owned HTTP timeout instead of overriding it, and HTTP task cancellation remains typed through narrowed transport errors | `zig build generation-reranking-vopr-test` |
| Distributed graph-query execution | Antfly `vopr/distributed_query.zig`; production `executeCrossRange` planning, two-shard fanout, optional hydration, bounded topology retry, retry exhaustion, stale snapshot rejection, in-flight cancellation, cross-table authorization, and production-neutral per-group operation charging. V3 applies a four-times node-local slowdown to group 22 across expand/hydrate, verifies exact owner costs, heals explicitly, and repeats the production coordinator pass at baseline cost. This row does not claim distributed-join coverage | `zig build distributed-query-vopr-test` |
| Public distributed join on production owners | Antfly `api/distributed_join.zig`, `api/internal_join_operations.zig`, `api/table_reads.zig`, `vopr/production_cluster.zig`, and `vopr/full_cluster.zig`; v19 executes a public inner `_id` join whose two right rows resolve through independently owned data-Raft groups before, during, and after an active split. V20 forces a 64-row durable shuffle, injects failure after the first finalizer persists its result, and proves a second owner imports that cached result and completes without repeated finalized work. Forward-only v30 cancels a public durable shuffle at its first internal partition-worker boundary, requires the worker token to observe cancellation and no canceled completion, then proves exact recovery and terminal worker accounting. Forward-only v31 fails the first partition worker before publication and requires exact same-partition failover to another group with one retry and all 64 rows. Forward-only v32 destroys the exact DataServer process that begins that partition, requires typed no-partial exhaustion, rebuilds its stable identity/listeners, and proves an identical fresh 64-row join plus a direct rebuilt-endpoint read. Forward-only v33 exhausts the original request under simultaneous real memory saturation and a matched exact-group link cut, heals both, and proves an identical complete retry. Forward-only v34 matches the remote link failure before canceling an alternate worker under the same all-owner memory pressure, heals both domains, and proves an identical complete retry. Forward-only v35 cancels the selected worker, reconstructs its exact production owner, and proves an identical join plus direct rebuilt-endpoint read | `zig build production-cluster-join-split-vopr-test production-cluster-durable-join-takeover-vopr-test production-cluster-durable-join-cancellation-vopr-test production-cluster-durable-join-worker-retry-vopr-test production-cluster-durable-join-owner-restart-vopr-test production-cluster-durable-join-retry-exhaustion-vopr-test production-cluster-durable-join-cancellation-overlap-vopr-test production-cluster-durable-join-cancellation-owner-restart-vopr-test -Doptimize=ReleaseSafe`; only these join shapes and lifecycles are claimed |
| Deployment-shaped full cluster | Antfly `vopr/full_cluster.zig`, `vopr/production_cluster.zig`, `vopr/serverless_workflow.zig`, `metadata/sim_harness.zig`, and the production HTTP/Raft runtimes share one `VoprIo`. V9 retains the hosted/public nine-fault campaign. V11-v40 add the cited production metadata/DataServer/public/serverless, transition, graph/join/global-query, fault, durable-worker, and reconstruction seams. V42 adds clean replication snapshot-to-stream work through alternating public coordinators and DataServer Raft/index visibility. Forward-only v43 adds schema-change interruption after the first accepted batch, durable resume with one exact duplicate batch, CDC completion, slowed/healed accounting, and the unchanged cluster cleanup/replay oracle. Forward-only v44 adds exact target-leader process teardown/reconstruction, bounded client reconnect, durable resume, and direct plus all-coordinator visibility. Forward-only v45 adds actual provider-query failure, exact source-session replacement, and resume without extra target work. Forward-only v46 adds post-checkpoint lease cancellation, replacement-session resume, and composed charging without a duplicate target batch. Forward-only v47 adds pre-checkpoint stale-owner rejection and one exact idempotent target replay. Forward-only v48 adds metadata-Raft source-catalog publication, exact-cutover authority replacement, predecessor retirement, and one exact replay. Forward-only v49 adds the actual `ApiHttpServer` cache's slowed deadline, process restart, empty replacement, recomputation, bounded reconnect, and durable rebound read. Remaining work is targeted replication timing and cross-domain fault breadth | `zig build full-cluster-vopr-test` plus the focused production-cluster v11-v49 gates |
| Production-owned full-cluster composition | Antfly `vopr/production_cluster.zig` plus `vopr/full_cluster.zig`; v11-v40 provide the detailed production-owner seams cataloged in Distributed Coverage. Public strong reads route to the current owner and require matching applied ReadState plus local derived-index visibility; remote work crosses typed internal HTTP while modeled time borrows the shared runtime. V23 installs one shared service-rate model; v42 runs the production replication runner against the public BatchRequest adapter and keeps production owners live behind a completion fence; v43 interrupts after the first accepted batch, changes schema, resumes from durable status, and requires exactly four accepted batches; v44 stops the target leader, reconstructs its stable process/listener identity, and requires exactly two bounded transport failures followed by three successes and final visibility through every coordinator; v45 fails the prepared provider session at its first query, balances exactly three opened/closed sessions, and resumes through a strictly newer session with three target successes; v46 cancels at durable offset 1, requires exact `CdcWorkLeaseLost`, sequential session replacement, and three target successes; v47 loses ownership after target apply, rejects checkpoint advancement at offset 0, and requires four target successes with one idempotent replay; v48 changes the exact source catalog after target apply, rejects authority A with `ReplicationSourceConfigChanged` at durable offset 0, claims authority B through metadata Raft, retires A, and completes with four target successes; v49 runs deadline/coalescing/restart/recompute against the cache owned by node 1's real `ApiHttpServer`, preserving the ordinary workload behind a completion fence and requiring one exact pooled reconnect plus a durable rebound read | The v11 smoke target passes 30/30; the cited v12-v40 gates use their documented budgets and fresh-world replay. V42's 160,000-transition clean replication gate, v43's 180,000-transition schema-recovery gate, v44's 220,000-transition target-owner-restart gate, v45's 220,000-transition source-session-crash gate, v46's 220,000-transition durable-cancellation gate, v47's 240,000-transition stale-owner gate, v48's 260,000-transition metadata-topology gate, and v49's 120,000-transition cache deadline/restart gate pass Debug and ReleaseSafe 15/15. Complete gates include named properties, record/fresh-world replay, cleanup, and leak checks; the 2,000-transition subprocess remains lifecycle-only evidence |
| Parquet cache, provisioning/startup, external lake, and media providers | Antfly `vopr/parquet_cache.zig`, `provisioning_startup.zig`, `external_lake.zig`, `media_runtime.zig`; borrowed `VoprIo`, real cache/reconcile/Iceberg-manifest/Parquet-query/provider-HTTP paths, injected I/O and object-store faults, provider retry/timeout/cancellation and active-request drain, cleanup, and exact replay | `zig build parquet-cache-vopr-test provisioning-startup-vopr-test external-lake-vopr-test media-runtime-vopr-test` |
| Product upgrade and compatibility campaign | Antfly `vopr/upgrade_compatibility.zig`; current production readers open v1 HA golden records, v12 manifests, v14 external inventories, and legacy serverless heads; incompatible data directories and future product artifacts fail closed; atomic data-directory publication recovers after a crash-before-rename. VOPR-native traces, checkpoints, and fixtures are intentionally outside this campaign because their schemas are forward-only | `zig build upgrade-compatibility-vopr-test` |

The real DataServer listener, httpx client/server transport, request lifecycle,
deadline, shutdown, partial writes, and Raft wire requests now execute as
deterministic `VoprIo` transitions. Routed data and split/merge operations
remain deliberately conservative
where production has not yet exposed a safe suspension point. VOPR must not
counterfeit concurrency by opening a competing writer or bypassing production
lease ownership. Ordinary socket reads and writes always use the injected
`std.Io`; a runtime-proven native server handle may use kernel timeout options
for scalable blocking I/O, while borrowed/virtual handles use logical
Select-based timeouts and never reach POSIX with a virtual descriptor.

## Design Influences

The design adopts the useful application-level parts of Antithesis:

- deterministic replay of controlled nondeterminism
- small compatible commands and independently overlapping faults
- non-fatal safety, reachability, exercise-quality, and recovery properties
- coverage- and property-guided state-space exploration
- branching timelines and counterfactual debugging

References:

- [How Antithesis works](https://antithesis.com/docs/introduction/how_antithesis_works/)
- [Deterministic simulation testing](https://antithesis.com/docs/resources/deterministic_simulation_testing/)
- [Assertions](https://antithesis.com/docs/product/writing_tests/assertions/)
- [Controlling faults](https://antithesis.com/docs/product/writing_tests/controlling_faults/)
- [Fault types and node scope](https://antithesis.com/docs/product/writing_tests/controlling_faults/fault_types/)
- [Multi-container test templates](https://antithesis.com/docs/product/writing_tests/test_templates/first_test/)
- [Debugging](https://antithesis.com/docs/product/debugging/)

These are influences, not a claim of hypervisor equivalence or identical search
algorithms.

### Distributed-System Correspondence

Antithesis treats containers or Kubernetes pods as distributed fault domains:
separate nodes can experience asymmetric network disruption, pause, kill,
restart, throttling, and independently placed workload commands. VOPR provides
the corresponding deterministic application-level mechanisms—logical nodes,
packet scheduling, directional partitions, process and node lifecycle,
node-scoped durable state, resource budgets, and multi-actor workloads—but
runs registered production entrypoints in one virtual `std.Io` world.

| Dimension | Antithesis | VOPR status |
| --- | --- | --- |
| Multiple logical nodes and clients | Multiple containers or pods | Integrated in metadata, distributed-data, distributed-transaction, Raft, HA, and data-plane suites |
| Link and packet faults | Asymmetric latency, loss, clogs, partitions, and recovery | Integrated drop, duplicate, reorder, delay, jam, outage, directional partition, and healing |
| Node lifecycle and pressure | Pause, stop/kill, restart, and throttling | Integrated at registered process/resource seams: pause, crash/restart, CPU-work exhaustion, descriptor, socket, allocator, and storage limits. Reversible per-node CPU throttling/modulation and arbitrary native-thread pause are not implemented |
| Deterministic replay and branching | Deterministic hypervisor execution | Exact choice/transition/observation replay plus reduction and multiverse branching |
| Whole unmodified deployment | Arbitrary containerized binaries and sidecars | Deliberate non-goal; only registered in-process entrypoints are deterministic |
| One full Antfly deployment history | Runs a supplied Docker Compose or Kubernetes topology | Integrated in-process at named complementary seams: v22 runs the real metadata quorum, three production DataServer/data-Raft owners and resource managers, public two-table I/O, serverless catalog, a metadata-driven active split, and public graph work before/during/after that split. Earlier modes add a fail-closed next-owner transport cut, stable-endpoint owner reconstruction, an exactly observed recoverable short write, all-owner memory denial, join recovery, and v21 overlaps the selected graph link cut with all-owner memory pressure; v22 adds exact selected-listener socket denial and recovery. V9 supplies the hosted public graph and broader nine-fault vocabulary. Both exact-replay, but remaining topology breadth, disk-capacity pressure, broader socket/short-write surfaces, and storage/process/restart fault overlaps are not yet on the production owners. Co-resident HA and richer cross-domain overlap remain ongoing. Separate address spaces, native sidecars, DNS, kernels, and live mixed binaries remain conditional/differential concerns |

Antithesis therefore does support distributed-system testing directly: its
fault domains are containers or Kubernetes pods, including asymmetric network
and node faults. VOPR's corresponding deterministic distributed foundation is
integrated, but whole-deployment composition is not finished merely because
the focused distributed suites pass. Composition breadth can still grow inside
the existing virtual OS. Native multi-process,
container, Kubernetes, DNS, init-system, mixed-binary, and cross-language
behavior remains a focused differential/integration tier.

## Goals and Non-Goals

### Goals

- Reproduce every retained failure from an explicit artifact, not merely a
  seed.
- Put workload, message, task, timer, storage, node, and fault interleavings
  under one scheduler.
- Compose multiple independently owned Antfly nodes and clients in one history,
  with node-local lifecycle, storage, resource, and transport identities.
- Run production components unchanged on `std.Io.Threaded` or deterministic
  `VoprIo` where their capability requirements are supported.
- Express correctness as stable named properties and distinguish product
  failures from harness failures.
- Guide exploration with semantic state, transition, property, and optional
  instrumentation feedback.
- Minimize failures while preserving their identity and promote only reviewed,
  replay-proven fixtures.
- Keep the entire workflow runnable locally and in CI without a hosted service.
- Reuse existing Raft, HA, LSM, storage, transaction, integration, and formal
  oracles rather than replacing them.

### Non-Goals

- A machine-code VM capable of running arbitrary binaries or operating systems.
- A Docker, Kubernetes, DNS, init-system, or general multi-process clone for
  unmodified cross-language deployments.
- Raw heap, thread-stack, socket, or process snapshots as replay truth.
- Silent fallback to host threads, clocks, entropy, files, sockets, or
  processes.
- Proving liveness while a deliberately unrecoverable fault remains active.
- Replacing focused unit, integration, differential, formal, or native chaos
  tests.
- Making a nightly wall-clock budget deterministic. Every completed history,
  rather than the number completed, must be deterministic.
- Automatically committing generated fixtures.

## Architecture

```text
campaign / replay / reducer / debugger
                  |
        explicit ChoiceSource
                  |
      one-transition Scheduler
                  |
      Scenario + named Properties
                  |
 narrow Runtime / std.Io capability boundary
                  |
 VoprIo tasks, clocks, files, sockets, processes, quotas
                  |
 Antfly metadata, Raft, storage, HA, and application adapters
```

### Package Boundary

Use `lib/vopr`, not a generic `lib/sim`, for the reusable engine. The VOPR name
communicates stable replay, scheduling, property, corpus, and reduction
contracts. A component should move into another independent library only when
it is useful without VOPR campaign semantics, such as a general modeled block
device.

The dependency rules are:

- `lib/vopr` imports no Antfly metadata, Raft integration, storage, or API code.
- Antfly scenarios import VOPR and their production domains.
- Production code depends only on narrow runtime, clock, entropy, transport,
  storage, and executor interfaces.
- Test and campaign policy stays in `pkg/antfly/src/vopr`.

Key layout:

```text
lib/vopr/src/
  choice.zig                 scenario.zig
  scheduler.zig              runner.zig
  runtime.zig                sim_runtime.zig
  vopr_io.zig                 vopr_io_task.zig
  vopr_io_file.zig            vopr_io_net.zig
  vopr_io_process.zig         vopr_io_instrumentation.zig
  time.zig                   clock_fault.zig
  fault.zig                  property.zig
  observation.zig            coverage.zig
  corpus.zig                 explorer.zig
  trace.zig                  replay.zig
  reducer.zig                fixture.zig
  snapshot.zig               splice.zig
  causal.zig                 multiverse.zig
  debugger.zig               collector.zig
  event_query.zig            flight_recorder.zig
  report.zig                 health.zig
  debug_recipe.zig           benchmark.zig
  vopr-trace-v1.schema.json

pkg/antfly/src/vopr/
  DETERMINISM_AUDIT.md
  cli.zig
  cli_runner.zig
  domain_vopr.zig
  request_lifecycle.zig
  data_server.zig
  object_store.zig
  admission.zig
  replication_backfill.zig
  supervision.zig
  auth_lifecycle.zig
  serverless_workflow.zig
  db_index_races.zig
  provider_boundaries.zig
  composed_query.zig
  fixtures/<scenario>/
```

`VoprIo`, the `vopr_io*.zig` modules, `pkg/antfly/src/vopr`, `vopr_tests`
identifiers, and `vopr-*` build steps are canonical. `vopr-test` is the fast
Antfly aggregate; `vopr-engine-test` is the focused application-independent
engine gate. VOPR is new code: it has no `sim-*` command aliases, legacy trace
extension, or old backend-ID namespace. Only `.voprtrace` is accepted, and
canonical transitions and backend IDs use `vopr-io.*`/`vopr-io-*`. Older
randomized real-I/O suites use `workload`/`integration` terminology when
renamed; they do not become VOPR suites merely by changing a label.
Configured instrumentation backend identity is constructed directly from its
canonical map digest; VOPR does not build a baseline identity and substitute
an "old" entry or translate a retained artifact.

This forward-only rule applies to every VOPR-native surface: APIs, saved event
sets, traces, run/results JSON, corpus indexes, debugger artifacts, command
names, and service-rate identities. New VOPR code has one canonical spelling
and one required schema. Do not add deprecated aliases, optional identity
synthesis, compatibility wrappers, fallback parsers, or migration paths for a
pre-canonical VOPR shape. This does not remove Antfly product-format upgrade
coverage: VOPR should continue testing old database, protocol, and serverless
artifacts wherever the production product promises to read them.

Domain adapters may remain beside their production domains when that preserves
the cleanest dependency direction.

## Determinism and Replay Contract

A history is a sequence of explicit decisions, not the output of a seed. Seeds
are discovery metadata. Each choice record contains:

- a stable namespaced choice-site ID
- a site occurrence number
- the complete stable enabled-alternative set
- the selected alternative
- structured parameter values where applicable

Choice namespaces distinguish scheduler, workload, fault, parameter, entropy,
and recovery decisions. Pointer values, source line numbers, container
iteration order, wall time, and native thread identity are forbidden from
stable IDs.

At replay, a clean world consumes the recorded choices and verifies the choice
site, enabled set, selected alternative, transition identity, configured
observations, and expected outcome. The first mismatch is
`ReplayDiverged`—a compatibility result, never a product failure.

Determinism applies to:

- initial configuration and fixture bytes
- entropy and IDs that can affect behavior
- runnable task and transition selection
- timer and storage completion delivery
- network packet delivery and waiter wake order
- filesystem directory order and modeled durability
- fault start, continuation, healing, and overlap
- property encounters and observation digests

Every retained or promoted artifact is exact-replayed from a clean world before
it affects corpus state.

## Scheduler and Logical Concurrency

The scheduler selects exactly one enabled transition at a time. Typical
transitions include a workload command, runnable task, message delivery, timer,
storage completion, node lifecycle action, fault action, or recovery step.

Actors expose small start, poll, completion, cancellation, and cleanup steps.
This provides deterministic logical concurrency without requiring native
threads. Large atomic operations are acceptable only when the production
interface does not yet expose a safe narrower seam; such boundaries must be
documented explicitly.

Transition outcomes distinguish:

- completed work
- expected product rejection
- blocked with enabled future work
- quiescent success
- property failure
- replay divergence
- harness capability or validity error
- transition or resource budget exhaustion

The runner must never report a harness error as an Antfly correctness failure.

## `VoprIo` Runtime

`VoprIo` is an application-scoped deterministic virtual OS implemented as a real
`std.Io.VTable`. The same production component should be able to use
`std.Io.Threaded` or `VoprIo` without a second simulation-only business-logic
implementation.

Executor ownership belongs at process, service, CLI, C-API, or test composition
roots. Leaf helpers and long-lived components accept `std.Io` and must not
silently create a private `Threaded` runtime. The current audit moved data-dir
format admission, persistent Parquet-cache workers, replica-root provisioning,
restore progress probes, DB enrichment startup, repair entropy, and HA repair
receipt persistence onto borrowed I/O. Compatibility wrappers may use
`std.Options.debug_io`, but they do not own another executor; new production
callers should always pass their runtime lane explicitly.

### Fail-Closed Capability Model

Construction preflights declared capabilities. Unsupported operations return
their documented error or latch a harness violation when the vtable operation
cannot return an error. No handler aliases or delegates to `std.Io.Threaded`.
The backend identity pins the Zig version, supported capability set, virtual-OS
model, and instrumentation map.

### Implemented Task Kernel

Stackful simulated tasks support futures, groups, await, cancellation, sleep,
futexes, queues, mutexes, and selection. Runnable-task selection, park, wake,
cancel, spurious wake, eager completion, timer delivery, and futex waiter
selection are scheduler-visible stable choices.

Virtual-OS model v2 made readiness and active synchronization epochs explicit:
futex pointer identities expire when the last waiter leaves; external-wake
sequence numbers advance independently per logical resource; and an eager
network producer still creates the same accept-readiness completion as a
waiting consumer. Model v3 removed process-global allocation order from task
and futex replay identities. Model v4 additionally scopes each fiber to its
logical parent, an ASLR-independent callsite offset, and a callsite-local epoch;
the raw process-global creation ordinal remains diagnostic metadata only. A
task's first external block binds its scheduler identity to the logical
resource and a resource-local waiter epoch, so unrelated sibling creation
cannot rename an established owner.

Network identities follow the same rule: listeners are scoped to stable
IP/Unix endpoints, provisional client/server pairs to the listener and
connecting owner, and packets/FINs/datagrams to a source-socket-local sequence.
Model v5 makes that steady-state connection identity a function of the logical
connecting owner plus first stream payload, so distinct clients sending the
same request cannot exchange a content-local occurrence. It migrates any
parked read/write resource before
delivery is selectable. Unrelated or reordered connections can no longer
exchange their scheduler-visible steady-state identities. Packet transitions
also carry a digest of their bytes, so equal-length payload changes fail exact
replay instead of hiding behind the same packet identity and byte-count
parameter.
Changing any of these rules requires a model-version bump because they are part
of exact replay, not diagnostic presentation.

### Implemented Files

Virtual integer handles provide directories, deterministic iteration, recursive
rename, positional and streaming I/O, atomic publication, locks, mappings,
metadata, descriptor and capacity limits, partial I/O, data sync, separate
namespace sync, dropped sync, precise one-shot read-range corruption, and crash
reconstruction from durable state. Composed deployments can query live bytes
under a logical path prefix, allowing each application-owned storage-capacity
domain to report modeled usage without inspecting the host filesystem.

Symlinks, hard links, and optimized file-to-file transfer currently fail
closed. Modeled storage distinguishes volatile from durable state; a crash
drops volatile state rather than invoking production close paths.

### Implemented Networking

IP and Unix listen/connect/accept, socket pairs, stream reads and writes,
half-close, close, deterministic loopback resolution, bounded send queues,
partial writes, and packet delivery are modeled in memory. Drop, duplicate,
reorder, outage, jam, directional partition, arbitrary delay, and backpressure
are explicit model state. Bound UDP datagrams preserve message boundaries and
source addresses while sharing scheduler-visible drop, duplicate, reorder, and
delivery behavior.

Network faults may also select one client direction at a stable listener and
match a semantic byte-stream marker across fragmented writes. The selector can
either fail matching writes as an outage or limit exactly one matching write
to a caller-owned byte count. A monotonic application counter distinguishes a
real short write from an armed-but-never-reached fault; v17 consumes that
evidence on the registered production coordinator-to-owner link.

One logical IP listener may also carry an endpoint-stable maximum for live
accepted connections. Zero rejects every new connection with
`ProcessFdQuotaExceeded` without closing established streams; clearing the
limit heals admission even while the listener is down. The identity survives listener close/rebind, limits do
not spill into other endpoints, and closed accepted sockets release capacity.
V22 consumes this primitive through fresh non-pooled production HTTP clients
and proves denial occurs before public handler ingress.

Every successful connect publishes a scheduler-visible readiness event before
`accept` may consume the connection, whether the producer or consumer arrives
first. This preserves the enabled-set contract across equivalent schedules and
prevents a producer-first fast path from hiding a choice that replay observes
when `accept` waits first.

### Implemented Processes and Resources

Only registered in-process entrypoints may spawn. Unknown executables fail
deterministically. Child arguments and virtual process identity are owned by
the model; wait, kill, pause, resume, cancellation, CPU-work budgets, process
limits, file descriptors, sockets, storage capacity, send buffers, and
allocator limits are modeled.

### Instrumentation

Optional stable safepoints count hits and may yield the current fiber. Their map
digest participates in compatibility only when enabled. Instrumentation may
improve search efficiency but cannot change the meaning of an already
compatible trace.

## Fault Model

Faults have explicit start, active, and heal lifecycles. Independent faults may
overlap subject to scenario budgets and preconditions.

Supported fault families include:

- network drop, duplicate, reorder, delay, jam, outage, and directional or
  node partition
- node pause, crash, restart, and leadership loss
- monotonic advance, realtime jump or skew, oscillator-rate change, and
  deferred timer delivery
- write, sync, rename, delete, capacity, namespace-durability, partial-write,
  crash, and selected storage-completion faults
- process, task, CPU-work, allocator, descriptor, socket, queue, and storage
  resource limits
- scenario-specific lease expiry, stale owner, provider outcome, and
  publication-phase faults

Crashing a node and crashing its durable device are related but distinct
choices. Realtime and monotonic time are also distinct domains.

Each scenario declares maximum active faults, which combinations are valid,
minimum surviving capacity or quorum, and its healing policy. State-aware
preconditions prevent campaigns from spending most of their budget in invalid
or permanently unrecoverable worlds.

## Workloads, Properties, and Coverage

Workload commands are small, typed, and state aware. Candidate commands expose
stable IDs and preconditions; the explorer chooses only among enabled commands.
Large scripted regressions remain useful corpus seeds but are not the scheduler
abstraction.

Properties are registered before execution and evaluated non-fatally where
continuing is safe. Supported meanings include:

- `always`: encountered at least once and never false
- `always_or_unreachable`: never false when encountered
- `reachable` and `unreachable`
- `sometimes`: true at least once during the history
- `eventually_after_quiescence`: true after the deterministic recovery phase

Properties have stable IDs independent of their human-readable messages.
Online checks cover safety; final-state and liveness checks run after a quiet
suffix. Failures use stable fingerprints containing failure class, property ID
or normalized error identity, scenario version, optional domain identity, and
optional canonical observation digest.

Semantic observations include topology shapes, Raft roles and progress,
message and storage states, active fault combinations, workload phases,
property encounters, ownership generations, queue pressure, and domain-specific
states. Corpus retention uses new states, transitions, property outcomes, and
fault/workload combinations. Optional instrumentation coverage is secondary
feedback and not replay input.

## Exploration, Artifacts, and Debugging

The campaign loop starts from reviewed fixtures and retained replayable corpus
entries, mutates structured choices, branches after an exact prefix, generates
a suffix, runs a deterministic quiet phase where configured, and exact-replays
any candidate before retention.

Mutation supports decision replacement, range deletion, fault simplification,
workload and configuration shrinking, scheduling simplification, and compatible
prefix/suffix splicing. Multiple workers receive deterministic history IDs and
seeds; worker completion order does not decide canonical corpus contents.

### Artifact Contract

`vopr-trace-v1` NDJSON records a versioned header and configuration, choices,
transitions, faults, canonical events, observations, property encounters,
failures, and a summary. The decision stream is authoritative; canonical
events and observations support diagnostics, coverage, and formal export.
Verbose flight-recorder details are deliberately outside replay truth, so
retaining or changing diagnostic detail cannot invalidate a canonical trace.

Generated artifacts normally live under:

```text
/tmp/antfly-vopr/<campaign>/<history-id>.voprtrace
```

Reviewed promoted fixtures live under:

```text
pkg/antfly/src/vopr/fixtures/<scenario>/<name>.voprtrace
```

Promotion is explicit. VOPR artifacts are not migrated: a scenario or engine
ABI change invalidates them, and replacements are newly recorded and reviewed.
VOPR artifacts use only the `.voprtrace` extension; there are no `sim-*`
command aliases or legacy filename fallbacks. The content format and serialized
runtime identities have one current replay ABI.

### Reduction

Reduction always starts from a clean world and preserves the target fingerprint.
It proceeds from broad decision-range deletion through fault, workload,
scheduling, and configuration simplification. Divergence, a harness error, or a
different property failure does not reproduce the target.

### Branching and Checkpoints

Rewind reconstructs a clean world and replays an explicit prefix. Logical
scenario snapshots may accelerate this only when their configuration and prefix
digests match and restore is replay-proven. Raw heap or fiber-stack copying is
never replay truth.

### Causality and Debugger Primitives

Bounded counterfactual analysis replaces a selected pre-failure decision,
explores deterministic descendants, exact-replays every child, ranks
failure-probability reductions, and records stable experiment IDs, trial-seed
digests, and a pointer-free parent/child multiverse graph. Prefix,
descendant-per-alternative, and total-experiment limits bound the analysis even
when a choice site has a large enabled set. The debugger cursor
can seek a choice prefix, list recorded alternatives, create and verify a child
branch, and collect deterministic state before, at, and after a failure.
`vopr-debug` exports replay-validated snapshots and provides the same
line-oriented `show`, `seek`, `causal`, `causal-window`, `collector`, `branch`,
and `compare` commands through a command file or interactive standard input.
Collector commands are available for scenarios that implement the generic
collector contract; unsupported scenarios fail closed.

## Quiet Suffix and Liveness

Safety is checked during active faults. A scenario validates liveness only
after this explicit deterministic recovery protocol:

1. Stop starting new workload operations.
2. Heal faults the scenario promises are recoverable.
3. Restart nodes covered by the recovery contract.
4. Restore normal network and resource policy.
5. Advance enabled transitions under a documented fair policy.
6. Stop on the recovery predicate, quiescence, or the transition budget.
7. Evaluate eventual and final-state properties.

Every heal and recovery transition is recorded. The runner never mutates
product state to make recovery succeed.

## Implemented Scenario Inventory

These are independent domains with durable CLI identities, scenario ABIs,
fixture namespaces, focused gates, and production-regression dependencies.
`domain-vopr-test` is only a lightweight convenience aggregate; it is not an
owner or prerequisite of the five application domains.

### Metadata and Distributed Data

The metadata scenario schedules virtual Raft HTTP delivery, partitions, drop,
duplication, delay, reordering, node pause/restart, table lifecycle, placement,
topology, split, merge, and a quiet recovery phase.

The distributed-data integration composes real public API writes and reads,
split, partition/restart, modeled durable-device crash/recovery, merge, and an
acknowledged-operation oracle. A focused production composition additionally
runs the real DataServer public listener and `/healthz` request through httpx on
borrowed `VoprIo`, including partial writes and deadline-first shutdown. Routed
write/read, Raft, and split/merge internals remain the next microstep boundary.

Focused gates: `lib-metadata-vopr-test`,
`lib-metadata-vopr-data-test`, `data-server-vopr-test`, and
`metadata-vopr-replay-stability-test`.

### Transaction

The focused transaction scenario exports TLA+ events. The independent
distributed-transaction scenario drives three production `TxnManager`
instances over independent Antfly stores: coordinator and participant setup,
prepare, durable decision, ambiguous response, crash/reopen, lease adoption,
stale-owner conflict, phase-two delivery, acknowledgement, and repair. Its
oracle reads production transaction records and visible values.

Focused gates: `transaction-vopr-test` and
`distributed-transaction-vopr-test`.

### Raft

The real `RawNode` cluster exposes message delivery/drop, deferred persistence
and application, proposal, restart, partition, and compaction independently.
Existing Raft invariant, differential, snapshot, and TLA+ checks remain
complementary gates and corpus sources.

Focused gate: `raft-vopr-test`.

### Storage: WAL, LMDB, LSM, Persistent, Index Manager, and DB Split

Storage scenarios adapt their real action vocabularies and oracles to the
common choice, artifact, replay, campaign, and fault lifecycle. Coverage
includes C-versus-Zig LMDB, memory-versus-real LSM, WAL publication and reopen,
real PersistentIndex, split IndexManager, full DB split, maintenance,
compaction, crash recovery, volatile/durable state, and typed storage faults.

Focused aggregate: `storage-vopr-test`. Exact fixture and legacy real-I/O
commands are documented under Test-Tier Policy below.

### HA

The HA scenario drives real primary and standby logs, progress WALs, slot and
fence stores, replication, application, partition, crash, retention, backup,
promotion, rejoin assessment, stale-owner fencing, and ordered applied-prefix
properties.

Focused gates: `ha-vopr-test` and `ha-chaos-test`.

### Data Plane

The independent modeled scenario decomposes admission, route, virtual packet,
Raft-log persistence, application, acknowledgement, writer-epoch handoff,
split copy/cutover, and point/query visibility. It directly consumes `VoprIo`
sockets and durable modeled files. Its focused gate also composes the real
metadata distributed-data and per-group Raft suites.

Focused gate: `data-plane-vopr-test`.

### Distributed Coverage Boundary

The suites above prove distributed semantics at complementary production
boundaries; they are not merely single-node unit models. They already exercise
multiple logical nodes, clients, stores, transports, consensus participants,
failure domains, and recovery owners under one exact-replay scheduler.

`full-cluster-vopr-test` now forms one deployment-shaped in-process campaign
containing all of the following at once:

- a metadata quorum and multiple independently restartable hosted data nodes;
- multiple public API clients with concurrent write, read, query, cross-node
  routing, two-table isolation, and a cross-range graph workload;
- a co-scheduled serverless build/enrichment/publication fixture, including a
  stale-generation fencing mode and a production serverless HTTP listener over
  the exact object-backed catalog mutated by the worker;
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
in-flight graph-transport failure/recovery, partial-HTTP-write,
stale-serverless-generation, and aggregate node-memory denial/recovery modes
exact replay. After the worker completes, a
production `ServerlessHttpClient` lists `docs` and queries version 3 through
the real serverless handler and `httpx` listener; the expected documents differ
for the stale-generation mode, so this also proves fencing at the public seam.
The public DataServer client also writes a graph whose two edges cross the
table's range boundary, waits for full-index acknowledgement, and traverses
both hops through the production public-query parser, range planner, internal
HTTP expansion protocol, shard fanout, and canonical response decoder.
Raft time and delivery eligibility remain explicit modeled rounds, but
each successful delivered frame crosses the production binary codec, fault
router, `IoHttpExecutor`, VOPR socket, httpx listener, and production Raft HTTP
handler. The serverless object catalog and metadata placement catalog are
separate production domains; joining them would invent an ownership
relationship that Antfly does not have. The current limits are instead that HA
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
`VoprIo`. Full-cluster v11 now reuses those production owners behind the live
metadata quorum for a static public/serverless baseline. V12 reaches the real
hosted transition path and records destination bootstrap/apply convergence,
finalization, publication, and a post-split public read; cancellation of an
active hosted callback is covered by the bounded cutoff. After transition retry
jitter became a stable per-node deterministic input, its complete
320,000-transition history passes record, fresh-state exact replay, properties,
cleanup, and leak checks. The active transition is integrated at this stated
seam. V13 adds the production-owner graph, v14 overlaps it with that active
split, v15 cuts the real next-owner graph stream, and v16 stops and reconstructs
that next production owner and both stable-port service listeners before Raft
and graph recovery. V17 limits one semantic-stream-selected request write on
the actual registered coordinator-to-owner link and proves production stream
resumption preserves the complete graph. V18 adds real all-owner memory
pressure; v19/v20 add public cross-owner join and durable-finalizer takeover;
v21/v22 add simultaneous link-plus-memory and selected-listener admission
faults; and v23 proves reversible per-node/per-operation service rates across
DataServer, graph, and serverless owners. V24-v29 promote public hydration,
cancellation, in-flight authorization mutation, stale-snapshot retry
exhaustion, and cancellation under a matched transport outage. Forward-only
v30-v35 then promote durable-worker cancellation, same-partition group
failover, actual partition-owner process destruction/reconstruction, retry
exhaustion plus cancellation while a real resource domain overlaps one exact-
group link cut, and cancellation followed by exact worker-owner
reconstruction. V36 sends one two-line global NDJSON request through the real
public `/db/v1/query` listener, production routing, and independently owned
`docs` and `tenant_b_docs` tables. Its structural oracle requires exactly two
flattened responses in request order and exact, disjoint ID sets for each
table. Forward-only v37 cancels that public request after the first production
table result is assembled, requires typed client cancellation, listener drain,
and exactly one assembled result from the canceled operation, then proves an
exact ordered two-table recovery on a fresh request. Forward-only v38 revokes
the authenticated principal's `tenant_b_docs` read permission immediately
after the first `docs` result is assembled. The same admitted NDJSON request
must fail closed with an exact 403 body, exactly one assembled result, and no
protected-table payload; the history then restores policy and proves the exact
ordered two-table response on a fresh request. Forward-only v39 selects and
registers the real directional public-coordinator-to-tenant-owner link, then
cuts only the internal `tenant_b_docs/query` semantic stream after the first
`docs` result is assembled. The request must discard that result and return
the exact retryable distributed-query 503, with one matched outage and no
second assembled result; the history explicitly heals the link and proves the
exact ordered two-table response on a fresh request. Forward-only v40 reaches
the same boundary, but destroys the exact process that owns `tenant_b_docs`
instead of applying a link policy. A separate deterministic lifecycle owner
tears down the DataServer and its public and Raft listeners, keeps that stable
identity absent until the request returns the exact retryable 503 without a
second result, then reconstructs the process with a fresh incarnation. The
history does not call reconstruction complete until the exact rebound public
endpoint serves the durable tenant document; it then proves the exact ordered
two-table response on a fresh request. Every incomplete public
graph, join, or global-query operation must fail closed without partial rows;
each faulted history heals and proves a complete fresh request.
Forward-only v49 composes the production query-embedding cache with the same
deployment-shaped service-rate history instead of treating the cache as an
isolated focused suite. The cache is the instance actually owned by node 1's
live `ApiHttpServer`; its work port is reinstalled whenever that `DataServer`
process is reconstructed. While the node-wide two-times slowdown is active,
one same-key waiter must cross the real coalescing ledger and expire at its
logical deadline while one producer remains in flight. After healing, one
retained hit succeeds. The ordinary public workload then reaches an external
completion fence, node 1 is destroyed and reconstructed from its stable roots,
the replacement cache is proven empty, and the same key produces exactly one
new computation plus one retained hit. The long-lived public client must fail
exactly once on its stale pooled connection, reconnect, and read a pre-restart
durable document through the rebound endpoint. The property requires exact
pre/post-heal units, exact pre/post-restart cache ledgers and byte accounting,
two computations, four owned results, zero in-flight work/effects, public
visibility, quiet cleanup, and fresh-world replay. Its dedicated 120,000-
transition Debug and ReleaseSafe gates pass 15/15.
Forward-only v42 composes the production replication runner with that same
deployment rather than copying its snapshot-to-stream state machine into the
cluster harness. The fixture borrows the cluster `VoprIo`, uses the shared
node-1 service-rate model, and targets a production adapter that serializes the
canonical `BatchRequest` and alternates ordinary public coordinators. Two
snapshot batches and one streaming upsert therefore cross public HTTP,
routing, leader forwarding, `DataServer`, data Raft, and full-index visibility.
A generic completion fence keeps listeners, Raft owners, and storage alive
until the external runner finishes. Snapshot work and the first accepted
public batch execute under the two-times node slowdown; healing then permits the remaining
snapshot and stream work at baseline. The exact oracle requires three attempts
and three accepted responses, every source document visible through every
public coordinator, exact snapshot/stream accounting, the pre-existing
DataServer/graph/serverless invariants, zero active effects, quiet cleanup, and
fresh-world replay. Its dedicated 160,000-transition Debug and ReleaseSafe
gates pass 15/15. This promotes the clean replication target path; focused
fault modes remain in the focused suite at this checkpoint.
Forward-only v43 promotes the schema-change and duplicate-resume boundary. The
first snapshot batch is accepted through node 1 while slowed, after which the
production lifecycle hook changes the source configuration and interrupts the
attempt. The same runner resumes from its durable status under the new schema,
and the source-boundary witness requires the resumed snapshot to query
`users_v2`. It replays exactly one already-applied batch through the public
target, completes the remaining snapshot and CDC stream, and preserves final
idempotent state.
The property requires an observed first-attempt failure, the schema switch,
an observed `users_v2` query, exactly four accepted target batches, three final
documents visible through every public coordinator, pre/post-heal work, the
complete cluster oracle,
cleanup, and fresh-world exact replay. Its dedicated 180,000-transition Debug
and ReleaseSafe gates pass 15/15. Additional cancellation timings and cross-
domain overlap remain separate future modes; v44 promotes one exact target-
owner restart, v45 one exact source-session crash, v46 one durable-checkpoint
cancellation, v47 one pre-checkpoint stale-owner rejection, and v48 one exact
metadata source-catalog/authority rotation below.
Forward-only v44 promotes one exact target-owner crash/reconstruction boundary.
After the first snapshot batch is durably accepted, it waits for the ordinary
public graph workload to reach its terminal boundary while the external
completion fence keeps every production owner alive. It selects the current
data-group leader, preserves its public URI, destroys that `DataServer` and
both stable-port listeners, and sends the next canonical batch to the stopped
endpoint. The production executor must report `SendFailed`; reconstruction
then reuses the stable node/store IDs, rotates the reporter incarnation,
rebinds public and Raft listeners, republishes metadata, and waits for leader
and route recovery. A fresh bounded VOPR-backed public client must read an
already-indexed durable document directly from the rebound endpoint, while a
local exact-group read proves the first replicated `doc:d` survived the
restart. The long-lived production client then exercises one stale pooled-
connection failure before the durable runner resumes.
The property requires exactly five target attempts, exactly three successes,
the two bounded transport failures, no reconstruction error, durable local
recovery, a direct 200 from the replacement process, all three replicated
documents through every coordinator, exact pre/post-heal work, every existing
cluster oracle, cleanup, and fresh-world replay. Its dedicated 220,000-
transition Debug and ReleaseSafe gates pass 15/15. This promotes that exact
post-first-batch target-leader restart; additional source-crash, cancellation,
topology, and target-crash timings plus cross-domain overlap remain separate
future modes.
Forward-only v45 promotes one exact source-session crash boundary. After the
runner durably records provider preparation but before any snapshot query or
target batch, the first actual provider `query` callback returns
`ConnectionResetByPeer`. The failed source object is then deinitialized; durable
status drives a resumed snapshot through a strictly newer owned provider
session, followed by a third independently owned streaming session. This is a
provider-operation failure, not an exception thrown by the lifecycle hook.
The property requires the exact typed query failure, exact source generations
1/2 for failure/recovery, exactly three source sessions opened and closed, a
peak of one live session and zero terminal sessions, one failed attempt,
exactly three target attempts and successes,
three final documents through every public coordinator, unchanged pre/post-
heal work, every existing cluster oracle, cleanup, and fresh-world replay. Its
dedicated 220,000-transition Debug and ReleaseSafe gates pass 15/15. Additional
source-crash timings or source-process loss, additional cancellation timings,
other topology/target-crash timings, metadata leadership loss during cutover,
and cross-domain overlap remain separate modes.
Forward-only v46 promotes one exact durable-cancellation boundary. After the
first snapshot target batch succeeds and offset 1 is persisted, the fixture
revokes its production work lease. The next work checkpoint must return
`CdcWorkLeaseLost`, close source generation 1, and resume from durable status
through generation 2; generation 3 independently owns streaming. The
compositional permit wrapper validates the lease before delegating to the
cluster service-rate permit, so installing production charging cannot disable
fencing or cancellation. The property requires offset 1, the exact typed
error, generations 1/2, exactly three opened/closed sessions with peak one and
zero live at completion, exactly three target attempts and successes (no
duplicate committed batch), unchanged charged work, all-node visibility,
every cluster oracle, cleanup, and fresh-world replay. Its dedicated 220,000-
transition Debug and ReleaseSafe gates pass 15/15. Earlier/later cancellation,
other topology-change timings, metadata leadership loss during cutover,
source/target process overlap, and other cross-domain fault shapes remain
separate modes.
Forward-only v47 promotes one exact stale-work-owner boundary. The first owner
successfully applies `doc:d`, then loses its lease at the
`snapshot_batch_applied` lifecycle boundary before offset 1 is durable. The
production snapshot runner now revalidates `WorkPermit` ownership between
target apply and checkpoint publication, so that owner must receive
`CdcWorkLeaseLost` while durable offset remains 0. Source generation 1 closes;
generation 2 resumes from offset 0 and idempotently reapplies `doc:d`; generation
3 owns streaming. The property requires exact failure/recovery generations,
three balanced non-overlapping source sessions, four target attempts and four
successes, exactly one replayed batch, all-node final visibility, charged work,
every cluster oracle, cleanup, and fresh-world replay. Its dedicated 240,000-
transition Debug and ReleaseSafe gates pass 15/15. V48 promotes the exact
metadata topology and cutover-authority rotation below; other ownership-loss
timings and cross-domain overlap remain separate modes.
Forward-only v48 promotes the source-catalog/authority boundary through the
live metadata quorum. Before provider work begins, the fixture publishes an
exact-cutover source definition for production table 6841 through metadata
Raft and waits for all three projections. Provider generation 1 persists
intent and authority A through the atomic cutover-claim command. After its
first target batch succeeds, the lifecycle boundary publishes source config v2
through metadata Raft without throwing a scripted failure. The runner's real
post-apply authority check observes the byte-exact catalog mismatch and returns
`ReplicationSourceConfigChanged` while durable offset remains 0. Generation 2
then derives a new intent and authority B, atomically claims against A, carries
and completes A's physical slot/publication retirement, and replays the
undurable batch. Generation 3 independently owns streaming. The property
requires two prepared snapshots, two durable claims, one retirement, distinct
nonzero authorities, the exact typed failure, three balanced non-overlapping
source sessions, four target attempts and successes, all-node visibility,
charged work, every existing cluster oracle, cleanup, and fresh-world replay.
Its dedicated 260,000-transition Debug and ReleaseSafe gates pass 15/15.
Additional topology-change timings, metadata leader loss between claim/check/
retirement, and link/storage/resource/process overlap remain separate modes.
V42-v49 directly add their modes, properties, trace revisions,
observations, and build targets; there are no legacy aliases, readers, or
migration paths.
V49 reaches the cache instance embedded inside the production `ApiHttpServer`
through an owner-scoped keyed operation and the server's real ResourceManager
budget. It does not claim that a complete public semantic HTTP request or an
external inference provider executed; that end-to-end boundary remains
distinct follow-up breadth.
Each named complete gate exact-replays and passes 15/15 at its explicitly cited
optimization checkpoint.
This is distributed process-lifecycle coverage at an application-owned
`std.Io` boundary, but not an arbitrary process matrix: metadata/coordinator
restart, multiple simultaneous owner failures, process loss composed with
storage/resource/partition faults, disjoint placement, derived-state equality,
bounded retained-history replay, snapshot-install rehydration, and co-resident
v9 fault breadth remain the work below.

### Derived Workflows

The scenario schedules checkpoint, generation publication, repair, compaction,
cancellation, cleanup, and leadership fencing through the production
`DurableJobLane` adapter. Its focused gate composes real enrichment, index
lifecycle, repair, and background-lane regressions.

Focused gate: `derived-workflow-vopr-test`.

### Backup and Restore

The scenario models partial and duplicate transfer, crash/resume, manifest
publication, retention pins, durable restore jobs, download, topology
reconstruction, activation versus cancellation, and generation GC. It
round-trips and verifies the production HA backup manifest; the focused gate
also runs portable, restore-job, Raft restore, standalone, and HA regressions.

Focused gate: `backup-restore-vopr-test`.

### Clock Faults

The Antfly-independent clock surface separates realtime jumps, oscillator
frequency, node pause, monotonic passage, timer delivery, and stabilization.
The focused gate composes production TTL, transaction lease, HA retention, and
seed-lifecycle regressions.

Focused gate: `clock-fault-vopr-test`.

## Defects Found

VOPR work has found concrete production and harness defects:

- The v38 global-query authorization history found that each NDJSON line was
  checked only against the identity snapshot admitted at request ingress. A
  permission revoked after the first table result therefore remained usable by
  later lines in the same request. Multi-stage graph target authorization and
  global-query line dispatch now intersect the admitted scope with current
  Basic or API-key authority; later grants cannot broaden an in-flight request,
  while deletion, expiry, and revocation fail closed before the next protected
  operation. The exact history requires one permitted result, an exact 403
  without a protected result, policy restoration, and a complete fresh retry.
- The v39 global-query transport history found that ordinary remote shard
  execution let `SendFailed` escape from serial and parallel query fanout. The
  public boundary consequently returned a generic 500 even though graph fanout
  already classified the same internal transport failure as retryable
  distributed-query unavailability. Remote query fanout now normalizes
  operational failures at the shard boundary through the generic
  `normalizeDistributedQueryOperationalError`; the registered-link history
  requires one exact semantic-stream match, an exact machine-readable 503 with
  no partial response, explicit healing, and exact fresh-request recovery in
  Debug and ReleaseSafe. The focused
  `lib-api-distributed-query-availability-test` keeps the shared classification
  contract in ordinary root-test discovery.
- The v40 global-query process-loss history found that Raft leadership and
  catalog routing recovery were insufficient reconstruction evidence. The
  replacement DataServer could have a stable identity, rebound endpoint, and
  locally readable group while the public client still held a stale connection
  to the destroyed listener; its first idempotent request failed with
  `SendFailed`. Reconstruction now has a deterministic lifecycle owner and a
  bounded exact-endpoint readiness check that requires the rebound public
  listener to serve the durable tenant document before recovery is published.
  The complete history then requires the exact no-partial 503 from the failed
  operation and the exact ordered two-table response from a fresh request.
- The v41 cluster composition audit caught a VOPR harness scheduling defect
  before promotion: cache phase barriers initially polled at 1 ns, which would
  stay globally earlier than the production cluster's 1 ms Raft/service
  timers and spend the history budget exploring the polling loop. The cache
  lifecycle barriers now use the cluster cadence and wait on semantic cache
  evidence (`coalesced_waiters`) rather than a merely started task. This was a
  harness-quality defect, not an Antfly production defect; the final strict
  property and fresh-world replay pass without weakening the workload oracle.
- The v49 fidelity audit found that the earlier cluster history instantiated a
  production cache type beside the cluster and merely assigned it node 1's
  service-rate identity. Destroying the `DataServer` therefore could not
  destroy that cache, so it was not evidence about the cache actually owned by
  `ApiHttpServer`. V49 removes the adjacent cache, installs the generic work-
  cost port only after `startPublicHttp()` creates the production owner, and
  reinstalls it on every reconstructed incarnation. The first integration
  attempt installed the port before that lifecycle boundary and deterministically
  trapped on the absent server; the final history proves the replacement cache
  starts empty and the persistent public client requires one bounded reconnect.
  These were coverage-fidelity and fixture-ordering defects, not newly found
  Antfly data corruption.
- The v42 composition first reused focused keys below the cluster's published
  lexical range. Public routing correctly returned `NotFound`; the fixture now
  uses in-range keys. This was a harness-boundary defect, not a production
  routing defect, and the final oracle reads every replicated document through
  all public coordinators.
- The first v42 target adapter duplicated a reduced insert/delete serializer
  and rejected the focused stream's upsert transform. The adapter now calls the
  canonical production `encodeBatchRequest`, so snapshot inserts and CDC
  transforms share the same public wire contract. This was an integration-
  adapter defect; record and fresh-world replay now prove the full transform
  path without a compatibility shim.
- The first v43 oracle inherited v42's exact three-batch target count. The
  schema-change interruption occurs after the first batch is durably accepted,
  so resume correctly replays that batch before advancing; the production
  target therefore observes four successful batches while final state remains
  idempotent. V43 now requires that exact duplicate instead of accepting an
  open-ended count. This was an oracle defect caught by the new composition,
  not a production data-loss defect.
- The first v44 target-owner restart fired as soon as replication accepted its
  first batch, while unrelated public workload handlers could still borrow the
  same DataServer. Teardown consequently surfaced `ClientShuttingDown` in
  ordinary cluster work and obscured the replication invariant. The history
  now waits for the public graph workload's terminal semantic boundary while
  the external replication completion fence keeps every owner and driver live.
  This was a composition-ordering defect; the final history still crashes the
  owner before the next replication batch and does not serialize recovery with
  the replication retry.
- V44 then exposed an unsafe observation window: `primaryGroupProgress`
  sampled a DataServer's multi-Raft map while the stable array slot was being
  destroyed and reconstructed, producing an alignment panic. Restart already
  published `data_server_paused` as the process exclusion boundary for Raft and
  control drivers; observations now honor the same boundary. The final oracle
  also records the real executor contract—`SendFailed` at the stopped endpoint
  plus one bounded stale pooled-connection retry—rather than assuming a lower-
  level `ConnectionRefused` or resetting the client to hide reconnect behavior.
  These were harness/observation defects, not evidence of lost production data:
  the replacement proves local `doc:d` durability, direct public availability,
  and final all-coordinator visibility.
- The v45 audit found that the focused `source_crash` mode returned
  `ConnectionResetByPeer` from a lifecycle hook immediately after preparation.
  That proved durable runner resume but did not prove failure, destruction, or
  replacement of the provider object that owns the query. The fixture now
  allocates each source session independently, arms the fault at preparation,
  fails the actual first `query` callback, closes that exact generation, and
  requires resume through the immediately newer generation with all opens and
  closes balanced, no overlap, and zero terminal sessions. This was a test-
  evidence defect rather than an Antfly data-
  path defect; the full-cluster v45 gate preserves exactly three target
  attempts/successes and every existing visibility/cleanup oracle.
- The first v46 run recorded durable cancellation but did not fail the worker:
  installing the deployment service-rate `WorkPermit` had replaced the
  fixture's lease-validity permit. Terminal evidence showed
  `cancellation-injected=1`, but `first-attempt-failed=0`, two source sessions,
  and otherwise successful replication. The permit is now a compositional
  wrapper: it validates lease/fencing first, then delegates checkpoint cost and
  deadline to the production permit. The focused charging gate and full v46
  history prove both capabilities remain active. This was a harness capability-
  composition defect that could have produced false cancellation coverage, not
  a production data-path defect.
- The stale-owner audit found an ownership-sensitive gap in the production
  snapshot runner: after a target batch returned success, the runner invoked
  the lifecycle boundary and then advanced durable source progress without a
  second `WorkPermit` check. A lease lost in that gap could therefore let a
  stale worker publish checkpoint progress. The runner now revalidates
  ownership between target apply and checkpoint publication. V47 proves the
  stale owner receives `CdcWorkLeaseLost` at durable offset 0 and a replacement
  performs exactly one idempotent target replay before normal completion. This
  is a production fencing fix; it prevents stale progress even when eventual
  target state would otherwise appear correct.
- The replication-topology audit found that the focused `topology_change`
  mode only renamed a fixture table and returned an injected connection error.
  It never created a prepared exact-cutover snapshot, published the source
  catalog through metadata Raft, checked a committed authority, or retired a
  predecessor, so it could not substantiate topology/authority coverage. The
  focused mode now exercises two exact authorities and one retirement, while
  v48 delegates the same source and status transitions to the live three-node
  metadata quorum and binds the runner to the production table ID. This was a
  coverage-fidelity defect rather than a newly discovered production data-path
  defect.
- Replaying the adjacent v27 authorization history after v38 exposed a real
  internal graph-hydration wire mismatch. The handler serialized the production
  `GraphHydrateResponse` directly, yielding nested
  `incoming_index_identity`, while the canonical remote decoder requires the
  flattened `incoming_index_incarnation` and
  `incoming_index_config_hash` fields. A locally routed request passed, but a
  fresh recovery routed through internal HTTP and failed with `UnknownField`.
  The handler now uses the canonical hydrate encoder; graph-edge responses use
  their canonical encoder as well. A focused wire regression and the complete
  v27 Debug/ReleaseSafe replay prove the remote recovery path.
- The remote generation/reranking composition found that the production
  Antfly generator unconditionally installed a 300-second per-request timeout,
  overriding the timeout owned by its borrowed `httpx.Client`. A caller asking
  for a bounded generation request could therefore receive a successful late
  response instead of `Timeout`. The provider now inherits the client policy;
  the v2 chain history proves a 10-millisecond logical deadline beats a
  50-millisecond remote response and fresh-world replay reproduces it.
- The same history found that canceling a `std.Io.Future` during an HTTP/1
  response read could surface `RecvFailed` or `InvalidResponse` instead of
  `Canceled`. Narrow socket/reader adapters intentionally re-publish task
  cancellation when their error sets cannot carry it, but the HTTP retry
  boundary did not consume that signal before classifying the narrowed
  transport error. The client now restores typed cancellation before retry or
  response classification, and retry backoff no longer swallows cancellation.
  Remote generation and reranking cancellation after server ingress both
  exact-replay as `Canceled`.
- The first live-event observer called its output sink inline from the runner,
  so a slow or blocked diagnostic consumer could perturb or deadlock a history
  despite the feature being described as diagnostic-only. The forward-only v2
  stream now publishes only into caller-owned bounded slots and drains sinks
  outside execution, with explicit overflow, close, retry, and delivery
  evidence. The same audit found that overlapping fractional service-rate
  effects were evaluated in activation order and reordered by healing, making
  rounded cost depend on fault history rather than the current active set.
  Effects are now kept in canonical fault-ID order, and an order-sensitive
  regression proves equivalent active sets have identical cost.
- Transaction recovery retained the address of the temporary `DB` wrapper
  constructed inside `DB.open`, although the wrapper is returned by value.
  ReleaseSafe poisoned the stale address and recovery later entered the HA
  mutation barrier through it. Recovery now owns a separately allocated
  callback context with an atomic binding to the stable caller wrapper; close
  clears the binding and joins recovery before tearing down dependent state.
- The standard HTTP listener shutdown path could close the listening socket
  before waking the accept loop, producing Debug `BADF` failures. Shutdown now
  wakes the loop before close.
- A stop published before listener ownership, or after `listen` returned,
  could try to wake through an unacquired/released `HttpRuntime` lease. Wakeup
  is now gated by the listener's atomic ownership publication; startup still
  observes the already-published stop on both sides of bind.
- The v25 public-cancellation gate found that the production-owner cluster had
  not installed VOPR's backend-neutral HTTP disconnect probe. Canceling a
  public request could therefore leave the listener token unset while graph
  hydration completed. The initial repair treated any ordered FIN as peer
  abandonment; the later DataServer half-close gate exposed that a client may
  validly finish request bytes with `shutdownWrite` while retaining its read
  side for the response. `VoprIo` now carries write-half FIN and read-side
  abandonment as distinct ordered controls behind all preceding bytes. Only a
  full close or reset cancels the handler, and v25 still proves canceled fanout
  has no completion before a clean retry. The ambiguous disconnect APIs were
  renamed directly rather than retained as aliases.
- The v30 durable-join cancellation gate found that internal join handlers
  called `RequestContext.ensureActive` and then discarded that request's
  cancellation and deadline by executing with the process-scoped
  `JoinContext`. Public cancellation could tear down the coordinator while
  partition work continued without a semantic token. All finalize, rows,
  unmatched, and partition operations now derive a request-bound join context,
  and join-engine `Cancelled` maps to the API's canonical `Canceled` result.
  The exact-replay property parks at the real partition-worker boundary,
  requires the canceled worker not to complete, and requires a clean retry.
- That same gate found that v30 encoded an opaque 64-bit durable job identity
  into the signed observation feature domain with numeric `@intCast`. Valid
  high-bit job IDs therefore failed in safety-enabled builds. Identity evidence
  now uses the same bit-preserving `@bitCast` convention as body digests, while
  semantic nonzero checks remain on the original `u64`. This was a harness
  observation defect and is not counted as a production join defect.
- The expanded ReleaseSafe VOPR CLI/meta artifact peaked at 15.05 GiB while its
  macOS build step still declared a 12 GiB ceiling, so the compiler was killed
  after successfully reaching the test. The focused meta gate now uses the
  same 16 GiB macOS ceiling as the largest production-owner VOPR gate; the
  non-macOS ceiling is unchanged.
- The focused DataServer deadline scenario always preferred logical time from
  startup but still required server ingress and response preparation. It could
  therefore pass or fail based on whether the request reached the listener
  before its timer, without exercising the cancellation seam its name claimed.
  The deadline mode now enables the stable production request safepoints,
  drives non-time work until ingress is observed, and only then lets logical
  time win. Response preparation may be preempted or race safely with client
  cancellation; admitted ingress, the client error, absence of a received
  response, drained API leases, and cleanup remain mandatory.
- The independent-domain checkpoint exposed a latent compile defect caused by
  a local durable-job-lane variable shadowing the `lane` method.
- The distributed-fanout service-rate checkpoint exposed a stale native graph
  regression that still assigned a raw atomic pointer after request
  cancellation became the semantic `CancellationToken` capability. The test
  now constructs the canonical token directly; no pointer overload or legacy
  adapter was added, and the full root gate compiles and passes.
- The full-cluster v23 gate exposed a stale restart assignment left behind by
  the read-index/safety capability split: the public-cluster harness tried to
  reset the deleted `requester` field. Restart now resets the canonical
  `read_safety_barrier` directly. No compatibility field or alias was added.
- httpx bypassed its supplied `std.Io` backend for ordinary POSIX reads and
  listener creation. That made virtual sockets incomplete and split timeout
  semantics between host and modeled I/O. Reads, ordinary listen, and logical
  timeouts now stay on the injected backend.
- httpx listener shutdown used a hidden global `std.Io.Threaded` loopback
  connection to wake `accept`. On `VoprIo` that mixed unrelated handle spaces
  and failed with `BADF`; the wake connection now uses the listener's own I/O.
- Shared httpx clients admitted requests while provider-runtime teardown was
  freeing their transports and provider configuration. `Client.shutdown` now
  closes admission, optionally cancels active I/O, and drains every committed
  request lease before destroying shared state; the media campaign exact-
  replays cancellation and replacement with an accepted request in flight.
- The production-owned cluster initially advanced metadata's deliberately
  coarse manual clock on every 1 ms data-Raft driver tick, manufacturing
  metadata election churn. Production control now borrows the same `VoprIo`
  clock and advances the metadata harness only after a data control round can
  have produced transition traffic.
- External modeled storage had no durable physical-root incarnation. Reopening
  the same logical path through a different device could therefore collide
  with or silently reuse writer identity. `DB.OpenOptions` now accepts an
  external root incarnation; Lite persists one per physical root and modeled
  DB devices derive a stable device-plus-path identity. Focused Lite and
  modeled-configurator regressions pass.
- External-data-plane metadata correctly stopped creating shadow hosted data
  replicas, but originally left its local transition executor alive. Once real
  DataServers used the same physical roots, both owners attempted transition
  work and produced writer conflicts. External mode now retires the local
  executor and installs production hosted-operation adapters backed by the
  committed catalog and real DataServer HTTP endpoints.
- The production full-cluster substitution exposed a second shared-client
  lifetime case. A public write can form a nested public-client → metadata-
  client → data-Raft-client dependency, leaving one admitted request lease in
  each lane when a bounded history is canceled. Publishing shutdown on every
  lane before draining any one lane fixes the ownership cycle, but callback-
  backed observers were still parked until their next poll. `httpx` cancellation
  tokens now carry an optional wake word; request-gate shutdown publishes and
  wakes it immediately, and request completion wakes either park target. The
  full HTTP client suite and focused immediate-wake regression pass. The v11
  production-owner baseline now completes and exact-replays. Later v12 records
  unwind the active hosted callback, while the full exact-replay gate remains
  open for a different reason.
- The first deep v12 run exposed two distributed defects rather than a
  completed feature. During destination bootstrap the elected two-voter group
  accepted proposals, but its applied index repeatedly lagged the new target;
  retries appended more work and surfaced `RaftBatchWriteOutcomeUnknown`.
  Cutting off that history then left a hosted callback in the nested metadata-
  to-data HTTP graph, and the deterministic cancel/drain suffix exceeded its
  transition budget. Driving control and Raft independently, publishing every
  nested owner's stop before drain, and propagating cancellation through write
  retry loops removed those blockers; a later record reaches publication and
  the post-split read. Subsequent model revisions made the remaining divergence
  precise instead of treating this early diagnosis as the final root cause.
- `VoprIo` teardown repeatedly chose the first stable task. A runnable task at
  the end of the stable set could starve throughout the bounded suffix even
  though every step was deterministic. Teardown selection now uses a stable
  round-robin cursor and its regression proves every runnable owner advances.
- Two table-write retry loops caught `Canceled`, re-canceled the current I/O,
  and continued retrying. That converted cooperative shutdown into a permanent
  loop. They now propagate the cancellation after preserving its token.
- DB transaction-maintenance workers had no publish-before-drain lifecycle.
  A deterministic parent could destroy a cache or DB while a nested worker
  still borrowed it. The transaction runtime, DB, provisioned write caches,
  and DataServer now expose a begin-teardown phase that publishes all stop bits
  before scheduler drain and destruction.
- DataServer cache TTLs, reconciliation suppression, repair scheduling, status
  refresh, startup catch-up, provisioning, and runtime metrics sampled host
  clocks even when the owner borrowed `VoprIo`. `RemoteMetadataSource` did the
  same for request budgets and polling. Those choices now use the borrowed
  `std.Io` clock; group-status freshness is normalized into that clock domain
  before metadata reconciliation.
- `DataServer.refreshRemoteMetadataSnapshot` promised an explicit refresh but
  called the ordinary cached fetch path, so a just-published catalog or
  document-identity update could remain hidden for the snapshot TTL. The
  production-owner graph composition reproduced the stale observation. The
  method now invalidates the cached head and snapshot before fetching.
- Public reads through `ProvisionedTableReadSource` assumed that the accepting
  `DataServer` owned every routed group. A request accepted by another node
  attempted its local RawNode, failed `UnknownGroup`, and returned HTTP 500;
  the former no-op readable-lease requester had hidden the routing defect.
  Public Provisioned lookup, scan, query, preflight, and artifact operations
  now adapt to the production hosted router while internal group-local
  endpoints remain on resident owners. The first adapter repair had a second
  ownership defect: a route that resolved back to the accepting node used the
  hosted direct-open path and bypassed the Provisioned resident DB and read-
  admission owner. The hosted coordinator now carries the original local
  group source and delegates every local lookup, scan, query, artifact,
  preflight, statistics, algebraic, join, and graph phase through that owner.
  Remote phases still cross the typed internal HTTP endpoints. A follow-up
  ownership audit found that several group-local helpers also claimed an outer
  read-admission lease unconditionally. That claim is now propagated from the
  actual `ReadPreparation.Activity`; when no outer owner exists, the resident
  owner self-admits. Routed algebraic aggregation uses a catalog-only planner
  and admitted group-local partial callbacks rather than borrowing a resident
  index pointer beyond its lease.
- The managed readable-lease requester treated enqueueing a Raft ReadIndex as
  completion. Data Raft now assigns a request identity and completes only
  after the matching ReadState index is applied locally; the graph barrier
  then waits for the corresponding full-index visibility before read admission.
  A strong distributed graph phase no longer falls back from `NotLeader` to a
  successful stale result. These repairs convert the former partial HTTP 200
  into a complete v13 result and exact replay.
- The first production-owner distributed join reached a right group through an
  accepting node that did not own it. `HostedProvisionedTableReadSource` ran
  the exact-group callback against that node's local source, returned
  `UnknownGroup`, and the join boundary collapsed the ownership result into an
  HTTP 500. Exact-group query callbacks now resolve the current route and cross
  the typed internal HTTP endpoint when remote; stale ownership and transport
  outcomes normalize to retryable `distributed_query_unavailable` rather than
  an internal failure.
- After that routing repair, an acknowledged `full_index` batch was visible to
  point lookup while a public match-all query returned HTTP 200 with zero hits.
  The primary join then skipped execution entirely; after its barrier was
  repaired, the optimized right-side `SearchResult` path exposed the same
  defect independently. Production Provisioned preflight, response-producing
  group query, and optimized group-result query now use the combined applied
  ReadState/full-index barrier whenever the data-Raft deployment installs it,
  and acquire table read admission only after that wait. V19's exact two-row
  join oracle prevents either empty-success form from regressing silently.
- The active-transition driver waited for another complete control round after
  publication, creating avoidable background work and packet choices during
  handoff. An explicit active-round handshake now disables and joins control at
  the exact safe boundary. Hosted structural-operation polling now also uses a
  dedicated non-pooled `VoprIo` client, so its control-plane connection
  lifetime cannot couple repeated observation to unrelated pooled public
  traffic. Public, metadata, and Raft paths continue to exercise keep-alive.
- `VoprIo` originally derived a futex identity from a pointer for the lifetime
  of the virtual OS. Reusing the address for a later, unrelated contention
  epoch could therefore expose a stale completion identity during clean-world
  replay. Virtual-OS model v2 scopes pointer identities to active contention
  epochs and retires them when the last waiter leaves; a focused reuse
  regression preserves the new contract.
- A virtual connect that arrived before `accept` made the connection directly
  available to the consumer and omitted the readiness transition that appears
  when `accept` waits first. Producer arrival order could consequently change
  the scheduler's enabled set. Successful connects now always enqueue an
  explicit accept-readiness completion, and external wake sequence numbers are
  local to the logical resource. The focused producer-first/consumer-first
  regression requires identical readiness identity in both orders.
- `WriteCacheTransitionLocks` ordered the production write and startup cache
  mutexes by allocator address. A fresh process could reverse those addresses
  and therefore the futex acquisition order in an otherwise identical v12
  replay. The lock order is now the stable semantic role order—write cache,
  then startup cache—and the cache-lifecycle shard includes a deliberately
  reversed-address regression.
- `VoprIo` task and futex IDs originally shared a process-global creation
  ordinal. Live inspection of the choice-36,298 v12 divergence showed that the
  same numeric child-task ID represented a metadata-Raft batch request in one
  world and a DataServer public-listener wake in the other. The enabled IDs
  could therefore remain superficially equal until those unrelated operations
  published different completions. Model v3 derives child-task and futex
  identities from their logical parent plus a parent-local epoch, then binds a
  first external waiter to its logical resource and a resource-local epoch.
  The global ordinal remains diagnostic only. Focused regressions create the
  same logical parent/child/futex graph under different global allocation
  interleavings and bind listener tasks created at different root ordinals to
  the same stable external scheduler identity.
- Parent-local child ordinals were still too coarse when two different
  production task roles were spawned in opposite order before either reached
  its first external wait. Model v4 classifies a fiber by its parent and an
  ASLR-independent start-callsite offset, with a callsite-local epoch. A
  regression creates distinct callsites in opposite global orders and requires
  the same IDs; persisted trace compatibility pins the target and source
  revision that define those offsets.
- Exact choice replay compared the enabled stable IDs but did not immediately
  compare the selected transition's metadata. The v12 choice-37,718 failure
  demonstrated why IDs are necessary but insufficient: record and replay
  selected the same packet ID/actor/resource while reporting 315 and 641 bytes
  respectively. Replay now compares the selected name, kind, actor, resource,
  parameter, and payload digest with the recorded `TransitionRecord` and fails
  with `ReplaySelectedTransitionDiverged`; a focused regression preserves the
  fail-closed behavior.
- The network model allocated listener, connection, and packet IDs from one
  process-global sequence. Choice 37,718 showed the same numeric client/server
  socket pair and packet ID representing different logical HTTP connections:
  replay simultaneously published the port-20007 accept wake and sent 641
  bytes where record sent 315. Listeners now derive identity from their stable
  endpoint and epoch, accepted connection halves derive from that listener,
  and each source socket owns its packet/FIN/datagram sequence. A regression
  creates and connects two listeners in opposite global orders and requires
  identical endpoint-local identities.
- Listener scoping alone still allowed concurrent clients of the same endpoint
  to exchange connection ordinals. VOPR gives connect/accept a provisional
  owner-scoped identity, then atomically rebinds both halves from the logical
  owner plus first stream payload and migrates any parked read/write waiter
  before delivery can become a choice. Model v5 fixed the remaining case where
  two distinct logical clients sent identical first payloads and could exchange
  the content occurrence ordinal. Focused regressions reverse same-listener
  connect and first-write order with identical payloads and require matching
  owner-specific steady-state socket identities.
- The first model-v5 v12 status retry still made a task's next outbound
  connection the child of that task's mutable scheduler identity. Parking on
  the preceding response socket had rebound that identity, so a repeated,
  byte-identical `observe-split` request formed a chain through prior socket
  occurrence IDs and changed across clean worlds. Model v6 keeps an immutable
  resource-creation owner beside the externally bound scheduler identity. The
  focused task/network regressions pass, and the original socket-alias failure
  is gone; the deep history nevertheless exposes a later request/response
  readiness ordering escape in the same production status region, so this is
  recorded as a repaired defect plus a distinct open blocker rather than a v12
  completion claim.
- Packet replay initially treated the byte count as sufficient payload
  evidence. Once endpoint-local identities removed the choice-37,718 alias,
  an equal-length difference reached choice 8,640 under the same logical HTTP
  connection. Packet transitions now include a semantic digest of their bytes;
  a focused regression sends different equal-length payloads and requires
  replay to distinguish them.
- The new packet digest identified the equal-length difference as a production
  DataServer status report. `ProvisionedGroupStorage.attachSources` installed a
  host-filesystem capacity probe even when the owner ran on `VoprIo`, so
  `capacity_bytes` and `available_bytes` serialized physical machine state into
  modeled HTTP. DataServer configuration now accepts an operator-owned
  `CapacitySource`; the full-cluster fixture installs one per node over that
  node's virtual replica-root and catalog prefixes, while ordinary production
  startup retains the physical probe default. Prefix-accounting and the
  content-sensitive production smoke replay preserve the boundary.
- The production control task previously ran continuously while the workload
  polled transition status, so record and replay could observe adjacent control
  rounds at the same logical boundary. It now parks on a request semaphore,
  executes exactly one round, and publishes one completion semaphore before
  the requester may observe status. Raft ticker tasks remain independently
  scheduled. Current full-cluster failures exact-replay, closing the old
  166-versus-165 round drift without serializing Raft behind metadata control.
- The full-cluster runner asserted the scenario oracle before fresh-world
  replay. A newly found property failure could therefore exit without proving
  that it was reproducible. It now exact-replays every recorded history first,
  then evaluates the expected completion or bounded-lifecycle result.
- Two pre-proposal DataServer readiness branches returned
  `RaftBatchWriteOutcomeUnknown` even though no proposal could have been
  accepted. They now return retryable `LeaderUnavailable`; post-acceptance
  failures retain the explicit ambiguous contract. The full-cluster workload
  also distinguishes acknowledgments from ambiguity, never retries an
  acknowledged write, and retries only its known-idempotent fixed-ID upserts
  after bounded reads fail to resolve the outcome.
- Model v5 lifecycle evidence disproved the apparent post-acceptance Raft
  liveness diagnosis: every failing v11 request stopped before proposal
  acceptance. Sampling the live process found `prepareResidentDbForReadRetry`
  sleeping in native mutex backoff while its owner needed the same single
  borrowed-I/O scheduler to run. Resident-open contention on borrowed I/O now
  uses `tryLock` and returns the existing retryable
  `StorageReadTemporarilyUnavailable`; native threaded runtimes retain their
  blocking wait. A focused regression proves the borrowed scheduler keeps
  making progress.
- Once that scheduler escape was closed, all forwarded writes failed before
  the routed handler with an unmarked 503: the production composition had not
  configured its internal-service identity, so the deliberately fail-closed
  middleware rejected the requests. The fixture now supplies one issuer and
  secret to both every DataServer and every forwarding adapter; it does not
  weaken or bypass authentication.
- Enabling authentication exposed a true replay escape. Internal-service JWTs
  were signed from host realtime even though their HTTP transport used
  `VoprIo`, producing different packet digests in a fresh world. The generic
  request executor now optionally supplies a realtime authority, the borrowed-
  I/O executor derives it from `std.Io.Clock.real`, signing uses that authority,
  and server verification uses the owning backend runtime's same clock. The
  focused signing regression fixes time at 42 seconds and requires the exact
  expected token. With all three repairs, v11 passes 30/30.
- A fresh run of the focused three-owner composition exposed
  `SplitReplicationSequenceGap` when the first relevant source delta used Raft
  index 7 after an unrelated index 6 entry. Split watermarks intentionally use
  sparse Raft indexes, so requiring `sequence == applied + 1` confused an
  irrelevant log entry with omitted replication work. Each new delta now
  carries its exact predecessor; the destination accepts a sparse advance only
  when that predecessor equals its durable watermark, while duplicates remain
  idempotent and legacy requests retain consecutive validation. A durable v4
  protocol barrier prevents old replicas from silently applying the new
  envelope.
- The repaired history then showed every replica with a finalized source
  terminal but an empty destination ownership range. Destination checkpoints
  updated the document DB but were not projected into the Raft apply store used
  by topology observation and snapshot transfer. Non-source checkpoints now
  project the same range in their committed entry, with a focused apply-store
  regression.
- After owner restart, background projection could widen the finalized source
  from `[doc:a,doc:t)` back to the document DB's physical pre-cutover
  `[doc:a,)` range. Source lifecycle entries deliberately bypass that DB, so a
  terminal split makes the apply-store range authoritative during document
  reconciliation. The three-owner restart history proves that boundary across
  every replica and exact replay.
- The same restart proof initially passed the specialized data-Raft peer
  executor to a public hosted-operation adapter. That executor correctly
  rejected the rotated public endpoint even though the listener was healthy.
  The composition now owns a general `IoHttpExecutor`, matching the production
  hosted-adapter contract and keeping routed terminal retry on `VoprIo`.
- Early cancellation of the production composition reached a fixture allocated
  from poisoned debug memory before its optional serverless and completion
  Futures had been initialized. Teardown interpreted the poison as live Future
  pointers and failed alignment checks. Every optional Future is now explicitly
  initialized before the initialization task can fail, and the bounded-
  lifecycle exact gate exercises that early teardown twice.
- `VoprIo` could cancel host-owned task records without resuming their stacks,
  skipping `defer` cleanup and leaking nested owner state. It now exposes a
  bounded post-history cancel-and-drain suffix that cancels every current and
  newly spawned task, schedules deterministic unwind, and host-reaps completed
  task records. A focused nested parent/child Future regression proves both
  defers execute and task ownership reaches zero.
- Canceling a task parked in `Future.await` made the parent runnable before the
  child completed. The parent could unwind and destroy state still borrowed by
  the child. `Future.await` is no longer treated as a cancellation point: the
  child completes first, wakes its waiter through the normal finish path, and
  only then can the canceled parent unwind. The same nested teardown regression
  covers this ordering.
- An HTTP request watchdog treated its ordinary `.stopped` Select result as
  unreachable. Parent-task cancellation during a bounded history legitimately
  produces that result and previously panicked during teardown. Request and
  writer paths now translate it to the appropriate canceled error, with focused
  cancellation tests and the production bounded-lifecycle exact replay as
  evidence.
- Data-Raft's stable-metadata-epoch fast path also treated peer transport
  endpoints as if they were placement identity. Publishing or rotating a
  store's Raft URL without changing placement epoch could therefore leave peer
  routes empty or stale. Reconciliation now maintains a separate transport
  fingerprint over group peers, store identity/liveness, and Raft URLs; the
  full composition still needs a focused endpoint-rotation regression before
  this fix is promoted as verified.
- A borrowed DataServer runtime could still construct a native
  `StdHttpExecutor` and listener for data-Raft when its backend exposed general
  `std.Io` but no specialized Raft lanes. `DataServerConfig` now accepts a
  caller-owned request executor and external-listener ownership, allowing the
  v11/v12/v13/v14/v15/v16/v17/v18/v19/v20/v21/v22 production modes to keep the real Raft HTTP codec and
  handler on `VoprIo` with no hidden Threaded transport. The production
  defaults remain unchanged.
- Serverless public-catalog teardown published `public_live = false` only after
  poisoning its client, listener, and status owners. Parent-history
  cancellation could enter a second fixture teardown while that flag was still
  true and deinitialize the poisoned client again. Ownership release is now
  published before teardown, and the focused serverless workflow exact gate
  passes with the idempotent shutdown path.
- Media-runtime startup published provider globals as each provider loaded.
  When a later provider failed to initialize, the earlier thread-local global
  could outlive its rolled-back allocation. Startup now loads every provider
  before publishing any global, and the media campaign preserves this partial-
  startup rollback contract.
- `HttpRuntime` unconditionally created three hidden Threaded executors and a
  native descriptor observer. It now supports caller-owned backend-neutral
  lanes, preserves bounded admission, and fails closed when native disconnect
  observation is requested from a backend that cannot provide it.
- The metadata HTTP test runtime accepted a caller's `std.Io` but did not tell
  httpx to borrow it for accept and connection work. A VOPR composition could
  therefore place the listener on a hidden Threaded runtime and observe
  deterministic clients failing with `ConnectionRefused` while work escaped
  the selected schedule. The runtime now borrows the supplied I/O, disables
  host-only timeouts and descriptor observation, and keeps bounded connection
  admission inside the caller's capability domain.
- Borrowed HTTP runtimes could not observe a peer reset because the native
  descriptor observer cannot inspect virtual socket handles; DataServer
  consequently disabled hard-disconnect cancellation under deterministic I/O.
  httpx now accepts a backend-neutral probe, and `VoprIo` distinguishes reset,
  full read-side abandonment, and an ordered write-half FIN even when unread
  pipelined bytes remain.
- Supplying server TLS certificate and key paths only printed a warning and
  continued serving plaintext. Binding now rejects incomplete TLS
  configuration and fails closed before reserving runtime or socket capacity;
  the supported production boundary remains explicit TLS termination.
- Production metadata/table-read setup assumed concrete Threaded executors.
  Transport-neutral request executors and generic `std.Io` fanout now allow the
  same DataServer composition to run deterministically.
- The standard testing allocator's stack-trace unwinder was unsafe across
  manually switched fiber stacks. VOPR production compositions retain leak
  detection with stack-trace capture disabled rather than weakening allocator
  checks globally.
- Virtual socket admission counted every historical handle rather than live
  handles. Closing a connection therefore never restored capacity and could
  prevent the listener's shutdown wake connection. `VoprIo` now accounts live
  descriptors and proves reuse after close.
- The production DataServer HTTP scenario performed its joining teardown from
  a task owned by the borrowed VOPR scheduler. Under minimum socket capacity,
  the join could park behind listener cleanup while the external driver saw no
  ready transition and incorrectly declared the history complete; subsequent
  backend teardown then waited forever on the still-live API-lane lease. The
  scenario now waits for the completed request's client/server pair to return to the
  listener-only descriptor baseline before publishing stop, because the
  listener wake itself needs a temporary connection pair at minimum capacity,
  then follows the required two-phase lifecycle: publish `beginTeardown`, drive
  the scheduler to quiescence, and join. It also requires zero outstanding
  API-lane leases. This was a harness lifecycle defect, not a product-property
  failure.
- Closing a virtual listener released the listening handle but retained
  completed server-side connections still waiting in its accept queue. A
  shutdown-wakeup connection therefore leaked one live socket and could leave
  a ghost peer behind a stable-port process restart. Listener close now drains
  and closes every pending accepted handle; the focused network shard proves
  the count falls from three live handles to the surviving client alone and
  then to zero.
- Virtual TCP half-close marked EOF immediately even when earlier payload bytes
  were still queued, allowing FIN to overtake data. FIN is now its own ordered,
  scheduler-visible packet transition.
- The threaded durable-job lane held its global reap mutex while awaiting an
  owner job. A job that closed a nested DB/background owner then tried to enter
  the same reap path, deadlocking both teardown operations. Drains now detach
  entries under the mutex and await them after releasing it; a focused nested-
  owner regression preserves this reentrant lifetime contract.
- A replicated split action transferred its per-source lane into a durable job
  but retained an unconditional caller-side `errdefer`. Manual runtimes execute
  jobs inline, so a failed bootstrap released the job-owned lane and returned
  its error to submission; caller cleanup then unlocked the same mutex a second
  time and panicked. Submission cleanup now interrogates the transferred job's
  `lane_held` state, lane release is idempotent, and a focused regression
  preserves the inline-failure ownership contract.
- `RaftTableApplyStateMachine.applyReady` treated a Ready containing no
  snapshot and no committed entries as empty, even when it carried applied
  ReadStates. A normal strong read could therefore reach quorum but strand its
  waiter until timeout, which made the reconstructed v16 owner appear unable
  to recover. The fast path now observes ReadStates before returning, and the
  production state-machine regression drives a ReadState-only Ready through
  the actual `applyReady` interface.
- The VOPR task kernel treated current-task identity as ordinary mutable state
  across a stackful context switch. ReleaseSafe could expose the main fiber's
  cleared identity after a task resumed, crashing every scheduler path that
  parked a fiber. Task fibers now publish their identity on entry/resume, the
  main fiber clears it after return, and the compiler boundary uses atomic,
  non-inline access. Debug and ReleaseSafe run the same 90-test engine gate.
- Counterfactual analysis bounded the choice-prefix and descendants per
  alternative but not the number of enabled alternatives. A high-cardinality
  choice could therefore monopolize a campaign worker. The reusable engine now
  enforces an explicit total experiment budget and spends it on choices nearest
  the failure first.
- Enabling durable workflow leases made serverless maintenance shutdown reach
  a cancellation point while `buildStatus` still owned cloned search-source
  descriptors. Canceling the Future could discard that in-progress ownership
  and leak the descriptors. Maintenance shutdown now publishes stop and awaits
  cooperative loop completion, so scoped cleanup runs before runtime teardown.
- The modeled filesystem represented a file lock with one aggregate owner, so
  a second legitimate shared reader was rejected and closing either reader
  could incorrectly unlock the other. `VoprIo` now tracks each handle's lock
  mode and a per-node shared-owner count, including upgrade, downgrade, close,
  unlock, and crash semantics.
- Metadata's backfill-marker cache used zero as both "never scanned" and a
  valid monotonic scan timestamp. A deterministic world starting at logical
  time zero therefore rescanned an empty cache immediately. Borrowed-I/O clock
  sampling now preserves a nonzero sentinel while keeping throttle decisions
  replayable.
- Portable directory creation, absolute file creation, and directory fsync
  escaped a borrowed `std.Io` through raw POSIX calls. A virtual descriptor
  could therefore reach the host kernel during secret publication and other
  durable rename protocols. These operations now dispatch through `std.Io`;
  `VoprIo` treats a directory-file sync as the namespace durability boundary,
  and the cold-start campaign proves secret crash/reopen persistence through
  the production atomic writer.
- Native Lite unconditionally constructed its own `std.Io.Threaded`, and its
  docstore locks, index timestamps, and index-root canonicalization continued
  to use native helpers even when the surrounding Embedded or C API owner had
  a caller-supplied runtime. The lifecycle campaign exposed the index-root
  escape as a modeled-file `FileNotFound`. Native Lite now retains either an
  owned Threaded implementation or a borrowed `std.Io`, and every dependent
  lock, clock, and real-path operation uses the same runtime.
- Portable C API restore originally had no in-process runtime seam around its
  staging writer, writer lock, import DB, activation, or parent-directory
  durability boundary. Its first borrowed-runtime composition also selected
  the `io_threaded` derived executor, which requires an owned Threaded
  implementation and failed closed with `MissingBackendRuntimeIo`. Restore now
  accepts caller-owned I/O, runtime, and cancellation; uses the manual executor
  for that synchronous composition; checks cancellation before activation; and
  syncs the parent directory after atomic replacement.
- Portable directory-sync helpers relied on a platform-specialized inferred
  error set. On platforms where the unsupported branch was compiled out,
  portable callers could not name `DurableDirectorySyncUnsupported` in their
  cross-platform recovery logic. The helpers now expose an explicit portable
  error contract.
- The persistent object-range cache bounded its own queue and disk footprint,
  but its pending key/payload memory and concurrent physical growth were
  invisible to the node-wide `ResourceManager`. Independent services could
  therefore remain below their local limits while exceeding the shared
  process or volume envelope. Cache queue ownership now uses an exactly-once
  `lake_range_cache_queue` reservation, and each worker reserves capacity-domain
  growth before file I/O; completion, coalescing, allocation failure,
  cancellation, and shutdown all release the corresponding ownership.
- Lite `openOrCreate` propagated a shared `ResourceManager` into its missing-file
  fallback but dropped the caller's borrowed `std.Io`, silently returning to
  a native Threaded create path. The cross-service DB composition now exercises
  this fallback on `VoprIo`, and the create side retains both injected owners.
- The production query-embedding cache waited for a coalesced miss with
  `waitUncancelable` when the caller had no deadline. Canceling that waiter
  could therefore park it forever behind another request. The cache now uses
  the caller's cancellable `std.Io.Event.wait` path for both deadline and no-
  deadline waits; `query-embedding-cache-vopr-test` preserves the interleaving.
- The reusable generation chain performed retry backoff with POSIX
  `nanosleep`, escaping a caller-owned runtime. `executeChainWithIo` now sleeps
  through borrowed `std.Io`, and Antfly production wrappers use it; the legacy
  no-I/O entry point remains only for compatibility.
- Local reranking accepted a score vector with the wrong document count or
  non-finite values. The production boundary now rejects both as
  `InvalidRerankerResponse`, preventing malformed provider output from being
  published as a valid ranking.
- The public HTTP test runtime still created native Threaded listener,
  connection, and request lanes even when the listener and clients borrowed
  `VoprIo`. httpx now has an explicit borrowed-runtime mode, so all of those
  lanes participate in the same scheduler; native-only disconnect probing is
  disabled for virtual handles.
- The Raft HTTP frame driver accepted borrowed `std.Io` but still spawned raw
  `std.Thread` workers, allowing native concurrency to race the deterministic
  scheduler. Its long-lived senders are now owned `std.Io.Future` workers; a
  zero-worker synchronous mode lets bounded modeled Raft rounds complete real
  wire delivery without escaping task ownership. Simulated hosts also disable
  the unused native Raft listener instead of constructing a second hidden
  transport owner.
- Replacing the in-memory Raft target with a real VOPR/httpx hop made virtual
  network delivery suspend. That exposed reentrant `drainDue` calls selecting
  and removing from the same queue, corrupting `ArrayList` ownership. Queue
  mutation now has one explicit drainer plus a narrow enqueue/select mutex, so
  listener and Raft tasks may enqueue while a wire delivery is in flight
  without sharing an index owner.
- Several focused VOPR build filters compiled the Antfly root without forcing
  their exported scenario modules into test discovery, so a green command
  could execute zero matching scenario tests. The root now references every
  exported VOPR module, and the determinism audit checks the same manifest.
  Enabling real discovery exposed and repaired stale casts/error handling,
  uninitialized fixture state, disabled auth safepoints, a replication fake-
  source cursor bug, and a composed-query progress-counter overflow. These are
  harness defects, not product-property failures.
- The long end-to-end VOPR meta test completed its artifact and indexing work
  but the default test-server protocol surfaced only an opaque failed-command
  line, with no test attribution, assertion, or leak result. The gate now uses
  Antfly's simple runner, which reports the selected test and its allocator/I/O
  cleanup explicitly; the same history passes 1/1 with zero leaks.
- The shared audio-provider `ActiveRuntime` published pointers to client and
  provider fields inside a wrapper returned by value. The pointers could become
  stale immediately after initialization, and block-scoped `errdefer` cleanup
  failed to roll back earlier providers when a later provider failed. Client
  and provider registries now have stable heap identities, initialize before
  global publication, and roll back from function scope.
- httpx raced every blocking socket operation against a short cancellation-
  polling timer even though the outer request watchdog already canceled the
  task and shut down its published socket. A read could consume stream bytes,
  lose the Select race to the timer, and have its completed result discarded.
  Socket operations now race only real socket/request deadlines; cancellation
  remains owned by the outer watchdog.
- Borrowed `BackendRuntime` compositions still exposed only the native storage
  pool, so a DB/LSM opened on `VoprIo` could silently select native storage or
  an executor that required owned Threaded I/O. A reusable `std.Io`-backed LSM
  `Storage` adapter now carries file, durability, rename, deletion, clock, and
  root-identity operations through the caller's runtime.
- `VoprIo` accepted file lock options on open/create but ignored them, and its
  injected rename failure used an error outside Zig's rename contract. Open and
  create now acquire the requested modeled lock or fail with `WouldBlock`, and
  rename injection reports `HardwareFailure`.
- A pending futex or external wake could be consumed by a task that parked only
  after the wake was issued, stealing the wake from its intended waiter. Wake
  records now include an eligible wait-sequence cutoff, with a focused
  regression. Group await/cancel also assumed one scheduler yield drained every
  child and left a stale awaiter pointer during nested cancellation; both paths
  now loop until the group is empty and clear ownership between yields.
- `ResourceManager` allocated its capacity-domain table with the allocator of
  whichever cache or storage consumer reserved first, but freed the table with
  the manager owner's allocator. Composing the persistent lake cache with a
  node-owned manager produced an invalid free. All manager-owned identity and
  capacity tables now use the configured lifetime allocator regardless of the
  consumer allocator; a mismatched-allocator regression preserves the rule.
- The cross-service resource fixture used a zero-duration sleep as if it were a
  cooperative scheduler yield and multiplexed readiness, release, background,
  and provider events through one condition. Executed seed variation exposed
  both the spin and a stranded-holder schedule. The scenario now uses blocking
  condition waits with separate logical resources. This was a harness defect,
  but it also validated VOPR's ability to distinguish harness liveness from a
  product property failure.
- The internal graph-expansion HTTP handler encoded its response as a nested
  `graph_result`, while the production cross-range client decoded the canonical
  flattened wire contract. The first public cross-range graph history failed
  with `UnknownField`; the handler now uses the shared canonical encoder.
- Canonical graph JSON omits null path fields, but `GraphResultNode` required
  the nullable `path` and `path_edges` keys during decoding. Those fields now
  default to null, with a contract regression for omitted fields.
- Distributed traversal and shortest-path execution hydrated documents even
  when `include_documents` was false. Besides violating the request contract,
  the unnecessary DB phase enlarged the ownership window and exposed an LSM
  root-writer race. Hydration is now conditional, and the focused suite proves
  zero hydrate calls for a nodes-only request.
- Hosted query responses measured `took_ms` with the host monotonic clock even
  when all transport and storage used `VoprIo`. Identical graph histories could
  therefore differ by one response byte. Hosted reads now sample the injected
  `std.Io` clock, restoring byte-exact replay.
- Distributed graph coordinator cancellation/deadline checks still sampled the
  host monotonic clock even when fanout borrowed `VoprIo`. That could make
  timeout behavior depend on wall execution rather than the recorded logical
  schedule. The coordinator worker now derives its current time from the
  injected fanout I/O;
  its lifecycle regression also proves snapshot, failed-attempt, completed-
  round, and hydration boundaries across a topology retry.
- Full-cluster bootstrap used `std.testing.allocator` inside a task scheduled
  on `VoprIo`; its native debug stack-trace mutex could deadlock the stackful
  task. The helper now uses the fixture allocator. The same bootstrap could
  create replica stores under an identity namespace different from metadata's
  projected namespace; seeding now preserves the projected identity contract.
- A later production index-cache expansion made the public HTTP-to-transaction-
  to-index-open call chain overflow the full-cluster fixture's 4 MiB fiber
  stack and strand Zig's signal unwinder. The deployment history now declares
  an 8 MiB task stack, matching a conventional native main-thread budget;
  focused suites keep the smaller reusable default.
- The first graph-transport fault chose a coordinator by client ordinal. That
  node could itself host the next range, so the intended network boundary was
  never crossed and the unexpected successful request remained active during
  teardown. The fixture now derives a truly remote coordinator from live
  replica status, and every unexpected response path heals the fault and
  terminates its obligation before cleanup. This was a harness/topology defect,
  not a product property failure.
- A whole-network or persistent endpoint outage was too broad for the
  production-owner graph/split composition: it could starve data-Raft or
  intercept split-control traffic sharing the same listener. `VoprIo` now owns
  an endpoint-, request-direction-, and semantic-byte-stream-scoped outage, so
  v15 cuts only the selected remote `/graph-expand` request while responses and
  unrelated protocols continue. This was a fault-model composition defect,
  not an Antfly product failure.
- The first semantic-stream selector searched each socket write independently,
  so a route marker fragmented across HTTP writes could evade the injected
  outage. The selector now retains per-connection KMP match state across
  writes, owns its pattern for the fault lifetime, and has a focused fragmented-
  write regression. This was a `VoprIo` model defect found while making the
  production failure evidence non-vacuous.
- The first v16 restart prototype rebound the reconstructed owner to new
  ephemeral ports and attempted to publish that endpoint change while the
  metadata-owned split was active. That modeled a topology mutation rather
  than a process restart, triggered synchronized deterministic election churn,
  and obscured the intended recovery property. Production services now expose
  a caller-owned stable-bind path; the history preserves advertised public and
  Raft endpoint identity across incarnations and refreshes route consumers from
  the unchanged catalog. This was a harness/fault-semantics defect, not an
  Antfly product failure.
- A real internal graph-fanout send failure escaped through two public dispatch
  wrappers as `SendFailed` and then `InternalFailure`, produced error-level
  logs for an expected availability fault, and returned an opaque HTTP 500.
  Distributed graph transport failures now normalize at the coordinator
  boundary to `DistributedQueryUnavailable`; both public dispatch forms retain
  that type and return the OpenAPI/SDK-backed structured retryable 503
  `distributed_query_unavailable` without any partial graph payload.
- A public read racing restart of its Raft-hosting DataServer returned HTTP 200
  with a partial result during exploration. The baseline durability read remains
  sequenced before restart. Full-cluster v9 now separately pauses a depth-two
  public graph after its first consistent round, restarts the actual next-range
  leader, waits for recovery, resumes the same request, and requires the full
  response. A second mode cuts real internal fanout after the same boundary,
  requires the typed fail-closed 503 with no graph result, heals the network,
  and requires a clean complete retry. This closes the public graph restart and
  transport-failure partial-result schedules; non-graph distributed joins and
  global-query publication remain explicit roadmap work.
- The topology-churn mode initially surfaced `TopologyChanged` and
  `UnknownGroup` as opaque public 500s after the bounded retry budget was
  exhausted. The distributed graph coordinator now normalizes both terminal
  topology outcomes to `DistributedQueryUnavailable`, preserving the typed,
  retryable 503 contract and never publishing a partial success body.
- The production data-Raft leader/proposal path measured deadlines with host
  monotonic time, materialized replicated document timestamps from the host
  realtime clock, and retried with POSIX `nanosleep`. A `DataServer` borrowing
  `VoprIo` could therefore neither control nor replay the waits or timestamps;
  its action fiber could be parked while logical time had no authority over
  progress. Data-Raft capability probes, campaigns, forwarding, apply waits,
  deadlines, retry sleeps, and timestamp materialization now borrow the
  backend runtime's `std.Io` clocks and sleep. The host clocks remain only the
  explicit fallback for a server with no injected runtime, and a focused
  regression proves the retry/action path on `VoprIo`.
- Production merge catch-up parsed only legacy `put:`/`del:` entries, while
  current DataServer Raft logs retain JSON batch envelopes. A merge could
  therefore finalize without replaying current writes. Merge replay now
  decodes the production envelope, skips protocol barriers, preserves Raft and
  within-request operation order, and retains legacy compatibility.
- Receiver byte-range coverage was being used as implicit proof that merge
  bootstrap had completed. That is unsound when the receiver already covers
  the donor or metadata changes precede the data copy. Merge state now persists
  an explicit bootstrap-complete marker and applied-index watermark; status and
  catch-up require that evidence, while a pre-covering receiver can advance
  once the marker is durable.
- A rolled-back receiver checkpoint permanently conflicted with every later
  transition ID, even after rollback restored the exact base range. Metadata
  could legitimately admit a fresh merge, but its accept command then stopped
  receiver Raft apply with `ConflictingMergeTransition`. Receiver checkpoint
  planning now replaces a terminal rolled-back receipt only for a different
  transition's `accept` at the restored base range. Same-transition reopening,
  non-accept starts, wrong ranges, and active/finalized conflicts still fail
  closed. The production `DataServer` VOPR regression proves rollback followed
  by a fresh transition and finalization.
- The authoritative Raft apply projection contains primary documents, not all
  materialized graph and embedding state. A primary-only merge lost live graph
  edges after cutover. Bootstrap now imports graph and embedding artifacts from
  the retained donor DB lease, rebuilds range-derived artifacts, and syncs the
  receiver before publishing the durable bootstrap marker.
- The composed metadata harness advertised a merge transition but its adapter
  only mutated an in-memory status. Replacing it with the real coordinator
  exposed three ownership defects: it assumed donor and receiver were local to
  the metadata leader, reopened live LSM roots instead of retaining hosted
  writers, and destroyed coordinators when Raft descriptors churned before
  terminal observation. Full-cluster v9 now routes across the actual donor and
  receiver leader roots, uses retained hosted-writer leases, and gives the
  transition runtime an explicit lifetime independent of replica descriptors.
  The remaining fidelity gap is execution through the replicated DataServer
  transition action on every receiver replica, which stays explicit below.
- The completion-claim audit then exposed the corresponding production safety
  issue: `DataServer` used the same direct-DB `MergeCoordinator` branch even
  when data Raft was enabled. A hosted coordinator could therefore acknowledge
  a leader-local accept/catch-up/finalize that followers had never applied.
  The data-Raft path now uses first-class source prepare/finalize/rollback
  controls, receiver accept/bootstrap/finalize/rollback checkpoints, ordinary
  Raft writes for copied documents, replicated observation, durable source
  fencing, and snapshot-carried control state. These private fields require a
  version-3 Raft batch capability: the leader revalidates every applying peer,
  appends an irreversible durable v3 barrier, and the projection rejects merge
  controls that do not follow it. This closes a second audit defect in which an
  older replica could otherwise ignore an unknown private JSON field and apply
  an empty command. Three independent apply stores converge in the focused
  regression, and the broad storage/runtime gates pass. The focused VOPR
  histories now cover both rollback/fresh-transition retry through one owner
  and a three-owner, six-replica deployment with networked forwarding, leader
  transfer, owner restart, every-replica transition/watermark convergence,
  document equality, terminal retry, and actor-owned teardown. Substitution
  into the broader full-cluster graph/serverless history, disjoint placement,
  derived-state equivalence, bounded retained-history replay, and explicit
  snapshot-install rehydration remain required below.
- The first three-owner DataServer history exposed five additional production
  seams that the single-owner test could not reach. Cross-owner Raft batch
  forwarding constructed a hidden native `StdHttpExecutor`; it now uses the
  caller's `std.Io`. The durable data apply store also created a private
  `std.Io.Threaded`; it now borrows the managed host runtime with a bounded
  native fallback only for callers that do not inject I/O. A pristine receiver
  replica rejected the first merge checkpoint because its apply projection had
  no range record; first accept now initializes only a genuinely pristine
  projection and preserves exact fail-closed validation thereafter.
- Receiver merge checkpoints were incorrectly classified as projection-only,
  so the live DB did not persist the expanded range/merge receipt, and later
  copy entries could require a live catalog lookup during Raft apply. Every
  merge-copy and lifecycle command now carries a validated receiver replay
  identity through the internal batch codec; cached and cacheless owners reopen
  the prepared local manifest without catalog I/O. Schema-less tables now
  persist an explicit empty schema manifest, and write validation consumes a
  cached authoritative admin snapshot before attempting a remote refresh.
- Retrying `finalize_merge` after a successful restart recopied the donor and
  proposed a larger post-finalize bootstrap watermark. The DB correctly
  rejected it as `ConflictingMergeTransition`, but the public action was no
  longer idempotent. Catch-up and finalize now treat matching replicated
  terminal receipts as the retry boundary and return success before proposing
  new work. The distributed regression exact-retries finalization after owner
  restart and verifies all replicas and both documents before teardown. Its
  first broad run also caught a harness oracle that assumed terminal merge
  receipts implied immediate equality of later Raft bookkeeping watermarks;
  the history now waits for bounded durable-watermark convergence before
  declaring success and replays its recorded schedule from fresh roots.
- Canceling a local group-status refresh leaked each owned
  `MergedGroupStatus.doc_identity_lifecycle` string because teardown freed only
  the outer merged-status slice. `OwnedLocalGroupStatusRefresh.deinit` now uses
  the same deep ownership helper as the normal path, and a testing-allocator
  regression cancels and releases a populated refresh without leaks.
- Production transition retry jitter was seeded from
  `std.Options.debug_io.random` even when the managed service borrowed
  `VoprIo`. Record and replay could therefore schedule bootstrap status retries
  at different logical times. Managed services now resolve one configured
  retry-jitter salt at construction, preserve it across service replacement,
  and the full-cluster fixture derives a stable per-node salt inside the
  deterministic world. The 60,000-transition v12 diagnostic then completed
  record and fresh-world replay with identical canonical history through the
  formerly divergent choice region.
- Exact replay formerly rendered both complete histories to JSONL before byte
  comparison. A 320,000-transition production history reached canonical
  comparison successfully but the first render grew the process to roughly
  14 GB and spent more than an hour serializing. Canonical equality now walks
  canonical wire values directly and renders only the first mismatching record
  into bounded diagnostic buffers. Render/parse equivalence remains an engine
  regression, so this changes comparison cost rather than replay truth.
- The first production-owner resource-pressure composition bypassed the
  `DataServer` managed-loop retry policy by calling its raw Raft round helper.
  `runRaftProgressRoundOnly` now applies the same transient-progress policy as
  the production owner, and both the managed callback and VOPR driver use it.
  This was a harness/production-seam fidelity defect.
- Local persisted Raft apply treated `ResourceBudgetExceeded` while acquiring
  an apply writer as a terminal progress failure. The production table writer,
  atomic document apply state machine, and durable data projection now
  normalize resource exhaustion to `RaftApplyWriterUnavailable`, retaining the
  exact retry checkpoint and entry identity. Focused regressions prove that a
  non-idempotent transform is not applied twice and snapshot projection also
  resumes after pressure clears. This was a production liveness defect.
- The resource campaign's accelerated 1 ms Raft driver retried a production
  100 ms cadence path thousands of times, and the v17/v18 mode predicates
  initially omitted their own graph/split properties. The pressure history now
  uses the production cadence and both modes participate in every applicable
  non-vacuity property. These were harness timing and coverage defects.
- Group-local lookup converted transient ownership and resource failures to
  `Internal`, while public point reads and queries let split-cutover
  `TopologyChanged` escape as generic HTTP 500/`InternalFailure`. Internal and
  public boundaries now preserve typed 409/503/504 outcomes, point reads retry
  only as idempotent GETs, and public query execution performs a bounded
  topology retry before returning typed read-unavailable. That retry also used
  host `clock_gettime`/`nanosleep`, allowing its successful record to diverge
  during exact replay; it now borrows the API runtime's `std.Io` clock and
  sleep. These were production availability, determinism, and API-contract
  defects exposed after resource recovery.
- The public distributed-join core entry point did not install its
  `JoinContext` on `JoinJobStore`; callers that bypassed convenience wrappers
  silently downgraded an eligible durable shuffle to transient execution. The
  core boundary now binds context before eligibility, lease, or state work.
- A finalizer result was persisted before the lifecycle hook ran, but an error
  from that post-persistence hook skipped ownership cleanup for the result.
  The first v20 takeover history leaked all 64 joined-hit allocations. The
  finalizer now guards result ownership across ambiguous acknowledgement
  failure, and the ReleaseSafe fixture's leak checker covers the path.
- Distributed join deadlines serialized absolute monotonic timestamps across
  owners, and transport-neutral internal partition/finalizer operations carried
  a valid `JoinContext` without binding it to their durable store. Remote
  leases consequently used host realtime and entered nondeterministic response
  packets. Deadlines now cross the wire as relative remaining budgets, real
  and awake time come from the borrowed `std.Io`, and typed worker boundaries
  install the effective context before durable state access. V20 exact replay
  exposed and closes both clock-domain defects.
- The durable-shuffle completion boundary normalized ownership and transport
  failures only on the ordinary distributed-join path. A partition-owner loss
  could therefore reach `DistributedQueryUnavailable` internally and still
  become an opaque HTTP 500 after shuffle finalization. The v32 process-loss
  history reproduced this at the public API and now preserves the typed,
  retryable 503 while prohibiting partial hits.
- The same operational normalization was missing around the join's primary
  left-side query. A transient `SendFailed` immediately after endpoint
  reconstruction became `InternalFailure`/HTTP 500 instead of allowing the
  idempotent public operation to retry another owner. Both primary and shuffle
  phases now use the same ownership/transport classification, with focused
  regression assertions for `SendFailed` and the existing transport classes.
- A `DataServer` that did not host a routed group trusted its managed Raft
  host's cached leader hint indefinitely. After destroying and reconstructing
  the former leader, a non-member coordinator could keep routing strong reads
  to a healthy follower even though authoritative metadata named the new
  leader. Member hosts still use their live local Raft observation; non-members
  now resolve the healthy leader store from the current merged metadata
  snapshot. V32 additionally requires every live coordinator's route to end at
  the actual local leader before recovery is declared.
- Internal exact-group join routing converted any unexpected remote HTTP
  status into a null remote result. The durable coordinator then treated that
  null as permission to execute the foreign group against its local source,
  violating the exact-group ownership contract and potentially hiding a failed
  owner behind wrong-process work. Remote partition, rows, unmatched,
  finalizer, and job-state operations now preserve typed absence and
  unavailability; only an actual local route returns the local-worker sentinel,
  and unexpected remote responses fail closed. V33 reproduced the forbidden
  fallback while resource and link faults overlapped.
- Internal join workers collapsed ownership and resource exhaustion to
  `Internal`/HTTP 500, while the join HTTP client did not preserve 503 as
  `DistributedQueryUnavailable`. Resource, descriptor, storage-read,
  leadership, and ownership failures now map through the internal operation,
  HTTP, exact-group router, shuffle engine, and public response as typed
  retryable unavailability. Focused client/operation regressions and v33's
  exact no-partial public 503 cover the repaired boundary. Removing the unsafe
  fallback also exposed that the older v31/v32 fault observers returned
  VOPR-private error names, which correctly became unclassified 500s at the
  production boundary. Those observers now inject the production
  `GroupLeaderUnavailable` condition directly; no compatibility mapping for
  test-private errors was added.
- The focused three-owner merge/split history configured routed production
  HTTP without the internal-service credentials required by its own middleware.
  Every cross-owner write was therefore rejected before the handler as an
  unauthenticated 503, which the forwarding client correctly classified as an
  ambiguous outcome because it had no not-proposed proof. The fixture now
  gives every owner and the hosted router one shared test identity, retains
  stable public/Raft ports across restart, and uses the runtime's bounded
  synchronous delivery mode for this topology-transition composition. The
  deployment-shaped production-cluster fixture independently keeps the async
  Raft sender and retry queue under VOPR control.
- That same history represented restart with independent `initialized` and
  `paused` booleans. Budget and router diagnostics could consequently enter a
  partially deinitialized Raft host and dereference poisoned hash-map state.
  Nodes now publish one explicit `starting`/`running`/`quiescing`/`stopped`
  lifecycle; drivers, routing, convergence checks, diagnostics, restart, and
  final teardown all use it. The repaired history passes record, clean-world
  exact replay, cleanup, and leak checks.

The bounded independent-domain model campaigns completed without an additional
semantic product-property failure or replay divergence. Reports should keep
that result distinct from the defects above.

## CLI

The VOPR command is a harness-only, test-mode artifact. It must not be installed
or represented as a production Antfly binary.

```sh
# Generate one history.
zig build vopr-run -- \
  --scenario metadata \
  --seed 0xa17f0001 \
  --transitions 500 \
  --trace-out /tmp/metadata.voprtrace

# Exact replay, reduction, and reviewed promotion.
zig build vopr-replay -- --trace /tmp/metadata.voprtrace
zig build vopr-reduce -- \
  --trace /tmp/metadata.voprtrace \
  --out /tmp/metadata-reduced.voprtrace
zig build vopr-promote -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --name split-leader-restart-before-finalize

# Formal export and causal explanation.
zig build vopr-tla -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --domain raft \
  --out /tmp/metadata-raft.ndjson
zig build vopr-explain -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --failure 0 \
  --out /tmp/metadata-causal.json

# Replay-validated navigation at an arbitrary choice prefix.
zig build vopr-debug -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --prefix 12 \
  --out /tmp/metadata-debug.json

# Run a repeatable debugger recipe or enter the same line-oriented frontend.
zig build vopr-debug -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --commands /tmp/debug.commands
zig build vopr-debug -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --interactive

# Stable machine-readable/static results and a temporal event query.
zig build vopr-results -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --json-out /tmp/results.json \
  --html-out /tmp/results.html
zig build vopr-events -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --query /tmp/event-query.json \
  --out /tmp/event-matches.json

# Atomically update and query the repository-owned cross-run index.
zig build vopr-index -- \
  --index /tmp/vopr-run-index.json \
  --add /tmp/results.json \
  --revision vopr-metadata-phase1 \
  --min-transitions 1 \
  --json-out /tmp/vopr-run-query.json \
  --html-out /tmp/vopr-run-summary.html

# One reviewable failure package and deterministic corpus merge.
zig build vopr-recipe -- \
  --trace /tmp/metadata.voprtrace \
  --flight-filter /tmp/flight-filter.json \
  --flight-before 8 \
  --flight-after 8 \
  --out /tmp/failure.recipe.json \
  --reduced-out /tmp/failure-reduced.voprtrace
zig build vopr-corpus-merge -- \
  --base /tmp/metadata-reduced.voprtrace \
  --trace /tmp/failure-reduced.voprtrace \
  --out-dir /tmp/merged-vopr-corpus

# Bounded deterministic campaign.
zig build vopr-campaign -- \
  --scenario metadata \
  --histories 1000 \
  --transitions 500 \
  --workers 8 \
  --artifact-dir /tmp/antfly-vopr

# Host-independent checkpoint and multi-bug search-quality benchmark. Output
# is vopr-search-quality-v2 JSONL with recurrence and 95% confidence evidence.
zig build vopr-benchmark
```

Registered CLI scenario names are:

```text
metadata              transaction          distributed-data
distributed-transaction                    data-plane
derived-workflow      backup-restore       clock-fault
wal                   persistent           index-manager
db-split              raft                 lmdb
lsm                   ha
```

## Test-Tier Policy

### `root-test`

- Fast root-module compile smoke coverage.
- No wall-clock sleeps or generated campaigns.
- Broad unit coverage belongs in focused unit buckets rather than a monolithic
  root-module test.

### `vopr-test`

- Fast deterministic virtual-time and modeled-I/O smoke coverage.
- Promoted VOPR fixtures, replay-equivalence checks, bounded domain scenarios,
  metadata virtual transport, Raft scheduling, production public HTTP, and
  `storage-vopr-test`.
- No legacy real-I/O storage workload pretending to be modeled I/O.
- `vopr-engine-test` runs only the reusable `lib/vopr` contract.

### `chaos-test`

- Longer but transition- or history-bounded deterministic campaigns.
- Independent labeled nodes for metadata, transaction, Raft, WAL, LMDB, LSM,
  persistent index, index manager, DB split, HA, and application domains.
- Every failure prints or stores an exact replay artifact.

### `chaos-soak-test`

- Larger history counts, broader fault budgets, and retained legacy chaos
  suites.
- Never part of the default fast gate.

### Integration and legacy storage tests

- Real HTTP, native threads, processes, sockets, and local object stores remain
  focused integration differentials.
- Deterministic storage workloads that still use real LMDB/WAL/files stay in
  `storage-workload-test`; longer ones stay in `storage-workload-soak`.

Legacy storage commands are:

- LMDB: `storage-lmdb-test`, `storage-lmdb-test -Dlmdb_backend=c`,
  `lmdb-replay-fixtures`, and `lmdb-workload-soak`.
- WAL: `wal-test`, `wal-workload-test`, `wal-replay-fixtures`, and
  `wal-workload-soak`.
- Persistent index: `persistent-test`, `persistent-workload-test`,
  `persistent-replay-fixtures`, and `persistent-workload-soak`.
- Index manager: `index-manager-test`, `index-manager-workload-test`, and
  `index-manager-replay-fixtures`.
- DB split: `db-split-workload-test` and `db-split-replay-fixtures`.
- Aggregate legacy workloads: `storage-workload-test` and
  `storage-workload-soak`.

The old `*-sim-test` and `storage-sim-soak` spellings are compatibility aliases,
not canonical suite names.

Reduced legacy artifacts are written under `/tmp` with an
`antfly-{lmdb,wal,persistent,index-manager,db-split}-replay-` prefix. Promote a
reviewed artifact with `zig build storage-fixture-promote -- <artifact>`;
`--latest`, an optional destination stem, and `--force` are supported. Fixture
directories retain their existing `_sim_fixtures` names as checked-in format
and path compatibility, just as trace ABI identifiers do. Fixed-map LMDB stays
out of randomized reopen matrices because persisted addresses are host-layout
sensitive. Crash-mode WAL, persistent, and index-manager fixtures require the
Zig backend's publish-phase hooks; the C backend remains the differential
oracle.

PR gates use deterministic transition and history counts. Nightly/manual
controllers may use a wall-clock allocation across independently replayable
histories, merge and deduplicate corpus artifacts, run reduction, and validate
eligible TLA+ traces.

## Corpus and Fixture Policy

- Existing regressions and replay fixtures seed exploration.
- A candidate affecting corpus state must exact-replay from a clean world.
- Generated non-failing entries remain CI artifacts unless review identifies
  durable coverage value.
- Failures are reduced before promotion and named for behavior, not raw seeds.
- Campaigns never modify tracked files.
- A scenario ABI change invalidates its old VOPR fixtures. Delete them rather
  than migrate them; any replacement is freshly recorded, exact-replayed,
  reduced when failing, and reviewed before promotion.
- Fixed regressions do not depend on corpus scheduling or search heuristics.

## Roadmap

Calling these VOPR tests, the remaining opportunities are targeted integration
scenarios and operational tooling rather than missing foundational
infrastructure. They are not a second numbered phase plan and are not
dependencies of the already implemented domain suites.

### Current Answer: Coverage, Parity, and Completeness

The short answer is **yes, there are still valuable VOPR tests and
Antithesis-class features to add; no, the complete roadmap is not
implemented**. The reusable deterministic runtime is no longer the main gap.
The highest-value work is composing more production owners, workflows, and
fault domains in the same replayable history.

| Question | Current answer | Highest-value next work |
| --- | --- | --- |
| Where should Antfly add VOPR testing? | At production orchestration boundaries that combine durable state, ownership, public visibility, and recovery | Deepen the v11-v49 production-owner cluster with cache topology/link/storage/resource overlap; additional replication topology, cancellation, and source/target-crash timings; metadata leadership loss during cutover; and cross-domain fault overlap. Add cancellation under storage faults, disk pressure, simultaneous process loss before cancellation drain, richer fault combinations, and broader socket/topology/short-write targets. Finish the repository-wide strong-read and managed-index publication/readiness contracts; extend durable joins across authorization/generation and broader forms; extend global query across topology, storage, resource, coordinator/metadata and multi-process loss; then compose metadata administration, MCP/A2A, cloud authentication, extension invocation, and live credential/provider replacement |
| Which Antithesis ideas remain worth porting locally? | The large engine features, saved cross-run event-set programs, non-blocking bounded live streams, and reversible logical service rates with per-node/per-operation evidence are implemented at the registered in-process boundary; query-cache, DataServer, graph, replication, and serverless work are production-charged seams, and v42-v49 compose all five production work families plus schema-recovery, target-owner-restart, source-session-replacement, durable-cancellation, stale-owner fencing, metadata-backed exact-cutover rotation, and actual cache-owner deadline/restart recovery in the cluster | Transitive determinism auditing; nightly sharding, retention, quarantine review, notifications, and dashboards; compiler coverage as guidance when Zig instrumentation is stable; broader service-rate fault combinations and production/search adoption |
| Is distributed VOPR missing? | **Partly.** In-process application-level distributed VOPR exists: logical nodes, directional links, process/storage/resource domains, independent and overlapping link-plus-resource faults, selected-listener socket admission, quiet suffixes, and exact replay are integrated | Antithesis-style separate-address-space orchestration is not implemented, and whole-deployment breadth is incomplete. Co-resident HA/data-plane/serverless ownership, disk-capacity pressure, broader socket/storage/process/restart overlap, federated process agents, and live mixed binaries remain future or conditional work |
| Are the features called finished actually finished? | Only within each narrowly stated **integrated** seam and its named green replay gate | Do not infer current aggregate health, transitive call-graph determinism, every cross-domain combination, arbitrary native/container determinism, or Antithesis product parity. Partial, ongoing, conditional, and explicitly excluded work remains unfinished |

The distinction between a green focused seam and a finished platform is
material. At this checkpoint the reusable engine and registered-source audit
are green, and the cited v13-v40 production-owner gates plus the v42-v49
Debug and ReleaseSafe gates passed their complete record/fresh-world replay
oracles. The
repository-wide `vopr-test` aggregate is
not currently cited as green, the dedicated v12 deep result is from its named
earlier checkpoint, and a source-manifest audit is not a proof over every
transitive production callee. These limitations are part of the completion
claim, not footnotes to it.

### Verification Audit and Meaning of "Finished"

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

The production-owned gate is intentionally tiered by deterministic work, not
by a weaker oracle. `production-cluster-vopr-smoke-test` runs v11 exact replay
and v12's bounded lifecycle and is included by ordinary `vopr-test`;
`production-cluster-vopr-deep-test` runs only the 320,000-transition complete
v12 history; `production-cluster-graph-vopr-test` runs v13's complete
production-owner graph; `production-cluster-graph-split-vopr-test` runs v14's
400,000-transition graph-before/during/after-split history;
`production-cluster-graph-split-transport-vopr-test` runs v15's 450,000-
transition fail-closed owner-transport history;
`production-cluster-graph-split-owner-restart-vopr-test` runs v16's 650,000-
transition stable-endpoint owner-reconstruction history;
`production-cluster-graph-split-partial-write-vopr-test` runs v17's 500,000-
transition scoped short-write history;
`production-cluster-graph-split-resource-pressure-vopr-test` runs v18's
550,000-transition three-owner memory denial/recovery history. The later
focused targets cover v19/v20 join and durable-finalizer takeover, v21/v22
overlapping link-memory and listener-socket pressure, v23 service rates, v24
hydration, v25 cancellation, v27 in-flight authorization revocation, and v28
stale-snapshot retry exhaustion, plus v29 cancellation under a scoped
hydration-transport outage, v30 cancellation, v31 partition failover, v32
partition-owner reconstruction, v33 overlapping-fault retry exhaustion, and
v34 cancellation under overlapping resource-plus-link faults, plus v35
cancellation followed by exact worker-owner reconstruction, v36 ordered
two-table global-query dispatch, and v37 fail-closed in-flight global-query
cancellation plus exact recovery, and v38 live cross-table authorization
revocation after the first result plus exact fail-closed recovery. V39 cuts the
registered directional tenant-query stream after that result, requires the
exact retryable 503 without partial output, heals it, and exactly recovers.
V40 destroys that exact tenant-owner process after the first result, requires
the same exact no-partial 503, reconstructs its stable identity and listeners,
requires a direct durable read from the rebound endpoint, and exactly recovers.
V49 composes one production `ApiHttpServer` cache flight and coalesced deadline
before shared-node service-rate healing, one retained hit afterward, then exact
owner reconstruction, empty-cache recomputation, bounded reconnect, durable
read, exact accounting, and full cluster recovery.
V42 composes production replication snapshot-to-stream work through public
HTTP, routing, DataServer Raft, and index visibility before and after the same
logical healing boundary, with exact accounting and full cluster recovery.
V43 changes the source schema after the first accepted snapshot batch, requires
durable resume with one exact duplicate application, and completes the same
public visibility and cluster recovery oracle.
V44 destroys the current target leader before the next snapshot batch,
reconstructs its stable identity and listeners, proves bounded reconnect plus
durable local/direct public recovery, and resumes to exact all-node visibility.
V45 fails the first provider query after durable preparation, closes that
source session, resumes through a newer session, and preserves the exact three
target attempts/successes plus every cluster oracle.
V46 revokes the work lease after durable snapshot offset 1, requires typed
lease loss and sequential session replacement, and resumes without replaying
the committed target batch while charged work remains active.
V47 loses ownership after target apply but before checkpoint publication,
keeps durable offset 0, and requires one exact idempotent replay for four
target successes.
V48 publishes an exact source-catalog replacement and both cutover authorities
through metadata Raft, rejects authority A before checkpoint publication,
retires it under authority B, and preserves four target successes.
`production-cluster-vopr-test` requires every focused production-owner gate
through v48; each large subprocess uses ordinary
exit-code-checked test mode and is serialized by the build graph's inherited
stdio lock.
Record and
fresh-state replay, properties, cleanup, and enabled-set equality remain
identical in every tier. The split keeps a multi-gigabyte, tens-of-minutes
witness out of the default edit loop without treating an early cutoff as
active-reconfiguration completion evidence. Exact replay now compares
canonical wire records structurally and renders only a first mismatch, so the
deep gate no longer retains two complete JSONL artifacts merely to prove
equality.

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
snapshot/lifecycle, and determinism-audit gates. Completion labels have these
strict scopes:

- **Integrated foundation** means the reusable engine/runtime capability exists,
  is exercised by a focused gate, and exact replay is part of its contract.
- **Integrated seam** means the named production path and listed fault modes are
  implemented and replay-proven; it does not include residual work named in the
  same row.
- **Partially integrated** means focused production seams exist but are not yet
  composed through the whole public/deployment path.
- **Ongoing** and **conditional** are not implemented completion claims.

Therefore the features labeled integrated below are implemented to their stated
boundaries, but the complete roadmap is not finished. In particular, local
run/index/report tooling is implemented while nightly retention, corpus merging,
notifications, and dashboards are operational follow-up; distributed VOPR is an
implemented runtime foundation with incomplete composition breadth; and the
event-query layer, saved cross-run set algebra, validation/counting commands,
and bounded live stream are implemented while nightly retention and broader
operational integration remain future work.

The word **fully** is consequently never implicit. The conformance audit is:

| Claim class | Audit result | Explicit exclusion |
| --- | --- | --- |
| Named VOPR engine/tooling features | **Implemented at the registered in-process `std.Io` boundary.** `vopr-engine-test` and the registered-source `vopr-determinism-audit` pass at this checkpoint | The audit manifest is not a transitive production call-graph proof; arbitrary guest-kernel RNG/syscall interception, uninstrumented native libraries, and separate process address spaces are also excluded |
| Rows labeled integrated | **Implemented for the production seam, schedules, properties, and exact-replay gate named in that row** | Residual work stated in the row and combinations with other independently tested domains |
| Rows labeled partially integrated | **Not complete end to end** | Promotion requires the remaining public/deployment composition and its replay gate |
| Local results/index/corpus tooling | **Implemented as repository-owned commands and formats** | Nightly sharding, retention policy, notifications, dashboards, and routine quarantine review |
| Antithesis parity | **Not claimed** | Hosted orchestration/UI, deterministic execution of arbitrary containers or kernels, and operational service parity |

This is the answer to “have we fully implemented what we call finished?”: only
for a narrowly stated integrated contract whose named focused gate has passed
at the cited checkpoint. No such label applies to the overall roadmap,
Antithesis product parity, residual work named beside a contract, or any
extension beyond the named green seams. A changed tree must rerun the
named gate before carrying the claim forward; documentation is not evidence. A
broader sentence must not erase those boundaries.

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

HA, per-group Raft, LSM/WAL/LMDB/persistent/index-manager/DB-split, metadata
distributed data, the deployment-shaped full cluster, and the newer P0/P1/P2
boundary suites are implemented at the exact production seams and modes stated
in their conformance rows. “Integrated” is not upgraded to “fully implemented
everywhere,” and a row whose focused or aggregate gate regresses must be
downgraded or repaired rather than defended by this document.

That does not make the roadmap empty or make the system equivalent to the
Antithesis hypervisor. Items marked **ongoing** or **conditional**, and residual
boundaries explicitly named in an integrated row, are not finished. In
particular, one trace does not yet co-reside every HA/data-plane/serverless
owner; v9's public graph covers an in-flight leader restart and a production-
coordinator range merge across different leader roots, while v13 covers the
static traversal on production `DataServer` owners, v14 composes that
production traversal with a metadata-driven range split and replicated
transition execution, and v15 adds one scoped next-owner graph-transport cut.
V16 adds one scoped next-owner production-process restart with real listener,
DataServer, and Raft-owner reconstruction.
V17 adds one scoped, exactly observed next-owner request short write with
transparent stream resumption.
V18 adds one scoped all-production-owner memory-denial/recovery composition.
V24 adds exact selected-field document hydration through the public graph
response and proves one production hydration lifecycle.
V25 cancels scheduled hydration fanout through the real public HTTP disconnect
path and proves one clean retry. V27 mutates authorization inside the live
request, and v28 rejects a retained stale source snapshot through both bounded
topology attempts without partial output. V29 proves real outstanding hydration
reaches one scoped transport outage before cancellation, then heals and
recovers. Cancellation under resource, storage, process, restart, and
overlapping faults is not yet composed into that production-owner history. V9's fail-
closed topology/transport interruption and post-recovery complete retry,
v13's strong-read/derived-visibility barrier, v14's graph-during-split
contract, and v15's graph-stream failure during that split are integrated;
v16's single stable-endpoint owner restart, v17's single graph-request short
write, and v18's all-owner memory denial are integrated; broader restart,
topology, disk-capacity pressure, broader socket/partial-write, and fault-overlap breadth is not;
live mixed-
binary operation is not modeled; and arbitrary unmodified sidecars or process
address spaces remain differential/integration concerns.

### Distributed Completion Audit

This audit prevents a focused seam from being mistaken for a finished whole-
deployment campaign. It also answers the Antithesis comparison directly:
Antithesis runs distributed Docker Compose or
[Kubernetes](https://antithesis.com/docs/setup/kubernetes/) topologies and
[scopes faults to containers or
pods](https://antithesis.com/docs/product/writing_tests/controlling_faults/fault_types/); VOPR
implements the analogous application-level fault domains inside a registered
`std.Io` world.

| Requirement | Current status | Remaining work |
| --- | --- | --- |
| Deterministic multi-node runtime, clocks, links, storage, restart, resources, replay, and quiet suffix | **Integrated foundation.** The reusable deployment composer registers node/role/domain/fault/quiet obligations; metadata, Raft, HA, transaction, data-plane, and full-cluster gates exercise complementary real owners | Adopt the manifest in the remaining distributed suites and maintain fail-closed audits as new owners appear |
| Metadata quorum, production `DataServer` replicas, public clients, and real HTTP/Raft transport in one history | **Integrated at the named v11-v49 seams.** Full-cluster v9 remains the complementary hosted/public nine-fault campaign, and `data-server-transition-vopr-test` independently proves replicated merge-to-split behavior. V11-v40 provide the cited production-owner split, graph, join, fault, durable-worker, reconstruction, and global-query seams. V42 runs the clean replication snapshot-to-stream path through alternating public coordinators and DataServer Raft/index visibility. V43 changes schema after the first accepted batch, requires the interrupted attempt, durable resume, one exact duplicate batch, completed CDC, and visibility through every coordinator. V44 destroys the current target leader before the next batch, reconstructs its stable identity/listeners, requires exactly two bounded transport failures and three successes, and proves durable local, direct rebound, and all-coordinator recovery. V45 fails the first actual provider query after durable preparation, closes that exact source session, resumes through a strictly newer session, and preserves exactly three target successes. V46 revokes the lease after durable offset 1, requires exact `CdcWorkLeaseLost`, sequential source replacement, composed charging, and no duplicate target batch. V47 rejects a stale owner between target apply and checkpoint publication, preserves durable offset 0, and requires one idempotent replay for four target successes. V48 publishes exact source catalogs and cutover claims through metadata Raft, rejects authority A after a catalog change, claims and retires it with authority B, and preserves the same four-success terminal oracle. V49 runs cache deadline and recomputation through node 1's actual `ApiHttpServer`, reconstructs its DataServer owner, requires an empty replacement cache, one bounded pooled reconnect, and a durable rebound read. The complete v42-v49 modes pass Debug and ReleaseSafe 15/15 at 160,000, 180,000, 220,000, 220,000, 220,000, 240,000, 260,000, and 120,000 transitions | Add remaining topology breadth, disk and broader socket/short-write pressure, cache topology/link/storage/resource overlap, additional replication topology/cancellation/source/target-crash timings, metadata leadership loss during cutover, and overlapping-fault variants, cancellation under storage/process and richer multi-fault combinations, disjoint placement, retained-history paging, snapshot/derived-state rehydration, partitions, and coordinator/metadata or multi-owner loss |
| Serverless worker output through its production public catalog and ownership graph | **Integrated at the stated seam.** The production worker, durable lease, object stores, catalog service, HTTP handler/listener, and public client share one `VoprIo`. Every mode lists the worker-created table and queries the published head/documents; stale generation remains fenced. This correctly retains the distinct serverless object and metadata placement catalogs | Overlap serverless lease/object-store failures with metadata topology and node-resource faults, then add multi-worker placement when production owns that topology |
| HA, data-plane, metadata, public API, and serverless owners all co-resident | **Ongoing.** Each domain has an integrated exact-replay suite; they do not yet all coexist in one history | Build one bounded deployment composition and cluster-wide recovery oracle without duplicating business logic |
| Public distributed graph request from HTTP planning through fanout/hydration | **Partially integrated, with static, active-split, transport-fault, owner-restart, recoverable short-write, three-owner memory-pressure, one overlapping link-plus-memory path, selected-listener socket pressure, exact document hydration, clean and scoped-transport-fault cancellation/recovery shapes, in-flight cross-table permission revocation, and stale-source-snapshot retry exhaustion promoted.** Full-cluster v9 executes a public depth-two graph across hosted ranges. V13 executes it across real `DataServer`/data-Raft owners with current-owner routing and matching ReadState/derived-index visibility; v14 composes active split; v15 adds a next-owner transport cut; v16 adds owner reconstruction; v17 adds a one-byte request write; v18 adds all-owner memory denial/recovery; v21 overlaps that pressure with the selected graph link cut; v22 denies then heals new connections at one exact public listener; v24 validates exact hydrated titles plus one start/fanout/completion lifecycle; v25 cancels after multi-owner hydration tasks are scheduled, requires the listener cancellation token and no completion, then proves one exact clean retry; forward-only v27 authenticates the whole public workload, revokes the target-table read policy at `target_authorization_started` inside the live request, proves concealed no-leak output, restores permission, and requires exact cross-table hydration on a fresh request; forward-only v28 publishes a real split after `source_snapshot_acquired`, requires exactly two `TopologyChanged` attempts and a typed no-partial 503, then proves exact post-split hydration on a fresh request; forward-only v29 requires real `/graph-hydrate` traffic to match a scoped coordinator-to-owner outage before cancellation, then heals and exactly recovers | Add broader restart/topology and request/response/Raft short-write faults, disk-capacity pressure, broader socket-pressure and storage/process/restart overlaps, and cancellation under resource/storage/process/restart and multi-fault combinations to the production-owner history. Add global queries with the same fail-closed publication rule; v19/v20's join seams are audited separately |
| Distributed joins and global-query orchestration | **Partially integrated.** V19-v35 provide the cited public join, durable worker, cancellation, retry, overlapping-fault, and owner-reconstruction seams. V36 adds the first production-owner global-query claim: one two-line NDJSON request reaches `docs` and `tenant_b_docs` through `/db/v1/query`, preserves line order while flattening, and returns exact disjoint ID sets. Forward-only v37 cancels after the first production result, requires typed client cancellation plus handler drain and no second partial result, then proves an exact two-result recovery. Forward-only v38 revokes the second table's live read authority after the first result, requires an exact 403 with no protected result, restores policy, and proves exact recovery. Forward-only v39 cuts the registered tenant-owner query stream after the first result, returns the exact retryable 503 without a partial response, heals, and proves exact recovery. Forward-only v40 destroys the exact tenant-owner process at the same first-result boundary, requires the same no-partial 503, reconstructs its stable DataServer and listeners, requires a direct durable read from the rebound endpoint, and proves exact recovery. Focused composed-query tests cover additional result assembly | Add join cancellation under storage faults, disk pressure, simultaneous process loss before cancellation drain, and other fault combinations; auth and stale-generation changes; right/nested/foreign and multi-range-left joins; overlapping owner faults; and global-query topology, storage, resource, coordinator or metadata process loss, multi-process loss, and broader transport/overlap-fault recovery histories |
| Query cache, replication backfill, and service rates | **Integrated focused seams plus deployment composition.** Cache, DataServer Raft/LSM, distributed graph, replication snapshot/stream, and serverless workflow histories each prove exact slowed/healed production charging. V23 installs one shared model across DataServer, graph, and serverless owners. V42 adds clean production replication through the public/DataServer/Raft path. V43 keeps the same slowed first-batch boundary, changes schema, resumes from durable status with one exact duplicate batch, and completes baseline snapshot/stream work. V44 preserves the same accounting through target-owner teardown/reconstruction, bounded reconnect, durable resume, and direct/all-node recovery. V45 preserves it through actual provider-query failure, balanced session replacement, and resume without extra target work. V46 proves lease cancellation remains outermost while production checkpoint charging/deadlines are delegated and preserved. V47 adds charged ownership revalidation between target apply and checkpoint publication. V48 preserves the same accounting across metadata-Raft source publication, exact authority replacement, retirement, and replay. V49 installs cache charging on the actual ApiHttpServer owner and crosses a public logical deadline, explicit healing, DataServer reconstruction, exact recomputation, and bounded reconnect. All retain the complete cluster visibility, cleanup, and fresh-world replay oracle | Add cache topology/link/storage/resource overlap, additional replication topology/cancellation/source/target-crash timings, metadata leadership loss during cutover, and broader combinations of the existing link/storage/resource/restart algebra |
| Generation/reranking provider replacement and fallback | **Integrated local and remote production seams.** `generation-reranking-vopr-test` exact-replays remote OpenAI-to-Antfly fallback, malformed generation, truncated reranking, generation/reranking deadlines and in-flight cancellation, and request-scoped replacement for both adapters: each established remote call completes on its captured backend while the next call routes locally. The same trace proves one exact local generation and reranking call, ten remote requests, result validation, cleanup, record, and fresh-world replay | Actual model execution and GPU kernels remain differential; compose provider faults into a deployment history only when a product workflow owns that routing |
| Multi-table/tenant/resource/mixed-version breadth | **Partial/conditional.** Two-table cross-node isolation, in-cluster node-memory interference, one selected production-listener socket-denial history, and separate focused disk/socket quota and upgrade-artifact suites are integrated | Add authenticated tenants plus in-cluster disk-capacity interference and broader socket interference/overlap; live mixed-version nodes remain conditional on runnable compatible binaries |

Accordingly, “distributed VOPR is integrated” means the runtime foundation and
named seams are real and replay-proven. It does **not** mean the roadmap's
whole-deployment compositions are already complete.

### Implemented Extension Seams

The following seams are already implemented. They remain documented here so
new product work extends the same ownership and scheduling contracts instead of
introducing native-only alternatives.

#### Deeper DataServer and Raft Microsteps

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

#### HTTP Lifecycle and Backpressure

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

#### Background Runtime Lifecycle

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

#### Replication Backfill and Rebalancing

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

#### Standalone and Serverless Supervision

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

#### User and Authentication Lifecycle

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

#### Serverless Object-Store Protocols

Real WAL, catalog, manifest, artifact, and progress-store operations now run
over the reusable `ScriptedFaultClient`, covering partial committed transfer,
delayed visibility, duplicate completion, timeout-after-commit, cancellation,
retry, publication, reconciliation, and client crash. Object-backed WAL append
now also accepts a durable caller-supplied operation identity: retry after an
ambiguous timeout returns the original LSN, conflicting reuse fails closed,
and the identity survives read and truncation. Stores that cannot uphold the
contract reject idempotent append instead of silently degrading it. The full
HA seed backup/restore workflow uses the same provider: a committed chunk with
a lost response is reconciled by a restarted publisher, repeated publication
selects the same generation, cancellation before restore staging is harmless,
and retry downloads and verifies the complete chunked artifact. Extend these
protocol campaigns when new production object-store consumers are introduced.

#### Complete Serverless Workflow

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

#### DB and Index Request Races

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

#### Admission and Resource Pressure

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

#### Provider Boundaries

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

#### Composed Query Lifecycle

`composed-query-vopr-test` treats text, vector, and graph completion as
independent stable transitions, then executes the production distributed
`mergeSearchResults` and graph-union implementation at the global publication
boundary. It exact-replays every component completion ordering, graph partial
failure followed by retry, early and late cancellation, admission pressure,
capacity release, and final reassembly. Properties prove that no partial or
cancelled result is published, the canonical text/vector/graph set survives
every assembly ordering, graph results remain attached, and final admission
ownership is released.

#### Self-Contained Antithesis-Class Tooling and Search Quality

- Persisted pointer-free multiverse nodes, ranked counterfactual experiments,
  stable trial metadata, explicit total experiment budgets, cross-revision
  property history, scheduler-controlled completion order, and bounded
  systematic starvation are implemented in the self-contained repository.
- Every generic explored history has a bounded structured flight recorder.
  Parallel Antfly campaigns also exact-replay each candidate through a
  recorder and materialize it only for corpus insertion or failure. Verbose
  owned details and name/value fields are excluded from canonical trace bytes.
  Runner-backed scenarios and the custom metadata/domain paths all populate
  the recorder during exact replay. Conjunctive field/text predicates select
  bounded before/after windows, and automatic debug recipes package the
  selected reduced-replay window.
- Fielded event queries support kind, name, actor, resource, fault phase,
  transition and logical-time windows, `preceded_by`, `followed_by`, and
  same-actor/resource correlation. Versioned `vopr-event-set-v1` plans compose
  those selectors with union, intersection, difference, complement,
  distinct/first/last-per-moment, previous, next, and bounded sequence
  operators. Plans are forward-only validated DAGs with explicit result and
  match limits. `vopr events` accepts the saved format, clean-replays one or
  more histories, validates without running, or emits count-only/full results.
  A forward-only v2 runner observer serializes canonical events into a
  caller-owned fixed-slot NDJSON queue while a history is active. Publication
  cannot allocate, call external code, or block. Drop-newest overflow and
  publication after close are counted; a separate consumer drains complete
  records, retains the oldest record after sink failure, and may retry or drain
  the remaining queue after close. None of those diagnostics affect replay.
  Saved plans require an explicit format and use prior-step indexes, for example:

  ```json
  {
    "format": "vopr-event-set-v1",
    "name": "requests-followed-by-errors",
    "steps": [
      { "name": "requests", "operation": "select", "query": { "selector": { "name": "request" } } },
      { "name": "errors", "operation": "select", "query": { "selector": { "kind": "injected_error" } } },
      { "name": "sequence", "operation": "sequence", "inputs": [0, 1], "max_transition_distance": 8 }
    ],
    "result": 2,
    "max_matches": 100000
  }
  ```

  `vopr events --query query.json --validate` performs schema and DAG
  validation without requiring a trace. Repeated `--trace` arguments evaluate
  one plan across runs; `--count` omits match materialization from the result
  artifact. There is intentionally no compatibility parser for the earlier
  ad-hoc single-selector command input because no released VOPR artifact
  contract requires it.
- `vopr-results` emits stable `vopr-results-v1` JSON and an optional static
  local HTML report containing run metadata, budgets, property results,
  first-failure and rare-success evidence, declared-but-never-encountered
  properties, corpus entries, quarantine state, and artifact references.
  Parallel campaigns additionally publish aggregate `vopr-run-results-v1`
  JSON and static HTML after workers and quarantine export finish.
- `vopr-index` transactionally merges either results form into an atomically
  persisted canonical `vopr-run-index-v1`. It indexes runs and source
  revisions, properties, fingerprints, retained and quarantined corpus state,
  typed artifacts, and budget consumption. Stable CLI predicates emit
  `vopr-run-index-query-v1` JSON and an optional static local HTML summary.
- For every new failure fingerprint, campaigns write one automatic debug
  recipe containing same-fingerprint reduction, causal-window extraction,
  bounded counterfactual experiments, selected event queries, logical
  before/after collectors, and the reduced exact-replay artifact.
- `vopr-corpus-merge` exact-replays compatible local/CI/nightly candidates,
  quarantines incompatible or divergent bytes, and publishes a deterministic
  merged manifest after referenced artifacts are written.
- Fault definitions explicitly encode precedence, overlap, and exclusion
  groups. `fault_vopr_io.zig` applies the effective order to persistent and
  one-shot virtual network/storage effects, and the Parquet-cache suite uses
  it outside the algebra unit tests. `service_rate.zig` adds reversible,
  composable node/operation logical cost through borrowed `std.Io`; deployment
  and generic fault registries type the effect separately from pause and
  terminal resource exhaustion. The production query-embedding cache now opts
  in at request, hit-copy, coalesced-wait, and producer-compute boundaries;
  other production loops must still opt in at reviewed boundaries. The runner
  audits every choice record for typed, immediate scenario-level selection
  instead of delayed seed interpretation.
- The reusable command composer executes `first`, `parallel`, `serial`,
  `singleton`, `anytime`, `eventually`, and `finally` roles. Commands declare
  symmetric allow/deny compatibility, exclusion groups, active-fault policy,
  and before/after quiescence requirements. Quiet-suffix entry snapshots
  eventual obligations, waits for them and all active actors, then runs final
  obligations before completion; a focused scenario exact-replays the entire
  phase sequence.
- The determinism source gate rejects direct host entropy, delayed private
  PRNGs, host clocks, native threads/`Threaded` I/O, host filesystem access,
  native libraries, unordered map iteration, and pointer-derived identities in
  replayable adapters. Narrow native differential boundaries require a
  line-local category allowance with a non-empty rationale. The checked
  manifest must cover every exported Antfly VOPR source and explicitly audits
  the two replay regions in the mixed legacy metadata harness. Runtime evidence
  reports immediate structured choices and deterministic `std.Io` entropy
  calls separately.
- Default report health defines no progress/deadlock, unexpected crash, task
  and descriptor leaks, allocator/storage exhaustion, eventual recovery, final
  consistency, cleanup, replay divergence, and harness errors. The runner
  automatically samples continuous, recovery/quiescent, and final phases;
  generic scenarios receive progress/cleanup evidence, `VoprIo` scenarios use
  a reusable resource adapter, and mature P0/P1 suites add domain recovery and
  consistency evidence. These diagnostics are deliberately excluded from
  canonical trace bytes and are rematerialized by exact replay for results.
- `vopr-benchmark` runs intentionally injected scheduling-starvation,
  unstable-publication, and cancellation/admission defects across random,
  guided, spliced, starvation, and checkpoint-assisted exploration.
  `vopr-search-quality-v2` reports repeated occurrences, empirical discovery
  probability and rarity, Wilson 95% confidence bounds, executed-transition
  and logical-search cost, first discovery cost, and the simplest witnesses by
  transition count and retained canonical output bytes. Every retained
  generated, mutated, spliced, or checkpoint-assisted history is exact-replayed
  before corpus insertion.
- `vopr-debug` supports replay-proven navigation, branch creation, logical
  collectors, causal and bounded counterfactual windows, and child comparison
  from command files or an interactive terminal.
- The virtual filesystem supports one-shot read corruption, durable sector
  corruption, clearing persistent corruption, and torn synchronization that
  preserves only a selected durable prefix while retaining the prior tail.
- The generic datagram model is ready, but this repository currently has no
  production provider-specific datagram consumer to campaign. Add such a
  campaign with the first real consumer rather than inventing a test-only one.
- Add stable source/basic-block coverage as secondary guidance when the Zig
  instrumentation surface can remain outside the replay ABI.

### Priority Test Roadmap

| Priority | Area | What to exercise |
| --- | --- | --- |
| P0 integrated | Replication backfill and rebalancing | `replication-backfill-vopr-test` covers snapshot-to-streaming cutover, resumable checkpoints, duplicate work, cancellation, source and target crashes, topology changes, stale ownership, schema changes, and exact replay through the production runners. Its source-crash mode fails the actual provider query and proves exact session close/replacement. Its cancellation mode revokes the lease after durable offset 1 and proves replacement-session resume without duplicate target application. Its stale-owner mode revokes before checkpoint publication and proves exactly one safe replay. Its topology mode now uses prepared exact-cutover snapshots, rejects the old source catalog at offset 0, rotates nonzero authority, retires the predecessor, and replays exactly once instead of throwing from a fixture hook. The same gate proves typed snapshot/stream charging. Full-cluster v42 promotes the clean path through canonical public BatchRequest routing, leader forwarding, DataServer Raft, index visibility, and exact pre/post-heal work. Forward-only v43 changes schema after the first accepted batch, requires interruption and durable resume, accepts exactly one duplicate batch, completes CDC, and verifies all documents from every public node. Forward-only v44 destroys and reconstructs the current target leader before the next batch, requires one stopped-endpoint and one bounded pooled-reconnect failure, then proves stable-identity recovery, three exact successes, durable local/direct public reads, and all-node visibility. Forward-only v45 fails the prepared source session on its first real query, requires exact deinit and a newer recovery session, and preserves three target attempts/successes plus all-node visibility. Forward-only v46 cancels after the first durable checkpoint, requires exact lease loss and sequential session replacement, and preserves three target attempts/successes plus all-node visibility. Forward-only v47 rejects the stale owner before offset 1 publication and requires four target successes with one idempotent replay. Forward-only v48 publishes both source catalogs and exact authority transitions through the live metadata quorum, rotates and retires authority, and preserves four target successes plus every cluster oracle. Additional topology/cancellation/source/target-crash timings, metadata leadership loss during cutover, and overlapping-fault cases remain future breadth. |
| P0 integrated | Standalone and serverless supervision | `supervision-vopr-test` covers partial-startup rollback, readiness publication, child-service failure, coordinated shutdown, virtual watchdog expiry, and restart through the production supervisor. The serverless manager now owns a borrowed-`std.Io` Future instead of a native run-loop thread. |
| P0 integrated | User and authentication lifecycle | `auth-lifecycle-vopr-test` covers password, API-key, permission, and row-filter changes; deterministic seed capture; revoke and rotate; durable reload; partial persistence rollback; and stale-reader behavior through the production manager. |
| P1 integrated | Complete serverless workflow | `serverless-workflow-vopr-test` covers durable claim/fencing, build, compaction, publication, and query-visible catalog cutover with duplicate workers, lease takeover, ambiguous completion, retry, cancellation, crash recovery, stale-enricher generation rejection, and progress-conflict fencing under exact replay through production orchestration. Typed runtime work rounds additionally prove overlapping node/publish slowdown, independent healing, baseline compaction, exact cost, and final fenced visibility. In full-cluster v9 the same fixture borrows the cluster `VoprIo`, serves its worker-owned object catalog through the production serverless HTTP stack, and proves public table/head/document visibility. Cross-domain fault overlap and multi-worker placement remain follow-up depth. |
| P1 integrated | DB and index request races | `db-index-race-vopr-test` exact-replays cross-index admission, same-index FIFO fairness, delete/materialize linearizations, published-reader/catalog-writer capture, cancellation, shutdown, and cleanup through production-safe seams rather than native test threads. |
| P1 ongoing | Managed-index publication and public readiness | Compose the current dense-index generation and API readiness contracts on one `VoprIo`: atomic versus progressive publication, backfill and dense-replay watermarks, partial coverage, rate-limit recovery, alias/catalog cutover, public-query visibility, cancellation, and crash/restart between durable generation, catalog publication, and readiness reporting. The DB/index race and generation-lifecycle gates prove constituent ownership and publication seams; they do not yet prove this end-to-end readiness state machine. |
| P0 integrated combined active-transition/graph/resource/service-rate seam | Full-cluster distributed composition | Full-cluster v9 retains the registered hosted/public nine-fault deployment. V11-v40 add the cited real metadata/DataServer/public/serverless, active-transition, graph/join/global-query, fault, cancellation, authorization, durable-worker, and reconstruction seams. V42 adds clean production replication through alternating public coordinators and DataServer Raft/index visibility; its 160,000-transition gates pass 15/15. Forward-only v43 adds schema-change interruption, durable resume, one exact duplicate target batch, CDC completion, and every existing cluster oracle; its 180,000-transition gates pass 15/15. Forward-only v44 adds exact target-leader process restart, stable listener/identity reconstruction, bounded reconnect, durable resume, and every existing cluster oracle; its 220,000-transition Debug and ReleaseSafe gates pass 15/15. Forward-only v45 adds exact provider-query failure, source-session replacement, durable resume without extra target work, and every existing cluster oracle; its 220,000-transition Debug and ReleaseSafe gates pass 15/15. Forward-only v46 adds durable-checkpoint lease cancellation, sequential session replacement, composed charging, no duplicate target work, and every existing cluster oracle; its 220,000-transition Debug and ReleaseSafe gates pass 15/15. Forward-only v47 adds stale-owner rejection in the apply-to-checkpoint gap, durable offset fencing, one exact idempotent replay, and every existing cluster oracle; its 240,000-transition Debug and ReleaseSafe gates pass 15/15. Forward-only v48 adds exact source-catalog publication, cutover-authority replacement and retirement through metadata Raft, one idempotent replay, and every existing cluster oracle; its 260,000-transition Debug and ReleaseSafe gates pass 15/15. Forward-only v49 replaces the adjacent cache fixture with the live node-1 `ApiHttpServer` owner and proves slowed coalesced deadline expiry, retained hit, exact process reconstruction, empty replacement cache, one recomputation, one stale pooled reconnect, and a durable rebound read; its 120,000-transition Debug and ReleaseSafe gates pass 15/15. These promote only the named seams. Next add cache topology/link/storage/resource overlap; additional replication topology/cancellation/source/target-crash timings, metadata leadership loss during cutover, and cross-domain fault variants; storage/process/multi-owner cancellation and recovery; global-query topology/storage/resource/coordinator/metadata loss; broader socket/short-write/restart targets; disjoint placement; retained-history and snapshot/derived-state recovery; HA/data-plane co-residency; richer public queries; and multi-worker placement. |
| P0 ongoing | Repository-wide strong-read contract | V13 proves an owner-specific synchronous DataServer barrier: matching ReadState apply plus derived-state visibility, bounded by the request timeout/cancellation. V19 extends that barrier from graph callbacks to production Provisioned preflight, response-producing exact-group queries, and optimized `SearchResult` callbacks after reproducing acknowledged-but-empty full-text reads on both join sides. `ReadIndexRequester` is now an enqueue-only capability and `ReadSafetyBarrier` is a distinct synchronous capability; managed host services expose only the former, while public table-read sources require the latter. DataServer uses reusable `AppliedReadTracker` ownership for one canonical request context, matching-group ReadState observation, applied-index completion, cancellation, and group retirement. Replicated DataServer construction starts fail closed with `unavailableReadSafetyBarrier`; startup installs the real barrier after Raft wiring, while direct non-Raft ownership is explicitly marked `alreadyReadSafeBarrier`. The three-production-owner merge/split history now exact-replays the DataServer behavioral matrix: follower forwarding or typed `NotLeader` without retained ownership, leader-change completion-or-timeout plus replacement-leader retry, logical timeout, cancellation, state-machine group retirement, and post-split graph/full-index visibility. The old readable-lease types, service adapters, no-op API, and metric names were deleted rather than aliased. Remaining repository-wide work is to audit every custom callback barrier and add the same behavioral depth for each distinct production owner before another public source relies on it. |
| P0 integrated | Query-embedding cache | `query-embedding-cache-vopr-test` exact-replays concurrent-miss coalescing, waiter cancellation, deadlines, in-flight admission, TTL, byte-budget/LRU eviction, pinned hits, and cleanup through the production cache on one `VoprIo`. V2 adds a production-neutral cost port at request, hit-copy, coalesced-wait, and producer-compute boundaries and proves baseline cost, overlapping node/hit slowdown, real deadline expiry, independent healing, resumed success, exact usage, and cleanup. Forward-only full-cluster v49 installs that port on node 1's actual production `ApiHttpServer` cache. One producer remains in flight while a slowed same-key waiter crosses its logical deadline; healing permits the producer and one retained hit to complete. After the ordinary public workload establishes durable state, the exact DataServer owner is reconstructed: its replacement cache must start empty, recompute the same key exactly once, retain the next hit, absorb exactly one stale pooled-connection failure, and serve a pre-restart durable document through the rebound endpoint. DataServer, graph, serverless, visibility, quiet cleanup, and fresh-world replay oracles remain active. Its 120,000-transition Debug and ReleaseSafe gate passes 15/15. Cache topology/link/storage/resource overlap remains future breadth. |
| P1 partially integrated | Distributed graph/public-query boundaries | `distributed-query-vopr-test` exact-replays production cross-range planning, two-shard fanout/hydration, topology retry/exhaustion, stale generations, cancellation, cross-table authorization, and per-group charging. Full-cluster v9 and production-owner v22-v35 add the cited public HTTP, active split, transport, restart, short-write, memory/socket pressure, hydration, in-flight authorization, stale-snapshot, durable-join, overlapping-fault, and owner-reconstruction histories. V36 adds the production-owner global NDJSON baseline with exact response order and table isolation. Forward-only v37 cancels at the first production result boundary, requires typed cancellation, handler drain, and exactly one canceled-request result, then proves exact recovery. Forward-only v38 revokes live second-table authority at the same result boundary, requires an exact 403 without a protected result, restores policy, and proves exact recovery. Forward-only v39 cuts the registered tenant-owner query stream at that boundary, requires one matched outage and the exact retryable 503 without partial output, heals, and proves exact recovery. Forward-only v40 destroys the exact tenant-owner process at the same first-result boundary, requires the same no-partial 503, reconstructs its stable DataServer and listeners, requires a direct durable read from the rebound endpoint, and proves exact recovery. Cancellation under storage faults, disk pressure, simultaneous process loss before cancellation drain, and other fault combinations; broader join forms; additional overlapping-owner fault shapes; global-query topology/storage/resource faults, coordinator/metadata or multi-process loss, and broader transport/overlap shapes; broader restart/short-write/socket overlap; and storage/process/restart overlaps are not yet composed on the production owners. |
| P1 integrated | Generation and reranking chains | `generation-reranking-vopr-test` exact-replays generation success, retry/backoff on borrowed `std.Io`, timeout and rate-limit fallback, cancellation, reranking success, malformed count/non-finite results, timeout, and cancellation through production chain and local-provider boundaries. Its v2 composed mode additionally sends ten requests through production remote OpenAI/Antfly generation and Antfly reranking adapters in one trace: fallback, malformed/truncated responses, logical deadlines, in-flight cancellation, and request-scoped replacement for both adapters while the next generation and reranking requests route locally. Record and fresh-world exact replay require exact remote/local call counts, result values, typed errors, cleanup, and no capability violation. Actual models and GPU kernels remain differential. |
| P1 ongoing | Remote-content credential use boundary | Join the integrated live-reference configuration/store contract to a real scraping or object-fetch request. Resolve access key, secret, session token, and header references immediately before provider use; rotate while an old request is in flight; retry and cancel through borrowed `std.Io`; and prove that snapshots retain references while each new request observes one coherent secret generation. The current config lifecycle proves publication and the production resolver independently, while `lib/scraping` still copies credential strings at its lower request-construction seam. |
| P2 integrated | Multi-table and cross-node workload dimensions | The full-cluster history provisions two independently replicated tables and drives four concurrent clients through three public nodes. A tenant sentinel must remain visible in its table and absent from the other table while both share the same scheduler, HTTP transport, sockets, and node resources. This proves table isolation and routing interference, not authenticated tenant identity. |
| P2 partial | Resource-interference workload dimension | `admission-vopr-test` proves cross-service memory, disk, task, file, socket, and cancellation ownership. The current full-cluster campaign composes explicit node-memory denial/recovery and one exact selected-listener socket denial/recovery with real public clients, production resource managers, background database owners, HTTP routing, and cleanup. Carry disk-capacity denial into the true reservation-consuming managed-index/cache seam, broaden socket targets, and overlap pressure with link/storage/restart faults. |
| P2 conditional | Live mixed-version workload dimension | Add rolling old/new binary operation only when two compatible runnable versions and an upgrade contract exist. `upgrade-compatibility-vopr-test` currently proves artifact readers, migration, safe rejection, and crash recovery; it is not live mixed-version cluster coverage. |
| P2 integrated | Provider boundaries | `provider-boundary-vopr-test` uses the real ManagedEmbedder and PostgreSQL Source boundaries for timeout, partial response, cancellation, retry, malformed data, SQL construction, and admission ownership. Actual models, GPU kernels, and libpq internals remain differential/integration concerns. |
| P2 integrated | Composed query lifecycle | `composed-query-vopr-test` exact-replays vector, text, graph, and global-query completion under partial failure/retry, cancellation, resource pressure, and every result-assembly ordering through production merge and graph-union code. |
| P1 integrated | Persistent Parquet cache | `parquet-cache-vopr-test` runs the real borrowed-I/O worker, bounded queue, duplicate-write coalescing, read/write faults, durable sync, crash, reopen, and checksum-protected reads. It is the first production consumer of the reusable fault-to-`VoprIo` adapter. |
| P1 integrated | Provisioning and startup | `provisioning-startup-vopr-test` runs real format admission and replica-root reconciliation through a manual `BackendRuntime` borrowing `VoprIo`, including repeat startup, partial markers, legacy-store rejection, failed atomic-write retry, and crash/restart. |
| P1 integrated | External lake | `external-lake-vopr-test` retains the focused range/cache histories and composes catalog binding, object-backed Iceberg metadata discovery, production Avro manifest decoding, schema evolution, pinned inventory, Parquet footer metadata, row-group cache, and query assembly. Twelve exact-replayed modes cover cache reuse, short responses, timeout/admission, stale object versions, deletion, ambiguous completed downloads with retry, bounded eviction, and durable persistent-cache crash/reopen without an object re-download. |
| P2 integrated | Media-provider execution and runtime | `media-runtime-vopr-test` exact-replays production Antfly STT and OpenAI-compatible TTS HTTP success, malformed JSON, truncated bodies, logical timeout, POST retry, partial-startup rollback, nested and in-flight runtime replacement, and shutdown cancellation/drain on borrowed `VoprIo`. `httpx.Client` closes admission and drains committed requests before shared provider state is destroyed. Real codecs, models, and GPU execution remain differential/integration concerns. |
| P2 integrated | Product upgrade and compatibility campaigns | `upgrade-compatibility-vopr-test` exact-replays ten histories covering v1 HA golden replication/checkpoint/backup bytes, legacy and future data-directory admission, crash-before-rename recovery, and legacy/future serverless head, v14 inventory, and v12 manifest artifacts. Outcomes are explicit forward completion, rollback/retry, or safe rejection. VOPR-native artifacts are excluded: traces, checkpoints, saved plans, run indexes, and fixtures have one current schema and no compatibility or migration path. |

The source-boundary audit also identifies four useful additions after the
current P0 distributed composition. These are independent candidates, not
prerequisites for the integrated rows above:

| Priority | Area | Why it is still useful |
| --- | --- | --- |
| P1 | Production metadata-admin/control path | Drive the actual `MetadataService`/`MetadataHttpService` mutation path—including reallocation timestamps, leadership change, ambiguous admission, status reporting, and split/merge requests—over borrowed clocks and real metadata HTTP. Current distributed histories prove the underlying quorum and workflows, while some HTTP simulations intentionally use a harness source instead of the complete production service owner. |
| P1 | MCP and A2A session/task state machines | Compose production session expiry, event-id replay, task reservation/generation fencing, cancellation races, bounded capacity, SSE disconnect/reconnect, and shutdown on `VoprIo`. Both libraries already accept caller-owned `std.Io`; the missing value is an Antfly-level public orchestration history, not another runtime abstraction. |
| P2 | Cloud authentication and signed object requests | Put Google token refresh and S3/GCS signing immediately in front of the existing deterministic object-store response adapters. Exercise credential rotation, refresh collapse, clock skew, timeout-after-send, retry, cancellation, and stale-token rejection without contacting a cloud service. |
| P2 | Extension invocation lifecycle | Extend the integrated extension install/configuration/startup histories through a real bounded Wasm invocation: concurrent configuration replacement, host-call cancellation, fuel/memory admission, trap, shutdown, and durable restart. Keep the Wasmtime engine itself differential; schedule the Antfly ownership and publication boundaries. |

Replication backfill and the standalone/serverless supervisor were the first
targets because they have the richest combinations of durable state,
ownership, concurrency, and recovery; both are now integrated. The complete
serverless workflow and DB/index request-race compositions are also
integrated, as are the P2 provider, query, media, and compatibility suites.
The query-cache and generation/reranking focused campaigns are integrated.
The full-cluster active-transition seam is integrated, while its broadest
cross-composition roadmap and the distributed-query campaign remain partial at
the boundaries described in their rows. Future test work is targeted
composition, workload dimensions, and newly exposed safe suspension points
rather than missing scheduler or replay foundations.

### Integrated Targeted Suites

These are new suites or compositions, not retroactive dependencies of the
integrated rows above.

| Priority | Area | What to exercise |
| --- | --- | --- |
| P0 integrated | Generation publication and cleanup | `generation-lifecycle-vopr-test` drives the production transition manager with one borrowed `std.Io` through clean publication, prepared rollback, rename retry, uncertain directory sync and reconciliation, prepared crash recovery, shared-reader/exclusive-publisher locking, canonical aliases, and stale-generation cleanup. Restore and HA materialization now propagate the same I/O through transition locks and publication cleanup. |
| P0 integrated | Metadata backfill-marker discovery | `backfill-marker-discovery-vopr-test` drives the production scanner and cache on borrowed filesystem and monotonic-clock capabilities through absent, legacy, valid-owned, corrupt, ownership-mismatch, throttled appearance, disappearance/rescan, and read-fault/restart histories. Metadata service and HTTP rounds use their backend runtime I/O for scans and rechecks. |
| P0 integrated | Configuration, secrets, remote content, and extensions | `config-extension-lifecycle-vopr-test` exact-replays valid, malformed, and incomplete cold starts; secret rotation with retained readers; crash between durable secret and configuration publication; remote-content replacement, rejected-candidate rollback, and recovery; extension administrative install/dry-run, replacement, disable/enable, and configuration; malformed package recovery; and failed Wasm startup. The snapshot deliberately preserves live `${secret:...}` references and the scenario resolves them through the production store instead of falsely requiring eager substitution. The production secret store, remote-content runtime, extension lifecycle timestamping, package scanner, and Wasmtime artifact loader borrow `std.Io`; portable directory durability no longer escapes through POSIX. Resolving and rotating those references at an actual scraping/object-fetch request boundary remains an ongoing composition below. |
| P1 integrated | Embedded, C API, and Lite lifecycle | `embedded-lite-lifecycle-vopr-test` exact-replays native Lite crash/reopen, overlapping Embedded writer/reader lifetimes, C API readable-lease callback install/remove, canceled restore, atomic replacement with a pinned old reader, and current-generation visibility. Native Lite, Embedded DB, opaque C API handles, and restore staging share caller-owned `std.Io`/`BackendRuntime`; a physical-versus-`VoprIo` differential compares logical values and checkpoint sequences. |
| P1 integrated | Cross-service resource pressure | `admission-vopr-test` composes one production `ResourceManager`, request-admission controllers, and `VoprIo` envelope across query/write request ownership, a real Lite-backed DB write and lookup, the durable-job lane, a cancelable ManagedEmbedder provider call, and the persistent Parquet range cache. Eight schedules exact-replay aggregate and slice-memory denial, cache queue denial, capacity-domain denial before disk I/O, task/file/socket exhaustion, cancellation cleanup, and progress after pressure clears. `resource-budget-test` guards the named lake-queue default mapping. |
| P1 integrated | Full external-lake composition | `external-lake-vopr-test` now traverses catalog binding, object-backed Iceberg metadata and manifest discovery, schema evolution, Parquet footer discovery, row-group caching, and production query assembly. It fails closed on stale versions and deleted objects, retries an ambiguous completed download through the reusable object-store fault adapter, proves bounded cache eviction, and reopens a durable cache after a `VoprIo` filesystem crash without downloading cached objects again. |

### Ported Antithesis-Class Features

VOPR already has the important core: structured controlled choices, the major
Antithesis assertion kinds and assertion cataloging, deterministic scheduling
and I/O, logical checkpoints, exact replay, reduction, semantic coverage,
starvation, causal and counterfactual analysis, and multiverse navigation. The
documented [Antithesis assertion
model](https://antithesis.com/docs/product/writing_tests/assertions/) is
therefore substantially covered. The hard, high-value features identified for
local replacement are integrated:

1. bounded retroactive flight recording with structured field/text selection
   and diagnostic before/after windows, the self-contained analogue of the
   [August 2026 Retroactive Logging
   feature](https://antithesis.com/docs/release_notes/);
2. fielded and temporal event-history queries comparable to [Antithesis event
   logs](https://antithesis.com/docs/reference/event_logs/);
3. stable repository-owned JSON results, a persistent cross-run usage index,
   and static reports corresponding to
   the run/log APIs described in the [Antithesis release
   notes](https://antithesis.com/docs/release_notes/);
4. automatic reduction/causal/counterfactual/query/collector/flight-window
   debug recipes;
5. first-failure, rare-success, structured-detail, and never-encountered
   property evidence;
6. explicit overlapping-fault precedence and exclusion algebra, matching the
   useful application-level behavior of [overlapping
   faults](https://antithesis.com/docs/product/writing_tests/controlling_faults/fault_types/);
7. automatic structured-choice auditing, following the controlled-alternative
   principle in [Antithesis controlled
   randomness](https://antithesis.com/docs/reference/sdk/generate_randomness/);
8. default harness-health reporting with automatic continuous,
   recovery/quiescent, and final snapshots, plus reusable `VoprIo` resource
   evidence and mature P0/P1 domain adapters; and
9. a representative injected-bug search-quality regression corpus with
   recurrence, rarity, confidence, modeled cost, and smallest-witness evidence
   across all five exploration policies; and
10. a registered deployment composer for roles, instances, readiness
    dependencies, directional links, typed process/storage/resource fault
    domains, measured node policy, and cluster-wide quiet suffixes;
11. versioned saved event-set programs with fail-closed DAG validation,
    selection and set algebra, distinct/first/last moment and
    previous/next/sequence operators, bounded cross-history evaluation,
    count-only CLI results, and repeated-run input. Operator-complete tests and
    CLI meta coverage reject obsolete/ad-hoc schemas rather than adapting them;
    a runner-owned live NDJSON observer publishes into a power-of-two,
    fixed-slot SPSC queue with release/acquire record publication, explicit
    drop-newest backpressure, atomic saturating telemetry, and close semantics.
    One consumer may drain concurrently; sink delivery and retry remain outside
    execution. Invalid capacity, oversize, concurrent accounting, retry, and
    close have direct tests, while queue pressure has byte-equivalence against
    an unobserved history;
    and
12. a reversible service-rate model with unambiguous stable node and operation
    identities, fully checked base multiplication, wide rounding, and
    compositional multipliers in canonical fault-ID order,
    per-node and per-operation charge/unit/nanosecond accounting, and a
    node-bound borrowed-`std.Io` charge port. The generic fault and deployment
    registries recognize this effect independently from pause and terminal CPU budgets;
    the production query-embedding cache supplies the first reviewed adapter
    and exact-replay overlap/deadline/healing proof. DataServer supplies a
    second adapter at each Raft progress round, with node-local slowdown,
    leader transfer, exact accounting, healing, and a full recovery suffix.
    The distributed graph coordinator supplies a third adapter keyed by target
    group and expand/hydrate/get-edges operation, with slowed and healed
    parallel-fanout passes and exact per-owner accounting. DataServer LSM
    maintenance supplies a fourth reviewed operation class with a slowed real
    attempt, explicit healing, a baseline retry, and exact logical usage.
    Replication supplies the fifth boundary: typed snapshot and stream permit
    checkpoints with overlapping node/snapshot slowdown, independent healing,
    completed snapshot-to-stream cutover, and exact per-class accounting.
    Serverless orchestration supplies the sixth boundary: publish, enrichment,
    compaction, and prune rounds, with overlapping node/publish slowdown healed
    across two publications before baseline compaction and fenced visibility.
    Full-cluster v23 composes the DataServer, graph, and serverless adapters on
    one model and clock: node-wide two-times costs are exact before healing,
    baseline DataServer/graph costs are exact afterward, no effect survives,
    and public visibility plus quiet cleanup exact-replay. Forward-only v49
    installs the cache work port on the actual cache owned by node 1's public
    `ApiHttpServer`/DataServer process. It proves exact slowed same-key
    producer/coalesced-wait work with real deadline expiry before healing, an
    exact baseline retained hit afterward, then destroys and reconstructs the
    owner, proves the replacement cache is empty, recomputes once, retains one
    hit, reconnects the long-lived client after one exact stale-pool failure,
    and reads pre-restart durable data. Exact byte/cost accounting, zero
    in-flight work/effects, full cluster visibility and cleanup, and fresh-
    world replay remain required. Forward-only v42 adds the production replication
    runner and public BatchRequest target to the same deployment: snapshot
    work and the first accepted batch occur before healing, remaining snapshot
    and stream work runs at baseline, and every replicated document is
    Raft/index visible through all public coordinators with exact accounting
    and fresh-world replay.
    Forward-only v43 interrupts after the first accepted snapshot batch,
    changes schema, resumes from durable status, and requires one exact
    duplicate target batch before CDC completion and the same terminal oracle.
    Forward-only v44 destroys and reconstructs the current target leader at
    that boundary, preserves exact slowed/healed accounting across two bounded
    transport failures, and resumes to three exact successes plus durable
    local, direct public, and all-coordinator visibility.
    Forward-only v45 fails the first actual source query after durable
    preparation, closes that session, resumes through a strictly newer owned
    session, and preserves exact target work and the same terminal oracle.
    Forward-only v46 revokes the lease after durable snapshot offset 1,
    composes outer fencing with delegated production charging, replaces the
    source session, and resumes without a duplicate target batch.
    Forward-only v47 revalidates ownership between target apply and checkpoint
    publication, rejects the stale owner at offset 0, and resumes with one
    exact idempotent replay.
    Forward-only v48 keeps that charged path active while the exact source
    catalog, authority claims, authority checks, and predecessor retirement
    cross the live metadata quorum.

These capabilities are local libraries, commands, reports, and CI gates. They
do not require an Antithesis account or hosted runtime. This is not a claim of
full Antithesis product parity: the implemented claim is limited to the named
self-contained engine features and application-level distributed fault domains,
while separate-address-space determinism and the operational work below remain
explicitly unfinished or conditional.

The remaining Antithesis-class opportunities are narrower than the engine
work already completed:

- extend the v11-v49 cluster compositions with search policies that overlap
  node-wide and operation-class slowdowns
  with remaining topology/ownership changes and the existing
  link/storage/resource/restart algebra, additional replication topology/
  cancellation/source/target-crash timings, metadata leadership loss during
  cutover, and quiet recovery.
  The reusable effect, clean backfill, schema-resume, exact target-owner-
  restart, prepared-source-session-crash, durable-checkpoint-cancellation, and
  pre-checkpoint stale-owner, metadata-backed exact-authority-rotation, and
  cache deadline/owner-reconstruction compositions are complete;
  arbitrary instruction-level native-thread throttling is still
  a federated/native instrumentation concern rather than an in-process claim;
- grow the saved event-set expression vocabulary only in response to concrete
  debugger/report needs. The versioned plan deliberately provides typed set,
  moment, previous/next, and sequence operations instead of an embedded
  general-purpose map/fold language; reports can add reviewed aggregations
  without making arbitrary code part of a saved query;
- make nightly campaign sharding, corpus merge, retention, quarantine review,
  usage indexing, and notifications a repository-owned operational workflow;
- extend the registered-source determinism audit through transitive production
  callees reached by borrowed-`std.Io` scenarios, with reviewed exceptions and
  stable semantic lock/operation identities. The present manifest is a useful
  fail-closed gate but did not detect the production cache-role address order
  that the v12 deep history exposed;
- ingest source/basic-block coverage only as search guidance, with fail-closed
  symbolization, when Zig instrumentation is stable enough to avoid entering
  the replay ABI; and
- add more automatic debug-recipe policies as evidence accumulates, such as
  selecting collectors and counterfactual budgets from a failure class.

Antithesis's container/Kubernetes execution, browser notebook, arbitrary shell
and file injection, and hosted control plane are not missing VOPR correctness
features. They are deliberately replaced by registered `std.Io` production
entrypoints, exact local artifacts, the line-oriented debugger, and ordinary
native/container differential tests. Reimplementing a deterministic
hypervisor or hosted UI would be the expensive part of Antithesis without
improving the in-process scheduler's visibility.

Antithesis also announced control of the guest kernel's internal random-number
generator in its [August 2026 release
notes](https://antithesis.com/docs/release_notes/). VOPR's self-contained
equivalent is complete only for code that draws entropy through registered
`std.Io.randomSecure` and whose source is included in the registered audit.
Transitive production-call-graph proof, interception inside an arbitrary
unmodified native dependency, and guest-kernel RNG control are not implemented;
they belong with the audit and conditional federated/native fidelity work, not
under the integrated entropy claim.

Deterministic distributed testing itself is therefore not an unported
Antithesis engine feature: VOPR already supplies registered
node/process/resource/link domains. Antithesis currently scopes node faults to
containers or Kubernetes pods and can inject asymmetric network loss between
separate nodes, as documented in [Types of
faults](https://antithesis.com/docs/product/writing_tests/controlling_faults/fault_types/);
VOPR's application-level equivalent is its registered instance and directional-
link manifest. Deterministic execution of arbitrary
separate address spaces, sidecars, DNS resolvers, kernels, and mixed binaries
*is* an unimplemented fidelity layer. It should remain a native/container
differential or conditional future project unless a real defect class cannot
be reached through registered `std.Io` entrypoints. The immediate material gap
is which Antfly owners and public workflows have been composed into the same
history, tracked in the Distributed Completion Audit and Ongoing Roadmap.

If separate-address-space fidelity becomes necessary, keep it self-contained
as a **federated VOPR agent protocol**, not a hidden claim that ordinary
containers are exactly replayable. The repository-owned coordinator would
remain the sole source of structured choices, logical time, fault composition,
property identity, and artifacts. One versioned agent per instrumented Antfly
process would register its manifest instance and stable actor/resource IDs;
all cross-agent network, clock, entropy, process, and modeled-storage requests
would pass through a framed broker, which releases one completion at a time and
records the decision in the ordinary trace. Agent loss, protocol mismatch,
unregistered native I/O/thread/clock use, or a broker bypass must fail closed.
Logical checkpoints would require an acknowledged application checkpoint from
every agent rather than pretending to snapshot OS process memory.

That design has two explicitly different modes. **Brokered deterministic mode**
would accept only instrumented compatible binaries and could earn exact replay.
**Native/container differential mode** could launch unmodified binaries,
sidecars, DNS, TLS, and real kernels from the same deployment manifest and
collect the same properties/events, but would never be labeled exact replay.
Build the brokered mode only when an address-space-specific defect or live
mixed-version requirement justifies its protocol and operational cost; until
then, deepen the in-process registered-owner composition first.

### Integrated Self-Contained Platform Work

| Priority | Capability | Required work |
| --- | --- | --- |
| P0 integrated | Reusable command-template composer | `lib/vopr/command.zig` implements Antithesis-style `first`, parallel, serial, singleton, anytime, eventually, and finally roles. Commands declare symmetric compatibility/deny lists, exclusion groups, fault policy, and before/after quiescence requirements. The composer tracks stable active invocation identities, enforces singleton and serial admission, snapshots quiet-suffix obligations, and exact-replays eventual/final completion. |
| P0 integrated | Registered deployment composer | `lib/vopr/deployment.zig` validates deployment roles and acyclic readiness dependencies, node and instance identities, directional links, globally disjoint process/storage/resource domains, typed fault/domain compatibility, node-local resource policies, and per-node quiet acknowledgments. Full-cluster v9 registers its four owners, seven role instances, six metadata links, and all infrastructure fault modes, then refuses completion until every fault is healed and every required node supplies bounded, task/socket-quiet evidence. |
| P0 integrated | Registered-source entropy and determinism audit | `lib/vopr/determinism.zig` admits only immediate structured choices and borrowed-`std.Io` entropy as runtime evidence. `vopr-determinism-audit` fail-closes on host RNG, delayed private PRNGs, host clocks, native threads/I/O, filesystem escapes, native libraries, unordered iteration, and pointer-derived identity in sources explicitly listed in its manifest; reviewed differential boundaries require line-local categorized rationale. The manifest contains every exported VOPR scenario and both legacy metadata replay regions. This is an implemented registered-source gate, not proof over every transitive production callee, guest-kernel interception, an arbitrary unmodified C library, or a separate process. |
| P1 integrated | Continuous and quiescent validation phases | The runner automatically samples every history in continuous, recovery/quiescent, and final phases, aggregates bounded no-progress and recovery evidence, classifies allocator and unexpected process/panic failures, and retains the pointer-free diagnostic outside canonical replay bytes. `VoprIo.healthSnapshot` automatically populates task, descriptor, and optional physical-storage evidence; the replication, supervision, auth, serverless-workflow, DB/index, provider, composed-query, resource-pressure, cache, startup, generation, configuration, Embedded/Lite, external-lake, media, and upgrade/compatibility suites add domain progress, consistency, exhaustion, and cleanup semantics. `vopr-results` uses exact-replayed evidence automatically. |
| P1 integrated | Richer retroactive logging | `event.Event` and the bounded `flight_recorder` own diagnostic name/value fields and text independently of canonical replay fields. Filters combine event identity, kind, actor/resource, logical index, exact or substring field predicates, and text search; materialization adds bounded before/after context. Generic, domain, distributed-data, and custom metadata replays feed the recorder directly, and every automatic debug recipe exact-replays its reduced artifact into a configurable flight window. `vopr-recipe` exposes filter, window, capacity, and limit controls. |
| P1 integrated | Local run index and usage API | `lib/vopr/run_index.zig` transactionally ingests canonical per-history, aggregate, and existing index JSON; requires an explicit stable run identity; validates referential integrity; deduplicates stable run/history keys; and canonically indexes source revision, properties, fingerprints, corpus/quarantine counts, typed artifacts, and transition/resource/history budgets. `vopr-index` atomically persists the index and exposes run/revision/scenario/property/fingerprint/corpus/artifact/budget predicates as deterministic JSON or a static local HTML summary. Parallel campaign results carry stable run identity, source/target/optimize metadata, and retained/quarantine artifact references. There is no legacy aggregate parser or synthesized fallback identity. |
| P1 integrated | Search recurrence and rarity reporting | `benchmark.zig` owns three reviewable injected defects representing scheduler starvation, publication after unstable durability, and cancellation/admission leakage. Each runs under random, guided, spliced, starvation, and checkpoint-assisted policies. `vopr-search-quality-v2` reports repeated occurrences, discovery probability and complement rarity in ppm, Wilson 95% confidence bounds, total and per-occurrence executed transitions and logical work, first-discovery work, retained bytes, and independently minimal witnesses by transition count and canonical retained-output bytes. Explorer failure examples track these stable digests, and all retained paths now exact-replay before insertion. |

### Conditional Work

Wait for a real product or toolchain requirement before adding:

- datagram campaigns, until Antfly has a production datagram consumer;
- server-side TLS fault campaigns, until httpx has a production server TLS
  implementation;
- source/basic-block guidance, until Zig exposes sufficiently stable
  instrumentation—semantic coverage remains replay truth, while compiler
  coverage only guides exploration;
- a graphical hosted debugger—the line-oriented debugger plus machine-readable
  and static reports is sufficient initially;
- the federated VOPR agent/broker protocol described above, until a
  separate-address-space or live mixed-version defect class justifies it; or
- guest-kernel RNG/syscall interception for arbitrary native dependencies,
  until the federated/native mode exists and source-audit fail-closed behavior
  is insufficient for a demonstrated defect class; or
- a deterministic hypervisor clone, which would duplicate the expensive part
  of Antithesis without improving Antfly's in-process `std.Io` strategy.

### Ongoing Roadmap

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
   Next add cancellation under storage faults, disk pressure, simultaneous
   process loss before cancellation drain, and other fault combinations, plus the
   remaining topology breadth, disk-capacity pressure, broader socket/short-write
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
   Add the managed-index progressive-publication/readiness lifecycle introduced
   by the current dense indexing work: candidate generation, coverage and
   replay watermarks, rate-limit recovery, alias/catalog publication, and
   crash/restart while public queries observe only the promised readiness
   state.
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
   Co-locate HA and the data plane, add disk-capacity pressure at a real
   reservation-consuming seam, broaden the v22 listener-pressure targets, and extend that
   algebra across storage, restart/process, serverless lease/object-store, and
   multi-owner faults.
4. **Broaden workloads at those same seams.** Add authenticated tenants,
   remote-content secret rotation at actual request use, remote generation and
   reranking adapters, production metadata-admin mutations, MCP/A2A
   orchestration, cloud-auth refresh/signing, bounded extension invocation, and
   client/background fairness.
5. **Operationalize the self-contained platform.** Run sharded nightly
   campaigns, deterministic corpus merges, retention/quarantine workflows,
   usage indexing, notifications, dashboards, and search-quality regression
   tracking.
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
   Next add broader restart, short-write, and socket-pressure
   targets/overlaps, remaining topology modes, disk-capacity pressure, and
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
   Next add broader restart/short-write/socket targets and overlaps,
   remaining topology modes, disk-capacity pressure, and storage/process/restart
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
   results. At the same public seam, add a managed-index
   generation history for atomic and progressive publication: backfill and
   dense replay must advance coherent target/published revisions and coverage,
   rate-limit recovery must not falsely regress readiness, a candidate must not
   leak through an active alias before its durable publication point, and a
   crash between generation durability, catalog publication, and readiness
   reporting must recover to one coherent generation. Existing DB/index race
   and generation-lifecycle suites prove useful primitives but do not yet prove
   this newly composed public readiness contract.
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
   Next overlap
   operation-specific slowdown
   with cross-domain ownership, public deadlines, and the existing
   link/storage/resource/restart faults. The real
   metadata/placement Raft wire hop, serverless public catalog path, memory-
   pressure recovery, and quiet cluster-wide suffix are complete at their
   named seams; broaden their fault combinations after item 1 instead of
   rebuilding them.
4. Extend full-cluster workload dimensions instead of multiplying suites:
   authenticated multi-tenant identities in addition to current two-table
   isolation, concurrent range split and replicated merge routing changes,
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

## Risks and Required Safeguards

- **Hidden host nondeterminism:** audit every scenario and fail closed on an
  unsupported `std.Io` capability.
- **Simulator drift:** run the same component on Threaded and VoprIo where
  possible and retain focused real-backend differential tests.
- **False cluster fidelity:** keep logical node identity, storage roots,
  lifecycle, resource ownership, and link direction explicit; do not label a
  shared-owner composition full-cluster coverage.
- **Coarse atomicity:** document atomic boundaries and add only production-safe
  suspension points.
- **Invalid histories:** use typed preconditions, fault budgets, minimum quorum
  policies, and explicit quiet suffixes.
- **Property defects:** test property aggregation independently and keep stable
  IDs separate from messages.
- **Artifact fragility:** pin scenario and runtime compatibility IDs and report
  the first enabled-set or observation mismatch.
- **Wrong-failure reduction:** require exact fingerprint equality and reject
  divergence or harness errors.
- **State explosion:** prioritize semantic novelty and rare property outcomes;
  keep search policy separate from replay meaning.
- **Resource cost:** use bounded histories, compact observations, ReleaseSafe
  campaigns, and full diagnostics only near novel or failing histories.

## Success Metrics

Track replay success, replay divergence, reduction ratio, unique fingerprints,
time to local reproduction, semantic states and transitions per CPU hour,
property reachability, meaningful overlapping-fault coverage, promoted
regressions, and modeled-versus-production differential agreement.

The practical success condition is simple: a developer receiving a chaos
failure can reproduce it with one command, inspect a short causal history, and
retain the reduced case as a permanent reviewed regression.

## Stable Decisions

1. Build an in-process deterministic virtual OS behind `std.Io`, not a
   general-purpose hypervisor.
2. Make a single selected transition the scheduler abstraction.
3. Record structured choices; seeds are discovery metadata only.
4. Treat replay from a clean world as truth and logical snapshots as an
   optimization.
5. Use non-fatal stable properties and deterministic recovery suffixes.
6. Preserve the same failure fingerprint during reduction.
7. Keep semantic coverage stable and compiler coverage secondary.
8. Keep `lib/vopr` Antfly independent and scenario policy under Antfly.
9. Use a harness-only CLI artifact; production code never imports the explorer.
10. Fail closed instead of falling back to host services.
11. Preserve existing HA, Raft, LSM, storage, integration, and formal tests as
    independent complementary gates.
12. Require human review before fixture promotion.
13. Evolve VOPR-native APIs, modes, trace revisions, saved-query formats, and
    properties forward-only. VOPR is new code, so do not add aliases, legacy
    readers, migrations, or fallback parsers; product-data compatibility stays
    in its separate explicit campaigns.

## Conclusion

Antfly already owns the hard self-contained foundation: explicit deterministic
choices, one-transition scheduling, virtual tasks, files, sockets, processes,
clocks and durability, guided exploration, exact replay, reduction, formal
export, counterfactual analysis, and independent production-shaped scenarios.

The P0, P1, and non-conditional P2 rows explicitly marked **integrated** are
implemented at the production seams stated in their conformance rows; rows
marked partial, ongoing, or conditional remain future work. Application-level,
in-process distributed VOPR is integrated across metadata, transactions, Raft,
HA, the data plane, distributed graph fanout, and a deployment-shaped
full-cluster campaign; separate-address-space orchestration is not. At the current checkpoint, a
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
combinations, remaining topology breadth, disk-capacity pressure, broader
socket-pressure targets/overlap, or broader
request/response/Raft short-write surfaces. Continue
by extending coverage through disjoint placement, bounded transfer, partitions,
and projection/DB/derived-state snapshot recovery; then by running public graph
requests under those replicated topology transitions, cancellation, authorization, and
hydration faults; distributed joins/global queries under real worker failure;
co-resident HA/data-plane owners; authenticated tenants; resource interference;
and eventually live mixed-version operation—without
requiring a new scheduler, virtual network, replay format, or container
hypervisor. The hard
Antithesis-class local tooling is integrated; the persistent local cross-run
index, filtered retroactive debug
pipeline, and multi-bug search-quality regression corpus are integrated. The
next operational work is to retain and merge nightly corpora, publish the
existing search-quality measurements, and extend scenarios when new
production consumers expose safe ownership boundaries.
Compiler-guided coverage and provider-specific datagram campaigns remain
conditional on stable instrumentation and real consumers. New product services
should cross the existing `std.Io`, lifecycle-hook, admission, and owner seams
rather than creating native-only loops, multiplying suite names, or weakening
production ownership contracts. If a future defect requires separate processes
or live mixed binaries, the next fidelity step is the versioned federated VOPR
agent/broker described above; unmodified container runs remain differential
evidence rather than exact deterministic replay.
