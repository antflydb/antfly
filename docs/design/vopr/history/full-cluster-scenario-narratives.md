# VOPR Full-Cluster Scenario Narratives

> Relocated verbatim from `zig/VOPR.md` (lines 878–1148, 1579–1607, 1729–1808, and 1858–1890 at commit ada708f739) on 2026-10-12. Durable decisions were folded into [VOPR.md](../../../../zig/VOPR.md) before the move: each scenario version is now described once in its Full-Cluster Scenario Versions table, binding per-mode contracts are listed beneath that table, and the borrowed-clock invariants are under `VoprIo` Runtime.

## Per-version production-owner narrative, v11–v53 (originally lines 878–1148)

> Relocated verbatim from `zig/VOPR.md` (lines 878–1148 at commit ada708f739), starting at "Full-cluster v11 now reuses".

Full-cluster v11 now reuses those production owners behind the live
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
Forward-only v50 promotes serverless generation and publication-progress
fencing into the production-owner cluster. The builder exposes one optional,
production-neutral lifecycle hook after its immutable candidate manifest is
durable and before the guarded progress CAS. The hook is unset in production
and shared by every builder publication path. In the VOPR history, a stale
enrichment mutation captured at head 1 produces candidate version 2; at that
exact boundary an authoritative metadata publisher encounters the immutable
version collision, persists generation 3, and advances HEAD from 1 to 3. The
suspended publication path must then lose with `HeadChanged`, leaving version 2 as a
lineage-tracked orphan without changing visible progress. A clean retry from
head 3 consumes the stale WAL position without applying its full-body overwrite
and publishes version 4. The public serverless HTTP client must observe only the
authoritative document at version 4. Unlike the removed standalone full-cluster
mode, v50 starts the ordinary two-table public/DataServer/Raft workload before
the serverless history finishes, so both owner graphs make progress on the same
`VoprIo`. The exact property requires one hook call, candidate 2, cutover 3,
final head 4, one interrupted publication, generation-fenced data, successful
public writes/reads and Raft wire traffic, cleanup, and fresh-world replay. Its
dedicated 120,000-transition Debug and ReleaseSafe gates pass 15/15.
Forward-only v51 promotes authenticated multi-tenant isolation on the same
production owners. Two valid Basic identities have disjoint table-scoped
read/write permissions: the docs identity owns only `docs`, while the tenant
identity owns only `tenant_b_docs`. The ordinary three-ingress workload uses
those clients concurrently for its acknowledged writes and strong reads. It
then crosses different public nodes in both directions: each identity's read
and write against the other table must return exact 403, and an owning-identity
lookup must return exact 404 for each forbidden key. Response-body checks
prevent protected document values from leaking through either denial. The
property also retains the ordinary write/read/table-isolation, metadata
topology, real Raft-wire, serverless publication, quiet cleanup, and fresh-
world replay oracles. Its dedicated 120,000-transition Debug and ReleaseSafe
gates pass 15/15.
Forward-only v52 promotes one explicit per-node disk-capacity interference
history into the production-owner cluster. Node 2's modeled volume now has a
healthy 2 GiB baseline, above the production `ResourceManager`'s 1 GiB safety
floor. The mode attaches the real durable persistent object-range cache worker
to that live DataServer's capacity source, lowers only that source to zero,
and requires the accepted cache task to increase capacity denials without
creating a file, completing a write, or acquiring a reservation. A public
document read through the pressured node must remain available. The history
then restores the same volume, retries under a different cache key, requires
one exact capacity reservation/write/release cycle, reads the checksum-
protected payload back, and proves public data remains visible through another
coordinator. The registered node resource fault, ordinary two-table
DataServer/Raft workload, serverless publication, quiet cleanup, and fresh-
world replay oracles remain active. Its dedicated 120,000-transition Debug
and ReleaseSafe gates pass 15/15.
This promotes the real persistent-cache reservation seam, not managed-index
repair under disk pressure or disk overlap with link/storage/process faults.
Forward-only v53 begins one managed-index publication and reconstruction
seam through the same production owners. A public create-index request first
publishes a fresh incarnation through metadata Raft while the managed provider
admits its dimension probe but returns a retryable failure for document work.
attempts before the provider is released. The current 35,000-transition gate
exact-replays that bounded lifecycle and cleanup. The intended suffix requires
coherent coverage and replay watermarks, three indexed documents, durable node
1 reconstruction, and the same semantic query through all three public nodes.
That suffix is not promoted: its current deep run exposes packet-level replay
divergence around Raft status publication. Stabilize and complete that gate
before adding crashes inside each candidate/catalog/alias publication gap or
managed-index disk-pressure overlap.
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
V42-v53 directly add their modes, properties, trace revisions,
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

## Timer and deadline defect narrative (originally lines 1579–1607)

> Relocated verbatim from `zig/VOPR.md` (lines 1579–1607 at commit ada708f739).

The same CI gate runs the production Raft transport tests and determinism
audit. A bounded-fair standby/scaling history exposed a timer divergence: the HTTP
frame queue calculated retry deadlines and jitter from host time while sleeping
on borrowed I/O. Retry readiness and snapshot-transfer deadlines now use their
owning I/O's monotonic clock throughout. HTTP host construction propagates
that authority to route reconciliation, admission retries, policy rechecks,
and bootstrap status timestamps. Snapshot staging names also obtain
entropy from that I/O instead of host time and a process-global counter.
Virtual-clock regressions cover retry readiness, repeatable jitter, and a
transfer's remaining deadline across realtime clock changes.

Queued Raft delivery preserves the request's timeout when taking ownership of
its copied payload. The original failed history's suspended stacks showed a
node-status request waiting inside a queued Raft batch delivery; dropping that
timeout made the nested request unbounded when socket timeouts were disabled.
The transport gate exercises a real HTTP peer that never responds, verifies
expiry on virtual time, and delivers a subsequent request to prove the
serialized drain owner was released. Queued requests do not retain the enqueue
caller's borrowed cancellation or delivery-tracker pointers.

The DataServer maintenance worker also uses its borrowed monotonic clock for
vector-publication deadlines. An adversarial history advanced virtual time past
host uptime and exposed an outer-loop spin: readiness checked virtual time,
but publication kept its deadline in host time. Every bounded maintenance
attempt now establishes a retry boundary, including idle and failed attempts,
and releases its reservation before waiting. Accepted wakes survive that
backoff until the next eligible attempt. A far-future virtual-clock
regression verifies independent task progress, reservation release, renewed
deadlines, and worker cancellation; the determinism audit covers this worker.

## Production-owner gate tiers by version (originally lines 1729–1808)

> Relocated verbatim from `zig/VOPR.md` (lines 1729–1808 at commit ada708f739). Superseded by the gate column of the scenario-version table.

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
V50 suspends the real builder after immutable candidate persistence, advances
the authoritative object-backed progress generation, requires the losing CAS
to return `HeadChanged`, rejects the stale derived mutation on retry, and
proves version-4 public visibility while the ordinary production public,
DataServer, and Raft workload runs on the same `VoprIo`.
V51 enables authentication on every production data-plane listener, uses
disjoint docs and tenant identities for concurrent ordinary work, requires
four exact cross-table 403 responses, and proves both denied fixed-ID writes
remain absent through exact owning-identity 404 reads.
`production-cluster-vopr-test` requires every focused production-owner gate
through v51, including
`production-cluster-query-cache-deadline-restart-vopr-test` and
`production-cluster-serverless-fencing-vopr-test` plus
`production-cluster-authenticated-tenant-vopr-test`; each large subprocess uses ordinary
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

## v13–v18 completion recap (originally lines 1858–1890)

> Relocated verbatim from `zig/VOPR.md` (lines 1858–1890 at commit ada708f739). Superseded: it says cancellation under resource faults is not yet composed into the production-owner history, but v33 and v34 compose join cancellation and retry exhaustion under overlapping resource and link faults.

That does not make the roadmap empty or make the system equivalent to the
Antithesis hypervisor. Items marked **ongoing** or **conditional**, and residual
boundaries explicitly named in an integrated row, are not finished. In
particular, one trace does not yet co-reside every standby/data-plane/serverless
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
topology, managed-index and graph/query disk-pressure combinations, broader
socket/partial-write, and fault-overlap breadth is not;
live mixed-
binary operation is not modeled; and arbitrary unmodified sidecars or process
address spaces remain differential/integration concerns.
