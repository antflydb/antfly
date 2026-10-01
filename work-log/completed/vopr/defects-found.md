# VOPR Defects Found

> Relocated verbatim from `zig/VOPR.md` (lines 1703–2864 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`VOPR.md`](../../../zig/VOPR.md). Durable decisions from this log were folded into that document before the move; a curated list of defect classes remains under the `## Defects Found` heading there.

## Defects Found

VOPR work has found concrete production and harness defects:

- The v53 managed-index composition found six executor/time ownership leaks
  that focused DB tests did not expose. Structural reconciliation scheduled on
  a source-owned `Threaded` executor while calling a catalog bound to `VoprIo`;
  production DataServer sources now borrow their `BackendRuntime` control I/O
  and do not construct that native fallback. Read-only configured-index opens
  also spawned raw `std.Thread` workers during owner reconstruction; they now
  use a borrowed `std.Io.Group`, with deterministic bounded fanout instead of
  host CPU-count discovery. Public index creation generated
  `_index_incarnation` through a hidden global executor; the mutation path now
  receives the request's `std.Io`. Enrichment lease and batch timestamps used
  the platform clock; an optional explicit test clock now otherwise defaults
  to the realtime clock paired with `BackendRuntime`. Foreground enrichment
  and derived-index deadlines use the paired monotonic clock, so realtime
  jumps cannot change timeout semantics. Finally, public runtime-status
  payloads exposed physical DB/index-open, LSM, flush, merge, and enrichment
  duration telemetry. Those durations differ across fresh worlds even when
  the logical schedule is identical. Status snapshots owned by a borrowed
  modeled runtime now stabilize only those diagnostic duration fields while
  preserving semantic timestamps, deadlines, readiness, and counters. These
  fixes keep request bytes, readiness status, retries, and cancellation in one
  replay domain.
- The same history found a production teardown ordering bug. DataServer could
  begin draining its DB while enrichment still owned background work, allowing
  a reconstructed managed-index owner to race destruction. EnrichmentRuntime
  and DB now publish shutdown in `beginTeardown` before the scheduler drains;
  destruction remains in the ordinary joined close path. The VOPR durable-job
  adapter also now launches ordinary `std.Io.Group` tasks, so repair jobs may
  sleep, perform I/O, and observe cancellation instead of pretending the whole
  job is one atomic simulator callback. Index-repair submission no longer
  retains a native mutex across that potentially suspending admission point.
- Exact replay of v53 found a harness capacity-fidelity defect: physical LSM
  paths include a unique process-local temporary namespace, and exact byte
  counts could differ by one byte when encoded path material changed an LSM
  run size. The modeled capacity source now observes allocated 4 KiB blocks,
  matching the resource quantity it claims to model while leaving physical
  backend bytes as an explicit differential boundary.
- The first deep v53 gate also exposed trace-retention amplification: every
  transition separately allocated repeated choice-site, transition, event,
  property, failure-identity, and observation names, pushing record-only memory above
  13 GiB before replay. `Trace` now interns record strings once per artifact
  while retaining identical records and canonical wire bytes. Further
  value/delta compaction remains optional efficiency work and may not weaken
  exact observation comparison.
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
  contract in ordinary antfly-root-test discovery.
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
- The v50 completion audit found that the design claimed publication-progress
  conflict fencing while the existing serverless history exercised only a
  stale enrichment generation. It never persisted a losing candidate between
  manifest durability and the guarded progress CAS, so the claim had no
  executable witness. The builder now exposes that exact optional lifecycle
  boundary; focused v5 and full-cluster v50 require candidate version 2,
  authoritative cutover version 3, exact `HeadChanged`, a lineage-tracked
  orphan, stale-derived rejection, and public recovery at version 4. The
  production CAS behaved correctly once exercised. This was a completion-claim
  and coverage-fidelity defect, not newly found production data corruption.
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
- The assembled runtime exposed an error-domain mismatch that single-archive
  VOPR tests could not detect: `FileStore` retained its creator's `std.Io`, but
  API-archive methods called that foreign vtable directly. An absent optional
  secret file's `FileNotFound` became `EndOfStream`, breaking status refresh and
  managed semantic queries with HTTP 400. Store refresh, lookup, listing, and
  mutations now execute in the creating archive through the existing stable
  callback error transport. `lib-common-secrets-abi-test` compiles a separate
  provider archive and verifies missing-file handling, typed errors, publication,
  rotation, retained values, deletion, layered precedence, malformed/missing
  replacement retention, and injected cancellation through every dispatched
  operation. It runs in `lib-common-secrets-test`
  and `antfly-unit-test`; production semantic E2E tests cover the assembled executable.
  Matching `std.Io` layouts/toolchains alone does not make error-returning
  vtable calls safe across independent Zig compilation units.
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
  therefore finalize without copying current writes. The initial envelope
  decoder was subsequently superseded by the committed-outcome snapshot and
  artifact transfer described in "Merge Outcome and Artifact Preservation"
  below: raw requests are not a safe materialized-effects log.
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

