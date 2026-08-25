# VOPR: Deterministic Autonomous Testing for Antfly

Status (2026-08-25): the common VOPR engine and deterministic `std.Io` runtime
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
the recorded actor/time schedule. Clock-only stutter is
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
That full-cluster mode does not yet route the transition through production
`DataServer` owners or replicate its structural actions through every receiver
replica. The separate focused multi-owner composition closes that production
action/proposal/apply, merge-to-split composition, ordinary-write delta replay,
every-replica convergence, failover, and restart seam. Integrating it into the
full-cluster graph/serverless history, replacing its unavailable metadata stub
with the real metadata quorum for active transition forwarding, adding
disjoint placement, derived-state equality, bounded retained history, and
snapshot-install rehydration are the remaining gaps tracked below.
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

Verification audit (2026-08-25): full-cluster v9 passes all nine recorded
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
regression, passes at this checkpoint. It is a focused production cluster, not yet
the replacement for the hosted rig inside `full-cluster-vopr-test`. The
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
starts with substituting the proven focused `DataServer` owners and data-Raft
apply path for the hosted public-source rig, then adds deeper fault overlap,
joins/global queries, and workload breadth. This is an execution-fidelity gap,
not a missing deterministic-distributed foundation.

## Conformance Status

Completion claims use three levels:

- **Integrated** means a production or reusable path is exercised, exact replay
  is verified, and the focused gate is part of `vopr-test`.
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
| Reusable VOPR engine, `VoprIo`, replay, reduction, properties, event queries, flight recording, local reports, debug recipes, fault algebra, and search-quality fixtures | Implemented and exercised by the focused engine/meta gates named below | Nightly sharding, retention, review, notifications, and richer cross-run event-set algebra are operational or ongoing work |
| Metadata, Raft, HA, transaction, data-plane, storage, backfill, supervision, authentication, serverless, cache, provider, generation/reranking, and query suites | Implemented at each row's named production seam and fault vocabulary | The suites are not all co-resident in one deployment history |
| Distributed graph | Focused production coordinator paths plus the public hosted-source composition are implemented | Public split churn, public cancellation/auth/hydration composition, joins, and global query are not complete |
| Full-cluster v9 | The documented metadata/placement Raft, hosted data roots, public/serverless HTTP, graph-fanout, resource, merge-coordinator, replay, and cleanup behaviors are implemented | It is not yet a cluster of production `DataServer` owners. Data writes and merge structural actions are not proven through replicated DataServer apply on every replica |
| Replicated data-Raft merge/split protocols | Integrated focused multi-owner seam: merge v3 capability/barrier activation, source fencing, receiver checkpoints, catalog-independent replay identity, copied-document proposals, snapshot-carried controls, and replicated observation are implemented. Three production `DataServer` owners chain that merge into split prepare/bootstrap, a post-bootstrap public HTTP write, delta catch-up, cutover, restart, exact routed terminal retry, and every-replica range/document/transition/watermark convergence under fresh-state schedule replay on one `VoprIo` | This focused cluster is not yet substituted into `full-cluster-vopr-test`. Active split commands execute on the current production group leader because the fixture intentionally has no live metadata service; networked transition forwarding against the real metadata quorum, disjoint donor/receiver replica sets, bounded retained-history paging under pressure, derived graph/index equivalence, explicit snapshot-install rehydration of every live DB owner, and co-resident HA/data-plane/serverless faults remain unproven |
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
| Modeled files, durability, persistent sector corruption, torn synchronization, streams, datagrams, processes, and quotas | `vopr_io_file.zig`, `vopr_io_net.zig`, `vopr_io_process.zig` | `zig build vopr-engine-test` |
| Stable optional safepoints | `vopr_io_instrumentation.zig` | `zig build vopr-engine-test` |
| Clocks, timers, storage completions, and lifecycle faults | `time.zig`, `clock_fault.zig`, `fault.zig`, storage `sim_runtime.zig` | `zig build vopr-engine-test storage-vopr-runtime-test` |
| Properties, observations, semantic coverage, cross-revision corpus quarantine, property history, and guided search | `property.zig`, `observation.zig`, `coverage.zig`, `corpus.zig`, `explorer.zig` | `zig build vopr-engine-test vopr-benchmark` |
| Integrated retroactive flight recording and fielded temporal event queries | `flight_recorder.zig`, `event_query.zig`, `debug_recipe.zig`; bounded recordings own structured fields and verbose text outside canonical bytes, support conjunctive field/text filters and before/after windows, and are populated directly by runner-backed and custom metadata/domain replay paths. Every retained/failing campaign writes `.flight.json`, while every debug recipe packages a filtered reduced-replay window | `zig build vopr-engine-test vopr-meta-test vopr-events vopr-recipe` |
| Integrated per-history and aggregate run/results API with phased health evidence | `runner.zig`, `report.zig`, `health.zig`, `vopr_io.zig`, `vopr-results`; every runner history samples continuous/recovery/final health without changing canonical trace bytes, exact replay rematerializes the evidence, `VoprIo.healthSnapshot` supplies task/descriptor/storage data, and mature P0/P1 adapters add domain progress, recovery, consistency, allocator/crash classification, and cleanup | `zig build vopr-engine-test vopr-contract-test vopr-registry-test vopr-results` |
| Integrated persistent local run/results index and usage query API | `run_index.zig`, `vopr-index`; atomically persisted `vopr-run-index-v1` projects per-history and aggregate results into canonical run, revision, property, fingerprint, corpus/quarantine, artifact, and budget records. CLI predicates and `vopr-run-index-query-v1` cover every dimension, and the same query renders a static local HTML summary | `zig build vopr-engine-test vopr-meta-test vopr-index` |
| Automatic debug recipes and deterministic corpus merging | `debug_recipe.zig`, callback-based `reducer.zig`, `corpus.zig`; `vopr-recipe`, `vopr-corpus-merge` | `zig build vopr-engine-test vopr-meta-test` |
| Integrated fault composition, structured-choice auditing, and search-quality regression corpus | `fault.zig`, `fault_vopr_io.zig`, `choice.zig`, `explorer.zig`, `benchmark.zig`; precedence drives real `VoprIo` effects in the Parquet-cache suite. Three distinct scheduling, durability, and cancellation defects run under random, guided, spliced, starvation, and checkpoint-assisted policies with replay-before-retention, recurrence, Wilson confidence, logical-cost, and minimal-output evidence | `zig build vopr-engine-test parquet-cache-vopr-test vopr-benchmark` |
| Integrated command-template composition and fail-closed entropy/source audit | `command.zig` implements first/parallel/serial/singleton/anytime/eventually/finally roles with compatibility, exclusion, fault, and quiescence policies; `determinism.zig` combines immediate-choice and borrowed-I/O entropy evidence; Antfly `vopr/determinism_audit.zig` covers every exported VOPR source and both legacy metadata replay regions | `zig build vopr-engine-test vopr-determinism-audit` |
| Registered deployment topology and quiet suffix | `lib/vopr/src/deployment.zig` validates role dependencies, node/instance identity, directional links, disjoint process/storage/resource domains, typed fault compatibility, readiness, measured resource policy, and per-node quiet acknowledgments. Full-cluster v9 registers four owners, seven role instances, six directional links, and every scenario fault before requiring cluster-wide quiescence | `zig build vopr-engine-test full-cluster-vopr-test` |
| Replay-before-retention campaigns, deterministic workers, bounded counterfactual graphs, and quarantine manifests/raw artifacts | Antfly `vopr/cli.zig` | `zig build vopr-meta-test` |
| Same-fingerprint reduction, promotion, and migration | `reducer.zig`, `fixture.zig`, `vopr-reduce`, `vopr-promote`, `vopr-migrate` | `zig build vopr-engine-test vopr-meta-test` |
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
| Query embedding cache | Antfly `vopr/query_embedding_cache.zig`; production cache miss coalescing, cancellation, deadline, admission, TTL, LRU, pinned eviction, and cleanup on one `VoprIo` | `zig build query-embedding-cache-vopr-test` |
| Generation and reranking chains | Antfly `vopr/generation_reranking.zig`; production generation fallback/retry with borrowed `std.Io`, provider errors and cancellation, plus local reranker response validation | `zig build generation-reranking-vopr-test` |
| Distributed graph-query execution | Antfly `vopr/distributed_query.zig`; production `executeCrossRange` planning, two-shard fanout, optional hydration, bounded topology retry, retry exhaustion, stale snapshot rejection, in-flight cancellation, and cross-table authorization. This row does not claim distributed-join coverage | `zig build distributed-query-vopr-test` |
| Deployment-shaped full cluster | Antfly `vopr/full_cluster.zig`, `vopr/serverless_workflow.zig`, `serverless_http_server.zig`, `metadata/sim_harness.zig`, and Raft `transport/httpx_runtime.zig`; one `VoprIo` owns a metadata quorum, two-placement hosted ranges on node-local data roots, two isolated tables, three production public API HTTP listeners backed by hosted table sources, a real serverless catalog listener, four concurrent cross-node clients, distinct per-node production resource managers, and a co-scheduled serverless worker. Serialized metadata/placement Raft frames and all public requests cross real `httpx`/VOPR sockets. Nine modes—clean, partition/heal, non-host restart, in-flight graph-leader restart, in-flight graph range-merge churn, in-flight graph transport failure/recovery, partial-write, stale-serverless-generation, and node-memory denial/recovery—exact replay. Full-cluster v9 mirrors infrastructure faults into the registered deployment manifest, treats the merge as an operator workload, requires post-heal resource evidence and quiet acknowledgment from all four nodes, lists and queries the worker's public table, and executes a public depth-two graph traversal across two ranges through production fanout. The transport mode cuts the internal graph-fanout fabric after expansion round one. The topology mode borrows the actual donor/receiver leader writers on different node roots, runs the production `MergeCoordinator`, rejects stale partial publication as a structured retryable 503, finalizes the merge, and requires the recovered route to return the complete graph. It does not instantiate production `DataServer` owners or prove data-Raft replication of the merge; those are P0 completion work. HA/data-plane services also remain separate suites | `zig build full-cluster-vopr-test` |
| Parquet cache, provisioning/startup, external lake, and media providers | Antfly `vopr/parquet_cache.zig`, `provisioning_startup.zig`, `external_lake.zig`, `media_runtime.zig`; borrowed `VoprIo`, real cache/reconcile/Iceberg-manifest/Parquet-query/provider-HTTP paths, injected I/O and object-store faults, provider retry/timeout/cancellation and active-request drain, cleanup, and exact replay | `zig build parquet-cache-vopr-test provisioning-startup-vopr-test external-lake-vopr-test media-runtime-vopr-test` |
| Upgrade and compatibility campaign | Antfly `vopr/upgrade_compatibility.zig`; current production readers open v1 HA golden records, v12 manifests, v14 external inventories, legacy serverless heads, and v1 VOPR traces; fixture migration requires source and target exact replay plus semantic equivalence; incompatible traces, checkpoints, data directories, and future serverless artifacts fail closed; atomic data-directory publication recovers after a crash-before-rename | `zig build upgrade-compatibility-vopr-test` |

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
- [Fault types and node scope](https://antithesis.com/docs/product/fault_injection/fault_types/)
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
| Node lifecycle and pressure | Pause, stop/kill, restart, and throttling | Integrated pause, crash/restart, CPU work, descriptor, socket, allocator, and storage limits |
| Deterministic replay and branching | Deterministic hypervisor execution | Exact choice/transition/observation replay plus reduction and multiverse branching |
| Whole unmodified deployment | Arbitrary containerized binaries and sidecars | Deliberate non-goal; only registered in-process entrypoints are deterministic |
| One full Antfly deployment history | Runs a supplied Docker Compose or Kubernetes topology | Partially integrated in-process: `full-cluster-vopr-test` joins metadata/placement Raft, hosted node-local data roots, public clients, real HTTP/Raft wire paths, per-node resources, a cross-root production-coordinator merge, and a co-scheduled serverless worker whose object catalog is served and queried through the production public API. Production DataServer/data-Raft owners, co-resident HA/data-plane owners, fully replicated transition execution, and cross-domain fault overlap remain ongoing; native sidecars, DNS, kernels, and mixed binaries remain differential concerns |

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
engine gate. The old `sim-*` CLI, contract-test, and aggregate steps remain
temporary workflow aliases. Older randomized real-I/O suites use
`workload`/`integration` terminology when renamed; they do not become VOPR
suites merely by changing a label. Serialized `sim-io-*` backend identifiers
remain unchanged because they are part of the `vopr-trace-v1` replay ABI, not
source names.

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

### Implemented Files

Virtual integer handles provide directories, deterministic iteration, recursive
rename, positional and streaming I/O, atomic publication, locks, mappings,
metadata, descriptor and capacity limits, partial I/O, data sync, separate
namespace sync, dropped sync, precise one-shot read-range corruption, and crash
reconstruction from durable state.

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

Promotion is explicit. Migration must replay the source, translate it, replay
the target, and verify semantic outcome equivalence before writing a new
canonical artifact. Readers continue to accept `.simtrace` as a legacy filename
extension; the content format and serialized `sim-io-*` identities remain the
versioned replay ABI.

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
`data-server-vopr-test` now composes three production `DataServer` owners, two
three-replica groups, real public HTTP and Raft listeners, routed forwarding,
leader transfer, owner restart, every-replica transition/watermark convergence,
document equality, exact terminal retry, and actor-owned teardown on one
`VoprIo`. This closes the focused multi-owner merge/failover gap without
claiming that the hosted full-cluster history has already been replaced.
Transport partition campaigns, disjoint placement, split execution,
derived-state equality, bounded retained-history replay, and snapshot-install
rehydration remain the work below.

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
- The independent-domain checkpoint exposed a latent compile defect caused by
  a local durable-job-lane variable shadowing the `lane` method.
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
- Media-runtime startup published provider globals as each provider loaded.
  When a later provider failed to initialize, the earlier thread-local global
  could outlive its rolled-back allocation. Startup now loads every provider
  before publishing any global, and the media campaign preserves this partial-
  startup rollback contract.
- `HttpRuntime` unconditionally created three hidden Threaded executors and a
  native descriptor observer. It now supports caller-owned backend-neutral
  lanes, preserves bounded admission, and fails closed when native disconnect
  observation is requested from a backend that cannot provide it.
- Borrowed HTTP runtimes could not observe a peer reset because the native
  descriptor observer cannot inspect virtual socket handles; DataServer
  consequently disabled hard-disconnect cancellation under deterministic I/O.
  httpx now accepts a backend-neutral reset probe, and `VoprIo` distinguishes
  reset from ordered FIN even when unread pipelined bytes remain.
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

# Replay-proven format migration.
zig build vopr-migrate -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --out /tmp/metadata-migrated.voprtrace

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
- `vopr-engine-test` runs only the reusable `lib/vopr` contract; `sim-test` is
  a temporary compatibility alias for this aggregate.

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
- Obsolete fixtures change only through explicit replay-proven migration.
- Fixed regressions do not depend on corpus scheduling or search heuristics.

## Roadmap

Calling these VOPR tests, the remaining opportunities are targeted integration
scenarios and operational tooling rather than missing foundational
infrastructure. They are not a second numbered phase plan and are not
dependencies of the already implemented domain suites.

### Verification Audit and Meaning of "Finished"

The 2026-08-25 design audit forced every exported scenario module into test
discovery and gave each integrated row an executing focused test, exact replay,
and an aggregate dependency. The current distributed checkpoint directly
reran the Raft transport, determinism, serverless-workflow, and full-cluster
gates successfully. The complete aggregate was attempted but is not a clean
verification result for this checkout, as recorded in the status above.

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
event-query layer is implemented for current filters/temporal predicates while
the richer cross-run set algebra remains future work.

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
owner; the public graph composition now covers an in-flight leader restart and
a production-coordinator range merge across different leader roots, but does
not yet compose a range split, multi-replica transition execution,
cancellation, authorization changes, or public hydration. Its fail-closed
topology/transport interruption and post-recovery complete retry are
integrated; live mixed-
binary operation is not modeled; and arbitrary unmodified sidecars or process
address spaces remain differential/integration concerns.

### Distributed Completion Audit

This audit prevents a focused seam from being mistaken for a finished whole-
deployment campaign. It also answers the Antithesis comparison directly:
Antithesis runs distributed Docker Compose or
[Kubernetes](https://antithesis.com/docs/setup/kubernetes/) topologies and
[scopes faults to containers or
pods](https://antithesis.com/docs/product/fault_injection/fault_types/); VOPR
implements the analogous application-level fault domains inside a registered
`std.Io` world.

| Requirement | Current status | Remaining work |
| --- | --- | --- |
| Deterministic multi-node runtime, clocks, links, storage, restart, resources, replay, and quiet suffix | **Integrated foundation.** The reusable deployment composer registers node/role/domain/fault/quiet obligations; metadata, Raft, HA, transaction, data-plane, and full-cluster gates exercise complementary real owners | Adopt the manifest in the remaining distributed suites and maintain fail-closed audits as new owners appear |
| Metadata quorum, production `DataServer` replicas, public clients, and real HTTP/Raft transport in one history | **Partially integrated across two complementary histories.** Full-cluster v9 sends serialized metadata/placement Raft frames through deterministic link policy and real `httpx`/`VoprIo` sockets, drives three production public API listeners over hosted node-local roots plus the serverless catalog listener, injects three distinct production resource managers, exact-replays aggregate node-memory denial/recovery, public graph requests across an in-flight range-leader restart and range merge, and a real fanout transport failure with fail-closed 503/recovery, and proves registered cluster quiescence. It does not instantiate `DataServer` or its data-Raft apply owner. Separately, `data-server-transition-vopr-test` runs three production `DataServer` owners and three replicated groups over time, chaining merge into split with real public HTTP/Raft listeners, leader transfer, a post-bootstrap public write, replicated delta catch-up/finalize, owner restart, routed terminal retry, document/range equality, and every-replica transition/watermark convergence on one `VoprIo` | Substitute the proven production owners into the full-cluster history so active split actions forward against the real metadata quorum; add disjoint donor/receiver placement, bounded retained-history paging, snapshot-install DB/derived-state rehydration, derived graph/index equality, partitions, disk/socket pressure, and richer overlapping failures |
| Serverless worker output through its production public catalog and ownership graph | **Integrated at the stated seam.** The production worker, durable lease, object stores, catalog service, HTTP handler/listener, and public client share one `VoprIo`. Every mode lists the worker-created table and queries the published head/documents; stale generation remains fenced. This correctly retains the distinct serverless object and metadata placement catalogs | Overlap serverless lease/object-store failures with metadata topology and node-resource faults, then add multi-worker placement when production owns that topology |
| HA, data-plane, metadata, public API, and serverless owners all co-resident | **Ongoing.** Each domain has an integrated exact-replay suite; they do not yet all coexist in one history | Build one bounded deployment composition and cluster-wide recovery oracle without duplicating business logic |
| Public distributed graph request from HTTP planning through fanout/hydration | **Partially integrated.** Full-cluster v9 executes a public depth-two graph traversal across two ranges through real HTTP, planning, internal expansion fanout, and canonical response assembly. Production-neutral lifecycle events pause after the first consistent round. One mode restarts the actual second-range leader and requires the resumed request's complete result; another cuts the real internal transport and requires a typed retryable 503 before a complete retry; the ninth runs a production `MergeCoordinator` across the actual donor/receiver leader roots, requires topology churn to fail closed, finalizes the merge, and requires the recovered route's complete graph. The focused distributed suite separately covers optional hydration, topology retry/exhaustion, cancellation, stale generations, and authorization; the production DataServer seam now proves replicated split independently | Substitute that DataServer seam and add public range-split churn, then compose cancellation, authorization changes, and public hydration; add distributed joins and global queries with the same fail-closed publication rule |
| Distributed joins and global-query orchestration | **Ongoing.** Focused composed-query tests cover result assembly, but there is no claim that a public multi-node join/global-query request has been faulted end to end | Add planning, worker fanout, cancellation, retry exhaustion, stale topology/generation, partial-worker failure, and fail-closed public response/recovery histories |
| Query-embedding cache | **Integrated focused seam.** Coalescing, cancellation, deadlines, admission, TTL, byte/LRU/pinned eviction, and cleanup exact replay | Compose node-local cache pressure with public full-cluster requests |
| Generation/reranking provider replacement and fallback | **Integrated local/focused seam.** Production chain and validation paths exact replay | Add remote HTTP provider replacement, malformed/truncated response, and local/remote routing in one trace |
| Multi-table/tenant/resource/mixed-version breadth | **Partial/conditional.** Two-table cross-node isolation, in-cluster node-memory interference, and separate disk/socket pressure and upgrade-artifact suites are integrated | Add authenticated tenants plus in-cluster disk/socket interference; live mixed-version nodes remain conditional on runnable compatible binaries |

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
cancels an active handler even with unread pipelined input. Direct server TLS
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
and every interrupted attempt recovers.

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
  same-actor/resource correlation. `vopr-events` runs a query over a clean
  exact replay.
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
  it outside the algebra unit tests. The runner audits every choice record for typed, immediate
  scenario-level selection instead of delayed seed interpretation.
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
| P0 integrated | Replication backfill and rebalancing | `replication-backfill-vopr-test` covers snapshot-to-streaming cutover, resumable checkpoints, duplicate work, cancellation, source and target crashes, topology changes, stale ownership, schema changes, and exact replay through the production runners. |
| P0 integrated | Standalone and serverless supervision | `supervision-vopr-test` covers partial-startup rollback, readiness publication, child-service failure, coordinated shutdown, virtual watchdog expiry, and restart through the production supervisor. The serverless manager now owns a borrowed-`std.Io` Future instead of a native run-loop thread. |
| P0 integrated | User and authentication lifecycle | `auth-lifecycle-vopr-test` covers password, API-key, permission, and row-filter changes; deterministic seed capture; revoke and rotate; durable reload; partial persistence rollback; and stale-reader behavior through the production manager. |
| P1 integrated | Complete serverless workflow | `serverless-workflow-vopr-test` covers durable claim/fencing, build, compaction, publication, and query-visible catalog cutover with duplicate workers, lease takeover, ambiguous completion, retry, cancellation, crash recovery, stale-enricher generation rejection, and progress-conflict fencing under exact replay through production orchestration. In full-cluster v9 the same fixture borrows the cluster `VoprIo`, serves its worker-owned object catalog through the production serverless HTTP stack, and proves public table/head/document visibility. Cross-domain fault overlap and multi-worker placement remain follow-up depth. |
| P1 integrated | DB and index request races | `db-index-race-vopr-test` exact-replays cross-index admission, same-index FIFO fairness, delete/materialize linearizations, published-reader/catalog-writer capture, cancellation, shutdown, and cleanup through production-safe seams rather than native test threads. |
| P0 partially integrated | Full-cluster distributed composition | `full-cluster-vopr-test` exact-replays a three-node metadata quorum, two-placement hosted ranges on node-local roots, three production public API HTTP listeners plus a production serverless catalog listener, non-host and cross-node clients, two tables, three distinct production resource managers, and a serverless workflow fixture on one `VoprIo`. Serialized metadata/placement Raft frames and public requests pass through real `httpx`/VOPR sockets. Nine modes cover clean operation, metadata partition/heal, non-host restart, in-flight graph range-leader restart, in-flight graph range-merge churn, in-flight graph transport failure/recovery, partial HTTP write, stale serverless generation, and node-memory denial/recovery. The restart mode pauses the public graph after expansion round one, restarts the actual second-range leader, waits for recovery, resumes the request, and requires the complete depth-two response. The transport mode cuts real internal fanout after round one, requires the structured retryable 503 with no graph payload, heals the network, and requires a complete retry. The topology mode uses retained hosted-writer leases and the production `MergeCoordinator` across actual donor/receiver leader roots; it requires a fail-closed 503 during churn, merge finalization, and a complete graph from the recovered route. The pressure mode fills every production process envelope through exactly-once reservations, requires a public write denial, releases pressure, then requires public write/read recovery. Version 9 registers the in-process owners, role dependencies, directional links, and typed fault scopes with the reusable deployment composer, requires measured resource evidence and cluster-wide quiet acknowledgment, and queries the worker's published version/documents through `ServerlessHttpClient`. The production data-Raft merge seam implements v3 fail-closed activation, source fencing, receiver checkpoints, ordinary-Raft document transfer, snapshot-carried markers, replicated observation, and three-store convergence. Focused DataServer histories cover rollback/fresh retry through one owner and now chain merge into split across three production owners and replicated groups, including leader changes, a post-bootstrap public write, replicated delta catch-up/finalize, owner restart, exact routed terminal retry, document/range equality, and every-replica convergence. These owners are not yet substituted into the full-cluster history. Remaining P0 work is that substitution and active transition forwarding against the real metadata quorum. Disjoint donor/receiver placement, bounded retained-history paging, DB/derived-state snapshot rehydration, derived graph/index equality, partitions, HA/data-plane co-residency, disk/socket pressure, richer overlapping cross-domain faults, and multi-worker placement remain follow-up depth. |
| P0 integrated | Query-embedding cache | `query-embedding-cache-vopr-test` exact-replays concurrent-miss coalescing, waiter cancellation, deadlines, in-flight admission, TTL, byte-budget/LRU eviction, pinned hits, and cleanup through the production cache on one `VoprIo`. |
| P1 partially integrated | Distributed graph/public-query boundaries | `distributed-query-vopr-test` exact-replays production cross-range planning, two-shard fanout with and without document hydration, topology change between plan and fanout, one-retry success, retry exhaustion, stale per-shard generation rejection, cancellation with outstanding shard work, and cross-table authorization. Full-cluster v9 joins public HTTP to real two-range graph fanout and depth-two assembly, exact-replays an in-flight restart of the actual second-range leader, proves that real transport loss after round one returns a typed retryable 503 without partial graph data before a complete post-heal retry, and runs a cross-node production-coordinator merge with fail-closed topology exhaustion and complete recovery. Public range-split churn, replicated transition execution, cancellation, authorization changes, and public hydration are not yet composed into that history; distributed joins and global queries also remain and are not claimed by this row. |
| P1 integrated | Generation and reranking chains | `generation-reranking-vopr-test` exact-replays generation success, retry/backoff on borrowed `std.Io`, timeout and rate-limit fallback, cancellation, reranking success, malformed count/non-finite results, timeout, and cancellation through production chain and local-provider boundaries. Remote HTTP provider parsing remains covered by `provider-boundary-vopr-test` and `media-runtime-vopr-test`, not duplicated here. |
| P1 ongoing | Remote-content credential use boundary | Join the integrated live-reference configuration/store contract to a real scraping or object-fetch request. Resolve access key, secret, session token, and header references immediately before provider use; rotate while an old request is in flight; retry and cancel through borrowed `std.Io`; and prove that snapshots retain references while each new request observes one coherent secret generation. The current config lifecycle proves publication and the production resolver independently, while `lib/scraping` still copies credential strings at its lower request-construction seam. |
| P2 integrated | Multi-table and cross-node workload dimensions | The full-cluster history provisions two independently replicated tables and drives four concurrent clients through three public nodes. A tenant sentinel must remain visible in its table and absent from the other table while both share the same scheduler, HTTP transport, sockets, and node resources. This proves table isolation and routing interference, not authenticated tenant identity. |
| P2 partial | Resource-interference workload dimension | `admission-vopr-test` proves cross-service memory, disk, task, file, socket, and cancellation ownership. The current full-cluster campaign composes explicit node-memory denial/recovery with real public clients, production resource managers, background database owners, HTTP routing, and cleanup. Carry disk and socket denial/recovery into the same deployment and overlap pressure with link/storage/restart faults. |
| P2 conditional | Live mixed-version workload dimension | Add rolling old/new binary operation only when two compatible runnable versions and an upgrade contract exist. `upgrade-compatibility-vopr-test` currently proves artifact readers, migration, safe rejection, and crash recovery; it is not live mixed-version cluster coverage. |
| P2 integrated | Provider boundaries | `provider-boundary-vopr-test` uses the real ManagedEmbedder and PostgreSQL Source boundaries for timeout, partial response, cancellation, retry, malformed data, SQL construction, and admission ownership. Actual models, GPU kernels, and libpq internals remain differential/integration concerns. |
| P2 integrated | Composed query lifecycle | `composed-query-vopr-test` exact-replays vector, text, graph, and global-query completion under partial failure/retry, cancellation, resource pressure, and every result-assembly ordering through production merge and graph-union code. |
| P1 integrated | Persistent Parquet cache | `parquet-cache-vopr-test` runs the real borrowed-I/O worker, bounded queue, duplicate-write coalescing, read/write faults, durable sync, crash, reopen, and checksum-protected reads. It is the first production consumer of the reusable fault-to-`VoprIo` adapter. |
| P1 integrated | Provisioning and startup | `provisioning-startup-vopr-test` runs real format admission and replica-root reconciliation through a manual `BackendRuntime` borrowing `VoprIo`, including repeat startup, partial markers, legacy-store rejection, failed atomic-write retry, and crash/restart. |
| P1 integrated | External lake | `external-lake-vopr-test` retains the focused range/cache histories and composes catalog binding, object-backed Iceberg metadata discovery, production Avro manifest decoding, schema evolution, pinned inventory, Parquet footer metadata, row-group cache, and query assembly. Twelve exact-replayed modes cover cache reuse, short responses, timeout/admission, stale object versions, deletion, ambiguous completed downloads with retry, bounded eviction, and durable persistent-cache crash/reopen without an object re-download. |
| P2 integrated | Media-provider execution and runtime | `media-runtime-vopr-test` exact-replays production Antfly STT and OpenAI-compatible TTS HTTP success, malformed JSON, truncated bodies, logical timeout, POST retry, partial-startup rollback, nested and in-flight runtime replacement, and shutdown cancellation/drain on borrowed `VoprIo`. `httpx.Client` closes admission and drains committed requests before shared provider state is destroyed. Real codecs, models, and GPU execution remain differential/integration concerns. |
| P2 integrated | Upgrade and compatibility campaigns | `upgrade-compatibility-vopr-test` exact-replays eighteen histories covering v1 HA golden replication/checkpoint/backup bytes, legacy and future data-directory admission, crash-before-rename recovery, v1 trace roundtrip and incompatible format/scenario versions, replay-proven v1-to-v2 fixture migration and semantic-change rejection, logical checkpoint restore/version/corruption, and legacy/future serverless head, v14 inventory, and v12 manifest artifacts. Outcomes are explicit forward completion, rollback/retry, or safe rejection. |

Replication backfill and the standalone/serverless supervisor were the first
targets because they have the richest combinations of durable state,
ownership, concurrency, and recovery; both are now integrated. The complete
serverless workflow and DB/index request-race compositions are also
integrated, as are the P2 provider, query, media, and compatibility suites.
The full-cluster, query-cache, distributed-query, and generation/reranking
campaigns are now integrated. Future test work is targeted composition,
workload dimensions, and newly exposed safe suspension points rather than
missing scheduler or replay foundations.

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
   and diagnostic before/after windows;
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
   faults](https://antithesis.com/docs/environment/fault_injection/);
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
    domains, measured node policy, and cluster-wide quiet suffixes.

These capabilities are local libraries, commands, reports, and CI gates. They
do not require an Antithesis account or hosted runtime. This is not a claim of
full Antithesis product parity: the implemented claim is limited to the named
self-contained engine features and application-level distributed fault domains,
while separate-address-space determinism and the operational work below remain
explicitly unfinished or conditional.

The remaining Antithesis-class opportunities are narrower than the engine
work already completed:

- extend the current conjunctive/temporal event queries into a reusable event-
  set algebra for joins, unions, differences, quantified sequences, and saved
  queries across runs;
- make nightly campaign sharding, corpus merge, retention, quarantine review,
  usage indexing, and notifications a repository-owned operational workflow;
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

Deterministic distributed testing itself is therefore not an unported
Antithesis engine feature: VOPR already supplies registered
node/process/resource/link domains. Deterministic execution of arbitrary
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
| P0 integrated | Complete entropy interception and audit for replayable in-process code | `lib/vopr/determinism.zig` admits only immediate structured choices and borrowed-`std.Io` entropy as runtime evidence. `vopr-determinism-audit` fail-closes on host RNG, delayed private PRNGs, host clocks, native threads/I/O, filesystem escapes, native libraries, unordered iteration, and pointer-derived identity in replayable adapters; reviewed differential boundaries require line-local categorized rationale. Its manifest is checked against every exported VOPR source and includes both legacy metadata replay regions. This is complete at the registered `std.Io` boundary, not guest-kernel RNG interception for an arbitrary unmodified C library or separate process. |
| P1 integrated | Continuous and quiescent validation phases | The runner automatically samples every history in continuous, recovery/quiescent, and final phases, aggregates bounded no-progress and recovery evidence, classifies allocator and unexpected process/panic failures, and retains the pointer-free diagnostic outside canonical replay bytes. `VoprIo.healthSnapshot` automatically populates task, descriptor, and optional physical-storage evidence; the replication, supervision, auth, serverless-workflow, DB/index, provider, composed-query, resource-pressure, cache, startup, generation, configuration, Embedded/Lite, external-lake, media, and upgrade/compatibility suites add domain progress, consistency, exhaustion, and cleanup semantics. `vopr-results` uses exact-replayed evidence automatically. |
| P1 integrated | Richer retroactive logging | `event.Event` and the bounded `flight_recorder` own diagnostic name/value fields and text independently of canonical replay fields. Filters combine event identity, kind, actor/resource, logical index, exact or substring field predicates, and text search; materialization adds bounded before/after context. Generic, domain, distributed-data, and custom metadata replays feed the recorder directly, and every automatic debug recipe exact-replays its reduced artifact into a configurable flight window. `vopr-recipe` exposes filter, window, capacity, and limit controls. |
| P1 integrated | Local run index and usage API | `lib/vopr/run_index.zig` transactionally ingests per-history, aggregate, legacy aggregate, and existing index JSON; validates referential integrity; deduplicates stable run/history keys; and canonically indexes source revision, properties, fingerprints, corpus/quarantine counts, typed artifacts, and transition/resource/history budgets. `vopr-index` atomically persists the index and exposes run/revision/scenario/property/fingerprint/corpus/artifact/budget predicates as deterministic JSON or a static local HTML summary. Parallel campaign results now carry stable run identity, source/target/optimize metadata, and retained/quarantine artifact references. |
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

1. Finish the P0 full-cluster data plane by substituting the already-proven
   three-owner production `DataServer` composition for the hosted
   table-source/API rig. The focused histories already cover rollback and fresh
   retry through one owner, then networked forwarding, leader transfer, one
   owner restart, terminal retry, every-replica transition/watermark
   convergence, and document equality across three owners and replicated
   groups on one `VoprIo`. The merge protocol provides the durable v3 capability
   barrier, source prepare/finalize fencing, receiver checkpoints,
   catalog-independent replay identity, Raft-mediated copy, snapshot-carried
   controls, replicated observation, and actor-owned teardown. The same
   focused history now admits a new split generation, bootstraps it, performs a
   public post-bootstrap write, replays that delta after leader changes,
   finalizes cutover, restarts one owner, and verifies both ranges and all
   documents before fresh-state replay. The full-cluster composition must now
   substitute those owners so active transition commands route against the
   real metadata quorum while public graph/serverless clients and faults remain
   co-scheduled. Then remove the current
   co-location assumption with disjoint donor/receiver replica sets, page
   retained delete-history replay within an explicit resource budget, inject
   partitions, and prove snapshot install rehydrates each live DB owner and its
   derived graph/index state as well as the Raft projection. Only then may the
   full-cluster row be promoted from partially integrated to integrated.
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
   Next use the production DataServer/data-Raft composition from item 1, add
   public range-split churn, and compose cancellation, authorization
   change, and public document hydration. Add
   distributed joins and global queries with the same fail-closed publication
   rule; introduce an explicit partial-response schema only if product semantics
   ever require partial results.
3. Co-locate production HA and data-plane owners, extend the integrated
   node-memory denial/recovery mode to disk and socket pressure, and combine
   directional link, storage-crash, restart, serverless lease/object-store,
   and pressure faults. The real metadata/placement Raft wire hop, serverless
   public catalog path, memory-pressure recovery, and quiet cluster-wide suffix
   are complete at their named seams; broaden their fault combinations after
   item 1 instead of rebuilding them.
4. Extend full-cluster workload dimensions instead of multiplying suites:
   authenticated multi-tenant identities in addition to current two-table
   isolation, concurrent range split and replicated merge routing changes,
   explicit per-node disk/socket interference in addition to current memory
   pressure, and fairness between clients and background workers.
5. Close the remote-content live-secret use boundary: resolve preserved
   credential and header references at the actual scraping/object request,
   rotate during an in-flight request, and prove coherent per-request
   generations across retry, cancellation, refresh, and crash/reopen.
6. Add remote generation/reranking HTTP adapters to the existing chain history
   so fallback, provider replacement, malformed/truncated response, timeout,
   cancellation, and local/remote routing compose in one trace. Keep actual
   model execution and GPU kernels differential.
7. Maintain all focused integrated suites as production seams evolve. A gate
   is only "integrated" when the scenario module is forced into test discovery,
   its focused command executes at least one matching test, exact replay passes,
   and the command remains a dependency of `vopr-test`.
8. Adopt the integrated registered-deployment composer beyond full-cluster in
   the HA, data-plane, distributed-transaction, and serverless suites so node
   identity, readiness, fault scope, local storage/resource ownership, and
   quiet-suffix obligations remain uniform as those compositions converge.
9. Maintain the command composer, determinism audit, phased health adapters,
   recorder/event queries, debug recipes, results/index APIs, corpus merge, and
   injected-bug benchmarks. Wire their already implemented artifacts and
   recurrence/rarity reports into nightly retention and dashboards.
10. Continuously audit production loops so they borrow `std.Io` and `VoprIo`
   instead of creating native-only runtime paths. Add a manifest entry whenever
   a replayable source is exported, and preserve Threaded/physical-backend
   differential tests to detect simulator drift.
11. Add live rolling mixed-version cluster operation only after the repository
   has two runnable compatible binaries and an explicit upgrade contract.
   Current artifact compatibility and golden-reader campaigns remain the
   deterministic prerequisite, not a claim of live mixed-binary execution.
12. Add datagram, server TLS, or compiler-guided campaigns only when the
   conditional prerequisites above become real. A hosted graphical debugger
   and a container/hypervisor clone remain non-goals.

The defects already found—lifetime errors, listener shutdown races, provider
publication and teardown races, query-cache cancellation, malformed reranker
acceptance, host-only retry sleep, hidden Threaded I/O, virtual socket
accounting, FIN ordering, teardown deadlocks, TLS fail-open behavior,
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

## Conclusion

Antfly already owns the hard self-contained foundation: explicit deterministic
choices, one-transition scheduling, virtual tasks, files, sockets, processes,
clocks and durability, guided exploration, exact replay, reduction, formal
export, counterfactual analysis, and independent production-shaped scenarios.

The previously identified P0, P1, and non-conditional P2 orchestration, media,
and upgrade/compatibility suites are integrated at the production seams stated
in their conformance rows. Distributed VOPR is integrated across metadata,
transactions, Raft, HA, the data plane, distributed graph fanout, and a
deployment-shaped full-cluster campaign. A focused three-owner production
DataServer history also proves routed replicated merge execution followed by
replicated split bootstrap, a post-bootstrap public write, delta catch-up,
cutover, every-replica range/document convergence, leader transfer, owner
restart, and routed terminal retry. Remaining work deepens composition—first by
substituting those owners into the hosted full-cluster data rig so active
transitions forward against the real metadata quorum, then extending coverage
through disjoint placement, bounded transfer, partitions, and
projection/DB/derived-state snapshot recovery; then by running public graph requests
under those replicated topology transitions, cancellation, authorization, and
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
production ownership contracts.
