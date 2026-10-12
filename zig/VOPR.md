# VOPR: Deterministic Autonomous Testing for Antfly

Status: VOPR pairs a reusable, Antfly-independent deterministic simulation
engine (`lib/vopr`) with `VoprIo`, an application-scoped virtual `std.Io`
implementation that lets unmodified production code run under full
scheduling, fault, and replay control. Antfly scenarios built on this engine
cover metadata, transaction, Raft, storage, standby, data-plane, derived-workflow,
backup/restore, and clock-fault families, alongside a deployment-shaped
full-cluster composition that runs multiple production `DataServer`/metadata
owners, real HTTP/Raft transports, and a serverless workflow on one shared
clock and scheduler. Production-owner compositions progressively layer active
split/merge, distributed graph and join execution, cancellation,
authorization, replication backfill, and resource/transport/process fault
recovery onto that same deployment. Every promoted scenario carries a named
exact-replay gate: a clean-world replay must reproduce the recorded choices,
transitions, and observations byte-for-byte before a history counts as proof.
See Conformance Status below for what "integrated" does and does not claim,
and the Completion-Claim Audit for the residual boundaries of each family.

> **Relocated:** The versioned status narrative (v9-v53, 539 lines) that previously lived here is preserved verbatim in [docs/design/vopr/history/status-history.md](../docs/design/vopr/history/status-history.md). Durable decisions from it are in Conformance Status and the Completion-Claim Audit below.

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
ownership seams.

A deployment-shaped `full-cluster-vopr-test` composes a three-node metadata
quorum, hosted data ranges on node-local replica roots, two tables, three
production public API HTTP listeners, concurrent clients, and a serverless
workflow with its own production public catalog listener on one `VoprIo`. The
worker publishes into the same object-backed catalog that listener serves; the
serverless object catalog and the metadata placement catalog remain
intentionally distinct production domains. From v11 the production-owner modes
replace the hosted data nodes with real `DataServer`/data-Raft owners (see
[Full-Cluster Scenario Versions](#full-cluster-scenario-versions)). The
remaining distributed work is composition and fidelity breadth, not a missing
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
not yet co-reside every independently tested standby/data-plane owner.

### Completion-Claim Audit

The implementation is not the complete roadmap. Claims are valid only at the
following boundaries:

| Claim family | Audit result | Important exclusion |
| --- | --- | --- |
| Reusable VOPR engine, `VoprIo`, replay, reduction, properties, saved cross-run event-set queries, bounded live event streaming, flight recording, local reports, debug recipes, fault/service-rate algebra, and search-quality fixtures | Implemented and exercised by the focused engine/meta gates named below. Query cache, DataServer Raft and LSM maintenance, distributed graph fanout, replication snapshot/stream steps, and serverless publish/compaction rounds are reviewed service-rate charge seams; full-cluster v23, v42–v48, and v49 compose them with production owners | Nightly sharding, retention, review, notifications, and broader production adoption and cross-domain combinations of service-rate charging are operational or ongoing work |
| Metadata, Raft, standby, transaction, data-plane, storage, backfill, supervision, authentication, serverless, cache, provider, generation/reranking, and query suites | Implemented at each row's named production seam and fault vocabulary | The suites are not all co-resident in one deployment history |
| Distributed graph | Focused production coordinator paths, v9's hosted public graph, and production-owner graph modes v13–v18, v21–v22, v24–v25, and v27–v29 | V9's remaining topology breadth, disk-capacity overlap, broader socket-pressure and partial-write surfaces, and storage/process/restart overlaps are not on the production owners; graph cancellation is composed only clean (v25) and under a scoped transport outage (v29), not under resource/storage/process/restart or multi-fault combinations |
| Distributed join | V19–v20 and v30–v35: public inner `_id` join during a split, durable-finalizer takeover, worker cancellation, same-partition failover, partition-owner reconstruction, and cancellation or retry exhaustion under overlapping memory-plus-link faults | Cancellation under storage faults, disk pressure, or simultaneous process loss before cancellation drain; authorization and generation mutation; right/nested/foreign joins; multi-range left inputs; overlapping owner faults beyond the v33/v34 shapes |
| Global query | V36–v40: ordered two-table NDJSON dispatch, then cancellation, authorization revocation, transport cut, and owner-process loss at the first-result boundary | Topology/storage/resource faults, coordinator or metadata process loss, multi-process loss, and broader transport/overlap composition |
| Full-cluster v9 | The documented metadata/placement Raft, hosted data roots, public/serverless HTTP, graph-fanout, resource, merge-coordinator, replay, and cleanup behaviors are implemented | It is not a cluster of production `DataServer` owners. Data writes and merge structural actions are not proven through replicated DataServer apply on every replica |
| Full-cluster v11–v53 production owners | **Integrated at the named seams through v52; v53 is bounded-lifecycle evidence only.** Each version's seam and gate is in [Full-Cluster Scenario Versions](#full-cluster-scenario-versions). The focused LSM gate remains the exact LSM-maintenance witness | Complete and stabilize v53 record/fresh-world replay before promoting its readiness/reconstruction claim; remaining breadth is listed in the [Distributed Completion Audit](#distributed-completion-audit) |
| Replicated data-Raft merge/split protocols | **Integrated focused seam.** The multi-owner checkpoint implements merge v3 capability/barrier activation, split-delta predecessor fencing behind durable protocol v4, source fencing, receiver checkpoints, catalog-independent replay identity, copied-document proposals, snapshot-carried controls, replicated observation, merge-to-split, post-bootstrap write, sparse delta catch-up, cutover, restart, routed terminal retry, and every-replica range/document/transition/watermark convergence. Its record and fresh-state replay pass | Disjoint replica sets, retained-history pressure, derived graph/index equivalence, snapshot-install rehydration, and co-resident standby/data-plane/serverless faults remain unproven |
| Antithesis-style distributed execution | Registered in-process node/process/storage/resource/link domains and exact replay are implemented | Arbitrary separate-address-space binaries, sidecars, DNS, kernels, and live mixed binaries require the conditional federated-agent or native differential modes described below |

Therefore “integrated” must never be shortened in release notes or reviews to
“all planned VOPR work is fully implemented.” In particular, a test using
`ApiHttpServer`, `HostedProvisionedTableWriteSource`, and a node-local replica
root is not evidence that the `DataServer` owner, its data-Raft proposal/apply
state machine, or follower recovery participated.

Metadata's harness is `pkg/antfly/src/metadata/vopr_harness.zig`, exported as
`metadata_vopr_harness`. Its node/cluster and split/merge fixtures use VOPR
names, as do their test labels, imports, and determinism-audit paths. Metadata
harness targets use `lib-metadata-vopr-*`; there are no legacy sim aliases.
The lane suffix still describes fidelity:

- `lib-metadata-vopr-virtual-smoke-test` and
  `lib-metadata-vopr-virtual-transport-test` run the stepped virtual-transport
  smoke and convergence selections. The broader convergence selection includes
  median-key fixtures with native HTTP listeners; virtual Raft transport alone
  is not an end-to-end deterministic I/O claim.
- `antfly-metadata-vopr-test`, `lib-metadata-vopr-data-test`, and
  `antfly-metadata-vopr-chaos-test` retain their seeded campaign selections;
  `metadata-vopr-replay-stability-test` checks repeated exact replay.
- `lib-metadata-vopr-http-integration-test`,
  `lib-metadata-vopr-public-integration-test`, and
  `lib-metadata-vopr-forwarding-integration-test` retain native HTTP integration
  coverage. Naming them VOPR harness tests does not make them fully virtual.
- `lib-metadata-vopr-transition-chaos-test`,
  `lib-metadata-vopr-public-chaos-test`, and
  `lib-metadata-vopr-placement-chaos-test` select the broader fault suites;
  `lib-metadata-vopr-chaos-soak-test` combines those three selections.

The aggregate `vopr-test`, `antfly-integration-test`, and `antfly-chaos-test`
retain their existing selections. `vopr-soak-test` includes the broader fault
selections alongside retained-corpus search campaigns.

The production split planner requires a size observation to explicitly set
`disk_bytes_known` before trusting it; an observation with unknown size, or a
known-zero size paired with live documents, is treated as inconclusive and
fences planning rather than proceeding on unverified data.

Merge copy effects apply only to the exact active receiver transition: stale
or unfenced copy/delete/artifact commands become no-ops (`MergeCopyFenced`)
rather than mutating data. Each copy also carries a `(donor_term, sequence)`
attempt token so a delayed or superseded donor attempt cannot resurrect stale
writes after a newer attempt or a finalized/rolled-back receiver state. Merge
receipts live in the protected system-metadata prefix so a later physical
split retains them on the parent rather than the child, and physical split
assigns new LSM L0 run IDs oldest-first so older data cannot outrank newer
writes.

> **Relocated:** The dated 2026-09-07 metadata planning fix narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/follow-ups-2026-09.md](../docs/design/vopr/history/follow-ups-2026-09.md).

| Capability | Implementation evidence | Verification |
| --- | --- | --- |
| Stable structured choices and exact clean-world replay | `lib/vopr/src/choice.zig`, `runner.zig`, `replay.zig`, canonical `vopr-trace-v1`; a mismatch reports its byte offset and bounded first differing JSON lines without weakening byte equality | `zig build vopr-engine-test` |
| One-transition scheduling and typed termination | `scheduler.zig`, `scenario.zig`, `outcome.zig` | `zig build vopr-engine-test` |
| Antfly-independent runtime boundary | `runtime.zig`, `sim_runtime.zig`; `VoprIo` composes the narrow atomic executor into its scheduler; Antfly `DurableJobLane` adapter | `zig build vopr-engine-test vopr-runtime-test` |
| Deterministic `std.Io` tasks and synchronization | `vopr_io.zig`, `vopr_io_task.zig` | `zig build vopr-engine-test` in Debug and ReleaseSafe |
| Typed strong-read capabilities | `raft/read_gate.zig` separates enqueue-only `ReadIndexRequester` from synchronous `ReadSafetyBarrier`; managed Raft services expose only initiation, while public table-read sources accept only the barrier type. DataServer implements the barrier with canonical matching-group ReadState plus applied-index completion, starts replicated sources with a fail-closed unavailable barrier, and installs `alreadyReadSafeBarrier` only after explicitly selecting direct non-Raft ownership. The three-owner production history exact-replays follower success/typed stale-leader rejection, old-to-new leader transfer and retry, logical timeout, cancellation cleanup, state-machine group retirement, and graph/full-index visibility. The former readable-lease API, no-op name, service adapters, and metrics were renamed or deleted directly; there are no compatibility aliases | `zig build antfly-raft-test antfly-root-test antfly-data-runtime-test` |
| Modeled files, durability, persistent sector corruption, torn synchronization, streams, datagrams, processes, global quotas, and endpoint-stable reversible listener connection limits | `vopr_io_file.zig`, `vopr_io_net.zig`, `vopr_io_process.zig`; virtual streams distinguish an ordered write-half FIN from full peer read abandonment and hard reset. Both FIN and full-close control remain ordered behind prior payload, while only read abandonment/reset satisfies HTTPX's production-neutral H1 disconnect probe. The forward-only API is named for peer abandonment rather than generic disconnect, full close has its own stable transition identity, and this semantic change directly advances the virtual-OS replay model to v7. Endpoint/payload outage matching exposes a monotonic post-heal witness so scenarios can prove production traffic crossed an armed boundary | `zig build vopr-engine-test data-server-vopr-test production-cluster-graph-cancellation-vopr-test production-cluster-graph-cancellation-transport-fault-vopr-test` |
| Stable optional safepoints | `vopr_io_instrumentation.zig` | `zig build vopr-engine-test` |
| Clocks, timers, storage completions, and lifecycle faults | `time.zig`, `clock_fault.zig`, `fault.zig`, storage `sim_runtime.zig` | `zig build vopr-engine-test storage-vopr-runtime-test` |
| Properties, observations, semantic coverage, cross-revision corpus quarantine, property history, and guided search | `property.zig`, `observation.zig`, `coverage.zig`, `corpus.zig`, `explorer.zig` | `zig build vopr-engine-test vopr-benchmark` |
| Integrated retroactive flight recording and fielded temporal event queries | `flight_recorder.zig`, `event_query.zig`, `debug_recipe.zig`; bounded recordings own structured fields and verbose text outside canonical bytes, support conjunctive field/text filters and before/after windows, and are populated directly by runner-backed and custom metadata/domain replay paths. Every retained/failing campaign writes `.flight.json`, while every debug recipe packages a filtered reduced-replay window | `zig build vopr-engine-test vopr-meta-test`; `vopr events` and `vopr recipe` are argument-taking commands, not standalone test gates |
| Saved cross-run event sets, validation, counting, and live streams | `event_set.zig` validates a versioned forward-only query DAG and evaluates selection, union/intersection/difference/complement, distinct/first/last moment, previous/next, and bounded sequence operations across canonical histories. Its tests execute every operator and reject forward references, malformed operators, duplicate names, v0 formats, and the former ad-hoc selector shape. `vopr events` accepts only this saved-plan format and exact-replayed traces, with repeated `--trace`, `--validate`, and `--count`. Forward-only `vopr-event-stream-v2` serializes into a caller-owned fixed-slot SPSC queue: publication cannot allocate, block, or invoke external code, while one consumer may drain concurrently. Release/acquire publication prevents partial records; power-of-two capacity and wrapping monotonic positions preserve bounded operation across counter rollover. Drop-newest backpressure, oversize records, publication after close, delivery, and sink failures have separate atomically sampled saturating counters. Failed delivery retains the oldest record for retry, and close still permits draining. Tests cover invalid capacity, oversize rejection, concurrent producer/consumer accounting, retry, close, and pressure; runner pressure leaves canonical output byte-identical to an unobserved run. Custom observers remain an expert synchronous interface and must not block or panic | `zig build vopr-engine-test vopr-meta-test` |
| Reversible node and operation service rates | `service_rate.zig` registers unambiguous stable node/operation identities, composes fully checked node/operation costs (wide rounding, multipliers applied in canonical fault-ID order), charges logical time through borrowed `std.Io`, and heals individual effects by stable fault ID. Accounting distinguishes calls, work units, and logical nanoseconds. The generic fault and deployment registries type the effect separately from pause and terminal CPU budgets. Six production-neutral boundaries are integrated: query cache (request, hit-copy, coalesced-wait, producer-compute), DataServer Raft progress rounds and LSM maintenance, distributed graph (keyed by target group and expand/hydrate/get-edges), replication snapshot/stream permits, and serverless publish/enrichment/compaction/prune rounds. Focused histories prove exact slowed/healed behavior; full-cluster v23, v42–v48, and v49 compose them with production owners | `zig build vopr-engine-test query-embedding-cache-vopr-test antfly-data-runtime-test data-server-vopr-test distributed-query-vopr-test replication-backfill-vopr-test serverless-workflow-vopr-test` plus the full-cluster gates in [Full-Cluster Scenario Versions](#full-cluster-scenario-versions); cache topology/link/storage/resource overlap and remaining replication fault variants in the deployment remain roadmap breadth |
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
| standby lifecycle | replication, fencing, promotion, retention, restart, and rejoin | `zig build standby-vopr-test antfly-storage-hot-standby-chaos-test` |
| Independent application domains | distributed transaction, data plane, derived workflow, backup/restore, and clock faults | their five focused `*-vopr-test` gates |
| Production public HTTP on deterministic I/O | `vopr/data_server.zig`, `vopr/http_lifecycle.zig`, borrowed `HttpRuntime` and `BackendRuntime` lanes, transport-neutral metadata executor; chunked upload, keep-alive pipeline, streaming response, and half-close | `zig build data-server-vopr-test` |
| Production DataServer replicated merge/split seam | `data/runtime.zig`; the focused rollback/fresh-retry history uses one owner and two groups, while `data-server-transition-vopr-test` chains merge into split across three real `DataServer` owners and three replicated groups over time. It uses public HTTP/Raft listeners, routed merge actions, leader transfer, a public post-bootstrap delta write, replicated bootstrap/catch-up/finalize, owner restart, catalog-independent replay, exact routed terminal retry, every-replica range/transition/watermark convergence, document equality, actor-owned teardown, and fresh-root replay of the recorded actor/time schedule on one `VoprIo`. Clock-only stutter is normalized at the explicit physical LSM differential boundary; no different actor may execute, and a recorded actor that does not become ready within the bound is replay divergence. A regression preserves exactly-once split-action lane release when an inline durable job fails | `zig build data-server-transition-vopr-test`; broader `data-server-vopr-test antfly-data-runtime-test lib-data-storage-test` gates remain required before release |
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
| Public distributed join on production owners | Antfly `api/distributed_join.zig`, `api/internal_join_operations.zig`, `api/table_reads.zig`, `vopr/production_cluster.zig`, and `vopr/full_cluster.zig`; full-cluster v19, v20, and v30–v35 (see [Full-Cluster Scenario Versions](#full-cluster-scenario-versions)) | The eight `production-cluster-*join*-vopr-test` gates in that table, with `-Doptimize=safe`; only those join shapes and lifecycles are claimed |
| Deployment-shaped full cluster | Antfly `vopr/full_cluster.zig`, `vopr/production_cluster.zig`, `vopr/serverless_workflow.zig`, `metadata/vopr_harness.zig`, and the production HTTP/Raft runtimes share one `VoprIo`. V9 retains the hosted/public campaign; v11–v53 add production-owner seams one version at a time. Public strong reads route to the current owner and require matching applied ReadState plus local derived-index visibility; remote work crosses typed internal HTTP while modeled time borrows the shared runtime | `zig build full-cluster-vopr-test production-cluster-vopr-test`. Complete gates include named properties, record/fresh-world replay, cleanup, and leak checks; the 2,000-transition subprocess and v53's 35,000-transition gate are bounded-lifecycle evidence only |
| Parquet cache, provisioning/startup, external lake, and media providers | Antfly `vopr/parquet_cache.zig`, `provisioning_startup.zig`, `external_lake.zig`, `media_runtime.zig`; borrowed `VoprIo`, real cache/reconcile/Iceberg-manifest/Parquet-query/provider-HTTP paths, injected I/O and object-store faults, provider retry/timeout/cancellation and active-request drain, cleanup, and exact replay | `zig build parquet-cache-vopr-test provisioning-startup-vopr-test external-lake-vopr-test media-runtime-vopr-test` |
| Product upgrade and compatibility campaign | Antfly `vopr/upgrade_compatibility.zig`; current production readers open v1 standby golden records, v12 manifests, v14 external inventories, and legacy serverless heads; incompatible data directories and future product artifacts fail closed; atomic data-directory publication recovers after a crash-before-rename. VOPR-native traces, checkpoints, and fixtures are intentionally outside this campaign because their schemas are forward-only | `zig build upgrade-compatibility-vopr-test` |

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
| Multiple logical nodes and clients | Multiple containers or pods | Integrated in metadata, distributed-data, distributed-transaction, Raft, standby, and data-plane suites |
| Link and packet faults | Asymmetric latency, loss, clogs, partitions, and recovery | Integrated drop, duplicate, reorder, delay, jam, outage, directional partition, and healing |
| Node lifecycle and pressure | Pause, stop/kill, restart, and throttling | Integrated at registered process/resource seams: pause, crash/restart, CPU-work exhaustion, descriptor, socket, allocator, and storage limits. Reversible logical per-node and per-operation slowdown/cost modeling is integrated through borrowed `std.Io`; arbitrary native CPU/thread throttling and native-thread pause are not implemented |
| Deterministic replay and branching | Deterministic hypervisor execution | Exact choice/transition/observation replay plus reduction and multiverse branching |
| Whole unmodified deployment | Arbitrary containerized binaries and sidecars | Deliberate non-goal; only registered in-process entrypoints are deterministic |
| One full Antfly deployment history | Runs a supplied Docker Compose or Kubernetes topology | Integrated in-process at named complementary seams: the production-owner full-cluster modes run the real metadata quorum, three production DataServer/data-Raft owners and resource managers, public two-table I/O, the serverless catalog, and an active split on one `VoprIo`, adding one fault or workflow seam per [scenario version](#full-cluster-scenario-versions); v9 supplies the hosted public graph and broader nine-fault vocabulary. Remaining topology, disk-pressure, socket/short-write, and storage/process/restart overlap, v53 completion, and co-resident standby are not yet on the production owners. Separate address spaces, native sidecars, DNS, kernels, and live mixed binaries remain conditional/differential concerns |

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
- Reuse existing Raft, standby, LSM, storage, transaction, integration, and formal
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
 Antfly metadata, Raft, storage, standby, and application adapters
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
restore progress probes, DB enrichment startup, repair entropy, and standby repair
receipt persistence onto borrowed I/O. Compatibility wrappers may use
`std.Options.debug_io`, but they do not own another executor; new production
callers should always pass their runtime lane explicitly.

Deadlines and retry timing follow the same rule: a component that sleeps on
borrowed I/O must also compute its deadlines from that I/O.

- Raft HTTP frame-queue retry readiness and jitter, and snapshot-transfer
  deadlines, use the owning I/O's monotonic clock. HTTP host construction
  propagates that authority to route reconciliation, admission retries,
  policy rechecks, and bootstrap status timestamps; snapshot staging names
  draw entropy from the same I/O, not host time or a process-global counter.
- Queued Raft delivery keeps the request's timeout when it takes ownership of
  the copied payload, and a queued request does not retain the enqueuing
  caller's borrowed cancellation or delivery-tracker pointers.
- The DataServer maintenance worker computes vector-publication deadlines from
  its borrowed monotonic clock. Every bounded attempt, including idle and
  failed ones, sets a retry boundary and releases its reservation before
  waiting; accepted wakes survive the backoff.

Virtual-clock regressions cover retry readiness, repeatable jitter, transfer
deadlines across realtime changes, a never-responding Raft peer, and
far-future maintenance deadlines; the determinism audit covers these workers.

### Fail-Closed Capability Model

Construction preflights declared capabilities. Unsupported operations return
their documented error or latch a harness violation when the vtable operation
cannot return an error. No handler aliases or delegates to `std.Io.Threaded`.
The backend identity pins the Zig version, supported capability set, virtual-OS
model, and instrumentation map.

The current native ABI is version 4, which rejects borrows and shared-manager
layouts recorded under older versions; the current virtual-OS model is version
8, which folds cancellation semantics into backend identity. A history
recorded under an earlier native ABI or model version must be freshly
recorded, not migrated.

### Implemented Task Kernel

Stackful simulated tasks support futures, groups, await, cancellation, sleep,
futexes, queues, mutexes, and selection. Runnable-task selection, park, wake,
cancel, spurious wake, eager completion, timer delivery, and futex waiter
selection are scheduler-visible stable choices.

Scheduler-visible identity rules (virtual-OS model 8):

- Futex pointer identities expire when the last waiter leaves; external-wake
  sequence numbers advance independently per logical resource; an eager
  network producer creates the same accept-readiness completion as a waiting
  consumer.
- Task and futex identities exclude process-global allocation order. Each
  fiber is scoped to its logical parent, an ASLR-independent callsite offset,
  and a callsite-local epoch; the raw creation ordinal is diagnostic metadata
  only. A task's first external block binds its scheduler identity to the
  logical resource and a resource-local waiter epoch, so unrelated sibling
  creation cannot rename an established owner.
- Listeners are scoped to stable IP/Unix endpoints, provisional client/server
  pairs to the listener and connecting owner, and packets/FINs/datagrams to a
  source-socket-local sequence. Steady-state connection identity is a function
  of the logical connecting owner plus first stream payload, and parked
  read/write resources migrate before delivery is selectable, so distinct or
  reordered connections cannot exchange identities.
- Packet transitions carry a digest of their bytes, so equal-length payload
  changes fail exact replay.

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
Its crash/restart oracle verifies surviving-quorum reads and retirement before
healing, then verifies every replica; healing restores metadata blackhole
routes as well as virtual network faults, and an uncertain reconcile-lease
proposal stays pending until committed authority is observed rather than
aborting the partition history or granting authority from an unknown response.

Focused gates: `antfly-metadata-vopr-test`,
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
complementary gates and corpus sources. The crash/restart oracle checks
volatile monotonicity within an incarnation plus durable term/commit and
completed application across restarts, and preserves each node's append and
apply lane order against the async Raft storage contract.

Focused gate: `raft-vopr-test`.

### Storage: WAL, LMDB, LSM, Persistent, Index Manager, and DB Split

Storage scenarios adapt their real action vocabularies and oracles to the
common choice, artifact, replay, campaign, and fault lifecycle. Coverage
includes C-versus-Zig LMDB, memory-versus-real LSM, WAL publication and reopen,
real PersistentIndex, split IndexManager, full DB split, maintenance,
compaction, crash recovery, volatile/durable state, and typed storage faults.

Focused aggregate: `storage-vopr-test`. Exact fixture and legacy real-I/O
commands are documented under Test-Tier Policy below.

### Standby

The standby scenario drives real primary and standby logs, progress WALs, slot and
fence stores, replication, application, partition, crash, retention, backup,
promotion, rejoin assessment, stale-owner fencing, and ordered applied-prefix
properties.

Focused gates: `standby-vopr-test` and `antfly-storage-hot-standby-chaos-test`.

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
- a co-scheduled serverless build/enrichment/publication fixture and production
  serverless HTTP listener over the exact object-backed catalog mutated by the
  worker; forward-only v50 adds its stale-generation/progress-conflict mode
  beside live production metadata, DataServers, and public clients;
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
in-flight graph-transport failure/recovery, partial-HTTP-write, and aggregate
node-memory denial/recovery modes exact replay in the hosted campaign. After a
clean worker completes, a production `ServerlessHttpClient` lists `docs` and
queries version 3 through the real serverless handler and `httpx` listener.
Transaction creation and recovery use the same `BackendRuntime` realtime
clock. Borrowing worker I/O alone is insufficient: a host-clock recovery pass
can expire a fresh virtual-time transaction between begin and prepare.
`transaction-runtime-regression-test` checks fresh and expired transactions
through both write-source configurations, along with bounded stateless retry
behavior and preservation of conditional conflicts and unknown outcomes.
The resource-pressure recording and exact replay remain in
`full-cluster-vopr-test` as a separately selectable test.
`vopr-runtime-regression-test` groups the runtime ownership, clock, snapshot,
and cluster-replay regressions and accepts runtime `--test-filter` arguments.
Forward-only v50 replaces the older standalone stale-generation cluster mode:
the production-owner campaign queries version 4 and requires the authoritative
document after a losing publication CAS and stale-derived-record rejection.
The public DataServer client also writes a graph whose two edges cross the
table's range boundary, waits for full-index acknowledgement, and traverses
both hops through the production public-query parser, range planner, internal
HTTP expansion protocol, shard fanout, and canonical response decoder.
Raft time and delivery eligibility remain explicit modeled rounds, but
each successful delivered frame crosses the production binary codec, fault
router, `IoHttpExecutor`, VOPR socket, httpx listener, and production Raft HTTP
handler. The serverless object catalog and metadata placement catalog are
separate production domains; joining them would invent an ownership
relationship that Antfly does not have. The current limits are instead that standby
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
`VoprIo`. Every incomplete public graph, join, or global-query operation in
the production-owner modes must fail closed without partial rows; each faulted
history heals and proves a complete fresh request.

> **Relocated:** The per-version v11–v53 narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/full-cluster-scenario-narratives.md](../docs/design/vopr/history/full-cluster-scenario-narratives.md).

#### Full-Cluster Scenario Versions

Each version is a forward-only mode of the `full-cluster` scenario. V11 onward
run on the real metadata quorum, three production `DataServer`/data-Raft
owners, real public HTTP and Raft listeners, two tables, and the serverless
catalog on one `VoprIo`; each later version adds one seam to that deployment.
Gates are `zig build <gate>` steps; the number is the transition budget of the
complete record/fresh-world exact-replay history. `production-cluster-vopr-test`
runs every gate below from v11 through v53. Version numbers without a row are
not current modes (v41's cache composition was replaced by v49).

| Version | Seam / what it adds | Gate (budget) | Status |
| --- | --- | --- | --- |
| v9 | Hosted/public campaign: metadata/placement Raft, hosted data roots, public and serverless HTTP, cross-range graph, and nine faults (metadata partition, node restart, in-flight graph-leader restart, graph range-merge churn, graph transport failure, partial HTTP write, node-memory denial) | `full-cluster-vopr-test` | Integrated (hosted nodes, not `DataServer` owners) |
| v11 | Static production-owner baseline: public/serverless work on the live metadata quorum and three `DataServer` owners | `production-cluster-baseline-vopr-test` (240,000; part of `production-cluster-vopr-smoke-test`) | Integrated |
| v12 | Metadata-driven active split through the hosted transition path: destination bootstrap/apply, finalization, publication, post-split read | `production-cluster-vopr-deep-test` (320,000); 2,000-transition bounded cutoff in the smoke gate | Integrated |
| v13 | Depth-two public graph on production owners with the strong-read/derived-visibility barrier | `production-cluster-graph-vopr-test` (35,000) | Integrated |
| v14 | V13's graph before, during, and after the active split | `production-cluster-graph-split-vopr-test` (400,000) | Integrated |
| v15 | Fail-closed cut of the real next-owner graph stream | `production-cluster-graph-split-transport-vopr-test` (450,000) | Integrated |
| v16 | Next owner stopped and reconstructed with stable-port public/Raft listeners before Raft and graph recovery | `production-cluster-graph-split-owner-restart-vopr-test` (650,000) | Integrated |
| v17 | One semantic-stream-selected short write on the registered coordinator-to-owner link; production stream resumption | `production-cluster-graph-split-partial-write-vopr-test` (500,000) | Integrated |
| v18 | All-owner memory denial and recovery during the split | `production-cluster-graph-split-resource-pressure-vopr-test` (550,000) | Integrated |
| v19 | Public inner `_id` join; two right rows on independently owned ranges; before/during/after the split | `production-cluster-join-split-vopr-test` (11,000 bounded + 420,000) | Integrated |
| v20 | Forced 64-row durable shuffle; first finalizer fails after persisting; another owner imports the cached result (two-attempt ledger) | `production-cluster-durable-join-takeover-vopr-test` (300,000) | Integrated |
| v21 | Selected graph link cut overlapped with all-owner memory pressure | `production-cluster-graph-split-overlapping-faults-vopr-test` (600,000) | Integrated |
| v22 | Selected-listener connection-limit denial before handler ingress, then recovery | `production-cluster-graph-split-socket-pressure-vopr-test` (500,000) | Integrated |
| v23 | One service-rate model across DataServer, graph, and serverless owners; exact two-times costs before healing, baseline after | `production-cluster-service-rate-vopr-test` (90,000) | Integrated |
| v24 | Exact selected-field document hydration through the public graph response | `production-cluster-graph-hydration-vopr-test` (90,000) | Integrated |
| v25 | Hydration fanout cancelled through the public HTTP disconnect path; one clean retry | `production-cluster-graph-cancellation-vopr-test` (110,000) | Integrated |
| v27 | Authenticated workload; target-table read policy revoked at `target_authorization_started`; concealed no-leak output; restore and fresh retry | `production-cluster-graph-inflight-authorization-vopr-test` (120,000) | Integrated |
| v28 | Split published after `source_snapshot_acquired`; exactly two `TopologyChanged` attempts and a typed no-partial 503 | `production-cluster-graph-stale-snapshot-vopr-test` (340,000) | Integrated |
| v29 | Real `/graph-hydrate` traffic matches a scoped coordinator-to-owner outage before cancellation; heal and recover | `production-cluster-graph-cancellation-transport-fault-vopr-test` (140,000) | Integrated |
| v30 | Durable shuffle cancelled at its first partition-worker boundary; worker token observes cancellation; no canceled completion | `production-cluster-durable-join-cancellation-vopr-test` (360,000) | Integrated |
| v31 | Pre-publication partition-worker failure; same-partition failover to another group with one retry | `production-cluster-durable-join-worker-retry-vopr-test` (360,000) | Integrated |
| v32 | Partition-owner process destroyed; typed no-partial exhaustion; stable identity rebuilt; direct rebuilt-endpoint read | `production-cluster-durable-join-owner-restart-vopr-test` (420,000) | Integrated |
| v33 | Retry exhaustion under real memory saturation plus a matched exact-group link cut; both healed independently | `production-cluster-durable-join-retry-exhaustion-vopr-test` (420,000) | Integrated |
| v34 | Remote worker-link outage, then alternate-worker cancellation with every memory envelope full | `production-cluster-durable-join-cancellation-overlap-vopr-test` (420,000) | Integrated |
| v35 | Worker cancellation followed by exact worker-owner reconstruction | `production-cluster-durable-join-cancellation-owner-restart-vopr-test` (420,000) | Integrated |
| v36 | Two-line global NDJSON request through `/db/v1/query` to `docs` and `tenant_b_docs`; ordered, exact, disjoint results | `production-cluster-global-query-vopr-test` (90,000) | Integrated |
| v37 | Cancellation after the first table result; typed client cancellation, listener drain, exactly one assembled result | `production-cluster-global-query-cancellation-vopr-test` (110,000) | Integrated |
| v38 | `tenant_b_docs` read permission revoked after the first result; exact 403 without protected payload | `production-cluster-global-query-authorization-vopr-test` (120,000) | Integrated |
| v39 | Registered tenant-owner query stream cut after the first result; exact retryable 503 | `production-cluster-global-query-transport-vopr-test` (140,000) | Integrated |
| v40 | Tenant-owner process destroyed at the same boundary; 503; reconstruction; direct durable read from the rebound endpoint | `production-cluster-global-query-owner-restart-vopr-test` (240,000) | Integrated |
| v42 | Production replication runner through canonical public `BatchRequest` routing, leader forwarding, DataServer Raft, and full-index visibility; slowed, then healed | `production-cluster-replication-backfill-vopr-test` (160,000) | Integrated |
| v43 | Source schema change after the first accepted batch; durable resume queries `users_v2`; exactly one duplicate batch | `production-cluster-replication-schema-change-vopr-test` (180,000) | Integrated |
| v44 | Target leader destroyed and reconstructed before the next batch; `SendFailed` then one stale pooled-connection failure; five attempts, three successes | `production-cluster-replication-owner-restart-vopr-test` (220,000) | Integrated |
| v45 | First provider `query` returns `ConnectionResetByPeer`; newer owned session resumes (source generations 1/2, three balanced sessions) | `production-cluster-replication-source-crash-vopr-test` (220,000) | Integrated |
| v46 | Work lease revoked after durable offset 1; `CdcWorkLeaseLost`; no duplicate committed batch | `production-cluster-replication-cancellation-vopr-test` (220,000) | Integrated |
| v47 | Lease lost between target apply and checkpoint; durable offset stays 0; one idempotent replay, four successes | `production-cluster-replication-stale-owner-vopr-test` (240,000) | Integrated |
| v48 | Exact-cutover source catalog v2 and authorities published through metadata Raft; `ReplicationSourceConfigChanged`; authority A retired under B | `production-cluster-replication-topology-change-vopr-test` (260,000) | Integrated |
| v49 | Node 1's real `ApiHttpServer` query cache: coalesced waiter expires at its logical deadline under slowdown; heal; owner reconstruction; empty-cache recompute; one stale pooled-connection reconnect | `production-cluster-query-cache-deadline-restart-vopr-test` (120,000) | Integrated |
| v50 | Real serverless candidate (version 2) loses its progress CAS with `HeadChanged` after generation 3 cutover, beside ordinary cluster work; retry publishes version 4 | `production-cluster-serverless-fencing-vopr-test` (120,000) | Integrated |
| v51 | Authentication on all three public servers; two disjoint table identities; exact bidirectional cross-node 403 read/write denials and owning-identity 404 absence | `production-cluster-authenticated-tenant-vopr-test` (120,000) | Integrated |
| v52 | Node 2's live capacity source lowered to zero: persistent-cache reservation denied with public reads still available; heal; one reservation/write/release | `production-cluster-disk-capacity-vopr-test` (120,000) | Integrated |
| v53 | Public create-index through metadata Raft; pending readiness while the provider fails document work; production reconciliation begins | `production-cluster-managed-index-vopr-test` (35,000, bounded lifecycle) | Bounded evidence only |
| 54 | Current `Scenario.version`; closes the join/split packet-replay and replay-error teardown failures without adding a mode | (all of the above) | Current ABI |

The complete v42–v52 gates passed 15/15 Debug and ReleaseSafe runs at their
promotion checkpoint; a changed tree must rerun the named gate before carrying
a claim forward. V53's intended suffix (coherent coverage and replay
watermarks, three indexed documents, durable node-1 reconstruction, and the
same semantic query through all three public nodes) is not promoted: its deep
run exposes packet-level replay divergence around Raft status publication.
Stabilize and complete it before adding crashes inside each
candidate/catalog/alias publication gap or managed-index disk-pressure overlap.
The `production-cluster-managed-index-vopr-test` step's filter (`…managed
index publication recovery exact replay`) does not currently match the test
name (`…managed index publication bounded lifecycle exact replay`), so that
step selects no test until the two are reconciled.

Contracts the production-owner modes rely on:

- Transition retry jitter is a stable per-node deterministic input (v12).
- A generic completion fence keeps listeners, Raft owners, and storage alive
  until an external runner (replication, cache, serverless) finishes (v42+).
- The replication permit wrapper validates the work lease before delegating to
  the cluster service-rate permit, so charging cannot disable fencing or
  cancellation (v46). The snapshot runner revalidates `WorkPermit` ownership
  between target apply and checkpoint publication (v47).
- The serverless builder exposes one optional production-neutral lifecycle
  hook after the immutable candidate manifest is durable and before the
  guarded progress CAS; it is unset in production and shared by every builder
  publication path (v50).
- V49 reaches the cache embedded in the production `ApiHttpServer` through an
  owner-scoped keyed operation and the server's real ResourceManager budget;
  it does not claim a complete public semantic HTTP request or an external
  inference provider executed. The cache work port is reinstalled whenever
  that `DataServer` is reconstructed.
- Node 2's modeled volume has a healthy 2 GiB baseline, above the production
  `ResourceManager`'s 1 GiB safety floor (v52). V52 promotes the persistent-cache
  reservation seam, not managed-index repair under disk pressure or disk
  overlap with link/storage/process faults.
- V42 onward add modes, properties, trace revisions, observations, and build
  targets directly; there are no legacy aliases, readers, or migration paths.
- Gates are tiered by deterministic work, not by a weaker oracle: record and
  fresh-state replay, properties, cleanup, and enabled-set equality are
  identical in every tier. Each large subprocess uses exit-code-checked test
  mode, serialized by the build graph's inherited stdio lock. Exact replay
  compares canonical wire records structurally and renders only the first
  mismatch.

This is distributed process-lifecycle coverage at an application-owned
`std.Io` boundary, not an arbitrary process matrix: metadata/coordinator
restart, multiple simultaneous owner failures, process loss composed with
storage/resource/partition faults, disjoint placement, derived-state equality,
bounded retained-history replay, snapshot-install rehydration, and co-resident
v9 fault breadth remain open.

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
round-trips and verifies the production standby backup manifest; the focused gate
also runs portable, restore-job, Raft restore, standalone, and standby regressions.

Focused gate: `backup-restore-vopr-test`.

### Clock Faults

The Antfly-independent clock surface separates realtime jumps, oscillator
frequency, node pause, monotonic passage, timer delivery, and stabilization.
The focused gate composes production TTL, transaction lease, standby retention, and
seed-lifecycle regressions.

Focused gate: `clock-fault-vopr-test`.

## Defects Found

VOPR has caught real defects across these classes:

- Executor and clock escapes, where production code silently constructs a
  native thread, a threaded I/O runtime, or reads the host wall clock instead
  of using the caller's borrowed deterministic runtime.
- Scheduler-identity aliasing, where task, futex, socket, listener, and packet
  IDs derived from process-global counters or raw pointers could alias across
  unrelated operations and defeat exact replay.
- Shallow replay comparison, where replay checked only an ID, a byte count, or
  an enabled-set match instead of the actual selected transition and payload
  content, letting real differences pass silently.
- Cancellation-ordering bugs, where a canceled parent task could unwind before
  its children finished, or a retry loop re-caught and discarded its own
  cancellation instead of propagating it.
- Teardown and restart races, where resources were destroyed out of lifecycle
  order, uninitialized or poisoned memory was read during early teardown, or
  an owner's identity was not preserved across reconstruction.
- Write-outcome misclassification, where ambiguous and definite write
  failures were conflated, or resource/leadership/topology errors collapsed
  into an unretryable failure instead of a typed retryable one.
- Ownership misrouting, where a request reached a non-owning replica or node
  and either failed outright or was silently executed against the wrong local
  store.
- Replication and merge/split protocol gaps, where sparse sequence
  watermarks, unprojected checkpoints, or rolled-back/unbootstrapped state
  could be mistaken for real progress, risking a non-idempotent replay.
- Authorization boundary gaps, where fail-closed credentials were missing
  from a fixture, or a permission change was not enforced against work
  already admitted into a request.
- Resource-accounting blind spots, where memory, disk, or connection usage
  was invisible to the shared resource manager, or a capacity source leaked
  real host state into modeled behavior.
- Fault-injection fidelity bugs, where an injected fault was too coarse
  (crossing unrelated protocols) or too narrow (missing fragmented writes, an
  accidentally local "remote" target) to exercise the intended boundary.
- Ownership and leak bugs on error and cancellation paths, where allocated
  results or handles were not released when a request was canceled or a
  lifecycle hook failed.
- Cross-boundary encoding and ABI mismatches, where independent compilation
  units, internal handlers, or clients disagreed on wire encoding or
  error-domain translation for the same logical value.

> **Relocated:** The full dated defects ledger (1,162 lines) that previously lived here is preserved verbatim in [docs/design/vopr/history/defects-found.md](../docs/design/vopr/history/defects-found.md).

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
lsm                   standby              standby-scaling
```

## Test-Tier Policy

### `antfly-root-test`

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

### `antfly-chaos-test`

- Longer but transition- or history-bounded deterministic campaigns.
- Independent labeled nodes for metadata, transaction, Raft, WAL, LMDB, LSM,
  persistent index, index manager, DB split, standby, and application domains.
- Every failure prints or stores an exact replay artifact.

### `vopr-soak-test`

- Larger history counts, broader fault budgets, and retained native
  differentials.
- Runs the campaign CLI for standby, Raft, distributed data, and production
  standby/scaling with `--fail-on-findings` and one worker. Defaults are 100 histories
  for the smaller scenarios and two for production standby/scaling.
  Property findings fail the gate after reports and replay artifacts are
  written; replay divergence and harness errors also fail it.
- Uses `--defer-diagnostics` to retain findings and flight recordings without
  spending the soak budget on automatic reduction and counterfactual replay.
  Run `vopr recipe` against a retained trace for that separate analysis.
- Retains the metadata transition/public/placement and Raft differential
  selections from the former soak target. Their native I/O is still native;
  sharing a tier does not turn them into exact-replay campaigns.
- Never part of the default fast gate.

```sh
zig build vopr-soak-test -Doptimize=safe -j1 \
  -Dvopr-soak-histories=1000 -Dvopr-soak-production-histories=2 \
  -Dvopr-soak-seed=2709476608 \
  -Dvopr-soak-artifacts=/tmp/antfly-vopr-soak
```

The artifact directory has separate `standby`, `raft`, `distributed-data`, and
`standby-scaling` corpora, with `results.json`, HTML reports, retained traces, and failure
diagnostics. Reusing a directory resumes its corpus. Reproducing the entire
guided search requires the same initial corpus as well as the same seed and
budget; each retained history independently supports exact replay.

### Standby, Raft, and scaling coverage

The merged fault algebra supplies explicit overlap, precedence, exclusions,
fault budgets, and healing. Those engine features are not evidence that every
production ownership transition has been composed with every fault, and the
bounded `vopr-test` gate is not evidence of a completed soak run.

The `standby` lifecycle scenario (version 2) uses `VoprIo` files and monotonic
time for the production primary log, replication slots, standby
receive/progress WALs, and fencing receipts; standby apply deadlines borrow the
progress WAL clock, including deadlines built by the production DataServer
caller. Version-1 native-storage traces must be regenerated. Its application
callback checks an ordered payload model rather than applying into the
production cluster's DB, and its restarts close and reopen owners rather than
injecting power loss between storage operations. Campaign, scenario, property,
and fixture names use `standby`/`standby-scaling` with no HA-named aliases;
pre-rename corpora replay only with their retained original runner.

`standby-scaling` (`production-standby-scaling`, version 2) composes the
production metadata quorum, three Raft-backed DataServers, and a separate
production primary/standby DataServer pair on the shared scheduler. Hot
standby is the documented single-primary mode: promotion does not replace a
Raft voter. The standby owners use a fixed standalone table catalog; the Raft
deployment uses its real metadata quorum. `standby-scaling-vopr-test` records
and exactly replays one complete history that drains and restarts the third
data owner, raises and lowers desired replicas, lets production status
collection and median-key RPCs choose a split, fences and promotes the standby
through the authenticated admin API during that active transition (with a
metadata-leader restart), and then merges the split siblings back while
public writes continue. `standby-production-vopr-test` isolates the production
standby lifecycle and cancels right after promotion to verify task and
network cleanup.

The oracles check acknowledged values, exact range coverage without gaps or
overlap, standby safe-read/apply bounds, fencing, promotion identity, replica
convergence, bounded completion, and owner cleanup. Contracts the history
relies on:

- Standby log/slot/progress and fencing writes borrow VOPR storage and clocks,
  including after promotion. Standby replication uses its own internal bearer
  credential, configured independently of the standby admin endpoint.
- Automatic planning borrows the DataServer wall and monotonic clocks, so
  shard cooldown is independent of host time; median keys come through the
  production routed shard-DB adapter; disk-size collection uses the owner's
  (possibly virtual) filesystem; disk-scan admission uses primary document
  cardinality while derived indexes catch up.
- Reconstructed metadata owners reinstall their shard RPC callbacks, and
  restarted Raft owners keep the externally bound listener URL in their
  registration. The maintenance coordinator closes admission and exits when
  its borrowed lane is canceled.
- Split finalization atomically persists the narrowed range and Raft entry
  receipt through a metadata-only path, so a later merge validates the same
  base in the Raft projection and document DB and replaying an older split
  entry cannot narrow the merged range again. Merging split siblings preserves
  the independent document-ID namespaces of the two original ranges.

This is a bounded composition, not exhaustive fault coverage. The fixture keeps
a native temporary namespace for ancillary stores such as the unused API
restore-job LMDB; that boundary is not a power-loss model. Torn standby writes,
disjoint placement, broader link/disk/resource fault combinations, retention
pressure, and the Kubernetes operator/cloud provisioning loop remain follow-up
coverage. Threshold and desired-replica changes exercise the Zig controllers,
not a simulated Kubernetes autoscaler.
### Scheduled retained-corpus soaks

[zig-vopr-soak.yml](../.github/workflows/zig-vopr-soak.yml) runs daily at 10:00
UTC once merged into the default branch, and supports manual dispatch. It
builds one ReleaseSafe runner, then runs two shards each of `standby`, `raft`,
`distributed-data`, and `standby-scaling`. Per-shard history budgets are 1000,
1000, 12, and 2 respectively; the dispatch input can override them. Each shard
uses one worker and records its exact seed, revision, command, initial corpus,
and completion status in `run.json`, including the executable SHA-256 and
restored trace digests. Shard 0 uses bounded-fair mutation and shard 1 uses
adversarial mutation. Initial histories use the scenario's baseline generator
(cooperative scheduling for standby/scaling). `--exploration-policy cooperative`
selects the cooperative mutation suffix explicitly for comparisons.

The parallel `production-e2e` job builds and retains one production executable,
checks its SHA-256 before and after testing, repeats the compiled storage-owner
publication tests 20 times, and runs 200 public restore cases. It also runs
200 multi-node Autograph resolution/promotion/hydration cases: 50 original and
50 data-restart cases per normal and 256-FD profile, with two workers. Restart
cases reopen every data node after the initial document commit and require
promotion without a new write to wake recovered owners.

The same job runs 100 large-catalog control/diagnostic isolation cases (50 per
profile), plus 200 concurrent aggregation/savepoint-transform cases (50 of each
per profile). Catalog fixtures use production Raft/control cadence and retain
HTTP rejection details without retrying failed mutations. The aggregation case
retains its exact-result assertions and 15-second per-request deadline.

`scripts/ci/zig-e2e-autograph-soak.sh`, `scripts/ci/zig-e2e-catalog-soak.sh`, and
`scripts/ci/zig-e2e-query-transaction-soak.sh` expose these profiles locally. Set
`ANTFLY_E2E_REGRESSION_REPORT_DIR` to a fresh directory and optionally override
workers/repetitions. These native stress tests complement deterministic runtime
and Raft tests; they do not provide VOPR schedule replay. Session unit tests
separately force foreground/recovery overlap and cancellable execution handoff.
Aggregation unit tests advance the primary generation after result capture and
require exact results from the captured rows. They also require one read lease
across selection and collection, with Raft admission before writer exclusion.
The `production-e2e-soak` artifact retains the executable, logs, exact per-case
JUnit reports, and failed server roots. Missing, skipped, failed, or incorrectly
counted cases cannot qualify a run. PR-only qualification skips these full
production soaks.

A bounded-fair suffix randomizes runnable work, ages continuously enabled
alternatives, and services the oldest overdue alternative after a 256-choice
window. An adversarial suffix first prefers time advancement and delays a
selected ready actor for 128 choices, then restores fair scheduling so the
scenario can demonstrate recovery. Exact replay uses the recorded choices.
Each `*.schedule.json` records the mutation point, replacement, suffix seed,
policy bounds and parent digest, or both parents and the selected splice point.
Scheduling provenance follows retained traces into the next campaign.

Scheduled campaigns use `--defer-diagnostics`: every finding still retains its
trace, flight recording, and aggregate summary, but automatic reduction and
counterfactual searches run separately through `vopr recipe`. This keeps one
production finding from consuming the entire scheduled budget before reports
are published.

The workflow caches the built runner by source revision and target, then keys
each scenario's working corpus by that executable's SHA-256. Unchanged
revisions reuse the exact runner, including its fiber identities; a different
runner starts a fresh corpus rather than treating an old executable layout as
a new replay divergence. Older artifacts remain available for diagnosis with
their retained executable.

The workflow restores the last compatible scenario corpus, copies it into a
fresh run directory, and uploads reports, traces, logs, and diagnostics even
when the campaign fails. A separate job replays and merges the uploaded shard
corpora, deduplicates them, and saves a bounded working corpus for the next run.
The CLI chooses its compatibility authority by exact replay with the current
runner, preferring fresh histories. It deduplicates bytes before validation and
replays each unique compatible candidate once, streaming retained bytes to disk
before opening the next candidate. `validation.json` identifies the current
input and cumulative replay/byte cost. The default validation budget is eight
million recorded transitions (`corpus-merge --max-replay-transitions`); the
wrapper also enforces an independent 110-minute wall-clock budget.
Duplicate-only campaigns can use a replayed seed; divergent candidates remain
inputs for quarantine instead of aborting the merge before valid histories are
retained. If no candidate replays, the job
fails with an authority-selection log and leaves the uploaded shard traces
available for diagnosis without publishing a working corpus.
The working corpus keeps up to 128 clean traces for standby/Raft, eight for
distributed data, and two for production standby/scaling, plus the smallest retained
representative of every distinct failure fingerprint. `retention.json` records
that selection; the full merged corpus remains in the uploaded artifact.
Incompatible versions are quarantined for review; unexpected replay errors or
replay divergence fail the run and preserve evidence. A still-reproducing
finding in the initial corpus also fails the campaign. Campaign timeouts leave
room for artifact upload; interrupted runs never acquire a stale success report.
The wrapper gives campaign subprocesses 230 minutes, sends TERM on cancellation
or timeout, then kills the process group after a 30-second grace period and
reaps the child. Each history writes a small phase report, and its generated
trace is saved atomically before exact replay starts. Small reports and logs
upload separately before large traces.
Only a completed validation manifest can publish a working corpus.

An exhausted history remains a failing soak. standby/scaling records it explicitly
as `transition-budget-exhausted`; this does not by itself diagnose starvation
or a production deadlock. Its cleanup property runs after cancellation and
owner release and checks remaining tasks, file handles, sockets, queued
executor work and transport closes. A partial history can no longer pass
cleanup merely because it did not complete. Cutoff logs name the active
operation and wait owner, plus task dependencies and actual sleep deadlines
with their clock domains. `campaign --scenario standby-scaling --transitions N`
can target startup and intermediate cancellation boundaries explicitly.

`scripts/ci/zig_vopr_qualification.py` runs a small real campaign, validates and
retains it, copies the corpus into a new run, and requires the second campaign
to consume compatible entries. It also checks duplicates, incompatible and
malformed traces, replay divergence, rejection without a valid authority,
validation-budget exhaustion and exact replay of cleanup at transition budgets
1, 2, 4, 8, 16, 32 and 8,192. The larger cut cancels active Raft/HTTP owners,
timers and external waits as well as the early deployment-admission boundaries.
This gate runs in PR CI and before scheduled soaks. Full operational
qualification requires two successful default-budget Linux workflow runs; the
second dispatch must set `require_seed=true` and report nonzero consumed seeds
for every shard. A local gate alone is not full-soak evidence.

The same CI gate runs the production Raft transport tests and the determinism
audit.

> **Relocated:** The timer/deadline defect narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/full-cluster-scenario-narratives.md](../docs/design/vopr/history/full-cluster-scenario-narratives.md#timer-and-deadline-defect-narrative-originally-lines-15791607). The resulting borrowed-clock rules are under [`VoprIo` Runtime](#voprio-runtime).

The executable, run and merged-corpus artifacts are retained for 90 days.
Fiber callsite identities are scoped to a pinned executable layout; keep the
original executable when investigating older traces. The diagnostic dispatch
accepts `diagnostic_run`, `diagnostic_shard` and `diagnostic_trace`, downloads
that run's executable and trace, and captures native Linux operation boundaries
and suspended owner stacks without rebuilding the runner. Version 2 is inspected
before finalization releases owners; older executables are inspected at deinit.
Its output is
diagnostic replay evidence, not a new soak qualification.

The cache is an acceleration/resumption mechanism and can be evicted; download the retained
corpus artifact to resume manually after eviction. Scheduling a workflow does
not establish that a nightly budget has completed: use its uploaded `run.json`,
`results.json`, and corpus `index.json` as evidence. Replaying a single trace
requires only that trace and a compatible runner; repeating corpus-guided
search requires the initial corpus, seed, budget, and runner revision.

```sh
zig build vopr-build -Doptimize=safe -j1
python3 ../scripts/ci/zig_vopr_soak.py --binary zig-out/bin/vopr run \
  --scenario standby-scaling --seed 0xa17f5500 --histories 2 \
  --corpus /tmp/previous-vopr-corpus --output /tmp/new-vopr-run
python3 ../scripts/ci/zig_vopr_soak.py --binary zig-out/bin/vopr merge \
  --inputs /tmp/new-vopr-run --output /tmp/merged-vopr-corpus
```

> **Relocated:** The soak-investigation defect narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/follow-ups-2026-09.md](../docs/design/vopr/history/follow-ups-2026-09.md). Durable invariants from it are in the Raft and Metadata and Distributed Data sections above.

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

> **Relocated:** The nine dated Follow-up subsections (2026-09-06/07, 617 lines) that previously lived here are preserved verbatim in [docs/design/vopr/history/follow-ups-2026-09.md](../docs/design/vopr/history/follow-ups-2026-09.md).

### Current Answer: Coverage, Parity, and Completeness

There are still valuable VOPR tests and Antithesis-class features to add; the
complete roadmap is not implemented. Fixed findings have focused regressions,
which does not certify the remaining roadmap or every production callback
boundary. The highest-value work is composing more production owners,
workflows, and fault domains in the same replayable history.

| Question | Current answer | Highest-value next work |
| --- | --- | --- |
| Where should Antfly add VOPR testing? | At production orchestration boundaries that combine durable state, ownership, public visibility, and recovery | Deepen the production-owner cluster along the [Distributed Completion Audit](#distributed-completion-audit) rows, then compose metadata administration, MCP/A2A, cloud authentication, extension invocation, and live credential/provider replacement |
| Which Antithesis ideas remain worth porting locally? | The large engine features, saved event-set programs, bounded live streams, and reversible service rates are implemented at the registered in-process boundary | Transitive determinism auditing and broader packet-level replay coverage; v53 completion; nightly sharding, retention, quarantine review, notifications, and dashboards; compiler coverage as guidance when Zig instrumentation is stable |
| Is distributed VOPR missing? | **Partly.** In-process application-level distributed VOPR (logical nodes, directional links, process/storage/resource domains, overlapping faults, quiet suffixes, exact replay) is integrated | Separate-address-space orchestration is not implemented, and whole-deployment breadth is incomplete (co-resident standby/data-plane/serverless, cross-domain disk pressure, broader socket/storage/process/restart overlap, live mixed binaries) |
| Are the features called finished actually finished? | Only within each narrowly stated **integrated** seam and its named green replay gate | Do not infer aggregate health, transitive call-graph determinism, every cross-domain combination, arbitrary native/container determinism, or Antithesis product parity |

At the last checkpoint the reusable engine and registered-source audit were
green, as were the cited v13–v40 and v42–v52 production-owner gates. V53 has
only a bounded-lifecycle gate. The repository-wide `vopr-test` aggregate is not
cited as green, the v12 deep result is from its own earlier checkpoint, and a
source-manifest audit is not a proof over every transitive production callee.
These limitations are part of the completion claim, not footnotes to it.

### Verification Audit and Meaning of "Finished"

> **Relocated:** The dated 2026-08-26 verification-audit narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/status-history.md](../docs/design/vopr/history/status-history.md).

Production-owner gates, budgets, and the aggregate
`production-cluster-vopr-test` are listed once in
[Full-Cluster Scenario Versions](#full-cluster-scenario-versions).
`production-cluster-vopr-smoke-test` (v11 plus v12's bounded cutoff) is part of
ordinary `vopr-test`; the deeper histories are named tiers so a
multi-gigabyte, tens-of-minutes witness stays out of the default edit loop
without treating an early cutoff as completion evidence.

> **Relocated:** The per-version gate-tier narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/full-cluster-scenario-narratives.md](../docs/design/vopr/history/full-cluster-scenario-narratives.md#production-owner-gate-tiers-by-version-originally-lines-17291808).

> **Relocated:** The dated checkpoint-recap narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/status-history.md](../docs/design/vopr/history/status-history.md).

Completion labels in the tables below use the three-level scale defined
under Conformance Status. A row may narrow an **Integrated** claim with a
scope qualifier such as "foundation" (the reusable engine/runtime capability
and its focused gate) or "at the named seam" (the listed production path and
fault modes only, excluding residual work named in the same row). **Partially
integrated** is the Executable-foundation level: focused production seams
exist but are not yet composed through the whole public/deployment path.
**Ongoing** and **conditional** are not completion claims.

Therefore the features labeled integrated below are implemented to their stated
boundaries, but the complete roadmap is not finished. In particular, local
run/index/report tooling and scheduled corpus-retaining campaigns are implemented,
while completed scheduled runs, notifications, and dashboards remain operational
follow-up; distributed VOPR is an
implemented runtime foundation with incomplete composition breadth; and the
event-query layer, saved cross-run set algebra, validation/counting commands,
and bounded live stream are implemented while routine quarantine review and
broader operational integration remain future work.

The word **fully** is consequently never implicit. The conformance audit is:

| Claim class | Audit result | Explicit exclusion |
| --- | --- | --- |
| Named VOPR engine/tooling features | **Implemented at the registered in-process `std.Io` boundary.** `vopr-engine-test` and the registered-source `vopr-determinism-audit` pass at this checkpoint | The audit manifest is not a transitive production call-graph proof; arbitrary guest-kernel RNG/syscall interception, uninstrumented native libraries, and separate process address spaces are also excluded |
| Rows labeled integrated | **Implemented for the production seam, schedules, properties, and exact-replay gate named in that row** | Residual work stated in the row and combinations with other independently tested domains |
| Rows labeled partially integrated | **Not complete end to end** | Promotion requires the remaining public/deployment composition and its replay gate |
| Local results/index/corpus tooling | **Implemented as repository-owned commands, formats, and a scheduled sharded workflow with replay-validated corpus retention** | Completed scheduled-run evidence, notifications, dashboards, and routine quarantine review |
| Antithesis parity | **Not claimed** | Hosted orchestration/UI, deterministic execution of arbitrary containers or kernels, and operational service parity |

This is the answer to “have we fully implemented what we call finished?”: only
for a narrowly stated integrated contract whose named focused gate has passed
at the cited checkpoint. No such label applies to the overall roadmap,
Antithesis product parity, residual work named beside a contract, or any
extension beyond the named green seams. A changed tree must rerun the
named gate before carrying the claim forward; documentation is not evidence. A
broader sentence must not erase those boundaries.

> **Relocated:** The dated v13-v18 defect-and-repair narrative that previously lived here is preserved verbatim in [docs/design/vopr/history/status-history.md](../docs/design/vopr/history/status-history.md).

Standby, per-group Raft, LSM/WAL/LMDB/persistent/index-manager/DB-split, metadata
distributed data, the deployment-shaped full cluster, and the newer P0/P1/P2
boundary suites are implemented at the exact production seams and modes stated
in their conformance rows. “Integrated” is not upgraded to “fully implemented
everywhere,” and a row whose focused or aggregate gate regresses must be
downgraded or repaired rather than defended by this document.

That does not make the roadmap empty or make the system equivalent to the
Antithesis hypervisor. Items marked **ongoing** or **conditional**, and residual
boundaries named in an integrated row, are not finished; live mixed-binary
operation is not modeled; and arbitrary unmodified sidecars or process address
spaces remain differential/integration concerns.

> **Relocated:** The superseded v13–v18 completion recap that previously lived here is preserved verbatim in [docs/design/vopr/history/full-cluster-scenario-narratives.md](../docs/design/vopr/history/full-cluster-scenario-narratives.md#v13v18-completion-recap-originally-lines-18581890).

### Distributed Completion Audit

This audit keeps a focused seam from being mistaken for a finished
whole-deployment campaign. Antithesis runs Docker Compose or
[Kubernetes](https://antithesis.com/docs/setup/kubernetes/) topologies and
[scopes faults to containers or
pods](https://antithesis.com/docs/product/writing_tests/controlling_faults/fault_types/);
VOPR implements the analogous application-level fault domains inside a
registered `std.Io` world. Versions refer to
[Full-Cluster Scenario Versions](#full-cluster-scenario-versions).

| Requirement | Current status | Remaining work |
| --- | --- | --- |
| Deterministic multi-node runtime, clocks, links, storage, restart, resources, replay, and quiet suffix | **Integrated foundation.** The deployment composer registers node/role/domain/fault/quiet obligations; metadata, Raft, standby, transaction, data-plane, and full-cluster gates exercise complementary real owners | Adopt the manifest in the remaining distributed suites; keep fail-closed audits as owners appear |
| Metadata quorum, production `DataServer` replicas, public clients, and real HTTP/Raft transport in one history | **Integrated at v11–v52; v53 is bounded evidence.** V9 remains the hosted/public campaign, and `data-server-transition-vopr-test` independently proves replicated merge-to-split | Complete v53. Add remaining topology breadth; managed-index inner-publication and disk-pressure faults; graph/query cancellation under disk pressure; broader disk/socket/short-write and cache topology/link/storage/resource overlap; additional replication topology/cancellation/source/target-crash timings and metadata leadership loss during cutover; cancellation under storage/process and richer multi-fault combinations; disjoint placement; retained-history paging; snapshot/derived-state rehydration; partitions; coordinator/metadata or multi-owner loss |
| Serverless worker output through its production public catalog | **Integrated at the stated seam** (worker, durable lease, object stores, catalog service, HTTP listener, public client on one `VoprIo`; v50 adds the progress-conflict race beside cluster work). The serverless object and metadata placement catalogs stay distinct | Overlap lease/object-store failures with metadata topology and node-resource faults; multi-worker placement when production owns that topology |
| Standby, data-plane, metadata, public API, and serverless owners all co-resident | **Ongoing.** Each domain has an integrated exact-replay suite; they do not yet coexist in one history | One bounded deployment composition with a cluster-wide recovery oracle, without duplicating business logic |
| Public distributed graph request | **Partially integrated:** v13–v18, v21–v22, v24–v25, v27–v29 | Broader restart/topology and request/response/Raft short-write faults, disk pressure, broader socket and storage/process/restart overlaps, cancellation under resource/storage/process/restart and multi-fault combinations |
| Distributed joins and global queries | **Partially integrated:** joins v19–v20, v30–v35; global query v36–v40; focused composed-query tests cover more result assembly | Join cancellation under storage faults, disk pressure, or process loss before drain; auth and stale-generation changes; right/nested/foreign and multi-range-left joins; global-query topology, storage, resource, coordinator/metadata, and multi-process loss |
| Query cache, replication backfill, and service rates | **Integrated focused seams plus deployment composition:** v23, v42–v48, v49 | Cache topology/link/storage/resource overlap; more replication timings; metadata leadership loss during cutover; broader link/storage/resource/restart combinations |
| Generation/reranking provider replacement and fallback | **Integrated local and remote production seams** (`generation-reranking-vopr-test`) | Model execution and GPU kernels stay differential; compose provider faults into a deployment only when a product workflow owns that routing |
| Multi-table/tenant/resource/mixed-version breadth | **Partial/conditional.** Two-table isolation, v51 authenticated table identities, node-memory interference, v22 listener-socket denial, v52 disk denial, and focused disk/socket quota and upgrade-artifact suites are integrated | Row-level tenant scoping and identity mutation races; managed-index and cross-domain disk pressure; broader socket interference; live mixed-version nodes remain conditional on runnable compatible binaries |

“Distributed VOPR is integrated” therefore means the runtime foundation and
named seams are real and replay-proven, not that whole-deployment composition
is complete.

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
standby seed backup/restore workflow uses the same provider: a committed chunk with
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

The capability table under Conformance Status names each module and gate. The
contracts that matter to users of the tooling:

- **Flight recording.** Every explored history has a bounded structured
  recorder; parallel campaigns exact-replay each candidate through it and
  materialize it only for corpus insertion or failure. Verbose details and
  name/value fields never enter canonical trace bytes.
- **Event queries and saved sets.** Fielded queries select kind, name, actor,
  resource, fault phase, transition and logical-time windows, `preceded_by`,
  `followed_by`, and same-actor/resource correlation. `vopr-event-set-v1`
  plans compose those selectors with set algebra, distinct/first/last moment,
  previous/next, and bounded sequence operators as a forward-only validated
  DAG with explicit result and match limits:

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

  `vopr events --query query.json --validate` checks schema and DAG without a
  trace; repeated `--trace` evaluates one plan across runs; `--count` omits
  match materialization. There is no parser for the earlier ad-hoc
  single-selector input.
- **Results and index.** `vopr-results` emits `vopr-results-v1` JSON and an
  optional static HTML report (run metadata, budgets, property results,
  first-failure and rare-success evidence, never-encountered properties,
  corpus, quarantine, artifacts); parallel campaigns add aggregate
  `vopr-run-results-v1`. `vopr-index` merges either form into an atomically
  persisted `vopr-run-index-v1` and answers `vopr-run-index-query-v1`.
- **Debug recipes and corpus merge.** Every new failure fingerprint gets one
  recipe: same-fingerprint reduction, causal window, bounded counterfactuals,
  event queries, before/after collectors, and the reduced replay artifact.
  `vopr-corpus-merge` exact-replays candidates, quarantines incompatible or
  divergent bytes, and publishes a deterministic manifest only after
  referenced artifacts are written.
- **Faults and commands.** Fault definitions encode precedence, overlap, and
  exclusion groups, applied to real `VoprIo` effects by `fault_vopr_io.zig`.
  Production loops opt into service-rate charging only at reviewed
  boundaries. The runner audits every choice record for typed, immediate
  selection rather than delayed seed interpretation.
- **Determinism gate.** Rejects host entropy, delayed private PRNGs, host
  clocks, native threads/`Threaded` I/O, host filesystem access, native
  libraries, unordered map iteration, and pointer-derived identities in
  replayable adapters; narrow differential boundaries need a line-local
  categorized rationale.
- **Health.** Default report health covers no progress/deadlock, unexpected
  crash, task and descriptor leaks, allocator/storage exhaustion, eventual
  recovery, final consistency, cleanup, replay divergence, and harness errors,
  sampled in continuous, recovery/quiescent, and final phases outside
  canonical bytes.
- **Search quality.** `vopr-benchmark` runs three injected defects under
  random, guided, spliced, starvation, and checkpoint-assisted policies;
  `vopr-search-quality-v2` reports recurrence, rarity, Wilson 95% bounds,
  logical cost, and smallest witnesses.
- **Storage faults.** The virtual filesystem supports one-shot read
  corruption, durable sector corruption and clearing, and torn
  synchronization that keeps only a selected durable prefix.
- The generic datagram model has no production consumer yet; add a campaign
  with the first real consumer rather than a test-only one. Add source/basic-
  block coverage as secondary guidance only when Zig instrumentation can stay
  outside the replay ABI.
### Priority Test Roadmap

Integrated focused suites (each exact-replays its production seam; the
capability table and the sections above give detail):

| Priority | Area | Gate and remaining breadth |
| --- | --- | --- |
| P0 | Replication backfill and rebalancing | `replication-backfill-vopr-test`; deployment modes v42–v48. More topology/cancellation/source/target-crash timings, metadata leadership loss during cutover, overlapping faults |
| P0 | Standalone and serverless supervision | `supervision-vopr-test` |
| P0 | User and authentication lifecycle | `auth-lifecycle-vopr-test` |
| P0 | Query-embedding cache | `query-embedding-cache-vopr-test` (v2 adds the cost port); deployment mode v49. Cache topology/link/storage/resource overlap |
| P0 | Full-cluster distributed composition | `full-cluster-vopr-test`, `production-cluster-vopr-test`; see the [Distributed Completion Audit](#distributed-completion-audit) |
| P1 | Complete serverless workflow | `serverless-workflow-vopr-test` (v5 adds the combined stale-enricher/progress-conflict history); deployment mode v50. Cross-domain object-store/process/resource overlap, multi-worker placement |
| P1 | DB and index request races | `db-index-race-vopr-test` |
| P1 | Generation and reranking chains | `generation-reranking-vopr-test` |
| P1 | Persistent Parquet cache | `parquet-cache-vopr-test` |
| P1 | Provisioning and startup | `provisioning-startup-vopr-test` |
| P1 | External lake | `external-lake-vopr-test` (twelve modes) |
| P2 | Multi-table and authenticated tenants | Full-cluster two-table workload and v51. Row-level multitenancy and identity mutation during a request |
| P2 | Resource interference | `admission-vopr-test`; deployment modes v18, v22, v52. Managed-index disk pressure, broader socket targets, disk pressure overlapped with cancellation/link/storage/restart/process faults |
| P2 | Provider boundaries, composed queries, media runtime, upgrade compatibility | `provider-boundary-vopr-test`, `composed-query-vopr-test`, `media-runtime-vopr-test`, `upgrade-compatibility-vopr-test` |

Partial, ongoing, and conditional work:

| Priority | Area | What remains |
| --- | --- | --- |
| P1 partial | Managed-index publication and public readiness | Complete v53 (see its row), then atomic versus progressive publication policy, cancellation, disk/rate-limit overlap, and crashes inside each durable-generation/catalog/alias/readiness gap |
| P1 partial | Distributed graph/public-query boundaries | Rows in the Distributed Completion Audit |
| P0 ongoing | Repository-wide strong-read contract | `ReadIndexRequester` (enqueue-only) and `ReadSafetyBarrier` (synchronous) are distinct capabilities; replicated DataServer starts with `unavailableReadSafetyBarrier`, installs the real barrier after Raft wiring, and marks direct non-Raft ownership `alreadyReadSafeBarrier`. Audit every custom callback barrier and add the same behavioral depth for each distinct production owner before another public source relies on it |
| P1 ongoing | Remote-content credential use boundary | Resolve access key, secret, session token, and header references immediately before a real scraping or object-fetch request; rotate mid-request; retry and cancel on borrowed `std.Io`; prove each request observes one coherent secret generation. `lib/scraping` still copies credential strings at its request-construction seam |
| P2 conditional | Live mixed-version workload | Only once two compatible runnable versions and an upgrade contract exist; `upgrade-compatibility-vopr-test` covers artifacts, not live mixed binaries |

The source-boundary audit also identifies four independent candidates; they
are not prerequisites for the integrated rows above:

| Priority | Area | Why it is still useful |
| --- | --- | --- |
| P1 | Production metadata-admin/control path | Drive the actual `MetadataService`/`MetadataHttpService` mutation path—including reallocation timestamps, leadership change, ambiguous admission, status reporting, and split/merge requests—over borrowed clocks and real metadata HTTP. Some HTTP simulations intentionally use a harness source instead of the complete production service owner. |
| P1 | MCP and A2A session/task state machines | Compose production session expiry, event-id replay, task reservation/generation fencing, cancellation races, bounded capacity, SSE disconnect/reconnect, and shutdown on `VoprIo`. Both libraries already accept caller-owned `std.Io`; the missing value is an Antfly-level public orchestration history. |
| P2 | Cloud authentication and signed object requests | Put Google token refresh and S3/GCS signing in front of the deterministic object-store response adapters: credential rotation, refresh collapse, clock skew, timeout-after-send, retry, cancellation, and stale-token rejection. |
| P2 | Extension invocation lifecycle | Extend extension install/configuration/startup histories through a real bounded Wasm invocation: concurrent configuration replacement, host-call cancellation, fuel/memory admission, trap, shutdown, and durable restart. Keep Wasmtime itself differential. |

### Integrated Targeted Suites

These are new suites or compositions, not retroactive dependencies of the
integrated rows above.

| Priority | Area | Gate and coverage |
| --- | --- | --- |
| P0 | Generation publication and cleanup | `generation-lifecycle-vopr-test`: clean publication, prepared rollback, rename retry, uncertain directory sync, prepared crash recovery, reader/publisher locking, canonical aliases, stale-generation cleanup; restore and standby materialization propagate the same I/O |
| P0 | Metadata backfill-marker discovery | `backfill-marker-discovery-vopr-test`: absent, legacy, valid-owned, corrupt, ownership-mismatch, throttled appearance, rescan, and read-fault/restart on borrowed filesystem and monotonic clock |
| P0 | Configuration, secrets, remote content, and extensions | `config-extension-lifecycle-vopr-test`: cold starts, secret rotation with retained readers, crash between secret and configuration publication, remote-content replacement/rollback, extension install/replace/disable/configure, malformed packages, failed Wasm startup. Snapshots keep live `${secret:...}` references |
| P1 | Embedded, C API, and Lite lifecycle | `embedded-lite-lifecycle-vopr-test`, including a physical-versus-`VoprIo` differential of logical values and checkpoint sequences |
| P1 | Cross-service resource pressure | `admission-vopr-test` (eight schedules); `resource-budget-test` guards the lake-queue default mapping |

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
11. versioned saved event-set programs with fail-closed DAG validation and
    bounded cross-history evaluation, plus a runner-owned bounded live NDJSON
    stream that cannot block or perturb replay (see the capability table);
    and
12. a reversible service-rate model with stable node and operation identities
    and per-node/per-operation accounting, adopted at six production
    boundaries (see the capability table).

These capabilities are local libraries, commands, reports, and CI gates. They
do not require an Antithesis account or hosted runtime. This is not a claim of
full Antithesis product parity: the implemented claim is limited to the named
self-contained engine features and application-level distributed fault domains,
while separate-address-space determinism and the operational work below remain
explicitly unfinished or conditional.

The remaining Antithesis-class opportunities are narrower than the engine
work already completed:

- extend the v11–v53 cluster compositions with search policies that overlap
  node-wide and operation-class slowdowns with remaining topology/ownership
  changes, the existing link/storage/resource/restart algebra, additional
  replication timings, metadata leadership loss during cutover, and quiet
  recovery. Arbitrary instruction-level native-thread throttling remains a
  federated/native instrumentation concern rather than an in-process claim;
- grow the saved event-set expression vocabulary only in response to concrete
  debugger/report needs. The versioned plan deliberately provides typed set,
  moment, previous/next, and sequence operations instead of an embedded
  general-purpose map/fold language; reports can add reviewed aggregations
  without making arbitrary code part of a saved query;
- delta-encode or intern repeated canonical observations in long histories.
  Exact replay must still compare every semantic observation, but a deep Debug
  campaign should not require multi-gigabyte retention merely because most
  adjacent states repeat. This is artifact/runtime efficiency work, not a
  relaxation of replay truth or a substitute for the bounded flight recorder;
- operate the scheduled sharded corpus-retaining workflow, review its quarantined
  traces, and add usage indexing and notifications;
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

> **Relocated:** The version-by-version roadmap narrative that previously lived here (445 lines) is
> preserved verbatim in [docs/design/vopr/history/status-history.md](../docs/design/vopr/history/status-history.md#ongoing-roadmap-narrative-relocated-from-voprmd).
> Durable decisions from it are folded into the compact roadmap below. The former "shortest current
> summary" and "detailed backlog" lists narrated the same seams in the same order, so they are merged
> into one list here.

1. **Deepen the promoted full-cluster reconfiguration seam.** The seam exact-replays a three-owner
   merge-to-split reconfiguration behind the real metadata quorum, three `DataServer`/data-Raft
   owners, public clients, two tables, and the serverless catalog, with predecessor-fenced sparse
   deltas, durable destination ranges, and restart-safe reconciliation. Composed on that owner set: a
   depth-two public graph, owner transport/process/short-write faults, memory pressure, listener
   connection-limit denial, hydration/durable-shuffle cancellation, partition-worker failover,
   distributed joins under cancellation/authorization/transport faults, a shared query-cache domain,
   replication under schema/leader/lease faults, and a bounded managed-index lifecycle.
   - Complete the managed-index lifecycle: provider retry, coverage/replay publication, owner
     reconstruction, all-node semantic recovery, and packet-level replay stabilization.
   - Add cancellation under storage/disk faults and simultaneous process loss before cancellation
     drain; cover the remaining topology, restart, short-write, and socket-pressure breadth.
   - Remove the co-location assumption (test disjoint donor/receiver replica sets); page retained
     delete-history replay within a resource budget; inject partitions.
   - Prove snapshot install rehydrates each live DB owner's derived graph/index state and the Raft
     projection.

2. **Deepen public distributed operations and cross-range graph composition.** The public request
   path composes cross-owner joins during active splits, finalizer takeover, document hydration with
   cancellation, permission revocation mid-request, split publication behind a retained snapshot,
   transport-outage and durable-shuffle cancellation, partition-worker failover, process
   destruction/reconstruction, an ordered two-table global-query baseline, and the first managed-index
   progressive-readiness lifecycle — all with typed no-partial failure and exact recovery.
   - Add cancellation under storage/disk faults combined with process loss and other combinations;
     extend joins to right/nested/foreign and multi-range-left shapes; add global queries under the
     same fail-closed rule with broader topology and process-loss histories.
   - Introduce an explicit partial-response schema only if product semantics ever require it.
   - Complete and stabilize the managed-index pending-to-ready path, then extend it across
     atomic-vs-progressive policy, rate-limit/disk/cancellation overlap, alias fencing, and
     publication-gap crashes.
   - Generalize the synchronous applied-index/derived-visibility read barrier to every production
     strong-read owner with a distinct blocking boundary; every incomplete fanout must keep failing
     closed as this expands.

3. **Compose independently proven fault domains, including standby and the data plane.** Already composed:
   link-plus-resource overlap under one quiet-suffix oracle, selected-node disk-capacity
   denial/healing at the persistent-cache reservation consumer, reversible operation costs as a fault
   distinct from CPU-work exhaustion, a shared query-cache domain, replication under
   schema/target/session/lease/checkpoint faults, generation-cutover CAS races, and two-tenant
   concurrent work with exact cross-tenant denial evidence.
   - Co-locate standby and the data plane; add managed-index and graph/query disk-pressure combinations;
     broaden listener-pressure targets to disk-capacity and wider socket surfaces.
   - Extend the fault algebra across storage, restart/process, serverless lease/object-store, and
     multi-owner faults; overlap operation-specific slowdown with cross-domain ownership and public
     deadlines.
   - Broaden fault combinations at the already-complete named seams (metadata/placement Raft wire hop,
     serverless public catalog path, memory-pressure recovery, quiet cluster-wide suffix) rather than
     rebuilding them.

4. **Broaden workloads at those same seams.** Two disjoint authenticated table identities already run
   concurrent authorized work with exact bidirectional denied-read, denied-write, and absence
   evidence.
   - Add row-level tenant scoping and identity mutation races, concurrent range-split/replicated-merge
     routing changes, and fairness between clients and background workers.
   - Extend per-node disk-capacity and socket interference beyond current memory/listener pressure.
   - Add remote generation/reranking adapters, metadata-admin mutations, MCP/A2A orchestration,
     cloud-auth refresh/signing, and bounded extension invocation.
   - Close the remote-content live-secret boundary: resolve preserved credential/header references at
     the actual request, rotate mid-request, and prove coherent per-request generations across
     retry/cancellation/refresh/crash-reopen.

5. **Operationalize and maintain the self-contained platform tooling.** The command composer,
   determinism audit, phased health adapters, recorder/event queries, debug recipes, results/index
   APIs, corpus merge, and injected-bug benchmarks are implemented and produce recurrence/rarity
   evidence. A suite counts as integrated only while its scenario module is forced into test
   discovery, its command runs at least one passing exact-replay test, and it remains a `vopr-test`
   dependency.
   - Gather evidence from the scheduled sharded campaigns and replay-validated corpus retention; add
     quarantine review, usage indexing, notifications, dashboards, and search-quality regression
     tracking; wire recurrence/rarity reports into nightly retention.
   - Adopt the registered-deployment composer beyond full-cluster in the standby, data-plane,
     distributed-transaction, and serverless suites so node identity, readiness, fault scope, and
     quiet-suffix obligations stay uniform.

6. **Expand the determinism audit toward full transitive coverage.** The explicit source-manifest gate
   is real and green, but transitive coverage of the production call graph reachable from every
   borrowed-`std.Io` scenario seam is not complete. Optional/default `Threaded` ownership (A2A/MCP
   orchestration, cloud-auth and object-store constructors, remote-provider adapters) must receive the
   scenario's borrowed `std.Io` before joining an exact-replay composition; native fallbacks remain
   physical differential paths, not VOPR evidence. Source-to-source cache transfer and stale-cache
   pruning still use address-ordered dual locking in production paths the current topology does not
   exercise.
   - Expand toward the transitive call graph continuously as sources are exported; fail closed on
     pointer-derived ordering/identity, host clocks, native threads/I/O, filesystem escapes, native
     libraries, and unordered iteration unless a narrow reviewed exception applies; preserve
     Threaded/physical-backend differential tests to detect simulator drift.
   - Before the cache-transfer/pruning paths become replayable, give each owner a stable semantic lock
     key or route both operations through one coordinator; do not simply reverse lock order.

7. **Keep distributed fidelity explicit.** The integrated in-process multi-node mode remains the
   vehicle for production owners that borrow `std.Io`; unmodified container runs are classified as
   differential, never exact replay.
   - Add live rolling mixed-version operation only once two runnable compatible binaries and an
     explicit upgrade contract exist; current artifact-compatibility and golden-reader campaigns are
     the prerequisite, not a claim of live mixed-binary execution.
   - Add a federated agent/broker only for a demonstrated separate-address-space or mixed-binary
     requirement (see Conditional Work above for the other gated additions). Do not build a hosted
     graphical debugger or a deterministic-hypervisor clone for nominal parity alone.

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
11. Preserve existing standby, Raft, LSM, storage, integration, and formal tests as
    independent complementary gates.
12. Require human review before fixture promotion.
13. Evolve VOPR-native APIs, modes, trace revisions, saved-query formats, and
    properties forward-only. VOPR is new code, so do not add aliases, legacy
    readers, migrations, or fallback parsers; product-data compatibility stays
    in its separate explicit campaigns.

## Conclusion

Antfly owns the self-contained foundation: explicit deterministic choices,
one-transition scheduling, a virtual OS behind `std.Io`, guided exploration,
exact replay, reduction, formal export, counterfactual analysis, and
production-shaped scenarios. Rows marked **integrated** hold at their stated
seams; partial, ongoing, and conditional rows remain future work.

> **Relocated:** The dated v11-v18 completion recap that previously lived here is preserved verbatim in [docs/design/vopr/history/status-history.md](../docs/design/vopr/history/status-history.md).

New product services should cross the existing `std.Io`, lifecycle-hook,
admission, and owner seams rather than creating native-only loops or weakening
production ownership contracts. If a future defect requires separate processes
or live mixed binaries, the next fidelity step is the federated VOPR
agent/broker described above.
