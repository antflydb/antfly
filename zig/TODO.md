# TODO

This is the live tracker for current bugs, active product work, and remaining
Go-parity gaps. Historical parity items that are already implemented are
intentionally omitted.

Use [ROADMAP.md](ROADMAP.md) for project-wide sequencing and
[DOCUMENTATION.md](DOCUMENTATION.md) for the full doc index.

## Current Bugs

No current release-blocking bugs are tracked here. Keep this section for live
failures that need owner-visible follow-up, not for historical CI incidents.

### Recently Cleared From The Live Bug List

These are no longer tracked as active bugs unless a new failure reappears:

- PR 145 generated OpenAPI/server drift from missing Go
  `ListArtifactEnrichments`.
- PR 145 document artifact manifest E2E visibility under
  `sync_level=full_index`.
- PR 145 transaction trace validation drift.
- Stateful lookup / full-text derived-index race.
- CDC distributed apply and projected status summary counters.
- API-only remote index status propagation.
- Backup/restore managed chunked semantic restore.
- Managed embedding pacing.
- Schema migration full-text rebuild.
- Stateless OCC conflict response mapping.
- Chunked full-text materialization.
- Data-raft multinode autoscaling split finalization.

## CI Coverage Shape

Current GitHub coverage is broader than the old PR smoke path:

- `Zig Tests` PR CI runs generated checks, SQL/API typed-plan parity,
  application-time temporal coverage, hermetic unit tests, TLA trace validation,
  and base Antfly/inference E2E when relevant files change.
- `Zig Tests` full-default runs on merge queue, `main` pushes, and manual
  dispatch. It covers unit, simulation, integration, recall, recall harness
  sweep, and chaos tests.
- Full Antfly and inference E2E are available through the `e2e-full`
  workflow-dispatch job.
- `Go`, `Python CI`, and `TypeScript CI` still exercise non-Zig product and
  tooling surfaces on PRs.
- Full OpenAPI codegen drift remains split by surface. The safe Zig check is
  `zig build openapi-root-check`; generated-source checks run through
  `make zig-generated-check`.

Open CI follow-up:

- [ ] Decide which E2E lanes should be required versus informational for normal
  PRs.
- [ ] Keep TLA trace validation stable enough that a single failing segment can
  be triaged without rerunning the whole suite manually.
- [ ] Keep generated OpenAPI checks aligned across Go and Zig when API routes
  move or new generated methods appear.

## Serverless Table Architecture

### Public Contract Alignment

- [ ] Keep the public contract table-first and shared across stateful and
  serverless where parity is intended:
  - [ ] same `/tables/...` surface
  - [ ] same request/response shapes from OpenAPI
  - [ ] same error semantics for unsupported vs unimplemented features
- [ ] Keep serverless-only deployment/runtime controls under `/_internal/...`
  instead of leaking provider-only knobs into the shared table API.
- [ ] Finish documenting which serverless reads are published-only vs
  latest/exact-read paths.
- [ ] Decide which contract features are intentionally deferred in serverless
  and expose those as explicit unsupported responses.

### Canonical Table State

- [ ] Make canonical table metadata the source of truth for both engines:
  - [ ] serverless should consume table-owned schema, `read_schema`, and index
    metadata
  - [ ] serverless policy/runtime state should stop implying index ownership
  - [ ] publication decisions should consume canonical table metadata snapshots
  - [ ] `buildStatus` / `TablePublicationState` should report
    table-definition-derived publication intent
- [ ] Keep table -> publication binding explicit throughout catalog, build, and
  query code.
- [ ] Rename remaining internal serverless layers away from namespace-first
  semantics once table/publication bindings are stable.

### Index Lifecycle And Publication

- [ ] Add the remaining public serverless index lifecycle parity:
  - [ ] richer index status during pending publication/rebuild windows
  - [ ] same-name index config update semantics
  - [ ] execution parity for schema-driven index version transitions
    (`read_schema` / `full_text_index_vN`)
- [ ] Define the conditions for clearing `read_schema` after publication catches
  up.
- [ ] Extend the planner from coarse families to concrete publication semantics:
  - [ ] distinguish head-republish-safe changes from materialization-only
    rebuilds
  - [ ] add explicit `chunk_embeddings` publication actions instead of
    inferring through dense-vector rebuilds
  - [ ] drive builder execution from per-index/per-version full-text actions
  - [ ] represent stored/document-field rebuild requirements separately from
    index-family rebuilds
- [ ] Move build/publish toward per-family and per-index artifact reuse:
  - [ ] document / stored fields
  - [ ] full-text per index/version
  - [ ] dense vector per named index
  - [ ] sparse per named index
  - [ ] graph per named index
  - [ ] chunk/enrichment outputs per stage/family
- [ ] Make metadata-only republishes cheap by construction.
- [ ] Reuse unaffected artifact refs across generations with explicit
  retention/GC ownership.

### Visibility And E2E Parity

- [ ] Make `TablePublicationState` explain planner state clearly:
  - [ ] publication reasons
  - [ ] artifact actions
  - [ ] derived-output actions
  - [ ] head-republish-safe vs waiting-on-materialization
- [ ] Add or keep current serverless parity E2Es for:
  - [ ] schema migration / `read_schema` visibility
  - [ ] metadata-only republish of graph/vector/full-text families
  - [ ] incremental publication reuse across generations
- [ ] Add operator-facing visibility for why publish is recommended, deferred,
  or waiting on enrichment/materialization.

## Stateful / Control Plane Follow-Up

- [ ] Strengthen restore/provisioning around shard/replica-owned bootstrap
  descriptors so split-runtime recovery does not rely on metadata-node-local
  assumptions.
- [ ] Make transient disappearing group/store handling explicit in metadata
  reconciliation and status output.
- [ ] Keep strong-sync graph coverage current across split, merge, and
  multi-node routed query paths.
- [ ] Keep automatic split/merge parity focused on externally visible table and
  range behavior rather than copying Go internals.
- [ ] Keep autoscaling E2E parity focused on high-level orchestration use cases:
  - [x] multi-metadata discovery with 3 metadata and 5 data nodes
  - [x] adding a data node and assigning placements to it
  - [x] draining, stopping, and finalizing a data node after replacement
  - [x] automatic shard split finalization from a configured size threshold
  - [x] node churn while routed reads remain available
  - [x] Raft-backed data writes and state-machine application for provisioned
    data nodes
- [ ] Broaden backup/restore parity beyond the current matrix where it still
  intersects public table semantics.

## Query, Search, And Retrieval Parity

The basic surfaces exist. The live work is depth, provider breadth, and keeping
the public contract covered while behavior evolves.

- [ ] Add parity coverage before introducing new public query/search API shapes.
- [ ] Keep OpenAPI and public docs aligned with actual Zig behavior as parity
  moves.
- [ ] Broaden quickstart-style query pipeline coverage:
  - [x] hybrid merge behavior
  - [x] pruning/reranking stages
  - [x] provider-backed query stages for current local/inference providers
  - [ ] multi-stage distributed service semantics
- [ ] Deepen foreign source and join coverage beyond the implemented
  transport/query paths:
  - [x] basic PostgreSQL foreign table query/filter/pagination coverage
  - [x] Antfly-to-Postgres joins
  - [x] nested foreign leaf joins
  - [x] CDC-backed foreign joins
  - [ ] broader routing/failure coverage for foreign joins across distributed
    topologies
- [ ] Broaden retrieval agent behavior:
  - [x] pipeline query and generation steps
  - [x] semantic and hybrid bounded retrieval
  - [x] tree-search coverage from seed hits and `$roots`
  - [x] JSON generation and fixed-body SSE streaming
  - [x] bounded classification/confidence/follow-up coverage
  - [ ] remote-content parity
  - [ ] broader provider matrix / built-in provider parity
  - [ ] session/conversation carry-forward semantics once JSON and SSE contracts
    are stable
- [ ] Keep graph query depth current as the distributed graph implementation
  grows beyond the current stateful/serverless graph pattern coverage.

## API, OpenAPI, And Config

- [ ] Fix generated route/server drift promptly when OpenAPI routes are added
  or renamed across Go and Zig.
- [ ] Keep the remaining dynamic join/runtime layer explicit and small.
- [ ] Push generated server-surface parity further where it buys real leverage,
  while keeping handwritten routing where behavior is still moving.
- [ ] Keep `openapi_contract.zig` as the bundled compatibility/codegen smoke
  test for stable contract slices.
- [ ] Extend `lib/jsonschema` with deeper semantics such as composition
  keywords and advanced constraints.
- [ ] Finish remaining common-config parity seams:
  - [ ] add typed speech-to-text provider/default handling where it makes sense
  - [ ] decide whether to preserve the remaining top-level validated-only fields
    as first-class Zig config state

## Resolved / Pruned Parity Notes

These old parity items were checked against the current tree and are no longer
tracked as open bring-up work:

- Query-builder API: implemented in `pkg/antfly/src/api/query_builder_agent.zig`
  with HTTP route/client/test coverage.
- TOON support: implemented under `lib/toon` and exposed through generated
  OpenAPI/template helpers.
- Full-text schema mapping: runtime schema, dynamic templates, analyzer
  binding, and rebuild coverage exist; new regressions should be tracked as
  concrete test failures.
- Basic foreign source, join, CDC, and retrieval transport: implemented enough
  that the live TODOs now track depth/status/coverage gaps rather than initial
  bring-up.
- Auth/UserMgr basic surface: implemented with focused API and E2E coverage;
  track only concrete current gaps.
