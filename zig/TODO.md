# TODO

This is the single live tracker for current bugs, active product work, and
remaining Go-parity gaps. Historical parity items that are already implemented
are intentionally omitted.

## Current Bugs

None currently tracked as active full-suite failures.

> **Relocated:** The dated 2026-05-11 test-run status and per-test
> resolved/superseded failure narrative (71 lines) that previously lived here
> is preserved verbatim in
> [work-log/completed/e2e/resolved-failures-2026-05.md](../work-log/completed/e2e/resolved-failures-2026-05.md).

### Resolved

- Stateful lookup / full-text derived index race: fixed by making lookup
  prefer live local writer DB leases, falling back only to lightweight
  primary/status opens, plus transaction torn-state conflict mapping and WAL
  directory retry handling.
- Stateless OCC conflict 500: fixed by mapping torn participant state to
  conflict responses.

## CI Coverage Gaps

- PR/push Zig CI runs focused serverless tests, ReleaseFast CLI build, and CLI
  smoke.
- Full `make test` is nightly/manual.
- Antfly Python E2E is not currently part of the GitHub workflows found in
  `.github/workflows`.
- Antfly inference standalone and Python E2E coverage is not part of normal PR CI.
- Full OpenAPI codegen drift (`make openapi-check`) is still not suitable for
  CI until all OpenAPI source specs are local to this repository. The safe check
  today is `zig build openapi-root-check`.

## Serverless Table Architecture

### Public Contract Alignment

- [ ] Keep the public contract table-first and shared across stateful and
  serverless where parity is intended:
  - [ ] same `/tables/...` surface
  - [ ] same request/response shapes from Go/OpenAPI
  - [ ] same error semantics for unsupported vs unimplemented features
- [ ] Keep serverless-only deployment/runtime controls under `/_internal/...`
  instead of leaking provider-only knobs into the shared `TableApi`.
- [ ] Finish documenting which serverless reads are published-only vs
  latest/exact-read paths.
- [ ] Decide which Go contract features are intentionally deferred in
  serverless and expose those as explicit unsupported responses.

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
- [ ] Add serverless parity E2Es for:
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
- [ ] Keep autoscaling E2E parity focused on Go's high-level orchestration use
  cases:
  - [x] multi-metadata discovery with 3 metadata and 5 data nodes
  - [x] adding a data node and assigning placements to it
  - [x] draining, stopping, and finalizing a data node after replacement
  - [x] automatic shard split finalization from a configured size threshold
  - [x] node churn while routed reads remain available
  - [x] Raft-backed data writes and state-machine application for provisioned data nodes
- [ ] Broaden backup/restore parity beyond the current matrix where it still
  intersects public table semantics.

## Query, Search, And Retrieval Parity

- [ ] Add parity coverage before introducing new public query/search API shapes.
- [ ] Keep OpenAPI and public docs aligned with actual Zig behavior as parity
  moves.
- [ ] Broaden quickstart-style query pipeline coverage:
  - [ ] hybrid merge behavior
  - [ ] pruning/reranking stages
  - [ ] provider-backed query stages
  - [ ] multi-stage distributed service semantics
- [ ] Deepen foreign source and join coverage beyond the currently implemented
  basic transport/query paths:
  - [ ] richer foreign query routing
  - [ ] distributed shuffle semantics where needed
  - [ ] CDC-backed foreign join depth
- [ ] Broaden retrieval agent behavior:
  - [ ] planner depth beyond the current bounded loop
  - [ ] deeper tree / RAG strategy coverage
  - [ ] remote-content parity
  - [ ] broader provider matrix / built-in provider parity
  - [ ] evaluation/reporting behavior
  - [ ] session/conversation carry-forward semantics once JSON and SSE contracts
    are stable
- [ ] Keep graph query depth current as the distributed graph implementation
  grows beyond the narrow v1 path.

## API, OpenAPI, And Config

- [ ] Keep the remaining dynamic join/runtime layer explicit and small.
- [ ] Push generated server-surface parity further where it buys real leverage,
  while keeping handwritten routing where behavior is still moving.
- [ ] Keep `openapi_contract.zig` as the bundled compatibility/codegen smoke
  test for stable contract slices.
- [ ] Extend `go/pkg/antfly/lib/jsonschema` with deeper semantics such as composition
  keywords and advanced constraints.
- [ ] Finish remaining common-config parity seams:
  - [ ] add typed speech-to-text provider/default handling where it makes sense
  - [ ] decide whether to preserve the remaining top-level validated-only fields
    as first-class Zig config state

## Agent And Protocol Parity

- [x] Implement MCP server support if Antfly should expose the Go MCP surface.
- [x] Decide whether A2A remains a product target; implement or explicitly mark
  unsupported once the API contract is settled.

## Pruned Stale Parity Items

These items from the former inference `PARITY.md` (removed; it tracked parity against the retired Go inference server) were checked against the current tree and are no
longer tracked as open bring-up work:

- Query-builder API: implemented in `pkg/antfly/src/api/query_builder_agent.zig`
  with HTTP route/client/test coverage.
- TOON support: implemented under `go/pkg/antfly/lib/toon` and exposed through generated
  OpenAPI/template helpers.
- Full-text schema mapping: runtime schema, dynamic template, and analyzer
  binding work is implemented; only the active migration rebuild bug remains.
- Basic foreign source, join, CDC, and retrieval transport: implemented enough
  that the live TODOs now track depth/status/coverage gaps rather than initial
  bring-up.
- Auth/UserMgr basic surface: omitted from parity TODOs until a concrete
  current gap is identified.
