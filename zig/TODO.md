# TODO

This is the single live tracker for current bugs, active product work, and
remaining Go-parity gaps. Historical parity items that are already implemented
are intentionally omitted.

## Current Bugs

Observed on 2026-05-11:

Latest full-suite status:

- Termite E2E is green:
  - command: `bash pkg/termite/scripts/debug_metal_command.sh command --timeout 1800 -- e2e/termite/.venv/bin/pytest -q -s e2e/termite`
  - result: `63 passed, 31 skipped in 825.24s (0:13:45)`
  - debug bundle: `pkg/termite/.debug/metal-command-20260510-195358`
- Antfly Python E2E is green:
  - command: `UV_CACHE_DIR=/tmp/uv-cache ANTFLY_E2E_PRESERVE_ROOT=1 uv run --project e2e/antfly pytest -q e2e/antfly`
  - result: `192 passed, 10 skipped in 1256.92s (0:20:56)`
  - note: the sandboxed Antfly run failed with localhost port bind permission
    errors and is not considered meaningful

### Antfly E2E Status

Status: green in the latest full Antfly run on 2026-05-11.

Latest full-suite result:

- command: `UV_CACHE_DIR=/tmp/uv-cache ANTFLY_E2E_PRESERVE_ROOT=1 uv run --project e2e/antfly pytest -q e2e/antfly`
- result: `192 passed, 10 skipped in 1256.92s (0:20:56)`

Focused verification after the lookup/transaction fixes:

- command: `UV_CACHE_DIR=/tmp/uv-cache ANTFLY_E2E_PRESERVE_ROOT=1 uv run --project e2e/antfly pytest -q -s e2e/antfly/test_transactions.py`
  - result: `21 passed in 145.58s (0:02:25)`
- command: `UV_CACHE_DIR=/tmp/uv-cache ANTFLY_E2E_PRESERVE_ROOT=1 uv run --project e2e/antfly pytest -q -s e2e/antfly/test_index_lifecycle.py`
  - result: `27 passed in 203.23s (0:03:23)`

Passing / skipped Antfly E2E areas in the latest full run:

- passed: `192`
- skipped: `10`
- the full CDC file is passing in the current Antfly suite
- previously failing backup/restore managed chunked semantic, managed
  embedding pacing, quickstart chunked semantic, and schema migration full-text
  rebuild cases are no longer failing in the latest full run

### Recently Resolved / Superseded E2E Failures

- Stateful lookup / full-text derived index race:
  - fixed by making lookup prefer live local writer DB leases and fall back only
    to lightweight primary/status opens, plus transaction torn-state conflict
    mapping and WAL directory retry handling
  - verified by focused transaction/index-lifecycle runs and the latest full
    Antfly suite
- CDC distributed apply and projected status summary counters:
  - current full Antfly run includes the CDC file passing
  - focused verification previously passed with `8 passed in 36.61s`
- API-only remote index status:
  - `test_non_host_api_reports_remote_index_status_from_metadata_heartbeat`
    is no longer a current full-suite failure
- Backup / restore managed chunked semantic restore:
  - no longer a current full-suite failure
- Managed embedding pacing:
  - no longer a current full-suite failure
- Schema migration full-text rebuild:
  - no longer a current full-suite failure
- Stateless OCC conflict 500:
  - transaction focused coverage and the latest full Antfly run are green after
    mapping torn participant state to conflict responses
- Chunked full-text materialization:
  - `test_mutable_table_chunker_full_text_index_persists_chunks` is no longer a
    current full-suite failure
- Data-raft multinode scaling:
  - `test_autoscaling_finalizes_shard_split_from_size_threshold` is no longer a
    current full-suite failure

## CI Coverage Gaps

- PR/push Zig CI runs focused serverless tests, ReleaseFast CLI build, and CLI
  smoke.
- Full `zig build test` is nightly/manual.
- Antfly Python E2E is not currently part of the GitHub workflows found in
  `.github/workflows`.
- Termite standalone and Python E2E coverage is not part of normal PR CI.
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
- [ ] Extend `lib/jsonschema` with deeper semantics such as composition
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

These old `PARITY.md` items were checked against the current tree and are no
longer tracked as open bring-up work:

- Query-builder API: implemented in `pkg/antfly/src/api/query_builder_agent.zig`
  with HTTP route/client/test coverage.
- TOON support: implemented under `lib/toon` and exposed through generated
  OpenAPI/template helpers.
- Full-text schema mapping: runtime schema, dynamic template, and analyzer
  binding work is implemented; only the active migration rebuild bug remains.
- Basic foreign source, join, CDC, and retrieval transport: implemented enough
  that the live TODOs now track depth/status/coverage gaps rather than initial
  bring-up.
- Auth/UserMgr basic surface: omitted from parity TODOs until a concrete
  current gap is identified.
