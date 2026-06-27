# Project Roadmap

This file is the top-level execution map for `antfly-zig`. Use it to answer:

- what the major project lanes are
- what order they should move in
- which detailed subsystem plan to open next

Use [TODO.md](TODO.md) for the live bug list and remaining parity gaps,
[DOCUMENTATION.md](DOCUMENTATION.md) for the documentation index, and
[README.md](README.md) for repository layout, build commands, and day-to-day
development notes.

Detailed execution belongs in subsystem docs:

- [TODO.md](TODO.md)
- [DOCUMENTATION.md](DOCUMENTATION.md)
- [BACKUPS.md](BACKUPS.md)
- [SERVERLESS.md](SERVERLESS.md)
- [STARTUP.md](STARTUP.md)
- [DB.md](DB.md)
- [BATCH.md](BATCH.md)
- [FULL_TEXT.md](FULL_TEXT.md)
- [QUERY_STRING.md](QUERY_STRING.md)
- [HBC.md](HBC.md)
- [pkg/antfly/src/storage/db/README.md](pkg/antfly/src/storage/db/README.md)
- [pkg/antfly/src/metadata/METADATA.md](pkg/antfly/src/metadata/METADATA.md)
- [pkg/antfly/src/api/PLAN.md](pkg/antfly/src/api/PLAN.md)
- [pkg/antfly/src/raft/RAFT.md](pkg/antfly/src/raft/RAFT.md)
- [pkg/antfly/src/lmdb/LMDB.md](pkg/antfly/src/lmdb/LMDB.md)
- [pkg/inference/ROADMAP.md](pkg/inference/ROADMAP.md)
- [lib/raft/ROADMAP.md](lib/raft/ROADMAP.md)
- [lib/json/JSON.md](lib/json/JSON.md)
- [lib/regex/REGEX.md](lib/regex/REGEX.md)
- [lib/image/IMAGE.md](lib/image/IMAGE.md)
- [lib/audio/AUDIO.md](lib/audio/AUDIO.md)

## Test Targets

Use these build targets for the current coarse test split:

- `zig build unit-test`
  - focused fast/unit-style buckets, storage, auth, serverless, API, and other
    non-chaos lanes
- `zig build sim-test`
  - deterministic metadata, DB, and runtime simulation coverage without
    delayed/restart/partition chaos
- `zig build chaos-test`
  - delayed transport, restart, partition, and long-running chaos coverage
- `zig build integration-test`
  - higher-level integration coverage that is not purely hermetic unit or sim
    work
- `zig build recall-test`
  - recall and retrieval-quality regression coverage
- `zig build test`
  - umbrella target for the normal local suite

Generated-contract checks are intentionally separate from the broad test
targets when they need a narrower rerun:

- `zig build openapi-root-check`
- `make zig-generated-check`

## Current Shape

The project is past basic substrate bring-up. The main work now is product
correctness, public-contract convergence, and making the stateful and serverless
execution modes feel like one database product.

Substantial pieces already exist:

- hosted Raft/runtime substrate with split/merge coordination
- metadata service/server, desired topology, placement, reconciliation, and
  status surfaces
- DB-backed shard transitions, durable replica state, LMDB/WAL paths, and LSM
  backend work
- table/index lifecycle, routed reads/writes, graph/query/retrieval surfaces,
  and OpenAPI-shaped API contracts
- reusable full-text, vector, graph, JSON, regex, image, audio, raft, and
  inference library modules
- serverless manifest/artifact/publication work with a table-first public
  contract under active convergence

The live gaps are tracked in [TODO.md](TODO.md):

- serverless table architecture and publication parity
- stateful/control-plane follow-up
- query, search, retrieval, API, config, and protocol parity

## Major Lanes

### 1. Serverless Table Product

Primary reference:

- [SERVERLESS.md](SERVERLESS.md)

Near-term goals:

- keep `/tables/...` as the public serverless product surface
- keep provider/runtime controls under `/_internal/...`
- make published read/search behavior operationally credible before adding new
  serverless-only product surface area
- harden object-store publication, prune, retry, crash, and remote-backend
  semantics
- make published/latest/exact-read freshness semantics explicit

Principles:

- serverless should be the same product with a different execution model, not a
  namespace-only database model
- reuse engine code from search, vector, graph, indexing, and segment machinery
- do not make serverless depend on hosted-Raft lifecycle or replica placement
  as first-order architecture

### 2. Public API, OpenAPI, And Table Contract

Primary references:

- [TODO.md](TODO.md)
- [pkg/antfly/src/api/PLAN.md](pkg/antfly/src/api/PLAN.md)

Near-term goals:

- keep generated route/server drift fixed promptly when OpenAPI routes are
  added or renamed
- keep the public contract table-first and shared across stateful and
  serverless where parity is intended
- keep provider/runtime controls under internal surfaces instead of leaking
  deployment-specific knobs into the shared table API
- keep SQL/API typed-plan parity coverage close to the table and query
  behavior it is protecting

Principles:

- internal control-plane/runtime seams stay as Zig modules
- external user/operator APIs should converge on the OpenAPI contract
- both stateful and serverless paths should converge on the table-centric
  product contract wherever the capability makes sense

### 3. Stateful Metadata And Runtime

Primary references:

- [pkg/antfly/src/metadata/METADATA.md](pkg/antfly/src/metadata/METADATA.md)
- [pkg/antfly/src/raft/RAFT.md](pkg/antfly/src/raft/RAFT.md)
- [lib/raft/ROADMAP.md](lib/raft/ROADMAP.md)

Near-term goals:

- keep metadata/data-node orchestration stable across split, merge, recovery,
  and remote status reporting
- strengthen replica/bootstrap descriptors and disappearing group/store
  handling
- keep product policy out of raft-core where the metadata layer can own it
- keep simulation and chaos coverage focused on externally visible table,
  shard, and range behavior rather than on copied implementation details

### 4. Query, Search, And Retrieval

Primary references:

- [TODO.md](TODO.md)
- [pkg/antfly/src/api/PLAN.md](pkg/antfly/src/api/PLAN.md)
- [pkg/inference/ROADMAP.md](pkg/inference/ROADMAP.md)

Near-term goals:

- add parity coverage before introducing new public query/search API shapes
- deepen hybrid, reranking, provider-backed query stages, foreign sources,
  joins, retrieval agents, and graph depth against public contract behavior
- keep remote-content and provider-matrix expansion behind explicit tests
- keep inference integration work separate from Antfly product API planning
  unless the integration surface requires a shared contract change

### 5. Storage Engine And Durability

Primary references:

- [DB.md](DB.md)
- [pkg/antfly/src/storage/db/README.md](pkg/antfly/src/storage/db/README.md)
- [pkg/antfly/src/lmdb/LMDB.md](pkg/antfly/src/lmdb/LMDB.md)
- [BACKUPS.md](BACKUPS.md)

Near-term goals:

- keep DB package boundaries understandable for future storage work
- keep LMDB/WAL durability and crash confidence improving
- keep LSM/HBC/vector write guardrails aligned with production-shaped ingest
- support metadata/data workflows without storage regressions
- keep reopen/recovery, split/restore, and simulation matrices strong

### 6. Inference And Shared Libraries

Primary references:

- [pkg/inference/ROADMAP.md](pkg/inference/ROADMAP.md)
- [lib/json/JSON.md](lib/json/JSON.md)
- [lib/regex/REGEX.md](lib/regex/REGEX.md)
- [lib/image/IMAGE.md](lib/image/IMAGE.md)
- [lib/audio/AUDIO.md](lib/audio/AUDIO.md)

Near-term goals:

- keep reusable libraries documented where their implementation lives
- keep Antfly inference API/model work separate from Antfly product API
  planning unless the integration surface requires it
- use library-level docs for design details and root docs for repository
  orientation
- keep build targets at the root only when they are useful product-level
  aliases; package-local detail should stay with the package

## Immediate Project Order

1. Keep CI and generated-contract checks green as guardrails, with focused
   reruns when route or trace behavior changes.
2. Tighten generated/OpenAPI checks around Go and Zig surface changes so route
   additions fail close to the source of drift.
3. Keep the CI lanes stable enough to decide which E2E and TLA checks should be
   required for normal PRs.
4. Continue serverless table/publication convergence:
   - canonical table metadata
   - index/schema publication execution
   - per-family artifact reuse
   - explicit freshness/read semantics
5. Deepen public query/search/retrieval parity only with matching coverage.
6. Continue stateful metadata/runtime hardening around split, merge, recovery,
   backup/restore, and remote status propagation.
7. Keep shared library docs under `lib/` and inference docs under
   `pkg/inference/` as those modules become stable user-facing design surfaces.

## Planning Rules

- Put project-wide sequencing here.
- Put current bugs and parity task detail in `TODO.md`.
- Put subsystem implementation detail in the subsystem roadmap/plan.
- If a task is mostly about one directory, update that subsystem plan first.
- If a task changes project priorities or ordering, update this file too.
