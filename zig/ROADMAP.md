# Project Roadmap

This file is the top-level execution map for `antfly-zig`. Use it to answer:

- what the major project lanes are
- what order they should move in
- which detailed subsystem plan to open next

Use [TODO.md](TODO.md)
for the live bug list and remaining parity gaps. Use
[README.md](README.md)
for repository layout, build commands, and day-to-day development notes.

Detailed execution belongs in subsystem docs:

- [TODO.md](TODO.md)
- [BACKUPS.md](BACKUPS.md)
- [SERVERLESS.md](SERVERLESS.md)
- [STARTUP.md](STARTUP.md)
- [DB.md](DB.md)
- [BATCH.md](BATCH.md)
- [FULL_TEXT.md](FULL_TEXT.md)
- [QUERY_STRING.md](QUERY_STRING.md)
- [HBC.md](HBC.md)
- [pkg/antfly/src/metadata/METADATA.md](pkg/antfly/src/metadata/METADATA.md)
- [pkg/antfly/src/api/PLAN.md](pkg/antfly/src/api/PLAN.md)
- [pkg/antfly/src/raft/RAFT.md](pkg/antfly/src/raft/RAFT.md)
- [pkg/antfly/src/lmdb/LMDB.md](pkg/antfly/src/lmdb/LMDB.md)
- [go/pkg/termite/ROADMAP.md](go/pkg/termite/ROADMAP.md)
- [go/pkg/antfly/lib/raft/ROADMAP.md](go/pkg/antfly/lib/raft/ROADMAP.md)

## Test Targets

Use these build targets for the current test split:

- `zig build unit-test`
  - focused fast/unit-style buckets, storage, auth, serverless, and other
    non-chaos lanes
- `zig build sim-test`
  - deterministic metadata simulation and public parity suites without
    delayed/restart/partition chaos
- `zig build chaos-test`
  - delayed transport, restart, partition, and long-running metadata chaos
    coverage
- `zig build test`
  - umbrella target that runs all of the above

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
- reusable full-text, vector, graph, JSON, regex, image, audio, and Antfly inference
  library modules
- serverless manifest/artifact/publication work with a table-first public
  contract under active convergence

The live gaps are tracked in [TODO.md](TODO.md):

- current E2E failures and CI coverage gaps
- serverless table architecture and publication parity
- stateful/control-plane follow-up
- query, search, retrieval, API, config, and protocol parity

## Major Lanes

### 1. Product Correctness And CI

Primary reference:
- [TODO.md](TODO.md)

Near-term goals:
- fix current Antfly Python E2E failures before expanding public surface area
- make readiness/status bugs diagnosable from preserved roots and server logs
- move high-signal E2E coverage into regular CI once it is stable enough
- keep `zig build openapi-root-check` as the safe contract drift check until
  all source specs are local

### 2. Stateful Metadata And Runtime

Primary references:
- [pkg/antfly/src/metadata/METADATA.md](pkg/antfly/src/metadata/METADATA.md)
- [pkg/antfly/src/raft/RAFT.md](pkg/antfly/src/raft/RAFT.md)

Near-term goals:
- keep metadata/data-node orchestration stable across split, merge, recovery,
  and remote status reporting
- strengthen replica/bootstrap descriptors and disappearing group/store
  handling
- keep product policy out of raft-core where the metadata layer can own it

### 3. Public API, Query, Search, And Retrieval

Primary references:
- [TODO.md](TODO.md)
- [pkg/antfly/src/api/PLAN.md](pkg/antfly/src/api/PLAN.md)
- [../antfly/openapi.yaml](../antfly/openapi.yaml)
- [../antfly2/openapi.yaml](../antfly2/openapi.yaml)

Near-term goals:
- fix status/readiness gaps before broadening API behavior
- add parity coverage before new public query/search shapes
- deepen hybrid, foreign source, join, graph, and retrieval behavior against
  Go/OpenAPI expectations
- keep handwritten behavior and generated contract surfaces aligned

Principle:
- internal control-plane/runtime seams stay as Zig modules
- external user/operator APIs should converge on the OpenAPI contract
- both the stateful and serverless paths should converge on the table-centric
  product contract wherever the capability makes sense

### 4. Serverless Table Product

Primary references:
- [SERVERLESS.md](SERVERLESS.md)

Near-term goals:
- keep `/tables/...` as the public serverless product surface
- keep provider/runtime controls under `/_internal/...`
- finish canonical table metadata, publication state, and build-status
  alignment
- make index/schema changes publish through concrete per-family and per-index
  artifact actions
- make published/latest/exact-read freshness semantics explicit

Principle:
- serverless should be the same product with a different execution model, not a
  namespace-only database model
- reuse engine code from search, vector, graph, indexing, and segment machinery
- do not make serverless depend on hosted-Raft lifecycle or replica placement
  as first-order architecture

### 5. Storage Engine And Durability

Primary reference:
- [pkg/antfly/src/lmdb/LMDB.md](pkg/antfly/src/lmdb/LMDB.md)

Near-term goals:
- keep LMDB/WAL durability and crash confidence improving
- keep LSM/HBC/vector write guardrails aligned with production-shaped ingest
- support metadata/data workflows without storage regressions
- keep reopen/recovery and simulation matrices strong

### 6. Antfly inference And Shared Libraries

Primary references:
- [go/pkg/termite/ROADMAP.md](go/pkg/termite/ROADMAP.md)
- [go/pkg/antfly/lib/json/JSON.md](go/pkg/antfly/lib/json/JSON.md)
- [go/pkg/antfly/lib/regex/REGEX.md](go/pkg/antfly/lib/regex/REGEX.md)
- [go/pkg/antfly/lib/image/IMAGE.md](go/pkg/antfly/lib/image/IMAGE.md)
- [go/pkg/antfly/lib/audio/AUDIO.md](go/pkg/antfly/lib/audio/AUDIO.md)

Near-term goals:
- keep reusable libraries documented where their implementation lives
- keep Antfly inference API/model work separate from Antfly product API planning unless
  the integration surface requires it
- use library-level docs for design details and root docs for repository
  orientation

## Immediate Project Order

1. Stabilize the active Antfly E2E failures in `TODO.md`, especially status and
   readiness bugs that obscure actual data-path health.
2. Tighten CI around the stable parts of the current verification matrix.
3. Continue serverless table/publication convergence:
   - canonical table metadata
   - index/schema publication execution
   - per-family artifact reuse
   - explicit freshness/read semantics
4. Deepen public query/search/retrieval parity only with matching coverage.
5. Continue stateful metadata/runtime hardening around split, merge, recovery,
   backup/restore, and remote status propagation.
6. Keep shared library docs and implementation colocated under `go/pkg/antfly/lib/` as those
   modules become stable user-facing design surfaces.

## Planning Rules

- Put project-wide sequencing here.
- Put current bugs and parity task detail in `TODO.md`.
- Put subsystem implementation detail in the subsystem roadmap/plan.
- If a task is mostly about one directory, update that subsystem plan first.
- If a task changes project priorities or ordering, update this file too.
