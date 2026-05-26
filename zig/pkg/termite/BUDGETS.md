# Termite Budget Plan

## Goal

Make memory budgeting a first-class runtime subsystem instead of a set of
per-path heuristics.

The immediate problems are:

- request-time `RunBudget` decisions do not coordinate globally
- model loading has no comparable admission control
- concurrent cold loads can overcommit memory before inference starts
- large GGUF/Metal runs still fall back to coarse eager estimates instead of a
  bounded working-set model

The intended end state is still Hypura-like:

- keep a single model artifact on disk
- keep weights mmap-backed by default
- promote only a bounded hot working set into host and backend memory
- treat budget pressure as a scheduling and residency problem, not an instant
  failure path

## Current State

### What exists now

- `RunBudget` in `src/runtime/tier/memory.zig`
  - request-scoped host/backend/combined/KV/scratch accounting
- tier planning in `src/runtime/tier/planner.zig`
  - `disk`, `host`, `backend`
- mmap-backed GGUF tensor storage in `src/models/tensor_store.zig`
- lazy promotion / eviction paths in:
  - `src/ops/metal_compute.zig`
  - `src/ops/native_compute.zig`
- model cache and cold-load path in `src/server/model_manager.zig`

### What is missing

- no load-time budget or load admission control
- no in-flight load dedup at the model-manager layer
- no single coordinator that sees:
  - resident model footprint
  - in-flight model loads
  - active request budgets
- no clear separation between:
  - temporary load-time memory
  - persistent model residency
  - request-scoped run memory

## Budget Model

### 1. `LoadBudget`

`LoadBudget` should be owned by `ModelManager`.

Its job is to control cold model loads and prevent:

- too many models loading at once
- one oversized load from consuming all host/backend headroom
- duplicate concurrent loads of the same model path

`LoadBudget` should cover:

- temporary mmap / metadata inspection overhead
- tokenizer/materialization buffers created during load
- eager host-side weight materialization
- eager backend-side residency created during session construction

It should explicitly track two classes of bytes:

- temporary bytes
  - released when load finishes or fails
- resident bytes
  - transferred into the loaded-model record on success

### 2. `RunBudget`

`RunBudget` should stay request-scoped.

It already models the right things for an inference run:

- host/backend/combined totals
- KV budget
- scratch budget
- request denial reporting

What should change is not its scope, but how it is admitted.

### 3. Global Coordinator

`ModelManager` should not become the direct owner of all request budgeting.

Instead, termite should add a shared coordinator above the model-manager layer.
That coordinator should own global memory policy and expose:

- load admission
- resident model accounting
- run admission
- cross-request visibility

Practical split:

- `ModelManager`
  - owns `LoadBudget`
  - owns loaded-model residency records
  - exposes resident footprint to the coordinator
- `BudgetCoordinator`
  - owns global policy
  - sees active loads and active runs together
  - decides whether a new load or request can start
- `RunBudget`
  - remains one run's local ledger after admission

## Hypura-Like Runtime Requirements

The Hypura-style pieces that need to be preserved in the budgeting design are:

- single stored model artifact
- direct paging from GGUF instead of repacked duplicate artifacts
- GPU / RAM / NVMe placement planning
- on-demand staging
- hot-set caching
- expert-aware prefetch
- dense-model streaming, not just MoE expert streaming

That means budgets cannot only be "fail if estimate > limit". They also need to
support:

- eviction before denial
- staged execution before denial
- degraded host-backed execution before denial
- load serialization when the system is already near the limit

## Proposed Implementation Plan

### Phase 1: Make load admission real

Add a `LoadBudget` implementation and wire it into
`src/server/model_manager.zig`.

Deliverables:

- `LoadBudget` type with host/backend/combined limits
- model-load reservation API
- load-time temporary vs resident byte accounting
- in-flight load table keyed by model path
- same-model dedup so concurrent requests do not double-load the same model

Acceptance:

- concurrent cold loads no longer race to overcommit memory
- a second request for the same unloaded model waits on the first load

### Phase 2: Teach `ModelManager` about resident footprint

Persist model residency information after load succeeds.

Deliverables:

- per-loaded-model resident host/backend estimate
- release path on unload
- hooks for auxiliary sessions and projections
- API for exposing aggregate resident footprint to the coordinator

Acceptance:

- termite can report how much memory is already pinned by loaded models
- unload returns that capacity to the system

### Phase 3: Add a global `BudgetCoordinator`

Introduce a coordinator above `ModelManager` and request execution.

Deliverables:

- global memory coordinator object
- load admission against resident footprint plus active runs
- run admission against resident footprint plus active loads
- shared denial reason reporting

Acceptance:

- run budgets stop being isolated local guesses
- new runs can be denied or delayed based on actual global state

### Phase 4: Tighten run admission

Keep `RunBudget`, but create it through the coordinator.

Deliverables:

- request admission API
- request release API
- shared accounting for active KV/scratch/resident temporary allocations
- explicit policy for draft-model and multimodel requests

Acceptance:

- two individually valid requests cannot jointly exceed the real process budget

### Phase 5: Move from coarse estimates to working-set policy

Once admission exists, refine the load and runtime estimates to match the
Hypura-like execution model better.

Deliverables:

- distinguish eager-resident bytes from lazy-mmap-backed bytes
- treat large GGUF models as bounded working sets by default
- avoid full-model reservations where only a hot set is actually pinned
- reserve backend hot-set bytes separately from file-backed storage

Acceptance:

- large Metal/GGUF models stop failing solely because of full-artifact estimates

### Phase 6: Tie budgets to tiered degradation

Use budget pressure to trigger better behavior before hard failure.

Deliverables:

- evict-before-deny policy
- host-backed fallback for backend pressure
- staged execution for oversized dense weights
- serialized or rate-limited cold loads under pressure

Acceptance:

- "memory budget exceeded" becomes the last resort, not the first reaction

## Concrete Ownership

### `src/server/model_manager.zig`

Add:

- `LoadBudget`
- in-flight load map
- resident model accounting

Do not add:

- per-request KV/scratch policy logic
- generation scheduling policy

### `src/runtime/tier/`

Add shared primitives here for:

- generic reservations
- shared totals
- denial reporting

Keep `RunBudget` as the run-scoped wrapper over those primitives.

### `src/server/server.zig`

Own the coordinator that combines:

- request concurrency
- model residency
- load admission
- request admission

## Non-Goals

- do not merge all budgets into one giant unscoped `ResourceBudget` API
- do not make session constructors directly own global memory policy
- do not force `create*Session(...)` to take `RunBudget`

Those would blur lifecycle boundaries that are currently useful:

- load-time
- resident-model lifetime
- request lifetime

## Short Version

The right direction is:

1. `ModelManager` owns `LoadBudget`
2. `RunBudget` stays request-scoped
3. a new global coordinator makes them aware of each other

That gets termite to the real goal:

- no accidental OOM during load
- no independent run budgets making incompatible assumptions
- a cleaner path to Hypura-like tiered execution
