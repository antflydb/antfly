# NVMe Weight Tiering Plan

## Goal

Finish termite's large-model execution path so generator models can run from a disk-backed working set instead of failing when full promoted weights do not fit in backend memory.

The desired end state is closer to "Hypura-style" execution:

- model weights stay mmap-backed on disk by default
- the runtime promotes only a bounded hot set into host RAM and backend memory
- backend pressure causes eviction and smaller staged execution, not immediate request failure
- embedded termite and swarm expose enough knobs to tune budgets when heuristics are wrong

## Progress

### Implemented on 2026-04-09

- Phase 3.1 now has an Metal packed-MoE staging path for Mixtral-style GGUF packed experts:
  - packed expert direct-quant refs remain mmap-backed and are no longer charged as host-resident cache copies
  - grouped Metal MoE execution stages only the selected expert byte ranges when CPU router IDs are available
  - fused Metal MoE execution reads selected expert IDs back from Metal and stages compact selected-expert buffers before launching the native quant kernels
  - staged owned buffers use Metal-owned arrays instead of borrowed mmap arrays
- Verified in `pkg/termite` with:
  - `zig build`
  - `zig build test`
- Runtime validation completed for `mistralai/Mixtral-8x7B-Instruct-v0.1-Q5_K_M` on Metal with a low host budget:
  - `--max-tokens 1 --host-budget-mb 1024 --combined-budget-mb 6144 --backend-budget-mb 4096` completed with `finish_reason=length tokens=1`
  - `--max-tokens 4 --host-budget-mb 1024 --combined-budget-mb 6144 --backend-budget-mb 4096` completed with `finish_reason=length tokens=4`
- This is still selected-expert staging for packed MoE tensors, not a complete general tensor tile executor.

### Implemented on 2026-04-08

- Embedded termite now has server-side generation budget overrides in:
  - [server.zig](pkg/termite/src/server/server.zig)
- `antfly termite run` and `antfly swarm` now accept budget overrides for embedded termite generation:
  - `--host-budget-mb`
  - `--backend-budget-mb`
  - `--combined-budget-mb`
  - `--kv-budget-mb`
  - `--scratch-budget-mb`
  - `--termite-host-budget-mb`
  - `--termite-backend-budget-mb`
  - `--termite-combined-budget-mb`
  - `--termite-kv-budget-mb`
  - `--termite-scratch-budget-mb`
- The live swarm e2e fixture now passes these via environment-backed defaults for the pulled `ggml-org/gemma-4-e2b-it-gguf` path:
  - [test_swarm.py](../../e2e/antfly/test_swarm.py)
- Metal eager resident-weight reservation is now capped to a bounded hot set instead of reserving the full eager estimate up front:
  - [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
- Metal quantized linear execution now degrades on backend budget pressure for non-expert weights by falling back to the existing wrapper path instead of surfacing an immediate `MemoryBudgetExceeded`:
  - [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
- Metal quantized pair execution now falls back to per-weight execution when the device-native pair path hits memory budget pressure for degradable weights:
  - [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
- Host-tier cache denial for non-expert quantized GGUF weights now has an ephemeral mmap-backed fallback path instead of failing before quant execution can run:
  - [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
- Dense linear non-expert weights now fall back to the existing CPU chunked source-tensor execution path when host/backend temporary reservations or host-cache admission fail:
  - [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
- BLAS now mirrors the same non-expert fallback policy:
  - host-tier cache denial for non-expert quantized GGUF weights can return an ephemeral mmap-backed quantized placeholder instead of failing immediately
  - dense linear non-expert weights can route into the existing CPU chunked source-tensor path when host-cache admission fails
  - [native_compute.zig](pkg/termite/src/ops/native_compute.zig)
- Host shared-cache denials are now recorded into the run-budget telemetry for both Metal and BLAS, and shared-cache fallback logs now identify the denial stage and weight name:
  - [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
  - [native_compute.zig](pkg/termite/src/ops/native_compute.zig)
- Host-tier lazy-load admission now tries to evict cold non-expert host residents before degrading to ephemeral execution:
  - [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
  - [native_compute.zig](pkg/termite/src/ops/native_compute.zig)
- Metal quantized placeholder caching on first access now honors backend shared-cache admission and degrades to a non-cached placeholder when backend cache admission still fails after eviction:
  - [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
- `pkg/termite` currently verifies with both:
  - `zig build test`

### Still not done

- This is still not tile-based NVMe execution.
- The runtime can now be tuned, is less eager to fail on the resident-weight estimate, and handles some non-expert quantized and dense-linear host/backend budget pressure without aborting the request, but it still does not execute large tensors from disk at tile granularity.
- The remaining substantive work is still Phase 2 and Phase 3 below.

## Current State

The codebase already has several pieces of the design:

- GGUF weights are mmap-backed in [tensor_store.zig](pkg/termite/src/models/tensor_store.zig).
- The tier model exists in [planner.zig](pkg/termite/src/runtime/tier/planner.zig):
  - `disk`
  - `host`
  - `backend`
- Metal and BLAS both have lazy-weight promotion and eviction paths:
  - [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
  - [native_compute.zig](pkg/termite/src/ops/native_compute.zig)
- Dynamic system-memory-based budget derivation exists in [memory.zig](pkg/termite/src/runtime/tier/memory.zig).

## What Is Not Finished

The runtime is not yet fully NVMe-first for large generator models.

### 1. Upfront reservations are still too coarse

Metal still reserves a large resident-weight estimate up front in:

- [session_factory.zig](pkg/termite/src/architectures/session_factory.zig)
- [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)

That means we can fail before the runtime has a chance to behave like a bounded working set.

### 2. Some generator paths still go eager instead of fully lazy

For the NVMe path to be real, large generator models should default to:

- mmap the GGUF
- build lazy refs
- promote only what is touched

Today some Metal setup paths still materialize too much too early in [session_factory.zig](pkg/termite/src/architectures/session_factory.zig).

### 3. Promotion is still too tensor-oriented

The biggest missing piece is granularity.

Today large weights are still often promoted as whole tensors or whole packed buffers in:

- [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
- [native_compute.zig](pkg/termite/src/ops/native_compute.zig)

That is not the same as tiled execution from disk-backed storage.

### 4. Budget pressure still fails too early

The runtime does retry smaller prefill chunks in [generation.zig](pkg/termite/src/pipelines/generation.zig), but weight promotion pressure still often surfaces as `MemoryBudgetExceeded` rather than:

- evict
- retry smaller
- degrade to host-backed execution
- continue

### 5. Embedded termite does not expose enough override controls

`antfly swarm` and embedded termite do not currently provide a clean way to override generation budgets for live runs, so the default heuristic can block otherwise viable models.

Relevant paths:

- [pkg/antfly/src/termite/runtime.zig](pkg/antfly/src/termite/runtime.zig)
- [pkg/antfly/src/swarm/runtime.zig](pkg/antfly/src/swarm/runtime.zig)

## Practical Constraints

### Unified memory is not infinite

This machine may have roughly 31 GB free, but the current budget logic intentionally keeps headroom in [memory.zig](pkg/termite/src/runtime/tier/memory.zig). That is reasonable for safety, but today the heuristic is conservative enough to reject models that should still be runnable with a better working-set strategy.

### mmap is necessary but not sufficient

We already mmap GGUF weights, which is good. But mmap alone does not give us "run arbitrarily large models from NVMe." We still need tile-level execution or staged promotion so we are not forced to upload whole large weights to the backend.

## Plan

## Phase 1: Make The Current Design Usable

### 1.1 Expose budget overrides in embedded termite and swarm

Add CLI/config/env support for:

- host budget
- backend budget
- combined budget
- KV budget
- scratch budget

Targets:

- [pkg/antfly/src/termite/runtime.zig](pkg/antfly/src/termite/runtime.zig)
- [pkg/antfly/src/swarm/runtime.zig](pkg/antfly/src/swarm/runtime.zig)

Outcome:

- live e2e tests can raise limits when heuristics are too conservative
- users can actually run models their machine can handle without patching code

### 1.2 Reduce or remove oversized upfront resident-weight reservations

Audit and narrow:

- `resident_weight_estimate_bytes`
- `tryReserveWeight(.host, ...)`

Targets:

- [session_factory.zig](pkg/termite/src/architectures/session_factory.zig)
- [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)

Outcome:

- runtime reserves only pinned hot weights, not a speculative near-full model footprint

### 1.3 Prefer lazy loading for large generator models by default

Tighten Metal generator setup so large GPT-family GGUF models do not take eager-dense paths unless explicitly needed.

Target:

- [session_factory.zig](pkg/termite/src/architectures/session_factory.zig)

Outcome:

- more models start successfully
- fewer failures before lazy promotion can do useful work

## Phase 2: Make Budget Pressure Degrade Instead Of Fail

### 2.1 Add backend-to-host fallback for non-expert weights

When backend promotion cannot fit:

- evict backend residents first
- if still blocked, keep the weight host-backed
- run the op through host-backed quant or staged dense execution

Targets:

- [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
- [native_compute.zig](pkg/termite/src/ops/native_compute.zig)

Outcome:

- backend pressure hurts latency, not correctness

### 2.2 Improve retry behavior around weight pressure

Today prefill chunking retries on memory pressure in [generation.zig](pkg/termite/src/pipelines/generation.zig), but weight promotion paths still return hard failure too easily.

Add retry/degrade behavior around:

- backend promotion denial
- shared cache denial
- quant placeholder promotion failure

Outcome:

- requests continue with smaller staging instead of surfacing `MemoryBudgetExceeded`

### 2.3 Improve telemetry for denials and demotions

Add structured logging/metrics for:

- denial reason
- current tier-cache occupancy
- bytes requested
- fallback taken
- evictions performed

Targets:

- [memory.zig](pkg/termite/src/runtime/tier/memory.zig)
- [cache.zig](pkg/termite/src/runtime/tier/cache.zig)
- compute backends

Outcome:

- runtime behavior becomes debuggable
- heuristics can be tuned with real evidence

## Phase 3: Finish Real NVMe-Backed Execution

### 3.1 Move from tensor-level promotion to tile-level promotion

This is the core unfinished work.

Instead of promoting full large tensors, introduce:

- tensor tile descriptors
- tile iterators over GGUF-backed bytes
- small backend tile cache
- tile LRU / eviction

Targets:

- [tensor_store.zig](pkg/termite/src/models/tensor_store.zig)
- [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
- [native_compute.zig](pkg/termite/src/ops/native_compute.zig)

Outcome:

- runtime no longer needs full tensor residency to use large weights

### 3.2 Add Metal quant kernels that consume staged tiles

The Metal path currently uploads larger prepared buffers than we want under tight budgets.

We need:

- tiled prepared-byte views
- staged upload of only the active slice
- execution that accepts partial backend residency

Targets:

- [metal_native_provider.zig](pkg/termite/src/backends/metal_native_provider.zig) or adjacent Metal quant code
- [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)

Outcome:

- backend memory scales with active tiles, not full promoted tensors

### 3.3 Make host RAM a bounded cache, not a shadow copy of the model

For full NVMe behavior, host RAM should be a cache tier, not a second full-resident model.

That means:

- aggressive demotion from host back to disk
- no silent accumulation of host-loaded tensors
- eviction policy for non-expert weights as well as experts

Targets:

- [metal_compute.zig](pkg/termite/src/ops/metal_compute.zig)
- [native_compute.zig](pkg/termite/src/ops/native_compute.zig)
- tier cache / residency helpers

Outcome:

- disk remains the true source of truth
- RAM stays proportional to the active set

## Phase 4: Validate The Design

### 4.1 Add dedicated smoke tests

We need reproducible tests for:

- model loads under tight backend budget
- runtime falls back instead of failing
- host and backend cache eviction work
- repeated generation reuses hot tiles

Targets:

- termite smoke tests
- unit tests in tier/cache/planner
- live e2e swarm coverage

### 4.2 Add stress profiles

Create repeatable runs for:

- small backend budget
- small host budget
- large prompt prefill
- multi-request concurrency
- Metal and BLAS

Outcome:

- confidence that the path is robust, not just barely passing a demo model

## Suggested Immediate Work Order

1. Add embedded termite/swarm budget overrides.
2. Stop reserving oversized resident-weight estimates.
3. Force large GGUF generators down the lazy path by default.
4. Add non-fatal fallback on backend promotion denial.
5. Add tile-level promotion design and prototype it on Metal quantized linear.

## Concrete Questions To Resolve

### 1. What should remain permanently resident?

Likely candidates:

- tiny norms
- routers
- some attention projections
- maybe output head, depending on size

Everything else should be justified, not assumed.

### 2. Should the first tiled implementation be Metal-only?

Probably yes.

The Metal path is where the pain is most visible right now, and BLAS can follow after the tile interfaces stabilize.

### 3. What is the minimum acceptable fallback?

If backend promotion fails, the runtime should still:

- stay correct
- avoid process OOM
- avoid request failure when latency-only degradation is possible

That is a stricter requirement than the current behavior.

## Definition Of Done

This work is done when all of the following are true:

- large GGUF generator models default to lazy disk-backed loading
- embedded termite in swarm can run quantized generators without manual model repacking
- backend pressure causes eviction and degraded execution instead of immediate failure in common cases
- host RAM and backend memory both behave like bounded caches
- live e2e tests cover the embedded swarm path with pulled models from the default termite models directory
- runtime logs explain why it demoted, evicted, or failed
