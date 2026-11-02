# Inference preparation and execution

GGUF, safetensors and imported ONNX models share preparation and execution
contracts. Architecture adapters describe model semantics and checkpoint layouts;
backends retain their tuned kernels and whole-model implementations. Deployment
uses the existing platform and driver interfaces without an external inference
framework.

## Ownership

Paths below are relative to `zig/pkg/inference/src/`.

| Owner | Responsibility |
|---|---|
| `architectures/model_definition.zig` | Backend-independent configuration and loaded weight sources |
| `models/session_contract.zig` | Resolved task, processor, padding, pooling, normalization and modality semantics |
| `backends/session.zig` | Published session contracts and typed backend acquisition across wrappers |
| `backends/compute_context.zig` | Common backend construction, exact typed imports and shared context lifetime |
| `execution/program.zig` | Optimized IR, composed node mappings and input/output/weight/state bindings |
| `execution/executable.zig` | Preparation, reusable execution, output placement and optional diagnostics |
| `execution/model_candidate.zig` | Admission and dispatch of specialized whole-model implementations |
| `execution/physical_buffers.zig` | Assigned outputs, transfers, workspace and bounded invocation frames |
| `graph/decode_state_runtime.zig` | KV/recurrent/paged state ownership and advancement |

`execution/root.zig` exposes the common `ModelProgram`, `prepare`, `Executable`
and `BackendContext` boundary. Imported, directly constructed and traced graphs
use the shared fixed-point optimizer. Public and state bindings follow composed
node mappings through rewrites. Already analyzed graphs can borrow their cached
analysis and immutable weights during shape specialization.

## Execution invariants

- Select a backend or specialized candidate using dtype, shape, layout, storage,
  semantics and required features. A model candidate may decline before
  submission; errors after submission propagate without replaying partially
  advanced state through another implementation.
- Separate legality from profitability. Placement accounts for computation,
  launches and transfers while preserving exposed outputs and stateful regions.
  Unsupported signatures fail explicitly or use an observable admitted fallback.
- Bind eligible operations to planned physical outputs and workspace. Native
  schedules and compatibility arenas remain available behind their existing
  admission rules. Logical lifetimes include reordered submissions and aliases.
- Retain storage leases for views and published results across frame reuse,
  executable eviction and producing-session teardown. External contexts must
  outlive values that do not retain their context themselves.
- CUDA capture retains stable operand, output and workspace addresses. Completion
  protects reuse and retirement; captured executables retire before their storage
  owners. Reset and cancellation preserve the existing state/capture guards.
- Resident stage contracts keep intermediates on their admitted backend. Explicit
  host export reads directly from the producer. Typed transfers preserve storage
  bytes or reject the operation; they do not silently convert through F32.
- Diagnostics record actual dispatch, allocation/reuse, transfers and bytes,
  cache behavior and fallback. Nested transfer audits cannot weaken an outer
  strict policy. Planner-only assertions do not establish execution parity.

## Kernel ownership and validation

`ops/kernel_descriptor.zig` describes operation semantics and requirements;
`ops/kernel_geometry.zig` shares geometry with legality checks. Backend plans
retain SIMD/BLAS, packed quantization, native GPU schedules and profitable region
fusions. Compile-time recipes share GGUF encoding mathematics without imposing
one accumulation or scheduling policy on every backend.

See [kernel authoring](kernel-authoring.md) for source ownership, typed schedules,
regeneration, source identities and hardware qualification. Portable Tile model
execution remains a prototype; its scope and commands are in
`zig/pkg/inference/src/tile/README.md`.

Preparation and lifetime regressions live with their implementation owners,
including `execution/executable.zig`, `execution/model_candidate.zig`,
`execution/transfer_audit.zig` and `backends/imported_onnx_session.zig`. Run focused
scopes from the inference package with the pinned Zig compiler, for example:

```sh
zig build test-embeddinggemma2 -Dmetal=false -Dcuda=false \
  -Dtest-filter='ModelProgram' -Dtest-filter='prepared physical'
zig build check-kernel-ownership test-tile-kernels
```

Enable the backend under test for physical device coverage. Qualify numerical
changes with independent references, retained native resource/schedule checks,
and controlled baseline/candidate timing before retiring a fast path.

## Remaining migration boundaries

Issue [#1166](https://github.com/antflydb/antfly/issues/1166) also requests a
complete migration of execution policy. Shared kernel ownership and the common
preparation API do not by themselves complete these remaining requirements:

- Imported ONNX and architecture graphs share the bounded specialization owner
  in `execution/specialization_cache.zig`, with stable executable handles,
  allocation-before-eviction admission and explicit retirement. Their limits
  remain workload-specific. Traced resources are scoped to the exact backend
  context; switching contexts retires device bindings while preserving semantic
  graphs and analysis. ONNX initializers retain their shared immutable context.
- Assigned outputs/workspace cover admitted native/CUDA descriptors, while Metal
  consumes its existing physical allocation plan. Prepared CUDA graphs with full
  assigned-kernel coverage can capture the plan's stable output, workspace,
  mutable-input and F32-literal banks. Binding roles, exact context identity,
  immutable-handle ownership and copy/launch costs gate admission. Backend
  predicates exclude packed/reduced and vendor-workspace routes whose complete
  temporary ownership is not yet in that plan, preserving their tuned dispatch.
  F32 literals share one immutable allocation across frames; explicit literal
  overrides use ordinary execution without mutating captured storage. Dense
  metadata reshapes preserve backing storage and output shapes. Recording and
  instantiation failures fall back before compute submission; submission errors
  propagate.
  Captures own their address leases; published outputs and foreign captures
  prevent reuse of mutable frame storage. Cancellation/side-channel requests and unsupported
  graphs retain ordinary execution, with explicit replay decisions in diagnostics.
  Model-specific decode capture retains its learned arena as a compatibility
  path. Universal physical planning and retirement of that fallback remain open.
- `model_definition.zig` owns artifact loading. Native and PJRT sessions share
  one failure-atomic host preparation path for normalization, resident/lazy
  weights and prefetch ownership. CUDA reuses its admitted manifest snapshot
  when opening that source, retaining early device/artifact preflight. Prefetch
  guards are installed only after the final session owner is in place.
  `compute_context.zig`, prepared executables and model candidates separate
  further responsibilities. The session factory, model
  manager and generation pipeline still contain model-specific loading and
  physical execution decisions. Optional specialized hooks now use typed
  operation regions (decoder, replay, relative/window attention, encoder,
  projection, vision/audio, speculative verification and token selection).
  Historical public method names are compatibility adapters; new implementations
  extend operation descriptors/regions, preserving each admitted native schedule.
  `graph/execution.Context` declares backend, model-weight adapter, execution
  control and compiled-backend dependencies explicitly. Serving and offline
  compilation use that same typed contract instead of pipeline-shaped structs.
  Backend pointers are borrowed from their real owner; temporary context values
  never become the owner of cached executors or physical bindings.
  Embedding indices explicitly select host IDs or a resident tensor. Traced
  host-ID placeholders need no materialized tensor; imported integer tensors
  retain their resident fast path and exact integer fallback.
- Resident stage contracts, exact typed transfers and strict readback auditing
  are implemented. Migrating every task to resident stage chaining and moving
  all KV/recurrent execution choices behind runtime owners remains incremental.
  `graph/decode_batch.zig` now owns uniform, mixed prefill/decode and packed
  batch metadata assembly, including per-sequence physical KV views, recurrent
  caches and position ownership. Generation retains compatibility exports and
  supplies typed state/geometry; batch layout does not depend on a task pipeline.
- `execution/testing/model_formats.zig` compares deterministic one-layer BERT
  and sliding-window decoder graphs loaded through real ONNX, safetensors and
  GGUF parsers. Deliberately lossy Q8_0 weights use an independently rounded CPU
  reference, with a `2e-4` execution tolerance. Decoder K/V stays resident across
  four transitions; held earlier outputs detect frame overwrites. Immutable
  matrix packing is warmed before strict readback checks. This matrix validates
  format equivalence; it does not benchmark packed quantized serving kernels.
- Execution diagnostics include per-invocation context counter deltas and
  specialization decisions, alongside dispatch/transfer/reuse statistics and
  explicitly scoped memory gauges. Unmeasured values are null, and lifetime
  high-water memory is not presented as an invocation peak. Complete Metal
  launch/synchronization and persistent/transient peak accounting remains open.

The issue checklist records implementation and evidence separately from remaining
migration. Compatibility paths stay until their replacements satisfy correctness,
residency, lifetime and performance checks. Physical L4/A100 qualification remains
deferred; CPU, local Metal and Glowworm GB10 provide the available hardware tests.
