# Metal Command Planner and Frame-Execution Slice Plans

> Relocated verbatim from `zig/pkg/inference/METAL.md` (lines 1628–1910 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`METAL.md`](../../../../zig/pkg/inference/METAL.md). Durable decisions from this log were folded into that document before the move.

# Generalize Existing Metal Command Planner

## Summary

The Metal command planner is the canonical graph command-plan abstraction; do
not introduce a parallel planner. `GraphCommandPlan`, `GraphCommandPlanView`,
and `GraphCommandOp` own ordered op records, resource ranges, encoder scopes,
barrier placement, scratch lifetimes, and operator metadata. Model-specific
paths lower into this generic command plan through temporary lowerers.

Success criteria:
- No new parallel planner is introduced.
- `GraphCommandPlan` remains the canonical generic command plan.
- Specialized types such as `GatedFrameCommandLowerer` remain lowering helpers
  or disappear as the generic frame executor takes over.
- Metal prefill consumes one generic frame command plan, not per-layer specialized slices.
- Existing token/residency anchors remain correct.

## Key Changes

- Generic planner surface.
  - Use `GraphCommandPlan`, `GraphCommandPlanView`, and `GraphCommandOp` in
    production code.
  - Keep `ResourceRange`, `ResourceUse`, `EncoderScope`, `ScratchSlotLifetime`, `OperatorPlan`, and `QuantMatmulPlan` as shared concepts.
  - Do not add compatibility aliases for the old runtime-command names.

- Generalize op kinds.
  - Replace decode-specific `OpKind` names with structural names:
    `rms_norm`, `qkv_linear`, `head_norm_rope`, `kv_seed`, `attention`, `attention_output_linear`, `residual_norm_add`, `ffn_gate_up`, `ffn_down`, `ple_gate`, `ple_projection`, `tail_norm`, `lm_head`, `argmax`, `sample`, `quant_get_rows`, `quant_set_rows`, `quant_copy`, etc.
  - Keep phase, qLen, KV layout, quant format, and activation in metadata, not in the enum name.
  - Update tests to assert structural op names plus operator metadata rather than decode-prefixed names.

- Add a real `FrameDescriptor`.
  - Define one backend-neutral frame descriptor for command-plan lowering.
  - Include frame mode (`prefill`, `decode`, `embedding`, `classification`), batch/query lengths, sequence positions, requested outputs, KV mutation policy, KV layout, activation dtype, and backend target.
  - Replace scattered prefill/decode-only fields where practical; keep model-specific layer specs separate from backend execution policy.

- Convert specialized builders into generic lowerers.
  - Keep current Gemma/Q8/f32-KV logic as a lowering path initially, but make it emit generic `GraphCommandOp`s into `GraphCommandPlan`.
  - Keep `GatedFrameCommandLowerer`, `GatedLayerCommandLowerer`,
    `PrefillGatedLayerCommandLowerer`, and related helpers as lowerers, not as
    the public planner abstraction.
  - The specialized lowering helper may still validate Q8_0/f32-KV support, but the output plan must be generic.

- Make Metal consume the generic frame plan.
  - Change Metal runtime entrypoints to accept `GraphCommandPlanView`.
  - Execute the whole prefill frame from that view instead of slicing per-layer command views and returning to Zig orchestration.
  - Use the existing resource ranges/scopes/barriers to group encoder work and reduce command/encoder churn.
  - Unsupported op/operator combinations must return explicit unsupported diagnostics and counters.

## Migration Order

1. Mechanical rename.
   - Use generic type names at imports/call sites.
   - Keep old names out of production source.
   - Run unit tests to verify no behavior change.

2. Structural op-kind migration.
   - Introduce generic op names and map old decode-prefixed values to the new names.
   - Update planner tests and Metal cursor validation.
   - Keep operator plans unchanged.

3. Frame descriptor introduction.
   - Add `FrameDescriptor`.
   - Use it in the Gemma prefill/decode lowering path.
   - Preserve existing `DecoderRuntimePrefillFramePlanRequest` as an adapter until all call sites migrate.

4. Generic Gemma prefill lowering.
   - Build a generic `GraphCommandPlan` using `FrameDescriptor` plus Gemma
     layer specs.
   - Keep the existing specialized lowerer only while it is the adapter from
     Gemma metadata to graph command records.

5. Whole-frame Metal execution.
   - Make Metal execute the generic prefill plan directly.
   - Remove per-layer command-view slicing from the active prefill path.
   - Track `commands`, `planned_commands`, `total_compute_encoders`, and `total_blit_encoders`.

6. Cleanup.
   - Remove deprecated aliases and specialized production planner names.
   - Update `METAL.md` and `GRAPH.md` to describe the final abstraction.

## Test Plan

- Mechanical tests:
  - Run existing planner and Metal unit tests after rename.
  - Add compile-time checks or grep-style tests only if the repo already has that pattern; otherwise keep tests behavioral.

- Planner tests:
  - Assert Gemma4 prefill and decode emit the same structural op names where appropriate.
  - Assert qLen 2..8 selects `mul_mv_ext`, qLen >= 9 selects `mul_mm`.
  - Assert attention operator metadata selects `attention_flash` or `attention_paged` correctly.

- Metal correctness tests:
  - Run `test-metal-gemma4-prefill-block-parity`.
  - Run `hi --max-tokens 1 --temperature 0` and assert token `10979`.
  - Run existing 4-token anchor and assert `10979 236888 2088 740`.
  - Assert `interpreter_fallbacks=0` and `host_outputs=0`.

- Performance checks:
  - Run with `TERMITE_GRAPH_EXECUTOR_STATS=1 TERMITE_DEBUG_METAL_TIMING=1`.
  - Record `commands`, `planned_commands`, `total_compute_encoders`, `total_blit_encoders`, `prefill`, and `gpu_ms`.
  - Compare against current local baseline: `commands=924`, `planned_commands=176`, `total_compute_encoders=942`, `total_blit_encoders=53`, `prefill≈1194-1331ms`.

## Assumptions

- This is a refactor plus wiring change, not a new planner implementation.
- Existing graph command-plan semantics are the source of truth.
- Metal is the first consumer, but names and contracts should be backend-neutral enough for WebGPU/native later.
- Specialized Q8_0/f32-KV checks can remain internally during migration, but not in the final public abstraction names.

# Whole-Frame Metal Graph Execution Plan

## Summary
The current refactor made `GraphCommandPlanView` the generic command-plan shape, and the Metal backend now has frame-level Gemma4 prefill planning/execution hooks wired through `ComputeBackend`. That is real infrastructure, not just a design target. Accepted qLen>1 Q8_0 prefill layers now route setup plus attention/FFN/PLE block work through one composed runtime layer dispatch, so the fast path no longer bounces from Zig into separate setup and block runtime calls. The remaining gap is that the accepted frame still gets sliced back into per-layer `PlannedLayerContract` windows. The next larger performance step is to make Metal consume the full prefill `GraphCommandPlanView` as one backend-owned op stream, with explicit timing that separates host encode/orchestration cost from GPU work.

Target outcome for the Gemma4 `hi --max-tokens 1` Metal smoke:
- Correct token remains `10979`.
- `interpreter_fallbacks=0`, `host_outputs=0`.
- `commands` and `total_compute_encoders` drop materially from the current `924` / `942`.
- Prefill improves only if command/encoder count drops; do not claim success from naming/refactor alone.

## Key Changes
- Continue the frame-level Metal execution entrypoint.
  - `decoderRuntimePlanPrefillFrame` and `decoderRuntimeExecuteGraphCommandPlanFrame` exist and are called by Gemma4 direct prefill for qLen>1.
  - The first supported contract remains Gemma gated RMS + PLE shared-KV prefill only; unsupported plans return `false` with diagnostics, preserving current fallback behavior.
  - The executor now derives layer and tail windows with a structural cursor over `frame_plan.view()`, so it no longer depends on the lowerer's side-channel layer starts for accepted frames.
  - The cursor now feeds one composed runtime dispatch for each accepted Q8_0 prefill layer, carrying the setup and block contracts together. The remaining part is direct execution of the full cursor as one op stream instead of per-layer windows plus a tail contract.

- Move planning-to-execution ownership into Metal.
  - Keep `GatedFrameCommandLowerer` as the temporary Gemma-to-`GraphCommandPlan` lowerer.
  - Stop using per-layer `PlannedLayerContract` slices for the accepted prefill fast path.
  - Build a Metal-side command-plan cursor over `GraphCommandOp` records and encode by structural op kind: setup/QKV, KV seed, attention, attention output, FFN, PLE, tail norm/head.
  - Keep existing helper kernels initially, but call them from one frame executor so scope/barrier decisions are centralized.

- Collapse encoder scopes before adding new kernels.
  - Use `GraphCommandPlanView.scopes` as the source of truth for compute encoder grouping.
  - Within a scope, encode all supported ops into the active encoder and insert planned barriers only where `barrier_before` requires it.
  - Do not add model-named monolithic kernels in this slice; use existing quant/attention/norm primitives behind structural op dispatch.

- Add missing timing and counters.
  - Add counters for `frame_plan_ops`, `frame_plan_scopes`, `frame_plan_scope_encoders`, `frame_plan_encode_ms`, `frame_plan_submit_ms`, `frame_plan_wait_ms`, and `frame_plan_gpu_ms`.
  - Keep existing `graph_executor_stats` fields, but distinguish node-level graph commands from frame-plan commands.
  - Print a diagnostic reason when full-frame execution declines: unsupported op kind, unsupported operator plan, shape mismatch, missing prepared slot, scratch reservation failure.

## Test Plan
- Unit planner tests:
  - Assert `GatedFrameCommandLowerer.view()` emits one contiguous frame plan with expected structural op kinds and scratch lifetimes.
  - Add a test that full-frame eligibility rejects unsupported op/operator combinations without mutating frame state.
  - Add a test that frame-scope cursor groups ops by `scope_index` and preserves planned barriers.
  - Keep accepted-frame cursor tests that prove execution derives layer/tail windows from the full frame plan rather than lowerer side-channel views.

- Metal executor tests:
  - Add a focused mock/fake runtime test for `decoderRuntimeExecuteGraphCommandPlanFrame` that verifies op dispatch order, scope begin/end counts, and barrier count.
  - Keep fallback tests proving unsupported frames still run through current per-layer helpers.

- Runtime smoke:
  - Build `pkg/inference` with `-Dmetal=true -Doptimize=ReleaseFast`.
  - Run Gemma4 unsandboxed through `debug_metal_command.sh`.
  - Acceptance for this slice: token `10979`, no fallbacks/host outputs, no diagnostic reports, and reduced command/encoder counts versus `commands=924`, `total_compute_encoders=942`.
  - Record both cold and warm runs; use warm run for performance comparison.

## Assumptions
- Optimize command/encoder orchestration before adding new Metal kernels.
- Preserve current correctness fallback paths until full-frame execution is proven.
- Scope this slice to Gemma gated prefill; decode and ClipClap/ONNX graph execution use the same abstractions later but are not required for first success.
- Existing backend hooks and active-frame plumbing are not the blocker. The blocker is replacing per-layer contract slicing inside the accepted frame with a single command-plan cursor and scope/barrier executor.
- Performance success is measured by command/encoder reduction plus warm prefill timing, not by GPU time alone, because current `gpu_ms≈17` while prefill is about `1s`.

# Metal Graph Command-Volume Reduction Plan

## Summary
Reduce real Gemma4 Metal graph command volume by moving from per-node partition execution toward ggml-style whole-graph region planning. Use `../ggml` as the reference model: optimize the graph first, fuse only when liveness/aliasing proves safety, reorder independent regions by memory ranges, then encode larger runtime regions instead of many helper calls.

Current anchor: Gemma4 compiled partitioned Metal is around `commands=819`, `planned_commands=141`, `interpreter_fallbacks=0`, `host_outputs=0`, `prefill=998ms`.

Target for this chunk: keep correctness and residency, reduce enabled command volume to `<=500` with a stretch target of `<=250`.

## Key Changes
- Continue the Metal graph region planner on top of the existing partition plan.
  - Runtime region planning/execution already exists in the partition executor for Q linear, QKV, RMS/grouped QKV, attention-output residual, FFN residual, and PLE residual patterns.
  - Existing diagnostics already track region counts and fallbacks, so new work should extend those counters instead of adding parallel statistics.
  - The remaining region work is integration: promote compatible regions into a larger whole-frame command sequence instead of executing many small planned scopes.

- Implement ggml-style fusion eligibility.
  - Use existing use-count/last-use checks plus buffer/resource ranges as Antfly inference’s equivalent of `ggml_can_fuse`.
  - Fuse only when intermediates have no escaping uses.
  - Reject write/read or write/write overlap; allow source-source overlap.
  - Preserve stateful order for KV writes, paged attention, rope position mutation, and requested graph outputs.

- Extend frame/region execution for Metal.
  - The backend frame hook exists for Gemma4 prefill; extend the implementation so it consumes the region/frame plan directly.
  - Other backends keep the existing interpreter/partition path.
  - If a region is not supported, it falls back to the current node executor with an explicit fallback reason.
  - Keep this backend-neutral at the graph interface level; no Gemma-specific public API.

- Fuse the highest-volume real regions first.
  - Attention region: QKV projection, Q/K normalization or reshape/transpose layout ops, rope, fused/paged GQA attention, output projection, residual/norm where eligible.
  - FFN region: up/gate projections, activation, elementwise multiply, down projection, residual/norm where eligible.
  - Tail region: final norm, LM head, argmax/sampling setup where eligible.
  - Prefer existing kernels initially; only add a small fused GLU/activation-multiply kernel if current activation+mul still creates avoidable command churn.

- Add command-volume diagnostics.
  - Track `graph_regions`, `graph_region_ops`, `graph_region_fallbacks`, per-region counts, compute encoders, command buffers, frame encode/wait/GPU time, and top fallback reasons.
  - Keep `interpreter_fallbacks`, `host_outputs`, and `device_outputs` semantics correct after region execution.
- Keep FFN intermediates explicitly typed in command plans.
  - FFN scratch now carries activation dtype intent, with f16 currently enabled for the gated activation buffer and f32 retained for projected/residual-facing buffers.
  - Planned command contracts now carry input/output activation dtype metadata through the Zig/C Metal ABI.
  - Multi-row Q8_0 prefill uses a f16 FFN route for supported descriptors, with a dedicated fused pair-activation MM output kernel and a matching f16-input Q8_0 down-projection MM kernel. The down projection still writes f32 until residual/RMS epilogues support f16 inputs.
  - The planner selects f16 automatically; runtime descriptor/pipeline checks fail closed when a specific shape, quant family, or kernel variant is unsupported.
  - Track `pair_act_mm_out_f16` and `linear_mm_in_f16` counters to prove the real prefill path is using those kernels.
  - Q8_0 pair-activation selection should prefer the fused pair-activation kernel; the split simdgroup two-matmul plus activation/multiply path is a fallback only when the fused pair kernel is unavailable.

## Test Plan
- Add planner unit tests for synthetic attention, FFN, and tail regions.
- Add negative tests for escaped intermediates, unsafe resource overlap, requested intermediate outputs, and KV-state ordering.
- Add real-model planner coverage for Gemma4 graph layout so synthetic coverage cannot drift away from production topology.
- Run CPU unit suite to ensure non-Metal graph behavior is unchanged.
- Run Metal validation through the repo’s debug wrapper only, with API validation and crash bundle capture enabled.
- A/B runtime with region execution enabled and disabled.

## Acceptance Criteria
- Gemma4 Metal compiled partitioned generation still produces the known smoke token output, including token `10979` for the existing short prompt check.
- `interpreter_fallbacks=0` and `host_outputs=0` remain true on the Gemma4 Metal smoke path.
- Enabled command volume drops from `819` to `<=500`; stretch target `<=250`.
- `planned_commands` drops from `141` to `<=100`; stretch target `<=75`.
- No Metal diagnostic crash reports from validation runs.
- If command/encoder volume improves but latency does not, accept this chunk as structural progress and record the remaining bottleneck as kernel quality or scheduling, not graph residency.

## Assumptions
- We use `../ggml` as a design reference, not as a linked dependency.
- The first implementation prioritizes Gemma4 prefill/decode graph shape, but abstractions must stay graph/backend-oriented for CLIP/CLAP/ClipClap and ONNX/GGUF/Safetensors paths.
- The current per-node executor remains the correctness fallback until each region type is proven safe.

# Metal Command Reduction Implementation Plan

## Summary
Reduce Gemma4 partitioned Metal command volume by matching ggml’s execution model more closely: view-like ops become metadata-only, frame-time descriptor construction moves into planning/load paths, and attention/epilogue chains are fused into larger resident regions. Target success is fewer Metal commands with unchanged token output, `interpreter_fallbacks=0`, `host_outputs=0`, and no Metal diagnostic reports.

## Key Changes
- Treat shape/view-only graph ops as aliases in the Metal partition executor: `reshape`, simple last-dim `slice`, and quantized `concat_prim` descriptors should not increment command dispatch or encode kernels when they can be represented as retained tensor views or descriptor metadata.
- Add a planner/cache path for grouped concat QKV weights so concat descriptor construction happens once per graph/runtime slot, not every frame.
- Add an attention-prep graph region that matches exact Gemma layouts: grouped QKV outputs, Q/K/V head RMSNorm, Q scale, Q/K rope, GQA, output projection/norm/residual.
- Extend existing region execution so scalar multiply/add epilogues fold into producer regions when the scalar/broadcast shape is safe and the result has a single expected use.
- Keep all new fusions conservative: exact op sequence, exact shapes, single-use checks, same backend device, and fall back to current execution if any condition fails.

## Implementation Steps
- First implement metadata-only view handling in `metal_partition_executor`: detect no-copy reshapes/slices and publish aliases without treating them as backend commands; add ownership tests to prevent double-free and leaked aliases.
- Move quantized concat QKV descriptors into a reusable slot/cache keyed by concat tree and quant metadata; executor should reuse the descriptor instead of rebuilding it as a command.
- Add matcher tests for real Gemma-like attention prep graphs, then implement the region in stages: QKV outputs through head norms, then rope/scale, then GQA plus existing output residual.
- Add epilogue folding for scalar `mul`/`add` after RMSNorm, attention, FFN, and modulation patterns only when current trace proves the exact producer/consumer shape.
- Update stats to distinguish `metadata_aliases`, `planned_descriptors`, and real command dispatches so reductions are visible and not hidden by counter semantics.

## Test Plan
- Run `zig build test -Dmetal=false --summary failures` after each stage.
- Run focused Metal validation smoke through `pkg/inference/scripts/debug_metal_command.sh command --api-validate -- ... --backend metal --mode compiled --compiled-target partitioned`.
- Acceptance checks for Gemma4 smoke: token id remains `10979`, `interpreter_fallbacks=0`, `host_outputs=0`, no diagnostic reports, command count decreases from current `724`.
- Add unit tests for alias ownership, concat descriptor reuse, attention-prep matcher rejection on extra uses, and scalar epilogue rejection on non-scalar/broadcast-unsafe inputs.
- Keep a traced command histogram before/after each slice and document the command deltas.

## Assumptions
- Prioritize command-count and residency correctness over timing until API-validation noise is removed.
- Do not introduce broad graph rewrites yet; implement conservative executor/planner regions first.
- Treat ggml as the behavioral model for views and descriptors: metadata-only unless a real contiguous copy is required.

## Current Status: Planned Graph Region Scopes
- Gemma4 Metal compiled partitioned remains fully resident for the smoke path:
  `interpreter_fallbacks=0`, `host_outputs=0`, `graph_region_fallbacks=0`.
- PLE residual execution now supports the Q8 fused fast path and a generic device descriptor path for Q4 and other supported single-stage quant formats. Focused Metal validation covers both Q8 and Q4 PLE residual paths.
- The partition executor now pre-materializes constants/zero tensors before opening the active Metal frame, avoiding constant uploads during the hot frame where possible.
- Empty active frames are no longer submitted/waited: `flush_active_frame` cancels/restarts empty frames, and submit-and-wait cancels empty active frames before submit. The traced Gemma4 one-token run dropped from `456` frame traces with `350` empty frames to `106` frame traces with `0` empty-frame entries.
- Fused graph regions now enter planned Metal compute scopes. The graph executor opens a planned region scope for attention-output residual, FFN residual, and PLE residual. If the partition-level frame has already been flushed by preparation/runtime paths, the planned scope owns a small frame and submits it safely at scope exit.
- The traced Gemma4 one-token run now reports planned scopes in `105` of `106` frame traces, with no empty-frame entries:
  `graph_regions=120`, `graph_region_ops=635`, `graph_region_fallbacks=0`, `interpreter_fallbacks=0`, `host_outputs=0`.
- Current non-validation 8-token Gemma4 Metal compiled partitioned measurement:
  `prefill=1200ms`, `decode=4574ms`, `total=5775ms`.
- This is structural progress, not the final ggml-style execution model. The remaining bottleneck is still frame fragmentation: most graph regions submit one small planned frame each instead of one whole-frame command sequence.

## Next Required Slice
- Promote per-region planned scopes into a whole-frame `GraphCommandPlanView` executor for Gemma prefill/decode so compatible attention/FFN/PLE/tail regions share a command buffer and encoder scopes.
- Use `GraphCommandPlanView.scopes` as the source of truth for grouping; region-local scopes are the fallback, not the destination.
- Move per-frame quant descriptor/slot preparation out of execution hot paths; execution should reference prepared resident slots/descriptors.
- Add frame counters for real submitted frame count, empty-frame cancels, planned-scope count, and top frame-break reasons so regressions are visible without verbose `TERMITE_METAL_TRACE_FRAME=all`.
