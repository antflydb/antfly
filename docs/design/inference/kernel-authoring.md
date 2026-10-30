# Authoring native kernel programs

Put reusable numerical programs under operation-based owners. Model adapters
interpret checkpoint names and configuration; native programs consume physical
layouts, numerical contracts and admitted schedules. A new model should usually
add an adapter or a catalog entry, rather than another numerical kernel body.

The agent-facing rules are in `zig/pkg/inference/src/ops/AGENTS.md`.

| Operation | Existing owner | Static choices to preserve |
|---|---|---|
| Causal depthwise stencil | `src/ops/gpu/depthwise_program.h` | Weight order, history, fixed/dynamic kernel size, activation, separate product rounding and index width |
| Spatial depthwise residual | `src/ops/cuda/prototypes/vision_f16_programs.cu` | Native 3x3 traversal and explicit FMA/add boundaries |
| Row statistics and normalization | `src/ops/cuda/kernels/row_program.cuh` | Warp/block, shared tree and fixed-256 warp-finish reductions; publication dtype |
| Rotary products and axes | `src/ops/cuda/kernels/rotary_program.cuh` and Metal rotary helpers | Axis-local versus head-wide halves, product rounding, frequency evaluation and upper-product order |
| MPP projection | `antfly_mpp_affine`, `antfly_mpp_split_projection`, `TERMITE_MSL_MPP_GATE_PROGRAM` in `metal_kernels.m` | Rows/columns, SIMD groups, split-K, projection planes, F32/BF16 storage and finite-gate epilogue |
| Gated delta recurrence | `metal_gdn_source.h`, `prototypes/gated_delta_recurrent.cu` | Register/shared staging, history ownership, epsilon placement and state-update schedule |
| Dense decoder tile attention | `ANTFLY_MSL_DENSE_Q16_*` and causal-score phases in `metal_kernels.m` | One/two-head ownership, direct half-device loads, staged tails and diagonal publication |
| Vision tile attention | `ANTFLY_MSL_VISION64_*` phases in `metal_kernels.m` | Q16/Q32 geometry, half/full staging and Q-storage tail reuse |
| Segmented inference tile attention | `ANTFLY_MSL_SEGMENT_*` phases in `metal_kernels.m` | Wide/blocked row ownership, 8/16-key MMA and softmax schedules, explicit scales and GQA storage |
| Range/segment training attention | `range_attention_reference.zig`, `segment_training_attention.zig`, Metal segment fragments | Mask/control layout, replay/dropout, orientation, tile geometry and gradient ownership |
| Relative training attention | `kernels/relative_attention_training.cuh`, `relative_attention_tensor_*` and the GPU training renderer | Relative terms, bucket layout, deterministic replay and distinct tensor-core schedules |
| Dense projection admission | `dense_projection_policy.zig` | Workload, rows, output width, quant layout and packed-expert exclusion |

Paths above are relative to `zig/pkg/inference`; Metal helpers live under
`src/backends`. CUDA encoder source owners are `encoder_multimodal_programs.cuh`
and `encoder_fused_programs.cuh`, rather than a particular model's name.

## Names and compatibility

Use operation, storage and schedule names for new helpers, entrypoints and source
owners. For example, `antfly_qkv_rope_two_axis_bf16` describes its contract;
a model-prefixed copy would hide that reuse opportunity. Keep shape variants
under the existing static family and catalog.

Some exported CUDA symbols and graph operations retain historical names for
compatibility. They are adapters to shared programs, not naming examples for new
kernels. Renaming a private entrypoint requires updating every lookup, catalog,
artifact symbol check, source identity and validation tool that consumes it.

`RangeTrainingAttentionAttrs` owns the deterministic range-attention layout;
`ModernBertTrainingAttentionAttrs` is its compatibility alias. The pre-existing
`SegmentTrainingAttentionAttrs` includes dropout and has a different layout
contract. Preserve these differences rather than interchange types by name.

New model loaders can populate `QuantizedStorage.dense_workload` directly.
`models/projection_workload.zig` is a compatibility adapter for checkpoint naming
conventions. Explicit workload metadata is authoritative; unknown source names
leave canonical operation hints available. Promotion still uses the existing
cache budgets, ownership guards and fallback behavior. A workload hint does not
permit bypassing cache lifetime or allocation checks.

## Sharing without losing a schedule

Share matching phases, retaining the native geometry and storage that make each
schedule efficient. Prefer compile-time traits or an existing source fragment.
The MPP gate fragment emits the same native programs for two storage/publication
contracts. The segmented-attention backward score fragment retains the callers'
original barrier sites and distinct dQ versus dK/dV accumulation directions.

The portable causal stencil is compiled directly as CUDA and stringified as MSL.
It is not a runtime interpreter. Protect emitted source from host SDK macros and
reserved shader identifiers. Separate-product rounding has an explicit CUDA
intrinsic implementation and a Metal contraction/reassociation boundary; its
near-cancellation test runs in both fast and safe math modes.

Frequency evaluation, BF16 product rounding, finite-input policies and reduction
association are part of the numerical contract. Similar operation names do not
make different contracts interchangeable. Keep a native schedule when sharing
would alter its performance or arithmetic.

## Qualification and accounting

Freeze the original programs before replacement. Compare original bits and
independent references across tails, layouts, masks, reset/continuation and
rounding boundaries. Check native registers, stack/local/shared storage and
retained MMA paths. Measure on actual Metal and CUDA hardware with paired orders
and controls; retain misses and distinguish shared-GPU measurements from isolated
results. Source-preserving changes can also prove emitted program identity.

New includes must enter the JIT/build identity closure. Relocated validation
sources must retain their native include closure; the CUDA oracle generators use
`lightonocr3/native_source_closure.py` for that purpose.

Report maintained device code, host policy, tests/docs, generated text and image
bytes separately. Count every shared owner once. Renaming a file or generating
several copies of one body does not itself reduce authored code. Use
`scripts/kernel_source_accounting.py --revision BASELINE --output RECEIPT`.

`check_metal_segment_blocks.m` can freeze the main shader with `--dump-source`
and validate a source-phase refactor with `--require-source-identity`. Check
authored owners before estimating savings from expanded-shader similarity.

`check_metal_dense_tiles.m` applies the same freeze/identity workflow to decoder
and vision tiles, with separate half-stage and full-F32 reference contracts.
