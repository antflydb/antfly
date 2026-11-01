# Authoring native kernel programs

Put reusable numerical programs under operation-based owners. Model adapters
interpret checkpoint names and configuration; native programs consume physical
layouts, numerical contracts and admitted schedules. A new model should usually
add an adapter or a catalog entry, rather than another numerical kernel body.

The agent-facing rules are in `zig/pkg/inference/src/ops/AGENTS.md`.

| Operation | Existing owner | Static choices to preserve |
|---|---|---|
| Causal depthwise stencil | `src/ops/gpu/depthwise_program.h` | Weight order, history, fixed/dynamic kernel size, activation, separate product rounding and index width |
| Masked token-major pooling | `src/ops/gpu/masked_pool_program.h` | Mask optionality, independent coordinate/address widths, count ownership and per-term versus post-sum division; callers retain row normalization |
| Prefix L2 normalization and accumulated VJP | `cuda/prefix_normalization.zig`, `kernels/prefix_normalization_program.cuh` | Explicit input stride, dense output, untouched gradient suffix, four-accumulator reduction, clamp and division boundaries; static 768-stride compatibility schedule |
| Spatial depthwise residual | `src/ops/cuda/prototypes/vision_f16_programs.cu` | Native 3x3 traversal and explicit FMA/add boundaries |
| Native CUDA WMMA score products | `src/ops/cuda/kernels/wmma_tile_program.cuh` | F16/BF16 storage, B layout, depth, pitches, caller-owned scope and preloaded left-fragment reuse |
| Row statistics and normalization | `src/ops/cuda/kernels/row_program.cuh` | Warp/block, shared tree, four-accumulator training fold and fixed-256 warp-finish reductions; publication dtype |
| Rotary products and axes | `src/ops/cuda/kernels/rotary_program.cuh` and Metal rotary helpers | Axis-local versus head-wide halves, product rounding, frequency evaluation and upper-product order |
| MPP projection | `antfly_mpp_affine`, `antfly_mpp_split_projection`, `TERMITE_MSL_MPP_GATE_PROGRAM` in `metal_kernels.m` | Rows/columns, SIMD groups, split-K, projection planes, F32/BF16 storage and finite-gate epilogue |
| Gated delta recurrence | `metal_gdn_source.h`, `prototypes/gated_delta_recurrent.cu` | Register/shared staging, history ownership, epsilon placement and state-update schedule |
| Dense decoder tile attention | `ANTFLY_MSL_DENSE_Q16_*` and causal-score phases in `metal_kernels.m` | One/two-head ownership, direct half-device loads, staged tails and diagonal publication |
| Eight-query scalar SIMD attention | `ANTFLY_MSL_Q8_*` in `metal_kernels.m` | Fixed-256 versus tail tiles, token/head-major layout, mask and zero-denominator policies; identical staged shader phases |
| Vision tile attention | `ANTFLY_MSL_VISION64_*` phases in `metal_kernels.m` | Q16/Q32 geometry, half/full staging and Q-storage tail reuse |
| Segmented inference tile attention | `ANTFLY_MSL_SEGMENT_*` phases in `metal_kernels.m` | Wide/blocked row ownership, 8/16-key MMA and softmax schedules, explicit scales and GQA storage |
| Range/segment training attention | `range_attention_reference.zig`, `segment_training_attention.zig`, Metal segment fragments | Mask/control layout, replay/dropout, orientation, tile geometry and gradient ownership |
| Relative training attention | `kernels/relative_attention_training.cuh`, `relative_attention_tensor_*` and the GPU training renderer | Relative terms, bucket layout, deterministic replay and distinct tensor-core schedules |
| Dense attention physical storage | `src/ops/kernel_geometry.zig` (`DenseAttentionLayout`) | Token-major 2D and head-major 3D/4D contracts; reject mixed layouts and preserve backend compatibility defaults |
| CPU online attention rows | `linalg.attention.updateOnlineRow` | Conditional/scalar/SIMD maxima, empty-tile handling, rescale guards, vector width and statistic read/store order |
| Quantized Metal matrix phases | `ANTFLY_MSL_QUANT_SWIZZLE16`, `ANTFLY_MSL_MATRIX_4X1_*`, `ANTFLY_MSL_MATRIX_4X2_*` | Swizzle groups, scalar/vector acquisition, unroll and SIMD barriers, M32/M64 right-operand advance, output pitch and scratch publication |
| Wide CPU segment GEMM | `native.WideSegmentAttentionGemm` | 512-query BLAS tile, explicit key reuse; model admission remains in adapters |
| F32 attention input normalization and partial M-RoPE | `src/ops/cuda/attention_inputs_f32.zig` | Separate Q/K norms, gate extraction, coordinate axes and partial rotary width |
| BF16 encoder block phases | `src/ops/cuda/encoder_block_bf16.zig` | BF16/F32 publication boundaries, image segmentation, normalization and rotary schedules |
| BF16 projection epilogues | `src/ops/cuda/projection_epilogues_bf16.zig`, `prototypes/projection_dot_bias_bf16.py` | SM121 tiles, explicit FP32 bias rounding, GELU versus residual epilogue and input/output precision |
| BF16 projection with top-1 reduction | `src/ops/cuda/projection_top1_bf16.zig` | Fixed K1024/N248320 schedule, suppression policy, two-stage reduction and 32768/16-byte shared memory |
| Embedded CUDA program ownership | `src/ops/cuda/embedded_module.zig` | Transactional entry lookup, lazy per-image ownership and direct caller-owned launches |
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

Use `native/cuda_phases.zig` and `native/metal_phases.zig` for exact shared
indexing, score, reduction and publication phases. Expand readable MSL before
`program.metal` encoding; existing Objective-C owners use the shared boundary
representation. Keep specialization tags in their instance context. Tags use
identifier names so native aggregate initializers stay literal. For this form of
source sharing, freeze and compare the complete native compiler inputs byte for
byte; shrinking the authored phase copies does not shrink the emitted kernels.

Paged split attention uses the static admission policies in
`native/metal_attention_gqa_split.zig`: extend those stage/merge instances for
an admitted layout instead of copying the kernels into a model owner. Dense
F32/F16/BF16 scalar/reduction exports share `metal_phases.dense_rows`; keep the
weight storage type explicit so the original precision overload is retained.
CUDA's online-softmax family retains distinct shared-half and register-F32
probability publication. Sharing a template does not permit merging those
rounding or scratch-lifetime contracts.

Q4/Q8 paired-activation and fixed-row projection helper definitions belong to
their generated specialization families. Their shared dual-dot phases retain
explicit dense U32 versus routed U64 indexing and full/tail scopes. Keep these
width and schedule contracts when adding an instance. Before removing a
computed helper fragment, inspect both its generator consumers and references
in the complete native bundle plus recursively included project headers.

CUDA multi-row Q4/Q8 linear and QKV programs share `cuda_phases` row
accumulation/publication with explicit operand-array extents and warp counts.
Keep the named two-row operand schedule separate unless new qualification
supports changing its register contract. Metal Flash value/output phases retain
explicit key-tile pitch, head width, output owners and launch stride; their
score/softmax schedules remain independently admitted.

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

Use the shared `qualify_kernel_families.py` runner and native manifest executors
for model and operator comparisons. `check_cuda_native_programs.py` compares
instructions and resources across retained architectures. Their interfaces are
documented in `zig/pkg/inference/scripts/README.md`. Build-integrated checks and
regression tests remain with their current owners.

## Tile descriptions and native ownership

The portable Tile IR in `src/tile/` is a prototype with CPU vector lowering,
Metal/CUDA emitters and scalar references. Its model descriptions are adapters;
they do not replace the qualified native schedules by default. The optional
Whisper logits hook is qualified separately from the production library.

`families/native.zig` registers native source specializations. Extend an existing
operation-named owner, preserving its layout, precision and static schedule.
`native_program.zig` expands descriptions before native compilation. Keep Tile
regions disjoint from `quant-kernel-codegen` regions: each has one generator.
Both freshness checks are required. New Tile files must enter the generated
`source_inputs.zig` manifest and the enabled backend qualification identities.

Run `zig build tile-kernel-codegen quant-kernel-codegen test-tile-kernels` and
`zig build check-tile-whisper` for host checks. The latter compiles the prototype
CLI without requiring a model. `scripts/check_metal_tile_programs.m` compiles all
prototype Metal kernels and checks the actual opt-in catalog hook against an
independent logits reference, canaries and optional AA/AB/BA timing controls.
Moving existing programs into generated regions earns no reduction credit; all
Tile renderer and native specialization sources remain authored production code.

## Native source files and offline programs

Direct Metal/CUDA exports belong to a generated region. Freshness checking
rejects new direct or ABI-macro exports outside generator ownership. Fourteen
legacy ABI shells contain quant-owned numerical regions and retain that owner.
Do not add a second generator around them.

New native families can keep readable `.cu` or `.metal` source under
`src/tile/families/native/`. Use `cudaSourceFamily` or `metalSourceFamily` and
`// native-program:entry SYMBOL` descriptions. Source order determines the
family's region order. The extractor validates unique names and boundaries;
Metal encoding happens once at the Objective-C boundary. Existing Objective-C
macro splices use `encodedMetalSourceFamily`. These files participate in the
checked source-input manifest and backend qualification identity.

`src/tile/cuda_standalone.zig` owns offline CUDA image identities, export names
and fixed launch resources. Tile codegen exports its JSON for the Python
producers; host launch owners consume the same typed catalog. Shared producer
support enforces pinned compiler, export and zero-scratch/resource contracts.
Normal serving does not require Python, Triton or source parsing.

On the qualified Linux toolchain, run:

```sh
python3 -B zig/pkg/inference/scripts/regen_cuda_standalone.py --report RECEIPT.json
```

The default checks all seventeen catalog images byte-for-byte. `--write` publishes images
only after every image's export contract passes. Scratch and compiler caches
stay in the worktree. This path requires NVCC 13.0.88 and Triton 3.7.1; canonical
multi-architecture fatbin generation separately uses NVCC 13.2.86.

## CUDA artifact size and export ownership

Source sharing and binary size are separate measurements: templates can still
instantiate every original program. Before removing an export, inspect computed
host lookups and planner/benchmark identities as well as textual call sites.
Consolidate identical programs only after checking parameter layouts, native
instructions and resources on every supported architecture. Preserve public
model adapters around operation-named launch ownership when needed.

`regen-cuda-artifacts.sh` preserves all seven native targets and compute75 PTX,
and uses fast compression for every fatbin image. That format requires CUDA
12.4-compatible or newer drivers at the format level; the canonical runtime
enforces CUDA Driver API 13.0 for native images and 13.2 for bundled PTX JIT.
See the inference README for deployment requirements. Compression trades about
1 ms of measured module
loading/first-launch time for a substantially smaller packaged image. Validate
exports through `cuobjdump` entry symbol tables; compressed blobs cannot be
checked reliably with raw byte searches. Portable PTX and standalone SM89 remain
separate profiles. Record packaged size, native code/resource identity and
startup cost separately in qualification receipts.

Model comparisons use exact output identity by default. If a model has an
explicit numerical budget, pin an independently generated F32 reference by SHA-256
and declare its absolute tolerance in the qualification case. Both revisions
must meet that reference; the baseline is never implicitly trusted as an oracle.
A baseline reference failure remains a failure, even when the candidate passes.
Export full and chunked-prefill logits to check state/position handling against
the same independent decoder reference without widening the chunk parity gate.

Standalone CUDA images share `EmbeddedModule` for fixed images and
`SelectedModule` for architecture-selected images. A dense export enum can be
used directly when its names are the device ABI. Use `initCurrent` or
`loadCurrent` when the caller has already selected the context; preserve the
qualified placement of context calls and cached returns. Keep launch geometry
and parameter packing with the tuned schedule. Retire children through checked
APIs before resetting their parent, so a failed unload remains retryable.

The standalone catalog now owns 17 PTX images, including activation, paged
attention, recurrence, vision and encoder/decoder families. Native NVCC flags
are per-family precision policies. `cuda-standalone-check -- --family <name>`
checks one exact family without requiring unrelated producer dependencies.
Historical image filenames and exported symbols are compatibility contracts;
new producer names describe operations and physical schedules.

## Consumed ownership inventory

`zig build check-kernel-ownership` runs both generator freshness checks, exports
an inventory from their actual registries, and verifies every recorded region
and generated file against the shipped source. CI runs this check and its
negative controls. An absent, stale, duplicated, overlapping or unregistered
region fails the check. Native owner paths and registered families reject known
model-name tokens; use operation, dtype, layout and static schedule names when
adding an implementation. Compatibility symbols can retain their historical ABI.

For a retained report, run from the inference package:

```sh
zig build quant-kernel-codegen -- --inventory /tmp/kernel-ownership-inputs.json
python3 -B scripts/kernel_ownership_inventory.py --root ../../.. \
  --inventory /tmp/kernel-ownership-inputs.json --output /tmp/kernel-ownership.json
```

The report includes both registries' regions/files, standalone CUDA producers,
Tile source identities and the Q4/Q8 row schedule contracts. Canonical source
surrounds and recursively included native leaves are hashed and classified as
retained authored implementation. Include/template/macro references remain
consumers; a textual single-reference count never authorizes deletion.

`tile/q4_q8_schedule.zig` supplies the qualified two/four/eight-row linear and
four-row QKV extents to native emission and actual launch adapters. The native
ABI determines the named argument records. Its shared-memory sizes describe
static compiled storage, not additional dynamic launch scratch. Keep the named
two-row operands: replacing them with arrays increased registers in qualification.
New schedules require independent output/tail/replay checks and resource/performance
evidence before extending this contract.
