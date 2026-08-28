# Quant Kernel Compiler

## Scope

The quant kernel compiler is a build-time Antfly inference tool for GGUF quant
kernels. It is deliberately small: descriptors, tiny IR, backend-specific
lowering metadata, generated source files, route manifests, and evidence gates.
Its original scope was small-batch quant matmul; it now carries an **op-kind**
dimension (see below) so the same single-sourcing + conformance-evidence
discipline covers non-matmul kernels — fused microkernels (RMSNorm today; rope /
KV read-write next) and, as they land, a family of narrowly-routed attention
kernels.

It is not a runtime JIT and not a general GPU compiler. Production runtime must
not depend on Python, Triton, CUDA toolkit, cuBLAS, XLA, or network access.

**Explicit non-goal — general/large dense GEMM.** Large FP16/BF16/f32 GEMM stays
on vendor libraries (MPS / Accelerate / cuBLASLt / CUTLASS); those are hard to
beat and are not worth generating. The hand-written `termite_*_linear_mm_sg`
tensor-core kernels are ggml-port-grade and stay hand-written. Generation targets
the cases where the shape/layout matrix *explodes* and hand-written code is
*multiplying*: quant matmul + epilogue coverage, attention route variants, and
fused attention-adjacent microkernels — not a replacement for library GEMM.

## Source Of Truth

The dispatch-facing API is still the existing graph planning contract:

- `src/graph/quant_matmul.zig`: format, row bucket, dispatch bucket, and generic
  quant matmul planning vocabulary.
- `src/graph/quant_kernel_op.zig`: dependency-light operation and epilogue tags
  shared by the compiler and backend renderers.
- `src/graph/quant_kernel_compiler.zig`: compiler descriptors, IR, generated
  artifacts, route registry, benchmark manifests, promotion evidence policy, and
  the `metal_production_schedules` table (the single source for each generated
  Metal route's dispatch schedule: threads/cols/reduction).
- `src/graph/quant_kernel_metal_renderer.zig`: the descriptor-driven Metal
  renderer. `renderKernel(kernel_id, decoder, schedule, epilogue)` builds a full
  standalone MSL kernel from one canonical small-batch skeleton parametrized by
  the schedule, with per-format dequant math supplied as `FormatDecoder` leaf
  fragments over a shared `antfly_qk_*` vocabulary. This is what makes a route's
  body a function of its schedule rather than frozen text.
- `src/graph/quant_kernel_cuda_renderer.zig`: the typed CUDA renderer. Each
  artifact selects a validated `KernelLowering` and exact launch contract (grid
  mapping, output tile, threads, static/dynamic shared memory, and dimension
  constraints). The Q4_0 MMV/MM/pair family shares one parameterized projection
  skeleton; fixed fused-FFN bodies remain explicit specializations behind exact
  640-thread/256-thread and model-dimension contracts.
- `src/backends/metal_kernels.m`: native Metal runtime and provider lowering.

Production Metal compiles the inline kernel copies embedded in
`metal_kernels.m`. Those copies are **single-sourced from the renderer**: there
are no hand-pasted body constants. `renderMetalRuntimeQuantRegion` builds the
region by rendering each route from `metal_production_schedules` (schedule +
`FormatDecoder` + epilogue) via `renderRuntimeRegion`, and the checked-in
`.metal` files render from the *same* renderer at comptime
(`renderMetalSmallBatchSource`, and `renderMetalMicrokernelSource` for
microkernels). `zig build quant-kernel-codegen -- --write` writes both the
`.metal` files and the marker-delimited region of `metal_kernels.m`
(`// quant-kernel-codegen:begin/end generated quant kernels`); `--check` verifies
both byte-for-byte. So a schedule edit fully regenerates the kernel text and the
runtime copy, the checked-in sources, and the source fingerprints cannot drift by
construction — re-tuning a route is a table edit + `--write`, never a hand-paste.
(The former frozen `metal_rt_body_*`/`metal_rt_helper_*` constants,
`metal_runtime_quant_sections` table, and `metal_runtime_body_pins` hash table are
all gone.) The runtime-validated tuned text is the canonical text: runtime-route
correctness and promotion timing evidence are measured against the same bytes that
`xcrun` source checks compile. One shared helper (`termite_q8_0_block_scale`) is
owned by the handwritten termite kernels outside the region; the codegen check
verifies the compiler's duplicated copy still matches it.

Generated artifacts are checked in:

- `src/ops/metal/generated/*.metal`: generated Metal candidates (promoted
  kernels included; promotion is recorded in the manifest, not by a separate
  file copy).
- `src/ops/cuda/generated/quant_kernel_*.cu`: generated CUDA candidates.
  These files are rendered from typed plans rather than stored source templates.
  Benchmark-qualified CUDA kernels and explicitly runtime-wired dev candidates
  are also embedded in the
  `src/ops/cuda/artifacts/inference_cuda_kernels.{cu,ptx,fatbin,_sm89.cubin}`
  bundle. The dev candidates remain default-off and keep
  `production_enabled: false`; standalone generated files are never direct
  artifact inputs. Compiler tests validate every renderer-owned helper fragment
  and complete kernel body against the canonical bundle source.
- `src/ops/cuda/generated/evidence/*.json`: checked-in CUDA promotion
  benchmark evidence.
- `src/ops/cuda/generated/quant_kernel_*.json`: spec, artifact, benchmark, and
  conformance manifests. The path is historical; the manifests cover both CUDA
  and Metal.

`zig build quant-kernel-codegen -- --check` verifies the single generated
source path for every manifested artifact plus the marker-delimited runtime
region of `metal_kernels.m`. Stale or missing generated files or a drifted
runtime region fail the codegen check instead of becoming a packaging surprise
during review.

## Op-Kind Framework

`OpKind { small_batch_matmul, attention, microkernel }` (`quant_kernel_compiler.zig`)
is the routing dimension that selects which skeleton, conformance reference, and
dispatch a generated route uses. The renderer, evidence, and manifest machinery
are keyed by `kernel_id` and are op-agnostic; `op_kind` lives on the shared
`GeneratedArtifact`. Matmul-specific descriptors (`QuantKernelSpec`,
`QuantKernelLowering`) are deliberately *not* threaded with op-kind — attention
and microkernels do not reuse them.

Key design rule: **keep the matmul path byte-identical.** Non-matmul routes live
in the authoritative `first_generated_artifacts` registry as a tagged
`GeneratedOp`. Matmul promotion and benchmark code consumes the derived
`first_generated_matmul_artifacts` compatibility view; attention and microkernel
views are derived from the same registry. The codegen routing seam is
`compiledSourceForArtifact` (`native_quant_kernel_codegen.zig`), which switches
on `artifact.opKind()`.

Rendering: `RegionKernel` is op-kind-aware (defaulted fields keep matmul
construction byte-identical); `renderRuntimeRegion` dispatches body rendering per
op-kind while still globally deduping the shared helper vocabulary across all
op-kinds. `renderMicrokernel` / `renderRmsNormKernel` are self-contained (no
`FormatDecoder`, no epilogue). The `.metal` file for a microkernel is single-
sourced the same way as matmul, via a comptime `renderMetalMicrokernelSource`
constant.

**First non-matmul route (landed): RMSNorm** (`op_kind = .microkernel`, kernel
`antfly_rms_norm_generated_msl_v1`, `src/ops/metal/generated/microkernel_rms_norm.metal`).
Pure f32, one threadgroup per row, threadgroup-tree reduction. It is opt-in
(`TERMITE_METAL_ENABLE_RMS_NORM_GENERATED`); when requested, the regular rows
encoder routes through the generated kernel and records `generated_rms_norm`.
Pipeline creation fails closed if the requested function cannot be built. The
hand-written `termite_apply_rms_norm_rows` stays the default production
baseline. Conformance runs on
device via a sister FFI (`termite_metal_run_generated_microkernel_check`) against
a self-contained CPU reference (validated bit-identical vs the hand-written kernel,
`max_abs_error ≤ ~3e-6` across `n∈{1,2,3,4} × d∈{64,128,512,2048,4096}`).

**Attention (landed as dev-only candidates).** The compiler owns generated
decode-1x paged-attention and flash-prefill routes under `op_kind = .attention`,
with `AttentionSchedule` and the shared Metal attention renderer providing the
lowering. CUDA also has a typed `AttentionRenderPlan` for dense GQA decode,
specialized to `q_seq_len=1`, `head_dim=256`, and the nullable device-scalar ABI.
Its standalone `.cu`, manifest launch contract, and runtime-bundle body are
drift-checked from the same renderer. The CUDA route is gated by
`ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE=1`, fails closed when its
symbol is missing, and records `launch_attention_gqa_decode_generated`.
Because attention feeds the whole model, their correctness gate is
**bit-identical model tokens** (`scripts/gemma4/compare_metal_gemma4_e4b_qat.sh` oracle
text + greedy token-id prefix), not raw-float conformance — argmax is robust to
summation-order drift. Promotion also requires reviewed performance evidence
across representative models, context lengths, masks, and sliding windows.

## Compile Flow

The internal compile API is intentionally small. Callers build or look up a
`QuantKernelCompileRequest`, then use `compileQuantKernelSource(...)` or the
Metal convenience wrapper `compileMetalKernelSource(format, row_bucket,
epilogue)`. The result ties together the descriptor, IR, route lowering,
generated artifact metadata, canonical source text, check command, source
path, and runtime gate.

`compileQuantKernelSource(...)` is also a guardrail. Before returning a compiled
source record, it verifies that the descriptor, IR, route lowering, generated
artifact, canonical source text, source path, check command,
production bit, and Metal runtime gate all describe the same route. Drift returns
`null` and fails the codegen path instead of emitting an orphan kernel.

Typical Metal use is:

```zig
const compiled = quant_kernel_compiler.compileMetalKernelSource(.q6_k, .rows_2_8, .bias_gelu) orelse
    return error.UnsupportedQuantKernelRoute;
const emitted = try quant_kernel_compiler.emitCompiledSource(allocator, compiled);
defer emitted.deinit(allocator);

// emitted.data is the canonical generated source text.
// compiled.source_path, check_command, runtime_gate_env,
// artifact.runtime_default_enabled, and production_enabled describe how that
// source is checked and routed.
```

`emitCompiledSource(...)` is the source emission boundary. Metal small-batch
families return the canonical assembled source: license header, plan metadata,
the helper constants the kernel references, and the runtime-region kernel body
byte-for-byte. CUDA families render allocator-owned source from the artifact's
typed backend plan. Generated files stay checked in, production dispatch stays
artifact-backed, and full-file golden checks keep source fingerprints and
promotion evidence stable.

The codegen executable is always built for the build host, not the selected
runtime target, so source checks still work during Linux/CUDA cross builds.

Current single-sourced Metal families:

- `Q2_K` small batch: `none`, `bias`, `bias_gelu`.
- `Q3_K` small batch: `none`, `bias`, `bias_gelu`.
- `Q4_0` small batch: `none`.
- `Q4_1` small batch: `none`.
- `Q4_K` small batch: `none`, `bias`, `bias_gelu`.
- `Q5_0` small batch: `none`.
- `Q5_1` small batch: `none`.
- `Q5_K` small batch: `none`, `bias`, `bias_gelu`.
- `Q6_K` small batch: `none`, `bias`, `bias_gelu`.
- `Q8_0` small batch: `none`, `bias`, `bias_gelu`, `relu`.
- `Q8_1` small batch: `none`.
- `Q8_K` small batch: `none`.

The focused compiler test proves every emitted source matches its checked-in
full-file golden. Every generated Metal source ends with its runtime-region
kernel body byte-for-byte; every CUDA source is freshly rendered and owned by
the emission result. A separate CUDA production-bundle test validates route
metadata, every helper fragment, and every complete promoted body.

1. Add or change a `QuantKernelSpec` in
   `src/graph/quant_kernel_compiler.zig`.
2. Add or update `GeneratedArtifact` route metadata.
3. For a Metal small-batch family, add a `FormatDecoder` + a
   `metal_production_schedules` entry; the renderer generates the body at
   `--write` time (no hand-written body/helper constants, no section table). For
   CUDA, add a typed `KernelKind`/`KernelLowering`, its exact launch contract,
   and a renderer body family or specialization. Do not add a whole-source
   template.
4. From `zig/pkg/inference`, run:

   ```sh
   zig build quant-kernel-codegen -- --write
   ```

5. Review the generated Metal/CUDA manifest diffs.
6. Run the focused compiler tests:

   ```sh
   zig build test -Dmetal=true -Dcuda=false -- --test-filter "quant kernel compiler"
   ```

7. Run the Metal gates before treating any Metal route as locally ready:

   ```sh
   zig build quant-kernel-metal-production-regression-check -Dmetal=true -Dcuda=false
   zig build quant-kernel-metal-local-check -Dmetal=true -Dcuda=false
   zig build quant-kernel-metal-industry-local-check -Dmetal=true -Dcuda=false
   ```

   These Metal evidence targets require a macOS target with `xcrun`/Metal
   available. On non-macOS targets they fail closed instead of passing as
   source-only checks.

   The generic `quant-kernel-local-check` stays usable on non-macOS for
   generated-source freshness and CUDA source-policy checks. On macOS it also
   depends on the Metal local gate.

## Schedule Autotuning And Promotion Workflow

A generated Metal route's dispatch schedule (threads per threadgroup, columns
per threadgroup, reduction strategy) lives in `metal_production_schedules`, and
the renderer emits the kernel body from it. That makes schedules tunable:
`quant-kernel-metal-sweep` renders every valid schedule variant for a route,
benchmarks each on-device against the current production kernel, and reports the
best worst-shape winner as `antfly.quant_kernel_metal_sweep.v1` evidence.

```sh
# Sweep all routes (or one) — read-only, changes nothing:
zig build quant-kernel-metal-sweep -Dmetal=true -Dcuda=false -- \
  --measure-iters 300 --repeat-runs 3 --sweep-evidence-out /private/tmp/sweep.json
zig build quant-kernel-metal-sweep -Dmetal=true -Dcuda=false -- --sweep-route q6_k/bias
```

**Critical caveat — the sweep is a directional filter, not a magnitude oracle.**
It times kernels through the standalone one-shot-command-buffer harness, whose
large fixed per-iteration overhead compresses (and can mis-rank) speedups. It
massively under-reports the real win — a route the sweep rates 1.03x can be ~2.5x
faster in the decode runtime. Always confirm a candidate schedule with a
decode-runtime measurement before promoting:

```sh
# Probe a route's real decode-runtime speedup vs handwritten (evidence path
# MUST contain the kernel_id):
zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- \
  --promotion-ready-kernel <kernel_id> --repeat-runs 5 --measure-iters 500 \
  --evidence-out /private/tmp/probe-<kernel_id>.json
```

**Re-tuning an already-promoted route** (change its schedule, keep it promoted):
edit its `metal_production_schedules` entry, update the schedule-marker guardrail
tests (e.g. the hybrid reduction's `lane_id >= {N}u` where `N = threads/32`), then
`quant-kernel-codegen -- --write` — the renderer regenerates the body, the
`.metal` file, and the runtime region from the new schedule (no hand-paste of a
body constant), and run the production-regression gate. The gate's decode-runtime
timing is the real arbiter — verify a genuine improvement via a before/after A/B
(`git stash` the change, re-run the gate), not the sweep.

**Promoting a candidate to production** additionally requires: flip the
`GeneratedArtifact.production_enabled` to true and update the source constant's
header; add a `first_metal_runtime_evidence` entry and remove the
`first_metal_promotion_blocker_evidence` entry (the production-regression suite
then auto-includes the route via `artifactHasPromotionEvidence`). Qualification
does not imply rollout: leave `runtime_default_enabled = false` and the positive
`ENABLE_*` gate in place until normal model-path release evidence supports a
default-on route. At that point, set `runtime_default_enabled = true` and swap
the `metal_kernels.m` dispatch gate to `promoted_gate(DISABLE_*)`. Fused-bias
routes must also be called by normal model execution before rollout; direct
harness and provider API coverage alone are shadow-route evidence. Then update
the guardrail count tests (evidence/blocker/route-summary counts, manifest
rollout fields, route-lowering pins, and `ENABLE`/`DISABLE` env counts).

## Promotion Policy

Generated kernels start as dev candidates. A candidate may become production
only when all of these are true:

- Checked-in generated source matches the manifest fingerprint.
- Runtime route evidence proves the generated route is actually selected.
- Provider route evidence exists when the provider surface exposes the same
  candidate.
- Promotion evidence is sequential, uses `--repeat-runs 5 --measure-iters 500`,
  and names the exact kernel in the evidence path.
- Repeated promotion and production-regression checks run two unrecorded warmup
  repeats before the measured repeats; evidence records this as
  `warmup_repeat_runs`, and generated manifests expose
  `metal_promotion_warmup_repeat_runs`, so stale evidence is rejected when the
  policy changes.
- Promotion evidence times the generated decode-runtime route in an active-frame
  batch when that route is selected, while still running the standalone
  generated source check for compile/correctness coverage.
- Every promoted case clears the minimum speedup gate and the worst-repeat
  speedup gate.
- `quant-kernel-metal-production-regression-check` passes after promotion.

Cleared blocker evidence is not enough by itself. It is only a signal to
investigate. Production promotion requires checked-in evidence plus the
production-regression gate.

CUDA promotion uses the same shape with CUDA-specific mechanics: the canonical
generated source remains under `src/ops/cuda/generated/`, while the production
artifact source and compiled bundle remain under `src/ops/cuda/artifacts/` and
are checked byte-for-byte against the promoted generated bodies. The benchmark
command is sequential `bench-cuda` with `--warmup-iters 5 --measure-iters 50
--quant-compiler-repeat-runs 3` and an `--quant-compiler-evidence-out` path, the
measured geomean speedup must clear `minimum_speedup` (1.0) against the named
handwritten baseline with CPU-reference correctness inside tolerance, and a
matching comptime `BenchmarkEvidence` record plus the checked-in evidence JSON
are required before `production_enabled` may flip. Qualified kernels share the
driver-loaded artifact bundle with compiler-managed, default-off dev candidates;
the manifest preserves that distinction. Runtime dispatch stays driver-only
(`cuModuleLoadDataEx`; nvcc is dev-time only).

## Current CUDA State

The current checked-in state has 5 promotion-evidenced generated CUDA
artifacts, all Q4_0, measured on an NVIDIA L4 (driver 580.159.03, CUDA 13.2
nvcc; evidence JSONs under `src/ops/cuda/generated/evidence/`). Their runtime
routes remain explicit opt-ins pending model-level release evidence:

| kernel | plan | handwritten baseline | measured speedup (geomean) |
|---|---|---|---|
| `antfly_q4_0_mmv_f32_v1` | `cuda/q4_0/rows_1/none/mmv` | `termite_linear_q4_0_f32_tile4` | 1.18 (worst shape 1.02, LM head 1.30) |
| `antfly_q4_0_mm_f32_v1` | `cuda/q4_0/rows_9_64/none/mm` | `termite_linear_q4_0_f32` | 5.91 eligible-shape geomean (worst 2.62; 0.75 at in_dim=256 is excluded) |
| `antfly_q4_0_pair_mmv_f32_v1` | `cuda/q4_0/rows_1/pair/mmv` | `termite_linear_q4_0_pair_nobias_f32_tile4_w4` | 1.29 |
| `antfly_q4_0_pair_activation_q8_1_mmv_v1` | `cuda/q4_0/rows_1/pair_activation/mmv` | `termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn` | 1.28 |
| `antfly_q4_0_down_q8_1_mmv_v1` | `cuda/q4_0/rows_1/gated_down/mmv` | `termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down` | 1.31 |

The `pair_activation` and `gated_down` epilogues describe the tuned Gemma4
E4B decode FFN contract (q8_1-quantized activations via DP4A; `pair_activation`
also fuses the activation multiply and q8_1 output requantization; both are
hardcoded to the 2560/10240 E4B dims and CUDA-only). Their win over the tuned
handwritten kernels comes from fetching q4_0 weight words as aligned u16 pairs
instead of four single-byte loads. They dispatch inside the existing opt-in
`ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE` path and also
require their own `ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR_Q8=1` or
`..._DOWN_Q8=1` opt-in.
Measured e2e on the tuned E4B QAT llama.cpp pair harness: about -2.1% Antfly
E2E median and +3% decode from the pair kernel (the down kernel's increment is
below the harness noise floor); the paired margin vs llama.cpp roughly halved.
The `pair_activation` output comparison is quantization-aware: the harness
bounds the dequantized output diff by the global amax quantization step, which
is looser than the manifest's `correctness_tolerance_abs` field (that field
applies to the f32-output targets).

Speedups are geomeans across the Gemma4 E2B QAT dispatch shapes recorded in the
bench targets (`bench-cuda --quant-compiler-q4-0-{mmv,mm,pair}-ptx`), with
CPU-reference and baseline-output max-abs-diff at or below 3e-6. The mm
evidence also records a losing shape: `in_dim=256` (E2B PLE projection) runs at
about 0.75x the handwritten baseline, so the runtime route only claims rows
9..64 shapes with `in_dim >= 512` (`cuda_q4_0_generated_mm_min_in_dim` in
`cuda_compute.zig`); narrower shapes stay on the handwritten kernel. The mmv
bench additionally cross-checks the Gemma4 LM-head shape (`1536 -> 262144`)
generated-vs-baseline on device (the CPU reference is skipped above
`quant_compiler_q4_0_max_reference_out_dim` because it dequantizes the full
weight tensor to dense f32).

End-to-end impact, Gemma4 E2B QAT (`--backend cuda`, 128 tokens, generated
kernels explicitly enabled vs disabled, interleaved runs, bit-identical output):

- prefill: 146-151 ms vs 200-201 ms (about -25%).
- decode: 61.1/60.8 tok/s vs 59.6/59.5 tok/s (about +2.4%).
- end-to-end total: 2241/2257 ms vs 2349/2353 ms (about -4.5%).
- per-run generated launches: 16256 mmv + 92 mm + 4445 pair, zero fallbacks.

Q4_K-only models are unaffected by construction (the promoted routes are keyed
to Q4_0): clipclap embeddings are bit-identical with the kernels on/off (all
quant matmuls ride the q4_k tensor-core route), and gliner2 passes the
`scripts/gliner2/verify_gliner2_cuda.sh` native/CUDA parity gate with identical entity
counts and warm extraction timing unchanged (12.0 vs 12.1 ms).

Runtime dispatch for the five promoted CUDA routes is default-off. Their
manifest records keep `production_enabled: true` for benchmark qualification,
set `runtime_default_enabled: false`, and report the positive opt-in variable
as `runtime_gate_env`. Enable only
the route under evaluation with `ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MMV=1`,
`..._MM=1`, `..._PAIR=1`, `..._PAIR_Q8=1`, or `..._DOWN_Q8=1`; target
restrictions still apply. The corresponding per-kernel `DISABLE_` variables
override those opt-ins. `ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0=1` is the master
rollback and disables every generated Q4_0 route, including FFN candidates.
The two `_Q8` kernels additionally require the opt-in
`ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE` tuned path.
Actual launch counts are reported by `generate --print-timing` as
`cuda_q4_0_generated_counts:` with per-kernel hit/fallback counters (also in
`--json-timing`); a fallback dispatches the handwritten baseline for that
call.

The Q4_0 `pair` epilogue (two no-bias projections sharing one input) is
CUDA-only; Metal is carved out in `supportsEpilogueForBackend` and keeps its
own Q4_0 small-batch route unchanged.

Dispatch precedence note: when explicitly enabled, the rows==1 Q4_0 pair
generated kernel runs ahead of the default-on handwritten
`ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_TILE4_W4` path (that env defaults to true, so
it is the production default rather than an opt-in experiment). To select the
handwritten pair kernel during an opt-in A/B, set
`ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_PAIR=1`. The plain
`linearNoBias` route keeps the opposite order: the genuinely opt-in tile8 and
tile4_w4 experiment envs (default off) still take precedence over the
generated mmv kernel when explicitly enabled.

Local reproduction note: the manifest-pinned commands build benchmark modules
as fatbins containing `sm_75`, `sm_80`, `sm_89`, and `sm_90` cubins plus a
`compute_75` PTX fallback. An L4 therefore loads `sm_89` SASS even when its R580
driver cannot JIT PTX ISA emitted by CUDA 13.2. The benchmark CLI option names
still end in `-ptx` for compatibility, but their paths are CUDA module images and
accept PTX, cubin, or fatbin through `cuModuleLoadDataEx`. Use the exact
manifest command and `.fatbin` path when refreshing evidence.

The unified generated-artifact registry currently contains 21 non-promoted
CUDA entries across matmul and attention families. The first lazy Q4_K
`rows_2_8`/`bias_gelu` kernel is one of them; it loses to its handwritten
baseline (0.74x) and stays dev-only with blocker evidence, as designed. The
other entries likewise remain candidates until their route-specific correctness
and performance gates are satisfied.

## Current Metal State

The current checked-in state has 7 production-qualified generated Metal routes:
`Q2_K none`, `Q3_K none`, `Q4_K none`, `Q5_K none`, `Q6_K none`, `Q8_0 none`,
and `Q8_K none`. Qualification is distinct from rollout: all 7 keep
`runtime_default_enabled: false` and require their positive
`TERMITE_METAL_ENABLE_ANTFLY_*` gate. The production-regression gate covers 14
generated-vs-handwritten benchmark cases for those qualified routes.

`Q4_K bias`, `Q5_K bias`, `Q6_K bias`, and `Q8_0 bias` are validated shadow
routes. Their direct decode-runtime and provider APIs have correctness, route,
provider-route, and speed evidence, but normal model execution uses the
promoted no-bias quant route followed by its existing bias op. The fused APIs
therefore remain fail-closed behind `TERMITE_METAL_ENABLE_ANTFLY_*_BIAS` (or
the global candidate opt-in) until a model path actually calls them.

Several of these were re-tuned or newly qualified via the schedule-autotuning
workflow above: the sweep found that 256-value k-quant routes want a
higher-thread hybrid-simd reduction (rather than the original simd_sum /
threadgroup-tree), which the decode-runtime gate confirmed at 2–5x kernel
speedups vs handwritten. `Q4_K none`, `Q5_K none`, and `Q8_K none` were dev
candidates that only crossed the qualification bar after that re-tune. `Q4_K
bias_gelu` clears the bar too but is intentionally left as a dev candidate: it
is the `first_lazy_*` route that anchors the blocked-candidate test coverage.

All generated Metal routes stay on handwritten production dispatch by default.
The 7 production-qualified routes and 18 dev candidates remain opt-in through
`TERMITE_METAL_ENABLE_ANTFLY_*` gates, or all at once with
`ANTFLY_METAL_GENERATED_QUANT=1` / `TERMITE_METAL_ENABLE_ANTFLY_GENERATED_QUANT=1`,
and are tracked with explicit blockers. Setting
`TERMITE_METAL_DISABLE_ANTFLY_GENERATED_QUANT=1` overrides every positive
opt-in, providing one handwritten-only oracle switch.
The route-all evidence covers 50 generated cases: all 50 must be route-ready,
46 must have provider-route evidence, and every non-promoted candidate must
explain itself with an explicit blocker such as `speedup_gate_missing`,
`runtime_route_only`, or `unsupported_handwritten_baseline`. Benchmark-ready
counts for non-promoted candidates may vary with timing noise; production
promotion does not rely on route-all speedup alone.

The dedicated blocker evidence table remains intentionally stricter than
route-all: all 18 candidate kernels are guarded. Nine have benchmark-evidence paths
(`speedup_gate_missing` for Q4_0, Q5_0, Q5_1, Q6_K bias+GELU, and Q8_0
bias+GELU; `unstable_benchmark_timing` for Q4_1, Q4_K bias+GELU, Q5_K
bias+GELU, and Q8_1 none), four fused-bias shadows are blocked by
`runtime_route_only`, and five are route-evidence-only because their handwritten
baseline is unsupported. Cleared one-off evidence for
these kernels is production-regression guarded and does not promote the kernel
by itself.

This is intentional. A slow or noisy candidate is useful for compiler coverage,
route testing, and future tuning, but it must not silently become the production
route.

Current Q4_0, Q4_1, Q5_0, Q5_1, Q4_K/Q5_K/Q6_K/Q8_0 bias, Q4_K/Q5_K/Q6_K
bias+GELU, Q8_0 bias+GELU, and Q8_1 none note: these generated kernels are
correct and useful for route coverage, but they are not production defaults.
The four bias-only routes additionally require a normal model caller before
promotion; benchmark success in their direct APIs is not enough.

Current Q2_K/Q3_K bias and bias+GELU plus Q8_0 relu note: these generated
kernels prove correctness and dispatch wiring through route-all, but they do not
have comparable handwritten baselines. They cannot be promoted by the sequential
speedup gate until a non-speedup promotion policy or comparable baseline exists.

Current compiler note: every small-batch Metal family listed above is
single-sourced from the runtime region data, but promotion remains per-kernel
and evidence-gated. Source single-sourcing is source generation, not
production selection.

Current local validation snapshot:

- `zig build test -Dmetal=true -Dcuda=false -- --test-filter "quant kernel compiler"`
  covers descriptor determinism, IR generation, route summaries, manifests,
  generated source freshness, doc guardrails, and CPU/reference checks.
- `zig build quant-kernel-codegen -- --check-metal` compiles the generated and
  promoted Metal sources through `xcrun`.
- `zig build quant-kernel-metal-production-regression-check -Dmetal=true -Dcuda=false`
  runs the 7 promoted kernels across 14 generated-vs-handwritten cases.
  `src/ops/cuda/generated/quant_kernel_benchmarks.json` enumerates those 14
  Metal production-regression cases with shape, dims, tolerance, source
  fingerprint, and benchmark command metadata. The target fails on hard route
  blockers such as missing generated/provider routes, unsupported production
  baselines, or average speedup below the promotion threshold. Timing drift from
  an individual repeated run is reported as `production_regression_timing_drift`
  with slow-fallback counters, but does not hide the route/provider evidence.
- `zig build quant-kernel-metal-runtime-route-all -Dmetal=true -Dcuda=false`
  checks all generated Metal routes. The latest local run was route-ready for
  50/50 cases, provider-checked 46 cases, and reported explicit non-promoted
  blockers instead of silently promoting candidates.
- `zig build quant-kernel-metal-industry-local-check -Dmetal=true -Dcuda=false`
  adds blocker evidence refresh/strict checks and the Gemma4 generated-route
  prefill-frame smoke. The generated-route smoke first records an exact-token
  oracle with generated routes disabled, then requires both normal and
  stage-synchronized generated runs to match it. The local Gemma4 E4B path must
  also report at least two generated quant families, with `q4_0` as the top
  family and Q8_0/Q4_0 generated dispatch counters present.

Metal-named evidence targets are macOS-only by contract. They report a clear
build failure off macOS so a Linux/CUDA VM can still run CUDA/source checks
without producing false Metal runtime evidence. Use `quant-kernel-local-check`
for the cross-platform source/policy gate and `quant-kernel-metal-local-check`
when Metal runtime evidence is required.

## Evidence Commands

Metal runtime evidence uses the `antfly.quant_kernel_metal_runtime_evidence.v11`
schema. In addition to per-case timings, each evidence file records the
machine-checked compiled-vs-handwritten summary:
`benchmark_supported_count`, `benchmark_speedup_pass_count`,
`benchmark_speedup_min`, `benchmark_speedup_min_case`, `benchmark_speedup_max`,
`benchmark_speedup_max_case`, and `benchmark_speedup_avg`. The runtime evidence
checker recomputes these fields from the case list and rejects stale summaries.
Repeated timing evidence also records `repeat_gate_index`, allowing the
promotion gate to tolerate one isolated outlier while still requiring the sorted
repeat speedup gate to clear.

Version 11 provides an explicit, locally attested promotion path. Evidence still
defaults to `provenance_status: local_unattested` with the hard blocker
`missing_reproducible_provenance`. Passing `--attest-provenance` instead collects
an `attested_v1` record from the running system: clean Git commit and status
digest, host OS and architecture, the selected Metal device, Metal compiler and
Zig versions, and a UTC capture time. The promotion checker recollects and
matches the source, host, device, and toolchain fields. Missing or forged fields,
a dirty source tree, or local-only evidence fail closed. This is local machine
attestation; external signing remains outside this evidence contract.

The seven production-qualified Q2_K, Q3_K, Q4_K, Q5_K, Q6_K, Q8_0, and Q8_K
routes predate that contract. Their typed evidence records are deliberately
marked `legacy_unattested` with `missing_reproducible_provenance` and a
kernel-ID-scoped `legacy_production_exception`. The exception preserves their
qualification and regression history, not rollout: their manifests keep
`runtime_default_enabled: false`, runtime dispatch requires a positive opt-in,
and clean attested evidence is required before any default-on transition. The
exception cannot be used by a new kernel ID.

Route-all evidence is an observability check, not a promotion decision. Promoted
generated-production routes report an empty `promotion_blocker` when route
evidence and benchmark evidence are healthy. Non-promoted generated candidates
continue to report explicit blockers such as `runtime_route_only`,
`speedup_gate_missing`, or `unsupported_handwritten_baseline`.
Unsupported-handwritten-baseline candidates are route-evidence-only. They may
prove correctness and dispatch wiring through route-all. They cannot be promoted
by the sequential speedup gate until a comparable handwritten baseline or
explicit non-speedup promotion policy is added.

Production-regression evidence must match the compiler-owned
`metal_production_regression_cases` manifest exactly. Each production-regression
evidence file also records the compiler benchmark manifest schema, case count,
and case fingerprint, and the evidence checker rejects stale or partial case
sets before trusting benchmark speedups. Successful production-regression checks
also print the identity in the summary line as:
`benchmark_manifest=antfly.quant_kernel_benchmarks.v6:22:<fingerprint>`.

## Runtime Observability

Per-run stats expose `quant_kernel_plan` in Metal compact JSON, plain timing
logs, and Prometheus metrics. The important counters are:

- `planned`: quant matmul ops observed by the shared plan.
- `generated_production`: generated kernels selected as production routes.
- `generated_candidates`: generated candidates that were planned or, for
  runtime-derived Metal counters, actually dispatched while still marked as
  candidates.
- `fast_path_misses`: the sum of explicit, mutually exclusive fallback reasons.
  This includes
  generated candidates that fell back to handwritten production because the
  generated artifact is missing or not wired, plus unsupported format, shape,
  epilogue, backend, and tensor-core-repack reasons. The `unsupported` counter is
  still exposed separately as the roll-up for unsupported format/shape/epilogue/
  backend routes.
- `top_fallback_reason` / `top_fallback_count`: the largest concrete fallback
  family for the run.

Actual generated Metal dispatch counters are not treated as handwritten
fallbacks. If a dev candidate is opt-in enabled and executes, it increments
`generated_candidates` without adding a fast-path miss. If the same planned
candidate does not execute and falls back to handwritten production, its
fallback reason contributes to `fast_path_misses`.

Generate promotion evidence for one candidate:

```sh
zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- \
  --evidence-out /private/tmp/antfly-quant-metal-<kernel_id>-promotion-evidence.json \
  --repeat-runs 5 \
  --measure-iters 500 \
  --attest-provenance \
  --promotion-ready-kernel <kernel_id>
```

Check promotion evidence:

```sh
zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- \
  --check-evidence /private/tmp/antfly-quant-metal-<kernel_id>-promotion-evidence.json \
  --require-promotion-ready \
  --require-kernel <kernel_id>
```

Refresh and audit blocker evidence:

```sh
zig build quant-kernel-metal-blocker-evidence-refresh -Dmetal=true -Dcuda=false
zig build quant-kernel-metal-blocker-strict-check -Dmetal=true -Dcuda=false
```

## Adding A Format Or Epilogue

The minimal path for a matmul format/epilogue is:

1. Add the descriptor data: block values, block bytes, fields, decode ops,
   schedules, epilogues, dtype policy, and backend support.
2. Add a `FormatDecoder` (dequant leaf fragment over the shared `antfly_qk_*`
   vocabulary) and a `metal_production_schedules` entry (threads/cols/reduction)
   for the route. **There is no body constant to write — the renderer generates
   the kernel from the schedule + decoder + epilogue.**
3. Add generated source/artifact metadata for the route.
4. Add route/conformance expectations.
5. `zig build quant-kernel-codegen -- --write` to regenerate the `.metal` file,
   the `metal_kernels.m` region, and the manifests; `--check` to byte-verify.
6. Prove CPU/reference correctness and route coverage.
7. Leave the route opt-in until promotion evidence and production regression
   both pass.

To add a **microkernel op-kind** (worked example — RMSNorm is the reference
implementation):

1. Add the operation identity to `quant_kernel_op.zig` and a
   `renderMicrokernel` skeleton (self-contained MSL; no `FormatDecoder` or
   matmul epilogue) to the backend renderer.
2. Add one `GeneratedArtifact` to the authoritative
   `first_generated_artifacts` registry. Its tagged `.op.microkernel` payload
   carries the operation kind and schedule; do not add placeholder quant format,
   row-bucket, or epilogue fields.
3. Wire its standalone source renderer. The codegen tool and Metal runtime
   region both iterate the unified registry, while operation-specific harness
   views are derived at comptime.
4. Run `zig build quant-kernel-codegen -- --write`, then `--check`. Registry
   validation rejects missing sources, empty identities, and duplicate global
   kernel IDs or source paths in normal codegen builds.
5. Add a conformance case: a CPU reference and (if the buffer layout differs from
   matmul) a sister on-device FFI runner. Validate bit-identical vs the
   hand-written baseline.
6. Add an opt-in kill switch + pipeline/dispatch in `metal_kernels.m`; keep the
   hand-written kernel as the production baseline until the route is promoted.

Do not add a general compiler abstraction unless it removes real duplicated
format/schedule/op work in this file and the backend lowering. In particular, do
not generate large dense GEMM (see the non-goal in Scope).

## Extension Points

**Full single-sourcing (DONE).** The renderer is the single source of truth for
both the `metal_kernels.m` runtime region and the checked-in `.metal` files; the
frozen `metal_rt_body_*` constants and the `metal_runtime_quant_sections` table
are gone. A schedule edit + `--write` fully regenerates the kernel — no hand-paste.
See Source Of Truth above.

**Attention family.** Decode-1x paged attention and sweep-tunable flash prefill
are now generated routes. The remaining value is broader route coverage:
fixed-layout Gemma/Qwen decode variants, quantized-KV read/write, and more fused
microkernels such as head-rope and KV write/read. Each addition still needs a
`renderAttention`/`renderMicrokernel` skeleton, conformance case, dispatch
evidence, and a model-level token/coherence gate.

**Autotune automation.** `--sweep` is a directional filter followed by a manual
decode-runtime A/B before promotion. A closer-to-automated
sweep → conformance → decode-runtime-gate → promote loop (with the existing
machine-load-aware timing discipline) would make exploiting the new op-kinds
cheap.

**CUDA renderer (FIRST FAMILIES DONE).** The first six quant CUDA artifacts render
from typed backend plans. Q4_0 MMV, 8-row MM, and paired MMV share one projection
lowering; Q4_K small batch and the two fixed q8_1 FFN routes are tagged families
with validated launch contracts. A seventh, operation-typed CUDA artifact covers
the first GQA decode-attention specialization without placeholder quant route
fields. Artifact manifest v4 serializes exact CUDA launch metadata, and checked-in
`.cu` files are full-byte goldens. Remaining work is broader quant and attention
coverage, schedule variants/autotuning, and making the marker-delimited
production CUDA region codegen-owned rather than separately deployed with strict
fragment drift validation.

The Q4_0 Q8_1 pair-activation and gated-down CUDA families now use runtime
`rows`, `in_dim`, and `out_dim` launch parameters instead of Gemma E4B constants.
They are therefore valid candidates for E2B and future compatible shapes, but
shape generality is not promotion evidence: the E2B long-decode comparison
currently keeps Q8_1 FFN precompute default-off. The generated online-softmax
GQA decode route is independently gated and remains covered by exact-token and
paired llama.cpp benchmarks. Continuous batching uses the same rule: all
multi-row CUDA execution remains experimental until the long-run KV
page-boundary, response-equivalence, and throughput gates pass.
