# LightOnOCR-3 runtime

Execution details, tuning switches, and measurements for the LightOnOCR-3
readers. Usage is in the [LightOnOCR-3 guide](../../guides/models/lightonocr3.mdx).

## Model geometry

The 4B checkpoint's 4,539,265,536 BF16 parameters occupy approximately 9.1 GB of weights. It has 32 decoder layers with hidden
size 2560, 16 recurrent key heads and 32 value heads, plus a 24-layer vision
encoder with hidden size 1024. The 0.8B model has 24 decoder layers with hidden
size 1024 and a 12-layer vision encoder with hidden size 768. Both retain
128-channel recurrent heads, four-tap convolution, and partial M-RoPE.

Both Metal models defer paged-attention completion within the active
decoder frame, avoiding a GPU wait at every full-attention layer. The rollback
is `TERMITE_METAL_DISABLE_ACTIVE_PAGED_SLOT_ATTENTION_DEFERRED=1`.

On M4 Max and M5 Pro, both projection paths use SIMD reductions for single-token
projections, including the small recurrent gates, and fuse FFN gate/up
projection with SiLU. Their wide decode and attention-head RMS normalizations
also use parallel F32 reductions instead of scanning each row in one lane.
Eligible multi-token projections use Metal 4 TensorOps
on macOS 26.1 or newer. Unsupported shapes, alignment, or shader availability
retain the existing implementation. The master rollback is
`TERMITE_METAL_DISABLE_LIGHTONOCR3_PROJECTIONS=1`.
Normalization alone can be rolled back with
`TERMITE_METAL_DISABLE_LIGHTONOCR3_PARALLEL_NORM=1`.

F32 activations, accumulation, and recurrent state remain the default.
`TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS=1` experiments with BF16 inputs
only for eligible multi-token TensorOps projections. It preserves F32 outputs
and recurrent state; it does not convert the whole runtime to BF16.

The 4B model automatically sizes its GPU cache and admission envelope from the
weight artifact to cover both mapped source weights and prepared device
weights. Explicit operator memory limits still take precedence. Working memory
is additional to checkpoint size; the local comparison uses 36 GiB of unified
memory.

## Resolution, vision, and decoder execution

The shared Qwen vision path supplies learned image-position embeddings and
two-axis vision rotary positions. Qwen3.5's full-attention layers use three-axis
M-RoPE on the first 64 channels of each 256-channel head, preserving the tail;
its remaining layers use recurrent linear attention. LightOn has no DeepStack
vision taps. On Metal, the qualified 128-channel, four-tap DeltaNet path keeps
causal convolution, normalized Q/K, gates, recurrent matrices, and gated RMSNorm
on-device. Prefill distributes each value column across a 32-lane SIMD group;
single-token decoding uses a separate kernel. Buffers belong to the request,
and reset invalidates their history before reuse. The numerical test covers
short prefixes, grouped heads, continued decoding, reset, and zero core
readbacks against the CPU reference.

Metal batches decoder commands across layers and vision commands within each
block. The M4 Max and M5 Pro also enable vectorized BF16 weight loads for the
qualified 0.8B and 4B text/vision projection shapes; activations and accumulation remain F32.
Other shapes retain the existing dispatch. The CPU fallback uses vectorized
recurrence and activations. Measure release-build latency on deployment
hardware before setting an SLO; this does not qualify CUDA or vLLM throughput.

The qualified Qwen Metal image-attention path supports up to 16,384 unmerged
patches, matching the runtime cap of 4096 merged vision tokens. Qualified
LightOn single-token split attention supports contexts of 192 through 16,384
tokens, including image, prompt, and generated tokens. Longer contexts retain
the generic fallback and need separate latency measurements. The numerical
checks cover the 2048- and 4096-token boundaries, the 16,384-token limit,
batched pages with different lengths, and independent CPU reference values.
The Metal `/read` throughput profile requires
`ANTFLY_LIGHTONOCR3_THROUGHPUT=1`,
`ANTFLY_LIGHTONOCR3_DEVICE_RECURRENT_STATE=1`, and
`ANTFLY_LIGHTONOCR3_RESIDENT_VISION_BUDGET=1`. It keeps the configured row and
memory limits. LightOn's M5 Pro qualification is model-scoped; hardware defaults
for other models retain their existing device gates.

Qualified LightOn decode also enables prepared frames and bounded frame overlap.
`TERMITE_METAL_DISABLE_FAST_PREPARED_FRAME=1` restores normal frame preparation;
`TERMITE_METAL_DISABLE_PIPELINED_DECODE_FRAME=1` disables overlap and selects
the conservative reader profile.

Run the focused tests from `zig/` with the repository's Zig 0.17 toolchain:

```bash
zig build inference-test -Dtest-filter=LightOnOCR3
```

An opt-in smoke test uses a local installed checkpoint and an invoice image:

```bash
ANTFLY_TEST_LIGHTONOCR3_MODEL_DIR=/absolute/path/to/model \
  zig build inference-test -Dtest-filter=LightOnOCR3
```

For a shared-machine 4B benchmark, the opt-in test also accepts
`ANTFLY_TEST_LIGHTONOCR3_MEMORY_LIMIT_GIB=13` and
`ANTFLY_TEST_LIGHTONOCR3_SHARED_LIMIT_GIB=24`. These set independent process
and shared-pool limits using normal operator memory policy;
live availability, process usage, and the emergency reserve remain enforced.
The shared limit defaults to the process limit when omitted. These test-only
options do not change production defaults.

The 0.8B configuration fixture in `src/models/testdata/lightonocr3/` comes from the
pinned [upstream checkpoint](https://huggingface.co/lightonai/LightOnOCR-3-0.8B/tree/36d8636925327c671bd8e479c517a004f7566bd9),
licensed Apache-2.0. The 4B fixtures come from its
[pinned checkpoint](https://huggingface.co/lightonai/LightOnOCR-3-4B/tree/b71010f095dc3735de7ee6b9969bff8f8f50b39a).
The invoice fixture is copied from Antfly's existing native
Apple OCR tests. Published quality and speed numbers are documented in
[LightOn's evaluation](https://huggingface.co/blog/lightonai/lightonocr-3);
they are not Antfly runtime measurements.

## CUDA execution

CUDA supports both original BF16 checkpoints with opt-in recurrent, attention
and vision kernels. The experimental DGX Spark presets in
`scripts/lightonocr3/cuda_ocr_profiles.json` require a CUDA build and cuDNN on the
dynamic library search path. They preserve operator limits and retain F32
decode projection outputs and recurrent state. Native attention KV is F16.

```bash
python3 zig/pkg/inference/scripts/lightonocr3/cuda_ocr_profile.py lighton08 --print-env
python3 zig/pkg/inference/scripts/lightonocr3/cuda_ocr_profile.py lighton4b -- ./antfly standalone
```

The LightOn presets use compact KV, allocating storage only for full
attention layers. Each recurrent layer retains its own state. Up to 128 physical
rows use recurrent and attention packets of at most 64. Client concurrency,
admitted live pages and physical GPU rows remain separate limits.

The experimental `--decode-graph` switch replays two dependent decode tokens
inside the reader epoch. It supports 2–64 pages, or up to 128 when the
physical-128 reader mode is enabled. Eligibility and available memory determine
whether a claim replays a graph. It retains the precision of the selected profile
and independently admits graph metadata and scratch under operator limits.
Benchmark with `--require-lighton-model-graph` to require actual replay.

The DGX Spark presets enable four-token chains for pure decode cohorts through
128 pages. `ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST=0` restores single-step decoding;
widths 2–8 remain bounded by each page's original generation budget. Prepared
vision features stay on GPU through prompt assembly.
`ANTFLY_LIGHTONOCR3_DISABLE_DEVICE_VISION=1` restores the host feature handoff
without changing shared-pool admission.

Pinned image uploads retain one independently admitted 32 MiB host arena.
`ANTFLY_CUDA_ENABLE_OCR_PINNED_INPUTS=0` restores pageable uploads.
`ANTFLY_INFERENCE_CUDA_DISABLE_PINNED_SCALAR_DOWNLOADS=1` restores synchronous
token readback. Operator overrides take precedence over preset defaults.

The optional `--bf16-prefill-ffn` switch uses BF16 FFN intermediates for larger
prefills. On SM12.1, `--bf16-recurrent-prefill LIBRARY` also selects a native BF16
recurrent prefill kernel. Both retain F32 residuals, recurrent state and decode;
they do not match all of vLLM's BF16 activation boundaries.

Build the optional library on the target GPU with CUDA 13 and the pinned
FlashInfer, CUTLASS DSL and TVM FFI dependencies checked by the build script:

```bash
python3 zig/pkg/inference/scripts/lightonocr3/build_cuda_bf16_prefill.py \
  --output-dir zig/pkg/inference/zig-out/lib/lighton-prefill
python3 zig/pkg/inference/scripts/lightonocr3/check_cuda_bf16_prefill.py \
  --library zig/pkg/inference/zig-out/lib/lighton-prefill/libantfly_lighton_prefill.so \
  --output .cache/lighton-prefill-qualification.json
python3 zig/pkg/inference/scripts/lightonocr3/cuda_ocr_profile.py lighton08 \
  --bf16-prefill-ffn \
  --bf16-recurrent-prefill zig/pkg/inference/zig-out/lib/lighton-prefill/libantfly_lighton_prefill.so \
  -- ./antfly standalone
```

The SM12.1 library can also include optional paged prefill attention:

```bash
python3 zig/pkg/inference/scripts/lightonocr3/build_cuda_prefill_attention.py \
  --recurrent-library zig/pkg/inference/zig-out/lib/lighton-prefill/libantfly_lighton_prefill.so \
  --output-dir zig/pkg/inference/zig-out/lib/lighton-attention
```

Select the resulting `lighton-attention/libantfly_lighton_prefill.so` with the
same `--bf16-recurrent-prefill` option. Keep all three generated shared libraries
together. This extension uses F16 query/KV operands and F32 attention output for
eligible prefills. Its device workspace and bounded pinned metadata ring are
admitted independently. Decode and recurrent state retain F32 precision.

Python is needed for building and qualification; serving loads the native shared
library. The build manifest records the pinned source, the native page-pointer
layout patch and generated artifacts. The kernel updates each owned F32 state
directly. Its extra workspace has an independent memory reservation and does not
increase operator limits. These modes remain experimental. CUDA LightOn
throughput is below the installed vLLM reference; the Florence canonical GEMM
option remains opt-in while an older rare output mismatch is investigated.

## Measurements (as of 2026-10-27)

The table combines archived Metal/Florence qualifications with the latest
LightOn CUDA measurements. Each qualifies the named build and three synthetic
fixtures, not broad document accuracy or an SLO.

| Hardware / model | Native pages/s | Reference pages/s |
| --- | ---: | ---: |
| M4 Max, LightOn 0.8B | 4.725–4.736 | vLLM 4.097–4.102 |
| M4 Max, LightOn 4B | 1.210–1.211 | vLLM 1.1517–1.1518 |
| M4 Max, Florence | 5.306–5.309 | MLX proxy 5.131–5.138 |
| DGX Spark, LightOn 0.8B, optional prefill | 13.073–13.096 | vLLM 14.426–14.485 |
| DGX Spark, LightOn 4B, optional prefill/attention | 4.036–4.083 | vLLM 4.630–4.634 |
| DGX Spark, Florence FP16 | 25.347–25.758 | vLLM 25.340–25.348 |

The Metal comparisons are paired. Latest LightOn CUDA runs use 256 warmup
requests and two 256-request measurements, with four-token chains, GPU-resident
vision features and asynchronous token readback for both models. The CUDA
reference is a separate isolated run. LightOn CUDA parity remains outstanding.
Metal Florence uses Q4_K weights and
an F16 encoder versus an FP16 MLX proxy. Encoded-image, decoded-image and HTTP
frontends have different overheads. These are not fully precision-matched runs.

The [evidence index](https://github.com/antflydb/antfly/blob/main/zig/pkg/inference/scripts/lightonocr3/results/README.md)
contains the versioned archive, hashes, limitations and reproducible validation
entry points. Benchmark scripts, fixture definitions and independent kernel
checks remain in the source tree; chronological logs and rejected experiments
are archived separately.
