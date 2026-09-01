# GLiNER2 CUDA Inference Status

This document records the GLiNER2 CUDA encoder work qualified on 2026-07-17.
It covers the measurement contract, current dispatch policy, correctness
evidence, and remaining performance work. The measurements use performance
baseline commit `1f609755d` (`cuda: close GLiNER2 batch encoder gap`);
subsequent hardening does not change the benchmark contract.

## Current Status

The warm, end-to-end FP16 B8 path is faster than the Fastino
`torch.compile` CUDA reference on the qualified NVIDIA L4 workload. The fully
fused, shape-specialized M32 tensor-core attention route is also faster than
Fastino, but the materialized cuBLASLt schedule remains the production B4+
default because it is faster still.

| Area | Status | Production behavior |
| --- | --- | --- |
| FP16 encoder and span-head weights | Qualified | Selected embeddings, encoder matrices, and span projection matrices remain FP16 and device resident |
| FP16 dense GEMM | Qualified | Bounded, synchronized cuBLASLt plan/descriptor cache with F32 accumulation/output and a compiled CUDA correctness fallback |
| FP16 bias + ReLU epilogue | Qualified | One in-place compiled CUDA kernel; no second activation allocation/pass |
| B1 DeBERTa attention | Qualified | Fused F32 attention |
| B4+ S128..256 DeBERTa attention | Qualified on L4/SM89 | Materialized FP16 tensor-core schedule selected automatically |
| Generated M32N16 attention | Qualified explicit route | Faster than Fastino at B8, but not the default because materialized attention is faster |
| Generated M16N32 attention | Diagnostic | Available for schedule comparison; slower than M32N16 at B8 |
| Q4_K span head | Qualified | Existing resident packed-weight route remains available |
| Request preprocessing | Qualified for repeated and heterogeneous rows | Schema work is shared, duplicate rows are prepared/decoded once, and distinct rows retain independent ownership |
| Runtime NVRTC specialization for GLiNER2 | Not implemented | Current GLiNER2 winners are shipped AOT CUDA artifacts |

The generated M32/M16 attention schedules are shape-specialized tensor-core
kernels, but they currently live in the canonical compiled CUDA artifact. They
are not yet emitted through the model-neutral quant-kernel catalog and do not
require runtime NVRTC JIT compilation. This distinction matters when reporting
"generated/JIT" performance: the generated schedule is measured here; a true
runtime-specialized GLiNER2 JIT route remains future work.

## Qualified Benchmark Contract

The comparison uses a checked-in text fixture and fails if either runtime does
not observe exactly 256 encoder tokens. The count includes GLiNER2's schema and
label prefix, not just the natural-language text.

- Fixture: `scripts/gliner2/fixtures/gliner2_256.txt`
- Labels: `person`, `organization`, `location`, `date`, `money`
- Tasks: entity extraction
- Batch inputs: the same realistic text repeated B1 or B8
- Warmups: 3
- Measured requests: 10
- Percentiles: identical linear-interpolation implementation in both harnesses
- Antfly timing: full `recognizeBatch` request after model load, including
  request preparation, tensor packing, CUDA execution, downloads, and decode
- Fastino timing: full `batch_extract_entities` API call after
  `model.compile()`, with CUDA synchronization around every sample
- Excluded: model loading and one-time compilation/startup

The repeated-row B8 result is a real supported workload and both implementations
receive the same inputs. It also exercises Antfly's request-local duplicate
reuse. It must not be presented as a distinct-text B8 result; a separate
distinct-text release fixture is checked in as
`scripts/gliner2/fixtures/gliner2_256_distinct_b8.txt`. That fixture is used for route,
memory, and fallback qualification, while the performance comparison table
below remains explicitly repeated-row because Fastino has not yet been rerun
on the same heterogeneous corpus.

The Fastino harness fails closed unless all of the following are true:

- CUDA is available and the selected device is the expected device.
- Model parameters and floating-point buffers are CUDA-resident FP16.
- A forward hook observes actual CUDA input tensors.
- The observed encoder sequence length is exactly 256.
- CUDA events observe nonzero device execution.

`USE_FLASHDEBERTA` is removed from the environment, so this reference measures
Fastino's normal PyTorch/Inductor CUDA route rather than an optional external
attention extension.

## Environment

| Component | Value |
| --- | --- |
| GPU | NVIDIA L4, compute capability 8.9 |
| GPU memory | 23,034 MiB |
| Driver | 580.159.03 |
| Checked-in CUDA artifact toolchain | CUDA 13.2 |
| Fastino PyTorch | 2.7.1+cu128 |
| Fastino model dtype | FP16 |
| Fastino attention | PyTorch/Inductor, FlashDeBERTa disabled |

## Results

These are warm end-to-end request latencies using the contract above.

| Runtime/route | Batch | Average | p50 | p95 | Entities |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fastino `torch.compile` FP16 | 1 | 11.684 ms | 11.612 ms | 12.183 ms | 12 |
| Antfly production auto | 1 | 16.339 ms | 16.258 ms | 17.067 ms | 12 |
| Fastino `torch.compile` FP16 | 8 | 49.741 ms | 49.616 ms | 50.912 ms | 96 |
| Antfly production auto | 8 | 46.783 ms | **46.504 ms** | **48.489 ms** | 96 |
| Antfly generated M32N16 | 8 | 48.031 ms | **48.065 ms** | **49.116 ms** | 96 |

At B8, production Antfly is 6.3% faster at p50 and 4.8% faster at p95.
Generated M32N16 is 3.1% faster at p50 and 3.5% faster at p95. Based on average
latency, production processes about 171.0 rows/s and Fastino about 160.8
rows/s.

B1 is not at parity: Antfly is about 40% slower at p50. The B1 route did improve
from the earlier branch result of roughly 18.2 ms, but small-batch attention,
launch overhead, and the FP16 span head remain the next optimization target.

The initial B8 Antfly profile was approximately 318 ms end to end, split between
roughly 167 ms of CPU preparation and 145 ms of CUDA session execution. The
final 46-48 ms result came from improving both sides rather than hiding CPU work
outside the timer.

## Dispatch Architecture

### FP16 weight residency and dense execution

CUDA keeps only matrices with a complete FP16 consumer path in their original
representation:

- `embeddings.word_embeddings.weight`
- `encoder.layer.*.weight`
- `span_rep.span_rep_layer.*.weight`

Biases, normalization vectors, relative-position tables, and head tensors with
F32-only consumers continue through F32. This is an operation-capability policy,
not a generic rank/shape heuristic.

FP16 linears stage the F32 graph activation to FP16, use cuBLASLt tensor-core
GEMM, accumulate and write F32, and then apply the graph epilogue. QKV and pair
routes share staged activations. `linearRelu` and `linearPairRelu` use
`termite_add_bias_relu_rows_f32`, which applies bias and ReLU in place.

cuBLASLt algorithm entries and their immutable operation/matrix descriptors
are cached by layout kind, dtype, complete shape, batch count, and workspace
limit. Cache access is synchronized, dense and strided-batched layouts cannot
collide, and each session's cache is capped at 256 entries. If cuBLASLt
cannot plan or execute an otherwise supported FP16 shape, the request stays
device resident and falls back to `termite_linear_f16_weight_f32_tiled` instead
of failing. The fallback is correctness-oriented and has its own route counter;
qualified performance runs require it to remain zero.

### Attention policy

The production `auto` policy is deliberately narrow:

- B1-B3: fused F32 attention.
- B4+, sequence length 128 through 256, head dimension 64: materialized FP16
  tensor-core attention.
- Other shapes: the established compatible fallback chain.

Route defaults follow the qualified-performance target. Parity and
performance evidence is exact to L4/SM89, so the fused F32 attention kernel
and the Q4_0-to-BF16 prefill weight mirrors are default-on only when the
device reports compute capability 8.9 (mirrors additionally require a loaded
cuBLASLt). Other architectures keep the reference elementwise kernel and the
retained Q4 route. `ANTFLY_CUDA_DEBERTA_FUSED_ATTENTION=1` and
`ANTFLY_INFERENCE_CUDA_BERT_Q4_0_BF16_PREFILL=1` force the fast routes on
unqualified hardware; `=0` disables them on the target. Attached mirrors
cost about two bytes per parameter of device memory; the session logs their
count and total size at load time.

The materialized route packs Q/K/V and relative projections by head, launches
three score GEMMs, applies the DeBERTa relative-position gathers and softmax,
launches P*V, and unpacks the output. It uses more workspace and launches than
the generated route, but remains faster on the measured L4 B8 shape.

All materialized intermediates occupy one aligned arena rather than ten
persistent scratch allocations. Admission happens before allocation, defaults
to a 512 MiB ceiling, and is further capped by the unreserved portion of the
configured runtime scratch budget. `ANTFLY_INFERENCE_CUDA_DEBERTA_MATERIALIZED_WORKSPACE_MB`
may narrow the ceiling. A rejected shape falls through to bounded fused
attention and records an explicit workspace-rejection counter.

The generated M32N16 route performs FP16 staging, all three DeBERTa score terms,
online softmax, and P*V inside one CTA-local tensor-core schedule. It avoids the
four cuBLASLt attention launches and global score/probability workspaces. M32N16
halves CTA count and K/V rereads relative to M16N32 at S256.

Canonical diagnostic controls:

```sh
# Explicit generated tensor-core attention.
ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE=generated-tc

# Select the qualified schedule explicitly.
ANTFLY_INFERENCE_CUDA_DEBERTA_GENERATED_TC_VARIANT=m32

# Other supported diagnostic modes.
ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE=fused-f32
ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE=streaming-f16
ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE=materialized-f16
```

`generated` is accepted as an alias for `generated-tc`; `streaming-f16` remains
the unambiguous streaming route. Invalid mode or variant values emit one
warning and use `auto` instead of silently selecting an unrelated route.

`ANTFLY_CUDA_DEBERTA_GENERATED_TC_AUTO=1` makes generated attention precede the
normal auto policy for eligible shapes. It defaults to false; production auto
therefore retains the faster materialized B4+ route.

### Request preparation

When the selected session backend is CUDA, the GLiNER pipeline performs the
following request-local reuse:

- tokenize the label/schema prefix once per batch;
- map exact duplicate text rows to one immutable prepared input;
- cache case-insensitive word tokenization within each unique input;
- keep token ranges into one unique-token buffer instead of concatenating a
  second token buffer;
- decode the first duplicate result and deep-clone independently owned entity
  text for later duplicate rows.

There is intentionally no persistent raw-text cache. Memory ownership and cache
lifetime remain bounded to one request. Other backends retain their prior
per-row preparation behavior. The existing loaded-model execution lock still
serializes the backend `session.run` section that owns CUDA scratch; host
preparation and decode remain request-local.

## Correctness and Route Evidence

`scripts/gliner2/verify_gliner2_cuda.sh` checks native, production CUDA, and optional
generated attention at the entity level. It requires identical label, byte
span, and text identity, bounds every entity-score difference, and verifies
that generated attention executed the M32 schedule rather than silently
falling back. On an auto-detected SM89 device, it also runs the heterogeneous
B8/S256 fixture, validates all eight row lengths independently, requires
production materialized attention, and fails on generated fallback,
materialized fallback/workspace rejection, or FP16 scalar fallback. Set
`ANTFLY_GLINER2_VERIFY_MATERIALIZED_AUTO=1` to require that gate explicitly on
qualification hardware. Relative model paths are canonicalized before the
script changes working directory.

The qualification runs passed for full FP16 and Q4_K bundles:

| Model | Maximum observed aggregate score delta | Entity identity |
| --- | ---: | --- |
| FP16 CUDA/generated versus native | 0.000511 | Exact |
| Q4_K CUDA/generated versus native | 0.000306 | Exact |

The benchmark CSV also records:

- FP16 cuBLASLt linear and QKV calls;
- FP16 activation staging and fallback counts;
- compiled FP16 scalar fallback calls;
- fused, streaming, materialized, and generated attention calls/fallbacks;
- generated M32 and M16 calls separately;
- CUDA H2D and D2H bytes.

On the qualified B8 FP16 production run, all 120 measured layer-attention calls
used materialized FP16 attention. The generated run recorded 120 M32 calls,
zero M16 calls, and zero generated fallbacks. Dense execution recorded 660 FP16
linear calls and 120 QKV calls over the ten samples with zero cuBLASLt fallback.

The hardened one-sample heterogeneous release gate recorded 93.304 ms for
production materialized attention and 93.445 ms for generated M32 on the FP16
bundle. Both recorded 66 FP16 linear calls, 12 QKV calls, and zero FP16
fallbacks; production used 12 materialized layers with a 166,502,400-byte peak
arena, while generated used 12 M32 layers. These samples validate dispatch and
memory behavior, not a stable Fastino performance comparison.

A forced 1 MiB materialized-workspace ceiling rejected all 12 materialized
layers before allocation, executed all 12 through fused attention, and
completed the distinct B8 request with zero FP16 linear fallbacks. This is a
fault-path qualification result, not a throughput target.

## Reproduction

From `zig/pkg/inference`:

```sh
ZIG=../../../.tools/zig-x86_64-linux-0.17.0-dev.1936+5a625d5f3/zig
MODEL=/absolute/path/to/fp16-gliner2-gguf-directory

$ZIG build -Dcuda=true -Dcuda-artifacts=sm89 -Dcuda-libs=auto \
  -Doptimize=ReleaseFast bench-gliner2-e2e -- \
  --model-dir "$MODEL" --backend cuda --task entities \
  --text-file scripts/gliner2/fixtures/gliner2_256.txt \
  --expect-encoder-seq-len 256 --batch-size 8 \
  --label person --label organization --label location \
  --label date --label money \
  --warmup-iters 3 --measure-iters 10 --format csv
```

Generated M32N16:

```sh
ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE=generated-tc \
ANTFLY_INFERENCE_CUDA_DEBERTA_GENERATED_TC_VARIANT=m32 \
  $ZIG build -Dcuda=true -Dcuda-artifacts=sm89 -Dcuda-libs=auto \
  -Doptimize=ReleaseFast bench-gliner2-e2e -- \
  --model-dir "$MODEL" --backend cuda --task entities \
  --text-file scripts/gliner2/fixtures/gliner2_256.txt \
  --expect-encoder-seq-len 256 --batch-size 8 \
  --label person --label organization --label location \
  --label date --label money \
  --warmup-iters 3 --measure-iters 10 --format csv
```

Fastino reference, from the repository root with an environment containing the
Fastino GLiNER2 package and CUDA PyTorch:

```sh
python3 zig/pkg/inference/scripts/gliner2/benchmark_fastino_gliner2_cuda.py \
  --model fastino/gliner2-base-v1 --mode compiled \
  --text-file zig/pkg/inference/scripts/gliner2/fixtures/gliner2_256.txt \
  --expect-encoder-seq-len 256 \
  --label person --label organization --label location \
  --label date --label money --warmups 3 --repeats 10
```

The Fastino harness requires `NVIDIA L4` by default. Pass an empty
`--require-device-name` only when intentionally collecting non-L4 evidence and
record the new device in the report.

Correctness and artifact gates:

```sh
ANTFLY_GLINER2_MODEL_DIR="$MODEL" \
ANTFLY_GLINER2_VERIFY_GENERATED_TC=1 \
ANTFLY_CUDA_ARTIFACTS=sm89 \
  scripts/gliner2/verify_gliner2_cuda.sh

scripts/regen-cuda-artifacts.sh --check --all
```

## Remaining Work

Priority order:

1. Close B1 latency versus Fastino. Profile launch and span-head costs after
   the current fused-F32 attention route; avoid promoting a B8 schedule that
   regresses B1.
2. Run the canonical distinct-text B8 corpus through Fastino and collect enough
   samples to report heterogeneous-batch throughput separately from repeated
   rows.
3. Qualify B2/B4 and sequence buckets below 128 and above 256, then replace the
   current narrow auto gate only where evidence supports it.
4. Validate the attention and FP16 residency policy on SM80, SM90, and a
   driver-PTX fallback device. SM89 evidence alone is not a universal CUDA
   promotion gate.
5. Add a broader extraction-quality corpus. Entity-level parity on the current
   cases is necessary but does not replace task-level precision/recall checks.
6. Move the M32/M16 attention schedule into the model-neutral generated-kernel
   pipeline, with shape inventory, target resolution, conformance evidence,
   and explicit promotion metadata.
7. Evaluate runtime JIT specialization only where it can outperform the shipped
   AOT schedule or materially reduce the work needed to support new encoder
   shapes. Do not add JIT startup cost merely to relabel an already-fast AOT
   path.
8. Reduce model-load/cold-start time separately from warm inference. Current
   measurements deliberately exclude roughly eight seconds of Antfly model
   loading and roughly 25 seconds of Fastino first compile.

Production promotion remains evidence-driven: exact route counters, bounded
numerical differences, entity identity, repeated measurements, and an explicit
rollback/fallback path are required before widening any auto-dispatch envelope.
