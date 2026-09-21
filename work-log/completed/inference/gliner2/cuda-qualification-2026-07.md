# GLiNER2 CUDA Qualification (2026-07)

> Relocated verbatim from `zig/pkg/inference/docs/GLINER2_CUDA.md` (Environment/Results lines 77–113, Correctness and Route Evidence lines 223–268, and Reproduction lines 270–328, at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`GLINER2_CUDA.md`](../../../../zig/pkg/inference/models/gliner2/CUDA.md). Durable decisions from this log were folded into that document before the move.

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
ZIG=../../../.tools/zig-x86_64-linux-0.16.0/zig
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
