# GLiNER2.5 CUDA versus native PJRT on L4

Measured 2026-11-17 on an NVIDIA L4 (SM89, 23,034 MiB), driver
580.159.03. Baseline: Antfly main `e86f0d2756`. This report records the unoptimized native baseline. The branch also adds a
qualified native CUDA optimization; its fresh comparison is recorded separately
in [OPTIMIZATION_RESULTS.md](OPTIMIZATION_RESULTS.md). Production precision is
unchanged.

## Scope

Both workers start with identical prepared host tensors and finish with
synchronized F32 head outputs. Timing includes input upload, the encoder and
requested heads, and output readback. It excludes rendering, tokenization, final
result decoding, HTTP, model loading, request setup and cleanup. The base worker
uses the production boundary engine and its reference-v1 CUDA policy. Extraction
also downloads intermediate head outputs for comparison. These are core timings,
not HTTP throughput or extraction-quality benchmarks.

Classification uses the same 14-label hotel intent/topic/urgency schema for both
models. Extraction uses the base model's five-label entity schema, including
candidate proposal and pair scoring. Each geometry has one synthetic input;
batch rows repeat that input. The measurements characterize shapes rather than
a diverse accuracy corpus.

Three fresh-process rounds alternate native/PJRT order. Each case has three
warmups and eight measured forwards per round: 24 samples per arm. Medians pool
those samples. All checkpoint parameters remain resident, including unused heads.
PJRT graphs are exported offline through torchax, then execute through Antfly's
Zig PJRT C API; Python does not execute timed inference.

PJRT FP32 uses highest JAX matmul precision. The separate BF16 experiment compares
PJRT BF16 against native FP32. Native DeBERTa's precision gate is preserved.

## FP32 performance

Median milliseconds; speedup is native time divided by PJRT time. Values below
1 mean native CUDA is faster.

| Workload | Tokens | Batch | Native FP32 ms | PJRT FP32 ms | PJRT speedup |
|---|---:|---:|---:|---:|---:|
| Decide classification | 128 | 1 | 19.54 | 22.55 | 0.87× |
| Decide classification | 128 | 8 | 101.29 | 93.17 | 1.09× |
| Decide classification | 512 | 1 | 96.57 | 63.05 | 1.53× |
| Decide classification | 512 | 8 | 642.01 | 445.46 | 1.44× |
| Base classification | 128 | 1 | 8.88 | 6.55 | 1.36× |
| Base classification | 128 | 8 | 41.11 | 30.34 | 1.36× |
| Base classification | 512 | 1 | 49.46 | 20.11 | 2.46× |
| Base classification | 512 | 8 | 358.75 | 148.15 | 2.42× |
| Base extraction | 128 | 1 | 10.02 | 7.11 | 1.41× |
| Base extraction | 512 | 1 | 50.45 | 20.88 | 2.42× |

All ten FP32 cases pass the frozen 0.002 raw-output tolerance and exact decision
or candidate-routing checks. Largest observed native/PJRT output difference:
**0.000145**. Independent CPU references cover the shortest singleton only.

## PJRT BF16 characterization

Native remains FP32. These timings include a precision change and **do not pass
full numerical qualification**.

| Workload | Tokens | Batch | Native FP32 ms | PJRT BF16 ms | PJRT speedup | Numerical gate |
|---|---:|---:|---:|---:|---:|---|
| Decide classification | 128 | 1 | 20.20 | 8.13 | 2.48× | Fail |
| Decide classification | 128 | 8 | 104.25 | 26.07 | 4.00× | Pass |
| Decide classification | 512 | 1 | 99.19 | 16.93 | 5.86× | Pass |
| Decide classification | 512 | 8 | 655.31 | 131.44 | 4.99× | Pass |
| Base classification | 128 | 1 | 9.06 | 2.90 | 3.12× | Fail |
| Base classification | 128 | 8 | 41.62 | 7.59 | 5.48× | Fail |
| Base classification | 512 | 1 | 50.99 | 5.30 | 9.61× | Fail |
| Base classification | 512 | 8 | 363.93 | 45.62 | 7.98× | Fail |

Both BF16 profiles fail the 0.02 raw-output gate. Per-case errors and decision
checks are retained in the aggregate JSON; label agreement alone does not qualify
the changed precision. All tested classification decisions agree, but the maximum
probability difference reaches 0.00294 for Decide and 0.02142 for base. Separate
BF16 CPU/PJRT preflights also fail on the short
singleton (0.09375 Decide; 0.171875 base).

## Memory

Median of three fresh-process peaks, in MiB; each row covers its whole campaign,
including model loading and cached executable deserialization. It is not memory
for one individual geometry. Cold XLA compilation was prepared separately and is
excluded here. PSS+GPU is a simultaneous peak, not a sum of independent maxima.

| Campaign | Native GPU | PJRT GPU | Native PSS+GPU | PJRT PSS+GPU |
|---|---:|---:|---:|---:|
| Decide classification (FP32) | 2432 | 4480 | 3353 | 5918 |
| Base classification (FP32) | 1234 | 2304 | 1718 | 3525 |
| Base extraction (FP32) | 1082 | 1272 | 1685 | 2810 |
| Decide classification (PJRT BF16) | 2432 | 2362 | 3274 | 3459 |
| Base classification (PJRT BF16) | 1234 | 1276 | 1711 | 2049 |

Memory includes the common Python feeder/harness for both workers. Native base
startup also initializes its tokenizer/processor. Different runtime inventories
and retained workspaces mean this does not isolate allocator efficiency.

## Validation

- Native CUDA and native PJRT ReleaseFast benchmark builds pass.
- PJRT library: 38 focused tests pass.
- Six Python protocol/numerical tests, Ruff, Zig formatting and diff whitespace checks pass.
- All FP32 campaigns pass; both BF16 campaigns intentionally retain failing numerical status.

## Reproduction and provenance

See [GLINER25.md](GLINER25.md) for build/export/run commands and
[gliner25-l4.json](gliner25-l4.json) for aggregate timings, numerical checks,
per-round memory peaks, dependency versions and artifact hashes. Raw vectors,
models, HLO, executable caches and traces remain in ignored local directories.

The base checkpoint is `fastino/gliner2.5-base-v1` at revision
`07d99c0ba5d15cb3b24e0a236fa99bdd4ef87a5c`. Decide uses the retained original
`fastino/GLiNER2.5-Decide` checkpoint; its weights match revision
`9d1bfb848cd16d93ee56ccbc59cd92bf0f284d90`, while its retained configuration
sidecars differ from the current HF serialization. Exact local sidecar hashes
are recorded; this is not a claim that Decide used a fresh current snapshot.

No Q8_0 or end-to-end HTTP campaign was run. These measurements do not establish
task accuracy, generalize to other GPUs, or qualify BF16 for production.
