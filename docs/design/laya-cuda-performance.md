# Laya CUDA performance

The CUDA Laya path now uses warp-parallel FP32 attention, packed exact GELU,
and stable length bucketing. The target is NVIDIA L4 with the pinned English
checkpoint, strict FP32, and unchanged numerical tolerances.

## Implementation

- Attention assigns independent keys to four warps and removes block barriers
  from the key loop. It retains bounded shared scratch, FP32 softmax and value
  accumulation, symmetric local windows, padding, and fully masked outputs.
  It applies to SM 8.9, batches of at least two, head dimensions 64/128, and
  sequences through 512. Other shapes retain the original path.
- Packed exact GELU reads both halves of the FFN projection directly and emits
  their activated product in one kernel. CPU/Metal and unavailable-kernel cases
  retain the existing implementation.
- Stable 64-token buckets require at least eight tasks and at least 20% padding
  savings. Each bucket still uses shape-based admission and chunks of at most
  128 tasks. Results are scattered back to original input/question order.
- `ANTFLY_CUDA_LAYA_OPTIMIZATIONS=0` restores the original path;
  `ANTFLY_CUDA_LAYA_FUSION=0` and `ANTFLY_CUDA_LAYA_BUCKETING=0` independently
  disable fusion and bucketing. Runtime counters verify the selected routes.

## Measurement protocol

`scripts/laya_cuda_performance.py` compares the original path and optimized
defaults in the same ReleaseSafe, x86_64_v3, fatbin executable against PyTorch
2.6.0+cu124 / Transformers 4.57.6, eager FP32 with TF32 disabled.

Full pipeline measurements start with text/questions and include tokenization,
padding, uploads, forward execution, downloads, calibration and decision decoding.
Model loading, HTTP transport, and parity assertions are excluded. Prepared host
inputs through host logits are measured separately for diagnosis. The PyTorch
script also supports a separate resident-forward measurement.

The gate runs three rounds, reversing implementation order, with ten warm-ups
and 100 samples per case. In every round, both p50 and p95 must satisfy:

- Fixed and mixed batch eight: at most 1.10 times PyTorch pipeline latency.
- Fixed and mixed batch one: at most 1.05 times the original CUDA path.
- Mixed batch eight with bucketing: at most 0.95 times fusion without bucketing.

Fixed input is 61 tokens per question. Mixed input uses the same reversed first
eight fixture rows on both implementations: 748 useful tokens, 1,192 padded
slots without bucketing, and 820 slots across three buckets with bucketing.

## Validation

The three-round matched performance gate passed every check. Pipeline latency
in milliseconds (each cell is p50 / p95):

| Eight-question profile | Original CUDA | Optimized CUDA | PyTorch CUDA |
| --- | ---: | ---: | ---: |
| Fixed, 61 tokens | 84.42 / 85.70 | 58.55 / 59.55 | 57.26 / 58.91 |
| Mixed, up to 149 tokens | 262.45 / 263.77 | 115.73 / 118.93 | 144.65 / 148.06 |

Cells show the median of the three round-level percentile measurements; the gate
checks every individual round. Optimized p50 improved about **1.44x fixed** and
**2.27x mixed** against the original path. Across all rounds and both percentiles,
fixed batches stayed within 5.1% of PyTorch, and mixed batches were 18–21% faster.
Every batch-one percentile also improved against its paired original-path
measurement. Bucketing improved both mixed-batch percentiles by at least 15.8%
against fusion alone. The existing GEMM policy met the target, so the conditional
cuBLASLt tuning phase was unnecessary.

The final focused ReleaseSafe test executable passed 53 tests with zero skips,
including all 192 released decisions and reversed 512-task execution. Maximum
released probability error was 0.0000118 against a 0.00005 limit; action
probabilities also passed. Synthetic intermediate and raw-logit checks retained
the 0.0002 limit. A separate complete sweep through batch 128 also passed.

CUDA memcheck passed all five direct kernel tests with zero errors. Racecheck
on the two new kernels passed with zero hazards, errors or warnings, including
attention boundary cases through 512 tokens. Five native regression tests and
twelve Python harness tests passed. PTX, fatbin and SM89 cubin freshness checks
passed after regeneration.

The focused production server also passed all 192 HTTP examples, concurrent
requests at 1/2/4 with batches of one and eight, 512-task stress, eviction/reload,
and disconnect recovery with parity after 6.14 seconds. The 30-minute soak
completed **10,953 checked requests**. Mixed-request HTTP p50/p95 were
137.72 / 271.24 ms; these include transport and are separate from pipeline timing.

Retained host RSS rose from 431.43 to 450.24 MiB (**+18.80 MiB**), while device
usage stayed at 2,920 MiB (**zero growth**). Both passed the 256 MiB growth gate.
Whole-GPU memory sampling peaked at 4.56 GiB during cold start, reload, stress,
and soak; 200 ms sampling can miss shorter transients. The first HTTP request,
including model loading, took 6.10 seconds. All GPU processes exited after the
qualification server shut down.

The [combined machine-readable report](laya-cuda-l4-optimized-report.json) records
the exact binaries, CUDA artifact, pinned checkpoint/fixture, all three performance
rounds, correctness results, sanitizers, and server qualification. The
[earlier qualification report](laya-cuda-qualification.md) remains unchanged and
records the original implementation.

This qualifies the focused production server in ReleaseSafe on L4 with fatbin
artifacts. Full-CLI ReleaseSafe CI remains pending; the local host used the
focused server to stay within its 16 GiB memory limit. Portable PTX remains
unqualified with this host's driver, as documented in the earlier qualification.

See [qualification instructions](laya-cuda-qualification.md#matched-performance-gate)
for the reproducible benchmark command and CI gate.
