# Concurrent native OCR and Apple GPU references

Exploratory measurements on a shared M4 Max with 36 GiB unified memory,
macOS 27.0.1. **Performance parity has not been achieved.** These runs are an
implementation and profiling baseline, not a release performance guarantee.
Machine workloads are uncontrolled and measurements vary substantially.

Loading and serial reference generation are outside the timed regions. All
runs use the same three synthetic PNGs, round-robin over 16 requests, greedy
decoding and a 256-token ceiling. Failed, truncated, mismatched or interrupted
runs do not count as throughput. Synthetic pages are smoke checks, not a
quality leaderboard.

## Reference targets

| Runtime and checkpoint | Concurrency / batch | Completed pages/s | Evidence |
| --- | ---: | ---: | --- |
| LightOn 0.8B BF16 / vLLM Metal, earlier refresh | 8 | 2.8011 | [vllm_metal_08_refresh](vllm_metal_08_refresh/report.json) |
| LightOn 4B BF16 / vLLM Metal | 8 | 0.8110 | [vllm_metal_4b_refresh](vllm_metal_4b_refresh/report.json) |
| Florence FP16 / MLX-VLM, earlier refresh | 1 | 4.8986 | [mlx_florence_serial](mlx_florence_serial/report.json) |
| LightOn 0.8B BF16 / vLLM Metal, adjacent refresh | 8 | 0.8459 | [vllm_metal_08_current](vllm_metal_08_current/report.json) |
| Florence FP16 / MLX-VLM, adjacent refresh | 1 | 1.5394 | [mlx_florence_current](mlx_florence_current/report.json) |
| LightOn 4B BF16 / vLLM Metal, adjacent refresh | 8 | 0.2696 | [vllm_metal_4b_current](vllm_metal_4b_current/report.json) |

The original `vllm_metal_08/` and `vllm_metal_4b/` sweeps covered concurrency
1/2/4/8 and reached 2.159/0.689 pages/s respectively. The refreshed targets above
were measured later. These are separate runs, not simultaneous paired trials.
Logs record exact package versions, commands, hashes and outputs. Server log
line endings and trailing whitespace are normalized for Git;
`log_normalization.json` records original and committed log hashes.

Prefix caching and multimodal processor caching are disabled. Each vLLM image
has a fresh UUID to avoid encoder-feature reuse. vLLM includes loopback HTTP;
native calls the encoded-image reader directly. Both include preprocessing
and generation. All valid timed outputs match their own serial reference.
Native LightOn retains F32 activations/state; the reference kernels may use
reduced precision.

Installed vLLM Metal 0.32.0 has no Florence adapter; the Florence reference is
**MLX-VLM 0.7.6**, not vLLM. Native Florence uses the Q4_K publisher artifact
(0.142 GB weights), while MLX uses the original Microsoft FP16 checkpoint
(0.463 GB). This is not a matched-precision comparison. The original FP16
checkpoint did not produce valid native OCR and is excluded from native
performance results. `mlx_florence/` also retains a rejected batch-2 experiment:
its output changed and it was slower, so that report's overall status is failed.
Only the successful serial reference is a target.

## Adjacent-run comparison

All native measurements below completed 16 pages at concurrency 8. The 0.8B
row uses the 20 ms cohort wait; 4B uses four rows/window four; Florence uses
an eight-page batch. The MLX reference is serial. Runs were consecutive on a
busy machine, not simultaneous or controlled paired trials. Precision differs
as described above; these ratios are rough local observations.

| Model | Native pages/s | Reference pages/s | Reference/native |
| --- | ---: | ---: | ---: |
| LightOn 0.8B | 0.3656 | 0.8459 (vLLM) | 2.31× |
| LightOn 4B | 0.0647 | 0.2696 (vLLM) | 4.17× |
| Florence Q4_K / FP16 | 1.1631 | 1.5394 (MLX) | 1.32× |

## Native results and qualification

| Native setting | Concurrency | Completed pages/s | Evidence |
| --- | ---: | ---: | --- |
| 0.8B, previous build, rows/window 8/8 | 8 | 0.9551 | [native_08_window8](native_08_window8/report.json) |
| 0.8B, previous build, rows/window 8/16 | 16 | 0.9877 | [native_08_window16](native_08_window16/report.json) |
| 0.8B, previous build, optional BF16 inputs | 16 | 0.9678 | [native_08_window16_bf16](native_08_window16_bf16/report.json) |
| 4B, previous build, rows/window 4/4 | 8 | 0.2780 | [native_4b_window4](native_4b_window4/report.json) |
| 4B, previous build, rows/window 8/8 | 8 | 0.2795 | [native_4b_window8](native_4b_window8/report.json) |
| Florence, earlier forced encoder mirrors | 1 | 1.6773 | [florence_mirror_policy](florence_mirror_policy/report.json) |
| Florence, earlier forced encoder mirrors | 8 | 3.2014 | [florence_mirror_policy](florence_mirror_policy/report.json) |
| 0.8B, final build, rows/window 8/8 | 8 | 0.2782 | [final_native_08](final_native_08/report.json) |
| 0.8B, final build, step pools disabled | 8 | 0.4165 | [final_native_08_no_step_pool](final_native_08_no_step_pool/report.json) |
| 0.8B, final build, 20 ms cohort wait | 8 | 0.3656 | [final_native_08_wait20](final_native_08_wait20/report.json) |
| 4B, final build, rows/window 4/4 | 8 | Failed; excluded | [final_native_4b](final_native_4b/report.json) |
| 4B, final build, completed retry | 8 | 0.0647 | [final_native_4b_retry](final_native_4b_retry/report.json) |
| Florence, final build, encoder mirrors default | 1 | 0.5425 | [final_native_florence](final_native_florence/report.json) |
| Florence, final build, encoder mirrors default | 8 | 1.1631 | [final_native_florence](final_native_florence/report.json) |

Earlier complete native runs reached about 1 page/s for 0.8B and 0.28 for 4B;
the final-build sweep slowed substantially for both 0.8B and Florence. Its 4B
first attempt was rejected at model load by live-memory admission, before any
timed requests. A retry later completed all 16 pages in 247.160 seconds
(0.0647 pages/s), with exact own-serial and cross-runtime content agreement.
Its later cohorts slowed markedly: one took about 110 seconds, including
81 seconds in measured decoder GPU frames. This sustained GPU slowdown remains
unresolved; the preceding 0.28 pages/s runs do not establish stable throughput
under pressure. No admission guard was bypassed. Adjacent reference refreshes
are retained to expose machine variation.
Ablations are single exploratory runs; different cohort fill and machine load
prevent attributing the entire timing difference to one switch.

The native harness checks every concurrent output against its own serial result.
`--reference-report` also verifies ordered image hashes and markup-stripped
content against the cross-runtime reference. Every successful row above passed
both checks and completed all 16 pages. Reports record binary/source-diff hashes
because the experiments preceded the implementation commit. The final native
binary's runtime source-diff SHA-256 is
`ac89027216ee90d61510404414460c9e0a2a2499b80975343012e7a68cc00bc1`,
which matches runtime commit `c33a76687e` relative to `066a5d0a1f`.

`focused_tests.txt` records **72/72 passing, zero skipped** on the final executable,
including the real 0.8B checkpoint, two-image HTTP smoke test, page-state isolation,
GPU recurrent continuation/reset, batch/coalescing policy and admission tests.
The final executable also completed the separate real 4B concurrency run,
including all three serial fixture references and all 16 timed pages, after the
initial admission failure described above. `metal_projection_checks.txt`
checks promoted projections for all row counts 2–8, output heads, partial rows,
missing-pipeline/rollback paths, fusion and norms against independent CPU
references (maximum absolute difference below 7.6e-6).
`metal_projection_timings.txt` contains kernel-only wall/GPU timings; requested
TensorOps modes can fall back for non-whitelisted shapes. A faster isolated
projection is not evidence of the same whole-page speedup.
`apache_boundary.txt` records the Apache product dependency check.

## What remains before parity

- Native reader batching uses bounded cohorts. Pages finishing early cannot be
  replaced until the entire cohort retires. Continuous refill is not implemented.
- Vision and prefill serialize through the shared backend. Wider cohort windows
  and optional BF16 TensorOps inputs did not establish an end-to-end improvement.
- New raw logs separate vision/runtime preparation, prefill and decode. Concurrent
  phase times include waits for other pages and must not be added across pages.
  Serial reference timings are useful for isolating phases. Frame GPU time covers
  decoder runtime frames, not every standalone command or vision operation.
- `observed_owned` and `observed_alias` account for most device allocation through
  tracked live Metal buffers, beyond the runtime-cache subtotal. Neither these
  gauges nor `currentAllocatedSize` measure physical GPU residency. Allocation
  attribution and scheduling/dispatch overhead need further profiling before
  choosing the next optimization.
- Prompt token counts match vLLM exactly (432/554/662), so a smaller input is not
  the explanation for the LightOn gap. Native completion counts exclude EOS;
  vLLM includes it. Decoded content agrees on the fixtures.

Earlier `serial/` results were 0.388/0.206 pages/s. `pre_vocabulary_fix/` and
`batched_vocabulary/` are diagnostics; their interrupted 4B concurrency-8 attempts
are not valid throughput. `pre_specialization_window16/` preserves an earlier
window experiment. The invoice-only result near 2 pages/s elsewhere in the guide
is a different short-output workload, not the mixed-page target measured here.

## Reproduction

Use a ReleaseFast stripped native inference-test executable. Run one resident
model/process at a time. For the final 0.8B eight-row configuration:

```sh
python3 zig/pkg/inference/scripts/lightonocr3/benchmark_concurrency.py \
  --native-test /absolute/path/to/test \
  --model lighton08=/absolute/path/to/LightOnOCR-3-0.8B \
  --images zig/pkg/inference/src/models/testdata/lightonocr3/invoice.png \
    zig/pkg/inference/scripts/lightonocr3/corpus/receipt.png \
    zig/pkg/inference/scripts/lightonocr3/corpus/shipping.png \
  --requests 16 --concurrency 8 --max-tokens 256 --modes batch \
  --batch-rows 8 --batch-window 8 \
  --memory-gib 24 --shared-memory-gib 28 --backend-memory-gib 16 \
  --kv-memory-gib 4 --scratch-memory-gib 4 \
  --reference-report /path/to/vllm/report.json --output-dir /tmp/native-ocr
```

The four-row 4B run uses `--batch-rows 4 --batch-window 4`. Florence uses
`--native-read-batch-size 8 --scratch-memory-gib 8`. All operator memory limits
and live-memory checks remain active. The benchmark explicitly provisions
admission work units for 16 concurrent requests; it does not bypass memory guards.
See each report for the exact executed configuration, latency and batch counts.

`benchmark_vllm_metal.py` owns only its own loopback server process group. Pass
`--python` pointing into an isolated environment containing matching official
macOS arm64 vLLM/vllm-metal 0.32.0 wheels. Do not substitute CUDA or PyTorch MPS.
`benchmark_mlx_florence.py --batch-sizes 1` selects the qualified MLX serial path.

Official support and installation references:

- https://docs.vllm.ai/projects/vllm-metal/en/stable/supported_models/
- https://docs.vllm.ai/projects/vllm-metal/en/stable/installation/
