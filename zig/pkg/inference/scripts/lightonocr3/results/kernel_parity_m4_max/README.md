# OCR Metal kernel optimization measurements

Shared Apple M4 Max, 36 GiB unified memory, macOS 27.0.1.
**End-to-end parity remains unfinished.** Other machine workloads are uncontrolled.
These are exploratory throughput measurements, not a release performance guarantee.
All primary valid rows complete the same 16 mixed PNG pages round-robin, with
256-token ceilings and greedy generation. Loading and serial reference reads
are outside timing. Three synthetic fixtures check consistency, not OCR accuracy.

## Final build and refreshed references

| Model / runtime | Pages/s | Evidence |
| --- | ---: | --- |
| LightOn 0.8B, native, concurrency/rows/window 8/8/8 | 1.5791 | [report](native_08_qkv/report.json) |
| LightOn 0.8B, vLLM Metal, concurrency 8 | 2.9864 | [report](vllm_08_final/report.json) |
| LightOn 4B, native, concurrency/rows/window 8/8/8 | 0.4911 | [report](native_4b_qkv/report.json) |
| LightOn 4B, vLLM Metal, concurrency 8 | 0.8520 | [report](vllm_4b_final/report.json) |
| Florence-2-base Q4_K, native, batch/concurrency 8 | 3.7127 | [report](native_florence_qkv_build/report.json) |
| Florence-2-base FP16, MLX-VLM, serial | 5.1237 | [report](mlx_florence_final/report.json) |

At these settings native reaches about 53%, 58% and 72% of its respective
reference. These successive runs are not controlled paired trials. Earlier
reference runs in this same session measured 2.9261/0.6106/2.9760 pages/s
(`vllm_08`, `vllm_4b`, `mlx_florence`); Florence native varied from 2.26 to 4.22.
This variation is why isolated kernel gains must not be presented as stable
whole-page gains or parity.

Native measures encoded-image API preprocessing, queueing and generation.
vLLM additionally includes loopback HTTP. Prefix and processor caches are
disabled, with unique media UUIDs to prevent encoder reuse. vLLM/vLLM Metal
are 0.32.0, MLX is 0.32.2 and MLX-VLM is 0.7.6. Installed vLLM Metal has no
Florence adapter, so Florence uses MLX-VLM. Native Florence uses the publisher
Q4_K artifact, while MLX uses original FP16. Native honors no-repeat n-gram
size 3; the MLX reference uses its package's default greedy generation without
that processor. This is not an identical-precision or generation-policy comparison.

Every successful final native row passes exact own-serial output and
markup-stripped cross-runtime content checks. Native reports keep the hashes
of the earlier reference files they actually checked; those files are retained.
Refreshed reference text is checked separately in `output_validation.json`.
The native final executable is stripped ReleaseFast, SHA256
`bd964574655903d1fca29c10660d63c6c81fc00559761a23f2e6e45d55e6d63e`.
`final_runtime_sources.json` records its file hashes and diff fingerprint
relative to commit `514dcf443bd9537445fc34480773ad0a540145fb`.

## Implemented changes

- Fused Qwen3.5 single-token convolution/history, Q/K normalization, gates,
  recurrent update and gated output RMS normalization into one kernel per page,
  replacing five stages. Concurrent pages keep independent histories.
- Persistent recurrent history and side weights use fresh storage. Previously
  a tiny state allocation could capture any larger released prefill buffer
  from the frame pool and retain it throughout decoding. A regression test seeds
  that pool, proves it is reusable, and checks persistent state cannot capture it.
- The 0.8B architecture now calls the existing qualified single-token FFN fusion.
- F32-input 8-by-32 TensorOps tiles serve 0.8B gate/up at seven/eight rows,
  4B gate/up at four through eight rows, and both QKV shapes at four through
  eight rows. Other projection shapes retain their prior dispatcher.
- Separate LightOn split-K SIMD matrix attention serves global single-query
  decode at head dimension 256, query/KV heads 8/2 or 16/4, F16 KV and contexts
  192–2048. It packs query and probability tiles to F16, with F32 accumulation
  and output. It uses frame-owned scratch and separate shaders/counters;
  exported Gemma schedule portfolios and probes remain unchanged.
- Florence channel-score reduction distributes spatial work over eight SIMD
  groups; window softmax uses parallel SIMD exponentiation and reduction.
- Qualified M4 Florence brokers collect pages for up to 5 ms before provider
  acquisition. Full cohorts run sooner. Explicit overrides are capped at 20 ms;
  zero restores the generic configured window, normally 200 µs. Sparse requests
  can wait longer. Other backends retain their existing collection policy.

Kernel defaults are qualified on M4 hardware, measured here on M4 Max.
Projection activations and recurrent state remain F32. The attention tile
precision change is explicit above; BF16 bulk projection inputs remain opt-in.
Memory admission, capacity subdivision, cancellation and provider order remain active.

## Isolated GPU measurements

Command-buffer GPU times, not whole-page requests. Comparisons alternate modes
in one process where available. Projection weights are nonuniform pseudo-random
BF16 values. Old periodic/constant-filled microbenchmarks are not reliable
checkpoint bandwidth proxies. CPU wall time can move differently under contention.

| Operation | Previous GPU µs | New GPU µs | Previous/new |
| --- | ---: | ---: | ---: |
| 0.8B recurrent core, 16 value heads | 223.17 | 84.52 | 2.64× |
| 4B recurrent core, 32 value heads | 224.53 | 88.61 | 2.53× |
| 0.8B gate/up projection, eight rows | 142.95 | 84.11 | 1.70× |
| 4B gate/up projection, eight rows | 760.26 | 278.51 | 2.73× |
| 0.8B QKV projection, eight rows | 219.53 | 88.11 | 2.49× |
| 4B QKV projection, eight rows | 676.68 | 237.06 | 2.85× |
| 0.8B attention, context 432 | 777 | 89 | 8.7× |
| 4B attention, context 432 | 776 | 92 | 8.4× |
| 0.8B attention, context 662 | 1096 | 85 | 12.8× |
| 4B attention, context 662 | 986 | 89 | 11.1× |
| Florence channel attention, 3136 positions | 1779.21 | 451.93 | 3.94× |
| Florence window attention, 64 windows | 641.78 | 576.61 | 1.11× |

Raw evidence: `metal_recurrent_timings.txt`, `metal_ffn_tile_timings.txt`,
`metal_qkv_tile_timings.txt`, `metal_ocr_gqa_timings.txt`,
`metal_channel_timings.txt`, `metal_window_timings.txt`. Do not multiply ratios
across primitives into a throughput estimate.

For the same single invoice, observed owned Metal allocation fell from
448,209,592 to 42,913,080 bytes for 0.8B, and from 1,510,606,904 to 100,941,624
for 4B. Logical tensor bytes remained 24,194,304 and 59,787,264 respectively.
These are live allocation gauges, not physical GPU residency. Total observed
4B device allocation fell roughly 9.93 to 8.52 GB; the much larger percentage
reduction in owned buffers does not describe total model memory.
`profile_08_before.txt` and `profile_08_initial_after.txt` retain initial evidence.

## Ablations and rejected experiments

| Same final executable, 16 mixed pages | Pages/s | Evidence |
| --- | ---: | --- |
| 0.8B QKV tile disabled | 1.5798 | [report](native_08_qkv_ablation/report.json) |
| 0.8B prefill SIMD attention opt-in | 1.5113 | [report](native_08_qkv_sg/report.json) |
| 0.8B BF16 prefill projection inputs opt-in | 1.5167 | [report](native_08_qkv_bf16_prefill/report.json) |
| 4B BF16 prefill projection inputs opt-in | 0.4450 | [report](native_4b_qkv_bf16_prefill/report.json) |

The QKV tile's isolated improvement did not establish an end-to-end gain in
this round. Earlier FFN tile enabled/disabled runs were similarly close,
0.7900/0.7816 (`native_08_tile*`). The existing prefill SIMD option had earlier
measured 1.5454 versus 1.3210 for 0.8B, but that improvement did not repeat on
the final build. Neither prefill option is promoted to a default.

The attention-stage executable measured 1.3210 pages/s versus 1.1830 with only
new OCR split-K attention disabled (`native_08_gqa*`), with matching text.
Its wider window measured 1.4692. The same stage successfully ran 4B at 0.3646
with rows/window 4/4 and 0.4905 with 8/8. Earlier 4B attempts were rejected at
load by live-memory admission (reservation 11,348,275,670 bytes); those reports
and `4b_load_denied*.txt` are retained as failures, not throughput. No guard
was bypassed to obtain the later successful runs.

Florence attention enabled/disabled comparisons measured 2.2649/1.8971
(`native_florence_after`, `native_florence_ablation`), about 19% apart. A later
pair was about 17% apart. Reader collection windows of 0/5/20 ms measured
2.5432/2.9282/3.2159 (`native_florence_wait*`). The 5 ms run filled all 16 native
items, while zero collected 15. Results precede the final binary and are
exploratory, not controlled causal estimates.

A GPU-candidate Florence n-gram experiment measured 2.3809 versus 2.7365 with
that experiment disabled. It introduced per-row projection/synchronization and
was removed. Reports in `native_florence_ngram*` and `native_florence_all_ablation`
refer to that discarded executable; their selection flags are no longer CLI
options. Global BF16 projection input rounding also regressed 0.8B in an earlier
run (0.7108 versus 0.8730, `native_08_bf16`).

Further eight-row tile probes found no meaningful output-head benefit for either
model, and a regression for the 0.8B down projection. A 4B down-projection
microbenchmark improved about 18%, but that candidate was not qualified or
promoted. Raw logs `rejected_*_tile.txt` and `unpromoted_down_4b_tile.txt` preserve
these probes; they are not production-path numerical qualification.

## Validation and reproduction

`final_focused_gpu_tests.txt`: **73 passed, zero skipped**, with the real 0.8B
checkpoint and two-image HTTP path on the final executable. Coverage includes
CPU/GPU recurrent continuation/reset, independent concurrent histories,
allocation retention, execution lanes, coalescing/admission, Florence policies,
and existing Gemma split-K policy regressions.

`final_metal_numerical_checks.txt`: production dispatchers, partial rows,
nonzero offsets, output canaries, rollback and missing pipelines. Recurrent
state/intermediates agree with staged execution within 4.5e-8; two-page checks
cover both real head counts with distinct histories. Projection errors are below
7.6e-6 against independent CPU samples. Florence attention CPU sample error is
below 2.1e-6. OCR attention checks both head layouts, contexts 128–2056, amplitudes
1/8, reversed physical page maps, bounds/fallbacks and independent double-precision
CPU softmax samples: maximum old/new difference below 6.14e-5, CPU error below
2.34e-5, enforced tolerance 2e-4.

`apache_boundary.txt`: 2423 source files pass. Python lint/syntax, Zig formatting,
normal commit hooks and whitespace checks pass. `checkpoints.json` records
verified weight hashes and pinned HF revisions. `final_kernel_sources.json`
identifies the standalone checker/runtime and its binary. Historical logs and
source manifests are named by stage; old logs do not describe the final binary.
Log line endings/trailing whitespace are normalized for Git; original and stored
hashes for copied follow-up logs are in `followup_log_hashes.json`.

Build a stripped ReleaseFast inference-test binary with Zig 0.17.0 and the
worktree-local `.zig-cache`. See the [previous baseline](../concurrent_readers_m4_max/README.md#reproduction)
for full native/reference commands. Use the same three fixtures and one GPU
process/model at a time. Final native settings: concurrency/rows/window 8/8/8,
LightOn wait 20 ms, decode wait 1 ms; Florence native read batch 8 and wait 5 ms.
Memory envelopes are process/shared/backend/KV/scratch 24/28/16/4/4 GiB;
Florence scratch is 8 GiB. Reports preserve exact settings, hardware,
checkpoint metadata, image hashes, binary/source fingerprints, references and logs.

Build the standalone checker as documented at its top. Run without arguments
for numerical conformance, or use `--benchmark-core`, `--benchmark-ffn-tile`,
`--benchmark-qkv-tile`, `--benchmark-ocr-gqa`, `--benchmark-florence-channel`,
`--benchmark-florence-window`. `--benchmark-small-rows` compares production and
explicit alternatives with nonuniform weights.

Rollback switches:

- `TERMITE_METAL_DISABLE_QWEN35_FUSED_DECODE=1`
- `TERMITE_METAL_DISABLE_BF16_FUSED_GATE_UP=1`
- `TERMITE_METAL_DISABLE_LIGHTONOCR3_FFN_SMALL_TILE=1`
- `TERMITE_METAL_DISABLE_LIGHTONOCR3_QKV_SMALL_TILE=1`
- `TERMITE_METAL_DISABLE_LIGHTONOCR3_GQA_SPLIT=1`
- `TERMITE_METAL_DISABLE_FLORENCE2_CHANNEL_REDUCTION=1`
- `TERMITE_METAL_DISABLE_FLORENCE2_WINDOW_SOFTMAX=1`

The runner exposes matching `--disable-*` arguments. Prefill experiments are
`--prefill-sg-attention` (`TERMITE_METAL_ENABLE_PREFILL_SG_ATTENTION=1`) and
`--bf16-prefill-inputs` (`TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS=1`).
The persistent-state allocation fix intentionally has no switch to restore
oversized retention.

## Wider cohorts and final profile

The final executable with concurrency/window 16 and physical rows 8 measured
1.4379 pages/s for 0.8B and 0.4288 for 4B, below the eight-concurrent runs.
See `native_08_qkv_window16` and `native_4b_qkv_window16`. The 4B request was
subdivided into eight-page cohorts by admission; requesting a larger window
does not bypass capacity checks. Wider windows are not promoted as a throughput win.

`native_08_profile` is an instrumented **eight-page** run, not a primary 16-page
throughput comparison. Its profiler also includes untimed cold/serial reference
reads. Across 578 frames it recorded 4769 ms GPU time: 3542 ms attributed to
named encoders and 1227 ms unattributed. Largest named costs were bulk F32
TensorOps projections (803 ms), GEMV (718 ms), four-row projections (377 ms),
fused recurrence (258 ms), prefill convolution (247 ms) and vision attention
(179 ms). The new OCR attention stage took 81 ms. This identifies remaining
projection, prefill and unlabelled GPU costs; it does not isolate timed-cohort
costs or prove scheduling accounts for the remaining end-to-end gap.

The 4B before-allocation-fix log is also retained as
`profile_4b_before_allocation_fix.txt`: its discarded prefill-pool drain experiment
did not reduce owned bytes. The later successful final 4B log contains the
100,941,624-byte owned-allocation gauge after the same invoice.

## Remaining gap

Native LightOn uses bounded cohorts with serialized vision/prefill. Completed
pages cannot admit replacements until their whole cohort retires. Continuous
refill and batched vision/prefill remain unimplemented. These are scheduling
limitations alongside remaining GPU/CPU costs; isolated kernel timings do not
attribute the entire gap to scheduling. The final numbers above do not establish
stable saturated throughput, broad-document accuracy, or all-reader parity.
