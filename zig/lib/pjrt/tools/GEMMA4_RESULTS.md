# Gemma 4 E4B QAT: L4 qualification and performance

The integrated small decode tile worker
`ae83452925cb4d67e50df926445a1050c1db5c1dc40e33fe88f979cc4bcb6b6a`
passes all eight numerical/argmax gates and all 585 official reference decisions.
All eight full first-step logit vectors match the previous lossless-projector
worker bit for bit. Both standalone SM89 and fatbin variants pass 72 bitwise
kernel checks. Per measured fixed-budget request, the worker records 288 new
FFN and 288 new PLE launch submissions, including graph capture submissions;
these counters are not a count of replayed GPU executions. The selected opt-in
[speed profile](gemma4-l4-speed-profile.json) now uses these kernels and disables the previous PLE gate fusion.
The flag-off control in the same worker also passes 8/8 and 585/585, records
zero new-tile calls, and matches the enabled profile on every first-step logit
bit and all 768 fixed-budget output tokens.
These qualification claims apply to the explicit L4 profile. Benchmarks select
it with `--native-env-file zig/lib/pjrt/tools/gemma4-l4-speed-profile.json`;
serving should apply the same JSON entries to the Gemma4 worker's environment.
The qualification does not establish the same result for bare process defaults.
The clean all-artifact freshness rerun passes: PTX, all seven SASS architectures
in the fatbin, and standalone SM89 match regeneration from source; all 325
required symbols are present.

A completed three-round comparison rotates engine order and retains six timed
calls per case and engine. All eight numerical/argmax gates pass, and every
native measured row matches its official CPU prefix (585 unique decisions,
3,510 comparisons). Fixed output budgets, prompt-token counts, per-request
optimization activity, stable greedy sequences and memory sampling all pass.
No compiler or competing GPU job ran during these measurements.

| Workload | Small-tile native ms | llama.cpp ms | BF16 PJRT ms |
|---|---:|---:|---:|
| text_short_256 | 3616.21 | 3629.76 | 11650.48 |
| page_summary_128 | 1878.57 | 1897.38 | 5968.79 |
| long2k_summary_64 | 1183.23 | 1199.94 | 3146.62 |
| image_64 | 1063.34 | 1049.95 | 3057.38 |
| audio_1s_64 | 963.74 | 946.08 | 2902.43 |
| audio_12s_64 | 1003.75 | 1004.63 | 2964.00 |
| audio_30s_64 | 1100.32 | 1120.46 | 3010.26 |
| video_6s_64 | 1090.68 | 1288.71 | 3152.93 |

Native median latency is 0.1–1.8% lower than llama.cpp on text and the two
longer audio cases. Image is 1.3% higher and 1-second audio is 1.9% higher;
video is 15.4% lower. Native p95 is lower on six cases; image and 1-second
audio retain p95 regressions of about 1.2% and 2.2%. Only video meets the
5% median improvement plus no-p95-regression clear-win criterion.
All seven non-video cases are within 1.9% of llama.cpp: this supports
approximate latency parity, not universal superiority. Native is 2.7–3.2 times
faster than the measured BF16 PJRT artifact runner, with lower p95 on every
case. This does not reproduce PR #1404's exact unified runtime.

The accuracy/performance profile trades additional GPU memory for these results.
Across the three rounds, native GPU peaks are about 15.0 GiB, versus 6.6 GiB
for llama.cpp; maximum simultaneous process-tree PSS plus GPU usage is 16.9 GiB
versus 11.1 GiB. Every requested modality is primed before native timing.
Memory superiority is not claimed.

The completed comparison is a suitable PR basis for qualified opt-in L4
improvements and approximate llama.cpp latency parity. The 5% guardrail defines
a clear-win claim, rather than a prerequisite for landing these improvements.
The broader all-modality superiority goal remains open. The raw summary and
hashes are retained in `final_performance_qualification` in the qualification
record; the earlier one-round screen remains as historical evidence.

CUDA probes using the matching official Gemma4 QAT assistant passed all eight
first-step gates and all 585 reference decisions with ordinary and deferred
materialization in Debug. The deferred path also passed in ReleaseFast, but its
native-only diagnostic timings were substantially slower than the selected
checkpoint's prior measurements. No fresh peer comparison was claimed for it.
Prepared/cached variants and three-row quantized verification changed exact
reference decisions. All experimental MTP runtime changes have been removed,
and the qualified V12 runtime source has been restored byte for byte. Their
patches and raw evidence are retained in the qualification record. The selected
speed profile remains unchanged.

The qualified worker's full 256-token short-text Nsight trace reproduces all
256 reference decisions. FFN gate/up and down account for about 53% of GPU
kernel time, the output head 16%, and attention 5%. A separate real-weight FFN
hardware-counter probe reaches about 96% of peak DRAM bandwidth with no spilling.
These are instrumented diagnostics, not comparative latency measurements.
The earlier 32-token trace uses a smaller replay capacity and a different
attention route; it does not represent the full benchmark's attention cost.

The comparison tool accepts an explicit `--baseline-env-file` for a qualified
non-speculative control. Both inference and Gemma4 environment namespaces are
recorded and isolated between arms; the control does not load the candidate's
assistant. `--require-native-speculation` rejects inactive MTP on any measured
request and records the assistant model hash. One-token logit validation remains
separate from the measured speculation checks.


## Earlier L4 speed experiments

The records below describe earlier workers and campaigns. The current qualified
profile and completed comparison are summarized above. Historical values and
source/binary hashes are retained without promoting rejected experiments.

The rebuilt worker `79468d00046d0ced3714150cd215c1301d94ed218c56fb96e85f60de9e88fae9`
passes 8/8 numerical/argmax gates and all 585 official decisions with both the
unfused PLE control and the corrected exact PLE fusion. All 262,144 first-step
logit values match bit for bit between those profiles on every case. The fusion retains the
qualified BF16 mirror path for prefill; its measured counters confirm that both
paths execute. This establishes correctness, without a demonstrated fusion
latency improvement. Production defaults remain at 5/8 numerical gates and
8/8 first-token argmax matches on both the previous and rebuilt worker.

A fresh four-arm campaign completed one of four planned rounds, with two
measured calls per case and engine. It was stopped to investigate the remaining
llama.cpp deficit. The completed round is retained as a performance screen;
the campaign's `measurement_valid` is false and it does not establish qualified
latency wins. No compiler or competing GPU job ran during these measurements.

| Workload | Candidate ms | Original qualified baseline ms | llama.cpp ms | BF16 PJRT ms |
|---|---:|---:|---:|---:|
| text_short_256 | 3678.43 | 4723.62 | 3611.98 | 11378.86 |
| page_summary_128 | 1911.85 | 2443.55 | 1883.94 | 5833.04 |
| long2k_summary_64 | 1197.78 | 1494.35 | 1197.09 | 3073.33 |
| image_64 | 1123.13 | 1813.21 | 1045.18 | 2987.07 |
| audio_1s_64 | 984.06 | 1385.61 | 943.40 | 2894.22 |
| audio_12s_64 | 1021.09 | 1491.60 | 1004.33 | 2897.76 |
| audio_30s_64 | 1124.79 | 1667.03 | 1120.04 | 2999.19 |
| video_6s_64 | 1160.34 | 1630.09 | 1275.96 | 3096.66 |

In that screen, candidate median latency is about 20–38% lower than the original
qualified baseline, and the measured PJRT artifact is about 2.6–3.2 times slower.
The candidate remains 0.1–7.5% slower than llama.cpp on seven cases; video latency
is about 9% lower. Every measured native candidate and baseline sequence matches
the 585 saved official decision prefixes. This PJRT comparison uses the artifact
runner, not PR #1404's exact unified runtime. The declared latency guardrail
still requires at least a 5% median improvement with no p95 regression in a
completed comparison.

The rebuilt lossless-projector worker
`8b7291c97ecb474dbb9af416c74f09f9a3c6b5845c4cf9db74dc17efabba9a21`
converts 232 BF16 projector matrices whose values are exactly representable in
FP16, retaining the residual precision path for the other 15 BF16 matrices.
The conversion predicate passes an exhaustive 65,536-encoding test. With
`ANTFLY_INFERENCE_CUDA_GEMMA4_PROJECTOR_LOSSLESS_F16=1`, this worker passes all
8/8 numerical/argmax gates and 585/585 official decisions. This preserves weight
values exactly; FP16 activation arithmetic still requires the model gates.
The 1-second audio cosine is 0.99990478 against the unchanged 0.9999 minimum.
Every measured media request records both FP16 and residual calls: image
112/1, each audio case 120/14, and video 672/6. Audio also records three resident
layout calls per request.

A completed one-round, two-call comparison of this worker gives the following
medians. It passes native numerical gates and all saved decision prefixes, but
one round remains a preliminary screen, not multi-round latency qualification.
The private llama.cpp compatibility runtime was restored from verified Debian
packages and the same official CUDA 12.8 runtime asset; runtime library hashes
are now recorded alongside the engine hashes.

| Workload | Candidate ms | llama.cpp ms | BF16 PJRT ms |
|---|---:|---:|---:|
| text_short_256 | 3644.67 | 3596.76 | 11796.95 |
| page_summary_128 | 1893.59 | 1869.69 | 5979.67 |
| long2k_summary_64 | 1185.20 | 1190.51 | 3174.05 |
| image_64 | 1068.59 | 1038.34 | 3088.61 |
| audio_1s_64 | 976.55 | 936.64 | 2914.55 |
| audio_12s_64 | 1014.85 | 995.79 | 2984.74 |
| audio_30s_64 | 1108.11 | 1109.46 | 3063.47 |
| video_6s_64 | 1102.30 | 1265.81 | 3149.78 |

Five cases remain 1.3–4.3% slower than llama.cpp. Long text and 30-second audio
are near parity; video latency is about 13% lower. Native is 2.7–3.2 times faster
than this PJRT artifact. The provisional profile now selects the qualified
lossless-projector settings; it does not establish an all-modality speed win.

The existing raw BF16 tensor-core route for prefill below 64 rows fails the
1-second audio numerical gate (cosine 0.99987249, relative L2 0.02033754) and
changes short-text decision 205. It is rejected. Integer tensor-core GEMV and
warp-per-output prototypes preserve the checked output bits but are slower.
These failures do not change the frozen tolerances.

Persistent BF16 prefill mirrors, F16 vision attention and the Q8 activation
head pass all eight first-step numerical/argmax gates and all 585 saved official
Transformers decisions. The other six-profile settings remain in force,
including disabled PLE gate fusion and F32 projector storage. The worst cosine
is 0.99992213 with the unchanged 0.9999 threshold. Persistent prefill mirrors
cost 7,890,534,400 additional GPU bytes; sampled process GPU usage reaches about
16.45 GB on the L4. Decode retains the quantized weights. This result covers
the retained fixtures and context budget, not general task quality or concurrency.

Three-term residual BF16 projector products also pass 8/8 and 585/585 with the
projector cache disabled. An initial cached experiment exercised that correction
only during weight creation: borrowed cache views lost the projector tag. Its
warmed timings cannot qualify the residual path. The source now preserves the
tag and records residual calls per request, with a gate rejecting zero activity
on any measured media request. The rebuilt cached path passes 8/8 and 585/585
with either two or three residual terms. Every measured image/video request
records 113/678 residual calls; audio records 134 residual and three resident
layout calls per request. Those CUDA layouts eliminate the subsampler's host
normalization and flattening round trips.

The [provisional speed profile](gemma4-l4-speed-profile.json) selects two terms,
RMS/Q8 mirror reuse and 32-token attention chunks. It passes the unchanged
numerical and decision gates. Its native-only timings remain diagnostic, and
the all-modality llama.cpp goal is still open. The combined prefill tuner and
gate/up fusion passes first-step gates but changes page-summary decision 101;
it is rejected. Raw-Q4 prefill and the online split-K dispatch diagnostic also
remain unpromoted because of substantial latency regressions; the latter never
entered its intended kernel on this model.

The head changes preserve F32 reduction order and Q6 unpacked integer values.
The differential harness passes 32 checks including real head weights,
vocabulary tails, multiple rows and suppression, plus four complete-vocabulary
Q8 logit bit comparisons. Kernel timings are diagnostic and do not establish
end-to-end performance. PLE fusion and near-threshold compact-media variants
that previously failed decision or numerical checks remain rejected; the exact
PLE correction above is a separately qualified implementation. SM89 and fatbin
head/PLE differential checks pass. Both the original and regenerated portable
PTX fail loading with CUDA error 222 on this host: its driver API is 13.0 while
the generating toolkit is 13.2. Portable PTX execution is not claimed; the L4
checks use SM89 machine code. Tolerances are unchanged.

## Opt-in eight-case correctness follow-up

The six-override accuracy profile in [GEMMA4.md](GEMMA4.md) passes all 8/8
independent Transformers first-step numerical/argmax gates and all 585 native
decisions through the first EOS or fixed budget. The video continuation matches
64/64 decisions after aligning its RGB8 conversion. The seven common
text/image/audio continuations exactly match the previously checked sequences;
video is regenerated with the unmodified CPU forward. These checks cover the
retained fixtures, not broad task-quality evaluation.

The opt-in video converter matches all six PyAV RGB frames byte for byte.
Its same-binary paired study passes accuracy and latency guardrails: first-step
relative L2 falls from 0.01367 to 0.01006; median latency changes from 1590.82
to 1590.30 ms, with median/p95 ratios 0.99967/1.00115. This is an accuracy
improvement with effectively unchanged latency. It covers eight-bit limited-range
BT.601; other color policies retain their existing paths.

Production defaults still pass 5/8 numerical gates. Fast precision with only
the RGB experiment passes 6/8; neither is the qualified accuracy profile.
The image experiment remains opt-in because its paired first-step-error metric
regresses despite passing the absolute and every-decision gates. The RGB
experiment also stays opt-in. The [RGB qualification record](gemma4-l4-rgb-qualification.json)
retains the exact worker, source snapshot, independent pixels, paired samples,
all decision checks and default-policy results.

A broad video run has 214 passes, 42 skips and one failure. The failure also
reproduces with RGB disabled and unchanged legacy PTX: bilinear upscaling
extrapolates at negative half-pixel coordinates. The saved scalar reproduction
matches the failing values exactly. That separate edge-clamp correction is
qualified in the subsequent check, rather than folded into the RGB study.

## Legacy bilinear edge correction

The CUDA fallback clamps source coordinates before calculating bilinear weights,
matching CPU image resizing at upscaled raster edges. The existing regression
previously produced 0.08180144 instead of 0.23284316 for the first upscaled
sample. It now passes with the original tolerance. The focused CUDA video suite
reports 65 passes and one optional timing test skipped; decoder parity, HDR,
rotation, RGB fixtures and PTX source integrity are included. Both RGB kernel
bodies are byte-for-byte unchanged by this fix, and all six real RGB frames
still match PyAV exactly. The large generated PTX diff is primarily virtual
register renumbering; its source change only clamps two coordinates.

Final worker checks are retained in the
[edge qualification record](gemma4-l4-edge-qualification.json). The interleaved
competitor comparison above remains the performance evidence; native-only
follow-up runs are spot checks, not a replacement comparison campaign.

## Current seven-case comparison

Three alternating fresh-process rounds, one warmup and two measured calls per
case give six samples per engine/case. All 126 measured calls complete their
fixed budgets with stable complete sequences; llama.cpp's backend-greedy tokens
also equal its independent full-logit oracle sequence. Native uses four precision
overrides and bicubic image resizing. The timed candidate enabled image bicubic
by default; the final source keeps it opt-in, with the equivalent fifth override
in [GEMMA4.md](GEMMA4.md).
PJRT executables were freshly compiled with the current reviewed worker.
llama.cpp uses official CUDA release b12521 with its asset digest verified.

| Workload | Native ms | BF16 PJRT ms | llama.cpp ms | PJRT/native |
|---|---:|---:|---:|---:|
| text_short_256 | 4710.74 | 11544.47 | 3610.69 | 2.45× |
| page_summary_128 | 2447.11 | 5873.00 | 1885.19 | 2.40× |
| long2k_summary_64 | 1494.13 | 3112.80 | 1195.43 | 2.08× |
| image_64 | 1815.36 | 3013.79 | 1042.87 | 1.66× |
| audio_1s_64 | 1389.78 | 2856.14 | 940.81 | 2.06× |
| audio_12s_64 | 1501.95 | 2923.46 | 1004.12 | 1.95× |
| audio_30s_64 | 1664.11 | 2995.80 | 1116.84 | 1.80× |

Native passes the declared latency-win guardrail against this PJRT runner on all
seven cases, and on none against llama.cpp. It is 25–74% slower than llama.cpp.
llama.cpp starts with a pre-rendered prompt and excludes output-text decoding;
native includes both. Both include encoded media preprocessing/towers and
synchronized fixed-budget token readback, excluding cold load/compile and HTTP.
These timing-boundary differences are retained in the receipt.

Independent Transformers first-step numerical/argmax gates pass 7/7 for native,
5/7 and 7/7 respectively for PJRT, and 3/7 and 6/7 for llama.cpp. The latter two
accuracy failures are retained independently of their performance results.
The native image resize correction also matches all 64 teacher-forced decisions.
Its first-step relative L2 increases from 0.01001 to 0.01450 under the accuracy
profile, remaining within the frozen absolute tolerance. Text/audio sequences
are identical to the previously checked continuations; together the seven common
cases match 521/521 decisions through EOS or budget. This does not establish
video correctness or task quality on a broad corpus.

The [follow-up record](gemma4-l4-followup.json) retains worker/library/source
hashes, timing samples, memory peaks, per-engine accuracy, generation checks and
the prior failed decisions. This measures the artifact PJRT runner, not PR
#1404's exact unified runtime. The all-modality performance goal remains unmet.

The default-image same-binary comparison runs three alternating rounds with
three measured calls per arm/case. Both first-step absolute gates pass, and the
median/p95 latency ratios are 1.00033/1.00252. However, relative L2 rises from
0.00803 to 0.01689, failing the separately declared non-increasing-error
criterion. The paired report remains failed; the successful 64-decision check
does not erase that result or establish a qualified production-default change.

A separate 32-token CUDA event diagnostic disables graph replay and compares
default with accuracy settings. LM-head time rises from 190,621 to 479,667 μs
over the diagnostic calls, while other recorded decoder categories remain
close. This identifies exact LM-head computation as a major cost to investigate.
These instrumented runs are not used as throughput measurements. All 39 Python
checks, Ruff, Zig formatting and C++ syntax/warning checks pass.

## Historical default-profile comparison

Warm isolated direct generation, encoded media read through synchronized fixed-budget tokens; no HTTP, cold model load, or compilation in latency; memory includes startup and inference. Native Q4_0/F16 KV versus dense BF16/BF16 KV from identical QAT GGUF sources.

NVIDIA L4 SM89, driver 580.159.03. Three alternating fresh-process rounds, one warmup and three measured calls per case per arm (nine samples). Native additionally primes all requested modalities. No other GPU compute process or build/CPU-oracle job ran during timing.

| Workload | Prompt / output tokens | Native ms | PJRT ms | Native speedup |
|---|---:|---:|---:|---:|
| text_short_256 | 32 / 256 | 3750.44 | 11810.23 | 3.15× |
| page_summary_128 | 419 / 128 | 1973.08 | 6027.42 | 3.05× |
| long2k_summary_64 | 1775 / 64 | 1287.19 | 3191.43 | 2.48× |
| image_64 | 284 / 64 | 1114.06 | 3079.08 | 2.76× |
| audio_1s_64 | 42 / 64 | 1012.07 | 2951.65 | 2.92× |
| audio_12s_64 | 317 / 64 | 1110.96 | 3011.91 | 2.71× |
| audio_30s_64 | 767 / 64 | 1280.63 | 3078.64 | 2.40× |
| video_6s_64 | 465 / 64 | 1147.80 | 3207.64 | 2.79× |

All 144 measured calls returned the complete token budget. Token IDs and per-round medians are retained in the ignored campaign; compact provenance and results are in `gemma4-l4-results.json`.

## Memory

Median of three per-process-tree campaign peaks, sampled every 0.2 s. Includes startup and all text/vision/audio weights requested by the suite.

| Arm | GPU GiB | PSS + GPU GiB |
|---|---:|---:|
| native | 7.65 | 9.60 |
| pjrt | 19.49 | 24.01 |

The runtime representations and weight inventories differ. Combined peaks include streamed BF16 upload buffers at startup; this is not a pure allocator-efficiency comparison.

## Numerical checks

| Arm | CPU-reference logit gate | Matching first-token argmax |
|---|---:|---:|
| native | 5/8 | 8/8 |
| pjrt | 6/8 | 8/8 |

Gate: cosine ≥0.9999 and relative L2 ≤0.02 against the FP32 CPU forward from the same dequantized source GGUF. The independent unmodified-forward regeneration below confirms the historical gate results. Native diagnostic readback bypasses device-greedy/replay fast paths. Numerical failures are retained below; the gate was not widened.

- r0-native audio_1s_64: cosine 0.9998950, relative L2 0.01474, max absolute difference 0.4001.
- r0-native audio_30s_64: cosine 0.9998628, relative L2 0.01681, max absolute difference 0.5912.
- r0-native video_6s_64: cosine 0.9998154, relative L2 0.02346, max absolute difference 1.1546.
- r0-pjrt audio_30s_64: cosine 0.9993670, relative L2 0.03706, max absolute difference 1.3242.
- r0-pjrt long2k_summary_64: cosine 0.9998185, relative L2 0.03384, max absolute difference 1.1351.

Measured first-token CPU agreement: native 72/72, PJRT 72/72. Both arms reproduce the same complete token sequence within each case across all nine repeats; this does not imply agreement between the two precisions.

## Validation and limits

- Native worker build succeeded after the PLE correction. The focused native PLE regression passed; the earlier CUDA-enabled inference type check passed 14/14 build steps.
- Four focused Python tests pass, including official-cache parity through sliding wraparound and shared KV. PJRT library checks passed before the campaigns.
- All logit vectors have 262,144 finite components. These are first-step checks, not full-generation parity.

- One retained prompt per case, repeated; synthetic media test geometry, not task quality.
- PJRT uses functional KV buffers through the native borrowed-buffer API, without donation.
- First-step diagnostic logits and argmax checks do not establish whole-generation parity.
- Native diagnostic logits use full readback with device-greedy/replay paths disabled; performance keeps defaults.
- This branch corrects PLE projection-normalization epsilon; the baseline CUDA optimizations were already present on main.

A broad default-suite attempt was not completed after its mock CUDA allocation runner failed; the focused PLE regression was built and run directly.

The native PLE normalization regression passes against the literal scaled-projection formula. A separate L4 fused-kernel check on checkpoint Q6_K rows passes six zero/nonzero-epsilon cases with maximum absolute error below 1.8e-6. Live Metal validation was not run on this Linux host.

Reproduction and artifact preparation: [GEMMA4.md](GEMMA4.md).

## PR scope and remaining qualification

The historical campaign above covers the PLE projection-normalization epsilon
correction across direct, fused, graph and prepared decoder paths. The follow-up
below also corrects CUDA video resizing. The CUDA
performance optimizations measured above were already present on the base
commit. This campaign establishes a comparison with this PJRT artifact runner;
it does not isolate a branch-versus-main performance improvement.

The earlier [L4 HTTP qualification against llama.cpp](../../../pkg/inference/scripts/gemma4/qualification/e4b-qat-l4-http-20261115.md)
is already on main. It reports 1.004–1.037× end-to-end speedups on five workloads,
with an explicit L4 profile, while TTFT and VRAM remain worse. Its measurement
boundary, inputs and native binary differ from this direct PJRT campaign.
It predates the PLE correction and does not cover the 12/30-second audio or video
cases here. These separate results cannot establish a current three-engine win
across all modalities.

The merge review reproduced the numerical failures from the retained vectors:
native passes 5/8 logit gates and PJRT 6/8, with 8/8 first-token argmax matches
for each. The comparator now saves these failures and exits nonzero. Missing
engine/round/vector coverage also fails; invalid memory samples cannot silently
produce a successful benchmark. The text-only exporter and runner from main are
preserved alongside the separate multimodal exporter and runner.

This is suitable for review as a correctness fix and experimental benchmark
harness. Full performance qualification remains open: resolve the frozen logit
gate failures, run task-quality/whole-generation checks, and collect a matched
post-correction comparison with llama.cpp before claiming broad superiority.
The historical result JSON and its source/binary hashes are unchanged.
The original campaign did not retain the native tuning environment. New
campaigns record inference overrides and the legacy Gemma backbone controls;
the old measurements cannot independently establish that every override was
unset.

## Merge review checks, 2026-11-18

- All three PJRT runners build; the library reports 38 passing checks, including
  two plugin-dependent checks that return early without an installed CPU plugin.
- CUDA inference and Gemma worker type checks pass all 19 build steps. The
  focused native CPU PLE regression passes with zero skips. Six fresh L4 fused
  PLE kernel cases match the literal FP64 reference within 1.8e-6 absolute error.
- All 23 Python tests pass, including retained text-runner compatibility,
  sliding-cache wraparound, failed-gate exit status and missing-result rejection.
  Ruff checks all 12 changed Python files; Zig formatting and whitespace pass.
- The rebuilt PJRT worker deserializes the retained, identity-checked CUDA
  executables and reproduces all eight complete PJRT token sequences. Its new
  first-step logits retain the same six passing frozen gates and two failures.

The [compact merge review record](gemma4-l4-merge-review.json) retains current
source, worker, executable and CUDA artifact hashes, numerical checks and attempt
history. Fresh compilation completed two text cases before switching to retained
executable compatibility checks. An initial compatibility startup stopped during
its largest parameter upload without a worker diagnostic; the retry succeeded
after host compilation freed memory. These are functional checks, with no new
performance measurements or cross-engine whole-generation parity claim.

## Independent qualification and PR #1404 follow-up

All eight CPU references were regenerated using the unmodified Transformers
5.23.0 forward. The reference module now checks the installed modeling source
against its wheel RECORD and verifies runtime forward origins. It shares strict
GGUF layout/name conversion with export, but imports no graph adapter and applies
no export forward patches. Folded GGUF audio scales are inverted when loading
weights so official attention applies softplus once. Video logits reproduce the
previous reference exactly; audio differences are below 6.5e-6 relative L2.
The original native gate failures therefore remain reproducible with this oracle.

[PR #1404](https://github.com/antflydb/antfly/pull/1404), reviewed at
`d723d594addf34ed5d0178b7fb0b7c83da332208`, keeps donor KV state resident and
separates F32 residual sums/scaling from BF16 branches and KV. This artifact
runner already retains donor KV buffers on the GPU. The PR's posted GB10 totals
cannot be compared directly with this L4 campaign: its benchmark defaults to
eight generated tokens with EOS stopping, while this suite forces 64–256. The
exact command and output counts for those posted rows were not available.

An opt-in `f32-residual-media` export tests F32 text residual accumulation plus
F32 media parameters, retaining BF16 text matrices and KV. This is inspired by
the PR, with an additional media precision experiment. On the two historically
failing PJRT cases:

| Case | BF16 relative L2 | Experimental relative L2 | Experimental cosine | Gate |
|---|---:|---:|---:|---|
| audio_30s_64 | 0.03706 | 0.00758 | 0.9999714 | pass |
| long2k_summary_64 | 0.03384 | 0.03443 | 0.9998003 | fail |

Both first-token argmaxes match. The experiment is not promoted or declared
qualified; the six other cases were not exported under this policy. It increases
resident parameter bytes from 15,882,208,908 to 16,838,385,676. Compilation and
validation overlapped host work, so their timings are not performance evidence.

The unmodified llama.cpp b12387 C API oracle also ran seven matched text, image
and audio cases, with fixed output budgets. It matches first-token argmax on six
of seven and the frozen numerical gate on three of seven. Its 1-second audio
projected embeddings differ from Transformers by 0.2836 relative L2 before the
decoder, despite identical prepared prompt tokens. This is an unresolved frontend
disagreement, not a diagnosed llama.cpp bug. Transformers remains the audio
source of truth. No new llama.cpp performance claim is made from these checks.

The [qualification record](gemma4-l4-qualification.json) retains independent
reference identities, vectors' hashes, precision experiments and rollback
results. All 31 Python checks pass, including oracle identity and the distinction
between improvement and absolute qualification. Watch builds panicked after
reporting success with both incremental and ordinary modes; the worker was
rebuilt with a one-shot command. The benchmark now honors `-Dstrip`.


## Matched native branch controls and accuracy profile

The completed same-binary campaign restores both the old PLE epsilon and bilinear
video in its legacy arm, then enables both corrections. Three alternating fresh
process rounds, one warmup and three measured calls per case yield nine samples
per case per arm. Both arms prime every modality; builds and CPU oracles were
stopped during timing. All 144 calls complete their fixed budgets, and all memory
samples are valid.

Every case passes the declared latency guardrail (candidate median ≤1.05× legacy,
p95 ≤1.10×). Median ratios range from 0.9948 to 1.0000 and the largest p95 ratio
is 1.0025: latency is effectively unchanged, with observed median reductions of
up to 0.52%. This is a controlled restoration of the old behaviors in one binary,
not a separately built main binary. Accuracy improves on all eight inputs.
The default candidate still passes only 5/8 frozen numerical gates, with 8/8
first-token decisions matching.

The CUDA video correction keeps pixels resident, uses the upstream bicubic
resize and preserves GGUF planar patch order. Its focused GPU regression passes
12 matrix/range/size combinations within 1e-7, with no pixel readback. On the real
video fixture, merged-embedding relative L2 improves from 0.12291 to 0.05000 and
logit relative L2 from 0.02346 to 0.01656. Its default-profile cosine remains below
the frozen gate; the resize correction alone does not close full-model qualification.

The common four-override profile documented in [GEMMA4.md](GEMMA4.md) passes all
8/8 independent Transformers first-step numerical gates and argmax checks.
It uses F32 media weights/vision attention, ordinary PLE gates and exact LM-head
multiplication, retaining Q4 text weights and F16 KV. Its three fresh rounds
also complete all 72 measured calls, with 72/72 reference first-token matches
and identical complete sequences across all nine repeats per case.

| Workload | Accuracy-profile median ms | Cost versus fresh default | Speedup versus retained BF16 PJRT |
|---|---:|---:|---:|
| audio_12s_64 | 1496.16 | 1.35× | 2.01× |
| audio_1s_64 | 1387.90 | 1.37× | 2.13× |
| audio_30s_64 | 1673.25 | 1.31× | 1.84× |
| image_64 | 1811.02 | 1.63× | 1.70× |
| long2k_summary_64 | 1497.15 | 1.17× | 2.13× |
| page_summary_128 | 2442.18 | 1.24× | 2.47× |
| text_short_256 | 4692.32 | 1.25× | 2.52× |
| video_6s_64 | 1630.95 | 1.42× | 1.97× |

The accuracy settings cost 17–63% latency depending on the workload. The
1.70–2.52× PJRT ratios compare fresh native measurements with the retained PJRT
campaign; they are not a fresh interleaved comparison or a reproduction of PR
#1404. Median native-profile campaign peaks are 7.96 GiB GPU and 10.48 GiB
process-tree PSS plus GPU, including the benchmark controller.

The independent CPU forward also checks the final native decision through the
first EOS or fixed budget, without including the predicted label in its input.
All 8/8 decisions match: text at steps 256/128/55, image at 64, audio at 4/7/7,
and video at 64. These are teacher-forced endpoint checks, not proof of every
intermediate decision or free-running generation parity. Late references carry
separate position metadata and cannot validate a first-step campaign.

Current checks: 31 Python tests pass; Ruff, Zig formatting and whitespace pass;
the final CUDA worker build succeeds in all 15 steps. The existing real-checkpoint
CUDA audio diagnostic also runs with zero skips: its component F32 projected
features match the unmodified CPU reference within 0.000301 relative L2. This
component result does not certify the default production precision policy.

The changes are reviewable as accuracy corrections and a qualification harness.
The broad performance target remains open: default numerical failures, broader
task-quality/intermediate-generation coverage, and matched current llama.cpp and
PR #1404 timing are still unresolved. No claim of a current all-modality win over
llama.cpp is established by that campaign. PR #1404's head matched the reviewed
revision at that checkpoint; the subsequent review below uses a newer head.

## Intermediate decisions and current-runtime follow-up

The expanded unmodified Transformers gate checks every native prediction through
the first EOS or fixed budget, instead of only the first and final predictions.
The previous four-override native profile matches 583/585 decisions. Text and
all three audio cases pass completely. Two intermediate failures remain:

| Case | Step | Native token | Reference token | Reference logit gap |
|---|---:|---|---|---:|
| image_64 | 17 | `rhombus` (211902) | `parallelogram` (152794) | 0.00597191 |
| video_6s_64 | 61 | `luminous` (79613) | `saturated` (31921) | 0.60105324 |

These failures were hidden by matching endpoint decisions. The gate is retained
unchanged; 8/8 first-step and endpoint matches did not establish whole-generation
correctness. A small official-model regression verifies that selecting all
prediction positions matches separate causal prefixes, including shared KV.

The raw video diagnostic compares all six selected CUDA NVDEC surfaces against
PyAV at identical timestamps. NV12 bytes match exactly. CUDA RGB8 conversion is
consistently brighter, with mean absolute differences of 1.14–1.16 out of 255
and a maximum difference of three. This localizes a real frontend difference
before resize; it does not prove that this difference alone causes the late
token mismatch. No color-policy change is promoted from this diagnostic.

The official llama.cpp b12521 CUDA release was downloaded with its published
asset SHA256 verified. Its first-step results remain 3/7 numerical gates and
6/7 matching argmaxes against the independent Transformers reference. The short
audio frontend disagreement remains present. The new timing harness uses GPU
greedy sampling and separately checks its complete sequence against the
full-logit llama.cpp oracle; oracle runs are explicitly excluded as timings.

PR #1404 was re-read at `5f75882a96d397a817e745b9268c05b33dded6ca`.
The newer commits primarily optimize the unified native backend and replay
lifecycle; they are not all PJRT changes. Its compiler's `scale_before_add`
handling does not occur in this runner's exported HLO path. The fresh artifact
runner comparison remains distinct from benchmarking that PR's unified runtime.
