# Gemma 4 E4B QAT: L4 qualification and performance

## Current seven-case comparison

Three alternating fresh-process rounds, one warmup and two measured calls per
case give six samples per engine/case. All 126 measured calls complete their
fixed budgets with stable complete sequences; llama.cpp's backend-greedy tokens
also equal its independent full-logit oracle sequence. Native uses the four
accuracy overrides in [GEMMA4.md](GEMMA4.md), including the image resize correction.
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
