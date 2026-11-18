# Gemma 4 E4B QAT: native CUDA versus native PJRT CUDA

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
llama.cpp is established by this work. PR #1404's head was rechecked and remains
the reviewed revision.
