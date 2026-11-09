# Latent-only operational QA and Python GPU latency

**Latent-only exact factual QA remains unqualified.** The released Stage3 reader
fails the exact operational-fact diagnostic in both
native CUDA and Python CUDA. Native F32 and Python F32 produce identical token
sequences on all 24 requests, including the incorrect factual answers. The
separately qualified source-verified hybrid route remains available within its
[bounded qualification scope](2026-11-08-resumed-qualification.md). These results
do not qualify latent-only factual QA for deployment.

## Matched F32 comparison

The benchmark uses NVIDIA GB10, driver 580.178.04, CUDA 13, pinned
Qwen3-Embedding-4B, Stage3, and Qwen3-8B. Python uses PyTorch 2.9.1+cu130 and
Transformers 4.53.3, eager attention, disabled TF32, original 151936-row decoder
vocabulary, F32 activations and F32 KV. Native uses the previously qualified
F32 binary `41a5db1d2d6425997cd7f601d259a55c6638ab8103a6085f827843241d045057`.
The 81-file runtime source snapshot and binary remain unchanged.

Both paths use the native system instruction, disabled thinking, greedy
decoding, the same persisted native document vectors, FP16/BF16 feature
conditioning, and a 48-token maximum. There are 24 serial requests from the
unchanged v1 diagnostic. Actual completions contain 2–6 tokens including EOS.
An initial request warms each path and is excluded from the medians. Document
ingestion, startup asset verification, and model loading are excluded. The
Python timed region actually encodes each question and runs the reader,
framing, and decoder with both base models resident together; it is not a sum
of independently timed components.

| Path | Factual correct | Missing-fact correct | Warm median | Range |
| --- | ---: | ---: | ---: | ---: |
| Native managed F32 API | 0/22 | 2/2 | 3.680 s | 3.445–4.042 s |
| Python Transformers F32 GPU | 0/22 | 2/2 | 0.819 s | 0.600–1.172 s |

All 24 output token sequences match exactly. Native is **4.49 times slower**
for this workload. This compares the managed HTTP serving path with Python
in-process inference; it includes native lifecycle and serving overhead. It
does not establish a decoder-kernel speed ratio, a vLLM throughput comparison,
or performance at long answers, large collections, or concurrent load.

Python median component times are 0.165 s for question encoding, 0.006 s for
the resident reader, 0.321 s for decoder prefill, and 0.341 s for cached decode.
Component medians do not sum to the end-to-end median. Native currently opens
and validates the reader, constructs its CUDA owner, and uploads reader weights
for each request. The existing managed composition verifier measured
2.280–2.287 s from decoder acquisition to reader-ready on its warm requests,
including bundle validation, conversion, CUDA setup and weight upload. Query
encoding took 0.119–0.124 s, and prefill plus three cached steps took
0.840–0.842 s. The numerical, cancellation, A/B/A, physical-resource and
64-document/128-token checks also passed. These intervals are host arrival
times of flushed diagnostic events, not individual CUDA kernel durations.

An earlier Python control released the encoder before loading the decoder and
added separately measured query time to reader/decoder time. Its 0.848 s median
is retained as a diagnostic; the table uses the subsequent fully resident
measurement instead.

## Released BF16 control

A second Python CUDA run uses the original helpful-assistant instruction and
BF16 encoder, normalization, reader, decoder and KV. It preserves upstream's
FP16 feature-cache intermediate and BF16 scatter-add memory injection. It also
scores **0/22 factual and 2/2 missing-fact answers**, with a 0.375 s warm median.
Its instruction, precision, features and some output tokens differ from F32, so
it is not the matched latency comparison above. The original vLLM engine was
not executed. These controls do not reproduce published benchmark scores.

The pre-existing failed mixed/BF16 encoder and F16-KV numerical gates remain
unchanged. A faster Python BF16 run does not qualify a native mixed profile.

## Operational reader adaptation

The requested release target is exact operational facts, including previously
unseen names and codes. The separate adaptation experiment preserves the
released checkpoints and does not change the serving reader allowlist.

The deterministic curriculum contains 1,536 training cases, 96 development
cases and 192 final cases across ownership, exact four-digit codes, two-hop
crate/courier bindings, dated ownership updates, missing facts, and conflicting
records. Counterfactual worlds retain the question while changing its source
fact. Entity strings and target codes are disjoint across splits, and scenario
worlds never cross splits. The curriculum SHA256 is
`c8130fa67ab43af90a012ebd9f237ee8fa764af05c6a60217697e53c29cab2cf`.

The reader starts from released Stage3 and learns in F32. Encoder features use
F32 with the native storage/conditioning recipe. The decoder is frozen and runs
BF16 SDPA during training. Only training targets enter the loss. The protocol
fixes 1,000 AdamW steps, batch two, peak learning rate 1e-4, ten-percent warmup,
cosine decay and gradient clipping at one. Development monitoring uses 12 fixed
cases and never contributes a gradient. Final evaluation requires all 96
development answers to be exact; it is not unsealed by a claimed summary score
without matching raw answers. A candidate also needs an independent F32 check
and native numerical/lifecycle qualification before serving.

The first saved candidate scored **53/96 development cases**: 7/16 ownership,
0/16 exact codes, 7/16 two-hop, 7/16 dated updates, 16/16 missing facts and 16/16
contradictions. Its SHA256 is
`bed60a5e87938d894f4907afd6ef8e3b770f166818da4962ed40a61459df51ec`.
It failed the development gate and did not execute any final case.

The first curriculum split assigned training codes to 1000–6999, withholding
entire leading digits from training. A follow-up changes only training code
values and their source facts, covering every digit in every position. Every
development and final record remains byte-for-byte unchanged. The follow-up
curriculum SHA256 is
`543b740a796c28047f83752dfdd3f83972b27492063f4371a3b3eaa90ba4e17a`.
Its continuation starts from the separate candidate and fixes 3,000 steps at
peak learning rate 3e-5. It writes atomic, fsynced reader/optimizer/RNG/order
checkpoints every 200 steps; resume requires the same trainer source and protocol.

The saved continuation scored **65/96 with the BF16 training decoder** and
**66/96 in an independent F32 decoder assessment**. Its F32 counts are 9/16
ownership, 1/16 codes, 13/16 two-hop, 11/16 updates, and 32/32 combined missing
facts/conflicts: **34/64 factual answers**. It also scored 0/22 on the original
v1 factual diagnostic. The checkpoint SHA256 is
`008bec941f9c77ec15344826e2c19fb381852fb0872010170673d525213cffc4`.
The independent F32 assessment reuses frozen encoder vectors and does not
claim full-pipeline latency. Every generation checks CUDA KV precision and the
unresized output vocabulary.

The candidate fails the gate. **No final-192 inference was performed, and no
candidate entered the native serving allowlist.** These adaptation results show
partial task learning, not qualification for exact unseen facts or a repair of
the original diagnostic. More training is not an established solution from
these experiments.

## Other released readers and feature compatibility

All five released readers were checked with the same F32 decoder, original
vocabulary, frozen v1 questions, 48-token ceiling and verified encoder vectors.
Stage1, Stage2, Stage3, PV4 and PMv2 each scored 0/22 under the unchanged scorer.
Stage1 produced one substantively correct material clause that the frozen
short-answer-format rule rejected, and ten Stage1 outputs reached the ceiling.
The other readers had no rejected gold-entity-containing factual answers and
no truncation. These are native-policy F32 diagnostics, not reproductions of
task-specific published benchmark scores.

A separate check compares fresh CPU/F32 encoder features with three original
question vectors from pinned Memorilla-Data revision
`c6b0d7eea65659254115ed31dd26869eb6dc0586`. Last-token pooling with EOS gives
cosines 0.999852–0.999898; mean pooling gives 0.598–0.645. This supports the
current pooling recipe on these examples. It does not qualify cross-engine
BF16 numerics or rule out every possible compatibility difference. Dataset
files are SHA256-verified against upstream metadata and cached under
`~/.cache/antfly/conformance-fixtures`, with worktree links to the shared bytes.

## Verified checkpoint recovery

The continuation was deliberately terminated after its first durable checkpoint.
Safe CPU loading recovered all 37 reader tensors, all 37 optimizer states at
step 200, CPU/CUDA RNG states and 1,136 queued training examples. The replacement
process restored the same checkpoint digest and continued at step 201 with its
previous optimizer and development histories intact. It completed step 3000.
This tests controlled process loss and recovery, not physical power cycling.

Complete archived source/support files reproduce the original protocol. Use
`RESUME_TRAINING.json` for a future interruption: it points to the run's archived
trainer and a fresh output directory. Current trainer revisions additionally
bind all Python/JSON support files in the protocol and archive them automatically.
Do not substitute a later working-tree trainer for a source-pinned checkpoint.

Training and its development assessments remain separate from the passing
hybrid receipts. No candidate inherits released-reader conformance evidence.

## Evidence and reproduction

The ignored evidence root is
`.benchmark-results/memorilla/latent-qualification-20261109/`:

- `native-f32-v1/result.json`: original managed API responses and timings.
- `python-f32-resident-v1/result.json`: actual warm Python pipeline timings,
  prompt IDs, output IDs and model identities.
- `python-f32-v1/result.json`: earlier sequential-residency timing control.
- `python-bf16-v1/result.json`: separate released-policy BF16 control.
- `training-v1/`: frozen protocol, copied curriculum/sources, features, optimizer
  trace, development outputs and separate candidate checkpoint.
- `native-stage-profile/`: unchanged composition verifier, timestamped events,
  passing full-logit checks and measured reader-loading intervals.
- `candidate-v1-f32-dev/`: independent F32 assessment of the saved first candidate.
- `training-v2/`: continuation with balanced training codes and durable checkpoints.
- `training-v2-resumed/`: restored state, final candidate and full development results.
- `all-readers-f32/`: all five released F32 readers and the trained candidate;
  frozen input-vector provenance, raw outputs and per-generation KV checks.
- `checkpoint-*-audit.json`: safe loading, archived replay and live continuation checks.
- `python-regression-tests-final.log`: 389 passing Python regression checks,
  excluding GPU-dependent native integration suites already qualified separately.

The GPU reference, curriculum generator and trainer live under
`zig/pkg/inference/scripts/memorilla/`. They make no implicit model downloads.
Use a separate CUDA virtual environment so the existing CPU reference remains
pinned. Run the trainer without `--run` to review its fixed protocol. The
candidate's explicit SHA256 is required for independent GPU evaluation; its
manifest cannot impersonate a released reader.

`validation.json` binds the receipts and records the completed assessment with
`latent_factual_qa_qualified: false`, `candidate_promoted: false` and production
rollout unqualified. All 81 qualified native runtime sources and the native
binary remain unchanged. The final audit confirms unchanged managed assets,
no owned GPU processes, removed session CUDA caches, and unchanged shared
fixtures. `make gc` completed with its safeguards and reclaimed 19.5 GB.
