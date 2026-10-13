# GLiNER2.5 Multi and Decide-1B CUDA Qualification Evidence

> Relocated verbatim from `zig/pkg/inference/CUDA.md` (lines 326–735 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [CUDA.md](../../../../../zig/pkg/inference/CUDA.md#gliner25-multi-and-decide-1b) before the move.

## GLiNER2.5 Multi and Decide-1B

The inference runtime has separate CUDA profiles for the multilingual boundary
models and the Ettin/ModernBERT decision encoder. The nested encoder config
selects the architecture; a span wrapper no longer implies DeBERTa. The new
profiles do not change GLiNER training or the existing Metal dispatch policy.

| Checkpoint | Implemented CUDA path | Context policy |
|---|---|---|
| `fastino/gliner2.5-multi-v1` | Boundary extraction and classification heads | Explicit boundary windows |
| `fastino/GLiNER2.5-multi-Decide` | Boundary classification and typed decisions | Explicit classification windows |
| `fastino/GLiNER2.5-Decide-1B` | Label-marker MLP over Ettin/ModernBERT | Native 7,999 encoder tokens, including schema |

Distinct decision rows are grouped by encoded length, padded with independent
attention/marker masks, and scattered back to request/task order. Default
physical batches are bounded to 64 rows and 16,384 padded tokens. CUDA boundary
requests and classification windows also batch through the managed executor.
The Ettin attention primitive handles both sliding and full attention without
materializing a quadratic score tensor, including fully masked rows. Boundary
inference has a separate tiled FP32 attention kernel; training keeps its
versioned replay implementation.

These model families remain **pending release qualification**. No new release
capability rows or registry model pins are granted by the diagnostic tools.
Exact upstream revisions, file hashes, source hashes, FP32 oracle captures and
capacity fixtures live in `testdata/gliner25/family/manifest.json`. The Decide
capacity matrices cover 128/512/2048 tokens at B1/B8 and 128 tokens at B32/B64.
The 1B matrix also covers 4096/7999 tokens at B1; these synthetic cases are not
a natural-language holdout.

Retained L4 measurements use 30 pairs and 200 tail samples per fixture or
capacity cell. The three reports and compressed samples are under
`testdata/gliner25/family/evidence/`:

| Fixture campaign | Fastino reference | Native geometric speedup (95% interval) |
|---|---|---|
| Multi, ten extraction cases | FP32 eager | 5.44× (5.41–5.47×) |
| Multi-Decide FP16, eight capacity cells | FlashDeBERTa FP16, FP16 resident weights | 1.53× (1.52–1.54×) |
| Decide-1B FP16, ten capacity cells | SDPA FP16, FP16 resident weights | 1.123× (1.120–1.126×) |

Reports distinguish `reference_dtype` from `reference_weight_dtype`. Fastino
FP16 autocast with FP32 resident weights is a different profile from FP16
resident weights; every profile must pass quality checks before comparison.

All Multi extraction cells passed the 10% regression guard. These are fixture-level
comparisons against the named profiles, not claims against the fastest valid
profile across the full release matrix. Multi's FlashDeBERTa FP16 candidate
changed extraction output structure and failed the quality gate.
The latest full Multi-Decide capacity campaign with reduced encoder projections
and wide/compact relative attention passes five of eight cells against
FlashDeBERTa with FP16 resident weights. Its geometric speedup is 1.53×
(1.52–1.54×), but 512 tokens/B8 and 2048 tokens/B1/B8 fail the per-cell guard
at 1.22×, 1.22× and 1.49× Python latency.
Passing the aggregate speedup does not override those failures. The same
candidate passes all 18 classification fixtures against the FP32 oracle;
the earlier Multi extraction candidate passes all ten fixtures after restoring
exact exponentiation in boundary attention. Passing and rejected quality
results are summarized in `evidence/summary.json`. The final Decide-1B FP16 campaign, after
QKV/RoPE fusion and the NFC tokenizer fix, passes all ten capacity cells against
the quality-checked Fastino SDPA profile with FP16 resident weights. Its
geometric speedup is 1.123× (1.120–1.126×). At 7,999 tokens/B1, native median
latency is 1,073 ms versus Fastino's 1,142 ms: 0.940× Python latency
(95% interval 0.938–0.944×). The previously failing 2,048-token/B8 cell now
passes at 1.066× Python latency. Raw paired samples, 200 tail measurements per
cell, both worker identities and the full report are archived under
`evidence/l4_decide_1b_capacity_fp16_nfc_vs_fastino_fp16_weights/`.
Intermediate campaign histories are external run artifacts. These
loaded-model measurements do not qualify production mixed precision, serving
latency or every possible Fastino dtype profile. Multi-Decide's three capacity
regressions and the remaining release checks are follow-up work.
Use `--stop-on-regression` to stop a
diagnostic campaign after the first measured performance failure without
discarding its samples. A completed campaign also exits unsuccessfully when
the aggregate or any per-cell performance guard fails; its complete report
and raw samples remain available.

`/decisions` accepts optional `long_document` with `mode`, `window_words`,
`overlap_words`, and `max_windows`. Omission keeps over-limit rejection.
Window mode is limited to qualified boundary decision models; Decide-1B keeps
its native context policy. Preload configuration accepts `cuda_precision`:

```json
{"kind":"extractor","name":"fastino/GLiNER2.5-Decide-1B","backend":"cuda","cuda_precision":"fp32"}
```

`auto` currently resolves to FP32. Managed loads reject explicit `fp16`/`bf16`
with `UnqualifiedGlinerCudaPrecision` until release evidence is available.
Diagnostic 1B constructors may measure those candidates: only encoder matrix
weights are reduced, while embeddings, norms, classifier weights and logits
remain FP32. Mixed candidates use masked FP16 tensor-core attention with FP32
online softmax/output accumulation. The saved initial measurements used FP32
local attention; later evidence records the local tensor-core candidate.
The boundary diagnostic worker also accepts `--precision fp16`; this candidate
reduces encoder projection matrices and attention while retaining authenticated
FP32 source weight storage, embeddings, norms and task heads. Projection mirrors
are model-owned and reject non-finite or overflowing values. Use
`--attention-only 1` on the worker (or `--attention-only` on the Python harness)
to retain FP32 projections. Reports distinguish the two compute policies. The
relative-position tiles use the exact processor bucket map and retain the
original exponential calculation. For at least 512 tokens, wider direct
attention tiles reuse query/key fragments and scatter compact relative
products without an extra global workspace. At 2048 tokens and above,
adjacent equal buckets share one relative product; arbitrary bucket order and
original score arithmetic are preserved. Earlier cached-product candidates
remain documented in the evidence reports. FP32 bias addition remains separate
from FP16 matrix multiplication: a cuBLASLt bias-epilogue candidate changed two
Arabic selections and failed the holdout gate, so it was removed from dispatch.
This diagnostic option does not enable managed mixed precision serving. The
ModernBERT profile fuses QKV splitting and full-head split-half RoPE; its rebuilt
worker passes all 20 short and capacity accuracy fixtures. Packed and branched
position layouts retain their existing operations.
The 1B public holdout subsequently found a Hindi token mismatch: its BPE
tokenizer declares NFC, including canonical composition exclusions. BPE now
applies that normalization before pre-tokenization, protecting added tokens.
The tokenizer suite passes 101 tests (one skipped), and the rebuilt 1B FP16
worker matches all 1,600 documents across eight languages, including every
token ID. The current replay oracle retains the corrected token IDs.
Precision is part of both resident and in-flight model identity.

The split Q8_0 Decide-1B bundle from
[Metal PR #1033](https://github.com/antflydb/antfly/pull/1033) also has an
offline CUDA route. Use the immutable
[Hugging Face revision](https://huggingface.co/antflydb/gliner2.5-decide-1B-gguf/tree/5c61a1a39ad4c6865d0e0ff98a7ac507e8c35cef).
Both GGUF files, all sidecars, the 199-tensor inventory and exact encoder
geometry are authenticated before CUDA upload. Shadowed tensor names and
other packed formats are rejected. The 113 encoder matrices (including token
embeddings) retain Q8 storage; norms, classifier and auxiliary heads stay F32.
Q8 tensor-core projections use FP16 operands and F32 accumulation/output.
`ANTFLY_CUDA_QMATMUL_VARIANT=legacy` selects direct Q8/F32 arithmetic without
the additional tensor-core weight packs. Missing optional tensor-core symbols
retain the existing direct-Q8 compatibility path. Explicit diagnostic
`fp16`/`bf16` upload precision is rejected for packed-only source weights.

Reproduce the offline comparison without adding model files or large fixtures
to Git (run from `zig/pkg/inference`):

```sh
MODEL=/tmp/gliner-decide-q8
hf download antflydb/gliner2.5-decide-1B-gguf \
  antfly_inference_bundle.json config.json encoder_config/config.json \
  tokenizer.json tokenizer_config.json gliner2-encoder.Q8_0.gguf gliner_head.gguf \
  benchmark-cases.json validation.json \
  --revision 5c61a1a39ad4c6865d0e0ff98a7ac507e8c35cef --local-dir "$MODEL"
python scripts/gliner25/prepare_decide_quant_capture.py \
  --cases "$MODEL/benchmark-cases.json" --validation "$MODEL/validation.json" \
  --output /tmp/gliner-decide-q8-cases.json
zig build bench-gliner-decide-quant-build test-gliner-decide-quant \
  -Dcuda=true -Dmetal=false -Donnx=false -Doptimize=fast -j1
zig build test -Dtest-filter='GLiNER Decide CUDA' \
  -Dcuda=true -Dmetal=false -Donnx=false -Doptimize=fast -j1
ANTFLY_CUDA_DISPATCH_STATS=1 \
  zig-out/bin/antfly-inference-gliner-decide-quant-bench \
  --model-dir "$MODEL" --capture /tmp/gliner-decide-q8-cases.json \
  --backend cuda --warmups 3 --reps 10 --tolerance 0.002
```

The capture builder preserves label order and binds published input IDs to
the independently decoded Q8 oracle, rather than the original FP32 checkpoint.
The benchmark verifies exact token IDs, finite logits and absolute errors on
every run. It reports both the full loaded pipeline and the prepared
encoder/classifier/completed-readback clock; HTTP and model load are separate.

`ANTFLY_CUDA_GLINER_1B_Q8_F16_MIRRORS=1` enables an opt-in cached FP16
projection candidate analogous to the Metal optimization. Only the 112 typed
encoder projections receive mirrors. Raw Q8 tensors remain resident; token
embedding lookup and F32 normalization, attention and task heads retain their
existing routes for short inputs. At 512 through 7,999 encoder tokens, complete
mirror profiles use tiled FP16 tensor-core attention with F32 online softmax
and output accumulation. Global layers use wider query tiles from 2,048 tokens;
local layers preserve their exact window and key masks. Missing optional
attention symbols retain F32 attention. Set
`ANTFLY_CUDA_GLINER_1B_Q8_F16_ATTENTION=0` to compare the previous attention
route while retaining the projection mirrors. This switch has no effect on
dense-source precision profiles or the public serving qualification gate.
Host decoding uses a 16 KiB F32 tile and at most 26.25 MiB of
FP16 staging per projection, checked for malformed blocks and finite FP16
range. The mirror replaces the Q8 tensor-core pack rather than adding a third
weight representation. Model teardown releases all mirrors. Disabling the
candidate, or lacking its complete FP16 kernel/library capabilities, retains
Q8 execution. Device residency includes all execution packs and mirrors.

On an NVIDIA L4 with driver 580.159.03, a fresh Q8 comparison uses 30 paired
rounds and 200 additional tail rounds per case, after three warmups and one
validation preflight. The previous mirror path and the new path use the same
optimized binary; only the attention switch differs. All three models remain
resident and execute serially in balanced permutations. Prepared-core medians
across all 230 timed rounds are:

| Tokens, batch 1 | Previous mirrors | Tiled-attention mirrors | PyTorch FP16 | CUDA/Python latency |
|---|---:|---:|---:|---:|
| 25 | 9.81 ms | 9.81 ms | 23.47 ms | 0.418× |
| 83 | 13.24 ms | 13.10 ms | 25.92 ms | 0.505× |
| 198 | 24.51 ms | 24.18 ms | 26.49 ms | 0.913× |
| 512 | 77.38 ms | 32.08 ms | 32.02 ms | 1.002× |
| 2,048 | 688.78 ms | 145.48 ms | 137.58 ms | 1.057× |

The paired native speedups are **2.409×** at 512 tokens (95% bootstrap CI
2.369–2.445×) and **4.745×** at 2,048 tokens (4.658–4.772×). The 512-token
cell reaches Python parity; the 2,048-token median remains **5.7% slower**.
All five cells pass the 10% latency regression guard against Python. At 2,048
tokens, native/Python p95 is 149.33/141.07 ms. Performance is scoped to these
batch-1 cells and this hardware; it does not establish serving qualification.

Both clocks include prepared CPU input upload and completed CPU logit readback.
Python uses Torch 2.14.0+cu130/Transformers 5.17.0, SDPA, disabled TF32 and an
FP16 encoder with an F32 classifier. Native retains F32 embedding outputs,
norms, classifier and attention accumulation, with FP16 attention operands at
long contexts. These arithmetic profiles differ. Both native mirror routes use
3.61 GB of resident weights, versus 2.73 GB for Q8 plus tensor-core packs and
1.74 GB for direct Q8 in earlier diagnostic measurements. Native retains the
complete bundle; Python loads only the encoder and classifier. No additional
projection mirrors or global attention score allocation are introduced.

The previous unpaired 70.58/664.25 ms native measurements motivated this change;
the paired table uses fresh baselines rather than mixing campaigns. Source,
binary and model pins, raw samples and validation receipts are saved under
`/tmp/antfly-gliner25-q8-long-context/`, including `paired/report.json` and
`validation-receipt.json`. Runtime counters confirm 13,104 FP16 attention
launches (2,340 wide global tiles), with F32 attention retained in short cases.

CUDA validation is independent of public serving qualification. The exact
Q8 bundle does not acquire CUDA `/decisions` qualification from the Metal PR
or this offline benchmark. HTTP admission, cancellation/concurrency, broad
holdouts and the complete capacity/performance matrix remain release checks.

The long-context route passes all 14 published cases and both synthetic
512/2,048-token capacity cases at the existing 0.002 absolute logit tolerance
(maximum error 0.001394). Seven direct attention checks cover key masks,
empty documents, batched/odd lengths, local windows and wide global tiles.
Eight additional multilingual/compound-padding holdouts through 7,999 tokens
preserve selected labels, but only five pass the same 0.002 logit bound in
either mirror route. Maximum absolute error is 0.004900 in both profiles;
the largest change between them is 0.007043 on a 512-token hard case. That
case also exceeds the Python FP16 0.01 error bound. These diagnostics do not
qualify FP16 numerical behavior on broader inputs or public CUDA serving.

For fresh paired Q8 measurements, provide a capture containing independent
decoded-Q8 reference logits and use the persistent worker controller:

```sh
python scripts/gliner25/benchmark_decide_quant.py \
  --model-dir "$MODEL" --capture /tmp/gliner-decide-q8-cases.json \
  --native zig-out/bin/antfly-inference-gliner-decide-quant-bench \
  --pairs 30 --tails 200 --warmups 3 --output /tmp/gliner-decide-q8-paired
```

The controller keeps the previous F32-attention mirror route, the new attention
route, and the decoded-Q8 PyTorch FP16 SDPA model resident. It executes them
serially in balanced permutations, checks logits and selected labels on every
invocation, and saves paired confidence intervals, latency distributions,
compressed raw samples, kernel counters, artifact hashes and runtime versions.
Python requires PyTorch, Transformers and NumPy. Captures and model artifacts
stay outside Git, and the output directory must be new.

Build optimized workers from `zig/pkg/inference`:

```sh
zig build bench-gliner25-decide-build bench-gliner25-cuda-build \
  -Dcuda=true -Dmetal=false -Doptimize=fast
ANTFLY_LAYA_BACKEND=cuda zig build test -Dcuda=true -Dmetal=false \
  -Dtest-filter='GLiNER CUDA long attention'
python -m unittest discover -s scripts/gliner25 -p test_family_contract.py
```

Use the pinned Fastino environment specified by the manifest, with `psutil`
installed for process monitoring. Download the exact model revisions/files and
provide explicit classification-head metadata when testing the raw 1B artifact
(`model_manifest.json` with `type: extractor`, `tasks: [extract, decide]`,
`capabilities: [classification, typed_decisions]`, `inputs: [text]`, and
`gliner_classification_head: label_marker_mlp`). Raw Multi-Decide directories
used by the typed-decision HTTP test need the same task/capability metadata,
with the `gliner_classification_head` field omitted. These local declarations
select routing; they do not grant boundary release qualification. The oracle
verifies downloaded files and imported upstream source before running:

```sh
python scripts/gliner25/check_family_decisions.py \
  --native zig-out/bin/antfly-inference-gliner25-decide-bench \
  --model-dir /models/decide-1b \
  --cases testdata/gliner25/family/decide_1b/capacity_cases.json \
  --oracle testdata/gliner25/family/decide_1b/capacity_oracle_fp32.jsonl \
  --output /tmp/decide-1b-parity.json
python scripts/gliner25/benchmark_family.py \
  --native zig-out/bin/antfly-inference-gliner25-decide-bench \
  --python /path/to/reference/bin/python --model-dir /models/decide-1b \
  --model decide_1b --attention sdpa --output /tmp/decide-1b-paired
python scripts/gliner25/family_oracle.py \
  --model multi_decide --model-dir /models/multi-decide --dtype fp32 \
  --cases testdata/gliner25/family/multi_decide/capacity_requests.json \
  > /tmp/multi-decide-capacity-oracle.jsonl
```

For Multi-Decide select `--model multi_decide` and
`zig-out/bin/antfly-inference-gliner25-cuda-bench` as the native worker.
Select `--model multi` to check the full boundary fixture suite, including
entity coordinates, attributes, relations, records and classification.
For Multi-Decide capacity campaigns, pass `--cases`, `--native-cases`, and
`--oracle` using its `capacity_requests.json`, `capacity_cases.json`, and
`capacity_oracle_fp32.jsonl` fixtures respectively.
Run separate campaigns for every quality-valid Fastino attention/dtype profile
and compare against the fastest one. Use `--reference-weight-dtype fp16` with
`--reference-dtype fp16` to measure reduced resident weights; the default keeps
FP32 resident weights. The harness checks all token IDs, selected
labels and confidences before alternating paired measurements; reports include
raw samples, tail distributions and bootstrap intervals. It requires at least
30 pairs and 200 tail samples per case and always emits `qualification: false`.
Serving concurrency, long-window performance, cancellation/OOM recovery and the
99.5% holdout gate must also pass before publishing release capabilities.

The public classification parity suite uses 200 deterministically selected
[MASSIVE test examples](https://huggingface.co/datasets/AmazonScience/massive)
per language: English, French, German, Spanish, Arabic, Hindi, Japanese and
Chinese. Dataset revision, file hashes, attribution and selection instructions
are under `testdata/gliner25/family/holdout/`. It measures agreement with Fastino
FP32, not gold accuracy or independence from checkpoint training data. Each
language must independently reach 99.5%; token drift or host fallback fails the
campaign. `--task entities --model multi` runs an additional entity-output and
coordinate comparison on the same texts. `--task structured --model multi`
uses the pinned `holdout/structured_schema.json` to compare entities,
attributes, relations and natural-mode records. It reports prediction counts
per head and refuses to pass when a head has no reference predictions across
the campaign. JointIE and other record modes still need their own holdouts.

`score_family_classification.py` separately scores these same pinned captures
against MASSIVE's gold intent labels, without rerunning inference or selecting
examples by outcome. Decide-1B native FP16 and Fastino FP32 both score 643/1,600
(40.1875%) on the 60-label public test subset. That measures task accuracy;
the 1,600/1,600 implementation agreement is a separate result. The gold score
and per-language results are retained in `evidence/summary.json`. The pinned
classification replay capture and scoring tool can reproduce the gold score.

The revised FP16 Multi-Decide candidate matched 1,599/1,600 documents: Spanish matched
199/200 and the other seven languages matched 200/200. Multi's FP16 candidate
failed the per-language gate (Arabic 198/200, Hindi 196/200, Chinese 198/200),
so its ten passing extraction fixtures do not justify enabling that precision.
Multi's FP32 candidate matched all 1,600 classification documents and 1,599/1,600
entity documents (Spanish 199/200, all other languages 200/200). The rejected
Multi-Decide bias-epilogue candidate matched 1,597/1,600 documents, with Arabic
at 198/200. Revised candidates need fresh measurements before qualification.
Current compressed holdout oracles and their report summaries are retained in
the evidence directory; rejected candidate results are in `evidence/summary.json`.
Multi FP32's structured-output campaign exercises 1,673 entity/attribute
predictions, 576 relations and 867 records. Strict parity fails French at
198/200 (Spanish 199/200, others 200/200): all three differing documents have
Fastino spans extending past the original input into its appended period.
Native preserves its existing source-valid span policy. The retained strict
structured oracle preserves that comparison. The approved compatibility policy excludes
Fastino entity predictions or relations whose spans include its single
synthetic terminal period beyond the original text. It does not clip spans,
ignore valid-coordinate differences, or change native decoding. Re-evaluating
the pinned capture under that policy matches all 1,600 documents, excluding
three entity predictions and one relation; 1,670 entities/attribute groups,
575 relations and 867 records remain checked. Every excluded prediction and
the original strict result are recorded. Other invalid coordinates, record
differences, token drift and confidence errors still fail. Use
`--source-span-policy original_text` for this explicit comparison, or
`recheck_family_holdout.py` to re-evaluate a pinned strict capture without
rerunning inference. This compatibility exception does not grant release
qualification.
The entity-only capture also matches all 1,600 documents under this policy,
with one synthetic-terminal entity prediction excluded; its strict comparison
already passed the per-language threshold.
Fastino's FlashDeBERTa FP16 reference matched 1,598/1,600 documents with either
FP32 or FP16 resident weights: Arabic and Hindi matched 199/200, the remaining
languages 200/200. This establishes classification quality for those profiles,
not their relative speed or suitability for Multi's extraction tasks.
The resident-FP16 Fastino profiles also pass all 18 Multi-Decide and all 20
Decide-1B short/capacity fixtures with FlashDeBERTa and SDPA respectively.
Decide-1B's resident-FP16 SDPA reference also passes its public comparison:
English, Arabic and Hindi match 199/200, and the other languages match 200/200.

```sh
python scripts/gliner25/check_family_holdout.py \
  --native zig-out/bin/antfly-inference-gliner25-cuda-bench \
  --python /path/to/reference/bin/python --model multi_decide \
  --model-dir /models/multi-decide --precision fp16 \
  --output /tmp/multi-decide-public-parity
python scripts/gliner25/check_family_reference_holdout.py \
  --python /path/to/reference/bin/python --model multi_decide \
  --model-dir /models/multi-decide --dtype fp16 --weight-dtype fp16 \
  --attention flashdeberta \
  --oracle-report testdata/gliner25/family/evidence/l4_multi_decide_fp16_compact_public_classification/report.json \
  --output /tmp/fastino-multi-decide-public-parity
ANTFLY_GLINER25_FAMILY_MODEL_DIR=/models/multi-decide \
  zig build test -Dcuda=true -Dmetal=false \
  -Dtest-filter='GLiNER family CUDA typed decisions'
```

The opt-in HTTP test uses actual loopback requests at concurrency 1/4/16,
compares distinct requests with their serial results and checks admission
cleanup. Multi-Decide FP32 passes all three levels with every lease released
and no leaks; recorded results and the source log hash are retained in
`evidence/summary.json`.
CUDA boundary requests queue before reserving device scratch, so waiting
requests do not each reserve the same session workspace. A queued client
disconnect also drains while that lane remains locked, with no device
reservation or kernel launches; retry returns the identical response.
It uses the existing test-only boundary qualification override;
passing it does not publish a production capability or establish serving
performance.
`ANTFLY_GLINER25_FAMILY_CUDA_PRECISION=fp16` selects the candidate precision in
these opt-in tests. Its per-manager override and factory entry point exist
only in test builds; production loads retain the release gate. The same
variable selects precision for the managed 1B full-context and boundary-window
tests.
The model-backed managed window test also passes 2/8/32 windows per document,
comparing window batch sizes one and four, exact window counts, pre-cancelled
and interrupted calls, and identical retry responses. Its retained summary is
correctness evidence; paired window performance is still required.

