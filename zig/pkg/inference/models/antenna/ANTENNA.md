# Antenna

Antenna is a proposed Antfly-owned encoder model. It reads a text once and
serves several learned heads from that one encoding: GLiNER2.5-style
extraction (entities, relations, records), classification, Laya typed
decisions, embeddings, and chunk boundaries. Its backbone is ModernBERT
(`antenna-large`) or mmBERT (`antenna-multilingual`), and it is distilled
from open Apache-2.0 teachers rather than trained from scratch.

This document records why we want it, the design, the decisions already
taken, the evidence behind them, and a gated plan. **Nothing here is
implemented yet.** Statements about third-party models are sourced; our own
inferences are labelled.

The name follows the ant theme of the project: an ant's antennae are one
organ that does many jobs (nestmate recognition, chemical identification,
trail following), as this model is one encoder with many heads.

## Summary

- [GLiNER2.5-Decide](https://fastino.ai/blog/gliner-2-5-decide-open-weight-decision-model)
  is a post-trained `fastino/gliner2-large-v1`. It has the **same 419 tensors
  and the same heads** as its base. It adds weights, not architecture.
- GLiNER2 already answers many questions about one text in **one forward
  pass**: every task schema is concatenated in front of the text under full
  attention. That buys most of the amortization Laya's tree packing buys,
  without retraining, but the text's encoding then depends on the question
  set, so it cannot be cached or reused as an embedding.
- We therefore do **not** port tree packing onto released GLiNER checkpoints.
  Instead we build one encoder whose heads include GLiNER's, on the backbone
  that already has tree packing, segment attention, a state cache, fused
  kernels, and 8k pretraining: ModernBERT (and mmBERT for multilingual).
- The GLiNER2 heads' outputs are indexed by **words, labels, and tasks, never
  by tokenizer pieces**. A ModernBERT student can therefore be distilled
  directly from DeBERTa teachers' outputs.
- The plan starts with a cheap PyTorch distillation experiment. Native
  runtime work begins only after the student is shown to hold the teacher's
  quality.

## Background

### GLiNER2.5-Decide

| Property | Value | Source |
| --- | --- | --- |
| Base | `fastino/gliner2-large-v1`, architecture `span` (`SpanExtractor`), `max_width` 8, `markerV0` span mode, `count_lstm` | [model card](https://huggingface.co/fastino/GLiNER2.5-Decide) and `config.json` at `7ee5da4c2415e32259bcdc0b1a7367c32ce8d6f6` |
| Encoder | `microsoft/deberta-v3-large`: 24 layers, hidden 1024, 128,011-token English vocabulary, relative positions (256 buckets) | `encoder_config/config.json` |
| Parameters | 340M (vendor figure) | blog |
| License | Apache-2.0 | model card |
| Language | English only (`language: en`) | model card |
| Benchmark | "Fast Decisions" suite, 5,100 examples over 17 datasets: 60.1% average, vs JevK5 57.5%, Laya 46.6%. Vendor-run; not reproduced | blog |
| Latency (p50, vendor) | 167.3 ms on 48-vCPU Xeon at 64 tokens; 38–53 ms on V100/T4/L4/A100 | blog |

**Tensor inventory (measured).** Reading the safetensors headers of Decide
and of `gliner2-large-v1` (`bf90d758a5d482bbfc276041b8cb7b570e5318e3`)
gives identical sets: 419 tensors each, with heads `classifier` (4 tensors),
`count_embed` (9), `count_pred` (4), and `span_rep` (12). None is an
attention head. `classifier` is a two-layer MLP applied to each `[L]` label
marker's encoder output.

**Input layout (upstream GLiNER2).** Tasks are serialized before the text,
for example
`( [P] intent ( [L] a [L] b ) ) [SEP_STRUCT] ( [P] urgency ( … ) ) [SEP_TEXT] text`.
Attention is full and bidirectional, so the text sees every schema and the
schemas see each other and the text. The model card's email-triage example
scores intent, urgency, and route "in one call".

**Upstream training losses** (`gliner2/models/span/model.py`, GitHub
`fastino-ai/GLiNER2` at `55656fbfa01d3d4a77485e1a1eeeaf682990ccdf`):

- classification: `classifier(emb[L])`, one logit per label, binary cross
  entropy per label, even for single-label tasks;
- spans: `compute_struct_loss`, binary cross entropy over (span, label)
  scores;
- counts: `count_pred(emb[P])`, cross entropy over 20 count classes.

**Constrained decoding is open source.** The same repository ships a
constraint AST and decoders (`gliner2/classification/constraints.py`,
`decoding/{exact,beam,independent}.py`) that pick the best joint assignment
under implications, exclusions, cardinality, and ordinal bounds. Long
documents are chunked, logits are aggregated, and decoding runs once on the
aggregate (`classification/long_text.py`), so constraints hold on the whole
document. The decoder consumes scores and is independent of the model.

**Upstream "boundary" is not chunking.** `gliner2/models/boundary/` is an
alternative extraction head (start/end boundaries with proposals), not a
document chunker. Boundary and span checkpoints are not interchangeable.

### What Antfly already runs

| Path | Checkpoints | Classification | Encoder | Location |
| --- | --- | --- | --- | --- |
| Legacy GLiNER2 pipeline | `span` checkpoints, including `gliner2-large-v1` and Decide | **Not the learned head.** Labels are encoded as an `entities` schema and scored as the maximum span sigmoid | DeBERTa only | `src/pipelines/gliner.zig` (`scoreLabels` at line 1329, `scoreLabelsFromLogits` at 1700); `session_factory.zig:414` |
| GLiNER2.5 boundary core | `fastino/gliner2.5-{small,base,multi}-v1` (`boundary`) | Learned `classifier` over `[L]` states (`classifyNative`) | DeBERTa only: `Backbone` is `small`/`base`/`multi` with hard-coded hidden and vocabulary sizes | `src/architectures/gliner/boundary_*.zig`, `src/models/gliner_boundary.zig`, [GLINER25.md](../gliner2/GLINER25.md) |
| GLiNER2 fine-tuning | span checkpoints | trains `classifier` | DeBERTa (`deberta_graph`) | `src/finetune/gliner2_real_autodiff.zig`, [FINETUNING.md](../gliner2/FINETUNING.md) |
| Laya | `convaiinnovations/laya*` | decision head | ModernBERT-large / mmBERT-base; tree packing, segment attention, trunk cache | [LAYA.md](../laya/LAYA.md) |

Two consequences:

- `POST /ai/v1/extract` schema v2 runs only boundary checkpoints
  (`server.zig`: `gliner_architecture != .boundary` →
  `UnsupportedExtractionModel`). Decide, a span checkpoint, would be served
  through the legacy pipeline, which ignores the head Decide was post-trained
  through.
- The boundary core runs DeBERTa at `max_len` 4096 per window using relative
  positions, and qualifies long-document windowing up to 182,000 bytes
  ([GLINER25.md](../gliner2/GLINER25.md), sections 9 and 11). DeBERTa-v3 is
  **not** hard-capped at 512 tokens; its 512 position embeddings are unused
  (`position_biased_input: false`). Its limits are pretraining length and
  cost, not a cap.

`pipelines/chunking.zig` is fixed-size text splitting with no model.

### How GLiNER2 compares with tree-packed Laya

In the table of open Jev reproductions in [LAYA.md](../laya/LAYA.md),
GLiNER2 sits with `open-jev-deberta-v3-large`: one pass over all questions
together, where the state sees every question.

| Property | GLiNER2 (schema prefix, full attention) | Tree-packed Laya |
| --- | --- | --- |
| Text encoded once per request | yes | yes |
| Cost of one more question | its schema tokens | its branch tokens |
| Text encoding independent of the questions | no | yes (trunk) |
| Reusable across requests (state cache) | no | yes |
| Questions isolated from each other | no | yes |
| Early fusion (text sees the question) | yes | no |
| Needs retraining to get the property | no, native | yes, and qualified: packed fine-tune 0.574 vs unpacked 0.572 on typed-decisions ([LAYA.md, Accuracy](../laya/LAYA.md#accuracy-step-0)) |

## Decisions

Each decision records its reason. Proposed decisions still need sign-off in
review.

1. **Do not tree-pack released GLiNER checkpoints.** Their extraction relies
   on text tokens attending to the schema. Removing that needs retraining
   and an accuracy gate, on a model whose benchmark inputs are about 64
   tokens, where there is little to save. GLiNER2's schema prefix already
   amortizes the text across questions.
2. **Backbone: ModernBERT, with mmBERT for multilingual.**
   - 8,192-token pretraining, against DeBERTa-v3's 512.
   - RoPE with explicit logical positions, which tree packing needs. This is
     already implemented (`modern_bert.forwardPackedCT`, segment attention
     on CPU and Metal). DeBERTa's disentangled relative attention would need
     it reimplemented.
   - Alternating local (128-token window) and global attention, so cost grows
     slowly with length.
   - Our fused Metal and CUDA kernels target ModernBERT.
   - One architecture covers English (ModernBERT) and multilingual (mmBERT),
     for Laya and Antenna alike. The DeBERTa multilingual option,
     `mdeberta-v3-base`, has no large size. `deberta-v3-large` is English
     only.
   - Laya already uses this backbone, so the Laya decision head can share the
     trunk.
3. **Distill; do not pretrain.** Fastino's post-training data and recipe are
   unpublished. The teachers are Apache-2.0, and their outputs line up with
   a ModernBERT student word for word and label for label (see
   [Distillation](#distillation)).
4. **Heads: the GLiNER2.5 boundary family, not the legacy span family
   (proposed).** The boundary core is what we already serve natively, with
   learned classification, relations, records, counts, abstention,
   long-document windowing, and native CPU, Metal, and CUDA training.
   Decide's value is in its classification head, whose outputs (one logit
   per `[L]`) are the same shape in both families, so Decide can still teach
   classification to a boundary student. Revisit if the PyTorch prototype
   shows the span head distills measurably better.
5. **Name: Antenna.** Variants `antenna-large` (ModernBERT-large),
   `antenna-multilingual` (mmBERT-base), and later `antenna-base`. "GLiNER"
   stays out of the model name to avoid implying Fastino's endorsement.
   Attribution: "GLiNER2-compatible heads, distilled from GLiNER2.5-Decide
   and GLiNER2.5 (Apache-2.0)".
6. **Port the constrained decoder separately.** It consumes scores, so it can
   sit in `/ai/v1/extract` in front of any backend, Laya included, and does
   not wait on Antenna.

## Design

### Model

```
text ─► ModernBERT / mmBERT encoder ─┬─► boundary extraction head   (entities, relations, records)
       (schema prefix, or tree-      ├─► classifier on [L]          (GLiNER2 classification)
        packed trunk + branches)     ├─► count / abstention heads
                                     ├─► Laya decision head         (choice, score, noul)
                                     ├─► attention-pool embedding   (dense vector)
                                     └─► boundary tagger            (chunk starts, per word)
```

- **Encoder:** ModernBERT-large (hidden 1024, 28 layers) or mmBERT-base.
  Special tokens `[P] [L] [E] [C] [R] [SEP_STRUCT] [SEP_TEXT]` are added to
  the tokenizer.
- **Word pooling:** first sub-token per word, as upstream's
  `token_pooling: first`. The word splitter must match upstream's so that
  teacher and student outputs align (see below).
- **Extraction and classification:** GLiNER2.5 boundary head and `classifier`
  MLP, unchanged except for the encoder width they read.
- **Decision head:** Laya's head ([LAYA.md](../laya/LAYA.md)), reading the
  shared encoder.
- **Embedding:** an attention-pool head, a learned query over the text
  tokens plus a projection. Late-interaction (ColBERT-style) per-token
  vectors could come from the same states later.
- **Chunking:** a per-word binary tagger for "a chunk starts here". Spans do
  not fit because `max_width` is 8 words.

### Why embeddings force a layout decision

With the GLiNER2 layout, text tokens attend to the schema, so a pooled
embedding would change with whatever tasks shared the pass. A document could
get different vectors from different requests, which breaks an index. The
options:

1. **Fixed prefix.** An index's enrichment schema is fixed by its config, so
   embeddings are deterministic within that index, and queries use the same
   prefix. Changing the schema means re-embedding.
2. **Tree-packed trunk.** The text is a schema-blind trunk and each task is a
   branch, the Laya layout. Embedding and chunk heads read the trunk, which
   is deterministic and cacheable. Extraction loses early fusion. A middle
   option keeps the trunk schema-blind in the lower layers, pools the
   embedding there, and runs full attention in the top few layers only.
   Laya's question-mode result (packed matched unpacked) is encouraging but
   covers decisions, not span extraction.
3. **Separate pass** for embedding and chunking. Always correct, no shared
   compute.

The plan starts with the GLiNER2 layout (the exact distillation target) and
decides between these options at step 5, with measurements.

### Expected cost

Not measured yet; these are expectations to test in step 0.

- For the same size, ModernBERT should be as fast as DeBERTa-v3 or faster,
  with the gap growing with length: disentangled attention adds
  content-to-position and position-to-content score terms and gathers,
  while RoPE is a cheap rotation and most ModernBERT layers are local.
- At about 64 tokens (Decide's benchmark length), attention is a small share
  of the work. ModernBERT-large's 28 layers against DeBERTa-large's 24 may
  cancel the gain. The GLiNER heads cost the same on either backbone.
- Laya's ModernBERT-large in our runtime: a 55-token input takes about
  58 ms on Metal and 336 ms on CPU (M4 Max, [LAYA.md, Cost](../laya/LAYA.md#cost)).
- mmBERT's large vocabulary helps non-English text; ModernBERT's 50k English
  BPE produces more tokens there.

## Distillation

### Why DeBERTa teachers can teach a ModernBERT student

Every learned output is indexed by word, label, or task:

| Output | Indexed by |
| --- | --- |
| classification logits | (task, label) via `[L]` markers |
| extraction scores | (word start, word end or width, label) |
| counts | task via `[P]` markers |
| decisions (Laya) | (question, option) |

None is indexed by tokenizer piece. With the same word splitter and the same
schema serialization, the teacher's tensors align element for element with
the student's, even though DeBERTa and ModernBERT tokenize differently.
Hidden-state matching is optional, and would need a word-level alignment.

### Teachers

| Target | Teacher | Notes |
| --- | --- | --- |
| Classification | `fastino/GLiNER2.5-Decide` | per-label probabilities from `classifier` |
| Extraction (English) | `fastino/gliner2.5-base-v1` (`72ac19b486cd4557424c8d61114e7530c243e9b0`) | same head family as the student |
| Extraction (multilingual) | `fastino/gliner2.5-multi-v1` (`aaecfe45db1d828c963717054ccb868e8ad1f1d5`) | mDeBERTa-based; covers `antenna-multilingual` |
| Decisions | released Laya, or a packed Laya fine-tune | as in `prepare_laya_packed_distillation.py` |
| Long documents | windowed teachers for extraction; an LLM teacher for document-level classification (LAYA.md step 2a) | |
| Embeddings | the embedder currently served for the index | plus a contrastive objective |

### Losses

| Head | Student loss | Target |
| --- | --- | --- |
| `classifier` | BCE per label (as upstream) | teacher probabilities, blended with gold where it exists: `w·gold + (1−w)·teacher`, the rule in `prepare_laya_packed_distillation.py` |
| Extraction | the boundary head's own losses | dense word-level targets where the head defines them; the teacher's scores over the teacher's candidates plus gold. Include confident negatives, which carry most of the signal |
| Counts | cross entropy | teacher count distribution |
| Decisions | RLCD or soft cross entropy | Laya's calibrated distribution |

Ablations once the baseline trains: logit mean-squared error instead of BCE
on probabilities; copying teacher head weights into the student (both large
models have hidden size 1024), with the heads frozen for a first stage so the
encoder is pulled into the teacher's representation space; starting from
Laya's encoder instead of raw ModernBERT-large.

### Data

The teachers are zero-shot, so both texts and schemas can be generated:

- **Texts:** the domains Decide targets (support and banking intents, email
  and ticket routing, reviews, moderation), public sets (Banking77, CLINC150,
  AG News, SST-5, MASSIVE, LocalLLaMA/typed-decisions), the enriched 10k
  Wikipedia set, and generic web text.
- **Schemas:** real label sets from those datasets plus LLM-generated label
  sets and descriptions. Randomize label order, distractor labels, and the
  number of tasks per sequence, and mix extraction with classification in one
  sequence, so the multi-task single pass is preserved.
- **Truncation rule:** never train on a teacher distribution computed from
  text the student sees but the teacher did not (the rule already used for
  Laya distillation).
- **Volume:** start with a few hundred thousand examples. This is a guess to
  revisit after the step-1 learning curves.
- Keep a provenance and hash sidecar for every generated set, as the Laya
  distillation script does.

## Plan

Each step has a gate. A step starts only after the previous gate passes.

| Step | Work | Gate |
| --- | --- | --- |
| 0. Baselines | Score Decide, `gliner2.5-base`, released Laya, and packed Laya on one harness: typed-decisions, Banking77, CLINC150, AG News, SST-5 (accuracy, soft CE, ECE), zero-shot NER (CrossNER, MIT) F1, and the `testdata/gliner25` pipeline cases. Time `gliner2.5-base` (native boundary core) against Laya's ModernBERT at 64, 512, and 2k tokens on CPU and Metal. Decide runs in PyTorch through `gliner2`. | None; this sets the targets. Record them here |
| 0b. Constrained decoder | Port upstream's constraint AST and decoders into the extract API, scoring-backend agnostic | Decisions equal upstream's on its tests; Laya and GLiNER backends both use it |
| 1. PyTorch prototype | Upstream `gliner2` boundary model with `model_name` = `answerdotai/ModernBERT-large` (upstream loads encoders with `AutoModel`; unverified for ModernBERT), teacher target dumps, distillation losses. Also a ModernBERT-base run against `gliner2.5-base` for a same-size comparison | Within about 2 points of the teacher's average on gold; ECE no worse; top-1 agreement and span F1 against the teacher reported. If the student is more than 2–3 points behind, stop and keep DeBERTa for short extraction and classification |
| 2. Long context | Windowed-teacher and LLM-teacher labels; train at 2k–8k | No loss at 512 tokens; better than windowed `gliner2.5-base` on long documents |
| 3. Native runtime | Generalize `gliner_boundary.Backbone` and the boundary engine to a ModernBERT/mmBERT encoder; converter; oracle fixtures and parity under the `testdata/gliner25` policy; a qualification row through the two-tier gate ([GLINER25.md](../gliner2/GLINER25.md)); boundary heads on the ModernBERT training graph Laya uses | Parity with PyTorch within the existing boundary tolerances; qualification row reviewed |
| 4. Decision head | Laya head on the Antenna trunk, trained with the extraction distillation losses kept on as an anchor | Within noise of packed Laya on typed-decisions; extraction metrics unchanged |
| 5. Embedding and chunk heads | Attention-pool embedding and per-word boundary tagger; choose among the three layouts above by measurement | Embedding: retrieval on the 10k Wikipedia set within an agreed tolerance of the teacher embedder. Chunking: retrieval with learned chunks no worse than fixed chunking |
| 6. Tree-packed trunk | Schema-blind text trunk with task branches, reusing Laya's packer, segment attention, and trunk cache | Extraction F1 and classification within noise of the unpacked Antenna, the same gate as LAYA.md step 0 |

Independent of Antenna, serving span checkpoints (Decide) through the
learned `classifier` would fix the legacy classification path. It is not on
the critical path because step 0 can score Decide in PyTorch.

## Risks and open questions

- **Quality:** DeBERTa-v3 is strong on short-context NER and classification.
  Community ModernBERT GLiNER models are believed to trade some accuracy for
  speed and context (not verified). Step 1 exists to measure this cheaply.
- **Embedding quality:** a head on an extraction encoder will not match a
  dedicated embedder without contrastive fine-tuning of the encoder, which
  risks the extraction heads. Multi-task batches and the distillation anchor
  are the mitigation; step 5 measures it.
- **Training memory:** the native training graph materializes attention and
  admits about 2k tokens at batch 1 (LAYA.md step 2c). Steps 1–2 run in
  PyTorch for this reason.
- **Vendor figures:** Decide's benchmark and latency numbers are Fastino's.
  Step 0 replaces them with ours.
- **Multilingual teachers:** `gliner2.5-multi-v1` is base size; whether it is
  a strong enough teacher for an mmBERT student is open.

## References

- Laya tree packing: [antflydb/antfly#875](https://github.com/antflydb/antfly/pull/875), [LAYA.md](../laya/LAYA.md)
- GLiNER2.5-Decide: [blog](https://fastino.ai/blog/gliner-2-5-decide-open-weight-decision-model), [model](https://huggingface.co/fastino/GLiNER2.5-Decide) (`7ee5da4c2415e32259bcdc0b1a7367c32ce8d6f6`)
- GLiNER2 base: [fastino/gliner2-large-v1](https://huggingface.co/fastino/gliner2-large-v1) (`bf90d758a5d482bbfc276041b8cb7b570e5318e3`)
- GLiNER2 library: [fastino-ai/GLiNER2](https://github.com/fastino-ai/GLiNER2) (`55656fbfa01d3d4a77485e1a1eeeaf682990ccdf`); paper [arXiv:2507.18546](https://arxiv.org/abs/2507.18546)
- GLiNER2.5 boundary checkpoints and pins: `scripts/gliner25/oracle_manifest.json`
- Antfly GLiNER2.5 runtime: [GLINER25.md](../gliner2/GLINER25.md), [scripts/gliner25/README.md](../../scripts/gliner25/README.md), [FINETUNING.md](../gliner2/FINETUNING.md)
- Encoders: [answerdotai/ModernBERT-large](https://huggingface.co/answerdotai/ModernBERT-large) ([arXiv:2412.13663](https://arxiv.org/abs/2412.13663)), [jhu-clsp/mmBERT-base](https://huggingface.co/jhu-clsp/mmBERT-base), [microsoft/deberta-v3-large](https://huggingface.co/microsoft/deberta-v3-large), [microsoft/mdeberta-v3-base](https://huggingface.co/microsoft/mdeberta-v3-base)
- Evaluation data: [LocalLLaMA/typed-decisions](https://huggingface.co/datasets/LocalLLaMA/typed-decisions)
