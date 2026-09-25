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
- Much of Antenna's training stack already exists. Laya's ModernBERT
  training graph (`src/finetune/laya/graph.zig`) already trains on resident
  Metal, and the GLiNER2.5 boundary trainer already separates its encoder
  from its heads. Antenna is a shared ModernBERT trunk extracted from Laya,
  with the GLiNER2.5 heads on it, then Laya's decision head, then embedding
  and chunking heads modelled on the fused chunker's.
- Training is **Zig-first**. PyTorch serves only as the parity oracle and to
  run the two span teachers we do not serve natively. The main missing piece
  is a fused ModernBERT training attention (forward and backward), which
  also removes Laya's ~2k-token training cap.
- The plan starts with a small native distillation: GLiNER2.5 boundary heads
  on the extracted ModernBERT trunk at 512 tokens on Metal. That needs no new
  kernels, so the quality question is answered before the kernel work.

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
| Fused chunker (training scaffolding) | own checkpoints (`fused_chunker_embedder/v1alpha1`) | per-token chunk-boundary MLP; embedding head with late-chunking mean pool; optional SPLADE | ModernBERT-base (legacy weight layout) | `src/finetune/fused_chunker*.zig`, [finetuning/FINETUNING.md](../../finetuning/FINETUNING.md#training-features-by-model-family) |

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

`pipelines/chunking.zig`, the served chunker, is fixed-size text splitting
with no model. The fused chunker is not served, and its trainer is
scaffolding rather than a working pipeline (read 2026-09-25):

- The production trainer runs the encoder eagerly with no backward, so only
  the boundary MLP head learns (`train/train_fused_chunker.zig`). Its "LoRA"
  applies the head-input gradient to every layer's Q/V projections, which is
  not backpropagation (`graph/segmented_encoder.zig`).
- The contrastive path receives all-zero chunk embeddings and discards its
  gradient, so InfoNCE, Matryoshka, and cross-batch negatives teach nothing
  yet (`fused_chunker_train.zig`). Hard negatives are dropped. SPLADE trains
  only its own projection.
- It runs on CPU only, and its graph encoder (`architectures/modern_bert_graph.zig`)
  uses full attention in every layer, one RoPE theta, and a legacy weight
  layout that a stock Hugging Face ModernBERT checkpoint does not match.

Its head designs and host loss code are reusable; its trainer is not.

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
   - Our fused Metal and CUDA *inference* kernels target ModernBERT. (Fused
     *training* attention exists only for DeBERTa today; see
     [Training infrastructure](#training-infrastructure).)
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
4. **Heads: the GLiNER2.5 boundary family, distilled from both head
   families (proposed; settled by the step 1 ablation).** The choice only
   affects extraction. Both families use the same `classifier`, a two-layer
   MLP on the `[L]` states (upstream `models/boundary/model.py` calls
   `self.classifier(choice_states)`, and our `classifyNative` reads
   `classifier.0` and `classifier.2`), so Decide teaches classification to
   either.

   | | Span (GLiNER2, Decide) | Boundary (GLiNER2.5) |
   | --- | --- | --- |
   | Entity length | at most `max_width` = 8 words | any length within the window |
   | Extraction cost | words × 8 × labels, dense | sparse top-k proposals (`candidate_budget` 192) |
   | Structure | entities, structures via `[P]` and counts | also sparse relations, records, abstention, attributes |
   | Distillation | dense, fixed grid; teacher and student score the same spans | teacher and student propose different candidates |
   | Largest teacher | `gliner2-large-v1` and Decide, hidden 1024 (same as ModernBERT-large, so head weights can seed the student) | `gliner2.5-base` and `multi` (768), `small` (384) |
   | Antfly runtime | legacy pipeline only; no learned classification | schema-v2 extraction, qualification gate, long-document windowing, CPU, Metal, and CUDA serving and training |

   Boundary heads win on capability and fit our product path. Span heads win
   on distillation: dense targets and large teachers. The hybrid keeps
   boundary heads in the student and takes extraction targets from both
   families:

   - **Entities of 8 words or fewer:** any student candidate this short gets
     a dense score from `gliner2-large-v1`. Use it rather than Decide, whose
     decision post-training may have cost extraction quality (not checked).
   - **Longer entities, relations, records:** from `gliner2.5-base`
     (English) and `gliner2.5-multi` (multilingual).
   - **Classification:** from Decide.

   Upstream publishes no span-versus-boundary extraction quality comparison;
   `docs/boundary_baseline.md` times proposals on untrained weights only.
   Step 1 trains both head families on the same encoder and decides. Choose
   span heads only if they win clearly on short entities and entities longer
   than 8 words are rare in our data.
5. **Name: Antenna.** Variants `antenna-large` (ModernBERT-large),
   `antenna-multilingual` (mmBERT-base), and later `antenna-base`. "GLiNER"
   stays out of the model name to avoid implying Fastino's endorsement.
   Attribution: "GLiNER2-compatible heads, distilled from GLiNER2.5-Decide
   and GLiNER2.5 (Apache-2.0)".
6. **Port the constrained decoder separately.** It consumes scores, so it can
   sit in `/ai/v1/extract` in front of any backend, Laya included, and does
   not wait on Antenna.
7. **Build on Laya's trunk and the GLiNER2.5 trainer, not the fused chunker's
   trainer.** Laya's graph is the correct ModernBERT (Hugging Face weight
   names, global and local layers, two RoPE tables) and already trains on
   resident Metal. The GLiNER2.5 boundary trainer's heads read only routed
   hidden states. The fused chunker contributes head designs (boundary MLP,
   late-chunking pool, SPLADE) and host loss code (InfoNCE, Matryoshka,
   cross-batch memory), rebuilt in the graph so their gradients reach the
   encoder.
8. **Train in Zig.** The training stack already has most of what the run
   needs (see [Training infrastructure](#training-infrastructure)), and the
   kernels we write for it are the kernels customer fine-tuning ships with.
   PyTorch is used only as the parity oracle, following the existing
   `scripts/gliner25/oracle.py` and `scripts/laya/*_reference.py` pattern,
   and to run the span teachers (Decide, `gliner2-large-v1`), which we do not
   serve natively.

## Design

### Model

```
text ─► ModernBERT / mmBERT encoder ─┬─► boundary extraction head   (entities, relations, records)
       (schema prefix, or tree-      ├─► classifier on [L]          (GLiNER2 classification)
        packed trunk + branches)     ├─► count / abstention heads
                                     ├─► Laya decision head         (choice, score, noul)
                                     ├─► embedding head             (dense, Matryoshka; optional SPLADE)
                                     └─► chunk-boundary MLP         (chunk starts, per token)
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
- **Embedding and chunking:** the fused chunker's head designs: a per-token
  two-layer boundary MLP, an embedding head with late-chunking mean pooling
  per chunk, and an optional SPLADE sparse head, trained with InfoNCE,
  Matryoshka dimensions, and cross-batch negatives. They are rebuilt in the
  training graph (or seeded with a host-computed cotangent, as Laya's
  objective is) so the gradient reaches the encoder. A learned
  attention pool is an ablation, not the default. Late-interaction
  (ColBERT-style) per-token vectors could come from the same states later.

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
decides between these options at step 7, with measurements.

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
| Extraction, entities of 8 words or fewer | `fastino/gliner2-large-v1` (`bf90d758a5d482bbfc276041b8cb7b570e5318e3`) | span family, large; dense scores for every short candidate (Decision 4) |
| Extraction (English): long entities, relations, records | `fastino/gliner2.5-base-v1` (`72ac19b486cd4557424c8d61114e7530c243e9b0`) | same head family as the student |
| Extraction (multilingual) | `fastino/gliner2.5-multi-v1` (`aaecfe45db1d828c963717054ccb868e8ad1f1d5`) | mDeBERTa-based; covers `antenna-multilingual` |
| Decisions | released Laya, or a packed Laya fine-tune | as in `prepare_laya_packed_distillation.py` |
| Long documents | windowed teachers for extraction; an LLM teacher for document-level classification (LAYA.md step 2a) | |
| Embeddings | ModernBERT-family embedders: [`ibm-granite/granite-embedding-english-r2`](https://huggingface.co/ibm-granite/granite-embedding-english-r2) (English), [`ibm-granite/granite-embedding-311m-multilingual-r2`](https://huggingface.co/ibm-granite/granite-embedding-311m-multilingual-r2) (multilingual; its config reports `modernbert`, presumably mmBERT-based) | plus the fused chunker's InfoNCE objective. Check that our embedding pipeline loads ModernBERT embedders before relying on them |

### Losses

| Head | Student loss | Target |
| --- | --- | --- |
| `classifier` | BCE per label (as upstream) | teacher probabilities, blended with gold where it exists: `w·gold + (1−w)·teacher`, the rule in `prepare_laya_packed_distillation.py` |
| Extraction | the boundary head's own losses | Candidates of 8 words or fewer: the span teacher's score for that (span, label), available for any candidate the student proposes. Longer candidates, relations, and records: dense word-level targets where the boundary head defines them, plus the boundary teacher's scores over its own candidates and gold. Include confident negatives, which carry most of the signal |
| Counts | cross entropy | teacher count distribution |
| Decisions | RLCD or soft cross entropy | Laya's calibrated distribution |

Ablations once the baseline trains: logit mean-squared error instead of BCE
on probabilities; copying teacher head weights into the student where widths
match (Decide's `classifier` and span heads into `antenna-large`, both 1024;
the boundary heads of `gliner2.5-base` only into a 768-wide `antenna-base`),
with the heads frozen for a first stage so the encoder is pulled into the
teacher's representation space; starting from Laya's encoder instead of raw
ModernBERT-large.

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

## Training infrastructure

Antenna trains natively. This inventory was taken from the tree on
2026-09-25 (`src/finetune/`, `src/graph/`, `lib/ml/src/graph/`,
[finetuning/FINETUNING.md](../../finetuning/FINETUNING.md)).

### Already in Zig

| Capability | Where | Notes |
| --- | --- | --- |
| Autodiff graph; AdamW, schedule-free AdamW, layer-wise LR decay, global gradient clipping, gradient accumulation | `lib/ml/src/graph/`, `src/finetune/` | all model families |
| LoRA, LoRA+, QLoRA (NF4), recursive LoRA | `src/finetune/lora*.zig`, `qlora_nf4.zig`, `recursive_lora.zig` | |
| Activation checkpointing and bounded recomputation | `lib/ml/src/graph/checkpoint.zig`, `src/graph/recomputed_training.zig` | |
| Device-resident training on Metal and CUDA | `src/graph/resident_training_*.zig` | GLiNER2.5 and Laya |
| Fused training attention with backward | `src/ops/cuda/gliner25.zig` (`debertaTrainingAttentionBackwardV1`) | **DeBERTa only**, CUDA |
| Host contrastive and embedding losses: InfoNCE, Matryoshka, cross-batch memory, SPLADE with FLOPS regularization, NEFTune | `src/finetune/fused_chunker_train.zig`, `infonce_cpu.zig` | loss code only; not yet connected to a trained encoder (see Background) |
| ModernBERT training graph on resident Metal | `src/finetune/laya/graph.zig`, `training.zig`, `job.zig` | Hugging Face layout, global and local layers, runtime RoPE tables and masks; attention materialized, capped at `batch·S²·heads ≤ 64Mi` |
| Encoder-independent boundary heads and losses | `src/finetune/gliner/boundary_encoder_graph.zig` (`RoutedNodes`), `boundary_train_step.zig` | encoder is one builder call; DeBERTa-specific today |
| Data-parallel training | `src/graph/distributed_training.zig`, `collective_ops.zig` | wired for ColQwen2 and Gemma4 |
| PJRT (XLA) training path | `src/graph/pjrt_*` | ColQwen2 and Gemma4 |
| Native GLiNER2.5 inference | `src/architectures/gliner/boundary_*.zig` | at least 2.3× PyTorch on CUDA in every benchmark case ([scripts/gliner25/CUDA.md](../../scripts/gliner25/CUDA.md)) |

The GLiNER2.5 CUDA trainer shows what the discipline buys. It started 1.9–4.2×
slower than eager PyTorch on full fine-tuning. Reusing device allocations,
cutting stream synchronizations (21,832 in one traced run), and replacing
generic backward reductions brought it to 1.2–2.0× faster on full training
and 1.8–3.6× on head-only training, with raw-bit-identical weights and
optimizer moments through 100 updates (small model, synthetic fixtures;
v39 in `CUDA.md`).

### Backend order

Development is **Metal first**, with CPU as the reference. The development
machines are Apple silicon (M4 Max, 36 GiB, where Laya's fine-tunes already
run), and Metal training kernels ship to customers who fine-tune on Macs.

- **CPU:** every new op gets a host kernel as its reference and fallback,
  backward included, for gradient checks. Laya's flash-style host segment
  attention (`linalg.segmentAttentionHost`) is the starting point. CPU
  *training* speed is not a target; CPU performance matters for serving.
- **Metal:** the performance target for steps 1–2 and the pilot.
- **CUDA:** each op implements the same `ComputeBackend` contract, parity
  tests, and fixtures as its Metal version, ported before step 4.

**Why step 4 probably needs CUDA (estimate, not measured).** Training costs
about 6 × parameters × tokens. ModernBERT-large (395M) on 300k examples of
512 tokens is about 3.6e17 FLOPs per epoch. An M4 Max GPU peaks around
15–18 TFLOPS in FP32; at 30% utilization that is roughly a day per epoch,
and the full run (several epochs, ablations, the 2k–8k stage) would take
weeks. A CUDA GPU running BF16 is one to two orders of magnitude faster (the
L4 used in `CUDA.md` is about 120 TFLOPS in BF16). Today's Metal trainer is
well below 30% utilization because attention is materialized: Laya's
2,000-decision fine-tune took 56 minutes. Step 4 therefore runs on CUDA
unless the Metal trainer, measured after gaps 1–2, turns out fast enough.

### Gaps, in priority order

1. **Fused ModernBERT training attention, forward and backward: Metal and a
   CPU reference first, then CUDA before step 4.** Plain attention in
   `lib/ml` (`Builder.sdpa`) is decomposed for backward, so scores are fully
   materialized; that is why the Laya trainer admits about 2k tokens at
   batch 1. The kernel needs RoPE at explicit logical positions, the
   128-token sliding window for local layers, variable-length packing (no
   padding), and the per-query segment ranges the inference-side segment
   attention already takes. It is a `ComputeBackend` op, like
   `segmentAttention`, and the ModernBERT counterpart of
   `debertaTrainingAttentionBackwardV1`. It also unblocks Laya's
   long-context step (LAYA.md step 2c) and packed training.
2. **A shared ModernBERT training trunk, and the GLiNER2.5 boundary trainer
   on it.** Extract the trunk from `laya/graph.zig`. Then make the boundary
   trainer's encoder config a choice of DeBERTa or ModernBERT, switch the
   encoder in `boundary_encoder_graph`, add a byte-level BPE tokenizer
   profile, and let PEFT handle bias-free linears. Start with the
   materialized attention profile and no layer recompute. The step 1 pilot
   needs this.
3. **Allocation and synchronization discipline on the ModernBERT graph**, the
   same kind of work that made the GLiNER2.5 CUDA trainer faster than
   PyTorch. Profile each new path before optimizing, as `CUDA.md` requires.
4. **BF16 training** with FP32 master weights. There is none today: the only
   mixed-precision path was MLX-only and was removed with the MLX backend.
   On CUDA it is a large speedup and belongs with the step 4 port. On Apple
   GPUs the certain gain is memory and bandwidth; whether BF16 arithmetic is
   faster than FP32 there is unverified, so measure on Metal before
   investing.
5. **Device all-reduce.** `collective_ops.allReduceSum` stages through the
   host (download, sum, upload). Needed only when a run needs more than one
   GPU.

### Teacher targets

- Boundary teachers (`gliner2.5-base`, `gliner2.5-multi`) run on our native
  GLiNER2.5 inference.
- Span teachers (Decide, `gliner2-large-v1`) run in PyTorch through
  upstream `gliner2`, because we do not serve span checkpoints through the
  learned heads.
- Targets are written once, sparsely (top candidates plus sampled
  negatives), with a provenance and hash sidecar, and read by every run.

### Throughput levers, independent of kernels

- Pack several examples per row with block-diagonal masks, and batch by
  length bucket.
- Train at 512 tokens first, then a separate long-context stage with
  activation checkpointing.
- Warm up the new heads with the encoder frozen before unfreezing it.
- Use LoRA or frozen lower layers when adding heads to a trained trunk.
- Tree-pack multi-task examples once the packed layout is qualified: for
  Laya this cut training time 3.4× at equal accuracy.

## Plan

Each step has a gate. A step starts only after the previous gate passes.
Step 2 is infrastructure and runs alongside step 1; step 3 is needed only
before step 4.

| Step | Work | Gate |
| --- | --- | --- |
| 0. Baselines | Score Decide, `gliner2.5-base`, released Laya, and packed Laya on one harness: typed-decisions, Banking77, CLINC150, AG News, SST-5 (accuracy, soft CE, ECE), zero-shot NER (CrossNER, MIT) F1, and the `testdata/gliner25` pipeline cases. Time `gliner2.5-base` (native boundary core) against Laya's ModernBERT at 64, 512, and 2k tokens on CPU and Metal. Decide runs in PyTorch through `gliner2`. | None; this sets the targets. Record them here |
| 0b. Constrained decoder | Port upstream's constraint AST and decoders into the extract API, scoring-backend agnostic | Decisions equal upstream's on its tests; Laya and GLiNER backends both use it |
| 1. Native pilot (Metal) | Put the GLiNER2.5 boundary heads and `classifier` (and span heads, for the Decision 4 ablation) on the shared ModernBERT-base training trunk at 512 tokens, on resident Metal (gap 2). Distill from the step-0 teachers' targets on a small dataset. Compare with `gliner2.5-base`, which is about the same size. Needs no new attention kernel | Within about 2 points of `gliner2.5-base` on gold; ECE no worse; top-1 agreement and span F1 against the teachers reported. If the student is more than 2–3 points behind, stop and keep DeBERTa for short extraction and classification. Head choice: entity F1 split into 8 words or fewer and longer, relation F1, and agreement with each teacher; keep boundary heads unless span heads win clearly on short entities and long entities are rare in our data |
| 2. Fused ModernBERT training attention (Metal, CPU reference) | Gap 1 above | Gradient parity with a PyTorch reference, as for the DeBERTa kernel, on Metal and CPU; memory linear in sequence length; Laya trains at 8k tokens on Metal. Then measure Metal training throughput on ModernBERT-large to decide where step 4 runs |
| 3. CUDA port and BF16 | Port the step-2 kernel to CUDA under the same contract (the boundary trainer already runs on resident CUDA); add BF16 with FP32 master weights (gap 4). Skip if step 2 shows Metal is fast enough for step 4 | CUDA gradient parity with the Metal and CPU paths; BF16 loss curve within tolerance of FP32 over a fixed number of updates; final evaluation within 0.5 points (the `CUDA.md` quality bar) |
| 4. Full distillation (CUDA, or Metal if fast enough) | `antenna-large` (ModernBERT-large) and `antenna-multilingual` (mmBERT-base) on the full dataset, then a 2k–8k long-context stage with windowed-teacher and LLM-teacher labels | Within about 2 points of the teachers on gold at 512 tokens; better than windowed `gliner2.5-base` on long documents |
| 5. Native serving | Generalize `gliner_boundary.Backbone` and the boundary engine to ModernBERT and mmBERT; converter; oracle fixtures and parity under the `testdata/gliner25` policy; a qualification row through the two-tier gate ([GLINER25.md](../gliner2/GLINER25.md)) | Parity with the trained checkpoint within the existing boundary tolerances; qualification row reviewed |
| 6. Decision head | Laya head on the Antenna trunk, trained with the extraction distillation losses kept on as an anchor | Within noise of packed Laya on typed-decisions; extraction metrics unchanged |
| 7. Embedding and chunk heads | The fused chunker's head designs and losses, rebuilt in the Antenna training graph; choose among the three layouts above by measurement | Embedding: retrieval on the 10k Wikipedia set within an agreed tolerance of the teacher embedder. Chunking: retrieval with learned chunks no worse than fixed chunking. Extraction metrics unchanged |
| 8. Tree-packed trunk | Schema-blind text trunk with task branches, reusing Laya's packer, segment attention, and trunk cache | Extraction F1 and classification within noise of the unpacked Antenna, the same gate as LAYA.md step 0 |

Gaps 3 and 5 (allocation discipline, device all-reduce) are taken on when
profiling or run size calls for them, each with its own benchmark evidence.

Independent of Antenna, serving span checkpoints (Decide) through the
learned `classifier` would fix the legacy classification path. It is not on
the critical path because step 0 can score Decide in PyTorch.

## Status (2026-09-25)

### Step 0: baselines (done)

Full tables, commands and pins:
[work-log/completed/inference/antenna/2026-09-25-baselines.md](../../../../../work-log/completed/inference/antenna/2026-09-25-baselines.md).
Seeded subsamples (500 classification records, 300 NER sentences per
dataset) carry roughly ±2–4 points of noise. "In-domain" means the pilot's
training datasets (Banking77, AG News, CrossNER AI/literature/music, MIT
Restaurant); "held-out" is CLINC150, SST-5, typed-decisions, CrossNER
politics/science, MIT Movie.

| Mean | Decide | gliner2.5-base | gliner2-large |
| --- | ---: | ---: | ---: |
| In-domain classification accuracy | 0.734 | 0.728 | 0.714 |
| In-domain NER F1 | 0.506 | 0.543 | 0.550 |
| Held-out classification accuracy | 0.504 | 0.483 | 0.519 |
| Held-out NER F1 | 0.534 | 0.515 | 0.544 |

Decide is the best calibrated (typed-decisions ECE 0.106 against 0.30–0.36
for the other GLiNER models). Encoder latency at 64/512/2,048 tokens:
gliner2.5-base 29/292/3,534 ms on CPU and 224/324/873 ms on Metal; Laya's
ModernBERT-large 311/1,473/12,720 ms and 209/781/4,903 ms (encoders of
different sizes; see the report for caveats).

### Step 1: pipeline (done) and pilot (running)

- `scripts/antenna/init_student.py` builds the student: pretrained
  `answerdotai/ModernBERT-base` (pinned) with fresh published heads, 158.8M
  parameters. The native training source loads it, and the processor
  matches upstream's token ids with its tokenizer.
- `scripts/antenna/teacher_targets.py` writes training rows from the
  in-domain train splits: classification rows with soft per-label targets
  `0.5 * gold + 0.5 * sigmoid(Decide logit)` over sampled label subsets,
  entity rows with gold spans. Training rows accept `probabilities`, which
  replace the 0/1 classification targets.
- `antfly-inference finetune train gliner25` trains a ModernBERT source on
  resident Metal and exports a portable model that upstream
  `AutoExtractor` loads, so `scripts/antenna/baselines.py` evaluates it.
- ModernBERT-base needs job budgets above the small-DeBERTa defaults (see
  `scripts/antenna/README.md`). On a shared machine the job's live-memory
  admission can refuse it; the admission's dynamic reserve is 9.66 GB on a
  36 GB Mac.
- The span-versus-boundary head ablation (Decision 4) is not run: span heads
  exist natively only on DeBERTa.

### Step 2: fused ModernBERT training attention (done)

A fused op computes global and sliding-window attention with linear storage
(per-row logsumexp, backward replay), CPU reference and Metal kernels.
Laya's gradient parity against PyTorch holds with it (2.1e-6 CPU and Metal),
as does the ModernBERT boundary encoder's (1.3e-5 CPU, 1.4e-5 Metal). A
4,096-token step runs on Metal past the old 64Mi score cap. On the released
Laya checkpoint, a resident Metal step at 2,048 tokens takes 25 s fused
against 65 s materialized. The materialized profile stays the default; Laya
selects `attention: fused_v1`, and the ModernBERT boundary encoder uses it
under `replay_tiled_v1`. With head dropout above 0, Laya's decision head
still materializes its attention.

## Risks and open questions

- **Quality:** DeBERTa-v3 is strong on short-context NER and classification.
  Community ModernBERT GLiNER models are believed to trade some accuracy for
  speed and context (not verified). Step 1 exists to measure this cheaply.
- **Embedding quality:** a head on an extraction encoder will not match a
  dedicated embedder without contrastive fine-tuning of the encoder, which
  risks the extraction heads. Multi-task batches and the distillation anchor
  are the mitigation; step 7 measures it.
- **Training kernels:** long-context training depends on the fused ModernBERT
  training attention (step 2). Until it lands, native training is limited to
  about 2k tokens at batch 1, which is enough for step 1 and the 512-token
  stage of step 4.
- **Per-word tokenization:** the GLiNER2.5 processor encodes each word on
  its own. SentencePiece adds its word marker to every word, but
  ModernBERT's byte-level BPE (`add_prefix_space=false`) produces the
  no-leading-space form, as at the start of a sentence, which is off the
  pretraining distribution. Check what upstream does and pin the choice in
  a fixture before training. Also confirm that id 0 is a valid pad id.
- **Compute for step 4:** the per-epoch estimate under
  [Backend order](#backend-order) is unmeasured. Step 2 ends with a real
  Metal throughput measurement that decides between Metal and CUDA.
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
