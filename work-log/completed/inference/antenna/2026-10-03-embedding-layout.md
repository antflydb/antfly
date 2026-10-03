# Antenna embedding layout and classification targets, 2026-10-03

Follows 2026-09-26-native-distillation.md. Two questions:
- **Embedding layout.** Can the shared Antenna trunk produce embeddings (ANTENNA.md, "Why embeddings force a layout decision", plan step 7)?
- **Classification targets.** Do GLiNER2.5-Decide's soft targets close the classification gap left after stage 3?

All runs use the clean recipe, with permissively licensed data only. The
student is run21, the clean distilled trunk.

## Classification: soft Decide targets do not help

Stage 3 from run21's student on the clean rows with SNIPS, as in run23, but
with classification targets `0.5 * gold + 0.5 * sigmoid(Decide)` instead of
gold. The teacher is GLiNER2.5-Decide (Apache 2.0); NER rows stay gold. This
is run24.

| Mean | Hard labels (run23) | Soft Decide targets (run24) |
| --- | --- | --- |
| All classification sets | 0.493 | 0.486 |
| Held-out classification (CLINC150, SST-5, typed decisions) | 0.385 | 0.378 |
| All NER sets | 0.636 | 0.636 |

Within noise, and if anything slightly worse. Earlier native runs with Decide
targets were void (the resident Metal Q·Kᵀ bug), so this is the first valid
comparison.

A second distillation epoch from run21's student is the remaining lever
(run25, in progress). Its stage 3 uses hard labels.

## Embedding: a head on the frozen trunk

`scripts/antenna/embedding_probe.py`, in PyTorch.

**Setup**
- **Features:** the frozen trunk's mean-pooled final states.
- **Head:** a linear or MLP head, trained on cached features with symmetric in-batch InfoNCE (temperature 0.05) over 30,000 SQuAD (question, paragraph) pairs, with or without cosine distillation toward granite-embedding-english-r2 (Apache 2.0, ModernBERT-base, CLS pooling).
- **Evaluation:** the 10,570 SQuAD validation questions retrieving their paragraph among all 2,067 validation paragraphs.

| Model | R@1 | R@10 | NDCG@10 |
| --- | --- | --- | --- |
| granite-embedding-english-r2 | 0.696 | 0.943 | 0.823 |
| ModernBERT-base, untrained, mean-pooled | 0.070 | 0.238 | 0.143 |
| Antenna trunk, no head | 0.004 | 0.025 | 0.013 |
| frozen trunk + linear head, InfoNCE | 0.269 | 0.670 | 0.454 |
| frozen trunk + linear head, granite distillation | 0.234 | 0.600 | 0.401 |
| frozen trunk + linear head, both | 0.262 | 0.665 | 0.447 |
| frozen trunk + MLP head, best | 0.234 | 0.620 | 0.410 |

**Findings**
- The best frozen-trunk head reaches 55% of the teacher's NDCG@10.
- The extraction-distilled trunk's pooled states carry almost no retrieval signal on their own (0.013), less than untrained ModernBERT-base's.
- Head capacity and the distillation target don't help, so the trunk itself would have to be trained for embeddings.

## Embedding: training the top of the trunk

`scripts/antenna/embedding_unfreeze_probe.py`.

**Setup**
- **Training:** fine-tune the top k trunk layers and the final norm (learning rate 2e-5) plus a linear head (1e-3) on 20,000 SQuAD pairs for one epoch, with InfoNCE.
- **Anchor:** optionally anchored by the z-space MSE between the updated and the original trunk's final states on paragraph tokens (weight 1).
- **Evaluation:** retrieval as above. Extraction is the GLiNER neck and heads on the updated trunk, scored with `baselines.py` against the unchanged student. These are distilled students before stage 3, so the numbers compare with each other, not with stage-3 results.

| Setting | Retrieval NDCG@10 | All NER sets | Held-out NER | All classification sets |
| --- | --- | --- | --- | --- |
| frozen trunk (student) | 0.454 (head only) | 0.510 | 0.520 | 0.485 |
| top 4 | 0.451 | 0.500 | 0.489 | 0.483 |
| top 4, anchored | 0.402 | 0.497 | 0.499 | 0.484 |
| top 8 | 0.570 | 0.463 | 0.462 | 0.481 |
| top 8, anchored | 0.476 | 0.486 | 0.503 | 0.481 |

**Findings**
- Four layers buy nothing over the frozen head.
- Eight reach 69% of the teacher and cost about 0.05 NER F1. The anchor keeps half the NER but gives back most of the retrieval gain.
- Longer training would raise retrieval, but every gain from the shared trunk is paid for in extraction quality, and it starts far below a dedicated embedder.

## Proposed step-7 decision: a separate embedding pass

Option 3 in ANTENNA.md is a separate pass. Antfly already ships
`Qwen/Qwen3-Embedding-0.6B-GGUF` (Q8_0, 1,024 dimensions, Apache 2.0) as its
qualified default text embedder on Metal and CUDA (`scripts/qwen3_embedding/`,
docs/guides/supported-models.mdx). So Antenna can do extraction,
classification and decisions while the existing embedder does embeddings.
Extraction stays untouched and no embedding training is needed.

Smaller options, if indexing cost matters:

| Model | License | Notes |
| --- | --- | --- |
| Alibaba-NLP/gte-modernbert-base | Apache 2.0 | ModernBERT-base, the trunk's architecture |
| ibm-granite/granite-embedding-english-r2 | Apache 2.0 | ModernBERT-base, 0.823 NDCG@10 above |
| Snowflake/snowflake-arctic-embed-m-v2.0 | Apache 2.0 | multilingual, Matryoshka |
| google/embeddinggemma-300m | Gemma terms (not an open-source license) | decoder-derived, instruction prompts, Matryoshka |

The teacher choice doesn't change the layout conclusion. The limit is the
extraction-trained trunk, not the target.

**Still open**
- **Decision:** owner sign-off.
- **Chunk-boundary head:** whether it can share the trunk. It is a token-level task, closer to extraction, and untested.
- **Comparison:** Qwen3-Embedding-0.6B on the same SQuAD probe, for a direct comparison with granite.
