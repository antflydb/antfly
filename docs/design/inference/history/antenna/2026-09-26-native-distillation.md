# Antenna native distillation, 2026-09-26

> Summary of a verbatim log. Full text: [docs/design/inference/history/antenna/2026-09-26-native-distillation.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/inference/history/antenna/2026-09-26-native-distillation.md). Follows the [pilot](2026-09-25-pilot.md); continues in [2026-10-03-embedding-layout.md](2026-10-03-embedding-layout.md). Living design: [ANTENNA.md](../../../../../zig/pkg/inference/models/antenna/ANTENNA.md).

The pilot found that a raw trunk trained on task labels collapses while feature distillation from gliner2.5-base's encoder through a fitted projection does not. This entry completes the recipe in PyTorch (stage 3 task fine-tuning), keeps the projection as a GLiNER neck, implements distillation natively in Zig, finds a resident-Metal Q·Kᵀ bug that had corrupted earlier ModernBERT results, and iterates on distillation pools and licensing. Outcome: a fully native ModernBERT student matching its teacher on NER (classification trails by 0.07 in-domain / 0.10 held out), published privately as antflydb/antenna-0 with a clean (permissive-data) recipe.

## Decisions
- **After distillation, plain task fine-tuning works**; the distillation anchor changes nothing (stage 3 with anchor 0.739 / 0.345 vs task-only 0.742 / 0.345 classification).
- **Projection stays as an explicit GLiNER neck** (`gliner_neck.{weight,bias}`, `"antenna_neck": "linear"`) — it cannot fold into the heads (boundary heads pad with learned BOS/EOS states and run attention blocks with normalization); other head families read raw trunk states; a later trunk can distill into the same space and reuse the heads; ~0.7% of ModernBERT-base per-token compute. ANTENNA.md decision 9.
- **Native implementation**: neck (97de247f5d; run fingerprints hash pre-neck fields so existing identities hold), z-space MSE distillation objective (93bacbf45a), jobs with a frozen GLiNER2.5 teacher and unlabeled rows (85cd3888b4, 2dfef6289c, 68be2cc656), ridge neck fit in f64 (ad38d2d06a, later on the job's backend), parity (56a738ed9c: routed states within 2e-5, gradients within 3.7e-4).
- **Distillation pool widened** (`distill_pool.py --source`): NuNER, MASSIVE, GoEmotions, SQuAD, DBpedia alongside news, Banking77, Wikipedia — 274,109 rows, 60% entity schemas; no evaluation sets. Then `--label-sets` (real label sets + 65 hand-written sets in `scripts/antenna/label_sets.json`) and `--source openjev` typed decisions: 252,352 rows (run19).
- **Clean recipe — only permissively licensed training data**: AG News (non-commercial terms) and MIT restaurant/movie (no license) become evaluation-only; `antenna_training_sets.py` pins HuffPost (CC BY 4.0), DBpedia, MASSIVE, Few-NERD, MultiCoNER v2; `teacher_targets.py --recipe clean`. Large type sets sampled per row (all gold + negatives, up to 24 queries) after `BoundaryQueryLimitExceeded` with all 66 Few-NERD types. Brackets/parentheses in type names are dropped (reserved by the native schema compiler).
- **antflydb/antenna-0** (private HF): clean `student/` (run21), `gliner/` (run22, then run23 with SNIPS BookRestaurant, CC0 1.0), `decision/` (dec7). Its git history still has the earlier version.
- **Laya decision head on the Antenna trunk** (`init_decision_head.py`): encoder tensors copy unchanged; `freeze_layers = num_hidden_layers + 1` freezes the whole encoder including final norm, without a new job field, so Laya run identities hold.

## Rejected / superseded
- **All earlier ModernBERT results trained on resident Metal with the materialized attention profile** (pilot, native stage 3, native distillation) — trained on corrupted encoder states; superseded. Discarded numbers: stage 3 hard labels 0.620 / 0.349 classification, 0.342 / 0.251 NER; pure distillation 2,250 steps 0.123 / 0.143.
- Ruled out during that investigation: schema augmentation (upstream without it 0.751 / 0.680), gold injection, negative-query sampling (removing it is worse, 0.616 / 0.226), export round trip (exact, 1.8e-12).
- **Mixing Open-Jev into the decision head for one epoch** — 0.404 vs 0.434, hurts score questions; as a first stage followed by three step-0 epochs it helps (0.476).
- **Larger microbatches** — batch 8 gives 960 vs 714 examples/min at batch 4 and still exceeds the Studio's device limits; batch 16/32 don't fit with the materialized attention profile on 36 GB.

## Defects found
- **Resident Metal dropped the transposed operand of Q·Kᵀ**: dots with a transposed right operand were validated but launched with `rhs_contract_axis = 0`, so `matmul3DTransB` used K untransposed, for every shape (tiny test models hid it). DeBERTa training and ModernBERT's fused profile never emit this dot. Fixed in 530424b2fe; encoder output now matches PyTorch at 1e-11 (was 3.69 z-space MSE; first step loss 79.37 instead of 43.25).
- Optional "touch" heads received explicit zero gradients under pure distillation (weight decay moved them); the job initialized its optional teacher by assigning `undefined`, which ReleaseFast read as null.
- Trainer's fixed 64 MiB `progress.jsonl` cap stopped run17 at microbatch 57,505; the cap now sizes itself to the run (#915).

## Key measurements (classification / NER F1, mean in-domain / held-out)

| Model | Classification | NER F1 |
|---|---|---|
| gliner2.5-base, released | 0.728 / 0.483 | 0.543 / 0.515 |
| gliner2.5-base + pilot recipe (upstream trainer) | 0.808 / 0.467 | 0.723 / 0.624 |
| PyTorch stage 2 (14,000 steps) | 0.662 / 0.378 | 0.449 / 0.364 |
| native stage 3 from PyTorch stage 2 (fixed) | 0.743 / 0.345 | 0.684 / 0.511 |
| upstream trainer, same start | 0.741 / 0.353 | 0.673 / 0.482 |
| native distillation, 80k pool, run14 (8,000 steps) | 0.603 / 0.353 | 0.323 / 0.285 |
| native distillation, Wikipedia pool, run15 (14,850 steps, 10.6 h) | 0.624 / 0.359 | 0.435 / 0.379 |
| native distillation, mixed pool, run17 (34,264 steps, ~23 h) | 0.637 / 0.372 | 0.516 / 0.524 |
| stage 3 from run15 (run16) | 0.740 / 0.316 | 0.696 / 0.497 |
| stage 3 from run17 (run18) | 0.734 / 0.368 | 0.719 / 0.629 |
| clean-pool distillation (run21) | 0.655 / 0.372 | 0.503 / 0.520 |

- Native training now matches upstream's trainer on real data, which the earlier CUDA-only parity campaign never established for CPU or Metal.
- **Gap probe** (`scripts/antenna/gap_probe.py`, run15 z-space error vs teacher): short utterances carry about twice the text error of news/Wikipedia; classification markers far more than entity markers; worst sources are the worst evaluation sets. The mixed pool (run17) cut e.g. CLINC150 words 0.513 -> 0.368, markers 0.679 -> 0.429; classification markers (0.43-0.70) remain the largest error. Marker errors vary by label draw (a second CLINC150 draw: 0.59).
- **Decision head on frozen trunk** (step-0 eval, 760 decisions): untrained 0.286; released Laya 0.387; step-0 split 3 epochs 0.434; Open-Jev then step-0 0.476 (run17 trunk); 0.443 on run19's; dec7 on run21's trunk **0.511**, best so far. Laya's full step-0 fine-tune is about 0.62.
- **Clean stage 3 (run22) vs run20 (AG News, MIT)**: equal on sets both saw or both held out (Banking77 0.626 vs 0.586; held-out classification 0.372 vs 0.388); loses AG News (0.728 vs 0.852) and MIT restaurant (0.436 vs 0.778). Adding SNIPS (run23): MIT restaurant 0.485, held-out classification 0.385, NER 0.625.
- Run21 on the throughput branch (#959): 209 microbatches per minute vs 49, bit-identical losses.

## Next (as recorded)
- Restaurant-style types beyond SNIPS (amenity, hours, price, rating) from a permissive source, or teacher-labelled restaurant queries.
- Embedding layout and classification targets continue in [2026-10-03-embedding-layout.md](2026-10-03-embedding-layout.md).
