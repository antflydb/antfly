# Decide-1B span-head experiment

This is an Antenna head ablation for issue #1083. It uses the existing Antenna
version-1 dataset, transactional seeded-gradient trainer, AdamW checkpoint
format, early-stopping state, and supervised process runtime. It does not train
the encoder or replace Antenna's shared ModernBERT boundary-head recipe.

The original Fastino Decide-1B checkpoint produces no entities or relations on
the reviewed corpus in both upstream Python and Antfly. Its extraction logits
are strongly negative. Numerical parity therefore cannot qualify useful
extraction. Original FP32 production extraction remains withheld; the published
Q8 bundle remains classification-only. A trained derivative needs its own
artifact identity, quality evaluation, backend parity, window and graph tests.

## Training and evaluation

From `zig/`, with Zig 0.17 and worktree-local build caches:

```sh
python3 tools/run_bounded_zig_build.py --zig /path/to/zig build \
  inference-train-gliner25-span -Doptimize=fast -Dmetal=true -Dcuda=false -j1 \
  -- /absolute/job.json
python3 tools/run_bounded_zig_build.py --zig /path/to/zig build \
  inference-eval-gliner25-span -Doptimize=fast -Dmetal=true -Dcuda=false -j1 \
  -- /absolute/evaluation.json
```

The integrated commands are `antfly inference finetune train gliner25-span` and
`antfly inference finetune eval gliner25-span`, with the same JSON argument.
A training job has this shape:

```json
{
  "version": 1,
  "model_dir": "/absolute/pinned-fastino-decide-1B",
  "train_file": "/absolute/train.jsonl",
  "eval_file": "/absolute/validation.jsonl",
  "output_dir": "/absolute/new-output-directory",
  "epochs": 40,
  "learning_rate": 0.00003,
  "positive_weight": 50,
  "max_host_bytes": 12884901888
}
```

Source admission requires the original complete FP32 artifact, including
weight SHA-256 `02c567d791aed26550d300064c7f0c0094fd65291503c65969b45b30786e33b3`,
all four sidecars, tensor inventory, geometry, and marker IDs. Training and
validation must have disjoint IDs and text. Rows use Antenna's existing
version-1 format, including UTF-8, codepoint, or UTF-16 source spans. Gold spans
must align exactly with prepared words and fit the eight-word span head.
Structures, entity attributes, joint extraction, and multi-label classification
are rejected. Single-label classification accepts hard labels or per-label
soft targets; accuracy always uses the human label.

The frozen encoder and earlier head layers produce cached features. Only final
affine projections `classifier.2`, `count_pred.2`, and
`span_rep.span_rep_layer.out_project.3` can update. The count-aware LSTM label
projection stays frozen. Binary span loss masks invalid/padded/source-external
cells and uses explicit positive weighting. The shared optimizer supplies
clipping, transactional updates, cancellation checks, and checkpoint resume.
The Native graph contraction uses BLAS for the gradient's dense matrix layouts.

`latest.safetensors` preserves the final optimizer state for resume.
`best.safetensors` selects held-out span-cell F1 (categorical accuracy for a
classification-only run). The exported model restores that selected checkpoint,
and the receipt distinguishes completed and exported optimizer counters.
`resume_from` binds source, exact train/eval bytes, and training configuration;
only output directory, resume path, and `stop_after_microbatches` are excluded
from that identity. Selection on a resumed invocation includes its initial
checkpoint and the subsequent epochs; earlier validation history is not
reconstructed. Cell F1 is a selection proxy, not decoded entity/relation F1.

The output contains all 199 FP32 tensors and the unchanged sidecars, with the six
selected tensors overlaid. Publication uses a staged directory and atomic rename.
The source weight digest is rechecked before publication. A receipt is attached
as `antfly_gliner_span_training.json`; it explicitly says `runtime_qualified:
false`. Nothing updates registry pins or production capabilities automatically.

Limits: at most 10,000 examples per split (default 1,024), 16 MiB per training
file, 1,024 complete prompt tokens per example, and a supervised deadline
(default four hours, maximum seven days). `max_host_bytes` bounds allocations
through the job allocator. Memory-mapped model pages, BLAS internals, and GPU
driver allocations are additional; allocator peak is not process or physical
RAM usage. Features are computed with per-record scratch storage, retained
features are bounded, and the encoder session closes before optimization.

An evaluation configuration supplies `model_dir`, `eval_file`, `output_file`,
`expected_weights_sha256`, and `backend` (`native` or `metal`). The output file
must not already exist. Evaluation validates artifact shape and sidecars,
records exact input IDs, checks decoded source offsets/text, and reports exact
label-and-span entity and directed relation scores. It grants no serving
capability. Independent upstream evaluation uses:

```sh
python pkg/inference/scripts/gliner25/evaluate_trained_span.py \
  --upstream /absolute/pinned-GLiNER2 \
  --source-model /absolute/pinned-fastino-decide-1B \
  --model-dir /absolute/output/model --rows /absolute/validation.jsonl \
  --native-report /absolute/native.json --output /absolute/upstream.json
```

This command verifies the untouched upstream tree, the original source pin,
the export digest, and every tensor. Changes outside the six declared tensors
fail. It compares every decoded span and confidence (absolute tolerance 0.0005),
and exact input IDs when present in the Native report. Relation pair confidence
is the weaker of upstream's two role confidences. Every failure is retained.
Full run reports belong outside Git, per the GLiNER fixture policy.

## Measured experiments (2026-10-17)

`prepare_span_probe.py` reuses Antenna's pinned CrossNER AI loader and adds gold
FewRel relation endpoints at commit
`278a2315d2138810a379cd8d5718914dc56e2582`. It verifies three source file hashes,
uses seed 20260925, excludes ambiguous, over-width, duplicate, and oversized
records, and records every exclusion. FewRel training and evaluation relation
types are disjoint. No generated labels are used.

A first NER-only experiment trained on 99 CrossNER AI sentences and evaluated
32 content-disjoint validation sentences (147 gold entities). Native and Metal
both produced 56 true positives, 121 false positives, and 91 false negatives:
precision 0.3164, recall 0.3810, F1 **0.3457**. All 177 decoded spans matched
between backends; maximum confidence difference was 0.00003464. Untouched
upstream Python matched all Native decoded spans/confidences. Every encoder and
classifier tensor was byte-identical to the source. Export SHA-256:
`4e3d9c4568b957380b2c3f40f7f33a7559352e6533eba7a1d025fa0b33dfcc0b`.
The original checkpoint scored zero F1 on the same sentences.

The mixed experiment used 163 training sentences (99 NER, 64 FewRel) and 64
validation sentences (32 each). Forty epochs at learning rate 0.00003 selected
epoch 39, optimizer step 12714, rather than the worse final epoch 40. Export
SHA-256: `6f8c87a2f34022905bfa7d8e96a6e15e013ecda4679d0b8911d34310704af646`.
Native decoded NER F1 was **0.2681** (37/92/110 TP/FP/FN); relation F1 was
**0.0417** (1/15/31). This is a useful negative result: repairing the last
projection alone does not establish a general extraction checkpoint. Neither
experiment is a production release or grounds to close #1083. Further shared
encoder/head training belongs in Antenna, followed by independent application
holdouts and the existing serving qualification gates.

All 64 mixed validation records passed independent untouched-upstream parity,
including literal input IDs, entity/relation spans, and confidences. Native and
physical Metal matched all 145 predictions and input IDs, with maximum
confidence difference 0.00006098. All encoder and classifier tensors remained
byte-identical. The never-trained-on 22-sentence Codex concept holdout scored
F1 **0.0923** (3/7/52 TP/FP/FN). Runtime parity passes; application quality fails.
