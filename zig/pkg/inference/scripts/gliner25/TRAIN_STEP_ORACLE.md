# Tiny mixed-task training-step oracle

The tiny source capture is complete for full-parameter, head-only, LoRA and
DoRA profiles. All four native CPU consumers passed exact preprocessing and
mention targets, every scalar loss, every trainable gradient's presence/value,
and both AdamW flushes,
including the next microbatch after native weight updates. This fixture does
not qualify a production training run. The capture targets upstream commit
`3c913c7369301133d3b7699252074c4303ada50e` and the already pinned
Torch/Transformers/PEFT runtime. It needs no downloaded model.

[`capture_training_step.py`](capture_training_step.py) writes
[`training_step/capture.json`](../../testdata/gliner25/training_step/capture.json),
the typed tensor file, and the exact tiny tokenizer artifacts. The tensor file
is 1,228,022 bytes; total fixture size stays below 2 MiB. Each profile contains
three microbatches, two exact source AdamW flushes, all parameter gradients with
`None` preserved, and a byte-exact in-memory mid-window resume. Source full,
head-only, LoRA and DoRA trainable tensor counts are 168, 130, 24 and 36.
All four native profiles now also pass durable fresh-controller mid-window
restore: weights, pending accumulators, moments, gradient presence and
counters restore byte-exactly, then both source-matched optimizer updates
complete. The separate [controlled-dropout composition](TRAIN_STEP_DROPOUT_ORACLE.md)
also passes all four CPU profiles, including durable resume and both updates.
All four zero-dropout and all four controlled-dropout Metal consumers now pass
component losses and gradient absence/zero/value at unchanged tolerances.

Each sample carries an ordered `schema_json` string and canonical original
UTF-8 annotations. Native tests replace the placeholder annotation fingerprint
with the actual compiled schema fingerprint, prepare tokens, and compile
targets independently. The exact 52 fragment tokenizations are provided for
a strict fixture tokenizer; unknown fragments must fail. This does not assert
general native WordLevel support. The legacy invoice annotation supplies its
field mention; only the four explicit natural/latent/anchorless records enter
the record-object matching objective.

```sh
PYTHONDONTWRITEBYTECODE=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/capture_training_step.py --output-dir /private/tmp/gliner25-training-step-new
```

The fixture and generator are enrolled in `reference_manifest.json`. The
following sections record the composition and its limits. GPU consumers bind
captured initial/post-update weights independently for each microbatch. They
prove composed losses and gradients; GPU updates/resume have separate tiny
Controller and managed full/head-only evidence.

The actual GPU checkpoint `/private/tmp/gliner25-managed-metal-integration-v3.log`
exited successfully with 15 selected tests passing, no skips and no leaks;
SHA-256 `aa17d7b1439b18324f5caafb5aaf5e2c40492f6b8395aad153d6e94c5fcb39b2`.
It includes all eight mixed-step profiles. Selected relation pairs and labels
are compared through an exhaustive identity bijection, including all padding;
controlled relation-dropout masks follow that bijection. During controlled-mask
replay, each backend must preserve its exact ordered decisions; cross-backend
near-tie ranking order
may differ. Production ranking, fixture bytes and numeric tolerances are
unchanged. The [mask/pair contract](TRAIN_STEP_DROPOUT_ORACLE.md) records the
specific near ties. Published-model jobs and real-model convergence remain
separate gates.

## Smallest useful composition

Construct `BoundaryExtractor` with an explicit random `DebertaV2Config` and
the pinned upstream `tests/fixtures/tiny_tokenizer.py` word-level tokenizer.
The existing `oracle.build_tiny_model` uses BERT, so a new generator must use
DeBERTa directly to exercise the native encoder that production uses. A supplied
encoder config follows [`AutoModel.from_config`](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/models/base.py#L151);
no network or pretrained load is necessary. Start with encoder hidden width 16,
two layers, four heads, intermediate width 32, relative attention, and boundary,
pair and record widths 8. Keep the complete config in the fixture and bound the
result to two examples, at most 128 encoded tokens, 32 words, 32 queries, 16
retained spans and 256 relation pairs per type. Raise if a declared bound is
exceeded; do not shorten the examples to hide a failed capture.

Use two ragged examples with complete fixed schemas. The first combines entity
positives/negatives, attribute labels, single/multilabel classification, one
directed relation, a legacy structure, and natural/latent/anchorless records,
including a list field and an enum prefix value. The second supplies empty
extraction supervision and a negative classification label. Reuse canonical
annotations from the qualified target fixtures where possible. Upstream has
no direct attribute training API: explicitly lower attribute supervision into
hidden entity queries and record that mapping. JointIE uses entity/relation
supervision; do not invent a differentiable constraint objective.

Call `collate_fn_inference(..., architecture="boundary", build_targets=True,
max_len=None, error_policy="raise", on_capacity_exceeded="raise")`, then
`model.train()` for the actual forward. This preserves fixed schema order and
complete supervision while using the upstream training losses. Normal training
collation otherwise samples/removes labels, fields and relations, and shuffles
task order even when individual shuffle settings are disabled.

Capture [`model.forward`](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/models/boundary/model.py#L1744)
once per microbatch: actual encoder inputs/routes, all component losses,
retained spans, query/negative masks, proposal logits, relation pairs/labels,
dense record membership and matching decisions, total loss and every parameter
gradient. Shared-pool selection is detached, but its retained compatibility and
proposal scores must be recomputed from live projections. Relation selection
must read that same live shared-pool forward; record loss must use
`forward_groups_dense` and the global batch denominators. No encoder or dropout
replay is allowed between these decisions.

## Determinism and supervision checks

The first integration capture should use zero dropout, injection probability
one, explicit uniform draws for negative-query selection, fixed consistency
and soft-IoU schedule values, and the declared source loss weights. For the
small random model, an explicit synthetic relation threshold of zero and a
complete bounded Cartesian pair budget can ensure positive-edge coverage.
This is a documented fixture setting, not a replacement for the published
training defaults. A second case adds the existing explicit inverted-mask
dropout protocol at every encoder, boundary, classifier and relation use site.

The source contains fallback behavior that must never make an apparently
successful integration capture lose supervision:

- `_finite_loss_term` replaces nonfinite scalar losses with zero. Observe its
  input and fail the capture before this replacement can hide an error.
- Classification shape disagreement is silently skipped. Check exact routed
  label counts, shapes and classifier invocations, not just a scalar loss key.
- Record and relation exceptions can drop auxiliary losses; record capacity
  failure can return zeros. Reject warning/drop events and verify every gold
  record has one active match with all distinct field values represented.
- Relation proposals are thresholded and capped; the “gold-inclusive”
  docstring does not inject missing gold edges. Assert every gold edge is
  present in the retained pair mask, with positive and negative supervision.
- Require nonzero finite gradient contribution from each intended learned
  task module. A differentiable zero that merely touches a module is not proof
  of that task's supervision.

## Optimizer and resume identity

Preserve gradient `None` separately from an explicit zero. The shared-pool
forward leaves legacy explicit proposer/reranker parameters unused, whereas
`_head_touch` explicitly supplies zero gradients to optional record/relation
heads. AdamW skips state creation and weight decay for `None`; zero gradients
still participate. Record initial weights, gradient-presence masks, unscaled
and clipped gradients, parameter groups, moments and per-parameter step counts.

The exact [`_create_optimizer`](https://github.com/fastino-ai/GLiNER2/blob/3c913c7369301133d3b7699252074c4303ada50e/gliner2/training/trainer.py#L1342)
routes every name containing `encoder` to `encoder_lr`, including
`boundary_head.boundary_encoder.*` and `boundary_head.candidate_encoder.*`.
Head-only freezing excludes only the top-level `model.encoder` subtree; it does
not change this learning-rate rule. All source groups retain the configured
weight decay, including biases and normalization tensors. With `use_lora`, all
trainable parameters use `task_lr`, including DoRA magnitude and any separately
trainable head. `training_optimizer_groups.json` captures the four native CPU
profiles (full, head-only, LoRA and DoRA) through this exact AST method with a
recording AdamW constructor and no Torch import.
`training_adamw.json` separately captures actual Torch updates and partial
accumulation.

The scalar head fixtures store shared cotangents once; an explicit case replaces
the entire seed set, including zeros. Optimizer-group cases append their adapter
names to one shared ordered base-name list. These storage changes preserve every
case, expected gradient and optimizer-group result.

Run fresh full-parameter and head-only instances first. Then compose the pinned
LoRA and DoRA profile, preserving actual adapter names, scale, per-use dropout,
detached DoRA norm, trainable magnitude and frozen base identity. At least two
updates should cover a complete accumulation window and a partial final window.
Use a fixed optimizer/scheduler profile; save before/after weights, moments,
step counters and the exact microbatch order. Compare uninterrupted execution
with save/reload/resume using the same masks and decisions, and independently
assert frozen and unused weights remain unchanged. Optimizer group membership
must come from the source contract rather than guessed module categories.

Native failures, cancellation, capacity overflow or missing supervision must
leave the parameter epoch and optimizer state unchanged. Numerical parity of
this synthetic composition establishes only its bounded step contract.
Published-model convergence, all three variants and profiles, managed GPU
adapter updates, production data, actual adapter export/reload and release
gates remain separate evidence.
