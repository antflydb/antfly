# GLiNER2 LoRA Finetuning Readiness

This note records the current production gates for the GLiNER2 full-autodiff
LoRA finetuning path. It should be updated whenever a gate is added, removed,
or reclassified.

## Current Status

Status: not production-ready yet, but the repeatable local smoke/readiness
gate now passes on this machine.

The current blocker is no longer basic model loading, adapter artifact
emission, PEFT config metadata validation, the zero-row token-loss
denominator, classifier-head writeback, empty-supervision smoke validation,
artifact reload/materialization, profiler visibility, or the broad-suite Metal
active-frame crash, decoded entity-level inference from real-model token
logits before and after a smoke training run, saved PEFT/task-head reload for
that smoke, or a 100-example/500-step non-toy MLX-backed training/validation
run. The remaining work is production fidelity: semantic trained-adapter
entity/span quality goldens, stable accelerated-run launch ergonomics, agreed
backend performance thresholds, and promoting the new graph-native
`span-start` objective from smoke-verified implementation to non-toy,
semantically accepted production path.

## Known Local Artifacts

- Base model: `/private/tmp/termite-models/gliner2`
- Bootstrapped HF-style LoRA bundle: `/private/tmp/termite-models/gliner2-lora-bootstrap`
- Span-start one-step MLX smoke:
  `/private/tmp/termite-gliner2-span-start-smoke`
- Hugging Face repo: `fastino/gliner2-base-v1`
- Model commit observed during local download: `f5b2ecedebe4381b088c1cf276f5bf72a52cac54`
- `model.safetensors` SHA256 observed locally:
  `845fc4bd93c525b86124c58ab4f56c9eacf8587953086b14c501fab25957c007`

The full-autodiff training command writes both:

- legacy Termite trainer adapter parameter files (`*.bin`)
- HF-style PEFT files: `adapter_model.safetensors` and `adapter_config.json`
- regular trainable task-head weights: `task_head.safetensors`

`run-gliner2-autodiff-smoke-workflow` validates the run artifacts and inspects
the exported PEFT bundle against the base model.
`eval-gliner2-autodiff-adapter` reloads a saved autodiff PEFT adapter plus
`task_head.safetensors`, runs a fixed-text decoded entity pass, and can enforce
expected top entity text, label, and minimum score for semantic golden gates.

## Fast Gate

```sh
scripts/verify_gliner2_autodiff_readiness.sh
zig build test
zig build test-finetune
zig build test-gliner2-e2e
zig build test-gliner2-data
zig build test-gliner2-run-validation
zig build test-gliner2-cleanup-bundle
```

`scripts/verify_gliner2_autodiff_readiness.sh` uses writable Zig caches under
`/private/tmp` by default and runs the focused GLiNER2 gates, `test-finetune`,
dataset readiness smoke/non-toy rejection, class-capacity rejection, one-step
real-model smoke workflow, artifact validation, opt-in real-model training
test, and the broad `zig build test` gate. Set
`TERMITE_GLINER2_READINESS_RUN_BROAD=0` to skip the broad suite when debugging
the GLiNER2-only portion. The script also enforces a smoke throughput floor via
`TERMITE_GLINER2_READINESS_MIN_SUPERVISED_TPS`, defaulting to `0.05`
supervised tokens/sec; this is a regression floor, not a production throughput
target. It also enforces conservative smoke performance ceilings via
`TERMITE_GLINER2_READINESS_MAX_AVG_STEP_WALL_MS`, defaulting to `300000`,
`TERMITE_GLINER2_READINESS_MAX_TOTAL_EXECUTE_MS`, defaulting to `300000`, and
`TERMITE_GLINER2_READINESS_MAX_PEAK_RESIDENT_BYTES`, defaulting to
`1200000000`. These are local regression ceilings for the current native smoke,
not final production service-level objectives. It also enforces manifest
cardinality via
`TERMITE_GLINER2_READINESS_MIN_EXAMPLES` and
`TERMITE_GLINER2_READINESS_MIN_STEPS`, both defaulting to `1` for the local
smoke, plus `TERMITE_GLINER2_READINESS_MIN_ENTITY_LABELS`, defaulting to `1`.
It also enforces minimum supervision volume via
`TERMITE_GLINER2_READINESS_MIN_SUPERVISED_TOKENS` and
`TERMITE_GLINER2_READINESS_MIN_ENTITY_TOKENS`, both defaulting to `1`.

Note: sandboxed runs may block localhost bind in registry download tests. In
that case the broad suite can fail with `registry.download` ephemeral-port
errors even though the same `zig build test --summary failures` passes outside
the sandbox.

## Model-Backed Gate

Real model/data tests are opt-in:

```sh
TERMITE_GLINER2_REAL_MODEL_DIR=/private/tmp/termite-models/gliner2 \
TERMITE_GLINER2_REAL_NER_JSONL=/path/to/ner_train.jsonl \
zig build test-gliner2-real-training --summary all
```

This gate now uses the same HF GLiNER2 tokenizer/prompt target-construction
path as `train-gliner2-autodiff`, and enrolls `classifier.weight` /
`classifier.bias` as regular trainable task-head parameters. It also runs a
fixed-text token-logits inference pass against the real model before training.

For a direct training smoke:

```sh
zig build run-gliner2-autodiff-smoke-workflow -- \
  /private/tmp/termite-models/gliner2 \
  testdata/gliner2_ner_smoke.jsonl \
  /private/tmp/termite-gliner2-workflow-smoke \
  --epochs 1 --batch-size 1 --max-examples 1 --seq-len 64 --num-classes 4
```

For the stronger local loss-decrease smoke:

```sh
zig build run-gliner2-autodiff-smoke-workflow -- \
  /private/tmp/termite-models/gliner2 \
  testdata/gliner2_ner_smoke.jsonl \
  /private/tmp/termite-gliner2-workflow-loss-smoke \
  --epochs 2 --batch-size 1 --max-examples 1 --seq-len 64 --num-classes 4 \
  --require-loss-decrease
```

For dataset readiness:

```sh
zig build inspect-gliner2-dataset -- \
  /private/tmp/termite-models/gliner2 \
  /path/to/ner_train.jsonl \
  person,organization,location \
  - 256 8 4 false \
  --preset non-toy --fail-on-readiness
```

The `non-toy` preset currently requires at least 100 examples, 100 total
entities, 2 labels, 100 target entities, 95% target coverage, at least one
target entity in every sample, and at least 100 positive span labels. Custom
thresholds can be supplied with `--min-examples`, `--min-entities`,
`--min-labels`, `--min-target-entities`, `--min-target-coverage`,
`--require-all-examples-with-target`, `--min-positive-span-labels`, and
`--min-positive-rate`.

For the repeatable non-toy acceptance workflow:

```sh
scripts/run_gliner2_non_toy_acceptance.sh
```

By default this script prepares the bounded CoNLL-derived JSONL when needed,
runs the non-toy dataset readiness gate, and prints the full model-backed
training, validator, and semantic-reload commands without launching the long
native training run. Set `TERMITE_GLINER2_NON_TOY_RUN_TRAIN=1` to execute the
full acceptance workflow. Golden enforcement for the final semantic gate is
enabled by setting `TERMITE_GLINER2_NON_TOY_EXPECT_TEXT`,
`TERMITE_GLINER2_NON_TOY_EXPECT_LABEL`, and
`TERMITE_GLINER2_NON_TOY_MIN_SCORE` after selecting the golden from a completed
acceptance run.
Set `TERMITE_GLINER2_NON_TOY_ZIG_BUILD_FLAGS="-Dmlx=true"` to print or run the
same acceptance workflow with the MLX build path. MLX-enabled training now
chooses the backend at runtime: it uses MLX when a Metal device is visible,
falls back to native CPU/BLAS when no Metal device is available, and can be
forced onto native CPU/BLAS with `TERMITE_GLINER2_FORCE_NATIVE=1`.
In this Codex sandbox, freshly compiled MLX binaries from explicit
`ZIG_*_CACHE_DIR` locations reported no Metal device, while the repo default
`.zig-cache` build artifact did use MLX. For long acceptance runs, verify the
training log says `backend: MLX (Apple Silicon)` before letting the run
continue; otherwise the native fallback will take hours at production volume.

Observed on this machine:

- focused synthetic GLiNER2 e2e gate after masked-loss/head-trainable change:
  `zig build test-gliner2-e2e --summary failures` passes.
- focused synthetic GLiNER2 e2e gate after fixed-text token inference:
  `zig build test-gliner2-e2e --summary failures` passes. The gate now
  tokenizes the fixed text `google in london`, executes the full GLiNER2
  encoder graph through a token-logits output, and verifies deterministic
  per-token logits / predicted class ids with a known classifier head.
- focused synthetic GLiNER2 e2e gate after trainer task-head export/reload:
  `zig build test-gliner2-e2e --summary failures` passes. The fixed-text
  inference test now exports the trainer-owned `classifier.weight` /
  `classifier.bias` to `task_head.safetensors`, reloads the checkpoint, checks
  exact tensor equality, and verifies golden zero-hidden logits/predictions.
- focused run validator gate after task-head artifact validation:
  `zig build test-gliner2-run-validation --summary failures` passes.
- validator now rejects manifest/artifact mismatches: recorded legacy LoRA
  file count, PEFT tensor count, and task-head tensor count must match the
  actual output directory.
- focused run validator gate after task-head shape validation:
  `zig build test-gliner2-run-validation --summary failures` passes with
  6 validator tests, including rejection of a task-head class-count mismatch.
- focused run validator gate after optional throughput threshold:
  `zig build test-gliner2-run-validation --summary failures` passes with
  7 validator tests, including rejection of a run below the requested
  supervised-token/sec floor.
- focused run validator gate after detailed trainer-profile enforcement:
  `zig build test-gliner2-run-validation --summary failures` passes. Step
  metrics must now include graph build, runtime-input binding, autodiff,
  execution, gradient extraction, optimizer update, trainer-total time, and
  peak resident bytes.
- focused run validator gate after manifest cardinality enforcement:
  `zig build test-gliner2-run-validation --summary failures` passes. The
  validator now requires manifest `epochs`, `batch_size`, `seq_len`,
  `example_count`, `total_steps`, and finite `final_avg_loss`; requires
  manifest epochs/total steps to match metric records; and rejects runs below
  requested `--min-examples` or `--min-steps` thresholds.
- focused run validator gate after manifest label-vocabulary enforcement:
  `zig build test-gliner2-run-validation --summary failures` passes. The
  training manifest now records deterministic sorted `entity_labels` and
  `entity_label_count`, and the validator rejects missing, empty, duplicate,
  over-capacity, mismatched, or below-threshold label vocabularies via
  `--min-entity-labels`.
- focused run validator gate after supervision-volume thresholds:
  `zig build test-gliner2-run-validation --summary failures` passes. The
  validator now rejects runs below requested `--min-supervised-tokens` or
  `--min-entity-tokens` thresholds, in addition to the existing nonzero
  supervision checks.
- focused run validator gate after strict PEFT config contract validation:
  `zig build test-gliner2-run-validation --summary failures` passes. The
  validator now requires `adapter_config.json` to match the training manifest
  for base model path, PEFT/task type, LoRA rank/alpha, target modules, and
  `use_dora=false`; the gate includes rejection of a target-module mismatch.
- focused run validator gate after native smoke performance ceilings:
  `zig build test-gliner2-run-validation --summary failures` passes with
  11 validator tests, including rejection when requested average step wall
  time, total execute time, or peak resident memory ceilings are exceeded.
- full finetune aggregate after masked-loss/head-trainable change:
  `zig build test-finetune --summary all` passes with 1389 passed and
  3 skipped tests.
- full finetune aggregate after task-head shape validation:
  `zig build test-finetune --summary all` passes with 1390 passed and
  3 skipped tests.
- full finetune aggregate after materialized task-head attachment:
  `zig build test-finetune --summary all` passes with 1390 passed and
  3 skipped tests.
- full finetune aggregate after classifier-head reload/scoring gate:
  `zig build test-finetune --summary all` passes with 1391 passed and
  3 skipped tests.
- full finetune aggregate after optional throughput threshold:
  `zig build test-finetune --summary all` passes with 1392 passed and
  3 skipped tests.
- full finetune aggregate after fixed-text token inference:
  `zig build test-finetune --summary all` passes with 1393 passed and
  3 skipped tests.
- full finetune aggregate after opt-in real-model fixed-text inference:
  `zig build test-finetune --summary all` passes with 1393 passed and
  3 skipped tests.
- full finetune aggregate after deterministic label mapping and label/class
  capacity validation:
  `zig build test-finetune --summary all` passes with 1394 passed and
  3 skipped tests.
- full finetune aggregate after materialized task-head reload/scoring:
  `zig build test-finetune --summary failures` passes. The materialization
  test now reloads the task head from both adapter `task_head.safetensors` and
  materialized `model.safetensors`, checks exact tensor equality, and verifies
  matching golden classifier scores.
- full finetune aggregate after detailed trainer-profile metrics:
  `zig build test-finetune --summary failures` passes with warning-only output.
- readiness script without broad suite:
  `TERMITE_GLINER2_READINESS_RUN_BROAD=0 scripts/verify_gliner2_autodiff_readiness.sh`
  passes. It runs the focused GLiNER2 tests, `test-finetune`, smoke dataset
  readiness, expected non-toy rejection, expected class-capacity training
  rejection, one-step real-model smoke workflow, run validator, and opt-in
  real-model training gate.
- readiness script with broad suite after detailed trainer-profile metrics:
  `scripts/verify_gliner2_autodiff_readiness.sh` passes all GLiNER2-specific
  gates, dataset gates, class-capacity rejection, the one-step real-model
  smoke workflow, throughput validation, and the opt-in real-model training
  gate. Inside the sandbox, the final broad `zig build test --summary
  failures` reaches 1617 passed, 118 skipped, and 3 failed tests; all three
  failures are `registry.download` localhost ephemeral-port bind failures with
  errno 1.
- broad aggregate after detailed trainer-profile metrics:
  `zig build test --summary failures` passes outside the sandbox with
  warning-only output.
- broad aggregate after masked-loss/head-trainable change:
  `zig build test --summary failures` exits successfully with warning-only
  output.
- broad aggregate after task-head shape validation:
  `zig build test --summary failures` exits successfully with warning-only
  output.
- broad aggregate after materialized task-head attachment:
  `zig build test --summary failures` exits successfully with warning-only
  output.
- broad aggregate after classifier-head reload/scoring gate:
  `zig build test --summary failures` exits successfully with warning-only
  output.
- broad aggregate after optional throughput threshold:
  `zig build test --summary failures` exits successfully with warning-only
  output.
- broad aggregate after fixed-text token inference:
  `zig build test --summary failures` exits successfully with warning-only
  output.
- broad aggregate after opt-in real-model fixed-text inference:
  `zig build test --summary failures` exits successfully with warning-only
  output.
- broad aggregate after deterministic label mapping and label/class capacity
  validation initially failed outside the GLiNER2 path with a reproducible
  crash in `backends.metal_runtime.test.metal native decoder runtime active
  frame keeps common op params stable`.
- broad aggregate after fixing the Metal active-frame test setup:
  `zig build -Druntime-test-filter=true test -- "metal native decoder runtime
  active frame keeps common op params stable"` passes with 1 selected test.
- broad aggregate after fixing the Metal active-frame test setup:
  `zig build test --summary failures` passes outside the sandbox with
  warning-only output. A sandboxed run using `/private/tmp` Zig caches reached
  1617 passed, 118 skipped, and 3 failed tests; the failures were all
  `registry.download` tests failing to bind localhost ephemeral ports with
  errno 1 under sandbox restrictions.
- one-step native full-model smoke after classifier-head trainability:
  `/private/tmp/termite-gliner2-head-trainable-smoke` passes; emits 48 legacy
  LoRA parameter files, 48 PEFT adapter tensors, and 2 task-head tensors.
- validator summary for that smoke: 1 step record, 1 epoch record,
  `first_step_loss=1.4243357181549072`, all step losses finite, manifest
  artifact counts match actual artifacts.
- one-step native full-model smoke after supervision-stat validation:
  `/private/tmp/termite-gliner2-target-stats-smoke` passes; emits 48 legacy
  LoRA parameter files, 48 PEFT adapter tensors, and 2 task-head tensors.
- validator summary for that smoke: 1 step record, 1 epoch record,
  `supervised_token_count=15`, `entity_token_count=4`,
  `ignored_token_count=49`, `first_step_loss=1.4243357181549072`, all step
  losses finite, manifest artifact counts match actual artifacts.
- one-step native full-model smoke after performance-metric validation:
  `/private/tmp/termite-gliner2-perf-smoke` passes; emits 48 legacy LoRA
  parameter files, 48 PEFT adapter tensors, and 2 task-head tensors.
- validator summary for that smoke: `total_step_wall_ms=165689.732`,
  `avg_step_wall_ms=165689.732`,
  `supervised_tokens_per_second=0.09053065521284083`, 1 step record,
  1 epoch record, all step losses finite, manifest artifact counts match
  actual artifacts.
- raw timing for that smoke: `target_build_ms=3.7`,
  `train_step_ms=165686.032`, `step_wall_ms=165689.732`.
- one-step native full-model smoke after task-head shape validation:
  `/private/tmp/termite-gliner2-task-head-shape-smoke` passes; emits 48
  legacy LoRA parameter files, 48 PEFT adapter tensors, and 2 task-head
  tensors.
- validator summary for that smoke: `task_head_num_classes=4`,
  `task_head_hidden_size=768`, `supervised_token_count=15`,
  `entity_token_count=4`, `ignored_token_count=49`,
  `total_step_wall_ms=164191.54`,
  `supervised_tokens_per_second=0.09135671667370925`,
  `first_step_loss=1.4243357181549072`, all step losses finite, manifest
  artifact counts match actual artifacts.
- one-step native full-model smoke after deterministic label mapping and
  label/class capacity validation:
  `/private/tmp/termite-gliner2-label-capacity-smoke` passes; emits 48 legacy
  LoRA parameter files, 48 PEFT adapter tensors, and 2 task-head tensors.
- validator summary for that smoke: `task_head_num_classes=4`,
  `task_head_hidden_size=768`, `supervised_token_count=15`,
  `entity_token_count=4`, `ignored_token_count=49`,
  `total_step_wall_ms=164122.204`,
  `supervised_tokens_per_second=0.09139531175196745`,
  `first_step_loss=1.427620530128479`, all step losses finite, manifest
  artifact counts match actual artifacts.
- one-step native full-model smoke through the readiness script:
  `/private/tmp/termite-gliner2-readiness-smoke` passes; emits 48 legacy LoRA
  parameter files, 48 PEFT adapter tensors, and 2 task-head tensors.
- validator summary for that readiness smoke: `task_head_num_classes=4`,
  `task_head_hidden_size=768`, `supervised_token_count=15`,
  `entity_token_count=4`, `ignored_token_count=49`,
  `total_step_wall_ms=172370.459`,
  `supervised_tokens_per_second=0.08702187188583166`,
  `total_graph_build_ms=58.802`, `total_runtime_input_ms=6.043`,
  `total_autodiff_ms=33.219`, `total_execute_ms=172227.338`,
  `total_extract_ms=16.988`, `total_optimizer_update_ms=10.206`,
  `max_peak_resident_bytes=790085632`,
  `first_step_loss=1.427620530128479`, all step losses finite, manifest
  artifact counts match actual artifacts.
- throughput regression floor for the readiness smoke:
  `zig build validate-gliner2-autodiff-run --
  /private/tmp/termite-gliner2-readiness-smoke
  --min-supervised-tokens-per-second 0.05` passes with observed
  `supervised_tokens_per_second=0.08702187188583166`.
- one-step native full-model smoke after detailed trainer-profile metrics:
  `/private/tmp/termite-gliner2-profile-smoke` passes; emits 48 legacy LoRA
  parameter files, 48 PEFT adapter tensors, and 2 task-head tensors.
- validator summary for that profile smoke:
  `task_head_num_classes=4`, `task_head_hidden_size=768`,
  `supervised_token_count=15`, `entity_token_count=4`,
  `ignored_token_count=49`, `total_step_wall_ms=165600.347`,
  `supervised_tokens_per_second=0.09057952034363793`,
  `total_graph_build_ms=57.027`, `total_runtime_input_ms=6.065`,
  `total_autodiff_ms=33.07`, `total_execute_ms=165457.875`,
  `total_extract_ms=17.848`, `total_optimizer_update_ms=10.334`,
  `max_peak_resident_bytes=790085632`, `first_step_loss=1.427620530128479`,
  all step losses finite, manifest artifact counts match actual artifacts.
- throughput regression floor for that profile smoke:
  `zig build validate-gliner2-autodiff-run --
  /private/tmp/termite-gliner2-profile-smoke
  --min-supervised-tokens-per-second 0.05` passes with observed
  `supervised_tokens_per_second=0.09057952034363793`.
- manifest cardinality regression floor for the readiness smoke:
  `zig build validate-gliner2-autodiff-run --
  /private/tmp/termite-gliner2-readiness-smoke
  --min-supervised-tokens-per-second 0.05 --min-examples 1 --min-steps 1`
  passes with `manifest_example_count=1`, `manifest_total_steps=1`,
  `manifest_epochs=1`, `manifest_batch_size=1`, and `manifest_seq_len=64`.
- production-style cardinality rejection for the smoke fixture:
  `zig build validate-gliner2-autodiff-run --
  /private/tmp/termite-gliner2-readiness-smoke --min-examples 100` fails with
  `ExampleCountBelowThreshold`, proving the smoke artifact cannot satisfy the
  non-toy production gate by accident.
- one-step native full-model smoke after manifest label-vocabulary recording:
  `/private/tmp/termite-gliner2-label-manifest-smoke` passes; emits 48 legacy
  LoRA parameter files, 48 PEFT adapter tensors, and 2 task-head tensors. Its
  manifest records sorted `entity_labels=["location","organization","person"]`
  and `entity_label_count=3`.
- validator summary for that label-manifest smoke:
  `manifest_entity_label_count=3`, `manifest_example_count=1`,
  `manifest_total_steps=1`, `task_head_num_classes=4`,
  `task_head_hidden_size=768`, `supervised_token_count=15`,
  `entity_token_count=4`, `ignored_token_count=49`,
  `total_step_wall_ms=166852.074`,
  `supervised_tokens_per_second=0.08989999129408485`,
  `total_graph_build_ms=56.45`, `total_runtime_input_ms=6.076`,
  `total_autodiff_ms=32.696`, `total_execute_ms=166711.38`,
  `total_extract_ms=16.86`, `total_optimizer_update_ms=10.304`,
  `max_peak_resident_bytes=790233088`, `first_step_loss=1.427620530128479`,
  all step losses finite, manifest artifact counts match actual artifacts.
- label-vocabulary regression floor for that smoke:
  `zig build validate-gliner2-autodiff-run --
  /private/tmp/termite-gliner2-label-manifest-smoke
  --min-supervised-tokens-per-second 0.05 --min-examples 1 --min-steps 1
  --min-entity-labels 3` passes.
- supervision-volume regression floor for that smoke:
  `zig build validate-gliner2-autodiff-run --
  /private/tmp/termite-gliner2-label-manifest-smoke
  --min-supervised-tokens-per-second 0.05 --min-examples 1 --min-steps 1
  --min-entity-labels 3 --min-supervised-tokens 15 --min-entity-tokens 4`
  passes.
- strict PEFT config contract validation for that smoke:
  `zig build validate-gliner2-autodiff-run --
  /private/tmp/termite-gliner2-label-manifest-smoke
  --min-supervised-tokens-per-second 0.05 --min-examples 1 --min-steps 1
  --min-entity-labels 3 --min-supervised-tokens 15 --min-entity-tokens 4`
  passes. The accepted `adapter_config.json` matches manifest
  `model_dir=/private/tmp/termite-models/gliner2`, `lora_rank=16`,
  `lora_alpha=32`, `lora_targets=query_proj,value_proj`, and
  `use_dora=false`.
- production-style label-count rejection for that smoke:
  `zig build validate-gliner2-autodiff-run --
  /private/tmp/termite-gliner2-label-manifest-smoke --min-entity-labels 4`
  fails with `EntityLabelCountBelowThreshold`.
- production-style supervision-volume rejection for that smoke:
  `zig build validate-gliner2-autodiff-run --
  /private/tmp/termite-gliner2-label-manifest-smoke --min-supervised-tokens
  16` fails with `SupervisedTokenCountBelowThreshold`, and
  `--min-entity-tokens 5` fails with `EntityTokenCountBelowThreshold`.
- dataset readiness smoke:
  `zig build inspect-gliner2-dataset -- /private/tmp/termite-models/gliner2
  testdata/gliner2_ner_smoke.jsonl person,organization,location - 256 8 4
  false --preset smoke --fail-on-readiness` passes with
  `target_coverage_ratio=1.0`, 3 examples, 9 target entities, and
  8 positive span labels.
- label/class capacity gate:
  `zig build test-gliner2-data --summary failures` passes after adding
  coverage that the three-label smoke fixture fits `num_classes=4`, fails at
  `num_classes=3`, and rejects `num_classes=1`.
- span prediction decoder gate:
  `zig build test-gliner2-data --summary failures` passes after adding a
  deterministic decoder from `[batch, max_spans, num_entity_types]` score
  grids back to `(sample, span, word_start, word_end, label, score)`
  predictions. This gives the future model-backed entity/span golden a stable
  postprocessing contract.
- entity prediction decoder gate:
  `zig build test-gliner2-data --summary failures` passes after extending
  postprocessing to decode score grids back to entity text, character offsets,
  labels, word spans, and scores. The deterministic fixture asserts `john`
  as `person` at `[0,4)` and `acme` as `organization` at `[14,18)`.
- token-logit-to-span-score bridge gate:
  `zig build test-gliner2-data --summary failures` passes after adding a
  softmax bridge from `[batch * seq_len, num_classes]` token logits into
  production-shaped `[batch, max_spans, num_entity_types]` span scores. The
  deterministic fixture decodes `john` as `person` and `acme` as
  `organization` from token logits rather than a prefilled span-label grid.
- full finetune aggregate after entity prediction decoder:
  `zig build test-finetune --summary failures` passes with warning-only
  output.
- full finetune aggregate after validator manifest cardinality gates:
  `zig build test-finetune --summary failures` passes with warning-only
  output.
- full finetune aggregate after manifest label-vocabulary gates:
  `zig build test-finetune --summary failures` passes with warning-only
  output.
- full finetune aggregate after supervision-volume validator gates:
  `zig build test-finetune --summary failures` passes with warning-only
  output.
- full finetune aggregate after strict PEFT config contract validation:
  `zig build test-finetune --summary failures` passes with warning-only
  output.
- full finetune aggregate after token-logit-to-span-score bridge:
  `zig build test-finetune --summary failures` passes with warning-only
  output.
- direct training capacity failure:
  `zig build train-gliner2-autodiff -- --model-dir
  /private/tmp/termite-models/gliner2 --train-data
  testdata/gliner2_ner_smoke.jsonl --out-dir
  /private/tmp/termite-gliner2-label-capacity-fail --epochs 1 --batch-size 1
  --max-examples 1 --seq-len 64 --num-classes 3` fails before training with
  `TooManyEntityTypes` and reports that 3 entity labels cannot fit into
  2 entity slots.
- dataset non-toy gate correctly rejects the smoke fixture:
  `--preset non-toy --fail-on-readiness` fails with reasons `min_examples`,
  `min_total_entities`, `min_target_entities`, and
  `min_positive_span_labels`.
- no non-toy NER training JSONL is currently present in the repo testdata or
  known local `/private/tmp` GLiNER2 paths. The implemented non-toy gate still
  needs a real dataset path before it can provide production evidence.
- opt-in real-model training gate with the downloaded model and smoke JSONL
  passes:

```sh
TERMITE_GLINER2_REAL_MODEL_DIR=/private/tmp/termite-models/gliner2 \
TERMITE_GLINER2_REAL_NER_JSONL=testdata/gliner2_ner_smoke.jsonl \
zig build test-gliner2-real-training --summary failures
```

  It uses the HF tokenizer/prompt path, proves non-empty supervision
  (`supervised_token_count=15`, `entity_token_count=4`,
  `ignored_token_count=49`), and decreases loss from `1.715445` to `1.503011`
  over 2 steps while updating both LoRA B weights and classifier bias.
- opt-in real-model fixed-text decoded inference in that same gate passes
  before and after training: `Alice joined Acme in Paris` is tokenized with
  the HF GLiNER2 tokenizer, produces finite repeatable token logits with
  `rows=64` and `classes=5`, converts them into span scores, and
  deterministically decodes the pre-training top entity as `text='in'`,
  `label='person'`, `score=0.251425`. After the 2-step smoke training run,
  the same decoded inference path runs through the updated in-memory
  LoRA/task-head state and decodes `text='joined'`,
  `label='organization'`, `score=0.190345`. This proves real-model forward
  output can flow through the entity decoder before and after training.
- saved PEFT/task-head reload in that same gate passes: after the 2-step
  smoke training run, the test exports `adapter_model.safetensors`,
  `adapter_config.json`, and `task_head.safetensors`, seeds a fresh trainer
  from those saved artifacts, and decodes the same fixed text. The reloaded
  trainer reproduces the in-memory trained top entity exactly:
  `text='joined'`, `label='organization'`, `score=0.190345`. This proves the
  saved adapter/task-head artifacts can restore the trained smoke state; it is
  still not a semantic quality golden for a non-toy trained adapter.
- synthetic GLiNER2 e2e now asserts `classifier.weight` / `classifier.bias`
  are enrolled as regular trainable parameters and updated independently from
  the backend seed weights.
- two-step native full-model smoke: passes `--require-loss-decrease`.
- two-step loss: `0.3338286876678467 -> 0.3333209455013275`.
- exported PEFT bundle inspection: passes with 24 LoRA pairs and 589,824
  trainable parameters.
- materialized deploy checkpoint after task-head attachment:
  `zig build materialize-gliner2-lora -- /private/tmp/termite-models/gliner2
  /private/tmp/termite-gliner2-task-head-shape-smoke
  /private/tmp/termite-gliner2-task-head-materialized` passes with
  `merged_lora_tensor_count=24`, `attached_task_head_tensor_count=2`, and
  `copied_base_tensor_count=231`.
- materialized checkpoint inspection:
  `zig build inspect-gliner2-checkpoint --
  /private/tmp/termite-gliner2-task-head-materialized` passes with
  `base_tensor_count=257`, `hidden_size=768`, `num_hidden_layers=12`,
  `query_proj_weights_found=12`, `value_proj_weights_found=12`,
  `span_rep_tensors_found=12`, `count_embed_tensors_found=37`, and
  `core_backbone_loadable=true`.
- classifier task-head reload/scoring gate:
  `zig build test-gliner2-cleanup-bundle --summary failures` passes after
  adding a deterministic golden test that writes `task_head.safetensors`,
  reloads `classifier.weight` / `classifier.bias`, scores fixed hidden rows,
  and verifies logits plus predicted class ids.
- explicit native throughput-threshold validation:
  `zig build validate-gliner2-autodiff-run --
  /private/tmp/termite-gliner2-task-head-shape-smoke
  --min-supervised-tokens-per-second 0.05` passes with observed
  `supervised_tokens_per_second=0.09135671667370925`.
- explicit native performance-ceiling validation:
  `zig build validate-gliner2-autodiff-run --
  /private/tmp/termite-gliner2-label-manifest-smoke
  --min-supervised-tokens-per-second 0.05 --min-examples 1 --min-steps 1
  --min-entity-labels 3 --min-supervised-tokens 15 --min-entity-tokens 4
  --max-avg-step-wall-ms 300000 --max-total-execute-ms 300000
  --max-peak-resident-bytes 1200000000` passes with observed
  `avg_step_wall_ms=166852.074`, `total_execute_ms=166711.38`,
  `max_peak_resident_bytes=790233088`, and
  `supervised_tokens_per_second=0.08989999129408485`. Intentionally tighter
  local checks also fail with `AvgStepWallAboveThreshold`,
  `TotalExecuteMsAboveThreshold`, and `PeakResidentBytesAboveThreshold`.
- fast wrapper gate with broad suite disabled:
  `TERMITE_GLINER2_READINESS_RUN_BROAD=0
  scripts/verify_gliner2_autodiff_readiness.sh` completes. The fresh
  `/private/tmp/termite-gliner2-readiness-smoke` run passed the scripted
  performance gates with `avg_step_wall_ms=164448.443`,
  `total_execute_ms=164304.882`, `max_peak_resident_bytes=790102016`, and
  `supervised_tokens_per_second=0.09121399829854272`; the same wrapper also
  verified smoke dataset readiness, expected non-toy rejection, expected
  class-capacity failure, PEFT/task-head artifact validation, and the
  real-model decoded/reloaded training smoke.
- native full-model training is slow; a two-step smoke takes several minutes.

## Output Contract

A valid `train-gliner2-autodiff` output directory contains:

- `training_manifest.json`
- `training_metrics.jsonl`
- one or more saved LoRA parameter `*.bin` files
- `adapter_model.safetensors`
- `adapter_config.json`
- `task_head.safetensors` containing F32 `classifier.weight` and
  `classifier.bias`

Before training, `train-gliner2-autodiff` builds a sorted label vocabulary and
requires `num_classes >= unique_entity_labels + 1`, reserving class 0 for
`O`. It fails with `TooManyEntityTypes` instead of silently collapsing labels
when the classifier head has too few entity slots.

The validator fails on missing manifest, missing metrics, zero step records,
non-finite loss values, zero supervised tokens, zero entity-positive tokens,
missing/invalid per-step performance metrics, missing adapter parameter files,
missing/empty PEFT adapter files, missing task-head checkpoint/tensors,
manifest/metrics step or epoch mismatch, and optionally on non-decreasing loss
or requested minimum examples/steps. It also requires the manifest schema,
artifact family, artifact filenames, and recorded artifact counts to match the
actual run directory. The manifest now records `model_dir`, `lora_rank`,
`lora_alpha`, `lora_targets`, `num_classes`, `hidden_size`, `epochs`,
`batch_size`, `seq_len`, `example_count`, `total_steps`, and
`final_avg_loss`; the validator requires `adapter_config.json` to match those
LoRA fields, to declare `peft_type=LORA` and `task_type=TOKEN_CLS`, and to
set `use_dora=false`. The validator also requires `entity_labels` and
`entity_label_count` to describe a non-empty deterministic label vocabulary
that fits within `num_classes`; and it requires `task_head.safetensors` to
contain F32 `classifier.weight` with shape `[num_classes, hidden_size]` and
F32 `classifier.bias` with shape `[num_classes]`. The smoke workflow also
loads the PEFT bundle with `inspect-gliner2-lora-bundle`.

`materialize-gliner2-lora` now consumes the separate `task_head.safetensors`
when it is present and attaches those classifier tensors to the merged
`model.safetensors`. The focused GLiNER2 bundle test checks exact
materialized classifier-head shape and values, and the real smoke artifact has
been materialized locally with `attached_task_head_tensor_count=2`.

The reusable classifier task-head loader now reloads a safetensors checkpoint
and scores hidden-state rows with the trained `classifier.weight` /
`classifier.bias`. The golden test fixes the score contract below the eventual
text-level inference API: known weights plus known hidden rows must produce
stable logits and class predictions.

The full-autodiff GLiNER2 context now exposes a read-only token-logits
execution helper for already-built trainers. It reuses the production graph,
binds input ids, attention mask, LoRA parameters, regular classifier-head
parameters, and architecture-specific attention bias, then returns
`[batch * seq_len, num_classes]` logits. The synthetic e2e gate uses this path
for a fixed text/tokenization inference check, and the opt-in real-model gate
uses the same helper for a downloaded-checkpoint fixed-text inference pass.

The dataset/batch layer now exposes `decodeSpanPredictionsAlloc` and
`decodeEntityPredictionsAlloc`, deterministic decoders for production-shaped
`[batch, max_spans, num_entity_types]` score grids. The first returns word-span
predictions; the second maps them back to entity text and character offsets.
It also exposes `tokenLogitsToSpanScoresAlloc`, a softmax bridge from
`[batch * seq_len, num_classes]` token logits into that span-score grid using
class 0 as `O` and entity classes 1..N as `entity_types[0..]`. This closes the
decoded-token-classifier inference path and gives the real-model gate an
entity-level deterministic check. It still does not close the full GLiNER2
span/objective parity gap or replace the eventual semantic trained-adapter
entity/span golden.

The full-autodiff token loss treats an all-zero target row as ignored and
normalizes by summed target-row mass rather than total token slots. Focused
constant-folded graph tests cover both partial ignored rows and the all-ignored
zero-loss case.

`training_metrics.jsonl` step and epoch records now include
`supervised_token_count`, `entity_token_count`, `ignored_token_count`, and
`entity_token_rate`. The run validator requires the step records to prove the
training job contained non-empty supervision and at least one entity-positive
target row.

Step records also include `target_build_ms`, `train_step_ms`, `step_wall_ms`,
`graph_build_ms`, `runtime_input_ms`, `autodiff_ms`, `execute_ms`,
`extract_ms`, `optimizer_update_ms`, `trainer_total_ms`,
`peak_resident_bytes`, and `supervised_tokens_per_second`; epoch records
include `epoch_wall_ms` and `supervised_tokens_per_second`. The validator
requires finite positive step wall time, positive autodiff/execute/profiled
memory evidence, and positive throughput. It reports aggregate wall time,
supervised-token throughput, profile timing totals, and max resident bytes in
its summary. `validate-gliner2-autodiff-run` also accepts
`--min-supervised-tokens-per-second <f64>`,
`--max-avg-step-wall-ms <f64>`, `--max-total-execute-ms <f64>`,
`--max-peak-resident-bytes <n>`, `--min-examples <n>`, `--min-steps <n>`,
`--min-entity-labels <n>`, `--min-supervised-tokens <n>`, and
`--min-entity-tokens <n>` to turn reported throughput, latency, memory,
manifest cardinality, manifest label coverage, and supervision volume into
explicit pass/fail gates for backend-specific production runs.

## Smoke Dataset Contract

`testdata/gliner2_ner_smoke.jsonl` is the cheap local NER fixture. The fast
`test-gliner2-data` gate asserts:

- 3 examples
- 9 entities
- 3 labels: `location`, `organization`, `person`
- full target coverage for `person,organization,location`
- stable simple-batch shape at `max_length=256`, `max_span_width=8`

`inspect-gliner2-dataset` now reports a `readiness` block with dataset stats,
target coverage, encoded batch shape, `target_coverage_ratio`, pass/fail, and
named failure reasons. Readiness now counts `span_targets` across the filtered
dataset separately from the encoded preview batch, so non-toy positive-span
thresholds do not require an oversized preview batch. With
`--fail-on-readiness`, it exits non-zero when the dataset does not meet the
selected thresholds.

`scripts/convert_conll_ner_to_gliner2_jsonl.py` converts CoNLL-style token/BIO
NER files into the GLiNER2 finetune JSONL shape with text, entity labels, and
character offsets. A bounded 200-example slice from the public CoNLL-2003
English training mirror
`https://raw.githubusercontent.com/autoih/conll2003/master/CoNLL-2003/eng.train`
was converted locally at
`/private/tmp/gliner2-conll2003-train-200.jsonl`, mapping `PER`, `ORG`, and
`LOC` to `person`, `organization`, and `location` while dropping `MISC` for the
current 3-label classifier-head contract. The non-toy dataset readiness gate
passes for that file with:

```sh
zig build inspect-gliner2-dataset -- \
  /private/tmp/termite-models/gliner2 \
  /private/tmp/gliner2-conll2003-train-200.jsonl \
  person,organization,location \
  - 256 8 4 false \
  --preset non-toy --fail-on-readiness
```

Observed readiness summary: `num_examples=200`, `total_entities=415`,
`target_entities=415`, `unique_labels=3`, `samples_without_target=0`,
`target_coverage_ratio=1`, dataset-wide `span_targets.positive_labels=415`,
preview `batch_shape.positive_labels=4`, and `passed=true`.

A bounded CoNLL-backed real-model training pilot also completes:

```sh
zig build run-gliner2-autodiff-smoke-workflow -- \
  /private/tmp/termite-models/gliner2 \
  /private/tmp/gliner2-conll2003-train-200.jsonl \
  /private/tmp/termite-gliner2-conll-pilot \
  --epochs 1 --batch-size 1 --max-examples 2 --seq-len 64 --num-classes 4

zig build validate-gliner2-autodiff-run -- \
  /private/tmp/termite-gliner2-conll-pilot \
  --require-loss-decrease \
  --min-supervised-tokens-per-second 0.05 \
  --min-examples 2 --min-steps 2 --min-entity-labels 2 \
  --min-supervised-tokens 26 --min-entity-tokens 4 \
  --max-avg-step-wall-ms 300000 \
  --max-total-execute-ms 600000 \
  --max-peak-resident-bytes 1200000000
```

Observed pilot validation summary: `manifest_example_count=2`,
`manifest_total_steps=2`, `manifest_entity_label_count=2`,
`supervised_token_count=26`, `entity_token_count=4`,
`avg_step_wall_ms=165871.44900000002`, `total_execute_ms=331521.35`,
`max_peak_resident_bytes=807305216`,
`supervised_tokens_per_second=0.07837394607917123`,
`first_step_loss=1.411008358001709`,
`final_step_loss=1.4108624458312988`, and `loss_decreased=true`.

The saved pilot adapter also passes the new fixed-text semantic reload gate:

```sh
zig build eval-gliner2-autodiff-adapter -- \
  /private/tmp/termite-models/gliner2 \
  /private/tmp/termite-gliner2-conll-pilot \
  "Alice joined Acme in Paris" \
  --seq-len 64 \
  --expect-text Alice \
  --expect-label organization \
  --min-score 0.27
```

Observed top decoded entity: `text=Alice`, `label=organization`, `start=0`,
`end=5`, `score=0.2772621214389801`, `entity_types=["organization","person"]`,
`loaded_base_weight_count=255`. This proves the saved PEFT/task-head artifacts
can be reloaded through a production-facing semantic gate. It is not yet a
quality claim because the adapter was trained on only two CoNLL examples.

The non-toy acceptance wrapper dry-run passes its dataset gate:

```sh
TERMITE_GLINER2_NON_TOY_FORCE_CONVERT=0 \
  scripts/run_gliner2_non_toy_acceptance.sh
```

It reports the same CoNLL dataset readiness pass and prints the full opt-in
acceptance commands:

- `run-gliner2-autodiff-smoke-workflow` with `--epochs 5`,
  `--max-examples 100`, `--seq-len 128`, `--num-classes 4`,
  `--learning-rate 1e-3`, and `--require-loss-decrease`
- `validate-gliner2-autodiff-run` with `--min-examples 100`,
  `--min-steps 500`, `--min-supervised-tokens 10000`,
  `--min-entity-tokens 2000`, throughput/memory ceilings, and
  `--require-loss-decrease`
- `eval-gliner2-autodiff-adapter` for the final fixed-text semantic reload
  gate

The wrapper also prints MLX build commands when requested:

```sh
TERMITE_GLINER2_NON_TOY_FORCE_CONVERT=0 \
TERMITE_GLINER2_NON_TOY_ZIG_BUILD_FLAGS="-Dmlx=true" \
  scripts/run_gliner2_non_toy_acceptance.sh
```

After the text-only target-mask change, prompt/entity-label/separator tokens
are now ignored by the token-classifier fallback loss instead of being trained
as `O`. This reduced the 100-example/2-epoch run's supervised-token count from
`6600` to `5000` while preserving the same `992` entity-positive tokens. That
2-epoch run exported artifacts but did not pass strict first-step/final-step
loss decrease: `first_step_loss=1.4132721424102783`,
`final_step_loss=1.4202791452407837`, `loss_decreased=false`.

A stronger high-learning-rate non-toy MLX-backed functional run completed
through the direct workflow command using the repo default `.zig-cache`
artifact:

```sh
zig build -Dmlx=true run-gliner2-autodiff-smoke-workflow -- \
  /private/tmp/termite-models/gliner2 \
  /private/tmp/gliner2-conll2003-train-200.jsonl \
  /private/tmp/termite-gliner2-non-toy-mlx-textonly-lr1e3 \
  --epochs 5 --batch-size 1 --max-examples 100 --seq-len 128 \
  --num-classes 4 --learning-rate 1e-3
```

The training log reported `backend: MLX (Apple Silicon)`, ran 5 epochs x
100 steps, and exported reloadable PEFT/task-head artifacts. A stricter
non-toy validator pass:

```sh
zig build validate-gliner2-autodiff-run -- \
  /private/tmp/termite-gliner2-non-toy-mlx-textonly-lr1e3 \
  --require-loss-decrease \
  --min-supervised-tokens-per-second 1 \
  --min-examples 100 --min-steps 500 --min-entity-labels 3 \
  --min-supervised-tokens 10000 --min-entity-tokens 2000 \
  --max-avg-step-wall-ms 10000 \
  --max-total-execute-ms 3000000 \
  --max-peak-resident-bytes 2500000000
```

Observed full-run validation summary: `manifest_example_count=100`,
`manifest_total_steps=500`, `manifest_entity_label_count=3`,
`supervised_token_count=12500`, `entity_token_count=2480`,
`ignored_token_count=51500`, `avg_step_wall_ms=615.3912219999996`,
`total_execute_ms=242634.71199999982`,
`max_peak_resident_bytes=2231926784`,
`supervised_tokens_per_second=40.624563864838514`,
`first_step_loss=1.4132721424102783`,
`final_step_loss=0.6767985224723816`, and `loss_decreased=true`.
The observed run needs an MLX non-toy memory ceiling above about 2.24 GB;
`2500000000` is the current local pass threshold.

The saved high-LR full-run adapter reloads, but still does not pass a semantic
quality golden. Example probes:

```sh
zig build eval-gliner2-autodiff-adapter -- \
  /private/tmp/termite-models/gliner2 \
  /private/tmp/termite-gliner2-non-toy-mlx-textonly-lr1e3 \
  "Alice joined Acme in Paris" \
  --seq-len 64
```

Observed top decoded entity: `text=in`, `label=location`, `start=18`,
`end=20`, `score=0.31528809666633606`, `entity_types=["location",
"organization","person"]`, `loaded_base_weight_count=255`. Additional probes
from the same adapter decode `John works at Google in London` as
`text=works`, `label=location`, `score=0.29172348976135254`, and
`Peter Blackburn` as `text=Peter`, `label=location`,
`score=0.2938356101512909`. The higher learning rate proves the random task
head can train and validation loss can decrease, but semantic quality remains
blocked by the token-classifier-to-span bridge / missing GLiNER2 span-objective
parity.

This means the non-toy training/export/reload/performance surface now has real
evidence, but the current token-classifier-to-span bridge and/or objective
parity is not yet semantically production-ready.

An actual one-step MLX smoke now passes with:

```sh
zig build -Dmlx=true run-gliner2-autodiff-smoke-workflow -- \
  /private/tmp/termite-models/gliner2 \
  testdata/gliner2_ner_smoke.jsonl \
  /private/tmp/termite-gliner2-mlx-fallback-smoke \
  --epochs 1 --batch-size 1 --max-examples 1 --seq-len 64 --num-classes 4
```

Observed MLX smoke validation summary: `manifest_example_count=1`,
`manifest_total_steps=1`, `manifest_entity_label_count=3`,
`supervised_token_count=15`, `entity_token_count=4`,
`avg_step_wall_ms=1080.671`, `total_execute_ms=469.928`,
`max_peak_resident_bytes=856866816`,
`supervised_tokens_per_second=13.880265131571031`,
`first_step_loss=1.4276202917099`, and reloadable PEFT/task-head artifacts.
The same artifact passes explicit local accelerated smoke ceilings:
`--min-supervised-tokens-per-second 1`, `--max-avg-step-wall-ms 10000`,
`--max-total-execute-ms 10000`, and
`--max-peak-resident-bytes 1200000000`.

The deterministic native fallback path for MLX-enabled builds was also checked
without launching a long training step:

```sh
TERMITE_GLINER2_FORCE_NATIVE=1 \
zig build -Dmlx=true train-gliner2-autodiff -- \
  --model-dir /private/tmp/termite-models/gliner2 \
  --train-data testdata/gliner2_ner_smoke.jsonl \
  --out-dir /private/tmp/termite-gliner2-mlx-forced-native-capacity \
  --epochs 1 --batch-size 1 --max-examples 1 --seq-len 64 --num-classes 3
```

It prints `TERMITE_GLINER2_FORCE_NATIVE is set`, loads `255` weights through
the native path, reports `backend: native CPU/BLAS`, and then fails at the
expected `TooManyEntityTypes` class-capacity guard. This proves an MLX-enabled
binary can select the native fallback path before training.

## Span-Start Objective Smoke

The first graph-native span objective is implemented behind
`train-gliner2-autodiff --objective span-start`. It packs
`gliner2_data.EncodedBatch` span targets into the existing trainer `__targets`
tensor as labels, repeated valid-span masks, and flat start-token indices,
then gathers encoder hidden states at span starts and scores entity classes
directly. The token-classifier objective remains the default.

A one-example real-model MLX smoke passed on this machine:

```sh
zig build -Dmlx=true train-gliner2-autodiff -- \
  --model-dir /private/tmp/termite-models/gliner2 \
  --train-data testdata/gliner2_ner_smoke.jsonl \
  --out-dir /private/tmp/termite-gliner2-span-start-smoke \
  --epochs 1 --batch-size 1 --max-examples 1 --seq-len 64 \
  --num-classes 4 --objective span-start --max-span-width 4 \
  --learning-rate 1e-3
```

Observed run summary: backend `MLX (Apple Silicon)`, `loss=0.26174166798591614`,
`grad_norm=2.1199`, `supervised_token_count=54`,
`entity_token_count=3`, `ignored_token_count=570`,
`avg_step_wall_ms=687.825`, `total_execute_ms=545.016`,
`max_peak_resident_bytes=858865664`, and
`supervised_tokens_per_second=78.50834151128558`. The artifact validates with:

```sh
zig build validate-gliner2-autodiff-run -- \
  /private/tmp/termite-gliner2-span-start-smoke \
  --min-examples 1 --min-steps 1 --min-entity-labels 3 \
  --min-supervised-tokens 1 --min-entity-tokens 1 \
  --min-supervised-tokens-per-second 0.05 \
  --max-avg-step-wall-ms 300000 \
  --max-total-execute-ms 300000 \
  --max-peak-resident-bytes 2500000000
```

Supporting gates after this change: `zig build test-gliner2-e2e
test-gliner2-data test-gliner2-run-validation --summary failures`,
`zig build test-finetune --summary failures`,
`zig build train-gliner2-autodiff --summary failures -- --help`, and
`zig build test --summary failures` all exit `0` locally. The broad suites
still print existing warning-only messages around PJRT export size, Metal
fallback rejection, and `where_select` broadcast mismatch.

`eval-gliner2-autodiff-adapter` now reads the manifest objective and runs
span-start reload eval without the token-logit-to-span bridge. The span-start
artifact reload command:

```sh
zig build eval-gliner2-autodiff-adapter -- \
  /private/tmp/termite-models/gliner2 \
  /private/tmp/termite-gliner2-span-start-smoke \
  "John works at Acme in Paris" \
  --seq-len 64 --max-span-width 4
```

exits `0`, reports `objective="span-start"`, loads `255` base weights, and
returns a top entity from direct span logits. The current one-step smoke still
predicts the wrong semantic result: `text=in`, `label=person`, `start=19`,
`end=21`, `score=0.562548041343689`. This is expected for a one-step smoke
and proves the reload/eval plumbing, not production quality.

The same eval tool remains backward compatible with token-objective artifacts:

```sh
zig build eval-gliner2-autodiff-adapter -- \
  /private/tmp/termite-models/gliner2 \
  /private/tmp/termite-gliner2-non-toy-mlx-textonly-lr1e3 \
  "Alice joined Acme in Paris" \
  --seq-len 64
```

exits `0`, reports `objective="token"`, and reproduces the known token-bridge
semantic failure: `text=in`, `label=location`, `score=0.31528809666633606`.

## Remaining Production Work

- Replace the deterministic reload golden with a real semantic quality golden.
  The bounded CoNLL-derived dataset readiness gate passes, and a
  100-example/500-step MLX-backed acceptance run trains, exports artifacts, and
  validates with decreasing loss on the token objective. A one-step
  graph-native `span-start` objective smoke now trains, validates, reloads, and
  evaluates from direct span logits, but non-toy span-start training plus a
  span-score semantic reload golden has not passed yet, so this cannot be
  called production-ready.
- Close remaining GLiNER2 fidelity gaps around span/objective parity and
  DeBERTa relative-attention behavior. The full-autodiff token loss now
  excludes all-zero ignored target rows from both numerator and denominator,
  and CLI target construction now ignores prompt/entity-label/separator tokens
  rather than supervising them as `O`. CLI training and the opt-in real-model
  gate both use the HF tokenizer/prompt path and train `classifier.weight` /
  `classifier.bias` as regular task-head params saved to
  `task_head.safetensors`. The new `span-start` path is only the first
  graph-native span objective; it uses start-token hidden states rather than
  full GLiNER span representation parity.
- Decide final native / accelerated throughput, latency, and memory thresholds.
  Native smoke timing is emitted, validator-summarized, and gated by
  conservative local smoke floors/ceilings. The current full non-toy MLX run
  gives an accelerated baseline of about `40.62` supervised tokens/sec,
  `615.39 ms` average step wall time, and `2.232 GB` peak resident memory, but
  the final production pass/fail thresholds still need to be agreed.
- Stabilize MLX acceptance launch ergonomics. The direct default-cache command
  used MLX, but freshly compiled binaries from explicit sandbox cache
  directories reported no Metal device and fell back to native. Long
  production-volume runs must fail fast or warn loudly when `-Dmlx=true` falls
  back to native, rather than silently running for hours.

## Non-Toy Acceptance Gate

Before declaring this path production-ready, the following gate must pass
against a real NER training JSONL with a semantically correct fixed-text
golden. The current CoNLL-derived functional run passes training/export/reload
validation, but its semantic golden is intentionally not accepted because the
observed labels are wrong.

The canonical wrapper for the current CoNLL-derived non-toy dataset is:

```sh
TERMITE_GLINER2_NON_TOY_RUN_TRAIN=1 \
TERMITE_GLINER2_NON_TOY_EXPECT_TEXT=<expected-span> \
TERMITE_GLINER2_NON_TOY_EXPECT_LABEL=<expected-label> \
TERMITE_GLINER2_NON_TOY_MIN_SCORE=<agreed-score-floor> \
scripts/run_gliner2_non_toy_acceptance.sh
```

The expanded command sequence is:

```sh
zig build inspect-gliner2-dataset -- \
  /private/tmp/termite-models/gliner2 \
  /path/to/ner_train.jsonl \
  person,organization,location \
  - 256 8 4 false \
  --preset non-toy --fail-on-readiness

zig build run-gliner2-autodiff-smoke-workflow -- \
  /private/tmp/termite-models/gliner2 \
  /path/to/ner_train.jsonl \
  /private/tmp/termite-gliner2-non-toy-run \
  --epochs 5 --batch-size 1 --max-examples 100 --seq-len 128 \
  --num-classes 4 --learning-rate 1e-3 --require-loss-decrease

zig build validate-gliner2-autodiff-run -- \
  /private/tmp/termite-gliner2-non-toy-run \
  --min-supervised-tokens-per-second 1 \
  --min-examples 100 --min-steps 500 --min-entity-labels 3 \
  --min-supervised-tokens 10000 --min-entity-tokens 2000 \
  --max-avg-step-wall-ms 10000 \
  --max-total-execute-ms 3000000 \
  --max-peak-resident-bytes 2500000000 \
  --require-loss-decrease

zig build eval-gliner2-autodiff-adapter -- \
  /private/tmp/termite-models/gliner2 \
  /private/tmp/termite-gliner2-non-toy-run \
  "Alice joined Acme in Paris" \
  --seq-len 64 \
  --expect-text <expected-span> \
  --expect-label <expected-label> \
  --min-score <agreed-score-floor>
```

The minimum evidence for production readiness is: non-toy dataset readiness
passes; the run contains at least 100 examples, at least 2 entity labels, and
non-zero entity-positive supervision; the validator reports finite decreasing
loss, reloadable PEFT/task-head artifacts, positive profile timings, and a
backend-specific throughput above the agreed floor; a fixed-text entity/span
golden passes after loading the trained adapter plus task head.
