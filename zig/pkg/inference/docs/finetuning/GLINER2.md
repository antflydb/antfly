# Production GLiNER2 Finetuning with Zig and Metal

This is the operator and release contract for GLiNER2 finetuning in Antfly.
The production implementation is the Zig trainer, Zig graph runtime, and Zig
Metal kernels. A pinned upstream Python checkout is used only as a correctness
oracle and performance baseline; it is not a deployed dependency. There is no
Go implementation, Go runtime dependency, or Go parity target in this flow.

## Current status

The branch contains a complete full-task LoRA lifecycle:

- upstream `{input, output}` GLiNER2 JSONL preprocessing for entities,
  classifications, JSON structures, relations, and count supervision;
- DeBERTa encoder and all GLiNER2 task heads in the Zig autodiff graph;
- LoRA-only training on the native interpreter or the resident Metal runtime;
- bounded shape-specialized graph caching, gradient accumulation, clipping,
  AdamW, schedulers, checkpoint retention, exact resume, held-out loss,
  early stopping, and best-checkpoint selection;
- saved-adapter validation and native held-out full-task scoring;
- transactional merged-model materialization and reload inspection;
- deterministic Python/Zig correctness gates, native/Metal gradient gates,
  repeated production-shape performance gates, and a five-seed statistical
  convergence contract.

"Production ready" is an evidence result, not a static feature flag. The
strict readiness wrapper writes `readiness_summary.json` and returns success
only when current artifacts pass every required gate. A successful unit test
or finite training loss alone is not a production-readiness result.

## Data contract

Production datasets use the upstream full-task form:

```json
{"input":"Alice founded Acme.","output":{"entities":{"person":["Alice"],"organization":["Acme"]},"classifications":[{"task":"priority","labels":["normal","urgent"],"true_label":["normal"]}],"json_structures":[{"company":{"name":"Acme"}}],"relations":[{"founded":{"head":"Alice","tail":"Acme"}}]}}
```

The loader fails closed on malformed schemas, duplicate task or label names,
unknown true labels, unknown label descriptions, few-shot examples whose
output is not declared, invalid choices, missing annotations, and unsupported
normalization. The legacy `{text, entities}` format remains available for
ASCII-only compatibility. Non-ASCII legacy annotations are rejected; use the
full-task format for Unicode data.

Release data must be non-synthetic, content-disjoint between train and eval,
full-task covered, and bound to the model and adapter fingerprints recorded in
the training manifest.

## Runtime correctness and safety

The same graph owns native and Metal training. The required backend parity
test compares token, span-start, and full GLiNER2 total-loss gradients for
every trainable parameter. `TERMITE_REQUIRE_METAL_TESTS=1` makes a disabled
Metal build or missing device an error instead of a skip.

The full-task parity defect fixed on this branch was native buffer donation
through zero-copy reshape aliases: an early loss branch could overwrite
encoder storage still needed by another branch. Donation now uses the last use
of the entire alias group. The unchanged `1e-3` gradient thresholds pass with
worst observed relative errors below `4e-6` on the synthetic parity fixture.

For production-length Metal training, the fused DeBERTa attention path is a
safety requirement. A Metal run with sequence length at least 128 fails before
training if that path is disabled. Production Metal runs should also use
`--compiled-required`; checkpoint/resume then cannot silently cross between
compiled and interpreter execution.

## Recipe lifecycle

The unified recipe runner can own the train-to-materialize lifecycle:

```sh
antfly inference finetune run gliner2-recipe.json
```

Example production-shaped recipe (quality floors are illustrative and must be
set from the release policy for the dataset):

```json
{
  "recipe": "lora-sft",
  "model": {"path": "/models/gliner2", "family": "gliner2"},
  "dataset": {
    "train_path": "/data/train.jsonl",
    "eval_path": "/data/eval.jsonl",
    "labels": "person,organization,location",
    "max_seq_len": 128
  },
  "adapter": {"rank": 16, "alpha": 32, "dropout": 0},
  "optimizer": {
    "learning_rate": 0.0003,
    "weight_decay": 0.01,
    "epochs": 8,
    "micro_batch_size": 32,
    "gradient_accumulation_steps": 1,
    "max_grad_norm": 1.0
  },
  "eval": {
    "path": "/data/eval.jsonl",
    "backend": "native",
    "every_epochs": 1,
    "batch_size": 8,
    "early_stopping_patience": 2,
    "improvement_threshold": 0.0001,
    "entity_minimums": {"f1": 0.70, "exact_match": 0.50},
    "full_task_minimums": {
      "classifications_micro_f1": 0.75,
      "classifications_exact_match": 0.60,
      "json_structures_micro_f1": 0.70,
      "json_structures_exact_match": 0.50,
      "relations_micro_f1": 0.70,
      "relations_exact_match": 0.50,
      "count_accuracy": 0.75
    }
  },
  "checkpoint": {"every_epochs": 1, "keep_last": 3},
  "runtime": {"compiled_required": true, "graph_cache_capacity": 2},
  "backend": "metal",
  "artifacts": {
    "root": "/runs/gliner2",
    "trained_adapter_dir": "/runs/gliner2/adapter",
    "materialized_dir": "/runs/gliner2/model",
    "validation_report_path": "/runs/gliner2/validation.json",
    "evaluation_report_path": "/runs/gliner2/heldout.json",
    "reload_report_path": "/runs/gliner2/reload.json"
  }
}
```

When held-out evaluation is configured, the recipe fails at plan construction
unless the required entity and seven structured/count floors are complete.
Full-task saved-adapter scoring currently uses the Zig native evaluator even
when training uses Metal; non-native full-task evaluation is rejected rather
than partially scored. The emitted steps are:

1. `train-gliner2-autodiff`
2. `validate-gliner2-autodiff-run`
3. `eval-gliner2-autodiff-adapter-dataset`
4. `materialize-gliner2-lora` when `materialized_dir` is configured
5. `inspect-gliner2-checkpoint` against the published model

Materialization refuses an existing output directory. It writes to a sibling
staging directory, copies the complete required tokenizer/config inventory,
merges LoRA and task-head tensors, reload-inspects the staged checkpoint,
syncs files and directories, writes `materialization_manifest.json` last, and
atomically renames the completed directory into place.

## Direct tools

The recipe is preferred for normal operation. Individual tools remain useful
for diagnosis and controlled automation:

```sh
# Full-task resident-Metal training
zig build -Dmetal=true train-gliner2-autodiff -- \
  --model-dir /models/gliner2 \
  --train-data /data/train.jsonl \
  --eval-data /data/eval.jsonl \
  --out-dir /runs/gliner2/adapter \
  --objective gliner2-total-loss \
  --lora-only-trainables \
  --backend metal --compiled-required \
  --seq-len 128 --batch-size 32 \
  --checkpoint-every-epochs 1 --checkpoint-keep-last 3 \
  --eval-every-epochs 1 --early-stopping-patience 2

# Structural/runtime artifact validation
zig build validate-gliner2-autodiff-run -- \
  /runs/gliner2/adapter --out /runs/gliner2/validation.json

# Transactional merged checkpoint publication
zig build materialize-gliner2-lora -- \
  /models/gliner2 /runs/gliner2/adapter /runs/gliner2/model
```

Unified CLI aliases are also registered under `finetune train run
gliner2-autodiff`, `finetune adapter validate gliner2-run`, `finetune eval run
gliner2-adapter-dataset`, `finetune adapter materialize gliner2`, and
`finetune adapter inspect gliner2-checkpoint`.

## Python oracle and statistical parity

The frozen oracle is fastino-ai/GLiNER2 commit
`8f3fc399bcc5a00749a62a1565e5c6529f04b574`, running on Python 3.12 with
Unicode 15.0. Tooling verifies the checkout commit, cleanliness, and imported
module path. Model fingerprints cover the ordered required inventory and the
present/absent state of optional `spm.model`.

Deterministic step/update parity disables stochastic augmentation, shuffle,
dropout, and negative-mask RNG so intermediate losses, gradients, and updates
can be compared directly. Normal stochastic training is judged by five paired
independent seeds: every Zig run must clear the nine quality floors, the mean
Zig deficit for each metric must be at most 0.02, and no paired deficit may
exceed 0.05. Exact equality with Python RNG streams is intentionally not part
of the contract.

Both scorers identify exact-match normalization as
`unicode_nfc_collapsed_whitespace_casefold/v1`. Python implements the complete
normalizer. Zig admits a conservative Unicode 15 subset for which NFC and
casefold equivalence is proven, collapses all pinned Unicode whitespace, and
fails with an explicit error for normalization-changing scalars, casefold
expansions, or invalid UTF-8. Unsupported input is a documented admission
boundary, not silent score drift.

Generate a convergence summary from five paired runs:

```sh
python3.12 scripts/summarize_gliner2_convergence.py \
  --input /runs/gliner2/convergence-study.json \
  --output /runs/gliner2/convergence-summary.json \
  --upstream-source /src/GLiNER2
```

Then run the authoritative gate with the model, release adapter, disjoint
datasets, pinned checkout, convergence summary, and all nine `--heldout-min`
values:

```sh
scripts/run_gliner2_lora_production_readiness.sh \
  --model-dir /models/gliner2 \
  --release-adapter-dir /runs/gliner2/adapter \
  --train-data /data/train.jsonl \
  --eval-data /data/eval.jsonl \
  --python-bin /usr/local/bin/python3.12 \
  --upstream-source /src/GLiNER2 \
  --convergence-summary /runs/gliner2/convergence-summary.json \
  --heldout-min entities.micro_f1=0.70 \
  --heldout-min entities.exact_match=0.50 \
  --heldout-min classifications.micro_f1=0.75 \
  --heldout-min classifications.exact_match=0.60 \
  --heldout-min json_structures.micro_f1=0.70 \
  --heldout-min json_structures.exact_match=0.50 \
  --heldout-min relations.micro_f1=0.70 \
  --heldout-min relations.exact_match=0.50 \
  --heldout-min count.accuracy=0.75
```

The wrapper requires five production-shape performance runs, deterministic
correctness and update parity, median warm Metal step time no slower than the
Python baseline (and no run above 1.10x), pinned-oracle held-out quality, Zig
native held-out quality under the same normalization, five-seed convergence,
and consistent model/train/eval fingerprints. The experimental head-MLP
fusion gate is reported separately unless explicitly required.

## Focused verification

```sh
zig build test-gliner2-data -Doptimize=ReleaseSafe
zig build test-gliner2-recipe -Doptimize=ReleaseSafe
zig build test-gliner2-e2e -Doptimize=ReleaseSafe
zig build test-gliner2-autodiff-trainer -Doptimize=ReleaseSafe -Dmetal=true
zig build test-gliner2-run-validation -Doptimize=ReleaseSafe
zig build test-gliner2-graph-cache -Doptimize=ReleaseSafe -Dmetal=true
zig build test-gliner2-native-eval -Doptimize=ReleaseSafe
TERMITE_REQUIRE_METAL_TESTS=1 zig build \
  test-gliner2-backend-grad-parity -Dmetal=true -Doptimize=ReleaseSafe

cd scripts
python3.12 -m unittest \
  test_gliner2_release_contract.py \
  test_gliner2_parity_data.py \
  test_benchmark_gliner2_lora_perf.py \
  test_validate_gliner2_release_data.py \
  test_evaluate_gliner2_full_task.py \
  test_evaluate_gliner2_native_release_smoke.py \
  test_summarize_gliner2_convergence.py \
  test_finalize_gliner2_readiness.py
```

CI runs the Python contract tests on Python 3.12 and the required native/Metal
gradient gate on a macOS runner. Real-model release readiness still requires
operator-supplied model, train/eval data, upstream checkout, and retained run
artifacts; tests without those inputs validate the code contracts, not model
quality.
