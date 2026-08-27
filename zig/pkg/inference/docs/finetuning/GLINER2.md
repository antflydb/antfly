# Production GLiNER2 Finetuning with Zig, Metal, and CUDA

This is the operator and release contract for GLiNER2 finetuning in Antfly.
The production implementation is the Zig trainer, Zig graph runtime, and Zig
Metal/CUDA kernels. A pinned upstream Python checkout is used only as a correctness
oracle and performance baseline; it is not a deployed dependency. There is no
Go implementation, Go runtime dependency, or Go parity target in this flow.

## Current status

The branch contains a complete full-task LoRA lifecycle:

- upstream `{input, output}` GLiNER2 JSONL preprocessing for entities,
  classifications, JSON structures, relations, and count supervision;
- DeBERTa encoder and all GLiNER2 task heads in the Zig autodiff graph;
- LoRA-only training on the native interpreter or a resident Metal/CUDA runtime;
- bounded shape-specialized graph caching, gradient accumulation, clipping,
  AdamW, schedulers, checkpoint retention, exact resume, held-out loss,
  early stopping, and best-checkpoint selection;
- saved-adapter validation and native held-out full-task scoring;
- transactional merged-model materialization and reload inspection;
- deterministic Python/Zig correctness gates, native/accelerator gradient gates,
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

The same graph owns native, Metal, and CUDA training. The required backend parity
test compares token, span-start, and full GLiNER2 total-loss gradients for
every trainable parameter. `TERMITE_REQUIRE_METAL_TESTS=1` makes a disabled
Metal build or missing device an error instead of a skip.
`TERMITE_REQUIRE_CUDA_TESTS=1` provides the equivalent fail-closed CUDA gate.

The full-task parity defect fixed on this branch was native buffer donation
through zero-copy reshape aliases: an early loss branch could overwrite
encoder storage still needed by another branch. Donation now uses the last use
of the entire alias group. The unchanged `1e-3` gradient thresholds pass with
worst observed relative errors below `4e-6` on the synthetic parity fixture.

The Metal embedding-gradient scatter now scans source-row indices once per
output-row/hidden-dimension tile and accumulates matches in source order. This
removes the previous full source-row scan from every output element while
preserving deterministic native/Metal equality; the focused parity fixture
crosses both 256-row and 256-column tile boundaries.

For production-length Metal training, the fused DeBERTa attention path remains
a safety requirement: a Metal run with sequence length at least 128 fails
before training if that path is disabled. CUDA provides dedicated forward and
two-stage backward DeBERTa kernels at the same production geometry. Production
accelerator runs should use `--compiled-required`; checkpoint/resume then
cannot silently cross between compiled and interpreter execution.
Metal's strict graph-executor contract allows at most six host metadata outputs
per step. Op tracing binds those to the `i64` index conversions consumed by
device gathers; trainable transfers remain zero, and any seventh host output or
full-step fallback fails the gate.
The CUDA training executor caches a single-device partition and buffer plan,
keeps graph constants and runtime placeholders resident, uses page-locked
asynchronous input staging, and applies clipping plus AdamW as batched device
operations. Step metrics expose CUDA allocation/free counts, H2D/D2H bytes,
stream and upload synchronizations, cache hits/misses, launches, packed
attention calls, exact-GELU calls, and the count and byte volume of runtime-input
uploads. CUDA telemetry spans the complete trainer step, beginning before input
staging and ending after the resident optimizer update. Strict comparisons
require nonzero, fully accounted asynchronous input H2D while separately
rejecting trainable/optimizer-state transfers, a zero-dispatch executor,
interpreter fallbacks, true host graph outputs, missing fused attention/GELU
coverage, or bulk D2H traffic.
The public step contract downloads exactly two independent f32 scalars—loss
and gradient norm—so strict readiness allows eight total D2H bytes while still
limiting every individual transfer to four bytes and rejecting a third host
materialization.
At most one pinned-upload synchronization is allowed on the first compiled
optimizer step; every later retained step must report zero.
Held-out evaluation intentionally uses the eager device interpreter for its
objective-specific graph while still dispatching Metal/CUDA kernels. This
keeps `--eval-data`, early stopping, and best-checkpoint selection compatible
with compiled-required training without moving evaluation to CPU. CUDA memory
preflight compares the incremental training estimate with live free VRAM and
reserves 10% of total VRAM for driver/library workspaces.

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

# Full-task resident-CUDA training. Use the SM matching the deployment GPU;
# fatbin is the portable release artifact across the checked-in SM set.
zig build -Dcuda=true -Dcuda-artifacts=fatbin \
  train-gliner2-autodiff -- \
  --model-dir /models/gliner2 \
  --train-data /data/train.jsonl \
  --eval-data /data/eval.jsonl \
  --out-dir /runs/gliner2/cuda-adapter \
  --objective gliner2-total-loss \
  --lora-only-trainables \
  --backend cuda --compiled-required \
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
Unicode 15.0 and the exact package versions in
`scripts/gliner2/requirements-gliner2-oracle.txt`. Tooling verifies dependency
versions, checkout commit and cleanliness, and the imported module path in
every trainer/evaluator report. Model fingerprints cover the ordered required
inventory and the present/absent state of optional `spm.model`.

Create the default oracle environment from the checked-in lock surface:

```sh
python3.12 -m venv /private/tmp/gliner2-parity-venv
/private/tmp/gliner2-parity-venv/bin/pip install \
  -r scripts/gliner2/requirements-gliner2-oracle.txt
```

The Fastino checkout is supplied separately with `--upstream-source`; release
tools put that clean pinned checkout first on `PYTHONPATH` and reject an import
from the installed package copy.

Deterministic step/update parity disables stochastic augmentation, shuffle,
dropout, and negative-mask RNG so intermediate losses, gradients, and updates
can be compared directly. Normal stochastic training is judged by five paired
independent seeds. Those Python runs must prove `deterministic=False`, the
exact Fastino `SamplingConfig` defaults, enabled model dropout, LoRA dropout
`0.0`, training-mode schema conditioning, shuffled examples, and the default
`0.5` negative-span mask. Zig manifests must prove the implementation's actual
policy: disabled SamplingConfig/model dropout, deterministic eval-form schema
conditioning, enabled epoch shuffle, LoRA dropout `0.0`, and negative-span
masking at `0.5`. Weakened or deterministic reports are rejected by the
convergence materializer. Every Zig run must clear the nine quality floors,
the mean Zig deficit for each metric must be at most 0.02, and no paired
deficit may exceed 0.05. Zig and Python use independent RNG streams, so exact
stochastic traces are intentionally not part of the contract.

The pinned PEFT configuration registers 184 adapter tensors, but four of them
are inert `count_embed` `MultiheadAttention.out_proj` A/B pairs: PyTorch reads
the projection weight directly instead of invoking the wrapped module. Zig
therefore trains the 180 effective tensors. Optimizer parity waives only these
exact pairs and only when the Python dump proves zero steps, gradients, Adam
state, and `lora_B`; any active or differently named missing tensor fails.

Both scorers identify exact-match normalization as
`unicode_nfc_collapsed_whitespace_casefold/v1`. Python implements the complete
normalizer. Zig admits a conservative Unicode 15 subset for which NFC and
casefold equivalence is proven, collapses all pinned Unicode whitespace, and
fails with an explicit error for normalization-changing scalars, casefold
expansions, or invalid UTF-8. Unsupported input is a documented admission
boundary, not silent score drift.

Generate a convergence summary from five paired runs:

```sh
python3.12 scripts/gliner2/summarize_gliner2_convergence.py \
  --input /runs/gliner2/convergence-study.json \
  --output /runs/gliner2/convergence-summary.json \
  --upstream-source /src/GLiNER2 \
  --model-dir /models/gliner2 \
  --train-data /data/train.jsonl \
  --eval-data /data/eval.jsonl \
  --zig-backend cuda
```

Cross-implementation held-out reports fingerprint the standard PEFT files
(`adapter_config.json` and `adapter_model.safetensors`) consumed by the pinned
upstream decoder. Zig release finalization separately retains the stricter
three-file bundle fingerprint, including `task_head.safetensors`.

Then run the authoritative gate with the model, release adapter, disjoint
datasets, pinned checkout, convergence summary, current CUDA hardware matrix,
and all nine `--heldout-min` values. Produce one source-bound lane on each
required GPU, then aggregate the reports:

```sh
python3.12 scripts/gliner2/qualify_gliner2_cuda_hardware.py \
  --output /runs/gliner2/cuda-lane.json

# After collecting reports from A100 (SM80), L4 (SM89), and H100 (SM90):
python3.12 scripts/gliner2/summarize_gliner2_cuda_hardware.py \
  --report /runs/gliner2/cuda-sm80.json \
  --report /runs/gliner2/cuda-sm89.json \
  --report /runs/gliner2/cuda-sm90.json \
  --output /runs/gliner2/cuda-hardware-matrix.json
```

Each lane verifies the checked-in CUDA 13.2 artifacts, runs the complete FP32
gradient/update/checkpoint parity suite, and runs memcheck, initcheck, and
racecheck. The matrix rejects missing architectures, mismatched GPU identities,
failed sanitizers, non-FP32 evidence, and reports whose source fingerprint no
longer matches the checkout.

```sh
scripts/gliner2/run_gliner2_lora_production_readiness.sh \
  --zig-backend cuda --zig-cuda-artifacts fatbin \
  --model-dir /models/gliner2 \
  --release-adapter-dir /runs/gliner2/adapter \
  --train-data /data/train.jsonl \
  --eval-data /data/eval.jsonl \
  --python-bin /usr/local/bin/python3.12 \
  --upstream-source /src/GLiNER2 \
  --convergence-summary /runs/gliner2/convergence-summary.json \
  --cuda-hardware-qualification /runs/gliner2/cuda-hardware-matrix.json \
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
correctness and update parity, median warm accelerator step time no slower than
the Python baseline (and no run above 1.10x), pinned-oracle held-out quality, Zig
native held-out quality under the same normalization, five-seed convergence,
consistent model/train/eval fingerprints, and a current FP32 L4/A100/H100
sanitizer/parity matrix. The experimental head-MLP
fusion gate is Metal-only and is reported separately unless explicitly required.
Production performance runs use isolated seeds `11,23,37,53,71` by default;
`--seeds` can override the sequence but its count must match `--runs`. CUDA's
batch-32 timing lane is checkpoint-free on both implementations because the L4
profile fits without recomputation.

Deterministic summed-loss comparisons use a combined `1e-4` absolute and
`5e-6` relative tolerance. The absolute floor keeps small component losses
strict, while the relative term prevents the same floating-point accumulation
noise from failing only because a production batch contains more summed terms.

For an apples-to-apples CUDA comparison, the harness synchronizes both CUDA
runtimes around measured steps and records the PyTorch GPU name:

```sh
python3.12 scripts/gliner2/compare_gliner2_lora_python_zig.py \
  --model-dir /models/gliner2 \
  --python-model /models/gliner2 \
  --train-data /data/train.jsonl \
  --upstream-source /src/GLiNER2 \
  --python-device cuda \
  --zig-backend cuda --zig-build-cuda \
  --zig-cuda-artifacts fatbin \
  --deterministic --dump-parity --dump-optimizer-parity --strict
```

`portable` embeds PTX and depends on the installed driver accepting the PTX
version emitted by the CUDA toolkit. Use an SM-specific artifact (for example
`sm89` on an NVIDIA L4) when validating against an older driver, and use
`fatbin` for the checked-in multi-architecture release bundle.

The production contract is explicitly FP32 for graph tensors, trainables, and
AdamW state, matching the pinned Fastino/PyTorch oracle. Training manifests,
deterministic comparisons, convergence summaries, hardware lanes, and final
readiness reports all bind that precision. BF16 remains a separately gated
follow-on: it must not replace the FP32 path until loss/gradient/update parity,
five-seed quality, and the L4 plus A100/H100 release matrix pass under a new
explicit precision contract.

The batch-32 Metal profile chunks structure-loss span work in groups of 16
samples. This preserves the reported loss and bounds the structure-head tensor
shapes, but it is an optimization hint rather than a guaranteed whole-graph
peak reduction: other live tensors can dominate as the executor evolves. The
performance report always records peak live device-owned bytes (the sum of
simultaneously live device tensors, not process RSS). Deployments with a fixed
memory budget can enforce it with
`--max-zig-metal-peak-live-bytes-median`; the generic release gate does not
assume one device-independent ceiling.

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
TERMITE_REQUIRE_CUDA_TESTS=1 zig build \
  test-gliner2-backend-grad-parity -Dcuda=true \
  -Dcuda-artifacts=sm89 -Doptimize=ReleaseFast

cd scripts
python3.12 -m unittest \
  test_gliner2_release_contract.py \
  test_gliner2_parity_data.py \
  test_compare_gliner2_contract.py \
  test_benchmark_gliner2_lora_perf.py \
  test_validate_gliner2_release_data.py \
  test_evaluate_gliner2_full_task.py \
  test_evaluate_gliner2_native_release_smoke.py \
  test_summarize_gliner2_convergence.py \
  test_summarize_gliner2_cuda_hardware.py \
  test_finalize_gliner2_readiness.py
```

The real-model Python-oracle and Metal/CUDA hardware gates are currently
operator-run rather than provisioned by CI. Their retained, source-bound
artifacts remain required for a production-readiness claim; ordinary unit and
contract tests validate the implementation surfaces but do not substitute for
that evidence. The scoped readiness command trains on
the selected accelerator but evaluates saved full-task adapters on native by
default; `--eval-backend` and `--eval-compiled-required` control that policy
independently. Real-model release readiness still requires
operator-supplied model, train/eval data, upstream checkout, and retained run
artifacts; tests without those inputs validate the code contracts, not model
quality.
