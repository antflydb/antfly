# Fine-Tuning in termite-zig

termite-zig supports training and fine-tuning through reverse-mode automatic differentiation on the graph IR. Features are implemented in pure Zig and run on CPU (native) or Apple Silicon (Metal) — no CUDA dependencies.

---

## Unified Recipes

The recipe runner gives post-training one stable entry point:

```sh
termite finetune run recipe.json
```

It follows the same direction as TRL's trainer taxonomy and Training Hub's algorithm-first routing layer: users choose an algorithm, keep common fields stable, and let termite route to the existing family-specific prepare, train/eval, and materialize tools.

### Schema

All recipes use the same top-level sections:

```json
{
  "recipe": "lora-sft",
  "model": {
    "path": "/models/gemma4",
    "family": "gemma4",
    "projector_path": "/models/gemma4/mmproj.gguf"
  },
  "dataset": {
    "path": "/data/chat.jsonl",
    "train_path": "/data/train.jsonl",
    "eval_path": "/data/eval.jsonl",
    "train_split": "train",
    "eval_split": "eval",
    "train_cache_path": "/runs/gemma4/train_cache.json",
    "eval_cache_path": "/runs/gemma4/eval_cache.json",
    "max_examples": 128,
    "eval_max_examples": 64,
    "max_seq_len": 512
  },
  "adapter": {
    "path": "/runs/gemma4/adapter-bootstrap",
    "rank": 16,
    "alpha": 32,
    "target_preset": "all-linear",
    "scaling": "standard",
    "init_lora_weights": "default",
    "use_dora": false,
    "layer_name": "model.layers.0"
  },
  "optimizer": {
    "learning_rate": 0.0002,
    "epochs": 2,
    "gradient_accumulation_steps": 4,
    "max_grad_norm": 1.0
  },
  "preference": {
    "beta": 0.1
  },
  "grpo": {
    "group_size": 8,
    "clip_epsilon": 0.2,
    "kl_coef": 0.04,
    "normalize_advantage": true
  },
  "eval": {
    "max_examples": 64
  },
  "artifacts": {
    "root": "/runs/gemma4",
    "manifest_path": "/runs/gemma4/recipe_run_manifest.json",
    "prepared_path": "/runs/gemma4/prepared_inputs.json",
    "trained_adapter_dir": "/runs/gemma4/adapter-trained"
  },
  "backend": "auto"
}
```

Supported recipe names are `sft`, `lora-sft`, `qlora-sft`, `dpo`, `grpo`, `reranker`, and `vlm-retrieval`. The parser accepts unknown future fields so recipe files can grow without breaking older runners.

### LoRA Defaults

Recipe-level LoRA defaults are intentionally PEFT-like:

- SFT, QLoRA, VLM retrieval, and encoder/reranker LoRA bootstrap at `rank = 16`, `alpha = 32`.
- GRPO adapter-training routes default to `rank = 8`, `alpha = 32`; raise rank for larger policy tasks after an eval sweep justifies the extra adapter capacity.
- Scaling is standard LoRA `alpha / rank`. Recipe `adapter.scaling` currently accepts only `standard` aliases; rank-stabilized scaling is not enabled in the graph trainer path.
- Gemma4 defaults to `target_preset = "all-linear"`, which expands to attention and MLP linear patterns. Explicit `target_modules` override the preset.
- Qwen/ColQwen optimizer-backed routes default to their all-linear target module lists. They also accept `target_preset = "all-linear"`, `attention-only`, or `mlp-only`; `moe-experts` is rejected until expert-aware rank and routing policy is wired through those bootstraps.
- `init_lora_weights` and `use_dora` are currently Gemma4-only recipe knobs.

For learning-rate selection, do not copy full-finetune LRs directly. Start LoRA sweeps around `1e-4`, `3e-4`, and `1e-3` with the real target metric, then keep the smallest rank/target set that passes. Smaller micro-batches plus gradient accumulation are usually a better first move than shrinking rank below the defaults.

Use `--dry-run` to print the routed tool plan without launching training:

```sh
termite finetune run recipe.json --dry-run
```

For a no-download recipe-layer verifier, use:

```sh
termite finetune smoke-fast
```

`smoke-fast` runs quick dry-runs across every family adapter fixture, executes synthetic no-download GLiNER2, Qwen2, and Gemma4 recipe cases plus the fast scalar DPO/GRPO recipes, verifies the normalized run artifacts reach `status = "succeeded"`, and writes a suite summary at `/tmp/termite-finetune-smoke-fast/fast_smoke_summary.json` by default.

### Adapter Matrix

| Recipe | Family | Current route |
|--------|--------|---------------|
| `lora-sft`, `qlora-sft` | `gemma4` | `prepare-gemma4-lora-inputs` → `bootstrap-gemma4-lora` → `train-eval-gemma4-lora-bundle` |
| `lora-sft`, `qlora-sft` | `gliner2` | `bootstrap-gliner2-lora` → `prepare-gliner2-top-layer-boundary-cache` → `train-eval-gliner2-lora-bundle` → optional `materialize-gliner2-lora` |
| `lora-sft`, `qlora-sft` | `layoutlmv3` | `bootstrap-layoutlmv3-lora` → `train-eval-layoutlmv3-lora-sequence` or `train-eval-layoutlmv3-lora-token` → optional `materialize-layoutlmv3-checkpoint` |
| `sft` | supported LoRA families | same route as the family `lora-sft` adapter while full-weight SFT backends are still family-specific |
| `dpo` | scalar preference fixtures | direct internal `preference_loss.zig` adapter over `dataset.format = "scalar-logprobs"` JSONL |
| `dpo` | decoder models with local weights | direct internal `preference_harness.zig` adapter over `dataset.format = "text-preference"` or `"rendered-text-preference"` JSONL with `model.path` and optional `model.reference_path`; Gemma4, Qwen2, ColQwen2, and Qwen3.5 text recipes now also have optimizer-backed adapter-training paths |
| `grpo` | scalar/token fixtures | direct internal `grpo.zig` adapter over `dataset.format = "token-logprobs"` JSONL |
| `grpo` | decoder models with local weights | direct internal adapter over `dataset.format = "text-grpo"` or `"rendered-text-grpo"` JSONL with deterministic decoder sampling, reward modes including `exact-match`, `exact-match-ci`, and `prefix-match`, and optional `model.reference_path`; Gemma4, Qwen2, ColQwen2, and Qwen3.5 text recipes now also have optimizer-backed adapter-training paths, and Gemma4 also has a multimodal path |
| `reranker` | `reranker` | `prepare-reranker-pooled-cache` → `train-eval-reranker-head-cached` → optional `materialize-reranker-head` |
| `lora-sft`, `qlora-sft` | `reranker` | `bootstrap-reranker-lora` → `prepare-reranker-top-layer-cache` → `train-eval-reranker-lora-top-layer-cached-surrogate` → optional `materialize-reranker-lora` |
| `vlm-retrieval` | `colqwen2` | `prepare-colqwen2-inputs` → `bootstrap-colqwen2-lora` → `train-eval-colqwen2-lora-bundle` |
| `sft`, `lora-sft`, `qlora-sft`, `dpo`, `grpo` | `qwen3_5`, Chandra OCR text-only | direct Qwen autodiff trainer route for text JSONL recipes; still requires real-weight CPU and Metal smokes before production readiness |

The runner first looks for a peer executable next to `termite`. If it is not installed, it falls back to the existing `zig build <tool> -- ...` build step from the package root, preserving today's build-step workflow.

### Run Artifacts

Non-dry runs write a normalized manifest at `artifacts.manifest_path` or `<artifacts.root>/recipe_run_manifest.json`.

The manifest schema version is `termite_finetune_recipe_run/v1` and records:

- the original parsed recipe
- artifact root
- expanded step names and argv
- overall run status: `planned`, `running`, `succeeded`, or `failed`
- per-step status, exit code, stdout byte count, and stderr byte count

The runner writes the manifest before execution starts, updates it before each step, and writes a final success or failure state.

Every non-dry run also writes:

- `<artifacts.root>/training_config.json` with the normalized recipe, expanded step plan, dataset fingerprints, backend build metadata, and optimizer summary
- `<artifacts.root>/training_report.json` with the normalized final status, per-step execution records, dataset fingerprints, backend build metadata, optimizer summary, and final artifact checksums

Direct DPO and GRPO adapters also write `artifacts.report_path`, or `<artifacts.root>/dpo_report.json` / `<artifacts.root>/grpo_report.json` when no explicit report path is provided.

The scalar DPO input format remains JSONL with precomputed logprob rows:

```json
{"policy_chosen_logp":-1.0,"policy_rejected_logp":-2.0,"ref_chosen_logp":-1.2,"ref_rejected_logp":-1.8}
```

Model-backed DPO also accepts text preference rows:

```json
{"prompt":"Answer with one word: yes or no?","chosen":"yes","rejected":"no"}
```

Use `dataset.format = "text-preference"` to treat `prompt` as user content and apply the model chat template when one exists, or `dataset.format = "rendered-text-preference"` when `prompt` is already the final rendered decoder prompt. `model.reference_path` is optional; when omitted, the runner reuses `model.path` as the reference model.

The current direct GRPO input is JSONL with token-level rows:

```json
{"prompt_idx":0,"tokens":[10,11],"old_logps":[-0.4,-0.6],"ref_logps":[-0.5,-0.7],"new_logps":[-0.35,-0.65],"reward":1.0}
```

Model-backed GRPO also accepts text prompt rows:

```json
{"prompt":"Answer with one word: yes or no?","target":"yes"}
```

Use `dataset.format = "text-grpo"` to treat `prompt` as user content and apply the model chat template when one exists, or `dataset.format = "rendered-text-grpo"` when `prompt` is already the final decoder prompt. The current model-backed GRPO route supports:

- `grpo.group_size`
- `grpo.max_completion_tokens`
- `grpo.reward_mode = "exact-match"`, `"exact-match-ci"`, or `"prefix-match"`
- optional `model.reference_path`

`exact-match-ci` is trimmed ASCII case-insensitive equality.

For Gemma4 multimodal GRPO, add `model.projector_path` and use prompt rows with media placeholders plus `image_paths` / `audio_paths`. The current optimizer-backed multimodal route reuses the Gemma projector-backed autodiff path and requires the reference path to stay on the same base model directory.

### Remaining Task List

Completed:

1. Added a common recipe schema and `termite finetune run <recipe.json>`.
2. Added adapter routing for Gemma4 LoRA, GLiNER2 LoRA, LayoutLMv3 LoRA, reranker head, reranker LoRA, and ColQwen2 VLM retrieval.
3. Split train/eval dataset and cache fields where existing tools require separate train/eval inputs.
4. Added dry-run expansion tests for every supported adapter family plus SFT, DPO, and GRPO recipes.
5. Added example recipe files under `testdata/`.
6. Added a normalized recipe-run manifest with status and expanded step records.
7. Promoted `sft`, `dpo`, and `grpo` from reserved schema values to runnable recipes.
8. Added direct internal DPO and GRPO adapters over normalized logprob fixture formats.
9. Added normalized `training_config.json` and `training_report.json` run artifacts.
10. Replaced shell-out execution for reranker head recipes with direct internal prepare, train/eval, and materialize adapters.
11. Replaced shell-out execution for Gemma4 recipes with direct internal prepare, bootstrap, and train/eval adapters.
12. Replaced shell-out execution for GLiNER2 recipes with direct internal bootstrap, cache prepare, train/eval, and materialize adapters.
13. Replaced shell-out execution for LayoutLMv3 recipes with direct internal bootstrap, train/eval, and materialize adapters.
14. Replaced shell-out execution for reranker LoRA recipes with direct internal bootstrap, top-layer cache prepare, surrogate train/eval, and materialize adapters.
15. Replaced shell-out execution for ColQwen2 recipes with direct internal prepare, bootstrap, and train/eval adapters.
16. Extended normalized recipe reports with dataset fingerprints, backend build metadata, optimizer summaries, and artifact checksums.
17. Added a first model-backed DPO route for decoder models using `preference_harness.zig`, real sequence logprobs, and optional explicit reference model paths.
18. Added a first model-backed GRPO route for decoder models using `preference_harness.zig`, deterministic decoder sampling, exact-match rewards, and optional explicit reference model paths.
19. Added `termite finetune smoke-fast` for fast no-download recipe-layer verification across family dry-runs and scalar preference executes.
20. Added one synthetic no-download GLiNER2 direct-family execute case to `smoke-fast`, covering bootstrap, cache prepare, train/eval, and normalized artifact finalization through the unified recipe runner.
21. Updated the fine-tuning docs so `termite finetune run` is the primary public entrypoint and family build-step commands are documented as backend reference.
22. Added an initial optimizer-backed Gemma4 LoRA DPO path for `dataset.format = "text-preference"`, using live autodiff policy logprobs plus `preference_loss` gradients to train adapters and emit a trained adapter bundle.
23. Added an initial optimizer-backed Gemma4 LoRA GRPO path for `dataset.format = "text-grpo"`, using live autodiff sampling plus token-logprob gradients to train adapters.
24. Added `prefix-match` as a second text reward mode for model-backed GRPO and covered the new dry-run route in `smoke-fast`.
25. Broadened the optimizer-backed Gemma4 LoRA DPO and GRPO routes to also accept `rendered-text-preference` and `rendered-text-grpo`, using token-based prepared examples for the rendered DPO path.
26. Tightened targeted Gemma autodiff coverage for token-logprob gradient projection across prompt/completion boundaries.
27. Broadened optimizer-backed Gemma4 GRPO to a multimodal route using `model.projector_path`, media-aware prompt preparation, and a frozen multimodal reference trainer for KL scoring.
28. Added `exact-match-ci` as a trimmed ASCII case-insensitive GRPO text reward mode and covered it with a `smoke-fast` dry-run recipe.
29. Broadened optimizer-backed DPO beyond Gemma4 by adding a Qwen2 text route that reuses the unified token-preference recipe flow and emits standard adapter artifacts.
30. Broadened optimizer-backed GRPO beyond Gemma4 by adding a Qwen2 text route that reuses the unified prompt-sampling recipe flow and is covered by `smoke-fast` dry-run recipes.
31. Broadened optimizer-backed text GRPO and DPO family routing to include ColQwen2 text-only recipes via the existing Qwen2-backed decoder trainer path.
32. Added execute-path verification for optimizer-backed Qwen2 DPO and GRPO in `smoke-fast`.
33. Added execute-path verification for optimizer-backed Gemma4 GRPO in `smoke-fast`; the native backend now preserves unshaped vector gather semantics for the current decoder graph.
34. Removed the external local tokenizer-bundle dependency from the synthetic decoder smoke assets by generating tiny fallback HF tokenizer files when needed.
35. Added Qwen3.5/Chandra fine-tune readiness gating so unified recipes no longer infer those models as Qwen2 or route adapter training through the Qwen2 autodiff graph.
36. Added the first Qwen3.5 training graph slice: full-attention text layers now build with gated `q_proj`, Qwen3.5 `1 + weight` RMSNorm, and partial-RoPE metadata, while linear-attention layers fail explicitly.
37. Added Qwen3.5 linear-attention graph IR and routed text SFT/DPO/GRPO adapter recipes through the Qwen autodiff trainer.

Remaining:

1. Add real-weight one-step smoke coverage for Qwen3.5 text SFT/DPO/GRPO on CPU and Metal.
2. Add execute-path verification for the broader Qwen-family text-decoder routes, including ColQwen2 if we keep that path.
3. Add Chandra multimodal training data preparation with dynamic image-token expansion before enabling multimodal fine-tune recipes.
4. Add more GRPO reward modes if we need tasks beyond exact, exact-match-ci, and prefix matching.

---

## Architecture

```
Forward:  trace → optimize → cache → interpreter.execute(fused ops)
Backward: autodiff.gradient() → optional checkpoint rewrite → interpreter.execute(primitive ops)
Update:   extract grad f32 → flat optimizer state update → upload updated weights
```

The key insight: fused ops (`linear`, `rms_norm`, `attention`) are efficient for inference but opaque to differentiation. Each fused op carries a `vjp_alternate` pointer to its primitive decomposition. `autodiff.gradient()` uses `lower.zig` to expand these, then applies VJP rules on the primitive graph.

---

## Autodiff

`lib/ml/src/graph/autodiff.zig`

Reverse-mode AD with ~25 VJP rules covering all primitive ops. Given a scalar loss node and a list of parameter nodes, returns gradient node IDs in the lowered graph.

Flow:
1. `lower.lower()` expands fused ops via `vjp_alternate` into primitives
2. Walk backward from the loss node, applying VJP rules at each primitive
3. Accumulate adjoints when a node has multiple consumers
4. Return `GradientResult { grad_graph, param_grad_ids, loss_id }`

### VJP Rules

- **Elementwise** (add, mul, exp, log, sqrt, tanh, ...): standard calculus
- **dot_general**: `dA = dY @ B^T`, `dB = A^T @ dY` (with batched 3D support)
- **reduce_sum**: broadcast gradient back to original shape
- **transpose/reshape**: inverse permutation/reshape
- **gather/scatter**: scatter_add for gather grad, gather for scatter grad
- **softmax**: `dX = softmax * (dY - sum(dY * softmax))`

`lib/ml/src/graph/grad_check.zig` provides finite-difference gradient verification for validating VJP implementations.

---

## Primitive Op Backend

The `ComputeBackend` vtable has optional methods for all primitive ops (defaulting to `null` so backends that don't support training compile unchanged). The native backend (`src/ops/native_compute.zig`) implements all of them:

- **Elementwise**: subtract, divide, negate, sqrt, rsqrt, exp, log, sin, cos, tanh, abs, erf, less_than, where_select
- **Shape-aware**: reduce_sum/max/mean (with axes), reshape, transpose (with permutation), broadcast_in_dim, slice, concat, pad
- **Structured**: dot_general (batched matmul via BLAS sgemm), gather, scatter_add
- **Fused primitives**: softmax, log_softmax (with last-dim-size parameter)

The interpreter (`src/graph/interpreter.zig`) dispatches primitive ops to these vtable methods, extracting shape info from graph nodes for shape-aware ops.

---

## Loss Functions

Built as compositions in the builder API (`lib/ml/src/graph/builder.zig`):

- **softmax / log_softmax**: emitted as fused ops (`fused_softmax`, `fused_log_softmax`) with `vjp_alternate` decompositions
- **Cross-entropy loss**: `-reduceSum(target * logSoftmax(logits)) / batch_size`
- **MSE loss**: `reduceMean((pred - target)^2)`

No special loss op nodes — losses compose from existing primitives and get their gradients automatically through autodiff.

---

## Training Step

`src/graph/training.zig`

```zig
pub fn trainStep(allocator, graph, loss_node, cb, options) !TrainStepResult
```

1. Run `autodiff.gradient()` to produce the combined forward+backward graph
2. Mark loss and gradient nodes as outputs
3. Execute the combined graph through the interpreter
4. Extract loss as f32 scalar and gradients as f32 slices keyed by parameter name

`TrainStepResult` fields:
- `loss`
- `gradients`
- `profile`: autodiff / checkpoint / execute / extract / total timing
- `checkpoint_summary`: optional savings analysis for checkpointed runs

---

## Optimizers

`lib/ml/src/graph/optimizers.zig` — pure f32 math, no backend dependency.

| Optimizer | Features |
|-----------|----------|
| SGD | Optional momentum |
| Adam | Bias-corrected first/second moments (beta1, beta2, epsilon) |
| AdamW | Decoupled weight decay |

Learning rate schedules: `constant`, `cosine` (with min_lr), `warmup_cosine` (linear warmup then cosine decay).

Gradient clipping via `clipGradients()` with configurable L2 norm threshold.

Per-parameter state (momentum `m`, second moment `v`) stored in `OptimizerState`.

The Adam/AdamW hot path also supports `stepSlices()` — a fused SIMD path for contiguous `f32` buffers used by the training loop.

---

## Training Loop

`src/graph/training_loop.zig`

### TrainingWeightStore

The inference `WeightStore` is read-only. `TrainingWeightStore` wraps it with mutable f32 copies for trainable parameters:

- `materializeTrainable(name)`: copies a weight from the base store to a mutable f32 buffer
- `setWeight(name, data)`: updates a trainable weight
- Frozen weights delegate reads to the base store

### FlatTrainingState

The training loop builds an internal flat execution layout over trainable parameters and optimizer state:

- deterministic per-parameter layout metadata
- one contiguous parameter buffer
- one contiguous gradient buffer
- one contiguous first-moment buffer
- one contiguous second-moment buffer

This removes name-hash lookups and fragmented iteration from the optimizer hot path.

### TrainingLoop

```
for each step:
    1. refresh flat state from named trainable weights + optimizer state
    2. build runtime inputs from contiguous parameter spans
    3. training.trainStep() → loss + gradients + timing profile
    4. copy gradients into contiguous gradient spans
    5. clipGradients()
    6. optimizer.stepSlices() on each contiguous trainable span
    7. sync updated params/moments back to the named stores
```

Per-step metrics include: runtime-input build time, optimizer time, total step time, flat-state size counters, and the underlying `TrainStepProfile`.

### Checkpointing (Binary)

Binary checkpoint format: header + named parameter blobs + optimizer state (`m`, `v` vectors per parameter + step counter). `saveCheckpoint(path)` / `loadCheckpoint(path)`.

---

## LoRA

`lib/ml/src/graph/lora.zig`

```
output = frozen_linear(x) + scale * (x @ A^T @ B^T)
```

- A is random-initialized, B is zero-initialized (so initial output equals the frozen output)
- `injectLoRA(graph, config)` finds `fused_linear_no_bias` nodes matching target patterns (e.g., `"q_proj"`, `"v_proj"`) and injects A/B parameter nodes + matmul + add
- Gradients flow through the injected ops automatically
- `mergeLoRA()` folds trained adapters back into base weights for inference

---

## Activation Checkpointing

`lib/ml/src/graph/checkpoint.zig`

Trades compute for memory by recomputing activations instead of storing them:

- Identifies checkpoint boundaries in the forward subgraph (every N layers or at attention outputs)
- For backward nodes referencing non-checkpoint activations, inserts recomputation chains from the preceding checkpoint
- Exposed through `TrainingConfig.checkpoint_config`
- Optional summary reporting: total forward activations, checkpointed activations, recomputable activations, savings ratio

Checkpointing is opt-in. In the current synthetic graph benchmark it increases step time while process RSS stays roughly flat — useful as a configurable memory lever.

---

## Distributed Training

`src/graph/distributed_training.zig`

Data-parallel training across multiple devices:

1. Each device runs forward+backward on its data shard
2. Gradients are averaged via `collective_ops.allReduceSum`
3. Each device applies `optimizer.step` with the averaged gradients

Threads through the same checkpoint configuration and checkpoint-analysis reporting as the single-device path.

---

## Training Features by Model Family

The fused-chunker embedder (`src/finetune/fused_chunker_train.zig`) is the reference implementation; other model families (LayoutLMv3, Reranker, ColQwen2, GLiNER2, Gemma4) progressively share these features.

| Feature | Fused Chunker | LayoutLMv3 Seq | LayoutLMv3 Token | Reranker LoRA | ColQwen2 | GLiNER2 LoRA | Gemma4 LoRA |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| AdamW optimizer | yes | yes | yes | yes | yes | yes | yes |
| Layer-wise LR decay (LLRD) | yes | yes | yes | yes | yes | yes | yes |
| Global gradient norm clipping | yes | yes | yes | yes | yes | yes | yes |
| Gradient accumulation | yes | yes | yes | — | yes | yes | yes |
| Schedule-Free AdamW | yes | — | — | yes | yes | — | yes |
| DDP (Metal allReduce) | — | — | — | — | yes | — | yes |
| PJRT fast path | — | — | — | — | yes | — | yes |
| Cross-Batch Memory (XBM) | yes | — | — | — | — | — | — |
| NEFTune noise | yes | — | — | — | — | — | — |
| SPLADE sparse embeddings | yes | — | — | — | — | — | — |
| Matryoshka Repr. Learning (MRL) | yes | — | — | — | — | — | — |
| Mixed precision (bf16) | yes | — | — | — | — | — | — |
| LoRA+ | yes | — | — | — | — | — | — |
| Checkpoint resume | yes | — | — | — | — | — | — |
| Optimizer state save/load | yes | — | — | — | — | — | — |

### AdamW Optimizer

Default hyperparameters:

| Parameter | Default |
|---|---|
| β1 (first moment decay) | 0.9 |
| β2 (second moment decay) | 0.999 |
| ε (numerical stability) | 1e-8 |
| weight decay | 0.01 |

All model families use these defaults. The fused-chunker routes through `lib/ml` `optimizers.Optimizer`; LayoutLMv3, ColQwen2, and reranker families use an inline `applyAdamWInPlace` helper with the same constants.

Note: optimizer moment buffers (`m`, `v`) must be allocated once per run and carried across all epochs and steps — resetting them inside the per-epoch loop causes the adaptive learning rate to restart from scratch.

### Layer-wise Learning Rate Decay (LLRD)

LLRD assigns a lower learning rate to shallower (earlier) encoder layers, preventing catastrophic forgetting in lower layers while allowing task-specific top layers to adapt more quickly.

**Formula:** for layer index `i` (0 = shallowest), with `N` total layers and decay factor `d`:

```
lr_i = base_lr × d^(depth_from_top)
```

where `depth_from_top = N - 1 - i`. At `d = 1.0` all layers receive `base_lr` (disabled). At `d = 0.9`, each shallower layer is multiplied by another factor of 0.9.

**CLI flag:** `--llrd-decay <float>` (default: `1.0` = disabled)

### Global Gradient Norm Clipping

The global L2 norm is computed across all trainable parameter gradients simultaneously, then all gradients are scaled uniformly if the global norm exceeds the threshold. Joint clipping preserves gradient direction between layers; per-tensor clipping does not.

**CLI flag:** `--max-grad-norm <float>` (default: `1.0`; `0` to disable)

### Gradient Accumulation

Sums gradients across multiple forward/backward passes before applying a single optimizer step, simulating a larger effective batch size. With `--grad-accum N`, the optimizer step is deferred until `N` batches have been processed; gradients are normalized by `N` before the step.

**CLI flag:** `--grad-accum <int>` (default: `1` = disabled)

### Schedule-Free AdamW (Defazio 2024)

Eliminates the need for an explicit LR schedule by maintaining two parameter vectors:

- **z**: the "base iterate", updated by the standard gradient step
- **x**: the "Polyak average" of z, used as actual model weights for inference

During training: `x = (1 - c) * x + c * z`, where `c = (1 - β1) * lr`.

**CLI flag:** `--schedule-free` (boolean)

### Cross-Batch Memory (XBM) — fused-chunker only

A ring buffer of chunk embeddings from recent training batches. During InfoNCE contrastive loss computation, stored embeddings are concatenated with the current batch to expand the effective negative set. The buffer stores up to `--xbm-capacity` embedding vectors as a flat circular array. A monotonically increasing doc-ID offset prevents false-negative collisions across batches.

**CLI flag:** `--xbm-capacity <int>` (default: `0` = disabled)

### NEFTune Noise — fused-chunker only

Adds uniform random noise to encoder hidden states during the forward pass, scaled as `alpha / sqrt(seq_len * hidden_size)` so that noise magnitude is independent of sequence length and model width.

**CLI flag:** `--neftune-alpha <float>` (default: `0.0` = disabled)

### SPLADE Sparse Embeddings — fused-chunker only

Adds a vocabulary-space sparse vector head alongside the dense chunk embedding:

```
v[vocab] = max_over_tokens( log(1 + relu(hidden[t] @ W^T)) )
```

Uses a FLOPS regularization term to keep sparse vectors truly sparse.

**CLI flags:**
- `--splade` — enable the SPLADE head
- `--lambda-splade <float>` — SPLADE contrastive loss weight (default: `0.15`)
- `--lambda-flops <float>` — FLOPS sparsity regularization weight (default: `3e-5`)
- `--splade-focus-epoch <int>` — epoch at which SPLADE loss activates (default: `4`)

### Matryoshka Representation Learning (MRL) — fused-chunker only

Trains the model to produce useful embeddings at multiple truncated dimensions simultaneously. At inference time, embedding size can be traded against retrieval quality by truncating to any of the trained sizes.

**CLI flags:**
- `--mrl` — enable MRL
- `--mrl-dims <string>` — comma-separated list of embedding dimensions (default: `"768,256,128"`)

### Mixed Precision (bf16) — fused-chunker only

Enables bfloat16 computation for the Metal backend. Weights and activations are stored and multiplied in bf16, with gradient accumulation in f32.

**CLI flag:** `--mixed-precision` (boolean; Metal backend only)

### LoRA+ — fused-chunker only

Applies a higher learning rate to the LoRA B matrix than to the A matrix, since B is initialized to zero and must learn a larger signal in early training.

**Formula:** B-matrix LR = `R * base_lr`; A-matrix LR = `base_lr`. At `R = 1.0` this reduces to standard LoRA.

**CLI flag:** `--lora-plus-ratio <float>` (default: `1.0`)

---

## SafeTensors Checkpoint Format

```
[8 bytes: header_size as u64 little-endian]
[header_size bytes: UTF-8 JSON]
[tensor data: concatenated f32 values, little-endian]
```

The JSON header contains per-tensor metadata: `dtype`, `shape`, and `data_offsets` relative to the start of the data section. Compatible with the Python `safetensors` library.

`FusedTrainer.loadCheckpoint` first attempts to parse as SafeTensors; if that fails it falls back to the legacy binary format.

**Optimizer state persistence:** When `--save-optimizer-state` is passed, AdamW first and second moment buffers (`adam_m_*`, `adam_v_*`) and the step counter (`adam_step`) are saved as a separate SafeTensors file. For Schedule-Free AdamW, `z` and `v` buffers are saved as `sf_z_*` and `sf_v_*`.

---

## Benchmarking

`src/bench/training.zig`

Two focused measurements:

- **Optimizer microbenchmark**: scalar AdamW reference vs. fused/SIMD AdamW slice path
- **Graph benchmark**: synthetic train graph with configurable depth/width/batch, optional GELU activations, checkpoint interval sweep (`off`, `2`, `4`, `8`), average timing and peak resident-memory reporting

```sh
zigup run master build bench-training
zigup run master build bench-training -- --mode both
zigup run master build bench-training -- --mode graph --graph-activation gelu --checkpoint-sweep
```

Current results:
- The fused/SIMD AdamW path improves the isolated optimizer kernel by ~1.25x–1.30x
- End-to-end graph time is dominated by graph execution rather than optimizer work
- Checkpointing is structurally correct: recompute cost is more visible than memory savings in synthetic benchmarks

---

## Family Command Reference

Use `termite finetune run <recipe.json>` for normal post-training work.

The family-specific `zig build <tool> -- ...` commands below are still useful
for implementation work, debugging, and narrow backend verification, but they
are backend-facing reference surfaces now, not the primary public workflow.

---

## LayoutLMv3 PEFT Surface

This section documents the LayoutLMv3 backend commands used by the recipe
runner.

Preferred user path:

```sh
termite finetune run recipe_layoutlmv3_lora_token.json
termite finetune run recipe_layoutlmv3_lora_token.json --dry-run
```

### Build Steps

| Step | Purpose |
|------|---------|
| `bootstrap-layoutlmv3-lora` | Initialize LoRA bundle from base model |
| `inspect-layoutlmv3-lora-bundle` | Inspect a bootstrapped or trained bundle |
| `materialize-layoutlmv3-checkpoint` | Merge LoRA adapters into base weights |
| `train-layoutlmv3-lora-one-step` | Bounded single-step LoRA training |
| `train-eval-layoutlmv3-lora-sequence` | Bounded sequence classification train/eval |
| `train-eval-layoutlmv3-lora-token` | Bounded token classification train/eval |
| `run-layoutlmv3-lora-smoke-workflow` | Full bootstrap→train→inspect→materialize chain |
| `test-layoutlmv3-finetune` | Unit test |

Local verification:

```bash
ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache \
ZIG_LOCAL_CACHE_DIR=/tmp/zig-local-cache \
zigup run master build test-layoutlmv3-finetune -Dsystem-blas=false -Donnx=false
```

### Expected Inputs

Base model directory: must contain `config.json` and `model.safetensors`.

Adapter directory after bootstrap or training:
- `adapter_model.safetensors`
- `adapter_config.json`
- After sequence training: `layoutdoc_sequence_head.safetensors`, `sequence_head_config.json`
- After token training: `layoutdoc_token_head.safetensors`, `token_head_config.json`

Dataset JSONL fields:
- `document_id`, `page_id`, `image_path`, `tokens` (required)
- `label` (required for sequence examples)
- `token_labels` (required for token examples; count must match token count)
- Optional: `runtime_token_weights`, `teacher_token_hidden`, `teacher_token_probs`

Each token: `{ "text": "...", "bbox": [x0, y0, x1, y1] }`. Bbox values must be within `0..1000`.

### Core Commands

Bootstrap a LoRA bundle:

```bash
zigup run master build bootstrap-layoutlmv3-lora -Dsystem-blas=false -Donnx=false -- \
  /path/to/layoutlmv3_base \
  /path/to/bootstrap_dir \
  8 \
  16
```

Inspect a bundle:

```bash
zigup run master build inspect-layoutlmv3-lora-bundle -Dsystem-blas=false -Donnx=false -- \
  /path/to/layoutlmv3_base \
  /path/to/adapter_dir \
  /tmp/layoutlmv3_lora_inspect.json
```

Materialize a merged checkpoint:

```bash
zigup run master build materialize-layoutlmv3-checkpoint -Dsystem-blas=false -Donnx=false -- \
  /path/to/layoutlmv3_base \
  /path/to/trained_adapter_dir \
  sequence \
  /path/to/materialized_dir \
  /tmp/layoutlmv3_materialize_report.json
```

Run bounded sequence training:

```bash
zigup run master build train-eval-layoutlmv3-lora-sequence -Dsystem-blas=false -Donnx=false -- \
  /path/to/layoutlmv3_base \
  /path/to/bootstrap_dir \
  /path/to/train.jsonl \
  /path/to/val.jsonl \
  /path/to/sequence_out \
  128 \
  0.001 \
  64 \
  4 \
  @layoutlmv3_sequence_top3
```

Run bounded token training:

```bash
zigup run master build train-eval-layoutlmv3-lora-token -Dsystem-blas=false -Donnx=false -- \
  /path/to/layoutlmv3_base \
  /path/to/bootstrap_dir \
  /path/to/train.jsonl \
  /path/to/val.jsonl \
  /path/to/token_out \
  128 \
  0.001 \
  64 \
  4 \
  @layoutlmv3_token_top3
```

### Smoke Workflow

The smoke workflow chains bootstrap, train, inspect, and materialize in one command:

```bash
zigup run master build run-layoutlmv3-lora-smoke-workflow -Dsystem-blas=false -Donnx=false -- \
  /path/to/layoutlmv3_base \
  /path/to/train.jsonl \
  /path/to/val.jsonl \
  sequence \
  /path/to/output_root \
  8 \
  16 \
  32 \
  0.001 \
  16 \
  2 \
  @layoutlmv3_sequence_top3
```

For token classification, replace `sequence` with `token` and use `@layoutlmv3_token_top3`.

Smoke workflow writes:
- `bootstrap/`, `trained/`, `materialized/`
- `smoke_workflow_report.json`
- `training_config.json`, `training_report.json`, `run_status.json`

Report contents: dataset stats, bootstrap summary, initial adapter inspection, train/eval summary, merged materialization summary.

### Layer Scope Presets

| Preset | Use |
|--------|-----|
| `@layoutlmv3_token_top1` | Single top-layer token scope |
| `@layoutlmv3_token_top3` | Top-3 layers, token |
| `@layoutlmv3_sequence_top3` | Top-3 layers, sequence |

### Artifact File Contracts

| Artifact | File |
|----------|------|
| Base checkpoint | `model.safetensors` |
| LoRA adapter | `adapter_model.safetensors` |
| Sequence head | `sequence_head.safetensors` + `sequence_head_config.json` |
| Token head | `token_head.safetensors` + `token_head_config.json` |
| Merged bundle | `model.safetensors` + `config.json` + tokenizer files |

### Runbook for Larger Machines

1. Confirm the base bundle contains `config.json` and `model.safetensors`.
2. Confirm the train/val JSONL data loads cleanly and matches the expected task.
3. Run `test-layoutlmv3-finetune` once on that machine.
4. Run `run-layoutlmv3-lora-smoke-workflow` with small example counts first.
5. Inspect `smoke_workflow_report.json`, `bootstrap/adapter_config.json`, and the trained head config.
6. If the smoke run is healthy, increase the bounded example counts and epochs.

### Limitations

- No full-backbone LayoutLMv3 fine-tuning
- No distributed training or mixed precision on this path
- Task heads are bounded termite-owned implementations

---

## CLI Reference

### Fused Chunker (`src/train_fused_chunker.zig`)

```
usage: train-fused-chunker --data <path> --output <dir> [options]

  --data <path>             JSONL data path (file or directory)
  --output <dir>            Output directory for checkpoints
  --model-dir <dir>         Model directory (tokenizer + encoder weights)
  --epochs <n>              Number of epochs (default: 10)
  --batch-size <n>          Batch size (default: 16)
  --lr <f>                  Learning rate (default: 1e-4)
  --hidden-size <n>         Encoder hidden size (default: 768)
  --max-seq-len <n>         Max token sequence length (default: 384)
  --checkpoint-every <n>    Save checkpoint every N epochs (0=disabled)
  --split <name>            Dataset split name filter (default: "train")
  --seed <n>                Random seed (default: 42)
  --lora-rank <n>           LoRA rank (default: 0 = disabled)
  --intermediate-size <n>   ModernBERT intermediate_size (default: 1152)
  --backend native|metal|auto   Compute backend (default: auto)
  --grad-accum <n>          Gradient accumulation steps (default: 1)
  --schedule-free           Use Schedule-Free AdamW
  --neftune-alpha <f>       NEFTune noise magnitude (default: 0.0=disabled)
  --xbm-capacity <n>        Cross-Batch Memory capacity (default: 0=disabled)
  --llrd-decay <f>          Layer-wise LR decay (default: 1.0=disabled)
  --lora-plus-ratio <f>     LoRA+ B/A LR ratio (default: 1.0=disabled)
  --length-bucketing        Enable length bucketing
  --bucket-size <n>         Bucket window size (default: 256)
  --mixed-precision         Enable bf16 mixed precision (Metal only)
  --splade                  Enable SPLADE sparse embedding head
  --lambda-splade <f>       SPLADE contrastive loss weight (default: 0.15)
  --lambda-flops <f>        SPLADE FLOPS regularization weight (default: 3e-5)
  --splade-focus-epoch <n>  Epoch when SPLADE activates (default: 4)
  --mrl                     Enable Matryoshka Representation Learning
  --mrl-dims <s>            Comma-separated MRL dims (default: "768,256,128")
  --resume-from <path>      Resume training from a checkpoint file
  --save-optimizer-state    Save Adam optimizer state alongside each checkpoint
```

### LayoutLMv3 Sequence and Token

These are backend implementation commands. Prefer a `layoutlmv3` recipe unless
you are debugging this family surface directly.

```
usage: train-eval-layoutlmv3-lora-sequence <base_model_dir> <adapter_model_dir>
    <train_jsonl_or_dir> <val_jsonl_or_dir> <out_dir>
    [max_train_examples]   default: 128
    [learning_rate]        default: 0.001
    [max_val_examples]     default: 64
    [epochs]               default: 4
    [layer_name|@layoutlmv3_token_top1|@layoutlmv3_token_top3|@layoutlmv3_sequence_top3]

Flags:
  --max-grad-norm <f>    Gradient norm clipping threshold (default: 1.0)
  --llrd-decay <f>       Layer-wise LR decay factor (default: 1.0=disabled)
  --grad-accum <n>       Gradient accumulation steps (default: 1)
```

Token classification uses identical positional and flag interface (`train-eval-layoutlmv3-lora-token`).

### Reranker Surrogate (`src/train_eval_reranker_lora_surrogate.zig`)

This is a backend command behind the `lora-sft` / `qlora-sft` reranker recipe.

```
usage: train-eval-reranker-lora-surrogate <model-dir> <adapter-dir>
    <head-dir-or-file> <train-jsonl-or-dir> <eval-jsonl-or-dir> <out-dir>
    [train-split] [eval-split]

Flags:
  --backend auto|native|metal   Compute backend (default: auto)
  --max-examples <n>        Max training examples (default: 128)
  --epochs <n>              Number of epochs (default: 1)
  --learning-rate <f>       Learning rate (default: 0.001)
  --layer-name <name>       Scope to a specific layer name
  --max-grad-norm <f>       Gradient norm clipping threshold (default: 1.0)
  --schedule-free           Enable Schedule-Free AdamW
```

Note: gradient accumulation is defined in `SurrogateTrainOptions` but the surrogate CLI does not yet expose `--grad-accum`.

### ColQwen2 LoRA Bundle

This is the backend train/eval command behind `vlm-retrieval` recipes.

```
usage: train-eval-colqwen2-lora-bundle <base_model_dir> <adapter_model_dir>
    <prepared_inputs_json> <out_dir> [options]

Flags:
  --lr, --learning-rate <f>     Learning rate (default: 0.001)
  --max-examples <n>            Max examples per epoch (default: 32)
  --epochs <n>                  Number of epochs (default: 1)
  --layer-name, --layer <str>   Scope to layer name or @colqwen2_focus_top3
  --max-grad-norm <f>           Gradient norm clipping threshold (default: 1.0, 0=disabled)
  --grad-accum <n>              Gradient accumulation steps (default: 1)
  --llrd-decay <f>              Layer-wise LR decay factor (default: 1.0=disabled)
  --schedule-free               Enable Schedule-Free AdamW (default: false)
```

### GLiNER2 LoRA Bundle

This is the backend train/eval command behind `lora-sft` / `qlora-sft`
GLiNER2 recipes.

Uses pre-cached top-layer boundary representations produced by `prepare-gliner2-top-layer-boundary-cache` (no live inference required at train time). Training loop uses cached `hidden_in` tensors from `CachedBoundarySummary`, computes mean-pooled span representations, runs MSE loss against deterministic probe targets, and applies AdamW with optional LLRD, norm clipping, and gradient accumulation.

**Prerequisites:** Run `prepare-gliner2-top-layer-boundary-cache` on train and eval datasets first.

```
usage: train-eval-gliner2-lora-bundle <base_model_dir> <adapter_model_dir>
    <train_cache.json> <eval_cache.json> <out_dir> [options]

Flags:
  --lr, --learning-rate <f>   Learning rate (default: 0.0001)
  --max-examples <n>          Max training examples (default: 0 = all)
  --epochs <n>                Number of epochs (default: 1)
  --layer-name, --layer <str> Scope to a specific layer name
  --max-grad-norm <f>         Gradient norm clipping threshold (default: 1.0, 0=disabled)
  --grad-accum <n>            Gradient accumulation steps (default: 1)
  --llrd-decay <f>            Layer-wise LR decay factor (default: 1.0=disabled)

example: train-eval-gliner2-lora-bundle /tmp/gliner2 /tmp/lora \
           /tmp/train_cache.json /tmp/eval_cache.json /tmp/out \
           --lr 0.0001 --epochs 3 --max-grad-norm 1.0 --grad-accum 4 --llrd-decay 0.9
```

Outputs:
- `<out_dir>/adapter_model.safetensors`
- `<out_dir>/train_eval_lora_report.json` — `eval_mse_before`, per-epoch `train_metrics`, `eval_mse_after`

### Gemma4 LoRA

This section documents the Gemma family backend commands. Prefer a recipe for
normal use:

```sh
termite finetune run recipe_gemma4_lora.json
termite finetune run recipe_gemma4_lora.json --dry-run
```

Three-step pipeline: dataset preparation, tokenization, then train/eval. Gemma now supports two training modes:
- `--trainer surrogate`: legacy bounded surrogate-gradient training
- `--trainer autodiff`: real token-level causal-LM LoRA training for Gemma text configs, including Gemma4 sliding-attention and shared-KV variants
- Multimodal Gemma prepared inputs can now be produced through `prepare-gemma4-lora-inputs --gguf-projector <projector.gguf>`, which expands image/audio placeholder runs before tokenization and records media references for the real-autodiff trainer

Adapters are saved in PEFT-compatible format (`task_type = "CAUSAL_LM"`).

#### Chat Dataset Contract

Gemma text finetuning now accepts a chat-native dataset schema, `gemma_chat/v1`, in addition to the legacy flat row formats.

Each `gemma_chat/v1` JSONL row may include:
- `schema`: must be `gemma_chat/v1`
- `id`: optional row identifier
- `split`: optional split name
- `messages`: ordered turns with `role` in `system|user|assistant|tool`
- `tools`: optional tool specifications for provenance/documentation
- `metadata`: optional fields such as `policy_version` and `source`

Assistant turns may include `tool_calls`, and tool turns may include `tool_call_id` plus `name`. On the Gemma rendering path:
- assistant text and assistant tool-call blocks are supervised
- tool responses are injected as context and masked from labels
- system text is merged into the first user turn, matching Gemma prompt conventions

This gives Gemma a single conversation contract for:
- plain SFT chat
- tool-calling traces
- multi-turn assistant/tool handoffs

**Step 1 — prepare text dataset:**
```
usage: prepare-gemma4-text-dataset <dataset-path> <split|-> <out_csv_path> <out_summary_path> [max_examples]
```

Accepted row shapes:
- `gemma_chat/v1` rows with `messages`
- legacy instruction rows with `prompt` or `instruction`, optional `input`, and `response|completion|output`
- legacy completion rows with `text`

Legacy rows are coerced internally into the chat contract before tokenization, so existing datasets continue to work.

**Step 1b — prepare multimodal dataset:**
```
usage: prepare-gemma4-multimodal-dataset <dataset-path> <split|-> <out_csv_path> <out_summary_path> [max_examples]
```

The multimodal preparation path now accepts the same `messages` contract for text turns and can extract image paths from:
- top-level `image_path|image|image_file|file_name`
- top-level `images`
- `messages[].content[]` image parts in `gemma_chat/v1` rows

The current multimodal converter still materializes a flat CSV (`image,prompt,response`) artifact, so it shares the conversation schema at ingestion time while keeping the existing output contract.

**Step 2 — tokenize examples:**
```
usage: prepare-gemma4-lora-inputs <model_dir> <dataset_path> <split|-> <out_summary_json> [options]

  --max-examples N    Maximum number of examples to prepare (default: 0 = all)
  --max-seq-len N     Maximum sequence length in tokens (default: 512)
```

#### Prepared Inputs v2/v3

`prepare-gemma4-lora-inputs` now emits a richer prepared-input summary for chat/tool datasets. Each prepared example records:
- legacy prompt/response token views for compatibility with the current surrogate trainer
- full rendered `input_ids`
- `labels` with non-assistant and tool-response tokens masked to the ignore label
- `num_input_tokens`
- `num_supervised_tokens`
- `turn_count`
- `has_tool_calls`
- `has_tool_results`
- optional `policy_version`

The summary also records aggregate metadata:
- `schema_version = "gemma4_prepared/v2"` for text/chat datasets
- `schema_version = "gemma4_prepared/v3"` for multimodal datasets with image/audio placeholder accounting
- `max_input_tokens`
- `max_supervised_tokens`
- `examples_with_tool_calls`
- `examples_with_tool_results`
- `examples_with_multiturn`

The loader only accepts these supported prepared schemas and rejects unknown versions at load time.

This prepared artifact now feeds both Gemma trainer modes. The surrogate path still reads the legacy prompt/response views, while the autodiff path trains directly from `input_ids + labels`.

**Optional — materialize teacher targets:**
```
usage: materialize-gemma4-teacher-targets <base_model_dir> <prepared_inputs_json> <out_summary_json> [options]

  --top-k N              Teacher tokens per row (default: 8)
  --temperature F        Temperature applied before top-k softmax (default: 1.0)
  --max-examples N       Maximum examples to materialize (default: 0 = all)
  --backend native|metal   Teacher inference backend (default: native)
```

This tool runs the full Gemma4 teacher model over prepared inputs and writes sparse row-major `teacher_top_k_token_ids` and `teacher_top_k_probs` into the output prepared-input JSON. For multimodal prepared inputs, pass `--gguf-projector <projector.gguf>` unless the prepared summary records a valid projector path. The autodiff trainer consumes those soft targets when present, which is the first distillation path for recursive LoRA compression.
Teacher probabilities are produced after applying `--temperature`, and the trainer applies the standard distillation `T^2` loss scale from each example's `teacher_temperature`.

**Optional — materialize compressed recursive base:**
```text
usage: materialize-gemma4-recursive-base <base_model_dir> <recursive_adapter_dir> <out_dir> [options]
```

The build step is `zig build materialize-gemma4-recursive-base -- <base_model_dir> <recursive_adapter_dir> <out_dir>`. It writes a compressed recursive base `model.safetensors` containing non-layer tensors plus only the physical shared layer block recorded in the recursive adapter metadata. The output also includes copied HF support files and `recursive_lora_base_config.json` with tensor counts, byte sizes, and compression ratio. The copied `config.json` keeps the original logical layer count; the recursive adapter metadata remains the runtime contract for mapping logical layers to physical tensors.

For the recursive compression path, the bounded smoke workflow is:

```text
usage: run-gemma4-recursive-lora-smoke-workflow <base_model_dir> <output_root> [options]
```

The corresponding build step is `zig build run-gemma4-recursive-lora-smoke-workflow -- <base_model_dir> <output_root> ...`. It bootstraps a recursive LoRA adapter, materializes teacher targets, trains/evaluates with the autodiff trainer, and validates that recursive and teacher metadata survive the round trip.
Successful real runs write `<output_root>/recursive_smoke_results.json` with adapter sizes, before/after loss, teacher coverage, elapsed time, and supervised-token throughput.
For Gemma4 E2B, use `--recursive-shared-block-size 5`; the text stack has 35 layers and the local/full attention pattern repeats every five layers. The current recursive smoke defaults to attention-only targets (`q_proj,k_proj,v_proj,o_proj`) because E2B MLP weights become double-wide after layer 15.

For baseline-vs-recursive comparison sweeps:

```text
usage: run-gemma4-recursive-lora-sweep <base_model_dir> <output_root> [options]
```

The build step is `zig build run-gemma4-recursive-lora-sweep -- <base_model_dir> <output_root> ...`. It runs normal LoRA baselines plus recursive variants across rank, shared-block-size, and teacher-temperature grids, then writes `<output_root>/recursive_lora_sweep_comparison.json`.

To turn a completed sweep into a pass/fail recommendation:

```text
usage: analyze-gemma4-recursive-lora-sweep <comparison_json> <out_dir> [options]
```

The build step is `zig build analyze-gemma4-recursive-lora-sweep -- <comparison_json> <out_dir> ...`. It writes `recursive_lora_sweep_decision.json` and `.md` using explicit loss, adapter-size, compressed-base-size, teacher-coverage, and throughput thresholds.

**Step 3 — train/eval:**
```
usage: train-eval-gemma4-lora-bundle <base_model_dir> <adapter_model_dir>
    <prepared_inputs_json> <out_dir> [options]

Flags:
  --trainer auto|surrogate|autodiff   Trainer implementation (default: auto)
  --lr, --learning-rate <f>     Learning rate (default: 0.001)
  --max-examples <n>            Max examples per epoch (default: 32)
  --epochs <n>                  Number of epochs (default: 1)
  --layer-name, --layer <str>   Scope to layer name
  --max-grad-norm <f>           Gradient norm clipping threshold (default: 1.0, 0=disabled)
  --grad-accum <n>              Gradient accumulation steps (default: 1)
  --llrd-decay <f>              Surrogate-only layer-wise LR decay (default: 1.0=disabled)
  --schedule-free               Surrogate-only Schedule-Free AdamW (default: false)
  --backend auto|metal|native       Compute backend (default: auto)
```

Trainer mode behavior:
- `auto` selects `autodiff` for currently supported Gemma text configs and falls back to `surrogate` for unsupported architecture variants
- `autodiff` currently supports Gemma4 sliding-attention, per-layer RoPE, omitted `v_proj` on full-attention layers, and shared-KV donor reuse
- `autodiff` also supports multimodal Gemma training when the prepared artifact includes media references and the train/eval command is given `--gguf-projector <projector.gguf>`; the trainer feeds projected soft-token embeddings through an internal `__input_embeddings` placeholder while continuing to train only the Gemma decoder/LoRA path
- `autodiff` still rejects MoE, PLE, and non-RoPE Gemma configs
- `autodiff` uses token-level next-token cross-entropy over the prepared `labels` mask, including assistant tool-call output while masking tool responses
- `autodiff` reuses the incoming Gemma PEFT adapter bundle as initialization and writes the trained adapters back out in the same bundle format

Default LoRA target modules: `q_proj`, `k_proj`, `v_proj`, `o_proj`, `gate_proj`, `up_proj`, `down_proj`

Outputs:
- `<out_dir>/adapter_model.safetensors` (PEFT format)
- `<out_dir>/adapter_config.json` with `task_type = "CAUSAL_LM"`
- `<out_dir>/train_eval_report.json` — `before`, per-epoch `epoch_history`, `after` metrics for the selected trainer
- `<out_dir>/training_config.json` and `<out_dir>/training_report.json`

---

## Run Contract

The following machine-readable workflow artifacts are emitted by the unified
recipe runner and by the lower-level family smoke-workflow entrypoints.

Contract versions: `run_status/v1`, `training_config/v1`, `training_report/v1`

Applies to:
- `termite finetune run <recipe.json>`
- `termite finetune smoke-fast`
- `run-layoutlmv3-lora-smoke-workflow`
- `run-gliner2-boundary-task-head-smoke-workflow`
- `train-eval-layoutlmv3-lora-sequence`
- `train-eval-layoutlmv3-lora-token`
- `train-eval-colqwen2-lora-bundle`
- `train-eval-gliner2-lora-bundle`
- `train-eval-gliner2-top-layer-boundary-task-head`

### `run_status.json`

Written by smoke workflows.

Required top-level fields: `contract_version`, `status`, `task`, `out_dir`, `resume_from`, `actions`, `derived`, `artifacts`

Required `derived` fields: `outcome_code`, `alerts`, `metric_summary`

Required `artifacts` fields: `report`, `best`, `latest`, `final`

Semantics:
- `status` is one of `running`, `failed`, or `completed`
- `artifacts.report` points to `training_report.json`
- `best` and `latest` point at the workflow's trained artifact dir
- `final` points at the materialized output dir
- `alerts` is currently an empty list placeholder

### `training_config.json`

Written by train/eval entrypoints and smoke workflows.

Required fields: `contract_version`, `artifact_family_version`, `task`, `inputs`, `training` (or workflow-specific config object)

Optional but recommended: `backend_policy`, `distributed`, `run_plan`, `output_root`

Semantics:
- `inputs` records the user-facing source paths
- `training` records bounded hyperparameters and layer selection
- `backend_policy.selected` is the backend actually requested/used by the CLI
- `backend_policy.preferred` is the current termite default for that entrypoint

### `training_report.json`

Written by train/eval entrypoints and smoke workflows.

Required fields: `contract_version`, `artifact_family_version`, `task`, `report` (or `summary`)

Optional but recommended: `backend_policy`, `distributed`

The `report`/`summary` object contains the workflow-specific bounded metrics payload. The contract intentionally does not normalize all model-family metrics into a single schema.

### Compatibility Policy

Stable: every required field listed above.

Current non-goals: `run_actions.json`, deferred quantize/shard promotion, full `shared_cache` and `loader_shared_cache` parity with `gopeft-zig`, family-wide metric normalization across all trainers.

---

## Key Files

| File | Purpose |
|------|---------|
| `lib/ml/src/graph/autodiff.zig` | Reverse-mode AD, ~25 VJP rules |
| `lib/ml/src/graph/lower.zig` | Fused → primitive lowering via vjp_alternate |
| `lib/ml/src/graph/grad_check.zig` | Finite-difference gradient verification |
| `lib/ml/src/graph/optimizers.zig` | SGD/Adam/AdamW + LR schedules |
| `lib/ml/src/graph/lora.zig` | LoRA adapter injection and merging |
| `lib/ml/src/graph/checkpoint.zig` | Activation checkpointing pass |
| `src/graph/training.zig` | Training step orchestration |
| `src/graph/training_loop.zig` | TrainingWeightStore + TrainingLoop + checkpoints |
| `src/graph/distributed_training.zig` | Data-parallel distributed training |
| `src/bench/training.zig` | Native optimizer / checkpoint benchmark |
| `src/finetune/fused_chunker_train.zig` | Reference fused-chunker training implementation |
| `src/ops/native_compute.zig` | native primitive op implementations |
| `src/graph/interpreter.zig` | Primitive op dispatch |
```

---

## Validation & Testing Plan

### Unit Tests (266 tests, all pass)

24 standalone files under `src/finetune/` exercise individual modules:
LoRA math, preference losses, optimizers, quantization, sequence packing,
chat templates, training guards, Hypura memory management, etc.

### E2E Integration Tests (6 tests)

Each validates the full level-3 pipeline: graph → LoRA injection → autodiff → execution → loss → optimizer step. Uses tiny configs with random weights; asserts loss decreases over 5 training steps.

| Test | Architecture | Params | Head |
|------|-------------|--------|------|
| `test_bert_e2e.zig` | BERT (2L, 4H, H=64) | 37 | MSE |
| `test_qwen2_e2e.zig` | Qwen2 (2L, 4H/2KV, H=32) | 26 | pooled MSE |
| `test_deberta_e2e.zig` | DeBERTa-v3 (2L, 4H, H=64) | 38 | MSE |
| `test_gliner2_e2e.zig` | DeBERTa + NER head (5 classes) | 40 | token CE |
| `test_fused_chunker_e2e.zig` | ModernBERT + boundary MLP | 39 | 2-class CE |
| `test_layoutlmv3_e2e.zig` | LayoutLMv3 + token cls (5 classes) | 43 | token CE |

### Real-World Validation Plan

**Phase 1 — BERT reranker (smallest model, fastest iteration)**
- Model: `bert-base-uncased` (110M params)
- Data: MS MARCO reranking subset (~1K pairs)
- Goal: loss converges, saved LoRA adapters produce correct scores at inference
- Metric: MRR@10 on eval split
- Compare: wall-clock speed vs HF PEFT on same hardware

**Phase 2 — DeBERTa + GLiNER2 (NER)**
- Model: `deberta-v3-base` (184M params)
- Data: CoNLL-2003 or custom NER dataset
- Goal: F1 on entity spans matches or exceeds HF PEFT baseline
- Validates: disentangled attention (C2C + C2P + P2C) gradients on real data

**Phase 3 — Qwen2 decoder (large vocab, causal)**
- Model: `Qwen2-0.5B` or `Qwen2-1.5B`
- Data: small instruction-tuning dataset (Alpaca subset)
- Goal: perplexity on held-out set decreases; generated text is coherent
- Validates: GQA fan-out, RoPE, SwiGLU, chunked CE on 150K+ vocab

**Phase 4 — LayoutLMv3 (document AI)**
- Model: `layoutlmv3-base` (133M params)
- Data: FUNSD or custom document field-extraction dataset
- Goal: token-level F1 on field labels
- Validates: 2D bbox embeddings, attention masking on padded layouts

**Phase 5 — ColQwen2 multimodal retrieval**
- Model: ColQwen2-VL (text-only MVP, then with vision tower)
- Data: document retrieval pairs with relevance scores
- Goal: late-interaction retrieval scores correlate with ground truth

### Known issues likely to surface during validation
- Weight name mismatches between graph builders and real HF checkpoints
- Tokenizer integration (CLI driver uses placeholder char-level; needs HF tokenizer)
- Memory pressure on larger models (good test for Hypura stack)
- `head_dim` edge cases on non-standard model sizes (e.g., Qwen2-7B has head_dim=128)

---
