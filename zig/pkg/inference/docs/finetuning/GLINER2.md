# GLiNER2 Finetuning State

This file summarizes the current GLiNER2 finetuning state in `antfly-inference-zig`.

It is intentionally focused on:
- what is implemented
- what was tried locally
- what changed while making it work
- what remains incomplete

## Scope

All work described here was implemented in `antfly-inference-zig`, not `gopeft-zig`.

`gopeft-zig` was used as:
- a reference implementation
- a source of local datasets
- a source of workflow ideas and artifact contracts

The active `antfly-inference-zig` GLiNER2 finetuning surface lives primarily in:
- `src/finetune/gliner2.zig`
- `src/finetune/gliner2_boundary.zig`
- `src/finetune/gliner2_data.zig`
- `src/finetune/text_encoder_boundary.zig`
- `src/finetune/graph_bridge.zig`
- `src/finetune/train/run_gliner2_boundary_task_head_smoke_workflow.zig`

## Current Implemented Surface

### 1. LoRA Artifact Layer

Implemented in `src/finetune/gliner2.zig`:
- LoRA bootstrap
- checkpoint inspection
- adapter inspection
- LoRA bundle load/save
- merged-checkpoint materialization
- tokenizer/supporting-artifact copy-through
- passthrough preservation for:
  - `span_rep.*`
  - `count_embed.*`
- sidecar boundary-head artifact copy-through during materialization

Current artifact-level CLI surface:
- `bootstrap-gliner2-lora`
- `inspect-gliner2-checkpoint`
- `inspect-gliner2-lora-bundle`
- `materialize-gliner2-lora`

### 2. Dataset / Task Data Layer

Implemented in `src/finetune/gliner2_data.zig`:
- JSONL loading
- optional split handling
- label filtering
- entity-type filtering
- tokenizer-backed encoding
- span-target shaping
- batch workspace support
- dataset stats

Supporting CLI:
- `inspect-gliner2-dataset`

### 3. Boundary Cache / Replay Layer

Implemented across:
- `src/finetune/text_encoder_boundary.zig`
- `src/finetune/gliner2_boundary.zig`

This layer supports:
- capture of hidden state before the last `k` DeBERTa layers
- persisted top-layer boundary caches
- replay of cached top-layer boundaries
- bounded eval over replayed logits

Supporting CLI:
- `prepare-gliner2-top-layer-boundary-cache`

### 4. Bounded Task-Head Training

Implemented in `src/finetune/gliner2_boundary.zig`:
- bounded scalar boundary head train/eval
- bounded label-aware boundary task-head train/eval
- saved-head reload/eval
- saved task-head reload/eval

Supporting CLIs:
- `eval-gliner2-top-layer-boundary-head`
- `train-eval-gliner2-top-layer-boundary-head`
- `eval-gliner2-top-layer-boundary-task-head`
- `train-eval-gliner2-top-layer-boundary-task-head`

### 5. Smoke Workflow

Implemented in `src/finetune/train/run_gliner2_boundary_task_head_smoke_workflow.zig`.

This workflow chains:
1. train boundary-cache prep
2. eval boundary-cache prep
3. adapter clone into `trained/`
4. boundary task-head train/eval
5. trained bundle inspect
6. merged materialization
7. one JSON workflow report

Supporting CLI:
- `run-gliner2-boundary-task-head-smoke-workflow`

## What Was Tried Locally

### Real Local Model

Used local GLiNER2 base model:
- `/Users/tim/.cache/huggingface/hub/models--fastino--gliner2-base-v1/snapshots/283f4af5e598631a5352b8c388b6906853146f07`

### Real Local Datasets

Used local JSONL data from `gopeft`:
- `/Users/tim/Documents/af/gopeft/data/gliner2_train.jsonl`
- `/Users/tim/Documents/af/gopeft/data/gliner2_val.jsonl`

Entity types used in local smoke:
- `person`
- `organization`
- `location`

### Local Runs That Completed

The following paths were exercised successfully on this machine:

1. Bootstrap adapter
- output root: `/tmp/termite-gliner2-smoke/bootstrap`

2. Tiny train boundary cache
- output file: `/tmp/termite-gliner2-smoke/train_boundary_1.json`

3. Tiny eval boundary cache
- output file: `/tmp/termite-gliner2-smoke/eval_boundary_1.json`

4. Tiny bounded GLiNER2 boundary task-head train/eval
- output dir: `/tmp/termite-gliner2-smoke/task_head_graphruntime`

5. Tiny merged materialization
- output dir: `/tmp/termite-gliner2-smoke/materialized_manual_clean`

6. Tiny full workflow run
- output dir: `/tmp/termite-gliner2-smoke/run2`

The tiny full workflow produced:
- `train_boundary_cache.json`
- `eval_boundary_cache.json`
- `trained/adapter_model.safetensors`
- `trained/gliner2_top_layer_boundary_task_head.json`
- `materialized/model.safetensors`
- `materialized/gliner2_top_layer_boundary_task_head.json`
- `smoke_workflow_report.json`

## Important Bugs Found And Fixed

### 1. Merge Regression In Session Factory

File:
- `src/architectures/session_factory.zig`

Issue:
- the merged tree had an outdated call to `recommendedMlxLazyQuantSharedCacheBudget(...)`
- BLAS/MLX-capable build paths failed once exercised through GLiNER2 finetune commands

Fix:
- updated the callsite to match the new function signature

### 2. Backend Wrapper Leaks

Files:
- `src/finetune/text_encoder_boundary.zig`
- `src/finetune/gliner2_boundary.zig`

Issue:
- `session_factory.getComputeBackend(...)` allocates backend wrapper state
- several GLiNER2 finetune paths acquired those wrappers and never deinitialized them

Fix:
- added missing `compute_backend.deinit()` / `cb.deinit()` calls at the finetune-owned boundaries

### 3. Adapter Config JSON Ownership Leak

File:
- `src/finetune/gliner2.zig`

Issue:
- optional adapter config loading used `parseFromSliceLeaky(...)`
- materialization completed but leaked parsed JSON-backed allocations

Fix:
- switched to parse-then-deep-copy ownership for `AdapterConfig`
- added explicit cleanup for owned adapter-config memory

### 4. Zig 0.16 Strictness Issues

File:
- `lib/ml/src/graph/optimizers.zig`

Issue:
- new targets surfaced `var` vs `const` compile failures under the current Zig dev toolchain

Fix:
- cleaned the surfaced strictness issue

### 5. Graph / Autodiff Regression On Scalar Bounded Training

Primary files:
- `lib/ml/src/graph/autodiff.zig`
- `lib/ml/src/graph/builder.zig`
- `src/finetune/graph_bridge.zig`

Issue:
- bounded GLiNER2 boundary task-head training originally crashed inside the graph runtime
- the failure showed up in scalar-regression-style linear training

What was discovered:
- `broadcast_in_dim` VJP handling was incomplete
- `broadcastToShape(...)` behavior was insufficient for scalar-reduction cases
- `dot_general` VJP needed to tolerate scalar adjoints for contraction outputs

Fixes applied:
- corrected `broadcast_in_dim` VJP
- hardened `broadcastToShape(...)`
- hardened `dot_general` VJP for scalar-adjoint cases
- added a scalar regression gradient test
- cleaned stale test type mismatches in `builder.zig`

Result:
- the tiny GLiNER2 bounded task-head run now succeeds on the generic graph runtime again

## What Was Temporarily Tried Along The Way

Before the graph fix was complete, a temporary bounded fallback was tried in `graph_bridge.zig`:
- direct MSE gradient updates for the scalar linear regressor path

That was useful to keep GLiNER2 moving and to separate:
- graph-runtime bugs
from
- GLiNER2 task-head logic bugs

After the real graph/autodiff fixes landed, the bounded linear regressor path was switched back onto generic `training.trainStep(...)`.

## Current Honest Status

### Working Now

- GLiNER2 LoRA artifact lifecycle
- GLiNER2 dataset/task-data loading
- boundary-cache preparation
- replay/eval on cached boundaries
- bounded boundary task-head train/eval
- merged materialization
- tiny full smoke workflow with real local model + real local dataset

### Not Yet Full Parity

Still not implemented:
- exact native GLiNER2 task-head backward/update parity
- exact DeBERTa-backbone GLiNER2 LoRA backward/update parity
- full `gopeft-zig` GLiNER2 taskgraph parity
- larger local workflow validation on this smaller machine
- full native distributed GLiNER2 finetuning ownership in antfly inference workflow code

## Practical Interpretation

GLiNER2 finetuning in `antfly-inference-zig` is now:
- real
- Antfly inference-owned
- exercised on actual local artifacts

But it is still a bounded finetuning stack centered on:
- boundary caches
- replayed top layers
- sidecar/task-head training

It is not yet the full exact-native GLiNER2 backbone/task-head finetuning system that `gopeft-zig` taskgraph research workflows point toward.

## Recommended Next Steps

1. Keep the repaired graph path and remove any remaining bounded special casing only after broader scalar-head coverage is verified.
2. Extend GLiNER2 from boundary task-head training toward native task-head replay/update parity.
3. Then move from task-head parity to exact backbone/LoRA update parity over the replayed DeBERTa top block.
4. Only after that, expand local smoke sizes and distributed finetuning ownership.
