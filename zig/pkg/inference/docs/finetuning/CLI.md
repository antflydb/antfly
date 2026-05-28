# Fine-Tuning CLI Refactor

## Goal

Antfly inference fine-tuning should have one durable user-facing CLI and one typed
programmatic workflow API. The build graph should build and run those entry
points, not define the product surface.

The target shape is:

- `antfly inference finetune ...` is the public interface.
- `src/finetune/cli/` only parses arguments and dispatches.
- `src/finetune/workflows/` composes typed operations directly.
- `zig build` exposes a small number of developer entry points.
- Tests call typed functions or fixed test roots, not subprocess chains.

There is no legacy-compatibility requirement for the existing many-step
`zig build <finetune-tool>` surface. The refactor should optimize for the
long-term shape, not alias preservation.

## Current Problem

Fine-tuning currently mixes several concerns:

- many standalone build steps for tools, train/eval runners, eval commands, and
  workflow runners
- workflow runners that shell back out to `zig build`
- generated pilot datasets living near fixed test fixtures
- per-command argument parsing embedded directly in train/tool implementation
  files
- build-step names acting as the CLI contract

This makes it hard to answer basic questions:

- Which commands are public?
- Which commands are internal workflow pieces?
- Which datasets are canonical fixtures versus generated smoke data?
- How should a test run the same logic as a CLI without spawning a command?
- Where should a new model family's fine-tuning pipeline be added?

The root issue is not naming. The build system, CLI, testdata generation, and
workflow engine are sharing one surface.

## Target CLI

The public surface should be hierarchical and task-oriented:

```sh
antfly inference finetune dataset inspect <family> ...
antfly inference finetune dataset generate <generator> ...
antfly inference finetune dataset prepare <family> ...

antfly inference finetune adapter bootstrap <family> ...
antfly inference finetune adapter inspect <family> ...
antfly inference finetune adapter materialize <family> ...
antfly inference finetune adapter compose ...

antfly inference finetune train <family-or-task> ...
antfly inference finetune eval <family-or-task> ...

antfly inference finetune workflow <workflow-name> ...
```

The unified dispatcher is now the only `antfly inference finetune` entry from the
top-level CLI. It routes recipe execution through `antfly inference finetune run` and
`antfly inference finetune smoke-fast`, routes hierarchical commands through
`src/finetune/cli/root.zig`, and still accepts legacy tool names as
compatibility wrappers:

```sh
antfly inference finetune run /tmp/recipe.json
antfly inference finetune dataset prepare colqwen2 /models/colqwen2 /data /tmp/examples.jsonl /tmp/prepared.json
antfly inference finetune prepare-colqwen2-inputs /models/colqwen2 /data /tmp/examples.jsonl /tmp/prepared.json
```

Concrete examples:

```sh
antfly inference finetune dataset generate gemma4-pilot /tmp/pilot.jsonl --count 1000 --split train
antfly inference finetune dataset prepare gemma4-lora /models/gemma4 /tmp/pilot.jsonl train /tmp/prepared.json

antfly inference finetune adapter bootstrap gemma4 /models/gemma4 /tmp/adapter --rank 16 --alpha 32 --target-preset all-linear
antfly inference finetune train gemma4-lora /models/gemma4 /tmp/adapter /tmp/prepared.json /tmp/out --trainer autodiff --backend mlx
antfly inference finetune adapter materialize gemma4 /models/gemma4 /tmp/out /tmp/merged

antfly inference finetune workflow gemma4-pilot text /models/gemma4 /tmp/pilot-run --count 1000 --backend mlx
antfly inference finetune workflow recursive-lora-smoke /models/gemma4 /tmp/recursive-smoke --count 16
antfly inference finetune workflow gliner2-entity-cleanup-smoke /models/gliner2 /tmp/adapter train.jsonl eval.jsonl /tmp/out
```

## Production Support Matrix

Use this matrix as the PR gate for declaring the unified CLI production ready.

| Family / Task | Dataset | Adapter | Train/Eval | Materialize | Required Backend Lane |
| --- | --- | --- | --- | --- | --- |
| Gemma4 text LoRA | prepare + teacher top-k | bootstrap/inspect | supervised, autodiff, recursive preference paths | LoRA merge + recursive base | native/BLAS CPU smoke, MLX optional |
| Gemma4 multimodal LoRA | multimodal prepare + pilot generation | bootstrap/inspect | autodiff with image/audio embeddings | LoRA merge | native/BLAS CPU smoke, MLX optional |
| ColQwen2 / Qwen2VL | multimodal prepared inputs | bootstrap/inspect | LoRA train/eval bundle | LoRA merge | native/BLAS CPU smoke |
| Qwen3.5 / Chandra OCR text-only | text SFT/DPO/GRPO JSONL; dynamic image preparation pending | bootstrap/inspect | Qwen autodiff trainer for text SFT/DPO/GRPO | adapter save; merged materialization pending | native/BLAS CPU smoke required, MLX/Metal smoke pending |
| GLiNER2 | dataset inspect + boundary caches | bootstrap/inspect | LoRA, autodiff, boundary heads | LoRA merge | native/BLAS CPU smoke |
| LayoutLMv3 | document token/sequence data | bootstrap/inspect | token and sequence train/eval | checkpoint materialize | native/BLAS CPU smoke |
| Reranker | dataset inspect + pooled/top-layer caches | bootstrap/inspect | head and LoRA surrogate paths | head and LoRA materialize | native/BLAS CPU smoke |
| Fused chunker | dataset fixtures | n/a | train/eval roots | checkpoint output | native/BLAS CPU smoke |

Optional lanes should prove MLX, Metal, PJRT, ONNX, and quantized export where
the model family actually supports them. Unsupported combinations must fail
with explicit errors rather than falling back silently.

## PR Readiness Checklist

- `antfly inference finetune` reaches the unified dispatcher from `src/main.zig`.
- `antfly inference finetune run` and `antfly inference finetune smoke-fast` keep the recipe
  engine stable.
- Each command has a unique canonical tuple
  `<domain, action, subject>` and a unique legacy alias.
- Legacy aliases remain wrappers, not a second product surface.
- `zig build finetune -- <args>` runs the same binary and dispatcher as the
  installed CLI.
- `zig build test-finetune` and the root antfly inference test cover the dispatcher,
  recipe plan manifests, and synthetic smoke fixtures.
- CPU-only smoke paths are mandatory in CI; accelerator paths are separate
  opt-in lanes with clear skip behavior.

The CLI names should describe the user's intent first and the implementation
second. For example, `adapter materialize gemma4` is clearer than a top-level
`materialize-gemma4-lora` command because it sits next to `bootstrap`,
`inspect`, and `compose`.

## Build Steps

The build graph should stop being the fine-tuning command namespace.

Keep a small developer surface:

```sh
zig build finetune -- <args passed to antfly inference finetune>
zig build test-finetune
```

Optional narrowly scoped test steps are acceptable when they are real test
roots:

```sh
zig build test-finetune-data
zig build test-finetune-gemma4
zig build test-finetune-gliner2
```

Avoid adding build steps for every tool, trainer, materializer, cache preparer,
or workflow. Those belong under `antfly inference finetune`.

## Programmatic Contract

Every operation should expose a typed API. The CLI should be a thin wrapper
around that API.

Use this shape for tools, trainers, evaluators, and workflows:

```zig
pub const Options = struct {
    // command-specific inputs
};

pub const Result = struct {
    // stable summary suitable for JSON reports and tests
};

pub fn run(ctx: RunContext, opts: Options) !Result {
    // implementation
}
```

The `main()` for a command should do only four things:

1. Parse CLI arguments into `Options`.
2. Build a `RunContext`.
3. Call `run(ctx, opts)`.
4. Render output, status, and artifacts.

Workflow code should call these `run()` functions directly. It should not spawn
`zig build`, `termite`, or another subprocess for in-repo operations.

## Run Context

Use one shared context for all fine-tuning operations:

```zig
pub const RunContext = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    cwd: std.fs.Dir,
    artifact_writer: ArtifactWriter,
    backend_policy: BackendPolicy,
    logger: ?Logger = null,
};
```

The exact fields can follow existing Antfly inference conventions, but the important
properties are:

- paths resolve consistently
- output writing is centralized
- backend selection is explicit
- tests can provide temporary directories and deterministic contexts
- workflow steps can share context without reconstructing process state

## Source Layout

Organize by responsibility first, then by model family:

```text
src/finetune/
  core/
    run_context.zig
    contracts.zig
    artifact_writer.zig
    training_budget.zig
    training_guards.zig

  data/
    jsonl_resolve.zig
    streaming_dataset.zig
    document_data.zig
    gemma_chat_data.zig
    generators/

  adapters/
    lora.zig
    peft.zig
    lora_adapter_set.zig
    safetensors_checkpoint.zig
    compose.zig

  trainers/
    real_autodiff_trainer.zig
    graph_bridge.zig
    graph_input_binder.zig
    graph_weight_bridge.zig
    optimizers_ext.zig

  families/
    gemma4/
      data.zig
      adapter.zig
      prepare.zig
      train.zig
      eval.zig
      recursive_lora.zig
    gliner2/
    reranker/
    colqwen2/
    layoutlmv3/
    fused_chunker/

  workflows/
    gemma4_pilot.zig
    gemma4_recursive_lora_smoke.zig
    gemma4_recursive_lora_sweep.zig
    gliner2_entity_cleanup_smoke.zig
    gliner2_boundary_task_head_smoke.zig
    layoutlmv3_lora_smoke.zig

  cli/
    root.zig
    dataset.zig
    adapter.zig
    train.zig
    eval.zig
    workflow.zig

  test/
```

The families do not need identical internals, but they should expose consistent
operation names where possible:

- `prepare`
- `bootstrapAdapter`
- `inspectAdapter`
- `materializeAdapter`
- `train`
- `eval`

## Workflow Rules

A workflow is a typed composition of operations. For example, the Gemma4 pilot
workflow should directly call:

1. dataset generator or dataset loader
2. adapter bootstrap
3. input preparation
4. optional teacher target materialization
5. train/eval
6. artifact validation and summary writing

It should not construct command arrays like:

```zig
.{ "zig", "build", "prepare-gemma4-lora-inputs", "--", ... }
```

Subprocesses should be reserved for external tools outside this Zig package.

## Artifact Contract

Keep the existing idea of explicit run artifacts, but make it a first-class
fine-tuning contract rather than ad hoc JSON per command.

Every train or workflow run should write:

```text
<out_dir>/
  run_status.json
  training_config.json
  training_report.json
```

When applicable:

```text
<out_dir>/
  prepared.json
  prepared.teacher.json
  adapter_model.safetensors
  adapter_config.json
  eval_report.json
  workflow_report.json
```

The reports should include:

- command or workflow name
- artifact contract version
- model family
- selected backend
- input paths and fingerprints where useful
- max examples, epochs, learning rate, and trainer mode
- before/after metrics
- output artifact paths
- validation failures or alerts

CLI output can be human-readable, but persisted reports should be stable JSON.

## Testdata Policy

Separate fixed fixtures from generated smoke data.

Use fixed fixtures under:

```text
pkg/inference/testdata/finetune/
  gemma4/
    smoke_train.jsonl
    smoke_eval.jsonl
  entity_cleanup/
    smoke_train.jsonl
    smoke_eval.jsonl
  reranker/
  gliner2/
```

Use deterministic generators for pilot data, but do not treat their outputs as
canonical checked-in fixtures unless a test specifically needs a frozen sample.

Generator commands belong under:

```sh
antfly inference finetune dataset generate ...
```

Tests should prefer fixed small fixtures. Real-model pilots should be explicit
integration workflows, not normal unit test steps.

## Testing Strategy

Use three tiers:

1. Unit tests for parsers, data loaders, adapter metadata, loss helpers, and
   artifact contracts.
2. Bounded integration tests using tiny fixed fixtures and synthetic or minimal
   model data.
3. Real-model workflows that are manually or CI-gated by model availability and
   backend support.

Tests should call typed `run()` functions where possible. CLI tests should be
limited to parser and dispatch behavior.

## Migration Order

Because there is no legacy support requirement, migrate toward the target
surface directly:

1. Add `src/finetune/core/run_context.zig`.
2. Add `src/finetune/cli/root.zig` and wire `antfly inference finetune`.
3. Convert the Gemma4 path first:
   - dataset generation
   - input preparation
   - adapter bootstrap/inspect/materialize
   - train/eval
   - pilot workflow
   - recursive LoRA smoke and sweep workflows
4. Remove Gemma4 workflow subprocess calls and replace them with direct typed
   calls.
5. Replace per-tool Gemma4 build steps with `zig build finetune -- ...`.
6. Move fixed fixtures to `testdata/finetune/...`.
7. Convert GLiNER2 workflows and tools.
8. Convert reranker, ColQwen2, LayoutLMv3, and fused chunker.
9. Collapse `build/finetune/tools.zig` and `build/finetune/workflows.zig` once
   the CLI owns the surface.
10. Keep only focused `test-finetune*` build steps.

## Non-Goals

- Preserve every existing `zig build <finetune-tool>` name.
- Keep subprocess-based workflow composition.
- Make generated pilot datasets the default unit-test fixture source.
- Force every model family into identical internals.
- Hide backend-specific behavior behind vague command names.

## Design Principle

Fine-tuning should have one public command tree and many typed internal
operations. The command tree should express user intent. The typed operations
should express reusable implementation boundaries. The build graph should only
assemble and run those entry points.
