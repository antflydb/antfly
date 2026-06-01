# Antfly inference Model Tasks

## Goal

Antfly inference should install models once, infer what they can be used for, and route
requests by task rather than by directory layout.

The current task-scoped layout:

```text
~/.antfly/inference/models/generators/<owner>/<name>
~/.antfly/inference/models/embedders/<owner>/<name>
...
```

does not scale well once a single model can serve multiple endpoints.

The target design is a canonical flat install layout:

```text
~/.antfly/inference/models/<owner>/<name>
```

with task metadata stored alongside the model and used by discovery, listing,
lazy pulls, and request-time resolution.

## Why "Tasks" Instead Of "Capabilities"

`tasks` is the user-facing concept:

- `generate`
- `embed`
- `rerank`
- `recognize`
- `classify`
- `read`
- `transcribe`
- `rewrite`
- `extract`

That is what users ask antfly inference to do, what API routes represent, and what
`/ai/v1/models` should advertise.

The word `capabilities` is still useful internally for narrower features inside
one task, for example:

- tool calling
- vision inputs
- audio inputs
- relation extraction
- structured extraction

But the top-level routing metadata should be called `tasks` so it is clear that
it answers "which endpoints can this model serve?" rather than "which optional
features might this model have?".

## Proposed Pull Behavior

`antfly inference pull` should:

1. Default bare model refs to Hugging Face.
2. Install into `~/.antfly/inference/models/<owner>/<name>` unless `--models-dir` is set.
3. Infer supported tasks best-effort from the downloaded files and manifestable
   model shape.
4. Accept an optional `--tasks` hint for ambiguous models.
5. Persist the resolved task metadata in the model directory.

Examples:

```bash
antfly inference pull ggml-org/gemma-4-e2b-it-gguf
antfly inference pull ggml-org/gemma-4-e2b-it-gguf --tasks generate,read
antfly inference pull BAAI/bge-small-en-v1.5 --tasks embed
```

The intent is:

- automatic inference is the default path
- `--tasks` is a hint or override for ambiguous cases
- one pulled model may advertise multiple tasks

## Proposed Metadata

Antfly inference already has `model_manifest.json`. That should become the canonical
place for persisted task metadata.

Suggested shape:

```json
{
  "type": "generator",
  "tasks": ["generate", "read"],
  "inputs": ["text", "image"],
  "features": ["vision", "tool_calling", "streaming"]
}
```

Notes:

- `type` remains useful as a primary loader hint.
- `tasks` is the routing surface.
- `inputs` describes accepted modalities.
- `features` is optional secondary metadata for task-specific behavior.

If `tasks` is absent, antfly inference should infer them from `type`, model files, and
known architecture rules.

## Task Inference

Inference should be conservative and loader-aware.

Strong signals:

- `model_manifest.json`
- `antfly_inference_bundle.json`
- `config.json`
- `tokenizer_config.json`
- GGUF metadata
- discovered ONNX payload shape
- successful loader-family detection

Examples:

- Gemma/Qwen/Llama decoder models with chat template support:
  - `tasks: ["generate"]`
- Vision-language generators:
  - `tasks: ["generate", "read"]`
- BERT embedding models:
  - `tasks: ["embed"]`
- Cross-encoders:
  - `tasks: ["rerank"]`
- GLiNER models:
  - `tasks: ["recognize"]`
  - optionally `extract` when extraction support is present
- Whisper:
  - `tasks: ["transcribe"]`

Inference should not over-advertise. If antfly inference is not confident that a model
can serve a task, it should leave that task out unless the user explicitly hints
with `--tasks`.

## Discovery And Listing

`/ai/v1/models` should stop relying on task folder names as the source of truth.

Instead it should:

1. Discover flat model directories under `~/.antfly/inference/models/<owner>/<name>`.
2. Load each model manifest.
3. Read or infer `tasks`.
4. Group the model under every task it supports.

That allows a single installed model to appear under multiple API task groups
without duplication on disk.

## Request-Time Resolution

When a request hits an endpoint, antfly inference already knows the requested task from
the route. Resolution should become:

1. If a model was explicitly named, load it from the flat store.
2. Validate that the model supports the requested task.
3. If no model was named, choose a default model that supports that task.
4. If the model is missing and lazy download is enabled, pull it, infer tasks,
   persist metadata, and retry once.

This keeps the lazy e2e flow simple:

- only pull when a model is actually missing
- do not duplicate installs by task
- do not require test-only task-specific paths

## Migration

Short term:

- keep reading existing task-scoped directories
- add flat discovery
- prefer manifest `tasks` over directory-based classification

Long term:

- `antfly inference pull` writes only to the flat layout
- `/ai/v1/models` classifies from manifest tasks
- task-scoped layout becomes compatibility-only

## CLI Direction

Planned user-facing pull syntax:

```bash
antfly inference pull <owner>/<name>
antfly inference pull <owner>/<name> --tasks generate,read
antfly inference pull hf:<owner>/<name>
```

Expected semantics:

- no prefix means `hf:`
- `--tasks` augments or overrides inference
- metadata is written once and reused by server discovery

## Practical Rule

Use `tasks` for top-level routing and model selection.

Use `features` or similar secondary metadata for optional behavior inside a
task.
