# Recursive LoRA Compression Plan

Recursive LoRA is a model-compression path, not a replacement for task LoRA.
The target artifact stores a smaller physical transformer block and runs it
multiple times. Each logical loop gets its own low-rank adapter so the shared
base weights are compact while different depths can still specialize.

## MVP Scope

- Gemma4 first. Other model families can reuse the metadata and graph adapter
  behavior after Gemma4 has a working distillation loop.
- Keep standard PEFT LoRA bundles unchanged.
- Represent recursive bundles with standard adapter tensors plus a
  `recursive_lora` section in `adapter_config.json`.
- Use `loop_{n}` tensor names for loop-specific adapter deltas:
  `model.layers.0.self_attn.q_proj.weight.loop_1.lora_A.weight`.
- Use graph LoRA `sharing = .by_use` for recursive training so repeated uses of
  the same physical weight receive distinct adapter parameters.

## Task List

1. [x] Add recursive metadata and layer-mapping helpers.
2. [x] Teach Gemma4 bootstrap to emit recursive adapter configs and per-loop
   adapter tensors.
3. [x] Teach Gemma4 inspection/load/save to preserve loop-specific adapter
   names.
4. [x] Add graph LoRA per-use injection for shared-weight recursive graphs.
5. [x] Wire recursive Gemma4 autodiff training to select per-use graph adapters.
6. [x] Add a recursive Gemma graph builder that maps logical layer `i` to physical
   layer `i % shared_block_size`.
7. [x] Add shape-compatibility validation for recursive block selection. The
   first implementation assumes repeated physical layers have compatible
   attention and MLP shapes across loops.
8. [x] Add residual-SVD initialization from full-model weights into loop
   adapters.
9. [x] Add sparse top-k teacher targets to the prepared-example training path.
10. [x] Add text teacher-logit materialization from the source full model.
11. [x] Add temperature accounting and teacher-target coverage metrics for
   distillation training.
12. [x] Add a smoke workflow: bootstrap recursive bundle, distill for a few steps,
   inspect artifact, and evaluate perplexity before/after.
13. [x] Add multimodal teacher-target materialization.
14. [x] Add recursive smoke result capture for size/loss/throughput metrics.
15. [x] Add baseline-vs-recursive sweep runner and comparison report.
16. [x] Add sweep promotion gate with explicit loss/size/coverage criteria.
17. [x] Run the recursive smoke workflow against a real local Gemma4 E2B
   checkpoint and archive the resulting metrics.
18. [x] Run a baseline-vs-recursive sweep against the local Gemma4 E2B checkpoint
   and archive the promotion-gate decision.
19. [x] Materialize a compressed recursive base checkpoint containing only
   non-layer tensors plus the physical shared layer block.
20. [x] Include compressed-base size and ratio in smoke/sweep promotion reports.

Items 1-20 are implemented for the Gemma4 E2B compression pipeline. Larger
sweeps and broader model-family support remain follow-up work before this
should be treated as a general-purpose compression pipeline.

## Current Behavior

When `adapter_config.json` contains:

```json
{
  "recursive_lora": {
    "enabled": true,
    "source_num_layers": 4,
    "shared_block_size": 2,
    "loop_count": 2,
    "init_strategy": "average_residual_svd"
  }
}
```

Gemma4 autodiff builds the full logical depth but names layer parameters with
the physical shared block: logical layers `0, 2` both reference
`model.layers.0.*`, and logical layers `1, 3` both reference `model.layers.1.*`.
Graph LoRA then creates loop-specific parameters:

- `model.layers.0.self_attn.q_proj.weight.loop_0.lora_A`
- `model.layers.0.self_attn.q_proj.weight.loop_1.lora_A`

The saved adapter bundle uses Hugging Face-style tensor names with `.weight`
suffixes for the same loop-specific adapters.

## Bootstrap Initialization

Recursive Gemma4 bootstrap validates that every loop target exists and has the
same shape as its physical shared-layer target. It currently supports:

- `--recursive-init residual_svd`
- `--recursive-init average_residual_svd`
- `--recursive-init zero`

`residual_svd` initializes loop adapter `n` from a PiSSA/SVD factorization of
`W_logical - W_physical`, where `W_physical` is the shared layer weight and
`W_logical` is the full source model layer weight for that logical depth.
`average_residual_svd` is accepted as a compatibility name for the same current
behavior. A future materialization step should average each shared block's base
weights first, then factor each logical residual around that averaged weight.

`zero` uses the original deterministic A / zero B bootstrap, which preserves the
unadapted physical shared layer at step zero.

## Distillation Targets

Prepared Gemma4 examples can now carry sparse teacher distributions:

```json
{
  "teacher_top_k": 2,
  "teacher_top_k_token_ids": [7, 8, 9, 10],
  "teacher_top_k_probs": [0.75, 0.25, 0.2, 0.8],
  "teacher_temperature": 1.0
}
```

The arrays are row-major over the prepared `input_ids` sequence:
`teacher_top_k_token_ids.len == teacher_top_k_probs.len` and both lengths must
be divisible by `teacher_top_k`. During training, rows whose label is `-100`
remain masked. Supervised rows use the sparse teacher probabilities as soft
cross-entropy targets, normalized per row and scaled the same way as the
one-hot fallback.

Text prepared inputs can be materialized with the full source teacher model:

```sh
zig build materialize-gemma4-teacher-targets -- \
  /tmp/gemma4-base \
  /tmp/gemma4-prepared.json \
  /tmp/gemma4-prepared.teacher.json \
  --top-k 8 \
  --temperature 2.0 \
  --backend native
```

The materializer runs the full source model over each prepared text example,
extracts logits for every sequence row, applies temperature before the sparse
top-k softmax, and writes a new prepared-input JSON that the trainer can use
directly. The trainer consumes the stored sparse probabilities as soft
cross-entropy targets and applies the usual distillation `T^2` scale by
multiplying teacher target rows by `teacher_temperature * teacher_temperature`.
Train/eval reports include teacher-target coverage fields:
`teacher_examples_seen`, `teacher_supervised_tokens_seen`, and
`mean_teacher_temperature`.

Multimodal examples are supported when the materializer receives a projector:

```sh
zig build materialize-gemma4-teacher-targets -- \
  /tmp/gemma4-base \
  /tmp/gemma4-mm-prepared.json \
  /tmp/gemma4-mm-prepared.teacher.json \
  --gguf-projector /tmp/gemma4-mmproj.gguf \
  --top-k 8 \
  --temperature 2.0
```

The materializer validates the prepared artifact's recorded projector
fingerprint when present, rebuilds the expanded text/media embeddings, runs the
teacher graph with `__input_embeddings`, and writes the same sparse teacher
target fields used by text training. The current reported loss is the active
training loss; separate hard-label CE versus teacher KL reporting can be added
after we introduce a mixed hard/soft objective.

## Smoke Workflow

The bounded text smoke workflow is:

```sh
zig build run-gemma4-recursive-lora-smoke-workflow -- \
  /tmp/gemma4-base \
  /tmp/gemma4-recursive-smoke \
  --count 16 \
  --max-examples 16 \
  --eval-max-examples 8 \
  --recursive-shared-block-size 5 \
  --target-modules q_proj,k_proj,v_proj,o_proj \
  --teacher-top-k 8 \
  --teacher-temperature 2.0
```

The Zig workflow runner delegates to the Gemma4 pilot workflow and forces the recursive
compression/distillation path:

1. Generate or reuse a text chat dataset.
2. Bootstrap a recursive LoRA seed adapter.
3. Prepare tokenized inputs.
4. Materialize teacher top-k targets from the full model.
5. Train/evaluate the recursive adapter.
6. Validate that recursive metadata, teacher targets, teacher metrics, and
   trained adapter metadata are present.

After a real run, the workflow also writes
`<output_root>/recursive_smoke_results.json` with:

- wall-clock runtime
- seed and trained adapter sizes
- base model directory size
- compressed recursive base checkpoint size and source-checkpoint ratio
- before/after eval loss
- final epoch loss
- teacher target coverage
- supervised-token throughput

Use `--dry-run` to print the exact command sequence without requiring a model
checkpoint.

## Sweep Workflow

The comparison sweep runs normal LoRA baselines and recursive LoRA variants
over the same dataset:

```sh
zig build run-gemma4-recursive-lora-sweep -- \
  /tmp/gemma4-base \
  /tmp/gemma4-recursive-sweep \
  --ranks 4,8 \
  --target-modules q_proj,k_proj,v_proj,o_proj \
  --shared-block-sizes 5 \
  --teacher-temperatures 1.0,2.0 \
  --count 16 \
  --max-examples 16
```

After a real run, it writes
`<output_root>/recursive_lora_sweep_comparison.json` with one row per baseline
and recursive variant. Rows include rank, shared block size, teacher
temperature, before/after loss, final epoch loss, teacher coverage, trained
adapter size, compressed base checkpoint ratio, and token throughput when
available.

For Gemma4 E2B checkpoints, use shared block size `5` unless deliberately
testing another divisor. The text stack has 35 layers and its local/full
attention pattern repeats every five layers, so block size `5` gives seven
recursive loops while preserving that pattern. The E2B MLP weights become
double-wide after layer 15, so the current real-checkpoint smoke defaults to
attention-only target modules: `q_proj,k_proj,v_proj,o_proj`.

Then apply the promotion gate:

```sh
zig build analyze-gemma4-recursive-lora-sweep -- \
  /tmp/gemma4-recursive-sweep/recursive_lora_sweep_comparison.json \
  /tmp/gemma4-recursive-sweep \
  --max-loss-ratio 1.10 \
  --max-adapter-ratio 1.25 \
  --max-compressed-base-ratio 0.75 \
  --min-teacher-tokens 1
```

The gate writes `recursive_lora_sweep_decision.json` and
`recursive_lora_sweep_decision.md` with pass/fail reasons and a recommended
recursive configuration when one satisfies the criteria.

## Compressed Base Materialization

After a recursive adapter has been trained, materialize the actual compressed
base checkpoint with:

```sh
zig build materialize-gemma4-recursive-base -- \
  /tmp/gemma4-base \
  /tmp/gemma4-recursive-train-out \
  /tmp/gemma4-recursive-compressed-base
```

The output directory contains:

- `model.safetensors`: non-layer tensors plus only physical layers
  `0..shared_block_size-1`
- copied HF support files such as `config.json`, tokenizer files, generation
  config, processor config, chat template, README, and projector GGUFs
- `recursive_lora_base_config.json`: source depth, shared block size, loop count,
  tensor counts, checkpoint byte sizes, and compression ratio

The copied `config.json` intentionally preserves the original logical depth.
Runtime still needs the recursive adapter metadata to map logical layer
parameters onto the smaller physical tensor set.
