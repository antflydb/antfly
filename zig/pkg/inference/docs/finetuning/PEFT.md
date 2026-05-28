# PEFT

This note tracks Termite's PEFT parity beyond basic LoRA. The implementation
surface lives under `pkg/inference/src/finetune/`; this file is the durable status
and follow-up checklist.

## Build And Validation

- [x] Fix the MLX distributed init signature drift so full test builds compile against the current C import.
- [x] Fix the Gemma4 test tokenizer signature so generic tokenizer budget checks compile.
- [x] Fix MLX link leakage in `zig build test -Dskip-openapi=true -Donnx=false -Dmlx=false` by gating raw Metal/MLX imports behind `enable_mlx`.
- [x] Keep `zig build test -Dskip-openapi=true -Donnx=false -Dmlx=false` green after the Gemma4 PEFT wiring pass.

## Implemented Surface

- [x] Keep DoRA as the shared reference implementation in `lora.zig`: magnitude vector, column-norm direction update, dense merge, and finite-difference gradient tests.
- [x] Wire DoRA storage into `LoRAAdapterSet` with magnitude allocation, zeroing, and base-weight norm initialization.
- [x] Fix `LoRAAdapterSet.applyAdapter` to use the graph/checkpoint LoRA layout: `A = [rank, in]`, `B = [out, rank]`.
- [x] Add target-module presets: `all-linear`, `attention-only`, `mlp-only`, and `moe-experts`.
- [x] Add QLoRA/NF4 LoftQ initialization helper that quantizes/dequantizes through the local NF4 codec.
- [x] Add generic weighted adapter composition for SafeTensors adapters.
- [x] Add `compose-lora-adapters` CLI with an optional pre-export eval gate.
- [x] Thread Gemma4 `use_dora` through bootstrap/load/save with PEFT-style magnitude-vector tensors initialized from base row norms.
- [x] Wire Gemma4 `pissa` and `loftq-nf4` adapter initialization through base tensor loading.
- [x] Add Gemma4 merged-checkpoint export with DoRA-aware dense merge.
- [x] Add Gemma4 adapter inspection output and CLI for initializer kind and DoRA magnitude accounting.
- [x] Add eval-before/eval-after report mode to `materialize-gemma4-lora`.
- [x] Extract eval execution/report result plumbing into shared PEFT utilities for reuse by other materializers.
- [x] Extend eval-before/eval-after report mode to GLiNER2, LayoutLMv3, and reranker materializers.

## Gemma4 Bootstrap Surface

- [x] Add `--target-preset all-linear|attention-only|mlp-only|moe-experts` to `bootstrap-gemma4-lora`.
- [x] Persist `target_preset`, `use_dora`, and `init_lora_weights` in Gemma4 `adapter_config.json`.
- [x] Add `--use-dora` and `--init-lora-weights` flags to the bootstrap CLI as explicit artifact metadata.
- [x] Materialize DoRA magnitude tensors into Gemma4 adapter checkpoints when `--use-dora` is enabled, initialized from base row norms.
- [x] Wire `loftq-nf4` bootstrap initialization through base tensor loading for Gemma4 QLoRA starts.
- [x] Wire `pissa` bootstrap initialization through the existing data-driven initializer substrate for Gemma4.

## Adapter Export And Runtime

- [x] Add a Gemma4 materialize/export command and merge DoRA using magnitude vectors when present.
- [x] Add Gemma4 adapter inspection output for initializer kind and DoRA magnitude parameter count.
- [x] Add eval-before and eval-after report support to `materialize-gemma4-lora`.
- [x] Extract eval execution/report result plumbing into shared PEFT utilities.
- [x] Extend eval-before and eval-after report support to GLiNER2, LayoutLMv3, and reranker materialize commands.
- [x] Port DoRA artifact load/save/inspect/materialize support to reranker, GLiNER2, and LayoutLMv3 adapter families.
- [x] Add a ColQwen2 merged-export/materialize surface and wire optional DoRA magnitude vectors through the same merge path.
- [x] Add runtime adapter hotswap registry: load, select, disable, and weighted-combine adapters without rewriting the base model.

## Broader PEFT Parity

- [x] Add EVA/LoRA-GA frontend flags that call the existing initializer substrate.
- [x] Add trainable token-index adapter artifacts for embedding-only specialization.
- [x] Add architecture-specific MoE expert parameter targeting where experts are parameters rather than linear modules.

## Follow-Up

- [ ] Repeat `use_dora` artifact support across the remaining model-family bootstrap/materialization paths.
- [ ] Extend LoftQ bootstrap initialization to the other model-specific adapter bootstrappers.
- [ ] Add data-driven initializer frontends for EVA and LoRA-GA on top of `lora_init.zig`.
- [ ] Add trainable token-index adapter tensors for embedding-only specialization.
- [ ] Add model-aware MoE expert parameter targeting for architectures that store experts as bare parameters instead of linear modules.
- [ ] Add eval-before-and-after regression thresholds once model-specific eval metrics are standardized.
- [ ] Add hotswap runtime registry hooks so multiple adapters can be loaded, selected, and combined without rebuilding the graph.
