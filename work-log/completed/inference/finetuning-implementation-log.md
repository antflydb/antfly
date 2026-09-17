# Fine-tuning implementation log

> Relocated verbatim from `zig/pkg/inference/docs/finetuning/FINETUNING.md` (lines 373–420 and 599–649 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`FINETUNING.md`](../../../zig/pkg/inference/finetuning/FINETUNING.md). Durable decisions from this log were folded into that document before the move.

Same-day end-to-end run of every gate (bundle rebuilt from the HF cache)
surfaced and fixed three further defects the gates had never executed:

- Optimizer step gating now mirrors PyTorch's grad-presence semantics on both
  host and device optimizer paths:
  upstream leaves `grad=None` for head modules whose task family is absent
  from a batch (no Adam state advance, no decoupled decay), while the Zig
  graph always produced zero-valued grads and stepped everything. The trainer
  gains conditional optimizer families
  (`registerConditionalOptimizerFamily`/`markOptimizerFamilyPresent`,
  presence ORed across the grad-accum window, reset per optimizer step); the
  gliner2-total-loss CLI registers `classifier.`, `count_pred.`,
  `span_rep.`, and `count_embed.` and marks presence from each micro-batch's
  task ids. The multi-step gate's per-parameter Adam step counts now match
  exactly. Presence is family-level (a structure task with gold count 0 marks
  its family), and absent device families have their accumulated gradients
  zeroed and skip their Adam step/decay.
- `--require-full-task-parity` slice-coverage checks required every task
  family in the compared slice unconditionally, which no single-family
  fixture could satisfy; they now require only the families present in the
  source fixture, and the multicount fixture moved a multi-instance
  relations record into the compared slice.
- The dedicated multi-step optimizer gate requires
  `trained_adapter_parity_ok`; one-step independently trained output deltas
  remain diagnostic because tiny cross-framework weight differences are
  amplified by the near-zero fixture. Same-artifact round-trip parity remains
  exact in every gate.
- `--deterministic` now also pins training data order on both sides (the
  Zig CLI skips its epoch shuffle; the harness passes the Python loader
  `--no-train-shuffle`), so deterministic comparisons cannot silently train
  on permuted batches.

Measured on the deterministic 3-step config (batch 2, seq 64, rank 4):
Python torch-CPU ≈ 0.2 s/step, Zig native ≈ 15 s/step (the correctness
reference, not a performance target), Zig Metal ≈ 0.55 s/step; the
performance target remains the Metal backend at production shapes
(`run_gliner2_lora_perf_gate.sh`, warm-step median ≤ 1.0× Python at
batch 32/seq 128). Historical short probes are diagnostic only; the production
wrapper now requires exactly five paired independent seeds, all deployment
floors, at most 0.02 mean Zig metric deficit, and at most 0.05 deficit in any
paired run.

The production batch-32 profile uses 16-sample structure-loss chunks and
reports the graph executor's device-owned peak-live metric separately from
process RSS. There is no generic release ceiling: operators can set an
explicit device-memory limit for their deployment, and that limit remains
fail-closed on representative release data.


### Remaining Task List

Completed:

1. Added a common recipe schema and `antfly inference finetune run <recipe.json>`.
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
19. Added `antfly inference finetune smoke-fast` for fast no-download recipe-layer verification across family dry-runs and scalar preference executes.
20. Added one synthetic no-download GLiNER2 direct-family execute case to `smoke-fast`, covering bootstrap, cache prepare, train/eval, and normalized artifact finalization through the unified recipe runner.
21. Updated the fine-tuning docs so `antfly inference finetune run` is the primary public entrypoint and family build-step commands are documented as backend reference.
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

