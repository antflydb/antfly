// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const common = @import("common.zig");

const gliner_boundary_imports = &.{ .build_options, .jinja, .termite_hf_tokenizer };
const gliner_boundary_train_imports = &.{ .build_options, .jinja, .ml, .termite_internal, .termite_hf_tokenizer };
const reranker_text_imports = &.{ .build_options, .jinja, .termite_tokenizer, .termite_hf_tokenizer };
const reranker_train_imports = &.{ .build_options, .jinja, .ml, .termite_tokenizer, .termite_hf_tokenizer };
const gemma_lora_imports = &.{ .build_options, .ml, .termite_internal };

const commands = [_]common.CommandSpec{
    .{
        .name = "inspect-layoutlmv3-bundle",
        .root_source_file = "src/finetune/tools/inspect_layoutlmv3_bundle.zig",
        .description = "Inspect a LayoutLMv3 runtime bundle",
        .imports = &.{ .build_options, .termite_hf_tokenizer },
    },
    .{
        .name = "compose-lora-adapters",
        .root_source_file = "src/finetune/tools/compose_lora_adapters.zig",
        .description = "Compose LoRA adapter SafeTensors with optional eval gate",
        .imports = &.{ .build_options, .termite_internal },
    },
    .{
        .name = "bootstrap-gliner2-lora",
        .root_source_file = "src/finetune/tools/bootstrap_gliner2_lora.zig",
        .description = "Bootstrap a GLiNER2 LoRA adapter bundle",
        .imports = gemma_lora_imports,
    },
    .{
        .name = "inspect-gliner2-checkpoint",
        .root_source_file = "src/finetune/tools/inspect_gliner2_checkpoint.zig",
        .description = "Inspect a GLiNER2 checkpoint and optional LoRA bundle",
        .imports = gemma_lora_imports,
    },
    .{
        .name = "inspect-gliner2-lora-bundle",
        .root_source_file = "src/finetune/tools/inspect_gliner2_lora_bundle.zig",
        .description = "Inspect a GLiNER2 LoRA adapter bundle",
        .imports = gemma_lora_imports,
    },
    .{
        .name = "materialize-gliner2-lora",
        .root_source_file = "src/finetune/tools/materialize_gliner2_lora.zig",
        .description = "Materialize a GLiNER2 LoRA adapter bundle into a merged checkpoint",
        .imports = gemma_lora_imports,
    },
    .{
        .name = "inspect-gliner2-dataset",
        .root_source_file = "src/finetune/tools/inspect_gliner2_dataset.zig",
        .description = "Inspect GLiNER2 finetune dataset stats, coverage, and encoded batch shapes",
        .imports = &.{.termite_finetune_data},
    },
    .{
        .name = "prepare-entity-cleanup-cache",
        .root_source_file = "src/finetune/tools/prepare_entity_cleanup_cache.zig",
        .description = "Prepare learned entity cleanup cache from annotated mention spans",
        .imports = &.{.termite_internal},
    },
    .{
        .name = "prepare-gliner2-entity-cleanup-cache",
        .root_source_file = "src/finetune/tools/prepare_gliner2_entity_cleanup_cache.zig",
        .description = "Prepare GLiNER2-native learned entity cleanup cache from annotated mention spans",
        .imports = &.{.termite_internal},
    },
    .{
        .name = "prepare-gliner2-top-layer-boundary-cache",
        .root_source_file = "src/finetune/tools/prepare_gliner2_top_layer_boundary_cache.zig",
        .description = "Precompute and persist GLiNER2 top-layer boundary caches for future task-head replay/update work",
        .imports = &.{ .build_options, .jinja, .termite_internal, .termite_hf_tokenizer },
        .native_link = .default,
    },
    .{
        .name = "eval-gliner2-top-layer-boundary-head",
        .root_source_file = "src/finetune/eval/eval_gliner2_top_layer_boundary_head.zig",
        .description = "Replay GLiNER2 top-layer boundary caches through the native span head and report bounded metrics",
        .imports = gliner_boundary_imports,
        .native_link = .default,
    },
    .{
        .name = "train-eval-gliner2-top-layer-boundary-head",
        .root_source_file = "src/finetune/train/train_eval_gliner2_top_layer_boundary_head.zig",
        .description = "Train a bounded GLiNER2 boundary-cache calibration head and report before/after metrics",
        .imports = gliner_boundary_train_imports,
        .native_link = .default,
    },
    .{
        .name = "eval-gliner2-top-layer-boundary-task-head",
        .root_source_file = "src/finetune/eval/eval_gliner2_top_layer_boundary_task_head.zig",
        .description = "Replay GLiNER2 top-layer boundary caches through a label-aware boundary task head and report bounded metrics",
        .imports = gliner_boundary_imports,
        .native_link = .default,
    },
    .{
        .name = "train-eval-gliner2-top-layer-boundary-task-head",
        .root_source_file = "src/finetune/train/train_eval_gliner2_top_layer_boundary_task_head.zig",
        .description = "Train a bounded GLiNER2 label-aware boundary task head and report before/after metrics",
        .imports = gliner_boundary_train_imports,
        .native_link = .default,
    },
    .{
        .name = "train-eval-gliner2-lora-bundle",
        .root_source_file = "src/finetune/train/train_eval_gliner2_lora_bundle.zig",
        .description = "Train GLiNER2 LoRA adapters using cached top-layer boundary representations and report before/after MSE",
        .imports = &.{ .build_options, .ml, .termite_internal, .pjrt },
        .native_link = .default,
    },
    .{
        .name = "train-eval-entity-cleanup-head",
        .root_source_file = "src/finetune/train/train_eval_entity_cleanup_head.zig",
        .description = "Train/eval a learned entity cleanup head from cached mention features",
        .imports = &.{.termite_internal},
    },
    .{
        .name = "train-gliner2-autodiff",
        .root_source_file = "src/finetune/train/train_gliner2_autodiff.zig",
        .description = "Train GLiNER2 NER with real autodiff through DeBERTa encoder (level-3 LoRA training)",
        .imports = &.{ .build_options, .ml, .termite_internal, .termite_hf_tokenizer, .protobuf, .termite_linalg },
        .native_link = .default,
    },
    .{
        .name = "inspect-reranker-dataset",
        .root_source_file = "src/finetune/tools/inspect_reranker_dataset.zig",
        .description = "Inspect reranker finetune dataset stats and grouped pair counts",
        .imports = &.{.termite_finetune_data},
    },
    .{
        .name = "eval-reranker-checkpoint",
        .root_source_file = "src/finetune/eval/eval_reranker_checkpoint.zig",
        .description = "Evaluate a reranker checkpoint on a JSONL finetune dataset",
        .imports = reranker_text_imports,
        .native_link = .default,
    },
    .{
        .name = "eval-fused-chunker",
        .root_source_file = "src/finetune/eval/eval_fused_chunker.zig",
        .description = "Evaluate a fused chunker-embedder boundary head checkpoint",
        .imports = &.{.ml},
        .native_link = .default,
    },
    .{
        .name = "train-eval-reranker-head",
        .root_source_file = "src/finetune/train/train_eval_reranker_head.zig",
        .description = "Train and evaluate a bounded termite-owned reranker head",
        .imports = reranker_train_imports,
        .native_link = .default,
    },
    .{
        .name = "prepare-reranker-pooled-cache",
        .root_source_file = "src/finetune/tools/prepare_reranker_pooled_cache.zig",
        .description = "Precompute and persist pooled reranker features for bounded finetune runs",
        .imports = reranker_text_imports,
        .native_link = .default,
    },
    .{
        .name = "prepare-reranker-top-layer-cache",
        .root_source_file = "src/finetune/tools/prepare_reranker_top_layer_cache.zig",
        .description = "Precompute and persist top-layer boundary caches for exact reranker finetune work",
        .imports = reranker_text_imports,
        .native_link = .default,
    },
    .{
        .name = "train-eval-reranker-head-cached",
        .root_source_file = "src/finetune/train/train_eval_reranker_head_cached.zig",
        .description = "Train and evaluate a termite-owned reranker head from persisted pooled caches",
        .imports = reranker_train_imports,
        .native_link = .default,
    },
    .{
        .name = "train-eval-reranker-head-top-layer-cached",
        .root_source_file = "src/finetune/train/train_eval_reranker_head_top_layer_cached.zig",
        .description = "Train and evaluate a reranker head from exact top-layer boundary caches",
        .imports = reranker_train_imports,
        .native_link = .default,
    },
    .{
        .name = "materialize-reranker-head",
        .root_source_file = "src/finetune/tools/materialize_reranker_head.zig",
        .description = "Materialize a trained termite-owned reranker head into native checkpoint tensors",
        .imports = reranker_text_imports,
        .native_link = .default,
    },
    .{
        .name = "bootstrap-reranker-lora",
        .root_source_file = "src/finetune/tools/bootstrap_reranker_lora.zig",
        .description = "Bootstrap a bounded encoder-side LoRA bundle for native text rerankers",
        .imports = reranker_text_imports,
        .link_libc = true,
    },
    .{
        .name = "inspect-reranker-lora-bundle",
        .root_source_file = "src/finetune/tools/inspect_reranker_lora_bundle.zig",
        .description = "Inspect a bounded encoder-side LoRA bundle for native text rerankers",
        .imports = reranker_text_imports,
        .link_libc = true,
    },
    .{
        .name = "materialize-reranker-lora",
        .root_source_file = "src/finetune/tools/materialize_reranker_lora.zig",
        .description = "Materialize a bounded encoder-side LoRA bundle into a merged reranker checkpoint",
        .imports = &.{ .build_options, .jinja, .termite_tokenizer, .termite_hf_tokenizer, .termite_internal },
        .link_libc = true,
    },
    .{
        .name = "train-eval-reranker-lora-surrogate",
        .root_source_file = "src/finetune/train/train_eval_reranker_lora_surrogate.zig",
        .description = "Train and evaluate a bounded surrogate encoder-side reranker LoRA path",
        .imports = reranker_train_imports,
        .native_link = .default,
    },
    .{
        .name = "train-eval-reranker-lora-surrogate-cached",
        .root_source_file = "src/finetune/train/train_eval_reranker_lora_surrogate_cached.zig",
        .description = "Train and evaluate a bounded surrogate encoder-side reranker LoRA path from persisted pooled caches",
        .imports = reranker_train_imports,
        .native_link = .default,
    },
    .{
        .name = "train-eval-reranker-lora-top-layer-cached-surrogate",
        .root_source_file = "src/finetune/train/train_eval_reranker_lora_top_layer_cached_surrogate.zig",
        .description = "Train and evaluate a bounded surrogate reranker LoRA path from exact top-layer boundary caches",
        .imports = reranker_train_imports,
        .native_link = .default,
    },
    .{
        .name = "bootstrap-colqwen2-lora",
        .root_source_file = "src/finetune/tools/bootstrap_colqwen2_lora.zig",
        .description = "Bootstrap a ColQwen2 LoRA adapter bundle",
        .imports = &.{ .build_options, .ml, .termite_hf_tokenizer },
    },
    .{
        .name = "inspect-colqwen2-checkpoint",
        .root_source_file = "src/finetune/tools/inspect_colqwen2_checkpoint.zig",
        .description = "Inspect a ColQwen2 checkpoint or adapter bundle",
        .imports = &.{ .build_options, .ml, .termite_hf_tokenizer },
    },
    .{
        .name = "inspect-colqwen2-lora-bundle",
        .root_source_file = "src/finetune/tools/inspect_colqwen2_lora_bundle.zig",
        .description = "Inspect a ColQwen2 LoRA adapter bundle",
        .imports = &.{ .build_options, .ml, .termite_hf_tokenizer },
    },
    .{
        .name = "materialize-colqwen2-lora",
        .root_source_file = "src/finetune/tools/materialize_colqwen2_lora.zig",
        .description = "Materialize a ColQwen2 LoRA adapter bundle into a merged checkpoint",
        .imports = &.{ .build_options, .ml, .termite_hf_tokenizer, .termite_internal },
    },
    .{
        .name = "prepare-colqwen2-inputs",
        .root_source_file = "src/finetune/tools/prepare_colqwen2_inputs.zig",
        .description = "Prepare bounded ColQwen2 multimodal finetune inputs",
        .imports = &.{ .build_options, .ml, .termite_tokenizer, .termite_hf_tokenizer, .antfly_image },
    },
    .{
        .name = "prepare-gemma4-text-dataset",
        .root_source_file = "src/finetune/tools/prepare_gemma4_text_dataset.zig",
        .description = "Convert Gemma4 text JSONL dataset to CSV for finetuning",
        .imports = &.{.termite_finetune_data},
    },
    .{
        .name = "prepare-gemma4-multimodal-dataset",
        .root_source_file = "src/finetune/tools/prepare_gemma4_multimodal_dataset.zig",
        .description = "Convert Gemma4 multimodal JSONL dataset to CSV for finetuning",
        .imports = &.{.termite_finetune_data},
    },
    .{
        .name = "prepare-gemma4-lora-inputs",
        .root_source_file = "src/finetune/tools/prepare_gemma4_lora_inputs.zig",
        .description = "Tokenize Gemma4 JSONL examples into prepared LoRA input JSON",
        .imports = &.{ .build_options, .ml, .termite_internal, .termite_hf_tokenizer },
        .native_link = .default,
    },
    .{
        .name = "materialize-gemma4-teacher-targets",
        .root_source_file = "src/finetune/tools/materialize_gemma4_teacher_targets.zig",
        .description = "Run Gemma4 teacher model over prepared inputs and attach sparse top-k targets",
        .imports = gemma_lora_imports,
        .native_link = .default,
    },
    .{
        .name = "materialize-gemma4-recursive-base",
        .root_source_file = "src/finetune/tools/materialize_gemma4_recursive_base.zig",
        .description = "Materialize a compressed Gemma4 recursive base checkpoint",
        .imports = gemma_lora_imports,
        .native_link = .default,
    },
    .{
        .name = "generate-gemma4-pilot-dataset",
        .root_source_file = "src/finetune/tools/generate_gemma4_pilot_dataset.zig",
        .description = "Generate a deterministic Gemma4 chat JSONL dataset for 100-example pilot runs",
        .imports = &.{ .build_options, .termite_internal },
    },
    .{
        .name = "generate-gemma4-multimodal-pilot-dataset",
        .root_source_file = "src/finetune/tools/generate_gemma4_multimodal_pilot_dataset.zig",
        .description = "Generate a deterministic Gemma4 chat JSONL dataset with image parts for multimodal pilot runs",
        .imports = &.{ .build_options, .termite_internal },
    },
    .{
        .name = "analyze-gemma4-recursive-lora-sweep",
        .root_source_file = "src/finetune/tools/analyze_gemma4_recursive_lora_sweep.zig",
        .description = "Analyze Gemma4 recursive LoRA sweep comparison and write promotion decision",
        .imports = &.{ .termite_io_compat, .termite_c_file },
        .link_libc = true,
    },
    .{
        .name = "train-eval-gemma4-lora-bundle",
        .root_source_file = "src/finetune/train/train_eval_gemma4_lora_bundle.zig",
        .description = "Run a bounded Gemma4 LoRA train/eval step",
        .imports = &.{ .build_options, .ml, .termite_internal, .termite_hf_tokenizer },
        .native_link = .default,
    },
    .{
        .name = "bootstrap-gemma4-lora",
        .root_source_file = "src/finetune/tools/bootstrap_gemma4_lora.zig",
        .description = "Bootstrap a Gemma4 LoRA adapter bundle",
        .imports = gemma_lora_imports,
    },
    .{
        .name = "inspect-gemma4-lora-bundle",
        .root_source_file = "src/finetune/tools/inspect_gemma4_lora_bundle.zig",
        .description = "Inspect a Gemma4 LoRA adapter bundle",
        .imports = gemma_lora_imports,
    },
    .{
        .name = "materialize-gemma4-lora",
        .root_source_file = "src/finetune/tools/materialize_gemma4_lora.zig",
        .description = "Materialize a Gemma4 LoRA adapter bundle into a merged checkpoint",
        .imports = gemma_lora_imports,
    },
    .{
        .name = "train-eval-colqwen2-lora-bundle",
        .root_source_file = "src/finetune/train/train_eval_colqwen2_lora_bundle.zig",
        .description = "Run a bounded ColQwen2 LoRA train/eval step",
        .imports = &.{ .build_options, .ml, .termite_internal, .termite_tokenizer, .termite_hf_tokenizer },
        .native_link = .no_accel,
    },
    .{
        .name = "bootstrap-layoutlmv3-lora",
        .root_source_file = "src/finetune/tools/bootstrap_layoutlmv3_lora.zig",
        .description = "Bootstrap a LayoutLMv3 LoRA adapter bundle",
        .imports = gemma_lora_imports,
    },
    .{
        .name = "inspect-layoutlmv3-lora-bundle",
        .root_source_file = "src/finetune/tools/inspect_layoutlmv3_lora_bundle.zig",
        .description = "Inspect a LayoutLMv3 LoRA adapter bundle",
        .imports = &.{ .build_options, .ml },
    },
    .{
        .name = "materialize-layoutlmv3-checkpoint",
        .root_source_file = "src/finetune/tools/materialize_layoutlmv3_checkpoint.zig",
        .description = "Materialize a merged LayoutLMv3 checkpoint from a LoRA adapter bundle",
        .imports = gemma_lora_imports,
    },
    .{
        .name = "train-layoutlmv3-lora-one-step",
        .root_source_file = "src/finetune/train/train_layoutlmv3_lora_one_step.zig",
        .description = "Run one deterministic LayoutLMv3 LoRA update step",
        .imports = &.{ .build_options, .ml },
    },
    .{
        .name = "train-eval-layoutlmv3-lora-sequence",
        .root_source_file = "src/finetune/train/train_eval_layoutlmv3_lora_sequence.zig",
        .description = "Train and evaluate a bounded LayoutLMv3 sequence head with LoRA adapters",
        .imports = gemma_lora_imports,
        .native_link = .no_accel,
    },
    .{
        .name = "train-eval-layoutlmv3-lora-token",
        .root_source_file = "src/finetune/train/train_eval_layoutlmv3_lora_token.zig",
        .description = "Train and evaluate a bounded LayoutLMv3 token head with LoRA adapters",
        .imports = gemma_lora_imports,
        .native_link = .no_accel,
    },
    .{
        .name = "train-fused-chunker",
        .root_source_file = "src/finetune_train_fused_chunker_root.zig",
        .description = "End-to-end training for the fused chunker-embedder model",
        .imports = &.{ .build_options, .ml, .termite_tokenizer, .termite_hf_tokenizer, .termite_linalg },
        .native_link = .default,
    },
};

pub fn register(ctx: common.Context) void {
    for (commands) |spec| common.addCommand(ctx, spec);
}
