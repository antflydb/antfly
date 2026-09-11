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

pub const lora = @import("lora.zig");
pub const command_registry = @import("command_registry.zig");
pub const peft = @import("peft.zig");
pub const graph_bridge = @import("graph_bridge.zig");
pub const graph_weight_bridge = @import("graph_weight_bridge.zig");
pub const graph_input_binder = @import("graph_input_binder.zig");
pub const colqwen2 = @import("colqwen2.zig");
pub const gliner2 = @import("gliner2.zig");
pub const gliner_boundary_losses = @import("gliner_boundary_losses.zig");
pub const gliner_boundary_targets = @import("gliner_boundary_targets.zig");
pub const gliner_boundary_selection = @import("gliner_boundary_selection.zig");
pub const gliner_boundary_matching = @import("gliner_boundary_matching.zig");
pub const gliner_boundary_record_loss = @import("gliner_boundary_record_loss.zig");
pub const gliner_boundary_encoder_graph = @import("gliner_boundary_encoder_graph.zig");
pub const gliner_boundary_peft_graph = @import("gliner_boundary_peft_graph.zig");
pub const gliner_boundary_adapter = @import("gliner_boundary_adapter.zig");
pub const gliner_boundary_adapter_layout = @import("gliner_boundary_adapter_layout.zig");
pub const gliner_boundary_train_step = @import("gliner_boundary_train_step.zig");
pub const gliner2_data = @import("gliner2_data.zig");
pub const gliner2_run_validation = @import("gliner2_run_validation.zig");
pub const gliner2_boundary = @import("gliner2_boundary.zig");
pub const entity_cleanup_data = @import("entity_cleanup_data.zig");
pub const entity_cleanup_gliner_cache = @import("entity_cleanup_gliner_cache.zig");
pub const entity_cleanup_model = @import("entity_cleanup_model.zig");
pub const document_data = @import("document_data.zig");
pub const layoutlmv3 = @import("layoutlmv3.zig");
pub const reranker_data = @import("reranker_data.zig");
pub const reranker = @import("reranker.zig");
pub const reranker_head = @import("reranker_head.zig");
pub const reranker_lora = @import("reranker_lora.zig");
pub const reranker_real_forward = @import("reranker_real_forward.zig");
pub const reranker_train = @import("reranker_train.zig");
pub const text_encoder_boundary = @import("text_encoder_boundary.zig");
pub const gliner2_real_autodiff = @import("gliner2_real_autodiff.zig");
pub const real_autodiff_trainer = @import("real_autodiff_trainer.zig");
pub const seeded_gradient_trainer = @import("seeded_gradient_trainer.zig");
pub const seeded_device_snapshot = @import("seeded_device_snapshot.zig");
pub const seeded_device_state = @import("seeded_device_state.zig");
pub const gliner_boundary_run = @import("gliner_boundary_run.zig");
pub const gliner_boundary_dataset = @import("gliner_boundary_dataset.zig");
pub const gliner_boundary_native_trainer = @import("gliner_boundary_native_trainer.zig");
pub const gliner_boundary_training_backend = @import("gliner_boundary_training_backend.zig");
pub const gliner_boundary_training_source = @import("gliner_boundary_training_source.zig");
pub const gliner_boundary_training_job = @import("gliner_boundary_training_job.zig");
pub const gliner_boundary_training_limits = @import("gliner_boundary_training_limits.zig");
pub const gliner_boundary_training_export = @import("gliner_boundary_training_export.zig");
pub const gliner_boundary_merge = @import("gliner_boundary_merge.zig");
pub const gliner_boundary_merge_job = @import("gliner_boundary_merge_job.zig");
pub const fused_chunker_data = @import("fused_chunker_data.zig");
pub const fused_chunker = @import("fused_chunker.zig");
pub const fused_chunker_loss = @import("fused_chunker_loss.zig");
pub const infonce_cpu = @import("infonce_cpu.zig");
pub const fused_chunker_splade = @import("fused_chunker_splade.zig");
pub const fused_chunker_train = @import("fused_chunker_train.zig");
pub const preference_loss = @import("preference_loss.zig");
pub const preference_harness = @import("preference_harness.zig");
pub const grpo = @import("grpo.zig");
pub const lora_adapter_set = @import("lora_adapter_set.zig");
pub const tokenizer_batch = @import("tokenizer_batch.zig");
pub const gemma_data = @import("gemma_data.zig");
pub const gemma_chat_data = @import("gemma_chat_data.zig");
pub const gemma_multimodal_data = @import("gemma_multimodal_data.zig");
pub const gemma4 = @import("gemma4.zig");
pub const gemma4_real_autodiff = @import("gemma4_real_autodiff.zig");
pub const gemma4_multimodal_real_autodiff = @import("gemma4_multimodal_real_autodiff.zig");
pub const qwen2_real_autodiff = @import("qwen2_real_autodiff.zig");
pub const recipe = @import("recipe.zig");

test {
    _ = lora;
    _ = command_registry;
    _ = peft;
    _ = graph_bridge;
    _ = graph_weight_bridge;
    _ = graph_input_binder;
    _ = colqwen2;
    _ = gliner2;
    _ = gliner_boundary_losses;
    _ = @import("seeded_device_transaction_test.zig");
    _ = @import("gliner_boundary_training_backend_test.zig");
    _ = gliner_boundary_targets;
    _ = gliner_boundary_selection;
    _ = gliner_boundary_matching;
    _ = gliner_boundary_record_loss;
    _ = gliner_boundary_encoder_graph;
    _ = @import("gliner_boundary_encoder_graph_test.zig");
    _ = gliner_boundary_peft_graph;
    _ = @import("gliner_boundary_peft_graph_test.zig");
    _ = gliner_boundary_adapter;
    _ = gliner_boundary_adapter_layout;
    _ = gliner_boundary_train_step;
    _ = @import("gliner_boundary_train_step_test.zig");
    _ = gliner2_data;
    _ = gliner2_run_validation;
    _ = gliner2_boundary;
    _ = entity_cleanup_data;
    _ = entity_cleanup_gliner_cache;
    _ = entity_cleanup_model;
    _ = document_data;
    _ = layoutlmv3;
    _ = reranker_data;
    _ = reranker;
    _ = reranker_head;
    _ = reranker_lora;
    _ = reranker_real_forward;
    _ = reranker_train;
    _ = text_encoder_boundary;
    _ = gliner2_real_autodiff;
    _ = real_autodiff_trainer;
    _ = seeded_gradient_trainer;
    _ = seeded_device_snapshot;
    _ = seeded_device_state;
    _ = @import("seeded_device_state_test.zig");
    _ = @import("seeded_gradient_trainer_device_test.zig");
    _ = gliner_boundary_run;
    _ = gliner_boundary_dataset;
    _ = gliner_boundary_native_trainer;
    _ = @import("gliner_boundary_native_trainer_test.zig");
    _ = @import("gliner_boundary_inactive_trainer_test.zig");
    _ = @import("gliner_boundary_inactive_oracle_test.zig");
    _ = @import("gliner_boundary_inactive_native_oracle_test.zig");
    _ = gliner_boundary_training_source;
    _ = gliner_boundary_training_job;
    _ = gliner_boundary_training_limits;
    _ = @import("gliner_boundary_training_job_test.zig");
    _ = gliner_boundary_training_export;
    _ = gliner_boundary_merge;
    _ = gliner_boundary_merge_job;
    _ = fused_chunker_data;
    _ = fused_chunker;
    _ = fused_chunker_loss;
    _ = infonce_cpu;
    _ = fused_chunker_splade;
    _ = fused_chunker_train;
    _ = preference_loss;
    _ = preference_harness;
    _ = grpo;
    _ = lora_adapter_set;
    _ = tokenizer_batch;
    _ = gemma_data;
    _ = gemma_chat_data;
    _ = gemma_multimodal_data;
    _ = gemma4;
    _ = gemma4_real_autodiff;
    _ = gemma4_multimodal_real_autodiff;
    _ = qwen2_real_autodiff;
    _ = recipe;
}
