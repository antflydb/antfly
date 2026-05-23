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

const build_options = @import("build_options");

pub const backends = @import("backends/backends.zig");
pub const metal_runtime = if (build_options.enable_metal) @import("backends/metal_runtime.zig") else struct {
    pub fn metalDeviceAvailable() bool {
        return false;
    }
};
pub const mlx_backend = if (build_options.enable_mlx) @import("backends/mlx.zig") else struct {
    pub fn metalDeviceAvailable() bool {
        return false;
    }
};
pub const graph = @import("graph/root.zig");
pub const io = @import("io/io.zig");
pub const ops = @import("ops/ops.zig");
pub const run = @import("run/root.zig");
pub const util = @import("util/util.zig");
pub const native_backend_guard = @import("native_backend_guard.zig");
pub const server = struct {
    pub const model_manager = @import("server/model_manager.zig");
};
pub const pipelines = struct {
    pub const embedding = @import("pipelines/embedding.zig");
};
pub const finetune = struct {
    pub const colqwen2 = @import("finetune/colqwen2.zig");
    pub const gemma4 = @import("finetune/gemma4.zig");
    pub const gemma_chat_data = @import("finetune/gemma_chat_data.zig");
    pub const gemma_data = @import("finetune/gemma_data.zig");
    pub const gemma_multimodal_data = @import("finetune/gemma_multimodal_data.zig");
    pub const gliner2 = @import("finetune/gliner2.zig");
    pub const gliner2_boundary = @import("finetune/gliner2_boundary.zig");
    pub const gliner2_data = @import("finetune/gliner2_data.zig");
    pub const entity_cleanup_data = @import("finetune/entity_cleanup_data.zig");
    pub const entity_cleanup_gliner_cache = @import("finetune/entity_cleanup_gliner_cache.zig");
    pub const entity_cleanup_model = @import("finetune/entity_cleanup_model.zig");
    pub const graph_bridge = @import("finetune/graph_bridge.zig");
    pub const document_data = @import("finetune/document_data.zig");
    pub const layoutlmv3 = @import("finetune/layoutlmv3.zig");
    pub const gemma4_real_autodiff = @import("finetune/gemma4_real_autodiff.zig");
    pub const gemma4_multimodal_real_autodiff = @import("finetune/gemma4_multimodal_real_autodiff.zig");
    pub const text_encoder_boundary = @import("finetune/text_encoder_boundary.zig");
    pub const gliner2_real_autodiff = @import("finetune/gliner2_real_autodiff.zig");
    pub const real_autodiff_trainer = @import("finetune/real_autodiff_trainer.zig");
    pub const graph_weight_bridge = @import("finetune/graph_weight_bridge.zig");
    pub const graph_input_binder = @import("finetune/graph_input_binder.zig");
    pub const reranker_data = @import("finetune/reranker_data.zig");
    pub const reranker = @import("finetune/reranker.zig");
    pub const reranker_lora = @import("finetune/reranker_lora.zig");
    pub const fused_chunker_data = @import("finetune/fused_chunker_data.zig");
    pub const fused_chunker = @import("finetune/fused_chunker.zig");
    pub const fused_chunker_loss = @import("finetune/fused_chunker_loss.zig");
    pub const infonce_cpu = @import("finetune/infonce_cpu.zig");
    pub const fused_chunker_splade = @import("finetune/fused_chunker_splade.zig");
    pub const fused_chunker_train = @import("finetune/fused_chunker_train.zig");
    pub const lora_adapter_set = @import("finetune/lora_adapter_set.zig");
    pub const peft = @import("finetune/peft.zig");
    pub const tokenizer_batch = @import("finetune/tokenizer_batch.zig");
};
pub const architectures = struct {
    pub const deberta = @import("architectures/deberta.zig");
    pub const deberta_graph = @import("architectures/deberta_graph.zig");
    pub const bert_graph = @import("architectures/bert_graph.zig");
    pub const qwen2_graph = @import("architectures/qwen2_graph.zig");
    pub const gemma_graph = @import("architectures/gemma_graph.zig");
    pub const modern_bert_graph = @import("architectures/modern_bert_graph.zig");
    pub const layoutlmv3_graph = @import("architectures/layoutlmv3_graph.zig");
    pub const clip = @import("architectures/clip.zig");
    pub const clap = @import("architectures/clap.zig");
    pub const gliner_head = @import("architectures/gliner_head.zig");
    pub const gliner_head_graph = @import("architectures/gliner_head_graph.zig");
};
pub const models = struct {
    pub const deberta = @import("models/deberta.zig");
    pub const clip = @import("models/clip.zig");
    pub const clap = @import("models/clap.zig");
    pub const weight_source = @import("models/weight_source.zig");
    pub const safetensors = @import("models/safetensors.zig");
};
pub const native_compute = struct {
    pub const native = @import("ops/native_compute.zig");
    pub const gpu_hosted_store = @import("ops/gpu_hosted_store.zig");
    pub const metal = if (build_options.enable_metal) @import("ops/metal_compute.zig") else struct {};
    pub const blas = @import("ops/blas_compute.zig");
    pub const mlx = if (build_options.enable_mlx) @import("ops/mlx_compute.zig") else struct {};
    pub const cuda = if (build_options.enable_cuda) @import("ops/cuda/cuda_compute.zig") else struct {};
    pub const wasm = if (build_options.enable_wasm) @import("ops/wasm_compute.zig") else struct {};
};
pub const gguf = struct {
    pub const quant_codec = @import("gguf/quant_codec.zig");
    pub const tensor_types = @import("gguf/tensor_types.zig");
};
