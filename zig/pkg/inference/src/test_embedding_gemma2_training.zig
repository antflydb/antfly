// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Isolated test root for the EmbeddingGemma 2 differentiable graph.

const finetune = @import("inference_internal").finetune;

test {
    try finetune.embedding_gemma2_cuda_encoder.compileProbe();
    try finetune.embedding_gemma2_cuda_session.compileProbe();
    _ = finetune.embedding_gemma2_training;
    _ = finetune.embedding_gemma2_graph;
    _ = finetune.embedding_gemma2_real_autodiff;
    _ = finetune.embedding_gemma2_adapter_state;
    _ = finetune.embedding_gemma2_checkpoint;
    _ = finetune.embedding_gemma2_cuda_tape;
    _ = finetune.embedding_gemma2_cuda_session;
    _ = finetune.embedding_gemma2_cuda_encoder;
    _ = finetune.embedding_gemma2_cuda_projection;
    _ = finetune.embedding_gemma2_cuda_attention;
    _ = finetune.embedding_gemma2_cuda_objective;
    _ = @import("finetune/tools/export_embedding_gemma2_lora.zig");
    _ = @import("finetune/tools/eval_embedding_gemma2_retrieval.zig");
}
