// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

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
