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

const tests = [_]common.TestSpec{
    .{
        .step_name = "test-layoutlmv3-finetune",
        .root_source_file = "src/finetune/test/test_layoutlmv3_finetune.zig",
        .description = "Run isolated LayoutLMv3 finetune tests",
        .imports = &.{ .build_options, .ml, .termite_internal },
        .native_link = .default,
    },
    .{
        .step_name = "test-colqwen2-finetune",
        .root_source_file = "src/finetune/test/test_colqwen2_finetune.zig",
        .description = "Run isolated ColQwen2 finetune tests",
        .imports = &.{ .build_options, .ml, .termite_tokenizer, .termite_hf_tokenizer, .antfly_image, .termite_internal },
        .native_link = .default,
    },
    .{
        .step_name = "test-gliner2-data",
        .root_source_file = "src/finetune/test/test_gliner2_data.zig",
        .description = "Run isolated GLiNER2 finetune data tests",
        .imports = &.{.termite_finetune_data},
    },
    .{
        .step_name = "test-entity-cleanup-data",
        .root_source_file = "src/test_entity_cleanup_data.zig",
        .description = "Run isolated entity cleanup finetune data tests",
    },
    .{
        .step_name = "test-entity-cleanup-model",
        .root_source_file = "src/test_entity_cleanup_model.zig",
        .description = "Run isolated learned entity cleanup model tests",
        .imports = &.{ .build_options, .termite_hf_tokenizer },
        .link_libc = true,
    },
    .{
        .step_name = "test-entity-cleanup-gliner-cache",
        .root_source_file = "src/test_entity_cleanup_gliner_cache.zig",
        .description = "Run isolated GLiNER2-native entity cleanup cache tests",
        .imports = &.{ .build_options, .termite_hf_tokenizer, .termite_linalg, .ml, .onnx_graph },
        .native_link = .default,
    },
    .{
        .step_name = "test-gliner2-cleanup-bundle",
        .root_source_file = "src/test_gliner2_cleanup_bundle.zig",
        .description = "Run GLiNER2 cleanup bundle propagation tests",
        .imports = &.{ .build_options, .ml, .pjrt, .termite_linalg, .onnx_graph },
        .native_link = .default,
    },
    .{
        .step_name = "test-entity-cleanup",
        .root_source_file = "src/test_entity_cleanup_pipeline.zig",
        .description = "Run isolated learned entity cleanup pipeline tests",
    },
    .{
        .step_name = "test-reranker-data",
        .root_source_file = "src/finetune/test/test_reranker_data.zig",
        .description = "Run isolated reranker finetune data tests",
        .imports = &.{.termite_finetune_data},
    },
    .{
        .step_name = "test-fused-chunker-data",
        .root_source_file = "src/finetune/test/test_fused_chunker_data.zig",
        .description = "Run fused chunker data tests",
        .imports = &.{.termite_finetune_data},
    },
    .{
        .step_name = "test-fused-chunker",
        .root_source_file = "src/finetune/test/test_fused_chunker.zig",
        .description = "Run fused chunker model tests",
        .imports = &.{.termite_internal},
    },
    .{
        .step_name = "test-fused-chunker-loss",
        .root_source_file = "src/finetune/test/test_fused_chunker_loss.zig",
        .description = "Run fused chunker loss graph tests",
        .imports = &.{ .ml, .termite_internal },
    },
    .{
        .step_name = "test-infonce-cpu",
        .root_source_file = "src/finetune/test/test_infonce_cpu.zig",
        .description = "Run CPU InfoNCE contrastive loss tests",
        .imports = &.{.termite_internal},
    },
    .{
        .step_name = "test-fused-chunker-splade",
        .root_source_file = "src/finetune/test/test_fused_chunker_splade.zig",
        .description = "Run SPLADE sparse embedding head tests",
        .imports = &.{.termite_internal},
    },
    .{
        .step_name = "test-fused-chunker-train",
        .root_source_file = "src/finetune/test/test_fused_chunker_train.zig",
        .description = "Run fused chunker trainer tests",
        .imports = &.{ .ml, .termite_internal },
    },
    .{
        .step_name = "test-fused-chunker-lora",
        .root_source_file = "src/finetune/test/test_fused_chunker_lora.zig",
        .description = "Run fused chunker LoRA adapter tests",
        .imports = &.{.termite_internal},
    },
    .{
        .step_name = "test-tokenizer-batch",
        .root_source_file = "src/finetune/test/test_tokenizer_batch.zig",
        .description = "Run TokenizerBatch wrapper tests",
        .imports = &.{.termite_finetune_tokenizer_batch},
    },
};

pub fn register(ctx: common.Context) void {
    const aggregate = ctx.b.step("test-finetune", "Run all focused fine-tuning tests");
    for (tests) |spec| {
        const step = common.addTest(ctx, spec);
        aggregate.dependOn(step);
    }
}
