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

// Antfly inference: ML service for embeddings, chunking, and reranking.
// Zig implementation with ONNX Runtime, Metal, CUDA, WASM, and native backends.

const std = @import("std");
const build_options = @import("build_options");

extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
extern fn unsetenv(name: [*:0]const u8) c_int;

pub const backends = @import("backends/backends.zig");
pub const sentencepiece = @import("inference_tokenizer").sentencepiece;
pub const hf_tokenizer = @import("inference_hf_tokenizer");
pub const tokenizer = @import("inference_tokenizer");
pub const audio = @import("inference_audio");
pub const chunker = @import("inference_chunker");
pub const pipelines = @import("pipelines/pipelines.zig");
pub const extractors = @import("extractors/mod.zig");
pub const server = if (build_options.skip_openapi) struct {} else @import("server/server.zig");
pub const cache = @import("cache/cache.zig");
pub const singleflight = @import("cache/singleflight.zig");
pub const registry = @import("registry/registry.zig");
pub const tabular = @import("tabular/root.zig");
pub const models = @import("models/models.zig");
pub const gguf = @import("gguf/root.zig");
pub const runtime = @import("runtime/root.zig");
pub const util = @import("util/util.zig");
pub const ops = @import("ops/ops.zig");
pub const io = @import("io/io.zig");
pub const codecs = @import("codecs/codecs.zig");
pub const compiled_artifact = @import("compiled_artifact.zig");
pub const graph = @import("graph/root.zig");
pub const architectures = struct {
    pub const deberta = @import("architectures/deberta.zig");
    pub const deberta_graph = @import("architectures/deberta_graph.zig");
};
pub const finetune = @import("finetune/root.zig");
pub const finetune_cli = @import("finetune/cli/root.zig");
pub const run = @import("run/root.zig");
pub const quantize = @import("quantize/root.zig");
pub const client = if (build_options.skip_openapi) struct {} else @import("inference_client");
pub const linalg = @import("inference_linalg");
pub const native_generate = @import("native_generate.zig");
pub const native_chat = @import("native_chat.zig");
pub const native_compile = @import("native_compile.zig");
pub const native_export = @import("native_export.zig");
pub const native_quantize = @import("native_quantize.zig");
pub const native_quant_kernel_codegen = @import("native_quant_kernel_codegen.zig");
pub const kernel_jit_profile_output = @import("kernel_jit_profile_output.zig");
pub const native_export_gguf = @import("native_export_gguf.zig");
pub const native_export_safetensors = @import("native_export_safetensors.zig");
pub const native_run_artifact = @import("native_run_artifact.zig");
pub const native_embed = @import("native_embed.zig");
pub const native_classify = @import("native_classify.zig");
pub const native_rerank = @import("native_rerank.zig");
pub const native_transcribe = @import("native_transcribe.zig");
pub const native_read = @import("native_read.zig");
pub const scraping = @import("antfly_scraping");
pub const native_extract = @import("native_extract.zig");
pub const compare_generate = @import("cli/compare_generate.zig");
pub const run_options = @import("cli/run_options.zig");
pub const native_smoke = @import("native_smoke.zig");
pub const cuda_info = @import("cuda_info.zig");
pub const cuda_microbench = @import("bench/cuda_microbench.zig");
pub const cuda_attention_diff = @import("quant_kernel_cuda_attention_diff.zig");
pub const cuda_ffn_diff = @import("quant_kernel_cuda_ffn_diff.zig");
pub const cuda_nvrtc = @import("ops/cuda/nvrtc.zig");
pub const metal_runtime = @import("backends/metal_runtime.zig");
pub const native_compute = struct {
    pub const native = @import("ops/native_compute.zig");
    pub const gpu_hosted_store = @import("ops/gpu_hosted_store.zig");
    pub const metal = if (build_options.enable_metal) @import("ops/metal_compute.zig") else struct {};
    pub const cuda = if (build_options.enable_cuda) @import("ops/cuda/cuda_compute.zig") else struct {};
    pub const wasm = if (build_options.enable_wasm) @import("ops/wasm_compute.zig") else struct {};
};

test {
    _ = backends;
    _ = sentencepiece;
    _ = hf_tokenizer;
    _ = tokenizer;
    _ = audio;
    _ = chunker;
    _ = pipelines;
    _ = extractors;
    _ = server;
    _ = cache;
    _ = singleflight;
    _ = registry;
    _ = models;
    _ = gguf;
    _ = runtime;
    _ = util;
    _ = ops;
    _ = io;
    _ = codecs;
    _ = compiled_artifact;
    _ = linalg;
    _ = graph;
    _ = architectures;
    _ = finetune;
    _ = finetune_cli;
    _ = run;
    _ = quantize;
    _ = client;
    _ = scraping;
    _ = native_generate;
    _ = native_chat;
    _ = native_compile;
    _ = native_export;
    _ = native_quantize;
    _ = kernel_jit_profile_output;
    _ = native_quant_kernel_codegen;
    _ = native_export_gguf;
    _ = native_export_safetensors;
    _ = native_run_artifact;
    _ = native_embed;
    _ = native_classify;
    _ = native_rerank;
    _ = native_transcribe;
    _ = native_read;
    _ = @import("metal_generated_quant_stats.zig");
    _ = @import("readers/reader.zig");
    _ = native_extract;
    _ = compare_generate;
    _ = run_options;
    _ = native_smoke;
    _ = cuda_info;
    _ = cuda_microbench;
    _ = cuda_attention_diff;
    _ = cuda_ffn_diff;
    _ = cuda_nvrtc;
    _ = native_compute.native;
    if (build_options.enable_metal) {
        _ = metal_runtime;
        _ = native_compute.metal;
    }
    if (build_options.enable_cuda) {
        _ = native_compute.cuda;
        _ = @import("ops/cuda/kernels.zig");
    }
    _ = @import("ml");
}

test "non-device-scalar GQA attention never returns generated decode" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;

    const buffer_mod = @import("ops/cuda/buffer.zig");
    const context_mod = @import("ops/cuda/context.zig");
    const driver_mod = @import("ops/cuda/driver.zig");
    const kernels_mod = @import("ops/cuda/kernels.zig");
    const Mocks = struct {
        fn setCurrent(_: driver_mod.CUcontext) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }

        fn launchKernel(
            _: driver_mod.CUfunction,
            _: c_uint,
            _: c_uint,
            _: c_uint,
            _: c_uint,
            _: c_uint,
            _: c_uint,
            _: c_uint,
            _: driver_mod.CUstream,
            _: ?[*]?*anyopaque,
            _: ?[*]?*anyopaque,
        ) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }
    };

    const env_name = "ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE";
    const old_value = std.c.getenv(env_name);
    const old_value_copy = if (old_value) |value| try std.testing.allocator.dupeZ(u8, std.mem.span(value)) else null;
    defer {
        if (old_value_copy) |value| {
            _ = setenv(env_name, value.ptr, 1);
            std.testing.allocator.free(value);
        } else {
            _ = unsetenv(env_name);
        }
    }
    try std.testing.expectEqual(@as(c_int, 0), setenv(env_name, "1", 1));

    const fake_function: driver_mod.CUfunction = @ptrFromInt(1);
    var module: kernels_mod.KernelModule = .{
        .gqa_attention_f32 = fake_function,
        .gqa_attention_decode_f32 = fake_function,
    };
    var ctx = context_mod.CudaContext{
        .driver = .{ .lib = undefined, .fns = undefined },
        .device = 0,
        .ctx = @ptrFromInt(1),
        .stream = @ptrFromInt(1),
        .info = .{},
    };
    ctx.driver.fns.cuCtxSetCurrent = &Mocks.setCurrent;
    ctx.driver.fns.cuLaunchKernel = &Mocks.launchKernel;

    const buffer = buffer_mod.DeviceBuffer{ .ptr = 1, .len = 256 * @sizeOf(f32) };
    const kind = try module.launchGqaAttentionF32(
        &ctx,
        buffer,
        buffer,
        buffer,
        buffer,
        .{},
        .{},
        1,
        1,
        1,
        1,
        1,
        256,
        0,
        0,
        0,
        1,
        0,
        0,
    );
    try std.testing.expectEqual(kernels_mod.GqaAttentionLaunchKind.decode, kind);
    try std.testing.expect(kind != .decode_generated);
    try std.testing.expectEqual(@as(usize, 1), ctx.stats.kernel_launches);
}

test "raw CUDA attention differential surface reports output drift" {
    const reference = [_]f32{ 0.0, 1.0, -2.0, 4.0 };
    const candidate = [_]f32{ -0.0, 1.0000001, -2.0, 3.5 };
    const stats = try cuda_attention_diff.compareOutputs(&reference, &candidate);
    try std.testing.expectEqual(@as(usize, 3), stats.bitwise_mismatch_count);
    try std.testing.expectEqual(@as(usize, 0), stats.first_mismatch_index.?);
    try std.testing.expect(stats.max_abs >= 0.5);
    try std.testing.expect(stats.max_ulp > 0);
    try std.testing.expectError(error.InvalidHeadDim, cuda_attention_diff.parseConfig(&.{ "--head-dim", "128" }));
}
