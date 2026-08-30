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

const std = @import("std");
const build_options = @import("build_options");

const cuda_buffer = if (build_options.enable_cuda) @import("../ops/cuda/buffer.zig") else struct {};
const cuda_context = if (build_options.enable_cuda) @import("../ops/cuda/context.zig") else struct {};
const cuda_driver = if (build_options.enable_cuda) @import("../ops/cuda/driver.zig") else struct {};
const cuda_artifact = if (build_options.enable_cuda) @import("../ops/cuda/artifact.zig") else struct {
    const image = "";
};
const native_embed = @import("../native_embed.zig");
const backend_contracts = @import("../graph/backend_contracts.zig");
const quant_kernel_compiler = @import("../graph/quant_kernel_compiler.zig");
const quant_kernel_cuda_renderer = @import("../graph/quant_kernel_cuda_renderer.zig");
const quant_matmul = @import("../graph/quant_matmul.zig");
const quant_codec = @import("../gguf/quant_codec.zig");
const compat = @import("../io/compat.zig");

const print = std.debug.print;

const q4_k_values_per_block: usize = 256;
const q4_k_block_bytes: usize = 144;
const q8_0_values_per_block: usize = 32;
const q8_0_block_bytes: usize = 34;
const q4_0_values_per_block: usize = 32;
const q4_0_block_bytes: usize = 18;

const Shape = struct {
    label: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
};

const shapes = [_]Shape{
    .{ .label = "CLIP text proj/QKV", .rows = 77, .in_dim = 768, .out_dim = 768 },
    .{ .label = "CLIP vision proj/QKV", .rows = 257, .in_dim = 768, .out_dim = 768 },
    .{ .label = "CLIP text MLP up", .rows = 77, .in_dim = 768, .out_dim = 3072 },
    .{ .label = "CLIP text MLP down", .rows = 77, .in_dim = 3072, .out_dim = 768 },
    .{ .label = "CLAP audio MLP up", .rows = 256, .in_dim = 768, .out_dim = 3072 },
    .{ .label = "pooled projection", .rows = 1, .in_dim = 768, .out_dim = 768 },
};

const gemma4_shapes = [_]Shape{
    .{ .label = "Gemma4 Q proj", .rows = 1, .in_dim = 3840, .out_dim = 4096 },
    .{ .label = "Gemma4 KV proj", .rows = 1, .in_dim = 3840, .out_dim = 2048 },
    .{ .label = "Gemma4 attn out", .rows = 1, .in_dim = 4096, .out_dim = 3840 },
    .{ .label = "Gemma4 full attn out", .rows = 1, .in_dim = 8192, .out_dim = 3840 },
    .{ .label = "Gemma4 FFN gate/up", .rows = 1, .in_dim = 3840, .out_dim = 15360 },
    .{ .label = "Gemma4 FFN down", .rows = 1, .in_dim = 15360, .out_dim = 3840 },
    .{ .label = "Gemma4 LM head", .rows = 1, .in_dim = 3840, .out_dim = 262144 },
};

const quant_compiler_lazy_shape = Shape{ .label = "Q4_K compiler lazy", .rows = 8, .in_dim = 512, .out_dim = 768 };

// Gemma4 E2B QAT q4_0 linear_no_bias shapes observed via ANTFLY_CUDA_DISPATCH_STATS.
const quant_compiler_q4_0_dims = [_]Shape{
    .{ .label = "E2B PLE proj", .rows = 0, .in_dim = 256, .out_dim = 1536 },
    .{ .label = "E2B attn out", .rows = 0, .in_dim = 2048, .out_dim = 1536 },
    .{ .label = "E2B FFN down", .rows = 0, .in_dim = 12288, .out_dim = 1536 },
    .{ .label = "E2B FFN up", .rows = 0, .in_dim = 1536, .out_dim = 8960 },
};
const quant_compiler_q4_0_mmv_rows: usize = 1;
const quant_compiler_q4_0_mm_rows: usize = 22;

// mmv-only coverage for the largest production rows==1 dispatch (Gemma4 LM
// head). The CPU reference dequantizes the whole weight tensor to dense f32,
// which is impractical above this threshold; larger shapes are cross-checked
// generated-vs-baseline on device instead.
const quant_compiler_q4_0_mmv_extra_dims = [_]Shape{
    .{ .label = "E2B LM head", .rows = 0, .in_dim = 1536, .out_dim = 262144 },
};
const quant_compiler_q4_0_max_reference_out_dim: usize = 16384;

const q4_0_q8_1_lm_argmax_e2b_rows: usize = 1;
const q4_0_q8_1_lm_argmax_e2b_in_dim: usize = 1536;
const q4_0_q8_1_lm_argmax_e2b_out_dim: usize = 262144;
const q4_0_q8_1_lm_argmax_tile8: usize = 8;
const q4_0_q8_1_lm_argmax_stage_threads: usize = 96;
const q4_0_q8_1_lm_argmax_reduce_threads: usize = 512;
const q4_0_q8_1_lm_argmax_default_warmups: usize = 20;
const q4_0_q8_1_lm_argmax_default_iterations: usize = 200;
const q4_0_q8_1_lm_argmax_default_repeats: usize = 5;
const q4_0_q8_1_lm_argmax_candidate_symbol = "antfly_q4_0_q8_1_argmax_rows_stage1_tile8_v1";

// Gemma4 E2B QAT q4_0 prefill projection shapes for the W4A16 tensor-core
// (tc_hmma) candidate. rows is filled per run from
// q4_0_tc_hmma_e2b_row_counts.
const q4_0_tc_hmma_e2b_dims = [_]Shape{
    .{ .label = "E2B Q proj", .rows = 0, .in_dim = 1536, .out_dim = 2048 },
    .{ .label = "E2B FFN gate/up", .rows = 0, .in_dim = 1536, .out_dim = 8960 },
    .{ .label = "E2B FFN down", .rows = 0, .in_dim = 12288, .out_dim = 1536 },
};
const q4_0_tc_hmma_e2b_row_counts = [_]usize{ 64, 512 };
const q4_0_tc_hmma_candidate_symbol = "termite_linear_q4_0_f32_tc_hmma";
// BF16-fragment mirror of the f16 tensor-core kernel. Same q4_0_hmma packed
// layout and launch geometry; bf16 has f32's exponent range so it stays exact
// on Gemma's large activations where the f16 tile overflows. Timed alongside
// the f16 candidate to confirm comparable tensor-core speed.
const q4_0_tc_hmma_bf16_candidate_symbol = "termite_linear_q4_0_f32_tc_hmma_bf16";
const q4_0_tc_hmma_generated_mm_symbol = "antfly_q4_0_mm_f32_v1";
// Launch geometry contract of termite_qtc_hmma_tile (TERMITE_QTC_M/N/THREADS).
const q4_0_tc_hmma_rows_per_tile: usize = 64;
const q4_0_tc_hmma_cols_per_tile: usize = 32;
const q4_0_tc_hmma_threads: usize = 256;
const q4_0_tc_hmma_default_warmups: usize = 20;
const q4_0_tc_hmma_default_iterations: usize = 200;
const q4_0_tc_hmma_default_repeats: usize = 5;
// f32 SIMT baselines share the quant-compiler q4_0 CPU-reference tolerance;
// the tc_hmma candidate rounds activations and dequantized weights to f16, so
// it gets a wider (still absolute) budget.
const q4_0_tc_hmma_f32_tolerance_abs: f32 = 0.01;
const q4_0_tc_hmma_candidate_tolerance_abs: f32 = 0.05;
// bf16 keeps 3 fewer mantissa bits than f16 (8x coarser rounding for in-range
// activations), so the bf16 tile gets a wider sanity budget. This is not a
// precision target -- bf16's win is exponent range on Gemma-scale activations,
// not in-range precision. maxAbsDiffFinite still hard-fails on NaN/inf, so a
// broken or no-op kernel is caught regardless of this bound.
const q4_0_tc_hmma_bf16_tolerance_abs: f32 = 0.5;
// q4_0_hmma packed layout constants (see termite_q4_0_tc_value_at).
const q4_0_tc_scale_bytes: usize = 2;
const q4_0_tc_q_bytes: usize = 16;

// Strided gate/up activation-multiply sanity target (fused [rows, 2F] input
// from a concatenated gate|up GEMM vs the contiguous two-buffer kernel).
const activation_multiply_strided_symbol = "termite_activation_multiply_fused_gate_up_f32";
const activation_multiply_strided_row_counts = [_]usize{ 64, 512 };
const activation_multiply_strided_f_dims = [_]usize{ 8960, 12288 };
const activation_multiply_strided_activation: u32 = @backingInt(backend_contracts.DecoderRuntimeActivationKind.gelu_new);

const q6_k_values_per_block: usize = 256;
const q6_k_block_bytes: usize = 210;
const q6_k_q8_1_lm_argmax_rows: usize = 1;
const q6_k_q8_1_lm_argmax_out_dim: usize = 262144;
const q6_k_q8_1_lm_argmax_tile8: usize = 8;
const q6_k_q8_1_lm_argmax_reduce_threads: usize = 512;
const q6_k_q8_1_lm_argmax_default_warmups: usize = 20;
const q6_k_q8_1_lm_argmax_default_iterations: usize = 200;
const q6_k_q8_1_lm_argmax_default_repeats: usize = 5;

const Q6KQ8_1LmArgmaxBaseline = enum {
    e4b,
    generic,
};

const Q6KQ8_1LmArgmaxVariant = struct {
    in_dim: usize,
    candidate_symbol: [:0]const u8,
    candidate_threads: usize,
    baseline: Q6KQ8_1LmArgmaxBaseline,
};

const q6_k_q8_1_lm_argmax_variants = [_]Q6KQ8_1LmArgmaxVariant{
    .{
        .in_dim = 2560,
        .candidate_symbol = "antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1",
        .candidate_threads = 160,
        .baseline = .e4b,
    },
    .{
        .in_dim = 3840,
        .candidate_symbol = "antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1",
        .candidate_threads = 256,
        .baseline = .generic,
    },
};

const q4_0_q8_1_e2b_ffn_rows: usize = 1;
const q4_0_q8_1_e2b_ffn_hidden_dim: usize = 1536;
const q4_0_q8_1_e2b_ffn_activation: u32 = @backingInt(backend_contracts.DecoderRuntimeActivationKind.gelu_new);
const q4_0_q8_1_e2b_ffn_pair_threads: usize = 384;
const q4_0_q8_1_e2b_ffn_ggml_down_threads: usize = 128;
const q4_0_q8_1_e2b_ffn_production_down_threads: usize = 256;
const q4_0_q8_1_e2b_ffn_default_warmups: usize = 20;
const q4_0_q8_1_e2b_ffn_default_iterations: usize = 200;
const q4_0_q8_1_e2b_ffn_default_repeats: usize = 5;
const q4_0_q8_1_e2b_ffn_weight_ring: usize = 4;
const q4_0_q8_1_e2b_ffn_artifact_filename = "inference_cuda_kernels_sm89.cubin";
const q4_0_q8_1_e2b_ffn_pair_float_tolerance_abs: f32 = 0.001;
const q4_0_q8_1_e2b_ffn_down_tolerance_abs: f32 = 0.01;

const E2BFfnCandidateSpec = struct {
    kernel_id: []const u8,
    source_path: []const u8,
    artifact_filename: []const u8,
    threads: usize,
    cols_per_block: usize,
    production_enabled: bool = false,
};

const E2BFfnVariant = struct {
    inner_dim: usize,
    pair: E2BFfnCandidateSpec,
    down: E2BFfnCandidateSpec,
};

const q4_0_q8_1_e2b_ffn_variants = [_]E2BFfnVariant{
    .{
        .inner_dim = 6144,
        .pair = .{
            .kernel_id = quant_kernel_compiler.first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_kernel_id,
            .source_path = quant_kernel_compiler.first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_source_path,
            .artifact_filename = q4_0_q8_1_e2b_ffn_artifact_filename,
            .threads = q4_0_q8_1_e2b_ffn_pair_threads,
            .cols_per_block = 32,
        },
        .down = .{
            .kernel_id = quant_kernel_compiler.first_e2b_cuda_q4_0_down_ggml_q8_1_6144_kernel_id,
            .source_path = quant_kernel_compiler.first_e2b_cuda_q4_0_down_ggml_q8_1_6144_source_path,
            .artifact_filename = q4_0_q8_1_e2b_ffn_artifact_filename,
            .threads = q4_0_q8_1_e2b_ffn_ggml_down_threads,
            .cols_per_block = 1,
        },
    },
    .{
        .inner_dim = 12288,
        .pair = .{
            .kernel_id = quant_kernel_compiler.first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_kernel_id,
            .source_path = quant_kernel_compiler.first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_source_path,
            .artifact_filename = q4_0_q8_1_e2b_ffn_artifact_filename,
            .threads = q4_0_q8_1_e2b_ffn_pair_threads,
            .cols_per_block = 32,
        },
        .down = .{
            .kernel_id = quant_kernel_compiler.first_e2b_cuda_q4_0_down_ggml_q8_1_12288_kernel_id,
            .source_path = quant_kernel_compiler.first_e2b_cuda_q4_0_down_ggml_q8_1_12288_source_path,
            .artifact_filename = q4_0_q8_1_e2b_ffn_artifact_filename,
            .threads = q4_0_q8_1_e2b_ffn_ggml_down_threads,
            .cols_per_block = 1,
        },
    },
};

const q4_0_ggml_q8_1_quantizer_spec = E2BFfnCandidateSpec{
    .kernel_id = quant_kernel_cuda_renderer.ggml_q8_1_quantize_rows_kernel_id,
    .source_path = "src/ops/cuda/artifacts/inference_cuda_kernels.cu",
    .artifact_filename = q4_0_q8_1_e2b_ffn_artifact_filename,
    .threads = 32,
    .cols_per_block = 32,
};

const TimingRunSummary = struct {
    median_ns: u64,
    mean_ns: f64,
    stddev_ns: f64,
    cv_percent: f64,
};

fn summarizeTimingRuns(runs: []const u64) !TimingRunSummary {
    if (runs.len == 0 or runs.len > 31) return error.InvalidTimingRuns;
    var sorted: [31]u64 = undefined;
    @memcpy(sorted[0..runs.len], runs);
    std.mem.sort(u64, sorted[0..runs.len], {}, std.sort.asc(u64));

    var sum: f64 = 0;
    for (runs) |run| sum += @floatFromInt(run);
    const mean = sum / @as(f64, @floatFromInt(runs.len));
    var variance_sum: f64 = 0;
    for (runs) |run| {
        const delta = @as(f64, @floatFromInt(run)) - mean;
        variance_sum += delta * delta;
    }
    const stddev = @sqrt(variance_sum / @as(f64, @floatFromInt(runs.len)));
    return .{
        .median_ns = sorted[runs.len / 2],
        .mean_ns = mean,
        .stddev_ns = stddev,
        .cv_percent = if (mean == 0) 0 else (stddev / mean) * 100.0,
    };
}

const QuantCompilerQ4_0Kind = enum { mmv, mm };

const Config = struct {
    warmup_iters: usize = 5,
    measure_iters: usize = 50,
    model_path: ?[]const u8 = null,
    text: []const u8 = "a photo of a document with audio metadata",
    full_iters: usize = 1,
    gemma4_shapes: bool = false,
    q4_0_tc_hmma_e2b: bool = false,
    activation_multiply_strided_e2b: bool = false,
    q4_0_q8_1_lm_argmax_e2b: bool = false,
    q6_k_q8_1_lm_argmax: bool = false,
    q4_0_q8_1_e2b_ffn_sm89_dir: ?[]const u8 = null,
    quant_compiler_lazy_target: bool = false,
    quant_compiler_generated_ptx_path: ?[]const u8 = null,
    quant_compiler_q4_0_mmv_ptx_path: ?[]const u8 = null,
    quant_compiler_q4_0_mm_ptx_path: ?[]const u8 = null,
    quant_compiler_q4_0_pair_ptx_path: ?[]const u8 = null,
    quant_compiler_q4_0_pair_q8_ptx_path: ?[]const u8 = null,
    quant_compiler_q4_0_down_q8_ptx_path: ?[]const u8 = null,
    quant_compiler_evidence_out_path: ?[]const u8 = null,
    quant_compiler_check_evidence_path: ?[]const u8 = null,
    quant_compiler_require_promotion_ready: bool = false,
    quant_compiler_repeat_runs: usize = 1,
    json_out_path: ?[]const u8 = null,
};

const quant_compiler_evidence_repeat_runs: usize = 3;

const BenchModule = if (build_options.enable_cuda) struct {
    module: cuda_driver.CUmodule = null,
    linear_q4_k_f32: cuda_driver.CUfunction = null,
    linear_q4_k_bias_f32: cuda_driver.CUfunction = null,
    linear_q8_0_f32: cuda_driver.CUfunction = null,
    linear_q8_0_f32_tile4: cuda_driver.CUfunction = null,
    linear_q4_k_f32_tiled: cuda_driver.CUfunction = null,
    linear_q4_k_bias_f32_tiled: cuda_driver.CUfunction = null,
    linear_q4_k_bias_quick_gelu_f32_tiled: cuda_driver.CUfunction = null,
    linear_q4_k_f32_tile4: cuda_driver.CUfunction = null,
    linear_q4_k_f32_tile8: cuda_driver.CUfunction = null,
    linear_q4_k_bias_f32_tile4: cuda_driver.CUfunction = null,
    linear_q4_k_bias_gelu_f32_tile4_r2: cuda_driver.CUfunction = null,
    linear_q4_k_bias_quick_gelu_f32_tile4: cuda_driver.CUfunction = null,
    linear_q4_k_triple_bias_f32: cuda_driver.CUfunction = null,
    linear_q4_k_triple_bias_f32_tiled: cuda_driver.CUfunction = null,
    linear_q4_0_f32: cuda_driver.CUfunction = null,
    linear_q4_0_f32_tile4: cuda_driver.CUfunction = null,
    linear_q4_0_f32_tc_hmma: cuda_driver.CUfunction = null,
    linear_q4_0_f32_tc_hmma_bf16: cuda_driver.CUfunction = null,
    generated_q4_0_mm_f32: cuda_driver.CUfunction = null,
    activation_multiply_f32: cuda_driver.CUfunction = null,
    activation_multiply_fused_gate_up_f32: cuda_driver.CUfunction = null,
    linear_q4_0_pair_nobias_f32_tile4_w4: cuda_driver.CUfunction = null,
    linear_q4_0_pair_activation_q8_1_e4b: cuda_driver.CUfunction = null,
    linear_q4_0_q8_1_e4b_down: cuda_driver.CUfunction = null,
    linear_q4_0_pair_activation_q8_1_f32_tile4: cuda_driver.CUfunction = null,
    linear_q4_0_q8_1_f32_tile4_w8: cuda_driver.CUfunction = null,
    quantize_f32_q8_1_rows: cuda_driver.CUfunction = null,
    linear_q4_0_q8_1_f32_tile4: cuda_driver.CUfunction = null,
    argmax_last_row_f32: cuda_driver.CUfunction = null,
    argmax_reduce_rows_pairs_f32_w16: cuda_driver.CUfunction = null,
    linear_q4_0_q8_1_argmax_rows_stage1_tile8_e2b: cuda_driver.CUfunction = null,
    linear_q6_k_q8_1_argmax_rows_stage1_tile8: cuda_driver.CUfunction = null,
    linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b: cuda_driver.CUfunction = null,
    linear_q6_k_q8_1_argmax_generated_k2560: cuda_driver.CUfunction = null,
    linear_q6_k_q8_1_argmax_generated_k3840: cuda_driver.CUfunction = null,

    fn loadOptional(ctx: *cuda_context.CudaContext, module: cuda_driver.CUmodule, name: [*:0]const u8) cuda_driver.CUfunction {
        var function: cuda_driver.CUfunction = null;
        ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, name)) catch return null;
        return function;
    }

    fn load(ctx: *cuda_context.CudaContext) cuda_driver.Error!BenchModule {
        try ctx.makeCurrent();
        var module: cuda_driver.CUmodule = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, cuda_artifact.image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);

        var linear_q4_k_f32: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_f32, module, "termite_linear_q4_k_f32"));
        var linear_q4_k_bias_f32: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_f32, module, "termite_linear_q4_k_bias_f32"));
        var linear_q8_0_f32: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q8_0_f32, module, "termite_linear_q8_0_f32"));
        var linear_q8_0_f32_tile4: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q8_0_f32_tile4, module, "termite_linear_q8_0_f32_tile4"));
        var linear_q4_k_f32_tiled: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_f32_tiled, module, "termite_linear_q4_k_f32_tiled"));
        var linear_q4_k_bias_f32_tiled: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_f32_tiled, module, "termite_linear_q4_k_bias_f32_tiled"));
        var linear_q4_k_bias_quick_gelu_f32_tiled: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_quick_gelu_f32_tiled, module, "termite_linear_q4_k_bias_quick_gelu_f32_tiled"));
        var linear_q4_k_f32_tile4: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_f32_tile4, module, "termite_linear_q4_k_f32_tile4"));
        var linear_q4_k_f32_tile8: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_f32_tile8, module, "termite_linear_q4_k_f32_tile8"));
        var linear_q4_k_bias_f32_tile4: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_f32_tile4, module, "termite_linear_q4_k_bias_f32_tile4"));
        var linear_q4_k_bias_gelu_f32_tile4_r2: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_gelu_f32_tile4_r2, module, "termite_linear_q4_k_bias_gelu_f32_tile4_r2"));
        var linear_q4_k_bias_quick_gelu_f32_tile4: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_quick_gelu_f32_tile4, module, "termite_linear_q4_k_bias_quick_gelu_f32_tile4"));
        var linear_q4_k_triple_bias_f32: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_triple_bias_f32, module, "termite_linear_q4_k_triple_bias_f32"));
        var linear_q4_k_triple_bias_f32_tiled: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_triple_bias_f32_tiled, module, "termite_linear_q4_k_triple_bias_f32_tiled"));
        var linear_q4_0_f32: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_0_f32, module, "termite_linear_q4_0_f32"));
        var linear_q4_0_f32_tile4: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_0_f32_tile4, module, "termite_linear_q4_0_f32_tile4"));
        const linear_q4_0_f32_tc_hmma = loadOptional(ctx, module, q4_0_tc_hmma_candidate_symbol);
        const linear_q4_0_f32_tc_hmma_bf16 = loadOptional(ctx, module, q4_0_tc_hmma_bf16_candidate_symbol);
        const generated_q4_0_mm_f32 = loadOptional(ctx, module, q4_0_tc_hmma_generated_mm_symbol);
        var activation_multiply_f32: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&activation_multiply_f32, module, "termite_activation_multiply_f32"));
        const activation_multiply_fused_gate_up_f32 = loadOptional(ctx, module, activation_multiply_strided_symbol);
        var linear_q4_0_pair_nobias_f32_tile4_w4: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_0_pair_nobias_f32_tile4_w4, module, "termite_linear_q4_0_pair_nobias_f32_tile4_w4"));
        var linear_q4_0_pair_activation_q8_1_e4b: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_0_pair_activation_q8_1_e4b, module, "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn"));
        var linear_q4_0_q8_1_e4b_down: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_0_q8_1_e4b_down, module, "termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down"));
        var linear_q4_0_pair_activation_q8_1_f32_tile4: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_0_pair_activation_q8_1_f32_tile4, module, "termite_linear_q4_0_pair_activation_q8_1_f32_tile4"));
        var linear_q4_0_q8_1_f32_tile4_w8: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_0_q8_1_f32_tile4_w8, module, "termite_linear_q4_0_q8_1_f32_tile4_w8"));
        var quantize_f32_q8_1_rows: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&quantize_f32_q8_1_rows, module, "termite_quantize_f32_q8_1_rows"));
        var linear_q4_0_q8_1_f32_tile4: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_0_q8_1_f32_tile4, module, "termite_linear_q4_0_q8_1_f32_tile4"));
        var argmax_last_row_f32: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&argmax_last_row_f32, module, "termite_argmax_last_row_f32"));
        var argmax_reduce_rows_pairs_f32_w16: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&argmax_reduce_rows_pairs_f32_w16, module, "termite_argmax_reduce_rows_pairs_f32_w16"));
        const linear_q4_0_q8_1_argmax_rows_stage1_tile8_e2b = loadOptional(ctx, module, q4_0_q8_1_lm_argmax_candidate_symbol);
        const linear_q6_k_q8_1_argmax_rows_stage1_tile8 = loadOptional(ctx, module, "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8");
        const linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b = loadOptional(ctx, module, "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b");
        const linear_q6_k_q8_1_argmax_generated_k2560 = loadOptional(ctx, module, q6_k_q8_1_lm_argmax_variants[0].candidate_symbol);
        const linear_q6_k_q8_1_argmax_generated_k3840 = loadOptional(ctx, module, q6_k_q8_1_lm_argmax_variants[1].candidate_symbol);

        return .{
            .module = module,
            .linear_q4_k_f32 = linear_q4_k_f32,
            .linear_q4_k_bias_f32 = linear_q4_k_bias_f32,
            .linear_q8_0_f32 = linear_q8_0_f32,
            .linear_q8_0_f32_tile4 = linear_q8_0_f32_tile4,
            .linear_q4_k_f32_tiled = linear_q4_k_f32_tiled,
            .linear_q4_k_bias_f32_tiled = linear_q4_k_bias_f32_tiled,
            .linear_q4_k_bias_quick_gelu_f32_tiled = linear_q4_k_bias_quick_gelu_f32_tiled,
            .linear_q4_k_f32_tile4 = linear_q4_k_f32_tile4,
            .linear_q4_k_f32_tile8 = linear_q4_k_f32_tile8,
            .linear_q4_k_bias_f32_tile4 = linear_q4_k_bias_f32_tile4,
            .linear_q4_k_bias_gelu_f32_tile4_r2 = linear_q4_k_bias_gelu_f32_tile4_r2,
            .linear_q4_k_bias_quick_gelu_f32_tile4 = linear_q4_k_bias_quick_gelu_f32_tile4,
            .linear_q4_k_triple_bias_f32 = linear_q4_k_triple_bias_f32,
            .linear_q4_k_triple_bias_f32_tiled = linear_q4_k_triple_bias_f32_tiled,
            .linear_q4_0_f32 = linear_q4_0_f32,
            .linear_q4_0_f32_tile4 = linear_q4_0_f32_tile4,
            .linear_q4_0_f32_tc_hmma = linear_q4_0_f32_tc_hmma,
            .linear_q4_0_f32_tc_hmma_bf16 = linear_q4_0_f32_tc_hmma_bf16,
            .generated_q4_0_mm_f32 = generated_q4_0_mm_f32,
            .activation_multiply_f32 = activation_multiply_f32,
            .activation_multiply_fused_gate_up_f32 = activation_multiply_fused_gate_up_f32,
            .linear_q4_0_pair_nobias_f32_tile4_w4 = linear_q4_0_pair_nobias_f32_tile4_w4,
            .linear_q4_0_pair_activation_q8_1_e4b = linear_q4_0_pair_activation_q8_1_e4b,
            .linear_q4_0_q8_1_e4b_down = linear_q4_0_q8_1_e4b_down,
            .linear_q4_0_pair_activation_q8_1_f32_tile4 = linear_q4_0_pair_activation_q8_1_f32_tile4,
            .linear_q4_0_q8_1_f32_tile4_w8 = linear_q4_0_q8_1_f32_tile4_w8,
            .quantize_f32_q8_1_rows = quantize_f32_q8_1_rows,
            .linear_q4_0_q8_1_f32_tile4 = linear_q4_0_q8_1_f32_tile4,
            .argmax_last_row_f32 = argmax_last_row_f32,
            .argmax_reduce_rows_pairs_f32_w16 = argmax_reduce_rows_pairs_f32_w16,
            .linear_q4_0_q8_1_argmax_rows_stage1_tile8_e2b = linear_q4_0_q8_1_argmax_rows_stage1_tile8_e2b,
            .linear_q6_k_q8_1_argmax_rows_stage1_tile8 = linear_q6_k_q8_1_argmax_rows_stage1_tile8,
            .linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b = linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b,
            .linear_q6_k_q8_1_argmax_generated_k2560 = linear_q6_k_q8_1_argmax_generated_k2560,
            .linear_q6_k_q8_1_argmax_generated_k3840 = linear_q6_k_q8_1_argmax_generated_k3840,
        };
    }

    fn unload(self: *BenchModule, ctx: *cuda_context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
            self.module = null;
            self.linear_q4_k_f32 = null;
            self.linear_q4_k_bias_f32 = null;
            self.linear_q8_0_f32 = null;
            self.linear_q8_0_f32_tile4 = null;
            self.linear_q4_k_f32_tiled = null;
            self.linear_q4_k_bias_f32_tiled = null;
            self.linear_q4_k_bias_quick_gelu_f32_tiled = null;
            self.linear_q4_k_f32_tile4 = null;
            self.linear_q4_k_f32_tile8 = null;
            self.linear_q4_k_bias_f32_tile4 = null;
            self.linear_q4_k_bias_gelu_f32_tile4_r2 = null;
            self.linear_q4_k_bias_quick_gelu_f32_tile4 = null;
            self.linear_q4_k_triple_bias_f32 = null;
            self.linear_q4_k_triple_bias_f32_tiled = null;
            self.linear_q4_0_f32 = null;
            self.linear_q4_0_f32_tile4 = null;
            self.linear_q4_0_f32_tc_hmma = null;
            self.linear_q4_0_f32_tc_hmma_bf16 = null;
            self.generated_q4_0_mm_f32 = null;
            self.activation_multiply_f32 = null;
            self.activation_multiply_fused_gate_up_f32 = null;
            self.linear_q4_0_pair_nobias_f32_tile4_w4 = null;
            self.linear_q4_0_pair_activation_q8_1_e4b = null;
            self.linear_q4_0_q8_1_e4b_down = null;
            self.linear_q4_0_pair_activation_q8_1_f32_tile4 = null;
            self.linear_q4_0_q8_1_f32_tile4_w8 = null;
            self.quantize_f32_q8_1_rows = null;
            self.linear_q4_0_q8_1_f32_tile4 = null;
            self.argmax_last_row_f32 = null;
            self.argmax_reduce_rows_pairs_f32_w16 = null;
            self.linear_q4_0_q8_1_argmax_rows_stage1_tile8_e2b = null;
            self.linear_q6_k_q8_1_argmax_rows_stage1_tile8 = null;
            self.linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b = null;
            self.linear_q6_k_q8_1_argmax_generated_k2560 = null;
            self.linear_q6_k_q8_1_argmax_generated_k3840 = null;
        }
    }
} else struct {};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    if (wantsHelp(args)) {
        printUsage();
        return;
    }
    const cfg = try parseArgs(args);
    if (cfg.quant_compiler_check_evidence_path) |path| {
        try checkQuantCompilerEvidenceFile(allocator, io, path, cfg.quant_compiler_require_promotion_ready);
        print("bench-cuda quant compiler evidence_check={s} ok\n", .{path});
        return;
    }
    if (!build_options.enable_cuda) {
        print("bench-cuda requires a build with -Dcuda=true\n", .{});
        return error.CudaUnavailable;
    }

    try runKernelBench(allocator, io, cfg);
    if (cfg.model_path) |model_path| {
        try runFullTextEmbedBench(allocator, io, cfg, model_path);
    }
}

fn parseArgs(args: []const []const u8) !Config {
    var cfg = Config{};
    var seen_quant_compiler_lazy_target = false;
    var seen_benchmark_option = false;
    var seen_warmup_iters = false;
    var seen_measure_iters = false;
    var seen_repeat_runs = false;
    var seen_standard_benchmark_option = false;
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--warmup-iters")) {
            seen_benchmark_option = true;
            seen_warmup_iters = true;
            i += 1;
            if (i >= args.len) return error.MissingWarmupIters;
            cfg.warmup_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            seen_benchmark_option = true;
            seen_measure_iters = true;
            i += 1;
            if (i >= args.len) return error.MissingMeasureIters;
            cfg.measure_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--model")) {
            seen_benchmark_option = true;
            seen_standard_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingModelPath;
            cfg.model_path = args[i];
        } else if (std.mem.eql(u8, arg, "--text")) {
            seen_benchmark_option = true;
            seen_standard_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingText;
            cfg.text = args[i];
        } else if (std.mem.eql(u8, arg, "--full-iters")) {
            seen_benchmark_option = true;
            seen_standard_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingFullIters;
            cfg.full_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--gemma4-shapes")) {
            seen_benchmark_option = true;
            seen_standard_benchmark_option = true;
            cfg.gemma4_shapes = true;
        } else if (std.mem.eql(u8, arg, "--q4-0-tc-hmma-e2b")) {
            if (cfg.q4_0_tc_hmma_e2b) return error.DuplicateQ4_0TcHmmaE2B;
            seen_benchmark_option = true;
            cfg.q4_0_tc_hmma_e2b = true;
        } else if (std.mem.eql(u8, arg, "--activation-multiply-strided-e2b")) {
            if (cfg.activation_multiply_strided_e2b) return error.DuplicateActivationMultiplyStridedE2B;
            seen_benchmark_option = true;
            cfg.activation_multiply_strided_e2b = true;
        } else if (std.mem.eql(u8, arg, "--q4-0-q8-1-lm-argmax-e2b")) {
            if (cfg.q4_0_q8_1_lm_argmax_e2b) return error.DuplicateQ4_0Q8_1LmArgmaxE2B;
            seen_benchmark_option = true;
            cfg.q4_0_q8_1_lm_argmax_e2b = true;
        } else if (std.mem.eql(u8, arg, "--q6-k-q8-1-lm-argmax")) {
            if (cfg.q6_k_q8_1_lm_argmax) return error.DuplicateQ6KQ8_1LmArgmax;
            seen_benchmark_option = true;
            cfg.q6_k_q8_1_lm_argmax = true;
        } else if (std.mem.eql(u8, arg, "--q4-0-q8-1-e2b-ffn-sm89")) {
            if (cfg.q4_0_q8_1_e2b_ffn_sm89_dir != null) return error.DuplicateQ4_0Q8_1E2BFfnSm89Dir;
            seen_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingQ4_0Q8_1E2BFfnSm89Dir;
            if (args[i].len == 0) return error.MissingQ4_0Q8_1E2BFfnSm89Dir;
            cfg.q4_0_q8_1_e2b_ffn_sm89_dir = args[i];
        } else if (std.mem.eql(u8, arg, "--quant-compiler-lazy-target")) {
            if (seen_quant_compiler_lazy_target) return error.DuplicateQuantCompilerLazyTarget;
            seen_quant_compiler_lazy_target = true;
            seen_benchmark_option = true;
            cfg.quant_compiler_lazy_target = true;
        } else if (std.mem.eql(u8, arg, "--quant-compiler-generated-ptx")) {
            if (cfg.quant_compiler_generated_ptx_path != null) return error.DuplicateGeneratedPtxPath;
            seen_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingGeneratedPtxPath;
            cfg.quant_compiler_lazy_target = true;
            cfg.quant_compiler_generated_ptx_path = args[i];
        } else if (std.mem.eql(u8, arg, "--quant-compiler-q4-0-mmv-ptx")) {
            if (cfg.quant_compiler_q4_0_mmv_ptx_path != null) return error.DuplicateQuantCompilerQ4_0MmvPtxPath;
            seen_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingQuantCompilerQ4_0MmvPtxPath;
            cfg.quant_compiler_q4_0_mmv_ptx_path = args[i];
        } else if (std.mem.eql(u8, arg, "--quant-compiler-q4-0-mm-ptx")) {
            if (cfg.quant_compiler_q4_0_mm_ptx_path != null) return error.DuplicateQuantCompilerQ4_0MmPtxPath;
            seen_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingQuantCompilerQ4_0MmPtxPath;
            cfg.quant_compiler_q4_0_mm_ptx_path = args[i];
        } else if (std.mem.eql(u8, arg, "--quant-compiler-q4-0-pair-ptx")) {
            if (cfg.quant_compiler_q4_0_pair_ptx_path != null) return error.DuplicateQuantCompilerQ4_0PairPtxPath;
            seen_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingQuantCompilerQ4_0PairPtxPath;
            cfg.quant_compiler_q4_0_pair_ptx_path = args[i];
        } else if (std.mem.eql(u8, arg, "--quant-compiler-q4-0-pair-q8-ptx")) {
            if (cfg.quant_compiler_q4_0_pair_q8_ptx_path != null) return error.DuplicateQuantCompilerQ4_0PairQ8PtxPath;
            seen_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingQuantCompilerQ4_0PairQ8PtxPath;
            cfg.quant_compiler_q4_0_pair_q8_ptx_path = args[i];
        } else if (std.mem.eql(u8, arg, "--quant-compiler-q4-0-down-q8-ptx")) {
            if (cfg.quant_compiler_q4_0_down_q8_ptx_path != null) return error.DuplicateQuantCompilerQ4_0DownQ8PtxPath;
            seen_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingQuantCompilerQ4_0DownQ8PtxPath;
            cfg.quant_compiler_q4_0_down_q8_ptx_path = args[i];
        } else if (std.mem.eql(u8, arg, "--quant-compiler-evidence-out")) {
            if (cfg.quant_compiler_evidence_out_path != null) return error.DuplicateQuantCompilerEvidenceOutPath;
            seen_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingQuantCompilerEvidenceOutPath;
            cfg.quant_compiler_evidence_out_path = args[i];
        } else if (std.mem.eql(u8, arg, "--quant-compiler-check-evidence")) {
            if (cfg.quant_compiler_check_evidence_path != null) return error.DuplicateQuantCompilerCheckEvidencePath;
            i += 1;
            if (i >= args.len) return error.MissingQuantCompilerCheckEvidencePath;
            cfg.quant_compiler_check_evidence_path = args[i];
        } else if (std.mem.eql(u8, arg, "--quant-compiler-require-promotion-ready")) {
            if (cfg.quant_compiler_require_promotion_ready) return error.DuplicateQuantCompilerRequirePromotionReady;
            cfg.quant_compiler_require_promotion_ready = true;
        } else if (std.mem.eql(u8, arg, "--quant-compiler-repeat-runs") or std.mem.eql(u8, arg, "--repeat-runs")) {
            if (seen_repeat_runs) return error.DuplicateQuantCompilerRepeatRuns;
            seen_repeat_runs = true;
            seen_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingQuantCompilerRepeatRuns;
            cfg.quant_compiler_repeat_runs = try parseQuantCompilerRepeatRuns(args[i]);
        } else if (std.mem.eql(u8, arg, "--json-out")) {
            seen_benchmark_option = true;
            seen_standard_benchmark_option = true;
            i += 1;
            if (i >= args.len) return error.MissingJsonOutPath;
            cfg.json_out_path = args[i];
        } else {
            print("unknown bench-cuda argument: {s}\n", .{arg});
            printUsage();
            return error.InvalidArgument;
        }
    }
    if (cfg.quant_compiler_require_promotion_ready and cfg.quant_compiler_check_evidence_path == null) return error.QuantCompilerPromotionReadyRequiresCheckEvidence;
    if (cfg.quant_compiler_check_evidence_path != null) {
        if (seen_benchmark_option) return error.QuantCompilerCheckEvidenceConflictsWithBenchmark;
        return cfg;
    }
    if (cfg.q4_0_q8_1_lm_argmax_e2b) {
        if (!seen_warmup_iters) cfg.warmup_iters = q4_0_q8_1_lm_argmax_default_warmups;
        if (!seen_measure_iters) cfg.measure_iters = q4_0_q8_1_lm_argmax_default_iterations;
        if (!seen_repeat_runs) cfg.quant_compiler_repeat_runs = q4_0_q8_1_lm_argmax_default_repeats;
    }
    if (cfg.q4_0_tc_hmma_e2b or cfg.activation_multiply_strided_e2b) {
        if (!seen_warmup_iters) cfg.warmup_iters = q4_0_tc_hmma_default_warmups;
        if (!seen_measure_iters) cfg.measure_iters = q4_0_tc_hmma_default_iterations;
        if (!seen_repeat_runs) cfg.quant_compiler_repeat_runs = q4_0_tc_hmma_default_repeats;
    }
    if (cfg.q6_k_q8_1_lm_argmax) {
        if (!seen_warmup_iters) cfg.warmup_iters = q6_k_q8_1_lm_argmax_default_warmups;
        if (!seen_measure_iters) cfg.measure_iters = q6_k_q8_1_lm_argmax_default_iterations;
        if (!seen_repeat_runs) cfg.quant_compiler_repeat_runs = q6_k_q8_1_lm_argmax_default_repeats;
    }
    if (cfg.q4_0_q8_1_e2b_ffn_sm89_dir != null) {
        if (!seen_warmup_iters) cfg.warmup_iters = q4_0_q8_1_e2b_ffn_default_warmups;
        if (!seen_measure_iters) cfg.measure_iters = q4_0_q8_1_e2b_ffn_default_iterations;
        if (!seen_repeat_runs) cfg.quant_compiler_repeat_runs = q4_0_q8_1_e2b_ffn_default_repeats;
    }
    if (cfg.measure_iters == 0) return error.InvalidArgument;
    if (cfg.full_iters == 0) return error.InvalidArgument;
    if (cfg.quant_compiler_lazy_target and cfg.quant_compiler_generated_ptx_path == null) return error.MissingGeneratedPtxPath;
    if (cfg.quant_compiler_lazy_target and (cfg.quant_compiler_q4_0_mmv_ptx_path != null or cfg.quant_compiler_q4_0_mm_ptx_path != null or cfg.quant_compiler_q4_0_pair_ptx_path != null or cfg.quant_compiler_q4_0_pair_q8_ptx_path != null or cfg.quant_compiler_q4_0_down_q8_ptx_path != null)) return error.QuantCompilerQ4_0ConflictsWithLazyTarget;
    const q4_0_target_count: usize = @as(usize, @intFromBool(cfg.quant_compiler_q4_0_mmv_ptx_path != null)) + @as(usize, @intFromBool(cfg.quant_compiler_q4_0_mm_ptx_path != null)) + @as(usize, @intFromBool(cfg.quant_compiler_q4_0_pair_ptx_path != null)) + @as(usize, @intFromBool(cfg.quant_compiler_q4_0_pair_q8_ptx_path != null)) + @as(usize, @intFromBool(cfg.quant_compiler_q4_0_down_q8_ptx_path != null));
    if (cfg.q4_0_tc_hmma_e2b and (cfg.activation_multiply_strided_e2b or cfg.q4_0_q8_1_lm_argmax_e2b or cfg.q6_k_q8_1_lm_argmax or cfg.q4_0_q8_1_e2b_ffn_sm89_dir != null or cfg.quant_compiler_lazy_target or q4_0_target_count != 0 or cfg.quant_compiler_evidence_out_path != null or seen_standard_benchmark_option)) return error.Q4_0TcHmmaE2BConflictsWithOtherBenchmark;
    if (cfg.activation_multiply_strided_e2b and (cfg.q4_0_q8_1_lm_argmax_e2b or cfg.q6_k_q8_1_lm_argmax or cfg.q4_0_q8_1_e2b_ffn_sm89_dir != null or cfg.quant_compiler_lazy_target or q4_0_target_count != 0 or cfg.quant_compiler_evidence_out_path != null or seen_standard_benchmark_option)) return error.ActivationMultiplyStridedE2BConflictsWithOtherBenchmark;
    if (cfg.q4_0_q8_1_lm_argmax_e2b and (cfg.q6_k_q8_1_lm_argmax or cfg.quant_compiler_lazy_target or q4_0_target_count != 0 or cfg.quant_compiler_evidence_out_path != null or cfg.gemma4_shapes or cfg.model_path != null or cfg.json_out_path != null)) return error.Q4_0Q8_1LmArgmaxE2BConflictsWithOtherBenchmark;
    if (cfg.q6_k_q8_1_lm_argmax and (cfg.quant_compiler_lazy_target or q4_0_target_count != 0 or cfg.quant_compiler_evidence_out_path != null or seen_standard_benchmark_option or cfg.q4_0_q8_1_e2b_ffn_sm89_dir != null)) return error.Q6KQ8_1LmArgmaxConflictsWithOtherBenchmark;
    if (cfg.q4_0_q8_1_e2b_ffn_sm89_dir != null and (cfg.q4_0_q8_1_lm_argmax_e2b or cfg.q6_k_q8_1_lm_argmax or cfg.quant_compiler_lazy_target or q4_0_target_count != 0 or cfg.quant_compiler_evidence_out_path != null or seen_standard_benchmark_option)) return error.Q4_0Q8_1E2BFfnSm89ConflictsWithOtherBenchmark;
    if (cfg.quant_compiler_evidence_out_path != null and !cfg.quant_compiler_lazy_target and q4_0_target_count == 0) return error.QuantCompilerEvidenceRequiresLazyTarget;
    if (cfg.quant_compiler_evidence_out_path != null and q4_0_target_count > 1) return error.QuantCompilerEvidenceRequiresSingleQ4_0Target;
    if (cfg.quant_compiler_evidence_out_path != null and (cfg.warmup_iters != 5 or cfg.measure_iters != 50)) return error.QuantCompilerEvidenceRequiresManifestIterations;
    if (cfg.quant_compiler_evidence_out_path != null and cfg.quant_compiler_repeat_runs != quant_compiler_evidence_repeat_runs) return error.QuantCompilerEvidenceRequiresManifestRepeatRuns;
    return cfg;
}

fn parseQuantCompilerRepeatRuns(text: []const u8) !usize {
    const runs = try std.fmt.parseInt(usize, text, 10);
    if (runs == 0 or runs > 31) return error.InvalidQuantCompilerRepeatRuns;
    return runs;
}

fn wantsHelp(args: []const []const u8) bool {
    for (args) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) return true;
    }
    return false;
}

test "cuda microbench quant compiler flags select lazy target" {
    const cfg = try parseArgs(&.{ "--quant-compiler-generated-ptx", "/tmp/generated.ptx", "--quant-compiler-repeat-runs", "3", "--quant-compiler-evidence-out", "/tmp/evidence.json" });
    try std.testing.expect(cfg.quant_compiler_lazy_target);
    try std.testing.expectEqualStrings("/tmp/generated.ptx", cfg.quant_compiler_generated_ptx_path.?);
    try std.testing.expectEqualStrings("/tmp/evidence.json", cfg.quant_compiler_evidence_out_path.?);
    try std.testing.expectEqual(@as(usize, 3), cfg.quant_compiler_repeat_runs);
    try std.testing.expectError(error.MissingGeneratedPtxPath, parseArgs(&.{"--quant-compiler-generated-ptx"}));
    try std.testing.expectError(error.MissingGeneratedPtxPath, parseArgs(&.{"--quant-compiler-lazy-target"}));
    try std.testing.expectError(error.MissingQuantCompilerEvidenceOutPath, parseArgs(&.{"--quant-compiler-evidence-out"}));
    const check_cfg = try parseArgs(&.{ "--quant-compiler-check-evidence", "/tmp/evidence.json" });
    try std.testing.expectEqualStrings("/tmp/evidence.json", check_cfg.quant_compiler_check_evidence_path.?);
    const promotion_check_cfg = try parseArgs(&.{ "--quant-compiler-check-evidence", "/tmp/evidence.json", "--quant-compiler-require-promotion-ready" });
    try std.testing.expect(promotion_check_cfg.quant_compiler_require_promotion_ready);
    try std.testing.expectError(error.MissingQuantCompilerCheckEvidencePath, parseArgs(&.{"--quant-compiler-check-evidence"}));
    try std.testing.expectError(error.DuplicateQuantCompilerLazyTarget, parseArgs(&.{ "--quant-compiler-lazy-target", "--quant-compiler-lazy-target" }));
    try std.testing.expectError(error.DuplicateGeneratedPtxPath, parseArgs(&.{ "--quant-compiler-generated-ptx", "/tmp/a.ptx", "--quant-compiler-generated-ptx", "/tmp/b.ptx" }));
    try std.testing.expectError(error.DuplicateQuantCompilerEvidenceOutPath, parseArgs(&.{ "--quant-compiler-evidence-out", "/tmp/a.json", "--quant-compiler-evidence-out", "/tmp/b.json" }));
    try std.testing.expectError(error.DuplicateQuantCompilerCheckEvidencePath, parseArgs(&.{ "--quant-compiler-check-evidence", "/tmp/a.json", "--quant-compiler-check-evidence", "/tmp/b.json" }));
    try std.testing.expectError(error.DuplicateQuantCompilerRequirePromotionReady, parseArgs(&.{ "--quant-compiler-check-evidence", "/tmp/evidence.json", "--quant-compiler-require-promotion-ready", "--quant-compiler-require-promotion-ready" }));
    try std.testing.expectError(error.QuantCompilerPromotionReadyRequiresCheckEvidence, parseArgs(&.{"--quant-compiler-require-promotion-ready"}));
    try std.testing.expectError(error.QuantCompilerCheckEvidenceConflictsWithBenchmark, parseArgs(&.{ "--quant-compiler-check-evidence", "/tmp/evidence.json", "--quant-compiler-lazy-target" }));
    try std.testing.expectError(error.QuantCompilerCheckEvidenceConflictsWithBenchmark, parseArgs(&.{ "--quant-compiler-check-evidence", "/tmp/evidence.json", "--quant-compiler-generated-ptx", "/tmp/generated.ptx" }));
    try std.testing.expectError(error.QuantCompilerCheckEvidenceConflictsWithBenchmark, parseArgs(&.{ "--quant-compiler-check-evidence", "/tmp/evidence.json", "--quant-compiler-evidence-out", "/tmp/out.json" }));
    try std.testing.expectError(error.QuantCompilerCheckEvidenceConflictsWithBenchmark, parseArgs(&.{ "--quant-compiler-check-evidence", "/tmp/evidence.json", "--warmup-iters", "5" }));
    try std.testing.expectError(error.QuantCompilerCheckEvidenceConflictsWithBenchmark, parseArgs(&.{ "--quant-compiler-check-evidence", "/tmp/evidence.json", "--gemma4-shapes" }));
    try std.testing.expectError(error.QuantCompilerCheckEvidenceConflictsWithBenchmark, parseArgs(&.{ "--quant-compiler-check-evidence", "/tmp/evidence.json", "--json-out", "/tmp/out.json" }));
    try std.testing.expectError(error.QuantCompilerCheckEvidenceConflictsWithBenchmark, parseArgs(&.{ "--quant-compiler-check-evidence", "/tmp/evidence.json", "--quant-compiler-repeat-runs", "3" }));
    try std.testing.expectError(error.MissingQuantCompilerRepeatRuns, parseArgs(&.{"--quant-compiler-repeat-runs"}));
    try std.testing.expectError(error.InvalidQuantCompilerRepeatRuns, parseArgs(&.{ "--quant-compiler-repeat-runs", "0" }));
    try std.testing.expectError(error.DuplicateQuantCompilerRepeatRuns, parseArgs(&.{ "--quant-compiler-repeat-runs", "2", "--quant-compiler-repeat-runs", "3" }));
    try std.testing.expectError(error.QuantCompilerEvidenceRequiresLazyTarget, parseArgs(&.{ "--quant-compiler-evidence-out", "/tmp/evidence.json" }));
    try std.testing.expectError(error.QuantCompilerEvidenceRequiresManifestRepeatRuns, parseArgs(&.{ "--quant-compiler-generated-ptx", "/tmp/generated.ptx", "--quant-compiler-evidence-out", "/tmp/evidence.json" }));
    try std.testing.expectError(error.QuantCompilerEvidenceRequiresManifestIterations, parseArgs(&.{ "--quant-compiler-generated-ptx", "/tmp/generated.ptx", "--warmup-iters", "1", "--quant-compiler-evidence-out", "/tmp/evidence.json" }));
    try std.testing.expectError(error.QuantCompilerEvidenceRequiresManifestIterations, parseArgs(&.{ "--quant-compiler-generated-ptx", "/tmp/generated.ptx", "--measure-iters", "49", "--quant-compiler-evidence-out", "/tmp/evidence.json" }));

    const repeat_cfg = try parseArgs(&.{ "--quant-compiler-generated-ptx", "/tmp/generated.ptx", "--quant-compiler-repeat-runs", "3", "--quant-compiler-evidence-out", "/tmp/evidence.json" });
    try std.testing.expectEqual(@as(usize, 3), repeat_cfg.quant_compiler_repeat_runs);

    const q4_0_cfg = try parseArgs(&.{ "--quant-compiler-q4-0-mmv-ptx", "/tmp/mmv.ptx", "--quant-compiler-q4-0-mm-ptx", "/tmp/mm.ptx" });
    try std.testing.expectEqualStrings("/tmp/mmv.ptx", q4_0_cfg.quant_compiler_q4_0_mmv_ptx_path.?);
    try std.testing.expectEqualStrings("/tmp/mm.ptx", q4_0_cfg.quant_compiler_q4_0_mm_ptx_path.?);
    try std.testing.expect(!q4_0_cfg.quant_compiler_lazy_target);
    try std.testing.expectError(error.MissingQuantCompilerQ4_0MmvPtxPath, parseArgs(&.{"--quant-compiler-q4-0-mmv-ptx"}));
    try std.testing.expectError(error.MissingQuantCompilerQ4_0MmPtxPath, parseArgs(&.{"--quant-compiler-q4-0-mm-ptx"}));
    try std.testing.expectError(error.DuplicateQuantCompilerQ4_0MmvPtxPath, parseArgs(&.{ "--quant-compiler-q4-0-mmv-ptx", "/tmp/a.ptx", "--quant-compiler-q4-0-mmv-ptx", "/tmp/b.ptx" }));
    try std.testing.expectError(error.DuplicateQuantCompilerQ4_0MmPtxPath, parseArgs(&.{ "--quant-compiler-q4-0-mm-ptx", "/tmp/a.ptx", "--quant-compiler-q4-0-mm-ptx", "/tmp/b.ptx" }));
    try std.testing.expectError(error.QuantCompilerQ4_0ConflictsWithLazyTarget, parseArgs(&.{ "--quant-compiler-generated-ptx", "/tmp/generated.ptx", "--quant-compiler-q4-0-mmv-ptx", "/tmp/mmv.ptx" }));
    try std.testing.expectError(error.QuantCompilerCheckEvidenceConflictsWithBenchmark, parseArgs(&.{ "--quant-compiler-check-evidence", "/tmp/evidence.json", "--quant-compiler-q4-0-mmv-ptx", "/tmp/mmv.ptx" }));
}

test "cuda microbench E2B LM argmax mode defaults and gate are exact" {
    const defaults = try parseArgs(&.{"--q4-0-q8-1-lm-argmax-e2b"});
    try std.testing.expect(defaults.q4_0_q8_1_lm_argmax_e2b);
    try std.testing.expectEqual(q4_0_q8_1_lm_argmax_default_warmups, defaults.warmup_iters);
    try std.testing.expectEqual(q4_0_q8_1_lm_argmax_default_iterations, defaults.measure_iters);
    try std.testing.expectEqual(q4_0_q8_1_lm_argmax_default_repeats, defaults.quant_compiler_repeat_runs);

    const overridden = try parseArgs(&.{ "--q4-0-q8-1-lm-argmax-e2b", "--warmup-iters", "7", "--measure-iters", "11", "--repeat-runs", "3" });
    try std.testing.expectEqual(@as(usize, 7), overridden.warmup_iters);
    try std.testing.expectEqual(@as(usize, 11), overridden.measure_iters);
    try std.testing.expectEqual(@as(usize, 3), overridden.quant_compiler_repeat_runs);
    try std.testing.expectError(error.DuplicateQ4_0Q8_1LmArgmaxE2B, parseArgs(&.{ "--q4-0-q8-1-lm-argmax-e2b", "--q4-0-q8-1-lm-argmax-e2b" }));
    try std.testing.expectError(error.DuplicateQuantCompilerRepeatRuns, parseArgs(&.{ "--q4-0-q8-1-lm-argmax-e2b", "--repeat-runs", "3", "--quant-compiler-repeat-runs", "5" }));
    try std.testing.expectError(error.Q4_0Q8_1LmArgmaxE2BConflictsWithOtherBenchmark, parseArgs(&.{ "--q4-0-q8-1-lm-argmax-e2b", "--gemma4-shapes" }));

    try validateQ4_0Q8_1LmArgmaxBenchmarkGate(false, false);
    try validateQ4_0Q8_1LmArgmaxBenchmarkGate(true, true);
    try std.testing.expectError(error.CudaKernelUnavailable, validateQ4_0Q8_1LmArgmaxBenchmarkGate(true, false));
}

test "cuda microbench q4_0 tc_hmma mode defaults and gate are exact" {
    const defaults = try parseArgs(&.{"--q4-0-tc-hmma-e2b"});
    try std.testing.expect(defaults.q4_0_tc_hmma_e2b);
    try std.testing.expectEqual(q4_0_tc_hmma_default_warmups, defaults.warmup_iters);
    try std.testing.expectEqual(q4_0_tc_hmma_default_iterations, defaults.measure_iters);
    try std.testing.expectEqual(q4_0_tc_hmma_default_repeats, defaults.quant_compiler_repeat_runs);

    const overridden = try parseArgs(&.{ "--q4-0-tc-hmma-e2b", "--warmup-iters", "7", "--measure-iters", "11", "--repeat-runs", "3" });
    try std.testing.expectEqual(@as(usize, 7), overridden.warmup_iters);
    try std.testing.expectEqual(@as(usize, 11), overridden.measure_iters);
    try std.testing.expectEqual(@as(usize, 3), overridden.quant_compiler_repeat_runs);
    try std.testing.expectError(error.DuplicateQ4_0TcHmmaE2B, parseArgs(&.{ "--q4-0-tc-hmma-e2b", "--q4-0-tc-hmma-e2b" }));
    try std.testing.expectError(error.Q4_0TcHmmaE2BConflictsWithOtherBenchmark, parseArgs(&.{ "--q4-0-tc-hmma-e2b", "--gemma4-shapes" }));
    try std.testing.expectError(error.Q4_0TcHmmaE2BConflictsWithOtherBenchmark, parseArgs(&.{ "--q4-0-tc-hmma-e2b", "--q4-0-q8-1-lm-argmax-e2b" }));
    try std.testing.expectError(error.Q4_0TcHmmaE2BConflictsWithOtherBenchmark, parseArgs(&.{ "--q4-0-tc-hmma-e2b", "--activation-multiply-strided-e2b" }));

    const strided = try parseArgs(&.{"--activation-multiply-strided-e2b"});
    try std.testing.expect(strided.activation_multiply_strided_e2b);
    try std.testing.expectEqual(q4_0_tc_hmma_default_warmups, strided.warmup_iters);
    try std.testing.expectEqual(q4_0_tc_hmma_default_iterations, strided.measure_iters);
    try std.testing.expectEqual(q4_0_tc_hmma_default_repeats, strided.quant_compiler_repeat_runs);
    try std.testing.expectError(error.DuplicateActivationMultiplyStridedE2B, parseArgs(&.{ "--activation-multiply-strided-e2b", "--activation-multiply-strided-e2b" }));
    try std.testing.expectError(error.ActivationMultiplyStridedE2BConflictsWithOtherBenchmark, parseArgs(&.{ "--activation-multiply-strided-e2b", "--gemma4-shapes" }));

    try validateQ4_0TcHmmaBenchmarkGate(false, false);
    try validateQ4_0TcHmmaBenchmarkGate(true, true);
    try std.testing.expectError(error.CudaKernelUnavailable, validateQ4_0TcHmmaBenchmarkGate(true, false));
    try validateActivationMultiplyStridedBenchmarkGate(true, true);
    try std.testing.expectError(error.CudaKernelUnavailable, validateActivationMultiplyStridedBenchmarkGate(true, false));
}

test "cuda microbench q4_0 tc_hmma pack separates scales and quants" {
    const allocator = std.testing.allocator;
    const in_dim: usize = 256;
    const out_dim: usize = 2;
    const row_blocks = in_dim / q4_0_values_per_block;
    const block_count = out_dim * row_blocks;
    const raw = try allocator.alloc(u8, block_count * q4_0_block_bytes);
    defer allocator.free(raw);
    for (raw, 0..) |*byte, i| byte.* = @truncate(i * 7 + 3);

    const packed_bytes = try packQ4_0TcHmmaWeights(allocator, raw, in_dim, out_dim);
    defer allocator.free(packed_bytes);
    try std.testing.expectEqual(block_count * q4_0_block_bytes, packed_bytes.len);
    for (0..block_count) |block| {
        try std.testing.expectEqualSlices(u8, raw[block * q4_0_block_bytes ..][0..q4_0_tc_scale_bytes], packed_bytes[block * q4_0_tc_scale_bytes ..][0..q4_0_tc_scale_bytes]);
        try std.testing.expectEqualSlices(u8, raw[block * q4_0_block_bytes + q4_0_tc_scale_bytes ..][0..q4_0_tc_q_bytes], packed_bytes[block_count * q4_0_tc_scale_bytes + block * q4_0_tc_q_bytes ..][0..q4_0_tc_q_bytes]);
    }
    try std.testing.expectError(error.InvalidArgument, packQ4_0TcHmmaWeights(allocator, raw[0 .. raw.len - 1], in_dim, out_dim));
    try std.testing.expectError(error.InvalidArgument, packQ4_0TcHmmaWeights(allocator, raw, in_dim + 1, out_dim));
}

test "cuda microbench Q6_K Q8_1 LM argmax mode defaults and gate are exact" {
    const defaults = try parseArgs(&.{"--q6-k-q8-1-lm-argmax"});
    try std.testing.expect(defaults.q6_k_q8_1_lm_argmax);
    try std.testing.expectEqual(q6_k_q8_1_lm_argmax_default_warmups, defaults.warmup_iters);
    try std.testing.expectEqual(q6_k_q8_1_lm_argmax_default_iterations, defaults.measure_iters);
    try std.testing.expectEqual(q6_k_q8_1_lm_argmax_default_repeats, defaults.quant_compiler_repeat_runs);

    const overridden = try parseArgs(&.{ "--q6-k-q8-1-lm-argmax", "--warmup-iters", "7", "--measure-iters", "11", "--repeat-runs", "3" });
    try std.testing.expectEqual(@as(usize, 7), overridden.warmup_iters);
    try std.testing.expectEqual(@as(usize, 11), overridden.measure_iters);
    try std.testing.expectEqual(@as(usize, 3), overridden.quant_compiler_repeat_runs);
    try std.testing.expectError(error.DuplicateQ6KQ8_1LmArgmax, parseArgs(&.{ "--q6-k-q8-1-lm-argmax", "--q6-k-q8-1-lm-argmax" }));
    try std.testing.expectError(error.DuplicateQuantCompilerRepeatRuns, parseArgs(&.{ "--q6-k-q8-1-lm-argmax", "--repeat-runs", "3", "--quant-compiler-repeat-runs", "5" }));
    try std.testing.expectError(error.Q6KQ8_1LmArgmaxConflictsWithOtherBenchmark, parseArgs(&.{ "--q6-k-q8-1-lm-argmax", "--gemma4-shapes" }));

    for (q6_k_q8_1_lm_argmax_variants) |variant| {
        try validateQ6KQ8_1LmArgmaxVariant(variant);
    }
    try validateQ6KQ8_1LmArgmaxBenchmarkGate(false, false);
    try validateQ6KQ8_1LmArgmaxBenchmarkGate(true, true);
    try std.testing.expectError(error.CudaKernelUnavailable, validateQ6KQ8_1LmArgmaxBenchmarkGate(true, false));
}

test "cuda microbench E2B FFN SM89 mode defaults and gate are exact" {
    const defaults = try parseArgs(&.{ "--q4-0-q8-1-e2b-ffn-sm89", "/tmp/e2b-sm89" });
    try std.testing.expectEqualStrings("/tmp/e2b-sm89", defaults.q4_0_q8_1_e2b_ffn_sm89_dir.?);
    try std.testing.expectEqual(q4_0_q8_1_e2b_ffn_default_warmups, defaults.warmup_iters);
    try std.testing.expectEqual(q4_0_q8_1_e2b_ffn_default_iterations, defaults.measure_iters);
    try std.testing.expectEqual(q4_0_q8_1_e2b_ffn_default_repeats, defaults.quant_compiler_repeat_runs);

    const overridden = try parseArgs(&.{ "--q4-0-q8-1-e2b-ffn-sm89", "/tmp/e2b-sm89", "--warmup-iters", "7", "--measure-iters", "11", "--repeat-runs", "3" });
    try std.testing.expectEqual(@as(usize, 7), overridden.warmup_iters);
    try std.testing.expectEqual(@as(usize, 11), overridden.measure_iters);
    try std.testing.expectEqual(@as(usize, 3), overridden.quant_compiler_repeat_runs);
    try std.testing.expectError(error.MissingQ4_0Q8_1E2BFfnSm89Dir, parseArgs(&.{"--q4-0-q8-1-e2b-ffn-sm89"}));
    try std.testing.expectError(error.DuplicateQ4_0Q8_1E2BFfnSm89Dir, parseArgs(&.{ "--q4-0-q8-1-e2b-ffn-sm89", "/tmp/a", "--q4-0-q8-1-e2b-ffn-sm89", "/tmp/b" }));
    try std.testing.expectError(error.Q4_0Q8_1E2BFfnSm89ConflictsWithOtherBenchmark, parseArgs(&.{ "--q4-0-q8-1-e2b-ffn-sm89", "/tmp/a", "--q4-0-q8-1-lm-argmax-e2b" }));
    try std.testing.expectError(error.Q4_0Q8_1E2BFfnSm89ConflictsWithOtherBenchmark, parseArgs(&.{ "--q4-0-q8-1-e2b-ffn-sm89", "/tmp/a", "--gemma4-shapes" }));

    try validateQ4_0Q8_1E2BFfnSm89Gate(false, 8, 0);
    try validateQ4_0Q8_1E2BFfnSm89Gate(true, 8, 9);
    try std.testing.expectError(error.CudaComputeCapabilityMismatch, validateQ4_0Q8_1E2BFfnSm89Gate(true, 9, 0));
    try std.testing.expectEqual(@as(u32, @backingInt(backend_contracts.DecoderRuntimeActivationKind.gelu_new)), q4_0_q8_1_e2b_ffn_activation);
    try std.testing.expectEqual(@as(usize, 256), q4_0_q8_1_e2b_ffn_production_down_threads);
    try std.testing.expectEqual(@as(usize, 128), q4_0_q8_1_e2b_ffn_ggml_down_threads);

    for (q4_0_q8_1_e2b_ffn_variants) |variant| {
        try validateQ4_0Q8_1E2BFfnVariant(variant);
        try std.testing.expect(!variant.pair.production_enabled);
        try std.testing.expect(!variant.down.production_enabled);
        try std.testing.expect(std.mem.containsAtLeast(u8, variant.pair.kernel_id, 1, "ggml_q8_1"));
        try std.testing.expect(std.mem.containsAtLeast(u8, variant.down.kernel_id, 1, "ggml_q8_1"));
        try std.testing.expectEqual(@as(usize, 1), variant.down.cols_per_block);
    }
    var invalid_variant = q4_0_q8_1_e2b_ffn_variants[0];
    invalid_variant.down.threads = 256;
    try std.testing.expectError(error.InvalidE2BFfnVariant, validateQ4_0Q8_1E2BFfnVariant(invalid_variant));
}

test "cuda microbench timing summary reports median and CV" {
    const summary = try summarizeTimingRuns(&.{ 90, 100, 100, 100, 110 });
    try std.testing.expectEqual(@as(u64, 100), summary.median_ns);
    try std.testing.expectApproxEqAbs(@as(f64, 100.0), summary.mean_ns, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, 6.324555320336759), summary.stddev_ns, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, 6.324555320336759), summary.cv_percent, 0.000001);
    try std.testing.expectError(error.InvalidTimingRuns, summarizeTimingRuns(&.{}));
}

test "cuda microbench exact byte comparison reports first mismatch" {
    try std.testing.expectEqual(@as(?usize, null), try firstByteMismatch(&.{ 1, 2, 3 }, &.{ 1, 2, 3 }));
    try std.testing.expectEqual(@as(?usize, 1), try firstByteMismatch(&.{ 1, 2, 3 }, &.{ 1, 9, 3 }));
    try std.testing.expectError(error.InvalidArgument, firstByteMismatch(&.{1}, &.{ 1, 2 }));
}

test "cuda microbench exact byte comparison preserves f32 bit patterns" {
    const positive_zero = [_]f32{0.0};
    const negative_zero = [_]f32{@bitCast(@as(u32, 0x80000000))};
    try std.testing.expect((try firstByteMismatch(std.mem.asBytes(&positive_zero), std.mem.asBytes(&negative_zero))) != null);
}

test "cuda microbench llama Q8_1 differential ignores only typed raw-sum field" {
    const legacy = @as([q8_1_block_bytes]u8, @splat(0));
    var ggml = legacy;
    ggml[2] = 0x34;
    ggml[3] = 0x12;
    try std.testing.expect((try firstByteMismatch(&legacy, &ggml)) != null);
    try std.testing.expectEqual(@as(?usize, null), try firstQ8PayloadMismatchIgnoringGgmlSum(&legacy, &ggml));
    ggml[4] = 1;
    try std.testing.expectEqual(@as(?usize, 4), try firstQ8PayloadMismatchIgnoringGgmlSum(&legacy, &ggml));

    const values = @as([q8_1_values_per_block]f32, @splat(1.0));
    const sum_half: f16 = 32.0;
    const sum_bits: u16 = @bitCast(sum_half);
    ggml[2] = @truncate(sum_bits);
    ggml[3] = @truncate(sum_bits >> 8);
    try std.testing.expectApproxEqAbs(@as(f32, 0), try maxGgmlQ8_1StoredSumDiff(&ggml, &values), 0.000001);
}

fn printUsage() void {
    print(
        \\usage: antfly inference bench-cuda [--warmup-iters N] [--measure-iters N]
        \\                         [--model <clipclap-model-dir>] [--text <prompt>] [--full-iters N]
        \\                         [--gemma4-shapes] [--json-out PATH]
        \\                         [--q4-0-tc-hmma-e2b]
        \\                         [--activation-multiply-strided-e2b]
        \\                         [--q4-0-q8-1-lm-argmax-e2b] [--repeat-runs N]
        \\                         [--q6-k-q8-1-lm-argmax]
        \\                         [--q4-0-q8-1-e2b-ffn-sm89 CUBIN_DIR]
        \\                         [--quant-compiler-lazy-target --quant-compiler-generated-ptx PATH]
        \\                         [--quant-compiler-q4-0-mmv-ptx PATH] [--quant-compiler-q4-0-mm-ptx PATH]
        \\                         [--quant-compiler-q4-0-pair-ptx PATH]
        \\                         [--quant-compiler-q4-0-pair-q8-ptx PATH] [--quant-compiler-q4-0-down-q8-ptx PATH]
        \\                         [--quant-compiler-evidence-out PATH]
        \\                         [--quant-compiler-check-evidence PATH]
        \\                         [--quant-compiler-require-promotion-ready]
        \\                         [--quant-compiler-repeat-runs N]
        \\
        \\Benchmarks CUDA Q4_K linear kernels on CLIP/CLAP-sized shapes.
        \\With --gemma4-shapes, also benchmarks Gemma4 Q8_0/Q4_K decode-sized matmuls.
        \\With --q4-0-tc-hmma-e2b, compares the W4A16 tensor-core Q4_0 linear
        \\(termite_linear_q4_0_f32_tc_hmma) against the SIMT q4_0 baselines at
        \\Gemma4 E2B prefill shapes (rows 64/512). Defaults: 20 warmups, 200
        \\iterations, 5 repeats.
        \\With --activation-multiply-strided-e2b, sanity-compares the strided
        \\fused gate/up activation multiply against the contiguous kernel.
        \\With --q4-0-q8-1-lm-argmax-e2b, compares complete E2B Q8_1 LM-head
        \\chains at 1536->262144. Defaults: 20 warmups, 200 iterations, 5 repeats.
        \\With --q6-k-q8-1-lm-argmax, compares complete Q6_K x Q8_1 LM-head
        \\chains at 2560->262144 and 3840->262144. Defaults: 20 warmups, 200 iterations, 5 repeats.
        \\With --q4-0-q8-1-e2b-ffn-sm89, loads the repository-generated canonical
        \\SM89 cubin and compares llama.cpp CUDA Q8_1 E2B FFN candidates at inner sizes
        \\6144 and 12288, including a four-way rotating input/weight/output chain.
        \\With --quant-compiler-generated-ptx, compares the dev generated Q4_K rows 2..8 bias_gelu
        \\candidate against the checked-in handwritten CUDA baseline and CPU quant reference.
        \\With --quant-compiler-q4-0-mmv-ptx / --quant-compiler-q4-0-mm-ptx, compares the
        \\generated Q4_0 rows=1 / rows 9..64 no-bias kernels against the handwritten
        \\q4_simt kernels on Gemma4 E2B QAT shapes.
        \\With --quant-compiler-q4-0-pair-ptx, compares the generated Q4_0 rows=1 FFN
        \\gate+up pair kernel against the handwritten pair baseline.
        \\With --quant-compiler-q4-0-pair-q8-ptx / --quant-compiler-q4-0-down-q8-ptx,
        \\compares the generated q8_1/DP4A E4B fused-FFN kernels against the tuned
        \\handwritten baselines.
        \\If --model is provided, also runs full ClipCLAP text embedding through the CUDA backend.
        \\
    , .{});
}

fn validateQ4_0Q8_1LmArgmaxBenchmarkGate(enabled: bool, candidate_available: bool) !void {
    if (enabled and !candidate_available) return error.CudaKernelUnavailable;
}

fn validateQ4_0TcHmmaBenchmarkGate(enabled: bool, candidate_available: bool) !void {
    if (enabled and !candidate_available) return error.CudaKernelUnavailable;
}

fn validateActivationMultiplyStridedBenchmarkGate(enabled: bool, candidate_available: bool) !void {
    if (enabled and !candidate_available) return error.CudaKernelUnavailable;
}

fn validateQ6KQ8_1LmArgmaxBenchmarkGate(enabled: bool, candidate_available: bool) !void {
    if (enabled and !candidate_available) return error.CudaKernelUnavailable;
}

fn validateQ4_0Q8_1E2BFfnSm89Gate(enabled: bool, compute_major: i32, compute_minor: i32) !void {
    if (enabled and (compute_major != 8 or compute_minor != 9)) return error.CudaComputeCapabilityMismatch;
}

fn runKernelBench(allocator: std.mem.Allocator, io: std.Io, cfg: Config) !void {
    var ctx = try cuda_context.CudaContext.initDefault();
    defer ctx.deinit();

    var module = try BenchModule.load(&ctx);
    defer module.unload(&ctx);

    if (cfg.q4_0_q8_1_e2b_ffn_sm89_dir) |cubin_dir| {
        validateQ4_0Q8_1E2BFfnSm89Gate(true, ctx.info.compute_major, ctx.info.compute_minor) catch |err| {
            print(
                "CUDA E2B Q4_0 x Q8_1 FFN SM89 benchmark unavailable: device={s} cc={d}.{d} required=8.9\n",
                .{ ctx.info.nameSlice(), ctx.info.compute_major, ctx.info.compute_minor },
            );
            return err;
        };
        try benchQ4_0Q8_1E2BFfnSm89(allocator, io, &ctx, &module, cfg, cubin_dir);
        return;
    }

    if (cfg.q4_0_tc_hmma_e2b) {
        validateQ4_0TcHmmaBenchmarkGate(true, module.linear_q4_0_f32_tc_hmma != null) catch |err| {
            print(
                "CUDA E2B Q4_0 tc_hmma benchmark unavailable: missing symbol={s} artifact={s}\n",
                .{ q4_0_tc_hmma_candidate_symbol, cuda_artifact.target },
            );
            return err;
        };
        try benchQ4_0TcHmmaE2B(allocator, &ctx, &module, cfg);
        return;
    }

    if (cfg.activation_multiply_strided_e2b) {
        validateActivationMultiplyStridedBenchmarkGate(true, module.activation_multiply_fused_gate_up_f32 != null) catch |err| {
            print(
                "CUDA strided activation-multiply benchmark unavailable: missing symbol={s} artifact={s}\n",
                .{ activation_multiply_strided_symbol, cuda_artifact.target },
            );
            return err;
        };
        try benchActivationMultiplyStridedE2B(allocator, &ctx, &module, cfg);
        return;
    }

    if (cfg.q4_0_q8_1_lm_argmax_e2b) {
        validateQ4_0Q8_1LmArgmaxBenchmarkGate(true, module.linear_q4_0_q8_1_argmax_rows_stage1_tile8_e2b != null) catch |err| {
            print(
                "CUDA E2B Q4_0 x Q8_1 LM argmax benchmark unavailable: missing symbol={s} artifact={s}\n",
                .{ q4_0_q8_1_lm_argmax_candidate_symbol, cuda_artifact.target },
            );
            return err;
        };
        try benchQ4_0Q8_1LmArgmaxE2B(allocator, &ctx, &module, cfg);
        return;
    }

    if (cfg.q6_k_q8_1_lm_argmax) {
        try benchQ6KQ8_1LmArgmax(allocator, &ctx, &module, cfg);
        return;
    }

    if (cfg.quant_compiler_lazy_target) {
        try benchQuantCompilerLazyTarget(allocator, io, &ctx, &module, cfg);
        return;
    }

    if (cfg.quant_compiler_q4_0_mmv_ptx_path != null or cfg.quant_compiler_q4_0_mm_ptx_path != null or cfg.quant_compiler_q4_0_pair_ptx_path != null or cfg.quant_compiler_q4_0_pair_q8_ptx_path != null or cfg.quant_compiler_q4_0_down_q8_ptx_path != null) {
        if (cfg.quant_compiler_q4_0_mmv_ptx_path) |ptx_path| {
            try benchQuantCompilerQ4_0Target(allocator, io, &ctx, &module, cfg, .mmv, ptx_path);
        }
        if (cfg.quant_compiler_q4_0_mm_ptx_path) |ptx_path| {
            try benchQuantCompilerQ4_0Target(allocator, io, &ctx, &module, cfg, .mm, ptx_path);
        }
        if (cfg.quant_compiler_q4_0_pair_ptx_path) |ptx_path| {
            try benchQuantCompilerQ4_0PairTarget(allocator, io, &ctx, &module, cfg, ptx_path);
        }
        if (cfg.quant_compiler_q4_0_pair_q8_ptx_path) |ptx_path| {
            try benchQuantCompilerQ4_0PairQ8Target(allocator, io, &ctx, &module, cfg, ptx_path);
        }
        if (cfg.quant_compiler_q4_0_down_q8_ptx_path) |ptx_path| {
            try benchQuantCompilerQ4_0DownQ8Target(allocator, io, &ctx, &module, cfg, ptx_path);
        }
        return;
    }

    print("CUDA Q4_K microbench: device={s} cc={d}.{d} warmup={d} measure={d}\n", .{
        ctx.info.nameSlice(),
        ctx.info.compute_major,
        ctx.info.compute_minor,
        cfg.warmup_iters,
        cfg.measure_iters,
    });
    print("{s:<24} {s:>8} {s:>8} {s:>8} {s:>14} {s:>14} {s:>14} {s:>14} {s:>14} {s:>12}\n", .{
        "shape",
        "rows",
        "in",
        "out",
        "scalar ns",
        "tiled ns",
        "scalar+b ns",
        "tiled+b ns",
        "qgelu ns",
        "checksum",
    });
    print("{s:<24} {s:>8} {s:>8} {s:>8} {s:>14} {s:>14} {s:>14} {s:>14} {s:>14} {s:>12}\n", .{
        "-----",
        "----",
        "--",
        "---",
        "------",
        "------",
        "-----------",
        "---------",
        "--------",
        "--------",
    });

    for (shapes) |shape| {
        try benchShape(allocator, &ctx, &module, cfg, shape);
    }

    print("\n{s:<24} {s:>8} {s:>8} {s:>8} {s:>14} {s:>14} {s:>12}\n", .{
        "triple shape",
        "rows",
        "in",
        "out",
        "scalar ns",
        "tiled ns",
        "checksum",
    });
    print("{s:<24} {s:>8} {s:>8} {s:>8} {s:>14} {s:>14} {s:>12}\n", .{
        "------------",
        "----",
        "--",
        "---",
        "---------",
        "--------",
        "--------",
    });
    try benchTripleShape(allocator, &ctx, &module, cfg, .{ .label = "CLIP text QKV", .rows = 77, .in_dim = 768, .out_dim = 768 });
    try benchTripleShape(allocator, &ctx, &module, cfg, .{ .label = "CLIP vision QKV", .rows = 257, .in_dim = 768, .out_dim = 768 });

    if (cfg.gemma4_shapes) {
        try runGemma4KernelBench(allocator, io, &ctx, &module, cfg);
    }
}

// Host-side mirror of the q4_0_hmma packed layout consumed by
// termite_q4_0_tc_value_at: scales region (block_count * 2 bytes of f16 LE
// scales) followed by the quants region (block_count * 16 raw GGUF nibble
// bytes), both indexed by block_index = col * row_blocks + block.
fn packQ4_0TcHmmaWeights(allocator: std.mem.Allocator, raw: []const u8, in_dim: usize, out_dim: usize) ![]u8 {
    if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidArgument;
    const row_blocks = in_dim / q4_0_values_per_block;
    const block_count = try std.math.mul(usize, out_dim, row_blocks);
    if (raw.len != try std.math.mul(usize, block_count, q4_0_block_bytes)) return error.InvalidArgument;

    const scales_bytes = try std.math.mul(usize, block_count, q4_0_tc_scale_bytes);
    const q_bytes = try std.math.mul(usize, block_count, q4_0_tc_q_bytes);
    const out = try allocator.alloc(u8, scales_bytes + q_bytes);
    errdefer allocator.free(out);

    for (0..block_count) |block| {
        const src = raw[block * q4_0_block_bytes ..][0..q4_0_block_bytes];
        @memcpy(out[block * q4_0_tc_scale_bytes ..][0..q4_0_tc_scale_bytes], src[0..2]);
        @memcpy(out[scales_bytes + block * q4_0_tc_q_bytes ..][0..q4_0_tc_q_bytes], src[2..18]);
    }
    return out;
}

fn benchQ4_0TcHmmaE2B(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
) !void {
    print("CUDA E2B Q4_0 tc_hmma microbench: device={s} cc={d}.{d} warmup={d} measure={d} repeats={d}\n", .{
        ctx.info.nameSlice(),
        ctx.info.compute_major,
        ctx.info.compute_minor,
        cfg.warmup_iters,
        cfg.measure_iters,
        cfg.quant_compiler_repeat_runs,
    });
    print("candidate kernel={s} baselines=termite_linear_q4_0_f32,{s} tolerance_f32={d:.6} tolerance_tc={d:.6}\n", .{
        q4_0_tc_hmma_candidate_symbol,
        q4_0_tc_hmma_generated_mm_symbol,
        q4_0_tc_hmma_f32_tolerance_abs,
        q4_0_tc_hmma_candidate_tolerance_abs,
    });
    if (module.generated_q4_0_mm_f32 == null) {
        print("baseline kernel={s} missing from artifact; skipping generated-mm comparison\n", .{q4_0_tc_hmma_generated_mm_symbol});
    }
    if (module.linear_q4_0_f32_tc_hmma_bf16 == null) {
        print("bf16 candidate kernel={s} missing from artifact; reporting tc_hmma_bf16_ns=0 (regenerate CUDA artifacts to include it)\n", .{q4_0_tc_hmma_bf16_candidate_symbol});
    } else {
        print("bf16 candidate kernel={s} tolerance_bf16={d:.6}\n", .{ q4_0_tc_hmma_bf16_candidate_symbol, q4_0_tc_hmma_bf16_tolerance_abs });
    }
    for (q4_0_tc_hmma_e2b_row_counts) |rows| {
        for (q4_0_tc_hmma_e2b_dims) |dims| {
            try benchQ4_0TcHmmaShape(allocator, ctx, module, cfg, .{
                .label = dims.label,
                .rows = rows,
                .in_dim = dims.in_dim,
                .out_dim = dims.out_dim,
            });
        }
    }
}

fn benchQ4_0TcHmmaShape(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    shape: Shape,
) !void {
    // The engine only packs tensor-core weights when in_dim % 256 == 0.
    if (shape.in_dim == 0 or shape.in_dim % q4_k_values_per_block != 0) return error.InvalidArgument;

    const input_count = try std.math.mul(usize, shape.rows, shape.in_dim);
    const output_count = try std.math.mul(usize, shape.rows, shape.out_dim);
    const row_blocks = shape.in_dim / q4_0_values_per_block;
    const weight_bytes = try std.math.mul(usize, try std.math.mul(usize, shape.out_dim, row_blocks), q4_0_block_bytes);

    const input_host = try allocator.alloc(f32, input_count);
    defer allocator.free(input_host);
    const weight_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_host);
    fillInput(input_host);
    fillQ4_0Weights(weight_host);

    const packed_host = try packQ4_0TcHmmaWeights(allocator, weight_host, shape.in_dim, shape.out_dim);
    defer allocator.free(packed_host);

    const has_cpu_reference = shape.out_dim <= quant_compiler_q4_0_max_reference_out_dim;
    const reference_host = try allocator.alloc(f32, output_count);
    defer allocator.free(reference_host);
    if (has_cpu_reference) {
        try quant_kernel_compiler.referenceMatmulNoBias(
            allocator,
            .q4_0,
            weight_host,
            input_host,
            shape.rows,
            shape.in_dim,
            shape.out_dim,
            reference_host,
        );
    }

    var input = try cuda_buffer.DeviceBuffer.alloc(ctx, input_count * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_host.len);
    defer weight.free(ctx);
    var weight_packed = try cuda_buffer.DeviceBuffer.alloc(ctx, packed_host.len);
    defer weight_packed.free(ctx);
    var output = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
    defer output.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(input_host));
    try weight.copyFromHost(ctx, weight_host);
    try weight_packed.copyFromHost(ctx, packed_host);
    try ctx.synchronize();

    const simt_ns = try repeatCudaStep(allocator, ctx, cfg, launchQ4_0Baseline, .{ module.linear_q4_0_f32, ctx, QuantCompilerQ4_0Kind.mm, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });
    const simt_host = try allocator.alloc(f32, output_count);
    defer allocator.free(simt_host);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(simt_host));
    try ctx.synchronize();
    const simt_cpu_max_abs_diff = if (has_cpu_reference) try maxAbsDiffFinite(simt_host, reference_host) else 0.0;
    if (has_cpu_reference and simt_cpu_max_abs_diff > q4_0_tc_hmma_f32_tolerance_abs) return error.HandwrittenBaselineMismatch;

    var generated_mm_ns: u64 = 0;
    // antfly_q4_0_mm_f32_v1 serves only the rows_9_64 bucket; invoking it at
    // larger row counts leaves the poisoned output uncovered (NaN) and is not a
    // meaningful comparison. Skip it (generated_mm_ns=0 = not compared).
    if (module.generated_q4_0_mm_f32 != null and shape.rows >= 9 and shape.rows <= 64) {
        try poisonDeviceOutput(allocator, ctx, output, output_count);
        generated_mm_ns = try repeatCudaStep(allocator, ctx, cfg, launchGeneratedQ4_0MmBaseline, .{ module.generated_q4_0_mm_f32, ctx, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });
        const generated_host = try allocator.alloc(f32, output_count);
        defer allocator.free(generated_host);
        try output.copyToHost(ctx, std.mem.sliceAsBytes(generated_host));
        try ctx.synchronize();
        const generated_simt_max_abs_diff = try maxAbsDiffFinite(generated_host, simt_host);
        if (generated_simt_max_abs_diff > q4_0_tc_hmma_f32_tolerance_abs) return error.HandwrittenBaselineMismatch;
    }

    // Poison the shared output buffer so a candidate that silently
    // early-returns cannot inherit the baseline's results and pass.
    try poisonDeviceOutput(allocator, ctx, output, output_count);
    const candidate_ns = try repeatCudaStep(allocator, ctx, cfg, launchQ4_0TcHmma, .{ module.linear_q4_0_f32_tc_hmma, ctx, output, input, weight_packed, shape.rows, shape.in_dim, shape.out_dim });
    const candidate_host = try allocator.alloc(f32, output_count);
    defer allocator.free(candidate_host);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(candidate_host));
    try ctx.synchronize();

    const candidate_simt_max_abs_diff = try maxAbsDiffFinite(candidate_host, simt_host);
    if (candidate_simt_max_abs_diff > q4_0_tc_hmma_candidate_tolerance_abs) return error.GeneratedCandidateMismatch;
    const candidate_cpu_max_abs_diff = if (has_cpu_reference) try maxAbsDiffFinite(candidate_host, reference_host) else 0.0;
    if (has_cpu_reference and candidate_cpu_max_abs_diff > q4_0_tc_hmma_candidate_tolerance_abs) return error.GeneratedCandidateMismatch;

    // BF16 tensor-core mirror. Identical q4_0_hmma packed weights, launch
    // geometry, and host launcher as the f16 candidate; only the WMMA element
    // type differs, so launchQ4_0TcHmma is reused verbatim. Optional in the
    // artifact (present once the bf16 entry points are compiled in); when
    // absent we report tc_hmma_bf16_ns=0 and skip the comparison.
    var candidate_bf16_ns: u64 = 0;
    var candidate_bf16_simt_max_abs_diff: f32 = 0.0;
    var candidate_bf16_cpu_max_abs_diff: f32 = 0.0;
    if (module.linear_q4_0_f32_tc_hmma_bf16 != null) {
        try poisonDeviceOutput(allocator, ctx, output, output_count);
        candidate_bf16_ns = try repeatCudaStep(allocator, ctx, cfg, launchQ4_0TcHmma, .{ module.linear_q4_0_f32_tc_hmma_bf16, ctx, output, input, weight_packed, shape.rows, shape.in_dim, shape.out_dim });
        const candidate_bf16_host = try allocator.alloc(f32, output_count);
        defer allocator.free(candidate_bf16_host);
        try output.copyToHost(ctx, std.mem.sliceAsBytes(candidate_bf16_host));
        try ctx.synchronize();
        candidate_bf16_simt_max_abs_diff = try maxAbsDiffFinite(candidate_bf16_host, simt_host);
        if (candidate_bf16_simt_max_abs_diff > q4_0_tc_hmma_bf16_tolerance_abs) return error.GeneratedCandidateMismatch;
        candidate_bf16_cpu_max_abs_diff = if (has_cpu_reference) try maxAbsDiffFinite(candidate_bf16_host, reference_host) else 0.0;
        if (has_cpu_reference and candidate_bf16_cpu_max_abs_diff > q4_0_tc_hmma_bf16_tolerance_abs) return error.GeneratedCandidateMismatch;
    }

    print("shape={s} rows={d} in={d} out={d} simt_ns={d} generated_mm_ns={d} tc_hmma_ns={d} tc_hmma_bf16_ns={d} speedup_vs_simt={d:.6} speedup_vs_generated_mm={d:.6} speedup_bf16_vs_simt={d:.6} speedup_bf16_vs_f16={d:.6} cpu_reference={} candidate_simt_max_abs_diff={d:.6} candidate_cpu_max_abs_diff={d:.6} bf16_candidate_simt_max_abs_diff={d:.6} bf16_candidate_cpu_max_abs_diff={d:.6}\n", .{
        shape.label,
        shape.rows,
        shape.in_dim,
        shape.out_dim,
        simt_ns,
        generated_mm_ns,
        candidate_ns,
        candidate_bf16_ns,
        speedup(simt_ns, candidate_ns),
        speedup(generated_mm_ns, candidate_ns),
        speedup(simt_ns, candidate_bf16_ns),
        speedup(candidate_ns, candidate_bf16_ns),
        has_cpu_reference,
        candidate_simt_max_abs_diff,
        candidate_cpu_max_abs_diff,
        candidate_bf16_simt_max_abs_diff,
        candidate_bf16_cpu_max_abs_diff,
    });
}

fn launchQ4_0TcHmma(
    function: cuda_driver.CUfunction,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight_packed: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    if (in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
    try checkF32Bytes(output, try checkedMul(rows, out_dim));
    try checkF32Bytes(input, try checkedMul(rows, in_dim));
    const row_blocks = in_dim / q4_0_values_per_block;
    try checkRawBytes(weight_packed, try checkedMul(try checkedMul(out_dim, row_blocks), q4_0_block_bytes));
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight_packed.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch2d(
        ctx,
        function,
        (out_dim + q4_0_tc_hmma_cols_per_tile - 1) / q4_0_tc_hmma_cols_per_tile,
        (rows + q4_0_tc_hmma_rows_per_tile - 1) / q4_0_tc_hmma_rows_per_tile,
        q4_0_tc_hmma_threads,
        &params,
    );
}

fn launchGeneratedQ4_0MmBaseline(
    function: cuda_driver.CUfunction,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4_0Buffers(output, input, weight, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&dst_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch2d(ctx, function, (out_dim + 3) / 4, (rows + 7) / 8, 256, &params);
}

fn benchActivationMultiplyStridedE2B(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
) !void {
    print("CUDA strided activation-multiply microbench: device={s} warmup={d} measure={d} repeats={d} activation={d}\n", .{
        ctx.info.nameSlice(),
        cfg.warmup_iters,
        cfg.measure_iters,
        cfg.quant_compiler_repeat_runs,
        activation_multiply_strided_activation,
    });
    print("candidate kernel={s} baseline kernel=termite_activation_multiply_f32\n", .{activation_multiply_strided_symbol});
    for (activation_multiply_strided_row_counts) |rows| {
        for (activation_multiply_strided_f_dims) |f| {
            try benchActivationMultiplyStridedShape(allocator, ctx, module, cfg, rows, f);
        }
    }
}

fn benchActivationMultiplyStridedShape(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    rows: usize,
    f: usize,
) !void {
    const count = try std.math.mul(usize, rows, f);
    const fused_count = try std.math.mul(usize, count, 2);

    const fused_host = try allocator.alloc(f32, fused_count);
    defer allocator.free(fused_host);
    fillInput(fused_host);
    const gate_host = try allocator.alloc(f32, count);
    defer allocator.free(gate_host);
    const up_host = try allocator.alloc(f32, count);
    defer allocator.free(up_host);
    for (0..rows) |row| {
        const fused_row = fused_host[row * 2 * f ..][0 .. 2 * f];
        @memcpy(gate_host[row * f ..][0..f], fused_row[0..f]);
        @memcpy(up_host[row * f ..][0..f], fused_row[f .. 2 * f]);
    }

    var fused = try cuda_buffer.DeviceBuffer.alloc(ctx, fused_count * @sizeOf(f32));
    defer fused.free(ctx);
    var gate = try cuda_buffer.DeviceBuffer.alloc(ctx, count * @sizeOf(f32));
    defer gate.free(ctx);
    var up = try cuda_buffer.DeviceBuffer.alloc(ctx, count * @sizeOf(f32));
    defer up.free(ctx);
    var contiguous_output = try cuda_buffer.DeviceBuffer.alloc(ctx, count * @sizeOf(f32));
    defer contiguous_output.free(ctx);
    var strided_output = try cuda_buffer.DeviceBuffer.alloc(ctx, count * @sizeOf(f32));
    defer strided_output.free(ctx);

    try fused.copyFromHost(ctx, std.mem.sliceAsBytes(fused_host));
    try gate.copyFromHost(ctx, std.mem.sliceAsBytes(gate_host));
    try up.copyFromHost(ctx, std.mem.sliceAsBytes(up_host));
    try poisonDeviceOutput(allocator, ctx, contiguous_output, count);
    try poisonDeviceOutput(allocator, ctx, strided_output, count);
    try ctx.synchronize();

    const contiguous_ns = try repeatCudaStep(allocator, ctx, cfg, launchActivationMultiplyContiguous, .{ module.activation_multiply_f32, ctx, contiguous_output, gate, up, count, activation_multiply_strided_activation });
    const strided_ns = try repeatCudaStep(allocator, ctx, cfg, launchActivationMultiplyFusedGateUp, .{ module.activation_multiply_fused_gate_up_f32, ctx, strided_output, fused, rows, f, activation_multiply_strided_activation });

    const contiguous_host = try allocator.alloc(f32, count);
    defer allocator.free(contiguous_host);
    const strided_host = try allocator.alloc(f32, count);
    defer allocator.free(strided_host);
    try contiguous_output.copyToHost(ctx, std.mem.sliceAsBytes(contiguous_host));
    try strided_output.copyToHost(ctx, std.mem.sliceAsBytes(strided_host));
    try ctx.synchronize();

    // Both kernels evaluate act(gate) * up with identical per-element
    // arithmetic, so the outputs must match exactly.
    const max_abs_diff = try maxAbsDiffFinite(strided_host, contiguous_host);
    if (max_abs_diff != 0.0) return error.StridedActivationMultiplyMismatch;

    print("shape=gate/up rows={d} f={d} contiguous_ns={d} strided_ns={d} speedup={d:.6} max_abs_diff={d:.6}\n", .{
        rows,
        f,
        contiguous_ns,
        strided_ns,
        speedup(contiguous_ns, strided_ns),
        max_abs_diff,
    });
}

fn launchActivationMultiplyContiguous(
    function: cuda_driver.CUfunction,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    gate: cuda_buffer.DeviceBuffer,
    up: cuda_buffer.DeviceBuffer,
    count: usize,
    activation: u32,
) cuda_driver.Error!void {
    try checkF32Bytes(output, count);
    try checkF32Bytes(gate, count);
    try checkF32Bytes(up, count);
    var dst_ptr = output.ptr;
    var gate_ptr = gate.ptr;
    var up_ptr = up.ptr;
    var count_u32 = try toU32(count);
    var activation_u32 = activation;
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&gate_ptr),
        @ptrCast(&up_ptr),
        @ptrCast(&count_u32),
        @ptrCast(&activation_u32),
    };
    try launch1d(ctx, function, count, &params);
}

fn launchActivationMultiplyFusedGateUp(
    function: cuda_driver.CUfunction,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    fused: cuda_buffer.DeviceBuffer,
    rows: usize,
    f: usize,
    activation: u32,
) cuda_driver.Error!void {
    const count = try checkedMul(rows, f);
    try checkF32Bytes(output, count);
    try checkF32Bytes(fused, try checkedMul(count, 2));
    var dst_ptr = output.ptr;
    var fused_ptr = fused.ptr;
    var rows_u32 = try toU32(rows);
    var f_u32 = try toU32(f);
    var activation_u32 = activation;
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&fused_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&f_u32),
        @ptrCast(&activation_u32),
    };
    try launch1d(ctx, function, count, &params);
}

fn benchQ4_0Q8_1LmArgmaxE2B(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
) !void {
    const rows = q4_0_q8_1_lm_argmax_e2b_rows;
    const in_dim = q4_0_q8_1_lm_argmax_e2b_in_dim;
    const out_dim = q4_0_q8_1_lm_argmax_e2b_out_dim;
    const row_blocks = in_dim / q4_0_values_per_block;
    const q8_bytes = try checkedMul(try checkedMul(rows, row_blocks), q8_1_block_bytes);
    const weight_bytes = try checkedMul(try checkedMul(out_dim, row_blocks), q4_0_block_bytes);
    const partial_count = try checkedMul(rows, (out_dim + q4_0_q8_1_lm_argmax_tile8 - 1) / q4_0_q8_1_lm_argmax_tile8);
    const candidate_function = module.linear_q4_0_q8_1_argmax_rows_stage1_tile8_e2b orelse return error.CudaKernelUnavailable;

    print(
        "CUDA E2B Q4_0 x Q8_1 LM argmax: device={s} rows={d} in={d} out={d} warmup={d} measure={d} repeats={d}\n",
        .{ ctx.info.nameSlice(), rows, in_dim, out_dim, cfg.warmup_iters, cfg.measure_iters, cfg.quant_compiler_repeat_runs },
    );

    const input_host = try allocator.alloc(f32, rows * in_dim);
    defer allocator.free(input_host);
    const weight_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_host);
    fillInput(input_host);
    fillQ4_0LmArgmaxWeights(weight_host);

    var input = try cuda_buffer.DeviceBuffer.alloc(ctx, input_host.len * @sizeOf(f32));
    defer input.free(ctx);
    var input_q8_1 = try cuda_buffer.DeviceBuffer.alloc(ctx, q8_bytes);
    defer input_q8_1.free(ctx);
    var weight = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_bytes);
    defer weight.free(ctx);
    var logits = try cuda_buffer.DeviceBuffer.alloc(ctx, out_dim * @sizeOf(f32));
    defer logits.free(ctx);
    var baseline_token = try cuda_buffer.DeviceBuffer.alloc(ctx, @sizeOf(u32));
    defer baseline_token.free(ctx);
    var candidate_token = try cuda_buffer.DeviceBuffer.alloc(ctx, @sizeOf(u32));
    defer candidate_token.free(ctx);
    var partial_values = try cuda_buffer.DeviceBuffer.alloc(ctx, partial_count * @sizeOf(f32));
    defer partial_values.free(ctx);
    var partial_indices = try cuda_buffer.DeviceBuffer.alloc(ctx, partial_count * @sizeOf(u32));
    defer partial_indices.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(input_host));
    try weight.copyFromHost(ctx, weight_host);
    try ctx.synchronize();

    try launchQ4_0Q8_1LmArgmaxBaseline(module, ctx, baseline_token, logits, input_q8_1, input, weight, rows, in_dim, out_dim);
    try launchQ4_0Q8_1LmArgmaxCandidate(candidate_function, module, ctx, candidate_token, partial_values, partial_indices, input_q8_1, input, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    var baseline_token_host: u32 = undefined;
    var candidate_token_host: u32 = undefined;
    try baseline_token.copyToHost(ctx, std.mem.asBytes(&baseline_token_host));
    try candidate_token.copyToHost(ctx, std.mem.asBytes(&candidate_token_host));
    try ctx.synchronize();
    if (baseline_token_host != candidate_token_host) {
        print("CUDA E2B Q4_0 x Q8_1 LM argmax token mismatch: baseline={d} candidate={d}\n", .{ baseline_token_host, candidate_token_host });
        return error.CudaSmokeMismatch;
    }

    var baseline_runs: [31]u64 = undefined;
    var candidate_runs: [31]u64 = undefined;
    const repeat_runs = cfg.quant_compiler_repeat_runs;
    for (0..repeat_runs) |run| {
        if (run % 2 == 0) {
            baseline_runs[run] = try timeCudaStep(ctx, cfg, launchQ4_0Q8_1LmArgmaxBaseline, .{ module, ctx, baseline_token, logits, input_q8_1, input, weight, rows, in_dim, out_dim });
            candidate_runs[run] = try timeCudaStep(ctx, cfg, launchQ4_0Q8_1LmArgmaxCandidate, .{ candidate_function, module, ctx, candidate_token, partial_values, partial_indices, input_q8_1, input, weight, rows, in_dim, out_dim });
        } else {
            candidate_runs[run] = try timeCudaStep(ctx, cfg, launchQ4_0Q8_1LmArgmaxCandidate, .{ candidate_function, module, ctx, candidate_token, partial_values, partial_indices, input_q8_1, input, weight, rows, in_dim, out_dim });
            baseline_runs[run] = try timeCudaStep(ctx, cfg, launchQ4_0Q8_1LmArgmaxBaseline, .{ module, ctx, baseline_token, logits, input_q8_1, input, weight, rows, in_dim, out_dim });
        }
    }
    const baseline_summary = try summarizeTimingRuns(baseline_runs[0..repeat_runs]);
    const candidate_summary = try summarizeTimingRuns(candidate_runs[0..repeat_runs]);
    print("CUDA E2B Q4_0 x Q8_1 LM argmax runs: baseline_ns={any} candidate_ns={any}\n", .{ baseline_runs[0..repeat_runs], candidate_runs[0..repeat_runs] });
    print(
        "CUDA E2B Q4_0 x Q8_1 LM argmax summary: token={d} exact=true baseline_median_ns={d} baseline_mean_ns={d:.3} baseline_stddev_ns={d:.3} baseline_cv_pct={d:.4} candidate_median_ns={d} candidate_mean_ns={d:.3} candidate_stddev_ns={d:.3} candidate_cv_pct={d:.4} speedup={d:.6}\n",
        .{
            baseline_token_host,
            baseline_summary.median_ns,
            baseline_summary.mean_ns,
            baseline_summary.stddev_ns,
            baseline_summary.cv_percent,
            candidate_summary.median_ns,
            candidate_summary.mean_ns,
            candidate_summary.stddev_ns,
            candidate_summary.cv_percent,
            speedup(baseline_summary.median_ns, candidate_summary.median_ns),
        },
    );
}

fn q6KQ8_1LmArgmaxCandidateFunction(module: *const BenchModule, variant: Q6KQ8_1LmArgmaxVariant) cuda_driver.CUfunction {
    return switch (variant.in_dim) {
        2560 => module.linear_q6_k_q8_1_argmax_generated_k2560,
        3840 => module.linear_q6_k_q8_1_argmax_generated_k3840,
        else => null,
    };
}

fn q6KQ8_1LmArgmaxBaselineFunction(module: *const BenchModule, variant: Q6KQ8_1LmArgmaxVariant) cuda_driver.CUfunction {
    return switch (variant.baseline) {
        .e4b => module.linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b,
        .generic => module.linear_q6_k_q8_1_argmax_rows_stage1_tile8,
    };
}

fn q6KQ8_1LmArgmaxBaselineName(variant: Q6KQ8_1LmArgmaxVariant) []const u8 {
    return switch (variant.baseline) {
        .e4b => "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b",
        .generic => "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8",
    };
}

fn q6KQ8_1LmArgmaxBaselineThreads(variant: Q6KQ8_1LmArgmaxVariant) usize {
    return switch (variant.baseline) {
        .e4b => 160,
        .generic => 256,
    };
}

fn benchQ6KQ8_1LmArgmax(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
) !void {
    for (q6_k_q8_1_lm_argmax_variants) |variant| {
        try validateQ6KQ8_1LmArgmaxVariant(variant);
        validateQ6KQ8_1LmArgmaxBenchmarkGate(true, q6KQ8_1LmArgmaxCandidateFunction(module, variant) != null) catch |err| {
            print(
                "CUDA Q6_K x Q8_1 LM argmax benchmark unavailable: missing candidate symbol={s} in={d} artifact={s}\n",
                .{ variant.candidate_symbol, variant.in_dim, cuda_artifact.target },
            );
            return err;
        };
        if (q6KQ8_1LmArgmaxBaselineFunction(module, variant) == null) {
            print(
                "CUDA Q6_K x Q8_1 LM argmax benchmark unavailable: missing baseline stage1={s} in={d} artifact={s}\n",
                .{ q6KQ8_1LmArgmaxBaselineName(variant), variant.in_dim, cuda_artifact.target },
            );
            return error.CudaKernelUnavailable;
        }
    }

    print(
        "CUDA Q6_K x Q8_1 LM argmax: device={s} rows={d} out={d} warmup={d} measure={d} repeats={d}\n",
        .{ ctx.info.nameSlice(), q6_k_q8_1_lm_argmax_rows, q6_k_q8_1_lm_argmax_out_dim, cfg.warmup_iters, cfg.measure_iters, cfg.quant_compiler_repeat_runs },
    );
    for (q6_k_q8_1_lm_argmax_variants) |variant| {
        const baseline_function = q6KQ8_1LmArgmaxBaselineFunction(module, variant) orelse return error.CudaKernelUnavailable;
        const candidate_function = q6KQ8_1LmArgmaxCandidateFunction(module, variant) orelse return error.CudaKernelUnavailable;
        try benchQ6KQ8_1LmArgmaxVariant(allocator, ctx, module, cfg, variant, baseline_function, candidate_function);
    }
}

fn benchQ6KQ8_1LmArgmaxVariant(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    variant: Q6KQ8_1LmArgmaxVariant,
    baseline_function: cuda_driver.CUfunction,
    candidate_function: cuda_driver.CUfunction,
) !void {
    const rows = q6_k_q8_1_lm_argmax_rows;
    const in_dim = variant.in_dim;
    const out_dim = q6_k_q8_1_lm_argmax_out_dim;
    const row_blocks = in_dim / q6_k_values_per_block;
    const q8_bytes = try checkedMul(try checkedMul(rows, in_dim / q8_1_values_per_block), q8_1_block_bytes);
    const weight_bytes = try checkedMul(try checkedMul(out_dim, row_blocks), q6_k_block_bytes);
    const partial_count = try checkedMul(rows, (out_dim + q6_k_q8_1_lm_argmax_tile8 - 1) / q6_k_q8_1_lm_argmax_tile8);

    const input_host = try allocator.alloc(f32, rows * in_dim);
    defer allocator.free(input_host);
    const weight_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_host);
    fillInput(input_host);
    fillQ6_KLmArgmaxWeights(weight_host);

    var input = try cuda_buffer.DeviceBuffer.alloc(ctx, input_host.len * @sizeOf(f32));
    defer input.free(ctx);
    var input_q8_1 = try cuda_buffer.DeviceBuffer.alloc(ctx, q8_bytes);
    defer input_q8_1.free(ctx);
    var weight = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_bytes);
    defer weight.free(ctx);
    var baseline_token = try cuda_buffer.DeviceBuffer.alloc(ctx, @sizeOf(u32));
    defer baseline_token.free(ctx);
    var candidate_token = try cuda_buffer.DeviceBuffer.alloc(ctx, @sizeOf(u32));
    defer candidate_token.free(ctx);
    var baseline_partial_values = try cuda_buffer.DeviceBuffer.alloc(ctx, partial_count * @sizeOf(f32));
    defer baseline_partial_values.free(ctx);
    var baseline_partial_indices = try cuda_buffer.DeviceBuffer.alloc(ctx, partial_count * @sizeOf(u32));
    defer baseline_partial_indices.free(ctx);
    var candidate_partial_values = try cuda_buffer.DeviceBuffer.alloc(ctx, partial_count * @sizeOf(f32));
    defer candidate_partial_values.free(ctx);
    var candidate_partial_indices = try cuda_buffer.DeviceBuffer.alloc(ctx, partial_count * @sizeOf(u32));
    defer candidate_partial_indices.free(ctx);
    const baseline_partial_values_host = try allocator.alloc(f32, partial_count);
    defer allocator.free(baseline_partial_values_host);
    const candidate_partial_values_host = try allocator.alloc(f32, partial_count);
    defer allocator.free(candidate_partial_values_host);
    const baseline_partial_indices_host = try allocator.alloc(u32, partial_count);
    defer allocator.free(baseline_partial_indices_host);
    const candidate_partial_indices_host = try allocator.alloc(u32, partial_count);
    defer allocator.free(candidate_partial_indices_host);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(input_host));
    try weight.copyFromHost(ctx, weight_host);
    try ctx.synchronize();

    try launchQ6KQ8_1LmArgmaxBaseline(baseline_function, module, ctx, baseline_token, baseline_partial_values, baseline_partial_indices, input_q8_1, input, weight, rows, in_dim, out_dim, q6KQ8_1LmArgmaxBaselineThreads(variant));
    try launchQ6KQ8_1LmArgmaxCandidate(candidate_function, module, ctx, candidate_token, candidate_partial_values, candidate_partial_indices, input_q8_1, input, weight, rows, in_dim, out_dim, variant.candidate_threads);
    try ctx.synchronize();
    var baseline_token_host: u32 = undefined;
    var candidate_token_host: u32 = undefined;
    try baseline_token.copyToHost(ctx, std.mem.asBytes(&baseline_token_host));
    try candidate_token.copyToHost(ctx, std.mem.asBytes(&candidate_token_host));
    // The reducer only reads these buffers, so their contents are the stage-one outputs.
    try baseline_partial_values.copyToHost(ctx, std.mem.sliceAsBytes(baseline_partial_values_host));
    try candidate_partial_values.copyToHost(ctx, std.mem.sliceAsBytes(candidate_partial_values_host));
    try baseline_partial_indices.copyToHost(ctx, std.mem.sliceAsBytes(baseline_partial_indices_host));
    try candidate_partial_indices.copyToHost(ctx, std.mem.sliceAsBytes(candidate_partial_indices_host));
    try ctx.synchronize();
    if (try firstByteMismatch(std.mem.sliceAsBytes(baseline_partial_indices_host), std.mem.sliceAsBytes(candidate_partial_indices_host))) |byte_index| {
        const partial_index = byte_index / @sizeOf(u32);
        print(
            "CUDA Q6_K x Q8_1 LM argmax stage1 index mismatch: in={d} partial={d} baseline={d} candidate={d}\n",
            .{ in_dim, partial_index, baseline_partial_indices_host[partial_index], candidate_partial_indices_host[partial_index] },
        );
        return error.CudaSmokeMismatch;
    }
    if (try firstByteMismatch(std.mem.sliceAsBytes(baseline_partial_values_host), std.mem.sliceAsBytes(candidate_partial_values_host))) |byte_index| {
        const partial_index = byte_index / @sizeOf(f32);
        const baseline_bits: u32 = @bitCast(baseline_partial_values_host[partial_index]);
        const candidate_bits: u32 = @bitCast(candidate_partial_values_host[partial_index]);
        print(
            "CUDA Q6_K x Q8_1 LM argmax stage1 value mismatch: in={d} partial={d} baseline_bits=0x{x:0>8} candidate_bits=0x{x:0>8}\n",
            .{ in_dim, partial_index, baseline_bits, candidate_bits },
        );
        return error.CudaSmokeMismatch;
    }
    if (baseline_token_host != candidate_token_host) {
        print(
            "CUDA Q6_K x Q8_1 LM argmax token mismatch: in={d} baseline={d} candidate={d}\n",
            .{ in_dim, baseline_token_host, candidate_token_host },
        );
        return error.CudaSmokeMismatch;
    }

    var baseline_runs: [31]u64 = undefined;
    var candidate_runs: [31]u64 = undefined;
    const repeat_runs = cfg.quant_compiler_repeat_runs;
    for (0..repeat_runs) |run| {
        if (run % 2 == 0) {
            baseline_runs[run] = try timeCudaStep(ctx, cfg, launchQ6KQ8_1LmArgmaxBaseline, .{ baseline_function, module, ctx, baseline_token, baseline_partial_values, baseline_partial_indices, input_q8_1, input, weight, rows, in_dim, out_dim, q6KQ8_1LmArgmaxBaselineThreads(variant) });
            candidate_runs[run] = try timeCudaStep(ctx, cfg, launchQ6KQ8_1LmArgmaxCandidate, .{ candidate_function, module, ctx, candidate_token, candidate_partial_values, candidate_partial_indices, input_q8_1, input, weight, rows, in_dim, out_dim, variant.candidate_threads });
        } else {
            candidate_runs[run] = try timeCudaStep(ctx, cfg, launchQ6KQ8_1LmArgmaxCandidate, .{ candidate_function, module, ctx, candidate_token, candidate_partial_values, candidate_partial_indices, input_q8_1, input, weight, rows, in_dim, out_dim, variant.candidate_threads });
            baseline_runs[run] = try timeCudaStep(ctx, cfg, launchQ6KQ8_1LmArgmaxBaseline, .{ baseline_function, module, ctx, baseline_token, baseline_partial_values, baseline_partial_indices, input_q8_1, input, weight, rows, in_dim, out_dim, q6KQ8_1LmArgmaxBaselineThreads(variant) });
        }
    }
    const baseline_summary = try summarizeTimingRuns(baseline_runs[0..repeat_runs]);
    const candidate_summary = try summarizeTimingRuns(candidate_runs[0..repeat_runs]);
    print("CUDA Q6_K x Q8_1 LM argmax runs: in={d} baseline_ns={any} candidate_ns={any}\n", .{ in_dim, baseline_runs[0..repeat_runs], candidate_runs[0..repeat_runs] });
    print(
        "CUDA Q6_K x Q8_1 LM argmax summary: in={d} baseline={s} candidate={s} token={d} exact=true baseline_median_ns={d} baseline_mean_ns={d:.3} baseline_stddev_ns={d:.3} baseline_cv_pct={d:.4} candidate_median_ns={d} candidate_mean_ns={d:.3} candidate_stddev_ns={d:.3} candidate_cv_pct={d:.4} speedup={d:.6}\n",
        .{
            in_dim,
            q6KQ8_1LmArgmaxBaselineName(variant),
            variant.candidate_symbol,
            baseline_token_host,
            baseline_summary.median_ns,
            baseline_summary.mean_ns,
            baseline_summary.stddev_ns,
            baseline_summary.cv_percent,
            candidate_summary.median_ns,
            candidate_summary.mean_ns,
            candidate_summary.stddev_ns,
            candidate_summary.cv_percent,
            speedup(baseline_summary.median_ns, candidate_summary.median_ns),
        },
    );
}

const E2BFfnVariantBenchResult = struct {
    pair_speedup: f64,
    down_speedup: f64,
    chain_speedup: f64,
};

fn benchQ4_0Q8_1E2BFfnSm89(
    allocator: std.mem.Allocator,
    io: std.Io,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    cubin_dir: []const u8,
) !void {
    print(
        "CUDA E2B Q4_0 x llama CUDA Q8_1 FFN SM89: device={s} cc={d}.{d} rows=1 hidden={d} activation=gelu_new({d}) warmup={d} measure={d} repeats={d} cubin_dir={s} layout=half2(d,raw_sum)+signed_i8 production=false\n",
        .{ ctx.info.nameSlice(), ctx.info.compute_major, ctx.info.compute_minor, q4_0_q8_1_e2b_ffn_hidden_dim, q4_0_q8_1_e2b_ffn_activation, cfg.warmup_iters, cfg.measure_iters, cfg.quant_compiler_repeat_runs, cubin_dir },
    );

    var pair_log_sum: f64 = 0;
    var down_log_sum: f64 = 0;
    var chain_log_sum: f64 = 0;
    var worst_pair_speedup = std.math.inf(f64);
    var worst_down_speedup = std.math.inf(f64);
    var worst_chain_speedup = std.math.inf(f64);
    for (q4_0_q8_1_e2b_ffn_variants) |variant| {
        try validateQ4_0Q8_1E2BFfnVariant(variant);
        const result = try benchQ4_0Q8_1E2BFfnSm89Variant(allocator, io, ctx, module, cfg, cubin_dir, variant);
        pair_log_sum += @log(result.pair_speedup);
        down_log_sum += @log(result.down_speedup);
        chain_log_sum += @log(result.chain_speedup);
        worst_pair_speedup = @min(worst_pair_speedup, result.pair_speedup);
        worst_down_speedup = @min(worst_down_speedup, result.down_speedup);
        worst_chain_speedup = @min(worst_chain_speedup, result.chain_speedup);
    }
    const variant_count: f64 = @floatFromInt(q4_0_q8_1_e2b_ffn_variants.len);
    print(
        "CUDA E2B Q4_0 x GGML Q8_1 FFN SM89 overall: shape_candidates=4 shared_quantizer=1 variants=2 down_topology=grid_rows_times_out_block128_c1 production=false correctness=typed_layout+sum_differential+pair_down_chain_tolerance pair_geomean_speedup={d:.6} pair_worst_speedup={d:.6} down_geomean_speedup={d:.6} down_worst_speedup={d:.6} chain_geomean_speedup={d:.6} chain_worst_speedup={d:.6}\n",
        .{
            @exp(pair_log_sum / variant_count),
            worst_pair_speedup,
            @exp(down_log_sum / variant_count),
            worst_down_speedup,
            @exp(chain_log_sum / variant_count),
            worst_chain_speedup,
        },
    );
}

fn loadQ4_0Q8_1E2BFfnSm89Candidate(
    allocator: std.mem.Allocator,
    io: std.Io,
    ctx: *cuda_context.CudaContext,
    cubin_dir: []const u8,
    spec: E2BFfnCandidateSpec,
) !GeneratedCandidateModule {
    const cubin_path = try std.fs.path.join(allocator, &.{ cubin_dir, spec.artifact_filename });
    defer allocator.free(cubin_path);
    const candidate = GeneratedCandidateModule.load(allocator, io, ctx, cubin_path, spec.kernel_id) catch |err| {
        print(
            "CUDA E2B FFN SM89 candidate unavailable: kernel={s} cubin={s} error={s}\ncompile: nvcc -cubin -arch=sm_89 {s} -o {s}\n",
            .{ spec.kernel_id, cubin_path, @errorName(err), spec.source_path, cubin_path },
        );
        return error.CudaKernelUnavailable;
    };
    print("CUDA E2B FFN SM89 candidate loaded: kernel={s} cubin={s} production={}\n", .{ spec.kernel_id, cubin_path, spec.production_enabled });
    return candidate;
}

fn benchQ4_0Q8_1E2BFfnSm89Variant(
    allocator: std.mem.Allocator,
    io: std.Io,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    cubin_dir: []const u8,
    variant: E2BFfnVariant,
) !E2BFfnVariantBenchResult {
    var quantizer_candidate = try loadQ4_0Q8_1E2BFfnSm89Candidate(allocator, io, ctx, cubin_dir, q4_0_ggml_q8_1_quantizer_spec);
    defer quantizer_candidate.unload(ctx);
    var pair_candidate = try loadQ4_0Q8_1E2BFfnSm89Candidate(allocator, io, ctx, cubin_dir, variant.pair);
    defer pair_candidate.unload(ctx);
    var down_candidate = try loadQ4_0Q8_1E2BFfnSm89Candidate(allocator, io, ctx, cubin_dir, variant.down);
    defer down_candidate.unload(ctx);

    const rows = q4_0_q8_1_e2b_ffn_rows;
    const hidden_dim = q4_0_q8_1_e2b_ffn_hidden_dim;
    const inner_dim = variant.inner_dim;
    const hidden_blocks = hidden_dim / q8_1_values_per_block;
    const inner_blocks = inner_dim / q8_1_values_per_block;
    const hidden_q8_bytes = try checkedMul(try checkedMul(rows, hidden_blocks), q8_1_block_bytes);
    const inner_q8_bytes = try checkedMul(try checkedMul(rows, inner_blocks), q8_1_block_bytes);
    const pair_weight_bytes = try checkedMul(try checkedMul(inner_dim, hidden_blocks), q4_0_block_bytes);
    const down_weight_bytes = try checkedMul(try checkedMul(hidden_dim, inner_blocks), q4_0_block_bytes);

    const hidden_host = try allocator.alloc(f32, rows * hidden_dim);
    defer allocator.free(hidden_host);
    const gate_weight_host = try allocator.alloc(u8, pair_weight_bytes);
    defer allocator.free(gate_weight_host);
    const up_weight_host = try allocator.alloc(u8, pair_weight_bytes);
    defer allocator.free(up_weight_host);
    const down_weight_host = try allocator.alloc(u8, down_weight_bytes);
    defer allocator.free(down_weight_host);
    fillInput(hidden_host);
    fillQ4_0LmArgmaxWeights(gate_weight_host);
    fillQ4_0WeightsAlt(up_weight_host);
    fillQ4_0LmArgmaxWeights(down_weight_host);

    var hidden = try cuda_buffer.DeviceBuffer.alloc(ctx, hidden_host.len * @sizeOf(f32));
    defer hidden.free(ctx);
    var hidden_q8 = try cuda_buffer.DeviceBuffer.alloc(ctx, hidden_q8_bytes);
    defer hidden_q8.free(ctx);
    var candidate_hidden_q8 = try cuda_buffer.DeviceBuffer.alloc(ctx, hidden_q8_bytes);
    defer candidate_hidden_q8.free(ctx);
    var gate_weight_storage = try cuda_buffer.DeviceBuffer.alloc(ctx, try checkedMul(pair_weight_bytes, q4_0_q8_1_e2b_ffn_weight_ring));
    defer gate_weight_storage.free(ctx);
    var up_weight_storage = try cuda_buffer.DeviceBuffer.alloc(ctx, try checkedMul(pair_weight_bytes, q4_0_q8_1_e2b_ffn_weight_ring));
    defer up_weight_storage.free(ctx);
    var down_weight_storage = try cuda_buffer.DeviceBuffer.alloc(ctx, try checkedMul(down_weight_bytes, q4_0_q8_1_e2b_ffn_weight_ring));
    defer down_weight_storage.free(ctx);
    var gate_weights: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer = undefined;
    var up_weights: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer = undefined;
    var down_weights: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer = undefined;
    for (0..q4_0_q8_1_e2b_ffn_weight_ring) |ring_index| {
        gate_weights[ring_index] = .{
            .ptr = gate_weight_storage.ptr + @as(u64, @intCast(ring_index * pair_weight_bytes)),
            .len = pair_weight_bytes,
        };
        up_weights[ring_index] = .{
            .ptr = up_weight_storage.ptr + @as(u64, @intCast(ring_index * pair_weight_bytes)),
            .len = pair_weight_bytes,
        };
        down_weights[ring_index] = .{
            .ptr = down_weight_storage.ptr + @as(u64, @intCast(ring_index * down_weight_bytes)),
            .len = down_weight_bytes,
        };
    }
    const gate_weight = gate_weights[0];
    const up_weight = up_weights[0];
    const down_weight = down_weights[0];
    var pair_f32 = try cuda_buffer.DeviceBuffer.alloc(ctx, inner_dim * @sizeOf(f32));
    defer pair_f32.free(ctx);
    var baseline_pair_q8 = try cuda_buffer.DeviceBuffer.alloc(ctx, inner_q8_bytes);
    defer baseline_pair_q8.free(ctx);
    var candidate_pair_q8 = try cuda_buffer.DeviceBuffer.alloc(ctx, inner_q8_bytes);
    defer candidate_pair_q8.free(ctx);
    var baseline_down = try cuda_buffer.DeviceBuffer.alloc(ctx, hidden_dim * @sizeOf(f32));
    defer baseline_down.free(ctx);
    var candidate_down = try cuda_buffer.DeviceBuffer.alloc(ctx, hidden_dim * @sizeOf(f32));
    defer candidate_down.free(ctx);

    const hidden_f32_bytes = try checkedMul(rows * hidden_dim, @sizeOf(f32));
    const pair_f32_bytes = try checkedMul(rows * inner_dim, @sizeOf(f32));
    const down_f32_bytes = try checkedMul(rows * hidden_dim, @sizeOf(f32));
    var chain_hidden_storage = try cuda_buffer.DeviceBuffer.alloc(ctx, try checkedMul(hidden_f32_bytes, q4_0_q8_1_e2b_ffn_weight_ring));
    defer chain_hidden_storage.free(ctx);
    var chain_baseline_hidden_q8_storage = try cuda_buffer.DeviceBuffer.alloc(ctx, try checkedMul(hidden_q8_bytes, q4_0_q8_1_e2b_ffn_weight_ring));
    defer chain_baseline_hidden_q8_storage.free(ctx);
    var chain_candidate_hidden_q8_storage = try cuda_buffer.DeviceBuffer.alloc(ctx, try checkedMul(hidden_q8_bytes, q4_0_q8_1_e2b_ffn_weight_ring));
    defer chain_candidate_hidden_q8_storage.free(ctx);
    var chain_pair_f32_storage = try cuda_buffer.DeviceBuffer.alloc(ctx, try checkedMul(pair_f32_bytes, q4_0_q8_1_e2b_ffn_weight_ring));
    defer chain_pair_f32_storage.free(ctx);
    var chain_baseline_pair_q8_storage = try cuda_buffer.DeviceBuffer.alloc(ctx, try checkedMul(inner_q8_bytes, q4_0_q8_1_e2b_ffn_weight_ring));
    defer chain_baseline_pair_q8_storage.free(ctx);
    var chain_candidate_pair_q8_storage = try cuda_buffer.DeviceBuffer.alloc(ctx, try checkedMul(inner_q8_bytes, q4_0_q8_1_e2b_ffn_weight_ring));
    defer chain_candidate_pair_q8_storage.free(ctx);
    var chain_baseline_down_storage = try cuda_buffer.DeviceBuffer.alloc(ctx, try checkedMul(down_f32_bytes, q4_0_q8_1_e2b_ffn_weight_ring));
    defer chain_baseline_down_storage.free(ctx);
    var chain_candidate_down_storage = try cuda_buffer.DeviceBuffer.alloc(ctx, try checkedMul(down_f32_bytes, q4_0_q8_1_e2b_ffn_weight_ring));
    defer chain_candidate_down_storage.free(ctx);
    const chain_hidden = try deviceBufferRing(chain_hidden_storage, hidden_f32_bytes);
    const chain_baseline_hidden_q8 = try deviceBufferRing(chain_baseline_hidden_q8_storage, hidden_q8_bytes);
    const chain_candidate_hidden_q8 = try deviceBufferRing(chain_candidate_hidden_q8_storage, hidden_q8_bytes);
    const chain_pair_f32 = try deviceBufferRing(chain_pair_f32_storage, pair_f32_bytes);
    const chain_baseline_pair_q8 = try deviceBufferRing(chain_baseline_pair_q8_storage, inner_q8_bytes);
    const chain_candidate_pair_q8 = try deviceBufferRing(chain_candidate_pair_q8_storage, inner_q8_bytes);
    const chain_baseline_down = try deviceBufferRing(chain_baseline_down_storage, down_f32_bytes);
    const chain_candidate_down = try deviceBufferRing(chain_candidate_down_storage, down_f32_bytes);

    try hidden.copyFromHost(ctx, std.mem.sliceAsBytes(hidden_host));
    for (0..q4_0_q8_1_e2b_ffn_weight_ring) |ring_index| {
        try chain_hidden[ring_index].copyFromHost(ctx, std.mem.sliceAsBytes(hidden_host));
        try gate_weights[ring_index].copyFromHost(ctx, gate_weight_host);
        try up_weights[ring_index].copyFromHost(ctx, up_weight_host);
        try down_weights[ring_index].copyFromHost(ctx, down_weight_host);
    }
    try poisonDeviceBytes(allocator, ctx, chain_baseline_hidden_q8_storage, chain_baseline_hidden_q8_storage.len);
    try poisonDeviceBytes(allocator, ctx, chain_candidate_hidden_q8_storage, chain_candidate_hidden_q8_storage.len);
    try poisonDeviceBytes(allocator, ctx, chain_pair_f32_storage, chain_pair_f32_storage.len);
    try poisonDeviceBytes(allocator, ctx, chain_baseline_pair_q8_storage, chain_baseline_pair_q8_storage.len);
    try poisonDeviceBytes(allocator, ctx, chain_candidate_pair_q8_storage, chain_candidate_pair_q8_storage.len);
    try poisonDeviceBytes(allocator, ctx, chain_baseline_down_storage, chain_baseline_down_storage.len);
    try poisonDeviceBytes(allocator, ctx, chain_candidate_down_storage, chain_candidate_down_storage.len);
    try ctx.synchronize();

    try launchQ4_0Q8_1LmQuantize(module, ctx, hidden_q8, hidden, rows, hidden_dim);
    try launchGgmlQ8_1QuantizeRows(quantizer_candidate.function, ctx, candidate_hidden_q8, hidden, rows, hidden_dim);
    try launchQ4_0Q8_1E2BFfnPairBaseline(module, ctx, pair_f32, baseline_pair_q8, hidden_q8, gate_weight, up_weight, rows, hidden_dim, inner_dim);
    try poisonDeviceBytes(allocator, ctx, candidate_pair_q8, inner_q8_bytes);
    try launchQ4_0Q8_1E2BFfnPairCandidate(pair_candidate.function, ctx, candidate_pair_q8, candidate_hidden_q8, gate_weight, up_weight, variant);
    try ctx.synchronize();

    const hidden_q8_host = try allocator.alloc(u8, hidden_q8_bytes);
    defer allocator.free(hidden_q8_host);
    const candidate_hidden_q8_host = try allocator.alloc(u8, hidden_q8_bytes);
    defer allocator.free(candidate_hidden_q8_host);
    const baseline_pair_q8_host = try allocator.alloc(u8, inner_q8_bytes);
    defer allocator.free(baseline_pair_q8_host);
    const candidate_pair_q8_host = try allocator.alloc(u8, inner_q8_bytes);
    defer allocator.free(candidate_pair_q8_host);
    try hidden_q8.copyToHost(ctx, hidden_q8_host);
    try candidate_hidden_q8.copyToHost(ctx, candidate_hidden_q8_host);
    try baseline_pair_q8.copyToHost(ctx, baseline_pair_q8_host);
    try candidate_pair_q8.copyToHost(ctx, candidate_pair_q8_host);
    try ctx.synchronize();
    if (try firstQ8PayloadMismatchIgnoringGgmlSum(hidden_q8_host, candidate_hidden_q8_host)) |mismatch| {
        print(
            "CUDA E2B FFN SM89 exact mismatch: inner={d} stage=hidden_q8_payload byte={d} legacy=0x{x:0>2} ggml=0x{x:0>2}\n",
            .{ inner_dim, mismatch, hidden_q8_host[mismatch], candidate_hidden_q8_host[mismatch] },
        );
        return error.GeneratedCandidateMismatch;
    }
    const hidden_sum_diff = try maxGgmlQ8_1StoredSumDiff(candidate_hidden_q8_host, hidden_host);
    if (!std.math.isFinite(hidden_sum_diff) or hidden_sum_diff > 0.05) return error.GeneratedCandidateMismatch;
    const pair_payload_exact = (try firstQ8PayloadMismatchIgnoringGgmlSum(baseline_pair_q8_host, candidate_pair_q8_host)) == null;

    const hidden_dequant = try allocator.alloc(f32, hidden_dim);
    defer allocator.free(hidden_dequant);
    try quant_codec.dequantizeToFloat32(.{ .known = .Q8_1 }, hidden_q8_host, hidden_dequant);
    const gate_ref = try allocator.alloc(f32, inner_dim);
    defer allocator.free(gate_ref);
    const up_ref = try allocator.alloc(f32, inner_dim);
    defer allocator.free(up_ref);
    try quant_kernel_compiler.referenceMatmulNoBias(allocator, .q4_0, gate_weight_host, hidden_dequant, rows, hidden_dim, inner_dim, gate_ref);
    try quant_kernel_compiler.referenceMatmulNoBias(allocator, .q4_0, up_weight_host, hidden_dequant, rows, hidden_dim, inner_dim, up_ref);
    const activated_ref = try allocator.alloc(f32, inner_dim);
    defer allocator.free(activated_ref);
    var activated_amax: f32 = 0;
    for (activated_ref, 0..) |*value, index| {
        value.* = geluTanhF32(gate_ref[index]) * up_ref[index];
        activated_amax = @max(activated_amax, @abs(value.*));
    }
    const pair_tolerance_abs = activated_amax / 127.0 + q4_0_q8_1_e2b_ffn_pair_float_tolerance_abs;
    const baseline_pair_dequant = try allocator.alloc(f32, inner_dim);
    defer allocator.free(baseline_pair_dequant);
    const candidate_pair_dequant = try allocator.alloc(f32, inner_dim);
    defer allocator.free(candidate_pair_dequant);
    try quant_codec.dequantizeToFloat32(.{ .known = .Q8_1 }, baseline_pair_q8_host, baseline_pair_dequant);
    try quant_codec.dequantizeToFloat32(.{ .known = .Q8_1 }, candidate_pair_q8_host, candidate_pair_dequant);
    const baseline_pair_cpu_diff = try maxAbsDiffFinite(baseline_pair_dequant, activated_ref);
    const candidate_pair_cpu_diff = try maxAbsDiffFinite(candidate_pair_dequant, activated_ref);
    const candidate_pair_sum_diff = try maxGgmlQ8_1StoredSumDiff(candidate_pair_q8_host, activated_ref);
    const candidate_pair_sum_tolerance = @max(@as(f32, 0.05), activated_amax / 32.0 + 0.01);
    if (baseline_pair_cpu_diff > pair_tolerance_abs) return error.HandwrittenBaselineMismatch;
    if (candidate_pair_cpu_diff > pair_tolerance_abs) return error.GeneratedCandidateMismatch;
    if (!std.math.isFinite(candidate_pair_sum_diff) or candidate_pair_sum_diff > candidate_pair_sum_tolerance) return error.GeneratedCandidateMismatch;

    try launchQ4_0Q8_1E2BFfnDownBaseline(module, ctx, baseline_down, baseline_pair_q8, down_weight, variant);
    try poisonDeviceOutput(allocator, ctx, candidate_down, hidden_dim);
    try launchQ4_0Q8_1E2BFfnDownCandidate(down_candidate.function, ctx, candidate_down, candidate_pair_q8, down_weight, variant);
    try ctx.synchronize();
    const baseline_down_host = try allocator.alloc(f32, hidden_dim);
    defer allocator.free(baseline_down_host);
    const candidate_down_host = try allocator.alloc(f32, hidden_dim);
    defer allocator.free(candidate_down_host);
    try baseline_down.copyToHost(ctx, std.mem.sliceAsBytes(baseline_down_host));
    try candidate_down.copyToHost(ctx, std.mem.sliceAsBytes(candidate_down_host));
    try ctx.synchronize();
    const down_exact = (try firstByteMismatch(std.mem.sliceAsBytes(baseline_down_host), std.mem.sliceAsBytes(candidate_down_host))) == null;

    const down_ref = try allocator.alloc(f32, hidden_dim);
    defer allocator.free(down_ref);
    try quant_kernel_compiler.referenceMatmulNoBias(allocator, .q4_0, down_weight_host, baseline_pair_dequant, rows, inner_dim, hidden_dim, down_ref);
    const candidate_down_ref = try allocator.alloc(f32, hidden_dim);
    defer allocator.free(candidate_down_ref);
    try quant_kernel_compiler.referenceMatmulNoBias(allocator, .q4_0, down_weight_host, candidate_pair_dequant, rows, inner_dim, hidden_dim, candidate_down_ref);
    const baseline_down_cpu_diff = try maxAbsDiffFinite(baseline_down_host, down_ref);
    const candidate_down_cpu_diff = try maxAbsDiffFinite(candidate_down_host, candidate_down_ref);
    const baseline_candidate_down_diff = try maxAbsDiffFinite(baseline_down_host, candidate_down_host);
    if (baseline_down_cpu_diff > q4_0_q8_1_e2b_ffn_down_tolerance_abs) return error.HandwrittenBaselineMismatch;
    if (candidate_down_cpu_diff > q4_0_q8_1_e2b_ffn_down_tolerance_abs) return error.GeneratedCandidateMismatch;
    if (baseline_candidate_down_diff > q4_0_q8_1_e2b_ffn_down_tolerance_abs) return error.GeneratedCandidateMismatch;

    try launchQ4_0Q8_1E2BFfnChainBaseline(module, ctx, baseline_down, pair_f32, baseline_pair_q8, hidden_q8, hidden, gate_weight, up_weight, down_weight, variant);
    try poisonDeviceBytes(allocator, ctx, candidate_pair_q8, inner_q8_bytes);
    try poisonDeviceOutput(allocator, ctx, candidate_down, hidden_dim);
    try launchQ4_0Q8_1E2BFfnChainCandidate(quantizer_candidate.function, pair_candidate.function, down_candidate.function, module, ctx, candidate_down, candidate_pair_q8, candidate_hidden_q8, hidden, gate_weight, up_weight, down_weight, variant);
    try ctx.synchronize();
    try baseline_down.copyToHost(ctx, std.mem.sliceAsBytes(baseline_down_host));
    try candidate_down.copyToHost(ctx, std.mem.sliceAsBytes(candidate_down_host));
    try ctx.synchronize();
    const chain_exact = (try firstByteMismatch(std.mem.sliceAsBytes(baseline_down_host), std.mem.sliceAsBytes(candidate_down_host))) == null;
    const baseline_chain_cpu_diff = try maxAbsDiffFinite(baseline_down_host, down_ref);
    const candidate_chain_cpu_diff = try maxAbsDiffFinite(candidate_down_host, candidate_down_ref);
    const baseline_candidate_chain_diff = try maxAbsDiffFinite(baseline_down_host, candidate_down_host);
    if (baseline_chain_cpu_diff > q4_0_q8_1_e2b_ffn_down_tolerance_abs) return error.HandwrittenBaselineMismatch;
    if (candidate_chain_cpu_diff > q4_0_q8_1_e2b_ffn_down_tolerance_abs) return error.GeneratedCandidateMismatch;
    if (baseline_candidate_chain_diff > q4_0_q8_1_e2b_ffn_down_tolerance_abs) return error.GeneratedCandidateMismatch;

    print(
        "CUDA E2B FFN SM89 correctness: inner={d} layout=llama_cuda_q8_1_raw_sum hidden_payload_exact=true hidden_sum_max_abs_diff={d:.6} pair_payload_exact={} pair_sum_max_abs_diff={d:.6} pair_sum_tolerance={d:.6} pair_tolerance={d:.6} pair_baseline_cpu_max_abs_diff={d:.6} pair_candidate_cpu_max_abs_diff={d:.6} down_exact={} down_tolerance={d:.6} down_baseline_cpu_max_abs_diff={d:.6} down_candidate_cpu_max_abs_diff={d:.6} down_baseline_candidate_max_abs_diff={d:.6} chain_exact={} chain_baseline_cpu_max_abs_diff={d:.6} chain_candidate_cpu_max_abs_diff={d:.6} chain_baseline_candidate_max_abs_diff={d:.6}\n",
        .{
            inner_dim,
            hidden_sum_diff,
            pair_payload_exact,
            candidate_pair_sum_diff,
            candidate_pair_sum_tolerance,
            pair_tolerance_abs,
            baseline_pair_cpu_diff,
            candidate_pair_cpu_diff,
            down_exact,
            q4_0_q8_1_e2b_ffn_down_tolerance_abs,
            baseline_down_cpu_diff,
            candidate_down_cpu_diff,
            baseline_candidate_down_diff,
            chain_exact,
            baseline_chain_cpu_diff,
            candidate_chain_cpu_diff,
            baseline_candidate_chain_diff,
        },
    );

    var pair_baseline_runs: [31]u64 = undefined;
    var pair_candidate_runs: [31]u64 = undefined;
    try timeAlternatingCudaSteps(
        ctx,
        cfg,
        launchQ4_0Q8_1E2BFfnPairBaseline,
        .{ module, ctx, pair_f32, baseline_pair_q8, hidden_q8, gate_weight, up_weight, rows, hidden_dim, inner_dim },
        launchQ4_0Q8_1E2BFfnPairCandidate,
        .{ pair_candidate.function, ctx, candidate_pair_q8, candidate_hidden_q8, gate_weight, up_weight, variant },
        pair_baseline_runs[0..cfg.quant_compiler_repeat_runs],
        pair_candidate_runs[0..cfg.quant_compiler_repeat_runs],
    );
    const pair_speedup = try reportQ4_0Q8_1E2BFfnTiming(inner_dim, "pair", "fixed", 1, "termite_linear_q4_0_pair_activation_q8_1_f32_tile4+termite_quantize_f32_q8_1_rows", variant.pair.kernel_id, pair_payload_exact, pair_baseline_runs[0..cfg.quant_compiler_repeat_runs], pair_candidate_runs[0..cfg.quant_compiler_repeat_runs]);

    var down_baseline_runs: [31]u64 = undefined;
    var down_candidate_runs: [31]u64 = undefined;
    try timeAlternatingCudaSteps(
        ctx,
        cfg,
        launchQ4_0Q8_1E2BFfnDownBaseline,
        .{ module, ctx, baseline_down, baseline_pair_q8, down_weight, variant },
        launchQ4_0Q8_1E2BFfnDownCandidate,
        .{ down_candidate.function, ctx, candidate_down, candidate_pair_q8, down_weight, variant },
        down_baseline_runs[0..cfg.quant_compiler_repeat_runs],
        down_candidate_runs[0..cfg.quant_compiler_repeat_runs],
    );
    const down_speedup = try reportQ4_0Q8_1E2BFfnTiming(inner_dim, "down", "fixed", 1, "termite_linear_q4_0_q8_1_f32_tile4_w8", variant.down.kernel_id, down_exact, down_baseline_runs[0..cfg.quant_compiler_repeat_runs], down_candidate_runs[0..cfg.quant_compiler_repeat_runs]);

    var chain_baseline_runs: [31]u64 = undefined;
    var chain_candidate_runs: [31]u64 = undefined;
    var chain_baseline_state = E2BFfnChainBaselineRotatingState{
        .module = module,
        .ctx = ctx,
        .outputs = chain_baseline_down,
        .pair_f32 = chain_pair_f32,
        .activated_q8 = chain_baseline_pair_q8,
        .hidden_q8 = chain_baseline_hidden_q8,
        .hidden = chain_hidden,
        .gate_weights = gate_weights,
        .up_weights = up_weights,
        .down_weights = down_weights,
        .variant = variant,
    };
    var chain_candidate_state = E2BFfnChainCandidateRotatingState{
        .quantize_function = quantizer_candidate.function,
        .pair_function = pair_candidate.function,
        .down_function = down_candidate.function,
        .module = module,
        .ctx = ctx,
        .outputs = chain_candidate_down,
        .activated_q8 = chain_candidate_pair_q8,
        .hidden_q8 = chain_candidate_hidden_q8,
        .hidden = chain_hidden,
        .gate_weights = gate_weights,
        .up_weights = up_weights,
        .down_weights = down_weights,
        .variant = variant,
    };
    try timeAlternatingCudaSteps(
        ctx,
        cfg,
        launchQ4_0Q8_1E2BFfnChainBaselineRotating,
        .{&chain_baseline_state},
        launchQ4_0Q8_1E2BFfnChainCandidateRotating,
        .{&chain_candidate_state},
        chain_baseline_runs[0..cfg.quant_compiler_repeat_runs],
        chain_candidate_runs[0..cfg.quant_compiler_repeat_runs],
    );
    const chain_speedup = try reportQ4_0Q8_1E2BFfnTiming(inner_dim, "chain", "rotating_weights", q4_0_q8_1_e2b_ffn_weight_ring, "termite_quantize_f32_q8_1_rows+termite_linear_q4_0_pair_activation_q8_1_f32_tile4+termite_quantize_f32_q8_1_rows+termite_linear_q4_0_q8_1_f32_tile4_w8", "antfly_quantize_f32_ggml_q8_1_rows_v1+generated_pair_ggml_q8_1+generated_down_ggml_q8_1", chain_exact, chain_baseline_runs[0..cfg.quant_compiler_repeat_runs], chain_candidate_runs[0..cfg.quant_compiler_repeat_runs]);

    return .{
        .pair_speedup = pair_speedup,
        .down_speedup = down_speedup,
        .chain_speedup = chain_speedup,
    };
}

fn reportQ4_0Q8_1E2BFfnTiming(
    inner_dim: usize,
    stage: []const u8,
    cache_mode: []const u8,
    weight_ring: usize,
    baseline_name: []const u8,
    candidate_name: []const u8,
    exact: bool,
    baseline_runs: []const u64,
    candidate_runs: []const u64,
) !f64 {
    const baseline_summary = try summarizeTimingRuns(baseline_runs);
    const candidate_summary = try summarizeTimingRuns(candidate_runs);
    const candidate_speedup = speedup(baseline_summary.median_ns, candidate_summary.median_ns);
    print(
        "CUDA E2B FFN SM89 runs: inner={d} stage={s} cache_mode={s} weight_ring={d} activation_ring={d} output_ring={d} baseline={s} candidate={s} baseline_ns={any} candidate_ns={any}\n",
        .{ inner_dim, stage, cache_mode, weight_ring, weight_ring, weight_ring, baseline_name, candidate_name, baseline_runs, candidate_runs },
    );
    print(
        "CUDA E2B FFN SM89 summary: inner={d} stage={s} cache_mode={s} weight_ring={d} activation_ring={d} output_ring={d} exact={} production=false baseline_median_ns={d} baseline_mean_ns={d:.3} baseline_stddev_ns={d:.3} baseline_cv_pct={d:.4} candidate_median_ns={d} candidate_mean_ns={d:.3} candidate_stddev_ns={d:.3} candidate_cv_pct={d:.4} speedup={d:.6}\n",
        .{
            inner_dim,
            stage,
            cache_mode,
            weight_ring,
            weight_ring,
            weight_ring,
            exact,
            baseline_summary.median_ns,
            baseline_summary.mean_ns,
            baseline_summary.stddev_ns,
            baseline_summary.cv_percent,
            candidate_summary.median_ns,
            candidate_summary.mean_ns,
            candidate_summary.stddev_ns,
            candidate_summary.cv_percent,
            candidate_speedup,
        },
    );
    return candidate_speedup;
}

fn benchShape(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    shape: Shape,
) !void {
    if (shape.in_dim % q4_k_values_per_block != 0) return error.InvalidArgument;

    const input_count = try std.math.mul(usize, shape.rows, shape.in_dim);
    const output_count = try std.math.mul(usize, shape.rows, shape.out_dim);
    const row_blocks = shape.in_dim / q4_k_values_per_block;
    const weight_bytes = try std.math.mul(usize, try std.math.mul(usize, shape.out_dim, row_blocks), q4_k_block_bytes);

    const input_host = try allocator.alloc(f32, input_count);
    defer allocator.free(input_host);
    const bias_host = try allocator.alloc(f32, shape.out_dim);
    defer allocator.free(bias_host);
    const weight_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_host);
    fillInput(input_host);
    fillBias(bias_host);
    fillQ4KWeights(weight_host);

    var input = try cuda_buffer.DeviceBuffer.alloc(ctx, input_count * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_host.len);
    defer weight.free(ctx);
    var bias = try cuda_buffer.DeviceBuffer.alloc(ctx, bias_host.len * @sizeOf(f32));
    defer bias.free(ctx);
    var output = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
    defer output.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(input_host));
    try weight.copyFromHost(ctx, weight_host);
    try bias.copyFromHost(ctx, std.mem.sliceAsBytes(bias_host));
    try ctx.synchronize();

    const no_bias_ns = try timeCudaStep(ctx, cfg, launchQ4K, .{ module, ctx, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });
    const no_bias_tiled_ns = try timeCudaStep(ctx, cfg, launchQ4KTiled, .{ module, ctx, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });
    const bias_ns = try timeCudaStep(ctx, cfg, launchQ4KBias, .{ module, ctx, output, input, weight, bias, shape.rows, shape.in_dim, shape.out_dim });
    const bias_tiled_ns = try timeCudaStep(ctx, cfg, launchQ4KBiasTiled, .{ module, ctx, output, input, weight, bias, shape.rows, shape.in_dim, shape.out_dim });
    const quick_gelu_ns = try timeCudaStep(ctx, cfg, launchQ4KBiasQuickGeluTiled, .{ module, ctx, output, input, weight, bias, shape.rows, shape.in_dim, shape.out_dim });

    var sample: [1]f32 = undefined;
    try output.copyToHost(ctx, std.mem.sliceAsBytes(&sample));
    try ctx.synchronize();

    print("{s:<24} {d:>8} {d:>8} {d:>8} {d:>14} {d:>14} {d:>14} {d:>14} {d:>14} {d:>12.4}\n", .{
        shape.label,
        shape.rows,
        shape.in_dim,
        shape.out_dim,
        no_bias_ns,
        no_bias_tiled_ns,
        bias_ns,
        bias_tiled_ns,
        quick_gelu_ns,
        sample[0],
    });
}

const GeneratedCandidateModule = if (build_options.enable_cuda) struct {
    module: cuda_driver.CUmodule = null,
    function: cuda_driver.CUfunction = null,

    fn load(
        allocator: std.mem.Allocator,
        io: std.Io,
        ctx: *cuda_context.CudaContext,
        ptx_path: []const u8,
        kernel_id: []const u8,
    ) !GeneratedCandidateModule {
        const ptx = try std.Io.Dir.cwd().readFileAlloc(io, ptx_path, allocator, .limited(16 * 1024 * 1024));
        defer allocator.free(ptx);
        const image = try allocator.alloc(u8, ptx.len + 1);
        defer allocator.free(image);
        @memcpy(image[0..ptx.len], ptx);
        image[ptx.len] = 0;

        try ctx.makeCurrent();
        var module: cuda_driver.CUmodule = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);

        const kernel_name = try allocator.dupeSentinel(u8, kernel_id, 0);
        defer allocator.free(kernel_name);
        var function: cuda_driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, kernel_name));

        return .{
            .module = module,
            .function = function,
        };
    }

    fn unload(self: *GeneratedCandidateModule, ctx: *cuda_context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
            self.module = null;
            self.function = null;
        }
    }
} else struct {};

fn benchQuantCompilerLazyTarget(
    allocator: std.mem.Allocator,
    io: std.Io,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
) !void {
    const bench = quant_kernel_compiler.first_lazy_benchmark;
    const lowering = quant_kernel_compiler.registryLoweringFor(.cuda, bench.format, bench.row_bucket, bench.epilogue, .small_batch);
    const route_diagnostic = try quant_kernel_compiler.loweringDiagnostic(allocator, lowering);
    defer allocator.free(route_diagnostic);
    const shape = quant_compiler_lazy_shape;
    const input_count = try std.math.mul(usize, shape.rows, shape.in_dim);
    const output_count = try std.math.mul(usize, shape.rows, shape.out_dim);
    const row_blocks = shape.in_dim / q4_k_values_per_block;
    const weight_bytes = try std.math.mul(usize, try std.math.mul(usize, shape.out_dim, row_blocks), q4_k_block_bytes);

    const input_host = try allocator.alloc(f32, input_count);
    defer allocator.free(input_host);
    const bias_host = try allocator.alloc(f32, shape.out_dim);
    defer allocator.free(bias_host);
    const weight_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_host);
    fillInput(input_host);
    fillBias(bias_host);
    fillQ4KWeights(weight_host);

    const reference_host = try allocator.alloc(f32, output_count);
    defer allocator.free(reference_host);
    try quant_kernel_compiler.referenceMatmulBiasGelu(
        allocator,
        bench.format,
        weight_host,
        input_host,
        bias_host,
        shape.rows,
        shape.in_dim,
        shape.out_dim,
        reference_host,
    );

    var input = try cuda_buffer.DeviceBuffer.alloc(ctx, input_count * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_host.len);
    defer weight.free(ctx);
    var bias = try cuda_buffer.DeviceBuffer.alloc(ctx, bias_host.len * @sizeOf(f32));
    defer bias.free(ctx);
    var output = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
    defer output.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(input_host));
    try weight.copyFromHost(ctx, weight_host);
    try bias.copyFromHost(ctx, std.mem.sliceAsBytes(bias_host));
    try ctx.synchronize();

    const baseline_ns = try repeatCudaStep(allocator, ctx, cfg, launchQ4KBiasGeluRows2, .{ module, ctx, output, input, weight, bias, shape.rows, shape.in_dim, shape.out_dim });
    const baseline_host = try allocator.alloc(f32, output_count);
    defer allocator.free(baseline_host);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(baseline_host));
    try ctx.synchronize();
    const baseline_cpu_max_abs_diff = try maxAbsDiffFinite(baseline_host, reference_host);
    if (baseline_cpu_max_abs_diff > bench.correctness_tolerance_abs) return error.HandwrittenBaselineMismatch;

    // Poison the shared output buffer so a generated kernel that silently
    // early-returns cannot inherit the baseline's results and pass.
    try poisonDeviceOutput(allocator, ctx, output, output_count);

    print("CUDA quant compiler lazy target: {s} rows={d} in={d} out={d}\n", .{ route_diagnostic, shape.rows, shape.in_dim, shape.out_dim });
    print("baseline kernel={s} ns={d} checksum={d:.6} cpu_max_abs_diff={d:.6}\n", .{ bench.handwritten_baseline, baseline_ns, baseline_host[0], baseline_cpu_max_abs_diff });
    print("candidate kernel={s} source={s} production=false\n", .{ bench.generated_kernel_id, bench.generated_source_path });

    const ptx_path = cfg.quant_compiler_generated_ptx_path orelse return error.MissingGeneratedPtxPath;
    if (cfg.quant_compiler_evidence_out_path) |evidence_path| try validateQuantCompilerEvidenceRequest(bench, ptx_path, evidence_path);

    var generated = try GeneratedCandidateModule.load(allocator, io, ctx, ptx_path, bench.generated_kernel_id);
    defer generated.unload(ctx);
    const generated_ns = try repeatCudaStep(allocator, ctx, cfg, launchGeneratedQ4KBiasGeluRows2, .{ &generated, ctx, output, input, weight, bias, shape.rows, shape.in_dim, shape.out_dim });

    const generated_host = try allocator.alloc(f32, output_count);
    defer allocator.free(generated_host);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(generated_host));
    try ctx.synchronize();

    const baseline_max_abs_diff = try maxAbsDiffFinite(generated_host, baseline_host);
    const cpu_max_abs_diff = try maxAbsDiffFinite(generated_host, reference_host);
    if (cpu_max_abs_diff > bench.correctness_tolerance_abs) return error.GeneratedCandidateMismatch;
    const candidate_speedup = speedup(baseline_ns, generated_ns);
    print("generated ptx={s} ns={d} speedup={d:.6} min_speedup={d:.6} checksum={d:.6} baseline_max_abs_diff={d:.6} cpu_max_abs_diff={d:.6} tolerance={d:.6}\n", .{
        ptx_path,
        generated_ns,
        candidate_speedup,
        bench.minimum_speedup,
        generated_host[0],
        baseline_max_abs_diff,
        cpu_max_abs_diff,
        bench.correctness_tolerance_abs,
    });
    if (candidate_speedup < bench.minimum_speedup) return error.GeneratedCandidateSlowerThanBaseline;
    if (cfg.quant_compiler_evidence_out_path) |evidence_path| {
        try writeQuantCompilerEvidence(
            allocator,
            io,
            evidence_path,
            bench,
            shape,
            ptx_path,
            cfg,
            baseline_ns,
            generated_ns,
            baseline_host[0],
            generated_host[0],
            baseline_cpu_max_abs_diff,
            baseline_max_abs_diff,
            cpu_max_abs_diff,
        );
        print("wrote quant compiler evidence: {s}\n", .{evidence_path});
    }
}

fn benchQuantCompilerQ4_0Target(
    allocator: std.mem.Allocator,
    io: std.Io,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    kind: QuantCompilerQ4_0Kind,
    ptx_path: []const u8,
) !void {
    const bench = switch (kind) {
        .mmv => quant_kernel_compiler.first_q4_0_mmv_benchmark,
        .mm => quant_kernel_compiler.first_q4_0_mm_benchmark,
    };
    const kernel_id = bench.generated_kernel_id;
    const rows: usize = switch (kind) {
        .mmv => quant_compiler_q4_0_mmv_rows,
        .mm => quant_compiler_q4_0_mm_rows,
    };
    const baseline_name = bench.handwritten_baseline;
    const baseline_function = switch (kind) {
        .mmv => module.linear_q4_0_f32_tile4,
        .mm => module.linear_q4_0_f32,
    };
    const lowering = switch (kind) {
        .mmv => quant_kernel_compiler.registryLoweringFor(.cuda, .q4_0, .rows_1, .none, .mmv),
        .mm => quant_kernel_compiler.registryLoweringFor(.cuda, .q4_0, .rows_9_64, .none, .mm),
    };
    const route_diagnostic = try quant_kernel_compiler.loweringDiagnostic(allocator, lowering);
    defer allocator.free(route_diagnostic);
    const correctness_tolerance_abs: f32 = bench.correctness_tolerance_abs;
    if (cfg.quant_compiler_evidence_out_path) |evidence_path| {
        if (!std.mem.eql(u8, ptx_path, bench.generated_ptx_path)) return error.GeneratedPtxPathMismatch;
        if (!std.mem.eql(u8, evidence_path, bench.benchmark_evidence_path)) return error.QuantCompilerEvidencePathMismatch;
    }

    var generated = try GeneratedCandidateModule.load(allocator, io, ctx, ptx_path, kernel_id);
    defer generated.unload(ctx);

    print("CUDA quant compiler q4_0 {s} target: {s} rows={d} ptx={s}\n", .{ @tagName(kind), route_diagnostic, rows, ptx_path });
    print("candidate kernel={s} baseline kernel={s} production={} tolerance={d:.6}\n", .{ kernel_id, baseline_name, bench.production_enabled, correctness_tolerance_abs });

    var dims_list = std.ArrayListUnmanaged(Shape).empty;
    defer dims_list.deinit(allocator);
    try dims_list.appendSlice(allocator, &quant_compiler_q4_0_dims);
    if (kind == .mmv) try dims_list.appendSlice(allocator, &quant_compiler_q4_0_mmv_extra_dims);

    var worst_speedup: f64 = std.math.inf(f64);
    var speedup_log_sum: f64 = 0.0;
    var eligible_shape_count: usize = 0;
    var shape_details = std.ArrayListUnmanaged(u8).empty;
    defer shape_details.deinit(allocator);
    for (dims_list.items, 0..) |dims, shape_index| {
        const shape = Shape{ .label = dims.label, .rows = rows, .in_dim = dims.in_dim, .out_dim = dims.out_dim };
        const has_cpu_reference = shape.out_dim <= quant_compiler_q4_0_max_reference_out_dim;
        const input_count = try std.math.mul(usize, shape.rows, shape.in_dim);
        const output_count = try std.math.mul(usize, shape.rows, shape.out_dim);
        const row_blocks = shape.in_dim / q4_0_values_per_block;
        const weight_bytes = try std.math.mul(usize, try std.math.mul(usize, shape.out_dim, row_blocks), q4_0_block_bytes);

        const input_host = try allocator.alloc(f32, input_count);
        defer allocator.free(input_host);
        const weight_host = try allocator.alloc(u8, weight_bytes);
        defer allocator.free(weight_host);
        fillInput(input_host);
        fillQ4_0Weights(weight_host);

        const reference_host = try allocator.alloc(f32, output_count);
        defer allocator.free(reference_host);
        if (has_cpu_reference) {
            try quant_kernel_compiler.referenceMatmulNoBias(
                allocator,
                .q4_0,
                weight_host,
                input_host,
                shape.rows,
                shape.in_dim,
                shape.out_dim,
                reference_host,
            );
        }

        var input = try cuda_buffer.DeviceBuffer.alloc(ctx, input_count * @sizeOf(f32));
        defer input.free(ctx);
        var weight = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_host.len);
        defer weight.free(ctx);
        var output = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
        defer output.free(ctx);

        try input.copyFromHost(ctx, std.mem.sliceAsBytes(input_host));
        try weight.copyFromHost(ctx, weight_host);
        try ctx.synchronize();

        const baseline_ns = try repeatCudaStep(allocator, ctx, cfg, launchQ4_0Baseline, .{ baseline_function, ctx, kind, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });
        const baseline_host = try allocator.alloc(f32, output_count);
        defer allocator.free(baseline_host);
        try output.copyToHost(ctx, std.mem.sliceAsBytes(baseline_host));
        try ctx.synchronize();
        const baseline_cpu_max_abs_diff = if (has_cpu_reference) try maxAbsDiffFinite(baseline_host, reference_host) else 0.0;
        if (has_cpu_reference and baseline_cpu_max_abs_diff > correctness_tolerance_abs) return error.HandwrittenBaselineMismatch;

        // Poison the shared output buffer so a generated kernel that silently
        // early-returns cannot inherit the baseline's results and pass.
        try poisonDeviceOutput(allocator, ctx, output, output_count);

        const generated_ns = try repeatCudaStep(allocator, ctx, cfg, launchGeneratedQ4_0, .{ &generated, ctx, kind, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });
        const generated_host = try allocator.alloc(f32, output_count);
        defer allocator.free(generated_host);
        try output.copyToHost(ctx, std.mem.sliceAsBytes(generated_host));
        try ctx.synchronize();

        const baseline_max_abs_diff = try maxAbsDiffFinite(generated_host, baseline_host);
        if (baseline_max_abs_diff > correctness_tolerance_abs) return error.GeneratedCandidateMismatch;
        const cpu_max_abs_diff = if (has_cpu_reference) try maxAbsDiffFinite(generated_host, reference_host) else 0.0;
        if (has_cpu_reference and cpu_max_abs_diff > correctness_tolerance_abs) return error.GeneratedCandidateMismatch;

        const candidate_speedup = speedup(baseline_ns, generated_ns);
        const production_route_eligible = quant_kernel_compiler.generatedArtifactSupportsPlan(
            .cuda,
            quant_matmul.plan(.{ .rows = shape.rows, .in_dim = shape.in_dim, .out_dim = shape.out_dim, .format = .q4_0 }),
            .none,
        );
        if (production_route_eligible) {
            worst_speedup = @min(worst_speedup, candidate_speedup);
            speedup_log_sum += @log(candidate_speedup);
            eligible_shape_count += 1;
        }
        print("shape={s} rows={d} in={d} out={d} baseline_ns={d} generated_ns={d} speedup={d:.6} cpu_reference={} baseline_cpu_max_abs_diff={d:.6} generated_cpu_max_abs_diff={d:.6} generated_baseline_max_abs_diff={d:.6}\n", .{
            shape.label,
            shape.rows,
            shape.in_dim,
            shape.out_dim,
            baseline_ns,
            generated_ns,
            candidate_speedup,
            has_cpu_reference,
            baseline_cpu_max_abs_diff,
            cpu_max_abs_diff,
            baseline_max_abs_diff,
        });
        try appendJsonFmt(allocator, &shape_details,
            \\{s}{{"label":{f},"rows":{d},"in_dim":{d},"out_dim":{d},"baseline_ns":{d},"generated_ns":{d},"speedup":{d:.6},"production_route_eligible":{},"cpu_reference":{},"baseline_cpu_max_abs_diff":{d:.6},"generated_cpu_max_abs_diff":{d:.6},"generated_baseline_max_abs_diff":{d:.6}}}
        , .{
            if (shape_index == 0) "" else ",",
            std.json.fmt(shape.label, .{}),
            shape.rows,
            shape.in_dim,
            shape.out_dim,
            baseline_ns,
            generated_ns,
            candidate_speedup,
            production_route_eligible,
            has_cpu_reference,
            baseline_cpu_max_abs_diff,
            cpu_max_abs_diff,
            baseline_max_abs_diff,
        });
    }
    if (eligible_shape_count == 0) return error.NoProductionEligibleBenchmarkShapes;
    const geomean_speedup = @exp(speedup_log_sum / @as(f64, @floatFromInt(eligible_shape_count)));
    print("q4_0 {s} summary: geomean_speedup={d:.6} worst_speedup={d:.6} eligible_shapes={d} measured_shapes={d}\n", .{ @tagName(kind), geomean_speedup, worst_speedup, eligible_shape_count, dims_list.items.len });
    if (cfg.quant_compiler_evidence_out_path) |evidence_path| {
        try writeQuantCompilerQ4_0Evidence(allocator, io, evidence_path, bench, cfg, geomean_speedup, worst_speedup, shape_details.items);
        print("wrote quant compiler evidence: {s}\n", .{evidence_path});
    }
}

const quant_compiler_q4_0_pair_dims = [_]Shape{
    .{ .label = "E2B FFN gate+up", .rows = 1, .in_dim = 1536, .out_dim = 8960 },
    .{ .label = "E4B FFN gate+up", .rows = 1, .in_dim = 2560, .out_dim = 10240 },
};

fn benchQuantCompilerQ4_0PairTarget(
    allocator: std.mem.Allocator,
    io: std.Io,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    ptx_path: []const u8,
) !void {
    const bench = quant_kernel_compiler.first_q4_0_pair_benchmark;
    const kernel_id = bench.generated_kernel_id;
    const baseline_name = bench.handwritten_baseline;
    const lowering = quant_kernel_compiler.registryLoweringFor(.cuda, .q4_0, .rows_1, .pair, .mmv);
    const route_diagnostic = try quant_kernel_compiler.loweringDiagnostic(allocator, lowering);
    defer allocator.free(route_diagnostic);
    const correctness_tolerance_abs: f32 = bench.correctness_tolerance_abs;
    if (cfg.quant_compiler_evidence_out_path) |evidence_path| {
        if (!std.mem.eql(u8, ptx_path, bench.generated_ptx_path)) return error.GeneratedPtxPathMismatch;
        if (!std.mem.eql(u8, evidence_path, bench.benchmark_evidence_path)) return error.QuantCompilerEvidencePathMismatch;
    }

    var generated = try GeneratedCandidateModule.load(allocator, io, ctx, ptx_path, kernel_id);
    defer generated.unload(ctx);

    print("CUDA quant compiler q4_0 pair target: {s} rows=1 ptx={s}\n", .{ route_diagnostic, ptx_path });
    print("candidate kernel={s} baseline kernel={s} production={} tolerance={d:.6}\n", .{ kernel_id, baseline_name, bench.production_enabled, correctness_tolerance_abs });

    var worst_speedup: f64 = std.math.inf(f64);
    var speedup_log_sum: f64 = 0.0;
    var shape_details = std.ArrayListUnmanaged(u8).empty;
    defer shape_details.deinit(allocator);
    for (quant_compiler_q4_0_pair_dims, 0..) |shape, shape_index| {
        const input_count = shape.in_dim;
        const output_count = shape.out_dim;
        const row_blocks = shape.in_dim / q4_0_values_per_block;
        const weight_bytes = try std.math.mul(usize, try std.math.mul(usize, shape.out_dim, row_blocks), q4_0_block_bytes);

        const input_host = try allocator.alloc(f32, input_count);
        defer allocator.free(input_host);
        const weight_a_host = try allocator.alloc(u8, weight_bytes);
        defer allocator.free(weight_a_host);
        const weight_b_host = try allocator.alloc(u8, weight_bytes);
        defer allocator.free(weight_b_host);
        fillInput(input_host);
        fillQ4_0Weights(weight_a_host);
        fillQ4_0WeightsAlt(weight_b_host);

        const reference_a = try allocator.alloc(f32, output_count);
        defer allocator.free(reference_a);
        const reference_b = try allocator.alloc(f32, output_count);
        defer allocator.free(reference_b);
        try quant_kernel_compiler.referenceMatmulNoBias(allocator, .q4_0, weight_a_host, input_host, 1, shape.in_dim, shape.out_dim, reference_a);
        try quant_kernel_compiler.referenceMatmulNoBias(allocator, .q4_0, weight_b_host, input_host, 1, shape.in_dim, shape.out_dim, reference_b);

        var input = try cuda_buffer.DeviceBuffer.alloc(ctx, input_count * @sizeOf(f32));
        defer input.free(ctx);
        var weight_a = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_a_host.len);
        defer weight_a.free(ctx);
        var weight_b = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_b_host.len);
        defer weight_b.free(ctx);
        var output_a = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
        defer output_a.free(ctx);
        var output_b = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
        defer output_b.free(ctx);

        try input.copyFromHost(ctx, std.mem.sliceAsBytes(input_host));
        try weight_a.copyFromHost(ctx, weight_a_host);
        try weight_b.copyFromHost(ctx, weight_b_host);
        try ctx.synchronize();

        const baseline_ns = try repeatCudaStep(allocator, ctx, cfg, launchQ4_0PairBaseline, .{ module, ctx, output_a, output_b, input, weight_a, weight_b, shape.in_dim, shape.out_dim });
        const baseline_a = try allocator.alloc(f32, output_count);
        defer allocator.free(baseline_a);
        const baseline_b = try allocator.alloc(f32, output_count);
        defer allocator.free(baseline_b);
        try output_a.copyToHost(ctx, std.mem.sliceAsBytes(baseline_a));
        try output_b.copyToHost(ctx, std.mem.sliceAsBytes(baseline_b));
        try ctx.synchronize();
        if (try maxAbsDiffFinite(baseline_a, reference_a) > correctness_tolerance_abs) return error.HandwrittenBaselineMismatch;
        if (try maxAbsDiffFinite(baseline_b, reference_b) > correctness_tolerance_abs) return error.HandwrittenBaselineMismatch;

        // Poison the shared output buffers so a generated kernel that silently
        // early-returns cannot inherit the baseline's results and pass.
        try poisonDeviceOutput(allocator, ctx, output_a, output_count);
        try poisonDeviceOutput(allocator, ctx, output_b, output_count);

        const generated_ns = try repeatCudaStep(allocator, ctx, cfg, launchGeneratedQ4_0Pair, .{ &generated, ctx, output_a, output_b, input, weight_a, weight_b, shape.in_dim, shape.out_dim });
        const generated_a = try allocator.alloc(f32, output_count);
        defer allocator.free(generated_a);
        const generated_b = try allocator.alloc(f32, output_count);
        defer allocator.free(generated_b);
        try output_a.copyToHost(ctx, std.mem.sliceAsBytes(generated_a));
        try output_b.copyToHost(ctx, std.mem.sliceAsBytes(generated_b));
        try ctx.synchronize();

        const cpu_max_abs_diff_a = try maxAbsDiffFinite(generated_a, reference_a);
        const cpu_max_abs_diff_b = try maxAbsDiffFinite(generated_b, reference_b);
        if (cpu_max_abs_diff_a > correctness_tolerance_abs or cpu_max_abs_diff_b > correctness_tolerance_abs) return error.GeneratedCandidateMismatch;

        const candidate_speedup = speedup(baseline_ns, generated_ns);
        worst_speedup = @min(worst_speedup, candidate_speedup);
        speedup_log_sum += @log(candidate_speedup);
        print("shape={s} rows=1 in={d} out={d} baseline_ns={d} generated_ns={d} speedup={d:.6} cpu_max_abs_diff_a={d:.6} cpu_max_abs_diff_b={d:.6}\n", .{
            shape.label,
            shape.in_dim,
            shape.out_dim,
            baseline_ns,
            generated_ns,
            candidate_speedup,
            cpu_max_abs_diff_a,
            cpu_max_abs_diff_b,
        });
        try appendJsonFmt(allocator, &shape_details,
            \\{s}{{"label":{f},"rows":1,"in_dim":{d},"out_dim":{d},"baseline_ns":{d},"generated_ns":{d},"speedup":{d:.6},"generated_cpu_max_abs_diff_a":{d:.6},"generated_cpu_max_abs_diff_b":{d:.6}}}
        , .{
            if (shape_index == 0) "" else ",",
            std.json.fmt(shape.label, .{}),
            shape.in_dim,
            shape.out_dim,
            baseline_ns,
            generated_ns,
            candidate_speedup,
            cpu_max_abs_diff_a,
            cpu_max_abs_diff_b,
        });
    }
    const geomean_speedup = @exp(speedup_log_sum / @as(f64, @floatFromInt(quant_compiler_q4_0_pair_dims.len)));
    print("q4_0 pair summary: geomean_speedup={d:.6} worst_speedup={d:.6} shapes={d}\n", .{ geomean_speedup, worst_speedup, quant_compiler_q4_0_pair_dims.len });
    if (cfg.quant_compiler_evidence_out_path) |evidence_path| {
        try writeQuantCompilerQ4_0Evidence(allocator, io, evidence_path, bench, cfg, geomean_speedup, worst_speedup, shape_details.items);
        print("wrote quant compiler evidence: {s}\n", .{evidence_path});
    }
}

// Experimental race harness for the tuned Gemma4 E4B decode FFN gate+up
// kernel (q8_1 activations in, fused activation multiply, q8_1 out). The
// candidate is compiler-owned (plan cuda/q4_0/rows_1/pair_activation/mmv)
// and shares the baseline's exact launch contract.
const quant_compiler_q4_0_pair_q8_kernel_id = "antfly_q4_0_pair_activation_q8_1_mmv_v1";
const quant_compiler_q4_0_pair_q8_in_dim: usize = 2560;
const quant_compiler_q4_0_pair_q8_out_dim: usize = 10240;
const quant_compiler_q4_0_pair_q8_activation: u32 = 0; // GELU-tanh
const q8_1_values_per_block: usize = 32;
const q8_1_block_bytes: usize = 36;

fn geluTanhF32(x: f32) f32 {
    const inner = 0.7978845608028654 * (x + 0.044715 * x * x * x);
    return 0.5 * x * (1.0 + std.math.tanh(inner));
}

fn benchQuantCompilerQ4_0PairQ8Target(
    allocator: std.mem.Allocator,
    io: std.Io,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    ptx_path: []const u8,
) !void {
    const bench = quant_kernel_compiler.first_q4_0_pair_q8_benchmark;
    const in_dim = quant_compiler_q4_0_pair_q8_in_dim;
    const out_dim = quant_compiler_q4_0_pair_q8_out_dim;
    const out_blocks = out_dim / q8_1_values_per_block;
    const row_blocks = in_dim / q4_0_values_per_block;
    const weight_bytes = out_dim * row_blocks * q4_0_block_bytes;
    const baseline_name = bench.handwritten_baseline;
    if (cfg.quant_compiler_evidence_out_path) |evidence_path| {
        if (!std.mem.eql(u8, ptx_path, bench.generated_ptx_path)) return error.GeneratedPtxPathMismatch;
        if (!std.mem.eql(u8, evidence_path, bench.benchmark_evidence_path)) return error.QuantCompilerEvidencePathMismatch;
    }

    var generated = try GeneratedCandidateModule.load(allocator, io, ctx, ptx_path, bench.generated_kernel_id);
    defer generated.unload(ctx);

    print("CUDA quant compiler q4_0 pair-q8 target: rows=1 in={d} out={d} activation=gelu_tanh ptx={s}\n", .{ in_dim, out_dim, ptx_path });
    print("candidate kernel={s} baseline kernel={s} production={}\n", .{ bench.generated_kernel_id, baseline_name, bench.production_enabled });

    const input_host = try allocator.alloc(f32, in_dim);
    defer allocator.free(input_host);
    fillInput(input_host);
    const q8_input_host = try quant_codec.quantizeQ8_1FromF32(allocator, input_host);
    defer allocator.free(q8_input_host);
    const weight_gate_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_gate_host);
    const weight_up_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_up_host);
    fillQ4_0Weights(weight_gate_host);
    fillQ4_0WeightsAlt(weight_up_host);

    // CPU reference: dequantize the q8 activations the kernels actually
    // consume, run both projections through the q4_0 reference matmul, then
    // apply the fused activation multiply.
    const act_dequant = try allocator.alloc(f32, in_dim);
    defer allocator.free(act_dequant);
    try quant_codec.dequantizeToFloat32(.{ .known = .Q8_1 }, q8_input_host, act_dequant);
    const gate_ref = try allocator.alloc(f32, out_dim);
    defer allocator.free(gate_ref);
    const up_ref = try allocator.alloc(f32, out_dim);
    defer allocator.free(up_ref);
    try quant_kernel_compiler.referenceMatmulNoBias(allocator, .q4_0, weight_gate_host, act_dequant, 1, in_dim, out_dim, gate_ref);
    try quant_kernel_compiler.referenceMatmulNoBias(allocator, .q4_0, weight_up_host, act_dequant, 1, in_dim, out_dim, up_ref);
    const activated_ref = try allocator.alloc(f32, out_dim);
    defer allocator.free(activated_ref);
    var activated_amax: f32 = 0.0;
    for (activated_ref, 0..) |*value, i| {
        value.* = geluTanhF32(gate_ref[i]) * up_ref[i];
        activated_amax = @max(activated_amax, @abs(value.*));
    }
    // The kernel output is per-block q8_1-quantized; allow one quantization
    // step at the global amax plus dot-product float noise.
    const correctness_tolerance_abs: f32 = activated_amax / 127.0 + 0.05;

    var q8_input = try cuda_buffer.DeviceBuffer.alloc(ctx, q8_input_host.len);
    defer q8_input.free(ctx);
    var weight_gate = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_bytes);
    defer weight_gate.free(ctx);
    var weight_up = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_bytes);
    defer weight_up.free(ctx);
    var dst_q8 = try cuda_buffer.DeviceBuffer.alloc(ctx, out_blocks * q8_1_block_bytes);
    defer dst_q8.free(ctx);
    try q8_input.copyFromHost(ctx, q8_input_host);
    try weight_gate.copyFromHost(ctx, weight_gate_host);
    try weight_up.copyFromHost(ctx, weight_up_host);
    try ctx.synchronize();

    const baseline_ns = try repeatCudaStep(allocator, ctx, cfg, launchQ4_0PairQ8, .{ module.linear_q4_0_pair_activation_q8_1_e4b, ctx, dst_q8, q8_input, weight_gate, weight_up, out_blocks });
    const baseline_q8_host = try allocator.alloc(u8, out_blocks * q8_1_block_bytes);
    defer allocator.free(baseline_q8_host);
    try dst_q8.copyToHost(ctx, baseline_q8_host);
    try ctx.synchronize();
    const baseline_dequant = try allocator.alloc(f32, out_dim);
    defer allocator.free(baseline_dequant);
    try quant_codec.dequantizeToFloat32(.{ .known = .Q8_1 }, baseline_q8_host, baseline_dequant);
    const baseline_cpu_max_abs_diff = try maxAbsDiffFinite(baseline_dequant, activated_ref);
    if (baseline_cpu_max_abs_diff > correctness_tolerance_abs) return error.HandwrittenBaselineMismatch;

    try poisonDeviceBytes(allocator, ctx, dst_q8, out_blocks * q8_1_block_bytes);

    const generated_ns = try repeatCudaStep(allocator, ctx, cfg, launchGeneratedQ4_0PairQ8, .{ generated.function, ctx, dst_q8, q8_input, weight_gate, weight_up, 1, in_dim, out_dim, out_blocks });
    const generated_q8_host = try allocator.alloc(u8, out_blocks * q8_1_block_bytes);
    defer allocator.free(generated_q8_host);
    try dst_q8.copyToHost(ctx, generated_q8_host);
    try ctx.synchronize();
    const generated_dequant = try allocator.alloc(f32, out_dim);
    defer allocator.free(generated_dequant);
    try quant_codec.dequantizeToFloat32(.{ .known = .Q8_1 }, generated_q8_host, generated_dequant);
    const cpu_max_abs_diff = try maxAbsDiffFinite(generated_dequant, activated_ref);
    if (cpu_max_abs_diff > correctness_tolerance_abs) return error.GeneratedCandidateMismatch;
    const baseline_max_abs_diff = try maxAbsDiffFinite(generated_dequant, baseline_dequant);

    const candidate_speedup = speedup(baseline_ns, generated_ns);
    print("shape=E4B FFN gate+up q8_1 rows=1 in={d} out={d} baseline_ns={d} generated_ns={d} speedup={d:.6} tolerance={d:.6} baseline_cpu_max_abs_diff={d:.6} generated_cpu_max_abs_diff={d:.6} generated_baseline_max_abs_diff={d:.6}\n", .{
        in_dim,
        out_dim,
        baseline_ns,
        generated_ns,
        candidate_speedup,
        correctness_tolerance_abs,
        baseline_cpu_max_abs_diff,
        cpu_max_abs_diff,
        baseline_max_abs_diff,
    });
    print("q4_0 pair-q8 summary: speedup={d:.6}\n", .{candidate_speedup});
    if (cfg.quant_compiler_evidence_out_path) |evidence_path| {
        var shape_details = std.ArrayListUnmanaged(u8).empty;
        defer shape_details.deinit(allocator);
        try appendJsonFmt(allocator, &shape_details,
            \\{{"label":"E4B FFN gate+up q8_1","rows":1,"in_dim":{d},"out_dim":{d},"baseline_ns":{d},"generated_ns":{d},"speedup":{d:.6},"quantized_output_tolerance":{d:.6},"baseline_cpu_max_abs_diff":{d:.6},"generated_cpu_max_abs_diff":{d:.6},"generated_baseline_max_abs_diff":{d:.6}}}
        , .{
            in_dim,
            out_dim,
            baseline_ns,
            generated_ns,
            candidate_speedup,
            correctness_tolerance_abs,
            baseline_cpu_max_abs_diff,
            cpu_max_abs_diff,
            baseline_max_abs_diff,
        });
        try writeQuantCompilerQ4_0Evidence(allocator, io, evidence_path, bench, cfg, candidate_speedup, candidate_speedup, shape_details.items);
        print("wrote quant compiler evidence: {s}\n", .{evidence_path});
    }
}

const quant_compiler_q4_0_down_q8_kernel_id = "antfly_q4_0_down_q8_1_mmv_v1";
const quant_compiler_q4_0_down_q8_in_dim: usize = 10240;
const quant_compiler_q4_0_down_q8_out_dim: usize = 2560;

fn benchQuantCompilerQ4_0DownQ8Target(
    allocator: std.mem.Allocator,
    io: std.Io,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    ptx_path: []const u8,
) !void {
    const bench = quant_kernel_compiler.first_q4_0_down_q8_benchmark;
    const in_dim = quant_compiler_q4_0_down_q8_in_dim;
    const out_dim = quant_compiler_q4_0_down_q8_out_dim;
    const row_blocks = in_dim / q4_0_values_per_block;
    const weight_bytes = out_dim * row_blocks * q4_0_block_bytes;
    const baseline_name = bench.handwritten_baseline;
    const correctness_tolerance_abs: f32 = bench.correctness_tolerance_abs;
    if (cfg.quant_compiler_evidence_out_path) |evidence_path| {
        if (!std.mem.eql(u8, ptx_path, bench.generated_ptx_path)) return error.GeneratedPtxPathMismatch;
        if (!std.mem.eql(u8, evidence_path, bench.benchmark_evidence_path)) return error.QuantCompilerEvidencePathMismatch;
    }

    var generated = try GeneratedCandidateModule.load(allocator, io, ctx, ptx_path, bench.generated_kernel_id);
    defer generated.unload(ctx);

    print("CUDA quant compiler q4_0 down-q8 target: rows=1 in={d} out={d} ptx={s}\n", .{ in_dim, out_dim, ptx_path });
    print("candidate kernel={s} baseline kernel={s} production={} tolerance={d:.6}\n", .{ bench.generated_kernel_id, baseline_name, bench.production_enabled, correctness_tolerance_abs });

    const input_host = try allocator.alloc(f32, in_dim);
    defer allocator.free(input_host);
    fillInput(input_host);
    const q8_input_host = try quant_codec.quantizeQ8_1FromF32(allocator, input_host);
    defer allocator.free(q8_input_host);
    const weight_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_host);
    fillQ4_0Weights(weight_host);

    const act_dequant = try allocator.alloc(f32, in_dim);
    defer allocator.free(act_dequant);
    try quant_codec.dequantizeToFloat32(.{ .known = .Q8_1 }, q8_input_host, act_dequant);
    const reference_host = try allocator.alloc(f32, out_dim);
    defer allocator.free(reference_host);
    try quant_kernel_compiler.referenceMatmulNoBias(allocator, .q4_0, weight_host, act_dequant, 1, in_dim, out_dim, reference_host);

    var q8_input = try cuda_buffer.DeviceBuffer.alloc(ctx, q8_input_host.len);
    defer q8_input.free(ctx);
    var weight = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_bytes);
    defer weight.free(ctx);
    var output = try cuda_buffer.DeviceBuffer.alloc(ctx, out_dim * @sizeOf(f32));
    defer output.free(ctx);
    try q8_input.copyFromHost(ctx, q8_input_host);
    try weight.copyFromHost(ctx, weight_host);
    try ctx.synchronize();

    const baseline_ns = try repeatCudaStep(allocator, ctx, cfg, launchQ4_0DownQ8, .{ module.linear_q4_0_q8_1_e4b_down, ctx, output, q8_input, weight, out_dim });
    const baseline_host = try allocator.alloc(f32, out_dim);
    defer allocator.free(baseline_host);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(baseline_host));
    try ctx.synchronize();
    const baseline_cpu_max_abs_diff = try maxAbsDiffFinite(baseline_host, reference_host);
    if (baseline_cpu_max_abs_diff > correctness_tolerance_abs) return error.HandwrittenBaselineMismatch;

    try poisonDeviceOutput(allocator, ctx, output, out_dim);

    const generated_ns = try repeatCudaStep(allocator, ctx, cfg, launchGeneratedQ4_0DownQ8, .{ generated.function, ctx, output, q8_input, weight, 1, in_dim, out_dim });
    const generated_host = try allocator.alloc(f32, out_dim);
    defer allocator.free(generated_host);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(generated_host));
    try ctx.synchronize();
    const cpu_max_abs_diff = try maxAbsDiffFinite(generated_host, reference_host);
    if (cpu_max_abs_diff > correctness_tolerance_abs) return error.GeneratedCandidateMismatch;
    const baseline_max_abs_diff = try maxAbsDiffFinite(generated_host, baseline_host);

    const candidate_speedup = speedup(baseline_ns, generated_ns);
    print("shape=E4B FFN down q8_1 rows=1 in={d} out={d} baseline_ns={d} generated_ns={d} speedup={d:.6} baseline_cpu_max_abs_diff={d:.6} generated_cpu_max_abs_diff={d:.6} generated_baseline_max_abs_diff={d:.6}\n", .{
        in_dim,
        out_dim,
        baseline_ns,
        generated_ns,
        candidate_speedup,
        baseline_cpu_max_abs_diff,
        cpu_max_abs_diff,
        baseline_max_abs_diff,
    });
    print("q4_0 down-q8 summary: speedup={d:.6}\n", .{candidate_speedup});
    if (cfg.quant_compiler_evidence_out_path) |evidence_path| {
        var shape_details = std.ArrayListUnmanaged(u8).empty;
        defer shape_details.deinit(allocator);
        try appendJsonFmt(allocator, &shape_details,
            \\{{"label":"E4B FFN down q8_1","rows":1,"in_dim":{d},"out_dim":{d},"baseline_ns":{d},"generated_ns":{d},"speedup":{d:.6},"baseline_cpu_max_abs_diff":{d:.6},"generated_cpu_max_abs_diff":{d:.6},"generated_baseline_max_abs_diff":{d:.6}}}
        , .{
            in_dim,
            out_dim,
            baseline_ns,
            generated_ns,
            candidate_speedup,
            baseline_cpu_max_abs_diff,
            cpu_max_abs_diff,
            baseline_max_abs_diff,
        });
        try writeQuantCompilerQ4_0Evidence(allocator, io, evidence_path, bench, cfg, candidate_speedup, candidate_speedup, shape_details.items);
        print("wrote quant compiler evidence: {s}\n", .{evidence_path});
    }
}

fn validateQ4_0Q8_1LmArgmaxShape(rows: usize, in_dim: usize, out_dim: usize) cuda_driver.Error!void {
    if (rows != q4_0_q8_1_lm_argmax_e2b_rows or
        in_dim != q4_0_q8_1_lm_argmax_e2b_in_dim or
        out_dim != q4_0_q8_1_lm_argmax_e2b_out_dim)
    {
        return error.InvalidCudaState;
    }
}

fn launchQ4_0Q8_1LmQuantize(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    dst_q8_1: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
) cuda_driver.Error!void {
    const row_blocks = in_dim / q8_1_values_per_block;
    const total_blocks = try checkedMul(rows, row_blocks);
    try checkRawBytes(dst_q8_1, try checkedMul(total_blocks, q8_1_block_bytes));
    try checkF32Bytes(input, try checkedMul(rows, in_dim));
    var dst_ptr = dst_q8_1.ptr;
    var input_ptr = input.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
    };
    try launchBlocks(ctx, module.quantize_f32_q8_1_rows, (total_blocks + 3) / 4, 128, &params);
}

fn launchQ4_0Q8_1LmArgmaxBaseline(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    token: cuda_buffer.DeviceBuffer,
    logits: cuda_buffer.DeviceBuffer,
    input_q8_1: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4_0Q8_1LmArgmaxShape(rows, in_dim, out_dim);
    const row_blocks = in_dim / q4_0_values_per_block;
    try checkRawBytes(token, try checkedMul(rows, @sizeOf(u32)));
    try checkF32Bytes(logits, try checkedMul(rows, out_dim));
    try checkRawBytes(input_q8_1, try checkedMul(try checkedMul(rows, row_blocks), q8_1_block_bytes));
    try checkF32Bytes(input, try checkedMul(rows, in_dim));
    try checkRawBytes(weight, try checkedMul(try checkedMul(out_dim, row_blocks), q4_0_block_bytes));
    try launchQ4_0Q8_1LmQuantize(module, ctx, input_q8_1, input, rows, in_dim);

    var logits_ptr = logits.ptr;
    var input_ptr = input_q8_1.ptr;
    var weight_ptr = weight.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var linear_params = [_]?*anyopaque{
        @ptrCast(&logits_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch2d(ctx, module.linear_q4_0_q8_1_f32_tile4, (out_dim + 3) / 4, rows, 128, &linear_params);

    var token_ptr = token.ptr;
    var argmax_params = [_]?*anyopaque{
        @ptrCast(&token_ptr),
        @ptrCast(&logits_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&out_dim_u32),
    };
    try launchBlocks(ctx, module.argmax_last_row_f32, 1, 256, &argmax_params);
}

fn launchQ4_0Q8_1LmArgmaxCandidate(
    candidate_function: cuda_driver.CUfunction,
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    token: cuda_buffer.DeviceBuffer,
    partial_values: cuda_buffer.DeviceBuffer,
    partial_indices: cuda_buffer.DeviceBuffer,
    input_q8_1: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4_0Q8_1LmArgmaxShape(rows, in_dim, out_dim);
    const row_blocks = in_dim / q4_0_values_per_block;
    const col_tiles = (out_dim + q4_0_q8_1_lm_argmax_tile8 - 1) / q4_0_q8_1_lm_argmax_tile8;
    const partial_count = try checkedMul(rows, col_tiles);
    try checkRawBytes(token, try checkedMul(rows, @sizeOf(u32)));
    try checkF32Bytes(partial_values, partial_count);
    try checkRawBytes(partial_indices, try checkedMul(partial_count, @sizeOf(u32)));
    try checkRawBytes(input_q8_1, try checkedMul(try checkedMul(rows, row_blocks), q8_1_block_bytes));
    try checkF32Bytes(input, try checkedMul(rows, in_dim));
    try checkRawBytes(weight, try checkedMul(try checkedMul(out_dim, row_blocks), q4_0_block_bytes));
    try launchQ4_0Q8_1LmQuantize(module, ctx, input_q8_1, input, rows, in_dim);

    var partial_values_ptr = partial_values.ptr;
    var partial_indices_ptr = partial_indices.ptr;
    var input_ptr = input_q8_1.ptr;
    var weight_ptr = weight.ptr;
    var suppress_ptr: cuda_driver.CUdeviceptr = 0;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var suppress_count_u32: u32 = 0;
    var stage_params = [_]?*anyopaque{
        @ptrCast(&partial_values_ptr),
        @ptrCast(&partial_indices_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&suppress_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
        @ptrCast(&suppress_count_u32),
    };
    try launchBlocks(ctx, candidate_function, partial_count, q4_0_q8_1_lm_argmax_stage_threads, &stage_params);

    var token_ptr = token.ptr;
    var col_tiles_u32 = try toU32(col_tiles);
    var reduce_params = [_]?*anyopaque{
        @ptrCast(&token_ptr),
        @ptrCast(&partial_values_ptr),
        @ptrCast(&partial_indices_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&col_tiles_u32),
    };
    try launchBlocks(ctx, module.argmax_reduce_rows_pairs_f32_w16, rows, q4_0_q8_1_lm_argmax_reduce_threads, &reduce_params);
}

fn validateQ6KQ8_1LmArgmaxVariant(variant: Q6KQ8_1LmArgmaxVariant) !void {
    if (variant.in_dim != 2560 and variant.in_dim != 3840) return error.InvalidQ6KQ8_1LmArgmaxVariant;
    const expected_candidate_threads: usize = if (variant.in_dim == 2560) 160 else 256;
    if (variant.candidate_threads != expected_candidate_threads) return error.InvalidQ6KQ8_1LmArgmaxVariant;
    if (variant.candidate_symbol.len == 0) return error.InvalidQ6KQ8_1LmArgmaxVariant;
    const expected_baseline: Q6KQ8_1LmArgmaxBaseline = if (variant.in_dim == 2560) .e4b else .generic;
    if (variant.baseline != expected_baseline) return error.InvalidQ6KQ8_1LmArgmaxVariant;
}

fn validateQ6KQ8_1LmArgmaxShape(rows: usize, in_dim: usize, out_dim: usize) cuda_driver.Error!void {
    if (rows != q6_k_q8_1_lm_argmax_rows or
        (in_dim != 2560 and in_dim != 3840) or
        out_dim != q6_k_q8_1_lm_argmax_out_dim)
    {
        return error.InvalidCudaState;
    }
}

fn launchQ6KQ8_1LmArgmaxBaseline(
    baseline_function: cuda_driver.CUfunction,
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    token: cuda_buffer.DeviceBuffer,
    partial_values: cuda_buffer.DeviceBuffer,
    partial_indices: cuda_buffer.DeviceBuffer,
    input_q8_1: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    stage_threads: usize,
) cuda_driver.Error!void {
    try validateQ6KQ8_1LmArgmaxShape(rows, in_dim, out_dim);
    const row_blocks = in_dim / q6_k_values_per_block;
    const q8_blocks = in_dim / q8_1_values_per_block;
    const col_tiles = (out_dim + q6_k_q8_1_lm_argmax_tile8 - 1) / q6_k_q8_1_lm_argmax_tile8;
    const partial_count = try checkedMul(rows, col_tiles);
    try checkRawBytes(token, try checkedMul(rows, @sizeOf(u32)));
    try checkF32Bytes(partial_values, partial_count);
    try checkRawBytes(partial_indices, try checkedMul(partial_count, @sizeOf(u32)));
    try checkRawBytes(input_q8_1, try checkedMul(try checkedMul(rows, q8_blocks), q8_1_block_bytes));
    try checkF32Bytes(input, try checkedMul(rows, in_dim));
    try checkRawBytes(weight, try checkedMul(try checkedMul(out_dim, row_blocks), q6_k_block_bytes));
    try launchQ4_0Q8_1LmQuantize(module, ctx, input_q8_1, input, rows, in_dim);

    var partial_values_ptr = partial_values.ptr;
    var partial_indices_ptr = partial_indices.ptr;
    var input_ptr = input_q8_1.ptr;
    var weight_ptr = weight.ptr;
    var suppress_ptr: cuda_driver.CUdeviceptr = 0;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var suppress_count_u32: u32 = 0;
    var stage_params = [_]?*anyopaque{
        @ptrCast(&partial_values_ptr),
        @ptrCast(&partial_indices_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&suppress_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
        @ptrCast(&suppress_count_u32),
    };
    try launchBlocks(ctx, baseline_function, partial_count, stage_threads, &stage_params);

    var token_ptr = token.ptr;
    var col_tiles_u32 = try toU32(col_tiles);
    var reduce_params = [_]?*anyopaque{
        @ptrCast(&token_ptr),
        @ptrCast(&partial_values_ptr),
        @ptrCast(&partial_indices_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&col_tiles_u32),
    };
    try launchBlocks(ctx, module.argmax_reduce_rows_pairs_f32_w16, rows, q6_k_q8_1_lm_argmax_reduce_threads, &reduce_params);
}

fn launchQ6KQ8_1LmArgmaxCandidate(
    candidate_function: cuda_driver.CUfunction,
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    token: cuda_buffer.DeviceBuffer,
    partial_values: cuda_buffer.DeviceBuffer,
    partial_indices: cuda_buffer.DeviceBuffer,
    input_q8_1: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    stage_threads: usize,
) cuda_driver.Error!void {
    try validateQ6KQ8_1LmArgmaxShape(rows, in_dim, out_dim);
    const expected_stage_threads: usize = if (in_dim == 2560) 160 else 256;
    if (stage_threads != expected_stage_threads) return error.InvalidCudaState;
    const row_blocks = in_dim / q6_k_values_per_block;
    const q8_blocks = in_dim / q8_1_values_per_block;
    const col_tiles = (out_dim + q6_k_q8_1_lm_argmax_tile8 - 1) / q6_k_q8_1_lm_argmax_tile8;
    const partial_count = try checkedMul(rows, col_tiles);
    try checkRawBytes(token, try checkedMul(rows, @sizeOf(u32)));
    try checkF32Bytes(partial_values, partial_count);
    try checkRawBytes(partial_indices, try checkedMul(partial_count, @sizeOf(u32)));
    try checkRawBytes(input_q8_1, try checkedMul(try checkedMul(rows, q8_blocks), q8_1_block_bytes));
    try checkF32Bytes(input, try checkedMul(rows, in_dim));
    try checkRawBytes(weight, try checkedMul(try checkedMul(out_dim, row_blocks), q6_k_block_bytes));
    try launchQ4_0Q8_1LmQuantize(module, ctx, input_q8_1, input, rows, in_dim);

    var partial_values_ptr = partial_values.ptr;
    var partial_indices_ptr = partial_indices.ptr;
    var input_ptr = input_q8_1.ptr;
    var weight_ptr = weight.ptr;
    var suppress_ptr: cuda_driver.CUdeviceptr = 0;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var suppress_count_u32: u32 = 0;
    var stage_params = [_]?*anyopaque{
        @ptrCast(&partial_values_ptr),
        @ptrCast(&partial_indices_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&suppress_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
        @ptrCast(&suppress_count_u32),
    };
    try launchBlocks(ctx, candidate_function, partial_count, stage_threads, &stage_params);

    var token_ptr = token.ptr;
    var col_tiles_u32 = try toU32(col_tiles);
    var reduce_params = [_]?*anyopaque{
        @ptrCast(&token_ptr),
        @ptrCast(&partial_values_ptr),
        @ptrCast(&partial_indices_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&col_tiles_u32),
    };
    try launchBlocks(ctx, module.argmax_reduce_rows_pairs_f32_w16, rows, q6_k_q8_1_lm_argmax_reduce_threads, &reduce_params);
}

fn validateQ4_0Q8_1E2BFfnVariant(variant: E2BFfnVariant) !void {
    if (variant.inner_dim != 6144 and variant.inner_dim != 12288) return error.InvalidE2BFfnVariant;
    if (variant.pair.threads != q4_0_q8_1_e2b_ffn_pair_threads) return error.InvalidE2BFfnVariant;
    if (variant.pair.cols_per_block != 32) return error.InvalidE2BFfnVariant;
    if (variant.down.threads != q4_0_q8_1_e2b_ffn_ggml_down_threads or variant.down.cols_per_block != 1) return error.InvalidE2BFfnVariant;
    if (variant.pair.kernel_id.len == 0 or variant.down.kernel_id.len == 0) return error.InvalidE2BFfnVariant;
    if (!std.mem.eql(u8, variant.pair.artifact_filename, q4_0_q8_1_e2b_ffn_artifact_filename) or
        !std.mem.eql(u8, variant.down.artifact_filename, q4_0_q8_1_e2b_ffn_artifact_filename)) return error.InvalidE2BFfnVariant;
    if (variant.pair.production_enabled or variant.down.production_enabled) return error.InvalidE2BFfnVariant;
}

fn validateQ4_0Q8_1E2BFfnShape(rows: usize, hidden_dim: usize, inner_dim: usize) cuda_driver.Error!void {
    if (rows != q4_0_q8_1_e2b_ffn_rows or hidden_dim != q4_0_q8_1_e2b_ffn_hidden_dim or
        (inner_dim != 6144 and inner_dim != 12288)) return error.InvalidCudaState;
}

fn launchGgmlQ8_1QuantizeRows(
    function: cuda_driver.CUfunction,
    ctx: *cuda_context.CudaContext,
    dst_q8: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    rows: usize,
    dim: usize,
) cuda_driver.Error!void {
    if (rows == 0 or dim == 0 or dim % q8_1_values_per_block != 0) return error.InvalidCudaState;
    const block_count = try checkedMul(rows, dim / q8_1_values_per_block);
    try checkRawBytes(dst_q8, try checkedMul(block_count, q8_1_block_bytes));
    try checkF32Bytes(input, try checkedMul(rows, dim));
    var dst_ptr = dst_q8.ptr;
    var input_ptr = input.ptr;
    var rows_u32 = try toU32(rows);
    var dim_u32 = try toU32(dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr), @ptrCast(&input_ptr), @ptrCast(&rows_u32), @ptrCast(&dim_u32),
    };
    try launchBlocks(ctx, function, block_count, 32, &params);
}

fn launchQ4_0Q8_1E2BFfnPairBaseline(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    pair_f32: cuda_buffer.DeviceBuffer,
    dst_q8: cuda_buffer.DeviceBuffer,
    hidden_q8: cuda_buffer.DeviceBuffer,
    gate_weight: cuda_buffer.DeviceBuffer,
    up_weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    hidden_dim: usize,
    inner_dim: usize,
) cuda_driver.Error!void {
    try validateQ4_0Q8_1E2BFfnShape(rows, hidden_dim, inner_dim);
    const hidden_blocks = hidden_dim / q8_1_values_per_block;
    const inner_blocks = inner_dim / q8_1_values_per_block;
    const pair_weight_bytes = try checkedMul(try checkedMul(inner_dim, hidden_blocks), q4_0_block_bytes);
    try checkF32Bytes(pair_f32, try checkedMul(rows, inner_dim));
    try checkRawBytes(dst_q8, try checkedMul(try checkedMul(rows, inner_blocks), q8_1_block_bytes));
    try checkRawBytes(hidden_q8, try checkedMul(try checkedMul(rows, hidden_blocks), q8_1_block_bytes));
    try checkRawBytes(gate_weight, pair_weight_bytes);
    try checkRawBytes(up_weight, pair_weight_bytes);

    var dst_ptr = pair_f32.ptr;
    var input_ptr = hidden_q8.ptr;
    var gate_weight_ptr = gate_weight.ptr;
    var up_weight_ptr = up_weight.ptr;
    var rows_u32 = try toU32(rows);
    var hidden_dim_u32 = try toU32(hidden_dim);
    var inner_dim_u32 = try toU32(inner_dim);
    var activation_u32: u32 = q4_0_q8_1_e2b_ffn_activation;
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&gate_weight_ptr),
        @ptrCast(&up_weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&hidden_dim_u32),
        @ptrCast(&inner_dim_u32),
        @ptrCast(&activation_u32),
    };
    try launchBlocks(ctx, module.linear_q4_0_pair_activation_q8_1_f32_tile4, try checkedMul(rows, (inner_dim + 3) / 4), 128, &params);
    try launchQ4_0Q8_1LmQuantize(module, ctx, dst_q8, pair_f32, rows, inner_dim);
}

fn launchQ4_0Q8_1E2BFfnPairCandidate(
    function: cuda_driver.CUfunction,
    ctx: *cuda_context.CudaContext,
    dst_q8: cuda_buffer.DeviceBuffer,
    hidden_q8: cuda_buffer.DeviceBuffer,
    gate_weight: cuda_buffer.DeviceBuffer,
    up_weight: cuda_buffer.DeviceBuffer,
    variant: E2BFfnVariant,
) cuda_driver.Error!void {
    const rows = q4_0_q8_1_e2b_ffn_rows;
    const hidden_dim = q4_0_q8_1_e2b_ffn_hidden_dim;
    const inner_dim = variant.inner_dim;
    try validateQ4_0Q8_1E2BFfnShape(rows, hidden_dim, inner_dim);
    const hidden_blocks = hidden_dim / q8_1_values_per_block;
    const inner_blocks = inner_dim / q8_1_values_per_block;
    const pair_weight_bytes = try checkedMul(try checkedMul(inner_dim, hidden_blocks), q4_0_block_bytes);
    try checkRawBytes(dst_q8, try checkedMul(try checkedMul(rows, inner_blocks), q8_1_block_bytes));
    try checkRawBytes(hidden_q8, try checkedMul(try checkedMul(rows, hidden_blocks), q8_1_block_bytes));
    try checkRawBytes(gate_weight, pair_weight_bytes);
    try checkRawBytes(up_weight, pair_weight_bytes);

    var dst_ptr = dst_q8.ptr;
    var input_ptr = hidden_q8.ptr;
    var gate_weight_ptr = gate_weight.ptr;
    var up_weight_ptr = up_weight.ptr;
    var activation_u32: u32 = q4_0_q8_1_e2b_ffn_activation;
    var rows_u32 = try toU32(rows);
    var hidden_dim_u32 = try toU32(hidden_dim);
    var inner_dim_u32 = try toU32(inner_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&gate_weight_ptr),
        @ptrCast(&up_weight_ptr),
        @ptrCast(&activation_u32),
        @ptrCast(&rows_u32),
        @ptrCast(&hidden_dim_u32),
        @ptrCast(&inner_dim_u32),
    };
    try launchBlocks(ctx, function, try checkedMul(rows, inner_blocks), variant.pair.threads, &params);
}

fn launchQ4_0Q8_1E2BFfnDownBaseline(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    activated_q8: cuda_buffer.DeviceBuffer,
    down_weight: cuda_buffer.DeviceBuffer,
    variant: E2BFfnVariant,
) cuda_driver.Error!void {
    const rows = q4_0_q8_1_e2b_ffn_rows;
    const hidden_dim = q4_0_q8_1_e2b_ffn_hidden_dim;
    const inner_dim = variant.inner_dim;
    try validateQ4_0Q8_1E2BFfnShape(rows, hidden_dim, inner_dim);
    const inner_blocks = inner_dim / q8_1_values_per_block;
    try checkF32Bytes(output, try checkedMul(rows, hidden_dim));
    try checkRawBytes(activated_q8, try checkedMul(try checkedMul(rows, inner_blocks), q8_1_block_bytes));
    try checkRawBytes(down_weight, try checkedMul(try checkedMul(hidden_dim, inner_blocks), q4_0_block_bytes));

    const function = module.linear_q4_0_q8_1_f32_tile4_w8;
    var dst_ptr = output.ptr;
    var input_ptr = activated_q8.ptr;
    var weight_ptr = down_weight.ptr;
    var rows_u32 = try toU32(rows);
    var inner_dim_u32 = try toU32(inner_dim);
    var hidden_dim_u32 = try toU32(hidden_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&inner_dim_u32),
        @ptrCast(&hidden_dim_u32),
    };
    try launch2d(ctx, function, (hidden_dim + 3) / 4, rows, q4_0_q8_1_e2b_ffn_production_down_threads, &params);
}

fn launchQ4_0Q8_1E2BFfnDownCandidate(
    function: cuda_driver.CUfunction,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    activated_q8: cuda_buffer.DeviceBuffer,
    down_weight: cuda_buffer.DeviceBuffer,
    variant: E2BFfnVariant,
) cuda_driver.Error!void {
    const rows = q4_0_q8_1_e2b_ffn_rows;
    const hidden_dim = q4_0_q8_1_e2b_ffn_hidden_dim;
    const inner_dim = variant.inner_dim;
    try validateQ4_0Q8_1E2BFfnShape(rows, hidden_dim, inner_dim);
    const inner_blocks = inner_dim / q8_1_values_per_block;
    try checkF32Bytes(output, try checkedMul(rows, hidden_dim));
    try checkRawBytes(activated_q8, try checkedMul(try checkedMul(rows, inner_blocks), q8_1_block_bytes));
    try checkRawBytes(down_weight, try checkedMul(try checkedMul(hidden_dim, inner_blocks), q4_0_block_bytes));

    var dst_ptr = output.ptr;
    var input_ptr = activated_q8.ptr;
    var weight_ptr = down_weight.ptr;
    var rows_u32 = try toU32(rows);
    var inner_dim_u32 = try toU32(inner_dim);
    var hidden_dim_u32 = try toU32(hidden_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&inner_dim_u32),
        @ptrCast(&hidden_dim_u32),
    };
    try launchBlocks(
        ctx,
        function,
        try checkedMul(rows, (hidden_dim + variant.down.cols_per_block - 1) / variant.down.cols_per_block),
        variant.down.threads,
        &params,
    );
}

fn launchQ4_0Q8_1E2BFfnChainBaseline(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    pair_f32: cuda_buffer.DeviceBuffer,
    activated_q8: cuda_buffer.DeviceBuffer,
    hidden_q8: cuda_buffer.DeviceBuffer,
    hidden: cuda_buffer.DeviceBuffer,
    gate_weight: cuda_buffer.DeviceBuffer,
    up_weight: cuda_buffer.DeviceBuffer,
    down_weight: cuda_buffer.DeviceBuffer,
    variant: E2BFfnVariant,
) cuda_driver.Error!void {
    try launchQ4_0Q8_1LmQuantize(module, ctx, hidden_q8, hidden, q4_0_q8_1_e2b_ffn_rows, q4_0_q8_1_e2b_ffn_hidden_dim);
    try launchQ4_0Q8_1E2BFfnPairBaseline(module, ctx, pair_f32, activated_q8, hidden_q8, gate_weight, up_weight, q4_0_q8_1_e2b_ffn_rows, q4_0_q8_1_e2b_ffn_hidden_dim, variant.inner_dim);
    try launchQ4_0Q8_1E2BFfnDownBaseline(module, ctx, output, activated_q8, down_weight, variant);
}

fn launchQ4_0Q8_1E2BFfnChainCandidate(
    quantize_function: cuda_driver.CUfunction,
    pair_function: cuda_driver.CUfunction,
    down_function: cuda_driver.CUfunction,
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    activated_q8: cuda_buffer.DeviceBuffer,
    hidden_q8: cuda_buffer.DeviceBuffer,
    hidden: cuda_buffer.DeviceBuffer,
    gate_weight: cuda_buffer.DeviceBuffer,
    up_weight: cuda_buffer.DeviceBuffer,
    down_weight: cuda_buffer.DeviceBuffer,
    variant: E2BFfnVariant,
) cuda_driver.Error!void {
    _ = module;
    try launchGgmlQ8_1QuantizeRows(quantize_function, ctx, hidden_q8, hidden, q4_0_q8_1_e2b_ffn_rows, q4_0_q8_1_e2b_ffn_hidden_dim);
    try launchQ4_0Q8_1E2BFfnPairCandidate(pair_function, ctx, activated_q8, hidden_q8, gate_weight, up_weight, variant);
    try launchQ4_0Q8_1E2BFfnDownCandidate(down_function, ctx, output, activated_q8, down_weight, variant);
}

const E2BFfnChainBaselineRotatingState = struct {
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    outputs: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    pair_f32: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    activated_q8: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    hidden_q8: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    hidden: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    gate_weights: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    up_weights: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    down_weights: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    variant: E2BFfnVariant,
    cursor: usize = 0,
};

fn launchQ4_0Q8_1E2BFfnChainBaselineRotating(state: *E2BFfnChainBaselineRotatingState) cuda_driver.Error!void {
    const index = state.cursor;
    state.cursor = (state.cursor + 1) % q4_0_q8_1_e2b_ffn_weight_ring;
    return launchQ4_0Q8_1E2BFfnChainBaseline(
        state.module,
        state.ctx,
        state.outputs[index],
        state.pair_f32[index],
        state.activated_q8[index],
        state.hidden_q8[index],
        state.hidden[index],
        state.gate_weights[index],
        state.up_weights[index],
        state.down_weights[index],
        state.variant,
    );
}

const E2BFfnChainCandidateRotatingState = struct {
    quantize_function: cuda_driver.CUfunction,
    pair_function: cuda_driver.CUfunction,
    down_function: cuda_driver.CUfunction,
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    outputs: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    activated_q8: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    hidden_q8: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    hidden: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    gate_weights: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    up_weights: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    down_weights: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer,
    variant: E2BFfnVariant,
    cursor: usize = 0,
};

fn launchQ4_0Q8_1E2BFfnChainCandidateRotating(state: *E2BFfnChainCandidateRotatingState) cuda_driver.Error!void {
    const index = state.cursor;
    state.cursor = (state.cursor + 1) % q4_0_q8_1_e2b_ffn_weight_ring;
    return launchQ4_0Q8_1E2BFfnChainCandidate(
        state.quantize_function,
        state.pair_function,
        state.down_function,
        state.module,
        state.ctx,
        state.outputs[index],
        state.activated_q8[index],
        state.hidden_q8[index],
        state.hidden[index],
        state.gate_weights[index],
        state.up_weights[index],
        state.down_weights[index],
        state.variant,
    );
}

fn deviceBufferRing(
    storage: cuda_buffer.DeviceBuffer,
    item_bytes: usize,
) cuda_driver.Error![q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer {
    if (storage.len < try checkedMul(item_bytes, q4_0_q8_1_e2b_ffn_weight_ring)) return error.InvalidCudaState;
    var ring: [q4_0_q8_1_e2b_ffn_weight_ring]cuda_buffer.DeviceBuffer = undefined;
    for (&ring, 0..) |*item, index| {
        item.* = .{
            .ptr = storage.ptr + @as(u64, @intCast(index * item_bytes)),
            .len = item_bytes,
        };
    }
    return ring;
}

fn firstByteMismatch(expected: []const u8, actual: []const u8) !?usize {
    if (expected.len != actual.len) return error.InvalidArgument;
    for (expected, actual, 0..) |expected_byte, actual_byte, index| {
        if (expected_byte != actual_byte) return index;
    }
    return null;
}

fn firstQ8PayloadMismatchIgnoringGgmlSum(expected: []const u8, actual: []const u8) !?usize {
    if (expected.len != actual.len or expected.len % q8_1_block_bytes != 0) return error.InvalidArgument;
    for (0..expected.len / q8_1_block_bytes) |block| {
        const base = block * q8_1_block_bytes;
        for (0..q8_1_block_bytes) |offset| {
            if (offset == 2 or offset == 3) continue;
            if (expected[base + offset] != actual[base + offset]) return base + offset;
        }
    }
    return null;
}

fn halfLeToF32(bytes: []const u8) !f32 {
    if (bytes.len < 2) return error.InvalidArgument;
    const bits = @as(u16, bytes[0]) | (@as(u16, bytes[1]) << 8);
    const value: f16 = @bitCast(bits);
    return @floatCast(value);
}

fn maxGgmlQ8_1StoredSumDiff(q8: []const u8, values: []const f32) !f32 {
    if (values.len % q8_1_values_per_block != 0 or
        q8.len != (values.len / q8_1_values_per_block) * q8_1_block_bytes) return error.InvalidArgument;
    var max_diff: f32 = 0;
    for (0..values.len / q8_1_values_per_block) |block| {
        var expected_sum: f32 = 0;
        for (values[block * q8_1_values_per_block ..][0..q8_1_values_per_block]) |value| expected_sum += value;
        const base = block * q8_1_block_bytes;
        const stored_sum = try halfLeToF32(q8[base + 2 .. base + 4]);
        max_diff = @max(max_diff, @abs(stored_sum - expected_sum));
    }
    return max_diff;
}

fn timeAlternatingCudaSteps(
    ctx: *cuda_context.CudaContext,
    cfg: Config,
    comptime baseline_step: anytype,
    baseline_args: anytype,
    comptime candidate_step: anytype,
    candidate_args: anytype,
    baseline_runs: []u64,
    candidate_runs: []u64,
) !void {
    if (baseline_runs.len == 0 or baseline_runs.len != candidate_runs.len or baseline_runs.len > 31) return error.InvalidTimingRuns;
    for (baseline_runs, candidate_runs, 0..) |*baseline_run, *candidate_run, run| {
        if (run % 2 == 0) {
            baseline_run.* = try timeCudaStep(ctx, cfg, baseline_step, baseline_args);
            candidate_run.* = try timeCudaStep(ctx, cfg, candidate_step, candidate_args);
        } else {
            candidate_run.* = try timeCudaStep(ctx, cfg, candidate_step, candidate_args);
            baseline_run.* = try timeCudaStep(ctx, cfg, baseline_step, baseline_args);
        }
    }
}

fn launchQ4_0DownQ8(
    function: cuda_driver.CUfunction,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    q8_input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    out_dim: usize,
) cuda_driver.Error!void {
    var dst_ptr = output.ptr;
    var input_ptr = q8_input.ptr;
    var weight_ptr = weight.ptr;
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
    };
    try launchBlocks(ctx, function, out_dim / 4, 256, &params);
}

fn launchGeneratedQ4_0DownQ8(
    function: cuda_driver.CUfunction,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    q8_input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    var dst_ptr = output.ptr;
    var input_ptr = q8_input.ptr;
    var weight_ptr = weight.ptr;
    var rows_u32: u32 = @intCast(rows);
    var in_dim_u32: u32 = @intCast(in_dim);
    var out_dim_u32: u32 = @intCast(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launchBlocks(ctx, function, rows * ((out_dim + 3) / 4), 256, &params);
}

fn launchQ4_0PairQ8(
    function: cuda_driver.CUfunction,
    ctx: *cuda_context.CudaContext,
    dst_q8: cuda_buffer.DeviceBuffer,
    q8_input: cuda_buffer.DeviceBuffer,
    weight_gate: cuda_buffer.DeviceBuffer,
    weight_up: cuda_buffer.DeviceBuffer,
    out_blocks: usize,
) cuda_driver.Error!void {
    var dst_ptr = dst_q8.ptr;
    var input_ptr = q8_input.ptr;
    var weight_gate_ptr = weight_gate.ptr;
    var weight_up_ptr = weight_up.ptr;
    var activation_u32: u32 = quant_compiler_q4_0_pair_q8_activation;
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_gate_ptr),
        @ptrCast(&weight_up_ptr),
        @ptrCast(&activation_u32),
    };
    try launchBlocks(ctx, function, out_blocks, 640, &params);
}

fn launchGeneratedQ4_0PairQ8(
    function: cuda_driver.CUfunction,
    ctx: *cuda_context.CudaContext,
    dst_q8: cuda_buffer.DeviceBuffer,
    q8_input: cuda_buffer.DeviceBuffer,
    weight_gate: cuda_buffer.DeviceBuffer,
    weight_up: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    out_blocks: usize,
) cuda_driver.Error!void {
    var dst_ptr = dst_q8.ptr;
    var input_ptr = q8_input.ptr;
    var weight_gate_ptr = weight_gate.ptr;
    var weight_up_ptr = weight_up.ptr;
    var activation_u32: u32 = quant_compiler_q4_0_pair_q8_activation;
    var rows_u32: u32 = @intCast(rows);
    var in_dim_u32: u32 = @intCast(in_dim);
    var out_dim_u32: u32 = @intCast(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_gate_ptr),
        @ptrCast(&weight_up_ptr),
        @ptrCast(&activation_u32),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launchBlocks(ctx, function, rows * out_blocks, 640, &params);
}

fn poisonDeviceBytes(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    buffer: cuda_buffer.DeviceBuffer,
    byte_count: usize,
) !void {
    const poison = try allocator.alloc(u8, byte_count);
    defer allocator.free(poison);
    @memset(poison, 0x7f);
    try buffer.copyFromHost(ctx, poison);
    try ctx.synchronize();
}

fn launchQ4_0PairBaseline(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output_a: cuda_buffer.DeviceBuffer,
    output_b: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight_a: cuda_buffer.DeviceBuffer,
    weight_b: cuda_buffer.DeviceBuffer,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4_0Buffers(output_a, input, weight_a, 1, in_dim, out_dim);
    try validateQ4_0Buffers(output_b, input, weight_b, 1, in_dim, out_dim);
    var dst_a_ptr = output_a.ptr;
    var dst_b_ptr = output_b.ptr;
    var input_ptr = input.ptr;
    var weight_a_ptr = weight_a.ptr;
    var weight_b_ptr = weight_b.ptr;
    var rows_u32: u32 = 1;
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_a_ptr),
        @ptrCast(&dst_b_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_a_ptr),
        @ptrCast(&weight_b_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    const total_tiles = try checkedMul((out_dim + 3) / 4, 2);
    try launchBlocks(ctx, module.linear_q4_0_pair_nobias_f32_tile4_w4, total_tiles, 128, &params);
}

fn launchGeneratedQ4_0Pair(
    module: *GeneratedCandidateModule,
    ctx: *cuda_context.CudaContext,
    output_a: cuda_buffer.DeviceBuffer,
    output_b: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight_a: cuda_buffer.DeviceBuffer,
    weight_b: cuda_buffer.DeviceBuffer,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4_0Buffers(output_a, input, weight_a, 1, in_dim, out_dim);
    try validateQ4_0Buffers(output_b, input, weight_b, 1, in_dim, out_dim);
    var input_ptr = input.ptr;
    var weight_a_ptr = weight_a.ptr;
    var weight_b_ptr = weight_b.ptr;
    var dst_a_ptr = output_a.ptr;
    var dst_b_ptr = output_b.ptr;
    var rows_u32: u32 = 1;
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&input_ptr),
        @ptrCast(&weight_a_ptr),
        @ptrCast(&weight_b_ptr),
        @ptrCast(&dst_a_ptr),
        @ptrCast(&dst_b_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch2d(ctx, module.function, (out_dim + 3) / 4, 1, 256, &params);
}

fn writeQuantCompilerQ4_0Evidence(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    bench: quant_kernel_compiler.BenchmarkCase,
    cfg: Config,
    geomean_speedup: f64,
    worst_speedup: f64,
    shape_details_json: []const u8,
) !void {
    const benchmark_passed = std.math.isFinite(geomean_speedup) and std.math.isFinite(worst_speedup) and geomean_speedup >= bench.minimum_speedup and worst_speedup >= bench.minimum_speedup;
    const runtime_shape = (quant_kernel_compiler.generatedArtifactForKernel(.cuda, bench.generated_kernel_id) orelse return error.MissingGeneratedArtifact).runtime_shape;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    try appendJsonFmt(allocator, &out,
        \\{{
        \\"schema":"antfly.quant_kernel_q4_0_benchmark_evidence.v1",
        \\"kernel_id":{f},
        \\"generated_source_path":{f},
        \\"generated_source_fingerprint":{d},
        \\"generated_ptx_path":{f},
        \\"generated_ptx_command":{f},
        \\"benchmark_command":{f},
        \\"correctness_evidence_path":{f},
        \\"benchmark_evidence_path":{f},
        \\"benchmark_mode":"sequential",
        \\"repeat_runs":{d},
        \\"runtime_min_in_dim":{d},
        \\"timing_aggregation":"median",
        \\"production_enabled":{},
        \\"correctness_passed":true,
        \\"benchmark_passed":{},
        \\"promotion_ready":{},
        \\"measured_speedup":{d:.6},
        \\"worst_shape_speedup":{d:.6},
        \\"minimum_speedup":{d:.6},
        \\"correctness_tolerance_abs":{d:.6},
        \\"baseline_kernel":{f},
        \\"warmup_iters":{d},
        \\"measure_iters":{d},
        \\"shapes":[{s}]
        \\}}
        \\
    , .{
        std.json.fmt(bench.generated_kernel_id, .{}),
        std.json.fmt(bench.generated_source_path, .{}),
        bench.generated_source_fingerprint,
        std.json.fmt(bench.generated_ptx_path, .{}),
        std.json.fmt(bench.generated_ptx_command, .{}),
        std.json.fmt(bench.benchmark_command, .{}),
        std.json.fmt(bench.correctness_evidence_path, .{}),
        std.json.fmt(bench.benchmark_evidence_path, .{}),
        cfg.quant_compiler_repeat_runs,
        runtime_shape.min_in_dim,
        bench.production_enabled,
        benchmark_passed,
        benchmark_passed,
        geomean_speedup,
        worst_speedup,
        bench.minimum_speedup,
        bench.correctness_tolerance_abs,
        std.json.fmt(bench.handwritten_baseline, .{}),
        cfg.warmup_iters,
        cfg.measure_iters,
        shape_details_json,
    });
    try writeFileCreatingParent(io, path, out.items);
}

fn launchQ4_0Baseline(
    function: cuda_driver.CUfunction,
    ctx: *cuda_context.CudaContext,
    kind: QuantCompilerQ4_0Kind,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4_0Buffers(output, input, weight, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    switch (kind) {
        .mmv => try launch2d(ctx, function, (out_dim + 3) / 4, rows, 256, &params),
        .mm => try launch1d(ctx, function, try checkedMul(rows, out_dim), &params),
    }
}

fn launchGeneratedQ4_0(
    module: *GeneratedCandidateModule,
    ctx: *cuda_context.CudaContext,
    kind: QuantCompilerQ4_0Kind,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4_0Buffers(output, input, weight, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&dst_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    switch (kind) {
        .mmv => try launch2d(ctx, module.function, (out_dim + 3) / 4, 1, 256, &params),
        .mm => try launch2d(ctx, module.function, (out_dim + 3) / 4, (rows + 7) / 8, 256, &params),
    }
}

fn validateQ4_0Buffers(
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    if (in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
    try checkF32Bytes(output, try checkedMul(rows, out_dim));
    try checkF32Bytes(input, try checkedMul(rows, in_dim));
    const row_blocks = in_dim / q4_0_values_per_block;
    try checkRawBytes(weight, try checkedMul(try checkedMul(out_dim, row_blocks), q4_0_block_bytes));
}

fn timeCudaStep(
    ctx: *cuda_context.CudaContext,
    cfg: Config,
    comptime step: anytype,
    args: anytype,
) !u64 {
    for (0..cfg.warmup_iters) |_| {
        try @call(.auto, step, args);
        try ctx.synchronize();
    }

    var total_ns: u64 = 0;
    for (0..cfg.measure_iters) |_| {
        const started = nowNs();
        try @call(.auto, step, args);
        try ctx.synchronize();
        total_ns += nowNs() - started;
    }
    return total_ns / cfg.measure_iters;
}

fn repeatCudaStep(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    cfg: Config,
    comptime step: anytype,
    args: anytype,
) !u64 {
    if (cfg.quant_compiler_repeat_runs == 1) return timeCudaStep(ctx, cfg, step, args);
    const runs = try allocator.alloc(u64, cfg.quant_compiler_repeat_runs);
    defer allocator.free(runs);
    for (runs) |*run| {
        run.* = try timeCudaStep(ctx, cfg, step, args);
    }
    std.mem.sort(u64, runs, {}, std.sort.asc(u64));
    return runs[runs.len / 2];
}

fn benchTripleShape(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    shape: Shape,
) !void {
    const input_count = try std.math.mul(usize, shape.rows, shape.in_dim);
    const output_count = try std.math.mul(usize, shape.rows, shape.out_dim);
    const row_blocks = shape.in_dim / q4_k_values_per_block;
    const weight_bytes = try std.math.mul(usize, try std.math.mul(usize, shape.out_dim, row_blocks), q4_k_block_bytes);

    const input_host = try allocator.alloc(f32, input_count);
    defer allocator.free(input_host);
    const bias_host = try allocator.alloc(f32, shape.out_dim);
    defer allocator.free(bias_host);
    const weight_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_host);
    fillInput(input_host);
    fillBias(bias_host);
    fillQ4KWeights(weight_host);

    var input = try cuda_buffer.DeviceBuffer.alloc(ctx, input_count * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_host.len);
    defer weight.free(ctx);
    var bias = try cuda_buffer.DeviceBuffer.alloc(ctx, bias_host.len * @sizeOf(f32));
    defer bias.free(ctx);
    var output_a = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
    defer output_a.free(ctx);
    var output_b = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
    defer output_b.free(ctx);
    var output_c = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
    defer output_c.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(input_host));
    try weight.copyFromHost(ctx, weight_host);
    try bias.copyFromHost(ctx, std.mem.sliceAsBytes(bias_host));
    try ctx.synchronize();

    const scalar_ns = try timeCudaStep(ctx, cfg, launchQ4KTripleBias, .{ module, ctx, output_a, output_b, output_c, input, weight, bias, shape.rows, shape.in_dim, shape.out_dim });
    const tiled_ns = try timeCudaStep(ctx, cfg, launchQ4KTripleBiasTiled, .{ module, ctx, output_a, output_b, output_c, input, weight, bias, shape.rows, shape.in_dim, shape.out_dim });

    var sample: [1]f32 = undefined;
    try output_a.copyToHost(ctx, std.mem.sliceAsBytes(&sample));
    try ctx.synchronize();
    print("{s:<24} {d:>8} {d:>8} {d:>8} {d:>14} {d:>14} {d:>12.4}\n", .{
        shape.label,
        shape.rows,
        shape.in_dim,
        shape.out_dim,
        scalar_ns,
        tiled_ns,
        sample[0],
    });
}

const Gemma4Q8BenchResult = struct {
    scalar_ns: u64,
    tile4_ns: u64,
    checksum: f32,
};

const Gemma4Q4BenchResult = struct {
    scalar_ns: u64,
    tile4_ns: u64,
    tile8_ns: u64,
    checksum: f32,
};

fn speedup(base_ns: u64, candidate_ns: u64) f64 {
    if (base_ns == 0 or candidate_ns == 0) return 0;
    return @as(f64, @floatFromInt(base_ns)) / @as(f64, @floatFromInt(candidate_ns));
}

fn validateQuantCompilerEvidencePtxPath(bench: quant_kernel_compiler.BenchmarkCase, ptx_path: []const u8) !void {
    if (!std.mem.eql(u8, ptx_path, bench.generated_ptx_path)) return error.GeneratedPtxPathMismatch;
}

fn validateQuantCompilerEvidenceRequest(bench: quant_kernel_compiler.BenchmarkCase, ptx_path: []const u8, evidence_path: []const u8) !void {
    try validateQuantCompilerEvidencePtxPath(bench, ptx_path);
    if (!std.mem.eql(u8, evidence_path, quant_kernel_compiler.first_lazy_benchmark_evidence_path)) return error.QuantCompilerEvidencePathMismatch;
}

fn poisonDeviceOutput(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    buffer: cuda_buffer.DeviceBuffer,
    count: usize,
) !void {
    const poison = try allocator.alloc(f32, count);
    defer allocator.free(poison);
    @memset(poison, std.math.nan(f32));
    try buffer.copyFromHost(ctx, std.mem.sliceAsBytes(poison));
    try ctx.synchronize();
}

fn maxAbsDiffFinite(a: []const f32, b: []const f32) !f32 {
    if (a.len != b.len) return error.InvalidArgument;
    var max_abs_diff: f32 = 0.0;
    for (a, b) |left, right| {
        if (!std.math.isFinite(left) or !std.math.isFinite(right)) return error.NonFiniteBenchmarkOutput;
        max_abs_diff = @max(max_abs_diff, @abs(left - right));
    }
    return max_abs_diff;
}

fn writeQuantCompilerEvidence(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    bench: quant_kernel_compiler.BenchmarkCase,
    shape: Shape,
    ptx_path: []const u8,
    cfg: Config,
    baseline_ns: u64,
    generated_ns: u64,
    baseline_checksum: f32,
    generated_checksum: f32,
    baseline_cpu_max_abs_diff: f32,
    baseline_max_abs_diff: f32,
    cpu_max_abs_diff: f32,
) !void {
    const benchmark_command = try std.fmt.allocPrint(allocator, "zig-out/bin/antfly-inference bench-cuda --warmup-iters {d} --measure-iters {d} --quant-compiler-lazy-target {s} {s} --quant-compiler-repeat-runs {d} --quant-compiler-evidence-out {s}", .{
        cfg.warmup_iters,
        cfg.measure_iters,
        bench.generated_ptx_arg,
        ptx_path,
        cfg.quant_compiler_repeat_runs,
        path,
    });
    defer allocator.free(benchmark_command);

    const measured_speedup = speedup(baseline_ns, generated_ns);
    const benchmark_passed = std.math.isFinite(measured_speedup) and measured_speedup >= bench.minimum_speedup;
    const promotion_blocker = if (benchmark_passed) "dev_only_candidate" else "generated_slower_than_handwritten";
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    try appendJsonFmt(allocator, &out,
        \\{{
        \\"schema":"antfly.quant_kernel_benchmark_evidence.v1",
        \\"kernel_id":{f},
        \\"generated_source_path":{f},
        \\"generated_source_fingerprint":{d},
        \\"generated_ptx_path":{f},
        \\"generated_ptx_command":{f},
        \\"benchmark_command":{f},
        \\"correctness_evidence_path":{f},
        \\"benchmark_evidence_path":{f},
        \\"benchmark_mode":"sequential",
        \\"repeat_runs":{d},
        \\"timing_aggregation":{f},
        \\"production_enabled":false,
        \\"correctness_passed":true,
        \\"benchmark_passed":{s},
        \\"promotion_ready":false,
        \\"promotion_blocker":{f},
        \\"measured_speedup":{d:.6},
        \\"minimum_speedup":{d:.6},
        \\"correctness_tolerance_abs":{d:.6},
        \\"baseline_kernel":{f},
        \\"baseline_ns":{d},
        \\"generated_ns":{d},
        \\"baseline_checksum":{d:.6},
        \\"generated_checksum":{d:.6},
        \\"baseline_cpu_max_abs_diff":{d:.6},
        \\"baseline_max_abs_diff":{d:.6},
        \\"cpu_max_abs_diff":{d:.6},
        \\"warmup_iters":{d},
        \\"measure_iters":{d},
        \\"shape":{{"label":{f},"rows":{d},"in_dim":{d},"out_dim":{d}}}
        \\}}
        \\
    , .{
        std.json.fmt(bench.generated_kernel_id, .{}),
        std.json.fmt(bench.generated_source_path, .{}),
        bench.generated_source_fingerprint,
        std.json.fmt(ptx_path, .{}),
        std.json.fmt(bench.generated_ptx_command, .{}),
        std.json.fmt(benchmark_command, .{}),
        std.json.fmt(path, .{}),
        std.json.fmt(path, .{}),
        cfg.quant_compiler_repeat_runs,
        std.json.fmt(if (cfg.quant_compiler_repeat_runs == 1) "single" else "median", .{}),
        if (benchmark_passed) "true" else "false",
        std.json.fmt(promotion_blocker, .{}),
        measured_speedup,
        bench.minimum_speedup,
        bench.correctness_tolerance_abs,
        std.json.fmt(bench.handwritten_baseline, .{}),
        baseline_ns,
        generated_ns,
        baseline_checksum,
        generated_checksum,
        baseline_cpu_max_abs_diff,
        baseline_max_abs_diff,
        cpu_max_abs_diff,
        cfg.warmup_iters,
        cfg.measure_iters,
        std.json.fmt(shape.label, .{}),
        shape.rows,
        shape.in_dim,
        shape.out_dim,
    });
    try checkQuantCompilerEvidenceJson(allocator, out.items, false);
    try writeFileCreatingParent(io, path, out.items);
}

fn checkQuantCompilerEvidenceJson(allocator: std.mem.Allocator, bytes: []const u8, require_promotion_ready: bool) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidQuantCompilerEvidence;
    const bench = quant_kernel_compiler.first_lazy_benchmark;

    const schema = jsonString(root.object.get("schema")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.mem.eql(u8, schema, "antfly.quant_kernel_benchmark_evidence.v1")) return error.InvalidQuantCompilerEvidence;
    const benchmark_mode = jsonString(root.object.get("benchmark_mode")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.mem.eql(u8, benchmark_mode, "sequential")) return error.InvalidQuantCompilerEvidence;
    const repeat_runs = jsonUsize(root.object.get("repeat_runs")) orelse return error.InvalidQuantCompilerEvidence;
    if (repeat_runs == 0 or repeat_runs > 31) return error.InvalidQuantCompilerEvidence;
    const timing_aggregation = jsonString(root.object.get("timing_aggregation")) orelse return error.InvalidQuantCompilerEvidence;
    const expected_timing_aggregation = if (repeat_runs == 1) "single" else "median";
    if (!std.mem.eql(u8, timing_aggregation, expected_timing_aggregation)) return error.InvalidQuantCompilerEvidence;
    const production_enabled = jsonBoolValue(root.object.get("production_enabled")) orelse return error.InvalidQuantCompilerEvidence;
    if (require_promotion_ready) {
        if (!production_enabled) return error.QuantCompilerEvidencePromotionNotReady;
    } else if (production_enabled) {
        return error.InvalidQuantCompilerEvidence;
    }
    const correctness_passed = jsonBoolValue(root.object.get("correctness_passed")) orelse return error.InvalidQuantCompilerEvidence;
    if (!correctness_passed) return error.InvalidQuantCompilerEvidence;
    const promotion_ready = jsonBoolValue(root.object.get("promotion_ready")) orelse return error.InvalidQuantCompilerEvidence;
    if (require_promotion_ready) {
        if (!promotion_ready) return error.QuantCompilerEvidencePromotionNotReady;
        if (repeat_runs < quant_compiler_evidence_repeat_runs) return error.QuantCompilerEvidencePromotionNotReady;
    } else if (promotion_ready) {
        return error.InvalidQuantCompilerEvidence;
    }

    const baseline_ns = jsonU64(root.object.get("baseline_ns")) orelse return error.InvalidQuantCompilerEvidence;
    const generated_ns = jsonU64(root.object.get("generated_ns")) orelse return error.InvalidQuantCompilerEvidence;
    if (baseline_ns == 0 or generated_ns == 0) return error.InvalidQuantCompilerEvidence;
    const measured_speedup = jsonF64(root.object.get("measured_speedup")) orelse return error.InvalidQuantCompilerEvidence;
    if (!approximately(measured_speedup, speedup(baseline_ns, generated_ns), 0.000001)) return error.InvalidQuantCompilerEvidence;

    const minimum_speedup = jsonF64(root.object.get("minimum_speedup")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.math.isFinite(minimum_speedup) or minimum_speedup < 1.0) return error.InvalidQuantCompilerEvidence;
    const benchmark_passed = jsonBoolValue(root.object.get("benchmark_passed")) orelse return error.InvalidQuantCompilerEvidence;
    if (benchmark_passed != (measured_speedup >= minimum_speedup)) return error.InvalidQuantCompilerEvidence;
    const blocker = jsonString(root.object.get("promotion_blocker")) orelse return error.InvalidQuantCompilerEvidence;
    if (promotion_ready) {
        if (!benchmark_passed) return error.InvalidQuantCompilerEvidence;
        if (blocker.len != 0) return error.InvalidQuantCompilerEvidence;
    } else {
        const expected_blocker = if (benchmark_passed) "dev_only_candidate" else "generated_slower_than_handwritten";
        if (!std.mem.eql(u8, blocker, expected_blocker)) return error.InvalidQuantCompilerEvidence;
    }

    const tolerance = jsonF64(root.object.get("correctness_tolerance_abs")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.math.isFinite(tolerance) or tolerance <= 0.0) return error.InvalidQuantCompilerEvidence;
    if (!approximately(tolerance, @as(f64, @floatCast(bench.correctness_tolerance_abs)), 0.000001)) return error.InvalidQuantCompilerEvidence;
    const baseline_cpu_max_abs_diff = jsonF64(root.object.get("baseline_cpu_max_abs_diff")) orelse return error.InvalidQuantCompilerEvidence;
    const baseline_max_abs_diff = jsonF64(root.object.get("baseline_max_abs_diff")) orelse return error.InvalidQuantCompilerEvidence;
    const cpu_max_abs_diff = jsonF64(root.object.get("cpu_max_abs_diff")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.math.isFinite(baseline_cpu_max_abs_diff) or baseline_cpu_max_abs_diff > tolerance) return error.InvalidQuantCompilerEvidence;
    if (!std.math.isFinite(baseline_max_abs_diff) or baseline_max_abs_diff > tolerance) return error.InvalidQuantCompilerEvidence;
    if (!std.math.isFinite(cpu_max_abs_diff) or cpu_max_abs_diff > tolerance) return error.InvalidQuantCompilerEvidence;

    const kernel_id = jsonString(root.object.get("kernel_id")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.mem.eql(u8, kernel_id, bench.generated_kernel_id)) return error.InvalidQuantCompilerEvidence;
    const generated_source_path = jsonString(root.object.get("generated_source_path")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.mem.eql(u8, generated_source_path, bench.generated_source_path)) return error.InvalidQuantCompilerEvidence;
    const generated_source_fingerprint = jsonU64(root.object.get("generated_source_fingerprint")) orelse return error.InvalidQuantCompilerEvidence;
    if (generated_source_fingerprint != bench.generated_source_fingerprint) return error.InvalidQuantCompilerEvidence;
    const generated_ptx_path = jsonString(root.object.get("generated_ptx_path")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.mem.eql(u8, generated_ptx_path, bench.generated_ptx_path)) return error.InvalidQuantCompilerEvidence;
    const generated_ptx_command = jsonString(root.object.get("generated_ptx_command")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.mem.eql(u8, generated_ptx_command, bench.generated_ptx_command)) return error.InvalidQuantCompilerEvidence;
    const baseline_kernel = jsonString(root.object.get("baseline_kernel")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.mem.eql(u8, baseline_kernel, bench.handwritten_baseline)) return error.InvalidQuantCompilerEvidence;
    inline for (&.{ "kernel_id", "generated_source_path", "generated_ptx_path", "generated_ptx_command", "benchmark_command", "correctness_evidence_path", "benchmark_evidence_path", "baseline_kernel" }) |field| {
        const text = jsonString(root.object.get(field)) orelse return error.InvalidQuantCompilerEvidence;
        if (text.len == 0) return error.InvalidQuantCompilerEvidence;
    }
    const correctness_path = jsonString(root.object.get("correctness_evidence_path")) orelse return error.InvalidQuantCompilerEvidence;
    const benchmark_path = jsonString(root.object.get("benchmark_evidence_path")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.mem.eql(u8, correctness_path, benchmark_path)) return error.InvalidQuantCompilerEvidence;
    const benchmark_command = jsonString(root.object.get("benchmark_command")) orelse return error.InvalidQuantCompilerEvidence;
    const expected_benchmark_command = try std.fmt.allocPrint(
        allocator,
        "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-lazy-target {s} {s} --quant-compiler-repeat-runs {d} --quant-compiler-evidence-out {s}",
        .{ bench.generated_ptx_arg, bench.generated_ptx_path, repeat_runs, benchmark_path },
    );
    defer allocator.free(expected_benchmark_command);
    if (!std.mem.eql(u8, benchmark_command, expected_benchmark_command)) return error.InvalidQuantCompilerEvidence;

    if ((jsonUsize(root.object.get("warmup_iters")) orelse 0) != 5) return error.InvalidQuantCompilerEvidence;
    if ((jsonUsize(root.object.get("measure_iters")) orelse 0) != 50) return error.InvalidQuantCompilerEvidence;
    if (repeat_runs != quant_compiler_evidence_repeat_runs) return error.InvalidQuantCompilerEvidence;
    const shape_value = root.object.get("shape") orelse return error.InvalidQuantCompilerEvidence;
    if (shape_value != .object) return error.InvalidQuantCompilerEvidence;
    const shape_label = jsonString(shape_value.object.get("label")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.mem.eql(u8, shape_label, quant_compiler_lazy_shape.label)) return error.InvalidQuantCompilerEvidence;
    if ((jsonUsize(shape_value.object.get("rows")) orelse 0) != quant_compiler_lazy_shape.rows) return error.InvalidQuantCompilerEvidence;
    if ((jsonUsize(shape_value.object.get("in_dim")) orelse 0) != quant_compiler_lazy_shape.in_dim) return error.InvalidQuantCompilerEvidence;
    if ((jsonUsize(shape_value.object.get("out_dim")) orelse 0) != quant_compiler_lazy_shape.out_dim) return error.InvalidQuantCompilerEvidence;
}

fn checkQuantCompilerEvidenceFile(allocator: std.mem.Allocator, io: std.Io, path: []const u8, require_promotion_ready: bool) !void {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(1024 * 1024));
    defer allocator.free(bytes);
    try checkQuantCompilerEvidenceJson(allocator, bytes, require_promotion_ready);
    try checkQuantCompilerEvidenceJsonPath(allocator, bytes, path);
}

fn checkQuantCompilerEvidenceJsonPath(allocator: std.mem.Allocator, bytes: []const u8, path: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidQuantCompilerEvidence;
    const correctness_path = jsonString(root.object.get("correctness_evidence_path")) orelse return error.InvalidQuantCompilerEvidence;
    const benchmark_path = jsonString(root.object.get("benchmark_evidence_path")) orelse return error.InvalidQuantCompilerEvidence;
    if (!std.mem.eql(u8, correctness_path, path)) return error.InvalidQuantCompilerEvidence;
    if (!std.mem.eql(u8, benchmark_path, path)) return error.InvalidQuantCompilerEvidence;
}

fn writeFileCreatingParent(io: std.Io, path: []const u8, data: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| {
        if (parent.len > 0) try compat.cwd().createDirPath(io, parent);
    }
    if (std.fs.path.isAbsolute(path)) {
        var file = try std.Io.Dir.createFileAbsolute(io, path, .{ .truncate = true });
        defer file.close(io);
        try file.writeStreamingAll(io, data);
    } else {
        try compat.cwd().writeFile(io, .{ .sub_path = path, .data = data });
    }
}

fn approximately(actual: f64, expected: f64, tolerance: f64) bool {
    return std.math.isFinite(actual) and std.math.isFinite(expected) and @abs(actual - expected) <= tolerance;
}

fn jsonString(value: ?std.json.Value) ?[]const u8 {
    const actual = value orelse return null;
    return switch (actual) {
        .string => |text| text,
        else => null,
    };
}

fn jsonBoolValue(value: ?std.json.Value) ?bool {
    const actual = value orelse return null;
    return switch (actual) {
        .bool => |val| val,
        else => null,
    };
}

fn jsonUsize(value: ?std.json.Value) ?usize {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |val| if (val >= 0) @intCast(val) else null,
        else => null,
    };
}

fn jsonU64(value: ?std.json.Value) ?u64 {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |val| if (val >= 0) @intCast(val) else null,
        .number_string => |text| std.fmt.parseUnsigned(u64, text, 10) catch null,
        else => null,
    };
}

fn jsonF64(value: ?std.json.Value) ?f64 {
    const actual = value orelse return null;
    return switch (actual) {
        .float => |val| val,
        .integer => |val| @floatFromInt(val),
        .number_string => |text| std.fmt.parseFloat(f64, text) catch null,
        else => null,
    };
}

test "cuda microbench finite diff guards reference comparisons" {
    try std.testing.expectEqual(@as(f32, 0.25), try maxAbsDiffFinite(&.{ 1.0, 2.0 }, &.{ 1.25, 1.75 }));
    try std.testing.expectError(error.InvalidArgument, maxAbsDiffFinite(&.{1.0}, &.{ 1.0, 2.0 }));
    try std.testing.expectError(error.NonFiniteBenchmarkOutput, maxAbsDiffFinite(&.{std.math.nan(f32)}, &.{0.0}));
}

test "cuda microbench promotion evidence requires manifest ptx path" {
    const bench = quant_kernel_compiler.first_lazy_benchmark;
    try validateQuantCompilerEvidencePtxPath(bench, bench.generated_ptx_path);
    try std.testing.expectError(error.GeneratedPtxPathMismatch, validateQuantCompilerEvidencePtxPath(bench, "/tmp/other.ptx"));
    try validateQuantCompilerEvidenceRequest(bench, bench.generated_ptx_path, quant_kernel_compiler.first_lazy_benchmark_evidence_path);
    try std.testing.expectError(error.QuantCompilerEvidencePathMismatch, validateQuantCompilerEvidenceRequest(bench, bench.generated_ptx_path, "/tmp/other-evidence.json"));
}

test "cuda microbench writes quant compiler evidence json" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const evidence_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "evidence", "q4.json" });
    defer std.testing.allocator.free(evidence_path);

    try writeQuantCompilerEvidence(
        std.testing.allocator,
        std.testing.io,
        evidence_path,
        quant_kernel_compiler.first_lazy_benchmark,
        quant_compiler_lazy_shape,
        quant_kernel_compiler.first_lazy_benchmark.generated_ptx_path,
        .{ .warmup_iters = 5, .measure_iters = 50, .quant_compiler_repeat_runs = quant_compiler_evidence_repeat_runs },
        100,
        80,
        1.0,
        1.0,
        0.001,
        0.001,
        0.001,
    );

    const actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, evidence_path, std.testing.allocator, .limited(4096));
    defer std.testing.allocator.free(actual);
    try checkQuantCompilerEvidenceJson(std.testing.allocator, actual, false);
    try checkQuantCompilerEvidenceFile(std.testing.allocator, std.testing.io, evidence_path, false);
    try std.testing.expectError(error.QuantCompilerEvidencePromotionNotReady, checkQuantCompilerEvidenceJson(std.testing.allocator, actual, true));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"schema\":\"antfly.quant_kernel_benchmark_evidence.v1\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"generated_source_fingerprint\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"generated_ptx_command\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, quant_kernel_compiler.first_lazy_benchmark.generated_ptx_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"benchmark_command\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "--warmup-iters 5"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "--measure-iters 50"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "--quant-compiler-repeat-runs 3"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"benchmark_mode\":\"sequential\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"repeat_runs\":3"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"timing_aggregation\":\"median\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"production_enabled\":false"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"correctness_passed\":true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"benchmark_passed\":true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"promotion_ready\":false"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"promotion_blocker\":\"dev_only_candidate\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, "\"measured_speedup\":1.250000"));
    try std.testing.expect(std.mem.containsAtLeast(u8, actual, 1, evidence_path));

    const copied_evidence_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "evidence", "q4_copied.json" });
    defer std.testing.allocator.free(copied_evidence_path);
    try writeFileCreatingParent(std.testing.io, copied_evidence_path, actual);
    try std.testing.expectError(error.InvalidQuantCompilerEvidence, checkQuantCompilerEvidenceFile(std.testing.allocator, std.testing.io, copied_evidence_path, false));

    const wrong_speedup = try replaceOnce(std.testing.allocator, actual, "\"measured_speedup\":1.250000", "\"measured_speedup\":1.100000");
    defer std.testing.allocator.free(wrong_speedup);
    try std.testing.expectError(error.InvalidQuantCompilerEvidence, checkQuantCompilerEvidenceJson(std.testing.allocator, wrong_speedup, false));
    const wrong_command = try replaceOnce(std.testing.allocator, actual, "--quant-compiler-lazy-target", "--gemma4-shapes");
    defer std.testing.allocator.free(wrong_command);
    try std.testing.expectError(error.InvalidQuantCompilerEvidence, checkQuantCompilerEvidenceJson(std.testing.allocator, wrong_command, false));
    const wrong_ptx_path = try replaceOnce(std.testing.allocator, actual, quant_kernel_compiler.first_lazy_benchmark.generated_ptx_path, "/tmp/wrong.ptx");
    defer std.testing.allocator.free(wrong_ptx_path);
    try std.testing.expectError(error.InvalidQuantCompilerEvidence, checkQuantCompilerEvidenceJson(std.testing.allocator, wrong_ptx_path, false));
    const wrong_shape = try replaceOnce(std.testing.allocator, actual, "\"rows\":8", "\"rows\":7");
    defer std.testing.allocator.free(wrong_shape);
    try std.testing.expectError(error.InvalidQuantCompilerEvidence, checkQuantCompilerEvidenceJson(std.testing.allocator, wrong_shape, false));
    const loose_tolerance = try replaceOnce(std.testing.allocator, actual, "\"correctness_tolerance_abs\":0.010000", "\"correctness_tolerance_abs\":0.020000");
    defer std.testing.allocator.free(loose_tolerance);
    try std.testing.expectError(error.InvalidQuantCompilerEvidence, checkQuantCompilerEvidenceJson(std.testing.allocator, loose_tolerance, false));
    const bad_evidence_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "evidence", "q4_bad.json" });
    defer std.testing.allocator.free(bad_evidence_path);
    try writeFileCreatingParent(std.testing.io, bad_evidence_path, wrong_speedup);
    try std.testing.expectError(error.InvalidQuantCompilerEvidence, checkQuantCompilerEvidenceFile(std.testing.allocator, std.testing.io, bad_evidence_path, false));

    const slow_evidence_path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "evidence", "q4_slow.json" });
    defer std.testing.allocator.free(slow_evidence_path);
    try writeQuantCompilerEvidence(
        std.testing.allocator,
        std.testing.io,
        slow_evidence_path,
        quant_kernel_compiler.first_lazy_benchmark,
        quant_compiler_lazy_shape,
        quant_kernel_compiler.first_lazy_benchmark.generated_ptx_path,
        .{ .warmup_iters = 5, .measure_iters = 50, .quant_compiler_repeat_runs = quant_compiler_evidence_repeat_runs },
        100,
        125,
        1.0,
        1.0,
        0.001,
        0.001,
        0.001,
    );
    const slow_actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, slow_evidence_path, std.testing.allocator, .limited(4096));
    defer std.testing.allocator.free(slow_actual);
    try checkQuantCompilerEvidenceJson(std.testing.allocator, slow_actual, false);
    try std.testing.expect(std.mem.containsAtLeast(u8, slow_actual, 1, "\"benchmark_passed\":false"));
    try std.testing.expect(std.mem.containsAtLeast(u8, slow_actual, 1, "\"promotion_blocker\":\"generated_slower_than_handwritten\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, slow_actual, 1, "\"measured_speedup\":0.800000"));

    const wrong_blocker = try replaceOnce(std.testing.allocator, slow_actual, "\"promotion_blocker\":\"generated_slower_than_handwritten\"", "\"promotion_blocker\":\"dev_only_candidate\"");
    defer std.testing.allocator.free(wrong_blocker);
    try std.testing.expectError(error.InvalidQuantCompilerEvidence, checkQuantCompilerEvidenceJson(std.testing.allocator, wrong_blocker, false));
    const production_slow = try replaceOnce(std.testing.allocator, slow_actual, "\"production_enabled\":false", "\"production_enabled\":true");
    defer std.testing.allocator.free(production_slow);
    const promoted_slow_tmp = try replaceOnce(std.testing.allocator, production_slow, "\"promotion_ready\":false", "\"promotion_ready\":true");
    defer std.testing.allocator.free(promoted_slow_tmp);
    const promoted_slow = try replaceOnce(std.testing.allocator, promoted_slow_tmp, "\"promotion_blocker\":\"generated_slower_than_handwritten\"", "\"promotion_blocker\":\"\"");
    defer std.testing.allocator.free(promoted_slow);
    try std.testing.expectError(error.InvalidQuantCompilerEvidence, checkQuantCompilerEvidenceJson(std.testing.allocator, promoted_slow, true));
}

fn replaceOnce(allocator: std.mem.Allocator, input: []const u8, needle: []const u8, replacement: []const u8) ![]u8 {
    const index = std.mem.indexOf(u8, input, needle) orelse return error.InvalidArgument;
    return std.fmt.allocPrint(allocator, "{s}{s}{s}", .{ input[0..index], replacement, input[index + needle.len ..] });
}

test "cuda microbench evidence writer creates absolute parent directories" {
    const root = try std.fmt.allocPrint(std.testing.allocator, "/tmp/antfly_quant_evidence_test_{d}", .{std.posix.system.getpid()});
    defer std.testing.allocator.free(root);
    compat.cwd().deleteTree(std.testing.io, root) catch {};
    defer compat.cwd().deleteTree(std.testing.io, root) catch {};

    const evidence_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/nested/q4.json", .{root});
    defer std.testing.allocator.free(evidence_path);

    try writeFileCreatingParent(std.testing.io, evidence_path, "evidence");
    const actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, evidence_path, std.testing.allocator, .limited(64));
    defer std.testing.allocator.free(actual);
    try std.testing.expectEqualStrings("evidence", actual);
}

fn q8CandidateName(q8: Gemma4Q8BenchResult) []const u8 {
    return if (q8.tile4_ns <= q8.scalar_ns) "tile4" else "scalar";
}

fn q8CandidateNs(q8: Gemma4Q8BenchResult) u64 {
    return if (q8.tile4_ns <= q8.scalar_ns) q8.tile4_ns else q8.scalar_ns;
}

fn q4CandidateName(q4: Gemma4Q4BenchResult) []const u8 {
    if (q4.tile4_ns <= q4.scalar_ns and q4.tile4_ns <= q4.tile8_ns) return "tile4";
    if (q4.tile8_ns <= q4.scalar_ns and q4.tile8_ns <= q4.tile4_ns) return "tile8";
    return "scalar";
}

fn q4CandidateNs(q4: Gemma4Q4BenchResult) u64 {
    if (q4.tile4_ns <= q4.scalar_ns and q4.tile4_ns <= q4.tile8_ns) return q4.tile4_ns;
    if (q4.tile8_ns <= q4.scalar_ns and q4.tile8_ns <= q4.tile4_ns) return q4.tile8_ns;
    return q4.scalar_ns;
}

fn runGemma4KernelBench(
    allocator: std.mem.Allocator,
    io: std.Io,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
) !void {
    var json_out = std.ArrayListUnmanaged(u8).empty;
    defer json_out.deinit(allocator);
    if (cfg.json_out_path != null) {
        try appendJsonFmt(allocator, &json_out,
            \\{{
            \\"warmup_iters":{d},
            \\"measure_iters":{d},
            \\"gemma4_shapes":[
        , .{ cfg.warmup_iters, cfg.measure_iters });
    }

    print("\nCUDA Gemma4 decode matvec microbench: synthetic rows=1 weights\n", .{});
    print("{s:<24} {s:>8} {s:>8} {s:>8} {s:>14} {s:>14} {s:>14} {s:>14} {s:>14} {s:>12} {s:>12}\n", .{
        "shape",
        "rows",
        "in",
        "out",
        "q8 scalar",
        "q8 tile4",
        "q4 scalar",
        "q4 tile4",
        "q4 tile8",
        "q8 check",
        "q4 check",
    });
    print("{s:<24} {s:>8} {s:>8} {s:>8} {s:>14} {s:>14} {s:>14} {s:>14} {s:>14} {s:>12} {s:>12}\n", .{
        "-----",
        "----",
        "--",
        "---",
        "---------",
        "--------",
        "---------",
        "--------",
        "--------",
        "--------",
        "--------",
    });

    for (gemma4_shapes, 0..) |shape, idx| {
        const q8 = try benchGemma4Q8Shape(allocator, ctx, module, cfg, shape);
        const q4 = try benchGemma4Q4Shape(allocator, ctx, module, cfg, shape);
        print("{s:<24} {d:>8} {d:>8} {d:>8} {d:>14} {d:>14} {d:>14} {d:>14} {d:>14} {d:>12.4} {d:>12.4}\n", .{
            shape.label,
            shape.rows,
            shape.in_dim,
            shape.out_dim,
            q8.scalar_ns,
            q8.tile4_ns,
            q4.scalar_ns,
            q4.tile4_ns,
            q4.tile8_ns,
            q8.checksum,
            q4.checksum,
        });
        if (cfg.json_out_path != null) {
            const q8_candidate = q8CandidateName(q8);
            const q8_candidate_ns = q8CandidateNs(q8);
            const q4_candidate = q4CandidateName(q4);
            const q4_candidate_ns = q4CandidateNs(q4);
            if (idx != 0) try json_out.appendSlice(allocator, ",");
            try appendJsonFmt(allocator, &json_out,
                \\{{
                \\"label":{f},
                \\"rows":{d},
                \\"in_dim":{d},
                \\"out_dim":{d},
                \\"q8_scalar_ns":{d},
                \\"q8_tile4_ns":{d},
                \\"q4_scalar_ns":{d},
                \\"q4_tile4_ns":{d},
                \\"q4_tile8_ns":{d},
                \\"q8_baseline_ns":{d},
                \\"q8_candidate":{f},
                \\"q8_candidate_ns":{d},
                \\"q8_candidate_speedup":{d:.6},
                \\"q4_baseline_ns":{d},
                \\"q4_candidate":{f},
                \\"q4_candidate_ns":{d},
                \\"q4_candidate_speedup":{d:.6},
                \\"q8_checksum":{d:.6},
                \\"q4_checksum":{d:.6}
                \\}}
            , .{
                std.json.fmt(shape.label, .{}),
                shape.rows,
                shape.in_dim,
                shape.out_dim,
                q8.scalar_ns,
                q8.tile4_ns,
                q4.scalar_ns,
                q4.tile4_ns,
                q4.tile8_ns,
                q8.scalar_ns,
                std.json.fmt(q8_candidate, .{}),
                q8_candidate_ns,
                speedup(q8.scalar_ns, q8_candidate_ns),
                q4.scalar_ns,
                std.json.fmt(q4_candidate, .{}),
                q4_candidate_ns,
                speedup(q4.scalar_ns, q4_candidate_ns),
                q8.checksum,
                q4.checksum,
            });
        }
    }

    if (cfg.json_out_path) |path| {
        try json_out.appendSlice(allocator,
            \\]
            \\}
            \\
        );
        try compat.cwd().writeFile(io, .{ .sub_path = path, .data = json_out.items });
    }
}

fn appendJsonFmt(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const chunk = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(chunk);
    try out.appendSlice(allocator, chunk);
}

fn benchGemma4Q8Shape(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    shape: Shape,
) !Gemma4Q8BenchResult {
    if (shape.in_dim % q8_0_values_per_block != 0) return error.InvalidArgument;

    const input_count = try std.math.mul(usize, shape.rows, shape.in_dim);
    const output_count = try std.math.mul(usize, shape.rows, shape.out_dim);
    const row_blocks = shape.in_dim / q8_0_values_per_block;
    const weight_bytes = try std.math.mul(usize, try std.math.mul(usize, shape.out_dim, row_blocks), q8_0_block_bytes);

    const input_host = try allocator.alloc(f32, input_count);
    defer allocator.free(input_host);
    const weight_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_host);
    fillInput(input_host);
    fillQ8Weights(weight_host);

    var input = try cuda_buffer.DeviceBuffer.alloc(ctx, input_count * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_host.len);
    defer weight.free(ctx);
    var output = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
    defer output.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(input_host));
    try weight.copyFromHost(ctx, weight_host);
    try ctx.synchronize();

    const scalar_ns = try timeCudaStep(ctx, cfg, launchQ8, .{ module, ctx, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });
    const tile4_ns = try timeCudaStep(ctx, cfg, launchQ8Tile4, .{ module, ctx, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });

    var sample: [1]f32 = undefined;
    try output.copyToHost(ctx, std.mem.sliceAsBytes(&sample));
    try ctx.synchronize();
    return .{
        .scalar_ns = scalar_ns,
        .tile4_ns = tile4_ns,
        .checksum = sample[0],
    };
}

fn benchGemma4Q4Shape(
    allocator: std.mem.Allocator,
    ctx: *cuda_context.CudaContext,
    module: *BenchModule,
    cfg: Config,
    shape: Shape,
) !Gemma4Q4BenchResult {
    if (shape.in_dim % q4_k_values_per_block != 0) return error.InvalidArgument;

    const input_count = try std.math.mul(usize, shape.rows, shape.in_dim);
    const output_count = try std.math.mul(usize, shape.rows, shape.out_dim);
    const row_blocks = shape.in_dim / q4_k_values_per_block;
    const weight_bytes = try std.math.mul(usize, try std.math.mul(usize, shape.out_dim, row_blocks), q4_k_block_bytes);

    const input_host = try allocator.alloc(f32, input_count);
    defer allocator.free(input_host);
    const weight_host = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(weight_host);
    fillInput(input_host);
    fillQ4KWeights(weight_host);

    var input = try cuda_buffer.DeviceBuffer.alloc(ctx, input_count * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try cuda_buffer.DeviceBuffer.alloc(ctx, weight_host.len);
    defer weight.free(ctx);
    var output = try cuda_buffer.DeviceBuffer.alloc(ctx, output_count * @sizeOf(f32));
    defer output.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(input_host));
    try weight.copyFromHost(ctx, weight_host);
    try ctx.synchronize();

    const scalar_ns = try timeCudaStep(ctx, cfg, launchQ4K, .{ module, ctx, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });
    const tile4_ns = try timeCudaStep(ctx, cfg, launchQ4KTiled, .{ module, ctx, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });
    const tile8_ns = try timeCudaStep(ctx, cfg, launchQ4KTile8, .{ module, ctx, output, input, weight, shape.rows, shape.in_dim, shape.out_dim });

    var sample: [1]f32 = undefined;
    try output.copyToHost(ctx, std.mem.sliceAsBytes(&sample));
    try ctx.synchronize();
    return .{
        .scalar_ns = scalar_ns,
        .tile4_ns = tile4_ns,
        .tile8_ns = tile8_ns,
        .checksum = sample[0],
    };
}

fn launchQ8(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ8Raw(ctx, module.linear_q8_0_f32, output, input, weight, rows, in_dim, out_dim);
}

fn launchQ8Tile4(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ8Tile4(ctx, module.linear_q8_0_f32_tile4, output, input, weight, rows, in_dim, out_dim);
}

fn launchQ4K(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KRaw(ctx, module.linear_q4_k_f32, output, input, weight, rows, in_dim, out_dim);
}

fn launchQ4KBias(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KBiasRaw(ctx, module.linear_q4_k_bias_f32, output, input, weight, bias, rows, in_dim, out_dim);
}

fn launchQ4KTiled(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KTile4(ctx, module.linear_q4_k_f32_tile4, output, input, weight, .{}, rows, in_dim, out_dim, false, false);
}

fn launchQ4KTile8(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KTile8(ctx, module.linear_q4_k_f32_tile8, output, input, weight, rows, in_dim, out_dim);
}

fn launchQ4KBiasTiled(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KTile4(ctx, module.linear_q4_k_bias_f32_tile4, output, input, weight, bias, rows, in_dim, out_dim, true, false);
}

fn launchQ4KBiasGeluRows2(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KBiasGeluRows2(ctx, module.linear_q4_k_bias_gelu_f32_tile4_r2, output, input, weight, bias, rows, in_dim, out_dim);
}

fn launchGeneratedQ4KBiasGeluRows2(
    module: *GeneratedCandidateModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchGeneratedLinearQ4KBiasGeluRows2(ctx, module.function, output, input, weight, bias, rows, in_dim, out_dim);
}

fn launchQ4KBiasQuickGeluTiled(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KTile4(ctx, module.linear_q4_k_bias_quick_gelu_f32_tile4, output, input, weight, bias, rows, in_dim, out_dim, true, false);
}

fn launchLinearQ4KTile4(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    has_bias: bool,
    has_residual: bool,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, if (has_bias) bias else null, rows, in_dim, out_dim);
    if (has_residual) return error.InvalidCudaState;
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var bias_ptr = bias.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&bias_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    if (!has_bias) {
        params[3] = @ptrCast(&rows_u32);
        params[4] = @ptrCast(&in_dim_u32);
        params[5] = @ptrCast(&out_dim_u32);
    }
    try launch2d(ctx, function, (out_dim + 3) / 4, rows, 256, &params);
}

fn launchLinearQ4KBiasGeluRows2(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, bias, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var bias_ptr = bias.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&bias_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch2d(ctx, function, (out_dim + 3) / 4, (rows + 1) / 2, 256, &params);
}

fn launchGeneratedLinearQ4KBiasGeluRows2(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, bias, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var bias_ptr = bias.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&bias_ptr),
        @ptrCast(&dst_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch2d(ctx, function, out_dim, rows, 128, &params);
}

fn launchLinearQ4KTile8(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, null, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch2d(ctx, function, (out_dim + 7) / 8, rows, 256, &params);
}

fn launchLinearQ8Raw(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ8Buffers(output, input, weight, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch1d(ctx, function, try checkedMul(rows, out_dim), &params);
}

fn launchLinearQ8Tile4(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ8Buffers(output, input, weight, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch2d(ctx, function, (out_dim + 3) / 4, rows, 256, &params);
}

fn launchQ4KTripleBias(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output_a: cuda_buffer.DeviceBuffer,
    output_b: cuda_buffer.DeviceBuffer,
    output_c: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KTripleBiasRaw(ctx, module.linear_q4_k_triple_bias_f32, output_a, output_b, output_c, input, weight, bias, rows, in_dim, out_dim, false);
}

fn launchQ4KTripleBiasTiled(
    module: *BenchModule,
    ctx: *cuda_context.CudaContext,
    output_a: cuda_buffer.DeviceBuffer,
    output_b: cuda_buffer.DeviceBuffer,
    output_c: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !void {
    try launchLinearQ4KTripleBiasRaw(ctx, module.linear_q4_k_triple_bias_f32_tiled, output_a, output_b, output_c, input, weight, bias, rows, in_dim, out_dim, true);
}

fn launchLinearQ4KRaw(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, null, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch1d(ctx, function, try checkedMul(rows, out_dim), &params);
}

fn launchLinearQ4KBiasRaw(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, bias, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var bias_ptr = bias.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&bias_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch1d(ctx, function, try checkedMul(rows, out_dim), &params);
}

fn launchLinearQ4KBlocks(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, null, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launchBlocks(ctx, function, try checkedMul(rows, out_dim), 256, &params);
}

fn launchLinearQ4KBiasBlocks(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output, input, weight, bias, rows, in_dim, out_dim);
    var dst_ptr = output.ptr;
    var input_ptr = input.ptr;
    var weight_ptr = weight.ptr;
    var bias_ptr = bias.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_ptr),
        @ptrCast(&bias_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launchBlocks(ctx, function, try checkedMul(rows, out_dim), 256, &params);
}

fn launchLinearQ4KTripleBiasRaw(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    output_a: cuda_buffer.DeviceBuffer,
    output_b: cuda_buffer.DeviceBuffer,
    output_c: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    tiled: bool,
) cuda_driver.Error!void {
    try validateQ4KBuffers(output_a, input, weight, bias, rows, in_dim, out_dim);
    try validateQ4KBuffers(output_b, input, weight, bias, rows, in_dim, out_dim);
    try validateQ4KBuffers(output_c, input, weight, bias, rows, in_dim, out_dim);
    var dst_a_ptr = output_a.ptr;
    var dst_b_ptr = output_b.ptr;
    var dst_c_ptr = output_c.ptr;
    var input_ptr = input.ptr;
    var weight_a_ptr = weight.ptr;
    var bias_a_ptr = bias.ptr;
    var weight_b_ptr = weight.ptr;
    var bias_b_ptr = bias.ptr;
    var weight_c_ptr = weight.ptr;
    var bias_c_ptr = bias.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_a_ptr),
        @ptrCast(&dst_b_ptr),
        @ptrCast(&dst_c_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_a_ptr),
        @ptrCast(&bias_a_ptr),
        @ptrCast(&weight_b_ptr),
        @ptrCast(&bias_b_ptr),
        @ptrCast(&weight_c_ptr),
        @ptrCast(&bias_c_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    const count = try checkedMul(try checkedMul(rows, out_dim), 3);
    if (tiled) {
        try launchBlocks(ctx, function, count, 256, &params);
    } else {
        try launch1d(ctx, function, count, &params);
    }
}

fn validateQ4KBuffers(
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    bias: ?cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
    const row_blocks = in_dim / q4_k_values_per_block;
    try checkF32Bytes(output, try checkedMul(rows, out_dim));
    try checkF32Bytes(input, try checkedMul(rows, in_dim));
    try checkRawBytes(weight, try checkedMul(try checkedMul(out_dim, row_blocks), q4_k_block_bytes));
    if (bias) |bias_buf| try checkF32Bytes(bias_buf, out_dim);
}

fn validateQ8Buffers(
    output: cuda_buffer.DeviceBuffer,
    input: cuda_buffer.DeviceBuffer,
    weight: cuda_buffer.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) cuda_driver.Error!void {
    if (in_dim == 0 or in_dim % q8_0_values_per_block != 0) return error.InvalidCudaState;
    const row_blocks = in_dim / q8_0_values_per_block;
    try checkF32Bytes(output, try checkedMul(rows, out_dim));
    try checkF32Bytes(input, try checkedMul(rows, in_dim));
    try checkRawBytes(weight, try checkedMul(try checkedMul(out_dim, row_blocks), q8_0_block_bytes));
}

fn launch1d(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    count: usize,
    params: [*]?*anyopaque,
) cuda_driver.Error!void {
    const block: c_uint = 256;
    const grid = try toU32((count + block - 1) / block);
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        grid,
        1,
        1,
        block,
        1,
        1,
        0,
        ctx.stream,
        params,
        null,
    ));
}

fn launchBlocks(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    blocks: usize,
    threads: usize,
    params: [*]?*anyopaque,
) cuda_driver.Error!void {
    const grid = try toU32(blocks);
    const block = try toU32(threads);
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        grid,
        1,
        1,
        block,
        1,
        1,
        0,
        ctx.stream,
        params,
        null,
    ));
}

fn launch2d(
    ctx: *cuda_context.CudaContext,
    function: cuda_driver.CUfunction,
    grid_x: usize,
    grid_y: usize,
    threads: usize,
    params: [*]?*anyopaque,
) cuda_driver.Error!void {
    const gx = try toU32(grid_x);
    const gy = try toU32(grid_y);
    const block = try toU32(threads);
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        gx,
        gy,
        1,
        block,
        1,
        1,
        0,
        ctx.stream,
        params,
        null,
    ));
}

fn checkedMul(a: usize, b: usize) cuda_driver.Error!usize {
    return std.math.mul(usize, a, b) catch error.InvalidCudaState;
}

fn toU32(value: usize) cuda_driver.Error!u32 {
    if (value > std.math.maxInt(u32)) return error.InvalidCudaState;
    return @intCast(value);
}

fn checkF32Bytes(buffer: cuda_buffer.DeviceBuffer, count: usize) cuda_driver.Error!void {
    try checkRawBytes(buffer, try checkedMul(count, @sizeOf(f32)));
}

fn checkRawBytes(buffer: cuda_buffer.DeviceBuffer, byte_count: usize) cuda_driver.Error!void {
    if (byte_count > buffer.len) return error.InvalidCudaState;
}

fn fillInput(values: []f32) void {
    for (values, 0..) |*value, i| {
        const lane: f32 = @floatFromInt(i % 97);
        value.* = (lane - 48.0) * 0.005;
    }
}

fn fillBias(values: []f32) void {
    for (values, 0..) |*value, i| {
        const lane: f32 = @floatFromInt(i % 23);
        value.* = (lane - 11.0) * 0.001;
    }
}

fn fillQ4KWeights(bytes: []u8) void {
    var block_input: [q4_k_values_per_block]f32 = undefined;
    var offset: usize = 0;
    var block_index: usize = 0;
    while (offset < bytes.len) : ({
        offset += q4_k_block_bytes;
        block_index += 1;
    }) {
        for (&block_input, 0..) |*value, i| {
            const lane: f32 = @floatFromInt((i + block_index * 13) % 31);
            value.* = (lane - 15.0) * 0.01;
        }
        quant_codec.quantizeQ4_KBlock(&block_input, bytes[offset..][0..q4_k_block_bytes]);
    }
}

fn fillQ4_0Weights(bytes: []u8) void {
    var block_input: [q4_0_values_per_block]f32 = undefined;
    var offset: usize = 0;
    var block_index: usize = 0;
    while (offset < bytes.len) : ({
        offset += q4_0_block_bytes;
        block_index += 1;
    }) {
        for (&block_input, 0..) |*value, i| {
            const lane: f32 = @floatFromInt((i + block_index * 11) % 29);
            value.* = (lane - 14.0) * 0.01;
        }
        quant_codec.quantizeQ4_0Block(&block_input, bytes[offset..][0..q4_0_block_bytes]);
    }
}

fn fillQ4_0LmArgmaxWeights(bytes: []u8) void {
    const pattern_blocks = 29;
    var patterns: [pattern_blocks][q4_0_block_bytes]u8 = undefined;
    for (&patterns, 0..) |*pattern, block_index| {
        var block_input: [q4_0_values_per_block]f32 = undefined;
        for (&block_input, 0..) |*value, i| {
            const lane: f32 = @floatFromInt((i + block_index * 11) % pattern_blocks);
            value.* = (lane - 14.0) * 0.01;
        }
        quant_codec.quantizeQ4_0Block(&block_input, pattern);
    }
    var offset: usize = 0;
    var block_index: usize = 0;
    while (offset < bytes.len) : ({
        offset += q4_0_block_bytes;
        block_index += 1;
    }) {
        @memcpy(bytes[offset..][0..q4_0_block_bytes], &patterns[block_index % pattern_blocks]);
    }
}

fn fillQ6_KLmArgmaxWeights(bytes: []u8) void {
    const pattern_blocks = 29;
    std.debug.assert(bytes.len % q6_k_block_bytes == 0);
    var patterns: [pattern_blocks][q6_k_block_bytes]u8 = undefined;
    for (&patterns, 0..) |*pattern, block_index| {
        var block_input: [q6_k_values_per_block]f32 = undefined;
        for (&block_input, 0..) |*value, i| {
            const lane: f32 = @floatFromInt((i + block_index * 17) % pattern_blocks);
            value.* = (lane - 14.0) * 0.01;
        }
        quant_codec.quantizeQ6_KBlock(&block_input, pattern);
    }
    var offset: usize = 0;
    var block_index: usize = 0;
    while (offset < bytes.len) : ({
        offset += q6_k_block_bytes;
        block_index += 1;
    }) {
        @memcpy(bytes[offset..][0..q6_k_block_bytes], &patterns[block_index % pattern_blocks]);
    }
}

fn fillQ4_0WeightsAlt(bytes: []u8) void {
    var block_input: [q4_0_values_per_block]f32 = undefined;
    var offset: usize = 0;
    var block_index: usize = 0;
    while (offset < bytes.len) : ({
        offset += q4_0_block_bytes;
        block_index += 1;
    }) {
        for (&block_input, 0..) |*value, i| {
            const lane: f32 = @floatFromInt((i * 3 + block_index * 17) % 23);
            value.* = (lane - 11.0) * 0.012;
        }
        quant_codec.quantizeQ4_0Block(&block_input, bytes[offset..][0..q4_0_block_bytes]);
    }
}

fn fillQ8Weights(bytes: []u8) void {
    var block_input: [q8_0_values_per_block]f32 = undefined;
    var offset: usize = 0;
    var block_index: usize = 0;
    while (offset < bytes.len) : ({
        offset += q8_0_block_bytes;
        block_index += 1;
    }) {
        for (&block_input, 0..) |*value, i| {
            const lane: f32 = @floatFromInt((i + block_index * 7) % 37);
            value.* = (lane - 18.0) * 0.008;
        }
        quant_codec.quantizeQ8_0Block(&block_input, bytes[offset..][0..q8_0_block_bytes]);
    }
}

fn runFullTextEmbedBench(allocator: std.mem.Allocator, io: std.Io, cfg: Config, model_path: []const u8) !void {
    print("\nfull ClipCLAP text embed via termite embed --backend cuda: model={s} iters={d}\n", .{ model_path, cfg.full_iters });

    var total_ns: u64 = 0;
    for (0..cfg.full_iters) |iter| {
        const embed_args = [_][]const u8{
            model_path,
            "--backend",
            "cuda",
            "--text",
            cfg.text,
            "--print-timing",
        };
        const started = nowNs();
        try native_embed.main(allocator, io, &embed_args);
        const elapsed = nowNs() - started;
        total_ns += elapsed;
        print("full_text_embed_iter={d} elapsed_ms={d}\n", .{ iter, elapsed / std.time.ns_per_ms });
    }
    print("full_text_embed_avg_ms={d}\n", .{(total_ns / cfg.full_iters) / std.time.ns_per_ms});
}

fn nowNs() u64 {
    var timespec: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &timespec))) {
        .SUCCESS => return @intCast(@as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec),
        else => return 0,
    }
}
