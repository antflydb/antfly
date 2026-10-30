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

//! Renderers for the hand-tuned CUDA linear / matmul kernels in
//! inference_cuda_kernels.cu.
//!
//! Most of these kernels are `extern "C"` entry points over a shared
//! `__device__` template (`termite_q8_0_tile_rows_cols<...>`, `*_impl<...>`):
//! a table row names the kernel, its signature and the instantiation, and
//! `wrappers` renders the exact text. Kernels that are close variants of each
//! other are written once as a `Source` text with `$(var)` substitutions and
//! `?flag` lines (see `fill`). The output is meant to be character-identical
//! to the hand-written kernels it replaced, so performance cannot change.

const std = @import("std");

/// One `extern "C"` kernel that forwards to a device template.
pub const Wrapper = struct {
    name: []const u8,
    /// Parameters as `type name`.
    sig: []const []const u8,
    /// Callee with template arguments, e.g. `termite_q4k_tile_cols<4u, 1u>`.
    call: []const u8,
    /// Call arguments; null passes the signature through in order.
    args: ?[]const u8 = null,
    /// `) { call(...); }` on one line instead of a block.
    inline_body: bool = false,
    /// Statements before the call (block style only).
    pre: []const u8 = "",
    /// Wraps the call in `#if <guard>` / `#endif`.
    guard: ?[]const u8 = null,
    /// `//` comment lines written above the kernel.
    comment: []const u8 = "",
};

/// The name of a `type name` parameter.
pub fn paramName(comptime p: []const u8) []const u8 {
    const i = std.mem.lastIndexOfAny(u8, p, " *").?;
    return p[i + 1 ..];
}

fn join(comptime items: []const []const u8, comptime sep: []const u8) []const u8 {
    var out: []const u8 = "";
    for (items, 0..) |it, i| out = out ++ (if (i == 0) "" else sep) ++ it;
    return out;
}

fn passArgs(comptime sig: []const []const u8) []const u8 {
    var names: []const []const u8 = &.{};
    for (sig) |p| names = names ++ .{paramName(p)};
    return join(names, ", ");
}

/// A parameter list, one `    type name,` per line.
pub fn sigText(comptime sig: []const []const u8) []const u8 {
    var out: []const u8 = "";
    for (sig, 0..) |p, i| out = out ++ "    " ++ p ++ (if (i + 1 < sig.len) ",\n" else "\n");
    return out;
}

pub fn wrapper(comptime w: Wrapper) []const u8 {
    comptime {
        var out: []const u8 = w.comment ++ "extern \"C\" __global__ void " ++ w.name ++ "(\n" ++ sigText(w.sig);
        const call = w.call ++ "(" ++ (w.args orelse passArgs(w.sig)) ++ ");";
        if (w.inline_body) return out ++ ") { " ++ call ++ " }\n";
        out = out ++ ") {\n" ++ w.pre;
        if (w.guard) |g| return out ++ "#if " ++ g ++ "\n    " ++ call ++ "\n#endif\n}\n";
        return out ++ "    " ++ call ++ "\n}\n";
    }
}

/// Kernels separated by blank lines.
pub fn wrappers(comptime ws: []const Wrapper) []const u8 {
    comptime {
        @setEvalBranchQuota(10_000_000);
        var out: []const u8 = "";
        for (ws, 0..) |w, i| out = out ++ (if (i == 0) "" else "\n") ++ wrapper(w);
        return out;
    }
}

// Signature pieces.
const dst = "float* dst";
const input = "const float* input";
const weight = "const unsigned char* weight";
const bias = "const float* bias";
const residual = "const float* residual";
const rows = "unsigned int rows";
const in_dim = "unsigned int in_dim";
const out_dim = "unsigned int out_dim";
const shape = [_][]const u8{ rows, in_dim, out_dim };
const q8_input = "const unsigned char* q8_input";

/// Which epilogue inputs a linear kernel takes. `bias_residual_unused`
/// declares `residual` in the signature but passes nullptr.
pub const Epi = enum { none, bias, bias_residual, bias_residual_unused };

/// `dst = epilogue(input · weightᵀ)` kernels over a template taking
/// `(dst, input, weight, bias, residual, rows, in_dim, out_dim)`; missing
/// epilogue inputs are passed as nullptr.
pub fn linear(comptime name: []const u8, comptime call: []const u8, comptime w: []const u8, comptime epi: Epi) Wrapper {
    const has_bias = epi != .none;
    const has_res = epi == .bias_residual or epi == .bias_residual_unused;
    const sig = [_][]const u8{ dst, input, w } ++
        (if (has_bias) [_][]const u8{bias} else [_][]const u8{}) ++
        (if (has_res) [_][]const u8{residual} else [_][]const u8{}) ++ shape;
    const wname = paramName(w);
    return .{
        .name = name,
        .sig = &sig,
        .call = call,
        .args = "dst, input, " ++ wname ++ ", " ++
            (if (has_bias) "bias" else "nullptr") ++ ", " ++
            (if (epi == .bias_residual) "residual" else "nullptr") ++ ", rows, in_dim, out_dim",
    };
}

fn q8Linear(comptime name: []const u8, comptime call: []const u8, comptime epi: Epi) Wrapper {
    return linear(name, call, weight, epi);
}

// ---------------------------------------------------------------- f32 linear

pub const linear_f32_tile = [_]Wrapper{
    linear("termite_linear_bias_f32_tile4_r2", "termite_linear_bias_f32_tile_rows_cols<2u, 4u, 0u>", "const float* weight", .bias),
    linear("termite_linear_bias_relu_f32_tile4_r2", "termite_linear_bias_f32_tile_rows_cols<2u, 4u, 1u>", "const float* weight", .bias),
    linear("termite_linear_bias_gelu_f32_tile4_r2", "termite_linear_bias_f32_tile_rows_cols<2u, 4u, 2u>", "const float* weight", .bias),
    linear("termite_linear_bias_add_f32_tile4_r2", "termite_linear_bias_f32_tile_rows_cols<2u, 4u, 3u>", "const float* weight", .bias_residual),
};

/// Two or three f32 projections with bias of the same input, 4 columns x 2
/// rows per 256-thread block.
const multi_bias_f32_src =
    \\extern "C" __global__ void $(name)(
    \\    float* dst_a,
    \\    float* dst_b,
    \\?triple     float* dst_c,
    \\    const float* input,
    \\    const float* weight_a,
    \\    const float* bias_a,
    \\    const float* weight_b,
    \\    const float* bias_b,
    \\?triple     const float* weight_c,
    \\?triple     const float* bias_c,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    unsigned int col_tile = blockIdx.x * 4u;
    \\    unsigned int row_base = blockIdx.y * 2u;
    \\    unsigned int tid = threadIdx.x;
    \\    __shared__ float partial_a[2][4][256];
    \\    __shared__ float partial_b[2][4][256];
    \\?triple     __shared__ float partial_c[2][4][256];
    \\    float acc_a[2][4];
    \\    float acc_b[2][4];
    \\?triple     float acc_c[2][4];
    \\    #pragma unroll
    \\    for (unsigned int r = 0; r < 2u; ++r) {
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            acc_a[r][c] = 0.0f;
    \\            acc_b[r][c] = 0.0f;
    \\?triple             acc_c[r][c] = 0.0f;
    \\        }
    \\    }
    \\
    \\    for (unsigned int k = tid; k < in_dim; k += 256u) {
    \\        float x[2];
    \\        #pragma unroll
    \\        for (unsigned int r = 0; r < 2u; ++r) {
    \\            unsigned int row = row_base + r;
    \\            x[r] = row < rows ? input[row * in_dim + k] : 0.0f;
    \\        }
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (col < out_dim) {
    \\                float wa = weight_a[col * in_dim + k];
    \\                float wb = weight_b[col * in_dim + k];
    \\?triple                 float wc = weight_c[col * in_dim + k];
    \\                #pragma unroll
    \\                for (unsigned int r = 0; r < 2u; ++r) {
    \\                    acc_a[r][c] += x[r] * wa;
    \\                    acc_b[r][c] += x[r] * wb;
    \\?triple                     acc_c[r][c] += x[r] * wc;
    \\                }
    \\            }
    \\        }
    \\    }
    \\
    \\    #pragma unroll
    \\    for (unsigned int r = 0; r < 2u; ++r) {
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            partial_a[r][c][tid] = acc_a[r][c];
    \\            partial_b[r][c][tid] = acc_b[r][c];
    \\?triple             partial_c[r][c][tid] = acc_c[r][c];
    \\        }
    \\    }
    \\    __syncthreads();
    \\    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
    \\        if (tid < stride) {
    \\            #pragma unroll
    \\            for (unsigned int r = 0; r < 2u; ++r) {
    \\                #pragma unroll
    \\                for (unsigned int c = 0; c < 4u; ++c) {
    \\                    partial_a[r][c][tid] += partial_a[r][c][tid + stride];
    \\                    partial_b[r][c][tid] += partial_b[r][c][tid + stride];
    \\?triple                     partial_c[r][c][tid] += partial_c[r][c][tid + stride];
    \\                }
    \\            }
    \\        }
    \\        __syncthreads();
    \\    }
    \\    if (tid == 0u) {
    \\        #pragma unroll
    \\        for (unsigned int r = 0; r < 2u; ++r) {
    \\            unsigned int row = row_base + r;
    \\            if (row >= rows) continue;
    \\            #pragma unroll
    \\            for (unsigned int c = 0; c < 4u; ++c) {
    \\                unsigned int col = col_tile + c;
    \\                if (col >= out_dim) continue;
    \\                unsigned int out_idx = row * out_dim + col;
    \\                dst_a[out_idx] = partial_a[r][c][0] + bias_a[col];
    \\                dst_b[out_idx] = partial_b[r][c][0] + bias_b[col];
    \\?triple                 dst_c[out_idx] = partial_c[r][c][0] + bias_c[col];
    \\            }
    \\        }
    \\    }
    \\}
    \\
;

pub const multi_bias_f32 = fillAll(multi_bias_f32_src, .{
    .{ .name = "termite_linear_pair_bias_f32_tile4_r2", .triple = false },
    .{ .name = "termite_linear_triple_bias_f32_tile4_r2", .triple = true },
});

// ---------------------------------------------------------------- Q8_0

pub const q8_0_tile = [_]Wrapper{
    q8Linear("termite_linear_q8_0_f32_tile4_r2", "termite_q8_0_tile_rows_cols<2u, 4u, 0u>", .none),
    q8Linear("termite_linear_q8_0_bias_f32_tile4_r2", "termite_q8_0_tile_rows_cols<2u, 4u, 1u>", .bias_residual_unused),
    q8Linear("termite_linear_q8_0_bias_gelu_f32_tile4_r2", "termite_q8_0_tile_rows_cols<2u, 4u, 2u>", .bias),
    q8Linear("termite_linear_q8_0_bias_add_f32_tile4_r2", "termite_q8_0_tile_rows_cols<2u, 4u, 3u>", .bias_residual),
    q8Linear("termite_linear_q8_0_f32_fast_r2c8", "termite_q8_0_tile_rows_cols_fast<2u, 8u, 0u>", .none),
    q8Linear("termite_linear_q8_0_bias_f32_fast_r2c8", "termite_q8_0_tile_rows_cols_fast<2u, 8u, 1u>", .bias),
    q8Linear("termite_linear_q8_0_bias_gelu_f32_fast_r2c8", "termite_q8_0_tile_rows_cols_fast<2u, 8u, 2u>", .bias),
    q8Linear("termite_linear_q8_0_bias_add_f32_fast_r2c8", "termite_q8_0_tile_rows_cols_fast<2u, 8u, 3u>", .bias_residual),
    q8Linear("termite_linear_q8_0_f32_fast_r4c4", "termite_q8_0_tile_rows_cols_fast<4u, 4u, 0u>", .none),
    q8Linear("termite_linear_q8_0_bias_f32_fast_r4c4", "termite_q8_0_tile_rows_cols_fast<4u, 4u, 1u>", .bias),
    q8Linear("termite_linear_q8_0_bias_gelu_f32_fast_r4c4", "termite_q8_0_tile_rows_cols_fast<4u, 4u, 2u>", .bias),
    q8Linear("termite_linear_q8_0_bias_add_f32_fast_r4c4", "termite_q8_0_tile_rows_cols_fast<4u, 4u, 3u>", .bias_residual),
    .{ .name = "termite_linear_q8_0_f32_tile4", .sig = &linear_sig, .call = "termite_q8_0_tile_cols<4u>" },
};

const linear_sig = [_][]const u8{ dst, input, weight } ++ shape;

// ---------------------------------------------------------------- Q4_0

const q8_1_sig = [_][]const u8{ dst, q8_input, weight } ++ shape;
const e4b_sig = [_][]const u8{ dst, q8_input, weight };
const gated_down_sig = [_][]const u8{ dst, "const float* gate", "const float* up", weight } ++ shape ++ .{"unsigned int activation"};

fn passInline(comptime name: []const u8, comptime sig: []const []const u8, comptime call: []const u8) Wrapper {
    return .{ .name = name, .sig = sig, .call = call, .inline_body = true };
}

pub const q4_0_tile = [_]Wrapper{
    .{ .name = "termite_linear_q4_0_f32_tile4", .sig = &linear_sig, .call = "termite_q4_0_tile_cols<4u>" },
    passInline("termite_linear_q4_0_q8_1_f32_tile4", &q8_1_sig, "termite_linear_q4_0_q8_1_f32_tile4_impl<4u>"),
    passInline("termite_linear_q4_0_q8_1_f32_tile4_e4b_attn_2048", &e4b_sig, "termite_linear_q4_0_q8_1_f32_tile4_e4b_impl<64u, 4u>"),
    passInline("termite_linear_q4_0_q8_1_f32_tile4_e4b_attn_4096", &e4b_sig, "termite_linear_q4_0_q8_1_f32_tile4_e4b_impl<128u, 4u>"),
    passInline("termite_linear_q4_0_q8_1_f32_tile4_w8", &q8_1_sig, "termite_linear_q4_0_q8_1_f32_tile4_impl<8u>"),
    passInline("termite_linear_q4_0_q8_1_f32_tile4_w8_rows4", &q8_1_sig, "termite_linear_q4_0_q8_1_f32_tile4_w8_rows_impl<4u>"),
    passInline("termite_linear_q4_0_q8_1_f32_tile4_w8_rows8_c4", &q8_1_sig, "termite_linear_q4_0_q8_1_f32_tile4_w8_rows_impl<8u>"),
    passInline("termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down", &e4b_sig, "termite_linear_q4_0_q8_1_f32_tile4_e4b_impl<320u, 8u>"),
    passInline("termite_linear_q4_0_q8_1_f32_tile4_w10_e4b_down", &e4b_sig, "termite_linear_q4_0_q8_1_f32_tile4_e4b_impl<320u, 10u>"),
    .{ .name = "termite_linear_q4_0_f32_tile8", .sig = &linear_sig, .call = "termite_q4_0_tile_cols<8u>" },
    passInline("termite_linear_q4_0_gated_down_f32_tile4", &gated_down_sig, "termite_linear_q4_0_gated_down_f32_tile_impl<4u>"),
    passInline("termite_linear_q4_0_gated_down_f32_tile8", &gated_down_sig, "termite_linear_q4_0_gated_down_f32_tile_impl<8u>"),
    passInline("termite_linear_q4_0_gated_down_f32_tile16", &gated_down_sig, "termite_linear_q4_0_gated_down_f32_tile_impl<16u>"),
};

/// Pair / QKV projections and the argmax first stage over Q4_0 / Q6_K.
const pair_sig = [_][]const u8{ "float* dst_a", "float* dst_b", input, "const unsigned char* weight_a", "const unsigned char* weight_b" } ++ shape;
const pair_q8_1_sig = [_][]const u8{ "float* dst_a", "float* dst_b", q8_input, "const unsigned char* weight_a", "const unsigned char* weight_b" } ++ shape;
const pair_act_sig = [_][]const u8{ dst, q8_input, "const unsigned char* weight_gate", "const unsigned char* weight_up", "unsigned int activation" };
const argmax_sig = [_][]const u8{ "float* partial_values", "unsigned int* partial_indices", input, weight, "const int* suppress_token_ids" } ++ shape ++ .{"unsigned int suppress_count"};
const qkv_sig = [_][]const u8{ "float* dst_q", "float* dst_k", "float* dst_v", input, "const unsigned char* weight_q", "const unsigned char* weight_k", "const unsigned char* weight_v", rows, in_dim, "unsigned int q_out_dim", "unsigned int kv_out_dim" };

pub const q4_0_projections = [_]Wrapper{
    passInline("termite_linear_q4_0_pair_nobias_f32_tile4", &pair_sig, "termite_linear_q4_0_pair_nobias_f32_tile_impl<4u>"),
    passInline("termite_linear_q4_0_pair_nobias_q8_1_f32_tile4", &pair_q8_1_sig, "termite_linear_q4_0_pair_nobias_q8_1_f32_tile4_impl<4u>"),
    passInline("termite_linear_q4_0_pair_nobias_q8_1_f32_tile4_w8", &pair_q8_1_sig, "termite_linear_q4_0_pair_nobias_q8_1_f32_tile4_impl<8u>"),
    passInline("termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8_e4b_ffn", &pair_act_sig, "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w_impl<8u>"),
    passInline("termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn", &pair_act_sig, "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w_impl<5u>"),
    passInline("termite_linear_q4_0_pair_nobias_f32_tile8", &pair_sig, "termite_linear_q4_0_pair_nobias_f32_tile_impl<8u>"),
    passInline("termite_linear_q4_0_argmax_rows_stage1_tile4", &argmax_sig, "termite_linear_q4_0_argmax_rows_stage1_tile_impl<4u>"),
    passInline("termite_linear_q4_0_argmax_rows_stage1_tile16", &argmax_sig, "termite_linear_q4_0_argmax_rows_stage1_tile_impl<16u>"),
    passInline("termite_linear_q6_k_argmax_rows_stage1_tile4", &argmax_sig, "termite_linear_q6_k_argmax_rows_stage1_tile_impl<4u>"),
    passInline("termite_linear_q6_k_argmax_rows_stage1_tile8", &argmax_sig, "termite_linear_q6_k_argmax_rows_stage1_tile_impl<8u>"),
    passInline("termite_linear_q6_k_argmax_rows_stage1_tile16", &argmax_sig, "termite_linear_q6_k_argmax_rows_stage1_tile_impl<16u>"),
    passInline("termite_linear_q4_0_qkv_nobias_f32_tile4", &qkv_sig, "termite_linear_q4_0_qkv_nobias_f32_tile_impl<4u>"),
    passInline("termite_linear_q4_0_qkv_nobias_f32_tile8", &qkv_sig, "termite_linear_q4_0_qkv_nobias_f32_tile_impl<8u>"),
};

// ---------------------------------------------------------------- tensor cores

const packed_weight = "const unsigned char* packed_weight";

fn tc(comptime quant: []const u8, comptime epi_name: []const u8, comptime mode: []const u8, comptime fmt: []const u8, comptime epi: Epi) Wrapper {
    return linear("termite_linear_" ++ quant ++ epi_name ++ "_f32_tc_hmma", "termite_qtc_hmma_tile<" ++ mode ++ ", TERMITE_QTC_FMT_" ++ fmt ++ ">", packed_weight, epi);
}

/// The four Q4_0 epilogues on BF16 tensor cores (sm_80+).
fn tcBf16(comptime epi_name: []const u8, comptime mode: []const u8, comptime epi: Epi) Wrapper {
    var w = linear("termite_linear_q4_0" ++ epi_name ++ "_f32_tc_hmma_bf16", "termite_qtc_hmma_tile<" ++ mode ++ ", TERMITE_QTC_FMT_Q4_0, __nv_bfloat16>", packed_weight, epi);
    w.guard = "__CUDA_ARCH__ >= 800";
    return w;
}

/// `count` projections sharing one input, picked by blockIdx.z, each with
/// its own dst / packed weight / bias, all with the bias epilogue.
fn tcProjections(comptime name: []const u8, comptime count: usize) Wrapper {
    const suffix = [_][]const u8{ "_a", "_b", "_c" };
    var sig: []const []const u8 = &.{};
    for (suffix[0..count]) |s| sig = sig ++ .{"float* dst" ++ s};
    sig = sig ++ .{input};
    for (suffix[0..count]) |s| sig = sig ++ .{ packed_weight ++ s, bias ++ s };
    sig = sig ++ shape;
    var pre: []const u8 = "    unsigned int projection = blockIdx.z;\n";
    for ([_][2][]const u8{ .{ "float* ", "dst" }, .{ "const unsigned char* ", "packed_weight" }, .{ "const float* ", "bias" } }) |v| {
        pre = pre ++ "    " ++ v[0] ++ v[1] ++ " = " ++ select(v[1], suffix[0..count]) ++ ";\n";
    }
    return .{
        .name = name,
        .sig = sig,
        .call = "termite_qtc_hmma_tile<1u, TERMITE_QTC_FMT_Q4_K>",
        .args = "dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim",
        .pre = pre,
    };
}

/// `projection == 0u ? x_a : (projection == 1u ? x_b : x_c)`.
fn select(comptime v: []const u8, comptime suffix: []const []const u8) []const u8 {
    return selectFrom(v, suffix, 0);
}

fn selectFrom(comptime v: []const u8, comptime suffix: []const []const u8, comptime i: usize) []const u8 {
    if (i + 1 == suffix.len) return v ++ suffix[i];
    const rest = selectFrom(v, suffix, i + 1);
    return std.fmt.comptimePrint("projection == {d}u ? ", .{i}) ++ v ++ suffix[i] ++ " : " ++
        (if (i + 2 < suffix.len) "(" ++ rest ++ ")" else rest);
}

pub const tc_hmma = [_]Wrapper{
    tc("q8_0", "", "0u", "Q8_0", .none),
    tc("q8_0", "_bias", "1u", "Q8_0", .bias),
    tc("q8_0", "_bias_gelu", "2u", "Q8_0", .bias),
    tc("q8_0", "_bias_add", "3u", "Q8_0", .bias_residual),
    tc("q4_k", "", "0u", "Q4_K", .none),
    tc("q4_k", "_bias", "1u", "Q4_K", .bias),
    tc("q4_k", "_bias_gelu", "2u", "Q4_K", .bias),
    tc("q4_k", "_bias_add", "3u", "Q4_K", .bias_residual),
    tc("q4_k", "_bias_quick_gelu", "4u", "Q4_K", .bias),
    tc("q4_k", "_bias_relu", "5u", "Q4_K", .bias),
    tcProjections("termite_linear_q4_k_triple_bias_f32_tc_hmma", 3),
    tcProjections("termite_linear_q4_k_pair_bias_f32_tc_hmma", 2),
    tc("q4_0", "", "0u", "Q4_0", .none),
    tc("q4_0", "_bias", "1u", "Q4_0", .bias),
    tc("q4_0", "_bias_gelu", "2u", "Q4_0", .bias),
    tc("q4_0", "_bias_add", "3u", "Q4_0", .bias_residual),
    blk: {
        var w = tcBf16("", "0u", .none);
        w.comment =
            \\// BF16 tensor-core mirror of the Q4_0 tc_hmma linear set. Same signatures and
            \\// launch geometry as the f16 kernels above (block=256, grid.x=ceil(out_dim/32),
            \\// grid.y=ceil(rows/64), no dynamic smem); the only difference is the WMMA
            \\// element type. Consumes the identical q4_0_hmma packed layout. bf16 has f32's
            \\// exponent range, so these stay numerically close to the exact f32 mirror even
            \\// when Gemma activations exceed f16's 65504 limit.
            \\
        ;
        break :blk w;
    },
    tcBf16("_bias", "1u", .bias),
    tcBf16("_bias_gelu", "2u", .bias),
    tcBf16("_bias_add", "3u", .bias_residual),
};

// ---------------------------------------------------------------- Q4_K

fn q4k(comptime name: []const u8, comptime call: []const u8, comptime epi: Epi) Wrapper {
    return linear("termite_linear_q4_k" ++ name, call, weight, epi);
}

/// Q4_K span pairs: two projections (optionally of two inputs) per launch.
fn q4kPair(comptime name: []const u8, comptime targs: []const u8, comptime two_inputs: bool) Wrapper {
    const inputs: []const []const u8 = if (two_inputs) &.{ "const float* input_a", "const float* input_b" } else &.{input};
    return .{
        .name = "termite_linear_q4_k" ++ name,
        .sig = &[_][]const u8{ "float* dst_a", "float* dst_b" } ++ inputs ++
            [_][]const u8{ "const unsigned char* weight_a", "const float* bias_a", "const unsigned char* weight_b", "const float* bias_b" } ++ shape,
        .call = "termite_q4k_pair_tile_rows_cols<" ++ targs ++ ">",
        .args = "dst_a, dst_b, " ++ (if (two_inputs) "input_a, input_b" else "input, nullptr") ++ ", weight_a, bias_a, weight_b, bias_b, rows, in_dim, out_dim",
    };
}

pub const q4_k_tile = [_]Wrapper{
    q4k("_f32_tile4", "termite_q4k_tile_cols<4u, 0u>", .none),
    q4k("_f32_tile8", "termite_q4k_tile_cols<8u, 0u>", .none),
    q4k("_bias_f32_tile4", "termite_q4k_tile_cols<4u, 1u>", .bias),
    q4k("_bias_f32_tile4_r2", "termite_q4k_tile_rows_cols<2u, 4u, 1u>", .bias),
    q4k("_bias_gelu_f32_tile4_r2", "termite_q4k_tile_rows_cols<2u, 4u, 2u>", .bias),
    q4k("_bias_add_f32_tile4_r2", "termite_q4k_tile_rows_cols<2u, 4u, 3u>", .bias_residual),
    q4k("_bias_f32_fast_r2c8", "termite_q4k_tile_rows_cols_fast<2u, 8u, 1u>", .bias),
    q4k("_bias_gelu_f32_fast_r2c8", "termite_q4k_tile_rows_cols_fast<2u, 8u, 2u>", .bias),
    q4k("_bias_add_f32_fast_r2c8", "termite_q4k_tile_rows_cols_fast<2u, 8u, 3u>", .bias_residual),
    q4k("_bias_f32_fast_r4c4", "termite_q4k_tile_rows_cols_fast<4u, 4u, 1u>", .bias),
    q4k("_bias_gelu_f32_fast_r4c4", "termite_q4k_tile_rows_cols_fast<4u, 4u, 2u>", .bias),
    q4k("_bias_add_f32_fast_r4c4", "termite_q4k_tile_rows_cols_fast<4u, 4u, 3u>", .bias_residual),
    q4k("_span_bias_f32_tile8_r2", "termite_q4k_tile_rows_cols<2u, 8u, 1u>", .bias),
    q4k("_span_bias_relu_f32_tile8_r2", "termite_q4k_tile_rows_cols<2u, 8u, 4u>", .bias),
    q4k("_span_bias_f32_tile4_r8", "termite_q4k_tile_rows_cols<8u, 4u, 1u>", .bias),
    q4k("_span_bias_relu_f32_tile4_r8", "termite_q4k_tile_rows_cols<8u, 4u, 4u>", .bias),
    q4kPair("_span_pair_bias_f32_tile8_r2", "2u, 8u, false, false", false),
    q4kPair("_span_pair_bias_relu_f32_tile8_r2", "2u, 8u, true, false", false),
    q4kPair("_span_pair2_bias_f32_tile8_r2", "2u, 8u, false, true", true),
    q4k("_bias_quick_gelu_f32_tile4", "termite_q4k_tile_cols<4u, 2u>", .bias),
    q4k("_bias_relu_f32_tile4", "termite_q4k_tile_cols<4u, 4u>", .bias),
    q4k("_bias_relu_f32_tile4_r2", "termite_q4k_tile_rows_cols<2u, 4u, 4u>", .bias),
    q4k("_bias_add_f32_tile4", "termite_q4k_tile_cols<4u, 3u>", .bias_residual),
};

// ---------------------------------------------------------------- text variants

/// Renders `src` for one variant. A line starting with `?flag ` (or a bare
/// `?flag`, an empty line) is kept only when the bool field `flag` of
/// `vars` is true (`?!flag` when false), without the prefix and one space;
/// `$(name)` is replaced by the string field `name`.
pub fn fill(comptime src: []const u8, vars: anytype) []const u8 {
    comptime {
        @setEvalBranchQuota(100_000_000);
        var out: []const u8 = "";
        var lines = std.mem.splitScalar(u8, src, '\n');
        while (lines.next()) |raw| {
            var line = raw;
            if (line.len > 0 and line[0] == '?') {
                const sp = std.mem.indexOfScalar(u8, line, ' ') orelse line.len;
                const neg = line[1] == '!';
                const flag = line[(if (neg) 2 else 1)..sp];
                if (@field(vars, flag) == neg) continue;
                line = line[@min(sp + 1, line.len)..];
            }
            var i: usize = 0;
            while (std.mem.indexOfPos(u8, line, i, "$(")) |a| {
                const b = std.mem.indexOfScalarPos(u8, line, a, ')').?;
                out = out ++ line[i..a] ++ @field(vars, line[a + 2 .. b]);
                i = b + 1;
            }
            out = out ++ line[i..] ++ "\n";
        }
        return out[0 .. out.len - 1];
    }
}

/// `fill` for each variant, separated by blank lines.
pub fn fillAll(comptime src: []const u8, comptime variants: anytype) []const u8 {
    comptime {
        var out: []const u8 = "";
        for (variants, 0..) |v, i| out = out ++ (if (i == 0) "" else "\n") ++ fill(src, v);
        return out;
    }
}

// ---------------------------------------------------------------- embeddings

/// Quantized embedding rows gathered by token id (i64 or i32 ids), times a
/// scale, or added to a weighted `rhs`.
const embedding_src =
    \\extern "C" __global__ void $(name)(
    \\    float* dst,
    \\    const unsigned char* weight,
    \\    const $(id)* ids,
    \\?add     const float* rhs,
    \\    unsigned int total,
    \\    unsigned int dim,
    \\?!add     float scale
    \\?add     float lhs_scale,
    \\?add     float rhs_scale
    \\) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\    unsigned int count = total * dim;
    \\    if (idx >= count) return;
    \\    unsigned int out_row = idx / dim;
    \\    unsigned int col = idx - out_row * dim;
    \\    unsigned long long src_row = $(src_row);
    \\    unsigned int row_blocks = dim / $(block)u;
    \\    unsigned int block = col / $(block)u;
    \\    unsigned int $(sub) = col - block * $(block)u;
    \\?nibble     unsigned int q_offset = 2u + (lane & 15u);
    \\?nibble     unsigned int high_nibble = lane >> 4u;
    \\    const unsigned char* bp = weight + (src_row * row_blocks + block) * $(bytes)ull;
    \\    dst[idx] = $(value) * $(scale);
    \\}
    \\
;

const EmbedFormat = struct { name: []const u8, block: []const u8, sub: []const u8, bytes: []const u8, value: []const u8, nibble: bool = false };
const q4_k_fmt = EmbedFormat{ .name = "q4_k", .block = "256", .sub = "value_index", .bytes = "144", .value = "termite_q4k_value(bp, value_index)" };
const q6_k_fmt = EmbedFormat{ .name = "q6_k", .block = "256", .sub = "value_index", .bytes = "210", .value = "termite_q6k_value(bp, value_index)" };
const q8_0_fmt = EmbedFormat{ .name = "q8_0", .block = "32", .sub = "lane", .bytes = "34", .value = "termite_q8_0_value(bp, lane)" };
const q4_0_fmt = EmbedFormat{ .name = "q4_0", .block = "32", .sub = "lane", .bytes = "18", .value = "termite_q4_0_value_nibble(bp, q_offset, high_nibble)", .nibble = true };

fn embed(comptime f: EmbedFormat, comptime i32_ids: bool, comptime add: bool) struct {
    name: []const u8,
    id: []const u8,
    src_row: []const u8,
    block: []const u8,
    sub: []const u8,
    bytes: []const u8,
    value: []const u8,
    scale: []const u8,
    nibble: bool,
    add: bool,
} {
    return .{
        .name = "termite_embedding_" ++ (if (add) "add_weighted" else "lookup") ++ (if (i32_ids) "_i32_" else "_") ++ f.name ++ "_f32",
        .id = if (i32_ids) "int" else "long long",
        .src_row = if (i32_ids) "(unsigned long long)((unsigned int)ids[out_row])" else "(unsigned long long)ids[out_row]",
        .block = f.block,
        .sub = f.sub,
        .bytes = f.bytes,
        .value = f.value,
        .scale = if (add) "lhs_scale + rhs[idx] * rhs_scale" else "scale",
        .nibble = f.nibble,
        .add = add,
    };
}

pub const embedding = fillAll(embedding_src, .{
    embed(q4_k_fmt, false, false),
    embed(q6_k_fmt, false, false),
    embed(q8_0_fmt, false, false),
    embed(q4_0_fmt, false, false),
    embed(q4_k_fmt, true, false),
    embed(q6_k_fmt, true, false),
    embed(q6_k_fmt, true, true),
    embed(q8_0_fmt, true, false),
    embed(q4_0_fmt, true, false),
});

// ---------------------------------------------------------------- dense f32

/// One output element per thread, or one per 256-thread block with a
/// shared-memory tree; weights f32, bf16 or f16.
const dense_scalar_src =
    \\extern "C" __global__ void $(name)(
    \\$(sig)) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\    unsigned int total = rows * out_dim;
    \\    if (idx >= total) return;
    \\    unsigned int row = idx / out_dim;
    \\    unsigned int col = idx - row * out_dim;
    \\    float acc = $(init);
    \\    for (unsigned int k = 0; k < in_dim; ++k) {
    \\        acc += input[row * in_dim + k] * weight[col * in_dim + k];
    \\    }
    \\    dst[idx] = acc;
    \\}
    \\
;

const dense_tiled_src =
    \\$(comment)extern "C" __global__ void $(name)(
    \\$(sig)) {
    \\    unsigned int global = blockIdx.x;
    \\    unsigned int total = rows * out_dim;
    \\    if (global >= total) return;
    \\    unsigned int row = global / out_dim;
    \\    unsigned int col = global - row * out_dim;
    \\    unsigned int tid = threadIdx.x;
    \\    __shared__ float partial[256];
    \\    float acc = 0.0f;
    \\    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
    \\        acc += input[row * in_dim + i] * $(load_open)weight[col * in_dim + i]$(load_close);
    \\    }
    \\    partial[tid] = acc;
    \\    __syncthreads();
    \\    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
    \\        if (tid < stride) partial[tid] += partial[tid + stride];
    \\        __syncthreads();
    \\    }
    \\    if (tid == 0u) dst[global] = partial[0];
    \\}
    \\
;

const f32_weight = "const float* weight";
const u16_weight = "const unsigned short* weight";

pub const dense_f32 =
    fill(dense_scalar_src, .{ .name = "termite_linear_f32", .sig = sigText(&([_][]const u8{ dst, input, f32_weight } ++ shape)), .init = "0.0f" }) ++ "\n" ++
    fill(dense_scalar_src, .{ .name = "termite_linear_bias_f32", .sig = sigText(&([_][]const u8{ dst, input, f32_weight, bias } ++ shape)), .init = "bias[col]" }) ++ "\n" ++
    fillAll(dense_tiled_src, .{
        .{ .name = "termite_linear_f32_tiled", .sig = sigText(&([_][]const u8{ dst, input, f32_weight } ++ shape)), .comment = "", .load_open = "", .load_close = "" },
        .{ .name = "termite_linear_bf16_weight_f32_tiled", .sig = sigText(&([_][]const u8{ dst, input, u16_weight } ++ shape)), .comment = "", .load_open = "termite_bf16_to_f32(", .load_close = ")" },
        .{
            .name = "termite_linear_f16_weight_f32_tiled",
            .sig = sigText(&([_][]const u8{ dst, input, u16_weight } ++ shape)),
            .comment =
            \\// Compatibility route for native FP16 encoder weights when cuBLASLt cannot
            \\// produce or execute an algorithm for a newly introduced dense shape. This is
            \\// intentionally simple and universally shaped: qualified tensor-core plans
            \\// remain the fast path, while an unsupported heuristic degrades to correct
            \\// device-resident execution instead of failing the request.
            \\
            ,
            .load_open = "__half2float(__ushort_as_half(",
            .load_close = "))",
        },
    });

// ---------------------------------------------------------------- Q4_K rows

/// Q4_K linear without tiles: one output per thread (`scalar`) or per
/// 256-thread block (`tiled`), optionally over 2-3 projections that share
/// the input (`proj`, the projection picked from the flat index).
const q4_k_scalar_src =
    \\extern "C" __global__ void $(name)(
    \\$(sig)) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\?!proj     unsigned int total = rows * out_dim;
    \\?!proj     if (idx >= total) return;
    \\?!proj     unsigned int row = idx / out_dim;
    \\?!proj     unsigned int col = idx - row * out_dim;
    \\?proj     unsigned int plane = rows * out_dim;
    \\?proj     unsigned int total = plane * $(count)u;
    \\?proj     if (idx >= total) return;
    \\?proj     unsigned int projection = idx / plane;
    \\?proj     unsigned int local = idx - projection * plane;
    \\?proj     unsigned int row = local / out_dim;
    \\?proj     unsigned int col = local - row * out_dim;
    \\?proj $(select)
    \\    unsigned int row_blocks = in_dim / 256u;
    \\    float acc = $(init);
    \\    for (unsigned int block = 0; block < row_blocks; ++block) {
    \\        const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
    \\        for (unsigned int i = 0; i < 256u; ++i) {
    \\            acc += input[row * in_dim + block * 256u + i] * termite_q4k_value(bp, i);
    \\        }
    \\    }
    \\    dst[$(out)] = acc;
    \\}
    \\
;

const q4_k_tiled_src =
    \\extern "C" __global__ void $(name)(
    \\$(sig)) {
    \\?!proj     unsigned int out_idx = blockIdx.x;
    \\?!proj     unsigned int total = rows * out_dim;
    \\?!proj     if (out_idx >= total) return;
    \\?!proj     unsigned int row = out_idx / out_dim;
    \\?!proj     unsigned int col = out_idx - row * out_dim;
    \\?proj     unsigned int plane = rows * out_dim;
    \\?proj     unsigned int global = blockIdx.x;
    \\?proj     if (global >= plane * $(count)u) return;
    \\?proj     unsigned int projection = global / plane;
    \\?proj     unsigned int local = global - projection * plane;
    \\?proj     unsigned int row = local / out_dim;
    \\?proj     unsigned int col = local - row * out_dim;
    \\?proj $(select)
    \\    unsigned int row_blocks = in_dim / 256u;
    \\    unsigned int tid = threadIdx.x;
    \\    __shared__ float partial[256];
    \\    float acc = 0.0f;
    \\    if (tid < 256u) {
    \\        for (unsigned int block = 0; block < row_blocks; ++block) {
    \\            const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
    \\            acc += input[row * in_dim + block * 256u + tid] * termite_q4k_value(bp, tid);
    \\        }
    \\        partial[tid] = acc;
    \\    }
    \\    __syncthreads();
    \\    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
    \\        if (tid < stride) partial[tid] += partial[tid + stride];
    \\        __syncthreads();
    \\    }
    \\    if (tid == 0u) $(store)
    \\}
    \\
;

/// Signature and per-projection selects of a 2- or 3-way projection kernel.
fn projSig(comptime count: usize) []const u8 {
    const suffix = [_][]const u8{ "_a", "_b", "_c" };
    var sig: []const []const u8 = &.{};
    for (suffix[0..count]) |sx| sig = sig ++ .{"float* dst" ++ sx};
    sig = sig ++ .{input};
    for (suffix[0..count]) |sx| sig = sig ++ .{ weight ++ sx, bias ++ sx };
    return sigText(sig ++ shape);
}

fn projSelect(comptime count: usize) []const u8 {
    const suffix = [_][]const u8{ "_a", "_b", "_c" };
    return "    const unsigned char* weight = " ++ select("weight", suffix[0..count]) ++ ";\n" ++
        "    const float* bias = " ++ select("bias", suffix[0..count]) ++ ";\n" ++
        "    float* dst = " ++ select("dst", suffix[0..count]) ++ ";";
}

const q4_k_sig = sigText(&linear_sig);
const q4_k_bias_sig = sigText(&([_][]const u8{ dst, input, weight, bias } ++ shape));

pub const q4_k_rows =
    fill(q4_k_scalar_src, .{ .name = "termite_linear_q4_k_f32", .sig = q4_k_sig, .proj = false, .init = "0.0f", .out = "idx" }) ++ "\n" ++
    fill(q4_k_scalar_src, .{ .name = "termite_linear_q4_k_bias_f32", .sig = q4_k_bias_sig, .proj = false, .init = "bias[col]", .out = "idx" }) ++ "\n" ++
    fillAll(q4_k_tiled_src, .{
        .{ .name = "termite_linear_q4_k_f32_tiled", .sig = q4_k_sig, .proj = false, .store = "dst[out_idx] = partial[0];" },
        .{ .name = "termite_linear_q4_k_bias_f32_tiled", .sig = q4_k_bias_sig, .proj = false, .store = "dst[out_idx] = partial[0] + bias[col];" },
        .{ .name = "termite_linear_q4_k_bias_quick_gelu_f32_tiled", .sig = q4_k_bias_sig, .proj = false, .store =
        \\{
        \\        float x = partial[0] + bias[col];
        \\        dst[out_idx] = x / (1.0f + expf(-1.702f * x));
        \\    }
        },
    }) ++ "\n" ++
    fill(q4_k_scalar_src, .{ .name = "termite_linear_q4_k_triple_bias_f32", .sig = projSig(3), .proj = true, .count = "3", .select = projSelect(3), .init = "bias[col]", .out = "local" }) ++ "\n" ++
    fillAll(q4_k_tiled_src, .{
        .{ .name = "termite_linear_q4_k_triple_bias_f32_tiled", .sig = projSig(3), .proj = true, .count = "3", .select = projSelect(3), .store = "dst[local] = partial[0] + bias[col];" },
        .{ .name = "termite_linear_q4_k_pair_bias_f32_tiled", .sig = projSig(2), .proj = true, .count = "2", .select = projSelect(2), .store = "dst[local] = partial[0] + bias[col];" },
    });

// ---------------------------------------------------------------- K-quant tile4

/// Four output columns per block over 256-value Q4_K / Q6_K super-blocks,
/// optionally of a gated activation `act(gate) * up` instead of `input`.
const k_tile4_src =
    \\extern "C" __global__ void $(name)(
    \\$(sig)) {
    \\    unsigned int col_tile = blockIdx.x * 4u;
    \\    unsigned int row = blockIdx.y;
    \\    if (row >= rows) return;
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int row_blocks = in_dim / 256u;
    \\    __shared__ float partial[4][256];
    \\    float acc[4];
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;
    \\    if (tid < 256u) {
    \\        for (unsigned int block = 0; block < row_blocks; ++block) {
    \\            unsigned int input_idx = row * in_dim + block * 256u + tid;
    \\            float x = $(x);
    \\            #pragma unroll
    \\            for (unsigned int c = 0; c < 4u; ++c) {
    \\                unsigned int col = col_tile + c;
    \\                if (col < out_dim) {
    \\                    const unsigned char* bp = weight + (col * row_blocks + block) * $(bytes)u;
    \\                    acc[c] += x * $(value)(bp, tid);
    \\                }
    \\            }
    \\        }
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    \\    }
    \\    __syncthreads();
    \\    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
    \\        if (tid < stride) {
    \\            #pragma unroll
    \\            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
    \\        }
    \\        __syncthreads();
    \\    }
    \\    if (tid == 0u) {
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (col < out_dim) dst[row * out_dim + col] = partial[c][0];
    \\        }
    \\    }
    \\}
    \\
;

const gated_x = "termite_decoder_activation_f32(gate[input_idx], activation) * up[input_idx]";

pub const k_quant_tile4 = fillAll(k_tile4_src, .{
    .{ .name = "termite_linear_q6_k_f32_tile4", .sig = sigText(&linear_sig), .x = "input[input_idx]", .bytes = "210", .value = "termite_q6k_value" },
    .{ .name = "termite_linear_q4_k_gated_down_f32_tile4", .sig = sigText(&gated_down_sig), .x = gated_x, .bytes = "144", .value = "termite_q4k_value" },
    .{ .name = "termite_linear_q6_k_gated_down_f32_tile4", .sig = sigText(&gated_down_sig), .x = gated_x, .bytes = "210", .value = "termite_q6k_value" },
});

// ---------------------------------------------------------------- argmax stage 1

/// Fused linear + argmax first stage over 4-column tiles: the last row
/// (`termite_*_argmax_stage1_tile4`) or every row (`*_rows_*`, blockIdx.x =
/// row * col_tiles + tile), skipping suppressed token ids. One partial
/// (value, index) per block.
const argmax_stage1_src =
    \\extern "C" __global__ void $(name)(
    \\$(sig)) {
    \\    const unsigned int cols = 4u;
    \\?!rows     unsigned int tile = blockIdx.x;
    \\?rows     unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    \\?rows     unsigned int global_tile = blockIdx.x;
    \\?rows     unsigned int row = col_tiles == 0u ? 0u : global_tile / col_tiles;
    \\?rows     unsigned int tile = col_tiles == 0u ? 0u : global_tile - row * col_tiles;
    \\    unsigned int col_tile = tile * cols;
    \\    unsigned int tid = threadIdx.x;
    \\?!rows     unsigned int row = rows == 0u ? 0u : rows - 1u;
    \\    unsigned int row_blocks = in_dim / $(block)u;
    \\    __shared__ float partial[4][256];
    \\    float acc[4];
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;
    \\
    \\$(loop)    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    \\    __syncthreads();
    \\    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
    \\        if (tid < stride) {
    \\            #pragma unroll
    \\            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
    \\        }
    \\        __syncthreads();
    \\    }
    \\    if (tid == 0u) {
    \\        float best_value = -3.402823466e+38f;
    \\        unsigned int best_index = 0xffffffffu;
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            unsigned int col = col_tile + c;
    \\?!rows             if (col >= out_dim) continue;
    \\?rows             if (row >= rows || col >= out_dim) continue;
    \\            bool suppressed = false;
    \\            for (unsigned int j = 0; j < suppress_count; ++j) {
    \\                int token_id = suppress_token_ids[j];
    \\                if (token_id >= 0 && (unsigned int)token_id == col) {
    \\                    suppressed = true;
    \\                    break;
    \\                }
    \\            }
    \\            if (suppressed) continue;
    \\            float value = partial[c][0];
    \\            if (value > best_value || (value == best_value && col < best_index)) {
    \\                best_value = value;
    \\                best_index = col;
    \\            }
    \\        }
    \\        partial_values[$(tile)] = best_value;
    \\        partial_indices[$(tile)] = best_index;
    \\    }
    \\}
    \\
;

/// The input · weight accumulation into acc[4] of one row.
const q8_0_cols4_loop =
    \\for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
    \\    float x = input[row * in_dim + i];
    \\    unsigned int block = i / 32u;
    \\    unsigned int lane = i - block * 32u;
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) {
    \\        unsigned int col = col_tile + c;
    \\        if (col < out_dim) {
    \\            const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
    \\            acc[c] += x * termite_q8_0_value(bp, lane);
    \\        }
    \\    }
    \\}
    \\
;

const q4_k_cols4_loop =
    \\for (unsigned int block = 0; block < row_blocks; ++block) {
    \\    float x = input[row * in_dim + block * 256u + tid];
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) {
    \\        unsigned int col = col_tile + c;
    \\        if (col < out_dim) {
    \\            const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
    \\            acc[c] += x * termite_q4k_value(bp, tid);
    \\        }
    \\    }
    \\}
    \\
;

/// Prefixes every line of `text` with `prefix`.
fn indent(comptime prefix: []const u8, comptime text: []const u8) []const u8 {
    @setEvalBranchQuota(1_000_000);
    var out: []const u8 = "";
    var lines = std.mem.splitScalar(u8, text[0 .. text.len - 1], '\n');
    while (lines.next()) |l| out = out ++ prefix ++ l ++ "\n";
    return out;
}

fn argmaxStage1(comptime quant: []const u8, comptime block: []const u8, comptime loop: []const u8, comptime rows_variant: bool) struct {
    name: []const u8,
    sig: []const u8,
    rows: bool,
    block: []const u8,
    loop: []const u8,
    tile: []const u8,
} {
    return .{
        .name = "termite_linear_" ++ quant ++ (if (rows_variant) "_argmax_rows_stage1_tile4" else "_argmax_stage1_tile4"),
        .sig = sigText(&argmax_sig),
        .rows = rows_variant,
        .block = block,
        .loop = if (rows_variant) "    if (row < rows) {\n" ++ indent("        ", loop) ++ "    }\n" else indent("    ", loop),
        .tile = if (rows_variant) "global_tile" else "tile",
    };
}

pub const argmax_stage1 = fillAll(argmax_stage1_src, .{
    argmaxStage1("q8_0", "32", q8_0_cols4_loop, false),
    argmaxStage1("q4_k", "256", q4_k_cols4_loop, false),
    argmaxStage1("q8_0", "32", q8_0_cols4_loop, true),
    argmaxStage1("q4_k", "256", q4_k_cols4_loop, true),
});

// ---------------------------------------------------------------- Q4_0 cols4 warps

/// Four Q4_0 columns per block, block-strided over the input row, then a
/// warp sum over `warps` warps; the input is `input` or `act(gate) * up`.
/// The `slice` epilogue multiplies `act(y)` into a column slice of `source`.
const q4_0_cols4_src =
    \\extern "C" __global__ void $(name)(
    \\$(sig)) {
    \\    unsigned int col_tile = blockIdx.x * 4u;
    \\    unsigned int row = blockIdx.y;
    \\    if (row >= rows) return;
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int lane = tid & 31u;
    \\    unsigned int warp = tid >> 5;
    \\    unsigned int row_blocks = in_dim / 32u;
    \\    __shared__ float warp_partial[4][$(warps)];
    \\    float acc[4];
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;
    \\
    \\    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
    \\?!gated         float x = input[row * in_dim + i];
    \\?gated         unsigned int input_idx = row * in_dim + i;
    \\?gated         float x = termite_decoder_activation_f32(gate[input_idx], activation) * up[input_idx];
    \\        unsigned int block = i / 32u;
    \\        unsigned int lane = i - block * 32u;
    \\        unsigned int q_offset = 2u + (lane & 15u);
    \\        unsigned int high_nibble = lane >> 4u;
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (col < out_dim) {
    \\                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
    \\                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
    \\            }
    \\        }
    \\    }
    \\?!slice     termite_store_q4_0_cols_warp_sum_warps<4u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
    \\?slice
    \\?slice     #pragma unroll
    \\?slice     for (unsigned int c = 0; c < 4u; ++c) {
    \\?slice         float sum = termite_warp_reduce_sum(acc[c]);
    \\?slice         if (lane == 0u$(warp_guard)) warp_partial[c][warp] = sum;
    \\?slice     }
    \\?slice     __syncthreads();
    \\?slice     if (tid == 0u) {
    \\?slice         #pragma unroll
    \\?slice         for (unsigned int c = 0; c < 4u; ++c) {
    \\?slice             unsigned int col = col_tile + c;
    \\?slice             if (col < out_dim) {
    \\?slice                 float y = 0.0f;
    \\?slice                 #pragma unroll
    \\?slice                 for (unsigned int w = 0; w < $(warps)u; ++w) y += warp_partial[c][w];
    \\?slice                 dst[row * out_dim + col] =
    \\?slice                     termite_decoder_activation_f32(y, activation) *
    \\?slice                     source[row * source_cols + source_start + col];
    \\?slice             }
    \\?slice         }
    \\?slice     }
    \\}
    \\
;

const slice_sig = [_][]const u8{ dst, input, weight, "const float* source" } ++ shape ++
    .{ "unsigned int source_cols", "unsigned int source_start", "unsigned int activation" };

pub const q4_0_cols4 = fillAll(q4_0_cols4_src, .{
    .{ .name = "termite_linear_q4_0_f32_tile4_w4", .sig = sigText(&linear_sig), .warps = "4", .gated = false, .slice = false },
    .{ .name = "termite_linear_q4_0_activation_slice_last_dim_f32_tile4", .sig = sigText(&slice_sig), .warps = "8", .gated = false, .slice = true, .warp_guard = "" },
    .{ .name = "termite_linear_q4_0_activation_slice_last_dim_f32_tile4_w4", .sig = sigText(&slice_sig), .warps = "4", .gated = false, .slice = true, .warp_guard = " && warp < 4u" },
    .{ .name = "termite_linear_q4_0_gated_down_f32_tile4_w4", .sig = sigText(&gated_down_sig), .warps = "4", .gated = true, .slice = false },
});

// ---------------------------------------------------------------- Q8_1 activations

/// Rows of f32 (or `act(gate) * up`) quantized to Q8_1 blocks, one warp
/// per 32-value block.
const quantize_q8_1_src =
    \\extern "C" __global__ void $(name)(
    \\$(sig)) {
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int lane = tid & 31u;
    \\    unsigned int warp = tid >> 5;
    \\    unsigned int warps_per_block = blockDim.x >> 5;
    \\    unsigned int row_blocks = in_dim / 32u;
    \\    unsigned int q8_block = blockIdx.x * warps_per_block + warp;
    \\    unsigned int total_blocks = rows * row_blocks;
    \\    if (q8_block >= total_blocks) return;
    \\
    \\    unsigned int row = q8_block / row_blocks;
    \\    unsigned int block = q8_block - row * row_blocks;
    \\?!gated     float x = input[row * in_dim + block * 32u + lane];
    \\?gated     unsigned int index = row * in_dim + block * 32u + lane;
    \\?gated     float x = termite_decoder_activation_f32(gate[index], activation) * up[index];
    \\    float amax = termite_warp_reduce_max_f32(fabsf(x));
    \\    float d = amax > 0.0f ? amax / 127.0f : 0.0f;
    \\    int q = 0;
    \\    if (d > 0.0f) {
    \\        q = __float2int_rn(x / d);
    \\        q = max(-127, min(127, q));
    \\    }
    \\    unsigned char* bp = dst + q8_block * 36u;
    \\    bp[4u + lane] = (unsigned char)(signed char)q;
    \\    if (lane == 0u) {
    \\        termite_store_half_bytes(bp, d);
    \\        bp[2u] = 0u;
    \\        bp[3u] = 0u;
    \\    }
    \\}
    \\
;

pub const quantize_q8_1 = fillAll(quantize_q8_1_src, .{
    .{ .name = "termite_quantize_f32_q8_1_rows", .sig = sigText(&.{ "unsigned char* dst", input, rows, in_dim }), .gated = false },
    .{ .name = "termite_quantize_gated_f32_q8_1_rows", .sig = sigText(&.{ "unsigned char* dst", "const float* gate", "const float* up", rows, in_dim, "unsigned int activation" }), .gated = true },
});

/// `act(gate) * up` of two Q4_0 projections of one Q8_1 row, 4 columns per
/// block, `warps` warps.
const pair_act_q8_1_src =
    \\extern "C" __global__ void $(name)(
    \\$(sig)) {
    \\    const unsigned int cols = 4u;
    \\    unsigned int tiles = (out_dim + cols - 1u) / cols;
    \\    unsigned int global = blockIdx.x;
    \\    unsigned int row = global / tiles;
    \\    if (row >= rows) return;
    \\    unsigned int col_tile = (global - row * tiles) * cols;
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int lane = tid & 31u;
    \\    unsigned int warp = tid >> 5;
    \\    unsigned int row_blocks = in_dim / 32u;
    \\?early     bool full_tile = col_tile + 3u < out_dim;
    \\    __shared__ float gate_partial[4][$(warps)];
    \\    __shared__ float up_partial[4][$(warps)];
    \\    float gate_acc[4];
    \\    float up_acc[4];
    \\?!early     bool full_tile = col_tile + 3u < out_dim;
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) {
    \\        gate_acc[c] = 0.0f;
    \\        up_acc[c] = 0.0f;
    \\    }
    \\
    \\    unsigned int iqs = (tid & 1u) * 2u;
    \\    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
    \\        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
    \\        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
    \\        float q8_d = termite_half_to_float(q8_d_h);
    \\        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
    \\        unsigned int q8_base0 = iqs * 4u;
    \\        unsigned int q8_base1 = q8_base0 + 4u;
    \\        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
    \\        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
    \\        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
    \\        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);
    \\
    \\        if (full_tile) {
    \\            #pragma unroll
    \\            for (unsigned int c = 0u; c < 4u; ++c) {
    \\                unsigned int col = col_tile + c;
    \\                const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
    \\                const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
    \\                gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
    \\                up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
    \\            }
    \\        } else {
    \\            #pragma unroll
    \\            for (unsigned int c = 0u; c < 4u; ++c) {
    \\                unsigned int col = col_tile + c;
    \\                if (col < out_dim) {
    \\                    const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
    \\                    const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
    \\                    gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
    \\                    up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
    \\                }
    \\            }
    \\        }
    \\    }
    \\
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) {
    \\        float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
    \\        float up_sum = termite_warp_reduce_sum(up_acc[c]);
    \\        if (lane == 0u && warp < $(warps)u) {
    \\            gate_partial[c][warp] = gate_sum;
    \\            up_partial[c][warp] = up_sum;
    \\        }
    \\    }
    \\    __syncthreads();
    \\    if (tid == 0u) {
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (full_tile || col < out_dim) {
    \\                float gate_y = 0.0f;
    \\                float up_y = 0.0f;
    \\                #pragma unroll
    \\                for (unsigned int w = 0; w < $(warps)u; ++w) {
    \\                    gate_y += gate_partial[c][w];
    \\                    up_y += up_partial[c][w];
    \\                }
    \\                dst[row * out_dim + col] = termite_decoder_activation_f32(gate_y, activation) * up_y;
    \\            }
    \\        }
    \\    }
    \\}
    \\
;

const pair_act_rows_sig = [_][]const u8{ dst, q8_input, "const unsigned char* weight_gate", "const unsigned char* weight_up" } ++ shape ++ .{"unsigned int activation"};

pub const pair_activation_q8_1 = fillAll(pair_act_q8_1_src, .{
    .{ .name = "termite_linear_q4_0_pair_activation_q8_1_f32_tile4", .sig = sigText(&pair_act_rows_sig), .warps = "4", .early = false },
    .{ .name = "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8", .sig = sigText(&pair_act_rows_sig), .warps = "8", .early = true },
});

// ---------------------------------------------------------------- pair / QKV projections

/// Two projections of one f32 row, 4 columns per 256-thread block
/// (blockIdx.x = (row * 2 + projection) * tiles + tile), tree-reduced.
const pair_cols4_src =
    \\extern "C" __global__ void $(name)(
    \\$(sig)) {
    \\    const unsigned int cols = 4u;
    \\    unsigned int tiles = (out_dim + cols - 1u) / cols;
    \\    unsigned int tiles_per_row = tiles * 2u;
    \\    unsigned int global = blockIdx.x;
    \\    unsigned int row = global / tiles_per_row;
    \\    if (row >= rows) return;
    \\    unsigned int tile_local = global - row * tiles_per_row;
    \\    unsigned int projection = tile_local / tiles;
    \\    unsigned int col_tile = (tile_local - projection * tiles) * cols;
    \\    const unsigned char* weight = projection == 0u ? weight_a : weight_b;
    \\    float* dst = projection == 0u ? dst_a : dst_b;
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int row_blocks = in_dim / $(block)u;
    \\    __shared__ float partial[4][256];
    \\    float acc[4];
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;
    \\
    \\$(loop)    __syncthreads();
    \\    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
    \\        if (tid < stride) {
    \\            #pragma unroll
    \\            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
    \\        }
    \\        __syncthreads();
    \\    }
    \\    if (tid == 0u) {
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (col < out_dim) dst[row * out_dim + col] = partial[c][0];
    \\        }
    \\    }
    \\}
    \\
;

const partial_store =
    \\#pragma unroll
    \\for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    \\
;

pub const pair_nobias_cols4 = fillAll(pair_cols4_src, .{
    .{ .name = "termite_linear_q8_0_pair_nobias_f32_tile4", .sig = sigText(&pair_sig), .block = "32", .loop = indent("    ", q8_0_cols4_loop ++ partial_store) },
    .{ .name = "termite_linear_q4_k_pair_nobias_f32_tile4", .sig = sigText(&pair_sig), .block = "256", .loop = "    if (tid < 256u) {\n" ++ indent("        ", q4_k_cols4_loop ++ partial_store) ++ "    }\n" },
});

/// Q, K and V projections of one Q8_1 row: tiles of `cols` columns over
/// q_out_dim then twice kv_out_dim, a `cols`-warp sum per tile.
const qkv_q8_1_src =
    \\extern "C" __global__ void $(name)(
    \\$(sig)) {
    \\    const unsigned int cols = $(cols)u;
    \\    unsigned int q_tiles = (q_out_dim + cols - 1u) / cols;
    \\    unsigned int kv_tiles = (kv_out_dim + cols - 1u) / cols;
    \\    unsigned int tiles_per_row = q_tiles + kv_tiles * 2u;
    \\    unsigned int global = blockIdx.x;
    \\    unsigned int row = global / tiles_per_row;
    \\    if (row >= rows) return;
    \\    unsigned int tile = global - row * tiles_per_row;
    \\
    \\    const unsigned char* weight;
    \\    float* dst;
    \\    unsigned int out_dim;
    \\    unsigned int col_tile;
    \\    if (tile < q_tiles) {
    \\        weight = weight_q;
    \\        dst = dst_q;
    \\        out_dim = q_out_dim;
    \\        col_tile = tile * cols;
    \\    } else if (tile < q_tiles + kv_tiles) {
    \\        weight = weight_k;
    \\        dst = dst_k;
    \\        out_dim = kv_out_dim;
    \\        col_tile = (tile - q_tiles) * cols;
    \\    } else {
    \\        weight = weight_v;
    \\        dst = dst_v;
    \\        out_dim = kv_out_dim;
    \\        col_tile = (tile - q_tiles - kv_tiles) * cols;
    \\    }
    \\
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int lane = tid & 31u;
    \\    unsigned int warp = tid >> 5;
    \\    unsigned int row_blocks = in_dim / 32u;
    \\    __shared__ float warp_partial[$(cols)][$(cols)];
    \\    float acc[$(cols)];
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < $(bound); ++c) acc[c] = 0.0f;
    \\
    \\    unsigned int iqs = (tid & 1u) * 2u;
    \\    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
    \\        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
    \\        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
    \\        float q8_d = termite_half_to_float(q8_d_h);
    \\        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
    \\        unsigned int q8_base0 = iqs * 4u;
    \\        unsigned int q8_base1 = q8_base0 + 4u;
    \\        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
    \\        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
    \\        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
    \\        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);
    \\
    \\        #pragma unroll
    \\        for (unsigned int c = 0u; c < $(bound); ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (col < out_dim) {
    \\                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
    \\                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
    \\            }
    \\        }
    \\    }
    \\    termite_store_q4_0_cols_warp_sum_warps<$(cols)u, $(cols)u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
    \\}
    \\
;

const qkv_q8_1_sig = [_][]const u8{ "float* dst_q", "float* dst_k", "float* dst_v", q8_input, "const unsigned char* weight_q", "const unsigned char* weight_k", "const unsigned char* weight_v", rows, in_dim, "unsigned int q_out_dim", "unsigned int kv_out_dim" };

pub const qkv_q8_1 = fillAll(qkv_q8_1_src, .{
    .{ .name = "termite_linear_q4_0_qkv_nobias_q8_1_f32_tile4", .sig = sigText(&qkv_q8_1_sig), .cols = "4", .bound = "4u" },
    .{ .name = "termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8", .sig = sigText(&qkv_q8_1_sig), .cols = "8", .bound = "cols" },
});

// ---------------------------------------------------------------- BSHD <-> BHSD

/// EmbeddingGemma attention layout moves between [batch, seq, heads, dim]
/// and [batch, heads, seq, dim] (embedding_gemma2.cuh), one element per
/// thread: `pack` gathers from BSHD, `unpack` scatters to it.
const bshd_src =
    \\extern "C" __global__ void $(name)(
    \\    $(out)* out, const $(in)* input, unsigned int batch,
    \\    unsigned int sequence, unsigned int heads, unsigned int dim) {
    \\    size_t count = (size_t)batch * sequence * heads * dim;
    \\    size_t i = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    \\    if (i >= count) return;
    \\    unsigned int d = i % dim;
    \\    size_t t = i / dim;
    \\    unsigned int position = t % sequence;
    \\    t /= sequence;
    \\    unsigned int head = t % heads;
    \\    unsigned int b = t / heads;
    \\    size_t $(index) = (((size_t)b * sequence + position) * heads + head) * dim + d;
    \\    $(store);
    \\}
    \\
;

pub const bshd = fillAll(bshd_src, .{
    .{ .name = "termite_embedding_gemma2_pack_bshd_bf16_f32", .out = "float", .in = "__nv_bfloat16", .index = "source", .store = "out[i] = __bfloat162float(input[source])" },
    .{ .name = "termite_embedding_gemma2_pack_bshd_f32", .out = "float", .in = "float", .index = "source", .store = "out[i] = input[source]" },
    .{ .name = "termite_embedding_gemma2_unpack_bhsd_f32", .out = "float", .in = "float", .index = "destination", .store = "out[destination] = input[i]" },
    .{ .name = "termite_embedding_gemma2_unpack_bhsd_f32_bf16", .out = "__nv_bfloat16", .in = "float", .index = "destination", .store = "out[destination] = __float2bfloat16_rn(input[i])" },
});
