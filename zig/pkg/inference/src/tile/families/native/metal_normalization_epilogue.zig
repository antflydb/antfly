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

const program = @import("../../native_program.zig");
const tile = @import("antfly_tile");
const f = tile.fexpr;
fn emit(comptime expr: f.FExpr, comptime value: []const u8, comptime bias: []const u8, comptime gate: []const u8, comptime residual: []const u8) f.Emission {
    return .{ .expr = expr, .source = tile.native_math.metal, .inputs = .{ .value = value, .bias = bias, .gate = gate, .residual = residual } };
}
pub const source_0 = program.metal(program.render(
    \\template<uint Mode> inline float termite_layer_norm_load(device const float *input, device const float *other, device const float *bias, uint index, uint column) {
    \\    if (Mode == 2u) return input[index] + bias[column] + other[index];
    \\    if (Mode == 1u) return input[index] + other[index];
    \\    return input[index];
    \\}
    \\template<uint Mode, bool SaveSum, bool Rows> inline void termite_layer_norm_family(device const float *input, device const float *other, device const float *bias, device const float *gamma, device const float *beta, device float *sum_out, device float *output, constant termite_metal_apply_layer_norm_params &p, threadgroup float *scratch, uint row, uint tid, uint width) {
    \\    if (p.hidden_size == 0u || (Rows && (width == 0u || tid >= 256u))) return;
    \\    uint row_base = row * p.hidden_size; uint first = Rows ? tid : 0u; uint step = Rows ? width : 1u;
    \\    float sum = 0.0f; float sq = 0.0f;
    \\    for (uint i = first; i < p.hidden_size; i += step) { float v = termite_layer_norm_load<Mode>(input, other, bias, row_base + i, i); if (SaveSum) sum_out[row_base + i] = v; sum += v; if (Rows) sq += v * v; }
    \\    float mean; float variance;
    \\    if (Rows) {
    \\        threadgroup float *sums = scratch; threadgroup float *sqs = scratch + 256u;
    \\        sums[tid] = sum; sqs[tid] = sq; threadgroup_barrier(mem_flags::mem_threadgroup);
    \\        for (uint stride = width >> 1; stride > 0u; stride >>= 1u) { if (tid < stride && tid + stride < 256u) { sums[tid] += sums[tid + stride]; sqs[tid] += sqs[tid + stride]; } threadgroup_barrier(mem_flags::mem_threadgroup); }
    \\        mean = sums[0] / float(p.hidden_size); variance = max(sqs[0] / float(p.hidden_size) - mean * mean, 0.0f);
    \\    } else {
    \\        mean = sum / float(p.hidden_size); variance = 0.0f;
    \\        for (uint i = 0u; i < p.hidden_size; ++i) { float v = SaveSum ? sum_out[row_base + i] : termite_layer_norm_load<Mode>(input, other, bias, row_base + i, i); float centered = v - mean; variance += centered * centered; }
    \\        variance /= float(p.hidden_size);
    \\    }
    \\    float inv_std = rsqrt(variance + p.eps);
    \\    for (uint i = first; i < p.hidden_size; i += step) { float v = SaveSum ? sum_out[row_base + i] : termite_layer_norm_load<Mode>(input, other, bias, row_base + i, i); output[row_base + i] = {{affine}}; }
    \\}
    \\
, .{
    .affine = emit(tile.native_math.layer_affine, "(v - mean)", "gamma[i]", "inv_std", "beta[i]"),
}));
pub const source_1 = program.metal(program.render(
    \\template<typename Input, bool Precomputed>
    \\__attribute__((always_inline)) inline float antfly_rms_tree_inverse(device const Input*input,uint base,uint hidden,float eps,threadgroup float*shmem,uint tid,uint tpg,device const float*sumsq,uint partial_base) {
    \\    float mean_square = 0.0f;
    \\    if (Precomputed) { for (uint i = tid; i < (hidden + 7u) / 8u; i += tpg) mean_square += sumsq[partial_base + i]; } else { for (uint i = tid; i < hidden; i += tpg) { float x = float(input[base + i]); mean_square += x * x; } }
    \\    shmem[tid] = mean_square; threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    for (uint stride = tpg >> 1; stride > 0; stride >>= 1) { if (tid < stride) shmem[tid] += shmem[tid + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); }
    \\    mean_square = shmem[0] / float(hidden); return rsqrt(mean_square + eps);
    \\}
    \\template<bool Add, bool Scale, typename Input = float, bool Precomputed = false, bool ExpressionEpilogue = false> __attribute__((always_inline)) inline void termite_rms_norm_tree(device const Input *input, device const float *weight, device const float *residual, device float *output, threadgroup float *shmem, uint base, uint hidden, float eps, float scale, uint tid, uint tpg, device const float *sumsq = nullptr, uint partial_base = 0u) {
    \\    if (hidden == 0u) return;
    \\    float inv_rms = antfly_rms_tree_inverse<Input, Precomputed>(input, base, hidden, eps, shmem, tid, tpg, sumsq, partial_base);
    \\    for (uint i = tid; i < hidden; i += tpg) { if (ExpressionEpilogue) { if (Scale) output[base + i] = ({{normalized}} + residual[base + i]) * scale; else output[base + i] = {{normalized}} + residual[base + i]; } else { float value = {{normalized}}; if (Add) value = value + residual[base + i]; if (Scale) value = value * scale; output[base + i] = value; } }
    \\}
    \\
, .{
    .normalized = f.binding(tile.native_math.rms(.scale_then_weight), tile.native_math.metal, .{ .value = "float(input[base + i])", .weight = "weight[i]", .inverse = "inv_rms" }),
}));
pub const source_2 = program.metal(program.render(
    \\template<bool Clamp> inline void termite_rms_norm_simd4_impl(
    \\    device const float *input, device const float *weight, device float *output,
    \\    uint rows, uint dim, float eps, uint flags, float lo, float hi,
    \\    threadgroup float *shmem, uint tid, uint tpg, uint simd_group, uint simd_lane, uint row) {
    \\    if (row >= rows || dim == 0u || (dim & 3u) != 0u) return;
    \\    uint row_base = row * dim;
    \\    uint vectors = dim >> 2u;
    \\    float inv_rms = termite_rms_norm_simd4_inverse(input, row_base, dim, eps, shmem, tid, tpg, simd_group, simd_lane);
    \\    for (uint v = tid; v < vectors; v += tpg) {
    \\        uint j = v << 2u;
    \\        uint i = row_base + j;
    \\        float4 x = float4(input[i], input[i + 1u], input[i + 2u], input[i + 3u]);
    \\        float4 w = float4(weight[j], weight[j + 1u], weight[j + 2u], weight[j + 3u]);
    \\        float4 y = {{normalized}};
    \\        if (Clamp) { if (flags & 1u) y = max(y, float4(lo)); if (flags & 2u) y = min(y, float4(hi)); }
    \\        output[i] = y.x; output[i + 1u] = y.y; output[i + 2u] = y.z; output[i + 3u] = y.w;
    \\    }
    \\}
    \\
, .{
    .normalized = f.binding(tile.native_math.rms(.weighted_inverse), tile.native_math.metal, .{ .value = "x", .weight = "w", .inverse = "inv_rms" }),
}));
