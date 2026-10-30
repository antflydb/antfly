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

//! Kernels that exist as an f32 and an f16 flavour of the same schedule,
//! differing only in the activation input or output element type. Each is
//! written once with holes for the name, the element type and its
//! conversion.

const msl = @import("quant_metal_msl.zig");
const fill = msl.fill;

/// Q8_0 matmul for small batches: 8 x 4 lanes per simdgroup, two output
/// rows per lane group, float4 activation chunks.
const q8_0_linear_mm =
    \\kernel void ${name}(device const ${in} *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_linear_params &p [[buffer(3)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
    \\    weight += p.weight_offset;
    \\    const uint NX = 8u; const uint NY = 4u; const uint NSG = 4u; const uint RPTG = 8u; const uint CHPT = 4u; const uint OPG = 2u;
    \\    uint tx = uint(lane) & 7u; uint ty = uint(lane) >> 3; uint first_o = (tg.x * (NY * NSG) + uint(sgitg) * NY + ty) * OPG; uint first_r = tg.y * RPTG; if (first_o >= p.out_dim) return;
    \\    float acc0[8]; float acc1[8]; for (uint i = 0u; i < RPTG; ++i) { acc0[i] = 0.0f; acc1[i] = 0.0f; }
    \\    uint row_offset0 = first_o * p.row_blocks * 34u; uint row_offset1 = row_offset0 + p.row_blocks * 34u; uint total_chunks = p.row_blocks * 8u; bool has_o1 = first_o + 1u < p.out_dim;
    \\    for (uint chunk = tx; chunk < total_chunks; chunk += CHPT * NX) { for (uint ch = 0u; ch < CHPT; ++ch) { uint c = chunk + ch * NX; if (c >= total_chunks) continue; uint b = c >> 3; uint ci = c & 7u; uint off0 = row_offset0 + b * 34u; ushort bits0 = (ushort(weight[off0 + 1u]) << 8) | ushort(weight[off0]); float d0 = float(as_type<half>(bits0)); float4 w0 = float4(float(as_type<char>(weight[off0 + 2u + ci * 4u])), float(as_type<char>(weight[off0 + 3u + ci * 4u])), float(as_type<char>(weight[off0 + 4u + ci * 4u])), float(as_type<char>(weight[off0 + 5u + ci * 4u]))) * d0; float4 w1 = float4(0.0f); if (has_o1) { uint off1 = row_offset1 + b * 34u; ushort bits1 = (ushort(weight[off1 + 1u]) << 8) | ushort(weight[off1]); float d1 = float(as_type<half>(bits1)); w1 = float4(float(as_type<char>(weight[off1 + 2u + ci * 4u])), float(as_type<char>(weight[off1 + 3u + ci * 4u])), float(as_type<char>(weight[off1 + 4u + ci * 4u])), float(as_type<char>(weight[off1 + 5u + ci * 4u]))) * d1; } uint in_col = c * 4u; for (uint rr = 0u; rr < RPTG; ++rr) { uint r = first_r + rr; if (r >= p.rows) continue; uint in_off = r * p.in_dim + in_col; float4 x = float4(${x4}); acc0[rr] += dot(x, w0); if (has_o1) acc1[rr] += dot(x, w1); } } }
    \\    for (uint rr = 0u; rr < RPTG; ++rr) { float total0 = acc0[rr]; float total1 = acc1[rr]; total0 += simd_shuffle_down(total0, 4u); total1 += simd_shuffle_down(total1, 4u); total0 += simd_shuffle_down(total0, 2u); total1 += simd_shuffle_down(total1, 2u); total0 += simd_shuffle_down(total0, 1u); total1 += simd_shuffle_down(total1, 1u); uint r = first_r + rr; if (tx == 0u && r < p.rows) { uint idx = r * p.out_dim + first_o; output[idx] = total0; if (has_o1) output[idx + 1u] = total1; } }
    \\}
    \\
;

fn x4(comptime cvt: []const u8) []const u8 {
    var parts: []const []const u8 = &.{};
    for ([_][]const u8{ "input[in_off]", "input[in_off + 1u]", "input[in_off + 2u]", "input[in_off + 3u]" }, 0..) |x, i|
        parts = parts ++ .{ if (i == 0) "" else ", ", if (cvt.len == 0) x else cvt ++ "(" ++ x ++ ")" };
    return msl.join(parts);
}

pub const q8_0_linear_mm_kernels = [_]msl.Item{
    msl.msl(fill(q8_0_linear_mm, .{ .name = "termite_q8_0_linear_mm", .in = "float", .x4 = x4("") })),
    msl.msl(fill(q8_0_linear_mm, .{ .name = "termite_q8_0_linear_mm_in_f16", .in = "half", .x4 = x4("float") })),
};

/// Q8_0 gate/up activation-multiply matmul for small batches.
const q8_0_pair_activation_multiply_mm =
    \\kernel void ${name}(device const float *input [[buffer(0)]], device const uchar *weight_gate [[buffer(1)]], device const uchar *weight_up [[buffer(2)]], device ${out} *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], constant termite_metal_apply_activation_params &ap [[buffer(5)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
    \\    weight_gate += p.weight_offset; weight_up += p.second_weight_offset;
    \\    const uint NX = 8u; const uint NY = 4u; const uint NSG = 4u; const uint RPTG = 8u; const uint CHPT = 4u; const uint OPG = 2u;
    \\    uint tx = uint(lane) & 7u; uint ty = uint(lane) >> 3; uint first_o = (tg.x * (NY * NSG) + uint(sgitg) * NY + ty) * OPG; uint first_r = tg.y * RPTG; if (first_o >= p.out_dim) return;
    \\    float gate0[8]; float up0[8]; float gate1[8]; float up1[8]; for (uint i = 0u; i < RPTG; ++i) { gate0[i] = 0.0f; up0[i] = 0.0f; gate1[i] = 0.0f; up1[i] = 0.0f; }
    \\    uint row_offset0 = first_o * p.row_blocks * 34u; uint row_offset1 = row_offset0 + p.row_blocks * 34u; uint total_chunks = p.row_blocks * 8u; bool has_o1 = first_o + 1u < p.out_dim;
    \\    for (uint chunk = tx; chunk < total_chunks; chunk += CHPT * NX) { for (uint ch = 0u; ch < CHPT; ++ch) { uint c = chunk + ch * NX; if (c >= total_chunks) continue; uint b = c >> 3; uint ci = c & 7u; uint off0 = row_offset0 + b * 34u; ushort bits_g0 = (ushort(weight_gate[off0 + 1u]) << 8) | ushort(weight_gate[off0]); ushort bits_u0 = (ushort(weight_up[off0 + 1u]) << 8) | ushort(weight_up[off0]); float dg0 = float(as_type<half>(bits_g0)); float du0 = float(as_type<half>(bits_u0)); float4 wg0 = float4(float(as_type<char>(weight_gate[off0 + 2u + ci * 4u])), float(as_type<char>(weight_gate[off0 + 3u + ci * 4u])), float(as_type<char>(weight_gate[off0 + 4u + ci * 4u])), float(as_type<char>(weight_gate[off0 + 5u + ci * 4u]))) * dg0; float4 wu0 = float4(float(as_type<char>(weight_up[off0 + 2u + ci * 4u])), float(as_type<char>(weight_up[off0 + 3u + ci * 4u])), float(as_type<char>(weight_up[off0 + 4u + ci * 4u])), float(as_type<char>(weight_up[off0 + 5u + ci * 4u]))) * du0; float4 wg1 = float4(0.0f); float4 wu1 = float4(0.0f); if (has_o1) { uint off1 = row_offset1 + b * 34u; ushort bits_g1 = (ushort(weight_gate[off1 + 1u]) << 8) | ushort(weight_gate[off1]); ushort bits_u1 = (ushort(weight_up[off1 + 1u]) << 8) | ushort(weight_up[off1]); float dg1 = float(as_type<half>(bits_g1)); float du1 = float(as_type<half>(bits_u1)); wg1 = float4(float(as_type<char>(weight_gate[off1 + 2u + ci * 4u])), float(as_type<char>(weight_gate[off1 + 3u + ci * 4u])), float(as_type<char>(weight_gate[off1 + 4u + ci * 4u])), float(as_type<char>(weight_gate[off1 + 5u + ci * 4u]))) * dg1; wu1 = float4(float(as_type<char>(weight_up[off1 + 2u + ci * 4u])), float(as_type<char>(weight_up[off1 + 3u + ci * 4u])), float(as_type<char>(weight_up[off1 + 4u + ci * 4u])), float(as_type<char>(weight_up[off1 + 5u + ci * 4u]))) * du1; } uint in_col = c * 4u; for (uint rr = 0u; rr < RPTG; ++rr) { uint r = first_r + rr; if (r >= p.rows) continue; uint in_off = r * p.in_dim + in_col; float4 x = float4(input[in_off], input[in_off + 1u], input[in_off + 2u], input[in_off + 3u]); gate0[rr] += dot(x, wg0); up0[rr] += dot(x, wu0); if (has_o1) { gate1[rr] += dot(x, wg1); up1[rr] += dot(x, wu1); } } } }
    \\    for (uint rr = 0u; rr < RPTG; ++rr) { float total_gate0 = gate0[rr]; float total_up0 = up0[rr]; float total_gate1 = gate1[rr]; float total_up1 = up1[rr]; total_gate0 += simd_shuffle_down(total_gate0, 4u); total_up0 += simd_shuffle_down(total_up0, 4u); total_gate1 += simd_shuffle_down(total_gate1, 4u); total_up1 += simd_shuffle_down(total_up1, 4u); total_gate0 += simd_shuffle_down(total_gate0, 2u); total_up0 += simd_shuffle_down(total_up0, 2u); total_gate1 += simd_shuffle_down(total_gate1, 2u); total_up1 += simd_shuffle_down(total_up1, 2u); total_gate0 += simd_shuffle_down(total_gate0, 1u); total_up0 += simd_shuffle_down(total_up0, 1u); total_gate1 += simd_shuffle_down(total_gate1, 1u); total_up1 += simd_shuffle_down(total_up1, 1u); uint r = first_r + rr; if (tx == 0u && r < p.rows) { uint idx = r * p.out_dim + first_o; output[idx] = ${o0}; if (has_o1) output[idx + 1u] = ${o1}; } }
    \\}
    \\
;

fn gated(comptime k: []const u8, comptime half_out: bool) []const u8 {
    const v = fill("termite_gated_activation_product(total_gate${k}, total_up${k}, ap.activation_kind)", .{ .k = k });
    return if (half_out) "half(" ++ v ++ ")" else v;
}

pub const q8_0_pair_activation_multiply_mm_kernels = [_]msl.Item{
    msl.msl(fill(q8_0_pair_activation_multiply_mm, .{ .name = "termite_q8_0_pair_activation_multiply_mm", .out = "float", .o0 = gated("0", false), .o1 = gated("1", false) })),
    msl.msl(fill(q8_0_pair_activation_multiply_mm, .{ .name = "termite_q8_0_pair_activation_multiply_mm_out_f16", .out = "half", .o0 = gated("0", true), .o1 = gated("1", true) })),
};

/// Florence Q4_K 64 x 64 simdgroup-matrix tile.
const florence_q4_k_mm_matrix =
    \\kernel void ${name}(device const ${in} *input [[buffer(0)]], device const uchar *weight_q4_k [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    \\    const uint tid = thread_pos.x; const uint first_o = group_pos.x * 64u; const uint first_r = group_pos.y * 64u;
    \\    if (rows < 2 || in_dim <= 0 || (uint(in_dim) & 255u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return;
    \\    threadgroup uchar shared[16384]; threadgroup half *weight_tile = (threadgroup half *)shared; threadgroup half *input_tile = weight_tile + 2048;
    \\    simdgroup_half8x8 mw[8]; simdgroup_half8x8 mx; simdgroup_float8x8 acc[8]; for (uint i = 0u; i < 8u; ++i) acc[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);
    \\    const uint block_count = uint(in_dim) >> 8; const uint sg_row = uint(simdgroup_id) * 8u;
    \\    for (uint k0 = 0u; k0 < uint(in_dim); k0 += 32u) {
    \\        { const uint col = tid & 63u; const uint k_lane = tid >> 6u; const uint global_col = first_o + col; if (global_col < uint(out_dim)) { const uint block_index = k0 >> 8u; const uint sub = (k0 & 255u) >> 5u; device const uchar *block = weight_q4_k + (global_col * block_count + block_index) * 144u; float raw_scale = 0.0f; float raw_min = 0.0f; antfly_qk_unpack_scale_min_6bit(block + 4u, int(sub), raw_scale, raw_min); const float dsc = antfly_qk_half_le_to_float(block) * raw_scale; const float dmn = antfly_qk_half_le_to_float(block + 2u) * raw_min; device const uchar *qs = block + 16u + (sub >> 1u) * 32u; for (uint step = 0u; step < 8u; ++step) { const uint k = k_lane + step * 4u; const uchar packed = qs[k]; const uint qv = (sub & 1u) == 0u ? uint(packed & 0x0fu) : uint(packed >> 4u); weight_tile[k * 64u + col] = half(dsc * float(qv) - dmn); } } else { for (uint step = 0u; step < 8u; ++step) { const uint k = k_lane + step * 4u; weight_tile[k * 64u + col] = half(0.0f); } } }
    \\        for (uint index = tid; index < 2048u; index += 256u) { uint row = index >> 5; uint k = index & 31u; uint global_row = first_r + row; input_tile[row * 32u + k] = global_row < uint(rows) ? ${x} : half(0.0f); }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\        for (uint k = 0u; k < 32u; k += 8u) { for (uint i = 0u; i < 8u; ++i) simdgroup_load(mw[i], weight_tile + k * 64u + i * 8u, 64u); simdgroup_load(mx, input_tile + sg_row * 32u + k, 32u); for (uint i = 0u; i < 8u; ++i) simdgroup_multiply_accumulate(acc[i], mx, mw[i], acc[i]); }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    }
    \\    threadgroup float *result_tile = (threadgroup float *)shared; for (uint i = 0u; i < 8u; ++i) simdgroup_store(acc[i], result_tile + sg_row * 64u + i * 8u, 64u);
    \\    threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    for (uint index = tid; index < 4096u; index += 256u) { uint row = index >> 6; uint col = index & 63u; uint global_row = first_r + row; if (global_row < uint(rows) && first_o + col < uint(out_dim)) output[global_row * uint(out_dim) + first_o + col] = result_tile[index]; }
    \\}
    \\
;

pub const florence_q4_k_mm_matrix_m64_n64 = msl.msl(fill(florence_q4_k_mm_matrix, .{ .name = "termite_florence_q4_k_mm_matrix_m64_n64", .in = "float", .x = "half(input[global_row * uint(in_dim) + k0 + k])" }));
pub const florence_q4_k_mm_matrix_m64_n64_in_f16 = msl.msl(fill(florence_q4_k_mm_matrix, .{ .name = "termite_florence_q4_k_mm_matrix_m64_n64_in_f16", .in = "half", .x = "input[global_row * uint(in_dim) + k0 + k]" }));
