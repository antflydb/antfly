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

//! Routed (MUL_MV_ID) Q4_0 matvec: the mmv16 schedule of
//! quant_metal_q4_0.zig with the expert chosen per input row, including
//! the fixed-shape Gemma4 A4B variants. The activation staging and the
//! four-way split nibble dot (`qacc[4]`) are shared; the routing prologue
//! and epilogue are written per kernel.

const msl = @import("quant_metal_msl.zig");
const q4_0 = @import("quant_metal_q4_0.zig");
const fill = msl.fill;
const join = msl.join;
const per = q4_0.per;

/// Row dot with four partial nibble sums per weight (`u`: suffix of the
/// `+ 1` byte index literal).
fn qacc4w(comptime ws: []const q4_0.Weight, comptime u: []const u8) []const u8 {
    return join(&.{
        per(ws, fill("ushort $${pre}bits = (ushort($${w}[$${off} + 1${u}]) << 8) | ushort($${w}[$${off}]);", .{ .u = u })),                                                                                                                        " ",
        per(ws, "float d${s} = float(as_type<half>(${pre}bits));"),                                                                                                                                                                                " ",
        per(ws, "const device ushort *qs${s} = reinterpret_cast<const device ushort *>(${w} + ${off} + 2u) + (il >> 1);"),                                                                                                                         " ",
        per(ws, "float qacc${s}[4] = {0.0f, 0.0f, 0.0f, 0.0f};"),                                                                                                                                                                                  " TERMITE_FOR_UNROLL (uint i = 0u; i < 8u; i += 2u) { ",
        per(ws, "uint packed${s} = uint(qs${s}[i >> 1]);"),                                                                                                                                                                                        " ",
        per(ws, "qacc${s}[0] += yl[i] * float(packed${s} & 0x000Fu); qacc${s}[1] += yl[i + 1u] * float(packed${s} & 0x0F00u); qacc${s}[2] += yl[i + 8u] * float(packed${s} & 0x00F0u); qacc${s}[3] += yl[i + 9u] * float(packed${s} & 0xF000u);"), " } ",
        per(ws, "${acc}[row] += d${s} * (qacc${s}[0] + qacc${s}[1] + qacc${s}[2] + qacc${s}[3] - 8.0f * sumy);"),
    });
}

fn qacc4(comptime u: []const u8) []const u8 {
    return qacc4w(&q4_0.single, u);
}

const linear_id_t =
    \\kernel void termite_q4_0_linear_id(device const float *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device float *output [[buffer(2)]], device const uint *expert_ids [[buffer(3)]], constant termite_metal_linear_id_params &p [[buffer(4)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
    \\    const uint NR0 = 4u; const uint NSG = 2u; const uint NQ = 16u; const uint NW = 32u; uint first_o = (tg.x * NSG + uint(sgitg)) * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return; uint expert = expert_ids[r];
    \\    if (expert >= p.expert_count) { if (lane == 0u) { uint idx = r * p.out_dim + first_o; TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) if (first_o + row < p.out_dim) output[idx + row] = 0.0f; } return; }
    \\    uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * 8u; uint ib0 = ix; uint input_row = p.reserved == 0u ? r : r / p.reserved; uint in_row = input_row * p.in_dim; uint expert_row = expert * p.source_out_dim + p.row_offset; float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};
    \\    for (uint b = ib0; b < p.row_blocks; b += NQ) { ${stage} TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = (expert_row + o) * p.row_blocks * 18u + b * 18u; ${dot} } }
    \\    TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) acc[row] = simd_sum(acc[row]); if (lane == 0u) { uint idx = r * p.out_dim + first_o; TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) if (first_o + row < p.out_dim) output[idx + row] = acc[row]; }
    \\}
    \\
;

pub const linear_id = msl.msl(fill(linear_id_t, .{ .stage = q4_0.stage("TERMITE_FOR_UNROLL", "in_row + b * 32u + il", ""), .dot = qacc4("") }));

/// Fixed A4B shapes: route count, dimensions and row strides are constants.
const a4b_t =
    \\kernel void ${name}(device const float *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device float *output [[buffer(2)]], device const uint *expert_ids [[buffer(3)]], constant termite_metal_linear_id_params &p [[buffer(4)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
    \\    const uint NR0 = 4u; const uint NSG = 2u; const uint NQ = 16u; const uint NW = 32u; const uint ROW_BLOCKS = ${blocks}u;${in_dim} const uint OUT_DIM = ${out}u; const uint EXPERTS = 128u; uint first_o = (tg.x * NSG + uint(sgitg)) * NR0; uint route = tg.y; if (route >= 8u || lane >= NW) return; uint expert = expert_ids[route];
    \\    if (expert >= EXPERTS) { if (lane == 0u) { uint idx = route * OUT_DIM + first_o; TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) output[idx + row] = 0.0f; } return; }
    \\    uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * 8u; ${in_row}uint expert_row = expert * OUT_DIM; float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};
    \\    for (uint b = ix; b < ROW_BLOCKS; b += NQ) { ${stage} TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) { uint off = (expert_row + first_o + row) * ${stride}u + b * 18u; ${dot} } }
    \\    TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) acc[row] = simd_sum(acc[row]); if (lane == 0u) { uint idx = route * OUT_DIM + first_o; TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) output[idx + row] = acc[row]; }
    \\}
    \\
;

fn a4b(comptime name: []const u8, comptime blocks: u32, comptime in_dim: u32, comptime out: u32) msl.Item {
    const in_row = if (in_dim == 0) "" else "in_row + ";
    return msl.msl(fill(a4b_t, .{
        .name = name,
        .blocks = blocks,
        .in_dim = if (in_dim == 0) "" else fill(" const uint IN_DIM = ${d}u;", .{ .d = in_dim }),
        .out = out,
        .in_row = if (in_dim == 0) "" else "uint in_row = route * IN_DIM; ",
        .stage = q4_0.stage("TERMITE_FOR_UNROLL", in_row ++ "b * 32u + il", ""),
        .stride = blocks * 18,
        .dot = qacc4("u"),
    }));
}

/// Gate/up rows read the shared hidden state directly (88 blocks); down
/// rows read their route's 704 activations (22 blocks).
pub const a4b_gate_up = a4b("termite_q4_0_linear_id_a4b_gate_up", 88, 0, 1408);
pub const a4b_down = a4b("termite_q4_0_linear_id_a4b_down", 22, 704, 2816);

const a4b_down_reduce_t =
    \\kernel void termite_q4_0_linear_id_a4b_down_reduce(device const float *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device const uint *expert_ids [[buffer(2)]], device const float *route_weights [[buffer(3)]], device const float *expert_scale [[buffer(4)]], device float *output [[buffer(5)]], constant termite_metal_a4b_down_reduce_params &p [[buffer(6)]], threadgroup float *route_values [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
    \\    const uint NR0 = 4u; const uint NSG = 2u; const uint NQ = 16u; const uint NW = 32u; const uint ROW_BLOCKS = 22u; const uint IN_DIM = 704u; const uint OUT_DIM = 2816u; const uint EXPERTS = 128u; uint first_o = (tg.x * NSG + uint(sgitg)) * NR0; if (lane >= NW) return; uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * 8u;
    \\    TERMITE_FOR_UNROLL (uint route = 0u; route < 8u; ++route) { uint expert = expert_ids[route]; float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f}; if (expert < EXPERTS) { uint in_row = route * IN_DIM; uint expert_row = expert * OUT_DIM; for (uint b = ix; b < ROW_BLOCKS; b += NQ) { ${stage} TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) { uint off = (expert_row + first_o + row) * 396u + b * 18u; ${dot} } } } TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) acc[row] = simd_sum(acc[row]); if (lane == 0u) { TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) route_values[(uint(sgitg) * NR0 + row) * 8u + route] = acc[row]; } }
    \\    threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    {
    \\#pragma clang fp contract(off)
    \\#pragma clang fp reassociate(off)
    \\    if (lane == 0u) { TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) { float value = 0.0f; TERMITE_FOR_UNROLL (uint route = 0u; route < 8u; ++route) { uint expert = expert_ids[route]; float scale = p.has_expert_scale != 0u ? expert_scale[expert] : 1.0f; value += (route_values[(uint(sgitg) * NR0 + row) * 8u + route] * scale) * route_weights[route]; } output[first_o + row] = value; } }
    \\    }
    \\}
    \\
;

pub const a4b_down_reduce = msl.msl(fill(a4b_down_reduce_t, .{ .stage = q4_0.stage("TERMITE_FOR_UNROLL", "in_row + b * 32u + il", ""), .dot = qacc4("u") }));

const pair_activation_id_t =
    \\kernel void termite_q4_0_pair_activation_id(device const float *input [[buffer(0)]], device const uchar *weight_gate [[buffer(1)]], device const uchar *weight_up [[buffer(2)]], device float *output [[buffer(3)]], device const uint *expert_ids [[buffer(4)]], constant termite_metal_pair_activation_id_params &p [[buffer(5)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
    \\    constant termite_metal_linear_id_params &gate_p = p.pair.first; constant termite_metal_linear_id_params &up_p = p.pair.second; const uint NR0 = 4u; const uint NSG = 2u; const uint NQ = 16u; const uint NW = 32u; uint first_o = (tg.x * NSG + uint(sgitg)) * NR0; uint route = tg.y; if (route >= gate_p.rows || lane >= NW || p.top_k == 0u) return; uint expert = expert_ids[route];
    \\    if (expert >= gate_p.expert_count || expert >= up_p.expert_count) { if (lane == 0u) { uint idx = route * gate_p.out_dim + first_o; TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) if (first_o + row < gate_p.out_dim) output[idx + row] = 0.0f; } return; }
    \\    uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * 8u; uint input_row = route / p.top_k; uint in_row = input_row * gate_p.in_dim; uint gate_expert_row = expert * gate_p.source_out_dim + gate_p.row_offset; uint up_expert_row = expert * up_p.source_out_dim + up_p.row_offset; float gate[4] = {0.0f, 0.0f, 0.0f, 0.0f}; float up[4] = {0.0f, 0.0f, 0.0f, 0.0f};
    \\    for (uint block = ix; block < gate_p.row_blocks; block += NQ) { ${stage} TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= gate_p.out_dim) continue; uint gate_off = (gate_expert_row + o) * gate_p.row_blocks * 18u + block * 18u; uint up_off = (up_expert_row + o) * up_p.row_blocks * 18u + block * 18u; ${dot} } }
    \\    TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) { gate[row] = simd_sum(gate[row]); up[row] = simd_sum(up[row]); } if (lane == 0u) { uint idx = route * gate_p.out_dim + first_o; TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) if (first_o + row < gate_p.out_dim) output[idx + row] = termite_gated_activation_product(gate[row], up[row], p.activation_kind); }
    \\}
    \\
;

const gate_up_routed = [_]q4_0.Weight{ .{ .buf = "weight_gate", .acc = "gate", .sfx = "_gate", .pre = "gate_", .off = "gate_off" }, .{ .buf = "weight_up", .acc = "up", .sfx = "_up", .pre = "up_", .off = "up_off" } };
pub const pair_activation_id = msl.msl(fill(pair_activation_id_t, .{ .stage = q4_0.stage("TERMITE_FOR_UNROLL", "in_row + block * 32u + il", ""), .dot = qacc4w(&gate_up_routed, "u") }));

const pair_linear_id_t =
    \\kernel void termite_q4_0_pair_linear_id(device const float *input [[buffer(0)]], device const uchar *weight_a [[buffer(1)]], device const uchar *weight_b [[buffer(2)]], device float *output_a [[buffer(3)]], device float *output_b [[buffer(4)]], device const uint *expert_ids [[buffer(5)]], constant termite_metal_linear_pair_id_params &pair [[buffer(6)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
    \\    constant termite_metal_linear_id_params &pa = pair.first; constant termite_metal_linear_id_params &pb = pair.second; const uint NR0 = 4u; const uint NSG = 2u; const uint NQ = 16u; const uint NW = 32u; uint first_o = (tg.x * NSG + uint(sgitg)) * NR0; uint r = tg.y; if (r >= pa.rows || lane >= NW) return; uint expert = expert_ids[r];
    \\    if (expert >= pa.expert_count || expert >= pb.expert_count) { if (lane == 0u) { uint idx = r * pa.out_dim + first_o; for (uint row = 0u; row < NR0; ++row) if (first_o + row < pa.out_dim) { output_a[idx + row] = 0.0f; output_b[idx + row] = 0.0f; } } return; }
    \\    uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * 8u; uint ib0 = ix; uint in_row = r * pa.in_dim; uint expert_row_a = expert * pa.source_out_dim + pa.row_offset; uint expert_row_b = expert * pb.source_out_dim + pb.row_offset; float acc_a[4] = {0.0f, 0.0f, 0.0f, 0.0f}; float acc_b[4] = {0.0f, 0.0f, 0.0f, 0.0f};
    \\    for (uint block = ib0; block < pa.row_blocks; block += NQ) { ${stage} for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= pa.out_dim) continue; uint off_a = (expert_row_a + o) * pa.row_blocks * 18u + block * 18u; uint off_b = (expert_row_b + o) * pb.row_blocks * 18u + block * 18u; ushort bits_a = (ushort(weight_a[off_a + 1]) << 8) | ushort(weight_a[off_a]); ushort bits_b = (ushort(weight_b[off_b + 1]) << 8) | ushort(weight_b[off_b]); float d_a = float(as_type<half>(bits_a)); float d_b = float(as_type<half>(bits_b)); const device ushort *qs_a = reinterpret_cast<const device ushort *>(weight_a + off_a + 2u) + (il >> 1); const device ushort *qs_b = reinterpret_cast<const device ushort *>(weight_b + off_b + 2u) + (il >> 1); float qacc_a = 0.0f; float qacc_b = 0.0f; for (uint i = 0u; i < 8u; i += 2u) { uint qa = uint(qs_a[i >> 1]); uint qb = uint(qs_b[i >> 1]); qacc_a += yl[i] * float(qa & 0x000Fu); qacc_a += yl[i + 1u] * float(qa & 0x0F00u); qacc_a += yl[i + 8u] * float(qa & 0x00F0u); qacc_a += yl[i + 9u] * float(qa & 0xF000u); qacc_b += yl[i] * float(qb & 0x000Fu); qacc_b += yl[i + 1u] * float(qb & 0x0F00u); qacc_b += yl[i + 8u] * float(qb & 0x00F0u); qacc_b += yl[i + 9u] * float(qb & 0xF000u); } acc_a[row] += d_a * (qacc_a - 8.0f * sumy); acc_b[row] += d_b * (qacc_b - 8.0f * sumy); } }
    \\    for (uint row = 0u; row < NR0; ++row) { acc_a[row] = simd_sum(acc_a[row]); acc_b[row] = simd_sum(acc_b[row]); } if (lane == 0u) { uint idx = r * pa.out_dim + first_o; for (uint row = 0u; row < NR0; ++row) if (first_o + row < pa.out_dim) { output_a[idx + row] = acc_a[row]; output_b[idx + row] = acc_b[row]; } }
    \\}
    \\
;

pub const pair_linear_id = msl.msl(fill(pair_linear_id_t, .{ .stage = q4_0.stage("for", "in_row + block * 32u + il", "") }));
