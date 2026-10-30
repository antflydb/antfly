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

//! Q4_K 1x_reduce matvec: lanes take eight values of a 32-value sub-block
//! (NQ = 8 sub-blocks per simdgroup pass, NSG simdgroups striding), each
//! row folds d * scale and dmin * min into one fused dot, then simd and
//! threadgroup sums. Variants carry one or two weights (pair outputs or
//! gate / up with the activation) and differ in their epilogue.

const msl = @import("quant_metal_msl.zig");
const wrap = @import("quant_metal_wrap.zig");
const q4_0 = @import("quant_metal_q4_0.zig");
const gemv = @import("quant_metal_gemv.zig");
const fill = msl.fill;
const join = msl.join;
const Weight = q4_0.Weight;
const per = q4_0.per;

/// The sub-block row dot of each weight, then `accumulate`.
fn rowDot(comptime ws: []const Weight, comptime accumulate: []const u8) []const u8 {
    return join(&.{
        per(ws, "ushort d_bits${s} = (ushort(${w}[off + 1]) << 8) | ushort(${w}[off]); ushort dmin_bits${s} = (ushort(${w}[off + 3]) << 8) | ushort(${w}[off + 2]);"), " ",
        per(ws, "float d${s} = float(as_type<half>(d_bits${s})); float dmin${s} = float(as_type<half>(dmin_bits${s}));"),                                              " ",
        per(ws, "const device uchar *scales${s} = ${w} + off + 4u;"),                                                                                                  " ",
        per(ws, "const device uchar *qs${s} = ${w} + off + 16u;"),                                                                                                     " ",
        per(ws, "float dsc${s} = d${s} * q4k_scale_at(scales${s}, sub); float dmn${s} = dmin${s} * q4k_min_at(scales${s}, sub);"),                                     " uint q_off = chunk * 32u + j0; ",
        per(ws, "float block${s} = 0.0f;"),                                                                                                                            " for (uint i = 0; i < NQ; ++i) { ",
        per(ws, "uchar packed${s} = qs${s}[q_off + i];"),                                                                                                              " ",
        per(ws, "float q${s} = high ? float(packed${s} >> 4) : float(packed${s} & 0x0Fu);"),                                                                           " ",
        per(ws, "block${s} += x[i] * (dsc${s} * q${s} - dmn${s});"),                                                                                                   " } ",
        accumulate,
    });
}

const K = struct {
    name: []const u8,
    weights: []const Weight = &q4_0.single,
    /// Buffers between the weights and params.
    io: []const []const u8 = &.{wrap.out_f32},
    activation: bool = false,
    nr: u32,
    accs: []const u8,
    accumulate: []const u8,
    /// Reduction and store lines.
    tail: []const u8,
};

fn kernel(comptime k: K) msl.Item {
    comptime {
        var args: []const []const u8 = &.{wrap.in_f32};
        for (k.weights) |x| args = args ++ .{"device const uchar *" ++ x.buf};
        args = args ++ k.io ++ .{wrap.linear_p};
        if (k.activation) args = args ++ .{wrap.act_p};
        args = args ++ .{"threadgroup float *shmem"} ++ wrap.sg;
        return msl.msl(msl.signature(k.name, args) ++ fill(
            \\
            \\    const uint NR0 = ${nr}u; const uint NSG = 4u; const uint NQ = 8u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;
            \\    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint sb0 = sgitg * NQ + ix; uint in_row = r * p.in_dim; uint total_subblocks = p.row_blocks * 8u; ${accs}
            \\    for (uint sb = sb0; sb < total_subblocks; sb += NSG * NQ) { uint b = sb >> 3; uint sub = sb & 7u; uint chunk = sub >> 1; bool high = (sub & 1u) != 0u; uint j0 = il * NQ; uint input_off = in_row + b * 256u + sub * 32u + j0; float x[8]; for (uint i = 0; i < NQ; ++i) { x[i] = input[input_off + i]; } for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 144u + b * 144u; ${dot} } }
            \\${tail}}
            \\
        , .{ .nr = k.nr, .accs = k.accs, .dot = rowDot(k.weights, k.accumulate), .tail = k.tail }));
    }
}

const ab = [_]Weight{ .{ .buf = "weight_a", .acc = "acc_a", .sfx = "_a" }, .{ .buf = "weight_b", .acc = "acc_b", .sfx = "_b" } };

pub const linear = kernel(.{
    .name = "termite_q4_k_linear_1x_reduce",
    .nr = 4,
    .accs = "float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};",
    .accumulate = "acc[row] += block;",
    .tail = gemv.arrayTail(4),
});

pub const pair_linear = kernel(.{
    .name = "termite_q4_k_pair_linear_1x_reduce",
    .weights = &ab,
    .io = &.{ "device float *output_a", "device float *output_b" },
    .nr = 2,
    .accs = "float acc_a0 = 0.0f; float acc_a1 = 0.0f; float acc_b0 = 0.0f; float acc_b1 = 0.0f;",
    .accumulate = "if (row == 0u) { acc_a0 += block_a; acc_b0 += block_b; } else { acc_a1 += block_a; acc_b1 += block_b; }",
    .tail =
    \\    if (sgitg == 0u) { shmem[lane] = 0.0f; shmem[32u + lane] = 0.0f; shmem[64u + lane] = 0.0f; shmem[96u + lane] = 0.0f; } acc_a0 = simd_sum(acc_a0); acc_a1 = simd_sum(acc_a1); acc_b0 = simd_sum(acc_b0); acc_b1 = simd_sum(acc_b1); threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    if (lane == 0u) { shmem[sgitg] = acc_a0; shmem[32u + sgitg] = acc_a1; shmem[64u + sgitg] = acc_b0; shmem[96u + sgitg] = acc_b1; } threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    float total_a0 = simd_sum(shmem[lane]); float total_a1 = simd_sum(shmem[32u + lane]); float total_b0 = simd_sum(shmem[64u + lane]); float total_b1 = simd_sum(shmem[96u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; if (first_o < p.out_dim) { output_a[idx] = total_a0; output_b[idx] = total_b0; } if (first_o + 1u < p.out_dim) { output_a[idx + 1u] = total_a1; output_b[idx + 1u] = total_b1; } }
    \\
    ,
});

pub const pair_activation = [_]msl.Item{
    kernel(.{
        .name = "termite_q4_k_pair_activation_1x_reduce",
        .weights = &q4_0.gate_up,
        .activation = true,
        .nr = 2,
        .accs = "float gate0 = 0.0f; float up0 = 0.0f; float gate1 = 0.0f; float up1 = 0.0f;",
        .accumulate = "if (row == 0u) { gate0 += block_gate; up0 += block_up; } else { gate1 += block_gate; up1 += block_up; }",
        .tail =
        \\    if (sgitg == 0u) { shmem[lane] = 0.0f; shmem[32u + lane] = 0.0f; shmem[64u + lane] = 0.0f; shmem[96u + lane] = 0.0f; } gate0 = simd_sum(gate0); up0 = simd_sum(up0); gate1 = simd_sum(gate1); up1 = simd_sum(up1); threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    if (lane == 0u) { shmem[sgitg] = gate0; shmem[32u + sgitg] = up0; shmem[64u + sgitg] = gate1; shmem[96u + sgitg] = up1; } threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    float total_gate0 = simd_sum(shmem[lane]); float total_up0 = simd_sum(shmem[32u + lane]); float total_gate1 = simd_sum(shmem[64u + lane]); float total_up1 = simd_sum(shmem[96u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; if (first_o < p.out_dim) output[idx] = termite_gated_activation_product(total_gate0, total_up0, ap.activation_kind); if (first_o + 1u < p.out_dim) output[idx + 1u] = termite_gated_activation_product(total_gate1, total_up1, ap.activation_kind); }
        \\
        ,
    }),
    kernel(.{
        .name = "termite_q4_k_pair_activation_1x_reduce_out_f16",
        .weights = &q4_0.gate_up,
        .io = &.{"device half *output"},
        .activation = true,
        .nr = 4,
        .accs = "float gate[4] = {0.0f, 0.0f, 0.0f, 0.0f}; float up[4] = {0.0f, 0.0f, 0.0f, 0.0f};",
        .accumulate = "gate[row] += block_gate; up[row] += block_up;",
        .tail =
        \\    if (sgitg == 0u) { for (uint row = 0; row < NR0; ++row) { shmem[row * 64u + lane] = 0.0f; shmem[row * 64u + 32u + lane] = 0.0f; } } for (uint row = 0; row < NR0; ++row) { gate[row] = simd_sum(gate[row]); up[row] = simd_sum(up[row]); } threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    if (lane == 0u) { for (uint row = 0; row < NR0; ++row) { shmem[row * 64u + sgitg] = gate[row]; shmem[row * 64u + 32u + sgitg] = up[row]; } } threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    float total_gate[4]; float total_up[4]; for (uint row = 0; row < NR0; ++row) { total_gate[row] = simd_sum(shmem[row * 64u + lane]); total_up[row] = simd_sum(shmem[row * 64u + 32u + lane]); } if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o < p.out_dim) output[idx + row] = half(termite_gated_activation_product(total_gate[row], total_up[row], ap.activation_kind)); } }
        \\
        ,
    }),
    kernel(.{
        .name = "termite_q4_k_activation_rhs_1x_reduce",
        .io = &.{ "device const float *rhs", wrap.out_f32 },
        .activation = true,
        .nr = 2,
        .accs = "float acc0 = 0.0f; float acc1 = 0.0f;",
        .accumulate = "if (row == 0u) acc0 += block; else acc1 += block;",
        .tail =
        \\    if (sgitg == 0u) { shmem[lane] = 0.0f; shmem[32u + lane] = 0.0f; } acc0 = simd_sum(acc0); acc1 = simd_sum(acc1); threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    if (lane == 0u) { shmem[sgitg] = acc0; shmem[32u + sgitg] = acc1; } threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    float total0 = simd_sum(shmem[lane]); float total1 = simd_sum(shmem[32u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; if (first_o < p.out_dim) output[idx] = termite_gated_activation_product(total0, rhs[idx], ap.activation_kind); if (first_o + 1u < p.out_dim) output[idx + 1u] = termite_gated_activation_product(total1, rhs[idx + 1u], ap.activation_kind); }
        \\
        ,
    }),
};
