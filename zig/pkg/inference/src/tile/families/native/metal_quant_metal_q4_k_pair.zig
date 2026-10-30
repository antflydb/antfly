// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

pub const source_0 =
    program.join(&.{
        \\           TERMITE_MSL_ABI_QUANT_PAIR_PROJECTION_REDUCE(termite_q4_k_pair_linear_1x_reduce) "\n"
        \\
        ,
        program.metal(
            \\    const uint NR0 = 2u; const uint NSG = 4u; const uint NQ = 8u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;
            \\    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint sb0 = sgitg * NQ + ix; uint in_row = r * p.in_dim; uint total_subblocks = p.row_blocks * 8u; float acc_a0 = 0.0f; float acc_a1 = 0.0f; float acc_b0 = 0.0f; float acc_b1 = 0.0f;
            \\    for (uint sb = sb0; sb < total_subblocks; sb += NSG * NQ) { uint b = sb >> 3; uint sub = sb & 7u; uint chunk = sub >> 1; bool high = (sub & 1u) != 0u; uint j0 = il * NQ; uint input_off = in_row + b * 256u + sub * 32u + j0; float x[8]; for (uint i = 0; i < NQ; ++i) { x[i] = input[input_off + i]; } for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; float2 block = termite_q4k_pair_dot8(x, weight_a, weight_b, p.row_blocks, o, b, sub, chunk, high, j0); if (row == 0u) { acc_a0 += block.x; acc_b0 += block.y; } else { acc_a1 += block.x; acc_b1 += block.y; } } }
            \\    if (sgitg == 0u) { shmem[lane] = 0.0f; shmem[32u + lane] = 0.0f; shmem[64u + lane] = 0.0f; shmem[96u + lane] = 0.0f; } acc_a0 = simd_sum(acc_a0); acc_a1 = simd_sum(acc_a1); acc_b0 = simd_sum(acc_b0); acc_b1 = simd_sum(acc_b1); threadgroup_barrier(mem_flags::mem_threadgroup);
            \\    if (lane == 0u) { shmem[sgitg] = acc_a0; shmem[32u + sgitg] = acc_a1; shmem[64u + sgitg] = acc_b0; shmem[96u + sgitg] = acc_b1; } threadgroup_barrier(mem_flags::mem_threadgroup);
            \\    float total_a0 = simd_sum(shmem[lane]); float total_a1 = simd_sum(shmem[32u + lane]); float total_b0 = simd_sum(shmem[64u + lane]); float total_b1 = simd_sum(shmem[96u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; if (first_o < p.out_dim) { output_a[idx] = total_a0; output_b[idx] = total_b0; } if (first_o + 1u < p.out_dim) { output_a[idx + 1u] = total_a1; output_b[idx + 1u] = total_b1; } }
            \\}
            \\
        ),

        \\           TERMITE_MSL_ABI_QUANT_PAIR_REDUCE(termite_q4_k_pair_activation_1x_reduce) "\n"
        \\
        ,
        program.metal(
            \\    const uint NR0 = 2u; const uint NSG = 4u; const uint NQ = 8u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;
            \\    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint sb0 = sgitg * NQ + ix; uint in_row = r * p.in_dim; uint total_subblocks = p.row_blocks * 8u; float gate0 = 0.0f; float up0 = 0.0f; float gate1 = 0.0f; float up1 = 0.0f;
            \\    for (uint sb = sb0; sb < total_subblocks; sb += NSG * NQ) { uint b = sb >> 3; uint sub = sb & 7u; uint chunk = sub >> 1; bool high = (sub & 1u) != 0u; uint j0 = il * NQ; uint input_off = in_row + b * 256u + sub * 32u + j0; float x[8]; for (uint i = 0; i < NQ; ++i) { x[i] = input[input_off + i]; } for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; float2 block = termite_q4k_pair_dot8(x, weight_gate, weight_up, p.row_blocks, o, b, sub, chunk, high, j0); if (row == 0u) { gate0 += block.x; up0 += block.y; } else { gate1 += block.x; up1 += block.y; } } }
            \\    if (sgitg == 0u) { shmem[lane] = 0.0f; shmem[32u + lane] = 0.0f; shmem[64u + lane] = 0.0f; shmem[96u + lane] = 0.0f; } gate0 = simd_sum(gate0); up0 = simd_sum(up0); gate1 = simd_sum(gate1); up1 = simd_sum(up1); threadgroup_barrier(mem_flags::mem_threadgroup);
            \\    if (lane == 0u) { shmem[sgitg] = gate0; shmem[32u + sgitg] = up0; shmem[64u + sgitg] = gate1; shmem[96u + sgitg] = up1; } threadgroup_barrier(mem_flags::mem_threadgroup);
            \\    float total_gate0 = simd_sum(shmem[lane]); float total_up0 = simd_sum(shmem[32u + lane]); float total_gate1 = simd_sum(shmem[64u + lane]); float total_up1 = simd_sum(shmem[96u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; if (first_o < p.out_dim) output[idx] = termite_gated_activation_product(total_gate0, total_up0, ap.activation_kind); if (first_o + 1u < p.out_dim) output[idx + 1u] = termite_gated_activation_product(total_gate1, total_up1, ap.activation_kind); }
            \\}
            \\
        ),

        \\           TERMITE_MSL_ABI_QUANT_PAIR_ACTIVATION_F16(termite_q4_k_pair_activation_1x_reduce_out_f16) "\n"
        \\
        ,
        program.metal(
            \\    const uint NR0 = 4u; const uint NSG = 4u; const uint NQ = 8u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;
            \\    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint sb0 = sgitg * NQ + ix; uint in_row = r * p.in_dim; uint total_subblocks = p.row_blocks * 8u; float gate[4] = {0.0f, 0.0f, 0.0f, 0.0f}; float up[4] = {0.0f, 0.0f, 0.0f, 0.0f};
            \\    for (uint sb = sb0; sb < total_subblocks; sb += NSG * NQ) { uint b = sb >> 3; uint sub = sb & 7u; uint chunk = sub >> 1; bool high = (sub & 1u) != 0u; uint j0 = il * NQ; uint input_off = in_row + b * 256u + sub * 32u + j0; float x[8]; for (uint i = 0; i < NQ; ++i) { x[i] = input[input_off + i]; } for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; float2 block = termite_q4k_pair_dot8(x, weight_gate, weight_up, p.row_blocks, o, b, sub, chunk, high, j0); gate[row] += block.x; up[row] += block.y; } }
            \\    if (sgitg == 0u) { for (uint row = 0; row < NR0; ++row) { shmem[row * 64u + lane] = 0.0f; shmem[row * 64u + 32u + lane] = 0.0f; } } for (uint row = 0; row < NR0; ++row) { gate[row] = simd_sum(gate[row]); up[row] = simd_sum(up[row]); } threadgroup_barrier(mem_flags::mem_threadgroup);
            \\    if (lane == 0u) { for (uint row = 0; row < NR0; ++row) { shmem[row * 64u + sgitg] = gate[row]; shmem[row * 64u + 32u + sgitg] = up[row]; } } threadgroup_barrier(mem_flags::mem_threadgroup);
            \\    float total_gate[4]; float total_up[4]; for (uint row = 0; row < NR0; ++row) { total_gate[row] = simd_sum(shmem[row * 64u + lane]); total_up[row] = simd_sum(shmem[row * 64u + 32u + lane]); } if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o < p.out_dim) output[idx + row] = half(termite_gated_activation_product(total_gate[row], total_up[row], ap.activation_kind)); } }
            \\}
            \\
        ),

        \\           TERMITE_MSL_ABI_QUANT_RHS_REDUCE(termite_q4_k_activation_rhs_1x_reduce) "\n"
        \\
        ,
        program.metal(
            \\    const uint NR0 = 2u; const uint NSG = 4u; const uint NQ = 8u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;
            \\    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint sb0 = sgitg * NQ + ix; uint in_row = r * p.in_dim; uint total_subblocks = p.row_blocks * 8u; float acc0 = 0.0f; float acc1 = 0.0f;
            \\    for (uint sb = sb0; sb < total_subblocks; sb += NSG * NQ) { uint b = sb >> 3; uint sub = sb & 7u; uint chunk = sub >> 1; bool high = (sub & 1u) != 0u; uint j0 = il * NQ; uint input_off = in_row + b * 256u + sub * 32u + j0; float x[8]; for (uint i = 0; i < NQ; ++i) { x[i] = input[input_off + i]; } for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; float block = termite_q4k_scalar_dot8(x, weight, p.row_blocks, o, b, sub, chunk, high, j0); if (row == 0u) acc0 += block; else acc1 += block; } }
            \\    if (sgitg == 0u) { shmem[lane] = 0.0f; shmem[32u + lane] = 0.0f; } acc0 = simd_sum(acc0); acc1 = simd_sum(acc1); threadgroup_barrier(mem_flags::mem_threadgroup);
            \\    if (lane == 0u) { shmem[sgitg] = acc0; shmem[32u + sgitg] = acc1; } threadgroup_barrier(mem_flags::mem_threadgroup);
            \\    float total0 = simd_sum(shmem[lane]); float total1 = simd_sum(shmem[32u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; if (first_o < p.out_dim) output[idx] = termite_gated_activation_product(total0, rhs[idx], ap.activation_kind); if (first_o + 1u < p.out_dim) output[idx + 1u] = termite_gated_activation_product(total1, rhs[idx + 1u], ap.activation_kind); }
            \\}
            \\
        ),

        \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q5_k_linear) "\n"
        \\
        ,
        program.metal(
            \\    termite_legacy_linear<termite_legacy_q5_k, 176u>(input, weight, output, p, gid);
            \\}
            \\
        ),
    });
