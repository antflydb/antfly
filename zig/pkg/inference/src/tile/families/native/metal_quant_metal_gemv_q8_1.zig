// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q8_1_linear) "\n"
    \\           "    termite_legacy_linear<termite_legacy_q8_1, 36u>(input, weight, output, p, gid);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q8_1_linear_1x_reduce) "\n"
    \\           "    termite_legacy_reduce<termite_legacy_q8_1, 36u, 2u, 4u>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q8_0_linear_mmv) "\n"
    \\           "    termite_q8_0_mmv_single(input, weight + p.weight_offset, output, p.rows, p.in_dim, p.out_dim, p.row_blocks, shmem, lane, sgitg, tg.x * 2u, tg.y);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_F16_REDUCE(termite_q8_0_linear_mmv_in_f16) "\n"
    \\           "    termite_q8_0_mmv_single(input, weight + p.weight_offset, output, p.rows, p.in_dim, p.out_dim, p.row_blocks, shmem, lane, sgitg, tg.x * 2u, tg.y);\n"
    \\           "}\n"
    \\           "kernel void termite_q8_0_linear_rms_scale_mmv(device const float *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_linear_params &p [[buffer(3)]], device const float *norm_weight [[buffer(4)]], device const float *inv_scale [[buffer(5)]], threadgroup float *shmem [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {\n"
    \\           "    weight += p.weight_offset;\n"
    \\           "    const uint NR0 = 4u; const uint NSG = 4u; const uint NQ = 8u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;\n"
    \\           "    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint ib0 = uint(sgitg) * NQ + ix; uint in_row = r * p.in_dim; float scale = inv_scale[r]; float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};\n"
    \\           "    for (uint b = ib0; b < p.row_blocks; b += NSG * NQ) { float x[8]; uint in_off = in_row + b * 32u + il * NQ; uint norm_off = b * 32u + il * NQ; for (uint i = 0u; i < NQ; ++i) { x[i] = input[in_off + i] * norm_weight[norm_off + i] * scale; } for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 34u + b * 34u; float d = termite_q8_0_block_scale(weight, off); float block_acc = 0.0f; for (uint i = 0u; i < NQ; ++i) { block_acc += x[i] * termite_q8_0_block_value(weight, off, il * NQ + i); } acc[row] += d * block_acc; } }\n"
    \\           "    if (sgitg == 0u) { for (uint row = 0u; row < NR0; ++row) shmem[row * 32u + lane] = 0.0f; } for (uint row = 0u; row < NR0; ++row) acc[row] = simd_sum(acc[row]); threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    if (lane == 0u) { for (uint row = 0u; row < NR0; ++row) shmem[row * 32u + sgitg] = acc[row]; } threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    float total[4]; for (uint row = 0u; row < NR0; ++row) total[row] = simd_sum(shmem[row * 32u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o < p.out_dim) output[idx + row] = total[row]; } }\n"
    \\           "}\n"
    \\
;
