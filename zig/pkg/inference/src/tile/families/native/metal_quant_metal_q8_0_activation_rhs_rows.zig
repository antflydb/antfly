// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const program_0 =
    \\           TERMITE_MSL_ABI_QUANT_PAIR_ACTIVATION_SIMD({{name}}) " termite_q4_0_pair_activation_multiply_r_ext_impl(input, weight_gate, weight_up, output, p, ap, lane, sgitg, tg, {{literal_0}}); }\n"
    \\
;

const program_1 =
    \\           TERMITE_MSL_ABI_QUANT_GATHER({{name}}) "\n"
    \\           "    termite_quant_rows_decode<{{literal_0}}, true>(weight, ids, output, p, gid);\n"
    \\           "}\n"
    \\
;

const program_2 =
    \\           TERMITE_MSL_ABI_QUANT_DECODE_COPY({{name}}) "\n"
    \\           "    termite_quant_rows_decode<{{literal_0}}, false>(weight, nullptr, output, p, gid);\n"
    \\           "}\n"
    \\
;

pub const source_0 = program.join(&.{
    program.render(program_0, .{ .name = "termite_q4_0_pair_activation_multiply_r2_ext", .literal_0 = "2u" }),
    program.render(program_0, .{ .name = "termite_q4_0_pair_activation_multiply_r3_ext", .literal_0 = "3u" }),
    program.render(program_0, .{ .name = "termite_q4_0_pair_activation_multiply_r4_ext", .literal_0 = "4u" }),
    program.render(program_0, .{ .name = "termite_q4_0_pair_activation_multiply_r5_ext", .literal_0 = "5u" }),

    \\           "kernel void termite_q8_0_activation_multiply_1r_ext(device const float *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device const float *rhs [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_linear_params &p [[buffer(4)]], constant termite_metal_apply_activation_params &ap [[buffer(5)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {\n"
    \\           "    weight += p.weight_offset;\n"
    \\           "    const uint NX = 8u; const uint NY = 4u; const uint NSG = 4u; const uint CHPT = 4u; uint tx = uint(lane) & 7u; uint ty = uint(lane) >> 3; uint o = tg.x * (NY * NSG) + uint(sgitg) * NY + ty; uint r = tg.y; if (r >= p.rows || o >= p.out_dim) return; uint row_offset = o * p.row_blocks * 34u; uint total_chunks = p.row_blocks * 8u; uint input_row = r * p.in_dim; float acc = 0.0f;\n"
    \\           "    for (uint chunk = tx; chunk < total_chunks; chunk += CHPT * NX) { for (uint ch = 0u; ch < CHPT; ++ch) { uint c = chunk + ch * NX; if (c >= total_chunks) continue; uint b = c >> 3; uint ci = c & 7u; uint off = row_offset + b * 34u; ushort bits = (ushort(weight[off + 1u]) << 8) | ushort(weight[off]); float d = float(as_type<half>(bits)); float4 w = float4(float(as_type<char>(weight[off + 2u + ci * 4u])), float(as_type<char>(weight[off + 3u + ci * 4u])), float(as_type<char>(weight[off + 4u + ci * 4u])), float(as_type<char>(weight[off + 5u + ci * 4u]))) * d; uint in_col = input_row + c * 4u; float4 x = float4(input[in_col], input[in_col + 1u], input[in_col + 2u], input[in_col + 3u]); acc += dot(x, w); } }\n"
    \\           "    acc += simd_shuffle_down(acc, 4u); acc += simd_shuffle_down(acc, 2u); acc += simd_shuffle_down(acc, 1u); if (tx == 0u) { uint idx = r * p.out_dim + o; output[idx] = termite_gated_activation_product(acc, rhs[idx], ap.activation_kind); }\n"
    \\           "}\n"
    \\
    ,

    \\           TERMITE_MSL_ABI_QUANT_RHS_REDUCE(termite_q8_0_activation_multiply_1x_reduce4) "\n"
    \\           "    weight += p.weight_offset;\n"
    \\           "    const uint NR0 = 4u; const uint NSG = 4u; const uint NQ = 8u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;\n"
    \\           "    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint ib0 = sgitg * NQ + ix; uint in_row = r * p.in_dim; float acc0 = 0.0f; float acc1 = 0.0f; float acc2 = 0.0f; float acc3 = 0.0f;\n"
    \\           "    for (uint b = ib0; b < p.row_blocks; b += NSG * NQ) { float x[8]; uint in_off = in_row + b * 32u + il * NQ; for (uint i = 0; i < NQ; ++i) { x[i] = input[in_off + i]; } for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 34u + b * 34u; ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]); float d = float(as_type<half>(bits)); float block = 0.0f; for (uint i = 0; i < NQ; ++i) block += x[i] * float(as_type<char>(weight[off + 2u + il * NQ + i])); if (row == 0u) acc0 += d * block; else if (row == 1u) acc1 += d * block; else if (row == 2u) acc2 += d * block; else acc3 += d * block; } }\n"
    \\           "    if (sgitg == 0u) { shmem[lane] = 0.0f; shmem[32u + lane] = 0.0f; shmem[64u + lane] = 0.0f; shmem[96u + lane] = 0.0f; } acc0 = simd_sum(acc0); acc1 = simd_sum(acc1); acc2 = simd_sum(acc2); acc3 = simd_sum(acc3); threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    if (lane == 0u) { shmem[sgitg] = acc0; shmem[32u + sgitg] = acc1; shmem[64u + sgitg] = acc2; shmem[96u + sgitg] = acc3; } threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    float total0 = simd_sum(shmem[lane]); float total1 = simd_sum(shmem[32u + lane]); float total2 = simd_sum(shmem[64u + lane]); float total3 = simd_sum(shmem[96u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; if (first_o < p.out_dim) output[idx] = termite_gated_activation_product(total0, rhs[idx], ap.activation_kind); if (first_o + 1u < p.out_dim) output[idx + 1u] = termite_gated_activation_product(total1, rhs[idx + 1u], ap.activation_kind); if (first_o + 2u < p.out_dim) output[idx + 2u] = termite_gated_activation_product(total2, rhs[idx + 2u], ap.activation_kind); if (first_o + 3u < p.out_dim) output[idx + 3u] = termite_gated_activation_product(total3, rhs[idx + 3u], ap.activation_kind); }\n"
    \\           "}\n"
    \\
});

pub const source_1 = program.render(program_1, .{ .name = "termite_q4_0_get_rows", .literal_0 = "0u" });

pub const source_2 = program.join(&.{
    \\           TERMITE_MSL_ABI_QUANT_SCATTER(termite_q4_0_set_rows) "\n"
    \\           "    termite_q4_0_encode_rows_impl<true>(input, ids, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_2, .{ .name = "termite_q4_0_cpy_q_to_f32", .literal_0 = "0u" }),

    \\           TERMITE_MSL_ABI_QUANT_ENCODE_COPY(termite_q4_0_cpy_f32_to_q) "\n"
    \\           "    termite_q4_0_encode_rows_impl<false>(input, nullptr, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_1, .{ .name = "termite_q4_1_get_rows", .literal_0 = "1u" }),
});

pub const source_3 = program.join(&.{
    \\           TERMITE_MSL_ABI_QUANT_SCATTER(termite_q4_1_set_rows) "\n"
    \\           "    termite_q4_1_encode_rows_impl<true>(input, ids, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_2, .{ .name = "termite_q4_1_cpy_q_to_f32", .literal_0 = "1u" }),

    \\           TERMITE_MSL_ABI_QUANT_ENCODE_COPY(termite_q4_1_cpy_f32_to_q) "\n"
    \\           "    termite_q4_1_encode_rows_impl<false>(input, nullptr, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_1, .{ .name = "termite_q5_0_get_rows", .literal_0 = "2u" }),
});

pub const source_4 = program.join(&.{
    \\           TERMITE_MSL_ABI_QUANT_SCATTER(termite_q5_0_set_rows) "\n"
    \\           "    termite_q5_0_encode_rows_impl<true>(input, ids, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_2, .{ .name = "termite_q5_0_cpy_q_to_f32", .literal_0 = "2u" }),

    \\           TERMITE_MSL_ABI_QUANT_ENCODE_COPY(termite_q5_0_cpy_f32_to_q) "\n"
    \\           "    termite_q5_0_encode_rows_impl<false>(input, nullptr, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_1, .{ .name = "termite_q5_1_get_rows", .literal_0 = "3u" }),
});

pub const source_5 = program.join(&.{
    \\           TERMITE_MSL_ABI_QUANT_SCATTER(termite_q5_1_set_rows) "\n"
    \\           "    termite_q5_1_encode_rows_impl<true>(input, ids, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_2, .{ .name = "termite_q5_1_cpy_q_to_f32", .literal_0 = "3u" }),

    \\           TERMITE_MSL_ABI_QUANT_ENCODE_COPY(termite_q5_1_cpy_f32_to_q) "\n"
    \\           "    termite_q5_1_encode_rows_impl<false>(input, nullptr, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_1, .{ .name = "termite_q4_k_get_rows", .literal_0 = "4u" }),
});

pub const source_6 = program.join(&.{
    \\           TERMITE_MSL_ABI_QUANT_SCATTER(termite_q4_k_set_rows) "\n"
    \\           "    termite_q4_k_encode_rows_impl<true>(input, ids, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_2, .{ .name = "termite_q4_k_cpy_q_to_f32", .literal_0 = "4u" }),

    \\           TERMITE_MSL_ABI_QUANT_ENCODE_COPY(termite_q4_k_cpy_f32_to_q) "\n"
    \\           "    termite_q4_k_encode_rows_impl<false>(input, nullptr, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_1, .{ .name = "termite_q5_k_get_rows", .literal_0 = "5u" }),
});

pub const source_7 = program.join(&.{
    \\           TERMITE_MSL_ABI_QUANT_SCATTER(termite_q5_k_set_rows) "\n"
    \\           "    termite_q5_k_encode_rows_impl<true>(input, ids, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_2, .{ .name = "termite_q5_k_cpy_q_to_f32", .literal_0 = "5u" }),

    \\           TERMITE_MSL_ABI_QUANT_ENCODE_COPY(termite_q5_k_cpy_f32_to_q) "\n"
    \\           "    termite_q5_k_encode_rows_impl<false>(input, nullptr, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_1, .{ .name = "termite_q6_k_get_rows", .literal_0 = "6u" }),
});

pub const source_8 = program.join(&.{
    \\           TERMITE_MSL_ABI_QUANT_SCATTER(termite_q6_k_set_rows) "\n"
    \\           "    termite_q6_k_encode_rows_impl<true>(input, ids, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_2, .{ .name = "termite_q6_k_cpy_q_to_f32", .literal_0 = "6u" }),

    \\           TERMITE_MSL_ABI_QUANT_ENCODE_COPY(termite_q6_k_cpy_f32_to_q) "\n"
    \\           "    termite_q6_k_encode_rows_impl<false>(input, nullptr, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_1, .{ .name = "termite_q8_0_get_rows", .literal_0 = "7u" }),
});

pub const source_9 = program.join(&.{
    \\           TERMITE_MSL_ABI_QUANT_SCATTER(termite_q8_0_set_rows) "\n"
    \\           "    termite_q8_0_encode_rows_impl<true>(input, ids, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_2, .{ .name = "termite_q8_0_cpy_q_to_f32", .literal_0 = "7u" }),

    \\           TERMITE_MSL_ABI_QUANT_ENCODE_COPY(termite_q8_0_cpy_f32_to_q) "\n"
    \\           "    termite_q8_0_encode_rows_impl<false>(input, nullptr, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_1, .{ .name = "termite_q8_1_get_rows", .literal_0 = "8u" }),
});

pub const source_10 = program.join(&.{
    \\           TERMITE_MSL_ABI_QUANT_SCATTER(termite_q8_1_set_rows) "\n"
    \\           "    termite_q8_1_encode_rows_impl<true>(input, ids, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,
    program.render(program_2, .{ .name = "termite_q8_1_cpy_q_to_f32", .literal_0 = "8u" }),

    \\           TERMITE_MSL_ABI_QUANT_ENCODE_COPY(termite_q8_1_cpy_f32_to_q) "\n"
    \\           "    termite_q8_1_encode_rows_impl<false>(input, nullptr, weight, p, gid);\n"
    \\           "}\n"
    \\
    ,

    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q8_k_linear) "\n"
    \\           "    termite_legacy_linear<termite_legacy_q8_k, 292u>(input, weight, output, p, gid);\n"
    \\           "}\n"
    \\
    ,

    \\           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q8_k_linear_1x_reduce) "\n"
    \\           "    const uint NR0 = 2u; const uint NSG = 2u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;\n"
    \\           "    uint ix = lane / 4u; uint il = lane - ix * 4u; uint chunk0 = uint(sgitg) * 8u + ix; uint in_row = r * p.in_dim; uint total_chunks = p.row_blocks * 64u; float acc0 = 0.0f; float acc1 = 0.0f;\n"
    \\           "    for (uint chunk = chunk0; chunk < total_chunks; chunk += NSG * 8u) { uint b = chunk >> 6; uint qj = ((chunk & 63u) << 2) + il; float x = input[in_row + b * 256u + qj]; for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 292u + b * 292u; uint bits = uint(weight[off]) | (uint(weight[off + 1u]) << 8) | (uint(weight[off + 2u]) << 16) | (uint(weight[off + 3u]) << 24); float d = as_type<float>(bits); char q = as_type<char>(weight[off + 4u + qj]); float block = x * (d * float(q)); if (row == 0u) acc0 += block; else acc1 += block; } }\n"
    \\           "    float acc[2] = {acc0, acc1};\n"
    \\           "    termite_legacy_reduce_store<2u>(acc, shmem, output, p, r, first_o, lane, sgitg);\n"
    \\           "}\n"
    \\
});
