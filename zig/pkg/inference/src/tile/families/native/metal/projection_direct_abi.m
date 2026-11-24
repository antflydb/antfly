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

// native-program:entry termite_q4_0_linear_id_a4b_down_nq8
           TERMITE_MSL_ABI_QUANT_EXPERT_SIMD(termite_q4_0_linear_id_a4b_down_nq8) "\n"
           "    const uint NR0 = 4u; const uint NSG = 2u; const uint NQ = 8u; const uint NW = 32u; const uint ROW_BLOCKS = 22u; const uint IN_DIM = 704u; const uint OUT_DIM = 2816u; const uint EXPERTS = 128u; uint first_o = (tg.x * NSG + uint(sgitg)) * NR0; uint route = tg.y; if (route >= 8u || lane >= NW) return; uint expert = expert_ids[route];\n"
           "    if (expert >= EXPERTS) { if (lane == 0u) { uint idx = route * OUT_DIM + first_o; TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) output[idx + row] = 0.0f; } return; }\n"
           "    uint ix = uint(lane) >> 2u; uint il = uint(lane) & 3u; uint qj0 = il * NQ; uint byte_base = (il & 1u) * 8u; bool high = il >= 2u; uint in_row = route * IN_DIM; uint expert_row = expert * OUT_DIM; float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};\n"
           "    for (uint b = ix; b < ROW_BLOCKS; b += NQ) { uint in_off = in_row + b * 32u + qj0; float x0 = input[in_off]; float x1 = input[in_off + 1u]; float x2 = input[in_off + 2u]; float x3 = input[in_off + 3u]; float x4 = input[in_off + 4u]; float x5 = input[in_off + 5u]; float x6 = input[in_off + 6u]; float x7 = input[in_off + 7u]; TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) { uint off = (expert_row + first_o + row) * 396u + b * 18u; ushort bits = (ushort(weight[off + 1u]) << 8) | ushort(weight[off]); float d = float(as_type<half>(bits)); const device ushort *qs = reinterpret_cast<const device ushort *>(weight + off + 2u + byte_base); uint p0 = uint(qs[0]); uint p1 = uint(qs[1]); uint p2 = uint(qs[2]); uint p3 = uint(qs[3]); uint shift0 = high ? 4u : 0u; uint shift1 = shift0 + 8u; float a0 = x0 * float(int((p0 >> shift0) & 15u) - 8) + x1 * float(int((p0 >> shift1) & 15u) - 8); float a1 = x2 * float(int((p1 >> shift0) & 15u) - 8) + x3 * float(int((p1 >> shift1) & 15u) - 8); float a2 = x4 * float(int((p2 >> shift0) & 15u) - 8) + x5 * float(int((p2 >> shift1) & 15u) - 8); float a3 = x6 * float(int((p3 >> shift0) & 15u) - 8) + x7 * float(int((p3 >> shift1) & 15u) - 8); acc[row] += d * ((a0 + a1) + (a2 + a3)); } }\n"
           "    TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) acc[row] = simd_sum(acc[row]); if (lane == 0u) { uint idx = route * OUT_DIM + first_o; TERMITE_FOR_UNROLL (uint row = 0u; row < NR0; ++row) output[idx + row] = acc[row]; }\n"
           "}\n"
// native-program:entry termite_q4_0_pair_linear
           TERMITE_MSL_ABI_QUANT_PAIR_SCALAR(termite_q4_0_pair_linear) "\n"
           "    uint o = tid.x; uint r = tid.y; if (o >= p.out_dim || r >= p.rows) return; uint row_offset = o * p.row_blocks * 18u; float acc_a = 0.0f; float acc_b = 0.0f;\n"
           "    for (uint b = 0; b < p.row_blocks; ++b) { uint off = row_offset + b * 18u; ushort bits_a = (ushort(weight_a[off + 1]) << 8) | ushort(weight_a[off]); ushort bits_b = (ushort(weight_b[off + 1]) << 8) | ushort(weight_b[off]); float d_a = float(as_type<half>(bits_a)); float d_b = float(as_type<half>(bits_b)); const device uchar *qs_a = weight_a + off + 2u; const device uchar *qs_b = weight_b + off + 2u; uint in_off = r * p.in_dim + b * 32u; for (uint i = 0; i < 16; ++i) { float x_lo = input[in_off + i]; float x_hi = input[in_off + 16u + i]; uchar packed_a = qs_a[i]; uchar packed_b = qs_b[i]; float lo_a = float(int(packed_a & 0x0Fu) - 8); float hi_a = float(int((packed_a >> 4) & 0x0Fu) - 8); float lo_b = float(int(packed_b & 0x0Fu) - 8); float hi_b = float(int((packed_b >> 4) & 0x0Fu) - 8); acc_a += x_lo * (d_a * lo_a) + x_hi * (d_a * hi_a); acc_b += x_lo * (d_b * lo_b) + x_hi * (d_b * hi_b); } }\n"
           "    output_a[r * p.out_dim + o] = acc_a;\n"
           "    output_b[r * p.out_dim + o] = acc_b;\n"
           "}\n"
// native-program:entry termite_q4_0_pair_linear_1x_reduce
           TERMITE_MSL_ABI_QUANT_PAIR_PROJECTION_REDUCE(termite_q4_0_pair_linear_1x_reduce) "\n"
           "    const uint NR0 = 2u; const uint NSG = 4u; const uint NQ = 16u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;\n"
           "    uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * 8u; uint ib0 = sgitg * NQ + ix; uint in_row = r * p.in_dim; float acc_a0 = 0.0f; float acc_a1 = 0.0f; float acc_b0 = 0.0f; float acc_b1 = 0.0f;\n"
           "    for (uint b = ib0; b < p.row_blocks; b += NSG * NQ) { float yl[16]; float sumy = 0.0f; uint in_off = in_row + b * 32u + il; for (uint i = 0u; i < 8u; i += 2u) { float x0 = input[in_off + i]; float x1 = input[in_off + i + 1u]; float x16 = input[in_off + 16u + i]; float x17 = input[in_off + 16u + i + 1u]; sumy += x0 + x1 + x16 + x17; yl[i] = x0; yl[i + 1u] = x1 / 256.0f; yl[i + 8u] = x16 / 16.0f; yl[i + 9u] = x17 / 4096.0f; } for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 18u + b * 18u; ushort bits_a = (ushort(weight_a[off + 1]) << 8) | ushort(weight_a[off]); ushort bits_b = (ushort(weight_b[off + 1]) << 8) | ushort(weight_b[off]); float d_a = float(as_type<half>(bits_a)); float d_b = float(as_type<half>(bits_b)); const device ushort *qs_a = reinterpret_cast<const device ushort *>(weight_a + off + 2u) + (il >> 1); const device ushort *qs_b = reinterpret_cast<const device ushort *>(weight_b + off + 2u) + (il >> 1); float qacc_a = 0.0f; float qacc_b = 0.0f; for (uint i = 0u; i < 8u; i += 2u) { uint packed_a = uint(qs_a[i >> 1]); uint packed_b = uint(qs_b[i >> 1]); qacc_a += yl[i] * float(packed_a & 0x000Fu); qacc_a += yl[i + 1u] * float(packed_a & 0x0F00u); qacc_a += yl[i + 8u] * float(packed_a & 0x00F0u); qacc_a += yl[i + 9u] * float(packed_a & 0xF000u); qacc_b += yl[i] * float(packed_b & 0x000Fu); qacc_b += yl[i + 1u] * float(packed_b & 0x0F00u); qacc_b += yl[i + 8u] * float(packed_b & 0x00F0u); qacc_b += yl[i + 9u] * float(packed_b & 0xF000u); } if (row == 0u) { acc_a0 += d_a * (qacc_a - 8.0f * sumy); acc_b0 += d_b * (qacc_b - 8.0f * sumy); } else { acc_a1 += d_a * (qacc_a - 8.0f * sumy); acc_b1 += d_b * (qacc_b - 8.0f * sumy); } } }\n"
{{q4k_pair_reduce}}
// native-program:entry termite_q4_0_linear_1x_reduce
           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q4_0_linear_1x_reduce) "\n"
           "    termite_q4_0_scalar_packed16_impl<8u, 2u, float, float>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
           "}\n"
// native-program:entry termite_q4_0_linear_1x_reduce_small
           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q4_0_linear_1x_reduce_small) "\n"
           "    termite_q4_0_scalar_packed16_impl<4u, 2u, float, float>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
           "}\n"
// native-program:entry termite_q4_0_linear_1x_reduce_nr4_nsg4
           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q4_0_linear_1x_reduce_nr4_nsg4) "\n"
           "    termite_q4_0_scalar_packed16_impl<4u, 4u, float, float>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
           "}\n"
// native-program:entry termite_q4_0_linear_1x_reduce_nr8_nsg4
           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q4_0_linear_1x_reduce_nr8_nsg4) "\n"
           "    termite_q4_0_scalar_packed16_impl<8u, 4u, float, float>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
           "}\n"
// native-program:entry termite_q8_0_pair_linear
           TERMITE_MSL_ABI_QUANT_PAIR_PROJECTION(termite_q8_0_pair_linear) "\n"
           "    weight_a += p.weight_offset; weight_b += p.second_weight_offset;\n"
           "    uint total = p.rows * p.out_dim; if (gid >= total) return; uint r = gid / p.out_dim; uint o = gid % p.out_dim; uint row_offset = o * p.row_blocks * 34u; float acc_a = 0.0f; float acc_b = 0.0f;\n"
           "    for (uint b = 0; b < p.row_blocks; ++b) { uint off = row_offset + b * 34u; float d_a = termite_q8_0_block_scale(weight_a, off); float d_b = termite_q8_0_block_scale(weight_b, off); float block_acc_a = 0.0f; float block_acc_b = 0.0f; uint in_off = r * p.in_dim + b * 32u; for (uint i = 0; i < 32; ++i) { float x = input[in_off + i]; block_acc_a += x * termite_q8_0_block_value(weight_a, off, i); block_acc_b += x * termite_q8_0_block_value(weight_b, off, i); } acc_a += d_a * block_acc_a; acc_b += d_b * block_acc_b; }\n"
           "    output_a[gid] = acc_a; output_b[gid] = acc_b;\n"
           "}\n"
// native-program:entry termite_q8_0_pair_linear_mmv
           TERMITE_MSL_ABI_QUANT_PAIR_PROJECTION_REDUCE(termite_q8_0_pair_linear_mmv) "\n"
           "    termite_q8_0_mmv_pair(input, weight_a + p.weight_offset, weight_b + p.second_weight_offset, output_a, output_b, p.rows, p.in_dim, p.out_dim, p.row_blocks, shmem, lane, sgitg, tg.x * 2u, tg.y);\n"
           "}\n"
// native-program:entry termite_i2_s_linear
           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_i2_s_linear) "\n"
           "    termite_i2_s_projection<false, false >(input, weight, nullptr, nullptr, output, nullptr, p, gid);\n"
           "}\n"
// native-program:entry termite_i2_s_pair_linear
           TERMITE_MSL_ABI_QUANT_PAIR_PROJECTION(termite_i2_s_pair_linear) "\n"
           "    termite_i2_s_projection<true, false >(input, weight_a, weight_b, nullptr, output_a, output_b, p, gid);\n"
           "}\n"
// native-program:entry termite_apply_linear_multi_row_tiled32_f32
           TERMITE_MSL_ABI_DENSE_TILED(termite_apply_linear_multi_row_tiled32_f32) "\n"
           "    threadgroup float a_tile[8][32]; threadgroup float b_tile[32][32];\n"
           "    termite_dense_tile<8u, 32u, false>(input, weight, bias, output, p, 0u, &a_tile[0][0], &b_tile[0][0], tid, tg);\n"
           "}\n"
// native-program:entry termite_apply_linear_multi_row_tiled32_activation_f32
           TERMITE_MSL_ABI_DENSE_ACTIVATION_TILED(termite_apply_linear_multi_row_tiled32_activation_f32) "\n"
           "    threadgroup float a_tile[8][32]; threadgroup float b_tile[32][32];\n"
           "    termite_dense_tile<8u, 32u, true>(input, weight, bias, output, p, ap.activation_kind, &a_tile[0][0], &b_tile[0][0], tid, tg);\n"
           "}\n"
// native-program:entry termite_apply_linear_multi_row_tiled32_m16_activation_f32
           TERMITE_MSL_ABI_DENSE_ACTIVATION_TILED(termite_apply_linear_multi_row_tiled32_m16_activation_f32) "\n"
           "    threadgroup float a_tile[16][32]; threadgroup float b_tile[32][32];\n"
           "    termite_dense_tile<16u, 32u, true>(input, weight, bias, output, p, ap.activation_kind, &a_tile[0][0], &b_tile[0][0], tid, tg);\n"
           "}\n"
// native-program:entry termite_apply_linear_bf16_mm_m64_n32_vec4
           TERMITE_MSL_ABI_BF16_LINEAR_MATRIX(termite_apply_linear_bf16_mm_m64_n32_vec4) "\n"
           "    // Preserve the production M64/N32/8-SG layout and F32 accumulation order.\n"
           "    threadgroup float4 storage[768];\n"
           "    threadgroup float *shared = reinterpret_cast<threadgroup float *>(storage);\n"
           "    threadgroup float *a = shared, *b = shared + 2048;\n"
           "    const uint row0 = tg.y * 64u, col0 = tg.x * 32u;\n"
           "    const uint sr = (uint(sg) >> 1u) * 16u, sc = (uint(sg) & 1u) * 16u;\n"
           "    simdgroup_float8x8 acc[4];\n"
           "    TERMITE_FOR_UNROLL(uint i = 0u; i < 4u; ++i) acc[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);\n"
           "    for (uint k0 = 0u; k0 < p.in_dim; k0 += 32u) {\n"
           "        // Dispatch admits aligned input/weight bases. Aligned row strides\n"
           "        // and a complete tile let each lane issue float4/ushort4 global loads.\n"
           "        // Every row, column, K tail and misaligned slice uses scalar staging.\n"
           "        const bool full = (p.in_dim & 3u) == 0u &&\n"
           "            row0 + 64u <= p.rows && col0 + 32u <= p.out_dim && k0 + 32u <= p.in_dim;\n"
           "        if (full) {\n"
           "            termite_bf16_matrix_stage_input_vec4(input, a, p, row0, k0, tid);\n"
           "            for (uint i = uint(tid); i < 256u; i += 256u) {\n"
           "                uint c = i / 8u, k = (i % 8u) * 4u;\n"
           "                ushort4 bits = *reinterpret_cast<device const ushort4 *>(weight + (col0 + c) * p.in_dim + k0 + k);\n"
           "                float4 values = as_type<float4>(uint4(bits) << 16u);\n"
           "                uint j = (k / 8u * 4u + c / 8u) * 64u + k % 8u * 8u + c % 8u;\n"
           "                TERMITE_FOR_UNROLL(uint q = 0u; q < 4u; ++q) b[j + q * 8u] = values[q];\n"
           "            }\n"
           "        } else {\n"
           "        termite_bf16_matrix_stage_input_scalar(input, a, p, row0, k0, tid);\n"
           "        for (uint i = uint(tid); i < 1024u; i += 256u) {\n"
           "            uint c = i / 32u, k = i % 32u;\n"
           "            b[(k / 8u * 4u + c / 8u) * 64u + k % 8u * 8u + c % 8u] =\n"
           "                col0 + c < p.out_dim && k0 + k < p.in_dim ? termite_bf16_to_f32(weight[(col0 + c) * p.in_dim + k0 + k]) : 0.0f;\n"
           "        }\n"
           "        }\n"
           "\n"
           TERMITE_MSL_BF16_LINEAR_MMA
           "    }\n"
           TERMITE_MSL_BF16_LINEAR_PUBLISH
           "}\n"
// native-program:entry termite_apply_linear_bf16_mm_m64_n32
           TERMITE_MSL_ABI_BF16_LINEAR_MATRIX(termite_apply_linear_bf16_mm_m64_n32) "\n"
           "    // BF16 is expanded exactly to F32; activations and accumulators remain F32.\n"
           "    // Eight simdgroups each own a 16x16 region of the 64x32 result tile.\n"
           "    threadgroup float shared[3072];\n"
           "    threadgroup float *a = shared;\n"
           "    threadgroup float *b = shared + 2048;\n"
           "    const uint row0 = tg.y * 64u, col0 = tg.x * 32u;\n"
           "    const uint sr = (uint(sg) >> 1u) * 16u, sc = (uint(sg) & 1u) * 16u;\n"
           "    simdgroup_float8x8 acc[4];\n"
           "    TERMITE_FOR_UNROLL(uint i = 0u; i < 4u; ++i) acc[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);\n"
           "    for (uint k0 = 0u; k0 < p.in_dim; k0 += 32u) {\n"
           "        termite_bf16_matrix_stage_input_scalar(input, a, p, row0, k0, tid);\n"
           "        for (uint i = uint(tid); i < 1024u; i += 256u) {\n"
           "            uint c = i / 32u, k = i % 32u;\n"
           "            b[(k / 8u * 4u + c / 8u) * 64u + k % 8u * 8u + c % 8u] = col0 + c < p.out_dim && k0 + k < p.in_dim ? termite_bf16_to_f32(weight[(col0 + c) * p.in_dim + k0 + k]) : 0.0f;\n"
           "        }\n"
           TERMITE_MSL_BF16_LINEAR_MMA
           "    }\n"
           TERMITE_MSL_BF16_LINEAR_PUBLISH
           "}\n"
// native-program:entry termite_q4_k_linear_1x_reduce_v2
           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q4_k_linear_1x_reduce_v2) "\n"
           "    const uint NR0 = 2u; const uint NSG = 2u; const uint nb = p.row_blocks; uint first_row = (tg.x * NSG + uint(sgitg)) * NR0; uint r = tg.y; if (r >= p.rows || first_row >= p.out_dim) return;\n"
           "    const uint row_bytes = nb * 144u; const uint step16 = row_bytes / 2u; const ushort ix = lane / 8u; const ushort it = lane % 8u; const ushort iq = it / 4u; const ushort ir = it % 4u; const uint row_limit = min(NR0, p.out_dim - first_row);\n"
           "    device const uchar *x0 = weight + first_row * row_bytes; device const float *yy = input + r * p.in_dim; float yl[16]; float yh[16]; float sumf[2] = {0.0f, 0.0f}; ushort sc16[4]; thread const uchar *sc8 = reinterpret_cast<thread const uchar *>(sc16);\n"
           "    device const float *y4 = yy + ix * 256u + 64u * uint(iq) + 8u * uint(ir);\n"
           "    for (uint ib = ix; ib < nb; ib += 4u) {\n"
           "        float4 sumy = {0.0f, 0.0f, 0.0f, 0.0f};\n"
           "        for (uint i = 0; i < 8u; ++i) { yl[i] = y4[i]; sumy[0] += yl[i]; yl[i + 8u] = y4[i + 32u]; sumy[1] += yl[i + 8u]; yh[i] = y4[i + 128u]; sumy[2] += yh[i]; yh[i + 8u] = y4[i + 160u]; sumy[3] += yh[i + 8u]; }\n"
           "        device const ushort *sc = reinterpret_cast<device const ushort *>(x0 + ib * 144u + 4u) + iq;\n"
           "        device const ushort *q1 = reinterpret_cast<device const ushort *>(x0 + ib * 144u + 16u) + 16u * uint(iq) + 4u * uint(ir);\n"
           "        device const half *dh = reinterpret_cast<device const half *>(x0 + ib * 144u);\n"
           "        for (uint row = 0; row < row_limit; ++row) {\n"
           "            sc16[0] = sc[0] & 0x3f3f; sc16[1] = sc[2] & 0x3f3f; sc16[2] = ((sc[4] >> 0) & 0x0f0f) | ((sc[0] & 0xc0c0) >> 2); sc16[3] = ((sc[4] >> 4) & 0x0f0f) | ((sc[2] & 0xc0c0) >> 2);\n"
           "            device const ushort *q2 = q1 + 32u; float4 acc1 = {0.0f, 0.0f, 0.0f, 0.0f}; float4 acc2 = {0.0f, 0.0f, 0.0f, 0.0f};\n"
           "            for (uint i = 0; i < 8u; i += 2u) { acc1[0] += yl[i] * float(q1[i / 2u] & 0x000F); acc1[1] += yl[i + 1u] * float(q1[i / 2u] & 0x0F00); acc1[2] += yl[i + 8u] * float(q1[i / 2u] & 0x00F0); acc1[3] += yl[i + 9u] * float(q1[i / 2u] & 0xF000); acc2[0] += yh[i] * float(q2[i / 2u] & 0x000F); acc2[1] += yh[i + 1u] * float(q2[i / 2u] & 0x0F00); acc2[2] += yh[i + 8u] * float(q2[i / 2u] & 0x00F0); acc2[3] += yh[i + 9u] * float(q2[i / 2u] & 0xF000); }\n"
           "            float dall = float(dh[0]); float dmin = float(dh[1]);\n"
           "            sumf[row] += dall * ((acc1[0] + (1.0f / 256.0f) * acc1[1]) * float(sc8[0]) + (acc1[2] + (1.0f / 256.0f) * acc1[3]) * float(sc8[1]) * (1.0f / 16.0f) + (acc2[0] + (1.0f / 256.0f) * acc2[1]) * float(sc8[4]) + (acc2[2] + (1.0f / 256.0f) * acc2[3]) * float(sc8[5]) * (1.0f / 16.0f)) - dmin * (sumy[0] * float(sc8[2]) + sumy[1] * float(sc8[3]) + sumy[2] * float(sc8[6]) + sumy[3] * float(sc8[7]));\n"
           "            q1 += step16; sc += step16; dh += step16;\n"
           "        }\n"
           "        y4 += 1024u;\n"
           "    }\n"
           "    for (uint row = 0; row < row_limit; ++row) { float total = simd_sum(sumf[row]); if (lane == 0u) output[r * p.out_dim + first_row + row] = total; }\n"
           "}\n"
// native-program:entry termite_q4_k_pair_linear
           TERMITE_MSL_ABI_QUANT_PAIR_SCALAR(termite_q4_k_pair_linear) "\n"
           "    uint o = tid.x; uint r = tid.y; if (o >= p.out_dim || r >= p.rows) return; uint row_offset = o * p.row_blocks * 144u; float acc_a = 0.0f; float acc_b = 0.0f;\n"
           "    for (uint b = 0; b < p.row_blocks; ++b) { uint off_a = row_offset + b * 144u; uint off_b = row_offset + b * 144u; ushort d_bits_a = (ushort(weight_a[off_a + 1]) << 8) | ushort(weight_a[off_a]); ushort dmin_bits_a = (ushort(weight_a[off_a + 3]) << 8) | ushort(weight_a[off_a + 2]); ushort d_bits_b = (ushort(weight_b[off_b + 1]) << 8) | ushort(weight_b[off_b]); ushort dmin_bits_b = (ushort(weight_b[off_b + 3]) << 8) | ushort(weight_b[off_b + 2]); float d_a = float(as_type<half>(d_bits_a)); float dmin_a = float(as_type<half>(dmin_bits_a)); float d_b = float(as_type<half>(d_bits_b)); float dmin_b = float(as_type<half>(dmin_bits_b)); const device uchar *scales_a = weight_a + off_a + 4u; const device uchar *scales_b = weight_b + off_b + 4u; const device uchar *qs_a = weight_a + off_a + 16u; const device uchar *qs_b = weight_b + off_b + 16u; thread float scs_a[8]; thread float mins_a[8]; thread float scs_b[8]; thread float mins_b[8]; unpack_q4k_scale_mins(scales_a, scs_a, mins_a); unpack_q4k_scale_mins(scales_b, scs_b, mins_b); uint q_off = 0u; for (uint chunk = 0; chunk < 4; ++chunk) { uint sub = chunk * 2u; float dsc0_a = d_a * scs_a[sub]; float dmn0_a = dmin_a * mins_a[sub]; float dsc1_a = d_a * scs_a[sub + 1u]; float dmn1_a = dmin_a * mins_a[sub + 1u]; float dsc0_b = d_b * scs_b[sub]; float dmn0_b = dmin_b * mins_b[sub]; float dsc1_b = d_b * scs_b[sub + 1u]; float dmn1_b = dmin_b * mins_b[sub + 1u]; uint input_off = r * p.in_dim + b * 256u + chunk * 64u; for (uint i = 0; i < 32; ++i) { uchar packed_a = qs_a[q_off + i]; uchar packed_b = qs_b[q_off + i]; float x0 = input[input_off + i]; float x1 = input[input_off + 32u + i]; acc_a += x0 * (dsc0_a * float(packed_a & 0x0Fu) - dmn0_a); acc_a += x1 * (dsc1_a * float(packed_a >> 4) - dmn1_a); acc_b += x0 * (dsc0_b * float(packed_b & 0x0Fu) - dmn0_b); acc_b += x1 * (dsc1_b * float(packed_b >> 4) - dmn1_b); } q_off += 32u; } }\n"
           "    output_a[r * p.out_dim + o] = acc_a;\n"
           "    output_b[r * p.out_dim + o] = acc_b;\n"
           "}\n"
// native-program:entry termite_q5_k_linear_1x_reduce
           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q5_k_linear_1x_reduce) "\n"
           "    const uint NSG = 2u; const uint NQ = 8u; const uint NW = 32u; uint o = tg.x; uint r = tg.y; if (o >= p.out_dim || r >= p.rows || lane >= NW) return;\n"
           "    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint sb0 = sgitg * NQ + ix; uint in_row = r * p.in_dim; uint total_subblocks = p.row_blocks * 8u; float acc = 0.0f;\n"
           "    for (uint sb = sb0; sb < total_subblocks; sb += NSG * NQ) { uint b = sb >> 3; uint sub = sb & 7u; uint chunk = sub >> 1; bool high = (sub & 1u) != 0u; uint j0 = il * NQ; uint off = o * p.row_blocks * 176u + b * 176u; ushort d_bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]); ushort dmin_bits = (ushort(weight[off + 3]) << 8) | ushort(weight[off + 2]); float d = float(as_type<half>(d_bits)); float dmin = float(as_type<half>(dmin_bits)); const device uchar *scales = weight + off + 4u; const device uchar *qh = weight + off + 16u; const device uchar *ql = weight + off + 48u; thread float scs[8]; thread float mins[8]; unpack_q4k_scale_mins(scales, scs, mins); float dsc = d * scs[sub]; float dmn = dmin * mins[sub]; uint ql_off = chunk * 32u + j0; uint input_off = in_row + b * 256u + sub * 32u + j0; for (uint i = 0; i < NQ; ++i) { uchar low = high ? (ql[ql_off + i] >> 4) : (ql[ql_off + i] & 0x0Fu); int q = int(low) + int((qh[j0 + i] >> sub) & 1u) * 16; acc += input[input_off + i] * (dsc * float(q) - dmn); } }\n"
           "    if (sgitg == 0u) shmem[lane] = 0.0f; acc = simd_sum(acc); threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    if (lane == 0u) shmem[sgitg] = acc; threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    float total = simd_sum(shmem[lane]); if (lane == 0u && sgitg == 0u) output[r * p.out_dim + o] = total;\n"
           "}\n"
// native-program:entry termite_q6_k_linear_1x_reduce
           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q6_k_linear_1x_reduce) "\n"
           "    termite_q6_k_scalar_even_impl<2u>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
           "}\n"
// native-program:entry termite_q6_k_linear_1x_reduce_nsg4
           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q6_k_linear_1x_reduce_nsg4) "\n"
           "    termite_q6_k_scalar_even_impl<4u>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
           "}\n"
// native-program:entry termite_q6_k_linear_1x_reduce_nsg8
           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q6_k_linear_1x_reduce_nsg8) "\n"
           "    termite_q6_k_scalar_even_impl<8u>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
           "}\n"
// native-program:entry termite_q6_k_linear_r2_reduce
           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q6_k_linear_r2_reduce) "\n"
           "    const uint NR0 = 2u; const uint NSG = 2u; const uint nb = p.row_blocks; uint first_row = (tg.x * NSG + uint(sgitg)) * NR0; uint r0 = tg.y * 2u; if (r0 >= p.rows || first_row >= p.out_dim) return; const bool has_r1 = (r0 + 1u) < p.rows;\n"
           "    const uint row_bytes = nb * 210u; device const uchar *x0 = weight + first_row * row_bytes; device const float *yy0 = input + r0 * p.in_dim; device const float *yy1 = has_r1 ? input + (r0 + 1u) * p.in_dim : yy0; float sumf[2][2] = {{0.0f, 0.0f}, {0.0f, 0.0f}}; float yl0[16]; float yl1[16];\n"
           "    const ushort tid = lane / 2u; const ushort ix = lane % 2u; const ushort ip = tid / 8u; const ushort il = tid % 8u; const ushort l0 = 4u * il; const ushort is = 8u * ip + l0 / 16u; const uint y_offset = 128u * uint(ip) + uint(l0); const uint q_offset_l = 64u * uint(ip) + uint(l0); const uint q_offset_h = 32u * uint(ip) + uint(l0); const uint row_limit = min(NR0, p.out_dim - first_row);\n"
           "    for (uint i = ix; i < nb; i += 2u) { device const float *y0 = yy0 + i * 256u + y_offset; device const float *y1 = yy1 + i * 256u + y_offset; for (ushort l = 0; l < 4u; ++l) { yl0[4u * l + 0u] = y0[l]; yl0[4u * l + 1u] = y0[l + 32u]; yl0[4u * l + 2u] = y0[l + 64u]; yl0[4u * l + 3u] = y0[l + 96u]; yl1[4u * l + 0u] = y1[l]; yl1[4u * l + 1u] = y1[l + 32u]; yl1[4u * l + 2u] = y1[l + 64u]; yl1[4u * l + 3u] = y1[l + 96u]; } device const uchar *q1 = x0 + i * 210u + q_offset_l; device const uchar *q2 = q1 + 32u; device const uchar *qh = x0 + i * 210u + 128u + q_offset_h; device const char *sc = reinterpret_cast<device const char *>(x0 + i * 210u + 192u) + is; device const uchar *dp = x0 + i * 210u + 208u; for (uint row = 0; row < row_limit; ++row) { float4 sums0 = {0.0f, 0.0f, 0.0f, 0.0f}; float4 sums1 = {0.0f, 0.0f, 0.0f, 0.0f}; for (ushort l = 0; l < 4u; ++l) { float w0 = float(char((q1[l] & 0x0Fu) | ((qh[l] & 0x03u) << 4)) - 32); float w1 = float(char((q2[l] & 0x0Fu) | ((qh[l] & 0x0Cu) << 2)) - 32); float w2 = float(char((q1[l] >> 4) | (qh[l] & 0x30u)) - 32); float w3 = float(char((q2[l] >> 4) | ((qh[l] & 0xC0u) >> 2)) - 32); sums0[0] += yl0[4u * l + 0u] * w0; sums0[1] += yl0[4u * l + 1u] * w1; sums0[2] += yl0[4u * l + 2u] * w2; sums0[3] += yl0[4u * l + 3u] * w3; sums1[0] += yl1[4u * l + 0u] * w0; sums1[1] += yl1[4u * l + 1u] * w1; sums1[2] += yl1[4u * l + 2u] * w2; sums1[3] += yl1[4u * l + 3u] * w3; } ushort d_bits = (ushort(dp[1]) << 8) | ushort(dp[0]); float d = float(as_type<half>(d_bits)); float s0 = float(sc[0]); float s2 = float(sc[2]); float s4 = float(sc[4]); float s6 = float(sc[6]); sumf[row][0] += d * (sums0[0] * s0 + sums0[1] * s2 + sums0[2] * s4 + sums0[3] * s6); sumf[row][1] += d * (sums1[0] * s0 + sums1[1] * s2 + sums1[2] * s4 + sums1[3] * s6); q1 += row_bytes; q2 += row_bytes; qh += row_bytes; sc += row_bytes; dp += row_bytes; } }\n"
           "    for (uint row = 0; row < row_limit; ++row) { float total0 = simd_sum(sumf[row][0]); float total1 = simd_sum(sumf[row][1]); if (lane == 0u) { output[r0 * p.out_dim + first_row + row] = total0; if (has_r1) output[(r0 + 1u) * p.out_dim + first_row + row] = total1; } }\n"
           "}\n"
// native-program:entry termite_q6_k_linear_1x_reduce_in_f16
           TERMITE_MSL_ABI_QUANT_F16_REDUCE(termite_q6_k_linear_1x_reduce_in_f16) "\n"
           "    const uint NR0 = 4u; const uint NSG = 4u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;\n"
           "    uint ix = lane >> 1; uint il = lane & 1u; uint sb0 = sgitg * 16u + ix; uint in_row = r * p.in_dim; uint total_subblocks = p.row_blocks * 16u; float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};\n"
           "    for (uint sb = sb0; sb < total_subblocks; sb += NSG * 16u) { uint b = sb >> 4; uint sub = sb & 15u; uint j0 = il * 8u; uint half_idx = sub / 8u; uint group = (sub % 8u) / 2u; uint l_base = (sub % 2u) * 16u; uint ql_off = half_idx * 64u + (group & 1u) * 32u; uint qh_off = half_idx * 32u; uint qh_shift = group * 2u; uint nibble_shift = (group / 2u) * 4u; uint input_off = in_row + b * 256u + sub * 16u + j0; float x[8]; for (uint i = 0; i < 8u; ++i) { x[i] = float(input[input_off + i]); } for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 210u + b * 210u; const device uchar *ql = weight + off; const device uchar *qh = weight + off + 128u; const device char *scales = reinterpret_cast<const device char *>(weight + off + 192u); ushort d_bits = (ushort(weight[off + 209]) << 8) | ushort(weight[off + 208]); float scale = float(as_type<half>(d_bits)) * float(scales[sub]); float block = 0.0f; for (uint i = 0; i < 8u; ++i) { uint l = l_base + j0 + i; int low4 = int((ql[ql_off + l] >> nibble_shift) & 0x0Fu); int high2 = int((qh[qh_off + l] >> qh_shift) & 0x03u); int q = (low4 | (high2 << 4)) - 32; block += x[i] * (scale * float(q)); } acc[row] += block; } }\n"
           "    if (sgitg == 0u) { for (uint row = 0; row < NR0; ++row) shmem[row * 32u + lane] = 0.0f; } for (uint row = 0; row < NR0; ++row) acc[row] = simd_sum(acc[row]); threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    if (lane == 0u) { for (uint row = 0; row < NR0; ++row) shmem[row * 32u + sgitg] = acc[row]; } threadgroup_barrier(mem_flags::mem_threadgroup);\n"
           "    float total[4]; for (uint row = 0; row < NR0; ++row) total[row] = simd_sum(shmem[row * 32u + lane]); if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o < p.out_dim) output[idx + row] = total[row]; } }\n"
           "}\n"
// native-program:entry termite_q6_k_pair_linear
           TERMITE_MSL_ABI_QUANT_PAIR_SCALAR(termite_q6_k_pair_linear) "\n"
           "    uint o = tid.x; uint r = tid.y; if (o >= p.out_dim || r >= p.rows) return; uint row_offset = o * p.row_blocks * 210u; float acc_a = 0.0f; float acc_b = 0.0f;\n"
           "    for (uint b = 0; b < p.row_blocks; ++b) { uint off = row_offset + b * 210u; const device uchar *ql_a = weight_a + off; const device uchar *qh_a = weight_a + off + 128u; const device char *scales_a = reinterpret_cast<const device char *>(weight_a + off + 192u); const device uchar *ql_b = weight_b + off; const device uchar *qh_b = weight_b + off + 128u; const device char *scales_b = reinterpret_cast<const device char *>(weight_b + off + 192u); ushort d_bits_a = (ushort(weight_a[off + 209]) << 8) | ushort(weight_a[off + 208]); ushort d_bits_b = (ushort(weight_b[off + 209]) << 8) | ushort(weight_b[off + 208]); float d_a = float(as_type<half>(d_bits_a)); float d_b = float(as_type<half>(d_bits_b)); for (uint sub = 0; sub < 16; ++sub) { float scale_a = d_a * float(scales_a[sub]); float scale_b = d_b * float(scales_b[sub]); uint half_idx = sub / 8u; uint group = (sub % 8u) / 2u; uint l_base = (sub % 2u) * 16u; uint ql_off = half_idx * 64u + (group & 1u) * 32u; uint qh_off = half_idx * 32u; uint qh_shift = group * 2u; uint nibble_shift = (group / 2u) * 4u; uint input_off = r * p.in_dim + b * 256u + sub * 16u; for (uint i = 0; i < 16; ++i) { uint l = l_base + i; float x = input[input_off + i]; int low4_a = int((ql_a[ql_off + l] >> nibble_shift) & 0x0Fu); int high2_a = int((qh_a[qh_off + l] >> qh_shift) & 0x03u); int q_a = (low4_a | (high2_a << 4)) - 32; acc_a += x * (scale_a * float(q_a)); int low4_b = int((ql_b[ql_off + l] >> nibble_shift) & 0x0Fu); int high2_b = int((qh_b[qh_off + l] >> qh_shift) & 0x03u); int q_b = (low4_b | (high2_b << 4)) - 32; acc_b += x * (scale_b * float(q_b)); } } }\n"
           "    output_a[r * p.out_dim + o] = acc_a;\n"
           "    output_b[r * p.out_dim + o] = acc_b;\n"
           "}\n"
