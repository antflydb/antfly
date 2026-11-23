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

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

pub const source_0 =
    program.join(&.{
        \\           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q4_0_linear_1x_reduce_nr4_nsg4_nq8) "\n"
        \\
        ,
        program.metal(
            \\    const uint NR0 = 4u; const uint NSG = 4u; const uint NQ = 8u; const uint NW = 32u; uint first_o = (tg.x * NSG + uint(sgitg)) * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;
            \\    uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * NQ; uint in_row = r * p.in_dim; float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};
            \\    for (uint b = ix; b < p.row_blocks; b += NQ) { float x[8]; uint in_off = in_row + b * 32u + il; for (uint i = 0u; i < NQ; ++i) x[i] = input[in_off + i]; for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 18u + b * 18u; ushort bits = (ushort(weight[off + 1u]) << 8) | ushort(weight[off]); float d = float(as_type<half>(bits)); const device uchar *qs = weight + off + 2u; float block = 0.0f; for (uint i = 0u; i < NQ; ++i) { uint j = il + i; uchar packed = qs[j & 15u]; float q = j < 16u ? float(int(packed & 0x0Fu) - 8) : float(int(packed >> 4) - 8); block += x[i] * q; } acc[row] += d * block; } }
            \\    for (uint row = 0u; row < NR0; ++row) acc[row] = simd_sum(acc[row]); if (lane == 0u) { uint idx = r * p.out_dim + first_o; for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o < p.out_dim) output[idx + row] = acc[row]; } }
            \\}
            \\
        ),

        \\           TERMITE_MSL_ABI_QUANT_LINEAR_SIMD(termite_q4_0_linear_1x_packed_t4_nsg4) "\n"
        \\
        ,
        program.metal(
            \\    const uint NR0 = 4u; const uint NSG = 4u; const uint NQ = 16u; const uint NW = 32u; uint first_o = (tg.x * NSG + uint(sgitg)) * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;
            \\    uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * 8u; uint in_row = r * p.in_dim; float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};
            \\    for (uint b = ix; b < p.row_blocks; b += NQ) { float yl[16]; float sumy = 0.0f; uint in_off = in_row + b * 32u + il; for (uint i = 0u; i < 8u; i += 2u) { float x0 = input[in_off + i]; float x1 = input[in_off + i + 1u]; float x16 = input[in_off + 16u + i]; float x17 = input[in_off + 16u + i + 1u]; sumy += x0 + x1 + x16 + x17; yl[i] = x0; yl[i + 1u] = x1 / 256.0f; yl[i + 8u] = x16 / 16.0f; yl[i + 9u] = x17 / 4096.0f; } for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = (((o >> 2u) * p.row_blocks + b) * 4u + (o & 3u)) * 18u; ushort bits = (ushort(weight[off + 1u]) << 8) | ushort(weight[off]); float d = float(as_type<half>(bits)); const device ushort *qs = reinterpret_cast<const device ushort *>(weight + off + 2u) + (il >> 1); float qacc = 0.0f; for (uint i = 0u; i < 8u; i += 2u) { uint packed = uint(qs[i >> 1]); qacc += yl[i] * float(packed & 0x000Fu); qacc += yl[i + 1u] * float(packed & 0x0F00u); qacc += yl[i + 8u] * float(packed & 0x00F0u); qacc += yl[i + 9u] * float(packed & 0xF000u); } acc[row] += d * (qacc - 8.0f * sumy); } }
            \\    for (uint row = 0u; row < NR0; ++row) acc[row] = simd_sum(acc[row]); if (lane == 0u) { uint idx = r * p.out_dim + first_o; for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o < p.out_dim) output[idx + row] = acc[row]; } }
            \\}
            \\
        ),
    });

pub const source_1 =
    program.join(&.{
        program.metal(
            \\kernel void termite_q4_0_linear_1x_reduce_sumsq(device const float *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_linear_params &p [[buffer(3)]], device float *sumsq [[buffer(4)]], threadgroup float *shmem [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
            \\    termite_q4_0_scalar_sumsq_impl<float, float>(input, weight, output, p, sumsq, shmem, lane, sgitg, tg);
            \\}
            \\kernel void termite_q4_0_linear_1x_reduce_out_f16(device const float *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device half *output [[buffer(2)]], constant termite_metal_linear_params &p [[buffer(3)]], threadgroup float *shmem [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
            \\    termite_q4_0_scalar_packed16_impl<16u, 4u, float, half>(input, weight, output, p, shmem, lane, sgitg, tg);
            \\}
            \\
        ),

        \\           TERMITE_MSL_ABI_QUANT_F16_REDUCE(termite_q4_0_linear_1x_reduce_in_f16) "\n"
        \\
        ,
        program.metal(
            \\    termite_q4_0_scalar_packed16_impl<8u, 4u, half, float>(input, weight, output, p, shmem, lane, sgitg, tg);
            \\}
            \\
        ),

        \\           TERMITE_MSL_ABI_QUANT_F16_REDUCE(termite_q4_0_linear_1x_reduce_in_f16_small) "\n"
        \\
        ,
        program.metal(
            \\    termite_q4_0_scalar_packed16_impl<4u, 2u, half, float>(input, weight, output, p, shmem, lane, sgitg, tg);
            \\}
            \\kernel void termite_q4_0_linear_1x_reduce_in_f16_sumsq(device const half *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_linear_params &p [[buffer(3)]], device float *sumsq [[buffer(4)]], threadgroup float *shmem [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
            \\    termite_q4_0_scalar_sumsq_impl<half, float>(input, weight, output, p, sumsq, shmem, lane, sgitg, tg);
            \\}
            \\kernel void termite_q4_0_linear_1x_reduce_in_f16_out_f16(device const half *input [[buffer(0)]], device const uchar *weight [[buffer(1)]], device half *output [[buffer(2)]], constant termite_metal_linear_params &p [[buffer(3)]], threadgroup float *shmem [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
            \\    termite_q4_0_scalar_packed16_impl<8u, 4u, half, half>(input, weight, output, p, shmem, lane, sgitg, tg);
            \\}
            \\
        ),
    });

pub const source_2 =
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q4_1_linear) "\n"
    \\           "    termite_legacy_linear<termite_legacy_q4_1, 20u>(input, weight, output, p, gid);\n"
    \\           "}\n"
    \\
;

pub const source_3 =
    \\           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q4_1_linear_1x_reduce) "\n"
    \\           "    termite_legacy_reduce<termite_legacy_q4_1, 20u, 4u, 2u>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q5_0_linear) "\n"
    \\           "    termite_legacy_linear<termite_legacy_q5_0, 22u>(input, weight, output, p, gid);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q5_0_linear_1x_reduce) "\n"
    \\           "    termite_legacy_reduce<termite_legacy_q5_0, 22u, 2u, 4u>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_LINEAR_SCALAR(termite_q5_1_linear) "\n"
    \\           "    termite_legacy_linear<termite_legacy_q5_1, 24u>(input, weight, output, p, gid);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_QUANT_REDUCE(termite_q5_1_linear_1x_reduce) "\n"
    \\           "    termite_legacy_reduce<termite_legacy_q5_1, 24u, 2u, 4u>(input, weight, output, p, shmem, lane, sgitg, tg);\n"
    \\           "}\n"
    \\
;
