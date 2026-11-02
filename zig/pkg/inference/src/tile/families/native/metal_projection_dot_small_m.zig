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

pub const source_0 =
    \\           TERMITE_MSL_ABI_DOT_SMALL_ROWS(termite_dot_general_2d_f32_small_m_tile) "\n"
    \\           "    uint row = tg.y; uint col0 = tg.x * 8u; uint width = tpg.x; if (row >= p.m || p.k == 0u || width == 0u) return;\n"
    \\           "    uint c0 = col0; uint c1 = col0 + 1u; uint c2 = col0 + 2u; uint c3 = col0 + 3u; uint c4 = col0 + 4u; uint c5 = col0 + 5u; uint c6 = col0 + 6u; uint c7 = col0 + 7u;\n"
    \\           "    float acc0 = 0.0f; float acc1 = 0.0f; float acc2 = 0.0f; float acc3 = 0.0f; float acc4 = 0.0f; float acc5 = 0.0f; float acc6 = 0.0f; float acc7 = 0.0f;\n"
    \\           "    for (uint kk = uint(tid); kk < p.k; kk += width) { float l = lhs[row * p.k + kk]; if (c0 < p.n) acc0 += l * (p.rhs_contract_axis == 0u ? rhs[kk * p.n + c0] : rhs[c0 * p.k + kk]); if (c1 < p.n) acc1 += l * (p.rhs_contract_axis == 0u ? rhs[kk * p.n + c1] : rhs[c1 * p.k + kk]); if (c2 < p.n) acc2 += l * (p.rhs_contract_axis == 0u ? rhs[kk * p.n + c2] : rhs[c2 * p.k + kk]); if (c3 < p.n) acc3 += l * (p.rhs_contract_axis == 0u ? rhs[kk * p.n + c3] : rhs[c3 * p.k + kk]); if (c4 < p.n) acc4 += l * (p.rhs_contract_axis == 0u ? rhs[kk * p.n + c4] : rhs[c4 * p.k + kk]); if (c5 < p.n) acc5 += l * (p.rhs_contract_axis == 0u ? rhs[kk * p.n + c5] : rhs[c5 * p.k + kk]); if (c6 < p.n) acc6 += l * (p.rhs_contract_axis == 0u ? rhs[kk * p.n + c6] : rhs[c6 * p.k + kk]); if (c7 < p.n) acc7 += l * (p.rhs_contract_axis == 0u ? rhs[kk * p.n + c7] : rhs[c7 * p.k + kk]); }\n"
    \\           "    partials[uint(tid)] = acc0; partials[width + uint(tid)] = acc1; partials[2u * width + uint(tid)] = acc2; partials[3u * width + uint(tid)] = acc3; partials[4u * width + uint(tid)] = acc4; partials[5u * width + uint(tid)] = acc5; partials[6u * width + uint(tid)] = acc6; partials[7u * width + uint(tid)] = acc7; threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    for (uint stride = width >> 1u; stride > 0u; stride >>= 1u) { if (tid < stride) { partials[uint(tid)] += partials[uint(tid) + stride]; partials[width + uint(tid)] += partials[width + uint(tid) + stride]; partials[2u * width + uint(tid)] += partials[2u * width + uint(tid) + stride]; partials[3u * width + uint(tid)] += partials[3u * width + uint(tid) + stride]; partials[4u * width + uint(tid)] += partials[4u * width + uint(tid) + stride]; partials[5u * width + uint(tid)] += partials[5u * width + uint(tid) + stride]; partials[6u * width + uint(tid)] += partials[6u * width + uint(tid) + stride]; partials[7u * width + uint(tid)] += partials[7u * width + uint(tid) + stride]; } threadgroup_barrier(mem_flags::mem_threadgroup); }\n"
    \\           "    if (tid == 0u) { uint base = row * p.n + col0; if (c0 < p.n) output[base] = partials[0]; if (c1 < p.n) output[base + 1u] = partials[width]; if (c2 < p.n) output[base + 2u] = partials[2u * width]; if (c3 < p.n) output[base + 3u] = partials[3u * width]; if (c4 < p.n) output[base + 4u] = partials[4u * width]; if (c5 < p.n) output[base + 5u] = partials[5u * width]; if (c6 < p.n) output[base + 6u] = partials[6u * width]; if (c7 < p.n) output[base + 7u] = partials[7u * width]; }\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_DOT_SMALL_ROWS(termite_dot_general_2d_f32_small_m_rows4_cols4) "\n"
    \\           "    uint row0 = tg.y * 4u; uint col0 = tg.x * 4u; uint width = tpg.x; if (row0 >= p.m || col0 >= p.n || p.k == 0u || width == 0u || p.rhs_contract_axis > 1u) return;\n"
    \\           "    uint r0 = row0; uint r1 = row0 + 1u; uint r2 = row0 + 2u; uint r3 = row0 + 3u; uint c0 = col0; uint c1 = col0 + 1u; uint c2 = col0 + 2u; uint c3 = col0 + 3u;\n"
    \\           "    float a00 = 0.0f; float a01 = 0.0f; float a02 = 0.0f; float a03 = 0.0f; float a10 = 0.0f; float a11 = 0.0f; float a12 = 0.0f; float a13 = 0.0f; float a20 = 0.0f; float a21 = 0.0f; float a22 = 0.0f; float a23 = 0.0f; float a30 = 0.0f; float a31 = 0.0f; float a32 = 0.0f; float a33 = 0.0f;\n"
    \\           "    bool vr1 = r1 < p.m; bool vr2 = r2 < p.m; bool vr3 = r3 < p.m; bool vc1 = c1 < p.n; bool vc2 = c2 < p.n; bool vc3 = c3 < p.n;\n"
    \\           "    for (uint kk = uint(tid); kk < p.k; kk += width) { float x0 = lhs[r0 * p.k + kk]; float x1 = vr1 ? lhs[r1 * p.k + kk] : 0.0f; float x2 = vr2 ? lhs[r2 * p.k + kk] : 0.0f; float x3 = vr3 ? lhs[r3 * p.k + kk] : 0.0f; uint rhs_base = p.rhs_contract_axis == 0u ? kk * p.n + col0 : col0 * p.k + kk; float y0 = rhs[rhs_base]; float y1 = vc1 ? rhs[rhs_base + (p.rhs_contract_axis == 0u ? 1u : p.k)] : 0.0f; float y2 = vc2 ? rhs[rhs_base + (p.rhs_contract_axis == 0u ? 2u : 2u * p.k)] : 0.0f; float y3 = vc3 ? rhs[rhs_base + (p.rhs_contract_axis == 0u ? 3u : 3u * p.k)] : 0.0f; a00 += x0 * y0; a01 += x0 * y1; a02 += x0 * y2; a03 += x0 * y3; a10 += x1 * y0; a11 += x1 * y1; a12 += x1 * y2; a13 += x1 * y3; a20 += x2 * y0; a21 += x2 * y1; a22 += x2 * y2; a23 += x2 * y3; a30 += x3 * y0; a31 += x3 * y1; a32 += x3 * y2; a33 += x3 * y3; }\n"
    \\           "    uint t = uint(tid); partials[t] = a00; partials[width + t] = a01; partials[2u * width + t] = a02; partials[3u * width + t] = a03; partials[4u * width + t] = a10; partials[5u * width + t] = a11; partials[6u * width + t] = a12; partials[7u * width + t] = a13; partials[8u * width + t] = a20; partials[9u * width + t] = a21; partials[10u * width + t] = a22; partials[11u * width + t] = a23; partials[12u * width + t] = a30; partials[13u * width + t] = a31; partials[14u * width + t] = a32; partials[15u * width + t] = a33; threadgroup_barrier(mem_flags::mem_threadgroup);\n"
    \\           "    for (uint stride = width >> 1u; stride > 0u; stride >>= 1u) { if (tid < stride) { for (uint i = 0u; i < 16u; ++i) partials[i * width + t] += partials[i * width + t + stride]; } threadgroup_barrier(mem_flags::mem_threadgroup); }\n"
    \\           "    if (tid == 0u) { uint b0 = r0 * p.n + col0; output[b0] = partials[0]; if (vc1) output[b0 + 1u] = partials[width]; if (vc2) output[b0 + 2u] = partials[2u * width]; if (vc3) output[b0 + 3u] = partials[3u * width]; if (vr1) { uint b1 = r1 * p.n + col0; output[b1] = partials[4u * width]; if (vc1) output[b1 + 1u] = partials[5u * width]; if (vc2) output[b1 + 2u] = partials[6u * width]; if (vc3) output[b1 + 3u] = partials[7u * width]; } if (vr2) { uint b2 = r2 * p.n + col0; output[b2] = partials[8u * width]; if (vc1) output[b2 + 1u] = partials[9u * width]; if (vc2) output[b2 + 2u] = partials[10u * width]; if (vc3) output[b2 + 3u] = partials[11u * width]; } if (vr3) { uint b3 = r3 * p.n + col0; output[b3] = partials[12u * width]; if (vc1) output[b3 + 1u] = partials[13u * width]; if (vc2) output[b3 + 2u] = partials[14u * width]; if (vc3) output[b3 + 3u] = partials[15u * width]; } }\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           TERMITE_MSL_ABI_DOT_SMALL_ROWS(termite_dot_general_2d_f32_small_m_rows4_cols8) "\n"
    \\           "    termite_small_m_rows4_dot<8u>(lhs, rhs, output, p, partials, tid, tpg, tg);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_DOT_SMALL_ROWS(termite_dot_general_2d_f32_small_m_rows4_cols16) "\n"
    \\           "    termite_small_m_rows4_dot<16u>(lhs, rhs, output, p, partials, tid, tpg, tg);\n"
    \\           "}\n"
    \\
;
