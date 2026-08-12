// Copyright 2026 Antfly, Inc.
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

// Dev-only generated Metal candidate from graph/quant_kernel_compiler.zig.
// plan_id=metal/q5_1/rows_2_8/none/small_batch
// kernel_id=antfly_q5_1_small_batch_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=false
// General MSL lowering smoke for descriptor-driven quant matmul.
// Production Metal dispatch stays on native handwritten MSL until this
// candidate clears correctness and benchmark gates.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline uint antfly_qk_u32_le(device const uchar *p) { return uint(p[0]) | (uint(p[1]) << 8) | (uint(p[2]) << 16) | (uint(p[3]) << 24); }
inline float antfly_q5_1_dequant_lane_v2(device const uchar *block, int lane) { float d = antfly_qk_half_le_to_float(block); float m = antfly_qk_half_le_to_float(block + 2); uint qh = antfly_qk_u32_le(block + 4); int packed_index = lane & 15; uchar packed = block[8 + packed_index]; int low4 = lane < 16 ? int(packed & 0x0fu) : int(packed >> 4); int high = int((qh >> uint(lane)) & 1u); return d * float(low4 | (high << 4)) + m; }
kernel void antfly_q5_1_small_batch_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q5_1 [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]) {
    uint tid = thread_pos.x; int col0 = int(group_pos.x << 1); int col1 = col0 + 1; int row = int(group_pos.y); if (row >= rows || rows < 2 || rows > 8 || col0 >= out_dim || (in_dim & 31) != 0) return; float acc0 = 0.0f; float acc1 = 0.0f; int block_count = in_dim >> 5;
    device const float *row_input = input + row * in_dim; device const uchar *col0_weight = weight_q5_1 + col0 * block_count * 24; bool has_col1 = col1 < out_dim; device const uchar *col1_weight = has_col1 ? weight_q5_1 + col1 * block_count * 24 : col0_weight;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) { device const uchar *block0 = col0_weight + block_idx * 24; device const uchar *block1 = col1_weight + block_idx * 24; int base = block_idx << 5; for (int lane = int(tid); lane < 32; lane += 32) { float x = row_input[base + lane]; acc0 += x * antfly_q5_1_dequant_lane_v2(block0, lane); if (has_col1) acc1 += x * antfly_q5_1_dequant_lane_v2(block1, lane); } }
    acc0 = simd_sum(acc0); acc1 = simd_sum(acc1); if (tid == 0) { output[row * out_dim + col0] = acc0; if (has_col1) output[row * out_dim + col1] = acc1; }
}
