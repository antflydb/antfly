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
// plan_id=metal/q5_0/rows_2_8/none/small_batch
// kernel_id=antfly_q5_0_small_batch_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=false
// General MSL lowering smoke for descriptor-driven quant matmul.
// Production Metal dispatch stays on native handwritten MSL until this
// candidate clears correctness and benchmark gates.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline uint antfly_qk_u32_le(device const uchar *p) { return uint(p[0]) | (uint(p[1]) << 8) | (uint(p[2]) << 16) | (uint(p[3]) << 24); }
inline float antfly_q5_0_dequant_lane_v2(device const uchar *block, int lane) {
    int v0 = lane;
    int v1 = 6;
    int v2 = 15;
    int v3 = v0 & v2;
    int v4 = v1 + v3;
    int v5 = int(block[v4]);
    int v6 = 16;
    int v7 = v0 < v6;
    int v8 = 15;
    int v9 = v5 & v8;
    int v10 = 4;
    int v11 = int(uint(v5) >> v10);
    int v12 = v7 ? v9 : v11;
    int v13 = 2;
    int v14 = int(antfly_qk_u32_le(block + v13));
    int v15 = int(uint(v14) >> v0);
    int v16 = 1;
    int v17 = v15 & v16;
    int v18 = 4;
    int v19 = int(uint(v17) << v18);
    int v20 = v12 | v19;
    int v21 = 16;
    int v22 = v20 - v21;
    int v23 = 0;
    float v24 = antfly_qk_half_le_to_float(block + v23);
    float v25 = float(v22);
    float v26 = v24 * v25;
    return v26;
}
kernel void antfly_q5_0_small_batch_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q5_0 [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]) {
    uint tid = thread_pos.x; int col = int(group_pos.x); int row = int(group_pos.y); if (row >= rows || rows < 2 || rows > 8 || col >= out_dim || (in_dim & 31) != 0) return; float acc = 0.0f; int block_count = in_dim >> 5;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) { device const uchar *block = weight_q5_0 + ((col * block_count + block_idx) * 22); int base = block_idx << 5; for (int lane = int(tid); lane < 32; lane += 32) acc += input[row * in_dim + base + lane] * antfly_q5_0_dequant_lane_v2(block, lane); }
    acc = simd_sum(acc); if (tid == 0) output[row * out_dim + col] = acc;
}
