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
// plan_id=metal/q2_k/rows_2_8/bias/small_batch
// kernel_id=antfly_q2_k_small_batch_bias_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=false
// General MSL lowering smoke for descriptor-driven quant matmul epilogues.
// Production Metal dispatch stays on native handwritten MSL until this
// candidate clears correctness and benchmark gates.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline float antfly_q2_k_dequant_lane_v2(device const uchar *block, int lane) {
    int v0 = lane;
    int v1 = 4;
    int v2 = int(uint(v0) >> v1);
    int v3 = 3;
    int v4 = int(uint(v2) >> v3);
    int v5 = 7;
    int v6 = v2 & v5;
    int v7 = 1;
    int v8 = int(uint(v6) >> v7);
    int v9 = 1;
    int v10 = v2 & v9;
    int v11 = 4;
    int v12 = int(uint(v10) << v11);
    int v13 = 15;
    int v14 = v0 & v13;
    int v15 = v12 + v14;
    int v16 = 5;
    int v17 = int(uint(v4) << v16);
    int v18 = v17 + v15;
    int v19 = 20;
    int v20 = v18 + v19;
    int v21 = int(block[v20]);
    int v22 = 1;
    int v23 = int(uint(v8) << v22);
    int v24 = int(uint(v21) >> v23);
    int v25 = 3;
    int v26 = v24 & v25;
    int v27 = int(block[v2]);
    int v28 = 16;
    float v29 = antfly_qk_half_le_to_float(block + v28);
    int v30 = 15;
    int v31 = v27 & v30;
    float v32 = float(v31);
    float v33 = v29 * v32;
    float v34 = float(v26);
    float v35 = v33 * v34;
    int v36 = 18;
    float v37 = antfly_qk_half_le_to_float(block + v36);
    int v38 = 4;
    int v39 = int(uint(v27) >> v38);
    float v40 = float(v39);
    float v41 = v37 * v40;
    float v42 = v35 - v41;
    return v42;
}
kernel void antfly_q2_k_small_batch_bias_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q2_k [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant int &rows [[buffer(4)]], constant int &in_dim [[buffer(5)]], constant int &out_dim [[buffer(6)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]) {
    uint tid = thread_pos.x; int col = int(group_pos.x); int row = int(group_pos.y); if (row >= rows || rows < 2 || rows > 8 || col >= out_dim || (in_dim & 255) != 0) return; float acc = 0.0f; int block_count = in_dim >> 8;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) { device const uchar *block = weight_q2_k + ((col * block_count + block_idx) * 84); int base = block_idx << 8; for (int lane = int(tid); lane < 256; lane += 32) acc += input[row * in_dim + base + lane] * antfly_q2_k_dequant_lane_v2(block, lane); }
    acc = simd_sum(acc); if (tid == 0) output[row * out_dim + col] = acc + bias[col];
}
