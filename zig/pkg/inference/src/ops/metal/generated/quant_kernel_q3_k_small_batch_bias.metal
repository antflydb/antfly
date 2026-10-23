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
// plan_id=metal/q3_k/rows_2_8/bias/small_batch
// kernel_id=antfly_q3_k_small_batch_bias_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=false
// General MSL lowering smoke for descriptor-driven quant matmul epilogues.
// Production Metal dispatch stays on native handwritten MSL until this
// candidate clears correctness and benchmark gates.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline float antfly_q3_k_dequant_lane_v2(device const uchar *block, int lane) {
    int v0 = lane;
    int v1 = 4;
    int v2 = int(uint(v0) >> v1);
    int v3 = 3;
    int v4 = int(uint(v2) >> v3);
    int v5 = 7;
    int v6 = v2 & v5;
    int v7 = 1;
    int v8 = int(uint(v6) >> v7);
    int v9 = v2 & v7;
    int v10 = int(uint(v9) << v1);
    int v11 = 15;
    int v12 = v0 & v11;
    int v13 = v10 + v12;
    int v14 = 5;
    int v15 = int(uint(v4) << v14);
    int v16 = v15 + v13;
    int v17 = 32;
    int v18 = v16 + v17;
    int v19 = int(block[v18]);
    int v20 = int(uint(v8) << v7);
    int v21 = int(uint(v19) >> v20);
    int v22 = v21 & v3;
    int v23 = int(block[v13]);
    int v24 = 2;
    int v25 = int(uint(v4) << v24);
    int v26 = v25 + v8;
    int v27 = int(uint(v23) >> v26);
    int v28 = v27 & v7;
    int v29 = int(uint(v28) << v24);
    int v30 = v22 + v29;
    int v31 = v30 - v1;
    int v32 = v2 & v3;
    int v33 = v6 < v1;
    int v34 = 96;
    int v35 = 100;
    int v36 = v33 ? v34 : v35;
    int v37 = v32 + v36;
    int v38 = int(block[v37]);
    int v39 = 8;
    int v40 = v2 < v39;
    int v41 = v38 & v11;
    int v42 = int(uint(v38) >> v1);
    int v43 = v40 ? v41 : v42;
    int v44 = 104;
    int v45 = v44 + v32;
    int v46 = int(block[v45]);
    int v47 = int(uint(v2) >> v24);
    int v48 = int(uint(v47) << v7);
    int v49 = int(uint(v46) >> v48);
    int v50 = v49 & v3;
    int v51 = int(uint(v50) << v1);
    int v52 = v43 | v51;
    int v53 = v52 - v17;
    int v54 = 108;
    float v55 = antfly_qk_half_le_to_float(block + v54);
    float v56 = float(v53);
    float v57 = v55 * v56;
    float v58 = float(v31);
    float v59 = v57 * v58;
    return v59;
}
kernel void antfly_q3_k_small_batch_bias_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q3_k [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant int &rows [[buffer(4)]], constant int &in_dim [[buffer(5)]], constant int &out_dim [[buffer(6)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]) {
    uint tid = thread_pos.x; int col = int(group_pos.x); int row = int(group_pos.y); if (row >= rows || rows < 2 || rows > 8 || col >= out_dim || (in_dim & 255) != 0) return; float acc = 0.0f; int block_count = in_dim >> 8;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) { device const uchar *block = weight_q3_k + ((col * block_count + block_idx) * 110); int base = block_idx << 8; for (int lane = int(tid); lane < 256; lane += 32) acc += input[row * in_dim + base + lane] * antfly_q3_k_dequant_lane_v2(block, lane); }
    acc = simd_sum(acc); if (tid == 0) output[row * out_dim + col] = acc + bias[col];
}
