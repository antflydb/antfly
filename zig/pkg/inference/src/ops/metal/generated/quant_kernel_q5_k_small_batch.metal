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

// Generated Metal artifact source from graph/quant_kernel_compiler.zig.
// plan_id=metal/q5_k/rows_2_8/none/small_batch
// kernel_id=antfly_q5_k_small_batch_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=true
// Promoted after the schedule sweep re-tuned this route to 256-thread
// hybrid-simd and the decode-runtime speedup gate cleared vs handwritten.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline float antfly_q5_k_dequant_lane_v2(device const uchar *block, int lane) {
    int v0 = lane;
    int v1 = 5;
    int v2 = int(uint(v0) >> v1);
    int v3 = 31;
    int v4 = v0 & v3;
    int v5 = 1;
    int v6 = int(uint(v2) >> v5);
    int v7 = 32;
    int v8 = v6 * v7;
    int v9 = v8 + v4;
    int v10 = 48;
    int v11 = v10 + v9;
    int v12 = int(block[v11]);
    int v13 = 1;
    int v14 = v2 & v13;
    int v15 = 4;
    int v16 = int(uint(v12) >> v15);
    int v17 = 15;
    int v18 = v12 & v17;
    int v19 = v14 ? v16 : v18;
    int v20 = 16;
    int v21 = v20 + v4;
    int v22 = int(block[v21]);
    int v23 = int(uint(v22) >> v2);
    int v24 = 1;
    int v25 = v23 & v24;
    int v26 = 4;
    int v27 = int(uint(v25) << v26);
    int v28 = v19 + v27;
    int v29 = 0;
    float v30 = antfly_qk_half_le_to_float(block + v29);
    int v31 = 4;
    int v32 = v2 < v31;
    int v33 = 4;
    int v34 = v2 + v33;
    int v35 = 8;
    int v36 = v2 + v35;
    int v37 = v32 ? v34 : v36;
    int v38 = int(block[v37]);
    int v39 = 15;
    int v40 = v38 & v39;
    int v41 = 4;
    int v42 = 0;
    int v43 = v2 + v42;
    int v44 = v32 ? v41 : v43;
    int v45 = int(block[v44]);
    int v46 = 6;
    int v47 = int(uint(v45) >> v46);
    int v48 = 4;
    int v49 = int(uint(v47) << v48);
    int v50 = v40 | v49;
    int v51 = 63;
    int v52 = v38 & v51;
    int v53 = v32 ? v52 : v50;
    float v54 = float(v53);
    float v55 = v30 * v54;
    float v56 = float(v28);
    float v57 = v55 * v56;
    int v58 = 2;
    float v59 = antfly_qk_half_le_to_float(block + v58);
    int v60 = 4;
    int v61 = v2 < v60;
    int v62 = 8;
    int v63 = v2 + v62;
    int v64 = 8;
    int v65 = v2 + v64;
    int v66 = v61 ? v63 : v65;
    int v67 = int(block[v66]);
    int v68 = 4;
    int v69 = int(uint(v67) >> v68);
    int v70 = 4;
    int v71 = 4;
    int v72 = v2 + v71;
    int v73 = v61 ? v70 : v72;
    int v74 = int(block[v73]);
    int v75 = 6;
    int v76 = int(uint(v74) >> v75);
    int v77 = 4;
    int v78 = int(uint(v76) << v77);
    int v79 = v69 | v78;
    int v80 = 63;
    int v81 = v67 & v80;
    int v82 = v61 ? v81 : v79;
    float v83 = float(v82);
    float v84 = v59 * v83;
    float v85 = v57 - v84;
    return v85;
}
kernel void antfly_q5_k_small_batch_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q5_k [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    uint tid = thread_pos.x; int col = int(group_pos.x); int row = int(group_pos.y); if (row >= rows || rows < 2 || rows > 8 || col >= out_dim || (in_dim & 255) != 0) return; float acc = 0.0f; int block_count = in_dim >> 8;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) { device const uchar *block = weight_q5_k + ((col * block_count + block_idx) * 176); int base = block_idx << 8; for (int lane = int(tid); lane < 256; lane += 256) acc += input[row * in_dim + base + lane] * antfly_q5_k_dequant_lane_v2(block, lane); }
    threadgroup float partial[32]; acc = simd_sum(acc); if (lane_id == 0u) partial[simdgroup_id] = acc; if (simdgroup_id == 0u && lane_id >= 8u) partial[lane_id] = 0.0f; threadgroup_barrier(mem_flags::mem_threadgroup); float total = simd_sum(partial[lane_id]); if (lane_id == 0u && simdgroup_id == 0u) output[row * out_dim + col] = total;
}
