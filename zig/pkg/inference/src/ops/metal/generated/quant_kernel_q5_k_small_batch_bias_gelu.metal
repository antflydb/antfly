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
// plan_id=metal/q5_k/rows_2_8/bias_gelu/small_batch
// kernel_id=antfly_q5_k_small_batch_bias_gelu_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=false
// General MSL lowering smoke for descriptor-driven quant matmul epilogues.
// Production Metal dispatch stays on native handwritten MSL until this
// candidate clears correctness and benchmark gates.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_gelu(float x) { if (!isfinite(x)) return 0.0f; float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x); if (inner > 10.0f) return x; if (inner < -10.0f) return 0.0f; float y = 0.5f * x * (1.0f + tanh(inner)); return isfinite(y) ? y : 0.0f; }
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
    int v13 = v2 & v5;
    int v14 = 4;
    int v15 = int(uint(v12) >> v14);
    int v16 = 15;
    int v17 = v12 & v16;
    int v18 = v13 ? v15 : v17;
    int v19 = 16;
    int v20 = v19 + v4;
    int v21 = int(block[v20]);
    int v22 = int(uint(v21) >> v2);
    int v23 = v22 & v5;
    int v24 = int(uint(v23) << v14);
    int v25 = v18 + v24;
    int v26 = 0;
    float v27 = antfly_qk_half_le_to_float(block + v26);
    int v28 = v2 < v14;
    int v29 = v2 + v14;
    int v30 = 8;
    int v31 = v2 + v30;
    int v32 = v28 ? v29 : v31;
    int v33 = int(block[v32]);
    int v34 = v33 & v16;
    int v35 = v2 + v26;
    int v36 = v28 ? v14 : v35;
    int v37 = int(block[v36]);
    int v38 = 6;
    int v39 = int(uint(v37) >> v38);
    int v40 = int(uint(v39) << v14);
    int v41 = v34 | v40;
    int v42 = 63;
    int v43 = v33 & v42;
    int v44 = v28 ? v43 : v41;
    float v45 = float(v44);
    float v46 = v27 * v45;
    float v47 = float(v25);
    float v48 = v46 * v47;
    int v49 = 2;
    float v50 = antfly_qk_half_le_to_float(block + v49);
    int v51 = v28 ? v31 : v31;
    int v52 = int(block[v51]);
    int v53 = int(uint(v52) >> v14);
    int v54 = v28 ? v14 : v29;
    int v55 = int(block[v54]);
    int v56 = int(uint(v55) >> v38);
    int v57 = int(uint(v56) << v14);
    int v58 = v53 | v57;
    int v59 = v52 & v42;
    int v60 = v28 ? v59 : v58;
    float v61 = float(v60);
    float v62 = v50 * v61;
    float v63 = v48 - v62;
    return v63;
}
struct antfly_projection_decode_q5_k {
    enum : int { values = 256, bytes = 176, shift = 8 };
    __attribute__((always_inline)) static float decode(device const uchar* block, int lane) { return antfly_q5_k_dequant_lane_v2(block, lane); }
};
#include "../../../graph/templates/metal_projection_native.metal"
kernel void antfly_q5_k_small_batch_bias_gelu_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q5_k [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant int &rows [[buffer(4)]], constant int &in_dim [[buffer(5)]], constant int &out_dim [[buffer(6)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    threadgroup float partial[32];
    antfly_projection_column<antfly_projection_decode_q5_k, antfly_projection_bias_gelu, 128u, 2u>(input, weight_q5_k, bias, output, rows, in_dim, out_dim, thread_pos, group_pos, lane_id, simdgroup_id, partial);
}
