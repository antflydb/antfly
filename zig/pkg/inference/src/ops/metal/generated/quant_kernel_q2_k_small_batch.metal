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
// plan_id=metal/q2_k/rows_2_8/none/small_batch
// kernel_id=antfly_q2_k_small_batch_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=true
// Promoted after sequential Metal runtime evidence cleared correctness,
// route, and speedup gates.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_gelu(float x) { if (!isfinite(x)) return 0.0f; float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x); if (inner > 10.0f) return x; if (inner < -10.0f) return 0.0f; float y = 0.5f * x * (1.0f + tanh(inner)); return isfinite(y) ? y : 0.0f; }
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
    int v9 = v2 & v7;
    int v10 = int(uint(v9) << v1);
    int v11 = 15;
    int v12 = v0 & v11;
    int v13 = v10 + v12;
    int v14 = 5;
    int v15 = int(uint(v4) << v14);
    int v16 = v15 + v13;
    int v17 = 20;
    int v18 = v16 + v17;
    int v19 = int(block[v18]);
    int v20 = int(uint(v8) << v7);
    int v21 = int(uint(v19) >> v20);
    int v22 = v21 & v3;
    int v23 = int(block[v2]);
    int v24 = 16;
    float v25 = antfly_qk_half_le_to_float(block + v24);
    int v26 = v23 & v11;
    float v27 = float(v26);
    float v28 = v25 * v27;
    float v29 = float(v22);
    float v30 = v28 * v29;
    int v31 = 18;
    float v32 = antfly_qk_half_le_to_float(block + v31);
    int v33 = int(uint(v23) >> v1);
    float v34 = float(v33);
    float v35 = v32 * v34;
    float v36 = v30 - v35;
    return v36;
}
struct antfly_projection_decode_q2_k {
    enum : int { values = 256, bytes = 84, shift = 8 };
    __attribute__((always_inline)) static float decode(device const uchar* block, int lane) { return antfly_q2_k_dequant_lane_v2(block, lane); }
};
#include "../../../graph/templates/metal_projection_native.metal"
kernel void antfly_q2_k_small_batch_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q2_k [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    threadgroup float partial[32];
    antfly_projection_column<antfly_projection_decode_q2_k, antfly_projection_plain, 128u, 2u>(input, weight_q2_k, nullptr, output, rows, in_dim, out_dim, thread_pos, group_pos, lane_id, simdgroup_id, partial);
}
