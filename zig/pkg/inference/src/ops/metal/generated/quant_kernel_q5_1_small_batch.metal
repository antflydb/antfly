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

inline float antfly_qk_gelu(float x) { if (!isfinite(x)) return 0.0f; float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x); if (inner > 10.0f) return x; if (inner < -10.0f) return 0.0f; float y = 0.5f * x * (1.0f + tanh(inner)); return isfinite(y) ? y : 0.0f; }
inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline uint antfly_qk_u32_le(device const uchar *p) { return uint(p[0]) | (uint(p[1]) << 8) | (uint(p[2]) << 16) | (uint(p[3]) << 24); }
inline float antfly_q5_1_dequant_lane_v2(device const uchar *block, int lane) {
    int v0 = lane;
    int v1 = 8;
    int v2 = 15;
    int v3 = v0 & v2;
    int v4 = v1 + v3;
    int v5 = int(block[v4]);
    int v6 = 16;
    int v7 = v0 < v6;
    int v8 = v5 & v2;
    int v9 = 4;
    int v10 = int(uint(v5) >> v9);
    int v11 = v7 ? v8 : v10;
    int v12 = int(antfly_qk_u32_le(block + v9));
    int v13 = int(uint(v12) >> v0);
    int v14 = 1;
    int v15 = v13 & v14;
    int v16 = int(uint(v15) << v9);
    int v17 = v11 | v16;
    int v18 = 0;
    float v19 = antfly_qk_half_le_to_float(block + v18);
    float v20 = float(v17);
    float v21 = v19 * v20;
    int v22 = 2;
    float v23 = antfly_qk_half_le_to_float(block + v22);
    float v24 = v21 + v23;
    return v24;
}
struct antfly_projection_decode_q5_1 {
    enum : int { values = 32, bytes = 24, shift = 5 };
    __attribute__((always_inline)) static float decode(device const uchar* block, int lane) { return antfly_q5_1_dequant_lane_v2(block, lane); }
};
#include "../../../graph/templates/metal_projection_native.metal"
kernel void antfly_q5_1_small_batch_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q5_1 [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]) {
    antfly_projection_two_columns<antfly_projection_decode_q5_1, antfly_projection_plain>(input, weight_q5_1, nullptr, output, rows, in_dim, out_dim, thread_pos, group_pos);
}
