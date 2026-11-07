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

// Generated Metal candidate artifact from graph/quant_kernel_compiler.zig.
// plan_id=metal/q4_0/rows_2_8/none/small_batch
// kernel_id=antfly_q4_0_small_batch_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=false
// General MSL lowering smoke for descriptor-driven quant matmul.
// Production Metal dispatch stays on native handwritten MSL until this
// candidate clears correctness and benchmark gates.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_gelu(float x) { if (!isfinite(x)) return 0.0f; float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x); if (inner > 10.0f) return x; if (inner < -10.0f) return 0.0f; float y = 0.5f * x * (1.0f + tanh(inner)); return isfinite(y) ? y : 0.0f; }
inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline float antfly_q4_0_dequant_lane_v2(device const uchar *block, int lane) {
    int v0 = lane;
    int v1 = 2;
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
    int v12 = 8;
    int v13 = v11 - v12;
    int v14 = 0;
    float v15 = antfly_qk_half_le_to_float(block + v14);
    float v16 = float(v13);
    float v17 = v15 * v16;
    return v17;
}
struct antfly_projection_decode_q4_0 {
    enum : int { values = 32, bytes = 18, shift = 5 };
    __attribute__((always_inline)) static float decode(device const uchar* block, int lane) { return antfly_q4_0_dequant_lane_v2(block, lane); }
};
#include "../../../graph/templates/metal_projection_native.metal"
kernel void antfly_q4_0_small_batch_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q4_0 [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]) {
    antfly_projection_column<antfly_projection_decode_q4_0, antfly_projection_plain, 32u, 0u>(input, weight_q4_0, nullptr, output, rows, in_dim, out_dim, thread_pos, group_pos, 0, 0, nullptr);
}
