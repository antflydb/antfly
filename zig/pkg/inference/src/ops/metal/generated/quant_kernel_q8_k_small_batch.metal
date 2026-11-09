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
// plan_id=metal/q8_k/rows_2_8/none/small_batch
// kernel_id=antfly_q8_k_small_batch_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=true
// Promoted after the schedule sweep re-tuned this route to 64-thread
// hybrid-simd and the decode-runtime speedup gate cleared vs handwritten.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_gelu(float x) { if (!isfinite(x)) return 0.0f; float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x); if (inner > 10.0f) return x; if (inner < -10.0f) return 0.0f; float y = 0.5f * x * (1.0f + tanh(inner)); return isfinite(y) ? y : 0.0f; }
inline float antfly_qk_f32_le_to_float(device const uchar *p) { uint bits = uint(p[0]) | (uint(p[1]) << 8) | (uint(p[2]) << 16) | (uint(p[3]) << 24); return as_type<float>(bits); }
inline float antfly_q8_k_dequant_lane_v2(device const uchar *block, int lane) {
    int v0 = lane;
    int v1 = 0;
    float v2 = antfly_qk_f32_le_to_float(block + v1);
    int v3 = 4;
    int v4 = v0 + v3;
    int v5 = int(((device const char *)block)[v4]);
    float v6 = float(v5);
    float v7 = v2 * v6;
    return v7;
}
struct antfly_projection_decode_q8_k {
    enum : int { values = 256, bytes = 292, shift = 8 };
    __attribute__((always_inline)) static float decode(device const uchar* block, int lane) { return antfly_q8_k_dequant_lane_v2(block, lane); }
};
#include "../../../graph/templates/metal_projection_native.metal"
kernel void antfly_q8_k_small_batch_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q8_k [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    threadgroup float partial[32];
    antfly_projection_column<antfly_projection_decode_q8_k, antfly_projection_plain, 64u, 2u>(input, weight_q8_k, nullptr, output, rows, in_dim, out_dim, thread_pos, group_pos, lane_id, simdgroup_id, partial);
}
