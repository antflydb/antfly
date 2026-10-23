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
// plan_id=metal/q6_k/rows_2_8/bias_gelu/small_batch
// kernel_id=antfly_q6_k_small_batch_bias_gelu_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=false
// General MSL lowering smoke for descriptor-driven quant matmul epilogues.
// Production Metal dispatch stays on native handwritten MSL until this
// candidate clears correctness and benchmark gates.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline float antfly_qk_gelu(float x) { if (!isfinite(x)) return 0.0f; float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x); if (inner > 10.0f) return x; if (inner < -10.0f) return 0.0f; float y = 0.5f * x * (1.0f + tanh(inner)); return isfinite(y) ? y : 0.0f; }
inline float antfly_qk_q6_k_scale(device const uchar *block, int lane) {
    int v0 = lane;
    int v1 = 4;
    int v2 = int(uint(v0) >> v1);
    int v3 = 208;
    float v4 = antfly_qk_half_le_to_float(block + v3);
    int v5 = 192;
    int v6 = v5 + v2;
    int v7 = int(((device const char *)block)[v6]);
    float v8 = float(v7);
    float v9 = v4 * v8;
    return v9;
}
inline float antfly_qk_q6_k_value(device const uchar *block, int lane) {
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
    int v14 = 64;
    int v15 = v4 * v14;
    int v16 = v8 & v7;
    int v17 = 32;
    int v18 = v16 * v17;
    int v19 = v15 + v18;
    int v20 = v19 + v13;
    int v21 = 128;
    int v22 = v4 * v17;
    int v23 = v21 + v22;
    int v24 = v23 + v13;
    int v25 = int(block[v20]);
    int v26 = int(uint(v8) >> v7);
    int v27 = v26 * v1;
    int v28 = int(uint(v25) >> v27);
    int v29 = v28 & v11;
    int v30 = int(block[v24]);
    int v31 = 2;
    int v32 = v8 * v31;
    int v33 = int(uint(v30) >> v32);
    int v34 = v33 & v3;
    int v35 = int(uint(v34) << v1);
    int v36 = v29 | v35;
    int v37 = v36 - v17;
    float v38 = float(v37);
    return v38;
}
inline float antfly_q6_k_dequant_lane_v2(device const uchar *block, int lane) {
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
    int v14 = 64;
    int v15 = v4 * v14;
    int v16 = v8 & v7;
    int v17 = 32;
    int v18 = v16 * v17;
    int v19 = v15 + v18;
    int v20 = v19 + v13;
    int v21 = 128;
    int v22 = v4 * v17;
    int v23 = v21 + v22;
    int v24 = v23 + v13;
    int v25 = int(block[v20]);
    int v26 = int(uint(v8) >> v7);
    int v27 = v26 * v1;
    int v28 = int(uint(v25) >> v27);
    int v29 = v28 & v11;
    int v30 = int(block[v24]);
    int v31 = 2;
    int v32 = v8 * v31;
    int v33 = int(uint(v30) >> v32);
    int v34 = v33 & v3;
    int v35 = int(uint(v34) << v1);
    int v36 = v29 | v35;
    int v37 = v36 - v17;
    int v38 = 208;
    float v39 = antfly_qk_half_le_to_float(block + v38);
    int v40 = 192;
    int v41 = v40 + v2;
    int v42 = int(((device const char *)block)[v41]);
    float v43 = float(v42);
    float v44 = v39 * v43;
    float v45 = float(v37);
    float v46 = v44 * v45;
    return v46;
}
kernel void antfly_q6_k_small_batch_bias_gelu_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q6_k [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant int &rows [[buffer(4)]], constant int &in_dim [[buffer(5)]], constant int &out_dim [[buffer(6)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    const uint NSG = 4u; const uint NC = 4u; const uint NR = 2u; const uint first_o = (group_pos.x * NSG + uint(simdgroup_id)) * NC; const uint first_r = group_pos.y * NR;
    if (rows < 2 || in_dim <= 0 || (uint(in_dim) & 255u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return; const uint block_count = uint(in_dim) >> 8; const uint sub = uint(lane_id) >> 1; const uint j0 = (uint(lane_id) & 1u) << 3; float acc[4][2]; for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] = 0.0f;
    for (uint b = 0u; b < block_count; ++b) { uint input_off = first_r * uint(in_dim) + b * 256u + sub * 16u + j0; float x[2][8]; for (uint rr = 0u; rr < NR; ++rr) for (uint i = 0u; i < 8u; ++i) x[rr][i] = first_r + rr < uint(rows) ? input[input_off + rr * uint(in_dim) + i] : 0.0f;
        for (uint c = 0u; c < NC; ++c) { uint col = first_o + c; if (col >= uint(out_dim)) continue; device const uchar *block = weight_q6_k + (col * block_count + b) * 210u; float dscale = antfly_qk_q6_k_scale(block, int(sub * 16u)); for (uint i = 0u; i < 8u; ++i) { float w = dscale * antfly_qk_q6_k_value(block, int(sub * 16u + j0 + i)); for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] += x[rr][i] * w; } } }
    for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) { float total = simd_sum(acc[c][rr]); uint col = first_o + c; uint row = first_r + rr; if (lane_id == 0u && col < uint(out_dim) && row < uint(rows)) output[row * uint(out_dim) + col] = antfly_qk_gelu(total + bias[col]); }
}
