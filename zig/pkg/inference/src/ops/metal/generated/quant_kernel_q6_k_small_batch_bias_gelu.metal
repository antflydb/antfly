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
    int v7 = int(block[v6]);
    int v8 = 128;
    int v9 = v7 < v8;
    int v10 = 256;
    int v11 = v7 - v10;
    int v12 = v9 ? v7 : v11;
    float v13 = float(v12);
    float v14 = v4 * v13;
    return v14;
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
    int v9 = 1;
    int v10 = v2 & v9;
    int v11 = 4;
    int v12 = int(uint(v10) << v11);
    int v13 = 15;
    int v14 = v0 & v13;
    int v15 = v12 + v14;
    int v16 = 64;
    int v17 = v4 * v16;
    int v18 = 1;
    int v19 = v8 & v18;
    int v20 = 32;
    int v21 = v19 * v20;
    int v22 = v17 + v21;
    int v23 = v22 + v15;
    int v24 = 128;
    int v25 = 32;
    int v26 = v4 * v25;
    int v27 = v24 + v26;
    int v28 = v27 + v15;
    int v29 = int(block[v23]);
    int v30 = 1;
    int v31 = int(uint(v8) >> v30);
    int v32 = 4;
    int v33 = v31 * v32;
    int v34 = int(uint(v29) >> v33);
    int v35 = 15;
    int v36 = v34 & v35;
    int v37 = int(block[v28]);
    int v38 = 2;
    int v39 = v8 * v38;
    int v40 = int(uint(v37) >> v39);
    int v41 = 3;
    int v42 = v40 & v41;
    int v43 = 4;
    int v44 = int(uint(v42) << v43);
    int v45 = v36 | v44;
    int v46 = 32;
    int v47 = v45 - v46;
    float v48 = float(v47);
    return v48;
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
    int v9 = 1;
    int v10 = v2 & v9;
    int v11 = 4;
    int v12 = int(uint(v10) << v11);
    int v13 = 15;
    int v14 = v0 & v13;
    int v15 = v12 + v14;
    int v16 = 64;
    int v17 = v4 * v16;
    int v18 = 1;
    int v19 = v8 & v18;
    int v20 = 32;
    int v21 = v19 * v20;
    int v22 = v17 + v21;
    int v23 = v22 + v15;
    int v24 = 128;
    int v25 = 32;
    int v26 = v4 * v25;
    int v27 = v24 + v26;
    int v28 = v27 + v15;
    int v29 = int(block[v23]);
    int v30 = 1;
    int v31 = int(uint(v8) >> v30);
    int v32 = 4;
    int v33 = v31 * v32;
    int v34 = int(uint(v29) >> v33);
    int v35 = 15;
    int v36 = v34 & v35;
    int v37 = int(block[v28]);
    int v38 = 2;
    int v39 = v8 * v38;
    int v40 = int(uint(v37) >> v39);
    int v41 = 3;
    int v42 = v40 & v41;
    int v43 = 4;
    int v44 = int(uint(v42) << v43);
    int v45 = v36 | v44;
    int v46 = 32;
    int v47 = v45 - v46;
    int v48 = 208;
    float v49 = antfly_qk_half_le_to_float(block + v48);
    int v50 = 192;
    int v51 = v50 + v2;
    int v52 = int(block[v51]);
    int v53 = 128;
    int v54 = v52 < v53;
    int v55 = 256;
    int v56 = v52 - v55;
    int v57 = v54 ? v52 : v56;
    float v58 = float(v57);
    float v59 = v49 * v58;
    float v60 = float(v47);
    float v61 = v59 * v60;
    return v61;
}
kernel void antfly_q6_k_small_batch_bias_gelu_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q6_k [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant int &rows [[buffer(4)]], constant int &in_dim [[buffer(5)]], constant int &out_dim [[buffer(6)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    const uint NSG = 4u; const uint NC = 4u; const uint NR = 2u; const uint first_o = (group_pos.x * NSG + uint(simdgroup_id)) * NC; const uint first_r = group_pos.y * NR;
    if (rows < 2 || in_dim <= 0 || (uint(in_dim) & 255u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return; const uint block_count = uint(in_dim) >> 8; const uint sub = uint(lane_id) >> 1; const uint j0 = (uint(lane_id) & 1u) << 3; float acc[4][2]; for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] = 0.0f;
    for (uint b = 0u; b < block_count; ++b) { uint input_off = first_r * uint(in_dim) + b * 256u + sub * 16u + j0; float x[2][8]; for (uint rr = 0u; rr < NR; ++rr) for (uint i = 0u; i < 8u; ++i) x[rr][i] = first_r + rr < uint(rows) ? input[input_off + rr * uint(in_dim) + i] : 0.0f;
        for (uint c = 0u; c < NC; ++c) { uint col = first_o + c; if (col >= uint(out_dim)) continue; device const uchar *block = weight_q6_k + (col * block_count + b) * 210u; float dscale = antfly_qk_q6_k_scale(block, int(sub * 16u)); for (uint i = 0u; i < 8u; ++i) { float w = dscale * antfly_qk_q6_k_value(block, int(sub * 16u + j0 + i)); for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] += x[rr][i] * w; } } }
    for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) { float total = simd_sum(acc[c][rr]); uint col = first_o + c; uint row = first_r + rr; if (lane_id == 0u && col < uint(out_dim) && row < uint(rows)) output[row * uint(out_dim) + col] = antfly_qk_gelu(total + bias[col]); }
}
