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
// plan_id=metal/q4_k/rows_2_8/bias_gelu/small_batch
// kernel_id=antfly_q4_k_small_batch_bias_gelu_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=false
// General MSL lowering smoke for descriptor-driven quant matmul epilogues.
// Production Metal dispatch stays on native handwritten MSL until this
// candidate clears correctness and benchmark gates.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline float antfly_qk_gelu(float x) { if (!isfinite(x)) return 0.0f; float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x); if (inner > 10.0f) return x; if (inner < -10.0f) return 0.0f; float y = 0.5f * x * (1.0f + tanh(inner)); return isfinite(y) ? y : 0.0f; }
inline float antfly_qk_header_scale(device const uchar *block, int lane) {
    int v0 = lane;
    int v1 = 4;
    int v2 = v0 < v1;
    int v3 = 0;
    int v4 = v0 + v3;
    int v5 = v0 + v1;
    int v6 = v2 ? v4 : v5;
    int v7 = int(block[v6]);
    int v8 = 15;
    int v9 = v7 & v8;
    int v10 = -4;
    int v11 = v0 + v10;
    int v12 = v2 ? v3 : v11;
    int v13 = int(block[v12]);
    int v14 = 6;
    int v15 = int(uint(v13) >> v14);
    int v16 = int(uint(v15) << v1);
    int v17 = v9 | v16;
    int v18 = 63;
    int v19 = v7 & v18;
    int v20 = v2 ? v19 : v17;
    float v21 = float(v20);
    return v21;
}
inline float antfly_qk_header_min(device const uchar *block, int lane) {
    int v0 = lane;
    int v1 = 4;
    int v2 = v0 < v1;
    int v3 = v0 + v1;
    int v4 = v2 ? v3 : v3;
    int v5 = int(block[v4]);
    int v6 = int(uint(v5) >> v1);
    int v7 = 0;
    int v8 = v0 + v7;
    int v9 = v2 ? v7 : v8;
    int v10 = int(block[v9]);
    int v11 = 6;
    int v12 = int(uint(v10) >> v11);
    int v13 = int(uint(v12) << v1);
    int v14 = v6 | v13;
    int v15 = 63;
    int v16 = v5 & v15;
    int v17 = v2 ? v16 : v14;
    float v18 = float(v17);
    return v18;
}
inline void antfly_qk_unpack_scale_min_6bit(device const uchar *scales, int sub, thread float &scale, thread float &min_v) { scale = antfly_qk_header_scale(scales, sub); min_v = antfly_qk_header_min(scales, sub); }
inline float antfly_q4_k_dequant_lane_v2(device const uchar *block, int lane) {
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
    int v10 = 16;
    int v11 = v10 + v9;
    int v12 = int(block[v11]);
    int v13 = v2 & v5;
    int v14 = 4;
    int v15 = int(uint(v12) >> v14);
    int v16 = 15;
    int v17 = v12 & v16;
    int v18 = v13 ? v15 : v17;
    int v19 = 0;
    float v20 = antfly_qk_half_le_to_float(block + v19);
    int v21 = v2 < v14;
    int v22 = v2 + v14;
    int v23 = 8;
    int v24 = v2 + v23;
    int v25 = v21 ? v22 : v24;
    int v26 = int(block[v25]);
    int v27 = v26 & v16;
    int v28 = v2 + v19;
    int v29 = v21 ? v14 : v28;
    int v30 = int(block[v29]);
    int v31 = 6;
    int v32 = int(uint(v30) >> v31);
    int v33 = int(uint(v32) << v14);
    int v34 = v27 | v33;
    int v35 = 63;
    int v36 = v26 & v35;
    int v37 = v21 ? v36 : v34;
    float v38 = float(v37);
    float v39 = v20 * v38;
    float v40 = float(v18);
    float v41 = v39 * v40;
    int v42 = 2;
    float v43 = antfly_qk_half_le_to_float(block + v42);
    int v44 = v21 ? v24 : v24;
    int v45 = int(block[v44]);
    int v46 = int(uint(v45) >> v14);
    int v47 = v21 ? v14 : v22;
    int v48 = int(block[v47]);
    int v49 = int(uint(v48) >> v31);
    int v50 = int(uint(v49) << v14);
    int v51 = v46 | v50;
    int v52 = v45 & v35;
    int v53 = v21 ? v52 : v51;
    float v54 = float(v53);
    float v55 = v43 * v54;
    float v56 = v41 - v55;
    return v56;
}
kernel void antfly_q4_k_small_batch_bias_gelu_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q4_k [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant int &rows [[buffer(4)]], constant int &in_dim [[buffer(5)]], constant int &out_dim [[buffer(6)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    const uint NSG = 4u; const uint NC = 4u; const uint NR = 2u; const uint first_o = (group_pos.x * NSG + uint(simdgroup_id)) * NC; const uint first_r = group_pos.y * NR;
    if (rows < 2 || in_dim <= 0 || (uint(in_dim) & 255u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return; const uint block_count = uint(in_dim) >> 8; const uint sub = uint(lane_id) >> 2; const uint j0 = (uint(lane_id) & 3u) << 3; const uint chunk = sub >> 1; const bool high = (sub & 1u) != 0u; float acc[4][2]; for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] = 0.0f;
    for (uint b = 0u; b < block_count; ++b) { uint input_off = first_r * uint(in_dim) + b * 256u + sub * 32u + j0; float x[2][8]; for (uint rr = 0u; rr < NR; ++rr) for (uint i = 0u; i < 8u; ++i) x[rr][i] = first_r + rr < uint(rows) ? input[input_off + rr * uint(in_dim) + i] : 0.0f;
        for (uint c = 0u; c < NC; ++c) { uint col = first_o + c; if (col >= uint(out_dim)) continue; device const uchar *block = weight_q4_k + (col * block_count + b) * 144u; float raw_scale = 0.0f; float raw_min = 0.0f; antfly_qk_unpack_scale_min_6bit(block + 4u, int(sub), raw_scale, raw_min); float dsc = antfly_qk_half_le_to_float(block) * raw_scale; float dmn = antfly_qk_half_le_to_float(block + 2u) * raw_min; device const uchar *qs = block + 16u + chunk * 32u + j0; for (uint i = 0u; i < 8u; ++i) { uchar packed = qs[i]; float w = dsc * float(high ? packed >> 4 : packed & 0x0fu) - dmn; for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] += x[rr][i] * w; } } }
    for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) { float total = simd_sum(acc[c][rr]); uint col = first_o + c; uint row = first_r + rr; if (lane_id == 0u && col < uint(out_dim) && row < uint(rows)) output[row * uint(out_dim) + col] = antfly_qk_gelu(total + bias[col]); }
}
