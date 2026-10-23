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

// Dev-only generated Metal shadow artifact from graph/quant_kernel_compiler.zig.
// plan_id=metal/q4_k/rows_2_8/bias/small_batch
// kernel_id=antfly_q4_k_small_batch_bias_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=false
// Validated in the direct runtime harness; normal model execution does not
// call the fused-bias API, so production stays on the no-bias route plus bias op.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline float antfly_qk_header_scale(device const uchar *block, int lane) {
    int v0 = lane;
    int v1 = 4;
    int v2 = v0 < v1;
    int v3 = 0;
    int v4 = v0 + v3;
    int v5 = 4;
    int v6 = v0 + v5;
    int v7 = v2 ? v4 : v6;
    int v8 = int(block[v7]);
    int v9 = 15;
    int v10 = v8 & v9;
    int v11 = 0;
    int v12 = -4;
    int v13 = v0 + v12;
    int v14 = v2 ? v11 : v13;
    int v15 = int(block[v14]);
    int v16 = 6;
    int v17 = int(uint(v15) >> v16);
    int v18 = 4;
    int v19 = int(uint(v17) << v18);
    int v20 = v10 | v19;
    int v21 = 63;
    int v22 = v8 & v21;
    int v23 = v2 ? v22 : v20;
    float v24 = float(v23);
    return v24;
}
inline float antfly_qk_header_min(device const uchar *block, int lane) {
    int v0 = lane;
    int v1 = 4;
    int v2 = v0 < v1;
    int v3 = 4;
    int v4 = v0 + v3;
    int v5 = 4;
    int v6 = v0 + v5;
    int v7 = v2 ? v4 : v6;
    int v8 = int(block[v7]);
    int v9 = 4;
    int v10 = int(uint(v8) >> v9);
    int v11 = 0;
    int v12 = 0;
    int v13 = v0 + v12;
    int v14 = v2 ? v11 : v13;
    int v15 = int(block[v14]);
    int v16 = 6;
    int v17 = int(uint(v15) >> v16);
    int v18 = 4;
    int v19 = int(uint(v17) << v18);
    int v20 = v10 | v19;
    int v21 = 63;
    int v22 = v8 & v21;
    int v23 = v2 ? v22 : v20;
    float v24 = float(v23);
    return v24;
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
    int v13 = 1;
    int v14 = v2 & v13;
    int v15 = 4;
    int v16 = int(uint(v12) >> v15);
    int v17 = 15;
    int v18 = v12 & v17;
    int v19 = v14 ? v16 : v18;
    int v20 = 0;
    float v21 = antfly_qk_half_le_to_float(block + v20);
    int v22 = 4;
    int v23 = v2 < v22;
    int v24 = 4;
    int v25 = v2 + v24;
    int v26 = 8;
    int v27 = v2 + v26;
    int v28 = v23 ? v25 : v27;
    int v29 = int(block[v28]);
    int v30 = 15;
    int v31 = v29 & v30;
    int v32 = 4;
    int v33 = 0;
    int v34 = v2 + v33;
    int v35 = v23 ? v32 : v34;
    int v36 = int(block[v35]);
    int v37 = 6;
    int v38 = int(uint(v36) >> v37);
    int v39 = 4;
    int v40 = int(uint(v38) << v39);
    int v41 = v31 | v40;
    int v42 = 63;
    int v43 = v29 & v42;
    int v44 = v23 ? v43 : v41;
    float v45 = float(v44);
    float v46 = v21 * v45;
    float v47 = float(v19);
    float v48 = v46 * v47;
    int v49 = 2;
    float v50 = antfly_qk_half_le_to_float(block + v49);
    int v51 = 4;
    int v52 = v2 < v51;
    int v53 = 8;
    int v54 = v2 + v53;
    int v55 = 8;
    int v56 = v2 + v55;
    int v57 = v52 ? v54 : v56;
    int v58 = int(block[v57]);
    int v59 = 4;
    int v60 = int(uint(v58) >> v59);
    int v61 = 4;
    int v62 = 4;
    int v63 = v2 + v62;
    int v64 = v52 ? v61 : v63;
    int v65 = int(block[v64]);
    int v66 = 6;
    int v67 = int(uint(v65) >> v66);
    int v68 = 4;
    int v69 = int(uint(v67) << v68);
    int v70 = v60 | v69;
    int v71 = 63;
    int v72 = v58 & v71;
    int v73 = v52 ? v72 : v70;
    float v74 = float(v73);
    float v75 = v50 * v74;
    float v76 = v48 - v75;
    return v76;
}
kernel void antfly_q4_k_small_batch_bias_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q4_k [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant int &rows [[buffer(4)]], constant int &in_dim [[buffer(5)]], constant int &out_dim [[buffer(6)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    const uint NSG = 4u; const uint NC = 4u; const uint NR = 2u; const uint first_o = (group_pos.x * NSG + uint(simdgroup_id)) * NC; const uint first_r = group_pos.y * NR;
    if (rows < 2 || in_dim <= 0 || (uint(in_dim) & 255u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return; const uint block_count = uint(in_dim) >> 8; const uint sub = uint(lane_id) >> 2; const uint j0 = (uint(lane_id) & 3u) << 3; const uint chunk = sub >> 1; const bool high = (sub & 1u) != 0u; float acc[4][2]; for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] = 0.0f;
    for (uint b = 0u; b < block_count; ++b) { uint input_off = first_r * uint(in_dim) + b * 256u + sub * 32u + j0; float x[2][8]; for (uint rr = 0u; rr < NR; ++rr) for (uint i = 0u; i < 8u; ++i) x[rr][i] = first_r + rr < uint(rows) ? input[input_off + rr * uint(in_dim) + i] : 0.0f;
        for (uint c = 0u; c < NC; ++c) { uint col = first_o + c; if (col >= uint(out_dim)) continue; device const uchar *block = weight_q4_k + (col * block_count + b) * 144u; float raw_scale = 0.0f; float raw_min = 0.0f; antfly_qk_unpack_scale_min_6bit(block + 4u, int(sub), raw_scale, raw_min); float dsc = antfly_qk_half_le_to_float(block) * raw_scale; float dmn = antfly_qk_half_le_to_float(block + 2u) * raw_min; device const uchar *qs = block + 16u + chunk * 32u + j0; for (uint i = 0u; i < 8u; ++i) { uchar packed = qs[i]; float w = dsc * float(high ? packed >> 4 : packed & 0x0fu) - dmn; for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] += x[rr][i] * w; } } }
    for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) { float total = simd_sum(acc[c][rr]); uint col = first_o + c; uint row = first_r + rr; if (lane_id == 0u && col < uint(out_dim) && row < uint(rows)) output[row * uint(out_dim) + col] = total + bias[col]; }
}
