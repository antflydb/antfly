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
// plan_id=metal/q4_k/rows_2_8/none/small_batch
// kernel_id=antfly_q4_k_small_batch_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=true
// Promoted after the two-row by sixteen-column register tile cleared
// BGE-M3 model correctness and speed checks; runtime remains opt-in.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline void antfly_qk_unpack_scale_min_6bit(device const uchar *scales, int sub, thread float &scale, thread float &min_v) {
    if (sub < 4) { scale = float(scales[sub] & 63u); min_v = float(scales[sub + 4] & 63u); return; }
    scale = float((scales[sub + 4] & 0x0fu) | ((scales[sub - 4] >> 6) << 4)); min_v = float((scales[sub + 4] >> 4) | ((scales[sub] >> 6) << 4));
}
inline float antfly_q4_k_dequant_lane_v2(device const uchar *block, int lane) {
    device const uchar *scales = block + 4; device const uchar *qs = block + 16; int sub = lane >> 5; int q_index = (sub >> 1) * 32 + (lane & 31);
    uchar packed = qs[q_index]; int q = (sub & 1) == 0 ? int(packed & 0x0fu) : int(packed >> 4);
    float raw_scale = 0.0f; float raw_min = 0.0f; antfly_qk_unpack_scale_min_6bit(scales, sub, raw_scale, raw_min);
    return antfly_qk_half_le_to_float(block) * raw_scale * float(q) - antfly_qk_half_le_to_float(block + 2) * raw_min;
}
kernel void antfly_q4_k_small_batch_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q4_k [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    const uint NSG = 4u; const uint NC = 4u; const uint NR = 2u; const uint first_o = (group_pos.x * NSG + uint(simdgroup_id)) * NC; const uint first_r = group_pos.y * NR;
    if (rows < 2 || in_dim <= 0 || (uint(in_dim) & 255u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return; const uint block_count = uint(in_dim) >> 8; const uint sub = uint(lane_id) >> 2; const uint j0 = (uint(lane_id) & 3u) << 3; const uint chunk = sub >> 1; const bool high = (sub & 1u) != 0u; float acc[4][2]; for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] = 0.0f;
    for (uint b = 0u; b < block_count; ++b) { uint input_off = first_r * uint(in_dim) + b * 256u + sub * 32u + j0; float x[2][8]; for (uint rr = 0u; rr < NR; ++rr) for (uint i = 0u; i < 8u; ++i) x[rr][i] = first_r + rr < uint(rows) ? input[input_off + rr * uint(in_dim) + i] : 0.0f;
        for (uint c = 0u; c < NC; ++c) { uint col = first_o + c; if (col >= uint(out_dim)) continue; device const uchar *block = weight_q4_k + (col * block_count + b) * 144u; float raw_scale = 0.0f; float raw_min = 0.0f; antfly_qk_unpack_scale_min_6bit(block + 4u, int(sub), raw_scale, raw_min); float dsc = antfly_qk_half_le_to_float(block) * raw_scale; float dmn = antfly_qk_half_le_to_float(block + 2u) * raw_min; device const uchar *qs = block + 16u + chunk * 32u + j0; for (uint i = 0u; i < 8u; ++i) { uchar packed = qs[i]; float w = dsc * float(high ? packed >> 4 : packed & 0x0fu) - dmn; for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] += x[rr][i] * w; } } }
    for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) { float total = simd_sum(acc[c][rr]); uint col = first_o + c; uint row = first_r + rr; if (lane_id == 0u && col < uint(out_dim) && row < uint(rows)) output[row * uint(out_dim) + col] = total; }
}
