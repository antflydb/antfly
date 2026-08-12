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
// plan_id=metal/q6_k/rows_2_8/bias/small_batch
// kernel_id=antfly_q6_k_small_batch_bias_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=false
// Validated in the direct runtime harness; normal model execution does not
// call the fused-bias API, so production stays on the no-bias route plus bias op.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline float antfly_q6_k_dequant_lane_v2(device const uchar *block, int lane) {
    device const uchar *ql = block; device const uchar *qh = block + 128; device const uchar *scales = block + 192; int sub = lane >> 4; int i = lane & 15; int half_idx = sub >> 3; int group = (sub & 7) >> 1; int l = ((sub & 1) << 4) + i;
    int ql_off = half_idx * 64 + (group & 1) * 32; int qh_off = half_idx * 32; int qh_shift = group * 2; int nibble_shift = (group >> 1) * 4; int low4 = int((ql[ql_off + l] >> nibble_shift) & 0x0fu); int high2 = int((qh[qh_off + l] >> qh_shift) & 0x03u);
    int scale_u = int(scales[sub]); int scale = scale_u >= 128 ? scale_u - 256 : scale_u; return antfly_qk_half_le_to_float(block + 208) * float(scale) * float((low4 | (high2 << 4)) - 32);
}
kernel void antfly_q6_k_small_batch_bias_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q6_k [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant int &rows [[buffer(4)]], constant int &in_dim [[buffer(5)]], constant int &out_dim [[buffer(6)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    const uint NSG = 4u; const uint NC = 4u; const uint NR = 2u; const uint first_o = (group_pos.x * NSG + uint(simdgroup_id)) * NC; const uint first_r = group_pos.y * NR;
    if (rows < 2 || in_dim <= 0 || (uint(in_dim) & 255u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return; const uint block_count = uint(in_dim) >> 8; const uint sub = uint(lane_id) >> 1; const uint j0 = (uint(lane_id) & 1u) << 3; const uint half_idx = sub >> 3; const uint group = (sub & 7u) >> 1; const uint l0 = ((sub & 1u) << 4) + j0; const uint ql_off = half_idx * 64u + (group & 1u) * 32u; const uint qh_off = half_idx * 32u; const uint qh_shift = group * 2u; const uint nibble_shift = (group >> 1) * 4u; float acc[4][2]; for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] = 0.0f;
    for (uint b = 0u; b < block_count; ++b) { uint input_off = first_r * uint(in_dim) + b * 256u + sub * 16u + j0; float x[2][8]; for (uint rr = 0u; rr < NR; ++rr) for (uint i = 0u; i < 8u; ++i) x[rr][i] = first_r + rr < uint(rows) ? input[input_off + rr * uint(in_dim) + i] : 0.0f;
        for (uint c = 0u; c < NC; ++c) { uint col = first_o + c; if (col >= uint(out_dim)) continue; device const uchar *block = weight_q6_k + (col * block_count + b) * 210u; device const uchar *ql = block; device const uchar *qh = block + 128u; int scale_u = int(block[192u + sub]); int scale = scale_u >= 128 ? scale_u - 256 : scale_u; float dscale = antfly_qk_half_le_to_float(block + 208u) * float(scale); for (uint i = 0u; i < 8u; ++i) { uint l = l0 + i; int low4 = int((ql[ql_off + l] >> nibble_shift) & 0x0fu); int high2 = int((qh[qh_off + l] >> qh_shift) & 0x03u); float w = dscale * float((low4 | (high2 << 4)) - 32); for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] += x[rr][i] * w; } } }
    for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) { float total = simd_sum(acc[c][rr]); uint col = first_o + c; uint row = first_r + rr; if (lane_id == 0u && col < uint(out_dim) && row < uint(rows)) output[row * uint(out_dim) + col] = total + bias[col]; }
}
