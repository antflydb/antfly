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
// plan_id=metal/q3_k/rows_2_8/none/small_batch
// kernel_id=antfly_q3_k_small_batch_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=true
// Promoted after sequential Metal runtime evidence cleared correctness,
// route, provider-route, and speedup gates.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline int antfly_q3_k_raw_scale(device const uchar *scale_data, uint sub) {
    uint i = sub & 3u; uint low = 0u; uint high = 0u;
    if (sub < 4u) { low = uint(scale_data[i] & 0x0fu); high = uint(scale_data[8u + i] & 0x03u); }
    else if (sub < 8u) { low = uint(scale_data[4u + i] & 0x0fu); high = uint((scale_data[8u + i] >> 2) & 0x03u); }
    else if (sub < 12u) { low = uint((scale_data[i] >> 4) & 0x0fu); high = uint((scale_data[8u + i] >> 4) & 0x03u); }
    else { low = uint((scale_data[4u + i] >> 4) & 0x0fu); high = uint((scale_data[8u + i] >> 6) & 0x03u); }
    return int(low | (high << 4)) - 32;
}
inline float antfly_q3_k_dequant_lane_v2(device const uchar *block, int lane) {
    uint sub = uint(lane) >> 4; uint i = uint(lane) & 15u; uint chunk = sub >> 3; uint group = (sub & 7u) >> 1; uint l = ((sub & 1u) << 4) + i; uint q_base = chunk << 5; uint shift = group << 1; uint hm_bit = (chunk << 2) + group;
    int low2 = int((uint(block[32u + q_base + l]) >> shift) & 0x03u); int high1 = int((uint(block[l]) >> hm_bit) & 0x01u); int q = low2 + high1 * 4 - 4;
    return antfly_qk_half_le_to_float(block + 108) * float(antfly_q3_k_raw_scale(block + 96, sub)) * float(q);
}
kernel void antfly_q3_k_small_batch_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q3_k [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]) {
    uint tid = thread_pos.x; int col = int(group_pos.x); int row = int(group_pos.y); if (row >= rows || rows < 2 || rows > 8 || col >= out_dim || (in_dim & 255) != 0) return; float acc = 0.0f; int block_count = in_dim >> 8;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) { device const uchar *block = weight_q3_k + ((col * block_count + block_idx) * 110); int base = block_idx << 8; for (int lane = int(tid); lane < 256; lane += 32) acc += input[row * in_dim + base + lane] * antfly_q3_k_dequant_lane_v2(block, lane); }
    acc = simd_sum(acc); if (tid == 0) output[row * out_dim + col] = acc;
}
