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
// plan_id=metal/q5_k/rows_2_8/none/small_batch
// kernel_id=antfly_q5_k_small_batch_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=true
// Promoted after the schedule sweep re-tuned this route to 256-thread
// hybrid-simd and the decode-runtime speedup gate cleared vs handwritten.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline void antfly_qk_unpack_scale_min_6bit(device const uchar *scales, int sub, thread float &scale, thread float &min_v) {
    if (sub < 4) { scale = float(scales[sub] & 63u); min_v = float(scales[sub + 4] & 63u); return; }
    scale = float((scales[sub + 4] & 0x0fu) | ((scales[sub - 4] >> 6) << 4)); min_v = float((scales[sub + 4] >> 4) | ((scales[sub] >> 6) << 4));
}
inline float antfly_q5_k_dequant_lane_v2(device const uchar *block, int lane) {
    device const uchar *scales = block + 4; device const uchar *qh = block + 16; device const uchar *ql = block + 48; int sub = lane >> 5; int i = lane & 31; int q_index = (sub >> 1) * 32 + i;
    uchar packed = ql[q_index]; int low = (sub & 1) == 0 ? int(packed & 0x0fu) : int(packed >> 4); int high = int((qh[i] >> sub) & 1u); int q = low + high * 16;
    float raw_scale = 0.0f; float raw_min = 0.0f; antfly_qk_unpack_scale_min_6bit(scales, sub, raw_scale, raw_min);
    return antfly_qk_half_le_to_float(block) * raw_scale * float(q) - antfly_qk_half_le_to_float(block + 2) * raw_min;
}
kernel void antfly_q5_k_small_batch_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q5_k [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    uint tid = thread_pos.x; int col = int(group_pos.x); int row = int(group_pos.y); if (row >= rows || rows < 2 || rows > 8 || col >= out_dim || (in_dim & 255) != 0) return; float acc = 0.0f; int block_count = in_dim >> 8;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) { device const uchar *block = weight_q5_k + ((col * block_count + block_idx) * 176); int base = block_idx << 8; for (int lane = int(tid); lane < 256; lane += 256) acc += input[row * in_dim + base + lane] * antfly_q5_k_dequant_lane_v2(block, lane); }
    threadgroup float partial[32]; acc = simd_sum(acc); if (lane_id == 0u) partial[simdgroup_id] = acc; if (simdgroup_id == 0u && lane_id >= 8u) partial[lane_id] = 0.0f; threadgroup_barrier(mem_flags::mem_threadgroup); float total = simd_sum(partial[lane_id]); if (lane_id == 0u && simdgroup_id == 0u) output[row * out_dim + col] = total;
}
