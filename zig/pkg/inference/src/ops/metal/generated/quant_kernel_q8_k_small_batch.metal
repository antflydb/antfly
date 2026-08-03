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

inline float antfly_qk_f32_le_to_float(device const uchar *p) { uint bits = uint(p[0]) | (uint(p[1]) << 8) | (uint(p[2]) << 16) | (uint(p[3]) << 24); return as_type<float>(bits); }
inline float antfly_q8_k_dequant_lane_v2(device const uchar *block, int lane) { float d = antfly_qk_f32_le_to_float(block); int q = int(as_type<char>(block[4 + lane])); return d * float(q); }
kernel void antfly_q8_k_small_batch_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q8_k [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]) {
    uint tid = thread_pos.x; int col = int(group_pos.x); int row = int(group_pos.y); if (row >= rows || rows < 2 || rows > 8 || col >= out_dim || (in_dim & 255) != 0) return; float acc = 0.0f; int block_count = in_dim >> 8;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) { device const uchar *block = weight_q8_k + ((col * block_count + block_idx) * 292); int base = block_idx << 8; for (int lane = int(tid); lane < 256; lane += 64) acc += input[row * in_dim + base + lane] * antfly_q8_k_dequant_lane_v2(block, lane); }
    threadgroup float partial[32]; acc = simd_sum(acc); if (lane_id == 0u) partial[simdgroup_id] = acc; if (simdgroup_id == 0u && lane_id >= 2u) partial[lane_id] = 0.0f; threadgroup_barrier(mem_flags::mem_threadgroup); float total = simd_sum(partial[lane_id]); if (lane_id == 0u && simdgroup_id == 0u) output[row * out_dim + col] = total;
}
