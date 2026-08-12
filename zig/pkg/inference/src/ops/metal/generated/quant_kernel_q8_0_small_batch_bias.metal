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
// plan_id=metal/q8_0/rows_2_8/bias/small_batch
// kernel_id=antfly_q8_0_small_batch_bias_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=false
// Validated in the direct runtime harness; normal model execution does not
// call the fused-bias API, so production stays on the no-bias route plus bias op.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline float antfly_q8_0_dequant_lane_v2(device const uchar *block, int lane) { float d = antfly_qk_half_le_to_float(block); int q = int(as_type<char>(block[2 + lane])); return d * float(q); }
kernel void antfly_q8_0_small_batch_bias_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q8_0 [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant int &rows [[buffer(4)]], constant int &in_dim [[buffer(5)]], constant int &out_dim [[buffer(6)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]) {
    uint tid = thread_pos.x; int col = int(group_pos.x); int row = int(group_pos.y); if (row >= rows || rows < 2 || rows > 8 || col >= out_dim || (in_dim & 31) != 0) return; float acc = 0.0f; int block_count = in_dim >> 5;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) { device const uchar *block = weight_q8_0 + ((col * block_count + block_idx) * 34); int base = block_idx << 5; for (int lane = int(tid); lane < 32; lane += 32) acc += input[row * in_dim + base + lane] * antfly_q8_0_dequant_lane_v2(block, lane); }
    acc = simd_sum(acc); if (tid == 0) output[row * out_dim + col] = acc + bias[col];
}
