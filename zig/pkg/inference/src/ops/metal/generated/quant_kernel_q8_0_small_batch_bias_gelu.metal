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
// plan_id=metal/q8_0/rows_2_8/bias_gelu/small_batch
// kernel_id=antfly_q8_0_small_batch_bias_gelu_msl_v1
// production_baseline=metal_handwritten_quant_matmul
// production_enabled=false
// General MSL lowering smoke for descriptor-driven quant matmul epilogues.
// Production Metal dispatch stays on native handwritten MSL until this
// candidate clears correctness and benchmark gates.

#include <metal_stdlib>
using namespace metal;

inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }
inline float antfly_qk_gelu(float x) { if (!isfinite(x)) return 0.0f; float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x); if (inner > 10.0f) return x; if (inner < -10.0f) return 0.0f; float y = 0.5f * x * (1.0f + tanh(inner)); return isfinite(y) ? y : 0.0f; }
inline float antfly_q8_0_dequant_lane_v2(device const uchar *block, int lane) { float d = antfly_qk_half_le_to_float(block); int q = int(as_type<char>(block[2 + lane])); return d * float(q); }
kernel void antfly_q8_0_small_batch_bias_gelu_msl_v1(device const float *input [[buffer(0)]], device const uchar *weight_q8_0 [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant int &rows [[buffer(4)]], constant int &in_dim [[buffer(5)]], constant int &out_dim [[buffer(6)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]) {
    uint tid = thread_pos.x; int col0 = int(group_pos.x << 1); int col1 = col0 + 1; int row = int(group_pos.y); if (row >= rows || rows < 2 || rows > 8 || col0 >= out_dim || (in_dim & 31) != 0) return; float acc0 = 0.0f; float acc1 = 0.0f; int block_count = in_dim >> 5;
    device const float *row_input = input + row * in_dim; device const uchar *col0_weight = weight_q8_0 + col0 * block_count * 34; bool has_col1 = col1 < out_dim; device const uchar *col1_weight = has_col1 ? weight_q8_0 + col1 * block_count * 34 : col0_weight;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) { device const uchar *block0 = col0_weight + block_idx * 34; device const uchar *block1 = col1_weight + block_idx * 34; int base = block_idx << 5; for (int lane = int(tid); lane < 32; lane += 32) { float x = row_input[base + lane]; acc0 += x * antfly_q8_0_dequant_lane_v2(block0, lane); if (has_col1) acc1 += x * antfly_q8_0_dequant_lane_v2(block1, lane); } }
    acc0 = simd_sum(acc0); acc1 = simd_sum(acc1); if (tid == 0) { output[row * out_dim + col0] = antfly_qk_gelu(acc0 + bias[col0]); if (has_col1) output[row * out_dim + col1] = antfly_qk_gelu(acc1 + bias[col1]); }
}
