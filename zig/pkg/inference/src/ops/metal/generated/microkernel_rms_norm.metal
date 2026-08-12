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

// Generated Metal microkernel artifact from graph/quant_kernel_compiler.zig.
// plan_id=metal/microkernel/rms_norm
// kernel_id=antfly_rms_norm_generated_msl_v1
// production_baseline=termite_apply_rms_norm_rows
// production_enabled=false
// Descriptor-driven RMSNorm microkernel (first non-matmul route).
// Production RMSNorm stays on the hand-written termite_apply_rms_norm_rows
// until this candidate clears its on-device conformance gate.

#include <metal_stdlib>
using namespace metal;

kernel void antfly_rms_norm_generated_msl_v1(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &hidden_size [[buffer(4)]], constant float &eps [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]) {
    uint tid = thread_pos.x; int row = int(group_pos.x); if (row >= rows || hidden_size < 1) return; device const float *row_input = input + row * hidden_size; float sq = 0.0f;
    for (int i = int(tid); i < hidden_size; i += 256) { float x = row_input[i]; sq += x * x; }
    threadgroup float partial[256]; partial[tid] = sq; threadgroup_barrier(mem_flags::mem_threadgroup); for (uint stride = 128u; stride > 0u; stride >>= 1) { if (tid < stride) partial[tid] += partial[tid + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); }
    float inv_rms = rsqrt(partial[0] / float(hidden_size) + eps);
    for (int i = int(tid); i < hidden_size; i += 256) output[row * hidden_size + i] = row_input[i] * inv_rms * weight[i];
}
