// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

// native-program:entry termite_encode_f16_convert
// native-program:indent 11
kernel void termite_encode_f16_convert(device const float *in [[buffer(0)]], device half *out [[buffer(1)]], constant termite_metal_encode_key_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    uint total = p.rows * p.num_kv_heads * p.head_dim; if (gid >= total) return; out[gid] = half(in[gid]);
}
// native-program:entry termite_conv1d_f32
// native-program:indent 11
kernel void termite_conv1d_f32(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_conv1d_f32_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
{{direct1d}}}
// native-program:entry termite_conv1d_im2col_f32
// native-program:indent 11
kernel void termite_conv1d_im2col_f32(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_conv1d_im2col_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    const uint cols = p.in_channels * p.kernel_size; const uint total = p.batch * p.out_time * cols; if (gid >= total || cols == 0u) return;
    const uint col = gid % cols; const uint row = gid / cols; const uint t = row % p.out_time; const uint b = row / p.out_time; const uint ic = col / p.kernel_size; const uint kk = col % p.kernel_size;
    const int in_t = int(t * p.stride + kk) - int(p.padding); float v = 0.0f;
    if (in_t >= 0 && uint(in_t) < p.time_steps) { v = p.time_major != 0u ? input[(b * p.time_steps + uint(in_t)) * p.in_channels + ic] : input[(b * p.in_channels + ic) * p.time_steps + uint(in_t)]; }
    output[gid] = v;
}
// native-program:entry termite_conv2d_f32
// native-program:indent 11
kernel void termite_conv2d_f32(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_conv2d_f32_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
{{direct2d}}}
// native-program:entry antfly_causal_history_f32
// native-program:indent 11
kernel void antfly_causal_history_f32(device const float *mixed [[buffer(0)]], device const float *history [[buffer(6)]], device float *next [[buffer(7)]], constant termite_qwen35_params &p [[buffer(15)]], uint gid [[thread_position_in_grid]]) {
    uint channels=(2*p.key_heads+p.value_heads)*p.dim;
    if(gid>=channels*p.kernel_size)return;
    uint c=gid/p.kernel_size,slot=gid%p.kernel_size;
    int src=int(p.seq)+int(slot)-int(p.kernel_size);
    next[gid]=src>=0?mixed[uint(src)*channels+c]:(p.initialized?history[c*p.kernel_size+uint(int(p.kernel_size)+src)]:0.0f);
}
