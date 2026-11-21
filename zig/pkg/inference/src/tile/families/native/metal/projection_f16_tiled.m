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

// Historical ABI exports remain consumed by the Metal runtime pipelines.
// native-program:entry termite_glide_linear_16_64
        "kernel void termite_glide_linear_16_64(device float *x [[buffer(0)]], device half *w [[buffer(1)]], device float *bias [[buffer(2)]], device float *y [[buffer(3)]], constant termite_glide_linear_params &p [[buffer(4)]], uint2 group [[threadgroup_position_in_grid]]) { termite_glide_linear_body<16, 64, 2>(x, w, bias, y, p, group); }\n"
// native-program:entry termite_glide_linear_32_64
        "kernel void termite_glide_linear_32_64(device float *x [[buffer(0)]], device half *w [[buffer(1)]], device float *bias [[buffer(2)]], device float *y [[buffer(3)]], constant termite_glide_linear_params &p [[buffer(4)]], uint2 group [[threadgroup_position_in_grid]]) { termite_glide_linear_body<32, 64, 4>(x, w, bias, y, p, group); }\n"
// native-program:entry termite_glide_geglu_16_64
        "kernel void termite_glide_geglu_16_64(device float *x [[buffer(0)]], device half *w [[buffer(1)]], device float *y [[buffer(3)]], constant termite_glide_linear_params &p [[buffer(4)]], uint2 group [[threadgroup_position_in_grid]]) { termite_glide_geglu_body<16, 64, 2>(x, w, y, p, group); }\n"
// native-program:entry termite_glide_geglu_32_64
        "kernel void termite_glide_geglu_32_64(device float *x [[buffer(0)]], device half *w [[buffer(1)]], device float *y [[buffer(3)]], constant termite_glide_linear_params &p [[buffer(4)]], uint2 group [[threadgroup_position_in_grid]]) { termite_glide_geglu_body<32, 64, 4>(x, w, y, p, group); }\n"
// native-program:entry termite_glide_cast_half
        "kernel void termite_glide_cast_half(device const float4 *x [[buffer(0)]], device half4 *y [[buffer(1)]], constant uint &count4 [[buffer(2)]], uint i [[thread_position_in_grid]]) {\n"
        "    if (i < count4) y[i] = half4(clamp(x[i], -65504.0f, 65504.0f));\n"
        "}\n"
// native-program:entry termite_glide_geglu_hh_16_64
        "kernel void termite_glide_geglu_hh_16_64(device half *x [[buffer(0)]], device half *w [[buffer(1)]], device half *y [[buffer(3)]], constant termite_glide_linear_params &p [[buffer(4)]], uint2 group [[threadgroup_position_in_grid]]) { termite_glide_geglu_hh_body<16, 64, 2>(x, w, y, p, group); }\n"
// native-program:entry termite_glide_geglu_hh_32_64
        "kernel void termite_glide_geglu_hh_32_64(device half *x [[buffer(0)]], device half *w [[buffer(1)]], device half *y [[buffer(3)]], constant termite_glide_linear_params &p [[buffer(4)]], uint2 group [[threadgroup_position_in_grid]]) { termite_glide_geglu_hh_body<32, 64, 4>(x, w, y, p, group); }\n"
// native-program:entry termite_glide_down_res_16_64
        "kernel void termite_glide_down_res_16_64(device half *x [[buffer(0)]], device half *w [[buffer(1)]], device float *bias [[buffer(2)]], device float *y [[buffer(3)]], constant termite_glide_linear_params &p [[buffer(4)]], device const float *res [[buffer(5)]], uint2 group [[threadgroup_position_in_grid]]) { termite_glide_down_res_body<16, 64, 2>(x, w, bias, y, res, p, group); }\n"
// native-program:entry termite_glide_down_res_32_64
        "kernel void termite_glide_down_res_32_64(device half *x [[buffer(0)]], device half *w [[buffer(1)]], device float *bias [[buffer(2)]], device float *y [[buffer(3)]], constant termite_glide_linear_params &p [[buffer(4)]], device const float *res [[buffer(5)]], uint2 group [[threadgroup_position_in_grid]]) { termite_glide_down_res_body<32, 64, 4>(x, w, bias, y, res, p, group); }\n"
