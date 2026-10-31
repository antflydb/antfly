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

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           "kernel void termite_apply_linear_1x(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_linear_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    if (gid >= p.out_dim) return; termite_dense_scalar(input, weight, bias, output, p.in_dim, p.out_dim, 0u, gid);\n"
    \\           "}\n"
    \\           "kernel void termite_apply_linear_1x_reduce(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *bias [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_linear_params &p [[buffer(4)]], uint lane [[thread_index_in_threadgroup]], uint group [[threadgroup_position_in_grid]]) {\n"
    \\           "    if (group >= p.out_dim) return; threadgroup float partials[32]; termite_dense_reduce(input, weight, bias, output, partials, p.in_dim, p.out_dim, 0u, group, lane);\n"
    \\           "}\n"
    \\
;
