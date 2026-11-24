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
    \\extern "C" __global__ void termite_laya_attention_warp_f32(
    \\    float* dst, const float* q, const float* k, const float* v,
    \\    const long long* mask, unsigned int batch, unsigned int seq,
    \\    unsigned int heads, unsigned int dim, unsigned int radius
    \\) {
    \\    __shared__ float scores[512];
    \\    termite_small_warp_attention_impl<512u, false>(dst, q, k, v, mask, batch, seq, heads, dim, radius, scores);
    \\}
    \\
;

pub const source_1 =
    \\extern "C" __global__ void termite_gliner_encoder_attention_f32(
    \\    float* dst, const float* q, const float* k, const float* v,
    \\    const long long* mask, unsigned int batch, unsigned int seq,
    \\    unsigned int heads, unsigned int dim, unsigned int radius
    \\) {
    \\    extern __shared__ float scores[];
    \\    termite_small_warp_attention_impl<8192u, true>(dst, q, k, v, mask, batch, seq, heads, dim, radius, scores);
    \\}
    \\
;
