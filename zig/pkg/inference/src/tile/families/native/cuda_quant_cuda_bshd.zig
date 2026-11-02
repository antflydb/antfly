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
    \\extern "C" __global__ void termite_embedding_gemma2_pack_bshd_bf16_f32(
    \\    float* out, const __nv_bfloat16* input, unsigned int batch,
    \\    unsigned int sequence, unsigned int heads, unsigned int dim) {
    \\    antfly_reorder_heads<float,__nv_bfloat16,false>(out, input, batch, sequence, heads, dim);
    \\}
    \\
    \\extern "C" __global__ void termite_embedding_gemma2_pack_bshd_f32(
    \\    float* out, const float* input, unsigned int batch,
    \\    unsigned int sequence, unsigned int heads, unsigned int dim) {
    \\    antfly_reorder_heads<float,float,false>(out, input, batch, sequence, heads, dim);
    \\}
    \\
    \\extern "C" __global__ void termite_embedding_gemma2_unpack_bhsd_f32(
    \\    float* out, const float* input, unsigned int batch,
    \\    unsigned int sequence, unsigned int heads, unsigned int dim) {
    \\    antfly_reorder_heads<float,float,true>(out, input, batch, sequence, heads, dim);
    \\}
    \\
    \\extern "C" __global__ void termite_embedding_gemma2_unpack_bhsd_f32_bf16(
    \\    __nv_bfloat16* out, const float* input, unsigned int batch,
    \\    unsigned int sequence, unsigned int heads, unsigned int dim) {
    \\    antfly_reorder_heads<__nv_bfloat16,float,true>(out, input, batch, sequence, heads, dim);
    \\}
    \\
;
