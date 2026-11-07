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
    \\extern "C" __global__ void termite_linear_f32_qkv_nobias_tiled(ANTFLY_CUDA_LINEAR_F32_QKV_NOBIAS_TILED_PARAMS) {
    \\    typedef float Weight;
    \\#include "../kernels/dense_qkv_tiled.cuh"
    \\}
    \\
    \\extern "C" __global__ void termite_linear_bf16_weight_f32_qkv_nobias_tiled(ANTFLY_CUDA_LINEAR_BF16_WEIGHT_F32_QKV_NOBIAS_TILED_PARAMS) {
    \\    typedef unsigned short Weight;
    \\#include "../kernels/dense_qkv_tiled.cuh"
    \\}
    \\
;
