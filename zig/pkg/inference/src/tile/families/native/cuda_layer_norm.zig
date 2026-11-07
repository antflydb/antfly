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
    \\extern "C" __global__ void termite_layer_norm_f32(ANTFLY_CUDA_LAYER_NORM_PARAMS) {
    \\    termite_layer_norm_impl<false>(dst, input, nullptr, gamma, beta, rows, dim, eps);
    \\}
    \\
    \\extern "C" __global__ void termite_add_layer_norm_f32(ANTFLY_CUDA_ADD_LAYER_NORM_PARAMS) {
    \\    termite_layer_norm_impl<true>(dst, a, b, gamma, beta, rows, dim, eps);
    \\}
    \\
;

/// The existing shared row-statistics body retains its 256-lane moments
/// tree; this export changes only the residual load and two-output epilogue.
pub const source_1 =
    \\extern "C" __global__ void termite_add_layer_norm_sum_f32(
    \\    float* dst, float* sum_out, const float* a, const float* b,
    \\    const float* gamma, const float* beta, unsigned rows, unsigned dim, float eps) {
    \\    const unsigned row = blockIdx.x;
    \\    if (row >= rows) return;
    \\    const unsigned base = row * dim;
    \\    __shared__ float sums[256], sumsq[256];
    \\    antfly_row_layer_norm(dim, eps, sums, sumsq,
    \\        [&](unsigned i) { return a[base + i] + b[base + i]; },
    \\        [&](unsigned i, float x, float mean, float inv) {
    \\            sum_out[base + i] = x;
    \\            dst[base + i] = (x - mean) * inv * gamma[i] + beta[i];
    \\        });
    \\}
    \\
;
