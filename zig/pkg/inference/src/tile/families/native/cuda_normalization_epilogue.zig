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

const program = @import("../../native_program.zig");
const tile = @import("antfly_tile");
const f = tile.fexpr;
const expr = tile.native_math.cudaExpression;
pub const source_0 = program.render(
    \\template<bool Weighted, bool Residual, bool Bf16, int ScaleStage>
    \\__device__ __forceinline__ void termite_rms_norm_impl(
    \\    float* dst, unsigned short* dst_bf16, const float* input,
    \\    const float* weight, const float* residual, const float* scalar,
    \\    unsigned int rows, unsigned int dim, float eps)
    \\{
    \\    unsigned int row = blockIdx.x;
    \\    if (row >= rows) return;
    \\    unsigned int tid = threadIdx.x;
    \\    const unsigned int base = row * dim;
    \\    __shared__ float partial[256];
    \\    const float scale = antfly_row_rms_tree_inverse(dim, eps, partial,
    \\        [&](unsigned i) { return input[base + i]; });
    \\    float output_scale = 1.0f;
    \\    if (ScaleStage != 0) output_scale = scalar[0];
    \\    for (unsigned int i = tid; i < dim; i += blockDim.x) {
    \\        unsigned int idx = base + i;
    \\        float value = {{scale}};
    \\        if (Weighted) value = {{weight}};
    \\        if (ScaleStage == 1) value = {{output_scale}};
    \\        if (Residual) value = {{residual}};
    \\        if (ScaleStage == 2) value = {{output_scale}};
    \\        dst[idx] = value;
    \\        if (Bf16) dst_bf16[idx] = termite_f32_to_bf16(value);
    \\    }
    \\}
    \\
, .{
    .scale = expr(f.acc.times(.gate), "input[idx]", "", "scale", ""),
    .weight = expr(f.acc.times(.bias), "value", "weight[i]", "", ""),
    .residual = expr(f.acc.plus(.residual), "value", "", "", "residual[idx]"),
    .output_scale = expr(f.acc.times(.gate), "value", "", "output_scale", ""),
});
