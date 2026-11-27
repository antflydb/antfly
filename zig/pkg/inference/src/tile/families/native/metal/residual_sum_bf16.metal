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

// native-program:entry antfly_residual_sum3_bf16_pointwise
struct ResidualSumBf16Params { uint values, previous; };

kernel void antfly_residual_sum3_bf16_pointwise(
    device const bfloat *attention [[buffer(0)]],
    device const bfloat *expert [[buffer(1)]],
    device const bfloat *previous [[buffer(2)]],
    device bfloat *output [[buffer(3)]],
    constant ResidualSumBf16Params &p [[buffer(4)]],
    uint index [[thread_position_in_grid]]
) {
    #pragma clang fp reassociate(off)
    if (index >= p.values) return;
    const float old = p.previous == 2u ? float(output[index])
        : p.previous == 1u ? float(previous[index]) : 0.0f;
    output[index] = bfloat((old + float(attention[index])) + float(expert[index]));
}
