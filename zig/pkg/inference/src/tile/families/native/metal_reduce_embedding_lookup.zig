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

const program = @import("../../native_program.zig");

pub const source_0 =
    program.metal(
        \\kernel void termite_embedding_lookup(device const float *weight [[buffer(0)]], device const uint *ids [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_embedding_lookup_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
        \\    uint total = p.total * p.dim;
        \\    if (gid >= total) return;
        \\    uint row = gid / p.dim;
        \\    uint col = gid - row * p.dim;
        \\    uint idx = ids[row];
        \\    output[gid] = weight[idx * p.dim + col];
        \\}
        \\
    );

pub const source_1 =
    program.join(&.{
        \\           TERMITE_MSL_ABI_DENSE_EMBEDDING_16(termite_embedding_lookup_bf16) "\n"
        \\
        ,
        program.metal(
            \\    uint total = p.total * p.dim;
            \\    if (gid >= total) return;
            \\    uint row = gid / p.dim;
            \\    uint col = gid - row * p.dim;
            \\    uint idx = ids[row];
            \\    output[gid] = termite_embedding_bf16_to_f32(weight[idx * p.dim + col]);
            \\}
            \\
        ),

        \\           TERMITE_MSL_ABI_DENSE_EMBEDDING_16(termite_embedding_lookup_f16) "\n"
        \\
        ,
        program.metal(
            \\    uint total = p.total * p.dim;
            \\    if (gid >= total) return;
            \\    uint row = gid / p.dim;
            \\    uint col = gid - row * p.dim;
            \\    uint idx = ids[row];
            \\    output[gid] = float(as_type<half>(weight[idx * p.dim + col]));
            \\}
            \\
        ),
    });
