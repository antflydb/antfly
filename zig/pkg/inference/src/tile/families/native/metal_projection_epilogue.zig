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
const f = @import("antfly_tile").fexpr;
pub const source_0 = program.render(
    \\// Exact gate_up publish phase; original arrays, nesting and barriers stay native.
    \\#define TERMITE_MSL_BF16_GATE_UP_PUBLISH \
    \\    "    TERMITE_FOR_UNROLL(uint r=0;r<2u;r++)TERMITE_FOR_UNROLL(uint c=0;c<2u;c++){\n" \
    \\    "        simdgroup_store(ag[r*2u+c],shared+(sr+r*8u)*32u+sc+c*8u,32u);\n" \
    \\    "        simdgroup_store(au[r*2u+c],shared+2048u+(sr+r*8u)*32u+sc+c*8u,32u);\n" \
    \\    "    }\n" \
    \\    "    threadgroup_barrier(mem_flags::mem_threadgroup);\n" \
    \\    "    for(uint i=uint(tid);i<2048u;i+=256u){\n" \
    \\    "        uint row=row0+i/32u,col=col0+i%32u;\n" \
    \\    "        if(row<p.rows&&col<p.out_dim){\n" \
    \\    "            float gate=shared[i]+bg[col],up=shared[2048u+i]+bu[col];\n" \
    \\    "            float y={{activation}};out[row*p.out_dim+col]={{multiply}};\n" \
    \\    "        }\n" \
    \\    "    }\n"
    \\
, .{
    .activation = f.Emission{ .expr = f.acc.siluOf(), .source = .{ .silu = "termite_silu", .expand_silu = false }, .inputs = .{ .value = "gate", .bias = "", .gate = "", .residual = "" } },
    .multiply = f.Emission{ .expr = f.acc.times(.gate), .source = .{ .compact = true }, .inputs = .{ .value = "y", .bias = "", .gate = "up", .residual = "" } },
});
