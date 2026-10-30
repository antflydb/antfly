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

//! Native stack/heap activation lifetimes have one source owner. Expansion
//! preserves each function's lexical stack frame, allocator, defer and phase
//! boundaries; dispatch admission and compute policy stay with the caller.
const std = @import("std");
const cpu = @import("quant_kernel_cpu_source.zig");
const Pipeline = struct { name: []const u8, allocator: []const u8, allocation_len: []const u8, indent: usize, else_branch: bool, quantified: bool, compute: []const u8 };
pub const owners = .{ Owner(0), Owner(1), Owner(2), Owner(3), Owner(4), Owner(5), Owner(6), Owner(7) };
fn Owner(comptime index: usize) type {
    return struct {
        const pipeline = pipelines[index];
        const spaces: [pipeline.indent]u8 = @splat(' ');
        pub const begin = spaces ++ "// quant-kernel-codegen:begin CPU activation " ++ pipeline.name;
        pub const end = spaces ++ "// quant-kernel-codegen:end CPU activation " ++ pipeline.name;
        pub fn render(allocator: std.mem.Allocator) ![]u8 {
            var out = cpu.Emitter{ .allocator = allocator };
            defer out.bytes.deinit(allocator);
            try out.raw("if (q_input_len <= q8_k_activation_stack_max_bytes) {\n");
            for ([_]bool{ true, false }) |stack| {
                if (stack) {
                    try out.raw("var q_input_buf: [q8_k_activation_stack_max_bytes]u8 = undefined;\nconst q_input = q_input_buf[0..q_input_len];\n");
                } else {
                    try out.raw(if (pipeline.else_branch) "} else {\n" else "}\n");
                    if (pipeline.quantified) try out.raw("const alloc_start = nativeQuantPhaseStart();\n");
                    try out.add("const q_input = try {s}.alloc(u8, {s});\ndefer {s}.free(q_input);\n", .{ pipeline.allocator, pipeline.allocation_len, pipeline.allocator });
                    if (pipeline.quantified) try out.raw("noteNativeQuantPhase(.q8k_activation_alloc, alloc_start);\n");
                }
                if (pipeline.quantified) try out.raw("const quant_start = nativeQuantPhaseStart();\n");
                if (stack and !pipeline.else_branch) try out.raw("if (rows == 1) {\nquantizeQ8_KActivationRows(input, q_input, rows, in_dim, row_blocks);\n} else {\n");
                try out.raw("try quantizeQ8_KActivationRowsDispatch(io, input, q_input, rows, in_dim, row_blocks);\n");
                if (stack and !pipeline.else_branch) try out.raw("}\n");
                if (pipeline.quantified) try out.raw("noteNativeQuantPhase(.q8k_activation_quant, quant_start);\n");
                try out.raw(pipeline.compute);
                if (stack and !pipeline.else_branch) try out.raw("return;\n");
            }
            if (pipeline.else_branch) try out.raw("}\n");
            return cpu.formatStatements(allocator, out.bytes.items, pipeline.indent);
        }
    };
}
const pipelines = [_]Pipeline{
    .{ .name = "linearNoBiasQuantizedPair pipeline 1", .allocator = "allocator", .allocation_len = "q_input_len", .else_branch = true, .quantified = true, .indent = 16, .compute =
    \\const compute_start = nativeQuantPhaseStart();
    \\try linearKPreparedQ8KActivationPairDispatch(false, io, q_input, storage_a.preparedBytes(.row_major_blocks).?, storage_a.preparedBytes(.panel4), storage_a.preparedBytes(.panel8), storage_a.preparedBytes(.panel16), output_a, storage_b.preparedBytes(.row_major_blocks).?, storage_b.preparedBytes(.panel4), storage_b.preparedBytes(.panel8), storage_b.preparedBytes(.panel16), output_b, rows, in_dim, out_dim, known);
    \\noteNativeQuantPhase(.q4q5_q8k_pair_compute, compute_start);
    },
    .{ .name = "linearNoBiasQuantizedPair pipeline 2", .allocator = "allocator", .allocation_len = "q_input_len", .else_branch = true, .quantified = true, .indent = 16, .compute =
    \\try linearKPreparedQ8KActivationPairDispatch(true, io, q_input, storage_a.preparedBytes(.row_major_blocks).?, storage_a.preparedBytes(.panel4), storage_a.preparedBytes(.panel8), panel16_a, output_a, storage_b.preparedBytes(.row_major_blocks).?, storage_b.preparedBytes(.panel4), storage_b.preparedBytes(.panel8), panel16_b, output_b, rows, in_dim, out_dim, null);
    },
    .{ .name = "linearQuantizedTriple pipeline 1", .allocator = "storage_a.allocator", .allocation_len = "q_input_len", .else_branch = true, .quantified = true, .indent = 16, .compute =
    \\const compute_start = nativeQuantPhaseStart();
    \\const packed_qkv_panel16_mr2 = use_packed_qkv_panel16;
    \\const fuse_triple_bias = q4q5FusedTripleBiasEnabled() and packed_qkv_blocks != null;
    \\try linearKPreparedQ8KActivationTripleDispatch(false, io, q_input, storage_a.preparedBytes(.row_major_blocks).?, storage_a.preparedBytes(.panel4), storage_a.preparedBytes(.panel8), storage_a.preparedBytes(.panel16), if (fuse_triple_bias) bias_a else null, output_a, storage_b.preparedBytes(.row_major_blocks).?, storage_b.preparedBytes(.panel4), storage_b.preparedBytes(.panel8), storage_b.preparedBytes(.panel16), if (fuse_triple_bias) bias_b else null, output_b, storage_c.preparedBytes(.row_major_blocks).?, storage_c.preparedBytes(.panel4), storage_c.preparedBytes(.panel8), storage_c.preparedBytes(.panel16), packed_qkv_blocks, packed_qkv_panel16_mr2, if (fuse_triple_bias) bias_c else null, output_c, rows, in_dim, out_dim, known);
    \\noteNativeQuantPhase(.q4q5_q8k_triple_compute, compute_start);
    \\if (!fuse_triple_bias) {
    \\    if (bias_a) |bias| addLinearBiasRows(output_a, bias, rows, out_dim);
    \\    if (bias_b) |bias| addLinearBiasRows(output_b, bias, rows, out_dim);
    \\    if (bias_c) |bias| addLinearBiasRows(output_c, bias, rows, out_dim);
    \\}
    },
    .{ .name = "linearQuantizedTriple pipeline 2", .allocator = "storage_a.allocator", .allocation_len = "q_input_len", .else_branch = true, .quantified = true, .indent = 16, .compute =
    \\try linearKPreparedQ8KActivationTripleDispatch(true, io, q_input, storage_a.preparedBytes(.row_major_blocks).?, storage_a.preparedBytes(.panel4), storage_a.preparedBytes(.panel8), panel16_a, null, output_a, storage_b.preparedBytes(.row_major_blocks).?, storage_b.preparedBytes(.panel4), storage_b.preparedBytes(.panel8), panel16_b, null, output_b, storage_c.preparedBytes(.row_major_blocks).?, storage_c.preparedBytes(.panel4), storage_c.preparedBytes(.panel8), panel16_c, null, false, null, output_c, rows, in_dim, out_dim, null);
    },
    .{ .name = "linearNoBiasQ8KQ8KActivationPair pipeline 1", .allocator = "allocator", .allocation_len = "q_input_len", .else_branch = false, .quantified = false, .indent = 4, .compute =
    \\try linearQ8KQ8KActivationPairDispatch(io, q_input, weight_raw_a, panel_blocks_a, panel8_blocks_a, panel16_blocks_a, output_a, weight_raw_b, panel_blocks_b, panel8_blocks_b, panel16_blocks_b, output_b, rows, in_dim, out_dim);
    },
    .{ .name = "linearQ8KQ8KActivationTriple pipeline 1", .allocator = "allocator", .allocation_len = "q_input_len", .else_branch = false, .quantified = false, .indent = 4, .compute =
    \\try linearQ8KQ8KActivationTripleDispatch(io, q_input, weight_raw_a, panel_blocks_a, panel8_blocks_a, panel16_blocks_a, output_a, weight_raw_b, panel_blocks_b, panel8_blocks_b, panel16_blocks_b, output_b, weight_raw_c, panel_blocks_c, panel8_blocks_c, panel16_blocks_c, bias_a, bias_b, bias_c, output_c, rows, in_dim, out_dim);
    },
    .{ .name = "linearNoBiasQ6KPreparedQ8KActivation pipeline 1", .allocator = "allocator", .allocation_len = "rows * row_blocks * q8_block_size", .else_branch = false, .quantified = true, .indent = 4, .compute =
    \\try linearQ6KPreparedQ8KActivationDispatch(io, q_input, prepared_blocks, panel_blocks, panel8_blocks, panel16_blocks, output, rows, in_dim, out_dim);
    },
    .{ .name = "linearNoBiasQ4Q5KPreparedQ8KActivation pipeline 1", .allocator = "allocator", .allocation_len = "rows * row_blocks * q8_block_size", .else_branch = false, .quantified = true, .indent = 4, .compute =
    \\const compute_start = nativeQuantPhaseStart();
    \\try linearQ4Q5KPreparedQ8KActivationDispatch(io, q_input, prepared_blocks, panel_blocks, panel8_blocks, panel16_blocks, bias, output, rows, in_dim, out_dim, known);
    \\noteNativeQuantPhase(.q4q5_q8k_compute, compute_start);
    },
};

test "activation pipelines retain allocation scope and exactly one free" {
    inline for (owners) |O| {
        const source = try O.render(std.testing.allocator);
        defer std.testing.allocator.free(source);
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "defer "));
        try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, source, "try quantizeQ8_KActivationRowsDispatch("));
    }
}
