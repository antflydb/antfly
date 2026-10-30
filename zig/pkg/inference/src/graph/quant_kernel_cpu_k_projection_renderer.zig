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

//! Q4/Q5 projection schedule lowering. Share row geometry, activation caches,
//! panel acquisition and native call arguments while retaining the qualified
//! scalar/vector registers, prepared-sum branches and static block schedules.
const std = @import("std");
const native = @import("quant_kernel_cpu_source.zig");
const Emitter = native.Emitter;
const Rows = native.Rows;

pub const owners = .{ DirectPanelRows, TriplePanelRows, FallbackPanelRows };
pub const DirectPanelRows = struct {
    pub const begin = "            // quant-kernel-codegen:begin CPU Q4 Q5 direct panel16 rows";
    pub const end = "            // quant-kernel-codegen:end CPU Q4 Q5 direct panel16 rows";
    pub fn render(allocator: std.mem.Allocator) ![]u8 {
        var out = Emitter{ .allocator = allocator };
        defer out.bytes.deinit(allocator);
        for ([_]usize{ 4, 2 }) |mr| {
            try out.add("while (r + {d} <= rows) : (r += {d}) {{\n", .{ mr, mr });
            try inputRows(&out, mr);
            try outputRows(&out, mr, null);
            try activationCache(&out, mr, "use_prepared_sums", "row_blocks <= q8_k_prepared_sum_stack_max_row_blocks", mr == 4);
            try out.raw("var o: usize = out_start;\nwhile (o < out_end and (o % prepared_k_panel16_nr) != 0) : (o += 1) {\n");
            try scalarColumn(&out, mr, true);
            try out.raw("}\n");
            try panel16Column(&out, mr);
            try out.raw("while (o < out_end) : (o += 1) {\n");
            try scalarColumn(&out, mr, true);
            try out.raw("}\n}\n");
        }
        return native.formatStatements(allocator, out.bytes.items, 12);
    }
};
const suffix = native.rowSuffix;

fn inputRows(out: *Emitter, mr: usize) !void {
    var rows: Rows = .{};
    for (0..mr) |r| try out.add("const {s} = q_input[{s} * input_row_stride ..][0..input_row_stride];\n", .{ rows.row(mr, r), rows.position(r) });
}
fn outputRows(out: *Emitter, mr: usize, projection: ?u8) !void {
    var rows: Rows = .{};
    for (0..mr) |r| {
        if (projection) |p| try out.add("const out_{c}{s} = output_{c}[{s} * out_dim ..][0..out_dim];\n", .{ p, suffix(mr, r), p, rows.position(r) }) else try out.add("const out_row{s} = output[{s} * out_dim ..][0..out_dim];\n", .{ suffix(mr, r), rows.position(r) });
    }
}
fn activationCache(out: *Emitter, mr: usize, enabled: []const u8, expression: []const u8, scales: bool) !void {
    try out.add("const {s} = {s};\n", .{ enabled, expression });
    for (0..mr) |r| try out.add("var activation_sums{s}: [q8_k_prepared_sum_stack_max_row_blocks][8]F32x8 = undefined;\n", .{suffix(mr, r)});
    if (scales) for (0..mr) |r| try out.add("var activation_scales{s}: [q8_k_prepared_sum_stack_max_row_blocks]F32x8 = undefined;\n", .{suffix(mr, r)});
    try out.add("if ({s}) {{\nfor (0..row_blocks) |block_idx| {{\nconst q8_off = block_idx * q8_block_size;\n", .{enabled});
    if (scales) for (0..mr) |r| try out.add("const block{s} = q_input_row{s}[q8_off..][0..q8_block_size];\n", .{ suffix(mr, r), suffix(mr, r) });
    for (0..mr) |r| {
        if (scales) try out.add("prepareQ8KActivationSums(block{s}, &activation_sums{s}[block_idx]);\n", .{ suffix(mr, r), suffix(mr, r) }) else try out.add("prepareQ8KActivationSums(q_input_row{s}[q8_off..][0..q8_block_size], &activation_sums{s}[block_idx]);\n", .{ suffix(mr, r), suffix(mr, r) });
    }
    if (scales) for (0..mr) |r| try out.add("activation_scales{s}[block_idx] = @splat(readF32Le(block{s}[0..4]));\n", .{ suffix(mr, r), suffix(mr, r) });
    try out.raw("}\n}\n");
}
fn scalarColumn(out: *Emitter, mr: usize, direct: bool) !void {
    try out.raw("const prepared_row = prepared_blocks[o * prepared_row_stride ..][0..prepared_row_stride];\n");
    for (0..mr) |r| try out.add("var acc{s}: f32 = 0.0;\n", .{suffix(mr, r)});
    try out.raw("for (0..row_blocks) |block_idx| {\n");
    if (mr != 1) try out.raw("const q8_off = block_idx * q8_block_size;\nconst prepared_off = block_idx * prepared_q4_k_block_bytes;\n");
    if (direct) try out.raw("const prepared_block = prepared_row[prepared_off..][0..prepared_q4_k_block_bytes];\n");
    for (0..mr) |r| {
        if (direct) try out.add("acc{d} += dotQ4Q5KPreparedQ8KBlock(q_input_row{d}[q8_off..][0..q8_block_size], prepared_block);\n", .{ r, r }) else if (mr == 1) try out.raw("acc += dotQ4Q5KPreparedQ8KBlock(\nq_input_row[block_idx * q8_block_size ..][0..q8_block_size],\nprepared_row[block_idx * prepared_q4_k_block_bytes ..][0..prepared_q4_k_block_bytes],\n);\n") else try out.add("acc{d} += dotQ4Q5KPreparedQ8KBlock(\nq_input_row{d}[q8_off..][0..q8_block_size],\nprepared_row[prepared_off..][0..prepared_q4_k_block_bytes],\n);\n", .{ r, r });
    }
    try out.raw("}\n");
    if (mr != 1) try out.raw("const bias_value = optionalLinearBiasAt(bias, o);\n");
    for (0..mr) |r| try out.add("out_row{s}[o] = acc{s} + {s};\n", .{ suffix(mr, r), suffix(mr, r), if (mr == 1) "optionalLinearBiasAt(bias, o)" else "bias_value" });
}
fn panel16Accumulators(out: *Emitter, mr: usize) !void {
    for (0..mr) |r| for ([_][]const u8{ "lo", "hi" }) |half| try out.add("var acc{s}_{s}: F32x8 = @splat(0.0);\n", .{ suffix(mr, r), half });
}
fn panel16BlockInputs(out: *Emitter, mr: usize, static_blocks: bool) !void {
    try out.raw("const q8_off = block_idx * q8_block_size;\n");
    if (static_blocks) try out.raw("const panel_off = panel_base + block_idx * prepared_k_panel16_block_bytes;\n") else try out.raw("const panel_off = ((o / prepared_k_panel16_nr) * row_blocks + block_idx) * prepared_k_panel16_block_bytes;\n");
    for (0..mr) |r| try out.add("const block{s} = q_input_row{s}[q8_off..][0..q8_block_size];\n", .{ suffix(mr, r), suffix(mr, r) });
    try out.raw("const panel_block = panel16[panel_off..][0..prepared_k_panel16_block_bytes];\n");
}
fn panel16Accumulate(out: *Emitter, mr: usize, prepared: bool) !void {
    try out.add("accumulateQ4Q5KPreparedQ8KPanel16MR{d}Block{s}(", .{ mr, if (prepared) "PreparedSums" else "" });
    if (prepared) try out.raw("\n");
    for (0..mr) |r| try out.add("block{s},{s}", .{ suffix(mr, r), if (prepared) "\n" else " " });
    if (prepared) {
        for (0..mr) |r| try out.add("&activation_sums{s}[block_idx],\n", .{suffix(mr, r)});
        if (mr == 4) for (0..mr) |r| try out.add("activation_scales{d}[block_idx],\n", .{r});
    }
    try out.add("panel_block,{s}", .{if (prepared) "\n" else " "});
    for (0..mr) |r| for ([_][]const u8{ "lo", "hi" }, 0..) |half, h| {
        const last = r + 1 == mr and h == 1;
        try out.add("&acc{s}_{s}{s}", .{ suffix(mr, r), half, if (prepared) ",\n" else if (last) "" else ", " });
    };
    try out.raw(");\n");
}
fn panel16Column(out: *Emitter, mr: usize) !void {
    if (mr == 1) try out.raw("while ((o % prepared_k_panel16_nr) == 0 and o + prepared_k_panel16_nr <= out_end) : (o += prepared_k_panel16_nr) {\n") else try out.raw("while (o + prepared_k_panel16_nr <= out_end) : (o += prepared_k_panel16_nr) {\n");
    try panel16Accumulators(out, mr);
    if (mr == 4) {
        try out.raw("if (use_prepared_sums and (row_blocks == 3 or row_blocks == 8 or row_blocks == 12)) {\ninline for (.{ 3, 8, 12 }) |block_count| {\nif (row_blocks == block_count) {\nconst panel_base = (o / prepared_k_panel16_nr) * block_count * prepared_k_panel16_block_bytes;\ninline for (0..block_count) |block_idx| {\n");
        try panel16BlockInputs(out, mr, true);
        try panel16Accumulate(out, mr, true);
        try out.raw("}\n}\n}\n} else {\n");
    }
    try out.raw("for (0..row_blocks) |block_idx| {\n");
    try panel16BlockInputs(out, mr, false);
    try out.raw("if (use_prepared_sums) {\n");
    try panel16Accumulate(out, mr, true);
    try out.raw("} else {\n");
    try panel16Accumulate(out, mr, false);
    try out.raw("}\n}\n");
    if (mr == 4) try out.raw("}\n");
    for (0..mr) |r| try out.add("storeF32x16WithBias(out_row{s}, bias, o, acc{s}_lo, acc{s}_hi);\n", .{ suffix(mr, r), suffix(mr, r), suffix(mr, r) });
    try out.raw("}\n");
}

test "native K projection regions parse and release all geometry allocations" {
    inline for (owners) |Owner| {
        const source = try Owner.render(std.testing.allocator);
        defer std.testing.allocator.free(source);
        try std.testing.expect(source.len != 0);
    }
}

pub const TriplePanelRows = struct {
    pub const begin = "    // quant-kernel-codegen:begin CPU Q4 Q5 triple panel16 rows";
    pub const end = "    // quant-kernel-codegen:end CPU Q4 Q5 triple panel16 rows";
    pub fn render(allocator: std.mem.Allocator) ![]u8 {
        var out = Emitter{ .allocator = allocator };
        defer out.bytes.deinit(allocator);
        for ([_]usize{ 4, 2, 1 }) |mr| {
            if (mr == 1) try out.raw("while (r < rows) : (r += 1) {\n") else try out.add("while (r + {d} <= rows) : (r += {d}) {{\n", .{ mr, mr });
            try inputRows(&out, mr);
            for ("abc") |p| try outputRows(&out, mr, p);
            const enabled = if (mr == 4) "row_blocks <= q8_k_prepared_sum_stack_max_row_blocks and (!triple_panel16_dot or packed_qkv_panel16 != null)" else if (mr == 2) "!triple_panel16_dot and row_blocks <= q8_k_prepared_sum_stack_max_row_blocks" else "row_blocks <= q8_k_prepared_sum_stack_max_row_blocks and packed_qkv_panel16 != null";
            try activationCache(&out, mr, "use_projection_prepared_sums", enabled, mr != 2);
            try out.raw("var o: usize = out_start;\nwhile (o < out_end and (o % prepared_k_panel16_nr) != 0) : (o += 1) {\n");
            try tripleScalarColumn(&out, mr);
            try out.raw("}\n");
            try triplePanel16Column(&out, mr);
            try out.raw("while (o < out_end) : (o += 1) {\n");
            try tripleScalarColumn(&out, mr);
            try out.raw("}\n}\n");
        }
        return native.formatStatements(allocator, out.bytes.items, 4);
    }
};
fn tripleScalarColumn(out: *Emitter, mr: usize) !void {
    for ("abc") |p| try out.add("const prepared_row_{c} = prepared_blocks_{c}[o * prepared_row_stride ..][0..prepared_row_stride];\n", .{ p, p });
    if (mr != 1) for ("abc") |p| try out.add("const bias_{c}_value = optionalLinearBiasAt(bias_{c}, o);\n", .{ p, p });
    for ("abc") |p| for (0..mr) |r| {
        try out.add("out_{c}{s}[o] = dotQ4Q5KPreparedQ8KPreparedRow(q_input_row{s}, prepared_row_{c}, row_blocks) + ", .{ p, suffix(mr, r), suffix(mr, r), p });
        if (mr == 1) try out.add("optionalLinearBiasAt(bias_{c}, o);\n", .{p}) else try out.add("bias_{c}_value;\n", .{p});
    };
}
const Call = struct {
    out: *Emitter,
    first: bool = true,
    fn arg(self: *Call, comptime fmt: []const u8, args: anytype) !void {
        if (!self.first) try self.out.raw(", ");
        self.first = false;
        try self.out.add(fmt, args);
    }
    fn finish(self: *Call) !void {
        try self.out.raw(");\n");
    }
};
fn projectionInputs(call: *Call, mr: usize, cached: ?bool) !void {
    for (0..mr) |r| try call.arg("q_input_row{s}", .{suffix(mr, r)});
    if (cached) |prepared| {
        for (0..mr) |r| {
            if (prepared) try call.arg("&activation_sums{s}", .{suffix(mr, r)}) else try call.arg("undefined", .{});
        }
        if (mr != 2) for (0..mr) |r| {
            if (prepared) try call.arg("&activation_scales{s}", .{suffix(mr, r)}) else try call.arg("undefined", .{});
        };
    }
}
fn projectionOutputs(call: *Call, mr: usize, projection: u8) !void {
    for (0..mr) |r| try call.arg("out_{c}{s}", .{ projection, suffix(mr, r) });
}
fn projectionFinish(call: *Call) !void {
    try call.arg("row_blocks", .{});
    try call.arg("o", .{});
    try call.finish();
}
fn tripleBias(out: *Emitter, mr: usize) !void {
    for ("abc") |p| {
        try out.raw("addBiasTile16");
        if (mr != 1) try out.add("Rows{d}", .{mr});
        try out.raw("(");
        var call = Call{ .out = out };
        try projectionOutputs(&call, mr, p);
        try call.arg("bias_{c}", .{p});
        try call.arg("o", .{});
        try call.finish();
    }
}
fn cachedProjection(out: *Emitter, mr: usize, prepared: bool, packed_projection: bool) !void {
    if (packed_projection) {
        try out.add("writeQ4Q5KPreparedQ8KPackedQKVPanel16MR{d}ProjectionWithSums(", .{mr});
        var call = Call{ .out = out };
        try call.arg("{s}", .{if (prepared) "true" else "false"});
        try projectionInputs(&call, mr, prepared);
        try call.arg("{s}", .{if (mr == 1) "packed_panel" else "packed_qkv_panel16.?"});
        for ("abc") |p| {
            if (mr == 4) try call.arg("bias_{c}", .{p});
            try projectionOutputs(&call, mr, p);
        }
        try projectionFinish(&call);
    } else {
        for ("abc") |p| {
            try out.add("writeQ4Q5KPreparedQ8KPanel16MR{d}ProjectionWithSums(", .{mr});
            var call = Call{ .out = out };
            try call.arg("{s}", .{if (prepared) "true" else "false"});
            try projectionInputs(&call, mr, prepared);
            try call.arg("panel16_blocks_{c}", .{p});
            try projectionOutputs(&call, mr, p);
            try projectionFinish(&call);
        }
    }
}
fn tripleDotProjection(out: *Emitter, mr: usize) !void {
    try out.add("writeQ4Q5KPreparedQ8KPanel16TripleMR{d}Projection(", .{mr});
    var call = Call{ .out = out };
    try projectionInputs(&call, mr, null);
    for ("abc") |p| try call.arg("panel16_blocks_{c}", .{p});
    try call.arg("{s}", .{if (mr == 2) "if (packed_qkv_panel16_mr2) packed_qkv_panel16 else null" else "packed_qkv_panel16"});
    for ("abc") |p| try projectionOutputs(&call, mr, p);
    try projectionFinish(&call);
}
fn triplePanel16Column(out: *Emitter, mr: usize) !void {
    if (mr == 1) try out.raw("if (packed_qkv_panel16) |packed_panel| {\n");
    try out.raw("while (o + prepared_k_panel16_nr <= out_end) : (o += prepared_k_panel16_nr) {\n");
    if (mr == 1) {
        try out.raw("if (use_projection_prepared_sums) {\n");
        try cachedProjection(out, mr, true, true);
        try out.raw("} else {\n");
        try cachedProjection(out, mr, false, true);
        try out.raw("}\n");
        try tripleBias(out, mr);
    } else {
        try out.raw("if (triple_panel16_dot) {\n");
        if (mr == 4) {
            try out.raw("if (use_projection_prepared_sums) {\n");
            try cachedProjection(out, mr, true, true);
            try out.raw("} else {\n");
        }
        try tripleDotProjection(out, mr);
        try tripleBias(out, mr);
        if (mr == 4) try out.raw("}\n");
        try out.raw("} else if (use_projection_prepared_sums) {\n");
        try cachedProjection(out, mr, true, false);
        try tripleBias(out, mr);
        try out.raw("} else {\n");
        try cachedProjection(out, mr, false, false);
        try tripleBias(out, mr);
        try out.raw("}\n");
    }
    try out.raw("}\n");
    if (mr == 1) try out.raw("}\n");
}

pub const FallbackPanelRows = struct {
    pub const begin = "    // quant-kernel-codegen:begin CPU Q4 Q5 fallback panel rows";
    pub const end = "    // quant-kernel-codegen:end CPU Q4 Q5 fallback panel rows";
    pub fn render(allocator: std.mem.Allocator) ![]u8 {
        var out = Emitter{ .allocator = allocator };
        defer out.bytes.deinit(allocator);
        for ([_]usize{ 2, 1 }) |mr| {
            if (mr == 1) try out.raw("while (r < rows) : (r += 1) {\n") else try out.raw("while (r + 2 <= rows) : (r += 2) {\n");
            try inputRows(&out, mr);
            try outputRows(&out, mr, null);
            if (mr == 1) try activationCache(&out, mr, "use_prepared_sums", "row_blocks <= q8_k_prepared_sum_stack_max_row_blocks", false);
            try out.raw("var o: usize = out_start;\nwhile (o < out_end and (o % 4) != 0) : (o += 1) {\n");
            try scalarColumn(&out, mr, false);
            try out.raw("}\n");
            if (mr == 1) {
                try out.raw("if (use_panel16_direct) {\nif (panel16_blocks) |panel16| {\n");
                try panel16Column(&out, mr);
                try out.raw("}\n}\n");
            }
            try panel8Column(&out, mr);
            try fourColumns(&out, mr);
            try out.raw("while (o < out_end) : (o += 1) {\n");
            try scalarColumn(&out, mr, false);
            try out.raw("}\n}\n");
        }
        return native.formatStatements(allocator, out.bytes.items, 4);
    }
};
fn panel8Column(out: *Emitter, mr: usize) !void {
    const width = if (mr == 1) "8" else "prepared_k_panel8_nr";
    try out.add("if (panel8_blocks) |panel8| {{\nwhile ((o % {s}) == 0 and o + {s} <= out_end) : (o += {s}) {{\n", .{ width, width, width });
    for (0..mr) |r| {
        if (mr == 1) try out.raw("var acc: [8]f32 = @as([8]f32, @splat(0.0));\n") else try out.add("var acc_r{d}: [prepared_k_panel8_nr]f32 = @as([prepared_k_panel8_nr]f32, @splat(0.0));\n", .{r});
    }
    try out.raw("for (0..row_blocks) |block_idx| {\nconst q8_off = block_idx * q8_block_size;\nconst panel_off = ((o / prepared_k_panel8_nr) * row_blocks + block_idx) * prepared_k_panel8_block_bytes;\nconst sums = dotQ4Q5KPreparedQ8KPanel8");
    if (mr != 1) try out.add("MR{d}", .{mr});
    try out.raw("(\n");
    for (0..mr) |r| try out.add("q_input_row{s}[q8_off..][0..q8_block_size],\n", .{suffix(mr, r)});
    try out.raw("panel8[panel_off..][0..prepared_k_panel8_block_bytes],\n);\n");
    if (mr == 1) {
        try out.raw("inline for (0..8) |idx| acc[idx] += sums[idx];\n}\ninline for (0..8) |idx| out_row[o + idx] = acc[idx] + optionalLinearBiasAt(bias, o + idx);\n");
    } else {
        try out.raw("inline for (0..prepared_k_panel8_nr) |idx| {\n");
        for (0..mr) |r| try out.add("acc_r{d}[idx] += sums.r{d}[idx];\n", .{ r, r });
        try out.raw("}\n}\ninline for (0..prepared_k_panel8_nr) |idx| {\nconst bias_value = optionalLinearBiasAt(bias, o + idx);\n");
        for (0..mr) |r| try out.add("out_row{d}[o + idx] = acc_r{d}[idx] + bias_value;\n", .{ r, r });
        try out.raw("}\n");
    }
    try out.raw("}\n}\n");
}
fn scalarTileName(out: *Emitter, mr: usize, row: usize, col: usize) !void {
    if (mr == 1) try out.add("acc{d}", .{col}) else try out.add("acc_r{d}_{d}", .{ row, col });
}
fn fourColumns(out: *Emitter, mr: usize) !void {
    try out.raw("while (o + 4 <= out_dim) : (o += 4) {\nif (o + 4 > out_end) break;\n");
    for (0..mr) |r| for (0..4) |c| {
        try out.raw("var ");
        try scalarTileName(out, mr, r, c);
        try out.raw(": f32 = 0.0;\n");
    };
    try out.raw("for (0..row_blocks) |block_idx| {\nconst q8_off = block_idx * q8_block_size;\n");
    if (mr == 1) try out.raw("const sums = dotQ4Q5KPreparedQ8KBlock4(\n") else try out.raw("const sums = dotPreparedKQ8KBlock4MR2(\nfalse,\n");
    for (0..mr) |r| try out.add("q_input_row{s}[q8_off..][0..q8_block_size],\n", .{suffix(mr, r)});
    for (0..4) |c| try out.add("preparedKPanelBlock(panel_blocks, prepared_blocks, row_blocks, prepared_row_stride, o, block_idx, {d}),\n", .{c});
    try out.raw(");\n");
    for (0..mr) |r| for (0..4) |c| {
        try scalarTileName(out, mr, r, c);
        try out.add(" += sums[{d}];\n", .{r * 4 + c});
    };
    try out.raw("}\n");
    if (mr != 1) for (0..4) |c| {
        try out.add("const bias{d} = optionalLinearBiasAt(bias, o", .{c});
        if (c != 0) try out.add(" + {d}", .{c});
        try out.raw(");\n");
    };
    for (0..mr) |r| for (0..4) |c| {
        try out.add("out_row{s}[o", .{suffix(mr, r)});
        if (c != 0) try out.add(" + {d}", .{c});
        try out.raw("] = ");
        try scalarTileName(out, mr, r, c);
        if (mr != 1) {
            try out.add(" + bias{d};\n", .{c});
        } else {
            try out.raw(" + optionalLinearBiasAt(bias, o");
            if (c != 0) try out.add(" + {d}", .{c});
            try out.raw(");\n");
        }
    };
    try out.raw("}\n");
}
