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

//! Prepared legacy quantization traversal, lowered with the original native
//! register layout and scale/min arithmetic for each row geometry.
const std = @import("std");
const cpu = @import("quant_kernel_cpu_source.zig");
const Emitter = cpu.Emitter;
pub const owners = .{ SingleRows, GroupedRows, RangeRows };
pub const SingleRows = struct {
    pub const name = "legacy single projection rows";
    pub const begin = "    // quant-kernel-codegen:begin CPU legacy single projection rows";
    pub const end = "    // quant-kernel-codegen:end CPU legacy single projection rows";
    pub fn render(allocator: std.mem.Allocator) ![]u8 {
        var out = Emitter{ .allocator = allocator };
        defer out.bytes.deinit(allocator);
        try out.raw("var r: usize = 0;\n");
        for ([_]usize{ 4, 2, 1 }) |mr| {
            if (mr == 4) try out.raw("if (use_mr4) {\nwhile (r + 4 <= rows) : (r += 4) {\n") else try out.raw(if (mr == 2) "while (r + 1 < rows) : (r += 2) {\n" else "if (r < rows) {\n");
            try rowViews(&out, mr, false);
            try out.raw("var o: usize = 0;\nwhile (o + prepared_q8_0_panel_nr <= out_dim) : (o += prepared_q8_0_panel_nr) {\n");
            try panelColumn(&out, mr, false);
            try out.raw("}\nwhile (o < out_dim) : (o += 1) {\n");
            try scalarColumn(&out, mr, false);
            try out.raw("}\n}\n");
            if (mr == 4) try out.raw("}\n");
        }
        return cpu.formatStatements(allocator, out.bytes.items, 4);
    }
};
fn rowName(out: *Emitter, prefix: []const u8, mr: usize, r: usize) !void {
    try out.raw(prefix);
    if (mr != 1) try out.add("{d}", .{r});
}
fn rowViews(out: *Emitter, mr: usize, grouped: bool) !void {
    var names: cpu.Rows = .{};
    for (0..(if (grouped) @as(usize, 3) else 1)) |projection| for (0..mr) |r| {
        const p: u8 = @as(u8, @intCast(projection)) + 'a';
        try out.raw("const ");
        if (grouped) {
            try out.add("out_row_{c}", .{p});
            if (mr != 1) try out.add("{d}", .{r});
        } else try rowName(out, "out_row", mr, r);
        try out.raw(" = ");
        if (grouped) {
            try thirdGuard(out, p);
            try out.add("output_{c}", .{p});
        } else try out.raw("output");
        try out.add("[{s} * out_dim ..][0..out_dim]", .{names.position(r)});
        if (grouped and p == 'c') try out.raw(" else {}");
        try out.raw(";\n");
    };
    const views = [_]struct { name: []const u8, source: []const u8, stride: []const u8 }{
        .{ .name = "input_q_row", .source = "q_values", .stride = "row_blocks * values_per_block" },
        .{ .name = "input_scale_row", .source = "input_scales", .stride = "row_blocks" },
        .{ .name = "input_sum_row", .source = "input_sums", .stride = "row_blocks" },
    };
    for (views) |view| for (0..mr) |r| {
        try out.raw("const ");
        try rowName(out, view.name, mr, r);
        try out.add(" = {s}[{s} * {s} ..][0 .. {s}];\n", .{ view.source, names.position(r), view.stride, view.stride });
    };
}
fn qViews(out: *Emitter, mr: usize) !void {
    for (0..mr) |r| {
        try out.raw("const ");
        try rowName(out, "qx", mr, r);
        try out.raw(": *const [32]u8 = ");
        try rowName(out, "input_q_row", mr, r);
        try out.raw("[block_idx * values_per_block ..][0..values_per_block];\n");
    }
}
fn accumulator(out: *Emitter, mr: usize, r: usize, c: usize) !void {
    if (mr == 1) try out.add("acc{d}", .{c}) else try out.add("acc{d}{d}", .{ r, c });
}
fn publish(out: *Emitter, mr: usize, panel: bool) !void {
    if (panel and mr == 4) try out.raw("inline for (0..4) |lane| {\n");
    var names: cpu.Rows = .{};
    for (0..mr) |r| {
        for (0..(if (panel and mr != 4) @as(usize, 4) else 1)) |c| {
            try rowName(out, "out_row", mr, r);
            try out.raw("[o");
            if (panel and mr == 4) try out.raw(" + lane") else if (c != 0) try out.add(" + {d}", .{c});
            try out.raw("] = ");
            if (panel and mr == 4) try out.add("acc[{d}][lane]", .{r}) else if (panel) try accumulator(out, mr, r, c) else try out.raw(names.acc(mr, r));
            try out.raw(";\n");
        }
    }
    if (panel and mr == 4) try out.raw("}\n");
}
fn panelWeights(out: *Emitter) !void {
    try out.raw("const panel_blocks = if (prepared_panel_blocks) |panel| blk: {\nconst panel_off = ((o / prepared_q8_0_panel_nr) * row_blocks + block_idx) * prepared_q8_0_panel_nr * prepared_legacy_block_bytes;\nbreak :blk panel[panel_off..][0 .. prepared_q8_0_panel_nr * prepared_legacy_block_bytes];\n} else null;\n");
    for (0..4) |c| {
        try out.add("const w{d} = if (panel_blocks) |panel| panel[", .{c});
        if (c == 0) try out.raw("0") else if (c == 1) try out.raw("prepared_legacy_block_bytes") else try out.add("{d} * prepared_legacy_block_bytes ", .{c});
        try out.add("..][0..prepared_legacy_block_bytes] else prepared_blocks[(o + {d}) * row_stride + block_idx * prepared_legacy_block_bytes ..][0..prepared_legacy_block_bytes];\n", .{c});
    }
}
fn panelColumn(out: *Emitter, mr: usize, range: bool) !void {
    if (mr == 4) {
        try out.raw("var acc: [4][4]f32 = undefined;\ninline for (0..4) |lane| {\nconst value = optionalLinearBiasAt(bias, o + lane);\n");
        for (0..4) |r| try out.add("acc[{d}][lane] = value;\n", .{r});
        try out.raw("}\n");
    } else for (0..mr) |r| for (0..4) |c| {
        try out.raw("var ");
        try accumulator(out, mr, r, c);
        try out.raw(" = ");
        if (r == 0) {
            try out.raw("optionalLinearBiasAt(bias, o");
            if (c != 0) try out.add(" + {d}", .{c});
            try out.raw(")");
        } else try accumulator(out, mr, 0, c);
        try out.raw(";\n");
    };
    try out.raw("for (0..row_blocks) |block_idx| {\n");
    try panelWeights(out);
    try qViews(out, mr);
    try out.raw("const dots = dotI8I8Block4");
    if (mr != 1) try out.add("MR{d}", .{mr});
    try out.raw("(");
    for (0..mr) |r| {
        try rowName(out, "qx", mr, r);
        try out.raw(", ");
    }
    for (0..4) |c| {
        try out.add("preparedLegacyValues(w{d})", .{c});
        if (c != 3) try out.raw(", ");
    }
    try out.raw(");\n");
    if (mr == 4) {
        try out.raw("const scales = [_]f32{ input_scale_row0[block_idx], input_scale_row1[block_idx], input_scale_row2[block_idx], input_scale_row3[block_idx] };\nconst sums = [_]f32{\n");
        for (0..4) |r| try out.add("@as(f32, @floatFromInt(input_sum_row{d}[block_idx])),\n", .{r});
        try out.raw("};\n");
        for ([_][]const u8{ "scales", "mins" }, [_][]const u8{ "Scale", "Min" }) |label, operation| {
            try out.add("const weight_{s} = [_]f32{{ ", .{label});
            for (0..4) |c| {
                try out.add("preparedLegacy{s}(w{d})", .{ operation, c });
                if (c != 3) try out.raw(", ");
            }
            try out.raw(" };\n");
        }
        try out.raw("inline for (0..4) |row_idx| {\ninline for (0..4) |lane| {\nacc[row_idx][lane] += scales[row_idx] * (weight_scales[lane] * @as(f32, @floatFromInt(dots[row_idx * 4 + lane])) + weight_mins[lane] * sums[row_idx]);\n}\n}\n");
    } else if (mr == 2) {
        for (0..2) |r| try out.add("const scale{d} = input_scale_row{d}[block_idx];\n", .{ r, r });
        for (0..4) |c| try out.add("const ws{d} = preparedLegacyScale(w{d});\n", .{ c, c });
        for ([_]bool{ false, true }) |with_min| {
            try out.raw(if (with_min) "} else {\n" else "if (!use_mr4) {\n");
            if (with_min) {
                for (0..2) |r| try out.add("const sum{d} = @as(f32, @floatFromInt(input_sum_row{d}[block_idx]));\n", .{ r, r });
                for (0..4) |c| try out.add("const wm{d} = preparedLegacyMin(w{d});\n", .{ c, c });
            }
            for (0..2) |r| for (0..4) |c| {
                try accumulator(out, mr, r, c);
                try out.add(" += scale{d} * ", .{r});
                if (with_min) try out.raw("(");
                try out.add("ws{d} * @as(f32, @floatFromInt(dots[{d}]))", .{ c, r * 4 + c });
                if (with_min) try out.add(" + wm{d} * sum{d})", .{ c, r });
                try out.raw(";\n");
            };
        }
        try out.raw("}\n");
    } else {
        try out.raw("const input_scale = input_scale_row[block_idx];\n");
        if (range) {
            try out.raw("if (!use_mr4) {\n");
            try singlePanelUpdates(out, false);
            try out.raw("} else {\n");
        }
        try out.raw("const input_sum_f = @as(f32, @floatFromInt(input_sum_row[block_idx]));\n");
        try singlePanelUpdates(out, true);
        if (range) try out.raw("}\n");
    }
    try out.raw("}\n");
    try publish(out, mr, true);
}
fn scalarRow(out: *Emitter, mr: usize, with_min: bool) !void {
    var names: cpu.Rows = .{};
    for (0..mr) |r| {
        try out.add("{s} += ", .{names.acc(mr, r)});
        try rowName(out, "input_scale_row", mr, r);
        try out.raw("[block_idx] * ");
        if (with_min) try out.raw("(");
        try out.raw("weight_scale * @as(f32, @floatFromInt(dotI8I8Block(");
        try rowName(out, "qx", mr, r);
        try out.raw(", qw)))");
        if (with_min) {
            try out.raw(" + weight_min * @as(f32, @floatFromInt(");
            try rowName(out, "input_sum_row", mr, r);
            try out.raw("[block_idx])))");
        }
        try out.raw(";\n");
    }
}
fn scalarColumn(out: *Emitter, mr: usize, range: bool) !void {
    var names: cpu.Rows = .{};
    for (0..mr) |r| try out.add("var {s} = {s};\n", .{ names.acc(mr, r), if (r == 0) "optionalLinearBiasAt(bias, o)" else "acc0" });
    try out.raw("const w_off = o * row_stride;\nfor (0..row_blocks) |block_idx| {\nconst block = prepared_blocks[w_off + block_idx * prepared_legacy_block_bytes ..][0..prepared_legacy_block_bytes];\n");
    if (mr == 4) try out.raw("const qw = preparedLegacyValues(block);\nconst weight_scale = preparedLegacyScale(block);\nconst weight_min = preparedLegacyMin(block);\n");
    if (mr != 1) {
        try qViews(out, mr);
        if (mr == 2) try out.raw("const qw = preparedLegacyValues(block);\nconst weight_scale = preparedLegacyScale(block);\nif (!use_mr4) {\n");
        try scalarRow(out, mr, mr == 4);
        if (mr == 2) {
            try out.raw("} else {\nconst weight_min = preparedLegacyMin(block);\n");
            try scalarRow(out, mr, true);
            try out.raw("}\n");
        }
    } else {
        try out.raw("const input_scale = input_scale_row[block_idx];\n");
        try qViews(out, mr);
        try out.raw("const dot = dotI8I8Block(qx, preparedLegacyValues(block));\n");
        if (!range) try out.raw("if (!use_mr4) {\nacc += input_scale * preparedLegacyScale(block) * @as(f32, @floatFromInt(dot));\n} else {\n");
        try out.raw("acc += input_scale * (preparedLegacyScale(block) * @as(f32, @floatFromInt(dot)) + preparedLegacyMin(block) * @as(f32, @floatFromInt(input_sum_row[block_idx])));\n");
        if (!range) try out.raw("}\n");
    }
    try out.raw("}\n");
    try publish(out, mr, false);
}

test "legacy row geometries retain min correction and native reduction arity" {
    const source = try SingleRows.render(std.testing.allocator);
    defer std.testing.allocator.free(source);
    try std.testing.expect(std.mem.indexOf(u8, source, "dotI8I8Block4MR4(qx0, qx1, qx2, qx3,") != null);
    try std.testing.expect(std.mem.indexOf(u8, source, "dotI8I8Block4MR2(qx0, qx1,") != null);
    try std.testing.expect(std.mem.indexOf(u8, source, "weight_mins[lane] * sums[row_idx]") != null);
}

pub const GroupedRows = struct {
    pub const name = "legacy grouped projection rows";
    pub const begin = "    // quant-kernel-codegen:begin CPU legacy grouped projection rows";
    pub const end = "    // quant-kernel-codegen:end CPU legacy grouped projection rows";
    pub fn render(allocator: std.mem.Allocator) ![]u8 {
        var out = Emitter{ .allocator = allocator };
        defer out.bytes.deinit(allocator);
        try out.raw("var r: usize = 0;\nif (use_mr4 and out_start == 0 and out_end == out_dim and (out_dim % prepared_q8_0_panel_nr) == 0) {\nwhile (r + 4 <= rows) : (r += 4) {\n");
        try rowViews(&out, 4, true);
        try out.raw("var o: usize = 0;\nwhile (o + prepared_q8_0_panel_nr <= out_dim) : (o += prepared_q8_0_panel_nr) {\n");
        for ("abc") |p| {
            try out.add("var acc_{c}: ", .{p});
            if (p == 'c') try out.raw("if (projection_count == 3) [4][4]f32 else void = undefined;\n") else try out.raw("[4][4]f32 = if (projection_count == 2) @splat(@as([4]f32, @splat(0.0))) else undefined;\n");
        }
        try out.raw("inline for (0..4) |rr| {\ninline for (0..4) |cc| {\n");
        for ("abc") |p| try out.add("if (projection_count == 3) acc_{c}[rr][cc] = optionalLinearBiasAt(bias_{c}, o + cc);\n", .{ p, p });
        try out.raw("}\n}\nfor (0..row_blocks) |block_idx| {\n");
        try qViews(&out, 4);
        for ([_]bool{ false, true }) |sums| {
            try out.add("const input_{s}_for_block = [_]f32{{\n", .{if (sums) "sums" else "scales"});
            for (0..4) |r| {
                if (sums) try out.add("@as(f32, @floatFromInt(input_sum_row{d}[block_idx])),\n", .{r}) else try out.add("input_scale_row{d}[block_idx],\n", .{r});
            }
            try out.raw("};\n");
        }
        for ("abc") |p| {
            try groupedWeights(&out, p);
            try thirdGuard(&out, p);
            try out.add("addLegacyPreparedQ8_0Block4MR4(qx0, qx1, qx2, qx3, &input_scales_for_block, &input_sums_for_block, {c}0, {c}1, {c}2, {c}3, &acc_{c});\n", .{ p, p, p, p, p });
        }
        try out.raw("}\ninline for (0..4) |lane| {\n");
        for ("abc") |p| for (0..4) |r| {
            try thirdGuard(&out, p);
            try out.add("out_row_{c}{d}[o + lane] = acc_{c}[{d}][lane];\n", .{ p, r, p, r });
        };
        try out.raw("}\n}\n}\n}\nwhile (r < rows) : (r += 1) {\n");
        try rowViews(&out, 1, true);
        try out.raw("var o = out_start;\nwhile (o < out_end and (o % prepared_q8_0_panel_nr) != 0) : (o += 1) {\n");
        try groupedColumn(&out, false);
        try out.raw("}\nwhile (o + prepared_q8_0_panel_nr <= out_end) : (o += prepared_q8_0_panel_nr) {\n");
        try groupedColumn(&out, true);
        try out.raw("}\nwhile (o < out_end) : (o += 1) {\n");
        try groupedColumn(&out, false);
        try out.raw("}\n}\n");
        return cpu.formatStatements(allocator, out.bytes.items, 4);
    }
};
fn thirdGuard(out: *Emitter, projection: u8) !void {
    if (projection == 'c') try out.raw("if (projection_count == 3) ");
}
fn groupedWeights(out: *Emitter, p: u8) !void {
    for (0..4) |c| {
        try out.add("const {c}{d} = ", .{ p, c });
        try thirdGuard(out, p);
        try out.add("preparedLegacyPanelBlock(prepared_panel_blocks_{c}, prepared_blocks_{c}, row_blocks, row_stride, o, block_idx, {d})", .{ p, p, c });
        if (p == 'c') try out.raw(" else {}");
        try out.raw(";\n");
    }
}
fn groupedAcc(out: *Emitter, p: u8, panel: bool, c: usize) !void {
    try out.add("acc_{c}", .{p});
    if (panel) try out.add("{d}", .{c});
}
fn groupedColumn(out: *Emitter, panel: bool) !void {
    const nr: usize = if (panel) 4 else 1;
    for ("abc") |p| for (0..nr) |c| {
        try out.raw("var ");
        try groupedAcc(out, p, panel, c);
        if (p == 'c') try out.raw(": if (projection_count == 3) f32 else void");
        try out.raw(" = ");
        try out.raw(if (p == 'c') "if (projection_count == 3) " else "if (projection_count == 2) @as(f32, 0) else ");
        try out.add("optionalLinearBiasAt(bias_{c}, o", .{p});
        if (c != 0) try out.add(" + {d}", .{c});
        try out.raw(")");
        if (p == 'c') try out.raw(" else {}");
        try out.raw(";\n");
    };
    try out.raw("for (0..row_blocks) |block_idx| {\n");
    try qViews(out, 1);
    try out.raw("const input_scale = input_scale_row[block_idx];\nconst input_sum_f = @as(f32, @floatFromInt(input_sum_row[block_idx]));\n");
    if (panel) {
        for ("abc") |p| try groupedWeights(out, p);
        for ("abc") |p| {
            try out.add("const dots_{c} = ", .{p});
            try thirdGuard(out, p);
            try out.add("dotI8I8Block4(qx, preparedLegacyValues({c}0), preparedLegacyValues({c}1), preparedLegacyValues({c}2), preparedLegacyValues({c}3))", .{ p, p, p, p });
            if (p == 'c') try out.raw(" else {}");
            try out.raw(";\n");
        }
    }
    for ("abc") |p| for (0..nr) |c| {
        try thirdGuard(out, p);
        try groupedAcc(out, p, panel, c);
        if (panel) try out.add(" += input_scale * (preparedLegacyScale({c}{d}) * @as(f32, @floatFromInt(dots_{c}[{d}])) + preparedLegacyMin({c}{d}) * input_sum_f);\n", .{ p, c, p, c, p, c }) else try out.add(" += dotLegacyPreparedQ8_0Block(qx, input_scale, input_sum_f, prepared_blocks_{c}[o * row_stride + block_idx * prepared_legacy_block_bytes ..][0..prepared_legacy_block_bytes]);\n", .{p});
    };
    try out.raw("}\n");
    for ("abc") |p| for (0..nr) |c| {
        try thirdGuard(out, p);
        try out.add("out_row_{c}[o", .{p});
        if (c != 0) try out.add(" + {d}", .{c});
        try out.raw("] = ");
        try groupedAcc(out, p, panel, c);
        try out.raw(";\n");
    };
}

pub const RangeRows = struct {
    pub const name = "legacy range projection rows";
    pub const begin = "    // quant-kernel-codegen:begin CPU legacy range projection rows";
    pub const end = "    // quant-kernel-codegen:end CPU legacy range projection rows";
    pub fn render(allocator: std.mem.Allocator) ![]u8 {
        var out = Emitter{ .allocator = allocator };
        defer out.bytes.deinit(allocator);
        try out.raw("for (0..rows) |r| {\n");
        try rowViews(&out, 1, false);
        try out.raw("var o = out_start;\nwhile (o < out_end and (o % prepared_q8_0_panel_nr) != 0) : (o += 1) {\n");
        try scalarColumn(&out, 1, true);
        try out.raw("}\nwhile (o + prepared_q8_0_panel_nr <= out_end) : (o += prepared_q8_0_panel_nr) {\n");
        try panelColumn(&out, 1, true);
        try out.raw("}\nwhile (o < out_end) : (o += 1) {\n");
        try scalarColumn(&out, 1, true);
        try out.raw("}\n}\n");
        return cpu.formatStatements(allocator, out.bytes.items, 4);
    }
};
fn singlePanelUpdates(out: *Emitter, with_min: bool) !void {
    for (0..4) |c| {
        try out.add("acc{d} += input_scale * ", .{c});
        if (with_min) try out.raw("(");
        try out.add("preparedLegacyScale(w{d}) * @as(f32, @floatFromInt(dots[{d}]))", .{ c, c });
        if (with_min) try out.add(" + preparedLegacyMin(w{d}) * input_sum_f)", .{c});
        try out.raw(";\n");
    }
}
