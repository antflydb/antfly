// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Lower grouped projection geometry into the native row-register schedules.
//! Projection order, panel acquisition, arithmetic and publication stay native;
//! row widths and column widths share traversal and tail ownership.
const std = @import("std");
const cpu = @import("quant_kernel_cpu_source.zig");

pub const owners = .{ Q6GroupedOwner, Q8GroupedOwner, K16GroupedOwner, Q3GroupedOwner, SingleK16Rows, SingleK16Panel16Rows };
pub const Q6GroupedOwner = GroupedOwner(false);
pub const Q8GroupedOwner = GroupedOwner(true);
fn GroupedOwner(comptime q8: bool) type {
    return struct {
        pub const name = if (q8) "linearQ8KQ8KActivationGroupedRange" else "linearQ6KPreparedQ8KActivationGroupedRange";
        pub const begin = "// quant-kernel-codegen:begin CPU grouped projection " ++ name;
        pub const end = "// quant-kernel-codegen:end CPU grouped projection " ++ name;
        pub fn render(allocator: std.mem.Allocator) ![]u8 {
            var out = Emitter{ .allocator = allocator };
            defer out.bytes.deinit(allocator);
            try projectionPrefix(&out, name, if (q8) .q8 else .q6);
            for ([_]usize{ 4, 2, 1 }) |mr| {
                if (mr == 1) try out.raw("while (r < rows) : (r += 1) {\n") else try out.add("while (r + {d} <= rows) : (r += {d}) {{\n", .{ mr, mr });
                var names: Names = .{};
                for (0..mr) |r| try out.add("const {s} = q_input[{s} * {s} ..][0..{s}];\n", .{ names.row(mr, r), names.position(r), if (q8) "row_stride" else "input_row_stride", if (q8) "row_stride" else "input_row_stride" });
                try out.raw("var o: usize = out_start;\nwhile (o < out_end and (o % panel_align) != 0) : (o += 1) {\n");
                try scalarColumn(&out, mr, q8);
                try out.raw("}\n");
                for ([_]usize{ 16, 8 }) |nr| {
                    if (q8) try q8WideColumn(&out, mr, nr, false) else try panelColumn(&out, mr, nr);
                }
                if (q8) {
                    try q8WideColumn(&out, mr, 8, true);
                    try q8FourColumns(&out, mr);
                } else try panelColumn(&out, mr, 4);
                try out.raw("while (o < out_end) : (o += 1) {\n");
                try scalarColumn(&out, mr, q8);
                try out.raw("}\n}\n");
            }
            try out.raw("}\n");
            return cpu.formatZig(allocator, out.bytes.items);
        }
    };
}

const ProjectionFamily = enum { q6, q8, k16, q3 };
fn projectionPrefix(out: *Emitter, name: []const u8, family: ProjectionFamily) !void {
    const P = cpu.Parameter;
    const args: []const P = switch (family) {
        .q6 => &.{ .projection_count_comptime, .q_input, .prepared_blocks_grouped, .panel_blocks_grouped, .panel8_optional_grouped, .panel16_optional_grouped, .outputs },
        .q8 => &.{ .projection_count_comptime, .q_input, .weight_raw_grouped, .panel_blocks_grouped, .panel8_optional_grouped, .panel16_optional_grouped, .outputs, .biases },
        .k16 => &.{ .has_min_comptime, .projection_count_comptime, .q_input, .prepared_blocks_grouped, .panel_blocks_grouped, .panel8_optional_grouped, .biases, .outputs },
        .q3 => &.{ .projection_count_comptime, .q_input, .prepared_blocks_grouped, .q3_panels_grouped, .biases, .outputs },
    };
    try out.add("fn {s}(\n", .{name});
    for (args) |arg| try out.add("{s},\n", .{arg.declaration()});
    for ([_]P{ .rows, .in_dim, .out_dim, .out_start, .out_end }) |arg| try out.add("{s},\n", .{arg.declaration()});
    try out.raw(") void {\n");
    if (family != .k16) try out.raw("@setEvalBranchQuota(20_000);\n");
    const q8 = family == .q8;
    const wide = family == .q6 or q8;
    const block = if (q8) "block_size" else "q8_block_size";
    try out.add("const values_per_block: usize = 256;\nconst {s}: usize = 292;\n", .{block});
    if (wide) try out.raw("std.debug.assert(in_dim % values_per_block == 0);\n");
    try out.raw("const row_blocks = in_dim / values_per_block;\n");
    if (!wide) try out.raw("const prepared_row_stride = row_blocks * prepared_k16_block_bytes;\n");
    try out.add("const {s} = row_blocks * {s};\n", .{ if (q8) "row_stride" else "input_row_stride", block });
    if (family == .q6) try out.raw("const prepared_row_stride = row_blocks * prepared_q4_k_block_bytes;\n\n");
    if (q8) try out.raw("const use_fused_sdot = q8KFusedSdotEnabled();\n");
    if (wide) {
        for ([_]usize{ 8, 16 }) |nr| {
            var storage: [32]u8 = undefined;
            const available = if (q8) try std.fmt.bufPrint(&storage, "have_panel{d}", .{nr}) else try std.fmt.bufPrint(&storage, "panel{d}_available", .{nr});
            try out.add("var {s} = true;\ninline for (0..projection_count) |projection_idx| {{\n{s} = {s} and panel{d}_blocks[projection_idx] != null;\n}}\n", .{ available, available, available, nr });
        }
        try out.add("const panel_align: usize = if ({s}) prepared_k_panel16_nr else if ({s}) prepared_k_panel8_nr else prepared_q8_0_panel_nr;\n\n", .{ if (q8) "have_panel16" else "panel16_available", if (q8) "have_panel8" else "panel8_available" });
    }
    try out.raw("var r: usize = 0;\n");
}

const Emitter = cpu.Emitter;
const Names = cpu.Rows;

fn accumulators(out: *Emitter, mr: usize, width: ?[]const u8) !void {
    var names: Names = .{};
    for (0..mr) |r| {
        if (width) |w| try out.add("var {s}: [projection_count][{s}]f32 = @as([projection_count][{s}]f32, @splat(@as([{s}]f32, @splat(0.0))));\n", .{ names.acc(mr, r), w, w, w }) else try out.add("var {s}: [projection_count]f32 = @as([projection_count]f32, @splat(0.0));\n", .{names.acc(mr, r)});
    }
}
fn scalarColumn(out: *Emitter, mr: usize, q8: bool) !void {
    if (q8) return q8ScalarColumn(out, mr);
    try accumulators(out, mr, null);
    try out.raw("for (0..row_blocks) |block_idx| {\nconst q8_off = block_idx * q8_block_size;\nconst prepared_off = block_idx * prepared_q4_k_block_bytes;\ninline for (0..projection_count) |projection_idx| {\nconst prepared_row = prepared_blocks[projection_idx][o * prepared_row_stride ..][0..prepared_row_stride];\n");
    var names: Names = .{};
    if (mr != 1) try out.raw("const prepared_block = prepared_row[prepared_off..][0..prepared_q4_k_block_bytes];\n");
    for (0..mr) |r| {
        if (mr == 1) try out.raw("acc[projection_idx] += dotQ6KPreparedQ8KBlock(\nq_input_row[q8_off..][0..q8_block_size],\nprepared_row[prepared_off..][0..prepared_q4_k_block_bytes],\n);\n") else try out.add("{s}[projection_idx] += dotQ6KPreparedQ8KBlock({s}[q8_off..][0..q8_block_size], prepared_block);\n", .{ names.acc(mr, r), names.row(mr, r) });
    }
    try out.raw("}\n}\ninline for (0..projection_count) |projection_idx| {\n");
    for (0..mr) |r| try out.add("outputs[projection_idx][{s} * out_dim + o] = {s}[projection_idx];\n", .{ names.position(r), names.acc(mr, r) });
    try out.raw("}\n");
}
// Keep row accumulators and publication lexical: their register schedule is
// qualified native code. Only format/layout spelling differs across families.
fn wideColumn(out: *Emitter, mr: usize, nr: usize, q8: bool, raw: bool) !void {
    const width = if (raw or (!q8 and nr == 8 and mr == 1)) "8" else if (nr == 16) "prepared_k_panel16_nr" else "prepared_k_panel8_nr";
    const bytes = if (q8) (if (nr == 16) "prepared_q8_k_panel16_block_bytes" else "prepared_q8_k_panel8_block_bytes") else if (nr == 16) "prepared_k_panel16_block_bytes" else "prepared_q6_k_panel8_block_bytes";
    if (raw) try out.raw("if (q8KRawNR8DirectEnabled()) {\nwhile (o + 8 <= out_end) : (o += 8) {\n") else {
        try out.add("if ({s}{d}{s}) {{\n", .{ if (q8) "have_panel" else "panel", nr, if (q8) "" else "_available" });
        if (q8 and nr == 8) try out.raw("while (o + prepared_k_panel8_nr <= out_end) : (o += prepared_k_panel8_nr) {\n") else try out.add("while ((o % prepared_k_panel{d}_nr) == 0 and o + prepared_k_panel{d}_nr <= out_end) : (o += prepared_k_panel{d}_nr) {{\n", .{ nr, nr, nr });
    }
    try accumulators(out, mr, width);
    try out.raw("for (0..row_blocks) |block_idx| {\n");
    var names: Names = .{};
    if (q8) try q8BlockViews(out, mr) else {
        try out.raw("const q8_off = block_idx * q8_block_size;\n");
        for (0..mr) |r| try out.add("const {s} = {s}[q8_off..][0..q8_block_size];\n", .{ names.block(mr, r), names.row(mr, r) });
    }
    if (!raw and (q8 or mr != 1 or nr != 8)) try out.add("const panel_off = ((o / prepared_k_panel{d}_nr) * row_blocks + block_idx) * {s};\n", .{ nr, bytes });
    try out.raw("inline for (0..projection_count) |projection_idx| {\n");
    if (raw) try out.raw("const weights = q8KQ8KWeightBlock8(weight_raw[projection_idx], panel_blocks[projection_idx], row_blocks, row_stride, o, block_idx, true);\n");
    if (!q8) {
        try out.add("const panel{d} = panel{d}_blocks[projection_idx].?;\n", .{ nr, nr });
        if (mr == 1 and nr == 8) try out.add("const panel_off = ((o / prepared_k_panel8_nr) * row_blocks + block_idx) * {s};\n", .{bytes});
        if (mr == 2 and nr == 8) try out.raw("const panel_block = panel8[panel_off..][0..prepared_q6_k_panel8_block_bytes];\n");
    }
    try out.add("const sums = {s}{d}", .{ if (q8) "dotQ8KQ8KBlock" else "dotQ6KPreparedQ8KPanel", nr });
    if (mr != 1) try out.add("MR{d}", .{mr});
    if (raw) try out.raw("Blocks");
    try out.raw("(");
    for (0..mr) |r| try out.add("{s}, ", .{names.block(mr, r)});
    if (raw) try out.raw("weights);\n") else if (q8) try out.add("panel{d}_blocks[projection_idx].?[panel_off..][0..{s}]);\n", .{ nr, bytes }) else if (mr == 2 and nr == 8) try out.raw("panel_block);\n") else try out.add("panel{d}[panel_off..][0..{s}]);\n", .{ nr, bytes });
    if (q8 and mr == 1) try out.add("inline for (0..{s}) |lane| acc[projection_idx][lane] += sums[lane];\n", .{width}) else {
        try out.add("inline for (0..{s}) |lane| {{\n", .{width});
        for (0..mr) |r| {
            if (mr == 1) try out.raw("acc[projection_idx][lane] += sums[lane];\n") else try out.add("acc{d}[projection_idx][lane] += sums.r{d}[lane];\n", .{ r, r });
        }
        try out.raw("}\n");
    }
    try out.raw("}\n}\n");
    try publishPanel(out, mr, width, if (q8) .q8_panel else .q6);
    try out.raw("}\n}\n");
}
fn panelColumn(out: *Emitter, mr: usize, nr: usize) !void {
    if (nr != 4) return wideColumn(out, mr, nr, false, false);
    const width = "4";
    try out.raw("while (o + prepared_q8_0_panel_nr <= out_end) : (o += prepared_q8_0_panel_nr) {\n");
    try accumulators(out, mr, width);
    try out.raw("for (0..row_blocks) |block_idx| {\nconst q8_off = block_idx * q8_block_size;\n");
    var names: Names = .{};
    for (0..mr) |r| try out.add("const {s} = {s}[q8_off..][0..q8_block_size];\n", .{ names.block(mr, r), names.row(mr, r) });
    try out.raw("inline for (0..projection_count) |projection_idx| {\n");
    try fourColumnDot(out, mr);
    try out.raw("inline for (0..4) |lane| {\n");
    for (0..mr) |r| {
        if (mr == 4) try out.add("{s}[projection_idx][lane] += sums{s}[lane{s}];\n", .{ names.acc(mr, r), if (r < 2) "01" else "23", if (r % 2 == 0) "" else " + 4" }) else try out.add("{s}[projection_idx][lane] += sums[lane{s}];\n", .{ names.acc(mr, r), if (r == 0) "" else " + 4" });
    }
    try out.raw("}\n}\n}\n");
    try publishPanel(out, mr, width, .q6);
    try out.raw("}\n");
}
fn publishPanel(out: *Emitter, mr: usize, width: []const u8, policy: enum { q6, q8_panel, q8_four }) !void {
    const q8 = policy != .q6;
    var names: Names = .{};
    try out.raw("inline for (0..projection_count) |projection_idx| {\n");
    for (0..mr) |r| {
        if (mr == 1) try out.raw("const out_row = outputs[projection_idx][r * out_dim ..][0..out_dim];\n") else try out.add("const out_row{d} = outputs[projection_idx][{s} * out_dim ..][0..out_dim];\n", .{ r, names.position(r) });
    }
    if (policy == .q8_panel and mr == 1) {
        try out.add("inline for (0..{s}) |lane| out_row[o + lane] = acc[projection_idx][lane] + q8KGroupedBias(projection_count, biases, projection_idx, o + lane);\n}}\n", .{width});
        return;
    }
    try out.add("inline for (0..{s}) |lane| {{\n", .{width});
    if (q8 and mr != 1) try out.raw("const bias = q8KGroupedBias(projection_count, biases, projection_idx, o + lane);\n");
    for (0..mr) |r| {
        if (mr == 1) {
            try out.add("out_row[o + lane] = acc[projection_idx][lane]{s};\n", .{if (q8) " + q8KGroupedBias(projection_count, biases, projection_idx, o + lane)" else ""});
        } else if (policy == .q8_four and mr == 4) {
            try out.add("out_row{d}[o + lane] = acc[projection_idx][", .{r});
            if (r != 0) try out.add("{d} + ", .{r * 4});
            try out.raw("lane] + bias;\n");
        } else try out.add("out_row{d}[o + lane] = acc{d}[projection_idx][lane]{s};\n", .{ r, r, if (q8) " + bias" else "" });
    }
    try out.raw("}\n}\n");
}

fn fourColumnDot(out: *Emitter, mr: usize) !void {
    if (mr == 4) {
        for (0..4) |c| try out.add("const b{d} = preparedKPanelBlock(panel_blocks[projection_idx], prepared_blocks[projection_idx], row_blocks, prepared_row_stride, o, block_idx, {d});\n", .{ c, c });
        try out.raw("const sums01 = dotPreparedKQ8KBlock4MR2(true, q0, q1, b0, b1, b2, b3);\nconst sums23 = dotPreparedKQ8KBlock4MR2(true, q2, q3, b0, b1, b2, b3);\n");
    } else {
        if (mr == 2) try out.raw("const sums = dotPreparedKQ8KBlock4MR2(\ntrue,\nq0,\nq1,\n") else try out.raw("const sums = dotQ6KPreparedQ8KBlock4(\nq,\n");
        for (0..4) |c| try out.add("preparedKPanelBlock(panel_blocks[projection_idx], prepared_blocks[projection_idx], row_blocks, prepared_row_stride, o, block_idx, {d}),\n", .{c});
        try out.raw(");\n");
    }
}

test "grouped projection lowering preserves valid native geometry" {
    inline for (owners) |Owner| {
        const source = try Owner.render(std.testing.allocator);
        defer std.testing.allocator.free(source);
        try std.testing.expect(std.mem.indexOf(u8, source, "while (r") != null);
    }
}

fn q8BlockViews(out: *Emitter, mr: usize) !void {
    try out.raw("const off = block_idx * block_size;\n");
    var names: Names = .{};
    for (0..mr) |r| try out.add("const {s} = {s}[off..][0..block_size];\n", .{ names.block(mr, r), names.row(mr, r) });
}
fn q8ScalarColumn(out: *Emitter, mr: usize) !void {
    try accumulators(out, mr, null);
    try out.raw("for (0..row_blocks) |block_idx| {\n");
    try q8BlockViews(out, mr);
    try out.raw("inline for (0..projection_count) |projection_idx| {\nconst weight_block = q8KQ8KWeightBlock(weight_raw[projection_idx], panel_blocks[projection_idx], row_blocks, row_stride, o, block_idx, false);\n");
    var names: Names = .{};
    for (0..mr) |r| try out.add("{s}[projection_idx] += dotQ8KQ8KBlock({s}, weight_block);\n", .{ names.acc(mr, r), names.block(mr, r) });
    try out.raw("}\n}\ninline for (0..projection_count) |projection_idx| {\n");
    if (mr != 1) try out.raw("const bias = q8KGroupedBias(projection_count, biases, projection_idx, o);\n");
    for (0..mr) |r| try out.add("outputs[projection_idx][{s} * out_dim + o] = {s}[projection_idx] + {s};\n", .{ names.position(r), names.acc(mr, r), if (mr == 1) "q8KGroupedBias(projection_count, biases, projection_idx, o)" else "bias" });
    try out.raw("}\n");
}
fn q8WideColumn(out: *Emitter, mr: usize, nr: usize, raw_weights: bool) !void {
    try wideColumn(out, mr, nr, true, raw_weights);
}
fn q8FourColumns(out: *Emitter, mr: usize) !void {
    try out.raw("while (o + prepared_q8_0_panel_nr <= out_end) : (o += prepared_q8_0_panel_nr) {\n");
    if (mr == 4) try accumulators(out, 1, "16") else try accumulators(out, mr, "4");
    try out.raw("for (0..row_blocks) |block_idx| {\n");
    try q8BlockViews(out, mr);
    try out.raw("inline for (0..projection_count) |projection_idx| {\nconst maybe_panel = panel_blocks[projection_idx];\nconst panel_off = ((o / prepared_q8_0_panel_nr) * row_blocks + block_idx) * prepared_q8_0_panel_nr * block_size;\n");
    for (0..4) |c| {
        try out.add("const w{d} = if (maybe_panel) |panel| panel[panel_off", .{c});
        if (c == 1) try out.raw(" + block_size ") else if (c != 0) try out.add(" + {d} * block_size ", .{c});
        try out.raw("..][0..block_size] else q8KQ8KWeightBlock(weight_raw[projection_idx], null, row_blocks, row_stride, o");
        if (c != 0) try out.add(" + {d}", .{c});
        try out.raw(", block_idx, false);\n");
    }
    try out.raw("const sums = if (use_fused_sdot)\n");
    var names: Names = .{};
    for ([_]bool{ true, false }) |fused| {
        try out.raw("dotQ8KQ8KBlock4");
        if (mr != 1) try out.add("MR{d}", .{mr});
        if (fused) try out.raw("FusedSdot");
        try out.raw("(");
        for (0..mr) |r| try out.add("{s}, ", .{names.block(mr, r)});
        try out.raw("w0, w1, w2, w3)");
        if (fused) try out.raw("\nelse\n") else try out.raw(";\n");
    }
    if (mr == 4) {
        try out.raw("inline for (0..16) |idx| acc[projection_idx][idx] += sums[idx];\n");
    } else {
        try out.raw("inline for (0..4) |lane| {\n");
        for (0..mr) |r| try out.add("{s}[projection_idx][lane] += sums[lane{s}];\n", .{ names.acc(mr, r), if (r == 0) "" else " + 4" });
        try out.raw("}\n");
    }
    try out.raw("}\n}\n");
    try publishPanel(out, mr, "4", .q8_four);
    try out.raw("}\n");
}

pub const K16GroupedOwner = K16Owner(false);
pub const Q3GroupedOwner = K16Owner(true);
fn K16Owner(comptime q3: bool) type {
    return struct {
        pub const name = if (q3) "k16PreparedQ8KActivationQ3NoDmnPanelGroupedApply" else "k16PreparedQ8KActivationGroupedApply";
        pub const begin = "// quant-kernel-codegen:begin CPU grouped projection " ++ name;
        pub const end = "// quant-kernel-codegen:end CPU grouped projection " ++ name;
        pub fn render(allocator: std.mem.Allocator) ![]u8 {
            var out = Emitter{ .allocator = allocator };
            defer out.bytes.deinit(allocator);
            try projectionPrefix(&out, name, if (q3) .q3 else .k16);
            for ([_]usize{ 4, 2, 1 }) |mr| {
                if (!q3 and mr == 4) try out.raw("if (panel8_blocks[0] == null) {\n");
                if (mr == 1) try out.raw("while (r < rows) : (r += 1) {\n") else if (q3) try out.add("while (r + {d} <= rows) : (r += {d}) {{\n", .{ mr, mr }) else try out.add("while (r + {d} < rows) : (r += {d}) {{\n", .{ mr - 1, mr });
                var names: Names = .{};
                for (0..mr) |r| try out.add("const {s} = q_input[{s} * input_row_stride ..][0..input_row_stride];\n", .{ names.row(mr, r), names.position(r) });
                if (!q3 and mr == 4) try k16PreparedSums(&out, mr, false);
                try out.raw("var o: usize = out_start;\n");
                const alignment = if (q3) "prepared_q3_k_panel_nr" else if (mr == 4) "prepared_q8_0_panel_nr" else "panel_align";
                if (!q3 and mr != 4) try out.raw("const panel_align: usize = if (panel8_blocks[0] != null) prepared_k_panel8_nr else prepared_q8_0_panel_nr;\n");
                try out.add("while (o < out_end and (o % {s}) != 0) : (o += 1) {{\n", .{alignment});
                try k16ScalarColumn(&out, mr, q3);
                try out.raw("}\n");
                if (!q3 and mr != 4) try k16Panel8Column(&out, mr);
                try k16Panel4Column(&out, mr, q3);
                try out.raw("while (o < out_end) : (o += 1) {\n");
                try k16ScalarColumn(&out, mr, q3);
                try out.raw("}\n}\n");
                if (!q3 and mr == 4) try out.raw("}\n");
            }
            try out.raw("}\n");
            return cpu.formatZig(allocator, out.bytes.items);
        }
    };
}
fn k16PreparedSums(out: *Emitter, mr: usize, static_blocks: bool) !void {
    try out.raw("const use_prepared_sums = has_min and k16PreparedQ8SumsEnabledFor(rows, out_dim, row_blocks) and row_blocks <= q8_k_prepared_sum_stack_max_row_blocks;\n");
    for (0..mr) |r| try out.add("var activation_sums{d}: [q8_k_prepared_sum_stack_max_row_blocks][16]f32 = undefined;\n", .{r});
    try out.raw("if (comptime has_min) {\nif (use_prepared_sums) {\n");
    if (static_blocks) try singleK16Blocks(out) else try out.raw("for (0..row_blocks) |block_idx| {\n");
    try out.raw("const off = block_idx * q8_block_size;\n");
    for (0..mr) |r| try out.add("prepareQ8KActivationSums16(q_input_row{d}[off..][0..q8_block_size], &activation_sums{d}[block_idx]);\n", .{ r, r });
    if (static_blocks) try out.raw("}\n");
    try out.raw("}\n}\n}\n");
}
fn k16BlockViews(out: *Emitter, mr: usize, q3: bool) !void {
    if (mr != 1 and (q3 or mr == 4)) {
        try out.add("const {s} = block_idx * q8_block_size;\n", .{if (q3) "q_off" else "off"});
    } else {
        var names: Names = .{};
        for (0..mr) |r| try out.add("const {s} = {s}[block_idx * q8_block_size ..][0..q8_block_size];\n", .{ names.block(mr, r), names.row(mr, r) });
    }
}
fn k16BlockArgument(out: *Emitter, mr: usize, r: usize, q3: bool) !void {
    var names: Names = .{};
    if (mr != 1 and (q3 or mr == 4)) try out.add("{s}[{s}..][0..q8_block_size]", .{ names.row(mr, r), if (q3) "q_off" else "off" }) else try out.raw(names.block(mr, r));
}
fn biasedScalarAccumulators(out: *Emitter, mr: usize) !void {
    var names: Names = .{};
    for (0..mr) |r| try out.add("var {s}: [projection_count]f32 = undefined;\n", .{names.acc(mr, r)});
    try out.raw("inline for (0..projection_count) |projection_idx| {\n");
    if (mr == 4) {
        try out.raw("const bias_value = optionalLinearBiasAt(biases[projection_idx], o);\n");
        for (0..mr) |r| try out.add("acc{d}[projection_idx] = bias_value;\n", .{r});
    } else {
        try out.add("{s}[projection_idx] = optionalLinearBiasAt(biases[projection_idx], o);\n", .{names.acc(mr, 0)});
        if (mr == 2) try out.raw("acc1[projection_idx] = acc0[projection_idx];\n");
    }
    try out.raw("}\n");
}
fn k16ScalarColumn(out: *Emitter, mr: usize, q3: bool) !void {
    try biasedScalarAccumulators(out, mr);
    try out.raw("for (0..row_blocks) |block_idx| {\n");
    try k16BlockViews(out, mr, q3);
    try out.raw("inline for (0..projection_count) |projection_idx| {\nconst prepared_block = prepared_blocks[projection_idx][o * prepared_row_stride + block_idx * prepared_k16_block_bytes ..][0..prepared_k16_block_bytes];\n");
    var names: Names = .{};
    for (0..mr) |r| {
        try out.add("{s}[projection_idx] += dotK16PreparedQ8KBlock({s}, ", .{ names.acc(mr, r), if (q3) "false" else "has_min" });
        try k16BlockArgument(out, mr, r, q3);
        try out.raw(", prepared_block);\n");
    }
    try out.raw("}\n}\ninline for (0..projection_count) |projection_idx| {\n");
    for (0..mr) |r| try out.add("outputs[projection_idx][{s} * out_dim + o] = {s}[projection_idx];\n", .{ names.position(r), names.acc(mr, r) });
    try out.raw("}\n");
}
fn k16Panel4Column(out: *Emitter, mr: usize, q3: bool) !void {
    const width = if (q3) "prepared_q3_k_panel_nr" else "prepared_q8_0_panel_nr";
    try out.add("while (o + {s} <= out_end) : (o += {s}) {{\n", .{ width, width });
    var names: Names = .{};
    const flat = q3 and mr != 1;
    if (flat) try out.add("var acc: [projection_count][{d}]f32 = undefined;\n", .{mr * 4}) else for (0..mr) |r| try out.add("var {s}: [projection_count][{s}]f32 = undefined;\n", .{ names.acc(mr, r), if (mr == 4 or q3) width else "4" });
    try out.raw("inline for (0..projection_count) |projection_idx| {\n");
    if (mr == 4 or q3) {
        try out.add("inline for (0..{s}) |lane| {{\n", .{width});
        if (mr != 1) try out.raw("const bias_value = optionalLinearBiasAt(biases[projection_idx], o + lane);\n");
        for (0..mr) |r| {
            try k16AccumulatorLane(out, mr, r, width, flat);
            try out.add(" = {s};\n", .{if (mr == 1) "optionalLinearBiasAt(biases[projection_idx], o + lane)" else "bias_value"});
        }
        try out.raw("}\n");
    } else {
        try out.add("{s}[projection_idx] = .{{\n", .{names.acc(mr, 0)});
        for (0..4) |c| {
            try out.raw("optionalLinearBiasAt(biases[projection_idx], o");
            if (c != 0) try out.add(" + {d}", .{c});
            try out.raw("),\n");
        }
        try out.raw("};\n");
        if (mr == 2) try out.raw("acc1[projection_idx] = acc0[projection_idx];\n");
    }
    try out.raw("}\nfor (0..row_blocks) |block_idx| {\n");
    try k16BlockViews(out, mr, q3);
    if (!q3 and mr == 4) for (0..mr) |r| try out.add("const sums{d} = if (use_prepared_sums) &activation_sums{d}[block_idx] else null;\n", .{ r, r });
    try out.raw("inline for (0..projection_count) |projection_idx| {\n");
    if (q3) {
        try out.raw("const panel_off = ((o / prepared_q3_k_panel_nr) * row_blocks + block_idx) * prepared_q3_k_panel_block_bytes;\nconst sums = dotQ3KNoDmnPanel4");
    } else try out.raw("const sums = dotK16PreparedQ8KBlock4");
    if (mr != 1) try out.add("MR{d}", .{mr});
    if (!q3 and mr == 4) try out.raw("WithSums");
    try out.raw("(\n");
    if (!q3) try out.raw("has_min,\n");
    for (0..mr) |r| {
        try k16BlockArgument(out, mr, r, q3);
        try out.raw(",\n");
        if (!q3 and mr == 4) try out.add("sums{d},\n", .{r});
    }
    if (q3) try out.raw("q3_panel_blocks[projection_idx][panel_off..][0..prepared_q3_k_panel_block_bytes],\n") else for (0..4) |c| try out.add("preparedK16PanelBlock(panel_blocks[projection_idx], prepared_blocks[projection_idx], row_blocks, prepared_row_stride, o, block_idx, {d}),\n", .{c});
    try out.raw(");\n");
    if (flat) {
        try out.add("inline for (0..{d}) |idx| acc[projection_idx][idx] += sums[idx];\n", .{mr * 4});
    } else if (q3) {
        try out.raw("inline for (0..prepared_q3_k_panel_nr) |lane| acc[projection_idx][lane] += sums[lane];\n");
    } else {
        try out.add("inline for (0..{s}) |lane| {{\n", .{if (mr == 4) width else "4"});
        for (0..mr) |r| {
            try k16AccumulatorLane(out, mr, r, width, false);
            try out.raw(" += sums[");
            if (mr == 4) try k16SymbolicLane(out, r, width) else try out.add("lane{s}", .{if (r == 0) "" else " + 4"});
            try out.raw("];\n");
        }
        try out.raw("}\n");
    }
    try out.raw("}\n}\ninline for (0..projection_count) |projection_idx| {\n");
    try k16OutputViews(out, mr);
    if (q3 and mr == 1) {
        try out.raw("inline for (0..prepared_q3_k_panel_nr) |lane| out_row[o + lane] = acc[projection_idx][lane];\n");
    } else {
        try out.add("inline for (0..{s}) |lane| {{\n", .{if (q3 or mr == 4) width else "4"});
        for (0..mr) |r| {
            if (mr == 1) try out.raw("out_row[o + lane] = ") else try out.add("out_row{d}[o + lane] = ", .{r});
            try k16AccumulatorLane(out, mr, r, width, flat);
            try out.raw(";\n");
        }
        try out.raw("}\n");
    }
    try out.raw("}\n}\n");
}
fn k16SymbolicLane(out: *Emitter, r: usize, width: []const u8) !void {
    if (r == 0) try out.raw("lane") else if (r == 1) try out.add("{s} + lane", .{width}) else try out.add("{d} * {s} + lane", .{ r, width });
}
fn k16AccumulatorLane(out: *Emitter, mr: usize, r: usize, width: []const u8, flat: bool) !void {
    var names: Names = .{};
    try out.add("{s}[projection_idx][", .{if (flat) "acc" else names.acc(mr, r)});
    if (flat) try k16SymbolicLane(out, r, width) else try out.raw("lane");
    try out.raw("]");
}
fn k16OutputViews(out: *Emitter, mr: usize) !void {
    var names: Names = .{};
    for (0..mr) |r| {
        if (mr == 1) try out.raw("const out_row = outputs[projection_idx][r * out_dim ..][0..out_dim];\n") else try out.add("const out_row{d} = outputs[projection_idx][{s} * out_dim ..][0..out_dim];\n", .{ r, names.position(r) });
    }
}
fn k16Panel8Column(out: *Emitter, mr: usize) !void {
    try out.raw("if (panel8_blocks[0] != null) {\nwhile (o + prepared_k_panel8_nr <= out_end) : (o += prepared_k_panel8_nr) {\n");
    var names: Names = .{};
    for (0..mr) |r| try out.add("var {s}: [projection_count]F32x8 = undefined;\n", .{names.acc(mr, r)});
    try out.raw("inline for (0..projection_count) |projection_idx| {\n");
    try out.add("{s}[projection_idx] = optionalLinearBiasVec8(biases[projection_idx], o);\n", .{names.acc(mr, 0)});
    if (mr == 2) try out.raw("acc1[projection_idx] = acc0[projection_idx];\n");
    try out.raw("}\nfor (0..row_blocks) |block_idx| {\n");
    try k16BlockViews(out, mr, false);
    try out.raw("inline for (0..projection_count) |projection_idx| {\nif (panel8_blocks[projection_idx]) |panel8| {\nconst panel_off = ((o / prepared_k_panel8_nr) * row_blocks + block_idx) * prepared_k16_panel8_block_bytes;\n");
    if (mr == 1) try out.raw("acc[projection_idx] += dotK16PreparedQ8KPanel8Vec(has_min, q, panel8[panel_off..][0..prepared_k16_panel8_block_bytes]);\n") else {
        try out.raw("const sums = dotK16PreparedQ8KPanel8MR2Vec(has_min, q0, q1, panel8[panel_off..][0..prepared_k16_panel8_block_bytes]);\n");
        for (0..mr) |r| try out.add("acc{d}[projection_idx] += sums.r{d};\n", .{ r, r });
    }
    try out.raw("}\n}\n}\ninline for (0..projection_count) |projection_idx| {\n");
    try k16OutputViews(out, mr);
    for (0..mr) |r| {
        if (mr == 1) try out.raw("storeF32x8(out_row, o, acc[projection_idx]);\n") else try out.add("storeF32x8(out_row{d}, o, acc{d}[projection_idx]);\n", .{ r, r });
    }
    try out.raw("}\n}\n}\n");
}

/// Single-projection K16 geometry retains scalar registers and static K=3
/// traversal. The grouped schedule shares prepared activation sums.
pub const SingleK16Rows = struct {
    pub const name = "K16 single projection rows";
    pub const begin = "    // quant-kernel-codegen:begin CPU K16 single projection rows";
    pub const end = "    // quant-kernel-codegen:end CPU K16 single projection rows";
    pub fn render(allocator: std.mem.Allocator) ![]u8 {
        var out = Emitter{ .allocator = allocator };
        defer out.bytes.deinit(allocator);
        try out.raw("var r: usize = 0;\n");
        for ([_]usize{ 4, 4, 2, 1 }, 0..) |mr, schedule| {
            const vector_four = schedule == 1;
            if (mr == 4) try out.raw(if (vector_four) "if (panel8_blocks) |panel8| {\n" else "if (panel8_blocks == null) {\n");
            try out.raw(if (mr == 4) "while (r + 4 <= rows) : (r += 4) {\n" else if (mr == 2) "while (if (static_three) r + 2 <= rows else r + 1 < rows) : (r += 2) {\n" else "while (r < rows) : (r += 1) {\n");
            try singleK16RowViews(&out, mr);
            if (mr == 4 and !vector_four) try k16PreparedSums(&out, mr, true);
            try out.raw("var o: usize = out_start;\n");
            const alignment = if (mr != 4) "panel_align" else if (vector_four) "prepared_k_panel8_nr" else "prepared_q8_0_panel_nr";
            if (mr != 4) try out.raw("const panel_align: usize = if (panel8_blocks != null) prepared_k_panel8_nr else prepared_q8_0_panel_nr;\n");
            try out.add("while (o < out_end and (o % {s}) != 0) : (o += 1) {{\n", .{alignment});
            try singleK16Scalar(&out, mr, false);
            try out.raw("}\n");
            if (mr != 4 or vector_four) {
                if (mr != 4) try out.raw("if (panel8_blocks) |panel8| {\n");
                try singleK16Panel(&out, mr, true);
                if (mr != 4) try out.raw("}\n");
            }
            if (!vector_four) try singleK16Panel(&out, mr, false);
            try out.raw("while (o < out_end) : (o += 1) {\n");
            try singleK16Scalar(&out, mr, false);
            try out.raw("}\n}\n");
            if (mr == 4) try out.raw("}\n");
        }
        return cpu.formatStatements(allocator, out.bytes.items, 4);
    }
};
fn singleK16Blocks(out: *Emitter) !void {
    try out.raw("for (0..(if (static_three) 1 else row_blocks)) |runtime_block| {\ninline for (0..(if (static_three) 3 else 1)) |static_block| {\nconst block_idx = if (static_three) static_block else runtime_block;\n");
}
fn singleK16Scalar(out: *Emitter, mr: usize, wide_panel: bool) !void {
    var names: Names = .{};
    for (0..mr) |r| {
        if (wide_panel) try out.add("var {s}: f32 = 0.0;\n", .{names.acc(mr, r)}) else try out.add("var {s} = {s};\n", .{ names.acc(mr, r), if (r == 0) "optionalLinearBiasAt(bias, o)" else "acc0" });
    }
    if (wide_panel) try out.raw("for (0..row_blocks) |block_idx| {\n") else try singleK16Blocks(out);
    if (wide_panel and mr == 4) try out.raw("const q_off = block_idx * q8_block_size;\n");
    if (mr != 1) try out.raw("const prepared_block = prepared_blocks[o * prepared_row_stride + block_idx * prepared_k16_block_bytes ..][0..prepared_k16_block_bytes];\n");
    if (mr == 4 and !wide_panel) try out.raw("const off = block_idx * q8_block_size;\n");
    for (0..mr) |r| {
        if (mr == 1) try out.raw("acc += dotK16PreparedQ8KBlock(\nhas_min,\nq_input_row[block_idx * q8_block_size ..][0..q8_block_size],\nprepared_blocks[o * prepared_row_stride + block_idx * prepared_k16_block_bytes ..][0..prepared_k16_block_bytes],\n);\n") else try out.add("{s} += dotK16PreparedQ8KBlock(has_min, {s}[{s}..][0..q8_block_size], prepared_block);\n", .{ names.acc(mr, r), names.row(mr, r), if (mr == 4) (if (wide_panel) "q_off" else "off") else "block_idx * q8_block_size " });
    }
    try out.raw(if (wide_panel) "}\n" else "}\n}\n");
    for (0..mr) |r| {
        var buf: [32]u8 = undefined;
        try out.add("{s}[o] = {s};\n", .{ cpu.rowName(&buf, "out_row", mr, r), names.acc(mr, r) });
    }
}
fn singleK16PanelAcc(out: *Emitter, mr: usize, r: usize, c: usize) !void {
    if (mr == 1) try out.add("acc{d}", .{c}) else try out.add("acc{d}{d}", .{ r, c });
}
fn singleK16Panel(out: *Emitter, mr: usize, vector: bool) !void {
    const width = if (vector) "prepared_k_panel8_nr" else "prepared_q8_0_panel_nr";
    try out.add("while (o + {s} <= out_end) : (o += {s}) {{\n", .{ width, width });
    var names: Names = .{};
    for (0..mr) |r| {
        if (vector) {
            try out.add("var {s} = {s};\n", .{ names.acc(mr, r), if (r == 0) "optionalLinearBiasVec8(bias, o)" else "acc0" });
        } else for (0..4) |c| {
            try out.raw("var ");
            try singleK16PanelAcc(out, mr, r, c);
            try out.raw(" = ");
            if (r == 0) {
                try out.raw("optionalLinearBiasAt(bias, o");
                if (c != 0) try out.add(" + {d}", .{c});
                try out.raw(")");
            } else try singleK16PanelAcc(out, mr, 0, c);
            try out.raw(";\n");
        }
    }
    try singleK16Blocks(out);
    if (vector or mr == 4) try out.raw("const off = block_idx * q8_block_size;\n");
    if (vector) try out.raw("const panel_off = ((o / prepared_k_panel8_nr) * row_blocks + block_idx) * prepared_k16_panel8_block_bytes;\n") else if (mr == 4) for (0..mr) |r| try out.add("const sums{d} = if (use_prepared_sums) &activation_sums{d}[block_idx] else null;\n", .{ r, r });
    try out.add("const sums = dotK16PreparedQ8K{s}", .{if (vector) "Panel8" else "Block4"});
    if (mr != 1) try out.add("MR{d}", .{mr});
    if (vector) try out.raw("Vec") else if (mr == 4) try out.raw("WithSums");
    try out.raw("(\nhas_min,\n");
    for (0..mr) |r| {
        try out.add("{s}[{s}..][0..q8_block_size],\n", .{ names.row(mr, r), if (vector or mr == 4) "off" else "block_idx * q8_block_size " });
        if (!vector and mr == 4) try out.add("sums{d},\n", .{r});
    }
    if (vector) try out.raw("panel8[panel_off..][0..prepared_k16_panel8_block_bytes],\n") else for (0..4) |c| try out.add("preparedK16PanelBlock(panel_blocks, prepared_blocks, row_blocks, prepared_row_stride, o, block_idx, {d}),\n", .{c});
    try out.raw(");\n");
    for (0..mr) |r| {
        if (vector) {
            try out.add("{s} += sums", .{names.acc(mr, r)});
            if (mr != 1) try out.add(".r{d}", .{r});
            try out.raw(";\n");
        } else for (0..4) |c| {
            try singleK16PanelAcc(out, mr, r, c);
            try out.add(" += sums[{d}];\n", .{r * 4 + c});
        }
    }
    try out.raw("}\n}\n");
    for (0..mr) |r| {
        var buf: [32]u8 = undefined;
        const row = cpu.rowName(&buf, "out_row", mr, r);
        if (vector) try out.add("storeF32x8({s}, o, {s});\n", .{ row, names.acc(mr, r) }) else for (0..4) |c| {
            try out.add("{s}[o", .{row});
            if (c != 0) try out.add(" + {d}", .{c});
            try out.raw("] = ");
            try singleK16PanelAcc(out, mr, r, c);
            try out.raw(";\n");
        }
    }
    try out.raw("}\n");
}

pub const SingleK16Panel16Rows = struct {
    pub const name = "K16 single panel16 projection rows";
    pub const begin = "    // quant-kernel-codegen:begin CPU K16 single panel16 projection rows";
    pub const end = "    // quant-kernel-codegen:end CPU K16 single panel16 projection rows";
    pub fn render(allocator: std.mem.Allocator) ![]u8 {
        var out = Emitter{ .allocator = allocator };
        defer out.bytes.deinit(allocator);
        try out.raw("var r: usize = 0;\n");
        for ([_]usize{ 4, 2, 1 }) |mr| {
            if (mr == 1) try out.raw("while (r < rows) : (r += 1) {\n") else try out.add("while (r + {d} <= rows) : (r += {d}) {{\n", .{ mr, mr });
            try singleK16RowViews(&out, mr);
            try out.raw("var o: usize = out_start;\nwhile (o < out_end and (o % prepared_k_panel16_nr) != 0) : (o += 1) {\n");
            try singleK16Scalar(&out, mr, true);
            try out.raw("}\nwhile (o + prepared_k_panel16_nr <= out_end) : (o += prepared_k_panel16_nr) {\n");
            var names: Names = .{};
            for (0..mr) |r| for ([_][]const u8{ "lo", "hi" }) |half| try out.add("var {s}_{s}: F32x8 = @splat(0.0);\n", .{ names.acc(mr, r), half });
            try out.raw("for (0..row_blocks) |block_idx| {\nconst q_off = block_idx * q8_block_size;\nconst panel_off = ((o / prepared_k_panel16_nr) * row_blocks + block_idx) * prepared_k16_panel16_block_bytes;\nconst panel_lo = panel16_blocks[panel_off..][0..prepared_k16_panel8_block_bytes];\nconst panel_hi = panel16_blocks[panel_off + prepared_k16_panel8_block_bytes ..][0..prepared_k16_panel8_block_bytes];\n");
            for (0..mr) |r| try out.add("const {s} = {s}[q_off..][0..q8_block_size];\n", .{ names.block(mr, r), names.row(mr, r) });
            for ([_][]const u8{ "lo", "hi" }) |half| {
                if (mr == 1) try out.add("acc_{s} += ", .{half}) else try out.add("const sums_{s} = ", .{half});
                try out.raw("dotK16PreparedQ8KPanel8");
                if (mr != 1) try out.add("MR{d}", .{mr});
                try out.raw("Vec(has_min, ");
                for (0..mr) |r| try out.add("{s}, ", .{names.block(mr, r)});
                try out.add("panel_{s});\n", .{half});
            }
            if (mr != 1) for (0..mr) |r| for ([_][]const u8{ "lo", "hi" }) |half| try out.add("acc{d}_{s} += sums_{s}.r{d};\n", .{ r, half, half, r });
            try out.raw("}\n");
            for (0..mr) |r| {
                var buf: [32]u8 = undefined;
                const row = cpu.rowName(&buf, "out_row", mr, r);
                try out.add("storeF32x8({s}, o, {s}_lo);\nstoreF32x8({s}, o + prepared_k_panel8_nr, {s}_hi);\n", .{ row, names.acc(mr, r), row, names.acc(mr, r) });
            }
            try out.raw("}\nwhile (o < out_end) : (o += 1) {\n");
            try singleK16Scalar(&out, mr, true);
            try out.raw("}\n}\n");
        }
        return cpu.formatStatements(allocator, out.bytes.items, 4);
    }
};
fn singleK16RowViews(out: *Emitter, mr: usize) !void {
    var names: Names = .{};
    for (0..mr) |r| try out.add("const {s} = q_input[{s} * input_row_stride ..][0..input_row_stride];\n", .{ names.row(mr, r), names.position(r) });
    for (0..mr) |r| {
        var buf: [32]u8 = undefined;
        try out.add("const {s} = output[{s} * out_dim ..][0..out_dim];\n", .{ cpu.rowName(&buf, "out_row", mr, r), names.position(r) });
    }
}
