// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Static integer-dot tile lowering. Expand row/column ownership into the
//! qualified scalar register idioms before Zig optimization. This shares the
//! algorithm without requiring LLVM to recover those idioms from local arrays.
const std = @import("std");
const native_source = @import("quant_kernel_cpu_source.zig");

const Result = enum { tuple, array, row_vectors, vector };
const Tile = struct {
    name: []const u8,
    rows: usize,
    columns: usize,
    sdot: bool,
    result: Result,
    bytes: usize = 32,
    native_inline: bool = false,
    contiguous: bool = false,
};

pub const owners = .{
    Owner(.{ .name = "dotI8I8Block8MR2RowsContiguousVecAarch64Sdot", .rows = 2, .columns = 8, .sdot = true, .result = .row_vectors, .contiguous = true }),
    Owner(.{ .name = "dotI8I8Block4_16", .rows = 1, .columns = 4, .sdot = false, .result = .tuple, .bytes = 16 }),
    Owner(.{ .name = "dotI8I8Block4MR2_16", .rows = 2, .columns = 4, .sdot = false, .result = .tuple, .bytes = 16 }),
    Owner(.{ .name = "dotI8I8Block8_16Vec", .rows = 1, .columns = 8, .sdot = false, .result = .vector, .bytes = 16, .native_inline = true }),
    Owner(.{ .name = "dotI8I8Block4Portable", .rows = 1, .columns = 4, .sdot = false, .result = .tuple }),
    Owner(.{ .name = "dotI8I8Block4Aarch64Sdot", .rows = 1, .columns = 4, .sdot = true, .result = .tuple }),
    Owner(.{ .name = "dotI8I8Block4MR2Portable", .rows = 2, .columns = 4, .sdot = false, .result = .tuple }),
    Owner(.{ .name = "dotI8I8Block4MR2Aarch64Sdot", .rows = 2, .columns = 4, .sdot = true, .result = .tuple }),
    Owner(.{ .name = "dotI8I8Block8MR2Aarch64Sdot", .rows = 2, .columns = 8, .sdot = true, .result = .array }),
    Owner(.{ .name = "dotI8I8Block4MR4Aarch64Sdot", .rows = 4, .columns = 4, .sdot = true, .result = .array }),
    Owner(.{ .name = "dotI8I8Block8MR2RowsVecAarch64Sdot", .rows = 2, .columns = 8, .sdot = true, .result = .row_vectors }),
};

fn Owner(comptime tile: Tile) type {
    return struct {
        pub const name = tile.name;
        pub const begin = "// quant-kernel-codegen:begin CPU integer dot " ++ tile.name;
        pub const end = "// quant-kernel-codegen:end CPU integer dot " ++ tile.name;
        pub fn render(allocator: std.mem.Allocator) ![]u8 {
            return renderTile(allocator, tile);
        }
    };
}

const Emitter = native_source.Emitter;

fn accumulator(buffer: *[32]u8, tile: Tile, row: usize, col: usize) []const u8 {
    return if (tile.rows == 1)
        std.fmt.bufPrint(buffer, "acc{d}", .{col}) catch unreachable
    else
        std.fmt.bufPrint(buffer, "acc{d}{d}", .{ row, col }) catch unreachable;
}

fn renderTile(allocator: std.mem.Allocator, tile: Tile) ![]u8 {
    var out = Emitter{ .allocator = allocator };
    defer out.bytes.deinit(allocator);
    const step = if (tile.sdot) "16" else "VEC_LEN";
    const vector = if (tile.sdot) "I32x4" else "I32xN";
    const raw_vector = if (tile.sdot) "U8x16" else "U8xN";
    if (tile.native_inline) try out.raw("inline ");
    try out.add("fn {s}(\n", .{tile.name});
    for (0..tile.rows) |r| {
        if (tile.rows == 1) try out.add("q_input: *const [{d}]u8,\n", .{tile.bytes}) else try out.add("q_input_r{d}: *const [{d}]u8,\n", .{ r, tile.bytes });
    }
    if (tile.contiguous) try out.raw("values: []const u8,\n") else for (0..tile.columns) |c| try out.add("q_weight{d}: *const [{d}]u8,\n", .{ c, tile.bytes });
    switch (tile.result) {
        .tuple => {
            try out.add(") struct {{", .{});
            for (0..tile.rows * tile.columns) |i| try out.add("{s}i32", .{if (i == 0) "" else ", "});
            try out.add("}} {{\n", .{});
        },
        .array => try out.add(") [{d}]i32 {{\n", .{tile.rows * tile.columns}),
        .row_vectors => try out.add(") DotI8I8Block8MR2RowsVec {{\n", .{}),
        .vector => try out.raw(") I32x8 {\n"),
    }
    if (tile.contiguous) try out.raw("std.debug.assert(values.len >= 8 * 32);\n");
    var buf: [32]u8 = undefined;
    if (tile.bytes == 16) try directSdot16(&out, tile);
    for (0..tile.rows) |r| for (0..tile.columns) |c| {
        try out.add("var {s}: {s} = @splat(0);\n", .{ accumulator(&buf, tile, r, c), vector });
    };
    try out.add("comptime var i: usize = 0;\ninline while (i < {d}) : (i += {s}) {{\n", .{ tile.bytes, step });
    for (0..tile.rows) |r| {
        if (tile.rows == 1) try out.add("const raw_x: {s} = q_input[i..][0..{s}].*;\n", .{ raw_vector, step }) else try out.add("const raw_x{d}: {s} = q_input_r{d}[i..][0..{s}].*;\n", .{ r, raw_vector, r, step });
    }
    if (!tile.sdot) {
        for (0..tile.rows) |r| {
            if (tile.rows == 1) try out.add("const x: I32xN = @as(@Vector(VEC_LEN, i8), @bitCast(raw_x));\n", .{}) else try out.add("const x{d}: I32xN = @as(@Vector(VEC_LEN, i8), @bitCast(raw_x{d}));\n", .{ r, r });
        }
    }
    // The one-row SDOT schedule consumes weight loads directly. Multi-row
    // schedules acquire all weights before the row-major accumulator updates.
    if (tile.bytes == 32 and (!tile.sdot or tile.rows != 1)) {
        for (0..tile.columns) |c| {
            if (tile.contiguous) {
                try out.add("const raw_w{d}: U8x16 = values[", .{c});
                if (c == 1) try out.raw("32 + ") else if (c != 0) try out.add("{d} * 32 + ", .{c});
                try out.raw(if (c == 0) "i..][0..16].*;\n" else "i ..][0..16].*;\n");
            } else try out.add("const raw_w{d}: {s} = q_weight{d}[i..][0..{s}].*;\n", .{ c, raw_vector, c, step });
        }
    }
    if (!tile.sdot) {
        for (0..tile.columns) |c| {
            if (tile.bytes == 32) try out.add("const w{d}: I32xN = @as(@Vector(VEC_LEN, i8), @bitCast(raw_w{d}));\n", .{ c, c }) else if (tile.columns == 4) try out.add("const w{d}: I32xN = @as(@Vector(VEC_LEN, i8), @bitCast(q_weight{d}[i..][0..VEC_LEN].*));\n", .{ c, c });
        }
    }
    for (0..tile.rows) |r| for (0..tile.columns) |c| {
        const acc = accumulator(&buf, tile, r, c);
        if (tile.sdot) {
            if (tile.rows == 1) try out.add("{s} = aarch64Sdot16({s}, raw_x, q_weight{d}[i..][0..16].*);\n", .{ acc, acc, c }) else try out.add("{s} = aarch64Sdot16({s}, raw_x{d}, raw_w{d});\n", .{ acc, acc, r, c });
        } else {
            if (tile.bytes == 16 and tile.columns == 8) try out.add("{s} += x * @as(I32xN, @as(@Vector(VEC_LEN, i8), @bitCast(q_weight{d}[i..][0..VEC_LEN].*)));\n", .{ acc, c }) else if (tile.rows == 1) try out.add("{s} += x * w{d};\n", .{ acc, c }) else try out.add("{s} += x{d} * w{d};\n", .{ acc, r, c });
        }
    };
    try out.raw("}\n");
    try tileReturn(&out, tile);
    try out.raw("}\n");
    return formatSource(allocator, out.bytes.items);
}

test "CPU tile renderer parses all native result shapes without leaking" {
    inline for (owners) |OwnerType| {
        const source = try OwnerType.render(std.testing.allocator);
        defer std.testing.allocator.free(source);
    }
    const q6 = try Q6ProjectionOwner.render(std.testing.allocator);
    defer std.testing.allocator.free(q6);
    const q8k = try Q8KProjectionOwner.render(std.testing.allocator);
    defer std.testing.allocator.free(q8k);
    inline for (q4_triple_owners) |OwnerType| {
        const q4 = try OwnerType.render(std.testing.allocator);
        defer std.testing.allocator.free(q4);
    }
}

/// Prepared-Q6 traversal has one row/column schedule. The emitted forms keep
/// each existing dot leaf and scalar accumulator identifier/order intact.
pub const Q6ProjectionOwner = struct {
    pub const begin = "// quant-kernel-codegen:begin CPU Q6 projection rows";
    pub const end = "// quant-kernel-codegen:end CPU Q6 projection rows";
    pub fn render(allocator: std.mem.Allocator) ![]u8 {
        var out = Emitter{ .allocator = allocator };
        defer out.bytes.deinit(allocator);
        try out.add(
            \\fn linearQ6KPreparedQ8KActivationRange(
            \\q_input: []const u8, prepared_blocks: []const u8,
            \\panel_blocks: ?[]const u8, panel8_blocks: ?[]const u8,
            \\panel16_blocks: ?[]const u8, output: []f32, rows: usize,
            \\in_dim: usize, out_dim: usize, out_start: usize, out_end: usize,
            \\) void {{
            \\@setEvalBranchQuota(20_000);
            \\const values_per_block: usize = 256;
            \\const q8_block_size: usize = 292;
            \\std.debug.assert(in_dim % values_per_block == 0);
            \\const row_blocks = in_dim / values_per_block;
            \\const input_row_stride = row_blocks * q8_block_size;
            \\const prepared_row_stride = row_blocks * prepared_q4_k_block_bytes;
            \\const panel_align: usize = if (panel16_blocks != null) prepared_k_panel16_nr else if (panel8_blocks != null) prepared_k_panel8_nr else prepared_q8_0_panel_nr;
            \\var r: usize = 0;
            \\
        , .{});
        for ([_]usize{ 4, 2, 1 }) |mr| {
            if (mr == 1) try out.add("while (r < rows) : (r += 1) {{\n", .{}) else try out.add("while (r + {d} <= rows) : (r += {d}) {{\n", .{ mr, mr });
            try kRowViews(&out, mr, "input_row_stride");
            try out.add("var o: usize = out_start;\nwhile (o < out_end and (o % panel_align) != 0) : (o += 1) {{\n", .{});
            try q6ScalarColumn(&out, mr);
            try out.add("}}\n", .{});
            for ([_]usize{ 16, 8 }) |nr| try kPanel(&out, mr, nr, false);
            try out.add("while (o + 4 <= out_dim) : (o += 4) {{\nif (o + 4 > out_end) break;\n", .{});
            try q6FourColumns(&out, mr);
            try out.add("}}\nwhile (o < out_end) : (o += 1) {{\n", .{});
            try q6ScalarColumn(&out, mr);
            try out.add("}}\n}}\n", .{});
        }
        try out.add("}}\n", .{});
        return formatSource(allocator, out.bytes.items);
    }
};

const rowName = native_source.rowName;

fn q6ScalarColumn(out: *Emitter, mr: usize) !void {
    try out.add("const prepared_row = prepared_blocks[o * prepared_row_stride ..][0..prepared_row_stride];\n", .{});
    var buf: [32]u8 = undefined;
    for (0..mr) |r| try out.add("var {s}: f32 = 0.0;\n", .{rowName(&buf, "acc", mr, r)});
    try out.add("for (0..row_blocks) |block_idx| {{\n", .{});
    if (mr != 1) try out.add("const q8_off = block_idx * q8_block_size;\nconst prepared_off = block_idx * prepared_q4_k_block_bytes;\n", .{});
    if (mr == 4) try out.add("const prepared_block = prepared_row[prepared_off..][0..prepared_q4_k_block_bytes];\n", .{});
    for (0..mr) |r| {
        if (mr == 1) {
            try out.add("acc += dotQ6KPreparedQ8KBlock(q_input_row[block_idx * q8_block_size ..][0..q8_block_size], prepared_row[block_idx * prepared_q4_k_block_bytes ..][0..prepared_q4_k_block_bytes],);\n", .{});
        } else if (mr == 4) {
            try out.add("acc{d} += dotQ6KPreparedQ8KBlock(q_input_row{d}[q8_off..][0..q8_block_size], prepared_block);\n", .{ r, r });
        } else {
            try out.add("acc{d} += dotQ6KPreparedQ8KBlock(q_input_row{d}[q8_off..][0..q8_block_size], prepared_row[prepared_off..][0..prepared_q4_k_block_bytes],);\n", .{ r, r });
        }
    }
    try out.add("}}\n", .{});
    for (0..mr) |r| {
        if (mr == 1) try out.add("out_row[o] = acc;\n", .{}) else try out.add("out_row{d}[o] = acc{d};\n", .{ r, r });
    }
}

fn kPanel(out: *Emitter, mr: usize, nr: usize, q8: bool) !void {
    const width = if (nr == 16) "prepared_k_panel16_nr" else "prepared_k_panel8_nr";
    const bytes = if (q8) (if (nr == 16) "prepared_q8_k_panel16_block_bytes" else "prepared_q8_k_panel8_block_bytes") else (if (nr == 16) "prepared_k_panel16_block_bytes" else "prepared_q6_k_panel8_block_bytes");
    const index = if (q8) "lane" else "idx";
    const offset = if (q8) "off" else "q8_off";
    const block = if (q8) "block_size" else "q8_block_size";
    try out.add("if (panel{d}_blocks) |panel{d}| {{\n", .{ nr, nr });
    if (q8 and nr == 8) try out.add("while (o + {s} <= out_end) : (o += {s}) {{\n", .{ width, width }) else try out.add("while ((o % {s}) == 0 and o + {s} <= out_end) : (o += {s}) {{\n", .{ width, width, width });
    try kAccumulators(out, mr, width);
    try out.add("for (0..row_blocks) |block_idx| {{\nconst {s} = block_idx * {s};\nconst panel_off = ((o / {s}) * row_blocks + block_idx) * {s};\n", .{ offset, block, width, bytes });
    if (!q8 and mr == 2 and nr == 8) try out.add("const panel_block = panel8[panel_off..][0..prepared_q6_k_panel8_block_bytes];\n", .{});
    try out.add("const sums = {s}{d}", .{ if (q8) "dotQ8KQ8KBlock" else "dotQ6KPreparedQ8KPanel", nr });
    if (mr != 1) try out.add("MR{d}", .{mr});
    try out.add("(", .{});
    var buf: [32]u8 = undefined;
    if (!q8 or mr != 1) try out.add("\n", .{});
    for (0..mr) |r| try out.add("{s}[{s}..][0..{s}],{s}", .{ rowName(&buf, "q_input_row", mr, r), offset, block, if (q8 and mr == 1) " " else "\n" });
    if (!q8 and mr == 2 and nr == 8) try out.add("panel_block,\n);\n", .{}) else if (q8 and mr == 1) try out.add("panel{d}[panel_off..][0..{s}]);\n", .{ nr, bytes }) else try out.add("panel{d}[panel_off..][0..{s}],\n);\n", .{ nr, bytes });
    try kAccumulateRows(out, mr, width, index);
    try out.add("}}\n", .{});
    try kPublishRows(out, mr, width, index);
    try out.add("}}\n}}\n", .{});
}

fn kAccumulators(out: *Emitter, mr: usize, width: []const u8) !void {
    var buf: [32]u8 = undefined;
    for (0..mr) |r| try out.add("var {s}: [{s}]f32 = @as([{s}]f32, @splat(0.0));\n", .{ rowName(&buf, "acc", mr, r), width, width });
}

fn kAccumulateRows(out: *Emitter, mr: usize, width: []const u8, index: []const u8) !void {
    if (mr == 1) {
        try out.add("inline for (0..{s}) |{s}| acc[{s}] += sums[{s}];\n", .{ width, index, index, index });
    } else {
        try out.add("inline for (0..{s}) |{s}| {{\n", .{ width, index });
        for (0..mr) |r| try out.add("acc{d}[{s}] += sums.r{d}[{s}];\n", .{ r, index, r, index });
        try out.add("}}\n", .{});
    }
}

fn kPublishRows(out: *Emitter, mr: usize, width: []const u8, index: []const u8) !void {
    if (mr == 1) {
        try out.add("inline for (0..{s}) |{s}| out_row[o + {s}] = acc[{s}];\n", .{ width, index, index, index });
    } else {
        try out.add("inline for (0..{s}) |{s}| {{\n", .{ width, index });
        for (0..mr) |r| try out.add("out_row{d}[o + {s}] = acc{d}[{s}];\n", .{ r, index, r, index });
        try out.add("}}\n", .{});
    }
}

fn q6FourColumns(out: *Emitter, mr: usize) !void {
    for (0..mr) |r| for (0..4) |c| {
        if (mr == 1) try out.add("var acc{d}: f32 = 0.0;\n", .{c}) else try out.add("var acc_r{d}_{d}: f32 = 0.0;\n", .{ r, c });
    };
    try out.add("for (0..row_blocks) |block_idx| {{\nconst q8_off = block_idx * q8_block_size;\n", .{});
    for (0..@max(1, mr / 2)) |pair| {
        const sum = if (mr == 4) (if (pair == 0) "sums01" else "sums23") else "sums";
        if (mr == 1) try out.add("const {s} = dotQ6KPreparedQ8KBlock4(\n", .{sum}) else try out.add("const {s} = dotPreparedKQ8KBlock4MR2(\ntrue,\n", .{sum});
        for (0..@min(mr, 2)) |r| {
            var buf: [32]u8 = undefined;
            try out.add("{s}[q8_off..][0..q8_block_size],\n", .{rowName(&buf, "q_input_row", mr, pair * 2 + r)});
        }
        for (0..4) |c| try out.add("preparedKPanelBlock(panel_blocks, prepared_blocks, row_blocks, prepared_row_stride, o, block_idx, {d}),\n", .{c});
        try out.add(");\n", .{});
    }
    for (0..mr) |r| for (0..4) |c| {
        const sum = if (mr == 4) (if (r < 2) "sums01" else "sums23") else "sums";
        if (mr == 1) try out.add("acc{d} += sums[{d}];\n", .{ c, c }) else try out.add("acc_r{d}_{d} += {s}[{d}];\n", .{ r, c, sum, (r % 2) * 4 + c });
    };
    try out.add("}}\n", .{});
    for (0..mr) |r| for (0..4) |c| {
        var buf: [32]u8 = undefined;
        const output = rowName(&buf, "out_row", mr, r);
        if (mr == 1) {
            if (c == 0) try out.add("{s}[o] = acc0;\n", .{output}) else try out.add("{s}[o + {d}] = acc{d};\n", .{ output, c, c });
        } else {
            if (c == 0) try out.add("{s}[o] = acc_r{d}_0;\n", .{ output, r }) else try out.add("{s}[o + {d}] = acc_r{d}_{d};\n", .{ output, c, r, c });
        }
    };
}

pub const formatSource = native_source.formatZig;

pub const q4_triple_owners = .{ Q4TripleOwner(1), Q4TripleOwner(2) };
fn Q4TripleOwner(comptime rows: usize) type {
    return struct {
        pub const name = if (rows == 1) "processQ4KTripleRowSingle" else "processQ4KTripleRowPair";
        pub const begin = "// quant-kernel-codegen:begin CPU Q4 triple " ++ name;
        pub const end = "// quant-kernel-codegen:end CPU Q4 triple " ++ name;
        pub fn render(allocator: std.mem.Allocator) ![]u8 {
            var out = Emitter{ .allocator = allocator };
            defer out.bytes.deinit(allocator);
            try out.add("fn {s}(input: []const f32,\n", .{name});
            for ("abc") |p| try out.add("prepared_blocks_{c}: []const u8,\n", .{p});
            for ("abc") |p| try out.add("bias_{c}: ?[]const f32,\n", .{p});
            for ("abc") |p| try out.add("output_{c}: []f32,\n", .{p});
            try out.add("r: usize, in_dim: usize, out_dim: usize, out_start: usize, out_end: usize, prepared_row_stride: usize, row_blocks: usize, values_per_block: usize,\n) void {{\n", .{});
            if (rows == 1) {
                try out.add("const input_row = input[r * in_dim ..][0..in_dim];\n", .{});
            } else {
                try out.add("const input_r0 = input[r * in_dim ..][0..in_dim];\nconst input_r1 = input[(r + 1) * in_dim ..][0..in_dim];\n", .{});
            }
            for ("abc") |p| for (0..rows) |r| {
                if (rows == 1) try out.add("const out_{c} = output_{c}[r * out_dim ..][0..out_dim];\n", .{ p, p }) else if (r == 0) try out.add("const out_{c}_r0 = output_{c}[r * out_dim ..][0..out_dim];\n", .{ p, p }) else try out.add("const out_{c}_r1 = output_{c}[(r + 1) * out_dim ..][0..out_dim];\n", .{ p, p });
            };
            try out.add("var o: usize = out_start;\nwhile (o < out_end and (o % 4) != 0) : (o += 1) {{\n", .{});
            try q4TripleScalar(&out, rows);
            try out.add("}}\nwhile (o + 4 <= out_dim) : (o += 4) {{\nif (o + 4 > out_end) break;\n", .{});
            try q4TripleVector(&out, rows);
            try out.add("}}\nwhile (o < out_end) : (o += 1) {{\n", .{});
            try q4TripleScalar(&out, rows);
            try out.add("}}\n}}\n", .{});
            return formatSource(allocator, out.bytes.items);
        }
    };
}

fn q4TripleScalar(out: *Emitter, rows: usize) !void {
    try out.add("const prepared_row_offset = o * prepared_row_stride;\n", .{});
    for ("abc") |p| for (0..rows) |r| {
        if (rows == 1) try out.add("var acc_{c}: f32 = 0.0;\n", .{p}) else try out.add("var acc_{c}_r{d}: f32 = 0.0;\n", .{ p, r });
    };
    try out.add("for (0..row_blocks) |block_idx| {{\nconst prepared_off = prepared_row_offset + block_idx * prepared_q4_k_block_bytes;\n", .{});
    if (rows == 1) {
        try out.add("const input_block = input_row[block_idx * values_per_block ..][0..values_per_block];\n", .{});
        for ("abc") |p| try out.add("acc_{c} += dotQ4KPreparedBlock(input_block, prepared_blocks_{c}[prepared_off..][0..prepared_q4_k_block_bytes]);\n", .{ p, p });
    } else {
        try out.add("const block_off = block_idx * values_per_block;\n", .{});
        for ("abc") |p| try out.add("const sums_{c} = dotQ4KPreparedBlockMR2NR1(input_r0[block_off..][0..values_per_block], input_r1[block_off..][0..values_per_block], prepared_blocks_{c}[prepared_off..][0..prepared_q4_k_block_bytes],);\n", .{ p, p });
        for ("abc") |p| for (0..rows) |r| try out.add("acc_{c}_r{d} += sums_{c}[{d}];\n", .{ p, r, p, r });
    }
    try out.add("}}\n", .{});
    if (rows != 1) for ("abc") |p| try out.add("const bias_{c}_value = optionalLinearBiasAt(bias_{c}, o);\n", .{ p, p });
    for ("abc") |p| for (0..rows) |r| {
        if (rows == 1) try out.add("out_{c}[o] = acc_{c} + optionalLinearBiasAt(bias_{c}, o);\n", .{ p, p, p }) else try out.add("out_{c}_r{d}[o] = acc_{c}_r{d} + bias_{c}_value;\n", .{ p, r, p, r, p });
    };
}

fn q4VectorAcc(buffer: *[32]u8, rows: usize, p: u8, r: usize, c: usize) []const u8 {
    return if (rows == 1) std.fmt.bufPrint(buffer, "acc_{c}{d}", .{ p, c }) catch unreachable else std.fmt.bufPrint(buffer, "{c}_r{d}_{d}", .{ p, r, c }) catch unreachable;
}

fn q4TripleVector(out: *Emitter, rows: usize) !void {
    var buf: [32]u8 = undefined;
    for ("abc") |p| for (0..rows) |r| for (0..4) |c| try out.add("var {s}: F32xN = @splat(0.0);\n", .{q4VectorAcc(&buf, rows, p, r, c)});
    for (0..4) |c| {
        if (c == 0) try out.add("const prepared_row_offset0 = o * prepared_row_stride;\n", .{}) else try out.add("const prepared_row_offset{d} = (o + {d}) * prepared_row_stride;\n", .{ c, c });
    }
    try out.add("for (0..row_blocks) |block_idx| {{\nconst prepared_off = block_idx * prepared_q4_k_block_bytes;\n", .{});
    if (rows == 1) {
        try out.add("const input_block = input_row[block_idx * values_per_block ..][0..values_per_block];\n", .{});
    } else {
        try out.add("const block_input_off = block_idx * values_per_block;\nconst input_block_r0 = input_r0[block_input_off..][0..values_per_block];\nconst input_block_r1 = input_r1[block_input_off..][0..values_per_block];\n", .{});
    }
    for ("abc") |p| {
        try out.add("accumulateQ4KPreparedBlock4Rows({d}, .{{", .{rows});
        for (0..rows) |r| {
            if (r != 0) try out.add(", ", .{});
            if (rows == 1) try out.add("input_block", .{}) else try out.add("input_block_r{d}", .{r});
        }
        try out.add("}}, ", .{});
        for (0..4) |c| try out.add("prepared_blocks_{c}[prepared_row_offset{d} + prepared_off ..][0..prepared_q4_k_block_bytes], ", .{ p, c });
        try out.add(".{{", .{});
        for (0..rows) |r| {
            if (r != 0) try out.add(", ", .{});
            try out.add(".{{", .{});
            for (0..4) |c| {
                if (c != 0) try out.add(", ", .{});
                try out.add("&{s}", .{q4VectorAcc(&buf, rows, p, r, c)});
            }
            try out.add("}}", .{});
        }
        try out.add("}});\n", .{});
    }
    try out.add("}}\n", .{});
    if (rows != 1) for ("abc") |p| for (0..4) |c| {
        if (c == 0) try out.add("const bias_{c}0 = optionalLinearBiasAt(bias_{c}, o);\n", .{ p, p }) else try out.add("const bias_{c}{d} = optionalLinearBiasAt(bias_{c}, o + {d});\n", .{ p, c, p, c });
    };
    for ("abc") |p| for (0..rows) |r| for (0..4) |c| {
        if (rows == 1) try out.add("out_{c}[", .{p}) else try out.add("out_{c}_r{d}[", .{ p, r });
        if (c == 0) try out.add("o]", .{}) else try out.add("o + {d}]", .{c});
        try out.add(" = @reduce(.Add, {s}) + ", .{q4VectorAcc(&buf, rows, p, r, c)});
        if (rows == 1) {
            if (c == 0) try out.add("optionalLinearBiasAt(bias_{c}, o);\n", .{p}) else try out.add("optionalLinearBiasAt(bias_{c}, o + {d});\n", .{ p, c });
        } else try out.add("bias_{c}{d};\n", .{ p, c });
    };
}

pub const Q8KProjectionOwner = struct {
    pub const begin = "// quant-kernel-codegen:begin CPU Q8K projection rows";
    pub const end = "// quant-kernel-codegen:end CPU Q8K projection rows";
    pub fn render(allocator: std.mem.Allocator) ![]u8 {
        var out = Emitter{ .allocator = allocator };
        defer out.bytes.deinit(allocator);
        try out.add(
            \\fn linearQ8KQ8KActivationRange(
            \\q_input: []const u8, weight_raw: []const u8,
            \\panel_blocks: ?[]const u8, panel8_blocks: ?[]const u8,
            \\panel16_blocks: ?[]const u8, output: []f32, rows: usize,
            \\in_dim: usize, out_dim: usize, out_start: usize, out_end: usize,
            \\) void {{
            \\@setEvalBranchQuota(20_000);
            \\const values_per_block: usize = 256;
            \\const block_size: usize = 292;
            \\std.debug.assert(in_dim % values_per_block == 0);
            \\const row_blocks = in_dim / values_per_block;
            \\const row_stride = row_blocks * block_size;
            \\const use_fused_sdot = q8KFusedSdotEnabled();
            \\var r: usize = 0;
            \\
        , .{});
        for ([_]usize{ 4, 2, 1 }) |mr| {
            if (mr == 1) try out.add("while (r < rows) : (r += 1) {{\n", .{}) else try out.add("while (r + {d} <= rows) : (r += {d}) {{\n", .{ mr, mr });
            try kRowViews(&out, mr, "row_stride");
            try out.add("var o: usize = out_start;\nconst panel_align: usize = if (panel16_blocks != null) prepared_k_panel16_nr else if (panel8_blocks != null) prepared_k_panel8_nr else prepared_q8_0_panel_nr;\nwhile (o < out_end and (o % panel_align) != 0) : (o += 1) {{\n", .{});
            try q8KScalarColumn(&out, mr);
            try out.add("}}\n", .{});
            for ([_]usize{ 16, 8 }) |nr| try kPanel(&out, mr, nr, true);
            try q8KDirectEight(&out, mr);
            try out.add("while (o + 4 <= out_end) : (o += 4) {{\n", .{});
            try q8KFourColumns(&out, mr);
            try out.add("}}\nwhile (o < out_end) : (o += 1) {{\n", .{});
            try q8KScalarColumn(&out, mr);
            try out.add("}}\n}}\n", .{});
        }
        try out.add("}}\n", .{});
        return formatSource(allocator, out.bytes.items);
    }
};

fn kRowViews(out: *Emitter, mr: usize, input_stride: []const u8) !void {
    for (0..mr) |r| {
        if (mr == 1) try out.add("const q_input_row = q_input[r * {s} ..][0..{s}];\n", .{ input_stride, input_stride }) else if (r == 0) try out.add("const q_input_row0 = q_input[r * {s} ..][0..{s}];\n", .{ input_stride, input_stride }) else try out.add("const q_input_row{d} = q_input[(r + {d}) * {s} ..][0..{s}];\n", .{ r, r, input_stride, input_stride });
    }
    for (0..mr) |r| {
        if (mr == 1) try out.add("const out_row = output[r * out_dim ..][0..out_dim];\n", .{}) else if (r == 0) try out.add("const out_row0 = output[r * out_dim ..][0..out_dim];\n", .{}) else try out.add("const out_row{d} = output[(r + {d}) * out_dim ..][0..out_dim];\n", .{ r, r });
    }
}

fn q8KScalarColumn(out: *Emitter, mr: usize) !void {
    try out.add("const weight_row = weight_raw[o * row_stride ..][0..row_stride];\n", .{});
    var buf: [32]u8 = undefined;
    for (0..mr) |r| try out.add("var {s}: f32 = 0.0;\n", .{rowName(&buf, "acc", mr, r)});
    try out.add("for (0..row_blocks) |block_idx| {{\nconst off = block_idx * block_size;\n", .{});
    if (mr == 1) {
        try out.add("acc += dotQ8KQ8KBlock(q_input_row[off..][0..block_size], weight_row[off..][0..block_size]);\n", .{});
    } else {
        try out.add("const w = weight_row[off..][0..block_size];\n", .{});
        for (0..mr) |r| try out.add("acc{d} += dotQ8KQ8KBlock(q_input_row{d}[off..][0..block_size], w);\n", .{ r, r });
    }
    try out.add("}}\n", .{});
    for (0..mr) |r| {
        if (mr == 1) try out.add("out_row[o] = acc;\n", .{}) else try out.add("out_row{d}[o] = acc{d};\n", .{ r, r });
    }
}

fn q8KDirectEight(out: *Emitter, mr: usize) !void {
    try out.add("if (q8KRawNR8DirectEnabled()) {{\nwhile (o + 8 <= out_end) : (o += 8) {{\n", .{});
    try kAccumulators(out, mr, "8");
    try out.add("for (0..row_blocks) |block_idx| {{\nconst off = block_idx * block_size;\nconst weights = q8KQ8KWeightBlock8(weight_raw, panel_blocks, row_blocks, row_stride, o, block_idx, true);\n", .{});
    if (mr == 1) {
        try out.add("const sums = dotQ8KQ8KBlock8Blocks(q_input_row[off..][0..block_size], weights);\n", .{});
    } else {
        try out.add("const sums = dotQ8KQ8KBlock8MR{d}Blocks(\n", .{mr});
        for (0..mr) |r| try out.add("q_input_row{d}[off..][0..block_size],\n", .{r});
        try out.add("weights,\n);\n", .{});
    }
    try kAccumulateRows(out, mr, "8", "lane");
    try out.add("}}\n", .{});
    try kPublishRows(out, mr, "8", "lane");
    try out.add("}}\n}}\n", .{});
}

fn q8KFourColumns(out: *Emitter, mr: usize) !void {
    if (mr != 2) try q8KWeightOffsets(out);
    var buf: [32]u8 = undefined;
    if (mr == 4) {
        try out.add("var acc: [16]f32 = @as([16]f32, @splat(0.0));\n", .{});
    } else {
        for (0..mr) |r| for (0..4) |c| try out.add("var {s}: f32 = 0.0;\n", .{accumulator(&buf, .{ .name = "", .rows = mr, .columns = 4, .sdot = true, .result = .array }, r, c)});
    }
    try out.add("for (0..row_blocks) |block_idx| {{\nconst off = block_idx * block_size;\n", .{});
    if (mr == 2) try q8KWeightOffsets(out);
    try out.add("const panel_off = ((o / prepared_q8_0_panel_nr) * row_blocks + block_idx) * prepared_q8_0_panel_nr * block_size;\n", .{});
    for (0..4) |c| {
        try out.add("const w{d} = if (panel_blocks) |panel| panel[panel_off", .{c});
        if (c == 1) try out.add(" + block_size", .{}) else if (c != 0) try out.add(" + {d} * block_size", .{c});
        try out.add(" ..][0..block_size] else weight_raw[w{d}_off + off ..][0..block_size];\n", .{c});
    }
    try out.add("const sums = if (use_fused_sdot)\n", .{});
    for ([_]bool{ true, false }) |fused| {
        if (!fused) try out.add("else\n", .{});
        try out.add("dotQ8KQ8KBlock4", .{});
        if (mr != 1) try out.add("MR{d}", .{mr});
        if (fused) try out.add("FusedSdot", .{});
        try out.add("(\n", .{});
        for (0..mr) |r| try out.add("{s}[off..][0..block_size],\n", .{rowName(&buf, "q_input_row", mr, r)});
        try out.add("w0,\nw1,\nw2,\nw3,\n){s}\n", .{if (fused) "" else ";"});
    }
    if (mr == 4) {
        try out.add("inline for (0..16) |idx| acc[idx] += sums[idx];\n", .{});
    } else {
        for (0..mr) |r| for (0..4) |c| try out.add("{s} += sums[{d}];\n", .{ accumulator(&buf, .{ .name = "", .rows = mr, .columns = 4, .sdot = true, .result = .array }, r, c), r * 4 + c });
    }
    try out.add("}}\n", .{});
    if (mr == 4) {
        try out.add("inline for (0..4) |lane| {{\n", .{});
        for (0..mr) |r| {
            if (r == 0) try out.add("out_row0[o + lane] = acc[lane];\n", .{}) else try out.add("out_row{d}[o + lane] = acc[{d} + lane];\n", .{ r, r * 4 });
        }
        try out.add("}}\n", .{});
    } else {
        for (0..mr) |r| for (0..4) |c| {
            try out.add("{s}[o", .{rowName(&buf, "out_row", mr, r)});
            if (c != 0) try out.add(" + {d}", .{c});
            try out.add("] = {s};\n", .{accumulator(&buf, .{ .name = "", .rows = mr, .columns = 4, .sdot = true, .result = .array }, r, c)});
        };
    }
}

fn q8KWeightOffsets(out: *Emitter) !void {
    for (0..4) |c| {
        if (c == 0) try out.add("const w0_off = o * row_stride;\n", .{}) else try out.add("const w{d}_off = (o + {d}) * row_stride;\n", .{ c, c });
    }
}

// Sixteen-byte leaves keep the original direct SDOT idiom before the portable
// vector traversal. Both widths share row/column and result ownership.
fn directSdot16(out: *Emitter, tile: Tile) !void {
    try out.raw("if (comptime native_aarch64_dotprod) {\n");
    for (0..tile.rows) |r| {
        if (tile.rows == 1) try out.raw("const raw_x: U8x16 = q_input.*;\n") else try out.add("const raw_x{d}: U8x16 = q_input_r{d}.*;\n", .{ r, r });
    }
    if (tile.rows != 1) for (0..tile.columns) |c| try out.add("const raw_w{d}: U8x16 = q_weight{d}.*;\n", .{ c, c });
    var buf: [32]u8 = undefined;
    for (0..tile.rows) |r| for (0..tile.columns) |c| {
        try out.add("const {s} = aarch64Sdot16(@splat(0), ", .{accumulator(&buf, tile, r, c)});
        if (tile.rows == 1) try out.add("raw_x, q_weight{d}.*);\n", .{c}) else try out.add("raw_x{d}, raw_w{d});\n", .{ r, c });
    };
    try tileReturn(out, tile);
    try out.raw("}\n");
}
fn tileReturn(out: *Emitter, tile: Tile) !void {
    const flat_tuple = tile.result == .tuple and (tile.bytes == 16 or tile.rows == 1);
    try out.raw(if (flat_tuple) "return .{ " else "return .{\n");
    var buf: [32]u8 = undefined;
    for (0..tile.rows) |r| {
        if (tile.result == .row_vectors) try out.add(".r{d} = .{{\n", .{r});
        for (0..tile.columns) |c| {
            try out.add("@reduce(.Add, {s})", .{accumulator(&buf, tile, r, c)});
            if (flat_tuple) try out.raw(if (r + 1 == tile.rows and c + 1 == tile.columns) " " else ", ") else try out.raw(",\n");
        }
        if (tile.result == .row_vectors) try out.raw("},\n");
    }
    try out.raw("};\n");
}
