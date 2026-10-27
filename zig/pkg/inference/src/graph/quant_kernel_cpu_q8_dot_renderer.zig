// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Q8_K row/column dot schedules. Scalar integer registers, fused SDOT lanes
//! and packed panels are static source policies; scaling retains native order.
const std = @import("std");
const cpu = @import("quant_kernel_cpu_source.zig");
const Emitter = cpu.Emitter;
const Layout = enum { blocks4, fused4, panel4, panel8, panel16, blocks8 };
pub const owners = .{
    Owner(.blocks4, 1), Owner(.blocks4, 2), Owner(.blocks4, 4),
    Owner(.fused4, 1),  Owner(.fused4, 2),  Owner(.fused4, 4),
    Owner(.panel4, 1),  Owner(.panel4, 2),  Owner(.panel4, 4),
    Owner(.panel8, 1),  Owner(.panel8, 2),  Owner(.panel8, 4),
    Owner(.panel16, 1), Owner(.panel16, 2), Owner(.panel16, 4),
    Owner(.blocks8, 1), Owner(.blocks8, 2), Owner(.blocks8, 4),
};
fn suffix(comptime mr: usize) []const u8 {
    return if (mr == 1) "" else if (mr == 2) "MR2" else "MR4";
}
fn Owner(comptime layout: Layout, comptime mr: usize) type {
    return struct {
        pub const name = "dotQ8KQ8K" ++ (switch (layout) {
            .blocks4, .fused4 => "Block4",
            .panel4 => "Panel4",
            .panel8, .blocks8 => "Block8",
            .panel16 => "Block16",
        }) ++ suffix(mr) ++ (if (layout == .fused4) "FusedSdot" else if (layout == .blocks8) "Blocks" else "");
        pub const begin = "    // quant-kernel-codegen:begin CPU Q8K dot " ++ name;
        pub const end = "    // quant-kernel-codegen:end CPU Q8K dot " ++ name;
        pub fn render(allocator: std.mem.Allocator) ![]u8 {
            var out = Emitter{ .allocator = allocator };
            defer out.bytes.deinit(allocator);
            if (layout == .panel4) {
                try out.add("return q8KPanel4Rows({d}, .{{ ", .{mr});
                for (0..mr) |r| {
                    if (r != 0) try out.raw(", ");
                    try symbol(&out, "q8_input", mr, r);
                }
                try out.raw(" }, panel_block);\n");
            } else if (layout == .panel8 or layout == .panel16 or layout == .blocks8) try wideBody(&out, layout, mr) else try fourBody(&out, layout, mr);
            return cpu.formatStatements(allocator, out.bytes.items, 4);
        }
    };
}
fn symbol(out: *Emitter, prefix: []const u8, mr: usize, r: usize) !void {
    try out.raw(prefix);
    if (mr != 1) try out.add("{d}", .{r});
}
fn matrix(out: *Emitter, prefix: []const u8, mr: usize, r: usize, c: usize) !void {
    try out.raw(prefix);
    if (mr != 1) try out.add("{d}", .{r});
    try out.add("{d}", .{c});
}
fn inputScales(out: *Emitter, mr: usize, vector: bool) !void {
    for (0..mr) |r| {
        try out.raw("const ");
        try symbol(out, "input_scale", mr, r);
        try out.raw(if (vector) ": F32x8 = @splat(readF32Le(" else " = readF32Le(");
        try symbol(out, "q8_input", mr, r);
        try out.raw(if (vector) "[0..4]));\n" else "[0..4]);\n");
    }
}
fn queryViews(out: *Emitter, mr: usize) !void {
    for (0..mr) |r| {
        try out.raw("const ");
        try symbol(out, "qx", mr, r);
        try out.raw(" = ");
        try symbol(out, "q8_input", mr, r);
        try out.raw("[4..260];\n");
    }
}
fn asserts(out: *Emitter, mr: usize, layout: Layout) !void {
    for (0..mr) |r| {
        try out.raw("std.debug.assert(");
        try symbol(out, "q8_input", mr, r);
        try out.raw(".len == 292);\n");
    }
    switch (layout) {
        .blocks4, .fused4 => for (0..4) |c| try out.add("std.debug.assert(q8_weight{d}.len == 292);\n", .{c}),
        .blocks8 => try out.raw("inline for (0..8) |lane| std.debug.assert(weights[lane].len == 292);\n"),
        else => try out.add("std.debug.assert(panel_block.len == prepared_q8_k_panel{d}_block_bytes);\n", .{@as(usize, if (layout == .panel4) 4 else if (layout == .panel8) 8 else 16)}),
    }
}
fn fallback(out: *Emitter, mr: usize, fused: bool) !void {
    try out.raw("if (comptime !native_aarch64_dotprod) {\n");
    if (fused) {
        try out.add("return dotQ8KQ8KBlock4{s}(", .{switch (mr) {
            1 => "",
            2 => "MR2",
            4 => "MR4",
            else => unreachable,
        }});
        for (0..mr) |r| {
            try symbol(out, "q8_input", mr, r);
            try out.raw(", ");
        }
        try out.raw("q8_weight0, q8_weight1, q8_weight2, q8_weight3);\n");
    } else {
        for (0..2) |pair| try out.add("const rows{d}{d} = dotQ8KQ8KBlock4MR2(q8_input{d}, q8_input{d}, q8_weight0, q8_weight1, q8_weight2, q8_weight3);\n", .{ pair * 2, pair * 2 + 1, pair * 2, pair * 2 + 1 });
        try out.raw("return .{\n");
        for (0..2) |pair| for (0..8) |i| try out.add("rows{d}{d}[{d}],\n", .{ pair * 2, pair * 2 + 1, i });
        try out.raw("};\n");
    }
    try out.raw("}\n");
}
fn fourBody(out: *Emitter, layout: Layout, mr: usize) !void {
    const fused = layout == .fused4;
    const array = mr == 4 and !fused;
    if (fused or (layout == .blocks4 and mr == 4)) try fallback(out, mr, fused);
    if (fused or mr != 2) try asserts(out, mr, layout);
    if (!fused and mr != 4) try inputScales(out, mr, false);
    try queryViews(out, mr);
    for (0..4) |c| try out.add("const qw{d} = q8_weight{d}[4..260];\n", .{ c, c });
    if (array) {
        try out.add("var dot: [{d}]i32 = @as([{d}]i32, @splat(0));\n", .{ mr * 4, mr * 4 });
    } else for (0..mr) |r| for (0..4) |c| {
        try out.raw("var ");
        try matrix(out, if (fused) "acc" else "dot", mr, r, c);
        try out.raw(if (fused) ": I32x4 = @splat(0);\n" else ": i32 = 0;\n");
    };
    if (fused) {
        try fusedLoop(out, mr);
    } else {
        try out.raw("inline for (0..8) |chunk| {\nconst off = chunk * 32;\n");
        const vertical = mr != 1;
        try out.add("const {s} = dotI8I8Block4{s}{s}", .{ if (mr == 4) "sums" else "dots", switch (mr) {
            1 => "",
            2 => "MR2",
            4 => "MR4",
            else => unreachable,
        }, if (vertical) "(\n" else "(" });
        for (0..mr) |r| {
            try symbol(out, "qx", mr, r);
            try out.raw("[off..][0..32],");
            try out.raw(if (vertical) "\n" else " ");
        }
        for (0..4) |c| {
            try out.add("qw{d}[off..][0..32]", .{c});
            if (vertical) try out.raw(",\n") else if (c != 3) try out.raw(", ");
        }
        try out.raw(");\n");
        if (array and mr != 1) try out.add("inline for (0..{d}) |idx| dot[idx] += {s}[idx];\n", .{ mr * 4, "sums" }) else for (0..mr) |r| for (0..4) |c| {
            if (array) try out.add("dot[{d}]", .{c}) else try matrix(out, "dot", mr, r, c);
            try out.add(" += dots[{d}];\n", .{r * 4 + c});
        };
        try out.raw("}\n");
    }
    if (fused or (layout == .blocks4 and mr == 4)) try inputScales(out, mr, false);
    if (mr != 1) for (0..4) |c| {
        try out.add("const weight_scale{d} = readF32Le(q8_weight{d}[0..4]);\n", .{ c, c });
    };
    try out.raw("return .{\n");
    for (0..mr) |r| for (0..4) |c| {
        try symbol(out, "input_scale", mr, r);
        try out.raw(" * ");
        if (mr != 1) try out.add("weight_scale{d}", .{c}) else try out.add("readF32Le(q8_weight{d}[0..4])", .{c});
        try out.raw(" * @as(f32, @floatFromInt(");
        if (fused) try out.raw("@reduce(.Add, ");
        if (array) try out.add("dot[{d}]", .{r * 4 + c}) else try matrix(out, if (fused) "acc" else "dot", mr, r, c);
        if (fused) try out.raw(")");
        try out.raw(")),\n");
    };
    try out.raw("};\n");
}
fn fusedLoop(out: *Emitter, mr: usize) !void {
    try out.raw("comptime var off: usize = 0;\ninline while (off < 256) : (off += 16) {\n");
    for (0..mr) |r| {
        try out.raw("const ");
        try symbol(out, "raw_x", mr, r);
        try out.raw(": U8x16 = ");
        try symbol(out, "qx", mr, r);
        try out.raw("[off..][0..16].*;\n");
    }
    if (mr != 1) for (0..4) |c| try out.add("const raw_w{d}: U8x16 = qw{d}[off..][0..16].*;\n", .{ c, c });
    for (0..mr) |r| for (0..4) |c| {
        try matrix(out, "acc", mr, r, c);
        try out.raw(" = aarch64Sdot16(");
        try matrix(out, "acc", mr, r, c);
        try out.raw(", ");
        try symbol(out, "raw_x", mr, r);
        if (mr == 1) try out.add(", qw{d}[off..][0..16].*);\n", .{c}) else try out.add(", raw_w{d});\n", .{c});
    };
    try out.raw("}\n");
}
fn wideBody(out: *Emitter, layout: Layout, mr: usize) !void {
    try out.add("const result = q8KDotWide({d}, {d}, {s}, .{{", .{ mr, @as(usize, if (layout == .panel16) 16 else 8), if (layout == .blocks8) "true" else "false" });
    for (0..mr) |r| {
        if (r != 0) try out.raw(", ");
        try symbol(out, "q8_input", mr, r);
    }
    try out.raw(if (layout == .blocks8) "}, weights);\nreturn " else "}, panel_block);\nreturn ");
    if (mr == 1) try out.raw("result[0]") else {
        try out.raw(".{");
        for (0..mr) |r| try out.add(".r{d} = result[{d}],", .{ r, r });
        try out.raw("}");
    }
    try out.raw(";\n");
}

test "Q8K dot policies preserve native widths and integer scale boundaries" {
    inline for (owners) |O| {
        const source = try O.render(std.testing.allocator);
        defer std.testing.allocator.free(source);
        try std.testing.expect(std.mem.indexOf(u8, source, "[4..260]") != null or std.mem.indexOf(u8, source, "q8KDotWide(") != null or std.mem.indexOf(u8, source, "q8KPanel4Rows(") != null);
    }
}
