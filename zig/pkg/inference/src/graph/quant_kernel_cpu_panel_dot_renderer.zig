// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native panel-dot geometry. Q6 integer scale accumulation and K16 min
//! correction remain separate arithmetic policies over the same static rows.
const std = @import("std");
const cpu = @import("quant_kernel_cpu_source.zig");
const Emitter = cpu.Emitter;
const Kind = enum { q6_panel8, q6_panel16, k16_panel8, q4q5_panel16 };
pub const owners = .{
    K16BlockOwner(1),        K16BlockOwner(2),        K16BlockOwner(4),
    Owner(.q4q5_panel16, 2), Owner(.q4q5_panel16, 4), Owner(.q6_panel8, 1),
    Owner(.q6_panel8, 2),    Owner(.q6_panel8, 4),    Owner(.q6_panel16, 1),
    Owner(.q6_panel16, 2),   Owner(.q6_panel16, 4),   Owner(.k16_panel8, 1),
    Owner(.k16_panel8, 2),   Owner(.k16_panel8, 4),
};
fn Owner(comptime kind: Kind, comptime mr: usize) type {
    return struct {
        pub const name = "dot" ++ (if (kind == .k16_panel8) "K16" else if (kind == .q4q5_panel16) "Q4Q5K" else "Q6K") ++ "PreparedQ8KPanel" ++ (if (kind == .q6_panel16 or kind == .q4q5_panel16) "16" else "8") ++ (if (mr == 1) "" else if (mr == 2) "MR2" else "MR4") ++ (if (kind == .q4q5_panel16) "PreparedSums" else if (kind == .q6_panel16) "" else "Vec");
        pub const begin = "    // quant-kernel-codegen:begin CPU panel dot " ++ name;
        pub const end = "    // quant-kernel-codegen:end CPU panel dot " ++ name;
        pub fn render(allocator: std.mem.Allocator) ![]u8 {
            var out = Emitter{ .allocator = allocator };
            defer out.bytes.deinit(allocator);
            try body(&out, kind, mr);
            return cpu.formatStatements(allocator, out.bytes.items, 4);
        }
    };
}
fn symbol(out: *Emitter, prefix: []const u8, mr: usize, r: usize) !void {
    try out.raw(prefix);
    if (mr != 1) try out.add("{d}", .{r});
}
fn query(out: *Emitter, mr: usize, r: usize) !void {
    try out.raw("q8_block");
    if (mr != 1) try out.add("_r{d}", .{r});
}
fn body(out: *Emitter, kind: Kind, mr: usize) !void {
    if (kind == .k16_panel8 or kind == .q6_panel8) {
        if (kind == .q6_panel8) {
            try out.add("if (q6KPanel8FusedSdotEnabled()) return dotQ6KPreparedQ8KPanel8{s}VecFusedSdot(", .{if (mr == 1) "" else if (mr == 2) "MR2" else "MR4"});
            for (0..mr) |row| {
                try query(out, mr, row);
                try out.raw(", ");
            }
            try out.raw("panel_block);\n");
        }
        try out.add("const rows = {s}({d}, {s}, .{{ ", .{ if (kind == .q6_panel8) "q6KPanel8Rows" else "k16Panel8Rows", mr, if (kind == .q6_panel8) "false" else "has_min" });
        for (0..mr) |r| {
            if (r != 0) try out.raw(", ");
            try query(out, mr, r);
        }
        try out.raw(" }, panel_block);\nreturn ");
        if (mr == 1) {
            try out.raw("rows[0];\n");
        } else {
            try out.raw(".{ ");
            for (0..mr) |r| try out.add(".r{d} = rows[{d}], ", .{ r, r });
            try out.raw("};\n");
        }
        return;
    }
    if (kind == .q4q5_panel16) {
        for (0..mr) |row| for ([_][]const u8{ "lo", "hi" }) |half| try out.add("var acc{d}_{s}: F32x8 = @splat(0.0);\n", .{ row, half });
        try out.add("q4q5Panel16Accumulate({d}, .sums, false, .{{ ", .{mr});
        for (0..mr) |row| {
            if (row != 0) try out.raw(", ");
            try query(out, mr, row);
        }
        try out.raw(" }, .{ ");
        for (0..mr) |row| try out.add("activation_sum{d}, ", .{row});
        try out.raw(" }, undefined, panel_block, .{ ");
        for ([_][]const u8{ "lo", "hi" }) |half| {
            try out.raw(".{ ");
            for (0..mr) |row| try out.add("&acc{d}_{s}, ", .{ row, half });
            try out.raw(" }, ");
        }
        try out.raw(" });\n");
        return preparedSumsReturn(out, mr);
    }
    for (0..mr) |row| {
        try out.raw("std.debug.assert(");
        try query(out, mr, row);
        try out.raw(".len == 292);\n");
    }
    try out.raw("std.debug.assert(panel_block.len == prepared_k_panel16_block_bytes);\n");
    for (0..mr) |row| {
        try out.raw("const ");
        try symbol(out, "qx", mr, row);
        try out.raw(" = ");
        try query(out, mr, row);
        try out.raw("[4..260];\n");
    }
    for (0..mr) |row| {
        try out.raw("const ");
        try symbol(out, "x_scale", mr, row);
        try out.raw(": F32x8 = @splat(readF32Le(");
        try query(out, mr, row);
        try out.raw("[0..4]));\n");
    }
    for (0..mr) |row| for ([_][]const u8{ "lo", "hi" }) |half| {
        try out.raw("var ");
        try symbol(out, "acc", mr, row);
        try out.add("_{s}: F32x8 = @splat(0.0);\n", .{half});
    };
    try out.raw("for (0..16) |sub| {\nconst q_off = sub * 16;\nconst values_base = prepared_k_panel16_values_offset + sub * prepared_k_panel16_nr * 16;\n");
    for (0..2) |half| try q6DotCall(out, mr, half);
    try out.raw("const dsc_lo = readF32x8Le(panel_block[prepared_k_panel16_dsc_offset + sub * prepared_k_panel16_nr * 4 ..][0..32]);\nconst dsc_hi = readF32x8Le(panel_block[prepared_k_panel16_dsc_offset + (sub * prepared_k_panel16_nr + 8) * 4 ..][0..32]);\n");
    for ([_][]const u8{ "lo", "hi" }) |half| for (0..mr) |row| {
        try symbol(out, "acc", mr, row);
        try out.add("_{s} += (", .{half});
        try symbol(out, "x_scale", mr, row);
        try out.add(" * dsc_{s}) * @as(F32x8, @floatFromInt(dots_{s}", .{ half, half });
        if (mr != 1) try out.add(".r{d}", .{row});
        try out.raw("));\n");
    };
    try out.raw("}\nreturn panel16");
    if (mr != 1) try out.add("MR{d}", .{mr});
    try out.raw("FromAcc(");
    for (0..mr) |row| for ([_][]const u8{ "lo", "hi" }, 0..) |half, index| {
        try symbol(out, "acc", mr, row);
        try out.add("_{s}", .{half});
        if (row + 1 != mr or index == 0) try out.raw(", ");
    };
    try out.raw(");\n");
}
fn q6DotCall(out: *Emitter, mr: usize, half: usize) !void {
    try out.add("const dots_{s} = dotI8I8Block8", .{if (half == 0) "lo" else "hi"});
    if (mr == 1) try out.raw("_16Vec") else try out.add("MR{d}RowsVec16", .{mr});
    try out.raw("(\n");
    for (0..mr) |row| {
        try symbol(out, "qx", mr, row);
        try out.raw("[q_off..][0..16],\n");
    }
    for (0..8) |col| {
        try out.raw("panel_block[values_base");
        const index = col + half * 8;
        if (index == 1) try out.raw(" + 16 ") else if (index != 0) try out.add(" + {d} * 16 ", .{index});
        try out.raw("..][0..16],\n");
    }
    try out.raw(");\n");
}

test "panel-dot geometries render valid native statements without leaking" {
    inline for (owners) |O| {
        const source = try O.render(std.testing.allocator);
        defer std.testing.allocator.free(source);
        try std.testing.expect(source.len != 0);
    }
}

fn preparedSumsReturn(out: *Emitter, mr: usize) !void {
    for (0..mr) |r| try out.add("var r{d}: [16]f32 = undefined;\n", .{r});
    for (0..mr) |r| for ([_][]const u8{ "lo", "hi" }) |half| try out.add("const r{d}_{s} = f32x8ToArray(acc{d}_{s});\n", .{ r, half, r, half });
    try out.raw("inline for (0..8) |idx| {\n");
    for (0..mr) |r| {
        try out.add("r{d}[idx] = r{d}_lo[idx];\nr{d}[idx + 8] = r{d}_hi[idx];\n", .{ r, r, r, r });
    }
    try out.raw("}\nreturn .{ ");
    for (0..mr) |r| {
        try out.add(".r{d} = r{d}", .{ r, r });
        try out.raw(if (r + 1 == mr) " " else ", ");
    }
    try out.raw("};\n");
}

fn K16BlockOwner(comptime mr: usize) type {
    return struct {
        pub const name = "dotK16PreparedQ8KBlock4" ++ (if (mr == 1) "" else if (mr == 2) "MR2" else "MR4") ++ "WithSums";
        pub const begin = "    // quant-kernel-codegen:begin CPU panel dot " ++ name;
        pub const end = "    // quant-kernel-codegen:end CPU panel dot " ++ name;
        pub fn render(allocator: std.mem.Allocator) ![]u8 {
            var out = Emitter{ .allocator = allocator };
            defer out.bytes.deinit(allocator);
            for (0..mr) |r| {
                try out.raw("const ");
                try symbol(&out, "x_d", mr, r);
                try out.raw(" = readF32Le(");
                try query(&out, mr, r);
                try out.raw("[0..4]);\n");
            }
            for (0..mr) |r| {
                try out.raw("const ");
                try symbol(&out, "qx", mr, r);
                try out.raw(" = ");
                try query(&out, mr, r);
                try out.raw("[4..260];\n");
            }
            if (mr == 4) try out.raw("var acc: [16]f32 = @as([16]f32, @splat(0.0));\n") else for (0..mr) |r| for (0..4) |c| {
                try out.raw("var ");
                try k16Accumulator(&out, mr, r, c);
                try out.raw(": f32 = 0.0;\n");
            };
            try out.raw("for (0..16) |sub| {\n");
            if (mr != 1) try out.raw("const q_off = sub * 16;\n");
            for (0..(if (mr == 4) @as(usize, 2) else 1)) |pair| {
                try out.raw("const dots");
                if (mr == 4) try out.add("{d}{d}", .{ pair * 2, pair * 2 + 1 });
                try out.raw(if (mr == 1) " = dotI8I8Block4_16(\n" else " = dotI8I8Block4MR2_16(\n");
                for (0..(if (mr == 1) @as(usize, 1) else 2)) |r| {
                    try symbol(&out, "qx", mr, r + pair * 2);
                    try out.raw(if (mr == 1) "[sub * 16 ..][0..16],\n" else "[q_off..][0..16],\n");
                }
                for (0..4) |c| try out.add("preparedK16Values(prepared_block{d}, sub),\n", .{c});
                try out.raw(");\n");
            }
            if (mr != 1) for (0..4) |c| try out.add("const dsc{d} = preparedK16Dsc(prepared_block{d}, sub);\n", .{ c, c });
            for ([_]bool{ true, false }) |with_min| {
                try out.raw(if (with_min) "if (comptime has_min) {\n" else "} else {\n");
                if (with_min) {
                    for (0..mr) |r| {
                        try out.raw("const ");
                        try symbol(&out, if (mr == 1) "activation_sum" else "sum", mr, r);
                        try out.raw(" = q8KBlockSum16F(");
                        try query(&out, mr, r);
                        try out.raw(", activation_sums");
                        if (mr != 1) try out.add("_r{d}", .{r});
                        try out.raw(", sub);\n");
                    }
                    if (mr != 1) for (0..4) |c| try out.add("const dmn{d} = preparedK16Dmn(prepared_block{d}, sub);\n", .{ c, c });
                }
                for (0..mr) |r| for (0..4) |c| {
                    try k16Accumulator(&out, mr, r, c);
                    try out.raw(" += ");
                    if (mr == 1) try out.add("preparedK16Dsc(prepared_block{d}, sub)", .{c}) else try out.add("dsc{d}", .{c});
                    try out.raw(" * @as(f32, @floatFromInt(dots");
                    if (mr == 4) try out.add("{d}{d}", .{ (r / 2) * 2, (r / 2) * 2 + 1 });
                    try out.add("[{d}]))", .{(if (mr == 4) r % 2 else r) * 4 + c});
                    if (with_min) {
                        try out.raw(" - ");
                        if (mr == 1) try out.add("preparedK16Dmn(prepared_block{d}, sub) * activation_sum", .{c}) else try out.add("dmn{d} * sum{d}", .{ c, r });
                    }
                    try out.raw(";\n");
                };
            }
            try out.raw("}\n}\nreturn .{\n");
            for (0..mr) |r| for (0..4) |c| {
                try symbol(&out, "x_d", mr, r);
                try out.raw(" * ");
                try k16Accumulator(&out, mr, r, c);
                if (mr != 1 or c != 3) try out.raw(",\n");
            };
            try out.raw("};\n");
            return cpu.formatStatements(allocator, out.bytes.items, 4);
        }
    };
}
fn k16Accumulator(out: *Emitter, mr: usize, r: usize, c: usize) !void {
    if (mr == 4) try out.add("acc[{d}]", .{r * 4 + c}) else if (mr == 1) try out.add("acc{d}", .{c}) else try out.add("acc{d}{d}", .{ r, c });
}
