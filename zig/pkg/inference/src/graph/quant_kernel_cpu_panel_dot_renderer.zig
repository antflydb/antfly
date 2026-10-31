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
        try out.add("q4q5PanelAccumulate(16, {d}, .sums, false, .{{ ", .{mr});
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
    try out.add("const acc = q6KPanel16Rows({d}, .{{ ", .{mr});
    for (0..mr) |row| {
        if (row != 0) try out.raw(", ");
        try query(out, mr, row);
    }
    try out.raw(" }, panel_block);\n");
    try out.raw("return panel16");
    if (mr != 1) try out.add("MR{d}", .{mr});
    try out.raw("FromAcc(");
    for (0..mr) |row| for (0..2) |half| {
        if (row != 0 or half != 0) try out.raw(", ");
        try out.add("acc[{d}][{d}]", .{ half, row });
    };
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
            try out.add("const values = k16Block4Rows({d}, has_min, .{{ ", .{mr});
            for (0..mr) |row| {
                if (row != 0) try out.raw(", ");
                try query(&out, mr, row);
            }
            try out.raw(" }, .{ ");
            for (0..mr) |row| {
                if (row != 0) try out.raw(", ");
                try out.raw("activation_sums");
                if (mr != 1) try out.add("_r{d}", .{row});
            }
            try out.raw(" }, .{ prepared_block0, prepared_block1, prepared_block2, prepared_block3 });\n");
            if (mr == 4) try out.raw("return values;\n") else {
                try out.raw("return .{ ");
                for (0..mr * 4) |index| try out.add("values[{d}], ", .{index});
                try out.raw("};\n");
            }
            return cpu.formatStatements(allocator, out.bytes.items, 4);
        }
    };
}
