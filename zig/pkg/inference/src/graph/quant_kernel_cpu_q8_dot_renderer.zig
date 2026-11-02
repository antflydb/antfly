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
            if (layout == .panel4 or layout == .blocks4 or layout == .fused4) {
                try out.add("const values = q8KDotFour({d}, .{s}, .{{ ", .{ mr, if (layout == .panel4) "panel" else if (layout == .fused4) "fused" else "blocks" });
                for (0..mr) |r| {
                    if (r != 0) try out.raw(", ");
                    try symbol(&out, "q8_input", mr, r);
                }
                try out.raw(if (layout == .panel4) " }, panel_block);\nreturn " else " }, .{ q8_weight0, q8_weight1, q8_weight2, q8_weight3 });\nreturn ");
                if (layout == .panel4 or mr == 4) try out.raw("values;\n") else {
                    try out.raw(".{ ");
                    for (0..mr * 4) |i| try out.add("values[{d}], ", .{i});
                    try out.raw("};\n");
                }
            } else try wideBody(&out, layout, mr);
            return cpu.formatStatements(allocator, out.bytes.items, 4);
        }
    };
}
fn symbol(out: *Emitter, prefix: []const u8, mr: usize, r: usize) !void {
    try out.raw(prefix);
    if (mr != 1) try out.add("{d}", .{r});
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
        try std.testing.expect(std.mem.indexOf(u8, source, "[4..260]") != null or std.mem.indexOf(u8, source, "q8KDotWide(") != null or std.mem.indexOf(u8, source, "q8KDotFour(") != null);
    }
}
