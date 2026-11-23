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

//! Qualified row schedules consumed by both native emission and host launch.
const std = @import("std");

pub const Phase = struct { array_rows: []const u8, warp_count: []const u8 };
pub const Native = struct { entry: []const u8, abi: []const u8, rows: []const u8 };
pub const Storage = enum { named_two_rows, parameter_rows, fixed_rows };
pub const Schedule = struct {
    kernel_field: []const u8,
    rows: usize,
    columns: usize,
    warps: usize,
    planes: usize = 1,
    storage: Storage,

    pub inline fn threads(self: Schedule) usize {
        return self.warps * 32;
    }
    pub inline fn rowGroups(self: Schedule, rows: usize) usize {
        return (rows + self.rows - 1) / self.rows;
    }
    pub inline fn columnGroups(self: Schedule, columns: usize) usize {
        return (columns + self.columns - 1) / self.columns;
    }
    pub fn entry(comptime self: Schedule) []const u8 {
        return "termite_" ++ self.kernel_field;
    }
    pub fn abi(comptime self: Schedule) []const u8 {
        return if (self.planes == 3) "Q8_INPUT_QKV" else "Q8_INPUT_LINEAR";
    }
    pub fn cppAbi(comptime self: Schedule) []const u8 {
        return "ANTFLY_CUDA_" ++ self.abi() ++ "_PARAMS";
    }
    pub fn phase(comptime self: Schedule) Phase {
        if (self.storage == .named_two_rows) @compileError("named two-row operands must retain their independent program");
        return .{ .array_rows = if (self.storage == .parameter_rows) "Rows" else std.fmt.comptimePrint("{d}", .{self.rows}), .warp_count = std.fmt.comptimePrint("{d}", .{self.warps}) };
    }
    pub fn native(comptime self: Schedule) Native {
        return .{ .entry = self.entry(), .abi = self.cppAbi(), .rows = std.fmt.comptimePrint("{d}", .{self.rows}) };
    }
    /// Static native storage, not dynamic cuLaunchKernel scratch.
    pub inline fn staticSharedBytes(self: Schedule) usize {
        return self.rows * self.columns * self.warps * @sizeOf(f32);
    }
};

fn checked(comptime profile: Schedule) Schedule {
    if (profile.planes == 1) {
        if (profile.columns != 4 or profile.warps != 8 or (profile.rows != 2 and profile.rows != 4 and profile.rows != 8)) @compileError("unqualified linear row schedule");
        if ((profile.rows == 2) != (profile.storage == .named_two_rows)) @compileError("two-row operands require the named schedule");
        if (profile.rows != 2 and profile.storage != .parameter_rows) @compileError("linear row arrays require the parameterized schedule");
    } else if (profile.planes != 3 or profile.rows != 4 or profile.columns != 8 or profile.warps != 4 or profile.storage != .fixed_rows) @compileError("unqualified QKV row schedule");
    return profile;
}

pub const linear2 = checked(.{ .kernel_field = "linear_q4_0_q8_1_f32_tile4_w8_rows2", .rows = 2, .columns = 4, .warps = 8, .storage = .named_two_rows });
pub const linear4 = checked(.{ .kernel_field = "linear_q4_0_q8_1_f32_tile4_w8_rows4", .rows = 4, .columns = 4, .warps = 8, .storage = .parameter_rows });
pub const linear8 = checked(.{ .kernel_field = "linear_q4_0_q8_1_f32_tile4_w8_rows8_c4", .rows = 8, .columns = 4, .warps = 8, .storage = .parameter_rows });
pub const qkv4 = checked(.{ .kernel_field = "linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4", .rows = 4, .columns = 8, .warps = 4, .planes = 3, .storage = .fixed_rows });

test "qualified Q4 Q8 row contracts preserve frozen grids and static resources" {
    inline for (.{ linear2, linear4, linear8, qkv4 }, .{ 256, 512, 1024, 512 }) |profile, bytes| {
        try std.testing.expectEqual(@as(usize, bytes), profile.staticSharedBytes());
        for ([_]usize{ 0, 1, 2, 3, 4, 7, 8, 9, 257 }) |extent| {
            const groups = profile.rowGroups(extent);
            try std.testing.expect(groups * profile.rows >= extent);
            if (groups > 0) try std.testing.expect((groups - 1) * profile.rows < extent);
            const cols = profile.columnGroups(extent);
            try std.testing.expect(cols * profile.columns >= extent);
            if (cols > 0) try std.testing.expect((cols - 1) * profile.columns < extent);
        }
    }
    try std.testing.expectEqual(@as(usize, 256), linear4.threads());
    try std.testing.expectEqual(@as(usize, 128), qkv4.threads());
    try std.testing.expectEqualStrings("Rows", comptime linear8.phase().array_rows);
    try std.testing.expectEqualStrings("4", comptime qkv4.phase().array_rows);
}
