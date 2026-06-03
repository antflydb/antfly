// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const format = @import("format.zig");

pub const View = struct {
    file: *const format.File,

    pub fn init(file: *const format.File) View {
        return .{ .file = file };
    }

    pub fn find(self: View, key: []const u8) ?*const format.MetadataEntry {
        for (self.file.metadata) |*entry| {
            if (std.mem.eql(u8, entry.key, key)) return entry;
        }
        return null;
    }

    pub fn getString(self: View, key: []const u8) ?[]const u8 {
        const entry = self.find(key) orelse return null;
        return switch (entry.value) {
            .string => |value| value,
            else => null,
        };
    }

    pub fn getU64(self: View, key: []const u8) ?u64 {
        const entry = self.find(key) orelse return null;
        return switch (entry.value) {
            .u8 => |value| value,
            .u16 => |value| value,
            .u32 => |value| value,
            .u64 => |value| value,
            else => null,
        };
    }

    pub fn getI64(self: View, key: []const u8) ?i64 {
        const entry = self.find(key) orelse return null;
        return switch (entry.value) {
            .i8 => |value| value,
            .i16 => |value| value,
            .i32 => |value| value,
            .i64 => |value| value,
            else => null,
        };
    }

    pub fn getBool(self: View, key: []const u8) ?bool {
        const entry = self.find(key) orelse return null;
        return switch (entry.value) {
            .bool_ => |value| value,
            else => null,
        };
    }

    pub fn getF32(self: View, key: []const u8) ?f32 {
        const entry = self.find(key) orelse return null;
        return switch (entry.value) {
            .f32 => |value| value,
            .f64 => |value| @floatCast(value),
            .u8 => |value| @floatFromInt(value),
            .u16 => |value| @floatFromInt(value),
            .u32 => |value| @floatFromInt(value),
            .u64 => |value| @floatFromInt(value),
            .i8 => |value| @floatFromInt(value),
            .i16 => |value| @floatFromInt(value),
            .i32 => |value| @floatFromInt(value),
            .i64 => |value| @floatFromInt(value),
            else => null,
        };
    }

    pub fn alignment(self: View) u64 {
        return self.getU64("general.alignment") orelse format.default_alignment;
    }
};
