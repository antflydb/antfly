// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub const SqlValue = union(enum) {
    null,
    bool: bool,
    integer: i64,
    float: f64,
    string: []const u8,
    json: []const u8,

    pub fn jsonAlloc(self: SqlValue, alloc: std.mem.Allocator) ![]const u8 {
        return switch (self) {
            .null => try alloc.dupe(u8, "null"),
            .bool => |value| try alloc.dupe(u8, if (value) "true" else "false"),
            .integer => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
            .float => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
            .string => |value| try std.json.Stringify.valueAlloc(alloc, value, .{}),
            .json => |value| try alloc.dupe(u8, value),
        };
    }

    pub fn asU32(self: SqlValue) !u32 {
        return switch (self) {
            .integer => |value| if (value >= 0 and value <= std.math.maxInt(u32)) @intCast(value) else error.UnsupportedSqlShape,
            else => error.UnsupportedSqlShape,
        };
    }
};
