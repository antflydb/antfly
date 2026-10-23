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

//! Portable bounded cache encoding, independent of native pointer layout.
const std = @import("std");
const A = std.mem.Allocator;
pub fn encode(a: A, value: anytype) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try put(a, &out, value);
    return out.toOwnedSlice(a);
}
fn put(a: A, out: *std.ArrayList(u8), value: anytype) !void {
    switch (@typeInfo(@TypeOf(value))) {
        .int => {
            var b: [8]u8 = undefined;
            std.mem.writeInt(u64, &b, @intCast(value), .little);
            try out.appendSlice(a, &b);
        },
        .bool => try out.append(a, @intFromBool(value)),
        .optional => try put(a, out, value orelse std.math.maxInt(usize)),
        .@"struct" => inline for (@typeInfo(@TypeOf(value)).@"struct".field_names) |name| try put(a, out, @field(value, name)),
        .pointer => |p| {
            var b: [4]u8 = undefined;
            std.mem.writeInt(u32, &b, std.math.cast(u32, value.len) orelse return error.CacheTooLarge, .little);
            try out.appendSlice(a, &b);
            if (p.child == u8) try out.appendSlice(a, value) else for (value) |v| try put(a, out, v);
        },
        else => @compileError("unsupported cache field"),
    }
}
fn minimum(comptime T: type) usize {
    return switch (@typeInfo(T)) {
        .int, .optional => 8,
        .bool => 1,
        .pointer => 4,
        .@"struct" => blk: {
            var n: usize = 0;
            inline for (@typeInfo(T).@"struct".field_names) |name| n += minimum(@FieldType(T, name));
            break :blk n;
        },
        else => @compileError("unsupported cache field"),
    };
}
pub const Reader = struct {
    bytes: []const u8,
    fn take(self: *Reader, n: usize) ![]const u8 {
        if (n > self.bytes.len) return error.InvalidCache;
        const b = self.bytes[0..n];
        self.bytes = self.bytes[n..];
        return b;
    }
    pub fn get(self: *Reader, comptime T: type, a: A) !T {
        return switch (@typeInfo(T)) {
            .int => std.math.cast(T, std.mem.readInt(u64, (try self.take(8))[0..8], .little)) orelse error.InvalidCache,
            .bool => blk: {
                const b = (try self.take(1))[0];
                if (b > 1) return error.InvalidCache;
                break :blk b == 1;
            },
            .optional => |o| blk: {
                const n = try self.get(usize, a);
                break :blk if (n == std.math.maxInt(usize)) null else @as(o.child, n);
            },
            .@"struct" => blk: {
                var result: T = undefined;
                inline for (@typeInfo(T).@"struct".field_names) |name| @field(result, name) = try self.get(@FieldType(T, name), a);
                break :blk result;
            },
            .pointer => |p| blk: {
                const n = std.mem.readInt(u32, (try self.take(4))[0..4], .little);
                if (p.child == u8) break :blk try self.take(n);
                if (n > self.bytes.len / minimum(p.child)) return error.InvalidCache;
                const result = try a.alloc(p.child, n);
                for (result) |*v| v.* = try self.get(p.child, a);
                break :blk result;
            },
            else => @compileError("unsupported cache field"),
        };
    }
};

test "cache codec round trips and rejects truncated or hostile counts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Item = struct { name: []const u8, position: usize, parent: ?usize, valid: bool };
    const items = [_]Item{ .{ .name = "foo", .position = 300, .parent = null, .valid = true }, .{ .name = "bar", .position = 400, .parent = 0, .valid = false } };
    const bytes = try encode(a, items[0..]);
    var reader = Reader{ .bytes = bytes };
    const result = try reader.get([]Item, a);
    try std.testing.expectEqualStrings("bar", result[1].name);
    try std.testing.expectEqual(@as(?usize, null), result[0].parent);
    try std.testing.expectEqual(@as(?usize, 0), result[1].parent);
    try std.testing.expectEqual(@as(usize, 0), reader.bytes.len);
    for (0..bytes.len) |n| {
        var truncated = Reader{ .bytes = bytes[0..n] };
        try std.testing.expectError(error.InvalidCache, truncated.get([]Item, a));
    }
    var hostile = Reader{ .bytes = &.{ 255, 255, 255, 255 } };
    try std.testing.expectError(error.InvalidCache, hostile.get([]Item, a));
}
