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

//! Arena-owned metadata copying at immutable publication boundaries.
const std = @import("std");
const A = std.mem.Allocator;
/// Retain descriptor ownership directly, without a JSON round trip or old
/// producer arenas. Only the metadata touched by this file crosses the wave.
pub fn clone(comptime T: type, a: A, value: T) std.mem.Allocator.Error!T {
    return switch (@typeInfo(T)) {
        .pointer => |pointer| copied: {
            if (pointer.size != .slice) @compileError("metadata owns slices only");
            if (pointer.child == u8) break :copied try a.dupe(u8, value);
            const result = try a.alloc(pointer.child, value.len);
            for (result, value) |*target, source| target.* = try clone(pointer.child, a, source);
            break :copied result;
        },
        .array => |array| copied: {
            var result: T = undefined;
            for (0..array.len) |index| result[index] = try clone(array.child, a, value[index]);
            break :copied result;
        },
        .@"union" => @compileError("metadata union ownership requires an explicit interpretation"),
        .optional => |optional| if (value) |present| try clone(optional.child, a, present) else null,
        .@"struct" => |structure| copied: {
            var result: T = value;
            inline for (structure.field_names) |field| {
                @field(result, field) = try clone(@FieldType(T, field), a, @field(value, field));
            }
            break :copied result;
        },
        else => value,
    };
}

fn ownershipScenario(a: A) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var bytes = [_]u8{ 'a', 'b', 'c' };
    const Metadata = struct { names: [2][]const u8, child: ?struct { bytes: []const u8 } };
    const input: Metadata = .{ .names = .{ &bytes, "second" }, .child = .{ .bytes = &bytes } };
    const result = try clone(Metadata, arena.allocator(), input);
    @memset(&bytes, 'x');
    try std.testing.expectEqualStrings("abc", result.names[0]);
    try std.testing.expectEqualStrings("abc", result.child.?.bytes);
}
test "external lake metadata ownership retains nested descriptors and unwinds allocation faults" {
    try ownershipScenario(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, ownershipScenario, .{});
}
