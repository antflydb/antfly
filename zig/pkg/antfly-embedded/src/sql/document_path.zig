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

//! Schema-compiled object paths for document columns. Segments retain literal
//! dots and slashes; assignment copies only the changed container spine.
const std = @import("std");
const Json = std.json.Value;
pub const Path = []const []const u8;
pub const max_depth = 64;

pub fn clone(a: std.mem.Allocator, path: Path) !Path {
    const result = try a.alloc([]const u8, path.len);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |part| a.free(part);
        a.free(result);
    }
    for (path, result) |part, *out| {
        out.* = try a.dupe(u8, part);
        initialized += 1;
    }
    return result;
}
pub fn deinit(a: std.mem.Allocator, path: Path) void {
    for (path) |part| a.free(part);
    a.free(path);
}
pub fn get(root: Json, path: Path) ?Json {
    var value = root;
    for (path) |part| {
        if (value != .object) return null;
        value = value.object.get(part) orelse return null;
    }
    return value;
}

/// Retained input snapshots must never observe an SQL assignment. Missing
/// ancestors become objects for an assignment, but a NULL/absent removal does
/// not create containers. Scalar ancestors cannot be silently overwritten.
pub fn assign(a: std.mem.Allocator, root: *std.json.ObjectMap, path: Path, value: Json, remove: bool) !void {
    if (path.len == 0 or path.len > max_depth) return error.InvalidSqlBackendResponse;
    var parents: [max_depth]Json = undefined;
    var current: Json = .{ .object = root.* };
    for (path, 0..) |part, level| {
        if (current != .object) return error.SqlTypeMismatch;
        parents[level] = current;
        if (level + 1 != path.len) {
            current = current.object.get(part) orelse missing: {
                if (remove) return;
                break :missing .{ .object = .empty };
            };
            if (current == .null) {
                if (remove) return;
                current = .{ .object = .empty };
            }
        }
    }
    var made: [max_depth]std.json.ObjectMap = undefined;
    var initialized: usize = 0;
    errdefer for (made[0..initialized]) |*map| map.deinit(a);
    var replacement = value;
    var level = path.len;
    while (level != 0) {
        level -= 1;
        const source = parents[level].object;
        var copy: std.json.ObjectMap = .empty;
        try copy.ensureTotalCapacity(a, source.count() + 1);
        for (source.keys(), source.values()) |key, child| copy.putAssumeCapacity(key, child);
        if (remove and level + 1 == path.len) {
            _ = copy.orderedRemove(path[level]);
        } else try copy.put(a, path[level], replacement);
        made[initialized] = copy;
        initialized += 1;
        replacement = .{ .object = copy };
    }
    root.* = replacement.object;
}

test "SQL document paths preserve literal names and immutable assignment spines" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const original = try std.json.parseFromSliceLeaky(Json, a, "{\"metadata\":{\"plan\":\"free\",\"a.b\":7},\"metadata.plan\":99}", .{});
    var copy = original.object;
    try assign(a, &copy, &.{ "metadata", "plan" }, .{ .string = "pro" }, false);
    try std.testing.expectEqualStrings("free", get(original, &.{ "metadata", "plan" }).?.string);
    try std.testing.expectEqualStrings("pro", get(.{ .object = copy }, &.{ "metadata", "plan" }).?.string);
    try std.testing.expectEqual(@as(i64, 99), get(.{ .object = copy }, &.{"metadata.plan"}).?.integer);
    try std.testing.expectEqual(@as(i64, 7), get(.{ .object = copy }, &.{ "metadata", "a.b" }).?.integer);
    try assign(a, &copy, &.{ "missing", "leaf" }, .null, true);
    try std.testing.expect(!copy.contains("missing"));
    try assign(a, &copy, &.{ "new", "leaf" }, .{ .integer = 12 }, false);
    try std.testing.expectEqual(@as(i64, 12), get(.{ .object = copy }, &.{ "new", "leaf" }).?.integer);
    try assign(a, &copy, &.{ "metadata", "plan" }, .null, true);
    try std.testing.expect(get(.{ .object = copy }, &.{ "metadata", "plan" }) == null);
    try std.testing.expectError(error.SqlTypeMismatch, assign(a, &copy, &.{ "metadata.plan", "leaf" }, .null, false));
}
