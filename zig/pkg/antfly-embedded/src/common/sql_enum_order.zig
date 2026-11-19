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

//! Stable dense order keys for enum symbols. Existing keys never move when a
//! label is inserted before/after another label, preserving LSM index order.
const std = @import("std");
const A = std.mem.Allocator;

/// Zero is allowed inside a key, but never at its end. Excluding trailing zero
/// leaves an insertion point between every pair, including prefix neighbors.
pub fn validate(key: []const u8, max_bytes: usize) !void {
    if (key.len == 0 or key.len > max_bytes or key[key.len - 1] == 0) return error.InvalidSqlEnumOrder;
}

/// Null bounds denote negative/positive infinity. The returned key belongs to
/// the caller. Work and output allocation are bounded before each extension.
pub fn between(a: A, lower: ?[]const u8, upper: ?[]const u8, max_bytes: usize) ![]u8 {
    if (lower) |key| try validate(key, max_bytes);
    if (upper) |key| try validate(key, max_bytes);
    if (lower != null and upper != null and std.mem.order(u8, lower.?, upper.?) != .lt) return error.InvalidSqlEnumOrder;
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(a);
    var position: usize = 0;
    var above = upper;
    while (true) : (position += 1) {
        if (result.items.len >= max_bytes) return error.SqlProgramLimitExceeded;
        const left: u16 = if (lower) |key| (if (position < key.len) key[position] else 0) else 0;
        const right: u16 = if (above) |key| (if (position < key.len) key[position] else 256) else 256;
        if (right > left + 1) {
            try result.append(a, @intCast(left + (right - left) / 2));
            return result.toOwnedSlice(a);
        }
        try result.append(a, @intCast(left));
        if (left < right) above = null;
    }
}

test "SQL enum dense order keeps prefix neighbors and immutable LSM keys" {
    const a = std.testing.allocator;
    const Vector = struct { lower: ?[]const u8, upper: ?[]const u8 };
    const vectors = [_]Vector{
        .{ .lower = null, .upper = null },
        .{ .lower = null, .upper = &.{1} },
        .{ .lower = &.{255}, .upper = null },
        .{ .lower = &.{128}, .upper = &.{ 128, 1 } },
        .{ .lower = &.{ 128, 0, 1 }, .upper = &.{ 128, 1 } },
        .{ .lower = &.{ 128, 255 }, .upper = &.{129} },
    };
    for (vectors) |vector| {
        const key = try between(a, vector.lower, vector.upper, 32);
        defer a.free(key);
        try validate(key, 32);
        if (vector.lower) |bound| try std.testing.expect(std.mem.order(u8, bound, key) == .lt);
        if (vector.upper) |bound| try std.testing.expect(std.mem.order(u8, key, bound) == .lt);
    }
    try std.testing.expectError(error.InvalidSqlEnumOrder, between(a, &.{128}, &.{128}, 32));
    try std.testing.expectError(error.InvalidSqlEnumOrder, between(a, &.{0}, null, 32));
    try std.testing.expectError(error.SqlProgramLimitExceeded, between(a, &.{128}, &.{ 128, 1 }, 2));
}

test "SQL enum deep insertions retain every prior ordering key with bounded ownership" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const first = try between(scratch, null, null, 1024);
    const last = try between(scratch, first, null, 1024);
    var right: []const u8 = last;
    var retained: std.ArrayList([]const u8) = .empty;
    defer retained.deinit(a);
    for (0..1024) |_| {
        const middle = try between(scratch, first, right, 1024);
        try retained.append(a, middle);
        try std.testing.expect(std.mem.order(u8, first, middle) == .lt);
        try std.testing.expect(std.mem.order(u8, middle, right) == .lt);
        right = middle;
    }
    try std.testing.expectEqualSlices(u8, &.{128}, first);
    try std.testing.expectEqualSlices(u8, &.{192}, last);
    for (retained.items[1..], retained.items[0 .. retained.items.len - 1]) |later, earlier| try std.testing.expect(std.mem.order(u8, later, earlier) == .lt);
}
