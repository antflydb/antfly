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

//! Reverse-tree tuple envelopes for lazy physical-range admission.
const std = @import("std");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const A = std.mem.Allocator;
const Bounds = struct { lower: []const u8, upper: []const u8 };
fn decode(bytes: []const u8) !?Bounds {
    if (bytes.len < 12 or !std.mem.eql(u8, bytes[0..4], "LPR1")) return error.InvalidNativeLakeRowIndex;
    const n = std.mem.readInt(u32, bytes[4..8], .big);
    const m = std.mem.readInt(u32, bytes[8..12], .big);
    if (n == 0 and m == 0 and bytes.len == 12) return null;
    if (n == 0 or m == 0 or bytes.len > tree.max_aggregate_bytes or @as(u64, n) + m + 12 != bytes.len) return error.InvalidNativeLakeRowIndex;
    const result: Bounds = .{ .lower = bytes[12..][0..n], .upper = bytes[12 + n ..] };
    if (std.mem.order(u8, result.lower, result.upper) == .gt) return error.InvalidNativeLakeRowIndex;
    return result;
}
fn unknown(a: A) ![]u8 {
    const bytes = try a.alloc(u8, 12);
    @memcpy(bytes[0..4], "LPR1");
    @memset(bytes[4..], 0);
    return bytes;
}
pub fn reduce(a: A, entries: []const tree.Entry, height: u8) ![]u8 {
    const Views = struct { lower: tree.KeyView, upper: tree.KeyView };
    var result: ?Views = null;
    for (entries) |entry| {
        const bounds: Views = if (height == 0) leaf: {
            const key = entry.keyView();
            if (key.len() < 32 or key.slice(0, 16).order(key.slice(key.len() - 16, key.len())) != .eq) return error.InvalidNativeLakeRowIndex;
            break :leaf .{ .lower = key.slice(16, key.len()), .upper = key.slice(16, key.len()) };
        } else branch: {
            if (entry.aggregate.len == 0) return unknown(a);
            const value = (try decode(entry.aggregate)) orelse return unknown(a);
            break :branch .{ .lower = .from(value.lower), .upper = .from(value.upper) };
        };
        if (result) |*value| {
            if (bounds.lower.order(value.lower) == .lt) value.lower = bounds.lower;
            if (bounds.upper.order(value.upper) == .gt) value.upper = bounds.upper;
        } else result = bounds;
    }
    const value = result orelse return error.InvalidNativeLakeRowIndex;
    const prefix_bytes = (tree.max_aggregate_bytes - 12) / 2;
    var lower_buffer: [prefix_bytes]u8 = undefined;
    const lower = lower_buffer[0..@min(prefix_bytes, value.lower.len())];
    value.lower.copyRange(0, lower);
    var upper_buffer: [prefix_bytes]u8 = undefined;
    value.upper.copyRange(0, upper_buffer[0..@min(prefix_bytes, value.upper.len())]);
    const upper: []const u8 = if (value.upper.len() <= prefix_bytes) upper_buffer[0..value.upper.len()] else block: {
        var length = upper_buffer.len;
        while (length != 0 and upper_buffer[length - 1] == 255) length -= 1;
        if (length == 0) return unknown(a);
        upper_buffer[length - 1] += 1;
        break :block upper_buffer[0..length];
    };
    const bytes = try a.alloc(u8, 12 + lower.len + upper.len);
    @memcpy(bytes[0..4], "LPR1");
    std.mem.writeInt(u32, bytes[4..8], @intCast(lower.len), .big);
    std.mem.writeInt(u32, bytes[8..12], @intCast(upper.len), .big);
    @memcpy(bytes[12..][0..lower.len], lower);
    @memcpy(bytes[12 + lower.len ..], upper);
    return bytes;
}
/// null means bounded work could not establish absence. A necessary predicate
/// may admit false positives; final row membership always remains authoritative.
pub fn intersects(a: A, storage: tree.Store, root: ?tree.Ref, first: [16]u8, last: [16]u8, lower: []const u8, upper: ?[]const u8) !?bool {
    var after = last;
    var length = after.len;
    while (length != 0 and after[length - 1] == 255) length -= 1;
    if (length != 0) {
        after[length - 1] += 1;
        @memset(after[length..], 0);
    }
    const Visitor = struct {
        lower: []const u8,
        upper: ?[]const u8,
        work: usize = 0,
        fn check(v: *@This()) !void {
            if (v.work == 4096) return error.PredicateRangeUnknown;
            v.work += 1;
        }
        pub fn descend(v: *@This(), bytes: []const u8, _: u64) !bool {
            try v.check();
            if (bytes.len == 0) return true;
            const bounds = (try decode(bytes)) orelse return true;
            return std.mem.order(u8, bounds.upper, v.lower) != .lt and (v.upper == null or std.mem.order(u8, bounds.lower, v.upper.?) == .lt);
        }
        pub fn record(v: *@This(), key: []const u8, _: []const u8) !void {
            try v.check();
            if (key.len < 32 or !std.mem.eql(u8, key[0..16], key[key.len - 16 ..])) return error.InvalidNativeLakeRowIndex;
            const tuple = key[16..];
            if (std.mem.order(u8, tuple, v.lower) != .lt and (v.upper == null or std.mem.order(u8, tuple, v.upper.?) == .lt)) return error.PredicateRangeFound;
        }
    };
    var visitor: Visitor = .{ .lower = lower, .upper = upper };
    tree.visitRange(a, storage, root, &first, if (length == 0) null else &after, &visitor) catch |err| switch (err) {
        error.PredicateRangeFound => return true,
        error.PredicateRangeUnknown, error.ArtifactReadBudgetExceeded => return null,
        else => return err,
    };
    return false;
}

test "external lake global frontier physical range probes include high tuple bytes and reject disjoint values" {
    const a = std.testing.allocator;
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    var storage = memory.store();
    storage.reduce = reduce;
    const Source = struct {
        row: u8 = 0,
        key: [34]u8 = @splat(0),
        pub fn next(source: *@This()) !?tree.Cursor.Record {
            if (source.row == 20) return null;
            source.key[15] = source.row;
            source.key[16] = 255;
            source.key[17] = source.row;
            @memcpy(source.key[18..34], source.key[0..16]);
            source.row += 1;
            return .{ .key = &source.key, .value = "" };
        }
    };
    var source: Source = .{};
    const root = try tree.buildSorted(a, storage, &source);
    var first: [16]u8 = @splat(0);
    first[15] = 5;
    try std.testing.expectEqual(@as(?bool, true), try intersects(a, storage, root, first, first, &.{ 255, 5 }, &.{ 255, 6 }));
    first[15] = 7;
    var last = first;
    last[15] = 19;
    try std.testing.expectEqual(@as(?bool, false), try intersects(a, storage, root, first, last, &.{ 255, 5 }, &.{ 255, 6 }));
}

test "external lake global frontier reverse reductions bound long keys and retain unknown legacy children" {
    const a = std.testing.allocator;
    var key: [4096]u8 = @splat(0);
    @memset(key[16 .. key.len - 16], 'x');
    const encoded = try reduce(a, &.{.{ .key = &key, .value = "" }}, 0);
    defer a.free(encoded);
    try std.testing.expect(encoded.len <= tree.max_aggregate_bytes);
    const bounds = (try decode(encoded)).?;
    try std.testing.expect(std.mem.order(u8, bounds.lower, key[16..]) != .gt);
    try std.testing.expect(std.mem.order(u8, bounds.upper, key[16..]) != .lt);
    const parent = try reduce(a, &.{.{ .key = &key, .aggregate = encoded }}, 1);
    defer a.free(parent);
    const parent_bounds = (try decode(parent)).?;
    try std.testing.expect(std.mem.order(u8, parent_bounds.lower, key[16..]) != .gt);
    try std.testing.expect(std.mem.order(u8, parent_bounds.upper, key[16..]) != .lt);
    const legacy = try reduce(a, &.{.{ .key = &key, .aggregate = "" }}, 1);
    defer a.free(legacy);
    try std.testing.expectEqual(@as(?Bounds, null), try decode(legacy));
    const mixed = try reduce(a, &.{ .{ .key = &key, .aggregate = encoded }, .{ .key = &key, .aggregate = legacy } }, 1);
    defer a.free(mixed);
    try std.testing.expectEqual(@as(?Bounds, null), try decode(mixed));
    @memset(key[16 .. key.len - 16], 255);
    const saturated = try reduce(a, &.{.{ .key = &key, .value = "" }}, 0);
    defer a.free(saturated);
    try std.testing.expectEqual(@as(?Bounds, null), try decode(saturated));
}
