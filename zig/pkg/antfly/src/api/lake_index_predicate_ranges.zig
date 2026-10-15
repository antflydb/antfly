// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Reverse-tree tuple envelopes for lazy physical-range admission.
const std = @import("std");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const A = std.mem.Allocator;
const Bounds = struct { lower: []const u8, upper: []const u8 };
fn decode(bytes: []const u8) !Bounds {
    if (bytes.len < 12 or !std.mem.eql(u8, bytes[0..4], "LPR1")) return error.InvalidNativeLakeRowIndex;
    const n = std.mem.readInt(u32, bytes[4..8], .big);
    const m = std.mem.readInt(u32, bytes[8..12], .big);
    if (n < 16 or m < 16 or n > tree.max_key_bytes or m > tree.max_key_bytes or @as(u64, n) + m + 12 != bytes.len) return error.InvalidNativeLakeRowIndex;
    const result: Bounds = .{ .lower = bytes[12..][0..n], .upper = bytes[12 + n ..] };
    if (std.mem.order(u8, result.lower, result.upper) == .gt) return error.InvalidNativeLakeRowIndex;
    return result;
}
pub fn reduce(a: A, entries: []const tree.Entry, height: u8) ![]u8 {
    var result: ?Bounds = null;
    for (entries) |entry| {
        const bounds: Bounds = if (height == 0) leaf: {
            if (entry.key.len < 32 or !std.mem.eql(u8, entry.key[0..16], entry.key[entry.key.len - 16 ..])) return error.InvalidNativeLakeRowIndex;
            break :leaf .{ .lower = entry.key[16..], .upper = entry.key[16..] };
        } else if (entry.aggregate.len == 0) return a.alloc(u8, 0) else try decode(entry.aggregate);
        if (result) |*value| {
            if (std.mem.order(u8, bounds.lower, value.lower) == .lt) value.lower = bounds.lower;
            if (std.mem.order(u8, bounds.upper, value.upper) == .gt) value.upper = bounds.upper;
        } else result = bounds;
    }
    const value = result orelse return error.InvalidNativeLakeRowIndex;
    const bytes = try a.alloc(u8, 12 + value.lower.len + value.upper.len);
    @memcpy(bytes[0..4], "LPR1");
    std.mem.writeInt(u32, bytes[4..8], @intCast(value.lower.len), .big);
    std.mem.writeInt(u32, bytes[8..12], @intCast(value.upper.len), .big);
    @memcpy(bytes[12..][0..value.lower.len], value.lower);
    @memcpy(bytes[12 + value.lower.len ..], value.upper);
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
            const bounds = try decode(bytes);
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
