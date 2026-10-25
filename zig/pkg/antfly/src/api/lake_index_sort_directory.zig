// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Publication-owned ordered endpoints. Unknown/mixed layouts withdraw the
//! complete-cover proof; historical publications retain ordinary routing.
const std = @import("std");
const corpus = @import("lake_index_native_text.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const Bound = @import("lake_index_sort_bounds.zig").Bound;
const A = std.mem.Allocator;
pub const Record = struct { id: [32]u8, bound: ?Bound, planning_bytes: u64 };
fn layout(a: A, bound: ?Bound) ![32]u8 {
    const b = bound orelse return @splat(0);
    if (b.fields.len == 0 or b.fields.len != b.first.len or b.fields.len != b.last.len) return @splat(0);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-sort-endpoint-layout-v1");
    for (b.fields, b.first, b.last) |field, low, high| {
        if (low.kind != high.kind) return @splat(0);
        const bytes = try std.json.Stringify.valueAlloc(a, .{ field, low.kind }, .{});
        defer a.free(bytes);
        hash.update(bytes);
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
fn endpoint(a: A, b: Bound, reverse: bool) !?[]const u8 {
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(a);
    for (b.fields, if (reverse) b.last else b.first) |field, value| {
        if (std.mem.eql(u8, field.field, "_id")) continue;
        const start = bytes.items.len;
        const v = try value.native();
        switch (v) {
            .datetime_ns => |n| {
                var raw: [16]u8 = undefined;
                std.mem.writeInt(u128, &raw, @as(u128, @bitCast(n)) ^ (@as(u128, 1) << 127), .big);
                try bytes.appendSlice(a, &raw);
            },
            .i64_val => |n| {
                var raw: [8]u8 = undefined;
                std.mem.writeInt(u64, &raw, @as(u64, @bitCast(n)) ^ (@as(u64, 1) << 63), .big);
                try bytes.appendSlice(a, &raw);
            },
            .u64_val => |n| {
                var raw: [8]u8 = undefined;
                std.mem.writeInt(u64, &raw, n, .big);
                try bytes.appendSlice(a, &raw);
            },
            .f64_val => |n| {
                if (!std.math.isFinite(n)) return null;
                const bits: u64 = @bitCast(if (n == 0) @as(f64, 0) else n);
                var raw: [8]u8 = undefined;
                std.mem.writeInt(u64, &raw, if (bits >> 63 == 1) ~bits else bits ^ (@as(u64, 1) << 63), .big);
                try bytes.appendSlice(a, &raw);
            },
            .bool_val => |n| try bytes.append(a, @intFromBool(n)),
            .bytes_val => |text| {
                for (text) |byte| {
                    try bytes.append(a, byte);
                    if (byte == 0) try bytes.append(a, 255);
                }
                try bytes.appendSlice(a, &.{ 0, 0 });
            },
            .id => return null,
        }
        if (field.desc) for (bytes.items[start..]) |*byte| {
            byte.* = ~byte.*;
        };
    }
    return try bytes.toOwnedSlice(a);
}
pub fn mutate(a: A, changes: *corpus.text_directory.Mutations, route: corpus.SegmentRouting, planning_bytes: u64, deleting: bool) !void {
    const id = corpus.term_directory.identity(route.ref.artifact_id);
    const record: Record = .{ .id = id, .bound = route.sort_bound, .planning_bytes = planning_bytes };
    const shape = try layout(a, record.bound);
    const body = try std.json.Stringify.valueAlloc(a, record, .{});
    defer a.free(body);
    for ([_]bool{ false, true }) |reverse| {
        const prefix = if (record.bound) |b| try endpoint(a, b, reverse) else null;
        defer if (prefix) |p| a.free(p);
        const key = try std.mem.concat(a, u8, &.{ &.{@intFromBool(reverse)}, prefix orelse "", &id });
        defer a.free(key);
        const value = try std.mem.concat(a, u8, &.{ if (prefix != null) &shape else &@as([32]u8, @splat(0)), body });
        defer a.free(value);
        try changes.addRecord(key, if (deleting) null else value);
    }
}
pub fn reduce(a: A, entries: []const tree.Entry, height: u8) ![]u8 {
    var shape: ?[32]u8 = null;
    for (entries) |entry| {
        const bytes = if (height == 0) entry.value else entry.aggregate;
        if (bytes.len < 32) return error.InvalidNativeLakeTextCorpus;
        const next = bytes[0..32].*;
        if (shape == null) shape = next else if (!std.mem.eql(u8, &shape.?, &next)) shape = @splat(0);
    }
    return a.dupe(u8, &(shape orelse return error.InvalidNativeLakeTextCorpus));
}
pub fn decode(a: A, bytes: []const u8) !Record {
    if (bytes.len <= 32) return error.InvalidNativeLakeTextCorpus;
    const record = try std.json.parseFromSliceLeaky(Record, a, bytes[32..], .{});
    if (!std.mem.eql(u8, bytes[0..32], &try layout(a, record.bound))) return error.InvalidNativeLakeTextCorpus;
    return record;
}
