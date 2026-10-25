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
    hash.update("native-sort-endpoint-layout-v2");
    for (b.fields, b.first, b.last) |field, low, high| {
        const domain = if (low.kind == .null_value) high.kind else low.kind;
        if (high.kind != .null_value and low.kind != .null_value and low.kind != high.kind) return @splat(0);
        const bytes = try std.json.Stringify.valueAlloc(a, .{ .field = field.field, .desc = field.desc, .domain = domain }, .{});
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
        try bytes.append(a, if (v == .null_value) 0 else 1);
        switch (v) {
            .null_value => {},
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
        const effective_shape: [32]u8 = if (prefix != null) shape else @splat(0);
        const payload = if (prefix != null) body else try std.json.Stringify.valueAlloc(a, Record{ .id = id, .bound = null, .planning_bytes = planning_bytes }, .{});
        defer if (prefix == null) a.free(payload);
        const key = try std.mem.concat(a, u8, &.{ &effective_shape, &.{@intFromBool(reverse)}, prefix orelse "", &id });
        defer a.free(key);
        const value = try std.mem.concat(a, u8, &.{ &effective_shape, payload });
        defer a.free(value);
        try changes.addRecord(key, if (deleting) null else value);
    }
}
pub const max_cohorts = 32;
pub const Cohort = struct { shape: [32]u8, records: u64 };
pub const Summary = struct {
    cohorts: [max_cohorts]Cohort = undefined,
    count: usize = 0,
    overflow: bool = false,
    fn add(self: *Summary, shape: [32]u8, count: u64) !void {
        for (self.cohorts[0..self.count]) |*cohort| if (std.mem.eql(u8, &cohort.shape, &shape)) {
            cohort.records = try std.math.add(u64, cohort.records, count);
            return;
        };
        if (self.count == max_cohorts) {
            self.overflow = true;
            return;
        }
        self.cohorts[self.count] = .{ .shape = shape, .records = count };
        self.count += 1;
    }
    pub fn read(bytes: []const u8) !Summary {
        if (bytes.len < 10 or !std.mem.startsWith(u8, bytes, "AFCO001\x00") or bytes[8] > max_cohorts or bytes[9] > 1 or bytes.len != 10 + @as(usize, bytes[8]) * 40) return error.InvalidNativeLakeTextCorpus;
        var result: Summary = .{ .count = bytes[8], .overflow = bytes[9] == 1 };
        for (result.cohorts[0..result.count], 0..) |*cohort, i| {
            cohort.* = .{ .shape = bytes[10 + i * 40 ..][0..32].*, .records = std.mem.readInt(u64, bytes[42 + i * 40 ..][0..8], .little) };
            if (cohort.records == 0) return error.InvalidNativeLakeTextCorpus;
            for (result.cohorts[0..i]) |prior| if (std.mem.eql(u8, &prior.shape, &cohort.shape)) return error.InvalidNativeLakeTextCorpus;
        }
        return result;
    }
};
pub fn reduce(a: A, entries: []const tree.Entry, height: u8) ![]u8 {
    var result: Summary = .{};
    for (entries) |entry| {
        if (height == 0) {
            if (entry.value.len < 32) return error.InvalidNativeLakeTextCorpus;
            try result.add(entry.value[0..32].*, 1);
        } else {
            const child = try Summary.read(entry.aggregate);
            result.overflow = result.overflow or child.overflow;
            for (child.cohorts[0..child.count]) |cohort| try result.add(cohort.shape, cohort.records);
        }
    }
    const bytes = try a.alloc(u8, 10 + result.count * 40);
    @memcpy(bytes[0..8], "AFCO001\x00");
    bytes[8] = @intCast(result.count);
    bytes[9] = @intFromBool(result.overflow);
    for (result.cohorts[0..result.count], 0..) |cohort, i| {
        @memcpy(bytes[10 + i * 40 ..][0..32], &cohort.shape);
        std.mem.writeInt(u64, bytes[42 + i * 40 ..][0..8], cohort.records, .little);
    }
    return bytes;
}
pub fn decode(a: A, bytes: []const u8) !Record {
    if (bytes.len <= 32) return error.InvalidNativeLakeTextCorpus;
    const record = try std.json.parseFromSliceLeaky(Record, a, bytes[32..], .{});
    if (!std.mem.eql(u8, bytes[0..32], &try layout(a, record.bound))) return error.InvalidNativeLakeTextCorpus;
    return record;
}

/// Historical flat directories hashed only field name/direction and scalar
/// domain. Do not reinterpret their aggregate under the cohort encoding.
pub fn decodeLegacy(a: A, bytes: []const u8) !Record {
    if (bytes.len <= 32) return error.InvalidNativeLakeTextCorpus;
    const record = try std.json.parseFromSliceLeaky(Record, a, bytes[32..], .{});
    const bound = record.bound orelse return record;
    if (bound.fields.len == 0 or bound.fields.len != bound.first.len or bound.fields.len != bound.last.len) return error.InvalidNativeLakeTextCorpus;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-sort-endpoint-layout-v1");
    for (bound.fields, bound.first, bound.last) |field, low, high| {
        if (low.kind != high.kind or low.kind == .null_value) return error.InvalidNativeLakeTextCorpus;
        const value = try std.json.Stringify.valueAlloc(a, .{ .{ .field = field.field, .desc = field.desc }, low.kind }, .{});
        defer a.free(value);
        hash.update(value);
    }
    var expected: [32]u8 = undefined;
    hash.final(&expected);
    if (!std.mem.eql(u8, bytes[0..32], &expected)) return error.InvalidNativeLakeTextCorpus;
    return record;
}
