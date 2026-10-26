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
/// A constant-size interval witness. Cohorts live in the ordered keyspace, not
/// in a page aggregate. Large tuples withdraw pruning without blocking writes.
pub const RangeSummary = struct {
    const magic = "AFCO002\x00";
    const header = 52;
    records: u64 = 0,
    shape: [32]u8 = @splat(0),
    reverse: bool = false,
    uniform: bool = true,
    complete: bool = true,
    key: []const u8 = "",
    tuple: []const u8 = "",
    pub fn read(bytes: []const u8) !?RangeSummary {
        if (!std.mem.startsWith(u8, bytes, magic)) return null;
        if (bytes.len < header or bytes.len > tree.max_aggregate_bytes or bytes[48] > 1 or bytes[49] > 3 or bytes.len != header + @as(usize, bytes[50]) + bytes[51]) return error.InvalidNativeLakeTextCorpus;
        const result: RangeSummary = .{ .records = std.mem.readInt(u64, bytes[8..16], .little), .shape = bytes[16..48].*, .reverse = bytes[48] != 0, .uniform = bytes[49] & 1 != 0, .complete = bytes[49] & 2 != 0, .key = bytes[header..][0..bytes[50]], .tuple = bytes[header + bytes[50] ..] };
        if (result.records == 0 or (result.complete and (!result.uniform or result.tuple.len == 0))) return error.InvalidNativeLakeTextCorpus;
        return result;
    }
};
pub fn reduce(a: A, entries: []const tree.Entry, height: u8) ![]u8 {
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var result: RangeSummary = .{};
    for (entries) |entry| {
        const child: RangeSummary = if (height != 0)
            (try RangeSummary.read(entry.aggregate)) orelse return error.InvalidNativeLakeTextCorpus
        else leaf: {
            if (entry.key.len < 65 or entry.value.len <= 32 or entry.key[32] > 1) return error.InvalidNativeLakeTextCorpus;
            const record = try decode(scratch, entry.value);
            var summary: RangeSummary = .{ .records = 1, .shape = entry.key[0..32].*, .reverse = entry.key[32] != 0 };
            if (!std.mem.eql(u8, &summary.shape, entry.value[0..32])) return error.InvalidNativeLakeTextCorpus;
            const bound = record.bound orelse {
                summary.complete = false;
                break :leaf summary;
            };
            summary.key = (try endpoint(scratch, bound, !summary.reverse)) orelse {
                summary.complete = false;
                break :leaf summary;
            };
            var keys: std.ArrayList(@import("antfly_local_sources").segment.SegmentIndexSortBoundValue) = .empty;
            for (bound.fields, if (summary.reverse) bound.first else bound.last) |field, value| {
                if (!std.mem.eql(u8, field.field, "_id")) try keys.append(scratch, try value.native());
            }
            summary.tuple = (try @import("lake_ordering_keys.zig").encode(scratch, @splat(0), keys.items)) orelse {
                summary.complete = false;
                break :leaf summary;
            };
            if (summary.key.len + summary.tuple.len > tree.max_aggregate_bytes - RangeSummary.header) summary.complete = false;
            break :leaf summary;
        };
        if (result.records == 0) {
            result = child;
            continue;
        }
        result.records = try std.math.add(u64, result.records, child.records);
        result.uniform = result.uniform and child.uniform and result.reverse == child.reverse and std.mem.eql(u8, &result.shape, &child.shape);
        result.complete = result.complete and child.complete and result.uniform;
        if (result.complete) {
            const order = std.mem.order(u8, child.key, result.key);
            if (order == (if (result.reverse) std.math.Order.lt else .gt)) {
                result.key = child.key;
                result.tuple = child.tuple;
            }
        }
    }
    if (result.records == 0) return error.InvalidNativeLakeTextCorpus;
    const key = if (result.complete) result.key else "";
    const tuple = if (result.complete) result.tuple else "";
    const bytes = try a.alloc(u8, RangeSummary.header + key.len + tuple.len);
    @memcpy(bytes[0..8], RangeSummary.magic);
    std.mem.writeInt(u64, bytes[8..16], result.records, .little);
    @memcpy(bytes[16..48], &result.shape);
    bytes[48] = @intFromBool(result.reverse);
    bytes[49] = @intFromBool(result.uniform) | (@as(u8, @intFromBool(result.complete)) << 1);
    bytes[50] = @intCast(key.len);
    bytes[51] = @intCast(tuple.len);
    @memcpy(bytes[RangeSummary.header..][0..key.len], key);
    @memcpy(bytes[RangeSummary.header + key.len ..], tuple);
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
