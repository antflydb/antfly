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
const Allocator = std.mem.Allocator;
const algebraic_segment = @import("types.zig");

const magic = "AFAS";
const version: u32 = 1;

pub fn encodeAlloc(alloc: Allocator, segment: algebraic_segment.Segment) ![]u8 {
    try segment.validate();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, magic);
    try appendU32(alloc, &out, version);
    try out.append(alloc, @intFromEnum(segment.source.kind));
    try appendBytes(alloc, &out, segment.source.snapshot_id);
    try appendBytes(alloc, &out, segment.source.schema_fingerprint);
    try appendBytes(alloc, &out, segment.source.source_id);

    try out.append(alloc, @intFromEnum(segment.aggregate.op));
    try appendBytes(alloc, &out, segment.aggregate.group_column);
    try appendBytes(alloc, &out, segment.aggregate.value_column);
    try appendU32(alloc, &out, @intCast(segment.aggregate.groups.len));

    for (segment.aggregate.groups) |group| {
        try appendBytes(alloc, &out, group.key);
        try encodeAggregateValue(alloc, &out, segment.aggregate.op, group.value);
    }

    return try out.toOwnedSlice(alloc);
}

pub fn decodeAlloc(alloc: Allocator, bytes: []const u8) !algebraic_segment.Segment {
    var cursor: usize = 0;
    if (bytes.len < magic.len + 4) return error.InvalidAlgebraicSegment;
    if (!std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidAlgebraicSegmentMagic;
    cursor += magic.len;
    const got_version = try readU32(bytes, &cursor);
    if (got_version != version) return error.UnsupportedAlgebraicSegmentVersion;

    if (cursor >= bytes.len) return error.InvalidAlgebraicSegment;
    const source_kind = try decodeSourceKind(bytes[cursor]);
    cursor += 1;
    const snapshot_id = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(snapshot_id);
    const schema_fingerprint = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(schema_fingerprint);
    const source_id = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(source_id);

    if (cursor >= bytes.len) return error.InvalidAlgebraicSegment;
    const op = try decodeAggregateOp(bytes[cursor]);
    cursor += 1;
    const group_column = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(group_column);
    const value_column = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(value_column);
    const group_count = try readU32(bytes, &cursor);

    const groups = try alloc.alloc(algebraic_segment.GroupFold, group_count);
    errdefer alloc.free(groups);
    var initialized_groups: usize = 0;
    errdefer {
        for (groups[0..initialized_groups]) |*group| group.deinit(alloc);
    }

    for (groups) |*group| {
        var keep_group = false;
        const key = try readBytesAlloc(alloc, bytes, &cursor);
        errdefer if (!keep_group) alloc.free(key);
        group.* = .{
            .key = key,
            .value = try decodeAggregateValue(bytes, &cursor, op),
        };
        keep_group = true;
        initialized_groups += 1;
    }

    if (cursor != bytes.len) return error.InvalidAlgebraicSegment;

    var segment = algebraic_segment.Segment{
        .source = .{
            .kind = source_kind,
            .snapshot_id = snapshot_id,
            .schema_fingerprint = schema_fingerprint,
            .source_id = source_id,
        },
        .aggregate = .{
            .group_column = group_column,
            .value_column = value_column,
            .op = op,
            .groups = groups,
        },
    };
    errdefer segment.deinit(alloc);
    try segment.validate();
    return segment;
}

fn encodeAggregateValue(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    op: algebraic_segment.AggregateOp,
    value: algebraic_segment.AggregateValue,
) !void {
    if (std.meta.activeTag(value) != op) return error.InvalidAlgebraicSegment;
    switch (value) {
        .count => |count| try appendU64(alloc, out, count),
        .sum_i64, .min_i64, .max_i64 => |int| try appendI64(alloc, out, int),
    }
}

fn decodeAggregateValue(
    bytes: []const u8,
    cursor: *usize,
    op: algebraic_segment.AggregateOp,
) !algebraic_segment.AggregateValue {
    return switch (op) {
        .count => .{ .count = try readU64(bytes, cursor) },
        .sum_i64 => .{ .sum_i64 = try readI64(bytes, cursor) },
        .min_i64 => .{ .min_i64 = try readI64(bytes, cursor) },
        .max_i64 => .{ .max_i64 = try readI64(bytes, cursor) },
    };
}

fn decodeSourceKind(raw: u8) !algebraic_segment.SourceKind {
    return switch (raw) {
        1 => .serverless_fragment,
        2 => .external_parquet,
        3 => .external_iceberg,
        4 => .external_lance,
        5 => .relational_store,
        else => error.InvalidAlgebraicSegment,
    };
}

fn decodeAggregateOp(raw: u8) !algebraic_segment.AggregateOp {
    return switch (raw) {
        1 => .count,
        2 => .sum_i64,
        3 => .min_i64,
        4 => .max_i64,
        else => error.InvalidAlgebraicSegment,
    };
}

fn appendBytes(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    try appendU32(alloc, out, @intCast(bytes.len));
    try out.appendSlice(alloc, bytes);
}

fn readBytesAlloc(alloc: Allocator, bytes: []const u8, cursor: *usize) ![]u8 {
    const len = try readU32(bytes, cursor);
    if (cursor.* + len > bytes.len) return error.InvalidAlgebraicSegment;
    const out = try alloc.dupe(u8, bytes[cursor.* .. cursor.* + len]);
    cursor.* += len;
    return out;
}

fn appendU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn appendI64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: i64) !void {
    try appendU64(alloc, out, @bitCast(value));
}

fn readU32(bytes: []const u8, cursor: *usize) !u32 {
    if (cursor.* + 4 > bytes.len) return error.InvalidAlgebraicSegment;
    const value = std.mem.readInt(u32, bytes[cursor.*..][0..4], .little);
    cursor.* += 4;
    return value;
}

fn readU64(bytes: []const u8, cursor: *usize) !u64 {
    if (cursor.* + 8 > bytes.len) return error.InvalidAlgebraicSegment;
    const value = std.mem.readInt(u64, bytes[cursor.*..][0..8], .little);
    cursor.* += 8;
    return value;
}

fn readI64(bytes: []const u8, cursor: *usize) !i64 {
    return @bitCast(try readU64(bytes, cursor));
}

test "algebraic segment codec round-trips group-by folds" {
    const alloc = std.testing.allocator;
    var segment = algebraic_segment.Segment{
        .source = .{
            .kind = .serverless_fragment,
            .snapshot_id = try alloc.dupe(u8, "manifest-1"),
            .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
            .source_id = try alloc.dupe(u8, "orders"),
        },
        .aggregate = .{
            .group_column = try alloc.dupe(u8, "tenant"),
            .value_column = try alloc.dupe(u8, "amount"),
            .op = .sum_i64,
            .groups = try alloc.alloc(algebraic_segment.GroupFold, 2),
        },
    };
    defer segment.deinit(alloc);
    segment.aggregate.groups[0] = .{ .key = try alloc.dupe(u8, "t1"), .value = .{ .sum_i64 = 10 } };
    segment.aggregate.groups[1] = .{ .key = try alloc.dupe(u8, "t2"), .value = .{ .sum_i64 = 20 } };

    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);

    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(algebraic_segment.SourceKind.serverless_fragment, decoded.source.kind);
    try std.testing.expectEqualStrings("tenant", decoded.aggregate.group_column);
    try std.testing.expectEqual(@as(usize, 2), decoded.aggregate.groups.len);
    try std.testing.expectEqual(@as(i64, 20), decoded.aggregate.groups[1].value.sum_i64);
}
