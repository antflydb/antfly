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

//! Builder for serverless algebraic group-by aggregate artifacts.

const std = @import("std");
const Allocator = std.mem.Allocator;
const algebraic_segment = @import("types.zig");
const codec = @import("codec.zig");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const BuildOptions = struct {
    source_kind: algebraic_segment.SourceKind,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    source_id: []const u8 = &.{},
    group_column: []const u8,
    value_column: []const u8 = &.{},
    op: algebraic_segment.AggregateOp,
};

pub fn buildGroupByAggregateAlloc(
    alloc: Allocator,
    batch: rowsource.ColumnBatch,
    options: BuildOptions,
) !algebraic_segment.Segment {
    if (options.snapshot_id.len == 0) return error.InvalidAlgebraicSegmentBuildOptions;
    if (options.schema_fingerprint.len == 0) return error.InvalidAlgebraicSegmentBuildOptions;
    if (options.group_column.len == 0) return error.InvalidAlgebraicSegmentBuildOptions;
    if (options.op != .count and options.value_column.len == 0) return error.InvalidAlgebraicSegmentBuildOptions;
    try batch.validate();

    const group_column = batch.findColumn(options.group_column) orelse return error.RowSourceColumnNotFound;
    if (group_column.kind() != .bytes) return error.UnsupportedAlgebraicGroupColumnKind;

    const value_column = if (options.op == .count) null else batch.findColumn(options.value_column) orelse return error.RowSourceColumnNotFound;
    if (value_column) |column| {
        if (column.kind() != .i64) return error.UnsupportedAlgebraicValueColumnKind;
    }

    var map = std.StringHashMapUnmanaged(algebraic_segment.AggregateValue).empty;
    defer map.deinit(alloc);

    const group_values = group_column.values.bytes;
    for (0..batch.rowCount()) |row_idx| {
        if (group_column.nulls.isNull(row_idx)) continue;
        const key = group_values[row_idx];
        if (key.len == 0) continue;
        const next_value = (try rowAggregateValue(options.op, value_column, row_idx)) orelse continue;
        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        const entry = try map.getOrPut(alloc, key);
        if (!entry.found_existing) {
            entry.key_ptr.* = owned_key;
            entry.value_ptr.* = next_value;
        } else {
            alloc.free(owned_key);
            entry.value_ptr.* = try combine(entry.value_ptr.*, next_value);
        }
    }
    defer {
        var it = map.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
    }

    var groups = try alloc.alloc(algebraic_segment.GroupFold, map.count());
    errdefer alloc.free(groups);
    var initialized_groups: usize = 0;
    errdefer {
        for (groups[0..initialized_groups]) |*group| group.deinit(alloc);
    }

    var it = map.iterator();
    while (it.next()) |entry| {
        groups[initialized_groups] = .{
            .key = try alloc.dupe(u8, entry.key_ptr.*),
            .value = entry.value_ptr.*,
        };
        initialized_groups += 1;
    }
    std.mem.sort(algebraic_segment.GroupFold, groups, {}, compareGroupFold);

    var segment = algebraic_segment.Segment{
        .source = .{
            .kind = options.source_kind,
            .snapshot_id = try alloc.dupe(u8, options.snapshot_id),
            .schema_fingerprint = try alloc.dupe(u8, options.schema_fingerprint),
            .source_id = if (options.source_id.len == 0) &.{} else try alloc.dupe(u8, options.source_id),
        },
        .aggregate = .{
            .group_column = try alloc.dupe(u8, options.group_column),
            .value_column = if (options.value_column.len == 0) &.{} else try alloc.dupe(u8, options.value_column),
            .op = options.op,
            .groups = groups,
        },
    };
    errdefer segment.deinit(alloc);
    try segment.validate();
    return segment;
}

pub fn encodeGroupByAggregateAlloc(
    alloc: Allocator,
    batch: rowsource.ColumnBatch,
    options: BuildOptions,
) ![]u8 {
    var segment = try buildGroupByAggregateAlloc(alloc, batch, options);
    defer segment.deinit(alloc);
    return codec.encodeAlloc(alloc, segment);
}

fn rowAggregateValue(
    op: algebraic_segment.AggregateOp,
    value_column: ?rowsource.ColumnVector,
    row_idx: usize,
) !?algebraic_segment.AggregateValue {
    return switch (op) {
        .count => .{ .count = 1 },
        .sum_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .sum_i64 = value_column.?.values.i64[row_idx] },
        .min_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .min_i64 = value_column.?.values.i64[row_idx] },
        .max_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .max_i64 = value_column.?.values.i64[row_idx] },
    };
}

fn combine(
    lhs: algebraic_segment.AggregateValue,
    rhs: algebraic_segment.AggregateValue,
) !algebraic_segment.AggregateValue {
    return switch (lhs) {
        .count => |left| .{ .count = left + rhs.count },
        .sum_i64 => |left| .{ .sum_i64 = left + rhs.sum_i64 },
        .min_i64 => |left| .{ .min_i64 = @min(left, rhs.min_i64) },
        .max_i64 => |left| .{ .max_i64 = @max(left, rhs.max_i64) },
    };
}

fn compareGroupFold(_: void, lhs: algebraic_segment.GroupFold, rhs: algebraic_segment.GroupFold) bool {
    return std.mem.lessThan(u8, lhs.key, rhs.key);
}

test "algebraic builder folds i64 sums by byte group column" {
    const alloc = std.testing.allocator;
    const row_refs = [_]rowsource.RowRef{
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 0 } },
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 1 } },
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 2 } },
    };
    const tenants = [_][]const u8{ "t2", "t1", "t2" };
    const amounts = [_]i64{ 7, 11, 13 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "manifest-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    };

    var segment = try buildGroupByAggregateAlloc(alloc, batch, .{
        .source_kind = .serverless_fragment,
        .snapshot_id = "manifest-1",
        .schema_fingerprint = "schema-v1",
        .source_id = "orders",
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
    });
    defer segment.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), segment.aggregate.groups.len);
    try std.testing.expectEqualStrings("t1", segment.aggregate.groups[0].key);
    try std.testing.expectEqual(@as(i64, 11), segment.aggregate.groups[0].value.sum_i64);
    try std.testing.expectEqualStrings("t2", segment.aggregate.groups[1].key);
    try std.testing.expectEqual(@as(i64, 20), segment.aggregate.groups[1].value.sum_i64);
}

test "algebraic builder skips null value rows for i64 folds" {
    const alloc = std.testing.allocator;
    const row_refs = [_]rowsource.RowRef{
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 0 } },
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 1 } },
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 2 } },
    };
    const tenants = [_][]const u8{ "t1", "t1", "t2" };
    const amounts = [_]i64{ 0, 9, 0 };
    const amount_nulls = [_]u8{ 1, 0, 1 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts }, .nulls = .{ .bytes = &amount_nulls } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "manifest-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    };

    var segment = try buildGroupByAggregateAlloc(alloc, batch, .{
        .source_kind = .serverless_fragment,
        .snapshot_id = "manifest-1",
        .schema_fingerprint = "schema-v1",
        .source_id = "orders",
        .group_column = "tenant",
        .value_column = "amount",
        .op = .min_i64,
    });
    defer segment.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), segment.aggregate.groups.len);
    try std.testing.expectEqualStrings("t1", segment.aggregate.groups[0].key);
    try std.testing.expectEqual(@as(i64, 9), segment.aggregate.groups[0].value.min_i64);
}
