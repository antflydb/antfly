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

pub const ExpressionSpec = struct {
    name: []const u8,
    value_column: []const u8 = &.{},
    op: algebraic_segment.AggregateOp,
};

pub const ExpressionBuildOptions = struct {
    source_kind: algebraic_segment.SourceKind,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    source_id: []const u8 = &.{},
    expressions: []const ExpressionSpec,
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

pub fn buildExpressionFoldsAlloc(
    alloc: Allocator,
    batch: rowsource.ColumnBatch,
    options: ExpressionBuildOptions,
) !algebraic_segment.ExpressionMaterialization {
    if (options.snapshot_id.len == 0) return error.InvalidAlgebraicSegmentBuildOptions;
    if (options.schema_fingerprint.len == 0) return error.InvalidAlgebraicSegmentBuildOptions;
    if (options.expressions.len == 0) return error.InvalidAlgebraicSegmentBuildOptions;
    try batch.validate();

    const expressions = try alloc.alloc(algebraic_segment.ExpressionFold, options.expressions.len);
    errdefer alloc.free(expressions);
    var initialized_expressions: usize = 0;
    errdefer {
        for (expressions[0..initialized_expressions]) |*expression| expression.deinit(alloc);
    }

    for (options.expressions, expressions) |spec, *out| {
        if (spec.name.len == 0) return error.InvalidAlgebraicSegmentBuildOptions;
        if (spec.op != .count and spec.value_column.len == 0) return error.InvalidAlgebraicSegmentBuildOptions;
        const value_column = if (spec.op == .count) null else batch.findColumn(spec.value_column) orelse return error.RowSourceColumnNotFound;
        if (value_column) |column| {
            if (column.kind() != .i64) return error.UnsupportedAlgebraicValueColumnKind;
        }
        const name = try alloc.dupe(u8, spec.name);
        errdefer alloc.free(name);
        var value_column_name: []u8 = &.{};
        if (spec.value_column.len != 0) value_column_name = try alloc.dupe(u8, spec.value_column);
        errdefer if (value_column_name.len != 0) alloc.free(value_column_name);
        out.* = .{
            .name = name,
            .value_column = value_column_name,
            .op = spec.op,
            .value = try expressionValue(spec.op, value_column, batch.rowCount()),
        };
        initialized_expressions += 1;
    }

    var materialization = algebraic_segment.ExpressionMaterialization{
        .source = .{
            .kind = options.source_kind,
            .snapshot_id = try alloc.dupe(u8, options.snapshot_id),
            .schema_fingerprint = try alloc.dupe(u8, options.schema_fingerprint),
            .source_id = if (options.source_id.len == 0) &.{} else try alloc.dupe(u8, options.source_id),
        },
        .expressions = expressions,
    };
    errdefer materialization.deinit(alloc);
    try materialization.validate();
    return materialization;
}

pub fn encodeExpressionFoldsAlloc(
    alloc: Allocator,
    batch: rowsource.ColumnBatch,
    options: ExpressionBuildOptions,
) ![]u8 {
    var materialization = try buildExpressionFoldsAlloc(alloc, batch, options);
    defer materialization.deinit(alloc);
    return codec.encodeExpressionAlloc(alloc, materialization);
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
        .avg_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .avg_i64 = .{
            .sum_i64 = value_column.?.values.i64[row_idx],
            .count = 1,
        } },
    };
}

fn expressionValue(
    op: algebraic_segment.AggregateOp,
    value_column: ?rowsource.ColumnVector,
    row_count: usize,
) !algebraic_segment.AggregateValue {
    return switch (op) {
        .count => .{ .count = @intCast(row_count) },
        .sum_i64 => blk: {
            var total: i64 = 0;
            for (0..row_count) |row_idx| {
                if (value_column.?.nulls.isNull(row_idx)) continue;
                total += value_column.?.values.i64[row_idx];
            }
            break :blk .{ .sum_i64 = total };
        },
        .min_i64 => blk: {
            var found = false;
            var best: i64 = 0;
            for (0..row_count) |row_idx| {
                if (value_column.?.nulls.isNull(row_idx)) continue;
                const value = value_column.?.values.i64[row_idx];
                if (!found or value < best) {
                    best = value;
                    found = true;
                }
            }
            if (!found) return error.EmptyAlgebraicExpressionFold;
            break :blk .{ .min_i64 = best };
        },
        .max_i64 => blk: {
            var found = false;
            var best: i64 = 0;
            for (0..row_count) |row_idx| {
                if (value_column.?.nulls.isNull(row_idx)) continue;
                const value = value_column.?.values.i64[row_idx];
                if (!found or value > best) {
                    best = value;
                    found = true;
                }
            }
            if (!found) return error.EmptyAlgebraicExpressionFold;
            break :blk .{ .max_i64 = best };
        },
        .avg_i64 => blk: {
            var total: i64 = 0;
            var count: u64 = 0;
            for (0..row_count) |row_idx| {
                if (value_column.?.nulls.isNull(row_idx)) continue;
                total += value_column.?.values.i64[row_idx];
                count += 1;
            }
            if (count == 0) return error.EmptyAlgebraicExpressionFold;
            break :blk .{ .avg_i64 = .{ .sum_i64 = total, .count = count } };
        },
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
        .avg_i64 => |left| .{ .avg_i64 = .{
            .sum_i64 = left.sum_i64 + rhs.avg_i64.sum_i64,
            .count = left.count + rhs.avg_i64.count,
        } },
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

test "algebraic builder computes avg folds" {
    const alloc = std.testing.allocator;
    const row_refs = [_]rowsource.RowRef{
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 0 } },
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 1 } },
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 2 } },
    };
    const tenants = [_][]const u8{ "t1", "t1", "t2" };
    const amounts = [_]i64{ 10, 20, 99 };
    const amount_nulls = [_]u8{ 0, 0, 1 };
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
        .op = .avg_i64,
    });
    defer segment.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), segment.aggregate.groups.len);
    try std.testing.expectEqualStrings("t1", segment.aggregate.groups[0].key);
    try std.testing.expectEqual(@as(i64, 30), segment.aggregate.groups[0].value.avg_i64.sum_i64);
    try std.testing.expectEqual(@as(u64, 2), segment.aggregate.groups[0].value.avg_i64.count);
    try std.testing.expectEqual(@as(f64, 15), segment.aggregate.groups[0].value.avg_i64.value().?);
}

test "algebraic builder computes expression folds" {
    const alloc = std.testing.allocator;
    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{ .source_id = "events", .snapshot_id = "iceberg-1", .file_id = "file-a", .row_group_ordinal = 0, .row_ordinal = 0 } },
        .{ .external = .{ .source_id = "events", .snapshot_id = "iceberg-1", .file_id = "file-a", .row_group_ordinal = 0, .row_ordinal = 1 } },
        .{ .external = .{ .source_id = "events", .snapshot_id = "iceberg-1", .file_id = "file-a", .row_group_ordinal = 0, .row_ordinal = 2 } },
    };
    const amounts = [_]i64{ 7, 11, 13 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "events", .snapshot_id = "iceberg-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    };
    const expressions = [_]ExpressionSpec{
        .{ .name = "row_count", .op = .count },
        .{ .name = "amount_sum", .value_column = "amount", .op = .sum_i64 },
        .{ .name = "amount_max", .value_column = "amount", .op = .max_i64 },
        .{ .name = "amount_avg", .value_column = "amount", .op = .avg_i64 },
    };

    var materialization = try buildExpressionFoldsAlloc(alloc, batch, .{
        .source_kind = .external_iceberg,
        .snapshot_id = "iceberg-1",
        .schema_fingerprint = "schema-v1",
        .source_id = "events",
        .expressions = &expressions,
    });
    defer materialization.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 4), materialization.expressions.len);
    try std.testing.expectEqual(@as(u64, 3), materialization.expressions[0].value.count);
    try std.testing.expectEqual(@as(i64, 31), materialization.expressions[1].value.sum_i64);
    try std.testing.expectEqual(@as(i64, 13), materialization.expressions[2].value.max_i64);
    try std.testing.expectEqual(@as(i64, 31), materialization.expressions[3].value.avg_i64.sum_i64);
    try std.testing.expectEqual(@as(u64, 3), materialization.expressions[3].value.avg_i64.count);

    const encoded = try encodeExpressionFoldsAlloc(alloc, batch, .{
        .source_kind = .external_iceberg,
        .snapshot_id = "iceberg-1",
        .schema_fingerprint = "schema-v1",
        .source_id = "events",
        .expressions = &expressions,
    });
    defer alloc.free(encoded);
    try std.testing.expect(encoded.len > 0);
}
