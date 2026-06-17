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

//! Lake-native query execution scaffold over RowSource. This is intentionally
//! small: it proves serverless row fragments, local relational batches, and
//! external lake batches can share one execution path, while allowing an
//! algebraic segment to satisfy repeated group-by aggregate workloads.

const std = @import("std");
const Allocator = std.mem.Allocator;
const algebraic_segment = @import("../algebraic_segment/mod.zig");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const GroupByRequest = struct {
    group_column: []const u8,
    value_column: []const u8 = &.{},
    op: algebraic_segment.AggregateOp,
};

pub const GroupResult = struct {
    key: []u8,
    value: algebraic_segment.AggregateValue,

    pub fn deinit(self: *GroupResult, alloc: Allocator) void {
        alloc.free(self.key);
        self.* = undefined;
    }
};

pub const GroupByResult = struct {
    groups: []GroupResult,
    source: enum { rowsource_scan, algebraic_segment },

    pub fn deinit(self: *GroupByResult, alloc: Allocator) void {
        for (self.groups) |*group| group.deinit(alloc);
        alloc.free(self.groups);
        self.* = undefined;
    }

    pub fn find(self: GroupByResult, key: []const u8) ?algebraic_segment.AggregateValue {
        for (self.groups) |group| {
            if (std.mem.eql(u8, group.key, key)) return group.value;
        }
        return null;
    }
};

pub fn executeGroupByAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    request: GroupByRequest,
    materialized: ?*const algebraic_segment.Reader,
) !GroupByResult {
    try validateRequest(request);
    if (materialized) |reader| {
        if (materializedMatches(reader.*, request)) {
            return try resultFromAlgebraicAlloc(alloc, reader.*);
        }
    }
    return try resultFromSourceAlloc(alloc, source, request);
}

fn validateRequest(request: GroupByRequest) !void {
    if (request.group_column.len == 0) return error.InvalidLakeRowsQuery;
    if (request.op != .count and request.value_column.len == 0) return error.InvalidLakeRowsQuery;
}

fn materializedMatches(reader: algebraic_segment.Reader, request: GroupByRequest) bool {
    return std.mem.eql(u8, reader.segment.aggregate.group_column, request.group_column) and
        std.mem.eql(u8, reader.segment.aggregate.value_column, request.value_column) and
        reader.segment.aggregate.op == request.op;
}

fn resultFromAlgebraicAlloc(
    alloc: Allocator,
    reader: algebraic_segment.Reader,
) !GroupByResult {
    const groups = try alloc.alloc(GroupResult, reader.segment.aggregate.groups.len);
    errdefer alloc.free(groups);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |*group| group.deinit(alloc);
    }

    for (reader.segment.aggregate.groups, groups) |group, *out| {
        out.* = .{
            .key = try alloc.dupe(u8, group.key),
            .value = group.value,
        };
        initialized += 1;
    }

    return .{ .groups = groups, .source = .algebraic_segment };
}

fn resultFromSourceAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    request: GroupByRequest,
) !GroupByResult {
    var map = std.StringHashMapUnmanaged(algebraic_segment.AggregateValue).empty;
    defer map.deinit(alloc);
    defer {
        var key_it = map.keyIterator();
        while (key_it.next()) |key| alloc.free(key.*);
    }

    while (try source.next(alloc)) |batch| {
        const group_column = batch.findColumn(request.group_column) orelse return error.RowSourceColumnNotFound;
        if (group_column.kind() != .bytes) return error.UnsupportedLakeRowsGroupColumnKind;
        const value_column = if (request.op == .count) null else batch.findColumn(request.value_column) orelse return error.RowSourceColumnNotFound;
        if (value_column) |column| {
            if (column.kind() != .i64) return error.UnsupportedLakeRowsValueColumnKind;
        }

        for (0..batch.rowCount()) |row_idx| {
            if (group_column.nulls.isNull(row_idx)) continue;
            const key = group_column.values.bytes[row_idx];
            if (key.len == 0) continue;
            const next_value = rowAggregateValue(request.op, value_column, row_idx) orelse continue;
            const owned_key = try alloc.dupe(u8, key);
            errdefer alloc.free(owned_key);
            const entry = try map.getOrPut(alloc, key);
            if (!entry.found_existing) {
                entry.key_ptr.* = owned_key;
                entry.value_ptr.* = next_value;
            } else {
                alloc.free(owned_key);
                entry.value_ptr.* = combine(entry.value_ptr.*, next_value);
            }
        }
    }

    const groups = try alloc.alloc(GroupResult, map.count());
    errdefer alloc.free(groups);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |*group| group.deinit(alloc);
    }

    var it = map.iterator();
    while (it.next()) |entry| {
        groups[initialized] = .{
            .key = try alloc.dupe(u8, entry.key_ptr.*),
            .value = entry.value_ptr.*,
        };
        initialized += 1;
    }
    std.mem.sort(GroupResult, groups, {}, compareGroupResult);
    return .{ .groups = groups, .source = .rowsource_scan };
}

fn rowAggregateValue(
    op: algebraic_segment.AggregateOp,
    value_column: ?rowsource.ColumnVector,
    row_idx: usize,
) ?algebraic_segment.AggregateValue {
    return switch (op) {
        .count => .{ .count = 1 },
        .sum_i64 => .{ .sum_i64 = if (value_column.?.nulls.isNull(row_idx)) 0 else value_column.?.values.i64[row_idx] },
        .min_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .min_i64 = value_column.?.values.i64[row_idx] },
        .max_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .max_i64 = value_column.?.values.i64[row_idx] },
    };
}

fn combine(
    lhs: algebraic_segment.AggregateValue,
    rhs: algebraic_segment.AggregateValue,
) algebraic_segment.AggregateValue {
    return switch (lhs) {
        .count => |left| .{ .count = left + rhs.count },
        .sum_i64 => |left| .{ .sum_i64 = left + rhs.sum_i64 },
        .min_i64 => |left| .{ .min_i64 = @min(left, rhs.min_i64) },
        .max_i64 => |left| .{ .max_i64 = @max(left, rhs.max_i64) },
    };
}

fn compareGroupResult(_: void, lhs: GroupResult, rhs: GroupResult) bool {
    return std.mem.lessThan(u8, lhs.key, rhs.key);
}

test "lake rows group-by scans RowSource batches" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
        .{ .relational_key = "row:c" },
    };
    const tenants = [_][]const u8{ "t2", "t1", "t2" };
    const amounts = [_]i64{ 7, 11, 13 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "lsm-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try local.relationalStoreSource(&batches);

    var result = try executeGroupByAlloc(alloc, batch_source.rowSource(), .{
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
    }, null);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.groups.len);
    try std.testing.expectEqual(@as(i64, 11), result.find("t1").?.sum_i64);
    try std.testing.expectEqual(@as(i64, 20), result.find("t2").?.sum_i64);
    try std.testing.expectEqual(.rowsource_scan, result.source);
}

test "lake rows group-by can use algebraic segment materialization" {
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
            .groups = try alloc.alloc(algebraic_segment.GroupFold, 1),
        },
    };
    defer segment.deinit(alloc);
    segment.aggregate.groups[0] = .{
        .key = try alloc.dupe(u8, "t1"),
        .value = .{ .sum_i64 = 42 },
    };

    const encoded = try algebraic_segment.encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    var reader = try algebraic_segment.Reader.decodeAlloc(alloc, encoded);
    defer reader.deinit();

    const EmptySource = struct {
        fn next(_: *anyopaque, _: Allocator) !?rowsource.ColumnBatch {
            return null;
        }
    };
    var dummy: u8 = 0;
    const source = rowsource.Source{
        .kind = .serverless_fragment,
        .ctx = &dummy,
        .next_batch = EmptySource.next,
    };

    var result = try executeGroupByAlloc(alloc, source, .{
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
    }, &reader);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.groups.len);
    try std.testing.expectEqual(@as(i64, 42), result.find("t1").?.sum_i64);
    try std.testing.expectEqual(.algebraic_segment, result.source);
}
