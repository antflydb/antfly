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

pub const CellValue = union(rowsource.ColumnKind) {
    bytes: []u8,
    json: []u8,
    i64: i64,
    f64: f64,
    bool: bool,
    vector_f32: []f32,

    pub fn deinit(self: *CellValue, alloc: Allocator) void {
        switch (self.*) {
            .bytes => |value| alloc.free(value),
            .json => |value| alloc.free(value),
            .vector_f32 => |value| alloc.free(value),
            else => {},
        }
        self.* = undefined;
    }
};

pub const ProjectedCell = struct {
    name: []u8,
    value: ?CellValue,

    pub fn deinit(self: *ProjectedCell, alloc: Allocator) void {
        alloc.free(self.name);
        if (self.value) |*value| value.deinit(alloc);
        self.* = undefined;
    }
};

pub const ProjectedRow = struct {
    row_ref: rowsource.RowRef,
    cells: []ProjectedCell,

    pub fn deinit(self: *ProjectedRow, alloc: Allocator) void {
        for (self.cells) |*cell| cell.deinit(alloc);
        alloc.free(self.cells);
        self.* = undefined;
    }

    pub fn find(self: ProjectedRow, name: []const u8) ?ProjectedCell {
        for (self.cells) |cell| {
            if (std.mem.eql(u8, cell.name, name)) return cell;
        }
        return null;
    }
};

pub const HydrateResult = struct {
    rows: []ProjectedRow,
    total: u32 = 0,

    pub fn deinit(self: *HydrateResult, alloc: Allocator) void {
        for (self.rows) |*row| row.deinit(alloc);
        alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const PredicateOp = enum {
    eq_bytes,
    eq_i64,
    eq_bool,
};

pub const Predicate = struct {
    column: []const u8,
    op: PredicateOp,
    bytes_value: []const u8 = &.{},
    i64_value: i64 = 0,
    bool_value: bool = false,
};

pub const ScanRequest = struct {
    projected_columns: []const []const u8,
    predicate: ?Predicate = null,
    limit: ?usize = null,
};

pub const ScanResult = HydrateResult;

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

pub fn scanRowsAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    request: ScanRequest,
) !ScanResult {
    try validateScanRequest(request);

    var rows = std.ArrayListUnmanaged(ProjectedRow).empty;
    errdefer {
        for (rows.items) |*row| row.deinit(alloc);
        rows.deinit(alloc);
    }

    if (request.limit != null and request.limit.? == 0) {
        return .{ .rows = try rows.toOwnedSlice(alloc), .total = 0 };
    }

    var total: u32 = 0;
    while (try source.next(alloc)) |batch| {
        const predicate_column = if (request.predicate) |predicate|
            batch.findColumn(predicate.column) orelse return error.RowSourceColumnNotFound
        else
            null;

        for (0..batch.rowCount()) |row_idx| {
            if (request.predicate) |predicate| {
                if (!try predicateMatches(predicate, predicate_column.?, row_idx)) continue;
            }
            total += 1;
            if (!limitReached(rows.items.len, request.limit)) {
                try rows.append(alloc, try projectRowAlloc(alloc, batch, row_idx, request.projected_columns));
            }
        }
    }

    return .{ .rows = try rows.toOwnedSlice(alloc), .total = total };
}

pub fn hydrateRowsAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    wanted_refs: []const rowsource.RowRef,
    projected_columns: []const []const u8,
) !HydrateResult {
    if (wanted_refs.len == 0) return .{ .rows = try alloc.alloc(ProjectedRow, 0), .total = 0 };
    if (projected_columns.len == 0) return error.InvalidLakeRowsQuery;

    var rows = std.ArrayListUnmanaged(ProjectedRow).empty;
    errdefer {
        for (rows.items) |*row| row.deinit(alloc);
        rows.deinit(alloc);
    }

    while (try source.next(alloc)) |batch| {
        for (batch.row_refs, 0..) |row_ref, row_idx| {
            if (!containsRowRef(wanted_refs, row_ref)) continue;
            try rows.append(alloc, try projectRowAlloc(alloc, batch, row_idx, projected_columns));
            if (rows.items.len == wanted_refs.len) break;
        }
        if (rows.items.len == wanted_refs.len) break;
    }

    return .{ .rows = try rows.toOwnedSlice(alloc), .total = @intCast(rows.items.len) };
}

fn validateRequest(request: GroupByRequest) !void {
    if (request.group_column.len == 0) return error.InvalidLakeRowsQuery;
    if (request.op != .count and request.value_column.len == 0) return error.InvalidLakeRowsQuery;
}

fn validateScanRequest(request: ScanRequest) !void {
    if (request.projected_columns.len == 0) return error.InvalidLakeRowsQuery;
    for (request.projected_columns) |column| {
        if (column.len == 0) return error.InvalidLakeRowsQuery;
    }
    if (request.predicate) |predicate| {
        if (predicate.column.len == 0) return error.InvalidLakeRowsQuery;
    }
}

fn limitReached(row_count: usize, limit: ?usize) bool {
    return limit != null and row_count >= limit.?;
}

fn predicateMatches(predicate: Predicate, column: rowsource.ColumnVector, row_idx: usize) !bool {
    if (column.nulls.isNull(row_idx)) return false;
    return switch (predicate.op) {
        .eq_bytes => switch (column.values) {
            .bytes => |items| std.mem.eql(u8, items[row_idx], predicate.bytes_value),
            .json => |items| std.mem.eql(u8, items[row_idx], predicate.bytes_value),
            else => error.UnsupportedLakeRowsPredicateColumnKind,
        },
        .eq_i64 => switch (column.values) {
            .i64 => |items| items[row_idx] == predicate.i64_value,
            else => error.UnsupportedLakeRowsPredicateColumnKind,
        },
        .eq_bool => switch (column.values) {
            .bool => |items| items[row_idx] == predicate.bool_value,
            else => error.UnsupportedLakeRowsPredicateColumnKind,
        },
    };
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

fn projectRowAlloc(
    alloc: Allocator,
    batch: rowsource.ColumnBatch,
    row_idx: usize,
    projected_columns: []const []const u8,
) !ProjectedRow {
    const cells = try alloc.alloc(ProjectedCell, projected_columns.len);
    errdefer alloc.free(cells);
    var initialized: usize = 0;
    errdefer {
        for (cells[0..initialized]) |*cell| cell.deinit(alloc);
    }

    for (projected_columns, cells) |name, *out| {
        const column = batch.findColumn(name) orelse return error.RowSourceColumnNotFound;
        out.* = .{
            .name = try alloc.dupe(u8, name),
            .value = if (column.nulls.isNull(row_idx)) null else try cloneCellValueAlloc(alloc, column.values, row_idx),
        };
        initialized += 1;
    }

    return .{
        .row_ref = batch.row_refs[row_idx],
        .cells = cells,
    };
}

fn cloneCellValueAlloc(
    alloc: Allocator,
    values: rowsource.ColumnValues,
    row_idx: usize,
) !CellValue {
    return switch (values) {
        .bytes => |items| .{ .bytes = try alloc.dupe(u8, items[row_idx]) },
        .json => |items| .{ .json = try alloc.dupe(u8, items[row_idx]) },
        .i64 => |items| .{ .i64 = items[row_idx] },
        .f64 => |items| .{ .f64 = items[row_idx] },
        .bool => |items| .{ .bool = items[row_idx] },
        .vector_f32 => |items| .{ .vector_f32 = try alloc.dupe(f32, items[row_idx]) },
    };
}

fn containsRowRef(haystack: []const rowsource.RowRef, needle: rowsource.RowRef) bool {
    for (haystack) |candidate| {
        if (rowRefsEqual(candidate, needle)) return true;
    }
    return false;
}

fn rowRefsEqual(a: rowsource.RowRef, b: rowsource.RowRef) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .relational_key => |key| std.mem.eql(u8, key, b.relational_key),
        .serverless => |value| blk: {
            const other = b.serverless;
            break :blk std.mem.eql(u8, value.fragment_id, other.fragment_id) and
                value.row_ordinal == other.row_ordinal;
        },
        .external => |value| blk: {
            const other = b.external;
            break :blk std.mem.eql(u8, value.source_id, other.source_id) and
                std.mem.eql(u8, value.snapshot_id, other.snapshot_id) and
                std.mem.eql(u8, value.file_id, other.file_id) and
                value.row_group_ordinal == other.row_group_ordinal and
                value.row_ordinal == other.row_ordinal;
        },
    };
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

test "lake rows scans projected local rows with a predicate" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
        .{ .relational_key = "row:c" },
    };
    const tenants = [_][]const u8{ "t1", "t2", "t2" };
    const amounts = [_]i64{ 10, 20, 30 };
    const active = [_]bool{ true, true, false };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
        .{ .name = "active", .values = .{ .bool = &active } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "lsm-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try local.relationalStoreSource(&batches);

    const projection = [_][]const u8{ "amount", "active" };
    var result = try scanRowsAlloc(alloc, batch_source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "tenant",
            .op = .eq_bytes,
            .bytes_value = "t2",
        },
        .limit = 1,
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expect(rowRefsEqual(row_refs[1], result.rows[0].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
    try std.testing.expectEqual(true, result.rows[0].find("active").?.value.?.bool);
}

test "lake rows scans external rows through the same projection contract" {
    const alloc = std.testing.allocator;
    const external = @import("../../storage/rowsource/external.zig");

    const binding = external.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external.makeRowRef(binding, "file-a.parquet", 0, 0),
        try external.makeRowRef(binding, "file-a.parquet", 0, 1),
        try external.makeRowRef(binding, "file-b.parquet", 1, 0),
    };
    const tenants = [_][]const u8{ "t1", "t2", "t2" };
    const amounts = [_]i64{ 10, 20, 30 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try external.BatchSource.init(binding, &batches);

    const projection = [_][]const u8{"amount"};
    var result = try scanRowsAlloc(alloc, batch_source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "tenant",
            .op = .eq_bytes,
            .bytes_value = "t2",
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expect(rowRefsEqual(row_refs[1], result.rows[0].row_ref));
    try std.testing.expect(rowRefsEqual(row_refs[2], result.rows[1].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
    try std.testing.expectEqual(@as(i64, 30), result.rows[1].find("amount").?.value.?.i64);
}

test "lake rows hydrates projected cells by row ref" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
    };
    const amounts = [_]i64{ 10, 20 };
    const attrs = [_][]const u8{ "{\"tier\":\"free\"}", "{\"tier\":\"pro\"}" };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
        .{ .name = "attrs", .values = .{ .json = &attrs } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "lsm-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try local.relationalStoreSource(&batches);

    const wanted = [_]rowsource.RowRef{.{ .relational_key = "row:b" }};
    const projection = [_][]const u8{ "amount", "attrs" };
    var result = try hydrateRowsAlloc(alloc, batch_source.rowSource(), &wanted, &projection);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expect(rowRefsEqual(wanted[0], result.rows[0].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
    try std.testing.expectEqualStrings("{\"tier\":\"pro\"}", result.rows[0].find("attrs").?.value.?.json);
}

test "lake rows hydrates projected cells from serverless row fragments" {
    const alloc = std.testing.allocator;
    const row_fragment = @import("../row_fragment/mod.zig");

    var fragment = row_fragment.Fragment{
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .row_refs = try alloc.alloc(row_fragment.RowRef, 2),
        .columns = try alloc.alloc(row_fragment.Column, 1),
    };
    defer fragment.deinit(alloc);
    fragment.row_refs[0] = .{ .key = try alloc.dupe(u8, "row:a"), .ordinal = 0 };
    fragment.row_refs[1] = .{ .key = try alloc.dupe(u8, "row:b"), .ordinal = 1 };
    fragment.columns[0] = .{
        .name = try alloc.dupe(u8, "amount"),
        .kind = .i64,
        .values = try alloc.alloc(row_fragment.CellValue, 2),
    };
    fragment.columns[0].values[0] = .{ .i64 = 10 };
    fragment.columns[0].values[1] = .{ .i64 = 20 };

    var fragment_source = row_fragment.FragmentSource.init(
        .{ .table_id = "orders", .snapshot_id = "manifest-7" },
        "frag-1",
        &fragment,
    );
    defer fragment_source.deinit(alloc);

    const wanted = [_]rowsource.RowRef{.{ .serverless = .{
        .fragment_id = "frag-1",
        .row_ordinal = 1,
    } }};
    const projection = [_][]const u8{"amount"};
    var result = try hydrateRowsAlloc(alloc, fragment_source.rowSource(), &wanted, &projection);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expect(rowRefsEqual(wanted[0], result.rows[0].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
}
