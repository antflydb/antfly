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

//! Row-fragment publication core for Antfly-owned lake-native serverless data.

const std = @import("std");
const Allocator = std.mem.Allocator;
const row_fragment = @import("../row_fragment/mod.zig");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const BuildOptions = struct {
    schema_fingerprint: []const u8,
    projected_columns: ?[]const []const u8 = null,
    max_dictionary_samples: usize = 16,
};

pub fn buildFragmentFromBatchAlloc(
    alloc: Allocator,
    batch: rowsource.ColumnBatch,
    options: BuildOptions,
) !row_fragment.Fragment {
    if (options.schema_fingerprint.len == 0) return error.InvalidRowFragmentBuildOptions;
    try batch.validate();

    var builder = try row_fragment.Builder.init(alloc, options.schema_fingerprint);
    errdefer builder.deinit();

    for (batch.row_refs, 0..) |row_ref, ordinal| {
        const key = try rowRefKeyAlloc(alloc, row_ref);
        defer alloc.free(key);
        try builder.appendRowWithOrdinal(key, ordinal);
    }

    if (options.projected_columns) |projection| {
        for (projection) |name| {
            const column = batch.findColumn(name) orelse return error.RowSourceColumnNotFound;
            try appendColumnFromVector(alloc, &builder, column);
        }
    } else {
        for (batch.columns) |column| {
            try appendColumnFromVector(alloc, &builder, column);
        }
    }

    return try builder.finish();
}

pub fn encodeFragmentFromBatchAlloc(
    alloc: Allocator,
    batch: rowsource.ColumnBatch,
    options: BuildOptions,
) ![]u8 {
    var fragment = try buildFragmentFromBatchAlloc(alloc, batch, options);
    defer fragment.deinit(alloc);
    return row_fragment.encodeAlloc(alloc, fragment);
}

pub fn buildFragmentStatsFromBatchAlloc(
    alloc: Allocator,
    batch: rowsource.ColumnBatch,
    options: BuildOptions,
) !row_fragment.FragmentStats {
    var fragment = try buildFragmentFromBatchAlloc(alloc, batch, options);
    defer fragment.deinit(alloc);
    return try row_fragment.buildStatsAlloc(alloc, fragment, options.max_dictionary_samples);
}

pub fn encodeFragmentStatsFromBatchAlloc(
    alloc: Allocator,
    batch: rowsource.ColumnBatch,
    options: BuildOptions,
) ![]u8 {
    var stats = try buildFragmentStatsFromBatchAlloc(alloc, batch, options);
    defer stats.deinit(alloc);
    return try row_fragment.encodeStatsAlloc(alloc, stats);
}

fn appendColumnFromVector(
    alloc: Allocator,
    builder: *row_fragment.Builder,
    column: rowsource.ColumnVector,
) !void {
    const values = try alloc.alloc(row_fragment.CellValue, column.rowCount());
    defer alloc.free(values);

    for (values, 0..) |*out, idx| {
        out.* = if (column.nulls.isNull(idx)) .null else try cellFromColumnValue(column.values, idx);
    }

    try builder.appendColumn(column.name, try fragmentColumnKind(column.kind()), values);
}

fn fragmentColumnKind(kind: rowsource.ColumnKind) !row_fragment.ColumnKind {
    return switch (kind) {
        .bytes => .bytes,
        .json => .json,
        .i64 => .i64,
        .f64 => .f64,
        .bool => .bool,
        .vector_f32 => .vector_f32,
    };
}

fn cellFromColumnValue(values: rowsource.ColumnValues, idx: usize) !row_fragment.CellValue {
    return switch (values) {
        .bytes => |items| .{ .bytes = @constCast(items[idx]) },
        .json => |items| .{ .json = @constCast(items[idx]) },
        .i64 => |items| .{ .i64 = items[idx] },
        .f64 => |items| .{ .f64 = items[idx] },
        .bool => |items| .{ .bool = items[idx] },
        .vector_f32 => |items| .{ .vector_f32 = @constCast(items[idx]) },
    };
}

fn rowRefKeyAlloc(alloc: Allocator, row_ref: rowsource.RowRef) ![]u8 {
    return switch (row_ref) {
        .relational_key => |key| try alloc.dupe(u8, key),
        .serverless => |value| try std.fmt.allocPrint(
            alloc,
            "serverless:{s}:{d}",
            .{ value.fragment_id, value.row_ordinal },
        ),
        .external => |value| try std.fmt.allocPrint(
            alloc,
            "external:{s}:{s}:{s}:{d}:{d}",
            .{
                value.source_id,
                value.snapshot_id,
                value.file_id,
                value.row_group_ordinal,
                value.row_ordinal,
            },
        ),
    };
}

test "row fragment publisher builds projected fragment from column batch" {
    const alloc = std.testing.allocator;
    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
    };
    const amounts = [_]i64{ 10, 20 };
    const tenants = [_][]const u8{ "t1", "t2" };
    const embedding_a = [_]f32{ 1.0, 0.0 };
    const embedding_b = [_]f32{ 0.5, 0.5 };
    const embeddings = [_][]const f32{ &embedding_a, &embedding_b };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
        .{ .name = "embedding", .values = .{ .vector_f32 = &embeddings } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "snap-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    };

    const projection = [_][]const u8{ "amount", "embedding" };
    var fragment = try buildFragmentFromBatchAlloc(alloc, batch, .{
        .schema_fingerprint = "schema-v1",
        .projected_columns = &projection,
    });
    defer fragment.deinit(alloc);

    try std.testing.expectEqualStrings("schema-v1", fragment.schema_fingerprint);
    try std.testing.expectEqual(@as(usize, 2), fragment.rowCount());
    try std.testing.expectEqual(@as(usize, 2), fragment.columns.len);
    try std.testing.expectEqualStrings("amount", fragment.columns[0].name);
    try std.testing.expectEqual(@as(i64, 20), fragment.columns[0].values[1].i64);
    try std.testing.expectEqualStrings("embedding", fragment.columns[1].name);
    try std.testing.expectEqual(@as(f32, 0.5), fragment.columns[1].values[1].vector_f32[0]);
    try std.testing.expectEqual(@as(f32, 0.5), fragment.columns[1].values[1].vector_f32[1]);
}

test "row fragment publisher builds stats for projected columns" {
    const alloc = std.testing.allocator;
    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
        .{ .relational_key = "row:c" },
    };
    const amounts = [_]i64{ 30, 10, 20 };
    const tenants = [_][]const u8{ "t2", "t1", "t2" };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "snap-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    };

    const projection = [_][]const u8{"amount"};
    var stats = try buildFragmentStatsFromBatchAlloc(alloc, batch, .{
        .schema_fingerprint = "schema-v1",
        .projected_columns = &projection,
    });
    defer stats.deinit(alloc);

    try std.testing.expectEqual(@as(u64, 3), stats.row_count);
    try std.testing.expect(stats.findColumn("tenant") == null);
    const amount = stats.findColumn("amount").?;
    try std.testing.expectEqual(@as(i64, 10), amount.min.?.i64);
    try std.testing.expectEqual(@as(i64, 30), amount.max.?.i64);

    const encoded = try encodeFragmentStatsFromBatchAlloc(alloc, batch, .{
        .schema_fingerprint = "schema-v1",
        .projected_columns = &projection,
    });
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"name\":\"amount\"") != null);
}
