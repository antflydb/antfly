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

//! Iceberg delete application helpers.
//!
//! Position delete files describe rows by data-file path plus a zero-based row
//! position within that file. Antfly's lake row refs are row-group-local, so the
//! delete application path needs a pinned inventory with row-group metadata
//! before deleted rows can be filtered from scan/hydration results.

const std = @import("std");
const Allocator = std.mem.Allocator;
const external_source = @import("../external_source/types.zig");
const rowsource_bridge = @import("../external_source/rowsource_bridge.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const lake_rows = @import("lake_rows.zig");

pub const max_equality_delete_key_storage_bytes: usize = 256 * 1024 * 1024;
pub const equality_delete_key_entry_overhead_bytes: usize = 32;

pub const PositionDeleteRow = struct {
    data_file_path: []const u8,
    row_position: u64,

    pub fn validate(self: PositionDeleteRow) !void {
        if (self.data_file_path.len == 0) return error.InvalidIcebergPositionDelete;
    }
};

pub const PositionDeleteColumnNames = struct {
    data_file_path: []const u8 = "file_path",
    row_position: []const u8 = "pos",

    pub fn validate(self: PositionDeleteColumnNames) !void {
        if (self.data_file_path.len == 0) return error.InvalidIcebergPositionDeleteColumns;
        if (self.row_position.len == 0) return error.InvalidIcebergPositionDeleteColumns;
        if (std.mem.eql(u8, self.data_file_path, self.row_position)) return error.InvalidIcebergPositionDeleteColumns;
    }
};

pub fn positionDeleteScanResultToRowRefsAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    result: lake_rows.ScanResult,
    columns: PositionDeleteColumnNames,
) ![]rowsource.RowRef {
    try columns.validate();
    var delete_rows = std.ArrayListUnmanaged(PositionDeleteRow).empty;
    defer delete_rows.deinit(alloc);
    for (result.rows) |row| {
        try delete_rows.append(alloc, try positionDeleteRowFromProjectedRow(row, columns));
    }
    return try positionDeleteRowsToRowRefsAlloc(alloc, inventory, delete_rows.items);
}

pub fn positionDeleteRowsToRowRefsAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    deletes: []const PositionDeleteRow,
) ![]rowsource.RowRef {
    try inventory.validate();
    if (inventory.format != .iceberg) return error.InvalidIcebergPositionDeleteInventory;

    var out = std.ArrayListUnmanaged(rowsource.RowRef).empty;
    errdefer out.deinit(alloc);

    for (deletes) |delete_row| {
        try delete_row.validate();
        const file = inventoryFileForDeletePath(inventory, delete_row.data_file_path) orelse {
            return error.IcebergPositionDeleteDataFileNotFound;
        };
        if (file.row_groups.len == 0) return error.MissingIcebergPositionDeleteRowGroupMetadata;

        const mapped = rowGroupPositionForFilePosition(file, delete_row.row_position) orelse {
            return error.IcebergPositionDeleteRowOutOfBounds;
        };
        const row_ref = try rowsource_bridge.rowRefForInventoryRow(
            inventory,
            file.file_id,
            mapped.row_group_ordinal,
            mapped.row_ordinal,
        );
        if (!containsRowRef(out.items, row_ref)) try out.append(alloc, row_ref);
    }

    return try out.toOwnedSlice(alloc);
}

pub fn equalityDeleteScanResultsToRowRefsAlloc(
    alloc: Allocator,
    data_rows: lake_rows.ScanResult,
    delete_rows: lake_rows.ScanResult,
    equality_columns: []const []const u8,
) ![]rowsource.RowRef {
    try validateEqualityColumns(equality_columns);

    var delete_keys = std.StringHashMapUnmanaged(void).empty;
    defer {
        var iter = delete_keys.keyIterator();
        while (iter.next()) |key| alloc.free(key.*);
        delete_keys.deinit(alloc);
    }
    var stored_key_bytes: usize = 0;
    for (delete_rows.rows) |delete_row| {
        const key = try equalityKeyAlloc(alloc, delete_row, equality_columns);
        errdefer alloc.free(key);
        const entry = try delete_keys.getOrPut(alloc, key);
        if (entry.found_existing) {
            alloc.free(key);
        } else {
            const next_storage = try admitEqualityDeleteKeyStorage(stored_key_bytes, key.len, 0);
            entry.value_ptr.* = {};
            stored_key_bytes = next_storage;
        }
    }

    var out = std.ArrayListUnmanaged(rowsource.RowRef).empty;
    errdefer out.deinit(alloc);

    for (data_rows.rows) |data_row| {
        const key = try equalityKeyAlloc(alloc, data_row, equality_columns);
        defer alloc.free(key);
        if (delete_keys.contains(key)) try out.append(alloc, data_row.row_ref);
    }

    return try out.toOwnedSlice(alloc);
}

pub fn admitEqualityDeleteKeyStorage(
    current: usize,
    key_bytes: usize,
    file_index_count: usize,
) !usize {
    var additional = std.math.add(usize, key_bytes, equality_delete_key_entry_overhead_bytes) catch
        return error.IcebergEqualityDeleteSetTooLarge;
    const file_index_bytes = std.math.mul(usize, file_index_count, @sizeOf(usize)) catch
        return error.IcebergEqualityDeleteSetTooLarge;
    additional = std.math.add(usize, additional, file_index_bytes) catch
        return error.IcebergEqualityDeleteSetTooLarge;
    return admitEqualityDeleteStorage(current, additional);
}

pub fn admitEqualityDeleteStorage(current: usize, additional: usize) !usize {
    const next = std.math.add(usize, current, additional) catch
        return error.IcebergEqualityDeleteSetTooLarge;
    if (next > max_equality_delete_key_storage_bytes) return error.IcebergEqualityDeleteSetTooLarge;
    return next;
}

pub fn equalityKeyAlloc(
    alloc: Allocator,
    row: lake_rows.ProjectedRow,
    equality_columns: []const []const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (equality_columns) |column| {
        const cell = row.find(column) orelse return error.IcebergEqualityDeleteColumnNotFound;
        const value = cell.value orelse {
            try out.append(alloc, 0);
            continue;
        };
        switch (value) {
            .bytes => |bytes| {
                try out.append(alloc, 1);
                const len = std.math.cast(u64, bytes.len) orelse return error.IcebergEqualityDeleteSetTooLarge;
                var len_bytes: [8]u8 = undefined;
                std.mem.writeInt(u64, &len_bytes, len, .little);
                try out.appendSlice(alloc, &len_bytes);
                try out.appendSlice(alloc, bytes);
            },
            .i64 => |number| {
                try out.append(alloc, 2);
                var bytes: [8]u8 = undefined;
                std.mem.writeInt(i64, &bytes, number, .little);
                try out.appendSlice(alloc, &bytes);
            },
            .f64 => |number| {
                if (std.math.isNan(number)) return error.UnsupportedIcebergEqualityDeleteColumn;
                try out.append(alloc, 3);
                const canonical: f64 = if (number == 0) 0 else number;
                var bytes: [8]u8 = undefined;
                std.mem.writeInt(u64, &bytes, @bitCast(canonical), .little);
                try out.appendSlice(alloc, &bytes);
            },
            .bool => |boolean| {
                try out.append(alloc, 4);
                try out.append(alloc, @intFromBool(boolean));
            },
            .json, .vector_f32 => return error.UnsupportedIcebergEqualityDeleteColumn,
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn validateEqualityColumns(columns: []const []const u8) !void {
    if (columns.len == 0) return error.InvalidIcebergEqualityDeleteColumns;
    for (columns, 0..) |column, idx| {
        if (column.len == 0) return error.InvalidIcebergEqualityDeleteColumns;
        for (columns[0..idx]) |previous| {
            if (std.mem.eql(u8, previous, column)) return error.InvalidIcebergEqualityDeleteColumns;
        }
    }
}

fn positionDeleteRowFromProjectedRow(
    row: lake_rows.ProjectedRow,
    columns: PositionDeleteColumnNames,
) !PositionDeleteRow {
    const data_file_path = row.find(columns.data_file_path) orelse return error.IcebergPositionDeleteColumnNotFound;
    const row_position = row.find(columns.row_position) orelse return error.IcebergPositionDeleteColumnNotFound;
    const path_value = data_file_path.value orelse return error.InvalidIcebergPositionDelete;
    const position_value = row_position.value orelse return error.InvalidIcebergPositionDelete;
    const path = switch (path_value) {
        .bytes => |value| value,
        else => return error.UnsupportedIcebergPositionDeleteColumn,
    };
    const position_i64 = switch (position_value) {
        .i64 => |value| value,
        else => return error.UnsupportedIcebergPositionDeleteColumn,
    };
    if (position_i64 < 0) return error.InvalidIcebergPositionDelete;
    return .{
        .data_file_path = path,
        .row_position = @intCast(position_i64),
    };
}

const RowGroupPosition = struct {
    row_group_ordinal: u32,
    row_ordinal: u64,
};

fn inventoryFileForDeletePath(
    inventory: external_source.Inventory,
    data_file_path: []const u8,
) ?external_source.FileEntry {
    for (inventory.files) |file| {
        if (std.mem.eql(u8, file.file_id, data_file_path)) return file;
        if (std.mem.eql(u8, file.object_uri, data_file_path)) return file;
    }
    return null;
}

fn rowGroupPositionForFilePosition(
    file: external_source.FileEntry,
    row_position: u64,
) ?RowGroupPosition {
    if (row_position >= file.row_count) return null;
    var remaining = row_position;
    for (file.row_groups) |row_group| {
        if (remaining < row_group.row_count) {
            return .{
                .row_group_ordinal = row_group.ordinal,
                .row_ordinal = remaining,
            };
        }
        remaining -= row_group.row_count;
    }
    return null;
}

fn containsRowRef(haystack: []const rowsource.RowRef, needle: rowsource.RowRef) bool {
    for (haystack) |candidate| {
        if (rowRefsEqual(candidate, needle)) return true;
    }
    return false;
}

fn rowRefsEqual(a: rowsource.RowRef, b: rowsource.RowRef) bool {
    return switch (a) {
        .external => |left| switch (b) {
            .external => |right| std.mem.eql(u8, left.source_id, right.source_id) and
                std.mem.eql(u8, left.snapshot_id, right.snapshot_id) and
                std.mem.eql(u8, left.file_id, right.file_id) and
                left.row_group_ordinal == right.row_group_ordinal and
                left.row_ordinal == right.row_ordinal,
            else => false,
        },
        .serverless => |left| switch (b) {
            .serverless => |right| std.mem.eql(u8, left.fragment_id, right.fragment_id) and left.row_ordinal == right.row_ordinal,
            else => false,
        },
        .relational_key => |left| switch (b) {
            .relational_key => |right| std.mem.eql(u8, left, right),
            else => false,
        },
    };
}

test "iceberg position delete rows map to external row refs" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/t"),
        .snapshot_id = try alloc.dupe(u8, "12"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "s3://bucket/t/data/a.parquet"),
        .object_uri = try alloc.dupe(u8, "object://bucket/t/data/a.parquet"),
        .version_id = try alloc.dupe(u8, "iceberg:v1:snapshot=12"),
        .byte_len = 1024,
        .row_count = 5,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{ .ordinal = 0, .row_count = 2 },
            .{ .ordinal = 1, .row_count = 3 },
        }),
    };

    const delete_rows = [_]PositionDeleteRow{
        .{ .data_file_path = "s3://bucket/t/data/a.parquet", .row_position = 0 },
        .{ .data_file_path = "s3://bucket/t/data/a.parquet", .row_position = 3 },
        .{ .data_file_path = "s3://bucket/t/data/a.parquet", .row_position = 3 },
    };
    const refs = try positionDeleteRowsToRowRefsAlloc(alloc, inventory, &delete_rows);
    defer alloc.free(refs);

    try std.testing.expectEqual(@as(usize, 2), refs.len);
    try std.testing.expectEqualStrings("events", refs[0].external.source_id);
    try std.testing.expectEqualStrings("12", refs[0].external.snapshot_id);
    try std.testing.expectEqualStrings("s3://bucket/t/data/a.parquet", refs[0].external.file_id);
    try std.testing.expectEqual(@as(u32, 0), refs[0].external.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 0), refs[0].external.row_ordinal);
    try std.testing.expectEqual(@as(u32, 1), refs[1].external.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 1), refs[1].external.row_ordinal);
}

test "iceberg position delete scan result maps to external row refs" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/t"),
        .snapshot_id = try alloc.dupe(u8, "12"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "s3://bucket/t/data/a.parquet"),
        .object_uri = try alloc.dupe(u8, "object://bucket/t/data/a.parquet"),
        .version_id = try alloc.dupe(u8, "iceberg:v1:snapshot=12"),
        .byte_len = 1024,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{ .ordinal = 0, .row_count = 2 },
            .{ .ordinal = 1, .row_count = 2 },
        }),
    };

    var result = lake_rows.ScanResult{
        .rows = try alloc.alloc(lake_rows.ProjectedRow, 2),
        .total = 2,
    };
    var initialized: usize = 0;
    var partial_cleanup = true;
    errdefer if (partial_cleanup) {
        for (result.rows[0..initialized]) |*row| row.deinit(alloc);
        alloc.free(result.rows);
    };
    result.rows[0] = try testPositionDeleteProjectedRowAlloc(alloc, "s3://bucket/t/data/a.parquet", 1);
    initialized += 1;
    result.rows[1] = try testPositionDeleteProjectedRowAlloc(alloc, "s3://bucket/t/data/a.parquet", 2);
    initialized += 1;
    partial_cleanup = false;
    defer result.deinit(alloc);

    const refs = try positionDeleteScanResultToRowRefsAlloc(alloc, inventory, result, .{});
    defer alloc.free(refs);

    try std.testing.expectEqual(@as(usize, 2), refs.len);
    try std.testing.expectEqual(@as(u32, 0), refs[0].external.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 1), refs[0].external.row_ordinal);
    try std.testing.expectEqual(@as(u32, 1), refs[1].external.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 0), refs[1].external.row_ordinal);
}

test "iceberg position delete scan result rejects malformed rows" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/t"),
        .snapshot_id = try alloc.dupe(u8, "12"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "s3://bucket/t/data/a.parquet"),
        .object_uri = try alloc.dupe(u8, "object://bucket/t/data/a.parquet"),
        .version_id = try alloc.dupe(u8, "iceberg:v1:snapshot=12"),
        .byte_len = 1024,
        .row_count = 1,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{ .ordinal = 0, .row_count = 1 },
        }),
    };

    var result = lake_rows.ScanResult{
        .rows = try alloc.alloc(lake_rows.ProjectedRow, 1),
        .total = 1,
    };
    var partial_cleanup = true;
    errdefer if (partial_cleanup) alloc.free(result.rows);
    result.rows[0] = try testPositionDeleteProjectedRowAlloc(alloc, "s3://bucket/t/data/a.parquet", -1);
    partial_cleanup = false;
    defer result.deinit(alloc);

    try std.testing.expectError(
        error.InvalidIcebergPositionDelete,
        positionDeleteScanResultToRowRefsAlloc(alloc, inventory, result, .{}),
    );
}

test "iceberg position delete rows require row group metadata" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/t"),
        .snapshot_id = try alloc.dupe(u8, "12"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "s3://bucket/t/data/a.parquet"),
        .object_uri = try alloc.dupe(u8, "object://bucket/t/data/a.parquet"),
        .version_id = try alloc.dupe(u8, "iceberg:v1:snapshot=12"),
        .byte_len = 1024,
        .row_count = 5,
        .row_groups = &.{},
    };

    const delete_rows = [_]PositionDeleteRow{
        .{ .data_file_path = "s3://bucket/t/data/a.parquet", .row_position = 0 },
    };
    try std.testing.expectError(
        error.MissingIcebergPositionDeleteRowGroupMetadata,
        positionDeleteRowsToRowRefsAlloc(alloc, inventory, &delete_rows),
    );
}

test "iceberg position delete rows reject out of bounds positions" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/t"),
        .snapshot_id = try alloc.dupe(u8, "12"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "s3://bucket/t/data/a.parquet"),
        .object_uri = try alloc.dupe(u8, "object://bucket/t/data/a.parquet"),
        .version_id = try alloc.dupe(u8, "iceberg:v1:snapshot=12"),
        .byte_len = 1024,
        .row_count = 2,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{ .ordinal = 0, .row_count = 2 },
        }),
    };

    const delete_rows = [_]PositionDeleteRow{
        .{ .data_file_path = "s3://bucket/t/data/a.parquet", .row_position = 2 },
    };
    try std.testing.expectError(
        error.IcebergPositionDeleteRowOutOfBounds,
        positionDeleteRowsToRowRefsAlloc(alloc, inventory, &delete_rows),
    );
}

test "iceberg equality delete scan result maps matching data rows to row refs" {
    const alloc = std.testing.allocator;
    var data = lake_rows.ScanResult{
        .rows = try alloc.alloc(lake_rows.ProjectedRow, 3),
        .total = 3,
    };
    var data_initialized: usize = 0;
    var data_partial_cleanup = true;
    errdefer if (data_partial_cleanup) {
        for (data.rows[0..data_initialized]) |*row| row.deinit(alloc);
        alloc.free(data.rows);
    };
    data.rows[0] = try testEqualityProjectedRowAlloc(alloc, "data:0", "tenant-a", 10, null);
    data_initialized += 1;
    data.rows[1] = try testEqualityProjectedRowAlloc(alloc, "data:1", "tenant-b", 20, null);
    data_initialized += 1;
    data.rows[2] = try testEqualityProjectedRowAlloc(alloc, "data:2", "tenant-a", 30, null);
    data_initialized += 1;
    data_partial_cleanup = false;
    defer data.deinit(alloc);

    var deletes = lake_rows.ScanResult{
        .rows = try alloc.alloc(lake_rows.ProjectedRow, 3),
        .total = 3,
    };
    var delete_initialized: usize = 0;
    var delete_partial_cleanup = true;
    errdefer if (delete_partial_cleanup) {
        for (deletes.rows[0..delete_initialized]) |*row| row.deinit(alloc);
        alloc.free(deletes.rows);
    };
    deletes.rows[0] = try testEqualityProjectedRowAlloc(alloc, "delete:0", "tenant-a", 10, null);
    delete_initialized += 1;
    deletes.rows[1] = try testEqualityProjectedRowAlloc(alloc, "delete:1", "tenant-a", 30, null);
    delete_initialized += 1;
    deletes.rows[2] = try testEqualityProjectedRowAlloc(alloc, "delete:2", "tenant-a", 30, null);
    delete_initialized += 1;
    delete_partial_cleanup = false;
    defer deletes.deinit(alloc);

    const columns = [_][]const u8{ "tenant_id", "amount" };
    const refs = try equalityDeleteScanResultsToRowRefsAlloc(alloc, data, deletes, &columns);
    defer alloc.free(refs);

    try std.testing.expectEqual(@as(usize, 2), refs.len);
    try std.testing.expectEqualStrings("data:0", refs[0].relational_key);
    try std.testing.expectEqualStrings("data:2", refs[1].relational_key);
}

test "iceberg equality delete scan result treats matching nulls as equal" {
    const alloc = std.testing.allocator;
    var data = lake_rows.ScanResult{
        .rows = try alloc.alloc(lake_rows.ProjectedRow, 2),
        .total = 2,
    };
    var data_initialized: usize = 0;
    var data_partial_cleanup = true;
    errdefer if (data_partial_cleanup) {
        for (data.rows[0..data_initialized]) |*row| row.deinit(alloc);
        alloc.free(data.rows);
    };
    data.rows[0] = try testEqualityProjectedRowAlloc(alloc, "data:0", null, 10, true);
    data_initialized += 1;
    data.rows[1] = try testEqualityProjectedRowAlloc(alloc, "data:1", "tenant-b", 10, true);
    data_initialized += 1;
    data_partial_cleanup = false;
    defer data.deinit(alloc);

    var deletes = lake_rows.ScanResult{
        .rows = try alloc.alloc(lake_rows.ProjectedRow, 1),
        .total = 1,
    };
    var delete_partial_cleanup = true;
    errdefer if (delete_partial_cleanup) alloc.free(deletes.rows);
    deletes.rows[0] = try testEqualityProjectedRowAlloc(alloc, "delete:0", null, 10, true);
    delete_partial_cleanup = false;
    defer deletes.deinit(alloc);

    const columns = [_][]const u8{ "tenant_id", "amount", "active" };
    const refs = try equalityDeleteScanResultsToRowRefsAlloc(alloc, data, deletes, &columns);
    defer alloc.free(refs);

    try std.testing.expectEqual(@as(usize, 1), refs.len);
    try std.testing.expectEqualStrings("data:0", refs[0].relational_key);
}

test "iceberg equality delete hash keys preserve column boundaries" {
    const alloc = std.testing.allocator;
    const makeRow = struct {
        fn make(a: Allocator, key: []const u8, first: []const u8, second: []const u8) !lake_rows.ProjectedRow {
            const cells = try a.alloc(lake_rows.ProjectedCell, 2);
            var initialized: usize = 0;
            errdefer {
                for (cells[0..initialized]) |*cell| cell.deinit(a);
                a.free(cells);
            }
            cells[0] = .{ .name = try a.dupe(u8, "a"), .value = .{ .bytes = try a.dupe(u8, first) } };
            initialized += 1;
            cells[1] = .{ .name = try a.dupe(u8, "b"), .value = .{ .bytes = try a.dupe(u8, second) } };
            initialized += 1;
            return .{ .row_ref = .{ .relational_key = key }, .cells = cells };
        }
    }.make;

    var data = lake_rows.ScanResult{ .rows = try alloc.alloc(lake_rows.ProjectedRow, 2), .total = 2 };
    data.rows[0] = try makeRow(alloc, "match", "a", "bc");
    data.rows[1] = try makeRow(alloc, "not-match", "ab", "c");
    defer data.deinit(alloc);
    var deletes = lake_rows.ScanResult{ .rows = try alloc.alloc(lake_rows.ProjectedRow, 1), .total = 1 };
    deletes.rows[0] = try makeRow(alloc, "delete", "a", "bc");
    defer deletes.deinit(alloc);

    const refs = try equalityDeleteScanResultsToRowRefsAlloc(alloc, data, deletes, &.{ "a", "b" });
    defer alloc.free(refs);
    try std.testing.expectEqual(@as(usize, 1), refs.len);
    try std.testing.expectEqualStrings("match", refs[0].relational_key);
}

fn testPositionDeleteProjectedRowAlloc(
    alloc: Allocator,
    file_path: []const u8,
    pos: i64,
) !lake_rows.ProjectedRow {
    const cells = try alloc.alloc(lake_rows.ProjectedCell, 2);
    var initialized: usize = 0;
    errdefer {
        for (cells[0..initialized]) |*cell| cell.deinit(alloc);
        alloc.free(cells);
    }

    cells[0] = .{
        .name = try alloc.dupe(u8, "file_path"),
        .value = .{ .bytes = try alloc.dupe(u8, file_path) },
    };
    initialized += 1;
    cells[1] = .{
        .name = try alloc.dupe(u8, "pos"),
        .value = .{ .i64 = pos },
    };
    initialized += 1;

    return .{
        .row_ref = .{ .relational_key = "delete-row" },
        .cells = cells,
    };
}

fn testEqualityProjectedRowAlloc(
    alloc: Allocator,
    row_key: []const u8,
    tenant_id: ?[]const u8,
    amount: i64,
    active: ?bool,
) !lake_rows.ProjectedRow {
    const cells = try alloc.alloc(lake_rows.ProjectedCell, 3);
    var initialized: usize = 0;
    errdefer {
        for (cells[0..initialized]) |*cell| cell.deinit(alloc);
        alloc.free(cells);
    }

    cells[0] = .{
        .name = try alloc.dupe(u8, "tenant_id"),
        .value = if (tenant_id) |value| .{ .bytes = try alloc.dupe(u8, value) } else null,
    };
    initialized += 1;
    cells[1] = .{
        .name = try alloc.dupe(u8, "amount"),
        .value = .{ .i64 = amount },
    };
    initialized += 1;
    cells[2] = .{
        .name = try alloc.dupe(u8, "active"),
        .value = if (active) |value| .{ .bool = value } else null,
    };
    initialized += 1;

    return .{
        .row_ref = .{ .relational_key = row_key },
        .cells = cells,
    };
}
