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

pub const PositionDeleteRow = struct {
    data_file_path: []const u8,
    row_position: u64,

    pub fn validate(self: PositionDeleteRow) !void {
        if (self.data_file_path.len == 0) return error.InvalidIcebergPositionDelete;
    }
};

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
