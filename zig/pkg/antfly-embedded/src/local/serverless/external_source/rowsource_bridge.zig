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

//! Bridge from serverless external-source inventory artifacts to shared
//! RowSource bindings and row references.

const std = @import("std");
const external_source = @import("types.zig");
const rowsource_external = @import("../../storage/rowsource/external.zig");
const rowsource = @import("../../storage/rowsource/types.zig");

pub fn bindingFromInventory(inventory: external_source.Inventory) !rowsource_external.Binding {
    try inventory.validate();
    return bindingFromValidatedInventory(inventory);
}

/// Build a binding after the caller has validated the inventory once. Batch
/// readers use this to avoid repeating the inventory-wide validation for every
/// row reference they materialize.
pub fn bindingFromValidatedInventory(inventory: external_source.Inventory) rowsource_external.Binding {
    return .{
        .format = switch (inventory.format) {
            .parquet => .parquet,
            .iceberg => .iceberg,
            .lance => .lance,
        },
        .source_id = inventory.source_id,
        .source_uri = inventory.source_uri,
        .snapshot_id = inventory.snapshot_id,
        .schema_fingerprint = inventory.schema_fingerprint,
    };
}

pub fn rowRefForInventoryRow(
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    row_ordinal: u64,
) !rowsource.RowRef {
    const binding = try bindingFromInventory(inventory);
    const file = inventory.fileById(file_id) orelse return error.ExternalSourceFileNotFound;
    if (row_group_ordinal >= file.row_groups.len) return error.ExternalSourceRowOutOfBounds;
    if (row_ordinal >= file.row_groups[row_group_ordinal].row_count) return error.ExternalSourceRowOutOfBounds;
    return rowsource_external.makeRowRef(binding, file_id, row_group_ordinal, row_ordinal);
}

pub fn validateBatchAgainstInventory(
    inventory: external_source.Inventory,
    batch: rowsource.ColumnBatch,
) !void {
    return try validateBatchAgainstInventoryAlloc(std.heap.page_allocator, inventory, batch);
}

pub fn validateBatchAgainstInventoryAlloc(
    alloc: std.mem.Allocator,
    inventory: external_source.Inventory,
    batch: rowsource.ColumnBatch,
) !void {
    try inventory.validateAlloc(alloc);
    const binding = bindingFromValidatedInventory(inventory);
    try rowsource_external.validateExternalBatch(binding, batch);

    var files_by_id = std.StringHashMapUnmanaged(*const external_source.FileEntry).empty;
    defer files_by_id.deinit(alloc);
    const file_capacity = std.math.cast(u32, inventory.files.len) orelse return error.InvalidExternalSourceInventory;
    try files_by_id.ensureTotalCapacity(alloc, file_capacity);
    for (inventory.files) |*file| files_by_id.putAssumeCapacity(file.file_id, file);

    for (batch.row_refs) |row_ref| {
        const external = switch (row_ref) {
            .external => |external| external,
            else => return error.InvalidExternalRowSource,
        };
        const file = files_by_id.get(external.file_id) orelse return error.ExternalSourceFileNotFound;
        if (external.row_group_ordinal >= file.row_groups.len) return error.ExternalSourceRowOutOfBounds;
        if (external.row_ordinal >= file.row_groups[external.row_group_ordinal].row_count) {
            return error.ExternalSourceRowOutOfBounds;
        }
    }
}

test "external source inventory bridges to RowSource binding and row refs" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events"),
        .snapshot_id = try alloc.dupe(u8, "iceberg-123"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "file-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events/file-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-file-a"),
        .byte_len = 1024,
        .row_count = 2,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{ .ordinal = 0, .row_count = 2 },
        }),
    };

    const binding = try bindingFromInventory(inventory);
    try std.testing.expectEqual(rowsource.SourceKind.external_iceberg, binding.format.sourceKind());

    const row_refs = [_]rowsource.RowRef{
        try rowRefForInventoryRow(inventory, "file-a.parquet", 0, 0),
        try rowRefForInventoryRow(inventory, "file-a.parquet", 0, 1),
    };
    const values = [_]i64{ 10, 20 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &values } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    };
    try validateBatchAgainstInventory(inventory, batch);
    try std.testing.expectError(
        error.ExternalSourceRowOutOfBounds,
        rowRefForInventoryRow(inventory, "file-a.parquet", 0, 2),
    );
}
