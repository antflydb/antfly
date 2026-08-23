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

//! Projected lake scan planning over catalog bindings and pinned inventories.

const std = @import("std");
const Allocator = std.mem.Allocator;
const external_binding = @import("../external_source/catalog_binding.zig");
const external_source = @import("../external_source/types.zig");
const range_io = @import("lake_range_io.zig");

pub const Request = struct {
    binding: external_binding.Binding,
    inventory: external_source.Inventory,
    projected_columns: []const []const u8 = &.{},
    include_footer_reads: bool = false,
    footer_probe_bytes: u64 = 64 * 1024,
    coalesce_options: range_io.CoalesceOptions = .{},
};

pub const Plan = struct {
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    logical_reads: []range_io.RangeRead,
    physical_reads: []range_io.RangeRead,

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        alloc.free(self.logical_reads);
        alloc.free(self.physical_reads);
        self.* = undefined;
    }
};

pub fn planProjectedScanAlloc(alloc: Allocator, request: Request) !Plan {
    try validateBindingInventory(request.binding, request.inventory);
    if (request.include_footer_reads and request.footer_probe_bytes == 0) return error.InvalidLakeScanPlan;
    for (request.projected_columns) |column| {
        if (column.len == 0) return error.InvalidLakeScanPlan;
    }

    var logical = std.ArrayListUnmanaged(range_io.RangeRead).empty;
    errdefer logical.deinit(alloc);

    for (request.inventory.files) |file| {
        const object = try range_io.objectRefForExternalFileUri(file);
        if (request.include_footer_reads) {
            try logical.append(alloc, try range_io.planParquetFooterRead(object, request.footer_probe_bytes));
        }
        for (file.row_groups) |row_group| {
            for (row_group.column_chunks) |chunk| {
                if (!projectIncludesColumn(request.projected_columns, chunk.column_id)) continue;
                try logical.append(alloc, try range_io.planColumnChunkRead(object, chunk));
            }
        }
    }

    const logical_reads = try logical.toOwnedSlice(alloc);
    errdefer alloc.free(logical_reads);
    const physical_reads = try range_io.coalescePhysicalReadsAlloc(alloc, logical_reads, request.coalesce_options);
    errdefer alloc.free(physical_reads);

    return .{
        .snapshot_id = request.inventory.snapshot_id,
        .schema_fingerprint = request.inventory.schema_fingerprint,
        .logical_reads = logical_reads,
        .physical_reads = physical_reads,
    };
}

pub fn validateBindingInventory(
    binding: external_binding.Binding,
    inventory: external_source.Inventory,
) !void {
    try binding.validateReadOnlyMvp();
    try inventory.validate();
    if (binding.format != inventory.format) return error.ExternalLakeSnapshotMismatch;
    if (!std.mem.eql(u8, binding.table_id, inventory.source_id)) return error.ExternalLakeSnapshotMismatch;
    if (!std.mem.eql(u8, binding.source_uri, inventory.source_uri)) return error.ExternalLakeSnapshotMismatch;
    if (!std.mem.eql(u8, binding.schema_fingerprint, inventory.schema_fingerprint)) {
        return error.ExternalLakeSnapshotMismatch;
    }
    if (binding.snapshot_mode.pinnedSnapshotId()) |pinned| {
        if (!std.mem.eql(u8, pinned, inventory.snapshot_id)) return error.ExternalLakeSnapshotMismatch;
    }
}

fn projectIncludesColumn(projected_columns: []const []const u8, column_id: []const u8) bool {
    if (projected_columns.len == 0) return true;
    for (projected_columns) |projected| {
        if (std.mem.eql(u8, projected, column_id)) return true;
    }
    return false;
}

test "lake scan planner projects column chunk reads and coalesces physical ranges" {
    const alloc = std.testing.allocator;
    var inventory = try testInventory(alloc);
    defer inventory.deinit(alloc);

    const binding = external_binding.Binding{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "s3://bucket/events",
        .snapshot_mode = .{ .object_version_digest = "sha256:objects" },
        .schema_fingerprint = "schema-v1",
    };

    var plan = try planProjectedScanAlloc(alloc, .{
        .binding = binding,
        .inventory = inventory,
        .projected_columns = &[_][]const u8{"amount"},
        .coalesce_options = .{ .max_gap_bytes = 0 },
    });
    defer plan.deinit(alloc);

    try std.testing.expectEqualStrings("sha256:objects", plan.snapshot_id);
    try std.testing.expectEqual(@as(usize, 2), plan.logical_reads.len);
    try std.testing.expectEqual(@as(usize, 1), plan.physical_reads.len);
    try std.testing.expectEqualStrings("bucket", plan.logical_reads[0].object.bucket);
    try std.testing.expectEqualStrings("events/part-a.parquet", plan.logical_reads[0].object.key);
    try std.testing.expectEqual(range_io.RangePurpose.parquet_column_chunk, plan.logical_reads[0].purpose);
    try std.testing.expectEqual(@as(u64, 100), plan.physical_reads[0].range.offset);
    try std.testing.expectEqual(@as(u64, 80), plan.physical_reads[0].range.len);
}

test "lake scan planner can include footer probes" {
    const alloc = std.testing.allocator;
    var inventory = try testInventory(alloc);
    defer inventory.deinit(alloc);

    const binding = external_binding.Binding{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "s3://bucket/events",
        .snapshot_mode = .current,
        .schema_fingerprint = "schema-v1",
    };

    var plan = try planProjectedScanAlloc(alloc, .{
        .binding = binding,
        .inventory = inventory,
        .projected_columns = &[_][]const u8{"user_id"},
        .include_footer_reads = true,
        .footer_probe_bytes = 256,
    });
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), plan.logical_reads.len);
    try std.testing.expectEqual(range_io.RangePurpose.parquet_footer, plan.logical_reads[0].purpose);
    try std.testing.expectEqual(@as(u64, 768), plan.logical_reads[0].range.offset);
}

test "lake scan planner rejects stale inventory" {
    const alloc = std.testing.allocator;
    var inventory = try testInventory(alloc);
    defer inventory.deinit(alloc);

    const binding = external_binding.Binding{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "s3://bucket/events",
        .snapshot_mode = .{ .object_version_digest = "sha256:stale" },
        .schema_fingerprint = "schema-v1",
    };

    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, planProjectedScanAlloc(alloc, .{
        .binding = binding,
        .inventory = inventory,
        .projected_columns = &[_][]const u8{"amount"},
    }));
}

fn testInventory(alloc: Allocator) !external_source.Inventory {
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1024,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{
                .ordinal = 0,
                .row_count = 2,
                .file_offset = 100,
                .total_byte_len = 80,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                    .{
                        .column_id = try alloc.dupe(u8, "amount"),
                        .file_offset = 100,
                        .compressed_len = 40,
                        .compression_codec = try alloc.dupe(u8, "zstd"),
                    },
                    .{
                        .column_id = try alloc.dupe(u8, "user_id"),
                        .file_offset = 220,
                        .compressed_len = 40,
                        .compression_codec = try alloc.dupe(u8, "zstd"),
                    },
                }),
            },
            .{
                .ordinal = 1,
                .row_count = 2,
                .file_offset = 140,
                .total_byte_len = 80,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                    .{
                        .column_id = try alloc.dupe(u8, "amount"),
                        .file_offset = 140,
                        .compressed_len = 40,
                        .compression_codec = try alloc.dupe(u8, "zstd"),
                    },
                    .{
                        .column_id = try alloc.dupe(u8, "user_id"),
                        .file_offset = 260,
                        .compressed_len = 40,
                        .compression_codec = try alloc.dupe(u8, "zstd"),
                    },
                }),
            },
        }),
    };
    errdefer inventory.deinit(alloc);
    try inventory.validate();
    return inventory;
}
