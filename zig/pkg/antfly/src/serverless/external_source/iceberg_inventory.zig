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

//! Iceberg snapshot inventory planning.
//!
//! Iceberg metadata and Avro manifests identify the Parquet data files that
//! belong to a pinned snapshot. This module converts those decoded data-file
//! entries into Antfly's external-source inventory format so the existing
//! Parquet footer discovery and object-range scanner can take over.

const std = @import("std");
const Allocator = std.mem.Allocator;
const external_source = @import("types.zig");
const iceberg_avro = @import("iceberg_avro.zig");

pub const InventoryRequest = struct {
    source_id: []const u8,
    source_uri: []const u8,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    data_files: []const iceberg_avro.DataFileEntry,

    pub fn validate(self: InventoryRequest) !void {
        if (self.source_id.len == 0) return error.InvalidIcebergInventory;
        if (self.source_uri.len == 0) return error.InvalidIcebergInventory;
        if (self.snapshot_id.len == 0) return error.InvalidIcebergInventory;
        if (self.schema_fingerprint.len == 0) return error.InvalidIcebergInventory;
    }
};

pub fn planInventoryFromDataFilesAlloc(
    alloc: Allocator,
    request: InventoryRequest,
) !external_source.Inventory {
    try request.validate();

    var active_count: usize = 0;
    for (request.data_files) |file| {
        try file.validate();
        if (file.content != .data) return error.UnsupportedIcebergDeletes;
        if (file.status != .deleted) active_count += 1;
    }
    if (active_count == 0) return error.EmptyIcebergInventory;

    const sorted_indexes = try alloc.alloc(usize, active_count);
    defer alloc.free(sorted_indexes);
    var next_index: usize = 0;
    for (request.data_files, 0..) |file, idx| {
        if (file.status == .deleted) continue;
        sorted_indexes[next_index] = idx;
        next_index += 1;
    }
    std.mem.sort(usize, sorted_indexes, request.data_files, struct {
        fn lessThan(ctx: []const iceberg_avro.DataFileEntry, left_idx: usize, right_idx: usize) bool {
            const left = ctx[left_idx];
            const right = ctx[right_idx];
            const path_order = std.mem.order(u8, left.file_path, right.file_path);
            if (path_order != .eq) return path_order == .lt;
            if (left.snapshot_id != right.snapshot_id) return left.snapshot_id < right.snapshot_id;
            if (left.data_sequence_number != right.data_sequence_number) return left.data_sequence_number < right.data_sequence_number;
            return left.file_sequence_number < right.file_sequence_number;
        }
    }.lessThan);

    const files = try alloc.alloc(external_source.FileEntry, active_count);
    errdefer alloc.free(files);
    var initialized: usize = 0;
    errdefer {
        for (files[0..initialized]) |*file| file.deinit(alloc);
    }

    for (sorted_indexes, 0..) |source_idx, out_idx| {
        const data_file = request.data_files[source_idx];
        const file_id = try alloc.dupe(u8, data_file.file_path);
        errdefer alloc.free(file_id);
        const object_uri = try alloc.dupe(u8, data_file.file_path);
        errdefer alloc.free(object_uri);
        const version_id = try syntheticVersionIdAlloc(alloc, request.snapshot_id, data_file);
        errdefer alloc.free(version_id);
        files[out_idx] = .{
            .file_id = file_id,
            .object_uri = object_uri,
            .version_id = version_id,
            .byte_len = data_file.file_size_in_bytes,
            .row_count = data_file.record_count,
            .row_groups = &.{},
        };
        initialized += 1;
    }

    const source_id = try alloc.dupe(u8, request.source_id);
    errdefer alloc.free(source_id);
    const source_uri = try alloc.dupe(u8, request.source_uri);
    errdefer alloc.free(source_uri);
    const snapshot_id = try alloc.dupe(u8, request.snapshot_id);
    errdefer alloc.free(snapshot_id);
    const schema_fingerprint = try alloc.dupe(u8, request.schema_fingerprint);
    errdefer alloc.free(schema_fingerprint);

    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = source_id,
        .source_uri = source_uri,
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .files = files,
    };
    errdefer inventory.deinit(alloc);
    try inventory.validate();
    return inventory;
}

fn syntheticVersionIdAlloc(
    alloc: Allocator,
    pinned_snapshot_id: []const u8,
    file: iceberg_avro.DataFileEntry,
) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "iceberg:v1:snapshot={s}:entry_snapshot={d}:status={s}:data_seq={d}:file_seq={d}",
        .{
            pinned_snapshot_id,
            file.snapshot_id,
            @tagName(file.status),
            file.data_sequence_number,
            file.file_sequence_number,
        },
    );
}

test "iceberg inventory planner creates active parquet inventory" {
    const alloc = std.testing.allocator;
    var data_files = [_]iceberg_avro.DataFileEntry{
        .{
            .status = .added,
            .snapshot_id = 12,
            .data_sequence_number = 4,
            .file_sequence_number = 5,
            .content = .data,
            .file_path = try alloc.dupe(u8, "s3://bucket/t/data/b.parquet"),
            .file_format = try alloc.dupe(u8, "PARQUET"),
            .record_count = 2,
            .file_size_in_bytes = 2048,
        },
        .{
            .status = .deleted,
            .snapshot_id = 11,
            .data_sequence_number = 3,
            .file_sequence_number = 4,
            .content = .data,
            .file_path = try alloc.dupe(u8, "s3://bucket/t/data/old.parquet"),
            .file_format = try alloc.dupe(u8, "PARQUET"),
            .record_count = 1,
            .file_size_in_bytes = 1024,
        },
        .{
            .status = .existing,
            .snapshot_id = 10,
            .data_sequence_number = 2,
            .file_sequence_number = 3,
            .content = .data,
            .file_path = try alloc.dupe(u8, "s3://bucket/t/data/a.parquet"),
            .file_format = try alloc.dupe(u8, "PARQUET"),
            .record_count = 3,
            .file_size_in_bytes = 4096,
        },
    };
    defer for (&data_files) |*file| file.deinit(alloc);

    var inventory = try planInventoryFromDataFilesAlloc(alloc, .{
        .source_id = "events",
        .source_uri = "s3://bucket/t",
        .snapshot_id = "12",
        .schema_fingerprint = "iceberg-schema:7",
        .data_files = &data_files,
    });
    defer inventory.deinit(alloc);

    try std.testing.expectEqual(external_source.Format.iceberg, inventory.format);
    try std.testing.expectEqualStrings("12", inventory.snapshot_id);
    try std.testing.expectEqual(@as(usize, 2), inventory.files.len);
    try std.testing.expectEqualStrings("s3://bucket/t/data/a.parquet", inventory.files[0].file_id);
    try std.testing.expectEqualStrings("s3://bucket/t/data/a.parquet", inventory.files[0].object_uri);
    try std.testing.expectEqual(@as(u64, 3), inventory.files[0].row_count);
    try std.testing.expectEqual(@as(u64, 4096), inventory.files[0].byte_len);
    try std.testing.expect(std.mem.startsWith(u8, inventory.files[0].version_id, "iceberg:v1:snapshot=12:"));
    try std.testing.expectEqualStrings("s3://bucket/t/data/b.parquet", inventory.files[1].file_id);
}

test "iceberg inventory planner rejects delete-file manifests until deletes are applied" {
    const alloc = std.testing.allocator;
    var data_files = [_]iceberg_avro.DataFileEntry{.{
        .status = .added,
        .snapshot_id = 12,
        .content = .position_deletes,
        .file_path = try alloc.dupe(u8, "s3://bucket/t/delete/d0.parquet"),
        .file_format = try alloc.dupe(u8, "PARQUET"),
        .record_count = 1,
        .file_size_in_bytes = 256,
    }};
    defer data_files[0].deinit(alloc);

    try std.testing.expectError(error.UnsupportedIcebergDeletes, planInventoryFromDataFilesAlloc(alloc, .{
        .source_id = "events",
        .source_uri = "s3://bucket/t",
        .snapshot_id = "12",
        .schema_fingerprint = "iceberg-schema:7",
        .data_files = &data_files,
    }));
}

test "iceberg inventory planner rejects empty active snapshots" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.EmptyIcebergInventory, planInventoryFromDataFilesAlloc(alloc, .{
        .source_id = "events",
        .source_uri = "s3://bucket/t",
        .snapshot_id = "12",
        .schema_fingerprint = "iceberg-schema:7",
        .data_files = &.{},
    }));
}
