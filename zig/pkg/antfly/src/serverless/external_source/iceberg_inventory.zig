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
const iceberg_metadata = @import("iceberg_metadata.zig");

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

pub const DecodedManifest = struct {
    manifest_path: []const u8,
    manifest: iceberg_avro.DataManifest,

    pub fn validate(self: DecodedManifest) !void {
        if (self.manifest_path.len == 0) return error.InvalidIcebergInventory;
        try self.manifest.validate();
    }
};

pub const SnapshotInventoryRequest = struct {
    source_id: []const u8,
    metadata_plan: iceberg_metadata.Plan,
    manifest_list: iceberg_avro.ManifestList,
    data_manifests: []const DecodedManifest,

    pub fn validate(self: SnapshotInventoryRequest) !void {
        if (self.source_id.len == 0) return error.InvalidIcebergInventory;
        try self.metadata_plan.validate();
        try self.manifest_list.validate();
        for (self.data_manifests) |manifest| try manifest.validate();
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
        try validateResolvedManifestEntry(file);
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
            if (left.snapshot_id.? != right.snapshot_id.?) return left.snapshot_id.? < right.snapshot_id.?;
            if (left.data_sequence_number.? != right.data_sequence_number.?) return left.data_sequence_number.? < right.data_sequence_number.?;
            return left.file_sequence_number.? < right.file_sequence_number.?;
        }
    }.lessThan);

    var inventory_owns_allocations = false;
    const files = try alloc.alloc(external_source.FileEntry, active_count);
    errdefer if (!inventory_owns_allocations) alloc.free(files);
    var initialized: usize = 0;
    errdefer if (!inventory_owns_allocations) {
        for (files[0..initialized]) |*file| file.deinit(alloc);
    };

    for (sorted_indexes, 0..) |source_idx, out_idx| {
        const data_file = request.data_files[source_idx];
        const file_id = try alloc.dupe(u8, data_file.file_path);
        errdefer alloc.free(file_id);
        const object_uri = try alloc.dupe(u8, data_file.file_path);
        errdefer alloc.free(object_uri);
        const version_id = try syntheticVersionIdAlloc(alloc, request.snapshot_id, data_file);
        errdefer alloc.free(version_id);
        const partition_values = try clonePartitionValuesAlloc(alloc, data_file.partition_values);
        errdefer {
            for (partition_values) |*partition| partition.deinit(alloc);
            if (partition_values.len > 0) alloc.free(partition_values);
        }
        files[out_idx] = .{
            .file_id = file_id,
            .object_uri = object_uri,
            .version_id = version_id,
            .byte_len = data_file.file_size_in_bytes,
            .row_count = data_file.record_count,
            .data_sequence_number = data_file.data_sequence_number,
            .partition_spec_id = data_file.partition_spec_id,
            .partition_field_count = if (data_file.partition_field_count != 0)
                data_file.partition_field_count
            else
                @intCast(data_file.partition_values.len),
            .partition_values = partition_values,
            .row_groups = &.{},
        };
        initialized += 1;
    }

    const source_id = try alloc.dupe(u8, request.source_id);
    errdefer if (!inventory_owns_allocations) alloc.free(source_id);
    const source_uri = try alloc.dupe(u8, request.source_uri);
    errdefer if (!inventory_owns_allocations) alloc.free(source_uri);
    const snapshot_id = try alloc.dupe(u8, request.snapshot_id);
    errdefer if (!inventory_owns_allocations) alloc.free(snapshot_id);
    const schema_fingerprint = try alloc.dupe(u8, request.schema_fingerprint);
    errdefer if (!inventory_owns_allocations) alloc.free(schema_fingerprint);

    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = source_id,
        .source_uri = source_uri,
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .files = files,
    };
    inventory_owns_allocations = true;
    errdefer inventory.deinit(alloc);
    try inventory.validate();
    return inventory;
}

pub fn planInventoryFromSnapshotManifestsAlloc(
    alloc: Allocator,
    request: SnapshotInventoryRequest,
) !external_source.Inventory {
    try request.validate();

    var total_entries: usize = 0;
    for (request.manifest_list.entries) |manifest_entry| {
        if (manifest_entry.content != .data) {
            if (manifest_entry.content == .deletes and deleteManifestEntryHasNoActiveDeletes(manifest_entry)) continue;
            return error.UnsupportedIcebergDeletes;
        }
        const decoded = try decodedManifestForPath(request.data_manifests, manifest_entry.manifest_path);
        try validateManifestSummary(manifest_entry, decoded.manifest);
        total_entries += decoded.manifest.entries.len;
    }
    if (total_entries == 0) return error.EmptyIcebergInventory;

    const data_files = try alloc.alloc(iceberg_avro.DataFileEntry, total_entries);
    defer alloc.free(data_files);
    var next_file: usize = 0;
    for (request.manifest_list.entries) |manifest_entry| {
        if (manifest_entry.content != .data) continue;
        const decoded = try decodedManifestForPath(request.data_manifests, manifest_entry.manifest_path);
        for (decoded.manifest.entries) |entry| {
            data_files[next_file] = try resolveManifestEntry(entry, manifest_entry, request.metadata_plan.format_version);
            data_files[next_file].partition_spec_id = manifest_entry.partition_spec_id;
            next_file += 1;
        }
    }

    return try planInventoryFromDataFilesAlloc(alloc, .{
        .source_id = request.source_id,
        .source_uri = request.metadata_plan.location,
        .snapshot_id = request.metadata_plan.current_snapshot_id,
        .schema_fingerprint = request.metadata_plan.schema_fingerprint,
        .data_files = data_files,
    });
}

/// Resolve Iceberg manifest-entry inheritance while the owning manifest-list
/// row and table format version are still available. Keeping nullable values
/// through Avro decoding is essential: zero is a real sequence number for v1,
/// not a safe stand-in for an inherited v2 value.
pub fn resolveManifestEntry(
    entry: iceberg_avro.DataFileEntry,
    manifest_entry: iceberg_avro.ManifestListEntry,
    format_version: u8,
) !iceberg_avro.DataFileEntry {
    if (format_version != 1 and format_version != 2) return error.UnsupportedIcebergMetadataVersion;
    var resolved = entry;
    if (format_version == 1) {
        // snapshot_id is required in the v1 manifest-entry schema. Missing it
        // is corruption, not an inheritance opportunity.
        resolved.snapshot_id = entry.snapshot_id orelse return error.InvalidIcebergDataManifest;
        resolved.data_sequence_number = 0;
        resolved.file_sequence_number = 0;
    } else {
        // Unlike sequence numbers, snapshot_id is inherited whenever null.
        resolved.snapshot_id = entry.snapshot_id orelse manifest_entry.added_snapshot_id orelse
            return error.InvalidIcebergDataManifest;
        const manifest_sequence = manifest_entry.sequence_number orelse
            return error.InvalidIcebergDataManifest;
        switch (entry.status) {
            .added => {
                resolved.data_sequence_number = entry.data_sequence_number orelse manifest_sequence;
                resolved.file_sequence_number = entry.file_sequence_number orelse manifest_sequence;
            },
            .existing, .deleted => {
                // A v2 table may still reference v1 manifests after upgrade.
                // Their sequence columns are absent and the v2 manifest-list
                // sequence is the initial sequence number (zero).
                if (manifest_sequence == 0) {
                    resolved.data_sequence_number = entry.data_sequence_number orelse 0;
                    resolved.file_sequence_number = entry.file_sequence_number orelse 0;
                } else if (entry.data_sequence_number == null or entry.file_sequence_number == null) {
                    return error.InvalidIcebergDataManifest;
                }
            },
        }
    }
    try validateResolvedManifestEntry(resolved);
    return resolved;
}

test "iceberg v2 added entries inherit nullable manifest sequence fields" {
    const manifest = iceberg_avro.ManifestListEntry{
        .manifest_path = @constCast("metadata/m.avro"),
        .manifest_length = 1,
        .sequence_number = 42,
        .added_snapshot_id = 12,
    };
    const entry = iceberg_avro.DataFileEntry{
        .status = .added,
        .file_path = @constCast("data/a.parquet"),
        .file_format = @constCast("PARQUET"),
        .record_count = 1,
        .file_size_in_bytes = 1,
    };

    const resolved = try resolveManifestEntry(entry, manifest, 2);
    try std.testing.expectEqual(@as(?i64, 12), resolved.snapshot_id);
    try std.testing.expectEqual(@as(?i64, 42), resolved.data_sequence_number);
    try std.testing.expectEqual(@as(?i64, 42), resolved.file_sequence_number);
}

test "iceberg v2 existing entries fail closed instead of inheriting sequence fields" {
    const manifest = iceberg_avro.ManifestListEntry{
        .manifest_path = @constCast("metadata/m.avro"),
        .manifest_length = 1,
        .sequence_number = 42,
        .added_snapshot_id = 12,
    };
    const entry = iceberg_avro.DataFileEntry{
        .status = .existing,
        .file_path = @constCast("data/a.parquet"),
        .file_format = @constCast("PARQUET"),
        .record_count = 1,
        .file_size_in_bytes = 1,
    };

    try std.testing.expectError(error.InvalidIcebergDataManifest, resolveManifestEntry(entry, manifest, 2));
}

test "iceberg v2 existing entries inherit snapshot id and upgraded v1 sequence zero" {
    const manifest = iceberg_avro.ManifestListEntry{
        .manifest_path = @constCast("metadata/m.avro"),
        .manifest_length = 1,
        .sequence_number = 0,
        .added_snapshot_id = 12,
    };
    const entry = iceberg_avro.DataFileEntry{
        .status = .existing,
        .file_path = @constCast("data/a.parquet"),
        .file_format = @constCast("PARQUET"),
        .record_count = 1,
        .file_size_in_bytes = 1,
    };

    const resolved = try resolveManifestEntry(entry, manifest, 2);
    try std.testing.expectEqual(@as(?i64, 12), resolved.snapshot_id);
    try std.testing.expectEqual(@as(?i64, 0), resolved.data_sequence_number);
    try std.testing.expectEqual(@as(?i64, 0), resolved.file_sequence_number);
}

fn validateResolvedManifestEntry(entry: iceberg_avro.DataFileEntry) !void {
    const snapshot_id = entry.snapshot_id orelse return error.InvalidIcebergDataManifest;
    const data_sequence = entry.data_sequence_number orelse return error.InvalidIcebergDataManifest;
    const file_sequence = entry.file_sequence_number orelse return error.InvalidIcebergDataManifest;
    if (snapshot_id < 0 or data_sequence < 0 or file_sequence < 0) return error.InvalidIcebergDataManifest;
}

fn clonePartitionValuesAlloc(
    alloc: Allocator,
    source: []const iceberg_avro.PartitionValue,
) ![]external_source.PartitionValue {
    if (source.len == 0) return &.{};
    const cloned = try alloc.alloc(external_source.PartitionValue, source.len);
    errdefer alloc.free(cloned);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*partition| partition.deinit(alloc);
    }
    for (source, 0..) |partition, idx| {
        cloned[idx] = .{
            .column_id = try alloc.dupe(u8, partition.column_id),
            .string_value = try alloc.dupe(u8, partition.string_value),
        };
        initialized += 1;
    }
    return cloned;
}

fn decodedManifestForPath(
    decoded_manifests: []const DecodedManifest,
    manifest_path: []const u8,
) !DecodedManifest {
    var found: ?DecodedManifest = null;
    for (decoded_manifests) |decoded| {
        if (!std.mem.eql(u8, decoded.manifest_path, manifest_path)) continue;
        if (found != null) return error.DuplicateIcebergManifest;
        found = decoded;
    }
    return found orelse error.MissingIcebergManifest;
}

fn validateManifestSummary(
    manifest_entry: iceberg_avro.ManifestListEntry,
    manifest: iceberg_avro.DataManifest,
) !void {
    if (!hasManifestSummary(manifest_entry)) return;

    var added_files: u32 = 0;
    var existing_files: u32 = 0;
    var deleted_files: u32 = 0;
    var added_rows: u64 = 0;
    var existing_rows: u64 = 0;
    var deleted_rows: u64 = 0;
    for (manifest.entries) |entry| {
        if (entry.content != .data) return error.UnsupportedIcebergDeletes;
        switch (entry.status) {
            .added => {
                added_files += 1;
                added_rows += entry.record_count;
            },
            .existing => {
                existing_files += 1;
                existing_rows += entry.record_count;
            },
            .deleted => {
                deleted_files += 1;
                deleted_rows += entry.record_count;
            },
        }
    }

    if (added_files != manifest_entry.added_files_count) return error.IcebergManifestSummaryMismatch;
    if (existing_files != manifest_entry.existing_files_count) return error.IcebergManifestSummaryMismatch;
    if (deleted_files != manifest_entry.deleted_files_count) return error.IcebergManifestSummaryMismatch;
    if (added_rows != manifest_entry.added_rows_count) return error.IcebergManifestSummaryMismatch;
    if (existing_rows != manifest_entry.existing_rows_count) return error.IcebergManifestSummaryMismatch;
    if (deleted_rows != manifest_entry.deleted_rows_count) return error.IcebergManifestSummaryMismatch;
}

fn hasManifestSummary(manifest_entry: iceberg_avro.ManifestListEntry) bool {
    return manifest_entry.added_files_count != 0 or
        manifest_entry.existing_files_count != 0 or
        manifest_entry.deleted_files_count != 0 or
        manifest_entry.added_rows_count != 0 or
        manifest_entry.existing_rows_count != 0 or
        manifest_entry.deleted_rows_count != 0;
}

fn deleteManifestEntryHasNoActiveDeletes(manifest_entry: iceberg_avro.ManifestListEntry) bool {
    return manifest_entry.added_files_count == 0 and
        manifest_entry.existing_files_count == 0 and
        manifest_entry.added_rows_count == 0 and
        manifest_entry.existing_rows_count == 0;
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
            file.snapshot_id.?,
            @tagName(file.status),
            file.data_sequence_number.?,
            file.file_sequence_number.?,
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
            .partition_values = try alloc.dupe(iceberg_avro.PartitionValue, &[_]iceberg_avro.PartitionValue{.{
                .column_id = try alloc.dupe(u8, "region"),
                .string_value = try alloc.dupe(u8, "us-west"),
            }}),
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
    try std.testing.expectEqual(@as(?i64, 2), inventory.files[0].data_sequence_number);
    try std.testing.expect(std.mem.startsWith(u8, inventory.files[0].version_id, "iceberg:v1:snapshot=12:"));
    try std.testing.expectEqualStrings("s3://bucket/t/data/b.parquet", inventory.files[1].file_id);
    try std.testing.expectEqual(@as(usize, 1), inventory.files[1].partition_values.len);
    try std.testing.expectEqualStrings("region", inventory.files[1].partition_values[0].column_id);
    try std.testing.expectEqualStrings("us-west", inventory.files[1].partition_values[0].string_value);
}

test "iceberg inventory planner expands snapshot manifests into pinned inventory" {
    const alloc = std.testing.allocator;
    var plan = try testMetadataPlanAlloc(alloc);
    defer plan.deinit(alloc);
    var manifest_list = try testManifestListAlloc(alloc);
    defer manifest_list.deinit(alloc);
    var manifest_a = try testDataManifestAlloc(alloc, &[_]TestDataFileSpec{
        .{ .status = .added, .path = "s3://bucket/t/data/a.parquet", .rows = 3, .bytes = 4096 },
        .{ .status = .deleted, .path = "s3://bucket/t/data/old.parquet", .rows = 1, .bytes = 1024 },
    });
    defer manifest_a.deinit(alloc);
    var manifest_b = try testDataManifestAlloc(alloc, &[_]TestDataFileSpec{
        .{ .status = .existing, .path = "s3://bucket/t/data/b.parquet", .rows = 2, .bytes = 2048 },
    });
    defer manifest_b.deinit(alloc);
    const decoded = [_]DecodedManifest{
        .{ .manifest_path = manifest_list.entries[1].manifest_path, .manifest = manifest_b },
        .{ .manifest_path = manifest_list.entries[0].manifest_path, .manifest = manifest_a },
    };

    var inventory = try planInventoryFromSnapshotManifestsAlloc(alloc, .{
        .source_id = "events",
        .metadata_plan = plan,
        .manifest_list = manifest_list,
        .data_manifests = &decoded,
    });
    defer inventory.deinit(alloc);

    try std.testing.expectEqual(external_source.Format.iceberg, inventory.format);
    try std.testing.expectEqualStrings("events", inventory.source_id);
    try std.testing.expectEqualStrings("s3://bucket/t", inventory.source_uri);
    try std.testing.expectEqualStrings("12", inventory.snapshot_id);
    try std.testing.expectEqualStrings("iceberg-schema:7", inventory.schema_fingerprint);
    try std.testing.expectEqual(@as(usize, 2), inventory.files.len);
    try std.testing.expectEqualStrings("s3://bucket/t/data/a.parquet", inventory.files[0].file_id);
    try std.testing.expectEqual(@as(?i32, 7), inventory.files[0].partition_spec_id);
    try std.testing.expectEqualStrings("s3://bucket/t/data/b.parquet", inventory.files[1].file_id);
    try std.testing.expectEqual(@as(?i32, 8), inventory.files[1].partition_spec_id);
}

test "iceberg inventory planner rejects missing decoded manifests" {
    const alloc = std.testing.allocator;
    var plan = try testMetadataPlanAlloc(alloc);
    defer plan.deinit(alloc);
    var manifest_list = try testManifestListAlloc(alloc);
    defer manifest_list.deinit(alloc);
    var manifest_b = try testDataManifestAlloc(alloc, &[_]TestDataFileSpec{
        .{ .status = .existing, .path = "s3://bucket/t/data/b.parquet", .rows = 2, .bytes = 2048 },
    });
    defer manifest_b.deinit(alloc);
    const decoded = [_]DecodedManifest{.{
        .manifest_path = manifest_list.entries[1].manifest_path,
        .manifest = manifest_b,
    }};

    try std.testing.expectError(error.MissingIcebergManifest, planInventoryFromSnapshotManifestsAlloc(alloc, .{
        .source_id = "events",
        .metadata_plan = plan,
        .manifest_list = manifest_list,
        .data_manifests = &decoded,
    }));
}

test "iceberg inventory planner rejects delete manifests before delete application exists" {
    const alloc = std.testing.allocator;
    var plan = try testMetadataPlanAlloc(alloc);
    defer plan.deinit(alloc);
    var manifest_list = try testManifestListAlloc(alloc);
    defer manifest_list.deinit(alloc);
    manifest_list.entries[0].content = .deletes;

    try std.testing.expectError(error.UnsupportedIcebergDeletes, planInventoryFromSnapshotManifestsAlloc(alloc, .{
        .source_id = "events",
        .metadata_plan = plan,
        .manifest_list = manifest_list,
        .data_manifests = &.{},
    }));
}

test "iceberg inventory planner ignores inactive delete manifests" {
    const alloc = std.testing.allocator;
    var plan = try testMetadataPlanAlloc(alloc);
    defer plan.deinit(alloc);
    var manifest_list = try testManifestListAlloc(alloc);
    defer manifest_list.deinit(alloc);
    manifest_list.entries[1].content = .deletes;
    manifest_list.entries[1].existing_files_count = 0;
    manifest_list.entries[1].existing_rows_count = 0;
    manifest_list.entries[1].deleted_files_count = 1;
    manifest_list.entries[1].deleted_rows_count = 1;

    var manifest_a = try testDataManifestAlloc(alloc, &[_]TestDataFileSpec{
        .{ .status = .added, .path = "s3://bucket/t/data/a.parquet", .rows = 3, .bytes = 4096 },
        .{ .status = .deleted, .path = "s3://bucket/t/data/old.parquet", .rows = 1, .bytes = 1024 },
    });
    defer manifest_a.deinit(alloc);
    const decoded = [_]DecodedManifest{.{
        .manifest_path = manifest_list.entries[0].manifest_path,
        .manifest = manifest_a,
    }};

    var inventory = try planInventoryFromSnapshotManifestsAlloc(alloc, .{
        .source_id = "events",
        .metadata_plan = plan,
        .manifest_list = manifest_list,
        .data_manifests = &decoded,
    });
    defer inventory.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), inventory.files.len);
    try std.testing.expectEqualStrings("s3://bucket/t/data/a.parquet", inventory.files[0].file_id);
}

test "iceberg inventory planner validates manifest-list summaries when present" {
    const alloc = std.testing.allocator;
    var plan = try testMetadataPlanAlloc(alloc);
    defer plan.deinit(alloc);
    var manifest_list = try testManifestListAlloc(alloc);
    defer manifest_list.deinit(alloc);
    manifest_list.entries[0].added_files_count = 2;
    var manifest_a = try testDataManifestAlloc(alloc, &[_]TestDataFileSpec{
        .{ .status = .added, .path = "s3://bucket/t/data/a.parquet", .rows = 3, .bytes = 4096 },
        .{ .status = .deleted, .path = "s3://bucket/t/data/old.parquet", .rows = 1, .bytes = 1024 },
    });
    defer manifest_a.deinit(alloc);
    var manifest_b = try testDataManifestAlloc(alloc, &[_]TestDataFileSpec{
        .{ .status = .existing, .path = "s3://bucket/t/data/b.parquet", .rows = 2, .bytes = 2048 },
    });
    defer manifest_b.deinit(alloc);
    const decoded = [_]DecodedManifest{
        .{ .manifest_path = manifest_list.entries[0].manifest_path, .manifest = manifest_a },
        .{ .manifest_path = manifest_list.entries[1].manifest_path, .manifest = manifest_b },
    };

    try std.testing.expectError(error.IcebergManifestSummaryMismatch, planInventoryFromSnapshotManifestsAlloc(alloc, .{
        .source_id = "events",
        .metadata_plan = plan,
        .manifest_list = manifest_list,
        .data_manifests = &decoded,
    }));
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

fn testMetadataPlanAlloc(alloc: Allocator) !iceberg_metadata.Plan {
    const snapshots = try alloc.alloc(iceberg_metadata.SnapshotRef, 1);
    errdefer alloc.free(snapshots);
    snapshots[0] = .{
        .snapshot_id = try alloc.dupe(u8, "12"),
        .manifest_list_uri = try alloc.dupe(u8, "s3://bucket/t/metadata/snap-12.avro"),
        .sequence_number = 42,
        .schema_id = 7,
    };
    errdefer snapshots[0].deinit(alloc);
    return .{
        .format_version = 2,
        .metadata_uri = try alloc.dupe(u8, "s3://bucket/t/metadata/v1.metadata.json"),
        .table_uuid = try alloc.dupe(u8, "uuid-events"),
        .location = try alloc.dupe(u8, "s3://bucket/t"),
        .current_snapshot_id = try alloc.dupe(u8, "12"),
        .schema_fingerprint = try alloc.dupe(u8, "iceberg-schema:7"),
        .snapshots = snapshots,
        .current_snapshot_index = 0,
    };
}

fn testManifestListAlloc(alloc: Allocator) !iceberg_avro.ManifestList {
    const entries = try alloc.alloc(iceberg_avro.ManifestListEntry, 2);
    errdefer alloc.free(entries);
    entries[0] = .{
        .manifest_path = try alloc.dupe(u8, "s3://bucket/t/metadata/m-a.avro"),
        .manifest_length = 512,
        .partition_spec_id = 7,
        .content = .data,
        .sequence_number = 42,
        .added_files_count = 1,
        .deleted_files_count = 1,
        .added_rows_count = 3,
        .deleted_rows_count = 1,
    };
    errdefer entries[0].deinit(alloc);
    entries[1] = .{
        .manifest_path = try alloc.dupe(u8, "s3://bucket/t/metadata/m-b.avro"),
        .manifest_length = 256,
        .partition_spec_id = 8,
        .content = .data,
        .sequence_number = 42,
        .existing_files_count = 1,
        .existing_rows_count = 2,
    };
    errdefer entries[1].deinit(alloc);
    return .{ .entries = entries };
}

const TestDataFileSpec = struct {
    status: iceberg_avro.ManifestEntryStatus,
    path: []const u8,
    rows: u64,
    bytes: u64,
};

fn testDataManifestAlloc(
    alloc: Allocator,
    specs: []const TestDataFileSpec,
) !iceberg_avro.DataManifest {
    const entries = try alloc.alloc(iceberg_avro.DataFileEntry, specs.len);
    errdefer alloc.free(entries);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
    }
    for (specs, 0..) |spec, idx| {
        entries[idx] = .{
            .status = spec.status,
            .snapshot_id = 12,
            .data_sequence_number = 42,
            .file_sequence_number = 43,
            .content = .data,
            .file_path = try alloc.dupe(u8, spec.path),
            .file_format = try alloc.dupe(u8, "PARQUET"),
            .record_count = spec.rows,
            .file_size_in_bytes = spec.bytes,
        };
        initialized += 1;
    }
    return .{ .entries = entries };
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
