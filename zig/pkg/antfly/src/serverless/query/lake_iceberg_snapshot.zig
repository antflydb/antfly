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

//! Object-storage backed Iceberg snapshot planning.
//!
//! This stitches the Iceberg pieces together: read table metadata, read the
//! pinned manifest list, read each data manifest, and produce an Antfly external
//! source inventory that the existing Parquet footer discovery path can enrich.

const std = @import("std");
const Allocator = std.mem.Allocator;
const external_source = @import("../external_source/types.zig");
const iceberg_avro = @import("../external_source/iceberg_avro.zig");
const iceberg_inventory = @import("../external_source/iceberg_inventory.zig");
const iceberg_metadata = @import("../external_source/iceberg_metadata.zig");
const lake_object_reader = @import("lake_object_reader.zig");
const lake_parquet_rowgroup = @import("lake_parquet_rowgroup.zig");
const lake_range_io = @import("lake_range_io.zig");
const object_storage = @import("../../storage/object_storage.zig");

pub const SnapshotReadRequest = struct {
    client: object_storage.ObjectStorage,
    source_id: []const u8,
    metadata_uri: []const u8,
    requested_snapshot_id: ?[]const u8 = null,
    cache: ?*lake_parquet_rowgroup.ObjectRangeCache = null,

    pub fn validate(self: SnapshotReadRequest) !void {
        if (self.source_id.len == 0) return error.InvalidIcebergSnapshotRead;
        if (self.metadata_uri.len == 0) return error.InvalidIcebergSnapshotRead;
    }
};

pub fn readSnapshotInventoryAlloc(
    alloc: Allocator,
    request: SnapshotReadRequest,
) !external_source.Inventory {
    try request.validate();
    var client = request.client;
    client.allocator = alloc;

    const metadata_bytes = try readFullObjectAlloc(alloc, &client, request.cache, request.metadata_uri, .iceberg_metadata, null);
    defer alloc.free(metadata_bytes);
    var metadata_plan = try iceberg_metadata.parseMetadataPlanAlloc(
        alloc,
        request.metadata_uri,
        metadata_bytes,
        request.requested_snapshot_id,
    );
    defer metadata_plan.deinit(alloc);

    const current_snapshot = metadata_plan.currentSnapshot();
    const manifest_list_bytes = try readFullObjectAlloc(alloc, &client, request.cache, current_snapshot.manifest_list_uri, .iceberg_metadata, null);
    defer alloc.free(manifest_list_bytes);
    var manifest_list = try iceberg_avro.parseManifestListAlloc(alloc, manifest_list_bytes);
    defer manifest_list.deinit(alloc);

    const decoded_manifests = try alloc.alloc(iceberg_inventory.DecodedManifest, manifest_list.entries.len);
    defer alloc.free(decoded_manifests);
    var initialized: usize = 0;
    defer {
        for (decoded_manifests[0..initialized]) |*decoded| {
            decoded.manifest.deinit(alloc);
        }
    }

    for (manifest_list.entries) |manifest_entry| {
        switch (manifest_entry.content) {
            .data => {
                const manifest_bytes = try readFullObjectAlloc(
                    alloc,
                    &client,
                    request.cache,
                    manifest_entry.manifest_path,
                    .iceberg_metadata,
                    manifest_entry.manifest_length,
                );
                defer alloc.free(manifest_bytes);
                decoded_manifests[initialized] = .{
                    .manifest_path = manifest_entry.manifest_path,
                    .manifest = try iceberg_avro.parseDataManifestAlloc(alloc, manifest_bytes),
                };
                initialized += 1;
            },
            .deletes => try readUnsupportedDeleteManifestAlloc(
                alloc,
                &client,
                request.cache,
                manifest_entry,
            ),
        }
    }

    return try iceberg_inventory.planInventoryFromSnapshotManifestsAlloc(alloc, .{
        .source_id = request.source_id,
        .metadata_plan = metadata_plan,
        .manifest_list = manifest_list,
        .data_manifests = decoded_manifests[0..initialized],
    });
}

fn readUnsupportedDeleteManifestAlloc(
    alloc: Allocator,
    client: *object_storage.ObjectStorage,
    cache: ?*lake_parquet_rowgroup.ObjectRangeCache,
    manifest_entry: iceberg_avro.ManifestListEntry,
) !void {
    const manifest_bytes = try readFullObjectAlloc(
        alloc,
        client,
        cache,
        manifest_entry.manifest_path,
        .iceberg_delete_metadata,
        manifest_entry.manifest_length,
    );
    defer alloc.free(manifest_bytes);

    var delete_manifest = try iceberg_avro.parseDataManifestAlloc(alloc, manifest_bytes);
    defer delete_manifest.deinit(alloc);
    try validateUnsupportedDeleteManifest(manifest_entry, delete_manifest);
    return error.UnsupportedIcebergDeletes;
}

fn validateUnsupportedDeleteManifest(
    manifest_entry: iceberg_avro.ManifestListEntry,
    manifest: iceberg_avro.DataManifest,
) !void {
    var added_files: u32 = 0;
    var existing_files: u32 = 0;
    var deleted_files: u32 = 0;
    var added_rows: u64 = 0;
    var existing_rows: u64 = 0;
    var deleted_rows: u64 = 0;
    for (manifest.entries) |entry| {
        switch (entry.content) {
            .data => return error.InvalidIcebergDeleteManifest,
            .position_deletes, .equality_deletes => {},
        }
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
    if (!hasManifestSummary(manifest_entry)) return;
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

fn readFullObjectAlloc(
    alloc: Allocator,
    client: *object_storage.ObjectStorage,
    cache: ?*lake_parquet_rowgroup.ObjectRangeCache,
    uri: []const u8,
    purpose: lake_range_io.RangePurpose,
    expected_byte_len: ?u64,
) ![]u8 {
    const location = try lake_range_io.objectLocationForUri(uri);
    var meta = try client.statObject(location.bucket, location.key);
    defer meta.deinit(alloc);
    if (expected_byte_len) |expected| {
        if (meta.content_length != expected) return error.IcebergManifestLengthMismatch;
    }
    const version = try objectVersionForMetadataAlloc(alloc, meta, uri);
    defer {
        if (version.etag.len > 0) alloc.free(@constCast(version.etag));
        if (version.version_id.len > 0) alloc.free(@constCast(version.version_id));
    }
    const object = try lake_range_io.objectRefForIcebergUri(uri, meta.content_length, version);
    const read = switch (purpose) {
        .iceberg_metadata => try lake_range_io.planIcebergManifestListRead(object),
        .iceberg_delete_metadata => try lake_range_io.planIcebergManifestRead(object, .deletes),
        else => return error.InvalidIcebergSnapshotRead,
    };
    return try readMaybeCachedIcebergRangeAlloc(alloc, client.*, cache, read);
}

fn readMaybeCachedIcebergRangeAlloc(
    alloc: Allocator,
    client: object_storage.ObjectStorage,
    cache: ?*lake_parquet_rowgroup.ObjectRangeCache,
    read: lake_range_io.RangeRead,
) ![]u8 {
    var range_reader = lake_object_reader.ObjectStorageRangeReader.init(client);
    const parquet_reader = range_reader.parquetReader();
    if (cache) |range_cache| {
        return try range_cache.readAlloc(alloc, parquet_reader, read);
    }
    return try parquet_reader.readPlannedAlloc(alloc, read);
}

fn objectVersionForMetadataAlloc(
    alloc: Allocator,
    meta: object_storage.ObjectMetadata,
    uri: []const u8,
) !lake_range_io.ObjectVersion {
    const etag = if (meta.etag) |etag| try alloc.dupe(u8, etag) else &.{};
    errdefer if (etag.len > 0) alloc.free(etag);
    const version_id = if (meta.version_id) |version_id| try alloc.dupe(u8, version_id) else if (etag.len == 0) blk: {
        break :blk try std.fmt.allocPrint(
            alloc,
            "object-stat:v1:uri={s}:len={d}",
            .{ uri, meta.content_length },
        );
    } else &.{};
    errdefer if (version_id.len > 0) alloc.free(version_id);
    const version = lake_range_io.ObjectVersion{
        .etag = etag,
        .version_id = version_id,
    };
    try version.validate();
    return version;
}

test "iceberg snapshot reader plans inventory from object storage metadata and manifests" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var data_manifest = try buildDataManifestFixture(alloc);
    defer data_manifest.deinit(alloc);
    var manifest_list = try buildManifestListFixture(alloc, data_manifest.items.len);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var data_manifest_put = try client.putObject("bucket", "t/metadata/m-a.avro", data_manifest.items, .{});
    defer data_manifest_put.deinit(alloc);

    var inventory = try readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
    });
    defer inventory.deinit(alloc);

    try std.testing.expectEqual(external_source.Format.iceberg, inventory.format);
    try std.testing.expectEqualStrings("events", inventory.source_id);
    try std.testing.expectEqualStrings("s3://bucket/t", inventory.source_uri);
    try std.testing.expectEqualStrings("12", inventory.snapshot_id);
    try std.testing.expectEqualStrings("iceberg-schema:7", inventory.schema_fingerprint);
    try std.testing.expectEqual(@as(usize, 2), inventory.files.len);
    try std.testing.expectEqualStrings("s3://bucket/t/data/a.parquet", inventory.files[0].file_id);
    try std.testing.expectEqualStrings("s3://bucket/t/data/b.parquet", inventory.files[1].file_id);
}

test "iceberg snapshot reader reuses cached metadata and manifest ranges" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var data_manifest = try buildDataManifestFixture(alloc);
    defer data_manifest.deinit(alloc);
    var manifest_list = try buildManifestListFixture(alloc, data_manifest.items.len);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var data_manifest_put = try client.putObject("bucket", "t/metadata/m-a.avro", data_manifest.items, .{});
    defer data_manifest_put.deinit(alloc);

    var cache = lake_parquet_rowgroup.ObjectRangeCache{};
    defer cache.deinit(alloc);

    var first = try readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
        .cache = &cache,
    });
    defer first.deinit(alloc);
    const first_stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 0), first_stats.hits);
    try std.testing.expectEqual(@as(usize, 3), first_stats.misses);
    try std.testing.expect(first_stats.stored_bytes > 0);
    try std.testing.expectEqual(@as(usize, 3), first_stats.lane(.metadata).misses);
    try std.testing.expect(first_stats.lane(.metadata).stored_bytes > 0);

    var second = try readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
        .cache = &cache,
    });
    defer second.deinit(alloc);
    const second_stats = cache.statsSnapshot();
    try std.testing.expectEqual(first_stats.misses, second_stats.misses);
    try std.testing.expectEqual(@as(usize, 3), second_stats.hits);
    try std.testing.expectEqual(@as(usize, 3), second_stats.lane(.metadata).hits);
    try std.testing.expectEqualStrings(first.snapshot_id, second.snapshot_id);
    try std.testing.expectEqual(first.files.len, second.files.len);
}

test "iceberg snapshot reader applies object range cache metadata lane admission" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var data_manifest = try buildDataManifestFixture(alloc);
    defer data_manifest.deinit(alloc);
    var manifest_list = try buildManifestListFixture(alloc, data_manifest.items.len);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var data_manifest_put = try client.putObject("bucket", "t/metadata/m-a.avro", data_manifest.items, .{});
    defer data_manifest_put.deinit(alloc);

    var cache = lake_parquet_rowgroup.ObjectRangeCache{
        .policy = lake_parquet_rowgroup.ObjectRangeCachePolicy.withLaneLimit(.metadata, 1),
    };
    defer cache.deinit(alloc);

    var inventory = try readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
        .cache = &cache,
    });
    defer inventory.deinit(alloc);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 3), stats.misses);
    try std.testing.expectEqual(@as(usize, 3), stats.lane(.metadata).misses);
    try std.testing.expectEqual(@as(usize, 0), stats.stored_bytes);
    try std.testing.expect(stats.rejected_bytes >= testMetadataJson().len + manifest_list.items.len + data_manifest.items.len);
    try std.testing.expectEqual(stats.rejected_bytes, stats.lane(.metadata).rejected_bytes);
}

test "iceberg snapshot reader rejects corrupted cached metadata range lengths" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);

    var meta = try client.statObject("bucket", "t/metadata/v1.metadata.json");
    defer meta.deinit(alloc);
    const version = try objectVersionForMetadataAlloc(alloc, meta, "s3://bucket/t/metadata/v1.metadata.json");
    defer {
        if (version.etag.len > 0) alloc.free(@constCast(version.etag));
        if (version.version_id.len > 0) alloc.free(@constCast(version.version_id));
    }
    const object = try lake_range_io.objectRefForIcebergUri(
        "s3://bucket/t/metadata/v1.metadata.json",
        meta.content_length,
        version,
    );
    const read = try lake_range_io.planIcebergManifestListRead(object);

    var cache = lake_parquet_rowgroup.ObjectRangeCache{};
    defer cache.deinit(alloc);
    const cache_key = try read.cacheKeyAlloc(alloc);
    const corrupt_bytes = try alloc.dupe(u8, "{}");
    cache.entries.put(alloc, cache_key, .{
        .bytes = corrupt_bytes,
        .checksum = [_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length,
    }) catch |err| {
        alloc.free(cache_key);
        alloc.free(corrupt_bytes);
        return err;
    };
    cache.stats.stored_bytes += corrupt_bytes.len;

    try std.testing.expectError(error.InvalidLakeRangeRead, readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
        .cache = &cache,
    }));
    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 0), stats.hits);
    try std.testing.expectEqual(@as(usize, 0), stats.misses);
}

test "iceberg snapshot reader rejects manifest length mismatches" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var data_manifest = try buildDataManifestFixture(alloc);
    defer data_manifest.deinit(alloc);
    var manifest_list = try buildManifestListFixture(alloc, data_manifest.items.len + 1);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var data_manifest_put = try client.putObject("bucket", "t/metadata/m-a.avro", data_manifest.items, .{});
    defer data_manifest_put.deinit(alloc);

    try std.testing.expectError(error.IcebergManifestLengthMismatch, readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
    }));
}

test "iceberg snapshot reader reads delete manifest metadata before failing closed" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var delete_manifest = try buildDeleteManifestFixture(alloc);
    defer delete_manifest.deinit(alloc);
    var manifest_list = try buildDeleteManifestListFixture(alloc, delete_manifest.items.len);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var delete_manifest_put = try client.putObject("bucket", "t/metadata/d-a.avro", delete_manifest.items, .{});
    defer delete_manifest_put.deinit(alloc);

    var cache = lake_parquet_rowgroup.ObjectRangeCache{};
    defer cache.deinit(alloc);

    try std.testing.expectError(error.UnsupportedIcebergDeletes, readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
        .cache = &cache,
    }));
    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 0), stats.hits);
    try std.testing.expectEqual(@as(usize, 3), stats.misses);
    try std.testing.expectEqual(@as(usize, 3), stats.lane(.metadata).misses);
    try std.testing.expect(stats.stored_bytes >= testMetadataJson().len + manifest_list.items.len + delete_manifest.items.len);
}

test "iceberg snapshot reader rejects requested snapshot mismatch" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);

    try std.testing.expectError(error.IcebergSnapshotMismatch, readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "11",
    }));
}

fn testMetadataJson() []const u8 {
    return
    \\{
    \\  "format-version": 2,
    \\  "table-uuid": "uuid-events",
    \\  "location": "s3://bucket/t",
    \\  "current-schema-id": 7,
    \\  "current-snapshot-id": 12,
    \\  "snapshots": [
    \\    {
    \\      "snapshot-id": 12,
    \\      "sequence-number": 42,
    \\      "timestamp-ms": 1700000000000,
    \\      "manifest-list": "s3://bucket/t/metadata/snap-12.avro"
    \\    }
    \\  ]
    \\}
    ;
}

fn buildManifestListFixture(alloc: Allocator, manifest_length: usize) !std.ArrayListUnmanaged(u8) {
    return try buildOneManifestListFixture(alloc, "s3://bucket/t/metadata/m-a.avro", .data, manifest_length, .{
        .added_files = 0,
        .existing_files = 0,
        .deleted_files = 0,
        .added_rows = 0,
        .existing_rows = 0,
        .deleted_rows = 0,
    });
}

fn buildDeleteManifestListFixture(alloc: Allocator, manifest_length: usize) !std.ArrayListUnmanaged(u8) {
    return try buildOneManifestListFixture(alloc, "s3://bucket/t/metadata/d-a.avro", .deletes, manifest_length, .{
        .added_files = 1,
        .existing_files = 0,
        .deleted_files = 0,
        .added_rows = 1,
        .existing_rows = 0,
        .deleted_rows = 0,
    });
}

const ManifestSummaryFixture = struct {
    added_files: i64,
    existing_files: i64,
    deleted_files: i64,
    added_rows: i64,
    existing_rows: i64,
    deleted_rows: i64,
};

fn buildOneManifestListFixture(
    alloc: Allocator,
    manifest_path: []const u8,
    content: iceberg_avro.ManifestContent,
    manifest_length: usize,
    summary: ManifestSummaryFixture,
) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendAvroHeader(alloc, &out, manifestListSchema(), "0123456789abcdef");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendString(alloc, &block, manifest_path);
    try appendLong(alloc, &block, @intCast(manifest_length));
    try appendLong(alloc, &block, 0);
    try appendLong(alloc, &block, @intFromEnum(content));
    try appendLong(alloc, &block, 42);
    try appendLong(alloc, &block, summary.added_files);
    try appendLong(alloc, &block, summary.existing_files);
    try appendLong(alloc, &block, summary.deleted_files);
    try appendLong(alloc, &block, summary.added_rows);
    try appendLong(alloc, &block, summary.existing_rows);
    try appendLong(alloc, &block, summary.deleted_rows);

    try appendAvroBlock(alloc, &out, block.items, 1, "0123456789abcdef");
    return out;
}

fn buildDataManifestFixture(alloc: Allocator) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendAvroHeader(alloc, &out, dataManifestSchema(), "fedcba9876543210");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendDataManifestRecord(alloc, &block, .added, .data, "s3://bucket/t/data/a.parquet", 3, 4096);
    try appendDataManifestRecord(alloc, &block, .existing, .data, "s3://bucket/t/data/b.parquet", 2, 2048);

    try appendAvroBlock(alloc, &out, block.items, 2, "fedcba9876543210");
    return out;
}

fn buildDeleteManifestFixture(alloc: Allocator) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendAvroHeader(alloc, &out, dataManifestSchema(), "fedcba9876543210");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendDataManifestRecord(alloc, &block, .added, .position_deletes, "s3://bucket/t/deletes/pos-a.parquet", 1, 1024);

    try appendAvroBlock(alloc, &out, block.items, 1, "fedcba9876543210");
    return out;
}

fn manifestListSchema() []const u8 {
    return
    \\{"type":"record","name":"manifest_file","fields":[
    \\{"name":"manifest_path","type":"string"},
    \\{"name":"manifest_length","type":"long"},
    \\{"name":"partition_spec_id","type":"int"},
    \\{"name":"content","type":"int"},
    \\{"name":"sequence_number","type":"long"},
    \\{"name":"added_files_count","type":"int"},
    \\{"name":"existing_files_count","type":"int"},
    \\{"name":"deleted_files_count","type":"int"},
    \\{"name":"added_rows_count","type":"long"},
    \\{"name":"existing_rows_count","type":"long"},
    \\{"name":"deleted_rows_count","type":"long"}]}
    ;
}

fn dataManifestSchema() []const u8 {
    return
    \\{"type":"record","name":"manifest_entry","fields":[
    \\{"name":"status","type":"int"},
    \\{"name":"snapshot_id","type":["null","long"]},
    \\{"name":"data_sequence_number","type":["null","long"]},
    \\{"name":"file_sequence_number","type":["null","long"]},
    \\{"name":"data_file","type":{"type":"record","name":"data_file","fields":[
    \\{"name":"content","type":"int"},
    \\{"name":"file_path","type":"string"},
    \\{"name":"file_format","type":"string"},
    \\{"name":"record_count","type":"long"},
    \\{"name":"file_size_in_bytes","type":"long"}]}}]}
    ;
}

fn appendDataManifestRecord(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    status: iceberg_avro.ManifestEntryStatus,
    content: iceberg_avro.DataFileContent,
    file_path: []const u8,
    record_count: i64,
    file_size_in_bytes: i64,
) !void {
    try appendLong(alloc, out, @intFromEnum(status));
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 12);
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 42);
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 43);
    try appendLong(alloc, out, @intFromEnum(content));
    try appendString(alloc, out, file_path);
    try appendString(alloc, out, "PARQUET");
    try appendLong(alloc, out, record_count);
    try appendLong(alloc, out, file_size_in_bytes);
}

fn appendAvroHeader(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    schema: []const u8,
    sync: []const u8,
) !void {
    try out.appendSlice(alloc, "Obj\x01");
    try appendLong(alloc, out, 2);
    try appendString(alloc, out, "avro.schema");
    try appendBytes(alloc, out, schema);
    try appendString(alloc, out, "avro.codec");
    try appendBytes(alloc, out, "null");
    try appendLong(alloc, out, 0);
    try out.appendSlice(alloc, sync);
}

fn appendAvroBlock(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    block: []const u8,
    count: i64,
    sync: []const u8,
) !void {
    try appendLong(alloc, out, count);
    try appendLong(alloc, out, @intCast(block.len));
    try out.appendSlice(alloc, block);
    try out.appendSlice(alloc, sync);
}

fn appendBytes(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    try appendLong(alloc, out, @intCast(bytes.len));
    try out.appendSlice(alloc, bytes);
}

fn appendString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), text: []const u8) !void {
    try appendBytes(alloc, out, text);
}

fn appendLong(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: i64) !void {
    const raw = encodeZigzag(value);
    var remaining = raw;
    while (remaining >= 0x80) {
        try out.append(alloc, @as(u8, @intCast(remaining & 0x7f)) | 0x80);
        remaining >>= 7;
    }
    try out.append(alloc, @intCast(remaining));
}

fn encodeZigzag(value: i64) u64 {
    if (value >= 0) return @as(u64, @intCast(value)) << 1;
    const magnitude: u64 = @intCast(-(value + 1));
    return (magnitude << 1) | 1;
}
