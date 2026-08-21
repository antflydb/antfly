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

//! Object-storage range planning for lake scans.
//!
//! Parquet and Iceberg readers should not assemble ad hoc object-store reads.
//! This module provides the stable physical read contract: tail footer ranges,
//! coalesced byte ranges, version-aware cache keys, and cache lanes that keep
//! broad scans from evicting serving-critical sidecars.

const std = @import("std");
const external_source = @import("../external_source/types.zig");
const iceberg_avro = @import("../external_source/iceberg_avro.zig");
const Allocator = std.mem.Allocator;

/// Hard ceiling for one physical object-store response. Cache capacity is a
/// retention policy, not a safe fetch limit, so this is enforced by every
/// planned read before network I/O begins.
pub const max_physical_range_read_bytes: u64 = 256 * 1024 * 1024;
pub const max_parquet_footer_metadata_bytes: u64 = 64 * 1024 * 1024;

pub const CacheLane = enum {
    metadata,
    compressed_range,
    decoded_column,
    projected_batch,
    serving_sidecar,
    broad_scan_scratch,
};

pub const RangePurpose = enum {
    parquet_footer,
    parquet_column_chunk,
    parquet_page_index,
    iceberg_metadata,
    iceberg_delete_metadata,
    decoded_column_page,
    projected_row_batch,
    sidecar_payload,
    broad_scan_scratch,

    pub fn cacheLane(self: RangePurpose) CacheLane {
        return switch (self) {
            .parquet_footer, .parquet_page_index, .iceberg_metadata, .iceberg_delete_metadata => .metadata,
            .parquet_column_chunk => .compressed_range,
            .decoded_column_page => .decoded_column,
            .projected_row_batch => .projected_batch,
            .sidecar_payload => .serving_sidecar,
            .broad_scan_scratch => .broad_scan_scratch,
        };
    }
};

pub const ObjectVersion = struct {
    etag: []const u8 = &.{},
    version_id: []const u8 = &.{},

    pub fn validate(self: ObjectVersion) !void {
        if (self.etag.len == 0 and self.version_id.len == 0) return error.InvalidLakeRangeRead;
    }
};

pub const ObjectRef = struct {
    bucket: []const u8,
    key: []const u8,
    byte_len: u64,
    version: ObjectVersion,

    pub fn validate(self: ObjectRef) !void {
        if (self.bucket.len == 0) return error.InvalidLakeRangeRead;
        if (self.key.len == 0) return error.InvalidLakeRangeRead;
        if (self.byte_len == 0) return error.InvalidLakeRangeRead;
        try self.version.validate();
    }
};

pub const ByteRange = struct {
    offset: u64,
    len: u64,

    pub fn end(self: ByteRange) u64 {
        return self.offset + self.len;
    }

    pub fn validate(self: ByteRange, object_len: u64) !void {
        if (self.len == 0) return error.InvalidLakeRangeRead;
        if (self.offset > object_len) return error.InvalidLakeRangeRead;
        if (self.len > object_len - self.offset) return error.InvalidLakeRangeRead;
    }
};

pub const RangeRead = struct {
    object: ObjectRef,
    range: ByteRange,
    purpose: RangePurpose,
    compression_codec: []const u8 = &.{},
    decoded_column_id: []const u8 = &.{},

    pub fn validate(self: RangeRead) !void {
        try self.object.validate();
        try self.range.validate(self.object.byte_len);
        if (self.range.len > max_physical_range_read_bytes) return error.LakeRangeReadTooLarge;
        switch (self.purpose) {
            .decoded_column_page, .projected_row_batch => {
                if (self.decoded_column_id.len == 0) return error.InvalidLakeRangeRead;
            },
            else => {},
        }
    }

    pub fn cacheLane(self: RangeRead) CacheLane {
        return self.purpose.cacheLane();
    }

    pub fn cacheKeyAlloc(self: RangeRead, alloc: Allocator) ![]u8 {
        try self.validate();
        return std.fmt.allocPrint(
            alloc,
            "lake-range:v1:{s}/{s}:etag={s}:version={s}:offset={d}:len={d}:purpose={s}:codec={s}:column={s}",
            .{
                self.object.bucket,
                self.object.key,
                self.object.version.etag,
                self.object.version.version_id,
                self.range.offset,
                self.range.len,
                @tagName(self.purpose),
                self.compression_codec,
                self.decoded_column_id,
            },
        );
    }
};

pub const CoalesceOptions = struct {
    max_gap_bytes: u64 = 0,
};

pub fn planParquetFooterRead(object: ObjectRef, max_probe_bytes: u64) !RangeRead {
    try object.validate();
    if (max_probe_bytes == 0) return error.InvalidLakeRangeRead;
    const read_len = @min(object.byte_len, max_probe_bytes);
    const read = RangeRead{
        .object = object,
        .range = .{ .offset = object.byte_len - read_len, .len = read_len },
        .purpose = .parquet_footer,
    };
    try read.validate();
    return read;
}

pub fn objectRefForExternalFile(
    bucket: []const u8,
    key: []const u8,
    file: external_source.FileEntry,
) !ObjectRef {
    const object = ObjectRef{
        .bucket = bucket,
        .key = key,
        .byte_len = file.byte_len,
        .version = .{
            .etag = file.etag,
            .version_id = file.version_id,
        },
    };
    try object.validate();
    return object;
}

pub fn objectRefForExternalFileUri(file: external_source.FileEntry) !ObjectRef {
    const location = try parseObjectUri(file.object_uri);
    return try objectRefForExternalFile(location.bucket, location.key, file);
}

pub fn icebergObjectVersionIdAlloc(
    alloc: Allocator,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    sequence_number: i64,
    uri: []const u8,
) ![]u8 {
    if (snapshot_id.len == 0 or schema_fingerprint.len == 0 or uri.len == 0) return error.InvalidLakeRangeRead;
    return try std.fmt.allocPrint(
        alloc,
        "iceberg-metadata:v1:snapshot={s}:schema={s}:seq={d}:uri={s}",
        .{ snapshot_id, schema_fingerprint, sequence_number, uri },
    );
}

pub fn objectRefForIcebergUri(
    uri: []const u8,
    byte_len: u64,
    version: ObjectVersion,
) !ObjectRef {
    const location = try parseObjectUri(uri);
    const object = ObjectRef{
        .bucket = location.bucket,
        .key = location.key,
        .byte_len = byte_len,
        .version = version,
    };
    try object.validate();
    return object;
}

pub fn planIcebergManifestListRead(object: ObjectRef) !RangeRead {
    return try planFullObjectMetadataRead(object, .iceberg_metadata);
}

pub fn planIcebergManifestRead(
    object: ObjectRef,
    content: iceberg_avro.ManifestContent,
) !RangeRead {
    return try planFullObjectMetadataRead(
        object,
        switch (content) {
            .data => .iceberg_metadata,
            .deletes => .iceberg_delete_metadata,
        },
    );
}

pub fn planColumnChunkRead(object: ObjectRef, chunk: external_source.ColumnChunk) !RangeRead {
    const read = RangeRead{
        .object = object,
        .range = .{ .offset = chunk.file_offset, .len = chunk.compressed_len },
        .purpose = .parquet_column_chunk,
        .compression_codec = chunk.compression_codec,
        .decoded_column_id = chunk.column_id,
    };
    try read.validate();
    return read;
}

fn planFullObjectMetadataRead(object: ObjectRef, purpose: RangePurpose) !RangeRead {
    try object.validate();
    const read = RangeRead{
        .object = object,
        .range = .{ .offset = 0, .len = object.byte_len },
        .purpose = purpose,
    };
    try read.validate();
    return read;
}

pub fn coalescePhysicalReadsAlloc(
    alloc: Allocator,
    reads: []const RangeRead,
    options: CoalesceOptions,
) ![]RangeRead {
    if (reads.len == 0) return try alloc.alloc(RangeRead, 0);

    var sorted = try alloc.dupe(RangeRead, reads);
    errdefer alloc.free(sorted);
    for (sorted) |read| try read.validate();
    std.mem.sort(RangeRead, sorted, {}, lessThanRangeRead);

    var out = std.ArrayListUnmanaged(RangeRead).empty;
    errdefer out.deinit(alloc);

    var current = sorted[0];
    for (sorted[1..]) |next| {
        if (canCoalesce(current, next, options.max_gap_bytes)) {
            const new_end = @max(current.range.end(), next.range.end());
            current.range.len = new_end - current.range.offset;
            if (!std.mem.eql(u8, current.decoded_column_id, next.decoded_column_id)) {
                current.decoded_column_id = &.{};
            }
        } else {
            try out.append(alloc, current);
            current = next;
        }
    }
    try out.append(alloc, current);
    alloc.free(sorted);
    return try out.toOwnedSlice(alloc);
}

fn canCoalesce(a: RangeRead, b: RangeRead, max_gap_bytes: u64) bool {
    if (!sameObjectVersion(a.object, b.object)) return false;
    if (a.purpose != b.purpose) return false;
    if (!std.mem.eql(u8, a.compression_codec, b.compression_codec)) return false;
    switch (a.purpose) {
        .decoded_column_page, .projected_row_batch => {
            if (!std.mem.eql(u8, a.decoded_column_id, b.decoded_column_id)) return false;
        },
        else => {},
    }
    if (b.range.offset < a.range.offset) return false;
    const a_end = a.range.end();
    if (b.range.offset > a_end and b.range.offset - a_end > max_gap_bytes) return false;
    const new_end = @max(a_end, b.range.end());
    return new_end - a.range.offset <= max_physical_range_read_bytes;
}

fn sameObjectVersion(a: ObjectRef, b: ObjectRef) bool {
    return std.mem.eql(u8, a.bucket, b.bucket) and
        std.mem.eql(u8, a.key, b.key) and
        a.byte_len == b.byte_len and
        std.mem.eql(u8, a.version.etag, b.version.etag) and
        std.mem.eql(u8, a.version.version_id, b.version.version_id);
}

pub const ObjectLocation = struct {
    bucket: []const u8,
    key: []const u8,
};

pub fn objectLocationForUri(uri: []const u8) !ObjectLocation {
    if (std.mem.startsWith(u8, uri, "s3://")) return parseBucketKey(uri["s3://".len..]);
    if (std.mem.startsWith(u8, uri, "gs://")) return parseBucketKey(uri["gs://".len..]);
    if (std.mem.startsWith(u8, uri, "object://")) return parseBucketKey(uri["object://".len..]);
    if (std.mem.startsWith(u8, uri, "file://")) {
        const key = uri["file://".len..];
        if (key.len == 0) return error.InvalidLakeRangeRead;
        return .{ .bucket = "file", .key = key };
    }
    return error.UnsupportedLakeObjectUri;
}

fn parseObjectUri(uri: []const u8) !ObjectLocation {
    return objectLocationForUri(uri);
}

fn parseBucketKey(rest: []const u8) !ObjectLocation {
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return error.InvalidLakeRangeRead;
    const bucket = rest[0..slash];
    const key = rest[slash + 1 ..];
    if (bucket.len == 0 or key.len == 0) return error.InvalidLakeRangeRead;
    return .{ .bucket = bucket, .key = key };
}

fn lessThanRangeRead(_: void, a: RangeRead, b: RangeRead) bool {
    const bucket_order = std.mem.order(u8, a.object.bucket, b.object.bucket);
    if (bucket_order != .eq) return bucket_order == .lt;
    const key_order = std.mem.order(u8, a.object.key, b.object.key);
    if (key_order != .eq) return key_order == .lt;
    if (a.range.offset != b.range.offset) return a.range.offset < b.range.offset;
    return a.range.len < b.range.len;
}

test "lake range planner creates tail footer reads with versioned cache keys" {
    const alloc = std.testing.allocator;
    const object = ObjectRef{
        .bucket = "warehouse",
        .key = "events/date=2026-06-18/part-0.parquet",
        .byte_len = 1_048_576,
        .version = .{ .etag = "etag-1" },
    };
    const read = try planParquetFooterRead(object, 64 * 1024);
    try std.testing.expectEqual(@as(u64, 983_040), read.range.offset);
    try std.testing.expectEqual(@as(u64, 65_536), read.range.len);
    try std.testing.expectEqual(CacheLane.metadata, read.cacheLane());

    const key = try read.cacheKeyAlloc(alloc);
    defer alloc.free(key);
    try std.testing.expect(std.mem.indexOf(u8, key, "etag=etag-1") != null);
    try std.testing.expect(std.mem.indexOf(u8, key, "offset=983040") != null);
    try std.testing.expect(std.mem.indexOf(u8, key, "purpose=parquet_footer") != null);
}

test "lake range planner rejects oversized reads and bounded coalescing" {
    const alloc = std.testing.allocator;
    const oversized_object = ObjectRef{
        .bucket = "warehouse",
        .key = "events/oversized.parquet",
        .byte_len = max_physical_range_read_bytes + 1,
        .version = .{ .etag = "etag-oversized" },
    };
    try std.testing.expectError(error.LakeRangeReadTooLarge, planColumnChunkRead(oversized_object, .{
        .column_id = @constCast("amount"),
        .file_offset = 0,
        .compressed_len = max_physical_range_read_bytes + 1,
    }));

    const object = ObjectRef{
        .bucket = "warehouse",
        .key = "events/coalesced.parquet",
        .byte_len = max_physical_range_read_bytes * 2,
        .version = .{ .etag = "etag-coalesced" },
    };
    const reads = [_]RangeRead{
        .{ .object = object, .range = .{ .offset = 0, .len = max_physical_range_read_bytes / 2 + 1 }, .purpose = .parquet_column_chunk },
        .{ .object = object, .range = .{ .offset = max_physical_range_read_bytes / 2 + 1, .len = max_physical_range_read_bytes / 2 + 1 }, .purpose = .parquet_column_chunk },
    };
    const coalesced = try coalescePhysicalReadsAlloc(alloc, &reads, .{});
    defer alloc.free(coalesced);
    try std.testing.expectEqual(@as(usize, 2), coalesced.len);
}

test "lake range planner derives object refs from external file uris" {
    const alloc = std.testing.allocator;
    var file = external_source.FileEntry{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1024,
        .row_count = 0,
        .row_groups = &.{},
    };
    defer file.deinit(alloc);
    const object = try objectRefForExternalFileUri(file);
    try std.testing.expectEqualStrings("bucket", object.bucket);
    try std.testing.expectEqualStrings("events/part-a.parquet", object.key);
    try std.testing.expectEqualStrings("etag-a", object.version.etag);

    alloc.free(file.object_uri);
    file.object_uri = try alloc.dupe(u8, "file:///tmp/events/part-a.parquet");
    const file_object = try objectRefForExternalFileUri(file);
    try std.testing.expectEqualStrings("file", file_object.bucket);
    try std.testing.expectEqualStrings("/tmp/events/part-a.parquet", file_object.key);

    alloc.free(file.object_uri);
    file.object_uri = try alloc.dupe(u8, "object://external-lake/part-a.parquet");
    const object_store_object = try objectRefForExternalFileUri(file);
    try std.testing.expectEqualStrings("external-lake", object_store_object.bucket);
    try std.testing.expectEqualStrings("part-a.parquet", object_store_object.key);
}

test "lake range planner creates Iceberg metadata and manifest reads" {
    const alloc = std.testing.allocator;
    const version_id = try icebergObjectVersionIdAlloc(
        alloc,
        "snapshot-12",
        "iceberg-schema:7",
        42,
        "s3://bucket/t/metadata/snap-12.avro",
    );
    defer alloc.free(version_id);

    const manifest_list_object = try objectRefForIcebergUri(
        "s3://bucket/t/metadata/snap-12.avro",
        2048,
        .{ .version_id = version_id },
    );
    const manifest_list_read = try planIcebergManifestListRead(manifest_list_object);
    try std.testing.expectEqual(@as(u64, 0), manifest_list_read.range.offset);
    try std.testing.expectEqual(@as(u64, 2048), manifest_list_read.range.len);
    try std.testing.expectEqual(CacheLane.metadata, manifest_list_read.cacheLane());
    try std.testing.expectEqual(RangePurpose.iceberg_metadata, manifest_list_read.purpose);
    const key = try manifest_list_read.cacheKeyAlloc(alloc);
    defer alloc.free(key);
    try std.testing.expect(std.mem.indexOf(u8, key, "purpose=iceberg_metadata") != null);
    try std.testing.expect(std.mem.indexOf(u8, key, "version=iceberg-metadata:v1:snapshot=snapshot-12") != null);

    const data_manifest_read = try planIcebergManifestRead(manifest_list_object, .data);
    try std.testing.expectEqual(RangePurpose.iceberg_metadata, data_manifest_read.purpose);
    const delete_manifest_read = try planIcebergManifestRead(manifest_list_object, .deletes);
    try std.testing.expectEqual(RangePurpose.iceberg_delete_metadata, delete_manifest_read.purpose);
}

test "lake range planner derives Iceberg refs from supported object URI schemes" {
    const version = ObjectVersion{ .version_id = "iceberg-v1" };
    const gs_object = try objectRefForIcebergUri("gs://bucket/t/metadata/m0.avro", 128, version);
    try std.testing.expectEqualStrings("bucket", gs_object.bucket);
    try std.testing.expectEqualStrings("t/metadata/m0.avro", gs_object.key);

    const file_object = try objectRefForIcebergUri("file:///tmp/t/metadata/m0.avro", 128, version);
    try std.testing.expectEqualStrings("file", file_object.bucket);
    try std.testing.expectEqualStrings("/tmp/t/metadata/m0.avro", file_object.key);

    try std.testing.expectError(error.InvalidLakeRangeRead, objectRefForIcebergUri(
        "s3://bucket/t/metadata/m0.avro",
        0,
        version,
    ));
    try std.testing.expectError(error.InvalidLakeRangeRead, objectRefForIcebergUri(
        "s3://bucket/t/metadata/m0.avro",
        128,
        .{},
    ));
}

test "lake range planner coalesces adjacent column chunks by object version" {
    const alloc = std.testing.allocator;
    const object = ObjectRef{
        .bucket = "warehouse",
        .key = "events/part-0.parquet",
        .byte_len = 10_000,
        .version = .{ .etag = "etag-1", .version_id = "v1" },
    };
    const changed_version = ObjectRef{
        .bucket = "warehouse",
        .key = "events/part-0.parquet",
        .byte_len = 10_000,
        .version = .{ .etag = "etag-2", .version_id = "v2" },
    };
    const reads = [_]RangeRead{
        .{
            .object = object,
            .range = .{ .offset = 200, .len = 50 },
            .purpose = .parquet_column_chunk,
            .compression_codec = "zstd",
            .decoded_column_id = "amount",
        },
        .{
            .object = object,
            .range = .{ .offset = 100, .len = 80 },
            .purpose = .parquet_column_chunk,
            .compression_codec = "zstd",
            .decoded_column_id = "tenant_id",
        },
        .{
            .object = changed_version,
            .range = .{ .offset = 260, .len = 20 },
            .purpose = .parquet_column_chunk,
            .compression_codec = "zstd",
            .decoded_column_id = "amount",
        },
    };

    const coalesced = try coalescePhysicalReadsAlloc(alloc, &reads, .{ .max_gap_bytes = 32 });
    defer alloc.free(coalesced);
    try std.testing.expectEqual(@as(usize, 2), coalesced.len);
    try std.testing.expectEqual(@as(u64, 100), coalesced[0].range.offset);
    try std.testing.expectEqual(@as(u64, 150), coalesced[0].range.len);
    try std.testing.expectEqualStrings("", coalesced[0].decoded_column_id);
    try std.testing.expectEqualStrings("etag-2", coalesced[1].object.version.etag);
}

test "lake range planner validates cache lanes and decoded column keys" {
    const alloc = std.testing.allocator;
    const object = ObjectRef{
        .bucket = "warehouse",
        .key = "events/part-0.parquet",
        .byte_len = 4096,
        .version = .{ .version_id = "v1" },
    };
    const decoded = RangeRead{
        .object = object,
        .range = .{ .offset = 0, .len = 1024 },
        .purpose = .decoded_column_page,
        .compression_codec = "plain",
        .decoded_column_id = "amount",
    };
    try std.testing.expectEqual(CacheLane.decoded_column, decoded.cacheLane());
    const key = try decoded.cacheKeyAlloc(alloc);
    defer alloc.free(key);
    try std.testing.expect(std.mem.indexOf(u8, key, "version=v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, key, "column=amount") != null);

    const invalid = RangeRead{
        .object = object,
        .range = .{ .offset = 0, .len = 1024 },
        .purpose = .decoded_column_page,
    };
    try std.testing.expectError(error.InvalidLakeRangeRead, invalid.validate());
}

test "lake range planner does not coalesce distinct decoded column identities" {
    const alloc = std.testing.allocator;
    const object = ObjectRef{
        .bucket = "warehouse",
        .key = "events/part-0.parquet",
        .byte_len = 4096,
        .version = .{ .version_id = "v1" },
    };
    const reads = [_]RangeRead{
        .{
            .object = object,
            .range = .{ .offset = 0, .len = 128 },
            .purpose = .decoded_column_page,
            .decoded_column_id = "amount",
        },
        .{
            .object = object,
            .range = .{ .offset = 128, .len = 128 },
            .purpose = .decoded_column_page,
            .decoded_column_id = "tenant_id",
        },
    };
    const physical = try coalescePhysicalReadsAlloc(alloc, &reads, .{});
    defer alloc.free(physical);
    try std.testing.expectEqual(@as(usize, 2), physical.len);
    for (physical) |read| try read.validate();
}

test "lake range planner creates column chunk reads from external inventory metadata" {
    const alloc = std.testing.allocator;
    var file = external_source.FileEntry{
        .file_id = try alloc.dupe(u8, "file-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://warehouse/events/file-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-file-a"),
        .byte_len = 4096,
        .row_count = 2,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 2,
            .file_offset = 4,
            .total_byte_len = 512,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 128,
                .compressed_len = 64,
                .uncompressed_len = 256,
                .compression_codec = try alloc.dupe(u8, "zstd"),
                .encoding = try alloc.dupe(u8, "plain"),
            }}),
        }}),
    };
    defer file.deinit(alloc);

    const object = try objectRefForExternalFile("warehouse", "events/file-a.parquet", file);
    const read = try planColumnChunkRead(object, file.row_groups[0].column_chunks[0]);
    try std.testing.expectEqual(@as(u64, 128), read.range.offset);
    try std.testing.expectEqual(@as(u64, 64), read.range.len);
    try std.testing.expectEqual(CacheLane.compressed_range, read.cacheLane());
    try std.testing.expectEqualStrings("amount", read.decoded_column_id);
    try std.testing.expectEqualStrings("zstd", read.compression_codec);
}
