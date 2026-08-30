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

//! Object-storage backed range reader for lake scans.

const std = @import("std");
const lake_range_io = @import("lake_range_io.zig");
const lake_parquet_rowgroup = @import("lake_parquet_rowgroup.zig");
const object_storage = @import("../../storage/object_storage.zig");

const Allocator = std.mem.Allocator;

pub const ObjectStorageRangeReader = struct {
    client: object_storage.ObjectStorage,
    retry_policy: RetryPolicy = .{},

    pub const RetryPolicy = struct {
        max_attempts: u8 = 1,

        fn attempts(self: RetryPolicy) u8 {
            return @max(@as(u8, 1), self.max_attempts);
        }
    };

    pub fn init(client: object_storage.ObjectStorage) ObjectStorageRangeReader {
        return .{ .client = client };
    }

    pub fn initWithRetry(client: object_storage.ObjectStorage, retry_policy: RetryPolicy) ObjectStorageRangeReader {
        return .{ .client = client, .retry_policy = retry_policy };
    }

    pub fn parquetReader(self: *@This()) lake_parquet_rowgroup.ObjectRangeReader {
        return .{
            .ctx = self,
            .read_range_alloc = readRangeAlloc,
            .read_planned_range_alloc = readPlannedRangeAlloc,
        };
    }

    fn readRangeAlloc(
        ctx: *anyopaque,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        offset: u64,
        len: usize,
    ) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        var result = try self.getObjectWithRetry(alloc, bucket, key, .{
            .range = .{ .offset = offset, .length = len },
        });
        defer result.deinit(alloc);
        if (result.body.len != len) return error.InvalidLakeRangeRead;
        return try alloc.dupe(u8, result.body);
    }

    fn readPlannedRangeAlloc(
        ctx: *anyopaque,
        alloc: Allocator,
        read: lake_range_io.RangeRead,
    ) ![]u8 {
        try read.validate();
        const self: *@This() = @ptrCast(@alignCast(ctx));
        const len: usize = std.math.cast(usize, read.range.len) orelse return error.InvalidLakeRangeRead;
        var result = try self.getObjectWithRetry(alloc, read.object.bucket, read.object.key, .{
            .range = .{ .offset = read.range.offset, .length = len },
            .if_match_etag = if (read.object.version.etag.len == 0) null else read.object.version.etag,
            .version_id = if (read.object.version.version_id.len == 0) null else read.object.version.version_id,
        });
        defer result.deinit(alloc);
        if (result.body.len != len) return error.InvalidLakeRangeRead;
        try validatePlannedObjectMetadata(read, result.metadata);
        try validatePlannedObjectChecksum(read, result.metadata, result.body);
        return try alloc.dupe(u8, result.body);
    }

    fn getObjectWithRetry(
        self: *@This(),
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        opts: object_storage.GetOptions,
    ) !object_storage.GetResult {
        const max_attempts = self.retry_policy.attempts();
        var attempt: u8 = 0;
        while (true) {
            attempt += 1;
            var client = self.client;
            client.allocator = alloc;
            return client.getObject(bucket, key, opts) catch |err| {
                if (attempt >= max_attempts or !isRetryableObjectReadError(err)) return err;
                continue;
            };
        }
    }
};

fn validatePlannedObjectMetadata(read: lake_range_io.RangeRead, metadata: object_storage.ObjectMetadata) !void {
    if (read.object.version.etag.len != 0) {
        const returned_etag = metadata.etag orelse return error.PreconditionFailed;
        if (!std.mem.eql(u8, returned_etag, read.object.version.etag)) return error.PreconditionFailed;
    }
    if (read.object.version.version_id.len != 0) {
        const returned_version = metadata.version_id orelse return error.PreconditionFailed;
        if (!std.mem.eql(u8, returned_version, read.object.version.version_id)) return error.PreconditionFailed;
    }
}

fn validatePlannedObjectChecksum(
    read: lake_range_io.RangeRead,
    metadata: object_storage.ObjectMetadata,
    body: []const u8,
) !void {
    const checksum = metadata.checksum orelse return;
    if (body.len != read.range.len) return error.InvalidLakeRangeRead;
    if (!try plannedChecksumCoversBody(read, metadata, body.len)) return;
    // Multipart/composite checksums do not describe the bytes returned by a
    // normal full-object GET. Treat them as provider metadata, not a digest we
    // can compare to the response body.
    if (checksum.checksum_type == .composite) return;
    if (metadata.checksum_scope == .object and checksum.checksum_type == .unknown) return;

    switch (checksum.algorithm) {
        .crc32_base64 => {
            var digest: [4]u8 = undefined;
            std.mem.writeInt(u32, &digest, std.hash.crc.@"CRC-32/ISO-HDLC".hash(body), .big);
            try validateBase64Digest(checksum.value, &digest);
        },
        .crc32c_base64 => {
            var digest: [4]u8 = undefined;
            std.mem.writeInt(u32, &digest, std.hash.crc.@"CRC-32/ISCSI".hash(body), .big);
            try validateBase64Digest(checksum.value, &digest);
        },
        .crc64nvme_base64 => {
            const Crc64Nvme = std.hash.crc.Generic(u64, .{
                .polynomial = 0xad93d23594c93659,
                .initial = 0xffffffffffffffff,
                .reflect_input = true,
                .reflect_output = true,
                .xor_output = 0xffffffffffffffff,
            });
            var digest: [8]u8 = undefined;
            std.mem.writeInt(u64, &digest, Crc64Nvme.hash(body), .big);
            try validateBase64Digest(checksum.value, &digest);
        },
        .sha1_base64 => {
            var digest: [std.crypto.hash.Sha1.digest_length]u8 = undefined;
            std.crypto.hash.Sha1.hash(body, &digest, .{});
            try validateBase64Digest(checksum.value, &digest);
        },
        .sha256_hex => {
            var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
            var expected: [std.crypto.hash.sha2.Sha256.digest_length * 2]u8 = undefined;
            for (digest, 0..) |byte, idx| {
                expected[idx * 2] = hexNibble(byte >> 4);
                expected[idx * 2 + 1] = hexNibble(byte & 0x0f);
            }
            if (!std.ascii.eqlIgnoreCase(&expected, checksum.value)) return error.PreconditionFailed;
        },
        .sha256_base64 => {
            var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
            try validateBase64Digest(checksum.value, &digest);
        },
        .sha512_base64 => {
            var digest: [std.crypto.hash.sha2.Sha512.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha512.hash(body, &digest, .{});
            try validateBase64Digest(checksum.value, &digest);
        },
        .md5_base64 => {
            const digest = std.crypto.hash.Md5.hashResult(body);
            try validateBase64Digest(checksum.value, &digest);
        },
        // Provider byte-order and XXH3-128 portability are not part of the
        // object-store contract yet. Identity guards still fail closed via
        // ETag/version; optional algorithms must not make valid objects
        // unreadable until their encoding semantics are explicit.
        .xxhash64_base64, .xxhash3_base64, .xxhash128_base64 => return,
    }
}

fn validateBase64Digest(
    expected: []const u8,
    digest: []const u8,
) !void {
    const encoded_len = std.base64.standard.Encoder.calcSize(digest.len);
    var encoded_buffer: [std.base64.standard.Encoder.calcSize(std.crypto.hash.sha2.Sha512.digest_length)]u8 = undefined;
    const encoded = encoded_buffer[0..encoded_len];
    _ = std.base64.standard.Encoder.encode(encoded, digest);
    if (!std.mem.eql(u8, encoded, expected)) return error.PreconditionFailed;
}

fn plannedChecksumCoversBody(read: lake_range_io.RangeRead, metadata: object_storage.ObjectMetadata, body_len: usize) !bool {
    return switch (metadata.checksum_scope) {
        .object => read.range.offset == 0 and read.range.len == read.object.byte_len,
        .response_body => {
            if (metadata.content_length != body_len) return error.InvalidLakeRangeRead;
            return true;
        },
    };
}

fn hexNibble(value: u8) u8 {
    return if (value < 10) '0' + value else 'a' + (value - 10);
}

fn isRetryableObjectReadError(err: anyerror) bool {
    return switch (err) {
        error.RemoteUnavailable,
        error.RateLimited,
        error.UnexpectedHttpStatus,
        error.ConnectionResetByPeer,
        error.ConnectionRefused,
        error.NetworkUnreachable,
        error.TemporaryNameServerFailure,
        error.BrokenPipe,
        error.Timeout,
        error.WouldBlock,
        => true,
        else => false,
    };
}

test "object storage range reader delegates lake parquet range reads" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();

    var client = memory.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "events/part-a.parquet", "0123456789abcdef", .{});
    defer put.deinit(alloc);

    var range_reader = ObjectStorageRangeReader.init(client);
    const parquet_reader = range_reader.parquetReader();
    const bytes = try parquet_reader.readAlloc(alloc, "bucket", "events/part-a.parquet", 4, 6);
    defer alloc.free(bytes);
    try std.testing.expectEqualStrings("456789", bytes);
}

test "object storage range reader enforces planned object etag" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();

    var client = memory.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "events/part-a.parquet", "0123456789abcdef", .{});
    defer put.deinit(alloc);

    var range_reader = ObjectStorageRangeReader.init(client);
    const parquet_reader = range_reader.parquetReader();
    const read = lake_range_io.RangeRead{
        .object = .{
            .bucket = "bucket",
            .key = "events/part-a.parquet",
            .byte_len = 16,
            .version = .{ .etag = put.etag.? },
        },
        .range = .{ .offset = 4, .len = 6 },
        .purpose = .parquet_footer,
    };

    const bytes = try parquet_reader.readPlannedAlloc(alloc, read);
    defer alloc.free(bytes);
    try std.testing.expectEqualStrings("456789", bytes);

    try client.deleteObject("bucket", "events/part-a.parquet", .{});
    var overwritten = try client.putObject("bucket", "events/part-a.parquet", "stale-object-body", .{});
    defer overwritten.deinit(alloc);
    try std.testing.expectError(error.PreconditionFailed, parquet_reader.readPlannedAlloc(alloc, read));
}

test "object storage range reader validates returned planned object metadata" {
    const alloc = std.testing.allocator;
    const MismatchedObjectStorage = struct {
        etag: ?[]const u8 = null,
        version_id: ?[]const u8 = null,

        fn client(self: *@This()) object_storage.ObjectStorage {
            return .{
                .allocator = alloc,
                .ptr = self,
                .vtable = &.{
                    .deinit = deinit,
                    .bucket_exists = bucketExists,
                    .make_bucket = makeBucket,
                    .put_object = putObject,
                    .get_object = getObject,
                    .get_object_attributes = getObjectAttributes,
                    .stat_object = statObject,
                    .delete_object = deleteObject,
                    .list_objects = listObjects,
                },
            };
        }

        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn bucketExists(_: *anyopaque, _: []const u8) !bool {
            return true;
        }
        fn makeBucket(_: *anyopaque, _: []const u8) !void {}
        fn putObject(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: object_storage.PutOptions) !object_storage.PutResult {
            return error.UnsupportedOperation;
        }
        fn getObject(ptr: *anyopaque, a: Allocator, bucket: []const u8, key: []const u8, opts: object_storage.GetOptions) !object_storage.GetResult {
            _ = opts;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .body = try a.dupe(u8, "456789"),
                .metadata = .{
                    .bucket = try a.dupe(u8, bucket),
                    .key = try a.dupe(u8, key),
                    .etag = if (self.etag) |value| try a.dupe(u8, value) else null,
                    .version_id = if (self.version_id) |value| try a.dupe(u8, value) else null,
                    .content_length = 6,
                },
            };
        }
        fn getObjectAttributes(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !object_storage.ObjectAttributes {
            return error.UnsupportedOperation;
        }
        fn statObject(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !object_storage.ObjectMetadata {
            return error.UnsupportedOperation;
        }
        fn deleteObject(_: *anyopaque, _: []const u8, _: []const u8, _: object_storage.DeleteOptions) !void {
            return error.UnsupportedOperation;
        }
        fn listObjects(_: *anyopaque, a: Allocator, _: []const u8, _: object_storage.ListOptions) !object_storage.ListResult {
            return .{
                .entries = try a.alloc(object_storage.ListEntry, 0),
                .common_prefixes = try a.alloc([]u8, 0),
            };
        }
    };

    const read = lake_range_io.RangeRead{
        .object = .{
            .bucket = "bucket",
            .key = "events/part-a.parquet",
            .byte_len = 16,
            .version = .{ .etag = "etag-a", .version_id = "v1" },
        },
        .range = .{ .offset = 4, .len = 6 },
        .purpose = .parquet_footer,
    };

    var missing_etag = MismatchedObjectStorage{ .version_id = "v1" };
    var missing_etag_reader = ObjectStorageRangeReader.init(missing_etag.client());
    try std.testing.expectError(error.PreconditionFailed, missing_etag_reader.parquetReader().readPlannedAlloc(alloc, read));

    var wrong_etag = MismatchedObjectStorage{ .etag = "etag-b", .version_id = "v1" };
    var wrong_etag_reader = ObjectStorageRangeReader.init(wrong_etag.client());
    try std.testing.expectError(error.PreconditionFailed, wrong_etag_reader.parquetReader().readPlannedAlloc(alloc, read));

    var missing_version = MismatchedObjectStorage{ .etag = "etag-a" };
    var missing_version_reader = ObjectStorageRangeReader.init(missing_version.client());
    try std.testing.expectError(error.PreconditionFailed, missing_version_reader.parquetReader().readPlannedAlloc(alloc, read));

    var wrong_version = MismatchedObjectStorage{ .etag = "etag-a", .version_id = "v2" };
    var wrong_version_reader = ObjectStorageRangeReader.init(wrong_version.client());
    try std.testing.expectError(error.PreconditionFailed, wrong_version_reader.parquetReader().readPlannedAlloc(alloc, read));

    var matching = MismatchedObjectStorage{ .etag = "etag-a", .version_id = "v1" };
    var matching_reader = ObjectStorageRangeReader.init(matching.client());
    const bytes = try matching_reader.parquetReader().readPlannedAlloc(alloc, read);
    defer alloc.free(bytes);
    try std.testing.expectEqualStrings("456789", bytes);
}

test "lake object storage range reader validates full object checksums" {
    const alloc = std.testing.allocator;
    const ChecksumObjectStorage = struct {
        const ContentLengthScope = enum { object, body };

        checksum: ?struct {
            algorithm: object_storage.ObjectChecksumAlgorithm,
            value: []const u8,
            checksum_type: object_storage.ObjectChecksumType = .full_object,
        } = null,
        checksum_scope: object_storage.ObjectChecksumScope = .object,
        content_length_scope: ContentLengthScope = .object,

        fn client(self: *@This()) object_storage.ObjectStorage {
            return .{
                .allocator = alloc,
                .ptr = self,
                .vtable = &.{
                    .deinit = deinit,
                    .bucket_exists = bucketExists,
                    .make_bucket = makeBucket,
                    .put_object = putObject,
                    .get_object = getObject,
                    .get_object_attributes = getObjectAttributes,
                    .stat_object = statObject,
                    .delete_object = deleteObject,
                    .list_objects = listObjects,
                },
            };
        }

        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn bucketExists(_: *anyopaque, _: []const u8) !bool {
            return true;
        }
        fn makeBucket(_: *anyopaque, _: []const u8) !void {}
        fn putObject(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: object_storage.PutOptions) !object_storage.PutResult {
            return error.UnsupportedOperation;
        }
        fn getObject(ptr: *anyopaque, a: Allocator, bucket: []const u8, key: []const u8, opts: object_storage.GetOptions) !object_storage.GetResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const full_body = "0123456789";
            const body = if (opts.range) |range| blk: {
                const start: usize = @intCast(range.offset);
                const len: usize = @intCast(range.length orelse return error.InvalidRange);
                if (start > full_body.len or len > full_body.len - start) return error.InvalidRange;
                break :blk full_body[start..][0..len];
            } else full_body;
            return .{
                .body = try a.dupe(u8, body),
                .metadata = .{
                    .bucket = try a.dupe(u8, bucket),
                    .key = try a.dupe(u8, key),
                    .etag = try a.dupe(u8, "etag-a"),
                    .checksum = if (self.checksum) |checksum| .{
                        .algorithm = checksum.algorithm,
                        .value = try a.dupe(u8, checksum.value),
                        .checksum_type = checksum.checksum_type,
                    } else null,
                    .checksum_scope = self.checksum_scope,
                    .content_length = if (self.content_length_scope == .body) @intCast(body.len) else full_body.len,
                },
            };
        }
        fn getObjectAttributes(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !object_storage.ObjectAttributes {
            return error.UnsupportedOperation;
        }
        fn statObject(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !object_storage.ObjectMetadata {
            return error.UnsupportedOperation;
        }
        fn deleteObject(_: *anyopaque, _: []const u8, _: []const u8, _: object_storage.DeleteOptions) !void {
            return error.UnsupportedOperation;
        }
        fn listObjects(_: *anyopaque, a: Allocator, _: []const u8, _: object_storage.ListOptions) !object_storage.ListResult {
            return .{
                .entries = try a.alloc(object_storage.ListEntry, 0),
                .common_prefixes = try a.alloc([]u8, 0),
            };
        }
    };

    const full_read = lake_range_io.RangeRead{
        .object = .{
            .bucket = "bucket",
            .key = "events/part-a.parquet",
            .byte_len = 10,
            .version = .{ .etag = "etag-a" },
        },
        .range = .{ .offset = 0, .len = 10 },
        .purpose = .iceberg_metadata,
    };
    const partial_read = lake_range_io.RangeRead{
        .object = full_read.object,
        .range = .{ .offset = 2, .len = 4 },
        .purpose = .parquet_footer,
    };

    var matching = ChecksumObjectStorage{ .checksum = .{
        .algorithm = .sha256_hex,
        .value = "84d89877f0d4041efb6bf91a16f0248f2fd573e6af05c19f96bedb9f882f7882",
    } };
    var matching_reader = ObjectStorageRangeReader.init(matching.client());
    const bytes = try matching_reader.parquetReader().readPlannedAlloc(alloc, full_read);
    defer alloc.free(bytes);
    try std.testing.expectEqualStrings("0123456789", bytes);

    var mismatched = ChecksumObjectStorage{ .checksum = .{
        .algorithm = .sha256_hex,
        .value = "0000000000000000000000000000000000000000000000000000000000000000",
    } };
    var mismatched_reader = ObjectStorageRangeReader.init(mismatched.client());
    try std.testing.expectError(error.PreconditionFailed, mismatched_reader.parquetReader().readPlannedAlloc(alloc, full_read));

    var crc32c_digest: [4]u8 = undefined;
    std.mem.writeInt(u32, &crc32c_digest, std.hash.crc.@"CRC-32/ISCSI".hash("0123456789"), .big);
    var crc32c_encoded: [std.base64.standard.Encoder.calcSize(crc32c_digest.len)]u8 = undefined;
    _ = std.base64.standard.Encoder.encode(&crc32c_encoded, &crc32c_digest);
    var gcs_crc32c = ChecksumObjectStorage{ .checksum = .{
        .algorithm = .crc32c_base64,
        .value = &crc32c_encoded,
    } };
    var gcs_crc32c_reader = ObjectStorageRangeReader.init(gcs_crc32c.client());
    const gcs_bytes = try gcs_crc32c_reader.parquetReader().readPlannedAlloc(alloc, full_read);
    defer alloc.free(gcs_bytes);
    try std.testing.expectEqualStrings("0123456789", gcs_bytes);

    var composite = ChecksumObjectStorage{ .checksum = .{
        .algorithm = .sha256_base64,
        .value = "not-a-full-object-digest",
        .checksum_type = .composite,
    } };
    var composite_reader = ObjectStorageRangeReader.init(composite.client());
    const composite_bytes = try composite_reader.parquetReader().readPlannedAlloc(alloc, full_read);
    defer alloc.free(composite_bytes);
    try std.testing.expectEqualStrings("0123456789", composite_bytes);

    var partial_reader = ObjectStorageRangeReader.init(mismatched.client());
    const partial_bytes = try partial_reader.parquetReader().readPlannedAlloc(alloc, partial_read);
    defer alloc.free(partial_bytes);
    try std.testing.expectEqualStrings("2345", partial_bytes);

    var scoped = ChecksumObjectStorage{
        .checksum = .{
            .algorithm = .sha256_hex,
            .value = "38083c7ee9121e17401883566a148aa5c2e2d55dc53bc4a94a026517dbff3c6b",
        },
        .checksum_scope = .response_body,
        .content_length_scope = .body,
    };
    var scoped_reader = ObjectStorageRangeReader.init(scoped.client());
    const scoped_bytes = try scoped_reader.parquetReader().readPlannedAlloc(alloc, partial_read);
    defer alloc.free(scoped_bytes);
    try std.testing.expectEqualStrings("2345", scoped_bytes);

    var mismatched_scoped = ChecksumObjectStorage{
        .checksum = .{
            .algorithm = .sha256_hex,
            .value = "0000000000000000000000000000000000000000000000000000000000000000",
        },
        .checksum_scope = .response_body,
        .content_length_scope = .body,
    };
    var mismatched_scoped_reader = ObjectStorageRangeReader.init(mismatched_scoped.client());
    try std.testing.expectError(error.PreconditionFailed, mismatched_scoped_reader.parquetReader().readPlannedAlloc(alloc, partial_read));
}

test "object storage range reader retries transient planned reads only" {
    const alloc = std.testing.allocator;
    const FlakyObjectStorage = struct {
        body: []const u8,
        fail_count: usize,
        get_attempts: usize = 0,
        last_if_match: ?[]const u8 = null,

        fn client(self: *@This()) object_storage.ObjectStorage {
            return .{
                .allocator = alloc,
                .ptr = self,
                .vtable = &.{
                    .deinit = deinit,
                    .bucket_exists = bucketExists,
                    .make_bucket = makeBucket,
                    .put_object = putObject,
                    .get_object = getObject,
                    .get_object_attributes = getObjectAttributes,
                    .stat_object = statObject,
                    .delete_object = deleteObject,
                    .list_objects = listObjects,
                },
            };
        }

        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn bucketExists(_: *anyopaque, _: []const u8) !bool {
            return true;
        }
        fn makeBucket(_: *anyopaque, _: []const u8) !void {}
        fn putObject(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: object_storage.PutOptions) !object_storage.PutResult {
            return error.UnsupportedOperation;
        }
        fn getObject(ptr: *anyopaque, a: Allocator, bucket: []const u8, key: []const u8, opts: object_storage.GetOptions) !object_storage.GetResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.get_attempts += 1;
            self.last_if_match = opts.if_match_etag;
            if (self.get_attempts <= self.fail_count) return error.RemoteUnavailable;
            const range = opts.range orelse return error.InvalidRange;
            const len = range.length orelse return error.InvalidRange;
            const start: usize = std.math.cast(usize, range.offset) orelse return error.InvalidLakeRangeRead;
            const read_len: usize = std.math.cast(usize, len) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or read_len > self.body.len - start) return error.InvalidRange;
            return .{
                .body = try a.dupe(u8, self.body[start..][0..read_len]),
                .metadata = .{
                    .bucket = try a.dupe(u8, bucket),
                    .key = try a.dupe(u8, key),
                    .etag = try a.dupe(u8, "etag-a"),
                    .content_length = read_len,
                },
            };
        }
        fn getObjectAttributes(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !object_storage.ObjectAttributes {
            return error.UnsupportedOperation;
        }
        fn statObject(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !object_storage.ObjectMetadata {
            return error.UnsupportedOperation;
        }
        fn deleteObject(_: *anyopaque, _: []const u8, _: []const u8, _: object_storage.DeleteOptions) !void {
            return error.UnsupportedOperation;
        }
        fn listObjects(_: *anyopaque, a: Allocator, _: []const u8, _: object_storage.ListOptions) !object_storage.ListResult {
            return .{
                .entries = try a.alloc(object_storage.ListEntry, 0),
                .common_prefixes = try a.alloc([]u8, 0),
            };
        }
    };

    var flaky = FlakyObjectStorage{
        .body = "0123456789abcdef",
        .fail_count = 2,
    };
    var range_reader = ObjectStorageRangeReader.initWithRetry(flaky.client(), .{ .max_attempts = 3 });
    const parquet_reader = range_reader.parquetReader();
    const read = lake_range_io.RangeRead{
        .object = .{
            .bucket = "bucket",
            .key = "events/part-a.parquet",
            .byte_len = 16,
            .version = .{ .etag = "etag-a" },
        },
        .range = .{ .offset = 4, .len = 6 },
        .purpose = .parquet_footer,
    };

    const bytes = try parquet_reader.readPlannedAlloc(alloc, read);
    defer alloc.free(bytes);
    try std.testing.expectEqualStrings("456789", bytes);
    try std.testing.expectEqual(@as(usize, 3), flaky.get_attempts);
    try std.testing.expectEqualStrings("etag-a", flaky.last_if_match.?);

    flaky.fail_count = 10;
    flaky.get_attempts = 0;
    try std.testing.expectError(error.RemoteUnavailable, parquet_reader.readPlannedAlloc(alloc, read));
    try std.testing.expectEqual(@as(usize, 3), flaky.get_attempts);
}

test "object storage range reader does not retry stale object identity" {
    const alloc = std.testing.allocator;
    const PreconditionObjectStorage = struct {
        get_attempts: usize = 0,

        fn client(self: *@This()) object_storage.ObjectStorage {
            return .{
                .allocator = alloc,
                .ptr = self,
                .vtable = &.{
                    .deinit = deinit,
                    .bucket_exists = bucketExists,
                    .make_bucket = makeBucket,
                    .put_object = putObject,
                    .get_object = getObject,
                    .get_object_attributes = getObjectAttributes,
                    .stat_object = statObject,
                    .delete_object = deleteObject,
                    .list_objects = listObjects,
                },
            };
        }

        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn bucketExists(_: *anyopaque, _: []const u8) !bool {
            return true;
        }
        fn makeBucket(_: *anyopaque, _: []const u8) !void {}
        fn putObject(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: object_storage.PutOptions) !object_storage.PutResult {
            return error.UnsupportedOperation;
        }
        fn getObject(ptr: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: object_storage.GetOptions) !object_storage.GetResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.get_attempts += 1;
            return error.PreconditionFailed;
        }
        fn getObjectAttributes(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !object_storage.ObjectAttributes {
            return error.UnsupportedOperation;
        }
        fn statObject(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !object_storage.ObjectMetadata {
            return error.UnsupportedOperation;
        }
        fn deleteObject(_: *anyopaque, _: []const u8, _: []const u8, _: object_storage.DeleteOptions) !void {
            return error.UnsupportedOperation;
        }
        fn listObjects(_: *anyopaque, a: Allocator, _: []const u8, _: object_storage.ListOptions) !object_storage.ListResult {
            return .{
                .entries = try a.alloc(object_storage.ListEntry, 0),
                .common_prefixes = try a.alloc([]u8, 0),
            };
        }
    };

    var stale = PreconditionObjectStorage{};
    var range_reader = ObjectStorageRangeReader.initWithRetry(stale.client(), .{ .max_attempts = 3 });
    const parquet_reader = range_reader.parquetReader();
    const read = lake_range_io.RangeRead{
        .object = .{
            .bucket = "bucket",
            .key = "events/part-a.parquet",
            .byte_len = 16,
            .version = .{ .etag = "old-etag" },
        },
        .range = .{ .offset = 4, .len = 6 },
        .purpose = .parquet_footer,
    };
    try std.testing.expectError(error.PreconditionFailed, parquet_reader.readPlannedAlloc(alloc, read));
    try std.testing.expectEqual(@as(usize, 1), stale.get_attempts);
}
