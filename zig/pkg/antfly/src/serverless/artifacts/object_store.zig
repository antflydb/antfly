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

const std = @import("std");
const objectstore = @import("objectstore");
const artifact_store = @import("store.zig");
const remote_uri = @import("../remote_uri.zig");
const object_store_support = @import("../object_store_support.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

pub const ObjectStore = struct {
    alloc: std.mem.Allocator,
    client: objectstore.Client,
    fs_client: ?*objectstore.FilesystemClient = null,
    gcs_client: ?*objectstore.Gcs.JsonApiClient = null,
    s3_client: ?*objectstore.S3.Client = null,
    owns_client: bool = true,
    bucket: []u8,
    prefix: []u8,

    pub fn initRemoteUri(alloc: std.mem.Allocator, uri: []const u8) !ObjectStore {
        return try initRemoteUriWithS3Options(alloc, uri, null);
    }

    pub fn initRemoteUriWithS3Options(
        alloc: std.mem.Allocator,
        uri: []const u8,
        s3_options: ?object_store_support.S3Options,
    ) !ObjectStore {
        var parsed = try remote_uri.parseAlloc(alloc, uri);
        defer switch (parsed) {
            .file => |value| alloc.free(value),
            .gcs => |*value| value.deinit(alloc),
            .s3 => |*value| value.deinit(alloc),
        };

        return switch (parsed) {
            .file => |path| blk: {
                const file_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{path});
                defer alloc.free(file_uri);
                break :blk try initFileUri(alloc, file_uri);
            },
            .gcs => |value| try initGcsUri(alloc, value.bucket, value.prefix),
            .s3 => |value| try initS3UriWithOptions(alloc, value.bucket, value.prefix, s3_options),
        };
    }

    pub fn initFileUri(alloc: std.mem.Allocator, uri: []const u8) !ObjectStore {
        const path = try remote_uri.filePathFromUriAlloc(alloc, uri);
        defer alloc.free(path);
        const fs = try alloc.create(objectstore.FilesystemClient);
        errdefer alloc.destroy(fs);
        fs.* = try objectstore.FilesystemClient.init(alloc, path);

        var owned_client = fs.client();
        var client_initialized = true;
        errdefer if (client_initialized) owned_client.deinit();
        if (!(try owned_client.bucketExists("serverless-artifacts"))) try owned_client.makeBucket("serverless-artifacts");
        const owned_bucket = try alloc.dupe(u8, "serverless-artifacts");
        errdefer alloc.free(owned_bucket);
        const owned_prefix = try alloc.dupe(u8, "");
        errdefer alloc.free(owned_prefix);
        client_initialized = false;
        return .{
            .alloc = alloc,
            .client = owned_client,
            .fs_client = fs,
            .bucket = owned_bucket,
            .prefix = owned_prefix,
        };
    }

    pub fn initGcsUri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !ObjectStore {
        const gcs = try alloc.create(objectstore.Gcs.JsonApiClient);
        errdefer alloc.destroy(gcs);
        var cfg = try objectstore.Gcs.jsonApiClientConfigFromEnvAlloc(alloc);
        var config_owned = true;
        errdefer if (config_owned) cfg.deinit(alloc);
        gcs.* = try objectstore.Gcs.JsonApiClient.init(alloc, cfg);
        config_owned = false;

        var owned_client = gcs.client();
        var client_initialized = true;
        errdefer if (client_initialized) owned_client.deinit();
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        const owned_bucket = try alloc.dupe(u8, bucket);
        errdefer alloc.free(owned_bucket);
        const owned_prefix = try alloc.dupe(u8, prefix);
        errdefer alloc.free(owned_prefix);
        client_initialized = false;
        return .{
            .alloc = alloc,
            .client = owned_client,
            .gcs_client = gcs,
            .bucket = owned_bucket,
            .prefix = owned_prefix,
        };
    }

    pub fn initS3Uri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !ObjectStore {
        return try initS3UriWithOptions(alloc, bucket, prefix, null);
    }

    pub fn initS3UriWithOptions(
        alloc: std.mem.Allocator,
        bucket: []const u8,
        prefix: []const u8,
        options: ?object_store_support.S3Options,
    ) !ObjectStore {
        const s3 = try alloc.create(objectstore.S3.Client);
        errdefer alloc.destroy(s3);
        var cfg = try object_store_support.s3ConfigAlloc(alloc, options);
        var config_owned = true;
        errdefer if (config_owned) cfg.deinit(alloc);
        s3.* = try objectstore.S3.Client.init(alloc, cfg);
        config_owned = false;

        var owned_client = s3.client();
        var client_initialized = true;
        errdefer if (client_initialized) owned_client.deinit();
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        const owned_bucket = try alloc.dupe(u8, bucket);
        errdefer alloc.free(owned_bucket);
        const owned_prefix = try alloc.dupe(u8, prefix);
        errdefer alloc.free(owned_prefix);
        client_initialized = false;
        return .{
            .alloc = alloc,
            .client = owned_client,
            .s3_client = s3,
            .bucket = owned_bucket,
            .prefix = owned_prefix,
        };
    }

    pub fn initWithClient(alloc: std.mem.Allocator, client: objectstore.Client, bucket: []const u8, prefix: []const u8) !ObjectStore {
        var owned_client = client;
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        const owned_bucket = try alloc.dupe(u8, bucket);
        errdefer alloc.free(owned_bucket);
        const owned_prefix = try alloc.dupe(u8, prefix);
        errdefer alloc.free(owned_prefix);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .owns_client = false,
            .bucket = owned_bucket,
            .prefix = owned_prefix,
        };
    }

    pub fn deinit(self: *ObjectStore) void {
        if (self.owns_client) self.client.deinit();
        if (self.fs_client) |fs| self.alloc.destroy(fs);
        if (self.gcs_client) |gcs| self.alloc.destroy(gcs);
        if (self.s3_client) |s3| self.alloc.destroy(s3);
        self.alloc.free(self.bucket);
        self.alloc.free(self.prefix);
        self.* = undefined;
    }

    pub fn artifactStore(self: *ObjectStore) artifact_store.ArtifactStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn put(self: *ObjectStore, alloc: std.mem.Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        const checksum = try sha256StringAlloc(alloc, contents);
        errdefer alloc.free(checksum);
        const artifact_id = try makeArtifactIdAlloc(alloc, checksum);
        errdefer alloc.free(artifact_id);
        const key = try keyForChecksumAlloc(self.alloc, self.prefix, checksum);
        defer self.alloc.free(key);

        var result = try self.client.putObject(self.bucket, key, contents, .{
            .content_type = "application/octet-stream",
        });
        defer result.deinit(self.client.allocator);

        return .{
            .artifact_id = artifact_id,
            .byte_len = @intCast(contents.len),
            .checksum = checksum,
        };
    }

    pub fn getAlloc(self: *ObjectStore, alloc: std.mem.Allocator, artifact_id: []const u8) ![]u8 {
        return try self.getAllocWithCancellation(alloc, artifact_id, .none);
    }

    pub fn getAllocWithCancellation(
        self: *ObjectStore,
        alloc: std.mem.Allocator,
        artifact_id: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        const checksum = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        const key = try keyForChecksumAlloc(self.alloc, self.prefix, checksum);
        defer self.alloc.free(key);
        var result = self.client.getObject(self.bucket, key, .{
            .cancellation = objectstore.CancellationToken.fromCallback(cancellation.ptr, cancellation.is_cancelled_fn),
        }) catch |err| return normalizeCancellationError(err);
        defer result.deinit(self.client.allocator);
        try cancellation.check();
        return try dupeWithCancellationAlloc(alloc, result.body, cancellation);
    }

    pub fn getRangeAlloc(self: *ObjectStore, alloc: std.mem.Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        return try self.getRangeAllocWithCancellation(alloc, artifact_id, offset, len, .none);
    }

    pub fn getRangeAllocWithCancellation(
        self: *ObjectStore,
        alloc: std.mem.Allocator,
        artifact_id: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        const checksum = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        const key = try keyForChecksumAlloc(self.alloc, self.prefix, checksum);
        defer self.alloc.free(key);
        var result = self.client.getObject(self.bucket, key, .{
            .range = .{ .offset = offset, .length = len },
            .skip_metadata_probe = true,
            .max_response_bytes = len,
            .cancellation = objectstore.CancellationToken.fromCallback(cancellation.ptr, cancellation.is_cancelled_fn),
        }) catch |err| return normalizeCancellationError(err);
        defer result.deinit(self.client.allocator);
        try cancellation.check();
        return try dupeWithCancellationAlloc(alloc, result.body, cancellation);
    }

    pub fn stat(self: *ObjectStore, alloc: std.mem.Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        return try self.statWithCancellation(alloc, artifact_id, .none);
    }

    pub fn statWithCancellation(
        self: *ObjectStore,
        alloc: std.mem.Allocator,
        artifact_id: []const u8,
        cancellation: CancellationToken,
    ) !artifact_store.ArtifactMetadata {
        try cancellation.check();
        const checksum_value = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        const checksum = try alloc.dupe(u8, checksum_value);
        errdefer alloc.free(checksum);
        const key = try keyForChecksumAlloc(self.alloc, self.prefix, checksum);
        defer self.alloc.free(key);
        var meta = self.client.statObjectWithOptions(self.bucket, key, .{
            .cancellation = objectstore.CancellationToken.fromCallback(cancellation.ptr, cancellation.is_cancelled_fn),
        }) catch |err| return normalizeCancellationError(err);
        defer meta.deinit(self.client.allocator);
        try cancellation.check();
        return .{
            .artifact_id = try alloc.dupe(u8, artifact_id),
            .byte_len = meta.content_length,
            .checksum = checksum,
        };
    }

    pub fn verifyContent(
        self: *ObjectStore,
        _: std.mem.Allocator,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        cancellation: CancellationToken,
    ) !void {
        const checksum = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        if (!std.mem.eql(u8, checksum, expected_checksum)) return error.ArtifactIntegrityMismatch;
        const key = try keyForChecksumAlloc(self.alloc, self.prefix, checksum);
        defer self.alloc.free(key);
        var meta = self.client.statObjectWithOptions(self.bucket, key, .{
            .cancellation = objectstore.CancellationToken.fromCallback(cancellation.ptr, cancellation.is_cancelled_fn),
        }) catch |err| return normalizeCancellationError(err);
        defer meta.deinit(self.client.allocator);
        if (meta.content_length != expected_byte_len) return error.ArtifactIntegrityMismatch;
        if (meta.checksum_scope == .object) {
            if (meta.checksum) |native| {
                if (native.checksum_type == .full_object) switch (native.algorithm) {
                    .sha256_hex => {
                        if (!std.ascii.eqlIgnoreCase(native.value, expected_checksum)) return error.ArtifactIntegrityMismatch;
                        return;
                    },
                    .sha256_base64 => {
                        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
                        _ = std.fmt.hexToBytes(&digest, expected_checksum) catch return error.ArtifactIntegrityMismatch;
                        var encoded: [std.base64.standard.Encoder.calcSize(digest.len)]u8 = undefined;
                        const value = std.base64.standard.Encoder.encode(&encoded, &digest);
                        if (!std.mem.eql(u8, native.value, value)) return error.ArtifactIntegrityMismatch;
                        return;
                    },
                    else => {},
                };
            }
        }

        // Providers without a comparable SHA-256 metadata checksum are read in
        // bounded ranges pinned to the provider identity when one is exposed.
        // The final digest is authoritative and memory remains O(1).
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        const chunk_bytes: u64 = 8 * 1024 * 1024;
        var offset: u64 = 0;
        while (offset < expected_byte_len) {
            try cancellation.check();
            const len = @min(chunk_bytes, expected_byte_len - offset);
            var part = self.client.getObject(self.bucket, key, .{
                .range = .{ .offset = offset, .length = len },
                .version_id = meta.version_id,
                .if_match_etag = meta.etag,
                .skip_metadata_probe = true,
                .max_response_bytes = @intCast(len),
                .cancellation = objectstore.CancellationToken.fromCallback(cancellation.ptr, cancellation.is_cancelled_fn),
            }) catch |err| return normalizeCancellationError(err);
            defer part.deinit(self.client.allocator);
            if (part.body.len != len) return error.ArtifactIntegrityMismatch;
            hasher.update(part.body);
            offset += len;
        }
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        hasher.final(&digest);
        const actual = std.fmt.bytesToHex(digest, .lower);
        if (!std.mem.eql(u8, &actual, expected_checksum)) return error.ArtifactIntegrityMismatch;
        try cancellation.check();
    }

    pub fn delete(self: *ObjectStore, artifact_id: []const u8) !void {
        const checksum = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        const key = try keyForChecksumAlloc(self.alloc, self.prefix, checksum);
        defer self.alloc.free(key);
        try self.client.deleteObject(self.bucket, key, .{});
    }

    const vtable: artifact_store.ArtifactStore.VTable = .{
        .deinit = erasedDeinit,
        .put = erasedPut,
        .get_alloc = erasedGetAlloc,
        .get_alloc_with_cancellation = erasedGetAllocWithCancellation,
        .get_range_alloc = erasedGetRangeAlloc,
        .get_range_alloc_with_cancellation = erasedGetRangeAllocWithCancellation,
        .stat = erasedStat,
        .stat_with_cancellation = erasedStatWithCancellation,
        .verify_content = erasedVerifyContent,
        .delete = erasedDelete,
    };

    fn erasedDeinit(_: std.mem.Allocator, ptr: *anyopaque) void {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedPut(ptr: *anyopaque, alloc: std.mem.Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.put(alloc, contents);
    }

    fn erasedGetAlloc(ptr: *anyopaque, alloc: std.mem.Allocator, artifact_id: []const u8) ![]u8 {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.getAlloc(alloc, artifact_id);
    }

    fn erasedGetAllocWithCancellation(ptr: *anyopaque, alloc: std.mem.Allocator, artifact_id: []const u8, cancellation: CancellationToken) ![]u8 {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.getAllocWithCancellation(alloc, artifact_id, cancellation);
    }

    fn erasedGetRangeAlloc(ptr: *anyopaque, alloc: std.mem.Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.getRangeAlloc(alloc, artifact_id, offset, len);
    }

    fn erasedGetRangeAllocWithCancellation(ptr: *anyopaque, alloc: std.mem.Allocator, artifact_id: []const u8, offset: u64, len: usize, cancellation: CancellationToken) ![]u8 {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.getRangeAllocWithCancellation(alloc, artifact_id, offset, len, cancellation);
    }

    fn erasedStat(ptr: *anyopaque, alloc: std.mem.Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.stat(alloc, artifact_id);
    }

    fn erasedStatWithCancellation(ptr: *anyopaque, alloc: std.mem.Allocator, artifact_id: []const u8, cancellation: CancellationToken) !artifact_store.ArtifactMetadata {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.statWithCancellation(alloc, artifact_id, cancellation);
    }

    fn erasedVerifyContent(ptr: *anyopaque, alloc: std.mem.Allocator, artifact_id: []const u8, expected_byte_len: u64, expected_checksum: []const u8, cancellation: CancellationToken) !void {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        try self.verifyContent(alloc, artifact_id, expected_byte_len, expected_checksum, cancellation);
    }

    fn erasedDelete(ptr: *anyopaque, artifact_id: []const u8) !void {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        try self.delete(artifact_id);
    }
};

fn normalizeCancellationError(err: anyerror) anyerror {
    return if (err == error.Cancelled) error.Canceled else err;
}

fn dupeWithCancellationAlloc(
    alloc: std.mem.Allocator,
    source: []const u8,
    cancellation: CancellationToken,
) ![]u8 {
    const out = try alloc.alloc(u8, source.len);
    errdefer alloc.free(out);
    const cancellation_chunk_bytes = 1024 * 1024;
    var copied: usize = 0;
    while (copied < source.len) {
        try cancellation.check();
        const chunk_len = @min(cancellation_chunk_bytes, source.len - copied);
        @memcpy(out[copied..][0..chunk_len], source[copied..][0..chunk_len]);
        copied += chunk_len;
    }
    try cancellation.check();
    return out;
}

fn sha256StringAlloc(alloc: std.mem.Allocator, contents: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(contents, &digest, .{});

    const out = try alloc.alloc(u8, 64);
    for (digest, 0..) |byte, idx| {
        out[idx * 2] = hexNibble(byte >> 4);
        out[idx * 2 + 1] = hexNibble(byte & 0x0f);
    }
    return out;
}

fn makeArtifactIdAlloc(alloc: std.mem.Allocator, checksum: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "sha256:{s}", .{checksum});
}

fn keyForChecksumAlloc(alloc: std.mem.Allocator, prefix: []const u8, checksum: []const u8) ![]u8 {
    try artifact_store.validateSha256Checksum(checksum);
    if (prefix.len == 0) return try std.fmt.allocPrint(alloc, "sha256/{s}/{s}", .{ checksum[0..2], checksum[2..] });
    return try std.fmt.allocPrint(alloc, "{s}/sha256/{s}/{s}", .{ prefix, checksum[0..2], checksum[2..] });
}

fn hexNibble(v: u8) u8 {
    return if (v < 10) '0' + v else 'a' + (v - 10);
}

test "objectstore-backed artifacts store round-trips over file uri" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "artifacts");
    defer cleanupTmp(path);

    const uri = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{std.mem.span(path)});
    defer std.testing.allocator.free(uri);

    var store_impl = try ObjectStore.initFileUri(std.testing.allocator, uri);
    var store = store_impl.artifactStore();
    defer store.deinit();

    var meta = try store.put("payload");
    defer meta.deinit(std.testing.allocator);
    const got = try store.getAlloc(meta.artifact_id);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings("payload", got);
}

test "objectstore-backed artifacts reject malformed content addresses before I/O" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var impl = try ObjectStore.initWithClient(alloc, memory.client(), "bucket", "tenant/a");
    var store = impl.artifactStore();
    defer store.deinit();

    try std.testing.expectError(error.InvalidArtifactId, store.getAlloc("sha256:abcd"));
}

test "objectstore-backed artifacts store opens gs uri through parser with injected client" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var impl = try ObjectStore.initWithClient(alloc, memory.client(), "gcs-bucket", "tenant/a");
    var store = impl.artifactStore();
    defer store.deinit();

    var meta = try store.put("payload");
    defer meta.deinit(alloc);
    const got = try store.getAlloc(meta.artifact_id);
    defer alloc.free(got);
    try std.testing.expectEqualStrings("payload", got);
}

test "serverless objectstore-backed artifacts preserve distinct client and result allocators" {
    const result_alloc = std.testing.allocator;
    var client_buffer: [256 * 1024]u8 = undefined;
    var client_fba = std.heap.FixedBufferAllocator.init(&client_buffer);
    const client_alloc = client_fba.allocator();
    var memory = objectstore.MemoryClient.init(client_alloc);
    defer memory.deinit();
    var impl = try ObjectStore.initWithClient(result_alloc, memory.client(), "bucket", "tenant/a");
    var store = impl.artifactStore();
    defer store.deinit();

    var meta = try store.put("allocator-safe");
    defer meta.deinit(result_alloc);
    const got = try store.getAlloc(meta.artifact_id);
    defer result_alloc.free(got);
    try std.testing.expectEqualStrings("allocator-safe", got);
    const range = try store.getRangeAlloc(meta.artifact_id, 10, 4);
    defer result_alloc.free(range);
    try std.testing.expectEqualStrings("safe", range);
    var stat = try store.stat(meta.artifact_id);
    defer stat.deinit(result_alloc);
    try std.testing.expectEqual(@as(u64, "allocator-safe".len), stat.byte_len);
    try store.verifyContentWithCancellationUsingAllocator(result_alloc, meta.artifact_id, meta.byte_len, meta.checksum, .none);

    var query_buffer: [1024]u8 = undefined;
    var query_fba = std.heap.FixedBufferAllocator.init(&query_buffer);
    const query_alloc = query_fba.allocator();
    const verified = try store.getVerifiedAllocWithCancellationUsingAllocator(
        query_alloc,
        meta.artifact_id,
        meta.byte_len,
        meta.checksum,
        .none,
    );
    try std.testing.expect(query_fba.ownsSlice(verified));
    query_alloc.free(verified);
    try std.testing.expectEqual(@as(usize, 0), query_fba.end_index);

    const key = try keyForChecksumAlloc(result_alloc, "tenant/a", meta.checksum);
    defer result_alloc.free(key);
    var corrupt = try result_alloc.dupe(u8, "allocator-safe");
    defer result_alloc.free(corrupt);
    corrupt[0] ^= 1;
    var client = memory.client();
    var replaced = try client.putObject("bucket", key, corrupt, .{});
    defer replaced.deinit(client_alloc);
    try std.testing.expectError(
        error.ArtifactIntegrityMismatch,
        store.verifyContentWithCancellationUsingAllocator(result_alloc, meta.artifact_id, meta.byte_len, meta.checksum, .none),
    );
}

test "serverless objectstore-backed artifact initialization cleans up every allocation failure" {
    var file_root_buf: [256]u8 = undefined;
    const file_root = tmpPath(&file_root_buf, "owned-init-oom");
    defer cleanupTmp(file_root);
    var uri_buf: [320]u8 = undefined;
    const file_uri = try std.fmt.bufPrint(&uri_buf, "file://{s}", .{std.mem.span(file_root)});
    var seeded_store = try ObjectStore.initFileUri(std.testing.allocator, file_uri);
    seeded_store.deinit();

    const Runner = struct {
        fn borrowed(alloc: std.mem.Allocator) !void {
            var memory = objectstore.MemoryClient.init(alloc);
            defer memory.deinit();
            var impl = try ObjectStore.initWithClient(alloc, memory.client(), "bucket", "tenant/a");
            defer impl.deinit();
        }

        fn owned(alloc: std.mem.Allocator, uri: []const u8) !void {
            var impl = try ObjectStore.initFileUri(alloc, uri);
            defer impl.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.borrowed, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.owned, .{file_uri});
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-object-artifacts-{s}-{d}-{d}\x00", .{ label, nowNs(), nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
