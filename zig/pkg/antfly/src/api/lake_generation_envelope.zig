// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Opaque retained-cut envelopes bind the exact provider version without
//! changing content identities. Filesystem tokens omit a provider version.
const std = @import("std");
const stores = @import("../serverless/artifacts/store.zig");
const Token = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
pub const Envelope = struct { id: []const u8, bytes: usize, generation: ?[]u8 = null };
pub fn artifactId(prefix: []const u8, token: []const u8) ![]const u8 {
    if (!std.mem.startsWith(u8, token, prefix)) return error.InvalidQueryRequest;
    const end = std.mem.lastIndexOfScalar(u8, token, ':') orelse return error.InvalidQueryRequest;
    const body = token[prefix.len..end];
    return if (std.mem.indexOf(u8, body, ":g:")) |split| body[0..split] else body;
}
pub fn encode(a: A, prefix: []const u8, metadata: stores.ArtifactMetadata) ![]u8 {
    const version = metadata.provider_version orelse return std.fmt.allocPrint(a, "{s}{s}:{d}", .{ prefix, metadata.artifact_id, metadata.byte_len });
    if (version.len == 0 or version.len > @import("antfly_local_sources").serverless_manifest_artifact_ref.max_provider_generation_bytes or std.mem.eql(u8, version, "null")) return error.InvalidQueryRequest;
    const base64 = std.base64.url_safe_no_pad;
    const encoded = try a.alloc(u8, base64.Encoder.calcSize(version.len));
    defer a.free(encoded);
    _ = base64.Encoder.encode(encoded, version);
    return std.fmt.allocPrint(a, "{s}{s}:g:{s}:{d}", .{ prefix, metadata.artifact_id, encoded, metadata.byte_len });
}
pub fn decode(a: A, prefix: []const u8, token: []const u8, limit: usize) !Envelope {
    const id = try artifactId(prefix, token);
    _ = try stores.sha256ChecksumFromArtifactId(id);
    const end = std.mem.lastIndexOfScalar(u8, token, ':').?;
    const length = std.fmt.parseUnsigned(usize, token[end + 1 ..], 10) catch return error.InvalidQueryRequest;
    if (length == 0 or length > limit) return error.InvalidQueryRequest;
    const suffix = token[prefix.len + id.len .. end];
    if (suffix.len == 0) return .{ .id = id, .bytes = length };
    if (!std.mem.startsWith(u8, suffix, ":g:")) return error.InvalidQueryRequest;
    const decoder = std.base64.url_safe_no_pad.Decoder;
    const size = decoder.calcSizeForSlice(suffix[3..]) catch return error.InvalidQueryRequest;
    if (size == 0 or size > @import("antfly_local_sources").serverless_manifest_artifact_ref.max_provider_generation_bytes) return error.InvalidQueryRequest;
    const generation = try a.alloc(u8, size);
    errdefer a.free(generation);
    decoder.decode(generation, suffix[3..]) catch return error.InvalidQueryRequest;
    if (std.mem.eql(u8, generation, "null")) return error.InvalidQueryRequest;
    return .{ .id = id, .bytes = length, .generation = generation };
}
pub fn read(store: *stores.ArtifactStore, envelope: Envelope, token: Token) ![]u8 {
    if (envelope.generation) |generation| {
        const object = store.providerStore() orelse return error.CloudGenerationStoreUnavailable;
        const suffix = try stores.storageSuffixAlloc(store.allocator, envelope.id);
        defer store.allocator.free(suffix);
        const path = try std.fmt.allocPrint(store.allocator, "{s}{s}{s}", .{ object.prefix, if (object.prefix.len == 0) "" else "/", suffix });
        defer store.allocator.free(path);
        var client = object.client;
        client.allocator = store.allocator;
        var result = try client.getObject(object.bucket, path, .{ .version_id = generation, .skip_metadata_probe = true, .max_response_bytes = envelope.bytes, .cancellation = @import("objectstore").CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn) });
        defer result.deinit(store.allocator);
        if (result.body.len != envelope.bytes) return error.ArtifactIntegrityMismatch;
        try stores.validatePayloadSha256WithCancellation(result.body, try stores.sha256ChecksumFromArtifactId(envelope.id), token);
        const body = result.body;
        result.body = &.{};
        return body;
    }
    return store.getVerifiedAllocWithCancellation(envelope.id, envelope.bytes, try stores.sha256ChecksumFromArtifactId(envelope.id), token);
}
/// Durable immutable bytes are scoped to current credentials and the exact
/// provider generation. Native authority and retention stay with the caller.
pub fn readCached(store: *stores.ArtifactStore, envelope: Envelope, token: Token, cached: ?@import("lake_index_aggregate_artifact.zig").CachedRead) ![]u8 {
    const cache = cached orelse return read(store, envelope, token);
    try token.check();
    try cache.context.ensureActive();
    const identity = @import("lake_wal_image.zig").digest(.{ .format = "retained-versioned-bytes-v1", .ref = envelope });
    const key = std.fmt.bytesToHex(identity, .lower);
    const Loader = struct {
        store: stores.ArtifactStore,
        ref: Envelope,
        token: Token,
        fn load(raw: *anyopaque, a: A) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var backend = self.store;
            backend.allocator = a;
            return read(&backend, self.ref, self.token);
        }
    };
    var loader: Loader = .{ .store = store.*, .ref = envelope, .token = token };
    const value = try cache.cache.readImmutableAlloc(store.allocator, cache.scope, &key, envelope.bytes, try stores.sha256DigestFromChecksum(try stores.sha256ChecksumFromArtifactId(envelope.id)), cache.context, .{ .ptr = &loader, .load = Loader.load });
    errdefer store.allocator.free(value);
    try token.check();
    try cache.context.ensureActive();
    return value;
}
test "external lake global frontier retained envelopes preserve exact cloud selectors and filesystem identities" {
    const a = std.testing.allocator;
    const scope: stores.UploadScope = .{ .domain = @splat(1), .attempt = @splat(2) };
    const id = try scope.artifactId(&(@as([64]u8, @splat('a'))));
    const metadata: stores.ArtifactMetadata = .{ .artifact_id = @constCast(&id), .checksum = @constCast(id[7..71]), .byte_len = 42, .provider_version = @constCast("version/+one") };
    const token = try encode(a, "lake2:", metadata);
    defer a.free(token);
    const envelope = try decode(a, "lake2:", token, 42);
    defer a.free(envelope.generation.?);
    try std.testing.expectEqualStrings(&id, envelope.id);
    try std.testing.expectEqualStrings("version/+one", envelope.generation.?);
    try std.testing.expectEqualStrings(&id, try artifactId("lake2:", token));
    var filesystem = metadata;
    filesystem.provider_version = null;
    const unversioned = try encode(a, "lake2:", filesystem);
    defer a.free(unversioned);
    try std.testing.expect((try decode(a, "lake2:", unversioned, 42)).generation == null);
    try std.testing.expectError(error.InvalidQueryRequest, decode(a, "lake2:", token, 41));
}

test "external lake global frontier retained bytes reuse durable cache after restart and fence credentials" {
    const local = @import("antfly_local_sources");
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("retained-byte-cache");
    defer directory.cleanup();
    const cache_path = try std.fs.path.join(a, &.{ directory.path(), "cache" });
    defer a.free(cache_path);
    var backend = try @import("lake_index_store.zig").Store.openNative(a, null, null, false, .standalone, directory.path());
    defer backend.deinit();
    var store = backend.artifactStore();
    const scope = try stores.UploadScope.forPublication(@splat(1), 1, std.testing.io);
    var ref = try store.putScoped(scope, "retained image", .none);
    defer ref.deinit(a);
    const envelope: Envelope = .{ .id = ref.artifact_id, .bytes = @intCast(ref.byte_len) };
    const C = local.serverless_query_lake_serving_cache.Cache;
    for (0..2) |pass| {
        var cache = C.init(a);
        defer cache.deinit();
        try cache.ensurePersistent(std.testing.io, cache_path, .{ .max_entries = 32, .max_total_bytes = 1024 * 1024, .protected_bytes = 0 }, .{});
        const bytes = try readCached(&store, envelope, .none, .{ .cache = &cache, .scope = @splat(2), .context = .{ .io = std.testing.io } });
        defer a.free(bytes);
        try std.testing.expectEqualStrings("retained image", bytes);
        if (pass == 1) {
            try std.testing.expectEqual(@as(u64, 0), cache.snapshot().provider_reads);
            try std.testing.expect(cache.snapshot().disk_hits != 0);
            const alternate = try readCached(&store, envelope, .none, .{ .cache = &cache, .scope = @splat(3), .context = .{ .io = std.testing.io } });
            defer a.free(alternate);
            try std.testing.expectEqual(@as(u64, 1), cache.snapshot().provider_reads);
        }
        cache.persistent.?.flush();
    }
}
