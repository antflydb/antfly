// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Bounded transport cohorts for immutable text opening artifacts. Every member
//! retains its own SHA-256 identity; the cohort is only a cache population hint.
const std = @import("std");
const local = @import("antfly_local_sources");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const magic = "AFOP1";
pub const max_decoded_bytes = 4 * 1024 * 1024;
pub const Pack = struct {
    artifact: artifacts.ChunkRef,
    decoded_bytes: usize,
    pub fn validate(self: Pack, domain: [32]u8) !void {
        if (self.decoded_bytes < magic.len or self.decoded_bytes > max_decoded_bytes or self.artifact.byte_len == 0 or self.artifact.byte_len > max_decoded_bytes + max_decoded_bytes / 4) return error.InvalidNativeLakeTextOpeningPack;
        try stores.validateSha256ArtifactIdentity(self.artifact.artifact_id, self.artifact.checksum);
        const scope = (try stores.uploadScopeFromArtifactId(self.artifact.artifact_id)) orelse return error.InvalidNativeLakeTextOpeningPack;
        if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeTextOpeningPack;
    }
};
pub const Collector = struct {
    a: A,
    out: A,
    store: *stores.ArtifactStore,
    cancellation: Cancellation,
    bytes: std.ArrayList(u8) = .empty,
    refs: std.ArrayList(Pack) = .empty,
    members: usize = 0,
    pub fn deinit(self: *Collector) void {
        self.bytes.deinit(self.a);
    }
    pub fn add(self: *Collector, ref: artifacts.ChunkRef, bytes: []const u8) !void {
        try self.cancellation.check();
        if (ref.artifact_id.len > 4096 or ref.byte_len != bytes.len) return error.InvalidNativeLakeTextOpeningPack;
        if (bytes.len > 1024 * 1024) return; // Optional hint, never a build-size restriction.
        const size = 38 + ref.artifact_id.len + bytes.len;
        if (self.members == 256 or self.bytes.items.len + size > max_decoded_bytes) try self.flush();
        if (self.bytes.items.len == 0) try self.bytes.appendSlice(self.a, magic);
        var header: [38]u8 = undefined;
        std.mem.writeInt(u16, header[0..2], @intCast(ref.artifact_id.len), .little);
        std.mem.writeInt(u32, header[2..6], @intCast(bytes.len), .little);
        header[6..38].* = try stores.sha256DigestFromChecksum(ref.checksum);
        try self.bytes.appendSlice(self.a, &header);
        try self.bytes.appendSlice(self.a, ref.artifact_id);
        try self.bytes.appendSlice(self.a, bytes);
        self.members += 1;
    }
    pub fn flush(self: *Collector) !void {
        if (self.members == 0) return;
        try self.cancellation.check();
        const encoded = try local.encoding_snappy.encode(self.a, self.bytes.items);
        defer self.a.free(encoded);
        var store = self.store.*;
        store.allocator = self.out;
        const ref = try store.putWithCancellation(encoded, self.cancellation);
        defer if (ref.provider_version) |version| store.allocator.free(version);
        try self.refs.append(self.out, .{ .artifact = .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, .decoded_bytes = self.bytes.items.len });
        self.bytes.clearRetainingCapacity();
        self.members = 0;
    }
};

pub fn warm(a: A, store: stores.ArtifactStore, pack: Pack, domain: [32]u8, cached: artifacts.CachedRead, cancellation: Cancellation) !void {
    const Cache = local.serverless_query_lake_serving_cache.Cache;
    try cached.context.ensureActive();
    try cancellation.check();
    try pack.validate(domain);
    const encoded = try artifacts.readArtifact(a, store, pack.artifact, cancellation, cached);
    defer a.free(encoded);
    if (try local.encoding_snappy.decodedLen(encoded) != pack.decoded_bytes) return error.InvalidNativeLakeTextOpeningPack;
    const decoded = try a.alloc(u8, pack.decoded_bytes);
    defer a.free(decoded);
    try local.encoding_snappy.decodeInto(encoded, decoded);
    if (!std.mem.startsWith(u8, decoded, magic)) return error.InvalidNativeLakeTextOpeningPack;
    var position: usize = magic.len;
    var count: usize = 0;
    while (position < decoded.len) {
        try cached.context.ensureActive();
        try cancellation.check();
        if (decoded.len - position < 38 or count == 256) return error.InvalidNativeLakeTextOpeningPack;
        const header = decoded[position..][0..38];
        const id_len = std.mem.readInt(u16, header[0..2], .little);
        const length = std.mem.readInt(u32, header[2..6], .little);
        const digest = header[6..38].*;
        position += 38;
        if (id_len == 0 or id_len > 4096 or length > 1024 * 1024 or @as(u64, id_len) + length > decoded.len - position) return error.InvalidNativeLakeTextOpeningPack;
        const id = decoded[position..][0..id_len];
        position += id_len;
        const checksum = std.fmt.bytesToHex(digest, .lower);
        try stores.validateSha256ArtifactIdentity(id, &checksum);
        const scope = (try stores.uploadScopeFromArtifactId(id)) orelse return error.InvalidNativeLakeTextOpeningPack;
        if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeTextOpeningPack;
        const value = try Cache.VerifiedBytes.authenticate(decoded[position..][0..length], digest);
        position += length;
        count += 1;
        const owned = try a.dupe(u8, value.bytes);
        const lease = try cached.cache.admitVerifiedBlock(a, cached.scope, id, length, digest, cached.context, .{ .value = .{ .bytes = owned, .digest = digest }, .owner = .{ .bytes = owned, .owner = .{ .allocation = a } } });
        var release = lease;
        release.deinit();
    }
    if (count == 0) return error.InvalidNativeLakeTextOpeningPack;
    try cached.context.ensureActive();
    try cancellation.check();
}

test "external lake opening cohorts reject corrupted members and decoded-size substitution" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("opening-pack-integrity");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(31), 1, std.testing.io);
    var output = store;
    output.allocator = ca;
    const bytes = "authenticated opening metadata";
    const stored = try output.putWithCancellation(bytes, .none);
    defer if (stored.provider_version) |version| output.allocator.free(version);
    const ref: artifacts.ChunkRef = .{ .artifact_id = stored.artifact_id, .checksum = stored.checksum, .byte_len = stored.byte_len };
    var collector: Collector = .{ .a = a, .out = ca, .store = &store, .cancellation = .none };
    defer collector.deinit();
    try collector.add(ref, bytes);
    collector.bytes.items[collector.bytes.items.len - 1] ^= 1;
    try collector.flush();
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(4), .context = .{ .io = std.testing.io } };
    try std.testing.expectError(error.ArtifactIntegrityMismatch, warm(a, store, collector.refs.items[0], store.upload_scope.?.domain, cached, .none));
    var substituted = collector.refs.items[0];
    substituted.decoded_bytes = magic.len;
    try std.testing.expectError(error.InvalidNativeLakeTextOpeningPack, warm(a, store, substituted, store.upload_scope.?.domain, cached, .none));
    try std.testing.expectError(error.InvalidNativeLakeTextOpeningPack, warm(a, store, collector.refs.items[0], @splat(32), cached, .none));
    substituted.decoded_bytes = max_decoded_bytes + 1;
    try std.testing.expectError(error.InvalidNativeLakeTextOpeningPack, substituted.validate(store.upload_scope.?.domain));
}
