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

//! Bounded transport cohorts for immutable text opening artifacts. Every member
//! retains its own SHA-256 identity; the cohort is only a cache population hint.
const std = @import("std");
const local = @import("antfly_local_sources");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const magic = "AFOP1";
const range_magic = "AFOP2";
pub const max_decoded_bytes: usize = 4 * 1024 * 1024;
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
    /// Version two also seeds authenticated range identities. The caller
    /// bounds the entire publication, independently of per-pack compression.
    version: u8 = 1,
    max_packs: usize = std.math.maxInt(usize),
    seen: std.AutoHashMapUnmanaged([32]u8, void) = .empty,
    pub fn deinit(self: *Collector) void {
        self.bytes.deinit(self.a);
        self.seen.deinit(self.a);
    }
    pub fn add(self: *Collector, ref: artifacts.ChunkRef, bytes: []const u8) !void {
        if (ref.artifact_id.len > 4096 or ref.byte_len != bytes.len) return error.InvalidNativeLakeTextOpeningPack;
        return self.addIdentity(ref.artifact_id, try stores.sha256DigestFromChecksum(ref.checksum), bytes);
    }
    pub fn capture(raw: *anyopaque, identity: []const u8, digest: [32]u8, bytes: []const u8) !void {
        const self: *Collector = @ptrCast(@alignCast(raw));
        return self.addIdentity(identity, digest, bytes);
    }
    pub fn full(self: *const Collector) bool {
        return self.refs.items.len >= self.max_packs;
    }
    fn memberKey(identity: []const u8, digest: [32]u8) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(identity);
        hash.update(&digest);
        return hash.finalResult();
    }
    pub fn containsIdentity(self: *const Collector, identity: []const u8, digest: [32]u8) bool {
        return self.version == 2 and self.seen.contains(memberKey(identity, digest));
    }
    pub fn addIdentity(self: *Collector, identity: []const u8, digest: [32]u8, bytes: []const u8) !void {
        try self.cancellation.check();
        if (self.full()) return;
        if (identity.len == 0 or identity.len > 4096) return error.InvalidNativeLakeTextOpeningPack;
        if (bytes.len > (if (self.version == 1) 1024 * 1024 else max_decoded_bytes) or bytes.len + identity.len + 38 + magic.len > max_decoded_bytes) return;
        if (self.version == 2) {
            const key = memberKey(identity, digest);
            if (self.seen.contains(key)) return;
            try self.seen.put(self.a, key, {});
        }
        const size = 38 + identity.len + bytes.len;
        if (self.members == 256 or self.bytes.items.len + size > max_decoded_bytes) try self.flush();
        if (self.full()) return;
        if (self.bytes.items.len == 0) try self.bytes.appendSlice(self.a, if (self.version == 2) range_magic else magic);
        var header: [38]u8 = undefined;
        std.mem.writeInt(u16, header[0..2], @intCast(identity.len), .little);
        std.mem.writeInt(u32, header[2..6], @intCast(bytes.len), .little);
        header[6..38].* = digest;
        try self.bytes.appendSlice(self.a, &header);
        try self.bytes.appendSlice(self.a, identity);
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
    return warmInner(a, store, pack, domain, cached, cancellation, false);
}
/// The outer bundle is authenticated by the publication. Relocated indexes
/// can retain earlier artifact domains; each member remains content-verified
/// and demand must independently supply its exact identity, length and digest.
pub fn warmPublication(a: A, store: stores.ArtifactStore, pack: Pack, domain: [32]u8, cached: artifacts.CachedRead, cancellation: Cancellation) !void {
    return warmInner(a, store, pack, domain, cached, cancellation, true);
}
fn warmInner(a: A, store: stores.ArtifactStore, pack: Pack, domain: [32]u8, cached: artifacts.CachedRead, cancellation: Cancellation, publication: bool) !void {
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
    const ranges = std.mem.startsWith(u8, decoded, range_magic);
    if (!ranges and !std.mem.startsWith(u8, decoded, magic)) return error.InvalidNativeLakeTextOpeningPack;
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
        if (id_len == 0 or id_len > 4096 or length > (if (ranges) max_decoded_bytes else 1024 * 1024) or @as(u64, id_len) + length > decoded.len - position) return error.InvalidNativeLakeTextOpeningPack;
        const id = decoded[position..][0..id_len];
        position += id_len;
        const checksum = std.fmt.bytesToHex(digest, .lower);
        const parent = if (ranges) if (std.mem.indexOf(u8, id, ":page:")) |offset| id[0..offset] else id else id;
        if (parent.len != id.len) {
            var parts = std.mem.splitScalar(u8, id[parent.len + 6 ..], ':');
            const offset = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidNativeLakeTextOpeningPack, 16) catch return error.InvalidNativeLakeTextOpeningPack;
            const span = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidNativeLakeTextOpeningPack, 16) catch return error.InvalidNativeLakeTextOpeningPack;
            if (parts.next() != null or span != length or offset > std.math.maxInt(u64) - span) return error.InvalidNativeLakeTextOpeningPack;
            // The demand cache key also includes this independently verified
            // range digest; pack contents cannot substitute a different page.
            try stores.validateSha256ArtifactIdentity(parent, try stores.sha256ChecksumFromArtifactId(parent));
        } else try stores.validateSha256ArtifactIdentity(id, &checksum);
        const scope = (try stores.uploadScopeFromArtifactId(parent)) orelse return error.InvalidNativeLakeTextOpeningPack;
        if (!publication and !std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeTextOpeningPack;
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

test "external lake opening publication bundles seed verified ranges and retain credential isolation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("publication-opening-ranges");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(41), 1, std.testing.io);
    var output = store;
    output.allocator = ca;
    const parent = try output.putWithCancellation("full parent artifact", .none);
    const bytes = "page";
    const digest = blk: {
        var result: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
        break :blk result;
    };
    const identity = try std.fmt.allocPrint(ca, "{s}:page:2:4", .{parent.artifact_id});
    var collector: Collector = .{ .a = a, .out = ca, .store = &store, .cancellation = .none, .version = 2, .max_packs = 1 };
    defer collector.deinit();
    try collector.addIdentity(identity, digest, bytes);
    try collector.addIdentity(identity, digest, bytes);
    try std.testing.expectEqual(@as(usize, 1), collector.members);
    try collector.flush();
    try std.testing.expect(collector.full());
    try collector.addIdentity(identity, digest, bytes);
    try std.testing.expectEqual(@as(usize, 0), collector.members);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(4), .context = .{ .io = std.testing.io } };
    try warmPublication(a, store, collector.refs.items[0], store.upload_scope.?.domain, cached, .none);
    const Miss = struct {
        fn load(_: *anyopaque, _: A) ![]u8 {
            return error.UnexpectedOpeningCacheMiss;
        }
    };
    var dummy: u8 = 0;
    const hit = try cache.readImmutableAlloc(a, cached.scope, identity, bytes.len, digest, cached.context, .{ .ptr = &dummy, .load = Miss.load });
    defer a.free(hit);
    try std.testing.expectEqualStrings(bytes, hit);
    try std.testing.expectError(error.UnexpectedOpeningCacheMiss, cache.readImmutableAlloc(a, @splat(5), identity, bytes.len, digest, cached.context, .{ .ptr = &dummy, .load = Miss.load }));
    try std.testing.expectError(error.UnexpectedOpeningCacheMiss, cache.readImmutableAlloc(a, cached.scope, identity, bytes.len, @splat(7), cached.context, .{ .ptr = &dummy, .load = Miss.load }));
    for ([_][]const u8{ ":page:0:3", ":page:ffffffffffffffff:4", ":page:bad-offset:4", ":page:0:4:extra" }) |suffix| {
        var invalid: Collector = .{ .a = a, .out = ca, .store = &store, .cancellation = .none, .version = 2 };
        defer invalid.deinit();
        const malformed = try std.fmt.allocPrint(ca, "{s}{s}", .{ parent.artifact_id, suffix });
        try invalid.addIdentity(malformed, digest, bytes);
        try invalid.flush();
        try std.testing.expectError(error.InvalidNativeLakeTextOpeningPack, warmPublication(a, store, invalid.refs.items[0], store.upload_scope.?.domain, cached, .none));
    }
}
