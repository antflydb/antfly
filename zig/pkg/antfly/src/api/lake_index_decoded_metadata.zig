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

//! Verified immutable metadata, sharing the bounded decoded-cache lease owner.
const std = @import("std");
const local = @import("antfly_local_sources");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const Lease = local.serverless_query_lake_decoded_cache.Lease;
pub fn Owned(comptime T: type) type {
    return struct {
        lease: Lease,
        value: *const T,
        pub fn release(self: @This()) void {
            self.lease.release();
        }
    };
}
pub fn acquire(comptime T: type, cached: artifacts.CachedRead, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, cancellation: @import("antfly_cancellation").CancellationToken, comptime load: anytype) !Owned(T) {
    try cached.context.ensureActive();
    try cancellation.check();
    try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.native-decoded-metadata.v1");
    hash.update(@typeName(T));
    hash.update(&cached.scope);
    hash.update(ref.artifact_id);
    hash.update(ref.checksum);
    hash.update(@tagName(ref.kind));
    hash.update(ref.name);
    var version: [2]u8 = undefined;
    std.mem.writeInt(u16, &version, ref.metadata_version, .little);
    hash.update(&version);
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, ref.byte_len, .little);
    hash.update(&length);
    var key: [32]u8 = undefined;
    hash.final(&key);
    var loader = struct {
        store: stores.ArtifactStore,
        ref: local.serverless_manifest_artifact_ref.ArtifactRef,
        cancellation: @import("antfly_cancellation").CancellationToken,
        cached: artifacts.CachedRead,
        fn decode(raw: *anyopaque, item: *local.serverless_query_lake_decoded_cache.Item) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const a = item.arena.allocator();
            const value = try a.create(T);
            value.* = try load(a, self.store, self.ref, self.cancellation, self.cached);
            item.payload = .{ .extension = value };
        }
    }{ .store = store, .ref = ref, .cancellation = cancellation, .cached = cached };
    // Both API cancellation and the serving deadline remain active while
    // waiting for another decoder or global decode memory admission.
    const Check = struct {
        token: @import("antfly_cancellation").CancellationToken,
        parent: local.serverless_query_lake_read_context.Context,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.token.check();
            try self.parent.ensureActive();
        }
    };
    var check: Check = .{ .token = cancellation, .parent = cached.context };
    var context = cached.context;
    context.checkpoint = .{ .ptr = &check, .check = Check.check };
    const lease = try cached.cache.decoded.acquire(key, 64 * 1024 * 1024, context, .{ .ptr = &loader, .load = @TypeOf(loader).decode });
    return .{ .lease = lease, .value = @ptrCast(@alignCast(lease.item.payload.extension)) };
}

test "external lake decoded metadata reuses owned values while fencing scope version and deadlines" {
    var cache = local.serverless_query_lake_serving_cache.Cache.init(std.testing.allocator);
    defer cache.deinit();
    const Value = struct { text: []const u8 };
    const Loader = struct {
        var calls: usize = 0;
        fn load(a: std.mem.Allocator, _: stores.ArtifactStore, _: local.serverless_manifest_artifact_ref.ArtifactRef, _: @import("antfly_cancellation").CancellationToken, _: ?artifacts.CachedRead) !Value {
            calls += 1;
            return .{ .text = try a.dupe(u8, "owned metadata") };
        }
    };
    Loader.calls = 0;
    const scope = try stores.UploadScope.forPublication(@splat(4), 1, std.testing.io);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("metadata", &digest, .{});
    const checksum = std.fmt.bytesToHex(&digest, .lower);
    const id = try scope.artifactId(&checksum);
    var ref: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .kind = .external_base_source, .artifact_id = &id, .checksum = &checksum, .byte_len = 8 };
    var cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{} };
    const first = try acquire(Value, cached, undefined, ref, .none, Loader.load);
    const second = try acquire(Value, cached, undefined, ref, .none, Loader.load);
    try std.testing.expect(first.value == second.value);
    first.release();
    try std.testing.expectEqualStrings("owned metadata", second.value.text);
    second.release();
    try std.testing.expectEqual(@as(usize, 1), Loader.calls);
    cached.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, acquire(Value, cached, undefined, ref, .none, Loader.load));
    cached.context = .{};
    cached.scope = @splat(2);
    (try acquire(Value, cached, undefined, ref, .none, Loader.load)).release();
    ref.metadata_version += 1;
    (try acquire(Value, cached, undefined, ref, .none, Loader.load)).release();
    try std.testing.expectEqual(@as(usize, 3), Loader.calls);
}
