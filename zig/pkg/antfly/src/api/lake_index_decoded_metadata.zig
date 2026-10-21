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
fn cacheKey(comptime T: type, cached: artifacts.CachedRead, ref: local.serverless_manifest_artifact_ref.ArtifactRef) ![32]u8 {
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
    return key;
}

/// A planning hint only. Serving still acquires a validated, scope-fenced lease.
pub fn resident(comptime T: type, cached: artifacts.CachedRead, ref: local.serverless_manifest_artifact_ref.ArtifactRef) !bool {
    try cached.context.ensureActive();
    const lease = cached.cache.decoded.lookup(try cacheKey(T, cached, ref)) orelse return false;
    lease.release();
    return true;
}

/// Lookup never performs I/O; speculative readers pin only resident metadata.
pub fn lookup(comptime T: type, cached: artifacts.CachedRead, ref: local.serverless_manifest_artifact_ref.ArtifactRef) !?Owned(T) {
    try cached.context.ensureActive();
    const lease = cached.cache.decoded.lookup(try cacheKey(T, cached, ref)) orelse return null;
    errdefer lease.release();
    try cached.context.ensureActive();
    return .{ .lease = lease, .value = @ptrCast(@alignCast(lease.item.payload.extension)) };
}

pub fn acquire(comptime T: type, cached: artifacts.CachedRead, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, cancellation: @import("antfly_cancellation").CancellationToken, comptime load: anytype) !Owned(T) {
    const Adapter = struct {
        fn decode(a: std.mem.Allocator, source: stores.ArtifactStore, reference: local.serverless_manifest_artifact_ref.ArtifactRef, token: @import("antfly_cancellation").CancellationToken, cache: artifacts.CachedRead, _: void) !T {
            return load(a, source, reference, token, cache);
        }
    };
    return acquireInterpreted(T, cached, store, ref, cancellation, "", {}, Adapter.decode);
}

/// Interpretation is authenticated by the caller on every use. Include it in
/// the immutable cache key so a physical block cannot reuse incompatible state.
pub fn acquireInterpreted(comptime T: type, cached: artifacts.CachedRead, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, cancellation: @import("antfly_cancellation").CancellationToken, interpretation: []const u8, arguments: anytype, comptime load: anytype) !Owned(T) {
    return acquireInterpretedBounded(T, cached, store, ref, cancellation, interpretation, arguments, load, 64 * 1024 * 1024, null);
}

/// Optional interpretations remember only their own deterministic size decline.
/// Shared loading pressure and backing allocator failures remain retryable.
pub fn acquireInterpretedBounded(comptime T: type, cached: artifacts.CachedRead, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, cancellation: @import("antfly_cancellation").CancellationToken, interpretation: []const u8, arguments: anytype, comptime load: anytype, limit: usize, fallback: ?T) !Owned(T) {
    try cached.context.ensureActive();
    try cancellation.check();
    var key = try cacheKey(T, cached, ref);
    if (interpretation.len != 0) {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(&key);
        hash.update(interpretation);
        hash.final(&key);
    }
    if (fallback != null) {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(&key);
        var encoded_limit: [8]u8 = undefined;
        std.mem.writeInt(u64, &encoded_limit, limit, .little);
        hash.update(&encoded_limit);
        hash.final(&key);
    }
    var loader = struct {
        store: stores.ArtifactStore,
        ref: local.serverless_manifest_artifact_ref.ArtifactRef,
        cancellation: @import("antfly_cancellation").CancellationToken,
        cached: artifacts.CachedRead,
        arguments: @TypeOf(arguments),
        fallback: ?T,
        fn decode(raw: *anyopaque, item: *local.serverless_query_lake_decoded_cache.Item) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const a = item.arena.allocator();
            const decoded = load(a, self.store, self.ref, self.cancellation, self.cached, self.arguments) catch |err| declined: {
                if (err != error.OutOfMemory or !item.budget.exhausted or item.budget.admission_exhausted) return err;
                const result = self.fallback orelse return err;
                _ = item.arena.reset(.free_all);
                item.budget.exhausted = false;
                break :declined result;
            };
            const value = try a.create(T);
            value.* = decoded;
            item.payload = .{ .extension = value };
        }
    }{ .store = store, .ref = ref, .cancellation = cancellation, .cached = cached, .arguments = arguments, .fallback = fallback };
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
    const lease = try cached.cache.decoded.acquire(key, limit, context, .{ .ptr = &loader, .load = @TypeOf(loader).decode });
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
    try std.testing.expect(!try resident(Value, cached, ref));
    const first = try acquire(Value, cached, undefined, ref, .none, Loader.load);
    try std.testing.expect(try resident(Value, cached, ref));
    const second = try acquire(Value, cached, undefined, ref, .none, Loader.load);
    try std.testing.expect(first.value == second.value);
    first.release();
    try std.testing.expectEqualStrings("owned metadata", second.value.text);
    second.release();
    try std.testing.expectEqual(@as(usize, 1), Loader.calls);
    cached.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, resident(Value, cached, ref));
    try std.testing.expectError(error.DeadlineExceeded, acquire(Value, cached, undefined, ref, .none, Loader.load));
    cached.context = .{};
    cached.scope = @splat(2);
    try std.testing.expect(!try resident(Value, cached, ref));
    (try acquire(Value, cached, undefined, ref, .none, Loader.load)).release();
    ref.metadata_version += 1;
    try std.testing.expect(!try resident(Value, cached, ref));
    (try acquire(Value, cached, undefined, ref, .none, Loader.load)).release();
    try std.testing.expectEqual(@as(usize, 3), Loader.calls);
}

/// Covering blocks use the same bounded singleflight/lease owner as other
/// decoded immutable artifacts. All wire buffers and decoded views share its arena.
pub fn acquireColumnBlock(cached: artifacts.CachedRead, store: stores.ArtifactStore, ref: artifacts.ChunkRef, cancellation: @import("antfly_cancellation").CancellationToken) !Owned(local.sql_spill.ColumnarBlock) {
    const Loader = struct {
        fn load(a: std.mem.Allocator, source: stores.ArtifactStore, artifact: local.serverless_manifest_artifact_ref.ArtifactRef, token: @import("antfly_cancellation").CancellationToken, cache: ?artifacts.CachedRead) !local.sql_spill.ColumnarBlock {
            if (artifact.byte_len > artifacts.max_block_bytes) return error.InvalidNativeLakeRowIndex;
            const bytes = try artifacts.readArtifact(a, source, .{ .artifact_id = artifact.artifact_id, .checksum = artifact.checksum, .byte_len = artifact.byte_len }, token, cache);
            return local.sql_spill.decodeColumnarBlockInArena(a, bytes, artifacts.max_block_bytes);
        }
    };
    return acquire(local.sql_spill.ColumnarBlock, cached, store, .{ .kind = .ordered_row_index, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, Loader.load);
}

/// A verified pack already owns the immutable block bytes. Cache admission
/// still binds the exact content identity and copies into the cache lease.
pub fn acquireColumnBlockBytes(cached: artifacts.CachedRead, store: stores.ArtifactStore, ref: artifacts.ChunkRef, cancellation: @import("antfly_cancellation").CancellationToken, bytes: []const u8) !Owned(local.sql_spill.ColumnarBlock) {
    const Loader = struct {
        fn load(a: std.mem.Allocator, _: stores.ArtifactStore, _: local.serverless_manifest_artifact_ref.ArtifactRef, _: @import("antfly_cancellation").CancellationToken, _: ?artifacts.CachedRead, data: []const u8) !local.sql_spill.ColumnarBlock {
            return local.sql_spill.decodeColumnarBlockInArena(a, try a.dupe(u8, data), artifacts.max_block_bytes);
        }
    };
    return acquireInterpreted(local.sql_spill.ColumnarBlock, cached, store, .{ .kind = .ordered_row_index, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, "", bytes, Loader.load);
}

test "external lake decoded covering blocks share exact dictionaries and fence current readers" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("cover-decoded-cache");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(5), 1, std.testing.io);
    var cells: [32][1]local.sql_scalar.Datum = undefined;
    var rows: [32]local.sql_operators.Row = undefined;
    for (&cells, &rows, 0..) |*cell, *row, i| {
        cell[0] = if (i == 0) .{} else local.sql_scalar.Datum.json(.{ .integer = 9007199254740993 });
        row.* = .{ .values = cell, .keys = &.{}, .ordinal = i };
    }
    const bytes = try local.sql_spill.encodeColumnarBlockAlloc(a, &rows, artifacts.max_block_bytes);
    defer a.free(bytes);
    const stored = try store.put(bytes);
    defer a.free(stored.artifact_id);
    defer a.free(stored.checksum);
    const ref: artifacts.ChunkRef = .{ .artifact_id = stored.artifact_id, .checksum = stored.checksum, .byte_len = stored.byte_len };
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    var cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{ .io = std.testing.io } };
    const first = try acquireColumnBlock(cached, store, ref, .none);
    defer first.release();
    const second = try acquireColumnBlock(cached, store, ref, .none);
    defer second.release();
    try std.testing.expect(first.value == second.value);
    try std.testing.expect((try second.value.cell(0, 0)).sql_null);
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try second.value.cell(31, 0)).value.integer);
    try std.testing.expect((try second.value.dictionaryIdentity(31, 0, false)) != null);
    cached.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, acquireColumnBlock(cached, store, ref, .none));
    cached.context.deadline_ns = null;
    cached.scope = @splat(2);
    const isolated = try acquireColumnBlock(cached, store, ref, .none);
    defer isolated.release();
    try std.testing.expect(isolated.value != first.value);
}

test "external lake decoded metadata remembers local interpretation size declines" {
    const a = std.testing.allocator;
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const Value = struct { expanded: bool = false };
    const Loader = struct {
        var calls: usize = 0;
        fn load(alloc: std.mem.Allocator, _: stores.ArtifactStore, _: local.serverless_manifest_artifact_ref.ArtifactRef, _: @import("antfly_cancellation").CancellationToken, _: ?artifacts.CachedRead, _: void) !Value {
            calls += 1;
            _ = try alloc.alloc(u8, 128 * 1024);
            return .{ .expanded = true };
        }
    };
    Loader.calls = 0;
    const scope = try stores.UploadScope.forPublication(@splat(5), 1, std.testing.io);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("size-decline", &digest, .{});
    const checksum = std.fmt.bytesToHex(&digest, .lower);
    const id = try scope.artifactId(&checksum);
    const ref: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .kind = .algebraic_segment, .artifact_id = &id, .checksum = &checksum, .byte_len = 8 };
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{} };
    for (0..2) |_| {
        const owned = try acquireInterpretedBounded(Value, cached, undefined, ref, .none, "selected-column", {}, Loader.load, 4096, .{});
        defer owned.release();
        try std.testing.expect(!owned.value.expanded);
        try std.testing.expect(owned.lease.item.budget.live < 4096);
    }
    try std.testing.expectEqual(@as(usize, 1), Loader.calls);
    // A changed interpretation policy does not inherit a smaller policy's hint.
    const admitted = try acquireInterpretedBounded(Value, cached, undefined, ref, .none, "selected-column", {}, Loader.load, 256 * 1024, .{});
    defer admitted.release();
    try std.testing.expect(admitted.value.expanded);
    try std.testing.expectEqual(@as(usize, 2), Loader.calls);
    const Transient = struct {
        var calls: usize = 0;
        fn load(_: std.mem.Allocator, _: stores.ArtifactStore, _: local.serverless_manifest_artifact_ref.ArtifactRef, _: @import("antfly_cancellation").CancellationToken, _: ?artifacts.CachedRead, _: void) !Value {
            calls += 1;
            return error.OutOfMemory;
        }
    };
    Transient.calls = 0;
    for (0..2) |_| try std.testing.expectError(error.OutOfMemory, acquireInterpretedBounded(Value, cached, undefined, ref, .none, "transient-failure", {}, Transient.load, 4096, .{}));
    try std.testing.expectEqual(@as(usize, 2), Transient.calls);
}
