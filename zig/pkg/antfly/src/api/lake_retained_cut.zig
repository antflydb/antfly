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

//! Immutable, object-store backed search cuts. A content-addressed token is an
//! opaque capability to server-written data, not a client-supplied snapshot.
//! Current table permissions, schema/recipe and incarnation are rechecked on
//! every use. Index reader leases and lake snapshot pins protect the archive;
//! copying only the bounded WAL suffix makes WAL retirement independent of it.
const std = @import("std");
const local = @import("antfly_local_sources");
const stores = @import("../serverless/artifacts/store.zig");
const ingestion = @import("../serverless/lake_ingestion.zig");
const catalog = local.metadata_lake_index_catalog;
const A = std.mem.Allocator;
pub const ttl_ms: u64 = 300_000;
pub const max_ttl_ms: u64 = std.time.ms_per_hour;
pub fn configuredTtl(config: ?*const local.common_config.Config) u64 {
    return if (config) |value| value.lake_indexes.query_cursors.retention_ms else ttl_ms;
}
pub const max_bytes: usize = 40 * 1024 * 1024;
pub const prefix = "lake2:";
pub const Descriptor = struct {
    version: u16 = 1,
    expires_ms: u64,
    table_id: u64,
    object_generation: u64,
    desired: catalog.Digest,
    publication: catalog.Publication,
    reader_token: catalog.Token,
    pending: ingestion.Pending,
    catalog_metadata: ?local.serverless_external_source_mod.lake_catalog.types.Table = null,
    published_only: bool,
    recent: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = &.{},
    recent_cloud: ?catalog.CloudGenerations = null,
    recent_runs: []const @import("lake_recent_vectors.zig").Run = &.{},
};
fn domain(table: u64, store: catalog.Digest) catalog.Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-lake-retained-search-cuts-v1");
    hash.update(&store);
    var id: [8]u8 = undefined;
    std.mem.writeInt(u64, &id, table, .little);
    hash.update(&id);
    var digest: catalog.Digest = undefined;
    hash.final(&digest);
    return digest;
}
const generations = @import("lake_generation_envelope.zig");
const Token = @import("antfly_cancellation").CancellationToken;
const StateRef = generations.Envelope;
pub const Cache = @import("lake_metadata_memo.zig").Memo(StateRef);
const payload_ttl_ms = 24 * std.time.ms_per_hour;
const State = struct {
    version: u8 = 2,
    publication: catalog.Publication,
    pending: ingestion.Pending,
    change_parts: []const StateRef = &.{},
    catalog_metadata: ?local.serverless_external_source_mod.lake_catalog.types.Table,
    published_only: bool,
    recent: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact,
    recent_cloud: ?catalog.CloudGenerations,
    recent_runs: []const @import("lake_recent_vectors.zig").Run,
};
const LeaseEnvelope = struct {
    version: u16 = 2,
    expires_ms: u64,
    table_id: u64,
    object_generation: u64,
    desired: catalog.Digest,
    reader_token: catalog.Token,
    state: StateRef,
};
fn stateKey(descriptor: Descriptor, identity: [32]u8) [32]u8 {
    // Hash the small immutable authority/recipe view. The accepted image has
    // its own authenticated head identity; do not encode its rows on a hit.
    return @import("lake_wal_image.zig").digest(.{ .format = "retained-state-v3-native", .store = identity, .table = descriptor.table_id, .object_generation = descriptor.object_generation, .desired = descriptor.desired, .publication = descriptor.publication, .pending_identity = descriptor.pending.cut_identity orelse @import("lake_wal_image.zig").digest(descriptor.pending), .base_lsn = descriptor.pending.base_lsn, .lsn = descriptor.pending.lsn, .key_identity = descriptor.pending.key_identity, .catalog_metadata = if (descriptor.catalog_metadata) |value| .{ .version = value.version, .record_key = value.record_key, .metadata_location = value.metadata_location, .retirement_root = value.retirement_root, .metadata_json = if (value.version == null) value.metadata_json else null } else null, .published_only = descriptor.published_only, .recent = descriptor.recent, .recent_cloud = descriptor.recent_cloud, .recent_runs = descriptor.recent_runs });
}
fn upload(a: A, store: *stores.ArtifactStore, scope: stores.UploadScope, value: anytype, cancellation: Token) !StateRef {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    return uploadBytes(a, store, scope, bytes, cancellation);
}
fn uploadBytes(a: A, store: *stores.ArtifactStore, scope: stores.UploadScope, bytes: []const u8, cancellation: Token) !StateRef {
    if (bytes.len == 0 or bytes.len > max_bytes) return error.QueryCandidateBudgetExceeded;
    var metadata = try store.putScoped(scope, bytes, cancellation);
    defer metadata.deinit(store.allocator);
    return .{ .id = try a.dupe(u8, metadata.artifact_id), .bytes = @intCast(metadata.byte_len), .generation = if (metadata.provider_version) |version| try a.dupe(u8, version) else null };
}
pub fn save(a: A, store: *stores.ArtifactStore, store_identity: catalog.Digest, io: std.Io, descriptor: Descriptor, cancellation: Token) ![]const u8 {
    return saveCached(a, store, store_identity, io, descriptor, cancellation, null);
}
pub fn saveCached(a: A, store: *stores.ArtifactStore, store_identity: catalog.Digest, io: std.Io, descriptor: Descriptor, cancellation: Token, cache: ?*Cache) ![]const u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ref = try saveReference(arena.allocator(), store, store_identity, io, descriptor, cancellation, cache);
    return generations.encode(a, prefix, .{ .artifact_id = @constCast(ref.id), .checksum = @constCast(try stores.sha256ChecksumFromArtifactId(ref.id)), .byte_len = ref.bytes, .provider_version = ref.generation });
}
fn saveReference(a: A, store: *stores.ArtifactStore, store_identity: catalog.Digest, io: std.Io, descriptor: Descriptor, cancellation: Token, cache: ?*Cache) !StateRef {
    try cancellation.check();
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    if (descriptor.expires_ms <= now or descriptor.expires_ms > now +| max_ttl_ms) return error.DeadlineExceeded;
    const key = stateKey(descriptor, store_identity);
    const state_ref = (if (cache) |memo| try memo.get(a, key, descriptor.expires_ms) else null) orelse built: {
        const flight = if (cache) |memo| try memo.claim(key, io, cancellation) else null;
        defer if (flight) |held| held.deinit();
        if (cache) |memo| if (try memo.get(a, key, descriptor.expires_ms)) |ref| break :built ref;
        const expiry = @max(now +| payload_ttl_ms, descriptor.expires_ms +| 1);
        var horizon = expiry;
        const scope = try stores.UploadScope.forPublication(domain(descriptor.table_id, store_identity), expiry, io);
        var parts: std.ArrayList(StateRef) = .empty;
        var pending = descriptor.pending;
        var retained_bytes: usize = 0;
        if (pending.count() != 0) {
            // Sixteen independently reusable row groups cap cold fan-out.
            // New cuts serialize only groups containing changed row partitions.
            var groups: [16]std.ArrayList(ingestion.Batch.Change) = @splat(.empty);
            var hashes: [16]std.crypto.hash.Blake3 = undefined;
            var present: [16]bool = @splat(false);
            for (&hashes) |*hash| hash.* = .init(.{});
            if (pending.change_blocks.len == 256 and pending.block_digests.len == 256 and pending.changes.len == 0) {
                for (pending.change_blocks, pending.block_digests, 0..) |block, fingerprint, index| {
                    if (block.len == 0) continue;
                    const group = index >> 4;
                    present[group] = true;
                    hashes[group].update(&.{@intCast(index)});
                    hashes[group].update(&fingerprint);
                }
            } else {
                const flat = try pending.materialized(a);
                for (flat.changes) |change| {
                    const id = try ingestion.rowId(a, pending.key_fields, change.row, pending.key_identity);
                    defer a.free(id);
                    const group = @import("lake_wal_image.zig").partition(id) >> 4;
                    present[group] = true;
                    try groups[group].append(a, change);
                }
                for (&groups, &hashes) |*group, *hash| hash.update(&@import("lake_wal_image.zig").digest(group.items));
            }
            for (&hashes, present, 0..) |*hash, exists, index| {
                if (!exists) continue;
                var fingerprint: [32]u8 = undefined;
                hash.final(&fingerprint);
                const part_key = @import("lake_wal_image.zig").digest(.{ .format = "retained-row-group-v2-native", .domain = scope.domain, .keys = pending.key_fields, .key_identity = pending.key_identity, .prefix = index, .content = fingerprint });
                const part = (if (cache) |memo| try memo.get(a, part_key, descriptor.expires_ms) else null) orelse new: {
                    const part_flight = if (cache) |memo| try memo.claim(part_key, io, cancellation) else null;
                    defer if (part_flight) |held| held.deinit();
                    if (cache) |memo| if (try memo.get(a, part_key, descriptor.expires_ms)) |ref| break :new ref;
                    if (pending.change_blocks.len == 256 and pending.block_digests.len == 256 and pending.changes.len == 0) {
                        for (pending.change_blocks[index * 16 ..][0..16]) |block| try groups[index].appendSlice(a, block);
                    }
                    const bytes = try encodeChanges(a, groups[index].items);
                    defer a.free(bytes);
                    const ref = try uploadBytes(a, store, scope, bytes, cancellation);
                    if (cache) |memo| memo.put(part_key, ref, expiry);
                    break :new ref;
                };
                try validateStateRef(part, scope.domain, descriptor.expires_ms);
                // Root reuse must never extend the lifetime of a transitive
                // immutable row group. Later cursors renew only aging groups.
                horizon = @min(horizon, (try stores.uploadScopeFromArtifactId(part.id)).?.fencingToken());
                retained_bytes = std.math.add(usize, retained_bytes, part.bytes) catch return error.QueryCandidateBudgetExceeded;
                if (retained_bytes > max_bytes) return error.QueryCandidateBudgetExceeded;
                try parts.append(a, part);
            }
            pending.changes = &.{};
            pending.change_blocks = &.{};
            pending.block_digests = &.{};
        }
        const state: State = .{ .publication = descriptor.publication, .pending = pending, .change_parts = parts.items, .catalog_metadata = descriptor.catalog_metadata, .published_only = descriptor.published_only, .recent = descriptor.recent, .recent_cloud = descriptor.recent_cloud, .recent_runs = descriptor.recent_runs };
        const ref = try upload(a, store, scope, state, cancellation);
        if (ref.bytes > max_bytes -| retained_bytes) return error.QueryCandidateBudgetExceeded;
        if (cache) |memo| memo.put(key, ref, horizon);
        break :built ref;
    };
    try cancellation.check();
    try validateStateRef(state_ref, domain(descriptor.table_id, store_identity), descriptor.expires_ms);
    const envelope: LeaseEnvelope = .{ .expires_ms = descriptor.expires_ms, .table_id = descriptor.table_id, .object_generation = descriptor.object_generation, .desired = descriptor.desired, .reader_token = descriptor.reader_token, .state = state_ref };
    const scope = try stores.UploadScope.forPublication(domain(descriptor.table_id, store_identity), descriptor.expires_ms, io);
    return upload(a, store, scope, envelope, cancellation);
}
fn validateStateRef(ref: StateRef, expected_domain: [32]u8, expiry: u64) !void {
    _ = try stores.sha256ChecksumFromArtifactId(ref.id);
    const scope = (try stores.uploadScopeFromArtifactId(ref.id)) orelse return error.InvalidQueryRequest;
    if (ref.bytes == 0 or ref.bytes > max_bytes or !std.mem.eql(u8, &scope.domain, &expected_domain) or scope.fencingToken() < expiry) return error.CatalogGenerationChanged;
    if (ref.generation) |version| if (version.len == 0 or version.len > local.serverless_manifest_artifact_ref.max_provider_generation_bytes or std.mem.eql(u8, version, "null")) return error.InvalidQueryRequest;
}
fn encodeChanges(a: A, changes: []const ingestion.Batch.Change) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try out.appendSlice(a, "AFWC1");
    var count: [4]u8 = undefined;
    std.mem.writeInt(u32, &count, @intCast(changes.len), .little);
    try out.appendSlice(a, &count);
    for (changes) |change| {
        const row = try local.storage_typed_json.encodeBinary(a, change.row);
        defer a.free(row);
        if (row.len > max_bytes -| out.items.len or out.items.len + row.len > max_bytes - 5) return error.QueryCandidateBudgetExceeded;
        try out.append(a, if (change.op == .upsert) 0 else 1);
        std.mem.writeInt(u32, &count, @intCast(row.len), .little);
        try out.appendSlice(a, &count);
        try out.appendSlice(a, row);
    }
    return out.toOwnedSlice(a);
}
fn decodeChanges(a: A, bytes: []const u8) ![]const ingestion.Batch.Change {
    if (bytes.len < 9 or !std.mem.eql(u8, bytes[0..5], "AFWC1")) return error.InvalidQueryRequest;
    const count = std.mem.readInt(u32, bytes[5..9], .little);
    if (count > 65536 or count > (bytes.len - 9) / 10) return error.InvalidQueryRequest;
    const changes = try a.alloc(ingestion.Batch.Change, count);
    var at: usize = 9;
    for (changes) |*change| {
        if (bytes.len - at < 5 or bytes[at] > 1) return error.InvalidQueryRequest;
        const op = bytes[at];
        const length = std.mem.readInt(u32, bytes[at + 1 ..][0..4], .little);
        at += 5;
        if (length > bytes.len - at) return error.InvalidQueryRequest;
        const row = local.storage_typed_json.decodeBinary(a, bytes[at..][0..length]) catch |err| switch (err) {
            error.InvalidTypedJson => return error.InvalidQueryRequest,
            else => return err,
        };
        change.* = .{ .op = if (op == 0) .upsert else .delete, .row = row };
        if (change.row != .object) return error.InvalidQueryRequest;
        at += length;
    }
    if (at != bytes.len) return error.InvalidQueryRequest;
    return changes;
}
fn readParts(a: A, store: *stores.ArtifactStore, state: *State, expected_domain: [32]u8, expiry: u64, cancellation: Token, io: ?std.Io) !void {
    if (state.change_parts.len > 16 or (state.change_parts.len != 0 and state.pending.count() != 0)) return error.InvalidQueryRequest;
    var total: usize = 0;
    for (state.change_parts) |part| {
        try validateStateRef(part, expected_domain, expiry);
        total = std.math.add(usize, total, part.bytes) catch return error.QueryCandidateBudgetExceeded;
        if (total > max_bytes) return error.QueryCandidateBudgetExceeded;
    }
    const scheduler = local.sql_parallel_scheduler;
    const Worker = struct {
        fn read(artifacts: stores.ArtifactStore, ref: StateRef, token: Token) anyerror![]u8 {
            var borrowed = artifacts;
            borrowed.allocator = std.heap.page_allocator;
            return readRetained(&borrowed, ref, token);
        }
    };
    var changes: std.ArrayList(ingestion.Batch.Change) = .empty;
    var first: usize = 0;
    while (first < state.change_parts.len) {
        const count = @min(4, state.change_parts.len - first);
        var tasks: [4]?scheduler.Task(anyerror![]u8) = @splat(null);
        var bodies: [4]?[]u8 = @splat(null);
        defer {
            for (&tasks) |*task| if (task.*) |pending| {
                var running = pending;
                if (running.cancel(io.?)) |bytes| std.heap.page_allocator.free(bytes) else |_| {}
            };
            for (bodies) |body| if (body) |bytes| std.heap.page_allocator.free(bytes);
        }
        for (state.change_parts[first..][0..count], 0..) |part, i| {
            if (io) |runtime| tasks[i] = scheduler.global().submitIoTransient(runtime, part.bytes *| 2, Worker.read, .{ store.*, part, cancellation });
            if (tasks[i] == null) bodies[i] = try Worker.read(store.*, part, cancellation);
        }
        for (state.change_parts[first..][0..count], 0..) |_, i| {
            if (tasks[i]) |pending| {
                var running = pending;
                tasks[i] = null;
                bodies[i] = try running.await(io.?);
            }
            try cancellation.check();
            const part = if (state.version == 2) try decodeChanges(a, bodies[i].?) else try std.json.parseFromSliceLeaky([]const ingestion.Batch.Change, a, bodies[i].?, .{ .allocate = .alloc_always });
            if (part.len > 65536 -| changes.items.len) return error.LakeOverlayTooLarge;
            try changes.appendSlice(a, part);
            std.heap.page_allocator.free(bodies[i].?);
            bodies[i] = null;
        }
        first += count;
    }
    if (state.change_parts.len != 0) state.pending.changes = try changes.toOwnedSlice(a);
}
fn readRetained(store: *stores.ArtifactStore, ref: StateRef, cancellation: Token) ![]u8 {
    return generations.read(store, ref, cancellation) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => error.CatalogGenerationChanged,
        else => err,
    };
}
/// A query pins decoded data, never provider clients, request authority or
/// reader leases. Eviction leaves outstanding query pins independently valid.
pub const Pinned = struct {
    descriptor: Descriptor,
    overlay: ?@import("lake_search_overlay.zig").Overlay = null,
    lease: ?local.serverless_query_lake_decoded_cache.Lease = null,
    pub fn deinit(self: Pinned) void {
        if (self.lease) |held| held.release();
    }
};
const Image = @import("lake_wal_image.zig").Image;
const DecodedGroup = struct {
    changes: []const ingestion.Batch.Change,
    image: ?*Image = null,
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.image) |image| image.release();
    }
};
const GroupLoader = struct {
    store: stores.ArtifactStore,
    ref: StateRef,
    version: u8,
    keys: []const []const u8,
    encoding: ingestion.KeyIdentity,
    cancellation: Token,
    fn decode(raw: *anyopaque, item: *local.serverless_query_lake_decoded_cache.Item) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        // Decode workers never allocate through a borrowed request arena.
        // The temporary body and decoded values use this group's own budget.
        var backend = self.store;
        backend.allocator = item.budget.allocator();
        const bytes = try readRetained(&backend, self.ref, self.cancellation);
        defer backend.allocator.free(bytes);
        const a = item.arena.allocator();
        const value = try a.create(DecodedGroup);
        value.* = .{ .changes = if (self.version == 2) try decodeChanges(a, bytes) else try std.json.parseFromSliceLeaky([]const ingestion.Batch.Change, a, bytes, .{ .allocate = .alloc_always }) };
        if (value.changes.len > 65536) return error.LakeOverlayTooLarge;
        item.payload = .{ .extension = value };
        item.extension_deinit = DecodedGroup.close;
        if (self.version == 2 and self.encoding == .canonical) value.image = try Image.fromBorrowedChanges(item.budget.allocator(), .{ .key_fields = self.keys, .lsn = 0, .changes = value.changes, .key_identity = .canonical });
    }
};
fn readSharedParts(item: *local.serverless_query_lake_decoded_cache.Item, cache: *local.serverless_query_lake_decoded_cache.Cache, store: stores.ArtifactStore, state: *State, expected_domain: [32]u8, expiry: u64, token: Token, context: local.serverless_query_lake_read_context.Context) !?*Image {
    if (state.change_parts.len > 16 or (state.change_parts.len != 0 and state.pending.count() != 0)) return error.InvalidQueryRequest;
    const a = item.arena.allocator();
    const leases = try a.alloc(local.serverless_query_lake_decoded_cache.Lease, state.change_parts.len);
    var held: usize = 0;
    errdefer for (leases[0..held]) |lease| lease.release();
    const blocks = try a.alloc([]const ingestion.Batch.Change, state.change_parts.len);
    const view = try a.create(Image);
    view.* = .{ .a = a, .borrowed = true, .key_fields = state.pending.key_fields };
    var complete_view = state.version == 2 and state.pending.key_identity == .canonical and state.change_parts.len != 0;
    const scheduler = local.sql_parallel_scheduler;
    var rows: usize = 0;
    var retained_bytes: usize = 0;
    var first_part: usize = 0;
    while (first_part < state.change_parts.len) {
        const count = @min(4, state.change_parts.len - first_part);
        var tasks: [4]?scheduler.Task(anyerror!local.serverless_query_lake_decoded_cache.Lease) = @splat(null);
        var results: [4]?local.serverless_query_lake_decoded_cache.Lease = @splat(null);
        defer {
            for (&tasks) |*task| if (task.*) |pending| {
                var running = pending;
                if (running.cancel(context.io.?)) |lease| lease.release() else |_| {}
            };
            for (results) |result| if (result) |lease| lease.release();
        }
        for (state.change_parts[first_part..][0..count], 0..) |part, i| {
            try validateStateRef(part, expected_domain, expiry);
            try context.ensureActive();
            try token.check();
            if (context.io) |io| tasks[i] = scheduler.global().submitIoTransient(io, groupLimit(part), acquireGroup, .{ cache, store, part, state.version, state.pending.key_fields, state.pending.key_identity, expected_domain, token, context });
            if (tasks[i] == null) results[i] = try acquireGroup(cache, store, part, state.version, state.pending.key_fields, state.pending.key_identity, expected_domain, token, context);
        }
        for (0..count) |i| {
            if (tasks[i]) |pending| {
                var running = pending;
                tasks[i] = null;
                results[i] = try running.await(context.io.?);
            }
            try context.ensureActive();
            try token.check();
            leases[held] = results[i].?;
            results[i] = null;
            const lease = leases[held];
            held += 1;
            retained_bytes = try std.math.add(usize, retained_bytes, lease.item.budget.peak);
            if (retained_bytes > 256 * 1024 * 1024) return error.QueryCandidateBudgetExceeded;
            const group: *const DecodedGroup = @ptrCast(@alignCast(lease.item.payload.extension));
            blocks[first_part + i] = group.changes;
            if (group.image) |image| {
                for (image.parts, 0..) |part, index| if (part) |owned| {
                    if (view.parts[index] != null) return error.InvalidQueryRequest;
                    view.parts[index] = owned;
                    view.blocks[index] = owned.changes;
                    view.block_digests[index] = owned.wire_digest;
                };
                view.row_count += image.row_count;
                view.change_count += image.change_count;
            } else complete_view = false;
            rows = try std.math.add(usize, rows, group.changes.len);
            if (rows > 65536) return error.LakeOverlayTooLarge;
        }
        first_part += count;
    }
    if (blocks.len != 0) state.pending.change_blocks = blocks;
    item.foreign_dependencies = leases;
    for (leases) |lease| item.foreign_bytes += lease.item.budget.live;
    return if (complete_view) view else null;
}
fn groupLimit(part: StateRef) usize {
    return @min(64 * 1024 * 1024, @as(usize, part.bytes) *| 32 +| 1024 * 1024);
}
fn acquireGroup(cache: *local.serverless_query_lake_decoded_cache.Cache, store: stores.ArtifactStore, part: StateRef, version: u8, keys: []const []const u8, encoding: ingestion.KeyIdentity, expected_domain: [32]u8, token: Token, context: local.serverless_query_lake_read_context.Context) anyerror!local.serverless_query_lake_decoded_cache.Lease {
    const key_ = @import("lake_wal_image.zig").digest(.{ .format = "decoded-retained-row-group-v1", .domain = expected_domain, .version = version, .keys = keys, .encoding = encoding, .ref = part });
    var loader: GroupLoader = .{ .store = store, .ref = part, .version = version, .keys = keys, .encoding = encoding, .cancellation = token };
    return cache.acquire(key_, groupLimit(part), context, .{ .ptr = &loader, .load = GroupLoader.decode }) catch |err| switch (err) {
        error.OutOfMemory, error.SqlMemoryLimitExceeded => private: {
            const value = try cache.create(256 * 1024 * 1024);
            errdefer value.release();
            try GroupLoader.decode(&loader, value.item);
            try context.ensureActive();
            try token.check();
            break :private value;
        },
        else => return err,
    };
}
const Decoded = struct { state: State, overlay: ?@import("lake_search_overlay.zig").Overlay };
const StateLoader = struct {
    store: stores.ArtifactStore,
    ref: StateRef,
    domain: [32]u8,
    expiry: u64,
    cancellation: Token,
    groups: ?*local.serverless_query_lake_decoded_cache.Cache = null,
    context: local.serverless_query_lake_read_context.Context = .{},
    fn decode(raw: *anyopaque, item: *local.serverless_query_lake_decoded_cache.Item) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const a = item.arena.allocator();
        const value = try a.create(Decoded);
        value.* = try decodeState(a, &self.store, self.ref, self.domain, self.expiry, self.cancellation, if (self.groups) |cache| .{ .item = item, .cache = cache, .context = self.context } else null);
        item.payload = .{ .extension = value };
    }
};
const GroupOwner = struct { item: *local.serverless_query_lake_decoded_cache.Item, cache: *local.serverless_query_lake_decoded_cache.Cache, context: local.serverless_query_lake_read_context.Context };
fn decodeState(a: A, store: *stores.ArtifactStore, ref: StateRef, expected_domain: [32]u8, expiry: u64, cancellation: Token, groups: ?GroupOwner) !Decoded {
    const body = try readRetained(store, ref, cancellation);
    defer store.allocator.free(body);
    var state = try std.json.parseFromSliceLeaky(State, a, body, .{ .allocate = .alloc_always });
    if (state.version != 1 and state.version != 2) return error.InvalidQueryRequest;
    var retained_bytes: usize = body.len;
    for (state.change_parts) |part| {
        retained_bytes = std.math.add(usize, retained_bytes, part.bytes) catch return error.QueryCandidateBudgetExceeded;
        if (retained_bytes > max_bytes) return error.QueryCandidateBudgetExceeded;
    }
    const image = if (groups) |owner| try readSharedParts(owner.item, owner.cache, store.*, &state, expected_domain, expiry, cancellation, owner.context) else legacy: {
        try readParts(a, store, &state, expected_domain, expiry, cancellation, if (store.providerStore()) |object| object.cloud_io else null);
        break :legacy null;
    };
    const overlay: ?@import("lake_search_overlay.zig").Overlay = if (image) |view| .{ .pending = state.pending, .rows = .{ .image = view }, .key_filter = "" } else if (state.pending.count() != 0) try @import("lake_search_overlay.zig").Overlay.init(a, state.pending) else null;
    return .{ .state = state, .overlay = overlay };
}
pub fn load(a: A, store: *stores.ArtifactStore, store_identity: catalog.Digest, token: []const u8, table: local.common_topology_records.TableRecord, now: u64, cancellation: Token) !Descriptor {
    const pin = try loadImpl(a, store, store_identity, token, table, now, cancellation, null, .{}, null);
    defer pin.deinit();
    return pin.descriptor;
}
pub fn loadCached(a: A, store: *stores.ArtifactStore, store_identity: catalog.Digest, token: []const u8, table: local.common_topology_records.TableRecord, now: u64, cancellation: Token, cache: *local.serverless_query_lake_decoded_cache.Cache, context: local.serverless_query_lake_read_context.Context) !Pinned {
    return loadImpl(a, store, store_identity, token, table, now, cancellation, cache, context, null);
}
pub fn loadWithGroups(a: A, store: *stores.ArtifactStore, store_identity: catalog.Digest, token: []const u8, table: local.common_topology_records.TableRecord, now: u64, cancellation: Token, cache: *local.serverless_query_lake_decoded_cache.Cache, groups: *local.serverless_query_lake_decoded_cache.Cache, context: local.serverless_query_lake_read_context.Context) !Pinned {
    return loadImpl(a, store, store_identity, token, table, now, cancellation, cache, context, groups);
}
fn loadImpl(a: A, store: *stores.ArtifactStore, store_identity: catalog.Digest, token: []const u8, table: local.common_topology_records.TableRecord, now: u64, cancellation: Token, cache: ?*local.serverless_query_lake_decoded_cache.Cache, context: local.serverless_query_lake_read_context.Context, groups: ?*local.serverless_query_lake_decoded_cache.Cache) !Pinned {
    var held: ?local.serverless_query_lake_decoded_cache.Lease = null;
    errdefer if (held) |lease| lease.release();
    var overlay: ?@import("lake_search_overlay.zig").Overlay = null;
    try context.ensureActive();
    try cancellation.check();
    if (!std.mem.startsWith(u8, token, prefix)) return error.InvalidQueryRequest;
    const envelope = try generations.decode(a, prefix, token, max_bytes);
    defer if (envelope.generation) |value| a.free(value);
    const scope = (try stores.uploadScopeFromArtifactId(envelope.id)) orelse return error.InvalidQueryRequest;
    const expected_domain = domain(table.table_id, store_identity);
    if (!std.mem.eql(u8, &scope.domain, &expected_domain) or scope.fencingToken() <= now) return error.CatalogGenerationChanged;
    const bytes = try readRetained(store, envelope, cancellation);
    defer store.allocator.free(bytes);
    const version = (try std.json.parseFromSliceLeaky(struct { version: u16 }, a, bytes, .{ .ignore_unknown_fields = true })).version;
    const result: Descriptor = if (version == 1)
        try std.json.parseFromSliceLeaky(Descriptor, a, bytes, .{ .allocate = .alloc_always })
    else if (version == 2) modern: {
        const lease = try std.json.parseFromSliceLeaky(LeaseEnvelope, a, bytes, .{ .allocate = .alloc_always });
        if (lease.expires_ms <= now or lease.expires_ms > now +| max_ttl_ms or lease.expires_ms != scope.fencingToken() or lease.table_id != table.table_id or lease.object_generation != table.object_storage_generation or !std.mem.eql(u8, &lease.desired, &catalog.desiredFingerprint(table))) return error.CatalogGenerationChanged;
        try validateStateRef(lease.state, expected_domain, lease.expires_ms);
        const decoded: Decoded = if (cache) |memo| shared: {
            const key = @import("lake_wal_image.zig").digest(.{ .format = "retained-decoded-state-v1", .store = store_identity, .table = table.table_id, .incarnation = table.object_storage_generation, .ref = lease.state });
            var loader: StateLoader = .{ .store = store.*, .ref = lease.state, .domain = expected_domain, .expiry = lease.expires_ms, .cancellation = cancellation, .groups = groups, .context = context };
            const Check = struct {
                token: Token,
                parent: local.serverless_query_lake_read_context.Context,
                fn check(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try self.token.check();
                    try self.parent.ensureActive();
                }
            };
            var check: Check = .{ .token = cancellation, .parent = context };
            var checked = context;
            checked.checkpoint = .{ .ptr = &check, .check = Check.check };
            loader.context = checked;
            // Cache admission is optional. Legacy JSON can need a larger
            // bounded workspace than native values or cache residency allows.
            // Private items still use the cache's process-accounted allocator.
            held = memo.acquire(key, 64 * 1024 * 1024, checked, .{ .ptr = &loader, .load = StateLoader.decode }) catch |err| switch (err) {
                error.OutOfMemory, error.SqlMemoryLimitExceeded => private: {
                    try checked.ensureActive();
                    const pin = try memo.create(256 * 1024 * 1024);
                    errdefer pin.release();
                    try StateLoader.decode(&loader, pin.item);
                    try checked.ensureActive();
                    break :private pin;
                },
                else => return err,
            };
            const value: *const Decoded = @ptrCast(@alignCast(held.?.item.payload.extension));
            break :shared value.*;
        } else try decodeState(a, store, lease.state, expected_domain, lease.expires_ms, cancellation, null);
        const state = decoded.state;
        // A shared decode is data only: the current envelope still proves
        // every transitive scope covers this reader's independent lifetime.
        for (state.change_parts) |part| try validateStateRef(part, expected_domain, lease.expires_ms);
        overlay = decoded.overlay;
        break :modern .{ .expires_ms = lease.expires_ms, .table_id = lease.table_id, .object_generation = lease.object_generation, .desired = lease.desired, .reader_token = lease.reader_token, .publication = state.publication, .pending = state.pending, .catalog_metadata = state.catalog_metadata, .published_only = state.published_only, .recent = state.recent, .recent_cloud = state.recent_cloud, .recent_runs = state.recent_runs };
    } else return error.InvalidQueryRequest;
    if (result.expires_ms != scope.fencingToken() or result.expires_ms <= now or result.expires_ms > now +| max_ttl_ms or result.table_id != table.table_id or result.object_generation != table.object_storage_generation or !std.mem.eql(u8, &result.desired, &catalog.desiredFingerprint(table))) return error.CatalogGenerationChanged;
    try result.publication.validate();
    try context.ensureActive();
    try cancellation.check();
    if (version == 1 and result.pending.count() != 0) overlay = try @import("lake_search_overlay.zig").Overlay.init(a, result.pending);
    return .{ .descriptor = result, .overlay = overlay, .lease = held };
}
/// Bounded discovery over a dedicated domain; archive generations use a
/// different domain and cannot be collected by this path.
pub fn collect(store: *stores.ArtifactStore, table: u64, store_identity: catalog.Digest, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) !void {
    const Visitor = struct {
        store: *stores.ArtifactStore,
        now: u64,
        deleted: usize = 0,
        fn visit(raw: *anyopaque, scope: stores.UploadScope, artifact_id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (scope.fencingToken() +| 30_000 >= self.now) return;
            if (self.deleted == 128) return error.RetainedCutCollectionBound;
            try self.store.delete(artifact_id);
            self.deleted += 1;
        }
    };
    var visitor: Visitor = .{ .store = store, .now = now };
    store.visitScopedUploads(domain(table, store_identity), .{ .ptr = &visitor, .visit = Visitor.visit, .fencing_cutoff = now -| 30_000, .max_entries = 128 }, cancellation) catch |err| switch (err) {
        error.RetainedCutCollectionBound => {},
        else => return err,
    };
}

const CursorEnvelope = struct { version: u16 = 1, expires_ms: u64, payload: []const u8 };
pub fn saveCursor(a: A, store: *stores.ArtifactStore, identity: catalog.Digest, io: std.Io, payload: []const u8, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) ![]const u8 {
    if (payload.len > 1024 * 1024) return error.QueryCandidateBudgetExceeded;
    var expires = now +| max_ttl_ms;
    const state = try std.json.parseFromSliceLeaky(std.json.Value, a, payload, .{});
    if (state != .object) return error.InvalidQueryRequest;
    const sources = state.object.get("sources") orelse return error.InvalidQueryRequest;
    if (sources != .array) return error.InvalidQueryRequest;
    for (sources.array.items) |source| {
        if (source != .object) return error.InvalidQueryRequest;
        const remote = source.object.get("remote_snapshot") orelse return error.InvalidQueryRequest;
        if (remote != .string) return error.UnsupportedQueryRequest;
        const leaf_prefix = if (std.mem.startsWith(u8, remote.string, prefix)) prefix else if (std.mem.startsWith(u8, remote.string, @import("native_retained_cut.zig").prefix)) @import("native_retained_cut.zig").prefix else return error.UnsupportedQueryRequest;
        const leaf_scope = (try stores.uploadScopeFromArtifactId(try @import("lake_generation_envelope.zig").artifactId(leaf_prefix, remote.string))) orelse return error.InvalidQueryRequest;
        expires = @min(expires, leaf_scope.fencingToken());
    }
    if (expires <= now) return error.CatalogGenerationChanged;
    const encoded = try std.json.Stringify.valueAlloc(a, CursorEnvelope{ .expires_ms = expires, .payload = payload }, .{});
    defer a.free(encoded);
    const scope = try stores.UploadScope.forPublication(domain(0, identity), expires, io);
    var metadata = try store.putScoped(scope, encoded, cancellation);
    defer metadata.deinit(store.allocator);
    return @import("lake_generation_envelope.zig").encode(a, "source2:", metadata);
}
pub fn loadCursor(a: A, store: *stores.ArtifactStore, identity: catalog.Digest, token: []const u8, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) ![]const u8 {
    if (!std.mem.startsWith(u8, token, "source2:")) return error.InvalidQueryRequest;
    const selector = try @import("lake_generation_envelope.zig").decode(a, "source2:", token, 2 * 1024 * 1024);
    defer if (selector.generation) |value| a.free(value);
    const artifact = selector.id;
    const scope = (try stores.uploadScopeFromArtifactId(artifact)) orelse return error.InvalidQueryRequest;
    if (scope.fencingToken() <= now or !std.mem.eql(u8, &scope.domain, &domain(0, identity))) return error.CatalogGenerationChanged;
    const bytes = try @import("lake_generation_envelope.zig").read(store, selector, cancellation);
    defer store.allocator.free(bytes);
    const envelope = try std.json.parseFromSliceLeaky(CursorEnvelope, a, bytes, .{ .allocate = .alloc_always });
    if (envelope.version != 1 or envelope.expires_ms != scope.fencingToken() or envelope.expires_ms <= now or envelope.expires_ms > now +| max_ttl_ms or envelope.payload.len > 1024 * 1024) return error.CatalogGenerationChanged;
    return envelope.payload;
}

test "external lake bounded WAL retained cuts survive reopening and fence expiration incarnation and recipe" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-retained-cuts");
    defer directory.cleanup();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    defer a.free(json);
    var config = try local.common_config.Config.parseFromSlice(a, json);
    defer config.deinit();
    var store = try @import("lake_index_store.zig").Store.open(a, &config, null, false);
    var store_open = true;
    defer if (store_open) store.deinit();
    const identity = store.identity;
    var artifacts = store.artifactStore();
    const table: local.common_topology_records.TableRecord = .{ .table_id = 4, .name = "history", .schema_json = "{}", .indexes_json = "{}" };
    const publication: catalog.Publication = .{ .generation = 1, .token = @splat(1), .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = @splat(4) }, .published_at_ms = 1, .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://bucket/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = "inventory" } }, .inventory = .{ .artifact_id = "inventory", .kind = .external_base_source, .byte_len = 42, .checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" } };
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    var scratch_arena = std.heap.ArenaAllocator.init(a);
    defer scratch_arena.deinit();
    const sa = scratch_arena.allocator();
    const images = @import("lake_wal_image.zig");
    const first = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"id\":0,\"body\":\"first\"}", .{});
    const last = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"id\":1000,\"body\":\"last\"}", .{});
    const image = try images.Image.update(a, null, .{ .lsn = 8, .key_fields = &.{"id"}, .changes = &.{ .{ .op = .upsert, .row = first }, .{ .op = .upsert, .row = last } } });
    defer image.release();
    var descriptor: Descriptor = .{ .expires_ms = now + ttl_ms, .table_id = 4, .object_generation = 0, .desired = catalog.desiredFingerprint(table), .publication = publication, .reader_token = @splat(5), .pending = .{ .lsn = 8, .cut_identity = @splat(1), .key_identity = .canonical, .key_fields = image.key_fields, .change_blocks = &image.blocks, .block_digests = &image.block_digests }, .published_only = false };
    var cache: Cache = .{ .budget = .{ .backing = a, .limit = 1024 * 1024 } };
    defer cache.deinit();
    const token = try saveCached(a, &artifacts, identity, std.testing.io, descriptor, .none, &cache);
    defer a.free(token);
    const Read = struct {
        fn envelope(alloc: A, backend: *stores.ArtifactStore, value: []const u8) !LeaseEnvelope {
            const ref = try generations.decode(alloc, prefix, value, max_bytes);
            const bytes = try readRetained(backend, ref, .none);
            defer backend.allocator.free(bytes);
            return std.json.parseFromSliceLeaky(LeaseEnvelope, alloc, bytes, .{ .allocate = .alloc_always });
        }
        fn state(alloc: A, backend: *stores.ArtifactStore, ref: StateRef) !State {
            const bytes = try readRetained(backend, ref, .none);
            defer backend.allocator.free(bytes);
            return std.json.parseFromSliceLeaky(State, alloc, bytes, .{ .allocate = .alloc_always });
        }
    };
    const old_envelope = try Read.envelope(sa, &artifacts, token);
    const old_state = try Read.state(sa, &artifacts, old_envelope.state);
    try std.testing.expectEqual(@as(usize, 2), old_state.change_parts.len);
    try std.testing.expectEqual(@as(u64, 3), cache.puts);
    {
        // Model unchanged row groups retained from an earlier day's cut.
        // They still cover the current five-minute query, but not a later
        // twenty-minute query. A new root must inherit that earlier horizon.
        var aging: Cache = .{ .budget = .{ .backing = a, .limit = 1024 * 1024 } };
        defer aging.deinit();
        const root_key = stateKey(descriptor, identity);
        var entries = cache.entries.iterator();
        while (entries.next()) |entry| {
            if (std.mem.eql(u8, &entry.key_ptr.*, &root_key)) continue;
            const original = try std.json.parseFromSliceLeaky(StateRef, sa, entry.value_ptr.bytes, .{});
            const body = try readRetained(&artifacts, original, .none);
            defer artifacts.allocator.free(body);
            var earlier = try artifacts.putScoped(try stores.UploadScope.forPublication(domain(4, identity), now + 600_000, std.testing.io), body, .none);
            defer earlier.deinit(artifacts.allocator);
            aging.put(entry.key_ptr.*, .{ .id = earlier.artifact_id, .bytes = @intCast(earlier.byte_len), .generation = earlier.provider_version }, now + 600_000);
        }
        const short = try saveCached(sa, &artifacts, identity, std.testing.io, descriptor, .none, &aging);
        _ = try load(sa, &artifacts, identity, short, table, now + 1, .none);
        var decoded: local.serverless_query_lake_decoded_cache.Cache = .{ .a = a };
        defer decoded.deinit();
        const short_pin = try loadCached(sa, &artifacts, identity, short, table, now + 1, .none, &decoded, .{ .io = std.testing.io });
        defer short_pin.deinit();
        var forged = try Read.envelope(sa, &artifacts, short);
        forged.expires_ms = now + 1_200_000;
        const forged_ref = try upload(sa, &artifacts, try stores.UploadScope.forPublication(domain(4, identity), forged.expires_ms, std.testing.io), forged, .none);
        const forged_token = try generations.encode(sa, prefix, .{ .artifact_id = @constCast(forged_ref.id), .checksum = @constCast(try stores.sha256ChecksumFromArtifactId(forged_ref.id)), .byte_len = forged_ref.bytes, .provider_version = forged_ref.generation });
        try std.testing.expectError(error.CatalogGenerationChanged, loadCached(sa, &artifacts, identity, forged_token, table, now + 1, .none, &decoded, .{ .io = std.testing.io }));
        try std.testing.expect(decoded.hits != 0);
        var longer = descriptor;
        longer.expires_ms = now + 1_200_000;
        const long = try saveCached(sa, &artifacts, identity, std.testing.io, longer, .none, &aging);
        // Every successfully issued fresh token must remain replayable.
        _ = try load(sa, &artifacts, identity, long, table, now + 1, .none);
        const short_envelope = try Read.envelope(sa, &artifacts, short);
        const long_envelope = try Read.envelope(sa, &artifacts, long);
        try std.testing.expect(!std.mem.eql(u8, short_envelope.state.id, long_envelope.state.id));
        const long_state = try Read.state(sa, &artifacts, long_envelope.state);
        for (long_state.change_parts) |part| try std.testing.expect((try stores.uploadScopeFromArtifactId(part.id)).?.fencingToken() > longer.expires_ms);
    }
    {
        const Worker = struct {
            fn run(backend: *stores.ArtifactStore, id: [32]u8, value: Descriptor, memo: *Cache) anyerror![]const u8 {
                return saveCached(std.heap.page_allocator, backend, id, std.testing.io, value, .none, memo);
            }
        };
        var concurrent: Cache = .{ .budget = .{ .backing = a, .limit = 1024 * 1024 } };
        defer concurrent.deinit();
        var jobs: [8]?std.Io.Future(anyerror![]const u8) = @splat(null);
        defer for (&jobs) |*job| if (job.*) |*running| {
            if (running.cancel(std.testing.io)) |value| std.heap.page_allocator.free(value) else |_| {}
        };
        for (&jobs) |*job| job.* = try std.testing.io.concurrent(Worker.run, .{ &artifacts, identity, descriptor, &concurrent });
        var published: ?[]const u8 = null;
        for (&jobs) |*job| {
            var running = job.*.?;
            job.* = null;
            const value = try running.await(std.testing.io);
            defer std.heap.page_allocator.free(value);
            const envelope = try Read.envelope(sa, &artifacts, value);
            if (published) |prior| try std.testing.expectEqualStrings(prior, envelope.state.id) else published = envelope.state.id;
        }
        try std.testing.expectEqual(@as(u64, 3), concurrent.puts);
    }
    descriptor.expires_ms += 60_000;
    descriptor.reader_token = @splat(6);
    const fresh = try saveCached(a, &artifacts, identity, std.testing.io, descriptor, .none, &cache);
    defer a.free(fresh);
    const fresh_envelope = try Read.envelope(sa, &artifacts, fresh);
    try std.testing.expectEqualStrings(old_envelope.state.id, fresh_envelope.state.id);
    try std.testing.expectEqual(descriptor.expires_ms, fresh_envelope.expires_ms);
    try std.testing.expectEqual(descriptor.reader_token, fresh_envelope.reader_token);
    try std.testing.expectEqual(@as(u64, 3), cache.puts);
    {
        var decoded: local.serverless_query_lake_decoded_cache.Cache = .{ .a = a };
        defer decoded.deinit();
        const cold = try loadCached(sa, &artifacts, identity, token, table, now + 1, .none, &decoded, .{ .io = std.testing.io });
        defer cold.deinit();
        const warm = try loadCached(sa, &artifacts, identity, fresh, table, now + 1, .none, &decoded, .{ .io = std.testing.io });
        defer warm.deinit();
        try std.testing.expect(cold.lease.?.item == warm.lease.?.item);
        try std.testing.expect(cold.overlay.?.rows.local_rows.getEntry(try ingestion.rowId(sa, &.{"id"}, first, .canonical)).?.value_ptr == warm.overlay.?.rows.local_rows.getEntry(try ingestion.rowId(sa, &.{"id"}, first, .canonical)).?.value_ptr);
        try std.testing.expectEqual(descriptor.reader_token, warm.descriptor.reader_token);
        try std.testing.expectEqual(descriptor.expires_ms, warm.descriptor.expires_ms);
        try std.testing.expectError(error.DeadlineExceeded, loadCached(sa, &artifacts, identity, fresh, table, now + 1, .none, &decoded, .{ .deadline_ns = 1 }));
        try std.testing.expectError(error.CatalogGenerationChanged, loadCached(sa, &artifacts, identity, token, table, now + ttl_ms, .none, &decoded, .{}));
        decoded.max_entries = 1;
        const pressure = try decoded.create(1024);
        defer pressure.release();
        decoded.publish(@splat(99), pressure);
        try std.testing.expect(!pressure.item.cached);
        // Pinned data stays valid when residency admission is saturated.
        try std.testing.expectEqual(@as(usize, 2), warm.overlay.?.rows.count());
    }
    {
        var uploads: Cache = .{ .budget = .{ .backing = a, .limit = 1024 * 1024 } };
        defer uploads.deinit();
        const original = try saveCached(sa, &artifacts, identity, std.testing.io, descriptor, .none, &uploads);
        var groups: local.serverless_query_lake_decoded_cache.Cache = .{ .a = a };
        defer groups.deinit();
        var states: local.serverless_query_lake_decoded_cache.Cache = .{ .a = a };
        defer states.deinit();
        const old = try loadWithGroups(sa, &artifacts, identity, original, table, now + 1, .none, &states, &groups, .{ .io = std.testing.io });
        defer old.deinit();
        try std.testing.expectEqual(@as(usize, 2), old.lease.?.item.foreign_dependencies.len);
        try std.testing.expect(old.overlay.?.rows.image != null);
        try std.testing.expectEqual(@as(usize, 0), old.overlay.?.rows.local_rows.count());
        try std.testing.expectEqualStrings("", old.overlay.?.key_filter);
        try std.testing.expectEqual(@as(usize, 0), old.overlay.?.pending.changes.len);
        const replacement = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"id\":0,\"body\":\"changed\"}", .{});
        const next_image = try images.Image.update(a, image, .{ .lsn = 9, .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = replacement }} });
        defer next_image.release();
        var next = descriptor;
        next.pending.lsn = 9;
        next.pending.cut_identity = @splat(77);
        next.pending.change_blocks = &next_image.blocks;
        next.pending.block_digests = &next_image.block_digests;
        const next_token = try saveCached(sa, &artifacts, identity, std.testing.io, next, .none, &uploads);
        const recent = try loadWithGroups(sa, &artifacts, identity, next_token, table, now + 1, .none, &states, &groups, .{ .io = std.testing.io });
        defer recent.deinit();
        try std.testing.expect(old.lease.?.item != recent.lease.?.item);
        var reused: usize = 0;
        for (old.lease.?.item.foreign_dependencies) |previous_group| for (recent.lease.?.item.foreign_dependencies) |next_group| {
            if (previous_group.item == next_group.item) {
                reused += 1;
                const shared: *const DecodedGroup = @ptrCast(@alignCast(previous_group.item.payload.extension));
                try std.testing.expect(shared.image != null);
                for (shared.image.?.parts, 0..) |part, index| if (part) |held| {
                    try std.testing.expect(old.overlay.?.rows.image.?.parts[index].? == held);
                    try std.testing.expect(recent.overlay.?.rows.image.?.parts[index].? == held);
                };
            }
        };
        try std.testing.expectEqual(@as(usize, 1), reused);
        try std.testing.expectEqual(@as(usize, 2), old.overlay.?.rows.count());
        try std.testing.expectEqual(@as(usize, 2), recent.overlay.?.rows.count());
        // Eviction cannot free groups pinned by either independent cut.
        groups.max_bytes = 0;
        const evict = try groups.create(128);
        defer evict.release();
        groups.publish(@splat(88), evict);
        try std.testing.expectEqual(@as(usize, 2), old.overlay.?.rows.count());
        try std.testing.expectError(error.DeadlineExceeded, loadWithGroups(sa, &artifacts, identity, next_token, table, now + 1, .none, &states, &groups, .{ .deadline_ns = 1 }));
    }
    {
        // Legacy issued cuts can exceed cache residency while remaining valid.
        // The same image uses preallocated native values in new row groups.
        var wide_arena = std.heap.ArenaAllocator.init(a);
        defer wide_arena.deinit();
        const wa = wide_arena.allocator();
        const elements = try wa.alloc(std.json.Value, 125000);
        @memset(elements, .{ .integer = 1 });
        var row: std.json.Value = .{ .object = .empty };
        try row.object.put(wa, "id", .{ .integer = 100 });
        try row.object.put(wa, "body", .{ .string = "wide accepted row" });
        try row.object.put(wa, "payload", .{ .array = .fromOwnedSlice(wa, elements) });
        const changes = [_]ingestion.Batch.Change{.{ .op = .upsert, .row = row }};
        const native_payload = try encodeChanges(wa, &changes);
        try std.testing.expectError(error.InvalidQueryRequest, decodeChanges(wa, native_payload[0 .. native_payload.len - 1]));
        const invalid_payload = try wa.dupe(u8, native_payload);
        invalid_payload[14] = 'X'; // invalid native value magic
        try std.testing.expectError(error.InvalidQueryRequest, decodeChanges(wa, invalid_payload));
        const big = try images.Image.update(a, null, .{ .lsn = 10, .key_fields = &.{"id"}, .changes = &changes });
        defer big.release();
        var image_bytes: usize = 0;
        for (big.parts) |part| if (part) |held| {
            image_bytes += held.budget.live;
        };
        try std.testing.expect(image_bytes < 16 * 1024 * 1024);
        var wide = descriptor;
        wide.pending = .{ .lsn = 10, .cut_identity = @splat(77), .key_identity = .canonical, .key_fields = big.key_fields, .change_blocks = &big.blocks, .block_digests = &big.block_digests };
        var decoded: local.serverless_query_lake_decoded_cache.Cache = .{ .a = a };
        defer decoded.deinit();
        const native_token = try save(wa, &artifacts, identity, std.testing.io, wide, .none);
        const native_pin = try loadCached(wa, &artifacts, identity, native_token, table, now + 1, .none, &decoded, .{ .io = std.testing.io });
        defer native_pin.deinit();
        try std.testing.expectEqual(@as(usize, 125000), native_pin.overlay.?.row(try ingestion.rowId(wa, &.{"id"}, row, .canonical)).?.object.get("payload").?.array.items.len);
        try std.testing.expect(native_pin.lease.?.item.budget.live < 16 * 1024 * 1024);
        const native_envelope = try Read.envelope(wa, &artifacts, native_token);
        var legacy_state = try Read.state(wa, &artifacts, native_envelope.state);
        const scope = try stores.UploadScope.forPublication(domain(table.table_id, identity), now + payload_ttl_ms, std.testing.io);
        legacy_state.version = 1;
        legacy_state.change_parts = try wa.dupe(StateRef, &.{try upload(wa, &artifacts, scope, changes, .none)});
        var legacy_envelope = native_envelope;
        legacy_envelope.state = try upload(wa, &artifacts, scope, legacy_state, .none);
        const legacy_ref = try upload(wa, &artifacts, try stores.UploadScope.forPublication(scope.domain, descriptor.expires_ms, std.testing.io), legacy_envelope, .none);
        const legacy_token = try generations.encode(wa, prefix, .{ .artifact_id = @constCast(legacy_ref.id), .checksum = @constCast(try stores.sha256ChecksumFromArtifactId(legacy_ref.id)), .byte_len = legacy_ref.bytes, .provider_version = legacy_ref.generation });
        const legacy_pin = try loadCached(wa, &artifacts, identity, legacy_token, table, now + 1, .none, &decoded, .{ .io = std.testing.io });
        defer legacy_pin.deinit();
        try std.testing.expectEqual(@as(usize, 1), legacy_pin.descriptor.pending.count());
        try std.testing.expect(!legacy_pin.lease.?.item.cached);
        // Allocator remapping changes the legacy parser's peak across owners
        // and build modes. Explicit active-load pressure proves the private
        // replay path without depending on that incidental allocation layout.
        decoded.max_loading_bytes = 32 * 1024 * 1024;
        const constrained = try loadCached(wa, &artifacts, identity, legacy_token, table, now + 1, .none, &decoded, .{ .io = std.testing.io });
        defer constrained.deinit();
        try std.testing.expectEqual(@as(usize, 256 * 1024 * 1024), constrained.lease.?.item.budget.limit);
        try std.testing.expect(constrained.lease.?.item.budget.peak > decoded.max_loading_bytes);
        try std.testing.expect(!constrained.lease.?.item.cached);
        try std.testing.expectEqual(@as(usize, 125000), constrained.overlay.?.row(try ingestion.rowId(wa, &.{"id"}, row, .canonical)).?.object.get("payload").?.array.items.len);
    }
    const changed_row = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"id\":1000,\"body\":\"changed\"}", .{});
    const next = try images.Image.update(a, image, .{ .lsn = 9, .key_fields = image.key_fields, .changes = &.{.{ .op = .upsert, .row = changed_row }} });
    defer next.release();
    descriptor.pending = .{ .lsn = 9, .cut_identity = @splat(2), .key_identity = .canonical, .key_fields = next.key_fields, .change_blocks = &next.blocks, .block_digests = &next.block_digests };
    const updated = try saveCached(a, &artifacts, identity, std.testing.io, descriptor, .none, &cache);
    defer a.free(updated);
    const updated_envelope = try Read.envelope(sa, &artifacts, updated);
    const updated_state = try Read.state(sa, &artifacts, updated_envelope.state);
    var reused: usize = 0;
    for (old_state.change_parts) |old_part| for (updated_state.change_parts) |new_part| {
        if (std.mem.eql(u8, old_part.id, new_part.id)) reused += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), reused);
    try std.testing.expectEqual(@as(u64, 5), cache.puts);
    // A legacy descriptor retains its original JSON-array row IDs.
    var legacy = descriptor;
    const legacy_row = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"id\":-0.0,\"body\":\"legacy\"}", .{});
    legacy.pending = .{ .lsn = 8, .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = legacy_row }} };
    const legacy_ref = try upload(sa, &artifacts, try stores.UploadScope.forPublication(domain(4, identity), legacy.expires_ms, std.testing.io), legacy, .none);
    const legacy_token = try generations.encode(sa, prefix, .{ .artifact_id = @constCast(legacy_ref.id), .checksum = @constCast(try stores.sha256ChecksumFromArtifactId(legacy_ref.id)), .byte_len = legacy_ref.bytes, .provider_version = legacy_ref.generation });
    store.deinit();
    store_open = false;
    var reopened = try @import("lake_index_store.zig").Store.open(a, &config, null, false);
    defer reopened.deinit();
    reopened.implementation.cloud_io = std.testing.io;
    var reads = reopened.artifactStore();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const cursor_payload = try std.json.Stringify.valueAlloc(scratch, .{ .sources = .{.{ .remote_snapshot = token }} }, .{});
    const cursor_token = try saveCursor(scratch, &reads, identity, std.testing.io, cursor_payload, now + 20, .none);
    const cursor_split = std.mem.lastIndexOfScalar(u8, cursor_token, ':').?;
    const cursor_scope = (try stores.uploadScopeFromArtifactId(cursor_token[8..cursor_split])).?;
    try std.testing.expectEqual(now + ttl_ms, cursor_scope.fencingToken());
    try std.testing.expectEqualStrings(cursor_payload, try loadCursor(scratch, &reads, identity, cursor_token, now + 21, .none));
    try std.testing.expectError(error.CatalogGenerationChanged, loadCursor(scratch, &reads, identity, cursor_token, now + ttl_ms, .none));
    const loaded = try load(scratch, &reads, identity, token, table, now + 1, .none);
    try std.testing.expectEqual(@as(u64, 8), loaded.pending.lsn);
    try std.testing.expectEqual(ingestion.KeyIdentity.canonical, loaded.pending.key_identity);
    try std.testing.expectEqual(@as(usize, 2), loaded.pending.changes.len);
    const old_overlay = try @import("lake_search_overlay.zig").Overlay.init(scratch, loaded.pending);
    const last_id = try ingestion.rowId(scratch, &.{"id"}, last, .canonical);
    try std.testing.expectEqualStrings("last", old_overlay.row(last_id).?.object.get("body").?.string);
    const latest = try load(scratch, &reads, identity, updated, table, now + 2, .none);
    const latest_overlay = try @import("lake_search_overlay.zig").Overlay.init(scratch, latest.pending);
    try std.testing.expectEqualStrings("changed", latest_overlay.row(last_id).?.object.get("body").?.string);
    const recovered_legacy = try load(scratch, &reads, identity, legacy_token, table, now + 2, .none);
    try std.testing.expectEqual(ingestion.KeyIdentity.legacy_json, recovered_legacy.pending.key_identity);
    const legacy_overlay = try @import("lake_search_overlay.zig").Overlay.init(scratch, recovered_legacy.pending);
    const legacy_id = try ingestion.rowId(scratch, &.{"id"}, legacy_row, .legacy_json);
    const canonical_id = try ingestion.rowId(scratch, &.{"id"}, legacy_row, .canonical);
    try std.testing.expect(!std.mem.eql(u8, legacy_id, canonical_id));
    try std.testing.expectEqualStrings("legacy", legacy_overlay.row(legacy_id).?.object.get("body").?.string);
    try std.testing.expect(legacy_overlay.row(canonical_id) == null);
    const old_pending = try std.json.parseFromSliceLeaky(ingestion.Pending, scratch, "{\"lsn\":8,\"key_fields\":[\"id\"],\"changes\":[]}", .{});
    try std.testing.expectEqual(ingestion.KeyIdentity.legacy_json, old_pending.key_identity);
    // Expired envelopes can be collected while a later envelope still shares
    // their immutable state and row groups, including after process restart.
    try collect(&reads, 4, identity, now + ttl_ms + 30_001, .none);
    _ = try load(scratch, &reads, identity, fresh, table, now + ttl_ms + 30_001, .none);
    try reads.delete(old_state.change_parts[0].id);
    try std.testing.expectError(error.CatalogGenerationChanged, load(scratch, &reads, identity, fresh, table, now + ttl_ms + 30_001, .none));
    try std.testing.expectEqual(publication.generation, loaded.publication.generation);
    try std.testing.expectError(error.CatalogGenerationChanged, load(scratch, &reads, identity, token, table, now + ttl_ms, .none));
    var changed = table;
    changed.object_storage_generation = 2;
    try std.testing.expectError(error.CatalogGenerationChanged, load(scratch, &reads, identity, token, changed, now + 1, .none));
    changed = table;
    changed.indexes_json = "{\"text\":{\"type\":\"full_text\"}}";
    try std.testing.expectError(error.CatalogGenerationChanged, load(scratch, &reads, identity, token, changed, now + 1, .none));
}
