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
    version: u8 = 1,
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
    return @import("lake_wal_image.zig").digest(.{ .format = "retained-state-v2", .store = identity, .table = descriptor.table_id, .object_generation = descriptor.object_generation, .desired = descriptor.desired, .publication = descriptor.publication, .pending_identity = descriptor.pending.cut_identity orelse @import("lake_wal_image.zig").digest(descriptor.pending), .base_lsn = descriptor.pending.base_lsn, .lsn = descriptor.pending.lsn, .key_identity = descriptor.pending.key_identity, .catalog_metadata = if (descriptor.catalog_metadata) |value| .{ .version = value.version, .record_key = value.record_key, .metadata_location = value.metadata_location, .retirement_root = value.retirement_root, .metadata_json = if (value.version == null) value.metadata_json else null } else null, .published_only = descriptor.published_only, .recent = descriptor.recent, .recent_cloud = descriptor.recent_cloud, .recent_runs = descriptor.recent_runs });
}
fn upload(a: A, store: *stores.ArtifactStore, scope: stores.UploadScope, value: anytype, cancellation: Token) !StateRef {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
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
        const expiry = @max(now +| payload_ttl_ms, descriptor.expires_ms +| 1);
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
                const part_key = @import("lake_wal_image.zig").digest(.{ .format = "retained-row-group-v1", .domain = scope.domain, .keys = pending.key_fields, .key_identity = pending.key_identity, .prefix = index, .content = fingerprint });
                const part = (if (cache) |memo| try memo.get(a, part_key, descriptor.expires_ms) else null) orelse new: {
                    if (pending.change_blocks.len == 256 and pending.block_digests.len == 256 and pending.changes.len == 0) {
                        for (pending.change_blocks[index * 16 ..][0..16]) |block| try groups[index].appendSlice(a, block);
                    }
                    const ref = try upload(a, store, scope, groups[index].items, cancellation);
                    if (cache) |memo| memo.put(part_key, ref, expiry);
                    break :new ref;
                };
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
        if (cache) |memo| memo.put(key, ref, expiry);
        break :built ref;
    };
    try cancellation.check();
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
            const part = try std.json.parseFromSliceLeaky([]const ingestion.Batch.Change, a, bodies[i].?, .{ .allocate = .alloc_always });
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
pub fn load(a: A, store: *stores.ArtifactStore, store_identity: catalog.Digest, token: []const u8, table: local.common_topology_records.TableRecord, now: u64, cancellation: Token) !Descriptor {
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
        const body = try readRetained(store, lease.state, cancellation);
        defer store.allocator.free(body);
        var state = try std.json.parseFromSliceLeaky(State, a, body, .{ .allocate = .alloc_always });
        if (state.version != 1) return error.InvalidQueryRequest;
        var retained_bytes: usize = body.len;
        for (state.change_parts) |part| {
            retained_bytes = std.math.add(usize, retained_bytes, part.bytes) catch return error.QueryCandidateBudgetExceeded;
            if (retained_bytes > max_bytes) return error.QueryCandidateBudgetExceeded;
        }
        try readParts(a, store, &state, expected_domain, lease.expires_ms, cancellation, if (store.providerStore()) |object| object.cloud_io else null);
        break :modern .{ .expires_ms = lease.expires_ms, .table_id = lease.table_id, .object_generation = lease.object_generation, .desired = lease.desired, .reader_token = lease.reader_token, .publication = state.publication, .pending = state.pending, .catalog_metadata = state.catalog_metadata, .published_only = state.published_only, .recent = state.recent, .recent_cloud = state.recent_cloud, .recent_runs = state.recent_runs };
    } else return error.InvalidQueryRequest;
    if (result.expires_ms != scope.fencingToken() or result.expires_ms <= now or result.expires_ms > now +| max_ttl_ms or result.table_id != table.table_id or result.object_generation != table.object_storage_generation or !std.mem.eql(u8, &result.desired, &catalog.desiredFingerprint(table))) return error.CatalogGenerationChanged;
    try result.publication.validate();
    return result;
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
    descriptor.expires_ms += 60_000;
    descriptor.reader_token = @splat(6);
    const fresh = try saveCached(a, &artifacts, identity, std.testing.io, descriptor, .none, &cache);
    defer a.free(fresh);
    const fresh_envelope = try Read.envelope(sa, &artifacts, fresh);
    try std.testing.expectEqualStrings(old_envelope.state.id, fresh_envelope.state.id);
    try std.testing.expectEqual(descriptor.expires_ms, fresh_envelope.expires_ms);
    try std.testing.expectEqual(descriptor.reader_token, fresh_envelope.reader_token);
    try std.testing.expectEqual(@as(u64, 3), cache.puts);
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
