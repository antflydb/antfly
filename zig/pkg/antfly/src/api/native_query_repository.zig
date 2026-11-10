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

//! Complete retained native generations over the configured artifact provider.
//! Transfer uses bounded authenticated chunks, then one immutable manifest.
//! Restored filesystem trees are disposable caches of that durable authority.
const std = @import("std");
const local = @import("antfly_local_sources");
const cut = local.storage_db_native_query_cut;
const port = local.storage_db_native_query_cut_repository;
const objectstore = @import("objectstore");
const Store = @import("lake_index_store.zig").Store;
const retention_budget = @import("native_query_budget.zig");
const linked_gc = @import("native_query_link_gc.zig");
const artifacts = @import("../serverless/artifacts/store.zig");
const fs = @import("antfly_runtime_fs").fs_paths;
const A = std.mem.Allocator;
const Namespace = local.storage_db_doc_identity_namespace.Namespace;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const ChunkCache = local.serverless_query_lake_decoded_cache;
fn checkpointHintTimeoutNs(byte_limit: usize) u64 {
    // Small hints get about one second; large generation inventories also get
    // transfer headroom. Caller cancellation still wins over this five-second cap.
    return @min(5 * std.time.ns_per_s, std.time.ns_per_s + @as(u64, @intCast(byte_limit)) * std.time.ns_per_s / (1024 * 1024));
}

fn transientProviderFailure(err: anyerror) bool {
    return err == error.TlsTransportReadFailed or err == error.RemoteUnavailable or err == error.RateLimited or @import("httpx").isRetryableTransportError(err);
}

/// Snapshot operations implement reads, even when retaining immutable chunks.
/// Normalize provider failures before crossing the compilation-local error ABI.
fn repositoryFailure(comptime operation: []const u8, err: anyerror, cancellation: Cancellation) anyerror {
    cancellation.check() catch |cancelled| return cancelled;
    if (!transientProviderFailure(err)) return err;
    std.log.warn("native snapshot repository temporarily unavailable operation={s} cause={s}", .{ operation, @errorName(err) });
    return error.StorageReadTemporarilyUnavailable;
}

fn hintMiss(err: anyerror, cancellation: Cancellation) !bool {
    try cancellation.check();
    return transientProviderFailure(err) or err == error.Timeout or err == error.DeadlineExceeded or err == error.Canceled or err == error.Cancelled;
}
/// Register discovery before publishing a capability. Even a failed physical
/// capture must leave its descriptor discoverable after restart or table drop.
pub fn saveWithCover(a: A, store: *Store, io: std.Io, table: anytype, now: u64, retention_ms: u64, cover: anytype, cancellation: Cancellation) ![]const u8 {
    for (cover) |range| try registerOwner(a, store, range.namespace, cancellation);
    var artifact_store = store.artifactStore();
    const retained = @import("native_retained_cut.zig");
    return retained.saveWithCover(a, &artifact_store, try retained.storeIdentity(a, store.locator), io, table, now, retention_ms, cover, cancellation);
}
fn registerOwner(a: A, store: *Store, namespace: Namespace, cancellation: Cancellation) !void {
    try cancellation.check();
    const key = try std.fmt.allocPrint(a, "{s}{s}native-query-owners/{d}/{d}/{d}.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", namespace.table_id, namespace.shard_id, namespace.range_id });
    defer a.free(key);
    const bytes = try std.json.Stringify.valueAlloc(a, namespace, .{});
    defer a.free(bytes);
    const bridge: TokenBridge = .{ .token = cancellation };
    var result = store.opened.client.putObject(store.opened.bucket, key, bytes, .{ .if_none_match = true, .cancellation = bridge.object() }) catch |err| {
        try cancellation.check();
        switch (err) {
            error.PreconditionFailed, error.ConditionalCheckFailed => return,
            else => return err,
        }
    };
    result.deinit(store.opened.client.allocator);
    try cancellation.check();
}
const TokenBridge = struct {
    token: Cancellation,
    fn cancelled(raw: *const anyopaque) bool {
        const self: *const TokenBridge = @ptrCast(@alignCast(raw));
        self.token.check() catch return true;
        return false;
    }
    fn object(self: *const TokenBridge) objectstore.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = cancelled };
    }
};
const chunk_bytes = 4 * 1024 * 1024;
const max_generation_bytes: u64 = 64 * 1024 * 1024 * 1024;
const max_manifest_bytes = 16 * 1024 * 1024;
const cache_window_ms: u64 = std.time.ms_per_hour;
const LocalCommit = struct { version: u16 = 1, domain: [32]u8, request: cut.Request, namespace: Namespace };
const CachedFile = struct { domain: [32]u8, expires_ms: u64, source: local.storage_db_native_backup_seal.File, chunks: []const Ref };
const Ref = port.remote_storage.Chunk;
const File = port.remote_storage.File;
const PublicationResult = struct { complete: bool, proof: ?port.ReuseProof = null };
const Manifest = struct { version: u16 = 1, request: cut.Request, namespace: Namespace, sequence: u64, files: []const File };
pub const Repository = struct {
    a: A,
    store: Store,
    limits: cut.Limits = .{},
    background_warming: bool = false,
    warming_max_bytes: u64 = 256 * 1024 * 1024,
    retained_limits: cut.Limits = .{ .max_cuts = 4096 },
    owned_collection: CollectionState,
    owned_config: ?*local.common_config.Config = null,
    owned_base_dir: ?[]u8 = null,
    // Readers are short-lived, but immutable extents recur across logical cuts.
    // Keep verified payloads at the repository owner, with bounded residency and
    // process-accounted allocation. A manifest is still authorized on every open.
    chunks: ChunkCache.Cache,
    shared_chunks: ?*ChunkCache.Cache = null,
    pub fn attachResources(self: *Repository, manager: *local.storage_resource_manager.ResourceManager) void {
        self.shared_chunks = manager.nativeChunkCache(self.chunks.max_bytes);
        // Host admission failure disables optional residency, never reads.
        self.chunks.max_bytes = 0;
    }
    fn chunkCache(self: *Repository) *ChunkCache.Cache {
        return self.shared_chunks orelse &self.chunks;
    }
    /// The maintenance owner retains this across short-lived provider adapters.
    /// At most one namespace iterator and one tree reaper are open at a time.
    pub const CollectionState = struct {
        a: A,
        io: ?std.Io = null,
        mutex: std.atomic.Mutex = .unlocked,
        collector: ?linked_gc.Collector = null,
        namespace: ?Namespace = null,
        pending: std.atomic.Value(bool) = .init(false),
        pub fn init(a: A, io: std.Io) CollectionState {
            return .{ .a = a, .io = io };
        }
        pub fn deinit(self: *CollectionState) void {
            if (self.collector) |*value| value.deinit();
            self.collector = null;
            self.namespace = null;
            self.pending.store(false, .release);
        }
        pub fn hasPending(self: *const CollectionState) bool {
            return self.pending.load(.acquire);
        }
    };
    const Setup = struct { config_json: []const u8, deployment: local.common_config.DeploymentMode, base_dir: ?[]const u8 = null };
    pub fn setupJsonAlloc(a: A, config: ?*const local.common_config.Config, deployment: local.common_config.DeploymentMode, base_dir: ?[]const u8) ![]u8 {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var connections: std.json.Value = .{ .object = .empty };
        if (config) |value| {
            var iterator = value.connections.iterator();
            while (iterator.next()) |entry| {
                const external = entry.value_ptr.external_io orelse continue;
                var fields: std.json.Value = .{ .object = .empty };
                const allowed: []const []const u8 = switch (external.protocol) {
                    .s3 => &.{ "protocol", "endpoint", "region", "addressing_style", "bucket_provisioning", "buckets", "prefix", "credentials", "use_ssl" },
                    .gcs => &.{ "protocol", "endpoint", "upload_endpoint", "project_id", "bucket_provisioning", "buckets", "prefix" },
                    .filesystem => &.{ "protocol", "root" },
                    .http => &.{ "protocol", "hosts" },
                };
                inline for (@typeInfo(@TypeOf(external)).@"struct".field_names) |name| {
                    if (comptime !std.mem.eql(u8, name, "headers")) {
                        for (allowed) |accepted| {
                            if (!std.mem.eql(u8, name, accepted)) continue;
                            const encoded = try std.json.Stringify.valueAlloc(scratch, @field(external, name), .{ .emit_null_optional_fields = false });
                            if (!std.mem.eql(u8, encoded, "null")) try fields.object.put(scratch, name, try std.json.parseFromSliceLeaky(std.json.Value, scratch, encoded, .{}));
                        }
                    }
                }
                if (external.protocol == .gcs) {
                    const encoded = try std.json.Stringify.valueAlloc(scratch, external.gcs_credentials, .{ .emit_null_optional_fields = false });
                    try fields.object.put(scratch, "credentials", try std.json.parseFromSliceLeaky(std.json.Value, scratch, encoded, .{}));
                }
                if (external.protocol == .http) {
                    var headers: std.json.Value = .{ .object = .empty };
                    var header_iterator = external.headers.iterator();
                    while (header_iterator.next()) |header| try headers.object.put(scratch, header.key_ptr.*, .{ .string = header.value_ptr.* });
                    try fields.object.put(scratch, "headers", headers);
                }
                const encoded = try std.json.Stringify.valueAlloc(scratch, .{ .kind = entry.value_ptr.kind, .capabilities = entry.value_ptr.capabilities, .external_io = fields }, .{});
                try connections.object.put(scratch, entry.key_ptr.*, try std.json.parseFromSliceLeaky(std.json.Value, scratch, encoded, .{}));
            }
        }
        const bytes = if (config) |value| try std.json.Stringify.valueAlloc(a, .{ .storage = .{ .engine = "local", .artifacts = if (value.storage.artifacts.connection != null) @as(?local.common_config.Config.ObjectStorageLocation, value.storage.artifacts) else null, .local = .{ .base_dir = base_dir orelse value.storage.local_base_dir orelse "." } }, .connections = connections, .lake_cache = value.lake_cache, .lake_indexes = value.lake_indexes, .deployment_mode = deployment }, .{ .emit_null_optional_fields = false }) else try a.dupe(u8, "{}");
        defer a.free(bytes);
        return std.json.Stringify.valueAlloc(a, Setup{ .config_json = bytes, .deployment = deployment, .base_dir = base_dir }, .{});
    }
    pub fn initFromSetup(a: A, bytes: []const u8, secrets: ?*local.common_secrets.FileStore) !Repository {
        var parsed = try std.json.parseFromSlice(Setup, a, bytes, .{});
        defer parsed.deinit();
        // Store.reopen_options borrows this configuration. A heap owner keeps
        // that pointer valid when the returned repository moves into its slot.
        const config = try a.create(local.common_config.Config);
        errdefer a.destroy(config);
        config.* = try local.common_config.Config.parseFromSlice(a, parsed.value.config_json);
        errdefer config.deinit();
        config.deployment_mode = parsed.value.deployment;
        const base = if (parsed.value.base_dir) |value| try a.dupe(u8, value) else null;
        errdefer if (base) |value| a.free(value);
        var repository = try init(a, config, secrets, parsed.value.deployment, base);
        repository.owned_config = config;
        repository.owned_base_dir = base;
        return repository;
    }
    pub fn init(a: A, config: ?*const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, deployment: local.common_config.DeploymentMode, base_dir: ?[]const u8) !Repository {
        const indexes = if (config) |value| value.lake_indexes else local.common_config.Config.LakeIndexConfig{};
        return .{ .a = a, .owned_collection = .{ .a = a }, .chunks = .{ .a = a, .max_bytes = if (config) |value| if (value.lake_cache.enabled) @intCast(@min(value.lake_cache.max_memory_bytes / 4, 32 * 1024 * 1024)) else 0 else 16 * 1024 * 1024 }, .store = try Store.openNative(a, config, secrets, false, deployment, base_dir), .limits = .{ .max_cuts = indexes.query_cursors.max_native_cuts, .max_bytes = indexes.query_cursors.max_native_retained_bytes }, .warming_max_bytes = indexes.query_cursors.max_native_warm_bytes, .retained_limits = .{ .max_cuts = indexes.query_cursors.max_durable_native_cuts, .max_bytes = indexes.query_cursors.max_durable_native_bytes }, .background_warming = indexes.nativeSnapshotWarmingEnabled(deployment) };
    }
    pub fn deinit(self: *Repository) void {
        self.chunks.deinit();
        self.owned_collection.deinit();
        self.store.deinit();
        if (self.owned_config) |value| {
            value.deinit();
            self.a.destroy(value);
        }
        if (self.owned_base_dir) |value| self.a.free(value);
    }
    pub fn capability(self: *Repository) port.Port {
        if (self.store.opened.fs_client == null) return self.chunkedCapability();
        return self.configuredCapability(.{ .publish = publishLinked, .recover = recoverLinked, .open_read = openReadLinked, .reference = reference, .publish_with_proof = publishLinkedWithProof, .reuse = reuse, .can_evict = canEvictLinked });
    }
    fn chunkedCapability(self: *Repository) port.Port {
        return self.configuredCapability(.{ .publish = publish, .recover = recover, .open_read = openRead, .reference = reference, .publish_with_proof = publishWithProof, .reuse = reuse, .can_evict = canEvict });
    }
    fn configuredCapability(self: *Repository, comptime vtable: port.VTable) port.Port {
        return .{ .limits = self.limits, .warming_enabled = self.background_warming, .warming_max_bytes = self.warming_max_bytes, .ptr = self, .vtable = &vtable };
    }
    fn budgetStore(self: *Repository, a: A) !retention_budget.Store {
        return .{ .client = self.store.opened.client, .bucket = self.store.opened.bucket, .key = try std.fmt.allocPrint(a, "{s}{s}native-query-budget-v1.json", .{ self.store.opened.prefix, if (self.store.opened.prefix.len == 0) "" else "/" }), .limits = self.retained_limits };
    }
    fn reserveInventory(self: *Repository, a: A, request: cut.Request, namespace: Namespace, files: anytype, linked: bool, cancellation: Cancellation) !void {
        try request.validate(cut.nowMs());
        try self.register(a, namespace, cancellation);
        var bytes: u64 = 0;
        for (files) |file| bytes = std.math.add(u64, bytes, file.size) catch return error.QueryCandidateBudgetExceeded;
        const identity = try std.json.Stringify.valueAlloc(a, .{ namespace, files }, .{});
        const store = try self.budgetStore(a);
        try store.reserve(self.a, .{ .id = request.id[0..64].*, .namespace = namespace, .expires_ms = request.expires_ms, .bytes = bytes, .inventory = manifestChecksum(identity), .linked = linked }, cancellation);
    }
    fn reserveLocal(self: *Repository, a: A, io: std.Io, root: []const u8, request: cut.Request, namespace: Namespace, linked: bool, cancellation: Cancellation) !void {
        const path = try std.fs.path.join(a, &.{ root, "query-cut.json" });
        const bytes = try local.storage_db_native_backup.readFileAlloc(a, io, path, max_manifest_bytes);
        const manifest = try std.json.parseFromSliceLeaky(cut.Manifest, a, bytes, .{});
        try self.reserveInventory(a, request, namespace, manifest.files, linked, cancellation);
    }
    /// Filesystem repositories retain sealed files by inode, without hashing or
    /// copying the corpus on the query path. This is a second durable owner,
    /// independent of the evictable per-DB query-pin directory.
    fn linkedNamespace(self: *Repository, a: A, namespace: Namespace) ![]u8 {
        const filesystem = self.store.opened.fs_client orelse return error.Unsupported;
        const dirs = try filesystem.artifactDirectoriesAlloc(a, self.store.opened.bucket);
        defer a.free(dirs.objects);
        defer a.free(dirs.inventory);
        const identity = try self.domain(a, namespace);
        return std.fmt.allocPrint(a, "{s}/native-query-links/{s}", .{ dirs.inventory, std.fmt.bytesToHex(&identity, .lower) });
    }
    fn linkedRoot(self: *Repository, a: A, request: cut.Request, namespace: Namespace) ![]u8 {
        try request.validate(cut.nowMs());
        _ = try request.namespace(namespace);
        const parent = try self.linkedNamespace(a, namespace);
        defer a.free(parent);
        return std.fmt.allocPrint(a, "{s}/{d:0>20}/{s}", .{ parent, request.expires_ms, request.id });
    }
    fn linkedLock(a: A, io: std.Io, parent: []const u8, cancellation: Cancellation) !cut.Guard {
        try fs.createDirPathPortable(io, parent);
        const path = try std.fs.path.join(a, &.{ parent, ".lock" });
        defer a.free(path);
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = false });
        errdefer file.close(io);
        while (!try file.tryLock(io, .exclusive)) {
            try cancellation.check();
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        return .{ .io = io, .file = file };
    }
    fn linkGeneration(a: A, io: std.Io, source: []const u8, target: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !void {
        return linkGenerationAs(a, io, source, target, request, request, namespace, cancellation);
    }
    fn linkGenerationAs(a: A, io: std.Io, source: []const u8, target: []const u8, request: cut.Request, replacement: cut.Request, namespace: Namespace, cancellation: Cancellation) !void {
        try cut.validate(a, io, source, request, namespace, cancellation);
        const manifest_path = try std.fs.path.join(a, &.{ source, "query-cut.json" });
        defer a.free(manifest_path);
        const bytes = try local.storage_db_native_backup.readFileAlloc(a, io, manifest_path, max_manifest_bytes);
        defer a.free(bytes);
        var parsed = try std.json.parseFromSlice(cut.Manifest, a, bytes, .{});
        defer parsed.deinit();
        const staging = try std.fmt.allocPrint(a, "{s}.staging", .{target});
        defer a.free(staging);
        try std.Io.Dir.cwd().deleteTree(io, staging);
        try fs.createDirPathPortable(io, staging);
        errdefer std.Io.Dir.cwd().deleteTree(io, staging) catch {};
        for (parsed.value.files) |file| {
            try cancellation.check();
            const from_path = try std.fs.path.join(a, &.{ source, file.path });
            defer a.free(from_path);
            const to_path = try std.fs.path.join(a, &.{ staging, file.path });
            defer a.free(to_path);
            if (std.fs.path.dirname(to_path)) |parent| try fs.createDirPathPortable(io, parent);
            try std.Io.Dir.hardLink(.cwd(), from_path, .cwd(), to_path, io, .{});
        }
        // The manifest binds the exact linked inode/size/mtime inventory. No
        // payload reads are needed, including for very large immutable files.
        const to_manifest = try std.fs.path.join(a, &.{ staging, "query-cut.json" });
        defer a.free(to_manifest);
        try cut.validate(a, io, source, request, namespace, cancellation);
        const rebound = if (request.expires_ms == replacement.expires_ms and std.mem.eql(u8, request.id, replacement.id)) bytes else try std.json.Stringify.valueAlloc(a, cut.Manifest{ .request = replacement, .namespace = namespace, .sequence = parsed.value.sequence, .files = parsed.value.files, .reuse_digest = parsed.value.reuse_digest }, .{});
        defer if (rebound.ptr != bytes.ptr) a.free(rebound);
        _ = try local.storage_db_native_backup.writeFileDurable(io, to_manifest, rebound);
        try cut.validate(a, io, staging, replacement, namespace, cancellation);
        // Sync nested link directories before publishing their parent.
        var inventory_arena = std.heap.ArenaAllocator.init(a);
        defer inventory_arena.deinit();
        _ = try local.storage_db_native_backup_seal.inventoryAlloc(inventory_arena.allocator(), io, staging, cancellation);
        try fs.syncDirPortable(io, staging);
        try cancellation.check();
        try std.Io.Dir.rename(.cwd(), staging, .cwd(), target, io);
        try fs.syncDirPortable(io, std.fs.path.dirname(target).?);
    }
    fn publishLinked(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !void {
        _ = try publishLinkedLimited(raw, executor, root, request, namespace, cancellation, std.math.maxInt(u64), null);
    }
    fn publishLinkedWithProof(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !?port.ReuseProof {
        var proof: ?port.ReuseProof = null;
        _ = try publishLinkedLimited(raw, executor, root, request, namespace, cancellation, std.math.maxInt(u64), &proof);
        return proof;
    }
    fn publishLinkedBoundedWithProof(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation, max_bytes: u64) !PublicationResult {
        if (max_bytes < chunk_bytes or max_bytes > 32 * 1024 * 1024) return error.InvalidQueryRequest;
        var proof: ?port.ReuseProof = null;
        const complete = try publishLinkedLimited(raw, executor, root, request, namespace, cancellation, max_bytes, &proof);
        return .{ .complete = complete, .proof = proof };
    }
    fn publishLinkedLimited(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation, max_bytes: u64, proof: ?*?port.ReuseProof) !bool {
        var receiver = try executor.receive();
        const io = receiver.io();
        const self = from(raw);
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        try cut.validate(a, io, root, request, namespace, cancellation);
        const parent = try self.linkedNamespace(a, namespace);
        var guard = try linkedLock(a, io, parent, cancellation);
        var guarded = true;
        defer if (guarded) guard.deinit();
        try self.reserveLocal(a, io, root, request, namespace, true, cancellation);
        const target = try self.linkedRoot(a, request, namespace);
        var durable: objectstore.durable_directory.Cache = .{};
        defer durable.deinit(a);
        try durable.ensure(a, io, std.fs.path.dirname(target).?);
        cut.validate(a, io, target, request, namespace, cancellation) catch |err| switch (err) {
            error.CatalogGenerationChanged => {
                linkGeneration(a, io, root, target, request, namespace, cancellation) catch |link_err| switch (link_err) {
                    // A separately mounted filesystem artifact root retains
                    // the chunked transport rather than failing valid queries.
                    error.CrossDevice, error.OperationUnsupported, error.LinkQuotaExceeded => {
                        // Chunk publication has its own immutable commit. Do
                        // not serialize unrelated cursors behind payload I/O.
                        guard.deinit();
                        guarded = false;
                        return publishLimitedImpl(raw, io, root, request, namespace, cancellation, max_bytes, proof);
                    },
                    else => return link_err,
                };
                if (proof) |out| out.* = try linkedProof(a, io, target, request, namespace, cancellation);
                return true;
            },
            else => return err,
        };
        const source_manifest = try local.storage_db_native_backup.readFileAlloc(a, io, try std.fs.path.join(a, &.{ root, "query-cut.json" }), max_manifest_bytes);
        const retained_manifest = try local.storage_db_native_backup.readFileAlloc(a, io, try std.fs.path.join(a, &.{ target, "query-cut.json" }), max_manifest_bytes);
        if (!std.mem.eql(u8, source_manifest, retained_manifest)) return error.CatalogGenerationChanged;
        if (proof) |out| out.* = try linkedProof(a, io, target, request, namespace, cancellation);
        return true;
    }
    fn linkedProof(a: A, io: std.Io, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !?port.ReuseProof {
        const digest = (try cut.validateWithSourceProof(a, io, root, request, namespace, cancellation)) orelse return null;
        const path = try std.fs.path.join(a, &.{ root, "query-cut.json" });
        defer a.free(path);
        const bytes = try local.storage_db_native_backup.readFileAlloc(a, io, path, max_manifest_bytes);
        defer a.free(bytes);
        var manifest = try std.json.parseFromSlice(cut.Manifest, a, bytes, .{});
        defer manifest.deinit();
        return .{ .sequence = manifest.value.sequence, .digest = digest, .manifest_checksum = manifestChecksum(bytes) };
    }
    fn reuseLinked(self: *Repository, a: A, io: std.Io, previous: cut.Request, request: cut.Request, namespace: Namespace, proof: port.ReuseProof, cancellation: Cancellation) !?port.ReuseProof {
        var guard = try linkedLock(a, io, try self.linkedNamespace(a, namespace), cancellation);
        defer guard.deinit();
        const source = self.linkedRoot(a, previous, namespace) catch |err| switch (err) {
            error.CatalogGenerationChanged => return null,
            else => return err,
        };
        const held = linkedProof(a, io, source, previous, namespace, cancellation) catch |err| switch (err) {
            error.CatalogGenerationChanged, error.FileNotFound => return null,
            else => return err,
        };
        const current = held orelse return null;
        if (!std.meta.eql(current, proof)) return null;
        try self.reserveLocal(a, io, source, request, namespace, true, cancellation);
        const target = try self.linkedRoot(a, request, namespace);
        try fs.createDirPathPortable(io, std.fs.path.dirname(target).?);
        cut.validate(a, io, target, request, namespace, cancellation) catch |err| switch (err) {
            error.CatalogGenerationChanged => linkGenerationAs(a, io, source, target, previous, request, namespace, cancellation) catch |link_err| {
                try cancellation.check();
                // A source may expire during the bounded link operation. Its
                // optional proof cannot revoke the caller's fresh request.
                if (previous.expires_ms <= cut.nowMs()) return null;
                return link_err;
            },
            else => return err,
        };
        return linkedProof(a, io, target, request, namespace, cancellation);
    }
    // Missing caches read retained files in place, even across filesystems.
    // A shared expiry lease protects lazy reads and any escaped page sources.
    fn openReadLinked(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !?port.StorageLease {
        var receiver = try executor.receive();
        const io = receiver.io();
        const self = from(raw);
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const now = @import("antfly_platform").time.monotonicNs();
        const expiry = now +| (request.expires_ms -| cut.nowMs()) * std.time.ns_per_ms;
        const control: cut.Control = .{ .parent = cancellation, .deadline_ns = if (request.timeout_ms) |value| @min(expiry, now +| value *| std.time.ns_per_ms) else expiry };
        var guard = try linkedLock(a, io, try self.linkedNamespace(a, namespace), control.token());
        var guarded = true;
        defer if (guarded) guard.deinit();
        const physical = try self.linkedRoot(a, request, namespace);
        cut.validate(a, io, physical, request, namespace, control.token()) catch |err| switch (err) {
            error.CatalogGenerationChanged => {
                guard.deinit();
                guarded = false;
                const fallback = try openRead(raw, executor, root, request, namespace, cancellation);
                if (fallback != null or !request.create) return fallback;
                // Missing publication is a normal create probe. A present but
                // invalid immutable generation cannot be recaptured from live
                // state under the same identity.
                const manifest_path = try std.fs.path.join(a, &.{ physical, "query-cut.json" });
                std.Io.Dir.cwd().access(io, manifest_path, .{}) catch |access_err| switch (access_err) {
                    error.FileNotFound => return null,
                    else => return access_err,
                };
                return error.CatalogGenerationChanged;
            },
            else => return err,
        };
        const lease_path = try std.fs.path.join(a, &.{ std.fs.path.dirname(physical).?, ".lease" });
        const lease_file = try std.Io.Dir.cwd().createFile(io, lease_path, .{ .truncate = false });
        errdefer lease_file.close(io);
        if (!try lease_file.tryLock(io, .shared)) return error.QueryCandidateBudgetExceeded;
        const bytes = try local.storage_db_native_backup.readFileAlloc(a, io, try std.fs.path.join(a, &.{ physical, "query-cut.json" }), max_manifest_bytes);
        const manifest = try std.json.parseFromSliceLeaky(cut.Manifest, a, bytes, .{});
        const reader = try port.remote_storage.Reader.createLinkedWithExecutor(self.a, executor, root, physical, manifest.files, lease_file, control);
        return reader.lease();
    }
    fn recoverLinked(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !void {
        var receiver = try executor.receive();
        const io = receiver.io();
        const self = from(raw);
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        var guard = try linkedLock(a, io, try self.linkedNamespace(a, namespace), cancellation);
        var guarded = true;
        defer if (guarded) guard.deinit();
        const source = try self.linkedRoot(a, request, namespace);
        cut.validate(a, io, source, request, namespace, cancellation) catch |err| switch (err) {
            error.CatalogGenerationChanged => {
                guard.deinit();
                guarded = false;
                return recover(raw, executor, root, request, namespace, cancellation);
            },
            else => return err,
        };
        try linkGeneration(a, io, source, root, request, namespace, cancellation);
    }
    fn canEvictLinked(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, sequence: u64, cancellation: Cancellation) !bool {
        var receiver = try executor.receive();
        const self = from(raw);
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const path = try self.linkedRoot(a, request, namespace);
        cut.validate(a, receiver.io(), path, request, namespace, cancellation) catch |err| switch (err) {
            error.CatalogGenerationChanged => return canEvict(raw, executor, root, request, namespace, sequence, cancellation),
            else => return err,
        };
        const bytes = try local.storage_db_native_backup.readFileAlloc(a, receiver.io(), try std.fs.path.join(a, &.{ path, "query-cut.json" }), max_manifest_bytes);
        const manifest = try std.json.parseFromSliceLeaky(cut.Manifest, a, bytes, .{});
        return manifest.sequence == sequence;
    }
    fn collectLinked(self: *Repository, state: *CollectionState, namespace: Namespace, now: u64, budget: usize, cancellation: Cancellation) !linked_gc.Page {
        if (self.store.opened.fs_client == null) return .{ .complete = true };
        try cancellation.check();
        const parent = try self.linkedNamespace(state.a, namespace);
        defer state.a.free(parent);
        if (state.collector) |value| if (!state.namespace.?.eql(namespace) or !std.mem.eql(u8, value.root, parent)) state.deinit();
        errdefer state.deinit();
        if (state.collector == null) {
            const io = state.io orelse self.store.opened.fs_client.?.io;
            try fs.createDirPathPortable(io, parent);
            state.collector = try linked_gc.Collector.init(state.a, io, parent);
            if (state.collector == null) return .{ .complete = true }; // Another collector owns this namespace.
            state.namespace = namespace;
        }
        const page = state.collector.?.step(now, budget, cancellation) catch |err| switch (err) {
            // Another collector may have advanced the durable trash between
            // our pages. Reopen discovery; no private cursor is authority.
            error.FileNotFound => {
                state.deinit();
                state.pending.store(true, .release);
                return .{ .complete = false };
            },
            else => return err,
        };
        state.pending.store(!page.complete or page.visited == budget, .release);
        return page;
    }
    fn from(raw: *anyopaque) *Repository {
        return @ptrCast(@alignCast(raw));
    }
    fn key(self: *Repository, a: A, request: cut.Request, namespace: Namespace) ![]u8 {
        return std.fmt.allocPrint(a, "{s}{s}native-query-generations/{d}/{d}/{d}/{d:0>20}/{s}.json", .{ self.store.opened.prefix, if (self.store.opened.prefix.len == 0) "" else "/", namespace.table_id, namespace.shard_id, namespace.range_id, request.expires_ms, request.id });
    }
    fn domain(self: *Repository, a: A, namespace: Namespace) ![32]u8 {
        const encoded = try std.json.Stringify.valueAlloc(a, .{ .kind = "native-query-generation-v1", .locator = self.store.locator, .namespace = namespace }, .{});
        defer a.free(encoded);
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(encoded, &digest, .{});
        return digest;
    }
    fn readManifest(self: *Repository, a: A, request: cut.Request, namespace: Namespace, cancellation: Cancellation) ![]u8 {
        return (try self.readManifestIfPresent(a, request, namespace, cancellation)) orelse error.CatalogGenerationChanged;
    }
    fn readManifestIfPresent(self: *Repository, a: A, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !?[]u8 {
        try cancellation.check();
        const bridge: TokenBridge = .{ .token = cancellation };
        const object_key = try self.key(a, request, namespace);
        defer a.free(object_key);
        var result = self.store.opened.client.getObject(self.store.opened.bucket, object_key, .{ .skip_metadata_probe = true, .max_response_bytes = max_manifest_bytes, .cancellation = bridge.object() }) catch |err| {
            try cancellation.check();
            return switch (err) {
                error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
                else => err,
            };
        };
        defer result.deinit(self.store.opened.client.allocator);
        try cancellation.check();
        return try a.dupe(u8, result.body);
    }
    fn checkedManifest(a: A, bytes: []const u8, request: cut.Request, namespace: Namespace) !Manifest {
        try request.validate(cut.nowMs());
        const manifest = std.json.parseFromSliceLeaky(Manifest, a, bytes, .{ .allocate = .alloc_always }) catch return error.CatalogGenerationChanged;
        if (manifest.version != 1 or !manifest.namespace.eql(namespace) or manifest.request.table_id != request.table_id or manifest.request.expires_ms != request.expires_ms or !std.mem.eql(u8, manifest.request.id, request.id) or manifest.files.len > local.storage_db_native_backup_seal.max_files) return error.CatalogGenerationChanged;
        if (!manifest.request.indexesEqual(request)) return error.CatalogGenerationChanged;
        var total: u64 = 0;
        for (manifest.files, 0..) |file, i| {
            if (!safePath(file.path) or (std.mem.eql(u8, file.path, "query-cut.json") or std.mem.eql(u8, file.path, "query-remote.json"))) return error.CatalogGenerationChanged;
            if (i > 0 and std.mem.order(u8, manifest.files[i - 1].path, file.path) != .lt) return error.CatalogGenerationChanged;
            total = std.math.add(u64, total, file.size) catch return error.QueryCandidateBudgetExceeded;
            if (total > max_generation_bytes) return error.QueryCandidateBudgetExceeded;
            var size: u64 = 0;
            for (file.chunks) |ref| {
                if (ref.byte_len == 0 or ref.byte_len > chunk_bytes) return error.CatalogGenerationChanged;
                artifacts.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum) catch return error.CatalogGenerationChanged;
                size = std.math.add(u64, size, ref.byte_len) catch return error.CatalogGenerationChanged;
            }
            if (size != file.size) return error.CatalogGenerationChanged;
        }
        return manifest;
    }
    pub fn collect(self: *Repository, namespace: Namespace, now: u64, cancellation: Cancellation) !void {
        _ = try self.collectBounded(&self.owned_collection, namespace, now, 128, cancellation);
    }
    fn collectBounded(self: *Repository, state: *CollectionState, namespace: Namespace, now: u64, budget: usize, cancellation: Cancellation) !bool {
        if (!state.mutex.tryLock()) return false;
        defer state.mutex.unlock();
        if (budget == 0) {
            state.deinit();
            return true;
        }
        const links = try self.collectLinked(state, namespace, now, budget, cancellation);
        if (!links.complete or links.visited == budget) return false;
        defer state.deinit();
        const remaining_budget = budget - links.visited;
        const Visitor = struct {
            store: *artifacts.ArtifactStore,
            repository: *Repository,
            deleted: usize = 0,
            fn visit(raw: *anyopaque, _: artifacts.UploadScope, id: []const u8) !void {
                const visitor: *@This() = @ptrCast(@alignCast(raw));
                try visitor.store.delete(id);
                visitor.repository.chunkCache().discard(visitor.repository.chunkKey(id));
                visitor.deleted += 1;
            }
        };
        var store = self.store.artifactStore();
        const cut_identity = try @import("native_retained_cut.zig").storeIdentity(self.a, self.store.locator);
        const descriptors = try @import("native_retained_cut.zig").collectBounded(&store, namespace.table_id, cut_identity, now, remaining_budget, cancellation);
        if (descriptors == remaining_budget) return true;
        var visitor: Visitor = .{ .store = &store, .repository = self, .deleted = descriptors };
        var uploads_complete = true;
        store.visitScopedUploads(try self.domain(self.a, namespace), .{ .ptr = &visitor, .visit = Visitor.visit, .fencing_cutoff = now -| 30_000, .max_entries = remaining_budget - descriptors }, cancellation) catch |err| switch (err) {
            error.ArtifactEnumerationPaused => {
                uploads_complete = false;
            },
            else => return err,
        };
        if (visitor.deleted >= remaining_budget) return true;
        const remaining = remaining_budget - visitor.deleted;
        const prefix = try std.fmt.allocPrint(self.a, "{s}{s}native-query-generations/{d}/{d}/{d}/", .{ self.store.opened.prefix, if (self.store.opened.prefix.len == 0) "" else "/", namespace.table_id, namespace.shard_id, namespace.range_id });
        defer self.a.free(prefix);
        const bridge: TokenBridge = .{ .token = cancellation };
        var page = self.store.opened.client.listObjects(self.store.opened.bucket, .{ .prefix = prefix, .recursive = true, .max_keys = @intCast(@min(128, remaining)), .cancellation = bridge.object() }) catch |err| {
            try cancellation.check();
            return err;
        };
        defer page.deinit(self.store.opened.client.allocator);
        for (page.entries) |entry| {
            try cancellation.check();
            if (!std.mem.startsWith(u8, entry.key, prefix)) return error.CatalogGenerationChanged;
            const suffix = entry.key[prefix.len..];
            if (suffix.len < 21 or suffix[20] != '/') return error.CatalogGenerationChanged;
            const expires = std.fmt.parseInt(u64, suffix[0..20], 10) catch return error.CatalogGenerationChanged;
            // Keep the namespace witness until even its longest shared
            // chunk horizon is retired; orphan uploads remain discoverable.
            if (expires +| 2 * cache_window_ms +| 30_000 >= now) continue;
            self.store.opened.client.deleteObject(self.store.opened.bucket, entry.key, .{ .cancellation = bridge.object() }) catch |err| {
                try cancellation.check();
                return err;
            };
        }
        try self.releaseReclaimedBudget(state.io, namespace, now, remaining, uploads_complete, cancellation);
        try cancellation.check();
        return true;
    }
    fn releaseReclaimedBudget(self: *Repository, io: ?std.Io, namespace: Namespace, now: u64, max_entries: usize, uploads_complete: bool, cancellation: Cancellation) !void {
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const store = try self.budgetStore(a);
        if (max_entries == 0) return;
        const scan = try store.scan(a, namespace, cancellation);
        var entries: std.ArrayList(retention_budget.Entry) = .empty;
        for (scan.entries) |entry| {
            if (!entry.namespace.eql(namespace)) continue;
            const horizon = if (entry.linked) @as(u64, 30_000) else 3 * cache_window_ms + 30_000;
            if (entry.expires_ms +| horizon >= now) continue;
            try entries.append(a, entry);
        }
        if (entries.items.len == 0) return;
        std.mem.sort(retention_budget.Entry, entries.items, {}, struct {
            fn less(_: void, left: retention_budget.Entry, right: retention_budget.Entry) bool {
                return std.mem.lessThan(u8, &left.id, &right.id);
            }
        }.less);
        var start: usize = 0;
        if (scan.after) |after| {
            while (start < entries.items.len and !std.mem.lessThan(u8, &after, &entries.items[start].id)) start += 1;
            if (start == entries.items.len) start = 0;
        }
        var proven: std.ArrayList(retention_budget.Entry) = .empty;
        var last: ?[64]u8 = null;
        const bridge: TokenBridge = .{ .token = cancellation };
        for (0..@min(max_entries, entries.items.len)) |offset| {
            try cancellation.check();
            const entry = entries.items[(start + offset) % entries.items.len];
            last = entry.id;
            if (self.store.opened.fs_client) |filesystem| {
                const parent = try self.linkedNamespace(a, namespace);
                const expiry = try std.fmt.allocPrint(a, "{d:0>20}", .{entry.expires_ms});
                const physical_io = io orelse filesystem.io;
                var present = false;
                for ([_][]const u8{ try std.fs.path.join(a, &.{ parent, expiry }), try std.fs.path.join(a, &.{ parent, ".trash", expiry }) }) |path| {
                    std.Io.Dir.cwd().access(physical_io, path, .{}) catch |err| switch (err) {
                        error.FileNotFound => continue,
                        else => return err,
                    };
                    present = true;
                }
                if (present) continue;
                if (entry.linked) {
                    try proven.append(a, entry);
                    continue;
                }
            }
            if (!entry.linked) {
                if (!uploads_complete) continue;
                const request: cut.Request = .{ .id = &entry.id, .table_id = namespace.table_id, .expires_ms = entry.expires_ms };
                var manifest = self.store.opened.client.getObject(self.store.opened.bucket, try self.key(a, request, namespace), .{ .max_response_bytes = max_manifest_bytes, .cancellation = bridge.object() }) catch |err| switch (err) {
                    error.FileNotFound, error.NotFound, error.ObjectNotFound => {
                        try proven.append(a, entry);
                        continue;
                    },
                    else => return err,
                };
                manifest.deinit(self.store.opened.client.allocator);
            }
        }
        if (last) |after| try store.releaseBatch(self.a, proven.items, .{ .namespace = namespace, .expected_after = scan.after, .after = after }, cancellation);
    }
    fn ownerPrefix(self: *Repository, a: A) ![]u8 {
        return std.fmt.allocPrint(a, "{s}{s}native-query-owners/", .{ self.store.opened.prefix, if (self.store.opened.prefix.len == 0) "" else "/" });
    }
    fn register(self: *Repository, a: A, namespace: Namespace, cancellation: Cancellation) !void {
        try registerOwner(a, &self.store, namespace, cancellation);
    }
    /// One namespace per supervised pass. The caller owns the returned cursor;
    /// registry witnesses survive failed upload, owner loss and table drop.
    pub fn collectNextWithState(self: *Repository, a: A, state: *CollectionState, after: ?[]const u8, now: u64, budget: usize, cancellation: Cancellation) !?[]u8 {
        std.debug.assert(state.io != null); // Borrowed state outlives this adapter's executor.
        return self.collectNextImpl(a, state, after, now, budget, cancellation);
    }
    pub fn collectNext(self: *Repository, a: A, after: ?[]const u8, now: u64, budget: usize, cancellation: Cancellation) !?[]u8 {
        return self.collectNextImpl(a, &self.owned_collection, after, now, budget, cancellation);
    }
    fn collectNextImpl(self: *Repository, a: A, state: *CollectionState, after: ?[]const u8, now: u64, budget: usize, cancellation: Cancellation) !?[]u8 {
        const prefix = try self.ownerPrefix(a);
        defer a.free(prefix);
        const bridge: TokenBridge = .{ .token = cancellation };
        var page = self.store.opened.client.listObjects(self.store.opened.bucket, .{ .prefix = prefix, .recursive = true, .max_keys = 1, .start_after = after, .cancellation = bridge.object() }) catch |err| {
            try cancellation.check();
            return err;
        };
        defer page.deinit(self.store.opened.client.allocator);
        if (page.entries.len == 0) {
            if (state.mutex.tryLock()) {
                defer state.mutex.unlock();
                state.deinit();
            }
            return null;
        }
        const key_bytes = page.entries[0].key;
        if (!std.mem.startsWith(u8, key_bytes, prefix) or !std.mem.endsWith(u8, key_bytes, ".json")) return error.CatalogGenerationChanged;
        var parts = std.mem.splitScalar(u8, key_bytes[prefix.len .. key_bytes.len - 5], '/');
        const table = std.fmt.parseInt(u64, parts.next() orelse return error.CatalogGenerationChanged, 10) catch return error.CatalogGenerationChanged;
        const shard = std.fmt.parseInt(u64, parts.next() orelse return error.CatalogGenerationChanged, 10) catch return error.CatalogGenerationChanged;
        const range = std.fmt.parseInt(u64, parts.next() orelse return error.CatalogGenerationChanged, 10) catch return error.CatalogGenerationChanged;
        if (parts.next() != null or table == 0) return error.CatalogGenerationChanged;
        if (!try self.collectBounded(state, .{ .table_id = table, .shard_id = shard, .range_id = range }, now, budget, cancellation))
            return if (after) |previous| try a.dupe(u8, previous) else null;
        try cancellation.check();
        return if (page.next_continuation_token != null) try a.dupe(u8, key_bytes) else null;
    }
    fn cachedFile(a: A, io: std.Io, path: []const u8, expected_domain: [32]u8, expires: u64, source: local.storage_db_native_backup_seal.File, complete: bool, needed_until: u64) !?[]const Ref {
        const bytes = local.storage_db_native_backup.readFileAlloc(a, io, path, max_manifest_bytes) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        return checkedExtent(a, bytes, expected_domain, expires, source, complete, needed_until);
    }
    fn checkedExtent(a: A, bytes: []const u8, expected_domain: [32]u8, expires: u64, source: local.storage_db_native_backup_seal.File, complete: bool, needed_until: u64) !?[]const Ref {
        const cached = std.json.parseFromSliceLeaky(CachedFile, a, bytes, .{ .allocate = .alloc_always }) catch return null;
        if (!std.mem.eql(u8, &cached.domain, &expected_domain) or cached.expires_ms != expires or cached.source.inode != source.inode or cached.source.size != source.size or cached.source.mtime_ns != source.mtime_ns or !std.mem.eql(u8, cached.source.path, source.path)) return null;
        var size: u64 = 0;
        for (cached.chunks) |ref| {
            if (ref.byte_len == 0 or ref.byte_len > chunk_bytes) return null;
            artifacts.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum) catch return null;
            const scope = (artifacts.uploadScopeFromArtifactId(ref.artifact_id) catch return null) orelse return null;
            if (!std.mem.eql(u8, &scope.domain, &expected_domain) or scope.fencingToken() <= needed_until) return null;
            size = std.math.add(u64, size, ref.byte_len) catch return null;
        }
        if (size > source.size or (complete and size != source.size)) return null;
        return cached.chunks;
    }
    fn checkpointKey(self: *Repository, a: A, namespace: Namespace, expires: u64, digest: [32]u8) ![]u8 {
        // Use the generation inventory so the existing owner registry and
        // expiry collector reclaim references after their chunk horizon.
        return std.fmt.allocPrint(a, "{s}{s}native-query-generations/{d}/{d}/{d}/{d:0>20}/extent-{s}.json", .{ self.store.opened.prefix, if (self.store.opened.prefix.len == 0) "" else "/", namespace.table_id, namespace.shard_id, namespace.range_id, expires, std.fmt.bytesToHex(&digest, .lower) });
    }
    fn checkpointObject(self: *Repository, key_: []const u8, limit: usize, cancellation: Cancellation) !?objectstore.GetResult {
        try cancellation.check();
        const bridge: TokenBridge = .{ .token = cancellation };
        return self.store.opened.client.getObject(self.store.opened.bucket, key_, .{ .skip_metadata_probe = true, .max_response_bytes = limit, .cancellation = bridge.object() }) catch |err| {
            try cancellation.check();
            return switch (err) {
                error.ObjectNotFound, error.NotFound, error.FileNotFound, error.NoSuchKey, error.ResponseTooLarge => null,
                else => err,
            };
        };
    }
    fn checkpointExtent(self: *Repository, a: A, io: std.Io, namespace: Namespace, digest: [32]u8, expected_domain: [32]u8, expires: u64, source: local.storage_db_native_backup_seal.File, needed_until: u64, cancellation: Cancellation) !?[]const Ref {
        if (source.size > max_generation_bytes) return error.QueryCandidateBudgetExceeded;
        const limit: usize = @intCast(@min(max_manifest_bytes, 65536 + (source.size / chunk_bytes + 1) * 512));
        const control: cut.Control = .{ .parent = cancellation, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| checkpointHintTimeoutNs(limit) };
        const result = self.lookupCheckpointExtent(a, io, namespace, digest, expected_domain, expires, source, needed_until, control.token()) catch |err| {
            if (try hintMiss(err, cancellation)) return null;
            return err;
        };
        try cancellation.check();
        return result;
    }
    fn lookupCheckpointExtent(self: *Repository, a: A, io: std.Io, namespace: Namespace, digest: [32]u8, expected_domain: [32]u8, expires: u64, source: local.storage_db_native_backup_seal.File, needed_until: u64, cancellation: Cancellation) !?[]const Ref {
        if (source.size > max_generation_bytes) return error.QueryCandidateBudgetExceeded;
        // An extent descriptor cannot need an unbounded response for a tiny
        // native control file. Include conservative escaped-path/header space.
        const limit: usize = @intCast(@min(max_manifest_bytes, 65536 + (source.size / chunk_bytes + 1) * 512));
        const first = try self.checkpointKey(a, namespace, expires, digest);
        defer a.free(first);
        if (try self.checkpointObject(first, limit, cancellation)) |value| {
            var object = value;
            defer object.deinit(self.store.opened.client.allocator);
            if (try checkedExtent(a, object.body, expected_domain, expires, source, true, needed_until)) |refs| return refs;
        }
        // Keep the ordinary positive path at one GET. Only after a miss, look
        // in adjacent horizons in one bounded wave rather than serial RTTs.
        const epochs = [_]u64{ expires +| cache_window_ms, expires -| cache_window_ms };
        var keys: [2][]const u8 = undefined;
        var initialized: usize = 0;
        defer for (keys[0..initialized]) |key_| a.free(key_);
        for (&keys, epochs) |*key_, epoch| {
            key_.* = try self.checkpointKey(a, namespace, epoch, digest);
            initialized += 1;
        }
        var results: [2]?objectstore.GetResult = @splat(null);
        defer for (&results) |*result| if (result.*) |*object| object.deinit(self.store.opened.client.allocator);
        var failures: [2]?anyerror = @splat(null);
        const Worker = struct {
            fn run(repository: *Repository, key_: []const u8, maximum: usize, token: Cancellation, result: *?objectstore.GetResult, failure: *?anyerror) void {
                result.* = repository.checkpointObject(key_, maximum, token) catch |err| {
                    failure.* = err;
                    return;
                };
            }
        };
        var group: std.Io.Group = .init;
        for (0..2) |i| group.async(io, Worker.run, .{ self, keys[i], limit, cancellation, &results[i], &failures[i] });
        group.await(io) catch |err| {
            group.cancel(io);
            return err;
        };
        try cancellation.check();
        // A transient error in one optional lookup must not hide a terminal
        // authorization/protocol error returned by its sibling.
        for (failures) |failure| if (failure) |err| {
            if (!try hintMiss(err, cancellation)) return err;
        };
        for (epochs, results, failures) |epoch, object, failure| {
            if (failure) |err| return err;
            if (object) |value| if (try checkedExtent(a, value.body, expected_domain, epoch, source, true, needed_until)) |refs| return refs;
        }
        return null;
    }
    fn publishCheckpoint(self: *Repository, a: A, namespace: Namespace, expires: u64, digest: [32]u8, bytes: []const u8, cancellation: Cancellation) !void {
        // A hint is neither a commit nor required authority. Ambiguous hint
        // writes are safe to leave alone; the required manifest owns the chunks.
        const control: cut.Control = .{ .parent = cancellation, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| checkpointHintTimeoutNs(bytes.len) };
        self.publishCheckpointHint(a, namespace, expires, digest, bytes, control.token()) catch |err| {
            if (try hintMiss(err, cancellation)) return;
            return err;
        };
        try cancellation.check();
    }
    fn publishCheckpointHint(self: *Repository, a: A, namespace: Namespace, expires: u64, digest: [32]u8, bytes: []const u8, cancellation: Cancellation) !void {
        const bridge: TokenBridge = .{ .token = cancellation };
        const location = try self.checkpointKey(a, namespace, expires, digest);
        defer a.free(location);
        var result = self.store.opened.client.putObject(self.store.opened.bucket, location, bytes, .{ .if_none_match = true, .cancellation = bridge.object() }) catch |err| {
            try cancellation.check();
            switch (err) {
                error.PreconditionFailed, error.ConditionalCheckFailed => {
                    var previous = try self.store.opened.client.getObject(self.store.opened.bucket, location, .{ .skip_metadata_probe = true, .max_response_bytes = max_manifest_bytes, .cancellation = bridge.object() });
                    defer previous.deinit(self.store.opened.client.allocator);
                    if (!std.mem.eql(u8, previous.body, bytes)) return error.CatalogGenerationChanged;
                    return;
                },
                else => return err,
            }
        };
        result.deinit(self.store.opened.client.allocator);
        try cancellation.check();
    }
    fn cachedExtent(a: A, io: std.Io, parent: []const u8, digest: [32]u8, expected_domain: [32]u8, expires: u64, source: local.storage_db_native_backup_seal.File, complete: bool, needed_until: u64) !?[]const Ref {
        // Warming uses a longer private cut than an ordinary public cursor.
        // Reuse neighboring physical retention epochs only after validating
        // every immutable chunk's actual horizon against this logical reader.
        for ([_]u64{ expires, expires +| cache_window_ms, expires -| cache_window_ms }) |epoch| {
            const path = try std.fmt.allocPrint(a, "{s}/.chunk-cache/{d}/{s}.json", .{ parent, epoch, std.fmt.bytesToHex(&digest, .lower) });
            if (try cachedFile(a, io, path, expected_domain, epoch, source, complete, needed_until)) |refs| return refs;
        }
        return null;
    }
    fn safePath(path: []const u8) bool {
        return path.len != 0 and !std.fs.path.isAbsolute(path) and std.mem.indexOf(u8, path, "..") == null and std.mem.indexOfAny(u8, path, "\\\x00") == null;
    }
    fn localCommit(self: *Repository, a: A, io: std.Io, root: []const u8, request: cut.Request, namespace: Namespace) !bool {
        const path = try std.fmt.allocPrint(a, "{s}/query-remote.json", .{root});
        const bytes = local.storage_db_native_backup.readFileAlloc(a, io, path, 4096) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        const marked = std.json.parseFromSliceLeaky(LocalCommit, a, bytes, .{}) catch return false;
        const expected_domain = try self.domain(a, namespace);
        return marked.version == 1 and marked.namespace.eql(namespace) and marked.request.table_id == request.table_id and marked.request.expires_ms == request.expires_ms and std.mem.eql(u8, marked.request.id, request.id) and std.mem.eql(u8, &marked.domain, &expected_domain);
    }
    fn canEvict(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, sequence: u64, cancellation: Cancellation) !bool {
        return canEvictImpl(raw, executor, root, request, namespace, sequence, cancellation) catch |err| return repositoryFailure("can_evict", err, cancellation);
    }
    fn canEvictImpl(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, sequence: u64, cancellation: Cancellation) !bool {
        var receiver = try executor.receive();
        const io = receiver.io();
        const self = from(raw);
        try cancellation.check();
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        if (!try self.localCommit(a, io, root, request, namespace)) return false;
        const bytes = self.readManifest(a, request, namespace, cancellation) catch |err| switch (err) {
            error.CatalogGenerationChanged => return false,
            else => return err,
        };
        const manifest = checkedManifest(a, bytes, request, namespace) catch |err| switch (err) {
            error.CatalogGenerationChanged => return false,
            else => return err,
        };
        if (manifest.sequence != sequence) return false;
        _ = try self.validateManifestAuthority(a, manifest, request, namespace);
        try cancellation.check();
        return true;
    }
    fn markCommitted(self: *Repository, a: A, io: std.Io, root: []const u8, request: cut.Request, namespace: Namespace) !void {
        const path = try std.fmt.allocPrint(a, "{s}/query-remote.json", .{root});
        const bytes = try std.json.Stringify.valueAlloc(a, LocalCommit{ .domain = try self.domain(a, namespace), .request = request, .namespace = namespace }, .{});
        _ = try local.storage_db_native_backup.writeFileDurable(io, path, bytes);
    }
    fn publish(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !void {
        var receiver = try executor.receive();
        const io = receiver.io();
        _ = publishLimited(raw, io, root, request, namespace, cancellation, std.math.maxInt(u64)) catch |err| return repositoryFailure("publish", err, cancellation);
    }
    fn publishChunkedBounded(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation, max_bytes: u64) !bool {
        var receiver = try executor.receive();
        const io = receiver.io();
        if (max_bytes < chunk_bytes or max_bytes > 32 * 1024 * 1024) return error.InvalidQueryRequest;
        return publishLimited(raw, io, root, request, namespace, cancellation, max_bytes) catch |err| return repositoryFailure("bounded_publication", err, cancellation);
    }
    fn publishWithProof(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !?port.ReuseProof {
        var receiver = try executor.receive();
        const io = receiver.io();
        var proof: ?port.ReuseProof = null;
        _ = publishLimitedImpl(raw, io, root, request, namespace, cancellation, std.math.maxInt(u64), &proof) catch |err| return repositoryFailure("publish_with_proof", err, cancellation);
        return proof;
    }
    fn manifestChecksum(bytes: []const u8) [32]u8 {
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes, &digest, .{});
        return digest;
    }
    fn publishLimited(raw: *anyopaque, io: std.Io, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation, max_bytes: u64) !bool {
        return publishLimitedImpl(raw, io, root, request, namespace, cancellation, max_bytes, null);
    }
    fn publishLimitedImpl(raw: *anyopaque, io: std.Io, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation, max_bytes: u64, proof_out: ?*?port.ReuseProof) !bool {
        const self = from(raw);
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        try cancellation.check();
        try cut.validate(a, io, root, request, namespace, cancellation);
        try self.reserveLocal(a, io, root, request, namespace, false, cancellation);
        if (try self.localCommit(a, io, root, request, namespace)) {
            try cancellation.check();
            return true;
        }
        // collectNext runs under the background maintenance owner. A new cut
        // only publishes its own generation; sweeping older cuts here makes
        // query latency depend on the size of unrelated retained history.
        // An immutable commit proves a prior attempt completed every chunk.
        if (self.readManifest(a, request, namespace, cancellation)) |bytes| {
            _ = try checkedManifest(a, bytes, request, namespace);
            try self.markCommitted(a, io, root, request, namespace);
            try cancellation.check();
            return true;
        } else |err| if (err != error.CatalogGenerationChanged) return err;
        try cut.validate(a, io, root, request, namespace, cancellation);
        const bytes = try local.storage_db_native_backup.readFileAlloc(a, io, try std.fmt.allocPrint(a, "{s}/query-cut.json", .{root}), max_manifest_bytes);
        const pinned = try std.json.parseFromSliceLeaky(cut.Manifest, a, bytes, .{});
        var store = self.store.artifactStore();
        const generation_domain = try self.domain(a, namespace);
        // Immutable SST/vector extents share one expiring chunk set across
        // query cuts. Only new extents and the bounded WAL suffix are uploaded.
        const chunk_expiry = (request.expires_ms / cache_window_ms + 2) * cache_window_ms;
        const parent = std.fs.path.dirname(root) orelse return error.InvalidQueryRequest;
        const cache_root = try std.fmt.allocPrint(a, "{s}/.chunk-cache/{d}", .{ parent, chunk_expiry });
        try fs.createDirPathPortable(io, cache_root);
        const buffer = try self.a.alloc(u8, chunk_bytes);
        defer self.a.free(buffer);
        var files: std.ArrayListUnmanaged(File) = .empty;
        var uploaded: u64 = 0;
        var total: u64 = 0;
        var reference_bytes: usize = 0;
        for (pinned.files) |file| {
            try cancellation.check();
            total = std.math.add(u64, total, file.size) catch return error.QueryCandidateBudgetExceeded;
            if (total > max_generation_bytes) return error.QueryCandidateBudgetExceeded;
            const proof = try std.json.Stringify.valueAlloc(a, .{ .domain = generation_domain, .source = file }, .{});
            var digest: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(proof, &digest, .{});
            const cache_path = try std.fmt.allocPrint(a, "{s}/{s}.json", .{ cache_root, std.fmt.bytesToHex(&digest, .lower) });
            const refs_estimate = std.math.cast(usize, (file.size / chunk_bytes + 1) * 512 + file.path.len) orelse return error.QueryCandidateBudgetExceeded;
            reference_bytes = std.math.add(usize, reference_bytes, refs_estimate) catch return error.QueryCandidateBudgetExceeded;
            if (reference_bytes > max_manifest_bytes) return error.QueryCandidateBudgetExceeded;
            if (try cachedExtent(a, io, parent, digest, generation_domain, chunk_expiry, file, true, request.expires_ms)) |cached| {
                try files.append(a, .{ .path = file.path, .size = file.size, .chunks = cached });
                continue;
            }
            // Local hints are disposable. A completed remote checkpoint can
            // prove extent coverage after hint loss without uploading it again.
            if (try self.checkpointExtent(a, io, namespace, digest, generation_domain, chunk_expiry, file, request.expires_ms, cancellation)) |cached| {
                try files.append(a, .{ .path = file.path, .size = file.size, .chunks = cached });
                const cached_bytes = try std.json.Stringify.valueAlloc(a, CachedFile{ .domain = generation_domain, .expires_ms = chunk_expiry, .source = file, .chunks = cached }, .{});
                _ = try local.storage_db_native_backup.writeFileDurable(io, cache_path, cached_bytes);
                continue;
            }
            var attempt: [16]u8 = undefined;
            std.mem.writeInt(u64, attempt[0..8], chunk_expiry, .big);
            @memcpy(attempt[8..16], digest[0..8]);
            if (std.mem.allEqual(u8, attempt[8..16], 0)) attempt[15] = 1;
            const scope: artifacts.UploadScope = .{ .domain = generation_domain, .attempt = attempt };
            var source = try std.Io.Dir.cwd().openFile(io, try std.fmt.allocPrint(a, "{s}/{s}", .{ root, file.path }), .{});
            defer source.close(io);
            var chunks: std.ArrayListUnmanaged(Ref) = .empty;
            var offset: u64 = 0;
            if (try cachedExtent(a, io, parent, digest, generation_domain, chunk_expiry, file, false, request.expires_ms)) |prefix_refs| {
                try chunks.appendSlice(a, prefix_refs);
                for (prefix_refs) |ref| offset += ref.byte_len;
            }
            while (offset < file.size) {
                try cancellation.check();
                const wanted: usize = @intCast(@min(chunk_bytes, file.size - offset));
                if (wanted > max_bytes - uploaded) {
                    const cached = try std.json.Stringify.valueAlloc(a, CachedFile{ .domain = generation_domain, .expires_ms = chunk_expiry, .source = file, .chunks = chunks.items }, .{});
                    _ = try local.storage_db_native_backup.writeFileDurable(io, cache_path, cached);
                    return false;
                }
                if (try source.readPositionalAll(io, buffer[0..wanted], offset) != wanted) return error.CatalogGenerationChanged;
                var metadata = try store.putScoped(scope, buffer[0..wanted], cancellation);
                defer metadata.deinit(store.allocator);
                try chunks.append(a, .{ .artifact_id = try a.dupe(u8, metadata.artifact_id), .byte_len = metadata.byte_len, .checksum = try a.dupe(u8, metadata.checksum) });
                offset += wanted;
                uploaded += wanted;
            }
            const refs = try chunks.toOwnedSlice(a);
            try files.append(a, .{ .path = file.path, .size = file.size, .chunks = refs });
            const cache_bytes = try std.json.Stringify.valueAlloc(a, CachedFile{ .domain = generation_domain, .expires_ms = chunk_expiry, .source = file, .chunks = refs }, .{});
            try self.publishCheckpoint(a, namespace, chunk_expiry, digest, cache_bytes, cancellation);
            // Reuse is optional; failure to persist a hint cannot weaken the
            // immutable remote generation commit that follows.
            _ = try local.storage_db_native_backup.writeFileDurable(io, cache_path, cache_bytes);
        }
        try cut.validate(a, io, root, request, namespace, cancellation);
        const manifest = try std.json.Stringify.valueAlloc(a, Manifest{ .request = request, .namespace = namespace, .sequence = pinned.sequence, .files = files.items }, .{});
        if (manifest.len > max_manifest_bytes) return error.QueryCandidateBudgetExceeded;
        try cancellation.check();
        const bridge: TokenBridge = .{ .token = cancellation };
        var result = self.store.opened.client.putObject(self.store.opened.bucket, try self.key(a, request, namespace), manifest, .{ .if_none_match = true, .cancellation = bridge.object() }) catch |err| {
            try cancellation.check();
            switch (err) {
                error.PreconditionFailed, error.ConditionalCheckFailed => {
                    const existing = try self.readManifest(a, request, namespace, cancellation);
                    if (!std.mem.eql(u8, existing, manifest)) return error.CatalogGenerationChanged;
                    try self.markCommitted(a, io, root, request, namespace);
                    try cancellation.check();
                    if (proof_out) |out| if (pinned.reuse_digest) |digest| {
                        out.* = .{ .sequence = pinned.sequence, .digest = digest, .manifest_checksum = manifestChecksum(manifest) };
                    };
                    return true;
                },
                else => return err,
            }
        };
        result.deinit(self.store.opened.client.allocator);
        try self.markCommitted(a, io, root, request, namespace);
        try cancellation.check();
        if (proof_out) |out| if (pinned.reuse_digest) |digest| {
            out.* = .{ .sequence = pinned.sequence, .digest = digest, .manifest_checksum = manifestChecksum(manifest) };
        };
        return true;
    }
    /// Honor existing bindings first. A complete remote-native checkpoint can
    /// publish a new authenticated manifest without reading/downloading/re-uploading
    /// those extents or derive a checkpoint from a partial storage view.
    fn reference(raw: *anyopaque, executor: port.Executor, request: cut.Request, namespace: Namespace, source: port.Storage, cancellation: Cancellation) !bool {
        return referenceImpl(raw, executor, request, namespace, source, cancellation) catch |err| return repositoryFailure("reference", err, cancellation);
    }
    fn referenceImpl(raw: *anyopaque, executor: port.Executor, request: cut.Request, namespace: Namespace, source: port.Storage, cancellation: Cancellation) !bool {
        var receiver = try executor.receive();
        const io = receiver.io();
        const self = from(raw);
        try request.validate(cut.nowMs());
        if (!request.create or !(try request.namespace(namespace)).eql(namespace)) return error.CatalogGenerationChanged;
        try cancellation.check();
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        // Creation is idempotent: a committed generation is authoritative,
        // even when its local cache/hint disappeared or live rows changed.
        if (self.store.opened.fs_client != null) {
            const path = try std.fs.path.join(a, &.{ try self.linkedRoot(a, request, namespace), "query-cut.json" });
            const existing = local.storage_db_native_backup.readFileAlloc(a, io, path, max_manifest_bytes) catch |err| switch (err) {
                error.FileNotFound => null,
                else => return err,
            };
            if (existing) |value| {
                const committed = std.json.parseFromSliceLeaky(cut.Manifest, a, value, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.CatalogGenerationChanged;
                try cut.validateManifestBinding(committed, request, namespace);
                return true;
            }
        }
        if (try self.readManifestIfPresent(a, request, namespace, cancellation)) |value| {
            const committed = try checkedManifest(a, value, request, namespace);
            _ = try self.validateManifestAuthority(a, committed, request, namespace);
            return true;
        }
        // Existing bindings above attest their selection. An unbound remote
        // checkpoint only attests files, so use normal capture for a subset.
        if (request.indexes != null) return false;
        const bytes = (try source.nativeCheckpointReferenceAlloc(a)) orelse return false;
        if (bytes.len > max_manifest_bytes) return error.QueryCandidateBudgetExceeded;
        const checkpoint = try std.json.parseFromSliceLeaky(port.CheckpointReference, a, bytes, .{ .allocate = .alloc_always });
        const authority = try self.domain(a, namespace);
        if (checkpoint.version != 1 or !checkpoint.namespace.eql(namespace) or !std.mem.eql(u8, &authority, &checkpoint.authority)) return error.CatalogGenerationChanged;
        const manifest = try std.json.Stringify.valueAlloc(a, Manifest{ .request = request, .namespace = namespace, .sequence = checkpoint.sequence, .files = checkpoint.files }, .{});
        if (manifest.len > max_manifest_bytes) return error.QueryCandidateBudgetExceeded;
        _ = try checkedManifest(a, manifest, request, namespace);
        for (checkpoint.files) |file| for (file.chunks) |chunk| {
            const scope = (try artifacts.uploadScopeFromArtifactId(chunk.artifact_id)) orelse return error.CatalogGenerationChanged;
            if (!std.mem.eql(u8, &scope.domain, &authority) or scope.fencingToken() < request.expires_ms or scope.fencingToken() > request.expires_ms +| 3 * cache_window_ms) return error.CatalogGenerationChanged;
        };
        try self.reserveInventory(a, request, namespace, checkpoint.files, false, cancellation);
        const bridge: TokenBridge = .{ .token = cancellation };
        var published = self.store.opened.client.putObject(self.store.opened.bucket, try self.key(a, request, namespace), manifest, .{ .if_none_match = true, .cancellation = bridge.object() }) catch |err| {
            try cancellation.check();
            if (err != error.PreconditionFailed and err != error.ConditionalCheckFailed) return err;
            if (!std.mem.eql(u8, manifest, try self.readManifest(a, request, namespace, cancellation))) return error.CatalogGenerationChanged;
            return true;
        };
        published.deinit(self.store.opened.client.allocator);
        try cancellation.check();
        return true;
    }
    fn chunkKey(self: *Repository, artifact_id: []const u8) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("native-retained-chunk-v1");
        hash.update(&self.store.cache_identity);
        hash.update(artifact_id);
        var key_: [32]u8 = undefined;
        hash.final(&key_);
        return key_;
    }
    fn readChunk(raw: *anyopaque, a: A, ref: Ref, cancellation: Cancellation) ![]u8 {
        return readChunkImpl(raw, a, ref, cancellation) catch |err| return repositoryFailure("read_chunk", err, cancellation);
    }
    fn readChunkImpl(raw: *anyopaque, a: A, ref: Ref, cancellation: Cancellation) ![]u8 {
        const self: *Repository = @ptrCast(@alignCast(raw));
        try cancellation.check();
        // Identity includes the configured credential scope as well as location.
        // Invalid references cannot alias an already verified cache entry.
        try artifacts.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
        if (ref.byte_len == 0 or ref.byte_len > chunk_bytes) return error.CatalogGenerationChanged;
        const key_ = self.chunkKey(ref.artifact_id);
        const cache_ = self.chunkCache();
        const Cached = struct { bytes: []const u8 };
        if (cache_.lookup(key_)) |lease| {
            defer lease.release();
            const cached: *const Cached = @ptrCast(@alignCast(lease.item.payload.extension));
            if (cached.bytes.len != ref.byte_len) return error.CatalogGenerationChanged;
            try cancellation.check();
            return a.dupe(u8, cached.bytes);
        }
        var store = self.store.artifactStore();
        store.allocator = a;
        const bytes = store.getVerifiedAllocWithCancellation(ref.artifact_id, @intCast(ref.byte_len), ref.checksum, cancellation) catch |err| {
            std.log.warn("retained native chunk read failed err={s} bytes={d}", .{ @errorName(err), ref.byte_len });
            return switch (err) {
                error.NotFound, error.ObjectNotFound, error.FileNotFound, error.ArtifactIntegrityMismatch => error.CatalogGenerationChanged,
                else => err,
            };
        };
        errdefer a.free(bytes);
        try cancellation.check();
        // Cache admission is optional; denied residency must preserve the normal
        // verified read. Parallel misses may duplicate I/O but publish one key.
        if (cache_.max_entries != 0 and bytes.len <= cache_.max_bytes) cache: {
            const lease = cache_.create(chunk_bytes + 64 * 1024) catch break :cache;
            defer lease.release();
            const owned = lease.item.arena.allocator();
            const cached = owned.create(Cached) catch break :cache;
            cached.* = .{ .bytes = owned.dupe(u8, bytes) catch break :cache };
            lease.item.payload = .{ .extension = cached };
            cache_.publish(key_, lease);
        }
        return bytes;
    }
    /// Rebind a proved unchanged physical generation to a new logical cursor.
    /// Extent horizons must cover the new cursor; never extend them by assertion.
    fn reuse(raw: *anyopaque, executor: port.Executor, previous: cut.Request, request: cut.Request, namespace: Namespace, proof: port.ReuseProof, cancellation: Cancellation) !?port.ReuseProof {
        return reuseImpl(raw, executor, previous, request, namespace, proof, cancellation) catch |err| return repositoryFailure("reuse", err, cancellation);
    }
    fn reuseImpl(raw: *anyopaque, executor: port.Executor, previous: cut.Request, request: cut.Request, namespace: Namespace, proof: port.ReuseProof, cancellation: Cancellation) !?port.ReuseProof {
        const self = from(raw);
        if (!previous.indexesEqual(request)) return null;
        try cancellation.check();
        try request.validate(cut.nowMs());
        if (!request.create or !(try request.namespace(namespace)).eql(namespace)) return error.CatalogGenerationChanged;
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        if (self.store.opened.fs_client != null) {
            var receiver = try executor.receive();
            if (try self.reuseLinked(a, receiver.io(), previous, request, namespace, proof, cancellation)) |renewed| return renewed;
        }
        const bytes = self.readManifest(a, previous, namespace, cancellation) catch |err| switch (err) {
            error.CatalogGenerationChanged => return null,
            else => return err,
        };
        const checksum = manifestChecksum(bytes);
        if (!std.mem.eql(u8, &checksum, &proof.manifest_checksum)) return null;
        const manifest = checkedManifest(a, bytes, previous, namespace) catch |err| switch (err) {
            error.CatalogGenerationChanged => return null,
            else => return err,
        };
        if (manifest.sequence != proof.sequence) return null;
        _ = try self.validateManifestAuthority(a, manifest, previous, namespace);
        const authority = try self.domain(a, namespace);
        for (manifest.files) |file| for (file.chunks) |chunk| {
            const scope = (try artifacts.uploadScopeFromArtifactId(chunk.artifact_id)) orelse return error.CatalogGenerationChanged;
            if (!std.mem.eql(u8, &scope.domain, &authority)) return error.CatalogGenerationChanged;
            if (scope.fencingToken() <= request.expires_ms or scope.fencingToken() > request.expires_ms +| 3 * cache_window_ms) return null;
        };
        try self.reserveInventory(a, request, namespace, manifest.files, false, cancellation);
        const replacement = try std.json.Stringify.valueAlloc(a, Manifest{ .request = request, .namespace = namespace, .sequence = proof.sequence, .files = manifest.files }, .{});
        if (replacement.len > max_manifest_bytes) return error.QueryCandidateBudgetExceeded;
        _ = try checkedManifest(a, replacement, request, namespace);
        try cancellation.check();
        const bridge: TokenBridge = .{ .token = cancellation };
        var published = self.store.opened.client.putObject(self.store.opened.bucket, try self.key(a, request, namespace), replacement, .{ .if_none_match = true, .cancellation = bridge.object() }) catch |err| {
            try cancellation.check();
            if (err != error.PreconditionFailed and err != error.ConditionalCheckFailed) return err;
            if (!std.mem.eql(u8, replacement, try self.readManifest(a, request, namespace, cancellation))) return error.CatalogGenerationChanged;
            return .{ .sequence = proof.sequence, .digest = proof.digest, .manifest_checksum = manifestChecksum(replacement) };
        };
        published.deinit(self.store.opened.client.allocator);
        try cancellation.check();
        return .{ .sequence = proof.sequence, .digest = proof.digest, .manifest_checksum = manifestChecksum(replacement) };
    }
    fn validateManifestAuthority(self: *Repository, a: A, manifest: Manifest, request: cut.Request, namespace: Namespace) ![32]u8 {
        const expected_domain = try self.domain(a, namespace);
        // Publication may reuse the adjacent warmer epoch (one additional
        // hour) beyond its ordinary two-window chunk horizon.
        for (manifest.files) |file| for (file.chunks) |ref| {
            const scope = (try artifacts.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.CatalogGenerationChanged;
            if (!std.mem.eql(u8, &scope.domain, &expected_domain) or scope.fencingToken() < request.expires_ms or scope.fencingToken() > request.expires_ms +| 3 * cache_window_ms) return error.CatalogGenerationChanged;
        };
        return expected_domain;
    }
    fn openRead(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !?port.StorageLease {
        return openReadImpl(raw, executor, root, request, namespace, cancellation) catch |err| return repositoryFailure("open_read", err, cancellation);
    }
    fn openReadImpl(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !?port.StorageLease {
        const self: *Repository = @ptrCast(@alignCast(raw));
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const bytes = self.readManifest(a, request, namespace, cancellation) catch |err| {
            if (request.create and err == error.CatalogGenerationChanged) return null;
            std.log.warn("retained native manifest lookup failed err={s} table={d} shard={d} range={d} expires={d}", .{ @errorName(err), namespace.table_id, namespace.shard_id, namespace.range_id, request.expires_ms });
            return err;
        };
        const manifest = checkedManifest(a, bytes, request, namespace) catch |err| {
            std.log.warn("retained native manifest validation failed err={s}", .{@errorName(err)});
            return err;
        };
        const expected_domain = try self.validateManifestAuthority(a, manifest, request, namespace);
        const expiry = @import("antfly_platform").time.monotonicNs() +| (request.expires_ms -| cut.nowMs()) * std.time.ns_per_ms;
        const control: cut.Control = .{ .parent = cancellation, .deadline_ns = if (request.timeout_ms) |value| @min(expiry, @import("antfly_platform").time.monotonicNs() +| value *| std.time.ns_per_ms) else expiry };
        const reader = try port.remote_storage.Reader.createWithExecutor(self.a, executor, root, manifest.files, .{ .ptr = self, .read = readChunk }, control);
        errdefer reader.release();
        reader.checkpoint_reference = try std.json.Stringify.valueAlloc(reader.arena.allocator(), port.CheckpointReference{ .authority = expected_domain, .namespace = namespace, .sequence = manifest.sequence, .files = reader.files }, .{});
        return reader.lease();
    }
    fn recover(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !void {
        return recoverImpl(raw, executor, root, request, namespace, cancellation) catch |err| return repositoryFailure("recover", err, cancellation);
    }
    fn recoverImpl(raw: *anyopaque, executor: port.Executor, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !void {
        var receiver = try executor.receive();
        const io = receiver.io();
        const self = from(raw);
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const manifest = try checkedManifest(a, try self.readManifest(a, request, namespace, cancellation), request, namespace);
        const staging = try std.fmt.allocPrint(a, "{s}.staging", .{root});
        try fs.createDirPathPortable(io, staging);
        errdefer std.Io.Dir.cwd().deleteTree(io, staging) catch {};
        var store = self.store.artifactStore();
        const expected_domain = try self.domain(a, namespace);
        // Publication may reuse the adjacent warmer epoch (one additional
        // hour) beyond its ordinary two-window chunk horizon.
        for (manifest.files) |file| {
            try cancellation.check();
            const destination = try std.fmt.allocPrint(a, "{s}/{s}", .{ staging, file.path });
            if (std.fs.path.dirname(destination)) |parent| try fs.createDirPathPortable(io, parent);
            var output = try fs.createFilePortable(io, destination, .{ .exclusive = true });
            defer output.close(io);
            var offset: u64 = 0;
            for (file.chunks) |ref| {
                const scope = (try artifacts.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.CatalogGenerationChanged;
                if (!std.mem.eql(u8, &scope.domain, &expected_domain) or (scope.fencingToken() < request.expires_ms or scope.fencingToken() > request.expires_ms +| 3 * cache_window_ms)) return error.CatalogGenerationChanged;
                const payload = store.getVerifiedAllocWithCancellation(ref.artifact_id, @intCast(ref.byte_len), ref.checksum, cancellation) catch |err| switch (err) {
                    error.NotFound, error.ObjectNotFound, error.FileNotFound, error.ArtifactIntegrityMismatch => return error.CatalogGenerationChanged,
                    else => return err,
                };
                defer store.allocator.free(payload);
                try output.writePositionalAll(io, payload, offset);
                offset += payload.len;
            }
            try output.sync(io);
        }
        try cut.finish(a, io, staging, request, namespace, manifest.sequence, cancellation);
        try cut.validate(a, io, staging, request, namespace, cancellation);
        // Recovery creates new inode/mtime identities. Rebind the authenticated
        // remote references to those sealed local files before publication so
        // the replacement owner's first capture does not re-upload them.
        const restored_bytes = try local.storage_db_native_backup.readFileAlloc(a, io, try std.fmt.allocPrint(a, "{s}/query-cut.json", .{staging}), max_manifest_bytes);
        const restored = try std.json.parseFromSliceLeaky(cut.Manifest, a, restored_bytes, .{});
        for (restored.files) |file| {
            const remote = for (manifest.files) |candidate| {
                if (std.mem.eql(u8, candidate.path, file.path) and candidate.size == file.size) break candidate;
            } else return error.CatalogGenerationChanged;
            if (remote.chunks.len == 0) continue;
            const scope = (try artifacts.uploadScopeFromArtifactId(remote.chunks[0].artifact_id)) orelse return error.CatalogGenerationChanged;
            const horizon = scope.fencingToken();
            const encoded = try std.json.Stringify.valueAlloc(a, CachedFile{ .domain = expected_domain, .expires_ms = horizon, .source = file, .chunks = remote.chunks }, .{});
            const proof = try std.json.Stringify.valueAlloc(a, .{ .domain = expected_domain, .source = file }, .{});
            var digest: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(proof, &digest, .{});
            // Every chunk was verified during recovery; this proof is published
            // only after the replacement seal has passed validation.
            try self.publishCheckpoint(a, namespace, horizon, digest, encoded, cancellation);
        }
        try std.Io.Dir.rename(.cwd(), staging, .cwd(), root, io);
        if (std.fs.path.dirname(root)) |parent| try fs.syncDirPortable(io, parent);
    }
};

test "external lake native oversized optional chunks do not allocate an uncachable copy" {
    const a = std.testing.allocator;
    const Probe = struct {
        allocations: usize = 0,
        fn denied(raw: *anyopaque, _: usize, _: std.mem.Alignment, _: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.allocations += 1;
            return null;
        }
    };
    var directory = try local.common_test_directory.TestDirectory.init("native-query-small-cache");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    var store = repository.store.artifactStore();
    var ref = try store.put("larger than the optional residency cap");
    defer ref.deinit(a);
    var probe: Probe = .{};
    // No allocation can succeed, so the remaining allocator entrypoints are
    // unreachable. Store/mandatory read allocations still use the normal heap.
    var vtable = a.vtable.*;
    vtable.alloc = Probe.denied;
    repository.chunks.a = .{ .ptr = &probe, .vtable = &vtable };
    repository.chunks.max_bytes = 1;
    const bytes = try Repository.readChunk(&repository, a, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, .none);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("larger than the optional residency cap", bytes);
    try std.testing.expectEqual(@as(usize, 0), probe.allocations);
}

test "external lake native residency is node managed and safe after owner teardown" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-query-managed-chunks");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    var manager = local.storage_resource_manager.ResourceManager.init(.{});
    {
        defer manager.deinit(a);
        repository.attachResources(&manager);
        const cache = repository.shared_chunks.?;
        try std.testing.expectEqual(cache, manager.nativeChunkCache(16 * 1024 * 1024).?);
        const lease = try cache.create(8192);
        const bytes = try lease.item.arena.allocator().dupe(u8, "managed extent");
        cache.publish(@splat(42), lease);
        try std.testing.expect(cache.bytes != 0);
        try std.testing.expectEqual(@as(u64, 0), cache.reclaim(1));
        try std.testing.expectEqualStrings("managed extent", bytes);
        lease.release();
        try std.testing.expect(cache.reclaim(1) != 0);
        try std.testing.expectEqual(@as(usize, 0), cache.bytes);
        // The metadata owner and its allocator close with the manager. The
        // repository closes later and never dereferences the borrowed cache.
    }
}

test "external lake native verified chunks survive reader churn with bounded scoped residency" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-query-chunk-cache");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    repository.chunks.max_entries = 1;
    var store = repository.store.artifactStore();
    var first = try store.put("first immutable extent");
    defer first.deinit(a);
    var second = try store.put("second immutable extent");
    defer second.deinit(a);
    const ref: Ref = .{ .artifact_id = first.artifact_id, .checksum = first.checksum, .byte_len = first.byte_len };
    {
        const bytes = try Repository.readChunk(&repository, a, ref, .none);
        defer a.free(bytes);
        try std.testing.expectEqualStrings("first immutable extent", bytes);
    }
    {
        const bytes = try Repository.readChunk(&repository, a, ref, .none);
        defer a.free(bytes);
        try std.testing.expectEqualStrings("first immutable extent", bytes);
        try std.testing.expectEqual(@as(u64, 1), repository.chunks.hits);
    }
    var wrong = ref;
    wrong.checksum = &(@as([64]u8, @splat('0')));
    try std.testing.expectError(error.InvalidArtifactId, Repository.readChunk(&repository, a, wrong, .none));
    wrong = ref;
    wrong.byte_len += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, Repository.readChunk(&repository, a, wrong, .none));
    const cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, Repository.readChunk(&repository, a, ref, Cancellation.fromAtomic(&cancelled)));
    {
        const bytes = try Repository.readChunk(&repository, a, .{ .artifact_id = second.artifact_id, .checksum = second.checksum, .byte_len = second.byte_len }, .none);
        defer a.free(bytes);
    }
    try std.testing.expectEqual(@as(usize, 1), repository.chunks.entries.count());
    // Scope is part of the key even when content and physical location match.
    const previous_hits = repository.chunks.hits;
    repository.store.cache_identity[0] ^= 1;
    {
        const bytes = try Repository.readChunk(&repository, a, ref, .none);
        defer a.free(bytes);
    }
    try std.testing.expectEqual(previous_hits, repository.chunks.hits);
}

test "external lake native reuse avoids capture and invalidates writes metadata and ranges" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-query-reuse");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    // Disabling speculation must preserve on-demand durable capture, reuse,
    // immutable reads, and invalidation when the live document changes.
    try std.testing.expect(!repository.capability().warming_enabled);
    try std.testing.expect(!repository.chunkedCapability().warming_enabled);
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .manual, .filesystem_io = io });
    defer runtime.deinit();
    runtime.ptr().query_cut_repository = repository.chunkedCapability();
    const root = try std.fmt.allocPrint(a, "{s}/db", .{directory.path()});
    defer a.free(root);
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    var db = try local.storage_db_db.DB.open(a, root, .{ .backend_runtime = runtime.ptr(), .identity_namespace = namespace, .start_optional_runtimes = false });
    defer db.close();
    try db.addIndex(.{ .name = "body_text", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":1,\"body\":\"original retained\"}" }} });
    const first_id: [64]u8 = @splat('a');
    const first: cut.Request = .{ .id = &first_id, .table_id = 7, .expires_ms = cut.nowMs() + 300000, .create = true };
    try db.captureQueryCut(first, .none);
    try std.testing.expect(db.local_execution.native_query_reuse != null);
    const second_id: [64]u8 = @splat('b');
    const second: cut.Request = .{ .id = &second_id, .table_id = 7, .expires_ms = first.expires_ms + 1000, .timeout_ms = 5000, .create = true };
    {
        // Full capture cannot proceed while this lock is held. Reuse must not
        // drain maintenance, pause workers or build a filesystem generation.
        try std.testing.expect(db.snapshot_publication_mutex.tryLock());
        defer db.snapshot_publication_mutex.unlock();
        var frozen = try db.openQueryCut(second, .none);
        defer frozen.close();
        const bytes = (try frozen.get(a, "doc")).?;
        defer a.free(bytes);
        try std.testing.expect(std.mem.indexOf(u8, bytes, "\"amount\":1") != null);
        var matches = try frozen.search(a, .{ .index_name = "body_text", .full_text = .{ .match = .{ .field = "body", .text = "original" } } });
        defer matches.deinit();
        try std.testing.expectEqual(@as(u32, 1), matches.total_hits);
    }
    const second_root = try cut.pathAlloc(a, root, &second_id);
    defer a.free(second_root);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, second_root, .{}));
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const before = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, first, namespace, .none), first, namespace);
    const reused = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, second, namespace, .none), second, namespace);
    var different_indexes = second;
    different_indexes.indexes = &.{"body_text"};
    try std.testing.expectError(error.CatalogGenerationChanged, db.captureQueryCut(different_indexes, .none));
    try std.testing.expectEqualStrings(before.files[0].chunks[0].artifact_id, reused.files[0].chunks[0].artifact_id);
    var wrong = db.local_execution.native_query_reuse.?.proof();
    wrong.sequence += 1;
    try std.testing.expect(try repository.chunkedCapability().reuse(io, second, first, namespace, wrong, .none) == null);
    wrong.sequence -= 1;
    wrong.manifest_checksum = @splat(0);
    try std.testing.expect(try repository.chunkedCapability().reuse(io, second, first, namespace, wrong, .none) == null);
    try std.testing.expect(try repository.chunkedCapability().reuse(io, first, second, .{ .table_id = 7, .shard_id = 2, .range_id = 2 }, wrong, .none) == null);
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":2,\"body\":\"replacement\"}" }} });
    const third_id: [64]u8 = @splat('c');
    const third: cut.Request = .{ .id = &third_id, .table_id = 7, .expires_ms = first.expires_ms + 2000, .create = true };
    {
        var fresh = try db.openQueryCut(third, .none);
        defer fresh.close();
        const bytes = (try fresh.get(a, "doc")).?;
        defer a.free(bytes);
        try std.testing.expect(std.mem.indexOf(u8, bytes, "\"amount\":2") != null);
    }
    // The optional reuse hint now names a different generation and live
    // writes have advanced. A repeated create must still serve the old cut.
    {
        var same_cut = second;
        same_cut.timeout_ms = 4000;
        var retained = try db.openQueryCut(same_cut, .none);
        defer retained.close();
        const bytes = (try retained.get(a, "doc")).?;
        defer a.free(bytes);
        try std.testing.expect(std.mem.indexOf(u8, bytes, "\"amount\":1") != null);
    }
    // A primary metadata commit has no new source replay sequence, but changes
    // the frozen primary state and must invalidate the owner-local generation.
    const sequence = db.core.nextDerivedSequence();
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put("\x00query-reuse-fixture", "new metadata");
        try txn.commit();
    }
    try std.testing.expectEqual(sequence, db.core.nextDerivedSequence());
    const fourth_id: [64]u8 = @splat('d');
    const fourth: cut.Request = .{ .id = &fourth_id, .table_id = 7, .expires_ms = first.expires_ms + 3000, .create = true };
    try db.captureQueryCut(fourth, .none);
    const fourth_root = try cut.pathAlloc(a, root, &fourth_id);
    defer a.free(fourth_root);
    try std.Io.Dir.accessAbsolute(io, fourth_root, .{});
    try db.addIndex(.{ .name = "second_text", .kind = .full_text, .config_json = "{}" });
    const fifth_id: [64]u8 = @splat('e');
    const fifth: cut.Request = .{ .id = &fifth_id, .table_id = 7, .expires_ms = first.expires_ms + 4000, .create = true };
    try db.captureQueryCut(fifth, .none);
    const fifth_root = try cut.pathAlloc(a, root, &fifth_id);
    defer a.free(fifth_root);
    try std.Io.Dir.accessAbsolute(io, fifth_root, .{});
    try db.updateRange(.{ .start = "", .end = "m" });
    const sixth_id: [64]u8 = @splat('f');
    const sixth: cut.Request = .{ .id = &sixth_id, .table_id = 7, .expires_ms = first.expires_ms + 5000, .create = true };
    try db.captureQueryCut(sixth, .none);
    const sixth_root = try cut.pathAlloc(a, root, &sixth_id);
    defer a.free(sixth_root);
    try std.Io.Dir.accessAbsolute(io, sixth_root, .{});
    var old = first;
    old.create = false;
    var prior = try db.openQueryCut(old, .none);
    defer prior.close();
    const old_bytes = (try prior.get(a, "doc")).?;
    defer a.free(old_bytes);
    try std.testing.expect(std.mem.indexOf(u8, old_bytes, "\"amount\":1") != null);
}

test "external lake native linked renewal reuses immutable files without snapshot capture" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-linked-renewal");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .manual, .filesystem_io = io });
    defer runtime.deinit();
    runtime.ptr().query_cut_repository = repository.capability();
    const root = try std.fs.path.join(a, &.{ directory.path(), "db" });
    defer a.free(root);
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    var db = try local.storage_db_db.DB.open(a, root, .{ .backend_runtime = runtime.ptr(), .identity_namespace = namespace, .start_optional_runtimes = false });
    defer db.close();
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":1}" }} });
    const first: cut.Request = .{ .id = &@as([64]u8, @splat('a')), .table_id = 7, .expires_ms = cut.nowMs() + 240000, .create = true };
    try db.captureQueryCut(first, .none);
    const proof = db.local_execution.native_query_reuse.?.proof();
    const second: cut.Request = .{ .id = &@as([64]u8, @splat('b')), .table_id = 7, .expires_ms = first.expires_ms + 30000, .create = true };
    try std.testing.expect(db.snapshot_publication_mutex.tryLock());
    defer db.snapshot_publication_mutex.unlock();
    try db.captureQueryCut(second, .none);
    try std.testing.expectEqual(proof.digest, db.local_execution.native_query_reuse.?.proof().digest);
    // A repeated create can arrive with a smaller execution budget. The
    // published cut is immutable; do not take the snapshot lock or compare a
    // fresh manifest against the already-retained linked generation.
    var repeated = second;
    repeated.timeout_ms = 1000;
    var reader_again = try db.openQueryCut(repeated, .none);
    defer reader_again.close();
    const again = (try reader_again.get(a, "doc")).?;
    defer a.free(again);
    try std.testing.expectEqualStrings("{\"amount\":1}", again);
    const path = try cut.pathAlloc(a, root, second.id);
    defer a.free(path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, path, .{}));
    const retained = try repository.linkedRoot(a, second, namespace);
    defer a.free(retained);
    try cut.validate(a, io, retained, second, namespace, .none);
    var wrong = proof;
    wrong.digest = @splat(0);
    try std.testing.expect(try repository.capability().reuse(io, first, second, namespace, wrong, .none) == null);
    var reader = try db.openQueryCut(.{ .id = first.id, .table_id = 7, .expires_ms = first.expires_ms }, .none);
    defer reader.close();
    const value = (try reader.get(a, "doc")).?;
    defer a.free(value);
    try std.testing.expectEqualStrings("{\"amount\":1}", value);
    const manifest_path = try std.fs.path.join(a, &.{ retained, "query-cut.json" });
    defer a.free(manifest_path);
    _ = try local.storage_db_native_backup.writeFileDurable(io, manifest_path, "{}");
    try std.testing.expectError(error.CatalogGenerationChanged, db.captureQueryCut(repeated, .none));
}

test "external lake native reuse validates physical retention before extending cursors" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-reuse-horizon");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const now = cut.nowMs();
    const previous_id: [64]u8 = @splat('a');
    const next_id: [64]u8 = @splat('b');
    const previous: cut.Request = .{ .id = &previous_id, .table_id = 7, .expires_ms = now + 5 * std.time.ms_per_min, .create = true };
    var next: cut.Request = .{ .id = &next_id, .table_id = 7, .expires_ms = now + 20 * std.time.ms_per_min, .create = true };
    var attempt: [16]u8 = @splat(1);
    std.mem.writeInt(u64, attempt[0..8], now + 10 * std.time.ms_per_min, .big);
    var store = repository.store.artifactStore();
    var object = try store.putScoped(.{ .domain = try repository.domain(scratch, namespace), .attempt = attempt }, "bytes", .none);
    defer object.deinit(store.allocator);
    const chunks = [_]Ref{.{ .artifact_id = object.artifact_id, .byte_len = object.byte_len, .checksum = object.checksum }};
    const files = [_]File{.{ .path = "fixture.sst", .size = 5, .chunks = &chunks }};
    const bytes = try std.json.Stringify.valueAlloc(scratch, Manifest{ .request = previous, .namespace = namespace, .sequence = 123, .files = &files }, .{});
    var committed = try repository.store.opened.client.putObject(repository.store.opened.bucket, try repository.key(scratch, previous, namespace), bytes, .{ .if_none_match = true });
    committed.deinit(repository.store.opened.client.allocator);
    const proof: port.ReuseProof = .{ .sequence = 123, .digest = @splat(42), .manifest_checksum = Repository.manifestChecksum(bytes) };
    try std.testing.expect(try repository.chunkedCapability().reuse(io, previous, next, namespace, proof, .none) == null);
    try std.testing.expectError(error.CatalogGenerationChanged, repository.readManifest(scratch, next, namespace, .none));
    next.expires_ms = now + 9 * std.time.ms_per_min;
    const rebound = (try repository.chunkedCapability().reuse(io, previous, next, namespace, proof, .none)).?;
    const result = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, next, namespace, .none), next, namespace);
    try std.testing.expectEqual(@as(u64, 123), rebound.sequence);
    try std.testing.expectEqualStrings(object.artifact_id, result.files[0].chunks[0].artifact_id);
}

test "external lake native repository setup preserves optional warming and on-demand capabilities" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-query-warming-policy");
    defer directory.cleanup();
    const json = try std.json.Stringify.valueAlloc(a, .{
        .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() }, .artifacts = .{ .connection = "google", .bucket = "fixture-bucket", .prefix = "pins" } },
        // Check-only provisioning and skipped bucket probes keep this fixture
        // offline while exercising the configured serverless transport.
        .connections = .{ .google = .{ .kind = "external_io", .capabilities = .{"storage.primary"}, .external_io = .{ .protocol = "gcs", .buckets = .{"fixture-bucket"}, .prefix = "pins", .bucket_provisioning = "require_existing", .credentials = .{ .source = "bearer_token", .bearer_token = "fixture" } } } },
    }, .{});
    defer a.free(json);
    var config = try local.common_config.Config.parseFromSlice(a, json);
    defer config.deinit();
    for (std.meta.tags(local.common_config.DeploymentMode)) |deployment| {
        for ([_]?bool{ null, true, false }) |override| {
            config.lake_indexes.query_cursors.background_warming = override;
            const encoded = try Repository.setupJsonAlloc(a, &config, deployment, directory.path());
            defer a.free(encoded);
            var repository = try Repository.initFromSetup(a, encoded, null);
            defer repository.deinit();
            try std.testing.expectEqual(override, repository.owned_config.?.lake_indexes.query_cursors.background_warming);
            for ([_]port.Port{ repository.capability(), repository.chunkedCapability() }) |capability| {
                try std.testing.expectEqual(override orelse false, capability.warming_enabled);
                try std.testing.expect(capability.vtable.open_read != null);
                try std.testing.expect(capability.vtable.publish_with_proof != null);
                try std.testing.expect(capability.vtable.reference != null);
                try std.testing.expect(capability.vtable.reuse != null);
                try std.testing.expectEqual(@as(usize, 64), capability.limits.max_cuts);
            }
        }
    }
}

test "external lake native repository setup preserves cache limits and disabled residency" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-query-cache-policy");
    defer directory.cleanup();
    var config = try local.common_config.Config.parseFromSlice(a, "{\"storage\":{\"engine\":\"local\",\"local\":{\"base_dir\":\"/tmp/fixture\"}},\"lake_cache\":{\"max_memory_bytes\":1048576}}");
    defer config.deinit();
    for ([_]bool{ true, false }) |enabled| {
        config.lake_cache.enabled = enabled;
        const encoded = try Repository.setupJsonAlloc(a, &config, .standalone, directory.path());
        defer a.free(encoded);
        var repository = try Repository.initFromSetup(a, encoded, null);
        defer repository.deinit();
        try std.testing.expectEqual(enabled, repository.owned_config.?.lake_cache.enabled);
        try std.testing.expectEqual(@as(usize, 1048576), repository.owned_config.?.lake_cache.max_memory_bytes);
        try std.testing.expectEqual(@as(usize, if (enabled) 262144 else 0), repository.chunks.max_bytes);
        try std.testing.expect(repository.store.reopen_options.?.config == repository.owned_config.?);
        var reopened = try repository.store.reopenReadOnly(a);
        defer reopened.deinit();
        try std.testing.expectEqual(repository.store.identity, reopened.identity);
    }
}

test "external lake native repository setup round trips protocol-specific connection fields" {
    const a = std.testing.allocator;
    var config = try local.common_config.Config.parseFromSlice(a,
        \\{"storage":{"engine":"local","local":{"base_dir":"/tmp/fixture"},"artifacts":{"connection":"google","bucket":"fixture-bucket","prefix":"pins"}},
        \\"connections":{
        \\"google":{"kind":"external_io","capabilities":["storage.primary"],"external_io":{"protocol":"gcs","buckets":["fixture-bucket"],"prefix":"pins","credentials":{"source":"bearer_token","bearer_token":"fixture"}}},
        \\"amazon":{"kind":"external_io","capabilities":["storage.primary"],"external_io":{"protocol":"s3","buckets":["fixture-bucket"],"addressing_style":"path","credentials":{"source":"static","access_key_id":"fixture","secret_access_key":"fixture"}}},
        \\"http":{"kind":"external_io","capabilities":["content.fetch"],"external_io":{"protocol":"http","hosts":["example.com"],"headers":{"X-Fixture":"yes"}}},
        \\"files":{"kind":"external_io","capabilities":["backup.write"],"external_io":{"protocol":"filesystem","root":"/tmp/fixture"}}
        \\}}
    );
    defer config.deinit();
    const encoded = try Repository.setupJsonAlloc(a, &config, .standalone, "/tmp/fixture");
    defer a.free(encoded);
    var setup = try std.json.parseFromSlice(Repository.Setup, a, encoded, .{});
    defer setup.deinit();
    var recovered = try local.common_config.Config.parseFromSlice(a, setup.value.config_json);
    defer recovered.deinit();
    try std.testing.expectEqualStrings("fixture", recovered.connections.get("google").?.external_io.?.gcs_credentials.bearer_token.?);
    try std.testing.expectEqualStrings("fixture", recovered.connections.get("amazon").?.external_io.?.credentials.access_key_id.?);
    try std.testing.expectEqualStrings("yes", recovered.connections.get("http").?.external_io.?.headers.get("X-Fixture").?);
    try std.testing.expectEqualStrings("/tmp/fixture", recovered.connections.get("files").?.external_io.?.root.?);
}

test "external lake native repository opens retained vector storage after local cache loss" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-cut-vector-read");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .manual, .filesystem_io = io });
    defer runtime.deinit();
    runtime.ptr().query_cut_repository = repository.chunkedCapability();
    const root = try std.fmt.allocPrint(a, "{s}/db", .{directory.path()});
    defer a.free(root);
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    var db = try local.storage_db_db.DB.open(a, root, .{ .backend_runtime = runtime.ptr(), .identity_namespace = namespace, .table_storage = .{ .dense_embeddings = .vector_store } });
    var db_open = true;
    defer if (db_open) db.close();
    try db.addIndex(.{ .name = "body_text", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":1,\"body\":\"retained original\"}" }} });
    const id: [64]u8 = @splat('f');
    var request: cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = cut.nowMs() + 300_000, .create = true };
    {
        var retained = try db.openQueryCut(request, .none);
        defer retained.close();
        const prior = (try retained.get(a, "doc")).?;
        defer a.free(prior);
        var initial_matches = try retained.search(a, .{ .index_name = "body_text", .full_text = .{ .match = .{ .field = "body", .text = "retained" } } });
        defer initial_matches.deinit();
        try std.testing.expectEqual(@as(u32, 1), initial_matches.total_hits);
    }
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":2,\"body\":\"replacement\"}" }} });
    db.close();
    db_open = false;
    db = try local.storage_db_db.DB.open(a, root, .{ .backend_runtime = runtime.ptr(), .identity_namespace = namespace, .table_storage = .{ .dense_embeddings = .vector_store } });
    db_open = true;
    request.create = false;
    {
        var restarted = try db.openQueryCut(request, .none);
        defer restarted.close();
        const before_loss = (try restarted.get(a, "doc")).?;
        defer a.free(before_loss);
        try std.testing.expect(std.mem.indexOf(u8, before_loss, "\"amount\":1") != null);
    }
    const pinned = try cut.pathAlloc(a, root, &id);
    defer a.free(pinned);
    const local_manifest = try std.fmt.allocPrint(a, "{s}/query-cut.json", .{pinned});
    defer a.free(local_manifest);
    const original_manifest = try local.storage_db_native_backup.readFileAlloc(a, io, local_manifest, max_manifest_bytes);
    defer a.free(original_manifest);
    const vector_current = try std.fmt.allocPrint(a, "{s}/source-vectors/CURRENT", .{pinned});
    defer a.free(vector_current);
    request.create = false;
    for (0..3) |loss| {
        // Preserve the directory while losing metadata or a sealed file.
        // Neither invalid local bytes nor a partial cache can replace authority.
        switch (loss) {
            0 => try std.Io.Dir.cwd().deleteFile(io, local_manifest),
            1 => _ = try local.storage_db_native_backup.writeFileDurable(io, local_manifest, "{invalid"),
            2 => {
                _ = try local.storage_db_native_backup.writeFileDurable(io, local_manifest, original_manifest);
                try std.Io.Dir.cwd().deleteFile(io, vector_current);
            },
            else => unreachable,
        }
        var remote = try db.openQueryCut(request, .none);
        defer remote.close();
        const original = (try remote.get(a, "doc")).?;
        defer a.free(original);
        try std.testing.expect(std.mem.indexOf(u8, original, "\"amount\":1") != null);
        var matches = try remote.search(a, .{ .index_name = "body_text", .full_text = .{ .match = .{ .field = "body", .text = "retained" } } });
        defer matches.deinit();
        try std.testing.expectEqual(@as(u32, 1), matches.total_hits);
        try std.Io.Dir.accessAbsolute(io, pinned, .{});
        if (loss == 0) try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, local_manifest, .{}));
        if (loss == 2) try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, vector_current, .{}));
    }
    const remote_key = try repository.key(a, request, namespace);
    defer a.free(remote_key);
    try repository.store.opened.client.deleteObject(repository.store.opened.bucket, remote_key, .{});
    try std.testing.expectError(error.CatalogGenerationChanged, db.openQueryCut(request, .none));
    const latest = (try db.get(a, "doc")).?;
    defer a.free(latest);
    try std.testing.expect(std.mem.indexOf(u8, latest, "\"amount\":2") != null);
}

test "external lake invalid native cache admission protects active readers" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-cut-cache-admission");
    defer directory.cleanup();
    const root = try std.fmt.allocPrint(a, "{s}/db", .{directory.path()});
    defer a.free(root);
    const old_id: [64]u8 = @splat('a');
    const new_id: [64]u8 = @splat('b');
    const pinned = try cut.pathAlloc(a, root, &old_id);
    defer a.free(pinned);
    const manifest = try std.fmt.allocPrint(a, "{s}/query-cut.json", .{pinned});
    defer a.free(manifest);
    var guard = try cut.lock(a, io, root, .none);
    defer guard.deinit();
    for ([_]bool{ false, true }) |malformed| {
        try fs.createDirPathPortable(io, pinned);
        if (malformed) _ = try local.storage_db_native_backup.writeFileDurable(io, manifest, "{invalid");
        {
            var reader = try cut.readLease(a, io, root, &old_id, .none);
            defer reader.deinit();
            try std.testing.expectError(error.QueryCandidateBudgetExceeded, cut.admit(a, io, root, &new_id, .{ .max_cuts = 1, .max_bytes = 1024 }, .none));
            try std.Io.Dir.accessAbsolute(io, pinned, .{});
        }
        try cut.admit(a, io, root, &new_id, .{ .max_cuts = 1, .max_bytes = 1024 }, .none);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, pinned, .{}));
    }
}

test "external lake native repository reuses immutable extents and collects expired generations" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-cut-chunks");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const first_id: [64]u8 = @splat('a');
    const second_id: [64]u8 = @splat('b');
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const expires = cut.nowMs() + 300_000;
    const first: cut.Request = .{ .id = &first_id, .table_id = 7, .expires_ms = expires, .create = true };
    const second: cut.Request = .{ .id = &second_id, .table_id = 7, .expires_ms = expires, .create = true };
    const first_root = try std.fmt.allocPrint(scratch, "{s}/{s}", .{ directory.path(), first_id });
    const second_root = try std.fmt.allocPrint(scratch, "{s}/{s}", .{ directory.path(), second_id });
    try fs.createDirPathPortable(io, first_root);
    try fs.createDirPathPortable(io, second_root);
    const first_file = try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{first_root});
    const second_file = try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{second_root});
    _ = try local.storage_db_native_backup.writeFileDurable(io, first_file, "original immutable extent");
    try std.Io.Dir.hardLink(.cwd(), first_file, .cwd(), second_file, io, .{});
    try cut.finish(a, io, first_root, first, namespace, 1, .none);
    try cut.finish(a, io, second_root, second, namespace, 1, .none);
    try repository.chunkedCapability().publish(io, first_root, first, namespace, .none);
    // Reboot/loss of all local hints must reuse the durable checkpoint.
    try std.Io.Dir.cwd().deleteTree(io, try std.fmt.allocPrint(scratch, "{s}/.chunk-cache", .{directory.path()}));
    try repository.chunkedCapability().publish(io, second_root, second, namespace, .none);
    // Local commit evidence is private cache metadata, never snapshot data.
    try std.testing.expect(try repository.localCommit(scratch, io, first_root, first, namespace));
    try cut.finish(a, io, first_root, first, namespace, 1, .none);
    try repository.chunkedCapability().publish(io, first_root, first, namespace, .none);
    const first_manifest = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, first, namespace, .none), first, namespace);
    const second_manifest = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, second, namespace, .none), second, namespace);
    try std.testing.expectEqual(@as(usize, 1), first_manifest.files.len);
    try std.testing.expectEqualStrings(first_manifest.files[0].chunks[0].artifact_id, second_manifest.files[0].chunks[0].artifact_id);
    const recovered = try std.fmt.allocPrint(scratch, "{s}/recovered", .{directory.path()});
    try repository.chunkedCapability().recover(io, recovered, first, namespace, .none);
    const recovered_bytes = try local.storage_db_native_backup.readFileAlloc(scratch, io, try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{recovered}), 1024);
    try std.testing.expectEqualStrings("original immutable extent", recovered_bytes);
    const replacement_id: [64]u8 = @splat('c');
    const replacement: cut.Request = .{ .id = &replacement_id, .table_id = 7, .expires_ms = expires, .create = true };
    const replacement_root = try std.fmt.allocPrint(scratch, "{s}/{s}", .{ directory.path(), replacement_id });
    try fs.createDirPathPortable(io, replacement_root);
    try std.Io.Dir.hardLink(.cwd(), try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{recovered}), .cwd(), try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{replacement_root}), io, .{});
    try cut.finish(a, io, replacement_root, replacement, namespace, 1, .none);
    try std.testing.expect(try Repository.publishLimited(&repository, io, replacement_root, replacement, namespace, .none, 0));
    const replacement_manifest = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, replacement, namespace, .none), replacement, namespace);
    try std.testing.expectEqualStrings(first_manifest.files[0].chunks[0].artifact_id, replacement_manifest.files[0].chunks[0].artifact_id);
    // A virtual reader needs only the manifest, never a recovered directory.
    const virtual_root = try std.fmt.allocPrint(scratch, "{s}/virtual", .{directory.path()});
    var remote = (try repository.chunkedCapability().openRead(io, virtual_root, first, namespace, .none)).?;
    defer remote.deinit();
    const virtual_file = try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{virtual_root});
    const remote_bytes = try remote.view.readFileRangeAlloc(a, virtual_file, 9, 9);
    defer a.free(remote_bytes);
    try std.testing.expectEqualStrings("immutable", remote_bytes);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, virtual_root, .{}));
    try std.testing.expectError(error.ReadOnly, remote.view.writeFileAbsolute(virtual_file, "replacement"));
    // A replacement owner can pin the complete immutable remote generation
    // by reference, with neither a host checkpoint nor an extent transfer.
    const alias_id: [64]u8 = @splat('d');
    const alias: cut.Request = .{ .id = &alias_id, .table_id = 7, .expires_ms = expires, .create = true };
    try std.testing.expect(try repository.chunkedCapability().reference(io, alias, namespace, remote.view, .none));
    try std.testing.expect(try repository.chunkedCapability().reference(io, alias, namespace, remote.view, .none));
    const alias_manifest = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, alias, namespace, .none), alias, namespace);
    try std.testing.expectEqualStrings(first_manifest.files[0].chunks[0].artifact_id, alias_manifest.files[0].chunks[0].artifact_id);
    var alias_reader = (try repository.chunkedCapability().openRead(io, virtual_root, alias, namespace, .none)).?;
    defer alias_reader.deinit();
    const alias_bytes = try alias_reader.view.readFileRangeAlloc(a, virtual_file, 9, 9);
    defer a.free(alias_bytes);
    try std.testing.expectEqualStrings("immutable", alias_bytes);
    var invalid_alias = alias;
    invalid_alias.expires_ms += 4 * cache_window_ms;
    try std.testing.expectError(error.CatalogGenerationChanged, repository.chunkedCapability().reference(io, invalid_alias, namespace, remote.view, .none));
    try std.testing.expectError(error.CatalogGenerationChanged, repository.chunkedCapability().reference(io, alias, .{ .table_id = 7, .shard_id = 2, .range_id = 2 }, remote.view, .none));
    var store = repository.store.artifactStore();
    const retained = @import("native_retained_cut.zig");
    const table = .{ .table_id = @as(u64, 7), .schema_json = "{}", .read_schema_json = "{}", .indexes_json = "{}" };
    const identity = try retained.storeIdentity(a, repository.store.locator);
    const capability = try retained.save(a, &store, identity, io, table, cut.nowMs(), .none);
    defer a.free(capability);
    try store.delete(first_manifest.files[0].chunks[0].artifact_id);
    // Remove the repository's verified local copy as well: this assertion is
    // specifically about a reader with neither local bytes nor remote data.
    repository.chunks.discard(repository.chunkKey(first_manifest.files[0].chunks[0].artifact_id));
    // An uncached reader detects authoritative loss when it reads a page.
    // Open itself only authenticates the manifest and chunk references.
    var lost = (try repository.chunkedCapability().openRead(io, virtual_root, second, namespace, .none)).?;
    defer lost.deinit();
    try std.testing.expectError(error.CatalogGenerationChanged, lost.view.readFileRangeAlloc(a, virtual_file, 0, 1));
    const missing = try std.fmt.allocPrint(scratch, "{s}/missing", .{directory.path()});
    try std.testing.expectError(error.CatalogGenerationChanged, repository.chunkedCapability().recover(io, missing, second, namespace, .none));
    // A complete transfer remains scoped until both logical cuts and their
    // shared physical retention horizon are safely beyond every reader.
    // Registry discovery must still find the owner after all local pins vanish.
    const next = try repository.collectNext(a, null, expires + 2 * cache_window_ms + 30_001, 128, .none);
    defer if (next) |cursor| a.free(cursor);
    try std.testing.expect(next == null);
    try std.testing.expectError(error.CatalogGenerationChanged, repository.readManifest(scratch, first, namespace, .none));
    try std.testing.expectError(error.CatalogGenerationChanged, repository.readManifest(scratch, second, namespace, .none));
    try std.testing.expectError(error.CatalogGenerationChanged, retained.load(scratch, &store, identity, capability, table, cut.nowMs(), .none));
}

test "external lake native repository bounded publication resumes uploads after restart" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-cut-warming");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    repository.background_warming = true;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const id: [64]u8 = @splat('d');
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const request: cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = cut.nowMs() + cache_window_ms, .create = true };
    const root = try std.fmt.allocPrint(scratch, "{s}/{s}", .{ directory.path(), id });
    try fs.createDirPathPortable(io, root);
    const bytes = try scratch.alloc(u8, 2 * chunk_bytes + 17);
    @memset(bytes, 42);
    _ = try local.storage_db_native_backup.writeFileDurable(io, try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{root}), bytes);
    try cut.finish(a, io, root, request, namespace, 1, .none);
    try std.testing.expectError(error.InvalidQueryRequest, Repository.publishChunkedBounded(&repository, port.Executor.init(&io), root, request, namespace, .none, chunk_bytes - 1));
    try std.testing.expect(!try Repository.publishChunkedBounded(&repository, port.Executor.init(&io), root, request, namespace, .none, chunk_bytes));
    try std.testing.expectError(error.CatalogGenerationChanged, repository.readManifest(scratch, request, namespace, .none));
    repository.deinit();
    repository = try Repository.init(a, null, null, .standalone, directory.path());
    repository.background_warming = true;
    try std.testing.expect(!try Repository.publishChunkedBounded(&repository, port.Executor.init(&io), root, request, namespace, .none, chunk_bytes));
    try std.testing.expectError(error.CatalogGenerationChanged, repository.readManifest(scratch, request, namespace, .none));
    try std.testing.expect(try Repository.publishChunkedBounded(&repository, port.Executor.init(&io), root, request, namespace, .none, chunk_bytes));
    const manifest = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, request, namespace, .none), request, namespace);
    try std.testing.expectEqual(@as(usize, 3), manifest.files[0].chunks.len);
    // A shorter foreground cut reuses the warmer's different retention epoch.
    const short_id: [64]u8 = @splat('e');
    var short_request = request;
    short_request.id = &short_id;
    short_request.expires_ms = request.expires_ms - cache_window_ms + 300_000;
    const short_root = try std.fmt.allocPrint(scratch, "{s}/{s}", .{ directory.path(), short_id });
    try fs.createDirPathPortable(io, short_root);
    try std.Io.Dir.hardLink(.cwd(), try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{root}), .cwd(), try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{short_root}), io, .{});
    try cut.finish(a, io, short_root, short_request, namespace, 1, .none);
    // One turn cannot upload this file: completing proves cross-epoch reuse.
    try std.testing.expect(try Repository.publishChunkedBounded(&repository, port.Executor.init(&io), short_root, short_request, namespace, .none, chunk_bytes));
    const short_manifest = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, short_request, namespace, .none), short_request, namespace);
    try std.testing.expectEqualStrings(manifest.files[0].chunks[0].artifact_id, short_manifest.files[0].chunks[0].artifact_id);
    // The neighboring warmer epoch can extend beyond the foreground cut's
    // two-window horizon. Both virtual reads and fallback recovery must accept
    // this authenticated reuse without weakening namespace or expiry checks.
    const short_remote_path = try std.fmt.allocPrint(scratch, "{s}/short-remote", .{directory.path()});
    var short_remote = (try repository.chunkedCapability().openRead(io, short_remote_path, short_request, namespace, .none)).?;
    defer short_remote.deinit();
    const short_tail = try short_remote.view.readFileRangeAlloc(a, try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{short_remote_path}), 2 * chunk_bytes, 17);
    defer a.free(short_tail);
    try std.testing.expectEqualSlices(u8, bytes[2 * chunk_bytes ..][0..17], short_tail);
    const short_recovered = try std.fmt.allocPrint(scratch, "{s}/short-recovered", .{directory.path()});
    try repository.chunkedCapability().recover(io, short_recovered, short_request, namespace, .none);
    try repository.chunkedCapability().publish(io, root, request, namespace, .none);
    var remote = (try repository.chunkedCapability().openRead(io, try std.fmt.allocPrint(scratch, "{s}/remote", .{directory.path()}), request, namespace, .none)).?;
    defer remote.deinit();
    const tail = try remote.view.readFileRangeAlloc(a, try std.fmt.allocPrint(scratch, "{s}/remote/immutable.sst", .{directory.path()}), 2 * chunk_bytes, 17);
    defer a.free(tail);
    try std.testing.expectEqualSlices(u8, bytes[2 * chunk_bytes ..], tail);
}

test "external lake native repository honors check-only cancellation tokens" {
    const Stop = struct {
        fn check(_: *const anyopaque) !void {
            return error.Canceled;
        }
    };
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-cut-cancel");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    const id: [64]u8 = @splat('c');
    const request: cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = cut.nowMs() + 300_000 };
    const stopped: u8 = 0;
    const token: Cancellation = .{ .ptr = &stopped, .check_fn = Stop.check };
    try std.testing.expectError(error.Canceled, repository.chunkedCapability().publish(std.testing.io, directory.path(), request, .{ .table_id = 7, .shard_id = 1 }, token));
    try std.testing.expectError(error.Canceled, repository.chunkedCapability().recover(std.testing.io, directory.path(), request, .{ .table_id = 7, .shard_id = 1 }, token));
    try std.testing.expectError(error.Canceled, repository.chunkedCapability().openRead(std.testing.io, directory.path(), request, .{ .table_id = 7, .shard_id = 1 }, token));
    try std.testing.expectError(error.Canceled, repository.chunkedCapability().publishReusable(std.testing.io, directory.path(), request, .{ .table_id = 7, .shard_id = 1 }, token));
    try std.testing.expectError(error.Canceled, repository.chunkedCapability().reuse(std.testing.io, request, request, .{ .table_id = 7, .shard_id = 1 }, .{ .sequence = 1, .digest = @splat(0), .manifest_checksum = @splat(0) }, token));
}

test "external lake native cut admission counts shared extents and rejects existing over-budget cuts" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-cut-admission");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const db_path = try std.fmt.allocPrint(scratch, "{s}/db", .{directory.path()});
    const ids = [_][64]u8{ @splat('a'), @splat('b'), @splat('c') };
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    var guard = try cut.lock(a, io, db_path, .none);
    defer guard.deinit();
    const first_root = try cut.pathAlloc(scratch, db_path, &ids[0]);
    const second_root = try cut.pathAlloc(scratch, db_path, &ids[1]);
    try fs.createDirPathPortable(io, first_root);
    try fs.createDirPathPortable(io, second_root);
    const first_file = try std.fmt.allocPrint(scratch, "{s}/shared.sst", .{first_root});
    const second_file = try std.fmt.allocPrint(scratch, "{s}/shared.sst", .{second_root});
    _ = try local.storage_db_native_backup.writeFileDurable(io, first_file, "shared");
    try std.Io.Dir.hardLink(.cwd(), first_file, .cwd(), second_file, io, .{});
    const expires = cut.nowMs() + 300_000;
    for ([_][]const u8{ first_root, second_root }, 0..) |root, i| {
        try cut.finish(a, io, root, .{ .id = &ids[i], .table_id = 7, .expires_ms = expires }, namespace, 1, .none);
    }
    try cut.admit(a, io, db_path, &ids[0], .{ .max_cuts = 2, .max_bytes = 6 }, .none);
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, cut.admit(a, io, db_path, &ids[2], .{ .max_cuts = 2, .max_bytes = 6 }, .none));
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, cut.admit(a, io, db_path, &ids[0], .{ .max_cuts = 1, .max_bytes = 6 }, .none));
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, cut.admit(a, io, db_path, &ids[0], .{ .max_cuts = 2, .max_bytes = 5 }, .none));
    try cut.validate(a, io, first_root, .{ .id = &ids[0], .table_id = 7, .expires_ms = expires }, namespace, .none);
}

test "external lake native cache evicts durable idle generations and recovers retained cursors" {
    try testNativeCacheEviction(false);
}

test "external lake native filesystem cache evicts idle generations and recovers retained cursors" {
    try testNativeCacheEviction(true);
}

fn testNativeCacheEviction(linked: bool) !void {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-durable-cache-eviction");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    repository.limits.max_cuts = 1;
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .manual, .filesystem_io = io });
    defer runtime.deinit();
    runtime.ptr().query_cut_repository = if (linked) repository.capability() else repository.chunkedCapability();
    const root = try std.fmt.allocPrint(a, "{s}/db", .{directory.path()});
    defer a.free(root);
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    var db = try local.storage_db_db.DB.open(a, root, .{ .backend_runtime = runtime.ptr(), .identity_namespace = namespace });
    defer db.close();
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":1}" }} });
    const old_id: [64]u8 = @splat('a');
    const new_id: [64]u8 = @splat('b');
    const last_id: [64]u8 = @splat('c');
    const expires = cut.nowMs() + 300_000;
    var old: cut.Request = .{ .id = &old_id, .table_id = 7, .expires_ms = expires, .create = true };
    const next: cut.Request = .{ .id = &new_id, .table_id = 7, .expires_ms = expires, .create = true };
    var original = try db.openQueryCut(old, .none);
    var original_open = true;
    defer if (original_open) original.close();
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":2}" }} });
    // An active local reader must retain its files even with durable authority.
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, db.openQueryCut(next, .none));
    original.close();
    original_open = false;
    {
        var current = try db.openQueryCut(next, .none);
        defer current.close();
        const old_root = try cut.pathAlloc(a, root, &old_id);
        defer a.free(old_root);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, old_root, .{}));
    }
    // The original cursor reads the exact old generation through the repository.
    old.create = false;
    {
        var recovered = try db.openQueryCut(old, .none);
        defer recovered.close();
        const value = (try recovered.get(a, "doc")).?;
        defer a.free(value);
        try std.testing.expectEqualStrings("{\"amount\":1}", value);
    }
    // Missing durable authority must protect the only surviving local copy.
    if (linked) {
        const retained_root = try repository.linkedRoot(a, next, namespace);
        defer a.free(retained_root);
        try std.Io.Dir.cwd().deleteTree(io, retained_root);
    } else {
        const key = try repository.key(a, next, namespace);
        defer a.free(key);
        try repository.store.opened.client.deleteObject(repository.store.opened.bucket, key, .{});
    }
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, db.openQueryCut(.{ .id = &last_id, .table_id = 7, .expires_ms = expires, .create = true }, .none));
    const current_root = try cut.pathAlloc(a, root, &new_id);
    defer a.free(current_root);
    try std.Io.Dir.accessAbsolute(io, current_root, .{});
}

test "external lake native filesystem publication links a 24 GiB generation and recovers after pin loss" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const Probe = struct {
        var read_bytes: usize = 0;
        fn link(_: ?*anyopaque, _: std.Io.Dir, _: []const u8, _: std.Io.Dir, _: []const u8, _: std.Io.Dir.HardLinkOptions) std.Io.Dir.HardLinkError!void {
            return error.CrossDevice;
        }
        fn read(raw: ?*anyopaque, file: std.Io.File, data: []const []u8, offset: u64) std.Io.File.ReadPositionalError!usize {
            const bytes = try std.testing.io.vtable.fileReadPositional(raw, file, data, offset);
            read_bytes += bytes;
            // Opening and reading a few tail bytes must never scan/copy the
            // sparse 24 GiB payload, even when hardlinks are unavailable.
            if (read_bytes > 128 * 1024) return error.Unexpected;
            return bytes;
        }
    };
    Probe.read_bytes = 0;
    var measured_vtable = io.vtable.*;
    measured_vtable.dirHardLink = Probe.link;
    measured_vtable.fileReadPositional = Probe.read;
    const measured: std.Io = .{ .userdata = io.userdata, .vtable = &measured_vtable };
    var directory = try local.common_test_directory.TestDirectory.init("native-query-linked-large");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    repository.background_warming = true;
    const source = try std.fs.path.join(scratch, &.{ directory.path(), "source" });
    try fs.createDirPathPortable(io, source);
    const payload_path = try std.fs.path.join(scratch, &.{ source, "large.tbl" });
    const logical_size: u64 = 24 * 1024 * 1024 * 1024;
    {
        const file = try std.Io.Dir.cwd().createFile(io, payload_path, .{});
        defer file.close(io);
        // Sparse fixture: a corpus scan/copy would process 24 GiB; hardlink
        // publication touches only metadata and two data pages.
        try file.setLength(io, logical_size);
        try file.writePositionalAll(io, "begin", 0);
        try file.writePositionalAll(io, "end", logical_size - 3);
        try file.sync(io);
    }
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const id: [64]u8 = @splat('a');
    const request: cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = cut.nowMs() + 300_000, .create = true };
    try cut.finish(a, io, source, request, namespace, 42, .none);
    const warmed = try Repository.publishLinkedBoundedWithProof(&repository, port.Executor.init(&io), source, request, namespace, .none, chunk_bytes);
    try std.testing.expect(warmed.complete);
    try std.testing.expect(warmed.proof == null);
    const virtual = try std.fs.path.join(scratch, &.{ directory.path(), "virtual" });
    var lazy = (try repository.capability().openRead(measured, virtual, request, namespace, .none)).?;
    var lazy_open = true;
    defer if (lazy_open) lazy.deinit();
    var page = try lazy.view.openLeasedImmutableSource(a, try std.fmt.allocPrint(scratch, "{s}/large.tbl", .{virtual}));
    var page_open = true;
    defer if (page_open) page.close();
    const page_bytes = try lazy.view.readFileRangeAlloc(a, try std.fmt.allocPrint(scratch, "{s}/large.tbl", .{virtual}), logical_size - 3, 3);
    defer a.free(page_bytes);
    try std.testing.expectEqualStrings("end", page_bytes);
    {
        // The lazy view retains only its shared expiry lease, never the
        // publication lock while its caller performs payload I/O.
        const lock_path = try std.fs.path.join(scratch, &.{ try repository.linkedNamespace(scratch, namespace), ".lock" });
        const publication = try std.Io.Dir.cwd().createFile(io, lock_path, .{ .truncate = false });
        defer publication.close(io);
        try std.testing.expect(try publication.tryLock(io, .exclusive));
        publication.unlock(io);
    }
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, virtual, .{}));
    try repository.capability().publish(io, source, request, namespace, .none);
    try cut.finish(a, io, source, request, namespace, 43, .none);
    try std.testing.expectError(error.CatalogGenerationChanged, repository.capability().publish(io, source, request, namespace, .none));
    try cut.finish(a, io, source, request, namespace, 42, .none);
    const retained = try repository.linkedRoot(scratch, request, namespace);
    const linked_path = try std.fs.path.join(scratch, &.{ retained, "large.tbl" });
    const original_stat = try local.storage_db_native_backup.statRegularFile(io, payload_path);
    const linked_stat = try local.storage_db_native_backup.statRegularFile(io, linked_path);
    try std.testing.expectEqual(original_stat.inode, linked_stat.inode);
    try std.testing.expectEqual(logical_size, linked_stat.size);
    try std.testing.expect(try repository.capability().canEvict(io, source, request, namespace, 42, .none));
    try std.testing.expect(!try repository.capability().canEvict(io, source, request, namespace, 43, .none));
    try std.Io.Dir.cwd().deleteTree(io, source);
    const recovered = try std.fs.path.join(scratch, &.{ directory.path(), "recovered" });
    try repository.capability().recover(io, recovered, request, namespace, .none);
    try cut.validate(a, io, recovered, request, namespace, .none);
    const recovered_path = try std.fs.path.join(scratch, &.{ recovered, "large.tbl" });
    try std.testing.expectEqual(original_stat.inode, (try local.storage_db_native_backup.statRegularFile(io, recovered_path)).inode);
    const file = try std.Io.Dir.cwd().openFile(io, recovered_path, .{});
    defer file.close(io);
    var tail: [3]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 3), try file.readPositionalAll(io, &tail, logical_size - 3));
    try std.testing.expectEqualStrings("end", &tail);
    try repository.collect(namespace, request.expires_ms + 30_001, .none);
    try cut.validate(a, io, retained, request, namespace, .none);
    lazy.deinit();
    lazy_open = false;
    try repository.collect(namespace, request.expires_ms + 30_001, .none);
    try cut.validate(a, io, retained, request, namespace, .none);
    const reads_before_hit = Probe.read_bytes;
    try page.readInto(logical_size - 3, &tail);
    try std.testing.expectEqualStrings("end", &tail);
    try std.testing.expectEqual(reads_before_hit, Probe.read_bytes);
    page.close();
    page_open = false;
    var gc_turns: usize = 0;
    while (!(try repository.collectLinked(&repository.owned_collection, namespace, request.expires_ms + 30_001, 1, .none)).complete) : (gc_turns += 1) {
        try std.testing.expect(gc_turns < 1000);
    }
    try std.testing.expectError(error.CatalogGenerationChanged, cut.validate(a, io, retained, request, namespace, .none));
    try cut.validate(a, io, recovered, request, namespace, .none);
}

test "external lake native filesystem falls back to chunks across devices and reads linked recovery without copies" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const CrossDevice = struct {
        fn link(_: ?*anyopaque, _: std.Io.Dir, _: []const u8, _: std.Io.Dir, _: []const u8, _: std.Io.Dir.HardLinkOptions) std.Io.Dir.HardLinkError!void {
            return error.CrossDevice;
        }
    };
    var vtable = io.vtable.*;
    vtable.dirHardLink = CrossDevice.link;
    const cross_device_io: std.Io = .{ .userdata = io.userdata, .vtable = &vtable };
    var directory = try local.common_test_directory.TestDirectory.init("native-query-cross-device");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    repository.background_warming = true;
    const source = try std.fs.path.join(scratch, &.{ directory.path(), "source" });
    try fs.createDirPathPortable(io, source);
    _ = try local.storage_db_native_backup.writeFileDurable(io, try std.fs.path.join(scratch, &.{ source, "data.tbl" }), "original");
    const large_path = try std.fs.path.join(scratch, &.{ source, "large.tbl" });
    {
        const file = try std.Io.Dir.cwd().createFile(io, large_path, .{});
        defer file.close(io);
        const block = try a.alloc(u8, chunk_bytes);
        defer a.free(block);
        for (0..3) |i| {
            @memset(block, @intCast(i));
            try file.writePositionalAll(io, block, i * chunk_bytes);
        }
        try file.sync(io);
    }
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const id: [64]u8 = @splat('a');
    const request: cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = cut.nowMs() + 300_000, .create = true };
    try cut.finishWithReuseDigest(a, io, source, request, namespace, 42, @splat(7), .none);
    const capability = repository.capability();
    var warmed = try Repository.publishLinkedBoundedWithProof(&repository, port.Executor.init(&cross_device_io), source, request, namespace, .none, chunk_bytes);
    try std.testing.expect(!warmed.complete);
    var turns: usize = 1;
    while (!warmed.complete and turns < 10) : (turns += 1) {
        warmed = try Repository.publishLinkedBoundedWithProof(&repository, port.Executor.init(&cross_device_io), source, request, namespace, .none, chunk_bytes);
    }
    try std.testing.expect(warmed.complete);
    try std.testing.expect(warmed.proof != null);
    const alias_id: [64]u8 = @splat('b');
    var alias = request;
    alias.id = &alias_id;
    try std.testing.expect(try capability.reuse(io, request, alias, namespace, warmed.proof.?, .none) != null);
    const virtual = try std.fs.path.join(scratch, &.{ directory.path(), "not-materialized" });
    var lazy = (try capability.openRead(io, virtual, request, namespace, .none)).?;
    defer lazy.deinit();
    const lazy_bytes = try lazy.view.readFileAlloc(a, try std.fs.path.join(scratch, &.{ virtual, "data.tbl" }), 64);
    defer a.free(lazy_bytes);
    try std.testing.expectEqualStrings("original", lazy_bytes);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, virtual, .{}));
    const restored = try std.fs.path.join(scratch, &.{ directory.path(), "chunk-restored" });
    try repository.capability().recover(io, restored, request, namespace, .none);
    try cut.validate(a, io, restored, request, namespace, .none);
    // A linked generation stays lazy when the new cache cannot hardlink it.
    try repository.capability().publish(io, source, request, namespace, .none);
    const copied = try std.fs.path.join(scratch, &.{ directory.path(), "copy-restored" });
    try std.testing.expectError(error.CrossDevice, repository.capability().recover(cross_device_io, copied, request, namespace, .none));
    var linked_reader = (try repository.capability().openRead(cross_device_io, copied, request, namespace, .none)).?;
    defer linked_reader.deinit();
    const bytes = try linked_reader.view.readFileAlloc(a, try std.fmt.allocPrint(scratch, "{s}/data.tbl", .{copied}), 64);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("original", bytes);
}

test "external lake native linked recovery bypasses partial caches without replacing active readers" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-linked-partial-cache");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .manual, .filesystem_io = io });
    defer runtime.deinit();
    runtime.ptr().query_cut_repository = repository.capability();
    const root = try std.fs.path.join(a, &.{ directory.path(), "db" });
    defer a.free(root);
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    var db = try local.storage_db_db.DB.open(a, root, .{ .backend_runtime = runtime.ptr(), .identity_namespace = namespace, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false });
    defer db.close();
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":1}" }}, .sync_level = .write });
    const id: [64]u8 = @splat('a');
    var request: cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = cut.nowMs() + 300000, .create = true };
    var reader = try db.openQueryCut(request, .none);
    var reader_open = true;
    defer if (reader_open) reader.close();
    request.create = false;
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":2}" }}, .sync_level = .write });
    const pinned = try cut.pathAlloc(a, root, &id);
    defer a.free(pinned);
    const manifest_path = try std.fs.path.join(a, &.{ pinned, "query-cut.json" });
    defer a.free(manifest_path);
    const manifest_bytes = try local.storage_db_native_backup.readFileAlloc(a, io, manifest_path, max_manifest_bytes);
    defer a.free(manifest_bytes);
    try std.Io.Dir.cwd().deleteFile(io, manifest_path);
    {
        var parallel = try db.openQueryCut(request, .none);
        defer parallel.close();
        const value = (try parallel.get(a, "doc")).?;
        defer a.free(value);
        try std.testing.expectEqualStrings("{\"amount\":1}", value);
    }
    const still_readable = (try reader.get(a, "doc")).?;
    defer a.free(still_readable);
    try std.testing.expectEqualStrings("{\"amount\":1}", still_readable);
    reader.close();
    reader_open = false;
    for (0..3) |loss| {
        if (loss == 1) _ = try local.storage_db_native_backup.writeFileDurable(io, manifest_path, "{invalid");
        if (loss == 2) {
            _ = try local.storage_db_native_backup.writeFileDurable(io, manifest_path, manifest_bytes);
            var manifest = try std.json.parseFromSlice(cut.Manifest, a, manifest_bytes, .{});
            defer manifest.deinit();
            const file = try std.fs.path.join(a, &.{ pinned, manifest.value.files[0].path });
            defer a.free(file);
            try std.Io.Dir.cwd().deleteFile(io, file);
        }
        var recovered = try db.openQueryCut(request, .none);
        defer recovered.close();
        const original = (try recovered.get(a, "doc")).?;
        defer a.free(original);
        try std.testing.expectEqualStrings("{\"amount\":1}", original);
        try std.testing.expectError(error.CatalogGenerationChanged, cut.validate(a, io, pinned, request, namespace, .none));
    }
    const retained = try repository.linkedRoot(a, request, namespace);
    defer a.free(retained);
    try std.Io.Dir.cwd().deleteTree(io, retained);
    try std.Io.Dir.cwd().deleteFile(io, manifest_path);
    try std.testing.expectError(error.CatalogGenerationChanged, db.openQueryCut(request, .none));
    try std.Io.Dir.accessAbsolute(io, pinned, .{});
}

test "external lake native linked collection resumes across short lived repository adapters" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("linked-gc-adapters");
    defer directory.cleanup();
    const source = try std.fs.path.join(a, &.{ directory.path(), "source" });
    defer a.free(source);
    try fs.createDirPathPortable(io, source);
    const payload = try std.fs.path.join(a, &.{ source, "file" });
    defer a.free(payload);
    _ = try local.storage_db_native_backup.writeFileDurable(io, payload, "immutable");
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const id: [64]u8 = @splat('a');
    const request: cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = cut.nowMs() + 300000, .create = true };
    try cut.finish(a, io, source, request, namespace, 1, .none);
    {
        var repository = try Repository.init(a, null, null, .standalone, directory.path());
        defer repository.deinit();
        try repository.capability().publish(io, source, request, namespace, .none);
    }
    var state: Repository.CollectionState = .init(a, io);
    defer state.deinit();
    var turns: usize = 0;
    while (true) : (turns += 1) {
        try std.testing.expect(turns < 1000);
        var repository = try Repository.init(a, null, null, .standalone, directory.path());
        defer repository.deinit();
        const next = try repository.collectNextWithState(a, &state, null, request.expires_ms + 30001, 1, .none);
        defer if (next) |value| a.free(value);
        if (!state.hasPending()) {
            const retained = try repository.linkedRoot(a, request, namespace);
            defer a.free(retained);
            try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, retained, .{}));
            break;
        }
    }
    try std.testing.expect(turns > 7);
    try cut.validate(a, io, source, request, namespace, .none);
}

test "external lake native linked reader releases partial metadata ownership" {
    const Reader = port.remote_storage.Reader;
    const Fixture = struct {
        fn check(a: A, lease_path: []const u8) !void {
            const io = std.testing.io;
            const lease_file = try std.Io.Dir.cwd().createFile(io, lease_path, .{ .truncate = false });
            var transferred = false;
            defer if (!transferred) lease_file.close(io);
            try std.testing.expect(try lease_file.tryLock(io, .shared));
            const executor = @import("antfly_runtime_abi").io_abi.Borrow.init(&io);
            const reader = try Reader.createLinkedWithExecutor(a, executor, "/logical", "/physical", &.{
                .{ .path = "runs/1.sst", .size = 5, .inode = 1, .mtime_ns = 0 },
                .{ .path = "runs/2.sst", .size = 7, .inode = 2, .mtime_ns = 0 },
            }, lease_file, .{ .parent = .none, .deadline_ns = std.math.maxInt(u64) });
            transferred = true;
            defer reader.release();
            reader.control.deadline_ns = 0;
            try std.testing.expectEqual(@as(u64, 7), try reader.view().fileSize("/logical/runs/2.sst"));
            try std.testing.expectError(error.FileNotFound, reader.view().fileSize("/logical-other/runs/1.sst"));
            try std.testing.expectError(error.ReadOnly, reader.view().writeFileAbsolute("/logical/runs/1.sst", "mutation"));
            try std.testing.expectError(error.DeadlineExceeded, reader.view().readFileRangeAlloc(std.testing.allocator, "/logical/runs/1.sst", 0, 1));
        }
    };
    var directory = try local.common_test_directory.TestDirectory.init("linked-reader-allocations");
    defer directory.cleanup();
    try fs.createDirPathPortable(std.testing.io, directory.path());
    const path = try std.fs.path.join(std.testing.allocator, &.{ directory.path(), ".lease" });
    defer std.testing.allocator.free(path);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{path});
}

test "external lake native checkpoint misses overlap adjacent epochs and drain before return" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-checkpoint-miss-wave");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const expires = cut.nowMs() + 2 * cache_window_ms;
    const digest: [32]u8 = @splat(1);
    const primary = try repository.checkpointKey(a, namespace, expires, digest);
    defer a.free(primary);
    const Fixture = struct {
        io: std.Io,
        primary: []const u8,
        started: std.atomic.Value(usize) = .init(0),
        active: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        fn get(raw: *anyopaque, _: A, _: []const u8, key_: []const u8, _: objectstore.GetOptions) anyerror!objectstore.GetResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (std.mem.eql(u8, key_, self.primary)) return error.FileNotFound;
            const active = self.active.fetchAdd(1, .acq_rel) + 1;
            defer _ = self.active.fetchSub(1, .acq_rel);
            _ = self.peak.fetchMax(active, .monotonic);
            _ = self.started.fetchAdd(1, .acq_rel);
            const deadline = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s;
            while (self.started.load(.acquire) < 2) {
                if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.CheckpointLookupsWereSerial;
                try self.io.sleep(.fromMilliseconds(1), .awake);
            }
            return error.FileNotFound;
        }
    };
    var fixture: Fixture = .{ .io = io, .primary = primary };
    const original = repository.store.opened.client;
    defer repository.store.opened.client = original;
    var vtable = original.vtable.*;
    vtable.get_object = Fixture.get;
    repository.store.opened.client = .{ .allocator = a, .ptr = &fixture, .vtable = &vtable };
    try std.testing.expect(try repository.checkpointExtent(a, io, namespace, digest, @splat(2), expires, .{ .path = "control", .size = 32, .inode = 1, .mtime_ns = 0 }, expires - 1, .none) == null);
    try std.testing.expectEqual(@as(usize, 2), fixture.started.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), fixture.peak.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), fixture.active.load(.acquire));
}

test "external lake native checkpoint hints bound stalls preserve cancellation and reject terminal errors" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-checkpoint-hint-deadline");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const expires = cut.nowMs() + 2 * cache_window_ms;
    const digest: [32]u8 = @splat(1);
    const primary = try repository.checkpointKey(a, namespace, expires, digest);
    defer a.free(primary);
    const Fixture = struct {
        const Mode = enum { tls, stall, denied, integrity, protocol, mixed };
        var mode: Mode = .tls;
        var primary_key: []const u8 = undefined;
        var active: std.atomic.Value(usize) = .init(0);
        fn get(_: *anyopaque, _: A, _: []const u8, key_: []const u8, options: objectstore.GetOptions) anyerror!objectstore.GetResult {
            if (std.mem.eql(u8, key_, primary_key)) return error.FileNotFound;
            _ = active.fetchAdd(1, .acq_rel);
            defer _ = active.fetchSub(1, .acq_rel);
            switch (mode) {
                .tls => return error.TlsTransportReadFailed,
                .mixed => return if (std.mem.order(u8, key_, primary_key) == .gt) error.TlsTransportReadFailed else error.AccessDenied,
                .denied => return error.AccessDenied,
                .integrity => return error.ArtifactIntegrityMismatch,
                .protocol => return error.TlsBadRecordMac,
                .stall => {
                    const safety = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
                    while (true) {
                        try options.cancellation.?.check();
                        if (@import("antfly_platform").time.monotonicNs() >= safety) return error.CheckpointLookupWasUnbounded;
                        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                    }
                },
            }
        }
        fn cancelled(_: *const anyopaque) bool {
            return true;
        }
    };
    Fixture.primary_key = primary;
    const original = repository.store.opened.client;
    defer repository.store.opened.client = original;
    var vtable = original.vtable.*;
    vtable.get_object = Fixture.get;
    repository.store.opened.client.vtable = &vtable;
    const source: local.storage_db_native_backup_seal.File = .{ .path = "control", .size = 32, .inode = 1, .mtime_ns = 0 };
    for ([_]Fixture.Mode{ .tls, .stall, .denied, .integrity, .protocol, .mixed }) |mode| {
        Fixture.mode = mode;
        const result = repository.checkpointExtent(a, io, namespace, digest, @splat(2), expires, source, expires - 1, .none);
        switch (mode) {
            .tls, .stall => try std.testing.expect(try result == null),
            .denied, .mixed => try std.testing.expectError(error.AccessDenied, result),
            .integrity => try std.testing.expectError(error.ArtifactIntegrityMismatch, result),
            .protocol => try std.testing.expectError(error.TlsBadRecordMac, result),
        }
        try std.testing.expectEqual(@as(usize, 0), Fixture.active.load(.acquire));
    }
    const marker: u8 = 0;
    try std.testing.expectError(error.Canceled, repository.checkpointExtent(a, io, namespace, digest, @splat(2), expires, source, expires - 1, .{ .ptr = &marker, .is_cancelled_fn = Fixture.cancelled }));
}

test "external lake native transient hints cannot fail a sealed publication and required failures cross the ABI" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-checkpoint-hint-fallback");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const id: [64]u8 = @splat('e');
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const request: cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = cut.nowMs() + 300000, .create = true };
    const root = try std.fmt.allocPrint(scratch, "{s}/{s}", .{ directory.path(), id });
    try fs.createDirPathPortable(io, root);
    const filename = try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{root});
    _ = try local.storage_db_native_backup.writeFileDurable(io, filename, "complete sealed native data");
    try cut.finish(a, io, root, request, namespace, 1, .none);
    const Fixture = struct {
        var backend: *const objectstore.Client.VTable = undefined;
        var required_failure: bool = false;
        var hint_reads: usize = 0;
        var hint_writes: usize = 0;
        fn get(raw: *anyopaque, alloc: A, bucket: []const u8, key_: []const u8, options: objectstore.GetOptions) anyerror!objectstore.GetResult {
            if (required_failure) return error.TlsTransportReadFailed;
            if (std.mem.indexOf(u8, key_, "/extent-") != null) {
                hint_reads += 1;
                return error.TlsTransportReadFailed;
            }
            return backend.get_object(raw, alloc, bucket, key_, options);
        }
        fn put(raw: *anyopaque, alloc: A, bucket: []const u8, key_: []const u8, bytes: []const u8, options: objectstore.PutOptions) anyerror!objectstore.PutResult {
            if (required_failure) return error.TlsTransportReadFailed;
            if (std.mem.indexOf(u8, key_, "/extent-") != null) {
                hint_writes += 1;
                return error.TlsTransportReadFailed;
            }
            return backend.put_object(raw, alloc, bucket, key_, bytes, options);
        }
    };
    const original = repository.store.opened.client;
    const original_artifacts = repository.store.implementation.client;
    defer repository.store.implementation.client = original_artifacts;
    defer repository.store.opened.client = original;
    Fixture.backend = original.vtable;
    var vtable = original.vtable.*;
    vtable.get_object = Fixture.get;
    vtable.put_object = Fixture.put;
    repository.store.opened.client.vtable = &vtable;
    repository.store.implementation.client.vtable = &vtable;
    _ = try repository.chunkedCapability().publishReusable(io, root, request, namespace, .none);
    try std.testing.expect(Fixture.hint_reads > 0);
    try std.testing.expect(Fixture.hint_writes > 0);
    const virtual = try std.fmt.allocPrint(scratch, "{s}/virtual", .{directory.path()});
    var reader = (try repository.chunkedCapability().openRead(io, virtual, request, namespace, .none)).?;
    defer reader.deinit();
    const bytes = try reader.view.readFileRangeAlloc(a, try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{virtual}), 0, 27);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("complete sealed native data", bytes);
    const manifest = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, request, namespace, .none), request, namespace);
    var uncached_reader = (try repository.chunkedCapability().openRead(io, virtual, request, namespace, .none)).?;
    defer uncached_reader.deinit();
    const next_id: [64]u8 = @splat('f');
    const next_request: cut.Request = .{ .id = &next_id, .table_id = 7, .expires_ms = request.expires_ms, .create = true };
    const next_root = try std.fmt.allocPrint(scratch, "{s}/{s}", .{ directory.path(), next_id });
    try fs.createDirPathPortable(io, next_root);
    _ = try local.storage_db_native_backup.writeFileDurable(io, try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{next_root}), "complete sealed native data");
    try cut.finish(a, io, next_root, next_request, namespace, 1, .none);
    for (manifest.files) |file| for (file.chunks) |chunk| repository.chunkCache().discard(repository.chunkKey(chunk.artifact_id));
    Fixture.required_failure = true;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, uncached_reader.view.readFileRangeAlloc(a, try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{virtual}), 0, 27));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, repository.chunkedCapability().openRead(io, virtual, request, namespace, .none));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, repository.chunkedCapability().publishReusable(io, next_root, next_request, namespace, .none));
    try std.testing.expectEqual(error.AccessDenied, repositoryFailure("read", error.AccessDenied, .none));
    try std.testing.expectEqual(error.TlsBadRecordMac, repositoryFailure("read", error.TlsBadRecordMac, .none));
}

test "external lake native reuse falls back when its basis expires behind collection lock" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-expired-reuse-basis");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const parent = try repository.linkedNamespace(a, namespace);
    defer a.free(parent);
    var guard = try Repository.linkedLock(a, io, parent, .none);
    var guarded = true;
    defer if (guarded) guard.deinit();
    const Worker = struct {
        repository: *Repository,
        previous: cut.Request,
        request: cut.Request,
        started: std.Io.Event = .unset,
        failure: ?anyerror = null,
        proof: ?port.ReuseProof = null,
        fn run(self: *@This(), executor_io: std.Io, ns: Namespace) void {
            self.previous.expires_ms = cut.nowMs() + 100;
            self.previous.validate(cut.nowMs()) catch |err| {
                self.failure = err;
                self.started.set(executor_io);
                return;
            };
            self.started.set(executor_io);
            self.proof = self.repository.capability().reuse(executor_io, self.previous, self.request, ns, .{ .sequence = 1, .digest = @splat(1), .manifest_checksum = @splat(1) }, .none) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    var worker: Worker = .{
        .repository = &repository,
        .previous = .{ .id = &@as([64]u8, @splat('a')), .table_id = 7, .expires_ms = cut.nowMs() + 500, .create = true },
        .request = .{ .id = &@as([64]u8, @splat('b')), .table_id = 7, .expires_ms = cut.nowMs() + 300000, .create = true },
    };
    var task = try io.concurrent(Worker.run, .{ &worker, io, namespace });
    var awaited = false;
    defer if (!awaited) {
        if (guarded) {
            guard.deinit();
            guarded = false;
        }
        task.await(io);
    };
    worker.started.wait(io) catch |err| {
        guard.deinit();
        guarded = false;
        task.await(io);
        awaited = true;
        return err;
    };
    try io.sleep(.fromMilliseconds(@intCast(worker.previous.expires_ms -| cut.nowMs() + 10)), .awake);
    guard.deinit();
    guarded = false;
    task.await(io);
    awaited = true;
    try std.testing.expect(worker.failure == null);
    try std.testing.expect(worker.proof == null);
    try worker.request.validate(cut.nowMs());
}

test "external lake native time series capture remains immutable after preflight and hint loss" {
    // This regression inspects physical DB hints. Control-only test owners
    // exercise the protocol contracts; the API runtime owner runs this case.
    if (comptime !@hasDecl(local, "storage_db_db")) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ true, false }) |linked| {
        var directory = try local.common_test_directory.TestDirectory.init("native-capture-idempotent");
        defer directory.cleanup();
        var repository = try Repository.init(a, null, null, .standalone, directory.path());
        defer repository.deinit();
        var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .manual, .filesystem_io = io });
        defer runtime.deinit();
        runtime.ptr().query_cut_repository = if (linked) repository.capability() else repository.chunkedCapability();
        const root = try std.fs.path.join(a, &.{ directory.path(), "db" });
        defer a.free(root);
        const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
        const options: local.storage_db_db.OpenOptions = .{ .backend_runtime = runtime.ptr(), .identity_namespace = namespace, .start_optional_runtimes = false };
        var db = try local.storage_db_db.DB.open(a, root, options);
        defer db.close();
        var stage: []const u8 = "seed capture";
        errdefer std.debug.print("\nnative capture linked={} stage={s}\n", .{ linked, stage });
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":1}" }} });
        const seed: cut.Request = .{ .id = &@as([64]u8, @splat('a')), .table_id = 7, .expires_ms = cut.nowMs() + 240000, .create = true };
        try db.captureQueryCut(seed, .none);
        var request = seed;
        request.id = &@as([64]u8, @splat('b'));
        stage = "initial reuse";
        request.timeout_ms = 60000;
        try db.captureQueryCut(request, .none);
        const cache = try cut.pathAlloc(a, root, request.id);
        defer a.free(cache);
        // The preflight generation reused retained extents without a local pin.
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, cache, .{}));
        stage = "repeated capture";
        var different_indexes = request;
        different_indexes.indexes = &.{"other_index"};
        try std.testing.expectError(error.CatalogGenerationChanged, db.captureQueryCut(different_indexes, .none));
        request.timeout_ms = 55000;
        try db.captureQueryCut(request, .none);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, cache, .{}));
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":2}" }} });
        stage = "capture after write";
        request.timeout_ms = 50000;
        try db.captureQueryCut(request, .none);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, cache, .{}));
        // Reopening discards process-local hints. Committed authority still wins
        // over the newer live data and the caller's shorter transport deadline.
        db.close();
        db = try local.storage_db_db.DB.open(a, root, options);
        stage = "capture after reopen";
        request.timeout_ms = 40000;
        try db.captureQueryCut(request, .none);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, cache, .{}));
        stage = "open retained read";
        var read_request = request;
        read_request.create = false;
        {
            var reader = try db.openQueryCut(read_request, .none);
            defer reader.close();
            const value = (try reader.get(a, "doc")).?;
            defer a.free(value);
            try std.testing.expectEqualStrings("{\"amount\":1}", value);
        }
        const live = (try db.get(a, "doc")).?;
        defer a.free(live);
        try std.testing.expectEqualStrings("{\"amount\":2}", live);
        // An invalid committed binding is an error, never permission to replace
        // its immutable generation with another snapshot of the live database.
        stage = "reject corrupt binding";
        db.local_execution.native_query_reuse = null;
        if (linked) {
            const retained_root = try repository.linkedRoot(a, request, namespace);
            defer a.free(retained_root);
            const path = try std.fs.path.join(a, &.{ retained_root, "query-cut.json" });
            defer a.free(path);
            const bytes = try local.storage_db_native_backup.readFileAlloc(a, io, path, max_manifest_bytes);
            defer a.free(bytes);
            var manifest = try std.json.parseFromSlice(cut.Manifest, a, bytes, .{});
            defer manifest.deinit();
            manifest.value.namespace.range_id += 1;
            const corrupt = try std.json.Stringify.valueAlloc(a, manifest.value, .{});
            defer a.free(corrupt);
            _ = try local.storage_db_native_backup.writeFileDurable(io, path, corrupt);
        } else {
            const bytes = try repository.readManifest(a, request, namespace, .none);
            defer a.free(bytes);
            var manifest = try std.json.parseFromSlice(Manifest, a, bytes, .{});
            defer manifest.deinit();
            manifest.value.namespace.range_id += 1;
            const corrupt = try std.json.Stringify.valueAlloc(a, manifest.value, .{});
            defer a.free(corrupt);
            const key = try repository.key(a, request, namespace);
            defer a.free(key);
            var object = try repository.store.opened.client.putObject(repository.store.opened.bucket, key, corrupt, .{});
            defer object.deinit(repository.store.opened.client.allocator);
        }
        try std.testing.expectError(error.CatalogGenerationChanged, db.captureQueryCut(request, .none));
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, cache, .{}));
    }
}

test "external lake native durable budget spans namespaces and survives owner restart and uncertain writes" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-budget");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var store = try repository.budgetStore(scratch);
    store.limits = .{ .max_cuts = 2, .max_bytes = 8 };
    var fault = objectstore.ScriptedFaultClient.init(a, store.client);
    defer fault.deinit();
    store.client = fault.client();
    const first: retention_budget.Entry = .{ .id = @splat('a'), .namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .expires_ms = cut.nowMs() + 300_000, .bytes = 8, .inventory = @splat(1), .linked = true };
    fault.commitNextPutThenFail(error.RemoteUnavailable);
    try std.testing.expectError(error.RemoteUnavailable, store.reserve(a, first, .none));
    // A lost response must keep its reservation. Retry does not double-charge.
    try store.reserve(a, first, .none);
    var alias = first;
    alias.id = @splat('b');
    fault.failNextPutBefore(error.PreconditionFailed);
    try store.reserve(a, alias, .none);
    var next = first;
    next.id = @splat('c');
    next.namespace.table_id = 8;
    next.inventory = @splat(2);
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, store.reserve(a, next, .none));
    var reopened = try Repository.init(a, null, null, .standalone, directory.path());
    defer reopened.deinit();
    var other = try reopened.budgetStore(scratch);
    other.limits = .{ .max_cuts = 3, .max_bytes = 8 };
    // Count capacity is now available, but distinct retained bytes still fail.
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, other.reserve(a, next, .none));
    try std.testing.expectEqual(@as(usize, 2), (try other.snapshot(scratch, .none)).len);
    // Expiry alone does not release storage capacity; maintenance must attest
    // that the physical generation and retired trash have both disappeared.
    const io = std.testing.io;
    try repository.releaseReclaimedBudget(io, first.namespace, first.expires_ms + 30_001, 8, true, .none);
    try std.testing.expectEqual(@as(usize, 0), (try other.snapshot(scratch, .none)).len);
    try other.reserve(a, next, .none);
}

test "external lake native durable admission preserves issued cursors at capacity and waits for reader GC" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-durable-admission");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    repository.retained_limits.max_cuts = 1;
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .manual, .filesystem_io = io });
    defer runtime.deinit();
    runtime.ptr().query_cut_repository = repository.capability();
    const root = try std.fmt.allocPrint(a, "{s}/db", .{directory.path()});
    defer a.free(root);
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    var db = try local.storage_db_db.DB.open(a, root, .{ .backend_runtime = runtime.ptr(), .identity_namespace = namespace });
    defer db.close();
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":1}" }} });
    const expires = cut.nowMs() + 300_000;
    const first: cut.Request = .{ .id = &@as([64]u8, @splat('a')), .table_id = 7, .expires_ms = expires, .create = true };
    var reader = try db.openQueryCut(first, .none);
    var reader_open = true;
    defer if (reader_open) reader.close();
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"amount\":2}" }} });
    const next: cut.Request = .{ .id = &@as([64]u8, @splat('b')), .table_id = 7, .expires_ms = expires, .create = true };
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, db.openQueryCut(next, .none));
    const original = (try reader.get(a, "doc")).?;
    defer a.free(original);
    try std.testing.expectEqualStrings("{\"amount\":1}", original);
    // The cache's lease does not address durable links. Open the repository's
    // lazy reader too, to exercise the expiry-cohort reclamation fence.
    var remote = (try repository.capability().openRead(io, "unused", first, namespace, .none)).?;
    var remote_open = true;
    defer if (remote_open) remote.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const store = try repository.budgetStore(arena.allocator());
    for (0..8) |_| try repository.collect(namespace, expires + 30_001, .none);
    try std.testing.expectEqual(@as(usize, 1), (try store.snapshot(arena.allocator(), .none)).len);
    remote.deinit();
    remote_open = false;
    reader.close();
    reader_open = false;
    for (0..8) |_| try repository.collect(namespace, expires + 30_001, .none);
    try std.testing.expectEqual(@as(usize, 0), (try store.snapshot(arena.allocator(), .none)).len);
}

test "external lake native durable budget retries a real competing owner revision" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-budget-race");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var backing = try repository.budgetStore(arena.allocator());
    backing.limits = .{ .max_cuts = 1, .max_bytes = 8 };
    const entry: retention_budget.Entry = .{ .id = @splat('a'), .namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .expires_ms = cut.nowMs() + 300_000, .bytes = 8, .inventory = @splat(1), .linked = true };
    try backing.reserve(a, entry, .none);
    try backing.release(a, entry, .none);
    const Competitor = struct {
        store: retention_budget.Store,
        entry: retention_budget.Entry,
        failure: ?anyerror = null,
        fn run(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.store.reserve(std.testing.allocator, self.entry, .none) catch |err| {
                self.failure = err;
            };
        }
    };
    var competitor: Competitor = .{ .store = backing, .entry = entry };
    competitor.entry.id = @splat('b');
    var fault = objectstore.ScriptedFaultClient.init(a, backing.client);
    defer fault.deinit();
    fault.next_get = .{ .complete_then_callback = .{ .ptr = &competitor, .run = Competitor.run } };
    var racing = backing;
    racing.client = fault.client();
    // B commits after A's read but before A's CAS. A must reload B's charge,
    // rather than overwriting it or issuing another cursor over the limit.
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, racing.reserve(a, entry, .none));
    try std.testing.expect(competitor.failure == null);
    const saved = try backing.snapshot(arena.allocator(), .none);
    try std.testing.expectEqual(@as(usize, 1), saved.len);
    try std.testing.expectEqual(competitor.entry.id, saved[0].id);
}

test "external lake native budget reclamation advances past leased cohorts across owners" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-budget-fairness");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    repository.retained_limits = .{ .max_cuts = 5, .max_bytes = 5 };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const store = try repository.budgetStore(scratch);
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const expires = cut.nowMs() + 60_000;
    for ([_]u8{ 'a', 'b', 'c', 'd' }, 0..) |letter, index| {
        try store.reserve(a, .{ .id = @splat(letter), .namespace = namespace, .expires_ms = if (index < 2) expires else expires - 1000, .bytes = 1, .inventory = @splat(letter), .linked = true }, .none);
    }
    // A not-yet-expired entry sorts before the held cohort. It must remain
    // charged without consuming the physical inspection budget.
    try store.reserve(a, .{ .id = @splat('0'), .namespace = namespace, .expires_ms = expires + 120_000, .bytes = 1, .inventory = @splat('0'), .linked = true }, .none);
    const parent = try repository.linkedNamespace(scratch, namespace);
    const held = try std.fmt.allocPrint(scratch, "{s}/{d:0>20}", .{ parent, expires });
    try fs.createDirPathPortable(io, held);
    const lease = try std.Io.Dir.cwd().createFile(io, try std.fs.path.join(scratch, &.{ held, ".lease" }), .{ .truncate = false });
    defer lease.close(io);
    try std.testing.expect(try lease.tryLock(io, .shared));
    defer lease.unlock(io);
    // The entire first inspection budget is consumed by one leased cohort.
    try repository.releaseReclaimedBudget(io, namespace, expires + 30_001, 2, true, .none);
    try std.testing.expectEqual(@as(usize, 5), (try store.snapshot(scratch, .none)).len);
    try std.testing.expectEqual(@as([64]u8, @splat('b')), (try store.scan(scratch, namespace, .none)).after.?);
    // A new adapter must resume from the persisted cursor, not begin at a/b.
    var reopened = try Repository.init(a, null, null, .standalone, directory.path());
    defer reopened.deinit();
    reopened.retained_limits = repository.retained_limits;
    try reopened.releaseReclaimedBudget(io, namespace, expires + 30_001, 2, true, .none);
    const remaining = try store.snapshot(scratch, .none);
    try std.testing.expectEqual(@as(usize, 3), remaining.len);
    for (remaining) |entry| try std.testing.expect(entry.id[0] == '0' or entry.id[0] == 'a' or entry.id[0] == 'b');
    try std.Io.Dir.cwd().access(io, held, .{});
    // Reclaimed store-wide capacity is available to an unrelated namespace
    // while the original reader continues holding its cohort.
    try store.reserve(a, .{ .id = @splat('e'), .namespace = .{ .table_id = 8, .shard_id = 2, .range_id = 2 }, .expires_ms = expires, .bytes = 1, .inventory = @splat('e'), .linked = true }, .none);
}

test "external lake native budget reclaims a batch with constant ledger IO" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-budget-batch");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const store = try repository.budgetStore(scratch);
    const namespace: Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const expires = cut.nowMs() + 60_000;
    for (0..128) |index| {
        var id: [64]u8 = undefined;
        _ = try std.fmt.bufPrint(&id, "{x:0>64}", .{index});
        try store.reserve(a, .{ .id = id, .namespace = namespace, .expires_ms = expires, .bytes = 1, .inventory = @splat(1), .linked = true }, .none);
    }
    const backing = repository.store.opened.client;
    var fault = objectstore.ScriptedFaultClient.init(a, backing);
    defer fault.deinit();
    repository.store.opened.client = fault.client();
    defer repository.store.opened.client = backing;
    try repository.releaseReclaimedBudget(std.testing.io, namespace, expires + 30_001, 128, true, .none);
    // One scan snapshot, one fresh CAS read, and one conditional write for
    // the entire batch; physical filesystem checks do not fetch the ledger.
    try std.testing.expectEqual(@as(u64, 2), fault.get_attempts);
    try std.testing.expectEqual(@as(u64, 1), fault.put_attempts);
    try std.testing.expectEqual(@as(usize, 0), (try store.snapshot(scratch, .none)).len);
}

test "external lake native budget batch retry preserves changed entries and newer scan positions" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-budget-batch-race");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const store = try repository.budgetStore(scratch);
    const entry: retention_budget.Entry = .{ .id = @splat('a'), .namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .expires_ms = cut.nowMs() + 60_000, .bytes = 1, .inventory = @splat(1), .linked = true };
    var second = entry;
    second.id = @splat('b');
    try store.reserve(a, entry, .none);
    try store.reserve(a, second, .none);
    const Competitor = struct {
        store: retention_budget.Store,
        entry: retention_budget.Entry,
        failure: ?anyerror = null,
        fn run(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // A cross-device fallback extends the entry's proof requirement.
            self.store.reserve(std.testing.allocator, self.entry, .none) catch |err| {
                self.failure = err;
                return;
            };
            self.store.releaseBatch(std.testing.allocator, &.{}, .{ .namespace = self.entry.namespace, .expected_after = null, .after = @splat('f') }, .none) catch |err| {
                self.failure = err;
            };
        }
    };
    var competitor: Competitor = .{ .store = store, .entry = entry };
    competitor.entry.linked = false;
    var fault = objectstore.ScriptedFaultClient.init(a, store.client);
    defer fault.deinit();
    fault.next_get = .{ .complete_then_callback = .{ .ptr = &competitor, .run = Competitor.run } };
    var racing = store;
    racing.client = fault.client();
    try racing.releaseBatch(a, &.{ entry, second }, .{ .namespace = entry.namespace, .expected_after = null, .after = @splat('b') }, .none);
    try std.testing.expect(competitor.failure == null);
    const saved = try store.scan(scratch, entry.namespace, .none);
    try std.testing.expectEqual(@as(usize, 1), saved.entries.len);
    try std.testing.expect(!saved.entries[0].linked);
    try std.testing.expectEqual(@as([64]u8, @splat('f')), saved.after.?);
    try std.testing.expectEqual(@as(u64, 2), fault.get_attempts);
    try std.testing.expectEqual(@as(u64, 2), fault.put_attempts);
}
