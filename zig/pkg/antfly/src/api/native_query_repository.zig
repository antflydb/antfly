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

//! Complete retained native generations over the configured artifact provider.
//! Transfer uses bounded authenticated chunks, then one immutable manifest.
//! Restored filesystem trees are disposable caches of that durable authority.
const std = @import("std");
const local = @import("antfly_local_sources");
const cut = local.storage_db_native_query_cut;
const port = local.storage_db_native_query_cut_repository;
const objectstore = @import("objectstore");
const Store = @import("lake_index_store.zig").Store;
const artifacts = @import("../serverless/artifacts/store.zig");
const fs = @import("antfly_runtime_fs").fs_paths;
const A = std.mem.Allocator;
const Namespace = local.storage_db_doc_identity_namespace.Namespace;
const Cancellation = @import("antfly_cancellation").CancellationToken;
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
const Manifest = struct { version: u16 = 1, request: cut.Request, namespace: Namespace, sequence: u64, files: []const File };
pub const Repository = struct {
    a: A,
    store: Store,
    limits: cut.Limits = .{},
    owned_config: ?local.common_config.Config = null,
    owned_base_dir: ?[]u8 = null,
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
        const bytes = if (config) |value| try std.json.Stringify.valueAlloc(a, .{ .storage = .{ .engine = "local", .artifacts = if (value.storage.artifacts.connection != null) @as(?local.common_config.Config.ObjectStorageLocation, value.storage.artifacts) else null, .local = .{ .base_dir = base_dir orelse value.storage.local_base_dir orelse "." } }, .connections = connections, .lake_indexes = value.lake_indexes, .deployment_mode = deployment }, .{ .emit_null_optional_fields = false }) else try a.dupe(u8, "{}");
        defer a.free(bytes);
        return std.json.Stringify.valueAlloc(a, Setup{ .config_json = bytes, .deployment = deployment, .base_dir = base_dir }, .{});
    }
    pub fn initFromSetup(a: A, bytes: []const u8, secrets: ?*local.common_secrets.FileStore) !Repository {
        var parsed = try std.json.parseFromSlice(Setup, a, bytes, .{});
        defer parsed.deinit();
        var config = try local.common_config.Config.parseFromSlice(a, parsed.value.config_json);
        errdefer config.deinit();
        config.deployment_mode = parsed.value.deployment;
        const base = if (parsed.value.base_dir) |value| try a.dupe(u8, value) else null;
        errdefer if (base) |value| a.free(value);
        var repository = try init(a, &config, secrets, parsed.value.deployment, base);
        repository.owned_config = config;
        repository.owned_base_dir = base;
        return repository;
    }
    pub fn init(a: A, config: ?*const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, deployment: local.common_config.DeploymentMode, base_dir: ?[]const u8) !Repository {
        return .{ .a = a, .store = try Store.openNative(a, config, secrets, false, deployment, base_dir), .limits = if (config) |value| .{ .max_cuts = value.lake_indexes.query_cursors.max_native_cuts, .max_bytes = value.lake_indexes.query_cursors.max_native_retained_bytes } else .{} };
    }
    pub fn deinit(self: *Repository) void {
        self.store.deinit();
        if (self.owned_config) |*value| value.deinit();
        if (self.owned_base_dir) |value| self.a.free(value);
    }
    pub fn capability(self: *Repository) port.Port {
        const vtable: port.VTable = .{ .publish = publish, .recover = recover, .open_read = openRead, .warm = warm };
        return .{ .limits = self.limits, .ptr = self, .vtable = &vtable, .dispatch = @import("antfly_local_sources").runtime_callback_abi.Boundary(port.VTable).local_dispatch };
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
        try cancellation.check();
        const bridge: TokenBridge = .{ .token = cancellation };
        var result = self.store.opened.client.getObject(self.store.opened.bucket, try self.key(a, request, namespace), .{ .skip_metadata_probe = true, .max_response_bytes = max_manifest_bytes, .cancellation = bridge.object() }) catch |err| {
            try cancellation.check();
            return switch (err) {
                error.NotFound, error.ObjectNotFound, error.FileNotFound => error.CatalogGenerationChanged,
                else => err,
            };
        };
        defer result.deinit(self.store.opened.client.allocator);
        try cancellation.check();
        return a.dupe(u8, result.body);
    }
    fn checkedManifest(a: A, bytes: []const u8, request: cut.Request, namespace: Namespace) !Manifest {
        try request.validate(cut.nowMs());
        const manifest = std.json.parseFromSliceLeaky(Manifest, a, bytes, .{ .allocate = .alloc_always }) catch return error.CatalogGenerationChanged;
        if (manifest.version != 1 or !manifest.namespace.eql(namespace) or manifest.request.table_id != request.table_id or manifest.request.expires_ms != request.expires_ms or !std.mem.eql(u8, manifest.request.id, request.id) or manifest.files.len > local.storage_db_native_backup_seal.max_files) return error.CatalogGenerationChanged;
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
    fn collectCache(a: A, io: std.Io, parent: []const u8, cancellation: Cancellation) !void {
        const path = try std.fmt.allocPrint(a, "{s}/.chunk-cache", .{parent});
        var directory = try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
        defer directory.close(io);
        var iterator = directory.iterate();
        var inspected: usize = 0;
        while (try iterator.next(io)) |entry| {
            try cancellation.check();
            if (inspected == 128) break;
            inspected += 1;
            if (entry.kind != .directory) continue;
            const expires = std.fmt.parseInt(u64, entry.name, 10) catch continue;
            if (expires +| 30_000 >= cut.nowMs()) continue;
            try directory.deleteTree(io, entry.name);
        }
    }
    pub fn collect(self: *Repository, namespace: Namespace, now: u64, cancellation: Cancellation) !void {
        return self.collectBounded(namespace, now, 128, cancellation);
    }
    fn collectBounded(self: *Repository, namespace: Namespace, now: u64, budget: usize, cancellation: Cancellation) !void {
        if (budget == 0) return;
        const Visitor = struct {
            store: *artifacts.ArtifactStore,
            deleted: usize = 0,
            fn visit(raw: *anyopaque, _: artifacts.UploadScope, id: []const u8) !void {
                const visitor: *@This() = @ptrCast(@alignCast(raw));
                try visitor.store.delete(id);
                visitor.deleted += 1;
            }
        };
        var store = self.store.artifactStore();
        const cut_identity = try @import("native_retained_cut.zig").storeIdentity(self.a, self.store.locator);
        const descriptors = try @import("native_retained_cut.zig").collectBounded(&store, namespace.table_id, cut_identity, now, budget, cancellation);
        if (descriptors == budget) return;
        var visitor: Visitor = .{ .store = &store, .deleted = descriptors };
        store.visitScopedUploads(try self.domain(self.a, namespace), .{ .ptr = &visitor, .visit = Visitor.visit, .fencing_cutoff = now -| 30_000, .max_entries = budget - descriptors }, cancellation) catch |err| switch (err) {
            error.ArtifactEnumerationPaused => {},
            else => return err,
        };
        if (visitor.deleted >= budget) return;
        const remaining = budget - visitor.deleted;
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
        try cancellation.check();
    }
    fn ownerPrefix(self: *Repository, a: A) ![]u8 {
        return std.fmt.allocPrint(a, "{s}{s}native-query-owners/", .{ self.store.opened.prefix, if (self.store.opened.prefix.len == 0) "" else "/" });
    }
    fn register(self: *Repository, a: A, namespace: Namespace, cancellation: Cancellation) !void {
        const key_bytes = try std.fmt.allocPrint(a, "{s}{d}/{d}/{d}.json", .{ try self.ownerPrefix(a), namespace.table_id, namespace.shard_id, namespace.range_id });
        const bytes = try std.json.Stringify.valueAlloc(a, namespace, .{});
        const bridge: TokenBridge = .{ .token = cancellation };
        var result = self.store.opened.client.putObject(self.store.opened.bucket, key_bytes, bytes, .{ .if_none_match = true, .cancellation = bridge.object() }) catch |err| {
            try cancellation.check();
            switch (err) {
                error.PreconditionFailed, error.ConditionalCheckFailed => return,
                else => return err,
            }
        };
        result.deinit(self.store.opened.client.allocator);
        try cancellation.check();
    }
    /// One namespace per supervised pass. The caller owns the returned cursor;
    /// registry witnesses survive failed upload, owner loss and table drop.
    pub fn collectNext(self: *Repository, a: A, after: ?[]const u8, now: u64, budget: usize, cancellation: Cancellation) !?[]u8 {
        const prefix = try self.ownerPrefix(a);
        defer a.free(prefix);
        const bridge: TokenBridge = .{ .token = cancellation };
        var page = self.store.opened.client.listObjects(self.store.opened.bucket, .{ .prefix = prefix, .recursive = true, .max_keys = 1, .start_after = after, .cancellation = bridge.object() }) catch |err| {
            try cancellation.check();
            return err;
        };
        defer page.deinit(self.store.opened.client.allocator);
        if (page.entries.len == 0) return null;
        const key_bytes = page.entries[0].key;
        if (!std.mem.startsWith(u8, key_bytes, prefix) or !std.mem.endsWith(u8, key_bytes, ".json")) return error.CatalogGenerationChanged;
        var parts = std.mem.splitScalar(u8, key_bytes[prefix.len .. key_bytes.len - 5], '/');
        const table = std.fmt.parseInt(u64, parts.next() orelse return error.CatalogGenerationChanged, 10) catch return error.CatalogGenerationChanged;
        const shard = std.fmt.parseInt(u64, parts.next() orelse return error.CatalogGenerationChanged, 10) catch return error.CatalogGenerationChanged;
        const range = std.fmt.parseInt(u64, parts.next() orelse return error.CatalogGenerationChanged, 10) catch return error.CatalogGenerationChanged;
        if (parts.next() != null or table == 0) return error.CatalogGenerationChanged;
        try self.collectBounded(.{ .table_id = table, .shard_id = shard, .range_id = range }, now, budget, cancellation);
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
    fn checkpointExtent(self: *Repository, a: A, namespace: Namespace, digest: [32]u8, expected_domain: [32]u8, expires: u64, source: local.storage_db_native_backup_seal.File, needed_until: u64, cancellation: Cancellation) !?[]const Ref {
        const bridge: TokenBridge = .{ .token = cancellation };
        for ([_]u64{ expires, expires +| cache_window_ms, expires -| cache_window_ms }) |epoch| {
            try cancellation.check();
            var object = self.store.opened.client.getObject(self.store.opened.bucket, try self.checkpointKey(a, namespace, epoch, digest), .{ .skip_metadata_probe = true, .max_response_bytes = max_manifest_bytes, .cancellation = bridge.object() }) catch |err| {
                try cancellation.check();
                switch (err) {
                    error.ObjectNotFound, error.NotFound, error.FileNotFound, error.NoSuchKey => continue,
                    else => return err,
                }
            };
            defer object.deinit(self.store.opened.client.allocator);
            if (try checkedExtent(a, object.body, expected_domain, epoch, source, true, needed_until)) |refs| return refs;
        }
        return null;
    }
    fn publishCheckpoint(self: *Repository, a: A, namespace: Namespace, expires: u64, digest: [32]u8, bytes: []const u8, cancellation: Cancellation) !void {
        const bridge: TokenBridge = .{ .token = cancellation };
        const location = try self.checkpointKey(a, namespace, expires, digest);
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
    fn markCommitted(self: *Repository, a: A, io: std.Io, root: []const u8, request: cut.Request, namespace: Namespace) !void {
        const path = try std.fmt.allocPrint(a, "{s}/query-remote.json", .{root});
        const bytes = try std.json.Stringify.valueAlloc(a, LocalCommit{ .domain = try self.domain(a, namespace), .request = request, .namespace = namespace }, .{});
        _ = try local.storage_db_native_backup.writeFileDurable(io, path, bytes);
    }
    fn publish(raw: *anyopaque, io: std.Io, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !void {
        _ = try publishLimited(raw, io, root, request, namespace, cancellation, std.math.maxInt(u64));
    }
    fn warm(raw: *anyopaque, io: std.Io, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation, max_bytes: u64) !bool {
        if (max_bytes < chunk_bytes or max_bytes > 32 * 1024 * 1024) return error.InvalidQueryRequest;
        return publishLimited(raw, io, root, request, namespace, cancellation, max_bytes);
    }
    fn publishLimited(raw: *anyopaque, io: std.Io, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation, max_bytes: u64) !bool {
        const self = from(raw);
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        try cancellation.check();
        try cut.validate(a, io, root, request, namespace, cancellation);
        if (try self.localCommit(a, io, root, request, namespace)) {
            try cancellation.check();
            return true;
        }
        try self.register(a, namespace, cancellation);
        try self.collect(namespace, cut.nowMs(), cancellation);
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
        try collectCache(a, io, parent, cancellation);
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
            if (try self.checkpointExtent(a, namespace, digest, generation_domain, chunk_expiry, file, request.expires_ms, cancellation)) |cached| {
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
                    return true;
                },
                else => return err,
            }
        };
        result.deinit(self.store.opened.client.allocator);
        try self.markCommitted(a, io, root, request, namespace);
        try cancellation.check();
        return true;
    }
    fn readChunk(raw: *anyopaque, a: A, ref: Ref, cancellation: Cancellation) ![]u8 {
        const self: *Repository = @ptrCast(@alignCast(raw));
        var store = self.store.artifactStore();
        store.allocator = a;
        return store.getVerifiedAllocWithCancellation(ref.artifact_id, @intCast(ref.byte_len), ref.checksum, cancellation) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound, error.ArtifactIntegrityMismatch => error.CatalogGenerationChanged,
            else => err,
        };
    }
    fn openRead(raw: *anyopaque, io: std.Io, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !port.StorageLease {
        const self: *Repository = @ptrCast(@alignCast(raw));
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const manifest = try checkedManifest(a, try self.readManifest(a, request, namespace, cancellation), request, namespace);
        const expected_domain = try self.domain(a, namespace);
        // Publication may reuse the adjacent warmer epoch (one additional
        // hour) beyond its ordinary two-window chunk horizon.
        for (manifest.files) |file| for (file.chunks) |ref| {
            const scope = (try artifacts.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.CatalogGenerationChanged;
            if (!std.mem.eql(u8, &scope.domain, &expected_domain) or scope.fencingToken() < request.expires_ms or scope.fencingToken() > request.expires_ms +| 3 * cache_window_ms) return error.CatalogGenerationChanged;
        };
        const expiry = @import("antfly_platform").time.monotonicNs() +| (request.expires_ms -| cut.nowMs()) * std.time.ns_per_ms;
        const control: cut.Control = .{ .parent = cancellation, .deadline_ns = if (request.timeout_ms) |value| @min(expiry, @import("antfly_platform").time.monotonicNs() +| value *| std.time.ns_per_ms) else expiry };
        const reader = try port.remote_storage.Reader.create(self.a, io, root, manifest.files, .{ .ptr = self, .read = readChunk }, control);
        return reader.lease();
    }
    fn recover(raw: *anyopaque, io: std.Io, root: []const u8, request: cut.Request, namespace: Namespace, cancellation: Cancellation) !void {
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
    try repository.capability().publish(io, first_root, first, namespace, .none);
    // Reboot/loss of all local hints must reuse the durable checkpoint.
    try std.Io.Dir.cwd().deleteTree(io, try std.fmt.allocPrint(scratch, "{s}/.chunk-cache", .{directory.path()}));
    try repository.capability().publish(io, second_root, second, namespace, .none);
    // Local commit evidence is private cache metadata, never snapshot data.
    try std.testing.expect(try repository.localCommit(scratch, io, first_root, first, namespace));
    try cut.finish(a, io, first_root, first, namespace, 1, .none);
    try repository.capability().publish(io, first_root, first, namespace, .none);
    const first_manifest = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, first, namespace, .none), first, namespace);
    const second_manifest = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, second, namespace, .none), second, namespace);
    try std.testing.expectEqual(@as(usize, 1), first_manifest.files.len);
    try std.testing.expectEqualStrings(first_manifest.files[0].chunks[0].artifact_id, second_manifest.files[0].chunks[0].artifact_id);
    const recovered = try std.fmt.allocPrint(scratch, "{s}/recovered", .{directory.path()});
    try repository.capability().recover(io, recovered, first, namespace, .none);
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
    var remote = (try repository.capability().openRead(io, virtual_root, first, namespace, .none)).?;
    defer remote.deinit();
    const virtual_file = try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{virtual_root});
    const remote_bytes = try remote.view.readFileRangeAlloc(a, virtual_file, 9, 9);
    defer a.free(remote_bytes);
    try std.testing.expectEqualStrings("immutable", remote_bytes);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, virtual_root, .{}));
    try std.testing.expectError(error.ReadOnly, remote.view.writeFileAbsolute(virtual_file, "replacement"));
    var store = repository.store.artifactStore();
    const retained = @import("native_retained_cut.zig");
    const table = .{ .table_id = @as(u64, 7), .schema_json = "{}", .read_schema_json = "{}", .indexes_json = "{}" };
    const identity = try retained.storeIdentity(a, repository.store.locator);
    const capability = try retained.save(a, &store, identity, io, table, cut.nowMs(), .none);
    defer a.free(capability);
    try store.delete(first_manifest.files[0].chunks[0].artifact_id);
    // An uncached reader detects authoritative loss when it reads a page.
    // Open itself only authenticates the manifest and chunk references.
    var lost = (try repository.capability().openRead(io, virtual_root, second, namespace, .none)).?;
    defer lost.deinit();
    try std.testing.expectError(error.CatalogGenerationChanged, lost.view.readFileRangeAlloc(a, virtual_file, 0, 1));
    const missing = try std.fmt.allocPrint(scratch, "{s}/missing", .{directory.path()});
    try std.testing.expectError(error.CatalogGenerationChanged, repository.capability().recover(io, missing, second, namespace, .none));
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

test "external lake native repository warming resumes bounded uploads after restart" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-cut-warming");
    defer directory.cleanup();
    var repository = try Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
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
    try std.testing.expectError(error.InvalidQueryRequest, repository.capability().warm(io, root, request, namespace, .none, chunk_bytes - 1));
    try std.testing.expect(!try repository.capability().warm(io, root, request, namespace, .none, chunk_bytes));
    try std.testing.expectError(error.CatalogGenerationChanged, repository.readManifest(scratch, request, namespace, .none));
    repository.deinit();
    repository = try Repository.init(a, null, null, .standalone, directory.path());
    try std.testing.expect(!try repository.capability().warm(io, root, request, namespace, .none, chunk_bytes));
    try std.testing.expectError(error.CatalogGenerationChanged, repository.readManifest(scratch, request, namespace, .none));
    try std.testing.expect(try repository.capability().warm(io, root, request, namespace, .none, chunk_bytes));
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
    try std.testing.expect(try repository.capability().warm(io, short_root, short_request, namespace, .none, chunk_bytes));
    const short_manifest = try Repository.checkedManifest(scratch, try repository.readManifest(scratch, short_request, namespace, .none), short_request, namespace);
    try std.testing.expectEqualStrings(manifest.files[0].chunks[0].artifact_id, short_manifest.files[0].chunks[0].artifact_id);
    // The neighboring warmer epoch can extend beyond the foreground cut's
    // two-window horizon. Both virtual reads and fallback recovery must accept
    // this authenticated reuse without weakening namespace or expiry checks.
    const short_remote_path = try std.fmt.allocPrint(scratch, "{s}/short-remote", .{directory.path()});
    var short_remote = (try repository.capability().openRead(io, short_remote_path, short_request, namespace, .none)).?;
    defer short_remote.deinit();
    const short_tail = try short_remote.view.readFileRangeAlloc(a, try std.fmt.allocPrint(scratch, "{s}/immutable.sst", .{short_remote_path}), 2 * chunk_bytes, 17);
    defer a.free(short_tail);
    try std.testing.expectEqualSlices(u8, bytes[2 * chunk_bytes ..][0..17], short_tail);
    const short_recovered = try std.fmt.allocPrint(scratch, "{s}/short-recovered", .{directory.path()});
    try repository.capability().recover(io, short_recovered, short_request, namespace, .none);
    try repository.capability().publish(io, root, request, namespace, .none);
    var remote = (try repository.capability().openRead(io, try std.fmt.allocPrint(scratch, "{s}/remote", .{directory.path()}), request, namespace, .none)).?;
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
    try std.testing.expectError(error.Canceled, repository.capability().publish(std.testing.io, directory.path(), request, .{ .table_id = 7, .shard_id = 1 }, token));
    try std.testing.expectError(error.Canceled, repository.capability().recover(std.testing.io, directory.path(), request, .{ .table_id = 7, .shard_id = 1 }, token));
    try std.testing.expectError(error.Canceled, repository.capability().openRead(std.testing.io, directory.path(), request, .{ .table_id = 7, .shard_id = 1 }, token));
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
