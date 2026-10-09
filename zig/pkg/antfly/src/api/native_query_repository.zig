// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const Ref = struct { artifact_id: []const u8, byte_len: u64, checksum: []const u8 };
const File = struct { path: []const u8, size: u64, chunks: []const Ref };
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
        const vtable: port.VTable = .{ .publish = publish, .recover = recover };
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
    fn cachedFile(a: A, io: std.Io, path: []const u8, expected_domain: [32]u8, expires: u64, source: local.storage_db_native_backup_seal.File) !?[]const Ref {
        const bytes = local.storage_db_native_backup.readFileAlloc(a, io, path, max_manifest_bytes) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        const cached = std.json.parseFromSliceLeaky(CachedFile, a, bytes, .{}) catch return null;
        if (!std.mem.eql(u8, &cached.domain, &expected_domain) or cached.expires_ms != expires or cached.source.inode != source.inode or cached.source.size != source.size or cached.source.mtime_ns != source.mtime_ns or !std.mem.eql(u8, cached.source.path, source.path)) return null;
        var size: u64 = 0;
        for (cached.chunks) |ref| {
            if (ref.byte_len == 0 or ref.byte_len > chunk_bytes) return null;
            artifacts.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum) catch return null;
            const scope = (artifacts.uploadScopeFromArtifactId(ref.artifact_id) catch return null) orelse return null;
            if (!std.mem.eql(u8, &scope.domain, &expected_domain) or scope.fencingToken() != expires) return null;
            size = std.math.add(u64, size, ref.byte_len) catch return null;
        }
        if (size != source.size) return null;
        return cached.chunks;
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
        const self = from(raw);
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        try cancellation.check();
        try cut.validate(a, io, root, request, namespace, cancellation);
        if (try self.localCommit(a, io, root, request, namespace)) {
            try cancellation.check();
            return;
        }
        try self.register(a, namespace, cancellation);
        try self.collect(namespace, cut.nowMs(), cancellation);
        // An immutable commit proves a prior attempt completed every chunk.
        if (self.readManifest(a, request, namespace, cancellation)) |bytes| {
            _ = try checkedManifest(a, bytes, request, namespace);
            try self.markCommitted(a, io, root, request, namespace);
            try cancellation.check();
            return;
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
            if (try cachedFile(a, io, cache_path, generation_domain, chunk_expiry, file)) |cached| {
                try files.append(a, .{ .path = file.path, .size = file.size, .chunks = cached });
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
            while (offset < file.size) {
                try cancellation.check();
                const wanted: usize = @intCast(@min(chunk_bytes, file.size - offset));
                if (try source.readPositionalAll(io, buffer[0..wanted], offset) != wanted) return error.CatalogGenerationChanged;
                var metadata = try store.putScoped(scope, buffer[0..wanted], cancellation);
                defer metadata.deinit(store.allocator);
                try chunks.append(a, .{ .artifact_id = try a.dupe(u8, metadata.artifact_id), .byte_len = metadata.byte_len, .checksum = try a.dupe(u8, metadata.checksum) });
                offset += wanted;
            }
            const refs = try chunks.toOwnedSlice(a);
            try files.append(a, .{ .path = file.path, .size = file.size, .chunks = refs });
            const cache_bytes = try std.json.Stringify.valueAlloc(a, CachedFile{ .domain = generation_domain, .expires_ms = chunk_expiry, .source = file, .chunks = refs }, .{});
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
                    return;
                },
                else => return err,
            }
        };
        result.deinit(self.store.opened.client.allocator);
        try self.markCommitted(a, io, root, request, namespace);
        try cancellation.check();
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
        for (manifest.files) |file| {
            try cancellation.check();
            const destination = try std.fmt.allocPrint(a, "{s}/{s}", .{ staging, file.path });
            if (std.fs.path.dirname(destination)) |parent| try fs.createDirPathPortable(io, parent);
            var output = try fs.createFilePortable(io, destination, .{ .exclusive = true });
            defer output.close(io);
            var offset: u64 = 0;
            for (file.chunks) |ref| {
                const scope = (try artifacts.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.CatalogGenerationChanged;
                if (!std.mem.eql(u8, &scope.domain, &expected_domain) or (scope.fencingToken() < request.expires_ms or scope.fencingToken() > request.expires_ms +| 2 * cache_window_ms)) return error.CatalogGenerationChanged;
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
    var store = repository.store.artifactStore();
    const retained = @import("native_retained_cut.zig");
    const table = .{ .table_id = @as(u64, 7), .schema_json = "{}", .read_schema_json = "{}", .indexes_json = "{}" };
    const identity = try retained.storeIdentity(a, repository.store.locator);
    const capability = try retained.save(a, &store, identity, io, table, cut.nowMs(), .none);
    defer a.free(capability);
    try store.delete(first_manifest.files[0].chunks[0].artifact_id);
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
