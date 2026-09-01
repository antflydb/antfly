// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const support = @import("embedded_support");
const db_mod = support.db;
const db_core = support.db_core;
const lite_restore_staging = support.lite.restore_staging;
const template_remote_host = support.template_remote_host;

pub const lsm_storage = support.lsm_storage;
pub const enrichment_runtime = support.enrichment_runtime;
pub const enrichment_embedder = support.enrichment_embedder;
pub const ttl_runtime = support.ttl_runtime;

const Allocator = std.mem.Allocator;
const IndexBackendOptions = @TypeOf((db_mod.OpenOptions{}).index_backends);

pub const types = support.db_types;
pub const RemoteTemplateRenderConfig = template_remote_host.RenderConfig;
pub const RemoteTemplateRenderer = template_remote_host.HostRenderer;
pub const LiteCheckReport = support.lite.backend.CheckReport;
pub const LiteStableSnapshotReport = support.lite.backend.StableSnapshotReport;
pub const LiteVacuumReport = support.lite.backend.VacuumReport;
pub const LiteStorageStatus = support.lite.backend.StorageStatus;
pub const LiteStatus = support.lite.backend.FullStatus;

pub const OpenOptions = struct {
    open_mode: db_mod.OpenOptions.OpenMode = .writer,
    map_size: usize = 256 * 1024 * 1024,
    no_sync: bool = false,
    primary_backend: db_mod.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } },
    storage: ?support.lsm_storage.Storage = null,
    index_backends: IndexBackendOptions = .{},
    enrichment: ?support.enrichment_runtime.Config = null,
    inference: support.lite.backend.InferenceOpenOptions = .{},
    ttl_cleanup: ttl_runtime.Config = .{},
};

pub const Profile = support.lite.backend.Profile;
pub const Capabilities = support.lite.backend.Capabilities;
pub const InferenceOpenOptions = support.lite.backend.InferenceOpenOptions;
pub const InferenceStatus = support.lite.backend.InferenceStatus;

pub const DB = struct {
    allocator: Allocator,
    inner: *db_mod.DB,
    profile: Profile,
    open_mode: db_mod.OpenOptions.OpenMode,
    lite_inference_status: ?InferenceStatus = null,
    owned_lite_backend: ?support.lite.backend.Handle = null,

    pub fn open(alloc: Allocator, path: []const u8, opts: OpenOptions) !DB {
        return try openWithProfile(alloc, path, opts, .native);
    }

    pub fn openHosted(alloc: Allocator, path: []const u8, opts: OpenOptions) !DB {
        return try openWithProfile(alloc, path, opts, .hosted);
    }

    pub fn openLite(alloc: Allocator, path: []const u8, opts: OpenOptions) !DB {
        return try openLiteWithProfile(alloc, path, opts, .native);
    }

    pub fn createLite(alloc: Allocator, path: []const u8, opts: OpenOptions) !DB {
        return try createLiteWithProfile(alloc, path, opts, .native);
    }

    pub fn openLiteHosted(alloc: Allocator, path: []const u8, opts: OpenOptions) !DB {
        return try openLiteWithProfile(alloc, path, opts, .hosted);
    }

    pub fn createLiteHosted(alloc: Allocator, path: []const u8, opts: OpenOptions) !DB {
        return try createLiteWithProfile(alloc, path, opts, .hosted);
    }

    pub fn openWithProfile(alloc: Allocator, path: []const u8, opts: OpenOptions, profile: Profile) !DB {
        return .{
            .allocator = alloc,
            .inner = try db_mod.DB.openOwned(alloc, path, toDbOpenOptions(opts, profile)),
            .profile = profile,
            .open_mode = opts.open_mode,
            .lite_inference_status = null,
        };
    }

    pub fn openLiteWithProfile(alloc: Allocator, path: []const u8, opts: OpenOptions, profile: Profile) !DB {
        var lite_backend = try support.lite.backend.Handle.open(alloc, path, .{
            .read_only = openModeRequiresReadOnlyBackends(opts.open_mode),
            .no_sync = opts.no_sync,
        });
        return try openWithLiteBackend(alloc, path, opts, profile, &lite_backend);
    }

    pub fn createLiteWithProfile(alloc: Allocator, path: []const u8, opts: OpenOptions, profile: Profile) !DB {
        if (!openModeCanWrite(opts.open_mode)) return error.InvalidArgument;
        var lite_backend = try support.lite.backend.Handle.createWithOptions(alloc, path, .{
            .exclusive = true,
            .no_sync = opts.no_sync,
        });
        return try openWithLiteBackend(alloc, path, opts, profile, &lite_backend);
    }

    fn openWithLiteBackend(alloc: Allocator, path: []const u8, opts: OpenOptions, profile: Profile, lite_backend: *support.lite.backend.Handle) !DB {
        errdefer lite_backend.deinit();

        var db_opts = toDbOpenOptions(opts, profile);
        try lite_backend.configureDbOpenOptions(&db_opts);

        const inner = db_mod.DB.openOwned(alloc, path, db_opts) catch |err| {
            return err;
        };

        const moved_lite_backend = lite_backend.*;
        lite_backend.* = undefined;
        return .{
            .allocator = alloc,
            .inner = inner,
            .profile = profile,
            .open_mode = opts.open_mode,
            .lite_inference_status = support.lite.backend.inferenceStatusForProfileWithOptions(profile, opts.inference),
            .owned_lite_backend = moved_lite_backend,
        };
    }

    pub fn close(self: *DB) void {
        if (self.owned_lite_backend != null and openModeCanWrite(self.open_mode)) {
            self.inner.sync(true) catch {};
            self.inner.syncIndexes(true) catch {};
        }
        self.inner.closeOwned();
        if (self.owned_lite_backend) |*lite_backend| {
            lite_backend.deinit();
        }
        self.* = undefined;
    }

    pub fn engine(self: *DB) db_core.Engine {
        return self.inner.engine();
    }

    pub fn maintenanceDriver(self: *DB) db_core.MaintenanceDriver {
        return self.inner.maintenanceDriver();
    }

    pub fn services(self: *DB) db_core.Services {
        return self.inner.services();
    }

    pub fn capabilities(self: *DB) Capabilities {
        if (self.lite_inference_status) |status| {
            return support.lite.backend.capabilitiesForProfileWithInferenceStatus(self.profile, status);
        }
        return capabilitiesForProfile(self.profile);
    }

    pub fn liteStorageStatus(self: *DB) !LiteStorageStatus {
        const backend = if (self.owned_lite_backend) |*lite_backend| lite_backend else return error.NotLiteDatabase;
        return backend.storageStatus();
    }

    pub fn liteStatus(self: *DB, alloc: Allocator) !LiteStatus {
        const backend = if (self.owned_lite_backend) |*lite_backend| lite_backend else return error.NotLiteDatabase;
        var status = try backend.fullStatus(alloc, &self.inner, self.profile);
        status.inference = self.lite_inference_status orelse status.inference;
        status.capabilities = support.lite.backend.capabilitiesForProfileWithInferenceStatus(self.profile, status.inference);
        return status;
    }

    pub fn batch(self: *DB, req: types.BatchRequest) !void {
        try self.inner.batch(req);
    }

    pub fn lookup(self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) !?types.LookupResult {
        return try self.inner.lookup(alloc, key, opts);
    }

    pub fn scan(self: *DB, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) !types.ScanResult {
        return try self.inner.scan(alloc, from_key, to_key, opts);
    }

    pub fn search(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
        return try self.inner.search(alloc, req);
    }

    pub fn stats(self: *DB, alloc: Allocator) !types.DBStats {
        return try self.inner.stats(alloc);
    }

    pub fn listIndexes(self: *DB, alloc: Allocator) ![]types.IndexConfig {
        return try self.inner.listIndexes(alloc);
    }

    pub fn compactTextIndexes(self: *DB) !void {
        try self.inner.compactTextIndexes();
    }

    pub fn drainScheduledTextMerges(self: *DB) !void {
        try self.inner.drainScheduledTextMerges();
    }

    pub fn forceCompactTextIndexes(self: *DB) !void {
        try self.inner.forceCompactTextIndexes();
    }

    pub fn textIndexLayoutStats(self: *DB, alloc: Allocator, index_name: []const u8) !types.TextIndexLayoutStats {
        return try self.inner.textIndexLayoutStats(alloc, index_name);
    }

    pub fn searchTextKernel(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        text_query: types.TextQuery,
        options: types.TextKernelSearchOptions,
    ) !types.TextKernelResult {
        return try self.inner.searchTextKernel(alloc, index_name, text_query, options);
    }

    pub fn countTextKernel(self: *DB, alloc: Allocator, index_name: []const u8, text_query: types.TextQuery) !u32 {
        return try self.inner.countTextKernel(alloc, index_name, text_query);
    }

    pub fn bestEffortForceCompactTextIndexes(self: *DB) !void {
        try self.inner.bestEffortForceCompactTextIndexes();
    }

    pub fn listEnrichments(self: *DB, alloc: Allocator) ![]types.EnrichmentConfig {
        return try self.inner.listEnrichments(alloc);
    }

    pub fn pendingWorkStats(self: *DB) db_core.PendingWorkStats {
        return self.maintenanceDriver().pendingWorkStats();
    }

    pub fn runUntilIdle(self: *DB) !void {
        try self.maintenanceDriver().runUntilIdle();
    }

    pub fn replayGeneratedEnrichmentsFromStoredDocs(self: *DB, alloc: Allocator) !usize {
        return try self.inner.replayGeneratedEnrichmentsFromStoredDocs(alloc);
    }

    pub fn addIndex(self: *DB, cfg: types.IndexConfig) !void {
        try self.inner.addIndex(cfg);
    }

    pub fn deleteIndex(self: *DB, name: []const u8) !bool {
        return try self.inner.deleteIndex(name);
    }

    pub fn addEnrichment(self: *DB, cfg: types.EnrichmentConfig) !void {
        try self.inner.addEnrichment(cfg);
    }

    pub fn deleteEnrichment(self: *DB, kind: types.EnrichmentKind, name: []const u8) !bool {
        return try self.inner.deleteEnrichment(kind, name);
    }

    pub fn setSchema(self: *DB, table_schema: support.schema.TableSchema) !void {
        try self.inner.setSchema(table_schema);
    }

    pub fn setSchemaJson(self: *DB, alloc: Allocator, schema_json: []const u8) !void {
        try self.inner.setSchemaJson(alloc, schema_json);
    }

    pub fn getSchemaJson(self: *DB, alloc: Allocator) !?[]u8 {
        return try self.inner.getSchemaJson(alloc);
    }

    pub fn exportPortable(self: *DB, alloc: Allocator, out: *std.ArrayList(u8)) !void {
        try support.portable_backup.exportPortable(alloc, self.inner.core.store, out);
        try support.portable_backup.validatePortable(alloc, out.items);
    }

    pub fn importPortable(self: *DB, alloc: Allocator, backup: []const u8) !void {
        if (self.owned_lite_backend != null) {
            try lite_restore_staging.importPortableIntoLiteDb(alloc, &self.inner, backup);
            return;
        }
        try support.portable_backup.validatePortable(alloc, backup);
        try support.portable_backup.importPortable(alloc, self.inner.core.store, backup);
        const target_identity = self.inner.core.identity_namespace;
        try self.inner.reassignIdentityNamespaceForInternalTransition(target_identity);
    }

    pub fn checkLite(self: *DB) !LiteCheckReport {
        if (self.owned_lite_backend) |*lite_backend| return try lite_backend.check();
        return error.NotLiteDatabase;
    }

    pub fn copyStableLiteSnapshot(self: *DB, dest_path: []const u8, replace: bool) !LiteStableSnapshotReport {
        if (self.owned_lite_backend) |*lite_backend| return try lite_backend.copyStableSnapshot(dest_path, replace);
        return error.NotLiteDatabase;
    }

    pub fn vacuumLite(self: *DB) !LiteVacuumReport {
        if (self.owned_lite_backend) |*lite_backend| return try lite_backend.vacuum();
        return error.NotLiteDatabase;
    }
};

pub fn capabilitiesForProfile(profile: Profile) Capabilities {
    return support.lite.backend.capabilitiesForProfile(profile);
}

pub fn checkLiteFile(alloc: Allocator, path: []const u8) !LiteCheckReport {
    return try support.lite.backend.checkFile(alloc, path);
}

pub fn copyStableLiteSnapshotFile(alloc: Allocator, source_path: []const u8, dest_path: []const u8, replace: bool) !LiteStableSnapshotReport {
    return try support.lite.backend.copyStableSnapshotFile(alloc, source_path, dest_path, replace);
}

pub fn setRemoteTemplateRenderer(renderer: ?RemoteTemplateRenderer) void {
    template_remote_host.setHostRenderer(renderer);
}

pub fn renderRemoteTemplateText(
    alloc: Allocator,
    template_source: []const u8,
    json_doc: []const u8,
) ![]const u8 {
    return try template_remote_host.renderJsonToText(alloc, template_source, json_doc);
}

fn toDbOpenOptions(opts: OpenOptions, profile: Profile) db_mod.OpenOptions {
    const external_storage = opts.storage != null;
    var resolved: db_mod.OpenOptions = .{
        .open_mode = opts.open_mode,
        .map_size = opts.map_size,
        .no_sync = opts.no_sync,
        .primary_backend = opts.primary_backend,
        .storage = opts.storage,
        .index_backends = opts.index_backends,
        .ttl_cleanup = opts.ttl_cleanup,
        .physical_root_mode = if (external_storage) .external_backend else .filesystem_managed,
        .external_derived_checkpoints = !external_storage,
        .index_repair_checkpoint_storage = opts.storage,
    };
    if (profile == .hosted) {
        resolved.executor = .{ .backend = .manual };
        resolved.enrichment = opts.enrichment;
        resolved.ttl_cleanup = .{ .enabled = false };
        resolved.transaction_recovery = .{ .enabled = false };
        resolved.text_merge = .{ .enabled = false };
        resolved.sparse_compaction = .{ .enabled = false };
    }
    return resolved;
}

fn openModeRequiresReadOnlyBackends(open_mode: db_mod.OpenOptions.OpenMode) bool {
    return open_mode == .query_readonly or open_mode == .status_only;
}

fn openModeCanWrite(open_mode: db_mod.OpenOptions.OpenMode) bool {
    return switch (open_mode) {
        .writer, .writer_no_replay => true,
        else => false,
    };
}

fn testLitePath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "embedded db openLite persists documents in aflite file" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testLitePath(alloc, tmp, "embedded-open-lite.aflite");
    defer alloc.free(path);

    const opts = OpenOptions{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    };

    try std.testing.expectError(error.FileNotFound, DB.openLite(alloc, path, opts));

    {
        var db = try DB.createLite(alloc, path, opts);
        defer db.close();

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:lite",
                .value = "{\"title\":\"lite persisted\"}",
            }},
            .sync_level = .write,
        });
    }

    {
        var reopened = try DB.openLite(alloc, path, opts);
        defer reopened.close();

        var result = (try reopened.lookup(alloc, "doc:lite", .{})) orelse return error.MissingLiteDocument;
        defer result.deinit(alloc);

        try std.testing.expect(std.mem.indexOf(u8, result.json, "\"lite persisted\"") != null);
    }
}

test "embedded db openLite close syncs unsynced batch before readonly reopen" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testLitePath(alloc, tmp, "embedded-close-sync.aflite");
    defer alloc.free(path);

    {
        var db = try DB.createLite(alloc, path, .{});
        defer db.close();

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:embedded-close-sync",
                .value = "{\"title\":\"embedded close persists\"}",
            }},
            .sync_level = .propose,
        });
    }

    {
        var reopened = try DB.openLite(alloc, path, .{
            .open_mode = .query_readonly,
        });
        defer reopened.close();

        var result = (try reopened.lookup(alloc, "doc:embedded-close-sync", .{})) orelse return error.MissingLiteDocument;
        defer result.deinit(alloc);

        try std.testing.expect(std.mem.indexOf(u8, result.json, "\"embedded close persists\"") != null);
    }
}

test "embedded db openLite propagates no_sync to aflite backend" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testLitePath(alloc, tmp, "embedded-open-lite-no-sync.aflite");
    defer alloc.free(path);

    var db = try DB.createLite(alloc, path, .{
        .no_sync = true,
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try std.testing.expect(db.owned_lite_backend.?.native_docstore.?.file.no_sync);
}

test "embedded db openLite does not fall back to internal bridge files" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testLitePath(alloc, tmp, "embedded-open-lite-bridge.aflite");
    defer alloc.free(path);

    {
        var bridge = try support.lite.backend.Handle.open(alloc, path, .{ .engine = .bridge_lsm_container });
        defer bridge.deinit();
        try std.testing.expectEqual(support.lite.backend.EngineKind.bridge_lsm_container, bridge.engine);
    }

    {
        var bridge_readonly = try support.lite.backend.Handle.open(alloc, path, .{
            .engine = .bridge_lsm_container,
            .read_only = true,
        });
        defer bridge_readonly.deinit();
        try std.testing.expectEqual(support.lite.backend.EngineKind.bridge_lsm_container, bridge_readonly.engine);
    }

    const report = try checkLiteFile(alloc, path);
    try std.testing.expect(!report.valid);
    try std.testing.expect(report.issue != null);

    var opened = DB.openLite(alloc, path, .{}) catch |err| {
        switch (err) {
            error.TruncatedNativeHeader,
            error.InvalidNativeMagic,
            error.UnsupportedNativeFormatVersion,
            error.InvalidNativeHeaderSize,
            error.NativeHeaderChecksumMismatch,
            => return,
            else => return err,
        }
    };
    opened.close();
    return error.ExpectedNativeLiteOpenFailure;
}

test "embedded db liteStatus exposes storage stats work and capabilities" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const plain_path = try testLitePath(alloc, tmp, "embedded-status-plain");
    defer alloc.free(plain_path);

    {
        var plain = try DB.open(alloc, plain_path, .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        });
        defer plain.close();
        try std.testing.expectError(error.NotLiteDatabase, plain.liteStatus(alloc));
    }

    const path = try testLitePath(alloc, tmp, "embedded-status.aflite");
    defer alloc.free(path);

    var db = try DB.createLite(alloc, path, .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:status",
            .value = "{\"title\":\"typed status\"}",
        }},
        .sync_level = .write,
    });

    var status = try db.liteStatus(alloc);
    defer status.deinit(alloc);

    try std.testing.expectEqualStrings("aflite", status.storage.format);
    try std.testing.expectEqualStrings("native_single_file", status.storage.engine);
    try std.testing.expectEqualStrings("native_replay_lanes_in_document_catalog", status.storage.replay_layout);
    try std.testing.expectEqualStrings("__antfly_lite", status.storage.index_namespace.?);
    try std.testing.expectEqual(@as(?u32, 1), status.storage.format_version);
    try std.testing.expectEqual(@as(?u32, 4096), status.storage.page_size);
    try std.testing.expect(status.storage.active_checkpoint != null);
    try std.testing.expectEqual(@as(u64, 1), status.stats.doc_count);
    try std.testing.expect(!status.pending_work.has_async_indexes);
    try std.testing.expect(status.capabilities.text_search);
    try std.testing.expect(status.capabilities.dense_vector_search);
    try std.testing.expect(status.capabilities.sparse_vector_search);
    try std.testing.expect(status.capabilities.hybrid_search);
    try std.testing.expect(status.capabilities.graph_search);
    try std.testing.expect(status.capabilities.caller_supplied_embeddings);
    try std.testing.expect(status.capabilities.ttl_cleanup_runtime);
    try std.testing.expect(status.capabilities.no_inference_configured_ok);
    try std.testing.expect(!status.capabilities.raft_replication);
    try std.testing.expect(!status.capabilities.cluster_placement);
}

test "embedded db liteStatus reflects explicitly configured remote inference" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testLitePath(alloc, tmp, "embedded-status-remote-inference.aflite");
    defer alloc.free(path);

    var db = try DB.createLite(alloc, path, .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .inference = .{ .remote_provider_configured = true },
    });
    defer db.close();

    var status = try db.liteStatus(alloc);
    defer status.deinit(alloc);

    try std.testing.expectEqualStrings("remote_provider", status.inference.mode);
    try std.testing.expect(status.inference.configured);
    try std.testing.expect(status.inference.remote_provider_configured);
    try std.testing.expect(!status.inference.local_runtime_configured);
    const native_local_runtime_available = support.lite.backend.capabilitiesForProfile(.native).local_inference_runtime;
    try std.testing.expectEqual(native_local_runtime_available, status.inference.local_runtime_available);
    try std.testing.expect(status.inference.caller_supplied_artifacts);
    try std.testing.expect(status.inference.no_inference_configured_ok);
    try std.testing.expectEqualStrings("remote_provider", status.capabilities.inference_mode);
    try std.testing.expectEqual(native_local_runtime_available, status.capabilities.local_inference_runtime);

    const caps = db.capabilities();
    try std.testing.expectEqualStrings("remote_provider", caps.inference_mode);
    try std.testing.expectEqual(native_local_runtime_available, caps.local_inference_runtime);
}

test "embedded db liteStatus reports local inference request according to build support" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testLitePath(alloc, tmp, "embedded-status-local-inference.aflite");
    defer alloc.free(path);

    var db = try DB.createLite(alloc, path, .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .inference = .{ .local_runtime_configured = true },
    });
    defer db.close();

    var status = try db.liteStatus(alloc);
    defer status.deinit(alloc);

    const native_local_runtime_available = support.lite.backend.capabilitiesForProfile(.native).local_inference_runtime;
    if (native_local_runtime_available) {
        try std.testing.expectEqualStrings("local_embedded", status.inference.mode);
        try std.testing.expect(status.inference.configured);
    } else {
        try std.testing.expectEqualStrings("caller_supplied_or_disabled", status.inference.mode);
        try std.testing.expect(!status.inference.configured);
    }
    try std.testing.expect(!status.inference.remote_provider_configured);
    try std.testing.expect(status.inference.local_runtime_configured);
    try std.testing.expectEqual(native_local_runtime_available, status.inference.local_runtime_available);
    try std.testing.expect(status.inference.caller_supplied_artifacts);
    try std.testing.expect(status.inference.no_inference_configured_ok);
    if (native_local_runtime_available) {
        try std.testing.expectEqualStrings("local_embedded", status.capabilities.inference_mode);
        try std.testing.expect(status.capabilities.local_inference_runtime);
    } else {
        try std.testing.expectEqualStrings("caller_supplied_or_disabled", status.capabilities.inference_mode);
        try std.testing.expect(!status.capabilities.local_inference_runtime);
    }
    var found_local = false;
    for (status.capabilities.available_inference_modes) |mode| {
        if (std.mem.eql(u8, mode, "local_embedded")) found_local = true;
    }
    try std.testing.expectEqual(native_local_runtime_available, found_local);

    const caps = db.capabilities();
    if (native_local_runtime_available) {
        try std.testing.expectEqualStrings("local_embedded", caps.inference_mode);
        try std.testing.expect(caps.local_inference_runtime);
    } else {
        try std.testing.expectEqualStrings("caller_supplied_or_disabled", caps.inference_mode);
        try std.testing.expect(!caps.local_inference_runtime);
    }
}

test "embedded db openLite can run ttl cleanup over aflite file" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testLitePath(alloc, tmp, "embedded-open-lite-ttl-cleanup.aflite");
    defer alloc.free(path);

    var db = try DB.createLite(alloc, path, .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .ttl_cleanup = .{
            .enabled = true,
            .interval_ms = 10,
            .batch_size = 8,
            .grace_period_ns = 0,
        },
    });
    defer db.close();

    try db.setSchema(.{
        .version = 1,
        .default_type = "_default",
        .ttl_duration_ns = 1_000_000_000,
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:expired",
            .value = "{\"title\":\"gone\"}",
        }},
        .timestamp_ns = 1,
        .sync_level = .write,
    });

    var stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var attempts: usize = 0;
    while ((stats.ttl_cleanup.deleted_docs == 0 or stats.ttl_cleanup.scanned_timestamps == 0) and attempts < 200) : (attempts += 1) {
        std.Thread.sleep(10 * std.time.ns_per_ms);
        types.freeDBStats(alloc, stats);
        stats = try db.stats(alloc);
    }

    try std.testing.expect(stats.ttl_cleanup.enabled);
    try std.testing.expect(stats.ttl_cleanup.runs > 0);
    try std.testing.expect(stats.ttl_cleanup.scanned_timestamps > 0);
    try std.testing.expect(stats.ttl_cleanup.deleted_docs > 0);
    try std.testing.expect(stats.ttl_cleanup.last_run_ns > 0);
}

test "embedded db openLiteHosted exposes manual maintenance capabilities" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testLitePath(alloc, tmp, "embedded-open-lite-hosted.aflite");
    defer alloc.free(path);

    var db = try DB.createLiteHosted(alloc, path, .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    const caps = db.capabilities();
    try std.testing.expect(caps.hosted_profile);
    try std.testing.expect(caps.manual_maintenance);
    try std.testing.expect(!caps.background_enrichment_runtime);
    try std.testing.expect(!caps.ttl_cleanup_runtime);
    try std.testing.expect(!caps.transaction_recovery_runtime);
}

test "embedded db openLite query_readonly rejects writes" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testLitePath(alloc, tmp, "embedded-open-lite-readonly.aflite");
    defer alloc.free(path);

    {
        var db = try DB.createLite(alloc, path, .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:readonly",
                .value = "{\"title\":\"readonly visible\"}",
            }},
            .sync_level = .write,
        });
    }

    var readonly = try DB.openLite(alloc, path, .{
        .open_mode = .query_readonly,
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer readonly.close();

    var result = (try readonly.lookup(alloc, "doc:readonly", .{})) orelse return error.MissingLiteDocument;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"readonly visible\"") != null);

    try std.testing.expectError(error.ReadOnly, readonly.batch(.{
        .writes = &.{.{
            .key = "doc:denied",
            .value = "{}",
        }},
        .sync_level = .write,
    }));
}

test "embedded db openLite persists schema json in aflite file" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testLitePath(alloc, tmp, "embedded-open-lite-schema.aflite");
    defer alloc.free(path);

    const schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;

    {
        var db = try DB.createLite(alloc, path, .{});
        defer db.close();

        try db.setSchemaJson(alloc, schema_json);
        _ = try db.vacuumLite();
    }

    {
        var reopened = try DB.openLite(alloc, path, .{
            .open_mode = .status_only,
        });
        defer reopened.close();

        const loaded = (try reopened.getSchemaJson(alloc)) orelse return error.MissingLiteSchemaJson;
        defer alloc.free(loaded);
        try std.testing.expectEqualStrings(schema_json, loaded);
    }
}

test "embedded db openLite persists index and enrichment catalogs in aflite file" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testLitePath(alloc, tmp, "embedded-open-lite-catalogs.aflite");
    defer alloc.free(path);

    {
        var db = try DB.createLite(alloc, path, .{});
        defer db.close();

        try db.addEnrichment(.{
            .name = "body_chunks_v1",
            .kind = .chunk,
            .field = "body",
            .chunk_size = 128,
            .chunk_overlap = 16,
        });
        try db.addIndex(.{
            .name = "ft_body",
            .kind = .full_text,
            .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
        });
        _ = try db.vacuumLite();
    }

    {
        var reopened = try DB.openLite(alloc, path, .{
            .open_mode = .status_only,
        });
        defer reopened.close();

        const indexes = try reopened.listIndexes(alloc);
        defer types.freeIndexConfigs(alloc, indexes);
        try std.testing.expectEqual(@as(usize, 1), indexes.len);
        try std.testing.expectEqualStrings("ft_body", indexes[0].name);
        try std.testing.expectEqual(types.IndexKind.full_text, indexes[0].kind);
        try std.testing.expectEqualStrings("{\"chunk_name\":\"body_chunks_v1\"}", indexes[0].config_json);

        const enrichments = try reopened.listEnrichments(alloc);
        defer types.freeEnrichmentConfigs(alloc, enrichments);
        try std.testing.expectEqual(@as(usize, 1), enrichments.len);
        try std.testing.expectEqualStrings("body_chunks_v1", enrichments[0].name);
        try std.testing.expectEqual(types.EnrichmentKind.chunk, enrichments[0].kind);
        try std.testing.expectEqualStrings("body", enrichments[0].field);
        try std.testing.expectEqual(@as(u32, 128), enrichments[0].chunk_size);
        try std.testing.expectEqual(@as(u32, 16), enrichments[0].chunk_overlap);
    }
}
