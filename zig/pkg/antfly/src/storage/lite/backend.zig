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
const reflection = @import("../../common/reflection_compat.zig");
const builtin = @import("builtin");
const platform_sync = @import("antfly_platform").sync;
const db_mod = @import("../db/db.zig");
const db_core = @import("../db/core.zig");
const db_types = @import("../db/types.zig");
const backend_erased = @import("../backend_erased.zig");
const background_runtime = @import("../background_runtime.zig");
const maintenance = @import("../maintenance.zig");
const bridge = @import("bridge.zig");
const capabilities = @import("capabilities.zig");
const docstore = @import("docstore.zig");
const index_storage = @import("index_storage.zig");
const resource_manager_mod = @import("../resource_manager.zig");

const Allocator = std.mem.Allocator;
const native_index_base_path = "__antfly_lite";
const native_index_layout = "native_index_catalog_pages";
const root_namespace_alias_catalog_key = "system/lite-root-namespace-alias";
const artifact_profile_catalog_key = "system/lite-artifact-profile";
const embedded_artifact_profile = "embedded-v1";
const standalone_artifact_profile = "standalone-v1";

pub const native = @import("native.zig");
pub const CheckReport = native.CheckReport;
pub const StableSnapshotReport = native.StableSnapshotReport;
pub const VacuumReport = native.VacuumReport;

pub const Profile = capabilities.Profile;
pub const supported_inference_modes = capabilities.supported_inference_modes;
pub const Capabilities = capabilities.Capabilities;
pub const InferenceStatus = capabilities.InferenceStatus;
pub const InferenceOpenOptions = capabilities.InferenceOpenOptions;
pub const capabilitiesForProfile = capabilities.capabilitiesForProfile;
pub const capabilitiesForProfileWithInferenceStatus = capabilities.capabilitiesForProfileWithInferenceStatus;
pub const inferenceStatusForProfile = capabilities.inferenceStatusForProfile;
pub const inferenceStatusForProfileWithOptions = capabilities.inferenceStatusForProfileWithOptions;

pub const EngineKind = enum {
    /// Internal LSM bridge for explicit storage-engine development.
    bridge_lsm_container,

    /// Native v1 `.aflite` file engine. It owns real Lite pages and checkpoint
    /// roots and is the public default for newly-created Lite files.
    native_single_file,
};

pub const EngineSelection = enum {
    /// Open and create public Lite files as native v1.
    auto,
    bridge_lsm_container,
    native_single_file,
};

pub const OpenOptions = struct {
    engine: EngineSelection = .auto,
    read_only: bool = false,
    no_sync: bool = false,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
};

pub const CreateOptions = struct {
    exclusive: bool = false,
    no_sync: bool = false,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
};

pub fn isAflitePath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".aflite");
}

pub const StorageStatus = struct {
    format: []const u8 = "aflite",
    engine: []const u8,
    primary_layout: []const u8,
    replay_layout: []const u8,
    index_layout: []const u8,
    index_namespace: ?[]const u8 = null,
    format_version: ?u32 = null,
    page_size: ?u32 = null,
    active_checkpoint: ?u8 = null,
    checkpoint_sequence: ?u64 = null,
    page_count: ?u64 = null,
    fsync: ?bool = null,
};

pub fn Status(comptime Stats: type) type {
    return struct {
        storage: StorageStatus,
        stats: Stats,
        pending_work: db_core.PendingWorkStats,
        inference: InferenceStatus,
        capabilities: Capabilities,

        pub fn deinit(self: *@This(), allocator: Allocator) void {
            if (Stats == db_types.DBStats) {
                db_types.freeDBStats(allocator, self.stats);
            }
            self.* = undefined;
        }
    };
}

pub const FullStatus = Status(db_types.DBStats);

pub fn checkFile(allocator: Allocator, path: []const u8) !CheckReport {
    if (!isAflitePath(path)) return error.InvalidArgument;
    return try native.checkFile(allocator, path);
}

pub fn copyStableSnapshotFile(allocator: Allocator, source_path: []const u8, dest_path: []const u8, replace: bool) !StableSnapshotReport {
    if (!isAflitePath(source_path) or !isAflitePath(dest_path)) return error.InvalidArgument;
    return try native.copyStableSnapshot(allocator, source_path, dest_path, replace);
}

pub const Handle = struct {
    const NamespaceRuntime = struct {
        name: []u8,
        key_prefix: []u8,
        index_base_path: []u8,
        runtime_store: *backend_erased.Store,

        fn deinit(self: *NamespaceRuntime, allocator: Allocator) void {
            self.runtime_store.deinit();
            allocator.destroy(self.runtime_store);
            allocator.free(self.index_base_path);
            allocator.free(self.key_prefix);
            allocator.free(self.name);
            self.* = undefined;
        }
    };

    allocator: Allocator,
    engine: EngineKind,
    bridge_storage: ?*bridge.ContainerStorage = null,
    native_docstore: ?*docstore.Store = null,
    native_index_storage: ?*index_storage.Store = null,
    native_runtime_store: ?*backend_erased.Store = null,
    namespace_mutex: std.atomic.Mutex = .unlocked,
    namespace_runtimes: std.StringHashMapUnmanaged(NamespaceRuntime) = .empty,
    root_namespace_alias: ?[]u8 = null,
    owned_resource_manager: ?*resource_manager_mod.ResourceManager = null,

    pub fn open(allocator: Allocator, path: []const u8, opts: OpenOptions) !Handle {
        if (!isAflitePath(path)) return error.InvalidArgument;

        const engine = switch (opts.engine) {
            .auto => EngineKind.native_single_file,
            .bridge_lsm_container => EngineKind.bridge_lsm_container,
            .native_single_file => EngineKind.native_single_file,
        };

        return switch (engine) {
            .bridge_lsm_container => try openBridgeLsmContainer(allocator, path, opts),
            .native_single_file => try openNativeSingleFile(allocator, path, opts),
        };
    }

    pub fn create(allocator: Allocator, path: []const u8, exclusive: bool) !Handle {
        return try createWithOptions(allocator, path, .{ .exclusive = exclusive });
    }

    pub fn createWithOptions(allocator: Allocator, path: []const u8, opts: CreateOptions) !Handle {
        if (!isAflitePath(path)) return error.InvalidArgument;
        return try createNativeSingleFile(allocator, path, opts);
    }

    /// Opens an existing file or atomically creates it. Exclusive creation and
    /// the retry close the check/create race when two processes start together;
    /// the file's writer lock remains the final single-writer authority.
    pub fn openOrCreate(allocator: Allocator, path: []const u8, opts: OpenOptions) !Handle {
        return open(allocator, path, opts) catch |err| switch (err) {
            error.FileNotFound => createWithOptions(allocator, path, .{
                .exclusive = true,
                .no_sync = opts.no_sync,
                .resource_manager = opts.resource_manager,
            }) catch |create_err| switch (create_err) {
                error.PathAlreadyExists => try open(allocator, path, opts),
                else => return create_err,
            },
            else => return err,
        };
    }

    pub fn deinit(self: *Handle) void {
        switch (self.engine) {
            .bridge_lsm_container => {
                if (self.bridge_storage) |storage| {
                    storage.deinit();
                    self.allocator.destroy(storage);
                    self.bridge_storage = null;
                }
            },
            .native_single_file => {
                var runtimes = self.namespace_runtimes.valueIterator();
                while (runtimes.next()) |runtime| runtime.deinit(self.allocator);
                self.namespace_runtimes.deinit(self.allocator);
                if (self.root_namespace_alias) |alias| self.allocator.free(alias);
                if (self.native_index_storage) |storage| {
                    self.allocator.destroy(storage);
                    self.native_index_storage = null;
                }
                if (self.native_runtime_store) |runtime_store| {
                    runtime_store.deinit();
                    self.allocator.destroy(runtime_store);
                    self.native_runtime_store = null;
                }
                if (self.native_docstore) |store| {
                    store.close();
                    self.allocator.destroy(store);
                    self.native_docstore = null;
                }
                if (self.owned_resource_manager) |manager| {
                    manager.deinit(self.allocator);
                    self.allocator.destroy(manager);
                    self.owned_resource_manager = null;
                }
            },
        }
        self.* = undefined;
    }

    pub fn configureDbOpenOptions(self: *Handle, opts: *db_mod.OpenOptions) !void {
        switch (self.engine) {
            .bridge_lsm_container => {
                const storage = self.bridge_storage.?.storage();
                opts.storage = storage;
                opts.index_backends.text_lsm_storage = storage;
                opts.index_backends.dense_lsm_storage = storage;
                opts.index_backends.sparse_lsm_storage = storage;
                opts.index_backends.graph_lsm_storage = storage;
                opts.index_repair_checkpoint_storage = storage;
                opts.external_derived_checkpoints = false;
                opts.physical_root_mode = .external_backend;
            },
            .native_single_file => {
                if (opts.resource_manager == null) {
                    opts.resource_manager = self.native_docstore.?.resource_manager;
                }
                opts.primary_backend = .{ .mem = .{} };
                opts.primary_runtime_store = self.native_runtime_store.?;
                const storage = self.native_index_storage.?.storage();
                opts.index_backends.text_main_backend = .lsm;
                opts.index_backends.dense_storage_backend = .lsm;
                opts.index_backends.sparse_backend = .lsm;
                opts.index_backends.graph_reverse_backend = .lsm;
                opts.index_backends.text_lsm_storage = storage;
                opts.index_backends.dense_lsm_storage = storage;
                opts.index_backends.sparse_lsm_storage = storage;
                opts.index_backends.graph_lsm_storage = storage;
                opts.index_backends.text_main_lsm_options.storage = storage;
                opts.index_backends.text_wal_lsm_options.storage = storage;
                opts.index_backends.dense_lsm_options.storage = storage;
                opts.index_backends.sparse_lsm_options.storage = storage;
                opts.index_backends.graph_reverse_lsm_options.storage = storage;
                opts.index_repair_checkpoint_storage = storage;
                opts.index_base_path = native_index_base_path;
                opts.index_open_parallelism = 1;
                opts.external_derived_checkpoints = false;
                opts.physical_root_mode = .external_backend;
            },
        }
    }

    /// Configures a DB instance to use an isolated logical namespace inside
    /// this Lite file. Runtime stores and index paths are cached per namespace,
    /// so repeated reader/writer opens allocate nothing after the first open.
    pub fn configureDbOpenOptionsForNamespace(self: *Handle, opts: *db_mod.OpenOptions, namespace: []const u8) !void {
        if (self.engine != .native_single_file or namespace.len == 0 or std.mem.indexOfScalar(u8, namespace, 0) != null) {
            return error.InvalidArgument;
        }
        platform_sync.lockYielding(&self.namespace_mutex);
        defer self.namespace_mutex.unlock();

        const canonical_namespace = try canonicalDbNamespaceAlloc(self.allocator, namespace);
        defer self.allocator.free(canonical_namespace);

        var selected = self.namespace_runtimes.getPtr(canonical_namespace);
        if (selected == null) {
            const name = try self.allocator.dupe(u8, canonical_namespace);
            errdefer self.allocator.free(name);
            const root_alias = if (self.root_namespace_alias) |alias| std.mem.eql(u8, alias, canonical_namespace) else false;
            const key_prefix = if (root_alias)
                try self.allocator.dupe(u8, "")
            else
                try std.mem.concat(self.allocator, u8, &.{ "\x02db/", canonical_namespace, "\x00" });
            errdefer self.allocator.free(key_prefix);
            var namespace_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(canonical_namespace, &namespace_digest, .{});
            const namespace_hex = std.fmt.bytesToHex(namespace_digest, .lower);
            const index_base_path = if (root_alias)
                try self.allocator.dupe(u8, native_index_base_path)
            else
                try std.fmt.allocPrint(self.allocator, "{s}/tables/{s}", .{ native_index_base_path, namespace_hex });
            errdefer self.allocator.free(index_base_path);
            const runtime_store = try self.allocator.create(backend_erased.Store);
            errdefer self.allocator.destroy(runtime_store);
            runtime_store.* = try self.native_docstore.?.runtimeStoreWithPrefix(self.allocator, key_prefix);
            errdefer runtime_store.deinit();
            try self.namespace_runtimes.putNoClobber(self.allocator, name, .{
                .name = name,
                .key_prefix = key_prefix,
                .index_base_path = index_base_path,
                .runtime_store = runtime_store,
            });
            selected = self.namespace_runtimes.getPtr(name).?;
        }

        const runtime = selected.?;
        if (opts.resource_manager == null) opts.resource_manager = self.native_docstore.?.resource_manager;
        opts.primary_backend = .{ .mem = .{} };
        opts.primary_runtime_store = runtime.runtime_store;
        const storage = self.native_index_storage.?.storage();
        opts.index_backends.text_main_backend = .lsm;
        opts.index_backends.dense_storage_backend = .lsm;
        opts.index_backends.sparse_backend = .lsm;
        opts.index_backends.graph_reverse_backend = .lsm;
        opts.index_backends.text_lsm_storage = storage;
        opts.index_backends.dense_lsm_storage = storage;
        opts.index_backends.sparse_lsm_storage = storage;
        opts.index_backends.graph_lsm_storage = storage;
        opts.index_backends.text_main_lsm_options.storage = storage;
        opts.index_backends.text_wal_lsm_options.storage = storage;
        opts.index_backends.dense_lsm_options.storage = storage;
        opts.index_backends.sparse_lsm_options.storage = storage;
        opts.index_backends.graph_reverse_lsm_options.storage = storage;
        opts.index_repair_checkpoint_storage = storage;
        opts.index_base_path = runtime.index_base_path;
        opts.index_open_parallelism = 1;
        opts.external_derived_checkpoints = false;
        opts.physical_root_mode = .external_backend;
    }

    /// Permanently maps an existing embedded root database to a standalone
    /// table namespace. The alias is stored in the `.aflite` catalog, so moving
    /// the file or changing the standalone data directory cannot orphan data.
    pub fn adoptEmbeddedRootAsNamespace(self: *Handle, namespace: []const u8) !void {
        if (self.engine != .native_single_file) return error.InvalidArgument;
        const canonical = try canonicalDbNamespaceAlloc(self.allocator, namespace);
        errdefer self.allocator.free(canonical);
        platform_sync.lockYielding(&self.namespace_mutex);
        defer self.namespace_mutex.unlock();
        if (self.root_namespace_alias) |existing| {
            if (!std.mem.eql(u8, existing, canonical)) return error.LiteRootNamespaceAlreadyAdopted;
            self.allocator.free(canonical);
            return;
        }
        // Changing the namespace mapping after a DB has opened would leave
        // that caller bound to the old prefixed runtime. Fail closed instead
        // of publishing an alias that only future opens observe.
        if (self.namespace_runtimes.get(canonical)) |runtime| {
            if (runtime.key_prefix.len != 0) return error.LiteNamespaceAlreadyOpened;
        }
        try self.native_docstore.?.file.putCatalogRecord(root_namespace_alias_catalog_key, canonical);
        self.root_namespace_alias = canonical;
    }

    pub fn embeddedRootHasUserDocuments(self: *Handle) !bool {
        if (self.engine != .native_single_file) return false;
        const docs = try self.native_docstore.?.file.snapshotDocumentsAlloc(self.allocator);
        defer native.NativeFile.freeSnapshotDocuments(self.allocator, docs);
        for (docs) |doc| {
            if (!std.mem.startsWith(u8, doc.key, "\x02db/")) return true;
        }
        return false;
    }

    pub fn markEmbeddedArtifact(self: *Handle) !void {
        if (self.engine != .native_single_file) return error.InvalidArgument;
        if (try self.isEmbeddedArtifact()) return;
        try self.native_docstore.?.file.putCatalogRecord(artifact_profile_catalog_key, embedded_artifact_profile);
    }

    pub fn isEmbeddedArtifact(self: *const Handle) !bool {
        return try self.artifactHasProfile(embedded_artifact_profile);
    }

    pub fn markStandaloneArtifact(self: *Handle) !void {
        if (self.engine != .native_single_file) return error.InvalidArgument;
        if (try self.isStandaloneArtifact()) return;
        try self.native_docstore.?.file.putCatalogRecord(artifact_profile_catalog_key, standalone_artifact_profile);
    }

    pub fn isStandaloneArtifact(self: *const Handle) !bool {
        return try self.artifactHasProfile(standalone_artifact_profile);
    }

    fn artifactHasProfile(self: *const Handle, expected: []const u8) !bool {
        if (self.engine != .native_single_file) return false;
        const value = (try self.native_docstore.?.file.getCatalogRecordAlloc(self.allocator, artifact_profile_catalog_key)) orelse return false;
        defer self.allocator.free(value);
        return std.mem.eql(u8, value, expected);
    }

    pub fn hasStandaloneRootAdoption(self: *const Handle) bool {
        return self.root_namespace_alias != null;
    }

    /// Installs this file as the storage provider for DBs opened by a composed
    /// server runtime. The logical DB path is a stable namespace within the
    /// file; repeated opens reuse the cached runtime and index storage objects.
    pub fn dbOpenConfigurator(self: *Handle) background_runtime.DbOpenConfigurator {
        return .{ .ptr = self, .configure_fn = configureDbOpenOpaque };
    }

    fn configureDbOpenOpaque(ptr: *anyopaque, path: []const u8, raw_options: *anyopaque) anyerror!void {
        const self: *Handle = @ptrCast(@alignCast(ptr));
        const opts: *db_mod.OpenOptions = @ptrCast(@alignCast(raw_options));
        try self.configureDbOpenOptionsForNamespace(opts, path);
    }

    /// Returns a cached raw store for small system records such as the
    /// standalone metadata catalog. The pointer remains valid until deinit.
    pub fn runtimeStoreForNamespace(self: *Handle, namespace: []const u8) !*backend_erased.Store {
        var opts: db_mod.OpenOptions = .{};
        try self.configureDbOpenOptionsForNamespace(&opts, namespace);
        const canonical_namespace = try canonicalDbNamespaceAlloc(self.allocator, namespace);
        defer self.allocator.free(canonical_namespace);
        platform_sync.lockYielding(&self.namespace_mutex);
        defer self.namespace_mutex.unlock();
        return self.namespace_runtimes.getPtr(canonical_namespace).?.runtime_store;
    }

    pub fn storageStatus(self: *Handle) StorageStatus {
        return switch (self.engine) {
            .native_single_file => blk: {
                const file = &self.native_docstore.?.file;
                const checkpoint = file.activeCheckpoint();
                break :blk .{
                    .engine = @tagName(self.engine),
                    .primary_layout = "native_document_pages",
                    .replay_layout = "native_replay_lanes_in_document_catalog",
                    .index_layout = native_index_layout,
                    .index_namespace = native_index_base_path,
                    .format_version = native.format_version,
                    .page_size = file.header.page_size,
                    .active_checkpoint = file.header.active_checkpoint,
                    .checkpoint_sequence = checkpoint.commit_sequence,
                    .page_count = checkpoint.page_count,
                    .fsync = !file.no_sync,
                };
            },
            .bridge_lsm_container => .{
                .format = "aflite-internal",
                .engine = @tagName(self.engine),
                .primary_layout = "lsm_container",
                .replay_layout = "lsm_container",
                .index_layout = "lsm_container",
            },
        };
    }

    pub fn fullStatus(self: *Handle, allocator: Allocator, db: *db_mod.DB, profile: Profile) !FullStatus {
        const stats_value = try db.stats(allocator);
        errdefer db_types.freeDBStats(allocator, stats_value);

        return .{
            .storage = self.storageStatus(),
            .stats = stats_value,
            .pending_work = db.pendingWorkStats(),
            .inference = inferenceStatusForProfile(profile),
            .capabilities = capabilitiesForProfile(profile),
        };
    }

    pub fn check(self: *Handle) !CheckReport {
        return try self.checkWithCancel(null);
    }

    fn checkWithCancel(self: *Handle, cancel: ?*const maintenance.CancelToken) !CheckReport {
        return switch (self.engine) {
            .bridge_lsm_container => blk: {
                if (cancel) |token| try token.check();
                const report = try self.bridge_storage.?.check();
                if (cancel) |token| try token.check();
                break :blk toCheckReport(report);
            },
            .native_single_file => blk: {
                const store = self.native_docstore.?;
                platform_sync.lockYielding(&store.mutex);
                defer store.mutex.unlock();
                break :blk try store.file.checkWithCancel(cancel);
            },
        };
    }

    pub fn maintenanceSource(self: *Handle) maintenance.Source {
        return .{ .ptr = self, .vtable = &.{ .status = maintenanceStatus, .run = runMaintenance } };
    }

    fn maintenanceStatus(ptr: *anyopaque) maintenance.Status {
        const self: *Handle = @ptrCast(@alignCast(ptr));
        if (self.native_docstore) |store| platform_sync.lockYielding(&store.mutex);
        defer if (self.native_docstore) |store| store.mutex.unlock();
        const status = self.storageStatus();
        return .{
            .engine = "lite",
            .format = status.format,
            .fsync = status.fsync,
            // Native maintenance takes the file's exclusive maintenance gate.
            // It is callable through the asynchronous admin surface, but is
            // deliberately not advertised as availability-preserving.
            .maintenance = .{ .check = true, .compact = true, .vacuum = true, .online = false },
        };
    }

    fn runMaintenance(ptr: *anyopaque, operation: maintenance.Operation, cancel: *const maintenance.CancelToken) anyerror!maintenance.Result {
        const self: *Handle = @ptrCast(@alignCast(ptr));
        try cancel.check();
        return switch (operation) {
            .check => blk: {
                const report = try self.checkWithCancel(cancel);
                break :blk .{
                    .valid = report.valid,
                    .issue = report.issue,
                    .file_size = report.file_size,
                    .valid_prefix_size = report.valid_prefix_size,
                    .reclaimable_bytes = report.reclaimable_bytes,
                    .live_file_count = report.live_file_count,
                    .live_bytes = report.live_bytes,
                };
            },
            .compact => blk: {
                platform_sync.lockYielding(&self.namespace_mutex);
                defer self.namespace_mutex.unlock();
                var runtimes = self.namespace_runtimes.valueIterator();
                while (runtimes.next()) |runtime| {
                    try cancel.check();
                    try runtime.runtime_store.sync(true);
                }
                const report = try self.vacuumWithCancel(cancel);
                break :blk vacuumMaintenanceResult(report);
            },
            .vacuum => vacuumMaintenanceResult(try self.vacuumWithCancel(cancel)),
        };
    }

    fn vacuumMaintenanceResult(report: VacuumReport) maintenance.Result {
        return .{
            .before_size = report.before_size,
            .after_size = report.after_size,
            .reclaimed_bytes = report.reclaimed_bytes,
            .live_file_count = report.live_file_count,
            .live_bytes = report.live_bytes,
        };
    }

    pub fn vacuum(self: *Handle) !VacuumReport {
        return try self.vacuumWithCancel(null);
    }

    fn vacuumWithCancel(self: *Handle, cancel: ?*const maintenance.CancelToken) !VacuumReport {
        return switch (self.engine) {
            .bridge_lsm_container => blk: {
                if (cancel) |token| try token.check();
                const report = try self.bridge_storage.?.vacuum();
                if (cancel) |token| try token.check();
                break :blk toVacuumReport(report);
            },
            .native_single_file => try nativeVacuumReport(self, cancel),
        };
    }

    pub fn copyStableSnapshot(self: *Handle, dest_path: []const u8, replace: bool) !StableSnapshotReport {
        if (!isAflitePath(dest_path)) return error.InvalidArgument;
        return switch (self.engine) {
            .bridge_lsm_container => error.UnsupportedLiteSnapshot,
            .native_single_file => try nativeStableSnapshot(self, dest_path, replace),
        };
    }
};

fn openBridgeLsmContainer(allocator: Allocator, path: []const u8, opts: OpenOptions) !Handle {
    var storage = try allocator.create(bridge.ContainerStorage);
    errdefer allocator.destroy(storage);

    storage.* = try bridge.ContainerStorage.openWithOptions(allocator, path, .{
        .read_only = opts.read_only,
    });
    errdefer storage.deinit();

    return .{
        .allocator = allocator,
        .engine = .bridge_lsm_container,
        .bridge_storage = storage,
    };
}

fn openNativeSingleFile(allocator: Allocator, path: []const u8, opts: OpenOptions) !Handle {
    var owned_resource_manager: ?*resource_manager_mod.ResourceManager = null;
    const resource_manager = opts.resource_manager orelse blk: {
        const manager = try allocator.create(resource_manager_mod.ResourceManager);
        manager.* = resource_manager_mod.ResourceManager.init(.{});
        owned_resource_manager = manager;
        break :blk manager;
    };
    errdefer if (owned_resource_manager) |manager| {
        manager.deinit(allocator);
        allocator.destroy(manager);
    };

    const initial_store = try docstore.Store.openWithOptions(allocator, path, .{
        .read_only = opts.read_only,
        .no_sync = opts.no_sync,
        .resource_manager = resource_manager,
    });
    return try initNativeSingleFile(allocator, initial_store, owned_resource_manager);
}

fn createNativeSingleFile(allocator: Allocator, path: []const u8, opts: CreateOptions) !Handle {
    var owned_resource_manager: ?*resource_manager_mod.ResourceManager = null;
    const resource_manager = opts.resource_manager orelse blk: {
        const manager = try allocator.create(resource_manager_mod.ResourceManager);
        manager.* = resource_manager_mod.ResourceManager.init(.{});
        owned_resource_manager = manager;
        break :blk manager;
    };
    errdefer if (owned_resource_manager) |manager| {
        manager.deinit(allocator);
        allocator.destroy(manager);
    };

    const initial_store = try docstore.Store.createWithOptions(allocator, path, .{
        .exclusive = opts.exclusive,
        .no_sync = opts.no_sync,
        .resource_manager = resource_manager,
    });
    return try initNativeSingleFile(allocator, initial_store, owned_resource_manager);
}

fn initNativeSingleFile(
    allocator: Allocator,
    initial_store: docstore.Store,
    owned_resource_manager: ?*resource_manager_mod.ResourceManager,
) !Handle {
    var store_owner = initial_store;
    var close_store_owner = true;
    errdefer if (close_store_owner) store_owner.close();

    const store = try allocator.create(docstore.Store);
    errdefer allocator.destroy(store);

    store.* = store_owner;
    close_store_owner = false;
    errdefer store.close();

    const native_index_storage = try allocator.create(index_storage.Store);
    errdefer allocator.destroy(native_index_storage);
    native_index_storage.* = index_storage.Store.initWithNamespace(allocator, store, native_index_base_path);

    const runtime_store = try allocator.create(backend_erased.Store);
    errdefer allocator.destroy(runtime_store);

    runtime_store.* = try store.runtimeStore(allocator);
    errdefer runtime_store.deinit();

    const artifact_profile = try store.file.getCatalogRecordAlloc(allocator, artifact_profile_catalog_key);
    defer if (artifact_profile) |profile| allocator.free(profile);
    if (artifact_profile) |profile| {
        if (!std.mem.eql(u8, profile, embedded_artifact_profile) and
            !std.mem.eql(u8, profile, standalone_artifact_profile))
        {
            return error.InvalidLiteArtifactProfile;
        }
    }
    const root_namespace_alias = try store.file.getCatalogRecordAlloc(allocator, root_namespace_alias_catalog_key);
    errdefer if (root_namespace_alias) |alias| allocator.free(alias);
    if (root_namespace_alias) |alias| {
        if (!isStableGroupNamespace(alias)) return error.InvalidLiteRootNamespaceAlias;
    }
    return .{
        .allocator = allocator,
        .engine = .native_single_file,
        .native_docstore = store,
        .native_index_storage = native_index_storage,
        .native_runtime_store = runtime_store,
        .root_namespace_alias = root_namespace_alias,
        .owned_resource_manager = owned_resource_manager,
    };
}

fn canonicalDbNamespaceAlloc(allocator: Allocator, namespace: []const u8) ![]u8 {
    const table_suffix = "/table-db";
    if (std.mem.endsWith(u8, namespace, table_suffix)) {
        const before_table = namespace[0 .. namespace.len - table_suffix.len];
        const component_start = if (std.mem.lastIndexOfScalar(u8, before_table, '/')) |index| index + 1 else 0;
        const group_component = before_table[component_start..];
        if (std.mem.startsWith(u8, group_component, "group-") and group_component.len > "group-".len) {
            _ = std.fmt.parseUnsigned(u64, group_component["group-".len..], 10) catch return error.InvalidArgument;
            return try std.fmt.allocPrint(allocator, "{s}{s}", .{ group_component, table_suffix });
        }
    }
    return try allocator.dupe(u8, namespace);
}

fn isStableGroupNamespace(namespace: []const u8) bool {
    const table_suffix = "/table-db";
    if (!std.mem.endsWith(u8, namespace, table_suffix)) return false;
    const group_component = namespace[0 .. namespace.len - table_suffix.len];
    if (std.mem.indexOfScalar(u8, group_component, '/') != null or
        !std.mem.startsWith(u8, group_component, "group-") or
        group_component.len == "group-".len)
    {
        return false;
    }
    _ = std.fmt.parseUnsigned(u64, group_component["group-".len..], 10) catch return false;
    return true;
}

fn toCheckReport(report: bridge.ContainerStorage.CheckReport) CheckReport {
    return .{
        .valid = report.valid,
        .file_size = report.file_size,
        .valid_prefix_size = report.valid_prefix_size,
        .tail_bytes = report.tail_bytes,
        .record_count = report.record_count,
        .live_file_count = report.live_file_count,
        .live_bytes = report.live_bytes,
        .compact_size = report.compact_size,
        .reclaimable_bytes = report.reclaimable_bytes,
        .issue = report.issue,
    };
}

fn toVacuumReport(report: bridge.ContainerStorage.VacuumReport) VacuumReport {
    return .{
        .before_size = report.before_size,
        .after_size = report.after_size,
        .reclaimed_bytes = report.reclaimed_bytes,
        .live_file_count = report.live_file_count,
        .live_bytes = report.live_bytes,
    };
}

fn nativeVacuumReport(handle: *Handle, cancel: ?*const maintenance.CancelToken) !VacuumReport {
    const report = try handle.native_docstore.?.vacuumWithCancel(cancel);
    return .{
        .before_size = report.before_size,
        .after_size = report.after_size,
        .reclaimed_bytes = report.reclaimed_bytes,
        .live_file_count = report.live_file_count,
        .live_bytes = report.live_bytes,
    };
}

fn nativeStableSnapshot(handle: *Handle, dest_path: []const u8, replace: bool) !StableSnapshotReport {
    const store = handle.native_docstore.?;
    platform_sync.lockYielding(&store.mutex);
    defer store.mutex.unlock();
    return try store.file.copyStableSnapshotToPath(dest_path, replace);
}

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

fn hasMode(modes: []const []const u8, expected: []const u8) bool {
    for (modes) |mode| {
        if (std.mem.eql(u8, mode, expected)) return true;
    }
    return false;
}

test "lite backend capabilities distinguish native and hosted profiles" {
    const native_caps = capabilitiesForProfile(.native);
    const local_runtime_available = native_caps.local_inference_runtime;
    try std.testing.expect(!native_caps.hosted_profile);
    try std.testing.expect(!native_caps.manual_maintenance);
    try std.testing.expect(native_caps.generated_enrichment_planning);
    try std.testing.expect(native_caps.dense_vector_search);
    try std.testing.expect(native_caps.sparse_vector_search);
    try std.testing.expectEqualStrings("caller_supplied_or_disabled", native_caps.inference_mode);
    try std.testing.expect(!native_caps.inference_required);
    try std.testing.expect(native_caps.no_inference_configured_ok);
    try std.testing.expect(native_caps.caller_supplied_artifacts);
    try std.testing.expect(native_caps.caller_supplied_embeddings);
    try std.testing.expectEqual(local_runtime_available, hasMode(native_caps.available_inference_modes, "local_embedded"));
    try std.testing.expect(native_caps.text_search);
    try std.testing.expect(native_caps.hybrid_search);
    try std.testing.expect(native_caps.graph_search);
    try std.testing.expect(!native_caps.distributed_shard_ownership);
    try std.testing.expect(!native_caps.raft_replication);
    try std.testing.expect(!native_caps.cluster_placement);
    try std.testing.expect(!native_caps.cross_node_joins);
    try std.testing.expect(!native_caps.remote_shard_fanout);
    try std.testing.expect(!native_caps.distributed_transaction_coordination);
    try std.testing.expect(!native_caps.cluster_heartbeat_status_aggregation);
    try std.testing.expect(!native_caps.server_side_autoscaling);
    try std.testing.expect(!native_caps.kubernetes_operator);
    try std.testing.expect(!native_caps.object_storage_primary);

    const hosted_caps = capabilitiesForProfile(.hosted);
    try std.testing.expect(hosted_caps.hosted_profile);
    try std.testing.expect(hosted_caps.manual_maintenance);
    try std.testing.expect(!hosted_caps.background_enrichment_runtime);
    try std.testing.expect(!hosted_caps.ttl_cleanup_runtime);
    try std.testing.expect(!hosted_caps.transaction_recovery_runtime);

    const local_status = inferenceStatusForProfileWithOptions(.native, .{ .local_runtime_configured = true });
    const local_caps = capabilitiesForProfileWithInferenceStatus(.native, local_status);
    if (local_runtime_available) {
        try std.testing.expectEqualStrings("local_embedded", local_caps.inference_mode);
        try std.testing.expect(local_caps.local_inference_runtime);
        try std.testing.expect(hasMode(local_caps.available_inference_modes, "local_embedded"));
    } else {
        try std.testing.expectEqualStrings("caller_supplied_or_disabled", local_caps.inference_mode);
        try std.testing.expect(!local_caps.local_inference_runtime);
        try std.testing.expect(!hasMode(local_caps.available_inference_modes, "local_embedded"));
    }
}

test "lite backend capabilities contract is stable" {
    const expected_fields = [_][]const u8{
        "freestanding_build",
        "hosted_profile",
        "manual_maintenance",
        "background_enrichment_runtime",
        "ttl_cleanup_runtime",
        "transaction_recovery_runtime",
        "local_template_rendering",
        "remote_template_rendering",
        "remote_template_host_callbacks",
        "inference_mode",
        "supported_inference_modes",
        "available_inference_modes",
        "inference_required",
        "no_inference_configured_ok",
        "caller_supplied_artifacts",
        "caller_supplied_embeddings",
        "remote_inference_providers",
        "local_inference_runtime",
        "generated_enrichment_planning",
        "text_search",
        "dense_vector_search",
        "sparse_vector_search",
        "hybrid_search",
        "graph_search",
        "distributed_shard_ownership",
        "raft_replication",
        "cluster_placement",
        "cross_node_joins",
        "remote_shard_fanout",
        "distributed_transaction_coordination",
        "cluster_heartbeat_status_aggregation",
        "server_side_autoscaling",
        "kubernetes_operator",
        "object_storage_primary",
    };

    const fields = reflection.fields(Capabilities);
    try std.testing.expectEqual(expected_fields.len, fields.len);
    inline for (fields, 0..) |field, i| {
        try std.testing.expectEqualStrings(expected_fields[i], field.name);
    }

    const allocator = std.testing.allocator;
    const freestanding = builtin.os.tag == .freestanding;
    const native_json = try std.json.Stringify.valueAlloc(allocator, capabilitiesForProfile(.native), .{});
    defer allocator.free(native_json);
    const native_available_modes =
        if (freestanding)
            "[\"caller_supplied_artifacts\",\"disabled_deferred\"]"
        else if (capabilitiesForProfile(.native).local_inference_runtime)
            "[\"caller_supplied_artifacts\",\"remote_provider\",\"local_embedded\",\"disabled_deferred\"]"
        else
            "[\"caller_supplied_artifacts\",\"remote_provider\",\"disabled_deferred\"]";
    const supported_modes_json = "[\"caller_supplied_artifacts\",\"remote_provider\",\"local_embedded\",\"manual_maintenance\",\"disabled_deferred\"]";
    const native_local_runtime_available = capabilitiesForProfile(.native).local_inference_runtime;
    const expected_native = try std.fmt.allocPrint(allocator, "{{\"freestanding_build\":{},\"hosted_profile\":false,\"manual_maintenance\":false,\"background_enrichment_runtime\":{},\"ttl_cleanup_runtime\":{},\"transaction_recovery_runtime\":{},\"local_template_rendering\":true,\"remote_template_rendering\":{},\"remote_template_host_callbacks\":{},\"inference_mode\":\"caller_supplied_or_disabled\",\"supported_inference_modes\":{s},\"available_inference_modes\":{s},\"inference_required\":false,\"no_inference_configured_ok\":true,\"caller_supplied_artifacts\":true,\"caller_supplied_embeddings\":true,\"remote_inference_providers\":{},\"local_inference_runtime\":{},\"generated_enrichment_planning\":true,\"text_search\":true,\"dense_vector_search\":true,\"sparse_vector_search\":true,\"hybrid_search\":true,\"graph_search\":true,\"distributed_shard_ownership\":false,\"raft_replication\":false,\"cluster_placement\":false,\"cross_node_joins\":false,\"remote_shard_fanout\":false,\"distributed_transaction_coordination\":false,\"cluster_heartbeat_status_aggregation\":false,\"server_side_autoscaling\":false,\"kubernetes_operator\":false,\"object_storage_primary\":false}}", .{
        freestanding,
        !freestanding,
        !freestanding,
        !freestanding,
        !freestanding,
        freestanding,
        supported_modes_json,
        native_available_modes,
        !freestanding,
        native_local_runtime_available,
    });
    defer allocator.free(expected_native);
    try std.testing.expectEqualStrings(expected_native, native_json);

    const hosted_json = try std.json.Stringify.valueAlloc(allocator, capabilitiesForProfile(.hosted), .{});
    defer allocator.free(hosted_json);
    const hosted_available_modes =
        if (freestanding)
            "[\"caller_supplied_artifacts\",\"manual_maintenance\",\"disabled_deferred\"]"
        else if (capabilitiesForProfile(.hosted).local_inference_runtime)
            "[\"caller_supplied_artifacts\",\"remote_provider\",\"local_embedded\",\"manual_maintenance\",\"disabled_deferred\"]"
        else
            "[\"caller_supplied_artifacts\",\"remote_provider\",\"manual_maintenance\",\"disabled_deferred\"]";
    const hosted_local_runtime_available = capabilitiesForProfile(.hosted).local_inference_runtime;
    const expected_hosted = try std.fmt.allocPrint(allocator, "{{\"freestanding_build\":{},\"hosted_profile\":true,\"manual_maintenance\":true,\"background_enrichment_runtime\":false,\"ttl_cleanup_runtime\":false,\"transaction_recovery_runtime\":false,\"local_template_rendering\":true,\"remote_template_rendering\":{},\"remote_template_host_callbacks\":{},\"inference_mode\":\"caller_supplied_or_disabled\",\"supported_inference_modes\":{s},\"available_inference_modes\":{s},\"inference_required\":false,\"no_inference_configured_ok\":true,\"caller_supplied_artifacts\":true,\"caller_supplied_embeddings\":true,\"remote_inference_providers\":{},\"local_inference_runtime\":{},\"generated_enrichment_planning\":true,\"text_search\":true,\"dense_vector_search\":true,\"sparse_vector_search\":true,\"hybrid_search\":true,\"graph_search\":true,\"distributed_shard_ownership\":false,\"raft_replication\":false,\"cluster_placement\":false,\"cross_node_joins\":false,\"remote_shard_fanout\":false,\"distributed_transaction_coordination\":false,\"cluster_heartbeat_status_aggregation\":false,\"server_side_autoscaling\":false,\"kubernetes_operator\":false,\"object_storage_primary\":false}}", .{
        freestanding,
        !freestanding,
        freestanding,
        supported_modes_json,
        hosted_available_modes,
        !freestanding,
        hosted_local_runtime_available,
    });
    defer allocator.free(expected_hosted);
    try std.testing.expectEqualStrings(expected_hosted, hosted_json);
}

test "lite backend inference status reports disabled as clean state" {
    const expected_fields = [_][]const u8{
        "mode",
        "available_modes",
        "configured",
        "remote_provider_configured",
        "local_runtime_configured",
        "local_runtime_available",
        "caller_supplied_artifacts",
        "no_inference_configured_ok",
    };

    const fields = reflection.fields(InferenceStatus);
    try std.testing.expectEqual(expected_fields.len, fields.len);
    inline for (fields, 0..) |field, i| {
        try std.testing.expectEqualStrings(expected_fields[i], field.name);
    }

    const status = inferenceStatusForProfile(.native);
    try std.testing.expectEqualStrings("caller_supplied_or_disabled", status.mode);
    try std.testing.expect(std.mem.eql(u8, status.available_modes[0], "caller_supplied_artifacts"));
    try std.testing.expect(std.mem.eql(u8, status.available_modes[status.available_modes.len - 1], "disabled_deferred"));
    try std.testing.expect(!status.configured);
    try std.testing.expect(!status.remote_provider_configured);
    try std.testing.expect(!status.local_runtime_configured);
    try std.testing.expectEqual(capabilitiesForProfile(.native).local_inference_runtime, status.local_runtime_available);
    try std.testing.expect(status.caller_supplied_artifacts);
    try std.testing.expect(status.no_inference_configured_ok);

    const hosted = inferenceStatusForProfile(.hosted);
    try std.testing.expectEqualStrings("caller_supplied_or_disabled", hosted.mode);
    var hosted_has_manual = false;
    for (hosted.available_modes) |mode| {
        if (std.mem.eql(u8, mode, "manual_maintenance")) hosted_has_manual = true;
    }
    try std.testing.expect(hosted_has_manual);
    try std.testing.expect(!hosted.configured);
    try std.testing.expect(hosted.no_inference_configured_ok);
}

test "lite backend inference status reflects explicit remote provider configuration" {
    const remote = inferenceStatusForProfileWithOptions(.native, .{ .remote_provider_configured = true });
    try std.testing.expectEqualStrings("remote_provider", remote.mode);
    try std.testing.expect(remote.configured);
    try std.testing.expect(remote.remote_provider_configured);
    try std.testing.expect(!remote.local_runtime_configured);
    try std.testing.expectEqual(capabilitiesForProfile(.native).local_inference_runtime, remote.local_runtime_available);
    try std.testing.expect(remote.caller_supplied_artifacts);
    try std.testing.expect(remote.no_inference_configured_ok);

    const local_requested = inferenceStatusForProfileWithOptions(.native, .{ .local_runtime_configured = true });
    if (capabilitiesForProfile(.native).local_inference_runtime) {
        try std.testing.expectEqualStrings("local_embedded", local_requested.mode);
        try std.testing.expect(local_requested.configured);
    } else {
        try std.testing.expectEqualStrings("caller_supplied_or_disabled", local_requested.mode);
        try std.testing.expect(!local_requested.configured);
    }
    try std.testing.expect(!local_requested.remote_provider_configured);
    try std.testing.expect(local_requested.local_runtime_configured);
    try std.testing.expectEqual(capabilitiesForProfile(.native).local_inference_runtime, local_requested.local_runtime_available);
    try std.testing.expectEqual(capabilitiesForProfile(.native).local_inference_runtime, hasMode(local_requested.available_modes, "local_embedded"));
}

test "lite backend native engine creates and checks aflite file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-backend.aflite");
    defer allocator.free(path);

    var handle = try Handle.create(allocator, path, true);
    defer handle.deinit();

    try handle.native_docstore.?.file.putDocument("doc:1", "value");

    const report = try handle.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.record_count);

    var db_opts = db_mod.OpenOptions{};
    try handle.configureDbOpenOptions(&db_opts);
    try std.testing.expect(handle.owned_resource_manager != null);
    try std.testing.expect(handle.native_docstore.?.resource_manager == handle.owned_resource_manager.?);
    try std.testing.expect(db_opts.resource_manager == handle.owned_resource_manager.?);
    try std.testing.expect(db_opts.primary_runtime_store != null);
    try std.testing.expect(db_opts.primary_backend == .mem);
    try std.testing.expectEqual(@as(@TypeOf(db_opts.index_backends.text_main_backend), .lsm), db_opts.index_backends.text_main_backend);
    try std.testing.expectEqual(@as(@TypeOf(db_opts.index_backends.dense_storage_backend), .lsm), db_opts.index_backends.dense_storage_backend);
    try std.testing.expectEqual(@as(@TypeOf(db_opts.index_backends.sparse_backend), .lsm), db_opts.index_backends.sparse_backend);
    try std.testing.expectEqual(@as(@TypeOf(db_opts.index_backends.graph_reverse_backend), .lsm), db_opts.index_backends.graph_reverse_backend);
    try std.testing.expect(db_opts.index_backends.text_lsm_storage != null);
    try std.testing.expect(db_opts.index_backends.dense_lsm_storage != null);
    try std.testing.expect(db_opts.index_backends.sparse_lsm_storage != null);
    try std.testing.expect(db_opts.index_backends.graph_lsm_storage != null);
    try std.testing.expectEqualStrings(native_index_base_path, db_opts.index_base_path.?);
    try std.testing.expectEqual(@as(?usize, 1), db_opts.index_open_parallelism);
    try std.testing.expect(!db_opts.external_derived_checkpoints);
    try std.testing.expectEqual(
        db_mod.OpenOptions.PhysicalRootMode.external_backend,
        db_opts.physical_root_mode,
    );

    const vacuumed = try handle.vacuum();
    try std.testing.expectEqual(report.file_size, vacuumed.before_size);
    try std.testing.expectEqual(report.file_size, vacuumed.after_size);
    try std.testing.expectEqual(@as(u64, 0), vacuumed.reclaimed_bytes);
}

test "lite backend propagates no_sync to native engine" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-backend-no-sync.aflite");
    defer allocator.free(path);

    {
        var created = try Handle.create(allocator, path, true);
        defer created.deinit();
    }

    {
        var handle = try Handle.open(allocator, path, .{
            .engine = .native_single_file,
            .no_sync = true,
        });
        defer handle.deinit();

        try std.testing.expect(handle.native_docstore.?.file.no_sync);
        try handle.native_docstore.?.file.putDocument("doc:no-sync", "value");
    }

    {
        var handle = try Handle.open(allocator, path, .{
            .engine = .native_single_file,
            .read_only = true,
            .no_sync = true,
        });
        defer handle.deinit();

        try std.testing.expect(handle.native_docstore.?.file.no_sync);
        const value = (try handle.native_docstore.?.file.getDocumentAlloc(allocator, "doc:no-sync")) orelse return error.MissingNativeLiteDocument;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("value", value);
    }
}

test "lite backend reports native storage status from active checkpoint" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-status.aflite");
    defer allocator.free(path);

    var handle = try Handle.create(allocator, path, true);
    defer handle.deinit();

    try handle.native_docstore.?.file.putDocument("doc:status", "value");

    const status = handle.storageStatus();
    const checkpoint = handle.native_docstore.?.file.activeCheckpoint();
    try std.testing.expectEqualStrings("aflite", status.format);
    try std.testing.expectEqualStrings("native_single_file", status.engine);
    try std.testing.expectEqualStrings("native_document_pages", status.primary_layout);
    try std.testing.expectEqualStrings("native_replay_lanes_in_document_catalog", status.replay_layout);
    try std.testing.expectEqualStrings(native_index_layout, status.index_layout);
    try std.testing.expectEqualStrings(native_index_base_path, status.index_namespace.?);
    try std.testing.expectEqual(native.format_version, status.format_version.?);
    try std.testing.expectEqual(native.default_page_size, status.page_size.?);
    try std.testing.expectEqual(handle.native_docstore.?.file.header.active_checkpoint, status.active_checkpoint.?);
    try std.testing.expectEqual(checkpoint.commit_sequence, status.checkpoint_sequence.?);
    try std.testing.expectEqual(checkpoint.page_count, status.page_count.?);
}

test "lite backend native stable snapshot uses open handle checkpoint" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-backend-snapshot.aflite");
    defer allocator.free(path);
    const snapshot_path = try testPath(allocator, tmp, "native-backend-snapshot-copy.aflite");
    defer allocator.free(snapshot_path);

    var handle = try Handle.create(allocator, path, true);
    defer handle.deinit();

    try handle.native_docstore.?.file.putDocument("doc:1", "value");
    const snapshot_size = handle.native_docstore.?.file.activeCheckpoint().page_count *
        @as(u64, handle.native_docstore.?.file.header.page_size);
    try handle.native_docstore.?.file.file.writePositionalAll(
        handle.native_docstore.?.file.io_impl.io(),
        "tail",
        snapshot_size,
    );

    const source_report = try handle.check();
    try std.testing.expect(!source_report.valid);
    try std.testing.expectEqualStrings("tail_bytes", source_report.issue.?);

    const copied = try handle.copyStableSnapshot(snapshot_path, false);
    try std.testing.expectEqual(snapshot_size, copied.snapshot_size);
    try std.testing.expectEqual(@as(u64, 4), copied.tail_bytes);

    const snapshot_report = try checkFile(allocator, snapshot_path);
    try std.testing.expect(snapshot_report.valid);
    try std.testing.expectEqual(@as(u64, 0), snapshot_report.tail_bytes);

    var snapshot = try native.NativeFile.open(allocator, snapshot_path, true);
    defer snapshot.close();
    const value = (try snapshot.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(value);
    try std.testing.expectEqualStrings("value", value);
}

test "lite backend native stable snapshot is consistent with active writer" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-backend-snapshot-writer.aflite");
    defer allocator.free(path);
    const snapshot_path = try testPath(allocator, tmp, "native-backend-snapshot-writer-copy.aflite");
    defer allocator.free(snapshot_path);

    var handle = try Handle.create(allocator, path, true);
    defer handle.deinit();

    try handle.native_docstore.?.file.putDocument("doc:committed", "committed");

    var writer = try handle.native_docstore.?.beginWrite();
    defer writer.abort();
    try writer.put("doc:pending", "pending");
    try std.testing.expectError(error.FileBusy, handle.native_docstore.?.beginWrite());

    const copied = try handle.copyStableSnapshot(snapshot_path, false);
    try std.testing.expect(copied.checkpoint_sequence > 0);

    var snapshot = try native.NativeFile.open(allocator, snapshot_path, true);
    defer snapshot.close();

    const committed = (try snapshot.getDocumentAlloc(allocator, "doc:committed")).?;
    defer allocator.free(committed);
    try std.testing.expectEqualStrings("committed", committed);
    try std.testing.expectEqual(@as(?[]u8, null), try snapshot.getDocumentAlloc(allocator, "doc:pending"));
}

test "lite backend auto open requires existing native aflite file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "auto-native.aflite");
    defer allocator.free(path);

    try std.testing.expectError(error.FileNotFound, Handle.open(allocator, path, .{}));

    var handle = try Handle.create(allocator, path, true);
    defer handle.deinit();
    try std.testing.expectEqual(EngineKind.native_single_file, handle.engine);
    try handle.native_docstore.?.file.putDocument("doc:auto", "native");

    const report = try handle.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.record_count);
}

test "lite backend rejects non-aflite paths" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "not-lite.db");
    defer allocator.free(path);
    const snapshot_path = try testPath(allocator, tmp, "not-lite-copy.db");
    defer allocator.free(snapshot_path);
    const lite_path = try testPath(allocator, tmp, "valid-source.aflite");
    defer allocator.free(lite_path);

    try std.testing.expect(!isAflitePath(path));
    try std.testing.expectError(error.InvalidArgument, Handle.open(allocator, path, .{}));
    try std.testing.expectError(error.InvalidArgument, checkFile(allocator, path));

    var handle = try Handle.create(allocator, lite_path, true);
    defer handle.deinit();
    try std.testing.expectError(error.InvalidArgument, handle.copyStableSnapshot(snapshot_path, false));
}

test "lite backend auto rejects internal bridge aflite files" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "auto-bridge.aflite");
    defer allocator.free(path);

    {
        var handle = try Handle.open(allocator, path, .{ .engine = .bridge_lsm_container });
        defer handle.deinit();
        try std.testing.expectEqual(EngineKind.bridge_lsm_container, handle.engine);
        const status = handle.storageStatus();
        try std.testing.expectEqualStrings("aflite-internal", status.format);
        try std.testing.expectEqualStrings("bridge_lsm_container", status.engine);
        try std.testing.expectEqualStrings("lsm_container", status.replay_layout);
        try std.testing.expect(status.index_namespace == null);
    }

    try std.testing.expectError(error.TruncatedNativeHeader, Handle.open(allocator, path, .{}));
}

test "lite backend auto rejects invalid native headers without fallback" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const invalid_magic_path = try testPath(allocator, tmp, "auto-invalid-magic.aflite");
    defer allocator.free(invalid_magic_path);
    const unsupported_version_path = try testPath(allocator, tmp, "auto-unsupported-version.aflite");
    defer allocator.free(unsupported_version_path);

    {
        var encoded: [native.header_size]u8 = @splat(0);
        @memcpy(encoded[0.."AFLITE0X".len], "AFLITE0X");
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, invalid_magic_path, .{});
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, &encoded, 0);
    }

    try std.testing.expectError(error.InvalidNativeMagic, Handle.open(allocator, invalid_magic_path, .{}));
    const invalid_report = try checkFile(allocator, invalid_magic_path);
    try std.testing.expect(!invalid_report.valid);
    try std.testing.expectEqualStrings("invalid_magic", invalid_report.issue.?);

    {
        var encoded: [native.header_size]u8 = undefined;
        native.encodeHeader(&encoded, .{});
        std.mem.writeInt(u32, encoded[native.magic.len..][0..4], native.format_version + 1, .little);
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, unsupported_version_path, .{});
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, &encoded, 0);
    }

    try std.testing.expectError(error.UnsupportedNativeFormatVersion, Handle.open(allocator, unsupported_version_path, .{}));
    const unsupported_report = try checkFile(allocator, unsupported_version_path);
    try std.testing.expect(!unsupported_report.valid);
    try std.testing.expectEqualStrings("unsupported_format_version", unsupported_report.issue.?);
}

test "lite backend native engine can back db primary documents" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-db.aflite");
    defer allocator.free(path);

    {
        var handle = try Handle.create(allocator, path, true);
        defer handle.deinit();

        var db_opts = db_mod.OpenOptions{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .start_optional_runtimes = false,
            .ttl_cleanup = .{ .enabled = false },
        };
        try handle.configureDbOpenOptions(&db_opts);

        var db = try db_mod.DB.open(allocator, path, db_opts);
        defer db.close();
        try std.testing.expectError(error.DurableRootIncarnationUnavailable, db.durableRootIncarnation());

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:lite-native",
                .value = "{\"name\":\"native\"}",
            }},
            .sync_level = .write,
        });

        const value = try db.get(allocator, "doc:lite-native") orelse return error.MissingNativeLiteDocument;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("{\"name\":\"native\"}", value);
    }

    {
        var handle = try Handle.open(allocator, path, .{
            .engine = .native_single_file,
            .read_only = true,
        });
        defer handle.deinit();

        var db_opts = db_mod.OpenOptions{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .start_optional_runtimes = false,
            .ttl_cleanup = .{ .enabled = false },
        };
        try handle.configureDbOpenOptions(&db_opts);

        var db = try db_mod.DB.open(allocator, path, db_opts);
        defer db.close();

        const value = try db.get(allocator, "doc:lite-native") orelse return error.MissingNativeLiteDocument;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("{\"name\":\"native\"}", value);
    }
}

test "lite backend namespaced db options isolate tables in one file" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-namespaced-db.aflite");
    defer allocator.free(path);
    const logical_path_a = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/table/a", .{tmp.sub_path});
    defer allocator.free(logical_path_a);
    const logical_path_b = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/table/b", .{tmp.sub_path});
    defer allocator.free(logical_path_b);

    var handle = try Handle.create(allocator, path, true);
    defer handle.deinit();
    var opts_a = db_mod.OpenOptions{ .open_mode = .writer_no_replay, .start_index_workers = false, .start_optional_runtimes = false };
    var opts_b = opts_a;
    try handle.configureDbOpenOptionsForNamespace(&opts_a, "table/a");
    try handle.configureDbOpenOptionsForNamespace(&opts_b, "table/b");
    const runtime_a = try handle.runtimeStoreForNamespace("table/a");
    try std.testing.expect(runtime_a == try handle.runtimeStoreForNamespace("table/a"));
    try std.testing.expectEqual(@as(usize, 2), handle.namespace_runtimes.count());
    var db_a = try db_mod.DB.open(allocator, logical_path_a, opts_a);
    defer db_a.close();
    var db_b = try db_mod.DB.open(allocator, logical_path_b, opts_b);
    defer db_b.close();

    try db_a.batch(.{ .writes = &.{.{ .key = "doc:same", .value = "{\"table\":\"a\"}" }}, .sync_level = .write });
    try db_b.batch(.{ .writes = &.{.{ .key = "doc:same", .value = "{\"table\":\"b\"}" }}, .sync_level = .write });
    const value_a = (try db_a.get(allocator, "doc:same")).?;
    defer allocator.free(value_a);
    const value_b = (try db_b.get(allocator, "doc:same")).?;
    defer allocator.free(value_b);
    try std.testing.expectEqualStrings("{\"table\":\"a\"}", value_a);
    try std.testing.expectEqualStrings("{\"table\":\"b\"}", value_b);
    try std.testing.expect(!std.mem.eql(u8, opts_a.index_base_path.?, opts_b.index_base_path.?));
}

test "lite backend adopts embedded root into a move-stable standalone namespace" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "embedded-standalone.aflite");
    defer allocator.free(path);
    const logical_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/group-42/table-db", .{tmp.sub_path});
    defer allocator.free(logical_path);

    // Model data produced by `antfly lite batch` in the embedded root.
    {
        var handle = try Handle.create(allocator, path, true);
        defer handle.deinit();
        try handle.markEmbeddedArtifact();
        var opts = db_mod.OpenOptions{ .open_mode = .writer_no_replay, .start_index_workers = false, .start_optional_runtimes = false };
        try handle.configureDbOpenOptions(&opts);
        var db = try db_mod.DB.open(allocator, path, opts);
        defer db.close();
        try db.batch(.{ .writes = &.{.{ .key = "doc:portable", .value = "{\"source\":\"embedded\"}" }}, .sync_level = .write });
    }

    // The first standalone serve persists the logical table alias before it
    // exposes catalog metadata.
    {
        var handle = try Handle.open(allocator, path, .{});
        defer handle.deinit();
        try std.testing.expect(try handle.isEmbeddedArtifact());
        try handle.adoptEmbeddedRootAsNamespace("/var/lib/antfly/group-42/table-db");
        var opts = db_mod.OpenOptions{ .open_mode = .writer_no_replay, .start_index_workers = false, .start_optional_runtimes = false };
        try handle.configureDbOpenOptionsForNamespace(&opts, "/different/root/group-42/table-db");
        try std.testing.expectEqualStrings(native_index_base_path, opts.index_base_path.?);
        var db = try db_mod.DB.open(allocator, logical_path, opts);
        defer db.close();
        const value = (try db.get(allocator, "doc:portable")) orelse return error.MissingAdoptedEmbeddedDocument;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("{\"source\":\"embedded\"}", value);
    }

    // Reopen after a file move/data-directory change resolves the same stable
    // group namespace from the artifact catalog rather than the host path.
    {
        var handle = try Handle.open(allocator, path, .{ .read_only = true });
        defer handle.deinit();
        try std.testing.expect(handle.hasStandaloneRootAdoption());
        var opts = db_mod.OpenOptions{ .open_mode = .query_readonly, .start_index_workers = false, .start_optional_runtimes = false };
        try handle.configureDbOpenOptionsForNamespace(&opts, "/mnt/restored/group-42/table-db");
        var db = try db_mod.DB.open(allocator, logical_path, opts);
        defer db.close();
        const value = (try db.get(allocator, "doc:portable")) orelse return error.MissingReopenedAdoptedDocument;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("{\"source\":\"embedded\"}", value);
    }
}

test "lite backend rejects root adoption after the target namespace opens" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "embedded-adoption-open.aflite");
    defer allocator.free(path);

    var handle = try Handle.create(allocator, path, true);
    defer handle.deinit();
    var opts = db_mod.OpenOptions{};
    try handle.configureDbOpenOptionsForNamespace(&opts, "docs");
    try std.testing.expectError(error.LiteNamespaceAlreadyOpened, handle.adoptEmbeddedRootAsNamespace("docs"));
    try std.testing.expect(!handle.hasStandaloneRootAdoption());
}

test "lite backend fails closed on unknown artifact profile and invalid root alias" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const profile_path = try testPath(allocator, tmp, "invalid-profile.aflite");
    defer allocator.free(profile_path);
    {
        var handle = try Handle.create(allocator, profile_path, true);
        try handle.native_docstore.?.file.putCatalogRecord(artifact_profile_catalog_key, "future-unknown-profile");
        handle.deinit();
    }
    try std.testing.expectError(error.InvalidLiteArtifactProfile, Handle.open(allocator, profile_path, .{}));

    const alias_path = try testPath(allocator, tmp, "invalid-alias.aflite");
    defer allocator.free(alias_path);
    {
        var handle = try Handle.create(allocator, alias_path, true);
        try handle.native_docstore.?.file.putCatalogRecord(root_namespace_alias_catalog_key, "/host/path/group-1/table-db");
        handle.deinit();
    }
    try std.testing.expectError(error.InvalidLiteRootNamespaceAlias, Handle.open(allocator, alias_path, .{}));
}

test "lite backend native open requires an existing file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-missing.aflite");
    defer allocator.free(path);

    try std.testing.expectError(error.FileNotFound, Handle.open(allocator, path, .{ .engine = .native_single_file }));
    try std.testing.expectError(error.FileNotFound, Handle.open(allocator, path, .{
        .engine = .native_single_file,
        .read_only = true,
    }));
}
