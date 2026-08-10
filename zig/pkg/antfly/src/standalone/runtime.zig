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
const builtin = @import("builtin");
const platform_sync = @import("antfly_platform").sync;
const httpx = @import("httpx");
const antfly = @import("runtime_root.zig");
const group_ids = @import("../common/group_ids.zig");
const threaded_io_limits = @import("../common/threaded_io_limits.zig");
const inference = @import("inference_server");
const metadata_openapi = @import("antfly_metadata_openapi");
const usermgr_openapi = @import("antfly_usermgr_openapi");
const fs_paths = @import("../common/fs_paths.zig");
const platform_time = @import("antfly_platform").time;
const platform = @import("antfly_platform");

const AntflyApiHandler = antfly.public_api.httpx_handler.AntflyApiHandler;
const http_common = antfly.common.http;
const public_api_max_requests_per_connection: u32 = 64;
const public_api_max_body_size: usize = antfly.common.http.default_max_request_bytes;
const local_schema_migration_finalize_interval_ms: u64 = std.time.ms_per_s;

const LocalSchemaProgressProvider = struct {
    ptr: *anyopaque,
    collect: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const antfly.metadata.TableRecord,
        ranges: []const antfly.metadata.RangeRecord,
    ) anyerror!antfly.data.runtime.DataServer.LocalSchemaProgressSnapshot,
};
const default_public_port: u16 = 8080;
const antfarm_max_file_bytes: usize = 64 * 1024 * 1024;
const standalone_session_ttl_ns: u64 = std.time.ns_per_hour;
const standalone_session_cleanup_interval_ns: u64 = std.time.ns_per_min;
const standalone_session_max_count: usize = 1024;
const standalone_session_max_record_bytes: usize = 16 * 1024 * 1024;
const standalone_session_savepoint_limit: usize = 64;
const antfarm_installed_asset_root = "../share/antfly/antfarm";
const antfarm_asset_roots = [_][]const u8{
    "src/metadata/antfarm",
    "../src/metadata/antfarm",
    "/usr/share/antfly/antfarm",
    "antfarm",
};

var termination_requested: std.atomic.Value(bool) = .init(false);

fn terminationSignalHandler(_: std.posix.SIG) callconv(.c) void {
    // Atomic publication is async-signal-safe. Listener and storage teardown
    // remain on their owning threads.
    termination_requested.store(true, .release);
}

const TerminationSignalScope = struct {
    old_int: std.posix.Sigaction,
    old_term: std.posix.Sigaction,

    fn install() TerminationSignalScope {
        termination_requested.store(false, .release);
        const action = std.posix.Sigaction{
            .handler = .{ .handler = terminationSignalHandler },
            .mask = std.posix.sigemptyset(),
            .flags = 0,
        };
        var old_int: std.posix.Sigaction = undefined;
        var old_term: std.posix.Sigaction = undefined;
        std.posix.sigaction(.INT, &action, &old_int);
        std.posix.sigaction(.TERM, &action, &old_term);
        return .{ .old_int = old_int, .old_term = old_term };
    }

    fn deinit(self: *TerminationSignalScope) void {
        std.posix.sigaction(.INT, &self.old_int, null);
        std.posix.sigaction(.TERM, &self.old_term, null);
        termination_requested.store(false, .release);
        self.* = undefined;
    }
};

const CliConfig = struct {
    config_path: ?[]const u8 = null,
    experimental: bool = false,
    bind_host: ?[]const u8 = null,
    bind_port: ?u16 = null,
    health_enabled: ?bool = null,
    health_port: ?u16 = null,
    control_tick_ms: u64 = antfly.raft.RuntimeCadence.default_control_tick_ms,
    local_node_id: ?u64 = null,
    auth_enabled: ?bool = null,
    ard_base_url: ?[]const u8 = null,
    ard_publisher_domain: ?[]const u8 = null,
    ard_display_name: ?[]const u8 = null,
    ard_public_catalog_enabled: bool = false,
    inference_models_dir: ?[]const u8 = null,
    inference_ml_dir: ?[]const u8 = null,
    inference_host_budget_mb: usize = 0,
    inference_backend_budget_mb: usize = 0,
    inference_combined_budget_mb: usize = 0,
    inference_kv_budget_mb: usize = 0,
    inference_scratch_budget_mb: usize = 0,
    inference_preload_models: std.ArrayListUnmanaged(inference.server.WarmModel) = .empty,
    data_dir: ?[]const u8 = null,
    storage_engine: ?antfly.common.config.StorageEngine = null,
    storage_path: ?[]const u8 = null,
    storage_fsync: ?bool = null,
    replica_root_dir: ?[]const u8 = null,
    replica_catalog_path: ?[]const u8 = null,
    snapshot_root_dir: ?[]const u8 = null,
    extension_package_store_dir: ?[]const u8 = null,
    secret_store_paths: std.ArrayListUnmanaged([]const u8) = .empty,
    ha_primary_log: ?[]const u8 = null,
    ha_primary_slots: ?[]const u8 = null,
    ha_primary_node_id: ?[]const u8 = null,
    ha_fence_wal: ?[]const u8 = null,
    ha_former_primary_log: ?[]const u8 = null,
    admin_token_env: ?[]const u8 = null,
    ha_retention_max_lag_lsn: ?u64 = null,
    ha_retention_max_retained_bytes: ?u64 = null,
    ha_retention_max_retained_age_ns: ?u64 = null,
    ha_sync_mode: ?antfly.ha.primary.DurabilityMode = null,
    ha_sync_selection: ?antfly.ha.primary.StandbySelection = null,
    ha_sync_required: ?usize = null,
    ha_sync_failure_policy: ?antfly.ha.primary.FailurePolicy = null,
    ha_sync_standby_names: std.ArrayListUnmanaged([]const u8) = .empty,
    ha_standby_log: ?[]const u8 = null,
    ha_standby_progress: ?[]const u8 = null,
    ha_standby_node_id: ?[]const u8 = null,
    ha_standby_upstream_url: ?[]const u8 = null,
    ha_standby_slot: ?[]const u8 = null,
    ha_cluster_id: ?u64 = null,
    ha_shard_id: ?u64 = null,
    ha_table_id: ?u64 = null,
    ha_timeline_id: ?u64 = null,
    ha_epoch: ?u64 = null,
    help: bool = false,

    fn deinit(self: *CliConfig, alloc: std.mem.Allocator) void {
        self.secret_store_paths.deinit(alloc);
        self.ha_sync_standby_names.deinit(alloc);
        self.inference_preload_models.deinit(alloc);
        self.* = undefined;
    }

    fn primarySecretStorePath(self: *const CliConfig) ?[]const u8 {
        if (self.secret_store_paths.items.len == 0) return null;
        return self.secret_store_paths.items[0];
    }
};

const ResolvedPaths = struct {
    replica_root_dir: []u8,
    replica_catalog_path: []u8,
    local_metadata_catalog_path: []u8,
    snapshot_root_dir: []u8,
    extension_package_store_dir: []u8,
    secret_store_path: []u8,
    auth_store_root_dir: []u8,

    fn deinit(self: ResolvedPaths, alloc: std.mem.Allocator) void {
        alloc.free(self.replica_root_dir);
        alloc.free(self.replica_catalog_path);
        alloc.free(self.local_metadata_catalog_path);
        alloc.free(self.snapshot_root_dir);
        alloc.free(self.extension_package_store_dir);
        alloc.free(self.secret_store_path);
        alloc.free(self.auth_store_root_dir);
    }
};

const StandaloneHealthSource = struct {
    data_server: *antfly.data.runtime.DataServer,
    unified_api_ready: *const std.atomic.Value(bool),
    handler: *const AntflyApiHandler,
    unified_lifecycle: *UnifiedServerLifecycle,

    fn readiness(self: *StandaloneHealthSource) antfly.common.health_server.ReadinessChecker {
        return .{
            .ptr = self,
            .vtable = &.{ .check = checkReady },
        };
    }

    fn metricsWriter(self: *StandaloneHealthSource) antfly.common.health_server.MetricsWriter {
        return .{
            .ptr = self,
            .vtable = &.{ .write_metrics = writeMetrics },
        };
    }

    fn checkReady(ptr: *anyopaque) bool {
        const self: *StandaloneHealthSource = @ptrCast(@alignCast(ptr));
        if (self.data_server.http_server) |*api_server| {
            if (api_server.storageMaintenanceExclusiveActive()) return false;
        }
        return standaloneReadyFromState(
            self.data_server.http_server != null,
            self.unified_api_ready.load(.acquire),
        );
    }

    fn writeMetrics(ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void {
        const self: *StandaloneHealthSource = @ptrCast(@alignCast(ptr));
        var data_health = antfly.data.runtime.HealthSource{ .data_server = self.data_server };
        try data_health.metricsWriter().writeMetrics(writer);

        const query = self.handler.query_admission.stats();
        const query_body = self.handler.query_body_admission.stats();
        const handler = self.handler.runtimeStats();
        try antfly.common.health_server.appendPromMetric(writer, "antfly_query_capacity", "gauge", "Maximum concurrent expensive public queries", query.capacity);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_query_in_flight", "gauge", "Currently executing expensive public queries", query.in_flight);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_query_peak_in_flight", "gauge", "Peak concurrent expensive public queries since process start", query.peak_in_flight);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_query_rejected_total", "counter", "Public queries rejected by admission control", query.rejected_total);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_query_body_capacity", "gauge", "Maximum concurrent streaming H2 query bodies", query_body.capacity);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_query_bodies_in_flight", "gauge", "Streaming H2 query bodies currently admitted", query_body.in_flight);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_query_body_peak_in_flight", "gauge", "Peak concurrent streaming H2 query bodies since process start", query_body.peak_in_flight);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_query_body_rejected_total", "counter", "Streaming H2 query bodies rejected by admission control", query_body.rejected_total);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_http_cancellation_watcher_start_failures_total", "counter", "Public queries rejected because peer observation could not be scheduled", handler.cancellation_watcher_start_failures_total);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_http_peer_disconnects_total", "counter", "Public query peer disconnects propagated to cancellation", handler.peer_disconnect_cancellations_total);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_http_peer_observer_failures_total", "counter", "Public queries cancelled after peer observation failed", handler.peer_observer_failures_total);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_http_active_peer_observers", "gauge", "Public query sockets currently watched for disconnect", handler.active_peer_observers);

        if (self.unified_lifecycle.runtimeStats()) |http| {
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_connection_limit", "gauge", "Maximum concurrent public HTTP connections", http.max_connections);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_active_connections", "gauge", "Currently active public HTTP connections", http.active_connections);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_active_requests", "gauge", "Currently active public HTTP requests", http.active_requests);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_accept_errors_total", "counter", "Public HTTP listener accept failures", http.accept_errors_total);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_body_buffer_capacity_bytes", "gauge", "Aggregate HTTP request-body buffer capacity", http.body_buffer_capacity_bytes);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_body_buffer_in_use_bytes", "gauge", "HTTP request-body bytes admitted across HTTP/1 and HTTP/2", http.body_buffer_in_use_bytes);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_body_buffer_peak_bytes", "gauge", "Peak admitted HTTP request-body bytes since process start", http.body_buffer_peak_bytes);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_body_buffer_rejected_total", "counter", "HTTP request bodies rejected by aggregate memory admission", http.body_buffer_rejected_total);
        }
    }
};

fn standaloneReadyFromState(api_server_initialized: bool, unified_api_ready: bool) bool {
    return api_server_initialized and unified_api_ready;
}

const UnifiedServerLifecycle = struct {
    const State = enum(u8) { starting, ready, failed, stopping, stopped };

    state: std.atomic.Value(State) = .init(.starting),
    mutex: std.atomic.Mutex = .unlocked,
    server: ?*httpx.Server = null,
    failure: anyerror = error.Unexpected,

    fn attach(self: *UnifiedServerLifecycle, server: *httpx.Server) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.server = server;
    }

    fn detach(self: *UnifiedServerLifecycle, server: *httpx.Server) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.server == server) self.server = null;
    }

    fn publishReady(self: *UnifiedServerLifecycle) void {
        self.state.store(.ready, .release);
    }

    fn publishFailure(self: *UnifiedServerLifecycle, err: anyerror) void {
        if (self.state.load(.acquire) == .stopping) {
            self.state.store(.stopped, .release);
            return;
        }
        self.failure = err;
        self.state.store(.failed, .release);
    }

    fn publishStopped(self: *UnifiedServerLifecycle) void {
        self.state.store(.stopped, .release);
    }

    fn waitForStartup(self: *UnifiedServerLifecycle) !void {
        while (true) switch (self.state.load(.acquire)) {
            .starting => std.Thread.yield() catch {},
            .ready => return,
            .failed => return self.failure,
            .stopping, .stopped => return error.ServerStopped,
        };
    }

    fn runtimeFailure(self: *UnifiedServerLifecycle) ?anyerror {
        return if (self.state.load(.acquire) == .failed) self.failure else null;
    }

    fn runtimeStats(self: *UnifiedServerLifecycle) ?httpx.Server.RuntimeStats {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const server = self.server orelse return null;
        return server.runtimeStats();
    }

    fn stop(self: *UnifiedServerLifecycle) void {
        const prior = self.state.swap(.stopping, .acq_rel);
        if (prior == .failed or prior == .stopped) return;
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.server) |server| server.requestStop();
    }

    fn shutdown(self: *UnifiedServerLifecycle, timeout_ms: u64) void {
        const prior = self.state.swap(.stopping, .acq_rel);
        if (prior == .failed or prior == .stopped) return;
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.server) |server| server.shutdown(timeout_ms);
    }
};

/// Process-level ownership for a public bind tuple. Zig 0.16's POSIX
/// `reuse_address` also enables SO_REUSEPORT, so the kernel socket alone does
/// not provide exclusivity. Keeping this advisory lock for the listener
/// lifetime permits SO_REUSEADDR fast restarts without allowing two Antfly
/// processes on the same host to accept the same port.
const PublicListenerLease = struct {
    alloc: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    file: std.Io.File,
    path: []u8,

    fn acquire(alloc: std.mem.Allocator, bind_port: u16) !PublicListenerLease {
        var io_impl = threaded_io_limits.initService(alloc);
        errdefer io_impl.deinit();
        // Lease by port rather than host spelling: wildcard/specific binds and
        // IPv6 aliases can overlap even when their input strings differ.
        const path = try std.fmt.allocPrint(alloc, "/tmp/antfly-listener-{d}.lock", .{bind_port});
        errdefer alloc.free(path);
        const file = std.Io.Dir.cwd().createFile(io_impl.io(), path, .{
            .read = true,
            .truncate = false,
            .lock = .exclusive,
            .lock_nonblocking = true,
        }) catch |err| switch (err) {
            error.WouldBlock => return error.AddressInUse,
            error.FileLocksUnsupported => return error.ListenerLockUnsupported,
            else => return err,
        };
        return .{ .alloc = alloc, .io_impl = io_impl, .file = file, .path = path };
    }

    fn deinit(self: *PublicListenerLease) void {
        self.file.close(self.io_impl.io());
        self.io_impl.deinit();
        self.alloc.free(self.path);
        self.* = undefined;
    }
};

const LocalStandaloneMetadata = struct {
    alloc: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    manager: antfly.metadata.TableManager,
    extension_catalog: antfly.extensions.ExtensionCatalog,
    local_node_id: u64,
    store_id: u64,
    api_url: []const u8,
    replica_root_dir: []const u8,
    catalog_path: []const u8,
    catalog_store: ?*antfly.storage_backend_erased.Store,
    backend_runtime: *antfly.db.background_runtime.BackendRuntime,
    storage_engine: antfly.common.config.StorageEngine = .local,
    epoch: u64 = 1,
    last_schema_migration_finalize_at_ms: u64 = 0,
    local_schema_progress_provider: ?LocalSchemaProgressProvider = null,

    const PersistedCatalog = struct {
        epoch: u64 = 1,
        tables: []const antfly.metadata.TableRecord = &.{},
        ranges: []const antfly.metadata.RangeRecord = &.{},
        extension_packages: []const antfly.extensions.PackageManifest = &.{},
        installed_extensions: []const antfly.extensions.InstalledExtension = &.{},
        extension_members: []const antfly.extensions.ExtensionMember = &.{},
        extension_dependencies: []const antfly.extensions.ExtensionDependency = &.{},
    };

    const CatalogMutation = struct {
        previous_manager: antfly.metadata.TableManager,
        previous_extensions: antfly.extensions.ExtensionCatalog,
        previous_epoch: u64,
        committed: bool = false,

        fn commit(self: *CatalogMutation, metadata: *LocalStandaloneMetadata) !void {
            try metadata.persistLocked();
            self.committed = true;
        }

        fn deinit(self: *CatalogMutation, metadata: *LocalStandaloneMetadata) void {
            if (self.committed) {
                self.previous_extensions.deinit();
                self.previous_manager.deinit();
                return;
            }
            metadata.extension_catalog.deinit();
            metadata.manager.deinit();
            metadata.manager = self.previous_manager;
            metadata.extension_catalog = self.previous_extensions;
            metadata.epoch = self.previous_epoch;
        }
    };

    fn beginCatalogMutationLocked(self: *LocalStandaloneMetadata) !CatalogMutation {
        var manager = antfly.metadata.TableManager.init(self.alloc);
        errdefer manager.deinit();
        const tables = try self.manager.listTables(self.alloc);
        defer self.manager.freeTables(self.alloc, tables);
        const ranges = try self.manager.listRanges(self.alloc);
        defer self.manager.freeRanges(self.alloc, ranges);
        _ = try manager.replaceProjectedTopology(tables, ranges);
        return .{
            .previous_manager = manager,
            .previous_extensions = try self.cloneExtensionCatalogLocked(),
            .previous_epoch = self.epoch,
        };
    }

    fn init(
        alloc: std.mem.Allocator,
        local_node_id: u64,
        store_id: u64,
        api_url: []const u8,
        replica_root_dir: []const u8,
        catalog_path: []const u8,
        backend_runtime: *antfly.db.background_runtime.BackendRuntime,
        catalog_store: ?*antfly.storage_backend_erased.Store,
        storage_engine: antfly.common.config.StorageEngine,
    ) !LocalStandaloneMetadata {
        var owned_api_url: ?[]u8 = try alloc.dupe(u8, api_url);
        errdefer if (owned_api_url) |value| alloc.free(value);
        var owned_replica_root_dir: ?[]u8 = try alloc.dupe(u8, replica_root_dir);
        errdefer if (owned_replica_root_dir) |value| alloc.free(value);
        var owned_catalog_path: ?[]u8 = try alloc.dupe(u8, catalog_path);
        errdefer if (owned_catalog_path) |value| alloc.free(value);
        var self = LocalStandaloneMetadata{
            .alloc = alloc,
            .manager = antfly.metadata.TableManager.init(alloc),
            .extension_catalog = antfly.extensions.ExtensionCatalog.init(alloc),
            .local_node_id = local_node_id,
            .store_id = store_id,
            .api_url = owned_api_url.?,
            .replica_root_dir = owned_replica_root_dir.?,
            .catalog_path = owned_catalog_path.?,
            .catalog_store = catalog_store,
            .backend_runtime = backend_runtime,
            .storage_engine = storage_engine,
        };
        owned_api_url = null;
        owned_replica_root_dir = null;
        owned_catalog_path = null;
        errdefer self.deinit();
        try self.loadPersistedCatalog();
        return self;
    }

    fn deinit(self: *LocalStandaloneMetadata) void {
        self.extension_catalog.deinit();
        self.manager.deinit();
        self.alloc.free(self.catalog_path);
        self.alloc.free(self.replica_root_dir);
        self.alloc.free(self.api_url);
        self.* = undefined;
    }

    fn catalogSource(self: *LocalStandaloneMetadata) antfly.public_api.table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = catalogAdminSnapshot,
                .free_admin_snapshot = catalogFreeAdminSnapshot,
            },
        };
    }

    fn statusSource(self: *LocalStandaloneMetadata) antfly.public_api.http_server.StatusSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .status = status,
                .admin_snapshot = catalogAdminSnapshot,
                .cached_admin_snapshot = cachedAdminSnapshot,
                .free_admin_snapshot = catalogFreeAdminSnapshot,
                .create_table = createTable,
                .replace_table_definition = replaceTableDefinition,
                .restore_table = restoreTable,
                .drop_table = dropTable,
                .update_schema = updateSchema,
                .update_schema_versioned = updateSchemaVersioned,
                .create_index = createIndex,
                .drop_index = dropIndex,
                .put_artifact_enrichment = putArtifactEnrichment,
                .delete_artifact_enrichment = deleteArtifactEnrichment,
                .wait_table_lifecycle = waitTableLifecycle,
                .wait_table_projection = waitTableProjection,
                .run_round = runRound,
                .install_extension = installExtension,
                .update_extension = updateExtension,
                .drop_extension = dropExtension,
                .enable_extension = enableExtension,
                .disable_extension = disableExtension,
                .configure_extension = configureExtension,
                .restore_extensions = restoreExtensions,
            },
        };
    }

    fn status(ptr: *anyopaque) !antfly.metadata_api.MetadataStatus {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return .{
            .metadata_group_id = group_ids.main_metadata_group_id,
            .metadata_epoch = self.epoch,
            .metadata_raft_role = "disabled",
            .projected_tables = self.manager.tables.count(),
            .projected_extension_packages = self.extension_catalog.packages.items.len,
            .projected_installed_extensions = self.extension_catalog.installed.items.len,
            .projected_extension_members = self.extension_catalog.members.items.len,
            .projected_extension_dependencies = self.extension_catalog.dependencies.items.len,
            .projected_ranges = self.manager.ranges.count(),
            .projected_stores = 1,
            .projected_placement_intents = self.manager.ranges.count(),
            .metrics = .{},
        };
    }

    fn cachedAdminSnapshot(ptr: *anyopaque) !?antfly.metadata_api.AdminSnapshot {
        return try catalogAdminSnapshot(ptr);
    }

    fn catalogAdminSnapshot(ptr: *anyopaque) !antfly.metadata_api.AdminSnapshot {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const tables = try self.manager.listTables(self.alloc);
        errdefer self.manager.freeTables(self.alloc, tables);
        const ranges = try self.manager.listRanges(self.alloc);
        errdefer self.manager.freeRanges(self.alloc, ranges);
        const extension_packages = try self.extension_catalog.listPackages(self.alloc);
        errdefer self.extension_catalog.freePackages(self.alloc, extension_packages);
        const installed_extensions = try self.extension_catalog.listInstalled(self.alloc);
        errdefer self.extension_catalog.freeInstalled(self.alloc, installed_extensions);
        const extension_members = try self.extension_catalog.listMembers(self.alloc);
        errdefer self.extension_catalog.freeMembers(self.alloc, extension_members);
        const extension_dependencies = try self.extension_catalog.listDependencies(self.alloc);
        errdefer self.extension_catalog.freeDependencies(self.alloc, extension_dependencies);

        const stores = try self.alloc.alloc(antfly.metadata.StoreRecord, 1);
        errdefer self.alloc.free(stores);
        stores[0] = try antfly.metadata.table_manager.cloneStore(self.alloc, .{
            .store_id = self.store_id,
            .node_id = self.local_node_id,
            .api_url = self.api_url,
            .role = "data",
            .health_class = "healthy",
            .live = true,
        });
        errdefer antfly.metadata.table_manager.freeStore(self.alloc, stores[0]);

        const placement_intents = try self.alloc.alloc(antfly.raft.PlacementIntent, ranges.len);
        errdefer self.alloc.free(placement_intents);
        for (ranges, 0..) |range, i| {
            placement_intents[i] = .{
                .record = .{
                    .group_id = range.group_id,
                    .replica_id = 1,
                    .local_node_id = self.local_node_id,
                    .bootstrap_mode = .persisted,
                    .metadata_version = self.epoch,
                },
                .store_id = self.store_id,
                .peer_node_ids = &.{},
            };
        }

        return .{
            .status = .{
                .metadata_group_id = group_ids.main_metadata_group_id,
                .metadata_epoch = self.epoch,
                .metadata_raft_role = "disabled",
                .projected_tables = tables.len,
                .projected_extension_packages = extension_packages.len,
                .projected_installed_extensions = installed_extensions.len,
                .projected_extension_members = extension_members.len,
                .projected_extension_dependencies = extension_dependencies.len,
                .projected_ranges = ranges.len,
                .projected_stores = stores.len,
                .projected_placement_intents = placement_intents.len,
                .metrics = .{},
            },
            .tables = tables,
            .ranges = ranges,
            .stores = stores,
            .placement_intents = placement_intents,
            .extension_packages = extension_packages,
            .installed_extensions = installed_extensions,
            .extension_members = extension_members,
            .extension_dependencies = extension_dependencies,
            .split_transitions = try self.alloc.alloc(antfly.metadata.SplitTransitionRecord, 0),
            .merge_transitions = try self.alloc.alloc(antfly.metadata.MergeTransitionRecord, 0),
        };
    }

    fn catalogFreeAdminSnapshot(ptr: *anyopaque, snapshot: *antfly.metadata_api.AdminSnapshot) void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        self.manager.freeTables(self.alloc, snapshot.tables);
        self.manager.freeRanges(self.alloc, snapshot.ranges);
        for (snapshot.stores) |store| antfly.metadata.table_manager.freeStore(self.alloc, store);
        self.alloc.free(snapshot.stores);
        self.alloc.free(snapshot.placement_intents);
        self.extension_catalog.freePackages(self.alloc, snapshot.extension_packages);
        self.extension_catalog.freeInstalled(self.alloc, snapshot.installed_extensions);
        self.extension_catalog.freeMembers(self.alloc, snapshot.extension_members);
        self.extension_catalog.freeDependencies(self.alloc, snapshot.extension_dependencies);
        self.alloc.free(snapshot.split_transitions);
        self.alloc.free(snapshot.merge_transitions);
        snapshot.* = undefined;
    }

    fn createTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: antfly.public_api.tables.CreateTableRequest) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        const table = try deriveStandaloneTableRecord(self.storage_engine, table_name, req);
        const ranges = try antfly.public_api.tables.deriveInitialRanges(alloc, table);
        defer {
            for (ranges) |record| antfly.metadata.table_manager.freeRange(alloc, record);
            alloc.free(ranges);
        }

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.findTableByNameLocked(table_name) != null) return error.TableAlreadyExists;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(table);
        for (ranges) |range| try self.manager.upsertRange(range);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn adoptEmbeddedLiteRootIfNeeded(self: *LocalStandaloneMetadata, backend: *antfly.lite.backend.Handle) !void {
        const existing = try self.manager.listTables(self.alloc);
        defer self.manager.freeTables(self.alloc, existing);
        if (existing.len != 0) return;
        if (!(try backend.isEmbeddedArtifact()) and !(try backend.embeddedRootHasUserDocuments())) return;

        const table = try deriveStandaloneTableRecord(.lite, "default", .{});
        const ranges = try antfly.public_api.tables.deriveInitialRanges(self.alloc, table);
        defer {
            for (ranges) |record| antfly.metadata.table_manager.freeRange(self.alloc, record);
            self.alloc.free(ranges);
        }
        if (ranges.len != 1) return error.InvalidCreateTableRequest;
        const namespace = try std.fmt.allocPrint(self.alloc, "group-{d}/table-db", .{ranges[0].group_id});
        defer self.alloc.free(namespace);

        // The durable alias is published first. If catalog publication fails,
        // startup fails and the next attempt safely retries the idempotent
        // catalog adoption; it can never publish a table that points at an
        // empty namespace.
        try backend.adoptEmbeddedRootAsNamespace(namespace);

        // Embedded Lite is created with the deterministic identity of the
        // future standalone `default` table. Verify that invariant before
        // publishing metadata; never perform an O(live documents) identity
        // rewrite during startup or silently accept a mismatched artifact.
        const target_identity = antfly.db.DocIdentityNamespace{
            .table_id = table.table_id,
            .shard_id = ranges[0].group_id,
            .range_id = ranges[0].range_id,
        };
        try verifyAdoptedLiteIdentity(self.alloc, backend, namespace, target_identity);

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.findTableByNameLocked("default") != null) return;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(table);
        for (ranges) |range| try self.manager.upsertRange(range);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn replaceTableDefinition(ptr: *anyopaque, expected: antfly.metadata.TableRecord, replacement: antfly.metadata.TableRecord) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = self.findTableByNameLocked(replacement.name) orelse return error.TableNotFound;
        if (!antfly.metadata.table_manager.tableDefinitionsEqual(current.*, expected) or replacement.table_id != expected.table_id) return error.TableGenerationChanged;
        const previous = try antfly.metadata.table_manager.cloneTable(self.alloc, current.*);
        defer antfly.metadata.table_manager.freeTable(self.alloc, previous);
        const previous_epoch = self.epoch;

        try self.manager.upsertTable(replacement);
        self.epoch +|= 1;
        self.persistLocked() catch |err| {
            self.manager.upsertTable(previous) catch |rollback_err| {
                std.log.err("local metadata table definition rollback failed table={s} err={s}", .{ replacement.name, @errorName(rollback_err) });
            };
            self.epoch = previous_epoch;
            return err;
        };
    }

    fn restoreTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        location_uri: []const u8,
        connection: []const u8,
        artifact_backup_id: []const u8,
        manifest: *const antfly.public_api.backups.TableBackupManifest,
    ) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        try antfly.public_api.backups.validateTableManifest(alloc, manifest, manifest.backup_id);
        if (!std.mem.eql(u8, manifest.table_name, table_name)) return error.InvalidBackupRequest;
        var table = try antfly.public_api.backups.deriveRestoreTableRecord(alloc, table_name, location_uri, manifest);
        defer antfly.metadata.table_manager.freeTable(alloc, table);
        const ranges = try antfly.public_api.backups.deriveRestoreRanges(
            alloc,
            table.table_id,
            location_uri,
            connection,
            artifact_backup_id,
            manifest,
        );
        defer {
            for (ranges) |record| antfly.metadata.table_manager.freeRange(alloc, record);
            alloc.free(ranges);
        }
        if (self.storage_engine == .lite and ranges.len != 1) return error.InvalidBackupRequest;
        table.desired_replica_count = 1;

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.findTableByNameLocked(table_name) != null) return error.TableAlreadyExists;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(table);
        for (ranges) |range| try self.manager.upsertRange(range);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn dropTable(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        _ = self.manager.removeTableTopology(table.table_id);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn updateSchema(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
        _ = try updateSchemaVersioned(ptr, alloc, table_name, schema_json);
    }

    fn updateSchemaVersioned(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !u32 {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        const updated = try antfly.public_api.tables.applySchemaUpdateRecord(alloc, table, schema_json);
        defer antfly.metadata.table_manager.freeTable(alloc, updated);
        const version = try antfly.public_api.tables.schemaVersion(updated.schema_json);
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try mutation.commit(self);
        return version;
    }

    fn createIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        var updated = table.*;
        updated.indexes_json = try antfly.public_api.indexes.addIndexToTableIndexesJson(alloc, table.indexes_json, index_name, index_json);
        defer alloc.free(updated.indexes_json);
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn dropIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        const indexes_json = (try antfly.public_api.indexes.removeIndexFromTableIndexesJson(alloc, table.indexes_json, index_name)) orelse return error.IndexNotFound;
        defer alloc.free(indexes_json);
        var updated = table.*;
        updated.indexes_json = indexes_json;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn putArtifactEnrichment(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, artifact_name: []const u8, enrichment_json: []const u8) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        var updated = table.*;
        updated.indexes_json = try antfly.public_api.indexes.addEnrichmentToTableIndexesJson(alloc, table.indexes_json, artifact_name, enrichment_json);
        defer alloc.free(updated.indexes_json);
        try antfly.public_api.indexes.validateArtifactEnrichmentsForTableIndexesJson(alloc, updated.indexes_json);
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn deleteArtifactEnrichment(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, artifact_name: []const u8) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        const indexes_json = (try antfly.public_api.indexes.removeEnrichmentFromTableIndexesJson(alloc, table.indexes_json, artifact_name)) orelse return error.EnrichmentNotFound;
        defer alloc.free(indexes_json);
        try antfly.public_api.indexes.validateArtifactEnrichmentsForTableIndexesJson(alloc, indexes_json);
        var updated = table.*;
        updated.indexes_json = indexes_json;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn waitTableLifecycle(_: *anyopaque, _: []const u8, _: antfly.public_api.http_server.TableVisibility) !void {}

    fn waitTableProjection(ptr: *anyopaque, table_name: []const u8, schema_json: ?[]const u8, indexes_json: ?[]const u8) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableVisibilityTimeout;
        if (schema_json) |expected| {
            if (!std.mem.eql(u8, table.schema_json, expected)) return error.TableVisibilityTimeout;
        }
        if (indexes_json) |expected| {
            if (!std.mem.eql(u8, table.indexes_json, expected)) return error.TableVisibilityTimeout;
        }
    }

    fn runRound(ptr: *anyopaque) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        self.finalizeReadySchemaMigrations() catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LsmRootWriterAlreadyOpen, error.LmdbUnexpected, error.Corrupted => {},
            else => return err,
        };
    }

    fn installExtension(ptr: *anyopaque, alloc: std.mem.Allocator, extension_name: []const u8, req: antfly.extensions.InstallExtensionRequest) !antfly.extensions.InstalledExtension {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const installed_at_ms: i64 = @intCast(@divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms));
        var persisted_req = req;
        persisted_req.dry_run = false;
        if (req.dry_run) {
            var catalog = try self.cloneExtensionCatalogLocked();
            defer catalog.deinit();
            var planned = try catalog.installManifestOnly(extension_name, extension_name, persisted_req, installed_at_ms);
            defer planned.deinitOwned(self.alloc);
            return try antfly.extensions.cloneInstalledExtensionAlloc(alloc, planned);
        }
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        var installed = try self.extension_catalog.installManifestOnly(extension_name, extension_name, persisted_req, installed_at_ms);
        defer installed.deinitOwned(self.alloc);
        self.epoch +|= 1;
        try mutation.commit(self);
        return try self.extension_catalog.getInstalledAlloc(alloc, extension_name);
    }

    fn updateExtension(ptr: *anyopaque, alloc: std.mem.Allocator, extension_name: []const u8, req: antfly.extensions.UpdateExtensionRequest) !antfly.extensions.InstalledExtension {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var persisted_req = req;
        persisted_req.dry_run = false;
        if (req.dry_run) {
            var catalog = try self.cloneExtensionCatalogLocked();
            defer catalog.deinit();
            var planned = try catalog.updateManifestOnly(extension_name, persisted_req);
            defer planned.deinitOwned(self.alloc);
            return try antfly.extensions.cloneInstalledExtensionAlloc(alloc, planned);
        }
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        var installed = try self.extension_catalog.updateManifestOnly(extension_name, persisted_req);
        defer installed.deinitOwned(self.alloc);
        self.epoch +|= 1;
        try mutation.commit(self);
        return try self.extension_catalog.getInstalledAlloc(alloc, extension_name);
    }

    fn dropExtension(ptr: *anyopaque, _: std.mem.Allocator, extension_name: []const u8, req: antfly.extensions.DropExtensionRequest) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var persisted_req = req;
        persisted_req.dry_run = false;
        if (req.dry_run) {
            var catalog = try self.cloneExtensionCatalogLocked();
            defer catalog.deinit();
            return try catalog.dropInstalledWithMode(extension_name, persisted_req);
        }
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.extension_catalog.dropInstalledWithMode(extension_name, persisted_req);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn enableExtension(ptr: *anyopaque, alloc: std.mem.Allocator, extension_name: []const u8) !antfly.extensions.InstalledExtension {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.extension_catalog.enableInstalled(extension_name);
        self.epoch +|= 1;
        try mutation.commit(self);
        return try self.extension_catalog.getInstalledAlloc(alloc, extension_name);
    }

    fn disableExtension(ptr: *anyopaque, alloc: std.mem.Allocator, extension_name: []const u8) !antfly.extensions.InstalledExtension {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.extension_catalog.disableInstalled(extension_name);
        self.epoch +|= 1;
        try mutation.commit(self);
        return try self.extension_catalog.getInstalledAlloc(alloc, extension_name);
    }

    fn configureExtension(ptr: *anyopaque, alloc: std.mem.Allocator, extension_name: []const u8, req: antfly.extensions.ConfigureExtensionRequest) !antfly.extensions.InstalledExtension {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.extension_catalog.configureInstalled(extension_name, req);
        self.epoch +|= 1;
        try mutation.commit(self);
        return try self.extension_catalog.getInstalledAlloc(alloc, extension_name);
    }

    fn restoreExtensions(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        installed: []const antfly.extensions.InstalledExtension,
        members: []const antfly.extensions.ExtensionMember,
        dependencies: []const antfly.extensions.ExtensionDependency,
    ) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        if (installed.len == 0 and members.len == 0 and dependencies.len == 0) return;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        for (installed) |extension| try self.extension_catalog.upsertInstalled(extension);
        for (members) |member| try self.extension_catalog.upsertMember(member);
        for (dependencies) |dependency| try self.extension_catalog.upsertDependency(dependency);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn cloneExtensionCatalogLocked(self: *LocalStandaloneMetadata) !antfly.extensions.ExtensionCatalog {
        var catalog = antfly.extensions.ExtensionCatalog.init(self.alloc);
        errdefer catalog.deinit();
        try catalog.loadProjectedRows(
            self.extension_catalog.packages.items,
            self.extension_catalog.installed.items,
            self.extension_catalog.members.items,
            self.extension_catalog.dependencies.items,
        );
        return catalog;
    }

    fn syncExtensionPackageStore(self: *LocalStandaloneMetadata, io: std.Io, root_path: []const u8) !usize {
        const entries = try antfly.extensions.scanPackageStoreAlloc(self.alloc, io, root_path);
        defer antfly.extensions.freePackageStoreEntries(self.alloc, entries);

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var mutation = if (entries.len > 0) try self.beginCatalogMutationLocked() else null;
        defer if (mutation) |*active| active.deinit(self);
        for (entries) |entry| try self.extension_catalog.registerPackage(entry.manifest);
        if (entries.len > 0) {
            self.epoch +|= 1;
            try mutation.?.commit(self);
        }
        return entries.len;
    }

    fn finalizeReadySchemaMigrations(self: *LocalStandaloneMetadata) !void {
        const now_ms = monotonicMs();
        const snapshot = blk: {
            lockAtomic(&self.mutex);
            defer self.mutex.unlock();
            var active_migration = false;
            var table_it = self.manager.tables.valueIterator();
            while (table_it.next()) |table| {
                if (table.read_schema_json.len > 0) {
                    active_migration = true;
                    break;
                }
            }
            if (!active_migration) return;
            if (now_ms -| self.last_schema_migration_finalize_at_ms < local_schema_migration_finalize_interval_ms) return;
            self.last_schema_migration_finalize_at_ms = now_ms;

            const tables = try self.manager.listTables(self.alloc);
            errdefer self.manager.freeTables(self.alloc, tables);
            const ranges = try self.manager.listRanges(self.alloc);
            break :blk .{ .tables = tables, .ranges = ranges };
        };
        defer self.manager.freeRanges(self.alloc, snapshot.ranges);
        defer self.manager.freeTables(self.alloc, snapshot.tables);

        const hosted_group_ids = try self.alloc.alloc(u64, snapshot.ranges.len);
        defer self.alloc.free(hosted_group_ids);
        for (snapshot.ranges, 0..) |range, i| hosted_group_ids[i] = range.group_id;

        var runtime_progress: ?antfly.data.runtime.DataServer.LocalSchemaProgressSnapshot = null;
        defer if (runtime_progress) |*progress| progress.deinit(self.alloc);
        var filesystem_progress: ?[]antfly.metadata.SchemaProgressRecord = null;
        defer if (filesystem_progress) |progress| self.alloc.free(progress);
        const progress: []const antfly.metadata.SchemaProgressRecord = progress: {
            if (self.local_schema_progress_provider) |provider| {
                runtime_progress = try provider.collect(provider.ptr, self.alloc, snapshot.tables, snapshot.ranges);
                if (runtime_progress.?.records.len != 0) break :progress runtime_progress.?.records;
                // A complete runtime observation is authoritative even while
                // not ready. Do not contend with its live writer by reopening
                // the same root through the filesystem fallback.
                if (runtime_progress.?.runtime_coverage_complete) return;
            }
            filesystem_progress = try antfly.metadata.table_provisioner.collectLocalSchemaProgressWithOptions(
                self.alloc,
                self.replica_root_dir,
                group_ids.main_metadata_group_id,
                self.local_node_id,
                hosted_group_ids,
                snapshot.tables,
                snapshot.ranges,
                .{
                    .backend_runtime = self.backend_runtime,
                },
            );
            break :progress filesystem_progress.?;
        };
        if (progress.len == 0) return;

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        var changed = false;
        for (progress) |record| {
            const table = self.manager.tables.get(record.table_id) orelse continue;
            if (table.read_schema_json.len == 0) continue;

            const target_version = try localSchemaVersion(self.alloc, table.schema_json);
            if (record.schema_version != target_version) continue;

            var updated = try antfly.metadata.table_manager.cloneTable(self.alloc, table);
            defer antfly.metadata.table_manager.freeTable(self.alloc, updated);

            const read_version = try localSchemaVersion(self.alloc, updated.read_schema_json);
            if (read_version != target_version) {
                const next_indexes_json = try dropFullTextIndexForVersion(self.alloc, updated.indexes_json, read_version);
                self.alloc.free(updated.indexes_json);
                updated.indexes_json = next_indexes_json;
            }
            self.alloc.free(updated.read_schema_json);
            updated.read_schema_json = try self.alloc.dupe(u8, "");

            try self.manager.upsertTable(updated);
            changed = true;
        }

        if (changed) {
            self.epoch +|= 1;
            try mutation.commit(self);
        } else {
            mutation.committed = true;
        }
    }

    fn findTableByNameLocked(self: *LocalStandaloneMetadata, table_name: []const u8) ?*const antfly.metadata.TableRecord {
        var it = self.manager.tables.valueIterator();
        while (it.next()) |table| {
            if (std.mem.eql(u8, table.name, table_name)) return table;
        }
        return null;
    }

    fn loadPersistedCatalog(self: *LocalStandaloneMetadata) !void {
        const raw = if (self.catalog_store) |store| blk: {
            var txn = try store.beginRead();
            defer txn.abort();
            const value = txn.get("catalog") catch |err| switch (err) {
                error.NotFound => return,
                else => return err,
            };
            break :blk try self.alloc.dupe(u8, value);
        } else readFileAlloc(self.alloc, self.catalog_path, 64 * 1024 * 1024) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer self.alloc.free(raw);

        var parsed = try std.json.parseFromSlice(PersistedCatalog, self.alloc, raw, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();

        _ = try self.manager.replaceProjectedTopology(parsed.value.tables, parsed.value.ranges);
        try self.extension_catalog.loadProjectedRows(
            parsed.value.extension_packages,
            parsed.value.installed_extensions,
            parsed.value.extension_members,
            parsed.value.extension_dependencies,
        );
        self.epoch = @max(parsed.value.epoch, 1);
    }

    fn persistLocked(self: *LocalStandaloneMetadata) !void {
        const tables = try self.manager.listTables(self.alloc);
        defer self.manager.freeTables(self.alloc, tables);
        const ranges = try self.manager.listRanges(self.alloc);
        defer self.manager.freeRanges(self.alloc, ranges);
        const extension_packages = try self.extension_catalog.listPackages(self.alloc);
        defer self.extension_catalog.freePackages(self.alloc, extension_packages);
        const installed_extensions = try self.extension_catalog.listInstalled(self.alloc);
        defer self.extension_catalog.freeInstalled(self.alloc, installed_extensions);
        const extension_members = try self.extension_catalog.listMembers(self.alloc);
        defer self.extension_catalog.freeMembers(self.alloc, extension_members);
        const extension_dependencies = try self.extension_catalog.listDependencies(self.alloc);
        defer self.extension_catalog.freeDependencies(self.alloc, extension_dependencies);

        const encoded = try std.json.Stringify.valueAlloc(self.alloc, PersistedCatalog{
            .epoch = self.epoch,
            .tables = tables,
            .ranges = ranges,
            .extension_packages = extension_packages,
            .installed_extensions = installed_extensions,
            .extension_members = extension_members,
            .extension_dependencies = extension_dependencies,
        }, .{ .emit_null_optional_fields = false });
        defer self.alloc.free(encoded);

        if (self.catalog_store) |store| {
            var txn = try store.beginWrite();
            errdefer txn.abort();
            try txn.put("catalog", encoded);
            try txn.commit();
            try store.sync(true);
        } else {
            try writeFileAtomically(self.alloc, self.catalog_path, encoded);
        }
    }
};

fn verifyAdoptedLiteIdentity(
    alloc: std.mem.Allocator,
    backend: *antfly.lite.backend.Handle,
    namespace: []const u8,
    target_identity: antfly.db.DocIdentityNamespace,
) !void {
    if (!target_identity.eql(antfly.lite.connection.embeddedRootIdentity())) {
        return error.InvalidEmbeddedLiteIdentity;
    }
    var db_opts = antfly.db.OpenOptions{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
        .identity_namespace = target_identity,
    };
    try backend.configureDbOpenOptionsForNamespace(&db_opts, namespace);
    var adopted_db = try antfly.db.DB.open(alloc, namespace, db_opts);
    defer adopted_db.close();
    if (!adopted_db.core.identity_namespace.eql(target_identity)) return error.InvalidEmbeddedLiteIdentity;
}

fn deriveStandaloneTableRecord(
    storage_engine: antfly.common.config.StorageEngine,
    table_name: []const u8,
    req: antfly.public_api.tables.CreateTableRequest,
) !antfly.metadata.TableRecord {
    if (storage_engine == .lite and (req.num_shards orelse 1) != 1) {
        return error.InvalidCreateTableRequest;
    }
    var table = antfly.public_api.tables.deriveTableRecord(table_name, req);
    // A standalone process owns the only replica regardless of whether its
    // local persistence is directory-backed or Lite single-file storage.
    table.desired_replica_count = 1;
    return table;
}

fn localSchemaVersion(alloc: std.mem.Allocator, schema_json: []const u8) !u32 {
    if (schema_json.len == 0) return 0;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableSchema,
    };
    const version_value = object.get("version") orelse return 0;
    return switch (version_value) {
        .integer => |value| blk: {
            if (value < 0) return error.InvalidTableSchema;
            break :blk std.math.cast(u32, value) orelse return error.InvalidTableSchema;
        },
        else => return error.InvalidTableSchema,
    };
}

fn dropFullTextIndexForVersion(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    version: u32,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var versioned_name_buf: [64]u8 = undefined;
    const stale_name = if (version == 0)
        antfly.public_api.tables.default_full_text_index_name
    else
        try std.fmt.bufPrint(&versioned_name_buf, "full_text_index_v{d}", .{version});
    _ = object.swapRemove(stale_name);
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
}

fn monotonicMs() u64 {
    return @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
}

pub fn run(init: std.process.Init) !void {
    const alloc = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly_standalone";
    return try runFromIterator(init, argv0, &args);
}

pub fn runFromIterator(
    init: std.process.Init,
    _: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    const alloc = init.gpa;
    var cli = try parseCli(alloc, args);
    defer cli.deinit(alloc);
    if (cli.help) {
        printUsage();
        return;
    }

    var termination_signals = TerminationSignalScope.install();
    defer termination_signals.deinit();

    var secret_store: antfly.common.secrets.FileStore = undefined;
    var secret_store_initialized = false;
    defer if (secret_store_initialized) secret_store.deinit();

    if (cli.secret_store_paths.items.len > 0) {
        secret_store = try initLayeredSecretStore(alloc, cli.secret_store_paths.items);
        secret_store_initialized = true;
    }

    var loaded_config: ?antfly.common.config.Config = if (cli.config_path) |config_path|
        try antfly.common.config.loadFromPathWithSecretsForDeployment(
            alloc,
            config_path,
            if (secret_store_initialized) &secret_store else null,
            .standalone,
        )
    else
        null;
    defer if (loaded_config) |*cfg| cfg.deinit();

    var remote_content_runtime: antfly.common.remote_content_runtime.Runtime = undefined;
    var remote_content_runtime_initialized = false;
    defer if (remote_content_runtime_initialized) remote_content_runtime.deinit();
    var remote_content_facade = antfly.common.config.Config.RemoteContentConfig{};
    const remote_content = if (cli.config_path) |config_path| blk: {
        remote_content_runtime = try antfly.common.remote_content_runtime.Runtime.init(
            alloc,
            config_path,
            if (secret_store_initialized) &secret_store else null,
            .standalone,
        );
        remote_content_runtime_initialized = true;
        remote_content_runtime.attach(&remote_content_facade);
        break :blk &remote_content_facade;
    } else if (loaded_config) |*cfg|
        if (cfg.remote_content) |*configured| configured else null
    else
        null;

    const storage_engine = cli.storage_engine orelse if (loaded_config) |*cfg| cfg.storage.engine else .local;
    if (storage_engine == .object) return error.UnsupportedStandaloneStorageEngine;
    const lite_path = if (storage_engine == .lite)
        (cli.storage_path orelse if (loaded_config) |*cfg| cfg.storage.lite_path else null) orelse return error.MissingLiteStoragePath
    else
        null;
    const lite_fsync = cli.storage_fsync orelse if (loaded_config) |*cfg| cfg.storage.lite_fsync else true;
    try validateEffectiveStandaloneStorage(cli, storage_engine, lite_path, if (loaded_config) |*cfg| cfg else null);
    if (loaded_config) |*cfg| cfg.deployment_mode = .standalone;

    const data_dir = try resolveLocalBaseDir(alloc, cli, if (loaded_config) |*cfg| cfg else null);
    defer alloc.free(data_dir);
    if (storage_engine == .local) try antfly.common.data_format.ensureCompatible(alloc, data_dir);

    const resolved = try resolvePaths(alloc, cli, if (loaded_config) |*cfg| cfg else null);
    defer resolved.deinit(alloc);

    var setup_io = std.Io.Threaded.init(alloc, .{});
    defer setup_io.deinit();
    try ensureDirPath(setup_io.io(), resolved.replica_root_dir);
    try ensureParent(setup_io.io(), resolved.replica_catalog_path);
    try ensureParent(setup_io.io(), resolved.local_metadata_catalog_path);
    try ensureDirPath(setup_io.io(), resolved.snapshot_root_dir);
    try ensureParent(setup_io.io(), resolved.secret_store_path);
    try ensureDirPath(setup_io.io(), resolved.auth_store_root_dir);

    var node_backend_runtime = try antfly.db.background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer node_backend_runtime.deinit();
    var lite_backend: ?antfly.lite.backend.Handle = if (lite_path) |path|
        try antfly.lite.backend.Handle.openOrCreate(alloc, path, .{ .no_sync = !lite_fsync })
    else
        null;
    defer if (lite_backend) |*backend| backend.deinit();
    if (lite_backend) |*backend| {
        node_backend_runtime.ptr().db_open_configurator = backend.dbOpenConfigurator();
    }
    var storage_maintenance = try antfly.storage_maintenance.Coordinator.init(
        alloc,
        if (lite_backend) |*backend| backend.maintenanceSource() else antfly.storage_maintenance.localSource,
        node_backend_runtime.ptr(),
    );
    defer storage_maintenance.deinit();

    // Standalone always owns a local Antfly node. Antfly-managed embeddings use it
    // directly, and the public Antfly routes are registered on the unified
    // server for compatibility with external clients.
    var resolved_warm_models = try resolveInferenceWarmModels(alloc, cli, if (loaded_config) |*cfg| cfg else null);
    defer resolved_warm_models.deinit(alloc);
    var antfly_node_cfg = inference.server.NodeConfig{
        .models_dir = resolveInferenceModelsDir(cli, if (loaded_config) |*cfg| cfg else null) orelse
            antfly.inference_runtime.defaultModelsDirForDataDir(alloc, data_dir),
        .ml_dir = resolveInferenceMlDir(cli, if (loaded_config) |*cfg| cfg else null) orelse
            antfly.inference_runtime.defaultMlDirForDataDir(alloc, data_dir),
        .generation_budget_overrides = resolveInferenceBudgetOverrides(cli),
        .preload = resolved_warm_models.items,
    };
    if (loaded_config) |*cfg| {
        if (cfg.effectiveAntflyContentSecurity()) |security| antfly_node_cfg.content_security = security.*;
        if (cfg.inference.s3_credentials) |creds| antfly_node_cfg.s3_credentials = creds;
        if (cfg.inference.keep_alive) |value|
            antfly_node_cfg.keep_alive_ms = try parseInferenceKeepAliveMs(value);
        if (cfg.inference.max_loaded_models) |value|
            antfly_node_cfg.max_loaded_models =
                std.math.cast(usize, value) orelse return error.InvalidInferenceModelCacheConfig;
    }
    var antfly_node = try inference.server.Node.init(alloc, antfly_node_cfg);
    // Until DataServer exists, error cleanup is owned here. Once its
    // ResourceManager is attached below, the regular defer is registered
    // after DataServer's so tokenizer budget callbacks are torn down first.
    var antfly_node_needs_errdeinit = true;
    errdefer if (antfly_node_needs_errdeinit) antfly_node.deinit();

    var active_audio_runtime = try antfly.common.audio_runtime.ActiveRuntime.init(
        alloc,
        init.io,
        if (loaded_config) |*cfg| cfg else null,
    );
    defer active_audio_runtime.deinit();

    if (!secret_store_initialized) {
        secret_store = try antfly.common.secrets.FileStore.init(alloc, resolved.secret_store_path);
        secret_store_initialized = true;
    }

    const auth_enabled = resolveAuthEnabled(cli, if (loaded_config) |*cfg| cfg else null);
    var auth_backend: ?antfly.lsm_backend.BackendHandle = null;
    var auth_runtime: ?antfly.storage_backend_erased.NamespaceStore = null;
    var auth_user_store: ?antfly.usermgr.StorageUserStore = null;
    var auth_casbin_store: ?antfly.usermgr.StorageCasbinAdapter = null;
    var user_manager: ?antfly.usermgr.UserManager = null;
    if (auth_enabled) {
        auth_backend = try antfly.lsm_backend.BackendHandle.open(alloc, resolved.auth_store_root_dir, .{});
        errdefer if (auth_backend) |*backend| backend.close();
        auth_runtime = try auth_backend.?.backend.runtimeNamespaceStore(alloc);
        errdefer if (auth_runtime) |*runtime| runtime.deinit();
        auth_user_store = antfly.usermgr.StorageUserStore.init(alloc, auth_runtime.?);
        auth_casbin_store = antfly.usermgr.StorageCasbinAdapter.init(alloc, auth_runtime.?);
        user_manager = try antfly.usermgr.UserManager.init(
            alloc,
            auth_user_store.?.iface(),
            try antfly.usermgr.initDefaultEnforcer(alloc, auth_casbin_store.?.iface()),
        );
        errdefer if (user_manager) |*manager| manager.deinit();
        // This seeds only the local auth store and must remain auth-gated.
        // Raft-backed metadata writes during metadata bootstrap can block
        // clustered startup before raft listeners are running.
        try antfly.usermgr.ensureDefaultAdminUser(&user_manager.?);
    }
    defer if (user_manager) |*manager| manager.deinit();
    defer if (auth_runtime) |*runtime| runtime.deinit();
    defer if (auth_backend) |*backend| backend.close();

    const public_listener = resolvePublicListener(cli);
    const local_node_id = cli.local_node_id orelse 1;
    const public_api_url = try std.fmt.allocPrint(
        alloc,
        "http://{s}:{d}",
        .{ public_listener.bind_host, public_listener.bind_port },
    );
    defer alloc.free(public_api_url);

    var local_metadata = LocalStandaloneMetadata.init(
        alloc,
        local_node_id,
        1,
        public_api_url,
        resolved.replica_root_dir,
        resolved.local_metadata_catalog_path,
        node_backend_runtime.ptr(),
        if (lite_backend) |*backend| try backend.runtimeStoreForNamespace("system/metadata") else null,
        storage_engine,
    ) catch |err| {
        std.log.err("standalone startup failed step=local_metadata_init err={}", .{err});
        return err;
    };
    defer local_metadata.deinit();
    if (lite_backend) |*backend| {
        try local_metadata.adoptEmbeddedLiteRootIfNeeded(backend);
        // Mark only after embedded adoption and metadata publication succeed.
        // Offline root-only backup must reject every standalone artifact, not
        // merely artifacts that originated in the embedded profile.
        try backend.markStandaloneArtifact();
    }
    // API transaction sessions are engine state, not a sidecar. Keeping them
    // in a reserved Lite namespace makes a copied/reopened .aflite file a
    // complete database and preserves staged multi-request transactions.
    var lite_session_store = if (lite_backend) |*backend|
        antfly.public_api.transactions.DurableSessionStore.initRuntime(
            alloc,
            try backend.runtimeStoreForNamespace("system/api-transaction-sessions"),
        )
    else
        null;
    const synced_extension_packages = local_metadata.syncExtensionPackageStore(setup_io.io(), resolved.extension_package_store_dir) catch |err| {
        std.log.err("standalone startup failed step=sync_extension_packages err={}", .{err});
        return err;
    };
    if (synced_extension_packages > 0) {
        std.log.info("standalone synced extension package store path={s} packages={d}", .{ resolved.extension_package_store_dir, synced_extension_packages });
    }

    try validateHARole(cli);
    try validateHAPathsUnderRoot(cli, data_dir);
    var ha_sync_policy = try haSyncPolicyFromCli(alloc, cli);
    defer ha_sync_policy.deinit(alloc);
    const ha_retention_policy = try haRetentionPolicyFromCli(cli);
    var ha_primary = openHAPrimaryFromCli(alloc, setup_io.io(), cli) catch |err| {
        std.log.err("standalone startup failed step=open_ha_primary err={}", .{err});
        return err;
    };
    defer if (ha_primary) |*primary| primary.close();
    var ha_standby = openHAStandbyFromCli(alloc, setup_io.io(), cli) catch |err| {
        std.log.err("standalone startup failed step=open_ha_standby err={}", .{err});
        return err;
    };
    defer if (ha_standby) |*standby| standby.close();
    var ha_fence_store = openHAFenceStoreFromCli(alloc, setup_io.io(), cli) catch |err| {
        std.log.err("standalone startup failed step=open_ha_fence err={}", .{err});
        return err;
    };
    defer if (ha_fence_store) |*store| store.close();
    var ha_former_primary_log = openHAFormerPrimaryLogFromCli(alloc, setup_io.io(), cli) catch |err| {
        std.log.err("standalone startup failed step=open_ha_former_primary err={}", .{err});
        return err;
    };
    defer if (ha_former_primary_log) |*log| log.close();
    const admin_bearer_token = try resolveAdminBearerTokenFromCli(alloc, cli);
    defer if (admin_bearer_token) |token| alloc.free(token);

    // Initialize DataServer without starting its listener — the unified
    // httpx.Server will serve the public API instead.
    var data_server = antfly.data.runtime.DataServer.initFromLocalMetadataSources(alloc, .{
        .bind_host = public_listener.bind_host,
        .bind_port = public_listener.bind_port,
        .enable_data_raft = false,
        .replica_root_dir = resolved.replica_root_dir,
        .replica_catalog_path = resolved.replica_catalog_path,
        .snapshot_root_dir = resolved.snapshot_root_dir,
        .store_registration = .{
            .node_id = local_node_id,
            .store_id = 1,
            .api_url = public_api_url,
            .role = "data",
        },
        .api_server_cfg = .{
            .auth_enabled = auth_enabled,
            .experimental = cli.experimental,
            .ard_base_url = cli.ard_base_url,
            .ard_publisher_domain = cli.ard_publisher_domain orelse "antfly.local",
            .ard_display_name = cli.ard_display_name orelse "Antfly",
            .ard_public_catalog_enabled = cli.ard_public_catalog_enabled,
            .deployment_mode = .standalone,
            .storage_maintenance = &storage_maintenance,
            .admin_bearer_token = admin_bearer_token,
            .secret_store = &secret_store,
            .remote_content = remote_content,
            .inference_api_key = if (loaded_config) |*cfg| if (cfg.inference.api_key) |value| value else null else null,
            .extension_package_store_dir = resolved.extension_package_store_dir,
            .node_config = if (loaded_config) |*cfg| cfg else null,
            .user_manager = if (user_manager) |*manager| manager else null,
            .session_store = if (lite_session_store) |*store| store else null,
            .session_ttl_ns = if (loaded_config) |*cfg| cfg.transaction_sessions.ttl_seconds * std.time.ns_per_s else standalone_session_ttl_ns,
            .session_cleanup_interval_ns = if (loaded_config) |*cfg| cfg.transaction_sessions.cleanup_interval_seconds * std.time.ns_per_s else standalone_session_cleanup_interval_ns,
            .session_max_count = if (loaded_config) |*cfg| cfg.transaction_sessions.max_count else standalone_session_max_count,
            .session_max_record_bytes = if (loaded_config) |*cfg| cfg.transaction_sessions.max_record_bytes else standalone_session_max_record_bytes,
            .session_savepoint_limit = if (loaded_config) |*cfg| cfg.transaction_sessions.max_savepoints else standalone_session_savepoint_limit,
        },
        .ha = if (ha_primary != null or ha_standby != null or ha_fence_store != null or ha_former_primary_log != null) .{
            .admin_context = .{
                .primary = if (ha_primary) |*primary| primary else null,
                .primary_node_id = cli.ha_primary_node_id,
                .standby = if (ha_standby) |*standby| standby else null,
                .standby_node_id = cli.ha_standby_node_id,
                .fence_store = if (ha_fence_store) |*store| store else null,
                .former_primary_log = if (ha_former_primary_log) |*log| log else null,
            },
            .standby_owner = if (ha_standby != null) &ha_standby else null,
            .admin_bearer_token = admin_bearer_token,
            .internal_primary = if (ha_primary) |*primary| primary else null,
            .primary_retention_policy = ha_retention_policy,
            .primary_sync_policy = ha_sync_policy.policy,
            .standby_replication = try haStandbyReplicationConfigFromCli(cli),
        } else .{},
        .backend_runtime = node_backend_runtime.ptr(),
    }, local_metadata.catalogSource(), local_metadata.statusSource());
    defer data_server.deinit();
    antfly_node_needs_errdeinit = false;
    defer {
        // DataServer sources, recovery workers, and durable API jobs retain the
        // embedded provider. Drain them while the node is valid, then release
        // tokenizer reservations while DataServer's ResourceManager is valid.
        // The earlier data_server.deinit defer performs final storage teardown.
        data_server.quiesceBackgroundWork();
        antfly_node.deinit();
    }

    antfly_node.configureAdmissionResourceBudget(inferenceAdmissionResourceBudget(
        &data_server.provisioned_storage.resource_manager,
    ));
    antfly_node.config.prompt_cache_resource_usage_observer = promptCacheResourceUsageObserver(&data_server.provisioned_storage.resource_manager);
    try antfly_node.configureTokenizerCaches(.{
        // Keep the small lock-free front table hot while providing enough
        // second-tier slots for large ingestion corpora. The table and every
        // admitted entry are charged to inference.tokenizer_cache; pressure
        // can decline the optional table without making model warmup fail.
        .bulk_slots_per_shard = 16 * 1024,
        .resource_budget = tokenizerCacheResourceBudget(
            &data_server.provisioned_storage.resource_manager,
        ),
    });
    if (node_backend_runtime.ptr().io()) |io| antfly_node.attachIo(io);
    antfly_node.warmConfiguredModels(alloc) catch |err| {
        std.log.err("standalone startup failed step=warm_inference_models err={}", .{err});
        return err;
    };
    data_server.setAntflyProvider(localAntflyProvider(&antfly_node));

    // Initialize API server (wires caches + sources) without binding a listener.
    try data_server.initApiServer();
    local_metadata.local_schema_progress_provider = localSchemaProgressProvider(&data_server);
    const api_server = &data_server.http_server.?;
    if (lite_backend) |*backend| {
        try api_server.attachRestoreJobRuntimeStore(try backend.runtimeStoreForNamespace("system/api-restore-jobs"));
    }
    // Recovery is a startup concern: enqueue durable work before the listener is
    // marked ready instead of waiting for an unrelated request to arrive.
    try api_server.resumeRestoreJobsOnce();
    data_server.registerNodeIfConfigured() catch |err| {
        std.log.err("standalone startup failed step=register_node err={}", .{err});
        return err;
    };
    data_server.requestProvisionedStartupCatchUpNow() catch |err| {
        std.log.warn("standalone startup provisioned startup catch-up skipped err={}", .{err});
    };
    data_server.requestProvisionedCacheWarmup() catch |err| {
        std.log.warn("standalone startup provisioned cache warmup skipped err={}", .{err});
    };

    // ---------------------------------------------------------------
    // Unified httpx.Server — all routes on a single port
    // ---------------------------------------------------------------

    var handler = AntflyApiHandler{ .api_server = api_server };
    try handler.initRuntime(alloc);
    defer handler.deinitRuntime();

    const bind_host = public_listener.bind_host;
    const bind_port = public_listener.bind_port;
    var unified_api_ready = std.atomic.Value(bool).init(false);

    var public_listener_lease = try PublicListenerLease.acquire(alloc, bind_port);
    defer public_listener_lease.deinit();

    var unified_lifecycle = UnifiedServerLifecycle{};
    var standalone_health = StandaloneHealthSource{
        .data_server = &data_server,
        .unified_api_ready = &unified_api_ready,
        .handler = &handler,
        .unified_lifecycle = &unified_lifecycle,
    };
    const health_enabled = cli.health_enabled orelse if (loaded_config) |*cfg| cfg.health_enabled else true;
    const health_port = if (health_enabled)
        cli.health_port orelse if (loaded_config) |*cfg| cfg.health_port else antfly.common.config.default_health_port
    else
        null;
    const health_server = antfly.common.health_server.HealthServer.startIfConfiguredOnHost(
        alloc,
        "standalone",
        public_listener.bind_host,
        health_port,
        standalone_health.readiness(),
        standalone_health.metricsWriter(),
    ) catch |err| {
        std.log.err("standalone startup failed step=health_server err={}", .{err});
        return err;
    };
    defer if (health_server) |hs| hs.deinit();

    const public_io = node_backend_runtime.ptr().io() orelse return error.BackendRuntimeUnavailable;
    const thread = std.Thread.spawn(.{}, serveUnified, .{
        alloc,
        public_io,
        bind_host,
        bind_port,
        &handler,
        &antfly_node,
        api_server,
        &unified_api_ready,
        &unified_lifecycle,
    }) catch |err| {
        std.log.err("standalone startup failed step=spawn_unified_http err={}", .{err});
        return err;
    };
    var thread_joined = false;
    defer if (!thread_joined) {
        unified_lifecycle.stop();
        thread.join();
    };
    unified_lifecycle.waitForStartup() catch |err| {
        std.log.err("standalone startup failed step=bind_unified_http err={}", .{err});
        return err;
    };

    // Print only after the public listener has successfully bound.
    std.debug.print("standalone local metadata enabled (raft disabled)\n", .{});

    const runtime_cadence = antfly.raft.RuntimeCadence.fromMillis(
        antfly.raft.RuntimeCadence.default_raft_tick_ms,
        cli.control_tick_ms,
    ) catch return error.InvalidArguments;
    const tick_ms = @divExact(runtime_cadence.control_tick_ns, std.time.ns_per_ms);
    var req = std.posix.timespec{
        .sec = @intCast(tick_ms / std.time.ms_per_s),
        .nsec = @intCast((tick_ms % std.time.ms_per_s) * std.time.ns_per_ms),
    };
    while (!termination_requested.load(.acquire)) {
        if (unified_lifecycle.runtimeFailure()) |err| return err;
        data_server.runRound() catch |err| switch (err) {
            error.LsmRootWriterAlreadyOpen, error.WriterLocked => std.log.warn("standalone data round skipped err={}", .{err}),
            else => return err,
        };
        LocalStandaloneMetadata.runRound(&local_metadata) catch |err| switch (err) {
            error.LsmRootWriterAlreadyOpen, error.WriterLocked => std.log.warn("standalone metadata round skipped err={}", .{err}),
            else => return err,
        };
        const err = std.posix.errno(std.posix.system.nanosleep(&req, &req));
        switch (err) {
            .SUCCESS => {},
            .INTR => continue,
            else => return std.posix.unexpectedErrno(err),
        }
    }

    unified_lifecycle.shutdown(30_000);
    thread.join();
    thread_joined = true;
    if (unified_lifecycle.runtimeFailure()) |err| return err;
}

fn validateEffectiveStandaloneStorage(
    cli: CliConfig,
    storage_engine: antfly.common.config.StorageEngine,
    lite_path: ?[]const u8,
    loaded_config: ?*const antfly.common.config.Config,
) !void {
    if (storage_engine == .object) return error.UnsupportedStandaloneStorageEngine;
    if (storage_engine != .lite) {
        if (cli.storage_path != null or cli.storage_fsync != null) return error.InvalidArguments;
        return;
    }
    const path = lite_path orelse return error.MissingLiteStoragePath;
    if (!std.mem.endsWith(u8, path, ".aflite")) return error.InvalidLiteStoragePath;
    if (loaded_config) |cfg| {
        if (cfg.metadata.orchestration_urls.len != 0 or cfg.metadata.raft_urls.len != 0) {
            return error.LiteExternalMetadataUnsupported;
        }
        if (cfg.shard_allocation.default_shards_per_table != 1 or
            cfg.shard_allocation.min_shards_per_table != 1 or
            !cfg.shard_allocation.disable_shard_alloc)
        {
            return error.LiteHorizontalShardingUnsupported;
        }
    }
}

pub fn runLite(
    init: std.process.Init,
    path: []const u8,
    host: []const u8,
    port: u16,
    fsync: bool,
    extra_args: []const []const u8,
) !void {
    const path_z = try init.gpa.dupeZ(u8, path);
    defer init.gpa.free(path_z);
    const host_z = try init.gpa.dupeZ(u8, host);
    defer init.gpa.free(host_z);
    var port_buf: [16]u8 = undefined;
    const port_z = try std.fmt.bufPrintZ(&port_buf, "{d}", .{port});
    var argv = std.ArrayListUnmanaged([*:0]const u8).empty;
    defer argv.deinit(init.gpa);
    try argv.appendSlice(init.gpa, &.{
        "--storage-engine",
        "lite",
        "--storage-path",
        path_z.ptr,
        "--host",
        host_z.ptr,
        "--port",
        port_z.ptr,
        if (fsync) "--fsync=true" else "--fsync=false",
    });
    const owned_extra = try init.gpa.alloc([:0]u8, extra_args.len);
    var owned_extra_count: usize = 0;
    defer {
        for (owned_extra[0..owned_extra_count]) |value| init.gpa.free(value);
        init.gpa.free(owned_extra);
    }
    for (extra_args, 0..) |arg, i| {
        owned_extra[i] = try init.gpa.dupeZ(u8, arg);
        owned_extra_count += 1;
        try argv.append(init.gpa, owned_extra[i].ptr);
    }
    var args = std.process.Args.Iterator.init(.{ .vector = argv.items });
    try runFromIterator(init, "antfly standalone", &args);
}

fn localAntflyProvider(node: *inference.server.Node) antfly.inference.managed_embedder.AntflyProvider {
    return .{
        .ptr = node,
        .embed_dense_texts = localAntflyEmbedDenseTexts,
        .embed_dense_texts_with_context = localAntflyEmbedDenseTextsWithContext,
        .embed_sparse_texts = localAntflyEmbedSparseTexts,
        .embed_dense_parts = localAntflyEmbedDenseParts,
        .embed_dense_parts_with_context = localAntflyEmbedDensePartsWithContext,
        .rerank_texts = localAntflyRerankTexts,
        .generate_text = localAntflyGenerateText,
        .generate_messages = localAntflyGenerateMessages,
        .read_images = localAntflyReadImages,
        .transcribe_audio = localAntflyTranscribeAudio,
        .extract = localAntflyExtract,
        .list_models_json = localAntflyListModelsJson,
    };
}

fn promptCacheResourceUsageObserver(manager: *antfly.resource_manager.ResourceManager) inference.runtime.kv.prompt_cache.ResourceUsageObserver {
    return .{
        .context = manager,
        .update = observePromptCacheResourceUsage,
    };
}

fn inferenceAdmissionResourceBudget(
    manager: *antfly.resource_manager.ResourceManager,
) inference.runtime.tier.memory.AdmissionResourceBudget {
    return .{
        .context = manager,
        .try_reserve = reserveInferenceAdmissionResources,
        .release = releaseInferenceAdmissionResources,
    };
}

fn inferenceAdmissionSliceAmounts(
    amounts: inference.runtime.tier.memory.AdmissionAmounts,
) ![3]antfly.resource_manager.SliceAmount {
    const model_residency = try std.math.add(
        usize,
        amounts.host_weight_bytes,
        amounts.backend_weight_bytes,
    );
    const kv_working_set = try std.math.add(
        usize,
        amounts.host_kv_bytes,
        amounts.backend_kv_bytes,
    );
    const scratch_working_set = try std.math.add(
        usize,
        amounts.host_scratch_bytes,
        amounts.backend_scratch_bytes,
    );
    return .{
        .{ .slice = .inference_model_residency, .bytes = @intCast(model_residency) },
        .{ .slice = .inference_kv_working_set, .bytes = @intCast(kv_working_set) },
        .{ .slice = .inference_scratch_working_set, .bytes = @intCast(scratch_working_set) },
    };
}

fn reserveInferenceAdmissionResources(
    context: *anyopaque,
    amounts: inference.runtime.tier.memory.AdmissionAmounts,
) inference.runtime.tier.memory.AdmissionResourceError!void {
    const manager: *antfly.resource_manager.ResourceManager = @ptrCast(@alignCast(context));
    const slices = inferenceAdmissionSliceAmounts(amounts) catch
        return error.ResourceLimitExceeded;
    manager.reserveBatchClassified(&slices) catch |err| switch (err) {
        error.ResourceRequestTooLarge => return error.ResourceLimitExceeded,
        error.ResourceTemporarilyUnavailable => return error.ResourceTemporarilyUnavailable,
        // Duplicate slices are impossible in the fixed bridge plan.
        error.DuplicateResourceSlice => unreachable,
    };
}

fn releaseInferenceAdmissionResources(
    context: *anyopaque,
    amounts: inference.runtime.tier.memory.AdmissionAmounts,
) void {
    const manager: *antfly.resource_manager.ResourceManager = @ptrCast(@alignCast(context));
    const slices = inferenceAdmissionSliceAmounts(amounts) catch unreachable;
    manager.releaseBatch(&slices);
}

test "inference admission bridge charges combined native residency to resource manager" {
    var budgets = antfly.resource_manager.Options.defaultBudgets();
    budgets[@intFromEnum(antfly.resource_manager.Slice.inference_model_residency)] =
        .{ .hard_limit_bytes = 100 };
    var manager = antfly.resource_manager.ResourceManager.init(.{ .budgets = budgets });
    const budget = inferenceAdmissionResourceBudget(&manager);

    try std.testing.expectError(
        error.ResourceLimitExceeded,
        budget.try_reserve(budget.context, .{
            .host_weight_bytes = 80,
            .backend_weight_bytes = 30,
        }),
    );
    try std.testing.expectEqual(
        @as(u64, 0),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );

    const admitted = inference.runtime.tier.memory.AdmissionAmounts{
        .host_weight_bytes = 60,
        .backend_weight_bytes = 30,
    };
    try budget.try_reserve(budget.context, admitted);
    try std.testing.expectEqual(
        @as(u64, 90),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        budget.try_reserve(budget.context, .{
            .host_weight_bytes = 11,
        }),
    );
    budget.release(budget.context, admitted);
    try std.testing.expectEqual(
        @as(u64, 0),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );
}

fn observePromptCacheResourceUsage(context: *anyopaque, current: *u64, next: u64) void {
    const manager: *antfly.resource_manager.ResourceManager = @ptrCast(@alignCast(context));
    manager.observeUsage(.inference_prompt_cache, current, next);
}

fn tokenizerCacheResourceBudget(
    manager: *antfly.resource_manager.ResourceManager,
) inference.hf_tokenizer.HfTokenizer.BpeCacheResourceBudget {
    return .{
        .context = manager,
        .try_reserve = reserveTokenizerCacheBytes,
        .release = releaseTokenizerCacheBytes,
    };
}

fn reserveTokenizerCacheBytes(context: *anyopaque, bytes: usize) bool {
    const manager: *antfly.resource_manager.ResourceManager =
        @ptrCast(@alignCast(context));
    // Cache growth is optional: honor the slice's shrink policy at the soft
    // boundary by declining new entries/workspace retention. reserve() below
    // remains the atomic hard guard if another producer wins the race.
    if (manager.admissionDecision(
        .inference_tokenizer_cache,
        @intCast(bytes),
    ).action == .shrink_cache) return false;
    var reservation = manager.reserve(
        .inference_tokenizer_cache,
        @intCast(bytes),
    ) catch return false;
    // The tokenizer owns the reservation until its entry/cache is released.
    reservation.released = true;
    return true;
}

fn releaseTokenizerCacheBytes(context: *anyopaque, bytes: usize) void {
    const manager: *antfly.resource_manager.ResourceManager =
        @ptrCast(@alignCast(context));
    manager.releaseBytes(.inference_tokenizer_cache, @intCast(bytes));
}

fn localAntflyListModelsJson(ptr: *anyopaque, alloc: std.mem.Allocator) anyerror![]u8 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try node.listModelsJsonAlloc(alloc, io_impl.io());
}

fn localAntflyEmbedDenseTexts(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
) anyerror![][]f32 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.embedDenseTextsDirect(alloc, model, texts);
}

fn localAntflyEmbedDenseTextsWithContext(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
    context: antfly.inference.managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.embedDenseTextsDirectWithContext(alloc, context.io, context.deadline_ns, model, texts);
}

fn localAntflyEmbedDenseParts(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const antfly.template.ContentPart,
) anyerror![][]f32 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    return try localAntflyEmbedDensePartsWithExecutionContext(ptr, alloc, model, parts, io_impl.io(), null);
}

fn localAntflyEmbedDensePartsWithExecutionContext(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const antfly.template.ContentPart,
    io: std.Io,
    deadline_ns: ?u64,
) anyerror![][]f32 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    var values = std.json.Array.init(alloc);
    defer values.deinit();
    var encoded_buffers = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (encoded_buffers.items) |buf| alloc.free(buf);
        encoded_buffers.deinit(alloc);
    }

    for (parts) |part| {
        switch (part) {
            .text => |text| {
                var obj = std.json.ObjectMap.empty;
                errdefer obj.deinit(alloc);
                try obj.put(alloc, "type", .{ .string = "text" });
                try obj.put(alloc, "text", .{ .string = text });
                try values.append(.{ .object = obj });
            },
            .media_url => |url| {
                var image_url = std.json.ObjectMap.empty;
                errdefer image_url.deinit(alloc);
                try image_url.put(alloc, "url", .{ .string = url });

                var obj = std.json.ObjectMap.empty;
                errdefer obj.deinit(alloc);
                try obj.put(alloc, "type", .{ .string = "image_url" });
                try obj.put(alloc, "image_url", .{ .object = image_url });
                try values.append(.{ .object = obj });
            },
            .binary => |binary_part| {
                const encoded_len = std.base64.standard.Encoder.calcSize(binary_part.data.len);
                const encoded = try alloc.alloc(u8, encoded_len);
                errdefer alloc.free(encoded);
                _ = std.base64.standard.Encoder.encode(encoded, binary_part.data);
                try encoded_buffers.append(alloc, encoded);

                var obj = std.json.ObjectMap.empty;
                errdefer {
                    obj.deinit(alloc);
                    _ = encoded_buffers.pop();
                    alloc.free(encoded);
                }
                try obj.put(alloc, "type", .{ .string = "media" });
                try obj.put(alloc, "data", .{ .string = encoded });
                try obj.put(alloc, "mime_type", .{ .string = binary_part.mime_type });
                try values.append(.{ .object = obj });
            },
        }
    }

    return try node.embedDenseJsonInputDirectWithContext(alloc, io, deadline_ns, model, .{ .array = values });
}

fn localAntflyEmbedDensePartsWithContext(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const antfly.template.ContentPart,
    context: antfly.inference.managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    return try localAntflyEmbedDensePartsWithExecutionContext(ptr, alloc, model, parts, context.io, context.deadline_ns);
}

fn localAntflyEmbedSparseTexts(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
) anyerror![]antfly.db.embedder.SparseEmbedding {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    const sparse = try node.embedSparseTextsDirect(alloc, model, texts);
    errdefer {
        for (sparse) |*item| item.deinit(alloc);
        alloc.free(sparse);
    }
    const out = try alloc.alloc(antfly.db.embedder.SparseEmbedding, sparse.len);
    errdefer alloc.free(out);
    for (sparse, 0..) |item, i| {
        out[i] = .{
            .indices = item.indices,
            .values = item.values,
        };
    }
    alloc.free(sparse);
    return out;
}

fn localAntflyRerankTexts(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    query: []const u8,
    documents: []const []const u8,
) anyerror![]f32 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.rerankTextsDirect(alloc, model, query, documents);
}

fn localAntflyGenerateText(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    roles: []const []const u8,
    contents: []const []const u8,
) anyerror![]u8 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.generateTextDirect(alloc, model, roles, contents);
}

fn localAntflyGenerateMessages(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const antfly.inference.ChatMessage,
) anyerror![]u8 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    var converted = try convertLocalGenerateMessages(alloc, messages);
    defer converted.deinit(alloc);
    return try node.generateMessagesDirect(alloc, model, converted.messages);
}

fn localAntflyReadImages(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: antfly.readers.Request,
) anyerror![]antfly.readers.Result {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.readImagesDirect(alloc, model, request);
}

fn localAntflyTranscribeAudio(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: antfly.transcribing.Request,
) anyerror!antfly.transcribing.Response {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.transcribeAudioDirect(alloc, model, request);
}

fn localAntflyExtract(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: antfly.extracting.Request,
) anyerror!antfly.extracting.Response {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.extractDirect(alloc, model, request);
}

const LocalGenerateMessages = struct {
    messages: []inference.pipelines.GenerationMessage,
    owned_texts: std.ArrayListUnmanaged([]u8) = .empty,
    owned_media: std.ArrayListUnmanaged([]u8) = .empty,
    owned_slices: std.ArrayListUnmanaged([]const []const u8) = .empty,
    owned_parts: std.ArrayListUnmanaged([]inference.pipelines.GenerationMessage.ContentPart) = .empty,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.owned_texts.items) |text| alloc.free(text);
        self.owned_texts.deinit(alloc);
        for (self.owned_media.items) |media| alloc.free(media);
        self.owned_media.deinit(alloc);
        for (self.owned_slices.items) |slice| alloc.free(slice);
        self.owned_slices.deinit(alloc);
        for (self.owned_parts.items) |parts| alloc.free(parts);
        self.owned_parts.deinit(alloc);
        alloc.free(self.messages);
        self.* = undefined;
    }
};

fn convertLocalGenerateMessages(
    alloc: std.mem.Allocator,
    messages: []const antfly.inference.ChatMessage,
) !LocalGenerateMessages {
    var out = LocalGenerateMessages{
        .messages = try alloc.alloc(inference.pipelines.GenerationMessage, messages.len),
    };
    errdefer out.deinit(alloc);

    for (messages, 0..) |message, i| {
        out.messages[i] = try convertLocalGenerateMessage(alloc, &out, message);
    }
    return out;
}

fn convertLocalGenerateMessage(
    alloc: std.mem.Allocator,
    owner: *LocalGenerateMessages,
    message: antfly.inference.ChatMessage,
) !inference.pipelines.GenerationMessage {
    const role = message.role.toSlice();
    const content = message.content orelse {
        const text = try alloc.dupe(u8, "");
        var text_owned = true;
        errdefer if (text_owned) alloc.free(text);
        try owner.owned_texts.append(alloc, text);
        text_owned = false;
        return .{ .role = role, .content = text };
    };

    return switch (content) {
        .text => |text_value| blk: {
            const text = try alloc.dupe(u8, text_value);
            var text_owned = true;
            errdefer if (text_owned) alloc.free(text);
            try owner.owned_texts.append(alloc, text);
            text_owned = false;
            break :blk .{ .role = role, .content = text };
        },
        .parts => |parts| try convertLocalGenerateParts(alloc, owner, role, parts),
    };
}

fn convertLocalGenerateParts(
    alloc: std.mem.Allocator,
    owner: *LocalGenerateMessages,
    role: []const u8,
    parts: []const antfly.inference.ContentPart,
) !inference.pipelines.GenerationMessage {
    var text_buf = std.ArrayListUnmanaged(u8).empty;
    errdefer text_buf.deinit(alloc);
    var images = std.ArrayListUnmanaged([]const u8).empty;
    errdefer images.deinit(alloc);
    var audio = std.ArrayListUnmanaged([]const u8).empty;
    errdefer audio.deinit(alloc);
    var out_parts = std.ArrayListUnmanaged(inference.pipelines.GenerationMessage.ContentPart).empty;
    errdefer out_parts.deinit(alloc);

    for (parts) |part| {
        switch (part) {
            .text => |text| {
                const start = text_buf.items.len;
                try text_buf.appendSlice(alloc, text);
                _ = start;
                try out_parts.append(alloc, .{ .text = text });
            },
            .image_url => |image_url| {
                const decoded = try decodeLocalGenerateDataUri(alloc, image_url.url, null);
                var decoded_owned = true;
                errdefer if (decoded_owned) alloc.free(decoded.data);
                if (!std.mem.startsWith(u8, decoded.mime_type, "image/")) {
                    return error.UnsupportedGeneratorProvider;
                }
                try images.append(alloc, decoded.data);
                try out_parts.append(alloc, .{ .image = images.items.len - 1 });
                try owner.owned_media.append(alloc, decoded.data);
                decoded_owned = false;
            },
            .media => |media| {
                const raw = media.url orelse media.data;
                const decoded = try decodeLocalGenerateDataUri(alloc, raw, media.mime_type);
                var decoded_owned = true;
                errdefer if (decoded_owned) alloc.free(decoded.data);
                if (std.mem.startsWith(u8, decoded.mime_type, "image/")) {
                    try images.append(alloc, decoded.data);
                    try out_parts.append(alloc, .{ .image = images.items.len - 1 });
                    try owner.owned_media.append(alloc, decoded.data);
                    decoded_owned = false;
                } else if (std.mem.startsWith(u8, decoded.mime_type, "audio/")) {
                    try audio.append(alloc, decoded.data);
                    try out_parts.append(alloc, .{ .audio = audio.items.len - 1 });
                    try owner.owned_media.append(alloc, decoded.data);
                    decoded_owned = false;
                } else {
                    return error.UnsupportedGeneratorProvider;
                }
            },
        }
    }

    const text = try text_buf.toOwnedSlice(alloc);
    var text_owned = true;
    errdefer if (text_owned) alloc.free(text);
    try owner.owned_texts.append(alloc, text);
    text_owned = false;
    const image_slice = if (images.items.len > 0) blk: {
        const slice = try images.toOwnedSlice(alloc);
        var slice_owned = true;
        errdefer if (slice_owned) alloc.free(slice);
        try owner.owned_slices.append(alloc, slice);
        slice_owned = false;
        break :blk slice;
    } else null;
    const audio_slice = if (audio.items.len > 0) blk: {
        const slice = try audio.toOwnedSlice(alloc);
        var slice_owned = true;
        errdefer if (slice_owned) alloc.free(slice);
        try owner.owned_slices.append(alloc, slice);
        slice_owned = false;
        break :blk slice;
    } else null;
    const content_parts = if (out_parts.items.len > 0) blk: {
        const slice = try out_parts.toOwnedSlice(alloc);
        var slice_owned = true;
        errdefer if (slice_owned) alloc.free(slice);
        try owner.owned_parts.append(alloc, slice);
        slice_owned = false;
        break :blk slice;
    } else null;

    return .{
        .role = role,
        .content = text,
        .image_bytes = image_slice,
        .audio_bytes = audio_slice,
        .content_parts = content_parts,
    };
}

const DecodedLocalMedia = struct {
    data: []u8,
    mime_type: []const u8,
};

fn decodeLocalGenerateDataUri(
    alloc: std.mem.Allocator,
    raw: []const u8,
    declared_mime_type: ?[]const u8,
) !DecodedLocalMedia {
    var mime_type = declared_mime_type orelse "application/octet-stream";
    var payload = raw;
    if (std.mem.startsWith(u8, raw, "data:")) {
        const comma = std.mem.indexOfScalar(u8, raw, ',') orelse return error.UnsupportedGeneratorProvider;
        const meta = raw["data:".len..comma];
        if (!std.mem.endsWith(u8, meta, ";base64")) return error.UnsupportedGeneratorProvider;
        const embedded_mime = meta[0 .. meta.len - ";base64".len];
        if (embedded_mime.len > 0) {
            if (declared_mime_type) |declared| {
                if (!std.mem.eql(u8, declared, embedded_mime)) return error.UnsupportedGeneratorProvider;
            }
            mime_type = embedded_mime;
        }
        payload = raw[comma + 1 ..];
    }

    const decoded_len = try std.base64.standard.Decoder.calcSizeForSlice(payload);
    const decoded = try alloc.alloc(u8, decoded_len);
    errdefer alloc.free(decoded);
    try std.base64.standard.Decoder.decode(decoded, payload);
    return .{ .data = decoded, .mime_type = mime_type };
}

// ---------------------------------------------------------------
// Unified server thread
// ---------------------------------------------------------------

fn serveUnified(
    alloc: std.mem.Allocator,
    io: std.Io,
    bind_host: []const u8,
    bind_port: u16,
    handler: *AntflyApiHandler,
    antfly_node: ?*inference.server.Node,
    api_server: *antfly.public_api.http_server.ApiHttpServer,
    unified_api_ready: *std.atomic.Value(bool),
    lifecycle: *UnifiedServerLifecycle,
) void {
    serveUnifiedInner(alloc, io, bind_host, bind_port, handler, antfly_node, api_server, unified_api_ready, lifecycle) catch |err| {
        unified_api_ready.store(false, .release);
        lifecycle.publishFailure(err);
        std.debug.print("unified server error: {}\n", .{err});
        return;
    };
    unified_api_ready.store(false, .release);
    lifecycle.publishStopped();
}

fn serveUnifiedInner(
    alloc: std.mem.Allocator,
    io: std.Io,
    bind_host: []const u8,
    bind_port: u16,
    handler: *AntflyApiHandler,
    antfly_node: ?*inference.server.Node,
    api_server: *antfly.public_api.http_server.ApiHttpServer,
    unified_api_ready: *std.atomic.Value(bool),
    lifecycle: *UnifiedServerLifecycle,
) !void {
    var server = httpx.Server.initWithConfig(alloc, io, publicHttpServerConfig(bind_host, bind_port));
    defer server.deinit();
    lifecycle.attach(&server);
    defer lifecycle.detach(&server);

    // Register inference AI routes under /ai/v1 and Traditional ML routes under /ml/v1.
    if (antfly_node) |node| {
        try node.registerRoutesOn(inference.server.public_api_prefix, &server);
        try node.registerAiRoutesOn(inference.server.ai_api_prefix, &server);
    }

    // Register antfly public API routes under /db/v1
    const public_router = metadata_openapi.server.ServerRouter(AntflyApiHandler).init(handler);
    var public_prefixed = PrefixedServer("/db/v1", httpx.Server){ .inner = &server };
    try public_router.register(&public_prefixed);

    // Register user management routes under /auth/v1
    const usermgr_router = usermgr_openapi.server.ServerRouter(AntflyApiHandler).init(handler);
    try usermgr_router.register(&server);

    // Health/ready at root level
    try server.get("/healthz", healthzHandler);
    try server.get("/readyz", readyzHandler);

    // Internal group routes are still served by the legacy ApiHttpServer
    // implementation, but the shared httpx server owns the route table.
    active_api_server = api_server;
    defer {
        if (active_api_server == api_server) active_api_server = null;
    }
    try server.use(.{ .name = "storage-maintenance-admission", .handler = storageMaintenanceAdmission });
    try registerStorageMaintenanceRoutes(&server);
    try registerHAAdminRoutes(&server);
    try registerHAInternalRoutes(&server);
    try registerMcpRoutes(&server);
    try registerExperimentalRoutes(&server, api_server.cfg.experimental);
    try registerArdRoutes(&server);
    try registerExtensionRoutes(&server);
    try registerInternalGroupRoutes(&server);
    try registerAntfarmRoutes(&server);

    try server.bind();
    unified_api_ready.store(true, .release);
    lifecycle.publishReady();

    if (server.boundAddress()) |addr| {
        std.debug.print("standalone public api listening on http://{f}\n", .{addr});
    }

    try server.listen();
}

const public_http_connection_ceiling: u32 = 256;

fn publicHttpConnectionLimitForFdSoftLimit(fd_soft_limit: u64) u32 {
    // Public inbound sockets may use at most one quarter of the process FD
    // budget. Storage files, Raft, metadata, outbound providers, logs, and an
    // operator shell retain the rest even when the deployment lowers RLIMIT.
    const proportional_limit = @max(@as(u64, 1), fd_soft_limit / 4);
    return @intCast(@min(proportional_limit, public_http_connection_ceiling));
}

fn configuredPublicHttpConnectionLimit() u32 {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return public_http_connection_ceiling;
    const limit = std.posix.getrlimit(.NOFILE) catch return public_http_connection_ceiling;
    if (limit.cur == std.math.maxInt(@TypeOf(limit.cur))) return public_http_connection_ceiling;
    return publicHttpConnectionLimitForFdSoftLimit(@intCast(limit.cur));
}

fn publicHttpServerConfig(bind_host: []const u8, bind_port: u16) httpx.ServerConfig {
    return .{
        .host = bind_host,
        .port = bind_port,
        .max_body_size = antfly.public_api.http_server.public_api_max_request_body_bytes,
        // Bound aggregate request-body buffering across HTTP/1 and HTTP/2.
        // Four maximum-sized public requests may complete while excess uploads
        // are shed before allocator pressure becomes systemic.
        .request_body_buffer_budget_bytes = 256 * 1024 * 1024,
        // Query execution admits 32 requests. Apply the same bound while H1
        // bodies are still streaming so the remaining public connections can
        // service health, control, and recovery traffic.
        .max_h1_inflight_bodies = 32,
        .request_timeout_ms = 300_000,
        // Keep a large process-wide FD reserve for storage, Raft, outbound
        // clients, and diagnostics. This prevents the historical 1,000-socket
        // cliff under the common 1,024 descriptor soft limit.
        .max_connections = configuredPublicHttpConnectionLimit(),
        .accept_error_backoff_initial_ms = 5,
        .accept_error_backoff_max_ms = 1_000,
        .max_requests_per_connection = public_api_max_requests_per_connection,
        // std.Io currently maps this to SO_REUSEADDR + SO_REUSEPORT on POSIX.
        // The standalone listener lease supplies exclusivity while preserving
        // restart-safe address reuse for connections in TIME_WAIT.
        .reuse_address = true,
    };
}

fn PrefixedServer(comptime prefix: []const u8, comptime Inner: type) type {
    return struct {
        inner: *Inner,

        pub fn post(self: *const @This(), comptime path: []const u8, handler_fn: httpx.Handler) !void {
            try self.inner.post(prefix ++ path, handler_fn);
        }

        pub fn get(self: *const @This(), comptime path: []const u8, handler_fn: httpx.Handler) !void {
            try self.inner.get(prefix ++ path, handler_fn);
        }

        pub fn put(self: *const @This(), comptime path: []const u8, handler_fn: httpx.Handler) !void {
            try self.inner.put(prefix ++ path, handler_fn);
        }

        pub fn delete(self: *const @This(), comptime path: []const u8, handler_fn: httpx.Handler) !void {
            try self.inner.delete(prefix ++ path, handler_fn);
        }
    };
}

fn healthzHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return ctx.json(.{ .status = "ok" });
}

fn readyzHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    if (active_api_server) |api_server| {
        if (api_server.storageMaintenanceExclusiveActive()) {
            _ = ctx.status(503);
            return ctx.json(.{ .status = "maintenance" });
        }
    }
    return ctx.json(.{ .status = "ready" });
}

fn storageMaintenanceAdmission(ctx: *httpx.Context, next: *httpx.Next) anyerror!httpx.Response {
    const api_server = active_api_server orelse return next.call(ctx);
    if (!api_server.storageMaintenanceExclusiveActive()) return next.call(ctx);
    const path = ctx.request.uri.path;
    if (std.mem.eql(u8, path, "/healthz") or
        std.mem.eql(u8, path, "/readyz") or
        std.mem.startsWith(u8, path, antfly.admin.routes.maintenance ++ "/"))
    {
        return next.call(ctx);
    }
    _ = ctx.status(503);
    return ctx.text("storage maintenance in progress");
}

fn registerMcpRoutes(server: anytype) !void {
    const routes = antfly.public_api.http_routes.Routes;
    const mcp_paths = [_][]const u8{
        routes.mcp_v1,
        routes.mcp_v1_prefix ++ "*",
    };
    inline for (mcp_paths) |path| {
        try server.get(path, protocolBridgeHandler);
        try server.post(path, protocolBridgeHandler);
        try server.delete(path, protocolBridgeHandler);
    }
}

fn registerA2aRoutes(server: anytype) !void {
    const routes = antfly.public_api.http_routes.Routes;
    try server.post(routes.a2a, protocolBridgeHandler);
    try server.get(routes.agent_card, protocolBridgeHandler);
    try server.get(routes.agent_card_legacy, protocolBridgeHandler);
}

fn registerExperimentalRoutes(server: anytype, enabled: bool) !void {
    if (!enabled) return;
    try registerA2aRoutes(server);
}

fn registerArdRoutes(server: anytype) !void {
    const routes = antfly.public_api.http_routes.Routes;
    try server.get(routes.ai_catalog, protocolBridgeHandler);
    try server.get(routes.ard_v1, protocolBridgeHandler);
    try server.get(routes.ard_v1 ++ "/*", protocolBridgeHandler);
    try server.post(routes.ard_v1_search, protocolBridgeHandler);
    try server.post(routes.ard_v1_explore, protocolBridgeHandler);
}

fn registerHAAdminRoutes(server: anytype) !void {
    const ha_paths = [_][]const u8{
        antfly.admin.routes.ha,
        antfly.admin.routes.ha ++ "/*",
    };
    inline for (ha_paths) |path| {
        try server.get(path, haAdminBridgeHandler);
        try server.post(path, haAdminBridgeHandler);
        try server.put(path, haAdminBridgeHandler);
        try server.delete(path, haAdminBridgeHandler);
    }
}

fn registerStorageMaintenanceRoutes(server: anytype) !void {
    try server.post(antfly.admin.routes.maintenance_check, haAdminBridgeHandler);
    try server.post(antfly.admin.routes.maintenance_compact, haAdminBridgeHandler);
    try server.post(antfly.admin.routes.maintenance_vacuum, haAdminBridgeHandler);
    try server.get(antfly.admin.routes.maintenance_jobs_prefix ++ "*", haAdminBridgeHandler);
    try server.delete(antfly.admin.routes.maintenance_jobs_prefix ++ "*", haAdminBridgeHandler);
}

fn registerHAInternalRoutes(server: anytype) !void {
    const ha_paths = [_][]const u8{
        antfly.internal.routes.ha,
        antfly.internal.routes.ha ++ "/*",
    };
    inline for (ha_paths) |path| {
        try server.get(path, haInternalBridgeHandler);
        try server.post(path, haInternalBridgeHandler);
        try server.put(path, haInternalBridgeHandler);
        try server.delete(path, haInternalBridgeHandler);
    }
}

fn registerExtensionRoutes(server: anytype) !void {
    const routes = antfly.public_api.http_routes.Routes;
    const extension_paths = [_][]const u8{
        routes.extensions_v1,
        routes.extensions_v1_packages,
        routes.extensions_v1_packages_prefix ++ "*",
        routes.extensions_v1_installed,
        routes.extensions_v1_installed_prefix ++ "*",
    };
    inline for (extension_paths) |path| {
        try server.get(path, extensionBridgeHandler);
        try server.post(path, extensionBridgeHandler);
        try server.put(path, extensionBridgeHandler);
    }
}

fn registerAntfarmRoutes(server: anytype) !void {
    try server.get("/", antfarmIndexHandler);
    try server.get("/assets/*", antfarmAssetHandler);
    try server.get("/fonts/*", antfarmFontHandler);
    try server.get("/*", antfarmSpaHandler);
}

fn antfarmIndexHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveAntfarmFile(ctx, "index.html");
}

fn antfarmAssetHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveAntfarmPrefixedFile(ctx, "/assets/", "assets/");
}

fn antfarmFontHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveAntfarmPrefixedFile(ctx, "/fonts/", "fonts/");
}

fn antfarmSpaHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    const path = ctx.request.uri.path;
    if (isAntfarmReservedPath(path)) {
        return ctx.status(404).text("not found");
    }
    if (!std.mem.startsWith(u8, path, "/")) {
        return ctx.status(400).text("invalid path");
    }

    const rel_path = path[1..];
    if (rel_path.len > 0 and std.mem.indexOfScalar(u8, rel_path, '.') != null) {
        return serveAntfarmFile(ctx, rel_path);
    }
    return serveAntfarmFile(ctx, "index.html");
}

fn serveAntfarmPrefixedFile(ctx: *httpx.Context, prefix: []const u8, rel_prefix: []const u8) anyerror!httpx.Response {
    const path = ctx.request.uri.path;
    if (!std.mem.startsWith(u8, path, prefix)) {
        return ctx.status(400).text("invalid path");
    }
    const suffix = path[prefix.len..];
    if (suffix.len == 0) {
        return ctx.status(404).text("not found");
    }

    var rel_buf: [1024]u8 = undefined;
    const rel_path = std.fmt.bufPrint(&rel_buf, "{s}{s}", .{ rel_prefix, suffix }) catch {
        return ctx.status(414).text("path too long");
    };
    return serveAntfarmFile(ctx, rel_path);
}

fn serveAntfarmFile(ctx: *httpx.Context, rel_path: []const u8) anyerror!httpx.Response {
    if (hasUnsafeStaticPath(rel_path)) {
        return ctx.status(400).text("invalid path");
    }

    if (try serveInstalledAntfarmFile(ctx, rel_path)) |resp| {
        return resp;
    }

    for (antfarm_asset_roots) |root| {
        var full_path_buf: [4096]u8 = undefined;
        const full_path = std.fmt.bufPrint(&full_path_buf, "{s}/{s}", .{ root, rel_path }) catch continue;
        if (try serveAntfarmPath(ctx, rel_path, full_path)) |resp| return resp;
    }

    return ctx.status(404).text("not found");
}

fn serveInstalledAntfarmFile(ctx: *httpx.Context, rel_path: []const u8) anyerror!?httpx.Response {
    const exe_dir = std.process.executableDirPathAlloc(ctx.io, ctx.allocator) catch return null;
    defer ctx.allocator.free(exe_dir);

    var full_path_buf: [4096]u8 = undefined;
    const full_path = std.fmt.bufPrint(
        &full_path_buf,
        "{s}/{s}/{s}",
        .{ exe_dir, antfarm_installed_asset_root, rel_path },
    ) catch return null;

    return try serveAntfarmPath(ctx, rel_path, full_path);
}

fn serveAntfarmPath(ctx: *httpx.Context, rel_path: []const u8, full_path: []const u8) anyerror!?httpx.Response {
    const body = std.Io.Dir.cwd().readFileAlloc(
        ctx.io,
        full_path,
        ctx.allocator,
        std.Io.Limit.limited(antfarm_max_file_bytes),
    ) catch |err| switch (err) {
        error.FileNotFound, error.NotDir, error.IsDir => return null,
        error.StreamTooLong => return try ctx.status(413).text("file too large"),
        else => return err,
    };

    var resp = httpx.Response.init(ctx.allocator, 200);
    errdefer resp.deinit();
    try resp.headers.set("Content-Type", antfarmContentType(rel_path));
    resp.body = body;
    resp.body_owned = true;
    return resp;
}

fn hasUnsafeStaticPath(path: []const u8) bool {
    if (path.len == 0) return true;
    if (path[0] == '/') return true;
    if (std.mem.indexOfScalar(u8, path, 0) != null) return true;
    if (std.mem.indexOfScalar(u8, path, '\\') != null) return true;
    if (std.mem.indexOf(u8, path, "..") != null) return true;
    var i: usize = 0;
    while (i + 2 < path.len) : (i += 1) {
        if (path[i] != '%' or path[i + 1] != '2') continue;
        if (path[i + 2] == 'f' or path[i + 2] == 'F' or path[i + 2] == 'e' or path[i + 2] == 'E') return true;
    }
    return false;
}

fn isAntfarmReservedPath(path: []const u8) bool {
    const reserved = [_][]const u8{
        "/api",
        "/db",
        "/ai",
        "/ml",
        "/antfly",
        "/metadata",
        "/admin",
        "/internal",
        "/mcp",
        "/a2a",
        "/.well-known",
        "/extensions",
        "/healthz",
        "/readyz",
        "/registry",
    };
    for (reserved) |prefix| {
        if (std.mem.eql(u8, path, prefix)) return true;
        if (path.len > prefix.len and std.mem.startsWith(u8, path, prefix) and path[prefix.len] == '/') return true;
    }
    return isVersionedApiPath(path);
}

fn isVersionedApiPath(path: []const u8) bool {
    if (path.len < 5 or path[0] != '/') return false;
    const first_separator = std.mem.indexOfScalarPos(u8, path, 1, '/') orelse return false;
    if (first_separator == 1 or first_separator + 2 >= path.len or path[first_separator + 1] != 'v') return false;

    var cursor = first_separator + 2;
    const digits_start = cursor;
    while (cursor < path.len and std.ascii.isDigit(path[cursor])) : (cursor += 1) {}
    if (cursor == digits_start) return false;
    return cursor == path.len or path[cursor] == '/';
}

fn haAdminBridgeHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    const server = active_api_server orelse {
        _ = ctx.status(503);
        return ctx.text("not ready");
    };

    const method: http_common.Method = switch (ctx.request.method) {
        .GET => .GET,
        .POST => .POST,
        .PUT => .PUT,
        .DELETE => .DELETE,
        else => {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        },
    };

    const body_data = (try ctx.body()) orelse "";
    const idempotency_headers: []const http_common.RequestHeader = if (ctx.header("idempotency-key")) |value|
        &.{.{ .name = "Idempotency-Key", .value = value }}
    else
        &.{};
    const legacy_req = http_common.HttpRequest{
        .method = method,
        .uri = ctx.request.uri.raw,
        .headers = idempotency_headers,
        .authorization = ctx.header("authorization"),
        .content_type = ctx.header("content-type"),
        .body = body_data,
    };

    var resp = try server.handle(legacy_req);
    return AntflyApiHandler.respondWithAllocator(ctx, &resp, server.alloc);
}

fn haInternalBridgeHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    const server = active_api_server orelse {
        _ = ctx.status(503);
        return ctx.text("not ready");
    };

    const method: http_common.Method = switch (ctx.request.method) {
        .GET => .GET,
        .POST => .POST,
        .PUT => .PUT,
        .DELETE => .DELETE,
        else => {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        },
    };

    const body_data = (try ctx.body()) orelse "";
    const legacy_req = http_common.HttpRequest{
        .method = method,
        .uri = ctx.request.uri.raw,
        .authorization = ctx.header("authorization"),
        .content_type = ctx.header("content-type"),
        .body = body_data,
    };

    var resp = try server.handle(legacy_req);
    return AntflyApiHandler.respondWithAllocator(ctx, &resp, server.alloc);
}

fn antfarmContentType(path: []const u8) []const u8 {
    if (std.mem.endsWith(u8, path, ".html")) return "text/html; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".css")) return "text/css; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".js") or std.mem.endsWith(u8, path, ".mjs")) return "application/javascript; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".json")) return "application/json";
    if (std.mem.endsWith(u8, path, ".png")) return "image/png";
    if (std.mem.endsWith(u8, path, ".jpg") or std.mem.endsWith(u8, path, ".jpeg")) return "image/jpeg";
    if (std.mem.endsWith(u8, path, ".svg")) return "image/svg+xml";
    if (std.mem.endsWith(u8, path, ".ico")) return "image/x-icon";
    if (std.mem.endsWith(u8, path, ".webp")) return "image/webp";
    if (std.mem.endsWith(u8, path, ".woff")) return "font/woff";
    if (std.mem.endsWith(u8, path, ".woff2")) return "font/woff2";
    if (std.mem.endsWith(u8, path, ".ttf")) return "font/ttf";
    if (std.mem.endsWith(u8, path, ".wasm")) return "application/wasm";
    if (std.mem.endsWith(u8, path, ".map")) return "application/json";
    if (std.mem.endsWith(u8, path, ".txt")) return "text/plain; charset=utf-8";
    return "application/octet-stream";
}

fn localReplicaRootReconcileHook(data_server: *antfly.data.runtime.DataServer) antfly.metadata_service.LocalReplicaRootReconcileHook {
    return .{
        .ptr = data_server,
        .vtable = &.{
            .run = runLocalReplicaRootReconcileHook,
        },
    };
}

fn localSchemaProgressProvider(data_server: *antfly.data.runtime.DataServer) LocalSchemaProgressProvider {
    return .{
        .ptr = data_server,
        .collect = collectLocalSchemaProgress,
    };
}

fn collectLocalSchemaProgress(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    tables: []const antfly.metadata.TableRecord,
    ranges: []const antfly.metadata.RangeRecord,
) !antfly.data.runtime.DataServer.LocalSchemaProgressSnapshot {
    const data_server: *antfly.data.runtime.DataServer = @ptrCast(@alignCast(ptr));
    return try data_server.collectLocalSchemaProgressSnapshot(alloc, tables, ranges);
}

fn localReplicaRootReconcilePermitHook(data_server: *antfly.data.runtime.DataServer) antfly.metadata_service.LocalReplicaRootReconcilePermitHook {
    return .{
        .ptr = data_server,
        .vtable = &.{
            .should_reconcile = runLocalReplicaRootReconcilePermitHook,
        },
    };
}

fn runLocalReplicaRootReconcileHook(
    ptr: *anyopaque,
    request: antfly.metadata_service.LocalReplicaRootReconcileHook.Request,
) !antfly.metadata.table_provisioner.ProvisionSummary {
    const data_server: *antfly.data.runtime.DataServer = @ptrCast(@alignCast(ptr));
    return try data_server.reconcileVisibleProvisionedReplicaStateFromSnapshot(
        request.metadata_group_id,
        request.group_ids,
        request.tables,
        request.ranges,
    );
}

fn runLocalReplicaRootReconcilePermitHook(ptr: *anyopaque) bool {
    const data_server: *antfly.data.runtime.DataServer = @ptrCast(@alignCast(ptr));
    return !data_server.shouldDeferProvisionedReplicaRootReconcile();
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

fn readFileAlloc(alloc: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_bytes));
}

fn writeFileAtomically(alloc: std.mem.Allocator, path: []const u8, contents: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-standalone-metadata-{d}", .{ path, platform_time.monotonicNs() });
    defer alloc.free(tmp_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
        try file.sync(io);
    }

    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        return err;
    };
    const parent = std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".";
    try fs_paths.syncDirPortable(io, parent);
}

fn registerInternalGroupRoutes(server: anytype) !void {
    const routes = antfly.public_api.http_routes.Routes;
    const group_prefix = routes.internal_groups_prefix ++ ":group_id";
    const table_prefix = group_prefix ++ "/tables/:table_name";
    const internal_table_prefix = routes.internal_tables_prefix ++ ":table_name";
    const internal_table_repair_cancel_state = internal_table_prefix ++ routes.repair_jobs_marker ++ ":job_id" ++ routes.repair_attempts_marker ++ ":attempt_id" ++ routes.repair_cancel_state_suffix;

    const get_routes = [_][]const u8{
        group_prefix ++ routes.group_db_median_key_suffix,
        table_prefix ++ routes.documents_marker ++ ":key",
        internal_table_repair_cancel_state,
    };
    inline for (get_routes) |path| {
        try server.get(path, internalBridgeHandler);
    }

    const post_routes = [_][]const u8{
        internal_table_prefix ++ routes.corrupt_embedding_artifact_suffix,
        group_prefix ++ routes.shard_ops_observe_split_suffix,
        group_prefix ++ routes.shard_ops_observe_merge_suffix,
        group_prefix ++ routes.shard_ops_execute_suffix,
        table_prefix ++ routes.documents_suffix,
        table_prefix ++ routes.graph_expand_suffix,
        table_prefix ++ routes.graph_hydrate_suffix,
        table_prefix ++ routes.text_stats_suffix,
        table_prefix ++ routes.join_job_state_suffix,
        table_prefix ++ routes.join_finalize_suffix,
        table_prefix ++ routes.join_rows_suffix,
        table_prefix ++ routes.join_unmatched_suffix,
        table_prefix ++ routes.join_partition_suffix,
        table_prefix ++ routes.query_suffix,
        table_prefix ++ routes.batch_suffix,
        table_prefix ++ routes.txn_begin_suffix,
        table_prefix ++ routes.txn_prepare_suffix,
        table_prefix ++ routes.txn_resolve_suffix,
        table_prefix ++ routes.txn_status_suffix,
    };
    inline for (post_routes) |path| {
        try server.post(path, internalBridgeHandler);
    }
}

fn internalBridgeHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    const path = ctx.request.uri.path;
    const routes = antfly.public_api.http_routes.Routes;
    if (!std.mem.startsWith(u8, path, routes.internal_groups_prefix) and
        routes.matchInternalTableCorruptEmbeddingArtifact(path) == null and
        routes.matchInternalTableRepairCancelState(path) == null)
    {
        _ = ctx.status(404);
        return ctx.text("not found");
    }

    const server = active_api_server orelse {
        _ = ctx.status(503);
        return ctx.text("not ready");
    };

    var converted_req = AntflyApiHandler.httpRequestFromContext(ctx, null) catch |err| switch (err) {
        error.UnsupportedMethod => {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        },
        else => return err,
    };
    defer converted_req.deinit();

    var resp = (try server.handleInternalRoute(converted_req.value)) orelse {
        _ = ctx.status(404);
        return ctx.text("not found");
    };
    return AntflyApiHandler.respondWithAllocator(ctx, &resp, server.alloc);
}

fn protocolBridgeHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    const server = active_api_server orelse {
        _ = ctx.status(503);
        return ctx.text("not ready");
    };

    var converted_req = AntflyApiHandler.httpRequestFromContext(ctx, null) catch |err| switch (err) {
        error.UnsupportedMethod => {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        },
        else => return err,
    };
    defer converted_req.deinit();

    var resp = try server.handle(converted_req.value);
    return AntflyApiHandler.respondWithAllocator(ctx, &resp, server.alloc);
}

fn extensionBridgeHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    const server = active_api_server orelse {
        _ = ctx.status(503);
        return ctx.text("not ready");
    };

    var converted_req = AntflyApiHandler.httpRequestFromContext(ctx, null) catch |err| switch (err) {
        error.UnsupportedMethod => {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        },
        else => return err,
    };
    defer converted_req.deinit();

    var resp = try server.handle(converted_req.value);
    return AntflyApiHandler.respondWithAllocator(ctx, &resp, server.alloc);
}

// Module-level pointer set by the serve thread before listen().
// Used by explicitly registered protocol/internal bridge handlers.
var active_api_server: ?*antfly.public_api.http_server.ApiHttpServer = null;

// ---------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------

fn parsePreloadModelKind(value: []const u8) ?inference.server.WarmModelKind {
    inline for (std.meta.fields(inference.server.WarmModelKind)) |field| {
        if (std.mem.eql(u8, value, field.name)) return @enumFromInt(field.value);
    }
    return null;
}

fn parsePreloadModelFlag(value: []const u8) !inference.server.WarmModel {
    const separator = std.mem.indexOfScalar(u8, value, ':') orelse return error.InvalidArguments;
    const kind_name = value[0..separator];
    var model_name = value[separator + 1 ..];
    var backend: ?inference.backends.BackendType = null;
    if (std.mem.indexOfScalar(u8, model_name, ':')) |backend_separator| {
        const backend_name = model_name[0..backend_separator];
        backend = antfly.inference_runtime.parseBackendType(backend_name) orelse return error.InvalidArguments;
        model_name = model_name[backend_separator + 1 ..];
    }
    if (model_name.len == 0) return error.InvalidArguments;
    return .{
        .kind = parsePreloadModelKind(kind_name) orelse return error.InvalidArguments,
        .name = model_name,
        .backend = backend,
        .format = null,
        .quantization = null,
    };
}

fn parseCli(alloc: std.mem.Allocator, args: *std.process.Args.Iterator) !CliConfig {
    var cfg = CliConfig{};
    errdefer cfg.deinit(alloc);
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            cfg.help = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--experimental")) {
            cfg.experimental = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--config")) {
            cfg.config_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--id")) {
            cfg.local_node_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--host")) {
            cfg.bind_host = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--port")) {
            cfg.bind_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--health-port")) {
            cfg.health_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--health")) {
            const value = args.next() orelse return error.InvalidArguments;
            cfg.health_enabled = parseBoolFlag(value) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--health=")) {
            cfg.health_enabled = parseBoolFlag(arg["--health=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--control-tick-ms")) {
            cfg.control_tick_ms = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--auth")) {
            const value = args.next() orelse return error.InvalidArguments;
            cfg.auth_enabled = parseBoolFlag(value) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--auth=")) {
            cfg.auth_enabled = parseBoolFlag(arg["--auth=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ard-publisher-domain")) {
            cfg.ard_publisher_domain = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ard-base-url")) {
            cfg.ard_base_url = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ard-display-name")) {
            cfg.ard_display_name = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ard-public-catalog")) {
            const value = args.next() orelse return error.InvalidArguments;
            cfg.ard_public_catalog_enabled = parseBoolFlag(value) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--ard-public-catalog=")) {
            cfg.ard_public_catalog_enabled = parseBoolFlag(arg["--ard-public-catalog=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--models-dir")) {
            cfg.inference_models_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ml-dir")) {
            cfg.inference_ml_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--inference-host-budget-mb")) {
            cfg.inference_host_budget_mb = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--inference-backend-budget-mb")) {
            cfg.inference_backend_budget_mb = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--inference-combined-budget-mb")) {
            cfg.inference_combined_budget_mb = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--inference-kv-budget-mb")) {
            cfg.inference_kv_budget_mb = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--inference-scratch-budget-mb")) {
            cfg.inference_scratch_budget_mb = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--preload-model")) {
            try cfg.inference_preload_models.append(alloc, try parsePreloadModelFlag(args.next() orelse return error.InvalidArguments));
            continue;
        }
        if (std.mem.eql(u8, arg, "--data-dir")) {
            cfg.data_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--storage-engine")) {
            cfg.storage_engine = parseStorageEngine(args.next() orelse return error.InvalidArguments) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--storage-engine=")) {
            cfg.storage_engine = parseStorageEngine(arg["--storage-engine=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--storage-path")) {
            cfg.storage_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--storage-path=")) {
            cfg.storage_path = arg["--storage-path=".len..];
            continue;
        }
        if (std.mem.eql(u8, arg, "--fsync")) {
            cfg.storage_fsync = parseBoolFlag(args.next() orelse return error.InvalidArguments) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--fsync=")) {
            cfg.storage_fsync = parseBoolFlag(arg["--fsync=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--replica-root-dir")) {
            cfg.replica_root_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--replica-catalog-path")) {
            cfg.replica_catalog_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--snapshot-root-dir")) {
            cfg.snapshot_root_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--extension-package-store")) {
            cfg.extension_package_store_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--secret-store-path")) {
            try cfg.secret_store_paths.append(alloc, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-primary-log")) {
            cfg.ha_primary_log = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-primary-slots")) {
            cfg.ha_primary_slots = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-primary-node-id")) {
            cfg.ha_primary_node_id = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-fence-wal")) {
            cfg.ha_fence_wal = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-former-primary-log")) {
            cfg.ha_former_primary_log = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--admin-token-env")) {
            cfg.admin_token_env = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-retention-max-lag-lsn")) {
            cfg.ha_retention_max_lag_lsn = try parsePositiveU64(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-retention-max-retained-bytes")) {
            cfg.ha_retention_max_retained_bytes = try parsePositiveU64(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-retention-max-retained-age-ns")) {
            cfg.ha_retention_max_retained_age_ns = try parsePositiveU64(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-sync-mode")) {
            cfg.ha_sync_mode = try parseHASyncDurabilityMode(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-sync-selection")) {
            cfg.ha_sync_selection = try parseHASyncStandbySelection(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-sync-required")) {
            cfg.ha_sync_required = try parsePositiveUsize(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-sync-standby")) {
            try cfg.ha_sync_standby_names.append(alloc, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-sync-failure")) {
            cfg.ha_sync_failure_policy = try parseHASyncFailurePolicy(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-standby-log")) {
            cfg.ha_standby_log = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-standby-progress")) {
            cfg.ha_standby_progress = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-standby-node-id")) {
            cfg.ha_standby_node_id = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-standby-upstream-url")) {
            cfg.ha_standby_upstream_url = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-standby-slot")) {
            cfg.ha_standby_slot = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-cluster-id")) {
            cfg.ha_cluster_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-shard-id")) {
            cfg.ha_shard_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-table-id")) {
            cfg.ha_table_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-timeline-id")) {
            cfg.ha_timeline_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-epoch")) {
            cfg.ha_epoch = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        return error.InvalidArguments;
    }
    return cfg;
}

fn parseStorageEngine(value: []const u8) ?antfly.common.config.StorageEngine {
    if (std.mem.eql(u8, value, "local")) return .local;
    if (std.mem.eql(u8, value, "lite")) return .lite;
    if (std.mem.eql(u8, value, "object")) return .object;
    return null;
}

fn resolveLocalBaseDir(
    alloc: std.mem.Allocator,
    cli: CliConfig,
    cfg: ?*const antfly.common.config.Config,
) ![]u8 {
    if (cli.data_dir) |path| return try normalizeResolvedPathAlloc(alloc, path);
    return try antfly.common.config.resolveLocalBaseDir(alloc, cfg);
}

fn resolvePaths(
    alloc: std.mem.Allocator,
    cli: CliConfig,
    cfg: ?*const antfly.common.config.Config,
) !ResolvedPaths {
    const local_base = try resolveLocalBaseDir(alloc, cli, cfg);
    defer alloc.free(local_base);
    const data_base = try std.fmt.allocPrint(alloc, "{s}/data", .{local_base});
    defer alloc.free(data_base);
    const metadata_base = try std.fmt.allocPrint(alloc, "{s}/metadata", .{local_base});
    defer alloc.free(metadata_base);

    const replica_root_dir = if (cli.replica_root_dir) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/replicas", .{data_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(replica_root_dir);
    const replica_catalog_path = if (cli.replica_catalog_path) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/catalog.txt", .{data_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(replica_catalog_path);
    const local_metadata_catalog_path = blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/local-metadata.json", .{metadata_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(local_metadata_catalog_path);
    const snapshot_root_dir = if (cli.snapshot_root_dir) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/snapshots", .{data_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(snapshot_root_dir);
    const extension_package_store_dir = try resolveExtensionPackageStoreDir(alloc, cli.extension_package_store_dir, local_base);
    errdefer alloc.free(extension_package_store_dir);
    const secret_store_path = if (cli.primarySecretStorePath()) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/secrets.json", .{local_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(secret_store_path);
    const auth_store_root_dir = blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/auth", .{metadata_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(auth_store_root_dir);

    return .{
        .replica_root_dir = replica_root_dir,
        .replica_catalog_path = replica_catalog_path,
        .local_metadata_catalog_path = local_metadata_catalog_path,
        .snapshot_root_dir = snapshot_root_dir,
        .extension_package_store_dir = extension_package_store_dir,
        .secret_store_path = secret_store_path,
        .auth_store_root_dir = auth_store_root_dir,
    };
}

fn initLayeredSecretStore(
    alloc: std.mem.Allocator,
    raw_paths: []const []const u8,
) !antfly.common.secrets.FileStore {
    var normalized_paths: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (normalized_paths.items) |path| alloc.free(path);
        normalized_paths.deinit(alloc);
    }
    for (raw_paths) |raw_path| {
        const normalized_path = try normalizeResolvedPathAlloc(alloc, raw_path);
        errdefer alloc.free(normalized_path);
        try normalized_paths.append(alloc, normalized_path);
    }
    return try antfly.common.secrets.FileStore.initLayered(alloc, normalized_paths.items);
}

fn resolveExtensionPackageStoreDir(
    alloc: std.mem.Allocator,
    cli_path: ?[]const u8,
    local_base: []const u8,
) ![]u8 {
    const env_var_z = try alloc.dupeZ(u8, antfly.extensions.wasmtime_runtime.package_store_env);
    defer alloc.free(env_var_z);
    return try resolveExtensionPackageStoreDirWithEnv(
        alloc,
        cli_path,
        local_base,
        platform.env.getenvSlice(env_var_z),
    );
}

fn resolveExtensionPackageStoreDirWithEnv(
    alloc: std.mem.Allocator,
    cli_path: ?[]const u8,
    local_base: []const u8,
    env_path: ?[]const u8,
) ![]u8 {
    if (cli_path) |path| return try normalizeResolvedPathAlloc(alloc, path);
    if (env_path) |path| {
        if (std.mem.trim(u8, path, " \t\r\n").len > 0) {
            return try normalizeResolvedPathAlloc(alloc, path);
        }
    }

    const raw = try std.fmt.allocPrint(alloc, "{s}/extensions", .{local_base});
    defer alloc.free(raw);
    return try normalizeResolvedPathAlloc(alloc, raw);
}

fn normalizeResolvedPathAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    if (!std.fs.path.isAbsolute(path)) return try alloc.dupe(u8, path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var probe = path;
    while (true) {
        const resolved_z = std.Io.Dir.realPathFileAbsoluteAlloc(io_impl.io(), probe, alloc) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => null,
            else => return err,
        };
        if (resolved_z) |resolved| {
            defer alloc.free(resolved);
            const resolved_prefix = resolved[0..resolved.len];
            if (probe.len == path.len) return try alloc.dupe(u8, resolved_prefix);

            const suffix_start: usize = if (probe.len == 1) 1 else probe.len + 1;
            const suffix = path[suffix_start..];
            return try std.fs.path.join(alloc, &.{ resolved_prefix, suffix });
        }

        const parent = std.fs.path.dirname(probe) orelse return try alloc.dupe(u8, path);
        if (parent.len == probe.len) return try alloc.dupe(u8, path);
        probe = parent;
    }
}

fn resolveMetadataRaftListener(
    cli: CliConfig,
    local_node_id: u64,
    cfg: ?*const antfly.common.config.Config,
) antfly.metadata.runtime.ListenerConfig {
    if (cfg) |loaded| {
        if (antfly.metadata.runtime.metadataClusterPeerUrl(loaded, local_node_id)) |url| {
            return antfly.metadata.runtime.parseHostPort(url) catch .{ .bind_host = cli.bind_host orelse "127.0.0.1", .bind_port = 0 };
        }
    }
    return antfly.metadata.runtime.resolveListener(cli.bind_host, null, cfg);
}

fn resolveMetadataApiListener(
    cfg: ?*const antfly.common.config.Config,
    local_node_id: u64,
    fallback_host: []const u8,
) antfly.metadata.runtime.ListenerConfig {
    if (cfg) |loaded| {
        if (antfly.metadata.runtime.metadataOrchestrationPeerUrl(loaded, local_node_id)) |url| {
            return antfly.metadata.runtime.parseHostPort(url) catch .{ .bind_host = fallback_host, .bind_port = 0 };
        }
    }
    return .{ .bind_host = fallback_host, .bind_port = 0 };
}

fn resolveMetadataClusterPeers(
    alloc: std.mem.Allocator,
    cfg: ?*const antfly.common.config.Config,
) ![]antfly.metadata.runtime.MetadataClusterPeer {
    if (cfg) |loaded| return try antfly.metadata.runtime.metadataClusterPeersFromConfig(alloc, loaded);
    return &.{};
}

fn resolvePublicListener(cli: CliConfig) antfly.metadata.runtime.ListenerConfig {
    return .{
        .bind_host = cli.bind_host orelse "127.0.0.1",
        .bind_port = cli.bind_port orelse default_public_port,
    };
}

fn haPrimaryRequested(cli: CliConfig) bool {
    return cli.ha_primary_log != null or
        cli.ha_primary_slots != null or
        cli.ha_primary_node_id != null;
}

fn haStandbyRequested(cli: CliConfig) bool {
    return cli.ha_standby_log != null or
        cli.ha_standby_progress != null or
        cli.ha_standby_node_id != null or
        cli.ha_standby_upstream_url != null or
        cli.ha_standby_slot != null;
}

fn haIdentityRequested(cli: CliConfig) bool {
    return cli.ha_cluster_id != null or
        cli.ha_shard_id != null or
        cli.ha_table_id != null or
        cli.ha_timeline_id != null or
        cli.ha_epoch != null;
}

fn haSyncPolicyRequested(cli: CliConfig) bool {
    return cli.ha_sync_mode != null or
        cli.ha_sync_selection != null or
        cli.ha_sync_required != null or
        cli.ha_sync_failure_policy != null or
        cli.ha_sync_standby_names.items.len > 0;
}

fn haRetentionPolicyRequested(cli: CliConfig) bool {
    return cli.ha_retention_max_lag_lsn != null or
        cli.ha_retention_max_retained_bytes != null or
        cli.ha_retention_max_retained_age_ns != null;
}

fn validateHARole(cli: CliConfig) !void {
    const primary_requested = haPrimaryRequested(cli);
    const standby_requested = haStandbyRequested(cli);
    if (primary_requested and standby_requested) return error.HAMultipleRolesConfigured;
    if (haIdentityRequested(cli) and !primary_requested and !standby_requested) return error.HARoleMissing;
    if (cli.ha_fence_wal != null and !primary_requested and !standby_requested) return error.HARoleMissing;
    if (cli.ha_former_primary_log != null and !primary_requested and !standby_requested) return error.HARoleMissing;
    if (cli.ha_former_primary_log != null) {
        _ = try requireHAPath(cli.ha_former_primary_log, error.HAFormerPrimaryLogInvalid, error.HAFormerPrimaryLogInvalid);
    }
    if (cli.admin_token_env) |env_var| {
        switch (antfly.ha.validation.classifyHAString(env_var)) {
            .ok => {},
            .missing => return error.AdminTokenEnvMissing,
            .padded => return error.AdminTokenEnvInvalid,
        }
        if (!antfly.ha.validation.isEnvVarName(env_var)) return error.AdminTokenEnvInvalid;
    }
    if (primary_requested or standby_requested) {
        _ = try requireHAPath(cli.ha_fence_wal, error.HAFenceWalMissing, error.HAFenceWalInvalid);
    }
    if (primary_requested or standby_requested) try validateHAIdentity(cli);
    if (primary_requested) try validateHAPrimaryRoleComplete(cli);
    if (standby_requested) try validateHAStandbyRoleComplete(cli);
    if (haRetentionPolicyRequested(cli) and !primary_requested) return error.HARetentionPolicyRequiresPrimary;
    if (haSyncPolicyRequested(cli) and !primary_requested) return error.HASyncPolicyRequiresPrimary;
}

fn validateHAIdentity(cli: CliConfig) !void {
    if (cli.ha_cluster_id == null) return error.HAClusterIdMissing;
    if (cli.ha_timeline_id == null) return error.HATimelineIdMissing;
    if (cli.ha_epoch == null) return error.HAEpochMissing;
}

fn requireHAString(value: ?[]const u8, comptime missing_err: anyerror, comptime padded_err: anyerror) ![]const u8 {
    switch (antfly.ha.validation.classifyHAString(value)) {
        .ok => return value.?,
        .missing => return missing_err,
        .padded => return padded_err,
    }
}

fn requireHAPath(value: ?[]const u8, comptime missing_err: anyerror, comptime invalid_err: anyerror) ![]const u8 {
    const raw = try requireHAString(value, missing_err, invalid_err);
    if (!antfly.ha.validation.isAbsoluteNormalizedPath(raw)) return invalid_err;
    return raw;
}

fn requireHAPathWithinRoot(value: ?[]const u8, root: []const u8, comptime missing_err: anyerror, comptime invalid_err: anyerror) ![]const u8 {
    const raw = try requireHAPath(value, missing_err, invalid_err);
    if (!antfly.ha.validation.isAbsoluteNormalizedPathWithinRoot(raw, root)) return invalid_err;
    return raw;
}

fn requireHAIdentifier(value: ?[]const u8, comptime missing_err: anyerror, comptime invalid_err: anyerror) ![]const u8 {
    const raw = try requireHAString(value, missing_err, invalid_err);
    if (!antfly.ha.validation.isIdentifier(raw)) return invalid_err;
    return raw;
}

fn validateHAPrimaryRoleComplete(cli: CliConfig) !void {
    _ = try requireHAPath(cli.ha_primary_log, error.HAPrimaryLogMissing, error.HAPrimaryLogInvalid);
    _ = try requireHAPath(cli.ha_primary_slots, error.HAPrimarySlotsMissing, error.HAPrimarySlotsInvalid);
    _ = try requireHAIdentifier(cli.ha_primary_node_id, error.HAPrimaryNodeIdMissing, error.HAPrimaryNodeIdInvalid);
}

fn validateHAStandbyRoleComplete(cli: CliConfig) !void {
    _ = try requireHAPath(cli.ha_standby_log, error.HAStandbyLogMissing, error.HAStandbyLogInvalid);
    _ = try requireHAPath(cli.ha_standby_progress, error.HAStandbyProgressMissing, error.HAStandbyProgressInvalid);
    _ = try requireHAIdentifier(cli.ha_standby_node_id, error.HAStandbyNodeIdMissing, error.HAStandbyNodeIdInvalid);
}

fn validateHAPathsUnderRoot(cli: CliConfig, data_root: []const u8) !void {
    if (cli.ha_former_primary_log != null) {
        _ = try requireHAPathWithinRoot(cli.ha_former_primary_log, data_root, error.HAFormerPrimaryLogInvalid, error.HAFormerPrimaryLogInvalid);
    }
    if (haPrimaryRequested(cli) or haStandbyRequested(cli)) {
        _ = try requireHAPathWithinRoot(cli.ha_fence_wal, data_root, error.HAFenceWalMissing, error.HAFenceWalInvalid);
    }
    if (haPrimaryRequested(cli)) {
        _ = try requireHAPathWithinRoot(cli.ha_primary_log, data_root, error.HAPrimaryLogMissing, error.HAPrimaryLogInvalid);
        _ = try requireHAPathWithinRoot(cli.ha_primary_slots, data_root, error.HAPrimarySlotsMissing, error.HAPrimarySlotsInvalid);
    }
    if (haStandbyRequested(cli)) {
        _ = try requireHAPathWithinRoot(cli.ha_standby_log, data_root, error.HAStandbyLogMissing, error.HAStandbyLogInvalid);
        _ = try requireHAPathWithinRoot(cli.ha_standby_progress, data_root, error.HAStandbyProgressMissing, error.HAStandbyProgressInvalid);
    }
}

fn haStandbyReplicationConfigFromCli(cli: CliConfig) !?antfly.data.runtime.HAStandbyReplicationConfig {
    if (cli.ha_standby_upstream_url == null and cli.ha_standby_slot == null) return null;
    const upstream = try requireHAString(cli.ha_standby_upstream_url, error.HAStandbyUpstreamUrlMissing, error.HAStandbyUpstreamUrlInvalid);
    const slot = try requireHAIdentifier(cli.ha_standby_slot, error.HAStandbySlotMissing, error.HAStandbySlotInvalid);
    const parsed = antfly.ha.validation.parseURLNoHiddenWhitespace(upstream) catch return error.HAStandbyUpstreamUrlInvalid;
    if (!isHAReplicationUpstreamScheme(parsed)) return error.HAStandbyUpstreamUrlInvalid;
    if (parsed.host == null) return error.HAStandbyUpstreamUrlInvalid;
    return .{
        .upstream_base_uri = upstream,
        .slot_name = slot,
        .standby_log_path = cli.ha_standby_log,
        .standby_progress_path = cli.ha_standby_progress,
    };
}

fn isHAReplicationUpstreamScheme(parsed: std.Uri) bool {
    return std.mem.eql(u8, parsed.scheme, "http") or std.mem.eql(u8, parsed.scheme, "https");
}

const OwnedHASyncPolicy = struct {
    policy: antfly.ha.primary.SyncPolicy = .{},
    standby_names: []const []const u8 = &.{},

    fn deinit(self: *OwnedHASyncPolicy, alloc: std.mem.Allocator) void {
        if (self.standby_names.len > 0) alloc.free(self.standby_names);
        self.* = undefined;
    }
};

fn haSyncPolicyFromCli(alloc: std.mem.Allocator, cli: CliConfig) !OwnedHASyncPolicy {
    if (!haSyncPolicyRequested(cli)) return .{};
    if (!haPrimaryRequested(cli)) return error.HASyncPolicyRequiresPrimary;

    const names = try alloc.alloc([]const u8, cli.ha_sync_standby_names.items.len);
    errdefer alloc.free(names);
    @memcpy(names, cli.ha_sync_standby_names.items);
    const selection = cli.ha_sync_selection orelse .any;
    if (selection == .all and cli.ha_sync_required != null) return error.InvalidHASyncPolicy;

    const policy = antfly.ha.primary.SyncPolicy{
        .mode = cli.ha_sync_mode orelse .remote_write,
        .selection = selection,
        .required = if (selection == .all) names.len else cli.ha_sync_required orelse 1,
        .standby_names = names,
        .failure_policy = cli.ha_sync_failure_policy orelse .block,
    };
    try validateHASyncPolicy(policy);

    return .{
        .policy = policy,
        .standby_names = names,
    };
}

fn haRetentionPolicyFromCli(cli: CliConfig) !antfly.ha.slot_store.RetentionPolicy {
    if (!haRetentionPolicyRequested(cli)) return .{};
    if (!haPrimaryRequested(cli)) return error.HARetentionPolicyRequiresPrimary;
    return .{
        .max_lag_lsn = cli.ha_retention_max_lag_lsn orelse 0,
        .max_retained_bytes = cli.ha_retention_max_retained_bytes orelse 0,
        .max_retained_age_ns = cli.ha_retention_max_retained_age_ns orelse 0,
    };
}

fn validateHASyncPolicy(policy: antfly.ha.primary.SyncPolicy) !void {
    if (policy.required == 0) return error.InvalidHASyncPolicy;
    if (policy.mode == .async) return;
    if (policy.standby_names.len == 0) return error.InvalidHASyncPolicy;
    if (policy.selection != .all and policy.required > policy.standby_names.len) {
        return error.InvalidHASyncPolicy;
    }
}

fn haPrimaryIdentity(cli: CliConfig) !antfly.ha.primary.Identity {
    return .{
        .cluster_id = cli.ha_cluster_id orelse return error.HAClusterIdMissing,
        .shard_id = cli.ha_shard_id orelse 0,
        .table_id = cli.ha_table_id orelse 0,
        .timeline_id = cli.ha_timeline_id orelse return error.HATimelineIdMissing,
        .epoch = cli.ha_epoch orelse return error.HAEpochMissing,
    };
}

fn openHAPrimaryFromCli(alloc: std.mem.Allocator, io: std.Io, cli: CliConfig) !?antfly.ha.primary.Primary {
    if (!haPrimaryRequested(cli)) return null;
    const log_path = cli.ha_primary_log orelse return error.HAPrimaryLogMissing;
    const slots_path = cli.ha_primary_slots orelse return error.HAPrimarySlotsMissing;
    if (cli.ha_primary_node_id == null) return error.HAPrimaryNodeIdMissing;

    try ensureParent(io, log_path);
    try ensureParent(io, slots_path);

    const log_z = try alloc.dupeZ(u8, log_path);
    defer alloc.free(log_z);
    const slots_z = try alloc.dupeZ(u8, slots_path);
    defer alloc.free(slots_z);

    return try antfly.ha.primary.Primary.open(alloc, log_z.ptr, slots_z.ptr, try haPrimaryIdentity(cli), .{});
}

fn haStandbyIdentity(cli: CliConfig) !antfly.ha.standby.Identity {
    return .{
        .cluster_id = cli.ha_cluster_id orelse return error.HAClusterIdMissing,
        .shard_id = cli.ha_shard_id orelse 0,
        .table_id = cli.ha_table_id orelse 0,
        .timeline_id = cli.ha_timeline_id orelse return error.HATimelineIdMissing,
        .epoch = cli.ha_epoch orelse return error.HAEpochMissing,
    };
}

fn openHAStandbyFromCli(alloc: std.mem.Allocator, io: std.Io, cli: CliConfig) !?antfly.ha.standby.Standby {
    if (!haStandbyRequested(cli)) return null;
    const log_path = cli.ha_standby_log orelse return error.HAStandbyLogMissing;
    const progress_path = cli.ha_standby_progress orelse return error.HAStandbyProgressMissing;
    if (cli.ha_standby_node_id == null) return error.HAStandbyNodeIdMissing;

    try ensureParent(io, log_path);
    try ensureParent(io, progress_path);

    const log_z = try alloc.dupeZ(u8, log_path);
    defer alloc.free(log_z);
    const progress_z = try alloc.dupeZ(u8, progress_path);
    defer alloc.free(progress_z);

    return try antfly.ha.standby.Standby.open(alloc, log_z.ptr, progress_z.ptr, try haStandbyIdentity(cli), .{});
}

fn openHAFenceStoreFromCli(alloc: std.mem.Allocator, io: std.Io, cli: CliConfig) !?antfly.ha.fencing.Store {
    const fence_wal_path = cli.ha_fence_wal orelse return null;
    if (!haPrimaryRequested(cli) and !haStandbyRequested(cli)) return error.HARoleMissing;

    try ensureParent(io, fence_wal_path);

    const fence_wal_z = try alloc.dupeZ(u8, fence_wal_path);
    defer alloc.free(fence_wal_z);

    return try antfly.ha.fencing.Store.open(alloc, fence_wal_z.ptr, .{});
}

fn openHAFormerPrimaryLogFromCli(alloc: std.mem.Allocator, io: std.Io, cli: CliConfig) !?antfly.ha.replication_log.ReplicationLog {
    const former_primary_log_path = cli.ha_former_primary_log orelse return null;
    if (!haPrimaryRequested(cli) and !haStandbyRequested(cli)) return error.HARoleMissing;
    if (cli.ha_primary_log) |primary_log_path| {
        if (std.mem.eql(u8, former_primary_log_path, primary_log_path)) return null;
    }
    if (cli.ha_standby_log) |standby_log_path| {
        if (std.mem.eql(u8, former_primary_log_path, standby_log_path)) return null;
    }

    try ensureParent(io, former_primary_log_path);

    const former_primary_log_z = try alloc.dupeZ(u8, former_primary_log_path);
    defer alloc.free(former_primary_log_z);

    return try antfly.ha.replication_log.ReplicationLog.open(former_primary_log_z.ptr, .{});
}

fn resolveAdminBearerTokenFromCli(alloc: std.mem.Allocator, cli: CliConfig) !?[]u8 {
    const raw_env_var = cli.admin_token_env orelse return null;
    const env_var = std.mem.trim(u8, raw_env_var, " \t\r\n");
    if (env_var.len == 0) return error.AdminTokenEnvMissing;
    if (!antfly.ha.validation.isEnvVarName(env_var)) return error.AdminTokenEnvInvalid;

    const env_var_z = try alloc.dupeZ(u8, env_var);
    defer alloc.free(env_var_z);

    const raw_token_z = std.c.getenv(env_var_z.ptr) orelse return error.AdminTokenMissing;
    const token = std.mem.trim(u8, std.mem.span(raw_token_z), " \t\r\n");
    if (token.len == 0) return error.AdminTokenMissing;
    return try alloc.dupe(u8, token);
}

fn ensureDirPath(io: std.Io, dir_path: []const u8) !void {
    try std.Io.Dir.cwd().createDirPath(io, dir_path);
}

fn ensureParent(io: std.Io, file_path: []const u8) !void {
    if (std.fs.path.dirname(file_path)) |parent| {
        var dir = std.Io.Dir.cwd().openDir(io, parent, .{}) catch |err| switch (err) {
            error.FileNotFound => {
                try std.Io.Dir.cwd().createDirPath(io, parent);
                return;
            },
            else => return err,
        };
        dir.close(io);
    }
}

fn resolveAuthEnabled(cli: CliConfig, cfg: ?*const antfly.common.config.Config) bool {
    if (cli.auth_enabled) |value| return value;
    if (cfg) |loaded| return loaded.auth_enabled;
    return false;
}

fn resolveInferenceModelsDir(cli: CliConfig, cfg: ?*const antfly.common.config.Config) ?[]const u8 {
    if (cli.inference_models_dir) |value| return value;
    if (cfg) |loaded| return loaded.inference.models_dir;
    return null;
}

fn parseInferenceKeepAliveMs(raw: []const u8) !u64 {
    if (std.mem.eql(u8, raw, "0")) return 0;
    if (raw.len == 0) return error.InvalidInferenceModelCacheConfig;
    var i: usize = 0;
    var total_ns: u64 = 0;
    while (i < raw.len) {
        const start = i;
        while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) {}
        if (i == start) return error.InvalidInferenceModelCacheConfig;
        const value = std.fmt.parseUnsigned(u64, raw[start..i], 10) catch
            return error.InvalidInferenceModelCacheConfig;
        const unit_ns: u64 = if (std.mem.startsWith(u8, raw[i..], "ms")) blk: {
            i += 2;
            break :blk std.time.ns_per_ms;
        } else if (i < raw.len and raw[i] == 's') blk: {
            i += 1;
            break :blk std.time.ns_per_s;
        } else if (i < raw.len and raw[i] == 'm') blk: {
            i += 1;
            break :blk std.time.ns_per_min;
        } else if (i < raw.len and raw[i] == 'h') blk: {
            i += 1;
            break :blk std.time.ns_per_hour;
        } else return error.InvalidInferenceModelCacheConfig;
        const part_ns = std.math.mul(u64, value, unit_ns) catch
            return error.InvalidInferenceModelCacheConfig;
        total_ns = std.math.add(u64, total_ns, part_ns) catch
            return error.InvalidInferenceModelCacheConfig;
    }
    if (total_ns == 0) return 0;
    return @max(@as(u64, 1), total_ns / std.time.ns_per_ms);
}

test "standalone inference keep alive parses compound durations and zero" {
    try std.testing.expectEqual(@as(u64, 0), try parseInferenceKeepAliveMs("0"));
    try std.testing.expectEqual(@as(u64, 0), try parseInferenceKeepAliveMs("0s"));
    try std.testing.expectEqual(
        @as(u64, 90_000),
        try parseInferenceKeepAliveMs("1m30s"),
    );
    try std.testing.expectError(
        error.InvalidInferenceModelCacheConfig,
        parseInferenceKeepAliveMs("forever"),
    );
}

fn resolveInferenceMlDir(cli: CliConfig, cfg: ?*const antfly.common.config.Config) ?[]const u8 {
    if (cli.inference_ml_dir) |value| return value;
    if (cfg) |loaded| return loaded.inference.ml_dir;
    return null;
}

const ResolvedWarmModels = struct {
    items: []const inference.server.WarmModel,
    owned: bool = false,

    fn deinit(self: *ResolvedWarmModels, alloc: std.mem.Allocator) void {
        if (self.owned and self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }
};

fn resolveInferenceWarmModels(
    alloc: std.mem.Allocator,
    cli: CliConfig,
    cfg: ?*const antfly.common.config.Config,
) !ResolvedWarmModels {
    if (cli.inference_preload_models.items.len > 0) {
        return .{ .items = cli.inference_preload_models.items };
    }
    const loaded = cfg orelse return .{ .items = &.{} };
    if (loaded.inference.preload.len == 0) return .{ .items = &.{} };

    const out = try alloc.alloc(inference.server.WarmModel, loaded.inference.preload.len);
    errdefer alloc.free(out);
    for (loaded.inference.preload, 0..) |model, i| {
        out[i] = .{
            .kind = parsePreloadModelKind(model.kind) orelse return error.InvalidConfig,
            .name = model.name,
            .backend = antfly.inference_runtime.parseOptionalBackendType(model.backend) catch return error.InvalidConfig,
            .format = model.format,
            .quantization = model.quantization,
        };
    }
    return .{ .items = out, .owned = true };
}

fn resolveInferenceBudgetOverrides(cli: CliConfig) antfly.inference_runtime.ServerBudgetOverrides {
    return .{
        .host_limit_bytes = mbToBytes(cli.inference_host_budget_mb),
        .backend_limit_bytes = mbToBytes(cli.inference_backend_budget_mb),
        .combined_limit_bytes = mbToBytes(cli.inference_combined_budget_mb),
        .kv_limit_bytes = mbToBytes(cli.inference_kv_budget_mb),
        .scratch_limit_bytes = mbToBytes(cli.inference_scratch_budget_mb),
    };
}

fn mbToBytes(value: usize) usize {
    return value * 1024 * 1024;
}

fn printUsage() void {
    std.debug.print(
        \\Usage: antfly standalone [options]
        \\
        \\Options:
        \\  --config <path>                       JSON common config file
        \\  --host <host>                         Public API host (default: 127.0.0.1)
        \\  --port <port>                         Public API port (default: 8080)
        \\  --id <node-id>                        Local node id (default: 1)
        \\  --health <true|false>                 Enable health/metrics server (default: true)
        \\  --health-port <port>                  Dedicated health/metrics port on --host (default: 4200)
        \\  --experimental                        Enable experimental A2A protocol surfaces
        \\  --ard-base-url <url>                  Absolute public base URL for ARD catalog artifact links
        \\  --ard-publisher-domain <name>         ARD did:web publisher domain (default: antfly.local)
        \\  --ard-display-name <name>             ARD catalog host display name (default: Antfly)
        \\  --ard-public-catalog <bool>           Publish anonymous /.well-known ARD bootstrap when auth is enabled
        \\  --control-tick-ms <ms>                Control scheduling interval, 1-60000 (default: 100)
        \\  --models-dir <path>                   Embedded AI models directory (default: ~/.antfly/inference/models)
        \\  --ml-dir <path>                       Embedded Traditional ML directory (default: ~/.antfly/inference/ml)
        \\  --inference-host-budget-mb <n>        Embedded inference native generation host budget override
        \\  --inference-backend-budget-mb <n>     Embedded inference native generation backend budget override
        \\  --inference-combined-budget-mb <n>    Embedded inference native generation combined budget override
        \\  --inference-kv-budget-mb <n>          Embedded inference native generation KV cache budget override
        \\  --inference-scratch-budget-mb <n>     Embedded inference native generation scratch budget override
        \\  --preload-model <kind:name|kind:backend:name> Preload and warm an embedded model before serving
        \\  --data-dir <path>                     Local Antfly data directory root
        \\  --storage-engine lite                 Use the single-file Lite engine
        \\  --storage-path <path.aflite>          Lite database path (required with Lite)
        \\  --fsync <true|false>                  Lite commit durability (default: true)
        \\  --replica-root-dir <path>             Replica root directory
        \\  --replica-catalog-path <path>         Replica catalog file path
        \\  --snapshot-root-dir <path>            Snapshot root directory
        \\  --extension-package-store <path>      Extension package store directory
        \\  --secret-store-path <path>            Antfly secrets.json file path; repeat for fallback layers
        \\  --ha-primary-log <path>               Enable HA primary WAL/admin API with this replication log path
        \\  --ha-primary-slots <path>             HA primary replication slot store path
        \\  --ha-primary-node-id <id>             HA primary node id for typed admin receipts
        \\  --ha-fence-wal <path>                 Durable HA promotion fence WAL path
        \\  --ha-former-primary-log <path>        Durable HA log used by former-primary rewind admin workflows
        \\  --admin-token-env <name>              Require Authorization: Bearer token from this environment variable for admin and HA APIs
        \\  --ha-retention-max-lag-lsn <n>        HA primary marks slots reseed-required after this LSN retention lag
        \\  --ha-retention-max-retained-bytes <n> HA primary marks oldest slots reseed-required above this retained WAL byte cap
        \\  --ha-retention-max-retained-age-ns <n> HA primary marks oldest slots reseed-required above this retained WAL age cap
        \\  --ha-sync-mode <mode>                 HA primary sync mode: async, remote-write, remote-apply
        \\  --ha-sync-selection <selection>       HA sync standby selection: any, first, all
        \\  --ha-sync-required <n>                HA sync required standby acknowledgements
        \\  --ha-sync-standby <name>              HA sync standby name; repeat for multiple standbys
        \\  --ha-sync-failure <policy>            HA sync failure policy: block, fail-closed, degrade-to-async
        \\  --ha-standby-log <path>               Enable HA standby admin API with this received replication log path
        \\  --ha-standby-progress <path>          HA standby durable receive/apply progress WAL path
        \\  --ha-standby-node-id <id>             HA standby node id for typed admin receipts
        \\  --ha-standby-upstream-url <url>       Upstream primary URL for continuous standby pull/apply
        \\  --ha-standby-slot <name>              Upstream replication slot name for continuous standby pull/apply
        \\  --ha-cluster-id <id>                  HA replicated cluster id
        \\  --ha-shard-id <id>                    HA replicated shard id (default: 0)
        \\  --ha-table-id <id>                    HA replicated table id (default: 0)
        \\  --ha-timeline-id <id>                 HA primary timeline id
        \\  --ha-epoch <id>                       HA primary epoch
        \\  -h, --help                            Show this help
        \\
    , .{});
}

fn parseBoolFlag(raw: []const u8) ?bool {
    if (std.mem.eql(u8, raw, "true")) return true;
    if (std.mem.eql(u8, raw, "false")) return false;
    return null;
}

fn parseHASyncDurabilityMode(raw: []const u8) !antfly.ha.primary.DurabilityMode {
    if (std.mem.eql(u8, raw, "async")) return .async;
    if (std.mem.eql(u8, raw, "remote_write") or std.mem.eql(u8, raw, "remote-write")) return .remote_write;
    if (std.mem.eql(u8, raw, "remote_apply") or std.mem.eql(u8, raw, "remote-apply")) return .remote_apply;
    return error.InvalidHASyncMode;
}

fn parseHASyncStandbySelection(raw: []const u8) !antfly.ha.primary.StandbySelection {
    if (std.mem.eql(u8, raw, "any")) return .any;
    if (std.mem.eql(u8, raw, "first")) return .first;
    if (std.mem.eql(u8, raw, "all")) return .all;
    return error.InvalidHASyncSelection;
}

fn parseHASyncFailurePolicy(raw: []const u8) !antfly.ha.primary.FailurePolicy {
    if (std.mem.eql(u8, raw, "block")) return .block;
    if (std.mem.eql(u8, raw, "fail_closed") or std.mem.eql(u8, raw, "fail-closed")) return .fail_closed;
    if (std.mem.eql(u8, raw, "degrade_to_async") or std.mem.eql(u8, raw, "degrade-to-async")) return .degrade_to_async;
    return error.InvalidHASyncFailurePolicy;
}

fn parsePositiveUsize(raw: []const u8) !usize {
    const value = std.fmt.parseInt(usize, raw, 10) catch return error.InvalidArguments;
    if (value == 0) return error.InvalidHASyncPolicy;
    return value;
}

fn parsePositiveU64(raw: []const u8) !u64 {
    const value = std.fmt.parseInt(u64, raw, 10) catch return error.InvalidArguments;
    if (value == 0) return error.InvalidHARetentionPolicy;
    return value;
}

const RecordingRouteMethod = enum {
    get,
    post,
    put,
    delete,
};

const RecordingRoute = struct {
    method: RecordingRouteMethod,
    path: []u8,
};

const RecordingServer = struct {
    allocator: std.mem.Allocator,
    routes: std.ArrayListUnmanaged(RecordingRoute) = .empty,

    fn deinit(self: *@This()) void {
        for (self.routes.items) |route| self.allocator.free(route.path);
        self.routes.deinit(self.allocator);
    }

    fn append(self: *@This(), method: RecordingRouteMethod, comptime path: []const u8) !void {
        try self.routes.append(self.allocator, .{
            .method = method,
            .path = try self.allocator.dupe(u8, path),
        });
    }

    pub fn get(self: *@This(), comptime path: []const u8, _: httpx.Handler) !void {
        try self.append(.get, path);
    }

    pub fn post(self: *@This(), comptime path: []const u8, _: httpx.Handler) !void {
        try self.append(.post, path);
    }

    pub fn put(self: *@This(), comptime path: []const u8, _: httpx.Handler) !void {
        try self.append(.put, path);
    }

    pub fn delete(self: *@This(), comptime path: []const u8, _: httpx.Handler) !void {
        try self.append(.delete, path);
    }

    fn hasRoute(self: *const @This(), method: RecordingRouteMethod, path: []const u8) bool {
        for (self.routes.items) |route| {
            if (route.method == method and std.mem.eql(u8, route.path, path)) return true;
        }
        return false;
    }
};

test "standalone runtime module compiles" {
    _ = run;
    _ = runFromIterator;
}

test "standalone Lite enforces one shard and one replica" {
    const lite = try deriveStandaloneTableRecord(.lite, "docs", .{});
    try std.testing.expectEqual(@as(u32, 1), lite.min_ranges);
    try std.testing.expectEqual(@as(u32, 1), lite.desired_replica_count);
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        deriveStandaloneTableRecord(.lite, "split", .{ .num_shards = 2 }),
    );

    const local = try deriveStandaloneTableRecord(.local, "local", .{ .num_shards = 2 });
    try std.testing.expectEqual(@as(u32, 2), local.min_ranges);
    try std.testing.expectEqual(@as(u32, 1), local.desired_replica_count);
}

test "standalone Lite adoption preserves deterministic embedded document identity" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/identity-adoption.aflite", .{tmp.sub_path});
    defer alloc.free(path);

    {
        var embedded = try antfly.lite.connection.Connection.create(alloc, path, true);
        defer embedded.close();
        try embedded.db.batch(.{ .writes = &.{.{ .key = "doc:portable", .value = "{\"body\":\"portable\"}" }} });
    }

    var backend = try antfly.lite.backend.Handle.open(alloc, path, .{});
    defer backend.deinit();
    const target = antfly.lite.connection.embeddedRootIdentity();
    const default_table = try deriveStandaloneTableRecord(.lite, "default", .{});
    const default_range = antfly.public_api.tables.deriveInitialRange(default_table);
    try std.testing.expectEqual(default_table.table_id, target.table_id);
    try std.testing.expectEqual(default_range.group_id, target.shard_id);
    try std.testing.expectEqual(default_range.range_id, target.range_id);
    const namespace = try std.fmt.allocPrint(alloc, "group-{d}/table-db", .{target.shard_id});
    defer alloc.free(namespace);
    try backend.adoptEmbeddedRootAsNamespace(namespace);
    try verifyAdoptedLiteIdentity(alloc, &backend, namespace, target);
    // Retry after a crash boundary is a no-op.
    try verifyAdoptedLiteIdentity(alloc, &backend, namespace, target);

    var opts = antfly.db.OpenOptions{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .identity_namespace = target,
    };
    const restored_namespace = try std.fmt.allocPrint(alloc, "/restored/root/{s}", .{namespace});
    defer alloc.free(restored_namespace);
    try backend.configureDbOpenOptionsForNamespace(&opts, restored_namespace);
    var adopted = try antfly.db.DB.open(alloc, namespace, opts);
    defer adopted.close();
    try std.testing.expect(adopted.core.identity_namespace.eql(target));
    const value = (try adopted.get(alloc, "doc:portable")) orelse return error.MissingAdoptedDocument;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("{\"body\":\"portable\"}", value);
}

test "standalone validates effective Lite CLI and config settings" {
    const lite_cli = CliConfig{ .storage_engine = .lite, .storage_path = "data.aflite" };
    try validateEffectiveStandaloneStorage(lite_cli, .lite, "data.aflite", null);
    try std.testing.expectError(
        error.InvalidLiteStoragePath,
        validateEffectiveStandaloneStorage(lite_cli, .lite, "data.db", null),
    );
    try std.testing.expectError(
        error.InvalidArguments,
        validateEffectiveStandaloneStorage(.{ .storage_path = "unused.aflite" }, .local, null, null),
    );

    var distributed = try antfly.common.config.Config.parseFromSlice(std.testing.allocator,
        \\{"deployment_mode":"distributed","storage":{"engine":"local","local":{"base_dir":"data"}}}
    );
    defer distributed.deinit();
    try std.testing.expectError(
        error.LiteHorizontalShardingUnsupported,
        validateEffectiveStandaloneStorage(lite_cli, .lite, "data.aflite", &distributed),
    );
}

test "standalone runtime local generator accepts media url data uris" {
    const alloc = std.testing.allocator;
    const messages = [_]antfly.inference.ChatMessage{.{
        .role = .user,
        .content = .{ .parts = &.{
            .{ .text = "describe" },
            .{ .media = .{
                .url = "data:image/png;base64,AQI=",
                .mime_type = "image/png",
            } },
        } },
    }};

    var converted = try convertLocalGenerateMessages(alloc, &messages);
    defer converted.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), converted.messages.len);
    const message = converted.messages[0];
    try std.testing.expectEqualStrings("describe", message.content);
    try std.testing.expectEqual(@as(usize, 1), message.image_bytes.?.len);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2 }, message.image_bytes.?[0]);
    try std.testing.expectEqual(@as(usize, 2), message.content_parts.?.len);
    try std.testing.expectEqual(@as(usize, 0), message.content_parts.?[1].image);
}

test "standalone runtime leaves auth disabled unless config or cli enables it" {
    try std.testing.expect(!resolveAuthEnabled(.{}, null));
    try std.testing.expect(resolveAuthEnabled(.{ .auth_enabled = true }, null));
    try std.testing.expect(!resolveAuthEnabled(.{ .auth_enabled = false }, null));
}

test "standalone runtime parses experimental flag" {
    const argv = [_][*:0]const u8{"--experimental"};
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var parsed = try parseCli(std.testing.allocator, &iter);
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expect(parsed.experimental);
}

test "standalone bridge shared adapter preserves protocol headers and absent body" {
    const alloc = std.testing.allocator;

    var request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/mcp/v1/extensions/memoryaf");
    defer request.deinit();
    try request.setHeader("Mcp-Session-Id", "session-123");

    var ctx = httpx.Context.init(alloc, undefined, &request);
    defer ctx.deinit();

    var converted = try AntflyApiHandler.httpRequestFromContext(&ctx, null);
    defer converted.deinit();

    try std.testing.expectEqualStrings("session-123", converted.value.header("mcp-session-id") orelse return error.MissingHeader);
    try std.testing.expectEqualStrings("", converted.value.body);
}

test "standalone protocol bridge releases converted request headers" {
    const FakeSource = struct {
        fn iface(_: *@This()) antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{ .status = status },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
    };

    var source = FakeSource{};
    var api_server = antfly.public_api.http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{},
        source.iface(),
        null,
        null,
    );
    defer api_server.deinit();

    const previous_api_server = active_api_server;
    active_api_server = &api_server;
    defer active_api_server = previous_api_server;

    var request = try httpx.Request.init(std.testing.allocator, .GET, "http://127.0.0.1/mcp/v1");
    defer request.deinit();
    try request.setHeader("Mcp-Protocol-Version", "2025-06-18");

    var ctx = httpx.Context.init(std.testing.allocator, undefined, &request);
    defer ctx.deinit();

    var response = try protocolBridgeHandler(&ctx);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 404), response.status.code);
}

test "standalone runtime local replica reconcile permit blocks only active startup catch-up" {
    var data_server = antfly.data.runtime.DataServer{
        .alloc = std.testing.allocator,
        .provisioned_storage = undefined,
        .read_source = undefined,
        .write_source = undefined,
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(1),
        .listener_cfg = undefined,
    };

    data_server.provisioned_startup_catch_up_active.store(false, .monotonic);
    data_server.provisioned_startup_catch_up_dirty.store(true, .monotonic);
    data_server.last_provision_metadata_epoch = null;
    data_server.last_provision_fingerprint = null;
    try std.testing.expect(runLocalReplicaRootReconcilePermitHook(&data_server));

    data_server.last_provision_metadata_epoch = 17;
    try std.testing.expect(runLocalReplicaRootReconcilePermitHook(&data_server));

    data_server.last_provision_fingerprint = 99;
    try std.testing.expect(runLocalReplicaRootReconcilePermitHook(&data_server));

    data_server.provisioned_startup_catch_up_active.store(true, .monotonic);
    data_server.provisioned_startup_catch_up_dirty.store(false, .monotonic);
    try std.testing.expect(!runLocalReplicaRootReconcilePermitHook(&data_server));

    data_server.provisioned_startup_catch_up_active.store(false, .monotonic);
    data_server.provisioned_startup_catch_up_dirty.store(false, .monotonic);
    try std.testing.expect(runLocalReplicaRootReconcilePermitHook(&data_server));
}

test "standalone runtime registers internal group routes explicitly" {
    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try registerInternalGroupRoutes(&server);

    const routes = antfly.public_api.http_routes.Routes;
    const group_prefix = routes.internal_groups_prefix ++ ":group_id";
    const table_prefix = group_prefix ++ "/tables/:table_name";
    const internal_table_prefix = routes.internal_tables_prefix ++ ":table_name";

    try std.testing.expect(server.hasRoute(.get, group_prefix ++ routes.group_db_median_key_suffix));
    try std.testing.expect(server.hasRoute(.get, table_prefix ++ routes.documents_marker ++ ":key"));
    try std.testing.expect(server.hasRoute(.get, internal_table_prefix ++ routes.repair_jobs_marker ++ ":job_id" ++ routes.repair_attempts_marker ++ ":attempt_id" ++ routes.repair_cancel_state_suffix));

    try std.testing.expect(server.hasRoute(.post, internal_table_prefix ++ routes.corrupt_embedding_artifact_suffix));
    try std.testing.expect(server.hasRoute(.post, group_prefix ++ routes.shard_ops_observe_split_suffix));
    try std.testing.expect(server.hasRoute(.post, group_prefix ++ routes.shard_ops_observe_merge_suffix));
    try std.testing.expect(server.hasRoute(.post, group_prefix ++ routes.shard_ops_execute_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.documents_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.graph_expand_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.graph_hydrate_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.text_stats_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.join_job_state_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.join_finalize_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.join_rows_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.join_unmatched_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.join_partition_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.query_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.batch_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.txn_begin_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.txn_prepare_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.txn_resolve_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.txn_status_suffix));
}

test "standalone runtime registers HA admin bridge routes before antfarm catch-all" {
    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try registerHAAdminRoutes(&server);
    try registerAntfarmRoutes(&server);

    const ha_base = antfly.admin.routes.ha;
    const ha_prefix = antfly.admin.routes.ha ++ "/*";
    try std.testing.expect(server.hasRoute(.get, ha_base));
    try std.testing.expect(server.hasRoute(.post, ha_base));
    try std.testing.expect(server.hasRoute(.put, ha_base));
    try std.testing.expect(server.hasRoute(.delete, ha_base));
    try std.testing.expect(server.hasRoute(.get, ha_prefix));
    try std.testing.expect(server.hasRoute(.post, ha_prefix));
    try std.testing.expect(server.hasRoute(.put, ha_prefix));
    try std.testing.expect(server.hasRoute(.delete, ha_prefix));
    try std.testing.expect(server.hasRoute(.get, "/*"));
}

test "standalone runtime registers HA internal replication bridge routes before antfarm catch-all" {
    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try registerHAInternalRoutes(&server);
    try registerAntfarmRoutes(&server);

    const ha_base = antfly.internal.routes.ha;
    const ha_prefix = antfly.internal.routes.ha ++ "/*";
    try std.testing.expect(server.hasRoute(.get, ha_base));
    try std.testing.expect(server.hasRoute(.post, ha_base));
    try std.testing.expect(server.hasRoute(.put, ha_base));
    try std.testing.expect(server.hasRoute(.delete, ha_base));
    try std.testing.expect(server.hasRoute(.get, ha_prefix));
    try std.testing.expect(server.hasRoute(.post, ha_prefix));
    try std.testing.expect(server.hasRoute(.put, ha_prefix));
    try std.testing.expect(server.hasRoute(.delete, ha_prefix));
    try std.testing.expect(server.hasRoute(.get, "/*"));
}

test "standalone runtime registers mcp routes before antfarm catch-all" {
    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try registerMcpRoutes(&server);
    try registerAntfarmRoutes(&server);

    const routes = antfly.public_api.http_routes.Routes;
    try std.testing.expect(server.hasRoute(.get, routes.mcp_v1));
    try std.testing.expect(server.hasRoute(.post, routes.mcp_v1));
    try std.testing.expect(server.hasRoute(.delete, routes.mcp_v1));
    try std.testing.expect(server.hasRoute(.get, routes.mcp_v1_prefix ++ "*"));
    try std.testing.expect(server.hasRoute(.post, routes.mcp_v1_prefix ++ "*"));
    try std.testing.expect(server.hasRoute(.delete, routes.mcp_v1_prefix ++ "*"));
    try std.testing.expect(server.hasRoute(.get, "/*"));
}

test "standalone runtime registers A2A routes before antfarm catch-all" {
    const routes = antfly.public_api.http_routes.Routes;

    var disabled = RecordingServer{ .allocator = std.testing.allocator };
    defer disabled.deinit();
    try registerExperimentalRoutes(&disabled, false);
    try registerAntfarmRoutes(&disabled);
    try std.testing.expect(!disabled.hasRoute(.post, routes.a2a));
    try std.testing.expect(!disabled.hasRoute(.get, routes.agent_card));
    try std.testing.expect(!disabled.hasRoute(.get, routes.agent_card_legacy));
    try std.testing.expect(disabled.hasRoute(.get, "/*"));

    var enabled = RecordingServer{ .allocator = std.testing.allocator };
    defer enabled.deinit();
    try registerExperimentalRoutes(&enabled, true);
    try registerAntfarmRoutes(&enabled);
    try std.testing.expect(enabled.hasRoute(.post, routes.a2a));
    try std.testing.expect(enabled.hasRoute(.get, routes.agent_card));
    try std.testing.expect(enabled.hasRoute(.get, routes.agent_card_legacy));
    try std.testing.expect(enabled.hasRoute(.get, "/*"));
}

test "standalone runtime registers ARD routes before antfarm catch-all" {
    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try registerArdRoutes(&server);
    try registerAntfarmRoutes(&server);

    const routes = antfly.public_api.http_routes.Routes;
    try std.testing.expect(server.hasRoute(.get, routes.ai_catalog));
    try std.testing.expect(server.hasRoute(.get, routes.ard_v1));
    try std.testing.expect(server.hasRoute(.get, routes.ard_v1 ++ "/*"));
    try std.testing.expect(server.hasRoute(.post, routes.ard_v1_search));
    try std.testing.expect(server.hasRoute(.post, routes.ard_v1_explore));
    try std.testing.expect(server.hasRoute(.get, "/*"));
}

test "standalone runtime registers extension routes before antfarm catch-all" {
    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try registerExtensionRoutes(&server);
    try registerAntfarmRoutes(&server);

    const routes = antfly.public_api.http_routes.Routes;
    try std.testing.expect(server.hasRoute(.get, routes.extensions_v1));
    try std.testing.expect(server.hasRoute(.get, routes.extensions_v1_packages));
    try std.testing.expect(server.hasRoute(.get, routes.extensions_v1_packages_prefix ++ "*"));
    try std.testing.expect(server.hasRoute(.get, routes.extensions_v1_installed));
    try std.testing.expect(server.hasRoute(.get, routes.extensions_v1_installed_prefix ++ "*"));
    try std.testing.expect(server.hasRoute(.post, routes.extensions_v1_installed_prefix ++ "*"));
    try std.testing.expect(server.hasRoute(.put, routes.extensions_v1_installed_prefix ++ "*"));
    try std.testing.expect(server.hasRoute(.get, "/*"));
}

test "standalone runtime registers antfarm static routes" {
    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try registerAntfarmRoutes(&server);

    try std.testing.expect(server.hasRoute(.get, "/"));
    try std.testing.expect(server.hasRoute(.get, "/assets/*"));
    try std.testing.expect(server.hasRoute(.get, "/fonts/*"));
    try std.testing.expect(server.hasRoute(.get, "/*"));
}

test "standalone runtime antfarm path guards keep api routes reserved" {
    try std.testing.expect(isAntfarmReservedPath("/db/v1/tables"));
    try std.testing.expect(isAntfarmReservedPath("/ai/v1/models"));
    try std.testing.expect(isAntfarmReservedPath("/antfly/readyz"));
    try std.testing.expect(isAntfarmReservedPath("/admin/v1/ha/primary/status"));
    try std.testing.expect(isAntfarmReservedPath("/a2a"));
    try std.testing.expect(isAntfarmReservedPath("/.well-known/agent-card.json"));
    try std.testing.expect(isAntfarmReservedPath("/extensions/v1/packages"));
    try std.testing.expect(isAntfarmReservedPath("/unknown/v1/status"));
    try std.testing.expect(isAntfarmReservedPath("/unknown/v12/status"));
    try std.testing.expect(!isAntfarmReservedPath("/models/version/status"));
    try std.testing.expect(!isAntfarmReservedPath("/models"));
    try std.testing.expect(hasUnsafeStaticPath("../index.html"));
    try std.testing.expect(hasUnsafeStaticPath("%2e%2e/index.html"));
    try std.testing.expect(!hasUnsafeStaticPath("assets/index.js"));
}

test "parse cli accepts config path" {
    var argv = [_][*:0]const u8{ "--config", "antfly.json" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("antfly.json", cfg.config_path.?);
}

test "parse cli accepts secret store path" {
    var argv = [_][*:0]const u8{ "--secret-store-path", "/run/antfly/secrets/secrets.json" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("/run/antfly/secrets/secrets.json", cfg.secret_store_paths.items[0]);
}

test "parse cli accepts extension package store path" {
    var argv = [_][*:0]const u8{ "--extension-package-store", "/opt/antfly/extensions" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("/opt/antfly/extensions", cfg.extension_package_store_dir.?);
}

test "parse cli accepts ARD identity flags" {
    var argv = [_][*:0]const u8{
        "--ard-publisher-domain",
        "tenant.example.com",
        "--ard-base-url",
        "https://tenant.example.com",
        "--ard-display-name",
        "Tenant Antfly",
        "--ard-public-catalog",
        "true",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("https://tenant.example.com", cfg.ard_base_url.?);
    try std.testing.expectEqualStrings("tenant.example.com", cfg.ard_publisher_domain.?);
    try std.testing.expectEqualStrings("Tenant Antfly", cfg.ard_display_name.?);
    try std.testing.expect(cfg.ard_public_catalog_enabled);
}

test "parse cli accepts canonical host port and models dir flags" {
    var argv = [_][*:0]const u8{
        "--host",
        "127.0.0.1",
        "--port",
        "8080",
        "--models-dir",
        "/tmp/models",
        "--ml-dir",
        "/tmp/ml",
        "--preload-model",
        "generator:metal:gemma-e2b",
        "--data-dir",
        "/tmp/antfly-data",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("127.0.0.1", cfg.bind_host.?);
    try std.testing.expectEqual(@as(u16, 8080), cfg.bind_port.?);
    try std.testing.expectEqualStrings("/tmp/models", cfg.inference_models_dir.?);
    try std.testing.expectEqualStrings("/tmp/ml", cfg.inference_ml_dir.?);
    try std.testing.expectEqual(@as(usize, 1), cfg.inference_preload_models.items.len);
    try std.testing.expectEqual(inference.server.WarmModelKind.generator, cfg.inference_preload_models.items[0].kind);
    try std.testing.expectEqualStrings("gemma-e2b", cfg.inference_preload_models.items[0].name);
    try std.testing.expectEqual(inference.backends.BackendType.metal, cfg.inference_preload_models.items[0].backend.?);
    try std.testing.expectEqualStrings("/tmp/antfly-data", cfg.data_dir.?);
}

test "parse cli accepts HA primary runtime flags" {
    var argv = [_][*:0]const u8{
        "--ha-primary-log",
        "/tmp/ha-primary.log",
        "--ha-primary-slots",
        "/tmp/ha-slots.wal",
        "--ha-primary-node-id",
        "primary-a",
        "--ha-fence-wal",
        "/tmp/ha-fence.wal",
        "--ha-former-primary-log",
        "/tmp/ha-primary.log",
        "--admin-token-env",
        "ANTFLY_HA_ADMIN_TOKEN",
        "--ha-retention-max-lag-lsn",
        "500",
        "--ha-retention-max-retained-bytes",
        "8192",
        "--ha-retention-max-retained-age-ns",
        "1000000",
        "--ha-cluster-id",
        "100",
        "--ha-shard-id",
        "10",
        "--ha-table-id",
        "20",
        "--ha-timeline-id",
        "3",
        "--ha-epoch",
        "4",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expect(haPrimaryRequested(cfg));
    try std.testing.expectEqualStrings("/tmp/ha-primary.log", cfg.ha_primary_log.?);
    try std.testing.expectEqualStrings("/tmp/ha-slots.wal", cfg.ha_primary_slots.?);
    try std.testing.expectEqualStrings("primary-a", cfg.ha_primary_node_id.?);
    try std.testing.expectEqualStrings("/tmp/ha-fence.wal", cfg.ha_fence_wal.?);
    try std.testing.expectEqualStrings("/tmp/ha-primary.log", cfg.ha_former_primary_log.?);
    try std.testing.expectEqualStrings("ANTFLY_HA_ADMIN_TOKEN", cfg.admin_token_env.?);
    try std.testing.expectEqual(@as(u64, 500), cfg.ha_retention_max_lag_lsn.?);
    try std.testing.expectEqual(@as(u64, 8192), cfg.ha_retention_max_retained_bytes.?);
    try std.testing.expectEqual(@as(u64, 1000000), cfg.ha_retention_max_retained_age_ns.?);
    try std.testing.expectEqual(@as(u64, 100), cfg.ha_cluster_id.?);
    try std.testing.expectEqual(@as(u64, 10), cfg.ha_shard_id.?);
    try std.testing.expectEqual(@as(u64, 20), cfg.ha_table_id.?);
    try std.testing.expectEqual(@as(u64, 3), cfg.ha_timeline_id.?);
    try std.testing.expectEqual(@as(u64, 4), cfg.ha_epoch.?);
}

test "parse cli accepts HA primary sync policy flags" {
    var argv = [_][*:0]const u8{
        "--ha-primary-log",
        "/tmp/ha-primary.log",
        "--ha-primary-slots",
        "/tmp/ha-slots.wal",
        "--ha-primary-node-id",
        "primary-a",
        "--ha-fence-wal",
        "/tmp/ha-fence.wal",
        "--ha-cluster-id",
        "100",
        "--ha-timeline-id",
        "3",
        "--ha-epoch",
        "4",
        "--ha-sync-mode",
        "remote-apply",
        "--ha-sync-selection",
        "first",
        "--ha-sync-required",
        "2",
        "--ha-sync-standby",
        "standby-a",
        "--ha-sync-standby",
        "standby-b",
        "--ha-sync-failure",
        "fail-closed",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);

    try validateHARole(cfg);
    var sync_policy = try haSyncPolicyFromCli(std.testing.allocator, cfg);
    defer sync_policy.deinit(std.testing.allocator);

    try std.testing.expectEqual(antfly.ha.primary.DurabilityMode.remote_apply, sync_policy.policy.mode);
    try std.testing.expectEqual(antfly.ha.primary.StandbySelection.first, sync_policy.policy.selection);
    try std.testing.expectEqual(@as(usize, 2), sync_policy.policy.required);
    try std.testing.expectEqual(antfly.ha.primary.FailurePolicy.fail_closed, sync_policy.policy.failure_policy);
    try std.testing.expectEqual(@as(usize, 2), sync_policy.policy.standby_names.len);
    try std.testing.expectEqualStrings("standby-a", sync_policy.policy.standby_names[0]);
    try std.testing.expectEqualStrings("standby-b", sync_policy.policy.standby_names[1]);
}

test "parse cli treats ALL HA sync policy as all named standbys" {
    var argv = [_][*:0]const u8{
        "--ha-primary-log",
        "/tmp/ha-primary.log",
        "--ha-primary-slots",
        "/tmp/ha-primary.slots",
        "--ha-primary-node-id",
        "primary-a",
        "--ha-fence-wal",
        "/tmp/ha-fence.wal",
        "--ha-cluster-id",
        "100",
        "--ha-timeline-id",
        "3",
        "--ha-epoch",
        "4",
        "--ha-sync-mode",
        "remote-apply",
        "--ha-sync-selection",
        "all",
        "--ha-sync-standby",
        "standby-a",
        "--ha-sync-standby",
        "standby-b",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);

    try validateHARole(cfg);
    var sync_policy = try haSyncPolicyFromCli(std.testing.allocator, cfg);
    defer sync_policy.deinit(std.testing.allocator);

    try std.testing.expectEqual(antfly.ha.primary.DurabilityMode.remote_apply, sync_policy.policy.mode);
    try std.testing.expectEqual(antfly.ha.primary.StandbySelection.all, sync_policy.policy.selection);
    try std.testing.expectEqual(@as(usize, 2), sync_policy.policy.required);
    try std.testing.expectEqual(@as(usize, 2), sync_policy.policy.standby_names.len);

    cfg.ha_sync_required = 1;
    try std.testing.expectError(error.InvalidHASyncPolicy, haSyncPolicyFromCli(std.testing.allocator, cfg));
}

test "parse cli accepts HA primary retention policy flags" {
    var argv = [_][*:0]const u8{
        "--ha-primary-log",
        "/tmp/ha-primary.log",
        "--ha-primary-slots",
        "/tmp/ha-slots.wal",
        "--ha-primary-node-id",
        "primary-a",
        "--ha-fence-wal",
        "/tmp/ha-fence.wal",
        "--ha-cluster-id",
        "100",
        "--ha-timeline-id",
        "3",
        "--ha-epoch",
        "4",
        "--ha-retention-max-lag-lsn",
        "50",
        "--ha-retention-max-retained-bytes",
        "4096",
        "--ha-retention-max-retained-age-ns",
        "1000000",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);

    try validateHARole(cfg);
    const retention_policy = try haRetentionPolicyFromCli(cfg);
    try std.testing.expectEqual(@as(u64, 50), retention_policy.max_lag_lsn);
    try std.testing.expectEqual(@as(u64, 4096), retention_policy.max_retained_bytes);
    try std.testing.expectEqual(@as(u64, 1000000), retention_policy.max_retained_age_ns);
}

test "parse cli accepts HA standby runtime flags" {
    var argv = [_][*:0]const u8{
        "--ha-standby-log",
        "/tmp/ha-standby.log",
        "--ha-standby-progress",
        "/tmp/ha-standby-progress.wal",
        "--ha-standby-node-id",
        "standby-a",
        "--ha-fence-wal",
        "/tmp/ha-fence.wal",
        "--ha-standby-upstream-url",
        "http://primary.antfly.svc:8080",
        "--ha-standby-slot",
        "standby-a",
        "--ha-cluster-id",
        "100",
        "--ha-shard-id",
        "10",
        "--ha-table-id",
        "20",
        "--ha-timeline-id",
        "3",
        "--ha-epoch",
        "4",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try validateHARole(cfg);
    try std.testing.expect(!haPrimaryRequested(cfg));
    try std.testing.expect(haStandbyRequested(cfg));
    try std.testing.expectEqualStrings("/tmp/ha-standby.log", cfg.ha_standby_log.?);
    try std.testing.expectEqualStrings("/tmp/ha-standby-progress.wal", cfg.ha_standby_progress.?);
    try std.testing.expectEqualStrings("standby-a", cfg.ha_standby_node_id.?);
    try std.testing.expectEqualStrings("/tmp/ha-fence.wal", cfg.ha_fence_wal.?);
    try std.testing.expectEqualStrings("http://primary.antfly.svc:8080", cfg.ha_standby_upstream_url.?);
    try std.testing.expectEqualStrings("standby-a", cfg.ha_standby_slot.?);
    try std.testing.expectEqual(@as(u64, 100), cfg.ha_cluster_id.?);
    try std.testing.expectEqual(@as(u64, 10), cfg.ha_shard_id.?);
    try std.testing.expectEqual(@as(u64, 20), cfg.ha_table_id.?);
    try std.testing.expectEqual(@as(u64, 3), cfg.ha_timeline_id.?);
    try std.testing.expectEqual(@as(u64, 4), cfg.ha_epoch.?);

    const replication_cfg = (try haStandbyReplicationConfigFromCli(cfg)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("http://primary.antfly.svc:8080", replication_cfg.upstream_base_uri);
    try std.testing.expectEqualStrings("standby-a", replication_cfg.slot_name);
}

test "standalone HA standby replication flags require upstream and slot" {
    try std.testing.expectError(error.HAStandbySlotMissing, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary.antfly.svc:8080",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlMissing, haStandbyReplicationConfigFromCli(.{
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlMissing, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = " \t ",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbySlotMissing, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary.antfly.svc:8080",
        .ha_standby_slot = " \t ",
    }));

    try std.testing.expectError(error.HAStandbyUpstreamUrlInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "  http://primary.antfly.svc:8080 \n",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary.antfly.svc:8080/\treplication",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary antfly.svc:8080",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbySlotInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary.antfly.svc:8080",
        .ha_standby_slot = " standby-a\t",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "primary.antfly.svc:8080",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http:///replication",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "file:///tmp/primary",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbySlotInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary.antfly.svc:8080",
        .ha_standby_slot = "standby a",
    }));

    const replication_cfg = (try haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary.antfly.svc:8080",
        .ha_standby_slot = "standby-a",
    })) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("http://primary.antfly.svc:8080", replication_cfg.upstream_base_uri);
    try std.testing.expectEqualStrings("standby-a", replication_cfg.slot_name);
}

test "standalone HA string classifier distinguishes missing padded and valid values" {
    try std.testing.expectEqual(antfly.ha.validation.HAStringValidation.missing, antfly.ha.validation.classifyHAString(null));
    try std.testing.expectEqual(antfly.ha.validation.HAStringValidation.missing, antfly.ha.validation.classifyHAString(""));
    try std.testing.expectEqual(antfly.ha.validation.HAStringValidation.missing, antfly.ha.validation.classifyHAString(" \t\r\n"));
    try std.testing.expectEqual(antfly.ha.validation.HAStringValidation.padded, antfly.ha.validation.classifyHAString(" standby-a"));
    try std.testing.expectEqual(antfly.ha.validation.HAStringValidation.padded, antfly.ha.validation.classifyHAString("standby-a\n"));
    try std.testing.expectEqual(antfly.ha.validation.HAStringValidation.ok, antfly.ha.validation.classifyHAString("standby-a"));

    try std.testing.expectError(error.HAStandbySlotMissing, requireHAString(null, error.HAStandbySlotMissing, error.HAStandbySlotInvalid));
    try std.testing.expectError(error.HAStandbySlotMissing, requireHAString(" \t", error.HAStandbySlotMissing, error.HAStandbySlotInvalid));
    try std.testing.expectError(error.HAStandbySlotInvalid, requireHAString(" standby-a ", error.HAStandbySlotMissing, error.HAStandbySlotInvalid));
    try std.testing.expectEqualStrings("standby-a", try requireHAString("standby-a", error.HAStandbySlotMissing, error.HAStandbySlotInvalid));
}

test "standalone HA primary identity defaults shard and table to whole instance" {
    const identity = try haPrimaryIdentity(.{
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    });
    try std.testing.expectEqual(@as(u64, 100), identity.cluster_id);
    try std.testing.expectEqual(@as(u64, 0), identity.shard_id);
    try std.testing.expectEqual(@as(u64, 0), identity.table_id);
    try std.testing.expectEqual(@as(u64, 3), identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 4), identity.epoch);
}

test "standalone HA standby identity defaults shard and table to whole instance" {
    const identity = try haStandbyIdentity(.{
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    });
    try std.testing.expectEqual(@as(u64, 100), identity.cluster_id);
    try std.testing.expectEqual(@as(u64, 0), identity.shard_id);
    try std.testing.expectEqual(@as(u64, 0), identity.table_id);
    try std.testing.expectEqual(@as(u64, 3), identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 4), identity.epoch);
}

test "standalone HA runtime rejects ambiguous role flags" {
    try std.testing.expectError(error.HAMultipleRolesConfigured, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_standby_log = "/tmp/standby.log",
    }));
    try std.testing.expectError(error.HARoleMissing, validateHARole(.{
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HARoleMissing, validateHARole(.{
        .ha_fence_wal = "/tmp/fence.wal",
    }));
    try std.testing.expectError(error.HARoleMissing, validateHARole(.{
        .ha_former_primary_log = "/tmp/former-primary.wal",
    }));
    try validateHARole(.{
        .admin_token_env = "ANTFLY_HA_ADMIN_TOKEN",
    });
    try std.testing.expectError(error.AdminTokenEnvMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .admin_token_env = " \t ",
    }));
    try std.testing.expectError(error.AdminTokenEnvInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .admin_token_env = " ANTFLY_HA_ADMIN_TOKEN ",
    }));
    try std.testing.expectError(error.AdminTokenEnvInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .admin_token_env = "bad-token-env",
    }));
    try std.testing.expectError(error.AdminTokenEnvInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .admin_token_env = "9TOKEN",
    }));
    try std.testing.expectError(error.HAFenceWalMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
    }));
    try std.testing.expectError(error.HAFenceWalMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = " \t ",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAFenceWalInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = " /tmp/fence.wal ",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAFenceWalInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAFormerPrimaryLogInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_former_primary_log = " /tmp/former-primary.wal ",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAFormerPrimaryLogInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_former_primary_log = "/tmp/../former-primary.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAClusterIdMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HATimelineIdMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAEpochMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
    }));
    try std.testing.expectError(error.HAPrimaryLogMissing, validateHARole(.{
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimarySlotsMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimaryNodeIdMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimaryLogInvalid, validateHARole(.{
        .ha_primary_log = " /tmp/primary.log ",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimaryLogInvalid, validateHARole(.{
        .ha_primary_log = "primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimarySlotsInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = " /tmp/slots.wal ",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimarySlotsInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp//slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimaryNodeIdInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = " primary-a ",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimaryNodeIdInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAClusterIdMissing, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyLogMissing, validateHARole(.{
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyProgressMissing, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyNodeIdMissing, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyLogInvalid, validateHARole(.{
        .ha_standby_log = " /tmp/standby.log ",
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyLogInvalid, validateHARole(.{
        .ha_standby_log = "standby.log",
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyProgressInvalid, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_progress = " /tmp/progress.wal ",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyProgressInvalid, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_progress = "/tmp/../progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyNodeIdInvalid, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_standby_node_id = " standby-a ",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyNodeIdInvalid, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_standby_node_id = "standby a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HARetentionPolicyRequiresPrimary, validateHARole(.{
        .ha_retention_max_lag_lsn = 50,
    }));
    try std.testing.expectError(error.HARetentionPolicyRequiresPrimary, validateHARole(.{
        .ha_retention_max_retained_bytes = 4096,
    }));
    try std.testing.expectError(error.HARetentionPolicyRequiresPrimary, validateHARole(.{
        .ha_retention_max_retained_age_ns = 1000000,
    }));
    try std.testing.expectError(error.InvalidHARetentionPolicy, parsePositiveU64("0"));
    try std.testing.expectError(error.HASyncPolicyRequiresPrimary, validateHARole(.{
        .ha_sync_mode = .remote_write,
    }));
    try std.testing.expectError(error.InvalidHASyncPolicy, haSyncPolicyFromCli(std.testing.allocator, .{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
        .ha_sync_mode = .remote_write,
        .ha_sync_required = 1,
    }));
}

test "standalone HA runtime requires HA paths under resolved data root" {
    const root = "/tmp/antfly-data-root";
    const primary_cfg = CliConfig{
        .ha_primary_log = root ++ "/ha/primary.log",
        .ha_primary_slots = root ++ "/ha/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_former_primary_log = root ++ "/ha/primary.log",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    };
    try validateHARole(primary_cfg);
    try validateHAPathsUnderRoot(primary_cfg, root);

    try std.testing.expectError(error.HAPrimaryLogInvalid, validateHAPathsUnderRoot(.{
        .ha_primary_log = "/tmp/outside/primary.log",
        .ha_primary_slots = root ++ "/ha/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));
    try std.testing.expectError(error.HAPrimarySlotsInvalid, validateHAPathsUnderRoot(.{
        .ha_primary_log = root ++ "/ha/primary.log",
        .ha_primary_slots = "/tmp/outside/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));
    try std.testing.expectError(error.HAFenceWalInvalid, validateHAPathsUnderRoot(.{
        .ha_primary_log = root ++ "/ha/primary.log",
        .ha_primary_slots = root ++ "/ha/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/outside/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));
    try std.testing.expectError(error.HAFormerPrimaryLogInvalid, validateHAPathsUnderRoot(.{
        .ha_primary_log = root ++ "/ha/primary.log",
        .ha_primary_slots = root ++ "/ha/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_former_primary_log = "/tmp/outside/former-primary.log",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));

    const standby_cfg = CliConfig{
        .ha_standby_log = root ++ "/ha/standby.log",
        .ha_standby_progress = root ++ "/ha/standby-progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    };
    try validateHARole(standby_cfg);
    try validateHAPathsUnderRoot(standby_cfg, root);

    try std.testing.expectError(error.HAStandbyLogInvalid, validateHAPathsUnderRoot(.{
        .ha_standby_log = "/tmp/outside/standby.log",
        .ha_standby_progress = root ++ "/ha/standby-progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));
    try std.testing.expectError(error.HAStandbyProgressInvalid, validateHAPathsUnderRoot(.{
        .ha_standby_log = root ++ "/ha/standby.log",
        .ha_standby_progress = "/tmp/outside/standby-progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));
    try std.testing.expectError(error.HAPrimaryLogInvalid, validateHAPathsUnderRoot(.{
        .ha_primary_log = "/tmp/antfly-data-root2/ha/primary.log",
        .ha_primary_slots = root ++ "/ha/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));
}

test "standalone HA runtime validates bearer token env name before lookup" {
    const alloc = std.testing.allocator;
    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };
    const env_name = "ANTFLY_HA_ADMIN_TOKEN_TEST_VALUE";

    try std.testing.expect((try resolveAdminBearerTokenFromCli(alloc, .{})) == null);
    try std.testing.expectError(error.AdminTokenEnvMissing, resolveAdminBearerTokenFromCli(alloc, .{
        .admin_token_env = " \t ",
    }));
    try std.testing.expectError(error.AdminTokenEnvInvalid, resolveAdminBearerTokenFromCli(alloc, .{
        .admin_token_env = "bad-token-env",
    }));
    try std.testing.expectError(error.AdminTokenEnvInvalid, resolveAdminBearerTokenFromCli(alloc, .{
        .admin_token_env = "9TOKEN",
    }));
    try std.testing.expectError(error.AdminTokenMissing, resolveAdminBearerTokenFromCli(alloc, .{
        .admin_token_env = "ANTFLY_HA_ADMIN_TOKEN_SHOULD_NOT_EXIST",
    }));

    try std.testing.expectEqual(@as(c_int, 0), c.setenv(env_name, " secret-token\n", 1));
    defer _ = c.unsetenv(env_name);
    const token = try resolveAdminBearerTokenFromCli(alloc, .{
        .admin_token_env = env_name,
    });
    defer alloc.free(token.?);
    try std.testing.expectEqualStrings("secret-token", token.?);

    try std.testing.expectEqual(@as(c_int, 0), c.setenv(env_name, " \t\n", 1));
    try std.testing.expectError(error.AdminTokenMissing, resolveAdminBearerTokenFromCli(alloc, .{
        .admin_token_env = env_name,
    }));
}

test "standalone runtime defaults public listener to antfarm port" {
    const listener = resolvePublicListener(.{});
    try std.testing.expectEqualStrings("127.0.0.1", listener.bind_host);
    try std.testing.expectEqual(@as(u16, default_public_port), listener.bind_port);
}

test "standalone public HTTP server is restart-safe and uses public API request body limit" {
    const cfg = publicHttpServerConfig("127.0.0.1", 8080);
    try std.testing.expect(cfg.reuse_address);
    try std.testing.expectEqual(antfly.public_api.http_server.public_api_max_request_body_bytes, cfg.max_body_size);
    try std.testing.expectEqual(@as(usize, 256 * 1024 * 1024), cfg.request_body_buffer_budget_bytes);
    try std.testing.expect(cfg.max_connections >= 1);
    try std.testing.expect(cfg.max_connections <= public_http_connection_ceiling);
    try std.testing.expectEqual(@as(u32, 5), cfg.accept_error_backoff_initial_ms);
    try std.testing.expectEqual(@as(u32, 1_000), cfg.accept_error_backoff_max_ms);
    try std.testing.expectEqual(@as(u32, 256), publicHttpConnectionLimitForFdSoftLimit(1024));
    try std.testing.expectEqual(@as(u32, 128), publicHttpConnectionLimitForFdSoftLimit(512));
    try std.testing.expectEqual(@as(u32, 32), publicHttpConnectionLimitForFdSoftLimit(128));
    try std.testing.expectEqual(@as(u32, 1), publicHttpConnectionLimitForFdSoftLimit(3));
}

test "standalone Lite transaction sessions survive file reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sessions.aflite", .{tmp.sub_path});
    defer alloc.free(path);

    var txn_id: antfly.db.types.TxnId = undefined;
    {
        var backend = try antfly.lite.backend.Handle.openOrCreate(alloc, path, .{ .no_sync = false });
        defer backend.deinit();
        var durable = antfly.public_api.transactions.DurableSessionStore.initRuntime(
            alloc,
            try backend.runtimeStoreForNamespace("system/api-transaction-sessions"),
        );
        var registry = antfly.public_api.transactions.SessionRegistry.init(&durable);
        defer registry.deinit(alloc);
        txn_id = (try registry.begin(alloc, .{ .sync_level = .write }, 1)).txn_id;
    }

    {
        var backend = try antfly.lite.backend.Handle.open(alloc, path, .{});
        defer backend.deinit();
        var durable = antfly.public_api.transactions.DurableSessionStore.initRuntime(
            alloc,
            try backend.runtimeStoreForNamespace("system/api-transaction-sessions"),
        );
        var registry = antfly.public_api.transactions.SessionRegistry.init(&durable);
        defer registry.deinit(alloc);
        const restored = registry.getInfo(txn_id) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(txn_id, restored.txn_id);
        try std.testing.expectEqual(antfly.db.types.SyncLevel.write, restored.sync_level);
    }
}

test "standalone public listener lease is exclusive and immediately reusable" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const lease_key = tmp.sub_path;
    const lease_port: u16 = @intCast(20_000 + std.hash.Wyhash.hash(0, lease_key[0..]) % 30_000);
    var first = PublicListenerLease.acquire(std.testing.allocator, lease_port) catch |err| switch (err) {
        error.ListenerLockUnsupported => return error.SkipZigTest,
        else => return err,
    };
    try std.testing.expectError(error.AddressInUse, PublicListenerLease.acquire(std.testing.allocator, lease_port));
    first.deinit();

    var replacement = try PublicListenerLease.acquire(std.testing.allocator, lease_port);
    replacement.deinit();
}

test "antfly config uses cli override before common config" {
    const alloc = std.testing.allocator;
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .transcribers = antfly.transcribing.Registry.init(alloc),
        .readers = antfly.readers.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .inference = .{
            .api_url = try alloc.dupe(u8, "http://127.0.0.1:9000"),
            .models_dir = try alloc.dupe(u8, "/tmp/from-config"),
            .ml_dir = try alloc.dupe(u8, "/tmp/ml-from-config"),
        },
    };
    defer cfg.deinit();

    const cli = CliConfig{
        .inference_models_dir = "/tmp/from-cli",
        .inference_ml_dir = "/tmp/ml-from-cli",
        .inference_backend_budget_mb = 8192,
    };
    try std.testing.expectEqualStrings("/tmp/from-cli", resolveInferenceModelsDir(cli, &cfg).?);
    try std.testing.expectEqualStrings("/tmp/ml-from-cli", resolveInferenceMlDir(cli, &cfg).?);
    try std.testing.expectEqual(@as(usize, 8192 * 1024 * 1024), resolveInferenceBudgetOverrides(cli).backend_limit_bytes);
}

test "standalone public api caps keep alive request reuse" {
    try std.testing.expect(public_api_max_requests_per_connection > 0);
    try std.testing.expect(public_api_max_requests_per_connection < 1000);
}

test "standalone public api body limit matches common http listener" {
    try std.testing.expectEqual(antfly.common.http.default_max_request_bytes, public_api_max_body_size);
}

test "standalone readiness follows api initialization and unified listener" {
    try std.testing.expect(!standaloneReadyFromState(false, false));
    try std.testing.expect(!standaloneReadyFromState(false, true));
    try std.testing.expect(!standaloneReadyFromState(true, false));
    try std.testing.expect(standaloneReadyFromState(true, true));
}

test "parse cli accepts inference budget overrides" {
    var argv = [_][*:0]const u8{
        "--inference-host-budget-mb",
        "4096",
        "--inference-backend-budget-mb",
        "12288",
        "--inference-combined-budget-mb",
        "16384",
        "--inference-kv-budget-mb",
        "2048",
        "--inference-scratch-budget-mb",
        "1024",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 4096), cfg.inference_host_budget_mb);
    try std.testing.expectEqual(@as(usize, 12288), cfg.inference_backend_budget_mb);
    try std.testing.expectEqual(@as(usize, 16384), cfg.inference_combined_budget_mb);
    try std.testing.expectEqual(@as(usize, 2048), cfg.inference_kv_budget_mb);
    try std.testing.expectEqual(@as(usize, 1024), cfg.inference_scratch_budget_mb);
}

test "inference config falls back to common config" {
    const alloc = std.testing.allocator;
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .transcribers = antfly.transcribing.Registry.init(alloc),
        .readers = antfly.readers.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .inference = .{
            .api_url = try alloc.dupe(u8, "http://127.0.0.1:8089"),
            .models_dir = try alloc.dupe(u8, "/tmp/antfly-models"),
            .ml_dir = try alloc.dupe(u8, "/tmp/antfly-ml"),
            .preload = try alloc.dupe(antfly.common.config.Config.InferenceConfig.WarmModelConfig, &.{
                .{
                    .kind = try alloc.dupe(u8, "generator"),
                    .name = try alloc.dupe(u8, "antflydb/gemma-e2b"),
                    .backend = try alloc.dupe(u8, "metal"),
                    .format = try alloc.dupe(u8, "gguf"),
                    .quantization = try alloc.dupe(u8, "q4_k"),
                },
            }),
        },
    };
    defer cfg.deinit();

    try std.testing.expectEqualStrings("/tmp/antfly-models", resolveInferenceModelsDir(.{}, &cfg).?);
    try std.testing.expectEqualStrings("/tmp/antfly-ml", resolveInferenceMlDir(.{}, &cfg).?);
    var warm_models = try resolveInferenceWarmModels(alloc, .{}, &cfg);
    defer warm_models.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), warm_models.items.len);
    try std.testing.expectEqual(inference.server.WarmModelKind.generator, warm_models.items[0].kind);
    try std.testing.expectEqualStrings("antflydb/gemma-e2b", warm_models.items[0].name);
    try std.testing.expectEqual(inference.backends.BackendType.metal, warm_models.items[0].backend.?);
    try std.testing.expectEqualStrings("gguf", warm_models.items[0].format.?);
    try std.testing.expectEqualStrings("q4_k", warm_models.items[0].quantization.?);
}

test "standalone runtime resolves paths from common storage base dir" {
    const alloc = std.testing.allocator;
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .transcribers = antfly.transcribing.Registry.init(alloc),
        .readers = antfly.readers.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .metadata = .{},
        .storage = .{
            .local_base_dir = try alloc.dupe(u8, "/tmp/antflydb"),
        },
        .inference = .{},
    };
    defer cfg.deinit();

    const resolved = try resolvePaths(alloc, .{}, &cfg);
    defer resolved.deinit(alloc);
    const expected_data_base = try normalizeResolvedPathAlloc(alloc, "/tmp/antflydb/data");
    defer alloc.free(expected_data_base);
    const expected_metadata_base = try normalizeResolvedPathAlloc(alloc, "/tmp/antflydb/metadata");
    defer alloc.free(expected_metadata_base);
    const expected_replica_root = try std.fs.path.join(alloc, &.{ expected_data_base, "replicas" });
    defer alloc.free(expected_replica_root);
    const expected_replica_catalog = try std.fs.path.join(alloc, &.{ expected_data_base, "catalog.txt" });
    defer alloc.free(expected_replica_catalog);
    const expected_local_metadata = try std.fs.path.join(alloc, &.{ expected_metadata_base, "local-metadata.json" });
    defer alloc.free(expected_local_metadata);
    const expected_snapshot_root = try std.fs.path.join(alloc, &.{ expected_data_base, "snapshots" });
    defer alloc.free(expected_snapshot_root);
    const expected_extension_store = try normalizeResolvedPathAlloc(alloc, "/tmp/antflydb/extensions");
    defer alloc.free(expected_extension_store);
    try std.testing.expectEqualStrings(expected_replica_root, resolved.replica_root_dir);
    try std.testing.expectEqualStrings(expected_replica_catalog, resolved.replica_catalog_path);
    try std.testing.expectEqualStrings(expected_local_metadata, resolved.local_metadata_catalog_path);
    try std.testing.expectEqualStrings(expected_snapshot_root, resolved.snapshot_root_dir);
    try std.testing.expectEqualStrings(expected_extension_store, resolved.extension_package_store_dir);
    const expected_secret_store = try normalizeResolvedPathAlloc(alloc, "/tmp/antflydb/secrets.json");
    defer alloc.free(expected_secret_store);
    try std.testing.expectEqualStrings(expected_secret_store, resolved.secret_store_path);
}

test "standalone runtime resolves explicit extension package store path" {
    const alloc = std.testing.allocator;
    const resolved = try resolvePaths(alloc, .{ .extension_package_store_dir = "/opt/antfly/extensions" }, null);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("/opt/antfly/extensions", resolved.extension_package_store_dir);
}

test "standalone runtime resolves extension package store env before local default" {
    const alloc = std.testing.allocator;

    const env_resolved = try resolveExtensionPackageStoreDirWithEnv(alloc, null, "/tmp/antflydb", "/antfly-extension-env");
    defer alloc.free(env_resolved);
    try std.testing.expectEqualStrings("/antfly-extension-env", env_resolved);

    const cli_resolved = try resolveExtensionPackageStoreDirWithEnv(alloc, "/antfly-cli-extensions", "/tmp/antflydb", "/antfly-extension-env");
    defer alloc.free(cli_resolved);
    try std.testing.expectEqualStrings("/antfly-cli-extensions", cli_resolved);
}

test "standalone runtime resolves explicit secret store path" {
    const alloc = std.testing.allocator;
    var cli = CliConfig{};
    defer cli.deinit(alloc);
    try cli.secret_store_paths.append(alloc, "/run/antfly/secrets/secrets.json");
    const resolved = try resolvePaths(alloc, cli, null);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("/run/antfly/secrets/secrets.json", resolved.secret_store_path);
}

test "standalone runtime data dir overrides common storage base dir" {
    const alloc = std.testing.allocator;
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .transcribers = antfly.transcribing.Registry.init(alloc),
        .readers = antfly.readers.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .metadata = .{},
        .storage = .{
            .local_base_dir = try alloc.dupe(u8, "/tmp/from-config"),
        },
        .inference = .{},
    };
    defer cfg.deinit();

    const local_base = try resolveLocalBaseDir(alloc, .{ .data_dir = "/tmp/from-cli" }, &cfg);
    defer alloc.free(local_base);
    try std.testing.expectEqualStrings("/tmp/from-cli", local_base);

    const resolved = try resolvePaths(alloc, .{ .data_dir = "/tmp/from-cli" }, &cfg);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("/tmp/from-cli/data/replicas", resolved.replica_root_dir);
    try std.testing.expectEqualStrings("/tmp/from-cli/data/catalog.txt", resolved.replica_catalog_path);
    try std.testing.expectEqualStrings("/tmp/from-cli/metadata/local-metadata.json", resolved.local_metadata_catalog_path);
    try std.testing.expectEqualStrings("/tmp/from-cli/data/snapshots", resolved.snapshot_root_dir);
}

test "standalone metadata rolls back an undurable catalog mutation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/catalog-directory", .{tmp.sub_path});
    defer alloc.free(catalog_dir);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try ensureDirPath(io_impl.io(), catalog_dir);

    var backend_runtime = try antfly.db.background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer backend_runtime.deinit();
    var metadata = LocalStandaloneMetadata{
        .alloc = alloc,
        .manager = antfly.metadata.TableManager.init(alloc),
        .extension_catalog = antfly.extensions.ExtensionCatalog.init(alloc),
        .local_node_id = 1,
        .store_id = 1,
        .api_url = try alloc.dupe(u8, "http://127.0.0.1:8080"),
        .replica_root_dir = try alloc.dupe(u8, "."),
        .catalog_path = try alloc.dupe(u8, catalog_dir),
        .catalog_store = null,
        .backend_runtime = backend_runtime.ptr(),
    };
    defer metadata.deinit();

    {
        var mutation = try metadata.beginCatalogMutationLocked();
        defer mutation.deinit(&metadata);
        const table = antfly.public_api.tables.deriveTableRecord("docs", .{});
        try metadata.manager.upsertTable(table);
        metadata.epoch = 9;
        var persist_failed = false;
        mutation.commit(&metadata) catch {
            persist_failed = true;
        };
        try std.testing.expect(persist_failed);
    }
    try std.testing.expectEqual(@as(u64, 1), metadata.epoch);
    try std.testing.expect(metadata.findTableByNameLocked("docs") == null);
}

test "standalone metadata rejects corrupt catalog without double-freeing owned paths" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/corrupt-catalog.json", .{tmp.sub_path});
    defer alloc.free(catalog_path);
    try writeFileAtomically(alloc, catalog_path, "{not-json");

    var backend_runtime = try antfly.db.background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer backend_runtime.deinit();
    const result = LocalStandaloneMetadata.init(
        alloc,
        1,
        1,
        "http://127.0.0.1:8080",
        ".",
        catalog_path,
        backend_runtime.ptr(),
        null,
        .local,
    );
    if (result) |value| {
        var metadata = value;
        defer metadata.deinit();
        return error.TestUnexpectedResult;
    } else |_| {}
}

test "standalone metadata finalizes schema migration from resident runtime evidence" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/catalog.json", .{tmp.sub_path});
    defer alloc.free(catalog_path);

    var backend_runtime = try antfly.db.background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer backend_runtime.deinit();
    var metadata = LocalStandaloneMetadata{
        .alloc = alloc,
        .manager = antfly.metadata.TableManager.init(alloc),
        .extension_catalog = antfly.extensions.ExtensionCatalog.init(alloc),
        .local_node_id = 1,
        .store_id = 1,
        .api_url = try alloc.dupe(u8, "http://127.0.0.1:8080"),
        .replica_root_dir = try alloc.dupe(u8, "."),
        .catalog_path = try alloc.dupe(u8, catalog_path),
        .catalog_store = null,
        .backend_runtime = backend_runtime.ptr(),
    };
    defer metadata.deinit();
    try metadata.manager.upsertTable(.{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":1}",
        .read_schema_json = "{\"version\":0}",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
    });
    try metadata.manager.upsertRange(.{
        .group_id = 70,
        .table_id = 7,
        .start_key = "",
    });

    const Provider = struct {
        fn collect(
            _: *anyopaque,
            provider_alloc: std.mem.Allocator,
            _: []const antfly.metadata.TableRecord,
            _: []const antfly.metadata.RangeRecord,
        ) !antfly.data.runtime.DataServer.LocalSchemaProgressSnapshot {
            const records = try provider_alloc.alloc(antfly.metadata.SchemaProgressRecord, 1);
            records[0] = .{ .table_id = 7, .node_id = 1, .schema_version = 1 };
            return .{ .records = records, .runtime_coverage_complete = true };
        }
    };
    metadata.local_schema_progress_provider = .{
        .ptr = undefined,
        .collect = Provider.collect,
    };

    try metadata.finalizeReadySchemaMigrations();
    const table = metadata.findTableByNameLocked("docs") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("", table.read_schema_json);
    try std.testing.expect(std.mem.indexOf(u8, table.indexes_json, "full_text_index_v0") == null);
    try std.testing.expect(std.mem.indexOf(u8, table.indexes_json, "full_text_index_v1") != null);
}

test "standalone unified server lifecycle propagates startup failure" {
    var lifecycle = UnifiedServerLifecycle{};
    lifecycle.publishFailure(error.AddressInUse);
    try std.testing.expectError(error.AddressInUse, lifecycle.waitForStartup());
    try std.testing.expectEqual(error.AddressInUse, lifecycle.runtimeFailure().?);
}
