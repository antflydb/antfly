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
const httpx = @import("httpx");
const antfly = @import("../root.zig");
const group_ids = @import("../common/group_ids.zig");
const inference = @import("inference_server");
const metadata_openapi = @import("antfly_metadata_openapi");
const usermgr_openapi = @import("antfly_usermgr_openapi");
const fs_paths = @import("../common/fs_paths.zig");
const platform_time = @import("../platform/time.zig");

const AntflyApiHandler = antfly.public_api.httpx_handler.AntflyApiHandler;
const http_common = antfly.common.http.http_common;
const public_api_max_requests_per_connection: u32 = 64;
const public_api_max_body_size: usize = antfly.common.http.default_max_request_bytes;
const local_schema_migration_finalize_interval_ms: u64 = std.time.ms_per_s;
const default_public_port: u16 = 8080;
const antfarm_max_file_bytes: usize = 64 * 1024 * 1024;
const antfarm_installed_asset_root = "../share/antfly/antfarm";
const antfarm_asset_roots = [_][]const u8{
    "src/metadata/antfarm",
    "../src/metadata/antfarm",
    "/usr/share/antfly/antfarm",
    "antfarm",
};

const CliConfig = struct {
    config_path: ?[]const u8 = null,
    bind_host: ?[]const u8 = null,
    bind_port: ?u16 = null,
    health_enabled: ?bool = null,
    health_port: ?u16 = null,
    tick_ms: ?u64 = null,
    local_node_id: ?u64 = null,
    auth_enabled: ?bool = null,
    inference_models_dir: ?[]const u8 = null,
    inference_host_budget_mb: usize = 0,
    inference_backend_budget_mb: usize = 0,
    inference_combined_budget_mb: usize = 0,
    inference_kv_budget_mb: usize = 0,
    inference_scratch_budget_mb: usize = 0,
    data_dir: ?[]const u8 = null,
    replica_root_dir: ?[]const u8 = null,
    replica_catalog_path: ?[]const u8 = null,
    snapshot_root_dir: ?[]const u8 = null,
    secret_store_path: ?[]const u8 = null,
    help: bool = false,
};

const ResolvedPaths = struct {
    replica_root_dir: []u8,
    replica_catalog_path: []u8,
    local_metadata_catalog_path: []u8,
    snapshot_root_dir: []u8,
    secret_store_path: []u8,
    auth_store_root_dir: []u8,

    fn deinit(self: ResolvedPaths, alloc: std.mem.Allocator) void {
        alloc.free(self.replica_root_dir);
        alloc.free(self.replica_catalog_path);
        alloc.free(self.local_metadata_catalog_path);
        alloc.free(self.snapshot_root_dir);
        alloc.free(self.secret_store_path);
        alloc.free(self.auth_store_root_dir);
    }
};

const SwarmHealthSource = struct {
    data_server: *antfly.data.runtime.DataServer,

    fn readiness(self: *SwarmHealthSource) antfly.common.health_server.ReadinessChecker {
        return .{
            .ptr = self,
            .vtable = &.{ .check = checkReady },
        };
    }

    fn metricsWriter(self: *SwarmHealthSource) antfly.common.health_server.MetricsWriter {
        return .{
            .ptr = self,
            .vtable = &.{ .write_metrics = writeMetrics },
        };
    }

    fn checkReady(ptr: *anyopaque) bool {
        const self: *SwarmHealthSource = @ptrCast(@alignCast(ptr));
        var data_health = antfly.data.runtime.HealthSource{ .data_server = self.data_server };
        return data_health.readiness().check();
    }

    fn writeMetrics(ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void {
        const self: *SwarmHealthSource = @ptrCast(@alignCast(ptr));
        var data_health = antfly.data.runtime.HealthSource{ .data_server = self.data_server };
        try data_health.metricsWriter().writeMetrics(writer);
    }
};

const LocalSwarmMetadata = struct {
    alloc: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    manager: antfly.metadata.TableManager,
    local_node_id: u64,
    store_id: u64,
    api_url: []const u8,
    replica_root_dir: []const u8,
    catalog_path: []const u8,
    backend_runtime: *antfly.db.background_runtime.BackendRuntime,
    epoch: u64 = 1,
    last_schema_migration_finalize_at_ms: u64 = 0,

    const PersistedCatalog = struct {
        epoch: u64 = 1,
        tables: []const antfly.metadata.TableRecord = &.{},
        ranges: []const antfly.metadata.RangeRecord = &.{},
    };

    fn init(
        alloc: std.mem.Allocator,
        local_node_id: u64,
        store_id: u64,
        api_url: []const u8,
        replica_root_dir: []const u8,
        catalog_path: []const u8,
        backend_runtime: *antfly.db.background_runtime.BackendRuntime,
    ) !LocalSwarmMetadata {
        const owned_api_url = try alloc.dupe(u8, api_url);
        errdefer alloc.free(owned_api_url);
        const owned_replica_root_dir = try alloc.dupe(u8, replica_root_dir);
        errdefer alloc.free(owned_replica_root_dir);
        const owned_catalog_path = try alloc.dupe(u8, catalog_path);
        errdefer alloc.free(owned_catalog_path);
        var self = LocalSwarmMetadata{
            .alloc = alloc,
            .manager = antfly.metadata.TableManager.init(alloc),
            .local_node_id = local_node_id,
            .store_id = store_id,
            .api_url = owned_api_url,
            .replica_root_dir = owned_replica_root_dir,
            .catalog_path = owned_catalog_path,
            .backend_runtime = backend_runtime,
        };
        errdefer self.deinit();
        try self.loadPersistedCatalog();
        return self;
    }

    fn deinit(self: *LocalSwarmMetadata) void {
        self.manager.deinit();
        self.alloc.free(self.catalog_path);
        self.alloc.free(self.replica_root_dir);
        self.alloc.free(self.api_url);
        self.* = undefined;
    }

    fn catalogSource(self: *LocalSwarmMetadata) antfly.public_api.table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = catalogAdminSnapshot,
                .free_admin_snapshot = catalogFreeAdminSnapshot,
            },
        };
    }

    fn statusSource(self: *LocalSwarmMetadata) antfly.public_api.http_server.StatusSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .status = status,
                .admin_snapshot = catalogAdminSnapshot,
                .cached_admin_snapshot = cachedAdminSnapshot,
                .free_admin_snapshot = catalogFreeAdminSnapshot,
                .create_table = createTable,
                .drop_table = dropTable,
                .update_schema = updateSchema,
                .create_index = createIndex,
                .drop_index = dropIndex,
                .wait_table_lifecycle = waitTableLifecycle,
                .wait_table_projection = waitTableProjection,
                .run_round = runRound,
            },
        };
    }

    fn status(ptr: *anyopaque) !antfly.metadata_api.MetadataStatus {
        const self: *LocalSwarmMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return .{
            .metadata_group_id = group_ids.main_metadata_group_id,
            .metadata_epoch = self.epoch,
            .metadata_raft_role = "disabled",
            .projected_tables = self.manager.tables.count(),
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
        const self: *LocalSwarmMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const tables = try self.manager.listTables(self.alloc);
        errdefer self.manager.freeTables(self.alloc, tables);
        const ranges = try self.manager.listRanges(self.alloc);
        errdefer self.manager.freeRanges(self.alloc, ranges);

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
                .projected_ranges = ranges.len,
                .projected_stores = stores.len,
                .projected_placement_intents = placement_intents.len,
                .metrics = .{},
            },
            .tables = tables,
            .ranges = ranges,
            .stores = stores,
            .placement_intents = placement_intents,
            .split_transitions = try self.alloc.alloc(antfly.metadata.SplitTransitionRecord, 0),
            .merge_transitions = try self.alloc.alloc(antfly.metadata.MergeTransitionRecord, 0),
        };
    }

    fn catalogFreeAdminSnapshot(ptr: *anyopaque, snapshot: *antfly.metadata_api.AdminSnapshot) void {
        const self: *LocalSwarmMetadata = @ptrCast(@alignCast(ptr));
        self.manager.freeTables(self.alloc, snapshot.tables);
        self.manager.freeRanges(self.alloc, snapshot.ranges);
        for (snapshot.stores) |store| antfly.metadata.table_manager.freeStore(self.alloc, store);
        self.alloc.free(snapshot.stores);
        self.alloc.free(snapshot.placement_intents);
        self.alloc.free(snapshot.split_transitions);
        self.alloc.free(snapshot.merge_transitions);
        snapshot.* = undefined;
    }

    fn createTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: antfly.public_api.tables.CreateTableRequest) !void {
        const self: *LocalSwarmMetadata = @ptrCast(@alignCast(ptr));
        const table = antfly.public_api.tables.deriveTableRecord(table_name, req);
        const ranges = try antfly.public_api.tables.deriveInitialRanges(alloc, table);
        defer {
            for (ranges) |record| antfly.metadata.table_manager.freeRange(alloc, record);
            alloc.free(ranges);
        }

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.findTableByNameLocked(table_name) != null) return error.TableAlreadyExists;
        try self.manager.upsertTable(table);
        for (ranges) |range| try self.manager.upsertRange(range);
        self.epoch +|= 1;
        try self.persistLocked();
    }

    fn dropTable(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8) !void {
        const self: *LocalSwarmMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        _ = self.manager.removeTableTopology(table.table_id);
        self.epoch +|= 1;
        try self.persistLocked();
    }

    fn updateSchema(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
        const self: *LocalSwarmMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        const updated = try antfly.public_api.tables.applySchemaUpdateRecord(alloc, table, schema_json);
        defer antfly.metadata.table_manager.freeTable(alloc, updated);
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try self.persistLocked();
    }

    fn createIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
        const self: *LocalSwarmMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        var updated = table.*;
        updated.indexes_json = try antfly.public_api.indexes.addIndexToTableIndexesJson(alloc, table.indexes_json, index_name, index_json);
        defer alloc.free(updated.indexes_json);
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try self.persistLocked();
    }

    fn dropIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
        const self: *LocalSwarmMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        const indexes_json = (try antfly.public_api.indexes.removeIndexFromTableIndexesJson(alloc, table.indexes_json, index_name)) orelse return error.IndexNotFound;
        defer alloc.free(indexes_json);
        var updated = table.*;
        updated.indexes_json = indexes_json;
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try self.persistLocked();
    }

    fn waitTableLifecycle(_: *anyopaque, _: []const u8, _: antfly.public_api.http_server.TableVisibility) !void {}

    fn waitTableProjection(_: *anyopaque, _: []const u8, _: ?[]const u8, _: ?[]const u8) !void {}

    fn runRound(ptr: *anyopaque) !void {
        const self: *LocalSwarmMetadata = @ptrCast(@alignCast(ptr));
        self.finalizeReadySchemaMigrations() catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
            else => return err,
        };
    }

    fn finalizeReadySchemaMigrations(self: *LocalSwarmMetadata) !void {
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

        const progress = try antfly.metadata.table_provisioner.collectLocalSchemaProgressWithOptions(
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
        defer self.alloc.free(progress);
        if (progress.len == 0) return;

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

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
            try self.persistLocked();
        }
    }

    fn findTableByNameLocked(self: *LocalSwarmMetadata, table_name: []const u8) ?*const antfly.metadata.TableRecord {
        var it = self.manager.tables.valueIterator();
        while (it.next()) |table| {
            if (std.mem.eql(u8, table.name, table_name)) return table;
        }
        return null;
    }

    fn loadPersistedCatalog(self: *LocalSwarmMetadata) !void {
        const raw = readFileAlloc(self.alloc, self.catalog_path, 64 * 1024 * 1024) catch |err| switch (err) {
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
        self.epoch = @max(parsed.value.epoch, 1);
    }

    fn persistLocked(self: *LocalSwarmMetadata) !void {
        const tables = try self.manager.listTables(self.alloc);
        defer self.manager.freeTables(self.alloc, tables);
        const ranges = try self.manager.listRanges(self.alloc);
        defer self.manager.freeRanges(self.alloc, ranges);

        const encoded = try std.json.Stringify.valueAlloc(self.alloc, PersistedCatalog{
            .epoch = self.epoch,
            .tables = tables,
            .ranges = ranges,
        }, .{ .emit_null_optional_fields = false });
        defer self.alloc.free(encoded);

        try writeFileAtomically(self.alloc, self.catalog_path, encoded);
    }
};

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

    const argv0 = args.next() orelse "antfly_swarm";
    return try runFromIterator(init, argv0, &args);
}

pub fn runFromIterator(
    init: std.process.Init,
    _: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    const alloc = init.gpa;
    const cli = try parseCli(args);
    if (cli.help) {
        printUsage();
        return;
    }

    var secret_store: antfly.common.secrets.FileStore = undefined;
    var secret_store_initialized = false;
    defer if (secret_store_initialized) secret_store.deinit();

    if (cli.secret_store_path) |raw_secret_store_path| {
        const normalized_secret_store_path = try normalizeResolvedPathAlloc(alloc, raw_secret_store_path);
        defer alloc.free(normalized_secret_store_path);
        secret_store = try antfly.common.secrets.FileStore.init(alloc, normalized_secret_store_path);
        secret_store_initialized = true;
    }

    var loaded_config: ?antfly.common.config.Config = if (cli.config_path) |config_path|
        try antfly.common.config.loadFromPathWithSecrets(
            alloc,
            config_path,
            if (secret_store_initialized) &secret_store else null,
        )
    else
        null;
    defer if (loaded_config) |*cfg| cfg.deinit();

    const data_dir = try resolveLocalBaseDir(alloc, cli, if (loaded_config) |*cfg| cfg else null);
    defer alloc.free(data_dir);
    try antfly.common.data_format.ensureCompatible(alloc, data_dir);

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

    // Swarm always owns a local Antfly node. Antfly-managed embeddings use it
    // directly, and the public Antfly routes are registered on the unified
    // server for compatibility with external clients.
    var antfly_node_cfg = inference.server.NodeConfig{
        .models_dir = resolveInferenceModelsDir(cli, if (loaded_config) |*cfg| cfg else null) orelse
            antfly.inference_runtime.defaultModelsDirForDataDir(alloc, data_dir),
        .generation_budget_overrides = resolveInferenceBudgetOverrides(cli),
    };
    if (loaded_config) |*cfg| {
        if (cfg.effectiveAntflyContentSecurity()) |security| antfly_node_cfg.content_security = security.*;
        if (cfg.inference.s3_credentials) |creds| antfly_node_cfg.s3_credentials = creds;
    }
    var antfly_node = try inference.server.Node.init(alloc, antfly_node_cfg);
    defer antfly_node.deinit();

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

    var local_metadata = try LocalSwarmMetadata.init(
        alloc,
        local_node_id,
        1,
        public_api_url,
        resolved.replica_root_dir,
        resolved.local_metadata_catalog_path,
        node_backend_runtime.ptr(),
    );
    defer local_metadata.deinit();

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
            .swarm_mode = true,
            .secret_store = &secret_store,
            .remote_content = if (loaded_config) |*cfg| if (cfg.remote_content) |*remote_content| remote_content else null else null,
            .inference_api_key = if (loaded_config) |*cfg| if (cfg.inference.api_key) |value| value else null else null,
            .user_manager = if (user_manager) |*manager| manager else null,
        },
        .backend_runtime = node_backend_runtime.ptr(),
    }, local_metadata.catalogSource(), local_metadata.statusSource());
    defer data_server.deinit();

    data_server.setAntflyProvider(localAntflyProvider(&antfly_node));

    // Initialize API server (wires caches + sources) without binding a listener.
    data_server.initApiServer();
    data_server.registerNodeIfConfigured() catch |err| {
        std.log.err("swarm startup failed step=register_node err={}", .{err});
        return err;
    };
    data_server.requestProvisionedStartupCatchUpNow() catch |err| {
        std.log.warn("swarm startup provisioned startup catch-up skipped err={}", .{err});
    };
    data_server.requestProvisionedCacheWarmup() catch |err| {
        std.log.warn("swarm startup provisioned cache warmup skipped err={}", .{err});
    };

    const api_server = &data_server.http_server.?;

    // ---------------------------------------------------------------
    // Unified httpx.Server — all routes on a single port
    // ---------------------------------------------------------------

    var handler = AntflyApiHandler{ .api_server = api_server };

    const bind_host = public_listener.bind_host;
    const bind_port = public_listener.bind_port;

    const thread = std.Thread.spawn(.{}, serveUnified, .{
        alloc,
        bind_host,
        bind_port,
        &handler,
        &antfly_node,
        api_server,
    }) catch |err| {
        std.log.err("swarm startup failed step=spawn_unified_http err={}", .{err});
        return err;
    };
    _ = thread; // detach happens on process exit

    // Print bound address. The thread will print it after bind().
    std.debug.print("swarm local metadata enabled (raft disabled)\n", .{});

    var swarm_health = SwarmHealthSource{
        .data_server = &data_server,
    };
    const health_enabled = cli.health_enabled orelse if (loaded_config) |*cfg| cfg.health_enabled else true;
    const health_port = if (health_enabled)
        cli.health_port orelse if (loaded_config) |*cfg| cfg.health_port else antfly.common.config.default_health_port
    else
        null;
    const health_server = antfly.common.health_server.HealthServer.startIfConfiguredOnHost(
        alloc,
        "swarm",
        public_listener.bind_host,
        health_port,
        swarm_health.readiness(),
        swarm_health.metricsWriter(),
    ) catch |err| {
        std.log.err("swarm startup failed step=health_server err={}", .{err});
        return err;
    };
    defer if (health_server) |hs| hs.deinit();

    const tick_ms = cli.tick_ms orelse 25;
    var req = std.posix.timespec{
        .sec = @intCast(tick_ms / std.time.ms_per_s),
        .nsec = @intCast((tick_ms % std.time.ms_per_s) * std.time.ns_per_ms),
    };
    while (true) {
        try data_server.runRound();
        try LocalSwarmMetadata.runRound(&local_metadata);
        const err = std.posix.errno(std.posix.system.nanosleep(&req, &req));
        switch (err) {
            .SUCCESS => {},
            .INTR => continue,
            else => return std.posix.unexpectedErrno(err),
        }
    }
}

fn localAntflyProvider(node: *inference.server.Node) antfly.inference.managed_embedder.AntflyProvider {
    return .{
        .ptr = node,
        .embed_dense_texts = localAntflyEmbedDenseTexts,
        .embed_sparse_texts = localAntflyEmbedSparseTexts,
        .rerank_texts = localAntflyRerankTexts,
        .generate_text = localAntflyGenerateText,
        .generate_messages = localAntflyGenerateMessages,
        .read_images = localAntflyReadImages,
        .transcribe_audio = localAntflyTranscribeAudio,
        .extract = localAntflyExtract,
    };
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
    bind_host: []const u8,
    bind_port: u16,
    handler: *AntflyApiHandler,
    antfly_node: ?*inference.server.Node,
    api_server: *antfly.public_api.http_server.ApiHttpServer,
) void {
    serveUnifiedInner(alloc, bind_host, bind_port, handler, antfly_node, api_server) catch |err| {
        std.debug.print("unified server error: {}\n", .{err});
    };
}

fn serveUnifiedInner(
    alloc: std.mem.Allocator,
    bind_host: []const u8,
    bind_port: u16,
    handler: *AntflyApiHandler,
    antfly_node: ?*inference.server.Node,
    api_server: *antfly.public_api.http_server.ApiHttpServer,
) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();

    var server = httpx.Server.initWithConfig(alloc, io_impl.io(), publicHttpServerConfig(bind_host, bind_port));
    defer server.deinit();

    // Register antfly routes under /ai/v1
    if (antfly_node) |node| {
        try node.registerRoutesOn(inference.server.public_api_prefix, &server);
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
    try registerInternalGroupRoutes(&server);
    try registerAntfarmRoutes(&server);

    try server.bind();

    if (server.boundAddress()) |addr| {
        std.debug.print("swarm public api listening on http://{}\n", .{addr});
    }

    try server.listen();
}

fn publicHttpServerConfig(bind_host: []const u8, bind_port: u16) httpx.ServerConfig {
    return .{
        .host = bind_host,
        .port = bind_port,
        .max_body_size = antfly.public_api.http_server.public_api_max_request_body_bytes,
        .request_timeout_ms = 300_000,
        .max_requests_per_connection = public_api_max_requests_per_connection,
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
    return ctx.json(.{ .status = "ready" });
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
        "/ml",
        "/antfly",
        "/metadata",
        "/internal",
        "/mcp",
        "/healthz",
        "/readyz",
        "/registry",
    };
    for (reserved) |prefix| {
        if (std.mem.eql(u8, path, prefix)) return true;
        if (path.len > prefix.len and std.mem.startsWith(u8, path, prefix) and path[prefix.len] == '/') return true;
    }
    return false;
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

fn localReplicaRootReconcilePermitHook(data_server: *antfly.data.runtime.DataServer) antfly.metadata_service.LocalReplicaRootReconcilePermitHook {
    return .{
        .ptr = data_server,
        .vtable = &.{
            .should_reconcile = runLocalReplicaRootReconcilePermitHook,
        },
    };
}

fn runLocalReplicaRootReconcileHook(ptr: *anyopaque) !void {
    const data_server: *antfly.data.runtime.DataServer = @ptrCast(@alignCast(ptr));
    try data_server.reconcileVisibleProvisionedReplicaState();
}

fn runLocalReplicaRootReconcilePermitHook(ptr: *anyopaque) bool {
    const data_server: *antfly.data.runtime.DataServer = @ptrCast(@alignCast(ptr));
    return !data_server.shouldDeferProvisionedReplicaRootReconcile();
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn readFileAlloc(alloc: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_bytes));
}

fn writeFileAtomically(alloc: std.mem.Allocator, path: []const u8, contents: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-swarm-metadata-{d}", .{ path, platform_time.monotonicNs() });
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
    }

    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        return err;
    };
}

fn registerInternalGroupRoutes(server: anytype) !void {
    const routes = antfly.public_api.http_routes.Routes;
    const group_prefix = routes.internal_groups_prefix ++ ":group_id";
    const table_prefix = group_prefix ++ "/tables/:table_name";
    const internal_table_prefix = routes.internal_tables_prefix ++ ":table_name";

    const get_routes = [_][]const u8{
        group_prefix ++ routes.group_db_median_key_suffix,
        table_prefix ++ routes.lookup_marker ++ ":key",
    };
    inline for (get_routes) |path| {
        try server.get(path, internalBridgeHandler);
    }

    const post_routes = [_][]const u8{
        internal_table_prefix ++ routes.corrupt_embedding_artifact_suffix,
        group_prefix ++ routes.shard_ops_observe_split_suffix,
        group_prefix ++ routes.shard_ops_observe_merge_suffix,
        group_prefix ++ routes.shard_ops_execute_suffix,
        table_prefix ++ routes.lookup_suffix,
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
        routes.matchInternalTableCorruptEmbeddingArtifact(path) == null)
    {
        _ = ctx.status(404);
        return ctx.text("not found");
    }

    const server = active_api_server orelse {
        _ = ctx.status(503);
        return ctx.text("not ready");
    };

    // Reconstruct HttpRequest for legacy handler.
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

    var resp = (try server.handleInternalRoute(legacy_req)) orelse {
        _ = ctx.status(404);
        return ctx.text("not found");
    };
    return AntflyApiHandler.respond(ctx, &resp);
}

// Module-level pointer set by the serve thread before listen().
// Used by explicitly registered internal bridge handlers.
var active_api_server: ?*antfly.public_api.http_server.ApiHttpServer = null;

// ---------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------

fn parseCli(args: *std.process.Args.Iterator) !CliConfig {
    var cfg = CliConfig{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            cfg.help = true;
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
        if (std.mem.eql(u8, arg, "--tick-ms")) {
            cfg.tick_ms = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
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
        if (std.mem.eql(u8, arg, "--models-dir")) {
            cfg.inference_models_dir = args.next() orelse return error.InvalidArguments;
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
        if (std.mem.eql(u8, arg, "--data-dir")) {
            cfg.data_dir = args.next() orelse return error.InvalidArguments;
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
        if (std.mem.eql(u8, arg, "--secret-store-path")) {
            cfg.secret_store_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        return error.InvalidArguments;
    }
    return cfg;
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
    const secret_store_path = if (cli.secret_store_path) |path|
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
        .secret_store_path = secret_store_path,
        .auth_store_root_dir = auth_store_root_dir,
    };
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
        \\Usage: antfly swarm [options]
        \\
        \\Options:
        \\  --config <path>                       JSON common config file
        \\  --host <host>                         Public API host (default: 127.0.0.1)
        \\  --port <port>                         Public API port (default: 8080)
        \\  --id <node-id>                        Local node id (default: 1)
        \\  --health <true|false>                 Enable health/metrics server (default: true)
        \\  --health-port <port>                  Dedicated health/metrics port on --host (default: 4200)
        \\  --tick-ms <ms>                        Sleep interval while serving (default: 25)
        \\  --models-dir <path>                   Embedded inference models directory (default: ~/.antfly/inference/models)
        \\  --inference-host-budget-mb <n>        Embedded inference native generation host budget override
        \\  --inference-backend-budget-mb <n>     Embedded inference native generation backend budget override
        \\  --inference-combined-budget-mb <n>    Embedded inference native generation combined budget override
        \\  --inference-kv-budget-mb <n>          Embedded inference native generation KV cache budget override
        \\  --inference-scratch-budget-mb <n>     Embedded inference native generation scratch budget override
        \\  --data-dir <path>                     Local Antfly data directory root
        \\  --replica-root-dir <path>             Replica root directory
        \\  --replica-catalog-path <path>         Replica catalog file path
        \\  --snapshot-root-dir <path>            Snapshot root directory
        \\  --secret-store-path <path>            Antfly secrets.json file path
        \\  -h, --help                            Show this help
        \\
    , .{});
}

fn parseBoolFlag(raw: []const u8) ?bool {
    if (std.mem.eql(u8, raw, "true")) return true;
    if (std.mem.eql(u8, raw, "false")) return false;
    return null;
}

const RecordingRouteMethod = enum {
    get,
    post,
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

    fn hasRoute(self: *const @This(), method: RecordingRouteMethod, path: []const u8) bool {
        for (self.routes.items) |route| {
            if (route.method == method and std.mem.eql(u8, route.path, path)) return true;
        }
        return false;
    }
};

test "swarm runtime module compiles" {
    _ = run;
    _ = runFromIterator;
}

test "swarm runtime local generator accepts media url data uris" {
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

test "swarm runtime leaves auth disabled unless config or cli enables it" {
    try std.testing.expect(!resolveAuthEnabled(.{}, null));
    try std.testing.expect(resolveAuthEnabled(.{ .auth_enabled = true }, null));
    try std.testing.expect(!resolveAuthEnabled(.{ .auth_enabled = false }, null));
}

test "swarm runtime local replica reconcile permit stays blocked while startup debt is unresolved" {
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
    try std.testing.expect(!runLocalReplicaRootReconcilePermitHook(&data_server));

    data_server.last_provision_fingerprint = 99;
    try std.testing.expect(runLocalReplicaRootReconcilePermitHook(&data_server));

    data_server.provisioned_startup_catch_up_active.store(true, .monotonic);
    data_server.provisioned_startup_catch_up_dirty.store(false, .monotonic);
    try std.testing.expect(!runLocalReplicaRootReconcilePermitHook(&data_server));

    data_server.provisioned_startup_catch_up_active.store(false, .monotonic);
    data_server.provisioned_startup_catch_up_dirty.store(false, .monotonic);
    try std.testing.expect(runLocalReplicaRootReconcilePermitHook(&data_server));
}

test "swarm runtime registers internal group routes explicitly" {
    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try registerInternalGroupRoutes(&server);

    const routes = antfly.public_api.http_routes.Routes;
    const group_prefix = routes.internal_groups_prefix ++ ":group_id";
    const table_prefix = group_prefix ++ "/tables/:table_name";
    const internal_table_prefix = routes.internal_tables_prefix ++ ":table_name";

    try std.testing.expect(server.hasRoute(.get, group_prefix ++ routes.group_db_median_key_suffix));
    try std.testing.expect(server.hasRoute(.get, table_prefix ++ routes.lookup_marker ++ ":key"));

    try std.testing.expect(server.hasRoute(.post, internal_table_prefix ++ routes.corrupt_embedding_artifact_suffix));
    try std.testing.expect(server.hasRoute(.post, group_prefix ++ routes.shard_ops_observe_split_suffix));
    try std.testing.expect(server.hasRoute(.post, group_prefix ++ routes.shard_ops_observe_merge_suffix));
    try std.testing.expect(server.hasRoute(.post, group_prefix ++ routes.shard_ops_execute_suffix));
    try std.testing.expect(server.hasRoute(.post, table_prefix ++ routes.lookup_suffix));
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

test "swarm runtime registers antfarm static routes" {
    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try registerAntfarmRoutes(&server);

    try std.testing.expect(server.hasRoute(.get, "/"));
    try std.testing.expect(server.hasRoute(.get, "/assets/*"));
    try std.testing.expect(server.hasRoute(.get, "/fonts/*"));
    try std.testing.expect(server.hasRoute(.get, "/*"));
}

test "swarm runtime antfarm path guards keep api routes reserved" {
    try std.testing.expect(isAntfarmReservedPath("/db/v1/tables"));
    try std.testing.expect(isAntfarmReservedPath("/ai/v1/models"));
    try std.testing.expect(isAntfarmReservedPath("/antfly/readyz"));
    try std.testing.expect(!isAntfarmReservedPath("/models"));
    try std.testing.expect(hasUnsafeStaticPath("../index.html"));
    try std.testing.expect(hasUnsafeStaticPath("%2e%2e/index.html"));
    try std.testing.expect(!hasUnsafeStaticPath("assets/index.js"));
}

test "parse cli accepts config path" {
    var argv = [_][*:0]const u8{ "--config", "antfly.json" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const cfg = try parseCli(&iter);
    try std.testing.expectEqualStrings("antfly.json", cfg.config_path.?);
}

test "parse cli accepts secret store path" {
    var argv = [_][*:0]const u8{ "--secret-store-path", "/run/antfly/secrets/secrets.json" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const cfg = try parseCli(&iter);
    try std.testing.expectEqualStrings("/run/antfly/secrets/secrets.json", cfg.secret_store_path.?);
}

test "parse cli accepts canonical host port and models dir flags" {
    var argv = [_][*:0]const u8{
        "--host",
        "127.0.0.1",
        "--port",
        "8080",
        "--models-dir",
        "/tmp/models",
        "--data-dir",
        "/tmp/antfly-data",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const cfg = try parseCli(&iter);
    try std.testing.expectEqualStrings("127.0.0.1", cfg.bind_host.?);
    try std.testing.expectEqual(@as(u16, 8080), cfg.bind_port.?);
    try std.testing.expectEqualStrings("/tmp/models", cfg.inference_models_dir.?);
    try std.testing.expectEqualStrings("/tmp/antfly-data", cfg.data_dir.?);
}

test "swarm runtime defaults public listener to antfarm port" {
    const listener = resolvePublicListener(.{});
    try std.testing.expectEqualStrings("127.0.0.1", listener.bind_host);
    try std.testing.expectEqual(@as(u16, default_public_port), listener.bind_port);
}

test "swarm public HTTP server uses public API request body limit" {
    const cfg = publicHttpServerConfig("127.0.0.1", 8080);
    try std.testing.expectEqual(antfly.public_api.http_server.public_api_max_request_body_bytes, cfg.max_body_size);
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
        },
    };
    defer cfg.deinit();

    const cli = CliConfig{
        .inference_models_dir = "/tmp/from-cli",
        .inference_backend_budget_mb = 8192,
    };
    try std.testing.expectEqualStrings("/tmp/from-cli", resolveInferenceModelsDir(cli, &cfg).?);
    try std.testing.expectEqual(@as(usize, 8192 * 1024 * 1024), resolveInferenceBudgetOverrides(cli).backend_limit_bytes);
}

test "swarm public api caps keep alive request reuse" {
    try std.testing.expect(public_api_max_requests_per_connection > 0);
    try std.testing.expect(public_api_max_requests_per_connection < 1000);
}

test "swarm public api body limit matches common http listener" {
    try std.testing.expectEqual(antfly.common.http.default_max_request_bytes, public_api_max_body_size);
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
    const cfg = try parseCli(&iter);
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
        },
    };
    defer cfg.deinit();

    try std.testing.expectEqualStrings("/tmp/antfly-models", resolveInferenceModelsDir(.{}, &cfg).?);
}

test "swarm runtime resolves paths from common storage base dir" {
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
    try std.testing.expectEqualStrings(expected_replica_root, resolved.replica_root_dir);
    try std.testing.expectEqualStrings(expected_replica_catalog, resolved.replica_catalog_path);
    try std.testing.expectEqualStrings(expected_local_metadata, resolved.local_metadata_catalog_path);
    try std.testing.expectEqualStrings(expected_snapshot_root, resolved.snapshot_root_dir);
    const expected_secret_store = try normalizeResolvedPathAlloc(alloc, "/tmp/antflydb/secrets.json");
    defer alloc.free(expected_secret_store);
    try std.testing.expectEqualStrings(expected_secret_store, resolved.secret_store_path);
}

test "swarm runtime resolves explicit secret store path" {
    const alloc = std.testing.allocator;
    const resolved = try resolvePaths(alloc, .{ .secret_store_path = "/run/antfly/secrets/secrets.json" }, null);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("/run/antfly/secrets/secrets.json", resolved.secret_store_path);
}

test "swarm runtime data dir overrides common storage base dir" {
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
