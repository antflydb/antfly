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
const raft_engine = @import("raft_engine");
const catalog = @import("catalog.zig");
const data_storage = @import("../data/storage/mod.zig");
const host_mod = @import("host.zig");
const leader_runtime = @import("leader_runtime.zig");
const metadata_table_provisioner = @import("../metadata/table_provisioner.zig");
const metadata_storage = @import("../metadata/storage/mod.zig");
const metadata_view = @import("metadata_view.zig");
const reconciler = @import("reconciler.zig");
const state_machine = @import("state_machine/mod.zig");
const storage = @import("storage/mod.zig");
const backup_restore = @import("storage/backup_restore.zig");

pub const ManagedHostConfig = struct {
    host: host_mod.HostConfig,
    wal_replica_state: storage.WalReplicaStateConfig = .{},
    wal_flush_on_deinit: bool = true,
};

pub const ManagedHostDeps = struct {
    host: host_mod.HostDeps = .{},
    metadata_snapshot_builder: ?state_machine.SnapshotBuilder = null,
    data_snapshot_builder: ?state_machine.SnapshotBuilder = null,
    leader_observer: ?leader_runtime.LeaderObserver = null,
    read_state_observer: ?state_machine.ReadStateObserver = null,
};

pub const ManagedHttpHostConfig = struct {
    http: host_mod.HttpHostConfig,
    wal_replica_state: storage.WalReplicaStateConfig = .{},
    wal_flush_on_deinit: bool = true,
    replica_apply_store_no_sync: bool = false,
};

pub const ManagedHttpHostDeps = struct {
    http: host_mod.HttpHostDeps = .{},
    metadata_snapshot_builder: ?state_machine.SnapshotBuilder = null,
    data_snapshot_builder: ?state_machine.SnapshotBuilder = null,
    leader_observer: ?leader_runtime.LeaderObserver = null,
    read_state_observer: ?state_machine.ReadStateObserver = null,
};

pub const ManagedSyncResult = struct {
    reconcile: reconciler.ReconcileResult,
    runtime: raft_engine.runtime.multi_raft.HostRound,
};

pub const ManagedHost = struct {
    alloc: std.mem.Allocator,
    view: *metadata_view.MetadataView,
    host: *host_mod.Host,
    owned_backup_restore_bootstrapper: ?*ReplicaBackupRestoreBootstrapper = null,
    owned_replica_catalog: ?*storage.FileReplicaCatalog = null,
    owned_file_replica_provider: ?*storage.PersistentReplicaProvider = null,
    owned_wal_replica_provider: ?*storage.WalReplicaProvider = null,
    owned_metadata_store: ?*metadata_storage.RaftApplyStore = null,
    owned_data_store: ?*data_storage.RaftApplyStore = null,
    owned_metadata_state_machine: ?*state_machine.MetadataStateMachine = null,
    owned_data_state_machine: ?*state_machine.DataStateMachine = null,
    owned_routed_state_machine: ?*state_machine.RoutedStateMachine = null,
    owned_leadership_tracker: ?*leader_runtime.LeadershipTracker = null,
    reconciler_loop: reconciler.Reconciler,

    pub fn init(alloc: std.mem.Allocator, cfg: ManagedHostConfig, deps: ManagedHostDeps) !ManagedHost {
        const view = try alloc.create(metadata_view.MetadataView);
        errdefer alloc.destroy(view);
        view.* = metadata_view.MetadataView.init(alloc);
        errdefer view.deinit();

        var prepared_deps = try prepareHostDeps(
            alloc,
            cfg.host,
            cfg.wal_replica_state,
            cfg.wal_flush_on_deinit,
            false,
            deps.host,
            deps.metadata_snapshot_builder,
            deps.data_snapshot_builder,
            deps.read_state_observer,
        );
        errdefer prepared_deps.deinit(alloc);

        const host = try alloc.create(host_mod.Host);
        errdefer alloc.destroy(host);
        var host_deps = prepared_deps.host;
        host_deps.peer_resolver = view.peerResolver();
        host.* = host_mod.Host.init(alloc, cfg.host, host_deps);
        errdefer host.deinit();
        if (host_deps.replica_catalog != null) _ = try host.restoreReplicasFromCatalog(alloc);

        const owned_leadership_tracker = if (deps.leader_observer) |observer| blk: {
            const tracker = try alloc.create(leader_runtime.LeadershipTracker);
            tracker.* = leader_runtime.LeadershipTracker.init(alloc, cfg.host.local_node_id, observer);
            break :blk tracker;
        } else null;

        return .{
            .alloc = alloc,
            .view = view,
            .host = host,
            .owned_backup_restore_bootstrapper = prepared_deps.owned_backup_restore_bootstrapper,
            .owned_replica_catalog = prepared_deps.owned_replica_catalog,
            .owned_file_replica_provider = prepared_deps.owned_file_replica_provider,
            .owned_wal_replica_provider = prepared_deps.owned_wal_replica_provider,
            .owned_metadata_store = prepared_deps.owned_metadata_store,
            .owned_data_store = prepared_deps.owned_data_store,
            .owned_metadata_state_machine = prepared_deps.owned_metadata_state_machine,
            .owned_data_state_machine = prepared_deps.owned_data_state_machine,
            .owned_routed_state_machine = prepared_deps.owned_routed_state_machine,
            .owned_leadership_tracker = owned_leadership_tracker,
            .reconciler_loop = .{
                .alloc = alloc,
                .host = host,
                .provider = view.placementProvider(),
            },
        };
    }

    pub fn deinit(self: *ManagedHost) void {
        if (self.owned_leadership_tracker) |tracker| {
            _ = tracker.releaseAll() catch 0;
        }
        self.reconciler_loop.deinit();
        self.host.deinit();
        self.alloc.destroy(self.host);
        if (self.owned_backup_restore_bootstrapper) |bootstrapper| {
            bootstrapper.deinit(self.alloc);
            self.alloc.destroy(bootstrapper);
        }
        if (self.owned_file_replica_provider) |provider| {
            provider.deinit();
            self.alloc.destroy(provider);
        }
        if (self.owned_wal_replica_provider) |provider| {
            provider.deinit();
            self.alloc.destroy(provider);
        }
        if (self.owned_data_store) |store| {
            store.deinit();
            self.alloc.destroy(store);
        }
        if (self.owned_metadata_store) |store| {
            store.deinit();
            self.alloc.destroy(store);
        }
        if (self.owned_routed_state_machine) |sm| self.alloc.destroy(sm);
        if (self.owned_data_state_machine) |sm| self.alloc.destroy(sm);
        if (self.owned_metadata_state_machine) |sm| self.alloc.destroy(sm);
        if (self.owned_leadership_tracker) |tracker| {
            tracker.deinit();
            self.alloc.destroy(tracker);
        }
        if (self.owned_replica_catalog) |replica_catalog| {
            replica_catalog.deinit();
            self.alloc.destroy(replica_catalog);
        }
        self.view.deinit();
        self.alloc.destroy(self.view);
        self.* = undefined;
    }

    pub fn apply(self: *ManagedHost, update: metadata_view.MetadataUpdate) !void {
        try self.view.apply(update);
        try self.applyRuntimeUpdate(update);
    }

    pub fn reconcileOnce(self: *ManagedHost) !reconciler.ReconcileResult {
        return try self.reconciler_loop.reconcileOnce();
    }

    pub fn replacePlacementIntents(self: *ManagedHost, intents: []const reconciler.PlacementIntent) !void {
        try self.view.replaceReplicaIntents(intents);
    }

    pub fn applyBatch(self: *ManagedHost, updates: []const metadata_view.MetadataUpdate) !void {
        for (updates) |update| try self.apply(update);
    }

    pub fn applyAndReconcile(self: *ManagedHost, updates: []const metadata_view.MetadataUpdate) !reconciler.ReconcileResult {
        try self.applyBatch(updates);
        return try self.reconcileOnce();
    }

    pub fn runRound(self: *ManagedHost, max_tick_groups: usize, max_ready_groups: usize) !raft_engine.runtime.multi_raft.HostRound {
        const round = try self.host.runRound(max_tick_groups, max_ready_groups);
        _ = try self.pollLeadership();
        return round;
    }

    pub fn status(self: *ManagedHost, group_id: u64) host_mod.HostedReplicaStatus {
        return self.host.status(group_id);
    }

    pub fn bootstrapStatus(self: *const ManagedHost, group_id: u64) ?host_mod.BootstrapStatus {
        return self.host.bootstrapStatus(group_id);
    }

    pub fn listBootstrapStatuses(self: *const ManagedHost, alloc: std.mem.Allocator) ![]host_mod.BootstrapStatus {
        return try self.host.listBootstrapStatuses(alloc);
    }

    pub fn syncOnce(
        self: *ManagedHost,
        updates: []const metadata_view.MetadataUpdate,
        max_tick_groups: usize,
        max_ready_groups: usize,
    ) !ManagedSyncResult {
        const reconcile_result = try self.applyAndReconcile(updates);
        const runtime_round = try self.runRound(max_tick_groups, max_ready_groups);
        return .{
            .reconcile = reconcile_result,
            .runtime = runtime_round,
        };
    }

    pub fn pollLeadership(self: *ManagedHost) !usize {
        if (self.owned_leadership_tracker) |tracker| {
            return try tracker.pollHost(self.host);
        }
        return 0;
    }

    fn applyRuntimeUpdate(self: *ManagedHost, update: metadata_view.MetadataUpdate) !void {
        switch (update) {
            .replica_intent => {},
            .peer_route => |route| switch (route) {
                .upsert => |record| {
                    _ = try self.host.upsertResolvedPeerEndpoints(record.group_id, record.node_id, record.endpoints);
                },
                .remove => |record| {
                    _ = try self.host.removePeerRoute(record.group_id, record.node_id);
                },
            },
            .transition => {},
        }
    }
};

pub const ManagedHttpHost = struct {
    alloc: std.mem.Allocator,
    view: *metadata_view.MetadataView,
    http_host: *host_mod.HttpHost,
    owned_backup_restore_bootstrapper: ?*ReplicaBackupRestoreBootstrapper = null,
    owned_replica_catalog: ?*storage.FileReplicaCatalog = null,
    owned_file_replica_provider: ?*storage.PersistentReplicaProvider = null,
    owned_wal_replica_provider: ?*storage.WalReplicaProvider = null,
    owned_metadata_store: ?*metadata_storage.RaftApplyStore = null,
    owned_data_store: ?*data_storage.RaftApplyStore = null,
    owned_metadata_state_machine: ?*state_machine.MetadataStateMachine = null,
    owned_data_state_machine: ?*state_machine.DataStateMachine = null,
    owned_routed_state_machine: ?*state_machine.RoutedStateMachine = null,
    owned_leadership_tracker: ?*leader_runtime.LeadershipTracker = null,
    reconciler_loop: reconciler.Reconciler,

    pub fn init(alloc: std.mem.Allocator, cfg: ManagedHttpHostConfig, deps: ManagedHttpHostDeps) !ManagedHttpHost {
        const view = try alloc.create(metadata_view.MetadataView);
        errdefer alloc.destroy(view);
        view.* = metadata_view.MetadataView.init(alloc);
        errdefer view.deinit();

        var prepared_deps = try prepareHostDeps(
            alloc,
            cfg.http.host,
            cfg.wal_replica_state,
            cfg.wal_flush_on_deinit,
            cfg.replica_apply_store_no_sync,
            deps.http.host,
            deps.metadata_snapshot_builder,
            deps.data_snapshot_builder,
            deps.read_state_observer,
        );
        errdefer prepared_deps.deinit(alloc);

        const http_host = try alloc.create(host_mod.HttpHost);
        errdefer alloc.destroy(http_host);
        var http_deps = deps.http;
        http_deps.host = prepared_deps.host;
        http_deps.host.peer_resolver = view.peerResolver();
        http_host.* = try host_mod.HttpHost.init(alloc, cfg.http, http_deps);
        errdefer http_host.deinit();
        if (http_deps.host.replica_catalog != null) _ = try http_host.host.restoreReplicasFromCatalog(alloc);

        const owned_leadership_tracker = if (deps.leader_observer) |observer| blk: {
            const tracker = try alloc.create(leader_runtime.LeadershipTracker);
            tracker.* = leader_runtime.LeadershipTracker.init(alloc, cfg.http.host.local_node_id, observer);
            break :blk tracker;
        } else null;

        return .{
            .alloc = alloc,
            .view = view,
            .http_host = http_host,
            .owned_backup_restore_bootstrapper = prepared_deps.owned_backup_restore_bootstrapper,
            .owned_replica_catalog = prepared_deps.owned_replica_catalog,
            .owned_file_replica_provider = prepared_deps.owned_file_replica_provider,
            .owned_wal_replica_provider = prepared_deps.owned_wal_replica_provider,
            .owned_metadata_store = prepared_deps.owned_metadata_store,
            .owned_data_store = prepared_deps.owned_data_store,
            .owned_metadata_state_machine = prepared_deps.owned_metadata_state_machine,
            .owned_data_state_machine = prepared_deps.owned_data_state_machine,
            .owned_routed_state_machine = prepared_deps.owned_routed_state_machine,
            .owned_leadership_tracker = owned_leadership_tracker,
            .reconciler_loop = .{
                .alloc = alloc,
                .host = http_host.host,
                .provider = view.placementProvider(),
            },
        };
    }

    pub fn deinit(self: *ManagedHttpHost) void {
        if (self.owned_leadership_tracker) |tracker| {
            _ = tracker.releaseAll() catch 0;
        }
        self.reconciler_loop.deinit();
        self.http_host.deinit();
        self.alloc.destroy(self.http_host);
        if (self.owned_backup_restore_bootstrapper) |bootstrapper| {
            bootstrapper.deinit(self.alloc);
            self.alloc.destroy(bootstrapper);
        }
        if (self.owned_file_replica_provider) |provider| {
            provider.deinit();
            self.alloc.destroy(provider);
        }
        if (self.owned_wal_replica_provider) |provider| {
            provider.deinit();
            self.alloc.destroy(provider);
        }
        if (self.owned_data_store) |store| {
            store.deinit();
            self.alloc.destroy(store);
        }
        if (self.owned_metadata_store) |store| {
            store.deinit();
            self.alloc.destroy(store);
        }
        if (self.owned_routed_state_machine) |sm| self.alloc.destroy(sm);
        if (self.owned_data_state_machine) |sm| self.alloc.destroy(sm);
        if (self.owned_metadata_state_machine) |sm| self.alloc.destroy(sm);
        if (self.owned_leadership_tracker) |tracker| {
            tracker.deinit();
            self.alloc.destroy(tracker);
        }
        if (self.owned_replica_catalog) |replica_catalog| {
            replica_catalog.deinit();
            self.alloc.destroy(replica_catalog);
        }
        self.view.deinit();
        self.alloc.destroy(self.view);
        self.* = undefined;
    }

    pub fn start(self: *ManagedHttpHost) !void {
        try self.http_host.start();
    }

    pub fn stop(self: *ManagedHttpHost) void {
        self.http_host.stop();
    }

    pub fn baseUri(self: *ManagedHttpHost, alloc: std.mem.Allocator) ![]u8 {
        return try self.http_host.baseUri(alloc);
    }

    pub fn apply(self: *ManagedHttpHost, update: metadata_view.MetadataUpdate) !void {
        try self.view.apply(update);
        try self.applyRuntimeUpdate(update);
    }

    pub fn reconcileOnce(self: *ManagedHttpHost) !reconciler.ReconcileResult {
        return try self.reconciler_loop.reconcileOnce();
    }

    pub fn replacePlacementIntents(self: *ManagedHttpHost, intents: []const reconciler.PlacementIntent) !void {
        try self.view.replaceReplicaIntents(intents);
    }

    pub fn applyBatch(self: *ManagedHttpHost, updates: []const metadata_view.MetadataUpdate) !void {
        for (updates) |update| try self.apply(update);
    }

    pub fn applyAndReconcile(self: *ManagedHttpHost, updates: []const metadata_view.MetadataUpdate) !reconciler.ReconcileResult {
        try self.applyBatch(updates);
        return try self.reconcileOnce();
    }

    pub fn runRound(self: *ManagedHttpHost, max_tick_groups: usize, max_ready_groups: usize) !raft_engine.runtime.multi_raft.HostRound {
        const round = try self.http_host.runRound(max_tick_groups, max_ready_groups);
        _ = try self.pollLeadership();
        return round;
    }

    pub fn status(self: *ManagedHttpHost, group_id: u64) host_mod.HostedReplicaStatus {
        return self.http_host.status(group_id);
    }

    pub fn bootstrapStatus(self: *const ManagedHttpHost, group_id: u64) ?host_mod.BootstrapStatus {
        return self.http_host.bootstrapStatus(group_id);
    }

    pub fn listBootstrapStatuses(self: *const ManagedHttpHost, alloc: std.mem.Allocator) ![]host_mod.BootstrapStatus {
        return try self.http_host.listBootstrapStatuses(alloc);
    }

    pub fn syncOnce(
        self: *ManagedHttpHost,
        updates: []const metadata_view.MetadataUpdate,
        max_tick_groups: usize,
        max_ready_groups: usize,
    ) !ManagedSyncResult {
        const reconcile_result = try self.applyAndReconcile(updates);
        const runtime_round = try self.runRound(max_tick_groups, max_ready_groups);
        return .{
            .reconcile = reconcile_result,
            .runtime = runtime_round,
        };
    }

    pub fn pollLeadership(self: *ManagedHttpHost) !usize {
        if (self.owned_leadership_tracker) |tracker| {
            return try tracker.pollHttpHost(self.http_host);
        }
        return 0;
    }

    fn applyRuntimeUpdate(self: *ManagedHttpHost, update: metadata_view.MetadataUpdate) !void {
        switch (update) {
            .replica_intent => {},
            .peer_route => |route| switch (route) {
                .upsert => |record| {
                    _ = try self.http_host.upsertResolvedPeerEndpoints(record.group_id, record.node_id, record.endpoints);
                },
                .remove => |record| {
                    _ = try self.http_host.removePeerRoute(record.group_id, record.node_id);
                },
            },
            .transition => {},
        }
    }
};

const PreparedHostDeps = struct {
    host: host_mod.HostDeps,
    owned_backup_restore_bootstrapper: ?*ReplicaBackupRestoreBootstrapper = null,
    owned_replica_catalog: ?*storage.FileReplicaCatalog = null,
    owned_file_replica_provider: ?*storage.PersistentReplicaProvider = null,
    owned_wal_replica_provider: ?*storage.WalReplicaProvider = null,
    owned_metadata_store: ?*metadata_storage.RaftApplyStore = null,
    owned_data_store: ?*data_storage.RaftApplyStore = null,
    owned_metadata_state_machine: ?*state_machine.MetadataStateMachine = null,
    owned_data_state_machine: ?*state_machine.DataStateMachine = null,
    owned_routed_state_machine: ?*state_machine.RoutedStateMachine = null,

    fn deinit(self: *PreparedHostDeps, alloc: std.mem.Allocator) void {
        if (self.owned_backup_restore_bootstrapper) |bootstrapper| {
            bootstrapper.deinit(alloc);
            alloc.destroy(bootstrapper);
        }
        if (self.owned_file_replica_provider) |provider| {
            provider.deinit();
            alloc.destroy(provider);
        }
        if (self.owned_wal_replica_provider) |provider| {
            provider.deinit();
            alloc.destroy(provider);
        }
        if (self.owned_data_store) |store| {
            store.deinit();
            alloc.destroy(store);
        }
        if (self.owned_metadata_store) |store| {
            store.deinit();
            alloc.destroy(store);
        }
        if (self.owned_routed_state_machine) |sm| alloc.destroy(sm);
        if (self.owned_data_state_machine) |sm| alloc.destroy(sm);
        if (self.owned_metadata_state_machine) |sm| alloc.destroy(sm);
        if (self.owned_replica_catalog) |replica_catalog| {
            replica_catalog.deinit();
            alloc.destroy(replica_catalog);
        }
        self.* = undefined;
    }
};

const ReplicaBackupRestoreBootstrapper = struct {
    replica_root_dir: []u8,

    fn init(alloc: std.mem.Allocator, replica_root_dir: []const u8) !ReplicaBackupRestoreBootstrapper {
        return .{
            .replica_root_dir = try alloc.dupe(u8, replica_root_dir),
        };
    }

    fn deinit(self: *ReplicaBackupRestoreBootstrapper, alloc: std.mem.Allocator) void {
        alloc.free(self.replica_root_dir);
        self.* = undefined;
    }

    fn iface(self: *@This()) host_mod.BackupRestoreBootstrapper {
        return .{
            .ptr = self,
            .vtable = &.{
                .prepare_backup_restore = prepareBackupRestore,
            },
        };
    }

    fn prepareBackupRestore(ptr: *anyopaque, record: catalog.ReplicaRecord) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const restore = record.backup_restore_bootstrap orelse return;
        try metadata_table_provisioner.applyBackupRestoreBootstrap(
            std.heap.page_allocator,
            self.replica_root_dir,
            record.group_id,
            restore,
        );
    }
};

fn prepareHostDeps(
    alloc: std.mem.Allocator,
    cfg: host_mod.HostConfig,
    wal_replica_state_cfg: storage.WalReplicaStateConfig,
    wal_flush_on_deinit: bool,
    replica_apply_store_no_sync: bool,
    base: host_mod.HostDeps,
    metadata_snapshot_builder: ?state_machine.SnapshotBuilder,
    data_snapshot_builder: ?state_machine.SnapshotBuilder,
    read_state_observer: ?state_machine.ReadStateObserver,
) !PreparedHostDeps {
    var prepared = PreparedHostDeps{ .host = base };
    var effective_metadata_builder = metadata_snapshot_builder;
    var effective_data_builder = data_snapshot_builder;
    var applied_sink: ?state_machine.AppliedIndexSink = null;

    if (cfg.replica_catalog_path) |replica_catalog_path| {
        if (prepared.host.replica_catalog == null) {
            const owned_catalog = try alloc.create(storage.FileReplicaCatalog);
            errdefer alloc.destroy(owned_catalog);
            owned_catalog.* = try storage.FileReplicaCatalog.init(alloc, replica_catalog_path);
            prepared.owned_replica_catalog = owned_catalog;
            prepared.host.replica_catalog = owned_catalog.catalog();
        }
    }

    if (cfg.replica_root_dir) |replica_root_dir| {
        if (prepared.host.backup_restore_bootstrapper == null) {
            const bootstrapper = try alloc.create(ReplicaBackupRestoreBootstrapper);
            errdefer alloc.destroy(bootstrapper);
            bootstrapper.* = try ReplicaBackupRestoreBootstrapper.init(alloc, replica_root_dir);
            prepared.owned_backup_restore_bootstrapper = bootstrapper;
            prepared.host.backup_restore_bootstrapper = bootstrapper.iface();
        }
        if (effective_metadata_builder == null) {
            const owned_store = try alloc.create(metadata_storage.RaftApplyStore);
            errdefer alloc.destroy(owned_store);
            owned_store.* = try metadata_storage.RaftApplyStore.init(alloc, .{
                .root_dir = replica_root_dir,
                .no_sync = replica_apply_store_no_sync,
            });
            prepared.owned_metadata_store = owned_store;
            effective_metadata_builder = owned_store.snapshotBuilder();
        }
        if (effective_data_builder == null) {
            const owned_store = try alloc.create(data_storage.RaftApplyStore);
            errdefer alloc.destroy(owned_store);
            owned_store.* = try data_storage.RaftApplyStore.init(alloc, .{
                .root_dir = replica_root_dir,
                .no_sync = replica_apply_store_no_sync,
            });
            prepared.owned_data_store = owned_store;
            effective_data_builder = owned_store.snapshotBuilder();
        }

        const base_factory = prepared.host.descriptor_factory orelse return error.MissingReplicaDescriptorFactory;
        switch (cfg.replica_state_backend) {
            .file_image => {
                const owned_provider = try alloc.create(storage.PersistentReplicaProvider);
                errdefer alloc.destroy(owned_provider);
                owned_provider.* = try storage.PersistentReplicaProvider.init(alloc, .{
                    .root_dir = replica_root_dir,
                }, base_factory);
                prepared.owned_file_replica_provider = owned_provider;
                prepared.host.descriptor_factory = owned_provider.descriptorFactory();
                prepared.host.runtime_hooks = host_mod.mergeRuntimeHooks(prepared.host.runtime_hooks, owned_provider.runtimeHooks());
                applied_sink = owned_provider.appliedIndexSink();
            },
            .wal => {
                const owned_provider = try alloc.create(storage.WalReplicaProvider);
                errdefer alloc.destroy(owned_provider);
                owned_provider.* = try storage.WalReplicaProvider.init(alloc, .{
                    .root_dir = replica_root_dir,
                    .state = wal_replica_state_cfg,
                    .flush_on_deinit = wal_flush_on_deinit,
                }, base_factory);
                prepared.owned_wal_replica_provider = owned_provider;
                prepared.host.descriptor_factory = owned_provider.descriptorFactory();
                prepared.host.runtime_hooks = host_mod.mergeRuntimeHooks(prepared.host.runtime_hooks, owned_provider.runtimeHooks());
                applied_sink = owned_provider.appliedIndexSink();
            },
        }
    }

    if (prepared.host.runtime_hooks.apply_queue == null and
        (applied_sink != null or
            read_state_observer != null or
            effective_metadata_builder != null or
            effective_data_builder != null))
    {
        try installApplyStateMachines(
            alloc,
            cfg,
            &prepared,
            applied_sink orelse state_machine.noopAppliedIndexSink(),
            effective_metadata_builder,
            effective_data_builder,
            read_state_observer,
        );
    }

    return prepared;
}

fn installApplyStateMachines(
    alloc: std.mem.Allocator,
    cfg: host_mod.HostConfig,
    prepared: *PreparedHostDeps,
    applied_sink: state_machine.AppliedIndexSink,
    metadata_snapshot_builder: ?state_machine.SnapshotBuilder,
    data_snapshot_builder: ?state_machine.SnapshotBuilder,
    read_state_observer: ?state_machine.ReadStateObserver,
) !void {
    if (prepared.host.runtime_hooks.apply_queue != null) return;

    const delegate = prepared.host.runtime_hooks.state_machine;

    const metadata_sm = try alloc.create(state_machine.MetadataStateMachine);
    errdefer alloc.destroy(metadata_sm);
    metadata_sm.* = .{
        .alloc = alloc,
        .applied_sink = applied_sink,
        .snapshot_builder = metadata_snapshot_builder,
        .delegate = delegate,
    };

    const data_sm = try alloc.create(state_machine.DataStateMachine);
    errdefer alloc.destroy(data_sm);
    data_sm.* = .{
        .alloc = alloc,
        .applied_sink = applied_sink,
        .snapshot_builder = data_snapshot_builder,
        .delegate = delegate,
    };

    const routed_sm = try alloc.create(state_machine.RoutedStateMachine);
    errdefer alloc.destroy(routed_sm);
    routed_sm.* = .{
        .metadata_group_id = cfg.metadata_group_id,
        .metadata_state_machine = metadata_sm.stateMachine(),
        .data_state_machine = data_sm.stateMachine(),
        .read_state_observer = read_state_observer,
    };

    prepared.owned_metadata_state_machine = metadata_sm;
    prepared.owned_data_state_machine = data_sm;
    prepared.owned_routed_state_machine = routed_sm;
    prepared.host.runtime_hooks.state_machine = routed_sm.stateMachine();
}

test "managed host applies metadata updates and reconciles replicas" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{ record.local_node_id, 2 });
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var managed = try ManagedHost.init(std.testing.allocator, .{
        .host = .{ .local_node_id = 1 },
    }, .{
        .host = .{ .descriptor_factory = factory.iface() },
    });
    defer managed.deinit();

    try managed.apply(.{
        .replica_intent = .{
            .upsert = .{
                .record = .{
                    .group_id = 700,
                    .replica_id = 1,
                    .local_node_id = 1,
                },
                .peer_node_ids = &.{2},
            },
        },
    });
    try managed.apply(.{
        .peer_route = .{
            .upsert = .{
                .group_id = 700,
                .node_id = 2,
                .endpoints = &.{
                    .{
                        .protocol = .http,
                        .address = "http://n2:9000",
                        .metadata = "",
                    },
                },
            },
        },
    });

    const result = try managed.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 1), result.ensured);
    try std.testing.expectEqual(@as(usize, 1), result.refreshed_peers);
    try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, managed.host.status(700));
}

test "managed host restores replicas from file-backed catalog and persisted state" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-raft", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-raft/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var dummy_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer dummy_store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &dummy_store };

    {
        var managed = try ManagedHost.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        });
        defer managed.deinit();

        _ = try managed.host.ensureReplica(.{
            .group_id = 901,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });
        try managed.host.campaignGroup(901);
        _ = try managed.host.runRound(1, 1);
        try managed.host.propose(901, "restore-me");
        _ = try managed.host.runRound(1, 1);
    }

    {
        var managed = try ManagedHost.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        });
        defer managed.deinit();

        try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, managed.host.status(901));
        const group_ids = try managed.host.listGroupIds(std.testing.allocator);
        defer std.testing.allocator.free(group_ids);
        try std.testing.expectEqual(@as(usize, 1), group_ids.len);
        try std.testing.expectEqual(@as(u64, 901), group_ids[0]);
    }
}

test "managed host restores backup bootstrap replicas from file-backed catalog on restart" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const db_mod = @import("../storage/db/mod.zig");
    const backups_api = @import("../api/backups.zig");

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-raft-backup", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-raft-backup-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-raft-backups", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);
    const source_db_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-raft-backup/source", .{tmp.sub_path});
    defer std.testing.allocator.free(source_db_path);

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), source_db_path) catch {};
    std.Io.Dir.cwd().deleteFile(io_impl.io(), replica_catalog_path) catch {};

    var source_db = try db_mod.DB.open(std.testing.allocator, source_db_path, .{});
    defer {
        source_db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), source_db_path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
        std.Io.Dir.cwd().deleteFile(io_impl.io(), replica_catalog_path) catch {};
    }
    try source_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
        .sync_level = .full_index,
    });
    _ = try source_db.snapshot("snap1-g903");

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, "{s}.snapshots/snap1-g903", .{source_db_path});
    defer std.testing.allocator.free(snapshot_root);
    const dest_root = try backups_api.shardSnapshotPath(std.testing.allocator, backup_root, "snap1", 903);
    defer std.testing.allocator.free(dest_root);
    try backups_api.copyDirectoryRecursive(std.testing.allocator, snapshot_root, dest_root);
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const backup_root_abs = try std.fs.path.resolve(std.testing.allocator, &.{ cwd, backup_root });
    defer std.testing.allocator.free(backup_root_abs);
    const restore_location = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{backup_root_abs});
    defer std.testing.allocator.free(restore_location);

    const manifest = try backups_api.createManifest(
        std.testing.allocator,
        "snap1",
        &.{
            .table_id = 7,
            .name = "docs",
            .description = "docs table",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        },
        &.{.{
            .group_id = 903,
            .start_key = "doc:a",
            .end_key = null,
            .snapshot_path = "snap1/groups/903",
        }},
    );
    defer {
        var owned = manifest;
        owned.deinit(std.testing.allocator);
    }
    try backups_api.writeManifest(std.testing.allocator, backup_root, &manifest);

    var dummy_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer dummy_store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &dummy_store };

    {
        var managed = try ManagedHost.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        });
        defer managed.deinit();

        _ = try managed.host.ensureReplica(.{
            .group_id = 903,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .fetch_snapshot,
            .backup_restore_bootstrap = .{
                .backup_id = "snap1",
                .location = restore_location,
                .snapshot_path = "snap1/groups/903",
            },
        });
    }

    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};

    {
        var managed = try ManagedHost.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        });
        defer managed.deinit();

        try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, managed.host.status(903));
        const bootstrap_status = managed.bootstrapStatus(903) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(.backup_db_snapshot_restore, bootstrap_status.kind);
        try std.testing.expectEqual(.succeeded, bootstrap_status.phase);
        try std.testing.expectEqual(@as(u64, 1), bootstrap_status.attempts);
        try std.testing.expect(bootstrap_status.last_updated_at_millis > 0);
        try std.testing.expect(bootstrap_status.last_error == null);
        try std.testing.expectEqualStrings("snap1", bootstrap_status.backup_id orelse return error.TestExpectedEqual);
        try std.testing.expectEqualStrings("snap1/groups/903", bootstrap_status.snapshot_path orelse return error.TestExpectedEqual);
        const listed = try managed.listBootstrapStatuses(std.testing.allocator);
        defer managed.host.freeBootstrapStatuses(std.testing.allocator, listed);
        try std.testing.expectEqual(@as(usize, 1), listed.len);
        try std.testing.expectEqual(@as(u64, 903), listed[0].group_id);
        try std.testing.expectEqual(.succeeded, listed[0].phase);
        try std.testing.expectEqualStrings("snap1", listed[0].backup_id orelse return error.TestExpectedEqual);
        try std.testing.expectEqualStrings("snap1/groups/903", listed[0].snapshot_path orelse return error.TestExpectedEqual);
    }

    const db_path = try backup_restore.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root, 903);
    defer std.testing.allocator.free(db_path);
    var restored_db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
    defer restored_db.close();
    const doc = (try restored_db.get(std.testing.allocator, "doc:a")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(doc);
    try std.testing.expect(std.mem.indexOf(u8, doc, "\"alpha\"") != null);
}

test "managed host restores replicas from file-backed catalog with WAL replica state" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-raft-wal", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-raft-wal/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var dummy_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer dummy_store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &dummy_store };

    {
        var managed = try ManagedHost.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
                .replica_state_backend = .wal,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        });
        defer managed.deinit();

        _ = try managed.host.ensureReplica(.{
            .group_id = 902,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });
        try managed.host.campaignGroup(902);
        _ = try managed.host.runRound(1, 1);
        try managed.host.propose(902, "restore-me-wal");
        _ = try managed.host.runRound(1, 1);

        const provider = managed.owned_wal_replica_provider orelse return error.TestExpectedEqual;
        const state = provider.stateForGroup(902) orelse return error.TestExpectedEqual;
        const stats = state.statsSnapshot();
        try std.testing.expect(stats.replay_debt_records > 0);
        try std.testing.expect(stats.replay_debt_bytes > 0);
    }

    {
        var managed = try ManagedHost.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
                .replica_state_backend = .wal,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        });
        defer managed.deinit();

        try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, managed.host.status(902));
        const group_ids = try managed.host.listGroupIds(std.testing.allocator);
        defer std.testing.allocator.free(group_ids);
        try std.testing.expectEqual(@as(usize, 1), group_ids.len);
        try std.testing.expectEqual(@as(u64, 902), group_ids[0]);

        const provider = managed.owned_wal_replica_provider orelse return error.TestExpectedEqual;
        const state = provider.stateForGroup(902) orelse return error.TestExpectedEqual;
        const stats = state.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 0), stats.replayed_delta_records);
        try std.testing.expectEqual(@as(u64, 0), stats.replayed_delta_bytes);
        try std.testing.expectEqual(@as(u64, 0), stats.replay_debt_records);
        try std.testing.expectEqual(@as(u64, 0), stats.replay_debt_bytes);
    }
}

test "managed host removes live peer routes on metadata removal" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{ record.local_node_id, 2 });
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var transport = raft_engine.runtime.InMemoryTransportHost.init(std.testing.allocator);
    defer transport.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var managed = try ManagedHost.init(std.testing.allocator, .{
        .host = .{ .local_node_id = 1 },
    }, .{
        .host = .{
            .descriptor_factory = factory.iface(),
            .runtime_hooks = .{ .transport = transport.transport() },
        },
    });
    defer managed.deinit();

    _ = try managed.applyAndReconcile(&.{
        .{
            .replica_intent = .{
                .upsert = .{
                    .record = .{
                        .group_id = 701,
                        .replica_id = 1,
                        .local_node_id = 1,
                    },
                    .peer_node_ids = &.{2},
                },
            },
        },
        .{
            .peer_route = .{
                .upsert = .{
                    .group_id = 701,
                    .node_id = 2,
                    .endpoints = &.{
                        .{
                            .protocol = .http,
                            .address = "http://n2:9000",
                            .metadata = "",
                        },
                    },
                },
            },
        },
    });

    try std.testing.expectEqual(@as(usize, 1), transport.peerCount(701));

    try managed.apply(.{
        .peer_route = .{
            .remove = .{
                .group_id = 701,
                .node_id = 2,
            },
        },
    });

    try std.testing.expectEqual(@as(usize, 0), transport.peerCount(701));
    try std.testing.expectEqual(@as(usize, 1), managed.host.metricsSnapshot().endpoint_removals);
}

test "managed host syncOnce applies metadata and advances runtime" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var managed = try ManagedHost.init(std.testing.allocator, .{
        .host = .{ .local_node_id = 1 },
    }, .{
        .host = .{ .descriptor_factory = factory.iface() },
    });
    defer managed.deinit();

    const result = try managed.syncOnce(&.{
        .{
            .replica_intent = .{
                .upsert = .{
                    .record = .{
                        .group_id = 702,
                        .replica_id = 1,
                        .local_node_id = 1,
                    },
                    .peer_node_ids = &.{},
                },
            },
        },
    }, 1, 1);

    try std.testing.expectEqual(@as(usize, 1), result.reconcile.ensured);
    try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, managed.host.status(702));
}

test "managed host routes metadata and data groups through distinct apply builders" {
    const BuilderRecorder = struct {
        apply_count: usize = 0,
        last_group_id: u64 = 0,
        last_commit_index: u64 = 0,
        last_payload_len: usize = 0,

        fn builder(self: *@This()) state_machine.SnapshotBuilder {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_snapshot = buildSnapshot,
                    .apply_batch = applyBatch,
                },
            };
        }

        fn buildSnapshot(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
            return try std.fmt.allocPrint(alloc, "builder-{d}", .{group_id});
        }

        fn applyBatch(ptr: *anyopaque, batch: state_machine.ApplyBatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.apply_count += 1;
            self.last_group_id = batch.group_id;
            self.last_commit_index = batch.commit_index;
            self.last_payload_len = batch.entries_bytes.len;
        }
    };

    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-raft-builders", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var metadata_builder = BuilderRecorder{};
    var data_builder = BuilderRecorder{};

    var managed = try ManagedHost.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1000,
            .replica_root_dir = replica_root,
        },
    }, .{
        .host = .{ .descriptor_factory = factory.iface() },
        .metadata_snapshot_builder = metadata_builder.builder(),
        .data_snapshot_builder = data_builder.builder(),
    });
    defer managed.deinit();

    _ = try managed.host.ensureReplica(.{
        .group_id = 1000,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .persisted,
    });
    _ = try managed.host.ensureReplica(.{
        .group_id = 1001,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .persisted,
    });

    try managed.host.campaignGroup(1000);
    try managed.host.campaignGroup(1001);
    _ = try managed.host.runRound(2, 2);

    try managed.host.propose(1000, "metadata-entry");
    try managed.host.propose(1001, "data-entry");
    _ = try managed.host.runRound(2, 2);

    try std.testing.expect(metadata_builder.apply_count > 0);
    try std.testing.expectEqual(@as(u64, 1000), metadata_builder.last_group_id);
    try std.testing.expect(metadata_builder.last_commit_index > 0);
    try std.testing.expect(metadata_builder.last_payload_len > 0);

    try std.testing.expect(data_builder.apply_count > 0);
    try std.testing.expectEqual(@as(u64, 1001), data_builder.last_group_id);
    try std.testing.expect(data_builder.last_commit_index > 0);
    try std.testing.expect(data_builder.last_payload_len > 0);

    const provider = managed.owned_file_replica_provider orelse return error.MissingPersistentReplicaProvider;
    const metadata_state = provider.stateForGroup(1000) orelse return error.MissingMetadataState;
    const data_state = provider.stateForGroup(1001) orelse return error.MissingDataState;
    try std.testing.expect(metadata_state.appliedIndex() > 0);
    try std.testing.expect(data_state.appliedIndex() > 0);
}

test "managed host defaults metadata and data apply stores when durable state is enabled" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-raft-default-builders", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var managed = try ManagedHost.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1100,
            .replica_root_dir = replica_root,
        },
    }, .{
        .host = .{ .descriptor_factory = factory.iface() },
    });
    defer managed.deinit();

    try std.testing.expect(managed.owned_metadata_store != null);
    try std.testing.expect(managed.owned_data_store != null);

    _ = try managed.host.ensureReplica(.{
        .group_id = 1100,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .persisted,
    });
    _ = try managed.host.ensureReplica(.{
        .group_id = 1101,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .persisted,
    });

    try managed.host.campaignGroup(1100);
    try managed.host.campaignGroup(1101);
    _ = try managed.host.runRound(2, 2);
    try managed.host.propose(1100, "meta-default");
    try managed.host.propose(1101, "data-default");
    _ = try managed.host.runRound(2, 2);

    const metadata_store = managed.owned_metadata_store orelse return error.MissingMetadataStore;
    const data_store = managed.owned_data_store orelse return error.MissingDataStore;
    const metadata_batch = (try metadata_store.latestBatch(1100)) orelse return error.MissingMetadataBatch;
    const data_batch = (try data_store.latestBatch(1101)) orelse return error.MissingDataBatch;
    try std.testing.expect(metadata_batch.commit_index > 0);
    try std.testing.expect(metadata_batch.entries_bytes.len > 0);
    try std.testing.expect(data_batch.commit_index > 0);
    try std.testing.expect(data_batch.entries_bytes.len > 0);
}

test "managed host default metadata and data apply stores survive restart" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-raft-default-builders-restart", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-raft-default-builders-restart/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    {
        var managed = try ManagedHost.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .metadata_group_id = 1200,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        });
        defer managed.deinit();

        _ = try managed.host.ensureReplica(.{
            .group_id = 1200,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });
        _ = try managed.host.ensureReplica(.{
            .group_id = 1201,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });

        try managed.host.campaignGroup(1200);
        try managed.host.campaignGroup(1201);
        _ = try managed.host.runRound(2, 2);
        try managed.host.propose(1200, "meta-restart");
        try managed.host.propose(1201, "data-restart");
        _ = try managed.host.runRound(2, 2);
    }

    {
        var managed = try ManagedHost.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .metadata_group_id = 1200,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        });
        defer managed.deinit();

        try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, managed.host.status(1200));
        try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, managed.host.status(1201));

        const metadata_store = managed.owned_metadata_store orelse return error.MissingMetadataStore;
        const data_store = managed.owned_data_store orelse return error.MissingDataStore;
        const metadata_batch = (try metadata_store.latestBatch(1200)) orelse return error.MissingMetadataBatch;
        const data_batch = (try data_store.latestBatch(1201)) orelse return error.MissingDataBatch;
        try std.testing.expect(metadata_batch.commit_index > 0);
        try std.testing.expect(metadata_batch.entries_bytes.len > 0);
        try std.testing.expect(data_batch.commit_index > 0);
        try std.testing.expect(data_batch.entries_bytes.len > 0);
    }
}

test "managed host installs backup restore bootstrapper when replica root is configured" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/managed-backup-bootstrapper", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var managed = try ManagedHost.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 99,
            .replica_root_dir = replica_root,
        },
    }, .{
        .host = .{
            .descriptor_factory = factory.iface(),
        },
    });
    defer managed.deinit();

    try std.testing.expect(managed.owned_backup_restore_bootstrapper != null);
    try std.testing.expect(managed.host.deps.backup_restore_bootstrapper != null);
}

test "managed http host exposes backup bootstrap status from underlying host" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const Bootstrapper = struct {
        fn iface(self: *@This()) host_mod.BackupRestoreBootstrapper {
            return .{
                .ptr = self,
                .vtable = &.{
                    .prepare_backup_restore = prepareBackupRestore,
                },
            };
        }

        fn prepareBackupRestore(_: *anyopaque, _: catalog.ReplicaRecord) !void {}
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var bootstrapper = Bootstrapper{};

    var managed = try ManagedHttpHost.init(std.testing.allocator, .{
        .http = .{
            .host = .{
                .local_node_id = 1,
            },
            .transport = .{
                .snapshot = .{ .root_dir = ".zig-cache/tmp/managed-http-bootstrap-status-snapshots" },
            },
        },
    }, .{
        .http = .{
            .host = .{
                .descriptor_factory = factory.iface(),
                .backup_restore_bootstrapper = bootstrapper.iface(),
            },
        },
    });
    defer managed.deinit();

    _ = try managed.http_host.ensureReplica(.{
        .group_id = 1205,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .fetch_snapshot,
        .backup_restore_bootstrap = .{
            .backup_id = "snap-1205",
            .location = "file:///tmp/backups",
            .snapshot_path = "snap-1205/groups/1205",
        },
    });

    try std.testing.expectEqual(.active, managed.status(1205));
    const bootstrap_status = managed.bootstrapStatus(1205) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(.backup_db_snapshot_restore, bootstrap_status.kind);
    try std.testing.expectEqual(.succeeded, bootstrap_status.phase);
    try std.testing.expectEqualStrings("snap-1205", bootstrap_status.backup_id orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings("snap-1205/groups/1205", bootstrap_status.snapshot_path orelse return error.TestExpectedEqual);
    const listed = try managed.listBootstrapStatuses(std.testing.allocator);
    defer managed.http_host.host.freeBootstrapStatuses(std.testing.allocator, listed);
    try std.testing.expectEqual(@as(usize, 1), listed.len);
    try std.testing.expectEqual(@as(u64, 1205), listed[0].group_id);
}

test "managed host module compiles" {
    _ = ManagedSyncResult;
    _ = ManagedHostConfig;
    _ = ManagedHostDeps;
    _ = ManagedHttpHostConfig;
    _ = ManagedHttpHostDeps;
    _ = ManagedHost;
    _ = ManagedHttpHost;
}
