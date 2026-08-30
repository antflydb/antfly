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
const build_options = @import("build_options");
const raft_engine = @import("raft_engine");
const platform_time = @import("antfly_platform").time;
const tracing = @import("../tracing/mod.zig");
pub const catalog = @import("catalog.zig");
const backup_restore = @import("storage/backup_restore.zig");
const backend_runtime_mod = @import("../storage/background_runtime.zig");
const peer_resolver = @import("peer_resolver.zig");
const transport = @import("transport/mod.zig");
const snapshot_transfer = @import("transport/snapshot_transfer.zig");

pub const default_max_inbound_messages_per_round: usize = 1024;
pub const default_http_listener_max_connection_threads: u32 = 32;

pub fn httpListenerConfig(bind_host: []const u8, bind_port: u16) transport.StdHttpListenerConfig {
    return .{
        .bind_host = bind_host,
        .bind_port = bind_port,
        .serve_in_connection_threads = true,
        .max_connection_threads = default_http_listener_max_connection_threads,
    };
}

test "raft http listener config uses bounded per-connection serving" {
    const cfg = httpListenerConfig("127.0.0.1", 8081);
    try std.testing.expectEqualStrings("127.0.0.1", cfg.bind_host);
    try std.testing.expectEqual(@as(u16, 8081), cfg.bind_port);
    try std.testing.expect(cfg.serve_in_connection_threads);
    try std.testing.expectEqual(default_http_listener_max_connection_threads, cfg.max_connection_threads);
}

pub const ReplicaStateBackend = enum {
    file_image,
    wal,
};

pub const HostConfig = struct {
    local_node_id: u64,
    metadata_group_id: ?u64 = null,
    runtime: raft_engine.runtime.RuntimeConfig = .{},
    replica_root_dir: ?[]const u8 = null,
    replica_catalog_path: ?[]const u8 = null,
    replica_state_backend: ReplicaStateBackend = .file_image,
    trace_logger: ?raft_engine.core.TraceLogger = null,
    /// Aggregate ownership retained by HTTP snapshot materialization and the
    /// inbound Raft queue. This is a byte-weighted admission limit, not a
    /// per-request ceiling.
    max_pending_inbound_snapshot_bytes: usize = 1 << 30,
};

pub const RuntimeHooks = raft_engine.runtime.multi_raft.RuntimeHooks;
pub const GroupQuarantineStatus = raft_engine.runtime.multi_raft.GroupQuarantineStatus;
pub const ResumeQuarantineOptions = raft_engine.runtime.multi_raft.ResumeQuarantineOptions;
pub const QuarantineReason = raft_engine.runtime.QuarantineReason;

pub const ReplicaDescriptorFactory = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        build_descriptor: *const fn (ptr: *anyopaque, record: catalog.ReplicaRecord) anyerror!raft_engine.runtime.ReplicaDescriptor,
        free_descriptor: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void = null,
        accepts_record: ?*const fn (ptr: *anyopaque, record: catalog.ReplicaRecord) bool = null,
    };

    pub fn acceptsRecord(self: ReplicaDescriptorFactory, record: catalog.ReplicaRecord) bool {
        const accepts_record = self.vtable.accepts_record orelse return true;
        return accepts_record(self.ptr, record);
    }

    pub fn buildDescriptor(self: ReplicaDescriptorFactory, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        return try self.vtable.build_descriptor(self.ptr, record);
    }

    pub fn freeDescriptor(
        self: ReplicaDescriptorFactory,
        alloc: std.mem.Allocator,
        desc: *raft_engine.runtime.ReplicaDescriptor,
    ) void {
        if (self.vtable.free_descriptor) |free_descriptor| {
            free_descriptor(self.ptr, alloc, desc);
        }
    }
};

pub const BackupRestoreBootstrapper = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        prepare_backup_restore: *const fn (ptr: *anyopaque, record: catalog.ReplicaRecord) anyerror!void,
    };

    pub fn prepareBackupRestore(self: BackupRestoreBootstrapper, record: catalog.ReplicaRecord) !void {
        try self.vtable.prepare_backup_restore(self.ptr, record);
    }
};

pub fn stableRandomSeed(group_id: u64, local_node_id: u64) u64 {
    var x = group_id +% 0x9e3779b97f4a7c15;
    x ^= local_node_id +% 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 30)) *% 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 27)) *% 0x94d049bb133111eb;
    x = x ^ (x >> 31);
    return if (x == 0) 0x9e3779b97f4a7c15 else x;
}

pub const HostDeps = struct {
    replica_catalog: ?catalog.ReplicaCatalog = null,
    peer_resolver: ?peer_resolver.PeerResolver = null,
    runtime_hooks: RuntimeHooks = .{},
    descriptor_factory: ?ReplicaDescriptorFactory = null,
    backup_restore_bootstrapper: ?BackupRestoreBootstrapper = null,
};

pub const HostedReplicaStatus = enum {
    absent,
    starting,
    active,
    quiesced,
    quarantined,
    snapshotting,
    failed,
};

pub const PreparedReplica = struct {
    factory: ReplicaDescriptorFactory,
    descriptor: raft_engine.runtime.ReplicaDescriptor,
    bootstrap_prepared: bool,

    pub fn deinit(self: *PreparedReplica, alloc: std.mem.Allocator) void {
        self.factory.freeDescriptor(alloc, &self.descriptor);
        self.* = undefined;
    }
};

pub const BootstrapStatusKind = enum {
    backup_db_snapshot_restore,
};

pub const BootstrapStatusPhase = enum {
    preparing,
    durability_pending,
    succeeded,
    failed,
};

pub const BootstrapStatus = struct {
    group_id: u64,
    kind: BootstrapStatusKind,
    phase: BootstrapStatusPhase,
    attempts: u64 = 0,
    last_updated_at_millis: u64 = 0,
    last_error: ?[]const u8 = null,
    backup_id: ?[]const u8 = null,
    snapshot_path: ?[]const u8 = null,
};

pub const HostMetrics = struct {
    hosted_groups: usize = 0,
    quarantined_groups: usize = 0,
    reconcile_rounds: usize = 0,
    ensure_replica_calls: usize = 0,
    remove_replica_calls: usize = 0,
    endpoint_refreshes: usize = 0,
    endpoint_removals: usize = 0,
    inbound_message_enqueues: usize = 0,
    inbound_message_drains: usize = 0,
    quarantined_inbound_message_drops: usize = 0,
    pending_inbound_messages: usize = 0,
    pending_inbound_snapshot_bytes: usize = 0,
    inbound_snapshot_admission_denials: usize = 0,
    runtime_rounds: usize = 0,
    runtime_ticked_groups: usize = 0,
    runtime_processed_groups: usize = 0,
    runtime_transport_message_sends: usize = 0,
    runtime_pending_outbound_messages: usize = 0,
    runtime_pending_outbound_bytes: usize = 0,
    runtime_pending_apply_tasks: usize = 0,
    runtime_pending_apply_bytes: usize = 0,
    runtime_transport_queue_denials: usize = 0,
    runtime_apply_queue_denials: usize = 0,
    runtime_oversized_outbound_ready_rejections: usize = 0,
    runtime_oversized_apply_ready_rejections: usize = 0,
    runtime_quarantine_resume_attempts: usize = 0,
    runtime_quarantine_resume_successes: usize = 0,
    runtime_quarantine_resume_conflicts: usize = 0,
    runtime_snapshot_compaction_completions: usize = 0,
    runtime_snapshot_compaction_failures: usize = 0,
    runtime_snapshot_compaction_candidates: usize = 0,
    backup_bootstrap_attempts: usize = 0,
    backup_bootstrap_failures: usize = 0,
    backup_bootstrap_successes: usize = 0,
    backup_bootstrap_durability_pending: usize = 0,
    async_send_enqueued: u64 = 0,
    async_send_failed: u64 = 0,
    async_send_retried: u64 = 0,
    async_send_dropped: u64 = 0,
    async_send_queue_full: u64 = 0,
    async_send_peer_queue_full: u64 = 0,
    async_send_pending: usize = 0,
    async_snapshot_send_enqueued: u64 = 0,
    async_snapshot_send_failed: u64 = 0,
    async_snapshot_send_retried: u64 = 0,
    async_snapshot_send_dropped: u64 = 0,
    async_snapshot_send_deduplicated: u64 = 0,
    async_snapshot_send_queue_full: u64 = 0,
    async_snapshot_send_peer_queue_full: u64 = 0,
    async_snapshot_send_pending: usize = 0,
    async_snapshot_send_pending_bytes: usize = 0,
};

pub const HttpHostConfig = struct {
    host: HostConfig,
    executor: transport.StdHttpExecutorConfig = .{},
    transport: transport.HttpTransportStackConfig,
    listener: transport.StdHttpListenerConfig = .{},
    max_snapshot_bytes: usize = 1 << 30,
    snapshot_artifact_policy: transport.SnapshotArtifactPolicy = .{},
};

pub const HttpHostDeps = struct {
    host: HostDeps = .{},
    snapshot_store: ?transport.http_server.SnapshotStore = null,
    snapshot_resolver: ?transport.http_snapshot.SnapshotTargetResolver = null,
    request_executor: ?transport.RequestExecutor = null,
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
};

const PeerSnapshotTargetResolver = struct {
    peer_resolver: peer_resolver.PeerResolver,

    fn resolver(self: *PeerSnapshotTargetResolver) transport.http_snapshot.SnapshotTargetResolver {
        return .{
            .ptr = self,
            .vtable = &.{
                .resolve_upload_uri = resolveUploadUri,
                .resolve_base_uri = resolveBaseUri,
            },
        };
    }

    fn resolveUploadUri(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        node_id: u64,
        snapshot_id: []const u8,
    ) ![]u8 {
        const self: *PeerSnapshotTargetResolver = @ptrCast(@alignCast(ptr));
        const endpoints = try self.peer_resolver.resolveGroupPeer(alloc, group_id, node_id);
        defer {
            for (endpoints) |endpoint| {
                alloc.free(endpoint.address);
                alloc.free(endpoint.metadata);
            }
            alloc.free(endpoints);
        }
        for (endpoints) |endpoint| {
            switch (endpoint.protocol) {
                .http, .https, .http2, .http3 => {
                    const upload_path = try transport.Routes.snapshotUploadPath(alloc, snapshot_id);
                    defer alloc.free(upload_path);
                    return try transport.Routes.join(alloc, endpoint.address, upload_path);
                },
                .quic => {},
            }
        }
        return error.NoHttpSnapshotEndpoint;
    }

    fn resolveBaseUri(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        node_id: u64,
    ) ![]u8 {
        const self: *PeerSnapshotTargetResolver = @ptrCast(@alignCast(ptr));
        const endpoints = try self.peer_resolver.resolveGroupPeer(alloc, group_id, node_id);
        defer {
            for (endpoints) |endpoint| {
                alloc.free(endpoint.address);
                alloc.free(endpoint.metadata);
            }
            alloc.free(endpoints);
        }
        for (endpoints) |endpoint| switch (endpoint.protocol) {
            .http, .https, .http2, .http3 => return try alloc.dupe(u8, endpoint.address),
            .quic => {},
        };
        return error.NoHttpSnapshotEndpoint;
    }
};

pub const Host = struct {
    const PendingInboundMessage = struct {
        group_id: u64,
        message: raft_engine.core.Message,

        fn deinit(self: *PendingInboundMessage, alloc: std.mem.Allocator) void {
            self.message.deinit(alloc);
            self.* = undefined;
        }
    };

    const OwnedBootstrapStatus = struct {
        kind: BootstrapStatusKind,
        phase: BootstrapStatusPhase,
        attempts: u64 = 0,
        last_updated_at_millis: u64 = 0,
        last_error: ?[]u8 = null,
        backup_id: ?[]u8 = null,
        snapshot_path: ?[]u8 = null,

        fn deinit(self: *OwnedBootstrapStatus, alloc: std.mem.Allocator) void {
            if (self.last_error) |msg| alloc.free(msg);
            if (self.backup_id) |value| alloc.free(value);
            if (self.snapshot_path) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    alloc: std.mem.Allocator,
    cfg: HostConfig,
    deps: HostDeps,
    metrics: HostMetrics = .{},
    runtime_host: raft_engine.runtime.MultiRaft,
    bootstrap_statuses: std.AutoHashMapUnmanaged(u64, OwnedBootstrapStatus) = .empty,
    inbound_mutex: std.atomic.Mutex = .unlocked,
    pending_inbound: std.ArrayListUnmanaged(PendingInboundMessage) = .empty,

    pub fn init(alloc: std.mem.Allocator, cfg: HostConfig, deps: HostDeps) Host {
        var runtime_cfg = cfg.runtime;
        runtime_cfg.max_pending_snapshot_bytes = @min(
            runtime_cfg.max_pending_snapshot_bytes,
            cfg.max_pending_inbound_snapshot_bytes,
        );
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .deps = deps,
            .runtime_host = raft_engine.runtime.MultiRaft.init(alloc, runtime_cfg, deps.runtime_hooks),
        };
    }

    pub fn deinit(self: *Host) void {
        self.lockInbound();
        var pending = self.pending_inbound;
        self.pending_inbound = .empty;
        self.metrics.pending_inbound_messages = 0;
        self.inbound_mutex.unlock();
        for (pending.items) |*item| item.deinit(self.alloc);
        pending.deinit(self.alloc);
        var bootstrap_it = self.bootstrap_statuses.valueIterator();
        while (bootstrap_it.next()) |bootstrap_status| bootstrap_status.deinit(self.alloc);
        self.bootstrap_statuses.deinit(self.alloc);
        self.runtime_host.deinit();
        self.* = undefined;
    }

    pub fn ensureReplica(self: *Host, record: catalog.ReplicaRecord) !raft_engine.runtime.EnsureReplicaResult {
        if (!self.acceptsReplicaRecord(record)) return error.ReplicaAdmissionRejected;
        const prepare_bootstrap = !self.hasReplica(record.group_id) and record.backup_restore_bootstrap != null;
        if (prepare_bootstrap) self.noteReplicaBootstrapPreparing(record);
        var prepared = self.prepareReplica(record, prepare_bootstrap) catch |err| {
            if (prepare_bootstrap) self.noteReplicaBootstrapPreparationFailure(record, err);
            return err;
        };
        defer prepared.deinit(self.alloc);
        return try self.installPreparedReplica(record, &prepared);
    }

    pub fn hasReplica(self: *Host, group_id: u64) bool {
        return self.runtime_host.group(group_id) != null;
    }

    /// Performs filesystem, descriptor, and catalog durability work without
    /// mutating the live Raft runtime. Callers must serialize reconcile plans,
    /// but Raft progress may continue concurrently while this method runs.
    pub fn prepareReplica(
        self: *Host,
        record: catalog.ReplicaRecord,
        prepare_bootstrap: bool,
    ) !PreparedReplica {
        return try self.prepareReplicaWithCatalog(record, prepare_bootstrap, true);
    }

    /// Builds a descriptor and any replacement filesystem generation without
    /// publishing catalog admission. Reconciliation commits all catalog
    /// mutations together after its desired-state epoch is revalidated.
    pub fn prepareReplicaUnpublished(
        self: *Host,
        record: catalog.ReplicaRecord,
        prepare_bootstrap: bool,
    ) !PreparedReplica {
        return try self.prepareReplicaWithCatalog(record, prepare_bootstrap, false);
    }

    fn prepareReplicaWithCatalog(
        self: *Host,
        record: catalog.ReplicaRecord,
        prepare_bootstrap: bool,
        persist_catalog: bool,
    ) !PreparedReplica {
        if (!self.acceptsReplicaRecord(record)) return error.ReplicaAdmissionRejected;
        const should_prepare_bootstrap = prepare_bootstrap and record.backup_restore_bootstrap != null;
        if (should_prepare_bootstrap) {
            try record.backup_restore_bootstrap.?.validate();
            if (self.deps.backup_restore_bootstrapper) |bootstrapper| {
                try bootstrapper.prepareBackupRestore(record);
            } else {
                return error.MissingBackupRestoreBootstrapHandler;
            }
        }

        const factory = self.deps.descriptor_factory orelse return error.MissingReplicaDescriptorFactory;
        var descriptor = try factory.buildDescriptor(record);
        errdefer factory.freeDescriptor(self.alloc, &descriptor);
        descriptor.group.raft_config.trace_logger = self.cfg.trace_logger orelse
            if (comptime build_options.with_tla) tracing.stderrRaftTraceLogger() else null;
        // This check must precede the durable catalog mutation: overwriting
        // desired state and then rejecting a conflicting live descriptor would
        // leave restart behavior different from the running process.
        try self.runtime_host.validateReplicaAdmission(descriptor);

        // Persist admission before publication. A crash between these steps
        // leaves a recoverable catalog entry instead of an untracked live group.
        if (persist_catalog) if (self.deps.replica_catalog) |replica_catalog| {
            try replica_catalog.upsertReplica(record);
        };
        return .{
            .factory = factory,
            .descriptor = descriptor,
            .bootstrap_prepared = should_prepare_bootstrap,
        };
    }

    /// Publishes a fully prepared descriptor into the single-owner runtime.
    /// This method must execute under the runtime owner's serialization lock.
    pub fn installPreparedReplica(
        self: *Host,
        record: catalog.ReplicaRecord,
        prepared: *PreparedReplica,
    ) !raft_engine.runtime.EnsureReplicaResult {
        const result = self.runtime_host.installDurableReplica(prepared.descriptor) catch |err| {
            if (prepared.bootstrap_prepared) {
                self.noteBootstrapFailure(record.group_id, .backup_db_snapshot_restore, err, record.backup_restore_bootstrap);
            }
            return err;
        };
        self.metrics.ensure_replica_calls += 1;
        if (prepared.bootstrap_prepared) {
            self.noteBootstrapSuccess(record.group_id, .backup_db_snapshot_restore, record.backup_restore_bootstrap);
        }
        return result;
    }

    pub fn noteReplicaBootstrapPreparing(self: *Host, record: catalog.ReplicaRecord) void {
        if (record.backup_restore_bootstrap == null) return;
        self.noteBootstrapPreparing(record.group_id, .backup_db_snapshot_restore, record.backup_restore_bootstrap);
    }

    pub fn noteReplicaBootstrapPreparationFailure(
        self: *Host,
        record: catalog.ReplicaRecord,
        err: anyerror,
    ) void {
        if (record.backup_restore_bootstrap == null) return;
        self.noteBootstrapFailure(record.group_id, .backup_db_snapshot_restore, err, record.backup_restore_bootstrap);
    }

    pub fn cancelReplicaBootstrapPreparation(self: *Host, record: catalog.ReplicaRecord) void {
        if (record.backup_restore_bootstrap == null or self.hasReplica(record.group_id)) return;
        self.clearBootstrapStatus(record.group_id);
    }

    pub fn replicaCatalogRevision(self: *Host) ?u64 {
        const replica_catalog = self.deps.replica_catalog orelse return null;
        return replica_catalog.revision();
    }

    /// Returns the durable admission decision, independently of whether the
    /// live runtime has finished installing or removing the replica.
    pub fn replicaCatalogContains(self: *Host, group_id: u64) ?bool {
        const replica_catalog = self.deps.replica_catalog orelse return null;
        return replica_catalog.containsReplica(group_id);
    }

    pub fn commitReplicaCatalog(
        self: *Host,
        expected_revision: ?u64,
        upserts: []const catalog.ReplicaRecord,
        removals: []const u64,
    ) !void {
        const replica_catalog = self.deps.replica_catalog orelse return;
        try replica_catalog.applyBatch(
            expected_revision orelse return error.MissingReplicaCatalogRevision,
            upserts,
            removals,
        );
    }

    pub fn removePreparedReplica(self: *Host, group_id: u64) !void {
        if (self.runtime_host.group(group_id) == null) return error.UnknownGroup;
        try self.runtime_host.removeReplica(group_id);
        self.metrics.remove_replica_calls += 1;
        self.clearBootstrapStatus(group_id);
    }

    pub fn restoreReplicasFromCatalog(self: *Host, alloc: std.mem.Allocator) !usize {
        const replica_catalog = self.deps.replica_catalog orelse return error.MissingReplicaCatalog;
        _ = self.deps.descriptor_factory orelse return error.MissingReplicaDescriptorFactory;

        const records = try replica_catalog.listReplicas(alloc);
        defer catalog.freeReplicaRecords(alloc, records);

        var restored: usize = 0;
        for (records) |record| {
            if (self.runtime_host.group(record.group_id) != null) continue;
            if (!self.acceptsReplicaRecord(record)) {
                std.log.warn(
                    "raft host skipped catalog replica rejected by descriptor factory group_id={} replica_id={} local_node_id={}",
                    .{ record.group_id, record.replica_id, record.local_node_id },
                );
                continue;
            }
            const prepare_bootstrap = record.backup_restore_bootstrap != null;
            if (prepare_bootstrap) self.noteReplicaBootstrapPreparing(record);
            var prepared = self.prepareReplicaWithCatalog(record, prepare_bootstrap, false) catch |err| {
                if (prepare_bootstrap) self.noteReplicaBootstrapPreparationFailure(record, err);
                return err;
            };
            defer prepared.deinit(self.alloc);
            const result = try self.installPreparedReplica(record, &prepared);
            if (result.created or result.resumed or result.fetched_snapshot) restored += 1;
        }
        return restored;
    }

    fn acceptsReplicaRecord(self: *const Host, record: catalog.ReplicaRecord) bool {
        const factory = self.deps.descriptor_factory orelse return true;
        return factory.acceptsRecord(record);
    }

    pub fn removeReplica(self: *Host, group_id: u64) !void {
        if (self.runtime_host.group(group_id) == null) return error.UnknownGroup;
        // Persist removal intent before tearing down the live owner. A catalog
        // failure therefore remains retryable through normal reconciliation.
        if (self.deps.replica_catalog) |replica_catalog| {
            _ = try replica_catalog.removeReplica(group_id);
        }
        try self.runtime_host.removeReplica(group_id);
        self.metrics.remove_replica_calls += 1;
        self.clearBootstrapStatus(group_id);
    }

    pub fn refreshPeerEndpoints(self: *Host, group_id: u64, node_id: u64) !usize {
        const resolver = self.deps.peer_resolver orelse return 0;
        const endpoints = try resolver.resolveGroupPeer(self.alloc, group_id, node_id);
        defer {
            for (endpoints) |endpoint| {
                self.alloc.free(endpoint.address);
                self.alloc.free(endpoint.metadata);
            }
            self.alloc.free(endpoints);
        }

        return try self.upsertResolvedPeerEndpoints(group_id, node_id, endpoints);
    }

    pub fn upsertResolvedPeerEndpoints(self: *Host, group_id: u64, node_id: u64, endpoints: []const peer_resolver.PeerEndpoint) !usize {
        if (self.runtime_host.group(group_id) == null) return 0;

        const runtime_endpoints = try self.alloc.alloc(raft_engine.runtime.transport_iface.PeerEndpoint, endpoints.len);
        defer self.alloc.free(runtime_endpoints);
        for (endpoints, 0..) |endpoint, i| {
            runtime_endpoints[i] = .{
                .protocol = switch (endpoint.protocol) {
                    .http, .https => .http1,
                    .http2 => .http2,
                    .http3 => .http3,
                    .quic => .quic,
                },
                .address = endpoint.address,
                .metadata = endpoint.metadata,
            };
        }
        try self.runtime_host.upsertPeer(group_id, .{
            .node_id = node_id,
            .endpoints = runtime_endpoints,
        });
        self.metrics.endpoint_refreshes += 1;
        return endpoints.len;
    }

    pub fn removePeerRoute(self: *Host, group_id: u64, node_id: u64) !bool {
        if (self.runtime_host.group(group_id) == null) return false;
        try self.runtime_host.removePeer(group_id, node_id);
        self.metrics.endpoint_removals += 1;
        return true;
    }

    pub fn status(self: *Host, group_id: u64) HostedReplicaStatus {
        if (self.bootstrap_statuses.get(group_id)) |bootstrap_status| {
            return switch (bootstrap_status.phase) {
                .preparing, .durability_pending => .starting,
                .failed => .failed,
                .succeeded => self.runtimeReplicaStatus(group_id),
            };
        }
        return self.runtimeReplicaStatus(group_id);
    }

    fn runtimeReplicaStatus(self: *Host, group_id: u64) HostedReplicaStatus {
        if (self.runtime_host.group(group_id) == null) return .absent;
        if (self.runtime_host.groupQuarantine(group_id) != null) return .quarantined;
        if (self.runtime_host.isGroupQuiesced(group_id)) return .quiesced;
        return .active;
    }

    pub fn quarantineStatus(self: *const Host, group_id: u64) ?raft_engine.runtime.GroupQuarantine {
        return self.runtime_host.groupQuarantine(group_id);
    }

    pub fn listQuarantines(
        self: *const Host,
        alloc: std.mem.Allocator,
    ) ![]raft_engine.runtime.multi_raft.GroupQuarantineStatus {
        return try self.runtime_host.listQuarantines(alloc);
    }

    pub fn resumeQuarantinedGroup(
        self: *Host,
        group_id: u64,
        options: raft_engine.runtime.multi_raft.ResumeQuarantineOptions,
    ) !void {
        try self.runtime_host.resumeQuarantinedGroup(group_id, options);
    }

    pub fn bootstrapStatus(self: *const Host, group_id: u64) ?BootstrapStatus {
        const bootstrap_status = self.bootstrap_statuses.get(group_id) orelse return null;
        return .{
            .group_id = group_id,
            .kind = bootstrap_status.kind,
            .phase = bootstrap_status.phase,
            .attempts = bootstrap_status.attempts,
            .last_updated_at_millis = bootstrap_status.last_updated_at_millis,
            .last_error = bootstrap_status.last_error,
            .backup_id = bootstrap_status.backup_id,
            .snapshot_path = bootstrap_status.snapshot_path,
        };
    }

    pub fn listBootstrapStatuses(self: *const Host, alloc: std.mem.Allocator) ![]BootstrapStatus {
        const count = self.bootstrap_statuses.count();
        const statuses = try alloc.alloc(BootstrapStatus, count);
        errdefer self.freeBootstrapStatuses(alloc, statuses);

        var index: usize = 0;
        var it = self.bootstrap_statuses.iterator();
        while (it.next()) |entry| : (index += 1) {
            statuses[index] = .{
                .group_id = entry.key_ptr.*,
                .kind = entry.value_ptr.kind,
                .phase = entry.value_ptr.phase,
                .attempts = entry.value_ptr.attempts,
                .last_updated_at_millis = entry.value_ptr.last_updated_at_millis,
                .last_error = if (entry.value_ptr.last_error) |msg| try alloc.dupe(u8, msg) else null,
                .backup_id = if (entry.value_ptr.backup_id) |value| try alloc.dupe(u8, value) else null,
                .snapshot_path = if (entry.value_ptr.snapshot_path) |value| try alloc.dupe(u8, value) else null,
            };
        }
        return statuses;
    }

    pub fn freeBootstrapStatuses(self: *const Host, alloc: std.mem.Allocator, statuses: []BootstrapStatus) void {
        _ = self;
        for (statuses) |bootstrap_status| {
            if (bootstrap_status.last_error) |msg| alloc.free(msg);
            if (bootstrap_status.backup_id) |value| alloc.free(value);
            if (bootstrap_status.snapshot_path) |value| alloc.free(value);
        }
        alloc.free(statuses);
    }

    pub fn metricsSnapshot(self: *const Host) HostMetrics {
        const runtime_metrics = self.runtime_host.metricsSnapshot();
        var snapshot = self.metrics;
        snapshot.hosted_groups = runtime_metrics.group_count;
        snapshot.quarantined_groups = runtime_metrics.quarantined_group_count;
        snapshot.quarantined_inbound_message_drops = runtime_metrics.quarantined_inbound_messages;
        snapshot.runtime_rounds = runtime_metrics.rounds;
        snapshot.runtime_ticked_groups = runtime_metrics.ticked_groups;
        snapshot.runtime_processed_groups = runtime_metrics.processed_groups;
        snapshot.runtime_transport_message_sends = runtime_metrics.transport_message_sends;
        snapshot.runtime_pending_outbound_messages = runtime_metrics.pending_outbound_messages;
        snapshot.runtime_pending_outbound_bytes = runtime_metrics.pending_outbound_bytes;
        snapshot.runtime_pending_apply_tasks = runtime_metrics.pending_apply_tasks;
        snapshot.runtime_pending_apply_bytes = runtime_metrics.pending_apply_bytes;
        snapshot.pending_inbound_snapshot_bytes = runtime_metrics.pending_snapshot_bytes;
        snapshot.inbound_snapshot_admission_denials = runtime_metrics.snapshot_admission_denials;
        snapshot.runtime_transport_queue_denials = runtime_metrics.transport_queue_denials;
        snapshot.runtime_apply_queue_denials = runtime_metrics.apply_queue_denials;
        snapshot.runtime_oversized_outbound_ready_rejections = runtime_metrics.oversized_outbound_ready_rejections;
        snapshot.runtime_oversized_apply_ready_rejections = runtime_metrics.oversized_apply_ready_rejections;
        snapshot.runtime_quarantine_resume_attempts = runtime_metrics.quarantine_resume_attempts;
        snapshot.runtime_quarantine_resume_successes = runtime_metrics.quarantine_resume_successes;
        snapshot.runtime_quarantine_resume_conflicts = runtime_metrics.quarantine_resume_conflicts;
        snapshot.runtime_snapshot_compaction_completions = runtime_metrics.snapshot_compaction_completions;
        snapshot.runtime_snapshot_compaction_failures = runtime_metrics.snapshot_compaction_failures;
        snapshot.runtime_snapshot_compaction_candidates = runtime_metrics.snapshot_compaction_candidates;
        return snapshot;
    }

    pub fn listGroupIds(self: *Host, alloc: std.mem.Allocator) ![]u64 {
        return try self.runtime_host.listGroupIds(alloc);
    }

    pub fn runRound(self: *Host, max_tick_groups: usize, max_ready_steps: usize) !raft_engine.runtime.multi_raft.HostRound {
        return try self.runRoundBounded(default_max_inbound_messages_per_round, max_tick_groups, max_ready_steps);
    }

    pub fn runRoundBounded(
        self: *Host,
        max_inbound_messages: usize,
        max_tick_groups: usize,
        max_ready_steps: usize,
    ) !raft_engine.runtime.multi_raft.HostRound {
        const inbound_start_ns = platform_time.monotonicNs();
        _ = try self.drainInboundMessages(max_inbound_messages);
        const inbound_elapsed_ns = platform_time.monotonicNs() -| inbound_start_ns;
        var round = try self.runtime_host.runRound(max_tick_groups, max_ready_steps);
        round.inbound_drain_elapsed_ns = inbound_elapsed_ns;
        round.elapsed_ns += round.inbound_drain_elapsed_ns;
        return round;
    }

    pub fn runProgressRoundBounded(
        self: *Host,
        max_inbound_messages: usize,
        max_ready_steps: usize,
    ) !raft_engine.runtime.multi_raft.HostRound {
        const inbound_start_ns = platform_time.monotonicNs();
        _ = try self.drainInboundMessages(max_inbound_messages);
        const inbound_elapsed_ns = platform_time.monotonicNs() -| inbound_start_ns;
        var round = try self.runtime_host.runProgressRound(max_ready_steps);
        round.inbound_drain_elapsed_ns = inbound_elapsed_ns;
        round.elapsed_ns += inbound_elapsed_ns;
        return round;
    }

    pub fn step(self: *Host, group_id: u64, msg: raft_engine.core.Message) !void {
        try self.runtime_host.step(group_id, msg);
    }

    pub fn enqueueInboundBatch(self: *Host, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
        var pending = std.ArrayListUnmanaged(PendingInboundMessage).empty;
        defer pending.deinit(self.alloc);
        errdefer {
            for (pending.items) |*item| item.deinit(self.alloc);
        }

        for (batch.groups) |group_batch| {
            if (self.runtime_host.group(group_batch.group_id) == null) {
                continue;
            }
            for (group_batch.messages) |msg| {
                try pending.append(self.alloc, .{
                    .group_id = group_batch.group_id,
                    .message = try msg.clone(self.alloc),
                });
            }
        }

        if (pending.items.len > 0) {
            self.lockInbound();
            defer self.inbound_mutex.unlock();

            try self.pending_inbound.ensureUnusedCapacity(self.alloc, pending.items.len);
            for (pending.items) |item| self.pending_inbound.appendAssumeCapacity(item);
            self.metrics.inbound_message_enqueues += pending.items.len;
            self.metrics.pending_inbound_messages = self.pending_inbound.items.len;
            pending.clearRetainingCapacity();
        }
    }

    fn drainInboundMessages(self: *Host, max_messages: usize) !usize {
        if (max_messages == 0) return 0;
        var pending = std.ArrayListUnmanaged(PendingInboundMessage).empty;
        defer {
            for (pending.items) |*item| item.deinit(self.alloc);
            pending.deinit(self.alloc);
        }

        self.lockInbound();
        const drain_count = @min(max_messages, self.pending_inbound.items.len);
        if (drain_count > 0) {
            pending.ensureTotalCapacity(self.alloc, drain_count) catch |err| {
                self.inbound_mutex.unlock();
                return err;
            };
            pending.appendSliceAssumeCapacity(self.pending_inbound.items[0..drain_count]);
            if (drain_count < self.pending_inbound.items.len) {
                const remaining = self.pending_inbound.items.len - drain_count;
                std.mem.copyForwards(
                    PendingInboundMessage,
                    self.pending_inbound.items[0..remaining],
                    self.pending_inbound.items[drain_count..],
                );
                self.pending_inbound.items.len = remaining;
            } else {
                self.pending_inbound.clearRetainingCapacity();
            }
        }
        self.metrics.pending_inbound_messages = self.pending_inbound.items.len;
        self.inbound_mutex.unlock();

        var drained: usize = 0;
        for (pending.items) |item| {
            const disposition = self.runtime_host.stepWithDisposition(item.group_id, item.message) catch |err| switch (err) {
                error.UnknownGroup => null,
                else => return err,
            };
            if (disposition == .quarantined)
                self.metrics.quarantined_inbound_message_drops +|= 1;
            drained += 1;
        }
        self.metrics.inbound_message_drains += drained;
        return drained;
    }

    fn lockInbound(self: *Host) void {
        while (!self.inbound_mutex.tryLock()) {
            std.Thread.yield() catch {};
        }
    }

    fn mapGroupActivityError(err: anyerror) anyerror {
        return if (err == error.GroupHardQuarantined)
            error.GroupLeaderUnavailable
        else
            err;
    }

    pub fn campaignGroup(self: *Host, group_id: u64) !void {
        self.runtime_host.campaignGroup(group_id) catch |err| return mapGroupActivityError(err);
    }

    pub fn propose(self: *Host, group_id: u64, data: []const u8) !void {
        self.runtime_host.propose(group_id, data) catch |err| return mapGroupActivityError(err);
    }

    pub fn proposeWithReceipt(self: *Host, group_id: u64, data: []const u8, accepted_index: *?u64) !void {
        self.runtime_host.proposeWithReceipt(group_id, data, accepted_index) catch |err| return mapGroupActivityError(err);
    }

    pub fn proposeBatchWithReceipt(
        self: *Host,
        group_id: u64,
        payloads: []const []const u8,
        accepted_first_index: *?u64,
        accepted_last_index: *?u64,
    ) !void {
        self.runtime_host.proposeBatchWithReceipt(
            group_id,
            payloads,
            accepted_first_index,
            accepted_last_index,
        ) catch |err| return mapGroupActivityError(err);
    }

    pub fn prepareProposalReceiptTracking(self: *Host, group_id: u64) !void {
        try self.runtime_host.prepareProposalReceiptTracking(group_id);
    }

    pub fn trackProposalReceipt(self: *Host, group_id: u64, term: u64, index: u64) !void {
        try self.runtime_host.trackProposalReceipt(group_id, term, index);
    }

    pub fn acquireProposalReceipt(self: *Host, group_id: u64, term: u64, index: u64) bool {
        return self.runtime_host.acquireProposalReceipt(group_id, term, index);
    }

    pub fn releaseProposalReceipt(self: *Host, group_id: u64, term: u64, index: u64) void {
        self.runtime_host.releaseProposalReceipt(group_id, term, index);
    }

    pub fn transferLeader(self: *Host, group_id: u64, transferee: u64) !void {
        self.runtime_host.transferLeader(group_id, transferee) catch |err| return mapGroupActivityError(err);
    }

    pub fn handleSnapshotUpload(self: *Host, upload: transport.http_server.SnapshotUpload) !void {
        var owned = upload;
        errdefer owned.snapshot.deinit(self.alloc);
        const snapshot_bytes = owned.snapshot.data.len;
        var admission_reserved = owned.admission_reserved;
        if (!admission_reserved) {
            try self.admitSnapshotUpload(.{
                .group_id = upload.group_id,
                .to = upload.to,
                .data_len = snapshot_bytes,
            });
            admission_reserved = true;
        }
        errdefer if (admission_reserved) self.cancelSnapshotUpload(.{
            .group_id = upload.group_id,
            .to = upload.to,
            .data_len = snapshot_bytes,
        });
        const grp = self.runtime_host.group(upload.group_id) orelse return error.UnknownGroup;
        if (upload.to != grp.localNodeId()) return error.SnapshotUploadTargetMismatch;
        try self.runtime_host.attachSnapshotAdmission(&owned.snapshot);
        // The shared payload now owns the reservation and returns it only after
        // Raft persistence and state-machine apply release their final clone.
        admission_reserved = false;
        var msg: raft_engine.core.Message = .{
            .msg_type = .snapshot,
            .from = upload.from,
            .to = upload.to,
            .term = upload.term,
            .snapshot = owned.snapshot,
        };
        owned.snapshot = .{};
        errdefer msg.deinit(self.alloc);

        self.lockInbound();
        defer self.inbound_mutex.unlock();
        try self.pending_inbound.append(self.alloc, .{
            .group_id = upload.group_id,
            .message = msg,
        });
        self.metrics.inbound_message_enqueues += 1;
        self.metrics.pending_inbound_messages = self.pending_inbound.items.len;
    }

    pub fn admitSnapshotUpload(self: *Host, admission: transport.http_server.SnapshotUploadAdmission) !void {
        const data_len = std.math.cast(usize, admission.data_len) orelse return error.SnapshotTooLarge;
        try self.runtime_host.admitInboundSnapshot(admission.group_id, admission.to, data_len);
    }

    pub fn cancelSnapshotUpload(self: *Host, admission: transport.http_server.SnapshotUploadAdmission) void {
        const data_len = std.math.cast(usize, admission.data_len) orelse return;
        self.runtime_host.cancelSnapshotAdmission(data_len);
    }

    pub fn forgetLeader(self: *Host, group_id: u64) !void {
        self.runtime_host.forgetLeader(group_id) catch |err| return mapGroupActivityError(err);
    }

    pub fn readIndex(self: *Host, group_id: u64, request_ctx: []const u8) !void {
        self.runtime_host.readIndex(group_id, request_ctx) catch |err| return mapGroupActivityError(err);
    }

    pub fn proposeConfChange(self: *Host, group_id: u64, conf_change: raft_engine.core.ConfChange) !void {
        self.runtime_host.proposeConfChange(group_id, conf_change) catch |err| return mapGroupActivityError(err);
    }

    pub fn proposeConfChangeV2(self: *Host, group_id: u64, conf_change: raft_engine.core.ConfChangeV2) !void {
        self.runtime_host.proposeConfChangeV2(group_id, conf_change) catch |err| return mapGroupActivityError(err);
    }

    pub fn raftStatus(self: *Host, group_id: u64) ?raft_engine.core.Status {
        const grp = self.runtime_host.group(group_id) orelse return null;
        return grp.status();
    }

    /// Returns the Raft term stored at an exact log position. Callers use this
    /// together with the applied watermark to distinguish their accepted entry
    /// from a higher-term replacement at the same index.
    pub fn raftTermAt(self: *Host, group_id: u64, index: u64) !u64 {
        const grp = self.runtime_host.group(group_id) orelse return error.UnknownGroup;
        return try grp.termAt(index);
    }

    pub fn raftTermAtTrackedProposalReceipt(self: *Host, group_id: u64, term: u64, index: u64) !u64 {
        return try self.runtime_host.termAtTrackedProposalReceipt(group_id, term, index);
    }

    pub fn leaderId(self: *Host, group_id: u64) ?u64 {
        const raft_status = self.raftStatus(group_id) orelse return null;
        return raft_status.soft.leader_id;
    }

    pub fn isLocalLeader(self: *Host, group_id: u64) bool {
        const raft_status = self.raftStatus(group_id) orelse return false;
        return raft_status.soft.role == .leader and raft_status.soft.leader_id != null and raft_status.soft.leader_id == raft_status.id;
    }

    fn noteBootstrapPreparing(
        self: *Host,
        group_id: u64,
        kind: BootstrapStatusKind,
        restore: ?catalog.BackupRestoreBootstrapRecord,
    ) void {
        self.metrics.backup_bootstrap_attempts += 1;
        self.updateBootstrapStatus(group_id, kind, .preparing, null, restore, true);
    }

    fn noteBootstrapSuccess(
        self: *Host,
        group_id: u64,
        kind: BootstrapStatusKind,
        restore: ?catalog.BackupRestoreBootstrapRecord,
    ) void {
        self.metrics.backup_bootstrap_successes += 1;
        self.updateBootstrapStatus(group_id, kind, .succeeded, null, restore, false);
    }

    fn noteBootstrapFailure(
        self: *Host,
        group_id: u64,
        kind: BootstrapStatusKind,
        err: anyerror,
        restore: ?catalog.BackupRestoreBootstrapRecord,
    ) void {
        if (err == error.GenerationDurabilityUncertain) {
            self.metrics.backup_bootstrap_durability_pending += 1;
            self.updateBootstrapStatus(group_id, kind, .durability_pending, @errorName(err), restore, false);
            return;
        }
        self.metrics.backup_bootstrap_failures += 1;
        self.updateBootstrapStatus(group_id, kind, .failed, @errorName(err), restore, false);
    }

    fn updateBootstrapStatus(
        self: *Host,
        group_id: u64,
        kind: BootstrapStatusKind,
        phase: BootstrapStatusPhase,
        last_error: ?[]const u8,
        restore: ?catalog.BackupRestoreBootstrapRecord,
        bump_attempt: bool,
    ) void {
        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        if (self.bootstrap_statuses.getPtr(group_id)) |existing| {
            if (existing.last_error) |msg| self.alloc.free(msg);
            if (existing.backup_id) |value| self.alloc.free(value);
            if (existing.snapshot_path) |value| self.alloc.free(value);
            existing.last_error = null;
            existing.backup_id = null;
            existing.snapshot_path = null;
            if (last_error) |msg| {
                existing.last_error = self.alloc.dupe(u8, msg) catch null;
            }
            if (restore) |record| {
                existing.backup_id = self.alloc.dupe(u8, record.backup_id) catch null;
                existing.snapshot_path = self.alloc.dupe(u8, record.snapshot_path) catch null;
            }
            existing.kind = kind;
            existing.phase = phase;
            existing.last_updated_at_millis = now_ms;
            if (bump_attempt) existing.attempts += 1;
            return;
        }

        var owned = OwnedBootstrapStatus{
            .kind = kind,
            .phase = phase,
            .attempts = if (bump_attempt) 1 else 0,
            .last_updated_at_millis = now_ms,
            .last_error = null,
            .backup_id = null,
            .snapshot_path = null,
        };
        if (last_error) |msg| owned.last_error = self.alloc.dupe(u8, msg) catch null;
        if (restore) |record| {
            owned.backup_id = self.alloc.dupe(u8, record.backup_id) catch null;
            owned.snapshot_path = self.alloc.dupe(u8, record.snapshot_path) catch null;
        }
        self.bootstrap_statuses.put(self.alloc, group_id, owned) catch {
            if (owned.last_error) |msg| self.alloc.free(msg);
            if (owned.backup_id) |value| self.alloc.free(value);
            if (owned.snapshot_path) |value| self.alloc.free(value);
        };
    }

    fn clearBootstrapStatus(self: *Host, group_id: u64) void {
        if (self.bootstrap_statuses.fetchRemove(group_id)) |entry| {
            var bootstrap_status = entry.value;
            bootstrap_status.deinit(self.alloc);
        }
    }
};

pub const HttpHost = struct {
    alloc: std.mem.Allocator,
    cfg: HttpHostConfig,
    deps: HttpHostDeps,
    executor: ?*transport.StdHttpExecutor,
    request_executor: transport.RequestExecutor,
    transport_stack: *transport.HttpTransportStack,
    owned_snapshot_resolver: ?*PeerSnapshotTargetResolver,
    owned_snapshot_store: ?*transport.FileSnapshotStore,
    host: *Host,
    batch_handler: *transport.HostBatchHandler,
    server: *transport.HttpServer,
    listener: *transport.StdHttpListener,

    pub fn init(alloc: std.mem.Allocator, cfg: HttpHostConfig, deps: HttpHostDeps) !HttpHost {
        var executor: ?*transport.StdHttpExecutor = null;
        const request_executor = if (deps.request_executor) |override| override else blk: {
            const owned = try alloc.create(transport.StdHttpExecutor);
            errdefer alloc.destroy(owned);
            if (deps.backend_runtime) |backend_runtime| {
                if (backend_runtime.raftOutboundIoImpl()) |io_impl| {
                    owned.initSharedInPlace(alloc, cfg.executor, io_impl);
                } else {
                    owned.initInPlace(alloc, cfg.executor);
                }
            } else {
                owned.initInPlace(alloc, cfg.executor);
            }
            errdefer owned.deinit();
            executor = owned;
            break :blk owned.executor();
        };

        const transport_stack = try alloc.create(transport.HttpTransportStack);
        errdefer alloc.destroy(transport_stack);
        const owned_snapshot_resolver = if (deps.snapshot_resolver == null) blk: {
            const peer = deps.host.peer_resolver orelse break :blk null;
            const resolver = try alloc.create(PeerSnapshotTargetResolver);
            resolver.* = .{ .peer_resolver = peer };
            break :blk resolver;
        } else null;
        errdefer if (owned_snapshot_resolver) |resolver| alloc.destroy(resolver);
        const snapshot_resolver = if (deps.snapshot_resolver) |resolver|
            resolver
        else if (owned_snapshot_resolver) |resolver|
            resolver.resolver()
        else
            null;
        const transport_io = if (deps.backend_runtime) |runtime| runtime.raftOutboundIo() else null;
        var transport_config = cfg.transport;
        // V1 snapshots are one HTTP body in both directions. Bound publication
        // by the stricter listener/executor ceiling so a default-compatible
        // host cannot accept an artifact that another host cannot fetch.
        transport_config.snapshot.legacy_fallback_max_request_bytes = @min(
            transport_config.snapshot.legacy_fallback_max_request_bytes,
            @min(cfg.listener.max_request_bytes, cfg.executor.max_response_bytes),
        );
        if (transport_config.snapshot.legacy_fallback_max_request_bytes == 0)
            return error.InvalidSnapshotTransferLimits;
        // V2 chunks cross the same request/response boundary. Preserve the
        // operator's requested size when valid and otherwise reduce it to the
        // largest end-to-end safe chunk instead of failing only during fetch.
        transport_config.snapshot.chunk_size = @min(
            transport_config.snapshot.chunk_size,
            @min(
                snapshot_transfer.max_chunk_bytes,
                @min(cfg.listener.max_request_bytes, cfg.executor.max_response_bytes),
            ),
        );
        if (transport_config.snapshot.chunk_size < snapshot_transfer.min_chunk_bytes)
            return error.InvalidSnapshotTransferLimits;
        // The std Threaded timeout path uses process signals for cancellation.
        // Keep each asynchronous peer lane on an independent I/O pool so one
        // canceled request cannot interrupt another lane's connect syscall.
        transport_config.driver.isolated_worker_executors = executor != null;
        transport_config.driver.isolated_worker_executor_config = cfg.executor;
        transport_stack.* = try transport.HttpTransportStack.init(
            alloc,
            transport_config,
            request_executor,
            transport_io,
            snapshot_resolver,
        );
        errdefer transport_stack.deinit();

        const owned_snapshot_store = if (deps.snapshot_store == null) blk: {
            const snapshot_store = try alloc.create(transport.FileSnapshotStore);
            errdefer alloc.destroy(snapshot_store);
            snapshot_store.* = try transport.FileSnapshotStore.init(alloc, .{
                .root_dir = cfg.transport.snapshot.root_dir,
                .max_snapshot_bytes = @min(cfg.max_snapshot_bytes, transport_config.snapshot.max_snapshot_bytes),
                // Inbound acceptance is a protocol/listener property, not the
                // local client's outbound chunk preference. Peers negotiate
                // this independently through the capability endpoint.
                .max_chunk_bytes = @min(snapshot_transfer.max_chunk_bytes, cfg.listener.max_request_bytes),
                .artifact_policy = cfg.snapshot_artifact_policy,
            });
            break :blk snapshot_store;
        } else null;
        errdefer if (owned_snapshot_store) |snapshot_store| {
            snapshot_store.deinit();
            alloc.destroy(snapshot_store);
        };

        const host = try alloc.create(Host);
        errdefer alloc.destroy(host);
        var host_deps = deps.host;
        host_deps.runtime_hooks = mergeRuntimeHooks(host_deps.runtime_hooks, transport_stack.runtimeHooks());
        host.* = Host.init(alloc, cfg.host, host_deps);
        errdefer host.deinit();

        const batch_handler = try alloc.create(transport.HostBatchHandler);
        errdefer alloc.destroy(batch_handler);
        batch_handler.* = .{ .host = host };

        const server = try alloc.create(transport.HttpServer);
        errdefer alloc.destroy(server);
        server.* = transport_stack.makeServer(
            batch_handler.handler(),
            if (deps.snapshot_store) |snapshot_store| snapshot_store else if (owned_snapshot_store) |snapshot_store| snapshot_store.store() else null,
            batch_handler.snapshotHandler(),
        );

        const listener = try alloc.create(transport.StdHttpListener);
        errdefer alloc.destroy(listener);
        listener.* = if (deps.backend_runtime) |backend_runtime|
            if (backend_runtime.raftInboundIoImpl()) |io_impl|
                transport.StdHttpListener.initShared(alloc, cfg.listener, server.executor(), io_impl)
            else
                transport.StdHttpListener.init(alloc, cfg.listener, server.executor())
        else
            transport.StdHttpListener.init(alloc, cfg.listener, server.executor());

        return .{
            .alloc = alloc,
            .cfg = cfg,
            .deps = deps,
            .executor = executor,
            .request_executor = request_executor,
            .transport_stack = transport_stack,
            .owned_snapshot_resolver = owned_snapshot_resolver,
            .owned_snapshot_store = owned_snapshot_store,
            .host = host,
            .batch_handler = batch_handler,
            .server = server,
            .listener = listener,
        };
    }

    pub fn deinit(self: *HttpHost) void {
        self.listener.deinit();
        self.alloc.destroy(self.listener);
        self.alloc.destroy(self.server);
        self.alloc.destroy(self.batch_handler);
        self.host.deinit();
        self.alloc.destroy(self.host);
        self.transport_stack.deinit();
        self.alloc.destroy(self.transport_stack);
        if (self.owned_snapshot_resolver) |resolver| self.alloc.destroy(resolver);
        if (self.owned_snapshot_store) |snapshot_store| {
            snapshot_store.deinit();
            self.alloc.destroy(snapshot_store);
        }
        if (self.executor) |executor| {
            executor.deinit();
            self.alloc.destroy(executor);
        }
        self.* = undefined;
    }

    pub fn start(self: *HttpHost) !void {
        try self.listener.start();
    }

    pub fn stop(self: *HttpHost) void {
        self.listener.stop();
    }

    pub fn baseUri(self: *const HttpHost, alloc: std.mem.Allocator) ![]u8 {
        return try self.listener.baseUri(alloc);
    }

    /// Shared bounded outbound I/O used for small control-plane fan-outs.
    /// Callers must still provide request-level deadlines.
    pub fn outboundIo(self: *const HttpHost) std.Io {
        return self.transport_stack.driver.io;
    }

    pub fn metricsSnapshot(self: *const HttpHost) HostMetrics {
        var snapshot = self.host.metricsSnapshot();
        const async_send = self.transport_stack.asyncSendMetricsSnapshot();
        snapshot.async_send_enqueued = async_send.enqueued;
        snapshot.async_send_failed = async_send.failed;
        snapshot.async_send_retried = async_send.retried;
        snapshot.async_send_dropped = async_send.dropped;
        snapshot.async_send_queue_full = async_send.queue_full;
        snapshot.async_send_peer_queue_full = async_send.peer_queue_full;
        snapshot.async_send_pending = async_send.pending;
        const async_snapshot_send = self.transport_stack.asyncSnapshotSendMetricsSnapshot();
        snapshot.async_snapshot_send_enqueued = async_snapshot_send.enqueued;
        snapshot.async_snapshot_send_failed = async_snapshot_send.failed;
        snapshot.async_snapshot_send_retried = async_snapshot_send.retried;
        snapshot.async_snapshot_send_dropped = async_snapshot_send.dropped;
        snapshot.async_snapshot_send_deduplicated = async_snapshot_send.deduplicated;
        snapshot.async_snapshot_send_queue_full = async_snapshot_send.queue_full;
        snapshot.async_snapshot_send_peer_queue_full = async_snapshot_send.peer_queue_full;
        snapshot.async_snapshot_send_pending = async_snapshot_send.pending;
        snapshot.async_snapshot_send_pending_bytes = async_snapshot_send.pending_bytes;
        return snapshot;
    }

    pub fn status(self: *HttpHost, group_id: u64) HostedReplicaStatus {
        return self.host.status(group_id);
    }

    pub fn quarantineStatus(self: *const HttpHost, group_id: u64) ?raft_engine.runtime.GroupQuarantine {
        return self.host.quarantineStatus(group_id);
    }

    pub fn listQuarantines(
        self: *const HttpHost,
        alloc: std.mem.Allocator,
    ) ![]raft_engine.runtime.multi_raft.GroupQuarantineStatus {
        return try self.host.listQuarantines(alloc);
    }

    pub fn resumeQuarantinedGroup(
        self: *HttpHost,
        group_id: u64,
        options: raft_engine.runtime.multi_raft.ResumeQuarantineOptions,
    ) !void {
        try self.host.resumeQuarantinedGroup(group_id, options);
    }

    pub fn bootstrapStatus(self: *const HttpHost, group_id: u64) ?BootstrapStatus {
        return self.host.bootstrapStatus(group_id);
    }

    pub fn listBootstrapStatuses(self: *const HttpHost, alloc: std.mem.Allocator) ![]BootstrapStatus {
        return try self.host.listBootstrapStatuses(alloc);
    }

    pub fn ensureReplica(self: *HttpHost, record: catalog.ReplicaRecord) !raft_engine.runtime.EnsureReplicaResult {
        return try self.host.ensureReplica(record);
    }

    pub fn removeReplica(self: *HttpHost, group_id: u64) !void {
        try self.host.removeReplica(group_id);
    }

    pub fn refreshPeerEndpoints(self: *HttpHost, group_id: u64, node_id: u64) !usize {
        return try self.host.refreshPeerEndpoints(group_id, node_id);
    }

    pub fn upsertResolvedPeerEndpoints(self: *HttpHost, group_id: u64, node_id: u64, endpoints: []const peer_resolver.PeerEndpoint) !usize {
        return try self.host.upsertResolvedPeerEndpoints(group_id, node_id, endpoints);
    }

    pub fn removePeerRoute(self: *HttpHost, group_id: u64, node_id: u64) !bool {
        return try self.host.removePeerRoute(group_id, node_id);
    }

    pub fn runRound(self: *HttpHost, max_tick_groups: usize, max_ready_steps: usize) !raft_engine.runtime.multi_raft.HostRound {
        return try self.host.runRound(max_tick_groups, max_ready_steps);
    }

    pub fn runRoundBounded(
        self: *HttpHost,
        max_inbound_messages: usize,
        max_tick_groups: usize,
        max_ready_steps: usize,
    ) !raft_engine.runtime.multi_raft.HostRound {
        return try self.host.runRoundBounded(max_inbound_messages, max_tick_groups, max_ready_steps);
    }

    pub fn runProgressRoundBounded(
        self: *HttpHost,
        max_inbound_messages: usize,
        max_ready_steps: usize,
    ) !raft_engine.runtime.multi_raft.HostRound {
        return try self.host.runProgressRoundBounded(max_inbound_messages, max_ready_steps);
    }

    pub fn campaignGroup(self: *HttpHost, group_id: u64) !void {
        try self.host.campaignGroup(group_id);
    }

    pub fn propose(self: *HttpHost, group_id: u64, data: []const u8) !void {
        try self.host.propose(group_id, data);
    }

    pub fn proposeWithReceipt(self: *HttpHost, group_id: u64, data: []const u8, accepted_index: *?u64) !void {
        try self.host.proposeWithReceipt(group_id, data, accepted_index);
    }

    pub fn proposeBatchWithReceipt(
        self: *HttpHost,
        group_id: u64,
        payloads: []const []const u8,
        accepted_first_index: *?u64,
        accepted_last_index: *?u64,
    ) !void {
        try self.host.proposeBatchWithReceipt(
            group_id,
            payloads,
            accepted_first_index,
            accepted_last_index,
        );
    }

    pub fn transferLeader(self: *HttpHost, group_id: u64, transferee: u64) !void {
        try self.host.transferLeader(group_id, transferee);
    }

    pub fn forgetLeader(self: *HttpHost, group_id: u64) !void {
        try self.host.forgetLeader(group_id);
    }

    pub fn readIndex(self: *HttpHost, group_id: u64, request_ctx: []const u8) !void {
        try self.host.readIndex(group_id, request_ctx);
    }

    pub fn proposeConfChange(self: *HttpHost, group_id: u64, conf_change: raft_engine.core.ConfChange) !void {
        try self.host.proposeConfChange(group_id, conf_change);
    }

    pub fn proposeConfChangeV2(self: *HttpHost, group_id: u64, conf_change: raft_engine.core.ConfChangeV2) !void {
        try self.host.proposeConfChangeV2(group_id, conf_change);
    }

    pub fn raftStatus(self: *HttpHost, group_id: u64) ?raft_engine.core.Status {
        return self.host.raftStatus(group_id);
    }

    pub fn raftTermAt(self: *HttpHost, group_id: u64, index: u64) !u64 {
        return try self.host.raftTermAt(group_id, index);
    }

    pub fn leaderId(self: *HttpHost, group_id: u64) ?u64 {
        return self.host.leaderId(group_id);
    }

    pub fn isLocalLeader(self: *HttpHost, group_id: u64) bool {
        return self.host.isLocalLeader(group_id);
    }
};

pub fn mergeRuntimeHooks(base: RuntimeHooks, overlay: RuntimeHooks) RuntimeHooks {
    var merged = base;
    if (overlay.transport != null) merged.transport = overlay.transport;
    if (overlay.snapshot_transport != null) merged.snapshot_transport = overlay.snapshot_transport;
    if (overlay.group_storage != null) merged.group_storage = overlay.group_storage;
    if (overlay.disk_batcher != null) merged.disk_batcher = overlay.disk_batcher;
    if (overlay.state_machine != null) merged.state_machine = overlay.state_machine;
    if (overlay.apply_queue != null) merged.apply_queue = overlay.apply_queue;
    if (overlay.snapshot_throttle != null) merged.snapshot_throttle = overlay.snapshot_throttle;
    if (overlay.backpressure != null) merged.backpressure = overlay.backpressure;
    if (overlay.replica_catalog != null) merged.replica_catalog = overlay.replica_catalog;
    if (overlay.replica_factory != null) merged.replica_factory = overlay.replica_factory;
    return merged;
}

test "host can ensure and remove a replica" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) ReplicaDescriptorFactory {
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
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const RemovalCatalog = struct {
        fail_remove: bool = true,
        remove_calls: usize = 0,

        fn iface(self: *@This()) catalog.ReplicaCatalog {
            return .{
                .ptr = self,
                .vtable = &.{
                    .upsert_replica = upsertReplica,
                    .remove_replica = removeReplica,
                    .contains_replica = containsReplica,
                    .list_replicas = listReplicas,
                    .revision = revision,
                    .apply_batch = applyBatch,
                },
            };
        }

        fn upsertReplica(_: *anyopaque, _: catalog.ReplicaRecord) !void {}

        fn removeReplica(ptr: *anyopaque, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.remove_calls += 1;
            if (self.fail_remove) return error.InjectedCatalogRemovalFailure;
            return true;
        }

        fn containsReplica(_: *anyopaque, _: u64) bool {
            return true;
        }

        fn listReplicas(_: *anyopaque, alloc: std.mem.Allocator) ![]catalog.ReplicaRecord {
            return try alloc.alloc(catalog.ReplicaRecord, 0);
        }

        fn revision(_: *anyopaque) u64 {
            return 1;
        }

        fn applyBatch(
            _: *anyopaque,
            _: u64,
            _: []const catalog.ReplicaRecord,
            _: []const u64,
        ) !void {}
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var replica_catalog = RemovalCatalog{};
    var host = Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .replica_catalog = replica_catalog.iface(),
    });
    defer host.deinit();

    _ = try host.ensureReplica(.{
        .group_id = 41,
        .replica_id = 1,
        .local_node_id = 1,
    });
    try std.testing.expectEqual(.active, host.status(41));
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().hosted_groups);

    try std.testing.expectError(error.InjectedCatalogRemovalFailure, host.removeReplica(41));
    try std.testing.expectEqual(.active, host.status(41));

    replica_catalog.fail_remove = false;
    try host.removeReplica(41);
    try std.testing.expectEqual(.absent, host.status(41));
    try std.testing.expectEqual(@as(usize, 2), replica_catalog.remove_calls);
}

test "host rejects live snapshot uploads addressed to another node" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) ReplicaDescriptorFactory {
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
                .bootstrap = .empty,
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
    var host = Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
    });
    defer host.deinit();

    _ = try host.ensureReplica(.{
        .group_id = 41,
        .replica_id = 1,
        .local_node_id = 1,
    });

    const voters = try std.testing.allocator.dupe(u64, &[_]u64{1});
    const data = try std.testing.allocator.dupe(u8, "wrong-target");
    try std.testing.expectError(error.SnapshotUploadTargetMismatch, host.handleSnapshotUpload(.{
        .group_id = 41,
        .from = 2,
        .to = 3,
        .term = 7,
        .snapshot = .{
            .metadata = .{
                .index = 1,
                .term = 1,
                .conf_state = .{ .voters = voters },
            },
            .data = data,
        },
    }));
}

test "host queues live snapshot uploads for runtime round" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) ReplicaDescriptorFactory {
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
                .bootstrap = .empty,
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
    var host = Host.init(std.testing.allocator, .{
        .local_node_id = 1,
        .max_pending_inbound_snapshot_bytes = "queued-snapshot".len,
    }, .{
        .descriptor_factory = factory.iface(),
    });
    defer host.deinit();

    _ = try host.ensureReplica(.{
        .group_id = 41,
        .replica_id = 1,
        .local_node_id = 1,
    });

    const voters = try std.testing.allocator.dupe(u64, &[_]u64{1});
    const data = try std.testing.allocator.dupe(u8, "queued-snapshot");
    try host.handleSnapshotUpload(.{
        .group_id = 41,
        .from = 2,
        .to = 1,
        .term = 7,
        .snapshot = .{
            .metadata = .{
                .index = 1,
                .term = 1,
                .conf_state = .{ .voters = voters },
            },
            .data = data,
        },
    });

    try std.testing.expectEqual(@as(usize, 1), host.metrics.inbound_message_enqueues);
    try std.testing.expectEqual(@as(usize, 1), host.metrics.pending_inbound_messages);
    try std.testing.expectEqual(@as(usize, "queued-snapshot".len), host.metricsSnapshot().pending_inbound_snapshot_bytes);
    try std.testing.expectEqual(@as(usize, 0), host.metrics.inbound_message_drains);
    try std.testing.expectError(error.SnapshotAdmissionBackpressure, host.admitSnapshotUpload(.{
        .group_id = 41,
        .to = 1,
        .data_len = 1,
    }));
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().inbound_snapshot_admission_denials);

    _ = try host.runRoundBounded(1, 1, 1);
    try std.testing.expectEqual(@as(usize, 1), host.metrics.inbound_message_drains);
    try std.testing.expectEqual(@as(usize, 0), host.metrics.pending_inbound_messages);
    try std.testing.expectEqual(@as(usize, 0), host.metricsSnapshot().pending_inbound_snapshot_bytes);
}

test "host drops stale inbound peer batch groups without leaking pending storage" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) ReplicaDescriptorFactory {
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
                .bootstrap = .empty,
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
    var host = Host.init(std.testing.allocator, .{
        .local_node_id = 1,
        .runtime = .{ .max_single_apply_ready_bytes = 1 },
    }, .{
        .descriptor_factory = factory.iface(),
    });
    defer host.deinit();

    _ = try host.ensureReplica(.{
        .group_id = 41,
        .replica_id = 1,
        .local_node_id = 1,
    });

    const known_msg_a = raft_engine.core.Message{
        .msg_type = .heartbeat,
        .from = 2,
        .to = 1,
        .term = 1,
    };
    const known_msg_b = raft_engine.core.Message{
        .msg_type = .heartbeat,
        .from = 2,
        .to = 1,
        .term = 1,
    };
    const stale_msg = raft_engine.core.Message{
        .msg_type = .heartbeat,
        .from = 2,
        .to = 1,
        .term = 1,
    };
    try host.enqueueInboundBatch(.{
        .peer_id = 1,
        .groups = (&[_]raft_engine.runtime.transport_iface.GroupMessageBatch{
            .{
                .group_id = 41,
                .messages = (&[_]raft_engine.core.Message{ known_msg_a, known_msg_b })[0..],
            },
            .{
                .group_id = 99,
                .messages = (&[_]raft_engine.core.Message{stale_msg})[0..],
            },
        })[0..],
    });

    try std.testing.expectEqual(@as(usize, 2), host.metrics.inbound_message_enqueues);
    try std.testing.expectEqual(@as(usize, 2), host.metrics.pending_inbound_messages);

    _ = try host.runRoundBounded(1, 1, 1);
    try std.testing.expectEqual(@as(usize, 1), host.metrics.inbound_message_drains);
    try std.testing.expectEqual(@as(usize, 1), host.metrics.pending_inbound_messages);

    _ = try host.runRoundBounded(1, 1, 1);
    try std.testing.expectEqual(@as(usize, 2), host.metrics.inbound_message_drains);
    try std.testing.expectEqual(@as(usize, 0), host.metrics.pending_inbound_messages);

    // A single poisoned group must not terminate the shared progress driver.
    try host.campaignGroup(41);
    _ = try host.runRoundBounded(0, 1, 1);
    try std.testing.expectEqual(.quarantined, host.status(41));
    try std.testing.expectError(error.GroupLeaderUnavailable, host.campaignGroup(41));

    try host.enqueueInboundBatch(.{
        .peer_id = 1,
        .groups = (&[_]raft_engine.runtime.transport_iface.GroupMessageBatch{.{
            .group_id = 41,
            .messages = (&[_]raft_engine.core.Message{known_msg_a})[0..],
        }})[0..],
    });
    _ = try host.runRoundBounded(1, 1, 1);
    try std.testing.expectEqual(@as(usize, 1), host.metrics.quarantined_inbound_message_drops);
    try std.testing.expectEqual(@as(usize, 0), host.metrics.pending_inbound_messages);
}

test "host invokes backup restore bootstrapper exactly once before creating a replica" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) ReplicaDescriptorFactory {
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
        count: usize = 0,
        last_group_id: u64 = 0,
        last_snapshot_path: []const u8 = "",

        fn iface(self: *@This()) BackupRestoreBootstrapper {
            return .{
                .ptr = self,
                .vtable = &.{
                    .prepare_backup_restore = prepareBackupRestore,
                },
            };
        }

        fn prepareBackupRestore(ptr: *anyopaque, record: catalog.ReplicaRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const restore = record.backup_restore_bootstrap orelse return error.MissingBootstrapRecord;
            self.count += 1;
            self.last_group_id = record.group_id;
            self.last_snapshot_path = restore.snapshot_path;
        }
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var bootstrapper = Bootstrapper{};
    var host = Host.init(std.testing.allocator, .{
        .local_node_id = 1,
        // The injected bootstrapper owns preparation. This deliberately invalid
        // fallback root proves ensureReplica does not force a second path restore
        // after the raft group becomes active.
        .replica_root_dir = "/tmp/antfly-bootstrap-must-not-run-path-restore",
    }, .{
        .descriptor_factory = factory.iface(),
        .backup_restore_bootstrapper = bootstrapper.iface(),
    });
    defer host.deinit();

    _ = try host.ensureReplica(.{
        .group_id = 91,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .fetch_snapshot,
        .backup_restore_bootstrap = .{
            .backup_id = "snap-91",
            .artifact_backup_id = "snap-91",
            .location = "file:///tmp/backups",
            .snapshot_path = "snap-91/groups/91",
            .connection = "backup-store",
            .artifact_size_bytes = 4096,
            .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        },
    });

    try std.testing.expectEqual(@as(usize, 1), bootstrapper.count);
    try std.testing.expectEqual(@as(u64, 91), bootstrapper.last_group_id);
    try std.testing.expectEqualStrings("snap-91/groups/91", bootstrapper.last_snapshot_path);
    try std.testing.expectEqual(.active, host.status(91));
    const bootstrap_status = host.bootstrapStatus(91) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(.backup_db_snapshot_restore, bootstrap_status.kind);
    try std.testing.expectEqual(.succeeded, bootstrap_status.phase);
    try std.testing.expectEqual(@as(u64, 1), bootstrap_status.attempts);
    try std.testing.expect(bootstrap_status.last_updated_at_millis > 0);
    try std.testing.expect(bootstrap_status.last_error == null);
    try std.testing.expectEqualStrings("snap-91", bootstrap_status.backup_id orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings("snap-91/groups/91", bootstrap_status.snapshot_path orelse return error.TestExpectedEqual);
    const host_metrics = host.metricsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), host_metrics.backup_bootstrap_attempts);
    try std.testing.expectEqual(@as(usize, 1), host_metrics.backup_bootstrap_successes);
    try std.testing.expectEqual(@as(usize, 0), host_metrics.backup_bootstrap_failures);
    const listed = try host.listBootstrapStatuses(std.testing.allocator);
    defer host.freeBootstrapStatuses(std.testing.allocator, listed);
    try std.testing.expectEqual(@as(usize, 1), listed.len);
    try std.testing.expectEqual(@as(u64, 91), listed[0].group_id);
    try std.testing.expectEqual(.succeeded, listed[0].phase);
    try std.testing.expectEqualStrings("snap-91", listed[0].backup_id orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings("snap-91/groups/91", listed[0].snapshot_path orelse return error.TestExpectedEqual);

    _ = try host.ensureReplica(.{
        .group_id = 91,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .fetch_snapshot,
        .backup_restore_bootstrap = .{
            .backup_id = "snap-91",
            .artifact_backup_id = "snap-91",
            .location = "file:///tmp/backups",
            .snapshot_path = "snap-91/groups/91",
            .connection = "backup-store",
            .artifact_size_bytes = 4096,
            .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        },
    });
    try std.testing.expectEqual(@as(usize, 1), bootstrapper.count);
}

test "host records backup restore bootstrap failure when no handler is available" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) ReplicaDescriptorFactory {
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
    var host = Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
    });
    defer host.deinit();

    try std.testing.expectError(error.MissingBackupRestoreBootstrapHandler, host.ensureReplica(.{
        .group_id = 92,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .fetch_snapshot,
        .backup_restore_bootstrap = .{
            .backup_id = "snap-92",
            .artifact_backup_id = "snap-92",
            .location = "file:///tmp/backups",
            .snapshot_path = "snap-92/groups/92",
            .connection = "backup-store",
            .artifact_size_bytes = 4096,
            .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        },
    }));

    try std.testing.expectEqual(.failed, host.status(92));
    const bootstrap_status = host.bootstrapStatus(92) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(.backup_db_snapshot_restore, bootstrap_status.kind);
    try std.testing.expectEqual(.failed, bootstrap_status.phase);
    try std.testing.expectEqual(@as(u64, 1), bootstrap_status.attempts);
    try std.testing.expect(bootstrap_status.last_updated_at_millis > 0);
    try std.testing.expectEqualStrings("MissingBackupRestoreBootstrapHandler", bootstrap_status.last_error orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings("snap-92", bootstrap_status.backup_id orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings("snap-92/groups/92", bootstrap_status.snapshot_path orelse return error.TestExpectedEqual);
    const listed = try host.listBootstrapStatuses(std.testing.allocator);
    defer host.freeBootstrapStatuses(std.testing.allocator, listed);
    try std.testing.expectEqual(@as(usize, 1), listed.len);
    try std.testing.expectEqual(.failed, listed[0].phase);
    try std.testing.expectEqualStrings("MissingBackupRestoreBootstrapHandler", listed[0].last_error orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings("snap-92", listed[0].backup_id orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings("snap-92/groups/92", listed[0].snapshot_path orelse return error.TestExpectedEqual);
    const host_metrics = host.metricsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), host_metrics.backup_bootstrap_attempts);
    try std.testing.expectEqual(@as(usize, 0), host_metrics.backup_bootstrap_successes);
    try std.testing.expectEqual(@as(usize, 1), host_metrics.backup_bootstrap_failures);
}

test "host records committed bootstrap durability as pending instead of failed" {
    var host = Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{});
    defer host.deinit();
    const restore: catalog.BackupRestoreBootstrapRecord = .{
        .backup_id = "snap-pending",
        .artifact_backup_id = "snap-pending",
        .location = "file:///tmp/backups",
        .snapshot_path = "snap-pending/groups/94",
        .connection = "backup-store",
        .artifact_size_bytes = 4096,
        .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    };

    host.noteBootstrapPreparing(94, .backup_db_snapshot_restore, restore);
    host.noteBootstrapFailure(94, .backup_db_snapshot_restore, error.GenerationDurabilityUncertain, restore);

    try std.testing.expectEqual(.starting, host.status(94));
    const status_value = host.bootstrapStatus(94) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(.durability_pending, status_value.phase);
    try std.testing.expectEqualStrings("GenerationDurabilityUncertain", status_value.last_error orelse return error.TestExpectedEqual);
    const metrics = host.metricsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), metrics.backup_bootstrap_attempts);
    try std.testing.expectEqual(@as(usize, 1), metrics.backup_bootstrap_durability_pending);
    try std.testing.expectEqual(@as(usize, 0), metrics.backup_bootstrap_failures);
    try std.testing.expectEqual(@as(usize, 0), metrics.backup_bootstrap_successes);
}

test "host does not perform path restore without a bootstrap authority owner" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) ReplicaDescriptorFactory {
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

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/host-backup-failure-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var host = Host.init(std.testing.allocator, .{
        .local_node_id = 1,
        .replica_root_dir = replica_root,
    }, .{
        .descriptor_factory = factory.iface(),
    });
    defer host.deinit();

    try std.testing.expectError(error.MissingBackupRestoreBootstrapHandler, host.ensureReplica(.{
        .group_id = 93,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .fetch_snapshot,
        .backup_restore_bootstrap = .{
            .backup_id = "snap-93",
            .artifact_backup_id = "snap-93",
            .location = "file:///tmp/backups",
            .snapshot_path = "snap-93/groups/93",
            .connection = "backup-store",
            .artifact_size_bytes = 4096,
            .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        },
    }));

    try std.testing.expectEqual(.failed, host.status(93));
    const bootstrap_status = host.bootstrapStatus(93) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(.backup_db_snapshot_restore, bootstrap_status.kind);
    try std.testing.expectEqual(.failed, bootstrap_status.phase);
    try std.testing.expectEqual(@as(u64, 1), bootstrap_status.attempts);
    try std.testing.expect(bootstrap_status.last_updated_at_millis > 0);
    try std.testing.expectEqualStrings("MissingBackupRestoreBootstrapHandler", bootstrap_status.last_error orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings("snap-93", bootstrap_status.backup_id orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings("snap-93/groups/93", bootstrap_status.snapshot_path orelse return error.TestExpectedEqual);
    const host_metrics = host.metricsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), host_metrics.backup_bootstrap_attempts);
    try std.testing.expectEqual(@as(usize, 0), host_metrics.backup_bootstrap_successes);
    try std.testing.expectEqual(@as(usize, 1), host_metrics.backup_bootstrap_failures);
}

const TestBackupRestoreBootstrapper = struct {
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    open_options: @import("../api/backups.zig").OpenOptions,

    fn iface(self: *@This()) BackupRestoreBootstrapper {
        return .{
            .ptr = self,
            .vtable = &.{ .prepare_backup_restore = prepareBackupRestore },
        };
    }

    fn prepareBackupRestore(ptr: *anyopaque, record: catalog.ReplicaRecord) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try backup_restore.applyBackupRestoreFromRecordWithOptions(
            self.alloc,
            self.replica_root_dir,
            record.group_id,
            record.backup_restore_bootstrap orelse return error.MissingBootstrapRecord,
            self.open_options,
        );
    }
};

fn testBackupRestoreNodeConfig(alloc: std.mem.Allocator) !@import("../common/config.zig").Config {
    return @import("../common/config.zig").Config.parseFromSlice(alloc,
        \\{
        \\  "connections": {
        \\    "test-backups": {
        \\      "kind": "external_io",
        \\      "capabilities": ["restore.read"],
        \\      "external_io": { "protocol": "filesystem", "root": "/" }
        \\    }
        \\  }
        \\}
    );
}

test "host restores through an explicitly authorized bootstrap owner" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) ReplicaDescriptorFactory {
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

    const db_mod = @import("../storage/db/mod.zig");
    const backups_api = @import("../api/backups.zig");

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/host-backup-bootstrapper-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/host-backup-bootstrapper-backup", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);
    const source_db_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/host-backup-bootstrapper-source", .{tmp.sub_path});
    defer std.testing.allocator.free(source_db_path);

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), source_db_path) catch {};

    var source_db = try db_mod.DB.open(std.testing.allocator, source_db_path, .{});
    defer {
        source_db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), source_db_path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }
    try source_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
        .sync_level = .full_index,
    });
    _ = try source_db.snapshot("snap1-g91");

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, "{s}.snapshots/snap1-g91", .{source_db_path});
    defer std.testing.allocator.free(snapshot_root);
    const dest_root = try backups_api.shardSnapshotPath(std.testing.allocator, backup_root, "snap1", 91);
    defer std.testing.allocator.free(dest_root);
    try backups_api.copyDirectoryRecursive(std.testing.allocator, snapshot_root, dest_root);
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const backup_root_abs = try std.fs.path.resolve(std.testing.allocator, &.{ cwd, backup_root });
    defer std.testing.allocator.free(backup_root_abs);
    const restore_location = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{backup_root_abs});
    defer std.testing.allocator.free(restore_location);
    var artifact_integrity = try backups_api.artifactIntegrityAlloc(
        std.testing.allocator,
        std.testing.io,
        .native,
        dest_root,
    );
    defer artifact_integrity.deinit(std.testing.allocator);
    var node_config = try testBackupRestoreNodeConfig(std.testing.allocator);
    defer node_config.deinit();

    const manifest = try backups_api.createManifest(
        std.testing.allocator,
        "snap1",
        .native,
        &.{
            .table_id = 7,
            .name = "docs",
            .description = "docs table",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        },
        &.{.{
            .group_id = 91,
            .start_key = "doc:a",
            .end_key = null,
            .snapshot_path = "snap1/groups/91",
            .artifact_size_bytes = artifact_integrity.size_bytes,
            .artifact_sha256 = artifact_integrity.sha256,
        }},
    );
    defer {
        var owned = manifest;
        owned.deinit(std.testing.allocator);
    }
    try backups_api.writeManifest(std.testing.allocator, backup_root, &manifest);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var bootstrapper = TestBackupRestoreBootstrapper{
        .alloc = std.testing.allocator,
        .replica_root_dir = replica_root,
        .open_options = .{
            .node_config = &node_config,
            .io = io_impl.io(),
        },
    };
    var host = Host.init(std.testing.allocator, .{
        .local_node_id = 1,
        .replica_root_dir = replica_root,
    }, .{
        .descriptor_factory = factory.iface(),
        .backup_restore_bootstrapper = bootstrapper.iface(),
    });
    defer host.deinit();

    _ = try host.ensureReplica(.{
        .group_id = 91,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .fetch_snapshot,
        .backup_restore_bootstrap = .{
            .backup_id = "snap1",
            .artifact_backup_id = "snap1",
            .location = restore_location,
            .snapshot_path = "snap1/groups/91",
            .connection = "test-backups",
            .artifact_size_bytes = artifact_integrity.size_bytes,
            .artifact_sha256 = artifact_integrity.sha256,
        },
    });

    const db_path = try backup_restore.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root, 91);
    defer std.testing.allocator.free(db_path);
    var restored_db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
    defer restored_db.close();
    const doc = (try restored_db.get(std.testing.allocator, "doc:a")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(doc);
    try std.testing.expect(std.mem.indexOf(u8, doc, "\"alpha\"") != null);
}

test "host restores backup bootstrap replicas from file-backed catalog on restart" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) ReplicaDescriptorFactory {
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

    const db_mod = @import("../storage/db/mod.zig");
    const backups_api = @import("../api/backups.zig");
    const storage_mod = @import("storage/catalog.zig");

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/host-backup-restart-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const backup_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/host-backup-restart-backup", .{tmp.sub_path});
    defer std.testing.allocator.free(backup_root);
    const source_db_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/host-backup-restart-source", .{tmp.sub_path});
    defer std.testing.allocator.free(source_db_path);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/host-backup-restart-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

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
    _ = try source_db.snapshot("snap1-g92");

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, "{s}.snapshots/snap1-g92", .{source_db_path});
    defer std.testing.allocator.free(snapshot_root);
    const dest_root = try backups_api.shardSnapshotPath(std.testing.allocator, backup_root, "snap1", 92);
    defer std.testing.allocator.free(dest_root);
    try backups_api.copyDirectoryRecursive(std.testing.allocator, snapshot_root, dest_root);
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const backup_root_abs = try std.fs.path.resolve(std.testing.allocator, &.{ cwd, backup_root });
    defer std.testing.allocator.free(backup_root_abs);
    const restore_location = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{backup_root_abs});
    defer std.testing.allocator.free(restore_location);
    var artifact_integrity = try backups_api.artifactIntegrityAlloc(
        std.testing.allocator,
        std.testing.io,
        .native,
        dest_root,
    );
    defer artifact_integrity.deinit(std.testing.allocator);
    var node_config = try testBackupRestoreNodeConfig(std.testing.allocator);
    defer node_config.deinit();
    var bootstrapper = TestBackupRestoreBootstrapper{
        .alloc = std.testing.allocator,
        .replica_root_dir = replica_root,
        .open_options = .{
            .node_config = &node_config,
            .io = io_impl.io(),
        },
    };

    const manifest = try backups_api.createManifest(
        std.testing.allocator,
        "snap1",
        .native,
        &.{
            .table_id = 7,
            .name = "docs",
            .description = "docs table",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        },
        &.{.{
            .group_id = 92,
            .start_key = "doc:a",
            .end_key = null,
            .snapshot_path = "snap1/groups/92",
            .artifact_size_bytes = artifact_integrity.size_bytes,
            .artifact_sha256 = artifact_integrity.sha256,
        }},
    );
    defer {
        var owned = manifest;
        owned.deinit(std.testing.allocator);
    }
    try backups_api.writeManifest(std.testing.allocator, backup_root, &manifest);

    {
        var file_catalog = try storage_mod.FileReplicaCatalog.init(std.testing.allocator, replica_catalog_path);
        defer file_catalog.deinit();
        var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
        defer store.deinit();
        var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
        var host = Host.init(std.testing.allocator, .{
            .local_node_id = 1,
            .replica_root_dir = replica_root,
        }, .{
            .descriptor_factory = factory.iface(),
            .replica_catalog = file_catalog.catalog(),
            .backup_restore_bootstrapper = bootstrapper.iface(),
        });
        defer host.deinit();

        _ = try host.ensureReplica(.{
            .group_id = 92,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .fetch_snapshot,
            .backup_restore_bootstrap = .{
                .backup_id = "snap1",
                .artifact_backup_id = "snap1",
                .location = restore_location,
                .snapshot_path = "snap1/groups/92",
                .connection = "test-backups",
                .artifact_size_bytes = artifact_integrity.size_bytes,
                .artifact_sha256 = artifact_integrity.sha256,
            },
        });
    }

    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};

    {
        var reopened_catalog = try storage_mod.FileReplicaCatalog.init(std.testing.allocator, replica_catalog_path);
        defer reopened_catalog.deinit();
        var restarted_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
        defer restarted_store.deinit();
        var restarted_factory = Factory{ .alloc = std.testing.allocator, .store = &restarted_store };
        var restarted_host = Host.init(std.testing.allocator, .{
            .local_node_id = 1,
            .replica_root_dir = replica_root,
        }, .{
            .descriptor_factory = restarted_factory.iface(),
            .replica_catalog = reopened_catalog.catalog(),
            .backup_restore_bootstrapper = bootstrapper.iface(),
        });
        defer restarted_host.deinit();

        try std.testing.expectEqual(@as(usize, 1), try restarted_host.restoreReplicasFromCatalog(std.testing.allocator));
    }

    const db_path = try backup_restore.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root, 92);
    defer std.testing.allocator.free(db_path);
    var restored_db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
    defer restored_db.close();
    const doc = (try restored_db.get(std.testing.allocator, "doc:a")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(doc);
    try std.testing.expect(std.mem.indexOf(u8, doc, "\"alpha\"") != null);
}

test "http host starts listener and serves health route" {
    var http_host = try HttpHost.init(std.testing.allocator, .{
        .host = .{ .local_node_id = 1 },
        .transport = .{
            .snapshot = .{ .root_dir = "/tmp" },
        },
    }, .{});
    defer http_host.deinit();
    try http_host.start();

    const base_uri = try http_host.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const health_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/raft/v1/health", .{base_uri});
    defer std.testing.allocator.free(health_uri);

    var resp = try http_host.request_executor.execute(std.testing.allocator, .{
        .method = .GET,
        .uri = health_uri,
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("ok", resp.body);
}
