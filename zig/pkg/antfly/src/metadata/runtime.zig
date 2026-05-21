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
const antfly = @import("../root.zig");
const fs_paths = @import("../common/fs_paths.zig");
const group_ids = @import("../common/group_ids.zig");
const build_options = @import("build_options");
const raft_engine = @import("raft_engine");
const tracing = @import("../tracing/mod.zig");
const backend_runtime_mod = @import("../storage/background_runtime.zig");

const setup_io_thread_stack_size = 1 * 1024 * 1024;

const CliConfig = struct {
    config_path: ?[]const u8 = null,
    raft_host: ?[]const u8 = null,
    raft_port: ?u16 = null,
    api_host: ?[]const u8 = null,
    api_port: ?u16 = null,
    cluster_json: ?[]const u8 = null,
    join: bool = false,
    health_enabled: ?bool = null,
    health_port: ?u16 = null,
    tick_ms: ?u64 = null,
    local_node_id: ?u64 = null,
    replica_root_dir: ?[]const u8 = null,
    replica_catalog_path: ?[]const u8 = null,
    snapshot_root_dir: ?[]const u8 = null,
    help: bool = false,
};

const Factory = struct {
    alloc: std.mem.Allocator,
    store: *raft_engine.core.MemoryStorage,
    metadata_group_id: u64,
    metadata_peer_node_ids: []u64 = &.{},

    fn deinit(self: *@This()) void {
        if (self.metadata_peer_node_ids.len > 0) self.alloc.free(self.metadata_peer_node_ids);
        self.* = undefined;
    }

    fn iface(self: *@This()) antfly.raft.ReplicaDescriptorFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_descriptor = buildDescriptor,
                .free_descriptor = freeDescriptor,
            },
        };
    }

    fn buildDescriptor(ptr: *anyopaque, record: antfly.raft.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const peer_source = if (record.group_id == self.metadata_group_id and self.metadata_peer_node_ids.len > 0)
            self.metadata_peer_node_ids
        else
            &[_]u64{record.local_node_id};
        const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, peer_source);
        errdefer self.alloc.free(peers);
        var bootstrap = try antfly.raft.catalog.runtimeBootstrapFromRecord(self.alloc, record);
        errdefer antfly.raft.catalog.freeRuntimeBootstrap(self.alloc, &bootstrap);
        return .{
            .group = .{
                .group_id = record.group_id,
                .local_node_id = record.local_node_id,
                .raft_config = .{
                    .id = record.local_node_id,
                    .group_id = record.group_id,
                    .peers = peers,
                    .election_tick = 5,
                    .heartbeat_tick = 1,
                    .pre_vote = true,
                    .check_quorum = true,
                    .random_seed = antfly.raft.stableRandomSeed(record.group_id, record.local_node_id),
                },
                .storage = self.store.storage(),
            },
            .bootstrap = bootstrap,
        };
    }

    fn freeDescriptor(_: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
        antfly.raft.catalog.freeRuntimeBootstrap(alloc, &desc.bootstrap);
        alloc.free(desc.group.raft_config.peers);
    }
};

const ResolvedPaths = struct {
    replica_root_dir: []u8,
    replica_catalog_path: []u8,
    snapshot_root_dir: []u8,

    fn deinit(self: ResolvedPaths, alloc: std.mem.Allocator) void {
        alloc.free(self.replica_root_dir);
        alloc.free(self.replica_catalog_path);
        alloc.free(self.snapshot_root_dir);
    }
};

/// Backs the metadata server's health/metrics endpoints. Exposes local raft
/// host metrics and managed-service metrics as Prometheus text, and reports
/// readiness by probing the metadata service status. Shared by the
/// standalone metadata runtime and the swarm runtime so both expose the
/// same metric set.
pub const HealthSource = struct {
    server: *Server,

    pub fn readiness(self: *HealthSource) antfly.common.health_server.ReadinessChecker {
        return .{
            .ptr = self,
            .vtable = &.{ .check = checkReady },
        };
    }

    pub fn metricsWriter(self: *HealthSource) antfly.common.health_server.MetricsWriter {
        return .{
            .ptr = self,
            .vtable = &.{ .write_metrics = writeMetrics },
        };
    }

    fn checkReady(ptr: *anyopaque) bool {
        const self: *HealthSource = @ptrCast(@alignCast(ptr));
        _ = self.server.metadataHttpService().status() catch return false;
        return true;
    }

    fn writeMetrics(ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void {
        const self: *HealthSource = @ptrCast(@alignCast(ptr));
        const svc = self.server.metadataHttpService();
        const host_metrics = svc.raft.host.http_host.metricsSnapshot();
        const svc_metrics = svc.metrics();

        const append = antfly.common.health_server.appendPromMetric;

        try append(writer, "antfly_raft_hosted_groups", "gauge", "Number of raft groups hosted on this node", @intCast(host_metrics.hosted_groups));
        try append(writer, "antfly_raft_reconcile_rounds_total", "counter", "Total number of reconcile rounds", @intCast(host_metrics.reconcile_rounds));
        try append(writer, "antfly_raft_ensure_replica_calls_total", "counter", "Total ensure_replica calls", @intCast(host_metrics.ensure_replica_calls));
        try append(writer, "antfly_raft_remove_replica_calls_total", "counter", "Total remove_replica calls", @intCast(host_metrics.remove_replica_calls));
        try append(writer, "antfly_raft_runtime_rounds_total", "counter", "Total raft runtime rounds", @intCast(host_metrics.runtime_rounds));
        try append(writer, "antfly_raft_backup_bootstrap_attempts_total", "counter", "Total backup bootstrap attempts", @intCast(host_metrics.backup_bootstrap_attempts));
        try append(writer, "antfly_raft_backup_bootstrap_failures_total", "counter", "Total backup bootstrap failures", @intCast(host_metrics.backup_bootstrap_failures));
        try append(writer, "antfly_raft_backup_bootstrap_successes_total", "counter", "Total backup bootstrap successes", @intCast(host_metrics.backup_bootstrap_successes));

        try append(writer, "antfly_service_queued_updates", "gauge", "Pending metadata updates waiting to apply", @intCast(svc_metrics.queued_updates));
        try append(writer, "antfly_service_applied_updates_total", "counter", "Total applied metadata updates", @intCast(svc_metrics.applied_updates));
        try append(writer, "antfly_service_sync_rounds_total", "counter", "Total metadata sync rounds", @intCast(svc_metrics.sync_rounds));
        try append(writer, "antfly_service_read_lease_requests_total", "counter", "Total readable-lease requests", @intCast(svc_metrics.read_lease_requests));
        try append(writer, "antfly_service_split_transitions_queued", "gauge", "Queued split transitions", @intCast(svc_metrics.queued_split_transitions));
        try append(writer, "antfly_service_split_transitions_completed_total", "counter", "Completed split transitions", @intCast(svc_metrics.completed_split_transitions));
        try append(writer, "antfly_service_merge_transitions_queued", "gauge", "Queued merge transitions", @intCast(svc_metrics.queued_merge_transitions));
        try append(writer, "antfly_service_merge_transitions_completed_total", "counter", "Completed merge transitions", @intCast(svc_metrics.completed_merge_transitions));
    }
};

pub const ListenerConfig = struct {
    bind_host: []const u8,
    bind_port: u16,
};

pub const MetadataClusterPeer = struct {
    node_id: u64,
    raft_url: []const u8,
};

pub const ServerConfig = struct {
    local_node_id: u64 = 1,
    metadata_group_id: u64 = group_ids.main_metadata_group_id,
    metadata_cluster_peers: []const MetadataClusterPeer = &.{},
    metadata_orchestration_urls: []const antfly.metadata_service.MetadataOrchestrationUrl = &.{},
    replica_root_dir: []const u8,
    replica_catalog_path: []const u8,
    snapshot_root_dir: []const u8,
    observe_local_replica_root: bool = true,
    replica_state_backend: antfly.raft.ReplicaStateBackend = .wal,
    bind_host: []const u8 = "127.0.0.1",
    bind_port: u16 = 0,
    admin_bind_host: []const u8 = "127.0.0.1",
    admin_bind_port: u16 = 0,
    reconciler_config: antfly.metadata.reconciler.Reconciler.Config = .{},
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
    secret_store: ?*antfly.common.secrets.FileStore = null,
};

pub const Server = struct {
    alloc: std.mem.Allocator,
    store: *raft_engine.core.MemoryStorage,
    factory: *Factory,
    server: antfly.metadata_server.MetadataServer,
    replica_root_dir: []u8,
    replica_catalog_path: []u8,
    snapshot_root_dir: []u8,
    bind_host: []u8,
    admin_bind_host: []u8,

    pub fn init(alloc: std.mem.Allocator, cfg: ServerConfig) !Server {
        var result: Server = undefined;
        result.alloc = alloc;
        result.store = try alloc.create(raft_engine.core.MemoryStorage);
        errdefer alloc.destroy(result.store);
        result.store.* = raft_engine.core.MemoryStorage.init(alloc);
        errdefer result.store.deinit();
        result.factory = try alloc.create(Factory);
        errdefer alloc.destroy(result.factory);
        result.factory.* = .{
            .alloc = alloc,
            .store = result.store,
            .metadata_group_id = cfg.metadata_group_id,
            .metadata_peer_node_ids = try allocMetadataPeerNodeIds(alloc, cfg.local_node_id, cfg.metadata_cluster_peers),
        };
        errdefer result.factory.deinit();
        result.replica_root_dir = try alloc.dupe(u8, cfg.replica_root_dir);
        errdefer alloc.free(result.replica_root_dir);
        result.replica_catalog_path = try alloc.dupe(u8, cfg.replica_catalog_path);
        errdefer alloc.free(result.replica_catalog_path);
        result.snapshot_root_dir = try alloc.dupe(u8, cfg.snapshot_root_dir);
        errdefer alloc.free(result.snapshot_root_dir);
        result.bind_host = try alloc.dupe(u8, cfg.bind_host);
        errdefer alloc.free(result.bind_host);
        result.admin_bind_host = try alloc.dupe(u8, cfg.admin_bind_host);
        errdefer alloc.free(result.admin_bind_host);
        const service_cfg = antfly.metadata_service.MetadataServiceConfig{
            .observe_local_replica_root = cfg.observe_local_replica_root,
            .backend_runtime = cfg.backend_runtime,
            .metadata_orchestration_urls = cfg.metadata_orchestration_urls,
            .secret_store = cfg.secret_store,
        };
        result.server = try antfly.metadata_server.MetadataServer.init(alloc, .{
            .http = .{
                .http = .{
                    .host = .{
                        .local_node_id = cfg.local_node_id,
                        .metadata_group_id = cfg.metadata_group_id,
                        .replica_root_dir = result.replica_root_dir,
                        .replica_catalog_path = result.replica_catalog_path,
                        .replica_state_backend = cfg.replica_state_backend,
                        .trace_logger = if (build_options.with_tla) tracing.stderrRaftTraceLogger() else null,
                    },
                    .listener = .{
                        .bind_host = result.bind_host,
                        .bind_port = cfg.bind_port,
                    },
                    .transport = .{
                        .snapshot = .{
                            .root_dir = result.snapshot_root_dir,
                        },
                    },
                },
            },
            .admin_listener = .{
                .bind_host = result.admin_bind_host,
                .bind_port = cfg.admin_bind_port,
            },
            .service = service_cfg,
            .reconciler_config = cfg.reconciler_config,
        }, .{
            .http = .{
                .http = .{
                    .http = .{
                        .host = .{
                            .descriptor_factory = result.factory.iface(),
                        },
                    },
                },
            },
        });
        errdefer result.server.deinit();
        return result;
    }

    pub fn deinit(self: *Server) void {
        self.server.deinit();
        self.alloc.free(self.admin_bind_host);
        self.alloc.free(self.bind_host);
        self.alloc.free(self.snapshot_root_dir);
        self.alloc.free(self.replica_catalog_path);
        self.alloc.free(self.replica_root_dir);
        self.factory.deinit();
        self.store.deinit();
        self.alloc.destroy(self.factory);
        self.alloc.destroy(self.store);
        self.* = undefined;
    }

    pub fn start(self: *Server) !void {
        try self.server.start();
    }

    pub fn bootstrapCluster(
        self: *Server,
        metadata_group_id: u64,
        local_node_id: u64,
        cluster_peers: []const MetadataClusterPeer,
    ) !void {
        if (cluster_peers.len == 0) return try self.bootstrapLocal(metadata_group_id, local_node_id);

        const local_index = indexOfClusterPeer(cluster_peers, local_node_id) orelse return error.MissingLocalMetadataPeer;
        var peer_node_ids = try self.alloc.alloc(u64, cluster_peers.len - 1);
        defer self.alloc.free(peer_node_ids);
        var peer_index: usize = 0;
        for (cluster_peers) |peer| {
            if (peer.node_id == local_node_id) continue;
            peer_node_ids[peer_index] = peer.node_id;
            peer_index += 1;
        }

        const local_record: antfly.raft.catalog.ReplicaRecord = .{
            .group_id = metadata_group_id,
            .replica_id = @as(u64, @intCast(local_index + 1)),
            .local_node_id = local_node_id,
            .bootstrap_mode = .empty,
        };
        if (self.server.svc.raft.host.status(metadata_group_id) == .absent) {
            _ = try self.server.svc.ensureMetadataReplica(local_record);
        }
        var route_endpoints = try self.alloc.alloc([1]antfly.raft.PeerEndpoint, cluster_peers.len);
        defer self.alloc.free(route_endpoints);
        for (cluster_peers, 0..) |peer, index| {
            route_endpoints[index][0] = .{
                .protocol = .http,
                .address = peer.raft_url,
                .metadata = "",
            };
            _ = try self.server.svc.raft.host.http_host.upsertResolvedPeerEndpoints(
                metadata_group_id,
                peer.node_id,
                route_endpoints[index][0..],
            );
        }
        var updates = std.ArrayListUnmanaged(antfly.raft.MetadataUpdate).empty;
        defer updates.deinit(self.alloc);
        try updates.append(self.alloc, .{
            .replica_intent = .{
                .upsert = .{
                    .record = local_record,
                    .peer_node_ids = peer_node_ids,
                },
            },
        });
        for (cluster_peers, 0..) |peer, index| {
            try updates.append(self.alloc, .{
                .peer_route = .{
                    .upsert = .{
                        .group_id = metadata_group_id,
                        .node_id = peer.node_id,
                        .endpoints = route_endpoints[index][0..],
                    },
                },
            });
        }

        try self.server.svc.raft.submitBatch(updates.items);
        _ = try self.server.svc.syncPending();
    }

    pub fn bootstrapLocal(self: *Server, metadata_group_id: u64, local_node_id: u64) !void {
        if (self.server.svc.raft.host.status(metadata_group_id) == .absent) {
            _ = try self.server.svc.ensureMetadataReplica(.{
                .group_id = metadata_group_id,
                .replica_id = 1,
                .local_node_id = local_node_id,
                .bootstrap_mode = .empty,
            });
        }
        try self.server.campaignMetadataGroup();
        const observe_local_replica_root = self.server.svc.observe_local_replica_root;
        self.server.svc.observe_local_replica_root = false;
        defer self.server.svc.observe_local_replica_root = observe_local_replica_root;
        try self.server.runRound();
        _ = try self.server.svc.ensureMetadataReplica(.{
            .group_id = metadata_group_id,
            .replica_id = 1,
            .local_node_id = local_node_id,
            .bootstrap_mode = .persisted,
        });
    }

    pub fn runRound(self: *Server) !void {
        try self.server.runRound();
    }

    pub fn runCdcRound(self: *Server) !void {
        try self.server.runCdcRound();
    }

    pub fn status(self: *Server) !antfly.metadata_api.MetadataStatus {
        return try self.server.status();
    }

    pub fn setLocalReplicaRootReconcileHook(self: *Server, hook: ?antfly.metadata_service.LocalReplicaRootReconcileHook) void {
        self.server.setLocalReplicaRootReconcileHook(hook);
    }

    pub fn setLocalReplicaRootReconcilePermitHook(self: *Server, hook: ?antfly.metadata_service.LocalReplicaRootReconcilePermitHook) void {
        self.server.setLocalReplicaRootReconcilePermitHook(hook);
    }

    pub fn setCdcWriteSource(self: *Server, source: antfly.public_api.TableWriteSource) void {
        self.server.setCdcWriteSource(source);
    }

    pub fn metadataHttpService(self: *Server) *antfly.metadata_service.MetadataHttpService {
        return self.server.svc;
    }

    pub fn baseUri(self: *Server, alloc: std.mem.Allocator) ![]u8 {
        return try self.server.baseUri(alloc);
    }

    pub fn adminBaseUri(self: *Server, alloc: std.mem.Allocator) ![]u8 {
        return try self.server.adminBaseUri(alloc);
    }
};

fn indexOfClusterPeer(cluster_peers: []const MetadataClusterPeer, node_id: u64) ?usize {
    for (cluster_peers, 0..) |peer, index| {
        if (peer.node_id == node_id) return index;
    }
    return null;
}

fn allocMetadataPeerNodeIds(
    alloc: std.mem.Allocator,
    local_node_id: u64,
    cluster_peers: []const MetadataClusterPeer,
) ![]u64 {
    if (cluster_peers.len == 0) return &.{};
    if (indexOfClusterPeer(cluster_peers, local_node_id) == null) return error.MissingLocalMetadataPeer;
    var out = try alloc.alloc(u64, cluster_peers.len);
    errdefer alloc.free(out);
    for (cluster_peers, 0..) |peer, index| out[index] = peer.node_id;
    std.mem.sort(u64, out, {}, comptime std.sort.asc(u64));
    return out;
}

pub fn run(init: std.process.Init) !void {
    const alloc = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly_metadata";
    return try runFromIterator(init, argv0, &args);
}

pub fn runFromIterator(
    init: std.process.Init,
    argv0: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    const alloc = init.gpa;
    const cli = try parseCli(args);
    if (cli.help) {
        printUsage(argv0);
        return;
    }

    var loaded_config: ?antfly.common.config.Config = if (cli.config_path) |config_path|
        try antfly.common.config.loadFromPath(alloc, config_path)
    else
        null;
    defer if (loaded_config) |*cfg| cfg.deinit();

    const resolved = try resolvePaths(alloc, cli, if (loaded_config) |*cfg| cfg else null);
    defer resolved.deinit(alloc);

    var setup_io = std.Io.Threaded.init(alloc, .{ .stack_size = setup_io_thread_stack_size });
    defer setup_io.deinit();
    try ensureDirPath(setup_io.io(), resolved.replica_root_dir);
    try ensureParent(setup_io.io(), resolved.replica_catalog_path);
    try ensureDirPath(setup_io.io(), resolved.snapshot_root_dir);

    var active_audio_runtime = try antfly.common.audio_runtime.ActiveRuntime.init(
        alloc,
        init.io,
        if (loaded_config) |*cfg| cfg else null,
    );
    defer active_audio_runtime.deinit();

    const local_node_id = cli.local_node_id orelse 1;
    const metadata_group_id = group_ids.main_metadata_group_id;
    const cluster_peers = try resolveMetadataClusterPeers(alloc, cli.cluster_json, if (loaded_config) |*cfg| cfg else null);
    defer freeMetadataClusterPeers(alloc, cluster_peers);
    const orchestration_urls = try resolveMetadataOrchestrationUrls(alloc, if (loaded_config) |*cfg| cfg else null);
    defer freeMetadataOrchestrationUrls(alloc, orchestration_urls);
    if (cli.join) return error.UnsupportedMetadataJoin;
    const listener = resolveRaftListener(cli, if (loaded_config) |*cfg| cfg else null);
    const admin_listener = resolveAdminListener(cli, if (loaded_config) |*cfg| cfg else null, local_node_id, listener.bind_host);

    var server = try Server.init(alloc, .{
        .local_node_id = local_node_id,
        .metadata_group_id = metadata_group_id,
        .metadata_cluster_peers = cluster_peers,
        .metadata_orchestration_urls = orchestration_urls,
        .replica_root_dir = resolved.replica_root_dir,
        .replica_catalog_path = resolved.replica_catalog_path,
        .snapshot_root_dir = resolved.snapshot_root_dir,
        .bind_host = listener.bind_host,
        .bind_port = listener.bind_port,
        .admin_bind_host = admin_listener.bind_host,
        .admin_bind_port = admin_listener.bind_port,
        .reconciler_config = shardAllocationReconcilerConfig(if (loaded_config) |*cfg| cfg else null),
    });
    defer server.deinit();
    try server.start();
    try server.bootstrapCluster(metadata_group_id, local_node_id, cluster_peers);

    const base_uri = try server.baseUri(alloc);
    defer alloc.free(base_uri);
    std.debug.print("metadata raft api listening on {s}\n", .{base_uri});

    const admin_uri = try server.adminBaseUri(alloc);
    defer alloc.free(admin_uri);
    std.debug.print("metadata admin api listening on {s}\n", .{admin_uri});

    var metadata_health = HealthSource{ .server = &server };
    const health_enabled = cli.health_enabled orelse if (loaded_config) |*cfg| cfg.health_enabled else true;
    const health_port = if (health_enabled)
        cli.health_port orelse if (loaded_config) |*cfg| cfg.health_port else antfly.common.config.default_health_port
    else
        null;
    const health_server = try antfly.common.health_server.HealthServer.startIfConfigured(
        alloc,
        "metadata",
        health_port,
        metadata_health.readiness(),
        metadata_health.metricsWriter(),
    );
    defer if (health_server) |hs| hs.deinit();

    const tick_ms = cli.tick_ms orelse 25;
    var req = std.posix.timespec{
        .sec = @intCast(tick_ms / std.time.ms_per_s),
        .nsec = @intCast((tick_ms % std.time.ms_per_s) * std.time.ns_per_ms),
    };
    while (true) {
        try server.runRound();
        try server.runCdcRound();
        const err = std.posix.errno(std.posix.system.nanosleep(&req, &req));
        switch (err) {
            .SUCCESS => {},
            .INTR => continue,
            else => return std.posix.unexpectedErrno(err),
        }
    }
}

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
        if (std.mem.eql(u8, arg, "--raft-host")) {
            cfg.raft_host = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--raft-port")) {
            cfg.raft_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--api-host")) {
            cfg.api_host = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--api-port")) {
            cfg.api_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--cluster")) {
            cfg.cluster_json = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--join")) {
            cfg.join = true;
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
        return error.InvalidArguments;
    }
    return cfg;
}

fn resolvePaths(alloc: std.mem.Allocator, cli: CliConfig, cfg: ?*const antfly.common.config.Config) !ResolvedPaths {
    const base = try antfly.common.config.resolveLocalRoleBaseDir(alloc, cfg, "metadata");
    defer alloc.free(base);

    const replica_root_dir = if (cli.replica_root_dir) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/replicas", .{base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(replica_root_dir);
    const replica_catalog_path = if (cli.replica_catalog_path) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/catalog.txt", .{base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(replica_catalog_path);
    const snapshot_root_dir = if (cli.snapshot_root_dir) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/snapshots", .{base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(snapshot_root_dir);

    return .{
        .replica_root_dir = replica_root_dir,
        .replica_catalog_path = replica_catalog_path,
        .snapshot_root_dir = snapshot_root_dir,
    };
}

fn normalizeResolvedPathAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    if (!std.fs.path.isAbsolute(path)) return try alloc.dupe(u8, path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const resolved_z = std.Io.Dir.realPathFileAbsoluteAlloc(io_impl.io(), path, alloc) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => null,
        else => return err,
    };
    if (resolved_z) |resolved| return resolved[0..resolved.len];

    return try alloc.dupe(u8, path);
}

pub fn resolveListener(bind_host: ?[]const u8, bind_port: ?u16, cfg: ?*const antfly.common.config.Config) ListenerConfig {
    _ = cfg;
    if (bind_host != null or bind_port != null) {
        return .{
            .bind_host = bind_host orelse "127.0.0.1",
            .bind_port = bind_port orelse 0,
        };
    }
    return .{ .bind_host = "127.0.0.1", .bind_port = 0 };
}

fn resolveRaftListener(cli: CliConfig, cfg: ?*const antfly.common.config.Config) ListenerConfig {
    if (cli.raft_host != null or cli.raft_port != null) {
        return .{
            .bind_host = cli.raft_host orelse "127.0.0.1",
            .bind_port = cli.raft_port orelse 0,
        };
    }
    if (cfg) |loaded| {
        if (cli.local_node_id) |node_id| {
            if (metadataClusterPeerUrl(loaded, node_id)) |url| {
                return parseHostPort(url) catch .{ .bind_host = "127.0.0.1", .bind_port = 0 };
            }
        }
    }
    return resolveListener(null, null, cfg);
}

fn resolveAdminListener(
    cli: CliConfig,
    cfg: ?*const antfly.common.config.Config,
    local_node_id: u64,
    fallback_host: []const u8,
) ListenerConfig {
    if (cli.api_host != null or cli.api_port != null) {
        return .{
            .bind_host = cli.api_host orelse fallback_host,
            .bind_port = cli.api_port orelse 0,
        };
    }
    if (cfg) |loaded| {
        if (metadataOrchestrationPeerUrl(loaded, local_node_id)) |url| {
            return parseHostPort(url) catch .{ .bind_host = fallback_host, .bind_port = 0 };
        }
    }
    return .{ .bind_host = fallback_host, .bind_port = 0 };
}

fn shardAllocationReconcilerConfig(cfg: ?*const antfly.common.config.Config) antfly.metadata.reconciler.Reconciler.Config {
    if (cfg) |loaded| {
        return .{
            .max_shard_size_bytes = loaded.shard_allocation.max_shard_size_bytes,
            .min_shard_size_bytes = loaded.shard_allocation.min_shard_size_bytes,
            .min_shards_per_table = loaded.shard_allocation.min_shards_per_table,
            .max_shards_per_table = loaded.shard_allocation.max_shards_per_table,
            .disable_shard_alloc = loaded.shard_allocation.disable_shard_alloc,
            .auto_range_transition_per_table_limit = loaded.shard_allocation.auto_range_transition_per_table_limit,
            .auto_range_transition_cluster_limit = loaded.shard_allocation.auto_range_transition_cluster_limit,
            .shard_cooldown_millis = loaded.shard_allocation.shard_cooldown_millis,
            .min_shard_merge_age_millis = loaded.shard_allocation.min_shard_merge_age_millis,
        };
    }
    return .{};
}

pub fn parseHostPort(base_uri: []const u8) !ListenerConfig {
    const scheme_pos = std.mem.indexOf(u8, base_uri, "://") orelse return error.InvalidArguments;
    const host_port = base_uri[scheme_pos + 3 ..];
    const path_pos = std.mem.indexOfScalar(u8, host_port, '/');
    const authority = if (path_pos) |pos| host_port[0..pos] else host_port;
    const colon_pos = std.mem.lastIndexOfScalar(u8, authority, ':') orelse return error.InvalidArguments;
    const host = authority[0..colon_pos];
    const port = try std.fmt.parseInt(u16, authority[colon_pos + 1 ..], 10);
    if (host.len == 0) return error.InvalidArguments;
    return .{ .bind_host = host, .bind_port = port };
}

fn resolveMetadataClusterPeers(
    alloc: std.mem.Allocator,
    cluster_json: ?[]const u8,
    cfg: ?*const antfly.common.config.Config,
) ![]MetadataClusterPeer {
    if (cluster_json) |raw| return try parseMetadataClusterJson(alloc, raw);
    if (cfg) |loaded| return try metadataClusterPeersFromConfig(alloc, loaded);
    return &.{};
}

pub fn metadataClusterPeersFromConfig(
    alloc: std.mem.Allocator,
    cfg: *const antfly.common.config.Config,
) ![]MetadataClusterPeer {
    if (cfg.metadata.raft_urls.len == 0) return &.{};
    var out = try alloc.alloc(MetadataClusterPeer, cfg.metadata.raft_urls.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |peer| alloc.free(peer.raft_url);
        alloc.free(out);
    }
    for (cfg.metadata.raft_urls, 0..) |entry, index| {
        out[index] = .{
            .node_id = entry.node_id,
            .raft_url = try alloc.dupe(u8, entry.url),
        };
        initialized += 1;
    }
    return out;
}

pub fn metadataClusterPeerUrl(
    cfg: *const antfly.common.config.Config,
    node_id: u64,
) ?[]const u8 {
    for (cfg.metadata.raft_urls) |entry| {
        if (entry.node_id == node_id) return entry.url;
    }
    return null;
}

fn resolveMetadataOrchestrationUrls(
    alloc: std.mem.Allocator,
    cfg: ?*const antfly.common.config.Config,
) ![]antfly.metadata_service.MetadataOrchestrationUrl {
    const loaded = cfg orelse return &.{};
    if (loaded.metadata.orchestration_urls.len == 0) return &.{};
    var out = try alloc.alloc(antfly.metadata_service.MetadataOrchestrationUrl, loaded.metadata.orchestration_urls.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |entry| alloc.free(entry.url);
        alloc.free(out);
    }
    for (loaded.metadata.orchestration_urls, 0..) |entry, index| {
        out[index] = .{
            .node_id = entry.node_id,
            .url = try alloc.dupe(u8, entry.url),
        };
        initialized += 1;
    }
    return out;
}

fn freeMetadataOrchestrationUrls(
    alloc: std.mem.Allocator,
    urls: []antfly.metadata_service.MetadataOrchestrationUrl,
) void {
    for (urls) |entry| alloc.free(entry.url);
    if (urls.len > 0) alloc.free(urls);
}

pub fn metadataOrchestrationPeerUrl(
    cfg: *const antfly.common.config.Config,
    node_id: u64,
) ?[]const u8 {
    for (cfg.metadata.orchestration_urls) |entry| {
        if (entry.node_id == node_id) return entry.url;
    }
    return null;
}

pub fn parseMetadataClusterJson(alloc: std.mem.Allocator, raw: []const u8) ![]MetadataClusterPeer {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidArguments,
    };
    if (object.count() == 0) return &.{};

    var out = try alloc.alloc(MetadataClusterPeer, object.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |peer| alloc.free(peer.raft_url);
        alloc.free(out);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const url = switch (entry.value_ptr.*) {
            .string => |value| value,
            else => return error.InvalidArguments,
        };
        out[initialized] = .{
            .node_id = try parseMetadataNodeId(entry.key_ptr.*),
            .raft_url = try alloc.dupe(u8, url),
        };
        initialized += 1;
    }
    return out;
}

fn parseMetadataNodeId(raw: []const u8) !u64 {
    return std.fmt.parseInt(u64, raw, 10) catch
        std.fmt.parseInt(u64, raw, 16) catch
        error.InvalidArguments;
}

pub fn freeMetadataClusterPeers(alloc: std.mem.Allocator, peers: []MetadataClusterPeer) void {
    for (peers) |peer| alloc.free(peer.raft_url);
    if (peers.len > 0) alloc.free(peers);
}

fn ensureDirPath(io: std.Io, dir_path: []const u8) !void {
    try fs_paths.createDirPathPortable(io, dir_path);
}

fn ensureParent(io: std.Io, file_path: []const u8) !void {
    if (std.fs.path.dirname(file_path)) |parent| {
        var dir = std.Io.Dir.cwd().openDir(io, parent, .{}) catch |err| switch (err) {
            error.FileNotFound => {
                try fs_paths.createDirPathPortable(io, parent);
                return;
            },
            else => return err,
        };
        dir.close(io);
    }
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\Usage: {s} [options]
        \\
        \\Options:
        \\  --config <path>                Common Antfly config file
        \\  --id <node-id>                 Metadata node id (default: 1)
        \\  --raft-host <host>             Metadata raft bind host (default: 127.0.0.1)
        \\  --raft-port <port>             Metadata raft bind port (default: 0)
        \\  --api-host <host>              Metadata admin API bind host (default: raft host)
        \\  --api-port <port>              Metadata admin API bind port (default: 0)
        \\  --cluster <json>               Metadata raft peer URLs, e.g. {{"1":"http://127.0.0.1:9017"}}
        \\  --join                         Join an existing metadata cluster (not yet supported)
        \\  --health <true|false>          Enable health/metrics server (default: true)
        \\  --health-port <port>           Dedicated health/metrics bind port (default: 4200)
        \\  --tick-ms <ms>                 Sleep interval while serving (default: 25)
        \\  --replica-root-dir <path>      Replica root directory
        \\  --replica-catalog-path <path>  Replica catalog file path
        \\  --snapshot-root-dir <path>     Snapshot root directory
        \\  -h, --help                     Show this help
        \\
    , .{argv0});
}

fn parseBoolFlag(raw: []const u8) ?bool {
    if (std.mem.eql(u8, raw, "true")) return true;
    if (std.mem.eql(u8, raw, "false")) return false;
    return null;
}

test "metadata runtime module compiles" {
    _ = run;
    _ = runFromIterator;
}

test "metadata runtime server uses wal replica state backend by default" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-default-wal/replicas", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-default-wal/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-default-wal/snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var server = try Server.init(std.testing.allocator, .{
        .replica_root_dir = replica_root,
        .replica_catalog_path = replica_catalog_path,
        .snapshot_root_dir = snapshot_root,
    });
    defer server.deinit();

    try std.testing.expect(server.server.svc.raft.host.owned_wal_replica_provider != null);
    try std.testing.expect(server.server.svc.raft.host.owned_file_replica_provider == null);
}

test "metadata runtime prefers common config raft url for local id when cli bind is absent" {
    const alloc = std.testing.allocator;
    const raft_urls = try alloc.alloc(antfly.common.config.Config.MetadataConfig.NodeUrl, 1);
    raft_urls[0] = .{ .node_id = 7, .url = try alloc.dupe(u8, "http://127.0.0.1:7011") };
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .speech_to_text = antfly.transcribing.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .metadata = .{
            .raft_urls = raft_urls,
        },
        .storage = .{
            .local_base_dir = try alloc.dupe(u8, "antflydb"),
        },
    };
    defer cfg.deinit();

    const resolved = resolveRaftListener(.{ .local_node_id = 7 }, &cfg);
    try std.testing.expectEqualStrings("127.0.0.1", resolved.bind_host);
    try std.testing.expectEqual(@as(u16, 7011), resolved.bind_port);
}

test "metadata runtime resolves paths from common storage base dir" {
    const alloc = std.testing.allocator;
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .speech_to_text = antfly.transcribing.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .storage = .{
            .local_base_dir = try alloc.dupe(u8, "/tmp/antflydb"),
        },
    };
    defer cfg.deinit();

    const resolved = try resolvePaths(alloc, .{}, &cfg);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("/tmp/antflydb/metadata/replicas", resolved.replica_root_dir);
    try std.testing.expectEqualStrings("/tmp/antflydb/metadata/catalog.txt", resolved.replica_catalog_path);
    try std.testing.expectEqualStrings("/tmp/antflydb/metadata/snapshots", resolved.snapshot_root_dir);
}

test "metadata runtime derives reconciler config from common shard allocation settings" {
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(std.testing.allocator),
        .speech_to_text = antfly.transcribing.Registry.init(std.testing.allocator),
        .text_to_speech = antfly.synthesizing.Registry.init(std.testing.allocator),
        .shard_allocation = .{
            .max_shard_size_bytes = 2048,
            .min_shard_size_bytes = 512,
            .min_shards_per_table = 2,
            .max_shards_per_table = 9,
            .disable_shard_alloc = true,
            .auto_range_transition_per_table_limit = 3,
            .auto_range_transition_cluster_limit = 5,
            .shard_cooldown_millis = 90000,
            .min_shard_merge_age_millis = 180000,
        },
    };
    defer cfg.deinit();

    const derived = shardAllocationReconcilerConfig(&cfg);
    try std.testing.expectEqual(@as(u64, 2048), derived.max_shard_size_bytes);
    try std.testing.expectEqual(@as(u64, 512), derived.min_shard_size_bytes);
    try std.testing.expectEqual(@as(u32, 2), derived.min_shards_per_table);
    try std.testing.expectEqual(@as(u32, 9), derived.max_shards_per_table);
    try std.testing.expect(derived.disable_shard_alloc);
    try std.testing.expectEqual(@as(u32, 3), derived.auto_range_transition_per_table_limit);
    try std.testing.expectEqual(@as(u32, 5), derived.auto_range_transition_cluster_limit);
    try std.testing.expectEqual(@as(u64, 90000), derived.shard_cooldown_millis);
    try std.testing.expectEqual(@as(u64, 180000), derived.min_shard_merge_age_millis);
}

test "metadata runtime preserves projected tables across restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-restart/replicas", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-restart/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-restart/snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    {
        var server = try Server.init(std.testing.allocator, .{
            .local_node_id = 1,
            .metadata_group_id = group_ids.main_metadata_group_id,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
            .snapshot_root_dir = snapshot_root,
        });
        defer server.deinit();
        try server.start();
        try server.bootstrapLocal(group_ids.main_metadata_group_id, 1);

        try server.metadataHttpService().upsertTable(.{
            .table_id = 77,
            .name = "docs",
        });

        var rounds: usize = 0;
        while (rounds < 8) : (rounds += 1) try server.runRound();

        var snapshot = try server.metadataHttpService().adminSnapshot();
        defer server.metadataHttpService().freeAdminSnapshot(&snapshot);
        try std.testing.expectEqual(@as(usize, 1), snapshot.tables.len);
        try std.testing.expectEqualStrings("docs", snapshot.tables[0].name);
    }

    {
        var server = try Server.init(std.testing.allocator, .{
            .local_node_id = 1,
            .metadata_group_id = group_ids.main_metadata_group_id,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
            .snapshot_root_dir = snapshot_root,
        });
        defer server.deinit();
        try server.start();
        try server.bootstrapLocal(group_ids.main_metadata_group_id, 1);

        var rounds: usize = 0;
        while (rounds < 8) : (rounds += 1) try server.runRound();

        var snapshot = try server.metadataHttpService().adminSnapshot();
        defer server.metadataHttpService().freeAdminSnapshot(&snapshot);
        try std.testing.expectEqual(@as(usize, 1), snapshot.tables.len);
        try std.testing.expectEqualStrings("docs", snapshot.tables[0].name);
    }
}

test "metadata runtime bootstrapLocal skips local replica-root reconcile on the bootstrap round" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-bootstrap-skip/replicas", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-bootstrap-skip/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-bootstrap-skip/snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    const HookCtx = struct {
        runs: usize = 0,

        fn run(ptr: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.runs += 1;
        }
    };

    var server = try Server.init(std.testing.allocator, .{
        .local_node_id = 1,
        .metadata_group_id = group_ids.main_metadata_group_id,
        .replica_root_dir = replica_root,
        .replica_catalog_path = replica_catalog_path,
        .snapshot_root_dir = snapshot_root,
        .observe_local_replica_root = true,
    });
    defer server.deinit();
    try server.start();

    var hook_ctx = HookCtx{};
    server.setLocalReplicaRootReconcileHook(.{
        .ptr = &hook_ctx,
        .vtable = &.{ .run = HookCtx.run },
    });

    try server.bootstrapLocal(group_ids.main_metadata_group_id, 1);
    try std.testing.expectEqual(@as(usize, 0), hook_ctx.runs);

    var rounds: usize = 0;
    while (hook_ctx.runs == 0 and rounds < 8) : (rounds += 1) {
        try server.runRound();
    }
    try std.testing.expect(hook_ctx.runs > 0);
}
