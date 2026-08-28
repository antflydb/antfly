// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Production-owned data plane for the deployment-shaped full-cluster VOPR
//! history. Metadata keeps only its quorum replicas; three real DataServers
//! own public HTTP, data-Raft, storage, metadata polling, and status reporting.

const std = @import("std");
const vopr = @import("vopr");
const data_runtime = @import("../data/runtime.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_http_server = @import("../metadata/http_server.zig");
const metadata_http_test_runtime = @import("../metadata/http_test_runtime.zig");
const metadata_sim = @import("../metadata/sim_harness.zig");
const raft_transport = @import("../raft/transport/mod.zig");
const background_runtime = @import("../storage/background_runtime.zig");
const api_http_client = @import("../api/http_client.zig");
const api_http_server = @import("../api/http_server.zig");
const api_distributed_graph = @import("../api/distributed_graph.zig");
const test_contract_helpers = @import("../api/test_contract_helpers.zig");
const common_http = @import("../common/http/http_common.zig");
const io_http_executor = @import("../common/http/io_http_executor.zig");
const api_table_router = @import("../api/table_router.zig");
const hosted_shard_ops = @import("../raft/hosted_shard_ops.zig");
const shard_ops = @import("../raft/shard_ops.zig");
const transition_state = @import("../metadata/transition_state.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const resource_manager = @import("../storage/resource_manager.zig");
const metadata_openapi = @import("antfly_metadata_openapi");

// The composed deployment uses the same service identity as the metadata
// quorum fixture. Leaving this unset does not model an unauthenticated legacy
// cluster: production middleware fails every internal route closed with an
// unmarked 503, which correctly prevents the forwarding client from assuming
// that a mutation was not proposed.
const internal_service_secret = "metadata-simulation-internal-service-secret";
const internal_service_issuer = "metadata-sim";

const ModeledCapacitySource = struct {
    sim: *vopr.vopr_io.VoprIo,
    root: []const u8,
    catalog: []const u8,
    capacity_bytes: u64,
    domain_id: resource_manager.CapacityDomainId,

    fn source(self: *@This()) resource_manager.CapacitySource {
        return .{
            .ptr = self,
            .domain_id = self.domain_id,
            .observe = observe,
        };
    }

    fn observe(raw: *anyopaque) anyerror!resource_manager.CapacityObservation {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const root_bytes = try self.sim.storageBytesUnderPrefix(self.root);
        const catalog_bytes = try self.sim.storageBytesUnderPrefix(self.catalog);
        const used_bytes = root_bytes +| catalog_bytes;
        return .{
            .capacity_bytes = self.capacity_bytes,
            .available_bytes = self.capacity_bytes -| used_bytes,
        };
    }
};

fn parseHttpBaseUriAddress(base_uri: []const u8) !std.Io.net.IpAddress {
    const scheme_end = std.mem.indexOf(u8, base_uri, "://") orelse return error.InvalidProductionDataBaseUri;
    const authority_and_path = base_uri[scheme_end + 3 ..];
    const authority_end = std.mem.indexOfScalar(u8, authority_and_path, '/') orelse authority_and_path.len;
    const authority = authority_and_path[0..authority_end];
    const port_separator = std.mem.lastIndexOfScalar(u8, authority, ':') orelse
        return error.InvalidProductionDataBaseUri;
    var host = authority[0..port_separator];
    if (host.len >= 2 and host[0] == '[' and host[host.len - 1] == ']') host = host[1 .. host.len - 1];
    const port = std.fmt.parseInt(u16, authority[port_separator + 1 ..], 10) catch
        return error.InvalidProductionDataBaseUri;
    return std.Io.net.IpAddress.parse(host, port) catch error.InvalidProductionDataBaseUri;
}

pub const Fixture = struct {
    pub const FaultMode = enum {
        clean,
        graph_transport_failure,
        graph_owner_restart,
        graph_partial_write,
    };

    pub const Phase = enum(u8) {
        created,
        metadata_quorum_ready,
        metadata_http_ready,
        data_servers_ready,
        endpoints_published,
        topology_ready,
        leaders_ready,
        workload_started,
        writes_complete,
        reads_complete,
        graph_query_complete,
        split_requested,
        split_graph_query_started,
        split_finalized,
        split_published,
        post_split_read_complete,
        post_split_graph_query_complete,
        cleanup_complete,
        complete,
    };
    pub const RaftProgress = struct {
        commit_index: u64 = 0,
        applied_index: u64 = 0,
        last_index: u64 = 0,
        leaders: usize = 0,
        peer_routes: usize = 0,
        frames_enqueued: u64 = 0,
        frames_pending: usize = 0,
        frames_failed: u64 = 0,
        frames_sent: u64 = 0,
    };

    const node_count = metadata_sim.VoprPublicClusterFixture.node_count;
    const initial_groups = [_]u64{
        metadata_sim.VoprPublicClusterFixture.data_group_id,
        metadata_sim.VoprPublicClusterFixture.graph_data_group_id,
        metadata_sim.VoprPublicClusterFixture.tenant_data_group_id,
    };
    const split_transition_id: u64 = 6_842_001;
    const split_destination_group_id: u64 = 6_846;
    const split_key = "doc:g";
    const graph_index_name = "graph_idx";
    const left_batch_body =
        \\{"inserts":{"doc:c":{"title":"production-left","_edges":{"graph_idx":{"links":[{"target":"doc:x"}]}}},"doc:k":{"title":"production-split"}},"sync_level":"full_index"}
    ;
    const right_batch_body =
        \\{"inserts":{"doc:x":{"title":"production-right","_edges":{"graph_idx":{"links":[{"target":"doc:k"}]}}}},"sync_level":"full_index"}
    ;
    const ordinary_left_batch_body =
        "{\"inserts\":{\"doc:c\":{\"title\":\"production-left\"},\"doc:k\":{\"title\":\"production-split\"}},\"sync_level\":\"write\"}";
    const ordinary_right_batch_body =
        "{\"inserts\":{\"doc:x\":{\"title\":\"production-right\"}},\"sync_level\":\"write\"}";
    const DataServer = data_runtime.DataServer;

    alloc: std.mem.Allocator,
    sim: *vopr.vopr_io.VoprIo,
    metadata: ?*metadata_sim.VoprPublicClusterFixture = null,
    metadata_sources: [node_count]metadata_sim.MetadataAdminSimSource = undefined,
    metadata_servers: [node_count]metadata_http_server.MetadataHttpServer = undefined,
    metadata_server_count: usize = 0,
    metadata_listeners: [node_count]metadata_http_test_runtime.Runtime = undefined,
    metadata_listener_count: usize = 0,
    metadata_base_uris: [node_count][]u8 = undefined,
    metadata_uri_count: usize = 0,
    executor: io_http_executor.IoHttpExecutor = undefined,
    executor_live: bool = false,
    raft_executor: io_http_executor.IoHttpExecutor = undefined,
    raft_executor_live: bool = false,
    public_executor: io_http_executor.IoHttpExecutor = undefined,
    public_executor_live: bool = false,
    transition_executor: io_http_executor.IoHttpExecutor = undefined,
    transition_executor_live: bool = false,
    backend_runtimes: [node_count]background_runtime.BackendRuntimeHandle = undefined,
    backend_runtime_count: usize = 0,
    backend_runtime_owners_started: usize = 0,
    data_roots: [node_count][]u8 = undefined,
    data_root_count: usize = 0,
    capacity_sources: [node_count]ModeledCapacitySource = undefined,
    data_catalogs: [node_count][]u8 = undefined,
    data_catalog_count: usize = 0,
    data_servers: [node_count]DataServer = undefined,
    data_server_count: usize = 0,
    data_server_live: [node_count]bool = .{false} ** node_count,
    data_raft_listeners: [node_count]raft_transport.HttpxRuntime = undefined,
    data_raft_listener_count: usize = 0,
    data_raft_listener_live: [node_count]bool = .{false} ** node_count,
    data_api_uris: [node_count][]u8 = undefined,
    data_api_uri_count: usize = 0,
    data_api_uri_live: [node_count]bool = .{false} ** node_count,
    data_api_ports: [node_count]u16 = .{0} ** node_count,
    data_raft_uris: [node_count][]u8 = undefined,
    data_raft_uri_count: usize = 0,
    data_raft_uri_live: [node_count]bool = .{false} ** node_count,
    data_raft_ports: [node_count]u16 = .{0} ** node_count,
    transition_routers: [node_count]api_table_router.CatalogBackedGroupRouter = undefined,
    transition_adapters: [node_count]hosted_shard_ops.HostedShardOperationAdapter = undefined,
    transition_registrations: [node_count]?shard_ops.OwnedShardOperationAdapter.Registration = .{null} ** node_count,
    transition_registration_count: usize = 0,
    client: api_http_client.ApiHttpClient = undefined,
    driver_future: ?std.Io.Future(void) = null,
    raft_driver_futures: [node_count]?std.Io.Future(void) = .{null} ** node_count,
    workload_future: ?std.Io.Future(void) = null,
    driver_stop: bool = false,
    control_driver_stop: bool = false,
    driver_done: bool = false,
    raft_driver_done: [node_count]bool = .{false} ** node_count,
    raft_driver_active: [node_count]bool = .{false} ** node_count,
    data_server_paused: [node_count]bool = .{false} ** node_count,
    driver_failure: ?anyerror = null,
    driver_rounds: u64 = 0,
    metadata_recovery_campaigns: u64 = 0,
    raft_driver_rounds: [node_count]u64 = .{0} ** node_count,
    control_requests: std.Io.Semaphore = .{},
    control_completions: std.Io.Semaphore = .{},
    control_round_active: bool = false,
    public_request_ingress_count: u64 = 0,
    write_statuses: [3]u16 = .{ 0, 0, 0 },
    write_body_digests: [3]u64 = .{ 0, 0, 0 },
    write_attempts: [3]u64 = .{ 0, 0, 0 },
    write_outcome_unknowns: [3]u64 = .{ 0, 0, 0 },
    request_lifecycle_counts: [std.meta.fields(data_runtime.DataRequestLifecyclePhase).len]u64 =
        .{0} ** std.meta.fields(data_runtime.DataRequestLifecyclePhase).len,
    last_request_lifecycle_group: u64 = 0,
    last_request_lifecycle_index: u64 = 0,
    last_request_lifecycle_phase: data_runtime.DataRequestLifecyclePhase = .routing_started,
    read_statuses: [4]u16 = .{ 0, 0, 0, 0 },
    read_body_digests: [4]u64 = .{ 0, 0, 0, 0 },
    read_attempts: [4]u16 = .{ 0, 0, 0, 0 },
    workload_done: bool = false,
    write_sound: bool = false,
    read_sound: bool = false,
    tenant_sound: bool = false,
    graph_sound: bool = false,
    split_graph_inflight_started: bool = false,
    split_graph_inflight_complete: bool = false,
    split_graph_inflight_rejected: bool = false,
    split_graph_inflight_sound: bool = false,
    post_split_graph_sound: bool = false,
    graph_transport_failure_injected: bool = false,
    graph_transport_failure_observed: bool = false,
    graph_transport_failure_error_code: u16 = 0,
    graph_partial_rejected_sound: bool = false,
    graph_transport_fault_armed: bool = false,
    graph_transport_fault_endpoint: ?std.Io.net.IpAddress = null,
    graph_partial_write_injected: bool = false,
    graph_partial_write_observed: bool = false,
    graph_partial_write_count_before: u64 = 0,
    graph_partial_write_target_index: usize = 0,
    graph_partial_write_target_configured: bool = false,
    graph_restart_requested: std.Io.Semaphore = .{},
    graph_restart_down: std.Io.Semaphore = .{},
    graph_restart_recover: std.Io.Semaphore = .{},
    graph_restart_recovered: std.Io.Semaphore = .{},
    graph_restart_future: ?std.Io.Future(void) = null,
    graph_restart_target_index: usize = 0,
    graph_restart_target_configured: bool = false,
    graph_owner_restart_requested: bool = false,
    graph_owner_restart_down: bool = false,
    graph_owner_restart_failure_observed: bool = false,
    graph_owner_restart_recovered: bool = false,
    graph_owner_restart_error_code: u16 = 0,
    graph_owner_restart_failure: ?anyerror = null,
    graph_probe_route_index: usize = 1,
    topology_sound: bool = false,
    split_sound: bool = false,
    split_finalized: bool = false,
    split_published: bool = false,
    cleanup_started: bool = false,
    cleanup_sound: bool = false,
    complete: bool = false,
    failure: ?anyerror = null,
    final_resource_usage: [node_count]vopr.deployment.ResourceUsage = .{ .{}, .{}, .{} },
    final_raft_wire_requests: u64 = 0,
    phase: Phase = .created,
    teardown_started: bool = false,
    active_split_enabled: bool = true,
    graph_enabled: bool = false,
    fault_mode: FaultMode = .clean,

    pub fn create(alloc: std.mem.Allocator, sim: *vopr.vopr_io.VoprIo) !*Fixture {
        const self = try alloc.create(Fixture);
        self.* = .{ .alloc = alloc, .sim = sim };
        return self;
    }

    pub fn init(alloc: std.mem.Allocator, sim: *vopr.vopr_io.VoprIo) !*Fixture {
        const self = try create(alloc, sim);
        errdefer self.deinit();
        try self.bootstrap();
        return self;
    }

    pub fn setActiveSplitEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.active_split_enabled = enabled;
    }

    pub fn setGraphEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.graph_enabled = enabled;
    }

    pub fn setFaultMode(self: *Fixture, mode: FaultMode) void {
        std.debug.assert(self.phase == .created);
        self.fault_mode = mode;
    }

    pub fn currentGraphOwnerIndex(self: *Fixture) ?usize {
        return self.currentDataLeaderIndex(metadata_sim.VoprPublicClusterFixture.graph_data_group_id);
    }

    pub fn configureGraphRestartTarget(self: *Fixture, index: usize) !void {
        if (self.phase != .leaders_ready or index >= self.data_server_count or !self.data_server_live[index])
            return error.InvalidProductionGraphRestartTarget;
        self.graph_restart_target_index = index;
        self.graph_restart_target_configured = true;
    }

    /// Freeze the advertised link selected by the deployment manifest before
    /// workload start. The active history later fails closed if leadership
    /// changes would make that registered fault scope inaccurate.
    pub fn configureGraphPartialWriteTarget(self: *Fixture, target_index: usize) !usize {
        if (self.phase != .leaders_ready or target_index >= self.data_server_count or !self.data_server_live[target_index])
            return error.InvalidProductionGraphPartialWriteTarget;
        const start_index = self.currentDataLeaderIndex(metadata_sim.VoprPublicClusterFixture.data_group_id) orelse
            return error.ProductionDataGraphLeaderMissing;
        const coordinator_index = for (0..self.data_api_uri_count) |index| {
            if (index != start_index and index != target_index) break index;
        } else return error.ProductionDataGraphRemoteCoordinatorMissing;
        self.graph_partial_write_target_index = target_index;
        self.graph_partial_write_target_configured = true;
        self.graph_probe_route_index = coordinator_index;
        return coordinator_index;
    }

    pub fn bootstrap(self: *Fixture) !void {
        if (self.phase != .created) return error.ProductionFixtureAlreadyBootstrapped;
        if (self.fault_mode != .clean and (!self.graph_enabled or !self.active_split_enabled))
            return error.InvalidProductionClusterFaultMode;
        const alloc = self.alloc;
        const sim = self.sim;
        self.metadata = try metadata_sim.VoprPublicClusterFixture.create(alloc, sim);
        try self.metadata.?.bootstrapExternalDataPlane();
        try self.ensureMetadataIncarnation();
        self.phase = .metadata_quorum_ready;
        std.log.debug("production data-plane VOPR initialized metadata-only quorum", .{});

        for (0..node_count) |index| {
            self.metadata_sources[index] = .{ .node = self.metadata.?.cluster.node(index) };
            self.metadata_servers[index] = metadata_http_server.MetadataHttpServer.init(
                alloc,
                .{},
                self.metadata_sources[index].iface(),
            );
            self.metadata_server_count += 1;
            self.metadata_listeners[index] = try metadata_http_test_runtime.Runtime.startShared(
                alloc,
                self.sim.io(),
                &self.metadata_servers[index],
            );
            self.metadata_listener_count += 1;
            self.metadata_base_uris[index] = try self.metadata_listeners[index].baseUri(alloc);
            self.metadata_uri_count += 1;
        }

        self.executor = io_http_executor.IoHttpExecutor.init(alloc, self.sim.io(), .{
            .keep_alive = true,
            .connect_timeout_ms = 0,
            .read_timeout_ms = 0,
            .write_timeout_ms = 0,
            .pool_max_connections = 48,
            .pool_max_per_host = 16,
        });
        self.executor_live = true;
        self.raft_executor = io_http_executor.IoHttpExecutor.init(alloc, self.sim.io(), .{
            .keep_alive = true,
            .connect_timeout_ms = 0,
            .read_timeout_ms = 0,
            .write_timeout_ms = 0,
            .pool_max_connections = 32,
            .pool_max_per_host = 16,
        });
        self.raft_executor_live = true;
        self.public_executor = io_http_executor.IoHttpExecutor.init(alloc, self.sim.io(), .{
            .keep_alive = true,
            .connect_timeout_ms = 0,
            .read_timeout_ms = 0,
            .write_timeout_ms = 0,
            .pool_max_connections = 16,
            .pool_max_per_host = 8,
        });
        self.public_executor_live = true;
        // Hosted structural-operation polling is a control-plane protocol,
        // not the keep-alive subject of this deployment history. Give it a
        // dedicated non-pooled client so connection lifetime cannot couple
        // repeated observation requests to unrelated public traffic. HTTP/1
        // pooling remains exercised by the public, metadata, Raft, and focused
        // HTTP lifecycle VOPR suites.
        self.transition_executor = io_http_executor.IoHttpExecutor.init(alloc, self.sim.io(), .{
            .keep_alive = false,
            .connect_timeout_ms = 0,
            .read_timeout_ms = 0,
            .write_timeout_ms = 0,
        });
        self.transition_executor_live = true;
        for (self.metadata_base_uris[0..self.metadata_uri_count]) |uri|
            try self.waitForHttpListener(self.executor.executor(), uri);
        self.phase = .metadata_http_ready;
        std.log.debug("production data-plane VOPR started metadata HTTP listeners", .{});

        for (0..node_count) |index| {
            self.backend_runtimes[index] = try background_runtime.BackendRuntimeHandle.init(alloc, .{
                .backend = .manual,
                .borrowed_io = .{ .general = self.sim.io() },
            });
            self.backend_runtime_count += 1;
            self.backend_runtime_owners_started += 1;
            self.data_roots[index] = try std.fmt.allocPrint(
                alloc,
                ".zig-cache/tmp/{s}/production-data-{d}",
                .{ self.metadata.?.tmp.sub_path, index + 1 },
            );
            self.data_root_count += 1;
            self.data_catalogs[index] = try std.fmt.allocPrint(
                alloc,
                ".zig-cache/tmp/{s}/production-data-{d}.catalog",
                .{ self.metadata.?.tmp.sub_path, index + 1 },
            );
            self.data_catalog_count += 1;
            self.capacity_sources[index] = .{
                .sim = self.sim,
                .root = self.data_roots[index],
                .catalog = self.data_catalogs[index],
                .capacity_bytes = 64 * 1024 * 1024,
                .domain_id = @as(u128, vopr.id.derive(
                    "full-cluster.production-capacity-domain",
                    vopr.id.stable("full-cluster", "production-data"),
                    index + 1,
                )),
            };
            try self.initializeDataServer(index);
            self.data_server_count += 1;
            self.data_raft_listener_count += 1;
            self.data_raft_uri_count += 1;
            self.data_api_uri_count += 1;
        }
        for (self.data_api_uris[0..self.data_api_uri_count]) |uri|
            try self.waitForHttpListener(self.public_executor.executor(), uri);
        self.phase = .data_servers_ready;
        std.log.debug("production data-plane VOPR started DataServer HTTP/Raft listeners", .{});

        for (0..node_count) |index| try self.publishDataServerEndpoint(index);
        try self.waitForPublishedDataServerEndpoints();
        self.phase = .endpoints_published;
        std.log.debug("production data-plane VOPR published DataServer endpoints", .{});
        try self.waitForDataRaftTopology();
        self.phase = .topology_ready;
        try self.waitForInitialDataLeaders();
        self.phase = .leaders_ready;
        try self.installExternalTransitionRouting();
        self.client = api_http_client.ApiHttpClient.init(alloc, self.public_executor.executor());
    }

    fn initializeDataServer(self: *Fixture, index: usize) !void {
        std.debug.assert(index < node_count);
        std.debug.assert(!self.data_server_live[index]);
        std.debug.assert(!self.data_raft_listener_live[index]);
        const request_executors = [_]@TypeOf(self.executor.executor()){
            self.executor.executor(),
            self.executor.executor(),
            self.executor.executor(),
        };
        self.data_servers[index] = try DataServer.initFromMetadataApiUrls(self.alloc, .{
            .bind_port = self.data_api_ports[index],
            .replica_root_dir = self.data_roots[index],
            .replica_catalog_path = self.data_catalogs[index],
            .data_raft_state_backend = .file_image,
            .store_registration = .{
                .node_id = index + 1,
                .store_id = index + 1,
                .api_url = "",
                .raft_url = "",
                .failure_domain = if (index == 0) "rack-a" else if (index == 1) "rack-b" else "rack-c",
            },
            .process_memory_limit_bytes = 512 * 1024 * 1024,
            .capacity_source = self.capacity_sources[index].source(),
            .backend_runtime = self.backend_runtimes[index].ptr(),
            .api_server_cfg = .{
                .internal_service_secret = internal_service_secret,
                .internal_service_issuer = internal_service_issuer,
                .request_lifecycle_hook = .{
                    .ptr = self,
                    .reach_fn = observePublicRequestLifecycle,
                },
            },
            .metadata_request_executors = &request_executors,
            .data_raft_request_executor = self.raft_executor.executor(),
            .data_request_lifecycle_hook = .{
                .ptr = self,
                .reach_fn = observeDataRequestLifecycle,
            },
            .data_raft_listener_external = true,
            // Keep the production async Raft delivery owner active. Its
            // borrowed std.Io is VoprIo-backed, so the sender itself is a
            // deterministic fiber and multi-replica commits traverse the same
            // production queue/retry path as a native deployment.
            .data_raft_async_send_worker_count = 1,
        }, &self.metadata_base_uris);
        self.data_server_live[index] = true;
        errdefer {
            self.data_servers[index].deinit();
            self.data_server_live[index] = false;
        }
        _ = self.data_servers[index].read_source.withDistributedGraphLifecycleHook(.{
            .ptr = self,
            .reach_fn = observeDistributedGraphLifecycle,
        });
        const data_raft = self.data_servers[index].data_raft orelse return error.MissingDataRaft;
        self.data_raft_listeners[index] = try raft_transport.HttpxRuntime.startAt(
            self.alloc,
            self.sim.io(),
            data_raft.host.http_host.server.executor(),
            "127.0.0.1",
            self.data_raft_ports[index],
        );
        self.data_raft_listener_live[index] = true;
        errdefer {
            self.data_raft_listeners[index].deinit();
            self.data_raft_listener_live[index] = false;
        }
        self.data_raft_uris[index] = try self.alloc.dupe(u8, self.data_raft_listeners[index].base_uri);
        self.data_raft_uri_live[index] = true;
        self.data_raft_ports[index] = (try parseHttpBaseUriAddress(self.data_raft_uris[index])).getPort();
        errdefer {
            self.alloc.free(self.data_raft_uris[index]);
            self.data_raft_uri_live[index] = false;
        }
        try self.data_servers[index].startPublicHttp();
        self.data_api_uris[index] = try self.data_servers[index].baseUri(self.alloc);
        self.data_api_uri_live[index] = true;
        self.data_api_ports[index] = (try parseHttpBaseUriAddress(self.data_api_uris[index])).getPort();
    }

    fn installExternalTransitionRouting(self: *Fixture) !void {
        for (0..node_count) |index| {
            self.transition_routers[index] = api_table_router.CatalogBackedGroupRouter.init(
                try self.metadata.?.externalCatalogSource(index),
                0,
            );
            self.transition_adapters[index] = hosted_shard_ops.HostedShardOperationAdapter.init(
                self.alloc,
                try self.metadata.?.externalCatalogSource(index),
                self.transition_routers[index].router(),
                self.transition_executor.executor(),
                .{ .ptr = self, .readiness = externalTransitionReadiness },
                null,
            );
            _ = self.transition_adapters[index].withInternalServiceAuth(
                internal_service_secret,
                internal_service_issuer,
            );
            self.transition_registrations[index] = try self.metadata.?.replaceExternalTransitionOps(
                index,
                self.transition_adapters[index].adapter(),
            );
            self.transition_registration_count += 1;
        }
    }

    fn externalTransitionReadiness(
        ptr: *anyopaque,
        group_id: u64,
    ) !transition_state.StablePlacementReadiness {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        var replicas: usize = 0;
        var leader_known = false;
        for (&self.data_servers, 0..) |*server, index| {
            if (!self.data_server_live[index]) continue;
            const raft = server.data_raft orelse continue;
            const status = raft.host.http_host.host.raftStatus(group_id) orelse continue;
            replicas += 1;
            leader_known = leader_known or status.soft.role == .leader;
        }
        if (replicas == 0) return .status_unavailable;
        if (!leader_known) return .leader_unknown;
        if (replicas != 2) return .voter_count_mismatch;
        return .ready;
    }

    fn observeDataRequestLifecycle(
        raw: *anyopaque,
        event: data_runtime.DataRequestLifecycleEvent,
    ) anyerror!void {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        self.request_lifecycle_counts[@intFromEnum(event.phase)] +|= 1;
        self.last_request_lifecycle_group = event.group_id;
        self.last_request_lifecycle_index = event.log_index;
        self.last_request_lifecycle_phase = event.phase;
    }

    fn observePublicRequestLifecycle(
        ptr: *anyopaque,
        event: api_http_server.RequestLifecycleEvent,
    ) !void {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        if (event.phase == .ingress) self.public_request_ingress_count +|= 1;
    }

    fn observeDistributedGraphLifecycle(
        ptr: *anyopaque,
        event: api_distributed_graph.LifecycleEvent,
    ) void {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        if (self.fault_mode == .clean) return;
        switch (event.phase) {
            .expand_round_completed => {
                if (!self.graph_transport_fault_armed or event.depth != 1) return;
                switch (self.fault_mode) {
                    .clean => unreachable,
                    .graph_transport_failure => {
                        if (self.graph_transport_failure_injected) return;
                        self.graph_transport_fault_armed = false;
                        const endpoint = self.graph_transport_fault_endpoint orelse return;
                        self.sim.setOutboundEndpointPayloadOutage(endpoint, "/graph-expand") catch unreachable;
                        self.graph_transport_failure_injected = true;
                    },
                    .graph_owner_restart => {
                        if (self.graph_owner_restart_requested) return;
                        self.graph_transport_fault_armed = false;
                        self.graph_owner_restart_requested = true;
                        self.graph_restart_requested.post(self.sim.io());
                        self.graph_restart_down.wait(self.sim.io()) catch return;
                    },
                    .graph_partial_write => {
                        if (self.graph_partial_write_injected) return;
                        self.graph_transport_fault_armed = false;
                        const endpoint = self.graph_transport_fault_endpoint orelse return;
                        self.graph_partial_write_count_before = self.sim.outboundEndpointPayloadPartialWriteCount();
                        self.sim.setOutboundEndpointPayloadPartialWrite(endpoint, "/graph-expand", 1) catch unreachable;
                        self.graph_partial_write_injected = true;
                    },
                }
            },
            .attempt_failed => {
                switch (self.fault_mode) {
                    .clean => unreachable,
                    .graph_transport_failure => {
                        if (!self.graph_transport_failure_injected or self.graph_transport_failure_observed) return;
                        self.graph_transport_failure_observed = true;
                        self.graph_transport_failure_error_code = event.error_code;
                        // Heal before the coordinator converts the failed
                        // attempt into the public error response. This cuts the
                        // inter-owner fanout without making the client response
                        // itself undeliverable.
                        self.sim.setOutboundEndpointOutage(null);
                    },
                    .graph_owner_restart => {
                        if (!self.graph_owner_restart_down or self.graph_owner_restart_failure_observed) return;
                        self.graph_owner_restart_failure_observed = true;
                        self.graph_owner_restart_error_code = event.error_code;
                        self.graph_restart_recover.post(self.sim.io());
                        self.graph_restart_recovered.wait(self.sim.io()) catch return;
                    },
                    .graph_partial_write => {},
                }
            },
            else => {},
        }
    }

    fn ensureMetadataIncarnation(self: *Fixture) !void {
        const leader_index = self.metadata.?.cluster.currentMetadataLeaderIndex() orelse
            return error.MetadataLeaderUnavailable;
        var status = try self.metadata.?.cluster.node(leader_index).metadataStatus();
        if (status.metadata_incarnation == null) {
            const incarnation: metadata_api.MetadataClusterIncarnation =
                "33333333333333333333333333333333".*;
            try self.metadata.?.cluster.node(leader_index).proposeTransitionCommand(.{
                .initialize_metadata_incarnation = incarnation,
            });
        }
        for (0..64) |_| {
            try self.metadata.?.cluster.stepAll();
            var all_ready = true;
            for (0..node_count) |index| {
                status = try self.metadata.?.cluster.node(index).metadataStatus();
                if (status.metadata_incarnation == null) {
                    all_ready = false;
                    break;
                }
            }
            if (all_ready) return;
        }
        return error.MetadataIncarnationUnavailable;
    }

    fn waitForHttpListener(
        self: *Fixture,
        executor: common_http.RequestExecutor,
        uri: []const u8,
    ) !void {
        for (0..256) |_| {
            var response = executor.execute(self.alloc, .{
                .method = .GET,
                .uri = uri,
            }) catch |err| switch (err) {
                error.ConnectionRefused,
                error.ConnectionResetByPeer,
                error.EndOfStream,
                => {
                    try self.sim.io().sleep(.fromMilliseconds(1), .awake);
                    continue;
                },
                else => return err,
            };
            response.deinit(self.alloc);
            return;
        }
        return error.ProductionHttpListenerStartupTimeout;
    }

    fn publishDataServerEndpoint(self: *Fixture, index: usize) !void {
        for (0..128) |round| {
            const leader_index = self.metadata.?.cluster.currentMetadataLeaderIndex() orelse {
                try self.recoverMetadataLeadership(round);
                try self.metadata.?.cluster.stepAll();
                continue;
            };
            self.metadata.?.cluster.node(leader_index).upsertStore(.{
                .store_id = index + 1,
                .node_id = index + 1,
                .api_url = self.data_api_uris[index],
                .raft_url = self.data_raft_uris[index],
                .role = "data",
                .health_class = "healthy",
                .failure_domain = if (index == 0) "rack-a" else if (index == 1) "rack-b" else "rack-c",
                .live = true,
            }) catch |err| switch (err) {
                error.NotLeader => {
                    try self.recoverMetadataLeadership(round);
                    try self.metadata.?.cluster.stepAll();
                    continue;
                },
                else => return err,
            };
            try self.data_servers[index].acceptAuthoritativeStoreRegistration(index + 1, index + 1);
            return;
        }
        return error.ProductionDataServerEndpointPublicationTimeout;
    }

    fn waitForPublishedDataServerEndpoints(self: *Fixture) !void {
        for (0..128) |round| {
            try self.metadata.?.cluster.stepAll();
            try self.recoverMetadataLeadership(round);
            var all_published = true;
            for (0..node_count) |node_index| {
                var snapshot = try self.metadata.?.cluster.node(node_index).adminSnapshot();
                defer self.metadata.?.cluster.node(node_index).freeAdminSnapshot(&snapshot);
                for (0..node_count) |store_index| {
                    var found = false;
                    for (snapshot.stores) |store| {
                        if (store.store_id != store_index + 1 or store.node_id != store_index + 1) continue;
                        if (!std.mem.eql(u8, store.api_url, self.data_api_uris[store_index]) or
                            !std.mem.eql(u8, store.raft_url, self.data_raft_uris[store_index])) continue;
                        found = true;
                        break;
                    }
                    if (!found) {
                        all_published = false;
                        break;
                    }
                }
                if (!all_published) break;
            }
            if (all_published) return;
        }
        return error.ProductionDataServerEndpointPublicationTimeout;
    }

    fn recoverMetadataLeadership(self: *Fixture, round: usize) !void {
        if (self.metadata.?.cluster.currentMetadataLeaderIndex() != null or round % 8 != 7) return;
        const campaign_index = (round / 8) % node_count;
        try self.metadata.?.cluster.node(campaign_index).campaignMetadataGroup();
        self.metadata_recovery_campaigns +|= 1;
    }

    fn waitForDataRaftTopology(self: *Fixture) !void {
        for (0..32) |_| {
            for (&self.data_servers, 0..) |*server, index| {
                if (!self.data_server_live[index]) continue;
                server.runControlRoundOnly() catch |err| switch (err) {
                    error.LsmRootWriterAlreadyOpen,
                    error.WriterLocked,
                    error.PersistentDescriptorAdmissionExhausted,
                    => {},
                    else => return err,
                };
            }

            var hosted_replicas: usize = 0;
            for (initial_groups) |group_id| {
                var group_replicas: usize = 0;
                for (&self.data_servers, 0..) |*server, index| {
                    if (!self.data_server_live[index]) continue;
                    const raft = server.data_raft orelse continue;
                    if (raft.host.http_host.host.raftStatus(group_id) != null)
                        group_replicas += 1;
                }
                if (group_replicas == 2) hosted_replicas += group_replicas;
            }
            if (hosted_replicas == initial_groups.len * 2) return;

            try self.metadata.?.cluster.stepAll();
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        }
        return error.ProductionDataRaftTopologyTimeout;
    }

    fn waitForInitialDataLeaders(self: *Fixture) !void {
        for (0..512) |_| {
            for (&self.data_servers, 0..) |*server, index| {
                if (!self.data_server_live[index]) continue;
                try server.runRaftRoundOnly();
            }

            var leaders: usize = 0;
            for (initial_groups) |group_id| {
                for (&self.data_servers, 0..) |*server, index| {
                    if (!self.data_server_live[index]) continue;
                    const raft = server.data_raft orelse continue;
                    const status = raft.host.http_host.host.raftStatus(group_id) orelse continue;
                    if (status.soft.role == .leader) {
                        leaders += 1;
                        break;
                    }
                }
            }
            if (leaders == initial_groups.len) return;
            // HttpFrameDriver delivers Raft frames on its production async
            // sender owners. Yield between ticks so those VoprIo fibers can
            // drain the queues before the next convergence observation.
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        }
        return error.ProductionDataLeaderTimeout;
    }

    fn driveControl(self: *Fixture) void {
        defer self.driver_done = true;
        while (!self.driver_stop and !self.control_driver_stop) {
            self.control_requests.wait(self.sim.io()) catch |err| {
                if (self.control_driver_stop or (err == error.Canceled and self.driver_stop)) return;
                self.driver_failure = err;
                self.driver_stop = true;
                return;
            };
            if (self.driver_stop or self.control_driver_stop) return;
            self.runControlCycle() catch |err| {
                self.driver_failure = err;
                self.driver_stop = true;
                // A requester must always leave its completion wait before it
                // can observe and propagate the driver failure.
                self.control_completions.post(self.sim.io());
                return;
            };
            self.driver_rounds +|= 1;
            // Publish one scheduler-visible completion for exactly one
            // requested round. Raft tickers remain independent tasks, but the
            // workload cannot race an unbounded next control round against its
            // finalized-state observation.
            self.control_completions.post(self.sim.io());
        }
    }

    fn runControlCycle(self: *Fixture) !void {
        std.debug.assert(!self.control_round_active);
        self.control_round_active = true;
        defer self.control_round_active = false;
        try self.runDataControlRound();
        try self.metadata.?.cluster.stepAll();
        if (self.metadata.?.cluster.currentMetadataLeaderIndex() == null and self.driver_rounds % 8 == 7) {
            // The metadata simulation intentionally uses deterministic timers,
            // so a long data-plane outage can align every healthy candidate.
            // A production deployment gets the equivalent symmetry break from
            // randomized election timeouts. Campaign one rotating healthy
            // replica as a real Raft input; never fabricate leader state.
            try self.recoverMetadataLeadership(@intCast(self.driver_rounds));
        }
    }

    fn stopControlDriver(self: *Fixture) void {
        self.control_driver_stop = true;
        if (self.driver_future) |*future| {
            self.control_requests.post(self.sim.io());
            future.await(self.sim.io());
            self.driver_future = null;
        }
    }

    fn runOneControlRound(self: *Fixture) !void {
        if (self.driver_failure) |err| return err;
        if (self.driver_stop or self.control_driver_stop) return error.ProductionDataControlDriverStopped;
        self.control_requests.post(self.sim.io());
        try self.control_completions.wait(self.sim.io());
        if (self.driver_failure) |err| return err;
        // Preserve the production loop's one-millisecond control cadence and
        // give the independently owned Raft and HTTP tasks a deterministic
        // scheduling boundary before the next status observation.
        try self.sim.io().sleep(.fromMilliseconds(1), .awake);
    }

    fn driveRaft(self: *Fixture, index: usize) void {
        defer self.raft_driver_done[index] = true;
        while (!self.driver_stop) {
            if (self.data_server_paused[index] or !self.data_server_live[index]) {
                self.sim.io().sleep(.fromMilliseconds(1), .awake) catch |err| {
                    if (err == error.Canceled and self.driver_stop) return;
                    self.driver_failure = err;
                    self.driver_stop = true;
                    return;
                };
                continue;
            }
            self.raft_driver_active[index] = true;
            self.data_servers[index].runRaftRoundOnly() catch |err| {
                self.raft_driver_active[index] = false;
                self.driver_failure = err;
                self.driver_stop = true;
                return;
            };
            self.raft_driver_active[index] = false;
            self.raft_driver_rounds[index] +|= 1;
            self.sim.io().sleep(.fromMilliseconds(1), .awake) catch |err| {
                if (err == error.Canceled and self.driver_stop) return;
                self.driver_failure = err;
                self.driver_stop = true;
                return;
            };
        }
    }

    fn runDataControlRound(self: *Fixture) !void {
        for (&self.data_servers, 0..) |*server, index| {
            if (self.data_server_paused[index] or !self.data_server_live[index]) continue;
            server.runControlRoundOnly() catch |err| switch (err) {
                error.LsmRootWriterAlreadyOpen,
                error.WriterLocked,
                error.PersistentDescriptorAdmissionExhausted,
                => {},
                else => return err,
            };
        }
    }

    fn driveGraphOwnerRestart(self: *Fixture) void {
        self.graph_restart_requested.wait(self.sim.io()) catch return;
        if (self.driver_stop or self.teardown_started) return;
        self.stopDataServerForRestart(self.graph_restart_target_index) catch |err| {
            self.failGraphOwnerRestart(err);
            return;
        };
        self.graph_owner_restart_down = true;
        self.graph_restart_down.post(self.sim.io());

        self.graph_restart_recover.wait(self.sim.io()) catch return;
        if (self.teardown_started) return;
        self.restartDataServer(self.graph_restart_target_index) catch |err| {
            self.failGraphOwnerRestart(err);
            return;
        };
        self.graph_owner_restart_recovered = true;
        self.graph_restart_recovered.post(self.sim.io());
    }

    fn failGraphOwnerRestart(self: *Fixture, err: anyerror) void {
        self.graph_owner_restart_failure = err;
        self.driver_failure = err;
        self.graph_restart_down.post(self.sim.io());
        self.graph_restart_recovered.post(self.sim.io());
    }

    fn stopDataServerForRestart(self: *Fixture, index: usize) !void {
        if (index >= self.data_server_count or !self.data_server_live[index])
            return error.ProductionDataRestartTargetUnavailable;
        self.data_server_paused[index] = true;
        while (self.raft_driver_active[index] or self.control_round_active) {
            if (self.driver_failure) |err| return err;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        }

        // Publish stop to both listener owners before joining either. The
        // public listener is DataServer-owned; the Raft listener is external
        // so its handler may continue borrowing the server until joined.
        self.data_server_live[index] = false;
        self.data_raft_listener_live[index] = false;
        self.data_servers[index].beginTeardown();
        self.data_raft_listeners[index].requestStop();
        self.data_servers[index].quiesceBackgroundWork();
        self.data_raft_listeners[index].deinit();
        self.data_servers[index].deinit();
        self.alloc.free(self.data_api_uris[index]);
        self.data_api_uri_live[index] = false;
        self.alloc.free(self.data_raft_uris[index]);
        self.data_raft_uri_live[index] = false;
    }

    fn restartDataServer(self: *Fixture, index: usize) !void {
        try self.initializeDataServer(index);
        errdefer {
            if (self.data_raft_listener_live[index]) {
                self.data_raft_listeners[index].deinit();
                self.data_raft_listener_live[index] = false;
            }
            if (self.data_server_live[index]) {
                self.data_servers[index].deinit();
                self.data_server_live[index] = false;
            }
            if (self.data_api_uri_live[index]) {
                self.alloc.free(self.data_api_uris[index]);
                self.data_api_uri_live[index] = false;
            }
            if (self.data_raft_uri_live[index]) {
                self.alloc.free(self.data_raft_uris[index]);
                self.data_raft_uri_live[index] = false;
            }
        }
        // A process restart rebinds its stable advertised endpoints; it is not
        // a metadata topology mutation. Reassert the local durable identity,
        // then refresh every route consumer from the unchanged catalog.
        try self.data_servers[index].acceptAuthoritativeStoreRegistration(index + 1, index + 1);
        for (self.data_servers[0..self.data_server_count], 0..) |*server, server_index| {
            if (!self.data_server_live[server_index]) continue;
            try server.refreshRemoteMetadataSnapshot();
        }
        self.data_server_paused[index] = false;
        for (initial_groups) |group_id| try self.waitForDataLeader(group_id);
    }

    pub fn start(self: *Fixture) void {
        std.debug.assert(self.driver_future == null);
        std.debug.assert(self.workload_future == null);
        for (0..node_count) |index| {
            std.debug.assert(self.raft_driver_futures[index] == null);
            self.raft_driver_futures[index] = self.sim.io().async(driveRaft, .{ self, index });
        }
        self.driver_future = self.sim.io().async(driveControl, .{self});
        self.workload_future = self.sim.io().async(runWorkload, .{self});
        self.phase = .workload_started;
    }

    fn runWorkload(self: *Fixture) void {
        self.runWorkloadInner() catch |err| {
            self.failure = err;
        };
        self.workload_done = true;
        if (self.graph_restart_future) |*future| {
            if (self.graph_owner_restart_requested) {
                // An unexpected successful graph response must not strand the
                // stopped production owner. Complete recovery before reporting
                // the property failure and beginning global teardown.
                if (self.graph_owner_restart_down and !self.graph_owner_restart_recovered and
                    self.graph_owner_restart_failure == null)
                {
                    self.graph_restart_recover.post(self.sim.io());
                }
                future.await(self.sim.io());
            } else {
                future.cancel(self.sim.io());
            }
            self.graph_restart_future = null;
        }
        if (self.failure == null) self.failure = self.graph_owner_restart_failure;
        self.driver_stop = true;
        if (self.driver_future) |*future| {
            // The driver polls this stop bit at a 1 ms logical cadence. Join
            // it normally so an in-progress Raft round is not turned into a
            // synthetic Canceled failure after the workload has succeeded.
            if (self.teardown_started)
                future.cancel(self.sim.io())
            else {
                self.control_requests.post(self.sim.io());
                future.await(self.sim.io());
            }
            self.driver_future = null;
        }
        for (&self.raft_driver_futures) |*future| if (future.*) |*live| {
            if (self.teardown_started)
                live.cancel(self.sim.io())
            else
                live.await(self.sim.io());
            future.* = null;
        };
        if (self.failure == null and self.driver_failure != null) self.failure = self.driver_failure;
        // Bounded-history teardown owns service destruction after every
        // scheduler task has unwound. Draining the hosted transition
        // registration here can otherwise wait on the metadata callback whose
        // cancellation is what resumed this workload task.
        if (self.teardown_started) {
            self.complete = true;
            self.phase = .complete;
            return;
        }
        self.cleanupRuntime();
        self.complete = true;
        self.phase = .complete;
    }

    fn runWorkloadInner(self: *Fixture) !void {
        const left_body = if (self.graph_enabled) left_batch_body else ordinary_left_batch_body;
        const right_body = if (self.graph_enabled) right_batch_body else ordinary_right_batch_body;
        for (initial_groups) |group_id| {
            try self.waitForDataLeader(group_id);
            std.log.debug("production data-plane VOPR elected group leader group={}", .{group_id});
        }
        self.topology_sound = self.metadata.?.cluster.currentMetadataLeaderIndex() != null;

        var left_write = self.sim.io().async(runWrite, .{
            self,
            0,
            self.data_api_uris[2],
            "docs",
            left_body,
        });
        var right_write = self.sim.io().async(runWrite, .{
            self,
            1,
            self.data_api_uris[0],
            "docs",
            right_body,
        });
        var tenant_write = self.sim.io().async(runWrite, .{
            self,
            2,
            self.data_api_uris[1],
            "tenant_b_docs",
            "{\"inserts\":{\"tenant:q\":{\"title\":\"production-tenant\"}},\"sync_level\":\"write\"}",
        });
        const left_write_result = left_write.await(self.sim.io());
        const right_write_result = right_write.await(self.sim.io());
        const tenant_write_result = tenant_write.await(self.sim.io());
        const left_write_disposition = try writeDisposition(left_write_result);
        const right_write_disposition = try writeDisposition(right_write_result);
        const tenant_write_disposition = try writeDisposition(tenant_write_result);
        self.phase = .writes_complete;

        var left_read = self.sim.io().async(runRead, .{
            self, 0, 1, "docs", "doc:c", "production-left",
        });
        var right_read = self.sim.io().async(runRead, .{
            self, 1, 2, "docs", "doc:x", "production-right",
        });
        var tenant_read = self.sim.io().async(runRead, .{
            self, 2, 0, "tenant_b_docs", "tenant:q", "production-tenant",
        });
        const left_read_result = left_read.await(self.sim.io());
        const right_read_result = right_read.await(self.sim.io());
        const tenant_read_result = tenant_read.await(self.sim.io());
        var left_read_sound = try operationSucceeded(left_read_result);
        var right_read_sound = try operationSucceeded(right_read_result);
        var tenant_read_sound = try operationSucceeded(tenant_read_result);
        left_read_sound = try self.resolveIdempotentWrite(
            left_write_disposition,
            left_read_sound,
            0,
            self.data_api_uris[2],
            1,
            "docs",
            left_body,
            "doc:c",
            "production-left",
        );
        right_read_sound = try self.resolveIdempotentWrite(
            right_write_disposition,
            right_read_sound,
            1,
            self.data_api_uris[0],
            2,
            "docs",
            right_body,
            "doc:x",
            "production-right",
        );
        tenant_read_sound = try self.resolveIdempotentWrite(
            tenant_write_disposition,
            tenant_read_sound,
            2,
            self.data_api_uris[1],
            0,
            "tenant_b_docs",
            "{\"inserts\":{\"tenant:q\":{\"title\":\"production-tenant\"}},\"sync_level\":\"write\"}",
            "tenant:q",
            "production-tenant",
        );
        // A 409 "write outcome unknown" is never blindly retried: the generic
        // batch API may contain non-idempotent transforms. These particular
        // fixed-ID upserts first resolve ambiguity by reading the exact value
        // and may retry only their known-idempotent final state. An
        // acknowledged or ambiguous write is sound only when that value is
        // visible through the public routing path.
        self.write_sound = left_read_sound and right_read_sound and tenant_read_sound;
        self.read_sound = left_read_sound and right_read_sound;
        self.tenant_sound = tenant_read_sound;
        self.phase = .reads_complete;
        if (!self.write_sound or !self.read_sound or !self.tenant_sound)
            return error.ProductionDataPublicRoundTripFailed;

        if (self.graph_enabled) {
            if (!try self.waitForDocIdentityReady("docs", 64))
                return error.ProductionDataDocIdentityPublicationTimeout;
            const right_hop_sound = try self.runGraphQuery("doc:x", 1, &.{"doc:k"});
            const round_trip_sound = try self.runGraphQuery("doc:c", 2, &.{ "doc:x", "doc:k" });
            self.graph_sound = right_hop_sound and round_trip_sound;
            self.phase = .graph_query_complete;
            if (!self.graph_sound) return error.ProductionDataGraphQueryFailed;
        }

        if (!self.active_split_enabled) return;

        // A full distributed witness must do more than host a static
        // topology. Turn on production control rounds, admit a metadata split,
        // let the real DataServers bootstrap/copy/fence it, publish the new
        // routing topology, and prove a migrated key remains publicly visible.
        try self.metadata.?.requestExternalDataSplit(
            split_transition_id,
            metadata_sim.VoprPublicClusterFixture.data_group_id,
            split_destination_group_id,
            split_key,
        );
        self.phase = .split_requested;
        if (self.graph_enabled) {
            try self.waitForSplitInProgress();
            const ingress_before = self.public_request_ingress_count;
            self.split_graph_inflight_sound = probe: {
                if (self.fault_mode != .clean) {
                    const start_leader_index = self.currentDataLeaderIndex(metadata_sim.VoprPublicClusterFixture.data_group_id) orelse
                        return error.ProductionDataGraphLeaderMissing;
                    const target_index = self.currentDataLeaderIndex(metadata_sim.VoprPublicClusterFixture.graph_data_group_id) orelse
                        return error.ProductionDataGraphLeaderMissing;
                    self.graph_probe_route_index = for (0..self.data_api_uri_count) |index| {
                        if (index != start_leader_index and index != target_index) break index;
                    } else return error.ProductionDataGraphRemoteCoordinatorMissing;
                    switch (self.fault_mode) {
                        .clean => unreachable,
                        .graph_transport_failure => self.graph_transport_fault_endpoint =
                            try parseHttpBaseUriAddress(self.data_api_uris[target_index]),
                        .graph_owner_restart => {
                            if (self.graph_restart_target_configured and self.graph_restart_target_index != target_index)
                                return error.ProductionGraphRestartTargetLeadershipChanged;
                            self.graph_restart_target_index = target_index;
                            self.graph_restart_future = self.sim.io().async(driveGraphOwnerRestart, .{self});
                        },
                        .graph_partial_write => self.graph_transport_fault_endpoint =
                            blk: {
                                if (self.graph_partial_write_target_configured and
                                    self.graph_partial_write_target_index != target_index)
                                    return error.ProductionGraphPartialWriteTargetLeadershipChanged;
                                break :blk try parseHttpBaseUriAddress(self.data_api_uris[target_index]);
                            },
                    }
                    self.graph_transport_fault_armed = true;
                }
                var in_flight_graph = self.sim.io().async(runSplitGraphProbe, .{self});
                errdefer _ = in_flight_graph.cancel(self.sim.io()) catch {};
                try self.waitForPublicIngressAfter(ingress_before);
                self.split_graph_inflight_started = true;
                self.phase = .split_graph_query_started;
                try self.waitForSplitFinalized();
                break :probe try in_flight_graph.await(self.sim.io());
            };
            if (!self.split_graph_inflight_sound)
                return error.ProductionDataInFlightSplitGraphQueryFailed;
        } else {
            try self.waitForSplitFinalized();
        }
        self.split_finalized = true;
        self.phase = .split_finalized;

        // waitForSplitFinalized only observes state after a requested control
        // round has published its completion, so no later round can still be
        // racing publication at this handoff.
        std.debug.assert(!self.control_round_active);
        self.stopControlDriver();
        try self.metadata.?.retireExternalDataSplit(split_transition_id);
        self.split_published = true;
        self.phase = .split_published;
        try self.waitForDataLeader(split_destination_group_id);

        const post_split_read_result = self.runRead(
            3,
            0,
            "docs",
            "doc:k",
            "production-split",
        );
        self.split_sound = try operationSucceeded(post_split_read_result);
        self.topology_sound = self.topology_sound and self.split_sound;
        self.phase = .post_split_read_complete;
        if (!self.split_sound) return error.ProductionDataSplitRoundTripFailed;

        if (self.graph_enabled) {
            // The split key moves doc:k to the newly published destination
            // while doc:c and doc:x retain their original owners. Re-run both
            // directions through the public coordinator so this history proves
            // graph planning, Raft/derived-state visibility, and hydration
            // against the post-cutover three-range topology rather than merely
            // observing that the migrated document is individually readable.
            const right_hop_sound = try self.runGraphQuery("doc:x", 1, &.{"doc:k"});
            const round_trip_sound = try self.runGraphQuery("doc:c", 2, &.{ "doc:x", "doc:k" });
            self.post_split_graph_sound = right_hop_sound and round_trip_sound;
            self.graph_sound = self.graph_sound and self.post_split_graph_sound;
            self.phase = .post_split_graph_query_complete;
            if (!self.post_split_graph_sound)
                return error.ProductionDataPostSplitGraphQueryFailed;
        }
    }

    fn runGraphQuery(
        self: *Fixture,
        start_key: []const u8,
        max_depth: u32,
        expected_keys: []const []const u8,
    ) !bool {
        const query_body = try test_contract_helpers.encodeGraphTraverseQueryRequest(
            self.alloc,
            "walk",
            graph_index_name,
            &.{start_key},
            &.{"links"},
            max_depth,
            10,
        );
        defer self.alloc.free(query_body);

        // Queries are idempotent. Retry only typed availability responses;
        // never reinterpret a successful partial response as transient.
        for (0..node_count * 4) |attempt| {
            const uri = self.data_api_uris[attempt % node_count];
            var response = self.client.fetchQueryRaw(uri, "docs", query_body) catch |err| switch (err) {
                error.Canceled => return err,
                else => {
                    try self.sim.io().sleep(.fromMilliseconds(1), .awake);
                    continue;
                },
            };
            defer response.deinit(self.alloc);
            if (response.status != 200) {
                if (response.status != 409 and response.status != 503)
                    return error.UnexpectedHttpStatus;
                try self.sim.io().sleep(.fromMilliseconds(1), .awake);
                continue;
            }
            return try self.graphResponseComplete(response.body, start_key, max_depth, expected_keys);
        }
        return false;
    }

    fn runSplitGraphProbe(self: *Fixture) !bool {
        const query_body = try test_contract_helpers.encodeGraphTraverseQueryRequest(
            self.alloc,
            "walk",
            graph_index_name,
            &.{"doc:c"},
            &.{"links"},
            2,
            10,
        );
        defer self.alloc.free(query_body);

        var response = try self.client.fetchQueryRaw(self.data_api_uris[self.graph_probe_route_index], "docs", query_body);
        defer response.deinit(self.alloc);
        switch (response.status) {
            200 => {
                self.split_graph_inflight_complete = try self.graphResponseComplete(
                    response.body,
                    "doc:c",
                    2,
                    &.{ "doc:x", "doc:k" },
                );
                if (self.fault_mode == .graph_partial_write) {
                    self.graph_partial_write_observed = self.graph_partial_write_injected and
                        self.sim.outboundEndpointPayloadPartialWriteCount() ==
                            self.graph_partial_write_count_before + 1;
                    return self.split_graph_inflight_complete and self.graph_partial_write_observed;
                }
                if (self.fault_mode != .clean) return false;
                return self.split_graph_inflight_complete;
            },
            409, 503 => {
                // A request crossing topology publication may fail closed,
                // but it must never return a successful partial traversal.
                self.split_graph_inflight_rejected = graphResponseFailClosed(response.status, response.body);
                if (self.fault_mode == .graph_transport_failure) {
                    self.graph_partial_rejected_sound =
                        self.graph_transport_failure_injected and
                        self.graph_transport_failure_observed and
                        self.graph_transport_failure_error_code != 0 and
                        response.status == 503 and
                        std.mem.indexOf(u8, response.body, "\"code\":\"distributed_query_unavailable\"") != null and
                        std.mem.indexOf(u8, response.body, "\"retryable\":true") != null and
                        self.split_graph_inflight_rejected;
                    return self.graph_partial_rejected_sound;
                }
                if (self.fault_mode == .graph_owner_restart) {
                    self.graph_partial_rejected_sound =
                        self.graph_owner_restart_requested and
                        self.graph_owner_restart_down and
                        self.graph_owner_restart_failure_observed and
                        self.graph_owner_restart_recovered and
                        self.graph_owner_restart_failure == null and
                        self.graph_owner_restart_error_code != 0 and
                        response.status == 503 and
                        std.mem.indexOf(u8, response.body, "\"code\":\"distributed_query_unavailable\"") != null and
                        std.mem.indexOf(u8, response.body, "\"retryable\":true") != null and
                        self.split_graph_inflight_rejected;
                    return self.graph_partial_rejected_sound;
                }
                if (self.fault_mode == .graph_partial_write) return false;
                return self.split_graph_inflight_rejected;
            },
            else => return error.UnexpectedHttpStatus,
        }
    }

    fn graphResponseFailClosed(status: u16, body: []const u8) bool {
        return (status == 409 or status == 503) and
            std.mem.indexOf(u8, body, "\"code\":\"") != null and
            std.mem.indexOf(u8, body, "\"retryable\":") != null and
            std.mem.indexOf(u8, body, "graph_results") == null and
            std.mem.indexOf(u8, body, "doc:x") == null and
            std.mem.indexOf(u8, body, "doc:k") == null;
    }

    fn waitForPublicIngressAfter(self: *Fixture, baseline: u64) !void {
        for (0..1_024) |_| {
            if (self.public_request_ingress_count > baseline) return;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        }
        return error.ProductionDataGraphIngressTimeout;
    }

    fn graphResponseComplete(
        self: *Fixture,
        body: []const u8,
        start_key: []const u8,
        max_depth: u32,
        expected_keys: []const []const u8,
    ) !bool {
        var parsed = try std.json.parseFromSlice(
            metadata_openapi.QueryResponses,
            self.alloc,
            body,
            .{},
        );
        defer parsed.deinit();
        const responses = parsed.value.responses orelse return false;
        if (responses.len != 1) return false;
        const graph_results = responses[0].graph_results orelse return false;
        const walk = graph_results.map.get("walk") orelse return false;
        const nodes = walk.nodes orelse return false;
        if (walk.total != expected_keys.len or nodes.len != expected_keys.len) {
            std.debug.print(
                "production graph probe start={s} depth={} returned incomplete result: {s}\n",
                .{ start_key, max_depth, body },
            );
            return false;
        }
        for (expected_keys) |expected| {
            var found = false;
            for (nodes) |node| found = found or std.mem.eql(u8, node.key, expected);
            if (!found) {
                std.debug.print(
                    "production graph probe start={s} missing={s}: {s}\n",
                    .{ start_key, expected, body },
                );
                return false;
            }
        }
        return true;
    }

    fn waitForDocIdentityReady(self: *Fixture, table_name: []const u8, max_rounds: usize) !bool {
        for (0..max_rounds) |_| {
            _ = self.metadata.?.cluster.currentMetadataLeaderIndex() orelse {
                try self.runOneControlRound();
                continue;
            };
            var every_metadata_replica_ready = true;
            for (0..node_count) |node_index| {
                const node = self.metadata.?.cluster.node(node_index);
                var snapshot = try node.adminSnapshot();
                defer node.freeAdminSnapshot(&snapshot);

                var table_id: ?u64 = null;
                for (snapshot.tables) |table| {
                    if (!std.mem.eql(u8, table.name, table_name)) continue;
                    table_id = table.table_id;
                    break;
                }
                var range_count: usize = 0;
                var ready_count: usize = 0;
                if (table_id) |id| for (snapshot.ranges) |range| {
                    if (range.table_id != id) continue;
                    range_count += 1;
                    for (snapshot.merged_group_statuses) |status| {
                        if (status.group_id != range.group_id) continue;
                        const identity = status.doc_identity;
                        if (!status.doc_identity_reassignment_active and
                            !status.doc_identity_namespace_conflict and
                            !identity.rebuild_required and
                            identity.namespace_table_id == range.table_id and
                            identity.namespace_shard_id == metadata_table_manager.rangeDocIdentityShardId(range) and
                            identity.namespace_range_id == metadata_table_manager.rangeDocIdentityRangeId(range))
                        {
                            ready_count += 1;
                        }
                        break;
                    }
                };
                if (range_count == 0 or ready_count != range_count) {
                    every_metadata_replica_ready = false;
                    break;
                }
            }
            if (every_metadata_replica_ready) {
                for (&self.data_servers, 0..) |*server, index| {
                    if (!self.data_server_live[index]) continue;
                    try server.refreshRemoteMetadataSnapshot();
                }
                return true;
            }
            try self.runOneControlRound();
        }
        return false;
    }

    fn waitForSplitFinalized(self: *Fixture) !void {
        for (0..30_000) |_| {
            if (try self.metadata.?.externalDataSplitFinalized(split_transition_id)) return;
            try self.runOneControlRound();
        }
        return error.ProductionDataSplitTimeout;
    }

    fn waitForSplitInProgress(self: *Fixture) !void {
        for (0..30_000) |_| {
            const phase = (try self.metadata.?.externalDataSplitPhase(split_transition_id)) orelse {
                try self.runOneControlRound();
                continue;
            };
            switch (phase) {
                .bootstrap_peer, .replay_deltas, .cutover_ready => return,
                .finalized => return error.ProductionDataSplitFinalizedBeforeGraphProbe,
                .rolling_back, .rolled_back => return error.ProductionDataSplitRolledBackBeforeGraphProbe,
                .prepare => try self.runOneControlRound(),
            }
        }
        return error.ProductionDataSplitProgressTimeout;
    }

    const OperationResult = union(enum) {
        success: bool,
        outcome_unknown,
        failure: anyerror,
    };

    fn operationSucceeded(result: OperationResult) !bool {
        return switch (result) {
            .success => |sound| sound,
            .outcome_unknown => error.UnresolvedWriteOutcome,
            .failure => |err| err,
        };
    }

    const WriteDisposition = enum { acknowledged, outcome_unknown };

    fn writeDisposition(result: OperationResult) !WriteDisposition {
        return switch (result) {
            .success => |sound| if (sound) .acknowledged else error.UnsuccessfulWrite,
            .outcome_unknown => .outcome_unknown,
            .failure => |err| err,
        };
    }

    fn resolveIdempotentWrite(
        self: *Fixture,
        initial_disposition: WriteDisposition,
        initially_visible: bool,
        operation_index: usize,
        write_uri: []const u8,
        read_uri_index: usize,
        table_name: []const u8,
        body: []const u8,
        key: []const u8,
        expected: []const u8,
    ) !bool {
        if (initially_visible) return true;
        // An acknowledged-but-invisible value is a safety failure. Only the
        // explicit unknown-outcome contract permits this workload to retry,
        // and only because these fixed-ID upserts have an idempotent final
        // state. Generic batch callers must not infer the same permission.
        if (initial_disposition == .acknowledged) return false;
        for (0..3) |_| {
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
            const disposition = try writeDisposition(self.runWrite(
                operation_index,
                write_uri,
                table_name,
                body,
            ));
            const visible = try operationSucceeded(self.runRead(
                operation_index,
                read_uri_index,
                table_name,
                key,
                expected,
            ));
            if (visible) return true;
            if (disposition == .acknowledged) return false;
        }
        return false;
    }

    fn runWrite(
        self: *Fixture,
        operation_index: usize,
        uri: []const u8,
        table_name: []const u8,
        body: []const u8,
    ) OperationResult {
        self.write_attempts[operation_index] +|= 1;
        var response = self.client.fetchBatchResponse(uri, table_name, body) catch |err|
            return .{ .failure = err };
        defer response.deinit(self.alloc);
        self.write_statuses[operation_index] = response.status;
        self.write_body_digests[operation_index] = std.hash.Wyhash.hash(0, response.body);
        if (response.status == 409 and
            std.mem.eql(u8, std.mem.trim(u8, response.body, " \t\r\n"), "write outcome unknown"))
        {
            self.write_outcome_unknowns[operation_index] +|= 1;
            return .outcome_unknown;
        }
        if (response.status != 201 and response.status != 202) {
            std.debug.print(
                "production write operation={} status={} body={s}\n",
                .{ operation_index, response.status, response.body },
            );
            return .{ .failure = error.UnexpectedHttpStatus };
        }
        return .{ .success = true };
    }

    fn runRead(
        self: *Fixture,
        operation_index: usize,
        starting_uri_index: usize,
        table_name: []const u8,
        key: []const u8,
        expected: []const u8,
    ) OperationResult {
        // Public provisioned reads deliberately fall back to stale when a
        // read-index request lands on a non-leader. Preserve the transient
        // miss in `read_attempts`, then retry through the other public ingress
        // nodes rather than polling one stale local replica forever. GET is
        // safe to repeat; writes are not.
        for (0..node_count * 4) |attempt| {
            self.read_attempts[operation_index] +|= 1;
            const uri = self.data_api_uris[(starting_uri_index + attempt) % node_count];
            var response = self.client.fetchLookupResponse(uri, table_name, key, null) catch |err|
                return .{ .failure = err };
            const status = response.status;
            const visible = status == 200 and std.mem.indexOf(u8, response.body, expected) != null;
            self.read_statuses[operation_index] = status;
            self.read_body_digests[operation_index] = std.hash.Wyhash.hash(0, response.body);
            if (status != 200 and status != 404) {
                std.debug.print(
                    "production read operation={} table={s} key={s} status={} body={s}\n",
                    .{ operation_index, table_name, key, status, response.body },
                );
            }
            response.deinit(self.alloc);
            if (visible) return .{ .success = true };
            if (status != 200 and status != 404) {
                return .{ .failure = error.UnexpectedHttpStatus };
            }
            self.sim.io().sleep(.fromMilliseconds(1), .awake) catch |err|
                return .{ .failure = err };
        }
        return .{ .success = false };
    }

    fn waitForDataLeader(self: *Fixture, group_id: u64) !void {
        for (0..30_000) |_| {
            for (&self.data_servers, 0..) |*server, index| {
                if (!self.data_server_live[index]) continue;
                const raft = server.data_raft orelse continue;
                const status = raft.host.http_host.host.raftStatus(group_id) orelse continue;
                if (status.soft.role == .leader) return;
            }
            if (self.driver_failure) |err| return err;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        }
        return error.ProductionDataLeaderTimeout;
    }

    fn currentDataLeaderIndex(self: *Fixture, group_id: u64) ?usize {
        for (self.data_servers[0..self.data_server_count], 0..) |*server, index| {
            if (!self.data_server_live[index]) continue;
            const raft = server.data_raft orelse continue;
            const status = raft.host.http_host.host.raftStatus(group_id) orelse continue;
            if (status.soft.role == .leader) return index;
        }
        return null;
    }

    fn cleanupRuntime(self: *Fixture) void {
        if (self.cleanup_sound) return;
        self.cleanup_started = true;
        self.graph_transport_fault_armed = false;
        self.graph_transport_fault_endpoint = null;
        self.sim.setOutboundEndpointOutage(null);
        var transition_index = self.transition_registration_count;
        while (transition_index > 0) {
            transition_index -= 1;
            if (self.transition_registrations[transition_index]) |*registration|
                registration.deinit();
            self.transition_registrations[transition_index] = null;
        }
        self.transition_registration_count = 0;
        if (!self.complete and !self.workload_done and !self.teardown_started and self.executor_live and self.raft_executor_live and self.public_executor_live) std.log.err(
            "production data-plane VOPR cleanup begin active_http_requests metadata={} raft={} public={}",
            .{ self.executor.activeRequestCount(), self.raft_executor.activeRequestCount(), self.public_executor.activeRequestCount() },
        );
        // Publish cancellation on every nested HTTP lane before draining any
        // one service owner. Public requests can depend on metadata requests
        // which in turn wait on Raft delivery; closing lanes sequentially would
        // deadlock that dependency chain during bounded-history cancellation.
        if (self.public_executor_live) self.public_executor.beginShutdown();
        if (self.transition_executor_live) self.transition_executor.beginShutdown();
        if (self.executor_live) self.executor.beginShutdown();
        if (self.raft_executor_live) self.raft_executor.beginShutdown();
        if (self.metadata) |metadata| for (0..node_count) |index| {
            self.final_resource_usage[index] = metadata.deploymentResourceUsage(index) catch .{};
        };
        if (self.metadata) |metadata| {
            self.final_raft_wire_requests = metadata.raft_wire_requests;
            for (metadata.raft_wire_runtimes[0..metadata.raft_wire_runtime_count]) |*runtime|
                self.final_raft_wire_requests +|= runtime.requestCount();
        }
        for (self.data_raft_listeners[0..self.data_raft_listener_count], 0..) |*runtime, index| {
            if (!self.data_raft_listener_live[index]) continue;
            self.final_raft_wire_requests +|= runtime.requestCount();
        }

        var server_index = self.data_server_count;
        while (server_index > 0) {
            server_index -= 1;
            if (!self.data_server_live[server_index]) continue;
            self.data_servers[server_index].quiesceBackgroundWork();
        }
        if (!self.complete and !self.workload_done and !self.teardown_started and self.executor_live and self.raft_executor_live and self.public_executor_live) std.log.err(
            "production data-plane VOPR after DataServer quiesce active_http_requests metadata={} raft={} public={}",
            .{ self.executor.activeRequestCount(), self.raft_executor.activeRequestCount(), self.public_executor.activeRequestCount() },
        );
        var data_raft_listener_index = self.data_raft_listener_count;
        while (data_raft_listener_index > 0) {
            data_raft_listener_index -= 1;
            if (!self.data_raft_listener_live[data_raft_listener_index]) continue;
            self.data_raft_listeners[data_raft_listener_index].deinit();
            self.data_raft_listener_live[data_raft_listener_index] = false;
        }
        self.data_raft_listener_count = 0;
        if (!self.complete and !self.workload_done and !self.teardown_started and self.executor_live and self.raft_executor_live and self.public_executor_live) std.log.err(
            "production data-plane VOPR after data-Raft listener drain active_http_requests metadata={} raft={} public={}",
            .{ self.executor.activeRequestCount(), self.raft_executor.activeRequestCount(), self.public_executor.activeRequestCount() },
        );
        server_index = self.data_server_count;
        while (server_index > 0) {
            server_index -= 1;
            if (!self.data_server_live[server_index]) continue;
            self.data_servers[server_index].deinit();
            self.data_server_live[server_index] = false;
        }
        self.data_server_count = 0;
        if (!self.complete and !self.workload_done and !self.teardown_started and self.executor_live and self.raft_executor_live and self.public_executor_live) std.log.err(
            "production data-plane VOPR after DataServer deinit active_http_requests metadata={} raft={} public={}",
            .{ self.executor.activeRequestCount(), self.raft_executor.activeRequestCount(), self.public_executor.activeRequestCount() },
        );
        var backend_runtime_index = self.backend_runtime_count;
        while (backend_runtime_index > 0) {
            backend_runtime_index -= 1;
            self.backend_runtimes[backend_runtime_index].deinit();
        }
        self.backend_runtime_count = 0;
        for (self.data_api_uris[0..self.data_api_uri_count], 0..) |uri, index| {
            if (!self.data_api_uri_live[index]) continue;
            self.alloc.free(uri);
            self.data_api_uri_live[index] = false;
        }
        self.data_api_uri_count = 0;
        for (self.data_raft_uris[0..self.data_raft_uri_count], 0..) |uri, index| {
            if (!self.data_raft_uri_live[index]) continue;
            self.alloc.free(uri);
            self.data_raft_uri_live[index] = false;
        }
        self.data_raft_uri_count = 0;
        if (self.raft_executor_live) {
            self.raft_executor.drainShutdown();
            const active_requests = self.raft_executor.activeRequestCount();
            if (active_requests != 0) std.debug.panic(
                "production data-plane VOPR Raft executor still has {} active request lease(s) after owner drain",
                .{active_requests},
            );
            self.raft_executor.deinit();
            self.raft_executor_live = false;
        }
        if (self.transition_executor_live) {
            self.transition_executor.drainShutdown();
            const active_requests = self.transition_executor.activeRequestCount();
            if (active_requests != 0) std.debug.panic(
                "production data-plane VOPR transition executor still has {} active request lease(s) after owner drain",
                .{active_requests},
            );
            self.transition_executor.deinit();
            self.transition_executor_live = false;
        }
        if (self.public_executor_live) {
            self.public_executor.drainShutdown();
            const active_requests = self.public_executor.activeRequestCount();
            if (active_requests != 0) std.debug.panic(
                "production data-plane VOPR public executor still has {} active request lease(s) after owner drain",
                .{active_requests},
            );
            self.public_executor.deinit();
            self.public_executor_live = false;
        }
        if (self.executor_live) {
            self.executor.drainShutdown();
            const active_requests = self.executor.activeRequestCount();
            if (active_requests != 0) std.debug.panic(
                "production data-plane VOPR metadata executor still has {} active request lease(s) after owner drain",
                .{active_requests},
            );
            self.executor.deinit();
            self.executor_live = false;
        }
        var listener_index = self.metadata_listener_count;
        while (listener_index > 0) {
            listener_index -= 1;
            self.metadata_listeners[listener_index].deinit();
        }
        self.metadata_listener_count = 0;
        var metadata_server_index = self.metadata_server_count;
        while (metadata_server_index > 0) {
            metadata_server_index -= 1;
            self.metadata_servers[metadata_server_index].deinit();
        }
        self.metadata_server_count = 0;
        for (self.metadata_base_uris[0..self.metadata_uri_count]) |uri| self.alloc.free(uri);
        self.metadata_uri_count = 0;
        for (self.data_catalogs[0..self.data_catalog_count]) |catalog| self.alloc.free(catalog);
        self.data_catalog_count = 0;
        for (self.data_roots[0..self.data_root_count]) |root| self.alloc.free(root);
        self.data_root_count = 0;
        if (self.metadata) |metadata| {
            metadata.deinit();
            self.metadata = null;
        }
        self.cleanup_sound = true;
        self.phase = .cleanup_complete;
    }

    pub fn deinit(self: *Fixture) void {
        if (!self.complete and !self.teardown_started) {
            std.log.debug("production data-plane VOPR stopped before completion driver_failure={s}", .{
                if (self.driver_failure) |err| @errorName(err) else "none",
            });
            for (initial_groups) |group_id| for (self.data_servers[0..self.data_server_count], 0..) |*server, index| {
                if (!self.data_server_live[index]) continue;
                const raft = server.data_raft orelse continue;
                const metrics = raft.host.http_host.metricsSnapshot();
                const transport_host = &raft.host.http_host.transport_stack.transport_host;
                const transport_metrics = transport_host.metricsSnapshot();
                std.log.err("production data-plane VOPR group={} node={} status={any}", .{
                    group_id,
                    index + 1,
                    raft.host.http_host.host.raftStatus(group_id),
                });
                std.log.err(
                    "production data-plane VOPR node={} uri={s} listener_requests={} routes={} served={} frames={} frame_failures={} sends={} failed={} retried={} pending={}",
                    .{
                        index + 1,
                        self.data_raft_uris[index],
                        self.data_raft_listeners[index].requestCount(),
                        transport_host.peer_routes.count(),
                        transport_host.served_groups.count(),
                        transport_metrics.sent_frames,
                        transport_metrics.send_failures,
                        metrics.async_send_enqueued,
                        metrics.async_send_failed,
                        metrics.async_send_retried,
                        metrics.async_send_pending,
                    },
                );
            };
        }
        self.driver_stop = true;
        if (self.workload_future) |*future| {
            future.cancel(self.sim.io());
            self.workload_future = null;
        }
        if (self.graph_restart_future) |*future| {
            future.cancel(self.sim.io());
            self.graph_restart_future = null;
        }
        if (self.driver_future) |*future| {
            future.cancel(self.sim.io());
            self.driver_future = null;
        }
        for (&self.raft_driver_futures) |*future| if (future.*) |*live| {
            live.cancel(self.sim.io());
            future.* = null;
        };
        if (!self.cleanup_sound) self.cleanupRuntime();
        self.phase = .complete;
        self.alloc.destroy(self);
    }

    pub fn beginTeardown(self: *Fixture) void {
        if (self.teardown_started) return;
        if (!self.complete and self.phase != .created) std.debug.print(
            "production data-plane VOPR cutoff phase={s} driver_rounds={} workload_done={} split_finalized={} split_published={} failure={s} driver_failure={s}\n",
            .{
                @tagName(self.phase),
                self.driver_rounds,
                self.workload_done,
                self.split_finalized,
                self.split_published,
                if (self.failure) |err| @errorName(err) else "none",
                if (self.driver_failure) |err| @errorName(err) else "none",
            },
        );
        self.teardown_started = true;
        self.graph_transport_fault_armed = false;
        self.graph_transport_fault_endpoint = null;
        self.sim.setOutboundEndpointOutage(null);
        self.driver_stop = true;
        if (self.public_executor_live) self.public_executor.beginShutdown();
        if (self.transition_executor_live) self.transition_executor.beginShutdown();
        if (self.executor_live) self.executor.beginShutdown();
        if (self.raft_executor_live) self.raft_executor.beginShutdown();
        for (self.data_servers[0..self.data_server_count], 0..) |*server, index| {
            if (!self.data_server_live[index]) continue;
            server.beginTeardown();
        }
        for (self.metadata_listeners[0..self.metadata_listener_count]) |*listener|
            listener.requestStop();
        for (self.data_raft_listeners[0..self.data_raft_listener_count], 0..) |*listener, index| {
            if (!self.data_raft_listener_live[index]) continue;
            listener.requestStop();
        }
        if (self.metadata) |metadata| metadata.beginTeardown();
    }

    pub fn healthSnapshot(self: *const Fixture) struct {
        requests_ok: bool,
        topology_ok: bool,
        graph_query_ok: bool,
        split_graph_inflight_started: bool,
        split_graph_inflight_complete: bool,
        split_graph_inflight_rejected: bool,
        split_graph_inflight_ok: bool,
        post_split_graph_query_ok: bool,
        graph_transport_failure_injected: bool,
        graph_transport_failure_observed: bool,
        graph_transport_failure_error_code: u16,
        graph_owner_restart_requested: bool,
        graph_owner_restart_down: bool,
        graph_owner_restart_failure_observed: bool,
        graph_owner_restart_recovered: bool,
        graph_owner_restart_error_code: u16,
        graph_partial_write_injected: bool,
        graph_partial_write_observed: bool,
        graph_partial_rejected_sound: bool,
        cleanup_ok: bool,
        node_resource_managers: usize,
        hosts: usize,
        raft_wire_requests: u64,
    } {
        return .{
            .requests_ok = self.write_sound and
                self.read_sound and
                self.tenant_sound and
                (!self.graph_enabled or self.graph_sound) and
                (!self.active_split_enabled or self.split_sound) and
                self.failure == null,
            .topology_ok = self.topology_sound,
            .graph_query_ok = self.graph_sound,
            .split_graph_inflight_started = self.split_graph_inflight_started,
            .split_graph_inflight_complete = self.split_graph_inflight_complete,
            .split_graph_inflight_rejected = self.split_graph_inflight_rejected,
            .split_graph_inflight_ok = self.split_graph_inflight_sound,
            .post_split_graph_query_ok = self.post_split_graph_sound,
            .graph_transport_failure_injected = self.graph_transport_failure_injected,
            .graph_transport_failure_observed = self.graph_transport_failure_observed,
            .graph_transport_failure_error_code = self.graph_transport_failure_error_code,
            .graph_owner_restart_requested = self.graph_owner_restart_requested,
            .graph_owner_restart_down = self.graph_owner_restart_down,
            .graph_owner_restart_failure_observed = self.graph_owner_restart_failure_observed,
            .graph_owner_restart_recovered = self.graph_owner_restart_recovered,
            .graph_owner_restart_error_code = self.graph_owner_restart_error_code,
            .graph_partial_write_injected = self.graph_partial_write_injected,
            .graph_partial_write_observed = self.graph_partial_write_observed,
            .graph_partial_rejected_sound = self.graph_partial_rejected_sound,
            .cleanup_ok = self.cleanup_sound,
            // `backend_runtime_count` is live ownership and intentionally
            // reaches zero during cleanup. Keep the monotonic construction
            // witness so terminal observations can still prove that every
            // production node received its own resource owner.
            .node_resource_managers = self.backend_runtime_owners_started,
            .hosts = if (@intFromEnum(self.phase) >= @intFromEnum(Phase.topology_ready)) 2 else 0,
            .raft_wire_requests = self.final_raft_wire_requests,
        };
    }

    pub fn phaseOrdinal(self: *const Fixture) u8 {
        return @intFromEnum(self.phase);
    }

    pub fn metadataBootstrapPhaseOrdinal(self: *const Fixture) u8 {
        return if (self.metadata) |metadata| metadata.bootstrapPhaseOrdinal() else 0;
    }

    pub fn primaryGroupProgress(self: *const Fixture) RaftProgress {
        if (self.cleanup_started) return .{};
        var progress: RaftProgress = .{};
        for (self.data_servers[0..self.data_server_count], 0..) |*server, index| {
            if (!self.data_server_live[index]) continue;
            const raft = server.data_raft orelse continue;
            const status = raft.host.http_host.host.raftStatus(initial_groups[0]) orelse continue;
            progress.commit_index = @max(progress.commit_index, status.hard.commit_index);
            progress.applied_index = @max(progress.applied_index, status.applied_index);
            progress.last_index = @max(progress.last_index, status.last_index);
            progress.leaders += @intFromBool(status.soft.role == .leader);
            const metrics = raft.host.http_host.metricsSnapshot();
            const transport_host = &raft.host.http_host.transport_stack.transport_host;
            const transport_metrics = transport_host.metricsSnapshot();
            progress.peer_routes += transport_host.peer_routes.count();
            progress.frames_enqueued +|= metrics.async_send_enqueued;
            progress.frames_pending += metrics.async_send_pending;
            progress.frames_failed +|= metrics.async_send_failed;
            progress.frames_sent +|= transport_metrics.sent_frames;
        }
        return progress;
    }

    pub fn deploymentResourceUsage(self: *const Fixture, index: usize) !vopr.deployment.ResourceUsage {
        if (index >= node_count) return error.InvalidFullClusterNodeIndex;
        return self.final_resource_usage[index];
    }
};
