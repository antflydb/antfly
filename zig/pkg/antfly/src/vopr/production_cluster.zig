// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Production-owned data plane for the deployment-shaped full-cluster VOPR
//! history. Metadata keeps only its quorum replicas; three real DataServers
//! own public HTTP, data-Raft, storage, metadata polling, and status reporting.

const std = @import("std");
const casbin = @import("antfly_casbin");
const vopr = @import("vopr");
const data_runtime = @import("../data/runtime.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_http_server = @import("../metadata/http_server.zig");
const metadata_http_test_runtime = @import("../metadata/http_test_runtime.zig");
const metadata_sim = @import("../metadata/sim_harness.zig");
const raft_runtime_loop = @import("../raft/runtime_loop.zig");
const raft_transport = @import("../raft/transport/mod.zig");
const background_runtime = @import("../storage/background_runtime.zig");
const api_http_client = @import("../api/http_client.zig");
const api_http_server = @import("../api/http_server.zig");
const api_batch = @import("../api/batch.zig");
const api_distributed_graph = @import("../api/distributed_graph.zig");
const api_distributed_join = @import("../api/distributed_join.zig");
const api_table_write_source = @import("../api/table_write_source.zig");
const test_contract_helpers = @import("../api/test_contract_helpers.zig");
const common_http = @import("../common/http/http_common.zig");
const io_http_executor = @import("../common/http/io_http_executor.zig");
const api_table_router = @import("../api/table_router.zig");
const hosted_shard_ops = @import("../raft/hosted_shard_ops.zig");
const shard_ops = @import("../raft/shard_ops.zig");
const transition_state = @import("../metadata/transition_state.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const resource_manager = @import("../storage/resource_manager.zig");
const mem_backend = @import("../storage/mem_backend.zig");
const backend_erased = @import("../storage/backend_erased.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const http_disconnect = @import("http_disconnect.zig");
const usermgr = @import("../usermgr/user_manager.zig");
const db_types = @import("../storage/db/types.zig");

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

/// Fixture-owned public credential adapter. The ordinary ApiHttpClient keeps
/// constructing every production request; this boundary only supplies the
/// same request-level credential that an external authenticated client would.
const PublicAuthorizationExecutor = struct {
    inner: common_http.RequestExecutor,
    authorization: []const u8,

    fn iface(self: *@This()) common_http.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{ .execute = execute },
            .realtime_ns_fn = if (self.inner.realtime_ns_fn != null) realtimeNs else null,
        };
    }

    fn execute(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        request: common_http.HttpRequest,
    ) anyerror!common_http.HttpResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var authenticated = request;
        authenticated.authorization = self.authorization;
        return try self.inner.execute(alloc, authenticated);
    }

    fn realtimeNs(ptr: *anyopaque) i128 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.inner.realtimeNs().?;
    }
};

pub const Fixture = struct {
    pub const FaultMode = enum {
        clean,
        graph_transport_failure,
        graph_transport_resource_pressure,
        graph_hydration_transport_failure,
        graph_owner_restart,
        graph_partial_write,
        resource_pressure,
        socket_pressure,
        join_finalizer_ack_failure,

        fn hasGraphTransportFailure(self: FaultMode) bool {
            return self == .graph_transport_failure or
                self == .graph_transport_resource_pressure;
        }

        fn hasResourcePressure(self: FaultMode) bool {
            return self == .resource_pressure or
                self == .graph_transport_resource_pressure;
        }
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
        join_query_complete,
        graph_query_complete,
        split_requested,
        split_join_query_complete,
        split_graph_query_started,
        split_finalized,
        split_published,
        post_split_read_complete,
        post_split_join_query_complete,
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

    const JoinLifecycleObserver = struct {
        fixture: *Fixture,
        node_index: usize,
    };

    pub const JoinOwnerRestartFaultObserver = struct {
        ptr: *anyopaque,
        activate: *const fn (ptr: *anyopaque, node_index: usize) anyerror!void,
    };

    pub const JoinRetryExhaustionFaultObserver = struct {
        ptr: *anyopaque,
        activate: *const fn (
            ptr: *anyopaque,
            coordinator_index: usize,
            retry_target_index: usize,
        ) anyerror!void,
    };

    pub const JoinCancellationOverlapFaultObserver = struct {
        ptr: *anyopaque,
        activate: *const fn (
            ptr: *anyopaque,
            coordinator_index: usize,
            network_target_index: usize,
        ) anyerror!void,
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
    const graph_authorization_username = "vopr-graph-reader";
    const graph_authorization_password = "vopr-graph-secret";
    const graph_authorization_header = "Basic dm9wci1ncmFwaC1yZWFkZXI6dm9wci1ncmFwaC1zZWNyZXQ=";
    const left_batch_body =
        \\{"inserts":{"doc:c":{"title":"production-left","_edges":{"graph_idx":{"links":[{"target":"doc:x"}]}}},"doc:k":{"title":"production-split"}},"sync_level":"full_index"}
    ;
    const graph_authorization_left_batch_body =
        \\{"inserts":{"doc:a":{"title":"authorization-source","_edges":{"graph_idx":{"links":[{"target":"tenant:q","metadata":{"target_table":"tenant_b_docs"}}]}}},"doc:c":{"title":"production-left","_edges":{"graph_idx":{"links":[{"target":"doc:x"}]}}},"doc:k":{"title":"production-split"}},"sync_level":"full_index"}
    ;
    const right_batch_body =
        \\{"inserts":{"doc:x":{"title":"production-right","_edges":{"graph_idx":{"links":[{"target":"doc:k"}]}}}},"sync_level":"full_index"}
    ;
    const ordinary_left_batch_body =
        "{\"inserts\":{\"doc:c\":{\"title\":\"production-left\"},\"doc:k\":{\"title\":\"production-split\"}},\"sync_level\":\"write\"}";
    const ordinary_right_batch_body =
        "{\"inserts\":{\"doc:x\":{\"title\":\"production-right\"}},\"sync_level\":\"write\"}";
    const join_left_batch_body =
        "{\"inserts\":{\"doc:c\":{\"title\":\"production-left\"},\"doc:k\":{\"title\":\"production-split\"}},\"sync_level\":\"full_index\"}";
    const join_right_batch_body =
        "{\"inserts\":{\"doc:x\":{\"title\":\"production-right\"}},\"sync_level\":\"full_index\"}";
    const tenant_batch_body =
        "{\"inserts\":{\"tenant:q\":{\"title\":\"production-tenant\",\"body\":\"production join left\",\"customer_id\":\"doc:c\"},\"tenant:r\":{\"title\":\"production-tenant\",\"body\":\"production join left\",\"customer_id\":\"doc:x\"}},\"sync_level\":\"full_index\"}";
    const global_query_body =
        \\{"table":"docs","query":{"match_all":{}},"fields":["title"],"limit":10}
        \\{"table":"tenant_b_docs","query":{"match_all":{}},"fields":["title"],"limit":10}
    ;
    const join_query_body =
        "{\"query\":{\"match_all\":{}},\"fields\":[\"title\"],\"limit\":10,\"profile\":true,\"join\":{\"right_table\":\"docs\",\"join_type\":\"inner\",\"on\":{\"left_field\":\"customer_id\",\"right_field\":\"_id\",\"operator\":\"eq\"},\"right_fields\":[\"title\"]}}";
    const durable_join_row_count: usize = 64;
    const durable_join_query_body =
        "{\"query\":{\"match_all\":{}},\"fields\":[\"title\"],\"limit\":64,\"profile\":true,\"join\":{\"right_table\":\"docs\",\"join_type\":\"inner\",\"on\":{\"left_field\":\"customer_id\",\"right_field\":\"_id\",\"operator\":\"eq\"},\"strategy_hint\":\"shuffle\",\"right_fields\":[\"title\"]}}";
    const resource_probe_body =
        "{\"inserts\":{\"pressure:probe\":{\"title\":\"pressure\",\"body\":\"production-owner-resource-recovery\"}},\"sync_level\":\"write\"}";
    const DataServer = data_runtime.DataServer;

    pub const WorkCostPorts = struct {
        data: [node_count]data_runtime.DataServerWorkCostPort,
        graph: [node_count]api_distributed_graph.WorkCostPort,
    };

    /// Keeps production listeners, Raft drivers, and storage owners live until
    /// a co-scheduled deployment workload has reached its terminal boundary.
    pub const CompletionFence = struct {
        ptr: *anyopaque,
        ready_fn: *const fn (ptr: *anyopaque) bool,

        fn ready(self: @This()) bool {
            return self.ready_fn(self.ptr);
        }
    };

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
    public_authorization_executor: PublicAuthorizationExecutor = undefined,
    http_disconnect_probe: http_disconnect.Probe = undefined,
    transition_executor: io_http_executor.IoHttpExecutor = undefined,
    transition_executor_live: bool = false,
    backend_runtimes: [node_count]background_runtime.BackendRuntimeHandle = undefined,
    backend_runtime_count: usize = 0,
    backend_runtime_owners_started: usize = 0,
    join_job_backends: [node_count]mem_backend.Backend = undefined,
    join_job_backend_count: usize = 0,
    join_job_stores: [node_count]backend_erased.Store = undefined,
    join_job_store_count: usize = 0,
    auth_store: usermgr.MemoryStore = undefined,
    auth_store_live: bool = false,
    auth_policy_store: casbin.MemoryAdapter = undefined,
    auth_policy_store_live: bool = false,
    auth_manager: usermgr.UserManager = undefined,
    auth_manager_live: bool = false,
    data_roots: [node_count][]u8 = undefined,
    data_root_count: usize = 0,
    capacity_sources: [node_count]ModeledCapacitySource = undefined,
    data_catalogs: [node_count][]u8 = undefined,
    data_catalog_count: usize = 0,
    data_servers: [node_count]DataServer = undefined,
    join_lifecycle_observers: [node_count]JoinLifecycleObserver = undefined,
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
    public_response_ready_count: u64 = 0,
    replication_batch_attempts: u64 = 0,
    replication_batch_successes: u64 = 0,
    replication_last_status: u16 = 0,
    replication_owner_restart_enabled: bool = false,
    replication_owner_restart_injected: bool = false,
    replication_owner_restart_target_index: usize = 0,
    replication_owner_restart_down: bool = false,
    replication_owner_restart_rejected: bool = false,
    replication_owner_restart_error_code: u64 = 0,
    replication_owner_restart_reconstructed: bool = false,
    replication_owner_restart_durable_row_recovered: bool = false,
    replication_owner_restart_direct_read: bool = false,
    replication_owner_restart_direct_read_attempts: u64 = 0,
    replication_owner_restart_direct_read_status: u16 = 0,
    replication_owner_restart_direct_read_error_code: u64 = 0,
    replication_owner_restart_failure: ?anyerror = null,
    completion_fence: ?CompletionFence = null,
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
    global_query_sound: bool = false,
    global_query_status: u16 = 0,
    global_query_response_count: usize = 0,
    global_query_result_assembled_count: u64 = 0,
    global_query_cancellation_boundary: std.Io.Semaphore = .{},
    global_query_cancellation_release: std.Io.Semaphore = .{},
    global_query_cancellation_armed: bool = false,
    global_query_cancellation_boundary_observed: bool = false,
    global_query_cancellation_requested: bool = false,
    global_query_cancellation_observed: bool = false,
    global_query_cancellation_no_partial: bool = false,
    global_query_cancellation_recovered: bool = false,
    global_query_cancellation_sound: bool = false,
    global_query_authorization_revocation_armed: bool = false,
    global_query_authorization_boundary_observed: bool = false,
    global_query_authorization_revoked: bool = false,
    global_query_authorization_denied_without_leak: bool = false,
    global_query_authorization_restored: bool = false,
    global_query_authorization_recovered: bool = false,
    global_query_authorization_denied_status: u16 = 0,
    global_query_authorization_recovered_status: u16 = 0,
    global_query_authorization_sound: bool = false,
    global_query_transport_failure_armed: bool = false,
    global_query_transport_boundary_observed: bool = false,
    global_query_transport_fault_injected: bool = false,
    global_query_transport_fault_observed: bool = false,
    global_query_transport_fault_matches: u64 = 0,
    global_query_transport_fault_count_before: u64 = 0,
    global_query_transport_fault_healed: bool = false,
    global_query_transport_rejected_without_partial: bool = false,
    global_query_transport_recovered: bool = false,
    global_query_transport_rejected_status: u16 = 0,
    global_query_transport_recovered_status: u16 = 0,
    global_query_transport_sound: bool = false,
    global_query_transport_target_index: usize = 0,
    global_query_transport_target_configured: bool = false,
    global_query_transport_fault_endpoint: ?std.Io.net.IpAddress = null,
    global_query_route_index: usize = 1,
    global_query_owner_restart_armed: bool = false,
    global_query_owner_restart_boundary_observed: bool = false,
    global_query_owner_restart_target_index: usize = 0,
    global_query_owner_restart_target_configured: bool = false,
    global_query_owner_restart_down: bool = false,
    global_query_owner_restart_rejected_without_partial: bool = false,
    global_query_owner_restart_rejected_status: u16 = 0,
    global_query_owner_restart_reconstructed: bool = false,
    global_query_owner_restart_direct_read: bool = false,
    global_query_owner_restart_recovered: bool = false,
    global_query_owner_restart_recovered_status: u16 = 0,
    global_query_owner_restart_sound: bool = false,
    global_query_restart_requested: std.Io.Semaphore = .{},
    global_query_restart_down: std.Io.Semaphore = .{},
    global_query_restart_recover: std.Io.Semaphore = .{},
    global_query_restart_recovered: std.Io.Semaphore = .{},
    global_query_restart_future: ?std.Io.Future(void) = null,
    global_query_owner_restart_failure: ?anyerror = null,
    join_sound: bool = false,
    split_join_sound: bool = false,
    post_split_join_sound: bool = false,
    join_finalizer_ack_failure_injected: bool = false,
    join_finalizer_persisted_group_id: u64 = 0,
    durable_join_takeover_sound: bool = false,
    join_partition_worker_started_count: u64 = 0,
    join_partition_worker_completed_count: u64 = 0,
    join_worker_retry_failure_injected: bool = false,
    join_worker_retry_job_id: u64 = 0,
    join_worker_retry_partition_index: usize = 0,
    join_worker_retry_failed_group_id: u64 = 0,
    join_worker_retry_recovered_group_id: u64 = 0,
    join_worker_retry_sound: bool = false,
    join_owner_restart_job_id: u64 = 0,
    join_owner_restart_partition_index: usize = 0,
    join_owner_restart_failed_group_id: u64 = 0,
    join_owner_restart_recovered_group_id: u64 = 0,
    join_owner_restart_target_index: usize = 0,
    join_owner_restart_recovery_index: usize = 0,
    join_owner_restart_coordinator_index: usize = 0,
    join_owner_restart_campaign_configured: bool = false,
    join_owner_restart_target_configured: bool = false,
    join_owner_restart_fault_observer: ?JoinOwnerRestartFaultObserver = null,
    join_owner_restart_requested: bool = false,
    join_owner_restart_down: bool = false,
    join_owner_restart_recovered: bool = false,
    join_owner_restart_initial_status: u16 = 0,
    join_owner_restart_initial_rejected_without_partial: bool = false,
    join_owner_restart_initial_query_active: bool = false,
    join_owner_restart_recovery_query_active: bool = false,
    join_owner_restart_recovery_join: bool = false,
    join_owner_restart_post_reconstruction_read: bool = false,
    join_owner_restart_sound: bool = false,
    join_owner_restart_failure: ?anyerror = null,
    join_retry_exhaustion_job_id: u64 = 0,
    join_retry_exhaustion_partition_index: usize = 0,
    join_retry_exhaustion_first_group_id: u64 = 0,
    join_retry_exhaustion_retry_group_id: u64 = 0,
    join_retry_exhaustion_coordinator_index: usize = 0,
    join_retry_exhaustion_retry_target_index: usize = 0,
    join_retry_exhaustion_campaign_configured: bool = false,
    join_retry_exhaustion_fault_observer: ?JoinRetryExhaustionFaultObserver = null,
    join_retry_exhaustion_faults_injected: bool = false,
    join_retry_exhaustion_resource_observed: bool = false,
    join_retry_exhaustion_network_observed: bool = false,
    join_retry_exhaustion_overlap_observed: bool = false,
    join_retry_exhaustion_network_matches_before: u64 = 0,
    join_retry_exhaustion_initial_worker_starts: u64 = 0,
    join_retry_exhaustion_initial_worker_completions: u64 = 0,
    join_retry_exhaustion_initial_status: u16 = 0,
    join_retry_exhaustion_initial_rejected_without_partial: bool = false,
    join_retry_exhaustion_network_healed: bool = false,
    join_retry_exhaustion_resource_healed: bool = false,
    join_retry_exhaustion_recovery_query_active: bool = false,
    join_retry_exhaustion_recovery_join: bool = false,
    join_retry_exhaustion_sound: bool = false,
    join_restart_requested: std.Io.Semaphore = .{},
    join_restart_down: std.Io.Semaphore = .{},
    join_restart_recover: std.Io.Semaphore = .{},
    join_restart_recovered: std.Io.Semaphore = .{},
    join_restart_future: ?std.Io.Future(void) = null,
    join_cancellation_boundary_observed: bool = false,
    join_cancellation_job_id: u64 = 0,
    join_cancellation_owner_group_id: u64 = 0,
    join_cancellation_requested: bool = false,
    join_cancellation_observed: bool = false,
    join_cancellation_recovered: bool = false,
    join_cancellation_sound: bool = false,
    join_cancellation_overlap_first_group_id: u64 = 0,
    join_cancellation_overlap_worker_group_id: u64 = 0,
    join_cancellation_overlap_coordinator_index: usize = 0,
    join_cancellation_overlap_network_target_index: usize = 0,
    join_cancellation_overlap_campaign_configured: bool = false,
    join_cancellation_overlap_fault_observer: ?JoinCancellationOverlapFaultObserver = null,
    join_cancellation_overlap_network_armed: bool = false,
    join_cancellation_overlap_faults_injected: bool = false,
    join_cancellation_overlap_network_matches_before: u64 = 0,
    join_cancellation_overlap_network_observed: bool = false,
    join_cancellation_overlap_resource_observed: bool = false,
    join_cancellation_overlap_observed: bool = false,
    join_cancellation_overlap_network_healed: bool = false,
    join_cancellation_overlap_resource_healed: bool = false,
    graph_sound: bool = false,
    graph_hydration_sound: bool = false,
    graph_hydration_started_count: u64 = 0,
    graph_hydration_fanout_started_count: u64 = 0,
    graph_hydration_completed_count: u64 = 0,
    graph_cancellation_requested: bool = false,
    graph_cancellation_observed: bool = false,
    graph_cancellation_recovered: bool = false,
    graph_cancellation_sound: bool = false,
    graph_cancellation_fault_injected: bool = false,
    graph_cancellation_fault_observed: bool = false,
    graph_cancellation_fault_matches: u64 = 0,
    graph_cancellation_fault_count_before: u64 = 0,
    graph_cancellation_fault_healed: bool = false,
    graph_authorization_revoked: bool = false,
    graph_authorization_boundary_observed: bool = false,
    graph_authorization_revocation_armed: bool = false,
    graph_authorization_denied_without_leak: bool = false,
    graph_authorization_restored: bool = false,
    graph_authorization_recovered: bool = false,
    graph_authorization_denied_status: u16 = 0,
    graph_authorization_recovered_status: u16 = 0,
    graph_authorization_sound: bool = false,
    graph_stale_snapshot_armed: bool = false,
    graph_stale_snapshot_boundary_observed: bool = false,
    graph_stale_snapshot_attempt_failures: u64 = 0,
    graph_stale_snapshot_error_code: u16 = 0,
    graph_stale_snapshot_rejected_without_partial: bool = false,
    graph_stale_snapshot_status: u16 = 0,
    graph_stale_snapshot_recovered: bool = false,
    graph_stale_snapshot_sound: bool = false,
    split_graph_inflight_started: bool = false,
    split_graph_inflight_complete: bool = false,
    split_graph_inflight_rejected: bool = false,
    split_graph_inflight_sound: bool = false,
    post_split_graph_sound: bool = false,
    graph_transport_failure_injected: bool = false,
    graph_transport_failure_observed: bool = false,
    graph_transport_failure_error_code: u16 = 0,
    overlapping_faults_active_observed: bool = false,
    graph_partial_rejected_sound: bool = false,
    graph_transport_fault_armed: bool = false,
    graph_transport_fault_endpoint: ?std.Io.net.IpAddress = null,
    graph_transport_target_index: usize = 0,
    graph_transport_target_configured: bool = false,
    graph_partial_write_injected: bool = false,
    graph_partial_write_observed: bool = false,
    graph_partial_write_count_before: u64 = 0,
    graph_partial_write_target_index: usize = 0,
    graph_partial_write_target_configured: bool = false,
    socket_pressure_target_index: usize = 0,
    socket_pressure_target_configured: bool = false,
    socket_pressure_injected: bool = false,
    socket_pressure_denial_observed: bool = false,
    socket_pressure_error_code: u16 = 0,
    socket_pressure_no_ingress: bool = false,
    socket_pressure_recovered: bool = false,
    resource_reservations: [node_count]?resource_manager.BatchReservation = .{null} ** node_count,
    resource_pressure_observed: bool = false,
    resource_denial_sound: bool = false,
    resource_denial_status: u16 = 0,
    resource_denial_body_digest: u64 = 0,
    resource_preproposal_denial: bool = false,
    resource_outcome_unknown: bool = false,
    resource_read_before_retry: bool = false,
    resource_retry_attempted: bool = false,
    resource_proposals_before: u64 = 0,
    resource_proposals_after: u64 = 0,
    resource_absent_before_retry: bool = false,
    resource_recovery_sound: bool = false,
    resource_post_split_sound: bool = false,
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
    graph_hydration_enabled: bool = false,
    graph_cancellation_enabled: bool = false,
    graph_inflight_authorization_revocation_enabled: bool = false,
    graph_stale_snapshot_retry_exhaustion_enabled: bool = false,
    join_enabled: bool = false,
    global_query_enabled: bool = false,
    global_query_cancellation_enabled: bool = false,
    global_query_authorization_revocation_enabled: bool = false,
    global_query_transport_failure_enabled: bool = false,
    global_query_owner_restart_enabled: bool = false,
    join_cancellation_enabled: bool = false,
    join_cancellation_overlap_enabled: bool = false,
    join_cancellation_owner_restart_enabled: bool = false,
    join_worker_retry_enabled: bool = false,
    join_owner_restart_enabled: bool = false,
    join_retry_exhaustion_enabled: bool = false,
    fault_mode: FaultMode = .clean,
    work_cost_ports: ?WorkCostPorts = null,

    pub fn create(alloc: std.mem.Allocator, sim: *vopr.vopr_io.VoprIo) !*Fixture {
        const self = try alloc.create(Fixture);
        self.* = .{ .alloc = alloc, .sim = sim };
        for (&self.join_lifecycle_observers, 0..) |*observer, index| {
            observer.* = .{ .fixture = self, .node_index = index };
        }
        self.http_disconnect_probe = .{ .vopr_io = sim };
        return self;
    }

    pub fn init(alloc: std.mem.Allocator, sim: *vopr.vopr_io.VoprIo) !*Fixture {
        const self = try create(alloc, sim);
        errdefer self.deinit();
        try self.bootstrap();
        return self;
    }

    /// Metadata-backed replication status boundary used by the composed
    /// production-runner history. Every mutation crosses the live metadata
    /// quorum owned by this fixture; no VOPR-only status ledger substitutes
    /// for the Raft projection.
    pub fn replaceReplicationSources(
        self: *Fixture,
        replication_sources_json: []const u8,
    ) !void {
        const metadata = self.metadata orelse return error.MetadataClusterUnavailable;
        try metadata.replaceReplicationSources(replication_sources_json);
    }

    pub fn upsertReplicationSourceStatus(
        self: *Fixture,
        record: metadata_table_manager.ReplicationSourceStatusRecord,
    ) !void {
        const metadata = self.metadata orelse return error.MetadataClusterUnavailable;
        try metadata.upsertReplicationSourceStatus(record);
    }

    pub fn claimReplicationSourceCutoverDurable(
        self: *Fixture,
        expected_replication_sources_json: []const u8,
        expected_authority_id: u64,
        record: metadata_table_manager.ReplicationSourceStatusRecord,
    ) !void {
        const metadata = self.metadata orelse return error.MetadataClusterUnavailable;
        try metadata.claimReplicationSourceCutoverDurable(
            expected_replication_sources_json,
            expected_authority_id,
            record,
        );
    }

    pub fn replicationSourceAuthorityCurrent(
        self: *Fixture,
        expected_replication_sources_json: []const u8,
        expected: metadata_table_manager.ReplicationSourceStatusRecord,
    ) !void {
        const metadata = self.metadata orelse return error.MetadataClusterUnavailable;
        try metadata.replicationSourceAuthorityCurrent(
            expected_replication_sources_json,
            expected,
        );
    }

    pub fn completeReplicationSourceCutoverRetirementDurable(
        self: *Fixture,
        expected: metadata_table_manager.ReplicationSourceStatusRecord,
    ) !void {
        const metadata = self.metadata orelse return error.MetadataClusterUnavailable;
        try metadata.completeReplicationSourceCutoverRetirementDurable(expected);
    }

    pub fn setActiveSplitEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.active_split_enabled = enabled;
    }

    pub fn setGraphEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.graph_enabled = enabled;
    }

    pub fn setGraphHydrationEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.graph_hydration_enabled = enabled;
    }

    pub fn setGraphCancellationEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.graph_cancellation_enabled = enabled;
    }

    pub fn setGraphInflightAuthorizationRevocationEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.graph_inflight_authorization_revocation_enabled = enabled;
    }

    pub fn setGraphStaleSnapshotRetryExhaustionEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.graph_stale_snapshot_retry_exhaustion_enabled = enabled;
    }

    pub fn setJoinEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.join_enabled = enabled;
    }

    pub fn setGlobalQueryEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.global_query_enabled = enabled;
    }

    pub fn setGlobalQueryCancellationEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.global_query_cancellation_enabled = enabled;
    }

    pub fn setGlobalQueryAuthorizationRevocationEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.global_query_authorization_revocation_enabled = enabled;
    }

    pub fn setGlobalQueryTransportFailureEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.global_query_transport_failure_enabled = enabled;
    }

    pub fn setGlobalQueryOwnerRestartEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.global_query_owner_restart_enabled = enabled;
    }

    fn liveAuthorizationEnabled(self: *const Fixture) bool {
        return self.graph_inflight_authorization_revocation_enabled or
            self.global_query_authorization_revocation_enabled;
    }

    pub fn setJoinCancellationEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.join_cancellation_enabled = enabled;
    }

    pub fn setJoinCancellationOverlapEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.join_cancellation_overlap_enabled = enabled;
    }

    pub fn setJoinCancellationOwnerRestartEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.join_cancellation_owner_restart_enabled = enabled;
    }

    pub fn setJoinWorkerRetryEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.join_worker_retry_enabled = enabled;
    }

    pub fn setJoinOwnerRestartEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.join_owner_restart_enabled = enabled;
    }

    pub fn setJoinRetryExhaustionEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.join_retry_exhaustion_enabled = enabled;
    }

    pub fn setJoinOwnerRestartFaultObserver(
        self: *Fixture,
        observer: JoinOwnerRestartFaultObserver,
    ) void {
        std.debug.assert(self.phase == .leaders_ready);
        std.debug.assert(self.join_owner_restart_enabled);
        self.join_owner_restart_fault_observer = observer;
    }

    pub fn setJoinRetryExhaustionFaultObserver(
        self: *Fixture,
        observer: JoinRetryExhaustionFaultObserver,
    ) void {
        std.debug.assert(self.phase == .leaders_ready);
        std.debug.assert(self.join_retry_exhaustion_enabled);
        self.join_retry_exhaustion_fault_observer = observer;
    }

    pub fn setJoinCancellationOverlapFaultObserver(
        self: *Fixture,
        observer: JoinCancellationOverlapFaultObserver,
    ) void {
        std.debug.assert(self.phase == .leaders_ready);
        std.debug.assert(self.join_cancellation_enabled);
        std.debug.assert(self.join_cancellation_overlap_enabled);
        self.join_cancellation_overlap_fault_observer = observer;
    }

    pub fn setFaultMode(self: *Fixture, mode: FaultMode) void {
        std.debug.assert(self.phase == .created);
        self.fault_mode = mode;
    }

    pub fn setWorkCostPorts(self: *Fixture, ports: WorkCostPorts) void {
        std.debug.assert(self.phase == .created);
        self.work_cost_ports = ports;
    }

    pub fn setCompletionFence(self: *Fixture, fence: CompletionFence) void {
        self.completion_fence = fence;
    }

    pub fn setReplicationOwnerRestartEnabled(self: *Fixture, enabled: bool) void {
        std.debug.assert(self.phase == .created);
        self.replication_owner_restart_enabled = enabled;
    }

    pub fn replicationOwnerRestartFailureCode(self: *const Fixture) u64 {
        return if (self.replication_owner_restart_failure) |err| @intFromError(err) else 0;
    }

    /// Production target for composed replication-backfill histories. The
    /// runner supplies typed BatchRequest values; this adapter preserves them
    /// as public batch JSON and deliberately crosses the ordinary HTTP router,
    /// DataServer leader forwarding, data-Raft apply, and index visibility
    /// path. It is not a direct DB test hook.
    pub fn replicationWriteSource(self: *Fixture) api_table_write_source.TableWriteSource {
        return .{ .ptr = self, .vtable = &.{ .batch = applyReplicationBatch } };
    }

    fn applyReplicationBatch(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table_name: []const u8,
        req: db_types.BatchRequest,
    ) !?void {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        if (req.graph_writes.len != 0 or req.graph_deletes.len != 0 or
            req.predicates.len != 0 or
            req.split_checkpoint != null or req.split_replication != null or
            req.split_transition != null or req.merge_checkpoint != null or
            req.merge_replication != null or req.merge_source_transition != null or
            req.transaction != null)
            return error.UnsupportedReplicationPublicBatchShape;
        const body_slice = try api_batch.encodeBatchRequest(allocator, req);
        defer allocator.free(body_slice);

        if (self.replication_owner_restart_enabled and
            self.replication_batch_successes == 1 and
            !self.replication_owner_restart_injected)
        {
            try self.injectReplicationOwnerRestart(allocator, table_name, body_slice);
            unreachable;
        }

        const route_index: usize = @intCast(self.replication_batch_attempts % self.data_api_uri_count);
        self.replication_batch_attempts +|= 1;
        var response = try self.client.fetchBatch(
            self.data_api_uris[route_index],
            table_name,
            body_slice,
        );
        defer response.deinit(self.alloc);
        self.replication_last_status = response.status;
        self.replication_batch_successes +|= 1;
        return {};
    }

    fn injectReplicationOwnerRestart(
        self: *Fixture,
        allocator: std.mem.Allocator,
        table_name: []const u8,
        body: []const u8,
    ) !void {
        // The replication completion fence keeps every production owner live
        // after the ordinary public workload reaches its terminal graph
        // boundary. Crash the target there: no unrelated request is borrowing
        // the DataServer, while Raft drivers and all recovery owners remain
        // active for the replication retry.
        for (0..100_000) |_| {
            if (self.phase == .graph_query_complete) break;
            if (self.failure) |err| return err;
            if (self.teardown_started) return error.Canceled;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        } else return error.ProductionReplicationOwnerRestartBoundaryTimeout;

        const target_index = self.currentDataLeaderIndex(
            metadata_sim.VoprPublicClusterFixture.data_group_id,
        ) orelse return error.ProductionReplicationOwnerUnavailable;
        const stopped_uri = try allocator.dupe(u8, self.data_api_uris[target_index]);
        defer allocator.free(stopped_uri);

        self.replication_owner_restart_injected = true;
        self.replication_owner_restart_target_index = target_index;
        self.stopDataServerForRestart(target_index) catch |err| {
            self.replication_owner_restart_failure = err;
            return err;
        };
        self.replication_owner_restart_down = true;
        self.replication_batch_attempts +|= 1;

        var unexpected_response = self.client.fetchBatch(
            stopped_uri,
            table_name,
            body,
        ) catch |request_err| {
            self.replication_owner_restart_rejected = true;
            self.replication_owner_restart_error_code = @intFromError(request_err);
            self.reconstructReplicationOwner(target_index) catch |restart_err| {
                self.replication_owner_restart_failure = restart_err;
                return restart_err;
            };
            return request_err;
        };
        defer unexpected_response.deinit(self.alloc);
        self.replication_batch_successes +|= 1;
        self.replication_last_status = unexpected_response.status;
        self.reconstructReplicationOwner(target_index) catch |restart_err| {
            self.replication_owner_restart_failure = restart_err;
            return restart_err;
        };
        return error.ProductionReplicationStoppedOwnerAcceptedBatch;
    }

    fn reconstructReplicationOwner(self: *Fixture, target_index: usize) !void {
        try self.restartDataServer(target_index);
        self.replication_owner_restart_reconstructed = true;
        if (!try self.waitForReplicationOwnerRecovery(target_index))
            return error.ProductionReplicationOwnerPublicReadTimeout;
    }

    fn waitForReplicationOwnerRecovery(self: *Fixture, target_index: usize) !bool {
        // The first replication row is durably committed before the crash, but
        // its derived public index is advanced by the next successful batch.
        // Do not deadlock that batch behind an impossible queryability fence.
        // Instead prove both halves independently before retrying: the exact
        // reconstructed process recovered doc:d from its local group, and its
        // rebound public listener can serve an already-indexed durable row.
        // replicationBackfillVisible later requires doc:d/doc:e/doc:f through
        // every public coordinator after the durable runner has resumed.
        // A process restart invalidates pooled connections by definition. Use
        // a fresh external client with bounded transport deadlines for the
        // readiness proof; the long-lived workload client still performs the
        // failed target attempt, durable retry, and final all-coordinator
        // visibility checks.
        var probe_executor = io_http_executor.IoHttpExecutor.init(self.alloc, self.sim.io(), .{
            .keep_alive = false,
            .connect_timeout_ms = 100,
            .read_timeout_ms = 100,
            .write_timeout_ms = 100,
            .pool_max_connections = 1,
            .pool_max_per_host = 1,
        });
        defer probe_executor.deinit();
        var probe_authorization = PublicAuthorizationExecutor{
            .inner = probe_executor.executor(),
            .authorization = graph_authorization_header,
        };
        var probe_client = api_http_client.ApiHttpClient.init(
            self.alloc,
            if (self.liveAuthorizationEnabled())
                probe_authorization.iface()
            else
                probe_executor.executor(),
        );

        for (0..32) |_| {
            if (!self.replication_owner_restart_durable_row_recovered) {
                const recovered = self.data_servers[target_index].read_source.source().lookupGroupLocal(
                    self.alloc,
                    metadata_sim.VoprPublicClusterFixture.data_group_id,
                    "docs",
                    "doc:d",
                    .{},
                    .stale,
                ) catch null;
                if (recovered) |recovered_value| {
                    var lookup = recovered_value;
                    defer lookup.deinit(self.alloc);
                    self.replication_owner_restart_durable_row_recovered =
                        std.mem.indexOf(u8, lookup.json, "alpha") != null;
                }
            }

            if (!self.replication_owner_restart_direct_read) {
                self.replication_owner_restart_direct_read_attempts +|= 1;
                var response = probe_client.fetchLookupResponse(
                    self.data_api_uris[target_index],
                    "docs",
                    "doc:c",
                    null,
                ) catch |err| {
                    self.replication_owner_restart_direct_read_error_code = @intFromError(err);
                    switch (err) {
                        error.ConnectionRefused,
                        error.ConnectionResetByPeer,
                        error.EndOfStream,
                        error.SendFailed,
                        => {
                            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
                            continue;
                        },
                        else => return err,
                    }
                };
                defer response.deinit(self.alloc);
                self.replication_owner_restart_direct_read_status = response.status;
                self.replication_owner_restart_direct_read_error_code = 0;
                self.replication_owner_restart_direct_read = response.status == 200 and
                    std.mem.indexOf(u8, response.body, "production-left") != null;
            }

            if (self.replication_owner_restart_durable_row_recovered and
                self.replication_owner_restart_direct_read)
                return true;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        }
        return false;
    }

    pub fn replicationOwnerRestartSound(self: *const Fixture) bool {
        return self.replication_owner_restart_enabled and
            self.replication_owner_restart_injected and
            self.replication_owner_restart_down and
            self.replication_owner_restart_rejected and
            self.replication_owner_restart_error_code == @intFromError(error.SendFailed) and
            self.replication_owner_restart_reconstructed and
            self.replication_owner_restart_durable_row_recovered and
            self.replication_owner_restart_direct_read and
            self.replication_owner_restart_failure == null;
    }

    pub fn replicationBackfillVisible(self: *Fixture) !bool {
        const expected = [_]struct {
            key: []const u8,
            value: []const u8,
        }{
            .{ .key = "doc:d", .value = "alpha" },
            .{ .key = "doc:e", .value = "beta" },
            .{ .key = "doc:f", .value = "gamma" },
        };
        for (0..node_count * 64) |_| {
            var visible: usize = 0;
            for (self.data_api_uris[0..self.data_api_uri_count]) |uri| {
                var route_sound = true;
                for (expected) |document| {
                    var response = self.client.fetchLookupResponse(
                        uri,
                        "docs",
                        document.key,
                        null,
                    ) catch {
                        route_sound = false;
                        break;
                    };
                    defer response.deinit(self.alloc);
                    if (response.status != 200 or
                        std.mem.indexOf(u8, response.body, document.value) == null)
                    {
                        route_sound = false;
                        break;
                    }
                }
                if (route_sound) visible += 1;
            }
            if (visible == self.data_api_uri_count) return true;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        }
        return false;
    }

    pub fn currentGraphOwnerIndex(self: *Fixture) ?usize {
        return self.currentDataLeaderIndex(metadata_sim.VoprPublicClusterFixture.graph_data_group_id);
    }

    pub fn currentTenantOwnerIndex(self: *Fixture) ?usize {
        return self.currentDataLeaderIndex(metadata_sim.VoprPublicClusterFixture.tenant_data_group_id);
    }

    /// Freeze one real directional coordinator-to-tenant-owner link for the
    /// deployment manifest. The payload selector installed at the result
    /// boundary later limits the outage to the second table's query stream.
    pub fn configureGlobalQueryTransportTarget(self: *Fixture, target_index: usize) !usize {
        if (self.phase != .leaders_ready or !self.global_query_transport_failure_enabled or
            target_index >= self.data_server_count or !self.data_server_live[target_index])
            return error.InvalidProductionGlobalQueryTransportTarget;
        const coordinator_index = for (0..self.data_api_uri_count) |index| {
            if (index != target_index and self.data_server_live[index]) break index;
        } else return error.ProductionGlobalQueryRemoteCoordinatorMissing;
        self.global_query_transport_target_index = target_index;
        self.global_query_transport_target_configured = true;
        self.global_query_transport_fault_endpoint = try parseHttpBaseUriAddress(self.data_api_uris[target_index]);
        self.global_query_route_index = coordinator_index;
        return coordinator_index;
    }

    /// Select a public coordinator that cannot serve the tenant group locally
    /// and freeze the exact production process domain that will disappear at
    /// the first-result boundary.
    pub fn configureGlobalQueryOwnerRestartTarget(self: *Fixture, target_index: usize) !usize {
        if (self.phase != .leaders_ready or !self.global_query_owner_restart_enabled or
            target_index >= self.data_server_count or !self.data_server_live[target_index])
            return error.InvalidProductionGlobalQueryOwnerRestartTarget;
        const coordinator_index = for (0..self.data_api_uri_count) |index| {
            if (index != target_index and self.data_server_live[index]) break index;
        } else return error.ProductionGlobalQueryRemoteCoordinatorMissing;
        self.global_query_owner_restart_target_index = target_index;
        self.global_query_owner_restart_target_configured = true;
        self.global_query_route_index = coordinator_index;
        return coordinator_index;
    }

    pub fn configureGraphRestartTarget(self: *Fixture, index: usize) !void {
        if (self.phase != .leaders_ready or index >= self.data_server_count or !self.data_server_live[index])
            return error.InvalidProductionGraphRestartTarget;
        self.graph_restart_target_index = index;
        self.graph_restart_target_configured = true;
    }

    /// Select the deterministic right-table group and a public coordinator
    /// that cannot execute that group's worker locally. The exact process is
    /// deliberately selected at the production worker boundary: leadership
    /// may legitimately change while the fixture publishes its 64 durable
    /// rows, so selecting it during deployment registration would be stale.
    fn prepareJoinOwnerRestartCampaign(self: *Fixture) !void {
        if (self.phase != .reads_complete or !self.join_owner_restart_enabled)
            return error.InvalidProductionJoinOwnerRestartTarget;
        const group_id = metadata_sim.VoprPublicClusterFixture.data_group_id;
        const coordinator_index = if (self.join_cancellation_owner_restart_enabled)
            1
        else for (self.data_servers[0..self.data_server_count], 0..) |*server, index| {
            const raft = server.data_raft orelse continue;
            if (raft.host.http_host.host.raftStatus(group_id) == null) break index;
        } else return error.ProductionDataJoinRemoteCoordinatorMissing;
        self.join_owner_restart_coordinator_index = coordinator_index;
        self.join_owner_restart_failed_group_id = group_id;
        self.join_owner_restart_campaign_configured = true;
    }

    /// Select a public coordinator that is the live local leader for the
    /// shuffle's first worker group and a distinct live leader for its retry
    /// group. The first attempt can therefore enter the real local worker
    /// boundary under resource exhaustion, while the second attempt must
    /// traverse one exact directional production HTTP link.
    fn prepareJoinRetryExhaustionCampaign(self: *Fixture) !void {
        if (self.phase != .reads_complete or !self.join_retry_exhaustion_enabled)
            return error.InvalidProductionJoinRetryExhaustionTarget;
        const first_group_id = metadata_sim.VoprPublicClusterFixture.data_group_id;
        const retry_group_id = metadata_sim.VoprPublicClusterFixture.graph_data_group_id;
        const coordinator_index = self.currentDataLeaderIndex(first_group_id) orelse
            return error.ProductionDataJoinFirstLeaderMissing;
        const retry_target_index = self.currentDataLeaderIndex(retry_group_id) orelse
            return error.ProductionDataJoinRetryLeaderMissing;
        if (coordinator_index == retry_target_index)
            return error.ProductionDataJoinRetryRemoteOwnerMissing;
        self.join_retry_exhaustion_first_group_id = first_group_id;
        self.join_retry_exhaustion_retry_group_id = retry_group_id;
        self.join_retry_exhaustion_coordinator_index = coordinator_index;
        self.join_retry_exhaustion_retry_target_index = retry_target_index;
        self.join_retry_exhaustion_campaign_configured = true;
    }

    /// Route the first durable partition attempt across one exact production
    /// link and make the second worker local to the public coordinator. The
    /// first attempt can then be rejected by the registered network fault,
    /// while cancellation is held at a real alternate-worker boundary under
    /// all-owner memory saturation.
    fn prepareJoinCancellationOverlapCampaign(self: *Fixture) !void {
        if (self.phase != .reads_complete or !self.join_cancellation_enabled or
            !self.join_cancellation_overlap_enabled)
            return error.InvalidProductionJoinCancellationOverlapTarget;
        const first_group_id = metadata_sim.VoprPublicClusterFixture.data_group_id;
        const worker_group_id = metadata_sim.VoprPublicClusterFixture.graph_data_group_id;
        const network_target_index = self.currentDataLeaderIndex(first_group_id) orelse
            return error.ProductionDataJoinFirstLeaderMissing;
        const coordinator_index = self.currentDataLeaderIndex(worker_group_id) orelse
            return error.ProductionDataJoinRetryLeaderMissing;
        if (coordinator_index == network_target_index)
            return error.ProductionDataJoinCancellationOverlapRemoteOwnerMissing;
        self.join_cancellation_overlap_first_group_id = first_group_id;
        self.join_cancellation_overlap_worker_group_id = worker_group_id;
        self.join_cancellation_overlap_coordinator_index = coordinator_index;
        self.join_cancellation_overlap_network_target_index = network_target_index;
        self.join_cancellation_overlap_campaign_configured = true;
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

    /// Freeze the production coordinator-to-owner link represented by an
    /// endpoint-scoped graph transport fault. The workload rejects leadership
    /// drift instead of applying a fault outside its registered domain.
    pub fn configureGraphTransportTarget(self: *Fixture, target_index: usize) !usize {
        if (self.phase != .leaders_ready or target_index >= self.data_server_count or !self.data_server_live[target_index])
            return error.InvalidProductionGraphTransportTarget;
        const start_index = self.currentDataLeaderIndex(metadata_sim.VoprPublicClusterFixture.data_group_id) orelse
            return error.ProductionDataGraphLeaderMissing;
        const coordinator_index = for (0..self.data_api_uri_count) |index| {
            if (index != start_index and index != target_index) break index;
        } else return error.ProductionDataGraphRemoteCoordinatorMissing;
        self.graph_transport_target_index = target_index;
        self.graph_transport_target_configured = true;
        self.graph_transport_fault_endpoint = try parseHttpBaseUriAddress(self.data_api_uris[target_index]);
        self.graph_probe_route_index = coordinator_index;
        return coordinator_index;
    }

    /// Freeze the production node whose public listener owns the modeled
    /// descriptor-pressure domain. The workload later rejects new connections
    /// at this exact endpoint while established Raft and control streams remain
    /// available.
    pub fn configureSocketPressureTarget(self: *Fixture, target_index: usize) !void {
        if (self.phase != .leaders_ready or target_index >= self.data_server_count or !self.data_server_live[target_index])
            return error.InvalidProductionSocketPressureTarget;
        self.socket_pressure_target_index = target_index;
        self.socket_pressure_target_configured = true;
    }

    pub fn bootstrap(self: *Fixture) !void {
        if (self.phase != .created) return error.ProductionFixtureAlreadyBootstrapped;
        if (self.graph_hydration_enabled and !self.graph_enabled)
            return error.InvalidProductionClusterGraphHydrationMode;
        if (self.graph_cancellation_enabled and !self.graph_enabled)
            return error.InvalidProductionClusterGraphCancellationMode;
        if (self.graph_inflight_authorization_revocation_enabled and !self.graph_enabled)
            return error.InvalidProductionClusterGraphAuthorizationMode;
        if (self.global_query_authorization_revocation_enabled and
            (!self.global_query_enabled or self.global_query_cancellation_enabled))
            return error.InvalidProductionClusterGlobalQueryAuthorizationMode;
        if (self.global_query_transport_failure_enabled and
            (!self.global_query_enabled or self.global_query_cancellation_enabled or
                self.global_query_authorization_revocation_enabled))
            return error.InvalidProductionClusterGlobalQueryTransportMode;
        if (self.global_query_owner_restart_enabled and
            (!self.global_query_enabled or self.global_query_cancellation_enabled or
                self.global_query_authorization_revocation_enabled or
                self.global_query_transport_failure_enabled))
            return error.InvalidProductionClusterGlobalQueryOwnerRestartMode;
        if (self.graph_stale_snapshot_retry_exhaustion_enabled and
            (!self.graph_enabled or !self.active_split_enabled))
            return error.InvalidProductionClusterGraphStaleSnapshotMode;
        if (self.join_cancellation_enabled and
            (!self.join_enabled or self.active_split_enabled or self.fault_mode != .clean))
            return error.InvalidProductionClusterJoinCancellationMode;
        if (self.join_cancellation_overlap_enabled and !self.join_cancellation_enabled)
            return error.InvalidProductionClusterJoinCancellationOverlapMode;
        if (self.join_cancellation_owner_restart_enabled and
            (!self.join_cancellation_enabled or !self.join_owner_restart_enabled))
            return error.InvalidProductionClusterJoinCancellationOwnerRestartMode;
        if (self.join_worker_retry_enabled and
            (!self.join_enabled or self.join_cancellation_enabled or
                self.active_split_enabled or self.fault_mode != .clean))
            return error.InvalidProductionClusterJoinWorkerRetryMode;
        if (self.join_owner_restart_enabled and
            (!self.join_enabled or
                (self.join_cancellation_enabled and !self.join_cancellation_owner_restart_enabled) or
                self.join_worker_retry_enabled or self.active_split_enabled or
                self.fault_mode != .clean))
            return error.InvalidProductionClusterJoinOwnerRestartMode;
        if (self.join_retry_exhaustion_enabled and
            (!self.join_enabled or self.join_cancellation_enabled or
                self.join_worker_retry_enabled or self.join_owner_restart_enabled or
                self.active_split_enabled or self.fault_mode != .clean))
            return error.InvalidProductionClusterJoinRetryExhaustionMode;
        switch (self.fault_mode) {
            .clean => {},
            .graph_hydration_transport_failure => if (!self.graph_enabled or
                !self.graph_cancellation_enabled or self.active_split_enabled)
                return error.InvalidProductionClusterFaultMode,
            .join_finalizer_ack_failure => if (!self.join_enabled or self.active_split_enabled)
                return error.InvalidProductionClusterFaultMode,
            else => if (!self.graph_enabled or !self.active_split_enabled)
                return error.InvalidProductionClusterFaultMode,
        }
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
        if (self.liveAuthorizationEnabled()) {
            self.auth_store = usermgr.MemoryStore.init(alloc);
            self.auth_store_live = true;
            self.auth_policy_store = casbin.MemoryAdapter.init(alloc);
            self.auth_policy_store_live = true;
            self.auth_manager = try usermgr.UserManager.initWithIo(
                alloc,
                self.sim.io(),
                self.auth_store.iface(),
                try usermgr.initDefaultEnforcer(alloc, self.auth_policy_store.iface()),
            );
            self.auth_manager_live = true;
            var docs_read = try usermgr.Permission.initOwned(alloc, .table, "docs", .read);
            defer docs_read.deinit(alloc);
            var docs_write = try usermgr.Permission.initOwned(alloc, .table, "docs", .write);
            defer docs_write.deinit(alloc);
            var tenant_read = try usermgr.Permission.initOwned(alloc, .table, "tenant_b_docs", .read);
            defer tenant_read.deinit(alloc);
            var tenant_write = try usermgr.Permission.initOwned(alloc, .table, "tenant_b_docs", .write);
            defer tenant_write.deinit(alloc);
            const permissions = [_]usermgr.Permission{
                docs_read,
                docs_write,
                tenant_read,
                tenant_write,
            };
            var user = try self.auth_manager.createUser(
                graph_authorization_username,
                graph_authorization_password,
                &permissions,
            );
            user.deinit(alloc);
            self.public_authorization_executor = .{
                .inner = self.public_executor.executor(),
                .authorization = graph_authorization_header,
            };
        }
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
            self.join_job_backends[index] = mem_backend.Backend.init(alloc, .{});
            self.join_job_backend_count += 1;
            self.join_job_stores[index] = try self.join_job_backends[index].runtimeStore(alloc, .{
                .name = "system/distributed-join-jobs",
            });
            self.join_job_store_count += 1;
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
        self.client = api_http_client.ApiHttpClient.init(
            alloc,
            if (self.liveAuthorizationEnabled())
                self.public_authorization_executor.iface()
            else
                self.public_executor.executor(),
        );
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
            .h1_disconnect_probe = self.http_disconnect_probe.iface(),
            .api_server_cfg = .{
                .auth_enabled = self.liveAuthorizationEnabled(),
                .user_manager = if (self.liveAuthorizationEnabled()) &self.auth_manager else null,
                .internal_service_secret = internal_service_secret,
                .internal_service_issuer = internal_service_issuer,
                .distributed_join_lifecycle_hook = .{
                    .ptr = &self.join_lifecycle_observers[index],
                    .reach_fn = observeDistributedJoinLifecycle,
                },
                .join_job_store = &self.join_job_stores[index],
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
            .work_cost_port = if (self.work_cost_ports) |ports| ports.data[index] else null,
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
        _ = self.data_servers[index].read_source.withDistributedGraphWorkCostPort(
            if (self.work_cost_ports) |ports| ports.graph[index] else null,
        );
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
        if (!self.data_servers[index].http_server.?.join_job_store.hasDurableStore())
            return error.DurableJoinStoreUnavailable;
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
        if (event.phase == .query_result_assembled and
            std.mem.eql(u8, event.operation_id, "public.global.multi_query"))
        {
            self.global_query_result_assembled_count +|= 1;
            if (self.global_query_cancellation_enabled and
                self.global_query_cancellation_armed and
                std.mem.eql(u8, event.table_name, "docs"))
            {
                self.global_query_cancellation_armed = false;
                self.global_query_cancellation_boundary_observed = true;
                self.global_query_cancellation_boundary.post(self.sim.io());
                try self.global_query_cancellation_release.wait(self.sim.io());
            }
            if (self.global_query_authorization_revocation_enabled and
                self.global_query_authorization_revocation_armed and
                std.mem.eql(u8, event.table_name, "docs"))
            {
                self.global_query_authorization_revocation_armed = false;
                self.global_query_authorization_boundary_observed = true;
                try self.auth_manager.removePermissionFromUser(
                    graph_authorization_username,
                    "tenant_b_docs",
                    .table,
                );
                self.global_query_authorization_revoked = true;
            }
            if (self.global_query_transport_failure_enabled and
                self.global_query_transport_failure_armed and
                std.mem.eql(u8, event.table_name, "docs"))
            {
                const endpoint = self.global_query_transport_fault_endpoint orelse
                    return error.ProductionGlobalQueryTransportFaultEndpointMissing;
                self.global_query_transport_failure_armed = false;
                self.global_query_transport_boundary_observed = true;
                self.global_query_transport_fault_count_before =
                    self.sim.outboundEndpointPayloadOutageCount();
                try self.sim.setOutboundEndpointPayloadOutage(
                    endpoint,
                    "/tables/tenant_b_docs/query",
                );
                self.global_query_transport_fault_injected = true;
            }
            if (self.global_query_owner_restart_enabled and
                self.global_query_owner_restart_armed and
                std.mem.eql(u8, event.table_name, "docs"))
            {
                if (!self.global_query_owner_restart_target_configured)
                    return error.ProductionGlobalQueryOwnerRestartTargetMissing;
                if (self.currentTenantOwnerIndex() !=
                    self.global_query_owner_restart_target_index)
                    return error.ProductionGlobalQueryOwnerRestartTargetLeadershipChanged;
                self.global_query_owner_restart_armed = false;
                self.global_query_owner_restart_boundary_observed = true;
                self.global_query_restart_requested.post(self.sim.io());
                try self.global_query_restart_down.wait(self.sim.io());
                if (self.global_query_owner_restart_failure) |err| return err;
            }
        }
    }

    fn observePublicRequestLifecycle(
        ptr: *anyopaque,
        event: api_http_server.RequestLifecycleEvent,
    ) !void {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        if (event.phase == .ingress) self.public_request_ingress_count +|= 1;
        if (event.phase == .response_ready) self.public_response_ready_count +|= 1;
    }

    fn observeDistributedGraphLifecycle(
        ptr: *anyopaque,
        event: api_distributed_graph.LifecycleEvent,
    ) void {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        switch (event.phase) {
            .source_snapshot_acquired => {
                if (self.graph_stale_snapshot_retry_exhaustion_enabled and
                    self.graph_stale_snapshot_armed)
                {
                    self.graph_stale_snapshot_armed = false;
                    self.graph_stale_snapshot_boundary_observed = true;
                    self.publishSplitAtStaleGraphBoundary() catch |err| {
                        self.failure = err;
                        return;
                    };
                }
            },
            .target_authorization_started => {
                if (self.graph_inflight_authorization_revocation_enabled and
                    self.graph_authorization_revocation_armed and
                    std.mem.eql(u8, event.table_name, "tenant_b_docs"))
                {
                    self.graph_authorization_boundary_observed = true;
                    self.graph_authorization_revocation_armed = false;
                    self.auth_manager.removePermissionFromUser(
                        graph_authorization_username,
                        "tenant_b_docs",
                        .table,
                    ) catch |err| {
                        self.failure = err;
                        return;
                    };
                    self.graph_authorization_revoked = true;
                }
            },
            .hydration_started => self.graph_hydration_started_count +|= 1,
            .hydration_fanout_started => {
                self.graph_hydration_fanout_started_count +|= 1;
                if (self.graph_cancellation_enabled and
                    self.graph_hydration_fanout_started_count == 1)
                {
                    if (self.fault_mode == .graph_hydration_transport_failure) {
                        const endpoint = self.graph_transport_fault_endpoint orelse {
                            self.failure = error.ProductionGraphCancellationFaultEndpointMissing;
                            return;
                        };
                        self.graph_cancellation_fault_count_before =
                            self.sim.outboundEndpointPayloadOutageCount();
                        self.sim.setOutboundEndpointPayloadOutage(
                            endpoint,
                            "/graph-hydrate",
                        ) catch |err| {
                            self.failure = err;
                            return;
                        };
                        self.graph_cancellation_fault_injected = true;
                    }
                    const cancellation = event.cancellation orelse {
                        self.failure = error.ProductionGraphCancellationTokenMissing;
                        return;
                    };
                    const cancellation_wait_rounds: usize =
                        if (self.fault_mode == .graph_hydration_transport_failure) 1_000 else 250;
                    for (0..cancellation_wait_rounds) |_| {
                        if (cancellation.isCancelled()) break;
                        self.sim.io().sleep(.fromMilliseconds(1), .awake) catch return;
                    } else {
                        self.failure = error.ProductionGraphCancellationNotObserved;
                    }
                }
            },
            .hydration_completed => self.graph_hydration_completed_count +|= 1,
            .attempt_failed => {
                if (self.graph_stale_snapshot_retry_exhaustion_enabled and
                    self.graph_stale_snapshot_boundary_observed)
                {
                    self.graph_stale_snapshot_attempt_failures +|= 1;
                    self.graph_stale_snapshot_error_code = event.error_code;
                }
            },
            else => {},
        }
        if (self.fault_mode == .clean or self.fault_mode == .graph_hydration_transport_failure or
            self.fault_mode == .resource_pressure or
            self.fault_mode == .socket_pressure or
            self.fault_mode == .join_finalizer_ack_failure) return;
        switch (event.phase) {
            .expand_round_completed => {
                if (!self.graph_transport_fault_armed or event.depth != 1) return;
                switch (self.fault_mode) {
                    .clean => unreachable,
                    .graph_transport_failure, .graph_transport_resource_pressure => {
                        if (self.graph_transport_failure_injected) return;
                        self.graph_transport_fault_armed = false;
                        if (self.fault_mode.hasResourcePressure()) {
                            self.saturateNodeMemory() catch |err| {
                                self.failure = err;
                                return;
                            };
                            if (!self.allNodeMemorySaturated()) {
                                self.failure = error.ProductionDataResourceEnvelopeNotSaturated;
                                self.releaseNodeMemory();
                                return;
                            }
                        }
                        const endpoint = self.graph_transport_fault_endpoint orelse return;
                        self.sim.setOutboundEndpointPayloadOutage(endpoint, "/graph-expand") catch unreachable;
                        self.graph_transport_failure_injected = true;
                        self.overlapping_faults_active_observed =
                            self.fault_mode == .graph_transport_resource_pressure and
                            self.allNodeMemorySaturated();
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
                    .graph_hydration_transport_failure, .resource_pressure, .socket_pressure, .join_finalizer_ack_failure => unreachable,
                }
            },
            .attempt_failed => {
                switch (self.fault_mode) {
                    .clean => unreachable,
                    .graph_transport_failure, .graph_transport_resource_pressure => {
                        if (!self.graph_transport_failure_injected or self.graph_transport_failure_observed) return;
                        self.graph_transport_failure_observed = true;
                        self.graph_transport_failure_error_code = event.error_code;
                        if (self.fault_mode.hasResourcePressure()) {
                            self.overlapping_faults_active_observed =
                                self.overlapping_faults_active_observed and
                                self.allNodeMemorySaturated();
                            self.releaseNodeMemory();
                        }
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
                    .graph_hydration_transport_failure, .resource_pressure, .socket_pressure, .join_finalizer_ack_failure => unreachable,
                }
            },
            else => {},
        }
    }

    fn observeDistributedJoinLifecycle(
        ptr: *anyopaque,
        event: api_distributed_join.LifecycleEvent,
    ) !void {
        const observer: *JoinLifecycleObserver = @ptrCast(@alignCast(ptr));
        const self = observer.fixture;
        switch (event.phase) {
            .partition_worker_started => {
                self.join_partition_worker_started_count +|= 1;
                if (self.join_retry_exhaustion_enabled and
                    self.join_retry_exhaustion_recovery_query_active == false)
                {
                    if (self.join_retry_exhaustion_faults_injected) {
                        // A retry can reach another worker before the scoped
                        // stream matcher cuts a later exact-group attempt.
                        // Keep the genuinely saturated resource domain causal
                        // for every worker that does enter; the final oracle
                        // separately requires a matched network attempt.
                        return error.ResourceBudgetExceeded;
                    }
                    if (!self.join_retry_exhaustion_campaign_configured)
                        return error.ProductionJoinRetryExhaustionTargetMissing;
                    if (event.job_id == 0 or
                        event.owner_group_id != self.join_retry_exhaustion_first_group_id or
                        observer.node_index != self.join_retry_exhaustion_coordinator_index)
                    {
                        self.failure = error.ProductionJoinRetryExhaustionIdentityMismatch;
                        return error.ProductionJoinRetryExhaustionIdentityMismatch;
                    }
                    const fault_observer = self.join_retry_exhaustion_fault_observer orelse {
                        self.failure = error.ProductionJoinRetryExhaustionFaultObserverMissing;
                        return error.ProductionJoinRetryExhaustionFaultObserverMissing;
                    };
                    fault_observer.activate(
                        fault_observer.ptr,
                        self.join_retry_exhaustion_coordinator_index,
                        self.join_retry_exhaustion_retry_target_index,
                    ) catch |err| {
                        self.failure = err;
                        return err;
                    };
                    self.join_retry_exhaustion_network_matches_before =
                        self.sim.outboundEndpointPayloadOutageCount();
                    const retry_endpoint = try parseHttpBaseUriAddress(
                        self.data_api_uris[self.join_retry_exhaustion_retry_target_index],
                    );
                    self.sim.setOutboundEndpointPayloadOutage(
                        retry_endpoint,
                        "/join-partition",
                    ) catch |err| {
                        self.failure = err;
                        return err;
                    };
                    self.saturateNodeMemory() catch |err| {
                        self.sim.setOutboundEndpointOutage(null);
                        self.failure = err;
                        return err;
                    };
                    self.join_retry_exhaustion_job_id = event.job_id;
                    self.join_retry_exhaustion_partition_index = event.partition_index;
                    self.join_retry_exhaustion_resource_observed = self.allNodeMemorySaturated();
                    self.join_retry_exhaustion_faults_injected = true;
                    if (!self.join_retry_exhaustion_resource_observed) {
                        self.failure = error.ProductionDataResourceEnvelopeNotSaturated;
                        return error.ProductionDataResourceEnvelopeNotSaturated;
                    }
                    // Fail the first production worker at its operation
                    // boundary while the resource domain is genuinely full.
                    // The next exact-group attempt must cross the scoped HTTP
                    // stream above and exhaust the complete public operation.
                    return error.ResourceBudgetExceeded;
                }
                if (self.join_owner_restart_enabled) {
                    if (!self.join_owner_restart_requested) {
                        if (!self.join_owner_restart_campaign_configured)
                            return error.ProductionJoinOwnerRestartTargetMissing;
                        if (event.owner_group_id != self.join_owner_restart_failed_group_id) return;
                        if (event.job_id == 0) {
                            self.failure = error.ProductionJoinOwnerRestartIdentityMissing;
                            return error.ProductionJoinOwnerRestartIdentityMissing;
                        }
                        if (observer.node_index == self.join_owner_restart_coordinator_index) {
                            self.failure = error.ProductionDataJoinRemoteCoordinatorMissing;
                            return error.ProductionDataJoinRemoteCoordinatorMissing;
                        }
                        self.join_owner_restart_target_index = observer.node_index;
                        self.join_owner_restart_target_configured = true;
                        const fault_observer = self.join_owner_restart_fault_observer orelse {
                            self.failure = error.ProductionJoinOwnerRestartFaultObserverMissing;
                            return error.ProductionJoinOwnerRestartFaultObserverMissing;
                        };
                        fault_observer.activate(fault_observer.ptr, observer.node_index) catch |err| {
                            self.failure = err;
                            return err;
                        };
                        self.join_owner_restart_job_id = event.job_id;
                        self.join_owner_restart_partition_index = event.partition_index;
                        self.join_owner_restart_requested = true;
                        if (!self.join_cancellation_owner_restart_enabled) {
                            self.join_restart_requested.post(self.sim.io());
                            // Return immediately so the serving handler can
                            // unwind while the restart owner tears down this
                            // exact process.
                            return error.GroupLeaderUnavailable;
                        }
                    }
                    if (!self.join_cancellation_owner_restart_enabled and
                        self.join_owner_restart_recovered_group_id == 0 and
                        event.partition_index == self.join_owner_restart_partition_index and
                        event.owner_group_id != self.join_owner_restart_failed_group_id)
                    {
                        if (!self.join_owner_restart_down)
                            self.join_restart_down.wait(self.sim.io()) catch return;
                        if (observer.node_index == self.join_owner_restart_target_index)
                            return error.ProductionJoinOwnerRestartRetriedOnStoppedProcess;
                        self.join_owner_restart_recovered_group_id = event.owner_group_id;
                        self.join_owner_restart_recovery_index = observer.node_index;
                        self.join_restart_recover.post(self.sim.io());
                        self.join_restart_recovered.wait(self.sim.io()) catch return;
                        if (!self.join_owner_restart_recovered)
                            return error.ProductionJoinOwnerReconstructionMissing;
                    }
                    // The process failure invalidates the complete public
                    // operation. Once a different worker and reconstructed
                    // process have been observed, fence every remaining
                    // initial-request attempt so the coordinator exhausts as
                    // retryable unavailability without publishing a prefix.
                    // A fresh idempotent public request runs with this fence
                    // disabled and must return the complete result.
                    if (self.join_owner_restart_initial_query_active)
                        return error.DistributedQueryUnavailable;
                }
                if (self.join_worker_retry_enabled and
                    !self.join_worker_retry_failure_injected)
                {
                    if (event.job_id == 0 or event.owner_group_id == 0) {
                        self.failure = error.ProductionJoinWorkerRetryIdentityMissing;
                        return error.ProductionJoinWorkerRetryIdentityMissing;
                    }
                    self.join_worker_retry_failure_injected = true;
                    self.join_worker_retry_job_id = event.job_id;
                    self.join_worker_retry_partition_index = event.partition_index;
                    self.join_worker_retry_failed_group_id = event.owner_group_id;
                    // Fail before any right-row collection or result
                    // publication. The production shuffle engine must record
                    // this attempt and retry the same partition elsewhere.
                    return error.GroupLeaderUnavailable;
                }
                if (self.join_cancellation_overlap_enabled and
                    !self.join_cancellation_overlap_faults_injected and
                    self.join_partition_worker_started_count == 1)
                {
                    if (!self.join_cancellation_overlap_campaign_configured or
                        !self.join_cancellation_overlap_network_armed)
                        return error.ProductionJoinCancellationOverlapTargetMissing;
                    if (event.job_id == 0 or
                        event.owner_group_id != self.join_cancellation_overlap_worker_group_id or
                        observer.node_index != self.join_cancellation_overlap_coordinator_index)
                    {
                        self.failure = error.ProductionJoinCancellationOverlapIdentityMismatch;
                        return error.ProductionJoinCancellationOverlapIdentityMismatch;
                    }
                    self.join_cancellation_overlap_network_observed =
                        self.sim.outboundEndpointPayloadOutageCount() >
                        self.join_cancellation_overlap_network_matches_before;
                    self.saturateNodeMemory() catch |err| {
                        self.failure = err;
                        return err;
                    };
                    self.join_cancellation_overlap_resource_observed =
                        self.allNodeMemorySaturated();
                    self.join_cancellation_overlap_observed =
                        self.join_cancellation_overlap_network_observed and
                        self.join_cancellation_overlap_resource_observed;
                    self.join_cancellation_overlap_faults_injected = true;
                    if (!self.join_cancellation_overlap_observed) {
                        self.failure = error.ProductionJoinCancellationOverlapNotObserved;
                        return error.ProductionJoinCancellationOverlapNotObserved;
                    }
                }
                if (!self.join_cancellation_enabled or
                    self.join_partition_worker_started_count != 1)
                    return;
                const cancellation = event.cancellation orelse {
                    self.failure = error.ProductionJoinCancellationTokenMissing;
                    return error.ProductionJoinCancellationTokenMissing;
                };
                if (event.job_id == 0 or event.owner_group_id == 0) {
                    self.failure = error.ProductionJoinCancellationWorkerIdentityMissing;
                    return error.ProductionJoinCancellationWorkerIdentityMissing;
                }
                self.join_cancellation_boundary_observed = true;
                self.join_cancellation_job_id = event.job_id;
                self.join_cancellation_owner_group_id = event.owner_group_id;
                for (0..1_000) |_| {
                    if (cancellation.isCancelled()) return;
                    try self.sim.io().sleep(.fromMilliseconds(1), .awake);
                }
                self.failure = error.ProductionJoinCancellationNotObserved;
                return error.ProductionJoinCancellationNotObserved;
            },
            .partition_worker_completed => {
                self.join_partition_worker_completed_count +|= 1;
                if (self.join_worker_retry_enabled and
                    self.join_worker_retry_failure_injected and
                    self.join_worker_retry_recovered_group_id == 0 and
                    event.partition_index == self.join_worker_retry_partition_index and
                    event.owner_group_id != self.join_worker_retry_failed_group_id)
                {
                    self.join_worker_retry_recovered_group_id = event.owner_group_id;
                }
            },
            .finalizer_result_persisted => {
                if (self.fault_mode != .join_finalizer_ack_failure or
                    self.join_finalizer_ack_failure_injected or
                    event.owner_group_id == 0)
                    return;
                self.join_finalizer_ack_failure_injected = true;
                self.join_finalizer_persisted_group_id = event.owner_group_id;
                // The result and shared ownership record are durable, but the worker
                // process fails before acknowledging the internal finalizer request.
                // The coordinator must hand the stable job to another owner, which
                // imports the cached result instead of repeating completed work.
                return error.InjectedJoinFinalizerAcknowledgementFailure;
            },
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
        // Give independently owned Raft and HTTP tasks a deterministic
        // scheduling boundary before the next status observation. Promoted
        // histories retain their accelerated cadence; the resource campaign
        // uses the managed production cadence because capacity can keep a
        // committed apply pending for the full request deadline.
        try self.sim.io().sleep(.fromMilliseconds(self.driverCadenceMs()), .awake);
    }

    fn driverCadenceMs(self: *const Fixture) i64 {
        return if (self.fault_mode.hasResourcePressure())
            @intCast(raft_runtime_loop.RuntimeCadence.default_raft_tick_ms)
        else
            1;
    }

    fn driveRaft(self: *Fixture, index: usize) void {
        defer self.raft_driver_done[index] = true;
        // Existing promoted histories intentionally use an accelerated 1 ms
        // scheduling quantum. Resource admission can keep a committed apply
        // pending for the full public request deadline; run that campaign at
        // the managed production driver's real default cadence so it tests
        // bounded retries rather than manufacturing thousands of hot-loop
        // attempts that production would never schedule.
        const cadence_ms = self.driverCadenceMs();
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
            self.data_servers[index].runRaftProgressRoundOnly() catch |err| {
                self.raft_driver_active[index] = false;
                self.driver_failure = err;
                self.driver_stop = true;
                return;
            };
            self.raft_driver_active[index] = false;
            self.raft_driver_rounds[index] +|= 1;
            self.sim.io().sleep(.fromMilliseconds(cadence_ms), .awake) catch |err| {
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

    fn driveGlobalQueryOwnerRestart(self: *Fixture) void {
        self.global_query_restart_requested.wait(self.sim.io()) catch return;
        if (self.driver_stop or self.teardown_started) return;
        self.stopDataServerForRestart(self.global_query_owner_restart_target_index) catch |err| {
            self.failGlobalQueryOwnerRestart(err);
            return;
        };
        self.global_query_owner_restart_down = true;
        self.global_query_restart_down.post(self.sim.io());

        self.global_query_restart_recover.wait(self.sim.io()) catch return;
        if (self.teardown_started) return;
        self.restartDataServer(self.global_query_owner_restart_target_index) catch |err| {
            self.failGlobalQueryOwnerRestart(err);
            return;
        };
        self.global_query_owner_restart_direct_read =
            self.waitForGlobalQueryOwnerPublicRead() catch |err| {
                self.failGlobalQueryOwnerRestart(err);
                return;
            };
        if (!self.global_query_owner_restart_direct_read) {
            self.failGlobalQueryOwnerRestart(
                error.ProductionGlobalQueryOwnerPublicReadTimeout,
            );
            return;
        }
        self.global_query_owner_restart_reconstructed = true;
        self.global_query_restart_recovered.post(self.sim.io());
    }

    fn failGlobalQueryOwnerRestart(self: *Fixture, err: anyerror) void {
        self.global_query_owner_restart_failure = err;
        self.driver_failure = err;
        self.global_query_restart_down.post(self.sim.io());
        self.global_query_restart_recovered.post(self.sim.io());
    }

    /// Reconstruction is not ready merely because the stable Raft identity
    /// and routing catalog are live again. Require the rebound public listener
    /// on the exact replacement process to accept and serve a durable read.
    fn waitForGlobalQueryOwnerPublicRead(self: *Fixture) !bool {
        var last_transport_error: ?anyerror = null;
        for (0..1_024) |_| {
            var response = self.client.fetchLookupResponse(
                self.data_api_uris[self.global_query_owner_restart_target_index],
                "tenant_b_docs",
                "tenant:q",
                null,
            ) catch |err| {
                last_transport_error = err;
                try self.sim.io().sleep(.fromMilliseconds(1), .awake);
                continue;
            };
            const status = response.status;
            const sound = status == 200 and
                std.mem.indexOf(u8, response.body, "production-tenant") != null;
            response.deinit(self.alloc);
            if (sound) return true;
            if (status != 200 and status != 404 and status != 409 and
                status != 503 and status != 504)
                return error.UnexpectedHttpStatus;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        }
        if (last_transport_error) |err| return err;
        return false;
    }

    fn driveJoinOwnerRestart(self: *Fixture) void {
        self.join_restart_requested.wait(self.sim.io()) catch return;
        if (self.driver_stop or self.teardown_started) return;
        self.stopDataServerForRestart(self.join_owner_restart_target_index) catch |err| {
            self.failJoinOwnerRestart(err);
            return;
        };
        self.join_owner_restart_down = true;
        self.join_restart_down.post(self.sim.io());

        // Keep the exact serving process absent until the durable coordinator
        // has selected a different group for the same partition. Reconstruct
        // it before allowing that replacement worker to collect rows, so the
        // two-replica source group can recover quorum for the full scan.
        self.join_restart_recover.wait(self.sim.io()) catch return;
        if (self.teardown_started) return;
        self.restartDataServer(self.join_owner_restart_target_index) catch |err| {
            self.failJoinOwnerRestart(err);
            return;
        };
        self.join_owner_restart_recovered = true;
        self.join_restart_recovered.post(self.sim.io());
        // The fixture exposes the production control supervisor as an
        // explicitly driven owner. Keep that owner live across the public
        // recovery request; otherwise a two-voter group can lose the stable
        // leader observed above and remain in pre-candidate indefinitely.
        while (!self.driver_stop and !self.teardown_started and !self.workload_done and
            !self.join_owner_restart_recovery_join)
        {
            self.runOneControlRound() catch |err| {
                self.failJoinOwnerRestart(err);
                return;
            };
            self.sim.io().sleep(.fromMilliseconds(8), .awake) catch return;
        }
    }

    fn failJoinOwnerRestart(self: *Fixture, err: anyerror) void {
        self.join_owner_restart_failure = err;
        self.driver_failure = err;
        self.join_restart_down.post(self.sim.io());
        self.join_restart_recovered.post(self.sim.io());
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
        // A process restart preserves the stable node/store IDs while rotating
        // the reporter incarnation and rebinding its advertised endpoints.
        // Register through the production metadata path so delayed reports
        // from the destroyed process are fenced and leader routing can move.
        try self.data_servers[index].registerNodeIfConfigured();
        for (self.data_servers[0..self.data_server_count], 0..) |*server, server_index| {
            if (!self.data_server_live[server_index]) continue;
            try server.refreshRemoteMetadataSnapshot();
        }
        self.data_server_paused[index] = false;
        try self.waitForStableDataLeaders();
        try self.waitForDataRoutingReady();
    }

    fn waitForDataRoutingReady(self: *Fixture) !void {
        for (0..4_096) |round| {
            if (round % 8 == 0) {
                try self.runOneControlRound();
                try self.metadata.?.cluster.stepAll();
            }
            var all_routes_ready = true;
            for (self.data_servers[0..self.data_server_count], 0..) |*server, source_index| {
                if (!self.data_server_live[source_index]) continue;
                const router = server.read_source.distributed_router orelse {
                    all_routes_ready = false;
                    break;
                };
                groups: for (initial_groups) |group_id| {
                    var route = (try api_table_router.resolveGroupRoute(
                        self.alloc,
                        server.read_source.catalog,
                        router,
                        group_id,
                        .prefer_leader,
                    )) orelse {
                        all_routes_ready = false;
                        break;
                    };
                    defer route.deinit(self.alloc);
                    const routed_index = switch (route) {
                        .local => source_index,
                        .remote => |remote| blk: {
                            if (remote.node_id == 0 or remote.node_id > self.data_server_count) {
                                all_routes_ready = false;
                                break :groups;
                            }
                            break :blk @as(usize, @intCast(remote.node_id - 1));
                        },
                    };
                    if (!self.data_server_live[routed_index] or
                        self.data_servers[routed_index].data_raft == null)
                    {
                        all_routes_ready = false;
                        break;
                    }
                    const routed_status = self.data_servers[routed_index].data_raft.?.host.http_host.host.raftStatus(group_id) orelse {
                        all_routes_ready = false;
                        break;
                    };
                    if (routed_status.soft.role != .leader or
                        routed_status.soft.leader_id == null or
                        routed_status.soft.leader_id.? != routed_status.id)
                    {
                        all_routes_ready = false;
                        break;
                    }
                    const probe: struct {
                        table: []const u8,
                        key: []const u8,
                        expected: []const u8,
                    } = switch (group_id) {
                        metadata_sim.VoprPublicClusterFixture.data_group_id => .{
                            .table = "docs",
                            .key = "doc:c",
                            .expected = "production-left",
                        },
                        metadata_sim.VoprPublicClusterFixture.graph_data_group_id => .{
                            .table = "docs",
                            .key = "doc:x",
                            .expected = "production-right",
                        },
                        metadata_sim.VoprPublicClusterFixture.tenant_data_group_id => .{
                            .table = "tenant_b_docs",
                            .key = "tenant:q",
                            .expected = "production-tenant",
                        },
                        else => unreachable,
                    };
                    var lookup = self.data_servers[routed_index].read_source.source().lookupGroupLocal(
                        self.alloc,
                        group_id,
                        probe.table,
                        probe.key,
                        .{},
                        .stale,
                    ) catch {
                        all_routes_ready = false;
                        break;
                    } orelse {
                        all_routes_ready = false;
                        break;
                    };
                    defer lookup.deinit(self.alloc);
                    if (std.mem.indexOf(u8, lookup.json, probe.expected) == null) {
                        all_routes_ready = false;
                        break;
                    }
                }
                if (!all_routes_ready) break;
            }
            if (all_routes_ready) return;
            if (round % 16 == 15) {
                for (self.data_servers[0..self.data_server_count], 0..) |*server, server_index| {
                    if (!self.data_server_live[server_index]) continue;
                    try server.refreshRemoteMetadataSnapshot();
                }
            }
            if (self.driver_failure) |err| return err;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        }
        return error.ProductionDataRoutingRecoveryTimeout;
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
        if (self.failure == null) self.awaitCompletionFence() catch |err| {
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
        if (self.global_query_restart_future) |*future| {
            if (self.global_query_owner_restart_boundary_observed) {
                if (self.global_query_owner_restart_down and
                    !self.global_query_owner_restart_reconstructed and
                    self.global_query_owner_restart_failure == null)
                {
                    self.global_query_restart_recover.post(self.sim.io());
                }
                future.await(self.sim.io());
            } else {
                future.cancel(self.sim.io());
            }
            self.global_query_restart_future = null;
        }
        if (self.failure == null) self.failure = self.global_query_owner_restart_failure;
        if (self.join_restart_future) |*future| {
            if (self.join_owner_restart_requested) {
                // If the public operation failed before selecting another
                // worker, never leave the production owner intentionally down.
                if (self.join_owner_restart_down and !self.join_owner_restart_recovered and
                    self.join_owner_restart_failure == null)
                {
                    self.join_restart_recover.post(self.sim.io());
                }
                future.await(self.sim.io());
            } else {
                future.cancel(self.sim.io());
            }
            self.join_restart_future = null;
        }
        if (self.failure == null) self.failure = self.join_owner_restart_failure;
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

    fn awaitCompletionFence(self: *Fixture) !void {
        const fence = self.completion_fence orelse return;
        for (0..100_000) |_| {
            if (fence.ready()) return;
            if (self.teardown_started) return error.Canceled;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        }
        return error.ProductionDataExternalCompletionTimeout;
    }

    fn runWorkloadInner(self: *Fixture) !void {
        const left_body = if (self.graph_inflight_authorization_revocation_enabled)
            graph_authorization_left_batch_body
        else if (self.graph_enabled)
            left_batch_body
        else if (self.join_enabled or self.global_query_enabled)
            join_left_batch_body
        else
            ordinary_left_batch_body;
        const right_body = if (self.graph_enabled)
            right_batch_body
        else if (self.join_enabled or self.global_query_enabled)
            join_right_batch_body
        else
            ordinary_right_batch_body;
        const durable_tenant_body = if (self.durableJoinEnabled())
            try self.durableJoinTenantBatchAlloc()
        else
            null;
        defer if (durable_tenant_body) |body| self.alloc.free(body);
        const effective_tenant_body = durable_tenant_body orelse tenant_batch_body;
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
            effective_tenant_body,
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
            effective_tenant_body,
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

        if (self.global_query_enabled) {
            if (!try self.waitForDocIdentityReady("docs", 64) or
                !try self.waitForDocIdentityReady("tenant_b_docs", 64))
                return error.ProductionDataGlobalQueryIdentityPublicationTimeout;
            self.global_query_sound = if (self.global_query_cancellation_enabled)
                try self.runGlobalMultiQueryCancellation()
            else if (self.global_query_authorization_revocation_enabled)
                try self.runGlobalMultiQueryAuthorizationRevocation()
            else if (self.global_query_transport_failure_enabled)
                try self.runGlobalMultiQueryTransportFailure()
            else if (self.global_query_owner_restart_enabled)
                try self.runGlobalMultiQueryOwnerRestart()
            else
                try self.runGlobalMultiQuery();
            if (!self.global_query_sound)
                return error.ProductionDataGlobalQueryFailed;
        }

        if (self.join_enabled) {
            if (!try self.waitForDocIdentityReady("tenant_b_docs", 64) or
                !try self.waitForDocIdentityReady("docs", 64))
                return error.ProductionDataJoinIdentityPublicationTimeout;
            if (self.join_cancellation_enabled) {
                self.join_cancellation_sound = try self.runJoinCancellationQuery();
                self.join_sound = self.join_cancellation_sound;
            } else if (self.join_retry_exhaustion_enabled) {
                self.join_retry_exhaustion_sound = try self.runJoinRetryExhaustionQuery();
                self.join_sound = self.join_retry_exhaustion_sound;
            } else if (self.join_owner_restart_enabled) {
                self.join_restart_future = self.sim.io().async(driveJoinOwnerRestart, .{self});
                self.join_owner_restart_sound = try self.runJoinOwnerRestartQuery();
                self.join_sound = self.join_owner_restart_sound;
            } else if (self.join_worker_retry_enabled) {
                self.join_worker_retry_sound = try self.runJoinQuery();
                self.join_sound = self.join_worker_retry_sound;
            } else {
                self.join_sound = try self.runJoinQuery();
            }
            self.phase = .join_query_complete;
            if (!self.join_sound) return error.ProductionDataDistributedJoinFailed;
        }

        if (self.graph_enabled) {
            if (!try self.waitForDocIdentityReady("docs", 64))
                return error.ProductionDataDocIdentityPublicationTimeout;
            const right_hop_sound = try self.runGraphQuery("doc:x", 1, &.{"doc:k"});
            const round_trip_sound = try self.runGraphQuery("doc:c", 2, &.{ "doc:x", "doc:k" });
            self.graph_sound = right_hop_sound and round_trip_sound;
            self.phase = .graph_query_complete;
            if (!self.graph_sound) return error.ProductionDataGraphQueryFailed;
            if (self.graph_cancellation_enabled) {
                self.graph_cancellation_sound = try self.runGraphCancellationQuery();
                if (!self.graph_cancellation_sound)
                    return error.ProductionDataGraphCancellationQueryFailed;
            } else if (self.graph_inflight_authorization_revocation_enabled) {
                self.graph_authorization_sound = try self.runGraphInflightAuthorizationRevocationQuery();
                if (!self.graph_authorization_sound)
                    return error.ProductionDataGraphAuthorizationMutationFailed;
            } else if (self.graph_stale_snapshot_retry_exhaustion_enabled) {
                self.graph_stale_snapshot_sound = try self.runGraphStaleSnapshotRetryExhaustionQuery();
                if (!self.graph_stale_snapshot_sound)
                    return error.ProductionDataGraphStaleSnapshotRetryExhaustionFailed;
            } else if (self.graph_hydration_enabled) {
                self.graph_hydration_sound = try self.runGraphHydrationQuery();
                if (!self.graph_hydration_sound)
                    return error.ProductionDataGraphHydrationQueryFailed;
            }
        }

        if (!self.active_split_enabled or self.graph_stale_snapshot_retry_exhaustion_enabled) return;

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
        if (self.join_enabled) {
            // The control loop moves the split only when runOneControlRound is
            // explicitly requested. Once this returns, hold the transition in
            // a nonterminal production phase while the public join performs
            // its real left query and cross-owner right-row fanout.
            try self.waitForSplitInProgress();
            self.split_join_sound = try self.runJoinQuery();
            self.phase = .split_join_query_complete;
            if (!self.split_join_sound)
                return error.ProductionDataActiveSplitDistributedJoinFailed;
            // The active-split join deliberately runs before the destination
            // is allowed to affect public routing. Once that observation is
            // complete, invalidate each production owner's metadata snapshot
            // through its ordinary API-backed refresh boundary so this mode
            // exercises destination bootstrap/cutover instead of spending its
            // deep-tier budget re-testing the passive snapshot TTL covered by
            // the base split campaign.
            try self.refreshDataServerMetadataSnapshots();
        }
        if (self.graph_enabled) {
            if (!self.join_enabled) try self.waitForSplitInProgress();
            if (self.fault_mode.hasResourcePressure())
                try self.runResourcePressureDuringSplit();
            if (self.fault_mode == .socket_pressure)
                try self.runSocketPressureDuringSplit();
            const ingress_before = self.public_request_ingress_count;
            self.split_graph_inflight_sound = probe: {
                if (self.fault_mode != .clean and self.fault_mode != .resource_pressure and
                    self.fault_mode != .socket_pressure)
                {
                    const start_leader_index = self.currentDataLeaderIndex(metadata_sim.VoprPublicClusterFixture.data_group_id) orelse
                        return error.ProductionDataGraphLeaderMissing;
                    const target_index = self.currentDataLeaderIndex(metadata_sim.VoprPublicClusterFixture.graph_data_group_id) orelse
                        return error.ProductionDataGraphLeaderMissing;
                    self.graph_probe_route_index = for (0..self.data_api_uri_count) |index| {
                        if (index != start_leader_index and index != target_index) break index;
                    } else return error.ProductionDataGraphRemoteCoordinatorMissing;
                    switch (self.fault_mode) {
                        .clean => unreachable,
                        .graph_transport_failure, .graph_transport_resource_pressure => {
                            if (self.graph_transport_target_configured and
                                self.graph_transport_target_index != target_index)
                                return error.ProductionGraphTransportTargetLeadershipChanged;
                            self.graph_transport_fault_endpoint =
                                try parseHttpBaseUriAddress(self.data_api_uris[target_index]);
                        },
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
                        .graph_hydration_transport_failure, .resource_pressure, .socket_pressure, .join_finalizer_ack_failure => unreachable,
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

        if (self.fault_mode.hasResourcePressure()) {
            self.resource_post_split_sound = try self.lookupContains(
                "docs",
                "pressure:probe",
                "production-owner-resource-recovery",
            );
            if (!self.resource_post_split_sound)
                return error.ProductionDataResourceRecoveryLostAtSplitCutover;
        }

        if (self.join_enabled) {
            self.post_split_join_sound = try self.runJoinQuery();
            self.join_sound = self.join_sound and self.split_join_sound and self.post_split_join_sound;
            self.phase = .post_split_join_query_complete;
            if (!self.post_split_join_sound)
                return error.ProductionDataPostSplitDistributedJoinFailed;
        }

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

    fn durableJoinEnabled(self: *const Fixture) bool {
        return self.fault_mode == .join_finalizer_ack_failure or
            self.join_cancellation_enabled or
            self.join_worker_retry_enabled or
            self.join_owner_restart_enabled or
            self.join_retry_exhaustion_enabled;
    }

    fn durableJoinTenantBatchAlloc(self: *Fixture) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        defer out.deinit();
        try out.writer.writeAll("{\"inserts\":{");
        for (0..durable_join_row_count) |index| {
            if (index != 0) try out.writer.writeByte(',');
            if (index == 0)
                try out.writer.writeAll("\"tenant:q\"")
            else
                // The projected tenant range begins at `tenant:a`. A numeric
                // byte directly after `tenant:` sorts before that boundary
                // and is correctly rejected by public routing as NotFound.
                try out.writer.print("\"tenant:r{d:0>3}\"", .{index});
            try out.writer.print(
                ":{{\"title\":\"production-tenant\",\"body\":\"production durable join left\",\"customer_id\":\"{s}\"}}",
                .{if (index % 2 == 0) "doc:c" else "doc:x"},
            );
        }
        try out.writer.writeAll("},\"sync_level\":\"full_index\"}");
        return try out.toOwnedSlice();
    }

    fn runJoinQuery(self: *Fixture) !bool {
        // Public queries are idempotent. Preserve typed topology/availability
        // responses as retryable, and accept 200 only when the response itself
        // proves both expected matches came from a distributed right-side
        // execution spanning at least two production groups.
        for (0..node_count * 4) |attempt| {
            const uri = self.data_api_uris[attempt % node_count];
            const query_body = if (self.durableJoinEnabled())
                durable_join_query_body
            else
                join_query_body;
            var response = self.client.fetchQueryRaw(uri, "tenant_b_docs", query_body) catch |err| switch (err) {
                error.Canceled => return err,
                else => {
                    try self.sim.io().sleep(.fromMilliseconds(1), .awake);
                    continue;
                },
            };
            defer response.deinit(self.alloc);
            if (response.status != 200) {
                if (response.status != 409 and response.status != 503) {
                    std.debug.print("production distributed join status={} body={s}\n", .{
                        response.status,
                        response.body,
                    });
                    return error.UnexpectedHttpStatus;
                }
                try self.sim.io().sleep(.fromMilliseconds(1), .awake);
                continue;
            }
            if (try self.joinResponseComplete(response.body)) return true;
            if (attempt + 1 == node_count * 4) std.debug.print(
                "production distributed join incomplete response: {s}\n",
                .{response.body},
            );
            // A fixed-ID full-index write may be committed before every
            // derived reader on a different production owner has published
            // the indexed generation. Never accept the empty 200 as the join
            // witness; rotate ingress and wait for the complete result.
            try self.sim.io().sleep(.fromMilliseconds(10), .awake);
        }
        return false;
    }

    fn runJoinCancellationQuery(self: *Fixture) !bool {
        if (self.join_cancellation_owner_restart_enabled)
            try self.prepareJoinOwnerRestartCampaign();
        if (self.join_cancellation_overlap_enabled) {
            try self.prepareJoinCancellationOverlapCampaign();
            const fault_observer = self.join_cancellation_overlap_fault_observer orelse
                return error.ProductionJoinCancellationOverlapFaultObserverMissing;
            try fault_observer.activate(
                fault_observer.ptr,
                self.join_cancellation_overlap_coordinator_index,
                self.join_cancellation_overlap_network_target_index,
            );
            self.join_cancellation_overlap_network_matches_before =
                self.sim.outboundEndpointPayloadOutageCount();
            const network_target = try parseHttpBaseUriAddress(
                self.data_api_uris[self.join_cancellation_overlap_network_target_index],
            );
            try self.sim.setOutboundEndpointPayloadOutage(
                network_target,
                "/join-partition",
            );
            self.join_cancellation_overlap_network_armed = true;
        }
        defer {
            if (self.join_cancellation_overlap_network_armed and
                !self.join_cancellation_overlap_network_healed)
            {
                self.sim.setOutboundEndpointOutage(null);
                self.join_cancellation_overlap_network_healed = true;
            }
            if (self.join_cancellation_overlap_faults_injected and
                !self.join_cancellation_overlap_resource_healed)
            {
                self.releaseNodeMemory();
                self.join_cancellation_overlap_resource_healed =
                    self.nodeMemoryBelowHardLimit();
            }
        }
        const started_before = self.join_partition_worker_started_count;
        const completed_before = self.join_partition_worker_completed_count;
        const coordinator_index = if (self.join_cancellation_overlap_enabled)
            self.join_cancellation_overlap_coordinator_index
        else if (self.join_cancellation_owner_restart_enabled)
            self.join_owner_restart_coordinator_index
        else
            1;
        var in_flight = self.sim.io().async(fetchJoinQuery, .{
            self,
            self.data_api_uris[coordinator_index],
            durable_join_query_body,
        });
        defer if (in_flight.any_future != null) {
            const canceled_response: ?common_http.HttpResponse = in_flight.cancel(self.sim.io()) catch null;
            if (canceled_response) |response_value| {
                var response = response_value;
                response.deinit(self.alloc);
            }
        };

        for (0..1_000) |_| {
            if (self.join_partition_worker_started_count > started_before) break;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        } else return false;
        if (!self.join_cancellation_boundary_observed or
            self.join_cancellation_job_id == 0 or
            self.join_cancellation_owner_group_id == 0 or
            self.join_partition_worker_started_count != started_before + 1 or
            self.join_partition_worker_completed_count != completed_before or
            (self.join_cancellation_overlap_enabled and
                (!self.join_cancellation_overlap_faults_injected or
                    !self.join_cancellation_overlap_network_observed or
                    !self.join_cancellation_overlap_resource_observed or
                    !self.join_cancellation_overlap_observed or
                    self.join_cancellation_overlap_first_group_id == 0 or
                    self.join_cancellation_overlap_worker_group_id == 0 or
                    self.join_cancellation_overlap_first_group_id ==
                        self.join_cancellation_overlap_worker_group_id or
                    self.join_cancellation_overlap_coordinator_index ==
                        self.join_cancellation_overlap_network_target_index)) or
            (self.join_cancellation_owner_restart_enabled and
                (!self.join_owner_restart_requested or
                    !self.join_owner_restart_target_configured or
                    self.join_owner_restart_job_id != self.join_cancellation_job_id or
                    self.join_owner_restart_failed_group_id !=
                        self.join_cancellation_owner_group_id or
                    self.join_owner_restart_target_index ==
                        self.join_owner_restart_coordinator_index)))
            return false;

        self.join_cancellation_requested = true;
        const cancelled_request = blk: {
            var response = in_flight.cancel(self.sim.io()) catch |err| switch (err) {
                error.Canceled, error.Cancelled => break :blk true,
                else => break :blk false,
            };
            defer response.deinit(self.alloc);
            break :blk self.join_cancellation_owner_restart_enabled and
                response.status == 500 and
                std.mem.indexOf(u8, response.body, "\"code\":\"internal_failure\"") != null and
                std.mem.indexOf(u8, response.body, "\"hits\"") == null;
        };
        // V35 deliberately does not add a product error mapping for this
        // composed history: the partition stream and finalizer may surface
        // different terminal shapes. Its semantic oracle is the established
        // worker boundary, the public Future cancellation above, and zero
        // completion from that worker.
        self.join_cancellation_observed =
            (cancelled_request or self.join_cancellation_owner_restart_enabled) and
            self.join_partition_worker_completed_count == completed_before;
        if (!self.join_cancellation_observed and
            !self.join_cancellation_owner_restart_enabled) return false;

        if (self.join_cancellation_overlap_enabled) {
            self.sim.setOutboundEndpointOutage(null);
            self.join_cancellation_overlap_network_healed = true;
            self.releaseNodeMemory();
            self.join_cancellation_overlap_resource_healed =
                self.nodeMemoryBelowHardLimit();
            if (!self.join_cancellation_overlap_resource_healed) return false;
        }

        if (!self.join_cancellation_observed) return false;

        if (self.join_cancellation_owner_restart_enabled) {
            // The deployment fault was registered at the worker boundary.
            // Create and release the production restart owner only after the
            // canceled request has reached its terminal with no worker
            // completion. Until this point the runnable-task topology is the
            // proven V30 cancellation topology, and teardown cannot cause the
            // cancellation evidence.
            self.join_restart_future = self.sim.io().async(driveJoinOwnerRestart, .{self});
            self.join_restart_requested.post(self.sim.io());
            while (!self.join_owner_restart_down and self.join_owner_restart_failure == null)
                try self.sim.io().sleep(.fromMilliseconds(1), .awake);
            if (!self.join_owner_restart_down or self.join_owner_restart_failure != null)
                return false;
            self.join_restart_recover.post(self.sim.io());
            while (!self.join_owner_restart_recovered and self.join_owner_restart_failure == null)
                try self.sim.io().sleep(.fromMilliseconds(1), .awake);
            if (!self.join_owner_restart_recovered or self.join_owner_restart_failure != null)
                return false;
            self.join_owner_restart_recovery_query_active = true;
        }
        defer self.join_owner_restart_recovery_query_active = false;

        self.join_sound = try self.runJoinQuery();
        if (self.join_cancellation_owner_restart_enabled) {
            self.join_owner_restart_recovery_join = self.join_sound;
            if (!self.join_owner_restart_recovery_join) return false;
            var rebuilt_read = self.client.fetchLookupResponse(
                self.data_api_uris[self.join_owner_restart_target_index],
                "tenant_b_docs",
                "tenant:q",
                null,
            ) catch return false;
            defer rebuilt_read.deinit(self.alloc);
            self.join_owner_restart_post_reconstruction_read = rebuilt_read.status == 200 and
                std.mem.indexOf(u8, rebuilt_read.body, "production-tenant") != null;
            self.join_owner_restart_sound =
                self.join_owner_restart_post_reconstruction_read;
        }
        self.join_cancellation_recovered = self.join_sound and
            self.join_partition_worker_started_count > started_before + 1 and
            self.join_partition_worker_completed_count > completed_before and
            (!self.join_cancellation_overlap_enabled or
                (self.join_cancellation_overlap_network_healed and
                    self.join_cancellation_overlap_resource_healed)) and
            (!self.join_cancellation_owner_restart_enabled or
                (self.join_owner_restart_down and
                    self.join_owner_restart_recovered and
                    self.join_owner_restart_recovery_join and
                    self.join_owner_restart_post_reconstruction_read and
                    self.join_owner_restart_sound));
        return self.join_cancellation_recovered;
    }

    fn runJoinRetryExhaustionQuery(self: *Fixture) !bool {
        try self.prepareJoinRetryExhaustionCampaign();
        const started_before = self.join_partition_worker_started_count;
        const completed_before = self.join_partition_worker_completed_count;
        defer {
            if (self.join_retry_exhaustion_faults_injected and
                !self.join_retry_exhaustion_network_healed)
            {
                self.sim.setOutboundEndpointOutage(null);
                self.join_retry_exhaustion_network_healed = true;
            }
            if (self.join_retry_exhaustion_faults_injected and
                !self.join_retry_exhaustion_resource_healed)
            {
                self.releaseNodeMemory();
                self.join_retry_exhaustion_resource_healed = self.nodeMemoryBelowHardLimit();
            }
        }

        var response = self.client.fetchQueryRaw(
            self.data_api_uris[self.join_retry_exhaustion_coordinator_index],
            "tenant_b_docs",
            durable_join_query_body,
        ) catch return false;
        defer response.deinit(self.alloc);
        self.join_retry_exhaustion_initial_status = response.status;
        self.join_retry_exhaustion_initial_worker_starts =
            self.join_partition_worker_started_count -| started_before;
        self.join_retry_exhaustion_initial_worker_completions =
            self.join_partition_worker_completed_count -| completed_before;
        self.join_retry_exhaustion_network_observed =
            self.sim.outboundEndpointPayloadOutageCount() >
            self.join_retry_exhaustion_network_matches_before;
        self.join_retry_exhaustion_overlap_observed =
            self.join_retry_exhaustion_resource_observed and
            self.join_retry_exhaustion_network_observed and
            self.allNodeMemorySaturated();
        self.join_retry_exhaustion_initial_rejected_without_partial =
            response.status == 503 and
            std.mem.indexOf(u8, response.body, "\"code\":\"distributed_query_unavailable\"") != null and
            std.mem.indexOf(u8, response.body, "\"retryable\":true") != null and
            std.mem.indexOf(u8, response.body, "\"hits\"") == null and
            std.mem.indexOf(u8, response.body, "production-tenant") == null and
            std.mem.indexOf(u8, response.body, "production-left") == null and
            std.mem.indexOf(u8, response.body, "production-right") == null;
        if (!self.join_retry_exhaustion_faults_injected or
            !self.join_retry_exhaustion_initial_rejected_without_partial or
            !self.join_retry_exhaustion_overlap_observed or
            self.join_retry_exhaustion_job_id == 0 or
            self.join_retry_exhaustion_first_group_id == 0 or
            self.join_retry_exhaustion_retry_group_id == 0 or
            self.join_retry_exhaustion_first_group_id == self.join_retry_exhaustion_retry_group_id or
            self.join_retry_exhaustion_coordinator_index ==
                self.join_retry_exhaustion_retry_target_index or
            self.join_retry_exhaustion_initial_worker_starts == 0 or
            self.join_retry_exhaustion_initial_worker_completions != 0)
            return false;

        self.sim.setOutboundEndpointOutage(null);
        self.join_retry_exhaustion_network_healed = true;
        self.releaseNodeMemory();
        self.join_retry_exhaustion_resource_healed = self.nodeMemoryBelowHardLimit();
        if (!self.join_retry_exhaustion_resource_healed) return false;

        self.join_retry_exhaustion_recovery_query_active = true;
        defer self.join_retry_exhaustion_recovery_query_active = false;
        self.join_retry_exhaustion_recovery_join = try self.runJoinQuery();
        return self.join_retry_exhaustion_recovery_join;
    }

    fn runJoinOwnerRestartQuery(self: *Fixture) !bool {
        try self.prepareJoinOwnerRestartCampaign();

        self.join_owner_restart_initial_query_active = true;
        var response = self.client.fetchQueryRaw(
            self.data_api_uris[self.join_owner_restart_coordinator_index],
            "tenant_b_docs",
            durable_join_query_body,
        ) catch {
            self.join_owner_restart_initial_query_active = false;
            return false;
        };
        self.join_owner_restart_initial_query_active = false;
        defer response.deinit(self.alloc);
        self.join_owner_restart_initial_status = response.status;
        self.join_owner_restart_initial_rejected_without_partial =
            response.status == 503 and
            std.mem.indexOf(u8, response.body, "\"code\":\"distributed_query_unavailable\"") != null and
            std.mem.indexOf(u8, response.body, "\"retryable\":true") != null and
            std.mem.indexOf(u8, response.body, "\"hits\"") == null and
            std.mem.indexOf(u8, response.body, "production-tenant") == null and
            std.mem.indexOf(u8, response.body, "production-left") == null and
            std.mem.indexOf(u8, response.body, "production-right") == null;
        if (!self.join_owner_restart_initial_rejected_without_partial) return false;

        // The fail-closed response is emitted only after a replacement worker
        // has selected the partition and the stopped process is reconstructed.
        // Still settle the state bit here so the assertion is independent of
        // which waiter consumes the reconstruction semaphore.
        if (self.join_owner_restart_requested) {
            while (!self.join_owner_restart_down and self.join_owner_restart_failure == null)
                try self.sim.io().sleep(.fromMilliseconds(1), .awake);
            if (self.join_owner_restart_down and
                self.join_owner_restart_recovered_group_id == 0 and
                self.join_owner_restart_failure == null)
            {
                // No alternate owner was selected before the public operation
                // failed closed.
                // Reconstruct the deliberately stopped process for clean
                // ownership, then let the exact evidence check fail closed.
                self.join_restart_recover.post(self.sim.io());
            }
            while (!self.join_owner_restart_recovered and self.join_owner_restart_failure == null)
                try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        }
        if (!self.join_owner_restart_requested or !self.join_owner_restart_down or
            !self.join_owner_restart_recovered or self.join_owner_restart_failure != null or
            self.join_owner_restart_job_id == 0 or
            self.join_owner_restart_recovered_group_id == 0 or
            self.join_owner_restart_recovered_group_id == self.join_owner_restart_failed_group_id or
            self.join_owner_restart_recovery_index == self.join_owner_restart_target_index)
            return false;

        self.join_owner_restart_recovery_query_active = true;
        defer self.join_owner_restart_recovery_query_active = false;
        self.join_owner_restart_recovery_join = try self.runJoinQuery();
        if (!self.join_owner_restart_recovery_join) return false;

        // Address the rebuilt endpoint directly. This is stronger than merely
        // observing the replacement worker: it proves the destroyed process
        // rebound its stable listener and can serve production routing again.
        var rebuilt_read = self.client.fetchLookupResponse(
            self.data_api_uris[self.join_owner_restart_target_index],
            "tenant_b_docs",
            "tenant:q",
            null,
        ) catch return false;
        defer rebuilt_read.deinit(self.alloc);
        self.join_owner_restart_post_reconstruction_read = rebuilt_read.status == 200 and
            std.mem.indexOf(u8, rebuilt_read.body, "production-tenant") != null;
        return self.join_owner_restart_post_reconstruction_read;
    }

    fn fetchJoinQuery(
        self: *Fixture,
        uri: []const u8,
        body: []const u8,
    ) !common_http.HttpResponse {
        return self.client.fetchQueryRaw(uri, "tenant_b_docs", body);
    }

    fn joinResponseComplete(self: *Fixture, body: []const u8) !bool {
        var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, body, .{});
        defer parsed.deinit();
        const responses_value = parsed.value.object.get("responses") orelse return false;
        if (responses_value != .array or responses_value.array.items.len != 1) return false;
        const response_value = responses_value.array.items[0];
        if (response_value != .object) return false;
        const hits_value = response_value.object.get("hits") orelse return false;
        if (hits_value != .object) return false;
        const hit_items_value = hits_value.object.get("hits") orelse return false;
        const durable = self.durableJoinEnabled();
        const expected_rows: usize = if (durable) durable_join_row_count else 2;
        if (hit_items_value != .array or hit_items_value.array.items.len != expected_rows) return false;

        var saw_left = false;
        var saw_right = false;
        for (hit_items_value.array.items) |hit_value| {
            if (hit_value != .object) return false;
            const source_value = hit_value.object.get("_source") orelse return false;
            if (source_value != .object) return false;
            const title = source_value.object.get("title") orelse return false;
            if (title != .string or !std.mem.eql(u8, title.string, "production-tenant")) return false;
            const joined_title = source_value.object.get("docs.title") orelse return false;
            if (joined_title != .string) return false;
            saw_left = saw_left or std.mem.eql(u8, joined_title.string, "production-left");
            saw_right = saw_right or std.mem.eql(u8, joined_title.string, "production-right");
        }

        const profile_value = response_value.object.get("profile") orelse return false;
        if (profile_value != .object) return false;
        const join_value = profile_value.object.get("join") orelse return false;
        if (join_value != .object) return false;
        const distributed = join_value.object.get("distributed_execution") orelse return false;
        const groups = join_value.object.get("groups_queried") orelse return false;
        const rows = join_value.object.get("rows_matched") orelse return false;
        const worker_attempts = join_value.object.get("worker_attempts") orelse return false;
        // Broadcast reports each right owner in groups_queried. A shuffle
        // finalizer delegates partitions instead, so its production witness
        // is the exact-group worker-attempt ledger plus the finalizer ledger
        // checked below; groups_queried is not populated by that protocol.
        const ownership_sound = if (durable)
            worker_attempts == .array and worker_attempts.array.items.len > 0
        else
            groups == .integer and groups.integer >= 2;
        const base_sound = saw_left and saw_right and distributed == .bool and distributed.bool and
            ownership_sound and
            rows == .integer and rows.integer == expected_rows;
        if (!base_sound or !durable) return base_sound;

        const strategy = join_value.object.get("strategy_used") orelse return false;
        const execution_mode = join_value.object.get("execution_mode") orelse return false;
        const job_phase = join_value.object.get("job_phase") orelse return false;
        const worker_retries = join_value.object.get("worker_retries") orelse return false;
        const finalizer_retries = join_value.object.get("finalizer_retries") orelse return false;
        const attempts = join_value.object.get("finalizer_attempts") orelse return false;
        if (strategy != .string or !std.mem.eql(u8, strategy.string, "shuffle") or
            execution_mode != .string or !std.mem.eql(u8, execution_mode.string, "distributed_durable") or
            job_phase != .string or !std.mem.eql(u8, job_phase.string, "succeeded") or
            worker_retries != .integer or finalizer_retries != .integer or
            attempts != .array)
            return false;
        if ((self.join_owner_restart_enabled and self.join_owner_restart_recovery_query_active) or
            (self.join_retry_exhaustion_enabled and self.join_retry_exhaustion_recovery_query_active))
        {
            if (worker_retries.integer != 0 or worker_attempts != .array or
                worker_attempts.array.items.len == 0 or finalizer_retries.integer != 0 or
                attempts.array.items.len != 1)
                return false;
            for (worker_attempts.array.items) |worker_attempt| {
                if (worker_attempt != .object) return false;
                const worker_ok = worker_attempt.object.get("succeeded") orelse return false;
                if (worker_ok != .bool or !worker_ok.bool) return false;
            }
            const finalizer = attempts.array.items[0];
            if (finalizer != .object) return false;
            const finalizer_group = finalizer.object.get("worker_group_id") orelse return false;
            const finalizer_ok = finalizer.object.get("succeeded") orelse return false;
            return finalizer_group == .integer and finalizer_group.integer != 0 and
                finalizer_ok == .bool and finalizer_ok.bool;
        }
        if (self.join_worker_retry_enabled or self.join_owner_restart_enabled) {
            const failure_injected = if (self.join_owner_restart_enabled)
                self.join_owner_restart_requested and self.join_owner_restart_down and
                    self.join_owner_restart_recovered
            else
                self.join_worker_retry_failure_injected;
            const job_id = if (self.join_owner_restart_enabled)
                self.join_owner_restart_job_id
            else
                self.join_worker_retry_job_id;
            const partition_index = if (self.join_owner_restart_enabled)
                self.join_owner_restart_partition_index
            else
                self.join_worker_retry_partition_index;
            const failed_group_id = if (self.join_owner_restart_enabled)
                self.join_owner_restart_failed_group_id
            else
                self.join_worker_retry_failed_group_id;
            const recovered_group_id = if (self.join_owner_restart_enabled)
                self.join_owner_restart_recovered_group_id
            else
                self.join_worker_retry_recovered_group_id;
            if (!failure_injected or job_id == 0 or
                failed_group_id == 0 or recovered_group_id == 0 or
                worker_retries.integer != 1 or
                worker_attempts != .array or worker_attempts.array.items.len < 2 or
                finalizer_retries.integer != 0 or attempts.array.items.len != 1)
                return false;

            const failed_worker = worker_attempts.array.items[0];
            const recovered_worker = worker_attempts.array.items[1];
            if (failed_worker != .object or recovered_worker != .object) return false;
            const failed_partition = failed_worker.object.get("partition_index") orelse return false;
            const failed_group = failed_worker.object.get("worker_group_id") orelse return false;
            const failed_ok = failed_worker.object.get("succeeded") orelse return false;
            const recovered_partition = recovered_worker.object.get("partition_index") orelse return false;
            const recovered_group = recovered_worker.object.get("worker_group_id") orelse return false;
            const recovered_ok = recovered_worker.object.get("succeeded") orelse return false;
            const successful_finalizer = attempts.array.items[0];
            if (successful_finalizer != .object) return false;
            const finalizer_group = successful_finalizer.object.get("worker_group_id") orelse return false;
            const finalizer_ok = successful_finalizer.object.get("succeeded") orelse return false;
            for (worker_attempts.array.items[2..]) |later_attempt| {
                if (later_attempt != .object) return false;
                const later_ok = later_attempt.object.get("succeeded") orelse return false;
                if (later_ok != .bool or !later_ok.bool) return false;
            }
            return failed_partition == .integer and
                failed_partition.integer >= 0 and
                @as(usize, @intCast(failed_partition.integer)) == partition_index and
                failed_group == .integer and
                failed_group.integer == failed_group_id and
                failed_ok == .bool and !failed_ok.bool and
                recovered_partition == .integer and
                recovered_partition.integer >= 0 and
                @as(usize, @intCast(recovered_partition.integer)) == partition_index and
                recovered_group == .integer and
                recovered_group.integer == recovered_group_id and
                recovered_group.integer != failed_group.integer and
                recovered_ok == .bool and recovered_ok.bool and
                finalizer_group == .integer and finalizer_group.integer != 0 and
                finalizer_ok == .bool and finalizer_ok.bool;
        }
        if (self.join_cancellation_enabled) {
            if (finalizer_retries.integer != 0 or attempts.array.items.len != 1) return false;
            const successful_attempt = attempts.array.items[0];
            if (successful_attempt != .object) return false;
            const successful_group = successful_attempt.object.get("worker_group_id") orelse return false;
            const successful_ok = successful_attempt.object.get("succeeded") orelse return false;
            return successful_group == .integer and successful_group.integer != 0 and
                successful_ok == .bool and successful_ok.bool;
        }

        const imported_owner = join_value.object.get("imported_owner_group_id") orelse return false;
        const imported_cached = join_value.object.get("imported_cached_result") orelse return false;
        if (finalizer_retries.integer != 1 or
            imported_owner != .integer or imported_owner.integer != self.join_finalizer_persisted_group_id or
            imported_cached != .bool or !imported_cached.bool or
            attempts.array.items.len != 2)
            return false;
        const failed_attempt = attempts.array.items[0];
        const successful_attempt = attempts.array.items[1];
        if (failed_attempt != .object or successful_attempt != .object) return false;
        const failed_group = failed_attempt.object.get("worker_group_id") orelse return false;
        const failed_ok = failed_attempt.object.get("succeeded") orelse return false;
        const successful_group = successful_attempt.object.get("worker_group_id") orelse return false;
        const successful_ok = successful_attempt.object.get("succeeded") orelse return false;
        self.durable_join_takeover_sound = self.join_finalizer_ack_failure_injected and
            failed_group == .integer and failed_group.integer == self.join_finalizer_persisted_group_id and
            failed_ok == .bool and !failed_ok.bool and
            successful_group == .integer and successful_group.integer != failed_group.integer and
            successful_ok == .bool and successful_ok.bool;
        return self.durable_join_takeover_sound;
    }

    fn queryResponseHasExactHitIds(response: std.json.Value, expected_ids: []const []const u8) bool {
        const response_object = switch (response) {
            .object => |object| object,
            else => return false,
        };
        const hits_container = response_object.get("hits") orelse return false;
        const hits_object = switch (hits_container) {
            .object => |object| object,
            else => return false,
        };
        const hits_value = hits_object.get("hits") orelse return false;
        const hits = switch (hits_value) {
            .array => |array| array.items,
            else => return false,
        };
        if (hits.len != expected_ids.len) return false;

        for (expected_ids) |expected_id| {
            var found = false;
            for (hits) |hit| {
                const hit_object = switch (hit) {
                    .object => |object| object,
                    else => return false,
                };
                const id_value = hit_object.get("_id") orelse return false;
                const id = switch (id_value) {
                    .string => |value| value,
                    else => return false,
                };
                if (std.mem.eql(u8, id, expected_id)) found = true;
            }
            if (!found) return false;
        }
        return true;
    }

    fn runGlobalMultiQuery(self: *Fixture) !bool {
        var response = try self.client.fetchGlobalMultiQueryRaw(
            self.data_api_uris[self.global_query_route_index],
            global_query_body,
        );
        defer response.deinit(self.alloc);
        self.global_query_status = response.status;
        if (response.status != 200) return false;

        var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, response.body, .{
            .allocate = .alloc_always,
        }) catch return false;
        defer parsed.deinit();
        const root = switch (parsed.value) {
            .object => |object| object,
            else => return false,
        };
        const responses_value = root.get("responses") orelse return false;
        const responses = switch (responses_value) {
            .array => |array| array.items,
            else => return false,
        };
        self.global_query_response_count = responses.len;
        if (responses.len != 2) return false;

        // Flattened responses must preserve NDJSON line order. Exact ID sets
        // also prove that no result crossed the docs/tenant boundary.
        return queryResponseHasExactHitIds(responses[0], &.{ "doc:c", "doc:k", "doc:x" }) and
            queryResponseHasExactHitIds(responses[1], &.{ "tenant:q", "tenant:r" });
    }

    fn fetchGlobalMultiQuery(
        self: *Fixture,
        uri: []const u8,
        body: []const u8,
    ) !common_http.HttpResponse {
        return self.client.fetchGlobalMultiQueryRaw(uri, body);
    }

    fn runGlobalMultiQueryCancellation(self: *Fixture) !bool {
        const results_before = self.global_query_result_assembled_count;
        const responses_ready_before = self.public_response_ready_count;
        self.global_query_cancellation_armed = true;

        var in_flight = self.sim.io().async(fetchGlobalMultiQuery, .{
            self,
            self.data_api_uris[1],
            global_query_body,
        });
        defer if (in_flight.any_future != null) {
            const canceled_response: ?common_http.HttpResponse = in_flight.cancel(self.sim.io()) catch null;
            if (canceled_response) |response_value| {
                var response = response_value;
                response.deinit(self.alloc);
            }
        };

        try self.global_query_cancellation_boundary.wait(self.sim.io());
        if (!self.global_query_cancellation_boundary_observed or
            self.global_query_result_assembled_count != results_before + 1)
            return false;

        self.global_query_cancellation_requested = true;
        // Wake the server-owned lifecycle hook before cancel joins the client
        // task. `Future.cancel` abandons the socket first; when the handler
        // resumes, the next NDJSON line observes that request cancellation.
        self.global_query_cancellation_release.post(self.sim.io());
        const cancelled_request = blk: {
            var response = in_flight.cancel(self.sim.io()) catch |err| switch (err) {
                error.Canceled, error.Cancelled => break :blk true,
                else => break :blk false,
            };
            defer response.deinit(self.alloc);
            break :blk false;
        };
        self.global_query_cancellation_observed = cancelled_request;
        if (!self.global_query_cancellation_observed) return false;

        // Client unwinding and server request completion are distinct owners.
        // Do not inspect the no-partial oracle until the production handler
        // has crossed its response-ready boundary and released admission.
        for (0..1_000) |_| {
            if (self.public_response_ready_count > responses_ready_before) break;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        } else return false;
        self.global_query_cancellation_no_partial =
            self.global_query_result_assembled_count == results_before + 1;
        if (!self.global_query_cancellation_no_partial) return false;

        const recovered = try self.runGlobalMultiQuery();
        self.global_query_cancellation_recovered = recovered and
            self.global_query_result_assembled_count == results_before + 3;
        self.global_query_cancellation_sound =
            self.global_query_cancellation_boundary_observed and
            self.global_query_cancellation_requested and
            self.global_query_cancellation_observed and
            self.global_query_cancellation_no_partial and
            self.global_query_cancellation_recovered;
        return self.global_query_cancellation_sound;
    }

    fn runGlobalMultiQueryAuthorizationRevocation(self: *Fixture) !bool {
        if (!self.auth_manager_live)
            return error.ProductionGlobalQueryAuthorizationManagerMissing;
        const results_before = self.global_query_result_assembled_count;
        self.global_query_authorization_revocation_armed = true;

        var denied = try self.client.fetchGlobalMultiQueryRaw(
            self.data_api_uris[1],
            global_query_body,
        );
        defer denied.deinit(self.alloc);
        self.global_query_authorization_denied_status = denied.status;
        self.global_query_authorization_denied_without_leak =
            self.global_query_authorization_boundary_observed and
            self.global_query_authorization_revoked and
            self.global_query_result_assembled_count == results_before + 1 and
            denied.status == 403 and
            std.mem.eql(u8, denied.body, "{\"error\":\"forbidden\"}");
        if (!self.global_query_authorization_denied_without_leak) return false;

        var restored_permission = try usermgr.Permission.initOwned(
            self.alloc,
            .table,
            "tenant_b_docs",
            .read,
        );
        defer restored_permission.deinit(self.alloc);
        try self.auth_manager.addPermissionToUser(
            graph_authorization_username,
            restored_permission,
        );
        self.global_query_authorization_restored = true;

        const recovered = try self.runGlobalMultiQuery();
        self.global_query_authorization_recovered_status = self.global_query_status;
        self.global_query_authorization_recovered = recovered and
            self.global_query_result_assembled_count == results_before + 3;
        self.global_query_authorization_sound =
            self.global_query_authorization_boundary_observed and
            self.global_query_authorization_revoked and
            self.global_query_authorization_denied_without_leak and
            self.global_query_authorization_restored and
            self.global_query_authorization_recovered;
        return self.global_query_authorization_sound;
    }

    fn runGlobalMultiQueryTransportFailure(self: *Fixture) !bool {
        if (!self.global_query_transport_target_configured)
            return error.ProductionGlobalQueryTransportTargetMissing;
        if (self.currentTenantOwnerIndex() != self.global_query_transport_target_index)
            return error.ProductionGlobalQueryTransportTargetLeadershipChanged;
        defer if (self.global_query_transport_fault_injected and
            !self.global_query_transport_fault_healed)
        {
            self.sim.setOutboundEndpointOutage(null);
            self.global_query_transport_fault_healed = true;
        };

        const results_before = self.global_query_result_assembled_count;
        self.global_query_transport_failure_armed = true;
        var rejected = try self.client.fetchGlobalMultiQueryRaw(
            self.data_api_uris[self.global_query_route_index],
            global_query_body,
        );
        defer rejected.deinit(self.alloc);
        self.global_query_transport_rejected_status = rejected.status;
        self.global_query_transport_fault_matches =
            self.sim.outboundEndpointPayloadOutageCount() -|
            self.global_query_transport_fault_count_before;
        self.global_query_transport_fault_observed =
            self.global_query_transport_fault_matches == 1;
        self.global_query_transport_rejected_without_partial =
            self.global_query_transport_boundary_observed and
            self.global_query_transport_fault_injected and
            self.global_query_transport_fault_observed and
            self.global_query_result_assembled_count == results_before + 1 and
            rejected.status == 503 and
            std.mem.eql(
                u8,
                rejected.body,
                "{\"code\":\"distributed_query_unavailable\",\"message\":\"distributed query unavailable\",\"retryable\":true}",
            );
        if (!self.global_query_transport_rejected_without_partial) return false;

        self.sim.setOutboundEndpointOutage(null);
        self.global_query_transport_fault_healed = true;
        const recovered = try self.runGlobalMultiQuery();
        self.global_query_transport_recovered_status = self.global_query_status;
        self.global_query_transport_recovered = recovered and
            self.global_query_result_assembled_count == results_before + 3;
        self.global_query_transport_sound =
            self.global_query_transport_boundary_observed and
            self.global_query_transport_fault_injected and
            self.global_query_transport_fault_observed and
            self.global_query_transport_fault_healed and
            self.global_query_transport_rejected_without_partial and
            self.global_query_transport_recovered;
        return self.global_query_transport_sound;
    }

    fn runGlobalMultiQueryOwnerRestart(self: *Fixture) !bool {
        if (!self.global_query_owner_restart_target_configured)
            return error.ProductionGlobalQueryOwnerRestartTargetMissing;
        if (self.currentTenantOwnerIndex() !=
            self.global_query_owner_restart_target_index)
            return error.ProductionGlobalQueryOwnerRestartTargetLeadershipChanged;

        const results_before = self.global_query_result_assembled_count;
        self.global_query_restart_future = self.sim.io().async(
            driveGlobalQueryOwnerRestart,
            .{self},
        );
        self.global_query_owner_restart_armed = true;
        var rejected = try self.client.fetchGlobalMultiQueryRaw(
            self.data_api_uris[self.global_query_route_index],
            global_query_body,
        );
        defer rejected.deinit(self.alloc);
        self.global_query_owner_restart_rejected_status = rejected.status;
        self.global_query_owner_restart_rejected_without_partial =
            self.global_query_owner_restart_boundary_observed and
            self.global_query_owner_restart_down and
            !self.data_server_live[self.global_query_owner_restart_target_index] and
            self.global_query_result_assembled_count == results_before + 1 and
            rejected.status == 503 and
            std.mem.eql(
                u8,
                rejected.body,
                "{\"code\":\"distributed_query_unavailable\",\"message\":\"distributed query unavailable\",\"retryable\":true}",
            );
        if (!self.global_query_owner_restart_rejected_without_partial)
            return false;

        self.global_query_restart_recover.post(self.sim.io());
        try self.global_query_restart_recovered.wait(self.sim.io());
        if (self.global_query_owner_restart_failure) |err| return err;
        self.global_query_owner_restart_reconstructed =
            self.global_query_owner_restart_reconstructed and
            self.data_server_live[self.global_query_owner_restart_target_index] and
            self.data_api_uri_live[self.global_query_owner_restart_target_index] and
            self.data_raft_uri_live[self.global_query_owner_restart_target_index];
        if (!self.global_query_owner_restart_reconstructed or
            !self.global_query_owner_restart_direct_read) return false;

        const recovered = try self.runGlobalMultiQuery();
        self.global_query_owner_restart_recovered_status = self.global_query_status;
        self.global_query_owner_restart_recovered = recovered and
            self.global_query_result_assembled_count == results_before + 3;
        self.global_query_owner_restart_sound =
            self.global_query_owner_restart_boundary_observed and
            self.global_query_owner_restart_down and
            self.global_query_owner_restart_rejected_without_partial and
            self.global_query_owner_restart_reconstructed and
            self.global_query_owner_restart_direct_read and
            self.global_query_owner_restart_recovered;
        return self.global_query_owner_restart_sound;
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

    fn runGraphHydrationQuery(self: *Fixture) !bool {
        const query_body = try test_contract_helpers.encodeGraphTraverseQueryWithDocumentsRequest(
            self.alloc,
            "walk",
            graph_index_name,
            &.{"doc:c"},
            &.{"links"},
            2,
            10,
        );
        defer self.alloc.free(query_body);
        const started_before = self.graph_hydration_started_count;
        const completed_before = self.graph_hydration_completed_count;

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
            return self.graph_hydration_started_count == started_before + 1 and
                self.graph_hydration_completed_count == completed_before + 1 and
                try self.graphHydrationResponseComplete(response.body);
        }
        return false;
    }

    fn runGraphCancellationQuery(self: *Fixture) !bool {
        const query_body = try test_contract_helpers.encodeGraphTraverseQueryWithDocumentsRequest(
            self.alloc,
            "walk",
            graph_index_name,
            &.{"doc:c"},
            &.{"links"},
            2,
            10,
        );
        defer self.alloc.free(query_body);
        defer if (self.graph_cancellation_fault_injected and
            !self.graph_cancellation_fault_healed)
        {
            self.sim.setOutboundEndpointOutage(null);
            self.graph_cancellation_fault_healed = true;
        };

        const started_before = self.graph_hydration_started_count;
        const fanout_before = self.graph_hydration_fanout_started_count;
        const completed_before = self.graph_hydration_completed_count;
        var in_flight = self.sim.io().async(fetchGraphQuery, .{
            self,
            self.data_api_uris[self.graph_probe_route_index],
            query_body,
        });
        defer if (in_flight.any_future != null) {
            const canceled_response: ?common_http.HttpResponse = in_flight.cancel(self.sim.io()) catch null;
            if (canceled_response) |response_value| {
                var response = response_value;
                response.deinit(self.alloc);
            }
        };

        for (0..1_000) |_| {
            if (self.graph_hydration_fanout_started_count > fanout_before) break;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        } else return false;
        if (self.graph_hydration_started_count != started_before + 1 or
            self.graph_hydration_fanout_started_count != fanout_before + 1 or
            self.graph_hydration_completed_count != completed_before)
            return false;

        if (self.fault_mode == .graph_hydration_transport_failure) {
            for (0..1_000) |_| {
                const current = self.sim.outboundEndpointPayloadOutageCount();
                if (current > self.graph_cancellation_fault_count_before) {
                    self.graph_cancellation_fault_matches =
                        current - self.graph_cancellation_fault_count_before;
                    self.graph_cancellation_fault_observed = true;
                    break;
                }
                try self.sim.io().sleep(.fromMilliseconds(1), .awake);
            } else return false;
        }

        self.graph_cancellation_requested = true;
        const cancelled_request = blk: {
            var response = in_flight.cancel(self.sim.io()) catch |err| switch (err) {
                error.Canceled, error.Cancelled => break :blk true,
                else => break :blk false,
            };
            defer response.deinit(self.alloc);
            break :blk false;
        };
        self.graph_cancellation_observed = cancelled_request and
            self.graph_hydration_completed_count == completed_before;
        if (!self.graph_cancellation_observed) return false;

        if (self.fault_mode == .graph_hydration_transport_failure) {
            self.sim.setOutboundEndpointOutage(null);
            self.graph_cancellation_fault_healed = true;
        }

        self.graph_hydration_sound = try self.runGraphHydrationQuery();
        self.graph_cancellation_recovered = self.graph_hydration_sound and
            self.graph_hydration_started_count == started_before + 2 and
            self.graph_hydration_fanout_started_count == fanout_before + 2 and
            self.graph_hydration_completed_count == completed_before + 1 and
            (self.fault_mode != .graph_hydration_transport_failure or
                (self.graph_cancellation_fault_injected and
                    self.graph_cancellation_fault_observed and
                    self.graph_cancellation_fault_matches > 0 and
                    self.graph_cancellation_fault_healed));
        return self.graph_cancellation_recovered;
    }

    fn fetchGraphQuery(
        self: *Fixture,
        uri: []const u8,
        body: []const u8,
    ) !common_http.HttpResponse {
        return self.client.fetchQueryRaw(uri, "docs", body);
    }

    fn runGraphInflightAuthorizationRevocationQuery(self: *Fixture) !bool {
        if (!self.auth_manager_live) return error.ProductionGraphAuthorizationManagerMissing;

        const query_body = try test_contract_helpers.encodeGraphTraverseQueryWithDocumentsRequest(
            self.alloc,
            "walk",
            graph_index_name,
            &.{"doc:a"},
            &.{"links"},
            1,
            10,
        );
        defer self.alloc.free(query_body);

        self.graph_authorization_revocation_armed = true;

        var denied = try self.client.fetchQueryRaw(
            self.data_api_uris[self.graph_probe_route_index],
            "docs",
            query_body,
        );
        defer denied.deinit(self.alloc);
        self.graph_authorization_denied_status = denied.status;
        self.graph_authorization_denied_without_leak =
            self.graph_authorization_boundary_observed and
            self.graph_authorization_revoked and
            denied.status == 200 and
            try self.graphAuthorizationResponseSound(denied.body, false);
        if (!self.graph_authorization_denied_without_leak) return false;

        var restored_permission = try usermgr.Permission.initOwned(
            self.alloc,
            .table,
            "tenant_b_docs",
            .read,
        );
        defer restored_permission.deinit(self.alloc);
        try self.auth_manager.addPermissionToUser(
            graph_authorization_username,
            restored_permission,
        );
        self.graph_authorization_restored = true;

        var recovered = try self.client.fetchQueryRaw(
            self.data_api_uris[self.graph_probe_route_index],
            "docs",
            query_body,
        );
        defer recovered.deinit(self.alloc);
        self.graph_authorization_recovered_status = recovered.status;
        self.graph_authorization_recovered = recovered.status == 200 and
            try self.graphAuthorizationResponseSound(recovered.body, true);
        return self.graph_authorization_recovered;
    }

    fn runGraphStaleSnapshotRetryExhaustionQuery(self: *Fixture) !bool {
        const query_body = try test_contract_helpers.encodeGraphTraverseQueryWithDocumentsRequest(
            self.alloc,
            "walk",
            graph_index_name,
            &.{"doc:c"},
            &.{"links"},
            2,
            10,
        );
        defer self.alloc.free(query_body);

        const failures_before = self.graph_stale_snapshot_attempt_failures;
        self.graph_stale_snapshot_armed = true;
        var stale = try self.client.fetchQueryRaw(
            self.data_api_uris[self.graph_probe_route_index],
            "docs",
            query_body,
        );
        defer stale.deinit(self.alloc);
        self.graph_stale_snapshot_status = stale.status;
        self.graph_stale_snapshot_rejected_without_partial = stale.status == 503 and
            std.mem.indexOf(u8, stale.body, "distributed_query_unavailable") != null and
            std.mem.indexOf(u8, stale.body, "doc:c") == null and
            std.mem.indexOf(u8, stale.body, "doc:x") == null and
            std.mem.indexOf(u8, stale.body, "doc:k") == null and
            std.mem.indexOf(u8, stale.body, "production-right") == null and
            std.mem.indexOf(u8, stale.body, "production-split") == null;
        if (!self.graph_stale_snapshot_boundary_observed or
            !self.split_finalized or !self.split_published or
            self.graph_stale_snapshot_attempt_failures != failures_before + 2 or
            self.graph_stale_snapshot_error_code != @intFromError(error.TopologyChanged) or
            !self.graph_stale_snapshot_rejected_without_partial)
            return false;

        const post_split_read_result = self.runRead(
            3,
            0,
            "docs",
            "doc:k",
            "production-split",
        );
        self.split_sound = try operationSucceeded(post_split_read_result);
        self.graph_hydration_sound = try self.runGraphHydrationQuery();
        self.graph_stale_snapshot_recovered = self.split_sound and self.graph_hydration_sound;
        self.post_split_graph_sound = self.graph_stale_snapshot_recovered;
        self.graph_sound = self.graph_sound and self.post_split_graph_sound;
        self.topology_sound = self.topology_sound and self.split_sound;
        self.phase = .post_split_graph_query_complete;
        return self.graph_stale_snapshot_recovered;
    }

    fn graphAuthorizationResponseSound(
        self: *Fixture,
        body: []const u8,
        expect_visible: bool,
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
        if (!expect_visible) {
            return walk.total == 0 and nodes.len == 0 and
                std.mem.indexOf(u8, body, "tenant:q") == null and
                std.mem.indexOf(u8, body, "production-tenant") == null;
        }
        if (walk.total != 1 or nodes.len != 1) return false;
        const node = nodes[0];
        if (!std.mem.eql(u8, node.key, "tenant:q") or
            node.table == null or
            !std.mem.eql(u8, node.table.?, "tenant_b_docs")) return false;
        const document = node.document orelse return false;
        if (document != .object) return false;
        const title = document.object.get("title") orelse return false;
        return title == .string and std.mem.eql(u8, title.string, "production-tenant");
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
                if (self.fault_mode != .clean and self.fault_mode != .resource_pressure and
                    self.fault_mode != .socket_pressure) return false;
                return self.split_graph_inflight_complete;
            },
            409, 503 => {
                // A request crossing topology publication may fail closed,
                // but it must never return a successful partial traversal.
                self.split_graph_inflight_rejected = graphResponseFailClosed(response.status, response.body);
                if (self.fault_mode.hasGraphTransportFailure()) {
                    self.graph_partial_rejected_sound =
                        self.graph_transport_failure_injected and
                        self.graph_transport_failure_observed and
                        self.graph_transport_failure_error_code != 0 and
                        (self.fault_mode != .graph_transport_resource_pressure or
                            self.overlapping_faults_active_observed) and
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
                if (self.fault_mode == .resource_pressure or self.fault_mode == .socket_pressure) return false;
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

    /// Consume every remaining byte in each real DataServer process envelope,
    /// then drive the ordinary public write path while the metadata-owned
    /// split is durably nonterminal. Pressure may reject before proposal with
    /// a retryable 503 or cross the proposal boundary and return an explicit
    /// 409 outcome-unknown. The latter is always resolved by a read before the
    /// known-idempotent fixed-ID upsert may be retried. Recovery is not
    /// complete until the value is publicly visible; the caller separately
    /// verifies it again after split publication.
    fn runResourcePressureDuringSplit(self: *Fixture) !void {
        try self.saturateNodeMemory();
        defer self.releaseNodeMemory();
        self.resource_pressure_observed = self.allNodeMemorySaturated();
        if (!self.resource_pressure_observed)
            return error.ProductionDataResourceEnvelopeNotSaturated;

        const proposal_phase = @intFromEnum(data_runtime.DataRequestLifecyclePhase.proposal_accepted);
        self.resource_proposals_before = self.request_lifecycle_counts[proposal_phase];

        var denied = try self.client.fetchBatchResponse(
            self.data_api_uris[0],
            "docs",
            resource_probe_body,
        );
        defer denied.deinit(self.alloc);
        self.resource_denial_status = denied.status;
        self.resource_denial_body_digest = std.hash.Wyhash.hash(0, denied.body);
        self.resource_proposals_after = self.request_lifecycle_counts[proposal_phase];
        const body = std.mem.trim(u8, denied.body, " \t\r\n");
        self.resource_preproposal_denial = denied.status == 503 and
            std.mem.eql(u8, body, "write unavailable") and
            self.resource_proposals_after == self.resource_proposals_before;
        self.resource_outcome_unknown = denied.status == 409 and
            std.mem.eql(u8, body, "write outcome unknown") and
            self.resource_proposals_after > self.resource_proposals_before;
        self.resource_denial_sound = self.resource_preproposal_denial or self.resource_outcome_unknown;
        if (!self.resource_denial_sound)
            return error.ProductionDataResourcePressureDidNotFailSafe;

        self.releaseNodeMemory();
        self.resource_read_before_retry = true;
        if (self.resource_outcome_unknown) {
            // The proposal can finish after the HTTP request loses certainty.
            // Resolve that ambiguity through the ordinary public read path;
            // a visible fixed-ID value is already a successful recovery and
            // must not be blindly submitted again.
            self.resource_recovery_sound = try self.lookupContains(
                "docs",
                "pressure:probe",
                "production-owner-resource-recovery",
            );
            if (self.resource_recovery_sound) return;
        }

        self.resource_absent_before_retry = try self.lookupAbsentEverywhere("docs", "pressure:probe");
        if (!self.resource_absent_before_retry)
            return error.ProductionDataResourceAmbiguityUnresolved;

        self.resource_retry_attempted = true;
        var recovered = try self.client.fetchBatch(
            self.data_api_uris[0],
            "docs",
            resource_probe_body,
        );
        recovered.deinit(self.alloc);
        self.resource_recovery_sound = try self.lookupContains(
            "docs",
            "pressure:probe",
            "production-owner-resource-recovery",
        );
        if (!self.resource_recovery_sound)
            return error.ProductionDataResourceRecoveryFailed;
    }

    /// Deny every new connection to one registered production listener while
    /// leaving existing connections and all other node endpoints untouched.
    /// A fresh non-pooled client makes the denial non-vacuous; a second fresh
    /// client after healing proves the same public route recovers.
    fn runSocketPressureDuringSplit(self: *Fixture) !void {
        if (!self.socket_pressure_target_configured)
            return error.ProductionSocketPressureTargetMissing;
        const target_index = self.socket_pressure_target_index;
        if (target_index >= self.data_api_uri_count or !self.data_api_uri_live[target_index])
            return error.ProductionSocketPressureTargetUnavailable;
        const endpoint = try parseHttpBaseUriAddress(self.data_api_uris[target_index]);
        _ = self.sim.listenerConnectionCount(endpoint);

        try self.sim.setListenerConnectionLimit(endpoint, 0);
        var quota_active = true;
        errdefer if (quota_active) self.sim.setListenerConnectionLimit(endpoint, null) catch {};
        self.socket_pressure_injected = true;
        const ingress_before = self.public_request_ingress_count;

        {
            var denied_executor = io_http_executor.IoHttpExecutor.init(self.alloc, self.sim.io(), .{
                .connect_timeout_ms = 1_000,
                .read_timeout_ms = 1_000,
                .write_timeout_ms = 1_000,
                .keep_alive = false,
                .pool_max_connections = 1,
                .pool_max_per_host = 1,
            });
            defer {
                denied_executor.beginShutdown();
                denied_executor.drainShutdown();
                denied_executor.deinit();
            }
            var denied_client = api_http_client.ApiHttpClient.init(self.alloc, denied_executor.executor());
            if (denied_client.fetchLookupResponse(
                self.data_api_uris[target_index],
                "docs",
                "doc:k",
                null,
            )) |response_value| {
                var response = response_value;
                response.deinit(self.alloc);
                return error.ProductionSocketPressureRequestUnexpectedlyReachedServer;
            } else |err| {
                self.socket_pressure_error_code = @intFromError(err);
                self.socket_pressure_denial_observed = err == error.ProcessFdQuotaExceeded;
            }
        }
        self.socket_pressure_no_ingress = self.public_request_ingress_count == ingress_before;
        if (!self.socket_pressure_denial_observed or !self.socket_pressure_no_ingress)
            return error.ProductionSocketPressureDidNotDenyBeforeIngress;

        try self.sim.setListenerConnectionLimit(endpoint, null);
        quota_active = false;
        {
            var recovered_executor = io_http_executor.IoHttpExecutor.init(self.alloc, self.sim.io(), .{
                .connect_timeout_ms = 1_000,
                .read_timeout_ms = 1_000,
                .write_timeout_ms = 1_000,
                .keep_alive = false,
                .pool_max_connections = 1,
                .pool_max_per_host = 1,
            });
            defer {
                recovered_executor.beginShutdown();
                recovered_executor.drainShutdown();
                recovered_executor.deinit();
            }
            var recovered_client = api_http_client.ApiHttpClient.init(self.alloc, recovered_executor.executor());
            var response = try recovered_client.fetchLookupResponse(
                self.data_api_uris[target_index],
                "docs",
                "doc:k",
                null,
            );
            defer response.deinit(self.alloc);
            self.socket_pressure_recovered = response.status == 200 and
                std.mem.indexOf(u8, response.body, "production-split") != null;
        }
        if (!self.socket_pressure_recovered)
            return error.ProductionSocketPressureDidNotRecover;
    }

    fn saturateNodeMemory(self: *Fixture) !void {
        errdefer self.releaseNodeMemory();
        for (self.data_servers[0..self.data_server_count], 0..) |*server, index| {
            if (!self.data_server_live[index])
                return error.ProductionDataResourceOwnerUnavailable;
            const manager = &server.provisioned_storage.resource_manager;
            const memory = manager.snapshot().memory;
            if (memory.hard_limit_bytes == 0 or memory.used_bytes >= memory.hard_limit_bytes)
                return error.InvalidProductionDataResourceEnvelope;
            self.resource_reservations[index] = try manager.reserveBatchClassified(&.{.{
                .slice = .inference_model_residency,
                .bytes = memory.hard_limit_bytes - memory.used_bytes,
            }});
        }
    }

    fn releaseNodeMemory(self: *Fixture) void {
        for (&self.resource_reservations) |*slot| if (slot.*) |*reservation| {
            reservation.release();
            slot.* = null;
        };
    }

    fn allNodeMemorySaturated(self: *Fixture) bool {
        if (self.data_server_count != node_count) return false;
        for (self.data_servers[0..self.data_server_count], 0..) |*server, index| {
            if (!self.data_server_live[index]) return false;
            const memory = server.provisioned_storage.resource_manager.snapshot().memory;
            if (memory.hard_limit_bytes == 0 or memory.used_bytes != memory.hard_limit_bytes)
                return false;
        }
        return true;
    }

    fn nodeMemoryBelowHardLimit(self: *Fixture) bool {
        if (self.data_server_count != node_count) return false;
        for (self.data_servers[0..self.data_server_count], 0..) |*server, index| {
            if (!self.data_server_live[index]) return false;
            const memory = server.provisioned_storage.resource_manager.snapshot().memory;
            if (memory.hard_limit_bytes == 0 or memory.used_bytes >= memory.hard_limit_bytes)
                return false;
        }
        return true;
    }

    fn lookupAbsentEverywhere(
        self: *Fixture,
        table_name: []const u8,
        key: []const u8,
    ) !bool {
        for (0..4) |_| {
            var absent: usize = 0;
            for (self.data_api_uris[0..self.data_api_uri_count]) |uri| {
                var response = self.client.fetchLookupResponse(uri, table_name, key, null) catch break;
                defer response.deinit(self.alloc);
                if (response.status == 200) return false;
                if (response.status == 404) absent += 1;
            }
            if (absent == self.data_api_uri_count) return true;
            try self.sim.io().sleep(.fromMilliseconds(1), .awake);
        }
        return false;
    }

    fn lookupContains(
        self: *Fixture,
        table_name: []const u8,
        key: []const u8,
        expected: []const u8,
    ) !bool {
        // Span several production Raft ticks so a committed outcome-unknown
        // write can resume after capacity returns before the fixed-ID retry is
        // considered. This helper is resource-campaign-only.
        for (0..node_count * 32) |attempt| {
            const uri = self.data_api_uris[attempt % self.data_api_uri_count];
            var response = self.client.fetchLookupResponse(uri, table_name, key, null) catch {
                try self.sim.io().sleep(.fromMilliseconds(10), .awake);
                continue;
            };
            defer response.deinit(self.alloc);
            if (response.status == 200 and std.mem.indexOf(u8, response.body, expected) != null)
                return true;
            if (response.status != 200 and response.status != 404 and response.status != 503) {
                std.debug.print("production resource resolution read status={} body={s}\n", .{
                    response.status,
                    response.body,
                });
                return error.UnexpectedHttpStatus;
            }
            try self.sim.io().sleep(.fromMilliseconds(10), .awake);
        }
        return false;
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

    fn graphHydrationResponseComplete(self: *Fixture, body: []const u8) !bool {
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
        if (walk.total != 2 or nodes.len != 2) return false;
        for ([_]struct { key: []const u8, title: []const u8 }{
            .{ .key = "doc:x", .title = "production-right" },
            .{ .key = "doc:k", .title = "production-split" },
        }) |expected| {
            const node = for (nodes) |candidate| {
                if (std.mem.eql(u8, candidate.key, expected.key)) break candidate;
            } else return false;
            const document = node.document orelse return false;
            if (document != .object) return false;
            const title = document.object.get("title") orelse return false;
            if (title != .string or !std.mem.eql(u8, title.string, expected.title))
                return false;
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
                try self.refreshDataServerMetadataSnapshots();
                return true;
            }
            try self.runOneControlRound();
        }
        return false;
    }

    fn refreshDataServerMetadataSnapshots(self: *Fixture) !void {
        for (&self.data_servers, 0..) |*server, index| {
            if (!self.data_server_live[index]) continue;
            try server.refreshRemoteMetadataSnapshot();
        }
    }

    /// Publish a real metadata/data-plane split after the public coordinator
    /// has already acquired its source snapshot. Returning to the coordinator
    /// with refreshed production catalogs makes the retained snapshot stale;
    /// no graph response is fabricated by the harness.
    fn publishSplitAtStaleGraphBoundary(self: *Fixture) !void {
        try self.metadata.?.requestExternalDataSplit(
            split_transition_id,
            metadata_sim.VoprPublicClusterFixture.data_group_id,
            split_destination_group_id,
            split_key,
        );
        self.phase = .split_requested;
        try self.waitForSplitFinalized();
        self.split_finalized = true;
        self.phase = .split_finalized;
        std.debug.assert(!self.control_round_active);
        self.stopControlDriver();
        try self.metadata.?.retireExternalDataSplit(split_transition_id);
        self.split_published = true;
        self.phase = .split_published;
        try self.waitForDataLeader(split_destination_group_id);
        try self.refreshDataServerMetadataSnapshots();
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
            if (status != 200 and status != 404 and status != 409 and status != 503 and status != 504) {
                return .{ .failure = error.UnexpectedHttpStatus };
            }
            self.sim.io().sleep(.fromMilliseconds(self.driverCadenceMs()), .awake) catch |err|
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

    fn waitForStableDataLeaders(self: *Fixture) !void {
        var stable_rounds: usize = 0;
        for (0..30_000) |round| {
            // This fixture borrows the production control loop behind an
            // explicit driver. A restarted two-voter group can be momentarily
            // leaderful and then fall back to pre-candidate; keep the real
            // campaign/reconciliation owner advancing until every initial
            // group agrees on one leader for a sustained window.
            if (round % 8 == 0) try self.runOneControlRound();

            var all_stable = true;
            for (initial_groups) |group_id| {
                var replica_count: usize = 0;
                var leader_count: usize = 0;
                var leader_id: ?u64 = null;
                for (self.data_servers[0..self.data_server_count], 0..) |*server, index| {
                    if (!self.data_server_live[index]) continue;
                    const raft = server.data_raft orelse continue;
                    const status = raft.host.http_host.host.raftStatus(group_id) orelse continue;
                    replica_count += 1;
                    if (status.soft.role == .leader and
                        status.soft.leader_id != null and
                        status.soft.leader_id.? == status.id)
                    {
                        leader_count += 1;
                        leader_id = status.id;
                    }
                }
                if (replica_count != 2 or leader_count != 1) {
                    all_stable = false;
                    break;
                }
                for (self.data_servers[0..self.data_server_count], 0..) |*server, index| {
                    if (!self.data_server_live[index]) continue;
                    const raft = server.data_raft orelse continue;
                    const status = raft.host.http_host.host.raftStatus(group_id) orelse continue;
                    if (status.soft.leader_id != leader_id) {
                        all_stable = false;
                        break;
                    }
                }
                if (!all_stable) break;
            }
            if (all_stable) {
                stable_rounds += 1;
                if (stable_rounds == 32) return;
            } else {
                stable_rounds = 0;
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
        self.releaseNodeMemory();
        self.graph_transport_fault_armed = false;
        self.graph_transport_fault_endpoint = null;
        self.global_query_transport_failure_armed = false;
        self.global_query_transport_fault_endpoint = null;
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
        if (self.auth_manager_live) {
            self.auth_manager.deinit();
            self.auth_manager_live = false;
        }
        if (self.auth_policy_store_live) {
            self.auth_policy_store.deinit();
            self.auth_policy_store_live = false;
        }
        if (self.auth_store_live) {
            self.auth_store.deinit();
            self.auth_store_live = false;
        }
        var join_store_index = self.join_job_store_count;
        while (join_store_index > 0) {
            join_store_index -= 1;
            self.join_job_stores[join_store_index].deinit();
        }
        self.join_job_store_count = 0;
        var join_backend_index = self.join_job_backend_count;
        while (join_backend_index > 0) {
            join_backend_index -= 1;
            self.join_job_backends[join_backend_index].close();
        }
        self.join_job_backend_count = 0;
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
        if (self.global_query_restart_future) |*future| {
            future.cancel(self.sim.io());
            self.global_query_restart_future = null;
        }
        if (self.join_restart_future) |*future| {
            future.cancel(self.sim.io());
            self.join_restart_future = null;
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
        self.releaseNodeMemory();
        self.graph_transport_fault_armed = false;
        self.graph_transport_fault_endpoint = null;
        self.global_query_transport_failure_armed = false;
        self.global_query_transport_fault_endpoint = null;
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
        global_query_ok: bool,
        global_query_status: u16,
        global_query_response_count: usize,
        global_query_result_assembled_count: u64,
        global_query_cancellation_boundary_observed: bool,
        global_query_cancellation_requested: bool,
        global_query_cancellation_observed: bool,
        global_query_cancellation_no_partial: bool,
        global_query_cancellation_recovered: bool,
        global_query_cancellation_ok: bool,
        global_query_authorization_boundary_observed: bool,
        global_query_authorization_revoked: bool,
        global_query_authorization_denied_without_leak: bool,
        global_query_authorization_restored: bool,
        global_query_authorization_recovered: bool,
        global_query_authorization_denied_status: u16,
        global_query_authorization_recovered_status: u16,
        global_query_authorization_ok: bool,
        global_query_transport_boundary_observed: bool,
        global_query_transport_fault_injected: bool,
        global_query_transport_fault_observed: bool,
        global_query_transport_fault_matches: u64,
        global_query_transport_fault_healed: bool,
        global_query_transport_rejected_without_partial: bool,
        global_query_transport_recovered: bool,
        global_query_transport_rejected_status: u16,
        global_query_transport_recovered_status: u16,
        global_query_transport_ok: bool,
        global_query_owner_restart_boundary_observed: bool,
        global_query_owner_restart_down: bool,
        global_query_owner_restart_rejected_without_partial: bool,
        global_query_owner_restart_rejected_status: u16,
        global_query_owner_restart_reconstructed: bool,
        global_query_owner_restart_direct_read: bool,
        global_query_owner_restart_recovered: bool,
        global_query_owner_restart_recovered_status: u16,
        global_query_owner_restart_ok: bool,
        join_query_ok: bool,
        split_join_query_ok: bool,
        post_split_join_query_ok: bool,
        join_finalizer_ack_failure_injected: bool,
        join_finalizer_persisted_group_id: u64,
        durable_join_takeover_ok: bool,
        join_partition_worker_started_count: u64,
        join_partition_worker_completed_count: u64,
        join_worker_retry_failure_injected: bool,
        join_worker_retry_job_id: u64,
        join_worker_retry_partition_index: usize,
        join_worker_retry_failed_group_id: u64,
        join_worker_retry_recovered_group_id: u64,
        join_worker_retry_ok: bool,
        join_owner_restart_job_id: u64,
        join_owner_restart_partition_index: usize,
        join_owner_restart_failed_group_id: u64,
        join_owner_restart_recovered_group_id: u64,
        join_owner_restart_target_index: usize,
        join_owner_restart_recovery_index: usize,
        join_owner_restart_coordinator_index: usize,
        join_owner_restart_requested: bool,
        join_owner_restart_down: bool,
        join_owner_restart_recovered: bool,
        join_owner_restart_initial_status: u16,
        join_owner_restart_initial_rejected_without_partial: bool,
        join_owner_restart_recovery_join: bool,
        join_owner_restart_post_reconstruction_read: bool,
        join_owner_restart_ok: bool,
        join_retry_exhaustion_job_id: u64,
        join_retry_exhaustion_partition_index: usize,
        join_retry_exhaustion_first_group_id: u64,
        join_retry_exhaustion_retry_group_id: u64,
        join_retry_exhaustion_coordinator_index: usize,
        join_retry_exhaustion_retry_target_index: usize,
        join_retry_exhaustion_faults_injected: bool,
        join_retry_exhaustion_resource_observed: bool,
        join_retry_exhaustion_network_observed: bool,
        join_retry_exhaustion_overlap_observed: bool,
        join_retry_exhaustion_initial_worker_starts: u64,
        join_retry_exhaustion_initial_worker_completions: u64,
        join_retry_exhaustion_initial_status: u16,
        join_retry_exhaustion_initial_rejected_without_partial: bool,
        join_retry_exhaustion_network_healed: bool,
        join_retry_exhaustion_resource_healed: bool,
        join_retry_exhaustion_recovery_join: bool,
        join_retry_exhaustion_ok: bool,
        join_cancellation_boundary_observed: bool,
        join_cancellation_job_id: u64,
        join_cancellation_owner_group_id: u64,
        join_cancellation_requested: bool,
        join_cancellation_observed: bool,
        join_cancellation_recovered: bool,
        join_cancellation_ok: bool,
        join_cancellation_overlap_first_group_id: u64,
        join_cancellation_overlap_worker_group_id: u64,
        join_cancellation_overlap_coordinator_index: usize,
        join_cancellation_overlap_network_target_index: usize,
        join_cancellation_overlap_faults_injected: bool,
        join_cancellation_overlap_network_observed: bool,
        join_cancellation_overlap_resource_observed: bool,
        join_cancellation_overlap_observed: bool,
        join_cancellation_overlap_network_healed: bool,
        join_cancellation_overlap_resource_healed: bool,
        graph_query_ok: bool,
        graph_hydration_ok: bool,
        graph_hydration_started_count: u64,
        graph_hydration_fanout_started_count: u64,
        graph_hydration_completed_count: u64,
        graph_cancellation_requested: bool,
        graph_cancellation_observed: bool,
        graph_cancellation_recovered: bool,
        graph_cancellation_ok: bool,
        graph_cancellation_fault_injected: bool,
        graph_cancellation_fault_observed: bool,
        graph_cancellation_fault_matches: u64,
        graph_cancellation_fault_healed: bool,
        graph_authorization_boundary_observed: bool,
        graph_authorization_revoked: bool,
        graph_authorization_denied_without_leak: bool,
        graph_authorization_restored: bool,
        graph_authorization_recovered: bool,
        graph_authorization_denied_status: u16,
        graph_authorization_recovered_status: u16,
        graph_authorization_ok: bool,
        graph_stale_snapshot_boundary_observed: bool,
        graph_stale_snapshot_attempt_failures: u64,
        graph_stale_snapshot_error_code: u16,
        graph_stale_snapshot_rejected_without_partial: bool,
        graph_stale_snapshot_status: u16,
        graph_stale_snapshot_recovered: bool,
        graph_stale_snapshot_ok: bool,
        split_graph_inflight_started: bool,
        split_graph_inflight_complete: bool,
        split_graph_inflight_rejected: bool,
        split_graph_inflight_ok: bool,
        post_split_graph_query_ok: bool,
        graph_transport_failure_injected: bool,
        graph_transport_failure_observed: bool,
        graph_transport_failure_error_code: u16,
        overlapping_faults_active_observed: bool,
        graph_owner_restart_requested: bool,
        graph_owner_restart_down: bool,
        graph_owner_restart_failure_observed: bool,
        graph_owner_restart_recovered: bool,
        graph_owner_restart_error_code: u16,
        graph_partial_write_injected: bool,
        graph_partial_write_observed: bool,
        socket_pressure_injected: bool,
        socket_pressure_denial_observed: bool,
        socket_pressure_error_code: u16,
        socket_pressure_no_ingress: bool,
        socket_pressure_recovered: bool,
        resource_pressure_observed: bool,
        resource_denial_ok: bool,
        resource_denial_status: u16,
        resource_preproposal_denial: bool,
        resource_outcome_unknown: bool,
        resource_read_before_retry: bool,
        resource_retry_attempted: bool,
        resource_proposals_before: u64,
        resource_proposals_after: u64,
        resource_absent_before_retry: bool,
        resource_recovery_ok: bool,
        resource_post_split_ok: bool,
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
                (!self.global_query_enabled or self.global_query_sound) and
                (!self.join_enabled or self.join_sound) and
                (!self.join_cancellation_enabled or self.join_cancellation_sound) and
                (!self.join_worker_retry_enabled or self.join_worker_retry_sound) and
                (!self.join_owner_restart_enabled or self.join_owner_restart_sound) and
                (!self.join_retry_exhaustion_enabled or self.join_retry_exhaustion_sound) and
                (!self.graph_enabled or self.graph_sound) and
                (!self.graph_hydration_enabled or self.graph_hydration_sound) and
                (!self.graph_cancellation_enabled or self.graph_cancellation_sound) and
                (!self.graph_inflight_authorization_revocation_enabled or self.graph_authorization_sound) and
                (!self.graph_stale_snapshot_retry_exhaustion_enabled or self.graph_stale_snapshot_sound) and
                (!self.active_split_enabled or self.split_sound) and
                self.failure == null,
            .topology_ok = self.topology_sound,
            .global_query_ok = self.global_query_sound,
            .global_query_status = self.global_query_status,
            .global_query_response_count = self.global_query_response_count,
            .global_query_result_assembled_count = self.global_query_result_assembled_count,
            .global_query_cancellation_boundary_observed = self.global_query_cancellation_boundary_observed,
            .global_query_cancellation_requested = self.global_query_cancellation_requested,
            .global_query_cancellation_observed = self.global_query_cancellation_observed,
            .global_query_cancellation_no_partial = self.global_query_cancellation_no_partial,
            .global_query_cancellation_recovered = self.global_query_cancellation_recovered,
            .global_query_cancellation_ok = self.global_query_cancellation_sound,
            .global_query_authorization_boundary_observed = self.global_query_authorization_boundary_observed,
            .global_query_authorization_revoked = self.global_query_authorization_revoked,
            .global_query_authorization_denied_without_leak = self.global_query_authorization_denied_without_leak,
            .global_query_authorization_restored = self.global_query_authorization_restored,
            .global_query_authorization_recovered = self.global_query_authorization_recovered,
            .global_query_authorization_denied_status = self.global_query_authorization_denied_status,
            .global_query_authorization_recovered_status = self.global_query_authorization_recovered_status,
            .global_query_authorization_ok = self.global_query_authorization_sound,
            .global_query_transport_boundary_observed = self.global_query_transport_boundary_observed,
            .global_query_transport_fault_injected = self.global_query_transport_fault_injected,
            .global_query_transport_fault_observed = self.global_query_transport_fault_observed,
            .global_query_transport_fault_matches = self.global_query_transport_fault_matches,
            .global_query_transport_fault_healed = self.global_query_transport_fault_healed,
            .global_query_transport_rejected_without_partial = self.global_query_transport_rejected_without_partial,
            .global_query_transport_recovered = self.global_query_transport_recovered,
            .global_query_transport_rejected_status = self.global_query_transport_rejected_status,
            .global_query_transport_recovered_status = self.global_query_transport_recovered_status,
            .global_query_transport_ok = self.global_query_transport_sound,
            .global_query_owner_restart_boundary_observed = self.global_query_owner_restart_boundary_observed,
            .global_query_owner_restart_down = self.global_query_owner_restart_down,
            .global_query_owner_restart_rejected_without_partial = self.global_query_owner_restart_rejected_without_partial,
            .global_query_owner_restart_rejected_status = self.global_query_owner_restart_rejected_status,
            .global_query_owner_restart_reconstructed = self.global_query_owner_restart_reconstructed,
            .global_query_owner_restart_direct_read = self.global_query_owner_restart_direct_read,
            .global_query_owner_restart_recovered = self.global_query_owner_restart_recovered,
            .global_query_owner_restart_recovered_status = self.global_query_owner_restart_recovered_status,
            .global_query_owner_restart_ok = self.global_query_owner_restart_sound,
            .join_query_ok = self.join_sound,
            .split_join_query_ok = self.split_join_sound,
            .post_split_join_query_ok = self.post_split_join_sound,
            .join_finalizer_ack_failure_injected = self.join_finalizer_ack_failure_injected,
            .join_finalizer_persisted_group_id = self.join_finalizer_persisted_group_id,
            .durable_join_takeover_ok = self.durable_join_takeover_sound,
            .join_partition_worker_started_count = self.join_partition_worker_started_count,
            .join_partition_worker_completed_count = self.join_partition_worker_completed_count,
            .join_worker_retry_failure_injected = self.join_worker_retry_failure_injected,
            .join_worker_retry_job_id = self.join_worker_retry_job_id,
            .join_worker_retry_partition_index = self.join_worker_retry_partition_index,
            .join_worker_retry_failed_group_id = self.join_worker_retry_failed_group_id,
            .join_worker_retry_recovered_group_id = self.join_worker_retry_recovered_group_id,
            .join_worker_retry_ok = self.join_worker_retry_sound,
            .join_owner_restart_job_id = self.join_owner_restart_job_id,
            .join_owner_restart_partition_index = self.join_owner_restart_partition_index,
            .join_owner_restart_failed_group_id = self.join_owner_restart_failed_group_id,
            .join_owner_restart_recovered_group_id = self.join_owner_restart_recovered_group_id,
            .join_owner_restart_target_index = self.join_owner_restart_target_index,
            .join_owner_restart_recovery_index = self.join_owner_restart_recovery_index,
            .join_owner_restart_coordinator_index = self.join_owner_restart_coordinator_index,
            .join_owner_restart_requested = self.join_owner_restart_requested,
            .join_owner_restart_down = self.join_owner_restart_down,
            .join_owner_restart_recovered = self.join_owner_restart_recovered,
            .join_owner_restart_initial_status = self.join_owner_restart_initial_status,
            .join_owner_restart_initial_rejected_without_partial = self.join_owner_restart_initial_rejected_without_partial,
            .join_owner_restart_recovery_join = self.join_owner_restart_recovery_join,
            .join_owner_restart_post_reconstruction_read = self.join_owner_restart_post_reconstruction_read,
            .join_owner_restart_ok = self.join_owner_restart_sound,
            .join_retry_exhaustion_job_id = self.join_retry_exhaustion_job_id,
            .join_retry_exhaustion_partition_index = self.join_retry_exhaustion_partition_index,
            .join_retry_exhaustion_first_group_id = self.join_retry_exhaustion_first_group_id,
            .join_retry_exhaustion_retry_group_id = self.join_retry_exhaustion_retry_group_id,
            .join_retry_exhaustion_coordinator_index = self.join_retry_exhaustion_coordinator_index,
            .join_retry_exhaustion_retry_target_index = self.join_retry_exhaustion_retry_target_index,
            .join_retry_exhaustion_faults_injected = self.join_retry_exhaustion_faults_injected,
            .join_retry_exhaustion_resource_observed = self.join_retry_exhaustion_resource_observed,
            .join_retry_exhaustion_network_observed = self.join_retry_exhaustion_network_observed,
            .join_retry_exhaustion_overlap_observed = self.join_retry_exhaustion_overlap_observed,
            .join_retry_exhaustion_initial_worker_starts = self.join_retry_exhaustion_initial_worker_starts,
            .join_retry_exhaustion_initial_worker_completions = self.join_retry_exhaustion_initial_worker_completions,
            .join_retry_exhaustion_initial_status = self.join_retry_exhaustion_initial_status,
            .join_retry_exhaustion_initial_rejected_without_partial = self.join_retry_exhaustion_initial_rejected_without_partial,
            .join_retry_exhaustion_network_healed = self.join_retry_exhaustion_network_healed,
            .join_retry_exhaustion_resource_healed = self.join_retry_exhaustion_resource_healed,
            .join_retry_exhaustion_recovery_join = self.join_retry_exhaustion_recovery_join,
            .join_retry_exhaustion_ok = self.join_retry_exhaustion_sound,
            .join_cancellation_boundary_observed = self.join_cancellation_boundary_observed,
            .join_cancellation_job_id = self.join_cancellation_job_id,
            .join_cancellation_owner_group_id = self.join_cancellation_owner_group_id,
            .join_cancellation_requested = self.join_cancellation_requested,
            .join_cancellation_observed = self.join_cancellation_observed,
            .join_cancellation_recovered = self.join_cancellation_recovered,
            .join_cancellation_ok = self.join_cancellation_sound,
            .join_cancellation_overlap_first_group_id = self.join_cancellation_overlap_first_group_id,
            .join_cancellation_overlap_worker_group_id = self.join_cancellation_overlap_worker_group_id,
            .join_cancellation_overlap_coordinator_index = self.join_cancellation_overlap_coordinator_index,
            .join_cancellation_overlap_network_target_index = self.join_cancellation_overlap_network_target_index,
            .join_cancellation_overlap_faults_injected = self.join_cancellation_overlap_faults_injected,
            .join_cancellation_overlap_network_observed = self.join_cancellation_overlap_network_observed,
            .join_cancellation_overlap_resource_observed = self.join_cancellation_overlap_resource_observed,
            .join_cancellation_overlap_observed = self.join_cancellation_overlap_observed,
            .join_cancellation_overlap_network_healed = self.join_cancellation_overlap_network_healed,
            .join_cancellation_overlap_resource_healed = self.join_cancellation_overlap_resource_healed,
            .graph_query_ok = self.graph_sound,
            .graph_hydration_ok = self.graph_hydration_sound,
            .graph_hydration_started_count = self.graph_hydration_started_count,
            .graph_hydration_fanout_started_count = self.graph_hydration_fanout_started_count,
            .graph_hydration_completed_count = self.graph_hydration_completed_count,
            .graph_cancellation_requested = self.graph_cancellation_requested,
            .graph_cancellation_observed = self.graph_cancellation_observed,
            .graph_cancellation_recovered = self.graph_cancellation_recovered,
            .graph_cancellation_ok = self.graph_cancellation_sound,
            .graph_cancellation_fault_injected = self.graph_cancellation_fault_injected,
            .graph_cancellation_fault_observed = self.graph_cancellation_fault_observed,
            .graph_cancellation_fault_matches = self.graph_cancellation_fault_matches,
            .graph_cancellation_fault_healed = self.graph_cancellation_fault_healed,
            .graph_authorization_boundary_observed = self.graph_authorization_boundary_observed,
            .graph_authorization_revoked = self.graph_authorization_revoked,
            .graph_authorization_denied_without_leak = self.graph_authorization_denied_without_leak,
            .graph_authorization_restored = self.graph_authorization_restored,
            .graph_authorization_recovered = self.graph_authorization_recovered,
            .graph_authorization_denied_status = self.graph_authorization_denied_status,
            .graph_authorization_recovered_status = self.graph_authorization_recovered_status,
            .graph_authorization_ok = self.graph_authorization_sound,
            .graph_stale_snapshot_boundary_observed = self.graph_stale_snapshot_boundary_observed,
            .graph_stale_snapshot_attempt_failures = self.graph_stale_snapshot_attempt_failures,
            .graph_stale_snapshot_error_code = self.graph_stale_snapshot_error_code,
            .graph_stale_snapshot_rejected_without_partial = self.graph_stale_snapshot_rejected_without_partial,
            .graph_stale_snapshot_status = self.graph_stale_snapshot_status,
            .graph_stale_snapshot_recovered = self.graph_stale_snapshot_recovered,
            .graph_stale_snapshot_ok = self.graph_stale_snapshot_sound,
            .split_graph_inflight_started = self.split_graph_inflight_started,
            .split_graph_inflight_complete = self.split_graph_inflight_complete,
            .split_graph_inflight_rejected = self.split_graph_inflight_rejected,
            .split_graph_inflight_ok = self.split_graph_inflight_sound,
            .post_split_graph_query_ok = self.post_split_graph_sound,
            .graph_transport_failure_injected = self.graph_transport_failure_injected,
            .graph_transport_failure_observed = self.graph_transport_failure_observed,
            .graph_transport_failure_error_code = self.graph_transport_failure_error_code,
            .overlapping_faults_active_observed = self.overlapping_faults_active_observed,
            .graph_owner_restart_requested = self.graph_owner_restart_requested,
            .graph_owner_restart_down = self.graph_owner_restart_down,
            .graph_owner_restart_failure_observed = self.graph_owner_restart_failure_observed,
            .graph_owner_restart_recovered = self.graph_owner_restart_recovered,
            .graph_owner_restart_error_code = self.graph_owner_restart_error_code,
            .graph_partial_write_injected = self.graph_partial_write_injected,
            .graph_partial_write_observed = self.graph_partial_write_observed,
            .socket_pressure_injected = self.socket_pressure_injected,
            .socket_pressure_denial_observed = self.socket_pressure_denial_observed,
            .socket_pressure_error_code = self.socket_pressure_error_code,
            .socket_pressure_no_ingress = self.socket_pressure_no_ingress,
            .socket_pressure_recovered = self.socket_pressure_recovered,
            .resource_pressure_observed = self.resource_pressure_observed,
            .resource_denial_ok = self.resource_denial_sound,
            .resource_denial_status = self.resource_denial_status,
            .resource_preproposal_denial = self.resource_preproposal_denial,
            .resource_outcome_unknown = self.resource_outcome_unknown,
            .resource_read_before_retry = self.resource_read_before_retry,
            .resource_retry_attempted = self.resource_retry_attempted,
            .resource_proposals_before = self.resource_proposals_before,
            .resource_proposals_after = self.resource_proposals_after,
            .resource_absent_before_retry = self.resource_absent_before_retry,
            .resource_recovery_ok = self.resource_recovery_sound,
            .resource_post_split_ok = self.resource_post_split_sound,
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
            // A stable-identity restart keeps the array slot occupied while
            // the old runtime is destroyed and the replacement is brought to
            // readiness. `live` alone is intentionally published during
            // construction; `paused` is the process-level exclusion boundary
            // for observers and control/Raft drivers across the whole restart.
            if (self.data_server_paused[index] or !self.data_server_live[index]) continue;
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
