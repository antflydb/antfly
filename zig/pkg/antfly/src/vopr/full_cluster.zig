// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Deployment-shaped exact-replay composition. Hosted-data fault campaigns and
//! production-DataServer campaigns share a metadata quorum, three public API
//! nodes, concurrent clients, and a production serverless worker on one VoprIo
//! scheduler and virtual clock.

const std = @import("std");
const vopr = @import("vopr");
const data_runtime = @import("../data/runtime.zig");
const distributed_graph = @import("../api/distributed_graph.zig");
const metadata_sim = @import("../metadata/sim_harness.zig");
const production_cluster = @import("production_cluster.zig");
const serverless_workflow = @import("serverless_workflow.zig");
const serverless_runtime = @import("../serverless/runtime/manager.zig");
const FixtureAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

pub const Scenario = struct {
    pub const name: []const u8 = "full-cluster";
    pub const version: u32 = 39;

    const acknowledged_id = vopr.id.stable(name, "acknowledged-data-visible");
    const quorum_id = vopr.id.stable(name, "metadata-quorum-recovers");
    const routing_id = vopr.id.stable(name, "non-host-routing-sound");
    const isolation_id = vopr.id.stable(name, "multi-table-isolation");
    const graph_query_id = vopr.id.stable(name, "public-cross-range-graph-query-sound");
    const graph_split_id = vopr.id.stable(name, "public-graph-survives-active-split");
    const graph_split_transport_id = vopr.id.stable(name, "public-graph-active-split-transport-fails-closed");
    const graph_split_owner_restart_id = vopr.id.stable(name, "public-graph-active-split-owner-restart-fails-closed");
    const graph_split_partial_write_id = vopr.id.stable(name, "public-graph-active-split-partial-write-completes");
    const production_resource_split_id = vopr.id.stable(name, "production-owner-resource-denial-recovers-during-split");
    const production_join_split_id = vopr.id.stable(name, "public-distributed-join-survives-active-split");
    const production_durable_join_takeover_id = vopr.id.stable(name, "public-durable-shuffle-finalizer-takeover");
    const production_join_cancellation_id = vopr.id.stable(name, "public-durable-shuffle-cancellation-drains-worker");
    const production_join_worker_retry_id = vopr.id.stable(name, "public-durable-shuffle-partition-worker-failover");
    const production_join_owner_restart_id = vopr.id.stable(name, "public-durable-shuffle-partition-owner-reconstruction");
    const production_join_retry_exhaustion_id = vopr.id.stable(name, "public-durable-shuffle-overlapping-fault-retry-exhaustion");
    const production_join_cancellation_overlap_id = vopr.id.stable(name, "public-durable-shuffle-cancellation-under-overlapping-faults");
    const production_join_cancellation_owner_restart_id = vopr.id.stable(name, "public-durable-shuffle-cancellation-owner-reconstruction");
    const production_overlapping_faults_id = vopr.id.stable(name, "production-graph-overlapping-link-resource-faults-recover");
    const production_socket_pressure_id = vopr.id.stable(name, "production-listener-socket-pressure-recovers-during-split");
    const production_service_rate_id = vopr.id.stable(name, "production-service-rates-compose-and-heal");
    const production_graph_hydration_id = vopr.id.stable(name, "production-public-graph-hydrates-documents");
    const production_graph_cancellation_id = vopr.id.stable(name, "production-public-graph-cancellation-drains-fanout");
    const production_graph_cancellation_transport_id = vopr.id.stable(name, "production-public-graph-cancellation-under-transport-fault");
    const production_graph_authorization_id = vopr.id.stable(name, "production-public-graph-inflight-authorization-revocation");
    const production_graph_stale_snapshot_id = vopr.id.stable(name, "production-public-graph-stale-snapshot-retry-exhaustion");
    const production_global_query_id = vopr.id.stable(name, "production-public-global-query-isolates-table-results");
    const production_global_query_cancellation_id = vopr.id.stable(name, "production-public-global-query-cancellation-fails-closed");
    const production_global_query_authorization_id = vopr.id.stable(name, "production-public-global-query-inflight-authorization-revocation");
    const production_global_query_transport_id = vopr.id.stable(name, "production-public-global-query-transport-fails-closed");
    const graph_restart_id = vopr.id.stable(name, "public-graph-inflight-restart-sound");
    const graph_topology_id = vopr.id.stable(name, "public-graph-topology-churn-fails-closed");
    const graph_partial_id = vopr.id.stable(name, "public-graph-partial-result-rejected");
    const publication_id = vopr.id.stable(name, "serverless-publication-visible");
    const serverless_http_id = vopr.id.stable(name, "serverless-public-http-visible");
    const shared_io_id = vopr.id.stable(name, "one-shared-vopr-io");
    const raft_wire_id = vopr.id.stable(name, "raft-crosses-vopr-http-wire");
    const node_resources_id = vopr.id.stable(name, "node-resource-owners-distinct");
    const resource_recovery_id = vopr.id.stable(name, "node-resource-denial-recovers");
    const deployment_id = vopr.id.stable(name, "registered-deployment-quiesces");
    const cleanup_id = vopr.id.stable(name, "cluster-resources-cleaned");
    const complete_id = vopr.id.stable(name, "history-completes");

    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = acknowledged_id, .name = name ++ ".acknowledged-data-visible", .kind = .always },
        .{ .id = quorum_id, .name = name ++ ".metadata-quorum-recovers", .kind = .always },
        .{ .id = routing_id, .name = name ++ ".non-host-routing-sound", .kind = .always },
        .{ .id = isolation_id, .name = name ++ ".multi-table-isolation", .kind = .always },
        .{ .id = graph_query_id, .name = name ++ ".public-cross-range-graph-query-sound", .kind = .always },
        .{ .id = graph_split_id, .name = name ++ ".public-graph-survives-active-split", .kind = .always },
        .{ .id = graph_split_transport_id, .name = name ++ ".public-graph-active-split-transport-fails-closed", .kind = .always },
        .{ .id = graph_split_owner_restart_id, .name = name ++ ".public-graph-active-split-owner-restart-fails-closed", .kind = .always },
        .{ .id = graph_split_partial_write_id, .name = name ++ ".public-graph-active-split-partial-write-completes", .kind = .always },
        .{ .id = production_resource_split_id, .name = name ++ ".production-owner-resource-denial-recovers-during-split", .kind = .always },
        .{ .id = production_join_split_id, .name = name ++ ".public-distributed-join-survives-active-split", .kind = .always },
        .{ .id = production_durable_join_takeover_id, .name = name ++ ".public-durable-shuffle-finalizer-takeover", .kind = .always },
        .{ .id = production_join_cancellation_id, .name = name ++ ".public-durable-shuffle-cancellation-drains-worker", .kind = .always },
        .{ .id = production_join_worker_retry_id, .name = name ++ ".public-durable-shuffle-partition-worker-failover", .kind = .always },
        .{ .id = production_join_owner_restart_id, .name = name ++ ".public-durable-shuffle-partition-owner-reconstruction", .kind = .always },
        .{ .id = production_join_retry_exhaustion_id, .name = name ++ ".public-durable-shuffle-overlapping-fault-retry-exhaustion", .kind = .always },
        .{ .id = production_join_cancellation_overlap_id, .name = name ++ ".public-durable-shuffle-cancellation-under-overlapping-faults", .kind = .always },
        .{ .id = production_join_cancellation_owner_restart_id, .name = name ++ ".public-durable-shuffle-cancellation-owner-reconstruction", .kind = .always },
        .{ .id = production_overlapping_faults_id, .name = name ++ ".production-graph-overlapping-link-resource-faults-recover", .kind = .always },
        .{ .id = production_socket_pressure_id, .name = name ++ ".production-listener-socket-pressure-recovers-during-split", .kind = .always },
        .{ .id = production_service_rate_id, .name = name ++ ".production-service-rates-compose-and-heal", .kind = .always },
        .{ .id = production_graph_hydration_id, .name = name ++ ".production-public-graph-hydrates-documents", .kind = .always },
        .{ .id = production_graph_cancellation_id, .name = name ++ ".production-public-graph-cancellation-drains-fanout", .kind = .always },
        .{ .id = production_graph_cancellation_transport_id, .name = name ++ ".production-public-graph-cancellation-under-transport-fault", .kind = .always },
        .{ .id = production_graph_authorization_id, .name = name ++ ".production-public-graph-inflight-authorization-revocation", .kind = .always },
        .{ .id = production_graph_stale_snapshot_id, .name = name ++ ".production-public-graph-stale-snapshot-retry-exhaustion", .kind = .always },
        .{ .id = production_global_query_id, .name = name ++ ".production-public-global-query-isolates-table-results", .kind = .always },
        .{ .id = production_global_query_cancellation_id, .name = name ++ ".production-public-global-query-cancellation-fails-closed", .kind = .always },
        .{ .id = production_global_query_authorization_id, .name = name ++ ".production-public-global-query-inflight-authorization-revocation", .kind = .always },
        .{ .id = production_global_query_transport_id, .name = name ++ ".production-public-global-query-transport-fails-closed", .kind = .always },
        .{ .id = graph_restart_id, .name = name ++ ".public-graph-inflight-restart-sound", .kind = .always },
        .{ .id = graph_topology_id, .name = name ++ ".public-graph-topology-churn-fails-closed", .kind = .always },
        .{ .id = graph_partial_id, .name = name ++ ".public-graph-partial-result-rejected", .kind = .always },
        .{ .id = publication_id, .name = name ++ ".serverless-publication-visible", .kind = .always },
        .{ .id = serverless_http_id, .name = name ++ ".serverless-public-http-visible", .kind = .always },
        .{ .id = shared_io_id, .name = name ++ ".one-shared-vopr-io", .kind = .always },
        .{ .id = raft_wire_id, .name = name ++ ".raft-crosses-vopr-http-wire", .kind = .always },
        .{ .id = node_resources_id, .name = name ++ ".node-resource-owners-distinct", .kind = .always },
        .{ .id = resource_recovery_id, .name = name ++ ".node-resource-denial-recovers", .kind = .always },
        .{ .id = deployment_id, .name = name ++ ".registered-deployment-quiesces", .kind = .always },
        .{ .id = cleanup_id, .name = name ++ ".cluster-resources-cleaned", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".history-completes", .kind = .reachable },
    };

    const PublicFault = metadata_sim.VoprPublicClusterFixture.FaultMode;
    const Mode = enum {
        clean,
        metadata_partition,
        node_restart,
        graph_inflight_restart,
        graph_topology_churn,
        graph_transport_failure,
        partial_http_write,
        serverless_stale_generation,
        resource_pressure,
        production_data_plane_baseline,
        production_data_plane_graph,
        production_data_plane,
        production_data_plane_graph_split,
        production_data_plane_graph_split_transport_failure,
        production_data_plane_graph_split_owner_restart,
        production_data_plane_graph_split_partial_write,
        production_data_plane_graph_split_resource_pressure,
        production_data_plane_join_split,
        production_data_plane_durable_join_takeover,
        production_data_plane_durable_join_cancellation,
        production_data_plane_durable_join_worker_retry,
        production_data_plane_durable_join_owner_restart,
        production_data_plane_durable_join_retry_exhaustion,
        production_data_plane_durable_join_cancellation_overlapping_faults,
        production_data_plane_durable_join_cancellation_owner_restart,
        production_data_plane_graph_split_overlapping_faults,
        production_data_plane_graph_split_socket_pressure,
        production_data_plane_service_rate,
        production_data_plane_graph_hydration,
        production_data_plane_graph_cancellation,
        production_data_plane_graph_cancellation_transport_failure,
        production_data_plane_graph_inflight_authorization_revocation,
        production_data_plane_graph_stale_snapshot_retry_exhaustion,
        production_data_plane_global_query,
        production_data_plane_global_query_cancellation,
        production_data_plane_global_query_inflight_authorization_revocation,
        production_data_plane_global_query_transport_failure,

        fn isProduction(self: Mode) bool {
            return self == .production_data_plane_baseline or
                self == .production_data_plane_graph or
                self == .production_data_plane or
                self == .production_data_plane_graph_split or
                self == .production_data_plane_graph_split_transport_failure or
                self == .production_data_plane_graph_split_owner_restart or
                self == .production_data_plane_graph_split_partial_write or
                self == .production_data_plane_graph_split_resource_pressure or
                self == .production_data_plane_join_split or
                self == .production_data_plane_durable_join_takeover or
                self == .production_data_plane_durable_join_cancellation or
                self == .production_data_plane_durable_join_worker_retry or
                self == .production_data_plane_durable_join_owner_restart or
                self == .production_data_plane_durable_join_retry_exhaustion or
                self == .production_data_plane_durable_join_cancellation_overlapping_faults or
                self == .production_data_plane_durable_join_cancellation_owner_restart or
                self == .production_data_plane_graph_split_overlapping_faults or
                self == .production_data_plane_graph_split_socket_pressure or
                self == .production_data_plane_service_rate or
                self == .production_data_plane_graph_hydration or
                self == .production_data_plane_graph_cancellation or
                self == .production_data_plane_graph_cancellation_transport_failure or
                self == .production_data_plane_graph_inflight_authorization_revocation or
                self == .production_data_plane_graph_stale_snapshot_retry_exhaustion or
                self == .production_data_plane_global_query or
                self == .production_data_plane_global_query_cancellation or
                self == .production_data_plane_global_query_inflight_authorization_revocation or
                self == .production_data_plane_global_query_transport_failure;
        }

        fn publicFault(self: Mode) PublicFault {
            return switch (self) {
                .clean, .serverless_stale_generation, .production_data_plane_baseline, .production_data_plane_graph, .production_data_plane, .production_data_plane_graph_split, .production_data_plane_graph_split_transport_failure, .production_data_plane_graph_split_owner_restart, .production_data_plane_graph_split_partial_write, .production_data_plane_graph_split_resource_pressure, .production_data_plane_join_split, .production_data_plane_durable_join_takeover, .production_data_plane_durable_join_cancellation, .production_data_plane_durable_join_worker_retry, .production_data_plane_durable_join_owner_restart, .production_data_plane_durable_join_retry_exhaustion, .production_data_plane_durable_join_cancellation_overlapping_faults, .production_data_plane_durable_join_cancellation_owner_restart, .production_data_plane_graph_split_overlapping_faults, .production_data_plane_graph_split_socket_pressure, .production_data_plane_service_rate, .production_data_plane_graph_hydration, .production_data_plane_graph_cancellation, .production_data_plane_graph_cancellation_transport_failure, .production_data_plane_graph_inflight_authorization_revocation, .production_data_plane_graph_stale_snapshot_retry_exhaustion, .production_data_plane_global_query, .production_data_plane_global_query_cancellation, .production_data_plane_global_query_inflight_authorization_revocation, .production_data_plane_global_query_transport_failure => .clean,
                .metadata_partition => .metadata_partition,
                .node_restart => .node_restart,
                .graph_inflight_restart => .graph_inflight_restart,
                .graph_topology_churn => .graph_topology_churn,
                .graph_transport_failure => .graph_transport_failure,
                .partial_http_write => .partial_http_write,
                .resource_pressure => .resource_pressure,
            };
        }

        fn serverlessMode(self: Mode) serverless_workflow.Scenario.Mode {
            return if (self == .serverless_stale_generation)
                .stale_enrichment_generation
            else
                .clean;
        }
    };
    const mode_ids = ids: {
        @setEvalBranchQuota(10_000);
        break :ids [_]vopr.id.StableId{
            vopr.id.stable(name, "clean"),
            vopr.id.stable(name, "metadata-partition"),
            vopr.id.stable(name, "node-restart"),
            vopr.id.stable(name, "graph-inflight-restart"),
            vopr.id.stable(name, "graph-topology-churn"),
            vopr.id.stable(name, "graph-transport-failure"),
            vopr.id.stable(name, "partial-http-write"),
            vopr.id.stable(name, "serverless-stale-generation"),
            vopr.id.stable(name, "resource-pressure"),
            vopr.id.stable(name, "production-data-plane-baseline"),
            vopr.id.stable(name, "production-data-plane-graph"),
            vopr.id.stable(name, "production-data-plane"),
            vopr.id.stable(name, "production-data-plane-graph-split"),
            vopr.id.stable(name, "production-data-plane-graph-split-transport-failure"),
            vopr.id.stable(name, "production-data-plane-graph-split-owner-restart"),
            vopr.id.stable(name, "production-data-plane-graph-split-partial-write"),
            vopr.id.stable(name, "production-data-plane-graph-split-resource-pressure"),
            vopr.id.stable(name, "production-data-plane-join-split"),
            vopr.id.stable(name, "production-data-plane-durable-join-takeover"),
            vopr.id.stable(name, "production-data-plane-durable-join-cancellation"),
            vopr.id.stable(name, "production-data-plane-durable-join-worker-retry"),
            vopr.id.stable(name, "production-data-plane-durable-join-owner-restart"),
            vopr.id.stable(name, "production-data-plane-durable-join-retry-exhaustion"),
            vopr.id.stable(name, "production-data-plane-durable-join-cancellation-overlapping-faults"),
            vopr.id.stable(name, "production-data-plane-durable-join-cancellation-owner-restart"),
            vopr.id.stable(name, "production-data-plane-graph-split-overlapping-faults"),
            vopr.id.stable(name, "production-data-plane-graph-split-socket-pressure"),
            vopr.id.stable(name, "production-data-plane-service-rate"),
            vopr.id.stable(name, "production-data-plane-graph-hydration"),
            vopr.id.stable(name, "production-data-plane-graph-cancellation"),
            vopr.id.stable(name, "production-data-plane-graph-cancellation-transport-failure"),
            vopr.id.stable(name, "production-data-plane-graph-inflight-authorization-revocation"),
            vopr.id.stable(name, "production-data-plane-graph-stale-snapshot-retry-exhaustion"),
            vopr.id.stable(name, "production-data-plane-global-query"),
            vopr.id.stable(name, "production-data-plane-global-query-cancellation"),
            vopr.id.stable(name, "production-data-plane-global-query-inflight-authorization-revocation"),
            vopr.id.stable(name, "production-data-plane-global-query-transport-failure"),
        };
    };
    const mode_names = [_][]const u8{
        name ++ ".clean",
        name ++ ".metadata-partition",
        name ++ ".node-restart",
        name ++ ".graph-inflight-restart",
        name ++ ".graph-topology-churn",
        name ++ ".graph-transport-failure",
        name ++ ".partial-http-write",
        name ++ ".serverless-stale-generation",
        name ++ ".resource-pressure",
        name ++ ".production-data-plane-baseline",
        name ++ ".production-data-plane-graph",
        name ++ ".production-data-plane",
        name ++ ".production-data-plane-graph-split",
        name ++ ".production-data-plane-graph-split-transport-failure",
        name ++ ".production-data-plane-graph-split-owner-restart",
        name ++ ".production-data-plane-graph-split-partial-write",
        name ++ ".production-data-plane-graph-split-resource-pressure",
        name ++ ".production-data-plane-join-split",
        name ++ ".production-data-plane-durable-join-takeover",
        name ++ ".production-data-plane-durable-join-cancellation",
        name ++ ".production-data-plane-durable-join-worker-retry",
        name ++ ".production-data-plane-durable-join-owner-restart",
        name ++ ".production-data-plane-durable-join-retry-exhaustion",
        name ++ ".production-data-plane-durable-join-cancellation-overlapping-faults",
        name ++ ".production-data-plane-durable-join-cancellation-owner-restart",
        name ++ ".production-data-plane-graph-split-overlapping-faults",
        name ++ ".production-data-plane-graph-split-socket-pressure",
        name ++ ".production-data-plane-service-rate",
        name ++ ".production-data-plane-graph-hydration",
        name ++ ".production-data-plane-graph-cancellation",
        name ++ ".production-data-plane-graph-cancellation-transport-failure",
        name ++ ".production-data-plane-graph-inflight-authorization-revocation",
        name ++ ".production-data-plane-graph-stale-snapshot-retry-exhaustion",
        name ++ ".production-data-plane-global-query",
        name ++ ".production-data-plane-global-query-cancellation",
        name ++ ".production-data-plane-global-query-inflight-authorization-revocation",
        name ++ ".production-data-plane-global-query-transport-failure",
    };

    const production_baseline_ordinal: usize = @intFromEnum(Mode.production_data_plane_baseline);
    const production_graph_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph);
    const production_split_ordinal: usize = @intFromEnum(Mode.production_data_plane);
    const production_graph_split_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph_split);
    const production_graph_split_transport_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph_split_transport_failure);
    const production_graph_split_owner_restart_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph_split_owner_restart);
    const production_graph_split_partial_write_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph_split_partial_write);
    const production_graph_split_resource_pressure_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph_split_resource_pressure);
    const production_join_split_ordinal: usize = @intFromEnum(Mode.production_data_plane_join_split);
    const production_durable_join_takeover_ordinal: usize = @intFromEnum(Mode.production_data_plane_durable_join_takeover);
    const production_durable_join_cancellation_ordinal: usize = @intFromEnum(Mode.production_data_plane_durable_join_cancellation);
    const production_durable_join_worker_retry_ordinal: usize = @intFromEnum(Mode.production_data_plane_durable_join_worker_retry);
    const production_durable_join_owner_restart_ordinal: usize = @intFromEnum(Mode.production_data_plane_durable_join_owner_restart);
    const production_durable_join_retry_exhaustion_ordinal: usize = @intFromEnum(Mode.production_data_plane_durable_join_retry_exhaustion);
    const production_durable_join_cancellation_overlap_ordinal: usize = @intFromEnum(Mode.production_data_plane_durable_join_cancellation_overlapping_faults);
    const production_durable_join_cancellation_owner_restart_ordinal: usize = @intFromEnum(Mode.production_data_plane_durable_join_cancellation_owner_restart);
    const production_graph_split_overlapping_faults_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph_split_overlapping_faults);
    const production_graph_split_socket_pressure_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph_split_socket_pressure);
    const production_service_rate_ordinal: usize = @intFromEnum(Mode.production_data_plane_service_rate);
    const production_graph_hydration_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph_hydration);
    const production_graph_cancellation_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph_cancellation);
    const production_graph_cancellation_transport_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph_cancellation_transport_failure);
    const production_graph_authorization_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph_inflight_authorization_revocation);
    const production_graph_stale_snapshot_ordinal: usize = @intFromEnum(Mode.production_data_plane_graph_stale_snapshot_retry_exhaustion);
    const production_global_query_ordinal: usize = @intFromEnum(Mode.production_data_plane_global_query);
    const production_global_query_cancellation_ordinal: usize = @intFromEnum(Mode.production_data_plane_global_query_cancellation);
    const production_global_query_authorization_ordinal: usize = @intFromEnum(Mode.production_data_plane_global_query_inflight_authorization_revocation);
    const production_global_query_transport_ordinal: usize = @intFromEnum(Mode.production_data_plane_global_query_transport_failure);

    const metadata_role = vopr.id.stable(name, "role.metadata");
    const public_data_role = vopr.id.stable(name, "role.public-data");
    const serverless_role = vopr.id.stable(name, "role.serverless");
    const deployment_node_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "node.1"),
        vopr.id.stable(name, "node.2"),
        vopr.id.stable(name, "node.3"),
        vopr.id.stable(name, "node.serverless"),
    };
    const process_domains = [_]vopr.id.StableId{
        vopr.id.stable(name, "process.1"),
        vopr.id.stable(name, "process.2"),
        vopr.id.stable(name, "process.3"),
        vopr.id.stable(name, "process.serverless"),
    };
    const storage_domains = [_]vopr.id.StableId{
        vopr.id.stable(name, "storage.1"),
        vopr.id.stable(name, "storage.2"),
        vopr.id.stable(name, "storage.3"),
        vopr.id.stable(name, "storage.serverless"),
    };
    const resource_domains = [_]vopr.id.StableId{
        vopr.id.stable(name, "resource.1"),
        vopr.id.stable(name, "resource.2"),
        vopr.id.stable(name, "resource.3"),
        vopr.id.stable(name, "resource.serverless"),
    };
    const deployment_roles = [_]vopr.deployment.Role{
        .{ .id = metadata_role, .name = "metadata" },
        .{ .id = public_data_role, .name = "public-data", .depends_on = &.{metadata_role} },
        .{ .id = serverless_role, .name = "serverless", .depends_on = &.{metadata_role} },
    };
    const deployment_nodes = [_]vopr.deployment.Node{
        .{ .id = deployment_node_ids[0], .name = "node-1", .process_domain = process_domains[0], .storage_domain = storage_domains[0], .resource_domain = resource_domains[0], .resources = .{ .memory_limit_bytes = 512 * 1024 * 1024, .disk_limit_bytes = 64 * 1024 * 1024 } },
        .{ .id = deployment_node_ids[1], .name = "node-2", .process_domain = process_domains[1], .storage_domain = storage_domains[1], .resource_domain = resource_domains[1], .resources = .{ .memory_limit_bytes = 512 * 1024 * 1024, .disk_limit_bytes = 64 * 1024 * 1024 } },
        .{ .id = deployment_node_ids[2], .name = "node-3", .process_domain = process_domains[2], .storage_domain = storage_domains[2], .resource_domain = resource_domains[2], .resources = .{ .memory_limit_bytes = 512 * 1024 * 1024, .disk_limit_bytes = 64 * 1024 * 1024 } },
        .{ .id = deployment_node_ids[3], .name = "serverless-worker", .process_domain = process_domains[3], .storage_domain = storage_domains[3], .resource_domain = resource_domains[3], .resources = .{ .memory_limit_bytes = 512 * 1024 * 1024, .disk_limit_bytes = 64 * 1024 * 1024 } },
    };
    const deployment_instances = [_]vopr.deployment.Instance{
        .{ .id = vopr.id.stable(name, "instance.metadata.1"), .node_id = deployment_node_ids[0], .role_id = metadata_role },
        .{ .id = vopr.id.stable(name, "instance.metadata.2"), .node_id = deployment_node_ids[1], .role_id = metadata_role },
        .{ .id = vopr.id.stable(name, "instance.metadata.3"), .node_id = deployment_node_ids[2], .role_id = metadata_role },
        .{ .id = vopr.id.stable(name, "instance.public-data.1"), .node_id = deployment_node_ids[0], .role_id = public_data_role },
        .{ .id = vopr.id.stable(name, "instance.public-data.2"), .node_id = deployment_node_ids[1], .role_id = public_data_role },
        .{ .id = vopr.id.stable(name, "instance.public-data.3"), .node_id = deployment_node_ids[2], .role_id = public_data_role },
        .{ .id = vopr.id.stable(name, "instance.serverless"), .node_id = deployment_node_ids[3], .role_id = serverless_role },
    };
    const deployment_links = [_]vopr.deployment.Link{
        .{ .id = vopr.id.stable(name, "link.1-2"), .name = "1-to-2", .from_node = deployment_node_ids[0], .to_node = deployment_node_ids[1] },
        .{ .id = vopr.id.stable(name, "link.2-1"), .name = "2-to-1", .from_node = deployment_node_ids[1], .to_node = deployment_node_ids[0] },
        .{ .id = vopr.id.stable(name, "link.1-3"), .name = "1-to-3", .from_node = deployment_node_ids[0], .to_node = deployment_node_ids[2] },
        .{ .id = vopr.id.stable(name, "link.3-1"), .name = "3-to-1", .from_node = deployment_node_ids[2], .to_node = deployment_node_ids[0] },
        .{ .id = vopr.id.stable(name, "link.2-3"), .name = "2-to-3", .from_node = deployment_node_ids[1], .to_node = deployment_node_ids[2] },
        .{ .id = vopr.id.stable(name, "link.3-2"), .name = "3-to-2", .from_node = deployment_node_ids[2], .to_node = deployment_node_ids[1] },
    };
    const deployment_manifest: vopr.deployment.Manifest = .{
        .roles = &deployment_roles,
        .nodes = &deployment_nodes,
        .instances = &deployment_instances,
        .links = &deployment_links,
    };

    const ClusterHealth = struct {
        hosts: usize = 0,
        requests_ok: bool = false,
        topology_ok: bool = false,
        global_query_ok: bool = false,
        global_query_status: u16 = 0,
        global_query_response_count: usize = 0,
        global_query_result_assembled_count: u64 = 0,
        global_query_cancellation_boundary_observed: bool = false,
        global_query_cancellation_requested: bool = false,
        global_query_cancellation_observed: bool = false,
        global_query_cancellation_no_partial: bool = false,
        global_query_cancellation_recovered: bool = false,
        global_query_cancellation_ok: bool = false,
        global_query_authorization_boundary_observed: bool = false,
        global_query_authorization_revoked: bool = false,
        global_query_authorization_denied_without_leak: bool = false,
        global_query_authorization_restored: bool = false,
        global_query_authorization_recovered: bool = false,
        global_query_authorization_denied_status: u64 = 0,
        global_query_authorization_recovered_status: u64 = 0,
        global_query_authorization_ok: bool = false,
        global_query_transport_boundary_observed: bool = false,
        global_query_transport_fault_injected: bool = false,
        global_query_transport_fault_observed: bool = false,
        global_query_transport_fault_matches: u64 = 0,
        global_query_transport_fault_healed: bool = false,
        global_query_transport_rejected_without_partial: bool = false,
        global_query_transport_recovered: bool = false,
        global_query_transport_rejected_status: u64 = 0,
        global_query_transport_recovered_status: u64 = 0,
        global_query_transport_ok: bool = false,
        cleanup_ok: bool = false,
        raft_wire_requests: u64 = 0,
        node_resource_managers: usize = 0,
        resource_denial_ok: bool = false,
        resource_recovery_ok: bool = false,
        resource_pressure_observed: bool = false,
        resource_denial_error_code: u64 = 0,
        resource_denial_status: u16 = 0,
        resource_preproposal_denial: bool = false,
        resource_outcome_unknown: bool = false,
        resource_read_before_retry: bool = false,
        resource_retry_attempted: bool = false,
        resource_proposals_before: u64 = 0,
        resource_proposals_after: u64 = 0,
        resource_absent_before_retry: bool = false,
        resource_post_split_ok: bool = false,
        join_query_ok: bool = false,
        split_join_query_ok: bool = false,
        post_split_join_query_ok: bool = false,
        join_finalizer_ack_failure_injected: bool = false,
        join_finalizer_persisted_group_id: u64 = 0,
        durable_join_takeover_ok: bool = false,
        join_partition_worker_started_count: u64 = 0,
        join_partition_worker_completed_count: u64 = 0,
        join_worker_retry_failure_injected: bool = false,
        join_worker_retry_job_id: u64 = 0,
        join_worker_retry_partition_index: usize = 0,
        join_worker_retry_failed_group_id: u64 = 0,
        join_worker_retry_recovered_group_id: u64 = 0,
        join_worker_retry_ok: bool = false,
        join_owner_restart_job_id: u64 = 0,
        join_owner_restart_partition_index: usize = 0,
        join_owner_restart_failed_group_id: u64 = 0,
        join_owner_restart_recovered_group_id: u64 = 0,
        join_owner_restart_target_index: usize = 0,
        join_owner_restart_recovery_index: usize = 0,
        join_owner_restart_coordinator_index: usize = 0,
        join_owner_restart_requested: bool = false,
        join_owner_restart_down: bool = false,
        join_owner_restart_recovered: bool = false,
        join_owner_restart_initial_status: u16 = 0,
        join_owner_restart_initial_rejected_without_partial: bool = false,
        join_owner_restart_recovery_join: bool = false,
        join_owner_restart_post_reconstruction_read: bool = false,
        join_owner_restart_ok: bool = false,
        join_retry_exhaustion_job_id: u64 = 0,
        join_retry_exhaustion_partition_index: usize = 0,
        join_retry_exhaustion_first_group_id: u64 = 0,
        join_retry_exhaustion_retry_group_id: u64 = 0,
        join_retry_exhaustion_coordinator_index: usize = 0,
        join_retry_exhaustion_retry_target_index: usize = 0,
        join_retry_exhaustion_faults_injected: bool = false,
        join_retry_exhaustion_resource_observed: bool = false,
        join_retry_exhaustion_network_observed: bool = false,
        join_retry_exhaustion_overlap_observed: bool = false,
        join_retry_exhaustion_initial_worker_starts: u64 = 0,
        join_retry_exhaustion_initial_worker_completions: u64 = 0,
        join_retry_exhaustion_initial_status: u16 = 0,
        join_retry_exhaustion_initial_rejected_without_partial: bool = false,
        join_retry_exhaustion_network_healed: bool = false,
        join_retry_exhaustion_resource_healed: bool = false,
        join_retry_exhaustion_recovery_join: bool = false,
        join_retry_exhaustion_ok: bool = false,
        join_cancellation_boundary_observed: bool = false,
        join_cancellation_job_id: u64 = 0,
        join_cancellation_owner_group_id: u64 = 0,
        join_cancellation_requested: bool = false,
        join_cancellation_observed: bool = false,
        join_cancellation_recovered: bool = false,
        join_cancellation_ok: bool = false,
        join_cancellation_overlap_first_group_id: u64 = 0,
        join_cancellation_overlap_worker_group_id: u64 = 0,
        join_cancellation_overlap_coordinator_index: usize = 0,
        join_cancellation_overlap_network_target_index: usize = 0,
        join_cancellation_overlap_faults_injected: bool = false,
        join_cancellation_overlap_network_observed: bool = false,
        join_cancellation_overlap_resource_observed: bool = false,
        join_cancellation_overlap_observed: bool = false,
        join_cancellation_overlap_network_healed: bool = false,
        join_cancellation_overlap_resource_healed: bool = false,
        graph_query_ok: bool = false,
        graph_hydration_ok: bool = false,
        graph_hydration_started_count: u64 = 0,
        graph_hydration_fanout_started_count: u64 = 0,
        graph_hydration_completed_count: u64 = 0,
        graph_cancellation_requested: bool = false,
        graph_cancellation_observed: bool = false,
        graph_cancellation_recovered: bool = false,
        graph_cancellation_ok: bool = false,
        graph_cancellation_fault_injected: bool = false,
        graph_cancellation_fault_observed: bool = false,
        graph_cancellation_fault_matches: u64 = 0,
        graph_cancellation_fault_healed: bool = false,
        graph_authorization_boundary_observed: bool = false,
        graph_authorization_revoked: bool = false,
        graph_authorization_denied_without_leak: bool = false,
        graph_authorization_restored: bool = false,
        graph_authorization_recovered: bool = false,
        graph_authorization_denied_status: u64 = 0,
        graph_authorization_recovered_status: u64 = 0,
        graph_authorization_ok: bool = false,
        graph_stale_snapshot_boundary_observed: bool = false,
        graph_stale_snapshot_attempt_failures: u64 = 0,
        graph_stale_snapshot_error_code: u64 = 0,
        graph_stale_snapshot_rejected_without_partial: bool = false,
        graph_stale_snapshot_status: u64 = 0,
        graph_stale_snapshot_recovered: bool = false,
        graph_stale_snapshot_ok: bool = false,
        split_graph_inflight_started: bool = false,
        split_graph_inflight_complete: bool = false,
        split_graph_inflight_rejected: bool = false,
        split_graph_inflight_ok: bool = false,
        post_split_graph_query_ok: bool = false,
        graph_inflight_restart_observed: bool = false,
        graph_inflight_restart_recovered: bool = false,
        graph_topology_churn_observed: bool = false,
        graph_topology_churn_finalized: bool = false,
        graph_topology_churn_error_code: u64 = 0,
        graph_topology_partial_rejected_sound: bool = false,
        graph_transport_failure_injected: bool = false,
        graph_transport_failure_observed: bool = false,
        graph_transport_failure_error_code: u64 = 0,
        overlapping_faults_active_observed: bool = false,
        graph_owner_restart_requested: bool = false,
        graph_owner_restart_down: bool = false,
        graph_owner_restart_failure_observed: bool = false,
        graph_owner_restart_recovered: bool = false,
        graph_owner_restart_error_code: u64 = 0,
        graph_partial_write_injected: bool = false,
        graph_partial_write_observed: bool = false,
        socket_pressure_injected: bool = false,
        socket_pressure_denial_observed: bool = false,
        socket_pressure_error_code: u64 = 0,
        socket_pressure_no_ingress: bool = false,
        socket_pressure_recovered: bool = false,
        graph_partial_rejected_sound: bool = false,
    };

    const raft_operation = vopr.service_rate.Operation.named(name ++ ".raft-round", 10);
    const lsm_operation = vopr.service_rate.Operation.named(name ++ ".lsm-maintenance", 20);
    const graph_expand_operation = vopr.service_rate.Operation.named(name ++ ".graph-expand", 30);
    const graph_hydrate_operation = vopr.service_rate.Operation.named(name ++ ".graph-hydrate", 40);
    const graph_edges_operation = vopr.service_rate.Operation.named(name ++ ".graph-edges", 50);
    const publish_operation = vopr.service_rate.Operation.named(name ++ ".serverless-publish", 60);
    const enrichment_operation = vopr.service_rate.Operation.named(name ++ ".serverless-enrichment", 70);
    const compaction_operation = vopr.service_rate.Operation.named(name ++ ".serverless-compaction", 80);
    const prune_operation = vopr.service_rate.Operation.named(name ++ ".serverless-prune", 90);
    const service_nodes = [_]vopr.service_rate.Node{
        .{ .id = deployment_node_ids[0], .name = name ++ ".node.1" },
        .{ .id = deployment_node_ids[1], .name = name ++ ".node.2" },
        .{ .id = deployment_node_ids[2], .name = name ++ ".node.3" },
        .{ .id = deployment_node_ids[3], .name = name ++ ".node.serverless" },
    };
    const service_operations = [_]vopr.service_rate.Operation{
        raft_operation,
        lsm_operation,
        graph_expand_operation,
        graph_hydrate_operation,
        graph_edges_operation,
        publish_operation,
        enrichment_operation,
        compaction_operation,
        prune_operation,
    };

    const CostAccounting = struct {
        pre_heal_units: u64 = 0,
        post_heal_units: u64 = 0,
        sound: bool = true,

        fn record(self: *@This(), healed: bool, base_cost_ns: u64, units: u64, charged_ns: u64) void {
            var expected = std.math.mul(u64, base_cost_ns, units) catch {
                self.sound = false;
                return;
            };
            if (!healed) expected = std.math.mul(u64, expected, 2) catch {
                self.sound = false;
                return;
            };
            self.sound = self.sound and charged_ns == expected;
            if (healed)
                self.post_heal_units +|= units
            else
                self.pre_heal_units +|= units;
        }
    };

    const DataServiceRateAdapter = struct {
        port: vopr.service_rate.Port,
        healed: *const bool,
        accounting: CostAccounting = .{},

        fn iface(self: *@This()) data_runtime.DataServerWorkCostPort {
            return .{ .ptr = self, .charge_fn = charge };
        }

        fn charge(ptr: *anyopaque, kind: data_runtime.DataServerWorkKind, units: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const was_healed = self.healed.*;
            const operation = switch (kind) {
                .raft_round => raft_operation,
                .lsm_maintenance_step => lsm_operation,
            };
            const charged = try self.port.charge(operation.id, units);
            self.accounting.record(was_healed, operation.base_cost_ns, units, charged);
        }
    };

    const GraphServiceRateAdapter = struct {
        port: vopr.service_rate.Port,
        healed: *const bool,
        accounting: CostAccounting = .{},

        fn iface(self: *@This()) distributed_graph.WorkCostPort {
            return .{ .ptr = self, .charge_fn = charge };
        }

        fn charge(ptr: *anyopaque, _: u64, kind: distributed_graph.WorkKind, units: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const was_healed = self.healed.*;
            const operation = switch (kind) {
                .expand => graph_expand_operation,
                .hydrate => graph_hydrate_operation,
                .get_edges => graph_edges_operation,
            };
            const charged = try self.port.charge(operation.id, units);
            self.accounting.record(was_healed, operation.base_cost_ns, units, charged);
        }
    };

    const ServerlessServiceRateAdapter = struct {
        port: vopr.service_rate.Port,
        healed: *const bool,
        accounting: CostAccounting = .{},

        fn iface(self: *@This()) serverless_runtime.RuntimeWorkCostPort {
            return .{ .ptr = self, .charge_fn = charge };
        }

        fn charge(ptr: *anyopaque, kind: serverless_runtime.RuntimeWorkKind, units: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const was_healed = self.healed.*;
            const operation = switch (kind) {
                .publish_round => publish_operation,
                .enrichment_round => enrichment_operation,
                .compaction_round => compaction_operation,
                .prune_round => prune_operation,
            };
            const charged = try self.port.charge(operation.id, units);
            self.accounting.record(was_healed, operation.base_cost_ns, units, charged);
        }
    };

    const State = struct {
        owner_alloc: std.mem.Allocator,
        fixture_allocator: FixtureAllocator,
        sim: vopr.vopr_io.VoprIo,
        service_rate_model: vopr.service_rate.Model,
        data_service_rate_adapters: [3]DataServiceRateAdapter,
        graph_service_rate_adapters: [3]GraphServiceRateAdapter,
        serverless_service_rate_adapter: ServerlessServiceRateAdapter,
        service_rates_enabled: bool = false,
        service_rates_healed: bool = false,
        public_cluster: ?*metadata_sim.VoprPublicClusterFixture = null,
        production_cluster: ?*production_cluster.Fixture = null,
        deployment: ?vopr.deployment.Composer = null,
        serverless: *serverless_workflow.Scenario.Fixture,
        initialization_future: ?std.Io.Future(void) = null,
        serverless_future: ?std.Io.Future(void) = null,
        completion_future: ?std.Io.Future(void) = null,
        mode: ?Mode = null,
        initialization_done: bool = false,
        initialization_failed: bool = false,
        initialization_error_code: u64 = 0,
        serverless_done: bool = false,
        serverless_sound: bool = false,
        serverless_public_sound: bool = false,
        serverless_public_error_code: u64 = 0,
        shared_io_sound: bool = false,
        deployment_sound: bool = false,
        complete: bool = false,
        tearing_down: bool = false,

        fn init(alloc: std.mem.Allocator) !*State {
            const self = try alloc.create(State);
            errdefer alloc.destroy(self);
            self.owner_alloc = alloc;
            self.fixture_allocator = .init;
            errdefer _ = self.fixture_allocator.deinit();
            const fixture_alloc = self.fixture_allocator.allocator();
            self.sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0x4655_4c4c,
                // The production HTTP -> transaction -> DB/index-open path
                // has a deeper synchronous frame chain than focused suites.
                // Match a conventional native main-thread stack so VOPR does
                // not turn ordinary production stack use into a fiber fault.
                .tasks = .{ .stack_size = 8 * 1024 * 1024 },
                .network = .{ .max_sockets = 96, .stream_capacity = 256 * 1024 },
                .files = .{ .capacity_bytes = 64 * 1024 * 1024 },
                .instrumentation = .{ .enabled = false, .map_digest = 0x4655_4c4c },
            });
            errdefer self.sim.deinit();
            self.service_rates_enabled = false;
            self.service_rates_healed = false;
            self.service_rate_model = try vopr.service_rate.Model.init(
                fixture_alloc,
                &service_nodes,
                &service_operations,
            );
            errdefer self.service_rate_model.deinit();
            for (&self.data_service_rate_adapters, &self.graph_service_rate_adapters, service_nodes[0..3]) |*data_adapter, *graph_adapter, node| {
                data_adapter.* = .{
                    .port = try self.service_rate_model.port(self.sim.io(), node.id),
                    .healed = &self.service_rates_healed,
                };
                graph_adapter.* = .{
                    .port = try self.service_rate_model.port(self.sim.io(), node.id),
                    .healed = &self.service_rates_healed,
                };
            }
            self.serverless_service_rate_adapter = .{
                .port = try self.service_rate_model.port(self.sim.io(), service_nodes[3].id),
                .healed = &self.service_rates_healed,
            };
            self.serverless = try serverless_workflow.Scenario.Fixture.initWithVoprIo(fixture_alloc, &self.sim);
            errdefer self.serverless.deinit();
            self.mode = null;
            self.public_cluster = null;
            self.production_cluster = null;
            self.deployment = null;
            self.serverless_future = null;
            self.completion_future = null;
            self.initialization_done = false;
            self.initialization_failed = false;
            self.initialization_error_code = 0;
            self.serverless_done = false;
            self.serverless_sound = false;
            self.serverless_public_sound = false;
            self.serverless_public_error_code = 0;
            self.shared_io_sound = false;
            self.deployment_sound = false;
            self.complete = false;
            self.tearing_down = false;
            self.initialization_future = self.sim.io().async(initializeAndRun, .{self});
            return self;
        }

        fn deinit(self: *State) void {
            self.tearing_down = true;
            if (self.production_cluster) |fixture| fixture.beginTeardown();
            if (self.public_cluster) |fixture| fixture.beginTeardown();
            _ = self.sim.cancelAndDrainTasksForTeardown(
                self.fixture_allocator.allocator(),
                100_000,
            ) catch |err| std.debug.panic(
                "full-cluster VOPR teardown could not drain canceled tasks: {s}",
                .{@errorName(err)},
            );
            if (self.completion_future) |*future| {
                future.cancel(self.sim.io());
                self.completion_future = null;
            }
            if (self.serverless_future) |*future| {
                future.cancel(self.sim.io());
                self.serverless_future = null;
            }
            if (self.initialization_future) |*future| {
                future.cancel(self.sim.io());
                self.initialization_future = null;
            }
            if (self.deployment) |*deployment| deployment.deinit();
            self.serverless.deinit();
            if (self.public_cluster) |fixture| fixture.deinit();
            if (self.production_cluster) |fixture| fixture.deinit();
            self.service_rate_model.deinit();
            self.sim.deinit();
            std.debug.assert(self.fixture_allocator.deinit() == .ok);
            self.owner_alloc.destroy(self);
        }

        fn start(self: *State, mode: Mode) !void {
            self.mode = mode;
        }

        fn enableServiceRates(self: *State) !void {
            self.service_rates_enabled = true;
            self.service_rates_healed = false;
            for (service_nodes, 0..) |node, index| try self.service_rate_model.activate(.{
                .fault_id = vopr.id.derive(name ++ ".service-rate-slowdown", node.id, index),
                .node_id = node.id,
                .multiplier_ppm = 2 * vopr.service_rate.parts_per_million,
            });
            var data_ports: [3]data_runtime.DataServerWorkCostPort = undefined;
            var graph_ports: [3]distributed_graph.WorkCostPort = undefined;
            for (&data_ports, &graph_ports, &self.data_service_rate_adapters, &self.graph_service_rate_adapters) |*data_port, *graph_port, *data_adapter, *graph_adapter| {
                data_port.* = data_adapter.iface();
                graph_port.* = graph_adapter.iface();
            }
            self.production_cluster.?.setWorkCostPorts(.{
                .data = data_ports,
                .graph = graph_ports,
            });
            self.serverless.setWorkCostPort(self.serverless_service_rate_adapter.iface());
        }

        fn healServiceRates(self: *State) void {
            if (!self.service_rates_enabled or self.service_rates_healed) return;
            self.service_rate_model.healAll();
            self.service_rates_healed = true;
        }

        fn serviceRatesSound(self: *const State) bool {
            if (!self.service_rates_enabled) return true;
            var data_pre: u64 = 0;
            var data_post: u64 = 0;
            var graph_post: u64 = 0;
            var accounting_sound = self.serverless_service_rate_adapter.accounting.sound;
            for (self.data_service_rate_adapters) |adapter| {
                data_pre +|= adapter.accounting.pre_heal_units;
                data_post +|= adapter.accounting.post_heal_units;
                accounting_sound = accounting_sound and adapter.accounting.sound;
            }
            for (self.graph_service_rate_adapters) |adapter| {
                graph_post +|= adapter.accounting.post_heal_units;
                accounting_sound = accounting_sound and adapter.accounting.sound;
            }
            return self.service_rates_healed and
                self.service_rate_model.activeEffectCount() == 0 and
                data_pre > 0 and data_post > 0 and graph_post > 0 and
                self.serverless_service_rate_adapter.accounting.pre_heal_units > 0 and
                accounting_sound;
        }

        fn clusterHealth(self: *State) ?ClusterHealth {
            if (self.production_cluster) |fixture| {
                const snapshot = fixture.healthSnapshot();
                return .{
                    .hosts = snapshot.hosts,
                    .requests_ok = snapshot.requests_ok,
                    .topology_ok = snapshot.topology_ok,
                    .global_query_ok = snapshot.global_query_ok,
                    .global_query_status = snapshot.global_query_status,
                    .global_query_response_count = snapshot.global_query_response_count,
                    .global_query_result_assembled_count = snapshot.global_query_result_assembled_count,
                    .global_query_cancellation_boundary_observed = snapshot.global_query_cancellation_boundary_observed,
                    .global_query_cancellation_requested = snapshot.global_query_cancellation_requested,
                    .global_query_cancellation_observed = snapshot.global_query_cancellation_observed,
                    .global_query_cancellation_no_partial = snapshot.global_query_cancellation_no_partial,
                    .global_query_cancellation_recovered = snapshot.global_query_cancellation_recovered,
                    .global_query_cancellation_ok = snapshot.global_query_cancellation_ok,
                    .global_query_authorization_boundary_observed = snapshot.global_query_authorization_boundary_observed,
                    .global_query_authorization_revoked = snapshot.global_query_authorization_revoked,
                    .global_query_authorization_denied_without_leak = snapshot.global_query_authorization_denied_without_leak,
                    .global_query_authorization_restored = snapshot.global_query_authorization_restored,
                    .global_query_authorization_recovered = snapshot.global_query_authorization_recovered,
                    .global_query_authorization_denied_status = snapshot.global_query_authorization_denied_status,
                    .global_query_authorization_recovered_status = snapshot.global_query_authorization_recovered_status,
                    .global_query_authorization_ok = snapshot.global_query_authorization_ok,
                    .global_query_transport_boundary_observed = snapshot.global_query_transport_boundary_observed,
                    .global_query_transport_fault_injected = snapshot.global_query_transport_fault_injected,
                    .global_query_transport_fault_observed = snapshot.global_query_transport_fault_observed,
                    .global_query_transport_fault_matches = snapshot.global_query_transport_fault_matches,
                    .global_query_transport_fault_healed = snapshot.global_query_transport_fault_healed,
                    .global_query_transport_rejected_without_partial = snapshot.global_query_transport_rejected_without_partial,
                    .global_query_transport_recovered = snapshot.global_query_transport_recovered,
                    .global_query_transport_rejected_status = snapshot.global_query_transport_rejected_status,
                    .global_query_transport_recovered_status = snapshot.global_query_transport_recovered_status,
                    .global_query_transport_ok = snapshot.global_query_transport_ok,
                    .join_query_ok = snapshot.join_query_ok,
                    .split_join_query_ok = snapshot.split_join_query_ok,
                    .post_split_join_query_ok = snapshot.post_split_join_query_ok,
                    .join_finalizer_ack_failure_injected = snapshot.join_finalizer_ack_failure_injected,
                    .join_finalizer_persisted_group_id = snapshot.join_finalizer_persisted_group_id,
                    .durable_join_takeover_ok = snapshot.durable_join_takeover_ok,
                    .join_partition_worker_started_count = snapshot.join_partition_worker_started_count,
                    .join_partition_worker_completed_count = snapshot.join_partition_worker_completed_count,
                    .join_worker_retry_failure_injected = snapshot.join_worker_retry_failure_injected,
                    .join_worker_retry_job_id = snapshot.join_worker_retry_job_id,
                    .join_worker_retry_partition_index = snapshot.join_worker_retry_partition_index,
                    .join_worker_retry_failed_group_id = snapshot.join_worker_retry_failed_group_id,
                    .join_worker_retry_recovered_group_id = snapshot.join_worker_retry_recovered_group_id,
                    .join_worker_retry_ok = snapshot.join_worker_retry_ok,
                    .join_owner_restart_job_id = snapshot.join_owner_restart_job_id,
                    .join_owner_restart_partition_index = snapshot.join_owner_restart_partition_index,
                    .join_owner_restart_failed_group_id = snapshot.join_owner_restart_failed_group_id,
                    .join_owner_restart_recovered_group_id = snapshot.join_owner_restart_recovered_group_id,
                    .join_owner_restart_target_index = snapshot.join_owner_restart_target_index,
                    .join_owner_restart_recovery_index = snapshot.join_owner_restart_recovery_index,
                    .join_owner_restart_coordinator_index = snapshot.join_owner_restart_coordinator_index,
                    .join_owner_restart_requested = snapshot.join_owner_restart_requested,
                    .join_owner_restart_down = snapshot.join_owner_restart_down,
                    .join_owner_restart_recovered = snapshot.join_owner_restart_recovered,
                    .join_owner_restart_initial_status = snapshot.join_owner_restart_initial_status,
                    .join_owner_restart_initial_rejected_without_partial = snapshot.join_owner_restart_initial_rejected_without_partial,
                    .join_owner_restart_recovery_join = snapshot.join_owner_restart_recovery_join,
                    .join_owner_restart_post_reconstruction_read = snapshot.join_owner_restart_post_reconstruction_read,
                    .join_owner_restart_ok = snapshot.join_owner_restart_ok,
                    .join_retry_exhaustion_job_id = snapshot.join_retry_exhaustion_job_id,
                    .join_retry_exhaustion_partition_index = snapshot.join_retry_exhaustion_partition_index,
                    .join_retry_exhaustion_first_group_id = snapshot.join_retry_exhaustion_first_group_id,
                    .join_retry_exhaustion_retry_group_id = snapshot.join_retry_exhaustion_retry_group_id,
                    .join_retry_exhaustion_coordinator_index = snapshot.join_retry_exhaustion_coordinator_index,
                    .join_retry_exhaustion_retry_target_index = snapshot.join_retry_exhaustion_retry_target_index,
                    .join_retry_exhaustion_faults_injected = snapshot.join_retry_exhaustion_faults_injected,
                    .join_retry_exhaustion_resource_observed = snapshot.join_retry_exhaustion_resource_observed,
                    .join_retry_exhaustion_network_observed = snapshot.join_retry_exhaustion_network_observed,
                    .join_retry_exhaustion_overlap_observed = snapshot.join_retry_exhaustion_overlap_observed,
                    .join_retry_exhaustion_initial_worker_starts = snapshot.join_retry_exhaustion_initial_worker_starts,
                    .join_retry_exhaustion_initial_worker_completions = snapshot.join_retry_exhaustion_initial_worker_completions,
                    .join_retry_exhaustion_initial_status = snapshot.join_retry_exhaustion_initial_status,
                    .join_retry_exhaustion_initial_rejected_without_partial = snapshot.join_retry_exhaustion_initial_rejected_without_partial,
                    .join_retry_exhaustion_network_healed = snapshot.join_retry_exhaustion_network_healed,
                    .join_retry_exhaustion_resource_healed = snapshot.join_retry_exhaustion_resource_healed,
                    .join_retry_exhaustion_recovery_join = snapshot.join_retry_exhaustion_recovery_join,
                    .join_retry_exhaustion_ok = snapshot.join_retry_exhaustion_ok,
                    .join_cancellation_boundary_observed = snapshot.join_cancellation_boundary_observed,
                    .join_cancellation_job_id = snapshot.join_cancellation_job_id,
                    .join_cancellation_owner_group_id = snapshot.join_cancellation_owner_group_id,
                    .join_cancellation_requested = snapshot.join_cancellation_requested,
                    .join_cancellation_observed = snapshot.join_cancellation_observed,
                    .join_cancellation_recovered = snapshot.join_cancellation_recovered,
                    .join_cancellation_ok = snapshot.join_cancellation_ok,
                    .join_cancellation_overlap_first_group_id = snapshot.join_cancellation_overlap_first_group_id,
                    .join_cancellation_overlap_worker_group_id = snapshot.join_cancellation_overlap_worker_group_id,
                    .join_cancellation_overlap_coordinator_index = snapshot.join_cancellation_overlap_coordinator_index,
                    .join_cancellation_overlap_network_target_index = snapshot.join_cancellation_overlap_network_target_index,
                    .join_cancellation_overlap_faults_injected = snapshot.join_cancellation_overlap_faults_injected,
                    .join_cancellation_overlap_network_observed = snapshot.join_cancellation_overlap_network_observed,
                    .join_cancellation_overlap_resource_observed = snapshot.join_cancellation_overlap_resource_observed,
                    .join_cancellation_overlap_observed = snapshot.join_cancellation_overlap_observed,
                    .join_cancellation_overlap_network_healed = snapshot.join_cancellation_overlap_network_healed,
                    .join_cancellation_overlap_resource_healed = snapshot.join_cancellation_overlap_resource_healed,
                    .graph_query_ok = snapshot.graph_query_ok,
                    .graph_hydration_ok = snapshot.graph_hydration_ok,
                    .graph_hydration_started_count = snapshot.graph_hydration_started_count,
                    .graph_hydration_fanout_started_count = snapshot.graph_hydration_fanout_started_count,
                    .graph_hydration_completed_count = snapshot.graph_hydration_completed_count,
                    .graph_cancellation_requested = snapshot.graph_cancellation_requested,
                    .graph_cancellation_observed = snapshot.graph_cancellation_observed,
                    .graph_cancellation_recovered = snapshot.graph_cancellation_recovered,
                    .graph_cancellation_ok = snapshot.graph_cancellation_ok,
                    .graph_cancellation_fault_injected = snapshot.graph_cancellation_fault_injected,
                    .graph_cancellation_fault_observed = snapshot.graph_cancellation_fault_observed,
                    .graph_cancellation_fault_matches = snapshot.graph_cancellation_fault_matches,
                    .graph_cancellation_fault_healed = snapshot.graph_cancellation_fault_healed,
                    .graph_authorization_boundary_observed = snapshot.graph_authorization_boundary_observed,
                    .graph_authorization_revoked = snapshot.graph_authorization_revoked,
                    .graph_authorization_denied_without_leak = snapshot.graph_authorization_denied_without_leak,
                    .graph_authorization_restored = snapshot.graph_authorization_restored,
                    .graph_authorization_recovered = snapshot.graph_authorization_recovered,
                    .graph_authorization_denied_status = snapshot.graph_authorization_denied_status,
                    .graph_authorization_recovered_status = snapshot.graph_authorization_recovered_status,
                    .graph_authorization_ok = snapshot.graph_authorization_ok,
                    .graph_stale_snapshot_boundary_observed = snapshot.graph_stale_snapshot_boundary_observed,
                    .graph_stale_snapshot_attempt_failures = snapshot.graph_stale_snapshot_attempt_failures,
                    .graph_stale_snapshot_error_code = snapshot.graph_stale_snapshot_error_code,
                    .graph_stale_snapshot_rejected_without_partial = snapshot.graph_stale_snapshot_rejected_without_partial,
                    .graph_stale_snapshot_status = snapshot.graph_stale_snapshot_status,
                    .graph_stale_snapshot_recovered = snapshot.graph_stale_snapshot_recovered,
                    .graph_stale_snapshot_ok = snapshot.graph_stale_snapshot_ok,
                    .split_graph_inflight_started = snapshot.split_graph_inflight_started,
                    .split_graph_inflight_complete = snapshot.split_graph_inflight_complete,
                    .split_graph_inflight_rejected = snapshot.split_graph_inflight_rejected,
                    .split_graph_inflight_ok = snapshot.split_graph_inflight_ok,
                    .post_split_graph_query_ok = snapshot.post_split_graph_query_ok,
                    .graph_transport_failure_injected = snapshot.graph_transport_failure_injected,
                    .graph_transport_failure_observed = snapshot.graph_transport_failure_observed,
                    .graph_transport_failure_error_code = snapshot.graph_transport_failure_error_code,
                    .overlapping_faults_active_observed = snapshot.overlapping_faults_active_observed,
                    .graph_owner_restart_requested = snapshot.graph_owner_restart_requested,
                    .graph_owner_restart_down = snapshot.graph_owner_restart_down,
                    .graph_owner_restart_failure_observed = snapshot.graph_owner_restart_failure_observed,
                    .graph_owner_restart_recovered = snapshot.graph_owner_restart_recovered,
                    .graph_owner_restart_error_code = snapshot.graph_owner_restart_error_code,
                    .graph_partial_write_injected = snapshot.graph_partial_write_injected,
                    .graph_partial_write_observed = snapshot.graph_partial_write_observed,
                    .socket_pressure_injected = snapshot.socket_pressure_injected,
                    .socket_pressure_denial_observed = snapshot.socket_pressure_denial_observed,
                    .socket_pressure_error_code = snapshot.socket_pressure_error_code,
                    .socket_pressure_no_ingress = snapshot.socket_pressure_no_ingress,
                    .socket_pressure_recovered = snapshot.socket_pressure_recovered,
                    .graph_partial_rejected_sound = snapshot.graph_partial_rejected_sound,
                    .resource_pressure_observed = snapshot.resource_pressure_observed,
                    .resource_denial_ok = snapshot.resource_denial_ok,
                    .resource_denial_status = snapshot.resource_denial_status,
                    .resource_preproposal_denial = snapshot.resource_preproposal_denial,
                    .resource_outcome_unknown = snapshot.resource_outcome_unknown,
                    .resource_read_before_retry = snapshot.resource_read_before_retry,
                    .resource_retry_attempted = snapshot.resource_retry_attempted,
                    .resource_proposals_before = snapshot.resource_proposals_before,
                    .resource_proposals_after = snapshot.resource_proposals_after,
                    .resource_absent_before_retry = snapshot.resource_absent_before_retry,
                    .resource_recovery_ok = snapshot.resource_recovery_ok,
                    .resource_post_split_ok = snapshot.resource_post_split_ok,
                    .cleanup_ok = snapshot.cleanup_ok,
                    .raft_wire_requests = snapshot.raft_wire_requests,
                    .node_resource_managers = snapshot.node_resource_managers,
                };
            }
            if (self.public_cluster) |fixture| {
                const snapshot = fixture.healthSnapshot();
                return .{
                    .hosts = snapshot.hosts,
                    .requests_ok = snapshot.requests_ok,
                    .topology_ok = snapshot.topology_ok,
                    .cleanup_ok = snapshot.cleanup_ok,
                    .raft_wire_requests = snapshot.raft_wire_requests,
                    .node_resource_managers = snapshot.node_resource_managers,
                    .resource_denial_ok = snapshot.resource_denial_ok,
                    .resource_recovery_ok = snapshot.resource_recovery_ok,
                    .resource_pressure_observed = snapshot.resource_pressure_observed,
                    .resource_denial_error_code = snapshot.resource_denial_error_code,
                    .graph_query_ok = snapshot.graph_query_ok,
                    .graph_inflight_restart_observed = snapshot.graph_inflight_restart_observed,
                    .graph_inflight_restart_recovered = snapshot.graph_inflight_restart_recovered,
                    .graph_topology_churn_observed = snapshot.graph_topology_churn_observed,
                    .graph_topology_churn_finalized = snapshot.graph_topology_churn_finalized,
                    .graph_topology_churn_error_code = snapshot.graph_topology_churn_error_code,
                    .graph_topology_partial_rejected_sound = snapshot.graph_topology_partial_rejected_sound,
                    .graph_transport_failure_injected = snapshot.graph_transport_failure_injected,
                    .graph_transport_failure_observed = snapshot.graph_transport_failure_observed,
                    .graph_transport_failure_error_code = snapshot.graph_transport_failure_error_code,
                    .graph_partial_rejected_sound = snapshot.graph_partial_rejected_sound,
                };
            }
            return null;
        }

        fn initializeAndRun(self: *State) void {
            const mode = self.mode orelse {
                self.initialization_failed = true;
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            var public_fixture: ?*metadata_sim.VoprPublicClusterFixture = null;
            if (mode.isProduction()) {
                self.production_cluster = production_cluster.Fixture.create(
                    self.fixture_allocator.allocator(),
                    &self.sim,
                ) catch |err| {
                    std.log.err("production data-plane VOPR fixture creation failed: {s}", .{@errorName(err)});
                    self.initialization_failed = true;
                    self.initialization_error_code = @intFromError(err);
                    self.initialization_done = true;
                    self.complete = true;
                    return;
                };
                if (mode == .production_data_plane_service_rate) self.enableServiceRates() catch |err| {
                    self.initialization_failed = true;
                    self.initialization_error_code = @intFromError(err);
                    self.initialization_done = true;
                    self.complete = true;
                    return;
                };
                self.production_cluster.?.setActiveSplitEnabled(
                    mode == .production_data_plane or
                        mode == .production_data_plane_graph_split or
                        mode == .production_data_plane_graph_split_transport_failure or
                        mode == .production_data_plane_graph_split_owner_restart or
                        mode == .production_data_plane_graph_split_partial_write or
                        mode == .production_data_plane_graph_split_resource_pressure or
                        mode == .production_data_plane_graph_split_overlapping_faults or
                        mode == .production_data_plane_graph_split_socket_pressure or
                        mode == .production_data_plane_join_split or
                        mode == .production_data_plane_graph_stale_snapshot_retry_exhaustion,
                );
                self.production_cluster.?.setGraphEnabled(
                    mode == .production_data_plane_graph or
                        mode == .production_data_plane_service_rate or
                        mode == .production_data_plane_graph_hydration or
                        mode == .production_data_plane_graph_cancellation or
                        mode == .production_data_plane_graph_cancellation_transport_failure or
                        mode == .production_data_plane_graph_inflight_authorization_revocation or
                        mode == .production_data_plane_graph_stale_snapshot_retry_exhaustion or
                        mode == .production_data_plane_graph_split or
                        mode == .production_data_plane_graph_split_transport_failure or
                        mode == .production_data_plane_graph_split_owner_restart or
                        mode == .production_data_plane_graph_split_partial_write or
                        mode == .production_data_plane_graph_split_resource_pressure or
                        mode == .production_data_plane_graph_split_overlapping_faults or
                        mode == .production_data_plane_graph_split_socket_pressure,
                );
                self.production_cluster.?.setGraphHydrationEnabled(
                    mode == .production_data_plane_graph_hydration,
                );
                self.production_cluster.?.setGraphCancellationEnabled(
                    mode == .production_data_plane_graph_cancellation or
                        mode == .production_data_plane_graph_cancellation_transport_failure,
                );
                self.production_cluster.?.setGraphInflightAuthorizationRevocationEnabled(
                    mode == .production_data_plane_graph_inflight_authorization_revocation,
                );
                self.production_cluster.?.setGraphStaleSnapshotRetryExhaustionEnabled(
                    mode == .production_data_plane_graph_stale_snapshot_retry_exhaustion,
                );
                self.production_cluster.?.setJoinEnabled(
                    mode == .production_data_plane_join_split or
                        mode == .production_data_plane_durable_join_takeover or
                        mode == .production_data_plane_durable_join_cancellation or
                        mode == .production_data_plane_durable_join_worker_retry or
                        mode == .production_data_plane_durable_join_owner_restart or
                        mode == .production_data_plane_durable_join_retry_exhaustion or
                        mode == .production_data_plane_durable_join_cancellation_overlapping_faults or
                        mode == .production_data_plane_durable_join_cancellation_owner_restart,
                );
                self.production_cluster.?.setGlobalQueryEnabled(
                    mode == .production_data_plane_global_query or
                        mode == .production_data_plane_global_query_cancellation or
                        mode == .production_data_plane_global_query_inflight_authorization_revocation or
                        mode == .production_data_plane_global_query_transport_failure,
                );
                self.production_cluster.?.setGlobalQueryCancellationEnabled(
                    mode == .production_data_plane_global_query_cancellation,
                );
                self.production_cluster.?.setGlobalQueryAuthorizationRevocationEnabled(
                    mode == .production_data_plane_global_query_inflight_authorization_revocation,
                );
                self.production_cluster.?.setGlobalQueryTransportFailureEnabled(
                    mode == .production_data_plane_global_query_transport_failure,
                );
                self.production_cluster.?.setJoinCancellationEnabled(
                    mode == .production_data_plane_durable_join_cancellation or
                        mode == .production_data_plane_durable_join_cancellation_overlapping_faults or
                        mode == .production_data_plane_durable_join_cancellation_owner_restart,
                );
                self.production_cluster.?.setJoinCancellationOverlapEnabled(
                    mode == .production_data_plane_durable_join_cancellation_overlapping_faults,
                );
                self.production_cluster.?.setJoinCancellationOwnerRestartEnabled(
                    mode == .production_data_plane_durable_join_cancellation_owner_restart,
                );
                self.production_cluster.?.setJoinWorkerRetryEnabled(
                    mode == .production_data_plane_durable_join_worker_retry,
                );
                self.production_cluster.?.setJoinOwnerRestartEnabled(
                    mode == .production_data_plane_durable_join_owner_restart or
                        mode == .production_data_plane_durable_join_cancellation_owner_restart,
                );
                self.production_cluster.?.setJoinRetryExhaustionEnabled(
                    mode == .production_data_plane_durable_join_retry_exhaustion,
                );
                self.production_cluster.?.setFaultMode(switch (mode) {
                    .production_data_plane_graph_split_transport_failure => .graph_transport_failure,
                    .production_data_plane_graph_split_owner_restart => .graph_owner_restart,
                    .production_data_plane_graph_split_partial_write => .graph_partial_write,
                    .production_data_plane_graph_split_resource_pressure => .resource_pressure,
                    .production_data_plane_graph_split_overlapping_faults => .graph_transport_resource_pressure,
                    .production_data_plane_graph_split_socket_pressure => .socket_pressure,
                    .production_data_plane_graph_cancellation_transport_failure => .graph_hydration_transport_failure,
                    .production_data_plane_durable_join_takeover => .join_finalizer_ack_failure,
                    else => .clean,
                });
                self.production_cluster.?.bootstrap() catch |err| {
                    const teardown_cancelled = blk: {
                        std.Io.checkCancel(self.sim.io()) catch |cancel_err|
                            break :blk cancel_err == error.Canceled;
                        break :blk false;
                    };
                    if (err == error.Canceled or teardown_cancelled or self.tearing_down)
                        std.log.debug("production data-plane VOPR bootstrap canceled by bounded history", .{})
                    else {
                        std.log.err("production data-plane VOPR bootstrap failed: {s}", .{@errorName(err)});
                        if (@errorReturnTrace()) |trace| std.debug.dumpErrorReturnTrace(trace);
                    }
                    self.initialization_failed = true;
                    self.initialization_error_code = @intFromError(err);
                    self.initialization_done = true;
                    self.complete = true;
                    return;
                };
            } else {
                public_fixture = metadata_sim.VoprPublicClusterFixture.init(
                    self.fixture_allocator.allocator(),
                    &self.sim,
                ) catch |err| {
                    self.initialization_failed = true;
                    self.initialization_error_code = @intFromError(err);
                    self.initialization_done = true;
                    self.complete = true;
                    return;
                };
                self.public_cluster = public_fixture;
            }
            self.deployment = vopr.deployment.Composer.init(
                self.fixture_allocator.allocator(),
                deployment_manifest,
            ) catch |err| {
                self.initialization_failed = true;
                self.initialization_error_code = @intFromError(err);
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            self.registerDeployment(mode, public_fixture) catch |err| {
                self.initialization_failed = true;
                self.initialization_error_code = @intFromError(err);
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            self.serverless.startPublicCatalog() catch |err| {
                self.initialization_failed = true;
                self.initialization_error_code = @intFromError(err);
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            self.shared_io_sound = self.serverless.sim == &self.sim and
                if (self.production_cluster) |fixture| fixture.sim == &self.sim else public_fixture.?.sim == &self.sim;
            if (self.production_cluster == null) {
                public_fixture.?.start(mode.publicFault()) catch |err| {
                    self.initialization_failed = true;
                    self.initialization_error_code = @intFromError(err);
                    self.initialization_done = true;
                    self.complete = true;
                    return;
                };
            }
            self.serverless_future = self.sim.io().async(runServerless, .{self});
            self.completion_future = self.sim.io().async(waitForCompletion, .{self});
            self.initialization_done = true;
        }

        fn runServerless(self: *State) void {
            defer {
                self.serverless.stopPublicCatalog();
                self.serverless_done = true;
                self.healServiceRates();
                if (self.production_cluster) |fixture|
                    fixture.start()
                else if (self.public_cluster) |fixture|
                    fixture.allowGraphFaultWorkload();
            }
            const serverless_mode = self.mode.?.serverlessMode();
            self.serverless.runMode(serverless_mode) catch return;
            self.serverless_public_sound = self.serverless.observePublicCatalogForMode(serverless_mode) catch |err| blk: {
                self.serverless_public_error_code = @intFromError(err);
                break :blk false;
            };
            self.serverless_sound = self.serverless.workflowVisibleForMode(serverless_mode) and
                self.serverless_public_sound;
        }

        fn waitForCompletion(self: *State) void {
            while (!(if (self.production_cluster) |fixture| fixture.complete else self.public_cluster.?.complete) or
                !self.serverless_done)
            {
                // Poll at the same logical cadence as the production Raft
                // driver. A 1ns waiter becomes the globally earliest timer and
                // can force hundreds of millions of recorded time choices
                // before a 1ms service ticker is eligible.
                self.sim.io().sleep(.fromMilliseconds(1), .awake) catch return;
            }
            self.finishDeployment() catch {
                self.complete = true;
                return;
            };
            self.complete = true;
        }

        fn registerDeployment(
            self: *State,
            mode: Mode,
            fixture: ?*metadata_sim.VoprPublicClusterFixture,
        ) !void {
            const deployment = &self.deployment.?;
            for (deployment_node_ids) |node_id| try deployment.startNode(node_id);
            for (deployment_instances[0..3]) |instance| try deployment.publishReady(instance.id);
            for (deployment_instances[3..6]) |instance| try deployment.publishReady(instance.id);
            try deployment.publishReady(deployment_instances[6].id);
            switch (mode) {
                .clean => {},
                .metadata_partition => {
                    const leader = fixture.?.metadata_leader_index;
                    for (deployment_links, 0..) |link, index| {
                        if (link.from_node != deployment_node_ids[leader] and link.to_node != deployment_node_ids[leader]) continue;
                        try deployment.activateFault(vopr.id.derive("full-cluster.partition", link.id, index), .network, link.id);
                    }
                },
                .node_restart => try deployment.activateFault(
                    vopr.id.stable(name, "fault.node-restart"),
                    .node_pause,
                    process_domains[fixture.?.client_index],
                ),
                .graph_inflight_restart => try deployment.activateFault(
                    vopr.id.stable(name, "fault.graph-inflight-restart"),
                    .node_pause,
                    process_domains[fixture.?.graph_restart_node_index],
                ),
                // A range merge is an operator/workload transition, not an
                // infrastructure fault domain. Its durable metadata and data
                // owners are already registered above, so there is no fake
                // process, link, or storage fault to activate here.
                .graph_topology_churn => {},
                // VoprIo's current outage primitive covers the complete
                // inter-DataServer fabric. Mirror that scope in the manifest
                // instead of pretending the failure belongs to one arbitrary
                // directional link.
                .graph_transport_failure => for (deployment_links, 0..) |link, index| try deployment.activateFault(
                    vopr.id.derive("full-cluster.graph-transport-failure", link.id, index),
                    .network,
                    link.id,
                ),
                .partial_http_write => try deployment.activateFault(
                    vopr.id.stable(name, "fault.partial-http-write"),
                    .network,
                    deployment_links[0].id,
                ),
                .serverless_stale_generation => try deployment.activateFault(
                    vopr.id.stable(name, "fault.serverless-stale-generation"),
                    .storage,
                    storage_domains[3],
                ),
                .resource_pressure => for (resource_domains[0..3], 0..) |domain_id, index| try deployment.activateFault(
                    vopr.id.derive("full-cluster.resource-pressure", domain_id, index),
                    .resource,
                    domain_id,
                ),
                .production_data_plane_graph_split_transport_failure => for (deployment_links, 0..) |link, index| try deployment.activateFault(
                    vopr.id.derive("full-cluster.production-graph-split-transport-failure", link.id, index),
                    .network,
                    link.id,
                ),
                .production_data_plane_graph_split_owner_restart => {
                    const production = self.production_cluster orelse
                        return error.MissingProductionCluster;
                    const target_index = production.currentGraphOwnerIndex() orelse
                        return error.ProductionDataGraphLeaderMissing;
                    try production.configureGraphRestartTarget(target_index);
                    try deployment.activateFault(
                        vopr.id.stable(name, "fault.production-graph-split-owner-restart"),
                        .node_pause,
                        process_domains[target_index],
                    );
                },
                .production_data_plane_graph_split_partial_write => {
                    const production = self.production_cluster orelse
                        return error.MissingProductionCluster;
                    const target_index = production.currentGraphOwnerIndex() orelse
                        return error.ProductionDataGraphLeaderMissing;
                    const coordinator_index = try production.configureGraphPartialWriteTarget(target_index);
                    const link_id = for (deployment_links) |link| {
                        if (link.from_node == deployment_node_ids[coordinator_index] and
                            link.to_node == deployment_node_ids[target_index]) break link.id;
                    } else return error.ProductionGraphPartialWriteLinkMissing;
                    try deployment.activateFault(
                        vopr.id.stable(name, "fault.production-graph-split-partial-write"),
                        .network,
                        link_id,
                    );
                },
                .production_data_plane_graph_split_resource_pressure => for (resource_domains[0..3], 0..) |domain_id, index| try deployment.activateFault(
                    vopr.id.derive("full-cluster.production-resource-pressure", domain_id, index),
                    .resource,
                    domain_id,
                ),
                .production_data_plane_graph_split_overlapping_faults => {
                    const production = self.production_cluster orelse
                        return error.MissingProductionCluster;
                    const target_index = production.currentGraphOwnerIndex() orelse
                        return error.ProductionDataGraphLeaderMissing;
                    const coordinator_index = try production.configureGraphTransportTarget(target_index);
                    const link_id = for (deployment_links) |link| {
                        if (link.from_node == deployment_node_ids[coordinator_index] and
                            link.to_node == deployment_node_ids[target_index]) break link.id;
                    } else return error.ProductionGraphTransportLinkMissing;
                    try deployment.activateFault(
                        vopr.id.stable(name, "fault.production-overlap-transport"),
                        .network,
                        link_id,
                    );
                    for (resource_domains[0..3], 0..) |domain_id, index| try deployment.activateFault(
                        vopr.id.derive("full-cluster.production-overlap-resource", domain_id, index),
                        .resource,
                        domain_id,
                    );
                },
                .production_data_plane_graph_split_socket_pressure => {
                    const production = self.production_cluster orelse
                        return error.MissingProductionCluster;
                    const target_index = production.currentGraphOwnerIndex() orelse
                        return error.ProductionDataGraphLeaderMissing;
                    try production.configureSocketPressureTarget(target_index);
                    try deployment.activateFault(
                        vopr.id.stable(name, "fault.production-graph-split-socket-pressure"),
                        .resource,
                        resource_domains[target_index],
                    );
                },
                .production_data_plane_graph_cancellation_transport_failure => {
                    const production = self.production_cluster orelse
                        return error.MissingProductionCluster;
                    const target_index = production.currentGraphOwnerIndex() orelse
                        return error.ProductionDataGraphLeaderMissing;
                    const coordinator_index = try production.configureGraphTransportTarget(target_index);
                    const link_id = for (deployment_links) |link| {
                        if (link.from_node == deployment_node_ids[coordinator_index] and
                            link.to_node == deployment_node_ids[target_index]) break link.id;
                    } else return error.ProductionGraphCancellationTransportLinkMissing;
                    try deployment.activateFault(
                        vopr.id.stable(name, "fault.production-graph-cancellation-transport"),
                        .network,
                        link_id,
                    );
                },
                .production_data_plane_global_query_transport_failure => {
                    const production = self.production_cluster orelse
                        return error.MissingProductionCluster;
                    const target_index = production.currentTenantOwnerIndex() orelse
                        return error.ProductionDataTenantLeaderMissing;
                    const coordinator_index = try production.configureGlobalQueryTransportTarget(target_index);
                    const link_id = for (deployment_links) |link| {
                        if (link.from_node == deployment_node_ids[coordinator_index] and
                            link.to_node == deployment_node_ids[target_index]) break link.id;
                    } else return error.ProductionGlobalQueryTransportLinkMissing;
                    try deployment.activateFault(
                        vopr.id.stable(name, "fault.production-global-query-transport"),
                        .network,
                        link_id,
                    );
                },
                .production_data_plane_durable_join_takeover => for (process_domains[0..3], 0..) |domain_id, index| try deployment.activateFault(
                    vopr.id.derive("full-cluster.production-durable-join-finalizer-ack-failure", domain_id, index),
                    .custom,
                    domain_id,
                ),
                .production_data_plane_durable_join_owner_restart,
                .production_data_plane_durable_join_cancellation_owner_restart,
                => {
                    const production = self.production_cluster orelse
                        return error.MissingProductionCluster;
                    production.setJoinOwnerRestartFaultObserver(.{
                        .ptr = self,
                        .activate = activateJoinOwnerRestartFault,
                    });
                },
                .production_data_plane_durable_join_retry_exhaustion => {
                    const production = self.production_cluster orelse
                        return error.MissingProductionCluster;
                    production.setJoinRetryExhaustionFaultObserver(.{
                        .ptr = self,
                        .activate = activateJoinRetryExhaustionFaults,
                    });
                },
                .production_data_plane_durable_join_cancellation_overlapping_faults => {
                    const production = self.production_cluster orelse
                        return error.MissingProductionCluster;
                    production.setJoinCancellationOverlapFaultObserver(.{
                        .ptr = self,
                        .activate = activateJoinCancellationOverlapFaults,
                    });
                },
                .production_data_plane_baseline, .production_data_plane_graph, .production_data_plane, .production_data_plane_graph_split, .production_data_plane_join_split, .production_data_plane_durable_join_cancellation, .production_data_plane_durable_join_worker_retry, .production_data_plane_service_rate, .production_data_plane_graph_hydration, .production_data_plane_graph_cancellation, .production_data_plane_graph_inflight_authorization_revocation, .production_data_plane_graph_stale_snapshot_retry_exhaustion, .production_data_plane_global_query, .production_data_plane_global_query_cancellation, .production_data_plane_global_query_inflight_authorization_revocation => {},
            }
        }

        fn activateJoinOwnerRestartFault(ptr: *anyopaque, node_index: usize) !void {
            const self: *State = @ptrCast(@alignCast(ptr));
            if (node_index >= 3) return error.InvalidProductionJoinOwnerRestartTarget;
            try self.deployment.?.activateFault(
                vopr.id.stable(name, "fault.production-durable-join-owner-restart"),
                .node_pause,
                process_domains[node_index],
            );
        }

        fn activateJoinRetryExhaustionFaults(
            ptr: *anyopaque,
            coordinator_index: usize,
            retry_target_index: usize,
        ) !void {
            const self: *State = @ptrCast(@alignCast(ptr));
            if (coordinator_index >= 3 or retry_target_index >= 3 or
                coordinator_index == retry_target_index)
                return error.InvalidProductionJoinRetryExhaustionTarget;
            const link_id = for (deployment_links) |link| {
                if (link.from_node == deployment_node_ids[coordinator_index] and
                    link.to_node == deployment_node_ids[retry_target_index]) break link.id;
            } else return error.ProductionJoinRetryExhaustionLinkMissing;
            try self.deployment.?.activateFault(
                vopr.id.stable(name, "fault.production-durable-join-retry-exhaustion-network"),
                .network,
                link_id,
            );
            for (resource_domains[0..3], 0..) |domain_id, index| try self.deployment.?.activateFault(
                vopr.id.derive("full-cluster.production-durable-join-retry-exhaustion-resource", domain_id, index),
                .resource,
                domain_id,
            );
        }

        fn activateJoinCancellationOverlapFaults(
            ptr: *anyopaque,
            coordinator_index: usize,
            network_target_index: usize,
        ) !void {
            const self: *State = @ptrCast(@alignCast(ptr));
            if (coordinator_index >= 3 or network_target_index >= 3 or
                coordinator_index == network_target_index)
                return error.InvalidProductionJoinCancellationOverlapTarget;
            const link_id = for (deployment_links) |link| {
                if (link.from_node == deployment_node_ids[coordinator_index] and
                    link.to_node == deployment_node_ids[network_target_index]) break link.id;
            } else return error.ProductionJoinCancellationOverlapLinkMissing;
            try self.deployment.?.activateFault(
                vopr.id.stable(name, "fault.production-durable-join-cancellation-overlap-network"),
                .network,
                link_id,
            );
            for (resource_domains[0..3], 0..) |domain_id, index| try self.deployment.?.activateFault(
                vopr.id.derive("full-cluster.production-durable-join-cancellation-overlap-resource", domain_id, index),
                .resource,
                domain_id,
            );
        }

        fn finishDeployment(self: *State) !void {
            const deployment = &self.deployment.?;
            deployment.healAll();
            _ = try deployment.requestQuietSuffix();
            for (deployment_node_ids[0..3], 0..) |node_id, index| {
                const usage = if (self.production_cluster) |fixture|
                    try fixture.deploymentResourceUsage(index)
                else
                    try self.public_cluster.?.deploymentResourceUsage(index);
                try deployment.observeResources(node_id, usage);
                try deployment.acknowledgeNodeQuiet(node_id);
            }
            try deployment.observeResources(deployment_node_ids[3], .{});
            try deployment.acknowledgeNodeQuiet(deployment_node_ids[3]);
            self.deployment_sound = deployment.quietComplete();
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        return .{ .state = try State.init(allocator) };
    }

    pub fn deinit(world: *World, _: std.mem.Allocator) void {
        world.state.deinit();
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        const state = world.state;
        if (state.mode == null) {
            inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{
                .id = id,
                .name = mode_name,
                .kind = if (mode == .clean or mode == .graph_topology_churn) .workload else .fault,
            });
            return;
        }
        if (!state.sim.scheduler().quiescent()) try state.sim.scheduler().enumerateReady(list, allocator);
    }

    pub fn execute(
        world: *World,
        selected: vopr.transition.Transition,
        events: *vopr.event.Sink,
        allocator: std.mem.Allocator,
    ) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (state.mode == null) {
            var found = false;
            inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
                try state.start(mode);
                found = true;
            };
            if (!found) return error.InvalidFullClusterMode;
        } else {
            try state.sim.scheduler().executeReady(selected.id, events, allocator);
        }
        const requests_ok = if (state.clusterHealth()) |snapshot| snapshot.requests_ok else false;
        try events.emitNamed(allocator, .domain, selected.name, @intFromBool(requests_ok));
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        const fixture = state.public_cluster;
        const production = state.production_cluster;
        const production_progress = if (production) |production_fixture| production_fixture.primaryGroupProgress() else null;
        const cluster = state.clusterHealth();
        const resources = state.sim.resourceSnapshot();
        try builder.addNamed(allocator, name ++ ".mode", if (state.mode) |mode| @as(i64, @intFromEnum(mode)) + 1 else 0);
        try builder.addNamed(allocator, name ++ ".production-phase", if (production) |production_fixture| production_fixture.phaseOrdinal() else 0);
        try builder.addNamed(allocator, name ++ ".production-metadata-phase", if (production) |production_fixture| production_fixture.metadataBootstrapPhaseOrdinal() else 0);
        try builder.addNamed(allocator, name ++ ".production-primary-commit", if (production_progress) |progress| @intCast(progress.commit_index) else 0);
        try builder.addNamed(allocator, name ++ ".production-primary-applied", if (production_progress) |progress| @intCast(progress.applied_index) else 0);
        try builder.addNamed(allocator, name ++ ".production-primary-last", if (production_progress) |progress| @intCast(progress.last_index) else 0);
        try builder.addNamed(allocator, name ++ ".production-primary-leaders", if (production_progress) |progress| @intCast(progress.leaders) else 0);
        try builder.addNamed(allocator, name ++ ".production-primary-peer-routes", if (production_progress) |progress| @intCast(progress.peer_routes) else 0);
        try builder.addNamed(allocator, name ++ ".production-primary-frames-enqueued", if (production_progress) |progress| @intCast(progress.frames_enqueued) else 0);
        try builder.addNamed(allocator, name ++ ".production-primary-frames-pending", if (production_progress) |progress| @intCast(progress.frames_pending) else 0);
        try builder.addNamed(allocator, name ++ ".production-primary-frames-failed", if (production_progress) |progress| @intCast(progress.frames_failed) else 0);
        try builder.addNamed(allocator, name ++ ".production-primary-frames-sent", if (production_progress) |progress| @intCast(progress.frames_sent) else 0);
        try builder.addNamed(allocator, name ++ ".production-driver-rounds", if (production) |production_fixture| @intCast(production_fixture.driver_rounds) else 0);
        try builder.addNamed(allocator, name ++ ".production-metadata-recovery-campaigns", if (production) |production_fixture| @intCast(production_fixture.metadata_recovery_campaigns) else 0);
        try builder.addNamed(allocator, name ++ ".production-driver-done", @intFromBool(if (production) |production_fixture| production_fixture.driver_done else false));
        try builder.addNamed(allocator, name ++ ".production-driver-error", if (production) |production_fixture| if (production_fixture.driver_failure) |err| @intFromError(err) else 0 else 0);
        try builder.addNamed(allocator, name ++ ".production-left-write-status", if (production) |production_fixture| production_fixture.write_statuses[0] else 0);
        try builder.addNamed(allocator, name ++ ".production-right-write-status", if (production) |production_fixture| production_fixture.write_statuses[1] else 0);
        try builder.addNamed(allocator, name ++ ".production-tenant-write-status", if (production) |production_fixture| production_fixture.write_statuses[2] else 0);
        try builder.addNamed(allocator, name ++ ".production-left-write-attempts", if (production) |production_fixture| @intCast(production_fixture.write_attempts[0]) else 0);
        try builder.addNamed(allocator, name ++ ".production-right-write-attempts", if (production) |production_fixture| @intCast(production_fixture.write_attempts[1]) else 0);
        try builder.addNamed(allocator, name ++ ".production-tenant-write-attempts", if (production) |production_fixture| @intCast(production_fixture.write_attempts[2]) else 0);
        try builder.addNamed(allocator, name ++ ".production-left-write-unknowns", if (production) |production_fixture| @intCast(production_fixture.write_outcome_unknowns[0]) else 0);
        try builder.addNamed(allocator, name ++ ".production-right-write-unknowns", if (production) |production_fixture| @intCast(production_fixture.write_outcome_unknowns[1]) else 0);
        try builder.addNamed(allocator, name ++ ".production-tenant-write-unknowns", if (production) |production_fixture| @intCast(production_fixture.write_outcome_unknowns[2]) else 0);
        try builder.addNamed(allocator, name ++ ".production-request-routing-started", if (production) |production_fixture| @intCast(production_fixture.request_lifecycle_counts[@intFromEnum(data_runtime.DataRequestLifecyclePhase.routing_started)]) else 0);
        try builder.addNamed(allocator, name ++ ".production-request-forward-started", if (production) |production_fixture| @intCast(production_fixture.request_lifecycle_counts[@intFromEnum(data_runtime.DataRequestLifecyclePhase.remote_forward_started)]) else 0);
        try builder.addNamed(allocator, name ++ ".production-request-forward-completed", if (production) |production_fixture| @intCast(production_fixture.request_lifecycle_counts[@intFromEnum(data_runtime.DataRequestLifecyclePhase.remote_forward_completed)]) else 0);
        try builder.addNamed(allocator, name ++ ".production-request-proposal-accepted", if (production) |production_fixture| @intCast(production_fixture.request_lifecycle_counts[@intFromEnum(data_runtime.DataRequestLifecyclePhase.proposal_accepted)]) else 0);
        try builder.addNamed(allocator, name ++ ".production-request-proposal-persisted", if (production) |production_fixture| @intCast(production_fixture.request_lifecycle_counts[@intFromEnum(data_runtime.DataRequestLifecyclePhase.proposal_persisted)]) else 0);
        try builder.addNamed(allocator, name ++ ".production-request-apply-confirmed", if (production) |production_fixture| @intCast(production_fixture.request_lifecycle_counts[@intFromEnum(data_runtime.DataRequestLifecyclePhase.apply_confirmed)]) else 0);
        try builder.addNamed(allocator, name ++ ".production-request-visibility-confirmed", if (production) |production_fixture| @intCast(production_fixture.request_lifecycle_counts[@intFromEnum(data_runtime.DataRequestLifecyclePhase.visibility_confirmed)]) else 0);
        try builder.addNamed(allocator, name ++ ".production-request-ack-ready", if (production) |production_fixture| @intCast(production_fixture.request_lifecycle_counts[@intFromEnum(data_runtime.DataRequestLifecyclePhase.response_ack_ready)]) else 0);
        try builder.addNamed(allocator, name ++ ".production-request-last-group", if (production) |production_fixture| @intCast(production_fixture.last_request_lifecycle_group) else 0);
        try builder.addNamed(allocator, name ++ ".production-request-last-index", if (production) |production_fixture| @intCast(production_fixture.last_request_lifecycle_index) else 0);
        try builder.addNamed(allocator, name ++ ".production-request-last-phase", if (production) |production_fixture| @intFromEnum(production_fixture.last_request_lifecycle_phase) + 1 else 0);
        // Observation values are signed, but digests are opaque 64-bit
        // identities. Preserve every bit instead of trapping on hashes whose
        // high bit is set.
        try builder.addNamed(allocator, name ++ ".production-left-write-body", if (production) |production_fixture| @bitCast(production_fixture.write_body_digests[0]) else 0);
        try builder.addNamed(allocator, name ++ ".production-right-write-body", if (production) |production_fixture| @bitCast(production_fixture.write_body_digests[1]) else 0);
        try builder.addNamed(allocator, name ++ ".production-tenant-write-body", if (production) |production_fixture| @bitCast(production_fixture.write_body_digests[2]) else 0);
        try builder.addNamed(allocator, name ++ ".production-left-read-status", if (production) |production_fixture| production_fixture.read_statuses[0] else 0);
        try builder.addNamed(allocator, name ++ ".production-right-read-status", if (production) |production_fixture| production_fixture.read_statuses[1] else 0);
        try builder.addNamed(allocator, name ++ ".production-tenant-read-status", if (production) |production_fixture| production_fixture.read_statuses[2] else 0);
        try builder.addNamed(allocator, name ++ ".production-post-split-read-status", if (production) |production_fixture| production_fixture.read_statuses[3] else 0);
        try builder.addNamed(allocator, name ++ ".production-left-read-body", if (production) |production_fixture| @bitCast(production_fixture.read_body_digests[0]) else 0);
        try builder.addNamed(allocator, name ++ ".production-right-read-body", if (production) |production_fixture| @bitCast(production_fixture.read_body_digests[1]) else 0);
        try builder.addNamed(allocator, name ++ ".production-tenant-read-body", if (production) |production_fixture| @bitCast(production_fixture.read_body_digests[2]) else 0);
        try builder.addNamed(allocator, name ++ ".production-post-split-read-body", if (production) |production_fixture| @bitCast(production_fixture.read_body_digests[3]) else 0);
        try builder.addNamed(allocator, name ++ ".production-left-read-attempts", if (production) |production_fixture| production_fixture.read_attempts[0] else 0);
        try builder.addNamed(allocator, name ++ ".production-right-read-attempts", if (production) |production_fixture| production_fixture.read_attempts[1] else 0);
        try builder.addNamed(allocator, name ++ ".production-tenant-read-attempts", if (production) |production_fixture| production_fixture.read_attempts[2] else 0);
        try builder.addNamed(allocator, name ++ ".production-post-split-read-attempts", if (production) |production_fixture| production_fixture.read_attempts[3] else 0);
        try builder.addNamed(allocator, name ++ ".production-split-finalized", @intFromBool(if (production) |production_fixture| production_fixture.split_finalized else false));
        try builder.addNamed(allocator, name ++ ".production-split-published", @intFromBool(if (production) |production_fixture| production_fixture.split_published else false));
        try builder.addNamed(allocator, name ++ ".production-split-round-trip", @intFromBool(if (production) |production_fixture| production_fixture.split_sound else false));
        try builder.addNamed(allocator, name ++ ".service-rates-enabled", @intFromBool(state.service_rates_enabled));
        try builder.addNamed(allocator, name ++ ".service-rates-healed", @intFromBool(state.service_rates_healed));
        try builder.addNamed(allocator, name ++ ".service-rates-sound", @intFromBool(state.serviceRatesSound()));
        try builder.addNamed(allocator, name ++ ".service-rate-active-effects", @intCast(state.service_rate_model.activeEffectCount()));
        try builder.addNamed(allocator, name ++ ".data-hosts", if (cluster) |snapshot| @intCast(snapshot.hosts) else 0);
        try builder.addNamed(allocator, name ++ ".public-requests-ok", @intFromBool(if (cluster) |snapshot| snapshot.requests_ok else false));
        try builder.addNamed(allocator, name ++ ".write-ok", @intFromBool(if (production) |value| value.write_sound else if (fixture) |public_cluster| public_cluster.write_sound else false));
        try builder.addNamed(allocator, name ++ ".read-ok", @intFromBool(if (production) |value| value.read_sound else if (fixture) |public_cluster| public_cluster.read_sound else false));
        try builder.addNamed(allocator, name ++ ".tenant-write-ok", @intFromBool(if (production) |value| value.tenant_sound else if (fixture) |public_cluster| public_cluster.tenant_write_sound else false));
        try builder.addNamed(allocator, name ++ ".tenant-read-ok", @intFromBool(if (production) |value| value.tenant_sound else if (fixture) |public_cluster| public_cluster.tenant_read_sound else false));
        try builder.addNamed(allocator, name ++ ".table-isolation-ok", @intFromBool(if (production) |value| value.tenant_sound else if (fixture) |public_cluster| public_cluster.table_isolation_sound else false));
        try builder.addNamed(allocator, name ++ ".public-distributed-join", @intFromBool(if (cluster) |snapshot| snapshot.join_query_ok else false));
        try builder.addNamed(allocator, name ++ ".public-split-distributed-join", @intFromBool(if (cluster) |snapshot| snapshot.split_join_query_ok else false));
        try builder.addNamed(allocator, name ++ ".public-post-split-distributed-join", @intFromBool(if (cluster) |snapshot| snapshot.post_split_join_query_ok else false));
        try builder.addNamed(allocator, name ++ ".durable-join-finalizer-ack-failure", @intFromBool(if (cluster) |snapshot| snapshot.join_finalizer_ack_failure_injected else false));
        try builder.addNamed(allocator, name ++ ".durable-join-persisted-owner", if (cluster) |snapshot| @intCast(snapshot.join_finalizer_persisted_group_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-takeover", @intFromBool(if (cluster) |snapshot| snapshot.durable_join_takeover_ok else false));
        try builder.addNamed(allocator, name ++ ".durable-join-worker-started", if (cluster) |snapshot| @intCast(snapshot.join_partition_worker_started_count) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-worker-completed", if (cluster) |snapshot| @intCast(snapshot.join_partition_worker_completed_count) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-worker-retry-failure", @intFromBool(if (cluster) |snapshot| snapshot.join_worker_retry_failure_injected else false));
        try builder.addNamed(allocator, name ++ ".durable-join-worker-retry-job", if (cluster) |snapshot| @bitCast(snapshot.join_worker_retry_job_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-worker-retry-partition", if (cluster) |snapshot| @intCast(snapshot.join_worker_retry_partition_index) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-worker-retry-failed-group", if (cluster) |snapshot| @intCast(snapshot.join_worker_retry_failed_group_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-worker-retry-recovered-group", if (cluster) |snapshot| @intCast(snapshot.join_worker_retry_recovered_group_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-worker-retry-ok", @intFromBool(if (cluster) |snapshot| snapshot.join_worker_retry_ok else false));
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-job", if (cluster) |snapshot| @bitCast(snapshot.join_owner_restart_job_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-partition", if (cluster) |snapshot| @intCast(snapshot.join_owner_restart_partition_index) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-failed-group", if (cluster) |snapshot| @intCast(snapshot.join_owner_restart_failed_group_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-recovered-group", if (cluster) |snapshot| @intCast(snapshot.join_owner_restart_recovered_group_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-target-node", if (cluster) |snapshot| if (snapshot.join_owner_restart_requested) @intCast(snapshot.join_owner_restart_target_index + 1) else 0 else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-recovery-node", if (cluster) |snapshot| if (snapshot.join_owner_restart_recovered_group_id != 0) @intCast(snapshot.join_owner_restart_recovery_index + 1) else 0 else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-requested", @intFromBool(if (cluster) |snapshot| snapshot.join_owner_restart_requested else false));
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-down", @intFromBool(if (cluster) |snapshot| snapshot.join_owner_restart_down else false));
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-reconstructed", @intFromBool(if (cluster) |snapshot| snapshot.join_owner_restart_recovered else false));
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-initial-status", if (cluster) |snapshot| snapshot.join_owner_restart_initial_status else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-no-partial", @intFromBool(if (cluster) |snapshot| snapshot.join_owner_restart_initial_rejected_without_partial else false));
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-recovery-join", @intFromBool(if (cluster) |snapshot| snapshot.join_owner_restart_recovery_join else false));
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-read", @intFromBool(if (cluster) |snapshot| snapshot.join_owner_restart_post_reconstruction_read else false));
        try builder.addNamed(allocator, name ++ ".durable-join-owner-restart-ok", @intFromBool(if (cluster) |snapshot| snapshot.join_owner_restart_ok else false));
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-job", if (cluster) |snapshot| @bitCast(snapshot.join_retry_exhaustion_job_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-partition", if (cluster) |snapshot| @intCast(snapshot.join_retry_exhaustion_partition_index) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-first-group", if (cluster) |snapshot| @intCast(snapshot.join_retry_exhaustion_first_group_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-retry-group", if (cluster) |snapshot| @intCast(snapshot.join_retry_exhaustion_retry_group_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-coordinator", if (cluster) |snapshot| if (snapshot.join_retry_exhaustion_faults_injected) @intCast(snapshot.join_retry_exhaustion_coordinator_index + 1) else 0 else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-target", if (cluster) |snapshot| if (snapshot.join_retry_exhaustion_faults_injected) @intCast(snapshot.join_retry_exhaustion_retry_target_index + 1) else 0 else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-faults", @intFromBool(if (cluster) |snapshot| snapshot.join_retry_exhaustion_faults_injected else false));
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-resource", @intFromBool(if (cluster) |snapshot| snapshot.join_retry_exhaustion_resource_observed else false));
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-network", @intFromBool(if (cluster) |snapshot| snapshot.join_retry_exhaustion_network_observed else false));
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-overlap", @intFromBool(if (cluster) |snapshot| snapshot.join_retry_exhaustion_overlap_observed else false));
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-initial-starts", if (cluster) |snapshot| @intCast(snapshot.join_retry_exhaustion_initial_worker_starts) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-initial-completions", if (cluster) |snapshot| @intCast(snapshot.join_retry_exhaustion_initial_worker_completions) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-initial-status", if (cluster) |snapshot| snapshot.join_retry_exhaustion_initial_status else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-no-partial", @intFromBool(if (cluster) |snapshot| snapshot.join_retry_exhaustion_initial_rejected_without_partial else false));
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-network-healed", @intFromBool(if (cluster) |snapshot| snapshot.join_retry_exhaustion_network_healed else false));
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-resource-healed", @intFromBool(if (cluster) |snapshot| snapshot.join_retry_exhaustion_resource_healed else false));
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-recovery", @intFromBool(if (cluster) |snapshot| snapshot.join_retry_exhaustion_recovery_join else false));
        try builder.addNamed(allocator, name ++ ".durable-join-retry-exhaustion-ok", @intFromBool(if (cluster) |snapshot| snapshot.join_retry_exhaustion_ok else false));
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-boundary", @intFromBool(if (cluster) |snapshot| snapshot.join_cancellation_boundary_observed else false));
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-job", if (cluster) |snapshot| @bitCast(snapshot.join_cancellation_job_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-owner", if (cluster) |snapshot| @intCast(snapshot.join_cancellation_owner_group_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-requested", @intFromBool(if (cluster) |snapshot| snapshot.join_cancellation_requested else false));
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-observed", @intFromBool(if (cluster) |snapshot| snapshot.join_cancellation_observed else false));
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-recovered", @intFromBool(if (cluster) |snapshot| snapshot.join_cancellation_recovered else false));
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-ok", @intFromBool(if (cluster) |snapshot| snapshot.join_cancellation_ok else false));
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-overlap-first-group", if (cluster) |snapshot| @intCast(snapshot.join_cancellation_overlap_first_group_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-overlap-worker-group", if (cluster) |snapshot| @intCast(snapshot.join_cancellation_overlap_worker_group_id) else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-overlap-coordinator", if (cluster) |snapshot| if (snapshot.join_cancellation_overlap_faults_injected) @intCast(snapshot.join_cancellation_overlap_coordinator_index + 1) else 0 else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-overlap-network-target", if (cluster) |snapshot| if (snapshot.join_cancellation_overlap_faults_injected) @intCast(snapshot.join_cancellation_overlap_network_target_index + 1) else 0 else 0);
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-overlap-faults", @intFromBool(if (cluster) |snapshot| snapshot.join_cancellation_overlap_faults_injected else false));
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-overlap-network", @intFromBool(if (cluster) |snapshot| snapshot.join_cancellation_overlap_network_observed else false));
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-overlap-resource", @intFromBool(if (cluster) |snapshot| snapshot.join_cancellation_overlap_resource_observed else false));
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-overlap-observed", @intFromBool(if (cluster) |snapshot| snapshot.join_cancellation_overlap_observed else false));
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-overlap-network-healed", @intFromBool(if (cluster) |snapshot| snapshot.join_cancellation_overlap_network_healed else false));
        try builder.addNamed(allocator, name ++ ".durable-join-cancellation-overlap-resource-healed", @intFromBool(if (cluster) |snapshot| snapshot.join_cancellation_overlap_resource_healed else false));
        try builder.addNamed(allocator, name ++ ".public-cross-range-graph-query", @intFromBool(if (cluster) |snapshot| snapshot.graph_query_ok else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-ok", @intFromBool(if (cluster) |snapshot| snapshot.global_query_ok else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-status", if (cluster) |snapshot| snapshot.global_query_status else 0);
        try builder.addNamed(allocator, name ++ ".public-global-query-responses", if (cluster) |snapshot| @intCast(snapshot.global_query_response_count) else 0);
        try builder.addNamed(allocator, name ++ ".public-global-query-results-assembled", if (cluster) |snapshot| @intCast(snapshot.global_query_result_assembled_count) else 0);
        try builder.addNamed(allocator, name ++ ".public-global-query-cancellation-boundary", @intFromBool(if (cluster) |snapshot| snapshot.global_query_cancellation_boundary_observed else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-cancellation-requested", @intFromBool(if (cluster) |snapshot| snapshot.global_query_cancellation_requested else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-cancellation-observed", @intFromBool(if (cluster) |snapshot| snapshot.global_query_cancellation_observed else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-cancellation-no-partial", @intFromBool(if (cluster) |snapshot| snapshot.global_query_cancellation_no_partial else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-cancellation-recovered", @intFromBool(if (cluster) |snapshot| snapshot.global_query_cancellation_recovered else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-authorization-boundary", @intFromBool(if (cluster) |snapshot| snapshot.global_query_authorization_boundary_observed else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-authorization-revoked", @intFromBool(if (cluster) |snapshot| snapshot.global_query_authorization_revoked else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-authorization-denied-without-leak", @intFromBool(if (cluster) |snapshot| snapshot.global_query_authorization_denied_without_leak else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-authorization-restored", @intFromBool(if (cluster) |snapshot| snapshot.global_query_authorization_restored else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-authorization-recovered", @intFromBool(if (cluster) |snapshot| snapshot.global_query_authorization_recovered else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-authorization-denied-status", if (cluster) |snapshot| @intCast(snapshot.global_query_authorization_denied_status) else 0);
        try builder.addNamed(allocator, name ++ ".public-global-query-authorization-recovered-status", if (cluster) |snapshot| @intCast(snapshot.global_query_authorization_recovered_status) else 0);
        try builder.addNamed(allocator, name ++ ".public-global-query-transport-boundary", @intFromBool(if (cluster) |snapshot| snapshot.global_query_transport_boundary_observed else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-transport-fault-injected", @intFromBool(if (cluster) |snapshot| snapshot.global_query_transport_fault_injected else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-transport-fault-observed", @intFromBool(if (cluster) |snapshot| snapshot.global_query_transport_fault_observed else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-transport-fault-matches", if (cluster) |snapshot| @intCast(snapshot.global_query_transport_fault_matches) else 0);
        try builder.addNamed(allocator, name ++ ".public-global-query-transport-fault-healed", @intFromBool(if (cluster) |snapshot| snapshot.global_query_transport_fault_healed else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-transport-rejected-without-partial", @intFromBool(if (cluster) |snapshot| snapshot.global_query_transport_rejected_without_partial else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-transport-recovered", @intFromBool(if (cluster) |snapshot| snapshot.global_query_transport_recovered else false));
        try builder.addNamed(allocator, name ++ ".public-global-query-transport-rejected-status", if (cluster) |snapshot| @intCast(snapshot.global_query_transport_rejected_status) else 0);
        try builder.addNamed(allocator, name ++ ".public-global-query-transport-recovered-status", if (cluster) |snapshot| @intCast(snapshot.global_query_transport_recovered_status) else 0);
        try builder.addNamed(allocator, name ++ ".public-graph-hydration-ok", @intFromBool(if (cluster) |snapshot| snapshot.graph_hydration_ok else false));
        try builder.addNamed(allocator, name ++ ".public-graph-hydration-started", if (cluster) |snapshot| @intCast(snapshot.graph_hydration_started_count) else 0);
        try builder.addNamed(allocator, name ++ ".public-graph-hydration-fanout-started", if (cluster) |snapshot| @intCast(snapshot.graph_hydration_fanout_started_count) else 0);
        try builder.addNamed(allocator, name ++ ".public-graph-hydration-completed", if (cluster) |snapshot| @intCast(snapshot.graph_hydration_completed_count) else 0);
        try builder.addNamed(allocator, name ++ ".public-graph-cancellation-requested", @intFromBool(if (cluster) |snapshot| snapshot.graph_cancellation_requested else false));
        try builder.addNamed(allocator, name ++ ".public-graph-cancellation-observed", @intFromBool(if (cluster) |snapshot| snapshot.graph_cancellation_observed else false));
        try builder.addNamed(allocator, name ++ ".public-graph-cancellation-recovered", @intFromBool(if (cluster) |snapshot| snapshot.graph_cancellation_recovered else false));
        try builder.addNamed(allocator, name ++ ".public-graph-cancellation-ok", @intFromBool(if (cluster) |snapshot| snapshot.graph_cancellation_ok else false));
        try builder.addNamed(allocator, name ++ ".public-graph-cancellation-fault-injected", @intFromBool(if (cluster) |snapshot| snapshot.graph_cancellation_fault_injected else false));
        try builder.addNamed(allocator, name ++ ".public-graph-cancellation-fault-observed", @intFromBool(if (cluster) |snapshot| snapshot.graph_cancellation_fault_observed else false));
        try builder.addNamed(allocator, name ++ ".public-graph-cancellation-fault-matches", if (cluster) |snapshot| @intCast(snapshot.graph_cancellation_fault_matches) else 0);
        try builder.addNamed(allocator, name ++ ".public-graph-cancellation-fault-healed", @intFromBool(if (cluster) |snapshot| snapshot.graph_cancellation_fault_healed else false));
        try builder.addNamed(allocator, name ++ ".public-graph-authorization-boundary-observed", @intFromBool(if (cluster) |snapshot| snapshot.graph_authorization_boundary_observed else false));
        try builder.addNamed(allocator, name ++ ".public-graph-authorization-revoked", @intFromBool(if (cluster) |snapshot| snapshot.graph_authorization_revoked else false));
        try builder.addNamed(allocator, name ++ ".public-graph-authorization-denied-without-leak", @intFromBool(if (cluster) |snapshot| snapshot.graph_authorization_denied_without_leak else false));
        try builder.addNamed(allocator, name ++ ".public-graph-authorization-restored", @intFromBool(if (cluster) |snapshot| snapshot.graph_authorization_restored else false));
        try builder.addNamed(allocator, name ++ ".public-graph-authorization-recovered", @intFromBool(if (cluster) |snapshot| snapshot.graph_authorization_recovered else false));
        try builder.addNamed(allocator, name ++ ".public-graph-authorization-denied-status", if (cluster) |snapshot| @intCast(snapshot.graph_authorization_denied_status) else 0);
        try builder.addNamed(allocator, name ++ ".public-graph-authorization-recovered-status", if (cluster) |snapshot| @intCast(snapshot.graph_authorization_recovered_status) else 0);
        try builder.addNamed(allocator, name ++ ".public-graph-authorization-ok", @intFromBool(if (cluster) |snapshot| snapshot.graph_authorization_ok else false));
        try builder.addNamed(allocator, name ++ ".public-graph-stale-snapshot-boundary", @intFromBool(if (cluster) |snapshot| snapshot.graph_stale_snapshot_boundary_observed else false));
        try builder.addNamed(allocator, name ++ ".public-graph-stale-snapshot-attempt-failures", if (cluster) |snapshot| @intCast(snapshot.graph_stale_snapshot_attempt_failures) else 0);
        try builder.addNamed(allocator, name ++ ".public-graph-stale-snapshot-error", if (cluster) |snapshot| @intCast(snapshot.graph_stale_snapshot_error_code) else 0);
        try builder.addNamed(allocator, name ++ ".public-graph-stale-snapshot-no-partial", @intFromBool(if (cluster) |snapshot| snapshot.graph_stale_snapshot_rejected_without_partial else false));
        try builder.addNamed(allocator, name ++ ".public-graph-stale-snapshot-status", if (cluster) |snapshot| @intCast(snapshot.graph_stale_snapshot_status) else 0);
        try builder.addNamed(allocator, name ++ ".public-graph-stale-snapshot-recovered", @intFromBool(if (cluster) |snapshot| snapshot.graph_stale_snapshot_recovered else false));
        try builder.addNamed(allocator, name ++ ".public-graph-stale-snapshot-ok", @intFromBool(if (cluster) |snapshot| snapshot.graph_stale_snapshot_ok else false));
        try builder.addNamed(allocator, name ++ ".public-split-graph-inflight-started", @intFromBool(if (cluster) |snapshot| snapshot.split_graph_inflight_started else false));
        try builder.addNamed(allocator, name ++ ".public-split-graph-inflight-complete", @intFromBool(if (cluster) |snapshot| snapshot.split_graph_inflight_complete else false));
        try builder.addNamed(allocator, name ++ ".public-split-graph-inflight-rejected", @intFromBool(if (cluster) |snapshot| snapshot.split_graph_inflight_rejected else false));
        try builder.addNamed(allocator, name ++ ".public-split-graph-inflight-ok", @intFromBool(if (cluster) |snapshot| snapshot.split_graph_inflight_ok else false));
        try builder.addNamed(allocator, name ++ ".public-post-split-graph-query", @intFromBool(if (cluster) |snapshot| snapshot.post_split_graph_query_ok else false));
        try builder.addNamed(allocator, name ++ ".graph-inflight-restart-observed", @intFromBool(if (cluster) |snapshot| snapshot.graph_inflight_restart_observed else false));
        try builder.addNamed(allocator, name ++ ".graph-inflight-restart-recovered", @intFromBool(if (cluster) |snapshot| snapshot.graph_inflight_restart_recovered else false));
        try builder.addNamed(allocator, name ++ ".graph-topology-churn-observed", @intFromBool(if (cluster) |snapshot| snapshot.graph_topology_churn_observed else false));
        try builder.addNamed(allocator, name ++ ".graph-topology-churn-finalized", @intFromBool(if (cluster) |snapshot| snapshot.graph_topology_churn_finalized else false));
        try builder.addNamed(allocator, name ++ ".graph-topology-churn-error", if (cluster) |snapshot| @intCast(snapshot.graph_topology_churn_error_code) else 0);
        try builder.addNamed(allocator, name ++ ".graph-topology-partial-rejected", @intFromBool(if (cluster) |snapshot| snapshot.graph_topology_partial_rejected_sound else false));
        try builder.addNamed(allocator, name ++ ".graph-transport-failure-injected", @intFromBool(if (cluster) |snapshot| snapshot.graph_transport_failure_injected else false));
        try builder.addNamed(allocator, name ++ ".graph-transport-failure-observed", @intFromBool(if (cluster) |snapshot| snapshot.graph_transport_failure_observed else false));
        try builder.addNamed(allocator, name ++ ".graph-transport-failure-error", if (cluster) |snapshot| @intCast(snapshot.graph_transport_failure_error_code) else 0);
        try builder.addNamed(allocator, name ++ ".overlapping-link-resource-faults-active", @intFromBool(if (cluster) |snapshot| snapshot.overlapping_faults_active_observed else false));
        try builder.addNamed(allocator, name ++ ".graph-owner-restart-requested", @intFromBool(if (cluster) |snapshot| snapshot.graph_owner_restart_requested else false));
        try builder.addNamed(allocator, name ++ ".graph-owner-restart-down", @intFromBool(if (cluster) |snapshot| snapshot.graph_owner_restart_down else false));
        try builder.addNamed(allocator, name ++ ".graph-owner-restart-failure-observed", @intFromBool(if (cluster) |snapshot| snapshot.graph_owner_restart_failure_observed else false));
        try builder.addNamed(allocator, name ++ ".graph-owner-restart-recovered", @intFromBool(if (cluster) |snapshot| snapshot.graph_owner_restart_recovered else false));
        try builder.addNamed(allocator, name ++ ".graph-owner-restart-error", if (cluster) |snapshot| @intCast(snapshot.graph_owner_restart_error_code) else 0);
        try builder.addNamed(allocator, name ++ ".graph-partial-write-injected", @intFromBool(if (cluster) |snapshot| snapshot.graph_partial_write_injected else false));
        try builder.addNamed(allocator, name ++ ".graph-partial-write-observed", @intFromBool(if (cluster) |snapshot| snapshot.graph_partial_write_observed else false));
        try builder.addNamed(allocator, name ++ ".socket-pressure-injected", @intFromBool(if (cluster) |snapshot| snapshot.socket_pressure_injected else false));
        try builder.addNamed(allocator, name ++ ".socket-pressure-denial-observed", @intFromBool(if (cluster) |snapshot| snapshot.socket_pressure_denial_observed else false));
        try builder.addNamed(allocator, name ++ ".socket-pressure-error", if (cluster) |snapshot| @intCast(snapshot.socket_pressure_error_code) else 0);
        try builder.addNamed(allocator, name ++ ".socket-pressure-no-ingress", @intFromBool(if (cluster) |snapshot| snapshot.socket_pressure_no_ingress else false));
        try builder.addNamed(allocator, name ++ ".socket-pressure-recovered", @intFromBool(if (cluster) |snapshot| snapshot.socket_pressure_recovered else false));
        try builder.addNamed(allocator, name ++ ".graph-partial-result-rejected", @intFromBool(if (cluster) |snapshot| snapshot.graph_partial_rejected_sound else false));
        try builder.addNamed(allocator, name ++ ".request-errors", if (production) |value| @intFromBool(value.failure != null) else if (fixture) |public_cluster| @intCast(public_cluster.request_errors) else 0);
        try builder.addNamed(allocator, name ++ ".last-request-error", if (production) |value| if (value.failure) |err| @intFromError(err) else 0 else if (fixture) |public_cluster| @intCast(public_cluster.last_request_error_code) else 0);
        try builder.addNamed(allocator, name ++ ".serverless-visible", @intFromBool(state.serverless_sound));
        try builder.addNamed(allocator, name ++ ".serverless-public-http-visible", @intFromBool(state.serverless_public_sound));
        try builder.addNamed(allocator, name ++ ".serverless-public-http-error", @intCast(state.serverless_public_error_code));
        try builder.addNamed(allocator, name ++ ".raft-wire-requests", if (cluster) |snapshot| @intCast(snapshot.raft_wire_requests) else 0);
        try builder.addNamed(allocator, name ++ ".node-resource-managers", if (cluster) |snapshot| @intCast(snapshot.node_resource_managers) else 0);
        try builder.addNamed(allocator, name ++ ".resource-denial-ok", @intFromBool(if (cluster) |snapshot| snapshot.resource_denial_ok else false));
        try builder.addNamed(allocator, name ++ ".resource-recovery-ok", @intFromBool(if (cluster) |snapshot| snapshot.resource_recovery_ok else false));
        try builder.addNamed(allocator, name ++ ".resource-pressure-observed", @intFromBool(if (cluster) |snapshot| snapshot.resource_pressure_observed else false));
        try builder.addNamed(allocator, name ++ ".resource-denial-error", if (cluster) |snapshot| @intCast(snapshot.resource_denial_error_code) else 0);
        try builder.addNamed(allocator, name ++ ".production-resource-denial-status", if (cluster) |snapshot| snapshot.resource_denial_status else 0);
        try builder.addNamed(allocator, name ++ ".production-resource-preproposal-denial", @intFromBool(if (cluster) |snapshot| snapshot.resource_preproposal_denial else false));
        try builder.addNamed(allocator, name ++ ".production-resource-outcome-unknown", @intFromBool(if (cluster) |snapshot| snapshot.resource_outcome_unknown else false));
        try builder.addNamed(allocator, name ++ ".production-resource-read-before-retry", @intFromBool(if (cluster) |snapshot| snapshot.resource_read_before_retry else false));
        try builder.addNamed(allocator, name ++ ".production-resource-retry-attempted", @intFromBool(if (cluster) |snapshot| snapshot.resource_retry_attempted else false));
        try builder.addNamed(allocator, name ++ ".production-resource-proposals-before", if (cluster) |snapshot| @intCast(snapshot.resource_proposals_before) else 0);
        try builder.addNamed(allocator, name ++ ".production-resource-proposals-after", if (cluster) |snapshot| @intCast(snapshot.resource_proposals_after) else 0);
        try builder.addNamed(allocator, name ++ ".production-resource-absent-before-retry", @intFromBool(if (cluster) |snapshot| snapshot.resource_absent_before_retry else false));
        try builder.addNamed(allocator, name ++ ".production-resource-post-split-ok", @intFromBool(if (cluster) |snapshot| snapshot.resource_post_split_ok else false));
        try builder.addNamed(allocator, name ++ ".deployment-quiet", @intFromBool(state.deployment_sound));
        try builder.addNamed(allocator, name ++ ".initialization-failed", @intFromBool(state.initialization_failed));
        try builder.addNamed(allocator, name ++ ".initialization-error", @intCast(state.initialization_error_code));
        try builder.addNamed(allocator, name ++ ".open-sockets", @intCast(resources.open_sockets));
        try builder.addNamed(allocator, name ++ ".active-tasks", @intCast(resources.active_tasks));
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(state.complete));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        const fixture = state.public_cluster;
        const production = state.production_cluster;
        const cluster = state.clusterHealth();
        const production_mode = state.mode != null and state.mode.?.isProduction();
        const production_graph_mode = state.mode == .production_data_plane_graph or
            state.mode == .production_data_plane_service_rate or
            state.mode == .production_data_plane_graph_hydration or
            state.mode == .production_data_plane_graph_cancellation or
            state.mode == .production_data_plane_graph_cancellation_transport_failure or
            state.mode == .production_data_plane_graph_inflight_authorization_revocation or
            state.mode == .production_data_plane_graph_stale_snapshot_retry_exhaustion or
            state.mode == .production_data_plane_graph_split or
            state.mode == .production_data_plane_graph_split_transport_failure or
            state.mode == .production_data_plane_graph_split_owner_restart or
            state.mode == .production_data_plane_graph_split_partial_write or
            state.mode == .production_data_plane_graph_split_resource_pressure or
            state.mode == .production_data_plane_graph_split_overlapping_faults or
            state.mode == .production_data_plane_graph_split_socket_pressure;
        const production_graph_split_mode = state.mode == .production_data_plane_graph_split or
            state.mode == .production_data_plane_graph_split_transport_failure or
            state.mode == .production_data_plane_graph_split_owner_restart or
            state.mode == .production_data_plane_graph_split_partial_write or
            state.mode == .production_data_plane_graph_split_resource_pressure or
            state.mode == .production_data_plane_graph_split_overlapping_faults or
            state.mode == .production_data_plane_graph_split_socket_pressure;
        const resources = state.sim.resourceSnapshot();
        try sink.check(allocator, acknowledged_id, !state.complete or (cluster != null and cluster.?.requests_ok));
        try sink.check(allocator, quorum_id, !state.complete or (cluster != null and cluster.?.topology_ok));
        try sink.check(allocator, routing_id, !state.complete or (cluster != null and cluster.?.hosts == 2));
        try sink.check(allocator, isolation_id, !state.complete or
            (if (production) |value| value.tenant_sound else fixture != null and fixture.?.table_isolation_sound));
        try sink.check(allocator, production_global_query_id, !state.complete or
            (state.mode.? != .production_data_plane_global_query and
                state.mode.? != .production_data_plane_global_query_cancellation and
                state.mode.? != .production_data_plane_global_query_inflight_authorization_revocation and
                state.mode.? != .production_data_plane_global_query_transport_failure) or
            (cluster != null and cluster.?.global_query_ok and
                cluster.?.global_query_status == 200 and
                cluster.?.global_query_response_count == 2));
        try sink.check(allocator, production_global_query_cancellation_id, !state.complete or
            state.mode.? != .production_data_plane_global_query_cancellation or
            (cluster != null and cluster.?.global_query_cancellation_ok and
                cluster.?.global_query_cancellation_boundary_observed and
                cluster.?.global_query_cancellation_requested and
                cluster.?.global_query_cancellation_observed and
                cluster.?.global_query_cancellation_no_partial and
                cluster.?.global_query_cancellation_recovered and
                cluster.?.global_query_result_assembled_count == 3));
        try sink.check(allocator, production_global_query_authorization_id, !state.complete or
            state.mode.? != .production_data_plane_global_query_inflight_authorization_revocation or
            (cluster != null and cluster.?.global_query_authorization_ok and
                cluster.?.global_query_authorization_boundary_observed and
                cluster.?.global_query_authorization_revoked and
                cluster.?.global_query_authorization_denied_without_leak and
                cluster.?.global_query_authorization_denied_status == 403 and
                cluster.?.global_query_authorization_restored and
                cluster.?.global_query_authorization_recovered and
                cluster.?.global_query_authorization_recovered_status == 200 and
                cluster.?.global_query_result_assembled_count == 3));
        try sink.check(allocator, production_global_query_transport_id, !state.complete or
            state.mode.? != .production_data_plane_global_query_transport_failure or
            (cluster != null and cluster.?.global_query_transport_ok and
                cluster.?.global_query_transport_boundary_observed and
                cluster.?.global_query_transport_fault_injected and
                cluster.?.global_query_transport_fault_observed and
                cluster.?.global_query_transport_fault_matches == 1 and
                cluster.?.global_query_transport_fault_healed and
                cluster.?.global_query_transport_rejected_without_partial and
                cluster.?.global_query_transport_rejected_status == 503 and
                cluster.?.global_query_transport_recovered and
                cluster.?.global_query_transport_recovered_status == 200 and
                cluster.?.global_query_result_assembled_count == 3));
        try sink.check(allocator, graph_query_id, !state.complete or
            (if (production_mode)
                !production_graph_mode or (cluster != null and cluster.?.graph_query_ok)
            else
                cluster != null and cluster.?.graph_query_ok));
        try sink.check(allocator, graph_split_id, !state.complete or !production_graph_split_mode or
            (cluster != null and cluster.?.graph_query_ok and
                cluster.?.split_graph_inflight_started and cluster.?.split_graph_inflight_ok and
                (cluster.?.split_graph_inflight_complete != cluster.?.split_graph_inflight_rejected) and
                cluster.?.post_split_graph_query_ok));
        try sink.check(allocator, graph_split_transport_id, !state.complete or state.mode.? != .production_data_plane_graph_split_transport_failure or
            (cluster != null and cluster.?.graph_query_ok and
                cluster.?.split_graph_inflight_started and !cluster.?.split_graph_inflight_complete and
                cluster.?.split_graph_inflight_rejected and cluster.?.split_graph_inflight_ok and
                cluster.?.graph_transport_failure_injected and cluster.?.graph_transport_failure_observed and
                cluster.?.graph_transport_failure_error_code != 0 and cluster.?.graph_partial_rejected_sound and
                cluster.?.post_split_graph_query_ok));
        try sink.check(allocator, graph_split_owner_restart_id, !state.complete or state.mode.? != .production_data_plane_graph_split_owner_restart or
            (cluster != null and cluster.?.graph_query_ok and
                cluster.?.split_graph_inflight_started and !cluster.?.split_graph_inflight_complete and
                cluster.?.split_graph_inflight_rejected and cluster.?.split_graph_inflight_ok and
                cluster.?.graph_owner_restart_requested and cluster.?.graph_owner_restart_down and
                cluster.?.graph_owner_restart_failure_observed and cluster.?.graph_owner_restart_recovered and
                cluster.?.graph_owner_restart_error_code != 0 and cluster.?.graph_partial_rejected_sound and
                cluster.?.post_split_graph_query_ok));
        try sink.check(allocator, graph_split_partial_write_id, !state.complete or state.mode.? != .production_data_plane_graph_split_partial_write or
            (cluster != null and cluster.?.graph_query_ok and
                cluster.?.split_graph_inflight_started and cluster.?.split_graph_inflight_complete and
                !cluster.?.split_graph_inflight_rejected and cluster.?.split_graph_inflight_ok and
                cluster.?.graph_partial_write_injected and cluster.?.graph_partial_write_observed and
                cluster.?.post_split_graph_query_ok));
        try sink.check(allocator, production_resource_split_id, !state.complete or state.mode.? != .production_data_plane_graph_split_resource_pressure or
            (cluster != null and cluster.?.graph_query_ok and
                cluster.?.split_graph_inflight_started and cluster.?.split_graph_inflight_complete and
                !cluster.?.split_graph_inflight_rejected and cluster.?.split_graph_inflight_ok and
                cluster.?.resource_pressure_observed and cluster.?.resource_denial_ok and
                cluster.?.resource_read_before_retry and
                ((cluster.?.resource_preproposal_denial and cluster.?.resource_denial_status == 503 and
                    cluster.?.resource_proposals_after == cluster.?.resource_proposals_before) or
                    (cluster.?.resource_outcome_unknown and cluster.?.resource_denial_status == 409 and
                        cluster.?.resource_proposals_after > cluster.?.resource_proposals_before)) and
                (!cluster.?.resource_retry_attempted or cluster.?.resource_absent_before_retry) and
                cluster.?.resource_recovery_ok and cluster.?.resource_post_split_ok and
                cluster.?.post_split_graph_query_ok));
        try sink.check(allocator, production_join_split_id, !state.complete or state.mode.? != .production_data_plane_join_split or
            (cluster != null and cluster.?.join_query_ok and cluster.?.split_join_query_ok and
                cluster.?.post_split_join_query_ok));
        try sink.check(allocator, production_durable_join_takeover_id, !state.complete or state.mode.? != .production_data_plane_durable_join_takeover or
            (cluster != null and cluster.?.join_query_ok and
                cluster.?.join_finalizer_ack_failure_injected and
                cluster.?.join_finalizer_persisted_group_id != 0 and
                cluster.?.durable_join_takeover_ok));
        try sink.check(allocator, production_join_cancellation_id, !state.complete or
            state.mode.? != .production_data_plane_durable_join_cancellation or
            (cluster != null and cluster.?.join_query_ok and
                cluster.?.join_cancellation_ok and
                cluster.?.join_cancellation_boundary_observed and
                cluster.?.join_cancellation_job_id != 0 and
                cluster.?.join_cancellation_owner_group_id != 0 and
                cluster.?.join_cancellation_requested and
                cluster.?.join_cancellation_observed and
                cluster.?.join_cancellation_recovered and
                cluster.?.join_partition_worker_completed_count > 0 and
                cluster.?.join_partition_worker_started_count ==
                    cluster.?.join_partition_worker_completed_count + 1));
        try sink.check(allocator, production_join_cancellation_overlap_id, !state.complete or
            state.mode.? != .production_data_plane_durable_join_cancellation_overlapping_faults or
            (cluster != null and cluster.?.join_query_ok and
                cluster.?.join_cancellation_ok and
                cluster.?.join_cancellation_boundary_observed and
                cluster.?.join_cancellation_job_id != 0 and
                cluster.?.join_cancellation_owner_group_id != 0 and
                cluster.?.join_cancellation_requested and
                cluster.?.join_cancellation_observed and
                cluster.?.join_cancellation_recovered and
                cluster.?.join_cancellation_overlap_faults_injected and
                cluster.?.join_cancellation_overlap_network_observed and
                cluster.?.join_cancellation_overlap_resource_observed and
                cluster.?.join_cancellation_overlap_observed and
                cluster.?.join_cancellation_overlap_network_healed and
                cluster.?.join_cancellation_overlap_resource_healed and
                cluster.?.join_cancellation_overlap_first_group_id != 0 and
                cluster.?.join_cancellation_overlap_worker_group_id != 0 and
                cluster.?.join_cancellation_overlap_first_group_id !=
                    cluster.?.join_cancellation_overlap_worker_group_id and
                cluster.?.join_cancellation_owner_group_id ==
                    cluster.?.join_cancellation_overlap_worker_group_id and
                cluster.?.join_cancellation_overlap_coordinator_index !=
                    cluster.?.join_cancellation_overlap_network_target_index and
                cluster.?.join_partition_worker_completed_count > 0 and
                cluster.?.join_partition_worker_started_count ==
                    cluster.?.join_partition_worker_completed_count + 1));
        try sink.check(allocator, production_join_cancellation_owner_restart_id, !state.complete or
            state.mode.? != .production_data_plane_durable_join_cancellation_owner_restart or
            (cluster != null and cluster.?.join_query_ok and
                cluster.?.join_cancellation_ok and
                cluster.?.join_cancellation_boundary_observed and
                cluster.?.join_cancellation_job_id != 0 and
                cluster.?.join_cancellation_owner_group_id != 0 and
                cluster.?.join_cancellation_requested and
                cluster.?.join_cancellation_observed and
                cluster.?.join_cancellation_recovered and
                cluster.?.join_owner_restart_requested and
                cluster.?.join_owner_restart_job_id == cluster.?.join_cancellation_job_id and
                cluster.?.join_owner_restart_failed_group_id ==
                    cluster.?.join_cancellation_owner_group_id and
                cluster.?.join_owner_restart_target_index !=
                    cluster.?.join_owner_restart_coordinator_index and
                cluster.?.join_owner_restart_down and
                cluster.?.join_owner_restart_recovered and
                cluster.?.join_owner_restart_recovery_join and
                cluster.?.join_owner_restart_post_reconstruction_read and
                cluster.?.join_owner_restart_ok and
                cluster.?.join_partition_worker_completed_count > 0 and
                cluster.?.join_partition_worker_started_count ==
                    cluster.?.join_partition_worker_completed_count + 1));
        try sink.check(allocator, production_join_worker_retry_id, !state.complete or
            state.mode.? != .production_data_plane_durable_join_worker_retry or
            (cluster != null and cluster.?.join_query_ok and
                cluster.?.join_worker_retry_ok and
                cluster.?.join_worker_retry_failure_injected and
                cluster.?.join_worker_retry_job_id != 0 and
                cluster.?.join_worker_retry_failed_group_id != 0 and
                cluster.?.join_worker_retry_recovered_group_id != 0 and
                cluster.?.join_worker_retry_failed_group_id !=
                    cluster.?.join_worker_retry_recovered_group_id and
                cluster.?.join_partition_worker_completed_count > 0 and
                cluster.?.join_partition_worker_started_count >
                    cluster.?.join_partition_worker_completed_count));
        try sink.check(allocator, production_join_owner_restart_id, !state.complete or
            state.mode.? != .production_data_plane_durable_join_owner_restart or
            (cluster != null and cluster.?.join_query_ok and
                cluster.?.join_owner_restart_ok and
                cluster.?.join_owner_restart_requested and
                cluster.?.join_owner_restart_down and
                cluster.?.join_owner_restart_recovered and
                cluster.?.join_owner_restart_initial_status == 503 and
                cluster.?.join_owner_restart_initial_rejected_without_partial and
                cluster.?.join_owner_restart_recovery_join and
                cluster.?.join_owner_restart_post_reconstruction_read and
                cluster.?.join_owner_restart_job_id != 0 and
                cluster.?.join_owner_restart_failed_group_id != 0 and
                cluster.?.join_owner_restart_recovered_group_id != 0 and
                cluster.?.join_owner_restart_failed_group_id !=
                    cluster.?.join_owner_restart_recovered_group_id and
                cluster.?.join_owner_restart_target_index !=
                    cluster.?.join_owner_restart_recovery_index and
                cluster.?.join_partition_worker_completed_count > 0 and
                cluster.?.join_partition_worker_started_count >
                    cluster.?.join_partition_worker_completed_count));
        try sink.check(allocator, production_join_retry_exhaustion_id, !state.complete or
            state.mode.? != .production_data_plane_durable_join_retry_exhaustion or
            (cluster != null and cluster.?.join_query_ok and
                cluster.?.join_retry_exhaustion_ok and
                cluster.?.join_retry_exhaustion_faults_injected and
                cluster.?.join_retry_exhaustion_resource_observed and
                cluster.?.join_retry_exhaustion_network_observed and
                cluster.?.join_retry_exhaustion_overlap_observed and
                cluster.?.join_retry_exhaustion_job_id != 0 and
                cluster.?.join_retry_exhaustion_first_group_id != 0 and
                cluster.?.join_retry_exhaustion_retry_group_id != 0 and
                cluster.?.join_retry_exhaustion_first_group_id !=
                    cluster.?.join_retry_exhaustion_retry_group_id and
                cluster.?.join_retry_exhaustion_coordinator_index !=
                    cluster.?.join_retry_exhaustion_retry_target_index and
                cluster.?.join_retry_exhaustion_initial_worker_starts > 0 and
                cluster.?.join_retry_exhaustion_initial_worker_completions == 0 and
                cluster.?.join_retry_exhaustion_initial_status == 503 and
                cluster.?.join_retry_exhaustion_initial_rejected_without_partial and
                cluster.?.join_retry_exhaustion_network_healed and
                cluster.?.join_retry_exhaustion_resource_healed and
                cluster.?.join_retry_exhaustion_recovery_join and
                cluster.?.join_partition_worker_completed_count > 0 and
                cluster.?.join_partition_worker_started_count >
                    cluster.?.join_partition_worker_completed_count));
        try sink.check(allocator, production_overlapping_faults_id, !state.complete or state.mode.? != .production_data_plane_graph_split_overlapping_faults or
            (cluster != null and cluster.?.graph_query_ok and
                cluster.?.split_graph_inflight_started and !cluster.?.split_graph_inflight_complete and
                cluster.?.split_graph_inflight_rejected and cluster.?.split_graph_inflight_ok and
                cluster.?.graph_transport_failure_injected and cluster.?.graph_transport_failure_observed and
                cluster.?.graph_transport_failure_error_code != 0 and
                cluster.?.overlapping_faults_active_observed and
                cluster.?.graph_partial_rejected_sound and
                cluster.?.resource_pressure_observed and cluster.?.resource_denial_ok and
                cluster.?.resource_read_before_retry and
                ((cluster.?.resource_preproposal_denial and cluster.?.resource_denial_status == 503 and
                    cluster.?.resource_proposals_after == cluster.?.resource_proposals_before) or
                    (cluster.?.resource_outcome_unknown and cluster.?.resource_denial_status == 409 and
                        cluster.?.resource_proposals_after > cluster.?.resource_proposals_before)) and
                (!cluster.?.resource_retry_attempted or cluster.?.resource_absent_before_retry) and
                cluster.?.resource_recovery_ok and
                cluster.?.resource_post_split_ok and cluster.?.post_split_graph_query_ok));
        try sink.check(allocator, production_socket_pressure_id, !state.complete or state.mode.? != .production_data_plane_graph_split_socket_pressure or
            (cluster != null and cluster.?.graph_query_ok and
                cluster.?.split_graph_inflight_started and cluster.?.split_graph_inflight_complete and
                !cluster.?.split_graph_inflight_rejected and cluster.?.split_graph_inflight_ok and
                cluster.?.socket_pressure_injected and cluster.?.socket_pressure_denial_observed and
                cluster.?.socket_pressure_error_code == @intFromError(error.ProcessFdQuotaExceeded) and
                cluster.?.socket_pressure_no_ingress and cluster.?.socket_pressure_recovered and
                cluster.?.post_split_graph_query_ok));
        try sink.check(allocator, production_service_rate_id, !state.complete or
            state.mode.? != .production_data_plane_service_rate or state.serviceRatesSound());
        try sink.check(allocator, production_graph_hydration_id, !state.complete or
            state.mode.? != .production_data_plane_graph_hydration or
            (cluster != null and cluster.?.graph_hydration_ok and
                cluster.?.graph_hydration_started_count == 1 and
                cluster.?.graph_hydration_fanout_started_count == 1 and
                cluster.?.graph_hydration_completed_count == 1));
        try sink.check(allocator, production_graph_cancellation_id, !state.complete or
            state.mode.? != .production_data_plane_graph_cancellation or
            (cluster != null and cluster.?.graph_cancellation_ok and
                cluster.?.graph_cancellation_requested and
                cluster.?.graph_cancellation_observed and
                cluster.?.graph_cancellation_recovered and
                cluster.?.graph_hydration_started_count == 2 and
                cluster.?.graph_hydration_fanout_started_count == 2 and
                cluster.?.graph_hydration_completed_count == 1));
        try sink.check(allocator, production_graph_cancellation_transport_id, !state.complete or
            state.mode.? != .production_data_plane_graph_cancellation_transport_failure or
            (cluster != null and cluster.?.graph_cancellation_ok and
                cluster.?.graph_cancellation_requested and
                cluster.?.graph_cancellation_observed and
                cluster.?.graph_cancellation_recovered and
                cluster.?.graph_cancellation_fault_injected and
                cluster.?.graph_cancellation_fault_observed and
                cluster.?.graph_cancellation_fault_matches > 0 and
                cluster.?.graph_cancellation_fault_healed and
                cluster.?.graph_hydration_started_count == 2 and
                cluster.?.graph_hydration_fanout_started_count == 2 and
                cluster.?.graph_hydration_completed_count == 1));
        try sink.check(allocator, production_graph_authorization_id, !state.complete or
            state.mode.? != .production_data_plane_graph_inflight_authorization_revocation or
            (cluster != null and cluster.?.graph_authorization_ok and
                cluster.?.graph_authorization_boundary_observed and
                cluster.?.graph_authorization_revoked and
                cluster.?.graph_authorization_denied_without_leak and
                cluster.?.graph_authorization_denied_status == 200 and
                cluster.?.graph_authorization_restored and
                cluster.?.graph_authorization_recovered and
                cluster.?.graph_authorization_recovered_status == 200));
        try sink.check(allocator, production_graph_stale_snapshot_id, !state.complete or
            state.mode.? != .production_data_plane_graph_stale_snapshot_retry_exhaustion or
            (cluster != null and cluster.?.graph_stale_snapshot_ok and
                cluster.?.graph_stale_snapshot_boundary_observed and
                cluster.?.graph_stale_snapshot_attempt_failures == 2 and
                cluster.?.graph_stale_snapshot_error_code == @intFromError(error.TopologyChanged) and
                cluster.?.graph_stale_snapshot_rejected_without_partial and
                cluster.?.graph_stale_snapshot_status == 503 and
                cluster.?.graph_stale_snapshot_recovered and
                cluster.?.post_split_graph_query_ok));
        try sink.check(allocator, graph_restart_id, !state.complete or state.mode.? != .graph_inflight_restart or
            (cluster != null and cluster.?.graph_inflight_restart_observed and cluster.?.graph_inflight_restart_recovered and
                cluster.?.graph_query_ok));
        try sink.check(allocator, graph_topology_id, !state.complete or state.mode.? != .graph_topology_churn or
            (cluster != null and cluster.?.graph_topology_churn_observed and cluster.?.graph_topology_churn_finalized and
                cluster.?.graph_topology_churn_error_code != 0 and cluster.?.graph_topology_partial_rejected_sound and
                cluster.?.graph_query_ok));
        try sink.check(allocator, graph_partial_id, !state.complete or state.mode.? != .graph_transport_failure or
            (cluster != null and cluster.?.graph_transport_failure_injected and
                cluster.?.graph_transport_failure_observed and cluster.?.graph_transport_failure_error_code != 0 and
                cluster.?.graph_partial_rejected_sound and cluster.?.graph_query_ok));
        try sink.check(allocator, publication_id, !state.complete or state.serverless_sound);
        try sink.check(allocator, serverless_http_id, !state.complete or state.serverless_public_sound);
        try sink.check(allocator, shared_io_id, !state.initialization_done or state.shared_io_sound);
        try sink.check(allocator, raft_wire_id, !state.complete or (cluster != null and cluster.?.raft_wire_requests > 0));
        try sink.check(allocator, node_resources_id, !state.initialization_done or (cluster != null and cluster.?.node_resource_managers == 3));
        try sink.check(allocator, resource_recovery_id, !state.complete or state.mode.? != .resource_pressure or
            (cluster != null and cluster.?.resource_pressure_observed and cluster.?.resource_denial_ok and
                cluster.?.resource_recovery_ok));
        try sink.check(allocator, deployment_id, !state.complete or state.deployment_sound);
        try sink.check(allocator, cleanup_id, !state.complete or
            (cluster != null and cluster.?.cleanup_ok and resources.open_sockets == 0));
        try sink.check(allocator, complete_id, state.complete);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        const fixture = state.public_cluster;
        const production = state.production_cluster;
        const cluster = state.clusterHealth();
        return state.sim.healthSnapshot(.{
            .progress_expected = state.mode != null,
            .progress_units = @as(u64, @intFromBool(if (production) |value| value.workload_done else if (fixture) |public_cluster| public_cluster.write_finished else false)) +
                @as(u64, @intFromBool(if (fixture) |public_cluster| public_cluster.read_finished else false)) +
                @as(u64, @intFromBool(if (fixture) |public_cluster| public_cluster.tenant_write_finished else false)) +
                @as(u64, @intFromBool(if (fixture) |public_cluster| public_cluster.tenant_read_finished else false)) +
                @as(u64, @intFromBool(state.serverless_done)),
            .recovery_expected = state.mode != null and state.mode.? != .clean,
            .recovery_complete = state.complete and cluster != null and cluster.?.topology_ok,
            .consistency_valid = !state.initialization_failed and (cluster == null or cluster.?.requests_ok) and (!state.serverless_done or state.serverless_sound),
            .cleanup_complete = state.complete and cluster != null and cluster.?.cleanup_ok,
        });
    }

    pub fn done(world: *World) bool {
        // Initialization failures are terminal scenario outcomes. Their
        // service owners are deliberately stopped and drained by State.deinit;
        // waiting for pre-teardown quiescence here misclassifies a diagnosed
        // bootstrap error as a harness deadlock.
        return world.state.complete and
            (world.state.initialization_failed or world.state.sim.scheduler().quiescent());
    }
};

const CompletionExpectation = enum {
    complete,
    bounded_lifecycle,
};

fn runExactMode(
    history_alloc: std.mem.Allocator,
    mode_id: vopr.id.StableId,
    mode_ordinal: usize,
    transition_budget: usize,
    completion_expectation: CompletionExpectation,
) !void {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    const production_baseline_mode = mode_id == Scenario.mode_ids[Scenario.production_baseline_ordinal];
    const production_graph_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_ordinal];
    const production_split_mode = mode_id == Scenario.mode_ids[Scenario.production_split_ordinal];
    const production_graph_split_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_split_ordinal];
    const production_graph_split_transport_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_split_transport_ordinal];
    const production_graph_split_owner_restart_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_split_owner_restart_ordinal];
    const production_graph_split_partial_write_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_split_partial_write_ordinal];
    const production_graph_split_resource_pressure_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_split_resource_pressure_ordinal];
    const production_join_split_mode = mode_id == Scenario.mode_ids[Scenario.production_join_split_ordinal];
    const production_durable_join_takeover_mode = mode_id == Scenario.mode_ids[Scenario.production_durable_join_takeover_ordinal];
    const production_durable_join_cancellation_mode = mode_id == Scenario.mode_ids[Scenario.production_durable_join_cancellation_ordinal];
    const production_durable_join_worker_retry_mode = mode_id == Scenario.mode_ids[Scenario.production_durable_join_worker_retry_ordinal];
    const production_durable_join_owner_restart_mode = mode_id == Scenario.mode_ids[Scenario.production_durable_join_owner_restart_ordinal];
    const production_durable_join_retry_exhaustion_mode = mode_id == Scenario.mode_ids[Scenario.production_durable_join_retry_exhaustion_ordinal];
    const production_durable_join_cancellation_overlap_mode = mode_id == Scenario.mode_ids[Scenario.production_durable_join_cancellation_overlap_ordinal];
    const production_durable_join_cancellation_owner_restart_mode = mode_id == Scenario.mode_ids[Scenario.production_durable_join_cancellation_owner_restart_ordinal];
    const production_graph_split_overlapping_faults_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_split_overlapping_faults_ordinal];
    const production_graph_split_socket_pressure_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_split_socket_pressure_ordinal];
    const production_service_rate_mode = mode_id == Scenario.mode_ids[Scenario.production_service_rate_ordinal];
    const production_graph_hydration_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_hydration_ordinal];
    const production_graph_cancellation_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_cancellation_ordinal];
    const production_graph_cancellation_transport_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_cancellation_transport_ordinal];
    const production_graph_inflight_authorization_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_authorization_ordinal];
    const production_graph_stale_snapshot_mode = mode_id == Scenario.mode_ids[Scenario.production_graph_stale_snapshot_ordinal];
    const production_global_query_mode = mode_id == Scenario.mode_ids[Scenario.production_global_query_ordinal];
    const production_global_query_cancellation_mode = mode_id == Scenario.mode_ids[Scenario.production_global_query_cancellation_ordinal];
    const production_global_query_authorization_mode = mode_id == Scenario.mode_ids[Scenario.production_global_query_authorization_ordinal];
    const production_global_query_transport_mode = mode_id == Scenario.mode_ids[Scenario.production_global_query_transport_ordinal];
    const production_mode = production_baseline_mode or production_graph_mode or
        production_split_mode or production_graph_split_mode or
        production_graph_split_transport_mode or production_graph_split_owner_restart_mode or
        production_graph_split_partial_write_mode or production_graph_split_resource_pressure_mode or
        production_join_split_mode or production_durable_join_takeover_mode or
        production_durable_join_cancellation_mode or production_durable_join_worker_retry_mode or
        production_durable_join_owner_restart_mode or
        production_durable_join_retry_exhaustion_mode or production_durable_join_cancellation_overlap_mode or
        production_durable_join_cancellation_owner_restart_mode or
        production_graph_split_overlapping_faults_mode or production_graph_split_socket_pressure_mode or
        production_service_rate_mode or production_graph_hydration_mode or
        production_graph_cancellation_mode or production_graph_cancellation_transport_mode or
        production_graph_inflight_authorization_mode or
        production_graph_stale_snapshot_mode or production_global_query_mode or
        production_global_query_cancellation_mode or production_global_query_authorization_mode or
        production_global_query_transport_mode;
    // Fault extensions of the promoted graph/split history keep its
    // cooperative scheduling seed. The prefixed mode remains distinct replay
    // truth, while comparable scheduling ensures the experiment changes the
    // production fault rather than accidentally selecting a starvation-heavy
    // unrelated schedule solely because a new enum ordinal was appended.
    const schedule_ordinal = if (production_join_split_mode)
        Scenario.production_split_ordinal
    else if (production_durable_join_takeover_mode or production_durable_join_cancellation_mode or
        production_durable_join_worker_retry_mode or production_durable_join_owner_restart_mode or
        production_durable_join_retry_exhaustion_mode or production_durable_join_cancellation_overlap_mode or
        production_durable_join_cancellation_owner_restart_mode)
        Scenario.production_graph_ordinal
    else if (production_graph_split_owner_restart_mode or
        production_graph_split_partial_write_mode or production_graph_split_resource_pressure_mode)
        Scenario.production_graph_split_transport_ordinal
    else if (production_graph_split_overlapping_faults_mode or production_graph_split_socket_pressure_mode)
        Scenario.production_graph_split_transport_ordinal
    else if (production_service_rate_mode)
        Scenario.production_graph_ordinal
    else if (production_graph_hydration_mode)
        Scenario.production_graph_ordinal
    else if (production_graph_cancellation_mode or production_graph_cancellation_transport_mode)
        Scenario.production_graph_ordinal
    else if (production_graph_inflight_authorization_mode)
        Scenario.production_graph_ordinal
    else if (production_graph_stale_snapshot_mode)
        Scenario.production_graph_split_ordinal
    else if (production_global_query_mode or production_global_query_cancellation_mode or
        production_global_query_authorization_mode or production_global_query_transport_mode)
        Scenario.production_graph_ordinal
    else
        mode_ordinal;
    var fair_choices = vopr.choice.PrefixedFairSeeded.init(&.{mode_id}, 0x4655_4c4c + schedule_ordinal);
    var cooperative_choices = vopr.choice.PrefixedCooperativeSeeded.init(&.{mode_id}, 0x4655_4c4c + schedule_ordinal);
    const choice_source = if (production_mode)
        cooperative_choices.source()
    else
        fair_choices.source();
    var recorded = try vopr.runner.run(Scenario, history_alloc, choice_source, .{
        .system = "antfly",
        .transition_budget = transition_budget,
        .resource_budget = if (production_mode) 256 else 96,
        .backend_ids = &backend_ids,
        .source_revision = if (production_mode)
            (if (production_global_query_transport_mode)
                "full-cluster-vopr-v39-public-global-query-transport-failure"
            else if (production_global_query_authorization_mode)
                "full-cluster-vopr-v38-public-global-query-inflight-authorization"
            else if (production_global_query_cancellation_mode)
                "full-cluster-vopr-v37-public-global-query-cancellation"
            else if (production_global_query_mode)
                "full-cluster-vopr-v36-public-global-query"
            else if (production_durable_join_cancellation_owner_restart_mode)
                "full-cluster-vopr-v35-durable-join-cancellation-owner-restart"
            else if (production_durable_join_cancellation_overlap_mode)
                "full-cluster-vopr-v34-durable-join-cancellation-overlapping-faults"
            else if (production_durable_join_retry_exhaustion_mode)
                "full-cluster-vopr-v33-durable-join-retry-exhaustion"
            else if (production_durable_join_owner_restart_mode)
                "full-cluster-vopr-v32-durable-join-owner-restart"
            else if (production_durable_join_worker_retry_mode)
                "full-cluster-vopr-v31-durable-join-worker-retry"
            else if (production_durable_join_cancellation_mode)
                "full-cluster-vopr-v30-durable-join-cancellation"
            else if (production_graph_cancellation_transport_mode)
                "full-cluster-vopr-v29-graph-cancellation-transport-failure"
            else if (production_graph_inflight_authorization_mode)
                "full-cluster-vopr-v27-graph-inflight-authorization"
            else if (production_graph_stale_snapshot_mode)
                "full-cluster-vopr-v28-graph-stale-snapshot"
            else if (production_graph_cancellation_mode)
                "full-cluster-vopr-v25-graph-cancellation"
            else if (production_graph_hydration_mode)
                "full-cluster-vopr-v24-graph-hydration"
            else if (production_service_rate_mode)
                "full-cluster-vopr-v23-service-rate"
            else if (production_graph_split_socket_pressure_mode)
                "full-cluster-vopr-v22-graph-split-socket-pressure"
            else if (production_graph_split_overlapping_faults_mode)
                "full-cluster-vopr-v21-graph-split-overlapping-faults"
            else if (production_durable_join_takeover_mode)
                "full-cluster-vopr-v20-durable-join-takeover"
            else if (production_join_split_mode)
                "full-cluster-vopr-v19-join-split"
            else if (production_graph_split_resource_pressure_mode)
                "full-cluster-vopr-v18-graph-split-resource-pressure"
            else if (production_graph_split_partial_write_mode)
                "full-cluster-vopr-v17-graph-split-partial-write"
            else if (production_graph_split_owner_restart_mode)
                "full-cluster-vopr-v16-graph-split-owner-restart"
            else if (production_graph_split_transport_mode)
                "full-cluster-vopr-v15-graph-split-transport"
            else if (production_graph_split_mode)
                "full-cluster-vopr-v14-graph-split"
            else if (production_split_mode)
                "full-cluster-vopr-v12"
            else if (production_graph_mode)
                "full-cluster-vopr-v13-graph"
            else
                "full-cluster-vopr-v11")
        else
            "full-cluster-vopr-v9",
        .target = "native",
        .optimize = @tagName(@import("builtin").mode),
    });
    defer recorded.deinit();
    if (completion_expectation == .complete and recorded.summary.?.property_failures != 0) {
        for (recorded.failures.items) |failure| std.debug.print(
            "full cluster mode={s} failure={s} class={s}\n",
            .{ Scenario.mode_names[mode_ordinal], failure.identity, @tagName(failure.class) },
        );
        if (recorded.observations.items.len > 0) for (recorded.observations.items[recorded.observations.items.len - 1].features) |feature| {
            std.debug.print("  {s}={d}\n", .{ feature.name, feature.value });
            if (std.mem.eql(u8, feature.name, Scenario.name ++ ".last-request-error") and feature.value != 0) {
                const request_error: anyerror = @errorFromInt(@as(u16, @intCast(feature.value)));
                std.debug.print("  request-error-name={s}\n", .{@errorName(request_error)});
            }
            if (std.mem.eql(u8, feature.name, Scenario.name ++ ".production-driver-error") and feature.value != 0) {
                const driver_error: anyerror = @errorFromInt(@as(u16, @intCast(feature.value)));
                std.debug.print("  production-driver-error-name={s}\n", .{@errorName(driver_error)});
            }
            if (std.mem.eql(u8, feature.name, Scenario.name ++ ".serverless-public-http-error") and feature.value != 0) {
                const request_error: anyerror = @errorFromInt(@as(u16, @intCast(feature.value)));
                std.debug.print("  serverless-public-http-error-name={s}\n", .{@errorName(request_error)});
            }
        };
    }
    // Reproducibility is a prerequisite for interpreting either a passing or
    // failing property result. Replay before asserting the scenario oracle so
    // newly discovered failures cannot bypass the exact-replay gate.
    var replayed = vopr.replay.exact(Scenario, history_alloc, &recorded) catch |err| {
        std.debug.print("full cluster mode={s} exact replay failed: {s}\n", .{
            Scenario.mode_names[mode_ordinal],
            @errorName(err),
        });
        return err;
    };
    replayed.deinit();

    switch (completion_expectation) {
        .complete => try std.testing.expectEqual(@as(u64, 0), recorded.summary.?.property_failures),
        .bounded_lifecycle => {
            // This is deliberately not a completion proof. It retains a fast,
            // exact-replayed regression for cancellation and owner unwinding
            // while the production-sized composed witness is being phased.
            try std.testing.expectEqual(@as(u64, 1), recorded.summary.?.property_failures);
            try std.testing.expectEqual(@as(usize, 1), recorded.failures.items.len);
            try std.testing.expectEqual(Scenario.complete_id, recorded.failures.items[0].property_id.?);
            try std.testing.expectEqualStrings(Scenario.name ++ ".history-completes", recorded.failures.items[0].identity);
            std.debug.print("production bounded lifecycle transitions={d} observations={d}\n", .{
                recorded.summary.?.transitions,
                recorded.observations.items.len,
            });
            const last_observation = recorded.observations.items[recorded.observations.items.len - 1];
            for (last_observation.features) |feature| {
                if (std.mem.eql(u8, feature.name, Scenario.name ++ ".production-phase") or
                    std.mem.eql(u8, feature.name, Scenario.name ++ ".production-metadata-phase") or
                    std.mem.startsWith(u8, feature.name, Scenario.name ++ ".production-primary-") or
                    std.mem.startsWith(u8, feature.name, Scenario.name ++ ".production-driver-"))
                    std.debug.print("production bounded lifecycle {s}={d}\n", .{ feature.name, feature.value });
            }
        },
    }
}

test "full cluster VOPR exact replays the composed deployment and recovery" {
    // Stackful fibers make host unwinding both expensive and unsafe. Preserve
    // leak and ownership checking while keeping stack capture disabled, as the
    // production-sized fixtures below already do.
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const history_alloc = history_allocator.allocator();
    // Keep the promoted aggregate on its last green contract. Experimental
    // modes receive a distinct focused gate and join this slice only after
    // their recorded history and exact replay pass within the tier budget.
    const promoted_mode_count = Scenario.production_baseline_ordinal;
    for (Scenario.mode_ids[0..promoted_mode_count], 0..) |mode_id, mode_ordinal| {
        try runExactMode(history_alloc, mode_id, mode_ordinal, 50_000, .complete);
    }
}

test "full cluster production data plane VOPR active split exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_split_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        320_000,
        .complete,
    );
}

test "full cluster production data plane graph exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        60_000,
        .complete,
    );
}

test "full cluster production data plane graph active split exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_split_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        400_000,
        .complete,
    );
}

test "full cluster production data plane graph active split transport failure exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_split_transport_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        450_000,
        .complete,
    );
}

test "full cluster production data plane graph active split owner restart exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_split_owner_restart_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        650_000,
        .complete,
    );
}

test "full cluster production data plane graph active split partial write exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_split_partial_write_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        500_000,
        .complete,
    );
}

test "full cluster production data plane graph active split resource pressure exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_split_resource_pressure_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        550_000,
        .complete,
    );
}

test "full cluster production data plane distributed join active split exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_join_split_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        420_000,
        .complete,
    );
}

test "full cluster production data plane durable shuffle join finalizer takeover exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_durable_join_takeover_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        300_000,
        .complete,
    );
}

test "full cluster production durable shuffle join cancellation exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_durable_join_cancellation_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        360_000,
        .complete,
    );
}

test "full cluster production durable shuffle partition worker failover exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_durable_join_worker_retry_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        360_000,
        .complete,
    );
}

test "full cluster production durable shuffle partition owner reconstruction exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_durable_join_owner_restart_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        420_000,
        .complete,
    );
}

test "full cluster production durable shuffle overlapping fault retry exhaustion exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_durable_join_retry_exhaustion_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        420_000,
        .complete,
    );
}

test "full cluster production durable shuffle cancellation under overlapping faults exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_durable_join_cancellation_overlap_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        420_000,
        .complete,
    );
}

test "full cluster production durable shuffle cancellation with owner reconstruction exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_durable_join_cancellation_owner_restart_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        420_000,
        .complete,
    );
}

test "full cluster production data plane graph active split overlapping link resource faults exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_split_overlapping_faults_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        600_000,
        .complete,
    );
}

test "full cluster production data plane graph active split socket pressure exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_split_socket_pressure_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        500_000,
        .complete,
    );
}

test "full cluster production service rates compose heal and exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_service_rate_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        90_000,
        .complete,
    );
}

test "full cluster production public graph hydration exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_hydration_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        90_000,
        .complete,
    );
}

test "full cluster production public graph cancellation exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_cancellation_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        110_000,
        .complete,
    );
}

test "full cluster production public graph cancellation under transport fault exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_cancellation_transport_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        140_000,
        .complete,
    );
}

test "full cluster production public graph inflight authorization revocation exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_authorization_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        120_000,
        .complete,
    );
}

test "full cluster production public graph stale snapshot retry exhaustion exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_graph_stale_snapshot_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        340_000,
        .complete,
    );
}

test "full cluster production public global query exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_global_query_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        90_000,
        .complete,
    );
}

test "full cluster production public global query cancellation exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_global_query_cancellation_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        110_000,
        .complete,
    );
}

test "full cluster production public global query inflight authorization revocation exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_global_query_authorization_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        120_000,
        .complete,
    );
}

test "full cluster production public global query transport failure exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_global_query_transport_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        140_000,
        .complete,
    );
}

test "full cluster production data plane VOPR bounded cutoff exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_split_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        2_000,
        .bounded_lifecycle,
    );
}

test "full cluster production data plane baseline exact replay" {
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const ordinal = Scenario.production_baseline_ordinal;
    try runExactMode(
        history_allocator.allocator(),
        Scenario.mode_ids[ordinal],
        ordinal,
        30_000,
        .complete,
    );
}
