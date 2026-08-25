// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Deployment-shaped exact-replay composition. A metadata quorum, two hosted
//! data replicas, three public API nodes, two client tasks, and a production
//! serverless worker share one VoprIo scheduler and virtual clock.

const std = @import("std");
const vopr = @import("vopr");
const metadata_sim = @import("../metadata/sim_harness.zig");
const serverless_workflow = @import("serverless_workflow.zig");
const FixtureAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

pub const Scenario = struct {
    pub const name: []const u8 = "full-cluster";
    pub const version: u32 = 8;

    const acknowledged_id = vopr.id.stable(name, "acknowledged-data-visible");
    const quorum_id = vopr.id.stable(name, "metadata-quorum-recovers");
    const routing_id = vopr.id.stable(name, "non-host-routing-sound");
    const isolation_id = vopr.id.stable(name, "multi-table-isolation");
    const graph_query_id = vopr.id.stable(name, "public-cross-range-graph-query-sound");
    const graph_restart_id = vopr.id.stable(name, "public-graph-inflight-restart-sound");
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
        .{ .id = graph_restart_id, .name = name ++ ".public-graph-inflight-restart-sound", .kind = .always },
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
        graph_transport_failure,
        partial_http_write,
        serverless_stale_generation,
        resource_pressure,

        fn publicFault(self: Mode) PublicFault {
            return switch (self) {
                .clean, .serverless_stale_generation => .clean,
                .metadata_partition => .metadata_partition,
                .node_restart => .node_restart,
                .graph_inflight_restart => .graph_inflight_restart,
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
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "clean"),
        vopr.id.stable(name, "metadata-partition"),
        vopr.id.stable(name, "node-restart"),
        vopr.id.stable(name, "graph-inflight-restart"),
        vopr.id.stable(name, "graph-transport-failure"),
        vopr.id.stable(name, "partial-http-write"),
        vopr.id.stable(name, "serverless-stale-generation"),
        vopr.id.stable(name, "resource-pressure"),
    };
    const mode_names = [_][]const u8{
        name ++ ".clean",
        name ++ ".metadata-partition",
        name ++ ".node-restart",
        name ++ ".graph-inflight-restart",
        name ++ ".graph-transport-failure",
        name ++ ".partial-http-write",
        name ++ ".serverless-stale-generation",
        name ++ ".resource-pressure",
    };

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

    const State = struct {
        owner_alloc: std.mem.Allocator,
        fixture_allocator: FixtureAllocator,
        sim: vopr.vopr_io.VoprIo,
        public_cluster: ?*metadata_sim.VoprPublicClusterFixture = null,
        deployment: ?vopr.deployment.Composer = null,
        serverless: *serverless_workflow.Scenario.Fixture,
        mode: ?Mode = null,
        initialization_done: bool = false,
        initialization_failed: bool = false,
        serverless_done: bool = false,
        serverless_sound: bool = false,
        serverless_public_sound: bool = false,
        serverless_public_error_code: u64 = 0,
        shared_io_sound: bool = false,
        deployment_sound: bool = false,
        complete: bool = false,

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
            self.serverless = try serverless_workflow.Scenario.Fixture.initWithVoprIo(fixture_alloc, &self.sim);
            errdefer self.serverless.deinit();
            self.mode = null;
            self.public_cluster = null;
            self.deployment = null;
            self.initialization_done = false;
            self.initialization_failed = false;
            self.serverless_done = false;
            self.serverless_sound = false;
            self.serverless_public_sound = false;
            self.serverless_public_error_code = 0;
            self.shared_io_sound = false;
            self.deployment_sound = false;
            self.complete = false;
            _ = self.sim.io().async(initializeAndRun, .{self});
            return self;
        }

        fn deinit(self: *State) void {
            if (self.deployment) |*deployment| deployment.deinit();
            self.serverless.deinit();
            if (self.public_cluster) |fixture| fixture.deinit();
            self.sim.deinit();
            std.debug.assert(self.fixture_allocator.deinit() == .ok);
            self.owner_alloc.destroy(self);
        }

        fn start(self: *State, mode: Mode) !void {
            self.mode = mode;
        }

        fn initializeAndRun(self: *State) void {
            const mode = self.mode orelse {
                self.initialization_failed = true;
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            const fixture = metadata_sim.VoprPublicClusterFixture.init(
                self.fixture_allocator.allocator(),
                &self.sim,
            ) catch {
                self.initialization_failed = true;
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            self.public_cluster = fixture;
            self.deployment = vopr.deployment.Composer.init(
                self.fixture_allocator.allocator(),
                deployment_manifest,
            ) catch {
                self.initialization_failed = true;
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            self.registerDeployment(mode, fixture) catch {
                self.initialization_failed = true;
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            self.serverless.startPublicCatalog() catch {
                self.initialization_failed = true;
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            self.shared_io_sound = self.serverless.sim == &self.sim and fixture.sim == &self.sim;
            fixture.start(mode.publicFault()) catch {
                self.initialization_failed = true;
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            _ = self.sim.io().async(runServerless, .{self});
            _ = self.sim.io().async(waitForCompletion, .{self});
            self.initialization_done = true;
        }

        fn runServerless(self: *State) void {
            defer {
                self.serverless.stopPublicCatalog();
                self.serverless_done = true;
                if (self.public_cluster) |fixture| fixture.allowGraphFaultWorkload();
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
            while (!self.public_cluster.?.complete or !self.serverless_done) {
                self.sim.io().sleep(.fromNanoseconds(1), .awake) catch return;
            }
            self.finishDeployment() catch {
                self.complete = true;
                return;
            };
            self.complete = true;
        }

        fn registerDeployment(self: *State, mode: Mode, fixture: *metadata_sim.VoprPublicClusterFixture) !void {
            const deployment = &self.deployment.?;
            for (deployment_node_ids) |node_id| try deployment.startNode(node_id);
            for (deployment_instances[0..3]) |instance| try deployment.publishReady(instance.id);
            for (deployment_instances[3..6]) |instance| try deployment.publishReady(instance.id);
            try deployment.publishReady(deployment_instances[6].id);
            switch (mode) {
                .clean => {},
                .metadata_partition => {
                    const leader = fixture.metadata_leader_index;
                    for (deployment_links, 0..) |link, index| {
                        if (link.from_node != deployment_node_ids[leader] and link.to_node != deployment_node_ids[leader]) continue;
                        try deployment.activateFault(vopr.id.derive("full-cluster.partition", link.id, index), .network, link.id);
                    }
                },
                .node_restart => try deployment.activateFault(
                    vopr.id.stable(name, "fault.node-restart"),
                    .node_pause,
                    process_domains[fixture.client_index],
                ),
                .graph_inflight_restart => try deployment.activateFault(
                    vopr.id.stable(name, "fault.graph-inflight-restart"),
                    .node_pause,
                    process_domains[fixture.graph_restart_node_index],
                ),
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
            }
        }

        fn finishDeployment(self: *State) !void {
            const deployment = &self.deployment.?;
            deployment.healAll();
            _ = try deployment.requestQuietSuffix();
            for (deployment_node_ids[0..3], 0..) |node_id, index| {
                try deployment.observeResources(node_id, try self.public_cluster.?.deploymentResourceUsage(index));
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
                .kind = if (mode == .clean) .workload else .fault,
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
        const requests_ok = if (state.public_cluster) |fixture| fixture.healthSnapshot().requests_ok else false;
        try events.emitNamed(allocator, .domain, selected.name, @intFromBool(requests_ok));
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        const fixture = state.public_cluster;
        const cluster = if (fixture) |public_cluster| public_cluster.healthSnapshot() else null;
        const resources = state.sim.resourceSnapshot();
        try builder.addNamed(allocator, name ++ ".mode", if (state.mode) |mode| @as(i64, @intFromEnum(mode)) + 1 else 0);
        try builder.addNamed(allocator, name ++ ".data-hosts", if (cluster) |snapshot| @intCast(snapshot.hosts) else 0);
        try builder.addNamed(allocator, name ++ ".public-requests-ok", @intFromBool(if (cluster) |snapshot| snapshot.requests_ok else false));
        try builder.addNamed(allocator, name ++ ".write-ok", @intFromBool(if (fixture) |public_cluster| public_cluster.write_sound else false));
        try builder.addNamed(allocator, name ++ ".read-ok", @intFromBool(if (fixture) |public_cluster| public_cluster.read_sound else false));
        try builder.addNamed(allocator, name ++ ".tenant-write-ok", @intFromBool(if (fixture) |public_cluster| public_cluster.tenant_write_sound else false));
        try builder.addNamed(allocator, name ++ ".tenant-read-ok", @intFromBool(if (fixture) |public_cluster| public_cluster.tenant_read_sound else false));
        try builder.addNamed(allocator, name ++ ".table-isolation-ok", @intFromBool(if (fixture) |public_cluster| public_cluster.table_isolation_sound else false));
        try builder.addNamed(allocator, name ++ ".public-cross-range-graph-query", @intFromBool(if (cluster) |snapshot| snapshot.graph_query_ok else false));
        try builder.addNamed(allocator, name ++ ".graph-inflight-restart-observed", @intFromBool(if (cluster) |snapshot| snapshot.graph_inflight_restart_observed else false));
        try builder.addNamed(allocator, name ++ ".graph-inflight-restart-recovered", @intFromBool(if (cluster) |snapshot| snapshot.graph_inflight_restart_recovered else false));
        try builder.addNamed(allocator, name ++ ".graph-transport-failure-injected", @intFromBool(if (cluster) |snapshot| snapshot.graph_transport_failure_injected else false));
        try builder.addNamed(allocator, name ++ ".graph-transport-failure-observed", @intFromBool(if (cluster) |snapshot| snapshot.graph_transport_failure_observed else false));
        try builder.addNamed(allocator, name ++ ".graph-transport-failure-error", if (cluster) |snapshot| @intCast(snapshot.graph_transport_failure_error_code) else 0);
        try builder.addNamed(allocator, name ++ ".graph-partial-result-rejected", @intFromBool(if (cluster) |snapshot| snapshot.graph_partial_rejected_sound else false));
        try builder.addNamed(allocator, name ++ ".request-errors", if (fixture) |public_cluster| @intCast(public_cluster.request_errors) else 0);
        try builder.addNamed(allocator, name ++ ".last-request-error", if (fixture) |public_cluster| @intCast(public_cluster.last_request_error_code) else 0);
        try builder.addNamed(allocator, name ++ ".serverless-visible", @intFromBool(state.serverless_sound));
        try builder.addNamed(allocator, name ++ ".serverless-public-http-visible", @intFromBool(state.serverless_public_sound));
        try builder.addNamed(allocator, name ++ ".serverless-public-http-error", @intCast(state.serverless_public_error_code));
        try builder.addNamed(allocator, name ++ ".raft-wire-requests", if (cluster) |snapshot| @intCast(snapshot.raft_wire_requests) else 0);
        try builder.addNamed(allocator, name ++ ".node-resource-managers", if (cluster) |snapshot| @intCast(snapshot.node_resource_managers) else 0);
        try builder.addNamed(allocator, name ++ ".resource-denial-ok", @intFromBool(if (cluster) |snapshot| snapshot.resource_denial_ok else false));
        try builder.addNamed(allocator, name ++ ".resource-recovery-ok", @intFromBool(if (cluster) |snapshot| snapshot.resource_recovery_ok else false));
        try builder.addNamed(allocator, name ++ ".resource-pressure-observed", @intFromBool(if (cluster) |snapshot| snapshot.resource_pressure_observed else false));
        try builder.addNamed(allocator, name ++ ".resource-denial-error", if (cluster) |snapshot| @intCast(snapshot.resource_denial_error_code) else 0);
        try builder.addNamed(allocator, name ++ ".deployment-quiet", @intFromBool(state.deployment_sound));
        try builder.addNamed(allocator, name ++ ".initialization-failed", @intFromBool(state.initialization_failed));
        try builder.addNamed(allocator, name ++ ".open-sockets", @intCast(resources.open_sockets));
        try builder.addNamed(allocator, name ++ ".active-tasks", @intCast(resources.active_tasks));
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(state.complete));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        const fixture = state.public_cluster;
        const cluster = if (fixture) |public_cluster| public_cluster.healthSnapshot() else null;
        const resources = state.sim.resourceSnapshot();
        try sink.check(allocator, acknowledged_id, !state.complete or (cluster != null and cluster.?.requests_ok));
        try sink.check(allocator, quorum_id, !state.complete or (cluster != null and cluster.?.topology_ok));
        try sink.check(allocator, routing_id, !state.complete or (cluster != null and cluster.?.hosts == 2));
        try sink.check(allocator, isolation_id, !state.complete or (fixture != null and fixture.?.table_isolation_sound));
        try sink.check(allocator, graph_query_id, !state.complete or (cluster != null and cluster.?.graph_query_ok));
        try sink.check(allocator, graph_restart_id, !state.complete or state.mode.? != .graph_inflight_restart or
            (cluster != null and cluster.?.graph_inflight_restart_observed and cluster.?.graph_inflight_restart_recovered and
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
        const cluster = if (fixture) |public_cluster| public_cluster.healthSnapshot() else null;
        return state.sim.healthSnapshot(.{
            .progress_expected = state.mode != null,
            .progress_units = @as(u64, @intFromBool(if (fixture) |public_cluster| public_cluster.write_finished else false)) +
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
        return world.state.complete and world.state.sim.scheduler().quiescent();
    }
};

test "full cluster VOPR exact replays metadata data serverless HTTP clients and recovery" {
    // Stackful fibers make host unwinding both expensive and unsafe. Preserve
    // leak and ownership checking while keeping stack capture disabled, as the
    // production-sized fixtures below already do.
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const history_alloc = history_allocator.allocator();
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids, 0..) |mode_id, mode_ordinal| {
        var choices = vopr.choice.PrefixedFairSeeded.init(&.{mode_id}, 0x4655_4c4c + mode_ordinal);
        var recorded = try vopr.runner.run(Scenario, history_alloc, choices.source(), .{
            .system = "antfly",
            .transition_budget = 50_000,
            .resource_budget = 96,
            .backend_ids = &backend_ids,
            .source_revision = "full-cluster-vopr-v8",
            .target = "native",
            .optimize = @tagName(@import("builtin").mode),
        });
        defer recorded.deinit();
        if (recorded.summary.?.property_failures != 0) {
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
                if (std.mem.eql(u8, feature.name, Scenario.name ++ ".serverless-public-http-error") and feature.value != 0) {
                    const request_error: anyerror = @errorFromInt(@as(u16, @intCast(feature.value)));
                    std.debug.print("  serverless-public-http-error-name={s}\n", .{@errorName(request_error)});
                }
            };
        }
        try std.testing.expectEqual(@as(u64, 0), recorded.summary.?.property_failures);
        var replayed = vopr.replay.exact(Scenario, history_alloc, &recorded) catch |err| {
            std.debug.print("full cluster mode={s} exact replay failed: {s}\n", .{
                Scenario.mode_names[mode_ordinal],
                @errorName(err),
            });
            return err;
        };
        replayed.deinit();
    }
}
