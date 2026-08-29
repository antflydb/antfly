// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Exact-replay coverage for the production distributed graph planning,
//! fanout, retry, snapshot-generation, cancellation, and hydration seams.

const std = @import("std");
const vopr = @import("vopr");
const distributed_graph = @import("../api/distributed_graph.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const table_catalog = @import("../api/table_catalog.zig");
const db_types = @import("../storage/db/types.zig");
const graph_query = @import("../graph/query.zig");
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;

const FixtureAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

pub const Scenario = struct {
    pub const name: []const u8 = "distributed-query";
    pub const version: u32 = 3;

    const result_id = vopr.id.stable(name, "result-sound");
    const retry_id = vopr.id.stable(name, "retry-bounded");
    const cancellation_id = vopr.id.stable(name, "cancellation-propagates");
    const generation_id = vopr.id.stable(name, "stale-generation-rejected");
    const authorization_id = vopr.id.stable(name, "cross-table-authorization");
    const service_rate_id = vopr.id.stable(name, "service-rate-composes-and-heals");
    const cleanup_id = vopr.id.stable(name, "resources-cleaned");
    const complete_id = vopr.id.stable(name, "history-completes");

    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = result_id, .name = name ++ ".result-sound", .kind = .always },
        .{ .id = retry_id, .name = name ++ ".retry-bounded", .kind = .always },
        .{ .id = cancellation_id, .name = name ++ ".cancellation-propagates", .kind = .always },
        .{ .id = generation_id, .name = name ++ ".stale-generation-rejected", .kind = .always },
        .{ .id = authorization_id, .name = name ++ ".cross-table-authorization", .kind = .always },
        .{ .id = service_rate_id, .name = name ++ ".service-rate-composes-and-heals", .kind = .always },
        .{ .id = cleanup_id, .name = name ++ ".resources-cleaned", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".history-completes", .kind = .reachable },
    };

    const Mode = enum {
        fanout_hydrate,
        fanout_nodes_only,
        topology_retry,
        retry_exhausted,
        cancel_in_flight,
        stale_generation,
        cross_table_authorization,
        service_rate,
    };

    const mode_ids = ids: {
        var values: [@typeInfo(Mode).@"enum".fields.len]vopr.id.StableId = undefined;
        for (std.meta.tags(Mode), 0..) |mode, index|
            values[index] = vopr.id.stable(name, @tagName(mode));
        break :ids values;
    };
    const mode_names = names: {
        var values: [mode_ids.len][]const u8 = undefined;
        for (std.meta.tags(Mode), 0..) |mode, index|
            values[index] = name ++ "." ++ @tagName(mode);
        break :names values;
    };
    const cancel_id = vopr.id.stable(name, "cancel-outstanding-fanout");
    const heal_service_rate_id = vopr.id.stable(name, "heal-service-rate");
    const service_rate_fault_id = vopr.id.stable(name, "fault.group-22-slow");
    const expand_operation = vopr.service_rate.Operation.named(name ++ ".expand", 10);
    const hydrate_operation = vopr.service_rate.Operation.named(name ++ ".hydrate", 20);
    const get_edges_operation = vopr.service_rate.Operation.named(name ++ ".get-edges", 30);
    const service_nodes = [_]vopr.service_rate.Node{
        .{ .id = vopr.id.stable(name, "group.11"), .name = name ++ ".group.11" },
        .{ .id = vopr.id.stable(name, "group.22"), .name = name ++ ".group.22" },
        .{ .id = vopr.id.stable(name, "group.33"), .name = name ++ ".group.33" },
    };
    const service_operations = [_]vopr.service_rate.Operation{
        expand_operation,
        hydrate_operation,
        get_edges_operation,
    };

    const ServiceRateAdapter = struct {
        ports: [service_nodes.len]vopr.service_rate.Port,

        fn iface(self: *@This()) distributed_graph.WorkCostPort {
            return .{ .ptr = self, .charge_fn = charge };
        }

        fn charge(ptr: *anyopaque, group_id: u64, kind: distributed_graph.WorkKind, units: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const port_index: usize = switch (group_id) {
                11 => 0,
                22 => 1,
                33 => 2,
                else => return error.UnknownDistributedGraphServiceNode,
            };
            const operation_id = switch (kind) {
                .expand => expand_operation.id,
                .hydrate => hydrate_operation.id,
                .get_edges => get_edges_operation.id,
            };
            _ = try self.ports[port_index].charge(operation_id, units);
        }
    };

    const State = struct {
        owner_allocator: std.mem.Allocator,
        fixture_allocator: FixtureAllocator,
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        service_rate_model: vopr.service_rate.Model = undefined,
        service_rate_adapter: ServiceRateAdapter = undefined,
        mode: ?Mode = null,
        phase: u32 = 0,
        cancellation: std.atomic.Value(bool) = .init(false),
        expand_calls: std.atomic.Value(u32) = .init(0),
        hydrate_calls: std.atomic.Value(u32) = .init(0),
        expand_active: std.atomic.Value(u32) = .init(0),
        cancellation_selected: bool = false,
        result_sound: bool = true,
        retry_sound: bool = true,
        cancellation_sound: bool = true,
        generation_sound: bool = true,
        authorization_sound: bool = true,
        service_rate_sound: bool = true,
        slowdown_pass_complete: bool = false,
        slowdown_healed: bool = false,
        complete: bool = false,
        task_error: ?anyerror = null,

        fn init(owner_allocator: std.mem.Allocator) !*State {
            const self = try owner_allocator.create(State);
            errdefer owner_allocator.destroy(self);
            self.* = .{
                .owner_allocator = owner_allocator,
                .fixture_allocator = .init,
                .allocator = undefined,
                .sim = undefined,
            };
            errdefer _ = self.fixture_allocator.deinit();
            self.allocator = self.fixture_allocator.allocator();
            self.sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0x4451_5259,
                .instrumentation = .{ .enabled = false, .map_digest = 0x4451_5259 },
            });
            errdefer self.sim.deinit();
            self.service_rate_model = try vopr.service_rate.Model.init(
                self.allocator,
                &service_nodes,
                &service_operations,
            );
            errdefer self.service_rate_model.deinit();
            for (&self.service_rate_adapter.ports, service_nodes) |*port, node|
                port.* = try self.service_rate_model.port(self.sim.io(), node.id);
            return self;
        }

        fn deinit(self: *State) void {
            self.service_rate_model.deinit();
            self.sim.deinit();
            const owner_allocator = self.owner_allocator;
            std.debug.assert(self.fixture_allocator.deinit() == .ok);
            owner_allocator.destroy(self);
        }

        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        };
        const fanout_ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 11, .table_id = 7, .start_key = "", .end_key = "doc:m" },
            .{ .group_id = 22, .table_id = 7, .start_key = "doc:m", .end_key = null },
        };
        const retry_ranges = [_][1]metadata_table_manager.RangeRecord{
            .{.{ .group_id = 11, .table_id = 7, .start_key = "", .end_key = null }},
            .{.{ .group_id = 22, .table_id = 7, .start_key = "", .end_key = null }},
            .{.{ .group_id = 33, .table_id = 7, .start_key = "", .end_key = null }},
        };
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{ .group_id = 11 },
            .{ .group_id = 22 },
            .{ .group_id = 33 },
        };

        fn catalog(self: *State) table_catalog.CatalogSource {
            return .{ .ptr = self, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            } };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *State = @ptrCast(@alignCast(ptr));
            const ranges: []const metadata_table_manager.RangeRecord = switch (self.mode.?) {
                .fanout_hydrate, .fanout_nodes_only, .cancel_in_flight, .service_rate => fanout_ranges[0..],
                else => retry_ranges[@min(self.phase, retry_ranges.len - 1)][0..],
            };
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn worker(self: *State) distributed_graph.Worker {
            return .{
                .ptr = self,
                .work_cost_port = if (self.mode.? == .service_rate) self.service_rate_adapter.iface() else null,
                .vtable = &.{
                    .execute_graph_expand = executeGraphExpand,
                    .execute_graph_hydrate = executeGraphHydrate,
                    .fanout_io = fanoutIo,
                },
            };
        }

        fn fanoutIo(ptr: *anyopaque) ?std.Io {
            const self: *State = @ptrCast(@alignCast(ptr));
            return self.sim.io();
        }

        fn executeGraphExpand(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            _: []const u8,
            req: distributed_graph.GraphExpandRequest,
            _: @import("../raft/read_gate.zig").ReadConsistency,
        ) !distributed_graph.GraphExpandResponse {
            const self: *State = @ptrCast(@alignCast(ptr));
            _ = self.expand_calls.fetchAdd(1, .monotonic);
            const active = self.expand_active.fetchAdd(1, .monotonic) + 1;
            defer _ = self.expand_active.fetchSub(1, .monotonic);

            switch (self.mode.?) {
                .topology_retry => if (self.phase == 0) {
                    self.phase = 1;
                    return error.TopologyChanged;
                },
                .retry_exhausted => {
                    self.phase += 1;
                    return error.TopologyChanged;
                },
                .cancel_in_flight => {
                    std.debug.assert(active > 0);
                    try self.sim.io().sleep(.fromNanoseconds(20), .awake);
                },
                else => {},
            }
            if (self.cancellation.load(.monotonic)) return error.Cancelled;

            const node_key = if (group_id == 11) "doc:b" else "doc:o";
            const nodes = try alloc.alloc(graph_query.GraphResultNode, 1);
            nodes[0] = .{
                .key = try alloc.dupe(u8, node_key),
                .depth = 1,
                .distance = 1.0,
                .path = null,
                .path_edges = null,
            };
            const expansions = try alloc.alloc(distributed_graph.GraphExpansion, 1);
            expansions[0] = .{
                .frontier_id = req.frontier[0].id,
                .frontier_key = try alloc.dupe(u8, req.frontier[0].key),
                .graph_result = .{
                    .name = try alloc.dupe(u8, req.name),
                    .nodes = nodes,
                    .paths = @constCast((&[_]db_types.GraphPath{})[0..]),
                    .hits = @constCast((&[_]db_types.SearchHit{})[0..]),
                    .total_hits = 1,
                },
            };
            return .{ .expansions = expansions };
        }

        fn executeGraphHydrate(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            _: []const u8,
            req: distributed_graph.GraphHydrateRequest,
            _: @import("../raft/read_gate.zig").ReadConsistency,
        ) !distributed_graph.GraphHydrateResponse {
            const self: *State = @ptrCast(@alignCast(ptr));
            _ = self.hydrate_calls.fetchAdd(1, .monotonic);
            if (self.mode.? == .cancel_in_flight) try self.sim.io().sleep(.fromNanoseconds(20), .awake);
            if (self.cancellation.load(.monotonic)) return error.Cancelled;
            const hits = try alloc.alloc(db_types.SearchHit, req.keys.len);
            var initialized: usize = 0;
            errdefer {
                for (hits[0..initialized]) |*hit| hit.deinit(alloc);
                alloc.free(hits);
            }
            for (req.keys, 0..) |key, index| {
                hits[index] = .{
                    .id = try alloc.dupe(u8, key),
                    .stored_data = try std.fmt.allocPrint(alloc, "{{\"group\":{d}}}", .{group_id}),
                };
                initialized += 1;
            }
            return .{ .hits = hits };
        }

        fn run(self: *State) !void {
            if (self.mode.? == .cross_table_authorization) {
                try distributed_graph.testCrossTableHydrateAppliesTargetAuthorizationAndClearsOrdinals(self.allocator);
                self.authorization_sound = true;
                return;
            }

            const expand_before = self.expand_calls.load(.monotonic);
            const hydrate_before = self.hydrate_calls.load(.monotonic);
            const group_11_usage_before: ?vopr.service_rate.Usage = if (self.mode.? == .service_rate)
                try self.service_rate_model.nodeUsage(service_nodes[0].id)
            else
                null;
            const group_22_usage_before: ?vopr.service_rate.Usage = if (self.mode.? == .service_rate)
                try self.service_rate_model.nodeUsage(service_nodes[1].id)
            else
                null;
            const start_nodes = if (self.mode.? == .fanout_hydrate or self.mode.? == .fanout_nodes_only or self.mode.? == .cancel_in_flight or self.mode.? == .service_rate)
                &[_][]const u8{ "doc:a", "doc:n" }
            else
                &[_][]const u8{"doc:a"};
            const tokens = [_]db_types.ShardIdentityReadGeneration{.{ .group_id = 99, .generation = 77 }};
            const req = db_types.SearchRequest{
                .graph_queries = &.{.{
                    .name = "walk",
                    .query = .{
                        .query_type = .neighbors,
                        .index_name = "graph_idx",
                        .start_nodes = .{ .keys = start_nodes },
                        .params = .{},
                        .include_documents = self.mode.? != .fanout_nodes_only,
                    },
                }},
                .identity_read_generation = if (self.mode.? == .stale_generation) null else 77,
                .cancellation = CancellationToken.fromAtomic(&self.cancellation),
            };
            const base_result = db_types.SearchResult{
                .alloc = self.allocator,
                .hits = @constCast((&[_]db_types.SearchHit{})[0..]),
                .total_hits = 0,
                .graph_results = @constCast((&[_]db_types.GraphSearchResult{})[0..]),
                .shard_identity_read_generations = if (self.mode.? == .stale_generation) @constCast(tokens[0..]) else @constCast((&[_]db_types.ShardIdentityReadGeneration{})[0..]),
            };

            const results = distributed_graph.executeCrossRange(
                self.allocator,
                self.catalog(),
                self.worker(),
                "docs",
                req,
                base_result,
                .read_index,
            ) catch |err| {
                switch (self.mode.?) {
                    .retry_exhausted => self.retry_sound = err == error.TopologyChanged and self.expand_calls.load(.monotonic) == 2,
                    .cancel_in_flight => self.cancellation_sound = err == error.Cancelled and self.cancellation_selected,
                    .stale_generation => self.generation_sound = err == error.TopologyChanged and self.expand_calls.load(.monotonic) == 0,
                    else => return err,
                }
                return;
            };
            defer {
                for (results) |*result| result.deinit(self.allocator);
                self.allocator.free(results);
            }
            const query_sound = switch (self.mode.?) {
                .fanout_hydrate => self.expand_calls.load(.monotonic) == 2 and
                    self.hydrate_calls.load(.monotonic) == 2 and results.len == 1 and
                    results[0].nodes.len == 2 and results[0].hits.len == 2,
                .fanout_nodes_only => self.expand_calls.load(.monotonic) == 2 and
                    self.hydrate_calls.load(.monotonic) == 0 and results.len == 1 and
                    results[0].nodes.len == 2 and results[0].hits.len == 0,
                .topology_retry => self.expand_calls.load(.monotonic) == 2 and
                    self.hydrate_calls.load(.monotonic) == 1 and results.len == 1,
                .service_rate => self.expand_calls.load(.monotonic) - expand_before == 2 and
                    self.hydrate_calls.load(.monotonic) - hydrate_before == 2 and
                    results.len == 1 and results[0].nodes.len == 2 and results[0].hits.len == 2,
                else => false,
            };
            self.result_sound = self.result_sound and query_sound;
            if (self.mode.? == .service_rate) {
                const group_11_usage_after = try self.service_rate_model.nodeUsage(service_nodes[0].id);
                const group_22_usage_after = try self.service_rate_model.nodeUsage(service_nodes[1].id);
                const group_11_operations = group_11_usage_after.operations - group_11_usage_before.?.operations;
                const group_22_operations = group_22_usage_after.operations - group_22_usage_before.?.operations;
                const group_11_charged_ns = group_11_usage_after.charged_ns - group_11_usage_before.?.charged_ns;
                const group_22_charged_ns = group_22_usage_after.charged_ns - group_22_usage_before.?.charged_ns;
                const expected_group_22_charged_ns: u64 = if (self.slowdown_healed) 30 else 120;
                self.service_rate_sound = self.service_rate_sound and
                    group_11_operations == 2 and group_22_operations == 2 and
                    group_11_charged_ns == 30 and
                    group_22_charged_ns == expected_group_22_charged_ns;
            }
            self.retry_sound = self.mode.? != .topology_retry or self.phase == 1;
        }

        fn runTask(self: *State) void {
            self.run() catch |err| {
                self.task_error = err;
            };
            if (self.mode.? == .service_rate and !self.slowdown_healed)
                self.slowdown_pass_complete = true
            else
                self.complete = true;
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
                .kind = switch (mode) {
                    .fanout_hydrate, .fanout_nodes_only, .topology_retry, .cross_table_authorization => .workload,
                    else => .fault,
                },
                .fault_phase = if (mode == .service_rate) .start else null,
            });
            return;
        }
        if (state.mode.? == .cancel_in_flight and !state.cancellation_selected and state.expand_active.load(.monotonic) > 0)
            try list.append(allocator, .{ .id = cancel_id, .name = name ++ ".cancel-outstanding-fanout", .kind = .fault });
        if (state.mode.? == .service_rate and state.slowdown_pass_complete and !state.slowdown_healed)
            try list.append(allocator, .{ .id = heal_service_rate_id, .name = name ++ ".heal-service-rate", .kind = .fault, .fault_phase = .end });
        if (!state.sim.scheduler().quiescent()) try state.sim.scheduler().enumerateReady(list, allocator);
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (state.mode == null) {
            var found = false;
            inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
                state.mode = mode;
                if (mode == .service_rate) try state.service_rate_model.activate(.{
                    .fault_id = service_rate_fault_id,
                    .node_id = service_nodes[1].id,
                    .multiplier_ppm = 4 * vopr.service_rate.parts_per_million,
                });
                _ = state.sim.io().async(State.runTask, .{state});
                found = true;
            };
            if (!found) return error.InvalidDistributedQueryMode;
        } else if (selected.id == cancel_id) {
            state.cancellation_selected = true;
            state.cancellation.store(true, .monotonic);
        } else if (selected.id == heal_service_rate_id) {
            try state.service_rate_model.heal(service_rate_fault_id);
            state.slowdown_healed = true;
            _ = state.sim.io().async(State.runTask, .{state});
        } else {
            try state.sim.scheduler().executeReady(selected.id, events, allocator);
        }
        try events.emitNamed(allocator, .domain, selected.name, state.expand_calls.load(.monotonic));
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, name ++ ".expand-calls", state.expand_calls.load(.monotonic));
        try builder.addNamed(allocator, name ++ ".hydrate-calls", state.hydrate_calls.load(.monotonic));
        try builder.addNamed(allocator, name ++ ".cancelled", @intFromBool(state.cancellation_selected));
        try builder.addNamed(allocator, name ++ ".service-rate-healed", @intFromBool(state.slowdown_healed));
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(state.complete));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try sink.check(allocator, result_id, state.result_sound and state.task_error == null);
        try sink.check(allocator, retry_id, state.retry_sound);
        try sink.check(allocator, cancellation_id, state.cancellation_sound);
        try sink.check(allocator, generation_id, state.generation_sound);
        try sink.check(allocator, authorization_id, state.authorization_sound);
        try sink.check(allocator, service_rate_id, state.service_rate_sound);
        try sink.check(allocator, cleanup_id, !state.complete or state.sim.resourceSnapshot().active_tasks == 0);
        try sink.check(allocator, complete_id, state.complete);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        return state.sim.healthSnapshot(.{
            .progress_expected = state.mode != null,
            .progress_units = state.expand_calls.load(.monotonic) + state.hydrate_calls.load(.monotonic),
            .recovery_expected = state.mode != null and switch (state.mode.?) {
                .topology_retry, .retry_exhausted, .cancel_in_flight, .stale_generation, .service_rate => true,
                else => false,
            },
            .recovery_complete = state.complete,
            .consistency_valid = state.result_sound and state.retry_sound and state.cancellation_sound and state.generation_sound and state.authorization_sound and state.service_rate_sound,
            .cleanup_complete = state.complete and state.sim.resourceSnapshot().active_tasks == 0,
        });
    }

    pub fn done(world: *World) bool {
        return world.state.complete and world.state.sim.scheduler().quiescent();
    }
};

test "distributed query VOPR exact replays fanout topology snapshots cancellation and authorization" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids, 0..) |mode_id, ordinal| {
        var choices = vopr.choice.PrefixedFairSeeded.init(&.{mode_id}, 0x4451_5259 + ordinal);
        var recorded = try vopr.runner.run(Scenario, std.testing.allocator, choices.source(), .{
            .system = "antfly",
            .transition_budget = 2_000,
            .backend_ids = &backend_ids,
            .source_revision = "distributed-query-vopr-v3",
            .target = "native",
            .optimize = @tagName(@import("builtin").mode),
        });
        defer recorded.deinit();
        try std.testing.expectEqual(@as(u64, 0), recorded.summary.?.property_failures);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
        replayed.deinit();
    }
}
