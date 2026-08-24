// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Replayable vector/text/graph/global-query assembly. Component completions
//! are VOPR transitions; final publication executes the production distributed
//! result merge and graph-union implementation.

const std = @import("std");
const vopr = @import("vopr");
const api_operation = @import("../api/operation.zig");
const query_api = @import("../api/query.zig");
const request_admission = @import("../common/request_admission.zig");
const db_types = @import("../storage/db/types.zig");
const graph_exec = @import("../storage/db/query/graph_exec.zig");

pub const Scenario = struct {
    pub const name: []const u8 = "composed-query-lifecycle";
    pub const version: u32 = 1;

    const no_partial_id = vopr.id.stable(name, "no-partial-publication");
    const cancel_id = vopr.id.stable(name, "cancellation-before-publication");
    const pressure_id = vopr.id.stable(name, "pressure-releases-admission");
    const result_id = vopr.id.stable(name, "canonical-composed-result");
    const graph_id = vopr.id.stable(name, "graph-result-preserved");
    const complete_id = vopr.id.stable(name, "lifecycle-completes");

    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = no_partial_id, .name = name ++ ".no-partial-publication", .kind = .always },
        .{ .id = cancel_id, .name = name ++ ".cancellation-before-publication", .kind = .always },
        .{ .id = pressure_id, .name = name ++ ".pressure-releases-admission", .kind = .always },
        .{ .id = result_id, .name = name ++ ".canonical-composed-result", .kind = .always },
        .{ .id = graph_id, .name = name ++ ".graph-result-preserved", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".lifecycle-completes", .kind = .reachable },
    };

    const Mode = enum { normal, partial_failure, cancellation, pressure };

    const select_normal_id = vopr.id.stable(name, "select-normal");
    const select_partial_id = vopr.id.stable(name, "select-partial-failure");
    const select_cancel_id = vopr.id.stable(name, "select-cancellation");
    const select_pressure_id = vopr.id.stable(name, "select-pressure");
    const text_id = vopr.id.stable(name, "text-complete");
    const vector_id = vopr.id.stable(name, "vector-complete");
    const graph_complete_id = vopr.id.stable(name, "graph-complete");
    const graph_fail_id = vopr.id.stable(name, "graph-partial-failure");
    const graph_retry_id = vopr.id.stable(name, "graph-retry");
    const cancel_transition_id = vopr.id.stable(name, "cancel");
    const release_pressure_id = vopr.id.stable(name, "release-pressure");
    const assemble_id = vopr.id.stable(name, "assemble-global-result");

    const State = struct {
        allocator: std.mem.Allocator,
        mode: ?Mode = null,
        text_complete: bool = false,
        vector_complete: bool = false,
        graph_complete: bool = false,
        graph_failed: bool = false,
        cancelled: bool = false,
        published: bool = false,
        done: bool = false,
        partial_published: bool = false,
        cancelled_published: bool = false,
        pressure_observed: bool = false,
        canonical_result: bool = true,
        graph_preserved: bool = true,
        admission: request_admission.RequestAdmission,
        pressure_lease: ?request_admission.RequestAdmission.Lease = null,

        fn cancellationToken(self: *@This()) api_operation.CancellationToken {
            return .{ .ptr = self, .is_cancelled_fn = isCancelled };
        }

        fn isCancelled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.cancelled;
        }

        fn requestContext(self: *@This()) api_operation.RequestContext {
            return .{ .cancellation = self.cancellationToken(), .request_id = "vopr-composed-query" };
        }

        fn makeHit(self: *@This(), id_text: []const u8, score: f32, distance: ?f32) !db_types.SearchHit {
            return .{
                .id = try self.allocator.dupe(u8, id_text),
                .score = score,
                .distance = distance,
            };
        }

        fn makeInputs(self: *@This()) ![2]db_types.SearchResult {
            const text_hits = try self.allocator.alloc(db_types.SearchHit, 1);
            errdefer self.allocator.free(text_hits);
            text_hits[0] = try self.makeHit("doc:text", 0.60, null);
            errdefer text_hits[0].deinit(self.allocator);

            const vector_hits = try self.allocator.alloc(db_types.SearchHit, 1);
            errdefer self.allocator.free(vector_hits);
            vector_hits[0] = try self.makeHit("doc:vector", 0.90, 0.10);
            errdefer vector_hits[0].deinit(self.allocator);

            const graph_results = try self.allocator.alloc(db_types.GraphSearchResult, 1);
            errdefer self.allocator.free(graph_results);
            const graph_hits = try self.allocator.alloc(db_types.SearchHit, 1);
            errdefer self.allocator.free(graph_hits);
            graph_hits[0] = try self.makeHit("doc:graph", 0.70, null);
            errdefer graph_hits[0].deinit(self.allocator);
            graph_results[0] = .{
                .name = try self.allocator.dupe(u8, "neighbors"),
                .hits = graph_hits,
                .total_hits = 1,
            };

            return .{
                .{ .alloc = self.allocator, .hits = text_hits, .total_hits = 1 },
                .{ .alloc = self.allocator, .hits = vector_hits, .total_hits = 1, .graph_results = graph_results },
            };
        }

        fn containsHit(hits: []const db_types.SearchHit, wanted: []const u8) bool {
            for (hits) |hit| if (std.mem.eql(u8, hit.id, wanted)) return true;
            return false;
        }

        fn assemble(self: *@This()) !void {
            if (!self.text_complete or !self.vector_complete or !self.graph_complete) {
                self.partial_published = true;
                return error.QueryComponentsIncomplete;
            }
            self.requestContext().ensureActive() catch |err| {
                if (err == error.Canceled) self.cancelled_published = self.published;
                return err;
            };
            var lease = self.admission.tryAcquireLease() orelse {
                self.pressure_observed = true;
                return;
            };
            defer lease.release();

            var inputs = try self.makeInputs();
            defer for (&inputs) |*input| input.deinit();
            var merged = try query_api.mergeSearchResults(
                self.allocator,
                .{
                    .full_text = .{ .match_all = {} },
                    .dense = .{ .vector = @constCast(&[_]f32{ 1.0, 0.0 }), .k = 3 },
                    .limit = 3,
                },
                &inputs,
                0,
                3,
            );
            defer merged.deinit();
            self.graph_preserved = merged.graph_results.len == 1 and
                std.mem.eql(u8, merged.graph_results[0].name, "neighbors");
            try graph_exec.applyGraphUnion(self.allocator, &merged);
            self.canonical_result = merged.hits.len == 3 and
                containsHit(merged.hits, "doc:text") and
                containsHit(merged.hits, "doc:vector") and
                containsHit(merged.hits, "doc:graph");
            self.published = true;
            self.done = true;
        }

        fn cancel(self: *@This()) !void {
            self.cancelled = true;
            self.requestContext().ensureActive() catch |err| {
                if (err != error.Canceled) return err;
                self.cancelled_published = self.published;
                if (self.pressure_lease) |*lease| lease.release();
                self.pressure_lease = null;
                self.done = true;
                return;
            };
            return error.CancellationNotObserved;
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        state.* = .{
            .allocator = allocator,
            .admission = request_admission.RequestAdmission.init(1),
        };
        return .{ .state = state };
    }

    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        if (world.state.pressure_lease) |*lease| lease.release();
        allocator.destroy(world.state);
        world.* = undefined;
    }

    fn append(list: *vopr.transition.List, allocator: std.mem.Allocator, id: u64, transition_name: []const u8, kind: vopr.transition.Kind) !void {
        try list.append(allocator, .{ .id = id, .name = transition_name, .kind = kind });
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        const state = world.state;
        if (state.done) return;
        if (state.mode == null) {
            try append(list, allocator, select_normal_id, name ++ ".select-normal", .workload);
            try append(list, allocator, select_partial_id, name ++ ".select-partial-failure", .fault);
            try append(list, allocator, select_cancel_id, name ++ ".select-cancellation", .fault);
            try append(list, allocator, select_pressure_id, name ++ ".select-pressure", .fault);
            return;
        }
        if (!state.text_complete) try append(list, allocator, text_id, name ++ ".text-complete", .workload);
        if (!state.vector_complete) try append(list, allocator, vector_id, name ++ ".vector-complete", .workload);
        if (!state.graph_complete) {
            if (state.mode == .partial_failure and !state.graph_failed)
                try append(list, allocator, graph_fail_id, name ++ ".graph-partial-failure", .fault)
            else if (state.mode == .partial_failure)
                try append(list, allocator, graph_retry_id, name ++ ".graph-retry", .workload)
            else
                try append(list, allocator, graph_complete_id, name ++ ".graph-complete", .workload);
        }
        if (state.mode == .cancellation) try append(list, allocator, cancel_transition_id, name ++ ".cancel", .fault);
        if (state.text_complete and state.vector_complete and state.graph_complete)
            try append(list, allocator, assemble_id, name ++ ".assemble-global-result", .workload);
        if (state.mode == .pressure and state.pressure_observed)
            try append(list, allocator, release_pressure_id, name ++ ".release-pressure", .maintenance);
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (selected.id == select_normal_id) state.mode = .normal else if (selected.id == select_partial_id) state.mode = .partial_failure else if (selected.id == select_cancel_id) state.mode = .cancellation else if (selected.id == select_pressure_id) {
            state.mode = .pressure;
            state.pressure_lease = state.admission.tryAcquireLease() orelse return error.UnexpectedPressureAdmissionFailure;
        } else if (selected.id == text_id) state.text_complete = true else if (selected.id == vector_id) state.vector_complete = true else if (selected.id == graph_complete_id) state.graph_complete = true else if (selected.id == graph_fail_id) state.graph_failed = true else if (selected.id == graph_retry_id) state.graph_complete = true else if (selected.id == cancel_transition_id) try state.cancel() else if (selected.id == release_pressure_id) {
            if (state.pressure_lease) |*lease| lease.release();
            state.pressure_lease = null;
        } else if (selected.id == assemble_id) try state.assemble() else return error.InvalidComposedQueryTransition;
        try events.emitNamed(allocator, .domain, selected.name, @intFromBool(state.published));
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, name ++ ".components", @as(u64, @intFromBool(state.text_complete)) |
            (@as(u64, @intFromBool(state.vector_complete)) << 1) |
            (@as(u64, @intFromBool(state.graph_complete)) << 2));
        try builder.addNamed(allocator, name ++ ".published", @intFromBool(state.published));
        try builder.addNamed(allocator, name ++ ".cancelled", @intFromBool(state.cancelled));
        try builder.addNamed(allocator, name ++ ".in-flight", state.admission.stats().in_flight);
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try sink.check(allocator, no_partial_id, !state.partial_published);
        try sink.check(allocator, cancel_id, !state.cancelled_published);
        try sink.check(allocator, pressure_id, state.admission.stats().in_flight <= 1 and
            (!state.done or state.admission.stats().in_flight == 0));
        try sink.check(allocator, result_id, state.canonical_result);
        try sink.check(allocator, graph_id, state.graph_preserved);
        try sink.check(allocator, complete_id, state.done);
    }

    pub fn done(world: *World) bool {
        return world.state.done;
    }
};

fn recordAndReplay(selections: []const u64) !void {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    var scripted = vopr.choice.Scripted{ .selections = selections };
    var recorded = try vopr.runner.run(Scenario, std.testing.allocator, scripted.source(), .{
        .system = "antfly",
        .transition_budget = selections.len,
        .backend_ids = &backend_ids,
        .source_revision = "composed-query-vopr-v1",
        .target = "native",
        .optimize = @tagName(@import("builtin").mode),
    });
    defer recorded.deinit();
    try std.testing.expectEqual(@as(u64, 0), recorded.summary.property_failures);
    for (0..3) |_| {
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
        replayed.deinit();
    }
}

test "composed query lifecycle VOPR exact replays vector text graph and global assembly" {
    const orders = [_][3]u64{
        .{ Scenario.text_id, Scenario.vector_id, Scenario.graph_complete_id },
        .{ Scenario.text_id, Scenario.graph_complete_id, Scenario.vector_id },
        .{ Scenario.vector_id, Scenario.text_id, Scenario.graph_complete_id },
        .{ Scenario.vector_id, Scenario.graph_complete_id, Scenario.text_id },
        .{ Scenario.graph_complete_id, Scenario.text_id, Scenario.vector_id },
        .{ Scenario.graph_complete_id, Scenario.vector_id, Scenario.text_id },
    };
    for (orders) |order| try recordAndReplay(&.{ Scenario.select_normal_id, order[0], order[1], order[2], Scenario.assemble_id });
    try recordAndReplay(&.{ Scenario.select_partial_id, Scenario.text_id, Scenario.vector_id, Scenario.graph_fail_id, Scenario.graph_retry_id, Scenario.assemble_id });
    try recordAndReplay(&.{ Scenario.select_cancel_id, Scenario.vector_id, Scenario.cancel_transition_id });
    try recordAndReplay(&.{ Scenario.select_cancel_id, Scenario.text_id, Scenario.vector_id, Scenario.graph_complete_id, Scenario.cancel_transition_id });
    try recordAndReplay(&.{ Scenario.select_pressure_id, Scenario.text_id, Scenario.vector_id, Scenario.graph_complete_id, Scenario.assemble_id, Scenario.release_pressure_id, Scenario.assemble_id });
}
