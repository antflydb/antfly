// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Deterministic, semantic failure slices. This is intentionally not a claim
//! of proof-level causality: it identifies the smallest stable neighborhood
//! available from the canonical actor/resource/fault/event relationships and
//! gives humans a reproducible place to start.

const std = @import("std");
const choice = @import("choice.zig");
const ids = @import("id.zig");
const replay = @import("replay.zig");
const runner = @import("runner.zig");
const trace = @import("trace.zig");

pub const Role = enum {
    dependency,
    active_fault,
    client_outcome,
    failure_boundary,
};

pub const Item = struct {
    index: u64,
    transition_id: u64,
    name: []const u8,
    role: Role,
    actor_id: ?u64,
    resource_id: ?u64,
};

pub const Report = struct {
    allocator: std.mem.Allocator,
    failure_fingerprint: u64,
    failure_identity: []const u8,
    failure_index: u64,
    items: []Item,

    pub fn deinit(self: *Report) void {
        self.allocator.free(self.items);
        self.* = undefined;
    }

    pub fn renderAlloc(self: *const Report, allocator: std.mem.Allocator) ![]u8 {
        return try std.json.Stringify.valueAlloc(allocator, .{
            .failure_fingerprint = self.failure_fingerprint,
            .failure_identity = self.failure_identity,
            .failure_index = self.failure_index,
            .causes = self.items,
        }, .{ .whitespace = .indent_2 });
    }
};

pub const CounterfactualConfig = struct {
    prefix_window: usize = 16,
    descendants_per_alternative: usize = 4,
    suffix_seed: u64 = 0,
};

pub const Experiment = struct {
    choice_index: usize,
    original_id: ids.StableId,
    replacement_id: ids.StableId,
    trials: usize,
    same_failure_trials: usize,
    child_artifact_digest: u64,
};

pub const CounterfactualReport = struct {
    allocator: std.mem.Allocator,
    failure_fingerprint: u64,
    config: CounterfactualConfig,
    experiments: []Experiment,

    pub fn deinit(self: *CounterfactualReport) void {
        self.allocator.free(self.experiments);
        self.* = undefined;
    }

    pub fn renderAlloc(self: *const CounterfactualReport, allocator: std.mem.Allocator) ![]u8 {
        return std.json.Stringify.valueAlloc(allocator, .{
            .failure_fingerprint = self.failure_fingerprint,
            .config = self.config,
            .experiments = self.experiments,
        }, .{ .whitespace = .indent_2 });
    }
};

/// Replay bounded alternate decisions before a failure. Every child is exact
/// replayed from a clean world before it contributes to the report.
pub fn analyzeCounterfactual(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    artifact: *const trace.Trace,
    failure_ordinal: usize,
    config: CounterfactualConfig,
) !CounterfactualReport {
    try artifact.validate();
    if (failure_ordinal >= artifact.failures.items.len) return error.FailureOrdinalOutOfRange;
    if (config.descendants_per_alternative == 0) return error.InvalidCounterfactualDescendantBudget;
    const failure = artifact.failures.items[failure_ordinal];
    const failure_choice_end = @min(artifact.choices.items.len, std.math.cast(usize, failure.index) orelse artifact.choices.items.len);
    const choice_start = failure_choice_end -| config.prefix_window;
    var experiments: std.ArrayListUnmanaged(Experiment) = .empty;
    errdefer experiments.deinit(allocator);
    for (artifact.choices.items[choice_start..failure_choice_end], choice_start..) |record, choice_index| {
        for (record.enabled_ids) |replacement_id| {
            if (replacement_id == record.selected_id) continue;
            var experiment: Experiment = .{
                .choice_index = choice_index,
                .original_id = record.selected_id,
                .replacement_id = replacement_id,
                .trials = config.descendants_per_alternative,
                .same_failure_trials = 0,
                .child_artifact_digest = 0,
            };
            for (0..config.descendants_per_alternative) |trial| {
                const seed = ids.derive(
                    "counterfactual-suffix",
                    config.suffix_seed ^ replacement_id,
                    @as(u64, @intCast(choice_index)) ^ @as(u64, @intCast(trial)),
                );
                var source = choice.Mutating.init(artifact.choices.items, choice_index, replacement_id, seed);
                var child = try runner.run(Scenario, allocator, source.source(), runnerConfigFromArtifact(artifact));
                defer child.deinit();
                var replayed = try replay.exact(Scenario, allocator, &child);
                replayed.deinit();
                for (child.failures.items) |child_failure| {
                    if (child_failure.fingerprint == failure.fingerprint) {
                        experiment.same_failure_trials += 1;
                        break;
                    }
                }
                const encoded = try child.renderAlloc(allocator);
                defer allocator.free(encoded);
                experiment.child_artifact_digest = ids.derive(
                    "counterfactual-child-set",
                    experiment.child_artifact_digest,
                    ids.digest(encoded),
                );
            }
            try experiments.append(allocator, experiment);
        }
    }
    return .{
        .allocator = allocator,
        .failure_fingerprint = failure.fingerprint,
        .config = config,
        .experiments = try experiments.toOwnedSlice(allocator),
    };
}

fn runnerConfigFromArtifact(artifact: *const trace.Trace) runner.Config {
    return .{
        .system = artifact.header.system,
        .seed = artifact.config.seed,
        .transition_budget = artifact.config.transition_budget,
        .resource_budget = artifact.config.resource_budget,
        .fixture_hashes = artifact.config.fixture_hashes,
        .feature_flags = artifact.config.feature_flags,
        .backend_ids = artifact.config.backend_ids,
        .scenario_parameters = artifact.config.scenario_parameters,
        .source_revision = artifact.header.source_revision,
        .target = artifact.header.target,
        .optimize = artifact.header.optimize,
    };
}

pub fn analyzeAlloc(
    allocator: std.mem.Allocator,
    artifact: *const trace.Trace,
    failure_ordinal: usize,
) !Report {
    try artifact.validate();
    if (failure_ordinal >= artifact.failures.items.len) return error.FailureOrdinalOutOfRange;
    const failure = artifact.failures.items[failure_ordinal];
    const selected = try allocator.alloc(bool, artifact.transitions.items.len);
    defer allocator.free(selected);
    @memset(selected, false);

    var actor_frontier: [16]u64 = undefined;
    var actor_count: usize = 0;
    var resource_frontier: [16]u64 = undefined;
    var resource_count: usize = 0;
    const roles = try allocator.alloc(Role, artifact.transitions.items.len);
    defer allocator.free(roles);

    if (failure.index > 0 and failure.index <= artifact.transitions.items.len) {
        const boundary_index: usize = @intCast(failure.index - 1);
        select(selected, roles, boundary_index, .failure_boundary);
        addFrontier(&actor_frontier, &actor_count, artifact.transitions.items[boundary_index].actor_id);
        addFrontier(&resource_frontier, &resource_count, artifact.transitions.items[boundary_index].resource_id);
    }

    // Fault records already encode lifecycle semantics. Keep every pulse in
    // the immediate causal window and every start that remains active at the
    // failure boundary.
    for (artifact.faults.items, 0..) |fault, fault_index| {
        if (fault.index > failure.index) break;
        const active = fault.phase == .start and !hasLaterFaultEnd(artifact.faults.items, fault_index, failure.index, fault.id);
        const nearby = failure.index - fault.index <= 8;
        if (!active and !nearby) continue;
        const transition_index: usize = @intCast(fault.index - 1);
        select(selected, roles, transition_index, .active_fault);
        addFrontier(&actor_frontier, &actor_count, artifact.transitions.items[transition_index].actor_id);
        addFrontier(&resource_frontier, &resource_count, artifact.transitions.items[transition_index].resource_id);
    }

    // Public outcomes and injected errors are high-value causal landmarks.
    for (artifact.events.items) |event| {
        if (event.index > failure.index) break;
        if (event.kind != .client_response and event.kind != .injected_error) continue;
        if (failure.index - event.index > 8) continue;
        const transition_index: usize = @intCast(event.index - 1);
        select(selected, roles, transition_index, .client_outcome);
        addFrontier(&actor_frontier, &actor_count, event.actor_id);
        addFrontier(&resource_frontier, &resource_count, event.resource_id);
    }

    var reverse_index: usize = @intCast(@min(failure.index, @as(u64, @intCast(artifact.transitions.items.len))));
    while (reverse_index > 0) {
        reverse_index -= 1;
        const transition_record = artifact.transitions.items[reverse_index];
        if (!intersects(actor_frontier[0..actor_count], transition_record.actor_id) and
            !intersects(resource_frontier[0..resource_count], transition_record.resource_id)) continue;
        select(selected, roles, reverse_index, .dependency);
        addFrontier(&actor_frontier, &actor_count, transition_record.actor_id);
        addFrontier(&resource_frontier, &resource_count, transition_record.resource_id);
    }

    var item_count: usize = 0;
    for (selected) |is_selected| item_count += @intFromBool(is_selected);
    const items = try allocator.alloc(Item, item_count);
    errdefer allocator.free(items);
    var output_index: usize = 0;
    for (selected, artifact.transitions.items, 0..) |is_selected, transition_record, index| {
        if (!is_selected) continue;
        items[output_index] = .{
            .index = transition_record.index,
            .transition_id = transition_record.id,
            .name = transition_record.name,
            .role = roles[index],
            .actor_id = transition_record.actor_id,
            .resource_id = transition_record.resource_id,
        };
        output_index += 1;
    }
    return .{
        .allocator = allocator,
        .failure_fingerprint = failure.fingerprint,
        .failure_identity = failure.identity,
        .failure_index = failure.index,
        .items = items,
    };
}

fn select(selected: []bool, roles: []Role, index: usize, role: Role) void {
    if (!selected[index] or @intFromEnum(role) > @intFromEnum(roles[index])) roles[index] = role;
    selected[index] = true;
}

fn addFrontier(frontier: *[16]u64, count: *usize, value: ?u64) void {
    const id = value orelse return;
    for (frontier[0..count.*]) |existing| if (existing == id) return;
    if (count.* == frontier.len) return;
    frontier[count.*] = id;
    count.* += 1;
}

fn intersects(frontier: []const u64, value: ?u64) bool {
    const id = value orelse return false;
    return std.mem.indexOfScalar(u64, frontier, id) != null;
}

fn hasLaterFaultEnd(faults: []const trace.FaultRecord, start_index: usize, failure_index: u64, id: u64) bool {
    for (faults[start_index + 1 ..]) |fault| {
        if (fault.index > failure_index) break;
        if (fault.id == id and fault.phase == .end) return true;
    }
    return false;
}

test "causal report retains active faults outcomes and shared resources" {
    const observation = @import("observation.zig");
    var artifact = try trace.Trace.init(std.testing.allocator, .{ .scenario = "causal", .scenario_version = 1 }, .{ .transition_budget = 3 });
    defer artifact.deinit();
    for (0..3) |index| {
        try artifact.addChoice(.{ .site_id = 1, .site_name = "site", .occurrence = index, .enabled_ids = &.{10}, .selected_id = 10 });
    }
    try artifact.addTransition(.{ .index = 1, .id = 10, .name = "partition", .kind = .fault, .actor_id = 1, .resource_id = 2 });
    try artifact.addTransition(.{ .index = 2, .id = 11, .name = "write", .kind = .workload, .actor_id = 1, .resource_id = 7 });
    try artifact.addTransition(.{ .index = 3, .id = 12, .name = "check", .kind = .maintenance, .resource_id = 7 });
    try artifact.addFault(.{ .index = 1, .id = 10, .name = "partition", .phase = .start });
    try artifact.addEvent(.{ .index = 2, .ordinal = 0, .id = 21, .name = "write-rejected", .kind = .client_response, .actor_id = 1, .resource_id = 7, .payload_digest = 0 });
    const digest = observation.digestFeatures(&.{});
    for (0..4) |index| try artifact.addObservation(.{ .index = index, .digest = digest, .features = &.{} });
    try artifact.addFailure(.{ .index = 3, .class = .property, .property_id = 30, .identity = "broken", .fingerprint = 31, .observation_digest = digest });
    artifact.summary = .{ .transitions = 3, .final_observation_digest = digest, .property_failures = 1 };

    var report = try analyzeAlloc(std.testing.allocator, &artifact, 0);
    defer report.deinit();
    try std.testing.expectEqual(@as(usize, 3), report.items.len);
    try std.testing.expectEqual(Role.active_fault, report.items[0].role);
    try std.testing.expectEqual(Role.client_outcome, report.items[1].role);
    try std.testing.expectEqual(Role.failure_boundary, report.items[2].role);
    const encoded = try report.renderAlloc(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"failure_identity\": \"broken\"") != null);
}

test "counterfactual report exact replays alternate descendants" {
    const transition = @import("transition.zig");
    const event = @import("event.zig");
    const observation = @import("observation.zig");
    const outcome = @import("outcome.zig");
    const property = @import("property.zig");
    const Scenario = struct {
        const good_id = ids.stable("transition", "counterfactual.good");
        const bad_id = ids.stable("transition", "counterfactual.bad");
        const property_id = ids.stable("property", "counterfactual.safe");
        pub const name: []const u8 = "counterfactual-test";
        pub const version: u32 = 1;
        pub const properties = &[_]property.Declaration{.{ .id = property_id, .name = "counterfactual.safe", .kind = .always }};
        pub const World = struct { done: bool = false, bad: bool = false };
        pub fn init(_: std.mem.Allocator) !World {
            return .{};
        }
        pub fn deinit(_: *World, _: std.mem.Allocator) void {}
        pub fn enumerate(world: *World, list: *transition.List, allocator: std.mem.Allocator) !void {
            if (world.done) return;
            try list.append(allocator, .{ .id = good_id, .name = "counterfactual.good", .kind = .workload });
            try list.append(allocator, .{ .id = bad_id, .name = "counterfactual.bad", .kind = .fault });
        }
        pub fn execute(world: *World, selected: transition.Transition, _: *event.Sink, _: std.mem.Allocator) !outcome.TransitionOutcome {
            world.done = true;
            world.bad = selected.id == bad_id;
            return .applied();
        }
        pub fn observe(world: *World, builder: *observation.Builder, allocator: std.mem.Allocator) !void {
            try builder.addNamed(allocator, "bad", @intFromBool(world.bad));
        }
        pub fn evaluate(world: *World, sink: *property.Sink, allocator: std.mem.Allocator) !void {
            try sink.check(allocator, property_id, !world.bad);
        }
        pub fn done(world: *World) bool {
            return world.done;
        }
    };
    var script = choice.Scripted{ .selections = &.{Scenario.bad_id} };
    var artifact = try runner.run(Scenario, std.testing.allocator, script.source(), .{ .transition_budget = 1 });
    defer artifact.deinit();
    try std.testing.expectEqual(@as(usize, 1), artifact.failures.items.len);
    var report = try analyzeCounterfactual(Scenario, std.testing.allocator, &artifact, 0, .{
        .prefix_window = 1,
        .descendants_per_alternative = 2,
        .suffix_seed = 7,
    });
    defer report.deinit();
    try std.testing.expectEqual(@as(usize, 1), report.experiments.len);
    try std.testing.expectEqual(Scenario.good_id, report.experiments[0].replacement_id);
    try std.testing.expectEqual(@as(usize, 0), report.experiments[0].same_failure_trials);
    try std.testing.expect(report.experiments[0].child_artifact_digest != 0);
}
