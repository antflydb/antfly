// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const choice = @import("choice.zig");
const event = @import("event.zig");
const ids = @import("id.zig");
const observation = @import("observation.zig");
const property = @import("property.zig");
const scenario_contract = @import("scenario.zig");
const scheduler = @import("scheduler.zig");
const trace = @import("trace.zig");
const transition = @import("transition.zig");

pub const Config = struct {
    system: []const u8 = "generic",
    seed: ?u64 = null,
    transition_budget: u64,
    resource_budget: u64 = std.math.maxInt(u64),
    fixture_hashes: []const u64 = &.{},
    feature_flags: []const ids.StableId = &.{},
    backend_ids: []const ids.StableId = &.{},
    source_revision: []const u8 = "unknown",
    target: []const u8 = "native",
    optimize: []const u8 = "unknown",
};

const PropertyFailure = struct {
    declaration: property.Declaration,
    transition_index: u64,
};

pub fn run(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    choice_source: choice.Source,
    config: Config,
) !trace.Trace {
    comptime scenario_contract.assertContract(Scenario);
    if (config.transition_budget == 0) return error.InvalidTransitionBudget;

    var world = try Scenario.init(allocator);
    defer Scenario.deinit(&world, allocator);
    var tracker = try property.Tracker.init(allocator, Scenario.properties);
    defer tracker.deinit();
    var result = try trace.Trace.init(allocator, .{
        .system = config.system,
        .scenario = Scenario.name,
        .scenario_version = Scenario.version,
        .source_revision = config.source_revision,
        .target = config.target,
        .optimize = config.optimize,
    }, .{
        .seed = config.seed,
        .transition_budget = config.transition_budget,
        .resource_budget = config.resource_budget,
        .fixture_hashes = config.fixture_hashes,
        .feature_flags = config.feature_flags,
        .backend_ids = config.backend_ids,
    });
    errdefer result.deinit();

    var last_digest = try recordObservation(Scenario, allocator, &world, &result, 0);
    var transition_index: u64 = 0;
    while (!Scenario.done(&world)) {
        if (transition_index == config.transition_budget) return error.TransitionBudgetExceeded;

        var enabled = try scheduler.enumerateCanonical(Scenario, &world, allocator);
        defer enabled.deinit(allocator);
        if (enabled.items.items.len == 0) return error.ScenarioDeadlock;

        const occurrence = transition_index;
        const selected_id = try choice_source.choose(.{
            .site_id = ids.stable("choice", Scenario.name ++ ".transition"),
            .site_name = Scenario.name ++ ".transition",
            .occurrence = occurrence,
            .enabled = enabled.items.items,
        });
        const enabled_ids = try allocator.alloc(ids.StableId, enabled.items.items.len);
        defer allocator.free(enabled_ids);
        var selected: ?transition.Transition = null;
        for (enabled.items.items, 0..) |candidate, index| {
            enabled_ids[index] = candidate.id;
            if (candidate.id == selected_id) selected = candidate;
        }
        try result.addChoice(.{
            .site_id = ids.stable("choice", Scenario.name ++ ".transition"),
            .site_name = Scenario.name ++ ".transition",
            .occurrence = occurrence,
            .enabled_ids = enabled_ids,
            .selected_id = selected_id,
        });

        transition_index += 1;
        const selected_transition = selected orelse return error.ChoiceSourceSelectedDisabledAlternative;
        try scheduler.verifyStillEnabled(Scenario, &world, allocator, enabled.items.items, selected_transition);
        var events = event.Sink{};
        defer events.deinit(allocator);
        try Scenario.execute(&world, selected_transition, &events, allocator);
        try result.addTransition(.{
            .index = transition_index,
            .id = selected_transition.id,
            .name = selected_transition.name,
            .kind = selected_transition.kind,
        });
        if (selected_transition.kind == .fault) {
            try result.addFault(.{
                .index = transition_index,
                .id = selected_transition.id,
                .name = selected_transition.name,
                .phase = .pulse,
            });
        }
        for (events.events.items, 0..) |emitted, ordinal| {
            try result.addEvent(.{
                .index = transition_index,
                .ordinal = @intCast(ordinal),
                .id = emitted.id,
                .name = emitted.name,
                .kind = emitted.kind,
                .actor_id = emitted.actor_id,
                .resource_id = emitted.resource_id,
                .payload_digest = emitted.payload_digest,
            });
        }
        last_digest = try recordObservation(Scenario, allocator, &world, &result, transition_index);
        try evaluateProperties(Scenario, allocator, &world, &result, &tracker, transition_index);
        if (@hasDecl(Scenario, "quiescent") and Scenario.quiescent(&world)) tracker.beginQuiescence(transition_index);
    }
    try choice_source.finish();
    tracker.finish(transition_index);
    var property_failures: std.ArrayListUnmanaged(PropertyFailure) = .empty;
    defer property_failures.deinit(allocator);
    for (Scenario.properties, tracker.statuses) |declaration, status| {
        if (!status.failed) continue;
        try property_failures.append(allocator, .{ .declaration = declaration, .transition_index = status.first_failure_transition.? });
    }
    std.mem.sort(PropertyFailure, property_failures.items, {}, struct {
        fn lessThan(_: void, lhs: PropertyFailure, rhs: PropertyFailure) bool {
            if (lhs.transition_index != rhs.transition_index) return lhs.transition_index < rhs.transition_index;
            return ids.stable("failure", lhs.declaration.name) < ids.stable("failure", rhs.declaration.name);
        }
    }.lessThan);
    for (property_failures.items) |failure| {
        try result.addFailure(.{
            .index = failure.transition_index,
            .class = .property,
            .property_id = failure.declaration.id,
            .identity = failure.declaration.name,
            .fingerprint = ids.stable("failure", failure.declaration.name),
            .observation_digest = null,
        });
    }
    result.summary = .{
        .transitions = transition_index,
        .final_observation_digest = last_digest,
        .property_failures = @intCast(tracker.failureCount()),
    };
    try result.validate();
    return result;
}

fn recordObservation(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    world: *Scenario.World,
    result: *trace.Trace,
    index: u64,
) !u64 {
    var builder = observation.Builder{};
    defer builder.deinit(allocator);
    try Scenario.observe(world, &builder, allocator);
    try builder.canonicalize();
    const digest = builder.digest();
    try result.addObservation(.{ .index = index, .digest = digest, .features = builder.features.items });
    return digest;
}

fn evaluateProperties(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    world: *Scenario.World,
    result: *trace.Trace,
    tracker: *property.Tracker,
    index: u64,
) !void {
    var sink = property.Sink{};
    defer sink.deinit(allocator);
    try Scenario.evaluate(world, &sink, allocator);
    try sink.canonicalize();
    for (sink.evaluations.items) |evaluation| {
        const declaration = declarationFor(Scenario.properties, evaluation.property_id) orelse return error.UnknownPropertyId;
        try tracker.record(index, evaluation);
        try result.addProperty(.{
            .index = index,
            .property_id = evaluation.property_id,
            .name = declaration.name,
            .kind = declaration.kind,
            .condition = evaluation.condition,
            .details = evaluation.details,
        });
    }
}

fn declarationFor(declarations: []const property.Declaration, id: ids.StableId) ?property.Declaration {
    for (declarations) |declaration| if (declaration.id == id) return declaration;
    return null;
}
