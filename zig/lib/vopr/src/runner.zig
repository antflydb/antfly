// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const choice = @import("choice.zig");
const collector = @import("collector.zig");
const event = @import("event.zig");
const flight_recorder = @import("flight_recorder.zig");
const ids = @import("id.zig");
const observation = @import("observation.zig");
const outcome = @import("outcome.zig");
const property = @import("property.zig");
const scenario_contract = @import("scenario.zig");
const scheduler = @import("scheduler.zig");
const snapshot = @import("snapshot.zig");
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
    scenario_parameters: []const trace.Parameter = &.{},
    source_revision: []const u8 = "unknown",
    target: []const u8 = "native",
    optimize: []const u8 = "unknown",
    /// Optional diagnostic-only scenario dependency (for example, a formal
    /// trace sink). It is intentionally absent from the replay artifact and
    /// must not change choices, observations, properties, or failures.
    scenario_context: ?*anyopaque = null,
    /// Optional diagnostic-only ring. Recorder contents and capacity never
    /// participate in choices, observations, fingerprints, or replay.
    flight_recorder: ?*flight_recorder.Recorder = null,
};

const PropertyFailure = struct {
    declaration: property.Declaration,
    transition_index: u64,
};

const StepResult = enum { executed, stopped };

pub fn run(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    choice_source: choice.Source,
    config: Config,
) !trace.Trace {
    comptime scenario_contract.assertContract(Scenario);
    if (config.transition_budget == 0) return error.InvalidTransitionBudget;

    var world = try initWorld(Scenario, allocator, config.scenario_context);
    defer Scenario.deinit(&world, allocator);
    var tracker = try property.Tracker.init(allocator, Scenario.properties);
    defer tracker.deinit();
    var result = try initTrace(Scenario, allocator, config);
    errdefer result.deinit();

    const last_digest = try recordObservation(Scenario, allocator, &world, &result, 0);
    return continueRun(Scenario, allocator, choice_source, config.transition_budget, &world, &tracker, &result, 0, last_digest, config.flight_recorder);
}

/// Replays and validates an exact prefix, then captures all logical execution
/// state needed to resume: scenario-owned bytes and accumulated property
/// statuses. It never snapshots pointers, allocators, or OS resources.
pub fn captureCheckpoint(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    artifact: *const trace.Trace,
    prefix_len: usize,
) !snapshot.Logical {
    comptime scenario_contract.assertContract(Scenario);
    comptime snapshot.assertContract(Scenario);
    try artifact.validate();
    try validateCompatibility(Scenario, artifact);
    if (prefix_len > artifact.choices.items.len) return error.InvalidSnapshotPrefix;

    const config = configFromTrace(artifact);
    var world = try initWorld(Scenario, allocator, config.scenario_context);
    defer Scenario.deinit(&world, allocator);
    var tracker = try property.Tracker.init(allocator, Scenario.properties);
    defer tracker.deinit();
    var actual = try initTrace(Scenario, allocator, config);
    defer actual.deinit();
    var last_digest = try recordObservation(Scenario, allocator, &world, &actual, 0);
    var replay_source = choice.Replay{ .records = artifact.choices.items };
    var transition_index: u64 = 0;
    while (transition_index < prefix_len) {
        if (try executeStep(Scenario, allocator, replay_source.source(), config.transition_budget, &world, &tracker, &actual, &transition_index, &last_digest, null) == .stopped)
            return error.SnapshotPrefixPastScenarioEnd;
    }
    actual.summary = .{
        .transitions = transition_index,
        .final_observation_digest = last_digest,
        .property_failures = 0,
    };
    var expected = try initTraceFromArtifact(allocator, artifact);
    defer expected.deinit();
    try copyPrefix(&expected, artifact, prefix_len);
    expected.summary = actual.summary;
    const actual_bytes = try actual.renderAlloc(allocator);
    defer allocator.free(actual_bytes);
    const expected_bytes = try expected.renderAlloc(allocator);
    defer allocator.free(expected_bytes);
    if (!std.mem.eql(u8, actual_bytes, expected_bytes)) return error.CheckpointPrefixArtifactDiverged;

    return snapshot.captureExecution(
        Scenario,
        allocator,
        &world,
        tracker.statuses,
        transition_index,
        last_digest,
        try snapshot.prefixDigest(artifact, prefix_len),
    );
}

/// Clean-replay to a choice prefix, prove the reconstructed prefix equals the
/// artifact, then let the scenario emit pointer-free logical diagnostics.
/// Scenarios opt in with `pub fn collect(*World, *collector.Sink) !void`.
pub fn collectAt(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    artifact: *const trace.Trace,
    prefix_len: usize,
    sink: *collector.Sink,
) !void {
    comptime scenario_contract.assertContract(Scenario);
    if (!@hasDecl(Scenario, "collect")) return error.ScenarioCollectorsUnsupported;
    try artifact.validate();
    try validateCompatibility(Scenario, artifact);
    if (prefix_len > artifact.choices.items.len or sink.choice_prefix != prefix_len)
        return error.InvalidCollectorPrefix;
    const config = configFromTrace(artifact);
    var world = try initWorld(Scenario, allocator, config.scenario_context);
    defer Scenario.deinit(&world, allocator);
    var tracker = try property.Tracker.init(allocator, Scenario.properties);
    defer tracker.deinit();
    var actual = try initTrace(Scenario, allocator, config);
    defer actual.deinit();
    var last_digest = try recordObservation(Scenario, allocator, &world, &actual, 0);
    var replay_source = choice.Replay{ .records = artifact.choices.items };
    var transition_index: u64 = 0;
    while (transition_index < prefix_len) {
        if (try executeStep(Scenario, allocator, replay_source.source(), config.transition_budget, &world, &tracker, &actual, &transition_index, &last_digest, null) == .stopped)
            return error.CollectorPrefixPastScenarioEnd;
    }
    actual.summary = .{
        .transitions = transition_index,
        .final_observation_digest = last_digest,
        .property_failures = 0,
    };
    var expected = try initTraceFromArtifact(allocator, artifact);
    defer expected.deinit();
    try copyPrefix(&expected, artifact, prefix_len);
    expected.summary = actual.summary;
    const actual_bytes = try actual.renderAlloc(allocator);
    defer allocator.free(actual_bytes);
    const expected_bytes = try expected.renderAlloc(allocator);
    defer allocator.free(expected_bytes);
    if (!std.mem.eql(u8, actual_bytes, expected_bytes)) return error.CollectorPrefixArtifactDiverged;
    try Scenario.collect(&world, sink);
    sink.canonicalize();
}

/// Restores a validated logical checkpoint and executes a suffix source. The
/// returned artifact includes the original canonical prefix. Callers must
/// exact-replay the complete artifact from a clean world before retention.
pub fn resumeFromCheckpoint(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    artifact: *const trace.Trace,
    checkpoint: snapshot.Logical,
    suffix_source: choice.Source,
) !trace.Trace {
    return resumeFromCheckpointWithRecorder(Scenario, allocator, artifact, checkpoint, suffix_source, null);
}

pub fn resumeFromCheckpointWithRecorder(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    artifact: *const trace.Trace,
    checkpoint: snapshot.Logical,
    suffix_source: choice.Source,
    recorder: ?*flight_recorder.Recorder,
) !trace.Trace {
    comptime scenario_contract.assertContract(Scenario);
    comptime snapshot.assertContract(Scenario);
    try artifact.validate();
    try validateCompatibility(Scenario, artifact);
    const prefix_len = std.math.cast(usize, checkpoint.transition_index) orelse return error.InvalidSnapshotPrefix;
    if (prefix_len > artifact.choices.items.len or checkpoint.prefix_digest != try snapshot.prefixDigest(artifact, prefix_len))
        return error.CheckpointPrefixIdentityDiverged;
    if (checkpoint.observation_digest != artifact.observations.items[prefix_len].digest)
        return error.CheckpointPrefixObservationDiverged;

    const config = configFromTrace(artifact);
    var world = try initWorld(Scenario, allocator, config.scenario_context);
    defer Scenario.deinit(&world, allocator);
    try snapshot.restore(Scenario, &world, checkpoint, allocator);
    var tracker = try property.Tracker.init(allocator, Scenario.properties);
    defer tracker.deinit();
    try snapshot.restorePropertyTracker(checkpoint, &tracker);
    var result = try initTraceFromArtifact(allocator, artifact);
    errdefer result.deinit();
    try copyPrefix(&result, artifact, prefix_len);
    return continueRun(
        Scenario,
        allocator,
        suffix_source,
        config.transition_budget,
        &world,
        &tracker,
        &result,
        checkpoint.transition_index,
        checkpoint.observation_digest,
        recorder,
    );
}

fn continueRun(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    choice_source: choice.Source,
    transition_budget: u64,
    world: *Scenario.World,
    tracker: *property.Tracker,
    result: *trace.Trace,
    starting_transition_index: u64,
    starting_digest: u64,
    recorder: ?*flight_recorder.Recorder,
) !trace.Trace {
    var last_digest = starting_digest;
    var transition_index = starting_transition_index;
    while (!Scenario.done(world)) {
        if (try executeStep(Scenario, allocator, choice_source, transition_budget, world, tracker, result, &transition_index, &last_digest, recorder) == .stopped)
            break;
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
    std.mem.sort(trace.FailureRecord, result.failures.items, {}, struct {
        fn lessThan(_: void, lhs: trace.FailureRecord, rhs: trace.FailureRecord) bool {
            if (lhs.index != rhs.index) return lhs.index < rhs.index;
            return lhs.fingerprint < rhs.fingerprint;
        }
    }.lessThan);
    result.summary = .{
        .transitions = transition_index,
        .final_observation_digest = last_digest,
        .property_failures = @intCast(tracker.failureCount()),
    };
    try result.validate();
    _ = try choice.auditTrace(result);
    return result.*;
}

fn executeStep(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    choice_source: choice.Source,
    transition_budget: u64,
    world: *Scenario.World,
    tracker: *property.Tracker,
    result: *trace.Trace,
    transition_index: *u64,
    last_digest: *u64,
    recorder: ?*flight_recorder.Recorder,
) !StepResult {
    if (Scenario.done(world)) return error.SnapshotPrefixPastScenarioEnd;
    if (transition_index.* == transition_budget) return .stopped;

    var enabled = try scheduler.enumerateCanonical(Scenario, world, allocator);
    defer enabled.deinit(allocator);
    if (enabled.items.items.len == 0) {
        const classification: scenario_contract.NoTransition = if (@hasDecl(Scenario, "classifyNoTransition"))
            Scenario.classifyNoTransition(world)
        else
            .{ .harness_deadlock = "vopr.scenario_deadlock" };
        switch (classification) {
            .clean_quiescence, .expected_blocked => return .stopped,
            .liveness_failure => |identity| {
                if (identity.len == 0) return error.EmptyLivenessFailureIdentity;
                try result.addFailure(.{
                    .index = transition_index.*,
                    .class = .liveness,
                    .property_id = null,
                    .identity = identity,
                    .fingerprint = ids.derive("failure.liveness", ids.stable("failure", identity), Scenario.version),
                    .observation_digest = last_digest.*,
                });
                return .stopped;
            },
            .harness_deadlock => return error.ScenarioDeadlock,
        }
    }

    const occurrence = transition_index.*;
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

    transition_index.* += 1;
    const selected_transition = selected orelse return error.ChoiceSourceSelectedDisabledAlternative;
    try scheduler.verifyStillEnabled(Scenario, world, allocator, enabled.items.items, selected_transition);
    var events = event.Sink{};
    defer events.deinit(allocator);
    var transition_outcome: outcome.TransitionOutcome = try Scenario.execute(world, selected_transition, &events, allocator);
    try transition_outcome.validate();
    try result.addTransition(.{
        .index = transition_index.*,
        .id = selected_transition.id,
        .name = selected_transition.name,
        .kind = selected_transition.kind,
        .actor_id = selected_transition.actor_id,
        .resource_id = selected_transition.resource_id,
        .parameter = selected_transition.parameter,
        .payload_digest = selected_transition.payloadDigest(),
    });
    var has_explicit_fault_lifecycle = false;
    for (events.events.items) |emitted| {
        const phase: ?trace.FaultPhase = switch (emitted.kind) {
            .fault_started => .start,
            .fault_stopped => .end,
            else => null,
        };
        if (phase) |fault_phase| {
            has_explicit_fault_lifecycle = true;
            try result.addFault(.{
                .index = transition_index.*,
                .id = emitted.resource_id orelse return error.FaultLifecycleEventMissingFaultId,
                .name = emitted.name,
                .phase = fault_phase,
            });
        }
    }
    if (selected_transition.kind == .fault and !has_explicit_fault_lifecycle) {
        try result.addFault(.{
            .index = transition_index.*,
            .id = selected_transition.id,
            .name = selected_transition.name,
            .phase = selected_transition.fault_phase orelse .pulse,
        });
    }
    for (events.events.items, 0..) |emitted, ordinal| {
        try result.addEvent(.{
            .index = transition_index.*,
            .ordinal = @intCast(ordinal),
            .id = emitted.id,
            .name = emitted.name,
            .kind = emitted.kind,
            .actor_id = emitted.actor_id,
            .resource_id = emitted.resource_id,
            .payload_digest = emitted.payload_digest,
        });
        if (recorder) |flight| try flight.recordEvent(transition_index.*, @intCast(ordinal), emitted, emitted.details);
    }
    last_digest.* = try recordObservation(Scenario, allocator, world, result, transition_index.*);
    const prior_failure_count = tracker.failureCount();
    try evaluateProperties(Scenario, allocator, world, result, tracker, transition_index.*);
    if (@hasDecl(Scenario, "quiescent") and Scenario.quiescent(world)) tracker.beginQuiescence(transition_index.*);
    if (Scenario.done(world)) tracker.finish(transition_index.*);
    if (tracker.failureCount() > prior_failure_count) {
        for (Scenario.properties, tracker.statuses) |declaration, status| {
            if (status.first_failure_transition == transition_index.*) {
                transition_outcome = outcome.TransitionOutcome.propertyViolation(
                    declaration.id,
                    declaration.name,
                    last_digest.*,
                );
                break;
            }
        }
    }
    const outcome_event = transition_outcome.asEvent();
    try result.addEvent(.{
        .index = transition_index.*,
        .ordinal = @intCast(events.events.items.len),
        .id = outcome_event.id,
        .name = outcome_event.name,
        .kind = outcome_event.kind,
        .actor_id = selected_transition.actor_id,
        .resource_id = selected_transition.resource_id,
        .payload_digest = outcome_event.payload_digest,
    });
    if (recorder) |flight| try flight.recordEvent(
        transition_index.*,
        @intCast(events.events.items.len),
        outcome_event,
        transition_outcome.identity,
    );
    if (transition_outcome.failureClass()) |failure_class| {
        // Property failures are materialized from the property tracker at the
        // end so their first-failure identity remains the single authority.
        if (failure_class != .property) try result.addFailure(.{
            .index = transition_index.*,
            .class = failure_class,
            .property_id = transition_outcome.property_id,
            .identity = transition_outcome.identity,
            .fingerprint = transition_outcome.fingerprint(),
            .observation_digest = last_digest.*,
        });
    }
    return .executed;
}

fn initTrace(comptime Scenario: type, allocator: std.mem.Allocator, config: Config) !trace.Trace {
    return trace.Trace.init(allocator, .{
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
        .scenario_parameters = config.scenario_parameters,
    });
}

fn initWorld(comptime Scenario: type, allocator: std.mem.Allocator, context: ?*anyopaque) !Scenario.World {
    if (comptime @hasDecl(Scenario, "initWithContext")) return Scenario.initWithContext(allocator, context);
    if (context != null) return error.ScenarioDoesNotAcceptContext;
    return Scenario.init(allocator);
}

fn initTraceFromArtifact(allocator: std.mem.Allocator, artifact: *const trace.Trace) !trace.Trace {
    return trace.Trace.init(allocator, artifact.header, artifact.config);
}

fn configFromTrace(artifact: *const trace.Trace) Config {
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

fn validateCompatibility(comptime Scenario: type, artifact: *const trace.Trace) !void {
    if (!std.mem.eql(u8, artifact.header.scenario, Scenario.name)) return error.IncompatibleScenario;
    if (artifact.header.scenario_version != Scenario.version) return error.IncompatibleScenarioVersion;
}

fn copyPrefix(destination: *trace.Trace, source: *const trace.Trace, prefix_len: usize) !void {
    for (source.choices.items[0..prefix_len]) |record| try destination.addChoice(record);
    for (source.transitions.items[0..prefix_len]) |record| try destination.addTransition(record);
    for (source.faults.items) |record| {
        if (record.index > prefix_len) break;
        try destination.addFault(record);
    }
    for (source.events.items) |record| {
        if (record.index > prefix_len) break;
        try destination.addEvent(record);
    }
    for (source.observations.items[0 .. prefix_len + 1]) |record| try destination.addObservation(record);
    for (source.properties.items) |record| {
        if (record.index > prefix_len) break;
        try destination.addProperty(record);
    }
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

test "flight recording retains verbose details without changing replay truth" {
    const Scenario = struct {
        pub const name: []const u8 = "runner-flight-recorder";
        pub const version: u32 = 1;
        const step_id = ids.stable("transition", "runner-flight.step");
        pub const properties = &[_]property.Declaration{};
        pub const World = struct { done: bool = false };
        pub fn init(_: std.mem.Allocator) !World {
            return .{};
        }
        pub fn deinit(_: *World, _: std.mem.Allocator) void {}
        pub fn enumerate(world: *World, list: *transition.List, allocator_: std.mem.Allocator) !void {
            if (!world.done) try list.append(allocator_, .{ .id = step_id, .name = "runner-flight.step", .kind = .workload });
        }
        pub fn execute(world: *World, _: transition.Transition, events: *event.Sink, allocator_: std.mem.Allocator) !outcome.TransitionOutcome {
            world.done = true;
            try events.emitDetailed(allocator_, event.Event.named(.domain, "runner-flight.detail", 17), "verbose-only-secret");
            return .applied();
        }
        pub fn observe(world: *World, builder: *observation.Builder, allocator_: std.mem.Allocator) !void {
            try builder.addNamed(allocator_, "runner-flight.done", @intFromBool(world.done));
        }
        pub fn evaluate(_: *World, _: *property.Sink, _: std.mem.Allocator) !void {}
        pub fn done(world: *World) bool {
            return world.done;
        }
    };

    var recorder = try flight_recorder.Recorder.init(std.testing.allocator, 4);
    defer recorder.deinit();
    var recorded_source = choice.Seeded.init(9);
    var recorded = try run(Scenario, std.testing.allocator, recorded_source.source(), .{
        .seed = 9,
        .transition_budget = 1,
        .flight_recorder = &recorder,
    });
    defer recorded.deinit();
    var plain_source = choice.Seeded.init(9);
    var plain = try run(Scenario, std.testing.allocator, plain_source.source(), .{ .seed = 9, .transition_budget = 1 });
    defer plain.deinit();
    const recorded_bytes = try recorded.renderAlloc(std.testing.allocator);
    defer std.testing.allocator.free(recorded_bytes);
    const plain_bytes = try plain.renderAlloc(std.testing.allocator);
    defer std.testing.allocator.free(plain_bytes);
    try std.testing.expectEqualSlices(u8, plain_bytes, recorded_bytes);
    try std.testing.expect(std.mem.indexOf(u8, recorded_bytes, "verbose-only-secret") == null);

    var snapshot_value = try recorder.materialize(std.testing.allocator, .failure);
    defer snapshot_value.deinit();
    try std.testing.expectEqual(@as(usize, 2), snapshot_value.records.len);
    try std.testing.expectEqualStrings("verbose-only-secret", snapshot_value.records[0].details);
    var replayed = try @import("replay.zig").exact(Scenario, std.testing.allocator, &recorded);
    replayed.deinit();
}
