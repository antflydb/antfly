// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Clean-replay multiverse debugger primitives.

const std = @import("std");
const choice = @import("choice.zig");
const collector = @import("collector.zig");
const ids = @import("id.zig");
const replay = @import("replay.zig");
const runner = @import("runner.zig");
const trace = @import("trace.zig");

pub const Cursor = struct {
    artifact: *const trace.Trace,
    choice_prefix: usize = 0,

    pub fn seek(self: *Cursor, prefix: usize) !void {
        if (prefix > self.artifact.choices.items.len) return error.DebuggerPrefixOutOfRange;
        self.choice_prefix = prefix;
    }

    pub fn choice(self: *const Cursor) ?trace.ChoiceRecord {
        if (self.choice_prefix >= self.artifact.choices.items.len) return null;
        return self.artifact.choices.items[self.choice_prefix];
    }

    pub fn enabledAlternatives(self: *const Cursor) []const ids.StableId {
        return if (self.choice()) |record| record.enabled_ids else &.{};
    }

    pub fn transitionPrefix(self: *const Cursor) []const trace.TransitionRecord {
        return self.artifact.transitions.items[0..@min(self.choice_prefix, self.artifact.transitions.items.len)];
    }
};

pub fn branch(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    artifact: *const trace.Trace,
    choice_index: usize,
    replacement_id: ids.StableId,
    suffix_seed: u64,
) !trace.Trace {
    try artifact.validate();
    if (choice_index >= artifact.choices.items.len) return error.DebuggerPrefixOutOfRange;
    const record = artifact.choices.items[choice_index];
    if (std.mem.indexOfScalar(ids.StableId, record.enabled_ids, replacement_id) == null)
        return error.DebuggerAlternativeNotEnabled;
    var source = choice.Mutating.init(artifact.choices.items, choice_index, replacement_id, suffix_seed);
    var child = try runner.run(Scenario, allocator, source.source(), runnerConfigFromArtifact(artifact));
    errdefer child.deinit();
    var replayed = try replay.exact(Scenario, allocator, &child);
    replayed.deinit();
    return child;
}

pub fn collectAt(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    artifact: *const trace.Trace,
    choice_prefix: usize,
) !collector.Sink {
    var sink = collector.Sink.init(allocator, choice_prefix);
    errdefer sink.deinit();
    try runner.collectAt(Scenario, allocator, artifact, choice_prefix, &sink);
    return sink;
}

pub fn collectFailureWindow(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    artifact: *const trace.Trace,
    failure_ordinal: usize,
    future_choices: usize,
) ![3]collector.Sink {
    if (failure_ordinal >= artifact.failures.items.len) return error.FailureOrdinalOutOfRange;
    const failure_prefix = @min(artifact.choices.items.len, std.math.cast(usize, artifact.failures.items[failure_ordinal].index) orelse artifact.choices.items.len);
    const prefixes = [3]usize{
        failure_prefix -| 1,
        failure_prefix,
        @min(artifact.choices.items.len, failure_prefix +| future_choices),
    };
    var result: [3]collector.Sink = undefined;
    var initialized: usize = 0;
    errdefer for (result[0..initialized]) |*sink| sink.deinit();
    for (prefixes, 0..) |prefix, index| {
        result[index] = try collectAt(Scenario, allocator, artifact, prefix);
        initialized += 1;
    }
    return result;
}

pub fn exportCanonical(allocator: std.mem.Allocator, artifact: *const trace.Trace) ![]u8 {
    try artifact.validate();
    return artifact.renderAlloc(allocator);
}

/// Render a stable, non-interactive cursor snapshot suitable for a CLI, TUI,
/// or editor integration. Branch execution remains scenario-specific, while
/// navigation over an artifact is fully generic.
pub fn inspectAlloc(
    allocator: std.mem.Allocator,
    artifact: *const trace.Trace,
    choice_prefix: usize,
) ![]u8 {
    try artifact.validate();
    var cursor: Cursor = .{ .artifact = artifact };
    try cursor.seek(choice_prefix);
    return std.json.Stringify.valueAlloc(allocator, .{
        .format = "vopr-debug-snapshot-v1",
        .scenario = artifact.header.scenario,
        .scenario_version = artifact.header.scenario_version,
        .choice_prefix = cursor.choice_prefix,
        .choice = cursor.choice(),
        .enabled_alternatives = cursor.enabledAlternatives(),
        .transition_prefix = cursor.transitionPrefix(),
        .failures = artifact.failures.items,
        .summary = artifact.summary,
    }, .{ .whitespace = .indent_2 });
}

/// Compare a counterfactual child with its parent without retaining pointers
/// into either trace.  The stable JSON form is suitable for campaign
/// artifacts, debugger sessions, and editor integrations.
pub fn compareAlloc(
    allocator: std.mem.Allocator,
    parent: *const trace.Trace,
    child: *const trace.Trace,
) ![]u8 {
    try parent.validate();
    try child.validate();
    if (!std.mem.eql(u8, parent.header.scenario, child.header.scenario) or
        parent.header.scenario_version != child.header.scenario_version)
        return error.DebuggerScenarioMismatch;

    const shared_limit = @min(parent.choices.items.len, child.choices.items.len);
    var shared_prefix: usize = 0;
    while (shared_prefix < shared_limit) : (shared_prefix += 1) {
        const left = parent.choices.items[shared_prefix];
        const right = child.choices.items[shared_prefix];
        if (left.selected_id != right.selected_id or
            left.site_id != right.site_id or
            left.occurrence != right.occurrence or
            !std.mem.eql(ids.StableId, left.enabled_ids, right.enabled_ids)) break;
    }
    const parent_choice = if (shared_prefix < parent.choices.items.len)
        parent.choices.items[shared_prefix]
    else
        null;
    const child_choice = if (shared_prefix < child.choices.items.len)
        child.choices.items[shared_prefix]
    else
        null;

    const parent_bytes = try parent.renderAlloc(allocator);
    defer allocator.free(parent_bytes);
    const child_bytes = try child.renderAlloc(allocator);
    defer allocator.free(child_bytes);
    return std.json.Stringify.valueAlloc(allocator, .{
        .format = "vopr-debug-comparison-v1",
        .scenario = parent.header.scenario,
        .scenario_version = parent.header.scenario_version,
        .shared_choice_prefix = shared_prefix,
        .parent_choice = parent_choice,
        .child_choice = child_choice,
        .parent_summary = parent.summary,
        .child_summary = child.summary,
        .parent_failures = parent.failures.items,
        .child_failures = child.failures.items,
        .parent_trace_digest = ids.digest(parent_bytes),
        .child_trace_digest = ids.digest(child_bytes),
    }, .{ .whitespace = .indent_2 });
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

test "debugger seeks branches exact replays and collects failure moments" {
    const event = @import("event.zig");
    const observation = @import("observation.zig");
    const outcome = @import("outcome.zig");
    const property = @import("property.zig");
    const transition = @import("transition.zig");
    const Scenario = struct {
        const good_id = ids.stable("transition", "debug.good");
        const bad_id = ids.stable("transition", "debug.bad");
        const property_id = ids.stable("property", "debug.safe");
        pub const name: []const u8 = "debugger-test";
        pub const version: u32 = 1;
        pub const properties = &[_]property.Declaration{.{ .id = property_id, .name = "debug.safe", .kind = .always }};
        pub const World = struct { done: bool = false, bad: bool = false };
        pub fn init(_: std.mem.Allocator) !World {
            return .{};
        }
        pub fn deinit(_: *World, _: std.mem.Allocator) void {}
        pub fn enumerate(world: *World, list: *transition.List, allocator: std.mem.Allocator) !void {
            if (world.done) return;
            try list.append(allocator, .{ .id = good_id, .name = "debug.good", .kind = .workload });
            try list.append(allocator, .{ .id = bad_id, .name = "debug.bad", .kind = .fault });
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
        pub fn collect(world: *World, sink: *collector.Sink) !void {
            try sink.add("world", if (world.bad) "bad" else if (world.done) "good" else "initial");
        }
    };
    var script = choice.Scripted{ .selections = &.{Scenario.bad_id} };
    var artifact = try runner.run(Scenario, std.testing.allocator, script.source(), .{ .transition_budget = 1 });
    defer artifact.deinit();
    var cursor: Cursor = .{ .artifact = &artifact };
    try cursor.seek(0);
    try std.testing.expectEqual(@as(usize, 2), cursor.enabledAlternatives().len);
    var child = try branch(Scenario, std.testing.allocator, &artifact, 0, Scenario.good_id, 9);
    defer child.deinit();
    try std.testing.expectEqual(@as(usize, 0), child.failures.items.len);
    var at_failure = try collectAt(Scenario, std.testing.allocator, &artifact, 1);
    defer at_failure.deinit();
    try std.testing.expectEqualStrings("bad", at_failure.records.items[0].bytes);
    var window = try collectFailureWindow(Scenario, std.testing.allocator, &artifact, 0, 1);
    defer for (&window) |*sink| sink.deinit();
    try std.testing.expectEqualStrings("initial", window[0].records.items[0].bytes);
    try std.testing.expectEqualStrings("bad", window[1].records.items[0].bytes);
    const encoded = try exportCanonical(std.testing.allocator, &child);
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "debug.good") != null);
    const snapshot = try inspectAlloc(std.testing.allocator, &artifact, 0);
    defer std.testing.allocator.free(snapshot);
    try std.testing.expect(std.mem.indexOf(u8, snapshot, "vopr-debug-snapshot-v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot, "debug.safe") != null);
    const comparison = try compareAlloc(std.testing.allocator, &artifact, &child);
    defer std.testing.allocator.free(comparison);
    try std.testing.expect(std.mem.indexOf(u8, comparison, "vopr-debug-comparison-v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, comparison, "\"shared_choice_prefix\": 0") != null);
}
