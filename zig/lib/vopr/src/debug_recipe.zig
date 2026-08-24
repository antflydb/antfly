// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Automatic, bounded debug packaging for a newly observed fingerprint.

const std = @import("std");
const causal = @import("causal.zig");
const collector = @import("collector.zig");
const debugger = @import("debugger.zig");
const event_query = @import("event_query.zig");
const ids = @import("id.zig");
const reducer = @import("reducer.zig");
const replay = @import("replay.zig");
const execution_runner = @import("runner.zig");
const trace = @import("trace.zig");

pub const format = "vopr-debug-recipe-v1";

pub const QuerySpec = struct {
    name: []const u8,
    query: event_query.Query,
};

pub const Config = struct {
    failure_ordinal: usize = 0,
    reduction: reducer.Config = .{ .max_attempts = 1_024 },
    counterfactual: causal.CounterfactualConfig = .{},
    event_queries: []const QuerySpec = &.{},
    collect_failure_window: bool = true,
    collector_future_choices: usize = 1,
};

pub const QueryResult = struct {
    name: []const u8,
    matches: []event_query.Match,
};

pub const CollectorRunner = struct {
    context: ?*anyopaque = null,
    collect_fn: *const fn (?*anyopaque, std.mem.Allocator, *const trace.Trace, usize) anyerror!collector.Sink,

    pub fn collect(self: CollectorRunner, allocator: std.mem.Allocator, artifact: *const trace.Trace, prefix: usize) !collector.Sink {
        return self.collect_fn(self.context, allocator, artifact, prefix);
    }
};

pub const RecipeRunner = struct {
    execution: reducer.ExecutionRunner,
    collectors: ?CollectorRunner = null,
};

pub const Package = struct {
    allocator: std.mem.Allocator,
    fingerprint: ids.StableId,
    reduced: reducer.Result,
    causal_report: causal.Report,
    counterfactual_report: causal.CounterfactualReport,
    queries: []QueryResult,
    collectors: ?[3]collector.Sink,

    pub fn deinit(self: *Package) void {
        if (self.collectors) |*window| for (window) |*sink| sink.deinit();
        for (self.queries) |query| {
            self.allocator.free(query.name);
            self.allocator.free(query.matches);
        }
        self.allocator.free(self.queries);
        self.counterfactual_report.deinit();
        self.causal_report.deinit();
        self.reduced.deinit();
        self.* = undefined;
    }

    pub fn renderAlloc(self: *const Package, allocator: std.mem.Allocator) ![]u8 {
        const CollectorWire = struct {
            before: []const collector.Record,
            at_failure: []const collector.Record,
            after: []const collector.Record,
        };
        const collectors: ?CollectorWire = if (self.collectors) |*window| .{
            .before = window[0].records.items,
            .at_failure = window[1].records.items,
            .after = window[2].records.items,
        } else null;
        return std.json.Stringify.valueAlloc(allocator, .{
            .format = format,
            .fingerprint = self.fingerprint,
            .reduction = self.reduced.report,
            .causal = .{
                .failure_identity = self.causal_report.failure_identity,
                .failure_index = self.causal_report.failure_index,
                .items = self.causal_report.items,
            },
            .counterfactual = .{
                .config = self.counterfactual_report.config,
                .experiments = self.counterfactual_report.experiments,
                .nodes = self.counterfactual_report.graph.nodes.items,
                .edges = self.counterfactual_report.graph.edges.items,
            },
            .event_queries = self.queries,
            .collectors = collectors,
        }, .{ .whitespace = .indent_2 });
    }
};

pub fn run(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    original: *const trace.Trace,
    config: Config,
) !Package {
    const Adapter = struct {
        fn runCandidate(
            _: ?*anyopaque,
            child_allocator: std.mem.Allocator,
            parent: *const trace.Trace,
            source: @import("choice.zig").Source,
        ) !trace.Trace {
            return execution_runner.run(Scenario, child_allocator, source, runnerConfigFromArtifact(parent));
        }

        fn exactReplay(
            _: ?*anyopaque,
            child_allocator: std.mem.Allocator,
            artifact: *const trace.Trace,
        ) !trace.Trace {
            return replay.exact(Scenario, child_allocator, artifact);
        }

        fn collect(
            _: ?*anyopaque,
            child_allocator: std.mem.Allocator,
            artifact: *const trace.Trace,
            prefix: usize,
        ) !collector.Sink {
            return debugger.collectAt(Scenario, child_allocator, artifact, prefix);
        }
    };
    return runWithRunner(allocator, original, config, .{
        .execution = .{ .run_fn = Adapter.runCandidate, .replay_fn = Adapter.exactReplay },
        .collectors = if (@hasDecl(Scenario, "collect")) .{ .collect_fn = Adapter.collect } else null,
    });
}

pub fn runWithRunner(
    allocator: std.mem.Allocator,
    original: *const trace.Trace,
    config: Config,
    scenario_runner: RecipeRunner,
) !Package {
    try original.validate();
    if (config.failure_ordinal >= original.failures.items.len) return error.FailureOrdinalOutOfRange;
    for (config.event_queries, 0..) |query, index| {
        if (query.name.len == 0) return error.EmptyDebugRecipeQueryName;
        for (config.event_queries[0..index]) |prior| if (std.mem.eql(u8, prior.name, query.name))
            return error.DuplicateDebugRecipeQueryName;
    }
    var exact = try scenario_runner.execution.exactReplay(allocator, original);
    exact.deinit();
    const fingerprint = original.failures.items[config.failure_ordinal].fingerprint;
    var reduced = try reducer.reduceWithRunner(allocator, original, fingerprint, config.reduction, scenario_runner.execution);
    errdefer reduced.deinit();
    const reduced_failure_ordinal = failureOrdinal(&reduced.artifact, fingerprint) orelse
        return error.ReducedFingerprintMissing;
    var causal_report = try causal.analyzeAlloc(allocator, &reduced.artifact, reduced_failure_ordinal);
    errdefer causal_report.deinit();
    var counterfactual_report = try causal.analyzeCounterfactualWithRunner(
        allocator,
        &reduced.artifact,
        reduced_failure_ordinal,
        config.counterfactual,
        .{
            .context = scenario_runner.execution.context,
            .run_fn = scenario_runner.execution.run_fn,
            .replay_fn = scenario_runner.execution.replay_fn,
        },
    );
    errdefer counterfactual_report.deinit();
    const queries = try allocator.alloc(QueryResult, config.event_queries.len);
    errdefer allocator.free(queries);
    var queries_initialized: usize = 0;
    errdefer for (queries[0..queries_initialized]) |query| {
        allocator.free(query.name);
        allocator.free(query.matches);
    };
    for (config.event_queries, 0..) |query, index| {
        const name = try allocator.dupe(u8, query.name);
        errdefer allocator.free(name);
        const matches = try event_query.searchAlloc(allocator, &reduced.artifact, query.query);
        queries[index] = .{ .name = name, .matches = matches };
        queries_initialized += 1;
    }
    var collectors: ?[3]collector.Sink = null;
    errdefer if (collectors) |*window| for (window) |*sink| sink.deinit();
    if (config.collect_failure_window) {
        const collector_runner = scenario_runner.collectors orelse return error.DebugRecipeCollectorsUnsupported;
        const failure_prefix = @min(
            reduced.artifact.choices.items.len,
            std.math.cast(usize, reduced.artifact.failures.items[reduced_failure_ordinal].index) orelse reduced.artifact.choices.items.len,
        );
        const prefixes = [3]usize{
            failure_prefix -| 1,
            failure_prefix,
            @min(reduced.artifact.choices.items.len, failure_prefix +| config.collector_future_choices),
        };
        var window: [3]collector.Sink = undefined;
        var initialized: usize = 0;
        errdefer for (window[0..initialized]) |*sink| sink.deinit();
        for (prefixes, 0..) |prefix, index| {
            window[index] = try collector_runner.collect(allocator, &reduced.artifact, prefix);
            initialized += 1;
        }
        collectors = window;
    }
    return .{
        .allocator = allocator,
        .fingerprint = fingerprint,
        .reduced = reduced,
        .causal_report = causal_report,
        .counterfactual_report = counterfactual_report,
        .queries = queries,
        .collectors = collectors,
    };
}

fn runnerConfigFromArtifact(artifact: *const trace.Trace) execution_runner.Config {
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

fn failureOrdinal(history: *const trace.Trace, fingerprint: ids.StableId) ?usize {
    for (history.failures.items, 0..) |failure, index| if (failure.fingerprint == fingerprint) return index;
    return null;
}

test "automatic debug recipe packages reduction causality queries and collectors" {
    const choice = @import("choice.zig");
    const event = @import("event.zig");
    const observation = @import("observation.zig");
    const outcome = @import("outcome.zig");
    const property = @import("property.zig");
    const runner = @import("runner.zig");
    const transition = @import("transition.zig");
    const Scenario = struct {
        const good_id = ids.stable("transition", "recipe.good");
        const bad_id = ids.stable("transition", "recipe.bad");
        const property_id = ids.stable("property", "recipe.safe");
        pub const name: []const u8 = "debug-recipe";
        pub const version: u32 = 1;
        pub const properties = &[_]property.Declaration{.{ .id = property_id, .name = "recipe.safe", .kind = .always }};
        pub const World = struct { done: bool = false, bad: bool = false };
        pub fn init(_: std.mem.Allocator) !World {
            return .{};
        }
        pub fn deinit(_: *World, _: std.mem.Allocator) void {}
        pub fn enumerate(world: *World, list: *transition.List, allocator_: std.mem.Allocator) !void {
            if (world.done) return;
            try list.append(allocator_, .{ .id = good_id, .name = "recipe.good", .kind = .workload });
            try list.append(allocator_, .{ .id = bad_id, .name = "recipe.bad", .kind = .fault });
        }
        pub fn execute(world: *World, selected: transition.Transition, events: *event.Sink, allocator_: std.mem.Allocator) !outcome.TransitionOutcome {
            world.done = true;
            world.bad = selected.id == bad_id;
            try events.emitDetailed(allocator_, .{
                .id = ids.stable("event", "recipe.decision"),
                .name = "recipe.decision",
                .kind = .domain,
                .actor_id = 7,
                .resource_id = 9,
            }, if (world.bad) "bad" else "good");
            return .applied();
        }
        pub fn observe(world: *World, builder: *observation.Builder, allocator_: std.mem.Allocator) !void {
            try builder.addNamed(allocator_, "bad", @intFromBool(world.bad));
        }
        pub fn evaluate(world: *World, sink: *property.Sink, allocator_: std.mem.Allocator) !void {
            try sink.check(allocator_, property_id, !world.bad);
        }
        pub fn done(world: *World) bool {
            return world.done;
        }
        pub fn collect(world: *World, sink: *collector.Sink) !void {
            try sink.add("world", if (world.bad) "bad" else if (world.done) "good" else "initial");
        }
    };
    var script = choice.Scripted{ .selections = &.{Scenario.bad_id} };
    var history = try runner.run(Scenario, std.testing.allocator, script.source(), .{ .transition_budget = 1 });
    defer history.deinit();
    const queries = [_]QuerySpec{.{ .name = "decision", .query = .{ .selector = .{ .name = "recipe.decision" } } }};
    var package = try run(Scenario, std.testing.allocator, &history, .{
        .reduction = .{ .max_attempts = 8 },
        .counterfactual = .{ .prefix_window = 2, .descendants_per_alternative = 1, .max_experiments = 2 },
        .event_queries = &queries,
    });
    defer package.deinit();
    try std.testing.expectEqual(@as(usize, 1), package.queries[0].matches.len);
    try std.testing.expect(package.collectors != null);
    const rendered = try package.renderAlloc(std.testing.allocator);
    defer std.testing.allocator.free(rendered);
    try std.testing.expect(std.mem.indexOf(u8, rendered, format) != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "counterfactual") != null);
}
