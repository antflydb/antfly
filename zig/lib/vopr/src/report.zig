// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Stable repository-owned run/results API and optional static local report.

const std = @import("std");
const ids = @import("id.zig");
const health = @import("health.zig");
const property = @import("property.zig");
const trace = @import("trace.zig");

pub const format = "vopr-results-v1";
pub const aggregate_format = "vopr-run-results-v1";

pub const AggregateProperty = struct {
    property_id: ids.StableId,
    name: []const u8,
    status: []const u8,
    evaluations: u64,
    ever_true: bool,
    ever_false: bool,
};

pub const AggregateFailure = struct {
    fingerprint: u64,
    first_history: u64,
    first_artifact: []const u8,
    smallest_history: u64,
    smallest_transitions: u64,
    smallest_artifact: []const u8,
};

pub const AggregateHealth = struct {
    progress: bool,
    exact_replay: bool,
    harness: bool,
};

pub const Aggregate = struct {
    scenario: []const u8,
    base_seed: u64,
    histories_limit: u64,
    histories_consumed: u64,
    transition_limit_per_history: u64,
    transitions_consumed: u64,
    clean_histories: u64,
    failed_histories: u64,
    replay_divergences: u64,
    harness_errors: u64,
    exact_replays: u64,
    corpus_entries: u64,
    quarantined_entries: u64,
    retained_entries: u64,
    semantic_states: u64,
    transition_kinds: u64,
    faults_reached: u64,
    workloads_reached: u64,
    flight_recordings: u64,
    flight_records: u64,
    flight_records_dropped: u64,
    properties: []const AggregateProperty,
    failures: []const AggregateFailure,
    artifacts: []const []const u8,

    pub fn renderJsonAlloc(self: Aggregate, allocator: std.mem.Allocator) ![]u8 {
        return std.json.Stringify.valueAlloc(allocator, .{
            .format = aggregate_format,
            .scenario = self.scenario,
            .base_seed = self.base_seed,
            .budget = .{
                .histories_limit = self.histories_limit,
                .histories_consumed = self.histories_consumed,
                .transition_limit_per_history = self.transition_limit_per_history,
                .transitions_consumed = self.transitions_consumed,
            },
            .histories = .{
                .clean = self.clean_histories,
                .failed = self.failed_histories,
                .replay_divergences = self.replay_divergences,
                .harness_errors = self.harness_errors,
                .exact_replays = self.exact_replays,
            },
            .corpus = .{
                .entries = self.corpus_entries,
                .quarantined = self.quarantined_entries,
                .retained = self.retained_entries,
            },
            .coverage = .{
                .semantic_states = self.semantic_states,
                .transition_kinds = self.transition_kinds,
                .faults_reached = self.faults_reached,
                .workloads_reached = self.workloads_reached,
            },
            .flight_recorder = .{
                .materialized = self.flight_recordings,
                .records = self.flight_records,
                .dropped = self.flight_records_dropped,
            },
            .health = AggregateHealth{
                .progress = self.histories_consumed == 0 or self.transitions_consumed > 0,
                .exact_replay = self.replay_divergences == 0,
                .harness = self.harness_errors == 0,
            },
            .properties = self.properties,
            .failures = self.failures,
            .artifacts = self.artifacts,
        }, .{ .whitespace = .indent_2 });
    }

    pub fn renderHtmlAlloc(self: Aggregate, allocator: std.mem.Allocator) ![]u8 {
        const json = try self.renderJsonAlloc(allocator);
        defer allocator.free(json);
        const escaped = try escapeHtmlAlloc(allocator, json);
        defer allocator.free(escaped);
        return std.fmt.allocPrint(
            allocator,
            "<!doctype html><html><head><meta charset=\"utf-8\"><title>VOPR campaign {s}</title>" ++
                "<style>body{{font:14px ui-monospace,monospace;max-width:1100px;margin:2rem auto;padding:0 1rem}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;background:#111;color:#eee;padding:1rem;border-radius:6px}}</style>" ++
                "</head><body><h1>VOPR campaign: {s}</h1><p>{d} histories; {d} transitions; {d} failing histories.</p><pre>{s}</pre></body></html>",
            .{ self.scenario, self.scenario, self.histories_consumed, self.transitions_consumed, self.failed_histories, escaped },
        );
    }
};

pub const BudgetUsage = struct {
    transition_limit: u64,
    transitions_consumed: u64,
    resource_limit: u64,
    resources_consumed: u64 = 0,
    histories_limit: ?u64 = null,
    histories_consumed: u64 = 1,
};

pub const Context = struct {
    run_id: ?[]const u8 = null,
    history_id: ?[]const u8 = null,
    corpus_entries: u64 = 0,
    quarantined_entries: u64 = 0,
    resources_consumed: u64 = 0,
    histories_limit: ?u64 = null,
    histories_consumed: u64 = 1,
    artifacts: []const []const u8 = &.{},
    health_snapshot: ?health.Snapshot = null,
};

pub const PropertyEvidence = struct {
    property_id: ids.StableId,
    name: []const u8,
    kind: property.Kind,
    evaluations: u64,
    true_count: u64,
    false_count: u64,
    failed: bool,
    declared_never_encountered: bool,
    first_failure_index: ?u64,
    first_failure_details: []const u8,
    rarest_success_index: ?u64,
    rarest_success_details: []const u8,
    rarest_success_frequency: u64,
};

pub const Failure = struct {
    index: u64,
    class: trace.FailureClass,
    property_id: ?ids.StableId,
    identity: []const u8,
    fingerprint: u64,
    observation_digest: ?u64,
};

pub const Results = struct {
    allocator: std.mem.Allocator,
    run_id: []const u8,
    history_id: []const u8,
    system: []const u8,
    scenario: []const u8,
    scenario_version: u32,
    source_revision: []const u8,
    target: []const u8,
    optimize: []const u8,
    budget: BudgetUsage,
    summary: trace.Summary,
    event_count: u64,
    fault_count: u64,
    corpus_entries: u64,
    quarantined_entries: u64,
    properties: []PropertyEvidence,
    failures: []Failure,
    artifacts: [][]const u8,
    health_checks: [health.check_count]health.Check,

    pub fn build(
        allocator: std.mem.Allocator,
        history: *const trace.Trace,
        declarations: []const property.Declaration,
        context: Context,
    ) !Results {
        try history.validate();
        const rendered = try history.renderAlloc(allocator);
        defer allocator.free(rendered);
        const digest = ids.digest(rendered);
        const derived_id = try std.fmt.allocPrint(allocator, "{x:0>16}", .{digest});
        defer allocator.free(derived_id);
        const run_id = try allocator.dupe(u8, context.run_id orelse derived_id);
        errdefer allocator.free(run_id);
        const history_id = try allocator.dupe(u8, context.history_id orelse derived_id);
        errdefer allocator.free(history_id);
        const system = try allocator.dupe(u8, history.header.system);
        errdefer allocator.free(system);
        const scenario = try allocator.dupe(u8, history.header.scenario);
        errdefer allocator.free(scenario);
        const source_revision = try allocator.dupe(u8, history.header.source_revision);
        errdefer allocator.free(source_revision);
        const target = try allocator.dupe(u8, history.header.target);
        errdefer allocator.free(target);
        const optimize = try allocator.dupe(u8, history.header.optimize);
        errdefer allocator.free(optimize);
        const properties = try buildPropertyEvidence(allocator, history, declarations);
        errdefer freePropertyEvidence(allocator, properties);
        const failures = try allocator.alloc(Failure, history.failures.items.len);
        errdefer allocator.free(failures);
        var failures_initialized: usize = 0;
        errdefer for (failures[0..failures_initialized]) |failure| allocator.free(failure.identity);
        for (history.failures.items, 0..) |failure, index| {
            failures[index] = .{
                .index = failure.index,
                .class = failure.class,
                .property_id = failure.property_id,
                .identity = try allocator.dupe(u8, failure.identity),
                .fingerprint = failure.fingerprint,
                .observation_digest = failure.observation_digest,
            };
            failures_initialized += 1;
        }
        const artifacts = try allocator.alloc([]const u8, context.artifacts.len);
        errdefer allocator.free(artifacts);
        var artifacts_initialized: usize = 0;
        errdefer for (artifacts[0..artifacts_initialized]) |artifact| allocator.free(artifact);
        for (context.artifacts, 0..) |artifact, index| {
            artifacts[index] = try allocator.dupe(u8, artifact);
            artifacts_initialized += 1;
        }
        return .{
            .allocator = allocator,
            .run_id = run_id,
            .history_id = history_id,
            .system = system,
            .scenario = scenario,
            .scenario_version = history.header.scenario_version,
            .source_revision = source_revision,
            .target = target,
            .optimize = optimize,
            .budget = .{
                .transition_limit = history.config.transition_budget,
                .transitions_consumed = history.summary.?.transitions,
                .resource_limit = history.config.resource_budget,
                .resources_consumed = context.resources_consumed,
                .histories_limit = context.histories_limit,
                .histories_consumed = context.histories_consumed,
            },
            .summary = history.summary.?,
            .event_count = @intCast(history.events.items.len),
            .fault_count = @intCast(history.faults.items.len),
            .corpus_entries = context.corpus_entries,
            .quarantined_entries = context.quarantined_entries,
            .properties = properties,
            .failures = failures,
            .artifacts = artifacts,
            .health_checks = health.evaluate(history, context.health_snapshot),
        };
    }

    pub fn deinit(self: *Results) void {
        self.allocator.free(self.run_id);
        self.allocator.free(self.history_id);
        self.allocator.free(self.system);
        self.allocator.free(self.scenario);
        self.allocator.free(self.source_revision);
        self.allocator.free(self.target);
        self.allocator.free(self.optimize);
        freePropertyEvidence(self.allocator, self.properties);
        for (self.failures) |failure| self.allocator.free(failure.identity);
        self.allocator.free(self.failures);
        for (self.artifacts) |artifact| self.allocator.free(artifact);
        self.allocator.free(self.artifacts);
        self.* = undefined;
    }

    pub fn renderJsonAlloc(self: *const Results, allocator: std.mem.Allocator) ![]u8 {
        const Wire = struct {
            format: []const u8,
            run_id: []const u8,
            history_id: []const u8,
            system: []const u8,
            scenario: []const u8,
            scenario_version: u32,
            source_revision: []const u8,
            target: []const u8,
            optimize: []const u8,
            budget: BudgetUsage,
            summary: trace.Summary,
            event_count: u64,
            fault_count: u64,
            corpus_entries: u64,
            quarantined_entries: u64,
            properties: []const PropertyEvidence,
            failures: []const Failure,
            artifacts: []const []const u8,
            health_checks: []const health.Check,
        };
        return std.json.Stringify.valueAlloc(allocator, Wire{
            .format = format,
            .run_id = self.run_id,
            .history_id = self.history_id,
            .system = self.system,
            .scenario = self.scenario,
            .scenario_version = self.scenario_version,
            .source_revision = self.source_revision,
            .target = self.target,
            .optimize = self.optimize,
            .budget = self.budget,
            .summary = self.summary,
            .event_count = self.event_count,
            .fault_count = self.fault_count,
            .corpus_entries = self.corpus_entries,
            .quarantined_entries = self.quarantined_entries,
            .properties = self.properties,
            .failures = self.failures,
            .artifacts = self.artifacts,
            .health_checks = &self.health_checks,
        }, .{});
    }

    pub fn renderHtmlAlloc(self: *const Results, allocator: std.mem.Allocator) ![]u8 {
        const json = try self.renderJsonAlloc(allocator);
        defer allocator.free(json);
        const escaped = try escapeHtmlAlloc(allocator, json);
        defer allocator.free(escaped);
        return std.fmt.allocPrint(
            allocator,
            "<!doctype html><html><head><meta charset=\"utf-8\"><title>VOPR {s}</title>" ++
                "<style>body{{font:14px ui-monospace,monospace;max-width:1100px;margin:2rem auto;padding:0 1rem}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;background:#111;color:#eee;padding:1rem;border-radius:6px}}</style>" ++
                "</head><body><h1>VOPR results: {s}</h1><p>Run <code>{s}</code>; {d} transitions; {d} failures.</p>" ++
                "<pre id=\"vopr-results\">{s}</pre></body></html>",
            .{ self.scenario, self.scenario, self.run_id, self.summary.transitions, self.failures.len, escaped },
        );
    }
};

fn buildPropertyEvidence(
    allocator: std.mem.Allocator,
    history: *const trace.Trace,
    declarations: []const property.Declaration,
) ![]PropertyEvidence {
    var canonical: std.ArrayListUnmanaged(property.Declaration) = .empty;
    defer canonical.deinit(allocator);
    if (declarations.len != 0) {
        try canonical.appendSlice(allocator, declarations);
    } else {
        for (history.properties.items) |record| {
            var found = false;
            for (canonical.items) |declaration| if (declaration.id == record.property_id) {
                found = true;
                break;
            };
            if (!found) try canonical.append(allocator, .{ .id = record.property_id, .name = record.name, .kind = record.kind });
        }
    }
    std.mem.sort(property.Declaration, canonical.items, {}, struct {
        fn lessThan(_: void, lhs: property.Declaration, rhs: property.Declaration) bool {
            return lhs.id < rhs.id;
        }
    }.lessThan);
    for (canonical.items, 0..) |declaration, index| {
        if (declaration.id == 0 or declaration.name.len == 0) return error.InvalidPropertyDeclaration;
        if (index > 0 and canonical.items[index - 1].id == declaration.id) return error.DuplicatePropertyId;
    }

    const evidence = try allocator.alloc(PropertyEvidence, canonical.items.len);
    var initialized: usize = 0;
    errdefer {
        freePropertyEvidenceItems(allocator, evidence[0..initialized]);
        allocator.free(evidence);
    }
    for (canonical.items, 0..) |declaration, index| {
        var evaluations: u64 = 0;
        var true_count: u64 = 0;
        var false_count: u64 = 0;
        var first_failure: ?*const trace.PropertyRecord = null;
        var rarest_success: ?*const trace.PropertyRecord = null;
        var rarest_frequency: u64 = std.math.maxInt(u64);
        for (history.properties.items) |*record| {
            if (record.property_id != declaration.id) continue;
            evaluations += 1;
            true_count += @intFromBool(record.condition);
            false_count += @intFromBool(!record.condition);
            if (first_failure == null and immediateViolation(declaration.kind, record.condition)) first_failure = record;
            if (successfulWitness(declaration.kind, record.condition)) {
                var frequency: u64 = 0;
                for (history.properties.items) |candidate| {
                    if (candidate.property_id == declaration.id and successfulWitness(declaration.kind, candidate.condition) and
                        std.mem.eql(u8, candidate.details, record.details)) frequency += 1;
                }
                if (frequency < rarest_frequency or
                    (frequency == rarest_frequency and (rarest_success == null or record.index < rarest_success.?.index)))
                {
                    rarest_success = record;
                    rarest_frequency = frequency;
                }
            }
        }
        var failed = false;
        var failure_index: ?u64 = if (first_failure) |record| record.index else null;
        for (history.failures.items) |failure| {
            if (failure.class != .property or failure.property_id != declaration.id) continue;
            failed = true;
            if (failure_index == null) failure_index = failure.index;
            break;
        }
        const name = try allocator.dupe(u8, declaration.name);
        errdefer allocator.free(name);
        const first_details = try allocator.dupe(u8, if (first_failure) |record| record.details else "");
        errdefer allocator.free(first_details);
        const rare_details = try allocator.dupe(u8, if (rarest_success) |record| record.details else "");
        evidence[index] = .{
            .property_id = declaration.id,
            .name = name,
            .kind = declaration.kind,
            .evaluations = evaluations,
            .true_count = true_count,
            .false_count = false_count,
            .failed = failed,
            .declared_never_encountered = evaluations == 0,
            .first_failure_index = failure_index,
            .first_failure_details = first_details,
            .rarest_success_index = if (rarest_success) |record| record.index else null,
            .rarest_success_details = rare_details,
            .rarest_success_frequency = if (rarest_success == null) 0 else rarest_frequency,
        };
        initialized += 1;
    }
    return evidence;
}

fn freePropertyEvidence(allocator: std.mem.Allocator, evidence: []PropertyEvidence) void {
    freePropertyEvidenceItems(allocator, evidence);
    allocator.free(evidence);
}

fn freePropertyEvidenceItems(allocator: std.mem.Allocator, evidence: []PropertyEvidence) void {
    for (evidence) |item| {
        allocator.free(item.name);
        allocator.free(item.first_failure_details);
        allocator.free(item.rarest_success_details);
    }
}

fn immediateViolation(kind: property.Kind, condition: bool) bool {
    return switch (kind) {
        .always, .always_or_unreachable => !condition,
        .@"unreachable" => condition,
        .reachable, .sometimes, .eventually_after_quiescence => false,
    };
}

fn successfulWitness(kind: property.Kind, condition: bool) bool {
    return switch (kind) {
        .@"unreachable" => !condition,
        else => condition,
    };
}

fn escapeHtmlAlloc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var output: std.ArrayListUnmanaged(u8) = .empty;
    errdefer output.deinit(allocator);
    for (input) |byte| switch (byte) {
        '&' => try output.appendSlice(allocator, "&amp;"),
        '<' => try output.appendSlice(allocator, "&lt;"),
        '>' => try output.appendSlice(allocator, "&gt;"),
        else => try output.append(allocator, byte),
    };
    return output.toOwnedSlice(allocator);
}

test "results API preserves rich property evidence and renders static HTML" {
    const observation = @import("observation.zig");
    const declarations = [_]property.Declaration{
        .{ .id = 10, .name = "safe", .kind = .always },
        .{ .id = 20, .name = "declared-only", .kind = .reachable },
    };
    var history = try trace.Trace.init(std.testing.allocator, .{
        .system = "test",
        .scenario = "results",
        .scenario_version = 1,
        .source_revision = "abc",
    }, .{ .transition_budget = 1 });
    defer history.deinit();
    const digest = observation.digestFeatures(&.{});
    try history.addObservation(.{ .index = 0, .digest = digest, .features = &.{} });
    try history.addChoice(.{ .site_id = 1, .site_name = "results", .occurrence = 0, .enabled_ids = &.{2}, .selected_id = 2 });
    try history.addTransition(.{ .index = 1, .id = 2, .name = "step", .kind = .workload });
    try history.addObservation(.{ .index = 1, .digest = digest, .features = &.{} });
    try history.addProperty(.{ .index = 1, .property_id = 10, .name = "safe", .kind = .always, .condition = false, .details = "first bad witness" });
    try history.addFailure(.{ .index = 1, .class = .property, .property_id = 10, .identity = "safe", .fingerprint = 99, .observation_digest = digest });
    history.summary = .{ .transitions = 1, .final_observation_digest = digest, .property_failures = 1 };

    var results = try Results.build(std.testing.allocator, &history, &declarations, .{
        .corpus_entries = 3,
        .quarantined_entries = 1,
        .artifacts = &.{"failure.voprtrace"},
    });
    defer results.deinit();
    try std.testing.expect(results.properties[0].failed);
    try std.testing.expectEqualStrings("first bad witness", results.properties[0].first_failure_details);
    try std.testing.expect(results.properties[1].declared_never_encountered);
    const json = try results.renderJsonAlloc(std.testing.allocator);
    defer std.testing.allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"format\":\"vopr-results-v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "declared_never_encountered") != null);
    const html = try results.renderHtmlAlloc(std.testing.allocator);
    defer std.testing.allocator.free(html);
    try std.testing.expect(std.mem.startsWith(u8, html, "<!doctype html>"));
}
