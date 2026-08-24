// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Persistent, deterministic local index for VOPR results.
//!
//! The index deliberately owns a small relational projection instead of raw
//! result blobs. This makes run, revision, property, fingerprint, corpus,
//! quarantine, artifact, and budget queries stable across result versions.

const std = @import("std");
const ids = @import("id.zig");
const property = @import("property.zig");
const report = @import("report.zig");
const trace = @import("trace.zig");

pub const format = "vopr-run-index-v1";
pub const query_format = "vopr-run-index-query-v1";

pub const Budget = struct {
    transition_limit: ?u64 = null,
    transitions_consumed: u64,
    resource_limit: ?u64 = null,
    resources_consumed: u64 = 0,
    histories_limit: ?u64 = null,
    histories_consumed: u64,
};

pub const Run = struct {
    run_id: []const u8,
    history_id: ?[]const u8 = null,
    aggregate: bool,
    system: []const u8,
    scenario: []const u8,
    scenario_version: ?u32 = null,
    source_revision: []const u8,
    target: []const u8,
    optimize: []const u8,
    budget: Budget,
    transitions: u64,
    failures: u64,
    corpus_entries: u64,
    quarantined_entries: u64,
};

pub const PropertyStatus = enum { pass, fail, not_reached };

pub const Property = struct {
    run_id: []const u8,
    history_id: ?[]const u8 = null,
    property_id: ids.StableId,
    name: []const u8,
    kind: ?property.Kind = null,
    status: PropertyStatus,
    evaluations: u64,
    true_count: u64,
    false_count: u64,
};

pub const Fingerprint = struct {
    run_id: []const u8,
    history_id: ?[]const u8 = null,
    fingerprint: ids.StableId,
    index: ?u64 = null,
    class: ?trace.FailureClass = null,
    first_history: ?u64 = null,
    smallest_transitions: ?u64 = null,
};

pub const ArtifactKind = enum {
    trace,
    flight,
    recipe,
    reduced_trace,
    results,
    report,
    corpus_entry,
    quarantine,
    other,
};

pub const Artifact = struct {
    run_id: []const u8,
    history_id: ?[]const u8 = null,
    path: []const u8,
    kind: ArtifactKind,
    quarantined: bool = false,
};

pub const CorpusState = enum { retained, quarantined };

pub const Query = struct {
    run_id: ?[]const u8 = null,
    source_revision: ?[]const u8 = null,
    scenario: ?[]const u8 = null,
    property_id: ?ids.StableId = null,
    property_name: ?[]const u8 = null,
    fingerprint: ?ids.StableId = null,
    corpus_state: ?CorpusState = null,
    artifact_contains: ?[]const u8 = null,
    min_transitions_consumed: ?u64 = null,
    max_transitions_consumed: ?u64 = null,
    min_resources_consumed: ?u64 = null,
    max_resources_consumed: ?u64 = null,
    min_histories_consumed: ?u64 = null,
    limit: usize = std.math.maxInt(usize),

    pub fn validate(self: Query) !void {
        if (self.limit == 0) return error.InvalidRunIndexQueryLimit;
        if (self.min_transitions_consumed != null and self.max_transitions_consumed != null and
            self.min_transitions_consumed.? > self.max_transitions_consumed.?) return error.InvalidRunIndexTransitionRange;
        if (self.min_resources_consumed != null and self.max_resources_consumed != null and
            self.min_resources_consumed.? > self.max_resources_consumed.?) return error.InvalidRunIndexResourceRange;
    }
};

pub const Index = struct {
    allocator: std.mem.Allocator,
    runs: std.ArrayListUnmanaged(Run) = .empty,
    properties: std.ArrayListUnmanaged(Property) = .empty,
    fingerprints: std.ArrayListUnmanaged(Fingerprint) = .empty,
    artifacts: std.ArrayListUnmanaged(Artifact) = .empty,

    pub fn init(allocator: std.mem.Allocator) Index {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Index) void {
        for (self.runs.items) |value| freeRun(self.allocator, value);
        for (self.properties.items) |value| freeProperty(self.allocator, value);
        for (self.fingerprints.items) |value| freeFingerprint(self.allocator, value);
        for (self.artifacts.items) |value| freeArtifact(self.allocator, value);
        self.runs.deinit(self.allocator);
        self.properties.deinit(self.allocator);
        self.fingerprints.deinit(self.allocator);
        self.artifacts.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn ingestResults(self: *Index, results: *const report.Results) !bool {
        if (self.findRun(results.run_id, results.history_id)) |existing| {
            if (!std.mem.eql(u8, existing.source_revision, results.source_revision) or
                !std.mem.eql(u8, existing.scenario, results.scenario))
                return error.ConflictingRunIndexEntry;
            return false;
        }
        try self.appendRun(.{
            .run_id = results.run_id,
            .history_id = results.history_id,
            .aggregate = false,
            .system = results.system,
            .scenario = results.scenario,
            .scenario_version = results.scenario_version,
            .source_revision = results.source_revision,
            .target = results.target,
            .optimize = results.optimize,
            .budget = .{
                .transition_limit = results.budget.transition_limit,
                .transitions_consumed = results.budget.transitions_consumed,
                .resource_limit = results.budget.resource_limit,
                .resources_consumed = results.budget.resources_consumed,
                .histories_limit = results.budget.histories_limit,
                .histories_consumed = results.budget.histories_consumed,
            },
            .transitions = results.summary.transitions,
            .failures = results.failures.len,
            .corpus_entries = results.corpus_entries,
            .quarantined_entries = results.quarantined_entries,
        });
        errdefer self.removeLastRunRelations(results.run_id, results.history_id);
        for (results.properties) |value| try self.appendProperty(.{
            .run_id = results.run_id,
            .history_id = results.history_id,
            .property_id = value.property_id,
            .name = value.name,
            .kind = value.kind,
            .status = if (value.failed) .fail else if (value.declared_never_encountered) .not_reached else .pass,
            .evaluations = value.evaluations,
            .true_count = value.true_count,
            .false_count = value.false_count,
        });
        for (results.failures) |value| try self.appendFingerprint(.{
            .run_id = results.run_id,
            .history_id = results.history_id,
            .fingerprint = value.fingerprint,
            .index = value.index,
            .class = value.class,
        });
        for (results.artifacts) |path| try self.appendArtifact(.{
            .run_id = results.run_id,
            .history_id = results.history_id,
            .path = path,
            .kind = inferArtifactKind(path),
            .quarantined = std.mem.indexOf(u8, path, "quarantine") != null,
        });
        return true;
    }

    /// Ingest either `vopr-results-v1`, `vopr-run-results-v1`, or another
    /// `vopr-run-index-v1` document transactionally.
    pub fn ingestJson(self: *Index, encoded: []const u8) !u64 {
        var parsed_format = try std.json.parseFromSlice(struct { format: []const u8 }, self.allocator, encoded, .{ .ignore_unknown_fields = true });
        defer parsed_format.deinit();
        var incoming = Index.init(self.allocator);
        defer incoming.deinit();
        if (std.mem.eql(u8, parsed_format.value.format, report.format)) {
            try incoming.ingestResultsJson(encoded);
        } else if (std.mem.eql(u8, parsed_format.value.format, report.aggregate_format)) {
            try incoming.ingestAggregateJson(encoded);
        } else if (std.mem.eql(u8, parsed_format.value.format, format)) {
            try incoming.ingestIndexJson(encoded);
        } else return error.UnsupportedRunIndexInputFormat;
        try incoming.validateRelationships();
        const original_lengths = .{
            self.runs.items.len,
            self.properties.items.len,
            self.fingerprints.items.len,
            self.artifacts.items.len,
        };
        errdefer self.rollbackTo(original_lengths);
        var added: u64 = 0;
        for (incoming.runs.items) |run| {
            if (self.findRun(run.run_id, run.history_id)) |existing| {
                if (!sameRun(existing, run)) return error.ConflictingRunIndexEntry;
                continue;
            }
            try self.cloneRunRelations(&incoming, run);
            added += 1;
        }
        return added;
    }

    pub fn addArtifact(
        self: *Index,
        run_id: []const u8,
        history_id: ?[]const u8,
        path: []const u8,
        kind: ArtifactKind,
        quarantined: bool,
    ) !void {
        if (self.findRun(run_id, history_id) == null) return error.RunIndexRunMissing;
        for (self.artifacts.items) |existing| if (sameKey(existing.run_id, existing.history_id, run_id, history_id) and
            existing.kind == kind and std.mem.eql(u8, existing.path, path)) return;
        try self.appendArtifact(.{
            .run_id = run_id,
            .history_id = history_id,
            .path = path,
            .kind = kind,
            .quarantined = quarantined,
        });
    }

    pub fn canonicalize(self: *Index) void {
        std.mem.sort(Run, self.runs.items, {}, lessRun);
        std.mem.sort(Property, self.properties.items, {}, lessProperty);
        std.mem.sort(Fingerprint, self.fingerprints.items, {}, lessFingerprint);
        std.mem.sort(Artifact, self.artifacts.items, {}, lessArtifact);
    }

    pub fn renderJsonAlloc(self: *Index, allocator: std.mem.Allocator) ![]u8 {
        self.canonicalize();
        return std.json.Stringify.valueAlloc(allocator, .{
            .format = format,
            .runs = self.runs.items,
            .properties = self.properties.items,
            .fingerprints = self.fingerprints.items,
            .artifacts = self.artifacts.items,
        }, .{ .whitespace = .indent_2 });
    }

    pub fn renderQueryJsonAlloc(self: *Index, allocator: std.mem.Allocator, query: Query) ![]u8 {
        try query.validate();
        self.canonicalize();
        var runs: std.ArrayListUnmanaged(Run) = .empty;
        defer runs.deinit(allocator);
        for (self.runs.items) |run| {
            if (!self.matchesQuery(run, query)) continue;
            try runs.append(allocator, run);
            if (runs.items.len == query.limit) break;
        }
        var properties: std.ArrayListUnmanaged(Property) = .empty;
        defer properties.deinit(allocator);
        var fingerprints: std.ArrayListUnmanaged(Fingerprint) = .empty;
        defer fingerprints.deinit(allocator);
        var artifacts: std.ArrayListUnmanaged(Artifact) = .empty;
        defer artifacts.deinit(allocator);
        for (runs.items) |run| {
            for (self.properties.items) |value| if (sameKey(value.run_id, value.history_id, run.run_id, run.history_id))
                try properties.append(allocator, value);
            for (self.fingerprints.items) |value| if (sameKey(value.run_id, value.history_id, run.run_id, run.history_id))
                try fingerprints.append(allocator, value);
            for (self.artifacts.items) |value| if (sameKey(value.run_id, value.history_id, run.run_id, run.history_id))
                try artifacts.append(allocator, value);
        }
        return std.json.Stringify.valueAlloc(allocator, .{
            .format = query_format,
            .query = query,
            .counts = .{
                .runs = runs.items.len,
                .properties = properties.items.len,
                .fingerprints = fingerprints.items.len,
                .artifacts = artifacts.items.len,
            },
            .runs = runs.items,
            .properties = properties.items,
            .fingerprints = fingerprints.items,
            .artifacts = artifacts.items,
        }, .{ .whitespace = .indent_2 });
    }

    pub fn renderQueryHtmlAlloc(self: *Index, allocator: std.mem.Allocator, query: Query) ![]u8 {
        const json = try self.renderQueryJsonAlloc(allocator, query);
        defer allocator.free(json);
        const escaped = try escapeHtmlAlloc(allocator, json);
        defer allocator.free(escaped);
        return std.fmt.allocPrint(
            allocator,
            "<!doctype html><html><head><meta charset=\"utf-8\"><title>VOPR local run index</title>" ++
                "<style>body{{font:14px ui-monospace,monospace;max-width:1200px;margin:2rem auto;padding:0 1rem}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;background:#111;color:#eee;padding:1rem;border-radius:6px}}</style>" ++
                "</head><body><h1>VOPR local run index</h1><p>{d} indexed runs; {d} properties; {d} fingerprints; {d} artifacts.</p><pre>{s}</pre></body></html>",
            .{ self.runs.items.len, self.properties.items.len, self.fingerprints.items.len, self.artifacts.items.len, escaped },
        );
    }

    fn matchesQuery(self: *const Index, run: Run, query: Query) bool {
        if (query.run_id) |value| if (!std.mem.eql(u8, run.run_id, value)) return false;
        if (query.source_revision) |value| if (!std.mem.eql(u8, run.source_revision, value)) return false;
        if (query.scenario) |value| if (!std.mem.eql(u8, run.scenario, value)) return false;
        if (query.property_id != null or query.property_name != null) {
            var matched = false;
            for (self.properties.items) |value| {
                if (!sameKey(value.run_id, value.history_id, run.run_id, run.history_id)) continue;
                if (query.property_id) |expected| if (value.property_id != expected) continue;
                if (query.property_name) |expected| if (!std.mem.eql(u8, value.name, expected)) continue;
                matched = true;
                break;
            }
            if (!matched) return false;
        }
        if (query.fingerprint) |expected| {
            var matched = false;
            for (self.fingerprints.items) |value| if (sameKey(value.run_id, value.history_id, run.run_id, run.history_id) and
                value.fingerprint == expected)
            {
                matched = true;
                break;
            };
            if (!matched) return false;
        }
        if (query.corpus_state) |state| switch (state) {
            .retained => if (run.corpus_entries == 0) return false,
            .quarantined => if (run.quarantined_entries == 0) return false,
        };
        if (query.artifact_contains) |needle| {
            var matched = false;
            for (self.artifacts.items) |value| if (sameKey(value.run_id, value.history_id, run.run_id, run.history_id) and
                std.mem.indexOf(u8, value.path, needle) != null)
            {
                matched = true;
                break;
            };
            if (!matched) return false;
        }
        if (query.min_transitions_consumed) |minimum| if (run.budget.transitions_consumed < minimum) return false;
        if (query.max_transitions_consumed) |maximum| if (run.budget.transitions_consumed > maximum) return false;
        if (query.min_resources_consumed) |minimum| if (run.budget.resources_consumed < minimum) return false;
        if (query.max_resources_consumed) |maximum| if (run.budget.resources_consumed > maximum) return false;
        if (query.min_histories_consumed) |minimum| if (run.budget.histories_consumed < minimum) return false;
        return true;
    }

    fn findRun(self: *const Index, run_id: []const u8, history_id: ?[]const u8) ?Run {
        for (self.runs.items) |run| if (sameKey(run.run_id, run.history_id, run_id, history_id)) return run;
        return null;
    }

    fn validateRelationships(self: *const Index) !void {
        for (self.runs.items, 0..) |run, index| {
            if (run.run_id.len == 0 or run.system.len == 0 or run.scenario.len == 0 or run.source_revision.len == 0)
                return error.InvalidRunIndexEntry;
            for (self.runs.items[0..index]) |prior| if (sameKey(prior.run_id, prior.history_id, run.run_id, run.history_id))
                return error.DuplicateRunIndexEntry;
        }
        for (self.properties.items) |value| {
            if (value.property_id == 0 or value.name.len == 0 or self.findRun(value.run_id, value.history_id) == null)
                return error.OrphanRunIndexProperty;
        }
        for (self.fingerprints.items) |value| {
            if (value.fingerprint == 0 or self.findRun(value.run_id, value.history_id) == null)
                return error.OrphanRunIndexFingerprint;
        }
        for (self.artifacts.items) |value| {
            if (value.path.len == 0 or self.findRun(value.run_id, value.history_id) == null)
                return error.OrphanRunIndexArtifact;
        }
    }

    fn rollbackTo(self: *Index, lengths: struct { usize, usize, usize, usize }) void {
        while (self.artifacts.items.len > lengths[3]) freeArtifact(self.allocator, self.artifacts.pop().?);
        while (self.fingerprints.items.len > lengths[2]) freeFingerprint(self.allocator, self.fingerprints.pop().?);
        while (self.properties.items.len > lengths[1]) freeProperty(self.allocator, self.properties.pop().?);
        while (self.runs.items.len > lengths[0]) freeRun(self.allocator, self.runs.pop().?);
    }

    fn cloneRunRelations(self: *Index, source: *const Index, run: Run) !void {
        try self.appendRun(run);
        errdefer self.removeLastRunRelations(run.run_id, run.history_id);
        for (source.properties.items) |value| if (sameKey(value.run_id, value.history_id, run.run_id, run.history_id))
            try self.appendProperty(value);
        for (source.fingerprints.items) |value| if (sameKey(value.run_id, value.history_id, run.run_id, run.history_id))
            try self.appendFingerprint(value);
        for (source.artifacts.items) |value| if (sameKey(value.run_id, value.history_id, run.run_id, run.history_id))
            try self.appendArtifact(value);
    }

    fn removeLastRunRelations(self: *Index, run_id: []const u8, history_id: ?[]const u8) void {
        while (self.properties.items.len > 0) {
            const value = self.properties.items[self.properties.items.len - 1];
            if (!sameKey(value.run_id, value.history_id, run_id, history_id)) break;
            freeProperty(self.allocator, self.properties.pop().?);
        }
        while (self.fingerprints.items.len > 0) {
            const value = self.fingerprints.items[self.fingerprints.items.len - 1];
            if (!sameKey(value.run_id, value.history_id, run_id, history_id)) break;
            freeFingerprint(self.allocator, self.fingerprints.pop().?);
        }
        while (self.artifacts.items.len > 0) {
            const value = self.artifacts.items[self.artifacts.items.len - 1];
            if (!sameKey(value.run_id, value.history_id, run_id, history_id)) break;
            freeArtifact(self.allocator, self.artifacts.pop().?);
        }
        if (self.runs.items.len > 0 and sameKey(
            self.runs.items[self.runs.items.len - 1].run_id,
            self.runs.items[self.runs.items.len - 1].history_id,
            run_id,
            history_id,
        )) freeRun(self.allocator, self.runs.pop().?);
    }

    fn appendRun(self: *Index, value: Run) !void {
        const cloned = try cloneRun(self.allocator, value);
        errdefer freeRun(self.allocator, cloned);
        try self.runs.append(self.allocator, cloned);
    }
    fn appendProperty(self: *Index, value: Property) !void {
        const cloned = try cloneProperty(self.allocator, value);
        errdefer freeProperty(self.allocator, cloned);
        try self.properties.append(self.allocator, cloned);
    }
    fn appendFingerprint(self: *Index, value: Fingerprint) !void {
        const cloned = try cloneFingerprint(self.allocator, value);
        errdefer freeFingerprint(self.allocator, cloned);
        try self.fingerprints.append(self.allocator, cloned);
    }
    fn appendArtifact(self: *Index, value: Artifact) !void {
        for (self.artifacts.items) |existing| if (sameKey(existing.run_id, existing.history_id, value.run_id, value.history_id) and
            existing.kind == value.kind and existing.quarantined == value.quarantined and
            std.mem.eql(u8, existing.path, value.path)) return;
        const cloned = try cloneArtifact(self.allocator, value);
        errdefer freeArtifact(self.allocator, cloned);
        try self.artifacts.append(self.allocator, cloned);
    }

    fn ingestResultsJson(self: *Index, encoded: []const u8) !void {
        const PropertyWire = struct {
            property_id: ids.StableId,
            name: []const u8,
            kind: property.Kind,
            evaluations: u64,
            true_count: u64,
            false_count: u64,
            failed: bool,
            declared_never_encountered: bool,
        };
        const FailureWire = struct {
            index: u64,
            class: trace.FailureClass,
            fingerprint: ids.StableId,
        };
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
            budget: report.BudgetUsage,
            summary: trace.Summary,
            corpus_entries: u64,
            quarantined_entries: u64,
            properties: []const PropertyWire,
            failures: []const FailureWire,
            artifacts: []const []const u8,
        };
        var parsed = try std.json.parseFromSlice(Wire, self.allocator, encoded, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const value = parsed.value;
        try self.appendRun(.{
            .run_id = value.run_id,
            .history_id = value.history_id,
            .aggregate = false,
            .system = value.system,
            .scenario = value.scenario,
            .scenario_version = value.scenario_version,
            .source_revision = value.source_revision,
            .target = value.target,
            .optimize = value.optimize,
            .budget = .{
                .transition_limit = value.budget.transition_limit,
                .transitions_consumed = value.budget.transitions_consumed,
                .resource_limit = value.budget.resource_limit,
                .resources_consumed = value.budget.resources_consumed,
                .histories_limit = value.budget.histories_limit,
                .histories_consumed = value.budget.histories_consumed,
            },
            .transitions = value.summary.transitions,
            .failures = value.failures.len,
            .corpus_entries = value.corpus_entries,
            .quarantined_entries = value.quarantined_entries,
        });
        for (value.properties) |item| try self.appendProperty(.{
            .run_id = value.run_id,
            .history_id = value.history_id,
            .property_id = item.property_id,
            .name = item.name,
            .kind = item.kind,
            .status = if (item.failed) .fail else if (item.declared_never_encountered) .not_reached else .pass,
            .evaluations = item.evaluations,
            .true_count = item.true_count,
            .false_count = item.false_count,
        });
        for (value.failures) |item| try self.appendFingerprint(.{
            .run_id = value.run_id,
            .history_id = value.history_id,
            .fingerprint = item.fingerprint,
            .index = item.index,
            .class = item.class,
        });
        for (value.artifacts) |path| try self.appendArtifact(.{
            .run_id = value.run_id,
            .history_id = value.history_id,
            .path = path,
            .kind = inferArtifactKind(path),
            .quarantined = std.mem.indexOf(u8, path, "quarantine") != null,
        });
    }

    fn ingestAggregateJson(self: *Index, encoded: []const u8) !void {
        const BudgetWire = struct {
            histories_limit: u64,
            histories_consumed: u64,
            transition_limit_per_history: u64,
            transitions_consumed: u64,
        };
        const HistoriesWire = struct { clean: u64, failed: u64, replay_divergences: u64, harness_errors: u64, exact_replays: u64 };
        const CorpusWire = struct { entries: u64, quarantined: u64, retained: u64 };
        const PropertyWire = struct {
            property_id: ids.StableId,
            name: []const u8,
            status: []const u8,
            evaluations: u64,
            ever_true: bool,
            ever_false: bool,
        };
        const FailureWire = struct {
            fingerprint: ids.StableId,
            first_history: u64,
            first_artifact: []const u8,
            smallest_history: u64,
            smallest_transitions: u64,
            smallest_artifact: []const u8,
        };
        const Wire = struct {
            format: []const u8,
            run_id: ?[]const u8 = null,
            scenario: []const u8,
            source_revision: []const u8 = "unknown",
            target: []const u8 = "native",
            optimize: []const u8 = "unknown",
            budget: BudgetWire,
            histories: HistoriesWire,
            corpus: CorpusWire,
            properties: []const PropertyWire,
            failures: []const FailureWire,
            artifacts: []const []const u8,
        };
        var parsed = try std.json.parseFromSlice(Wire, self.allocator, encoded, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const value = parsed.value;
        var derived_run_id_buffer: [32]u8 = undefined;
        const run_id = value.run_id orelse try std.fmt.bufPrint(
            &derived_run_id_buffer,
            "legacy-{x:0>16}",
            .{ids.digest(encoded)},
        );
        try self.appendRun(.{
            .run_id = run_id,
            .aggregate = true,
            .system = "antfly",
            .scenario = value.scenario,
            .source_revision = value.source_revision,
            .target = value.target,
            .optimize = value.optimize,
            .budget = .{
                .transition_limit = value.budget.transition_limit_per_history,
                .transitions_consumed = value.budget.transitions_consumed,
                .histories_limit = value.budget.histories_limit,
                .histories_consumed = value.budget.histories_consumed,
            },
            .transitions = value.budget.transitions_consumed,
            .failures = value.histories.failed,
            .corpus_entries = value.corpus.entries,
            .quarantined_entries = value.corpus.quarantined,
        });
        for (value.properties) |item| try self.appendProperty(.{
            .run_id = run_id,
            .property_id = item.property_id,
            .name = item.name,
            .status = if (std.mem.eql(u8, item.status, "fail")) .fail else if (std.mem.eql(u8, item.status, "not-reached")) .not_reached else .pass,
            .evaluations = item.evaluations,
            .true_count = @intFromBool(item.ever_true),
            .false_count = @intFromBool(item.ever_false),
        });
        for (value.failures) |item| {
            try self.appendFingerprint(.{
                .run_id = run_id,
                .fingerprint = item.fingerprint,
                .first_history = item.first_history,
                .smallest_transitions = item.smallest_transitions,
            });
            try self.appendArtifact(.{ .run_id = run_id, .path = item.first_artifact, .kind = .corpus_entry });
            if (!std.mem.eql(u8, item.first_artifact, item.smallest_artifact))
                try self.appendArtifact(.{ .run_id = run_id, .path = item.smallest_artifact, .kind = .reduced_trace });
        }
        for (value.artifacts) |path| try self.appendArtifact(.{
            .run_id = run_id,
            .path = path,
            .kind = inferAggregateArtifactKind(path),
            .quarantined = std.mem.indexOf(u8, path, "quarantine") != null,
        });
    }

    fn ingestIndexJson(self: *Index, encoded: []const u8) !void {
        const Wire = struct {
            format: []const u8,
            runs: []const Run,
            properties: []const Property,
            fingerprints: []const Fingerprint,
            artifacts: []const Artifact,
        };
        var parsed = try std.json.parseFromSlice(Wire, self.allocator, encoded, .{ .ignore_unknown_fields = false });
        defer parsed.deinit();
        for (parsed.value.runs) |value| try self.appendRun(value);
        for (parsed.value.properties) |value| try self.appendProperty(value);
        for (parsed.value.fingerprints) |value| try self.appendFingerprint(value);
        for (parsed.value.artifacts) |value| try self.appendArtifact(value);
    }
};

fn cloneRun(allocator: std.mem.Allocator, value: Run) !Run {
    const run_id = try allocator.dupe(u8, value.run_id);
    errdefer allocator.free(run_id);
    const history_id = if (value.history_id) |text| try allocator.dupe(u8, text) else null;
    errdefer if (history_id) |text| allocator.free(text);
    const system = try allocator.dupe(u8, value.system);
    errdefer allocator.free(system);
    const scenario = try allocator.dupe(u8, value.scenario);
    errdefer allocator.free(scenario);
    const revision = try allocator.dupe(u8, value.source_revision);
    errdefer allocator.free(revision);
    const target = try allocator.dupe(u8, value.target);
    errdefer allocator.free(target);
    const optimize = try allocator.dupe(u8, value.optimize);
    var result = value;
    result.run_id = run_id;
    result.history_id = history_id;
    result.system = system;
    result.scenario = scenario;
    result.source_revision = revision;
    result.target = target;
    result.optimize = optimize;
    return result;
}

fn freeRun(allocator: std.mem.Allocator, value: Run) void {
    allocator.free(value.run_id);
    if (value.history_id) |text| allocator.free(text);
    allocator.free(value.system);
    allocator.free(value.scenario);
    allocator.free(value.source_revision);
    allocator.free(value.target);
    allocator.free(value.optimize);
}

fn cloneProperty(allocator: std.mem.Allocator, value: Property) !Property {
    const run_id = try allocator.dupe(u8, value.run_id);
    errdefer allocator.free(run_id);
    const history_id = if (value.history_id) |text| try allocator.dupe(u8, text) else null;
    errdefer if (history_id) |text| allocator.free(text);
    const name = try allocator.dupe(u8, value.name);
    var result = value;
    result.run_id = run_id;
    result.history_id = history_id;
    result.name = name;
    return result;
}

fn freeProperty(allocator: std.mem.Allocator, value: Property) void {
    allocator.free(value.run_id);
    if (value.history_id) |text| allocator.free(text);
    allocator.free(value.name);
}

fn cloneFingerprint(allocator: std.mem.Allocator, value: Fingerprint) !Fingerprint {
    const run_id = try allocator.dupe(u8, value.run_id);
    errdefer allocator.free(run_id);
    const history_id = if (value.history_id) |text| try allocator.dupe(u8, text) else null;
    var result = value;
    result.run_id = run_id;
    result.history_id = history_id;
    return result;
}

fn freeFingerprint(allocator: std.mem.Allocator, value: Fingerprint) void {
    allocator.free(value.run_id);
    if (value.history_id) |text| allocator.free(text);
}

fn cloneArtifact(allocator: std.mem.Allocator, value: Artifact) !Artifact {
    const run_id = try allocator.dupe(u8, value.run_id);
    errdefer allocator.free(run_id);
    const history_id = if (value.history_id) |text| try allocator.dupe(u8, text) else null;
    errdefer if (history_id) |text| allocator.free(text);
    const path = try allocator.dupe(u8, value.path);
    var result = value;
    result.run_id = run_id;
    result.history_id = history_id;
    result.path = path;
    return result;
}

fn freeArtifact(allocator: std.mem.Allocator, value: Artifact) void {
    allocator.free(value.run_id);
    if (value.history_id) |text| allocator.free(text);
    allocator.free(value.path);
}

fn sameKey(lhs_run: []const u8, lhs_history: ?[]const u8, rhs_run: []const u8, rhs_history: ?[]const u8) bool {
    if (!std.mem.eql(u8, lhs_run, rhs_run)) return false;
    if (lhs_history == null or rhs_history == null) return lhs_history == null and rhs_history == null;
    return std.mem.eql(u8, lhs_history.?, rhs_history.?);
}

fn sameRun(lhs: Run, rhs: Run) bool {
    return sameKey(lhs.run_id, lhs.history_id, rhs.run_id, rhs.history_id) and
        std.mem.eql(u8, lhs.source_revision, rhs.source_revision) and
        std.mem.eql(u8, lhs.scenario, rhs.scenario) and
        lhs.transitions == rhs.transitions and lhs.failures == rhs.failures;
}

fn compareOptionalText(lhs: ?[]const u8, rhs: ?[]const u8) std.math.Order {
    if (lhs == null or rhs == null) return if (lhs == null and rhs == null) .eq else if (lhs == null) .lt else .gt;
    return std.mem.order(u8, lhs.?, rhs.?);
}

fn lessRun(_: void, lhs: Run, rhs: Run) bool {
    const run_order = std.mem.order(u8, lhs.run_id, rhs.run_id);
    if (run_order != .eq) return run_order == .lt;
    return compareOptionalText(lhs.history_id, rhs.history_id) == .lt;
}

fn lessProperty(_: void, lhs: Property, rhs: Property) bool {
    if (!sameKey(lhs.run_id, lhs.history_id, rhs.run_id, rhs.history_id)) {
        const run_order = std.mem.order(u8, lhs.run_id, rhs.run_id);
        if (run_order != .eq) return run_order == .lt;
        return compareOptionalText(lhs.history_id, rhs.history_id) == .lt;
    }
    return lhs.property_id < rhs.property_id;
}

fn lessFingerprint(_: void, lhs: Fingerprint, rhs: Fingerprint) bool {
    if (!sameKey(lhs.run_id, lhs.history_id, rhs.run_id, rhs.history_id)) {
        const run_order = std.mem.order(u8, lhs.run_id, rhs.run_id);
        if (run_order != .eq) return run_order == .lt;
        return compareOptionalText(lhs.history_id, rhs.history_id) == .lt;
    }
    return lhs.fingerprint < rhs.fingerprint;
}

fn lessArtifact(_: void, lhs: Artifact, rhs: Artifact) bool {
    if (!sameKey(lhs.run_id, lhs.history_id, rhs.run_id, rhs.history_id)) {
        const run_order = std.mem.order(u8, lhs.run_id, rhs.run_id);
        if (run_order != .eq) return run_order == .lt;
        return compareOptionalText(lhs.history_id, rhs.history_id) == .lt;
    }
    const path_order = std.mem.order(u8, lhs.path, rhs.path);
    if (path_order != .eq) return path_order == .lt;
    return @intFromEnum(lhs.kind) < @intFromEnum(rhs.kind);
}

fn inferArtifactKind(path: []const u8) ArtifactKind {
    if (std.mem.indexOf(u8, path, "quarantine") != null) return .quarantine;
    if (std.mem.endsWith(u8, path, ".flight.json")) return .flight;
    if (std.mem.endsWith(u8, path, ".recipe.json")) return .recipe;
    if (std.mem.endsWith(u8, path, ".reduced.voprtrace")) return .reduced_trace;
    if (std.mem.endsWith(u8, path, ".voprtrace")) return .trace;
    if (std.mem.endsWith(u8, path, "results.json")) return .results;
    if (std.mem.endsWith(u8, path, ".html")) return .report;
    return .other;
}

fn inferAggregateArtifactKind(path: []const u8) ArtifactKind {
    const inferred = inferArtifactKind(path);
    return if (inferred == .trace) .corpus_entry else inferred;
}

fn escapeHtmlAlloc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var output: std.ArrayListUnmanaged(u8) = .empty;
    errdefer output.deinit(allocator);
    for (input) |byte| switch (byte) {
        '&' => try output.appendSlice(allocator, "&amp;"),
        '<' => try output.appendSlice(allocator, "&lt;"),
        '>' => try output.appendSlice(allocator, "&gt;"),
        '"' => try output.appendSlice(allocator, "&quot;"),
        else => try output.append(allocator, byte),
    };
    return output.toOwnedSlice(allocator);
}

test "run index merges results transactionally and queries every relation" {
    const observation = @import("observation.zig");
    var history = try trace.Trace.init(std.testing.allocator, .{
        .system = "antfly",
        .scenario = "index-test",
        .scenario_version = 1,
        .source_revision = "revision-a",
        .target = "native",
        .optimize = "Debug",
    }, .{ .transition_budget = 1 });
    defer history.deinit();
    const digest = observation.digestFeatures(&.{});
    try history.addObservation(.{ .index = 0, .digest = digest, .features = &.{} });
    try history.addChoice(.{ .site_id = 1, .site_name = "choice", .occurrence = 0, .enabled_ids = &.{2}, .selected_id = 2 });
    try history.addTransition(.{ .index = 1, .id = 2, .name = "step", .kind = .workload });
    try history.addObservation(.{ .index = 1, .digest = digest, .features = &.{} });
    try history.addProperty(.{ .index = 1, .property_id = 3, .name = "safe", .kind = .always, .condition = false, .details = "bad" });
    try history.addFailure(.{ .index = 1, .class = .property, .property_id = 3, .identity = "safe", .fingerprint = 4, .observation_digest = digest });
    history.summary = .{ .transitions = 1, .final_observation_digest = digest, .property_failures = 1 };
    const declarations = [_]property.Declaration{.{ .id = 3, .name = "safe", .kind = .always }};
    const artifact_paths = [_][]const u8{"failure.reduced.voprtrace"};
    var results = try report.Results.build(std.testing.allocator, &history, &declarations, .{
        .run_id = "run-a",
        .history_id = "history-a",
        .corpus_entries = 1,
        .quarantined_entries = 1,
        .artifacts = &artifact_paths,
    });
    defer results.deinit();
    var index = Index.init(std.testing.allocator);
    defer index.deinit();
    try std.testing.expect(try index.ingestResults(&results));
    try std.testing.expect(!(try index.ingestResults(&results)));
    const encoded = try index.renderJsonAlloc(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, format) != null);
    var reloaded = Index.init(std.testing.allocator);
    defer reloaded.deinit();
    try std.testing.expectEqual(@as(u64, 1), try reloaded.ingestJson(encoded));
    const roundtrip = try reloaded.renderJsonAlloc(std.testing.allocator);
    defer std.testing.allocator.free(roundtrip);
    try std.testing.expectEqualStrings(encoded, roundtrip);
    const queried = try reloaded.renderQueryJsonAlloc(std.testing.allocator, .{
        .source_revision = "revision-a",
        .property_id = 3,
        .fingerprint = 4,
        .corpus_state = .quarantined,
        .artifact_contains = "reduced",
        .min_transitions_consumed = 1,
        .max_transitions_consumed = 1,
    });
    defer std.testing.allocator.free(queried);
    try std.testing.expect(std.mem.indexOf(u8, queried, query_format) != null);
    try std.testing.expect(std.mem.indexOf(u8, queried, "run-a") != null);
    const html = try reloaded.renderQueryHtmlAlloc(std.testing.allocator, .{});
    defer std.testing.allocator.free(html);
    try std.testing.expect(std.mem.startsWith(u8, html, "<!doctype html>"));
}

test "run index ingests aggregate campaign results and legacy aggregate identities" {
    const aggregate_properties = [_]report.AggregateProperty{.{
        .property_id = 19,
        .name = "campaign.safe",
        .status = "pass",
        .evaluations = 8,
        .ever_true = true,
        .ever_false = false,
    }};
    const aggregate_failures = [_]report.AggregateFailure{.{
        .fingerprint = 23,
        .first_history = 2,
        .first_artifact = "history-2.voprtrace",
        .smallest_history = 7,
        .smallest_transitions = 3,
        .smallest_artifact = "history-7.voprtrace",
    }};
    const artifacts = [_][]const u8{ "results.json", "quarantine/index.json" };
    const aggregate = report.Aggregate{
        .run_id = "campaign-a",
        .scenario = "metadata",
        .source_revision = "revision-b",
        .target = "native",
        .optimize = "ReleaseSafe",
        .base_seed = 7,
        .histories_limit = 10,
        .histories_consumed = 10,
        .transition_limit_per_history = 20,
        .transitions_consumed = 80,
        .clean_histories = 9,
        .failed_histories = 1,
        .replay_divergences = 0,
        .harness_errors = 0,
        .exact_replays = 10,
        .corpus_entries = 4,
        .quarantined_entries = 1,
        .retained_entries = 4,
        .semantic_states = 12,
        .transition_kinds = 5,
        .faults_reached = 2,
        .workloads_reached = 3,
        .flight_recordings = 2,
        .flight_records = 14,
        .flight_records_dropped = 0,
        .properties = &aggregate_properties,
        .failures = &aggregate_failures,
        .artifacts = &artifacts,
    };
    const encoded = try aggregate.renderJsonAlloc(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    var index = Index.init(std.testing.allocator);
    defer index.deinit();
    try std.testing.expectEqual(@as(u64, 1), try index.ingestJson(encoded));
    try std.testing.expect(index.runs.items[0].aggregate);
    try std.testing.expectEqual(@as(u64, 80), index.runs.items[0].budget.transitions_consumed);
    try std.testing.expectEqual(@as(usize, 1), index.fingerprints.items.len);
    try std.testing.expectEqual(@as(usize, 4), index.artifacts.items.len);

    const legacy =
        \\{"format":"vopr-run-results-v1","scenario":"legacy","base_seed":1,"budget":{"histories_limit":1,"histories_consumed":1,"transition_limit_per_history":1,"transitions_consumed":1},"histories":{"clean":1,"failed":0,"replay_divergences":0,"harness_errors":0,"exact_replays":1},"corpus":{"entries":1,"quarantined":0,"retained":1},"properties":[],"failures":[],"artifacts":[]}
    ;
    try std.testing.expectEqual(@as(u64, 1), try index.ingestJson(legacy));
    try std.testing.expect(std.mem.startsWith(u8, index.runs.items[1].run_id, "legacy-"));
}

test "run index ingest merge and query are allocation-failure safe" {
    const Runner = struct {
        fn run(allocator: std.mem.Allocator) !void {
            const encoded =
                \\{"format":"vopr-run-results-v1","run_id":"allocation-run","scenario":"allocation","source_revision":"r1","target":"native","optimize":"Debug","base_seed":1,"budget":{"histories_limit":1,"histories_consumed":1,"transition_limit_per_history":1,"transitions_consumed":1},"histories":{"clean":1,"failed":0,"replay_divergences":0,"harness_errors":0,"exact_replays":1},"corpus":{"entries":1,"quarantined":0,"retained":1},"properties":[{"property_id":9,"name":"safe","status":"pass","evaluations":1,"ever_true":true,"ever_false":false}],"failures":[],"artifacts":["results.json"]}
            ;
            var index = Index.init(allocator);
            defer index.deinit();
            _ = try index.ingestJson(encoded);
            const persisted = try index.renderJsonAlloc(allocator);
            defer allocator.free(persisted);
            var merged = Index.init(allocator);
            defer merged.deinit();
            _ = try merged.ingestJson(persisted);
            const query = try merged.renderQueryJsonAlloc(allocator, .{ .property_id = 9 });
            defer allocator.free(query);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}
