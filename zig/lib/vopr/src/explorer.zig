// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Inspectable AFL-style semantic exploration loop.

const std = @import("std");
const choice = @import("choice.zig");
const corpus_mod = @import("corpus.zig");
const coverage_mod = @import("coverage.zig");
const runner = @import("runner.zig");
const trace = @import("trace.zig");

pub const Config = struct {
    system: []const u8 = "generic",
    histories: u64,
    transition_budget: u64,
    seed: u64,
    uniform_percent: u8 = 10,
    targets: []const coverage_mod.Target = &.{},

    pub fn validate(self: Config) !void {
        if (self.histories == 0) return error.InvalidHistoryBudget;
        if (self.transition_budget == 0) return error.InvalidTransitionBudget;
        if (self.uniform_percent > 100) return error.InvalidUniformPercent;
    }
};

pub const Report = struct {
    histories: u64 = 0,
    generated: u64 = 0,
    mutated: u64 = 0,
    retained: u64 = 0,
    failures: u64 = 0,
    semantic_features: usize = 0,
    target_hits: u64 = 0,
};

pub fn Campaign(comptime Scenario: type) type {
    return struct {
        allocator: std.mem.Allocator,
        config: Config,
        prng: std.Random.DefaultPrng,
        coverage: coverage_mod.Tracker,
        corpus: corpus_mod.Corpus,
        report: Report = .{},

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
            try config.validate();
            return .{
                .allocator = allocator,
                .config = config,
                .prng = std.Random.DefaultPrng.init(config.seed),
                .coverage = coverage_mod.Tracker.init(allocator),
                .corpus = corpus_mod.Corpus.init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            self.corpus.deinit();
            self.coverage.deinit();
            self.* = undefined;
        }

        pub fn run(self: *Self) !void {
            while (self.report.histories < self.config.histories) {
                const use_uniform = self.corpus.entries.items.len == 0 or
                    self.prng.random().uintLessThan(u8, 100) < self.config.uniform_percent;
                if (use_uniform) {
                    try self.runGenerated(self.prng.random().int(u64));
                } else {
                    const parent = try self.corpus.select(self.prng.random());
                    if (!try self.runMutation(parent)) try self.runGenerated(self.prng.random().int(u64));
                }
                self.report.histories += 1;
            }
            self.report.semantic_features = self.coverage.hits.count();
        }

        fn runGenerated(self: *Self, seed: u64) !void {
            var seeded = choice.Seeded.init(seed);
            var artifact = try runner.run(Scenario, self.allocator, seeded.source(), .{
                .system = self.config.system,
                .seed = seed,
                .transition_budget = self.config.transition_budget,
            });
            defer artifact.deinit();
            self.report.generated += 1;
            try self.consider(&artifact, null);
        }

        fn runMutation(self: *Self, parent_index: usize) !bool {
            const bytes = self.corpus.entries.items[parent_index].bytes;
            var parent = try trace.parseAlloc(self.allocator, bytes);
            defer parent.deinit();
            var mutable_count: usize = 0;
            for (parent.choices.items) |record| mutable_count += @intFromBool(record.enabled_ids.len > 1);
            if (mutable_count == 0) return false;
            var ordinal = self.prng.random().uintLessThan(usize, mutable_count);
            var mutation_index: usize = 0;
            for (parent.choices.items, 0..) |record, index| {
                if (record.enabled_ids.len <= 1) continue;
                if (ordinal == 0) {
                    mutation_index = index;
                    break;
                }
                ordinal -= 1;
            }
            const record = parent.choices.items[mutation_index];
            var alternatives: usize = 0;
            for (record.enabled_ids) |alternative| alternatives += @intFromBool(alternative != record.selected_id);
            var replacement_ordinal = self.prng.random().uintLessThan(usize, alternatives);
            var replacement = record.selected_id;
            for (record.enabled_ids) |alternative| {
                if (alternative == record.selected_id) continue;
                if (replacement_ordinal == 0) {
                    replacement = alternative;
                    break;
                }
                replacement_ordinal -= 1;
            }
            var mutating = choice.Mutating.init(parent.choices.items, mutation_index, replacement, self.prng.random().int(u64));
            var artifact = try runner.run(Scenario, self.allocator, mutating.source(), .{
                .system = self.config.system,
                .transition_budget = self.config.transition_budget,
            });
            defer artifact.deinit();
            self.report.mutated += 1;
            try self.consider(&artifact, parent_index);
            return true;
        }

        fn consider(self: *Self, artifact: *const trace.Trace, parent_index: ?usize) !void {
            var novelty = try self.coverage.observe(artifact);
            novelty.target_score = try coverage_mod.scoreTargets(self.allocator, artifact, self.config.targets);
            if (novelty.target_score > 0) self.report.target_hits += 1;
            const failed = artifact.failures.items.len > 0;
            if (failed) self.report.failures += 1;
            if (novelty.discovered == 0 and novelty.target_score == 0 and !failed and self.corpus.entries.items.len > 0) return;
            const added = try self.corpus.add(artifact, novelty);
            if (added.inserted) {
                self.report.retained += 1;
                if (parent_index) |index| try self.corpus.markProductive(index);
            }
        }
    };
}

test "campaign generates, mutates, and retains semantic histories" {
    const ToyScenario = @import("toy_scenario.zig").ToyScenario;
    var campaign = try Campaign(ToyScenario).init(std.testing.allocator, .{
        .system = "vopr-test",
        .histories = 8,
        .transition_budget = 4,
        .seed = 0x1234,
        .uniform_percent = 0,
    });
    defer campaign.deinit();
    try campaign.run();
    try std.testing.expectEqual(@as(u64, 8), campaign.report.histories);
    try std.testing.expect(campaign.report.generated >= 1);
    try std.testing.expect(campaign.report.mutated >= 1);
    try std.testing.expect(campaign.report.retained >= 1);
    try std.testing.expect(campaign.report.semantic_features > 0);
}

test "campaign retains and energizes target-state histories" {
    const ToyScenario = @import("toy_scenario.zig").ToyScenario;
    var campaign = try Campaign(ToyScenario).init(std.testing.allocator, .{
        .system = "vopr-test",
        .histories = 3,
        .transition_budget = 4,
        .seed = 0x55,
        .targets = &.{.{
            .feature_id = @import("id.zig").stable("observation", "toy.steps"),
            .value = 4,
            .weight = 8,
        }},
    });
    defer campaign.deinit();
    try campaign.run();
    try std.testing.expect(campaign.report.target_hits > 0);
    var has_target_energy = false;
    for (campaign.corpus.entries.items) |entry| has_target_energy = has_target_energy or entry.target_score > 0;
    try std.testing.expect(has_target_energy);
}
