// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Inspectable AFL-style semantic exploration loop.

const std = @import("std");
const choice = @import("choice.zig");
const corpus_mod = @import("corpus.zig");
const coverage_mod = @import("coverage.zig");
const event = @import("event.zig");
const ids = @import("id.zig");
const observation = @import("observation.zig");
const replay = @import("replay.zig");
const runner = @import("runner.zig");
const scheduler = @import("scheduler.zig");
const snapshot = @import("snapshot.zig");
const splice = @import("splice.zig");
const trace = @import("trace.zig");

pub const Config = struct {
    system: []const u8 = "generic",
    histories: u64,
    transition_budget: u64,
    seed: u64,
    uniform_percent: u8 = 10,
    splice_percent: u8 = 20,
    checkpoint_percent: u8 = 10,
    targets: []const coverage_mod.Target = &.{},

    pub fn validate(self: Config) !void {
        if (self.histories == 0) return error.InvalidHistoryBudget;
        if (self.transition_budget == 0) return error.InvalidTransitionBudget;
        if (self.uniform_percent > 100 or self.splice_percent > 100 or self.checkpoint_percent > 100 or
            @as(u16, self.uniform_percent) + @as(u16, self.splice_percent) > 100)
            return error.InvalidExplorationPercent;
    }
};

pub const Report = struct {
    histories: u64 = 0,
    generated: u64 = 0,
    mutated: u64 = 0,
    splice_attempts: u64 = 0,
    spliced: u64 = 0,
    splice_rejected: u64 = 0,
    checkpoint_preflights: u64 = 0,
    checkpoints_inserted: u64 = 0,
    checkpoints_deduplicated: u64 = 0,
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
        checkpoints: snapshot.Store,
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
                .checkpoints = snapshot.Store.init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            self.checkpoints.deinit();
            self.corpus.deinit();
            self.coverage.deinit();
            self.* = undefined;
        }

        pub fn run(self: *Self) !void {
            while (self.report.histories < self.config.histories) {
                const policy = self.prng.random().uintLessThan(u8, 100);
                const use_uniform = self.corpus.entries.items.len == 0 or policy < self.config.uniform_percent;
                if (use_uniform) {
                    try self.runGenerated(self.prng.random().int(u64));
                } else if (self.corpus.entries.items.len >= 2 and
                    policy < self.config.uniform_percent + self.config.splice_percent)
                {
                    const left = try self.corpus.select(self.prng.random());
                    var right = try self.corpus.select(self.prng.random());
                    if (right == left) right = (right + 1) % self.corpus.entries.items.len;
                    if (!try self.runSplice(left, right)) {
                        const parent = try self.corpus.select(self.prng.random());
                        if (!try self.runMutation(parent)) try self.runGenerated(self.prng.random().int(u64));
                    }
                } else {
                    const parent = try self.corpus.select(self.prng.random());
                    if (!try self.runMutation(parent)) try self.runGenerated(self.prng.random().int(u64));
                }
                self.report.histories += 1;
            }
            self.report.semantic_features = self.coverage.hits.count();
        }

        fn runSplice(self: *Self, left_index: usize, right_index: usize) !bool {
            self.report.splice_attempts += 1;
            var left = try trace.parseAlloc(self.allocator, self.corpus.entries.items[left_index].bytes);
            defer left.deinit();
            var right = try trace.parseAlloc(self.allocator, self.corpus.entries.items[right_index].bytes);
            defer right.deinit();
            if (!std.mem.eql(u8, left.header.scenario, right.header.scenario) or
                left.header.scenario_version != right.header.scenario_version)
            {
                self.report.splice_rejected += 1;
                return false;
            }
            const points = try splice.findPoints(self.allocator, &left, &right);
            defer self.allocator.free(points);
            var usable: usize = 0;
            for (points) |point| {
                const combined = point.left_choice_count + right.choices.items.len - point.right_choice_start;
                usable += @intFromBool(combined > 0 and combined <= self.config.transition_budget);
            }
            if (usable == 0) {
                self.report.splice_rejected += 1;
                return false;
            }
            var ordinal = self.prng.random().uintLessThan(usize, usable);
            const selected_point = for (points) |point| {
                const combined = point.left_choice_count + right.choices.items.len - point.right_choice_start;
                if (combined == 0 or combined > self.config.transition_budget) continue;
                if (ordinal == 0) break point;
                ordinal -= 1;
            } else unreachable;
            var source = try splice.Source.init(left.choices.items, right.choices.items, selected_point);
            var artifact = runner.run(Scenario, self.allocator, source.source(), .{
                .system = self.config.system,
                .transition_budget = @intCast(source.combinedLength()),
            }) catch |err| switch (err) {
                error.SpliceChoiceExhausted,
                error.SpliceChoiceSiteDiverged,
                error.SpliceEnabledSetDiverged,
                error.SpliceHasTrailingChoices,
                error.TransitionBudgetExceeded,
                => {
                    self.report.splice_rejected += 1;
                    return false;
                },
                else => return err,
            };
            defer artifact.deinit();
            var replayed = try replay.exact(Scenario, self.allocator, &artifact);
            replayed.deinit();
            self.report.spliced += 1;
            try self.consider(&artifact, left_index);
            return true;
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
            try self.maybePreflightCheckpoint(&parent);
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

        fn maybePreflightCheckpoint(self: *Self, parent: *const trace.Trace) !void {
            if (comptime !(@hasDecl(Scenario, "snapshotAlloc") and @hasDecl(Scenario, "restoreSnapshot"))) return;
            if (self.prng.random().uintLessThan(u8, 100) >= self.config.checkpoint_percent) return;
            self.report.checkpoint_preflights += 1;
            const prefix_len = self.prng.random().uintLessThan(usize, parent.choices.items.len + 1);
            var world = try Scenario.init(self.allocator);
            defer Scenario.deinit(&world, self.allocator);
            for (0..prefix_len) |index| try executeRecordedChoice(Scenario, self.allocator, &world, parent, index);
            const before_digest = try observeDigest(Scenario, self.allocator, &world);
            if (before_digest != parent.observations.items[prefix_len].digest) return error.CheckpointPrefixObservationDiverged;
            var checkpoint = try snapshot.capture(Scenario, self.allocator, &world, prefix_len, before_digest);
            var checkpoint_owned = true;
            errdefer if (checkpoint_owned) checkpoint.deinit(self.allocator);
            if (prefix_len < parent.choices.items.len) try executeRecordedChoice(Scenario, self.allocator, &world, parent, prefix_len);
            try snapshot.restore(Scenario, &world, checkpoint, self.allocator);
            if (try observeDigest(Scenario, self.allocator, &world) != before_digest) return error.CheckpointRestoreObservationDiverged;
            const added = try self.checkpoints.add(checkpoint);
            if (added.inserted) {
                checkpoint_owned = false;
                self.report.checkpoints_inserted += 1;
            } else {
                checkpoint.deinit(self.allocator);
                checkpoint_owned = false;
                self.report.checkpoints_deduplicated += 1;
            }
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

fn executeRecordedChoice(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    world: *Scenario.World,
    artifact: *const trace.Trace,
    index: usize,
) !void {
    const record = artifact.choices.items[index];
    if (record.site_id != ids.stable("choice", Scenario.name ++ ".transition") or
        record.occurrence != index) return error.CheckpointChoiceSiteDiverged;
    var enabled = try scheduler.enumerateCanonical(Scenario, world, allocator);
    defer enabled.deinit(allocator);
    if (record.enabled_ids.len != enabled.items.items.len) return error.CheckpointEnabledSetDiverged;
    var selected: ?@import("transition.zig").Transition = null;
    for (enabled.items.items, record.enabled_ids) |candidate, expected_id| {
        if (candidate.id != expected_id) return error.CheckpointEnabledSetDiverged;
        if (candidate.id == record.selected_id) selected = candidate;
    }
    const selected_transition = selected orelse return error.CheckpointSelectedTransitionMissing;
    var events = event.Sink{};
    defer events.deinit(allocator);
    try Scenario.execute(world, selected_transition, &events, allocator);
}

fn observeDigest(comptime Scenario: type, allocator: std.mem.Allocator, world: *Scenario.World) !u64 {
    var builder = observation.Builder{};
    defer builder.deinit(allocator);
    try Scenario.observe(world, &builder, allocator);
    try builder.canonicalize();
    return builder.digest();
}

test "campaign generates, mutates, and retains semantic histories" {
    const ToyScenario = @import("toy_scenario.zig").ToyScenario;
    var campaign = try Campaign(ToyScenario).init(std.testing.allocator, .{
        .system = "vopr-test",
        .histories = 8,
        .transition_budget = 4,
        .seed = 0x1234,
        .uniform_percent = 0,
        .splice_percent = 0,
    });
    defer campaign.deinit();
    try campaign.run();
    try std.testing.expectEqual(@as(u64, 8), campaign.report.histories);
    try std.testing.expect(campaign.report.generated >= 1);
    try std.testing.expect(campaign.report.mutated >= 1);
    try std.testing.expect(campaign.report.retained >= 1);
    try std.testing.expect(campaign.report.semantic_features > 0);
}

test "campaign exact-replays compatible splices before retention" {
    const ToyScenario = @import("toy_scenario.zig").ToyScenario;
    var campaign = try Campaign(ToyScenario).init(std.testing.allocator, .{
        .system = "vopr-test",
        .histories = 32,
        .transition_budget = 4,
        .seed = 0x5a11ce,
        .uniform_percent = 20,
        .splice_percent = 80,
    });
    defer campaign.deinit();
    try campaign.run();
    try std.testing.expect(campaign.report.splice_attempts > 0);
    try std.testing.expect(campaign.report.spliced > 0);
    for (campaign.corpus.entries.items) |entry| {
        var artifact = try trace.parseAlloc(std.testing.allocator, entry.bytes);
        defer artifact.deinit();
        var replayed = try replay.exact(ToyScenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}

test "campaign selects validates and deduplicates logical checkpoints" {
    const ToyScenario = @import("toy_scenario.zig").ToyScenario;
    var campaign = try Campaign(ToyScenario).init(std.testing.allocator, .{
        .system = "vopr-test",
        .histories = 12,
        .transition_budget = 4,
        .seed = 0xc4ec_9017,
        .uniform_percent = 0,
        .splice_percent = 0,
        .checkpoint_percent = 100,
    });
    defer campaign.deinit();
    try campaign.run();
    try std.testing.expect(campaign.report.checkpoint_preflights > 0);
    try std.testing.expect(campaign.report.checkpoints_inserted > 0);
    try std.testing.expectEqual(campaign.checkpoints.checkpoints.items.len, campaign.report.checkpoints_inserted);
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
