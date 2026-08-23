// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Owned, canonical trace corpus with deterministic deduplication and energy.

const std = @import("std");
const coverage = @import("coverage.zig");
const ids = @import("id.zig");
const trace = @import("trace.zig");

pub const Key = struct {
    trace_digest: u64,
    final_observation_digest: u64,
    failure_fingerprint: u64,
};

pub const Entry = struct {
    key: Key,
    bytes: []const u8,
    novelty: usize,
    rarity_score: u64,
    target_score: u64,
    selections: u64 = 0,
    productive_children: u64 = 0,

    pub fn energy(self: Entry) u64 {
        const novelty_energy: u64 = @intCast(@min(self.novelty, 1024));
        const productivity = self.productive_children *| 8;
        const selection_penalty = self.selections / 4;
        return @max(@as(u64, 1), 1 +| novelty_energy *| 16 +| self.rarity_score / 100_000 +| self.target_score *| 32 +| productivity -| selection_penalty);
    }
};

pub const QuarantineReason = enum {
    invalid_trace,
    scenario_changed,
    scenario_version_changed,
    backend_changed,
};

pub const Quarantined = struct {
    trace_digest: u64,
    reason: QuarantineReason,
    bytes: []const u8,
};

pub const Compatibility = struct {
    scenario: []const u8,
    scenario_version: u32,
    backend_ids: []const ids.StableId = &.{},
};

pub const ImportResult = union(enum) {
    retained: AddResult,
    quarantined: usize,
};

pub const AddResult = struct { index: usize, inserted: bool };

pub const PropertyHistory = struct {
    property_id: ids.StableId,
    name: []const u8,
    evaluations: u64 = 0,
    failures: u64 = 0,
    revisions_seen: u64 = 0,
    last_revision_digest: u64 = 0,
};

pub const Corpus = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    keys: std.AutoHashMapUnmanaged(Key, usize) = .empty,
    quarantined: std.ArrayListUnmanaged(Quarantined) = .empty,
    property_history: std.ArrayListUnmanaged(PropertyHistory) = .empty,

    pub fn init(allocator: std.mem.Allocator) Corpus {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Corpus) void {
        for (self.entries.items) |entry| self.allocator.free(entry.bytes);
        for (self.quarantined.items) |entry| self.allocator.free(entry.bytes);
        for (self.property_history.items) |entry| self.allocator.free(entry.name);
        self.entries.deinit(self.allocator);
        self.keys.deinit(self.allocator);
        self.quarantined.deinit(self.allocator);
        self.property_history.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn add(self: *Corpus, artifact: *const trace.Trace, novelty: coverage.Novelty) !AddResult {
        const bytes = try artifact.renderAlloc(self.allocator);
        errdefer self.allocator.free(bytes);
        const summary = artifact.summary orelse return error.MissingTraceSummary;
        const fingerprint = if (artifact.failures.items.len == 0) 0 else artifact.failures.items[0].fingerprint;
        const key = Key{
            .trace_digest = ids.digest(bytes),
            .final_observation_digest = summary.final_observation_digest,
            .failure_fingerprint = fingerprint,
        };
        const gop = try self.keys.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            self.allocator.free(bytes);
            return .{ .index = gop.value_ptr.*, .inserted = false };
        }
        errdefer _ = self.keys.remove(key);
        const index = self.entries.items.len;
        try self.entries.append(self.allocator, .{
            .key = key,
            .bytes = bytes,
            .novelty = novelty.discovered,
            .rarity_score = novelty.rarity_score,
            .target_score = novelty.target_score,
        });
        gop.value_ptr.* = index;
        try self.observeProperties(artifact);
        return .{ .index = index, .inserted = true };
    }

    /// Import a retained artifact without silently discarding histories that
    /// no longer match the active replay ABI. Structural and compatibility
    /// failures are copied to an explicit quarantine for inspection/migration.
    pub fn importBytes(
        self: *Corpus,
        bytes: []const u8,
        novelty: coverage.Novelty,
        compatibility: Compatibility,
    ) !ImportResult {
        var artifact = trace.parseAlloc(self.allocator, bytes) catch {
            return .{ .quarantined = try self.quarantineBytes(bytes, .invalid_trace) };
        };
        defer artifact.deinit();
        const reason: ?QuarantineReason = if (!std.mem.eql(u8, artifact.header.scenario, compatibility.scenario))
            .scenario_changed
        else if (artifact.header.scenario_version != compatibility.scenario_version)
            .scenario_version_changed
        else if (!std.mem.eql(ids.StableId, artifact.config.backend_ids, compatibility.backend_ids))
            .backend_changed
        else
            null;
        if (reason) |value| return .{ .quarantined = try self.quarantineBytes(bytes, value) };
        return .{ .retained = try self.add(&artifact, novelty) };
    }

    pub fn select(self: *Corpus, random: std.Random) !usize {
        if (self.entries.items.len == 0) return error.EmptyCorpus;
        var total: u64 = 0;
        for (self.entries.items) |entry| total +|= entry.energy();
        var ticket = random.intRangeLessThan(u64, 0, total);
        for (self.entries.items, 0..) |*entry, index| {
            const energy_value = entry.energy();
            if (ticket < energy_value) {
                entry.selections +|= 1;
                return index;
            }
            ticket -= energy_value;
        }
        unreachable;
    }

    pub fn markProductive(self: *Corpus, index: usize) !void {
        if (index >= self.entries.items.len) return error.InvalidCorpusIndex;
        self.entries.items[index].productive_children +|= 1;
    }

    pub fn quarantineBytes(self: *Corpus, bytes: []const u8, reason: QuarantineReason) !usize {
        const digest = ids.digest(bytes);
        for (self.quarantined.items, 0..) |entry, index| {
            if (entry.trace_digest == digest and entry.reason == reason) return index;
        }
        const owned = try self.allocator.dupe(u8, bytes);
        errdefer self.allocator.free(owned);
        const index = self.quarantined.items.len;
        try self.quarantined.append(self.allocator, .{
            .trace_digest = digest,
            .reason = reason,
            .bytes = owned,
        });
        return index;
    }

    fn observeProperties(self: *Corpus, artifact: *const trace.Trace) !void {
        const revision_digest = ids.digest(artifact.header.source_revision);
        for (artifact.properties.items) |record| {
            var history: ?*PropertyHistory = null;
            for (self.property_history.items) |*candidate| if (candidate.property_id == record.property_id) {
                history = candidate;
                break;
            };
            if (history == null) {
                const name = try self.allocator.dupe(u8, record.name);
                errdefer self.allocator.free(name);
                try self.property_history.append(self.allocator, .{
                    .property_id = record.property_id,
                    .name = name,
                });
                history = &self.property_history.items[self.property_history.items.len - 1];
            }
            const item = history.?;
            item.evaluations +|= 1;
            item.failures +|= @intFromBool(!record.condition);
            if (item.last_revision_digest != revision_digest) {
                item.revisions_seen +|= 1;
                item.last_revision_digest = revision_digest;
            }
        }
    }
};

test "corpus deduplicates canonical artifacts and selects deterministically" {
    var artifact = try trace.Trace.init(std.testing.allocator, .{ .scenario = "corpus", .scenario_version = 1 }, .{ .transition_budget = 1 });
    defer artifact.deinit();
    try artifact.addChoice(.{ .site_id = 1, .site_name = "choice", .occurrence = 0, .enabled_ids = &.{2}, .selected_id = 2 });
    try artifact.addTransition(.{ .index = 1, .id = 2, .name = "step", .kind = .workload });
    const empty_digest = @import("observation.zig").digestFeatures(&.{});
    try artifact.addObservation(.{ .index = 0, .digest = empty_digest, .features = &.{} });
    try artifact.addObservation(.{ .index = 1, .digest = empty_digest, .features = &.{} });
    artifact.summary = .{ .transitions = 1, .final_observation_digest = empty_digest, .property_failures = 0 };
    try artifact.validate();

    var corpus = Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    const first = try corpus.add(&artifact, .{ .discovered = 2, .rarity_score = 50 });
    const duplicate = try corpus.add(&artifact, .{ .discovered = 0 });
    try std.testing.expect(first.inserted);
    try std.testing.expect(!duplicate.inserted);
    try std.testing.expectEqual(@as(usize, 1), corpus.entries.items.len);
    var prng = std.Random.DefaultPrng.init(1);
    try std.testing.expectEqual(@as(usize, 0), try corpus.select(prng.random()));
}

test "corpus quarantines incompatible retained histories and preserves property history" {
    const observation = @import("observation.zig");
    const property = @import("property.zig");
    var artifact = try trace.Trace.init(std.testing.allocator, .{
        .scenario = "corpus-import",
        .scenario_version = 2,
        .source_revision = "revision-a",
    }, .{ .transition_budget = 1, .backend_ids = &.{11} });
    defer artifact.deinit();
    const empty_digest = observation.digestFeatures(&.{});
    try artifact.addObservation(.{ .index = 0, .digest = empty_digest, .features = &.{} });
    try artifact.addChoice(.{ .site_id = 1, .site_name = "import", .occurrence = 0, .enabled_ids = &.{2}, .selected_id = 2 });
    try artifact.addTransition(.{ .index = 1, .id = 2, .name = "import.step", .kind = .workload });
    try artifact.addObservation(.{ .index = 1, .digest = empty_digest, .features = &.{} });
    try artifact.addProperty(.{
        .index = 1,
        .property_id = 9,
        .name = "import.safe",
        .kind = property.Kind.always,
        .condition = false,
        .details = "failed",
    });
    artifact.summary = .{ .transitions = 1, .final_observation_digest = empty_digest, .property_failures = 0 };
    const bytes = try artifact.renderAlloc(std.testing.allocator);
    defer std.testing.allocator.free(bytes);

    var corpus = Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    const retained = try corpus.importBytes(bytes, .{ .discovered = 1 }, .{
        .scenario = "corpus-import",
        .scenario_version = 2,
        .backend_ids = &.{11},
    });
    try std.testing.expect(retained == .retained);
    try std.testing.expectEqual(@as(usize, 1), corpus.property_history.items.len);
    try std.testing.expectEqual(@as(u64, 1), corpus.property_history.items[0].failures);

    const quarantined = try corpus.importBytes(bytes, .{}, .{
        .scenario = "corpus-import",
        .scenario_version = 3,
        .backend_ids = &.{11},
    });
    try std.testing.expect(quarantined == .quarantined);
    try std.testing.expectEqual(QuarantineReason.scenario_version_changed, corpus.quarantined.items[0].reason);
    _ = try corpus.importBytes("not a trace", .{}, .{
        .scenario = "corpus-import",
        .scenario_version = 2,
    });
    try std.testing.expectEqual(@as(usize, 2), corpus.quarantined.items.len);
}
