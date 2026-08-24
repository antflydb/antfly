// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const ids = @import("id.zig");
const trace = @import("trace.zig");
const transition = @import("transition.zig");

pub const Request = struct {
    site_id: ids.StableId,
    site_name: []const u8,
    occurrence: u64,
    enabled: []const transition.Transition,
};

pub const AuditResult = struct {
    choices: u64,
    distinct_sites: u64,
    parameterized_choices: u64,
    maximum_enabled: usize,
};

/// Proves that every recorded scenario decision selected one immediate typed
/// transition. Seeds remain configuration/discovery metadata: a choice cannot
/// select an opaque value to be interpreted later because the selected stable
/// ID must be the transition executed at that exact occurrence.
pub fn auditTrace(history: *const trace.Trace) !AuditResult {
    try history.validate();
    var result = AuditResult{
        .choices = @intCast(history.choices.items.len),
        .distinct_sites = 0,
        .parameterized_choices = 0,
        .maximum_enabled = 0,
    };
    for (history.choices.items, history.transitions.items, 0..) |record, executed, index| {
        if (record.site_id != ids.stable("choice", record.site_name)) return error.UnstableChoiceSiteId;
        if (record.occurrence != index) return error.NonImmediateChoiceOccurrence;
        if (record.selected_id != executed.id) return error.DeferredChoiceInterpretation;
        result.maximum_enabled = @max(result.maximum_enabled, record.enabled_ids.len);
        result.parameterized_choices += @intFromBool(executed.parameter != 0);
        var seen_site = false;
        for (history.choices.items[0..index]) |prior| if (prior.site_id == record.site_id) {
            seen_site = true;
            break;
        };
        result.distinct_sites += @intFromBool(!seen_site);
    }
    return result;
}

pub const Source = struct {
    ptr: *anyopaque,
    choose_fn: *const fn (*anyopaque, Request) anyerror!ids.StableId,
    finish_fn: *const fn (*anyopaque) anyerror!void,

    pub fn choose(self: Source, request: Request) !ids.StableId {
        if (request.enabled.len == 0) return error.EmptyChoiceSet;
        const selected = try self.choose_fn(self.ptr, request);
        for (request.enabled) |alternative| if (alternative.id == selected) return selected;
        return error.ChoiceSourceSelectedDisabledAlternative;
    }

    pub fn finish(self: Source) !void {
        try self.finish_fn(self.ptr);
    }
};

pub const Seeded = struct {
    prng: std.Random.DefaultPrng,

    pub fn init(seed: u64) Seeded {
        return .{ .prng = std.Random.DefaultPrng.init(seed) };
    }

    pub fn source(self: *Seeded) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *Seeded = @ptrCast(@alignCast(ptr));
        var weighted = false;
        var total: u64 = 0;
        for (request.enabled) |alternative| {
            if (alternative.weight == 0) return error.InvalidTransitionWeight;
            weighted = weighted or alternative.weight != 1;
            total = std.math.add(u64, total, alternative.weight) catch return error.ChoiceWeightOverflow;
        }
        // Preserve the original uniform seeded stream byte-for-byte for the
        // overwhelmingly common all-ones case and existing replay fixtures.
        if (!weighted) {
            const index = self.prng.random().intRangeLessThan(usize, 0, request.enabled.len);
            return request.enabled[index].id;
        }
        var ordinal = self.prng.random().uintLessThan(u64, total);
        for (request.enabled) |alternative| {
            if (ordinal < alternative.weight) return alternative.id;
            ordinal -= alternative.weight;
        }
        unreachable;
    }

    fn finish(_: *anyopaque) !void {}
};

/// Deterministically withhold one transition for a bounded number of choice
/// points, then force it when it remains enabled. This turns starvation from a
/// low-probability random accident into a reproducible scheduling experiment.
pub const Starving = struct {
    target_id: ids.StableId,
    skip_budget: usize,
    skipped: usize = 0,
    forced: bool = false,
    fallback: Seeded,

    pub fn init(target_id: ids.StableId, skip_budget: usize, seed: u64) Starving {
        return .{ .target_id = target_id, .skip_budget = skip_budget, .fallback = Seeded.init(seed) };
    }

    pub fn source(self: *Starving) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *Starving = @ptrCast(@alignCast(ptr));
        var target_enabled = false;
        var alternatives: [64]transition.Transition = undefined;
        var alternative_count: usize = 0;
        for (request.enabled) |candidate| {
            if (candidate.id == self.target_id) {
                target_enabled = true;
            } else if (alternative_count < alternatives.len) {
                alternatives[alternative_count] = candidate;
                alternative_count += 1;
            }
        }
        if (target_enabled and self.skipped >= self.skip_budget) {
            self.forced = true;
            return self.target_id;
        }
        if (target_enabled and alternative_count != 0) {
            self.skipped += 1;
            var narrowed = request;
            narrowed.enabled = alternatives[0..alternative_count];
            return Seeded.choose(&self.fallback, narrowed);
        }
        return Seeded.choose(&self.fallback, request);
    }

    fn finish(_: *anyopaque) !void {}
};

pub const Scripted = struct {
    selections: []const ids.StableId,
    cursor: usize = 0,

    pub fn source(self: *Scripted) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, _: Request) !ids.StableId {
        const self: *Scripted = @ptrCast(@alignCast(ptr));
        if (self.cursor >= self.selections.len) return error.ScriptExhausted;
        defer self.cursor += 1;
        return self.selections[self.cursor];
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *Scripted = @ptrCast(@alignCast(ptr));
        if (self.cursor != self.selections.len) return error.UnusedScriptChoices;
    }
};

/// Deterministic depth-first enumeration of a dynamic choice tree.
///
/// Call `beginHistory` before each clean-world execution, run the scenario
/// through `source`, then call `advance` after a successful `finish`. The
/// enumerator stores ordinals rather than transition IDs because each history
/// supplies and validates its canonical enabled set. When an earlier branch
/// changes, its stale suffix is discarded. This keeps memory proportional to
/// history depth and never snapshots scenario state.
pub const Enumerating = struct {
    allocator: std.mem.Allocator,
    ordinals: std.ArrayListUnmanaged(usize) = .empty,
    radices: std.ArrayListUnmanaged(usize) = .empty,
    cursor: usize = 0,
    history_open: bool = false,
    exhausted: bool = false,

    pub fn init(allocator: std.mem.Allocator) Enumerating {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Enumerating) void {
        self.ordinals.deinit(self.allocator);
        self.radices.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn beginHistory(self: *Enumerating) !void {
        if (self.history_open) return error.EnumerationHistoryAlreadyOpen;
        if (self.exhausted) return error.EnumerationExhausted;
        self.cursor = 0;
        self.radices.clearRetainingCapacity();
        self.history_open = true;
    }

    pub fn source(self: *Enumerating) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    /// Advances to the next depth-first history. Returns false exactly once
    /// the complete reachable tree has been enumerated.
    pub fn advance(self: *Enumerating) !bool {
        if (self.history_open) return error.EnumerationHistoryStillOpen;
        if (self.exhausted) return false;
        if (self.ordinals.items.len != self.radices.items.len) return error.InvalidEnumerationState;
        var index = self.ordinals.items.len;
        while (index > 0) {
            index -= 1;
            if (self.ordinals.items[index] + 1 < self.radices.items[index]) {
                self.ordinals.items[index] += 1;
                self.ordinals.shrinkRetainingCapacity(index + 1);
                return true;
            }
        }
        self.exhausted = true;
        return false;
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *Enumerating = @ptrCast(@alignCast(ptr));
        if (!self.history_open) return error.EnumerationHistoryNotOpen;
        const index = self.cursor;
        self.cursor += 1;
        const ordinal = if (index < self.ordinals.items.len)
            self.ordinals.items[index]
        else blk: {
            try self.ordinals.append(self.allocator, 0);
            break :blk 0;
        };
        if (ordinal >= request.enabled.len) return error.EnumerationEnabledSetDiverged;
        try self.radices.append(self.allocator, request.enabled.len);
        return request.enabled[ordinal].id;
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *Enumerating = @ptrCast(@alignCast(ptr));
        if (!self.history_open) return error.EnumerationHistoryNotOpen;
        self.history_open = false;
        // A branch may terminate before a suffix inherited from its parent.
        // Only decisions actually reached belong to the new path.
        self.ordinals.shrinkRetainingCapacity(self.cursor);
        if (self.cursor == 0) self.exhausted = true;
    }
};

pub const Replay = struct {
    records: []const trace.ChoiceRecord,
    cursor: usize = 0,

    pub fn source(self: *Replay) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *Replay = @ptrCast(@alignCast(ptr));
        if (self.cursor >= self.records.len) return error.ReplayChoiceExhausted;
        const record = self.records[self.cursor];
        if (record.site_id != request.site_id or !std.mem.eql(u8, record.site_name, request.site_name) or record.occurrence != request.occurrence) return error.ReplayChoiceSiteDiverged;
        if (record.enabled_ids.len != request.enabled.len) return error.ReplayEnabledSetDiverged;
        for (request.enabled, record.enabled_ids) |enabled, recorded_id| {
            if (enabled.id != recorded_id) return error.ReplayEnabledSetDiverged;
        }
        self.cursor += 1;
        return record.selected_id;
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *Replay = @ptrCast(@alignCast(ptr));
        if (self.cursor != self.records.len) return error.ReplayHasTrailingChoices;
    }
};

/// Replays an exact prefix, replaces one decision with a requested enabled
/// alternative, then generates a new suffix. This is the basic branching
/// primitive; it rebuilds a clean world and never snapshots pointer state.
pub const Mutating = struct {
    base: []const trace.ChoiceRecord,
    mutation_index: usize,
    replacement_id: ids.StableId,
    prng: std.Random.DefaultPrng,
    cursor: usize = 0,
    mutated: bool = false,

    pub fn init(base: []const trace.ChoiceRecord, mutation_index: usize, replacement_id: ids.StableId, suffix_seed: u64) Mutating {
        return initAt(base, mutation_index, replacement_id, suffix_seed, 0);
    }

    /// Resume at an already-restored exact prefix. The caller is responsible
    /// for binding the restored state to `base[0..cursor]`.
    pub fn initAt(
        base: []const trace.ChoiceRecord,
        mutation_index: usize,
        replacement_id: ids.StableId,
        suffix_seed: u64,
        cursor: usize,
    ) Mutating {
        return .{
            .base = base,
            .mutation_index = mutation_index,
            .replacement_id = replacement_id,
            .prng = std.Random.DefaultPrng.init(suffix_seed),
            .cursor = cursor,
        };
    }

    pub fn source(self: *Mutating) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *Mutating = @ptrCast(@alignCast(ptr));
        const index = self.cursor;
        self.cursor += 1;
        if (index < self.mutation_index) {
            if (index >= self.base.len) return error.MutationPrefixExhausted;
            const record = self.base[index];
            try verifyRecord(record, request);
            return record.selected_id;
        }
        if (index == self.mutation_index) {
            if (index >= self.base.len) return error.MutationPointOutOfRange;
            const record = self.base[index];
            try verifySite(record, request);
            if (record.selected_id == self.replacement_id) return error.MutationDidNotChangeChoice;
            self.mutated = true;
            return self.replacement_id;
        }
        const selected_index = self.prng.random().intRangeLessThan(usize, 0, request.enabled.len);
        return request.enabled[selected_index].id;
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *Mutating = @ptrCast(@alignCast(ptr));
        if (!self.mutated) return error.MutationPointNotReached;
    }
};

/// Replays an exact prefix, removes a contiguous range of recorded logical
/// decisions, then attempts to rebase the recorded suffix onto the resulting
/// state. A suffix decision is reused only when its choice site and stable
/// transition ID are valid; otherwise the remainder is generated from the
/// deterministic suffix seed. Every candidate still executes from a clean
/// world, so deletion never edits an artifact in place.
pub const Deleting = struct {
    base: []const trace.ChoiceRecord,
    start: usize,
    end: usize,
    seeded: Seeded,
    cursor: usize = 0,
    suffix_only: bool = false,
    deletion_reached: bool = false,

    pub fn init(base: []const trace.ChoiceRecord, start: usize, end: usize, suffix_seed: u64) !Deleting {
        if (start >= end or end > base.len) return error.InvalidChoiceDeletionRange;
        return .{
            .base = base,
            .start = start,
            .end = end,
            .seeded = Seeded.init(suffix_seed),
        };
    }

    pub fn source(self: *Deleting) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *Deleting = @ptrCast(@alignCast(ptr));
        const index = self.cursor;
        self.cursor += 1;
        if (index < self.start) {
            if (index >= self.base.len) return error.DeletionPrefixExhausted;
            const record = self.base[index];
            try verifyRecord(record, request);
            return record.selected_id;
        }
        self.deletion_reached = true;
        if (!self.suffix_only) {
            const rebased_index = index + (self.end - self.start);
            if (rebased_index < self.base.len) {
                const record = self.base[rebased_index];
                if (record.site_id == request.site_id and std.mem.eql(u8, record.site_name, request.site_name)) {
                    for (request.enabled) |enabled| {
                        if (enabled.id == record.selected_id) return record.selected_id;
                    }
                }
            }
            self.suffix_only = true;
        }
        return try self.seeded.source().choose(request);
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *Deleting = @ptrCast(@alignCast(ptr));
        if (!self.deletion_reached) return error.DeletionPointNotReached;
        try self.seeded.source().finish();
    }
};

fn verifySite(record: trace.ChoiceRecord, request: Request) !void {
    if (record.site_id != request.site_id or !std.mem.eql(u8, record.site_name, request.site_name) or record.occurrence != request.occurrence) return error.ReplayChoiceSiteDiverged;
}

fn verifyRecord(record: trace.ChoiceRecord, request: Request) !void {
    try verifySite(record, request);
    if (record.enabled_ids.len != request.enabled.len) return error.ReplayEnabledSetDiverged;
    for (request.enabled, record.enabled_ids) |enabled, recorded_id| {
        if (enabled.id != recorded_id) return error.ReplayEnabledSetDiverged;
    }
}

test "replay diagnoses enabled-set divergence" {
    const records = [_]trace.ChoiceRecord{.{ .site_id = 7, .site_name = "site", .occurrence = 0, .enabled_ids = &.{ 1, 2 }, .selected_id = 1 }};
    const enabled = [_]transition.Transition{
        .{ .id = 1, .name = "one", .kind = .workload },
        .{ .id = 3, .name = "three", .kind = .workload },
    };
    var replay = Replay{ .records = &records };
    try std.testing.expectError(error.ReplayEnabledSetDiverged, replay.source().choose(.{ .site_id = 7, .site_name = "site", .occurrence = 0, .enabled = &enabled }));
}

test "mutating choice source replays a prefix and branches" {
    const records = [_]trace.ChoiceRecord{
        .{ .site_id = 7, .site_name = "site", .occurrence = 0, .enabled_ids = &.{ 1, 2 }, .selected_id = 1 },
        .{ .site_id = 7, .site_name = "site", .occurrence = 1, .enabled_ids = &.{ 1, 2 }, .selected_id = 1 },
    };
    const enabled = [_]transition.Transition{
        .{ .id = 1, .name = "one", .kind = .workload },
        .{ .id = 2, .name = "two", .kind = .workload },
    };
    var mutating = Mutating.init(&records, 1, 2, 99);
    const source = mutating.source();
    try std.testing.expectEqual(@as(u64, 1), try source.choose(.{ .site_id = 7, .site_name = "site", .occurrence = 0, .enabled = &enabled }));
    try std.testing.expectEqual(@as(u64, 2), try source.choose(.{ .site_id = 7, .site_name = "site", .occurrence = 1, .enabled = &enabled }));
    try source.finish();
}

test "deleting choice source removes a range and rebases a valid suffix" {
    const records = [_]trace.ChoiceRecord{
        .{ .site_id = 7, .site_name = "site", .occurrence = 0, .enabled_ids = &.{ 1, 2 }, .selected_id = 1 },
        .{ .site_id = 7, .site_name = "site", .occurrence = 1, .enabled_ids = &.{ 1, 2 }, .selected_id = 1 },
        .{ .site_id = 7, .site_name = "site", .occurrence = 2, .enabled_ids = &.{ 1, 2 }, .selected_id = 2 },
    };
    const enabled = [_]transition.Transition{
        .{ .id = 1, .name = "loop", .kind = .workload },
        .{ .id = 2, .name = "finish", .kind = .workload },
    };
    var deleting = try Deleting.init(&records, 0, 2, 99);
    const source = deleting.source();
    try std.testing.expectEqual(@as(u64, 2), try source.choose(.{ .site_id = 7, .site_name = "site", .occurrence = 0, .enabled = &enabled }));
    try source.finish();
}

test "seeded source honors weights while preserving enabled membership" {
    const enabled = [_]transition.Transition{
        .{ .id = 1, .name = "rare", .kind = .workload, .weight = 1 },
        .{ .id = 2, .name = "common", .kind = .workload, .weight = 7 },
    };
    var rare: usize = 0;
    var common: usize = 0;
    var seeded = Seeded.init(0x51e1_9a7e);
    const source = seeded.source();
    for (0..8_000) |occurrence| {
        switch (try source.choose(.{
            .site_id = 9,
            .site_name = "weighted",
            .occurrence = occurrence,
            .enabled = &enabled,
        })) {
            1 => rare += 1,
            2 => common += 1,
            else => return error.InvalidWeightedSelection,
        }
    }
    try source.finish();
    try std.testing.expect(common > rare * 5);
    try std.testing.expect(common < rare * 9);
}

test "starving source systematically delays then forces a runnable target" {
    const enabled = [_]transition.Transition{
        .{ .id = 1, .name = "target", .kind = .scheduler },
        .{ .id = 2, .name = "competitor", .kind = .scheduler },
    };
    var starving = Starving.init(1, 3, 7);
    const source = starving.source();
    for (0..3) |occurrence| try std.testing.expectEqual(@as(u64, 2), try source.choose(.{
        .site_id = 9,
        .site_name = "starvation",
        .occurrence = occurrence,
        .enabled = &enabled,
    }));
    try std.testing.expectEqual(@as(u64, 1), try source.choose(.{
        .site_id = 9,
        .site_name = "starvation",
        .occurrence = 3,
        .enabled = &enabled,
    }));
    try std.testing.expectEqual(@as(usize, 3), starving.skipped);
    try std.testing.expect(starving.forced);
}

test "enumerating source visits a dynamic choice tree exactly once" {
    const root = [_]transition.Transition{
        .{ .id = 10, .name = "short", .kind = .workload },
        .{ .id = 20, .name = "long", .kind = .workload },
    };
    const suffix = [_]transition.Transition{
        .{ .id = 21, .name = "left", .kind = .workload },
        .{ .id = 22, .name = "middle", .kind = .workload },
        .{ .id = 23, .name = "right", .kind = .workload },
    };
    var enumerating = Enumerating.init(std.testing.allocator);
    defer enumerating.deinit();
    var histories: usize = 0;
    var seen_short = false;
    var seen_suffix = [_]bool{ false, false, false };
    while (true) {
        try enumerating.beginHistory();
        const source = enumerating.source();
        const first = try source.choose(.{ .site_id = 1, .site_name = "root", .occurrence = 0, .enabled = &root });
        if (first == 10) {
            seen_short = true;
        } else {
            const second = try source.choose(.{ .site_id = 2, .site_name = "suffix", .occurrence = 1, .enabled = &suffix });
            seen_suffix[@intCast(second - 21)] = true;
        }
        try source.finish();
        histories += 1;
        if (!try enumerating.advance()) break;
    }
    try std.testing.expectEqual(@as(usize, 4), histories);
    try std.testing.expect(seen_short);
    try std.testing.expectEqual([_]bool{ true, true, true }, seen_suffix);
    try std.testing.expectError(error.EnumerationExhausted, enumerating.beginHistory());
}

test "structured-choice audit rejects deferred interpretation" {
    const observation = @import("observation.zig");
    var history = try trace.Trace.init(std.testing.allocator, .{ .scenario = "choice-audit", .scenario_version = 1 }, .{ .transition_budget = 1 });
    defer history.deinit();
    const digest = observation.digestFeatures(&.{});
    try history.addObservation(.{ .index = 0, .digest = digest, .features = &.{} });
    try history.addChoice(.{
        .site_id = ids.stable("choice", "choice-audit.transition"),
        .site_name = "choice-audit.transition",
        .occurrence = 0,
        .enabled_ids = &.{ 2, 3 },
        .selected_id = 2,
    });
    try history.addTransition(.{ .index = 1, .id = 2, .name = "typed.two", .kind = .workload, .parameter = 2 });
    try history.addObservation(.{ .index = 1, .digest = digest, .features = &.{} });
    history.summary = .{ .transitions = 1, .final_observation_digest = digest, .property_failures = 0 };
    const audited = try auditTrace(&history);
    try std.testing.expectEqual(@as(u64, 1), audited.parameterized_choices);
    history.transitions.items[0].id = 3;
    try std.testing.expectError(error.DeferredChoiceInterpretation, auditTrace(&history));
}
