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

/// Forces a reviewed prefix and then continues with deterministic seeded
/// choices. This is useful for selecting a scenario mode while retaining full
/// scheduler exploration inside that mode.
pub const PrefixedSeeded = struct {
    prefix: []const ids.StableId,
    cursor: usize = 0,
    fallback: Seeded,

    pub fn init(prefix: []const ids.StableId, seed: u64) PrefixedSeeded {
        return .{ .prefix = prefix, .fallback = Seeded.init(seed) };
    }

    pub fn source(self: *PrefixedSeeded) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *PrefixedSeeded = @ptrCast(@alignCast(ptr));
        if (self.cursor < self.prefix.len) {
            defer self.cursor += 1;
            return self.prefix[self.cursor];
        }
        return Seeded.choose(&self.fallback, request);
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *PrefixedSeeded = @ptrCast(@alignCast(ptr));
        if (self.cursor != self.prefix.len) return error.UnusedChoicePrefix;
    }
};

/// Forces a reviewed prefix and then makes deterministic seeded choices while
/// postponing virtual time jumps until no non-time transition is runnable.
/// This models the fairness of an ordinary event loop: explicit clock-fault
/// scenarios can still choose time independently, while a clean history does
/// not expire healthy sockets with packets or wakeups already pending.
pub const PrefixedFairSeeded = struct {
    prefix: []const ids.StableId,
    cursor: usize = 0,
    fallback: Seeded,

    pub fn init(prefix: []const ids.StableId, seed: u64) PrefixedFairSeeded {
        return .{ .prefix = prefix, .fallback = Seeded.init(seed) };
    }

    pub fn source(self: *PrefixedFairSeeded) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *PrefixedFairSeeded = @ptrCast(@alignCast(ptr));
        if (self.cursor < self.prefix.len) {
            defer self.cursor += 1;
            return self.prefix[self.cursor];
        }
        var runnable_count: usize = 0;
        var weighted = false;
        var total_weight: u64 = 0;
        for (request.enabled) |candidate| {
            if (std.mem.eql(u8, candidate.name, "sim-io.time_advance") or
                std.mem.eql(u8, candidate.name, "runtime.time_advance")) continue;
            runnable_count += 1;
            if (candidate.weight == 0) return error.InvalidTransitionWeight;
            weighted = weighted or candidate.weight != 1;
            total_weight = std.math.add(u64, total_weight, candidate.weight) catch
                return error.ChoiceWeightOverflow;
        }
        if (runnable_count == 0) return Seeded.choose(&self.fallback, request);
        var ordinal = if (weighted)
            self.fallback.prng.random().uintLessThan(u64, total_weight)
        else
            self.fallback.prng.random().intRangeLessThan(u64, 0, runnable_count);
        for (request.enabled) |candidate| {
            if (std.mem.eql(u8, candidate.name, "sim-io.time_advance") or
                std.mem.eql(u8, candidate.name, "runtime.time_advance")) continue;
            const width: u64 = if (weighted) candidate.weight else 1;
            if (ordinal < width) return candidate.id;
            ordinal -= width;
        }
        unreachable;
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *PrefixedFairSeeded = @ptrCast(@alignCast(ptr));
        if (self.cursor != self.prefix.len) return error.UnusedChoicePrefix;
    }
};

/// Forces a reviewed prefix, postpones virtual time while work is runnable,
/// and keeps selecting the current fiber until it parks or finishes. This is
/// a deterministic cooperative event-loop witness policy: broad campaigns
/// should still vary scheduler choices, while production-sized clean exact
/// gates can make forward progress without spending most of their budget on
/// unrelated ready service fibers.
pub const PrefixedCooperativeSeeded = struct {
    pub const default_max_non_time_choices: usize = 256;

    prefix: []const ids.StableId,
    cursor: usize = 0,
    preferred_actor: ?ids.StableId = null,
    non_time_choices: usize = 0,
    max_non_time_choices: usize = default_max_non_time_choices,
    fallback: Seeded,

    pub fn init(prefix: []const ids.StableId, seed: u64) PrefixedCooperativeSeeded {
        return .{ .prefix = prefix, .fallback = Seeded.init(seed) };
    }

    pub fn source(self: *PrefixedCooperativeSeeded) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *PrefixedCooperativeSeeded = @ptrCast(@alignCast(ptr));
        if (self.cursor < self.prefix.len) {
            const selected_id = self.prefix[self.cursor];
            defer self.cursor += 1;
            return self.noteSelection(request, selected_id);
        }
        if (self.non_time_choices >= self.max_non_time_choices) {
            for (request.enabled) |candidate| {
                if (isTimeAdvance(candidate.name))
                    return self.noteSelection(request, candidate.id);
            }
        }
        if (self.preferred_actor) |actor_id| {
            for (request.enabled) |candidate| {
                if (candidate.actor_id == actor_id and
                    std.mem.eql(u8, candidate.name, "sim-io.task_resume"))
                    return self.noteSelection(request, candidate.id);
            }
            self.preferred_actor = null;
        }

        var runnable_count: usize = 0;
        var weighted = false;
        var total_weight: u64 = 0;
        for (request.enabled) |candidate| {
            if (isTimeAdvance(candidate.name)) continue;
            runnable_count += 1;
            if (candidate.weight == 0) return error.InvalidTransitionWeight;
            weighted = weighted or candidate.weight != 1;
            total_weight = std.math.add(u64, total_weight, candidate.weight) catch
                return error.ChoiceWeightOverflow;
        }
        const selected_id = if (runnable_count == 0)
            try Seeded.choose(&self.fallback, request)
        else blk: {
            var ordinal = if (weighted)
                self.fallback.prng.random().uintLessThan(u64, total_weight)
            else
                self.fallback.prng.random().intRangeLessThan(u64, 0, runnable_count);
            for (request.enabled) |candidate| {
                if (isTimeAdvance(candidate.name)) continue;
                const width: u64 = if (weighted) candidate.weight else 1;
                if (ordinal < width) break :blk candidate.id;
                ordinal -= width;
            }
            unreachable;
        };
        return self.noteSelection(request, selected_id);
    }

    fn noteSelection(self: *PrefixedCooperativeSeeded, request: Request, selected_id: ids.StableId) ids.StableId {
        for (request.enabled) |candidate| {
            if (candidate.id != selected_id) continue;
            if (isTimeAdvance(candidate.name)) {
                self.non_time_choices = 0;
                self.preferred_actor = null;
                return selected_id;
            }
            self.non_time_choices +|= 1;
            if (std.mem.eql(u8, candidate.name, "sim-io.task_resume") or
                std.mem.eql(u8, candidate.name, "sim-io.futex_wake") or
                std.mem.eql(u8, candidate.name, "sim-io.external_wake"))
                self.preferred_actor = candidate.actor_id;
            break;
        }
        return selected_id;
    }

    fn isTimeAdvance(name: []const u8) bool {
        return std.mem.eql(u8, name, "sim-io.time_advance") or
            std.mem.eql(u8, name, "runtime.time_advance");
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *PrefixedCooperativeSeeded = @ptrCast(@alignCast(ptr));
        if (self.cursor != self.prefix.len) return error.UnusedChoicePrefix;
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
    expected_transitions: []const trace.TransitionRecord = &.{},
    cursor: usize = 0,
    diagnostics: bool = true,
    recent_actual: [8]transition.Transition = undefined,
    recent_actual_count: usize = 0,

    pub fn source(self: *Replay) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *Replay = @ptrCast(@alignCast(ptr));
        if (self.cursor >= self.records.len) return error.ReplayChoiceExhausted;
        const record = self.records[self.cursor];
        if (record.site_id != request.site_id or !std.mem.eql(u8, record.site_name, request.site_name) or record.occurrence != request.occurrence) return error.ReplayChoiceSiteDiverged;
        if (record.enabled_ids.len != request.enabled.len) {
            if (self.diagnostics) self.logEnabledSetDivergence(self.cursor, record, request);
            return error.ReplayEnabledSetDiverged;
        }
        for (request.enabled, record.enabled_ids) |enabled, recorded_id| {
            if (enabled.id != recorded_id) {
                if (self.diagnostics) self.logEnabledSetDivergence(self.cursor, record, request);
                return error.ReplayEnabledSetDiverged;
            }
        }
        var selected: ?transition.Transition = null;
        for (request.enabled) |enabled| {
            if (enabled.id != record.selected_id) continue;
            selected = enabled;
            self.recent_actual[self.recent_actual_count % self.recent_actual.len] = enabled;
            self.recent_actual_count += 1;
            break;
        }
        if (selected) |actual| {
            if (self.cursor < self.expected_transitions.len) {
                const expected = self.expected_transitions[self.cursor];
                if (actual.id != expected.id or
                    !std.mem.eql(u8, actual.name, expected.name) or
                    actual.kind != expected.kind or
                    actual.actor_id != expected.actor_id or
                    actual.resource_id != expected.resource_id or
                    actual.parameter != expected.parameter or
                    actual.payloadDigest() != expected.payload_digest)
                {
                    if (self.diagnostics) self.logSelectedTransitionDivergence(self.cursor, expected, actual);
                    return error.ReplaySelectedTransitionDiverged;
                }
            }
        }
        self.cursor += 1;
        return record.selected_id;
    }

    fn logSelectedTransitionDivergence(_: *const Replay, index: usize, expected: trace.TransitionRecord, actual: transition.Transition) void {
        std.debug.print(
            "exact replay selected transition diverged choice={d}\nexpected id={d} name={s} kind={s} actor={?d} resource={?d} parameter={d} payload={d}\nactual id={d} name={s} kind={s} actor={?d} resource={?d} parameter={d} payload={d}\n",
            .{
                index,
                expected.id,
                expected.name,
                @tagName(expected.kind),
                expected.actor_id,
                expected.resource_id,
                expected.parameter,
                expected.payload_digest,
                actual.id,
                actual.name,
                @tagName(actual.kind),
                actual.actor_id,
                actual.resource_id,
                actual.parameter,
                actual.payloadDigest(),
            },
        );
    }

    fn logEnabledSetDivergence(self: *const Replay, index: usize, record: trace.ChoiceRecord, request: Request) void {
        std.debug.print(
            "exact replay enabled set diverged choice={d} site={s} occurrence={d} expected_count={d} actual_count={d} recorded_selected={d}",
            .{ index, request.site_name, request.occurrence, record.enabled_ids.len, request.enabled.len, record.selected_id },
        );
        const diagnostic_limit = 32;
        for (record.enabled_ids[0..@min(record.enabled_ids.len, diagnostic_limit)], 0..) |id, ordinal|
            std.debug.print("\nexpected enabled[{d}]={d}", .{ ordinal, id });
        for (request.enabled[0..@min(request.enabled.len, diagnostic_limit)], 0..) |enabled, ordinal|
            std.debug.print(
                "\nactual enabled[{d}]={d} name={s} actor={?d} resource={?d} parameter={d}",
                .{ ordinal, enabled.id, enabled.name, enabled.actor_id, enabled.resource_id, enabled.parameter },
            );
        if (index < self.expected_transitions.len) {
            const expected = self.expected_transitions[index];
            std.debug.print(
                "\nrecorded selected transition id={d} name={s} actor={?d} resource={?d} parameter={d}",
                .{ expected.id, expected.name, expected.actor_id, expected.resource_id, expected.parameter },
            );
        }
        const recent_count = @min(self.recent_actual_count, self.recent_actual.len);
        const recent_start = self.recent_actual_count - recent_count;
        for (0..recent_count) |offset| {
            const choice_index = index - recent_count + offset;
            const actual = self.recent_actual[(recent_start + offset) % self.recent_actual.len];
            std.debug.print(
                "\nprior actual[{d}] id={d} name={s} actor={?d} resource={?d} parameter={d}",
                .{ choice_index, actual.id, actual.name, actual.actor_id, actual.resource_id, actual.parameter },
            );
            if (choice_index < self.expected_transitions.len) {
                const expected = self.expected_transitions[choice_index];
                std.debug.print(
                    "\nprior expected[{d}] id={d} name={s} actor={?d} resource={?d} parameter={d}",
                    .{ choice_index, expected.id, expected.name, expected.actor_id, expected.resource_id, expected.parameter },
                );
            }
        }
        std.debug.print("\n", .{});
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
    var replay = Replay{ .records = &records, .diagnostics = false };
    try std.testing.expectError(error.ReplayEnabledSetDiverged, replay.source().choose(.{ .site_id = 7, .site_name = "site", .occurrence = 0, .enabled = &enabled }));
}

test "replay rejects selected transition metadata hidden behind the same stable id" {
    const records = [_]trace.ChoiceRecord{.{ .site_id = 7, .site_name = "site", .occurrence = 0, .enabled_ids = &.{1}, .selected_id = 1 }};
    const expected_transition = transition.Transition{
        .id = 1,
        .name = "packet",
        .kind = .scheduler,
        .actor_id = 2,
        .resource_id = 3,
        .parameter = 315,
    };
    const expected = [_]trace.TransitionRecord{.{
        .index = 0,
        .id = 1,
        .name = "packet",
        .kind = .scheduler,
        .actor_id = 2,
        .resource_id = 3,
        .parameter = 315,
        .payload_digest = expected_transition.payloadDigest(),
    }};
    const enabled = [_]transition.Transition{.{
        .id = 1,
        .name = "packet",
        .kind = .scheduler,
        .actor_id = 2,
        .resource_id = 3,
        .parameter = 641,
    }};
    var replay = Replay{
        .records = &records,
        .expected_transitions = &expected,
        .diagnostics = false,
    };
    try std.testing.expectError(error.ReplaySelectedTransitionDiverged, replay.source().choose(.{
        .site_id = 7,
        .site_name = "site",
        .occurrence = 0,
        .enabled = &enabled,
    }));
}

test "replay retains the most recent selected transitions in logical order" {
    var records: [10]trace.ChoiceRecord = undefined;
    for (&records, 0..) |*record, occurrence| record.* = .{
        .site_id = 7,
        .site_name = "site",
        .occurrence = occurrence,
        .enabled_ids = &.{1},
        .selected_id = 1,
    };
    var replay = Replay{ .records = &records, .diagnostics = false };
    const source = replay.source();
    for (0..records.len) |occurrence| {
        const enabled = [_]transition.Transition{.{
            .id = 1,
            .name = "selected",
            .kind = .scheduler,
            .parameter = @intCast(occurrence),
        }};
        try std.testing.expectEqual(@as(u64, 1), try source.choose(.{
            .site_id = 7,
            .site_name = "site",
            .occurrence = occurrence,
            .enabled = &enabled,
        }));
    }
    try source.finish();
    try std.testing.expectEqual(@as(usize, records.len), replay.recent_actual_count);
    const oldest = replay.recent_actual_count - replay.recent_actual.len;
    for (0..replay.recent_actual.len) |offset| {
        const actual = replay.recent_actual[(oldest + offset) % replay.recent_actual.len];
        try std.testing.expectEqual(@as(i64, @intCast(offset + 2)), actual.parameter);
    }
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

test "prefixed seeded source forces reviewed setup then explores its suffix" {
    const enabled = [_]transition.Transition{
        .{ .id = 1, .name = "one", .kind = .workload },
        .{ .id = 2, .name = "two", .kind = .workload },
    };
    var prefixed = PrefixedSeeded.init(&.{2}, 17);
    const source = prefixed.source();
    try std.testing.expectEqual(@as(u64, 2), try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed",
        .occurrence = 0,
        .enabled = &enabled,
    }));
    const suffix = try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed",
        .occurrence = 1,
        .enabled = &enabled,
    });
    try std.testing.expect(suffix == 1 or suffix == 2);
    try source.finish();
}

test "prefixed fair seeded source postpones time while application work is runnable" {
    const enabled = [_]transition.Transition{
        .{ .id = 1, .name = "sim-io.time_advance", .kind = .scheduler },
        .{ .id = 2, .name = "request.ready", .kind = .workload },
    };
    var prefixed = PrefixedFairSeeded.init(&.{1}, 19);
    const source = prefixed.source();
    try std.testing.expectEqual(@as(u64, 1), try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed-fair",
        .occurrence = 0,
        .enabled = &enabled,
    }));
    try std.testing.expectEqual(@as(u64, 2), try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed-fair",
        .occurrence = 1,
        .enabled = &enabled,
    }));
    try source.finish();
}

test "prefixed cooperative source runs one fiber until it parks" {
    const both = [_]transition.Transition{
        .{ .id = 1, .name = "sim-io.time_advance", .kind = .scheduler },
        .{ .id = 2, .name = "sim-io.task_resume", .kind = .scheduler, .actor_id = 20 },
        .{ .id = 3, .name = "sim-io.task_resume", .kind = .scheduler, .actor_id = 30 },
    };
    var cooperative = PrefixedCooperativeSeeded.init(&.{1}, 23);
    const source = cooperative.source();
    try std.testing.expectEqual(@as(u64, 1), try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed-cooperative",
        .occurrence = 0,
        .enabled = &both,
    }));
    const first_task = try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed-cooperative",
        .occurrence = 1,
        .enabled = &both,
    });
    try std.testing.expect(first_task == 2 or first_task == 3);
    try std.testing.expectEqual(first_task, try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed-cooperative",
        .occurrence = 2,
        .enabled = &both,
    }));

    const other = if (first_task == 2) both[2] else both[1];
    try std.testing.expectEqual(other.id, try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed-cooperative",
        .occurrence = 3,
        .enabled = &.{other},
    }));
    try std.testing.expectEqual(other.id, try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed-cooperative",
        .occurrence = 4,
        .enabled = &both,
    }));
    try source.finish();
}

test "prefixed cooperative source hands a selected wake to its waiter" {
    const wake = transition.Transition{
        .id = 2,
        .name = "sim-io.external_wake",
        .kind = .scheduler,
        .actor_id = 20,
    };
    const unrelated = transition.Transition{
        .id = 3,
        .name = "sim-io.task_resume",
        .kind = .scheduler,
        .actor_id = 30,
    };
    const waiter = transition.Transition{
        .id = 4,
        .name = "sim-io.task_resume",
        .kind = .scheduler,
        .actor_id = 20,
    };
    var cooperative = PrefixedCooperativeSeeded.init(&.{wake.id}, 23);
    const source = cooperative.source();
    try std.testing.expectEqual(wake.id, try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed-cooperative-wake",
        .occurrence = 0,
        .enabled = &.{ wake, unrelated },
    }));
    try std.testing.expectEqual(waiter.id, try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed-cooperative-wake",
        .occurrence = 1,
        .enabled = &.{ unrelated, waiter },
    }));
    try source.finish();
}

test "prefixed cooperative source eventually advances time around runnable service work" {
    const time = transition.Transition{ .id = 1, .name = "sim-io.time_advance", .kind = .scheduler };
    const service = transition.Transition{ .id = 2, .name = "service.poll", .kind = .scheduler };
    var cooperative = PrefixedCooperativeSeeded.init(&.{ service.id, service.id }, 23);
    cooperative.max_non_time_choices = 2;
    const source = cooperative.source();
    for (0..2) |occurrence| try std.testing.expectEqual(service.id, try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed-cooperative-time",
        .occurrence = occurrence,
        .enabled = &.{ time, service },
    }));
    try std.testing.expectEqual(time.id, try source.choose(.{
        .site_id = 9,
        .site_name = "prefixed-cooperative-time",
        .occurrence = 2,
        .enabled = &.{ time, service },
    }));
    try source.finish();
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
