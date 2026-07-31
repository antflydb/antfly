// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const lsm_table_file = @import("../lsm/table_file.zig");
const state_mod = @import("state.zig");
const repository_mod = @import("repository.zig");
const runtime_mod = @import("runtime.zig");
const compaction_scheduler_mod = @import("compaction_scheduler.zig");

const State = state_mod.State;
const Run = repository_mod.Run;
pub const max_remembered_compaction_run_ids = 64;
pub const max_exact_l0_overlap_runs = 64;

const CompactionWork = struct {
    score: u64,
    input_runs: usize,
    input_bytes: u64,
    io_bytes: u64,
    run_ids: []u64,
    key_range: ?compaction_scheduler_mod.KeyRange,

    fn deinit(self: *CompactionWork, allocator: std.mem.Allocator) void {
        if (self.run_ids.len > 0) allocator.free(self.run_ids);
        self.* = undefined;
    }
};

pub const CompactionPlan = struct {
    source_level: u32,
    source_start: usize,
    source_len: usize,
    target_start: usize,
    target_len: usize,
    output_level: u32,
};

pub const RememberedCompaction = struct {
    plan: CompactionPlan,
    run_ids: [max_remembered_compaction_run_ids]u64 = undefined,
    run_count: usize = 0,
    input_runs: usize = 0,
    input_bytes: u64 = 0,
    score: u64 = 0,
};

const CompactionSelectionStats = struct {
    oversized_skips: u64 = 0,
};

const PlanScore = struct {
    rewrite_bytes: u64,
    target_bytes: u64,
    source_bytes: u64,
    source_len: usize,
    target_len: usize,
    source_start: usize,

    fn betterThan(self: PlanScore, other: PlanScore) bool {
        if (self.rewrite_bytes != other.rewrite_bytes) return self.rewrite_bytes < other.rewrite_bytes;
        if (self.target_bytes != other.target_bytes) return self.target_bytes < other.target_bytes;
        if (self.source_bytes != other.source_bytes) return self.source_bytes < other.source_bytes;
        if (self.target_len != other.target_len) return self.target_len < other.target_len;
        if (self.source_len != other.source_len) return self.source_len < other.source_len;
        return self.source_start < other.source_start;
    }
};

const ScoredCompactionPlan = struct {
    plan: CompactionPlan,
    priority: u64,
    tie: PlanScore,

    fn betterThan(self: ScoredCompactionPlan, other: ScoredCompactionPlan) bool {
        if (self.priority != other.priority) {
            const lower = @min(self.priority, other.priority);
            const delta = @max(self.priority, other.priority) - lower;
            // Scores within ten percent represent the same pressure episode.
            // Prefer the lower-rewrite closure in that band; otherwise a
            // marginally higher L0 score can repeatedly rewrite an almost as
            // pressured lower level. Materially higher pressure still wins.
            if (lower == 0 or delta > lower / 10) return self.priority > other.priority;
        }
        return self.tie.betterThan(other.tie);
    }
};

pub fn maybeFlushMutable(comptime BackendType: type, backend: *BackendType) !void {
    try maybeFlushMutableWithThreshold(BackendType, backend, backend.options.flush_threshold);
}

pub fn maybeFlushMutableWithThreshold(comptime BackendType: type, backend: *BackendType, flush_threshold: usize) !void {
    if (backend.mutable.entries.items.len < flush_threshold) return;
    try flushMutable(BackendType, backend);
}

pub fn flushMutable(comptime BackendType: type, backend: *BackendType) !void {
    if (backend.mutable.entries.items.len == 0) return;
    const start_ns = if (@hasDecl(BackendType, "writeStatsNowNs")) backend.writeStatsNowNs() else 0;
    var flushed = backend.mutable;
    backend.mutable = .{};
    errdefer flushed.deinit(backend.allocator);
    const input_entries = flushed.entries.items.len;
    var new_runs = try makeRuns(BackendType, backend, &flushed);
    errdefer discardOutputRuns(BackendType, backend, &new_runs);
    if (@hasDecl(BackendType, "recordFlushWriteStats")) {
        const elapsed_ns = if (@hasDecl(BackendType, "writeStatsNowNs")) elapsedNs(BackendType, backend, start_ns) else 0;
        backend.recordFlushWriteStats(input_entries, new_runs.items, elapsed_ns);
    }
    try appendOwnedRuns(&backend.runs, backend.allocator, &new_runs);
    sortRuns(backend.runs.items);
    if (@hasDecl(BackendType, "bulkIngestActive") and backend.bulkIngestActive()) {
        if (@hasDecl(BackendType, "markManifestDirty")) backend.markManifestDirty();
        return;
    }
    try maybeCompactRuns(BackendType, backend);
    if (@hasDecl(BackendType, "persistManifest")) {
        try backend.persistManifest();
    } else if (backend.root_dir != null) {
        try repository_mod.persistManifestWithStorage(
            backend.storage.?,
            backend.allocator,
            backend.root_dir.?,
            backend.next_run_id,
            backend.runs.items,
            backend.obsolete_paths.items,
        );
    }
}

pub fn maybeCompactRuns(comptime BackendType: type, backend: *BackendType) !void {
    while (selectCompactionPlan(
        backend.runs.items,
        backend.options.compact_threshold_runs,
        backend.options.l0_overlap_compact_threshold_runs,
        backend.options.level_target_runs_base,
        backend.options.level_target_runs_multiplier,
        backend.options.level_target_bytes_base,
        backend.options.level_target_bytes_multiplier,
        0,
        false,
    )) |plan| {
        try compactPlanAt(BackendType, backend, plan);
    }
}

pub fn maybeCompactRunsScheduled(comptime BackendType: type, backend: *BackendType, score: u64) !bool {
    return maybeCompactRunsScheduledWithL0Limit(BackendType, backend, backend.options.compact_threshold_runs, score);
}

pub fn maybeCompactRunsScheduledWithL0Limit(
    comptime BackendType: type,
    backend: *BackendType,
    l0_limit: usize,
    score: u64,
) !bool {
    if (try compactRememberedPlanIfValid(BackendType, backend)) return true;

    var selection_stats: CompactionSelectionStats = .{};
    const plan = selectCompactionPlanWithStats(
        backend.runs.items,
        l0_limit,
        backend.options.l0_overlap_compact_threshold_runs,
        backend.options.level_target_runs_base,
        backend.options.level_target_runs_multiplier,
        backend.options.level_target_bytes_base,
        backend.options.level_target_bytes_multiplier,
        backend.options.max_compaction_input_bytes,
        allowOversizedSingleCompactionInput(backend),
        &selection_stats,
    ) orelse {
        noteCompactionSelectionStats(BackendType, backend, selection_stats);
        return false;
    };
    noteCompactionSelectionStats(BackendType, backend, selection_stats);

    var work = try compactionWorkForPlan(backend.allocator, backend.runs.items, plan, score);
    defer work.deinit(backend.allocator);
    var grant = backend.acquireCompactionGrant(work) orelse {
        rememberDeniedCompaction(BackendType, backend, plan, score);
        return false;
    };
    defer grant.complete();
    try compactPlanAt(BackendType, backend, plan);
    return true;
}

pub fn compactOldestPair(comptime BackendType: type, backend: *BackendType) !void {
    const plan = selectL0Compaction(backend.runs.items, 0, 0, false) orelse return;
    try compactPlanAt(BackendType, backend, plan);
}

pub fn compactL0ToLimit(comptime BackendType: type, backend: *BackendType, l0_limit: usize) !void {
    const plan = selectL0Compaction(backend.runs.items, l0_limit, 0, false) orelse return;
    try compactPlanAt(BackendType, backend, plan);
}

pub fn compactL0ToLimitScheduled(comptime BackendType: type, backend: *BackendType, l0_limit: usize, score: u64) !bool {
    if (try compactRememberedPlanIfValid(BackendType, backend)) return true;

    var selection_stats: CompactionSelectionStats = .{};
    const plan = selectL0CompactionWithStats(
        backend.runs.items,
        l0_limit,
        backend.options.max_compaction_input_bytes,
        allowOversizedSingleCompactionInput(backend),
        &selection_stats,
    ) orelse {
        noteCompactionSelectionStats(BackendType, backend, selection_stats);
        return false;
    };
    noteCompactionSelectionStats(BackendType, backend, selection_stats);
    var work = try compactionWorkForPlan(backend.allocator, backend.runs.items, plan, score);
    defer work.deinit(backend.allocator);
    var grant = backend.acquireCompactionGrant(work) orelse {
        rememberDeniedCompaction(BackendType, backend, plan, score);
        return false;
    };
    defer grant.complete();
    try compactPlanAt(BackendType, backend, plan);
    return true;
}

pub fn compactL0ToLimitScheduledWithinBudget(
    comptime BackendType: type,
    backend: *BackendType,
    l0_limit: usize,
    score: u64,
    max_input_bytes: ?u64,
) !bool {
    const option_limit = backend.options.max_compaction_input_bytes;
    const effective_limit = if (max_input_bytes) |explicit_limit|
        if (option_limit > 0) @min(option_limit, explicit_limit) else explicit_limit
    else
        option_limit;
    var selection_stats: CompactionSelectionStats = .{};
    const plan = selectL0CompactionWithStats(
        backend.runs.items,
        l0_limit,
        effective_limit,
        max_input_bytes == null and allowOversizedSingleCompactionInput(backend),
        &selection_stats,
    ) orelse {
        noteCompactionSelectionStats(BackendType, backend, selection_stats);
        return false;
    };
    noteCompactionSelectionStats(BackendType, backend, selection_stats);
    var work = try compactionWorkForPlan(backend.allocator, backend.runs.items, plan, score);
    defer work.deinit(backend.allocator);
    var grant = backend.acquireCompactionGrant(work) orelse {
        return false;
    };
    defer grant.complete();
    try compactPlanAt(BackendType, backend, plan);
    return true;
}

pub fn compactAllRuns(comptime BackendType: type, backend: *BackendType) !void {
    while (selectCompactionPlan(
        backend.runs.items,
        0,
        0,
        backend.options.level_target_runs_base,
        backend.options.level_target_runs_multiplier,
        backend.options.level_target_bytes_base,
        backend.options.level_target_bytes_multiplier,
        0,
        false,
    )) |plan| {
        try compactPlanAt(BackendType, backend, plan);
    }
}

fn allowOversizedSingleCompactionInput(backend: anytype) bool {
    const OptionsType = @TypeOf(backend.options);
    if (!@hasField(OptionsType, "max_compaction_input_allow_oversized_single_job")) return false;
    return backend.options.max_compaction_input_allow_oversized_single_job;
}

fn compactionWorkForPlan(allocator: std.mem.Allocator, runs: []const Run, plan: CompactionPlan, score: u64) !CompactionWork {
    const total_runs = plan.source_len + plan.target_len;
    const run_ids = try allocator.alloc(u64, total_runs);
    errdefer allocator.free(run_ids);

    var input_runs: usize = 0;
    var input_bytes: u64 = 0;
    var run_count: usize = 0;
    var key_range: ?compaction_scheduler_mod.KeyRange = null;
    for (runs[plan.source_start .. plan.source_start + plan.source_len]) |run| {
        input_runs += 1;
        input_bytes +|= run.size_bytes;
        includeRunInWorkKeyRange(&key_range, plan.output_level, run);
        run_ids[run_count] = run.id;
        run_count += 1;
    }
    for (runs[plan.target_start .. plan.target_start + plan.target_len]) |run| {
        input_runs += 1;
        input_bytes +|= run.size_bytes;
        includeRunInWorkKeyRange(&key_range, plan.output_level, run);
        run_ids[run_count] = run.id;
        run_count += 1;
    }
    return .{
        .score = score,
        .input_runs = input_runs,
        .input_bytes = input_bytes,
        .io_bytes = input_bytes +| input_bytes,
        .run_ids = run_ids,
        .key_range = key_range,
    };
}

fn includeRunInWorkKeyRange(key_range: *?compaction_scheduler_mod.KeyRange, output_level: u32, run: Run) void {
    if (key_range.* == null) {
        key_range.* = .{
            .output_level = output_level,
            .smallest_namespace_name = run.smallest_namespace_name,
            .smallest_key = run.smallest_key,
            .largest_namespace_name = run.largest_namespace_name,
            .largest_key = run.largest_key,
        };
        return;
    }
    var range = key_range.*.?;
    if (compareRunBound(run.smallest_namespace_name, run.smallest_key, range.smallest_namespace_name, range.smallest_key) == .lt) {
        range.smallest_namespace_name = run.smallest_namespace_name;
        range.smallest_key = run.smallest_key;
    }
    if (compareRunBound(run.largest_namespace_name, run.largest_key, range.largest_namespace_name, range.largest_key) == .gt) {
        range.largest_namespace_name = run.largest_namespace_name;
        range.largest_key = run.largest_key;
    }
    key_range.* = range;
}

fn planWithinInputBudget(runs: []const Run, plan: CompactionPlan, max_input_bytes: u64) bool {
    if (max_input_bytes == 0) return true;
    return compactionInputBytes(runs, plan) <= max_input_bytes;
}

fn compactionInputBytes(runs: []const Run, plan: CompactionPlan) u64 {
    var input_bytes: u64 = 0;
    for (runs[plan.source_start .. plan.source_start + plan.source_len]) |run| {
        input_bytes +|= run.size_bytes;
    }
    for (runs[plan.target_start .. plan.target_start + plan.target_len]) |run| {
        input_bytes +|= run.size_bytes;
    }
    return input_bytes;
}

fn planScoreForPlan(runs: []const Run, plan: CompactionPlan) PlanScore {
    const source_bytes = sumRunBytes(runs[plan.source_start .. plan.source_start + plan.source_len]);
    const target_bytes = sumRunBytes(runs[plan.target_start .. plan.target_start + plan.target_len]);
    return .{
        .rewrite_bytes = source_bytes +| target_bytes,
        .target_bytes = target_bytes,
        .source_bytes = source_bytes,
        .source_len = plan.source_len,
        .target_len = plan.target_len,
        .source_start = plan.source_start,
    };
}

fn scoredPlan(runs: []const Run, plan: CompactionPlan, priority: u64) ScoredCompactionPlan {
    return .{
        .plan = plan,
        .priority = @max(@as(u64, 1), priority),
        .tie = planScoreForPlan(runs, plan),
    };
}

fn maybeAdoptBest(best: *?ScoredCompactionPlan, candidate: ?ScoredCompactionPlan) void {
    const next = candidate orelse return;
    if (best.* == null or next.betterThan(best.*.?)) best.* = next;
}

fn noteOversizedSelectionSkip(stats: ?*CompactionSelectionStats, max_input_bytes: u64) void {
    if (max_input_bytes == 0) return;
    if (stats) |selection_stats| selection_stats.oversized_skips +|= 1;
}

fn noteCompactionSelectionStats(comptime BackendType: type, backend: *BackendType, stats: CompactionSelectionStats) void {
    if (!@hasField(BackendType, "compaction_scheduler")) return;
    if (stats.oversized_skips > 0) backend.compaction_scheduler.noteOversizedSkips(stats.oversized_skips);
}

fn compactRememberedPlanIfValid(comptime BackendType: type, backend: *BackendType) !bool {
    if (!@hasField(BackendType, "remembered_compaction")) return false;
    const remembered = backend.remembered_compaction orelse return false;
    backend.compaction_scheduler.noteRememberedRetry();

    const plan = validateRememberedCompaction(backend.runs.items, remembered) orelse {
        backend.remembered_compaction = null;
        backend.compaction_scheduler.noteRememberedStale();
        return false;
    };

    var work = try compactionWorkForPlan(backend.allocator, backend.runs.items, plan, remembered.score);
    defer work.deinit(backend.allocator);
    if (backend.options.max_compaction_input_bytes > 0 and work.input_bytes > backend.options.max_compaction_input_bytes) {
        backend.remembered_compaction = null;
        backend.compaction_scheduler.noteRememberedStale();
        return false;
    }
    var grant = backend.acquireCompactionGrant(work) orelse {
        backend.compaction_scheduler.noteConflictDenial();
        return false;
    };
    defer grant.complete();

    backend.remembered_compaction = null;
    backend.compaction_scheduler.noteRememberedHit();
    try compactPlanAt(BackendType, backend, plan);
    return true;
}

fn rememberDeniedCompaction(comptime BackendType: type, backend: *BackendType, plan: CompactionPlan, score: u64) void {
    if (!@hasField(BackendType, "remembered_compaction")) return;
    const remembered = rememberCompactionPlan(backend.runs.items, plan, score) orelse return;
    backend.remembered_compaction = remembered;
    backend.compaction_scheduler.noteRememberedCandidate();
}

fn rememberCompactionPlan(runs: []const Run, plan: CompactionPlan, score: u64) ?RememberedCompaction {
    const total_runs = plan.source_len + plan.target_len;
    if (total_runs == 0 or total_runs > max_remembered_compaction_run_ids) return null;
    if (!planInBounds(runs, plan)) return null;

    var remembered = RememberedCompaction{
        .plan = plan,
        .run_count = total_runs,
        .input_runs = total_runs,
        .input_bytes = compactionInputBytes(runs, plan),
        .score = score,
    };
    var idx: usize = 0;
    for (runs[plan.source_start .. plan.source_start + plan.source_len]) |run| {
        remembered.run_ids[idx] = run.id;
        idx += 1;
    }
    for (runs[plan.target_start .. plan.target_start + plan.target_len]) |run| {
        remembered.run_ids[idx] = run.id;
        idx += 1;
    }
    return remembered;
}

fn validateRememberedCompaction(runs: []const Run, remembered: RememberedCompaction) ?CompactionPlan {
    const plan = remembered.plan;
    if (remembered.run_count == 0 or remembered.run_count != plan.source_len + plan.target_len) return null;
    if (!planInBounds(runs, plan)) return null;

    var idx: usize = 0;
    for (runs[plan.source_start .. plan.source_start + plan.source_len]) |run| {
        if (idx >= remembered.run_count or run.id != remembered.run_ids[idx]) return null;
        idx += 1;
    }
    for (runs[plan.target_start .. plan.target_start + plan.target_len]) |run| {
        if (idx >= remembered.run_count or run.id != remembered.run_ids[idx]) return null;
        idx += 1;
    }
    return plan;
}

fn planInBounds(runs: []const Run, plan: CompactionPlan) bool {
    if (plan.source_len == 0) return false;
    if (plan.source_start > runs.len or plan.source_len > runs.len - plan.source_start) return false;
    if (plan.target_start > runs.len or plan.target_len > runs.len - plan.target_start) return false;
    return true;
}

pub fn compactOldestWindow(comptime BackendType: type, backend: *BackendType, window_len: usize) !void {
    _ = window_len;
    const plan = selectL0Compaction(backend.runs.items, 0, 0, false) orelse return;
    try compactPlanAt(BackendType, backend, plan);
}

pub fn sortRuns(runs: []Run) void {
    std.sort.pdq(Run, runs, {}, struct {
        fn lessThan(_: void, lhs: Run, rhs: Run) bool {
            if (lhs.level != rhs.level) return lhs.level < rhs.level;
            if (lhs.level == 0) return lhs.id > rhs.id;
            const bound_order = compareRunBound(
                lhs.smallest_namespace_name,
                lhs.smallest_key,
                rhs.smallest_namespace_name,
                rhs.smallest_key,
            );
            if (bound_order != .eq) return bound_order == .lt;
            return lhs.id < rhs.id;
        }
    }.lessThan);
}

fn compactPlanAt(comptime BackendType: type, backend: *BackendType, plan: CompactionPlan) !void {
    if (comptime supportsUnlockedBackendCompaction(BackendType)) {
        try compactPlanAtWithUnlockedBuild(BackendType, backend, plan);
        return;
    }
    try compactPlanAtLockedOnly(BackendType, backend, plan);
}

fn supportsUnlockedBackendCompaction(comptime BackendType: type) bool {
    return @hasField(BackendType, "mu") and
        @hasField(BackendType, "storage") and
        @hasField(BackendType, "root_dir") and
        @hasDecl(BackendType, "retainReader") and
        @hasDecl(BackendType, "releaseReader");
}

fn compactPlanAtLockedOnly(comptime BackendType: type, backend: *BackendType, plan: CompactionPlan) !void {
    if (plan.source_len == 0) return;
    const start_ns = if (@hasDecl(BackendType, "writeStatsNowNs")) backend.writeStatsNowNs() else 0;

    var selected = try backend.allocator.alloc(*Run, plan.source_len + plan.target_len);
    defer backend.allocator.free(selected);
    var selected_len: usize = 0;
    for (backend.runs.items[plan.source_start .. plan.source_start + plan.source_len]) |*run| {
        selected[selected_len] = run;
        selected_len += 1;
    }
    for (backend.runs.items[plan.target_start .. plan.target_start + plan.target_len]) |*run| {
        selected[selected_len] = run;
        selected_len += 1;
    }
    const input_bytes = sumRunPtrBytes(selected[0..selected_len]);

    var compacted_runs = if (backend.root_dir != null)
        try makePersistedRunsFromSelectedRuns(BackendType, backend, selected[0..selected_len], plan.output_level)
    else
        try makeStateRunsFromSelectedRuns(BackendType, backend, selected[0..selected_len], plan.output_level);
    errdefer discardOutputRuns(BackendType, backend, &compacted_runs);

    var retained = std.ArrayListUnmanaged(Run).empty;
    errdefer {
        for (retained.items) |*run| run.deinit(backend.allocator);
        retained.deinit(backend.allocator);
    }
    try retained.ensureTotalCapacity(backend.allocator, backend.runs.items.len - selected_len + compacted_runs.items.len);

    var obsolete_runs = std.ArrayListUnmanaged(Run).empty;
    errdefer {
        for (obsolete_runs.items) |*run| run.deinit(backend.allocator);
        obsolete_runs.deinit(backend.allocator);
    }

    var remove = try backend.allocator.alloc(bool, backend.runs.items.len);
    defer backend.allocator.free(remove);
    @memset(remove, false);
    for (plan.source_start..plan.source_start + plan.source_len) |i| remove[i] = true;
    for (plan.target_start..plan.target_start + plan.target_len) |i| remove[i] = true;

    for (backend.runs.items, 0..) |*run, i| {
        if (remove[i]) {
            if (run.path) |path| try queueObsoleteFilePath(BackendType, backend, try backend.allocator.dupe(u8, path));
            try obsolete_runs.append(backend.allocator, run.*);
            run.* = undefined;
            continue;
        }
        retained.appendAssumeCapacity(run.*);
    }
    var output_bytes: u64 = 0;
    for (compacted_runs.items) |run| {
        output_bytes +|= run.size_bytes;
        retained.appendAssumeCapacity(run);
    }
    if (@hasDecl(BackendType, "recordCompactionWriteStats")) {
        const elapsed_ns = if (@hasDecl(BackendType, "writeStatsNowNs")) elapsedNs(BackendType, backend, start_ns) else 0;
        backend.recordCompactionWriteStats(compacted_runs.items, elapsed_ns);
    }
    disarmRunList(&compacted_runs);
    compacted_runs.deinit(backend.allocator);
    compacted_runs = .empty;
    sortRuns(retained.items);

    if (@hasField(BackendType, "compaction_stats")) {
        backend.compaction_stats.compactions += 1;
        backend.compaction_stats.input_runs += selected_len;
        backend.compaction_stats.input_bytes += input_bytes;
        backend.compaction_stats.output_bytes += output_bytes;
    }

    backend.runs.deinit(backend.allocator);
    backend.runs = retained;
    retained = .empty;
    try backend.queueObsoleteRuns(obsolete_runs);
}

fn compactPlanAtWithUnlockedBuild(comptime BackendType: type, backend: *BackendType, plan: CompactionPlan) !void {
    if (plan.source_len == 0) return;
    const start_ns = if (@hasDecl(BackendType, "writeStatsNowNs")) backend.writeStatsNowNs() else 0;

    var selected_runs = std.ArrayListUnmanaged(Run).empty;
    errdefer releaseCompactionSnapshots(BackendType, backend, &selected_runs);
    try appendPlanRunSnapshots(BackendType, backend, plan, &selected_runs);
    if (selected_runs.items.len == 0) return;

    var selected = try backend.allocator.alloc(*Run, selected_runs.items.len);
    defer backend.allocator.free(selected);
    var selected_run_ids = try backend.allocator.alloc(u64, selected_runs.items.len);
    defer backend.allocator.free(selected_run_ids);
    for (selected_runs.items, 0..) |*run, i| {
        selected[i] = run;
        selected_run_ids[i] = run.id;
    }
    const input_bytes = sumRunPtrBytes(selected);
    const reserved_run_ids = @max(@as(u64, 1), countRunPtrEntries(selected));
    const reserved_run_id_start = backend.next_run_id;
    backend.next_run_id +|= reserved_run_ids;
    const reserved_run_id_end = backend.next_run_id;

    if (@hasDecl(BackendType, "retainReaderKind")) {
        backend.retainReaderKind(.compaction);
    } else {
        backend.retainReader();
    }
    runtime_mod.unlockBackend(BackendType, backend, true);

    var build_result: std.ArrayListUnmanaged(Run) = .empty;
    var build_result_valid = false;
    var build_err: ?anyerror = null;
    build_result = buildCompactedRunsFromSnapshots(
        BackendType,
        backend,
        selected,
        plan.output_level,
        reserved_run_id_start,
        reserved_run_id_end,
    ) catch |err| blk: {
        build_err = err;
        break :blk .empty;
    };
    if (build_err == null) {
        build_result_valid = true;
    }

    const relocked = runtime_mod.lockBackend(BackendType, backend);
    std.debug.assert(relocked);
    var reader_retained = true;
    errdefer if (reader_retained) if (@hasDecl(BackendType, "releaseReaderKind")) backend.releaseReaderKind(.compaction) else backend.releaseReader();
    errdefer if (build_result_valid) discardOutputRuns(BackendType, backend, &build_result);
    if (build_err) |err| {
        return err;
    }

    if (!planRunIdsStillMatch(backend.runs.items, plan, selected_run_ids)) {
        if (@hasDecl(BackendType, "releaseReaderKind")) backend.releaseReaderKind(.compaction) else backend.releaseReader();
        reader_retained = false;
        discardOutputRuns(BackendType, backend, &build_result);
        releaseCompactionSnapshots(BackendType, backend, &selected_runs);
        return;
    }

    try installCompactedRuns(
        BackendType,
        backend,
        plan,
        selected.len,
        input_bytes,
        start_ns,
        &build_result,
    );
    if (@hasDecl(BackendType, "releaseReaderKind")) backend.releaseReaderKind(.compaction) else backend.releaseReader();
    reader_retained = false;
    releaseCompactionSnapshots(BackendType, backend, &selected_runs);
}

fn appendPlanRunSnapshots(
    comptime BackendType: type,
    backend: *BackendType,
    plan: CompactionPlan,
    out: *std.ArrayListUnmanaged(Run),
) !void {
    try out.ensureUnusedCapacity(backend.allocator, plan.source_len + plan.target_len);
    for (backend.runs.items[plan.source_start .. plan.source_start + plan.source_len]) |run| {
        try appendCompactionSnapshot(BackendType, backend, out, run);
    }
    for (backend.runs.items[plan.target_start .. plan.target_start + plan.target_len]) |run| {
        try appendCompactionSnapshot(BackendType, backend, out, run);
    }
}

fn appendCompactionSnapshot(
    comptime BackendType: type,
    backend: *BackendType,
    out: *std.ArrayListUnmanaged(Run),
    source: Run,
) !void {
    var snapshot = try repository_mod.cloneRunCompactionSnapshot(backend.allocator, source);
    errdefer snapshot.deinit(backend.allocator);
    if (@hasDecl(BackendType, "retainRunSnapshotRef")) {
        try backend.retainRunSnapshotRef(&snapshot);
        errdefer backend.releaseRunSnapshotRef(&snapshot);
    }
    out.appendAssumeCapacity(snapshot);
}

fn releaseCompactionSnapshots(
    comptime BackendType: type,
    backend: *BackendType,
    runs: *std.ArrayListUnmanaged(Run),
) void {
    if (@hasDecl(BackendType, "releaseRunSnapshotRef")) {
        for (runs.items) |*run| backend.releaseRunSnapshotRef(run);
    }
    deinitRunList(backend.allocator, runs);
}

fn buildCompactedRunsFromSnapshots(
    comptime BackendType: type,
    backend: *BackendType,
    selected: []const *Run,
    output_level: u32,
    reserved_run_id_start: u64,
    reserved_run_id_end: u64,
) !std.ArrayListUnmanaged(Run) {
    const BuildBackend = struct {
        allocator: std.mem.Allocator,
        storage: @TypeOf(backend.storage),
        root_dir: @TypeOf(backend.root_dir),
        options: @TypeOf(backend.options),
        next_run_id: u64,
    };
    var build_backend = BuildBackend{
        .allocator = backend.allocator,
        .storage = backend.storage,
        .root_dir = backend.root_dir,
        .options = backend.options,
        .next_run_id = reserved_run_id_start,
    };
    const runs = if (backend.root_dir != null)
        try makePersistedRunsFromSelectedRuns(BuildBackend, &build_backend, selected, output_level)
    else
        try makeStateRunsFromSelectedRuns(BuildBackend, &build_backend, selected, output_level);
    errdefer {
        var owned = runs;
        discardOutputRuns(BuildBackend, &build_backend, &owned);
    }
    if (build_backend.next_run_id > reserved_run_id_end) return error.CompactionRunIdReservationExhausted;
    return runs;
}

pub fn buildRunsFromStateBorrowedWithReservedIds(
    comptime BackendType: type,
    backend: *BackendType,
    state: *const State,
    reserved_run_id_start: u64,
    reserved_run_id_end: u64,
) !std.ArrayListUnmanaged(Run) {
    const BuildBackend = struct {
        allocator: std.mem.Allocator,
        storage: @TypeOf(backend.storage),
        root_dir: @TypeOf(backend.root_dir),
        options: @TypeOf(backend.options),
        next_run_id: u64,
    };
    var build_backend = BuildBackend{
        .allocator = backend.allocator,
        .storage = backend.storage,
        .root_dir = backend.root_dir,
        .options = backend.options,
        .next_run_id = reserved_run_id_start,
    };
    const runs = try makeRunsFromStateBorrowed(BuildBackend, &build_backend, state);
    errdefer {
        var owned = runs;
        discardOutputRuns(BuildBackend, &build_backend, &owned);
    }
    if (build_backend.next_run_id > reserved_run_id_end) return error.FlushRunIdReservationExhausted;
    return runs;
}

fn planRunIdsStillMatch(runs: []const Run, plan: CompactionPlan, selected_run_ids: []const u64) bool {
    if (!planInBounds(runs, plan)) return false;
    if (selected_run_ids.len != plan.source_len + plan.target_len) return false;
    var idx: usize = 0;
    for (runs[plan.source_start .. plan.source_start + plan.source_len]) |run| {
        if (run.id != selected_run_ids[idx]) return false;
        idx += 1;
    }
    for (runs[plan.target_start .. plan.target_start + plan.target_len]) |run| {
        if (run.id != selected_run_ids[idx]) return false;
        idx += 1;
    }
    return true;
}

fn installCompactedRuns(
    comptime BackendType: type,
    backend: *BackendType,
    plan: CompactionPlan,
    selected_len: usize,
    input_bytes: u64,
    start_ns: u64,
    compacted_runs: *std.ArrayListUnmanaged(Run),
) !void {
    var retained = std.ArrayListUnmanaged(Run).empty;
    errdefer {
        for (retained.items) |*run| run.deinit(backend.allocator);
        retained.deinit(backend.allocator);
    }
    try retained.ensureTotalCapacity(backend.allocator, backend.runs.items.len - selected_len + compacted_runs.items.len);

    var obsolete_runs = std.ArrayListUnmanaged(Run).empty;
    errdefer {
        for (obsolete_runs.items) |*run| run.deinit(backend.allocator);
        obsolete_runs.deinit(backend.allocator);
    }

    var remove = try backend.allocator.alloc(bool, backend.runs.items.len);
    defer backend.allocator.free(remove);
    @memset(remove, false);
    for (plan.source_start..plan.source_start + plan.source_len) |i| remove[i] = true;
    for (plan.target_start..plan.target_start + plan.target_len) |i| remove[i] = true;

    for (backend.runs.items, 0..) |*run, i| {
        if (remove[i]) {
            if (run.path) |path| try queueObsoleteFilePath(BackendType, backend, try backend.allocator.dupe(u8, path));
            try obsolete_runs.append(backend.allocator, run.*);
            run.* = undefined;
            continue;
        }
        retained.appendAssumeCapacity(run.*);
    }
    var output_bytes: u64 = 0;
    for (compacted_runs.items) |run| {
        output_bytes +|= run.size_bytes;
        retained.appendAssumeCapacity(run);
    }
    if (@hasDecl(BackendType, "recordCompactionWriteStats")) {
        const elapsed_ns = if (@hasDecl(BackendType, "writeStatsNowNs")) elapsedNs(BackendType, backend, start_ns) else 0;
        backend.recordCompactionWriteStats(compacted_runs.items, elapsed_ns);
    }
    disarmRunList(compacted_runs);
    compacted_runs.deinit(backend.allocator);
    compacted_runs.* = .empty;
    sortRuns(retained.items);

    if (@hasField(BackendType, "compaction_stats")) {
        backend.compaction_stats.compactions += 1;
        backend.compaction_stats.input_runs += selected_len;
        backend.compaction_stats.input_bytes += input_bytes;
        backend.compaction_stats.output_bytes += output_bytes;
    }

    backend.runs.deinit(backend.allocator);
    backend.runs = retained;
    retained = .empty;
    try backend.queueObsoleteRuns(obsolete_runs);
}

pub fn discardOutputRuns(comptime BackendType: type, backend: *BackendType, runs: *std.ArrayListUnmanaged(Run)) void {
    if (@hasField(BackendType, "storage")) {
        if (backend.storage) |storage| {
            for (runs.items) |run| {
                if (run.path) |path| repository_mod.deleteFileAbsoluteWithStorage(storage, path) catch {};
            }
        }
    }
    deinitRunList(backend.allocator, runs);
}

fn elapsedNs(comptime BackendType: type, backend: *BackendType, start_ns: u64) u64 {
    const end_ns = backend.writeStatsNowNs();
    return if (end_ns >= start_ns) end_ns - start_ns else 0;
}

fn selectCompactionPlan(
    runs: []const Run,
    l0_limit: usize,
    l0_overlap_compact_threshold_runs: usize,
    level_target_runs_base: usize,
    level_target_runs_multiplier: usize,
    level_target_bytes_base: usize,
    level_target_bytes_multiplier: usize,
    max_input_bytes: u64,
    allow_oversized_single_job: bool,
) ?CompactionPlan {
    return selectCompactionPlanWithStats(
        runs,
        l0_limit,
        l0_overlap_compact_threshold_runs,
        level_target_runs_base,
        level_target_runs_multiplier,
        level_target_bytes_base,
        level_target_bytes_multiplier,
        max_input_bytes,
        allow_oversized_single_job,
        null,
    );
}

fn selectCompactionPlanWithStats(
    runs: []const Run,
    l0_limit: usize,
    l0_overlap_compact_threshold_runs: usize,
    level_target_runs_base: usize,
    level_target_runs_multiplier: usize,
    level_target_bytes_base: usize,
    level_target_bytes_multiplier: usize,
    max_input_bytes: u64,
    allow_oversized_single_job: bool,
    selection_stats: ?*CompactionSelectionStats,
) ?CompactionPlan {
    if (runs.len < 2) return null;
    var best: ?ScoredCompactionPlan = null;
    const l0_count = countLeadingL0Runs(runs);
    // Exact hotspot selection is quadratic. Once L0 exceeds its ordinary
    // pressure limit, a linear pressure compaction is already eligible and
    // exact overlap ranking cannot affect correctness. Skipping it here keeps
    // compaction planning from monopolizing the backend mutex during bursts.
    if (l0_limit > 0 and l0_count <= @min(l0_limit, max_exact_l0_overlap_runs)) {
        maybeAdoptBest(&best, selectL0OverlapCompactionCandidateWithStats(
            runs,
            l0_overlap_compact_threshold_runs,
            @max(l0_overlap_compact_threshold_runs, l0_limit),
            max_input_bytes,
            selection_stats,
        ));
    }
    maybeAdoptBest(&best, selectL0CompactionCandidateWithStats(runs, l0_limit, max_input_bytes, allow_oversized_single_job, selection_stats));
    maybeAdoptBest(&best, selectLowerLevelRepairCompactionCandidateWithStats(runs, max_input_bytes, allow_oversized_single_job, selection_stats));
    maybeAdoptBest(&best, selectLowerLevelPressureCompactionCandidateWithStats(
        runs,
        level_target_runs_base,
        level_target_runs_multiplier,
        level_target_bytes_base,
        level_target_bytes_multiplier,
        max_input_bytes,
        allow_oversized_single_job,
        selection_stats,
    ));
    return if (best) |candidate| candidate.plan else null;
}

pub fn largestL0OverlapRunCount(runs: []const Run, threshold: usize) usize {
    if (threshold == 0) return 0;
    const l0_count = countLeadingL0Runs(runs);
    if (l0_count < threshold) return 0;
    var best: usize = 0;
    for (runs[0..l0_count]) |anchor| {
        var count: usize = 0;
        for (runs[0..l0_count]) |candidate| {
            if (rangesOverlapRun(anchor, candidate)) count += 1;
        }
        best = @max(best, count);
    }
    return if (best >= threshold) best else 0;
}

fn selectL0OverlapCompaction(runs: []const Run, threshold: usize, max_input_bytes: u64) ?CompactionPlan {
    return selectL0OverlapCompactionWithStats(runs, threshold, max_input_bytes, null);
}

fn selectL0OverlapCompactionWithStats(
    runs: []const Run,
    threshold: usize,
    max_input_bytes: u64,
    selection_stats: ?*CompactionSelectionStats,
) ?CompactionPlan {
    return if (selectL0OverlapCompactionCandidateWithStats(runs, threshold, threshold, max_input_bytes, selection_stats)) |candidate| candidate.plan else null;
}

fn selectL0OverlapCompactionCandidateWithStats(
    runs: []const Run,
    threshold: usize,
    pressure_target: usize,
    max_input_bytes: u64,
    selection_stats: ?*CompactionSelectionStats,
) ?ScoredCompactionPlan {
    if (threshold == 0) return null;
    const l0_count = countLeadingL0Runs(runs);
    if (l0_count < threshold) return null;

    var best: ?ScoredCompactionPlan = null;
    for (runs[0..l0_count]) |anchor| {
        var start: ?usize = null;
        var end: usize = 0;
        var bytes: u64 = 0;
        var count: usize = 0;
        for (runs[0..l0_count], 0..) |candidate, i| {
            if (!rangesOverlapRun(anchor, candidate)) continue;
            if (start == null) start = i;
            end = i + 1;
            count += 1;
            bytes +|= candidate.size_bytes;
        }
        if (count < threshold) continue;
        const span_start = start.?;
        const span_len = end - span_start;
        const plan = buildPlanForSourceRange(runs, 0, span_start, span_len) orelse continue;
        if (!planWithinInputBudget(runs, plan, max_input_bytes)) {
            noteOversizedSelectionSkip(selection_stats, max_input_bytes);
            continue;
        }
        // Compare compaction pressure as a ratio, not as absolute run debt.
        // Absolute L0 debt used to starve an already 10x-overfull L1: every
        // small L0 job then rewrote the oversized L1 again before L1 was ever
        // promoted. Ratio scoring is the standard leveled-LSM behavior and
        // lets the downstream level win as soon as it is the greater pressure.
        // The overlap threshold controls eligibility, but the configured L0
        // soft bound controls how this work competes with lower levels. Using
        // the small overlap trigger as the pressure denominator made 102
        // overlapping L0 runs appear 25x urgent and starved an 8.5x-overfull
        // L1 even though L0 was only 3.2x above its production soft bound.
        const priority = normalizedPressurePriority(count, @max(@as(usize, 1), pressure_target)) +| bytes / (64 * 1024);
        maybeAdoptBest(&best, scoredPlan(runs, plan, priority));
    }
    return best;
}

fn countLeadingL0Runs(runs: []const Run) usize {
    var l0_count: usize = 0;
    while (l0_count < runs.len and runs[l0_count].level == 0) : (l0_count += 1) {}
    return l0_count;
}

fn selectL0Compaction(runs: []const Run, l0_limit: usize, max_input_bytes: u64, allow_oversized_single_job: bool) ?CompactionPlan {
    return selectL0CompactionWithStats(runs, l0_limit, max_input_bytes, allow_oversized_single_job, null);
}

fn selectL0CompactionWithStats(
    runs: []const Run,
    l0_limit: usize,
    max_input_bytes: u64,
    allow_oversized_single_job: bool,
    selection_stats: ?*CompactionSelectionStats,
) ?CompactionPlan {
    return if (selectL0CompactionCandidateWithStats(runs, l0_limit, max_input_bytes, allow_oversized_single_job, selection_stats)) |candidate| candidate.plan else null;
}

fn selectL0CompactionCandidateWithStats(
    runs: []const Run,
    l0_limit: usize,
    max_input_bytes: u64,
    allow_oversized_single_job: bool,
    selection_stats: ?*CompactionSelectionStats,
) ?ScoredCompactionPlan {
    const l0_count = countLeadingL0Runs(runs);
    if (l0_count == 0 or l0_count <= l0_limit) return null;
    const target_l0_count = @max(@as(usize, 1), l0_limit / 2);
    const excess_len = @max(@as(usize, 1), l0_count - target_l0_count);
    // Drain the pressure episode in one target-overlap closure when the byte
    // budget permits. The old `2 * l0_limit` cap turned a 128-run backlog into
    // roughly sixteen eight-run jobs. For broad key ranges every job rewrote
    // the same large L1 target, multiplying CPU, I/O, and final-sync latency.
    // `max_input_bytes` below remains the production bound: when configured,
    // the loop shrinks this window until source plus overlapping target fits.
    var source_len = if (l0_limit == 0) @min(excess_len, @as(usize, 2)) else excess_len;
    var oversized_plan: ?CompactionPlan = null;
    while (source_len > 0) : (source_len -= 1) {
        const plan = buildPlanForSourceRange(runs, 0, l0_count - source_len, source_len) orelse continue;
        const priority = normalizedPressurePriority(l0_count, @max(@as(usize, 1), l0_limit)) +| @as(u64, @intCast(source_len)) * 10;
        if (planWithinInputBudget(runs, plan, max_input_bytes)) return scoredPlan(runs, plan, priority);
        if (allow_oversized_single_job and max_input_bytes > 0) {
            oversized_plan = plan;
        } else {
            noteOversizedSelectionSkip(selection_stats, max_input_bytes);
        }
    }
    return if (oversized_plan) |plan| scoredPlan(runs, plan, normalizedPressurePriority(l0_count, @max(@as(usize, 1), l0_limit))) else null;
}

fn selectLowerLevelRepairCompaction(runs: []const Run, max_input_bytes: u64, allow_oversized_single_job: bool) ?CompactionPlan {
    return selectLowerLevelRepairCompactionWithStats(runs, max_input_bytes, allow_oversized_single_job, null);
}

fn selectLowerLevelRepairCompactionWithStats(
    runs: []const Run,
    max_input_bytes: u64,
    allow_oversized_single_job: bool,
    selection_stats: ?*CompactionSelectionStats,
) ?CompactionPlan {
    return if (selectLowerLevelRepairCompactionCandidateWithStats(runs, max_input_bytes, allow_oversized_single_job, selection_stats)) |candidate| candidate.plan else null;
}

fn selectLowerLevelRepairCompactionCandidateWithStats(
    runs: []const Run,
    max_input_bytes: u64,
    allow_oversized_single_job: bool,
    selection_stats: ?*CompactionSelectionStats,
) ?ScoredCompactionPlan {
    var best: ?ScoredCompactionPlan = null;
    var oversized_plan: ?ScoredCompactionPlan = null;
    var i: usize = 0;
    while (i + 1 < runs.len) : (i += 1) {
        const level = runs[i].level;
        if (level == 0) continue;
        if (runs[i + 1].level != level) continue;
        if (!rangesOverlapRun(runs[i], runs[i + 1])) continue;

        const start = i;
        var end = i + 1;
        var smallest_namespace_name = runs[start].smallest_namespace_name;
        var smallest_key = runs[start].smallest_key;
        var largest_namespace_name = runs[start].largest_namespace_name;
        var largest_key = runs[start].largest_key;

        while (end < runs.len and runs[end].level == level and rangesOverlap(
            runs[end].smallest_namespace_name,
            runs[end].smallest_key,
            runs[end].largest_namespace_name,
            runs[end].largest_key,
            smallest_namespace_name,
            smallest_key,
            largest_namespace_name,
            largest_key,
        )) : (end += 1) {
            if (compareRunBound(runs[end].smallest_namespace_name, runs[end].smallest_key, smallest_namespace_name, smallest_key) == .lt) {
                smallest_namespace_name = runs[end].smallest_namespace_name;
                smallest_key = runs[end].smallest_key;
            }
            if (compareRunBound(runs[end].largest_namespace_name, runs[end].largest_key, largest_namespace_name, largest_key) == .gt) {
                largest_namespace_name = runs[end].largest_namespace_name;
                largest_key = runs[end].largest_key;
            }
        }
        const plan = buildPlanForSourceRange(runs, level, start, end - start) orelse continue;
        const plan_score = planScoreForPlan(runs, plan);
        const priority = @as(u64, @intCast(plan.source_len)) * 750 +| plan_score.rewrite_bytes / (64 * 1024);
        if (planWithinInputBudget(runs, plan, max_input_bytes)) {
            maybeAdoptBest(&best, .{ .plan = plan, .priority = priority, .tie = plan_score });
        } else if (allow_oversized_single_job and max_input_bytes > 0) {
            maybeAdoptBest(&oversized_plan, .{ .plan = plan, .priority = priority, .tie = plan_score });
        } else {
            noteOversizedSelectionSkip(selection_stats, max_input_bytes);
        }
    }
    return best orelse oversized_plan;
}

fn selectLowerLevelPressureCompaction(
    runs: []const Run,
    level_target_runs_base: usize,
    level_target_runs_multiplier: usize,
    level_target_bytes_base: usize,
    level_target_bytes_multiplier: usize,
    max_input_bytes: u64,
    allow_oversized_single_job: bool,
) ?CompactionPlan {
    return selectLowerLevelPressureCompactionWithStats(
        runs,
        level_target_runs_base,
        level_target_runs_multiplier,
        level_target_bytes_base,
        level_target_bytes_multiplier,
        max_input_bytes,
        allow_oversized_single_job,
        null,
    );
}

fn selectLowerLevelPressureCompactionWithStats(
    runs: []const Run,
    level_target_runs_base: usize,
    level_target_runs_multiplier: usize,
    level_target_bytes_base: usize,
    level_target_bytes_multiplier: usize,
    max_input_bytes: u64,
    allow_oversized_single_job: bool,
    selection_stats: ?*CompactionSelectionStats,
) ?CompactionPlan {
    return if (selectLowerLevelPressureCompactionCandidateWithStats(
        runs,
        level_target_runs_base,
        level_target_runs_multiplier,
        level_target_bytes_base,
        level_target_bytes_multiplier,
        max_input_bytes,
        allow_oversized_single_job,
        selection_stats,
    )) |candidate| candidate.plan else null;
}

fn selectLowerLevelPressureCompactionCandidateWithStats(
    runs: []const Run,
    level_target_runs_base: usize,
    level_target_runs_multiplier: usize,
    level_target_bytes_base: usize,
    level_target_bytes_multiplier: usize,
    max_input_bytes: u64,
    allow_oversized_single_job: bool,
    selection_stats: ?*CompactionSelectionStats,
) ?ScoredCompactionPlan {
    var best: ?ScoredCompactionPlan = null;
    var i: usize = 0;
    while (i < runs.len) {
        const level = runs[i].level;
        if (level == 0) {
            i += 1;
            continue;
        }

        const level_start = i;
        while (i < runs.len and runs[i].level == level) : (i += 1) {}
        const level_len = i - level_start;
        const level_bytes = sumRunBytes(runs[level_start..i]);
        const target_runs = levelRunTarget(level, level_target_runs_base, level_target_runs_multiplier);
        const target_bytes = levelByteTargetForRuns(runs, level, level_target_bytes_base, level_target_bytes_multiplier);
        const need_runs = level_len > target_runs;
        const need_bytes = target_bytes > 0 and level_bytes > target_bytes;
        if (!need_runs and !need_bytes) continue;

        const run_pressure = if (need_runs) normalizedPressurePriority(level_len, target_runs) else 0;
        const byte_pressure = if (need_bytes) normalizedPressurePriority(level_bytes, target_bytes) else 0;
        const priority = @max(run_pressure, byte_pressure);
        const source_len = if (need_runs) @max(@as(usize, 1), level_len - target_runs) else 1;
        const source_bytes = if (need_bytes) @max(@as(u64, 1), level_bytes - target_bytes) else 0;
        maybeAdoptBest(&best, selectLowestOverlapWindowCandidate(
            runs,
            level,
            level_start,
            level_len,
            source_len,
            source_bytes,
            max_input_bytes,
            allow_oversized_single_job,
            selection_stats,
            priority,
        ));
    }
    return best;
}

fn selectLowestOverlapWindow(
    runs: []const Run,
    level: u32,
    level_start: usize,
    level_len: usize,
    source_len: usize,
    source_bytes: u64,
    max_input_bytes: u64,
    allow_oversized_single_job: bool,
    selection_stats: ?*CompactionSelectionStats,
) ?CompactionPlan {
    return if (selectLowestOverlapWindowCandidate(
        runs,
        level,
        level_start,
        level_len,
        source_len,
        source_bytes,
        max_input_bytes,
        allow_oversized_single_job,
        selection_stats,
        1,
    )) |candidate| candidate.plan else null;
}

fn selectLowestOverlapWindowCandidate(
    runs: []const Run,
    level: u32,
    level_start: usize,
    level_len: usize,
    source_len: usize,
    source_bytes: u64,
    max_input_bytes: u64,
    allow_oversized_single_job: bool,
    selection_stats: ?*CompactionSelectionStats,
    priority: u64,
) ?ScoredCompactionPlan {
    std.debug.assert(level_len >= source_len);
    var best_plan: ?CompactionPlan = null;
    var best_score: ?PlanScore = null;
    var oversized_plan: ?CompactionPlan = null;
    var oversized_score: ?PlanScore = null;

    var offset: usize = 0;
    while (offset < level_len) : (offset += 1) {
        const source_start = level_start + offset;
        var selected_len: usize = 0;
        var selected_bytes: u64 = 0;
        while (offset + selected_len < level_len) : (selected_len += 1) {
            selected_bytes += runs[source_start + selected_len].size_bytes;
            if (selected_len + 1 < source_len) continue;
            if (selected_bytes < source_bytes) continue;
            const final_len = selected_len + 1;
            const plan = buildPlanForSourceRange(runs, level, source_start, final_len) orelse continue;
            const target_bytes = sumRunBytes(runs[plan.target_start .. plan.target_start + plan.target_len]);
            const score: PlanScore = .{
                .rewrite_bytes = selected_bytes +| target_bytes,
                .target_bytes = target_bytes,
                .source_bytes = selected_bytes,
                .source_len = final_len,
                .target_len = plan.target_len,
                .source_start = source_start,
            };
            if (!planWithinInputBudget(runs, plan, max_input_bytes)) {
                if (allow_oversized_single_job and max_input_bytes > 0 and (oversized_score == null or score.betterThan(oversized_score.?))) {
                    oversized_plan = plan;
                    oversized_score = score;
                } else {
                    noteOversizedSelectionSkip(selection_stats, max_input_bytes);
                }
                break;
            }
            if (best_score == null or score.betterThan(best_score.?)) {
                best_plan = plan;
                best_score = score;
            }
            break;
        }
    }
    if (best_plan) |plan| return .{ .plan = plan, .priority = priority, .tie = best_score.? };
    if (oversized_plan) |plan| return .{ .plan = plan, .priority = priority, .tie = oversized_score.? };
    return null;
}

fn levelRunTarget(level: u32, base: usize, multiplier: usize) usize {
    if (level == 0) return 0;
    var target = @max(@as(usize, 1), base);
    var remaining = level - 1;
    const factor = @max(@as(usize, 1), multiplier);
    while (remaining > 0) : (remaining -= 1) {
        target = std.math.mul(usize, target, factor) catch std.math.maxInt(usize);
    }
    return target;
}

const pressure_priority_scale: u64 = 1_000_000;

fn normalizedPressurePriority(current: anytype, target: @TypeOf(current)) u64 {
    if (target == 0) return std.math.maxInt(u64);
    const current_u64: u64 = @intCast(current);
    const target_u64: u64 = @intCast(target);
    const whole = current_u64 / target_u64;
    const remainder = current_u64 % target_u64;
    return std.math.mul(u64, whole, pressure_priority_scale) catch std.math.maxInt(u64) +|
        (std.math.mul(u64, remainder, pressure_priority_scale) catch std.math.maxInt(u64)) / target_u64;
}

fn staticLevelByteTarget(level: u32, base: usize, multiplier: usize) u64 {
    if (level == 0 or base == 0) return 0;
    var target = @max(@as(u64, 1), @as(u64, @intCast(base)));
    var remaining = level - 1;
    const factor = @max(@as(u64, 1), @as(u64, @intCast(multiplier)));
    while (remaining > 0) : (remaining -= 1) {
        target = std.math.mul(u64, target, factor) catch std.math.maxInt(u64);
    }
    return target;
}

/// Dynamically place the configured byte geometry so the current last level
/// can hold the live run set. A fixed 128 MiB/1.28 GiB/... ladder promotes a
/// multi-gigabyte database through an unnecessary extra level while it is
/// still growing, rewriting the same data at each threshold. The dynamic base
/// may move by at most one configured multiplier tier; larger datasets still
/// add another level instead of allowing one level to grow without bound.
pub fn levelByteTargetForRuns(runs: []const Run, level: u32, base: usize, multiplier: usize) u64 {
    if (level == 0 or base == 0) return 0;
    const factor: u64 = @intCast(@max(@as(usize, 1), multiplier));
    if (factor == 1) return staticLevelByteTarget(level, base, multiplier);

    var total_bytes: u64 = 0;
    var max_level: u32 = 0;
    for (runs) |run| {
        total_bytes +|= run.size_bytes;
        max_level = @max(max_level, run.level);
    }

    // Keep at least a two-level geometry: L1 remains a bounded staging level
    // and L2 is the first level sized to retain the live database.
    const last_level = @max(@as(u32, 2), max_level);
    var last_level_factor: u64 = 1;
    var remaining = last_level - 1;
    while (remaining > 0) : (remaining -= 1) {
        last_level_factor = std.math.mul(u64, last_level_factor, factor) catch std.math.maxInt(u64);
    }

    const configured_base: u64 = @intCast(base);
    const max_dynamic_base = std.math.mul(u64, configured_base, factor) catch std.math.maxInt(u64);
    const required_base = if (total_bytes == 0)
        configured_base
    else
        1 + (total_bytes - 1) / last_level_factor;
    const dynamic_base = @min(max_dynamic_base, @max(configured_base, required_base));

    var target = dynamic_base;
    remaining = level - 1;
    while (remaining > 0) : (remaining -= 1) {
        target = std.math.mul(u64, target, factor) catch return std.math.maxInt(u64);
    }
    return target;
}

fn sumRunBytes(runs: []const Run) u64 {
    var total: u64 = 0;
    for (runs) |run| total +|= run.size_bytes;
    return total;
}

fn sumRunPtrBytes(runs: []const *Run) u64 {
    var total: u64 = 0;
    for (runs) |run| total +|= run.size_bytes;
    return total;
}

fn countRunPtrEntries(runs: []const *Run) usize {
    var total: usize = 0;
    for (runs) |run| total +|= @intCast(run.entry_count);
    return total;
}

fn countRunEntries(runs: []const Run) usize {
    var total: usize = 0;
    for (runs) |run| total +|= @intCast(run.entry_count);
    return total;
}

const CompactionBounds = struct {
    smallest_namespace_name: ?[]const u8,
    smallest_key: []const u8,
    largest_namespace_name: ?[]const u8,
    largest_key: []const u8,

    fn include(self: *CompactionBounds, run: Run) void {
        if (compareRunBound(
            run.smallest_namespace_name,
            run.smallest_key,
            self.smallest_namespace_name,
            self.smallest_key,
        ) == .lt) {
            self.smallest_namespace_name = run.smallest_namespace_name;
            self.smallest_key = run.smallest_key;
        }
        if (compareRunBound(
            run.largest_namespace_name,
            run.largest_key,
            self.largest_namespace_name,
            self.largest_key,
        ) == .gt) {
            self.largest_namespace_name = run.largest_namespace_name;
            self.largest_key = run.largest_key;
        }
    }

    fn overlaps(self: CompactionBounds, run: Run) bool {
        return rangesOverlap(
            run.smallest_namespace_name,
            run.smallest_key,
            run.largest_namespace_name,
            run.largest_key,
            self.smallest_namespace_name,
            self.smallest_key,
            self.largest_namespace_name,
            self.largest_key,
        );
    }
};

const TargetLevelClosure = struct {
    level_start: usize,
    level_end: usize,
    probe_start: usize,
    selected_start: ?usize = null,
    selected_end: usize = 0,

    fn init(
        runs: []const Run,
        output_level: u32,
        bounds: CompactionBounds,
    ) ?TargetLevelClosure {
        var level_start = runs.len;
        var level_end = runs.len;
        for (runs, 0..) |run, i| {
            if (run.level < output_level) continue;
            level_start = i;
            level_end = i;
            while (level_end < runs.len and runs[level_end].level == output_level) : (level_end += 1) {}
            break;
        }

        var i = level_start;
        while (i + 1 < level_end) : (i += 1) {
            const current = runs[i];
            const next = runs[i + 1];
            if (compareRunBound(
                current.smallest_namespace_name,
                current.smallest_key,
                next.smallest_namespace_name,
                next.smallest_key,
            ) == .gt) return null;
            if (rangesOverlap(
                current.smallest_namespace_name,
                current.smallest_key,
                current.largest_namespace_name,
                current.largest_key,
                next.smallest_namespace_name,
                next.smallest_key,
                next.largest_namespace_name,
                next.largest_key,
            )) return null;
        }

        var probe_start = level_start;
        while (probe_start < level_end and compareRunBound(
            runs[probe_start].largest_namespace_name,
            runs[probe_start].largest_key,
            bounds.smallest_namespace_name,
            bounds.smallest_key,
        ) == .lt) : (probe_start += 1) {}

        return .{
            .level_start = level_start,
            .level_end = level_end,
            .probe_start = probe_start,
        };
    }

    fn expand(
        self: *TargetLevelClosure,
        runs: []const Run,
        bounds: *CompactionBounds,
    ) void {
        if (self.level_start == self.level_end) return;

        if (self.selected_start == null) {
            while (self.probe_start > self.level_start) {
                const previous = runs[self.probe_start - 1];
                if (compareRunBound(
                    previous.largest_namespace_name,
                    previous.largest_key,
                    bounds.smallest_namespace_name,
                    bounds.smallest_key,
                ) == .lt) break;
                self.probe_start -= 1;
            }
            if (self.probe_start < self.level_end and bounds.overlaps(runs[self.probe_start])) {
                self.selected_start = self.probe_start;
                self.selected_end = self.probe_start + 1;
                bounds.include(runs[self.probe_start]);
            }
        }

        while (self.selected_start != null) {
            const old_start = self.selected_start.?;
            const old_end = self.selected_end;
            while (self.selected_start.? > self.level_start and
                bounds.overlaps(runs[self.selected_start.? - 1]))
            {
                self.selected_start.? -= 1;
                bounds.include(runs[self.selected_start.?]);
            }
            while (self.selected_end < self.level_end and
                bounds.overlaps(runs[self.selected_end]))
            {
                bounds.include(runs[self.selected_end]);
                self.selected_end += 1;
            }
            if (self.selected_start.? == old_start and self.selected_end == old_end) break;
        }
    }
};

fn buildPlanForSourceRange(runs: []const Run, source_level: u32, source_start: usize, initial_source_len: usize) ?CompactionPlan {
    if (initial_source_len == 0 or source_start >= runs.len or initial_source_len > runs.len - source_start) return null;
    if (runs[source_start].level != source_level) return null;

    var source_end = source_start + initial_source_len;
    for (runs[source_start..source_end]) |run| {
        if (run.level != source_level) return null;
    }

    var bounds = CompactionBounds{
        .smallest_namespace_name = runs[source_start].smallest_namespace_name,
        .smallest_key = runs[source_start].smallest_key,
        .largest_namespace_name = runs[source_start].largest_namespace_name,
        .largest_key = runs[source_start].largest_key,
    };
    for (runs[source_start + 1 .. source_end]) |run| bounds.include(run);

    const output_level = source_level + 1;
    var target_closure = TargetLevelClosure.init(runs, output_level, bounds) orelse return null;
    target_closure.expand(runs, &bounds);

    // L0 has newest-first precedence. If an older run overlaps the output
    // closure, the contiguous prefix through that run is indivisible. Walking
    // older L0 inputs once and expanding the sorted target window
    // incrementally computes the fixed point in O(L0 + target-runs).
    if (source_level == 0) {
        const l0_count = countLeadingL0Runs(runs);
        var i = source_end;
        while (i < l0_count) : (i += 1) {
            if (!bounds.overlaps(runs[i])) continue;
            for (runs[source_end .. i + 1]) |run| bounds.include(run);
            source_end = i + 1;
            target_closure.expand(runs, &bounds);
        }
    }

    return .{
        .source_level = source_level,
        .source_start = source_start,
        .source_len = source_end - source_start,
        .target_start = target_closure.selected_start orelse source_end,
        .target_len = if (target_closure.selected_start) |start|
            target_closure.selected_end - start
        else
            0,
        .output_level = output_level,
    };
}

fn rangesOverlap(
    lhs_smallest_namespace_name: ?[]const u8,
    lhs_smallest_key: []const u8,
    lhs_largest_namespace_name: ?[]const u8,
    lhs_largest_key: []const u8,
    rhs_smallest_namespace_name: ?[]const u8,
    rhs_smallest_key: []const u8,
    rhs_largest_namespace_name: ?[]const u8,
    rhs_largest_key: []const u8,
) bool {
    return compareRunBound(lhs_smallest_namespace_name, lhs_smallest_key, rhs_largest_namespace_name, rhs_largest_key) != .gt and
        compareRunBound(lhs_largest_namespace_name, lhs_largest_key, rhs_smallest_namespace_name, rhs_smallest_key) != .lt;
}

fn testRun(id: u64, level: u32, smallest_key: []const u8, largest_key: []const u8, size_bytes: u64) Run {
    return .{
        .id = id,
        .level = level,
        .size_bytes = size_bytes,
        .path = null,
        .smallest_namespace_name = @constCast("docs"),
        .smallest_key = @constCast(smallest_key),
        .largest_namespace_name = @constCast("docs"),
        .largest_key = @constCast(largest_key),
        .entry_count = 1,
        .bloom_filter = null,
        .owns_metadata = false,
        .owns_bloom_filter = false,
        .state = null,
    };
}

test "lsm compaction lower-level repair can exceed input target for minimum job" {
    const runs = [_]Run{
        testRun(1, 1, "doc:a", "doc:m", 100),
        testRun(2, 1, "doc:h", "doc:z", 100),
    };

    try std.testing.expect(selectLowerLevelRepairCompaction(&runs, 1, false) == null);
    const plan = selectLowerLevelRepairCompaction(&runs, 1, true) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 1), plan.source_level);
    try std.testing.expectEqual(@as(usize, 0), plan.source_start);
    try std.testing.expectEqual(@as(usize, 2), plan.source_len);
    try std.testing.expectEqual(@as(u32, 2), plan.output_level);
}

test "lsm compaction lower-level pressure can exceed input target for minimum job" {
    const runs = [_]Run{
        testRun(1, 1, "doc:a", "doc:b", 100),
        testRun(2, 1, "doc:c", "doc:d", 100),
    };

    try std.testing.expect(selectLowerLevelPressureCompaction(&runs, 1, 1, 0, 8, 1, false) == null);
    const plan = selectLowerLevelPressureCompaction(&runs, 1, 1, 0, 8, 1, true) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 1), plan.source_level);
    try std.testing.expectEqual(@as(usize, 1), plan.source_len);
    try std.testing.expectEqual(@as(u32, 2), plan.output_level);
}

test "lsm compaction dynamically sizes the last level without unbounded growth" {
    const mib: u64 = 1024 * 1024;
    const runs = [_]Run{
        testRun(1, 1, "doc:a", "doc:f", 800 * mib),
        testRun(2, 2, "doc:g", "doc:l", 512 * mib),
        testRun(3, 2, "doc:m", "doc:r", 512 * mib),
        testRun(4, 2, "doc:s", "doc:x", 512 * mib),
        testRun(5, 2, "doc:y", "doc:z", 512 * mib),
    };
    const total_bytes = sumRunBytes(&runs);
    const l1_target = levelByteTargetForRuns(&runs, 1, 128 * mib, 10);
    const l2_target = levelByteTargetForRuns(&runs, 2, 128 * mib, 10);

    try std.testing.expect(l1_target > 128 * mib);
    try std.testing.expect(l1_target <= 1280 * mib);
    try std.testing.expect(l2_target >= total_bytes);
    try std.testing.expectEqual(l1_target * 10, l2_target);

    // L2 can retain the live set, while only the excess L1 staging bytes are
    // eligible for promotion. Static geometry would classify all of L2 as
    // overfull at 1.28 GiB and start an unnecessary L2-to-L3 rewrite.
    const plan = selectLowerLevelPressureCompaction(
        &runs,
        32,
        4,
        128 * mib,
        10,
        0,
        false,
    ) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 1), plan.source_level);
    try std.testing.expectEqual(@as(u32, 2), plan.output_level);

    const oversized = [_]Run{
        testRun(10, 2, "doc:a", "doc:z", 20 * 1024 * mib),
    };
    try std.testing.expectEqual(@as(u64, 1280 * mib), levelByteTargetForRuns(&oversized, 1, 128 * mib, 10));
    try std.testing.expectEqual(@as(u64, 12800 * mib), levelByteTargetForRuns(&oversized, 2, 128 * mib, 10));
}

test "lsm compaction promotes overfull lower level before repeated L0 rewrites" {
    var runs = std.ArrayListUnmanaged(Run).empty;
    defer runs.deinit(std.testing.allocator);

    // L0 remains well above its four-run trigger, but L1 is more than ten
    // times its byte target. Continuing to compact L0 would rewrite these L1
    // bytes repeatedly; normalized pressure must promote L1 first.
    for (0..39) |i| {
        try runs.append(std.testing.allocator, testRun(@intCast(i + 1), 0, "a", "z", 3 * 1024 * 1024));
    }
    try runs.append(std.testing.allocator, testRun(100, 1, "a", "f", 340 * 1024 * 1024));
    try runs.append(std.testing.allocator, testRun(101, 1, "g", "l", 340 * 1024 * 1024));
    try runs.append(std.testing.allocator, testRun(102, 1, "m", "r", 340 * 1024 * 1024));
    try runs.append(std.testing.allocator, testRun(103, 1, "s", "z", 340 * 1024 * 1024));

    const plan = selectCompactionPlan(
        runs.items,
        4,
        4,
        32,
        4,
        128 * 1024 * 1024,
        10,
        0,
        false,
    ) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 1), plan.source_level);
    try std.testing.expectEqual(@as(u32, 2), plan.output_level);
}

test "lsm maintenance compares soft L0 pressure with lower-level pressure" {
    var runs = std.ArrayListUnmanaged(Run).empty;
    defer runs.deinit(std.testing.allocator);

    // Reproduces the post-ingest state from the 1M server gate. L0 is above
    // its 32-run soft limit (3.2x), while L1 is about 8.5x its byte target.
    // Using the four-run compaction trigger as the pressure denominator would
    // incorrectly make L0 look 25x overfull and rewrite the entire L1.
    for (0..102) |i| {
        try runs.append(std.testing.allocator, testRun(@intCast(i + 1), 0, "a", "z", 3 * 1024 * 1024));
    }
    for (0..4) |i| {
        try runs.append(std.testing.allocator, testRun(@intCast(200 + i), 1, "a", "z", 284 * 1024 * 1024));
    }

    const plan = selectCompactionPlan(
        runs.items,
        32,
        4,
        32,
        4,
        128 * 1024 * 1024,
        10,
        0,
        false,
    ) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 1), plan.source_level);
    try std.testing.expectEqual(@as(u32, 2), plan.output_level);
}

test "lsm compaction L0 pressure selects a wider assist window" {
    const runs = [_]Run{
        testRun(9, 0, "doc:009", "doc:009", 10),
        testRun(8, 0, "doc:008", "doc:008", 10),
        testRun(7, 0, "doc:007", "doc:007", 10),
        testRun(6, 0, "doc:006", "doc:006", 10),
        testRun(5, 0, "doc:005", "doc:005", 10),
        testRun(4, 0, "doc:004", "doc:004", 10),
        testRun(3, 0, "doc:003", "doc:003", 10),
        testRun(2, 0, "doc:002", "doc:002", 10),
        testRun(1, 0, "doc:001", "doc:001", 10),
    };

    const plan = selectL0Compaction(&runs, 4, 0, false) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 0), plan.source_level);
    try std.testing.expectEqual(@as(usize, 2), plan.source_start);
    try std.testing.expectEqual(@as(usize, 7), plan.source_len);
    try std.testing.expectEqual(@as(u32, 1), plan.output_level);

    const oldest_pair = selectL0Compaction(&runs, 0, 0, false) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 7), oldest_pair.source_start);
    try std.testing.expectEqual(@as(usize, 2), oldest_pair.source_len);
}

test "lsm compaction drains a hard L0 backlog in one byte-bounded window" {
    var runs = std.ArrayListUnmanaged(Run).empty;
    defer runs.deinit(std.testing.allocator);
    for (0..128) |i| {
        try runs.append(std.testing.allocator, testRun(@intCast(128 - i), 0, "doc:000000", "doc:999999", 3 * 1024 * 1024));
    }

    const unbounded = selectL0Compaction(runs.items, 4, 0, false) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 126), unbounded.source_len);

    const bounded = selectL0Compaction(runs.items, 4, 20 * 1024 * 1024, false) orelse return error.TestUnexpectedResult;
    try std.testing.expect(bounded.source_len <= 6);
    try std.testing.expect(bounded.source_len > 0);
}

test "lsm L0 compaction closes over older inputs and expanded target ranges" {
    const runs = [_]Run{
        // A newer, disjoint run may remain in L0.
        testRun(6, 0, "doc:z", "doc:zz", 10),
        // The initial hotspot is these two runs.
        testRun(5, 0, "doc:a", "doc:b", 10),
        testRun(4, 0, "doc:b", "doc:c", 10),
        // This older run does not overlap the hotspot directly. The first L1
        // target expands the output range to it, so it must join the source.
        testRun(3, 0, "doc:e", "doc:g", 10),
        testRun(1, 1, "doc:a", "doc:f", 10),
        // The older L0 inclusion then expands into this target too.
        testRun(2, 1, "doc:g", "doc:k", 10),
    };

    const plan = buildPlanForSourceRange(&runs, 0, 1, 2) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), plan.source_start);
    try std.testing.expectEqual(@as(usize, 3), plan.source_len);
    try std.testing.expectEqual(@as(usize, 4), plan.target_start);
    try std.testing.expectEqual(@as(usize, 2), plan.target_len);

    // Correctness closures are indivisible: a byte budget can defer the job,
    // but it cannot compact the hotspot while leaving the stale older input.
    try std.testing.expect(selectL0OverlapCompaction(&runs, 2, 49) == null);
    const unbounded = selectL0OverlapCompaction(&runs, 2, 0) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(plan.source_start, unbounded.source_start);
    try std.testing.expectEqual(plan.source_len, unbounded.source_len);
    try std.testing.expectEqual(plan.target_start, unbounded.target_start);
    try std.testing.expectEqual(plan.target_len, unbounded.target_len);
}

test "lsm compaction plan selection chooses highest scored debt" {
    const runs = [_]Run{
        testRun(12, 0, "doc:012", "doc:012", 10),
        testRun(11, 0, "doc:011", "doc:011", 10),
        testRun(10, 0, "doc:010", "doc:010", 10),
        testRun(1, 1, "doc:a", "doc:b", 1024 * 1024),
        testRun(2, 1, "doc:c", "doc:d", 1024 * 1024),
        testRun(3, 1, "doc:e", "doc:f", 1024 * 1024),
        testRun(4, 1, "doc:g", "doc:h", 1024 * 1024),
        testRun(5, 1, "doc:i", "doc:j", 1024 * 1024),
    };

    const plan = selectCompactionPlan(
        &runs,
        2,
        0,
        1,
        1,
        0,
        8,
        0,
        false,
    ) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 1), plan.source_level);
    try std.testing.expectEqual(@as(u32, 2), plan.output_level);
}

fn rangesOverlapRun(lhs: Run, rhs: Run) bool {
    return rangesOverlap(
        lhs.smallest_namespace_name,
        lhs.smallest_key,
        lhs.largest_namespace_name,
        lhs.largest_key,
        rhs.smallest_namespace_name,
        rhs.smallest_key,
        rhs.largest_namespace_name,
        rhs.largest_key,
    );
}

fn compareRunBound(lhs_namespace_name: ?[]const u8, lhs_key: []const u8, rhs_namespace_name: ?[]const u8, rhs_key: []const u8) std.math.Order {
    const namespace_order = state_mod.compareNamespace(.{ .name = lhs_namespace_name }, .{ .name = rhs_namespace_name });
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, lhs_key, rhs_key);
}

fn makeStateRunsFromSelectedRuns(comptime BackendType: type, backend: *BackendType, runs: []const *Run, level: u32) !std.ArrayListUnmanaged(Run) {
    const end_index = runs.len - 1;
    var merged = try (try ensureRunStateForBackend(BackendType, backend, runs[end_index])).clone(backend.allocator);
    errdefer merged.deinit(backend.allocator);

    var run_index = end_index;
    while (run_index > 0) {
        run_index -= 1;
        const newer_state = try ensureRunStateForBackend(BackendType, backend, runs[run_index]);
        const next = try state_mod.mergeStates(backend.allocator, &merged, newer_state);
        merged.deinit(backend.allocator);
        merged = next;
    }

    if (merged.entries.items.len == 0) return error.EmptyRun;
    return try makeRunsFromStateAtLevel(BackendType, backend, &merged, level);
}

fn makePersistedRunsFromSelectedRuns(comptime BackendType: type, backend: *BackendType, window_runs: []const *Run, output_level: u32) !std.ArrayListUnmanaged(Run) {
    const allocator = backend.allocator;
    const expected_entries = countRunPtrEntries(window_runs);

    var cursors = try allocator.alloc(PersistedRunCursor, window_runs.len);
    var initialized_cursors: usize = 0;
    defer {
        for (cursors[0..initialized_cursors]) |*cursor| cursor.deinit();
        allocator.free(cursors);
    }
    for (window_runs, 0..) |run, i| {
        const path = run.path orelse return error.RunStateUnavailable;
        cursors[i] = try PersistedRunCursor.init(allocator, backend.storage.?, path);
        if (cursors[i].index.entry_count != run.entry_count) return error.InvalidTableFile;
        initialized_cursors += 1;
    }

    var runs = std.ArrayListUnmanaged(Run).empty;
    errdefer discardOutputRuns(BackendType, backend, &runs);
    var output: PersistedOutputRunBuilder(BackendType) = undefined;
    var output_active = false;
    defer if (output_active) output.deinit();
    const target_bytes = targetRunFileBytes(BackendType, backend);
    var consumed_entries: usize = 0;
    var emitted_entries: usize = 0;

    var heap = try PersistedRunMergeHeap.init(allocator, cursors[0..initialized_cursors]);
    defer heap.deinit();

    while (heap.peekSource()) |winner_source| {
        const winner = (try cursors[winner_source].currentEntry()) orelse return error.InvalidTableFile;
        const entry_bytes = tableEntryLogicalBytes(winner);
        if (output_active) {
            const partition_changed = output.entry_count > 0 and !sameRunPartition(
                output.smallest_namespace_name,
                output.smallest_key,
                winner.namespace_name,
                winner.key,
                backend.options.run_partition_prefix_bytes,
            );
            if (partition_changed or
                (output.entry_count > 0 and target_bytes > 0 and output.logical_bytes + entry_bytes > target_bytes))
            {
                try runs.ensureUnusedCapacity(allocator, 1);
                const run = try output.finish();
                output.deinit();
                output_active = false;
                runs.appendAssumeCapacity(run);
            }
        }

        if (!output_active) {
            try output.initInPlace(
                backend,
                output_level,
                @max(@as(usize, 1), expected_entries),
            );
            output_active = true;
        }
        try output.appendEntry(winner, entry_bytes);
        emitted_entries += 1;
        consumed_entries += try heap.advanceTopSourcesAtKey(winner);

        if (output_active) {
            if (output.entry_count > 0 and target_bytes > 0 and output.logical_bytes >= target_bytes) {
                try runs.ensureUnusedCapacity(allocator, 1);
                const run = try output.finish();
                output.deinit();
                output_active = false;
                runs.appendAssumeCapacity(run);
            }
        }
    }

    if (output_active) {
        try runs.ensureUnusedCapacity(allocator, 1);
        const run = try output.finish();
        output.deinit();
        output_active = false;
        runs.appendAssumeCapacity(run);
    }
    if (runs.items.len == 0) return error.EmptyRun;
    if (consumed_entries != expected_entries) return error.InvalidTableFile;
    if (countRunEntries(runs.items) != emitted_entries) return error.InvalidTableFile;
    return runs;
}

fn PersistedOutputRunBuilder(comptime BackendType: type) type {
    return struct {
        backend: *BackendType,
        writer: repository_mod.StreamingRunFileWriter = undefined,
        writer_active: bool = false,
        run_id: u64,
        output_level: u32,
        smallest_namespace_name: ?[]u8 = null,
        smallest_key: []u8 = &.{},
        largest_namespace_name: ?[]u8 = null,
        largest_key: []u8 = &.{},
        entry_count: usize = 0,
        logical_bytes: usize = 0,

        const Self = @This();

        fn initInPlace(self: *Self, backend: *BackendType, output_level: u32, expected_entries: usize) !void {
            const run_id = backend.next_run_id;
            backend.next_run_id += 1;
            self.* = .{
                .backend = backend,
                .run_id = run_id,
                .output_level = output_level,
            };
            errdefer self.deinit();
            try self.writer.initInPlace(
                backend.storage.?,
                backend.allocator,
                backend.root_dir.?,
                run_id,
                expected_entries,
                backend.options.bloom,
                backend.options.table_block_compression,
                backend.options.table_prefix_extractor,
                backend.options.resource_manager,
            );
            self.writer_active = true;
        }

        fn deinit(self: *Self) void {
            if (self.writer_active) {
                self.writer.deinit();
                self.writer_active = false;
            }
            if (self.smallest_namespace_name) |name| self.backend.allocator.free(name);
            if (self.smallest_key.len > 0) self.backend.allocator.free(self.smallest_key);
            if (self.largest_namespace_name) |name| self.backend.allocator.free(name);
            if (self.largest_key.len > 0) self.backend.allocator.free(self.largest_key);
            self.* = undefined;
        }

        fn appendEntry(self: *Self, entry: lsm_table_file.Entry, entry_bytes: usize) !void {
            var new_smallest_namespace_name: ?[]u8 = null;
            var new_smallest_key: []u8 = &.{};
            var new_largest_namespace_name: ?[]u8 = null;
            var new_largest_key: []u8 = &.{};
            errdefer {
                if (new_smallest_namespace_name) |name| self.backend.allocator.free(name);
                if (new_smallest_key.len > 0) self.backend.allocator.free(new_smallest_key);
                if (new_largest_namespace_name) |name| self.backend.allocator.free(name);
                if (new_largest_key.len > 0) self.backend.allocator.free(new_largest_key);
            }

            if (self.entry_count == 0) {
                new_smallest_namespace_name = if (entry.namespace_name) |name| try self.backend.allocator.dupe(u8, name) else null;
                new_smallest_key = try self.backend.allocator.dupe(u8, entry.key);
            }
            new_largest_namespace_name = if (entry.namespace_name) |name| try self.backend.allocator.dupe(u8, name) else null;
            new_largest_key = try self.backend.allocator.dupe(u8, entry.key);

            try self.writer.appendEntry(entry);

            if (self.entry_count == 0) {
                self.smallest_namespace_name = new_smallest_namespace_name;
                new_smallest_namespace_name = null;
                self.smallest_key = new_smallest_key;
                new_smallest_key = &.{};
            }
            if (self.largest_namespace_name) |name| self.backend.allocator.free(name);
            if (self.largest_key.len > 0) self.backend.allocator.free(self.largest_key);
            self.largest_namespace_name = new_largest_namespace_name;
            new_largest_namespace_name = null;
            self.largest_key = new_largest_key;
            new_largest_key = &.{};
            self.entry_count += 1;
            self.logical_bytes += entry_bytes;
        }

        fn finish(self: *Self) !Run {
            if (self.entry_count == 0) return error.EmptyRun;
            var persisted = try self.writer.finish();
            self.writer_active = false;
            errdefer {
                self.backend.allocator.free(persisted.path);
                persisted.filter.deinit(self.backend.allocator);
            }

            const smallest_namespace_name = self.smallest_namespace_name;
            self.smallest_namespace_name = null;
            const smallest_key = self.smallest_key;
            self.smallest_key = &.{};
            const largest_namespace_name = self.largest_namespace_name;
            self.largest_namespace_name = null;
            const largest_key = self.largest_key;
            self.largest_key = &.{};
            errdefer {
                if (smallest_namespace_name) |name| self.backend.allocator.free(name);
                self.backend.allocator.free(smallest_key);
                if (largest_namespace_name) |name| self.backend.allocator.free(name);
                self.backend.allocator.free(largest_key);
            }

            return .{
                .id = self.run_id,
                .level = self.output_level,
                .size_bytes = persisted.size_bytes,
                .compression_stats = persisted.compression_stats,
                .path = persisted.path,
                .smallest_namespace_name = smallest_namespace_name,
                .smallest_key = smallest_key,
                .largest_namespace_name = largest_namespace_name,
                .largest_key = largest_key,
                .entry_count = @intCast(persisted.entry_count),
                .bloom_filter = persisted.filter,
                .state = null,
            };
        }
    };
}

const PersistedRunCursor = struct {
    allocator: std.mem.Allocator,
    storage: @import("storage_io.zig").Storage,
    path: []const u8,
    index: lsm_table_file.SequentialTableIndex,
    position: ?usize = null,
    block_index: usize = 0,
    entry_in_block: usize = 0,
    block_offset: usize = 0,
    current_entry_len: usize = 0,
    loaded_window: ?lsm_table_file.EntryDataWindow = null,
    loaded_bytes: ?[]u8 = null,

    fn init(allocator: std.mem.Allocator, storage: @import("storage_io.zig").Storage, path: []const u8) !PersistedRunCursor {
        var index = try repository_mod.loadRunSequentialTableIndexAllocWithStorage(storage, allocator, path);
        errdefer index.deinit(allocator);
        return .{
            .allocator = allocator,
            .storage = storage,
            .path = path,
            .index = index,
            .position = if (index.entry_count > 0) 0 else null,
        };
    }

    fn deinit(self: *PersistedRunCursor) void {
        if (self.loaded_bytes) |bytes| self.allocator.free(bytes);
        self.index.deinit(self.allocator);
        self.* = undefined;
    }

    fn currentEntry(self: *PersistedRunCursor) !?lsm_table_file.Entry {
        _ = self.position orelse return null;
        try self.ensureCurrentWindow();
        const bytes = self.loaded_bytes orelse return error.InvalidTableFile;
        if (self.block_offset >= bytes.len) return error.InvalidTableFile;
        const entry = try lsm_table_file.parseEntryAt(bytes, self.block_offset);
        self.current_entry_len = tableEntryLogicalBytes(entry);
        if (self.current_entry_len > bytes.len - self.block_offset) return error.InvalidTableFile;
        return entry;
    }

    fn advance(self: *PersistedRunCursor) !void {
        const pos = self.position orelse return;
        if (self.current_entry_len == 0) _ = (try self.currentEntry()) orelse return error.InvalidTableFile;
        self.block_offset += self.current_entry_len;
        self.current_entry_len = 0;
        self.entry_in_block += 1;
        const block = self.index.blocks[self.block_index];
        if (self.entry_in_block > block.entry_count) return error.InvalidTableFile;
        if (self.entry_in_block == block.entry_count) {
            const bytes = self.loaded_bytes orelse return error.InvalidTableFile;
            if (self.block_offset != bytes.len) return error.InvalidTableFile;
            self.block_index += 1;
            self.entry_in_block = 0;
            self.block_offset = 0;
        }
        if (pos + 1 < self.index.entry_count) {
            if (self.block_index >= self.index.blocks.len) return error.InvalidTableFile;
            self.position = pos + 1;
        } else {
            if (self.block_index != self.index.blocks.len) return error.InvalidTableFile;
            self.position = null;
        }
    }

    fn ensureCurrentWindow(self: *PersistedRunCursor) !void {
        if (self.block_index >= self.index.blocks.len) return error.InvalidTableFile;
        const window = self.index.blocks[self.block_index].window;
        if (self.loaded_window) |loaded| {
            if (loaded.relative_offset == window.relative_offset and
                loaded.len == window.len and
                loaded.physical_relative_offset == window.physical_relative_offset and
                loaded.physical_len == window.physical_len and
                loaded.compression == window.compression)
            {
                return;
            }
        }

        if (self.loaded_bytes) |bytes| {
            self.allocator.free(bytes);
            self.loaded_bytes = null;
        }
        const payload = try self.storage.readFileRangeAlloc(
            self.allocator,
            self.path,
            @as(u64, @intCast(self.index.entry_data_start)) + window.physicalRelativeOffset(),
            window.physicalLen(),
        );
        defer self.allocator.free(payload);
        self.loaded_bytes = try lsm_table_file.decodeBlockPayloadAlloc(
            self.allocator,
            window.compression,
            payload,
            window.len,
            window.checksum,
        );
        self.loaded_window = window;
    }
};

const PersistedRunMergeHeap = struct {
    allocator: std.mem.Allocator,
    cursors: []PersistedRunCursor,
    sources: []usize,
    advanced_sources: []usize,
    len: usize = 0,

    fn init(allocator: std.mem.Allocator, cursors: []PersistedRunCursor) !PersistedRunMergeHeap {
        const sources = try allocator.alloc(usize, cursors.len);
        errdefer allocator.free(sources);
        const advanced_sources = try allocator.alloc(usize, cursors.len);
        errdefer allocator.free(advanced_sources);

        var heap = PersistedRunMergeHeap{
            .allocator = allocator,
            .cursors = cursors,
            .sources = sources,
            .advanced_sources = advanced_sources,
        };
        errdefer heap.deinit();

        for (cursors, 0..) |*cursor, source| {
            if (cursor.position != null) try heap.pushSource(source);
        }
        return heap;
    }

    fn deinit(self: *PersistedRunMergeHeap) void {
        self.allocator.free(self.sources);
        self.allocator.free(self.advanced_sources);
        self.* = undefined;
    }

    fn peekSource(self: *const PersistedRunMergeHeap) ?usize {
        if (self.len == 0) return null;
        return self.sources[0];
    }

    fn advanceTopSourcesAtKey(self: *PersistedRunMergeHeap, key_entry: lsm_table_file.Entry) !usize {
        var advanced_len: usize = 0;
        while (self.peekSource()) |source| {
            const entry = (try self.cursors[source].currentEntry()) orelse return error.InvalidTableFile;
            if (compareTableEntry(entry, key_entry) != .eq) break;
            _ = try self.popSource();
            try self.cursors[source].advance();
            self.advanced_sources[advanced_len] = source;
            advanced_len += 1;
        }

        for (self.advanced_sources[0..advanced_len]) |source| {
            if (self.cursors[source].position != null) try self.pushSource(source);
        }
        return advanced_len;
    }

    fn pushSource(self: *PersistedRunMergeHeap, source: usize) !void {
        std.debug.assert(self.len < self.sources.len);
        self.sources[self.len] = source;
        self.len += 1;
        try self.siftUp(self.len - 1);
    }

    fn popSource(self: *PersistedRunMergeHeap) !usize {
        if (self.len == 0) return error.InvalidTableFile;
        const source = self.sources[0];
        self.len -= 1;
        if (self.len > 0) {
            self.sources[0] = self.sources[self.len];
            try self.siftDown(0);
        }
        return source;
    }

    fn siftUp(self: *PersistedRunMergeHeap, start_index: usize) !void {
        var index = start_index;
        while (index > 0) {
            const parent = (index - 1) / 2;
            if (!try self.sourceLess(self.sources[index], self.sources[parent])) break;
            std.mem.swap(usize, &self.sources[index], &self.sources[parent]);
            index = parent;
        }
    }

    fn siftDown(self: *PersistedRunMergeHeap, start_index: usize) !void {
        var index = start_index;
        while (true) {
            const left = index * 2 + 1;
            if (left >= self.len) break;
            const right = left + 1;
            var child = left;
            if (right < self.len and try self.sourceLess(self.sources[right], self.sources[left])) {
                child = right;
            }
            if (!try self.sourceLess(self.sources[child], self.sources[index])) break;
            std.mem.swap(usize, &self.sources[index], &self.sources[child]);
            index = child;
        }
    }

    fn sourceLess(self: *PersistedRunMergeHeap, lhs_source: usize, rhs_source: usize) !bool {
        const lhs = (try self.cursors[lhs_source].currentEntry()) orelse return error.InvalidTableFile;
        const rhs = (try self.cursors[rhs_source].currentEntry()) orelse return error.InvalidTableFile;
        const order = compareTableEntry(lhs, rhs);
        if (order != .eq) return order == .lt;
        return lhs_source < rhs_source;
    }
};

fn tableEntryLogicalBytes(entry: lsm_table_file.Entry) usize {
    return 1 + (3 * @sizeOf(u32)) +
        (if (entry.namespace_name) |name| name.len else 0) +
        entry.key.len +
        entry.value.len;
}

fn ensureRunStateForBackend(comptime BackendType: type, backend: *BackendType, run: *Run) !*const State {
    if (@hasField(BackendType, "storage")) {
        if (backend.storage) |storage| return try run.ensureStateWithStorage(backend.allocator, storage);
    }
    return try run.ensureState(backend.allocator);
}

fn compareTableEntry(lhs: lsm_table_file.Entry, rhs: lsm_table_file.Entry) std.math.Order {
    const namespace_order = state_mod.compareNamespace(.{ .name = lhs.namespace_name }, .{ .name = rhs.namespace_name });
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, lhs.key, rhs.key);
}

fn queueObsoleteFilePath(comptime BackendType: type, backend: *BackendType, path: []u8) !void {
    if (@hasDecl(BackendType, "queueObsoleteFilePath")) {
        try backend.queueObsoleteFilePath(path);
    } else {
        repository_mod.deleteFileAbsoluteWithStorage(backend.storage.?, path) catch {};
        backend.allocator.free(path);
    }
}

pub fn makeRun(comptime BackendType: type, backend: *BackendType, state: State) !Run {
    return try makeRunAtLevel(BackendType, backend, state, 0);
}

pub fn makeRuns(comptime BackendType: type, backend: *BackendType, state: *State) !std.ArrayListUnmanaged(Run) {
    return try makeRunsFromStateAtLevel(BackendType, backend, state, 0);
}

pub fn makeRunsFromStateBorrowed(comptime BackendType: type, backend: *BackendType, state: *const State) !std.ArrayListUnmanaged(Run) {
    if (state.entries.items.len == 0) return error.EmptyRun;
    if (backend.root_dir != null) return try makePersistedRunsFromStateBorrowedAtLevel(BackendType, backend, state, 0);

    var scratch_bytes_accounted: u64 = 0;
    if (@hasField(BackendType, "options")) {
        if (backend.options.resource_manager) |manager| {
            const bytes = std.math.mul(u64, @intCast(state.entries.items.len), @sizeOf(lsm_table_file.Entry)) catch std.math.maxInt(u64);
            manager.observeUsage(.lsm_compaction_work, &scratch_bytes_accounted, bytes);
        }
    }
    defer if (@hasField(BackendType, "options")) {
        if (backend.options.resource_manager) |manager| {
            manager.observeUsage(.lsm_compaction_work, &scratch_bytes_accounted, 0);
        }
    };
    var entries = try backend.allocator.alloc(lsm_table_file.Entry, state.entries.items.len);
    defer backend.allocator.free(entries);
    for (state.entries.items, 0..) |entry, i| {
        entries[i] = .{
            .namespace_name = entry.namespace_name,
            .key = entry.key,
            .value = entry.value,
            .tombstone = entry.tombstone,
        };
    }
    return try makeRunsFromSortedTableEntriesAtLevel(BackendType, backend, entries, 0);
}

fn makePersistedRunsFromStateBorrowedAtLevel(comptime BackendType: type, backend: *BackendType, state: *const State, level: u32) !std.ArrayListUnmanaged(Run) {
    if (state.entries.items.len == 0) return error.EmptyRun;
    try validateSortedUniqueOwnedEntries(state.entries.items);

    var runs = std.ArrayListUnmanaged(Run).empty;
    errdefer discardOutputRuns(BackendType, backend, &runs);

    const target_bytes = targetRunFileBytes(BackendType, backend);
    var start: usize = 0;
    while (start < state.entries.items.len) {
        const end = splitOwnedEntriesEnd(state.entries.items, start, target_bytes, backend.options.run_partition_prefix_bytes);
        try runs.ensureUnusedCapacity(backend.allocator, 1);

        var output: PersistedOutputRunBuilder(BackendType) = undefined;
        try output.initInPlace(backend, level, end - start);
        var output_active = true;
        errdefer if (output_active) output.deinit();

        for (state.entries.items[start..end]) |entry| {
            const table_entry = tableEntryFromOwnedEntry(entry);
            try output.appendEntry(table_entry, estimateOwnedEntryBytes(entry));
        }

        const run = try output.finish();
        output.deinit();
        output_active = false;
        runs.appendAssumeCapacity(run);
        start = end;
    }

    return runs;
}

pub fn makeRunsFromSortedTableEntries(comptime BackendType: type, backend: *BackendType, entries: []const lsm_table_file.Entry) !std.ArrayListUnmanaged(Run) {
    return try makeRunsFromSortedTableEntriesAtLevel(BackendType, backend, entries, 0);
}

fn makeRunsFromStateAtLevel(comptime BackendType: type, backend: *BackendType, state: *State, level: u32) !std.ArrayListUnmanaged(Run) {
    if (state.entries.items.len == 0) return error.EmptyRun;

    var source_entries = state.entries;
    state.entries = .empty;
    var moved_until: usize = 0;
    errdefer {
        for (source_entries.items[moved_until..]) |*entry| entry.deinit(backend.allocator);
        source_entries.deinit(backend.allocator);
    }

    var runs = std.ArrayListUnmanaged(Run).empty;
    errdefer discardOutputRuns(BackendType, backend, &runs);

    const target_bytes = targetRunFileBytes(BackendType, backend);
    var start: usize = 0;
    while (start < source_entries.items.len) {
        try runs.ensureUnusedCapacity(backend.allocator, 1);
        const end = splitOwnedEntriesEnd(source_entries.items, start, target_bytes, backend.options.run_partition_prefix_bytes);

        var chunk: State = .{};
        errdefer chunk.deinit(backend.allocator);
        try chunk.entries.ensureTotalCapacity(backend.allocator, end - start);
        for (source_entries.items[start..end]) |entry| {
            chunk.entries.appendAssumeCapacity(entry);
        }
        moved_until = end;

        const run = try makeRunAtLevel(BackendType, backend, chunk, level);
        chunk = .{};
        runs.appendAssumeCapacity(run);
        start = end;
    }

    source_entries.deinit(backend.allocator);
    return runs;
}

fn makeRunsFromSortedTableEntriesAtLevel(comptime BackendType: type, backend: *BackendType, entries: []const lsm_table_file.Entry, level: u32) !std.ArrayListUnmanaged(Run) {
    if (entries.len == 0) return error.EmptyRun;
    try validateSortedUniqueTableEntries(entries);

    var runs = std.ArrayListUnmanaged(Run).empty;
    errdefer discardOutputRuns(BackendType, backend, &runs);

    const target_bytes = targetRunFileBytes(BackendType, backend);
    var start: usize = 0;
    while (start < entries.len) {
        try runs.ensureUnusedCapacity(backend.allocator, 1);
        const end = splitTableEntriesEnd(entries, start, target_bytes, backend.options.run_partition_prefix_bytes);
        const run = try makeRunFromSortedTableEntriesAtLevel(BackendType, backend, entries[start..end], level);
        runs.appendAssumeCapacity(run);
        start = end;
    }
    return runs;
}

pub fn makeRunAtLevel(comptime BackendType: type, backend: *BackendType, state: State, level: u32) !Run {
    if (state.entries.items.len == 0) return error.EmptyRun;
    const run_id = backend.next_run_id;
    backend.next_run_id += 1;

    const smallest_namespace_name = if (state.entries.items[0].namespace_name) |name| try backend.allocator.dupe(u8, name) else null;
    errdefer if (smallest_namespace_name) |name| backend.allocator.free(name);
    const smallest_key = try backend.allocator.dupe(u8, state.entries.items[0].key);
    errdefer backend.allocator.free(smallest_key);
    const largest_namespace_name = if (state.entries.items[state.entries.items.len - 1].namespace_name) |name| try backend.allocator.dupe(u8, name) else null;
    errdefer if (largest_namespace_name) |name| backend.allocator.free(name);
    const largest_key = try backend.allocator.dupe(u8, state.entries.items[state.entries.items.len - 1].key);
    errdefer backend.allocator.free(largest_key);

    var run = Run{
        .id = run_id,
        .level = level,
        .size_bytes = estimateStateBytes(&state),
        .path = null,
        .smallest_namespace_name = smallest_namespace_name,
        .smallest_key = smallest_key,
        .largest_namespace_name = largest_namespace_name,
        .largest_key = largest_key,
        .entry_count = @intCast(state.entries.items.len),
        .bloom_filter = try repository_mod.buildFilterForStateWithConfig(
            backend.allocator,
            &state,
            backend.options.bloom,
        ),
        .state = state,
    };
    errdefer if (run.bloom_filter) |*filter| filter.deinit(backend.allocator);

    if (backend.root_dir != null) {
        run.path = try repository_mod.persistRunFileWithStorageAccounted(
            backend.storage.?,
            backend.allocator,
            backend.root_dir.?,
            &run,
            backend.options.table_block_compression,
            backend.options.table_prefix_extractor,
            backend.options.resource_manager,
        );
        if (run.state) |*persisted_state| persisted_state.deinit(backend.allocator);
        run.state = null;
    }
    return run;
}

fn makeRunFromSortedTableEntriesAtLevel(comptime BackendType: type, backend: *BackendType, entries: []const lsm_table_file.Entry, level: u32) !Run {
    if (entries.len == 0) return error.EmptyRun;

    if (backend.root_dir == null) {
        var state: State = .{};
        errdefer state.deinit(backend.allocator);
        try state.entries.ensureTotalCapacity(backend.allocator, entries.len);
        for (entries) |entry| {
            state.entries.appendAssumeCapacity(try state_mod.initEntry(
                backend.allocator,
                .{ .name = entry.namespace_name },
                entry.key,
                entry.value,
                entry.tombstone,
            ));
        }
        return try makeRunAtLevel(BackendType, backend, state, level);
    }

    const run_id = backend.next_run_id;
    backend.next_run_id += 1;

    const first = entries[0];
    const last = entries[entries.len - 1];
    const smallest_namespace_name = if (first.namespace_name) |name| try backend.allocator.dupe(u8, name) else null;
    errdefer if (smallest_namespace_name) |name| backend.allocator.free(name);
    const smallest_key = try backend.allocator.dupe(u8, first.key);
    errdefer backend.allocator.free(smallest_key);
    const largest_namespace_name = if (last.namespace_name) |name| try backend.allocator.dupe(u8, name) else null;
    errdefer if (largest_namespace_name) |name| backend.allocator.free(name);
    const largest_key = try backend.allocator.dupe(u8, last.key);
    errdefer backend.allocator.free(largest_key);

    var writer: repository_mod.StreamingRunFileWriter = undefined;
    try writer.initInPlace(
        backend.storage.?,
        backend.allocator,
        backend.root_dir.?,
        run_id,
        entries.len,
        backend.options.bloom,
        backend.options.table_block_compression,
        backend.options.table_prefix_extractor,
        backend.options.resource_manager,
    );
    var writer_active = true;
    errdefer if (writer_active) writer.deinit();
    for (entries) |entry| try writer.appendEntry(entry);
    var persisted = try writer.finish();
    writer_active = false;
    errdefer {
        backend.allocator.free(persisted.path);
        persisted.filter.deinit(backend.allocator);
    }

    return Run{
        .id = run_id,
        .level = level,
        .size_bytes = persisted.size_bytes,
        .compression_stats = persisted.compression_stats,
        .path = persisted.path,
        .smallest_namespace_name = smallest_namespace_name,
        .smallest_key = smallest_key,
        .largest_namespace_name = largest_namespace_name,
        .largest_key = largest_key,
        .entry_count = @intCast(persisted.entry_count),
        .bloom_filter = persisted.filter,
        .state = null,
    };
}

fn validateSortedUniqueTableEntries(entries: []const lsm_table_file.Entry) !void {
    if (entries.len <= 1) return;
    var prev = entries[0];
    for (entries[1..]) |entry| {
        switch (compareTableEntry(prev, entry)) {
            .lt => prev = entry,
            .eq => return error.DuplicateBulkIngestKey,
            .gt => return error.UnsortedBulkIngestEntries,
        }
    }
}

fn validateSortedUniqueOwnedEntries(entries: []const state_mod.OwnedEntry) !void {
    if (entries.len <= 1) return;
    var prev = entries[0];
    for (entries[1..]) |entry| {
        switch (compareOwnedEntry(prev, entry)) {
            .lt => prev = entry,
            .eq => return error.DuplicateBulkIngestKey,
            .gt => return error.UnsortedBulkIngestEntries,
        }
    }
}

fn estimateStateBytes(state: *const State) u64 {
    var total: u64 = 0;
    for (state.entries.items) |entry| {
        total += 1 + 3 * 4;
        if (entry.namespace_name) |name| total += name.len;
        total += entry.key.len + entry.value.len;
    }
    return total;
}

fn deinitRunList(allocator: std.mem.Allocator, runs: *std.ArrayListUnmanaged(Run)) void {
    for (runs.items) |*run| run.deinit(allocator);
    runs.deinit(allocator);
    runs.* = .empty;
}

pub fn appendOwnedRuns(dst: *std.ArrayListUnmanaged(Run), allocator: std.mem.Allocator, src: *std.ArrayListUnmanaged(Run)) !void {
    try dst.ensureUnusedCapacity(allocator, src.items.len);
    for (src.items) |*run| {
        dst.appendAssumeCapacity(run.*);
        disarmRun(run);
    }
    src.items.len = 0;
    src.deinit(allocator);
    src.* = .empty;
}

fn disarmRunList(runs: *std.ArrayListUnmanaged(Run)) void {
    for (runs.items) |*run| disarmRun(run);
}

fn disarmRun(run: *Run) void {
    const id = run.id;
    const level = run.level;
    run.* = .{
        .id = id,
        .level = level,
        .size_bytes = 0,
        .compression_stats = .{},
        .path = null,
        .smallest_namespace_name = null,
        .smallest_key = &.{},
        .largest_namespace_name = null,
        .largest_key = &.{},
        .entry_count = 0,
        .bloom_filter = null,
        .owns_metadata = false,
        .owns_bloom_filter = false,
        .cached_state_index = null,
        .cached_index_index = null,
        .cached_table_index = null,
        .table_index = null,
        .state = null,
    };
}

fn targetRunFileBytes(comptime BackendType: type, backend: *BackendType) usize {
    return @max(@as(usize, 1), @min(backend.options.max_run_file_bytes, lsm_table_file.max_entry_data_len));
}

fn splitOwnedEntriesEnd(entries: []const state_mod.OwnedEntry, start: usize, target_bytes: usize, partition_prefix_bytes: usize) usize {
    var total: usize = 0;
    var end = start;
    while (end < entries.len) : (end += 1) {
        if (end > start and !sameRunPartition(
            entries[start].namespace_name,
            entries[start].key,
            entries[end].namespace_name,
            entries[end].key,
            partition_prefix_bytes,
        )) break;
        const entry_bytes = estimateOwnedEntryBytes(entries[end]);
        if (end > start and total +| entry_bytes > target_bytes) break;
        total +|= entry_bytes;
    }
    return end;
}

fn splitTableEntriesEnd(entries: []const lsm_table_file.Entry, start: usize, target_bytes: usize, partition_prefix_bytes: usize) usize {
    var total: usize = 0;
    var end = start;
    while (end < entries.len) : (end += 1) {
        if (end > start and !sameRunPartition(
            entries[start].namespace_name,
            entries[start].key,
            entries[end].namespace_name,
            entries[end].key,
            partition_prefix_bytes,
        )) break;
        const entry_bytes = estimateTableEntryBytes(entries[end]);
        if (end > start and total +| entry_bytes > target_bytes) break;
        total +|= entry_bytes;
    }
    return end;
}

fn sameRunPartition(
    lhs_namespace_name: ?[]const u8,
    lhs_key: []const u8,
    rhs_namespace_name: ?[]const u8,
    rhs_key: []const u8,
    prefix_bytes: usize,
) bool {
    if (prefix_bytes == 0) return true;
    if (state_mod.compareNamespace(
        .{ .name = lhs_namespace_name },
        .{ .name = rhs_namespace_name },
    ) != .eq) return false;
    const lhs_len = @min(prefix_bytes, lhs_key.len);
    const rhs_len = @min(prefix_bytes, rhs_key.len);
    return lhs_len == rhs_len and std.mem.eql(u8, lhs_key[0..lhs_len], rhs_key[0..rhs_len]);
}

fn estimateOwnedEntryBytes(entry: state_mod.OwnedEntry) usize {
    var total: usize = 1 + 3 * @sizeOf(u32);
    if (entry.namespace_name) |name| total +|= name.len;
    total +|= entry.key.len;
    total +|= entry.value.len;
    return total;
}

fn estimateTableEntryBytes(entry: lsm_table_file.Entry) usize {
    var total: usize = 1 + 3 * @sizeOf(u32);
    if (entry.namespace_name) |name| total +|= name.len;
    total +|= entry.key.len;
    total +|= entry.value.len;
    return total;
}

fn tableEntryFromOwnedEntry(entry: state_mod.OwnedEntry) lsm_table_file.Entry {
    return .{
        .namespace_name = entry.namespace_name,
        .key = entry.key,
        .value = entry.value,
        .tombstone = entry.tombstone,
    };
}

fn compareOwnedEntry(lhs: state_mod.OwnedEntry, rhs: state_mod.OwnedEntry) std.math.Order {
    return compareTableEntry(tableEntryFromOwnedEntry(lhs), tableEntryFromOwnedEntry(rhs));
}

test "unlocked compaction snapshots retain source file references until build cleanup" {
    const allocator = std.testing.allocator;
    const FakeBackend = struct {
        allocator: std.mem.Allocator,
        runs: std.ArrayListUnmanaged(Run) = .empty,
        retained: usize = 0,
        released: usize = 0,

        fn retainRunSnapshotRef(self: *@This(), run: *Run) !void {
            try std.testing.expect(!run.version_ref_pinned);
            run.version_ref_pinned = true;
            self.retained += 1;
        }

        fn releaseRunSnapshotRef(self: *@This(), run: *Run) void {
            if (!run.version_ref_pinned) return;
            run.version_ref_pinned = false;
            self.released += 1;
        }
    };

    var backend = FakeBackend{ .allocator = allocator };
    defer deinitRunList(allocator, &backend.runs);
    try backend.runs.append(allocator, .{
        .id = 1,
        .level = 0,
        .size_bytes = 7,
        .path = try allocator.dupe(u8, "/memory/runs/1.tbl"),
        .smallest_namespace_name = null,
        .smallest_key = try allocator.dupe(u8, "a"),
        .largest_namespace_name = null,
        .largest_key = try allocator.dupe(u8, "z"),
        .entry_count = 1,
        .bloom_filter = null,
        .state = null,
    });

    var snapshots = std.ArrayListUnmanaged(Run).empty;
    try appendPlanRunSnapshots(FakeBackend, &backend, .{
        .source_level = 0,
        .source_start = 0,
        .source_len = 1,
        .target_start = 1,
        .target_len = 0,
        .output_level = 1,
    }, &snapshots);
    try std.testing.expectEqual(@as(usize, 1), backend.retained);
    try std.testing.expectEqual(@as(usize, 0), backend.released);
    try std.testing.expect(snapshots.items[0].version_ref_pinned);

    releaseCompactionSnapshots(FakeBackend, &backend, &snapshots);
    try std.testing.expectEqual(@as(usize, 1), backend.released);
    try std.testing.expectEqual(@as(usize, 0), snapshots.items.len);
}
