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

const State = state_mod.State;
const Run = repository_mod.Run;
pub const max_remembered_compaction_run_ids = 64;

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
    errdefer deinitRunList(backend.allocator, &new_runs);
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
    if (try compactRememberedPlanIfValid(BackendType, backend)) return true;

    const plan = selectCompactionPlan(
        backend.runs.items,
        backend.options.compact_threshold_runs,
        backend.options.l0_overlap_compact_threshold_runs,
        backend.options.level_target_runs_base,
        backend.options.level_target_runs_multiplier,
        backend.options.level_target_bytes_base,
        backend.options.level_target_bytes_multiplier,
        backend.options.max_compaction_input_bytes,
        allowOversizedSingleCompactionInput(backend),
    ) orelse return false;

    const work = compactionWorkForPlan(backend.runs.items, plan, score);
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

    const plan = selectL0Compaction(
        backend.runs.items,
        l0_limit,
        backend.options.max_compaction_input_bytes,
        allowOversizedSingleCompactionInput(backend),
    ) orelse return false;
    const work = compactionWorkForPlan(backend.runs.items, plan, score);
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
    const plan = selectL0Compaction(
        backend.runs.items,
        l0_limit,
        effective_limit,
        max_input_bytes == null and allowOversizedSingleCompactionInput(backend),
    ) orelse return false;
    const work = compactionWorkForPlan(backend.runs.items, plan, score);
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

fn compactionWorkForPlan(runs: []const Run, plan: CompactionPlan, score: u64) struct {
    score: u64,
    input_runs: usize,
    input_bytes: u64,
    io_bytes: u64,
    run_ids: [max_remembered_compaction_run_ids]u64,
    run_count: usize,
} {
    var input_runs: usize = 0;
    var input_bytes: u64 = 0;
    var run_ids: [max_remembered_compaction_run_ids]u64 = undefined;
    var run_count: usize = 0;
    for (runs[plan.source_start .. plan.source_start + plan.source_len]) |run| {
        input_runs += 1;
        input_bytes +|= run.size_bytes;
        if (run_count < run_ids.len) {
            run_ids[run_count] = run.id;
        }
        run_count += 1;
    }
    for (runs[plan.target_start .. plan.target_start + plan.target_len]) |run| {
        input_runs += 1;
        input_bytes +|= run.size_bytes;
        if (run_count < run_ids.len) {
            run_ids[run_count] = run.id;
        }
        run_count += 1;
    }
    return .{
        .score = score,
        .input_runs = input_runs,
        .input_bytes = input_bytes,
        .io_bytes = input_bytes +| input_bytes,
        .run_ids = run_ids,
        .run_count = run_count,
    };
}

fn planWithinInputBudget(runs: []const Run, plan: CompactionPlan, max_input_bytes: u64) bool {
    if (max_input_bytes == 0) return true;
    return compactionWorkForPlan(runs, plan, 0).input_bytes <= max_input_bytes;
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

    const work = compactionWorkForPlan(backend.runs.items, plan, remembered.score);
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
    const work = compactionWorkForPlan(runs, plan, score);
    if (work.run_count != total_runs) return null;

    var remembered = RememberedCompaction{
        .plan = plan,
        .run_count = total_runs,
        .input_runs = work.input_runs,
        .input_bytes = work.input_bytes,
        .score = score,
    };
    @memcpy(remembered.run_ids[0..total_runs], work.run_ids[0..total_runs]);
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
    errdefer deinitRunList(backend.allocator, &compacted_runs);

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
    errdefer deinitRunList(backend.allocator, &selected_runs);
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

    backend.retainReader();
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
    errdefer if (reader_retained) backend.releaseReader();
    errdefer if (build_result_valid) discardOutputRuns(BackendType, backend, &build_result);
    if (build_err) |err| {
        return err;
    }

    if (!planRunIdsStillMatch(backend.runs.items, plan, selected_run_ids)) {
        backend.releaseReader();
        reader_retained = false;
        discardOutputRuns(BackendType, backend, &build_result);
        deinitRunList(backend.allocator, &selected_runs);
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
    backend.releaseReader();
    reader_retained = false;
    deinitRunList(backend.allocator, &selected_runs);
}

fn appendPlanRunSnapshots(
    comptime BackendType: type,
    backend: *BackendType,
    plan: CompactionPlan,
    out: *std.ArrayListUnmanaged(Run),
) !void {
    try out.ensureUnusedCapacity(backend.allocator, plan.source_len + plan.target_len);
    for (backend.runs.items[plan.source_start .. plan.source_start + plan.source_len]) |run| {
        out.appendAssumeCapacity(try repository_mod.cloneRunCompactionSnapshot(backend.allocator, run));
    }
    for (backend.runs.items[plan.target_start .. plan.target_start + plan.target_len]) |run| {
        out.appendAssumeCapacity(try repository_mod.cloneRunCompactionSnapshot(backend.allocator, run));
    }
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
    if (runs.len < 2) return null;
    if (selectL0OverlapCompaction(runs, l0_overlap_compact_threshold_runs, max_input_bytes)) |plan| return plan;
    if (selectL0Compaction(runs, l0_limit, max_input_bytes, allow_oversized_single_job)) |plan| return plan;
    if (selectLowerLevelRepairCompaction(runs, max_input_bytes, allow_oversized_single_job)) |plan| return plan;
    return selectLowerLevelPressureCompaction(
        runs,
        level_target_runs_base,
        level_target_runs_multiplier,
        level_target_bytes_base,
        level_target_bytes_multiplier,
        max_input_bytes,
        allow_oversized_single_job,
    );
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
    if (threshold == 0) return null;
    const l0_count = countLeadingL0Runs(runs);
    if (l0_count < threshold) return null;

    var best_start: usize = 0;
    var best_len: usize = 0;
    var best_bytes: u64 = std.math.maxInt(u64);
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
        if (!planWithinInputBudget(runs, plan, max_input_bytes)) continue;
        if (best_len == 0 or count > best_len or (count == best_len and bytes < best_bytes)) {
            best_start = span_start;
            best_len = span_len;
            best_bytes = bytes;
        }
    }
    if (best_len == 0) return null;
    return buildPlanForSourceRange(runs, 0, best_start, best_len);
}

fn countLeadingL0Runs(runs: []const Run) usize {
    var l0_count: usize = 0;
    while (l0_count < runs.len and runs[l0_count].level == 0) : (l0_count += 1) {}
    return l0_count;
}

fn selectL0Compaction(runs: []const Run, l0_limit: usize, max_input_bytes: u64, allow_oversized_single_job: bool) ?CompactionPlan {
    const l0_count = countLeadingL0Runs(runs);
    if (l0_count == 0 or l0_count <= l0_limit) return null;
    const target_l0_count = @max(@as(usize, 1), l0_limit / 2);
    const excess_len = @max(@as(usize, 1), l0_count - target_l0_count);
    const max_window_len = if (l0_limit == 0)
        @as(usize, 2)
    else
        std.math.mul(usize, @max(@as(usize, 1), l0_limit), 2) catch std.math.maxInt(usize);
    var source_len = @min(excess_len, max_window_len);
    var oversized_plan: ?CompactionPlan = null;
    while (source_len > 0) : (source_len -= 1) {
        const plan = buildPlanForSourceRange(runs, 0, l0_count - source_len, source_len) orelse continue;
        if (planWithinInputBudget(runs, plan, max_input_bytes)) return plan;
        if (allow_oversized_single_job and max_input_bytes > 0) oversized_plan = plan;
    }
    return oversized_plan;
}

fn selectLowerLevelRepairCompaction(runs: []const Run, max_input_bytes: u64, allow_oversized_single_job: bool) ?CompactionPlan {
    var oversized_plan: ?CompactionPlan = null;
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
        if (planWithinInputBudget(runs, plan, max_input_bytes)) return plan;
        if (allow_oversized_single_job and max_input_bytes > 0 and oversized_plan == null) oversized_plan = plan;
    }
    return oversized_plan;
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
        const target_bytes = levelByteTarget(level, level_target_bytes_base, level_target_bytes_multiplier);
        const need_runs = level_len > target_runs;
        const need_bytes = target_bytes > 0 and level_bytes > target_bytes;
        if (!need_runs and !need_bytes) continue;

        const source_len = if (need_runs) @max(@as(usize, 1), level_len - target_runs) else 1;
        const source_bytes = if (need_bytes) @max(@as(u64, 1), level_bytes - target_bytes) else 0;
        if (selectLowestOverlapWindow(
            runs,
            level,
            level_start,
            level_len,
            source_len,
            source_bytes,
            max_input_bytes,
            allow_oversized_single_job,
        )) |plan| return plan;
    }
    return null;
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
) ?CompactionPlan {
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
    return best_plan orelse oversized_plan;
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

fn levelByteTarget(level: u32, base: usize, multiplier: usize) u64 {
    if (level == 0 or base == 0) return 0;
    var target = @max(@as(u64, 1), @as(u64, @intCast(base)));
    var remaining = level - 1;
    const factor = @max(@as(u64, 1), @as(u64, @intCast(multiplier)));
    while (remaining > 0) : (remaining -= 1) {
        target = std.math.mul(u64, target, factor) catch std.math.maxInt(u64);
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

fn buildPlanForSourceRange(runs: []const Run, source_level: u32, source_start: usize, source_len: usize) ?CompactionPlan {
    if (source_len == 0) return null;
    var smallest_namespace_name = runs[source_start].smallest_namespace_name;
    var smallest_key = runs[source_start].smallest_key;
    var largest_namespace_name = runs[source_start].largest_namespace_name;
    var largest_key = runs[source_start].largest_key;

    for (runs[source_start + 1 .. source_start + source_len]) |run| {
        if (compareRunBound(run.smallest_namespace_name, run.smallest_key, smallest_namespace_name, smallest_key) == .lt) {
            smallest_namespace_name = run.smallest_namespace_name;
            smallest_key = run.smallest_key;
        }
        if (compareRunBound(run.largest_namespace_name, run.largest_key, largest_namespace_name, largest_key) == .gt) {
            largest_namespace_name = run.largest_namespace_name;
            largest_key = run.largest_key;
        }
    }

    const output_level = source_level + 1;
    var target_start: ?usize = null;
    var target_end: usize = 0;
    for (runs, 0..) |run, i| {
        if (run.level < output_level) continue;
        if (run.level > output_level) break;
        if (!rangesOverlap(
            run.smallest_namespace_name,
            run.smallest_key,
            run.largest_namespace_name,
            run.largest_key,
            smallest_namespace_name,
            smallest_key,
            largest_namespace_name,
            largest_key,
        )) continue;
        if (target_start == null) target_start = i;
        target_end = i + 1;
    }

    return .{
        .source_level = source_level,
        .source_start = source_start,
        .source_len = source_len,
        .target_start = target_start orelse source_start + source_len,
        .target_len = if (target_start == null) 0 else target_end - target_start.?,
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
        .encoded_bloom_filter = null,
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
        if (cursors[i].index.entryCount() != run.entry_count) return error.InvalidTableFile;
        initialized_cursors += 1;
    }

    var runs = std.ArrayListUnmanaged(Run).empty;
    errdefer deinitRunList(allocator, &runs);
    var output: PersistedOutputRunBuilder(BackendType) = undefined;
    var output_active = false;
    defer if (output_active) output.deinit();
    const target_bytes = targetRunFileBytes(BackendType, backend);
    var consumed_entries: usize = 0;
    var emitted_entries: usize = 0;

    while (true) {
        const candidate_source = try bestCursorSourceIndex(cursors[0..initialized_cursors]) orelse break;
        const winner_source = try newestCursorSourceAtKey(cursors[0..initialized_cursors], candidate_source);
        const winner = (try cursors[winner_source].currentEntry()) orelse return error.InvalidTableFile;
        const entry_bytes = tableEntryLogicalBytes(winner);
        if (output_active) {
            if (output.entry_count > 0 and target_bytes > 0 and output.logical_bytes + entry_bytes > target_bytes) {
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
        consumed_entries += try advanceCursorsAtKey(cursors[0..initialized_cursors], winner);

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
            if (self.entry_count == 0) {
                self.smallest_namespace_name = if (entry.namespace_name) |name| try self.backend.allocator.dupe(u8, name) else null;
                errdefer if (self.smallest_namespace_name) |name| self.backend.allocator.free(name);
                self.smallest_key = try self.backend.allocator.dupe(u8, entry.key);
                errdefer self.backend.allocator.free(self.smallest_key);
            }
            if (self.largest_namespace_name) |name| self.backend.allocator.free(name);
            if (self.largest_key.len > 0) self.backend.allocator.free(self.largest_key);
            self.largest_namespace_name = if (entry.namespace_name) |name| try self.backend.allocator.dupe(u8, name) else null;
            errdefer if (self.largest_namespace_name) |name| self.backend.allocator.free(name);
            self.largest_key = try self.backend.allocator.dupe(u8, entry.key);

            try self.writer.appendEntry(entry);
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
                .encoded_bloom_filter = null,
                .state = null,
            };
        }
    };
}

const PersistedRunCursor = struct {
    allocator: std.mem.Allocator,
    storage: @import("storage_io.zig").Storage,
    path: []const u8,
    index: lsm_table_file.TableIndex,
    position: ?usize = null,
    loaded_window: ?lsm_table_file.EntryDataWindow = null,
    loaded_bytes: ?[]u8 = null,

    fn init(allocator: std.mem.Allocator, storage: @import("storage_io.zig").Storage, path: []const u8) !PersistedRunCursor {
        var index = try repository_mod.loadRunTableIndexAllocWithStorage(storage, allocator, path);
        errdefer index.deinit(allocator);
        return .{
            .allocator = allocator,
            .storage = storage,
            .path = path,
            .index = index,
            .position = if (index.entryCount() > 0) 0 else null,
        };
    }

    fn deinit(self: *PersistedRunCursor) void {
        if (self.loaded_bytes) |bytes| self.allocator.free(bytes);
        self.index.deinit(self.allocator);
        self.* = undefined;
    }

    fn currentEntry(self: *PersistedRunCursor) !?lsm_table_file.Entry {
        const pos = self.position orelse return null;
        try self.ensureWindowForPosition(pos);
        const window = self.loaded_window orelse return error.InvalidTableFile;
        const bytes = self.loaded_bytes orelse return error.InvalidTableFile;
        const entry_start = self.index.entryStart(pos);
        if (entry_start < window.relative_offset) return error.InvalidTableFile;
        const relative_offset: usize = @intCast(entry_start - window.relative_offset);
        return try lsm_table_file.parseEntryAt(bytes, relative_offset);
    }

    fn advance(self: *PersistedRunCursor) void {
        const pos = self.position orelse return;
        self.position = if (pos + 1 < self.index.entryCount()) pos + 1 else null;
    }

    fn ensureWindowForPosition(self: *PersistedRunCursor, pos: usize) !void {
        const window = self.index.entryDataWindow(pos, lsm_table_file.default_block_size);
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
        self.loaded_bytes = try lsm_table_file.decodeBlockPayloadAlloc(self.allocator, window.compression, payload, window.len);
        self.loaded_window = window;
    }
};

fn bestCursorSourceIndex(cursors: []PersistedRunCursor) !?usize {
    var best: ?usize = null;
    for (cursors, 0..) |*cursor, i| {
        const candidate = (try cursor.currentEntry()) orelse continue;
        if (best == null) {
            best = i;
            continue;
        }
        const incumbent = (try cursors[best.?].currentEntry()) orelse {
            best = i;
            continue;
        };
        if (compareTableEntry(candidate, incumbent) == .lt) best = i;
    }
    return best;
}

fn newestCursorSourceAtKey(cursors: []PersistedRunCursor, candidate_source: usize) !usize {
    var winner = candidate_source;
    const winner_entry = (try cursors[winner].currentEntry()) orelse return error.InvalidTableFile;
    for (cursors, 0..) |*cursor, i| {
        const entry = (try cursor.currentEntry()) orelse continue;
        if (compareTableEntry(entry, winner_entry) != .eq) continue;
        if (i < winner) winner = i;
    }
    return winner;
}

fn advanceCursorsAtKey(cursors: []PersistedRunCursor, key_entry: lsm_table_file.Entry) !usize {
    var advanced: usize = 0;
    for (cursors) |*cursor| {
        const entry = (try cursor.currentEntry()) orelse continue;
        if (compareTableEntry(entry, key_entry) != .eq) continue;
        cursor.advance();
        advanced += 1;
    }
    return advanced;
}

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
    errdefer deinitRunList(backend.allocator, &runs);

    const target_bytes = targetRunFileBytes(BackendType, backend);
    var start: usize = 0;
    while (start < source_entries.items.len) {
        try runs.ensureUnusedCapacity(backend.allocator, 1);
        const end = splitOwnedEntriesEnd(source_entries.items, start, target_bytes);

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
    errdefer deinitRunList(backend.allocator, &runs);

    const target_bytes = targetRunFileBytes(BackendType, backend);
    var start: usize = 0;
    while (start < entries.len) {
        try runs.ensureUnusedCapacity(backend.allocator, 1);
        const end = splitTableEntriesEnd(entries, start, target_bytes);
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
        .encoded_bloom_filter = null,
        .state = state,
    };
    errdefer if (run.bloom_filter) |*filter| filter.deinit(backend.allocator);

    if (backend.root_dir != null) {
        run.path = try repository_mod.persistRunFileWithStorage(backend.storage.?, backend.allocator, backend.root_dir.?, &run, backend.options.table_block_compression);
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
        .encoded_bloom_filter = null,
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
        .encoded_bloom_filter = null,
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

fn splitOwnedEntriesEnd(entries: []const state_mod.OwnedEntry, start: usize, target_bytes: usize) usize {
    var total: usize = 0;
    var end = start;
    while (end < entries.len) : (end += 1) {
        const entry_bytes = estimateOwnedEntryBytes(entries[end]);
        if (end > start and total +| entry_bytes > target_bytes) break;
        total +|= entry_bytes;
    }
    return end;
}

fn splitTableEntriesEnd(entries: []const lsm_table_file.Entry, start: usize, target_bytes: usize) usize {
    var total: usize = 0;
    var end = start;
    while (end < entries.len) : (end += 1) {
        const entry_bytes = estimateTableEntryBytes(entries[end]);
        if (end > start and total +| entry_bytes > target_bytes) break;
        total +|= entry_bytes;
    }
    return end;
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
