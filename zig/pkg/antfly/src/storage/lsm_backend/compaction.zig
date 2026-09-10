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
const Directory = @import("run_directory.zig").Directory;
const run_store = @import("run_store.zig");
const ClosureJob = @import("closure_job.zig").Job;
const DependencyValidation = @import("dependency_validation.zig").Validation;
pub const BulkPolicy = @import("bulk_selection.zig").Policy;
const BulkSelection = @import("bulk_selection.zig").Job;
const resource_manager_mod = @import("../resource_manager.zig");

const State = state_mod.State;
const Run = repository_mod.Run;

test "resumable closure bounds discovery and emission and cleans up every allocation failure" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
        fn check(allocator: std.mem.Allocator, directory: *const Directory) !void {
            var manager = resource_manager_mod.ResourceManager.init(.{});
            defer std.debug.assert(manager.sliceStats(.lsm_table_builder_working_set).used_bytes == 0);
            var scratch = resource_manager_mod.BudgetedAllocator.init(&manager, .lsm_table_builder_working_set, allocator, 1);
            defer scratch.deinit();
            var job = try ClosureJob.init(scratch.allocator(), directory, &.{directory.at(0)}, 0, false);
            job.output_manager = &manager;
            defer job.deinit(allocator);
            try std.testing.expect(!try job.step(allocator, 0));
            try std.testing.expect(!try job.stepUntil(allocator, 7, 0));
            try std.testing.expectEqual(@as(usize, 0), job.visits);
            var slices: usize = 0;
            while (true) {
                const before = job.visits + job.emitted;
                const done = try job.step(allocator, 7);
                try std.testing.expect(job.visits + job.emitted - before <= 7);
                slices += 1;
                if (done) break;
            }
            try std.testing.expect(slices > 1);
            try std.testing.expect(try job.stepUntil(allocator, 1, 0));
            try std.testing.expectEqual(directory.count(), job.emitted);
            try std.testing.expectEqual(@as(usize, 1), job.source_len);
            for (job.handles.?, job.indices.?, 0..) |handle, index, rank| {
                try std.testing.expectEqual(rank, index);
                try std.testing.expectEqual(directory.at(rank).run.id, handle.run.id);
            }
        }
    };
    const allocator = std.testing.allocator;
    var fixture = Fixture{ .allocator = allocator };
    const directory = try Directory.create(allocator);
    defer directory.destroy(allocator);
    for (0..33) |i| {
        var key: [8]u8 = undefined;
        var end: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .big);
        std.mem.writeInt(u64, &end, if (i == 0) 33 else i, .big);
        try directory.put(&fixture, .{
            .id = i + 1,
            .level = if (i == 0) 0 else 1,
            .size_bytes = 1,
            .path = @constCast("closure.sst"),
            .smallest_namespace_name = null,
            .smallest_key = &key,
            .largest_namespace_name = null,
            .largest_key = &end,
            .entry_count = 1,
            .bloom_filter = null,
            .state = null,
        });
    }
    try std.testing.checkAllAllocationFailures(allocator, Fixture.check, .{directory});
    var limited = try ClosureJob.init(allocator, directory, &.{directory.at(0)}, 2, false);
    defer limited.deinit(allocator);
    while (!try limited.step(allocator, 1)) {}
    try std.testing.expect(limited.phase == .oversized);
}

fn gcNowNs() u64 {
    return @import("antfly_platform").time.realtimeNs();
}

pub fn nextTombstoneGcDelay(backend: anytype) ?u64 {
    // A denied, already-eligible job is independent of the age trigger. In
    // particular, disabling age-based GC must not disable admission retries.
    if (comptime @hasField(@TypeOf(backend.*), "tombstone_gc_retry_after_ns")) {
        if (backend.tombstone_gc_retry_after_ns != 0)
            return backend.tombstone_gc_retry_after_ns -| backend.nowNs();
    }
    if (comptime @hasField(@TypeOf(backend.*), "run_directory_dirty")) {
        if (!backend.run_directory_dirty) if (backend.run_directory) |directory|
            return directory.tombstoneGcDelay(backend.options.tombstone_gc_max_age_ns, gcNowNs());
    }
    for (0..run_store.count(backend)) |rank| {
        const run = run_store.at(backend, rank);
        if (run.gc_requested and (run.tombstone_count orelse 0) != 0) return 0;
    }
    if (comptime !@hasField(@TypeOf(backend.options), "tombstone_gc_max_age_ns")) return null;
    if (backend.options.tombstone_gc_max_age_ns == 0) return null;
    const now = gcNowNs();
    var delay: ?u64 = null;
    for (0..run_store.count(backend)) |rank| {
        const run = run_store.at(backend, rank).*;
        if ((run.tombstone_count orelse 0) == 0) continue;
        // Unknown ages and wall-clock rollback must not postpone GC indefinitely.
        const due = if (run.oldest_tombstone_unix_ns == 0 or run.oldest_tombstone_unix_ns > now) 0 else run.oldest_tombstone_unix_ns +| backend.options.tombstone_gc_max_age_ns;
        const candidate = due -| now;
        delay = if (delay) |current| @min(current, candidate) else candidate;
    }
    return delay;
}

fn tombstoneAgeDue(backend: anytype, run: Run) bool {
    if (comptime !@hasField(@TypeOf(backend.options), "tombstone_gc_max_age_ns")) return false;
    const age = backend.options.tombstone_gc_max_age_ns;
    const now = gcNowNs();
    return age != 0 and (run.oldest_tombstone_unix_ns == 0 or run.oldest_tombstone_unix_ns > now or now -| run.oldest_tombstone_unix_ns >= age);
}
pub const max_remembered_compaction_run_ids = 64;
pub const max_exact_l0_overlap_runs = 64;
/// Differential benchmark switch only; there is no production legacy planner
/// mode for a configured compaction domain.
pub var test_output_partitions_only: bool = false;

fn domainPlanningEnabled(backend: anytype) bool {
    if (comptime @hasDecl(@TypeOf(backend.*), "planningDirectory")) return !(@import("builtin").is_test and test_output_partitions_only);
    return backend.options.run_partition_key != null and !(@import("builtin").is_test and test_output_partitions_only);
}

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
    complete_coverage: ?bool = null,
    // A certificate may bypass repeated identity/coverage work only while
    // this exact publication generation remains current under the mutex.
    validated_generation: ?u64 = null,
    // Set only by discovery of a minimum indivisible oversized closure.
    oversized_indivisible: bool = false,
    source_level: u32,
    source_start: usize,
    source_len: usize,
    target_start: usize,
    target_len: usize,
    output_level: u32,
    // Domain-local positions map to the immutable global run version. The
    // selecting caller owns this slice until build/publication completes.
    run_indices: ?[]const usize = null,
    partition_key: PartitionKey = null,
    // A whole overlap component, potentially spanning several levels.
    tombstone_gc: bool = false,
    split_gc: bool = false,
    input_handles: ?[]const Directory.Handle = null,

    pub fn sourceIndex(self: @This(), i: usize) usize {
        const index = self.source_start + i;
        return if (self.run_indices) |indices| indices[index] else index;
    }

    pub fn targetIndex(self: @This(), i: usize) usize {
        const index = self.target_start + i;
        return if (self.run_indices) |indices| indices[index] else index;
    }
};

const SelectedPlan = struct {
    plan: CompactionPlan,
    borrowed_inputs: bool = false,
    complete_coverage: ?bool = null,
    reservation: ?resource_manager_mod.Reservation = null,
    gc_objective_handles: ?[]const Directory.Handle = null,
    gc_objective_indices: ?[]const usize = null,
    objective_reservation: ?resource_manager_mod.Reservation = null,
    released_inputs: usize = 0,
    released_objectives: usize = 0,
    fn deinit(self: @This(), allocator: std.mem.Allocator) void {
        var owned = self;
        var credits: usize = std.math.maxInt(usize);
        std.debug.assert(owned.deinitStep(allocator, &credits));
    }
    fn release(self: @This(), backend: anytype) void {
        if (comptime @typeInfo(@TypeOf(backend)) != .pointer) {
            self.deinit(backend.allocator);
            return;
        }
        if (comptime !supportsUnlockedBackendCompaction(@TypeOf(backend.*))) {
            self.deinit(backend.allocator);
            return;
        }
        if (self.borrowed_inputs) {
            self.deinit(backend.allocator);
            return;
        }
        // End-of-operation cleanup cannot extend the writer fence with K
        // handle releases. The caller no longer uses this selected plan.
        backend.retainReaderKind(.compaction);
        runtime_mod.unlockBackend(@TypeOf(backend.*), backend, true);
        var owned = self;
        while (true) {
            var credits: usize = 2048;
            const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
            var done = false;
            while (credits != 0 and @import("antfly_platform").time.monotonicNs() < deadline) {
                var quantum: usize = @min(credits, 64);
                const before = quantum;
                done = owned.deinitStep(backend.allocator, &quantum);
                credits -= before - quantum;
                if (done) break;
            }
            if (done) break;
            if (backend.manifestCoordinationIo()) |io| io.sleep(.fromNanoseconds(1), .awake) catch {};
        }
        _ = runtime_mod.lockBackend(@TypeOf(backend.*), backend);
        backend.releaseReaderKind(.compaction);
    }
    fn deinitStep(self: *@This(), allocator: std.mem.Allocator, credits: *usize) bool {
        if (!self.borrowed_inputs) if (self.plan.input_handles) |handles| {
            while (self.released_inputs < handles.len and credits.* != 0) {
                handles[self.released_inputs].release(allocator);
                self.released_inputs += 1;
                credits.* -= 1;
            }
            if (self.released_inputs != handles.len) return false;
            allocator.free(handles);
            self.plan.input_handles = null;
        };
        if (self.gc_objective_handles) |handles| {
            while (self.released_objectives < handles.len and credits.* != 0) {
                handles[self.released_objectives].release(allocator);
                self.released_objectives += 1;
                credits.* -= 1;
            }
            if (self.released_objectives != handles.len) return false;
            allocator.free(handles);
            self.gc_objective_handles = null;
        }
        if (self.plan.run_indices) |indices| allocator.free(indices);
        self.plan.run_indices = null;
        if (self.gc_objective_indices) |indices| allocator.free(indices);
        self.gc_objective_indices = null;
        if (self.reservation) |*lease| lease.release();
        self.reservation = null;
        if (self.objective_reservation) |*lease| lease.release();
        self.objective_reservation = null;
        return true;
    }
};

/// A continuation belongs to a request policy, not just a directory epoch.
/// Exact matching within a lane avoids reusing discovery after a caller
/// changes its pressure target or admission contract.
const PlanningPolicy = struct {
    l0_limit: usize,
    l0_only: bool,
    max_bytes: u64,
    allow_oversized: bool,

    fn matches(self: @This(), other: @This()) bool {
        return std.meta.eql(self, other);
    }

    fn admits(self: @This(), plan: CompactionPlan, input_bytes: u64) bool {
        if (self.l0_only and plan.source_level != 0) return false;
        return self.max_bytes == 0 or input_bytes <= self.max_bytes or
            (self.allow_oversized and plan.oversized_indivisible);
    }
};

test "compaction policy admission requires the requested level and explicit oversized proof" {
    const policy = PlanningPolicy{ .l0_limit = 1, .l0_only = true, .max_bytes = 100, .allow_oversized = false };
    var plan = CompactionPlan{ .source_level = 0, .source_start = 0, .source_len = 1, .target_start = 1, .target_len = 0, .output_level = 1 };
    try std.testing.expect(policy.admits(plan, 100));
    try std.testing.expect(!policy.admits(plan, 101));
    var oversized = policy;
    oversized.allow_oversized = true;
    try std.testing.expect(!oversized.admits(plan, 101));
    plan.oversized_indivisible = true;
    try std.testing.expect(oversized.admits(plan, 101));
    try std.testing.expect(!policy.admits(plan, 101));
    plan.source_level = 1;
    try std.testing.expect(!oversized.admits(plan, 1));
    try std.testing.expect(!policy.matches(oversized));
    var changed = policy;
    changed.l0_limit += 1;
    try std.testing.expect(!policy.matches(changed));
}

test "compaction policy lanes isolate budgets retain progress and drain abandoned requests" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const allocator = std.testing.allocator;
    var backend = Backend.init(allocator, .{ .compact_threshold_runs = 0, .level_target_runs_base = 100000, .level_target_bytes_base = 0 });
    defer backend.close();
    for (0..5001) |i| {
        var state: State = .{};
        errdefer state.deinit(allocator);
        var key: [16]u8 = undefined;
        const name = try std.fmt.bufPrint(&key, "doc:{d:0>4}", .{if (i == 5000) 0 else i});
        try state.appendUpsert(allocator, .{ .name = "docs" }, name, "value", false);
        if (i == 5000) try state.appendUpsert(allocator, .{ .name = "docs" }, "doc:9999", "value", false);
        var run = try makeRunAtLevel(Backend, &backend, state, if (i == 5000) 0 else 1);
        state = .{};
        errdefer run.deinit(allocator);
        try backend.runs.append(allocator, run);
    }
    try backend.runs.reindexForTest(allocator);
    const locked = runtime_mod.lockBackend(Backend, &backend);
    defer runtime_mod.unlockBackend(Backend, &backend, locked);
    var stats: CompactionSelectionStats = .{};
    try std.testing.expect(try selectDomainPlan(&backend, 0, false, 0, false, &stats) == null);
    const background = backend.pending_directory_closure.?;
    const visits = background.job.visits;
    const started = @import("antfly_platform").time.monotonicNs();
    for (0..64) |_| {
        try std.testing.expect(!try compactL0ToLimitScheduledWithinBudget(Backend, &backend, 0, 1, 1));
        try std.testing.expectEqual(background, backend.pending_directory_closure.?);
        try std.testing.expectEqual(visits, background.job.visits);
    }
    if (@import("builtin").mode == .ReleaseFast) std.debug.print("\nLSM policy isolation runs=5001 rejected_foreground_ns={d}\n", .{(@import("antfly_platform").time.monotonicNs() - started) / 64});
    try std.testing.expect(!try compactL0ToLimitScheduledWithinBudget(Backend, &backend, 0, 1, 0));
    try std.testing.expectEqual(@as(usize, 0), backend.compaction_stats.compactions);

    try std.testing.expect(try selectDomainPlan(&backend, 0, true, 0, false, &stats) == null);
    const foreground = backend.pending_l0_directory_closure.?;
    try std.testing.expect(foreground != background);
    // No lane may replace an owner while it is operating outside the mutex.
    backend.directory_planning_in_flight = true;
    try std.testing.expect(try selectDomainPlan(&backend, 0, true, 1, false, &stats) == null);
    try std.testing.expectEqual(foreground, backend.pending_l0_directory_closure.?);
    backend.directory_planning_in_flight = false;
    for (0..2) |_| try std.testing.expect(try selectDomainPlan(&backend, 0, false, 0, false, &stats) == null);
    try std.testing.expect(background.job.visits > visits);
    try std.testing.expect(foreground.job.visits > 0);
    // Changing a foreground policy retires only its own lane, off-lock.
    try std.testing.expect(try selectDomainPlan(&backend, 0, true, 1, false, &stats) == null);
    try std.testing.expect(backend.pending_l0_directory_closure == null);
    try std.testing.expectEqual(background, backend.pending_directory_closure.?);
    var selected = (try selectDomainPlanSynchronous(&backend, 0, false, 0, false, &stats)).?;
    try std.testing.expectEqual(@as(usize, 5001), selected.plan.source_len + selected.plan.target_len);
    selected.release(&backend);
    try std.testing.expect(backend.pending_directory_closure == null);

    // A request may disappear after its first slice. Ordinary background
    // maintenance must finish and release its continuation without that caller.
    try std.testing.expect(try selectDomainPlan(&backend, 0, true, 0, false, &stats) == null);
    var drained = false;
    for (0..1000) |_| {
        if (try selectDomainPlan(&backend, 1, false, 0, false, &stats)) |result| {
            result.release(&backend);
            drained = true;
            break;
        }
    }
    try std.testing.expect(drained);
    try std.testing.expect(backend.pending_l0_directory_closure == null);
    try std.testing.expect(backend.pending_directory_closure == null);
    // Background draining must still enforce ITS current admission budget,
    // even when the foreground job was discovered with an unlimited budget.
    try std.testing.expect(try selectDomainPlan(&backend, 0, true, 0, false, &stats) == null);
    for (0..1000) |_| {
        try std.testing.expect(!try compactDomainPlan(Backend, &backend, 0, false, true, 1, 1, false));
        if (backend.pending_l0_directory_closure == null) break;
    }
    try std.testing.expect(backend.pending_l0_directory_closure == null);
    try std.testing.expectEqual(@as(usize, 0), backend.compaction_stats.compactions);
    // Synchronous callers also drain the other lane across all of its slices.
    try std.testing.expect(try selectDomainPlan(&backend, 0, true, 0, false, &stats) == null);
    selected = (try selectDomainPlanSynchronous(&backend, 1, false, 0, false, &stats)).?;
    selected.release(&backend);
    try std.testing.expect(backend.pending_l0_directory_closure == null);
    // Leave both lanes queued: close must reclaim both, including reservations.
    try std.testing.expect(try selectDomainPlan(&backend, 0, false, 0, false, &stats) == null);
    try std.testing.expect(try selectDomainPlan(&backend, 0, true, 0, false, &stats) == null);
}

fn closureSlot(backend: anytype, l0_only: bool) *?*PendingDirectoryClosure {
    return if (l0_only) &backend.pending_l0_directory_closure else &backend.pending_directory_closure;
}

fn populateClosureDirectoryForTest(backend: anytype, count: usize) !void {
    try populateClosureDirectoryWithInputsForTest(backend, count, 5001);
}

fn populateClosureDirectoryWithInputsForTest(backend: anytype, count: usize, inputs: usize) !void {
    std.debug.assert(@import("builtin").is_test and inputs > 1 and count >= inputs);
    for (0..count) |i| {
        const lower = try backend.allocator.alloc(u8, 8);
        errdefer backend.allocator.free(lower);
        const upper = try backend.allocator.alloc(u8, 8);
        errdefer backend.allocator.free(upper);
        std.mem.writeInt(u64, lower[0..8], if (i == 0) 0 else i - 1, .big);
        std.mem.writeInt(u64, upper[0..8], if (i == 0) inputs - 2 else i - 1, .big);
        try backend.runs.append(backend.allocator, .{
            .id = i + 1,
            .level = if (i == 0) 0 else 1,
            .size_bytes = 1024,
            .path = null,
            .smallest_namespace_name = null,
            .smallest_key = lower,
            .largest_namespace_name = null,
            .largest_key = upper,
            .entry_count = 1,
            .bloom_filter = null,
            .state = .{},
        });
    }
    _ = try backend.planningDirectory();
}

test "compaction discovery receives turns under replenished foreground continuations" {
    const Backend = @import("../lsm_backend.zig").Backend;
    var backend = Backend.init(std.testing.allocator, .{ .level_target_runs_base = 4999, .level_target_bytes_base = 0 });
    defer backend.close();
    try populateClosureDirectoryForTest(&backend, 5001);
    const locked = runtime_mod.lockBackend(Backend, &backend);
    defer runtime_mod.unlockBackend(Backend, &backend, locked);
    var stats: CompactionSelectionStats = .{};
    for (0..8) |_| {
        try std.testing.expect(try selectDomainPlan(&backend, 0, true, 0, false, &stats) == null);
        const foreground = backend.pending_l0_directory_closure.?;
        const visits = foreground.job.visits;
        // No background job is queued. Its discovery turn must nevertheless
        // select the pressured L1, without consuming the foreground cursor.
        const selected = (try selectDomainPlan(&backend, 1, false, 0, false, &stats)).?;
        try std.testing.expectEqual(@as(u32, 1), selected.plan.source_level);
        try std.testing.expectEqual(visits, foreground.job.visits);
        selected.release(&backend);
        try std.testing.expect(backend.pending_directory_closure == null);
        try std.testing.expect(try selectDomainPlan(&backend, 1, false, 0, false, &stats) == null);
        try std.testing.expect(foreground.job.visits > visits);
        // Replenish on the next turn, as concurrent foreground arrivals can.
        backend.pending_l0_directory_closure = null;
        backend.retireClosurePlanning(foreground);
    }
    backend.options.level_target_runs_base = 1000000;
    try std.testing.expect(try selectDomainPlan(&backend, 0, true, 0, false, &stats) == null);
    const slices = backend.directory_planning_slices;
    try std.testing.expect(try selectDomainPlan(&backend, 1, false, 0, false, &stats) == null);
    try std.testing.expectEqual(slices + 1, backend.directory_planning_slices);
    try std.testing.expect(try selectDomainPlan(&backend, 1, false, 0, false, &stats) == null);
    try std.testing.expect(backend.pending_l0_directory_closure.?.job.visits > 0);
}

test "compaction scratch admission scales with selected inputs not unrelated runs" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const counts = if (@import("builtin").mode == .ReleaseFast) [_]usize{ 10000, 100000 } else [_]usize{ 12000, 24000 };
    var peaks: [2]u64 = undefined;
    for (counts, &peaks) |count, *peak| {
        var budgets = resource_manager_mod.Options.defaultBudgets();
        budgets[@intFromEnum(resource_manager_mod.Slice.lsm_table_builder_working_set)] = .{ .hard_limit_bytes = 4 * 1024 * 1024 };
        var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
        defer std.debug.assert(manager.sliceStats(.lsm_table_builder_working_set).used_bytes == 0);
        var backend = Backend.init(std.testing.allocator, .{ .level_target_runs_base = 1000000, .level_target_bytes_base = 0 });
        defer backend.close();
        try populateClosureDirectoryForTest(&backend, count);
        backend.options.resource_manager = &manager;
        const locked = runtime_mod.lockBackend(Backend, &backend);
        defer runtime_mod.unlockBackend(Backend, &backend, locked);
        var stats: CompactionSelectionStats = .{};
        const started = @import("antfly_platform").time.monotonicNs();
        try std.testing.expect(try selectDomainPlan(&backend, 0, false, 0, false, &stats) == null);
        try std.testing.expect(try selectDomainPlan(&backend, 0, true, 0, false, &stats) == null);
        for (0..1000) |_| {
            var done = true;
            for ([_]bool{ false, true }) |l0_only| {
                const pending = closureSlot(&backend, l0_only).*.?;
                if (!try pending.step(backend.allocator, std.math.maxInt(u64))) done = false;
            }
            if (done) break;
        }
        // Hold both completed discoveries before transfer so the peak is
        // independent of timing-dependent retirement interleavings.
        for ([_]bool{ false, true }) |l0_only| {
            const slot = closureSlot(&backend, l0_only);
            try std.testing.expect(slot.*.?.job.phase == .done);
            const result = try resumeClosureToSelectionForTest(&backend, slot);
            try std.testing.expectEqual(@as(usize, 5001), result.plan.source_len + result.plan.target_len);
            result.release(&backend);
        }
        try std.testing.expect(backend.pending_directory_closure == null and backend.pending_l0_directory_closure == null);
        // Include transient fast-path reservations, not just memory sampled
        // at continuation boundaries.
        peak.* = manager.sliceStats(.lsm_table_builder_working_set).peak_bytes;
        try std.testing.expect(peak.* < 2 * 1024 * 1024);
        if (@import("builtin").mode == .ReleaseFast) std.debug.print("\nLSM admitted closure runs={d} selected=5001 lanes=2 peak_bytes={d} elapsed_ns={d}\n", .{ count, peak.*, @import("antfly_platform").time.monotonicNs() - started });

        // Oversized discovery retires its arena before admitting the smaller
        // seed window, rather than retaining two attempts against the cap.
        const directory = try backend.planningDirectory();
        backend.pending_directory_closure = try PendingDirectoryClosure.create(&backend, &.{ directory.at(1), directory.at(2) }, 1500, false, 0, .{ .l0_limit = 0, .l0_only = false, .max_bytes = 1500, .allow_oversized = false });
        try std.testing.expect(try resumeDirectoryClosure(&backend, &backend.pending_directory_closure) == null);
        try std.testing.expectEqual(@as(?u64, 1500), backend.pending_directory_closure.?.restart_limit);
        try std.testing.expect(try resumeDirectoryClosure(&backend, &backend.pending_directory_closure) == null);
        try std.testing.expect(backend.pending_directory_closure.?.restart_limit == null);
        try std.testing.expectEqual(@as(usize, 1), backend.pending_directory_closure.?.job.count);
        const retried = try resumeClosureToSelectionForTest(&backend, &backend.pending_directory_closure);
        retried.release(&backend);

        // Denial during arena growth must remain an admission error and leave
        // all partially discovered scratch owned by sliced retirement/close.
        try std.testing.expect(try selectDomainPlan(&backend, 0, false, 0, false, &stats) == null);
        const remaining = 4 * 1024 * 1024 - manager.sliceStats(.lsm_table_builder_working_set).used_bytes;
        var blocker = try manager.reserve(.lsm_table_builder_working_set, remaining);
        defer blocker.release();
        var denied = false;
        for (0..1000) |_| {
            const selected = resumeDirectoryClosure(&backend, &backend.pending_directory_closure) catch |err| {
                try std.testing.expectEqual(error.ResourceBudgetExceeded, err);
                denied = true;
                break;
            };
            if (selected) |result| result.release(&backend);
            if (backend.pending_directory_closure == null) break;
        }
        try std.testing.expect(denied);
        try std.testing.expect(backend.pending_directory_closure == null);
    }
    // Extra unrelated runs may alter tree height/traversal, but not discovery
    // allocation: both inputs and both policy lanes are identical.
    try std.testing.expectEqual(peaks[0], peaks[1]);
}

fn resumeClosureToSelectionForTest(backend: anytype, slot: *?*PendingDirectoryClosure) !SelectedPlan {
    std.debug.assert(@import("builtin").is_test);
    for (0..4096) |_| {
        if (slot.* == null) break;
        if (try resumeDirectoryClosure(backend, slot)) |result| return result;
    }
    return error.MissingSelectedClosure;
}

test "compaction phase handoff bounds memory and preserves epoch validation" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const allocator = std.testing.allocator;
    const inputs = 20001;
    const cap = 3 * 1024 * 1024;
    for (0..3) |mode| {
        var budgets = resource_manager_mod.Options.defaultBudgets();
        budgets[@intFromEnum(resource_manager_mod.Slice.lsm_table_builder_working_set)] = .{ .hard_limit_bytes = cap };
        var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
        defer std.debug.assert(manager.sliceStats(.lsm_table_builder_working_set).used_bytes == 0);
        var backend = Backend.init(allocator, .{ .level_target_runs_base = 1000000, .level_target_bytes_base = 0 });
        defer backend.close();
        try populateClosureDirectoryWithInputsForTest(&backend, 30000, inputs);
        // Any persisted tombstone requires coverage validation, including when
        // there was no concurrent publication during discovery.
        const initial = try (try backend.planningDirectory()).fork(allocator);
        var tombstone = initial.at(1).run.*;
        tombstone.tombstone_count = 1;
        try initial.put(&backend, tombstone);
        backend.publishRunDirectory(initial);
        backend.options.resource_manager = &manager;
        const locked = runtime_mod.lockBackend(Backend, &backend);
        defer runtime_mod.unlockBackend(Backend, &backend, locked);
        const started = @import("antfly_platform").time.monotonicNs();
        var stats: CompactionSelectionStats = .{};
        try std.testing.expect(try selectDomainPlan(&backend, 0, false, 0, false, &stats) == null);
        const pending = backend.pending_directory_closure.?;
        for (0..4096) |_| {
            if (pending.phase == .reclaim_discovery) break;
            try std.testing.expect(try resumeDirectoryClosure(&backend, &backend.pending_directory_closure) == null);
        }
        try std.testing.expect(pending.phase == .reclaim_discovery);
        try std.testing.expect(pending.validation == null);
        const ranks = pending.selected.?.plan.run_indices.?.ptr;
        const used_before = manager.sliceStats(.lsm_table_builder_working_set).used_bytes;
        const validation_bytes = @sizeOf(DependencyValidation) + 8192 + inputs * 128;
        try std.testing.expect(used_before + validation_bytes > cap);
        if (manager.reserve(.lsm_table_builder_working_set, validation_bytes)) |lease| {
            var unexpected = lease;
            unexpected.release();
            return error.ExpectedPhaseOverlapAdmissionDenial;
        } else |err| try std.testing.expectEqual(error.ResourceBudgetExceeded, err);
        try std.testing.expect(!pending.reclaimDiscoveryUntil(allocator, 0, std.math.maxInt(u64)));
        try std.testing.expect(!pending.reclaimDiscoveryUntil(allocator, 1, 0));
        try std.testing.expectEqual(used_before, manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
        if (mode == 2) {
            // Leave partial cleanup to close, including selected array credit.
            try std.testing.expect(!pending.reclaimDiscoveryUntil(allocator, 1, std.math.maxInt(u64)));
            continue;
        }
        // Publish either an unrelated edit or a replacement of an input while
        // its discovery scratch is being retired. Handles/epoch stay pinned.
        const changed = try (try backend.planningDirectory()).fork(allocator);
        var replacement = changed.at(if (mode == 0) 29999 else 1).run.*;
        replacement.gc_requested = true;
        try changed.put(&backend, replacement);
        backend.invalidateReadVersion();
        backend.publishRunDirectory(changed);
        for (0..4096) |_| {
            if (pending.phase == .validate) break;
            try std.testing.expect(try resumeDirectoryClosure(&backend, &backend.pending_directory_closure) == null);
            try std.testing.expect(pending.validation == null);
        }
        try std.testing.expect(pending.phase == .validate);
        try std.testing.expectEqual(@as(u64, 0), pending.scratch.?.live_bytes);
        const retained = manager.sliceStats(.lsm_table_builder_working_set).used_bytes;
        try std.testing.expect(retained + validation_bytes <= cap);
        var accepted = false;
        for (0..4096) |_| {
            if (backend.pending_directory_closure == null) break;
            if (try resumeDirectoryClosure(&backend, &backend.pending_directory_closure)) |selected| {
                try std.testing.expectEqual(@as(usize, inputs), selected.plan.input_handles.?.len);
                try std.testing.expectEqual(ranks, selected.plan.run_indices.?.ptr);
                try std.testing.expect(manager.sliceStats(.lsm_table_builder_working_set).used_bytes <= retained + @sizeOf(DependencyValidation) + 8192);
                selected.release(&backend);
                accepted = true;
                break;
            }
        }
        try std.testing.expectEqual(mode == 0, accepted);
        try std.testing.expect(backend.pending_directory_closure == null);
        const peak = manager.sliceStats(.lsm_table_builder_working_set).peak_bytes;
        try std.testing.expect(peak <= cap);
        if (@import("builtin").mode == .ReleaseFast and mode == 0) std.debug.print("\nLSM phase handoff inputs={d} retained_bytes={d} previous_overlap_bytes={d} peak_bytes={d} elapsed_ns={d}\n", .{ inputs, retained, used_before + validation_bytes, peak, @import("antfly_platform").time.monotonicNs() - started });
    }
}

/// An exceptional broad ordinary closure belongs to maintenance, not to the
/// stack of whichever request first noticed pressure. Keep its epoch and
/// scratch reservation until the cursor completes or is discarded.
pub const PendingDirectoryClosure = struct {
    policy: PlanningPolicy,
    directory: *Directory,
    job: ClosureJob,
    phase: enum { discover, reclaim_discovery, validate } = .discover,
    restart_limit: ?u64 = null,
    retired_next: ?*@This() = null,
    seeds: []Directory.Handle,
    seed_len: usize,
    max_bytes: u64,
    allow_oversized: bool,
    overlap_threshold: usize,
    reservation: ?resource_manager_mod.Reservation = null,
    // Stable address: the arena borrows this allocator through sliced cleanup.
    scratch: ?resource_manager_mod.BudgetedAllocator = null,
    selected: ?SelectedPlan = null,
    validation: ?DependencyValidation = null,

    fn create(backend: anytype, seeds: []const Directory.Handle, max_bytes: u64, allow_oversized: bool, overlap_threshold: usize, policy: PlanningPolicy) !*@This() {
        const allocator = backend.allocator;
        var reservation: ?resource_manager_mod.Reservation = null;
        errdefer if (reservation) |*lease| lease.release();
        const current = try backend.planningDirectory();
        if (backend.options.resource_manager) |manager| reservation = try manager.reserve(.lsm_table_builder_working_set, @sizeOf(@This()) + @sizeOf(Directory) + seeds.len * @sizeOf(Directory.Handle));
        const directory = try current.fork(allocator);
        errdefer directory.destroy(allocator);
        const owned = try allocator.dupe(Directory.Handle, seeds);
        errdefer allocator.free(owned);
        const self = try allocator.create(@This());
        errdefer allocator.destroy(self);
        self.* = .{
            .policy = policy,
            .directory = directory,
            .job = undefined,
            .seeds = owned,
            .seed_len = owned.len,
            .max_bytes = max_bytes,
            .allow_oversized = allow_oversized,
            .overlap_threshold = overlap_threshold,
            .reservation = reservation,
            .scratch = if (backend.options.resource_manager) |manager| .init(manager, .lsm_table_builder_working_set, allocator, 1) else null,
        };
        errdefer if (self.scratch) |*scratch| scratch.deinit();
        self.job = try self.initJob(allocator, max_bytes);
        return self;
    }

    fn initJob(self: *@This(), allocator: std.mem.Allocator, limit: u64) !ClosureJob {
        const scratch_allocator = if (self.scratch) |*scratch| scratch.allocator() else allocator;
        var job = ClosureJob.init(scratch_allocator, self.directory, self.seeds[0..self.seed_len], limit, false) catch |err| return self.allocationError(err);
        job.output_manager = if (self.scratch) |*scratch| scratch.reservation.manager else null;
        return job;
    }

    fn allocationError(self: *@This(), err: anyerror) anyerror {
        if (err == error.OutOfMemory) if (self.scratch) |*scratch| if (scratch.denied()) return error.ResourceBudgetExceeded;
        return err;
    }

    pub fn cleanupStep(self: *@This(), allocator: std.mem.Allocator, credits: *usize) bool {
        if (self.validation) |*validation| if (!validation.cleanupStep(allocator, credits)) return false;
        if (self.selected) |*selected| {
            if (!selected.deinitStep(allocator, credits)) return false;
            self.selected = null;
        }
        return self.job.deinitStep(allocator, credits);
    }

    fn reclaimDiscoveryUntil(self: *@This(), allocator: std.mem.Allocator, credits_arg: usize, deadline: u64) bool {
        var credits = credits_arg;
        while (credits != 0 and @import("antfly_platform").time.monotonicNs() < deadline) {
            var quantum: usize = @min(credits, 64);
            const before = quantum;
            const done = self.job.deinitStep(allocator, &quantum);
            credits -= before - quantum;
            if (done) return true;
        }
        return false;
    }

    fn step(self: *@This(), allocator: std.mem.Allocator, deadline: u64) !bool {
        if (self.restart_limit) |limit| {
            if (!self.reclaimDiscoveryUntil(allocator, 2048, deadline)) return false;
            // Release the superseded arena before admitting replacement
            // seeds. A tight budget must not require both attempts to fit.
            self.job = try self.initJob(allocator, limit);
            self.restart_limit = null;
            return false;
        }
        return self.job.stepUntil(allocator, 2048, deadline) catch |err| return self.allocationError(err);
    }

    pub fn destroy(self: *@This(), backend: anytype) void {
        const allocator = backend.allocator;
        var credits: usize = std.math.maxInt(usize);
        std.debug.assert(self.cleanupStep(allocator, &credits));
        if (self.validation) |*validation| validation.deinit(backend);
        backend.retireCheckpointDirectory(self.directory);
        allocator.free(self.seeds);
        if (self.scratch) |*scratch| scratch.deinit();
        if (self.reservation) |*lease| lease.release();
        allocator.destroy(self);
    }
};

fn resumeDirectoryClosure(backend: anytype, slot: *?*PendingDirectoryClosure) !?SelectedPlan {
    const pending = slot.*.?;
    if (backend.directory_planning_in_flight) return null;
    backend.directory_planning_in_flight = true;
    defer backend.directory_planning_in_flight = false;
    backend.retainReaderKind(.compaction);
    defer backend.releaseReaderKind(.compaction);
    const BackendType = @TypeOf(backend.*);
    if (pending.phase != .discover) return resumeClosureValidation(backend, pending, slot);
    if (backend.manifestCoordinationIo()) |io| try io.checkCancel();
    runtime_mod.unlockBackend(BackendType, backend, true);
    const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
    const advanced = pending.step(backend.allocator, deadline);
    _ = runtime_mod.lockBackend(BackendType, backend);
    backend.directory_planning_slices +|= 1;
    var destroy = false;
    defer if (destroy) {
        slot.* = null;
        backend.retireClosurePlanning(pending);
    };
    const done = advanced catch |err| {
        destroy = true;
        return err;
    };
    if (!done) return null;
    if (pending.job.phase == .oversized) {
        const retry = pending.seed_len > 1 or (pending.allow_oversized and pending.job.max_bytes != 0);
        if (!retry) {
            destroy = true;
            return null;
        }
        pending.seed_len = @max(@as(usize, 1), pending.seed_len / 2);
        const limit = if (pending.seed_len == 1 and pending.job.max_bytes != 0 and pending.allow_oversized) 0 else pending.max_bytes;
        pending.restart_limit = limit;
        return null;
    }
    if (pending.job.source_len < pending.overlap_threshold) {
        destroy = true;
        return null;
    }
    const handles = pending.job.handles.?;
    var selected = SelectedPlan{ .plan = .{
        .source_level = pending.job.source_level,
        .source_start = 0,
        .source_len = pending.job.source_len,
        .target_start = pending.job.source_len,
        .target_len = handles.len - pending.job.source_len,
        .output_level = pending.job.source_level +| 1,
        .run_indices = pending.job.indices,
        .input_handles = handles,
        .partition_key = backend.options.run_partition_key,
        .oversized_indivisible = pending.policy.max_bytes != 0 and pending.job.max_bytes == 0,
    } };
    pending.job.handles = null;
    pending.job.indices = null;
    selected.reservation = pending.job.output_reservation;
    pending.job.output_reservation = null;
    pending.selected = selected;
    pending.phase = .reclaim_discovery;
    // Handoff is a separate maintenance quantum. Do not stack reclamation or
    // validation admission onto the final discovery/emission slice.
    return null;
}

fn resumeClosureValidation(backend: anytype, pending: *PendingDirectoryClosure, slot: *?*PendingDirectoryClosure) !?SelectedPlan {
    var retire = false;
    defer if (retire) {
        slot.* = null;
        backend.retireClosurePlanning(pending);
    };
    errdefer retire = true;
    if (pending.phase == .reclaim_discovery) {
        if (backend.manifestCoordinationIo()) |io| try io.checkCancel();
        runtime_mod.unlockBackend(@TypeOf(backend.*), backend, true);
        const done = pending.reclaimDiscoveryUntil(backend.allocator, 2048, @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms);
        _ = runtime_mod.lockBackend(@TypeOf(backend.*), backend);
        backend.directory_planning_slices +|= 1;
        if (done) {
            // Only the selected handles/arrays and pinned epoch cross phases.
            // The allocator stays address-stable until owner destruction, but
            // its arena has returned all credit before the next admission.
            if (pending.scratch) |*scratch| std.debug.assert(scratch.live_bytes == 0);
            const seed_bytes = pending.seeds.len * @sizeOf(Directory.Handle);
            backend.allocator.free(pending.seeds);
            pending.seeds = &.{};
            pending.seed_len = 0;
            if (pending.reservation) |*lease| lease.shrink(seed_bytes);
            pending.phase = .validate;
        }
        return null;
    }
    std.debug.assert(pending.phase == .validate);
    if (pending.validation == null) {
        const current = try backend.planningDirectory();
        if (current.tree.root == pending.directory.tree.root and current.tombstoneRunCount() == 0) {
            // Discovery already certified these identities and dependencies.
            // Without deletes there is no coverage work to repeat.
            retire = true;
            var selected = pending.selected.?;
            pending.selected = null;
            selected.plan.complete_coverage = false;
            selected.plan.validated_generation = backend.run_directory_generation;
            return selected;
        }
        pending.validation = try DependencyValidation.init(backend, pending.selected.?.plan);
        // Validation rewrites these ranks in place from stable handles. Keep
        // their existing output credit rather than allocating a second array.
        // This is the exclusively owned mutable buffer emitted by ClosureJob;
        // CompactionPlan exposes only a const view to execution consumers.
        pending.validation.?.job.indices = @constCast(pending.selected.?.plan.run_indices.?);
        pending.selected.?.plan.run_indices = null;
    }
    const result = try pending.validation.?.advanceLocked(backend);
    backend.directory_planning_slices +|= 1;
    if (result == .pending) return null;
    retire = true;
    if (result == .invalid) return null;
    var selected = pending.selected.?;
    pending.selected = null;
    selected.plan.run_indices = pending.validation.?.takeIndices();
    selected.plan.complete_coverage = pending.validation.?.job.covered;
    selected.plan.validated_generation = backend.run_directory_generation;
    return selected;
}

fn sameDomain(a: Run, b: Run, partition: *const fn ([]const u8) []const u8) bool {
    return state_mod.compareNamespace(.{ .name = a.smallest_namespace_name }, .{ .name = b.smallest_namespace_name }) == .eq and
        std.mem.eql(u8, partition(a.smallest_key), partition(b.smallest_key));
}

fn pureDomain(run: Run, partition: *const fn ([]const u8) []const u8) bool {
    return state_mod.compareNamespace(.{ .name = run.smallest_namespace_name }, .{ .name = run.largest_namespace_name }) == .eq and
        std.mem.eql(u8, partition(run.smallest_key), partition(run.largest_key));
}

/// Planning metadata for one published run version. Run descriptors borrow
/// the active version; callers hold the backend mutex, and publication drops
/// this index before another planner can observe the new run set.
pub const DomainIndex = struct {
    order: []usize,
    runs: []Run,
    ends: []usize,
    levels: []Level,
    gc_order: []usize,
    gc_ends: []usize,
    mixed: bool,
    const Level = struct { number: u32, count: usize, bytes: u64, target_runs: usize, target_bytes: u64 };

    pub fn buildMemoryBound(run_count: usize) u64 {
        return @sizeOf(DomainIndex) + @as(u64, @intCast(run_count)) *
            (@sizeOf(Run) + 24 * @sizeOf(usize) + 3 * @sizeOf(Level));
    }

    pub fn create(backend: anytype) !*DomainIndex {
        const allocator = backend.allocator;
        const source = try run_store.project(backend, allocator);
        defer allocator.free(source);
        const partition = backend.options.run_partition_key orelse wholeKeyspace;
        const self = try allocator.create(DomainIndex);
        errdefer allocator.destroy(self);
        const directory: ?*@import("run_directory.zig").Directory = if (comptime @hasField(@TypeOf(backend.*), "planning_directory"))
            backend.planning_directory
        else if (comptime @hasField(@TypeOf(backend.*), "run_directory"))
            (if (!backend.run_directory_dirty) backend.run_directory else null)
        else
            null;
        const maintained = if (directory) |root| try root.planningOrder(allocator) else null;
        errdefer if (maintained) |orders| allocator.free(orders.bounds);
        const order = if (maintained) |orders| orders.domain else try allocator.alloc(usize, source.len);
        errdefer allocator.free(order);
        if (maintained == null) for (order, 0..) |*slot, i| {
            slot.* = i;
        };
        const Context = struct {
            runs: []const Run,
            partition: *const fn ([]const u8) []const u8,
            fn less(ctx: @This(), a: usize, b: usize) bool {
                const lhs = ctx.runs[a];
                const rhs = ctx.runs[b];
                const ns = state_mod.compareNamespace(.{ .name = lhs.smallest_namespace_name }, .{ .name = rhs.smallest_namespace_name });
                if (ns != .eq) return ns == .lt;
                const domain = std.mem.order(u8, ctx.partition(lhs.smallest_key), ctx.partition(rhs.smallest_key));
                return if (domain == .eq) a < b else domain == .lt;
            }
        };
        if (maintained == null) std.mem.sort(usize, order, Context{ .runs = source, .partition = partition }, Context.less);
        const projected = try allocator.alloc(Run, source.len);
        errdefer allocator.free(projected);
        var ends: std.ArrayListUnmanaged(usize) = .empty;
        defer ends.deinit(allocator);
        var mixed = false;
        for (order, 0..) |index, i| {
            projected[i] = source[index];
            mixed = mixed or !pureDomain(source[index], partition);
            if (i != 0 and !sameDomain(projected[i - 1], projected[i], partition)) try ends.append(allocator, i);
        }
        if (source.len != 0) try ends.append(allocator, source.len);
        var levels: std.ArrayListUnmanaged(Level) = .empty;
        defer levels.deinit(allocator);
        var i: usize = 0;
        if (directory) |root| {
            for (0..root.levelCount()) |rank| {
                const aggregate = root.levelAt(rank);
                try levels.append(allocator, .{
                    .number = aggregate.level,
                    .count = aggregate.count,
                    .bytes = aggregate.bytes,
                    .target_runs = levelRunTarget(aggregate.level, backend.options.level_target_runs_base, backend.options.level_target_runs_multiplier),
                    .target_bytes = levelByteTargetForTotals(root.total_run_bytes, root.maxLevel(), aggregate.level, backend.options.level_target_bytes_base, backend.options.level_target_bytes_multiplier),
                });
            }
        } else while (i < source.len) {
            const level = source[i].level;
            const start = i;
            var bytes: u64 = 0;
            while (i < source.len and source[i].level == level) : (i += 1) bytes +|= source[i].size_bytes;
            try levels.append(allocator, .{
                .number = level,
                .count = i - start,
                .bytes = bytes,
                .target_runs = levelRunTarget(level, backend.options.level_target_runs_base, backend.options.level_target_runs_multiplier),
                .target_bytes = levelByteTargetForRuns(source, level, backend.options.level_target_bytes_base, backend.options.level_target_bytes_multiplier),
            });
        }
        const owned_ends = try ends.toOwnedSlice(allocator);
        errdefer allocator.free(owned_ends);
        const gc_order = if (maintained) |orders| orders.bounds else try allocator.dupe(usize, order);
        errdefer if (maintained == null) allocator.free(gc_order);
        if (maintained == null) std.mem.sort(usize, gc_order, source, struct {
            fn less(runs: []const Run, a: usize, b: usize) bool {
                return compareRunBound(runs[a].smallest_namespace_name, runs[a].smallest_key, runs[b].smallest_namespace_name, runs[b].smallest_key) == .lt;
            }
        }.less);
        var gc_ends: std.ArrayListUnmanaged(usize) = .empty;
        defer gc_ends.deinit(allocator);
        const component = try allocator.alloc(usize, source.len);
        defer allocator.free(component);
        var start: usize = 0;
        while (start < gc_order.len) {
            var largest = source[gc_order[start]];
            var end = start + 1;
            while (end < gc_order.len) : (end += 1) {
                const next = source[gc_order[end]];
                if (compareRunBound(next.smallest_namespace_name, next.smallest_key, largest.largest_namespace_name, largest.largest_key) == .gt) break;
                if (compareRunBound(next.largest_namespace_name, next.largest_key, largest.largest_namespace_name, largest.largest_key) == .gt) largest = next;
            }
            for (gc_order[start..end]) |run_index| component[run_index] = gc_ends.items.len;
            try gc_ends.append(allocator, end);
            start = end;
        }
        const owned_gc_ends = try gc_ends.toOwnedSlice(allocator);
        errdefer allocator.free(owned_gc_ends);
        // Stable bucket scatter restores read precedence without sorting each
        // connected component (one hot overlap may contain the entire store).
        const write_positions = try allocator.alloc(usize, owned_gc_ends.len);
        defer allocator.free(write_positions);
        for (write_positions, 0..) |*position, group| position.* = if (group == 0) 0 else owned_gc_ends[group - 1];
        for (component, 0..) |group, run_index| {
            gc_order[write_positions[group]] = run_index;
            write_positions[group] += 1;
        }
        self.* = .{ .order = order, .runs = projected, .ends = owned_ends, .levels = try levels.toOwnedSlice(allocator), .mixed = mixed, .gc_order = gc_order, .gc_ends = owned_gc_ends };
        return self;
    }

    pub fn destroy(self: *DomainIndex, allocator: std.mem.Allocator) void {
        allocator.free(self.order);
        allocator.free(self.runs);
        allocator.free(self.ends);
        allocator.free(self.levels);
        allocator.free(self.gc_order);
        allocator.free(self.gc_ends);
        allocator.destroy(self);
    }

    pub fn memoryBytes(self: *const DomainIndex) u64 {
        return @sizeOf(DomainIndex) + self.order.len * @sizeOf(usize) + self.runs.len * @sizeOf(Run) + self.ends.len * @sizeOf(usize) + self.levels.len * @sizeOf(Level) + self.gc_order.len * @sizeOf(usize) + self.gc_ends.len * @sizeOf(usize);
    }

    fn lowerPressure(self: *const DomainIndex, local: []const Run, max_bytes: u64, allow_oversized: bool, stats: *CompactionSelectionStats) ?ScoredCompactionPlan {
        var best: ?ScoredCompactionPlan = null;
        var start: usize = 0;
        for (self.levels) |level| {
            while (start < local.len and local[start].level < level.number) : (start += 1) {}
            var end = start;
            while (end < local.len and local[end].level == level.number) : (end += 1) {}
            defer start = end;
            if (level.number == 0 or start == end) continue;
            const need_runs = level.count > level.target_runs;
            const need_bytes = level.target_bytes > 0 and level.bytes > level.target_bytes;
            if (!need_runs and !need_bytes) continue;
            const priority = @max(if (need_runs) normalizedPressurePriority(level.count, level.target_runs) else 0, if (need_bytes) normalizedPressurePriority(level.bytes, level.target_bytes) else 0);
            maybeAdoptBest(&best, selectLowestOverlapWindowCandidate(local, level.number, start, end - start, if (need_runs) @min(end - start, level.count - level.target_runs) else 1, if (need_bytes) @min(sumRunBytes(local[start..end]), level.bytes - level.target_bytes) else 0, max_bytes, allow_oversized, stats, priority));
        }
        return best;
    }
};

fn wholeKeyspace(_: []const u8) []const u8 {
    return "";
}

const GcCandidate = struct { indices: []const usize, level: u32 };

fn gcComponentEligible(backend: anytype, indices: []const usize) bool {
    var tombstones: u64 = 0;
    var entries: u64 = 0;
    var requested = false;
    for (indices) |i| {
        const run = run_store.at(backend, i).*;
        const deletes = run.tombstone_count orelse 0;
        tombstones +|= deletes;
        entries = @max(entries, run.entry_count);
        if (deletes != 0) requested = requested or run.gc_requested or tombstoneAgeDue(backend, run);
    }
    const percent = if (comptime @hasField(@TypeOf(backend.options), "tombstone_gc_min_percent")) @min(@as(u8, 100), backend.options.tombstone_gc_min_percent) else 50;
    return tombstones != 0 and (requested or @as(u128, tombstones) * 100 >= @as(u128, entries) * percent);
}

/// Persist the collection objective on every delete-bearing input, not just
/// the next admitted window. Output runs inherit it until their deletes have
/// actually been collected, so splits, ordinary compaction and restart cannot
/// silently revoke a component's eligibility.
fn requestEligibleGcComponents(backend: anytype, index: *const DomainIndex) !void {
    var start: usize = 0;
    var changed = false;
    for (index.gc_ends) |end| {
        defer start = end;
        const indices = index.gc_order[start..end];
        if (!gcComponentEligible(backend, indices)) continue;
        if (comptime @hasDecl(@TypeOf(backend.*), "requestRunGcIntent")) {
            try backend.requestRunGcIntent(indices);
            continue;
        }
        for (indices) |i| {
            const run = run_store.at(backend, i);
            if ((run.tombstone_count orelse 0) == 0 or run.gc_requested) continue;
            run.gc_requested = true;
            changed = true;
        }
    }
    if (changed) if (comptime @hasDecl(@TypeOf(backend.*), "markManifestDirty")) backend.markManifestDirty();
}

fn tombstoneGcCandidate(backend: anytype, index: *const DomainIndex, max_bytes: u64) ?GcCandidate {
    var start: usize = 0;
    var best: []const usize = &.{};
    var best_bytes: u64 = std.math.maxInt(u64);
    var best_level: u32 = 1;
    for (index.gc_ends) |end| {
        defer start = end;
        const indices = index.gc_order[start..end];
        var bytes: u64 = 0;
        var level: u32 = 1;
        for (indices) |i| {
            const run = run_store.at(backend, i).*;
            bytes +|= run.size_bytes;
            level = @max(level, run.level);
        }
        if (bytes >= best_bytes or !gcComponentEligible(backend, indices)) continue;
        if (max_bytes != 0 and bytes > max_bytes) continue;
        best = indices;
        best_bytes = bytes;
        best_level = level;
    }
    if (best.len == 0) return null;
    // Larger closures advance through bounded level jobs or source splits.
    return .{ .indices = best, .level = best_level };
}

pub fn hasTombstoneGcDebt(backend: anytype) bool {
    if (comptime @hasField(@TypeOf(backend.*), "run_directory_dirty")) {
        if (!backend.run_directory_dirty) if (backend.run_directory) |directory| {
            if (directory.tombstoneRunCount() == 0) return false;
        };
    }
    if (comptime @hasField(@TypeOf(backend.*), "gc_debt_cache")) {
        if (backend.gc_debt_cache) |cache| {
            const now = gcNowNs();
            if (cache.generation == backend.run_directory_generation and cache.age == backend.options.tombstone_gc_max_age_ns and cache.percent == backend.options.tombstone_gc_min_percent and now >= cache.checked_at and now < cache.valid_until) return cache.has_debt;
        }
        // Unknown debt schedules one bounded/off-lock discovery, not a global
        // projection under the maintenance scoring lock.
        return true;
    }
    const index = backend.domainIndex() catch return true;
    // Debt is independent of temporary admission denial. Below the garbage
    // fraction there is no standalone job, so the scheduler can become idle.
    return tombstoneGcCandidate(backend, index, 0) != null;
}

pub const GcDebtCache = struct {
    generation: u64,
    age: u64,
    percent: u8,
    checked_at: u64,
    valid_until: u64 = std.math.maxInt(u64),
    has_debt: bool = false,
};

pub const PendingGc = struct {
    directory: *Directory,
    job: @import("gc_job.zig").Job,
    cache: GcDebtCache,
    reservation: ?resource_manager_mod.Reservation = null,
    retired_next: ?*PendingGc = null,
    selected: ?SelectedPlan = null,
    intent: ?Intent = null,
    validation: ?DependencyValidation = null,
    scratch: ?resource_manager_mod.BudgetedAllocator = null,
    phase: enum { discover, reclaim_discovery, validate } = .discover,
    objective_bounds: struct { lower_ns: ?[]const u8, lower: []const u8, upper_ns: ?[]const u8, upper: []const u8 } = undefined,

    const Intent = struct {
        base: *Directory,
        directory: ?*Directory,
        store: ?*run_store.Store,
        index: usize = 0,
        refreshed: usize = 0,
        wire: u64,
        header_bytes: u64,
        reservation: ?resource_manager_mod.Reservation = null,
        rebase: ?Rebase = null,

        const Rebase = struct {
            directory: *Directory,
            store: *run_store.Store,
            changes: Directory.ChangeCursor,
            pending_change: ?Directory.ChangeCursor.Change = null,
        };

        fn beginRebase(self: *Intent, backend: anytype) !void {
            const current = try backend.planningDirectory();
            const directory = try current.fork(backend.allocator);
            errdefer directory.destroy(backend.allocator);
            const store = try backend.allocator.create(run_store.Store);
            store.* = backend.runs.fork();
            self.rebase = .{ .directory = directory, .store = store, .changes = .init(self.base, directory) };
        }

        fn finishRebase(self: *Intent, backend: anytype) void {
            const rebase = self.rebase.?;
            std.debug.assert(rebase.changes.done());
            backend.retireCheckpointDirectory(self.base);
            self.base = rebase.directory;
            backend.retireRunStore(rebase.store);
            self.rebase = null;
        }

        fn stepRebase(self: *Intent, backend: anytype, component: anytype, selected: *const SelectedPlan, credits_arg: usize, deadline: u64) !bool {
            var credits = credits_arg;
            const rebase = &self.rebase.?;
            const objectives = selected.gc_objective_handles orelse selected.plan.input_handles.?;
            const first = objectives[0].run;
            const first_visibility = if (first.visibility_id == 0) first.id else first.visibility_id;
            while (credits != 0) {
                const change = rebase.pending_change orelse rebase.changes.next(&credits) orelse return rebase.changes.done();
                if (@import("antfly_platform").time.monotonicNs() >= deadline) {
                    rebase.pending_change = change;
                    return false;
                }
                // Recognize cursor exhaustion even when the last allocating
                // edit consumed the time quantum. Otherwise a one-edit delta
                // requires a second turn just to finish its cursor, and one
                // new write per turn can keep intent forever one epoch behind.
                rebase.pending_change = null;
                const run = change.run;
                if (Directory.containsReadOrdered(objectives, run)) return error.CompactionPlanningStale;
                const visibility = if (run.visibility_id == 0) run.id else run.visibility_id;
                const newer = run.level < first.level or
                    (first.level == 0 and run.level == 0 and visibility > first_visibility);
                const overlap = compareRunBound(run.largest_namespace_name, run.largest_key, component.lower_ns, component.lower) != .lt and
                    compareRunBound(run.smallest_namespace_name, run.smallest_key, component.upper_ns, component.upper) != .gt;
                if (overlap and !newer) return error.CompactionPlanningStale;
                // Admit the changed paths before cloning them. The original
                // broad intent is retained; only concurrent deltas add credit.
                if (self.reservation) |*lease| {
                    const height = @max(self.directory.?.tree.root.?.height, rebase.directory.tree.root.?.height);
                    const names = run.smallest_key.len + run.largest_key.len +
                        (if (run.path) |path| path.len else 0) +
                        (if (run.smallest_namespace_name) |name| name.len else 0) +
                        (if (run.largest_namespace_name) |name| name.len else 0);
                    try lease.growBoundedOversized(4096 + @as(u64, height) * 8192 + names +
                        (if (run.state) |*state| state.estimatedMemoryBytes() else 0), 1);
                }
                if (change.kind == .remove) {
                    try self.store.?.remove(backend.allocator, run);
                    try self.directory.?.remove(backend.allocator, run);
                } else {
                    const source = rebase.store.find(run) orelse return error.CompactionPlanningStale;
                    const revision = run_store.Store.revision(source, run.*);
                    try self.store.?.stageRevision(backend.allocator, revision);
                    self.store.?.adopt(&revision);
                    try self.directory.?.put(backend, revision);
                }
            }
            return rebase.changes.done();
        }

        fn create(backend: anytype, pending: *PendingGc) !Intent {
            const allocator = backend.allocator;
            const current = try backend.planningDirectory();
            const changes: u64 = pending.job.intent_runs;
            const height: u64 = @max(
                @max(if (current.tree.root) |root| root.height else 1, if (current.ids.root) |root| root.height else 1),
                @max(if (current.bounds.root) |root| root.height else 1, if (current.levels.root) |root| root.height else 1),
                if (current.ends.root) |root| root.height else 1,
                if (backend.runs.tree.root) |root| root.height else 1,
            );
            const nodes = @min(current.count(), changes *| height);
            const node_bytes = @sizeOf(@TypeOf(current.tree).Node) + @sizeOf(@TypeOf(current.ids).Node) + @sizeOf(@TypeOf(current.bounds).Node) + @sizeOf(@TypeOf(current.ends).Node) + @sizeOf(@TypeOf(current.levels).Node) + @sizeOf(run_store.Store.Tree.Node);
            const scratch = 8192 +| (nodes +| height * 8 +| 64) *| node_bytes +| changes *| (2 * @sizeOf(Run) + 256) +| pending.job.intent_data_bytes;
            var reservation: ?resource_manager_mod.Reservation = null;
            errdefer if (reservation) |*lease| lease.release();
            if (backend.options.resource_manager) |manager| reservation = try manager.reserve(.lsm_table_builder_working_set, scratch);
            const wire = try backend.reserveGcMetadata(if (changes != 0) pending.job.intent_wire_bytes else 0);
            errdefer backend.manifest_reserved_mutation_bytes -= wire;
            const base = try current.fork(allocator);
            errdefer base.destroy(allocator);
            const directory = try current.fork(allocator);
            errdefer directory.destroy(allocator);
            const store = try allocator.create(run_store.Store);
            store.* = backend.runs.fork();
            return .{ .base = base, .directory = directory, .store = store, .wire = wire, .reservation = reservation, .header_bytes = 3 * @sizeOf(Directory) + 2 * @sizeOf(run_store.Store) + (2 * @bitSizeOf(usize) * 8 + 128) * 6 * @sizeOf(usize) };
        }

        fn step(self: *Intent, backend: anytype, selected: *SelectedPlan, credits_arg: usize, deadline: u64) !bool {
            var credits = credits_arg;
            const objectives = selected.gc_objective_handles orelse selected.plan.input_handles.?;
            while (credits != 0 and @import("antfly_platform").time.monotonicNs() < deadline) {
                credits -= 1;
                if (self.index < objectives.len) {
                    const handle = objectives[self.index];
                    if ((handle.run.tombstone_count orelse 0) != 0 and !handle.run.gc_requested) {
                        const source = self.store.?.find(handle.run) orelse return error.CompactionPlanningStale;
                        var revision = run_store.Store.revision(source, handle.run.*);
                        revision.gc_requested = true;
                        try self.store.?.stageRevision(backend.allocator, revision);
                        self.store.?.adopt(&revision);
                        try self.directory.?.put(backend, revision);
                    }
                    self.index += 1;
                    continue;
                }
                const handles = selected.plan.input_handles orelse return true;
                if (self.refreshed == handles.len) return true;
                const old = handles[self.refreshed];
                const run = self.directory.?.byId(old.run.id) orelse return error.CompactionPlanningStale;
                const rank = self.directory.?.rankOf(run).?;
                @constCast(selected.plan.run_indices.?)[self.refreshed] = rank;
                @constCast(handles)[self.refreshed] = self.directory.?.at(rank).retain();
                old.release(backend.allocator);
                self.refreshed += 1;
            }
            return false;
        }

        fn discard(self: *Intent, backend: anytype) void {
            if (self.rebase) |rebase| {
                backend.retireCheckpointDirectory(rebase.directory);
                backend.retireRunStore(rebase.store);
            }
            if (self.store) |store| backend.retireRunStore(store);
            if (self.directory) |directory| backend.retireCheckpointDirectory(directory);
            backend.retireCheckpointDirectory(self.base);
            backend.manifest_reserved_mutation_bytes -= self.wire;
            if (self.reservation) |*lease| lease.release();
        }
    };

    pub fn cleanupStep(self: *@This(), allocator: std.mem.Allocator, credits: *usize) bool {
        if (self.validation) |*validation| if (!validation.cleanupStep(allocator, credits)) return false;
        if (self.selected) |*selected| {
            if (!selected.deinitStep(allocator, credits)) return false;
            self.selected = null;
        }
        return self.job.deinitStep(allocator, credits);
    }

    pub fn accountedMemoryBytes(self: *const @This(), pass: u64) u64 {
        // Candidate roots share the already-reachable atomic accounts. Their
        // mutable tree headers must not be inspected by a concurrent writer.
        return self.directory.accountedMemoryBytes(pass) + if (self.intent) |*intent| intent.header_bytes else @as(u64, 0);
    }

    pub fn discardIntent(self: *@This(), backend: anytype) void {
        if (self.intent) |*intent| intent.discard(backend);
        self.intent = null;
    }

    pub fn destroy(self: *@This(), backend: anytype) void {
        self.discardIntent(backend);
        if (self.validation) |*validation| validation.deinit(backend);
        if (self.selected) |selected| selected.deinit(backend.allocator);
        self.job.deinit(backend.allocator);
        if (self.scratch) |*scratch| scratch.deinit();
        backend.retireCheckpointDirectory(self.directory);
        if (self.reservation) |*lease| lease.release();
        backend.allocator.destroy(self);
    }
    fn take(self: *@This(), allocator: std.mem.Allocator) !?SelectedPlan {
        if (!self.job.eligible) return null;
        const component = &self.job.component.?;
        self.objective_bounds = .{ .lower_ns = component.lower_ns, .lower = component.lower, .upper_ns = component.upper_ns, .upper = component.upper };
        var result = SelectedPlan{ .plan = .{ .source_level = 0, .source_start = 0, .source_len = 0, .target_start = 0, .target_len = 0, .output_level = 0 } };
        if (self.job.progress == null) {
            const handles = component.handles.?;
            result.plan = .{ .source_level = handles[0].run.level, .source_start = 0, .source_len = handles.len, .target_start = handles.len, .target_len = 0, .output_level = @max(@as(u32, 1), handles[handles.len - 1].run.level), .run_indices = component.indices, .input_handles = handles, .tombstone_gc = true };
            result.reservation = component.output_reservation;
        } else {
            if (self.job.progress.?.phase == .done) {
                const progress = &self.job.progress.?;
                result.plan = .{ .source_level = progress.source_level, .source_start = 0, .source_len = progress.source_len, .target_start = progress.source_len, .target_len = progress.handles.?.len - progress.source_len, .output_level = progress.source_level +| 1, .run_indices = progress.indices, .input_handles = progress.handles };
                progress.indices = null;
                progress.handles = null;
                result.reservation = progress.output_reservation;
                progress.output_reservation = null;
            } else if (self.job.split) {
                if (self.job.output_manager) |manager| result.reservation = try manager.reserve(.lsm_table_builder_working_set, @sizeOf(Directory.Handle) + @sizeOf(usize));
                errdefer if (result.reservation) |*lease| lease.release();
                const handles = try allocator.alloc(Directory.Handle, 1);
                errdefer allocator.free(handles);
                const indices = try allocator.alloc(usize, 1);
                const anchor = self.job.anchor;
                handles[0] = anchor.retain();
                indices[0] = self.directory.rankOf(anchor.run).?;
                result.plan = .{ .source_level = anchor.run.level, .source_start = 0, .source_len = 1, .target_start = 1, .target_len = 0, .output_level = anchor.run.level, .run_indices = indices, .input_handles = handles, .split_gc = true };
            }
            result.gc_objective_handles = component.handles;
            result.gc_objective_indices = component.indices;
            result.objective_reservation = component.output_reservation;
        }
        component.handles = null;
        component.indices = null;
        component.output_reservation = null;
        return result;
    }
};

fn handleBytes(handles: []const Directory.Handle) u64 {
    var bytes: u64 = 0;
    for (handles) |handle| bytes +|= handle.run.size_bytes;
    return bytes;
}

test "GC phase admission ignores unrelated runs and releases discovery before validation" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const allocator = std.testing.allocator;
    for ([_]usize{ 10000, 30000 }) |count| {
        var budgets = resource_manager_mod.Options.defaultBudgets();
        const cap = 3 * 1024 * 1024;
        budgets[@intFromEnum(resource_manager_mod.Slice.lsm_table_builder_working_set)] = .{ .hard_limit_bytes = cap };
        var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
        defer std.debug.assert(manager.sliceStats(.lsm_table_builder_working_set).used_bytes == 0);
        var backend = Backend.init(allocator, .{});
        defer backend.close();
        try populateClosureDirectoryWithInputsForTest(&backend, count, 2);
        const changed = try (try backend.planningDirectory()).fork(allocator);
        var deleted = changed.at(count - 1).run.*;
        deleted.tombstone_count = 1;
        try changed.put(&backend, deleted);
        backend.publishRunDirectory(changed);
        backend.options.resource_manager = &manager;
        const locked = runtime_mod.lockBackend(Backend, &backend);
        defer runtime_mod.unlockBackend(Backend, &backend, locked);
        const started = @import("antfly_platform").time.monotonicNs();
        var saw_handoff = false;
        var accepted = false;
        for (0..4096) |_| {
            if (try selectDirectoryGc(&backend, 0)) |selected| {
                try std.testing.expectEqual(@as(usize, 1), selected.plan.input_handles.?.len);
                try std.testing.expect(selected.reservation != null);
                selected.release(&backend);
                accepted = true;
                break;
            }
            if (backend.pending_gc) |pending| {
                if (pending.phase == .reclaim_discovery) {
                    saw_handoff = true;
                    try std.testing.expect(pending.validation == null);
                    try std.testing.expect(pending.scratch.?.live_bytes > 0);
                }
                if (pending.phase == .validate) try std.testing.expectEqual(@as(u64, 0), pending.scratch.?.live_bytes);
            }
        }
        try std.testing.expect(accepted and saw_handoff);
        const peak = manager.sliceStats(.lsm_table_builder_working_set).peak_bytes;
        try std.testing.expect(peak < cap);
        if (@import("builtin").mode == .ReleaseFast) std.debug.print("\nLSM GC phase inputs=1 runs={d} peak_bytes={d} elapsed_ns={d}\n", .{ count, peak, @import("antfly_platform").time.monotonicNs() - started });
    }
}

test "GC phase handoff preserves epochs and drains partial cleanup or admission denial" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const allocator = std.testing.allocator;
    for (0..4) |mode| {
        var budgets = resource_manager_mod.Options.defaultBudgets();
        const cap: u64 = if (mode == 3) @sizeOf(PendingGc) + @sizeOf(Directory) + 1 else 1024 * 1024;
        budgets[@intFromEnum(resource_manager_mod.Slice.lsm_table_builder_working_set)] = .{ .hard_limit_bytes = cap };
        var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
        defer std.debug.assert(manager.sliceStats(.lsm_table_builder_working_set).used_bytes == 0);
        var backend = Backend.init(allocator, .{});
        defer backend.close();
        try populateClosureDirectoryWithInputsForTest(&backend, 6000, 5001);
        const initial = try (try backend.planningDirectory()).fork(allocator);
        var deleted = initial.at(0).run.*;
        deleted.tombstone_count = 1;
        deleted.gc_requested = true;
        try initial.put(&backend, deleted);
        backend.publishRunDirectory(initial);
        backend.options.resource_manager = &manager;
        const locked = runtime_mod.lockBackend(Backend, &backend);
        defer runtime_mod.unlockBackend(Backend, &backend, locked);
        if (mode == 3) {
            try std.testing.expectError(error.ResourceBudgetExceeded, selectDirectoryGc(&backend, 0));
            try std.testing.expect(backend.pending_gc == null);
            continue;
        }
        for (0..4096) |_| {
            try std.testing.expect(try selectDirectoryGc(&backend, 0) == null);
            if (backend.pending_gc.?.phase == .reclaim_discovery) break;
        }
        const pending = backend.pending_gc.?;
        try std.testing.expect(pending.phase == .reclaim_discovery);
        try std.testing.expect(pending.selected.?.reservation != null);
        const ranks = pending.selected.?.plan.run_indices.?.ptr;
        if (mode == 2) {
            var credit: usize = 1;
            try std.testing.expect(!pending.job.deinitStep(allocator, &credit));
            continue; // close owns all remaining arena/handle cleanup.
        }
        const changed = try (try backend.planningDirectory()).fork(allocator);
        var replacement = changed.at(if (mode == 0) 5999 else 1).run.*;
        replacement.gc_requested = true;
        try changed.put(&backend, replacement);
        backend.publishRunDirectory(changed);
        var accepted = false;
        for (0..4096) |_| {
            if (backend.pending_gc == null) break;
            if (try selectDirectoryGc(&backend, 0)) |selected| {
                try std.testing.expectEqual(@as(usize, 5001), selected.plan.input_handles.?.len);
                try std.testing.expectEqual(ranks, selected.plan.run_indices.?.ptr);
                selected.release(&backend);
                accepted = true;
                break;
            }
        }
        try std.testing.expectEqual(mode == 0, accepted);
        try std.testing.expect(backend.pending_gc == null);
        try std.testing.expect(manager.sliceStats(.lsm_table_builder_working_set).peak_bytes <= cap);
    }
}

test "GC intent rebases writes between slices without restarting or dropping newer rows" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const Fixture = struct {
        fn make(allocator: std.mem.Allocator, id: u64, deletes: bool) !Run {
            const first = try allocator.dupe(u8, "a");
            errdefer allocator.free(first);
            return .{ .id = id, .level = 0, .size_bytes = 1, .path = null, .smallest_namespace_name = null, .smallest_key = first, .largest_namespace_name = null, .largest_key = try allocator.dupe(u8, "a"), .entry_count = 1, .tombstone_count = @intFromBool(deletes), .oldest_tombstone_unix_ns = 1, .bloom_filter = null, .state = .{} };
        }
        fn append(backend: *Backend, id: u64) !void {
            var runs: std.ArrayListUnmanaged(Run) = .empty;
            defer {
                for (runs.items) |*run| run.deinit(backend.allocator);
                runs.deinit(backend.allocator);
            }
            try runs.append(backend.allocator, try make(backend.allocator, id, false));
            const directory = try backend.prepareRunDirectoryChange(null, runs.items);
            errdefer if (directory) |root| root.destroy(backend.allocator);
            try appendBackendRuns(backend, &runs);
            backend.invalidateReadVersion();
            backend.publishRunDirectory(directory);
        }
    };
    const allocator = std.testing.allocator;
    var backend = Backend.init(allocator, .{ .tombstone_gc_max_age_ns = 1, .tombstone_gc_min_percent = 1 });
    defer backend.close();
    const inputs = 3000;
    for (0..inputs) |i| try backend.runs.append(allocator, try Fixture.make(allocator, i + 1, true));
    const locked = runtime_mod.lockBackend(Backend, &backend);
    defer runtime_mod.unlockBackend(Backend, &backend, locked);
    var added: usize = 0;
    var rebasing_writes: usize = 0;
    var original: ?*PendingGc = null;
    for (0..10000) |_| {
        if (try selectDirectoryGc(&backend, 0)) |selected| {
            defer selected.deinit(allocator);
            try std.testing.expect(added != 0);
            try std.testing.expectEqual(@as(usize, inputs), selected.plan.source_len);
            try std.testing.expectEqual(inputs + added, backend.runs.count());
            for (0..inputs) |i| {
                const run = run_store.planAt(&backend, selected.plan, i);
                try std.testing.expect(run.gc_requested and run.id <= inputs);
            }
            for (0..added) |i| try std.testing.expect(backend.run_directory.?.byId(inputs + i + 1) != null);
            return;
        }
        if (backend.pending_gc) |pending| {
            if (original) |expected| try std.testing.expectEqual(expected, pending) else original = pending;
            // Exercise writes throughout preparation and a sustained rebase
            // burst, then permit catch-up. An infinite arrival stream can
            // exceed a 2 ms service quantum on a contended Debug allocator;
            // convergence must not depend on the test host's minimum speed.
            if (pending.intent != null and rebasing_writes < 64) {
                if (pending.intent.?.rebase != null) rebasing_writes += 1;
                added += 1;
                try Fixture.append(&backend, inputs + added);
            }
        }
    }
    if (backend.pending_gc) |pending| {
        if (pending.intent) |intent| std.debug.print("GC intent stalled: added={d} prepared={d} refreshed={d} rebasing={}\n", .{ added, intent.index, intent.refreshed, intent.rebase != null });
        if (pending.validation) |validation| std.debug.print("GC validation stalled: phase={s} inputs={d} slices={d} rebases={d}\n", .{ @tagName(validation.phase), validation.job.index, validation.slices, validation.rebases });
    }
    return error.GcIntentDidNotConverge;
}

fn resumeGcIntent(backend: anytype, pending: *PendingGc) !?SelectedPlan {
    var retire = false;
    defer if (retire) {
        backend.pending_gc = null;
        backend.retireGcPlanning(pending);
    };
    if (backend.manifestCoordinationIo()) |io| io.checkCancel() catch |err| {
        retire = true;
        return err;
    };
    backend.retainReaderKind(.compaction);
    defer backend.releaseReaderKind(.compaction);
    const intent = &pending.intent.?;
    var deadline: u64 = 0;
    var turns: usize = 0;
    while (true) {
        runtime_mod.unlockBackend(@TypeOf(backend.*), backend, true);
        // Reclamation has its own bounded quantum. Starting this deadline
        // before unlock can starve intent work under continuous publication.
        if (deadline == 0) deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
        const result = if (intent.rebase != null)
            intent.stepRebase(backend, &pending.objective_bounds, &pending.selected.?, 512, deadline)
        else
            intent.step(backend, &pending.selected.?, 512, deadline);
        _ = runtime_mod.lockBackend(@TypeOf(backend.*), backend);
        backend.directory_planning_slices +|= 1;
        const done = result catch |err| {
            retire = true;
            return err;
        };
        turns += 1;
        if (done) {
            if (intent.rebase != null) intent.finishRebase(backend);
            if ((try backend.planningDirectory()).tree.root == intent.base.tree.root) break;
            intent.beginRebase(backend) catch |err| {
                retire = true;
                return err;
            };
        }
        if (turns == 4 or @import("antfly_platform").time.monotonicNs() >= deadline) return null;
    }
    retire = true;
    // All record revisions, metadata clones and handle refreshes are prepared.
    // Publication changes two roots and the durable-metadata obligation only.
    std.mem.swap(run_store.Store, &backend.runs, intent.store.?);
    backend.publishRunDirectory(intent.directory);
    intent.directory = null;
    backend.manifest_pending_mutation_bytes +|= intent.wire;
    backend.manifest_reserved_mutation_bytes -= intent.wire;
    intent.wire = 0;
    backend.markManifestDirty();
    if (pending.selected.?.plan.source_len == 0) return null;
    var selected = pending.selected.?;
    pending.selected = null;
    selected.plan.partition_key = backend.options.run_partition_key;
    return selected;
}

test "GC intent rejects changes to selected newer levels above its tombstone anchor" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const allocator = std.testing.allocator;
    var backend = Backend.init(allocator, .{});
    defer backend.close();
    for (0..2) |i| {
        const first = try allocator.dupe(u8, "a");
        const last = allocator.dupe(u8, "c") catch |err| {
            allocator.free(first);
            return err;
        };
        var run = Run{ .id = i + 1, .level = @intCast(i), .size_bytes = 1, .path = null, .smallest_namespace_name = null, .smallest_key = first, .largest_namespace_name = null, .largest_key = last, .entry_count = 1, .tombstone_count = @intCast(i), .bloom_filter = null, .state = .{} };
        errdefer run.deinit(allocator);
        try backend.runs.append(allocator, run);
    }
    const locked = runtime_mod.lockBackend(Backend, &backend);
    defer runtime_mod.unlockBackend(Backend, &backend, locked);
    const original = try backend.planningDirectory();
    const handles = [_]Directory.Handle{ original.at(0), original.at(1) };
    var component = try ClosureJob.init(allocator, original, &.{handles[1]}, 0, true);
    defer component.deinit(allocator);
    while (!try component.step(allocator, 1)) {}
    var selected = SelectedPlan{ .plan = .{ .source_level = 0, .source_start = 0, .source_len = 2, .target_start = 2, .target_len = 0, .output_level = 1, .input_handles = &handles, .tombstone_gc = true }, .borrowed_inputs = true };
    const store = try allocator.create(run_store.Store);
    store.* = backend.runs.fork();
    var intent = PendingGc.Intent{ .base = try original.fork(allocator), .directory = try original.fork(allocator), .store = store, .wire = 0, .header_bytes = 0 };
    defer intent.discard(&backend);
    const latest = try original.fork(allocator);
    var revised = handles[0].run.*;
    revised.gc_requested = true;
    try latest.put(&backend, revised);
    backend.publishRunDirectory(latest);
    try intent.beginRebase(&backend);
    for (0..64) |_| {
        if (intent.rebase.?.pending_change != null) break;
        try std.testing.expect(!try intent.stepRebase(&backend, &component, &selected, 1, 0));
    }
    try std.testing.expect(intent.rebase.?.pending_change != null);
    try std.testing.expectError(error.CompactionPlanningStale, intent.stepRebase(&backend, &component, &selected, 2048, std.math.maxInt(u64)));
    try std.testing.expect(!intent.directory.?.byId(handles[0].run.id).?.gc_requested);
}

fn selectDirectoryGc(backend: anytype, max_bytes: u64) !?SelectedPlan {
    if (backend.directory_planning_in_flight) return null;
    // A completed negative sweep is not new work. Rebuilding it each turn
    // would report planner progress forever to run-until-idle callers.
    if (backend.pending_gc == null and !hasTombstoneGcDebt(backend)) return null;
    backend.directory_planning_in_flight = true;
    defer backend.directory_planning_in_flight = false;
    if (backend.pending_gc) |pending| if (pending.intent != null) return resumeGcIntent(backend, pending);
    if (backend.pending_gc) |pending| if (pending.selected != null) return resumeGcValidation(backend, pending);
    const allocator = backend.allocator;
    const configured = backend.options.tombstone_gc_max_input_bytes;
    const limit = if (max_bytes == 0) configured else if (configured == 0) max_bytes else @min(max_bytes, configured);
    if (backend.pending_gc == null) {
        const current_directory = try backend.planningDirectory();
        if (current_directory.tombstoneRunCount() == 0) return null;
        const directory = try current_directory.fork(allocator);
        errdefer backend.retireCheckpointDirectory(directory);
        var reservation: ?resource_manager_mod.Reservation = null;
        errdefer if (reservation) |*lease| lease.release();
        if (backend.options.resource_manager) |manager| reservation = try manager.reserve(.lsm_table_builder_working_set, @sizeOf(PendingGc) + @sizeOf(Directory));
        const pending = try allocator.create(PendingGc);
        const cache = GcDebtCache{ .generation = backend.run_directory_generation, .age = backend.options.tombstone_gc_max_age_ns, .percent = backend.options.tombstone_gc_min_percent, .checked_at = gcNowNs() };
        pending.* = .{ .directory = directory, .job = .init(directory, backend.gc_planning_next_rank, cache.age, cache.percent, cache.checked_at, limit), .cache = cache, .reservation = reservation };
        if (backend.options.resource_manager) |manager| {
            pending.scratch = .init(manager, .lsm_table_builder_working_set, allocator, 1);
            pending.job.scratch_allocator = pending.scratch.?.allocator();
            pending.job.output_manager = manager;
        }
        backend.pending_gc = pending;
    }
    const pending = backend.pending_gc.?;
    var retire = false;
    defer if (retire) {
        backend.pending_gc = null;
        backend.retireGcPlanning(pending);
    };
    if (backend.manifestCoordinationIo()) |io| try io.checkCancel();
    backend.retainReaderKind(.compaction);
    defer backend.releaseReaderKind(.compaction);
    runtime_mod.unlockBackend(@TypeOf(backend.*), backend, true);
    const result = pending.job.step(allocator, 2048, @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms);
    _ = runtime_mod.lockBackend(@TypeOf(backend.*), backend);
    backend.directory_planning_slices +|= 1;
    const done = result catch |err| {
        retire = true;
        if (err == error.OutOfMemory) if (pending.scratch) |*scratch| if (scratch.denied()) return error.ResourceBudgetExceeded;
        return err;
    };
    if (!done) return null;
    retire = true;
    backend.gc_planning_next_rank = pending.job.cursor.rank;
    pending.cache.has_debt = pending.job.eligible;
    pending.cache.valid_until = pending.job.valid_until;
    if (pending.cache.generation == backend.run_directory_generation) backend.gc_debt_cache = pending.cache;
    pending.selected = try pending.take(allocator) orelse return null;
    pending.phase = .reclaim_discovery;
    retire = false;
    return null;
}

fn resumeGcValidation(backend: anytype, pending: *PendingGc) !?SelectedPlan {
    var retire = false;
    defer if (retire) {
        backend.pending_gc = null;
        backend.retireGcPlanning(pending);
    };
    errdefer retire = true;
    if (pending.phase == .reclaim_discovery) {
        if (backend.manifestCoordinationIo()) |io| try io.checkCancel();
        backend.retainReaderKind(.compaction);
        defer backend.releaseReaderKind(.compaction);
        runtime_mod.unlockBackend(@TypeOf(backend.*), backend, true);
        var credits: usize = 2048;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
        var done = false;
        while (credits != 0 and @import("antfly_platform").time.monotonicNs() < deadline) {
            var quantum: usize = @min(credits, 64);
            const before = quantum;
            done = pending.job.deinitStep(backend.allocator, &quantum);
            credits -= before - quantum;
            if (done) break;
        }
        _ = runtime_mod.lockBackend(@TypeOf(backend.*), backend);
        backend.directory_planning_slices +|= 1;
        if (done) {
            if (pending.scratch) |*scratch| std.debug.assert(scratch.live_bytes == 0);
            pending.phase = .validate;
        }
        return null;
    }
    if (pending.validation == null) {
        const selected = &pending.selected.?;
        const objectives = selected.gc_objective_handles orelse selected.plan.input_handles.?;
        var objective = selected.plan;
        objective.input_handles = objectives;
        objective.source_level = objectives[0].run.level;
        objective.split_gc = false;
        objective.tombstone_gc = true;
        pending.validation = try DependencyValidation.init(backend, objective);
        const ranks = if (selected.gc_objective_handles != null) &selected.gc_objective_indices else &selected.plan.run_indices;
        pending.validation.?.job.indices = @constCast(ranks.*.?);
        ranks.* = null;
    }
    const result = try pending.validation.?.advanceLocked(backend);
    backend.directory_planning_slices +|= 1;
    if (result == .pending) return null;
    if (result == .invalid) {
        retire = true;
        return null;
    }
    const selected = &pending.selected.?;
    const ranks = if (selected.gc_objective_handles != null) &selected.gc_objective_indices else &selected.plan.run_indices;
    ranks.* = pending.validation.?.takeIndices();
    // The complete objective is certified against the live root. Capture the
    // intent roots before unlocking; intent's own continuation rebases later
    // publications without retaining validation scratch.
    pending.intent = try PendingGc.Intent.create(backend, pending);
    pending.validation.?.deinit(backend);
    pending.validation = null;
    return null;
}

fn selectTombstoneGc(backend: anytype, max_bytes: u64) !?SelectedPlan {
    if (comptime @hasDecl(@TypeOf(backend.*), "planningDirectory")) return selectDirectoryGc(backend, max_bytes) catch |err| {
        if (err == error.CompactionPlanningStale) return null;
        return err;
    };
    const index = if (comptime @hasDecl(@TypeOf(backend.*), "domainIndex")) backend.domainIndex() catch |err| {
        if (err == error.CompactionPlanningStale) return null;
        return err;
    } else try DomainIndex.create(backend);
    defer if (comptime !@hasDecl(@TypeOf(backend.*), "domainIndex")) index.destroy(backend.allocator);
    try requestEligibleGcComponents(backend, index);
    const configured = if (comptime @hasField(@TypeOf(backend.options), "tombstone_gc_max_input_bytes")) backend.options.tombstone_gc_max_input_bytes else max_bytes;
    const limit = if (max_bytes == 0) configured else if (configured == 0) max_bytes else @min(max_bytes, configured);
    const candidate = tombstoneGcCandidate(backend, index, limit) orelse {
        if (tombstoneGcCandidate(backend, index, 0) == null) return null;
        return try selectGcProgress(backend, index, limit);
    };
    const best = candidate.indices;
    return .{ .plan = .{ .source_level = run_store.at(backend, best[0]).*.level, .source_start = 0, .source_len = best.len, .target_start = 0, .target_len = 0, .output_level = candidate.level, .run_indices = try backend.allocator.dupe(usize, best), .tombstone_gc = true } };
}

/// A large connected component is not one indivisible GC job. Advance one
/// source window and its next-level overlap closure. Each manifest publication
/// is a durable checkpoint of progress, and normal level/recency rules apply.
fn selectGcProgress(backend: anytype, index: *const DomainIndex, limit: u64) !?SelectedPlan {
    var best: ?ScoredCompactionPlan = null;
    var best_indices: ?[]const usize = null;
    const all_end = [_]usize{run_store.count(backend)};
    const ends = if (index.mixed) &all_end else index.ends;
    var start: usize = 0;
    for (ends) |end| {
        defer start = end;
        const runs = if (index.mixed) (try run_store.oracleItems(backend)) else index.runs[start..end];
        for (runs, 0..) |run, i| {
            const deletes = run.tombstone_count orelse 0;
            if (deletes == 0 or run.level == std.math.maxInt(u32)) continue;
            const percent = if (comptime @hasField(@TypeOf(backend.options), "tombstone_gc_min_percent")) backend.options.tombstone_gc_min_percent else 50;
            // The projection may predate the request bit; use the live input.
            const live_index = if (index.mixed) i else index.order[start + i];
            if (!run_store.at(backend, live_index).*.gc_requested and !tombstoneAgeDue(backend, run) and @as(u64, deletes) * 100 < @as(u64, run.entry_count) * percent) continue;
            var plan = buildPlanForSourceRange(runs, run.level, i, 1) orelse continue;
            var priority: u64 = if (tombstoneAgeDue(backend, run)) 3 else 2;
            if (!planWithinInputBudget(runs, plan, limit)) {
                // Drain an older L0 dependency first if it makes the delete's
                // window indivisible. This work need not itself contain deletes.
                const split_index = if (run.level == 0 and plan.source_len > 1) plan.source_start + plan.source_len - 1 else i;
                plan = buildPlanForSourceRange(runs, run.level, split_index, 1) orelse continue;
                if (!planWithinInputBudget(runs, plan, limit)) {
                    const source = runs[split_index];
                    if (source.entry_count <= 1 or (limit != 0 and source.size_bytes > limit)) continue;
                    // Split wide sources; persistent visibility IDs retain L0 order.
                    plan = .{ .source_level = run.level, .source_start = split_index, .source_len = 1, .target_start = split_index, .target_len = 0, .output_level = run.level, .split_gc = true };
                }
                priority = 1;
            }
            const candidate = scoredPlan(runs, plan, priority);
            if (best) |previous| if (!candidate.betterThan(previous)) continue;
            best = candidate;
            best_indices = if (index.mixed) null else index.order[start..end];
        }
    }
    var plan = (best orelse return null).plan;
    if (best_indices) |indices| {
        plan.run_indices = try backend.allocator.dupe(usize, indices);
        plan.partition_key = backend.options.run_partition_key orelse wholeKeyspace;
    }
    return .{ .plan = plan };
}

pub fn compactTombstonesScheduled(comptime BackendType: type, backend: *BackendType, score: u64) !bool {
    if (comptime @hasField(BackendType, "tombstone_gc_retry_after_ns")) {
        if (backend.nowNs() < backend.tombstone_gc_retry_after_ns) return false;
        backend.tombstone_gc_retry_after_ns = 0;
    }
    const selected = try selectTombstoneGc(backend, backend.options.max_compaction_input_bytes) orelse {
        // A continuation yielding its quantum is progress, not an admission
        // denial. In particular, aged GC must not sleep 250 ms after every
        // identity, cleanup, or intent slice.
        if (comptime @hasField(BackendType, "pending_gc")) if (backend.pending_gc != null) return false;
        if ((nextTombstoneGcDelay(backend) orelse 1) == 0) deferTombstoneGc(backend);
        return false;
    };
    defer selected.release(backend);
    var work = try compactionWorkForPlan(backend.allocator, &backend.runs, selected.plan, score);
    defer work.deinit(backend.allocator);
    var grant = backend.acquireCompactionGrant(work) orelse {
        deferTombstoneGc(backend);
        return false;
    };
    defer grant.complete();
    try compactPlanAt(BackendType, backend, selected.plan);
    if (comptime @hasField(BackendType, "tombstone_gc_retry_after_ns")) backend.tombstone_gc_retry_after_ns = 0;
    return true;
}

fn deferTombstoneGc(backend: anytype) void {
    // A past age deadline with an inadmissible closure must not turn the idle
    // worker into a busy loop. New writes still use the ordinary wake path.
    if (comptime @hasField(@TypeOf(backend.*), "tombstone_gc_retry_after_ns"))
        backend.tombstone_gc_retry_after_ns = backend.nowNs() +| 250 * std.time.ns_per_ms;
}

/// Project each independently compactable domain into the existing leveled
/// planner. L0 precedence and target closure are unchanged *within* a domain;
/// unrelated interleaved runs are never added merely to make a global slice.
fn selectDomainPlan(backend: anytype, l0_limit: usize, l0_only: bool, max_bytes: u64, allow_oversized: bool, stats: *CompactionSelectionStats) !?SelectedPlan {
    if (comptime @hasField(@TypeOf(backend.*), "pending_directory_closure")) {
        // A slice temporarily drops the writer mutex. Never replace a slot
        // whose owner is using it off-lock, including the other policy lane.
        if (backend.directory_planning_in_flight) return null;
        const policy = PlanningPolicy{ .l0_limit = l0_limit, .l0_only = l0_only, .max_bytes = max_bytes, .allow_oversized = allow_oversized };
        const slot = closureSlot(backend, l0_only);
        if (slot.*) |pending| if (!pending.policy.matches(policy)) {
            slot.* = null;
            backend.retireClosurePlanning(pending);
        };
        // Alternate service opportunities, not just occupied slots. Discovery
        // of deeper-level work needs a turn even when only L0 has queued a job.
        // Foreground calls never consume or restart a background closure.
        if (!l0_only and backend.pending_l0_directory_closure != null) {
            const serve_l0 = backend.closure_service_l0_next;
            backend.closure_service_l0_next = !serve_l0;
            if (serve_l0) return resumeDirectoryClosure(backend, &backend.pending_l0_directory_closure);
            // Even an empty discovery turn advances the scheduling cursor.
            // Report that progress so a no-op-sensitive worker does not park
            // before giving the still-pending L0 job its next quantum.
            backend.directory_planning_slices +|= 1;
        }
        if (slot.* != null) return resumeDirectoryClosure(backend, slot);
    }
    if (comptime @hasDecl(@TypeOf(backend.*), "planningDirectory")) {
        return selectDirectoryPlan(backend, l0_limit, l0_only, max_bytes, allow_oversized, stats) catch |err| {
            if (err != error.CompactionPlanningBudgetExceeded) return err;
            // A broad closure continues on a pinned tree outside the writer
            // mutex. Never construct a complete DomainIndex/ID projection.
            return selectDirectoryPlanOffLock(backend, l0_limit, l0_only, max_bytes, allow_oversized, stats);
        };
    }
    return selectProjectedDomainPlan(backend, l0_limit, l0_only, max_bytes, allow_oversized, stats);
}

fn selectProjectedDomainPlan(backend: anytype, l0_limit: usize, l0_only: bool, max_bytes: u64, allow_oversized: bool, stats: *CompactionSelectionStats) !?SelectedPlan {
    const allocator = backend.allocator;
    const partition = backend.options.run_partition_key orelse wholeKeyspace;
    const index = if (comptime @hasDecl(@TypeOf(backend.*), "domainIndex")) backend.domainIndex() catch |err| {
        if (err == error.CompactionPlanningStale) return null;
        return err;
    } else try DomainIndex.create(backend);
    defer if (comptime !@hasDecl(@TypeOf(backend.*), "domainIndex")) index.destroy(allocator);
    const runs = (try run_store.oracleItems(backend));
    if (index.mixed) {
        // Previously written mixed SSTs must first be reshaped with the full
        // overlap closure. Never hide overlapping data behind a new domain.
        const plan = if (l0_only)
            selectL0CompactionWithStats(runs, l0_limit, max_bytes, allow_oversized, stats)
        else
            selectCompactionPlanWithStats(runs, l0_limit, backend.options.l0_overlap_compact_threshold_runs, backend.options.level_target_runs_base, backend.options.level_target_runs_multiplier, backend.options.level_target_bytes_base, backend.options.level_target_bytes_multiplier, max_bytes, allow_oversized, stats);
        return if (plan) |selected| .{ .plan = selected } else null;
    }
    var best_score: ?ScoredCompactionPlan = null;
    var best_indices: []const usize = &.{};
    const global_l0 = countLeadingL0Runs(runs);
    var first: usize = 0;
    for (index.ends) |end| {
        const indices = index.order[first..end];
        const local = index.runs[first..end];
        first = end;
        const local_l0 = countLeadingL0Runs(local);
        // Global admission pressure must still make progress when many small
        // domains each have fewer runs than the ordinary per-domain limit.
        const local_limit = if (global_l0 > l0_limit and local_l0 != 0) @min(l0_limit, local_l0 - 1) else l0_limit;
        var candidate: ?ScoredCompactionPlan = null;
        if (selectL0CompactionCandidateWithStats(local, local_limit, max_bytes, allow_oversized, stats)) |pressure| {
            maybeAdoptBest(&candidate, scoredPlan(local, pressure.plan, normalizedPressurePriority(global_l0, @max(@as(usize, 1), l0_limit))));
        }
        if (!l0_only) {
            if (local_l0 <= @min(l0_limit, max_exact_l0_overlap_runs)) maybeAdoptBest(&candidate, selectL0OverlapCompactionCandidateWithStats(local, backend.options.l0_overlap_compact_threshold_runs, @max(backend.options.l0_overlap_compact_threshold_runs, l0_limit), max_bytes, stats));
            maybeAdoptBest(&candidate, selectLowerLevelRepairCompactionCandidateWithStats(local, max_bytes, allow_oversized, stats));
            maybeAdoptBest(&candidate, index.lowerPressure(local, max_bytes, allow_oversized, stats));
        }
        const scored = candidate orelse continue;
        if (best_score) |previous| if (!scored.betterThan(previous)) continue;
        best_score = scored;
        best_indices = indices;
    }
    var plan = (best_score orelse return null).plan;
    plan.run_indices = try allocator.dupe(usize, best_indices);
    plan.partition_key = partition;
    return .{ .plan = plan };
}

/// Enumerate only the selected range's dependencies. Unlike DomainIndex this
/// path neither projects the full epoch nor creates a global ID-to-rank map.
const DirectoryPlanningBudget = struct {
    remaining: usize = 16384,
    max_inputs: usize = 4096,
    resumable: bool = false,
    io: ?std.Io = null,
    gc_all: bool = false,

    fn next(self: *@This(), cursor: *Directory.OverlapCursor) !?Directory.Handle {
        while (true) {
            if (cursor.next(&self.remaining)) |handle| return handle;
            if (cursor.done() or !self.resumable) return null;
            if (self.io) |io| try io.checkCancel();
            self.remaining = 16384;
        }
    }
};

fn directoryClosure(allocator: std.mem.Allocator, directory: *const Directory, seeds: []const Directory.Handle, max_bytes: u64, budget: *DirectoryPlanningBudget) !?SelectedPlan {
    const anchor = seeds[0];
    if (!budget.gc_all and anchor.run.level == std.math.maxInt(u32)) return null;
    var handles: std.ArrayListUnmanaged(Directory.Handle) = .empty;
    defer handles.deinit(allocator);
    var seen: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer seen.deinit(allocator);
    var bytes: u64 = 0;
    var bounds = CompactionBounds{ .smallest_namespace_name = anchor.run.smallest_namespace_name, .smallest_key = anchor.run.smallest_key, .largest_namespace_name = anchor.run.largest_namespace_name, .largest_key = anchor.run.largest_key };
    var visibility: u64 = 0;
    for (seeds) |seed| {
        try handles.append(allocator, seed);
        try seen.put(allocator, seed.run.id, {});
        bytes +|= seed.run.size_bytes;
        bounds.include(seed.run.*);
        visibility = @max(visibility, if (seed.run.visibility_id == 0) seed.run.id else seed.run.visibility_id);
    }
    var changed = true;
    while (changed) {
        changed = false;
        var cursor = directory.overlaps(bounds.smallest_namespace_name, bounds.smallest_key, bounds.largest_namespace_name, bounds.largest_key);
        while (try budget.next(&cursor)) |handle| {
            const run = handle.run;
            const older_l0 = anchor.run.level == 0 and run.level == 0 and (if (run.visibility_id == 0) run.id else run.visibility_id) <= visibility;
            if (!budget.gc_all and !older_l0 and run.level != anchor.run.level + 1) continue;
            if (seen.contains(run.id)) continue;
            bytes +|= run.size_bytes;
            if (max_bytes != 0 and bytes > max_bytes) return null;
            if (handles.items.len == budget.max_inputs) return error.CompactionPlanningBudgetExceeded;
            try seen.put(allocator, run.id, {});
            try handles.append(allocator, handle);
            const old = bounds;
            bounds.include(run.*);
            changed = changed or compareRunBound(old.smallest_namespace_name, old.smallest_key, bounds.smallest_namespace_name, bounds.smallest_key) != .eq or compareRunBound(old.largest_namespace_name, old.largest_key, bounds.largest_namespace_name, bounds.largest_key) != .eq;
        }
        if (!cursor.done()) return error.CompactionPlanningBudgetExceeded;
    }
    if (max_bytes != 0 and bytes > max_bytes) return null;
    // Read precedence is a metadata comparator, not a rank lookup. Looking up
    // two tree ranks per comparison turns a broad sort into O(K log K log N).
    std.mem.sort(Directory.Handle, handles.items, {}, Directory.readLess);
    var source_len: usize = 0;
    while (source_len < handles.items.len and handles.items[source_len].run.level == anchor.run.level) : (source_len += 1) {}
    const indices = try allocator.alloc(usize, handles.items.len);
    errdefer allocator.free(indices);
    if (handles.items.len > directory.count() / 4) {
        // Dense selections use an allocation-free merge walk. This is O(N)
        // only when N <= 4K, and never materializes an unselected run vector.
        var cursor = directory.readCursor();
        var selected: usize = 0;
        while (cursor.next()) |handle| {
            if (handle.run.id != handles.items[selected].run.id) continue;
            indices[selected] = cursor.rank - 1;
            selected += 1;
            if (selected == handles.items.len) break;
        }
        std.debug.assert(selected == handles.items.len);
    } else for (handles.items, indices) |handle, *index| index.* = directory.rankOf(handle.run).?;
    const owned = try handles.toOwnedSlice(allocator);
    for (owned) |handle| _ = handle.retain();
    var plan = CompactionPlan{ .source_level = anchor.run.level, .source_start = 0, .source_len = source_len, .target_start = source_len, .target_len = owned.len - source_len, .output_level = anchor.run.level +| 1, .run_indices = indices, .input_handles = owned, .partition_key = wholeKeyspace };
    if (budget.gc_all) {
        plan.source_level = owned[0].run.level;
        plan.source_len = owned.len;
        plan.target_start = owned.len;
        plan.target_len = 0;
        plan.output_level = @max(@as(u32, 1), owned[owned.len - 1].run.level);
        plan.tombstone_gc = true;
    }
    return .{ .plan = plan };
}

fn selectDirectoryPlan(backend: anytype, l0_limit: usize, l0_only: bool, max_bytes: u64, allow_oversized: bool, stats: *CompactionSelectionStats) !?SelectedPlan {
    return selectDirectoryPlanBudgeted(backend, l0_limit, l0_only, max_bytes, allow_oversized, stats, .{});
}

fn selectDirectoryPlanOffLock(backend: anytype, l0_limit: usize, l0_only: bool, max_bytes: u64, allow_oversized: bool, stats: *CompactionSelectionStats) !?SelectedPlan {
    const BackendType = @TypeOf(backend.*);
    if (comptime @hasField(BackendType, "directory_planning_in_flight")) {
        if (backend.directory_planning_in_flight) return null;
        backend.directory_planning_in_flight = true;
    }
    defer if (comptime @hasField(BackendType, "directory_planning_in_flight")) {
        backend.directory_planning_in_flight = false;
    };
    const directory = try (try backend.planningDirectory()).fork(backend.allocator);
    if (comptime !@hasDecl(BackendType, "retainReaderKind")) {
        defer directory.destroy(backend.allocator);
        return selectDirectoryPlanBudgeted(backend, l0_limit, l0_only, max_bytes, allow_oversized, stats, .{ .max_inputs = directory.count(), .resumable = true });
    }
    defer backend.retireCheckpointDirectory(directory);
    backend.retainReaderKind(.compaction);
    defer backend.releaseReaderKind(.compaction);
    const Snapshot = struct {
        allocator: std.mem.Allocator,
        options: @TypeOf(backend.options),
        planner_seed: usize,
        directory: *const Directory,
        pub fn planningDirectory(self: *@This()) !*const Directory {
            return self.directory;
        }
    };
    var snapshot = Snapshot{ .allocator = backend.allocator, .options = backend.options, .planner_seed = backend.planner_seed, .directory = directory };
    // Reserve distinct candidate tickets before allowing another planner in.
    backend.planner_seed +%= 8;
    const io: ?std.Io = if (snapshot.options.read_runtime) |runtime| runtime.io else null;
    runtime_mod.unlockBackend(BackendType, backend, true);
    const result = selectDirectoryPlanBudgeted(&snapshot, l0_limit, l0_only, max_bytes, allow_oversized, stats, .{ .max_inputs = directory.count(), .resumable = true, .io = io });
    _ = runtime_mod.lockBackend(BackendType, backend);
    var selected = (try result) orelse return null;
    var keep = false;
    defer if (!keep) selected.release(backend);
    // The pinned immutable root is an exact certificate for both dependencies
    // and positions. With no intervening publication, accepting even a broad
    // closure is O(1) under the writer mutex.
    if ((try backend.planningDirectory()).tree.root == directory.tree.root) {
        keep = true;
        return selected;
    }
    const relocated = try relocateDirectoryPlan(backend, selected.plan) orelse return null;
    backend.allocator.free(selected.plan.run_indices.?);
    selected.plan.run_indices = relocated.plan.run_indices;
    keep = true;
    return selected;
}

fn selectDirectoryPlanBudgeted(backend: anytype, l0_limit: usize, l0_only: bool, max_bytes: u64, allow_oversized: bool, stats: *CompactionSelectionStats, initial_budget: DirectoryPlanningBudget) !?SelectedPlan {
    const policy = PlanningPolicy{ .l0_limit = l0_limit, .l0_only = l0_only, .max_bytes = max_bytes, .allow_oversized = allow_oversized };
    const directory = try backend.planningDirectory();
    var selected_level: ?u32 = null;
    var best_pressure: u64 = 0;
    for (0..directory.levelCount()) |rank| {
        const level = directory.levelAt(rank);
        if (l0_only and level.level != 0) continue;
        const run_target = if (level.level == 0) l0_limit else levelRunTarget(level.level, backend.options.level_target_runs_base, backend.options.level_target_runs_multiplier);
        const byte_target = levelByteTargetForTotals(directory.total_run_bytes, directory.maxLevel(), level.level, backend.options.level_target_bytes_base, backend.options.level_target_bytes_multiplier);
        var pressure: u64 = 0;
        if (level.count > run_target and (level.level == 0 or run_target != 0)) pressure = normalizedPressurePriority(level.count, @max(@as(usize, 1), run_target));
        if (byte_target != 0 and level.bytes > byte_target) pressure = @max(pressure, normalizedPressurePriority(level.bytes, byte_target));
        if (pressure > best_pressure) {
            best_pressure = pressure;
            selected_level = level.level;
        }
    }
    const overlap_threshold = backend.options.l0_overlap_compact_threshold_runs;
    const hotspot = selected_level == null and !l0_only and overlap_threshold != 0 and directory.levelStats(0).count >= overlap_threshold;
    const level = selected_level orelse if (hotspot) @as(u32, 0) else return null;
    const count = directory.levelStats(level).count;
    const start = directory.levelStart(level);
    var reservation: ?resource_manager_mod.Reservation = null;
    // Inspect allocation-free pressure totals first: idle maintenance must
    // not fail merely because another table owns the builder budget.
    // Bounds cover the seed window, geometric scratch, and retained handles.
    if (backend.options.resource_manager) |manager| reservation = try manager.reserve(.lsm_table_builder_working_set, 64 * 1024 + @min(directory.count(), initial_budget.max_inputs) * 256);
    defer if (reservation) |*lease| lease.release();
    var budget = initial_budget;
    for (0..@min(count, 8)) |_| {
        const offset = backend.planner_seed % count;
        backend.planner_seed +%= 1;
        // Start L0 pressure at the oldest end and drain a bounded window in
        // one closure. Promoting one disjoint run at a time repeatedly rewrites
        // the same target and fails to drain a pressure episode efficiently.
        const seed_rank = if (level == 0) count - 1 - offset else offset;
        const anchor = directory.at(start + seed_rank);
        const target = if (level == 0) @max(@as(usize, 1), l0_limit / 2) else levelRunTarget(level, backend.options.level_target_runs_base, backend.options.level_target_runs_multiplier);
        const desired = if (hotspot) 1 else if (level == 0 and l0_limit == 0) @min(count, 2) else @max(@as(usize, 1), count -| target);
        var seeds: [4096]Directory.Handle = undefined;
        seeds[0] = anchor;
        var seed_len: usize = 1;
        for (1..@min(count, seeds.len)) |step| {
            if (seed_len >= desired) break;
            const rank = if (level == 0) (seed_rank + count - step) % count else (seed_rank + step) % count;
            const candidate = directory.at(start + rank);
            if (backend.options.run_partition_key) |partition| {
                if (!sameDomain(anchor.run.*, candidate.run.*, partition)) continue;
            }
            seeds[seed_len] = candidate;
            seed_len += 1;
        }
        var candidate: ?SelectedPlan = null;
        while (true) {
            candidate = directoryClosure(backend.allocator, directory, seeds[0..seed_len], max_bytes, &budget) catch |err| {
                if (comptime @hasField(@TypeOf(backend.*), "pending_directory_closure")) {
                    if (err == error.CompactionPlanningBudgetExceeded) {
                        if (reservation) |*lease| lease.release();
                        reservation = null;
                        closureSlot(backend, l0_only).* = try PendingDirectoryClosure.create(backend, seeds[0..seed_len], max_bytes, allow_oversized, if (hotspot) overlap_threshold else 0, policy);
                        backend.directory_planning_slices +|= 1;
                        return null;
                    }
                }
                return err;
            };
            if (candidate != null or budget.remaining == 0) break;
            stats.oversized_skips += 1;
            if (seed_len == 1) {
                // Only the minimum indivisible closure can bypass the byte
                // target, never the entire pressure window.
                if (allow_oversized) candidate = directoryClosure(backend.allocator, directory, seeds[0..1], 0, &budget) catch |err| {
                    if (comptime @hasField(@TypeOf(backend.*), "pending_directory_closure")) {
                        if (err == error.CompactionPlanningBudgetExceeded) {
                            if (reservation) |*lease| lease.release();
                            reservation = null;
                            closureSlot(backend, l0_only).* = try PendingDirectoryClosure.create(backend, seeds[0..1], 0, false, if (hotspot) overlap_threshold else 0, policy);
                            backend.directory_planning_slices +|= 1;
                            return null;
                        }
                    }
                    return err;
                };
                if (candidate) |*selected| selected.plan.oversized_indivisible = allow_oversized;
                break;
            }
            seed_len = @max(@as(usize, 1), seed_len / 2);
        }
        var selected = candidate orelse {
            if (budget.remaining == 0) break;
            continue;
        };
        selected.plan.partition_key = backend.options.run_partition_key;
        if (hotspot and selected.plan.source_len < overlap_threshold) {
            // This is bounded fast-path scratch, not end-of-operation cleanup.
            // Keep the writer fence: release() may unlock and reclaim the
            // borrowed directory that the next iteration still uses. Every
            // payload also remains owned by the live directory here.
            selected.deinit(backend.allocator);
            continue;
        }
        selected.reservation = reservation;
        reservation = null;
        return selected;
    }
    return null;
}

pub fn directorySelectionInputCountForTest(backend: anytype) !usize {
    std.debug.assert(@import("builtin").is_test);
    var stats: CompactionSelectionStats = .{};
    const selected = try selectDomainPlanSynchronous(backend, backend.options.compact_threshold_runs, false, backend.options.max_compaction_input_bytes, allowOversizedSingleCompactionInput(backend), &stats) orelse return 0;
    defer selected.release(backend);
    return selected.plan.source_len + selected.plan.target_len;
}

test "hotspot rejection preserves the borrowed directory writer fence" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const Fixture = struct {
        allocator: std.mem.Allocator,
        directory: *Directory,
        options: @import("../lsm_backend.zig").Options = .{ .compact_threshold_runs = 100, .level_target_bytes_base = 0 },
        planner_seed: usize = 0,
        mu: @TypeOf(@as(Backend, undefined).mu) = .unlocked,
        storage: ?void = null,
        root_dir: ?[]u8 = null,
        unlocks: usize = 0,
        pub fn planningDirectory(self: *@This()) !*const Directory {
            return self.directory;
        }
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
        pub fn retainReader(_: *@This()) void {}
        pub fn releaseReader(_: *@This()) void {}
        pub fn retainReaderKind(_: *@This(), _: anytype) void {}
        pub fn releaseReaderKind(_: *@This(), _: anytype) void {}
        pub fn manifestCoordinationIo(_: *@This()) ?std.Io {
            return null;
        }
        pub fn unlockWithReclamation(self: *@This()) void {
            self.unlocks += 1;
            self.mu.unlock();
        }
    };
    const allocator = std.testing.allocator;
    const directory = try Directory.create(allocator);
    defer directory.destroy(allocator);
    var fixture = Fixture{ .allocator = allocator, .directory = directory };
    for (0..12) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .big);
        try directory.put(&fixture, .{ .id = i + 1, .level = 0, .size_bytes = 1, .path = null, .smallest_namespace_name = null, .smallest_key = &key, .largest_namespace_name = null, .largest_key = &key, .entry_count = 1, .bloom_filter = null, .state = .{} });
    }
    const locked = runtime_mod.lockBackend(Fixture, &fixture);
    defer runtime_mod.unlockBackend(Fixture, &fixture, locked);
    var stats: CompactionSelectionStats = .{};
    try std.testing.expect(try selectDirectoryPlan(&fixture, 100, false, 0, false, &stats) == null);
    try std.testing.expectEqual(@as(usize, 8), fixture.planner_seed);
    try std.testing.expectEqual(@as(usize, 0), fixture.unlocks);
}

fn selectDomainPlanSynchronous(backend: anytype, l0_limit: usize, l0_only: bool, max_bytes: u64, allow_oversized: bool, stats: *CompactionSelectionStats) !?SelectedPlan {
    while (true) {
        if (try selectDomainPlan(backend, l0_limit, l0_only, max_bytes, allow_oversized, stats)) |selected| return selected;
        if (comptime @hasField(@TypeOf(backend.*), "pending_directory_closure")) {
            const pending = closureSlot(backend, l0_only).* != null or (!l0_only and backend.pending_l0_directory_closure != null);
            if (pending and !backend.directory_planning_in_flight) continue;
        }
        return null;
    }
}

fn compactDomainPlan(comptime BackendType: type, backend: *BackendType, l0_limit: usize, l0_only: bool, comptime scheduled: bool, score: u64, max_bytes: u64, allow_oversized: bool) !bool {
    const policy = PlanningPolicy{ .l0_limit = l0_limit, .l0_only = l0_only, .max_bytes = max_bytes, .allow_oversized = allow_oversized };
    if (scheduled and !l0_only) if (try compactRememberedPlanIfValid(BackendType, backend, policy)) return true;
    var stats: CompactionSelectionStats = .{};
    defer noteCompactionSelectionStats(BackendType, backend, stats);
    const selected = (if (scheduled)
        try selectDomainPlan(backend, l0_limit, l0_only, max_bytes, allow_oversized, &stats)
    else
        try selectDomainPlanSynchronous(backend, l0_limit, l0_only, max_bytes, allow_oversized, &stats)) orelse return false;
    defer selected.release(backend);
    if (scheduled) {
        var work = try compactionWorkForPlan(backend.allocator, &backend.runs, selected.plan, score);
        defer work.deinit(backend.allocator);
        if (!policy.admits(selected.plan, work.input_bytes)) return false;
        var grant = backend.acquireCompactionGrant(work) orelse {
            rememberDeniedCompaction(BackendType, backend, selected.plan, score);
            return false;
        };
        defer grant.complete();
        try compactPlanAt(BackendType, backend, selected.plan);
    } else {
        if (!policy.admits(selected.plan, compactionInputBytes(&backend.runs, selected.plan))) return false;
        try compactPlanAt(BackendType, backend, selected.plan);
    }
    return true;
}

fn relocateDomainPlan(allocator: std.mem.Allocator, runs: []const Run, plan: CompactionPlan, ids: []const u64) !?SelectedPlan {
    const partition = plan.partition_key.?;
    if (ids.len == 0) return null;
    var anchor: ?Run = null;
    for (runs) |run| {
        if (!pureDomain(run, partition)) return null;
        if (run.id == ids[0]) anchor = run;
    }
    const domain = anchor orelse return null;
    var indices: std.ArrayListUnmanaged(usize) = .empty;
    defer indices.deinit(allocator);
    var local: std.ArrayListUnmanaged(Run) = .empty;
    defer local.deinit(allocator);
    for (runs, 0..) |run, i| if (sameDomain(run, domain, partition)) {
        try indices.append(allocator, i);
        try local.append(allocator, run);
    };
    var original = plan;
    original.run_indices = null;
    original.partition_key = null;
    var relocated = relocatePlanIfInputsStillMatch(local.items, original, ids) orelse return null;
    relocated.run_indices = try indices.toOwnedSlice(allocator);
    relocated.partition_key = partition;
    return .{ .plan = relocated };
}

fn relocateDirectoryPlan(backend: anytype, plan: CompactionPlan) !?SelectedPlan {
    const allocator = backend.allocator;
    var validation = try DependencyValidation.init(backend, plan);
    validation.yield_between_slices = true;
    defer {
        // Synchronous build/publication owns its inputs on the stack. Drain
        // cancellation/error cleanup off-lock too; maintenance-owned jobs
        // instead retain this continuation on their retirement queues.
        if (validation.phase != .certificate) {
            backend.retainReaderKind(.compaction);
            runtime_mod.unlockBackend(@TypeOf(backend.*), backend, true);
            while (true) {
                var credits: usize = 2048;
                if (validation.cleanupStep(allocator, &credits)) break;
                if (backend.manifestCoordinationIo()) |io| io.sleep(.fromNanoseconds(1), .awake) catch {};
            }
            _ = runtime_mod.lockBackend(@TypeOf(backend.*), backend);
            backend.releaseReaderKind(.compaction);
        }
        validation.deinit(backend);
    }
    while (true) {
        // A synchronous installation must not chase a moving epoch forever.
        // Return a stale result after bounded rebase attempts; its caller
        // safely discards unpublished output and schedules a fresh plan.
        if (validation.rebases >= 4 and validation.changes == null) return null;
        switch (try validation.advanceLocked(backend)) {
            .pending => continue,
            .invalid => return null,
            .valid => break,
        }
    }
    var relocated = plan;
    relocated.run_indices = validation.takeIndices();
    relocated.complete_coverage = validation.job.covered;
    relocated.validated_generation = backend.run_directory_generation;
    return .{ .plan = relocated, .borrowed_inputs = true, .complete_coverage = validation.job.covered };
}

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
    if (backend.mutable.entryCount() < flush_threshold) return;
    try flushMutable(BackendType, backend);
}

pub fn flushMutable(comptime BackendType: type, backend: *BackendType) !void {
    if (backend.mutable.entryCount() == 0) return;
    const start_ns = if (@hasDecl(BackendType, "writeStatsNowNs")) backend.writeStatsNowNs() else 0;
    var flushed = if (comptime @TypeOf(backend.mutable) == state_mod.ActiveMemTable)
        try backend.mutable.toStateMove(backend.allocator)
    else blk: {
        const state = backend.mutable;
        backend.mutable = .{};
        break :blk state;
    };
    errdefer flushed.deinit(backend.allocator);
    const input_entries = flushed.entryCount();
    var new_runs = try makeRuns(BackendType, backend, &flushed);
    errdefer discardOutputRuns(BackendType, backend, &new_runs);
    if (@hasDecl(BackendType, "recordFlushWriteStats")) {
        const elapsed_ns = if (@hasDecl(BackendType, "writeStatsNowNs")) elapsedNs(BackendType, backend, start_ns) else 0;
        backend.recordFlushWriteStats(input_entries, new_runs.items, elapsed_ns);
    }
    var directory = if (comptime @hasDecl(BackendType, "prepareRunDirectoryChange")) try backend.prepareRunDirectoryChange(null, new_runs.items) else null;
    errdefer if (directory) |owned| owned.destroy(backend.allocator);
    if (@hasDecl(BackendType, "invalidateReadVersion")) backend.invalidateReadVersion();
    try appendBackendRuns(backend, &new_runs);
    if (comptime @hasDecl(BackendType, "publishRunDirectory")) backend.publishRunDirectory(directory);
    directory = null;
    if (comptime @TypeOf(backend.runs) != run_store.Store) sortRuns(backend.runs.items);
    if (@hasDecl(BackendType, "bulkIngestActive") and backend.bulkIngestActive()) {
        if (@hasDecl(BackendType, "markManifestDirty")) backend.markManifestDirty();
        return;
    }
    try maybeCompactRuns(BackendType, backend);
    if (@hasDecl(BackendType, "persistManifest")) {
        try backend.persistManifestLocked();
    } else if (backend.root_dir != null) {
        try repository_mod.persistManifestWithStorage(
            backend.storage.?,
            backend.allocator,
            backend.root_dir.?,
            backend.next_run_id,
            (try run_store.oracleItems(backend)),
            backend.obsolete_paths.items,
        );
    }
}

pub fn maybeCompactRuns(comptime BackendType: type, backend: *BackendType) !void {
    if (domainPlanningEnabled(backend)) {
        while (try compactDomainPlan(BackendType, backend, backend.options.compact_threshold_runs, false, false, 0, 0, false)) {}
        return;
    }
    while (selectCompactionPlan(
        (try run_store.oracleItems(backend)),
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
    if (domainPlanningEnabled(backend)) return compactDomainPlan(BackendType, backend, l0_limit, false, true, score, backend.options.max_compaction_input_bytes, allowOversizedSingleCompactionInput(backend));
    if (try compactRememberedPlanIfValid(BackendType, backend, .{ .l0_limit = l0_limit, .l0_only = false, .max_bytes = backend.options.max_compaction_input_bytes, .allow_oversized = allowOversizedSingleCompactionInput(backend) })) return true;

    var selection_stats: CompactionSelectionStats = .{};
    const plan = selectCompactionPlanWithStats(
        (try run_store.oracleItems(backend)),
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

    var work = try compactionWorkForPlan(backend.allocator, &backend.runs, plan, score);
    defer work.deinit(backend.allocator);
    var grant = backend.acquireCompactionGrant(work) orelse {
        rememberDeniedCompaction(BackendType, backend, plan, score);
        return false;
    };
    defer grant.complete();
    try compactPlanAtOptionalMaintenance(BackendType, backend, plan);
    return true;
}

/// Run only the leveled repair/pressure lanes. A size-tiered L0 owner uses
/// this while its bytes remain below the promotion threshold: physical L0
/// partition count must not make an otherwise healthy lower level wait, but
/// neither should it trigger a whole-base L0 -> L1 rewrite.
pub fn maybeCompactLowerLevelsScheduled(
    comptime BackendType: type,
    backend: *BackendType,
    score: u64,
) !bool {
    if (domainPlanningEnabled(backend))
        return compactDomainPlan(BackendType, backend, std.math.maxInt(usize), false, true, score, 0, false);
    var selection_stats: CompactionSelectionStats = .{};
    var best: ?ScoredCompactionPlan = null;
    maybeAdoptBest(&best, selectLowerLevelRepairCompactionCandidateWithStats(
        (try run_store.oracleItems(backend)),
        backend.options.max_compaction_input_bytes,
        allowOversizedSingleCompactionInput(backend),
        &selection_stats,
    ));
    maybeAdoptBest(&best, selectLowerLevelPressureCompactionCandidateWithStats(
        (try run_store.oracleItems(backend)),
        backend.options.level_target_runs_base,
        backend.options.level_target_runs_multiplier,
        backend.options.level_target_bytes_base,
        backend.options.level_target_bytes_multiplier,
        backend.options.max_compaction_input_bytes,
        allowOversizedSingleCompactionInput(backend),
        &selection_stats,
    ));
    noteCompactionSelectionStats(BackendType, backend, selection_stats);
    const plan = if (best) |candidate| candidate.plan else return false;

    var work = try compactionWorkForPlan(backend.allocator, (try run_store.oracleItems(backend)), plan, score);
    defer work.deinit(backend.allocator);
    var grant = backend.acquireCompactionGrant(work) orelse {
        rememberDeniedCompaction(BackendType, backend, plan, score);
        return false;
    };
    defer grant.complete();
    try compactPlanAtOptionalMaintenance(BackendType, backend, plan);
    return true;
}

pub fn compactOldestPair(comptime BackendType: type, backend: *BackendType) !void {
    if (domainPlanningEnabled(backend)) {
        _ = try compactDomainPlan(BackendType, backend, 0, true, false, 0, 0, false);
        return;
    }
    const plan = selectL0Compaction((try run_store.oracleItems(backend)), 0, 0, false) orelse return;
    try compactPlanAt(BackendType, backend, plan);
}

pub fn compactL0ToLimit(comptime BackendType: type, backend: *BackendType, l0_limit: usize) !bool {
    if (domainPlanningEnabled(backend)) {
        return try compactDomainPlan(BackendType, backend, l0_limit, true, false, 0, backend.options.max_compaction_input_bytes, allowOversizedSingleCompactionInput(backend));
    }
    var selection_stats: CompactionSelectionStats = .{};
    const plan = selectL0CompactionWithStats(
        (try run_store.oracleItems(backend)),
        l0_limit,
        backend.options.max_compaction_input_bytes,
        allowOversizedSingleCompactionInput(backend),
        &selection_stats,
    ) orelse {
        noteCompactionSelectionStats(BackendType, backend, selection_stats);
        return false;
    };
    noteCompactionSelectionStats(BackendType, backend, selection_stats);
    try compactPlanAt(BackendType, backend, plan);
    return true;
}

pub fn compactL0ToLimitScheduled(comptime BackendType: type, backend: *BackendType, l0_limit: usize, score: u64) !bool {
    if (domainPlanningEnabled(backend)) return compactDomainPlan(BackendType, backend, l0_limit, true, true, score, backend.options.max_compaction_input_bytes, allowOversizedSingleCompactionInput(backend));
    if (try compactRememberedPlanIfValid(BackendType, backend, .{ .l0_limit = l0_limit, .l0_only = true, .max_bytes = backend.options.max_compaction_input_bytes, .allow_oversized = allowOversizedSingleCompactionInput(backend) })) return true;

    var selection_stats: CompactionSelectionStats = .{};
    const plan = selectL0CompactionWithStats(
        (try run_store.oracleItems(backend)),
        l0_limit,
        backend.options.max_compaction_input_bytes,
        allowOversizedSingleCompactionInput(backend),
        &selection_stats,
    ) orelse {
        noteCompactionSelectionStats(BackendType, backend, selection_stats);
        return false;
    };
    noteCompactionSelectionStats(BackendType, backend, selection_stats);
    var work = try compactionWorkForPlan(backend.allocator, &backend.runs, plan, score);
    defer work.deinit(backend.allocator);
    var grant = backend.acquireCompactionGrant(work) orelse {
        rememberDeniedCompaction(BackendType, backend, plan, score);
        return false;
    };
    defer grant.complete();
    try compactPlanAtOptionalMaintenance(BackendType, backend, plan);
    return true;
}

pub fn compactL0ToLimitScheduledWithinBudget(
    comptime BackendType: type,
    backend: *BackendType,
    l0_limit: usize,
    score: u64,
    max_input_bytes: ?u64,
) !bool {
    // Zero in the public optional budget means no foreground input, whereas
    // zero in the internal planner's non-optional limit means unlimited.
    if (max_input_bytes == 0) return false;
    const option_limit = backend.options.max_compaction_input_bytes;
    const effective_limit = if (max_input_bytes) |explicit_limit|
        if (option_limit > 0) @min(option_limit, explicit_limit) else explicit_limit
    else
        option_limit;
    if (domainPlanningEnabled(backend)) return compactDomainPlan(BackendType, backend, l0_limit, true, true, score, effective_limit, max_input_bytes == null and allowOversizedSingleCompactionInput(backend));
    var selection_stats: CompactionSelectionStats = .{};
    const plan = selectL0CompactionWithStats(
        (try run_store.oracleItems(backend)),
        l0_limit,
        effective_limit,
        max_input_bytes == null and allowOversizedSingleCompactionInput(backend),
        &selection_stats,
    ) orelse {
        noteCompactionSelectionStats(BackendType, backend, selection_stats);
        return false;
    };
    noteCompactionSelectionStats(BackendType, backend, selection_stats);
    var work = try compactionWorkForPlan(backend.allocator, &backend.runs, plan, score);
    defer work.deinit(backend.allocator);
    var grant = backend.acquireCompactionGrant(work) orelse {
        return false;
    };
    defer grant.complete();
    try compactPlanAtOptionalMaintenance(BackendType, backend, plan);
    return true;
}

/// Merge one geometrically compatible set of L0 publication generations back
/// into L0. Unlike leveled L0->L1 compaction this does not repeatedly rewrite
/// the overlapping base during a sustained import. The output inherits the
/// newest selected logical sequence, so intervening newer/older generations
/// retain exactly the same read precedence.
pub fn compactBulkL0TierScheduled(
    comptime BackendType: type,
    backend: *BackendType,
    fan_in: usize,
    score: u64,
) !bool {
    return compactBulkL0TierScheduledBeforeSequence(BackendType, backend, fan_in, score, 0);
}

/// Foreground variant used only to relieve hard L0 run-count pressure. It
/// bypasses optional-maintenance admission/yielding because the write cannot
/// safely leave the hard envelope, while retaining the same input-size bound
/// and geometric-growth contract as the background lane.
pub fn compactBulkL0Tier(
    comptime BackendType: type,
    backend: *BackendType,
    fan_in: usize,
) !bool {
    if (comptime @hasField(BackendType, "pending_bulk_plan"))
        return compactBulkDirectory(backend, .{ .fan_in = fan_in, .max_bytes = backend.options.max_compaction_input_bytes }, false, 0);
    const plan = selectBulkL0Tier(
        &backend.runs,
        fan_in,
        backend.options.max_compaction_input_bytes,
        0,
    ) orelse return false;
    try compactPlanAt(BackendType, backend, plan);
    return true;
}

/// Collapse the fragmented generations newer than the largest L0 anchor into
/// one logical generation without promoting into or rewriting the anchor or
/// lower levels. The output must be at least `min_growth_factor` times its
/// largest selected generation. Consequently a later seal cannot rewrite this
/// output until a comparable amount of newer L0 data has accumulated.
pub fn compactBulkL0DeltaSeal(
    comptime BackendType: type,
    backend: *BackendType,
    min_growth_factor: usize,
) !bool {
    if (comptime @hasField(BackendType, "pending_bulk_plan"))
        return compactBulkDirectory(backend, .{ .mode = .delta, .fan_in = min_growth_factor, .max_bytes = backend.options.max_compaction_input_bytes }, false, 0);
    if (min_growth_factor < 2) return false;
    const l0_count = countLeadingL0Runs(&backend.runs);
    if (l0_count < 2) return false;

    var anchor_start: usize = 0;
    var anchor_bytes: u64 = 0;
    var start: usize = 0;
    while (start < l0_count) {
        const end = l0GenerationEnd(&backend.runs, start, l0_count);
        var generation_bytes: u64 = 0;
        for (start..end) |index| generation_bytes +|= run_store.get(&backend.runs, index).size_bytes;
        if (generation_bytes > anchor_bytes) {
            anchor_start = start;
            anchor_bytes = generation_bytes;
        }
        start = end;
    }

    // Bulk publications prepend newer generations. Treat the largest
    // established generation as an immutable anchor and seal only the newer
    // prefix. An anchor at the front has no newer delta for this lane.
    if (anchor_start == 0) return false;

    var generation_count: usize = 0;
    var max_generation_bytes: u64 = 0;
    var input_bytes: u64 = 0;
    start = 0;
    while (start < anchor_start) {
        const end = l0GenerationEnd(&backend.runs, start, anchor_start);
        var generation_bytes: u64 = 0;
        for (start..end) |index| generation_bytes +|= run_store.get(&backend.runs, index).size_bytes;
        generation_count += 1;
        max_generation_bytes = @max(max_generation_bytes, generation_bytes);
        input_bytes +|= generation_bytes;
        start = end;
    }
    if (generation_count < 2 or
        input_bytes < max_generation_bytes *| @as(u64, @intCast(min_growth_factor))) return false;
    if (backend.options.max_compaction_input_bytes > 0 and
        input_bytes > backend.options.max_compaction_input_bytes) return false;

    try compactPlanAt(BackendType, backend, .{
        .source_level = 0,
        .source_start = 0,
        .source_len = anchor_start,
        .target_start = anchor_start,
        .target_len = 0,
        .output_level = 0,
    });
    return true;
}

/// Variant used while a bulk window is open. A non-zero sequence ceiling
/// keeps the tier merger out of the request publications owned by that
/// window; those are combined exactly once by `compactBulkL0WindowScheduled`.
pub fn compactBulkL0TierScheduledBeforeSequence(
    comptime BackendType: type,
    backend: *BackendType,
    fan_in: usize,
    score: u64,
    before_sequence: u64,
) !bool {
    if (comptime @hasField(BackendType, "pending_bulk_plan"))
        return compactBulkDirectory(backend, .{ .fan_in = fan_in, .max_bytes = backend.options.max_compaction_input_bytes, .sequence = before_sequence }, true, score);
    const plan = selectBulkL0Tier(
        &backend.runs,
        fan_in,
        backend.options.max_compaction_input_bytes,
        before_sequence,
    ) orelse return false;
    var work = try compactionWorkForPlan(backend.allocator, &backend.runs, plan, score);
    defer work.deinit(backend.allocator);
    var grant = backend.acquireCompactionGrant(work) orelse return false;
    defer grant.complete();
    try compactPlanAtOptionalMaintenance(BackendType, backend, plan);
    return true;
}

/// The scheduler asks whether discovery is due, never performs discovery.
/// A negative result is cached for the exact epoch and policy. Actual jobs
/// survive unrelated publications and validate identities before execution.
pub fn bulkDirectoryPlanningDue(backend: anytype, policy: BulkPolicy) bool {
    if (backend.pending_bulk_plan != null) return true;
    if (backend.bulk_plan_negative_generation == backend.run_directory_generation and
        backend.bulk_plan_negative_policy.eql(policy)) return false;
    if (!backend.run_directory_dirty) if (backend.run_directory) |directory|
        return directory.generationCount() >= @max(@as(usize, 2), policy.fan_in);
    return backend.runs.l0Files() >= @max(@as(usize, 2), policy.fan_in);
}

pub const PendingBulkPlan = struct {
    directory: *Directory,
    generation: u64,
    selection: BulkSelection,
    phase: enum { select, emit, validate, done } = .select,
    cursor: ?Directory.Cursor = null,
    handles: ?[]Directory.Handle = null,
    indices: ?[]usize = null,
    emitted: usize = 0,
    released: usize = 0,
    selected: ?SelectedPlan = null,
    validation: ?DependencyValidation = null,
    reservation: ?resource_manager_mod.Reservation = null,
    output_reservation: ?resource_manager_mod.Reservation = null,
    retired_next: ?*@This() = null,
    active_next: ?*@This() = null,

    pub fn accountedMemoryBytes(self: *const @This(), pass: u64) u64 {
        var bytes = self.directory.accountedMemoryBytes(pass);
        if (self.validation) |validation| {
            bytes +|= validation.directory.accountedMemoryBytes(pass);
            if (validation.latest) |latest| bytes +|= latest.accountedMemoryBytes(pass);
        }
        return bytes;
    }

    fn create(backend: anytype, policy: BulkPolicy) !*@This() {
        var credit: ?resource_manager_mod.Reservation = null;
        errdefer if (credit) |*lease| lease.release();
        if (backend.options.resource_manager) |manager|
            credit = try manager.reserve(.lsm_table_builder_working_set, @sizeOf(@This()) + @sizeOf(Directory));
        const directory = try (try backend.planningDirectory()).fork(backend.allocator);
        errdefer directory.destroy(backend.allocator);
        const self = try backend.allocator.create(@This());
        self.* = .{ .directory = directory, .generation = backend.run_directory_generation, .selection = .init(directory, policy), .reservation = credit };
        self.active_next = backend.active_bulk_plans;
        backend.active_bulk_plans = self;
        return self;
    }
    fn advanceLocked(self: *@This(), backend: anytype) !bool {
        if (backend.manifestCoordinationIo()) |io| try io.checkCancel();
        if (self.phase == .done) return true;
        backend.directory_planning_slices +|= 1;
        if (self.phase == .validate) {
            if (self.validation == null) {
                self.validation = try DependencyValidation.init(backend, self.selected.?.plan);
                self.validation.?.job.indices = @constCast(self.selected.?.plan.run_indices.?);
                self.selected.?.plan.run_indices = null;
            }
            const result = try self.validation.?.advanceLocked(backend);
            if (result == .pending and self.validation.?.rebases < 4) return false;
            self.phase = .done;
            if (result == .valid) {
                self.selected.?.plan.run_indices = self.validation.?.takeIndices();
                self.selected.?.plan.complete_coverage = self.validation.?.job.covered;
                self.selected.?.plan.validated_generation = backend.run_directory_generation;
            }
            return true;
        }
        backend.retainReaderKind(.compaction);
        runtime_mod.unlockBackend(@TypeOf(backend.*), backend, true);
        const result = self.step(backend, 2048, @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms);
        _ = runtime_mod.lockBackend(@TypeOf(backend.*), backend);
        backend.releaseReaderKind(.compaction);
        try result;
        return self.phase == .done;
    }
    fn step(self: *@This(), backend: anytype, credits_arg: usize, deadline: u64) !void {
        var credits = credits_arg;
        const allocator = backend.allocator;
        if (self.phase == .select) {
            if (self.selection.step(credits, deadline)) self.phase = if (self.selection.result != null) .emit else .done;
            return;
        }
        const range = self.selection.result.?;
        if (self.handles == null) {
            if (backend.options.resource_manager) |manager|
                self.output_reservation = try manager.reserve(.lsm_table_builder_working_set, range.len * (@sizeOf(Directory.Handle) + @sizeOf(usize)));
            const handles = try allocator.alloc(Directory.Handle, range.len);
            errdefer allocator.free(handles);
            self.indices = try allocator.alloc(usize, range.len);
            self.handles = handles;
            self.cursor = self.directory.readCursor();
            self.cursor.?.rank = range.start;
        }
        while (self.emitted < range.len and credits != 0 and @import("antfly_platform").time.monotonicNs() < deadline) {
            self.handles.?[self.emitted] = self.cursor.?.next().?.retain();
            self.indices.?[self.emitted] = range.start + self.emitted;
            self.emitted += 1;
            credits -= 1;
        }
        if (self.emitted == range.len) {
            self.selected = .{ .plan = .{ .source_level = 0, .source_start = 0, .source_len = range.len, .target_start = range.len, .target_len = 0, .output_level = 0, .run_indices = self.indices, .input_handles = self.handles }, .reservation = self.output_reservation };
            self.output_reservation = null;
            self.handles = null;
            self.indices = null;
            self.emitted = 0;
            self.phase = .validate;
        }
    }
    fn take(self: *@This()) ?SelectedPlan {
        if (self.selected) |selected| if (selected.plan.validated_generation != null) {
            self.selected = null;
            return selected;
        };
        return null;
    }
    pub fn cleanupStep(self: *@This(), allocator: std.mem.Allocator, credits: *usize) bool {
        if (self.validation) |*validation| if (!validation.cleanupStep(allocator, credits)) return false;
        if (self.selected) |*selected| {
            if (!selected.deinitStep(allocator, credits)) return false;
            self.selected = null;
        }
        if (self.handles) |handles| {
            while (self.released < self.emitted and credits.* != 0) {
                handles[self.released].release(allocator);
                self.released += 1;
                credits.* -= 1;
            }
            if (self.released != self.emitted) return false;
            allocator.free(handles);
            self.handles = null;
        }
        if (self.indices) |indices| allocator.free(indices);
        self.indices = null;
        return true;
    }
    pub fn finish(self: *@This(), backend: anytype) void {
        backend.unregisterBulkPlanning(self);
        if (self.validation) |*validation| validation.deinit(backend);
        backend.retireCheckpointDirectory(self.directory);
        if (self.output_reservation) |*lease| lease.release();
        if (self.reservation) |*lease| lease.release();
        backend.allocator.destroy(self);
    }
    pub fn destroy(self: *@This(), backend: anytype) void {
        var credits: usize = std.math.maxInt(usize);
        std.debug.assert(self.cleanupStep(backend.allocator, &credits));
        self.finish(backend);
    }
};

fn compactBulkDirectory(backend: anytype, policy: BulkPolicy, scheduled: bool, score: u64) !bool {
    if (scheduled and backend.bulk_plan_in_flight) return false;
    if (scheduled and !bulkDirectoryPlanningDue(backend, policy)) return false;
    if (scheduled) if (backend.pending_bulk_plan) |pending| if (!pending.selection.policy.eql(policy)) {
        backend.retireBulkPlanning(pending);
        backend.pending_bulk_plan = null;
    };
    const pending = if (scheduled and backend.pending_bulk_plan != null) backend.pending_bulk_plan.? else try PendingBulkPlan.create(backend, policy);
    if (scheduled) backend.pending_bulk_plan = pending;
    if (scheduled) backend.bulk_plan_in_flight = true;
    defer if (scheduled) {
        backend.bulk_plan_in_flight = false;
    };
    var retire = !scheduled;
    defer if (retire) {
        if (scheduled) backend.pending_bulk_plan = null;
        backend.retireBulkPlanning(pending);
    };
    errdefer retire = true;
    while (!try pending.advanceLocked(backend)) {
        if (scheduled) return true; // A discovery quantum is scheduling progress.
        if (backend.manifestCoordinationIo()) |io| {
            backend.retainReaderKind(.compaction);
            runtime_mod.unlockBackend(@TypeOf(backend.*), backend, true);
            const yielded = io.sleep(.fromNanoseconds(1), .awake);
            _ = runtime_mod.lockBackend(@TypeOf(backend.*), backend);
            backend.releaseReaderKind(.compaction);
            try yielded;
        }
    }
    retire = true;
    var selected = pending.take() orelse {
        if (scheduled and pending.selection.result == null and pending.generation == backend.run_directory_generation) {
            backend.bulk_plan_negative_generation = pending.generation;
            backend.bulk_plan_negative_policy = policy;
        }
        return false;
    };
    defer selected.release(backend);
    if (scheduled or policy.mode == .window) {
        var work = try compactionWorkForPlan(backend.allocator, &backend.runs, selected.plan, score);
        defer work.deinit(backend.allocator);
        var grant = backend.acquireCompactionGrant(work) orelse return false;
        defer grant.complete();
        try compactPlanAtOptionalMaintenance(@TypeOf(backend.*), backend, selected.plan);
    } else try compactPlanAt(@TypeOf(backend.*), backend, selected.plan);
    return true;
}

test "bulk publication summaries match the flat oracle across budgets fences and epoch changes" {
    const allocator = std.testing.allocator;
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
    };
    var fixture = Fixture{ .allocator = allocator };
    for (0..24) |variant| {
        const directory = try Directory.create(allocator);
        defer directory.destroy(allocator);
        var runs: std.ArrayListUnmanaged(Run) = .empty;
        defer runs.deinit(allocator);
        for (0..12) |group| for (0..1 + (group + variant) % 5) |file| {
            const keys = [_][]const u8{ "a", "b", "c", "d", "e" };
            var run = testRun(group * 8 + file + 1, 0, keys[file], keys[file], if (variant == 0) 0 else (1 + (group * 13 + variant * 7) % 19) * 100);
            run.visibility_id = (group + 1) * 8;
            run.tombstone_count = 0;
            run.path = @constCast("bulk-fixture.sst");
            try directory.put(&fixture, run);
            try runs.append(allocator, run);
        };
        sortRuns(runs.items);
        try std.testing.expectEqual(@as(usize, 12), directory.generationCount());
        try std.testing.expectEqual(runs.items.len, directory.generationPrefix(12).files);
        for ([_]usize{ 2, 4, 8 }) |fan_in| for ([_]u64{ 0, 2000, 10000 }) |limit| for ([_]u64{ 0, 48, 49 }) |ceiling| {
            const expected = selectBulkL0Tier(runs.items, fan_in, limit, ceiling);
            var job = BulkSelection.init(directory, .{ .fan_in = fan_in, .max_bytes = limit, .sequence = ceiling });
            try std.testing.expect(!job.step(0, std.math.maxInt(u64)));
            try std.testing.expect(!job.step(1, 0));
            while (true) {
                const before = job.visits;
                const done = job.step(1, std.math.maxInt(u64));
                try std.testing.expect(job.visits - before <= 1);
                if (done) break;
            }
            try std.testing.expectEqual(expected == null, job.result == null);
            if (expected) |plan| {
                try std.testing.expectEqual(plan.source_start, job.result.?.start);
                try std.testing.expectEqual(plan.source_len, job.result.?.len);
            }
        };
        const pinned = try directory.fork(allocator);
        defer pinned.destroy(allocator);
        const removed = directory.at(0).run.*;
        try directory.remove(allocator, &removed);
        try std.testing.expectEqual(runs.items.len, pinned.generationPrefix(12).files);
        try std.testing.expectEqual(runs.items.len - 1, directory.generationPrefix(12).files);
        var moved = removed;
        moved.level = 1;
        try directory.put(&fixture, moved);
        try std.testing.expectEqual(runs.items.len - 1, directory.generationPrefix(12).files);
    }
}

test "bulk publication no-op scheduling scaling benchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    const allocator = std.heap.smp_allocator;
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
    };
    var fixture = Fixture{ .allocator = allocator };
    const clock = @import("antfly_platform").time;
    for ([_]usize{ 1000, 10000, 50000 }) |count| {
        const directory = try Directory.create(allocator);
        defer directory.destroy(allocator);
        var owner: run_store.Store = .{};
        defer owner.deinit(allocator);
        for (0..count) |i| {
            var key: [8]u8 = undefined;
            std.mem.writeInt(u64, &key, i, .big);
            var run = testRun(i + 1, 0, &key, &key, 1024);
            run.visibility_id = count;
            run.tombstone_count = 0;
            run.path = @constCast("bulk-benchmark.sst");
            try directory.put(&fixture, run);
            // The oracle only inspects level/sequence/bytes, not key bytes.
            run.smallest_key = &.{};
            run.largest_key = &.{};
            run.owns_metadata = false;
            try owner.append(allocator, run);
        }
        var started = clock.monotonicNs();
        for (0..20) |_| try std.testing.expect(!hasBulkL0Tier(&owner, 4));
        const old_ns = (clock.monotonicNs() - started) / 20;
        var scheduler = .{ .pending_bulk_plan = @as(?*PendingBulkPlan, null), .bulk_plan_negative_generation = @as(?u64, null), .bulk_plan_negative_policy = BulkPolicy{}, .run_directory_generation = @as(u64, 1), .run_directory_dirty = false, .run_directory = @as(?*Directory, directory), .runs = &owner };
        started = clock.monotonicNs();
        for (0..1000000) |_| {
            std.mem.doNotOptimizeAway(&scheduler);
            try std.testing.expect(!bulkDirectoryPlanningDue(&scheduler, .{}));
        }
        const new_ns = @as(f64, @floatFromInt(clock.monotonicNs() - started)) / 1000000;
        var job = BulkSelection.init(directory, .{});
        try std.testing.expect(job.step(1, std.math.maxInt(u64)));
        try std.testing.expect(job.result == null);
        std.debug.print("bulk-summary files={d} old_check_ns={d} scheduler_check_ns={d:.3} generations={d} discovery_visits={d} summary_bytes={d}\n", .{ count, old_ns, new_ns, directory.generationCount(), job.visits, directory.generations.memoryBytes() });
    }
}

test "bulk publication large generation discovery is time sliced" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    const allocator = std.heap.smp_allocator;
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
    };
    var fixture = Fixture{ .allocator = allocator };
    const directory = try Directory.create(allocator);
    defer directory.destroy(allocator);
    for (0..10000) |i| {
        var run = testRun(i + 1, 0, "a", "z", 1024);
        run.path = @constCast("bulk-generations.sst");
        run.tombstone_count = 0;
        try directory.put(&fixture, run);
    }
    const clock = @import("antfly_platform").time;
    var job = BulkSelection.init(directory, .{});
    var slices: usize = 0;
    var max_ns: u64 = 0;
    const started = clock.monotonicNs();
    while (true) {
        const before = clock.monotonicNs();
        const visits = job.visits;
        const done = job.step(2048, before +| 2 * std.time.ns_per_ms);
        try std.testing.expect(job.visits - visits <= 2048);
        max_ns = @max(max_ns, clock.monotonicNs() - before);
        slices += 1;
        if (done) break;
    }
    try std.testing.expect(slices > 1);
    try std.testing.expectEqual(@as(usize, 9996), job.result.?.start);
    try std.testing.expectEqual(@as(usize, 4), job.result.?.len);
    std.debug.print("bulk-discovery generations=10000 visits={d} slices={d} max_slice_ns={d} total_ns={d}\n", .{ job.visits, slices, max_ns, clock.monotonicNs() - started });
}

test "bulk publication admission and partial cleanup tolerate every allocation failure" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const allocator = std.testing.allocator;
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
        fn check(alloc: std.mem.Allocator, source: *const Directory) !void {
            var backend = Backend.init(alloc, .{});
            defer backend.close();
            backend.run_directory = try source.fork(alloc);
            backend.run_directory_dirty = false;
            var cursor = source.readCursor();
            while (cursor.next()) |handle| {
                var borrowed = handle.run.*;
                borrowed.owns_metadata = false;
                borrowed.owns_path = false;
                try backend.runs.append(alloc, borrowed);
            }
            const locked = runtime_mod.lockBackend(Backend, &backend);
            defer runtime_mod.unlockBackend(Backend, &backend, locked);
            const pending = try PendingBulkPlan.create(&backend, .{ .fan_in = 2 });
            defer pending.destroy(&backend);
            var turns: usize = 0;
            while (!try pending.advanceLocked(&backend)) {
                turns += 1;
                if (turns > 100) return error.TestUnexpectedResult;
            }
            try std.testing.expect(pending.selected != null);
            while (true) {
                var credits: usize = 1;
                if (pending.cleanupStep(alloc, &credits)) break;
            }
        }
    };
    var fixture = Fixture{ .allocator = allocator };
    const directory = try Directory.create(allocator);
    defer directory.destroy(allocator);
    for (0..4) |i| {
        var run = testRun(i + 1, 0, "a", "z", 100);
        run.path = @constCast("bulk-allocation.sst");
        run.tombstone_count = 0;
        try directory.put(&fixture, run);
    }
    try std.testing.checkAllAllocationFailures(allocator, Fixture.check, .{directory});
}

pub fn hasBulkL0Tier(runs: anytype, fan_in: usize) bool {
    return selectBulkL0Tier(runs, fan_in, 0, 0) != null;
}

pub fn hasBulkL0TierBeforeSequence(runs: anytype, fan_in: usize, before_sequence: u64) bool {
    return selectBulkL0Tier(runs, fan_in, 0, before_sequence) != null;
}

fn selectBulkL0Tier(runs: anytype, fan_in: usize, max_input_bytes: u64, before_sequence: u64) ?CompactionPlan {
    if (fan_in < 2) return null;
    const l0_count = countLeadingL0Runs(runs);
    if (l0_count < fan_in) return null;

    var best: ?CompactionPlan = null;
    var candidate_start: usize = 0;
    while (candidate_start < l0_count) {
        const first_end = l0GenerationEnd(runs, candidate_start, l0_count);
        if (before_sequence != 0 and l0Sequence(run_store.get(runs, candidate_start)) >= before_sequence) {
            candidate_start = first_end;
            continue;
        }
        var source_end = candidate_start;
        var group_count: usize = 0;
        var max_group_bytes: u64 = 0;
        var input_bytes: u64 = 0;
        while (source_end < l0_count) {
            const group_end = l0GenerationEnd(runs, source_end, l0_count);
            var group_bytes: u64 = 0;
            for (source_end..group_end) |index| group_bytes +|= run_store.get(runs, index).size_bytes;
            group_count += 1;
            max_group_bytes = @max(max_group_bytes, group_bytes);
            input_bytes +|= group_bytes;
            source_end = group_end;
            const geometric_target = max_group_bytes *| @as(u64, @intCast(fan_in));
            const geometric = group_count >= fan_in and input_bytes >= geometric_target;
            const within_budget = max_input_bytes == 0 or input_bytes <= max_input_bytes;
            if (geometric and within_budget) break;
            if (!within_budget) break;
        }
        const geometric_target = max_group_bytes *| @as(u64, @intCast(fan_in));
        const geometric = group_count >= fan_in and
            max_group_bytes > 0 and
            input_bytes >= geometric_target;
        const within_budget = max_input_bytes == 0 or input_bytes <= max_input_bytes;
        if (geometric and within_budget) {
            // Keep walking so the oldest compatible window wins. Rewriting
            // older tiers first bounds read amplification without disturbing
            // the chronology of newer generations. Requiring the output to
            // be at least `fan_in` times every input prevents an uneven stream
            // from degenerating into repeated two-way rewrites.
            best = .{
                .source_level = 0,
                .source_start = candidate_start,
                .source_len = source_end - candidate_start,
                .target_start = source_end,
                .target_len = 0,
                .output_level = 0,
            };
        }
        candidate_start = first_end;
    }
    return best;
}

/// Collapse every publication created by one durable bulk window into a
/// single logical L0 generation. The existing persisted k-way merger streams
/// the inputs, so memory is bounded by cursors and one output file rather than
/// by the size of the window. A crash before publication leaves the original
/// manifest authoritative.
pub fn compactBulkL0WindowScheduled(
    comptime BackendType: type,
    backend: *BackendType,
    first_sequence: u64,
    score: u64,
) !bool {
    // Closing a durable window drains its own continuation; it must neither
    // steal the background slot nor report success before that window settles.
    if (comptime @hasField(BackendType, "pending_bulk_plan"))
        return compactBulkDirectory(backend, .{ .mode = .window, .sequence = first_sequence, .max_bytes = backend.options.max_compaction_input_bytes }, false, score);
    if (first_sequence == 0) return false;
    const l0_count = countLeadingL0Runs(&backend.runs);
    var source_len: usize = 0;
    var generation_count: usize = 0;
    var prior_sequence: u64 = 0;
    while (source_len < l0_count) : (source_len += 1) {
        const sequence = l0Sequence(run_store.get(&backend.runs, source_len));
        if (sequence < first_sequence) break;
        if (sequence != prior_sequence) {
            generation_count += 1;
            prior_sequence = sequence;
        }
    }
    if (generation_count < 2) return false;

    const plan: CompactionPlan = .{
        .source_level = 0,
        .source_start = 0,
        .source_len = source_len,
        .target_start = source_len,
        .target_len = 0,
        .output_level = 0,
    };
    var work = try compactionWorkForPlan(backend.allocator, &backend.runs, plan, score);
    defer work.deinit(backend.allocator);
    if (backend.options.max_compaction_input_bytes > 0 and
        work.input_bytes > backend.options.max_compaction_input_bytes) return false;
    var grant = backend.acquireCompactionGrant(work) orelse return false;
    defer grant.complete();
    try compactPlanAtOptionalMaintenance(BackendType, backend, plan);
    return true;
}

fn l0GenerationEnd(runs: anytype, start: usize, l0_count: usize) usize {
    const sequence = l0Sequence(run_store.get(runs, start));
    var end = start + 1;
    while (end < l0_count and l0Sequence(run_store.get(runs, end)) == sequence) : (end += 1) {}
    return end;
}

pub fn compactAllRuns(comptime BackendType: type, backend: *BackendType) !void {
    if (domainPlanningEnabled(backend)) {
        while (try compactDomainPlan(BackendType, backend, 0, false, false, 0, 0, false)) {}
        return;
    }
    while (selectCompactionPlan(
        (try run_store.oracleItems(backend)),
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

fn compactionWorkForPlan(allocator: std.mem.Allocator, runs: anytype, plan: CompactionPlan, score: u64) !CompactionWork {
    const total_runs = plan.source_len + plan.target_len;
    const run_ids = try allocator.alloc(u64, total_runs);
    errdefer allocator.free(run_ids);

    var input_runs: usize = 0;
    var input_bytes: u64 = 0;
    var run_count: usize = 0;
    var key_range: ?compaction_scheduler_mod.KeyRange = null;
    for (0..plan.source_len) |i| {
        const run = run_store.planGet(runs, plan, i);
        input_runs += 1;
        input_bytes +|= run.size_bytes;
        includeRunInWorkKeyRange(&key_range, plan.output_level, run);
        run_ids[run_count] = run.id;
        run_count += 1;
    }
    for (0..plan.target_len) |i| {
        const run = run_store.planGet(runs, plan, plan.source_len + i);
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

fn compactionInputBytes(runs: anytype, plan: CompactionPlan) u64 {
    var input_bytes: u64 = 0;
    for (0..plan.source_len) |i| {
        const run = run_store.planGet(runs, plan, i);
        input_bytes +|= run.size_bytes;
    }
    for (0..plan.target_len) |i| {
        const run = run_store.planGet(runs, plan, plan.source_len + i);
        input_bytes +|= run.size_bytes;
    }
    return input_bytes;
}

fn planScoreForPlan(runs: []const Run, plan: CompactionPlan) PlanScore {
    var source_bytes: u64 = 0;
    var target_bytes: u64 = 0;
    for (0..plan.source_len) |i| source_bytes +|= runs[plan.sourceIndex(i)].size_bytes;
    for (0..plan.target_len) |i| target_bytes +|= runs[plan.targetIndex(i)].size_bytes;
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

fn compactRememberedPlanIfValid(comptime BackendType: type, backend: *BackendType, policy: PlanningPolicy) !bool {
    if (!@hasField(BackendType, "remembered_compaction")) return false;
    const remembered = backend.remembered_compaction orelse return false;
    backend.compaction_scheduler.noteRememberedRetry();

    const plan = validateRememberedCompaction(&backend.runs, remembered) orelse {
        backend.remembered_compaction = null;
        backend.compaction_scheduler.noteRememberedStale();
        return false;
    };

    var work = try compactionWorkForPlan(backend.allocator, &backend.runs, plan, remembered.score);
    defer work.deinit(backend.allocator);
    if (!policy.admits(plan, work.input_bytes)) {
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
    try compactPlanAtOptionalMaintenance(BackendType, backend, plan);
    return true;
}

fn rememberDeniedCompaction(comptime BackendType: type, backend: *BackendType, plan: CompactionPlan, score: u64) void {
    if (!@hasField(BackendType, "remembered_compaction")) return;
    const remembered = rememberCompactionPlan(&backend.runs, plan, score) orelse return;
    backend.remembered_compaction = remembered;
    backend.compaction_scheduler.noteRememberedCandidate();
}

fn rememberCompactionPlan(runs: anytype, plan: CompactionPlan, score: u64) ?RememberedCompaction {
    if (comptime @TypeOf(runs) == *run_store.Store or @TypeOf(runs) == *const run_store.Store) {
        if (plan.input_handles) |handles| {
            if (handles.len > max_remembered_compaction_run_ids) return null;
            var ranks: [max_remembered_compaction_run_ids]usize = undefined;
            for (handles, 0..) |handle, i| ranks[i] = runs.rankOf(handle.run) orelse return null;
            var current = plan;
            current.input_handles = null;
            current.run_indices = ranks[0..handles.len];
            return rememberCompactionPlan(runs, current, score);
        }
    }
    if (plan.run_indices != null and plan.partition_key == null) {
        // A directory-selected contiguous window can use the fixed-size retry
        // record without retaining either its scratch or an entire epoch.
        var contiguous = plan;
        contiguous.source_start = plan.sourceIndex(0);
        contiguous.target_start = if (plan.target_len != 0) plan.targetIndex(0) else contiguous.source_start + plan.source_len;
        for (0..plan.source_len) |i| if (plan.sourceIndex(i) != contiguous.source_start + i) return null;
        for (0..plan.target_len) |i| if (plan.targetIndex(i) != contiguous.target_start + i) return null;
        contiguous.run_indices = null;
        contiguous.input_handles = null;
        return rememberCompactionPlan(runs, contiguous, score);
    }
    // Domain plans own a transient mapping. A denied job is reselected from
    // the latest version instead of retaining pointers into planning scratch.
    if (plan.run_indices != null) return null;
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
    for (plan.source_start..plan.source_start + plan.source_len) |rank| {
        const run = run_store.get(runs, rank);
        remembered.run_ids[idx] = run.id;
        idx += 1;
    }
    for (plan.target_start..plan.target_start + plan.target_len) |rank| {
        const run = run_store.get(runs, rank);
        remembered.run_ids[idx] = run.id;
        idx += 1;
    }
    return remembered;
}

fn validateRememberedCompaction(runs: anytype, remembered: RememberedCompaction) ?CompactionPlan {
    const plan = remembered.plan;
    if (remembered.run_count == 0 or remembered.run_count != plan.source_len + plan.target_len) return null;
    if (!planInBounds(runs, plan)) return null;

    var idx: usize = 0;
    for (plan.source_start..plan.source_start + plan.source_len) |rank| {
        const run = run_store.get(runs, rank);
        if (idx >= remembered.run_count or run.id != remembered.run_ids[idx]) return null;
        idx += 1;
    }
    for (plan.target_start..plan.target_start + plan.target_len) |rank| {
        const run = run_store.get(runs, rank);
        if (idx >= remembered.run_count or run.id != remembered.run_ids[idx]) return null;
        idx += 1;
    }
    return plan;
}

fn planInBounds(runs: anytype, plan: CompactionPlan) bool {
    if (plan.source_len == 0) return false;
    const len = if (plan.run_indices) |indices| indices.len else run_store.len(runs);
    if (plan.source_start > len or plan.source_len > len - plan.source_start) return false;
    if (plan.target_start > len or plan.target_len > len - plan.target_start) return false;
    if (plan.run_indices) |indices| for (indices) |index| if (index >= run_store.len(runs)) return false;
    return true;
}

pub fn compactOldestWindow(comptime BackendType: type, backend: *BackendType, window_len: usize) !void {
    _ = window_len;
    if (domainPlanningEnabled(backend)) {
        _ = try compactDomainPlan(BackendType, backend, 0, true, false, 0, 0, false);
        return;
    }
    const plan = selectL0Compaction((try run_store.oracleItems(backend)), 0, 0, false) orelse return;
    try compactPlanAt(BackendType, backend, plan);
}

pub fn sortRuns(runs: []Run) void {
    std.sort.pdq(Run, runs, {}, struct {
        fn lessThan(_: void, lhs: Run, rhs: Run) bool {
            if (lhs.level != rhs.level) return lhs.level < rhs.level;
            if (lhs.level == 0) {
                const lhs_visibility = if (lhs.visibility_id == 0) lhs.id else lhs.visibility_id;
                const rhs_visibility = if (rhs.visibility_id == 0) rhs.id else rhs.visibility_id;
                if (lhs_visibility != rhs_visibility) return lhs_visibility > rhs_visibility;
                return compareRunBound(lhs.smallest_namespace_name, lhs.smallest_key, rhs.smallest_namespace_name, rhs.smallest_key) == .lt;
            }
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

pub fn l0Sequence(run: Run) u64 {
    return if (run.visibility_id != 0) run.visibility_id else run.id;
}

pub fn setL0Sequence(runs: []Run, sequence: u64) void {
    for (runs) |*run| {
        if (run.level == 0) run.visibility_id = sequence;
    }
}

fn compactPlanAt(comptime BackendType: type, backend: *BackendType, plan: CompactionPlan) !void {
    return try compactPlanAtWithForegroundPolicy(BackendType, backend, plan, false);
}

fn compactPlanAtOptionalMaintenance(comptime BackendType: type, backend: *BackendType, plan: CompactionPlan) !void {
    return try compactPlanAtWithForegroundPolicy(BackendType, backend, plan, true);
}

fn planSelectsIndex(plan: CompactionPlan, index: usize) bool {
    if (plan.run_indices) |indices| {
        const ranges = [_][]const usize{ indices[plan.source_start..][0..plan.source_len], indices[plan.target_start..][0..plan.target_len] };
        for (ranges) |range| {
            var lo: usize = 0;
            var hi = range.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (range[mid] < index) lo = mid + 1 else hi = mid;
            }
            if (lo < range.len and range[lo] == index) return true;
        }
        return false;
    }
    return (index >= plan.source_start and index - plan.source_start < plan.source_len) or
        (index >= plan.target_start and index - plan.target_start < plan.target_len);
}

fn planHasCompleteCoverage(runs: anytype, plan: CompactionPlan) bool {
    if (!planInBounds(runs, plan)) return false;
    var smallest = run_store.planGet(runs, plan, 0);
    var largest = smallest;
    for (0..run_store.len(runs)) |i| {
        if (!planSelectsIndex(plan, i)) continue;
        const run = run_store.get(runs, i);
        if (compareRunBound(run.smallest_namespace_name, run.smallest_key, smallest.smallest_namespace_name, smallest.smallest_key) == .lt) smallest = run;
        if (compareRunBound(run.largest_namespace_name, run.largest_key, largest.largest_namespace_name, largest.largest_key) == .gt) largest = run;
    }
    for (0..run_store.len(runs)) |i| {
        const run = run_store.get(runs, i);
        if (planSelectsIndex(plan, i)) continue;
        // Smaller levels (and earlier L0 runs) are newer than every selected
        // source. Their continued existence cannot reveal an older value.
        if (run.level < plan.source_level or (plan.source_level == 0 and run.level == 0 and i < plan.sourceIndex(0))) continue;
        if (compareRunBound(run.largest_namespace_name, run.largest_key, smallest.smallest_namespace_name, smallest.smallest_key) != .lt and
            compareRunBound(run.smallest_namespace_name, run.smallest_key, largest.largest_namespace_name, largest.largest_key) != .gt) return false;
    }
    return true;
}

fn compactPlanAtWithForegroundPolicy(comptime BackendType: type, backend: *BackendType, initial_plan: CompactionPlan, yield_for_foreground_queries: bool) !void {
    var plan = initial_plan;
    var validated: ?SelectedPlan = null;
    defer if (validated) |selected| selected.release(backend);
    if (comptime @hasDecl(BackendType, "planningDirectory")) {
        if (plan.input_handles != null) {
            if (plan.validated_generation != null and plan.validated_generation.? == backend.run_directory_generation) {
                std.debug.assert(plan.complete_coverage != null);
            } else if ((try backend.planningDirectory()).tombstoneRunCount() == 0 or plan.split_gc) {
                plan.complete_coverage = false;
            } else {
                validated = try relocateDirectoryPlan(backend, plan);
                const selected = validated orelse return;
                plan.run_indices = selected.plan.run_indices;
                plan.complete_coverage = selected.complete_coverage;
            }
        }
    }
    if (!plan.tombstone_gc and !plan.split_gc and plan.partition_key != null and plan.source_len == 1 and plan.target_len == 0 and
        ((run_store.planAt(backend, plan, 0).*.tombstone_count orelse 0) == 0 or !(plan.complete_coverage orelse planHasCompleteCoverage(&backend.runs, plan))))
    {
        // A closed, nonoverlapping domain needs only a manifest-level move.
        // SST bytes and file identity are immutable; do not decode/re-encode
        // a cold payload just to change its level.
        var metadata_credit = if (comptime @hasDecl(BackendType, "admitCompactionMetadata")) try backend.admitCompactionMetadata(plan, &.{run_store.planAt(backend, plan, 0).*}) else {};
        defer if (comptime @hasDecl(BackendType, "admitCompactionMetadata")) metadata_credit.release();
        const directory = if (comptime @hasDecl(BackendType, "prepareRunDirectoryMove")) try backend.prepareRunDirectoryMove(run_store.planAt(backend, plan, 0), plan.output_level) else null;
        errdefer if (directory) |root| root.destroy(backend.allocator);
        if (@hasDecl(BackendType, "invalidateReadVersion")) backend.invalidateReadVersion();
        const run = run_store.planAt(backend, plan, 0);
        const bytes = run.size_bytes;
        if (comptime @TypeOf(backend.runs) == run_store.Store) {
            var moved = run.*;
            moved.level = plan.output_level;
            const retired = try backend.allocator.create(run_store.Store);
            errdefer backend.allocator.destroy(retired);
            const candidate = try backend.runs.prepareReplace(backend.allocator, run, moved);
            retired.* = backend.runs;
            backend.runs = candidate;
            backend.retireRunStore(retired);
        } else {
            run.level = plan.output_level;
            if (comptime @TypeOf(backend.runs) != run_store.Store) sortRuns(backend.runs.items);
        }
        if (comptime @hasDecl(BackendType, "publishRunDirectory")) backend.publishRunDirectory(directory);
        if (comptime @hasDecl(BackendType, "admitCompactionMetadata")) metadata_credit.commit();
        if (@hasDecl(BackendType, "markManifestDirty")) backend.markManifestDirty();
        if (@hasField(BackendType, "compaction_stats")) {
            backend.compaction_stats.compactions += 1;
            backend.compaction_stats.input_runs += 1;
            backend.compaction_stats.input_bytes +|= bytes;
            backend.compaction_stats.output_bytes +|= bytes;
        }
        return;
    }
    if (comptime supportsUnlockedBackendCompaction(BackendType)) {
        try compactPlanAtWithUnlockedBuild(
            BackendType,
            backend,
            plan,
            yield_for_foreground_queries,
        );
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
    for (0..plan.source_len) |i| {
        const run = run_store.planAt(backend, plan, i);
        selected[selected_len] = run;
        selected_len += 1;
    }
    for (0..plan.target_len) |i| {
        const run = run_store.planAt(backend, plan, plan.source_len + i);
        selected[selected_len] = run;
        selected_len += 1;
    }
    const input_bytes = sumRunPtrBytes(selected[0..selected_len]);

    const drop_tombstones = !plan.split_gc and (plan.complete_coverage orelse planHasCompleteCoverage(&backend.runs, plan));
    const split_start = backend.next_run_id;
    if (plan.split_gc) backend.next_run_id +|= countRunPtrEntries(selected[0..selected_len]);
    var compacted_runs = if (plan.split_gc)
        try buildCompactedRunsFromSnapshots(BackendType, backend, selected[0..selected_len], plan.output_level, split_start, backend.next_run_id, false, true)
    else if (backend.root_dir != null)
        try makePersistedRunsFromSelectedRunsWithGc(BackendType, backend, selected[0..selected_len], plan.output_level, drop_tombstones)
    else
        try makeStateRunsFromSelectedRuns(BackendType, backend, selected[0..selected_len], plan.output_level, drop_tombstones);
    inheritTombstoneAge(compacted_runs.items, selected[0..selected_len]);
    errdefer discardOutputRuns(BackendType, backend, &compacted_runs);
    inheritL0Sequence(&compacted_runs, selected[0..selected_len], plan.output_level);
    if (comptime @TypeOf(backend.runs) == run_store.Store) {
        return installCompactedRuns(BackendType, backend, plan, selected_len, input_bytes, start_ns, &compacted_runs);
    }

    var retained = std.ArrayListUnmanaged(Run).empty;
    errdefer {
        for (retained.items) |*run| run.deinit(backend.allocator);
        retained.deinit(backend.allocator);
    }
    try retained.ensureTotalCapacity(backend.allocator, run_store.count(backend) - selected_len + compacted_runs.items.len);

    var obsolete_runs = std.ArrayListUnmanaged(Run).empty;
    errdefer {
        for (obsolete_runs.items) |*run| run.deinit(backend.allocator);
        obsolete_runs.deinit(backend.allocator);
    }
    try obsolete_runs.ensureTotalCapacity(backend.allocator, selected_len);

    var remove = try backend.allocator.alloc(bool, run_store.count(backend));
    defer backend.allocator.free(remove);
    @memset(remove, false);
    for (0..plan.source_len) |i| remove[plan.sourceIndex(i)] = true;
    for (0..plan.target_len) |i| remove[plan.targetIndex(i)] = true;

    // Prepare every allocation before transferring ownership from the active
    // version. A failed compaction publication must leave both the live run
    // set and its obsolete-file bookkeeping untouched.
    var obsolete_paths = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (obsolete_paths.items) |path| backend.allocator.free(path);
        obsolete_paths.deinit(backend.allocator);
    }
    try obsolete_paths.ensureTotalCapacity(backend.allocator, selected_len);
    for (backend.runs.items, remove) |run, removed| {
        if (!removed) continue;
        if (run.path) |path| obsolete_paths.appendAssumeCapacity(try backend.allocator.dupe(u8, path));
    }
    try backend.reserveObsoletePublication(obsolete_paths.items.len, @intFromBool(selected_len > 0));

    reconcileGcObjective(&backend.runs, plan, compacted_runs.items);
    const directory = if (comptime @hasDecl(BackendType, "prepareRunDirectoryChange")) try backend.prepareRunDirectoryChange(plan, compacted_runs.items) else null;

    for (backend.runs.items, 0..) |*run, i| {
        if (remove[i]) {
            obsolete_runs.appendAssumeCapacity(run.*);
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
        backend.recordCompactionWriteStats(input_bytes, compacted_runs.items, elapsed_ns);
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

    if (@hasDecl(BackendType, "invalidateReadVersion")) backend.invalidateReadVersion();
    backend.runs.deinit(backend.allocator);
    backend.runs = retained;
    retained = .empty;
    if (comptime @hasDecl(BackendType, "publishRunDirectory")) backend.publishRunDirectory(directory);
    for (obsolete_paths.items) |*path| {
        backend.queueObsoleteFilePathAssumeCapacity(path.*);
        path.* = &.{};
    }
    obsolete_paths.items.len = 0;
    backend.queueObsoleteRunsAssumeCapacity(obsolete_runs);
    obsolete_runs = .empty;
}

fn compactPlanAtWithUnlockedBuild(comptime BackendType: type, backend: *BackendType, initial_plan: CompactionPlan, yield_for_foreground_queries: bool) !void {
    var plan = initial_plan;
    var normalized_handles: ?[]Directory.Handle = null;
    var normalized_indices: ?[]usize = null;
    defer {
        if (normalized_handles) |handles| {
            for (handles) |handle| handle.release(backend.allocator);
            backend.allocator.free(handles);
        }
        if (normalized_indices) |indices| backend.allocator.free(indices);
    }
    if (comptime @hasDecl(BackendType, "planningDirectory")) {
        if (plan.input_handles == null and plan.source_len != 0) {
            const directory = try backend.planningDirectory();
            const indices = try backend.allocator.alloc(usize, plan.source_len + plan.target_len);
            normalized_indices = indices;
            const handles = try backend.allocator.alloc(Directory.Handle, indices.len);
            for (indices, handles, 0..) |*index, *handle, i| {
                index.* = if (i < plan.source_len) plan.sourceIndex(i) else plan.targetIndex(i - plan.source_len);
                handle.* = directory.at(index.*).retain();
            }
            normalized_handles = handles;
            plan.source_start = 0;
            plan.target_start = plan.source_len;
            plan.run_indices = indices;
            plan.input_handles = handles;
        }
    }
    if (plan.source_len == 0) return;
    const drop_tombstones = !plan.split_gc and (plan.complete_coverage orelse planHasCompleteCoverage(&backend.runs, plan));
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
        drop_tombstones,
        plan.split_gc,
        yield_for_foreground_queries,
    ) catch |err| blk: {
        build_err = err;
        break :blk .empty;
    };
    if (build_err == null) {
        inheritL0Sequence(&build_result, selected, plan.output_level);
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

    const domain_plan = if (plan.input_handles != null and comptime @hasDecl(BackendType, "planningDirectory"))
        try relocateDirectoryPlan(backend, plan)
    else if (plan.tombstone_gc or plan.split_gc)
        try relocateGcPlan(backend.allocator, (try run_store.oracleItems(backend)), plan, selected_runs.items)
    else if (plan.partition_key != null) try relocateDomainPlan(backend.allocator, (try run_store.oracleItems(backend)), plan, selected_run_ids) else null;
    defer if (domain_plan) |selected_plan| selected_plan.deinit(backend.allocator);
    const publish_plan = (if (plan.input_handles != null or plan.partition_key != null or plan.tombstone_gc or plan.split_gc)
        if (domain_plan) |selected_plan| selected_plan.plan else null
    else
        relocatePlanIfInputsStillMatch((try run_store.oracleItems(backend)), plan, selected_run_ids)) orelse {
        if (@hasDecl(BackendType, "releaseReaderKind")) backend.releaseReaderKind(.compaction) else backend.releaseReader();
        reader_retained = false;
        discardOutputRuns(BackendType, backend, &build_result);
        releaseCompactionSnapshots(BackendType, backend, &selected_runs);
        return;
    };

    // Revalidate all older persisted data, not only the target-level closure.
    // Concurrent newer L0 publication is harmless, but an older overlapping
    // value outside the selected inputs must prevent delete elision.
    const complete_coverage = if (domain_plan) |selected_plan| selected_plan.complete_coverage orelse planHasCompleteCoverage(&backend.runs, publish_plan) else planHasCompleteCoverage(&backend.runs, publish_plan);
    if (drop_tombstones and !complete_coverage) {
        if (@hasDecl(BackendType, "releaseReaderKind")) backend.releaseReaderKind(.compaction) else backend.releaseReader();
        reader_retained = false;
        discardOutputRuns(BackendType, backend, &build_result);
        releaseCompactionSnapshots(BackendType, backend, &selected_runs);
        return;
    }

    try installCompactedRuns(
        BackendType,
        backend,
        publish_plan,
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
    for (0..plan.source_len) |i| {
        const run = run_store.planAt(backend, plan, i).*;
        try appendCompactionSnapshot(BackendType, backend, out, run);
    }
    for (0..plan.target_len) |i| {
        const run = run_store.planAt(backend, plan, plan.source_len + i).*;
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
    drop_tombstones: bool,
    split_gc: bool,
    yield_for_foreground_queries: bool,
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
    if (comptime @hasField(@TypeOf(build_backend.options), "max_run_file_entries")) {
        if (split_gc) build_backend.options.max_run_file_entries = @max(@as(usize, 1), selected[0].entry_count / 2);
    } else std.debug.assert(!split_gc);
    const runs = if (backend.root_dir != null)
        try makePersistedRunsFromSelectedRunsWithForegroundPolicy(
            BuildBackend,
            &build_backend,
            selected,
            output_level,
            drop_tombstones,
            yield_for_foreground_queries,
        )
    else
        try makeStateRunsFromSelectedRuns(BuildBackend, &build_backend, selected, output_level, drop_tombstones);
    inheritTombstoneAge(runs.items, selected);
    if (split_gc) for (runs.items) |*run| {
        run.visibility_id = if (selected[0].visibility_id == 0) selected[0].id else selected[0].visibility_id;
    };
    errdefer {
        var owned = runs;
        discardOutputRuns(BuildBackend, &build_backend, &owned);
    }
    if (build_backend.next_run_id > reserved_run_id_end) return error.CompactionRunIdReservationExhausted;
    normalizeL0Publication(runs.items);
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
    normalizeL0Publication(runs.items);
    return runs;
}

fn relocateGcPlan(allocator: std.mem.Allocator, runs: []const Run, plan: CompactionPlan, selected: []const Run) !?SelectedPlan {
    var expected: std.AutoHashMapUnmanaged(u64, u32) = .empty;
    defer expected.deinit(allocator);
    for (selected) |run| try expected.put(allocator, run.id, run.level);
    var indices: std.ArrayListUnmanaged(usize) = .empty;
    defer indices.deinit(allocator);
    for (runs, 0..) |run, i| if (expected.get(run.id)) |level| {
        if (level != run.level) return null;
        try indices.append(allocator, i);
    };
    if (indices.items.len != selected.len) return null;
    var relocated = plan;
    relocated.run_indices = indices.items;
    if (plan.split_gc) {
        relocated.source_start = 0;
        relocated.target_start = 0;
    } else if (!planHasCompleteCoverage(runs, relocated)) return null;
    relocated.run_indices = try indices.toOwnedSlice(allocator);
    return .{ .plan = relocated };
}

/// Stream a newest-first window of immutable memtables into one persisted L0
/// publication. The states remain borrowed and independently readable while
/// the backend lock is released; duplicate keys are resolved in favor of the
/// lowest (newest) source index without materializing a combined heap state.
pub fn buildRunsFromStatesBorrowedWithReservedIds(
    comptime BackendType: type,
    backend: *BackendType,
    states_newest_first: []const *const State,
    reserved_run_id_start: u64,
    reserved_run_id_end: u64,
) !std.ArrayListUnmanaged(Run) {
    if (states_newest_first.len == 0) return error.EmptyRun;
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
    const runs = try makePersistedRunsFromStatesBorrowed(
        BuildBackend,
        &build_backend,
        states_newest_first,
        0,
    );
    errdefer {
        var owned = runs;
        discardOutputRuns(BuildBackend, &build_backend, &owned);
    }
    if (build_backend.next_run_id > reserved_run_id_end) return error.FlushRunIdReservationExhausted;
    normalizeL0Publication(runs.items);
    return runs;
}

fn relocatePlanIfInputsStillMatch(runs: []const Run, plan: CompactionPlan, selected_run_ids: []const u64) ?CompactionPlan {
    if (selected_run_ids.len != plan.source_len + plan.target_len or plan.source_len == 0) return null;

    // Building persisted output deliberately releases the backend lock. New
    // L0 runs sort ahead of the selected older window while that build is in
    // flight, so positional equality would reject valid work forever under a
    // continuously replenished writer. Relocate the immutable input IDs, then
    // rebuild the target overlap closure against the current run version.
    const source_ids = selected_run_ids[0..plan.source_len];
    const source_start = findContiguousRunIds(runs, plan.source_level, source_ids) orelse return null;
    if (plan.output_level == plan.source_level and plan.target_len == 0) {
        return .{
            .source_level = plan.source_level,
            .source_start = source_start,
            .source_len = plan.source_len,
            .target_start = source_start + plan.source_len,
            .target_len = 0,
            .output_level = plan.output_level,
        };
    }
    const relocated = buildPlanForSourceRange(runs, plan.source_level, source_start, plan.source_len) orelse return null;
    if (relocated.source_len != plan.source_len or
        relocated.output_level != plan.output_level or
        relocated.target_len != plan.target_len) return null;

    const target_ids = selected_run_ids[plan.source_len..];
    for (runs[relocated.target_start .. relocated.target_start + relocated.target_len], target_ids) |run, expected_id| {
        if (run.id != expected_id) return null;
    }
    return relocated;
}

fn inheritL0Sequence(output: *std.ArrayListUnmanaged(Run), inputs: []const *Run, output_level: u32) void {
    if (output_level != 0 or output.items.len == 0) return;
    var sequence: u64 = 0;
    for (inputs) |run| sequence = @max(sequence, l0Sequence(run.*));
    for (output.items) |*run| run.visibility_id = sequence;
}

fn findContiguousRunIds(runs: []const Run, level: u32, ids: []const u64) ?usize {
    if (ids.len == 0 or ids.len > runs.len) return null;
    var start: usize = 0;
    while (start + ids.len <= runs.len) : (start += 1) {
        if (runs[start].level != level or runs[start].id != ids[0]) continue;
        for (runs[start .. start + ids.len], ids) |run, expected_id| {
            if (run.level != level or run.id != expected_id) break;
        } else return start;
    }
    return null;
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
    if (comptime @TypeOf(backend.runs) == run_store.Store) {
        return installOwnedTreeRuns(backend, plan, selected_len, input_bytes, start_ns, compacted_runs);
    }
    var retained = std.ArrayListUnmanaged(Run).empty;
    errdefer {
        for (retained.items) |*run| run.deinit(backend.allocator);
        retained.deinit(backend.allocator);
    }
    try retained.ensureTotalCapacity(backend.allocator, run_store.count(backend) - selected_len + compacted_runs.items.len);

    var obsolete_runs = std.ArrayListUnmanaged(Run).empty;
    errdefer {
        for (obsolete_runs.items) |*run| run.deinit(backend.allocator);
        obsolete_runs.deinit(backend.allocator);
    }
    try obsolete_runs.ensureTotalCapacity(backend.allocator, selected_len);

    var remove = try backend.allocator.alloc(bool, run_store.count(backend));
    defer backend.allocator.free(remove);
    @memset(remove, false);
    for (0..plan.source_len) |i| remove[plan.sourceIndex(i)] = true;
    for (0..plan.target_len) |i| remove[plan.targetIndex(i)] = true;

    // The build ran without the backend lock. After revalidation, stage every
    // remaining allocation before changing the live version so OOM leaves the
    // old run set fully intact and the new files safely discardable.
    var obsolete_paths = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (obsolete_paths.items) |path| backend.allocator.free(path);
        obsolete_paths.deinit(backend.allocator);
    }
    try obsolete_paths.ensureTotalCapacity(backend.allocator, selected_len);
    for (backend.runs.items, remove) |run, removed| {
        if (!removed) continue;
        if (run.path) |path| obsolete_paths.appendAssumeCapacity(try backend.allocator.dupe(u8, path));
    }
    try backend.reserveObsoletePublication(obsolete_paths.items.len, @intFromBool(selected_len > 0));

    reconcileGcObjective(&backend.runs, plan, compacted_runs.items);
    const directory = if (comptime @hasDecl(BackendType, "prepareRunDirectoryChange")) try backend.prepareRunDirectoryChange(plan, compacted_runs.items) else null;

    for (backend.runs.items, 0..) |*run, i| {
        if (remove[i]) {
            obsolete_runs.appendAssumeCapacity(run.*);
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
        backend.recordCompactionWriteStats(input_bytes, compacted_runs.items, elapsed_ns);
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

    if (@hasDecl(BackendType, "invalidateReadVersion")) backend.invalidateReadVersion();
    backend.runs.deinit(backend.allocator);
    backend.runs = retained;
    retained = .empty;
    if (comptime @hasDecl(BackendType, "publishRunDirectory")) backend.publishRunDirectory(directory);
    for (obsolete_paths.items) |*path| {
        backend.queueObsoleteFilePathAssumeCapacity(path.*);
        path.* = &.{};
    }
    obsolete_paths.items.len = 0;
    backend.queueObsoleteRunsAssumeCapacity(obsolete_runs);
    obsolete_runs = .empty;
}

fn installOwnedTreeRuns(backend: anytype, plan: CompactionPlan, selected_len: usize, input_bytes: u64, start_ns: u64, outputs: *std.ArrayListUnmanaged(Run)) !void {
    const allocator = backend.allocator;
    const retired_store = try allocator.create(run_store.Store);
    errdefer allocator.destroy(retired_store);
    var metadata_credit = try backend.admitCompactionMetadata(plan, outputs.items);
    defer metadata_credit.release();
    var candidate = backend.runs.fork();
    errdefer candidate.deinit(allocator);
    var retired: std.ArrayListUnmanaged(Run) = .empty;
    errdefer {
        for (retired.items) |*run| run.deinit(allocator);
        retired.deinit(allocator);
    }
    try retired.ensureTotalCapacity(allocator, selected_len);
    var paths: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (paths.items) |path| allocator.free(path);
        paths.deinit(allocator);
    }
    try paths.ensureTotalCapacity(allocator, selected_len);
    for (0..selected_len) |i| {
        const run = run_store.planAt(backend, plan, i);
        if (run.path) |path| paths.appendAssumeCapacity(try allocator.dupe(u8, path));
        retired.appendAssumeCapacity(run.retainOwned());
        try candidate.remove(allocator, run);
    }
    reconcileGcObjective(&backend.runs, plan, outputs.items);
    for (outputs.items) |run| try candidate.stage(allocator, run);
    try backend.reserveObsoletePublication(paths.items.len, @intFromBool(selected_len != 0));
    const directory = try backend.prepareRunDirectoryChange(plan, outputs.items);
    // Everything below is allocation-free. Untouched runs stay shared; only
    // removed payloads move to the retirement owner and outputs are adopted.
    var output_bytes: u64 = 0;
    for (outputs.items) |run| {
        candidate.adopt(&run);
        output_bytes +|= run.size_bytes;
    }
    backend.recordCompactionWriteStats(input_bytes, outputs.items, elapsedNs(@TypeOf(backend.*), backend, start_ns));
    disarmRunList(outputs);
    outputs.deinit(allocator);
    outputs.* = .empty;
    backend.invalidateReadVersion();
    std.mem.swap(run_store.Store, &backend.runs, &candidate);
    retired_store.* = candidate;
    backend.retireRunStore(retired_store);
    backend.publishRunDirectory(directory);
    metadata_credit.commit();
    backend.compaction_stats.compactions += 1;
    backend.compaction_stats.input_runs += selected_len;
    backend.compaction_stats.input_bytes +|= input_bytes;
    backend.compaction_stats.output_bytes +|= output_bytes;
    for (paths.items) |path| backend.queueObsoleteFilePathAssumeCapacity(path);
    paths.items.len = 0;
    backend.queueObsoleteRunsAssumeCapacity(retired);
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

fn countLeadingL0Runs(runs: anytype) usize {
    if (comptime @TypeOf(runs) == *run_store.Store or @TypeOf(runs) == *const run_store.Store) return runs.l0Files();
    var l0_count: usize = 0;
    while (l0_count < run_store.len(runs) and run_store.get(runs, l0_count).level == 0) : (l0_count += 1) {}
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
    return levelByteTargetForTotals(total_bytes, max_level, level, base, multiplier);
}

pub fn levelByteTargetForTotals(total_bytes: u64, max_level: u32, level: u32, base: usize, multiplier: usize) u64 {
    if (level == 0 or base == 0) return 0;
    const factor: u64 = @intCast(@max(@as(usize, 1), multiplier));
    if (factor == 1) return staticLevelByteTarget(level, base, multiplier);

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

test "domain planner uses global lower level budgets and normalized pressure" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        runs: std.ArrayListUnmanaged(Run),
        options: struct {
            run_partition_key: PartitionKey = struct {
                fn key(bytes: []const u8) []const u8 {
                    return bytes;
                }
            }.key,
            l0_overlap_compact_threshold_runs: usize = 0,
            level_target_runs_base: usize = 2,
            level_target_runs_multiplier: usize = 10,
            level_target_bytes_base: usize = 0,
            level_target_bytes_multiplier: usize = 10,
        } = .{},
    };
    var runs = [_]Run{
        testRun(12, 0, "z", "z", 10), testRun(11, 0, "z", "z", 10),
        testRun(10, 0, "z", "z", 10), testRun(9, 0, "z", "z", 10),
        testRun(8, 1, "a", "a", 10),  testRun(7, 1, "b", "b", 10),
        testRun(6, 1, "c", "c", 10),  testRun(5, 1, "d", "d", 10),
        testRun(4, 1, "e", "e", 10),  testRun(3, 1, "f", "f", 10),
        testRun(2, 1, "g", "g", 10),  testRun(1, 1, "h", "h", 10),
    };
    var backend = Fixture{ .allocator = std.testing.allocator, .runs = .{ .items = &runs, .capacity = 0 } };
    var stats: CompactionSelectionStats = .{};
    const selected = (try selectDomainPlan(&backend, 3, false, 0, false, &stats)).?;
    defer selected.release(backend);
    // Each L1 domain is below the local budget, but global L1 is 4x its
    // budget. It must outrank the 1.33x L0 pressure in the unrelated z domain.
    try std.testing.expectEqual(@as(u32, 1), selected.plan.source_level);
    try std.testing.expectEqual(@as(u32, 2), selected.plan.output_level);
    try std.testing.expectEqual(@as(usize, 1), selected.plan.source_len);
}

test "tombstone GC coverage rejects deeper values but permits newer concurrent flushes" {
    const allocator = std.testing.allocator;
    var runs = [_]Run{
        testRun(3, 0, "a", "a", 10),
        testRun(2, 1, "a", "a", 10),
        testRun(1, 3, "a", "a", 10),
    };
    const partial = CompactionPlan{ .source_level = 0, .source_start = 0, .source_len = 1, .target_start = 1, .target_len = 1, .output_level = 1 };
    try std.testing.expect(!planHasCompleteCoverage(&runs, partial));
    const indices = [_]usize{ 0, 1, 2 };
    const complete = CompactionPlan{ .source_level = 0, .source_start = 0, .source_len = 3, .target_start = 0, .target_len = 0, .output_level = 3, .run_indices = &indices, .tombstone_gc = true };
    try std.testing.expect(planHasCompleteCoverage(&runs, complete));
    const same = (try relocateGcPlan(allocator, &runs, complete, &runs)).?;
    defer same.deinit(allocator);
    var concurrent = [_]Run{testRun(4, 0, "a", "a", 10)} ++ runs;
    const newer = (try relocateGcPlan(allocator, &concurrent, complete, &runs)).?;
    defer newer.deinit(allocator);
    concurrent[0] = testRun(4, 0, "z", "z", 10);
    const disjoint = (try relocateGcPlan(allocator, &concurrent, complete, &runs)).?;
    defer disjoint.deinit(allocator);
    concurrent[2].level = 2;
    try std.testing.expect((try relocateGcPlan(allocator, &concurrent, complete, &runs)) == null);
}

test "tombstone GC density bounds garbage without stranding duplicate older versions" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        runs: std.ArrayListUnmanaged(Run),
        options: struct {
            run_partition_key: PartitionKey = null,
            level_target_runs_base: usize = 32,
            level_target_runs_multiplier: usize = 4,
            level_target_bytes_base: usize = 0,
            level_target_bytes_multiplier: usize = 8,
            tombstone_gc_min_percent: u8 = 50,
            tombstone_gc_max_age_ns: u64 = std.time.ns_per_hour,
            max_compaction_input_allow_oversized_single_job: bool = false,
        } = .{},
    };
    var runs = [_]Run{ testRun(4, 0, "a", "z", 10), testRun(3, 1, "a", "z", 10), testRun(2, 2, "a", "z", 10), testRun(1, 3, "a", "z", 10) };
    for (&runs) |*run| run.entry_count = 100;
    runs[0].tombstone_count = 10;
    runs[0].oldest_tombstone_unix_ns = gcNowNs();
    var backend = Fixture{ .allocator = std.testing.allocator, .runs = .{ .items = &runs, .capacity = 0 } };
    try std.testing.expect((try selectTombstoneGc(&backend, 0)) == null);
    // Sparse deletes eventually qualify independently of their key fraction.
    runs[0].oldest_tombstone_unix_ns = 1;
    const aged = (try selectTombstoneGc(&backend, 0)).?;
    defer aged.deinit(backend.allocator);
    try std.testing.expectEqual(@as(?u64, 0), nextTombstoneGcDelay(&backend));
    runs[0].oldest_tombstone_unix_ns = std.math.maxInt(u64);
    try std.testing.expect(tombstoneAgeDue(&backend, runs[0]));
    try std.testing.expectEqual(@as(?u64, 0), nextTombstoneGcDelay(&backend));
    runs[0].oldest_tombstone_unix_ns = gcNowNs();
    runs[0].tombstone_count = 100;
    // One complete delete generation over three older copies must qualify,
    // even though tombstones are only 25% of physical input entries.
    const selected = (try selectTombstoneGc(&backend, 0)).?;
    defer selected.release(backend);
    try std.testing.expectEqual(@as(usize, 4), selected.plan.source_len);
    const bounded = (try selectTombstoneGc(&backend, 20)).?;
    defer bounded.deinit(backend.allocator);
    try std.testing.expect(compactionInputBytes(&runs, bounded.plan) <= 20);
    try std.testing.expect(!bounded.plan.tombstone_gc);
    backend.options.max_compaction_input_allow_oversized_single_job = true;
    const oversized = (try selectTombstoneGc(&backend, 20)).?;
    defer oversized.deinit(backend.allocator);
    try std.testing.expect(compactionInputBytes(&runs, oversized.plan) <= 20);
}

test "bounded GC carries aggregate eligibility across windows and retries independently of age" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        runs: std.ArrayListUnmanaged(Run),
        tombstone_gc_retry_after_ns: u64 = 250 * std.time.ns_per_ms,
        options: struct {
            run_partition_key: PartitionKey = null,
            level_target_runs_base: usize = 32,
            level_target_runs_multiplier: usize = 4,
            level_target_bytes_base: usize = 0,
            level_target_bytes_multiplier: usize = 8,
            tombstone_gc_min_percent: u8 = 50,
            tombstone_gc_max_age_ns: u64 = std.time.ns_per_hour,
        } = .{},
        pub fn nowNs(_: *@This()) u64 {
            return 0;
        }
    };
    var runs = [_]Run{ testRun(2, 1, "a", "z", 1024), testRun(1, 2, "a", "z", 1024) };
    for (&runs) |*run| {
        run.entry_count = 100;
        run.tombstone_count = 30;
        run.oldest_tombstone_unix_ns = gcNowNs();
    }
    var backend = Fixture{ .allocator = std.testing.allocator, .runs = .{ .items = &runs, .capacity = 0 } };
    try std.testing.expectEqual(@as(?u64, 250 * std.time.ns_per_ms), nextTombstoneGcDelay(&backend));
    backend.options.tombstone_gc_max_age_ns = 0;
    try std.testing.expectEqual(@as(?u64, 250 * std.time.ns_per_ms), nextTombstoneGcDelay(&backend));
    backend.tombstone_gc_retry_after_ns = 0;
    const selected = (try selectTombstoneGc(&backend, 1536)).?;
    defer selected.release(backend);
    try std.testing.expect(compactionInputBytes(&runs, selected.plan) <= 1536);
    try std.testing.expect(runs[0].gc_requested and runs[1].gc_requested);
    // Even if a preceding job lowers density, the outstanding objective stays.
    runs[0].tombstone_count = 1;
    runs[1].tombstone_count = 1;
    const next = (try selectTombstoneGc(&backend, 1536)).?;
    defer next.deinit(backend.allocator);
    try std.testing.expectEqual(@as(?u64, 0), nextTombstoneGcDelay(&backend));
    var outputs = [_]Run{ runs[0], runs[1] };
    outputs[1].tombstone_count = 0;
    inheritTombstoneAge(&outputs, &.{&runs[0]});
    try std.testing.expect(outputs[0].gc_requested);
    try std.testing.expect(!outputs[1].gc_requested);
}

test "persistent planner ordering matches rebuilt domain and GC indexes after level moves" {
    const Family = struct {
        fn key(bytes: []const u8) []const u8 {
            return bytes[0..@min(bytes.len, 1)];
        }
    };
    const Fixture = struct {
        allocator: std.mem.Allocator = std.testing.allocator,
        runs: std.ArrayListUnmanaged(Run),
        options: struct {
            run_partition_key: PartitionKey = Family.key,
            level_target_runs_base: usize = 32,
            level_target_runs_multiplier: usize = 4,
            level_target_bytes_base: usize = 0,
            level_target_bytes_multiplier: usize = 8,
        } = .{},
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
    };
    var runs = [_]Run{ testRun(8, 0, "a0", "a9", 10), testRun(7, 0, "z0", "z9", 10), testRun(6, 1, "a0", "a9", 10), testRun(5, 1, "v0", "v9", 10), testRun(4, 2, "a0", "z9", 10) };
    var fixture = Fixture{ .runs = .{ .items = &runs, .capacity = 0 } };
    for (&runs) |*run| run.path = @constCast("/planner-oracle.tbl");
    const directory = try Directory.create(fixture.allocator);
    defer directory.destroy(fixture.allocator);
    for (runs) |run| try directory.put(&fixture, run);
    const previous = try directory.fork(fixture.allocator);
    defer previous.destroy(fixture.allocator);
    for (0..2) |step| {
        if (step == 1) {
            try directory.remove(fixture.allocator, &runs[0]);
            runs[0].level = 3;
            try directory.put(&fixture, runs[0]);
            sortRuns(&runs);
        }
        var snapshot = .{ .allocator = fixture.allocator, .runs = fixture.runs, .options = fixture.options, .planning_directory = directory };
        const maintained = try DomainIndex.create(&snapshot);
        defer maintained.destroy(fixture.allocator);
        const rebuilt = try DomainIndex.create(&fixture);
        defer rebuilt.destroy(fixture.allocator);
        try std.testing.expectEqualSlices(usize, rebuilt.order, maintained.order);
        try std.testing.expectEqualSlices(usize, rebuilt.ends, maintained.ends);
        try std.testing.expectEqualSlices(usize, rebuilt.gc_order, maintained.gc_order);
        try std.testing.expectEqualSlices(usize, rebuilt.gc_ends, maintained.gc_ends);
        try std.testing.expectEqualDeep(rebuilt.levels, maintained.levels);
        try std.testing.expectEqual(rebuilt.mixed, maintained.mixed);
    }
    const Changes = struct {
        added: usize = 0,
        removed: usize = 0,
        pub fn put(self: *@This(), run: Run) !void {
            try std.testing.expectEqual(@as(u64, 8), run.id);
            self.added += 1;
        }
        pub fn remove(self: *@This(), run: Run) !void {
            try std.testing.expectEqual(@as(u64, 8), run.id);
            self.removed += 1;
        }
    };
    var changes: Changes = .{};
    try directory.changesSince(previous, &changes);
    try std.testing.expectEqual(@as(usize, 1), changes.added);
    try std.testing.expectEqual(@as(usize, 1), changes.removed);
}

test "compaction installation preserves GC requests advanced during its build" {
    var live = [_]Run{ testRun(3, 0, "a", "z", 1024), testRun(2, 1, "a", "z", 1024) };
    var outputs = [_]Run{ testRun(4, 1, "a", "z", 1024), testRun(5, 1, "z", "z", 1024) };
    outputs[0].tombstone_count = 1;
    outputs[1].tombstone_count = 0;
    const plan = CompactionPlan{ .source_level = 0, .source_start = 0, .source_len = 1, .target_start = 1, .target_len = 1, .output_level = 1 };
    // The build captured no request. A denied overlapping GC job then marks
    // the live target before the ordinary compaction reaches installation.
    live[1].gc_requested = true;
    reconcileGcObjective(&live, plan, &outputs);
    try std.testing.expect(outputs[0].gc_requested);
    try std.testing.expect(!outputs[1].gc_requested);
}

test "domain compaction maps interleaved inputs and revalidates concurrent publication" {
    const Family = struct {
        fn key(bytes: []const u8) []const u8 {
            return bytes[0..@min(bytes.len, 1)];
        }
    };
    const Fixture = struct {
        allocator: std.mem.Allocator,
        runs: std.ArrayListUnmanaged(Run),
        options: struct {
            run_partition_key: PartitionKey = Family.key,
            l0_overlap_compact_threshold_runs: usize = 2,
            level_target_runs_base: usize = 100,
            level_target_runs_multiplier: usize = 10,
            level_target_bytes_base: usize = 0,
            level_target_bytes_multiplier: usize = 10,
        } = .{},
    };
    var runs = [_]Run{
        testRun(10, 0, "a", "a", 1), testRun(9, 0, "z", "z", 1),
        testRun(8, 0, "a", "a", 1),  testRun(7, 0, "v", "v", 1024 * 1024),
        testRun(6, 1, "a", "a", 1),  testRun(5, 1, "v", "v", 1024 * 1024),
        testRun(4, 1, "z", "z", 1),
    };
    var backend = Fixture{ .allocator = std.testing.allocator, .runs = .{ .items = &runs, .capacity = 0 } };
    var stats: CompactionSelectionStats = .{};
    const selected = (try selectDomainPlan(&backend, 2, true, 8, false, &stats)).?;
    defer selected.release(backend);
    const plan = selected.plan;
    try std.testing.expect(plan.run_indices != null);
    try std.testing.expect(compactionInputBytes(&runs, plan) <= 8);
    var work = try compactionWorkForPlan(backend.allocator, &runs, plan, 1);
    defer work.deinit(backend.allocator);
    for (work.run_ids) |id| try std.testing.expect(id != 7 and id != 5);
    var concurrent = [_]Run{ testRun(12, 0, "a", "a", 1), testRun(11, 0, "z", "z", 1) } ++ runs;
    const relocated = (try relocateDomainPlan(backend.allocator, &concurrent, plan, work.run_ids)).?;
    defer relocated.deinit(backend.allocator);
    var current = try compactionWorkForPlan(backend.allocator, &concurrent, relocated.plan, 1);
    defer current.deinit(backend.allocator);
    try std.testing.expectEqualSlices(u64, work.run_ids, current.run_ids);
    concurrent[relocated.plan.targetIndex(0)].id = 99;
    try std.testing.expect((try relocateDomainPlan(backend.allocator, &concurrent, plan, work.run_ids)) == null);
}

test "lsm geometric L0 carry tolerates uneven generations without two-way rewrites" {
    const mib: u64 = 1024 * 1024;
    const uneven = [_]Run{
        testRun(7, 0, "doc:a", "doc:z", mib),
        testRun(6, 0, "doc:a", "doc:z", mib),
        testRun(5, 0, "doc:a", "doc:z", mib),
        testRun(4, 0, "doc:a", "doc:z", 2 * mib),
        testRun(3, 0, "doc:a", "doc:z", mib),
        testRun(2, 0, "doc:a", "doc:z", mib),
        testRun(1, 0, "doc:a", "doc:z", mib),
    };

    // Four uneven inputs would grow the largest generation by only 2.5x, so
    // the planner waits. Once adjacent deltas provide a 4x output, it carries
    // the whole chronological window in one streaming merge.
    try std.testing.expect(selectBulkL0Tier(uneven[0..4], 4, 0, 0) == null);
    const plan = selectBulkL0Tier(&uneven, 4, 0, 0) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 0), plan.source_start);
    try std.testing.expectEqual(@as(usize, 7), plan.source_len);
    try std.testing.expectEqual(@as(u32, 0), plan.output_level);

    // A strict input bound may postpone the carry, but must never silently
    // select a smaller low-growth rewrite.
    try std.testing.expect(selectBulkL0Tier(&uneven, 4, 7 * mib, 0) == null);
}

test "unlocked compaction publication relocates inputs after concurrent L0 prepend" {
    const before = [_]Run{
        testRun(5, 0, "doc:a", "doc:m", 10),
        testRun(4, 0, "doc:n", "doc:z", 10),
        testRun(1, 1, "doc:a", "doc:z", 20),
    };
    const original = buildPlanForSourceRange(&before, 0, 0, 2) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 0), original.source_start);
    try std.testing.expectEqual(@as(usize, 2), original.target_start);
    try std.testing.expectEqual(@as(usize, 1), original.target_len);

    // A writer flushes while the compaction output is built without the
    // backend lock. L0 is newest-first, so all original inputs move right even
    // though none of them changed.
    const after_prepend = [_]Run{
        testRun(6, 0, "doc:a", "doc:z", 10),
        testRun(5, 0, "doc:a", "doc:m", 10),
        testRun(4, 0, "doc:n", "doc:z", 10),
        testRun(1, 1, "doc:a", "doc:z", 20),
    };
    const relocated = relocatePlanIfInputsStillMatch(&after_prepend, original, &.{ 5, 4, 1 }) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), relocated.source_start);
    try std.testing.expectEqual(@as(usize, 3), relocated.target_start);
    try std.testing.expectEqual(@as(usize, 1), relocated.target_len);
}

test "unlocked compaction publication rejects a changed target overlap closure" {
    const before = [_]Run{
        testRun(5, 0, "doc:a", "doc:z", 10),
    };
    const original = buildPlanForSourceRange(&before, 0, 0, 1) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 0), original.target_len);

    // A concurrent compaction created an overlapping target-level run. The
    // selected source still exists, but publishing the old output would create
    // an invalid overlapping L1 version, so this remains a genuine stale plan.
    const changed_target = [_]Run{
        testRun(6, 0, "doc:zz", "doc:zz", 10),
        testRun(5, 0, "doc:a", "doc:z", 10),
        testRun(9, 1, "doc:a", "doc:z", 20),
    };
    try std.testing.expect(relocatePlanIfInputsStillMatch(&changed_target, original, &.{5}) == null);
}

test "lsm compaction sizes bloom filters per bounded output run" {
    var first = testRun(1, 0, "doc:a", "doc:m", 1000);
    first.entry_count = 100;
    var second = testRun(2, 0, "doc:n", "doc:z", 1000);
    second.entry_count = 100;
    const inputs = [_]*Run{ &first, &second };

    try std.testing.expectEqual(@as(usize, 38), estimatedCompactionOutputEntries(&inputs, 500));
    try std.testing.expectEqual(@as(usize, 3), estimatedCompactionOutputEntries(&inputs, 40));
    try std.testing.expectEqual(@as(usize, 1), estimatedCompactionOutputEntries(&inputs, 1));
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

fn makeStateRunsFromSelectedRuns(comptime BackendType: type, backend: *BackendType, runs: []const *Run, level: u32, drop_tombstones: bool) !std.ArrayListUnmanaged(Run) {
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

    if (drop_tombstones) try state_mod.stripTombstones(&merged, backend.allocator);
    if (merged.entries.items.len == 0) {
        merged.deinit(backend.allocator);
        return .empty;
    }
    return try makeRunsFromStateAtLevel(BackendType, backend, &merged, level);
}

pub fn makePersistedRunsFromSelectedRuns(comptime BackendType: type, backend: *BackendType, window_runs: []const *Run, output_level: u32) !std.ArrayListUnmanaged(Run) {
    return makePersistedRunsFromSelectedRunsWithGc(BackendType, backend, window_runs, output_level, false);
}

fn makePersistedRunsFromSelectedRunsWithGc(comptime BackendType: type, backend: *BackendType, window_runs: []const *Run, output_level: u32, drop_tombstones: bool) !std.ArrayListUnmanaged(Run) {
    return makePersistedRunsFromSelectedRunsWithForegroundPolicy(BackendType, backend, window_runs, output_level, drop_tombstones, false);
}

fn makePersistedRunsFromSelectedRunsWithForegroundPolicy(
    comptime BackendType: type,
    backend: *BackendType,
    window_runs: []const *Run,
    output_level: u32,
    drop_tombstones: bool,
    yield_for_foreground_queries: bool,
) !std.ArrayListUnmanaged(Run) {
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
        initialized_cursors += 1;
        if (cursors[i].index.entry_count != run.entry_count) return error.InvalidTableFile;
    }

    var runs = std.ArrayListUnmanaged(Run).empty;
    errdefer discardOutputRuns(BackendType, backend, &runs);
    var output: PersistedOutputRunBuilder(BackendType) = undefined;
    var output_active = false;
    defer if (output_active) output.deinit();
    const target_bytes = targetRunFileBytes(BackendType, backend);
    const expected_entries_per_output = estimatedCompactionOutputEntries(window_runs, target_bytes);
    var consumed_entries: usize = 0;
    var emitted_entries: usize = 0;

    var heap = try PersistedRunMergeHeap.init(allocator, cursors[0..initialized_cursors]);
    defer heap.deinit();

    while (heap.peekSource()) |winner_source| {
        if (yield_for_foreground_queries and consumed_entries % 256 == 0) {
            if (backend.options.resource_manager) |manager| {
                manager.yieldOptionalMaintenanceForForegroundQuery();
            }
        }
        const winner = (try cursors[winner_source].currentEntry()) orelse return error.InvalidTableFile;
        if (drop_tombstones and winner.tombstone) {
            consumed_entries += try heap.advanceTopSourcesAtKey(winner);
            continue;
        }
        const entry_bytes = tableEntryLogicalBytes(winner);
        if (output_active) {
            const partition_changed = output.entry_count > 0 and !sameRunPartition(
                output.smallest_namespace_name,
                output.smallest_key,
                winner.namespace_name,
                winner.key,
                backend.options.run_partition_prefix_bytes,
                backend.options.run_partition_key,
            );
            if (partition_changed or
                output.entry_count >= outputEntryLimit(backend) or
                (output.entry_count > 0 and target_bytes > 0 and output.logical_bytes + entry_bytes > target_bytes) or
                (output.entry_count > 0 and !output.canAppendEntry(winner)))
            {
                try runs.ensureUnusedCapacity(allocator, 1);
                const run = try output.finish();
                output.deinit();
                output_active = false;
                runs.appendAssumeCapacity(run);
            }
        }

        if (!output_active) {
            const remaining_entries = expected_entries - consumed_entries;
            try output.initInPlace(
                backend,
                output_level,
                @max(@as(usize, 1), @min(remaining_entries, expected_entries_per_output)),
            );
            output_active = true;
        }
        if (!output.canAppendEntry(winner)) return error.TableFileTooLarge;
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
    if (runs.items.len == 0 and !drop_tombstones) return error.EmptyRun;
    if (consumed_entries != expected_entries) return error.InvalidTableFile;
    if (countRunEntries(runs.items) != emitted_entries) return error.InvalidTableFile;
    normalizeL0Publication(runs.items);
    return runs;
}

fn makePersistedRunsFromStatesBorrowed(
    comptime BackendType: type,
    backend: *BackendType,
    states_newest_first: []const *const State,
    output_level: u32,
) !std.ArrayListUnmanaged(Run) {
    const allocator = backend.allocator;
    var expected_entries: usize = 0;
    for (states_newest_first) |state| {
        if (state.entryCount() == 0) return error.EmptyRun;
        expected_entries = std.math.add(usize, expected_entries, state.entryCount()) catch
            return error.OutOfMemory;
    }

    var heap = try StateMergeHeap.init(allocator, states_newest_first);
    defer heap.deinit();
    var runs = std.ArrayListUnmanaged(Run).empty;
    errdefer discardOutputRuns(BackendType, backend, &runs);
    var output: PersistedOutputRunBuilder(BackendType) = undefined;
    var output_active = false;
    defer if (output_active) output.deinit();
    const target_bytes = targetRunFileBytes(BackendType, backend);
    const expected_entries_per_output = @max(
        @as(usize, 1),
        @min(expected_entries, target_bytes / minimum_table_entry_logical_bytes),
    );
    var consumed_entries: usize = 0;
    var emitted_entries: usize = 0;

    while (heap.peekSource()) |winner_source| {
        const winner = heap.currentEntry(winner_source);
        const entry_bytes = tableEntryLogicalBytes(winner);
        if (output_active) {
            const partition_changed = output.entry_count > 0 and !sameRunPartition(
                output.smallest_namespace_name,
                output.smallest_key,
                winner.namespace_name,
                winner.key,
                backend.options.run_partition_prefix_bytes,
                backend.options.run_partition_key,
            );
            if (partition_changed or
                output.entry_count >= outputEntryLimit(backend) or
                (output.entry_count > 0 and target_bytes > 0 and output.logical_bytes + entry_bytes > target_bytes) or
                (output.entry_count > 0 and !output.canAppendEntry(winner)))
            {
                try runs.ensureUnusedCapacity(allocator, 1);
                runs.appendAssumeCapacity(try output.finish());
                output.deinit();
                output_active = false;
            }
        }
        if (!output_active) {
            const remaining_entries = expected_entries - consumed_entries;
            try output.initInPlace(
                backend,
                output_level,
                @max(@as(usize, 1), @min(remaining_entries, expected_entries_per_output)),
            );
            output_active = true;
        }
        if (!output.canAppendEntry(winner)) return error.TableFileTooLarge;
        try output.appendEntry(winner, entry_bytes);
        emitted_entries += 1;
        consumed_entries += try heap.advanceTopSourcesAtKey(winner);

        if (output.logical_bytes >= target_bytes) {
            try runs.ensureUnusedCapacity(allocator, 1);
            runs.appendAssumeCapacity(try output.finish());
            output.deinit();
            output_active = false;
        }
    }
    if (output_active) {
        try runs.ensureUnusedCapacity(allocator, 1);
        runs.appendAssumeCapacity(try output.finish());
        output.deinit();
        output_active = false;
    }
    if (runs.items.len == 0) return error.EmptyRun;
    if (consumed_entries != expected_entries) return error.InvalidTableFile;
    if (countRunEntries(runs.items) != emitted_entries) return error.InvalidTableFile;
    normalizeL0Publication(runs.items);
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
        tombstone_count: u32 = 0,
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
                physicalRunFileLimit(BackendType, backend),
                backend.options.bloom,
                backend.options.table_block_compression,
                backend.options.table_prefix_extractor,
                backend.options.resource_manager,
                .cold_sequential,
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
            self.tombstone_count += @intFromBool(entry.tombstone);
            self.logical_bytes += entry_bytes;
        }

        fn canAppendEntry(self: *const Self, entry: lsm_table_file.Entry) bool {
            return self.writer.canAppendEntry(entry);
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
                .tombstone_count = self.tombstone_count,
                .oldest_tombstone_unix_ns = if (self.tombstone_count != 0) gcNowNs() else 0,
                .bloom_filter = persisted.filter,
                .state = null,
            };
        }
    };
}

/// One admitted, immutable input. Unknown counts are reconciled without
/// changing read visibility, rewriting SSTs, or scanning unrelated runs.
pub const PendingTombstoneReconcile = struct {
    handle: Directory.Handle,
    account: *@import("memory_account.zig").Account,
    footer: ?lsm_table_file.Footer = null,
    cursor: ?PersistedRunCursor = null,
    state_cursor: State.EntryCursor = .{},
    rows: usize = 0,
    tombstones: u32 = 0,
    complete: bool = false,
    budget: ?resource_manager_mod.BudgetedAllocator = null,
    reservation: ?resource_manager_mod.Reservation = null,

    // Each slice admits exactly its next physical read before dropping the
    // backend lock. Buffered rows need no further I/O credit. Footer, index
    // and each compressed block are independent, resumable admission units.
    fn nextIoBytes(self: *const @This()) u64 {
        if (self.complete or self.handle.run.path == null) return 0;
        const footer = self.footer orelse return lsm_table_file.footer_len;
        const cursor = if (self.cursor) |*cursor| cursor else return footer.metadata_len;
        return cursor.nextReadBytes();
    }

    fn create(backend: anytype, handle: Directory.Handle) !*@This() {
        var credit: ?resource_manager_mod.Reservation = null;
        errdefer if (credit) |*lease| lease.release();
        if (backend.options.resource_manager) |manager|
            credit = try manager.reserve(.lsm_table_builder_working_set, @sizeOf(@This()));
        const self = try backend.allocator.create(@This());
        self.* = .{ .handle = handle.retain(), .account = handle.retainAccounting(), .reservation = credit, .budget = if (backend.options.resource_manager) |manager| .init(manager, .lsm_table_builder_working_set, backend.allocator, 1) else null };
        return self;
    }
    fn step(self: *@This(), backend: anytype, credits_arg: usize, deadline: u64) anyerror!bool {
        if (self.complete) return true;
        var credits = credits_arg;
        const run = self.handle.run;
        const allocator = if (self.budget) |*budget| budget.allocator() else backend.allocator;
        if (credits == 0 or @import("antfly_platform").time.monotonicNs() >= deadline) return false;
        if (run.path) |path| {
            if (self.footer == null) {
                self.footer = try repository_mod.loadRunFooterWithStorage(backend.storage.?, allocator, path);
                if (self.footer.?.entry_count != run.entry_count) return error.InvalidTableFile;
                return false;
            }
            if (self.cursor == null) {
                const footer = self.footer.?;
                const metadata = try backend.storage.?.readFileRangeAlloc(allocator, path, footer.metadata_offset, footer.metadata_len);
                defer allocator.free(metadata);
                self.cursor = try PersistedRunCursor.initWithIndex(allocator, backend.storage.?, path, try lsm_table_file.decodeSequentialIndexFromFooterAlloc(allocator, footer, metadata));
                if (self.cursor.?.index.entry_count != run.entry_count) return error.InvalidTableFile;
                return false; // Index admission/I/O owns a separate quantum.
            }
        }
        var may_read_window = if (self.cursor) |*cursor| cursor.nextReadBytes() != 0 else false;
        while (credits != 0 and @import("antfly_platform").time.monotonicNs() < deadline) {
            const deleted = if (self.cursor) |*cursor| blk: {
                if (cursor.nextReadBytes() != 0) {
                    if (!may_read_window) return false;
                    may_read_window = false;
                }
                const entry = (try cursor.currentEntry()) orelse {
                    if (self.rows != run.entry_count) return error.InvalidTableFile;
                    return self.finishScan();
                };
                break :blk entry.tombstone;
            } else blk: {
                const source = if (run.state) |*source| source else return error.RunStateUnavailable;
                if (self.rows == source.entryCount()) {
                    if (self.rows != run.entry_count) return error.InvalidTableFile;
                    return self.finishScan();
                }
                break :blk self.state_cursor.at(source, self.rows).tombstone;
            };
            self.rows += 1;
            if (self.rows > run.entry_count) return error.InvalidTableFile;
            self.tombstones += @intFromBool(deleted);
            if (self.cursor) |*cursor| try cursor.advance();
            credits -= 1;
        }
        return false;
    }
    fn finishScan(self: *@This()) bool {
        if (self.cursor) |*cursor| cursor.deinit();
        self.cursor = null;
        if (self.budget) |*budget| budget.deinit();
        self.budget = null;
        self.complete = true;
        return true;
    }
    fn releaseContents(self: *@This(), backend: anytype) void {
        if (self.cursor) |*cursor| cursor.deinit();
        if (self.budget) |*budget| budget.deinit();
        self.handle.release(backend.allocator);
    }
    fn finish(self: *@This(), backend: anytype) void {
        self.account.release();
        if (self.reservation) |*lease| lease.release();
        backend.allocator.destroy(self);
    }
    pub fn destroy(self: *@This(), backend: anytype) void {
        self.releaseContents(backend);
        self.finish(backend);
    }
};

pub fn reconcileTombstonesStep(backend: anytype) anyerror!bool {
    if (backend.tombstone_reconcile_in_flight) return false;
    if (backend.optionalMaintenanceDeferredLocked()) {
        backend.tombstone_reconcile_retry_ns = backend.nowNs() +| 100 * std.time.ns_per_ms;
        return false;
    }
    backend.tombstone_reconcile_in_flight = true;
    defer backend.tombstone_reconcile_in_flight = false;
    var release = false;
    defer if (release) if (backend.pending_tombstone_reconcile) |pending| {
        // Keep the admitted owner visible to accounting through reclamation.
        backend.retainReaderKind(.other);
        runtime_mod.unlockBackend(@TypeOf(backend.*), backend, true);
        pending.releaseContents(backend);
        _ = runtime_mod.lockBackend(@TypeOf(backend.*), backend);
        backend.pending_tombstone_reconcile = null;
        pending.finish(backend);
        backend.releaseReaderKind(.other);
    };
    errdefer |err| {
        // Publication admission can recover without rereading an already
        // verified SST. Completed jobs retain only their small metadata pin.
        release = !(err == error.ResourceBudgetExceeded and backend.pending_tombstone_reconcile != null and backend.pending_tombstone_reconcile.?.complete);
        backend.tombstone_reconcile_failures +|= 1;
        backend.tombstone_reconcile_failure_streak +|= 1;
        const shift: u6 = @intCast(@min(backend.tombstone_reconcile_failure_streak - 1, 7));
        backend.tombstone_reconcile_retry_ns = backend.nowNs() +| @min(@as(u64, 250 * std.time.ns_per_ms) << shift, 30 * std.time.ns_per_s);
    }
    const directory = try backend.planningDirectory();
    if (backend.pending_tombstone_reconcile == null) {
        const handle = directory.nextUnknownTombstone(backend.tombstone_reconcile_after_rank) orelse
            directory.nextUnknownTombstone(0) orelse return false;
        backend.tombstone_reconcile_after_rank = directory.rankOf(handle.run).? + 1;
        backend.pending_tombstone_reconcile = try PendingTombstoneReconcile.create(backend, handle);
    }
    const pending = backend.pending_tombstone_reconcile.?;
    if (directory.resolve(pending.handle) == null) {
        release = true;
        return true;
    }
    if (backend.manifestCoordinationIo()) |io| try io.checkCancel();
    if (!backend.tryReserveMaintenanceIoBudget(pending.nextIoBytes())) {
        // Admission denial is not corruption and must neither discard the
        // verified prefix nor schedule a zero-delay retry loop.
        backend.tombstone_reconcile_retry_ns = backend.nowNs() +| 100 * std.time.ns_per_ms;
        return false;
    }
    backend.tombstone_reconcile_retry_ns = 0;
    const before = pending.rows;
    backend.retainReaderKind(.compaction);
    runtime_mod.unlockBackend(@TypeOf(backend.*), backend, true);
    const result: anyerror!bool = pending.step(backend, 2048, @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms);
    _ = runtime_mod.lockBackend(@TypeOf(backend.*), backend);
    backend.releaseReaderKind(.compaction);
    backend.tombstone_reconcile_rows +|= pending.rows - before;
    backend.directory_planning_slices +|= 1;
    const done = result catch |err| {
        if (pending.budget) |*budget| if (budget.denied()) return error.ResourceBudgetExceeded;
        return @as(anyerror!bool, err);
    };
    if (!done) return true;
    release = true;
    const current = try backend.planningDirectory();
    if (current.resolve(pending.handle) == null) return true;
    const source = backend.runs.find(pending.handle.run).?;
    var metadata = run_store.Store.revision(source, source.*);
    metadata.tombstone_count = pending.tombstones;
    // The original delete timestamp is unknowable. Zero is explicitly due,
    // never a new grace period that repeated restarts can extend indefinitely.
    metadata.oldest_tombstone_unix_ns = 0;
    const plan = CompactionPlan{ .source_level = source.level, .source_start = 0, .source_len = 1, .target_start = 1, .target_len = 0, .output_level = source.level, .input_handles = &.{pending.handle} };
    var credit = try backend.admitCompactionMetadata(plan, &.{metadata});
    defer credit.release();
    var publication_owned = true;
    const retired = try backend.allocator.create(run_store.Store);
    errdefer if (publication_owned) backend.allocator.destroy(retired);
    var candidate = try backend.runs.prepareReplace(backend.allocator, source, metadata);
    errdefer if (publication_owned) candidate.deinit(backend.allocator);
    const updated = try current.fork(backend.allocator);
    errdefer if (publication_owned) updated.destroy(backend.allocator);
    try updated.put(backend, metadata);
    backend.invalidateReadVersion();
    retired.* = backend.runs;
    backend.runs = candidate;
    backend.retireRunStore(retired);
    backend.publishRunDirectory(updated);
    // Durable publication can fail after the live metadata has taken
    // ownership. Leave that state dirty and retryable, never free it here.
    publication_owned = false;
    credit.commit();
    backend.markManifestDirty();
    backend.tombstone_reconcile_completed +|= 1;
    backend.tombstone_reconcile_retry_ns = 0;
    backend.tombstone_reconcile_failure_streak = 0;
    if (backend.root_dir != null) try backend.persistManifestLocked();
    return true;
}

const PersistedRunCursor = struct {
    allocator: std.mem.Allocator,
    reader: @import("storage_io.zig").ColdSequentialReader,
    index: lsm_table_file.SequentialTableIndex,
    position: ?usize = null,
    block_index: usize = 0,
    entry_in_block: usize = 0,
    block_offset: usize = 0,
    current_entry_len: usize = 0,
    /// Heap maintenance compares one cursor against several peers before the
    /// cursor advances. Keep the decoded slice view stable for that interval
    /// instead of reparsing the same table bytes for every comparison.
    current_entry: ?lsm_table_file.Entry = null,
    loaded_window: ?lsm_table_file.EntryDataWindow = null,
    loaded_bytes: ?[]u8 = null,

    fn init(
        allocator: std.mem.Allocator,
        storage: @import("storage_io.zig").Storage,
        path: []const u8,
    ) !PersistedRunCursor {
        return initWithIndex(allocator, storage, path, try repository_mod.loadRunSequentialTableIndexAllocWithStorage(storage, allocator, path));
    }

    /// Consumes the index on both success and failure.
    fn initWithIndex(allocator: std.mem.Allocator, storage: @import("storage_io.zig").Storage, path: []const u8, owned_index: lsm_table_file.SequentialTableIndex) !PersistedRunCursor {
        var index = owned_index;
        errdefer index.deinit(allocator);
        // Run snapshots pin immutable paths through output publication. Keep
        // input descriptors window-scoped: per-compaction capacity estimates
        // cannot prevent two jobs from collectively exhausting the node pool
        // while both still need to open another input or output.
        const reader = try storage.beginWindowedColdRead(allocator, path);
        return .{
            .allocator = allocator,
            .reader = reader,
            .index = index,
            .position = if (index.entry_count > 0) 0 else null,
        };
    }

    fn deinit(self: *PersistedRunCursor) void {
        if (self.loaded_bytes) |bytes| self.allocator.free(bytes);
        self.reader.deinit();
        self.index.deinit(self.allocator);
        self.* = undefined;
    }

    fn currentEntry(self: *PersistedRunCursor) !?lsm_table_file.Entry {
        _ = self.position orelse return null;
        if (self.current_entry) |entry| return entry;
        try self.ensureCurrentWindow();
        const bytes = self.loaded_bytes orelse return error.InvalidTableFile;
        if (self.block_offset >= bytes.len) return error.InvalidTableFile;
        const entry = try lsm_table_file.parseEntryAt(bytes, self.block_offset);
        self.current_entry_len = tableEntryLogicalBytes(entry);
        if (self.current_entry_len > bytes.len - self.block_offset) return error.InvalidTableFile;
        self.current_entry = entry;
        return entry;
    }

    fn advance(self: *PersistedRunCursor) !void {
        const pos = self.position orelse return;
        if (self.current_entry_len == 0) _ = (try self.currentEntry()) orelse return error.InvalidTableFile;
        self.block_offset += self.current_entry_len;
        self.current_entry_len = 0;
        self.current_entry = null;
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
        if (self.windowLoaded(window)) return;

        if (self.loaded_bytes) |bytes| {
            self.allocator.free(bytes);
            self.loaded_bytes = null;
        }
        const payload = try self.reader.readRangeAlloc(
            self.allocator,
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

    fn nextReadBytes(self: *const PersistedRunCursor) u64 {
        if (self.position == null or self.block_index >= self.index.blocks.len) return 0;
        const window = self.index.blocks[self.block_index].window;
        return if (self.windowLoaded(window)) 0 else window.physicalLen();
    }

    fn windowLoaded(self: *const PersistedRunCursor, window: lsm_table_file.EntryDataWindow) bool {
        if (self.loaded_window) |loaded| {
            if (loaded.relative_offset == window.relative_offset and
                loaded.len == window.len and
                loaded.physical_relative_offset == window.physical_relative_offset and
                loaded.physical_len == window.physical_len and
                loaded.compression == window.compression)
            {
                return self.loaded_bytes != null;
            }
        }

        return false;
    }
};

const StateMergeHeap = struct {
    allocator: std.mem.Allocator,
    states: []const *const State,
    positions: []usize,
    sources: []usize,
    advanced_sources: []usize,
    cursors: []State.EntryCursor,
    len: usize = 0,

    fn init(allocator: std.mem.Allocator, states: []const *const State) !StateMergeHeap {
        const positions = try allocator.alloc(usize, states.len);
        errdefer allocator.free(positions);
        @memset(positions, 0);
        const sources = try allocator.alloc(usize, states.len);
        errdefer allocator.free(sources);
        const advanced_sources = try allocator.alloc(usize, states.len);
        errdefer allocator.free(advanced_sources);
        const cursors = try allocator.alloc(State.EntryCursor, states.len);
        errdefer allocator.free(cursors);
        @memset(cursors, .{});
        var heap = StateMergeHeap{
            .allocator = allocator,
            .states = states,
            .positions = positions,
            .sources = sources,
            .advanced_sources = advanced_sources,
            .cursors = cursors,
        };
        for (states, 0..) |state, source| {
            if (state.entryCount() != 0) try heap.pushSource(source);
        }
        return heap;
    }

    fn deinit(self: *StateMergeHeap) void {
        self.allocator.free(self.positions);
        self.allocator.free(self.sources);
        self.allocator.free(self.advanced_sources);
        self.allocator.free(self.cursors);
        self.* = undefined;
    }

    fn peekSource(self: *const StateMergeHeap) ?usize {
        return if (self.len == 0) null else self.sources[0];
    }

    fn currentEntry(self: *const StateMergeHeap, source: usize) lsm_table_file.Entry {
        return tableEntryFromOwnedEntry(self.cursors[source].at(self.states[source], self.positions[source]));
    }

    fn advanceTopSourcesAtKey(self: *StateMergeHeap, key_entry: lsm_table_file.Entry) !usize {
        var advanced_len: usize = 0;
        while (self.peekSource()) |source| {
            if (compareTableEntry(self.currentEntry(source), key_entry) != .eq) break;
            _ = self.popSource();
            self.positions[source] += 1;
            self.advanced_sources[advanced_len] = source;
            advanced_len += 1;
        }
        for (self.advanced_sources[0..advanced_len]) |source| {
            if (self.positions[source] < self.states[source].entryCount()) try self.pushSource(source);
        }
        return advanced_len;
    }

    fn pushSource(self: *StateMergeHeap, source: usize) !void {
        std.debug.assert(self.len < self.sources.len);
        self.sources[self.len] = source;
        self.len += 1;
        try self.siftUp(self.len - 1);
    }

    fn popSource(self: *StateMergeHeap) usize {
        std.debug.assert(self.len != 0);
        const source = self.sources[0];
        self.len -= 1;
        if (self.len > 0) {
            self.sources[0] = self.sources[self.len];
            self.siftDown(0) catch unreachable;
        }
        return source;
    }

    fn siftUp(self: *StateMergeHeap, start_index: usize) !void {
        var index = start_index;
        while (index > 0) {
            const parent = (index - 1) / 2;
            if (!self.sourceLess(self.sources[index], self.sources[parent])) break;
            std.mem.swap(usize, &self.sources[index], &self.sources[parent]);
            index = parent;
        }
    }

    fn siftDown(self: *StateMergeHeap, start_index: usize) !void {
        var index = start_index;
        while (true) {
            const left = index * 2 + 1;
            if (left >= self.len) break;
            const right = left + 1;
            var child = left;
            if (right < self.len and self.sourceLess(self.sources[right], self.sources[left])) child = right;
            if (!self.sourceLess(self.sources[child], self.sources[index])) break;
            std.mem.swap(usize, &self.sources[child], &self.sources[index]);
            index = child;
        }
    }

    fn sourceLess(self: *const StateMergeHeap, lhs_source: usize, rhs_source: usize) bool {
        const order = compareTableEntry(self.currentEntry(lhs_source), self.currentEntry(rhs_source));
        if (order != .eq) return order == .lt;
        // Callers pass newest to oldest, so the lower source index wins a
        // duplicate key and is emitted before all older copies are advanced.
        return lhs_source < rhs_source;
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

const minimum_table_entry_logical_bytes = 1 + 3 * @sizeOf(u32);

/// Size each output Bloom filter for one target-sized run, rather than for the
/// entire compaction window. The physical input density gives a useful
/// workload-specific estimate; the encoded minimum supplies a hard upper
/// bound even for highly compressed inputs or incomplete legacy metadata.
fn estimatedCompactionOutputEntries(window_runs: []const *Run, target_bytes: usize) usize {
    var total_entries: u128 = 0;
    var total_bytes: u128 = 0;
    for (window_runs) |run| {
        total_entries +|= run.entry_count;
        total_bytes +|= run.size_bytes;
    }
    if (total_entries == 0) return 1;

    const encoded_upper_bound = @max(@as(u128, 1), @as(u128, target_bytes) / minimum_table_entry_logical_bytes);
    const proportional = if (total_bytes == 0)
        encoded_upper_bound
    else
        (@as(u128, target_bytes) *| total_entries +| (total_bytes - 1)) / total_bytes;
    const bounded = @max(@as(u128, 1), @min(total_entries, @min(proportional, encoded_upper_bound)));
    return @intCast(@min(bounded, std.math.maxInt(usize)));
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

pub fn makeRun(comptime BackendType: type, backend: *BackendType, state: State) !Run {
    return try makeRunAtLevel(BackendType, backend, state, 0);
}

pub fn makeRuns(comptime BackendType: type, backend: *BackendType, state: *State) !std.ArrayListUnmanaged(Run) {
    return try makeRunsFromStateAtLevel(BackendType, backend, state, 0);
}

pub fn makeRunsFromStateBorrowed(comptime BackendType: type, backend: *BackendType, state: *const State) !std.ArrayListUnmanaged(Run) {
    if (state.entryCount() == 0) return error.EmptyRun;
    if (backend.root_dir != null) return try makePersistedRunsFromStateBorrowedAtLevel(BackendType, backend, state, 0);

    var scratch_bytes_accounted: u64 = 0;
    if (@hasField(BackendType, "options")) {
        if (backend.options.resource_manager) |manager| {
            const bytes = std.math.mul(u64, @intCast(state.entryCount()), @sizeOf(lsm_table_file.Entry)) catch std.math.maxInt(u64);
            manager.observeUsage(.lsm_compaction_work, &scratch_bytes_accounted, bytes);
        }
    }
    defer if (@hasField(BackendType, "options")) {
        if (backend.options.resource_manager) |manager| {
            manager.observeUsage(.lsm_compaction_work, &scratch_bytes_accounted, 0);
        }
    };
    var entries = try backend.allocator.alloc(lsm_table_file.Entry, state.entryCount());
    defer backend.allocator.free(entries);
    var cursor: State.EntryCursor = .{};
    for (0..state.entryCount()) |i| {
        const entry = cursor.at(state, i);
        entries[i] = .{
            .namespace_name = entry.namespace_name,
            .key = entry.key,
            .value = entry.value,
            .tombstone = entry.tombstone,
        };
    }
    return try makeRunsFromSortedTableEntriesAtLevel(BackendType, backend, entries, 0);
}

pub fn makePersistedRunsFromStateBorrowedAtLevel(comptime BackendType: type, backend: *BackendType, state: *const State, level: u32) !std.ArrayListUnmanaged(Run) {
    if (state.entryCount() == 0) return error.EmptyRun;
    try validateSortedUniqueOwnedEntries(state.entries.items);

    var runs = std.ArrayListUnmanaged(Run).empty;
    errdefer discardOutputRuns(BackendType, backend, &runs);

    const target_bytes = targetRunFileBytes(BackendType, backend);
    var start: usize = 0;
    var cursor: State.EntryCursor = .{};
    while (start < state.entryCount()) {
        const preferred_end = splitStateEnd(state, start, target_bytes, backend.options.run_partition_prefix_bytes, backend.options.run_partition_key);
        try runs.ensureUnusedCapacity(backend.allocator, 1);

        var output: PersistedOutputRunBuilder(BackendType) = undefined;
        try output.initInPlace(backend, level, preferred_end - start);
        var output_active = true;
        errdefer if (output_active) output.deinit();

        var end = start;
        while (end < preferred_end) : (end += 1) {
            const entry = cursor.at(state, end);
            const table_entry = tableEntryFromOwnedEntry(entry);
            if (!output.canAppendEntry(table_entry)) {
                if (end == start) return error.TableFileTooLarge;
                break;
            }
            try output.appendEntry(table_entry, estimateOwnedEntryBytes(entry));
        }

        const run = try output.finish();
        output.deinit();
        output_active = false;
        runs.appendAssumeCapacity(run);
        start = end;
    }

    normalizeL0Publication(runs.items);
    return runs;
}

pub fn makeRunsFromSortedTableEntries(comptime BackendType: type, backend: *BackendType, entries: []const lsm_table_file.Entry) !std.ArrayListUnmanaged(Run) {
    return try makeRunsFromSortedTableEntriesAtLevel(BackendType, backend, entries, 0);
}

fn makeRunsFromStateAtLevel(comptime BackendType: type, backend: *BackendType, state: *State, level: u32) !std.ArrayListUnmanaged(Run) {
    if (state.entryCount() == 0) return error.EmptyRun;

    if (backend.root_dir != null) {
        const runs = try makePersistedRunsFromStateBorrowedAtLevel(BackendType, backend, state, level);
        state.deinit(backend.allocator);
        state.* = .{};
        normalizeL0Publication(runs.items);
        return runs;
    }

    try state.ensureFlat(backend.allocator);
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
        const end = @min(splitOwnedEntriesEnd(source_entries.items, start, target_bytes, backend.options.run_partition_prefix_bytes, backend.options.run_partition_key), start +| outputEntryLimit(backend));

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
    normalizeL0Publication(runs.items);
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
        const preferred_end = splitTableEntriesEnd(entries, start, target_bytes, backend.options.run_partition_prefix_bytes, backend.options.run_partition_key);
        if (backend.root_dir == null) {
            const run = try makeRunFromSortedTableEntriesAtLevel(BackendType, backend, entries[start..preferred_end], level);
            runs.appendAssumeCapacity(run);
            start = preferred_end;
            continue;
        }

        var output: PersistedOutputRunBuilder(BackendType) = undefined;
        try output.initInPlace(backend, level, preferred_end - start);
        var output_active = true;
        errdefer if (output_active) output.deinit();

        var end = start;
        while (end < preferred_end) : (end += 1) {
            const entry = entries[end];
            if (!output.canAppendEntry(entry)) {
                if (end == start) return error.TableFileTooLarge;
                break;
            }
            try output.appendEntry(entry, estimateTableEntryBytes(entry));
        }
        const run = try output.finish();
        output.deinit();
        output_active = false;
        runs.appendAssumeCapacity(run);
        start = end;
    }
    normalizeL0Publication(runs.items);
    return runs;
}

pub fn makeRunAtLevel(comptime BackendType: type, backend: *BackendType, state: State, level: u32) !Run {
    if (state.entryCount() == 0) return error.EmptyRun;
    const run_id = backend.next_run_id;
    backend.next_run_id += 1;

    const smallest_namespace_name = if (state.entryAt(0).namespace_name) |name| try backend.allocator.dupe(u8, name) else null;
    errdefer if (smallest_namespace_name) |name| backend.allocator.free(name);
    const smallest_key = try backend.allocator.dupe(u8, state.entryAt(0).key);
    errdefer backend.allocator.free(smallest_key);
    const largest_namespace_name = if (state.entryAt(state.entryCount() - 1).namespace_name) |name| try backend.allocator.dupe(u8, name) else null;
    errdefer if (largest_namespace_name) |name| backend.allocator.free(name);
    const largest_key = try backend.allocator.dupe(u8, state.entryAt(state.entryCount() - 1).key);
    errdefer backend.allocator.free(largest_key);

    var frozen = state;
    frozen.freezeMemoryAccounting();
    var run = Run{
        .id = run_id,
        .level = level,
        .size_bytes = estimateStateBytes(&state),
        .path = null,
        .smallest_namespace_name = smallest_namespace_name,
        .smallest_key = smallest_key,
        .largest_namespace_name = largest_namespace_name,
        .largest_key = largest_key,
        .entry_count = @intCast(state.entryCount()),
        .tombstone_count = countStateTombstones(&state),
        .oldest_tombstone_unix_ns = gcNowNs(),
        .bloom_filter = try repository_mod.buildFilterForStateWithConfig(
            backend.allocator,
            &state,
            backend.options.bloom,
        ),
        .state = frozen,
    };
    errdefer if (run.bloom_filter) |*filter| filter.deinit(backend.allocator);

    if (backend.root_dir != null) {
        run.path = try repository_mod.persistRunFileWithStorageAccountedOptions(
            backend.storage.?,
            backend.allocator,
            backend.root_dir.?,
            &run,
            backend.options.bloom,
            backend.options.table_block_compression,
            backend.options.table_prefix_extractor,
            backend.options.resource_manager,
            .cold_sequential,
            physicalRunFileLimit(BackendType, backend),
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
        physicalRunFileLimit(BackendType, backend),
        backend.options.bloom,
        backend.options.table_block_compression,
        backend.options.table_prefix_extractor,
        backend.options.resource_manager,
        .cold_sequential,
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
        .tombstone_count = countTombstones(entries),
        .oldest_tombstone_unix_ns = gcNowNs(),
        .bloom_filter = persisted.filter,
        .state = null,
    };
}

fn countTombstones(entries: anytype) u32 {
    var count: u32 = 0;
    for (entries) |entry| count += @intFromBool(entry.tombstone);
    return count;
}

/// GC requests can advance while ordinary compaction builds off-lock. Merge
/// the live objective at the installation fence, including denied GC attempts.
fn reconcileGcObjective(live: anytype, plan: CompactionPlan, outputs: []Run) void {
    var requested = false;
    for (0..plan.source_len) |i| requested = requested or run_store.planGet(live, plan, i).gc_requested;
    for (0..plan.target_len) |i| requested = requested or run_store.planGet(live, plan, plan.source_len + i).gc_requested;
    if (requested) for (outputs) |*run| {
        if ((run.tombstone_count orelse 0) != 0) run.gc_requested = true;
    };
}

fn inheritTombstoneAge(outputs: []Run, inputs: []const *Run) void {
    var oldest = gcNowNs();
    var requested = false;
    for (inputs) |run| if ((run.tombstone_count orelse 0) != 0) {
        oldest = @min(oldest, run.oldest_tombstone_unix_ns);
        requested = requested or run.gc_requested;
    };
    for (outputs) |*run| {
        const has_deletes = (run.tombstone_count orelse 0) != 0;
        run.oldest_tombstone_unix_ns = if (has_deletes) oldest else 0;
        run.gc_requested = has_deletes and requested;
    }
}

fn countStateTombstones(state: *const State) u32 {
    var count: u32 = 0;
    var cursor: State.EntryCursor = .{};
    for (0..state.entryCount()) |i| count += @intFromBool(cursor.at(state, i).tombstone);
    return count;
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
    var cursor: State.EntryCursor = .{};
    for (0..state.entryCount()) |i| {
        const entry = cursor.at(state, i);
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

pub fn appendBackendRuns(backend: anytype, src: *std.ArrayListUnmanaged(Run)) !void {
    if (comptime !@hasDecl(@TypeOf(backend.*), "retireRunStore")) return appendOwnedRuns(&backend.runs, backend.allocator, src);
    if (backend.runs.count() == 0) return appendOwnedRuns(&backend.runs, backend.allocator, src);
    const retired = try backend.allocator.create(run_store.Store);
    errdefer backend.allocator.destroy(retired);
    var candidate = backend.runs.fork();
    errdefer candidate.deinit(backend.allocator);
    try appendOwnedRuns(&candidate, backend.allocator, src);
    retired.* = backend.runs;
    backend.runs = candidate;
    backend.retireRunStore(retired);
}

fn normalizeL0Publication(runs: []Run) void {
    // Files emitted by one sorted publication form one precedence generation.
    // Keeping the generation common is what lets later size-tiered rewrites
    // preserve chronology even when partitioning produces multiple files.
    var publication_sequence: u64 = 0;
    for (runs) |run| {
        if (run.level == 0 and run.visibility_id == 0) publication_sequence = @max(publication_sequence, run.id);
    }
    if (publication_sequence != 0) {
        for (runs) |*run| {
            if (run.level == 0 and run.visibility_id == 0) run.visibility_id = publication_sequence;
        }
    }
}

pub fn appendOwnedRuns(dst: anytype, allocator: std.mem.Allocator, src: *std.ArrayListUnmanaged(Run)) !void {
    if (comptime @TypeOf(dst) == *@import("run_store.zig").Store) {
        var candidate = dst.fork();
        errdefer candidate.deinit(allocator);
        for (src.items) |run| try candidate.stage(allocator, run);
        for (src.items) |*run| {
            candidate.adopt(run);
            if (run.owner) |owner| owner.release(allocator);
            disarmRun(run);
        }
        std.mem.swap(@import("run_store.zig").Store, dst, &candidate);
        candidate.deinit(allocator);
        src.items.len = 0;
        src.deinit(allocator);
        src.* = .empty;
        return;
    }
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
    return @max(@as(usize, 1), @min(
        backend.options.max_run_file_bytes,
        lsm_table_file.max_entry_data_len,
        physicalRunFileLimit(BackendType, backend),
    ));
}

fn physicalRunFileLimit(comptime BackendType: type, backend: *BackendType) usize {
    return @max(@as(usize, 1), @min(
        backend.options.max_run_file_physical_bytes,
        repository_mod.maxRunFileReadBytes(),
    ));
}

const PartitionKey = ?*const fn ([]const u8) []const u8;

fn splitOwnedEntriesEnd(entries: []const state_mod.OwnedEntry, start: usize, target_bytes: usize, partition_prefix_bytes: usize, partition_key: PartitionKey) usize {
    var total: usize = 0;
    var end = start;
    while (end < entries.len) : (end += 1) {
        if (end > start and !sameRunPartition(
            entries[start].namespace_name,
            entries[start].key,
            entries[end].namespace_name,
            entries[end].key,
            partition_prefix_bytes,
            partition_key,
        )) break;
        const entry_bytes = estimateOwnedEntryBytes(entries[end]);
        if (end > start and total +| entry_bytes > target_bytes) break;
        total +|= entry_bytes;
    }
    return end;
}

fn splitStateEnd(state: *const State, start: usize, target_bytes: usize, prefix_bytes: usize, partition: PartitionKey) usize {
    var cursor: State.EntryCursor = .{};
    const first = cursor.at(state, start);
    var total: usize = 0;
    var end = start;
    while (end < state.entryCount()) : (end += 1) {
        const entry = cursor.at(state, end);
        if (end > start and !sameRunPartition(first.namespace_name, first.key, entry.namespace_name, entry.key, prefix_bytes, partition)) break;
        const bytes = estimateOwnedEntryBytes(entry);
        if (end > start and total +| bytes > target_bytes) break;
        total +|= bytes;
    }
    return end;
}

fn outputEntryLimit(backend: anytype) usize {
    if (comptime @hasField(@TypeOf(backend.options), "max_run_file_entries")) {
        if (backend.options.max_run_file_entries != 0) return backend.options.max_run_file_entries;
    }
    return std.math.maxInt(usize);
}

fn splitTableEntriesEnd(entries: []const lsm_table_file.Entry, start: usize, target_bytes: usize, partition_prefix_bytes: usize, partition_key: PartitionKey) usize {
    var total: usize = 0;
    var end = start;
    while (end < entries.len) : (end += 1) {
        if (end > start and !sameRunPartition(
            entries[start].namespace_name,
            entries[start].key,
            entries[end].namespace_name,
            entries[end].key,
            partition_prefix_bytes,
            partition_key,
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
    partition_key: PartitionKey,
) bool {
    if (prefix_bytes == 0 and partition_key == null) return true;
    if (state_mod.compareNamespace(
        .{ .name = lhs_namespace_name },
        .{ .name = rhs_namespace_name },
    ) != .eq) return false;
    if (partition_key) |extract| return std.mem.eql(u8, extract(lhs_key), extract(rhs_key));
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

test "compaction publication OOM leaves the active run version intact" {
    const FakeBackend = struct {
        allocator: std.mem.Allocator,
        runs: std.ArrayListUnmanaged(Run) = .empty,
        obsolete_paths: std.ArrayListUnmanaged([]u8) = .empty,
        obsolete_runs: std.ArrayListUnmanaged(std.ArrayListUnmanaged(Run)) = .empty,

        fn reserveObsoletePublication(self: *@This(), path_count: usize, run_list_count: usize) !void {
            try self.obsolete_paths.ensureUnusedCapacity(self.allocator, path_count);
            try self.obsolete_runs.ensureUnusedCapacity(self.allocator, run_list_count);
        }

        fn queueObsoleteFilePathAssumeCapacity(self: *@This(), path: []u8) void {
            self.obsolete_paths.appendAssumeCapacity(path);
        }

        fn queueObsoleteRunsAssumeCapacity(self: *@This(), runs: std.ArrayListUnmanaged(Run)) void {
            self.obsolete_runs.appendAssumeCapacity(runs);
        }

        fn deinit(self: *@This()) void {
            deinitRunList(self.allocator, &self.runs);
            for (self.obsolete_paths.items) |path| self.allocator.free(path);
            self.obsolete_paths.deinit(self.allocator);
            for (self.obsolete_runs.items) |*runs| deinitRunList(self.allocator, runs);
            self.obsolete_runs.deinit(self.allocator);
        }
    };

    const makeTestRun = struct {
        fn make(allocator: std.mem.Allocator, id: u64, level: u32) !Run {
            const path = try std.fmt.allocPrint(allocator, "/runs/{}.tbl", .{id});
            errdefer allocator.free(path);
            const smallest_key = try allocator.dupe(u8, "a");
            errdefer allocator.free(smallest_key);
            const largest_key = try allocator.dupe(u8, "z");
            return .{
                .id = id,
                .level = level,
                .size_bytes = 1,
                .path = path,
                .smallest_namespace_name = null,
                .smallest_key = smallest_key,
                .largest_namespace_name = null,
                .largest_key = largest_key,
                .entry_count = 1,
                .bloom_filter = null,
                .state = null,
            };
        }
    }.make;

    var observed_preflight_failure = false;
    for (0..12) |failure_offset| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        const allocator = failing.allocator();
        var backend = FakeBackend{ .allocator = allocator };
        defer backend.deinit();
        try backend.runs.append(allocator, try makeTestRun(allocator, 1, 0));
        try backend.runs.append(allocator, try makeTestRun(allocator, 2, 0));

        var compacted_runs = std.ArrayListUnmanaged(Run).empty;
        defer discardOutputRuns(FakeBackend, &backend, &compacted_runs);
        try compacted_runs.append(allocator, try makeTestRun(allocator, 3, 1));

        failing.fail_index = failing.alloc_index + failure_offset;
        failing.resize_fail_index = failing.resize_index + failure_offset;
        const result = installCompactedRuns(
            FakeBackend,
            &backend,
            .{
                .source_level = 0,
                .source_start = 0,
                .source_len = 2,
                .target_start = 2,
                .target_len = 0,
                .output_level = 1,
            },
            2,
            2,
            0,
            &compacted_runs,
        );
        if (result) |_| {
            try std.testing.expectEqual(@as(usize, 1), run_store.count(backend));
            try std.testing.expectEqual(@as(u64, 3), run_store.at(backend, 0).*.id);
        } else |err| {
            if (err != error.OutOfMemory) return err;
            observed_preflight_failure = true;
            try std.testing.expectEqual(@as(usize, 2), run_store.count(backend));
            try std.testing.expectEqual(@as(u64, 1), run_store.at(backend, 0).*.id);
            try std.testing.expectEqual(@as(u64, 2), run_store.at(backend, 1).*.id);
            try std.testing.expectEqual(@as(usize, 1), compacted_runs.items.len);
            failing.fail_index = std.math.maxInt(usize);
            failing.resize_fail_index = std.math.maxInt(usize);
        }
    }
    try std.testing.expect(observed_preflight_failure);
}
