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
const builtin = @import("builtin");

const MiB: u64 = 1024 * 1024;
const dense_replay_window_min_bytes: u64 = 16 * MiB;
const dense_replay_window_growth_numerator: u64 = 5;
const dense_replay_window_growth_denominator: u64 = 4;
const dense_replay_window_shrink_numerator: u64 = 3;
const dense_replay_window_shrink_denominator: u64 = 4;
const dense_replay_finish_target_ns: u64 = 3 * std.time.ns_per_s;
const dense_replay_finish_hard_ns: u64 = 8 * std.time.ns_per_s;
const dense_replay_write_pressure_hard_ns: u64 = std.time.ns_per_s;

pub const Slice = enum(u8) {
    lsm_block_table_cache,
    lsm_compaction_work,
    lsm_in_memory_state,
    lsm_wal_write_working_set,
    hbc_node_metadata_cache,
    dense_search_working_set,
    dense_apply_working_set,
    dense_routing_working_set,
    derived_replay_window,
    full_text_pending_segments,
    derived_backlog,
    text_merge_buffers,
    algebraic_tensor_accumulators,

    pub fn name(self: Slice) []const u8 {
        return switch (self) {
            .lsm_block_table_cache => "lsm.block_table_cache",
            .lsm_compaction_work => "lsm.compaction_work",
            .lsm_in_memory_state => "lsm.in_memory_state",
            .lsm_wal_write_working_set => "lsm.wal_write_working_set",
            .hbc_node_metadata_cache => "hbc.node_metadata_cache",
            .dense_search_working_set => "dense.search_working_set",
            .dense_apply_working_set => "dense.apply_working_set",
            .dense_routing_working_set => "dense.routing_working_set",
            .derived_replay_window => "derived.replay_window",
            .full_text_pending_segments => "full_text.pending_segments",
            .derived_backlog => "derived.backlog",
            .text_merge_buffers => "text_merge.buffers",
            .algebraic_tensor_accumulators => "algebraic.tensor_accumulators",
        };
    }
};

pub const slice_count: usize = 13;

pub const Budget = struct {
    soft_limit_bytes: u64 = 0,
    hard_limit_bytes: u64 = 0,
};

pub const Pressure = enum(u8) {
    normal,
    soft,
    hard,
};

pub const PressureAction = enum(u8) {
    report,
    shrink_cache,
    defer_background_work,
    throttle_writes,
    reject_work,

    pub fn name(self: PressureAction) []const u8 {
        return switch (self) {
            .report => "report",
            .shrink_cache => "shrink_cache",
            .defer_background_work => "defer_background_work",
            .throttle_writes => "throttle_writes",
            .reject_work => "reject_work",
        };
    }
};

pub const Policy = struct {
    soft_action: PressureAction = .report,
    hard_action: PressureAction = .report,
};

pub const Options = struct {
    budgets: [slice_count]Budget = defaultBudgets(),
    policies: [slice_count]Policy = defaultPolicies(),

    pub fn defaultBudgets() [slice_count]Budget {
        return .{
            .{ .soft_limit_bytes = 192 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 512 * 1024 * 1024, .hard_limit_bytes = 768 * 1024 * 1024 },
            .{ .soft_limit_bytes = 512 * 1024 * 1024, .hard_limit_bytes = 768 * 1024 * 1024 },
            .{ .soft_limit_bytes = 256 * 1024 * 1024, .hard_limit_bytes = 512 * 1024 * 1024 },
            .{ .soft_limit_bytes = 384 * 1024 * 1024, .hard_limit_bytes = 512 * 1024 * 1024 },
            .{ .soft_limit_bytes = 96 * 1024 * 1024, .hard_limit_bytes = 160 * 1024 * 1024 },
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 96 * 1024 * 1024, .hard_limit_bytes = 160 * 1024 * 1024 },
            .{ .soft_limit_bytes = 192 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 192 * 1024 * 1024 },
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 192 * 1024 * 1024 },
            .{ .soft_limit_bytes = 96 * 1024 * 1024, .hard_limit_bytes = 160 * 1024 * 1024 },
        };
    }

    pub fn defaultPolicies() [slice_count]Policy {
        return .{
            .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .{ .soft_action = .report, .hard_action = .throttle_writes },
            .{ .soft_action = .report, .hard_action = .throttle_writes },
            .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .{ .soft_action = .report, .hard_action = .throttle_writes },
            .{ .soft_action = .report, .hard_action = .throttle_writes },
            .{ .soft_action = .report, .hard_action = .throttle_writes },
            .{ .soft_action = .report, .hard_action = .reject_work },
            .{ .soft_action = .defer_background_work, .hard_action = .defer_background_work },
            .{ .soft_action = .throttle_writes, .hard_action = .throttle_writes },
            .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .{ .soft_action = .throttle_writes, .hard_action = .reject_work },
        };
    }
};

pub const SliceStats = struct {
    name: []const u8,
    used_bytes: u64 = 0,
    peak_bytes: u64 = 0,
    soft_limit_bytes: u64 = 0,
    hard_limit_bytes: u64 = 0,
    soft_limit_events: u64 = 0,
    hard_limit_rejections: u64 = 0,
    pressure: Pressure = .normal,
    soft_action: PressureAction = .report,
    hard_action: PressureAction = .report,
};

pub const Stats = struct {
    slices: [slice_count]SliceStats,
};

pub const DenseReplayWindowBudgetOptions = struct {
    default_bytes: u64,
    max_bytes: u64,
    min_bytes: u64 = dense_replay_window_min_bytes,
};

pub const DenseReplayWindowResult = struct {
    finish_ns: u64 = 0,
    write_pressure_ns: u64 = 0,
    write_pressure_compactions: u64 = 0,
};

const MutableSlice = struct {
    budget: Budget = .{},
    policy: Policy = .{},
    used_bytes: u64 = 0,
    peak_bytes: u64 = 0,
    soft_limit_events: u64 = 0,
    hard_limit_rejections: u64 = 0,
};

pub const ResourceManager = struct {
    mutex: std.atomic.Mutex = .unlocked,
    slices: [slice_count]MutableSlice,
    dense_replay_window_budget_bytes: u64 = 0,
    dense_replay_last_finish_ns: u64 = 0,
    dense_replay_last_write_pressure_ns: u64 = 0,
    dense_replay_last_write_pressure_compactions: u64 = 0,

    pub fn init(options: Options) ResourceManager {
        var slices: [slice_count]MutableSlice = undefined;
        for (&slices, 0..) |*slice, i| {
            slice.* = .{
                .budget = options.budgets[i],
                .policy = options.policies[i],
            };
        }
        return .{ .slices = slices };
    }

    pub fn reserve(self: *ResourceManager, slice: Slice, bytes: u64) !Reservation {
        if (bytes == 0) return .{ .manager = self, .slice = slice, .bytes = 0 };

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const idx = sliceIndex(slice);
        const state = &self.slices[idx];
        const next = std.math.add(u64, state.used_bytes, bytes) catch {
            state.hard_limit_rejections += 1;
            return error.ResourceBudgetExceeded;
        };
        if (state.budget.hard_limit_bytes > 0 and next > state.budget.hard_limit_bytes) {
            state.hard_limit_rejections += 1;
            return error.ResourceBudgetExceeded;
        }
        state.used_bytes = next;
        state.peak_bytes = @max(state.peak_bytes, next);
        if (state.budget.soft_limit_bytes > 0 and next > state.budget.soft_limit_bytes) {
            state.soft_limit_events += 1;
        }
        return .{ .manager = self, .slice = slice, .bytes = bytes };
    }

    pub fn releaseBytes(self: *ResourceManager, slice: Slice, bytes: u64) void {
        if (bytes == 0) return;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const state = &self.slices[sliceIndex(slice)];
        state.used_bytes -|= bytes;
    }

    pub fn adjustUsage(self: *ResourceManager, slice: Slice, current: *u64, next: u64) !void {
        if (next == current.*) return;
        if (next < current.*) {
            self.releaseBytes(slice, current.* - next);
            current.* = next;
            return;
        }

        const delta = next - current.*;
        var reservation = try self.reserve(slice, delta);
        reservation.released = true;
        current.* = next;
    }

    pub fn observeUsage(self: *ResourceManager, slice: Slice, current: *u64, next: u64) void {
        if (next == current.*) return;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const state = &self.slices[sliceIndex(slice)];
        state.used_bytes = state.used_bytes -| current.*;
        state.used_bytes = state.used_bytes +| next;
        current.* = next;
        state.peak_bytes = @max(state.peak_bytes, state.used_bytes);
        if (state.budget.soft_limit_bytes > 0 and state.used_bytes > state.budget.soft_limit_bytes) {
            state.soft_limit_events += 1;
        }
        if (state.budget.hard_limit_bytes > 0 and state.used_bytes > state.budget.hard_limit_bytes) {
            state.hard_limit_rejections += 1;
        }
    }

    pub fn snapshot(self: *ResourceManager) Stats {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var stats: [slice_count]SliceStats = undefined;
        inline for (.{ Slice.lsm_block_table_cache, Slice.lsm_compaction_work, Slice.lsm_in_memory_state, Slice.lsm_wal_write_working_set, Slice.hbc_node_metadata_cache, Slice.dense_search_working_set, Slice.dense_apply_working_set, Slice.dense_routing_working_set, Slice.derived_replay_window, Slice.full_text_pending_segments, Slice.derived_backlog, Slice.text_merge_buffers, Slice.algebraic_tensor_accumulators }, 0..) |slice, i| {
            const state = self.slices[i];
            stats[i] = .{
                .name = slice.name(),
                .used_bytes = state.used_bytes,
                .peak_bytes = state.peak_bytes,
                .soft_limit_bytes = state.budget.soft_limit_bytes,
                .hard_limit_bytes = state.budget.hard_limit_bytes,
                .soft_limit_events = state.soft_limit_events,
                .hard_limit_rejections = state.hard_limit_rejections,
                .pressure = pressureFor(state.budget, state.used_bytes),
                .soft_action = state.policy.soft_action,
                .hard_action = state.policy.hard_action,
            };
        }
        return .{ .slices = stats };
    }

    pub fn sliceStats(self: *ResourceManager, slice: Slice) SliceStats {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const state = self.slices[sliceIndex(slice)];
        return sliceStatsFromState(slice, state);
    }

    pub fn denseReplayWindowBudget(self: *ResourceManager, options: DenseReplayWindowBudgetOptions) u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const cap = self.denseReplayWindowHardCapLocked(options);
        var current = self.dense_replay_window_budget_bytes;
        if (current == 0) current = options.default_bytes;
        current = clampU64(current, options.min_bytes, cap);

        const memory_pressure = self.slicePressureLocked(.derived_replay_window) != .normal or
            self.slicePressureLocked(.dense_apply_working_set) != .normal or
            self.slicePressureLocked(.dense_routing_working_set) != .normal;
        const finish_too_slow = self.dense_replay_last_finish_ns > dense_replay_finish_hard_ns;
        const write_pressure_too_high = self.dense_replay_last_write_pressure_compactions > 0 and
            self.dense_replay_last_write_pressure_ns > dense_replay_write_pressure_hard_ns;

        if (memory_pressure or finish_too_slow or write_pressure_too_high) {
            current = current * dense_replay_window_shrink_numerator / dense_replay_window_shrink_denominator;
        } else if (self.dense_replay_last_finish_ns == 0 or self.dense_replay_last_finish_ns < dense_replay_finish_target_ns) {
            current = current * dense_replay_window_growth_numerator / dense_replay_window_growth_denominator;
        }

        current = clampU64(current, options.min_bytes, cap);
        self.dense_replay_window_budget_bytes = current;
        return current;
    }

    pub fn noteDenseReplayWindowResult(self: *ResourceManager, result: DenseReplayWindowResult) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        self.dense_replay_last_finish_ns = result.finish_ns;
        self.dense_replay_last_write_pressure_ns = result.write_pressure_ns;
        self.dense_replay_last_write_pressure_compactions = result.write_pressure_compactions;
    }

    fn denseReplayWindowHardCapLocked(self: *ResourceManager, options: DenseReplayWindowBudgetOptions) u64 {
        var cap = @max(options.min_bytes, options.max_bytes);
        cap = @min(cap, self.sliceWindowCapLocked(.derived_replay_window, 1));
        cap = @min(cap, self.sliceWindowCapLocked(.dense_apply_working_set, 2));
        cap = @min(cap, self.sliceWindowCapLocked(.dense_routing_working_set, 2));
        return @max(options.min_bytes, cap);
    }

    fn sliceWindowCapLocked(self: *ResourceManager, slice: Slice, reserve_divisor: u64) u64 {
        const state = self.slices[sliceIndex(slice)];
        if (state.budget.hard_limit_bytes == 0) return std.math.maxInt(u64);
        const available = state.budget.hard_limit_bytes -| state.used_bytes;
        const reserved = if (reserve_divisor == 0) available else available / reserve_divisor;
        return @max(@as(u64, 1), reserved);
    }

    fn slicePressureLocked(self: *ResourceManager, slice: Slice) Pressure {
        const state = self.slices[sliceIndex(slice)];
        return pressureFor(state.budget, state.used_bytes);
    }
};

pub const Reservation = struct {
    manager: *ResourceManager,
    slice: Slice,
    bytes: u64,
    released: bool = false,

    pub fn release(self: *Reservation) void {
        if (self.released) return;
        self.manager.releaseBytes(self.slice, self.bytes);
        self.released = true;
    }
};

fn sliceIndex(slice: Slice) usize {
    return @intFromEnum(slice);
}

fn pressureFor(budget: Budget, used_bytes: u64) Pressure {
    if (budget.hard_limit_bytes > 0 and used_bytes > budget.hard_limit_bytes) return .hard;
    if (budget.soft_limit_bytes > 0 and used_bytes > budget.soft_limit_bytes) return .soft;
    return .normal;
}

fn sliceStatsFromState(slice: Slice, state: MutableSlice) SliceStats {
    return .{
        .name = slice.name(),
        .used_bytes = state.used_bytes,
        .peak_bytes = state.peak_bytes,
        .soft_limit_bytes = state.budget.soft_limit_bytes,
        .hard_limit_bytes = state.budget.hard_limit_bytes,
        .soft_limit_events = state.soft_limit_events,
        .hard_limit_rejections = state.hard_limit_rejections,
        .pressure = pressureFor(state.budget, state.used_bytes),
        .soft_action = state.policy.soft_action,
        .hard_action = state.policy.hard_action,
    };
}

fn clampU64(value: u64, min_value: u64, max_value: u64) u64 {
    if (max_value <= min_value) return min_value;
    return @min(@max(value, min_value), max_value);
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        if (comptime builtin.os.tag == .freestanding) {
            std.atomic.spinLoopHint();
            continue;
        }
        std.Thread.yield() catch {};
    }
}

test "resource manager tracks reservations and releases" {
    var manager = ResourceManager.init(.{});

    var reservation = try manager.reserve(.full_text_pending_segments, 4096);
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 4096), stats.slices[sliceIndex(.full_text_pending_segments)].used_bytes);
    try std.testing.expectEqual(@as(u64, 4096), stats.slices[sliceIndex(.full_text_pending_segments)].peak_bytes);

    reservation.release();
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.slices[sliceIndex(.full_text_pending_segments)].used_bytes);
}

test "resource manager records soft and hard budget pressure" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.derived_backlog)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });

    var first = try manager.reserve(.derived_backlog, 12);
    defer first.release();
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.slices[sliceIndex(.derived_backlog)].soft_limit_events);

    try std.testing.expectError(error.ResourceBudgetExceeded, manager.reserve(.derived_backlog, 9));
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.slices[sliceIndex(.derived_backlog)].hard_limit_rejections);
}

test "resource manager adjusts tracked usage" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.text_merge_buffers)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    var current: u64 = 0;

    try manager.adjustUsage(.text_merge_buffers, &current, 12);
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 12), current);
    try std.testing.expectEqual(@as(u64, 12), stats.slices[sliceIndex(.text_merge_buffers)].used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[sliceIndex(.text_merge_buffers)].soft_limit_events);
    try std.testing.expectEqual(Pressure.soft, stats.slices[sliceIndex(.text_merge_buffers)].pressure);
    try std.testing.expectEqual(PressureAction.defer_background_work, stats.slices[sliceIndex(.text_merge_buffers)].soft_action);
    try std.testing.expectEqual(PressureAction.reject_work, stats.slices[sliceIndex(.text_merge_buffers)].hard_action);

    try std.testing.expectError(error.ResourceBudgetExceeded, manager.adjustUsage(.text_merge_buffers, &current, 21));
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 12), current);
    try std.testing.expectEqual(@as(u64, 12), stats.slices[sliceIndex(.text_merge_buffers)].used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[sliceIndex(.text_merge_buffers)].hard_limit_rejections);

    try manager.adjustUsage(.text_merge_buffers, &current, 4);
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 4), current);
    try std.testing.expectEqual(@as(u64, 4), stats.slices[sliceIndex(.text_merge_buffers)].used_bytes);
}

test "resource manager observes over-budget external usage" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.lsm_block_table_cache)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    var current: u64 = 0;

    manager.observeUsage(.lsm_block_table_cache, &current, 25);
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 25), current);
    try std.testing.expectEqual(@as(u64, 25), stats.slices[sliceIndex(.lsm_block_table_cache)].used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[sliceIndex(.lsm_block_table_cache)].soft_limit_events);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[sliceIndex(.lsm_block_table_cache)].hard_limit_rejections);
    try std.testing.expectEqual(Pressure.hard, stats.slices[sliceIndex(.lsm_block_table_cache)].pressure);
    try std.testing.expectEqual(PressureAction.shrink_cache, stats.slices[sliceIndex(.lsm_block_table_cache)].hard_action);

    manager.observeUsage(.lsm_block_table_cache, &current, 5);
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 5), current);
    try std.testing.expectEqual(@as(u64, 5), stats.slices[sliceIndex(.lsm_block_table_cache)].used_bytes);
    try std.testing.expectEqual(Pressure.normal, stats.slices[sliceIndex(.lsm_block_table_cache)].pressure);
}
