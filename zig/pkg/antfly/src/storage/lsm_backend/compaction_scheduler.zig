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
const resource_manager_mod = @import("../resource_manager.zig");

pub const max_tracked_work_run_ids = 64;
pub const max_in_flight_run_ids = 256;
pub const max_in_flight_key_ranges = 256;

pub const Options = struct {
    max_concurrent_jobs: usize = 1,
    max_in_flight_input_bytes: u64 = 128 * 1024 * 1024,
    resource_reservation_bytes: u64 = 32 * 1024 * 1024,
    allow_oversized_single_job: bool = true,
};

pub const Work = struct {
    score: u64 = 0,
    input_runs: usize = 0,
    input_bytes: u64 = 0,
    run_ids: [max_tracked_work_run_ids]u64 = undefined,
    run_count: usize = 0,
    key_range: ?KeyRange = null,
};

pub const KeyRange = struct {
    output_level: u32,
    smallest_namespace_name: ?[]const u8,
    smallest_key: []const u8,
    largest_namespace_name: ?[]const u8,
    largest_key: []const u8,
};

pub const Stats = struct {
    active_jobs: u64 = 0,
    in_flight_input_bytes: u64 = 0,
    active_oldest_age_ns: u64 = 0,
    grants: u64 = 0,
    completions: u64 = 0,
    denied_capacity: u64 = 0,
    denied_resource_pressure: u64 = 0,
    oversized_grants: u64 = 0,
    oversized_skips: u64 = 0,
    remembered_candidates: u64 = 0,
    remembered_retries: u64 = 0,
    remembered_hits: u64 = 0,
    remembered_stale: u64 = 0,
    conflict_denials: u64 = 0,
};

pub const Grant = struct {
    scheduler: *Scheduler,
    input_bytes: u64,
    started_ns: u64 = 0,
    run_ids: [max_tracked_work_run_ids]u64 = undefined,
    run_count: usize = 0,
    key_range: ?KeyRange = null,
    reservation: ?resource_manager_mod.Reservation = null,
    completed: bool = false,

    pub fn complete(self: *Grant) void {
        if (self.completed) return;
        self.completed = true;
        if (self.reservation) |*reservation| reservation.release();
        self.scheduler.complete(self.input_bytes, self.run_ids[0..self.run_count], self.key_range, self.started_ns);
    }
};

pub const Scheduler = struct {
    options: Options = .{},
    active_jobs: usize = 0,
    in_flight_input_bytes: u64 = 0,
    in_flight_run_ids: [max_in_flight_run_ids]u64 = undefined,
    in_flight_run_count: usize = 0,
    in_flight_key_ranges: [max_in_flight_key_ranges]KeyRange = undefined,
    in_flight_key_range_count: usize = 0,
    active_start_ns: [max_in_flight_run_ids]u64 = undefined,
    active_start_count: usize = 0,
    grants: u64 = 0,
    completions: u64 = 0,
    denied_capacity: u64 = 0,
    denied_resource_pressure: u64 = 0,
    oversized_grants: u64 = 0,
    oversized_skips: u64 = 0,
    remembered_candidates: u64 = 0,
    remembered_retries: u64 = 0,
    remembered_hits: u64 = 0,
    remembered_stale: u64 = 0,
    conflict_denials: u64 = 0,

    pub fn init(options: Options) Scheduler {
        return .{ .options = options };
    }

    pub fn tryAcquire(self: *Scheduler, work: Work, resource_manager: ?*resource_manager_mod.ResourceManager) ?Grant {
        return self.tryAcquireAt(work, resource_manager, 0);
    }

    pub fn tryAcquireAt(self: *Scheduler, work: Work, resource_manager: ?*resource_manager_mod.ResourceManager, now_ns: u64) ?Grant {
        if (work.score == 0 or work.input_runs == 0) {
            self.denied_capacity += 1;
            return null;
        }
        if (work.run_count > work.run_ids.len or self.conflictsWithInFlightRuns(work.run_ids[0..work.run_count])) {
            self.conflict_denials += 1;
            return null;
        }
        if (work.key_range) |range| {
            if (self.conflictsWithInFlightKeyRange(range)) {
                self.conflict_denials += 1;
                return null;
            }
            if (self.in_flight_key_range_count >= self.in_flight_key_ranges.len) {
                self.denied_capacity += 1;
                return null;
            }
        }
        if (work.run_count > max_in_flight_run_ids - self.in_flight_run_count) {
            self.denied_capacity += 1;
            return null;
        }
        const max_jobs = @max(@as(usize, 1), self.options.max_concurrent_jobs);
        if (self.active_jobs >= max_jobs) {
            self.denied_capacity += 1;
            return null;
        }
        if (self.active_start_count >= self.active_start_ns.len) {
            self.denied_capacity += 1;
            return null;
        }

        const max_bytes = self.options.max_in_flight_input_bytes;
        const next_bytes = self.in_flight_input_bytes +| work.input_bytes;
        var oversized = false;
        if (max_bytes > 0 and next_bytes > max_bytes) {
            oversized = self.options.allow_oversized_single_job and self.active_jobs == 0 and self.in_flight_input_bytes == 0;
            if (!oversized) {
                self.denied_capacity += 1;
                return null;
            }
        }

        var reservation: ?resource_manager_mod.Reservation = null;
        if (resource_manager) |manager| {
            const reserve_bytes = self.options.resource_reservation_bytes;
            if (reserve_bytes > 0) {
                reservation = manager.reserve(.lsm_compaction_work, reserve_bytes) catch {
                    self.denied_resource_pressure += 1;
                    return null;
                };
            }
        }

        self.active_jobs += 1;
        self.in_flight_input_bytes = next_bytes;
        self.addInFlightRuns(work.run_ids[0..work.run_count]);
        self.addInFlightKeyRange(work.key_range);
        self.addActiveStart(now_ns);
        self.grants += 1;
        if (oversized) self.oversized_grants += 1;
        var grant = Grant{
            .scheduler = self,
            .input_bytes = work.input_bytes,
            .started_ns = now_ns,
            .key_range = work.key_range,
            .reservation = reservation,
        };
        grant.run_count = work.run_count;
        if (work.run_count > 0) {
            @memcpy(grant.run_ids[0..work.run_count], work.run_ids[0..work.run_count]);
        }
        return grant;
    }

    fn complete(self: *Scheduler, input_bytes: u64, run_ids: []const u64, key_range: ?KeyRange, started_ns: u64) void {
        self.active_jobs -|= 1;
        self.in_flight_input_bytes -|= input_bytes;
        self.removeInFlightRuns(run_ids);
        self.removeInFlightKeyRange(key_range);
        self.removeActiveStart(started_ns);
        self.completions += 1;
    }

    fn conflictsWithInFlightRuns(self: *const Scheduler, run_ids: []const u64) bool {
        for (run_ids) |candidate| {
            for (self.in_flight_run_ids[0..self.in_flight_run_count]) |active| {
                if (candidate == active) return true;
            }
        }
        return false;
    }

    fn conflictsWithInFlightKeyRange(self: *const Scheduler, range: KeyRange) bool {
        for (self.in_flight_key_ranges[0..self.in_flight_key_range_count]) |active| {
            if (keyRangesOverlap(active, range)) return true;
        }
        return false;
    }

    fn addInFlightRuns(self: *Scheduler, run_ids: []const u64) void {
        if (run_ids.len == 0) return;
        @memcpy(self.in_flight_run_ids[self.in_flight_run_count .. self.in_flight_run_count + run_ids.len], run_ids);
        self.in_flight_run_count += run_ids.len;
    }

    fn addInFlightKeyRange(self: *Scheduler, key_range: ?KeyRange) void {
        const range = key_range orelse return;
        self.in_flight_key_ranges[self.in_flight_key_range_count] = range;
        self.in_flight_key_range_count += 1;
    }

    fn removeInFlightRuns(self: *Scheduler, run_ids: []const u64) void {
        for (run_ids) |released| {
            var idx: usize = 0;
            while (idx < self.in_flight_run_count) : (idx += 1) {
                if (self.in_flight_run_ids[idx] != released) continue;
                const tail_len = self.in_flight_run_count - idx - 1;
                if (tail_len > 0) {
                    std.mem.copyForwards(u64, self.in_flight_run_ids[idx .. idx + tail_len], self.in_flight_run_ids[idx + 1 .. self.in_flight_run_count]);
                }
                self.in_flight_run_count -= 1;
                break;
            }
        }
    }

    fn removeInFlightKeyRange(self: *Scheduler, key_range: ?KeyRange) void {
        const released = key_range orelse return;
        var idx: usize = 0;
        while (idx < self.in_flight_key_range_count) : (idx += 1) {
            if (!keyRangesEqual(self.in_flight_key_ranges[idx], released)) continue;
            const tail_len = self.in_flight_key_range_count - idx - 1;
            if (tail_len > 0) {
                std.mem.copyForwards(KeyRange, self.in_flight_key_ranges[idx .. idx + tail_len], self.in_flight_key_ranges[idx + 1 .. self.in_flight_key_range_count]);
            }
            self.in_flight_key_range_count -= 1;
            return;
        }
        if (self.in_flight_key_range_count > 0) self.in_flight_key_range_count -= 1;
    }

    fn addActiveStart(self: *Scheduler, started_ns: u64) void {
        self.active_start_ns[self.active_start_count] = started_ns;
        self.active_start_count += 1;
    }

    fn removeActiveStart(self: *Scheduler, started_ns: u64) void {
        var idx: usize = 0;
        while (idx < self.active_start_count) : (idx += 1) {
            if (self.active_start_ns[idx] != started_ns) continue;
            const tail_len = self.active_start_count - idx - 1;
            if (tail_len > 0) {
                std.mem.copyForwards(u64, self.active_start_ns[idx .. idx + tail_len], self.active_start_ns[idx + 1 .. self.active_start_count]);
            }
            self.active_start_count -= 1;
            return;
        }
        if (self.active_start_count > 0) self.active_start_count -= 1;
    }

    fn activeOldestAgeNs(self: *const Scheduler, now_ns: u64) u64 {
        if (now_ns == 0 or self.active_start_count == 0) return 0;
        var oldest = self.active_start_ns[0];
        for (self.active_start_ns[1..self.active_start_count]) |started| {
            oldest = @min(oldest, started);
        }
        return if (now_ns >= oldest) now_ns - oldest else 0;
    }

    pub fn noteRememberedCandidate(self: *Scheduler) void {
        self.remembered_candidates += 1;
    }

    pub fn noteRememberedRetry(self: *Scheduler) void {
        self.remembered_retries += 1;
    }

    pub fn noteRememberedHit(self: *Scheduler) void {
        self.remembered_hits += 1;
    }

    pub fn noteRememberedStale(self: *Scheduler) void {
        self.remembered_stale += 1;
    }

    pub fn noteConflictDenial(self: *Scheduler) void {
        self.conflict_denials += 1;
    }

    pub fn noteOversizedSkips(self: *Scheduler, count: u64) void {
        self.oversized_skips +|= count;
    }

    pub fn snapshot(self: *const Scheduler) Stats {
        return self.snapshotAt(0);
    }

    pub fn snapshotAt(self: *const Scheduler, now_ns: u64) Stats {
        return .{
            .active_jobs = @intCast(self.active_jobs),
            .in_flight_input_bytes = self.in_flight_input_bytes,
            .active_oldest_age_ns = self.activeOldestAgeNs(now_ns),
            .grants = self.grants,
            .completions = self.completions,
            .denied_capacity = self.denied_capacity,
            .denied_resource_pressure = self.denied_resource_pressure,
            .oversized_grants = self.oversized_grants,
            .oversized_skips = self.oversized_skips,
            .remembered_candidates = self.remembered_candidates,
            .remembered_retries = self.remembered_retries,
            .remembered_hits = self.remembered_hits,
            .remembered_stale = self.remembered_stale,
            .conflict_denials = self.conflict_denials,
        };
    }
};

fn keyRangesOverlap(lhs: KeyRange, rhs: KeyRange) bool {
    if (lhs.output_level != rhs.output_level) return false;
    return compareBound(lhs.smallest_namespace_name, lhs.smallest_key, rhs.largest_namespace_name, rhs.largest_key) != .gt and
        compareBound(lhs.largest_namespace_name, lhs.largest_key, rhs.smallest_namespace_name, rhs.smallest_key) != .lt;
}

fn keyRangesEqual(lhs: KeyRange, rhs: KeyRange) bool {
    return lhs.output_level == rhs.output_level and
        namespaceNameEql(lhs.smallest_namespace_name, rhs.smallest_namespace_name) and
        std.mem.eql(u8, lhs.smallest_key, rhs.smallest_key) and
        namespaceNameEql(lhs.largest_namespace_name, rhs.largest_namespace_name) and
        std.mem.eql(u8, lhs.largest_key, rhs.largest_key);
}

fn compareBound(lhs_namespace_name: ?[]const u8, lhs_key: []const u8, rhs_namespace_name: ?[]const u8, rhs_key: []const u8) std.math.Order {
    const namespace_order = compareNamespaceName(lhs_namespace_name, rhs_namespace_name);
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, lhs_key, rhs_key);
}

fn compareNamespaceName(lhs: ?[]const u8, rhs: ?[]const u8) std.math.Order {
    if (lhs == null and rhs == null) return .eq;
    if (lhs == null) return .lt;
    if (rhs == null) return .gt;
    return std.mem.order(u8, lhs.?, rhs.?);
}

fn namespaceNameEql(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return std.mem.eql(u8, lhs.?, rhs.?);
}

fn testWork(score: u64, input_bytes: u64, run_ids: []const u64) Work {
    var work = Work{
        .score = score,
        .input_runs = run_ids.len,
        .input_bytes = input_bytes,
        .run_count = run_ids.len,
    };
    if (run_ids.len > 0) {
        @memcpy(work.run_ids[0..run_ids.len], run_ids);
    }
    return work;
}

fn testRange(output_level: u32, smallest_key: []const u8, largest_key: []const u8) KeyRange {
    return .{
        .output_level = output_level,
        .smallest_namespace_name = "docs",
        .smallest_key = smallest_key,
        .largest_namespace_name = "docs",
        .largest_key = largest_key,
    };
}

test "lsm compaction scheduler denies overlapping in-flight run ids" {
    var scheduler = Scheduler.init(.{
        .max_concurrent_jobs = 2,
        .max_in_flight_input_bytes = 1024 * 1024,
        .resource_reservation_bytes = 0,
    });

    var first = scheduler.tryAcquire(testWork(1, 10, &.{ 1, 2 }), null) orelse return error.TestUnexpectedResult;
    defer first.complete();

    try std.testing.expect(scheduler.tryAcquire(testWork(1, 10, &.{ 2, 3 }), null) == null);
    var stats = scheduler.snapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.active_jobs);
    try std.testing.expectEqual(@as(u64, 1), stats.conflict_denials);

    first.complete();
    var second = scheduler.tryAcquire(testWork(1, 10, &.{ 2, 3 }), null) orelse return error.TestUnexpectedResult;
    second.complete();

    stats = scheduler.snapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.active_jobs);
    try std.testing.expectEqual(@as(u64, 2), stats.grants);
    try std.testing.expectEqual(@as(u64, 2), stats.completions);
}

test "lsm compaction scheduler admits non-overlapping concurrent run ids" {
    var scheduler = Scheduler.init(.{
        .max_concurrent_jobs = 2,
        .max_in_flight_input_bytes = 1024 * 1024,
        .resource_reservation_bytes = 0,
    });

    var first = scheduler.tryAcquire(testWork(1, 10, &.{ 1, 2 }), null) orelse return error.TestUnexpectedResult;
    defer first.complete();
    var second = scheduler.tryAcquire(testWork(1, 10, &.{ 3, 4 }), null) orelse return error.TestUnexpectedResult;
    defer second.complete();

    var stats = scheduler.snapshot();
    try std.testing.expectEqual(@as(u64, 2), stats.active_jobs);
    try std.testing.expectEqual(@as(u64, 20), stats.in_flight_input_bytes);
    try std.testing.expectEqual(@as(u64, 2), stats.grants);
    try std.testing.expectEqual(@as(u64, 0), stats.conflict_denials);

    second.complete();
    first.complete();
    stats = scheduler.snapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.active_jobs);
    try std.testing.expectEqual(@as(u64, 0), stats.in_flight_input_bytes);
    try std.testing.expectEqual(@as(u64, 2), stats.completions);
}

test "lsm compaction scheduler denies overlapping in-flight key ranges" {
    var scheduler = Scheduler.init(.{
        .max_concurrent_jobs = 2,
        .max_in_flight_input_bytes = 1024 * 1024,
        .resource_reservation_bytes = 0,
    });

    var first_work = testWork(1, 10, &.{1});
    first_work.key_range = testRange(1, "doc:a", "doc:m");
    var first = scheduler.tryAcquire(first_work, null) orelse return error.TestUnexpectedResult;
    defer first.complete();

    var overlapping_work = testWork(1, 10, &.{2});
    overlapping_work.key_range = testRange(1, "doc:h", "doc:z");
    try std.testing.expect(scheduler.tryAcquire(overlapping_work, null) == null);

    var different_level_work = testWork(1, 10, &.{3});
    different_level_work.key_range = testRange(2, "doc:h", "doc:z");
    var different_level = scheduler.tryAcquire(different_level_work, null) orelse return error.TestUnexpectedResult;
    different_level.complete();

    var disjoint_work = testWork(1, 10, &.{4});
    disjoint_work.key_range = testRange(1, "doc:n", "doc:z");
    var disjoint = scheduler.tryAcquire(disjoint_work, null) orelse return error.TestUnexpectedResult;
    disjoint.complete();

    const stats = scheduler.snapshot();
    try std.testing.expectEqual(@as(u64, 3), stats.grants);
    try std.testing.expectEqual(@as(u64, 1), stats.conflict_denials);
}

test "lsm compaction scheduler reports oldest active job age" {
    var scheduler = Scheduler.init(.{
        .max_concurrent_jobs = 2,
        .max_in_flight_input_bytes = 1024 * 1024,
        .resource_reservation_bytes = 0,
    });

    var first = scheduler.tryAcquireAt(testWork(1, 10, &.{1}), null, 100) orelse return error.TestUnexpectedResult;
    defer first.complete();
    var second = scheduler.tryAcquireAt(testWork(1, 10, &.{2}), null, 175) orelse return error.TestUnexpectedResult;
    defer second.complete();

    var stats = scheduler.snapshotAt(250);
    try std.testing.expectEqual(@as(u64, 2), stats.active_jobs);
    try std.testing.expectEqual(@as(u64, 150), stats.active_oldest_age_ns);

    first.complete();
    stats = scheduler.snapshotAt(250);
    try std.testing.expectEqual(@as(u64, 1), stats.active_jobs);
    try std.testing.expectEqual(@as(u64, 75), stats.active_oldest_age_ns);

    second.complete();
    stats = scheduler.snapshotAt(250);
    try std.testing.expectEqual(@as(u64, 0), stats.active_jobs);
    try std.testing.expectEqual(@as(u64, 0), stats.active_oldest_age_ns);
}
