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

pub const max_in_flight_jobs = 256;

pub const Options = struct {
    allocator: std.mem.Allocator = std.heap.page_allocator,
    max_concurrent_jobs: usize = 1,
    max_in_flight_input_bytes: u64 = 128 * 1024 * 1024,
    resource_reservation_bytes: u64 = 32 * 1024 * 1024,
    allow_oversized_single_job: bool = true,
};

pub const Work = struct {
    score: u64 = 0,
    input_runs: usize = 0,
    input_bytes: u64 = 0,
    run_ids: []const u64 = &.{},
    /// Prepared off-lock by the owned job; borrowed until Grant.complete.
    run_id_index: ?RunIdIndex = null,
    key_range: ?KeyRange = null,
};

pub const RunIdIndex = std.AutoHashMapUnmanaged(u64, void);
/// Includes hash metadata, spare capacity, and the original ID array.
pub fn runIdMemoryBound(count: usize) u64 {
    if (count == 0) return 0;
    return @as(u64, @intCast(count)) *| 64 +| 128;
}

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
    job_id: u64 = 0,
    started_ns: u64 = 0,
    reservation: ?resource_manager_mod.Reservation = null,
    completed: bool = false,

    pub fn complete(self: *Grant) void {
        if (self.completed) return;
        self.completed = true;
        self.scheduler.complete(self.job_id, self.input_bytes);
        // Release credit only after any grant-owned index has been freed.
        if (self.reservation) |*reservation| reservation.release();
    }
};

const ActiveJob = struct {
    id: u64,
    started_ns: u64,
    run_id_index: RunIdIndex,
    owns_index: bool,
    key_range: ?KeyRange,
};

pub const Scheduler = struct {
    options: Options = .{},
    active_jobs: usize = 0,
    in_flight_input_bytes: u64 = 0,
    next_job_id: u64 = 1,
    active_job_slots: [max_in_flight_jobs]ActiveJob = undefined,
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
        const max_jobs = @max(@as(usize, 1), self.options.max_concurrent_jobs);
        if (self.active_jobs >= max_jobs) {
            self.denied_capacity += 1;
            return null;
        }
        if (self.active_jobs >= self.active_job_slots.len) {
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

        // Capacity denial must be O(1), even for a million-file candidate.
        if (work.key_range) |range| if (self.conflictsWithInFlightKeyRange(range)) {
            self.conflict_denials += 1;
            return null;
        };
        if (self.conflictsWithInFlightRuns(work)) {
            self.conflict_denials += 1;
            return null;
        }

        var reservation: ?resource_manager_mod.Reservation = null;
        if (resource_manager) |manager| {
            const reserve_bytes = self.options.resource_reservation_bytes +| if (work.run_id_index == null) runIdMemoryBound(work.run_ids.len) else @as(u64, 0);
            if (reserve_bytes > 0) {
                reservation = manager.reserve(.lsm_compaction_work, reserve_bytes) catch {
                    self.denied_resource_pressure += 1;
                    return null;
                };
            }
        }

        var index = work.run_id_index orelse RunIdIndex.empty;
        if (work.run_id_index == null) {
            index.ensureTotalCapacity(self.options.allocator, std.math.cast(u32, work.run_ids.len) orelse {
                if (reservation) |*lease| lease.release();
                self.denied_resource_pressure += 1;
                return null;
            }) catch {
                if (reservation) |*lease| lease.release();
                self.denied_resource_pressure += 1;
                return null;
            };
            for (work.run_ids) |id| index.putAssumeCapacity(id, {});
        }

        self.active_jobs += 1;
        self.in_flight_input_bytes = next_bytes;
        const job_id = self.nextJobId();
        self.active_job_slots[self.active_jobs - 1] = .{
            .id = job_id,
            .started_ns = now_ns,
            .run_id_index = index,
            .owns_index = work.run_id_index == null,
            .key_range = work.key_range,
        };
        self.grants += 1;
        if (oversized) self.oversized_grants += 1;
        return .{
            .scheduler = self,
            .input_bytes = work.input_bytes,
            .job_id = job_id,
            .started_ns = now_ns,
            .reservation = reservation,
        };
    }

    fn complete(self: *Scheduler, job_id: u64, input_bytes: u64) void {
        self.removeActiveJob(job_id);
        self.active_jobs -|= 1;
        self.in_flight_input_bytes -|= input_bytes;
        self.completions += 1;
    }

    fn nextJobId(self: *Scheduler) u64 {
        const id = self.next_job_id;
        self.next_job_id +|= 1;
        if (self.next_job_id == 0) self.next_job_id = 1;
        return id;
    }

    fn conflictsWithInFlightRuns(self: *const Scheduler, work: Work) bool {
        for (self.active_job_slots[0..self.active_jobs]) |job| {
            // Probe the smaller side when both jobs have prepared indexes.
            // A tiny active job must not force a scan of a broad candidate.
            if (work.run_id_index) |index| if (job.run_id_index.count() < work.run_ids.len) {
                var ids = job.run_id_index.keyIterator();
                while (ids.next()) |id| if (index.contains(id.*)) return true;
                continue;
            };
            for (work.run_ids) |candidate| if (job.run_id_index.contains(candidate)) return true;
        }
        return false;
    }

    fn conflictsWithInFlightKeyRange(self: *const Scheduler, range: KeyRange) bool {
        for (self.active_job_slots[0..self.active_jobs]) |job| {
            if (job.key_range) |active| {
                if (keyRangesOverlap(active, range)) return true;
            }
        }
        return false;
    }

    fn removeActiveJob(self: *Scheduler, job_id: u64) void {
        var idx: usize = 0;
        while (idx < self.active_jobs) : (idx += 1) {
            if (self.active_job_slots[idx].id != job_id) continue;
            if (self.active_job_slots[idx].owns_index)
                self.active_job_slots[idx].run_id_index.deinit(self.options.allocator);
            const tail_len = self.active_jobs - idx - 1;
            if (tail_len > 0) {
                std.mem.copyForwards(ActiveJob, self.active_job_slots[idx .. idx + tail_len], self.active_job_slots[idx + 1 .. self.active_jobs]);
            }
            return;
        }
    }

    fn activeOldestAgeNs(self: *const Scheduler, now_ns: u64) u64 {
        if (now_ns == 0 or self.active_jobs == 0) return 0;
        var oldest = self.active_job_slots[0].started_ns;
        for (self.active_job_slots[1..self.active_jobs]) |job| {
            oldest = @min(oldest, job.started_ns);
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

fn testWork(score: u64, input_bytes: u64, run_ids: []const u64) Work {
    const work = Work{
        .score = score,
        .input_runs = run_ids.len,
        .input_bytes = input_bytes,
        .run_ids = run_ids,
    };
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

test "lsm compaction scheduler tracks large run-id sets without fixed work cap" {
    var scheduler = Scheduler.init(.{
        .max_concurrent_jobs = 2,
        .max_in_flight_input_bytes = 1024 * 1024,
        .resource_reservation_bytes = 0,
    });

    var ids: [96]u64 = undefined;
    for (&ids, 0..) |*id, idx| id.* = @intCast(idx + 1);

    var first = scheduler.tryAcquire(testWork(1, 10, ids[0..]), null) orelse return error.TestUnexpectedResult;
    defer first.complete();

    var disjoint_ids: [96]u64 = undefined;
    for (&disjoint_ids, 0..) |*id, idx| id.* = @intCast(idx + 10_000);
    var second = scheduler.tryAcquire(testWork(1, 10, disjoint_ids[0..]), null) orelse return error.TestUnexpectedResult;
    second.complete();

    try std.testing.expect(scheduler.tryAcquire(testWork(1, 10, ids[95..96]), null) == null);
    first.complete();

    var after = scheduler.tryAcquire(testWork(1, 10, ids[95..96]), null) orelse return error.TestUnexpectedResult;
    after.complete();

    const stats = scheduler.snapshot();
    try std.testing.expectEqual(@as(u64, 3), stats.grants);
    try std.testing.expectEqual(@as(u64, 1), stats.conflict_denials);
    try std.testing.expectEqual(@as(u64, 0), stats.active_jobs);
}

test "lsm compaction scheduler index allocation denial releases resource credit" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var scheduler = Scheduler.init(.{ .allocator = failing.allocator(), .resource_reservation_bytes = 128 });
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    try std.testing.expect(scheduler.tryAcquire(testWork(1, 10, &.{ 9, 3, 9 }), &manager) == null);
    try std.testing.expectEqual(@as(usize, 0), scheduler.active_jobs);
    try std.testing.expectEqual(@as(u64, 1), scheduler.denied_resource_pressure);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_compaction_work).used_bytes);
    scheduler.options.allocator = std.testing.allocator;
    var grant = scheduler.tryAcquire(testWork(1, 10, &.{ 9, 3, 9 }), &manager) orelse return error.TestUnexpectedResult;
    grant.complete();
    grant.complete();
    try std.testing.expectEqual(@as(u64, 1), scheduler.completions);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_compaction_work).used_bytes);
}

test "lsm compaction scheduler prepared membership scales for concurrent admission" {
    const allocator = std.testing.allocator;
    for ([_]usize{ 1000, 10000, 50000 }) |count| {
        const ids = try allocator.alloc(u64, count * 2);
        defer allocator.free(ids);
        var first_index: RunIdIndex = .empty;
        defer first_index.deinit(allocator);
        var second_index: RunIdIndex = .empty;
        defer second_index.deinit(allocator);
        for (ids, 0..) |*id, i| id.* = i + 1;
        for (ids[0..count]) |id| try first_index.put(allocator, id, {});
        for (ids[count..]) |id| try second_index.put(allocator, id, {});
        var scheduler = Scheduler.init(.{ .allocator = allocator, .max_concurrent_jobs = 2, .resource_reservation_bytes = 0 });
        var first_work = testWork(1, 1, ids[0..count]);
        first_work.run_id_index = first_index;
        var second_work = testWork(1, 1, ids[count..]);
        second_work.run_id_index = second_index;
        var first = scheduler.tryAcquire(first_work, null) orelse return error.TestUnexpectedResult;
        defer first.complete();
        const started = @import("antfly_platform").time.monotonicNs();
        const rounds = if (@import("builtin").mode == .ReleaseFast) 100 else 1;
        for (0..rounds) |_| {
            var second = scheduler.tryAcquire(second_work, null) orelse return error.TestUnexpectedResult;
            second.complete();
        }
        const elapsed = @import("antfly_platform").time.monotonicNs() - started;
        if (@import("builtin").mode == .ReleaseFast) std.debug.print("\nLSM concurrent indexed admission inputs={d} ns_per_grant={d}\n", .{ count, elapsed / rounds });
        scheduler.options.max_concurrent_jobs = 1;
        const denied_start = @import("antfly_platform").time.monotonicNs();
        for (0..1000) |_| try std.testing.expect(scheduler.tryAcquire(second_work, null) == null);
        if (@import("builtin").mode == .ReleaseFast) std.debug.print("LSM capacity denial inputs={d} ns_per_denial={d}\n", .{ count, (@import("antfly_platform").time.monotonicNs() - denied_start) / 1000 });
        // Borrowed prepared indexes must survive all grants and denials.
        try std.testing.expectEqual(@as(u32, @intCast(count)), second_index.count());
    }
}
