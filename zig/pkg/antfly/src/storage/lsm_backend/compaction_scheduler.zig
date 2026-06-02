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
};

pub const Stats = struct {
    active_jobs: u64 = 0,
    in_flight_input_bytes: u64 = 0,
    grants: u64 = 0,
    completions: u64 = 0,
    denied_capacity: u64 = 0,
    denied_resource_pressure: u64 = 0,
    oversized_grants: u64 = 0,
    remembered_candidates: u64 = 0,
    remembered_retries: u64 = 0,
    remembered_hits: u64 = 0,
    remembered_stale: u64 = 0,
    conflict_denials: u64 = 0,
};

pub const Grant = struct {
    scheduler: *Scheduler,
    input_bytes: u64,
    run_ids: [max_tracked_work_run_ids]u64 = undefined,
    run_count: usize = 0,
    reservation: ?resource_manager_mod.Reservation = null,
    completed: bool = false,

    pub fn complete(self: *Grant) void {
        if (self.completed) return;
        self.completed = true;
        if (self.reservation) |*reservation| reservation.release();
        self.scheduler.complete(self.input_bytes, self.run_ids[0..self.run_count]);
    }
};

pub const Scheduler = struct {
    options: Options = .{},
    active_jobs: usize = 0,
    in_flight_input_bytes: u64 = 0,
    in_flight_run_ids: [max_in_flight_run_ids]u64 = undefined,
    in_flight_run_count: usize = 0,
    grants: u64 = 0,
    completions: u64 = 0,
    denied_capacity: u64 = 0,
    denied_resource_pressure: u64 = 0,
    oversized_grants: u64 = 0,
    remembered_candidates: u64 = 0,
    remembered_retries: u64 = 0,
    remembered_hits: u64 = 0,
    remembered_stale: u64 = 0,
    conflict_denials: u64 = 0,

    pub fn init(options: Options) Scheduler {
        return .{ .options = options };
    }

    pub fn tryAcquire(self: *Scheduler, work: Work, resource_manager: ?*resource_manager_mod.ResourceManager) ?Grant {
        if (work.score == 0 or work.input_runs == 0) {
            self.denied_capacity += 1;
            return null;
        }
        if (work.run_count > work.run_ids.len or self.conflictsWithInFlightRuns(work.run_ids[0..work.run_count])) {
            self.conflict_denials += 1;
            return null;
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
        self.grants += 1;
        if (oversized) self.oversized_grants += 1;
        var grant = Grant{
            .scheduler = self,
            .input_bytes = work.input_bytes,
            .reservation = reservation,
        };
        grant.run_count = work.run_count;
        if (work.run_count > 0) {
            @memcpy(grant.run_ids[0..work.run_count], work.run_ids[0..work.run_count]);
        }
        return grant;
    }

    fn complete(self: *Scheduler, input_bytes: u64, run_ids: []const u64) void {
        self.active_jobs -|= 1;
        self.in_flight_input_bytes -|= input_bytes;
        self.removeInFlightRuns(run_ids);
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

    fn addInFlightRuns(self: *Scheduler, run_ids: []const u64) void {
        if (run_ids.len == 0) return;
        @memcpy(self.in_flight_run_ids[self.in_flight_run_count .. self.in_flight_run_count + run_ids.len], run_ids);
        self.in_flight_run_count += run_ids.len;
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

    pub fn snapshot(self: *const Scheduler) Stats {
        return .{
            .active_jobs = @intCast(self.active_jobs),
            .in_flight_input_bytes = self.in_flight_input_bytes,
            .grants = self.grants,
            .completions = self.completions,
            .denied_capacity = self.denied_capacity,
            .denied_resource_pressure = self.denied_resource_pressure,
            .oversized_grants = self.oversized_grants,
            .remembered_candidates = self.remembered_candidates,
            .remembered_retries = self.remembered_retries,
            .remembered_hits = self.remembered_hits,
            .remembered_stale = self.remembered_stale,
            .conflict_denials = self.conflict_denials,
        };
    }
};

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
