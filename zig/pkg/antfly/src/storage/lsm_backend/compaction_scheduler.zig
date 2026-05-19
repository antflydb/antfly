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

const resource_manager_mod = @import("../resource_manager.zig");

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
    reservation: ?resource_manager_mod.Reservation = null,
    completed: bool = false,

    pub fn complete(self: *Grant) void {
        if (self.completed) return;
        self.completed = true;
        if (self.reservation) |*reservation| reservation.release();
        self.scheduler.complete(self.input_bytes);
    }
};

pub const Scheduler = struct {
    options: Options = .{},
    active_jobs: usize = 0,
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

    pub fn init(options: Options) Scheduler {
        return .{ .options = options };
    }

    pub fn tryAcquire(self: *Scheduler, work: Work, resource_manager: ?*resource_manager_mod.ResourceManager) ?Grant {
        if (work.score == 0 or work.input_runs == 0) {
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
        self.grants += 1;
        if (oversized) self.oversized_grants += 1;
        return .{
            .scheduler = self,
            .input_bytes = work.input_bytes,
            .reservation = reservation,
        };
    }

    fn complete(self: *Scheduler, input_bytes: u64) void {
        self.active_jobs -|= 1;
        self.in_flight_input_bytes -|= input_bytes;
        self.completions += 1;
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
