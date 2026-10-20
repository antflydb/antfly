// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Runnable permits are separate from operation admission and retained memory.
//! Entries belong to callers, remain stable until finish, and allocate nothing.
//! Blocking relinquishes execution slots without releasing operation ownership.
const std = @import("std");
const admission = @import("bounded_admission.zig");

/// A synchronous additional lifetime fence; callbacks never run under the
/// scheduler mutex and convey no fresh admission or deadline authority.
pub const AdmissionCheck = struct {
    context: *anyopaque,
    check: *const fn (*anyopaque) anyerror!void,
    pub fn checkpoint(self: @This()) !void {
        try self.check(self.context);
    }
};

pub const Class = enum(u3) { query, write, inference, background, control };
/// Trusted execution policy, independent of public operation admission classes.
/// Bounded eligibility requires a verified non-growing, non-fanout operator.
pub const ReadLane = enum { general, bounded, analytical };
const ordinary_queues = [_]usize{ 0, 1, 2, 3, 6, 7 };
/// Trusted accounting participants only. Callbacks run under the scheduler
/// mutex and may perform bounded, allocation-free accounting with scheduler ->
/// resource lock order. They must never perform IO or reenter the scheduler.
/// C booleans keep compilation-local error identities out of this boundary.
pub const ReadResourceTransfer = struct {
    context: *anyopaque,
    begin: *const fn (*anyopaque) callconv(.c) bool,
    try_general: *const fn (*anyopaque) callconv(.c) bool,
};
/// Trusted resource owners only. Both callbacks run under the scheduler lock,
/// use bounded try-lock accounting, and never allocate, wait, perform IO, call
/// reclaimers, or reenter this scheduler. Probe holds no capacity. Unknown
/// means acquisition may succeed but must not establish a CPU drain barrier.
/// Acquisition commits the complete incremental non-CPU bundle or nothing.
/// The caller retains acquired physical resources through their actual joins;
/// CPU park/finish does not release them. Metadata is prepared before submit.
pub const GrantResources = struct {
    pub const Probe = enum(c_int) { ready, wait, unknown, too_large, unsupported, mismatch };
    pub const Acquire = enum(c_int) { acquired, wait, too_large, unsupported, mismatch };
    context: *anyopaque,
    probe: *const fn (*anyopaque) callconv(.c) c_int,
    acquire: *const fn (*anyopaque) callconv(.c) c_int,
};
pub const ReadTransitionBudget = struct {
    /// Verified retained continuation plus its minimum cleanup metadata.
    max_retained_bytes: u64,
    max_wait_ms: u32,
    estimated_work_ns: u64,
};
/// Trusted prepared operator stage: one worker, fixed resources, no blocking,
/// fanout or mutation, with a verified bound between safe checkpoints.
/// CPU loans lend no heap, registry, cleanup or durable authority.
pub const CpuBorrowBudget = struct { max_non_yield_ns: u64 };
/// Hard dispatch scan bound, independent of queue length and bypass policy.
/// Each class has independent fresh-start and continuation windows, each
/// including its head plus at most this many younger entries.
pub const max_backfill_lookahead = 8;
const BackfillCandidates = struct {
    current: ?*Entry,
    remaining: usize = max_backfill_lookahead + 1,
    fn peek(self: *const @This()) ?*Entry {
        return if (self.remaining == 0) null else self.current;
    }
    fn take(self: *@This()) ?*Entry {
        const entry = self.peek() orelse return null;
        self.current = entry.next;
        self.remaining -= 1;
        return entry;
    }
};
/// Merge independently bounded windows in enqueue order. Fresh starts cannot
/// consume the continuation scan budget; both still share class service credit.
const ClassCandidates = struct {
    fresh: BackfillCandidates,
    resumed: BackfillCandidates,

    fn take(self: *@This()) ?*Entry {
        const fresh = self.fresh.peek() orelse return self.resumed.take();
        const resumed = self.resumed.peek() orelse return self.fresh.take();
        return if (fresh.sequence <= resumed.sequence) self.fresh.take() else self.resumed.take();
    }
};
pub const State = enum { fresh, queued, running, resource_blocked, output_blocked, stopped, finished };
/// Named because Config crosses separately compiled native ABI boundaries.
/// Anonymous enum type names include compilation-local declaration numbers.
pub const CapacityMode = enum(u8) { fixed, automatic };
pub const Config = struct {
    /// Automatic mode treats slots, max_bytes and control_bytes as ceilings.
    /// Fixed policies preserve their exact declared control reservation.
    capacity_mode: CapacityMode = .fixed,
    slots: usize = 8,
    control_slots: usize = 1,
    /// Dedicated cleanup CPU drawn from slots, separate from health controls.
    /// Zero retains the legacy shared-control policy until callers opt in.
    cleanup_slots: usize = 0,
    /// Reserve bounded_read_slots ordinary CPUs and one quarter of ordinary
    /// registry count/bytes for verified bounded reads.
    read_isolation: ?bool = null,
    /// Total bounded CPU reserve inside slots. One slot is a nonborrowable
    /// floor; only excess capacity may lend one CPU to a verified stage.
    bounded_read_slots: usize = 1,
    weights: [4]u16 = .{ 4, 2, 1, 1 },
    max_operations: usize = 128,
    max_bytes: u64 = 8 << 20,
    control_bytes: u64 = 64 << 10,
    max_wait_ms: u32 = 250,
    max_backfills: u16 = 8,
    quantum_ms: u32 = 2,
    checkpoint_budget: u32 = 64,

    /// Resolve once before publication. Protected lanes remain inside the
    /// declared slot ceiling, with a minimum ordinary lane even below that CPU
    /// count. Slots bound runnable ownership, not guaranteed dedicated cores.
    /// Metadata receives at most 1/32 of the visible memory envelope, subject to
    /// the configured ceiling. This is not a cap on storage/model/process heap.
    pub fn resolveCapacity(self: Config, cpu: @import("antfly_platform").process_memory.CpuCapacity, memory: @import("antfly_platform").process_memory.Envelope) !Config {
        try self.validate();
        if (self.capacity_mode == .fixed) return self;
        var resolved = self;
        resolved.capacity_mode = .fixed;
        resolved.cleanup_slots = 1;
        const minimum = self.control_slots + 2;
        if (minimum > self.slots) return error.InvalidRunnablePolicy;
        resolved.slots = @max(minimum, @min(self.slots, cpu.parallelism()));
        resolved.read_isolation = self.read_isolation orelse (resolved.ordinarySlots() > self.bounded_read_slots and self.max_operations >= 8);
        if (memory.limit_bytes != 0) resolved.max_bytes = @min(self.max_bytes, memory.limit_bytes / 32);
        // Large nodes can hold the complete protocol baseline without taking
        // foreground headroom. Smaller envelopes retain half for ordinary work;
        // never shrink below the generic protected-control minimum (or a
        // stricter caller-selected smaller ceiling).
        resolved.control_bytes = @min(self.control_bytes, resolved.max_bytes / 2);
        if (resolved.control_bytes < @min(self.control_bytes, @as(u64, 64 << 10))) return error.InvalidRunnablePolicy;
        try resolved.validateManagedCleanupCapacity();
        return resolved;
    }

    /// Honor a stricter configured process budget before publishing one
    /// effective policy to API/storage consumers. A fixed policy never probes.
    pub fn resolveForStartup(self: Config, process_limit_bytes: u64) !Config {
        if (self.capacity_mode == .fixed) {
            try self.validate();
            return self;
        }
        return self.resolveCapacity(@import("antfly_platform").process_memory.cpuCapacity(), startupMemoryEnvelope(process_limit_bytes));
    }

    pub fn startupMemoryEnvelope(process_limit_bytes: u64) @import("antfly_platform").process_memory.Envelope {
        var memory = @import("antfly_platform").process_memory.systemEnvelope();
        if (process_limit_bytes != 0) memory.limit_bytes = if (memory.limit_bytes == 0)
            process_limit_bytes
        else
            @min(memory.limit_bytes, process_limit_bytes);
        return memory;
    }

    pub fn ordinarySlots(self: Config) usize {
        return self.slots - self.control_slots - self.cleanup_slots;
    }

    pub fn readIsolation(self: Config) bool {
        return self.read_isolation orelse false;
    }

    pub fn generalSlots(self: Config) usize {
        return self.ordinarySlots() - self.boundedReadSlots();
    }

    pub fn boundedReadSlots(self: Config) usize {
        return if (self.readIsolation()) self.bounded_read_slots else 0;
    }

    pub fn boundedReadFloorSlots(self: Config) usize {
        return @intFromBool(self.readIsolation());
    }

    pub fn cpuBorrowSlots(self: Config) usize {
        // Keep at most one borrower until broader lending is qualified.
        return @min(@as(usize, 1), self.boundedReadSlots() - self.boundedReadFloorSlots());
    }

    pub fn boundedReadMetadataBytes(self: Config) u64 {
        return if (self.readIsolation()) (self.max_bytes - self.control_bytes) / 4 else 0;
    }

    pub fn generalMetadataBytes(self: Config) u64 {
        return self.max_bytes - self.control_bytes - self.boundedReadMetadataBytes();
    }

    /// Managed startup needs independent ordinary, health and cleanup capacity.
    /// Keep generic scheduler validation separate for standalone consumers.
    pub noinline fn validateManagedCleanupCapacity(self: Config) !void {
        try self.validate();
        if (self.max_operations < 4 or (self.cleanup_slots == 0 and self.ordinarySlots() < (if (self.readIsolation()) self.bounded_read_slots + 2 else 2)))
            return error.ManagerCleanupCapacityUnavailable;
    }

    /// Protect sparse arrivals as well as a rapid bypass stream. Derive an
    /// internal bound from the existing wait policy, without another clock or
    /// admission deadline: at most 25 ms and at most one quarter of queue wait.
    pub fn backfillAgeNs(self: Config) u64 {
        return @min(25 * std.time.ns_per_ms, @as(u64, self.max_wait_ms) * std.time.ns_per_ms / 4);
    }

    pub fn validate(self: Config) !void {
        if (self.slots < 2 or self.slots > 65535 or self.control_slots == 0 or self.control_slots >= self.slots or
            self.cleanup_slots > 1 or self.cleanup_slots >= self.slots - self.control_slots or
            self.bounded_read_slots == 0 or self.bounded_read_slots > 65533 or
            (self.readIsolation() and (self.ordinarySlots() <= self.bounded_read_slots or self.max_operations < 8 or self.max_bytes -| self.control_bytes < 4)) or
            self.max_operations < 2 or self.max_bytes == 0 or self.control_bytes == 0 or self.control_bytes >= self.max_bytes or self.max_wait_ms == 0 or self.max_wait_ms > 60000 or
            self.quantum_ms == 0 or self.quantum_ms > 100 or self.checkpoint_budget == 0 or self.checkpoint_budget > 1048576)
            return error.InvalidRunnablePolicy;
        for (self.weights) |weight| if (weight == 0) return error.InvalidRunnablePolicy;
    }
};

pub const Entry = struct {
    options: admission.Options,
    /// Parent and all of its original clock/cancellation owners remain stable
    /// until this entry's owning finish, including stopped and blocked work.
    parent: ?*Entry = null,
    children: usize = 0,
    class: Class,
    read_lane: ReadLane = .general,
    read_transition_budget: ?ReadTransitionBudget = null,
    read_resource_transfer: ?ReadResourceTransfer = null,
    read_transition_active: bool = false,
    read_transition_target: ReadLane = .general,
    read_transition_started_ns: u64 = 0,
    measured_read_work_ns: u64 = 0,
    service_debt: usize = 0,
    bounded_service_paid: u64 = 0,
    /// One acknowledged CPU lease, sampled only by its stable owning caller.
    service_started_ns: ?u64 = null,
    service_observed_ns: u64 = 0,
    service_estimate_ns: u64 = 0,
    service_class: ?usize = null,
    service_clock_valid: bool = true,
    slots: usize = 1,
    cpu_borrow_budget: ?CpuBorrowBudget = null,
    borrowed_read_cpu: bool = false,
    bytes: u64 = 0,
    state: State = .fresh,
    ready: std.Io.Event = .unset,
    failure: ?anyerror = null,
    scheduler: ?*Scheduler = null,
    next: ?*Entry = null,
    previous: ?*Entry = null,
    all_next: ?*Entry = null,
    all_previous: ?*Entry = null,
    sequence: u64 = 0,
    /// Running, parked, and cleanup owners use continuation queues on reentry.
    continuation: bool = false,
    pending_resources: ?GrantResources = null,
    resource_probe_epoch: u64 = 0,
    resource_probe: GrantResources.Probe = .unknown,

    bypasses: u16 = 0,
    age_protected: bool = false,
    queued_since_ns: u64 = 0,
    waited: bool = false,
    reserved_control: bool = false,
    /// Preauthorized before possible durable acceptance. These callbacks and
    /// their independent completion Io must remain valid through all joins.
    completion_options: ?admission.Options = null,
    completion_active: bool = false,
    /// Trusted physical cleanup reserved before publishing an owned resource.
    /// This grants no durable-write authority and never changes CPU partition.
    cleanup_options: ?admission.Options = null,
    /// One-shot physical cleanup duration, consumed at first activation.
    cleanup_duration_ns: ?u64 = null,
    cleanup_active: bool = false,
    protected_cleanup_root: bool = false,

    fn retainedPhase(self: *const Entry) bool {
        return self.completion_active or self.cleanup_active or self.protected_cleanup_root;
    }
};

const Queue = struct { head: ?*Entry = null, tail: ?*Entry = null };
pub const Scheduler = struct {
    config: Config,
    mutex: std.atomic.Mutex = .unlocked,
    queues: [8]Queue = @splat(.{}),
    continuation_queues: [8]Queue = @splat(.{}),
    entries: ?*Entry = null,
    used: [8]usize = @splat(0),
    credits: [6]usize = @splat(0),
    cursor: usize = 0,
    round_open: bool = false,
    progress_cursor: usize = 0,
    progress_round_open: bool = false,
    /// Signed slot-time correction retained across roots, parks and class
    /// changes. Positive remainder is rebate; negative remainder is owed work.
    work_adjustment_ns: [6]i128 = @splat(0),
    work_service: ServiceStats = .{},
    sequence: u64 = 0,
    closed: bool = false,
    dispatch_epoch: u64 = 0,
    resource_barrier: ?*Entry = null,

    metrics: Stats = .{},
    control_baseline_bytes: u64 = 0,
    cpu_borrower: ?*Entry = null,
    cpu_borrow_grants: u64 = 0,
    cpu_borrow_returns: u64 = 0,

    pub const ServiceStats = struct {
        estimated_slot_ns: [6]u64 = @splat(0),
        observed_slot_ns: [6]u64 = @splat(0),
        unobserved_estimate_ns: [6]u64 = @splat(0),
        refunded_slot_ns: [6]u64 = @splat(0),
        debt_paid_slot_ns: [6]u64 = @splat(0),
        clipped_adjustments: [6]u64 = @splat(0),
        adjustment_slot_ns: [6]i128 = @splat(0),
    };
    pub fn serviceStats(self: *Scheduler) ServiceStats {
        self.lock();
        defer self.mutex.unlock();
        var result = self.work_service;
        result.adjustment_slot_ns = self.work_adjustment_ns;
        return result;
    }
    fn serviceQuantumNs(self: *const Scheduler) u64 {
        return @as(u64, self.config.quantum_ms) * std.time.ns_per_ms;
    }
    /// A successfully returned wait acknowledges real execution. Queue time,
    /// wake latency and blocked time have no active sample. Host clocks stay
    /// outside accounting locks; a zero/backwards observation uses the estimate.
    pub fn beginService(self: *Scheduler, e: *Entry) !void {
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running) return e.failure orelse error.InvalidRunnableTransition;
        if (e.service_class != null and e.service_started_ns == null) e.service_started_ns = now;
    }
    fn sampleServiceLocked(self: *Scheduler, e: *Entry, now: u64) void {
        _ = self;
        const start = e.service_started_ns orelse return;
        if (now < start) e.service_clock_valid = false;
        e.service_observed_ns +|= (now -| start) *| @as(u64, @intCast(e.slots));
        e.service_started_ns = now;
    }
    fn settleServiceLocked(self: *Scheduler, e: *Entry, now: u64) void {
        const c = e.service_class orelse return;
        self.sampleServiceLocked(e, now);
        const estimate = e.service_estimate_ns;
        const observed = e.service_observed_ns;
        if (e.service_started_ns == null or !e.service_clock_valid or observed == 0) {
            self.work_service.unobserved_estimate_ns[c] +|= estimate;
        } else {
            self.work_service.observed_slot_ns[c] +|= observed;
            const correction = @as(i128, estimate) - @as(i128, observed);
            const limit = @as(i128, 64) * self.serviceQuantumNs();
            const next = self.work_adjustment_ns[c] + correction;
            self.work_adjustment_ns[c] = std.math.clamp(next, -limit, limit);
            if (next != self.work_adjustment_ns[c]) self.work_service.clipped_adjustments[c] +|= 1;
            // Return unused estimated work in this same open deficit round.
            // Fractional slot-time remains exact across many short requests.
            if (self.work_adjustment_ns[c] >= self.serviceQuantumNs()) {
                const units: usize = @intCast(@divTrunc(self.work_adjustment_ns[c], self.serviceQuantumNs()));
                const room = self.config.slots + self.weight(c) -| self.credits[c];
                const refund = @min(units, room);
                self.credits[c] += refund;
                self.work_service.refunded_slot_ns[c] +|= @as(u64, @intCast(refund)) * self.serviceQuantumNs();
                self.work_adjustment_ns[c] -= @as(i128, @intCast(units)) * self.serviceQuantumNs();
                if (refund != units) self.work_service.clipped_adjustments[c] +|= 1;
            }
        }
        e.service_started_ns = null;
        e.service_class = null;
        e.service_estimate_ns = 0;
        e.service_observed_ns = 0;
    }
    /// Completion/cleanup already owns mandatory progress. Preserve its prior
    /// scheduling behavior while any such receiver is in this bounded window.
    fn classHasRetainedProgress(self: *Scheduler, c: usize) bool {
        var candidates = self.classCandidates(ordinary_queues[c]);
        while (candidates.take()) |e| if (e.retainedPhase()) return true;
        return false;
    }
    fn payWorkDebt(self: *Scheduler, c: usize, floor: usize) void {
        if (self.work_adjustment_ns[c] >= 0 or self.classHasRetainedProgress(c)) return;
        const quantum = self.serviceQuantumNs();
        const owed: u64 = @intCast(-self.work_adjustment_ns[c]);
        const units = owed / quantum + @intFromBool(owed % quantum != 0);
        const paid = @min(units, self.credits[c] -| floor);
        self.credits[c] -= @intCast(paid);
        self.work_adjustment_ns[c] += @as(i128, paid) * quantum;
        self.work_service.debt_paid_slot_ns[c] +|= @min(owed, paid * quantum);
    }

    pub const Stats = struct {
        cpu_borrowed_slots: usize = 0,
        cpu_borrow_grants: u64 = 0,
        cpu_borrow_returns: u64 = 0,
        read_transition_reserved_operations: usize = 0,
        read_transition_reserved_bytes: u64 = 0,
        read_transition_operations: usize = 0,
        read_transition_bytes: u64 = 0,
        read_demotions: u64 = 0,
        read_transition_expired: u64 = 0,
        read_service_debt_paid: u64 = 0,
        bounded_read_operations: usize = 0,
        bounded_read_bytes: u64 = 0,
        bounded_read_slots: usize = 0,
        read_lane_grants: [3]u64 = @splat(0),
        operations: usize = 0,
        child_operations: usize = 0,
        retained_bytes: u64 = 0,
        control_operations: usize = 0,
        control_retained_bytes: u64 = 0,
        cleanup_operations: usize = 0,
        cleanup_retained_bytes: u64 = 0,
        queued: usize = 0,
        runnable_slots: usize = 0,
        control_slots: usize = 0,
        cleanup_runnable_slots: usize = 0,
        resource_blocked: usize = 0,
        output_blocked: usize = 0,
        grants: [5]u64 = @splat(0),
        yields: u64 = 0,
        backfills: u64 = 0,
        rejected: u64 = 0,
        wait_ns: u64 = 0,
    };

    pub fn init(config: Config) !Scheduler {
        try config.validate();
        const resolved = if (config.capacity_mode == .automatic)
            try config.resolveForStartup(0)
        else
            config;
        return .{ .config = resolved };
    }

    /// Move-only persistent node metadata reservation. It owns bytes through
    /// physical destruction, including idle periods, but no operation or CPU.
    pub const ControlBaseline = struct {
        scheduler: ?*Scheduler,
        bytes: u64,
        pub fn release(self: *ControlBaseline) void {
            const scheduler = self.scheduler orelse return;
            self.scheduler = null;
            scheduler.lock();
            defer scheduler.mutex.unlock();
            std.debug.assert(scheduler.control_baseline_bytes >= self.bytes);
            scheduler.control_baseline_bytes -= self.bytes;
            scheduler.metrics.control_retained_bytes -= self.bytes;
            scheduler.metrics.retained_bytes -= self.bytes;
            scheduler.dispatch();
        }
    };
    pub fn reserveControlBaseline(self: *Scheduler, bytes: u64) !ControlBaseline {
        if (bytes == 0) return error.InvalidArgument;
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.SchedulerClosed;
        // Fixed node consumers collectively use at most one quarter. Existing
        // protected cleanup may use half, leaving a quarter for health/control.
        // Include each consumer's maximum scratch in its persistent reservation.
        if (bytes > self.config.control_bytes / 4 -| self.control_baseline_bytes or
            bytes > self.config.control_bytes -| self.metrics.control_retained_bytes) return error.SchedulerFull;
        self.control_baseline_bytes += bytes;
        self.metrics.control_retained_bytes += bytes;
        self.metrics.retained_bytes += bytes;
        return .{ .scheduler = self, .bytes = bytes };
    }

    /// Establish the managed server's dedicated cleanup reserve before any
    /// runnable entry is published. Never resize an active execution policy.
    pub fn ensureCleanupCpuBeforePublication(self: *Scheduler) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.SchedulerClosed;
        try self.config.validateManagedCleanupCapacity();
        if (self.config.cleanup_slots != 0) return;
        if (self.entries != null)
            return error.ManagerCleanupCapacityUnavailable;
        self.config.cleanup_slots = 1;
    }

    fn lock(self: *Scheduler) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn boundedRead(self: *const Scheduler, e: *const Entry) bool {
        return self.config.readIsolation() and e.class == .query and e.read_lane == .bounded and !e.read_transition_active;
    }
    fn cpuIndex(self: *const Scheduler, e: *const Entry) usize {
        if ((e.protected_cleanup_root or e.cleanup_active) and self.config.cleanup_slots != 0) return 5;
        if (self.config.readIsolation() and e.class == .query) return switch (e.read_lane) {
            .general => 0,
            .bounded => 6,
            .analytical => 7,
        };
        return @backingInt(e.class);
    }
    fn capacityFor(self: *const Scheduler, e: *const Entry, class: Class) usize {
        if ((e.protected_cleanup_root or e.cleanup_active) and self.config.cleanup_slots != 0) return self.config.cleanup_slots;
        if (class == .control) return self.config.control_slots;
        if (!self.config.readIsolation()) return self.config.ordinarySlots();
        return if (class == .query and e.read_lane == .bounded) self.config.boundedReadSlots() else self.config.generalSlots();
    }
    fn generalUsed(self: *const Scheduler) usize {
        return self.ordinaryUsed() - self.used[6] - @intFromBool(self.cpu_borrower != null);
    }
    fn canBorrowCpu(self: *const Scheduler, e: *const Entry) bool {
        return self.config.cpuBorrowSlots() != 0 and e.cpu_borrow_budget != null and
            !self.boundedRead(e) and e.class != .control and !e.retainedPhase() and
            e.slots == 1 and e.children == 0 and e.parent == null and
            self.cpu_borrower == null and self.used[6] < self.config.boundedReadSlots() and self.queueHead(6) == null;
    }
    fn fitsCpu(self: *const Scheduler, e: *const Entry, free: usize) bool {
        if (e.slots > free) return false;
        if (!self.config.readIsolation()) return true;
        const lane_used = if (self.boundedRead(e)) self.used[6] else self.generalUsed();
        return e.slots <= self.capacityFor(e, e.class) -| lane_used or self.canBorrowCpu(e);
    }
    fn validateCpuBorrow(self: *const Scheduler, e: *const Entry, budget: CpuBorrowBudget) !void {
        if (budget.max_non_yield_ns == 0 or budget.max_non_yield_ns > @as(u64, self.config.quantum_ms) * std.time.ns_per_ms or
            e.slots != 1 or e.parent != null or e.children != 0 or e.class == .control or
            e.read_lane == .bounded or e.read_transition_active or e.retainedPhase() or e.completion_options != null)
            return error.InvalidRunnableTransition;
    }
    pub fn enableCpuBorrow(self: *Scheduler, e: *Entry, budget: CpuBorrowBudget) !void {
        try self.checkAuthority(e);
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running) return error.InvalidRunnableTransition;
        try self.validateCpuBorrow(e, budget);
        e.cpu_borrow_budget = budget;
        // A validated stage already owns this live grant. Move it onto elastic
        // capacity before waking queued ordinary work, without a suspend/resume
        // or an authority/quantum refresh. Never lend the bounded hard floor.
        if (self.canBorrowCpu(e) and self.generalUsed() == self.config.generalSlots()) {
            const waiting = for (ordinary_queues) |index| {
                if (index != 6 and self.queueHead(index) != null) break true;
            } else false;
            if (waiting) {
                self.startCpuLoan(e);
                self.dispatchWithLoan(true);
            }
        }
    }
    /// Disable before an unqualified phase; waitRetained must precede new work.
    pub fn disableCpuBorrow(self: *Scheduler, e: *Entry) !void {
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running) return error.InvalidRunnableTransition;
        e.cpu_borrow_budget = null;
        if (!e.borrowed_read_cpu) return;
        self.releaseCpu(e, now);
        e.bypasses = 0;
        self.sequence +|= 1;
        e.sequence = self.sequence;
        self.enqueue(e, now);
        self.dispatch();
        e.waited = e.state == .queued;
    }
    pub const CpuBorrowStats = struct { active: usize, grants: u64, returns: u64 };
    pub fn cpuBorrowStats(self: *Scheduler) CpuBorrowStats {
        self.lock();
        defer self.mutex.unlock();
        return .{ .active = @intFromBool(self.cpu_borrower != null), .grants = self.cpu_borrow_grants, .returns = self.cpu_borrow_returns };
    }
    fn borrowingCpu(self: *Scheduler, e: *const Entry) bool {
        self.lock();
        defer self.mutex.unlock();
        return e.scheduler == self and e.borrowed_read_cpu;
    }
    fn returnBorrowRequested(self: *Scheduler, e: *const Entry) bool {
        self.lock();
        defer self.mutex.unlock();
        return e.scheduler == self and e.borrowed_read_cpu and self.queueHead(6) != null;
    }
    fn returnCpuLoan(self: *Scheduler, e: *Entry) void {
        if (!e.borrowed_read_cpu) return;
        std.debug.assert(self.cpu_borrower == e);
        self.cpu_borrower = null;
        e.borrowed_read_cpu = false;
        self.cpu_borrow_returns +|= 1;
    }
    fn releaseCpu(self: *Scheduler, e: *Entry, now: u64) void {
        self.settleServiceLocked(e, now);
        self.used[self.cpuIndex(e)] -= e.slots;
        self.returnCpuLoan(e);
    }
    fn weight(self: *const Scheduler, index: usize) usize {
        const q = ordinary_queues[index];
        if (!self.config.readIsolation()) return self.config.weights[@min(q, 3)];
        const query_shares: usize = @intFromBool(self.queueHead(0) != null) +
            @as(usize, 2) * @intFromBool(self.queueHead(6) != null) +
            @intFromBool(self.queueHead(7) != null);
        // Scale every ordinary class by the same denominator, preserving the
        // configured query share when one or more read lanes are idle.
        if (q >= 1 and q <= 3) return @as(usize, self.config.weights[q]) * @max(@as(usize, 1), query_shares);
        return @as(usize, self.config.weights[0]) * (if (q == 6) @as(usize, 2) else 1);
    }
    fn boundedCountLimit(self: *const Scheduler) usize {
        return (self.config.max_operations - @min(@as(usize, 4), self.config.max_operations / 2)) / 4;
    }
    fn boundedByteLimit(self: *const Scheduler) u64 {
        return self.config.boundedReadMetadataBytes();
    }

    fn queueFor(self: *Scheduler, e: *const Entry) *Queue {
        const index = self.cpuIndex(e);
        return if (e.continuation) &self.continuation_queues[index] else &self.queues[index];
    }
    fn queueHead(self: *const Scheduler, index: usize) ?*Entry {
        const fresh = self.queues[index].head orelse return self.continuation_queues[index].head;
        const resumed = self.continuation_queues[index].head orelse return fresh;
        return if (fresh.sequence <= resumed.sequence) fresh else resumed;
    }
    fn classCandidates(self: *const Scheduler, index: usize) ClassCandidates {
        return .{
            .fresh = .{ .current = self.queues[index].head },
            .resumed = .{ .current = self.continuation_queues[index].head },
        };
    }
    fn enqueue(self: *Scheduler, e: *Entry, now: u64) void {
        e.queued_since_ns = now;
        e.age_protected = false;
        e.waited = false;
        const q = self.queueFor(e);
        e.previous = q.tail;
        e.next = null;
        if (q.tail) |tail| tail.next = e else q.head = e;
        q.tail = e;
        e.state = .queued;
        e.ready.reset();
        self.metrics.queued += 1;
    }
    fn unlink(self: *Scheduler, e: *Entry) void {
        if (self.resource_barrier == e) self.resource_barrier = null;
        const q = self.queueFor(e);
        if (e.previous) |p| p.next = e.next else q.head = e.next;
        if (e.next) |n| n.previous = e.previous else q.tail = e.previous;
        e.next = null;
        e.previous = null;
        self.metrics.queued -= 1;
    }
    fn ordinaryUsed(self: *const Scheduler) usize {
        return self.used[0] + self.used[1] + self.used[2] + self.used[3] + self.used[6] + self.used[7];
    }
    fn startCpuLoan(self: *Scheduler, e: *Entry) void {
        std.debug.assert(self.canBorrowCpu(e));
        self.cpu_borrower = e;
        e.borrowed_read_cpu = true;
        self.cpu_borrow_grants +|= 1;
    }
    fn convertCpuLoanToGeneral(self: *Scheduler) void {
        if (self.cpu_borrower) |e| {
            if (self.ordinaryUsed() - self.used[6] <= self.config.generalSlots()) self.returnCpuLoan(e);
        }
    }
    fn grant(self: *Scheduler, e: *Entry) void {
        std.debug.assert(e.pending_resources == null);
        self.unlink(e);
        e.continuation = true;
        e.state = .running;
        const c = @backingInt(e.class);
        if (self.config.readIsolation() and !self.boundedRead(e) and e.class != .control and !e.retainedPhase() and
            e.slots > self.config.generalSlots() -| self.generalUsed())
        {
            self.startCpuLoan(e);
        }
        self.used[self.cpuIndex(e)] += e.slots;
        self.metrics.grants[c] +|= 1;
        e.service_started_ns = null;
        e.service_observed_ns = 0;
        e.service_clock_valid = true;
        e.service_class = if (e.retainedPhase() or (self.boundedRead(e) and e.read_transition_budget != null)) null else self.barrierClass(e);
        e.service_estimate_ns = @as(u64, @intCast(e.slots)) * self.serviceQuantumNs();
        if (e.service_class) |index| self.work_service.estimated_slot_ns[index] +|= e.service_estimate_ns;
        if (self.boundedRead(e)) e.bounded_service_paid +|= e.slots;
        if (e.class == .query) self.metrics.read_lane_grants[@backingInt(e.read_lane)] +|= 1;
        e.ready.set(e.options.io);
    }

    /// A paid general team keeps its deficit round open while its peers drain.
    /// The isolated one-slot bounded partition can still make independent
    /// progress; charge its ordinary deficit without moving that open cursor.
    fn stopResourceGrant(self: *Scheduler, e: *Entry, cause: anyerror) void {
        self.unlink(e);
        e.state = .stopped;
        e.failure = cause;
        self.metrics.rejected +|= 1;
        e.ready.set(e.options.io);
    }
    fn resourceProbe(self: *Scheduler, e: *Entry) GrantResources.Probe {
        const resources = e.pending_resources orelse return .ready;
        if (e.resource_probe_epoch != self.dispatch_epoch) {
            e.resource_probe_epoch = self.dispatch_epoch;
            e.resource_probe = std.enums.fromInt(GrantResources.Probe, resources.probe(resources.context)) orelse .mismatch;
            switch (e.resource_probe) {
                .too_large => self.stopResourceGrant(e, error.ResourceRequestTooLarge),
                .unsupported => self.stopResourceGrant(e, error.ExecutionWaitUnsupported),
                .mismatch => self.stopResourceGrant(e, error.ResourceAccountingMismatch),
                else => {},
            }
        }
        return e.resource_probe;
    }
    fn acquireGrantResources(self: *Scheduler, e: *Entry) bool {
        const resources = e.pending_resources orelse return true;
        const probe = self.resourceProbe(e);
        if (probe != .ready and probe != .unknown) return false;
        switch (std.enums.fromInt(GrantResources.Acquire, resources.acquire(resources.context)) orelse .mismatch) {
            .acquired => {
                e.pending_resources = null;
                return true;
            },
            .wait => e.resource_probe = .wait,
            .too_large => self.stopResourceGrant(e, error.ResourceRequestTooLarge),
            .unsupported => self.stopResourceGrant(e, error.ExecutionWaitUnsupported),
            .mismatch => self.stopResourceGrant(e, error.ResourceAccountingMismatch),
        }
        return false;
    }
    fn barrierClass(self: *const Scheduler, e: *const Entry) ?usize {
        for (ordinary_queues, 0..) |index, c| if (self.cpuIndex(e) == index) return c;
        return null;
    }
    fn rememberResourceBarrier(self: *Scheduler, e: *Entry) void {
        if (e.state != .queued) return;
        if (self.resource_barrier == null or e.sequence < self.resource_barrier.?.sequence)
            self.resource_barrier = e;
    }
    fn earliestBarrier(self: *const Scheduler, prior: ?*Entry) ?*Entry {
        const older = if (prior) |e| (if (e.state == .queued) e else null) else null;
        const resource = self.resource_barrier orelse return older;
        if (older == null or resource.sequence < older.?.sequence) return resource;
        return older;
    }
    fn barrierAllows(self: *Scheduler, barrier: ?*Entry, e: *Entry) bool {
        const older = barrier orelse return true;
        if (older == e or (self.config.readIsolation() and self.boundedRead(older) != self.boundedRead(e))) return true;
        // Already admitted owners retain progress while new starts drain.
        // This also applies to a CPU-only barrier: a parked owner may hold
        // resources needed by work occupying the remaining CPUs.
        return e.continuation;
    }
    /// Attach only an incremental bundle prepared while this owner is parked.
    /// Original operation, deadline, cancellation and metadata remain retained.
    pub fn requestResources(self: *Scheduler, e: *Entry, resources: GrantResources) !void {
        try self.checkAuthority(e);
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.class == .control or e.retainedPhase() or e.pending_resources != null or
            (e.state != .resource_blocked and e.state != .output_blocked)) return error.InvalidRunnableTransition;
        e.pending_resources = resources;
        e.resource_probe_epoch = 0;
    }
    /// Only the synchronous waiter may detach its own participant after enter
    /// has returned. A refused retained root can outlive the temporary stage.
    fn detachResources(self: *Scheduler, e: *Entry, resources: GrantResources) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(e.state != .queued);
        if (e.pending_resources) |pending| {
            std.debug.assert(pending.context == resources.context and pending.probe == resources.probe and pending.acquire == resources.acquire);
            e.pending_resources = null;
            e.resource_probe_epoch = 0;
        }
    }

    /// The stable owning waiter observes its original clock outside the
    /// scheduler lock. Host clock/cancellation callbacks may use other owners.
    /// Dispatch consumes only this cached age flag; enqueue resets it once.
    pub fn pollWait(self: *Scheduler, e: *Entry) !void {
        self.checkAuthority(e) catch |err| {
            self.failWait(e, err, true);
            return err;
        };
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .queued) return;
        e.age_protected = e.age_protected or now -| e.queued_since_ns >= self.config.backfillAgeNs();
        self.dispatch();
    }

    /// Notify after physical resource retirement and after releasing every
    /// resource-owner lock. Waiters also retry on their original bounded clock.
    pub fn notifyResourceProgress(self: *Scheduler) void {
        self.lock();
        defer self.mutex.unlock();
        self.dispatch();
    }

    fn drainCandidate(self: *Scheduler, free: usize, barrier: *Entry, head: ?*Entry, e: *Entry) bool {
        if (e == barrier or !self.fitsCpu(e, free)) return false;
        if (!e.continuation and (!self.config.readIsolation() or self.boundedRead(e) == self.boundedRead(barrier) or e != head)) return false;
        const probe = self.resourceProbe(e);
        return e.state == .queued and (probe == .ready or probe == .unknown);
    }
    fn hasDrainCandidate(self: *Scheduler, free: usize, barrier: *Entry) bool {
        for (ordinary_queues) |index| {
            const head = self.queueHead(index);
            var candidates = self.classCandidates(index);
            while (candidates.take()) |e| if (self.drainCandidate(free, barrier, head, e)) return true;
        }
        return false;
    }

    /// Preserve the paid team's DRR cursor while existing owners retire. This
    /// pass shares the same bounded credit/debt ledger, never admits new work
    /// into the draining partition, and never spends the team's reserved credit.
    fn chooseDrainProgress(self: *Scheduler, free: usize, barrier: *Entry) ?*Entry {
        const reserved_class = self.barrierClass(barrier);
        const rounds = (self.config.slots + 65) * ordinary_queues.len;
        for (0..rounds) |round| {
            // Stop promptly when no retained owner can progress. Probe results
            // are cached in this dispatch, including a failed acquire attempt.
            if (round % ordinary_queues.len == 0 and !self.hasDrainCandidate(free, barrier)) return null;
            const c = self.progress_cursor;
            const head = self.queueHead(ordinary_queues[c]);
            const floor: usize = if (reserved_class == c) barrier.slots else 0;
            var candidates = self.classCandidates(ordinary_queues[c]);
            while (candidates.take()) |e| {
                if (!self.drainCandidate(free, barrier, head, e)) continue;
                if (!self.progress_round_open) {
                    self.credits[c] = @min(self.config.slots + self.weight(c), self.credits[c] +| self.weight(c));
                    self.progress_round_open = true;
                }
                self.payWorkDebt(c, floor);
                const available = self.credits[c] -| floor;
                const paid = @min(e.service_debt, available);
                e.service_debt -= paid;
                self.credits[c] -= paid;
                self.metrics.read_service_debt_paid +|= paid;
                if (e.service_debt == 0 and e.slots <= self.credits[c] -| floor) {
                    if (!self.acquireGrantResources(e)) break;
                    self.credits[c] -= e.slots;
                    return e;
                }
                break;
            }
            self.progress_cursor = (c + 1) % ordinary_queues.len;
            self.progress_round_open = false;
        }
        return null;
    }

    fn choose(self: *Scheduler, free: usize) ?*Entry {
        // Count and original-clock age independently protect a blocked head.
        // Only blocked heads age into a barrier; fitting work still follows DRR.
        // Enqueue sequence orders competing barriers without comparing clocks.
        var barrier: ?*Entry = null;
        for (ordinary_queues) |index| if (self.queueHead(index)) |head| {
            const age_blocked = head.age_protected and (!self.fitsCpu(head, free) or self.resourceProbe(head) == .wait);
            if (head.state == .queued and (head.bypasses >= self.config.max_backfills or age_blocked) and
                (barrier == null or head.sequence < barrier.?.sequence)) barrier = head;
        };
        if (self.resource_barrier) |resource| {
            if (barrier == null or resource.sequence < barrier.?.sequence) barrier = resource;
        }
        if (barrier) |e| {
            _ = self.resourceProbe(e);
            if (e.state != .queued) {
                barrier = null;
            } else {
                const resource_class = self.barrierClass(e);
                if (self.fitsCpu(e, free)) if (resource_class) |c| self.payWorkDebt(c, 0);
                const can_pay = if (resource_class) |c| e.slots <= self.credits[c] else true;
                if (can_pay and self.fitsCpu(e, free) and e.service_debt == 0 and self.acquireGrantResources(e)) {
                    if (resource_class) |c| self.credits[c] -= e.slots;
                    return e;
                }
                if (e.state != .queued) barrier = null else if (self.resourceProbe(e) == .ready and can_pay and !self.fitsCpu(e, free)) {
                    return self.chooseDrainProgress(free, e);
                } else self.rememberResourceBarrier(e);
            }
        }
        const eligible = outer: for (ordinary_queues) |index| {
            var candidates = self.classCandidates(index);
            while (candidates.take()) |e| {
                if (self.barrierAllows(barrier, e) and self.fitsCpu(e, free)) {
                    const probe = self.resourceProbe(e);
                    if (e.state == .queued and (probe == .ready or probe == .unknown)) break :outer true;
                }
            }
        } else false;
        if (!eligible) return null;
        // Deficit round robin charges slot demand, rather than grant count.
        // Idle classes accumulate no credit. Bound rounds even for large jobs.
        const rounds = (self.config.slots + 64) * ordinary_queues.len + ordinary_queues.len;
        for (0..rounds) |_| {
            const c = self.cursor;
            const head = self.queueHead(ordinary_queues[c]);
            if (!self.round_open) {
                self.credits[c] = @min(self.config.slots + self.weight(c), self.credits[c] +| self.weight(c));
                self.round_open = true;
            }
            var candidates = self.classCandidates(ordinary_queues[c]);
            var fitting_candidate = false;
            while (candidates.take()) |e| {
                if (!self.barrierAllows(barrier, e)) continue;
                if (self.fitsCpu(e, free)) {
                    fitting_candidate = true;
                    const probe = self.resourceProbe(e);
                    if (e.state != .queued) continue;
                    if (probe != .ready and probe != .unknown) {
                        if (head == e and e.slots <= self.credits[c]) {
                            self.rememberResourceBarrier(e);
                            barrier = self.earliestBarrier(barrier);
                        }
                        continue;
                    }
                    self.payWorkDebt(c, 0);
                    const paid = @min(e.service_debt, self.credits[c]);
                    e.service_debt -= paid;
                    self.credits[c] -= paid;
                    self.metrics.read_service_debt_paid +|= paid;
                    if (e.service_debt == 0 and e.slots <= self.credits[c]) {
                        if (!self.acquireGrantResources(e)) {
                            if (head == e) {
                                self.rememberResourceBarrier(e);
                                barrier = self.earliestBarrier(barrier);
                            }
                            continue;
                        }
                        self.credits[c] -= e.slots;
                        return e;
                    }
                    break;
                }
            }
            // A backlogged class which has paid for its head must be able to
            // assemble that team's CPU slots. Otherwise a stream of smaller
            // jobs in other classes steals every fragmented free slot until
            // the starvation backstop overrides weighted sharing. Retain a
            // paid admission barrier across dispatches while existing work
            // drains; retained continuations keep their own class credit.
            if (!fitting_candidate) if (head) |team| {
                if (team.state == .queued and team.slots <= self.credits[c]) {
                    if (self.resourceProbe(team) != .ready) {
                        self.rememberResourceBarrier(team);
                        barrier = self.earliestBarrier(barrier);
                    } else {
                        // Preserve this earned round until the team fits. A
                        // full one-slot isolated lane needs no assembly and
                        // cannot stop borrowing from the other lane's excess.
                        if (!self.config.readIsolation() or
                            (!self.boundedRead(team) and team.slots > 1 and team.slots <= self.capacityFor(team, team.class)))
                            return self.chooseDrainProgress(free, team);
                    }
                }
            };
            if (head == null) self.credits[c] = 0;
            self.cursor = (c + 1) % ordinary_queues.len;
            self.round_open = false;
        }
        return null;
    }
    fn dispatch(self: *Scheduler) void {
        self.dispatchWithLoan(false);
    }
    fn dispatchWithLoan(self: *Scheduler, transferring_live_grant: bool) void {
        self.dispatch_epoch +%= 1;
        if (self.dispatch_epoch == 0) {
            // Wrap is practically unreachable, but cannot revive a stale probe.
            var entry = self.entries;
            while (entry) |e| : (entry = e.all_next) e.resource_probe_epoch = 0;
            self.dispatch_epoch = 1;
        }
        // A just-transferred live loan first offers its former general capacity
        // to queued work. Other dispatches may immediately convert a loan back.
        if (!transferring_live_grant) self.convertCpuLoanToGeneral();
        // Close removes ordinary queued entries, but preauthorized active
        // completion/cleanup retains the same bounded queues and capacity partition.
        while (self.queueHead(4)) |e| {
            if (e.slots > self.config.control_slots - self.used[4]) break;
            self.grant(e);
        }
        while (self.queueHead(5)) |e| {
            if (e.slots > self.config.cleanup_slots - self.used[5]) break;
            self.grant(e);
        }
        const capacity = self.config.ordinarySlots();
        while (self.ordinaryUsed() < capacity) {
            const free = capacity - self.ordinaryUsed();
            const e = self.choose(free) orelse break;
            for (ordinary_queues) |index| if (self.queueHead(index)) |head| {
                const probe = self.resourceProbe(head);
                if (head.state == .queued and head != e and (head.slots > free or probe == .wait)) {
                    head.bypasses +|= 1;
                    self.metrics.backfills +|= 1;
                }
            };
            self.grant(e);
        }
        // A team barrier or current deficit may prevent the released general
        // capacity being used. Keep no loan when ordinary capacity still fits.
        if (transferring_live_grant) self.convertCpuLoanToGeneral();
    }

    pub fn submit(self: *Scheduler, e: *Entry) !void {
        try e.options.check();
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.parent != null) return error.InvalidRunnableTransition;
        try self.submitLocked(e, now, false, false);
    }

    /// Reserve a child before publishing its task. Each child uses the same
    /// finite operation/byte registry and fair CPU queue, with independent
    /// park/yield state. Its IO must be an independently owned worker lane;
    /// every deadline, clock and cancellation borrow comes from the parent.
    pub fn submitChild(self: *Scheduler, parent: *Entry, child: *Entry, io: std.Io) !void {
        return self.submitChildMode(parent, child, io, false);
    }

    /// Reserve registry ownership without publishing CPU demand. An unused
    /// packet cannot hold capacity needed by another physical worker or root.
    fn reserveChild(self: *Scheduler, parent: *Entry, child: *Entry, io: std.Io) !void {
        return self.submitChildMode(parent, child, io, true);
    }

    fn submitChildMode(self: *Scheduler, parent: *Entry, child: *Entry, io: std.Io, initially_blocked: bool) !void {
        try self.checkAuthority(parent);
        const now = parent.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (parent == child or parent.scheduler != self or parent.state != .running or parent.cleanup_active or parent.protected_cleanup_root or parent.cpu_borrow_budget != null or child.cpu_borrow_budget != null or
            child.state != .fresh or child.scheduler != null or child.parent != null or child.children != 0 or parent.slots != 1) return error.InvalidRunnableTransition;
        try self.checkAncestorsLocked(parent);
        if (child.bytes < @sizeOf(Entry)) return error.InvalidRunnableDemand;
        child.options = parent.options;
        // An implicit owner clock belongs to the parent Io, not to the
        // independently supplied worker lane. Preserve that clock explicitly.
        child.options.clock_io = parent.options.clock_io orelse parent.options.io;
        child.options.io = io;
        child.options.retained_bytes = child.bytes;
        if (self.boundedRead(parent)) return error.InvalidRunnableTransition;
        child.class = parent.class;
        child.read_lane = parent.read_lane;
        child.slots = 1;
        // Accepted completion is inherited, never reserved or extended by the
        // child. The parent and its original clock live through every join.
        child.completion_options = if (parent.completion_active) child.options else null;
        child.completion_active = parent.completion_active;
        try self.submitLocked(child, now, parent.completion_active, initially_blocked);
        child.parent = parent;
        parent.children += 1;
        self.metrics.child_operations += 1;
    }

    fn submitLocked(self: *Scheduler, e: *Entry, now: u64, completion_child: bool, initially_blocked: bool) !void {
        if (e.state != .fresh or e.scheduler != null or e.continuation) return error.InvalidRunnableTransition;
        if (e.class == .control and e.pending_resources != null) return error.InvalidRunnableTransition;
        const capacity = self.capacityFor(e, e.class);
        if (e.cpu_borrow_budget) |budget| try self.validateCpuBorrow(e, budget);
        if (e.borrowed_read_cpu) return error.InvalidRunnableTransition;
        // Retained consumers may admit finite trusted cleanup after ordinary
        // closure. Its independent reserve cannot become new request authority.
        if (e.protected_cleanup_root and (e.class != .control or e.slots != 1 or e.options.allow_wait or
            e.options.deadline_ns == null or e.options.deadline_ns.? > now +| (250 * std.time.ns_per_ms)))
            return error.InvalidArgument;
        if (self.closed and !completion_child and !e.protected_cleanup_root) return error.SchedulerClosed;
        if (e.slots == 0 or e.slots > capacity) return error.InvalidRunnableDemand;
        const control = e.class == .control;
        const control_count = @min(@as(usize, 4), self.config.max_operations / 2);
        const bounded = self.boundedRead(e);
        const bounded_count = if (self.config.readIsolation()) self.boundedCountLimit() else 0;
        const bounded_bytes = if (self.config.readIsolation()) self.boundedByteLimit() else 0;
        const count_limit = if (control) control_count else if (bounded) bounded_count else self.config.max_operations - control_count - bounded_count;
        const count_used = if (control) self.metrics.control_operations else if (bounded) self.metrics.bounded_read_operations else self.metrics.operations - self.metrics.control_operations - self.metrics.bounded_read_operations - self.metrics.read_transition_operations;
        const byte_limit = if (control) self.config.control_bytes else if (bounded) bounded_bytes else self.config.max_bytes - self.config.control_bytes - bounded_bytes;
        const byte_used = if (control) self.metrics.control_retained_bytes else if (bounded) self.metrics.bounded_read_bytes else self.metrics.retained_bytes - self.metrics.control_retained_bytes - self.metrics.bounded_read_bytes - self.metrics.read_transition_bytes;
        if (e.read_transition_budget) |budget| {
            if (!bounded or e.slots != 1 or budget.max_retained_bytes < e.bytes or
                budget.max_wait_ms == 0 or budget.max_wait_ms > self.config.max_wait_ms or budget.estimated_work_ns == 0)
                return error.InvalidRunnableDemand;
        }
        if (e.protected_cleanup_root) {
            // Cleanup roots share the control partition, but cannot consume
            // its last operation or more than half its retained byte reserve.
            const cleanup_limit = @min(@as(usize, 2), control_count -| 1);
            if (!control or self.metrics.cleanup_operations >= cleanup_limit or
                e.bytes > (self.config.control_bytes / 2) -| self.metrics.cleanup_retained_bytes)
            {
                self.metrics.rejected +|= 1;
                return error.ResourceTemporarilyUnavailable;
            }
        }
        const transition_count: usize = if (e.read_transition_budget != null) 1 else 0;
        const transition_bytes: u64 = if (e.read_transition_budget) |budget| budget.max_retained_bytes else 0;
        const reserved_count = if (bounded) self.metrics.read_transition_reserved_operations else 0;
        const reserved_bytes = if (bounded) self.metrics.read_transition_reserved_bytes else 0;
        if (count_used + reserved_count + transition_count + 1 > count_limit or
            e.bytes > byte_limit -| byte_used -| reserved_bytes or
            transition_bytes > byte_limit -| byte_used -| reserved_bytes -| e.bytes)
        {
            self.metrics.rejected +|= 1;
            return error.SchedulerFull;
        }
        self.metrics.read_transition_reserved_operations += transition_count;
        self.metrics.read_transition_reserved_bytes += transition_bytes;
        if (bounded) {
            self.metrics.bounded_read_operations += 1;
            self.metrics.bounded_read_bytes += e.bytes;
        }
        e.reserved_control = control;
        if (control) {
            self.metrics.control_operations += 1;
            self.metrics.control_retained_bytes += e.bytes;
        }
        if (e.protected_cleanup_root) {
            self.metrics.cleanup_operations += 1;
            self.metrics.cleanup_retained_bytes += e.bytes;
        }
        // Registry/count/bytes cover queued, running AND blocked operations.
        // Resume never needs another reservation or allocation.
        self.metrics.operations += 1;
        self.metrics.retained_bytes += e.bytes;
        e.scheduler = self;
        e.all_next = self.entries;
        if (self.entries) |head| head.all_previous = e;
        self.entries = e;
        self.sequence +|= 1;
        e.sequence = self.sequence;
        if (initially_blocked) {
            e.state = .resource_blocked;
            self.metrics.resource_blocked += 1;
        } else {
            self.enqueue(e, now);
            self.dispatch();
            e.waited = e.state == .queued;
        }
    }

    fn checkAuthority(self: *Scheduler, e: *const Entry) !void {
        e.options.check() catch |err| {
            // A parent worker's IO cancellation may be independent of the
            // explicit token copied into children. Publish its observed cause
            // to the retained family before returning to that worker.
            self.lock();
            const family_owned = e.scheduler == self and (e.children != 0 or e.parent != null);
            self.mutex.unlock();
            if (family_owned) self.failWait(@constCast(e), err, true);
            return err;
        };
        self.lock();
        defer self.mutex.unlock();
        try self.checkAncestorsLocked(e);
        // Close fences foreground publication even while the caller still
        // owns a live grant. Physical joins and owning finish retire it.
        if (self.closed and !e.retainedPhase()) return error.SchedulerClosed;
    }

    fn checkAncestorsLocked(self: *Scheduler, e: *const Entry) !void {
        var ancestor = e.parent;
        while (ancestor) |parent| : (ancestor = parent.parent) {
            if (parent.scheduler != self) return error.InvalidRunnableTransition;
            if (parent.state == .stopped) return parent.failure orelse error.InvalidRunnableTransition;
        }
    }

    pub fn state(self: *Scheduler, e: *const Entry) State {
        self.lock();
        defer self.mutex.unlock();
        return e.state;
    }

    /// Initial acquisition retires an unpublished entry on failure. A caller
    /// cannot use this API to discard retained completion/cleanup consumers.
    pub fn wait(self: *Scheduler, e: *Entry) !void {
        self.lock();
        const completing = e.scheduler == self and (e.retainedPhase() or e.children != 0);
        self.mutex.unlock();
        if (completing) return self.waitRetained(e);
        return self.waitImpl(e, false, null);
    }
    /// A published execution may lend its retained ownership to IO or a child.
    /// Denied reentry stops runnable execution, but only its owning finish may
    /// release operation/byte reservations after those consumers are fenced.
    pub fn waitRetained(self: *Scheduler, e: *Entry) !void {
        return self.waitImpl(e, true, null);
    }
    pub fn waitRetainedWithCheck(self: *Scheduler, e: *Entry, restriction: ?AdmissionCheck) !void {
        return self.waitImpl(e, true, restriction);
    }
    fn failWait(self: *Scheduler, e: *Entry, err: anyerror, comptime retain: bool) void {
        if (!retain) return self.finish(e);
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self) return;
        switch (e.state) {
            .queued => self.unlink(e),
            .running => self.releaseCpu(e, now),
            .resource_blocked => self.metrics.resource_blocked -= 1,
            .output_blocked => self.metrics.output_blocked -= 1,
            .stopped => return,
            .fresh, .finished => unreachable,
        }
        e.state = .stopped;
        e.failure = err;
        e.ready.set(e.options.io);
        self.dispatch();
    }
    fn waitImpl(self: *Scheduler, e: *Entry, comptime retain: bool, restriction: ?AdmissionCheck) !void {
        self.lock();
        const owned = e.scheduler == self;
        const terminal: ?anyerror = if (owned and e.state == .stopped) e.failure orelse error.InvalidRunnableTransition else null;
        self.mutex.unlock();
        if (!owned) return error.InvalidRunnableTransition;
        // A retained refusal already fenced execution. Later clocks or caller
        // restrictions cannot replace its cause or retire its owning resources.
        if (terminal) |cause| {
            if (!retain) self.finish(e);
            return cause;
        }
        const options = e.options;
        // Fail-fast initial consumers may run under a transport lock. Never
        // suspend that caller for CPU; unpublished rejection releases its
        // reservation. Published reentry still uses its bounded fair queue.
        if (!retain and !options.allow_wait and e.waited) {
            self.failWait(e, error.ResourceTemporarilyUnavailable, false);
            return error.ResourceTemporarilyUnavailable;
        }
        const start = options.now();
        const until = @min(options.deadline_ns orelse std.math.maxInt(u64), e.queued_since_ns +| @as(u64, self.config.max_wait_ms) * std.time.ns_per_ms);
        defer {
            const elapsed = options.now() -| start;
            self.lock();
            self.metrics.wait_ns +|= elapsed;
            self.mutex.unlock();
        }
        while (true) {
            if (restriction) |fence| fence.checkpoint() catch |err| {
                self.failWait(e, err, retain);
                return err;
            };
            self.pollWait(e) catch |err| {
                if (!retain) self.finish(e);
                return err;
            };
            self.lock();
            const still_owned = e.scheduler == self;
            const s = e.state;
            const failure = e.failure;
            const closed = self.closed;
            const completing = e.retainedPhase();
            const waited = e.waited;
            self.mutex.unlock();
            if (!still_owned) return error.InvalidRunnableTransition;
            if (waited and options.now() >= until) {
                self.failWait(e, error.RunnableWaitTimeout, retain);
                return error.RunnableWaitTimeout;
            }
            if (s == .running and (!closed or completing)) {
                self.checkAuthority(e) catch |err| {
                    self.failWait(e, err, retain);
                    return err;
                };
                if (restriction) |fence| fence.checkpoint() catch |err| {
                    self.failWait(e, err, retain);
                    return err;
                };
                try self.beginService(e);
                return;
            }
            if (closed and !completing) {
                self.failWait(e, error.SchedulerClosed, retain);
                return error.SchedulerClosed;
            }
            if (s != .queued) {
                const err = failure orelse error.InvalidRunnableTransition;
                self.failWait(e, err, retain);
                return err;
            }
            if (options.now() >= until) {
                self.failWait(e, error.RunnableWaitTimeout, retain);
                return error.RunnableWaitTimeout;
            }
            e.ready.waitTimeout(options.io, .{ .duration = .{
                .raw = .fromNanoseconds(@intCast(@min(until -| options.now(), 5 * std.time.ns_per_ms))),
                .clock = .awake,
            } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => {
                    self.failWait(e, err, retain);
                    return err;
                },
            };
        }
    }
    pub fn yield(self: *Scheduler, e: *Entry) !void {
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running) return error.InvalidRunnableTransition;
        self.releaseCpu(e, now);
        self.metrics.yields +|= 1;
        e.bypasses = 0;
        self.sequence +|= 1;
        e.sequence = self.sequence;
        if (self.closed and !e.retainedPhase()) {
            e.state = .stopped;
            e.failure = error.SchedulerClosed;
            self.dispatch();
            return error.SchedulerClosed;
        }
        self.enqueue(e, now);
        self.dispatch();
        e.waited = e.state == .queued;
    }
    /// Change a running capsule's CPU team demand. Call only after joining all
    /// children of the old team. Growth relinquishes the old grant and queues
    /// the full demand fairly; count/bytes stay reserved through denied reentry.
    pub fn resizeTeam(self: *Scheduler, e: *Entry, slots: usize) !void {
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running or e.children != 0) return error.InvalidRunnableTransition;
        const capacity = self.capacityFor(e, e.class);
        if (slots == 0 or slots > capacity) return error.InvalidRunnableDemand;
        if (e.cpu_borrow_budget != null and slots != 1) return error.InvalidRunnableTransition;
        // Only a separately reserved/activated retained phase may resize
        // after close. Other live work keeps its existing grant until finish.
        if (self.closed and !e.retainedPhase()) return error.SchedulerClosed;
        if (slots <= e.slots) {
            self.sampleServiceLocked(e, now);
            self.used[self.cpuIndex(e)] -= e.slots - slots;
            e.slots = slots;
            // Shrinking retains the current grant; no old queue deadline
            // applies to this synchronous return of CPU capacity.
            e.waited = false;
            self.dispatch();
            return;
        }
        self.releaseCpu(e, now);
        e.slots = slots;
        e.bypasses = 0;
        self.sequence +|= 1;
        e.sequence = self.sequence;
        self.enqueue(e, now);
        self.dispatch();
        e.waited = e.state == .queued;
    }
    pub fn block(self: *Scheduler, e: *Entry, reason: State) !void {
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running or
            (reason != .resource_blocked and reason != .output_blocked)) return error.InvalidRunnableTransition;
        self.releaseCpu(e, now);
        e.cpu_borrow_budget = null;
        if (self.closed and !e.retainedPhase()) {
            e.state = .stopped;
            e.failure = error.SchedulerClosed;
            e.ready.set(e.options.io);
            self.dispatch();
            return error.SchedulerClosed;
        }
        e.state = reason;
        if (reason == .resource_blocked) self.metrics.resource_blocked += 1 else self.metrics.output_blocked += 1;
        self.dispatch();
    }
    pub fn unblock(self: *Scheduler, e: *Entry) !void {
        // A blocked operation still owns its reservations, but cancellation,
        // expiry or family failure must be observed before dispatch can grant
        // CPU. Waiting checks authority again after any actual suspension.
        self.checkAuthority(e) catch |err| {
            self.failWait(e, err, true);
            return err;
        };
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self) return error.InvalidRunnableTransition;
        if (self.closed and !e.retainedPhase()) return error.SchedulerClosed;
        if (e.state != .resource_blocked and e.state != .output_blocked) return error.InvalidRunnableTransition;
        if (e.read_transition_active and !e.cleanup_active) return error.InvalidRunnableTransition;
        if (e.state == .resource_blocked) self.metrics.resource_blocked -= 1 else self.metrics.output_blocked -= 1;
        // Reserved children can already own external resources before their
        // first CPU grant. Reentry is a continuation even without prior CPU.
        e.continuation = true;
        e.bypasses = 0;
        self.sequence +|= 1;
        e.sequence = self.sequence;
        self.enqueue(e, now);
        self.dispatch();
        e.waited = e.state == .queued;
    }
    /// Only trusted adapters may promote a parked read for protected drain.
    /// Classification is never taken from client-provided scheduling labels.
    pub fn reclassifyBlocked(self: *Scheduler, e: *Entry, class: Class) !void {
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self) return error.InvalidRunnableTransition;
        if (self.closed and !e.retainedPhase()) return error.SchedulerClosed;
        if (e.state != .resource_blocked and e.state != .output_blocked) return error.InvalidRunnableTransition;
        if (e.read_transition_active and !e.cleanup_active) return error.InvalidRunnableTransition;
        if ((e.children != 0 or e.parent != null) and class != e.class) return error.InvalidRunnableTransition;
        // Retained completion/cleanup cannot promote its capacity partition.
        if (e.retainedPhase() and class != e.class) return error.InvalidRunnableTransition;
        const capacity = self.capacityFor(e, class);
        if (e.slots > capacity) return error.InvalidRunnableDemand;
        if (self.boundedRead(e) and class != .query) return error.InvalidRunnableTransition;
        e.class = class;
    }

    /// General/analytical classification changes only at a parked boundary.
    /// Bounded promotion/demotion needs its own verified transition owner.
    pub fn reclassifyReadBlocked(self: *Scheduler, e: *Entry, lane: ReadLane) !void {
        try self.checkAuthority(e);
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.SchedulerClosed;
        if (e.scheduler != self or e.class != .query or e.parent != null or e.children != 0 or
            e.retainedPhase() or lane == .bounded or e.read_lane == .bounded or
            (e.state != .resource_blocked and e.state != .output_blocked)) return error.InvalidRunnableTransition;
        e.read_lane = lane;
    }

    fn releaseReadTransitionCredit(self: *Scheduler, e: *Entry) void {
        if (e.read_transition_budget) |budget| {
            self.metrics.read_transition_reserved_operations -= 1;
            self.metrics.read_transition_reserved_bytes -= budget.max_retained_bytes;
            e.read_transition_budget = null;
        }
    }

    pub fn attachReadResources(self: *Scheduler, e: *Entry, transfer: ReadResourceTransfer) !void {
        try self.checkAuthority(e);
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running or !self.boundedRead(e) or e.read_transition_budget == null or
            e.read_resource_transfer != null or e.children != 0 or e.parent != null or e.retainedPhase()) return error.InvalidRunnableTransition;
        e.read_resource_transfer = transfer;
    }

    /// Trusted caller has reached a verified yield point and joined helpers.
    /// Actual ownership moves once; reserved capacity is not another live copy.
    pub fn beginReadDemotion(self: *Scheduler, e: *Entry, target: ReadLane) !void {
        try self.checkAuthority(e);
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        const budget = e.read_transition_budget orelse return error.InvalidRunnableTransition;
        if (e.scheduler != self or e.state != .running or !self.boundedRead(e) or target == .bounded or
            e.children != 0 or e.parent != null or e.retainedPhase() or e.cleanup_options == null or e.slots != 1)
            return error.InvalidRunnableTransition;
        if (e.read_resource_transfer) |transfer| if (!transfer.begin(transfer.context)) return error.ResourceTemporarilyUnavailable;
        self.settleServiceLocked(e, now);
        self.used[self.cpuIndex(e)] -= 1;
        self.metrics.bounded_read_operations -= 1;
        self.metrics.bounded_read_bytes -= e.bytes;
        self.metrics.read_transition_operations += 1;
        self.metrics.read_transition_bytes += e.bytes;
        self.metrics.resource_blocked += 1;
        self.metrics.read_demotions +|= 1;
        e.read_transition_active = true;
        e.read_transition_target = target;
        e.read_transition_started_ns = now;
        _ = budget;
        const quantum = @as(u64, self.config.quantum_ms) * std.time.ns_per_ms;
        const measured_service = e.measured_read_work_ns / quantum + @intFromBool(e.measured_read_work_ns % quantum != 0);
        e.service_debt = @intCast(@min(@as(u64, 64), measured_service -| e.bounded_service_paid));
        e.state = .resource_blocked;
        self.sequence +|= 1;
        e.sequence = self.sequence;
        self.dispatch();
    }

    /// No runnable authority is granted here. The caller must reenter through
    /// the original checked CPU queue after this atomic ownership transfer.
    pub fn tryCompleteReadDemotion(self: *Scheduler, e: *Entry) !bool {
        self.lock();
        const terminal = if (e.scheduler == self and e.state == .stopped) e.failure else null;
        self.mutex.unlock();
        if (terminal) |cause| return cause;
        self.checkAuthority(e) catch |err| {
            self.failWait(e, err, true);
            return err;
        };
        const now = e.options.now();
        self.lock();
        if (e.scheduler != self or !e.read_transition_active or e.state != .resource_blocked or e.retainedPhase()) {
            self.mutex.unlock();
            return error.InvalidRunnableTransition;
        }
        const budget = e.read_transition_budget.?;
        if (now -| e.read_transition_started_ns >= @as(u64, budget.max_wait_ms) * std.time.ns_per_ms) {
            self.metrics.read_transition_expired +|= 1;
            self.mutex.unlock();
            self.failWait(e, error.RunnableWaitTimeout, true);
            return error.RunnableWaitTimeout;
        }
        var other = self.entries;
        while (other) |older| : (other = older.all_next) {
            if (older != e and older.read_transition_active and older.state == .resource_blocked and older.sequence < e.sequence) {
                self.mutex.unlock();
                return false;
            }
        }
        const count_limit = self.config.max_operations - @min(@as(usize, 4), self.config.max_operations / 2) - self.boundedCountLimit();
        const count_used = self.metrics.operations - self.metrics.control_operations - self.metrics.bounded_read_operations - self.metrics.read_transition_operations;
        const bytes_used = self.metrics.retained_bytes - self.metrics.control_retained_bytes - self.metrics.bounded_read_bytes - self.metrics.read_transition_bytes;
        if (count_used >= count_limit or e.bytes > self.config.generalMetadataBytes() - bytes_used) {
            self.mutex.unlock();
            return false;
        }
        if (e.read_resource_transfer) |transfer| if (!transfer.try_general(transfer.context)) {
            self.mutex.unlock();
            return false;
        };
        self.metrics.read_transition_operations -= 1;
        self.metrics.read_transition_bytes -= e.bytes;
        e.read_transition_active = false;
        e.read_lane = e.read_transition_target;
        self.releaseReadTransitionCredit(e);
        self.mutex.unlock();
        return true;
    }

    /// Adopt a larger immutable input footprint under the same foreground root.
    /// Credits never shrink before owning finish. Original control/ordinary
    /// metadata partition and all clock/cancellation authority remain unchanged.
    pub fn growInputBytes(self: *Scheduler, e: *Entry, bytes: u64) !void {
        try e.options.check();
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.SchedulerClosed;
        if (e.scheduler != self or e.state != .running or e.parent != null or e.children != 0 or
            e.retainedPhase() or e.completion_options != null or e.cleanup_options != null) return error.InvalidRunnableTransition;
        if (e.cpu_borrow_budget != null and bytes != e.bytes) return error.InvalidRunnableTransition;
        if (bytes < e.bytes) return error.InvalidRunnableDemand;
        if (self.boundedRead(e) and bytes != e.bytes) return error.InvalidRunnableTransition;
        const delta = bytes - e.bytes;
        const limit = if (e.reserved_control) self.config.control_bytes else self.config.max_bytes - self.config.control_bytes - (if (self.config.readIsolation()) self.boundedByteLimit() else 0);
        const used = if (e.reserved_control) self.metrics.control_retained_bytes else self.metrics.retained_bytes - self.metrics.control_retained_bytes - self.metrics.bounded_read_bytes - self.metrics.read_transition_bytes;
        if (delta > limit - used) {
            self.metrics.rejected +|= 1;
            return error.SchedulerFull;
        }
        if (e.reserved_control) self.metrics.control_retained_bytes += delta;
        self.metrics.retained_bytes += delta;
        e.bytes = bytes;
        e.options.retained_bytes = bytes;
    }

    /// Incremental, conservative charge for actual retained read obligations.
    /// Unlike resizing untrusted input, this may follow preauthorized cleanup
    /// reservation. It grants no CPU, changes no clock/class, and never waits.
    /// A caller already owns the stable root through all physical dependents.
    pub fn retainReadBytes(self: *Scheduler, e: *Entry, additional: u64) !void {
        try self.checkAuthority(e);
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.SchedulerClosed;
        if (e.scheduler != self or e.parent != null or e.class != .query or
            e.retainedPhase() or e.completion_options != null or e.read_transition_active or
            (e.state != .running and e.state != .resource_blocked and e.state != .output_blocked)) return error.InvalidRunnableTransition;
        if (e.cpu_borrow_budget != null and additional != 0) return error.InvalidRunnableTransition;
        const total = std.math.add(u64, e.bytes, additional) catch return error.InvalidRunnableDemand;
        const bounded = self.boundedRead(e);
        if (bounded) {
            const budget = e.read_transition_budget orelse return error.InvalidRunnableTransition;
            if (total > budget.max_retained_bytes) return error.InvalidRunnableDemand;
        }
        const limit = if (bounded) self.boundedByteLimit() else self.config.max_bytes - self.config.control_bytes - (if (self.config.readIsolation()) self.boundedByteLimit() else 0);
        const used = if (bounded) self.metrics.bounded_read_bytes else self.metrics.retained_bytes - self.metrics.control_retained_bytes - self.metrics.bounded_read_bytes - self.metrics.read_transition_bytes;
        if (additional > limit -| used) {
            self.metrics.rejected +|= 1;
            return error.SchedulerFull;
        }
        if (bounded) self.metrics.bounded_read_bytes += additional;
        self.metrics.retained_bytes += additional;
        e.bytes = total;
        e.options.retained_bytes = total;
        if (e.cleanup_options) |*cleanup| cleanup.retained_bytes = total;
    }

    /// Tighten a native-clock deadline before publishing children. Decoding a
    /// body timeout after admission must never grant a fresh execution budget.
    pub fn tightenDeadline(self: *Scheduler, e: *Entry, deadline_ns: u64) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.SchedulerClosed;
        if (e.scheduler != self or e.state != .running or e.parent != null or e.children != 0 or
            e.retainedPhase() or e.completion_options != null or e.cleanup_options != null) return error.InvalidRunnableTransition;
        e.options.deadline_ns = @min(e.options.deadline_ns orelse deadline_ns, deadline_ns);
    }

    /// Reserve completion ownership while the original foreground execution
    /// is still running, before any potentially durable proposal. This uses
    /// the already bounded entry, operation and byte reservation, not a new
    /// foreground or control admission. It does not prove a storage decision.
    pub fn reserveCompletion(self: *Scheduler, e: *Entry, completion: admission.Options) !void {
        if (completion.deadline_ns == null or completion.cancellation != null or !completion.allow_wait)
            return error.InvalidArgument;
        try e.options.check();
        try completion.check();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running or
            (e.class != .write and e.class != .control) or
            e.completion_options != null or e.cpu_borrow_budget != null or e.retainedPhase() or e.parent != null) return error.InvalidRunnableTransition;
        if (self.closed) return error.SchedulerClosed;
        if (completion.retained_bytes > e.bytes) return error.InvalidRunnableDemand;
        var options = completion;
        options.retained_bytes = e.bytes;
        e.completion_options = options;
    }

    /// Reserve cleanup while foreground execution still owns a live grant.
    /// The independent task, callback context, allocator and all physical work
    /// must already be retained by the caller before publishing any resource.
    /// Operation/bytes remain the existing bounded entry; no new admission or
    /// control promotion is available when cleanup eventually activates.
    pub fn reserveCleanup(self: *Scheduler, e: *Entry, cleanup: admission.Options) !void {
        return self.reserveCleanupChecked(e, cleanup, null);
    }

    /// Preauthorize a finite physical cleanup duration before publishing a
    /// resource whose idle lifetime can exceed the foreground query deadline.
    /// This starts exactly once, including fair CPU wait, at activation. It
    /// never refreshes query authority or an already active cleanup deadline.
    pub fn reserveCleanupFor(self: *Scheduler, e: *Entry, cleanup: admission.Options, duration_ns: u64) !void {
        return self.reserveCleanupChecked(e, cleanup, duration_ns);
    }

    fn reserveCleanupChecked(self: *Scheduler, e: *Entry, cleanup: admission.Options, duration_ns: ?u64) !void {
        if (cleanup.cancellation != null or !cleanup.allow_wait) return error.InvalidArgument;
        if (duration_ns) |duration| {
            if (duration == 0 or cleanup.deadline_ns != null) return error.InvalidArgument;
            _ = std.math.add(u64, cleanup.now(), duration) catch return error.InvalidArgument;
        } else if (cleanup.deadline_ns == null) return error.InvalidArgument;
        try e.options.check();
        try cleanup.check();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running or e.cleanup_options != null or e.retainedPhase() or e.parent != null) return error.InvalidRunnableTransition;
        if (self.closed) return error.SchedulerClosed;
        if (cleanup.retained_bytes > e.bytes) return error.InvalidRunnableDemand;
        var options = cleanup;
        options.retained_bytes = e.bytes;
        e.cleanup_options = options;
        e.cleanup_duration_ns = duration_ns;
    }

    /// After fencing foreground children, transfer the retained entry to its
    /// preauthorized cleanup authority. Unlike durable completion, cleanup must
    /// activate after foreground cancellation, denied reentry or owner close.
    /// A stopped foreground is never resumed with its expired query authority.
    /// Fair CPU reentry can fail; then the cleanup owner still retains all bytes
    /// and must join or transfer physical ownership before owning finish.
    pub fn activateCleanup(self: *Scheduler, e: *Entry) !void {
        var cleanup = e.cleanup_options orelse return error.InvalidRunnableTransition;
        const foreground_now = e.options.now();
        const now = cleanup.now();
        if (e.cleanup_duration_ns) |duration| {
            cleanup.deadline_ns = std.math.add(u64, now, duration) catch return error.InvalidArgument;
        }
        try cleanup.check();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.retainedPhase() or e.children != 0 or e.parent != null) return error.InvalidRunnableTransition;
        switch (e.state) {
            .running => {},
            .queued => self.unlink(e),
            .resource_blocked => self.metrics.resource_blocked -= 1,
            .output_blocked => self.metrics.output_blocked -= 1,
            .stopped => {},
            .fresh, .finished => return error.InvalidRunnableTransition,
        }
        const running = e.state == .running;
        const isolated = self.config.cleanup_slots != 0;
        // Foreground children are fenced above. Retire the old team grant before
        // changing its CPU partition; cleanup is serial and cannot publish new
        // foreground children. Operation/policy class and bytes remain retained.
        const released_cpu = running and (isolated or e.borrowed_read_cpu);
        if (released_cpu) self.releaseCpu(e, foreground_now) else if (running) self.settleServiceLocked(e, foreground_now);
        e.cpu_borrow_budget = null;
        if (isolated) e.slots = 1;
        e.options = cleanup;
        e.cleanup_options = cleanup;
        e.cleanup_duration_ns = null;
        e.cleanup_active = true;
        e.pending_resources = null;
        // Inactive write completion was never accepted. Its authorization
        // cannot survive a transfer into physical cleanup.
        e.completion_options = null;
        e.failure = null;
        e.continuation = true;
        e.bypasses = 0;
        e.waited = false;
        if (!running or released_cpu) {
            self.sequence +|= 1;
            e.sequence = self.sequence;
            self.enqueue(e, now);
            self.dispatch();
            e.waited = e.state == .queued;
        }
    }

    /// Enter the independent completion phase before possible durable
    /// acceptance. The caller now owns all joins or an authoritative durable
    /// transfer on every exit, including an ambiguous storage outcome.
    pub fn activateCompletion(self: *Scheduler, e: *Entry) !void {
        try e.options.check();
        const completion = e.completion_options orelse return error.InvalidRunnableTransition;
        try completion.check();
        const foreground_now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running or e.retainedPhase() or e.cpu_borrow_budget != null or e.children != 0 or e.parent != null) return error.InvalidRunnableTransition;
        if (self.closed) return error.SchedulerClosed;
        const options = e.completion_options orelse return error.InvalidRunnableTransition;
        self.settleServiceLocked(e, foreground_now);
        e.options = options;
        e.completion_active = true;
        // No queue time from the foreground phase governs this retained grant.
        e.waited = false;
    }

    pub fn finish(self: *Scheduler, e: *Entry) void {
        // The owner retains clock receivers through this call; an idempotent
        // repeated finish must never call an already retired receiver.
        if (e.scheduler == null) return;
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler == null) return;
        std.debug.assert(e.scheduler == self and e.children == 0);
        switch (e.state) {
            .queued => self.unlink(e),
            .running => self.releaseCpu(e, now),
            .resource_blocked => self.metrics.resource_blocked -= 1,
            .output_blocked => self.metrics.output_blocked -= 1,
            else => {},
        }
        if (e.all_previous) |p| p.all_next = e.all_next else self.entries = e.all_next;
        if (e.all_next) |n| n.all_previous = e.all_previous;
        e.all_next = null;
        e.all_previous = null;
        if (e.read_transition_active) {
            self.metrics.read_transition_operations -= 1;
            self.metrics.read_transition_bytes -= e.bytes;
        } else if (self.boundedRead(e)) {
            self.metrics.bounded_read_operations -= 1;
            self.metrics.bounded_read_bytes -= e.bytes;
        }
        e.read_transition_active = false;
        self.releaseReadTransitionCredit(e);
        e.read_resource_transfer = null;
        self.metrics.operations -= 1;
        self.metrics.retained_bytes -= e.bytes;
        if (e.parent) |parent| {
            std.debug.assert(parent.scheduler == self and parent.children != 0);
            parent.children -= 1;
            self.metrics.child_operations -= 1;
            e.parent = null;
        }
        if (e.reserved_control) {
            self.metrics.control_operations -= 1;
            self.metrics.control_retained_bytes -= e.bytes;
        }
        if (e.protected_cleanup_root) {
            self.metrics.cleanup_operations -= 1;
            self.metrics.cleanup_retained_bytes -= e.bytes;
            e.protected_cleanup_root = false;
        }
        e.state = .finished;
        e.cpu_borrow_budget = null;
        std.debug.assert(!e.borrowed_read_cpu);
        e.scheduler = null;
        e.pending_resources = null;
        e.completion_options = null;
        e.completion_active = false;
        e.cleanup_options = null;
        e.cleanup_duration_ns = null;
        e.cleanup_active = false;
        self.dispatch();
    }
    pub fn close(self: *Scheduler) void {
        self.lock();
        defer self.mutex.unlock();
        self.closed = true;
        var next = self.entries;
        while (next) |e| : (next = e.all_next) {
            if (e.retainedPhase()) continue;
            switch (e.state) {
                .queued => self.unlink(e),
                .resource_blocked => self.metrics.resource_blocked -= 1,
                .output_blocked => self.metrics.output_blocked -= 1,
                // Live execution is joined by its caller, never revoked.
                else => continue,
            }
            e.state = .stopped;
            e.failure = error.SchedulerClosed;
            e.ready.set(e.options.io);
        }
        self.dispatch();
    }
    pub fn stats(self: *Scheduler) Stats {
        self.lock();
        defer self.mutex.unlock();
        return self.statsLocked();
    }
    pub fn policyAndStats(self: *Scheduler) struct { config: Config, stats: Stats, service: ServiceStats } {
        self.lock();
        defer self.mutex.unlock();
        var service = self.work_service;
        service.adjustment_slot_ns = self.work_adjustment_ns;
        return .{ .config = self.config, .stats = self.statsLocked(), .service = service };
    }
    fn statsLocked(self: *const Scheduler) Stats {
        var result = self.metrics;
        result.runnable_slots = self.ordinaryUsed() + self.used[4] + self.used[5];
        result.control_slots = self.used[4] + self.used[5];
        result.cleanup_runnable_slots = self.used[5];
        result.bounded_read_slots = self.used[6];
        result.cpu_borrowed_slots = @intFromBool(self.cpu_borrower != null);
        result.cpu_borrow_grants = self.cpu_borrow_grants;
        result.cpu_borrow_returns = self.cpu_borrow_returns;
        return result;
    }
};

/// Stable node lifetime, distinct from the API instances borrowing it. Native
/// archive consumers must use owner callbacks rather than dereference this type.
pub const NodeOwner = struct {
    scheduler: Scheduler,
    mutex: std.atomic.Mutex = .unlocked,
    borrowers: usize = 0,
    closed: bool = false,

    pub fn init(config: Config) !NodeOwner {
        return .{ .scheduler = try Scheduler.init(config) };
    }
    pub fn retain(self: *NodeOwner) !void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.closed) return error.SchedulerClosed;
        self.borrowers += 1;
    }
    /// A consumer retained before closure may still retire its own physical
    /// resources. A fresh unpinned consumer cannot resurrect a closed owner.
    pub fn retainCleanup(self: *NodeOwner) !void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.closed and self.borrowers == 0) return error.SchedulerClosed;
        self.borrowers += 1;
    }
    pub fn release(self: *NodeOwner) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        std.debug.assert(self.borrowers != 0);
        self.borrowers -= 1;
    }
    pub fn close(self: *NodeOwner) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        self.closed = true;
        self.mutex.unlock();
        self.scheduler.close();
    }
    pub fn deinit(self: *NodeOwner) void {
        self.tryDeinit() catch @panic("runnable node owner still in use");
    }
    pub fn tryDeinit(self: *NodeOwner) !void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        const retained = self.scheduler.stats();
        if (self.borrowers != 0 or retained.operations != 0 or retained.retained_bytes != 0) {
            self.mutex.unlock();
            return error.InvalidRunnableTransition;
        }
        self.closed = true;
        self.mutex.unlock();
        self.scheduler.close();
        self.* = undefined;
    }
};

/// Stable, single-worker execution capsule. Parallel children must get their
/// own ownership before sharing this capsule's adapter; it is not a gang lease.
pub const Execution = struct {
    entry: ?Entry = null,
    /// Trusted local adapter to an already admitted opaque owner. The adapter
    /// and original owner outlive every physical borrower and joined child.
    /// A root borrow's finish only retires the adapter; owned child adapters
    /// retire their owner-produced token after the worker has physically joined.
    borrowed: ?Borrow = null,
    child_pending: bool = false,
    quantum_start_ns: u64 = 0,
    read_sample_ns: u64 = 0,
    checkpoints: u32 = 0,
    /// Changes only after a successful grant. Readers use it to invalidate
    /// authority checks across cooperative yields and resource/output waits.
    resume_generation: u64 = 0,

    pub const Borrow = struct {
        ptr: *anyopaque,
        class: Class,
        current_class: ?*const fn (*anyopaque) anyerror!Class = null,
        cpu_borrow: ?*const fn (*anyopaque, ?CpuBorrowBudget) anyerror!void = null,
        request_resources: ?*const fn (*anyopaque, GrantResources) anyerror!void = null,
        enter_resources: ?*const fn (*anyopaque, GrantResources, ?AdmissionCheck) anyerror!void = null,
        checkpoint: *const fn (*anyopaque) anyerror!void,
        check_lifetime: *const fn (*anyopaque) anyerror!void,
        park: *const fn (*anyopaque, State) anyerror!void,
        enter: *const fn (*anyopaque, Class) anyerror!void,
        enter_checked: ?*const fn (*anyopaque, Class, AdmissionCheck) anyerror!void = null,
        reserve_child: *const fn (*anyopaque, std.Io, u64, *Execution) anyerror!void,
        activate_child: *const fn (*anyopaque) anyerror!void,
        finish: *const fn (*anyopaque) void,
    };

    pub fn bindBorrowed(self: *Execution, borrow: Borrow) !void {
        if (self.entry != null or self.borrowed != null or self.child_pending) return error.InvalidRunnableTransition;
        self.borrowed = borrow;
    }

    pub fn retainedClass(self: *const Execution) !Class {
        if (self.borrowed) |borrow| return if (borrow.current_class) |current| current(borrow.ptr) else borrow.class;
        const entry = if (self.entry) |*value| value else return error.InvalidRunnableTransition;
        const owner = entry.scheduler orelse return error.InvalidRunnableTransition;
        owner.lock();
        defer owner.mutex.unlock();
        return entry.class;
    }

    pub fn start(self: *Execution, scheduler: ?*Scheduler, options: admission.Options, class: Class) !void {
        try self.startWithSlots(scheduler, options, class, 1);
    }
    /// One stable capsule can account for a bounded CPU team. Its demand stays
    /// attached across yields and waits, independently of retained memory.
    pub fn startWithSlots(self: *Execution, scheduler: ?*Scheduler, options: admission.Options, class: Class, slots: usize) !void {
        return self.startRoot(scheduler, options, class, slots, false, .general, null, null);
    }

    /// Native owning executors only: initial CPU publication and a complete
    /// prepared resource bundle share one grant. There is no unscheduled fallback.
    /// Resource ownership remains with the caller through physical retirement.
    pub fn startWithResources(self: *Execution, scheduler: *Scheduler, options: admission.Options, class: Class, slots: usize, resources: GrantResources) !void {
        return self.startRoot(scheduler, options, class, slots, false, .general, null, resources);
    }
    pub fn requestResources(self: *Execution, resources: GrantResources) !void {
        if (self.borrowed) |borrow| {
            try self.checkLifetime();
            const callback = borrow.request_resources orelse return error.UnsupportedOperation;
            return callback(borrow.ptr, resources);
        }
        const e = if (self.entry) |*value| value else return error.InvalidRunnableTransition;
        const owner = try schedulerFor(e, false);
        try owner.requestResources(e, resources);
    }

    /// Synchronous native-owner stage transition. Participant metadata may be
    /// temporary: every return detaches it under the owner lock, including a
    /// refused reentry which retains the root for physical cleanup. Borrowed
    /// owners require an explicit detachable capability and are rejected before
    /// parking; ordinary requestResources retains its longer-lived contract.
    pub fn enterWithResources(self: *Execution, resources: GrantResources, restriction: ?AdmissionCheck) !void {
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (self.borrowed) |borrow| {
            const callback = borrow.enter_resources orelse return error.ExecutionWaitUnsupported;
            try callback(borrow.ptr, resources, restriction);
            self.resume_generation +%= 1;
            return;
        }
        const e = if (self.entry) |*value| value else return error.InvalidRunnableTransition;
        const owner = try schedulerFor(e, false);
        const class = try self.retainedClass();
        if (class == .control or e.retainedPhase() or e.pending_resources != null) return error.InvalidRunnableTransition;
        try self.park(.resource_blocked);
        self.requestResources(resources) catch |err| {
            owner.failWait(e, err, true);
            return err;
        };
        defer owner.detachResources(e, resources);
        try self.enterWithCheck(class, restriction);
    }

    /// Prepared cooperative stages only. Disable before unqualified work.
    pub fn startBorrowable(self: *Execution, scheduler: ?*Scheduler, options: admission.Options, class: Class, budget: CpuBorrowBudget) !void {
        return self.startRoot(scheduler, options, class, 1, false, .general, budget, null);
    }
    pub fn enableCpuBorrow(self: *Execution, budget: CpuBorrowBudget) !void {
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (self.borrowed) |borrow| {
            const callback = borrow.cpu_borrow orelse return error.UnsupportedOperation;
            return callback(borrow.ptr, budget);
        }
        const e = if (self.entry) |*value| value else return;
        const owner = try schedulerFor(e, true);
        try owner.enableCpuBorrow(e, budget);
    }
    /// Leave a qualified stage before arbitrary policy, allocation or IO work.
    /// Ordinary checkpoints must not refresh their quantum or generation merely
    /// because a validator runs. Opaque owners require the checked capability.
    pub fn endCpuBorrowStage(self: *Execution) !void {
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (self.borrowed != null) return self.disableCpuBorrow();
        const e = if (self.entry) |*value| value else return;
        const owner = try schedulerFor(e, true);
        owner.lock();
        const enabled = e.cpu_borrow_budget != null;
        owner.mutex.unlock();
        if (enabled) try self.disableCpuBorrow();
    }

    pub fn disableCpuBorrow(self: *Execution) !void {
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (self.borrowed) |borrow| {
            const callback = borrow.cpu_borrow orelse return error.UnsupportedOperation;
            try callback(borrow.ptr, null);
            self.resume_generation +%= 1;
            return;
        }
        const e = if (self.entry) |*value| value else return;
        const owner = try schedulerFor(e, true);
        try owner.disableCpuBorrow(e);
        try owner.waitRetained(e);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.read_sample_ns = self.quantum_start_ns;
        self.checkpoints = 0;
    }

    /// Trusted server classification only. A bounded operator must have a
    /// verified fixed footprint, one worker, no child fanout and no promotion
    /// to mutation/completion. Unknown plans always enter the general lane.
    pub fn startRead(self: *Execution, scheduler: ?*Scheduler, options: admission.Options, lane: ReadLane) !void {
        return self.startRoot(scheduler, options, .query, 1, false, lane, null, null);
    }

    /// Both transition credits and finite cleanup authority exist before the
    /// caller may publish its first operator resource or execute query work.
    pub fn startDemotableRead(self: *Execution, owner: *Scheduler, options: admission.Options, budget: ReadTransitionBudget, cleanup: admission.Options, cleanup_duration_ns: u64) !void {
        if (self.entry != null or self.borrowed != null) return error.InvalidRunnableTransition;
        self.entry = .{ .options = options, .class = .query, .read_lane = .bounded, .bytes = options.retained_bytes, .read_transition_budget = budget };
        errdefer self.finish();
        try owner.submit(&self.entry.?);
        try owner.wait(&self.entry.?);
        try owner.reserveCleanupFor(&self.entry.?, cleanup, cleanup_duration_ns);
        self.quantum_start_ns = options.now();
        self.read_sample_ns = self.quantum_start_ns;
        self.resume_generation +%= 1;
    }

    fn measureReadWork(self: *Execution, owner: *Scheduler, e: *Entry) void {
        const now = e.options.now();
        owner.lock();
        if (e.scheduler == owner and e.state == .running and e.read_transition_budget != null and !e.retainedPhase())
            e.measured_read_work_ns +|= now -| self.read_sample_ns;
        owner.mutex.unlock();
        self.read_sample_ns = now;
    }

    pub fn demoteRead(self: *Execution, target: ReadLane) !void {
        if (self.child_pending or self.borrowed != null) return error.InvalidRunnableTransition;
        const e = if (self.entry) |*entry| entry else return error.InvalidRunnableTransition;
        const owner = try schedulerFor(e, true);
        self.measureReadWork(owner, e);
        try owner.beginReadDemotion(e, target);
        while (!(try owner.tryCompleteReadDemotion(e))) {
            e.options.io.sleep(.fromMilliseconds(1), .awake) catch |err| {
                owner.failWait(e, err, true);
                return err;
            };
        }
        try owner.unblock(e);
        try owner.waitRetained(e);
        self.quantum_start_ns = e.options.now();
        self.read_sample_ns = self.quantum_start_ns;
        self.checkpoints = 0;
        self.resume_generation +%= 1;
    }

    pub fn classifyRead(self: *Execution, lane: ReadLane) !void {
        if (lane == .bounded) return error.InvalidRunnableTransition;
        // Older opaque owners retain the conservative general classification.
        if (self.borrowed != null) return;
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, true);
        try owner.checkAuthority(e);
        if (e.read_lane == lane) return;
        try owner.block(e, .resource_blocked);
        try owner.reclassifyReadBlocked(e, lane);
        try owner.unblock(e);
        try owner.waitRetained(e);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.read_sample_ns = self.quantum_start_ns;
        self.checkpoints = 0;
    }

    /// Independent finite cleanup authority, never a refreshed read scope.
    /// Admission is fail-fast and preserves global health/control headroom.
    pub fn startCleanupRoot(self: *Execution, scheduler: *Scheduler, options: admission.Options) !void {
        if (options.deadline_ns == null or options.allow_wait) return error.InvalidArgument;
        if (options.deadline_ns.? > options.now() +| (250 * std.time.ns_per_ms)) return error.InvalidArgument;
        return self.startRoot(scheduler, options, .control, 1, true, .general, null, null);
    }

    fn startRoot(self: *Execution, scheduler: ?*Scheduler, options: admission.Options, class: Class, slots: usize, protected_cleanup: bool, read_lane: ReadLane, cpu_borrow_budget: ?CpuBorrowBudget, resources: ?GrantResources) !void {
        std.debug.assert(self.entry == null and self.borrowed == null);
        const owner = scheduler orelse return;
        self.entry = .{ .options = options, .class = class, .read_lane = read_lane, .slots = slots, .bytes = options.retained_bytes, .protected_cleanup_root = protected_cleanup, .cpu_borrow_budget = cpu_borrow_budget, .pending_resources = resources };
        errdefer self.finish();
        try owner.submit(&self.entry.?);
        // A new cleanup root has no published consumer yet. Respect its
        // fail-fast acquisition; only subsequent reentry retains ownership.
        if (protected_cleanup) try owner.waitImpl(&self.entry.?, false, null) else try owner.wait(&self.entry.?);
        self.resume_generation +%= 1;
        self.quantum_start_ns = options.now();
        self.read_sample_ns = self.quantum_start_ns;
    }
    /// Stable child metadata and the independent worker IO must outlive every
    /// physical task join. Finishing the parent with live children is invalid.
    pub fn startChild(self: *Execution, parent: *Execution, io: std.Io, retained_bytes: u64) !void {
        try self.reserveChild(parent, io, retained_bytes);
        errdefer self.finish();
        try self.activateChild();
    }

    /// Reserve before task publication without waiting for CPU on the parent
    /// coordinator. The independent worker activates after publication.
    pub fn reserveChild(self: *Execution, parent: *Execution, io: std.Io, retained_bytes: u64) !void {
        if (self.entry != null or self.borrowed != null or self == parent or parent.child_pending) return error.InvalidRunnableTransition;
        if (retained_bytes < @sizeOf(Execution)) return error.InvalidRunnableDemand;
        if (parent.borrowed) |borrow| {
            try borrow.reserve_child(borrow.ptr, io, retained_bytes, self);
            self.child_pending = true;
            return;
        }
        const parent_entry = if (parent.entry) |*entry| entry else return error.InvalidRunnableTransition;
        const owner = try schedulerFor(parent_entry, true);
        self.entry = .{ .options = parent_entry.options, .class = parent_entry.class, .bytes = retained_bytes };
        errdefer self.finish();
        try owner.reserveChild(parent_entry, &self.entry.?, io);
        self.child_pending = true;
    }

    /// Published task ownership survives rejected activation until physical
    /// join and owning finish. This cannot replay an already activated child.
    pub fn activateChild(self: *Execution) !void {
        if (self.borrowed) |borrow| {
            if (!self.child_pending) return error.InvalidRunnableTransition;
            self.child_pending = false;
            try borrow.activate_child(borrow.ptr);
            self.resume_generation +%= 1;
            return;
        }
        const e = if (self.entry) |*entry| entry else return error.InvalidRunnableTransition;
        if (!self.child_pending or e.parent == null) {
            _ = try schedulerFor(e, false);
            return error.InvalidRunnableTransition;
        }
        self.child_pending = false;
        const owner = try schedulerFor(e, false);
        try owner.unblock(e);
        try owner.waitRetained(e);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.read_sample_ns = self.quantum_start_ns;
        self.checkpoints = 0;
    }

    fn schedulerFor(e: *Entry, comptime needs_running: bool) !*Scheduler {
        const owner = e.scheduler orelse return error.InvalidRunnableTransition;
        owner.lock();
        defer owner.mutex.unlock();
        // A denied resume is terminal until owning finish. Later phase or
        // cleanup checks must preserve that cause, not attempt to resurrect it.
        if (e.state == .stopped) return e.failure orelse error.InvalidRunnableTransition;
        if (needs_running and e.state != .running) return error.InvalidRunnableTransition;
        return owner;
    }
    /// Read original explicit lifetime and retained family state from another
    /// physical worker. This neither requires CPU nor observes the caller's
    /// thread-local IO cancellation, which must not stop a borrowed parent.
    pub fn checkLifetime(self: *const Execution) !void {
        if (self.borrowed) |borrow| return borrow.check_lifetime(borrow.ptr);
        const e = if (self.entry) |*entry| entry else return error.InvalidRunnableTransition;
        const owner = e.scheduler orelse return error.InvalidRunnableTransition;
        const options = blk: {
            owner.lock();
            defer owner.mutex.unlock();
            if (e.scheduler != owner) return error.InvalidRunnableTransition;
            if (e.state == .stopped) return e.failure orelse error.InvalidRunnableTransition;
            try owner.checkAncestorsLocked(e);
            if (owner.closed and !e.retainedPhase()) return error.SchedulerClosed;
            break :blk e.options;
        };
        if (options.cancellation) |token| if (token.requested(token.ptr)) return error.Canceled;
        if (options.deadline_ns) |deadline| if (options.now() >= deadline) return error.DeadlineExceeded;
    }

    pub fn growInputBytes(self: *Execution, bytes: u64) !void {
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (self.borrowed != null) return error.ExecutionWaitUnsupported;
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, true);
        try owner.growInputBytes(e, bytes);
    }

    pub fn retainReadBytes(self: *Execution, additional: u64) !void {
        if (self.child_pending or self.borrowed != null) return error.InvalidRunnableTransition;
        const e = if (self.entry) |*entry| entry else return error.InvalidRunnableTransition;
        const owner = try schedulerFor(e, false);
        try owner.retainReadBytes(e, additional);
    }

    pub fn tightenDeadline(self: *Execution, deadline_ns: u64) !void {
        if (self.child_pending or self.borrowed != null) return error.InvalidRunnableTransition;
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, true);
        try owner.tightenDeadline(e, deadline_ns);
        try self.checkLifetime();
    }

    /// A resume-time fence rejects execution and descendants, retaining every
    /// reservation for physical cleanup. Preserve the first terminal cause.
    pub fn rejectRetained(self: *Execution, cause: anyerror) !void {
        if (self.child_pending or self.borrowed != null) return error.InvalidRunnableTransition;
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, false);
        owner.failWait(e, cause, true);
    }

    /// Reserve bounded parallel CPU on this same execution before publishing
    /// child work. Children borrow the team grant and must all join before any
    /// resize, park, checkpoint or finish. Physical IO ownership and accepted
    /// durable completion require their separate capabilities.
    pub fn resizeTeam(self: *Execution, slots: usize) !void {
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (slots == 0) return error.InvalidRunnableDemand;
        if (self.borrowed != null) return error.UnsupportedOperation;
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, true);
        try owner.checkAuthority(e);
        try owner.resizeTeam(e, slots);
        try owner.waitRetained(e);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.read_sample_ns = self.quantum_start_ns;
        self.checkpoints = 0;
    }
    pub fn checkpoint(self: *Execution) !void {
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (self.borrowed) |borrow| {
            try borrow.checkpoint(borrow.ptr);
            // Opaque owners may yield on every checkpoint. Conservatively
            // invalidate authority caches after each successful callback.
            self.resume_generation +%= 1;
            return;
        }
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, true);
        owner.checkAuthority(e) catch |err| {
            if (owner.borrowingCpu(e)) {
                owner.failWait(e, err, true);
                return err;
            }
            if (err == error.SchedulerClosed) {
                self.checkpoints +|= 1;
                // Preserve the existing safe-quantum CPU return so accepted
                // completion can progress before foreground consumers join.
                if (self.checkpoints >= owner.config.checkpoint_budget or
                    e.options.now() -| self.quantum_start_ns >= @as(u64, owner.config.quantum_ms) * std.time.ns_per_ms)
                    try owner.yield(e);
            }
            return err;
        };
        self.measureReadWork(owner, e);
        if (e.read_transition_budget) |budget| {
            if (!e.read_transition_active and owner.boundedRead(e) and e.measured_read_work_ns > budget.estimated_work_ns) {
                try self.demoteRead(.general);
                return;
            }
        }
        self.checkpoints +|= 1;
        if (!owner.returnBorrowRequested(e) and self.checkpoints < owner.config.checkpoint_budget and
            e.options.now() -| self.quantum_start_ns < @as(u64, owner.config.quantum_ms) * std.time.ns_per_ms) return;
        try owner.yield(e);
        try owner.waitRetained(e);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.read_sample_ns = self.quantum_start_ns;
        self.checkpoints = 0;
    }
    pub fn park(self: *Execution, reason: State) !void {
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (self.borrowed) |borrow| return borrow.park(borrow.ptr, reason);
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, false);
        self.measureReadWork(owner, e);
        try owner.block(e, reason);
    }
    pub fn enter(self: *Execution, class: Class) !void {
        return self.enterWithCheck(class, null);
    }
    /// Acquire a move-only admission lease while the original root is parked.
    /// Preserve its address, bytes, clock and cancellation authority across the
    /// phase change. A lease acquired before failed resume is retired here.
    /// The operation must use the original request options; it must not create
    /// another runnable root or refresh the request deadline.
    pub fn acquirePhaseLease(self: *Execution, class: Class, comptime operation: anytype, args: anytype) anyerror!@typeInfo(@typeInfo(@TypeOf(operation)).@"fn".return_type.?).error_union.payload {
        if (self.entry == null and self.borrowed == null) return @call(.auto, operation, args);
        try self.checkLifetime();
        const previous_class = try self.retainedClass();
        try self.park(.resource_blocked);
        var lease = @call(.auto, operation, args) catch |err| {
            // Even a refused acquisition resumes the original owner before
            // propagating the failure. Failed resume keeps its retained root.
            try self.enter(previous_class);
            return err;
        };
        errdefer lease.release();
        try self.enter(class);
        try self.checkLifetime();
        return lease;
    }
    pub fn enterWithCheck(self: *Execution, class: Class, restriction: ?AdmissionCheck) !void {
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (self.borrowed) |*borrow| {
            if (restriction) |fence| {
                const callback = borrow.enter_checked orelse return error.ExecutionWaitUnsupported;
                try callback(borrow.ptr, class, fence);
            } else try borrow.enter(borrow.ptr, class);
            borrow.class = class;
            self.resume_generation +%= 1;
            return;
        }
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, false);
        owner.checkAuthority(e) catch |err| {
            owner.failWait(e, err, true);
            return err;
        };
        if (restriction) |fence| fence.checkpoint() catch |err| {
            owner.failWait(e, err, true);
            return err;
        };
        try owner.reclassifyBlocked(e, class);
        try owner.unblock(e);
        try owner.waitRetainedWithCheck(e, restriction);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.read_sample_ns = self.quantum_start_ns;
        self.checkpoints = 0;
    }
    /// The independent Io/deadline and all retained resources must be owned
    /// through joined completion. Foreground cancellation is deliberately not
    /// copied into this capability. This is local trusted authority only.
    pub fn reserveCompletion(self: *Execution, completion: admission.Options) !void {
        const e = if (self.entry) |*entry| entry else return error.ExecutionWaitUnsupported;
        const owner = try schedulerFor(e, true);
        try owner.checkAuthority(e);
        try owner.reserveCompletion(e, completion);
    }
    pub fn activateCompletion(self: *Execution) !void {
        const e = if (self.entry) |*entry| entry else return error.ExecutionWaitUnsupported;
        const owner = try schedulerFor(e, true);
        // Activation must precede acceptance and may not escape a foreground
        // fence which has already failed. Afterwards only completion fences
        // govern the still-retained root.
        try owner.checkAuthority(e);
        const options = e.completion_options orelse return error.InvalidRunnableTransition;
        try options.check();
        try owner.activateCompletion(e);
        self.quantum_start_ns = options.now();
        self.read_sample_ns = self.quantum_start_ns;
        self.checkpoints = 0;
    }
    /// Read the retained completion fence without issuing a new deadline.
    /// Foreground cancellation and scheduler close do not replace its clock.
    pub fn completionRemainingNs(self: *Execution) !u64 {
        const entry = if (self.entry) |*value| value else return error.ExecutionWaitUnsupported;
        if (!entry.completion_active) return error.ExecutionWaitUnsupported;
        const options = entry.completion_options orelse return error.InvalidRunnableTransition;
        try options.check();
        const deadline = options.deadline_ns orelse return error.InvalidArgument;
        const remaining = deadline -| options.now();
        if (remaining == 0) return error.DeadlineExceeded;
        return remaining;
    }

    pub fn reserveCleanup(self: *Execution, cleanup: admission.Options) !void {
        const e = if (self.entry) |*entry| entry else return error.ExecutionWaitUnsupported;
        const owner = try schedulerFor(e, true);
        try owner.reserveCleanup(e, cleanup);
    }
    pub fn reserveCleanupFor(self: *Execution, cleanup: admission.Options, duration_ns: u64) !void {
        const e = if (self.entry) |*entry| entry else return error.ExecutionWaitUnsupported;
        const owner = try schedulerFor(e, true);
        try owner.reserveCleanupFor(e, cleanup, duration_ns);
    }
    pub fn activateCleanup(self: *Execution) !void {
        const e = if (self.entry) |*entry| entry else return error.ExecutionWaitUnsupported;
        // A stopped foreground deliberately retains its entry for cleanup.
        // Validate ownership in the scheduler rather than rejecting that state
        // through schedulerFor's foreground transition check.
        const owner = e.scheduler orelse return error.InvalidRunnableTransition;
        try owner.activateCleanup(e);
        try owner.waitRetained(e);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.read_sample_ns = self.quantum_start_ns;
        self.checkpoints = 0;
    }
    pub fn finish(self: *Execution) void {
        if (self.borrowed) |borrow| borrow.finish(borrow.ptr);
        self.borrowed = null;
        if (self.entry) |*e| if (e.scheduler) |owner| owner.finish(e);
        self.entry = null;
        self.child_pending = false;
    }
};

fn testEntry(class: Class, slots: usize, bytes: u64) Entry {
    return .{ .options = .{ .io = std.testing.io }, .class = class, .slots = slots, .bytes = bytes };
}

test "runnable scheduler weighted execution retains CPU demand across waits and class changes" {
    var scheduler = try Scheduler.init(.{ .slots = 4, .checkpoint_budget = 1 });
    defer scheduler.close();
    var inference: Execution = .{};
    defer inference.finish();
    try inference.startWithSlots(&scheduler, .{ .io = std.testing.io, .retained_bytes = 11 }, .inference, 2);
    var background: Execution = .{};
    defer background.finish();
    try background.startWithSlots(&scheduler, .{ .io = std.testing.io, .retained_bytes = 7 }, .background, 1);
    var control: Execution = .{};
    defer control.finish();
    try control.start(&scheduler, .{ .io = std.testing.io }, .control);
    try std.testing.expectEqual(@as(usize, 4), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().control_slots);
    try inference.park(.resource_blocked);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 18), scheduler.stats().retained_bytes);
    try inference.enter(.inference);
    try inference.checkpoint();
    try std.testing.expect(inference.resume_generation > 2);
    try std.testing.expectEqual(@as(usize, 4), scheduler.stats().runnable_slots);
    background.finish();
    control.finish();
    try inference.park(.output_blocked);
    try inference.enter(.query);
    try std.testing.expectEqual(Class.query, inference.entry.?.class);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
    inference.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    var invalid: Execution = .{};
    defer invalid.finish();
    for ([_]usize{ 0, 4 }) |slots| {
        try std.testing.expectError(error.InvalidRunnableDemand, invalid.startWithSlots(&scheduler, .{ .io = std.testing.io, .retained_bytes = 19 }, .query, slots));
        try std.testing.expect(invalid.entry == null);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "runnable scheduler node owner outlives independent borrowers and executions" {
    var owner = try NodeOwner.init(.{ .slots = 2, .checkpoint_budget = 1 });
    defer owner.deinit();
    try owner.retain();
    try owner.retain();
    try std.testing.expectError(error.InvalidRunnableTransition, owner.tryDeinit());
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&owner.scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
    owner.release();
    try execution.checkpoint();
    try std.testing.expectEqual(@as(usize, 1), owner.borrowers);
    try std.testing.expectEqual(@as(u64, 17), owner.scheduler.stats().retained_bytes);
    owner.close();
    try std.testing.expectError(error.SchedulerClosed, owner.retain());
    try std.testing.expectEqual(@as(usize, 1), owner.scheduler.stats().operations);
    owner.release();
    try std.testing.expectError(error.InvalidRunnableTransition, owner.tryDeinit());
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 0), owner.scheduler.stats().retained_bytes);
}

test "runnable scheduler preserves blocked ownership and protected capacity" {
    var s = try Scheduler.init(.{ .slots = 4, .control_slots = 1, .max_operations = 3, .max_bytes = 40, .control_bytes = 10 });
    var read = testEntry(.query, 3, 10);
    var next = testEntry(.write, 3, 10);
    var control = testEntry(.control, 1, 10);
    defer s.finish(&read);
    defer s.finish(&next);
    defer s.finish(&control);
    try s.submit(&read);
    try s.submit(&next);
    try s.submit(&control);
    try std.testing.expectEqual(State.running, s.state(&read));
    try std.testing.expectEqual(State.queued, s.state(&next));
    try std.testing.expectEqual(State.running, s.state(&control));
    try s.block(&read, .output_blocked);
    try std.testing.expectEqual(State.running, s.state(&next));
    try std.testing.expectEqual(@as(usize, 3), s.stats().operations);
    try std.testing.expectEqual(@as(u64, 30), s.stats().retained_bytes);
    var extra = testEntry(.query, 1, 0);
    try std.testing.expectError(error.SchedulerFull, s.submit(&extra));
    // No allocation/new outstanding slot needed even at the hard envelope.
    try s.unblock(&read);
    try std.testing.expectEqual(State.queued, s.state(&read));
    s.finish(&next);
    try s.wait(&read);
    try std.testing.expectEqual(State.running, s.state(&read));
    s.finish(&read);
    s.finish(&read);
    s.finish(&control);
    try std.testing.expectEqual(@as(usize, 0), s.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), s.stats().retained_bytes);
}

test "runnable scheduler bounds backfill and grants a large operation" {
    var s = try Scheduler.init(.{ .slots = 4, .max_backfills = 2 });
    var holder = testEntry(.write, 2, 0);
    var large = testEntry(.query, 3, 0);
    var small: [4]Entry = undefined;
    defer s.finish(&holder);
    defer s.finish(&large);
    defer for (&small) |*e| s.finish(e);
    for (&small) |*e| e.* = testEntry(.query, 1, 0);
    try s.submit(&holder);
    try s.submit(&large);
    for (&small) |*e| try s.submit(e);
    try std.testing.expectEqual(State.running, s.state(&small[0]));
    s.finish(&small[0]);
    try std.testing.expectEqual(State.running, s.state(&small[1]));
    s.finish(&small[1]);
    try std.testing.expectEqual(State.queued, s.state(&small[2]));
    try std.testing.expectEqual(@as(u16, 2), large.bypasses);
    s.finish(&holder);
    try std.testing.expectEqual(State.running, s.state(&large));
    try std.testing.expectEqual(State.queued, s.state(&small[2]));
}

test "runnable scheduler weighted sharing charges slots across busy classes" {
    var s = try Scheduler.init(.{ .slots = 2, .weights = .{ 2, 1, 1, 1 } });
    var holder = testEntry(.background, 1, 0);
    try s.submit(&holder);
    defer s.finish(&holder);
    var reads: [24]Entry = undefined;
    var writes: [24]Entry = undefined;
    for (&reads) |*e| e.* = testEntry(.query, 1, 0);
    for (&writes) |*e| e.* = testEntry(.write, 1, 0);
    defer for (&reads) |*e| s.finish(e);
    defer for (&writes) |*e| s.finish(e);
    for (&reads, &writes) |*r, *w| {
        try s.submit(r);
        try s.submit(w);
    }
    s.finish(&holder);
    for (0..30) |_| {
        var running: ?*Entry = null;
        for (&reads) |*e| if (s.state(e) == .running) {
            running = e;
            break;
        };
        if (running == null) for (&writes) |*e| if (s.state(e) == .running) {
            running = e;
            break;
        };
        s.finish(running orelse return error.NoRunnableWork);
    }
    const counts = s.stats().grants;
    // Includes the next grant after each finish. Both classes remain busy.
    try std.testing.expect(counts[0] >= 20 and counts[0] <= 21);
    try std.testing.expect(counts[1] >= 10 and counts[1] <= 11);
}

test "runnable scheduler weighted sharing preserves mixed team slot demand under fragmented capacity" {
    var scheduler = try Scheduler.init(.{ .slots = 3, .weights = .{ 2, 1, 1, 1 }, .max_operations = 256 });
    defer scheduler.close();
    var holder = testEntry(.background, 2, 0);
    try scheduler.submit(&holder);
    defer scheduler.finish(&holder);
    var queries: [96]Entry = undefined;
    var writes: [96]Entry = undefined;
    for (&queries) |*entry| entry.* = testEntry(.query, 1, 0);
    for (&writes) |*entry| entry.* = testEntry(.write, 2, 0);
    defer for (&queries) |*entry| scheduler.finish(entry);
    defer for (&writes) |*entry| scheduler.finish(entry);
    for (&queries, &writes) |*query, *write| {
        try scheduler.submit(query);
        try scheduler.submit(write);
    }
    var health = testEntry(.control, 1, 0);
    try scheduler.submit(&health);
    try std.testing.expectEqual(State.running, scheduler.state(&health));
    scheduler.finish(&health);
    scheduler.finish(&holder);
    for (0..60) |_| {
        const running = blk: {
            for (&queries) |*entry| if (scheduler.state(entry) == .running) break :blk entry;
            for (&writes) |*entry| if (scheduler.state(entry) == .running) break :blk entry;
            return error.NoRunnableWork;
        };
        scheduler.finish(running);
    }
    const grants = scheduler.stats().grants;
    // Both classes remain backlogged. A two-slot write pays twice a one-slot
    // query: 2:1 weights should grant roughly four queries per write, with a
    // bounded deficit/grant-in-flight tolerance.
    const write_charge = grants[1] * 2;
    if (grants[0] < write_charge * 2 -| 4 or grants[0] > write_charge * 2 + 4)
        std.debug.print("mixed team grants query={d} write={d}\n", .{ grants[0], grants[1] });
    try std.testing.expect(grants[0] >= write_charge * 2 -| 4);
    try std.testing.expect(grants[0] <= write_charge * 2 + 4);
}

test "runnable scheduler isolated teams retain earned weighted share while bounded reads progress" {
    inline for (.{ Class.write, Class.inference, Class.background }) |team_class| {
        inline for (.{ ReadLane.general, ReadLane.analytical }) |lane| {
            inline for (.{ false, true }) |bounded_busy| {
                var owner = try Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .weights = .{ 2, 1, 1, 1 }, .max_operations = 512, .max_backfills = 65535 });
                defer owner.close();
                var holder = testEntry(.background, 2, 0);
                try owner.submit(&holder);
                defer owner.finish(&holder);
                const initial_team_grants = owner.stats().grants[@backingInt(team_class)];
                var queries: [96]Entry = undefined;
                var writes: [96]Entry = undefined;
                for (&queries) |*entry| {
                    entry.* = testEntry(.query, 1, 0);
                    entry.read_lane = lane;
                }
                for (&writes) |*entry| entry.* = testEntry(team_class, 2, 0);
                defer for (&queries) |*entry| owner.finish(entry);
                defer for (&writes) |*entry| owner.finish(entry);
                for (&queries, &writes) |*query, *write| {
                    try owner.submit(query);
                    try owner.submit(write);
                }
                var bounded = testEntry(.query, 1, 0);
                bounded.read_lane = .bounded;
                if (bounded_busy) try owner.submit(&bounded);
                defer owner.finish(&bounded);
                var health = testEntry(.control, 1, 0);
                try owner.submit(&health);
                try std.testing.expectEqual(State.running, owner.state(&health));
                owner.finish(&health);
                owner.finish(&holder);
                for (0..60) |_| {
                    if (bounded_busy) {
                        try owner.yield(&bounded);
                        try std.testing.expectEqual(State.running, owner.state(&bounded));
                    }
                    const running = blk: {
                        for (&queries) |*entry| if (owner.state(entry) == .running) break :blk entry;
                        for (&writes) |*entry| if (owner.state(entry) == .running) break :blk entry;
                        return error.NoRunnableWork;
                    };
                    owner.finish(running);
                }
                const observed = owner.stats();
                const query_charge = observed.grants[@backingInt(Class.query)];
                const team_charge = (observed.grants[@backingInt(team_class)] - initial_team_grants) * 2;
                std.debug.print("isolated team service team={s} lane={s} query={d} team_slots={d} bounded={d}\n", .{ @tagName(team_class), @tagName(lane), query_charge, team_charge, observed.read_lane_grants[@backingInt(ReadLane.bounded)] });
                // A paid team must assemble its slots before the backfill starvation
                // threshold. Query weight includes both read partitions; when busy,
                // the independent bounded partition stays productive.
                try std.testing.expect(team_charge > 0);
                try std.testing.expect(query_charge >= team_charge * 2 -| 4);
                try std.testing.expect(query_charge <= team_charge * 2 + 4);
                try std.testing.expectEqual(@as(u64, if (bounded_busy) 61 else 0), observed.read_lane_grants[@backingInt(ReadLane.bounded)]);
            }
        }
    }
}

test "runnable scheduler earned team share stops draining when queued authority expires" {
    const Token = struct {
        fn requested(raw: *const anyopaque) bool {
            const value: *const bool = @ptrCast(@alignCast(raw));
            return value.*;
        }
    };
    inline for (.{ false, true }) |isolation| {
        for ([_]bool{ false, true }) |cancel| {
            var scheduler = try Scheduler.init(.{ .slots = if (isolation) 5 else 3, .cleanup_slots = if (isolation) 1 else 0, .read_isolation = isolation, .weights = .{ 1, 2, 1, 1 } });
            defer scheduler.close();
            var holder = testEntry(.background, 2, 1);
            var first = testEntry(.query, 1, 3);
            var second = testEntry(.query, 1, 7);
            var team = testEntry(.write, 2, 11);
            defer scheduler.finish(&holder);
            defer scheduler.finish(&first);
            defer scheduler.finish(&second);
            defer scheduler.finish(&team);
            try scheduler.submit(&holder);
            try scheduler.submit(&first);
            try scheduler.submit(&second);
            try scheduler.submit(&team);
            scheduler.finish(&holder);
            try std.testing.expectEqual(State.running, scheduler.state(&first));
            try std.testing.expectEqual(State.queued, scheduler.state(&second));
            try std.testing.expectEqual(State.queued, scheduler.state(&team));
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(u64, 21), scheduler.stats().retained_bytes);
            var health = testEntry(.control, 1, 0);
            try scheduler.submit(&health);
            try std.testing.expectEqual(State.running, scheduler.state(&health));
            scheduler.finish(&health);
            var canceled = cancel;
            if (cancel) team.options.cancellation = .{ .ptr = &canceled, .requested = Token.requested } else team.options.deadline_ns = 0;
            const cause: anyerror = if (cancel) error.Canceled else error.DeadlineExceeded;
            try std.testing.expectError(cause, scheduler.wait(&team));
            try std.testing.expectEqual(State.finished, scheduler.state(&team));
            try std.testing.expectEqual(State.running, scheduler.state(&second));
            try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 10), scheduler.stats().retained_bytes);
        }
    }
}

test "runnable scheduler retained refusal preserves its first cause before later fences" {
    const Fence = struct {
        calls: usize = 0,
        cause: anyerror,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return self.cause;
        }
    };
    for ([_]bool{ false, true }) |later_restriction| {
        var scheduler = try Scheduler.init(.{ .slots = 3 });
        defer scheduler.close();
        var entry = testEntry(.write, 2, 23);
        defer scheduler.finish(&entry);
        try scheduler.submit(&entry);
        var original: Fence = .{ .cause = error.CatalogGenerationChanged };
        try std.testing.expectError(error.CatalogGenerationChanged, scheduler.waitRetainedWithCheck(&entry, .{ .context = &original, .check = Fence.check }));
        try std.testing.expectEqual(@as(usize, 1), original.calls);
        try std.testing.expectEqual(State.stopped, scheduler.state(&entry));
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        entry.options.deadline_ns = 0;
        var later: Fence = .{ .cause = error.Forbidden };
        const restriction: ?AdmissionCheck = if (later_restriction) .{ .context = &later, .check = Fence.check } else null;
        try std.testing.expectError(error.CatalogGenerationChanged, scheduler.waitRetainedWithCheck(&entry, restriction));
        try std.testing.expectEqual(@as(usize, 0), later.calls);
        try std.testing.expectEqual(error.CatalogGenerationChanged, entry.failure.?);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        var health = testEntry(.control, 1, 0);
        try scheduler.submit(&health);
        try std.testing.expectEqual(State.running, scheduler.state(&health));
        scheduler.finish(&health);
        scheduler.finish(&entry);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "runnable scheduler cancellation after grant returns every reservation" {
    var s = try Scheduler.init(.{ .slots = 2 });
    var holder = testEntry(.query, 1, 5);
    var next = testEntry(.query, 1, 7);
    try s.submit(&holder);
    try s.submit(&next);
    defer s.finish(&holder);
    defer s.finish(&next);
    const Token = struct {
        fn requested(_: *const anyopaque) bool {
            return true;
        }
    };
    var token: u8 = 0;
    holder.options.cancellation = .{ .ptr = &token, .requested = Token.requested };
    try std.testing.expectError(error.Canceled, s.wait(&holder));
    try std.testing.expectEqual(State.finished, s.state(&holder));
    try std.testing.expectEqual(State.running, s.state(&next));
    try std.testing.expectEqual(@as(u64, 7), s.stats().retained_bytes);
}

test "runnable scheduler deadline and shutdown retire blocked and queued owners" {
    var s = try Scheduler.init(.{ .slots = 2 });
    var holder = testEntry(.query, 1, 4);
    var queued = testEntry(.query, 1, 5);
    try s.submit(&holder);
    try s.submit(&queued);
    queued.options.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, s.wait(&queued));
    try s.block(&holder, .resource_blocked);
    s.close();
    try std.testing.expectEqual(State.stopped, s.state(&holder));
    try std.testing.expectError(error.SchedulerClosed, s.wait(&holder));
    try std.testing.expectEqual(@as(usize, 0), s.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), s.stats().resource_blocked);
    try std.testing.expectEqual(@as(u64, 0), s.stats().retained_bytes);
}

test "runnable scheduler yield hands execution to another operation" {
    var s = try Scheduler.init(.{ .slots = 2, .weights = .{ 1, 1, 1, 1 } });
    var first = testEntry(.query, 1, 4);
    var second = testEntry(.write, 1, 6);
    try s.submit(&first);
    try s.submit(&second);
    defer s.finish(&first);
    defer s.finish(&second);
    try s.yield(&first);
    try std.testing.expectEqual(State.queued, s.state(&first));
    try std.testing.expectEqual(State.running, s.state(&second));
    try std.testing.expectEqual(@as(u64, 10), s.stats().retained_bytes);
    s.finish(&second);
    try s.wait(&first);
    try std.testing.expectEqual(@as(u64, 1), s.stats().yields);
}

const VirtualClock = struct {
    var value: u64 = 0;
    fn now() u64 {
        return value;
    }
};

test "runnable scheduler queue bound starts at enqueue including grant race" {
    VirtualClock.value = 0;
    var s = try Scheduler.init(.{ .slots = 2, .max_wait_ms = 5 });
    var holder = testEntry(.query, 1, 4);
    var queued = testEntry(.query, 1, 5);
    holder.options.native_now_ns = VirtualClock.now;
    queued.options.native_now_ns = VirtualClock.now;
    queued.options.deadline_ns = 20 * std.time.ns_per_ms;
    try s.submit(&holder);
    try s.submit(&queued);
    defer s.finish(&holder);
    defer s.finish(&queued);
    VirtualClock.value = 6 * std.time.ns_per_ms;
    // A release can win the notification race. Expired work still cannot start.
    s.finish(&holder);
    try std.testing.expectEqual(State.running, s.state(&queued));
    try std.testing.expectError(error.RunnableWaitTimeout, s.wait(&queued));
    try std.testing.expectEqual(@as(usize, 0), s.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 0), s.stats().operations);
}

test "runnable scheduler real queue timeout releases count and bytes" {
    var s = try Scheduler.init(.{ .slots = 2, .max_wait_ms = 2 });
    var holder = testEntry(.query, 1, 4);
    var queued = testEntry(.query, 1, 5);
    try s.submit(&holder);
    try s.submit(&queued);
    defer s.finish(&holder);
    defer s.finish(&queued);
    try std.testing.expectError(error.RunnableWaitTimeout, s.wait(&queued));
    try std.testing.expectEqual(@as(usize, 1), s.stats().operations);
    try std.testing.expectEqual(@as(u64, 4), s.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), s.stats().queued);
}

test "runnable scheduler rejects invalid policy and oversized demand atomically" {
    for ([_]Config{
        .{ .slots = 0 },                 .{ .slots = 65536 }, .{ .control_slots = 8 },
        .{ .max_operations = 0 },        .{ .max_bytes = 0 }, .{ .max_wait_ms = 0 },
        .{ .weights = .{ 1, 0, 1, 1 } },
    }) |cfg| try std.testing.expectError(error.InvalidRunnablePolicy, Scheduler.init(cfg));
    var s = try Scheduler.init(.{ .slots = 2, .max_bytes = 4, .control_bytes = 1 });
    var bytes = testEntry(.query, 1, 5);
    var slots = testEntry(.query, 2, 4);
    try std.testing.expectError(error.SchedulerFull, s.submit(&bytes));
    try std.testing.expectError(error.InvalidRunnableDemand, s.submit(&slots));
    try std.testing.expectEqual(State.fresh, s.state(&bytes));
    try std.testing.expectEqual(@as(usize, 0), s.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), s.stats().retained_bytes);
}

test "runnable scheduler shutdown racing a new block cannot strand an owner" {
    var s = try Scheduler.init(.{ .slots = 2 });
    var live = testEntry(.query, 1, 8);
    try s.submit(&live);
    s.close();
    try std.testing.expectError(error.SchedulerClosed, s.block(&live, .resource_blocked));
    try std.testing.expectEqual(State.stopped, s.state(&live));
    try std.testing.expectEqual(@as(usize, 0), s.stats().runnable_slots);
    try std.testing.expectError(error.SchedulerClosed, s.unblock(&live));
    s.finish(&live);
    try std.testing.expectEqual(@as(usize, 0), s.stats().operations);
}

test "runnable scheduler execution quanta preserve deadline and parked ownership" {
    VirtualClock.value = 0;
    var s = try Scheduler.init(.{ .slots = 2, .quantum_ms = 2, .checkpoint_budget = 64 });
    var exec: Execution = .{};
    try exec.start(&s, .{ .io = std.testing.io, .native_now_ns = VirtualClock.now, .deadline_ns = 10 * std.time.ns_per_ms, .retained_bytes = 17 }, .query);
    defer exec.finish();
    try std.testing.expectEqual(@as(u64, 1), exec.resume_generation);
    try exec.checkpoint();
    try std.testing.expectEqual(@as(u64, 1), exec.resume_generation);
    try std.testing.expectEqual(@as(u64, 0), s.stats().yields);
    VirtualClock.value = 3 * std.time.ns_per_ms;
    try exec.checkpoint();
    try std.testing.expectEqual(@as(u64, 1), s.stats().yields);
    try std.testing.expectEqual(@as(u64, 2), exec.resume_generation);
    try exec.park(.output_blocked);
    try std.testing.expectEqual(@as(u64, 2), exec.resume_generation);
    try std.testing.expectEqual(@as(usize, 0), s.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 17), s.stats().retained_bytes);
    var other: Execution = .{};
    try other.start(&s, .{ .io = std.testing.io }, .query);
    defer other.finish();
    // Verified completion drain uses reserved control slots at full foreground.
    try exec.enter(.control);
    try std.testing.expectEqual(@as(u64, 3), exec.resume_generation);
    try std.testing.expectEqual(@as(usize, 2), s.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), s.stats().control_slots);
    VirtualClock.value = 11 * std.time.ns_per_ms;
    try std.testing.expectError(error.DeadlineExceeded, exec.checkpoint());
    try std.testing.expectEqual(@as(u64, 3), exec.resume_generation);
    try std.testing.expectEqual(@as(?u64, 10 * std.time.ns_per_ms), exec.entry.?.options.deadline_ns);
}

test "runnable phase admission retains one root and retires leases after failed resume" {
    const Case = enum { success, refused, canceled, deadline, shutdown };
    const Signal = struct {
        canceled: bool = false,
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).canceled;
        }
    };
    const Acquire = struct {
        scheduler: *Scheduler,
        root: *Execution,
        controller: *admission.Controller,
        signal: *Signal,
        case: Case,
        fn run(self: *@This()) !admission.Controller.Lease {
            try std.testing.expectEqual(State.resource_blocked, self.scheduler.state(&self.root.entry.?));
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 77), self.scheduler.stats().retained_bytes);
            // The parked root does not prevent an ordinary peer or a control
            // operation from running while coarse admission is acquired.
            var peer: Execution = .{};
            try peer.start(self.scheduler, .{ .io = std.testing.io }, .query);
            defer peer.finish();
            var control: Execution = .{};
            try control.start(self.scheduler, .{ .io = std.testing.io }, .control);
            defer control.finish();
            if (self.case == .refused) return error.AdmissionFull;
            const lease = self.controller.tryAcquireLease() orelse return error.AdmissionFull;
            switch (self.case) {
                .canceled => self.signal.canceled = true,
                .deadline => VirtualClock.value = 10,
                .shutdown => self.scheduler.close(),
                else => {},
            }
            return lease;
        }
    };
    for (std.enums.values(Case)) |case| {
        VirtualClock.value = 0;
        defer VirtualClock.value = 0;
        var signal: Signal = .{};
        var scheduler = try Scheduler.init(.{ .slots = 2 });
        var controller = admission.Controller.init(1);
        var root: Execution = .{};
        try root.start(&scheduler, .{
            .io = std.testing.io,
            .native_now_ns = VirtualClock.now,
            .deadline_ns = 10,
            .cancellation = .{ .ptr = &signal, .requested = Signal.requested },
            .retained_bytes = 77,
        }, .query);
        const original = &root.entry.?;
        var acquire: Acquire = .{ .scheduler = &scheduler, .root = &root, .controller = &controller, .signal = &signal, .case = case };
        const result = root.acquirePhaseLease(.write, Acquire.run, .{&acquire});
        if (case == .success) {
            var lease = try result;
            try std.testing.expectEqual(Class.write, try root.retainedClass());
            try std.testing.expectEqual(State.running, scheduler.state(original));
            lease.release();
        } else try std.testing.expectError(switch (case) {
            .refused => error.AdmissionFull,
            .canceled => error.Canceled,
            .deadline => error.DeadlineExceeded,
            .shutdown => error.SchedulerClosed,
            .success => unreachable,
        }, result);
        try std.testing.expect(original == &root.entry.?);
        try std.testing.expectEqual(@as(?u64, 10), root.entry.?.options.deadline_ns);
        try std.testing.expect(root.entry.?.options.cancellation.?.ptr == @as(*const anyopaque, &signal));
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 77), scheduler.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), controller.stats().in_flight);
        if (case == .refused) try std.testing.expectEqual(State.running, scheduler.state(original));
        root.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "runnable input growth preserves original root and metadata partition through control promotion" {
    var owner = try Scheduler.init(.{ .slots = 2, .max_operations = 8, .max_bytes = 100, .control_bytes = 20 });
    var root: Execution = .{};
    try root.start(&owner, .{ .io = std.testing.io, .retained_bytes = 20 }, .query);
    defer root.finish();
    const original = &root.entry.?;
    const sequence = original.sequence;
    try root.growInputBytes(50);
    try std.testing.expect(original == &root.entry.?);
    try std.testing.expectEqual(sequence, original.sequence);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 50), owner.stats().retained_bytes);
    try root.park(.resource_blocked);
    try root.enter(.control);
    try std.testing.expectError(error.SchedulerFull, root.growInputBytes(81));
    try std.testing.expectEqual(@as(u64, 50), original.bytes);
    try root.growInputBytes(80);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().control_retained_bytes);
    try std.testing.expectEqual(@as(u64, 80), owner.stats().retained_bytes);
    try root.park(.output_blocked);
    var control: Execution = .{};
    try control.start(&owner, .{ .io = std.testing.io, .retained_bytes = 15 }, .control);
    defer control.finish();
    try std.testing.expectError(error.SchedulerFull, control.growInputBytes(21));
    try std.testing.expectEqual(@as(u64, 15), owner.stats().control_retained_bytes);
}

test "runnable input growth rejects shrinking and preserves original deadline cancellation and cleanup fences" {
    const Signal = struct {
        canceled: bool = false,
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).canceled;
        }
    };
    VirtualClock.value = 0;
    defer VirtualClock.value = 0;
    var signal: Signal = .{};
    var owner = try Scheduler.init(.{ .slots = 2 });
    var root: Execution = .{};
    try root.start(&owner, .{ .io = std.testing.io, .native_now_ns = VirtualClock.now, .deadline_ns = 100, .retained_bytes = 17, .cancellation = .{ .ptr = &signal, .requested = Signal.requested } }, .query);
    defer root.finish();
    try std.testing.expectError(error.InvalidRunnableDemand, root.growInputBytes(16));
    try root.growInputBytes(33);
    try std.testing.expectEqual(@as(?u64, 100), root.entry.?.options.deadline_ns);
    try std.testing.expect(root.entry.?.options.cancellation.?.ptr == @as(*const anyopaque, &signal));
    signal.canceled = true;
    try std.testing.expectError(error.Canceled, root.growInputBytes(34));
    signal.canceled = false;
    VirtualClock.value = 100;
    try std.testing.expectError(error.DeadlineExceeded, root.growInputBytes(34));
    VirtualClock.value = 0;
    try root.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = VirtualClock.now }, 100);
    try std.testing.expectError(error.InvalidRunnableTransition, root.growInputBytes(34));
    try std.testing.expectEqual(@as(u64, 33), owner.stats().retained_bytes);
}

test "runnable phase admission disabled owner preserves coarse lease behavior" {
    var controller = admission.Controller.init(1);
    const Acquire = struct {
        fn run(owner: *admission.Controller) !admission.Controller.Lease {
            return owner.tryAcquireLease() orelse error.AdmissionFull;
        }
    };
    var root: Execution = .{};
    var lease = try root.acquirePhaseLease(.query, Acquire.run, .{&controller});
    try std.testing.expectEqual(@as(usize, 1), controller.stats().in_flight);
    try std.testing.expect(root.entry == null);
    lease.release();
    try std.testing.expectEqual(@as(usize, 0), controller.stats().in_flight);
}

test "runnable scheduler checkpoint work budget and disabled execution" {
    var disabled: Execution = .{};
    try disabled.start(null, .{ .io = std.testing.io }, .query);
    try disabled.checkpoint();
    try disabled.park(.resource_blocked);
    try disabled.enter(.query);
    disabled.finish();
    try std.testing.expectEqual(@as(u64, 0), disabled.resume_generation);
    var s = try Scheduler.init(.{ .slots = 2, .checkpoint_budget = 2, .quantum_ms = 100 });
    var exec: Execution = .{};
    try exec.start(&s, .{ .io = std.testing.io }, .query);
    defer exec.finish();
    try exec.checkpoint();
    try exec.checkpoint();
    try std.testing.expectEqual(@as(u64, 1), s.stats().yields);
}

test "runnable scheduler reserves control metadata at full ordinary outstanding limits" {
    var s = try Scheduler.init(.{ .slots = 2, .max_operations = 3, .max_bytes = 30, .control_bytes = 10 });
    var first = testEntry(.query, 1, 10);
    var second = testEntry(.query, 1, 10);
    var control = testEntry(.control, 1, 10);
    try s.submit(&first);
    try s.block(&first, .output_blocked);
    try s.submit(&second);
    try s.block(&second, .resource_blocked);
    defer s.finish(&first);
    defer s.finish(&second);
    defer s.finish(&control);
    var excess = testEntry(.query, 1, 0);
    try std.testing.expectError(error.SchedulerFull, s.submit(&excess));
    try s.submit(&control);
    try s.wait(&control);
    try std.testing.expectEqual(@as(usize, 3), s.stats().operations);
    try std.testing.expectEqual(@as(u64, 30), s.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), s.stats().control_operations);
    // Promotion retains the read's original metadata reservation. It needs no
    // new control metadata, even though that partition is completely full.
    try s.reclassifyBlocked(&first, .control);
    try s.unblock(&first);
    s.finish(&control);
    try s.wait(&first);
    try std.testing.expectEqual(@as(usize, 0), s.stats().control_operations);
    try std.testing.expectEqual(@as(usize, 1), s.stats().control_slots);
}

test "runnable scheduler denied reentry retains borrowed reservations until owning finish" {
    const Cause = enum { canceled, deadline, wait_timeout };
    const Flag = struct {
        value: bool = false,
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).value;
        }
    };
    for (std.enums.values(Cause)) |cause| {
        VirtualClock.value = 0;
        var flag: Flag = .{};
        var scheduler = try Scheduler.init(.{ .slots = 3, .max_operations = 4, .max_wait_ms = 1 });
        defer scheduler.close();
        var borrowed: Execution = .{};
        defer borrowed.finish();
        try borrowed.startWithSlots(&scheduler, .{
            .io = std.testing.io,
            .native_now_ns = if (cause == .wait_timeout) null else VirtualClock.now,
            .deadline_ns = if (cause == .deadline) 10 else null,
            .cancellation = .{ .ptr = &flag, .requested = Flag.requested },
            .retained_bytes = 17,
        }, .inference, 2);
        try borrowed.park(.resource_blocked);
        var peer: Execution = .{};
        defer peer.finish();
        try peer.startWithSlots(&scheduler, .{ .io = std.testing.io }, .query, 2);
        if (cause == .canceled) flag.value = true;
        if (cause == .deadline) VirtualClock.value = 11;
        const expected = switch (cause) {
            .canceled => error.Canceled,
            .deadline => error.DeadlineExceeded,
            .wait_timeout => error.RunnableWaitTimeout,
        };
        try std.testing.expectError(expected, borrowed.enter(.inference));
        const held = scheduler.stats();
        try std.testing.expectEqual(State.stopped, scheduler.state(&borrowed.entry.?));
        try std.testing.expectEqual(@as(usize, 2), held.operations);
        try std.testing.expectEqual(@as(u64, 17), held.retained_bytes);
        try std.testing.expectEqual(@as(usize, 2), held.runnable_slots);
        try std.testing.expectEqual(@as(usize, 0), held.queued);
        // A detached or still-unreaped consumer cannot silently free its
        // outstanding operation reservation for a fresh foreground request.
        var excess = testEntry(.query, 1, 0);
        try std.testing.expectError(error.SchedulerFull, scheduler.submit(&excess));
        // A subsequently cleared cancellation token or shutdown cannot replace
        // the first failure or allow a new runnable interval on this capsule.
        flag.value = false;
        const generation = borrowed.resume_generation;
        try std.testing.expectError(expected, borrowed.checkpoint());
        try std.testing.expectError(expected, borrowed.park(.output_blocked));
        try std.testing.expectError(expected, borrowed.enter(.control));
        scheduler.close();
        try std.testing.expectError(expected, borrowed.checkpoint());
        try std.testing.expectError(expected, borrowed.park(.resource_blocked));
        try std.testing.expectError(expected, borrowed.enter(.inference));
        try std.testing.expectEqual(generation, borrowed.resume_generation);
        try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
        peer.finish();
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
        borrowed.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "runnable scheduler canceled yielded capsule retains ownership while peer computes" {
    var scheduler = try Scheduler.init(.{ .slots = 3, .max_operations = 4, .checkpoint_budget = 1 });
    defer scheduler.close();
    var peer = testEntry(.query, 2, 3);
    const CancelAfterYield = struct {
        scheduler: *Scheduler,
        peer: *Entry,
        fn requested(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.scheduler.state(self.peer) == .running;
        }
    };
    const cancellation = CancelAfterYield{ .scheduler = &scheduler, .peer = &peer };
    var borrowed: Execution = .{};
    defer borrowed.finish();
    try borrowed.startWithSlots(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17, .cancellation = .{ .ptr = &cancellation, .requested = CancelAfterYield.requested } }, .inference, 2);
    try scheduler.submit(&peer);
    defer scheduler.finish(&peer);
    try std.testing.expectEqual(State.queued, scheduler.state(&peer));
    try std.testing.expectError(error.Canceled, borrowed.checkpoint());
    try std.testing.expectEqual(State.running, scheduler.state(&peer));
    try std.testing.expectEqual(State.stopped, scheduler.state(&borrowed.entry.?));
    const held = scheduler.stats();
    try std.testing.expectEqual(@as(usize, 2), held.operations);
    try std.testing.expectEqual(@as(u64, 20), held.retained_bytes);
    try std.testing.expectEqual(@as(usize, 2), held.runnable_slots);
    try std.testing.expectEqual(@as(u64, 1), held.yields);
    borrowed.finish();
    try std.testing.expectEqual(@as(u64, 3), scheduler.stats().retained_bytes);
    scheduler.finish(&peer);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "runnable fail-fast initial control admission never waits or leaks a reservation" {
    var scheduler = try Scheduler.init(.{ .slots = 3, .max_operations = 8, .max_bytes = 128, .control_bytes = 64, .max_wait_ms = 1000 });
    var ordinary: Execution = .{};
    defer ordinary.finish();
    try ordinary.startWithSlots(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .query, 2);
    var first: Execution = .{};
    defer first.finish();
    try first.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 31, .allow_wait = false }, .control);
    var rejected: Execution = .{};
    defer rejected.finish();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, rejected.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 19, .allow_wait = false }, .control));
    try std.testing.expect(rejected.entry == null);
    const active = scheduler.stats();
    try std.testing.expectEqual(@as(usize, 2), active.operations);
    try std.testing.expectEqual(@as(u64, 48), active.retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), active.control_operations);
    try std.testing.expectEqual(@as(usize, 3), active.runnable_slots);
    try std.testing.expectEqual(@as(usize, 0), active.queued);
    first.finish();
    try rejected.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 19, .allow_wait = false }, .control);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().control_slots);
    rejected.finish();
    ordinary.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "runnable fail-fast acquisition retains fair bounded published reentry" {
    const io = std.testing.io;
    var scheduler = try Scheduler.init(.{ .slots = 3, .max_operations = 8, .max_wait_ms = 1000 });
    var first: Execution = .{};
    defer first.finish();
    try first.start(&scheduler, .{ .io = io, .retained_bytes = 31, .allow_wait = false }, .control);
    try first.park(.output_blocked);
    var peer: Execution = .{};
    defer peer.finish();
    try peer.start(&scheduler, .{ .io = io, .retained_bytes = 11, .allow_wait = false }, .control);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().control_operations);
    try std.testing.expectEqual(@as(u64, 42), scheduler.stats().retained_bytes);
    var release = try io.concurrent(struct {
        fn run(value: *Execution, lane: std.Io) !void {
            try lane.sleep(.fromMilliseconds(20), .awake);
            value.finish();
        }
    }.run, .{ &peer, io });
    defer release.cancel(io) catch {};
    try first.enter(.control);
    try release.await(io);
    try std.testing.expectEqual(@as(u64, 2), first.resume_generation);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().control_operations);
    try std.testing.expectEqual(@as(u64, 31), scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().control_slots);
    first.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "runnable scheduler running write retains completion ownership through cancellation and shutdown" {
    const Flag = struct {
        canceled: bool = false,
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).canceled;
        }
    };
    var flag: Flag = .{};
    var scheduler = try Scheduler.init(.{ .slots = 3, .max_operations = 4 });
    defer scheduler.close();
    var write: Execution = .{};
    defer write.finish();
    try write.startWithSlots(&scheduler, .{
        .io = std.testing.io,
        .retained_bytes = 4096,
        .cancellation = .{ .ptr = &flag, .requested = Flag.requested },
    }, .write, 2);
    const generation = write.resume_generation;
    // The native write has crossed its own acceptance fence. Until that owner
    // reports completion, foreground cancellation must not reclaim its grant.
    // This tests the scheduler side of that contract, not WAL/Raft acceptance.
    flag.canceled = true;
    try std.testing.expectError(error.Canceled, write.checkpoint());
    try std.testing.expectEqual(State.running, scheduler.state(&write.entry.?));
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 4096), scheduler.stats().retained_bytes);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
    try health.checkpoint();
    try std.testing.expectEqual(@as(usize, 3), scheduler.stats().runnable_slots);
    health.finish();
    scheduler.close();
    // Close stops admission and blocked reentry; live native execution is
    // joined by its caller and remains charged until that caller finishes.
    try std.testing.expectEqual(State.running, scheduler.state(&write.entry.?));
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 4096), scheduler.stats().retained_bytes);
    var fresh: Execution = .{};
    defer fresh.finish();
    try std.testing.expectError(error.SchedulerClosed, fresh.start(&scheduler, .{ .io = std.testing.io }, .write));
    try std.testing.expectEqual(generation, write.resume_generation);
    write.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "runnable scheduler team growth releases old CPU while preserving fair retained ownership" {
    var scheduler = try Scheduler.init(.{ .slots = 5, .max_operations = 8 });
    defer scheduler.close();
    var parent = testEntry(.write, 1, 19);
    defer scheduler.finish(&parent);
    var peer = testEntry(.query, 3, 11);
    defer scheduler.finish(&peer);
    try scheduler.submit(&parent);
    try scheduler.wait(&parent);
    try scheduler.submit(&peer);
    try scheduler.wait(&peer);
    try scheduler.resizeTeam(&parent, 4);
    try std.testing.expectEqual(State.queued, scheduler.state(&parent));
    try std.testing.expectEqual(@as(usize, 3), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 30), scheduler.stats().retained_bytes);
    var health = testEntry(.control, 1, 7);
    defer scheduler.finish(&health);
    try scheduler.submit(&health);
    try scheduler.wait(&health);
    try std.testing.expectEqual(@as(usize, 4), scheduler.stats().runnable_slots);
    scheduler.finish(&health);
    scheduler.finish(&peer);
    try scheduler.waitRetained(&parent);
    try std.testing.expectEqual(State.running, scheduler.state(&parent));
    try std.testing.expectEqual(@as(usize, 4), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 19), scheduler.stats().retained_bytes);
    try scheduler.resizeTeam(&parent, 1);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 19), scheduler.stats().retained_bytes);
}

test "runnable execution team reservations preserve original fencing and denied reentry ownership" {
    const Flag = struct {
        canceled: bool = false,
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).canceled;
        }
    };
    var flag: Flag = .{};
    var scheduler = try Scheduler.init(.{ .slots = 5, .max_operations = 8 });
    defer scheduler.close();
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 23, .cancellation = .{ .ptr = &flag, .requested = Flag.requested } }, .write);
    const before = execution.resume_generation;
    try execution.resizeTeam(4);
    try std.testing.expectEqual(before + 1, execution.resume_generation);
    try std.testing.expectEqual(@as(usize, 4), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    try std.testing.expectError(error.InvalidRunnableDemand, execution.resizeTeam(0));
    try std.testing.expectError(error.InvalidRunnableDemand, execution.resizeTeam(5));
    try std.testing.expectEqual(@as(usize, 4), execution.entry.?.slots);
    try execution.resizeTeam(1);
    var peer: Execution = .{};
    defer peer.finish();
    try peer.startWithSlots(&scheduler, .{ .io = std.testing.io }, .query, 3);
    // Split the queueing step from the wait to inject cancellation deterministically.
    try scheduler.resizeTeam(&execution.entry.?, 4);
    flag.canceled = true;
    try std.testing.expectError(error.Canceled, scheduler.waitRetained(&execution.entry.?));
    try std.testing.expectEqual(State.stopped, scheduler.state(&execution.entry.?));
    try std.testing.expectEqual(@as(usize, 3), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    flag.canceled = false;
    try std.testing.expectError(error.Canceled, execution.resizeTeam(1));
    execution.finish();
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "runnable scheduler team rejection and shutdown preserve existing running grant" {
    var scheduler = try Scheduler.init(.{ .slots = 4, .control_slots = 1, .max_operations = 8 });
    defer scheduler.close();
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 31 }, .write);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&scheduler, .{ .io = std.testing.io }, .control);
    try std.testing.expectError(error.InvalidRunnableDemand, health.resizeTeam(2));
    try std.testing.expectEqual(@as(usize, 1), health.entry.?.slots);
    health.finish();
    scheduler.close();
    try std.testing.expectError(error.SchedulerClosed, execution.resizeTeam(3));
    try std.testing.expectEqual(State.running, scheduler.state(&execution.entry.?));
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 31), scheduler.stats().retained_bytes);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "runnable execution team shrink does not reuse an expired prior queue wait" {
    VirtualClock.value = 0;
    var scheduler = try Scheduler.init(.{ .slots = 5, .max_operations = 8, .max_wait_ms = 1 });
    defer scheduler.close();
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = VirtualClock.now, .retained_bytes = 13 }, .write);
    var peer = testEntry(.query, 3, 0);
    defer scheduler.finish(&peer);
    try scheduler.submit(&peer);
    try scheduler.wait(&peer);
    try scheduler.resizeTeam(&execution.entry.?, 4);
    try std.testing.expectEqual(State.queued, scheduler.state(&execution.entry.?));
    scheduler.finish(&peer);
    try scheduler.waitRetained(&execution.entry.?);
    // The team has computed for longer than its old queue timeout. Giving CPU
    // back is not a new wait and must not stop an otherwise valid execution.
    VirtualClock.value = 2 * std.time.ns_per_ms;
    try execution.resizeTeam(1);
    try std.testing.expectEqual(State.running, scheduler.state(&execution.entry.?));
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 13), scheduler.stats().retained_bytes);
}

test "runnable execution completion reservation survives foreground cancellation and close" {
    const Clock = struct {
        var now_ns: u64 = 0;
        fn now() u64 {
            return now_ns;
        }
    };
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    Clock.now_ns = 0;
    var canceled = std.atomic.Value(bool).init(false);
    var owner = try Scheduler.init(.{ .slots = 4, .control_slots = 1, .checkpoint_budget = 1 });
    var execution: Execution = .{};
    defer execution.finish();
    try execution.startWithSlots(&owner, .{ .io = std.testing.io, .retained_bytes = 256, .deadline_ns = 5, .native_now_ns = Clock.now, .cancellation = .{ .ptr = &canceled, .requested = Cancel.requested } }, .write, 3);
    try execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = 100, .native_now_ns = Clock.now });
    try execution.activateCompletion();
    canceled.store(true, .release);
    Clock.now_ns = 10;
    try execution.park(.resource_blocked);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().resource_blocked);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 256), owner.stats().retained_bytes);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&owner, .{ .io = std.testing.io }, .control);
    health.finish();
    owner.close();
    try std.testing.expectEqual(State.resource_blocked, execution.entry.?.state);
    try execution.enter(.write);
    try execution.checkpoint();
    try std.testing.expectEqual(@as(usize, 3), owner.stats().runnable_slots);
    try execution.park(.output_blocked);
    try execution.enter(.write);
    try execution.resizeTeam(2);
    try std.testing.expectEqual(@as(usize, 2), owner.stats().runnable_slots);
    var fresh: Execution = .{};
    defer fresh.finish();
    try std.testing.expectError(error.SchedulerClosed, fresh.start(&owner, .{ .io = std.testing.io }, .write));
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
}

test "runnable scheduler completion queued before close retains fair bounded ownership" {
    var owner = try Scheduler.init(.{ .slots = 4, .control_slots = 1 });
    var completion: Execution = .{};
    defer completion.finish();
    try completion.startWithSlots(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .write, 3);
    const deadline = completion.entry.?.options.now() + 10 * std.time.ns_per_s;
    try completion.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline });
    try completion.activateCompletion();
    try completion.park(.resource_blocked);
    var blocker = testEntry(.query, 3, 11);
    defer owner.finish(&blocker);
    try owner.submit(&blocker);
    try owner.unblock(&completion.entry.?);
    try std.testing.expectEqual(State.queued, completion.entry.?.state);
    var unaccepted = testEntry(.write, 1, 5);
    defer owner.finish(&unaccepted);
    try owner.submit(&unaccepted);
    owner.close();
    try std.testing.expectEqual(State.queued, completion.entry.?.state);
    try std.testing.expectEqual(State.stopped, unaccepted.state);
    try std.testing.expectEqual(@as(usize, 3), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 33), owner.stats().retained_bytes);
    owner.finish(&blocker);
    try owner.waitRetained(&completion.entry.?);
    try std.testing.expectEqual(State.running, completion.entry.?.state);
    try std.testing.expectEqual(@as(usize, 3), owner.stats().runnable_slots);
    owner.finish(&unaccepted);
    completion.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "runnable execution completion requires prior bounded independent authorization" {
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    var flag = std.atomic.Value(bool).init(false);
    var owner = try Scheduler.init(.{ .slots = 4, .control_slots = 1 });
    var execution: Execution = .{};
    defer execution.finish();
    try std.testing.expectError(error.ExecutionWaitUnsupported, execution.reserveCompletion(.{ .io = std.testing.io }));
    try execution.start(&owner, .{ .io = std.testing.io, .retained_bytes = 32, .cancellation = .{ .ptr = &flag, .requested = Cancel.requested } }, .write);
    const deadline = execution.entry.?.options.now() + 10 * std.time.ns_per_s;
    try std.testing.expectError(error.InvalidRunnableTransition, execution.activateCompletion());
    try std.testing.expectError(error.InvalidArgument, execution.reserveCompletion(.{ .io = std.testing.io }));
    try std.testing.expectError(error.InvalidArgument, execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline, .allow_wait = false }));
    try std.testing.expectError(error.InvalidArgument, execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline, .cancellation = .{ .ptr = &flag, .requested = Cancel.requested } }));
    try std.testing.expectError(error.InvalidRunnableDemand, execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline, .retained_bytes = 33 }));
    try std.testing.expect(execution.entry.?.completion_options == null);
    try execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline });
    try std.testing.expectError(error.InvalidRunnableTransition, execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline }));
    flag.store(true, .release);
    try std.testing.expectError(error.Canceled, execution.activateCompletion());
    try std.testing.expect(!execution.entry.?.completion_active);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().runnable_slots);
    flag.store(false, .release);
    owner.close();
    try std.testing.expectError(error.SchedulerClosed, execution.activateCompletion());
    try std.testing.expect(!execution.entry.?.completion_active);
    execution.finish();
    var reader_owner = try Scheduler.init(.{});
    try execution.start(&reader_owner, .{ .io = std.testing.io }, .query);
    try std.testing.expectError(error.InvalidRunnableTransition, execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline }));
}

test "runnable execution expired completion retains consumers until joined finish" {
    const Clock = struct {
        var now_ns: u64 = 0;
        fn now() u64 {
            return now_ns;
        }
    };
    Clock.now_ns = 0;
    var owner = try Scheduler.init(.{ .slots = 3, .control_slots = 1 });
    var execution: Execution = .{};
    defer execution.finish();
    try execution.startWithSlots(&owner, .{ .io = std.testing.io, .retained_bytes = 64, .native_now_ns = Clock.now }, .write, 2);
    try execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = 100, .native_now_ns = Clock.now });
    try execution.activateCompletion();
    try std.testing.expectError(error.InvalidRunnableTransition, execution.activateCompletion());
    try execution.park(.resource_blocked);
    try std.testing.expectError(error.InvalidRunnableTransition, execution.enter(.control));
    try std.testing.expectEqual(State.resource_blocked, execution.entry.?.state);
    Clock.now_ns = 100;
    try std.testing.expectError(error.DeadlineExceeded, execution.enter(.write));
    try std.testing.expectEqual(State.stopped, execution.entry.?.state);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 64), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
    Clock.now_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, execution.checkpoint());
    try std.testing.expectError(error.DeadlineExceeded, execution.park(.resource_blocked));
    owner.close();
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "runnable scheduler closing foreground releases CPU to retained completion before finish" {
    for ([_]bool{ false, true }) |park| {
        var owner = try Scheduler.init(.{ .slots = 4, .control_slots = 1, .checkpoint_budget = 1 });
        var completion: Execution = .{};
        defer completion.finish();
        try completion.startWithSlots(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .write, 3);
        const deadline = completion.entry.?.options.now() + 10 * std.time.ns_per_s;
        try completion.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline });
        try completion.activateCompletion();
        try completion.park(.resource_blocked);
        var foreground: Execution = .{};
        defer foreground.finish();
        try foreground.startWithSlots(&owner, .{ .io = std.testing.io, .retained_bytes = 11 }, .query, 3);
        try owner.unblock(&completion.entry.?);
        try std.testing.expectEqual(State.queued, completion.entry.?.state);
        owner.close();
        if (park) {
            try std.testing.expectError(error.SchedulerClosed, foreground.park(.resource_blocked));
        } else {
            try std.testing.expectError(error.SchedulerClosed, foreground.checkpoint());
        }
        // The stopped foreground still owns consumers and retained bytes.
        // Its released CPU must dispatch completion before those joins finish.
        try std.testing.expectEqual(State.stopped, foreground.entry.?.state);
        try std.testing.expectEqual(State.running, completion.entry.?.state);
        try owner.waitRetained(&completion.entry.?);
        try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 28), owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 3), owner.stats().runnable_slots);
        foreground.finish();
        completion.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    }
}

test "runnable scheduler initial wait cannot retire an active completion consumer" {
    const Clock = struct {
        var now_ns: u64 = 0;
        fn now() u64 {
            return now_ns;
        }
    };
    Clock.now_ns = 0;
    var owner = try Scheduler.init(.{ .slots = 3, .control_slots = 1 });
    const consumer = try std.testing.allocator.alloc(u8, 64);
    defer std.testing.allocator.free(consumer);
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .retained_bytes = consumer.len }, .write);
    try execution.reserveCompletion(.{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100 });
    try execution.activateCompletion();
    try execution.park(.resource_blocked);
    try owner.unblock(&execution.entry.?);
    Clock.now_ns = 100;
    // A legacy caller using initial wait still cannot discard a published
    // completion's live buffers. Only a joined owner may finish the root.
    try std.testing.expectError(error.DeadlineExceeded, owner.wait(&execution.entry.?));
    try std.testing.expectEqual(State.stopped, execution.entry.?.state);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 64), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
    consumer[0] = 17;
    try std.testing.expectEqual(@as(u8, 17), consumer[0]);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "runnable execution independent completion task outlives actual foreground cancellation" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const Fixture = struct {
        io: std.Io,
        owner: *Scheduler,
        execution: Execution = .{},
        ready: std.Io.Event = .unset,
        completion_go: std.Io.Event = .unset,
        never: std.Io.Event = .unset,
        completion: ?std.Io.Future(anyerror!void) = null,
        transferred: std.atomic.Value(bool) = .init(false),

        fn foreground(self: *@This()) anyerror!void {
            try self.execution.start(self.owner, .{ .io = self.io, .retained_bytes = 256 }, .write);
            var transferred = false;
            defer if (!transferred) self.execution.finish();
            // Reserve the independent task/resources before entering a phase
            // which might accept durable work. It initially waits for handoff.
            self.completion = try self.io.concurrent(complete, .{self});
            defer if (!transferred) {
                _ = self.completion.?.cancel(self.io) catch {};
            };
            const deadline = self.execution.entry.?.options.now() + 10 * std.time.ns_per_s;
            try self.execution.reserveCompletion(.{ .io = self.io, .deadline_ns = deadline });
            try self.execution.activateCompletion();
            try self.execution.park(.resource_blocked);
            self.transferred.store(true, .release);
            transferred = true;
            self.ready.set(self.io);
            // This is a real task cancellation point, not a mocked boolean.
            try self.never.wait(self.io);
            return error.TestUnexpectedResult;
        }

        fn complete(self: *@This()) anyerror!void {
            // A cancellation before the handoff leaves finish to the parent;
            // afterwards this joined task owns finish even if its wait fails.
            defer if (self.transferred.load(.acquire)) self.execution.finish();
            try self.completion_go.wait(self.io);
            // The original task was canceled and joined; this task has its
            // own cancellation scope on the still-owned executor instance.
            try self.io.checkCancel();
            try self.execution.enter(.write);
            try self.execution.checkpoint();
            try std.testing.expectEqual(@as(usize, 1), self.owner.stats().operations);
            try std.testing.expectEqual(@as(u64, 256), self.owner.stats().retained_bytes);
            try std.testing.expectEqual(@as(usize, 1), self.owner.stats().runnable_slots);
        }
    };
    for ([_]bool{ false, true }) |cancel_completion| {
        var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
        defer threaded.deinit();
        const io = threaded.io();
        var owner = try Scheduler.init(.{ .slots = 3, .control_slots = 1, .checkpoint_budget = 1 });
        var fixture = Fixture{ .io = io, .owner = &owner };
        var foreground = try io.concurrent(Fixture.foreground, .{&fixture});
        defer {
            _ = foreground.cancel(io) catch {};
            fixture.completion_go.set(io);
            if (fixture.completion) |*completion| _ = completion.cancel(io) catch {};
        }
        try fixture.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
        var health: Execution = .{};
        defer health.finish();
        try health.start(&owner, .{ .io = io }, .control);
        health.finish();
        try std.testing.expectError(error.Canceled, foreground.cancel(io));
        owner.close();
        try std.testing.expectEqual(@as(usize, 1), owner.stats().resource_blocked);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        if (cancel_completion) {
            // No storage has been contacted in this fixture. The failed task must
            // join before releasing the handed-off root and retained metadata.
            try std.testing.expectError(error.Canceled, fixture.completion.?.cancel(io));
        } else {
            fixture.completion_go.set(io);
            try fixture.completion.?.await(io);
        }
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
    }
}

test "runnable scheduler cleanup reenters retained foreground states after cancellation and close" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    for ([_]State{ .running, .resource_blocked, .output_blocked, .queued, .stopped }) |before| {
        Clock.value = 0;
        var canceled = false;
        var owner = try Scheduler.init(.{ .slots = 2, .control_slots = 1, .checkpoint_budget = 1 });
        defer owner.close();
        var query: Execution = .{};
        defer query.finish();
        var blocker: Execution = .{};
        defer blocker.finish();
        try query.start(&owner, .{ .io = std.testing.io, .deadline_ns = 5, .native_now_ns = Clock.now, .retained_bytes = 32, .cancellation = .{ .ptr = &canceled, .requested = Cancel.requested } }, .query);
        try query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = 100, .native_now_ns = Clock.now });
        switch (before) {
            .running => {},
            .resource_blocked, .output_blocked => try query.park(before),
            .queued => {
                try query.park(.resource_blocked);
                try blocker.start(&owner, .{ .io = std.testing.io }, .query);
                try owner.unblock(&query.entry.?);
            },
            .stopped => owner.failWait(&query.entry.?, error.Canceled, true),
            else => unreachable,
        }
        try std.testing.expectEqual(before, query.entry.?.state);
        canceled = true;
        Clock.value = 10;
        owner.close();
        try owner.activateCleanup(&query.entry.?);
        try std.testing.expect(query.entry.?.cleanup_active);
        try std.testing.expect(!query.entry.?.completion_active);
        try std.testing.expectEqual(Class.query, query.entry.?.class);
        try std.testing.expectEqual(@as(u64, 32), owner.stats().retained_bytes);
        if (before == .queued) {
            // Cleanup does not steal the occupied CPU or enter control capacity.
            try std.testing.expectEqual(State.queued, query.entry.?.state);
            try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
            try std.testing.expectEqual(@as(usize, 1), owner.stats().runnable_slots);
        }
        blocker.finish();
        try owner.waitRetained(&query.entry.?);
        try query.checkpoint();
        try query.park(.resource_blocked);
        try std.testing.expectError(error.InvalidRunnableTransition, query.enter(.control));
        try query.enter(.query);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().control_slots - owner.stats().cleanup_runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 32), owner.stats().retained_bytes);
        query.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    }
}

test "runnable execution cleanup activation preserves bounded ownership after denied foreground reentry" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var owner = try Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer owner.close();
    var query: Execution = .{};
    defer query.finish();
    try query.start(&owner, .{ .io = std.testing.io, .deadline_ns = 5, .native_now_ns = Clock.now, .retained_bytes = 13 }, .query);
    try query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = 100, .native_now_ns = Clock.now });
    try query.park(.resource_blocked);
    Clock.value = 10;
    try std.testing.expectError(error.DeadlineExceeded, query.enter(.query));
    try std.testing.expectEqual(State.stopped, query.entry.?.state);
    try std.testing.expectError(error.DeadlineExceeded, query.checkpoint());
    const generation = query.resume_generation;
    try query.activateCleanup();
    try std.testing.expect(query.resume_generation != generation);
    try query.checkpoint();
    try std.testing.expectEqual(@as(u64, 13), owner.stats().retained_bytes);
    try std.testing.expectError(error.InvalidRunnableTransition, query.activateCleanup());
    Clock.value = 100;
    // Even the public initial-wait API cannot discard live cleanup consumers.
    try std.testing.expectError(error.DeadlineExceeded, owner.wait(&query.entry.?));
    try std.testing.expectEqual(State.stopped, query.entry.?.state);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 13), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
}

test "runnable execution cleanup requires prior independent finite authorization" {
    var owner = try Scheduler.init(.{});
    defer owner.close();
    var query: Execution = .{};
    defer query.finish();
    try std.testing.expectError(error.ExecutionWaitUnsupported, query.activateCleanup());
    try query.start(&owner, .{ .io = std.testing.io, .retained_bytes = 8 }, .query);
    const deadline = query.entry.?.options.now() + 10 * std.time.ns_per_s;
    try std.testing.expectError(error.InvalidRunnableTransition, query.activateCleanup());
    try std.testing.expectError(error.InvalidArgument, query.reserveCleanup(.{ .io = std.testing.io }));
    try std.testing.expectError(error.InvalidArgument, query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = deadline, .allow_wait = false }));
    try std.testing.expectError(error.InvalidRunnableDemand, query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = deadline, .retained_bytes = 9 }));
    try std.testing.expect(query.entry.?.cleanup_options == null);
    try query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = deadline });
    try std.testing.expectEqual(@as(u64, 8), query.entry.?.cleanup_options.?.retained_bytes);
    try std.testing.expectError(error.InvalidRunnableTransition, query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = deadline }));
    try query.activateCleanup();
    try std.testing.expectError(error.InvalidRunnableTransition, query.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline }));
}

test "runnable scheduler cleanup cannot replace active durable completion or authorize expired cleanup" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var owner = try Scheduler.init(.{});
    defer owner.close();
    var write: Execution = .{};
    defer write.finish();
    try write.start(&owner, .{ .io = std.testing.io, .retained_bytes = 8 }, .write);
    try write.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = 5, .native_now_ns = Clock.now });
    try write.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = 5, .native_now_ns = Clock.now });
    try write.activateCompletion();
    try std.testing.expectError(error.InvalidRunnableTransition, write.activateCleanup());
    try std.testing.expect(write.entry.?.completion_active);
    var query: Execution = .{};
    defer query.finish();
    try query.start(&owner, .{ .io = std.testing.io, .retained_bytes = 13 }, .query);
    try query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = 5, .native_now_ns = Clock.now });
    Clock.value = 5;
    owner.close();
    try std.testing.expectError(error.DeadlineExceeded, query.activateCleanup());
    try std.testing.expect(!query.entry.?.cleanup_active);
    try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 21), owner.stats().retained_bytes);
}

test "runnable scheduler cleanup consumes unused write completion and times out fairly under saturation" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    for ([_]bool{ false, true }) |completion_first| {
        Clock.value = 0;
        var owner = try Scheduler.init(.{ .slots = 2, .control_slots = 1, .max_wait_ms = 1 });
        defer owner.close();
        var write: Execution = .{};
        defer write.finish();
        const options = admission.Options{ .io = std.testing.io, .deadline_ns = 10 * std.time.ns_per_s, .native_now_ns = Clock.now };
        try write.start(&owner, .{ .io = std.testing.io, .deadline_ns = 5, .retained_bytes = 13, .native_now_ns = Clock.now }, .write);
        if (completion_first) {
            try write.reserveCompletion(options);
            try write.reserveCleanup(options);
        } else {
            try write.reserveCleanup(options);
            try write.reserveCompletion(options);
        }
        try write.park(.resource_blocked);
        var blocker: Execution = .{};
        defer blocker.finish();
        try blocker.start(&owner, .{ .io = std.testing.io }, .query);
        Clock.value = 10;
        try owner.activateCleanup(&write.entry.?);
        try std.testing.expect(write.entry.?.completion_options == null);
        try std.testing.expect(!write.entry.?.completion_active);
        try std.testing.expectEqual(State.queued, write.entry.?.state);
        var control: Execution = .{};
        defer control.finish();
        try control.start(&owner, .{ .io = std.testing.io }, .control);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().control_slots);
        Clock.value = 10 + std.time.ns_per_ms;
        try std.testing.expectError(error.RunnableWaitTimeout, owner.wait(&write.entry.?));
        try std.testing.expectEqual(State.stopped, write.entry.?.state);
        try std.testing.expectEqual(@as(usize, 3), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 13), owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 2), owner.stats().runnable_slots);
        try std.testing.expectError(error.RunnableWaitTimeout, write.activateCompletion());
        blocker.finish();
        control.finish();
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 13), owner.stats().retained_bytes);
    }
}

test "runnable scheduler cleanup activation budget survives idle expiry and cannot renew" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    const Cancel = struct {
        requested: bool = false,
        fn check(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.requested;
        }
    };
    Clock.value = 0;
    var cancel: Cancel = .{};
    var owner = try Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer owner.close();
    var query: Execution = .{};
    defer query.finish();
    try query.start(&owner, .{ .io = std.testing.io, .deadline_ns = 5, .native_now_ns = Clock.now, .retained_bytes = 11, .cancellation = .{ .ptr = &cancel, .requested = Cancel.check } }, .query);
    try query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 10);
    try query.park(.output_blocked);
    Clock.value = 500;
    cancel.requested = true;
    owner.close();
    try query.activateCleanup();
    try std.testing.expectEqual(Class.query, query.entry.?.class);
    try std.testing.expectEqual(@as(?u64, 510), query.entry.?.options.deadline_ns);
    try std.testing.expect(query.entry.?.cleanup_duration_ns == null);
    try std.testing.expectError(error.InvalidRunnableTransition, query.enter(.control));
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 11), owner.stats().retained_bytes);
    Clock.value = 509;
    try query.checkpoint();
    Clock.value = 510;
    try std.testing.expectError(error.DeadlineExceeded, query.checkpoint());
    try std.testing.expectError(error.DeadlineExceeded, query.activateCleanup());
    try std.testing.expectEqual(@as(?u64, 510), query.entry.?.options.deadline_ns);
    // Deadline failure retains the operation and all bytes until physical
    // resource owners have joined and the owning caller explicitly finishes.
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 11), owner.stats().retained_bytes);
    query.finish();
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "runnable scheduler cleanup activation budget includes fair CPU wait and preserves control" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var owner = try Scheduler.init(.{ .slots = 2, .control_slots = 1, .max_wait_ms = 1000 });
    defer owner.close();
    var query: Execution = .{};
    defer query.finish();
    try query.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .retained_bytes = 13 }, .query);
    try query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 10);
    try query.park(.resource_blocked);
    var blocker: Execution = .{};
    defer blocker.finish();
    try blocker.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .write);
    Clock.value = 500;
    try owner.activateCleanup(&query.entry.?);
    try std.testing.expectEqual(State.queued, query.entry.?.state);
    try std.testing.expectEqual(@as(?u64, 510), query.entry.?.options.deadline_ns);
    var control: Execution = .{};
    defer control.finish();
    try control.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .control);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().control_slots);
    Clock.value = 510;
    try std.testing.expectError(error.DeadlineExceeded, owner.waitRetained(&query.entry.?));
    try std.testing.expectEqual(State.stopped, query.entry.?.state);
    try std.testing.expectEqual(@as(usize, 3), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 13), owner.stats().retained_bytes);
    blocker.finish();
    control.finish();
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 13), owner.stats().retained_bytes);
}

test "runnable scheduler cleanup activation budget rejects invalid and overflowing authority" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var owner = try Scheduler.init(.{});
    defer owner.close();
    var query: Execution = .{};
    defer query.finish();
    try query.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .retained_bytes = 17 }, .query);
    try std.testing.expectError(error.InvalidArgument, query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 0));
    try std.testing.expectError(error.InvalidArgument, query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100 }, 10));
    try std.testing.expectError(error.InvalidArgument, query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now, .allow_wait = false }, 10));
    try std.testing.expectError(error.InvalidRunnableDemand, query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now, .retained_bytes = 18 }, 10));
    Clock.value = std.math.maxInt(u64) - 5;
    try std.testing.expectError(error.InvalidArgument, query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 10));
    try std.testing.expect(query.entry.?.cleanup_options == null);
    Clock.value = 0;
    try query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 10);
    try std.testing.expectError(error.InvalidRunnableTransition, query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 20));
    Clock.value = std.math.maxInt(u64) - 5;
    try std.testing.expectError(error.InvalidArgument, query.activateCleanup());
    try std.testing.expect(!query.entry.?.cleanup_active);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 17), owner.stats().retained_bytes);
}

test "runnable scheduler cleanup activation budget cannot replace active durable completion" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var owner = try Scheduler.init(.{});
    defer owner.close();
    var write: Execution = .{};
    defer write.finish();
    try write.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .retained_bytes = 19 }, .write);
    try write.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = 1000, .native_now_ns = Clock.now });
    try write.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 10);
    try write.activateCompletion();
    Clock.value = 500;
    try std.testing.expectError(error.InvalidRunnableTransition, write.activateCleanup());
    try std.testing.expect(write.entry.?.completion_active);
    try std.testing.expect(!write.entry.?.cleanup_active);
    try std.testing.expectEqual(@as(?u64, 1000), write.entry.?.options.deadline_ns);
    try std.testing.expectEqual(@as(u64, 19), owner.stats().retained_bytes);
}

test "runnable scheduler children independently park yield and retain their parent through nested finish" {
    var owner = try Scheduler.init(.{ .slots = 5, .checkpoint_budget = 1 });
    defer owner.close();
    var parent: Execution = .{};
    defer parent.finish();
    try parent.start(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
    var first: Execution = .{};
    defer first.finish();
    try first.startChild(&parent, std.testing.io, @sizeOf(Execution));
    var second: Execution = .{};
    defer second.finish();
    try second.startChild(&parent, std.testing.io, @sizeOf(Execution));
    var nested: Execution = .{};
    defer nested.finish();
    try nested.startChild(&first, std.testing.io, @sizeOf(Execution));
    try std.testing.expectEqual(@as(usize, 4), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 3), owner.stats().child_operations);
    try std.testing.expectEqual(@as(usize, 2), parent.entry.?.children);
    try std.testing.expectEqual(@as(usize, 1), first.entry.?.children);
    try parent.park(.resource_blocked);
    try first.park(.output_blocked);
    const parent_generation = parent.resume_generation;
    try second.checkpoint();
    try nested.checkpoint();
    try std.testing.expectEqual(parent_generation, parent.resume_generation);
    try std.testing.expectEqual(@as(usize, 2), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 4), owner.stats().operations);
    try std.testing.expectError(error.InvalidRunnableTransition, owner.reclassifyBlocked(&parent.entry.?, .control));
    try parent.enter(.query);
    try first.enter(.query);
    try std.testing.expectError(error.InvalidRunnableTransition, parent.resizeTeam(2));
    nested.finish();
    first.finish();
    try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
    second.finish();
    try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
    parent.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().child_operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "runnable scheduler children share bounded reservation and leave control capacity available" {
    var owner = try Scheduler.init(.{ .slots = 4, .max_operations = 6, .max_bytes = 128 * 1024 });
    defer owner.close();
    var parent: Execution = .{};
    defer parent.finish();
    try parent.start(&owner, .{ .io = std.testing.io, .retained_bytes = 19 }, .inference);
    var first: Execution = .{};
    defer first.finish();
    var second: Execution = .{};
    defer second.finish();
    try first.startChild(&parent, std.testing.io, @sizeOf(Execution));
    try second.startChild(&parent, std.testing.io, @sizeOf(Execution));
    try first.park(.resource_blocked);
    try second.park(.output_blocked);
    const retained = owner.stats().retained_bytes;
    var rejected: Execution = .{};
    defer rejected.finish();
    try std.testing.expectError(error.SchedulerFull, rejected.startChild(&parent, std.testing.io, @sizeOf(Execution)));
    try std.testing.expect(rejected.entry == null);
    try std.testing.expectEqual(@as(usize, 2), parent.entry.?.children);
    try std.testing.expectEqual(retained, owner.stats().retained_bytes);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&owner, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 1 }, .control);
    try health.checkpoint();
    try std.testing.expectEqual(@as(usize, 1), owner.stats().control_slots);
    owner.close();
    try std.testing.expectError(error.SchedulerClosed, first.enter(.inference));
    try std.testing.expectEqual(retained + 1, owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 2), parent.entry.?.children);
    second.finish();
    first.finish();
    parent.finish();
    health.finish();
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "runnable scheduler children inherit original deadline cancellation and stopped ancestor cause" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.RunnableWaitTimeout }) |failure| {
        Clock.value = 0;
        var canceled = false;
        var owner = try Scheduler.init(.{ .slots = 4, .checkpoint_budget = 1 });
        defer owner.close();
        var parent: Execution = .{};
        defer parent.finish();
        try parent.start(&owner, .{ .io = std.testing.io, .retained_bytes = 23, .deadline_ns = 100, .native_now_ns = Clock.now, .cancellation = .{ .ptr = &canceled, .requested = Cancel.requested } }, .query);
        var child: Execution = .{};
        defer child.finish();
        try child.startChild(&parent, std.testing.io, @sizeOf(Execution));
        try std.testing.expectEqual(parent.entry.?.options.deadline_ns, child.entry.?.options.deadline_ns);
        const retained = owner.stats().retained_bytes;
        try child.park(.resource_blocked);
        if (failure == error.Canceled) canceled = true else if (failure == error.DeadlineExceeded) Clock.value = 100 else owner.failWait(&parent.entry.?, failure, true);
        try std.testing.expectError(failure, child.enter(.query));
        try std.testing.expectEqual(State.stopped, child.entry.?.state);
        try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
        try std.testing.expectEqual(retained, owner.stats().retained_bytes);
        try std.testing.expectError(failure, child.checkpoint());
        child.finish();
        try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
    }
}

test "runnable scheduler children do not inherit inactive completion cleanup or capacity promotion" {
    var owner = try Scheduler.init(.{ .slots = 4 });
    defer owner.close();
    var parent: Execution = .{};
    defer parent.finish();
    const until = @as(u64, @intCast(std.Io.Clock.awake.now(std.testing.io).nanoseconds)) + 10 * std.time.ns_per_s;
    try parent.start(&owner, .{ .io = std.testing.io, .retained_bytes = 32 }, .write);
    try parent.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = until });
    try parent.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = until });
    var child: Execution = .{};
    defer child.finish();
    try child.startChild(&parent, std.testing.io, @sizeOf(Execution));
    try std.testing.expect(child.entry.?.completion_options == null);
    try std.testing.expect(child.entry.?.cleanup_options == null);
    try std.testing.expectError(error.InvalidRunnableTransition, parent.activateCompletion());
    try std.testing.expectError(error.InvalidRunnableTransition, parent.activateCleanup());
    try std.testing.expectError(error.InvalidRunnableTransition, child.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = until }));
    try std.testing.expectError(error.InvalidRunnableTransition, child.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = until }));
    try child.park(.resource_blocked);
    try std.testing.expectError(error.InvalidRunnableTransition, child.enter(.control));
    child.finish();
    try parent.activateCompletion();
    try child.startChild(&parent, std.testing.io, @sizeOf(Execution));
    try std.testing.expect(child.entry.?.completion_active);
    try std.testing.expect(child.entry.?.cleanup_options == null);
    try std.testing.expectError(error.InvalidRunnableTransition, child.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = until + 1 }));
    try std.testing.expectError(error.InvalidRunnableTransition, child.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = until + 1 }));
    child.finish();
    var cleanup: Execution = .{};
    defer cleanup.finish();
    try cleanup.start(&owner, .{ .io = std.testing.io }, .write);
    try cleanup.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = until });
    try cleanup.activateCleanup();
    var denied: Execution = .{};
    defer denied.finish();
    try std.testing.expectError(error.InvalidRunnableTransition, denied.startChild(&cleanup, std.testing.io, @sizeOf(Execution)));
    try std.testing.expect(denied.entry == null);
}

test "runnable scheduler children reject malformed reuse before changing existing authority" {
    var owner = try Scheduler.init(.{ .slots = 4 });
    defer owner.close();
    var root = testEntry(.query, 1, 37);
    defer owner.finish(&root);
    try owner.submit(&root);
    try owner.wait(&root);
    try std.testing.expectError(error.InvalidRunnableTransition, owner.submitChild(&root, &root, std.testing.io));
    try std.testing.expectEqual(@as(u64, 37), root.bytes);
    try std.testing.expectEqual(Class.query, root.class);
    try std.testing.expect(root.parent == null);
    var existing = testEntry(.inference, 1, @sizeOf(Entry));
    defer owner.finish(&existing);
    try owner.submit(&existing);
    try owner.wait(&existing);
    try std.testing.expectError(error.InvalidRunnableTransition, owner.submitChild(&root, &existing, std.testing.io));
    try std.testing.expectEqual(Class.inference, existing.class);
    try std.testing.expect(existing.parent == null);
    try std.testing.expectEqual(@as(usize, 0), root.children);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().child_operations);
}

test "runnable scheduler child fairness preserves an older independent root ahead of later fanout" {
    var owner = try Scheduler.init(.{ .slots = 3 });
    defer owner.close();
    var parent = testEntry(.query, 1, 31);
    defer owner.finish(&parent);
    try owner.submit(&parent);
    try owner.wait(&parent);
    var first = testEntry(.query, 1, @sizeOf(Entry));
    defer owner.finish(&first);
    try owner.submitChild(&parent, &first, std.testing.io);
    try owner.wait(&first);
    var independent = testEntry(.query, 1, 41);
    defer owner.finish(&independent);
    try owner.submit(&independent);
    var later = testEntry(.query, 1, @sizeOf(Entry));
    defer owner.finish(&later);
    try owner.submitChild(&parent, &later, std.testing.io);
    try std.testing.expectEqual(State.queued, owner.state(&independent));
    try std.testing.expectEqual(State.queued, owner.state(&later));
    try owner.block(&first, .resource_blocked);
    try owner.wait(&independent);
    try std.testing.expectEqual(State.queued, owner.state(&later));
    try std.testing.expectEqual(@as(usize, 2), parent.children);
    owner.finish(&independent);
    try owner.wait(&later);
    owner.finish(&later);
    owner.finish(&first);
    try std.testing.expectEqual(@as(usize, 0), parent.children);
}

test "runnable scheduler child physical worker join retains original parent and bytes after cancellation" {
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const Worker = struct {
        child: *Execution,
        body: []u8,
        parked: std.Io.Event = .unset,
        go: std.Io.Event = .unset,
        freed: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) !void {
            defer {
                std.testing.allocator.free(self.body);
                self.freed.store(true, .release);
            }
            try self.child.park(.resource_blocked);
            self.parked.set(std.testing.io);
            self.go.waitUncancelable(std.testing.io);
            try self.child.enter(.query);
        }
    };
    var canceled: std.atomic.Value(bool) = .init(false);
    var owner = try Scheduler.init(.{ .slots = 3, .checkpoint_budget = 1 });
    defer owner.close();
    var parent: Execution = .{};
    defer parent.finish();
    try parent.start(&owner, .{ .io = std.testing.io, .retained_bytes = 43, .cancellation = .{ .ptr = &canceled, .requested = Cancel.requested } }, .query);
    var child: Execution = .{};
    defer child.finish();
    const child_bytes = @sizeOf(Execution) + 64;
    try child.startChild(&parent, std.testing.io, child_bytes);
    const body = try std.testing.allocator.alloc(u8, 64);
    var body_published = false;
    defer if (!body_published) std.testing.allocator.free(body);
    var worker = Worker{ .child = &child, .body = body };
    var future = try std.testing.io.concurrent(Worker.run, .{&worker});
    body_published = true;
    defer {
        worker.go.set(std.testing.io);
        future.await(std.testing.io) catch {};
    }
    try worker.parked.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try parent.checkpoint();
    canceled.store(true, .release);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&owner, .{ .io = std.testing.io, .allow_wait = false }, .control);
    try health.checkpoint();
    try std.testing.expect(!worker.freed.load(.acquire));
    try std.testing.expectEqual(@as(u64, 43 + child_bytes), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
    worker.go.set(std.testing.io);
    try std.testing.expectError(error.Canceled, future.await(std.testing.io));
    try std.testing.expect(worker.freed.load(.acquire));
    // Physical completion is distinct from owning metadata retirement.
    try std.testing.expectEqual(@as(u64, 43 + child_bytes), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().child_operations);
    child.finish();
    try std.testing.expectEqual(@as(u64, 43), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
    health.finish();
    parent.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "runnable scheduler child observes original parent worker IO cancellation without a separate token" {
    const Context = struct {
        var cancelled: bool = false;
        fn check(_: ?*anyopaque) std.Io.Cancelable!void {
            if (cancelled) return error.Canceled;
        }
    };
    var vtable = std.testing.io.vtable.*;
    vtable.checkCancel = Context.check;
    const parent_io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    Context.cancelled = false;
    var owner = try Scheduler.init(.{ .slots = 3 });
    defer owner.close();
    var parent: Execution = .{};
    defer parent.finish();
    try parent.start(&owner, .{ .io = parent_io, .retained_bytes = 13 }, .query);
    var child: Execution = .{};
    defer child.finish();
    try child.startChild(&parent, std.testing.io, @sizeOf(Execution));
    Context.cancelled = true;
    try std.testing.expectError(error.Canceled, parent.checkpoint());
    try std.testing.expectEqual(State.stopped, parent.entry.?.state);
    try std.testing.expectError(error.Canceled, child.checkpoint());
    try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
    child.finish();
    parent.finish();
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "runnable scheduler managed cleanup capacity rejects insufficient metadata without changing native policy" {
    for ([_]usize{ 2, 3 }) |operations| {
        for ([_]usize{ 0, 1 }) |cleanup| {
            var owner = try Scheduler.init(.{ .slots = 3, .cleanup_slots = cleanup, .max_operations = operations });
            defer owner.close();
            const before = owner.policyAndStats();
            try std.testing.expectError(error.ManagerCleanupCapacityUnavailable, owner.ensureCleanupCpuBeforePublication());
            try std.testing.expectEqualDeep(before, owner.policyAndStats());
        }
    }
    var standalone = try Scheduler.init(.{ .slots = 2, .max_operations = 2 });
    defer standalone.close();
    try std.testing.expectError(error.ManagerCleanupCapacityUnavailable, standalone.ensureCleanupCpuBeforePublication());
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&standalone, .{ .io = std.testing.io, .retained_bytes = 1 }, .query);
    try std.testing.expectEqual(@as(usize, 1), standalone.stats().runnable_slots);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), standalone.stats().operations);
    var managed = try Scheduler.init(.{ .slots = 3, .max_operations = 4 });
    defer managed.close();
    try managed.ensureCleanupCpuBeforePublication();
    const policy = managed.policyAndStats().config;
    try std.testing.expectEqual(@as(usize, 3), policy.slots);
    try std.testing.expectEqual(@as(usize, 1), policy.ordinarySlots());
    try std.testing.expectEqual(@as(usize, 1), policy.cleanup_slots);
}

test "runnable scheduler completion child inherits finite authority through close and retirement" {
    const std_test = @import("std");
    const Clock = struct {
        var foreground: u64 = 0;
        var completion: u64 = 0;
        fn original() u64 {
            return foreground;
        }
        fn retained() u64 {
            return completion;
        }
        fn canceled(raw: *const anyopaque) bool {
            const flag: *const std_test.atomic.Value(bool) = @ptrCast(@alignCast(raw));
            return flag.load(.acquire);
        }
    };
    for ([_]bool{ false, true }) |close_before_child| {
        Clock.foreground = 0;
        Clock.completion = 0;
        var scheduler = try Scheduler.init(.{ .slots = 3, .control_slots = 1 });
        defer scheduler.close();
        var parent: Execution = .{};
        defer parent.finish();
        var child: Execution = .{};
        defer child.finish();
        var flag = std_test.atomic.Value(bool).init(false);
        try parent.start(&scheduler, .{ .io = std_test.testing.io, .native_now_ns = Clock.original, .deadline_ns = 5, .retained_bytes = 256, .cancellation = .{ .ptr = &flag, .requested = Clock.canceled } }, .write);
        try parent.reserveCompletion(.{ .io = std_test.testing.io, .native_now_ns = Clock.retained, .deadline_ns = 20 });
        try parent.activateCompletion();
        Clock.foreground = 100;
        flag.store(true, .release);
        if (close_before_child) scheduler.close();
        try child.reserveChild(&parent, std_test.testing.io, @sizeOf(Execution));
        try std_test.testing.expect(child.entry.?.completion_active);
        try std_test.testing.expectEqual(@as(?u64, 20), child.entry.?.options.deadline_ns);
        try std_test.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
        try std_test.testing.expectEqual(@as(u64, 256 + @sizeOf(Execution)), scheduler.stats().retained_bytes);
        try child.activateChild();
        try child.checkLifetime();
        try child.park(.resource_blocked);
        try std_test.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
        try child.enter(.write);
        try std_test.testing.expectError(error.InvalidRunnableTransition, child.enter(.query));
        if (close_before_child) {
            var fresh: Execution = .{};
            defer fresh.finish();
            try std_test.testing.expectError(error.SchedulerClosed, fresh.start(&scheduler, .{ .io = std_test.testing.io }, .query));
        }
        Clock.completion = 20;
        try std_test.testing.expectError(error.DeadlineExceeded, child.checkLifetime());
        child.finish();
        try std_test.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
        parent.finish();
        try std_test.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std_test.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std_test.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "runnable scheduler completion child remains bounded by original operation and byte partitions" {
    const std_test = @import("std");
    const Clock = struct {
        fn now() u64 {
            return 0;
        }
    };
    for ([_]bool{ false, true }) |byte_limit| {
        var scheduler = try Scheduler.init(.{ .slots = 3, .control_slots = 1, .max_operations = if (byte_limit) 16 else 4, .max_bytes = 8192, .control_bytes = 4096 });
        defer scheduler.close();
        var parent: Execution = .{};
        defer parent.finish();
        var first: Execution = .{};
        defer first.finish();
        var denied: Execution = .{};
        defer denied.finish();
        try parent.start(&scheduler, .{ .io = std_test.testing.io, .native_now_ns = Clock.now, .retained_bytes = 256 }, .write);
        try parent.reserveCompletion(.{ .io = std_test.testing.io, .native_now_ns = Clock.now, .deadline_ns = 20 });
        try parent.activateCompletion();
        scheduler.close();
        try first.reserveChild(&parent, std_test.testing.io, @sizeOf(Execution));
        const before = scheduler.stats();
        try std_test.testing.expectError(error.SchedulerFull, denied.reserveChild(&parent, std_test.testing.io, if (byte_limit) 4096 else @sizeOf(Execution)));
        const after = scheduler.stats();
        try std_test.testing.expectEqual(before.operations, after.operations);
        try std_test.testing.expectEqual(before.retained_bytes, after.retained_bytes);
        try std_test.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
        try std_test.testing.expect(denied.entry == null);
        first.finish();
        parent.finish();
        try std_test.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std_test.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "runnable scheduler completion child preserves implicit parent clock on independent worker Io" {
    const std_test = @import("std");
    const Clock = struct {
        var current: i96 = 10;
        fn now(_: ?*anyopaque, _: std_test.Io.Clock) std_test.Io.Timestamp {
            return .{ .nanoseconds = current };
        }
    };
    var vtable = std_test.testing.io.vtable.*;
    vtable.now = Clock.now;
    const original_io: std_test.Io = .{ .userdata = std_test.testing.io.userdata, .vtable = &vtable };
    for ([_]bool{ false, true }) |completing| {
        Clock.current = 10;
        var scheduler = try Scheduler.init(.{ .slots = 3, .control_slots = 1 });
        defer scheduler.close();
        var parent: Execution = .{};
        defer parent.finish();
        var child: Execution = .{};
        defer child.finish();
        try parent.start(&scheduler, .{ .io = original_io, .deadline_ns = 20, .retained_bytes = 256 }, .write);
        if (completing) {
            try parent.reserveCompletion(.{ .io = original_io, .deadline_ns = 30 });
            try parent.activateCompletion();
        }
        try child.reserveChild(&parent, std_test.testing.io, @sizeOf(Execution));
        try child.activateChild();
        try child.checkLifetime();
        try std_test.testing.expectEqual(@as(u64, 10), child.entry.?.options.now());
        try std_test.testing.expectEqual(@as(u64, @sizeOf(Execution)), child.entry.?.options.retained_bytes);
        if (completing) {
            try std_test.testing.expectEqual(@as(u64, 20), try child.completionRemainingNs());
            try std_test.testing.expectEqual(@as(u64, @sizeOf(Execution)), child.entry.?.completion_options.?.retained_bytes);
        }
        Clock.current = if (completing) 30 else 20;
        try std_test.testing.expectError(error.DeadlineExceeded, child.checkLifetime());
        child.finish();
        parent.finish();
        try std_test.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std_test.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "runnable scheduler reserved child packets hold no CPU demand before physical activation" {
    var scheduler = try Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var parent: Execution = .{};
    defer parent.finish();
    try parent.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 256 }, .write);
    var unused: Execution = .{};
    defer unused.finish();
    var active: Execution = .{};
    defer active.finish();
    try unused.reserveChild(&parent, std.testing.io, @sizeOf(Execution));
    try active.reserveChild(&parent, std.testing.io, @sizeOf(Execution));
    const bytes = scheduler.stats().retained_bytes;
    try std.testing.expectEqual(@as(usize, 3), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().queued);
    try std.testing.expectEqual(State.resource_blocked, scheduler.state(&unused.entry.?));
    try parent.park(.resource_blocked);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try active.activateChild();
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(State.resource_blocked, scheduler.state(&unused.entry.?));
    try active.park(.resource_blocked);
    try parent.enter(.write);
    try std.testing.expectEqual(bytes, scheduler.stats().retained_bytes);
    active.finish();
    unused.finish();
    try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 256), scheduler.stats().retained_bytes);
}

test "runnable scheduler retained serial cleanup progresses with saturated ordinary CPU" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    for (0..3) |cause| {
        Clock.value = 0;
        var canceled = false;
        var owner = try Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1, .max_operations = 8 });
        defer owner.close();
        var original: Execution = .{};
        defer original.finish();
        try original.start(&owner, .{ .io = std.testing.io, .retained_bytes = 17, .deadline_ns = 5, .native_now_ns = Clock.now, .cancellation = .{ .ptr = &canceled, .requested = Cancel.requested } }, .write);
        try original.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = 100, .native_now_ns = Clock.now });
        try original.park(.resource_blocked);
        var holder: Execution = .{};
        defer holder.finish();
        try holder.start(&owner, .{ .io = std.testing.io, .retained_bytes = 23 }, .query);
        if (cause == 0) canceled = true else if (cause == 1) Clock.value = 10 else owner.close();
        const rejection: anyerror = if (cause == 0) error.Canceled else if (cause == 1) error.DeadlineExceeded else error.SchedulerClosed;
        try std.testing.expectError(rejection, original.enter(.write));
        try original.activateCleanup();
        try std.testing.expectEqual(Class.write, try original.retainedClass());
        try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 40), owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().cleanup_runnable_slots);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().control_slots - owner.stats().cleanup_runnable_slots);
        try std.testing.expectEqual(State.running, holder.entry.?.state);
        try std.testing.expectError(error.InvalidRunnableDemand, original.resizeTeam(2));
        try original.checkpoint();
        if (cause != 2) {
            var health: Execution = .{};
            try health.start(&owner, .{ .io = std.testing.io, .allow_wait = false }, .control);
            try std.testing.expectEqual(@as(usize, 3), owner.stats().runnable_slots);
            health.finish();
        }
        original.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().cleanup_runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().runnable_slots);
        holder.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    }
}

test "runnable scheduler cleanup transfers a fenced CPU team to one isolated slot" {
    var owner = try Scheduler.init(.{ .slots = 6, .control_slots = 1, .cleanup_slots = 1, .max_operations = 8 });
    defer owner.close();
    var original: Execution = .{};
    defer original.finish();
    try original.startWithSlots(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .inference, 3);
    const now: u64 = @intCast(@max(0, std.Io.Clock.now(.awake, std.testing.io).nanoseconds));
    try original.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = now + 250 * std.time.ns_per_ms });
    try original.activateCleanup();
    try std.testing.expectEqual(Class.inference, try original.retainedClass());
    try std.testing.expectEqual(@as(usize, 1), original.entry.?.slots);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().cleanup_runnable_slots);
    try std.testing.expectEqual(@as(u64, 17), owner.stats().retained_bytes);
    var holder: Execution = .{};
    defer holder.finish();
    try holder.startWithSlots(&owner, .{ .io = std.testing.io, .allow_wait = false }, .write, 4);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&owner, .{ .io = std.testing.io, .allow_wait = false }, .control);
    try std.testing.expectEqual(@as(usize, 6), owner.stats().runnable_slots);
    original.finish();
    holder.finish();
    health.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "runnable scheduler retained cleanup waits fairly for isolated capacity without refreshing its deadline" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var owner = try Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1, .max_operations = 8 });
    defer owner.close();
    var original: Execution = .{};
    defer original.finish();
    try original.start(&owner, .{ .io = std.testing.io, .retained_bytes = 17, .native_now_ns = Clock.now }, .query);
    try original.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = 30, .native_now_ns = Clock.now });
    try original.park(.resource_blocked);
    var holder: Execution = .{};
    defer holder.finish();
    try holder.start(&owner, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
    var retiring: Execution = .{};
    defer retiring.finish();
    try retiring.startCleanupRoot(&owner, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 3, .deadline_ns = 1000, .native_now_ns = Clock.now });
    try owner.activateCleanup(&original.entry.?);
    try std.testing.expectEqual(State.queued, original.entry.?.state);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().queued);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().cleanup_runnable_slots);
    try std.testing.expectEqual(@as(u64, 43), owner.stats().retained_bytes);
    var health: Execution = .{};
    try health.start(&owner, .{ .io = std.testing.io, .allow_wait = false }, .control);
    health.finish();
    Clock.value = 31;
    try std.testing.expectError(error.DeadlineExceeded, owner.waitRetained(&original.entry.?));
    try std.testing.expectEqual(State.stopped, original.entry.?.state);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().queued);
    try std.testing.expectEqual(@as(u64, 43), owner.stats().retained_bytes);
    // Only owning finish after cleanup/transfer returns the old reservation.
    original.finish();
    try std.testing.expectEqual(@as(u64, 26), owner.stats().retained_bytes);
    retiring.finish();
    holder.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "runnable scheduler closed ordinary admission preserves finite independent cleanup reserve" {
    const Clock = struct {
        fn now() u64 {
            return 100;
        }
    };
    var scheduler = try Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
    defer scheduler.close();
    scheduler.close();
    var ordinary: Execution = .{};
    try std.testing.expectError(error.SchedulerClosed, ordinary.start(&scheduler, .{ .io = std.testing.io }, .query));
    var health: Execution = .{};
    try std.testing.expectError(error.SchedulerClosed, health.start(&scheduler, .{ .io = std.testing.io }, .control));
    var cleanup: Execution = .{};
    defer cleanup.finish();
    try cleanup.startCleanupRoot(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .native_now_ns = Clock.now, .deadline_ns = 200, .retained_bytes = 23 });
    try cleanup.checkLifetime();
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    // Still one protected CPU reservation, even after ordinary admission closes.
    var second: Execution = .{};
    defer second.finish();
    const refused = second.startCleanupRoot(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .native_now_ns = Clock.now, .deadline_ns = 200, .retained_bytes = 7 });
    if (refused) |_| return error.TestUnexpectedResult else |_| {}
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    cleanup.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "runnable scheduler automatic capacity respects quota ceilings memory and protected progress" {
    const Clock = struct {
        fn now() u64 {
            return 100;
        }
    };
    const platform = @import("antfly_platform");
    const automatic: Config = .{ .capacity_mode = .automatic };
    for ([_]u64{ 250, 1000, 2000, 3500, 8000, 64000 }) |millicpus| {
        const policy = try automatic.resolveCapacity(.{ .millicpus = millicpus, .source = .cgroup_v2 }, .{ .limit_bytes = 64 << 20, .source = .cgroup_v2 });
        try std.testing.expectEqual(@max(@as(usize, 3), @min(@as(usize, 8), @as(usize, @intCast(millicpus / 1000)))), policy.slots);
        try std.testing.expectEqual(@as(usize, 1), policy.control_slots);
        try std.testing.expectEqual(@as(usize, 1), policy.cleanup_slots);
        try std.testing.expectEqual(@as(u64, 2 << 20), policy.max_bytes);
        try std.testing.expectEqual(automatic.max_wait_ms, policy.max_wait_ms);
        var scheduler = try Scheduler.init(.{ .slots = policy.slots, .cleanup_slots = policy.cleanup_slots, .max_bytes = policy.max_bytes });
        defer scheduler.close();
        var ordinary: Execution = .{};
        try ordinary.startWithSlots(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 23 }, .query, policy.ordinarySlots());
        defer ordinary.finish();
        var health: Execution = .{};
        try health.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 5 }, .control);
        defer health.finish();
        var cleanup: Execution = .{};
        try cleanup.startCleanupRoot(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 7, .deadline_ns = 200, .native_now_ns = Clock.now });
        defer cleanup.finish();
        try std.testing.expectEqual(policy.slots, scheduler.stats().runnable_slots);
        ordinary.finish();
        health.finish();
        cleanup.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
    const fixed: Config = .{};
    try std.testing.expectEqualDeep(fixed, try fixed.resolveCapacity(.{ .millicpus = 250 }, .{ .limit_bytes = 1 }));
    const unknown = try automatic.resolveCapacity(.{}, .{});
    try std.testing.expectEqual(@as(usize, 3), unknown.slots);
    try std.testing.expectEqual(automatic.max_bytes, unknown.max_bytes);
    try std.testing.expectError(error.InvalidRunnablePolicy, automatic.resolveCapacity(.{}, .{ .limit_bytes = 1 << 20 }));
    var detected = try Scheduler.init(automatic);
    defer detected.close();
    const expected = try automatic.resolveCapacity(platform.process_memory.cpuCapacity(), platform.process_memory.systemEnvelope());
    try std.testing.expectEqualDeep(expected, detected.policyAndStats().config);
}

test "runnable read lanes preserve bounded CPU count and bytes under general saturation" {
    const io = std.testing.io;
    var owner = try Scheduler.init(.{ .slots = 4, .control_slots = 1, .cleanup_slots = 1, .read_isolation = true, .max_operations = 12, .max_bytes = 8192, .control_bytes = 1024 });
    defer owner.close();
    var general: [6]Entry = undefined;
    for (&general) |*entry| {
        entry.* = .{ .options = .{ .io = io }, .class = .query, .bytes = 100 };
        try owner.submit(entry);
    }
    defer for (&general) |*entry| owner.finish(entry);
    try std.testing.expectEqual(@as(usize, 1), owner.ordinaryUsed());
    var denied: Entry = .{ .options = .{ .io = io }, .class = .write, .bytes = 100 };
    try std.testing.expectError(error.SchedulerFull, owner.submit(&denied));
    var bounded: Entry = .{ .options = .{ .io = io }, .class = .query, .read_lane = .bounded, .bytes = 100 };
    try owner.submit(&bounded);
    defer owner.finish(&bounded);
    try std.testing.expectEqual(State.running, bounded.state);
    try std.testing.expectEqual(@as(usize, 2), owner.ordinaryUsed());
    try std.testing.expectEqual(@as(usize, 1), owner.stats().bounded_read_operations);
    try std.testing.expectError(error.InvalidRunnableTransition, owner.growInputBytes(&bounded, 101));
    var child: Entry = .{ .options = .{ .io = io }, .class = .query, .bytes = @sizeOf(Entry) };
    try std.testing.expectError(error.InvalidRunnableTransition, owner.submitChild(&bounded, &child, io));
    var control: Entry = .{ .options = .{ .io = io }, .class = .control, .bytes = 100 };
    try owner.submit(&control);
    defer owner.finish(&control);
    try std.testing.expectEqual(State.running, control.state);
    try owner.block(&bounded, .output_blocked);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().bounded_read_slots);
    // Blocked bounded owners retain their count and byte floor, and general
    // work cannot borrow the slot and strand a later output resumption.
    try std.testing.expectEqual(@as(usize, 1), owner.ordinaryUsed());
    try owner.unblock(&bounded);
    try std.testing.expectEqual(State.running, bounded.state);
    owner.finish(&bounded);
    for (&general) |*entry| owner.finish(entry);
    owner.finish(&control);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().bounded_read_operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().bounded_read_bytes);
}

test "runnable read lanes isolate metadata and analytical service with original cancellation" {
    const io = std.testing.io;
    var owner = try Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .max_operations = 12, .max_bytes = 4096, .control_bytes = 1024 });
    defer owner.close();
    var general: Entry = .{ .options = .{ .io = io }, .class = .query, .bytes = 2304 };
    try owner.submit(&general);
    defer owner.finish(&general);
    var overflow: Entry = .{ .options = .{ .io = io }, .class = .background, .bytes = 1 };
    try std.testing.expectError(error.SchedulerFull, owner.submit(&overflow));
    var bounded: Entry = .{ .options = .{ .io = io }, .class = .query, .read_lane = .bounded, .bytes = 768 };
    try owner.submit(&bounded);
    defer owner.finish(&bounded);
    try std.testing.expectEqual(State.running, bounded.state);
    owner.finish(&general);
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    var canceled = false;
    var analytical: Entry = .{ .options = .{ .io = io, .cancellation = .{ .ptr = &canceled, .requested = Cancel.requested } }, .class = .query, .read_lane = .analytical, .bytes = 100 };
    try owner.submit(&analytical);
    defer owner.finish(&analytical);
    try std.testing.expectEqual(State.running, analytical.state);
    var peer: Entry = .{ .options = .{ .io = io }, .class = .query, .bytes = 100 };
    try owner.submit(&peer);
    defer owner.finish(&peer);
    for (0..40) |_| {
        if (analytical.state == .running) try owner.yield(&analytical);
        if (peer.state == .running) try owner.yield(&peer);
    }
    const grants = owner.stats().read_lane_grants;
    try std.testing.expect(grants[@backingInt(ReadLane.general)] > 10);
    try std.testing.expect(grants[@backingInt(ReadLane.analytical)] > 10);
    if (analytical.state == .running) try owner.block(&analytical, .resource_blocked);
    canceled = true;
    try std.testing.expectError(error.Canceled, owner.unblock(&analytical));
    try std.testing.expect(analytical.state != .running);
    owner.finish(&analytical);
    owner.finish(&bounded);
    owner.finish(&peer);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "runnable read lanes automatic policy preserves small envelopes and fixed rollback" {
    const platform = @import("antfly_platform");
    const memory: platform.process_memory.Envelope = .{ .limit_bytes = 64 << 20, .source = .cgroup_v2 };
    const automatic: Config = .{ .capacity_mode = .automatic };
    const small = try automatic.resolveCapacity(.{ .millicpus = 1000, .source = .cgroup_v2 }, memory);
    try std.testing.expect(!small.readIsolation());
    try std.testing.expectEqual(@as(usize, 1), small.generalSlots());
    const large = try automatic.resolveCapacity(.{ .millicpus = 8000, .source = .cgroup_v2 }, memory);
    try std.testing.expect(large.readIsolation());
    try std.testing.expectEqual(large.ordinarySlots() - 1, large.generalSlots());
    const fixed: Config = .{ .slots = 8, .cleanup_slots = 1 };
    try std.testing.expectEqualDeep(fixed, try fixed.resolveForStartup(1));
    try std.testing.expectEqual(fixed.ordinarySlots(), fixed.generalSlots());
    try std.testing.expectError(error.InvalidRunnablePolicy, (Config{ .slots = 3, .cleanup_slots = 1, .read_isolation = true }).validate());
    var constrained = automatic;
    constrained.max_operations = 4;
    const low_count = try constrained.resolveCapacity(.{ .millicpus = 8000, .source = .cgroup_v2 }, memory);
    try std.testing.expect(!low_count.readIsolation());
}

test "runnable read lanes reserve managed cleanup before publishing an isolated owner" {
    var too_small = try Scheduler.init(.{ .slots = 3, .read_isolation = true });
    defer too_small.close();
    try std.testing.expectError(error.ManagerCleanupCapacityUnavailable, too_small.ensureCleanupCpuBeforePublication());
    try std.testing.expectEqual(@as(usize, 0), too_small.config.cleanup_slots);
    var owner = try Scheduler.init(.{ .slots = 4, .read_isolation = true });
    defer owner.close();
    try owner.ensureCleanupCpuBeforePublication();
    try std.testing.expectEqual(@as(usize, 1), owner.config.cleanup_slots);
    try std.testing.expectEqual(@as(usize, 1), owner.config.generalSlots());
    try owner.config.validate();
}

test "runnable read lanes preserve query class weight with an idle bounded lane" {
    var owner = try Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
    defer owner.close();
    var entries: [6]Entry = undefined;
    for (&entries, 0..) |*entry, index| {
        entry.* = .{ .options = .{ .io = std.testing.io }, .class = if (index < 4) .query else .write, .read_lane = if (index >= 2 and index < 4) .analytical else .general, .bytes = 100 };
        try owner.submit(entry);
    }
    defer for (&entries) |*entry| owner.finish(entry);
    for (0..240) |_| {
        const running = for (&entries) |*entry| {
            if (entry.state == .running) break entry;
        } else return error.NoRunnablePeer;
        try owner.yield(running);
    }
    const observed = owner.stats();
    try std.testing.expect(observed.grants[0] >= observed.grants[1] * 2 -| 8);
    try std.testing.expect(observed.grants[0] <= observed.grants[1] * 2 + 8);
    try std.testing.expect(observed.read_lane_grants[@backingInt(ReadLane.general)] > 30);
    try std.testing.expect(observed.read_lane_grants[@backingInt(ReadLane.analytical)] > 30);
    for (&entries) |*entry| owner.finish(entry);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "runnable read lanes automatic sizing honors explicit isolation override" {
    const memory: @import("antfly_platform").process_memory.Envelope = .{ .limit_bytes = 64 << 20, .source = .cgroup_v2 };
    const disabled: Config = .{ .capacity_mode = .automatic, .read_isolation = false };
    const effective = try disabled.resolveCapacity(.{ .millicpus = 8000, .source = .cgroup_v2 }, memory);
    try std.testing.expect(!effective.readIsolation());
    try std.testing.expectEqual(effective.ordinarySlots(), effective.generalSlots());
    const required: Config = .{ .capacity_mode = .automatic, .read_isolation = true };
    try std.testing.expectError(error.InvalidRunnablePolicy, required.resolveCapacity(.{ .millicpus = 1000, .source = .cgroup_v2 }, memory));
}

test "runnable read lanes demotion transfers once while general capacity is full" {
    const io = std.testing.io;
    var owner = try Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .max_operations = 12, .max_bytes = 8192, .control_bytes = 1024 });
    defer owner.close();
    var general: [6]Entry = undefined;
    for (&general) |*entry| {
        entry.* = .{ .options = .{ .io = io }, .class = .query, .bytes = 100 };
        try owner.submit(entry);
    }
    defer for (&general) |*entry| owner.finish(entry);
    var read: Execution = .{};
    defer read.finish();
    try read.startDemotableRead(&owner, .{ .io = io, .retained_bytes = 100 }, .{ .max_retained_bytes = 100, .max_wait_ms = 100, .estimated_work_ns = 1 }, .{ .io = io }, 250 * std.time.ns_per_ms);
    const entry = &read.entry.?;
    const before = owner.stats();
    try std.testing.expectEqual(@as(usize, 7), before.operations);
    try std.testing.expectEqual(@as(usize, 1), before.read_transition_reserved_operations);
    try owner.beginReadDemotion(entry, .analytical);
    try std.testing.expect(!(try owner.tryCompleteReadDemotion(entry)));
    try std.testing.expectError(error.InvalidRunnableTransition, owner.unblock(entry));
    const waiting = owner.stats();
    try std.testing.expectEqual(before.operations, waiting.operations);
    try std.testing.expectEqual(before.retained_bytes, waiting.retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), waiting.bounded_read_operations);
    try std.testing.expectEqual(@as(usize, 1), waiting.read_transition_operations);
    try std.testing.expectEqual(@as(usize, 0), waiting.bounded_read_slots);
    var peer: Execution = .{};
    defer peer.finish();
    try peer.startRead(&owner, .{ .io = io, .allow_wait = false, .retained_bytes = 100 }, .bounded);
    try std.testing.expectEqual(State.running, peer.entry.?.state);
    owner.finish(&general[0]);
    try std.testing.expect(try owner.tryCompleteReadDemotion(entry));
    try std.testing.expectEqual(ReadLane.analytical, entry.read_lane);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().read_transition_reserved_operations);
    try owner.unblock(entry);
    for (&general) |*other| owner.finish(other);
    try owner.waitRetained(entry);
    try std.testing.expectEqual(State.running, entry.state);
    read.finish();
    peer.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().read_transition_reserved_bytes);
}

test "runnable read lanes transition expiry cancellation and close retain joined cleanup ownership" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    const Mode = enum { timeout, deadline, cancel, close };
    for (std.enums.values(Mode)) |mode| {
        Clock.tick = 0;
        var canceled = false;
        var owner = try Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
        defer owner.close();
        var read: Execution = .{};
        defer read.finish();
        try read.startDemotableRead(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100 * std.time.ns_per_ms, .cancellation = .{ .ptr = &canceled, .requested = Clock.requested }, .retained_bytes = 100 }, .{ .max_retained_bytes = 100, .max_wait_ms = 25, .estimated_work_ns = 1 }, .{ .io = std.testing.io, .native_now_ns = Clock.now }, 250 * std.time.ns_per_ms);
        try owner.beginReadDemotion(&read.entry.?, .general);
        const cause: anyerror = switch (mode) {
            .timeout => blk: {
                Clock.tick = 25 * std.time.ns_per_ms;
                break :blk error.RunnableWaitTimeout;
            },
            .deadline => blk: {
                Clock.tick = 100 * std.time.ns_per_ms;
                break :blk error.DeadlineExceeded;
            },
            .cancel => blk: {
                canceled = true;
                break :blk error.Canceled;
            },
            .close => blk: {
                owner.close();
                break :blk error.SchedulerClosed;
            },
        };
        try std.testing.expectError(cause, owner.tryCompleteReadDemotion(&read.entry.?));
        try std.testing.expectEqual(State.stopped, read.entry.?.state);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 100), owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
        Clock.tick = 200 * std.time.ns_per_ms;
        try std.testing.expectError(cause, owner.tryCompleteReadDemotion(&read.entry.?));
        try read.activateCleanup();
        try std.testing.expectEqual(@as(usize, 1), owner.stats().cleanup_runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().read_transition_operations);
        read.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().read_transition_reserved_operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().read_transition_reserved_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().read_transition_operations);
    }
}

test "runnable read lanes measured excess work demotes and repays bounded service debt" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    var owner = try Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .quantum_ms = 2 });
    defer owner.close();
    var read: Execution = .{};
    defer read.finish();
    try read.startDemotableRead(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .retained_bytes = 100 }, .{ .max_retained_bytes = 100, .max_wait_ms = 25, .estimated_work_ns = std.time.ns_per_ms }, .{ .io = std.testing.io, .native_now_ns = Clock.now }, 250 * std.time.ns_per_ms);
    Clock.tick = 10 * std.time.ns_per_ms;
    try read.checkpoint();
    try std.testing.expectEqual(ReadLane.general, read.entry.?.read_lane);
    try std.testing.expectEqual(@as(u64, 10 * std.time.ns_per_ms), read.entry.?.measured_read_work_ns);
    try std.testing.expectEqual(@as(u64, 4), owner.stats().read_service_debt_paid);
    try std.testing.expectEqual(@as(u64, 1), owner.stats().read_demotions);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().read_transition_reserved_operations);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    read.finish();
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "runnable read lanes concurrent demotion wait lends CPU and retains canceled resources" {
    const io = std.testing.io;
    const Job = struct {
        read: *Execution,
        done: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            defer self.done.store(true, .release);
            self.read.demoteRead(.general) catch |err| {
                self.failure = err;
            };
        }
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    var owner = try Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .max_operations = 12, .max_wait_ms = 5000 });
    defer owner.close();
    var general: [6]Entry = undefined;
    for (&general) |*entry| {
        entry.* = .{ .options = .{ .io = io }, .class = .query, .bytes = 100 };
        try owner.submit(entry);
    }
    defer for (&general) |*entry| owner.finish(entry);
    var canceled: std.atomic.Value(bool) = .init(false);
    var read: Execution = .{};
    defer read.finish();
    try read.startDemotableRead(&owner, .{ .io = io, .retained_bytes = 100, .cancellation = .{ .ptr = &canceled, .requested = Job.requested } }, .{ .max_retained_bytes = 100, .max_wait_ms = 5000, .estimated_work_ns = 1 }, .{ .io = io }, 250 * std.time.ns_per_ms);
    var job: Job = .{ .read = &read };
    var future = try io.concurrent(Job.run, .{&job});
    defer future.cancel(io);
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
    while (owner.stats().read_transition_operations == 0) {
        if (job.done.load(.acquire)) return job.failure orelse error.DemotionDidNotWait;
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.DemotionDidNotWait;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    var peer: Execution = .{};
    defer peer.finish();
    try peer.startRead(&owner, .{ .io = io, .allow_wait = false, .retained_bytes = 100 }, .bounded);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&owner, .{ .io = io, .allow_wait = false, .retained_bytes = 100 }, .control);
    canceled.store(true, .release);
    future.await(io);
    try std.testing.expectEqual(error.Canceled, job.failure.?);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().read_transition_operations);
    try read.activateCleanup();
    try std.testing.expectEqual(owner.config.slots, owner.stats().runnable_slots);
    read.finish();
    peer.finish();
    health.finish();
    for (&general) |*entry| owner.finish(entry);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().read_transition_reserved_bytes);
}

test "runnable read lanes transition admission rejects insufficient credits before publication" {
    var owner = try Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .max_operations = 12 });
    defer owner.close();
    for ([_]ReadTransitionBudget{
        .{ .max_retained_bytes = 99, .max_wait_ms = 25, .estimated_work_ns = 1 },
        .{ .max_retained_bytes = 100, .max_wait_ms = 0, .estimated_work_ns = 1 },
        .{ .max_retained_bytes = 100, .max_wait_ms = 251, .estimated_work_ns = 1 },
        .{ .max_retained_bytes = 100, .max_wait_ms = 25, .estimated_work_ns = 0 },
    }) |budget| {
        var read: Execution = .{};
        defer read.finish();
        try std.testing.expectError(error.InvalidRunnableDemand, read.startDemotableRead(&owner, .{ .io = std.testing.io, .retained_bytes = 100 }, budget, .{ .io = std.testing.io }, std.time.ns_per_s));
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().read_transition_reserved_bytes);
    }
    var oversized: Execution = .{};
    defer oversized.finish();
    try std.testing.expectError(error.SchedulerFull, oversized.startDemotableRead(&owner, .{ .io = std.testing.io, .retained_bytes = 100 }, .{ .max_retained_bytes = owner.config.boundedReadMetadataBytes(), .max_wait_ms = 25, .estimated_work_ns = 1 }, .{ .io = std.testing.io }, std.time.ns_per_s));
    var bad_cleanup: Execution = .{};
    defer bad_cleanup.finish();
    try std.testing.expectError(error.InvalidArgument, bad_cleanup.startDemotableRead(&owner, .{ .io = std.testing.io, .retained_bytes = 100 }, .{ .max_retained_bytes = 100, .max_wait_ms = 25, .estimated_work_ns = 1 }, .{ .io = std.testing.io }, 0));
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().read_transition_reserved_operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().read_transition_reserved_bytes);
}

test "runnable scheduler retained read bytes accumulate after cleanup reservation without lending or refreshing authority" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var scheduler = try Scheduler.init(.{ .slots = 2, .max_operations = 4, .max_bytes = 192, .control_bytes = 64 });
    defer scheduler.close();
    var read: Execution = .{};
    defer read.finish();
    try read.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 50, .retained_bytes = 17 }, .query);
    try read.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 100);
    try read.retainReadBytes(11);
    try read.park(.output_blocked);
    try read.retainReadBytes(13);
    try std.testing.expectEqual(@as(u64, 41), scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 50), read.entry.?.options.deadline_ns.?);
    try std.testing.expectEqual(@as(u64, 100), read.entry.?.cleanup_duration_ns.?);
    try std.testing.expectEqual(@as(u64, 41), read.entry.?.cleanup_options.?.retained_bytes);
    try read.retainReadBytes(87);
    try std.testing.expectError(error.SchedulerFull, read.retainReadBytes(1));
    try std.testing.expectEqual(@as(u64, 128), scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(u64, 128), read.entry.?.cleanup_options.?.retained_bytes);
    try std.testing.expectError(error.InvalidRunnableDemand, read.retainReadBytes(std.math.maxInt(u64)));
    Clock.value = 51;
    try std.testing.expectError(error.DeadlineExceeded, read.retainReadBytes(0));
    try std.testing.expectEqual(@as(u64, 128), scheduler.stats().retained_bytes);
    try read.activateCleanup();
    try std.testing.expectError(error.InvalidRunnableTransition, read.retainReadBytes(1));
    try std.testing.expectEqual(@as(u64, 151), read.entry.?.options.deadline_ns.?);
    read.finish();
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "runnable scheduler retained read bytes preserve bounded continuation and partition limits" {
    var scheduler = try Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .max_operations = 12, .max_bytes = 8192, .control_bytes = 1024 });
    defer scheduler.close();
    var read: Execution = .{};
    defer read.finish();
    try read.startDemotableRead(&scheduler, .{ .io = std.testing.io, .retained_bytes = 100 }, .{ .max_retained_bytes = 128, .max_wait_ms = 25, .estimated_work_ns = 1 }, .{ .io = std.testing.io }, 100 * std.time.ns_per_ms);
    try read.retainReadBytes(28);
    try std.testing.expectEqual(@as(u64, 128), scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(u64, 128), scheduler.stats().bounded_read_bytes);
    try std.testing.expectEqual(@as(u64, 128), scheduler.stats().read_transition_reserved_bytes);
    try std.testing.expectError(error.InvalidRunnableDemand, read.retainReadBytes(1));
    try std.testing.expectEqual(@as(u64, 128), scheduler.stats().bounded_read_bytes);
    read.finish();
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().bounded_read_bytes);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().read_transition_reserved_bytes);
    var write: Execution = .{};
    defer write.finish();
    try write.start(&scheduler, .{ .io = std.testing.io }, .write);
    try std.testing.expectError(error.InvalidRunnableTransition, write.retainReadBytes(1));
}

test "runnable scheduler persistent control baseline reserves idle bytes without operation or CPU grants" {
    var owner = try NodeOwner.init(.{ .slots = 2, .max_operations = 4, .max_bytes = 1024, .control_bytes = 256, .read_isolation = false });
    var owner_live = true;
    defer if (owner_live) owner.deinit();
    var first = try owner.scheduler.reserveControlBaseline(32);
    defer first.release();
    var second = try owner.scheduler.reserveControlBaseline(32);
    defer second.release();
    try std.testing.expectError(error.SchedulerFull, owner.scheduler.reserveControlBaseline(1));
    try std.testing.expectError(error.InvalidArgument, owner.scheduler.reserveControlBaseline(0));
    try std.testing.expectEqual(@as(usize, 0), owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 64), owner.scheduler.stats().control_retained_bytes);
    try std.testing.expectError(error.InvalidRunnableTransition, owner.tryDeinit());
    var query: Execution = .{};
    defer query.finish();
    try query.start(&owner.scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 768 }, .query);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&owner.scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 192 }, .control);
    try std.testing.expectEqual(@as(u64, 1024), owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 2), owner.scheduler.stats().operations);
    health.finish();
    query.finish();
    var cleanup: Execution = .{};
    defer cleanup.finish();
    const Clock = struct {
        fn now() u64 {
            return 100;
        }
    };
    try cleanup.startCleanupRoot(&owner.scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 128, .native_now_ns = Clock.now, .deadline_ns = 200 });
    try cleanup.park(.resource_blocked);
    try health.start(&owner.scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 64 }, .control);
    try std.testing.expectEqual(@as(u64, 256), owner.scheduler.stats().control_retained_bytes);
    try std.testing.expectEqual(@as(usize, 2), owner.scheduler.stats().operations);
    health.finish();
    cleanup.finish();
    owner.close();
    try std.testing.expectError(error.SchedulerClosed, owner.scheduler.reserveControlBaseline(1));
    first.release();
    try std.testing.expectEqual(@as(u64, 32), owner.scheduler.stats().retained_bytes);
    second.release();
    try std.testing.expectEqual(@as(u64, 0), owner.scheduler.stats().retained_bytes);
    try owner.tryDeinit();
    owner_live = false;
}

test "runnable CPU loan requires eligibility and returns before protected read execution" {
    inline for (.{ State.resource_blocked, State.output_blocked }) |reason| {
        var owner = try Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2 });
        defer owner.close();
        var base = testEntry(.background, 1, 11);
        var ordinary = testEntry(.query, 1, 13);
        var loan = testEntry(.write, 1, 17);
        loan.cpu_borrow_budget = .{ .max_non_yield_ns = 1000 };
        var protected = testEntry(.query, 1, 19);
        protected.read_lane = .bounded;
        defer owner.finish(&base);
        defer owner.finish(&ordinary);
        defer owner.finish(&loan);
        defer owner.finish(&protected);
        try owner.submit(&base);
        try owner.submit(&ordinary);
        try std.testing.expectEqual(State.queued, owner.state(&ordinary));
        try owner.submit(&loan);
        try std.testing.expectEqual(State.running, owner.state(&loan));
        try std.testing.expectEqual(@as(usize, 1), owner.cpuBorrowStats().active);
        var floor = testEntry(.query, 1, 7);
        floor.read_lane = .bounded;
        defer owner.finish(&floor);
        try owner.submit(&floor);
        try std.testing.expectEqual(State.running, owner.state(&floor));
        try std.testing.expectEqual(State.running, owner.state(&loan));
        try std.testing.expectEqual(@as(usize, 1), owner.cpuBorrowStats().active);
        var health = testEntry(.control, 1, 0);
        defer owner.finish(&health);
        var cleanup = testEntry(.control, 1, 0);
        cleanup.protected_cleanup_root = true;
        cleanup.options.allow_wait = false;
        cleanup.options.deadline_ns = cleanup.options.now() + 100 * std.time.ns_per_ms;
        defer owner.finish(&cleanup);
        try owner.submit(&health);
        try owner.submit(&cleanup);
        try std.testing.expectEqual(State.running, owner.state(&health));
        try std.testing.expectEqual(State.running, owner.state(&cleanup));
        try std.testing.expectEqual(owner.config.slots, owner.stats().runnable_slots);
        owner.finish(&health);
        owner.finish(&cleanup);
        try owner.submit(&protected);
        try std.testing.expectEqual(State.queued, owner.state(&protected));
        const before = owner.stats();
        try owner.yield(&loan);
        try std.testing.expectEqual(State.running, owner.state(&protected));
        try std.testing.expectEqual(State.queued, owner.state(&loan));
        try std.testing.expectEqual(before.operations, owner.stats().operations);
        try std.testing.expectEqual(before.retained_bytes, owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.cpuBorrowStats().active);
        owner.finish(&protected);
        try std.testing.expectEqual(State.running, owner.state(&loan));
        try owner.block(&loan, reason);
        try std.testing.expectEqual(@as(?CpuBorrowBudget, null), loan.cpu_borrow_budget);
        try owner.unblock(&loan);
        try std.testing.expectEqual(State.queued, owner.state(&loan));
        try std.testing.expectEqual(@as(usize, 0), owner.cpuBorrowStats().active);
        owner.finish(&floor);
        owner.finish(&base);
        owner.finish(&ordinary);
        owner.finish(&loan);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
        try std.testing.expectEqual(owner.cpuBorrowStats().grants, owner.cpuBorrowStats().returns);
    }
}

test "runnable CPU loan moves to released general capacity without revoking live execution" {
    var owner = try Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2 });
    defer owner.close();
    var base = testEntry(.background, 1, 11);
    var loan = testEntry(.query, 1, 17);
    loan.cpu_borrow_budget = .{ .max_non_yield_ns = 1000 };
    var protected = testEntry(.query, 1, 19);
    protected.read_lane = .bounded;
    defer owner.finish(&base);
    defer owner.finish(&loan);
    defer owner.finish(&protected);
    try owner.submit(&base);
    try owner.submit(&loan);
    try owner.submit(&protected);
    owner.finish(&base);
    try std.testing.expectEqual(State.running, owner.state(&loan));
    try std.testing.expectEqual(State.running, owner.state(&protected));
    try std.testing.expectEqual(@as(usize, 0), owner.cpuBorrowStats().active);
    try std.testing.expectEqual(@as(usize, 2), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 36), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(u64, 1), owner.cpuBorrowStats().returns);
}

test "runnable CPU loan checkpoint returns before quantum and preserves original lifetime" {
    const io = std.testing.io;
    const Clock = struct {
        fn now() u64 {
            return 0;
        }
    };
    const Worker = struct {
        fn run(execution: *Execution) !void {
            try execution.checkpoint();
        }
    };
    var owner = try Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2, .max_wait_ms = 5000, .checkpoint_budget = 64 });
    defer owner.close();
    var base = testEntry(.background, 1, 11);
    try owner.submit(&base);
    defer owner.finish(&base);
    var loan: Execution = .{};
    defer loan.finish();
    try loan.startBorrowable(&owner, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 1000000, .retained_bytes = 17 }, .query, .{ .max_non_yield_ns = 1000 });
    const original = loan.entry.?.options.deadline_ns;
    var floor = testEntry(.query, 1, 0);
    floor.read_lane = .bounded;
    try owner.submit(&floor);
    defer owner.finish(&floor);
    try std.testing.expectEqual(State.running, owner.state(&floor));
    try std.testing.expectEqual(@as(usize, 1), owner.cpuBorrowStats().active);
    var protected = testEntry(.query, 1, 19);
    protected.read_lane = .bounded;
    try owner.submit(&protected);
    defer owner.finish(&protected);
    var future = try io.concurrent(Worker.run, .{&loan});
    defer future.cancel(io) catch {};
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
    while (owner.state(&protected) != .running) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CpuLoanDidNotReturn;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(State.queued, owner.state(&loan.entry.?));
    try std.testing.expectEqual(@as(u64, 47), owner.stats().retained_bytes);
    owner.finish(&protected);
    try future.await(io);
    try std.testing.expectEqual(original, loan.entry.?.options.deadline_ns);
    try std.testing.expectEqual(@as(u64, 2), loan.resume_generation);
    try std.testing.expectEqual(@as(u64, 2), owner.cpuBorrowStats().grants);
}

test "runnable CPU loan cancellation expiry and close retain cleanup ownership" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    const Token = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    inline for (.{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        Clock.tick = 0;
        var canceled = false;
        var owner = try Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2 });
        defer owner.close();
        var base = testEntry(.background, 1, 11);
        try owner.submit(&base);
        defer owner.finish(&base);
        var loan: Execution = .{};
        defer loan.finish();
        try loan.startBorrowable(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 10, .retained_bytes = 17, .cancellation = .{ .ptr = &canceled, .requested = Token.requested } }, .query, .{ .max_non_yield_ns = 1000 });
        try loan.reserveCleanupFor(.{ .io = std.testing.io }, 100 * std.time.ns_per_ms);
        if (cause == error.Canceled) canceled = true else if (cause == error.DeadlineExceeded) Clock.tick = 10 else owner.close();
        try std.testing.expectError(cause, loan.checkpoint());
        try std.testing.expectEqual(State.stopped, owner.state(&loan.entry.?));
        try std.testing.expectEqual(@as(usize, 0), owner.cpuBorrowStats().active);
        try std.testing.expectEqual(@as(u64, 28), owner.stats().retained_bytes);
        try loan.activateCleanup();
        try std.testing.expectEqual(@as(usize, 1), owner.stats().cleanup_runnable_slots);
        try std.testing.expectEqual(@as(?CpuBorrowBudget, null), loan.entry.?.cpu_borrow_budget);
        loan.finish();
        owner.finish(&base);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    }
}

test "runnable CPU loan rejects unsafe growth fanout promotion and invalid work bounds" {
    var owner = try Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true });
    defer owner.close();
    var execution: Execution = .{};
    defer execution.finish();
    try execution.startBorrowable(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .write, .{ .max_non_yield_ns = 1000 });
    try std.testing.expectError(error.InvalidRunnableTransition, execution.resizeTeam(2));
    try std.testing.expectError(error.InvalidRunnableTransition, execution.growInputBytes(18));
    try std.testing.expectError(error.InvalidRunnableTransition, execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = std.math.maxInt(u64) }));
    var child: Execution = .{};
    defer child.finish();
    try std.testing.expectError(error.InvalidRunnableTransition, child.reserveChild(&execution, std.testing.io, @sizeOf(Execution)));
    try execution.disableCpuBorrow();
    try execution.resizeTeam(2);
    try std.testing.expectEqual(@as(usize, 2), execution.entry.?.slots);
    try execution.resizeTeam(1);
    try execution.enableCpuBorrow(.{ .max_non_yield_ns = 1000 });
    try std.testing.expectError(error.InvalidRunnableTransition, execution.resizeTeam(2));
    try execution.disableCpuBorrow();
    try execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = std.math.maxInt(u64) });
    try std.testing.expectError(error.InvalidRunnableTransition, execution.enableCpuBorrow(.{ .max_non_yield_ns = 1000 }));
    inline for (.{ @as(u64, 0), @as(u64, 101 * std.time.ns_per_ms) }) |bound| {
        var invalid = testEntry(.query, 1, 0);
        invalid.cpu_borrow_budget = .{ .max_non_yield_ns = bound };
        try std.testing.expectError(error.InvalidRunnableTransition, owner.submit(&invalid));
        try std.testing.expectEqual(State.fresh, invalid.state);
    }
}

test "runnable CPU loan disable reacquires general authority before an unqualified phase" {
    const io = std.testing.io;
    const Worker = struct {
        fn run(execution: *Execution) !void {
            try execution.disableCpuBorrow();
        }
    };
    var owner = try Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2, .max_wait_ms = 5000 });
    defer owner.close();
    var base = testEntry(.background, 1, 11);
    try owner.submit(&base);
    defer owner.finish(&base);
    var loan: Execution = .{};
    defer loan.finish();
    try loan.startBorrowable(&owner, .{ .io = io, .retained_bytes = 17 }, .query, .{ .max_non_yield_ns = 1000 });
    var future = try io.concurrent(Worker.run, .{&loan});
    defer future.cancel(io) catch {};
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
    while (owner.state(&loan.entry.?) != .queued) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CpuLoanDisableDidNotWait;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(usize, 0), owner.cpuBorrowStats().active);
    try std.testing.expectEqual(@as(u64, 28), owner.stats().retained_bytes);
    owner.finish(&base);
    try future.await(io);
    try std.testing.expectEqual(@as(?CpuBorrowBudget, null), loan.entry.?.cpu_borrow_budget);
    try std.testing.expectEqual(State.running, owner.state(&loan.entry.?));
    try std.testing.expectEqual(@as(u64, 1), owner.cpuBorrowStats().grants);
}

test "runnable CPU loan cleanup never carries a loan into dedicated or legacy cleanup" {
    inline for (.{ @as(usize, 0), @as(usize, 1) }) |cleanup_slots| {
        var owner = try Scheduler.init(.{ .slots = 4 + cleanup_slots, .cleanup_slots = cleanup_slots, .read_isolation = true, .bounded_read_slots = 2 });
        defer owner.close();
        var base = testEntry(.background, 1, 11);
        try owner.submit(&base);
        defer owner.finish(&base);
        var loan: Execution = .{};
        defer loan.finish();
        try loan.startBorrowable(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .query, .{ .max_non_yield_ns = 1000 });
        try loan.reserveCleanupFor(.{ .io = std.testing.io }, 100 * std.time.ns_per_ms);
        try owner.activateCleanup(&loan.entry.?);
        try std.testing.expectEqual(@as(usize, 0), owner.cpuBorrowStats().active);
        try std.testing.expectEqual(@as(?CpuBorrowBudget, null), loan.entry.?.cpu_borrow_budget);
        if (cleanup_slots == 0) try std.testing.expectEqual(State.queued, owner.state(&loan.entry.?)) else try std.testing.expectEqual(@as(usize, 1), owner.stats().cleanup_runnable_slots);
        owner.finish(&base);
        try owner.waitRetained(&loan.entry.?);
        loan.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
        try std.testing.expectEqual(owner.cpuBorrowStats().grants, owner.cpuBorrowStats().returns);
    }
}

test "runnable CPU loan preserves the default hard floor without a borrower checkpoint" {
    var owner = try Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
    defer owner.close();
    var base = testEntry(.background, 1, 11);
    defer owner.finish(&base);
    try owner.submit(&base);
    var loan = testEntry(.write, 1, 17);
    loan.cpu_borrow_budget = .{ .max_non_yield_ns = 1000 };
    defer owner.finish(&loan);
    try owner.submit(&loan);
    try std.testing.expectEqual(State.queued, owner.state(&loan));
    try std.testing.expectEqual(@as(usize, 0), owner.cpuBorrowStats().active);
    var floor = testEntry(.query, 1, 19);
    floor.read_lane = .bounded;
    defer owner.finish(&floor);
    try owner.submit(&floor);
    try std.testing.expectEqual(State.running, owner.state(&floor));
    try std.testing.expectEqual(State.running, owner.state(&base));
    try std.testing.expectEqual(@as(u64, 0), owner.cpuBorrowStats().grants);
}

test "runnable CPU loan elastic reserves keep a hard floor at every bounded occupancy" {
    for ([_]usize{ 2, 3, 4 }) |reserve| {
        for (0..reserve) |occupied| {
            var owner = try Scheduler.init(.{ .slots = reserve + 3, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = reserve });
            defer owner.close();
            var base = testEntry(.background, 1, 11);
            defer owner.finish(&base);
            try owner.submit(&base);
            var reads: [4]Entry = undefined;
            var count: usize = 0;
            defer for (reads[0..count]) |*read| owner.finish(read);
            for (0..occupied) |_| {
                reads[count] = testEntry(.query, 1, 1);
                reads[count].read_lane = .bounded;
                try owner.submit(&reads[count]);
                try std.testing.expectEqual(State.running, owner.state(&reads[count]));
                count += 1;
            }
            var loan = testEntry(.write, 1, 17);
            loan.cpu_borrow_budget = .{ .max_non_yield_ns = 1000 };
            defer owner.finish(&loan);
            try owner.submit(&loan);
            try std.testing.expectEqual(State.running, owner.state(&loan));
            try std.testing.expectEqual(@as(usize, 1), owner.cpuBorrowStats().active);
            // No checkpoint, yield, or block from the borrower. If the floor
            // was idle it must admit a bounded read immediately.
            if (occupied == 0) {
                reads[count] = testEntry(.query, 1, 1);
                reads[count].read_lane = .bounded;
                try owner.submit(&reads[count]);
                try std.testing.expectEqual(State.running, owner.state(&reads[count]));
                count += 1;
            }
            var second = testEntry(.inference, 1, 23);
            second.cpu_borrow_budget = .{ .max_non_yield_ns = 1000 };
            defer owner.finish(&second);
            try owner.submit(&second);
            try std.testing.expectEqual(State.queued, owner.state(&second));
            try std.testing.expectEqual(State.running, owner.state(&loan));
            try std.testing.expectEqual(@as(usize, 1), owner.cpuBorrowStats().active);
            try std.testing.expect(owner.stats().runnable_slots <= owner.config.slots);
            try std.testing.expectEqual(@as(u64, 51 + count), owner.stats().retained_bytes);
        }
    }
}

test "runnable scheduler bounded CPU reserve validates startup and preserves cleanup" {
    for ([_]usize{ 0, 65534, std.math.maxInt(usize) }) |reserve| {
        try std.testing.expectError(error.InvalidRunnablePolicy, (Config{ .bounded_read_slots = reserve }).validate());
    }
    const explicit: Config = .{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2 };
    try explicit.validateManagedCleanupCapacity();
    try std.testing.expectEqual(@as(usize, 1), explicit.generalSlots());
    try std.testing.expectEqual(@as(usize, 1), explicit.boundedReadFloorSlots());
    try std.testing.expectEqual(@as(usize, 1), explicit.cpuBorrowSlots());
    try std.testing.expectError(error.InvalidRunnablePolicy, (Config{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2 }).validate());
    try std.testing.expectError(error.ManagerCleanupCapacityUnavailable, (Config{ .slots = 4, .read_isolation = true, .bounded_read_slots = 2 }).validateManagedCleanupCapacity());
    const automatic: Config = .{ .capacity_mode = .automatic, .bounded_read_slots = 2 };
    const small = try automatic.resolveCapacity(.{ .millicpus = 4000, .source = .cgroup_v2 }, .{});
    try std.testing.expect(!small.readIsolation());
    try std.testing.expectEqual(@as(usize, 2), small.generalSlots());
    const large = try automatic.resolveCapacity(.{ .millicpus = 5000, .source = .cgroup_v2 }, .{});
    try std.testing.expect(large.readIsolation());
    try std.testing.expectEqual(@as(usize, 1), large.generalSlots());
    const required: Config = .{ .capacity_mode = .automatic, .bounded_read_slots = 2, .read_isolation = true };
    try std.testing.expectError(error.InvalidRunnablePolicy, required.resolveCapacity(.{ .millicpus = 4000, .source = .cgroup_v2 }, .{}));
    var unpublished = try Scheduler.init(.{ .slots = 5, .read_isolation = true, .bounded_read_slots = 2 });
    defer unpublished.close();
    try unpublished.ensureCleanupCpuBeforePublication();
    try std.testing.expectEqual(@as(usize, 1), unpublished.config.cleanup_slots);
    try std.testing.expectEqual(@as(usize, 1), unpublished.config.generalSlots());
}

test "runnable CPU loan stage exit preserves ordinary quantum and clears eligibility once" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var owner = try Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
    defer owner.close();
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 1000, .retained_bytes = 17 }, .query);
    Clock.tick = 10;
    try execution.checkpoint();
    const generation = execution.resume_generation;
    const quantum = execution.quantum_start_ns;
    const sample = execution.read_sample_ns;
    const checkpoints = execution.checkpoints;
    for (0..8) |_| try execution.endCpuBorrowStage();
    try std.testing.expectEqual(generation, execution.resume_generation);
    try std.testing.expectEqual(quantum, execution.quantum_start_ns);
    try std.testing.expectEqual(sample, execution.read_sample_ns);
    try std.testing.expectEqual(checkpoints, execution.checkpoints);
    try execution.enableCpuBorrow(.{ .max_non_yield_ns = 100 });
    Clock.tick = 20;
    try execution.endCpuBorrowStage();
    try std.testing.expectEqual(@as(?CpuBorrowBudget, null), execution.entry.?.cpu_borrow_budget);
    try std.testing.expectEqual(generation + 1, execution.resume_generation);
    try std.testing.expectEqual(@as(u64, 1000), execution.entry.?.options.deadline_ns.?);
    try std.testing.expectEqual(@as(u64, 17), owner.stats().retained_bytes);
    const after = execution.resume_generation;
    Clock.tick = 30;
    try execution.endCpuBorrowStage();
    try std.testing.expectEqual(after, execution.resume_generation);
    try std.testing.expectEqual(@as(u64, 20), execution.quantum_start_ns);
    try std.testing.expectEqual(@as(u64, 0), owner.cpuBorrowStats().grants);
}

test "runnable CPU loan transfers a live validated grant without suspending or refreshing authority" {
    const Clock = struct {
        fn now() u64 {
            return 0;
        }
    };
    for ([_]usize{ 1, 2 }) |reserve| {
        var owner = try Scheduler.init(.{ .slots = reserve + 3, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = reserve });
        defer owner.close();
        var execution: Execution = .{};
        defer execution.finish();
        try execution.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 1000000, .retained_bytes = 17 }, .query);
        try execution.checkpoint();
        var peer = testEntry(.write, 1, 13);
        try owner.submit(&peer);
        defer owner.finish(&peer);
        try std.testing.expectEqual(State.queued, owner.state(&peer));
        const generation = execution.resume_generation;
        const quantum = execution.quantum_start_ns;
        const checkpoints = execution.checkpoints;
        try execution.enableCpuBorrow(.{ .max_non_yield_ns = 1000 });
        try std.testing.expectEqual(State.running, owner.state(&execution.entry.?));
        try std.testing.expectEqual(if (reserve == 1) State.queued else State.running, owner.state(&peer));
        try std.testing.expectEqual(reserve == 2, execution.entry.?.borrowed_read_cpu);
        try std.testing.expectEqual(generation, execution.resume_generation);
        try std.testing.expectEqual(quantum, execution.quantum_start_ns);
        try std.testing.expectEqual(checkpoints, execution.checkpoints);
        var floor = testEntry(.query, 1, 19);
        floor.read_lane = .bounded;
        try owner.submit(&floor);
        defer owner.finish(&floor);
        var health = testEntry(.control, 1, 0);
        try owner.submit(&health);
        defer owner.finish(&health);
        var cleanup = testEntry(.control, 1, 0);
        cleanup.protected_cleanup_root = true;
        cleanup.options.allow_wait = false;
        cleanup.options.deadline_ns = cleanup.options.now() + 100 * std.time.ns_per_ms;
        try owner.submit(&cleanup);
        defer owner.finish(&cleanup);
        try std.testing.expectEqual(State.running, owner.state(&floor));
        try std.testing.expectEqual(State.running, owner.state(&health));
        try std.testing.expectEqual(State.running, owner.state(&cleanup));
        try std.testing.expectEqual(owner.config.slots, owner.stats().runnable_slots);
        try std.testing.expectEqual(@as(u64, 49), owner.stats().retained_bytes);
        try execution.park(.resource_blocked);
        try std.testing.expectEqual(@as(usize, 0), owner.cpuBorrowStats().active);
        try std.testing.expectEqual(@as(?CpuBorrowBudget, null), execution.entry.?.cpu_borrow_budget);
        try std.testing.expectEqual(State.running, owner.state(&peer));
        owner.finish(&peer);
        try execution.enter(.query);
        try std.testing.expectEqual(@as(u64, 1000000), execution.entry.?.options.deadline_ns.?);
        owner.finish(&health);
        owner.finish(&cleanup);
        owner.finish(&floor);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
        try std.testing.expectEqual(owner.cpuBorrowStats().grants, owner.cpuBorrowStats().returns);
    }
}

test "runnable CPU loan live transfer preserves team assembly when released capacity cannot fit" {
    var owner = try Scheduler.init(.{ .slots = 6, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2 });
    defer owner.close();
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
    var base = testEntry(.background, 1, 11);
    try owner.submit(&base);
    defer owner.finish(&base);
    var team = testEntry(.write, 2, 13);
    try owner.submit(&team);
    defer owner.finish(&team);
    try execution.enableCpuBorrow(.{ .max_non_yield_ns = 1000 });
    try std.testing.expectEqual(State.queued, owner.state(&team));
    try std.testing.expectEqual(@as(usize, 0), owner.cpuBorrowStats().active);
    try std.testing.expect(!execution.entry.?.borrowed_read_cpu);
    try std.testing.expectEqual(@as(usize, 2), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 41), owner.stats().retained_bytes);
    try execution.park(.resource_blocked);
    try std.testing.expectEqual(State.queued, owner.state(&team));
    owner.finish(&base);
    try std.testing.expectEqual(State.running, owner.state(&team));
    owner.finish(&team);
    try execution.enter(.query);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(owner.cpuBorrowStats().grants, owner.cpuBorrowStats().returns);
}

test "runnable scheduler bounds class lookahead and reveals fitting work after head retirement" {
    inline for (.{ false, true }) |isolated| {
        var scheduler = try Scheduler.init(.{ .slots = if (isolated) 6 else 5, .cleanup_slots = 1, .read_isolation = isolated, .max_backfills = 65535 });
        var holder = testEntry(.query, 2, 0);
        var blocked: [max_backfill_lookahead + 1]Entry = undefined;
        for (&blocked) |*entry| entry.* = testEntry(.write, 2, 0);
        var fitting = testEntry(.write, 1, 0);
        var control = testEntry(.control, 1, 0);
        var bounded = testEntry(.query, 1, 0);
        bounded.read_lane = .bounded;
        var cleanup: Execution = .{};
        defer cleanup.finish();
        defer scheduler.finish(&bounded);
        defer scheduler.finish(&holder);
        defer for (&blocked) |*entry| scheduler.finish(entry);
        defer scheduler.finish(&fitting);
        defer scheduler.finish(&control);
        try scheduler.submit(&holder);
        for (&blocked) |*entry| try scheduler.submit(entry);
        try scheduler.submit(&fitting);
        // Increasing the bypass-count limit does not make selection scan a whole
        // queue. The fitting operation is just beyond the bounded window.
        try std.testing.expectEqual(State.queued, scheduler.state(&fitting));
        try scheduler.submit(&control);
        try std.testing.expectEqual(State.running, scheduler.state(&control));
        if (isolated) {
            try scheduler.submit(&bounded);
            try std.testing.expectEqual(State.running, scheduler.state(&bounded));
        }
        try cleanup.startCleanupRoot(&scheduler, .{ .io = std.testing.io, .deadline_ns = holder.options.now() + 200 * std.time.ns_per_ms, .allow_wait = false });
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().cleanup_runnable_slots);
        scheduler.finish(&blocked[0]);
        try std.testing.expectEqual(State.running, scheduler.state(&fitting));
        scheduler.finish(&fitting);
        scheduler.finish(&holder);
        for (blocked[1..]) |*entry| {
            try std.testing.expectEqual(State.running, scheduler.state(entry));
            scheduler.finish(entry);
        }
        scheduler.finish(&control);
        scheduler.finish(&bounded);
        cleanup.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    }
}

test "runnable scheduler discovers continuations independently of full fresh lookahead" {
    inline for (.{ false, true }) |isolated| {
        inline for (.{ State.resource_blocked, State.output_blocked }) |reason| {
            var scheduler = try Scheduler.init(.{ .slots = if (isolated) 6 else 5, .cleanup_slots = 1, .read_isolation = isolated });
            var resumed = testEntry(.write, 1, 37);
            var holder = testEntry(.query, 2, 11);
            var starts: [max_backfill_lookahead + 1]Entry = undefined;
            for (&starts) |*entry| entry.* = testEntry(.write, 2, 5);
            var hidden_start = testEntry(.write, 1, 13);
            defer scheduler.finish(&resumed);
            defer scheduler.finish(&holder);
            defer for (&starts) |*entry| scheduler.finish(entry);
            defer scheduler.finish(&hidden_start);
            try scheduler.submit(&resumed);
            try scheduler.block(&resumed, reason);
            try scheduler.submit(&holder);
            for (&starts) |*entry| try scheduler.submit(entry);
            try scheduler.submit(&hidden_start);
            try std.testing.expectEqual(State.queued, scheduler.state(&hidden_start));
            const before = scheduler.stats();
            const original_io = resumed.options.io;
            try scheduler.unblock(&resumed);
            try std.testing.expectEqual(State.running, scheduler.state(&resumed));
            try std.testing.expectEqual(State.queued, scheduler.state(&hidden_start));
            try std.testing.expectEqual(before.operations, scheduler.stats().operations);
            try std.testing.expectEqual(before.retained_bytes, scheduler.stats().retained_bytes);
            try std.testing.expectEqual(original_io, resumed.options.io);
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().output_blocked);
            scheduler.finish(&resumed);
            // The independent continuation window does not widen fresh scans.
            try std.testing.expectEqual(State.queued, scheduler.state(&hidden_start));
            scheduler.finish(&starts[0]);
            try std.testing.expectEqual(State.running, scheduler.state(&hidden_start));
            scheduler.finish(&hidden_start);
            scheduler.finish(&holder);
            for (starts[1..]) |*entry| {
                try std.testing.expectEqual(State.running, scheduler.state(entry));
                scheduler.finish(entry);
            }
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().queued);
        }
    }
}

test "runnable scheduler merges fresh and resumed service in enqueue order and closes both" {
    var scheduler = try Scheduler.init(.{ .slots = 2 });
    var resumed = testEntry(.write, 1, 17);
    var holder = testEntry(.query, 1, 19);
    var fresh = testEntry(.write, 1, 23);
    var younger = testEntry(.write, 1, 29);
    defer scheduler.finish(&resumed);
    defer scheduler.finish(&holder);
    defer scheduler.finish(&fresh);
    defer scheduler.finish(&younger);
    try scheduler.submit(&resumed);
    try scheduler.block(&resumed, .resource_blocked);
    try scheduler.submit(&holder);
    try scheduler.submit(&fresh);
    try scheduler.unblock(&resumed);
    scheduler.finish(&holder);
    try std.testing.expectEqual(State.running, scheduler.state(&fresh));
    try std.testing.expectEqual(State.queued, scheduler.state(&resumed));
    scheduler.finish(&fresh);
    try std.testing.expectEqual(State.running, scheduler.state(&resumed));
    try scheduler.submit(&younger);
    try scheduler.yield(&resumed);
    try std.testing.expectEqual(State.running, scheduler.state(&younger));
    try std.testing.expectEqual(State.queued, scheduler.state(&resumed));
    scheduler.close();
    try std.testing.expectEqual(State.stopped, scheduler.state(&resumed));
    try std.testing.expectEqual(error.SchedulerClosed, resumed.failure.?);
    try std.testing.expectEqual(State.running, scheduler.state(&younger));
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().queued);
    try std.testing.expectEqual(@as(u64, 46), scheduler.stats().retained_bytes);
    scheduler.finish(&younger);
    scheduler.finish(&resumed);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "runnable scheduler bounds continuation lookahead independently of fresh candidates" {
    var scheduler = try Scheduler.init(.{ .slots = 4, .max_backfills = 65535 });
    var resumed: [max_backfill_lookahead + 1]Entry = undefined;
    for (&resumed) |*entry| entry.* = testEntry(.write, 2, 5);
    var hidden = testEntry(.write, 1, 7);
    var holder = testEntry(.query, 2, 11);
    var fresh = testEntry(.write, 1, 13);
    defer for (&resumed) |*entry| scheduler.finish(entry);
    defer scheduler.finish(&hidden);
    defer scheduler.finish(&holder);
    defer scheduler.finish(&fresh);
    for (&resumed) |*entry| {
        try scheduler.submit(entry);
        try scheduler.block(entry, .resource_blocked);
    }
    try scheduler.submit(&hidden);
    try scheduler.block(&hidden, .output_blocked);
    try scheduler.submit(&holder);
    for (&resumed) |*entry| try scheduler.unblock(entry);
    try scheduler.unblock(&hidden);
    try std.testing.expectEqual(State.queued, scheduler.state(&hidden));
    try scheduler.submit(&fresh);
    try std.testing.expectEqual(State.running, scheduler.state(&fresh));
    try std.testing.expectEqual(State.queued, scheduler.state(&hidden));
    scheduler.finish(&fresh);
    scheduler.finish(&resumed[0]);
    try std.testing.expectEqual(State.running, scheduler.state(&hidden));
    scheduler.finish(&hidden);
    scheduler.finish(&holder);
    for (resumed[1..]) |*entry| {
        try std.testing.expectEqual(State.running, scheduler.state(entry));
        scheduler.finish(entry);
    }
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().queued);
}

test "runnable scheduler discovers reserved child reentry before its first CPU grant" {
    var scheduler = try Scheduler.init(.{ .slots = 4 });
    var parent = testEntry(.query, 1, 17);
    var child = testEntry(.query, 1, @sizeOf(Entry));
    var holder = testEntry(.query, 1, 19);
    var starts: [max_backfill_lookahead + 1]Entry = undefined;
    for (&starts) |*entry| entry.* = testEntry(.query, 2, 23);
    defer scheduler.finish(&parent);
    defer scheduler.finish(&child);
    defer scheduler.finish(&holder);
    defer for (&starts) |*entry| scheduler.finish(entry);
    try scheduler.submit(&parent);
    try scheduler.reserveChild(&parent, &child, std.testing.io);
    try std.testing.expectEqual(State.resource_blocked, scheduler.state(&child));
    try scheduler.submit(&holder);
    for (&starts) |*entry| try scheduler.submit(entry);
    const before = scheduler.stats();
    try scheduler.unblock(&child);
    try std.testing.expectEqual(State.running, scheduler.state(&child));
    try std.testing.expectEqual(before.operations, scheduler.stats().operations);
    try std.testing.expectEqual(before.retained_bytes, scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), parent.children);
    scheduler.finish(&child);
    try std.testing.expectEqual(@as(usize, 0), parent.children);
    scheduler.finish(&holder);
    scheduler.finish(&parent);
    for (&starts) |*entry| {
        try std.testing.expectEqual(State.running, scheduler.state(entry));
        scheduler.finish(entry);
    }
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().queued);
}

test "runnable synchronous resource stage detaches temporary callbacks on policy and resource refusal" {
    const Stage = struct {
        probes: usize = 0,
        acquisitions: usize = 0,
        policy_denied: bool,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.policy_denied) return error.Forbidden;
        }
        fn probe(raw: *anyopaque) callconv(.c) c_int {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.probes += 1;
            return @backingInt(GrantResources.Probe.too_large);
        }
        fn acquire(raw: *anyopaque) callconv(.c) c_int {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.acquisitions += 1;
            return @backingInt(GrantResources.Acquire.acquired);
        }
    };
    for ([_]bool{ true, false }) |policy_denied| {
        var scheduler = try Scheduler.init(.{ .slots = 2 });
        defer scheduler.close();
        var execution: Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 72 }, .query);
        defer execution.finish();
        var stage: Stage = .{ .policy_denied = policy_denied };
        try std.testing.expectError(if (policy_denied) error.Forbidden else error.ResourceRequestTooLarge, execution.enterWithResources(.{ .context = &stage, .probe = Stage.probe, .acquire = Stage.acquire }, .{ .context = &stage, .check = Stage.check }));
        try std.testing.expect(execution.entry.?.pending_resources == null);
        try std.testing.expectEqual(State.stopped, execution.entry.?.state);
        try std.testing.expectEqual(@as(usize, if (policy_denied) 0 else 1), stage.probes);
        try std.testing.expectEqual(@as(usize, 0), stage.acquisitions);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 72), scheduler.stats().retained_bytes);
        scheduler.notifyResourceProgress();
        try std.testing.expectEqual(@as(usize, if (policy_denied) 0 else 1), stage.probes);
    }
    var scheduler = try Scheduler.init(.{ .slots = 2 });
    defer scheduler.close();
    var control: Execution = .{};
    try control.start(&scheduler, .{ .io = std.testing.io }, .control);
    defer control.finish();
    var stage: Stage = .{ .policy_denied = false };
    try std.testing.expectError(error.InvalidRunnableTransition, control.enterWithResources(.{ .context = &stage, .probe = Stage.probe, .acquire = Stage.acquire }, null));
    try std.testing.expectEqual(State.running, control.entry.?.state);
    try std.testing.expectEqual(@as(usize, 0), stage.probes);
}

test "runnable scheduler ages sparse backfills on the original clock while retained owners and controls progress" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var scheduler = try Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .weights = .{ 1, 1, 1, 1 }, .max_backfills = 65535 });
    var resumed = testEntry(.inference, 1, 17);
    var holder = testEntry(.write, 2, 19);
    var large = testEntry(.query, 3, 23);
    var small: [3]Entry = undefined;
    for (&small) |*e| e.* = testEntry(.query, 1, 29);
    var control = testEntry(.control, 1, 31);
    defer scheduler.finish(&resumed);
    defer scheduler.finish(&holder);
    defer scheduler.finish(&large);
    defer scheduler.finish(&control);
    defer for (&small) |*e| scheduler.finish(e);
    resumed.options.native_now_ns = Clock.now;
    holder.options.native_now_ns = Clock.now;
    large.options.native_now_ns = Clock.now;
    large.options.deadline_ns = std.time.ns_per_s;
    try scheduler.submit(&resumed);
    try scheduler.block(&resumed, .resource_blocked);
    try scheduler.submit(&holder);
    try scheduler.submit(&large);
    const original = large.queued_since_ns;
    try scheduler.submit(&small[0]);
    try std.testing.expectEqual(State.running, scheduler.state(&small[0]));
    scheduler.finish(&small[0]);
    Clock.tick = scheduler.config.backfillAgeNs() - 1;
    try scheduler.submit(&small[1]);
    try std.testing.expectEqual(State.running, scheduler.state(&small[1]));
    scheduler.finish(&small[1]);
    Clock.tick += 1;
    try scheduler.pollWait(&large);
    try scheduler.submit(&small[2]);
    try std.testing.expectEqual(State.queued, scheduler.state(&small[2]));
    try std.testing.expectEqual(@as(u16, 2), large.bypasses);
    try std.testing.expectEqual(original, large.queued_since_ns);
    const before = scheduler.stats();
    try scheduler.unblock(&resumed);
    try std.testing.expectEqual(State.running, scheduler.state(&resumed));
    try std.testing.expectEqual(State.queued, scheduler.state(&small[2]));
    try std.testing.expectEqual(before.operations, scheduler.stats().operations);
    try std.testing.expectEqual(before.retained_bytes, scheduler.stats().retained_bytes);
    try scheduler.submit(&control);
    try std.testing.expectEqual(State.running, scheduler.state(&control));
    var cleanup: Execution = .{};
    defer cleanup.finish();
    try cleanup.startCleanupRoot(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .deadline_ns = control.options.now() + 100 * std.time.ns_per_ms });
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().cleanup_runnable_slots);
    scheduler.finish(&resumed);
    scheduler.finish(&holder);
    try std.testing.expectEqual(State.running, scheduler.state(&large));
    try std.testing.expectEqual(State.queued, scheduler.state(&small[2]));
    try std.testing.expectEqual(@as(?u64, std.time.ns_per_s), large.options.deadline_ns);
    scheduler.finish(&large);
    try std.testing.expectEqual(State.running, scheduler.state(&small[2]));
    try std.testing.expectEqual(@as(u64, 2), scheduler.metrics.grants[@backingInt(Class.inference)]);
}

test "runnable scheduler orders aged barriers by enqueue sequence across unrelated original clocks" {
    const OlderClock = struct {
        var tick: u64 = 100 * std.time.ns_per_s;
        fn now() u64 {
            return tick;
        }
    };
    const YoungerClock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    OlderClock.tick = 100 * std.time.ns_per_s;
    YoungerClock.tick = 0;
    var scheduler = try Scheduler.init(.{ .slots = 4, .weights = .{ 1, 1, 1, 1 }, .max_backfills = 65535 });
    var holder = testEntry(.background, 2, 0);
    var older = testEntry(.query, 3, 0);
    var younger = testEntry(.write, 3, 0);
    defer scheduler.finish(&holder);
    defer scheduler.finish(&older);
    defer scheduler.finish(&younger);
    older.options.native_now_ns = OlderClock.now;
    younger.options.native_now_ns = YoungerClock.now;
    try scheduler.submit(&holder);
    try scheduler.submit(&older);
    try scheduler.submit(&younger);
    OlderClock.tick += scheduler.config.backfillAgeNs();
    YoungerClock.tick += scheduler.config.backfillAgeNs();
    try scheduler.pollWait(&older);
    try scheduler.pollWait(&younger);
    try std.testing.expect(scheduler.resource_barrier == &older);
    scheduler.finish(&holder);
    try std.testing.expectEqual(State.running, scheduler.state(&older));
    try std.testing.expectEqual(State.queued, scheduler.state(&younger));
    scheduler.finish(&older);
    try std.testing.expectEqual(State.running, scheduler.state(&younger));
    // The barrier is a paid class admission, including the direct dispatch path.
    try std.testing.expect(scheduler.credits[1] < younger.slots);
    try std.testing.expectEqual(@as(u64, std.time.ns_per_ms / 4), (Config{ .max_wait_ms = 1 }).backfillAgeNs());
    try std.testing.expectEqual(@as(u64, 25 * std.time.ns_per_ms), (Config{ .max_wait_ms = 60000 }).backfillAgeNs());
}

test "runnable scheduler retires aged CPU barriers on original cancellation and deadline without releasing retained ownership" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    const Token = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    for ([_]bool{ false, true }) |cancel| {
        Clock.tick = 0;
        var canceled = false;
        var scheduler = try Scheduler.init(.{ .slots = 4, .weights = .{ 1, 1, 1, 1 }, .max_backfills = 65535 });
        var holder = testEntry(.write, 2, 19);
        var large = testEntry(.query, 3, 23);
        var small = testEntry(.query, 1, 29);
        var younger = testEntry(.query, 1, 31);
        large.options.native_now_ns = Clock.now;
        large.options.deadline_ns = scheduler.config.backfillAgeNs() + 1;
        large.options.cancellation = .{ .ptr = &canceled, .requested = Token.requested };
        defer scheduler.finish(&holder);
        defer scheduler.finish(&large);
        defer scheduler.finish(&small);
        defer scheduler.finish(&younger);
        try scheduler.submit(&holder);
        try scheduler.submit(&large);
        try scheduler.submit(&small);
        scheduler.finish(&small);
        Clock.tick = scheduler.config.backfillAgeNs();
        try scheduler.pollWait(&large);
        try scheduler.submit(&younger);
        try std.testing.expectEqual(State.queued, scheduler.state(&younger));
        if (cancel) canceled = true else Clock.tick += 1;
        try std.testing.expectError(if (cancel) error.Canceled else error.DeadlineExceeded, scheduler.waitRetained(&large));
        try std.testing.expectEqual(State.stopped, scheduler.state(&large));
        try std.testing.expectEqual(State.running, scheduler.state(&younger));
        try std.testing.expectEqual(@as(usize, 3), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 19 + 23 + 31), scheduler.stats().retained_bytes);
        try std.testing.expectEqual(@as(u64, 0), large.queued_since_ns);
        try std.testing.expect(scheduler.resource_barrier == null);
    }
}

test "runnable scheduler draining continuations preserve class shares and reserved team credit" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var scheduler = try Scheduler.init(.{ .slots = 4, .weights = .{ 1, 2, 1, 1 }, .max_backfills = 65535 });
    var owners: [3][24]Entry = undefined;
    for (&owners, [_]Class{ .query, .write, .inference }) |*lane, class| for (lane) |*e| {
        e.* = testEntry(class, 1, 0);
        e.options.native_now_ns = Clock.now;
    };
    defer for (&owners) |*lane| for (lane) |*e| scheduler.finish(e);
    for (&owners) |*lane| for (lane) |*e| {
        try scheduler.submit(e);
        try scheduler.block(e, .resource_blocked);
    };
    var holder = testEntry(.background, 2, 0);
    var large = testEntry(.query, 3, 0);
    var small = testEntry(.query, 1, 0);
    holder.options.native_now_ns = Clock.now;
    large.options.native_now_ns = Clock.now;
    small.options.native_now_ns = Clock.now;
    defer scheduler.finish(&holder);
    defer scheduler.finish(&large);
    defer scheduler.finish(&small);
    try scheduler.submit(&holder);
    try scheduler.submit(&large);
    try scheduler.submit(&small);
    scheduler.finish(&small);
    // Exercise the earned-round drain path, with the team's admission cost
    // already accumulated. Other tests cover age before that cost is earned.
    scheduler.credits[0] = large.slots;
    Clock.tick = scheduler.config.backfillAgeNs();
    try scheduler.pollWait(&large);
    const baseline = scheduler.stats().grants;
    for (&owners) |*lane| for (lane) |*e| try scheduler.unblock(e);
    for (0..40) |_| {
        const running = found: for (&owners) |*lane| {
            for (lane) |*e| if (scheduler.state(e) == .running) break :found e;
        } else return error.NoRunnableContinuation;
        scheduler.finish(running);
        try std.testing.expectEqual(State.queued, scheduler.state(&large));
        try std.testing.expect(scheduler.credits[0] >= large.slots);
        try std.testing.expect(scheduler.credits[0] <= scheduler.config.slots + scheduler.weight(0));
    }
    const grants = scheduler.stats().grants;
    const reads = grants[0] - baseline[0];
    const writes = grants[1] - baseline[1];
    const inference = grants[2] - baseline[2];
    try std.testing.expectEqual(@as(u64, 41), reads + writes + inference);
    try std.testing.expect(reads >= 9 and reads <= 12);
    try std.testing.expect(writes >= 19 and writes <= 22);
    try std.testing.expect(inference >= 9 and inference <= 12);
}

test "runnable scheduler aged paid barrier reconciles retained debt before its CPU grant" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var scheduler = try Scheduler.init(.{ .slots = 4, .weights = .{ 1, 1, 1, 1 }, .max_backfills = 65535 });
    var holder = testEntry(.write, 2, 0);
    var large = testEntry(.query, 3, 0);
    var small = testEntry(.query, 1, 0);
    defer scheduler.finish(&holder);
    defer scheduler.finish(&large);
    defer scheduler.finish(&small);
    large.options.native_now_ns = Clock.now;
    try scheduler.submit(&holder);
    try scheduler.submit(&large);
    try scheduler.submit(&small);
    scheduler.finish(&small);
    large.service_debt = 64;
    scheduler.credits[0] = large.slots;
    Clock.tick = scheduler.config.backfillAgeNs();
    try scheduler.pollWait(&large);
    try std.testing.expectEqual(State.queued, scheduler.state(&large));
    scheduler.finish(&holder);
    try std.testing.expectEqual(State.running, scheduler.state(&large));
    try std.testing.expectEqual(@as(usize, 0), large.service_debt);
    try std.testing.expectEqual(@as(u64, 64), scheduler.stats().read_service_debt_paid);
    try std.testing.expect(scheduler.credits[0] < large.slots);
}

test "runnable scheduler age observation never calls an original host clock under its owner lock" {
    const Clock = struct {
        var tick: u64 = 0;
        var owner: ?*Scheduler = null;
        var called_locked: bool = false;
        fn now() u64 {
            if (owner) |scheduler| {
                if (scheduler.mutex.tryLock()) scheduler.mutex.unlock() else called_locked = true;
            }
            return tick;
        }
    };
    Clock.tick = 0;
    Clock.called_locked = false;
    var scheduler = try Scheduler.init(.{ .slots = 4, .weights = .{ 1, 1, 1, 1 }, .max_backfills = 65535 });
    Clock.owner = &scheduler;
    defer Clock.owner = null;
    var holder = testEntry(.write, 2, 0);
    var large = testEntry(.query, 3, 0);
    var small = testEntry(.query, 1, 0);
    var younger = testEntry(.query, 1, 0);
    holder.options.native_now_ns = Clock.now;
    large.options.native_now_ns = Clock.now;
    small.options.native_now_ns = Clock.now;
    younger.options.native_now_ns = Clock.now;
    defer scheduler.finish(&holder);
    defer scheduler.finish(&large);
    defer scheduler.finish(&small);
    defer scheduler.finish(&younger);
    try scheduler.submit(&holder);
    try scheduler.submit(&large);
    try scheduler.submit(&small);
    scheduler.finish(&small);
    Clock.tick = scheduler.config.backfillAgeNs();
    try scheduler.pollWait(&large);
    try std.testing.expect(large.age_protected);
    try scheduler.submit(&younger);
    try std.testing.expectEqual(State.queued, scheduler.state(&younger));
    scheduler.finish(&holder);
    try std.testing.expectEqual(State.running, scheduler.state(&large));
    try std.testing.expect(!Clock.called_locked);
}

test "runnable measured service preserves class overrun debt across owning finish and new roots" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var scheduler = try Scheduler.init(.{ .slots = 2, .weights = .{ 1, 1, 1, 1 }, .quantum_ms = 1 });
    var expensive: Execution = .{};
    try expensive.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .query);
    Clock.tick = 5 * std.time.ns_per_ms;
    expensive.finish();
    const spent = scheduler.serviceStats();
    try std.testing.expectEqual(@as(u64, std.time.ns_per_ms), spent.estimated_slot_ns[0]);
    try std.testing.expectEqual(@as(u64, 5 * std.time.ns_per_ms), spent.observed_slot_ns[0]);
    try std.testing.expectEqual(-@as(i128, 4 * std.time.ns_per_ms), spent.adjustment_slot_ns[0]);
    var holder = testEntry(.background, 1, 0);
    var next = testEntry(.query, 1, 0);
    var peers: [5]Entry = undefined;
    for (&peers) |*e| e.* = testEntry(.write, 1, 0);
    defer scheduler.finish(&holder);
    defer scheduler.finish(&next);
    defer for (&peers) |*e| scheduler.finish(e);
    try scheduler.submit(&holder);
    try scheduler.submit(&next);
    for (&peers) |*e| try scheduler.submit(e);
    scheduler.finish(&holder);
    for (peers[0..4]) |*e| {
        try std.testing.expectEqual(State.running, scheduler.state(e));
        try std.testing.expectEqual(State.queued, scheduler.state(&next));
        scheduler.finish(e);
    }
    try std.testing.expectEqual(State.running, scheduler.state(&next));
    try std.testing.expectEqual(@as(u64, 4 * std.time.ns_per_ms), scheduler.serviceStats().debt_paid_slot_ns[0]);
}

test "runnable measured service refunds fractional estimates and excludes parked time" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var scheduler = try Scheduler.init(.{ .slots = 2, .weights = .{ 1, 1, 1, 1 }, .quantum_ms = 1 });
    var execution: Execution = .{};
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .query);
    Clock.tick = std.time.ns_per_ms / 4;
    try execution.park(.resource_blocked);
    try std.testing.expectEqual(@as(u64, std.time.ns_per_ms / 4), scheduler.serviceStats().observed_slot_ns[0]);
    try std.testing.expectEqual(@as(i128, 3 * std.time.ns_per_ms / 4), scheduler.serviceStats().adjustment_slot_ns[0]);
    Clock.tick += 100 * std.time.ns_per_s;
    try execution.enter(.query);
    Clock.tick += std.time.ns_per_ms / 4;
    execution.finish();
    const stats = scheduler.serviceStats();
    try std.testing.expectEqual(@as(u64, 2 * std.time.ns_per_ms), stats.estimated_slot_ns[0]);
    try std.testing.expectEqual(@as(u64, std.time.ns_per_ms / 2), stats.observed_slot_ns[0]);
    try std.testing.expectEqual(@as(u64, std.time.ns_per_ms), stats.refunded_slot_ns[0]);
    try std.testing.expectEqual(@as(i128, std.time.ns_per_ms / 2), stats.adjustment_slot_ns[0]);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "runnable measured service preserves weighted slot time for short and underestimated requests" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    for ([_]bool{ false, true }) |continuations| for ([_]u64{ std.time.ns_per_ms / 4, 4 * std.time.ns_per_ms }) |query_cost| {
        Clock.tick = 0;
        var scheduler = try Scheduler.init(.{ .slots = 2, .weights = .{ 2, 1, 1, 1 }, .quantum_ms = 1, .max_operations = 512, .max_wait_ms = 60000 });
        var holder = testEntry(.background, 1, 0);
        var queries: [256]Entry = undefined;
        var writes: [128]Entry = undefined;
        for (&queries) |*e| {
            e.* = testEntry(.query, 1, 0);
            e.options.native_now_ns = Clock.now;
        }
        for (&writes) |*e| {
            e.* = testEntry(.write, 1, 0);
            e.options.native_now_ns = Clock.now;
        }
        defer scheduler.finish(&holder);
        defer for (&queries) |*e| scheduler.finish(e);
        defer for (&writes) |*e| scheduler.finish(e);
        try scheduler.submit(&holder);
        for (queries[0..if (continuations) 1 else queries.len]) |*e| try scheduler.submit(e);
        for (writes[0..if (continuations) 1 else writes.len]) |*e| try scheduler.submit(e);
        scheduler.finish(&holder);
        var q: usize = 0;
        var w: usize = 0;
        for (0..90) |_| {
            const query_index = if (continuations) 0 else q;
            const write_index = if (continuations) 0 else w;
            const query = scheduler.state(&queries[query_index]) == .running;
            const running = if (query) &queries[query_index] else &writes[write_index];
            try std.testing.expectEqual(State.running, scheduler.state(running));
            try scheduler.waitRetained(running);
            Clock.tick += if (query) query_cost else std.time.ns_per_ms;
            if (continuations) try scheduler.yield(running) else scheduler.finish(running);
            if (query) q += 1 else w += 1;
            for (scheduler.credits, 0..) |credit, c| try std.testing.expect(credit <= scheduler.config.slots + scheduler.weight(c));
        }
        const observed = scheduler.serviceStats();
        const query_time = @as(u64, @intCast(q)) * query_cost;
        const write_time = @as(u64, @intCast(w)) * std.time.ns_per_ms;
        try std.testing.expectEqual(query_time, observed.observed_slot_ns[0]);
        try std.testing.expectEqual(write_time, observed.observed_slot_ns[1]);
        try std.testing.expect(write_time != 0);
        try std.testing.expect(query_time <= 2 * write_time + 8 * std.time.ns_per_ms);
        try std.testing.expect(2 * write_time <= query_time + 8 * std.time.ns_per_ms);
        if (query_cost < std.time.ns_per_ms) try std.testing.expect(observed.refunded_slot_ns[0] != 0) else try std.testing.expect(observed.debt_paid_slot_ns[0] != 0);
    };
}

test "runnable measured service samples team shrink without charging parked growth twice" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var scheduler = try Scheduler.init(.{ .slots = 5, .weights = .{ 1, 1, 1, 1 }, .quantum_ms = 1 });
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .query);
    Clock.tick += std.time.ns_per_ms;
    try execution.resizeTeam(3);
    Clock.tick += std.time.ns_per_ms;
    try execution.resizeTeam(1);
    Clock.tick += 2 * std.time.ns_per_ms;
    execution.finish();
    const stats = scheduler.serviceStats();
    try std.testing.expectEqual(@as(u64, 4 * std.time.ns_per_ms), stats.estimated_slot_ns[0]);
    try std.testing.expectEqual(@as(u64, 6 * std.time.ns_per_ms), stats.observed_slot_ns[0]);
    try std.testing.expectEqual(-@as(i128, 2 * std.time.ns_per_ms), stats.adjustment_slot_ns[0]);
    execution.finish();
    try std.testing.expectEqualDeep(stats, scheduler.serviceStats());
}

test "runnable measured service retains origin debt through class changes and CPU loans" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var scheduler = try Scheduler.init(.{ .slots = 3, .quantum_ms = 1 });
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .query);
    Clock.tick += 4 * std.time.ns_per_ms;
    try execution.park(.resource_blocked);
    try execution.enter(.write);
    Clock.tick += std.time.ns_per_ms;
    execution.finish();
    const stats = scheduler.serviceStats();
    try std.testing.expectEqual(@as(u64, 4 * std.time.ns_per_ms), stats.observed_slot_ns[0]);
    try std.testing.expectEqual(@as(u64, std.time.ns_per_ms), stats.observed_slot_ns[1]);
    try std.testing.expectEqual(-@as(i128, 3 * std.time.ns_per_ms), stats.adjustment_slot_ns[0]);
    try std.testing.expectEqual(@as(i128, 0), stats.adjustment_slot_ns[1]);
    var isolated = try Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2, .quantum_ms = 1 });
    var base = testEntry(.background, 1, 0);
    defer isolated.finish(&base);
    try isolated.submit(&base);
    var loan: Execution = .{};
    defer loan.finish();
    try loan.startBorrowable(&isolated, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .write, .{ .max_non_yield_ns = 1000 });
    try std.testing.expectEqual(@as(usize, 1), isolated.cpuBorrowStats().active);
    Clock.tick += 100;
    loan.finish();
    try std.testing.expectEqual(@as(u64, 100), isolated.serviceStats().observed_slot_ns[1]);
    try std.testing.expectEqual(@as(u64, 0), isolated.serviceStats().observed_slot_ns[4]);
    try std.testing.expectEqual(@as(usize, 0), isolated.cpuBorrowStats().active);
}

test "runnable measured service preserves independent completion cleanup and health progress while class debt remains" {
    const Foreground = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    const Completion = struct {
        fn now() u64 {
            return std.time.ns_per_s;
        }
    };
    Foreground.tick = 0;
    var scheduler = try Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .weights = .{ 1, 1, 1, 1 }, .quantum_ms = 1 });
    var write: Execution = .{};
    defer write.finish();
    try write.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Foreground.now }, .write);
    try write.reserveCompletion(.{ .io = std.testing.io, .native_now_ns = Completion.now, .deadline_ns = 2 * std.time.ns_per_s });
    Foreground.tick = 5 * std.time.ns_per_ms;
    try write.activateCompletion();
    try write.park(.resource_blocked);
    var holder = testEntry(.query, 2, 0);
    var fresh = testEntry(.write, 1, 0);
    var health = testEntry(.control, 1, 0);
    defer scheduler.finish(&holder);
    defer scheduler.finish(&fresh);
    defer scheduler.finish(&health);
    try scheduler.submit(&holder);
    try scheduler.submit(&fresh);
    try scheduler.unblock(&write.entry.?);
    try scheduler.submit(&health);
    try std.testing.expectEqual(State.running, scheduler.state(&health));
    var cleanup: Execution = .{};
    defer cleanup.finish();
    try cleanup.startCleanupRoot(&scheduler, .{ .io = std.testing.io, .native_now_ns = Completion.now, .deadline_ns = std.time.ns_per_s + 100 * std.time.ns_per_ms, .allow_wait = false });
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().cleanup_runnable_slots);
    scheduler.finish(&holder);
    try std.testing.expectEqual(State.running, scheduler.state(&fresh));
    try std.testing.expectEqual(State.running, scheduler.state(&write.entry.?));
    try scheduler.waitRetained(&write.entry.?);
    write.finish();
    const stats = scheduler.serviceStats();
    try std.testing.expectEqual(@as(u64, 5 * std.time.ns_per_ms), stats.observed_slot_ns[1]);
    try std.testing.expectEqual(-@as(i128, 4 * std.time.ns_per_ms), stats.adjustment_slot_ns[1]);
    try std.testing.expectEqual(@as(u64, 0), stats.debt_paid_slot_ns[1]);
}

test "runnable measured service bounds extreme work and preserves estimates for unobserved invalid and zero clocks" {
    const Mode = enum { unobserved, zero, backwards, extreme };
    const Clock = struct {
        var tick: u64 = 0;
        var retired: bool = false;
        fn now() u64 {
            std.debug.assert(!retired);
            return tick;
        }
    };
    for (std.enums.values(Mode)) |mode| {
        Clock.tick = if (mode == .backwards) 10 * std.time.ns_per_ms else 0;
        Clock.retired = false;
        var scheduler = try Scheduler.init(.{ .slots = 3, .quantum_ms = 1 });
        var item = testEntry(.query, 2, 0);
        item.options.native_now_ns = Clock.now;
        try scheduler.submit(&item);
        if (mode != .unobserved) try scheduler.waitRetained(&item);
        Clock.tick = if (mode == .extreme) std.math.maxInt(u64) else if (mode == .unobserved) 5 * std.time.ns_per_ms else 0;
        scheduler.finish(&item);
        const stats = scheduler.serviceStats();
        if (mode == .extreme) {
            try std.testing.expectEqual(-@as(i128, 64 * std.time.ns_per_ms), stats.adjustment_slot_ns[0]);
            try std.testing.expectEqual(@as(u64, 1), stats.clipped_adjustments[0]);
            try std.testing.expectEqual(std.math.maxInt(u64), stats.observed_slot_ns[0]);
        } else {
            try std.testing.expectEqual(@as(u64, 2 * std.time.ns_per_ms), stats.unobserved_estimate_ns[0]);
            try std.testing.expectEqual(@as(i128, 0), stats.adjustment_slot_ns[0]);
        }
        Clock.retired = true;
        scheduler.finish(&item);
        try std.testing.expectEqualDeep(stats, scheduler.serviceStats());
    }
}

test "runnable measured service refunds short cooperative yields without an artificial quantum floor" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var scheduler = try Scheduler.init(.{ .slots = 2, .quantum_ms = 1 });
    var query: Execution = .{};
    defer query.finish();
    try query.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .query);
    for (0..2) |_| {
        Clock.tick += 100;
        try scheduler.yield(&query.entry.?);
        try scheduler.waitRetained(&query.entry.?);
    }
    try std.testing.expectEqual(@as(u64, 200), scheduler.serviceStats().observed_slot_ns[0]);
    try std.testing.expectEqual(@as(u64, std.time.ns_per_ms), scheduler.serviceStats().refunded_slot_ns[0]);
    Clock.tick += 100;
    query.finish();
    const stats = scheduler.serviceStats();
    try std.testing.expectEqual(@as(u64, 300), stats.observed_slot_ns[0]);
    try std.testing.expectEqual(@as(u64, 2 * std.time.ns_per_ms), stats.refunded_slot_ns[0]);
    try std.testing.expectEqual(@as(i128, std.time.ns_per_ms - 300), stats.adjustment_slot_ns[0]);
}

test "runnable measured service protects earned team credit while an overrunning continuation retires" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var scheduler = try Scheduler.init(.{ .slots = 4, .weights = .{ 1, 1, 1, 1 }, .quantum_ms = 1, .max_backfills = 65535 });
    var owner: Execution = .{};
    defer owner.finish();
    var holder = testEntry(.write, 2, 0);
    var team = testEntry(.query, 3, 0);
    team.options.native_now_ns = Clock.now;
    defer scheduler.finish(&holder);
    defer scheduler.finish(&team);
    try owner.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .query);
    try owner.park(.resource_blocked);
    try scheduler.submit(&holder);
    try scheduler.submit(&team);
    scheduler.credits[0] = team.slots;
    Clock.tick = scheduler.config.backfillAgeNs();
    try scheduler.pollWait(&team);
    try owner.enter(.query);
    Clock.tick += 5 * std.time.ns_per_ms;
    try owner.park(.resource_blocked);
    try std.testing.expectEqual(State.queued, scheduler.state(&team));
    try std.testing.expectEqual(team.slots, scheduler.credits[0]);
    try std.testing.expectEqual(-@as(i128, 4 * std.time.ns_per_ms), scheduler.serviceStats().adjustment_slot_ns[0]);
    scheduler.finish(&holder);
    try std.testing.expectEqual(State.running, scheduler.state(&team));
    try std.testing.expectEqual(@as(u64, 4 * std.time.ns_per_ms), scheduler.serviceStats().debt_paid_slot_ns[0]);
}
