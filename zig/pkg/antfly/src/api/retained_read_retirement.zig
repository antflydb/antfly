// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Bounded node-owned obligations, reserved before remote publication. The
//! publisher supplies an owned parent pin/charge, never a request-stack Port.
//! One node consumer polls idempotent retirement; no worker is created per read.
const std = @import("std");
const control_gate = @import("remote_control_admission.zig").Gate;
const http = @import("../raft/transport/http_common.zig");
const admission = @import("antfly_local_sources").common_read_admission;

const core = admission.Retirement;
pub const Parent = core.Parent;
pub const Outcome = core.Outcome;
pub const Poll = core.Poll;
pub const Resource = core.Resource;
pub const Binding = core.Binding;
pub const Registry = core.Registry;

/// Fixed node ticket metadata and payload ceilings. Admission fails within
/// these bounds; there is no task or worker allocation per connection/query.
pub const Policy = struct {
    capacity: usize = 128,
    bytes: usize = 8 << 20,
    item_bytes: usize = 64 << 10,
};

/// Bootstrap checks this before durable enrollment/publication. Both consumers
/// leave protected cleanup and health byte/operation capacity inside the policy.
pub fn validateNodeCapacity(config: @import("antfly_local_sources").common_request_admission.runnable_scheduler.Config, policy: Policy, capability_bytes: u64) !void {
    if (policy.item_bytes < Registry.slot_bytes or policy.item_bytes > policy.bytes) return error.InvalidArgument;
    const bytes = try std.math.add(u64, try std.math.add(u64, capability_bytes, try control_gate.reservationBytes(config.max_operations)), try std.math.add(u64, try Runtime.reservationBytes(policy.capacity), @import("remote_reconciliation.zig").Runtime.reservationBytes()));
    if (@min(@as(usize, 4), config.max_operations / 2) < 3 or bytes > config.control_bytes / 4) return error.CleanupCapacityUnavailable;
}

/// One consumer on an independently owned node executor. A stopped consumer
/// may join with outstanding obligations, but cannot destroy their owner.
pub const Runtime = struct {
    registry: Registry,
    baseline: scheduling.Scheduler.ControlBaseline,
    io: std.Io,
    scheduler: *@import("antfly_local_sources").common_request_admission.runnable_scheduler.Scheduler,
    future: std.Io.Future(void),
    stopping: std.atomic.Value(bool) = .init(false),
    joined: bool = false,
    control_admission: ?*control_gate = null,
    pin_mutex: std.atomic.Mutex = .unlocked,
    pins: usize = 0,
    pin_admission_closed: bool = false,
    const scheduling = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const clock = @import("antfly_platform").time;
    const scratch_bytes = 64 * 1024;
    pub fn create(allocator: std.mem.Allocator, io: std.Io, scheduler: *scheduling.Scheduler, capacity: usize, bytes: usize, item_bytes: usize) !*Runtime {
        return createWithControlAdmission(allocator, io, scheduler, capacity, bytes, item_bytes, null);
    }
    pub fn createWithControlAdmission(allocator: std.mem.Allocator, io: std.Io, scheduler: *scheduling.Scheduler, capacity: usize, bytes: usize, item_bytes: usize, gate: ?*control_gate) !*Runtime {
        if (@min(@as(usize, 4), scheduler.config.max_operations / 2) < 2 or
            @sizeOf(Runtime) + @sizeOf(Round) > scheduler.config.control_bytes / 2) return error.CleanupCapacityUnavailable;
        const reserved_bytes = try reservationBytes(capacity);
        var baseline = scheduler.reserveControlBaseline(reserved_bytes) catch |err| return if (err == error.SchedulerClosed) err else error.CleanupCapacityUnavailable;
        errdefer baseline.release();
        const self = try allocator.create(Runtime);
        errdefer allocator.destroy(self);
        self.* = .{ .registry = try Registry.init(allocator, capacity, bytes, item_bytes), .baseline = baseline, .io = io, .scheduler = scheduler, .future = undefined, .control_admission = gate };
        errdefer {
            self.registry.close();
            self.registry.deinit() catch unreachable;
        }
        self.future = try io.concurrent(run, .{self});
        return self;
    }
    pub fn reservationBytes(capacity: usize) !u64 {
        return std.math.add(u64, @sizeOf(Runtime) + @sizeOf(Round) + @sizeOf(admission.Payload) + @sizeOf(std.heap.FixedBufferAllocator) + @sizeOf(@import("remote_endpoint_source.zig").Source.Control) + @sizeOf(?control_gate.Lease) + @sizeOf(http.RequestCancellation), try Registry.metadataBytes(capacity));
    }
    pub fn reservedBytes(self: *const Runtime) u64 {
        return self.baseline.bytes;
    }
    const Round = struct {
        runtime: *Runtime,
        execution: scheduling.Execution = .{},
        deadline_ns: u64,
        scratch: [scratch_bytes]u8 = undefined,
        fn life(raw: *anyopaque) !void {
            const self: *Round = @ptrCast(@alignCast(raw));
            if (self.runtime.stopping.load(.acquire)) return error.Canceled;
            try self.execution.checkLifetime();
            if (clock.monotonicNs() >= self.deadline_ns) return error.DeadlineExceeded;
        }
        fn check(raw: *anyopaque) !void {
            const self: *Round = @ptrCast(@alignCast(raw));
            try life(raw);
            try self.execution.checkpoint();
        }
        fn begin(raw: *anyopaque) !void {
            const self: *Round = @ptrCast(@alignCast(raw));
            try life(raw);
            try self.execution.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *Round = @ptrCast(@alignCast(raw));
            try life(raw);
            try self.execution.enter(.control);
        }
        fn deadline(raw: *anyopaque) !?u64 {
            return @as(*Round, @ptrCast(@alignCast(raw))).deadline_ns;
        }
    };
    fn pass(self: *Runtime) void {
        const work_ns = 100 * std.time.ns_per_ms;
        const started = clock.monotonicNs();
        const parent_deadline = started +| control_gate.max_wait_ns +| work_ns;
        var waiting: http.RequestCancellation = .{ .borrowed = &self.stopping, .execution_deadline_ns = parent_deadline };
        var permit = if (self.control_admission) |gate|
            gate.acquireIdle(.{ .deadline_ns = started +| control_gate.max_wait_ns, .cancellation = &waiting }) catch return
        else
            null;
        defer if (permit) |*held| held.release();
        var round: Round = .{ .runtime = self, .deadline_ns = @min(parent_deadline, clock.monotonicNs() +| work_ns) };
        round.execution.startCleanupRoot(self.scheduler, .{ .io = self.io, .native_now_ns = clock.monotonicNs, .deadline_ns = round.deadline_ns, .retained_bytes = 0, .allow_wait = false }) catch return;
        defer round.execution.finish();
        const payload: admission.Payload = .{ .ptr = &round, .vtable = &.{ .checkpoint = Round.check, .check_lifetime = Round.life, .deadline_ns = Round.deadline }, .execution_wait = .{ .ptr = &round, .begin = Round.begin, .end = Round.end } };
        var scratch = std.heap.FixedBufferAllocator.init(&round.scratch);
        _ = self.registry.pollOne(.{ .allocator = scratch.allocator(), .control = .init(&payload), .control_permit = if (permit) |*held| held.borrow() else null });
        scratch.reset();
        if (clock.monotonicNs() < round.deadline_ns)
            _ = self.registry.reconcileOne(.{ .allocator = scratch.allocator(), .control = .init(&payload), .control_permit = if (permit) |*held| held.borrow() else null });
    }
    fn run(self: *Runtime) void {
        while (!self.stopping.load(.acquire)) {
            self.pass();
            self.io.sleep(.fromMilliseconds(10), .awake) catch return;
        }
    }
    fn lockPins(self: *Runtime) void {
        while (!self.pin_mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn retainPin(raw: *anyopaque) !void {
        const self: *Runtime = @ptrCast(@alignCast(raw));
        self.lockPins();
        defer self.pin_mutex.unlock();
        if (self.pin_admission_closed) return error.CleanupCapacityUnavailable;
        if (self.pins == std.math.maxInt(usize)) return error.CleanupCapacityUnavailable;
        self.pins += 1;
    }
    fn releasePin(raw: *anyopaque) void {
        const self: *Runtime = @ptrCast(@alignCast(raw));
        self.lockPins();
        defer self.pin_mutex.unlock();
        std.debug.assert(self.pins != 0);
        self.pins -= 1;
    }
    fn parentsPending(self: *Runtime) bool {
        self.lockPins();
        defer self.pin_mutex.unlock();
        return self.pins != 0;
    }
    pub fn binding(self: *Runtime, parent: Parent) !Binding {
        if (parent.pin != null) return error.InvalidArgument;
        self.lockPins();
        defer self.pin_mutex.unlock();
        if (self.pin_admission_closed) return error.CleanupCapacityUnavailable;
        var wrapped = parent;
        wrapped.pin = .{ .ptr = self, .retain = retainPin, .release = releasePin };
        return .{ .registry = &self.registry, .parent = wrapped };
    }

    /// Fence new publication while the independent consumer remains live.
    /// Existing tickets, parent pins and reconciliation still belong here.
    pub fn closeAdmission(self: *Runtime) void {
        self.lockPins();
        self.pin_admission_closed = true;
        self.pin_mutex.unlock();
        self.registry.close();
    }

    /// Bounded observation, never cancellation of the physical consumer.
    /// Expiry leaves the runtime, allocator, scheduler and executor owned by
    /// the caller; only drained/joined state may be destroyed. Before final
    /// destruction the node must also fence every borrower of a Binding.
    pub fn waitDrainedUntil(self: *Runtime, observer_io: std.Io, deadline: std.Io.Clock.Timestamp) !void {
        if (deadline.clock != .awake) return error.InvalidArgument;
        self.closeAdmission();
        try observer_io.checkCancel();
        while (self.registry.snapshot().retained != 0 or self.parentsPending()) {
            try observer_io.checkCancel();
            if (deadline.raw.nanoseconds <= deadline.clock.now(observer_io).nanoseconds) return error.CleanupPending;
            try observer_io.sleep(.fromMilliseconds(1), .awake);
        }
        try observer_io.checkCancel();
    }

    /// Serialized by the node supervisor, after fencing foreground borrowers.
    /// Refusing a pending join MUST leave the consumer running: a later exact
    /// ACK or generation proof can then drain it without creating a new scope,
    /// dropping metadata or starting another physical worker.
    pub fn join(self: *Runtime) !void {
        if (!self.joined) {
            self.closeAdmission();
            if (self.registry.snapshot().retained != 0 or self.parentsPending()) return error.CleanupPending;
            self.stopping.store(true, .release);
            const protection = self.io.swapCancelProtection(.blocked);
            defer _ = self.io.swapCancelProtection(protection);
            self.future.await(self.io);
            self.joined = true;
        }
        if (self.registry.snapshot().retained != 0 or self.parentsPending()) return error.CleanupPending;
    }
    pub fn destroyJoined(self: *Runtime) !void {
        if (!self.joined) return error.InvalidRunnableTransition;
        if (self.parentsPending()) return error.CleanupPending;
        const allocator = self.registry.allocator;
        try self.registry.deinit();
        var baseline = self.baseline;
        allocator.destroy(self);
        baseline.release();
    }
};

test "retained read owner retirement reservations bound metadata and fairly retain unavailable peers" {
    const Fixture = struct {
        owner: *Registry,
        pins: usize = 0,
        releases: usize = 0,
        destroyed: usize = 0,
        fail_pin: bool = false,
        close_on_pin: bool = false,
        fn retain(raw: *anyopaque, _: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail_pin) return error.TestParentRejected;
            self.pins += 1;
            if (self.close_on_pin) self.owner.close();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.owner.snapshot().retained > 0);
            self.pins -= 1;
            self.releases += 1;
        }
    };
    const ResourceFixture = struct {
        parent: *Fixture,
        pending: bool,
        calls: usize = 0,
        fn poll(raw: *anyopaque, _: Poll) !Outcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return if (self.pending) .pending else .retired;
        }
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.parent.owner.snapshot().retained > 0 and self.parent.pins > 0);
            self.parent.destroyed += 1;
        }
    };
    const item = Registry.slot_bytes + 64;
    var owner = try Registry.init(std.testing.allocator, 2, 2 * item, item);
    defer {
        owner.close();
        owner.deinit() catch unreachable;
    }
    var parent: Fixture = .{ .owner = &owner };
    const pin: Parent = .{ .ptr = &parent, .retain = Fixture.retain, .release = Fixture.release };
    parent.fail_pin = true;
    try std.testing.expectError(error.TestParentRejected, owner.reserve(pin, 64));
    try std.testing.expectEqual(@as(usize, 0), owner.snapshot().retained);
    parent.fail_pin = false;
    try std.testing.expectError(error.CleanupCapacityUnavailable, owner.reserve(pin, 65));
    var first: ResourceFixture = .{ .parent = &parent, .pending = true };
    var second: ResourceFixture = .{ .parent = &parent, .pending = false };
    const a = try owner.reserve(pin, 64);
    const b = try owner.reserve(pin, 64);
    try a.bind(.{ .ptr = &first, .poll = ResourceFixture.poll, .destroy = ResourceFixture.destroy });
    try b.bind(.{ .ptr = &second, .poll = ResourceFixture.poll, .destroy = ResourceFixture.destroy });
    try a.publish();
    try b.publish();
    try std.testing.expectError(error.InvalidRunnableTransition, a.abort());
    try std.testing.expectError(error.CleanupCapacityUnavailable, owner.reserve(pin, 1));
    try std.testing.expectEqual(@as(usize, 2), parent.pins);
    try a.transfer();
    try b.transfer();
    try std.testing.expectEqual(Registry.Progress.pending, owner.pollOne(.{ .allocator = std.testing.allocator }));
    try std.testing.expectEqual(Registry.Progress.retired, owner.pollOne(.{ .allocator = std.testing.allocator }));
    try std.testing.expectEqual(@as(usize, 1), first.calls);
    try std.testing.expectEqual(@as(usize, 1), second.calls);
    try std.testing.expectEqual(@as(usize, 1), parent.pins);
    first.pending = false;
    try std.testing.expectEqual(Registry.Progress.retired, owner.pollOne(.{ .allocator = std.testing.allocator }));
    try std.testing.expectEqual(@as(usize, 2), parent.destroyed);
    try std.testing.expectEqual(@as(usize, 0), owner.snapshot().bytes);
    const replacement = try owner.reserve(pin, 1);
    try std.testing.expectError(error.InvalidRunnableTransition, a.transfer());
    try replacement.abort();
    const unstarted = try owner.reserve(pin, 1);
    try unstarted.bind(.{ .ptr = &first, .poll = ResourceFixture.poll, .destroy = ResourceFixture.destroy });
    owner.close();
    try std.testing.expectError(error.CleanupCapacityUnavailable, unstarted.publish());
    try unstarted.abort();
    var closing = try Registry.init(std.testing.allocator, 1, item, item);
    defer {
        closing.close();
        closing.deinit() catch unreachable;
    }
    parent.owner = &closing;
    parent.close_on_pin = true;
    try std.testing.expectError(error.CleanupCapacityUnavailable, closing.reserve(pin, 1));
    try std.testing.expectEqual(@as(usize, 0), closing.snapshot().retained);
    try std.testing.expectEqual(@as(usize, 0), parent.pins);
}

test "retained read owner node drain retains its charged consumer through pending join and ordinary shutdown" {
    const scheduling = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const clock = @import("antfly_platform").time;
    const Shared = @import("../common/allocation_budget.zig").Shared;
    const io = std.testing.io;
    const Block = struct {
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        fn run(self: *@This(), worker_io: std.Io) void {
            self.entered.set(worker_io);
            self.release.wait(worker_io) catch {};
        }
    };
    const ParentState = struct {
        pins: std.atomic.Value(usize) = .init(0),
        releases: std.atomic.Value(usize) = .init(0),
        fn retain(raw: *anyopaque, _: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.pins.fetchAdd(1, .acq_rel);
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.releases.fetchAdd(1, .acq_rel);
            std.debug.assert(self.pins.fetchSub(1, .acq_rel) != 0);
        }
    };
    const State = struct {
        allocator: std.mem.Allocator,
        runtime: *Runtime,
        scheduler: *scheduling.Scheduler,
        parent: *ParentState,
        needs_fence: bool,
        allow_retirement: *std.atomic.Value(bool),
        health: *std.atomic.Value(bool),
        health_calls: *std.atomic.Value(usize),
        after_refusal: *std.atomic.Value(bool),
        continued: *std.Io.Event,
        destroyed: *std.atomic.Value(usize),
        fn poll(raw: *anyopaque, round: Poll) !Outcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const control = round.control orelse return error.TestMissingCleanupAuthority;
            try control.checkLifetime();
            try std.testing.expect((try control.deadlineNs()).? <= clock.monotonicNs() +| 100 * std.time.ns_per_ms);
            if (self.health.load(.acquire)) {
                const wait = (try control.executionWait()) orelse return error.TestMissingCleanupIoLoan;
                try wait.run(probeHealth, .{self});
            }
            if (self.after_refusal.load(.acquire)) self.continued.set(std.testing.io);
            return if (self.allow_retirement.load(.acquire)) .retired else if (self.needs_fence) .needs_fence else .pending;
        }
        fn probeHealth(self: *@This()) !void {
            var health: scheduling.Execution = .{};
            defer health.finish();
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            _ = self.health_calls.fetchAdd(1, .acq_rel);
        }
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.parent.pins.load(.acquire) != 0);
            std.debug.assert(self.runtime.registry.snapshot().retained != 0);
            _ = self.destroyed.fetchAdd(1, .acq_rel);
            const allocator = self.allocator;
            allocator.destroy(self);
        }
    };
    var workers = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer workers.deinit();
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .max_operations = 4, .max_bytes = 4 << 20, .control_bytes = 1 << 20 });
    defer scheduler.close();
    var memory: Shared = .{ .child = std.testing.allocator, .limit = 1 };
    // Both baseline allocation and an actual independent executor are required
    // before node publication. A failed start leaves neither metadata nor pins.
    try std.testing.expectError(error.OutOfMemory, Runtime.create(memory.allocator(), workers.io(), &scheduler, 2, 4096, 2048));
    try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
    memory.limit = 1 << 20;
    var block: Block = .{};
    var occupied = try workers.io().concurrent(Block.run, .{ &block, workers.io() });
    var occupied_live = true;
    defer if (occupied_live) {
        block.release.set(io);
        occupied.await(workers.io());
    };
    try block.entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectError(error.ConcurrencyUnavailable, Runtime.create(memory.allocator(), workers.io(), &scheduler, 2, 4096, 2048));
    try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
    block.release.set(io);
    occupied.await(workers.io());
    occupied_live = false;
    const owner = try Runtime.create(memory.allocator(), workers.io(), &scheduler, 2, 4096, 2048);
    var owner_live = true;
    var allow_retirement: [2]std.atomic.Value(bool) = .{ .init(false), .init(false) };
    var parents: ParentState = .{};
    var health: std.atomic.Value(bool) = .init(true);
    var health_calls: std.atomic.Value(usize) = .init(0);
    var after_refusal: std.atomic.Value(bool) = .init(false);
    var continued: std.Io.Event = .unset;
    var destroyed: std.atomic.Value(usize) = .init(0);
    const baseline = memory.used.load(.acquire);
    try std.testing.expect(scheduler.stats().control_retained_bytes >= owner.reservedBytes());
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expect(baseline >= @sizeOf(Runtime) + 2 * Registry.slot_bytes);
    defer if (owner_live) {
        health.store(false, .release);
        for (&allow_retirement) |*allow| allow.store(true, .release);
        const limit = std.Io.Clock.Timestamp.now(io, .awake).addDuration(.{ .raw = .fromSeconds(5), .clock = .awake });
        owner.waitDrainedUntil(io, limit) catch unreachable;
        owner.join() catch unreachable;
        owner.destroyJoined() catch unreachable;
    };
    for (&allow_retirement, 0..) |*allow, i| {
        const ticket = try owner.registry.reserve(.{ .ptr = &parents, .retain = ParentState.retain, .release = ParentState.release }, @sizeOf(State));
        var transferred = false;
        defer if (!transferred) ticket.abandon() catch unreachable;
        const state = try ticket.allocator().create(State);
        var bound = false;
        defer if (!bound) ticket.allocator().destroy(state);
        state.* = .{ .allocator = ticket.allocator(), .runtime = owner, .scheduler = &scheduler, .parent = &parents, .needs_fence = i == 1, .allow_retirement = allow, .health = &health, .health_calls = &health_calls, .after_refusal = &after_refusal, .continued = &continued, .destroyed = &destroyed };
        try ticket.bind(.{ .ptr = state, .poll = State.poll, .reconcile = State.poll, .destroy = State.destroy });
        bound = true;
        try ticket.publish();
        try ticket.transfer();
        transferred = true;
    }
    try std.testing.expectEqual(@as(usize, 2), parents.pins.load(.acquire));
    const live_bytes = memory.used.load(.acquire);
    try std.testing.expect(live_bytes >= baseline + 2 * @sizeOf(State));
    try std.testing.expectError(error.CleanupPending, owner.join());
    try std.testing.expect(!owner.joined and !owner.stopping.load(.acquire));
    try std.testing.expectError(error.InvalidRunnableTransition, owner.destroyJoined());
    try std.testing.expectError(error.CleanupCapacityUnavailable, owner.registry.reserve(.{ .ptr = &parents, .retain = ParentState.retain, .release = ParentState.release }, 1));
    try std.testing.expectEqual(@as(usize, 2), parents.pins.load(.acquire));
    const expired: std.Io.Clock.Timestamp = .{ .raw = .{ .nanoseconds = 0 }, .clock = .awake };
    try std.testing.expectError(error.CleanupPending, owner.waitDrainedUntil(io, expired));
    try std.testing.expect(!owner.stopping.load(.acquire));
    try std.testing.expectEqual(live_bytes, memory.used.load(.acquire));
    const Observer = struct {
        entered: std.Io.Event = .unset,
        fn run(self: *@This(), runtime: *Runtime, observer_io: std.Io) !void {
            self.entered.set(observer_io);
            const until = std.Io.Clock.Timestamp.now(observer_io, .awake).addDuration(.{ .raw = .fromSeconds(5), .clock = .awake });
            try runtime.waitDrainedUntil(observer_io, until);
        }
    };
    var observer_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer observer_threads.deinit();
    var observer: Observer = .{};
    var observed = try observer_threads.io().concurrent(Observer.run, .{ &observer, owner, observer_threads.io() });
    var observer_live = true;
    defer if (observer_live) {
        _ = observed.cancel(observer_threads.io()) catch {};
    };
    try observer.entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    const observer_result = observed.cancel(observer_threads.io());
    observer_live = false;
    try std.testing.expectError(error.Canceled, observer_result);
    try std.testing.expect(!owner.stopping.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), parents.pins.load(.acquire));
    try std.testing.expectEqual(live_bytes, memory.used.load(.acquire));
    after_refusal.store(true, .release);
    try continued.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expect(health_calls.load(.acquire) != 0);
    // Closing ordinary admission must not stop queued retirement or the
    // needs_fence reconciliation pass. Both retain their finite control root.
    health.store(false, .release);
    scheduler.close();
    for (&allow_retirement) |*allow| allow.store(true, .release);
    const limit = std.Io.Clock.Timestamp.now(io, .awake).addDuration(.{ .raw = .fromSeconds(5), .clock = .awake });
    try owner.waitDrainedUntil(io, limit);
    try std.testing.expectEqual(@as(usize, 2), destroyed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), parents.releases.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), parents.pins.load(.acquire));
    try std.testing.expectEqual(baseline, memory.used.load(.acquire));
    try owner.join();
    try std.testing.expectEqual(owner.reservedBytes(), scheduler.stats().retained_bytes);
    try owner.destroyJoined();
    owner_live = false;
    try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}
