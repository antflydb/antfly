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

const std = @import("std");
const CheckBoundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct {
    check_fn: *const fn (?*anyopaque) anyerror!void,
});
const platform_time = @import("antfly_platform").time;

/// Stable phases shared by API, enrichment, model loading, pipelines, and
/// backends. Progress is advisory; cancellation and deadlines are mandatory.
pub const Phase = enum(u8) {
    queued,
    loading_model,
    loading_weights,
    preparing_weights,
    tokenizing,
    executing,
    serializing,
    publishing,
};

pub const Progress = struct {
    phase: Phase,
    completed: u64 = 0,
    total: u64 = 0,
    model: []const u8 = "",
    backend: []const u8 = "",
};

pub const Cancellation = struct {
    ptr: ?*anyopaque = null,
    is_cancelled_fn: *const fn (?*anyopaque) bool,

    pub fn isCancelled(self: Cancellation) bool {
        return self.is_cancelled_fn(self.ptr);
    }
};

pub const ProgressSink = struct {
    ptr: ?*anyopaque = null,
    update_fn: *const fn (?*anyopaque, Progress) void,

    pub fn update(self: ProgressSink, progress: Progress) void {
        self.update_fn(self.ptr, progress);
    }
};

/// Describes the strongest interruption boundary required by an execution
/// primitive. Uninterruptible calls require either a supervised boundary or
/// an explicit in-process policy from the runtime owner.
pub const Interruption = enum {
    cooperative,
    uninterruptible,
};

/// Allocation-free view retained by a process watchdog while native code is
/// executing. The enclosing guard guarantees every borrowed pointer remains
/// alive until `disarm` returns.
pub const MonitorControl = struct {
    deadline_ns: ?u64 = null,
    cancellation: ?Cancellation = null,
    cancellation_grace_ns: ?u64 = null,
    ptr: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,
    check_dispatch: CheckBoundary.Dispatch = CheckBoundary.local_dispatch,

    pub fn check(self: MonitorControl) !void {
        if (self.deadline_ns) |deadline_ns| {
            if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
        }
        if (self.check_fn) |check_fn| try CheckBoundary.call("check_fn", self.check_dispatch, check_fn, .{self.ptr});
        if (self.cancellation) |token| {
            if (token.isCancelled()) return error.Cancelled;
        }
    }
};

/// Chosen by the runtime owner, never by a process-global exception. Embedded
/// callers retain buffers and sessions until native calls return. Server callers
/// require a real watchdog boundary for calls which cannot stop cooperatively.
pub const NativeCallPolicy = enum {
    require_supervision,
    in_process,
};

pub const HardCancellationBoundary = struct {
    ptr: *anyopaque,
    arm_fn: *const fn (*anyopaque, MonitorControl) anyerror!u64,
    disarm_fn: *const fn (*anyopaque, u64) void,

    fn arm(self: HardCancellationBoundary, control: MonitorControl) !u64 {
        return self.arm_fn(self.ptr, control);
    }

    fn disarm(self: HardCancellationBoundary, token: u64) void {
        self.disarm_fn(self.ptr, token);
    }
};

pub const UninterruptibleGuard = struct {
    boundary: ?HardCancellationBoundary = null,
    token: u64 = 0,

    pub fn deinit(self: *UninterruptibleGuard) void {
        const boundary = self.boundary orelse return;
        self.boundary = null;
        boundary.disarm(self.token);
    }
};

/// Request-scoped execution contract. `deadline_ns` is always in the process
/// monotonic clock domain. The legacy check hook is retained so existing
/// callers can migrate without weakening cancellation coverage.
pub const InferenceExecutionControl = struct {
    document_store: @import("antfly_runtime_abi").document_abi.View = .{},
    document_session: ?*@import("antfly_readers").document_runs.Session = null,
    document_dispatch_meter: ?*@import("antfly_readers").document_runs.DispatchMeter = null,
    /// Runtime used for cancellation monitors and other structured background
    /// work. HTTP requests always supply their owning runtime; library callers
    /// may omit it when no monitor is required.
    io: ?std.Io = null,
    deadline_ns: ?u64 = null,
    cancellation: ?Cancellation = null,
    progress: ?ProgressSink = null,
    ptr: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,
    check_dispatch: CheckBoundary.Dispatch = CheckBoundary.local_dispatch,
    /// Present only inside a worker process whose parent owns restart. It is
    /// deliberately request-scoped; a boolean "running out of process" flag
    /// cannot notice that one particular request was cancelled while blocked
    /// in a driver.
    hard_cancellation: ?HardCancellationBoundary = null,
    native_call_policy: NativeCallPolicy = .require_supervision,
    /// A cancelled request may finish an in-flight native call before the
    /// watchdog replaces the worker. Checks outside native calls stay immediate.
    cancellation_grace_ns: ?u64 = null,

    /// Borrow this control for structured image work. The caller must retain
    /// it until all preprocessing workers have joined.
    pub fn imageWorkControl(self: *const InferenceExecutionControl) @import("antfly_image").work_control.Control {
        const Adapter = struct {
            fn check(raw: ?*const anyopaque) !void {
                const control: *const InferenceExecutionControl = @ptrCast(@alignCast(raw.?));
                try control.check();
            }
        };
        return .{ .context = self, .check_fn = Adapter.check };
    }

    /// Replace a checkpoint in this compilation unit while retaining the
    /// request's IO, watchdog, deadline and progress capabilities. A callback
    /// and its error-domain dispatcher must always change together.
    pub fn withCheckpoint(self: InferenceExecutionControl, ptr: ?*anyopaque, callback: *const fn (?*anyopaque) anyerror!void) InferenceExecutionControl {
        var result = self;
        result.ptr = ptr;
        result.check_fn = callback;
        result.check_dispatch = CheckBoundary.local_dispatch;
        return result;
    }

    pub fn check(self: InferenceExecutionControl) !void {
        if (self.check_fn) |check_fn| try CheckBoundary.call("check_fn", self.check_dispatch, check_fn, .{self.ptr});
        if (self.cancellation) |token| {
            if (token.isCancelled()) return error.Cancelled;
        }
        if (self.deadline_ns) |deadline_ns| {
            if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
        }
    }

    pub fn update(self: InferenceExecutionControl, phase: Phase, completed: u64, total: u64) !void {
        return self.updateDetail(phase, completed, total, "", "");
    }

    pub fn updateDetail(
        self: InferenceExecutionControl,
        phase: Phase,
        completed: u64,
        total: u64,
        model: []const u8,
        backend: []const u8,
    ) !void {
        try self.check();
        if (self.progress) |sink| sink.update(.{
            .phase = phase,
            .completed = completed,
            .total = total,
            .model = model,
            .backend = backend,
        });
    }

    /// Acquire a process-local execution gate without turning queueing behind
    /// another inference request into an uncancellable wait.
    pub fn lock(self: InferenceExecutionControl, mutex: *std.atomic.Mutex) !void {
        return self.lockWithPoll(mutex, 0);
    }

    /// Shared inference epochs can park between gate attempts instead of
    /// repeatedly yielding during a long GPU step. Cancellation is checked on
    /// every attempt and after acquiring the gate. Cap each requested sleep
    /// at 1ms so a caller cannot create an unbounded cancellation interval.
    /// Existing callers requesting 100us retain that interval.
    pub fn lockWithPoll(self: InferenceExecutionControl, mutex: *std.atomic.Mutex, poll_ns: u64) !void {
        var spins: usize = 0;
        while (!mutex.tryLock()) {
            try self.check();
            if (spins < 64) {
                std.atomic.spinLoopHint();
                spins += 1;
            } else if (poll_ns != 0) {
                platform_time.sleepNs(@min(poll_ns, std.time.ns_per_ms));
            } else {
                @import("antfly_platform").time.yieldNow();
            }
        }
        self.check() catch |err| {
            mutex.unlock();
            return err;
        };
    }

    pub fn enterUninterruptible(
        self: InferenceExecutionControl,
        interruption: Interruption,
    ) !UninterruptibleGuard {
        if (interruption != .uninterruptible) return .{};
        const boundary = self.hard_cancellation orelse {
            if (self.native_call_policy == .in_process) return .{};
            return error.ProcessIsolationRequired;
        };
        return .{
            .boundary = boundary,
            .token = try boundary.arm(.{
                .deadline_ns = self.deadline_ns,
                .cancellation = self.cancellation,
                .cancellation_grace_ns = self.cancellation_grace_ns,
                .ptr = self.ptr,
                .check_fn = self.check_fn,
                .check_dispatch = self.check_dispatch,
            }),
        };
    }
};

test "execution control distinguishes cancellation and timeout" {
    const State = struct {
        cancelled: bool = false,
        fn isCancelled(raw: ?*anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.cancelled;
        }
    };
    var state = State{ .cancelled = true };
    const cancelled = InferenceExecutionControl{
        .cancellation = .{ .ptr = &state, .is_cancelled_fn = State.isCancelled },
    };
    try std.testing.expectError(error.Cancelled, cancelled.check());

    const timed_out = InferenceExecutionControl{ .deadline_ns = 0 };
    try std.testing.expectError(error.Timeout, timed_out.check());
}

test "execution control reports progress after checking" {
    const Capture = struct {
        last: ?Progress = null,
        fn update(raw: ?*anyopaque, progress: Progress) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.last = progress;
        }
    };
    var capture = Capture{};
    const control = InferenceExecutionControl{
        .progress = .{ .ptr = &capture, .update_fn = Capture.update },
    };
    try control.update(.executing, 3, 24);
    try std.testing.expectEqual(Phase.executing, capture.last.?.phase);
    try std.testing.expectEqual(@as(u64, 3), capture.last.?.completed);
}

test "execution control expires while waiting for an execution gate" {
    var mutex = std.atomic.Mutex.unlocked;
    try std.testing.expect(mutex.tryLock());
    defer mutex.unlock();
    const control = InferenceExecutionControl{
        .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_ms,
    };
    try std.testing.expectError(error.Timeout, control.lock(&mutex));
}

test "uninterruptible work fails closed without a process owner" {
    const control = InferenceExecutionControl{};
    try std.testing.expectError(
        error.ProcessIsolationRequired,
        control.enterUninterruptible(.uninterruptible),
    );
    var cooperative = try control.enterUninterruptible(.cooperative);
    cooperative.deinit();
}

test "native call policy is scoped to the execution control" {
    const embedded = InferenceExecutionControl{ .native_call_policy = .in_process };
    var guard = try embedded.enterUninterruptible(.uninterruptible);
    defer guard.deinit();
    try std.testing.expectError(error.ProcessIsolationRequired, (InferenceExecutionControl{}).enterUninterruptible(.uninterruptible));
    const cancelled = InferenceExecutionControl{ .native_call_policy = .in_process, .check_fn = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    }.check };
    try std.testing.expectError(error.Cancelled, cancelled.check());
}

test "execution control checkpoint replacement preserves carrier and resets foreign dispatch" {
    const abi = @import("antfly_runtime_abi");
    const Foreign = struct {
        fn dispatch(_: *const abi.native_abi.CallContract, _: *const anyopaque, _: *const anyopaque, _: ?*anyopaque) callconv(.c) abi.error_abi.Status {
            return abi.error_abi.statusFromError(error.DeadlineExceeded);
        }
        fn stop(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    const original: InferenceExecutionControl = .{ .io = std.testing.io, .check_fn = Foreign.stop, .check_dispatch = Foreign.dispatch, .deadline_ns = std.math.maxInt(u64), .native_call_policy = .in_process, .cancellation_grace_ns = 99 };
    try std.testing.expectError(error.DeadlineExceeded, original.check());
    const rebound = original.withCheckpoint(null, Foreign.stop);
    try std.testing.expectError(error.Cancelled, rebound.check());
    try std.testing.expectEqual(original.io.?.userdata, rebound.io.?.userdata);
    try std.testing.expectEqual(original.deadline_ns, rebound.deadline_ns);
    try std.testing.expectEqual(original.native_call_policy, rebound.native_call_policy);
    try std.testing.expectEqual(original.cancellation_grace_ns, rebound.cancellation_grace_ns);
}

/// Fair admission to one model's transient working set. Interactive work may
/// bypass bulk work, with a bounded burst so indexing continues under load.
/// Waiting requests own no workspace reservation. Stack-owned waiters are removed on cancellation,
/// so a timed-out request cannot leave a ticket hole or block later work.
pub const ExecutionLane = struct {
    const Waiter = struct {
        next: ?*Waiter = null,
        interactive: bool = false,
        ready: std.Io.Event = .unset,
        io: ?std.Io = null,
    };
    mutex: std.atomic.Mutex = .unlocked,
    head: ?*Waiter = null,
    tail: ?*Waiter = null,
    owned: bool = false,
    interactive_burst: usize = 0,
    acquisitions: usize = 0,
    event_waits: usize = 0,

    pub const Lease = struct {
        lane: *ExecutionLane,
        pub fn release(self: Lease) void {
            self.lane.lock();
            defer self.lane.mutex.unlock();
            std.debug.assert(self.lane.owned);
            self.lane.owned = false;
            self.lane.wakeHead();
        }
    };

    fn lock(self: *ExecutionLane) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn selected(self: *ExecutionLane) ?*Waiter {
        var interactive: ?*Waiter = null;
        var background: ?*Waiter = null;
        var current = self.head;
        while (current) |waiter| : (current = waiter.next) {
            if (waiter.interactive) {
                if (interactive == null) interactive = waiter;
            } else if (background == null) background = waiter;
        }
        if (self.interactive_burst >= 8 and background != null) return background;
        return interactive orelse background;
    }

    fn wakeHead(self: *ExecutionLane) void {
        if (!self.owned) if (self.selected()) |head| if (head.io) |io| head.ready.set(io);
    }

    fn remove(self: *ExecutionLane, waiter: *Waiter) void {
        var previous: ?*Waiter = null;
        var current = self.head;
        while (current) |item| : (current = item.next) {
            if (item == waiter) {
                if (previous) |p| p.next = item.next else self.head = item.next;
                if (self.tail == item) self.tail = previous;
                return;
            }
            previous = item;
        }
        unreachable;
    }

    pub fn acquire(self: *ExecutionLane, control: InferenceExecutionControl) !Lease {
        return self.acquireWithPriority(control, false);
    }

    pub fn acquireWithPriority(self: *ExecutionLane, control: InferenceExecutionControl, interactive: bool) !Lease {
        return self.acquireInternal(control, interactive, true);
    }

    /// Cleanup retains borrowed native state even when its owning Io task is
    /// already cancelled. Owner release still wakes this waiter immediately.
    pub fn acquireForCleanup(self: *ExecutionLane, io: ?std.Io, interactive: bool) Lease {
        return self.acquireInternal(.{ .io = io }, interactive, false) catch unreachable;
    }

    fn acquireInternal(self: *ExecutionLane, control: InferenceExecutionControl, interactive: bool, comptime cancelable: bool) !Lease {
        if (cancelable) try control.check();
        var waiter = Waiter{ .io = control.io, .interactive = interactive };
        self.lock();
        if (self.tail) |tail| tail.next = &waiter else self.head = &waiter;
        self.tail = &waiter;
        self.mutex.unlock();
        errdefer {
            self.lock();
            self.remove(&waiter);
            self.wakeHead();
            self.mutex.unlock();
        }
        while (true) {
            if (cancelable) try control.check();
            self.lock();
            if (!self.owned and self.selected() == &waiter) {
                self.remove(&waiter);
                self.owned = true;
                self.acquisitions += 1;
                self.interactive_burst = if (interactive) self.interactive_burst +| 1 else 0;
                self.mutex.unlock();
                return .{ .lane = self };
            }
            // A newly queued query can supersede a signaled bulk waiter. Reset
            // under the lane lock so that waiter sleeps without losing a wake.
            waiter.ready.reset();
            if (control.io != null) self.event_waits += 1;
            self.mutex.unlock();
            if (control.io) |io| {
                if (!cancelable) {
                    waiter.ready.waitUncancelable(io);
                    continue;
                }
                // Release signals the next waiter immediately. The bounded
                // timeout only observes callback cancellation that has no Io
                // wake capability, and always respects the request deadline.
                const remaining = if (control.deadline_ns) |deadline|
                    @min(10 * std.time.ns_per_ms, deadline -| platform_time.monotonicNs())
                else
                    10 * std.time.ns_per_ms;
                waiter.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromNanoseconds(@intCast(remaining)), .clock = .awake } }) catch |err| switch (err) {
                    error.Timeout => {},
                    else => return err,
                };
            } else platform_time.yieldNow();
        }
    }
};

test "execution lane removes cancelled and expired waiters without ticket holes" {
    var lane = ExecutionLane{};
    const first = try lane.acquire(.{ .io = std.testing.io });
    const State = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const state: *@This() = @ptrCast(@alignCast(raw.?));
            state.checks += 1;
            if (state.checks >= 3) return error.Cancelled;
        }
    };
    var state = State{};
    try std.testing.expectError(error.Cancelled, lane.acquire(.{ .io = std.testing.io, .ptr = &state, .check_fn = State.check }));
    try std.testing.expect(lane.head == null and lane.tail == null);
    try std.testing.expectError(error.Timeout, lane.acquire(.{ .io = std.testing.io, .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_ms }));
    try std.testing.expect(lane.head == null and lane.tail == null);
    first.release();
    const next = try lane.acquire(.{});
    next.release();
}

test "execution lane gives queued batches FIFO ownership" {
    const Worker = struct {
        fn run(lane: *ExecutionLane, next: *std.atomic.Value(usize)) !usize {
            const lease = try lane.acquire(.{ .io = std.testing.io });
            defer lease.release();
            return next.fetchAdd(1, .seq_cst);
        }
        fn waitForQueue(lane: *ExecutionLane, count: usize) !void {
            const deadline = platform_time.monotonicNs() + std.time.ns_per_s * 5;
            while (true) {
                lane.lock();
                var queued: usize = 0;
                var current = lane.head;
                while (current) |item| : (current = item.next) queued += 1;
                lane.mutex.unlock();
                if (queued == count) return;
                if (platform_time.monotonicNs() >= deadline) return error.Timeout;
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
        }
    };
    var lane = ExecutionLane{};
    var next = std.atomic.Value(usize).init(0);
    const owner = try lane.acquire(.{});
    var released = false;
    defer if (!released) owner.release();
    var first = try std.testing.io.concurrent(Worker.run, .{ &lane, &next });
    defer _ = first.cancel(std.testing.io) catch 0;
    try Worker.waitForQueue(&lane, 1);
    var second = try std.testing.io.concurrent(Worker.run, .{ &lane, &next });
    defer _ = second.cancel(std.testing.io) catch 0;
    try Worker.waitForQueue(&lane, 2);
    owner.release();
    released = true;
    try std.testing.expectEqual(@as(usize, 0), try first.await(std.testing.io));
    try std.testing.expectEqual(@as(usize, 1), try second.await(std.testing.io));
}

test "execution lane prioritizes queries with bounded background fairness" {
    var lane = ExecutionLane{};
    var bulk: ExecutionLane.Waiter = .{};
    var first: ExecutionLane.Waiter = .{ .interactive = true };
    var second: ExecutionLane.Waiter = .{ .interactive = true };
    lane.head = &bulk;
    lane.tail = &second;
    bulk.next = &first;
    first.next = &second;
    try std.testing.expectEqual(&first, lane.selected().?);
    lane.remove(&first);
    try std.testing.expectEqual(&second, lane.selected().?);
    lane.interactive_burst = 8;
    try std.testing.expectEqual(&bulk, lane.selected().?);
    lane.remove(&bulk);
    try std.testing.expectEqual(&second, lane.selected().?);
}

test "execution lane wakes a query ahead of queued enrichment" {
    const Worker = struct {
        fn run(lane: *ExecutionLane, order: *std.atomic.Value(usize), interactive: bool) !usize {
            const lease = try lane.acquireWithPriority(.{ .io = std.testing.io }, interactive);
            defer lease.release();
            return order.fetchAdd(1, .seq_cst);
        }
        fn queued(lane: *ExecutionLane, count: usize) !void {
            const deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
            while (true) {
                lane.lock();
                var length: usize = 0;
                var current = lane.head;
                while (current) |item| : (current = item.next) length += 1;
                lane.mutex.unlock();
                if (length == count) return;
                if (platform_time.monotonicNs() >= deadline) return error.Timeout;
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
        }
    };
    var lane = ExecutionLane{};
    var order = std.atomic.Value(usize).init(0);
    const held = try lane.acquire(.{});
    var released = false;
    defer if (!released) held.release();
    var bulk = try std.testing.io.concurrent(Worker.run, .{ &lane, &order, false });
    defer _ = bulk.cancel(std.testing.io) catch 0;
    try Worker.queued(&lane, 1);
    var query = try std.testing.io.concurrent(Worker.run, .{ &lane, &order, true });
    defer _ = query.cancel(std.testing.io) catch 0;
    try Worker.queued(&lane, 2);
    held.release();
    released = true;
    try std.testing.expectEqual(@as(usize, 0), try query.await(std.testing.io));
    try std.testing.expectEqual(@as(usize, 1), try bulk.await(std.testing.io));
}

test "LightOnOCR3 parked execution gate preserves cancellation and lock ownership" {
    const Cancel = struct {
        calls: usize = 0,
        limit: usize,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls >= self.limit) return error.Cancelled;
        }
    };
    var mutex = std.atomic.Mutex.unlocked;
    try std.testing.expect(mutex.tryLock());
    var blocked = Cancel{ .limit = 67 };
    const waiting = InferenceExecutionControl{ .ptr = &blocked, .check_fn = Cancel.check };
    const started = platform_time.monotonicNs();
    try std.testing.expectError(error.Cancelled, waiting.lockWithPoll(&mutex, std.time.ns_per_s));
    // A caller-supplied second cannot delay each cancellation check by a second.
    try std.testing.expect(platform_time.monotonicNs() - started < 500 * std.time.ns_per_ms);
    try std.testing.expectEqual(@as(usize, 67), blocked.calls);
    try std.testing.expect(!mutex.tryLock());
    mutex.unlock();
    var cancelled = Cancel{ .limit = 1 };
    const acquired = InferenceExecutionControl{ .ptr = &cancelled, .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, acquired.lockWithPoll(&mutex, 100 * std.time.ns_per_us));
    try std.testing.expect(mutex.tryLock());
    mutex.unlock();
    try (InferenceExecutionControl{}).lockWithPoll(&mutex, 100 * std.time.ns_per_us);
    try std.testing.expect(!mutex.tryLock());
    mutex.unlock();
}

test "LightOnOCR3 execution lane cleanup survives cancellation of its owning Io task" {
    const Worker = struct {
        fn run(lane: *ExecutionLane, started: *std.Io.Event, done: *std.atomic.Value(bool)) anyerror!void {
            defer {
                const cleanup = lane.acquireForCleanup(std.testing.io, true);
                cleanup.release();
                done.store(true, .release);
            }
            started.set(std.testing.io);
            return;
        }
        fn cancel(future: *std.Io.Future(anyerror!void), entered: *std.atomic.Value(bool)) void {
            entered.store(true, .release);
            _ = future.cancel(std.testing.io) catch {};
        }
    };
    var lane: ExecutionLane = .{};
    const owner = try lane.acquire(.{});
    var released = false;
    var started: std.Io.Event = .unset;
    var done: std.atomic.Value(bool) = .init(false);
    var pending = try std.testing.io.concurrent(Worker.run, .{ &lane, &started, &done });
    var cancel_task: ?std.Io.Future(void) = null;
    var cancel_entered: std.atomic.Value(bool) = .init(false);
    defer {
        if (!released) owner.release();
        if (cancel_task) |*task| task.cancel(std.testing.io);
        _ = pending.cancel(std.testing.io) catch {};
    }
    try started.wait(std.testing.io);
    const deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        lane.lock();
        const waiting = if (lane.head) |head| @atomicLoad(std.Io.Event, &head.ready, .acquire) == .waiting else false;
        lane.mutex.unlock();
        if (waiting) break;
        if (platform_time.monotonicNs() >= deadline) return error.Timeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    // Cancel while cleanup is already parked, rather than consuming the Io
    // cancellation in an earlier operation before entering cleanup.
    cancel_task = try std.testing.io.concurrent(Worker.cancel, .{ &pending, &cancel_entered });
    while (!cancel_entered.load(.acquire)) {
        if (platform_time.monotonicNs() >= deadline) return error.Timeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    try std.testing.expect(!done.load(.acquire));
    owner.release();
    released = true;
    cancel_task.?.await(std.testing.io);
    cancel_task = null;
    try std.testing.expect(done.load(.acquire));
    try std.testing.expect(!lane.owned and lane.head == null and lane.tail == null);
}
