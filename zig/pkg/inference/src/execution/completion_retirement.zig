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

//! Optional cold completion retirement. One process-owned CPU worker polls
//! independent endpoint records; no caller allocator/Io/model pointer escapes.
//! Reserve a bounded intrusive slot BEFORE SDK submission. Backend callbacks,
//! actual payload destruction and external quota callbacks run outside the lock.
const std = @import("std");
const platform = @import("antfly_platform");
const enabled = !@import("build_options").enable_wasm and switch (@import("builtin").os.tag) {
    .linux, .macos => true,
    else => false,
};
const control_mod = @import("../execution_control.zig");
const Watchdog = @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog;
const Supervision = struct {
    threaded: std.Io.Threaded,
    watchdog: *Watchdog,
};
var supervision_mutex: std.atomic.Mutex = .unlocked;
var supervision: ?*Supervision = null;
/// Cold caller-owned setup, before source/target lanes, quota commit or SDK.
/// Both Io and monitor storage are process-owned and intentionally process-long.
/// One queue worker means at most one SDK interval is armed at a time.
pub fn prepareSupervision() !void {
    if (comptime !enabled) return error.CompletionRetirementUnavailable;
    while (!supervision_mutex.tryLock()) std.atomic.spinLoopHint();
    defer supervision_mutex.unlock();
    if (supervision != null) return;
    const allocator = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
    const state = try allocator.create(Supervision);
    errdefer allocator.destroy(state);
    state.threaded = std.Io.Threaded.init(allocator, .{});
    errdefer state.threaded.deinit();
    state.watchdog = try Watchdog.create(allocator);
    errdefer state.watchdog.destroy();
    try state.watchdog.entries.ensureTotalCapacity(allocator, 1);
    try state.watchdog.start(state.threaded.io());
    supervision = state;
}
/// Called ONLY on the single retirement worker, after cold initialization.
/// The monitor receives a copied monotonic deadline and no caller pointers.
pub fn beginSdkInterval(deadline_ns: ?u64) !control_mod.UninterruptibleGuard {
    const deadline = deadline_ns orelse return error.CompletionRetirementDeadlineRequired;
    const state = supervision orelse return error.CompletionRetirementSupervisionRequired;
    return beginSdkIntervalWith(state.watchdog.boundary(), deadline);
}
fn beginSdkIntervalWith(boundary: control_mod.HardCancellationBoundary, deadline: u64) !control_mod.UninterruptibleGuard {
    const token = try boundary.arm_fn(boundary.ptr, .{ .deadline_ns = deadline });
    return .{ .boundary = boundary, .token = token };
}

pub const Node = struct {
    next: ?*Node = null,
    context: *anyopaque,
    ready_fn: *const fn (*anyopaque) anyerror!bool,
    retire_fn: *const fn (*anyopaque) void,
    /// Value-only request cleanup deadline, including the caller's grace.
    /// Null is an explicitly unavailable deadline, never fabricated progress.
    deadline_ns: ?u64 = null,
};
pub const Ticket = struct {
    held: bool = true,
    pub fn release(self: *Ticket) void {
        if (comptime !enabled) {
            self.held = false;
            return;
        }
        if (!self.held) return;
        self.held = false;
        queue.lock();
        defer queue.unlock();
        std.debug.assert(queue.reserved > 0);
        queue.reserved -= 1;
    }
    /// Moves the slot to a sealed, independently owned record. No allocation.
    pub fn submit(self: *Ticket, node: *Node) void {
        if (comptime !enabled) @panic("completion retirement unavailable");
        std.debug.assert(self.held and node.next == null);
        self.held = false;
        queue.lock();
        queue.append(node);
        std.debug.assert(std.c.pthread_cond_signal(&queue.changed) == .SUCCESS);
        queue.unlock();
    }
};
pub fn reserve() !?Ticket {
    if (comptime !enabled) return null;
    queue.lock();
    defer queue.unlock();
    if (queue.reserved == max_pending) return null;
    if (!queue.started) {
        // Use the standard thread stack: CUDA/glibc may require more static
        // TLS than a hand-selected small stack permits (pthread_create EINVAL).
        const worker = try std.Thread.spawn(.{}, Queue.run, .{});
        worker.detach();
        queue.started = true;
    }
    queue.reserved += 1;
    return .{};
}
/// Test-only lifecycle fence: zero means every retired callback (including
/// quota reentry and endpoint destruction) has returned, not merely ledger0.
pub fn testingPending() usize {
    if (comptime !@import("builtin").is_test) @compileError("test-only retirement observation");
    queue.lock();
    defer queue.unlock();
    return queue.reserved;
}
const max_pending = 64;
const Queue = struct {
    mutex: std.c.pthread_mutex_t = std.c.PTHREAD_MUTEX_INITIALIZER,
    changed: std.c.pthread_cond_t = std.c.PTHREAD_COND_INITIALIZER,
    first: ?*Node = null,
    last: ?*Node = null,
    reserved: usize = 0,
    queued: usize = 0,
    started: bool = false,
    fn lock(self: *Queue) void {
        std.debug.assert(std.c.pthread_mutex_lock(&self.mutex) == .SUCCESS);
    }
    fn unlock(self: *Queue) void {
        std.debug.assert(std.c.pthread_mutex_unlock(&self.mutex) == .SUCCESS);
    }
    fn append(self: *Queue, node: *Node) void {
        self.queued += 1;
        if (self.last) |last| last.next = node else self.first = node;
        self.last = node;
    }
    fn pop(self: *Queue) *Node {
        std.debug.assert(self.queued != 0);
        self.queued -= 1;
        const node = self.first.?;
        self.first = node.next;
        if (self.first == null) self.last = null;
        node.next = null;
        return node;
    }
    fn run() void {
        var remaining_in_round: usize = 0;
        var round_needs_backoff = false;
        while (true) {
            queue.lock();
            while (queue.first == null) std.debug.assert(std.c.pthread_cond_wait(&queue.changed, &queue.mutex) == .SUCCESS);
            if (remaining_in_round == 0) {
                remaining_in_round = queue.queued;
                round_needs_backoff = false;
            }
            const node = queue.pop();
            remaining_in_round -= 1;
            queue.unlock();
            // SDK query is never under metadata/provider/model exclusion.
            const ready = node.ready_fn(node.context) catch platform.inference_process_supervisor.restartWorker();
            if (ready) {
                const context = node.context;
                const retire = node.retire_fn;
                retire(context); // May destroy Node; never access it again.
                var slot = Ticket{};
                slot.release();
            } else {
                if (node.deadline_ns) |deadline| if (platform.time.monotonicNs() >= deadline)
                    platform.inference_process_supervisor.restartWorker();
                queue.lock();
                queue.append(node);
                queue.unlock();
                round_needs_backoff = true;
            }
            if (remaining_in_round == 0 and round_needs_backoff) {
                // Poll each already-queued record once before one bounded
                // relative backoff. Wall-clock changes cannot extend it, and
                // 64 pending records do not multiply the sleep to 64ms.
                // No Io/request pointer or queue lock survives this interval.
                const backoff: std.c.timespec = .{ .sec = 0, .nsec = std.time.ns_per_ms };
                const waited = std.c.errno(std.c.nanosleep(&backoff, null));
                if (waited != .SUCCESS and waited != .INTR)
                    platform.inference_process_supervisor.restartWorker();
            }
        }
    }
};
var queue: Queue = .{};

test "cold completion retirement reserves before submission and runs callbacks outside the queue lock" {
    if (comptime !enabled) return error.SkipZigTest;
    const Probe = struct {
        node: Node = undefined,
        polls: std.atomic.Value(usize) = .init(0),
        done: std.atomic.Value(bool) = .init(false),
        fn ready(raw: *anyopaque) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.polls.fetchAdd(1, .monotonic) >= 2;
        }
        fn retire(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var nested = (reserve() catch unreachable).?;
            nested.release(); // Would deadlock if retirement held queue lock.
            self.done.store(true, .release);
        }
    };
    const allocator = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
    const probe = try allocator.create(Probe);
    defer allocator.destroy(probe);
    probe.* = .{};
    probe.node = .{ .context = probe, .ready_fn = Probe.ready, .retire_fn = Probe.retire, .deadline_ns = platform.time.monotonicNs() + 5 * std.time.ns_per_s };
    var slot = (try reserve()).?;
    slot.submit(&probe.node);
    const deadline = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!probe.done.load(.acquire)) {
        if (platform.time.monotonicNs() >= deadline) return error.Timeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
}

test "cold completion reservation is bounded and cancellation returns every unpublished slot" {
    if (comptime !enabled) return error.SkipZigTest;
    var tickets: [max_pending]Ticket = undefined;
    var count: usize = 0;
    defer for (tickets[0..count]) |*ticket| ticket.release();
    while (count < tickets.len) {
        tickets[count] = (try reserve()) orelse break;
        count += 1;
    }
    try std.testing.expect(count > 0);
    try std.testing.expect((try reserve()) == null);
    tickets[count - 1].release();
    var replacement = (try reserve()) orelse return error.LostCompletionSlot;
    replacement.release();
}

test "completion SDK supervision arms copied value-only policy until blocked callback returns" {
    const Probe = struct {
        armed: std.atomic.Value(bool) = .init(false),
        disarmed: std.atomic.Value(bool) = .init(false),
        entered: std.atomic.Value(bool) = .init(false),
        resume_sdk: std.atomic.Value(bool) = .init(false),
        invalid: std.atomic.Value(bool) = .init(false),
        fn arm(raw: *anyopaque, policy: control_mod.MonitorControl) !u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (policy.ptr != null or policy.check_fn != null or policy.cancellation != null or policy.deadline_ns != 123456)
                self.invalid.store(true, .release);
            self.armed.store(true, .release);
            return 7;
        }
        fn disarm(raw: *anyopaque, token: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (token != 7 or !self.armed.load(.acquire)) self.invalid.store(true, .release);
            self.disarmed.store(true, .release);
        }
        fn worker(self: *@This()) void {
            var interval = beginSdkIntervalWith(.{ .ptr = self, .arm_fn = arm, .disarm_fn = disarm }, 123456) catch unreachable;
            defer interval.deinit();
            self.entered.store(true, .release);
            // Deterministic blocked SDK-policy callback. The real watchdog
            // would retain termination authority throughout this same interval.
            while (!self.resume_sdk.load(.acquire)) std.atomic.spinLoopHint();
        }
    };
    var probe = Probe{};
    const thread = try std.Thread.spawn(.{}, Probe.worker, .{&probe});
    var joined = false;
    defer if (!joined) {
        probe.resume_sdk.store(true, .release);
        thread.join();
    };
    const limit = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!probe.entered.load(.acquire)) {
        if (platform.time.monotonicNs() >= limit) return error.Timeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(probe.armed.load(.acquire));
    try std.testing.expect(!probe.disarmed.load(.acquire));
    try std.testing.expect(!probe.invalid.load(.acquire));
    probe.resume_sdk.store(true, .release);
    thread.join();
    joined = true;
    try std.testing.expect(probe.disarmed.load(.acquire));
}
