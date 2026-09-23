// Copyright 2026 Antfly, Inc.
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
const platform = @import("antfly_platform");
const execution_control_mod = @import("execution_control.zig");
fn spinLock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

/// Watches only calls which declared that cooperative or native termination is
/// insufficient. Expiry is process-fatal by design: the supervisor owns the
/// replacement generation, while continuing in this address space could reuse
/// buffers still retained by a wedged driver.
pub const HardCancellationWatchdog = struct {
    const Entry = struct {
        token: u64,
        control: execution_control_mod.MonitorControl,
        cancellation_observed_ns: ?u64 = null,

        fn shouldRestart(self: *Entry, now_ns: u64) bool {
            self.control.check() catch |err| {
                if (err == error.Cancelled or err == error.Canceled) {
                    if (self.control.cancellation_grace_ns) |grace_ns| {
                        const observed = self.cancellation_observed_ns orelse now_ns;
                        self.cancellation_observed_ns = observed;
                        return now_ns -| observed >= grace_ns;
                    }
                }
                return true;
            };
            self.cancellation_observed_ns = null;
            return false;
        }
    };

    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    next_token: u64 = 1,
    stopping: std.atomic.Value(bool) = .init(false),
    io: ?std.Io = null,
    group: std.Io.Group = .init,

    pub fn create(allocator: std.mem.Allocator) !*HardCancellationWatchdog {
        const self = try allocator.create(HardCancellationWatchdog);
        errdefer allocator.destroy(self);
        self.* = .{ .allocator = allocator };
        return self;
    }

    pub fn start(self: *HardCancellationWatchdog, io: std.Io) !void {
        if (self.io != null) return;
        self.io = io;
        errdefer self.io = null;
        try self.group.concurrent(io, run, .{ self, io });
    }

    pub fn destroy(self: *HardCancellationWatchdog) void {
        self.stopping.store(true, .release);
        if (self.io) |io| {
            self.group.cancel(io);
            self.group.await(io) catch {};
        }
        spinLock(&self.mutex);
        std.debug.assert(self.entries.items.len == 0);
        self.entries.deinit(self.allocator);
        self.mutex.unlock();
        const allocator = self.allocator;
        self.* = undefined;
        allocator.destroy(self);
    }

    pub fn boundary(self: *HardCancellationWatchdog) execution_control_mod.HardCancellationBoundary {
        return .{
            .ptr = self,
            .arm_fn = armOpaque,
            .disarm_fn = disarmOpaque,
        };
    }

    fn armOpaque(raw: *anyopaque, control: execution_control_mod.MonitorControl) !u64 {
        const self: *HardCancellationWatchdog = @ptrCast(@alignCast(raw));
        try control.check();
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.stopping.load(.acquire)) return error.InferenceWorkerShuttingDown;
        if (self.io == null) return error.HardCancellationWatchdogNotStarted;
        const token = self.next_token;
        self.next_token +%= 1;
        if (self.next_token == 0) self.next_token = 1;
        try self.entries.append(self.allocator, .{ .token = token, .control = control });
        return token;
    }

    fn disarmOpaque(raw: *anyopaque, token: u64) void {
        const self: *HardCancellationWatchdog = @ptrCast(@alignCast(raw));
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items, 0..) |entry, index| {
            if (entry.token != token) continue;
            _ = self.entries.swapRemove(index);
            return;
        }
        // A missing token is an ownership violation. Do not silently leave a
        // borrowed request pointer in the monitor.
        @panic("hard cancellation watchdog token was not armed");
    }

    fn run(self: *HardCancellationWatchdog, io: std.Io) std.Io.Cancelable!void {
        while (!self.stopping.load(.acquire)) {
            var fatal = false;
            spinLock(&self.mutex);
            const now_ns = platform.time.monotonicNs();
            for (self.entries.items) |*entry| {
                if (entry.shouldRestart(now_ns)) {
                    fatal = true;
                    break;
                }
            }
            self.mutex.unlock();
            if (fatal) {
                // A wedged worker may own stderr's lock. The supervisor records
                // the restart without blocking this watchdog on logging.
                platform.inference_process_supervisor.restartWorker();
            }
            try io.sleep(std.Io.Duration.fromMilliseconds(10), .awake);
        }
    }
};

test "native call cancellation gets a bounded grace but deadlines stay hard" {
    const State = struct {
        cancelled: bool = true,
        alternate_spelling: bool = false,
        timed_out: bool = false,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.timed_out) return error.Timeout;
            if (self.cancelled) return if (self.alternate_spelling) error.Canceled else error.Cancelled;
        }
    };
    var state = State{};
    var entry = HardCancellationWatchdog.Entry{
        .token = 1,
        .control = .{ .ptr = &state, .check_fn = State.check, .cancellation_grace_ns = 30 },
    };
    try std.testing.expect(!entry.shouldRestart(100));
    try std.testing.expect(!entry.shouldRestart(129));
    try std.testing.expect(entry.shouldRestart(130));
    state.cancelled = false;
    try std.testing.expect(!entry.shouldRestart(140));
    try std.testing.expect(entry.cancellation_observed_ns == null);
    state.timed_out = true;
    try std.testing.expect(entry.shouldRestart(141));

    state.timed_out = false;
    state.cancelled = true;
    state.alternate_spelling = true;
    entry.cancellation_observed_ns = null;
    try std.testing.expect(!entry.shouldRestart(200));
    try std.testing.expect(!entry.shouldRestart(229));
    try std.testing.expect(entry.shouldRestart(230));
}
