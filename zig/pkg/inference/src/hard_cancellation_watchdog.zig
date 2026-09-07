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
            var fatal: ?anyerror = null;
            spinLock(&self.mutex);
            for (self.entries.items) |entry| {
                entry.control.check() catch |err| {
                    fatal = err;
                    break;
                };
            }
            self.mutex.unlock();
            if (fatal) |err| {
                std.log.err(
                    "uninterruptible inference request expired; terminating supervised worker err={s}",
                    .{@errorName(err)},
                );
                platform.inference_process_supervisor.restartWorker();
            }
            try io.sleep(std.Io.Duration.fromMilliseconds(10), .awake);
        }
    }
};
