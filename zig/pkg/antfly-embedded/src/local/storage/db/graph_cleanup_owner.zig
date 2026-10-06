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
const runtime = @import("../background_runtime.zig");
/// Local bounded cleanup lifecycle. Storage owns write eligibility and each fenced page.
pub const Owner = struct {
    pub const Port = struct { ptr: *anyopaque, runtime: *runtime.BackendRuntime, run: *const fn (*anyopaque) anyerror!bool };
    port: Port = undefined,
    mutex: std.atomic.Mutex = .unlocked,
    registration: ?runtime.MaintenanceScheduler.Handle = null,
    stopping: std.atomic.Value(bool) = .init(false),
    fn lock(self: *Owner) void {
        while (!self.mutex.tryLock()) @import("antfly_platform").time.yieldNow();
    }
    pub fn start(self: *Owner, port: Port) void {
        self.lock();
        defer self.mutex.unlock();
        if (self.stopping.load(.acquire) or self.registration != null) return;
        const scheduler = port.runtime.maintenanceScheduler() catch |err| {
            std.log.warn("graph cleanup scheduler unavailable: {}", .{err});
            return;
        };
        self.port = port;
        self.registration = scheduler.register(self, scheduledStep) catch |err| {
            std.log.warn("graph cleanup worker unavailable: {}", .{err});
            return;
        };
    }
    pub fn stop(self: *Owner, io: ?std.Io) void {
        self.lock();
        self.stopping.store(true, .release);
        var task = self.registration;
        self.registration = null;
        self.mutex.unlock();
        if (task) |*handle| handle.await(io.?);
        self.lock();
        self.mutex.unlock();
    }
    fn scheduledStep(self: *Owner) ?u64 {
        return self.step(self.port);
    }
    pub fn step(self: *Owner, port: Port) ?u64 {
        if (self.stopping.load(.acquire)) return null;
        const progressed = port.run(port.ptr) catch |err| {
            std.log.warn("graph endpoint cleanup deferred: {}", .{err});
            return 250;
        };
        return if (progressed) 1 else 100;
    }
};

test "graph cleanup owner preserves bounded cadence and permanent shutdown" {
    const F = struct {
        progress: bool = false,
        fail: bool = false,
        calls: usize = 0,
        fn run(ptr: *anyopaque) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.fail) return error.WriterLocked;
            return self.progress;
        }
    };
    var f: F = .{};
    var owner: Owner = .{};
    const port: Owner.Port = .{ .ptr = &f, .runtime = undefined, .run = F.run };
    try std.testing.expectEqual(@as(?u64, 100), owner.step(port));
    f.progress = true;
    try std.testing.expectEqual(@as(?u64, 1), owner.step(port));
    f.fail = true;
    try std.testing.expectEqual(@as(?u64, 250), owner.step(port));
    owner.stop(null);
    owner.start(port);
    try std.testing.expect(owner.registration == null);
    try std.testing.expect(owner.step(port) == null);
    try std.testing.expectEqual(@as(usize, 3), f.calls);
}
