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
const builtin = @import("builtin");

pub const Clock = struct {
    ctx: ?*anyopaque = null,
    now_realtime_ns_fn: *const fn (?*anyopaque) u64,
    sleep_ms_fn: *const fn (?*anyopaque, u64) void,

    pub fn real() Clock {
        if (builtin.os.tag == .freestanding) {
            return .{
                .now_realtime_ns_fn = freestandingNowRealtimeNs,
                .sleep_ms_fn = freestandingSleepMs,
            };
        }
        return .{
            .now_realtime_ns_fn = realNowRealtimeNs,
            .sleep_ms_fn = realSleepMs,
        };
    }

    pub fn nowRealtimeNs(self: Clock) u64 {
        return self.now_realtime_ns_fn(self.ctx);
    }

    pub fn nowRealtimeMs(self: Clock) u64 {
        return @intCast(self.nowRealtimeNs() / std.time.ns_per_ms);
    }

    pub fn sleepMs(self: Clock, ms: u64) void {
        self.sleep_ms_fn(self.ctx, ms);
    }
};

pub const ManualClock = struct {
    mutex: std.atomic.Mutex = .unlocked,
    now_realtime_ns: u64 = 0,

    pub fn clock(self: *ManualClock) Clock {
        return .{
            .ctx = self,
            .now_realtime_ns_fn = manualNowRealtimeNs,
            .sleep_ms_fn = manualSleepMs,
        };
    }

    pub fn setRealtimeNs(self: *ManualClock, now_realtime_ns: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.now_realtime_ns = now_realtime_ns;
    }

    pub fn advanceMs(self: *ManualClock, delta_ms: u64) void {
        self.advanceNs(delta_ms * std.time.ns_per_ms);
    }

    pub fn advanceNs(self: *ManualClock, delta_ns: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.now_realtime_ns +%= delta_ns;
    }
};

fn realNowRealtimeNs(_: ?*anyopaque) u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => {},
        else => unreachable,
    }
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn realSleepMs(_: ?*anyopaque, ms: u64) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const sleep_ms = if (ms == 0) @as(u64, 1) else ms;
    std.Io.Clock.Duration.sleep(.{
        .clock = .awake,
        .raw = .fromMilliseconds(@intCast(sleep_ms)),
    }, io_impl.io()) catch {};
}

fn freestandingNowRealtimeNs(_: ?*anyopaque) u64 {
    return 0;
}

fn freestandingSleepMs(_: ?*anyopaque, _: u64) void {}

fn manualNowRealtimeNs(ctx: ?*anyopaque) u64 {
    const self: *ManualClock = @ptrCast(@alignCast(ctx.?));
    lockAtomic(&self.mutex);
    defer self.mutex.unlock();
    return self.now_realtime_ns;
}

fn manualSleepMs(ctx: ?*anyopaque, ms: u64) void {
    _ = ctx;
    _ = ms;
    if (builtin.os.tag != .freestanding) std.Thread.yield() catch {};
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}
