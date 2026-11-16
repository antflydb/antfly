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

//! Equal-work ticket queue comparison. Run each case in a subprocess with a
//! watchdog: the old condition may stall, which is a result, not a fast sample.
const std = @import("std");
const sync = @import("antfly_sync");

fn Queue(comptime Condition: type) type {
    return struct {
        state: std.Io.Mutex = .init,
        ready: Condition = .init,
        next: u64 = 0,
        serving: u64 = 0,
        value: u64 = 0,
        fn lock(self: *@This(), io: std.Io) void {
            self.state.lockUncancelable(io);
            defer self.state.unlock(io);
            const ticket = self.next;
            self.next +%= 1;
            while (ticket != self.serving) self.ready.waitUncancelable(io, &self.state);
        }
        fn unlock(self: *@This(), io: std.Io) void {
            self.state.lockUncancelable(io);
            self.serving +%= 1;
            self.ready.broadcast(io);
            self.state.unlock(io);
        }
    };
}

// Only Mutex/Condition methods receive this Io. Clocks and events always use
// the base Io, whose other vtable functions require the original userdata.
const Probe = struct {
    base: std.Io,
    condition_word: *const u32,
    waits: u64 = 0,
    wakes: u64 = 0,
    condition_waits: u64 = 0,
    condition_wakes: u64 = 0,
    fn wait(ptr: ?*anyopaque, word: *const u32, expected: u32) void {
        const self: *@This() = @ptrCast(@alignCast(ptr.?));
        self.waits += 1;
        if (word == self.condition_word) self.condition_waits += 1;
        self.base.futexWaitUncancelable(u32, word, expected);
    }
    fn wake(ptr: ?*anyopaque, word: *const u32, count: u32) void {
        const self: *@This() = @ptrCast(@alignCast(ptr.?));
        self.wakes += 1;
        if (word == self.condition_word) self.condition_wakes += 1;
        self.base.futexWake(u32, word, count);
    }
};

fn run(comptime Condition: type, init: std.process.Init, name: []const u8, threads: usize, operations: usize) !void {
    const Gate = Queue(Condition);
    const Worker = struct {
        gate: *Gate,
        start: *std.Io.Event,
        ready: *std.atomic.Value(usize),
        probe: Probe,
        samples: []u64,
        fn work(self: *@This()) void {
            var vtable = self.probe.base.vtable.*;
            vtable.futexWaitUncancelable = Probe.wait;
            vtable.futexWake = Probe.wake;
            const io: std.Io = .{ .userdata = &self.probe, .vtable = &vtable };
            _ = self.ready.fetchAdd(1, .release);
            self.start.waitUncancelable(self.probe.base);
            // The parent initially owns the queue until every worker has a
            // ticket. Exclude that first warm-up acquisition from latency.
            self.gate.lock(io);
            self.gate.unlock(io);
            for (self.samples) |*sample| {
                const before = std.Io.Clock.awake.now(self.probe.base).nanoseconds;
                self.gate.lock(io);
                sample.* = @intCast(@max(0, std.Io.Clock.awake.now(self.probe.base).nanoseconds - before));
                self.gate.value += 1;
                self.gate.unlock(io);
            }
        }
    };
    var gate: Gate = .{};
    var start: std.Io.Event = .unset;
    var ready: std.atomic.Value(usize) = .init(0);
    const samples = try init.gpa.alloc(u64, threads * operations);
    defer init.gpa.free(samples);
    const workers = try init.gpa.alloc(Worker, threads);
    defer init.gpa.free(workers);
    const handles = try init.gpa.alloc(std.Thread, threads);
    defer init.gpa.free(handles);
    // A dedicated Threaded Io has predictable futex behavior for foreign OS threads.
    var threaded: std.Io.Threaded = .init(init.gpa, .{ .async_limit = .nothing });
    defer threaded.deinit();
    const base = threaded.io();
    gate.lock(base);
    var launched: usize = 0;
    var gate_held = true;
    var joined = false;
    errdefer {
        if (gate_held) gate.unlock(base);
        start.set(base);
        if (!joined) for (handles[0..launched]) |handle| handle.join();
    }
    for (workers, 0..) |*worker, index| {
        worker.* = .{ .gate = &gate, .start = &start, .ready = &ready, .probe = .{ .base = base, .condition_word = &gate.ready.epoch.raw }, .samples = samples[index * operations ..][0..operations] };
        handles[index] = try std.Thread.spawn(.{}, Worker.work, .{worker});
        launched += 1;
    }
    while (ready.load(.acquire) != threads) try base.sleep(.fromMilliseconds(1), .awake);
    start.set(base);
    while (true) {
        gate.state.lockUncancelable(base);
        const queued = gate.next == threads + 1;
        gate.state.unlock(base);
        if (queued) break;
        try base.sleep(.fromMilliseconds(1), .awake);
    }
    const before = std.Io.Clock.awake.now(base).nanoseconds;
    gate.unlock(base);
    gate_held = false;
    for (handles) |handle| handle.join();
    joined = true;
    const elapsed: u64 = @intCast(std.Io.Clock.awake.now(base).nanoseconds - before);
    if (gate.value != samples.len) return error.LostOperations;
    std.mem.sort(u64, samples, {}, std.sort.asc(u64));
    var waits: u64 = 0;
    var wakes: u64 = 0;
    var condition_waits: u64 = 0;
    var condition_wakes: u64 = 0;
    for (workers) |worker| {
        waits += worker.probe.waits;
        wakes += worker.probe.wakes;
        condition_waits += worker.probe.condition_waits;
        condition_wakes += worker.probe.condition_wakes;
    }
    std.debug.print("{{\"condition\":\"{s}\",\"threads\":{d},\"operations\":{d},\"elapsed_ns\":{d},\"p50_ns\":{d},\"p95_ns\":{d},\"p99_ns\":{d},\"max_ns\":{d},\"futex_wait_calls\":{d},\"futex_wake_calls\":{d},\"condition_wait_calls\":{d},\"condition_wake_calls\":{d}}}\n", .{ name, threads, samples.len, elapsed, samples[(samples.len - 1) * 50 / 100], samples[(samples.len - 1) * 95 / 100], samples[(samples.len - 1) * 99 / 100], samples[samples.len - 1], waits, wakes, condition_waits, condition_wakes });
}

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const name = args.next() orelse return error.ExpectedCondition;
    const threads = try std.fmt.parseInt(usize, args.next() orelse return error.ExpectedThreads, 10);
    const operations = try std.fmt.parseInt(usize, args.next() orelse return error.ExpectedOperationsPerThread, 10);
    if (threads == 0 or threads > 128 or operations == 0 or operations > 1_000_000 or args.next() != null) return error.InvalidArguments;
    if (std.mem.eql(u8, name, "epoch")) return run(sync.BroadcastCondition, init, name, threads, operations);
    if (std.mem.eql(u8, name, "std")) return run(std.Io.Condition, init, name, threads, operations);
    return error.UnknownCondition;
}
