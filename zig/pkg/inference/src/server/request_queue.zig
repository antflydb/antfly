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

// Request queue: limits concurrent inference requests with backpressure.
//
// A Node may serve HTTP and in-process providers from different OS threads,
// so admission and accounting share one small atomic critical section.

const std = @import("std");

pub const RequestQueue = struct {
    pub const Snapshot = struct {
        active_requests: usize,
        active_units: usize,
        capacity: usize,
        available: usize,
    };

    mutex: std.atomic.Mutex = .unlocked,
    active_requests: usize = 0,
    active_units: usize = 0,
    max_concurrent: usize,

    pub fn init(max_concurrent: usize) RequestQueue {
        return .{ .max_concurrent = max_concurrent };
    }

    /// Acquire a slot. Returns error.QueueFull if at capacity.
    pub fn acquire(self: *RequestQueue) !void {
        try self.acquireUnits(1);
    }

    /// Acquire weighted capacity units. Single-slot callers should continue using acquire().
    pub fn acquireUnits(self: *RequestQueue, units: usize) !void {
        const requested = self.capacityUnits(units);
        self.lock();
        defer self.mutex.unlock();
        if (self.max_concurrent != 0 and requested > self.max_concurrent - self.active_units) {
            return error.QueueFull;
        }
        self.active_requests +|= 1;
        self.active_units +|= requested;
    }

    /// Release a slot after request completes.
    pub fn release(self: *RequestQueue) void {
        self.releaseUnits(1);
    }

    pub fn releaseUnits(self: *RequestQueue, units: usize) void {
        const requested = self.capacityUnits(units);
        self.lock();
        defer self.mutex.unlock();
        if (self.active_requests > 0) self.active_requests -= 1;
        if (self.active_units > requested) {
            self.active_units -= requested;
        } else {
            self.active_units = 0;
        }
    }

    pub fn capacityUnits(self: *const RequestQueue, units: usize) usize {
        const requested = @max(units, 1);
        return if (self.max_concurrent == 0) requested else @min(requested, self.max_concurrent);
    }

    pub fn snapshot(self: *RequestQueue) Snapshot {
        self.lock();
        defer self.mutex.unlock();
        return .{
            .active_requests = self.active_requests,
            .active_units = self.active_units,
            .capacity = self.max_concurrent,
            .available = if (self.max_concurrent == 0)
                std.math.maxInt(usize)
            else
                self.max_concurrent - self.active_units,
        };
    }

    pub fn depth(self: *RequestQueue) usize {
        return self.snapshot().active_units;
    }

    pub fn requests(self: *RequestQueue) usize {
        return self.snapshot().active_requests;
    }

    pub fn available(self: *RequestQueue) usize {
        return self.snapshot().available;
    }

    fn lock(self: *RequestQueue) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
};

test "request queue basic" {
    var q = RequestQueue.init(2);

    try q.acquire();
    try std.testing.expectEqual(@as(usize, 1), q.depth());

    try q.acquire();
    try std.testing.expectEqual(@as(usize, 2), q.depth());

    // Should be full
    try std.testing.expectError(error.QueueFull, q.acquire());

    q.release();
    try std.testing.expectEqual(@as(usize, 1), q.depth());

    // Can acquire again
    try q.acquire();
    try std.testing.expectEqual(@as(usize, 2), q.depth());
}

test "request queue weighted capacity" {
    var q = RequestQueue.init(4);

    try q.acquireUnits(3);
    try std.testing.expectEqual(@as(usize, 3), q.depth());
    try std.testing.expectEqual(@as(usize, 1), q.requests());

    try std.testing.expectError(error.QueueFull, q.acquireUnits(2));

    try q.acquire();
    try std.testing.expectEqual(@as(usize, 4), q.depth());
    try std.testing.expectEqual(@as(usize, 2), q.requests());

    q.releaseUnits(3);
    try std.testing.expectEqual(@as(usize, 1), q.depth());
    try std.testing.expectEqual(@as(usize, 1), q.requests());
    try std.testing.expectEqual(@as(usize, 3), q.available());
    try std.testing.expectEqual(@as(usize, 4), q.capacityUnits(100));
    try std.testing.expectEqual(@as(usize, 1), q.capacityUnits(0));
}

test "request queue zero capacity limit is unlimited" {
    var q = RequestQueue.init(0);

    try q.acquireUnits(3);
    try q.acquire();
    try std.testing.expectEqual(@as(usize, 4), q.depth());
    try std.testing.expectEqual(@as(usize, 2), q.requests());
    try std.testing.expectEqual(std.math.maxInt(usize), q.available());

    q.releaseUnits(3);
    q.release();
    try std.testing.expectEqual(@as(usize, 0), q.depth());
}

test "request queue serializes admission across OS threads" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;

    const Worker = struct {
        fn run(q: *RequestQueue, violations: *std.atomic.Value(usize)) void {
            for (0..1000) |_| {
                q.acquire() catch |err| switch (err) {
                    error.QueueFull => {
                        std.Thread.yield() catch {};
                        continue;
                    },
                };
                const state = q.snapshot();
                if (state.active_units > state.capacity) _ = violations.fetchAdd(1, .monotonic);
                std.Thread.yield() catch {};
                q.release();
            }
        }
    };

    var q = RequestQueue.init(2);
    var violations = std.atomic.Value(usize).init(0);
    var threads: [4]std.Thread = undefined;
    for (&threads) |*thread| thread.* = try std.Thread.spawn(.{}, Worker.run, .{ &q, &violations });
    for (threads) |thread| thread.join();

    try std.testing.expectEqual(@as(usize, 0), violations.load(.monotonic));
    const final = q.snapshot();
    try std.testing.expectEqual(@as(usize, 0), final.active_requests);
    try std.testing.expectEqual(@as(usize, 0), final.active_units);
}
