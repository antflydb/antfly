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
// The same Node can serve httpx requests and embedded/direct providers from
// different server worker threads, so admission accounting is synchronized.

const std = @import("std");

fn spinLock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

pub const RequestQueue = struct {
    // Zig 0.16's Io.Mutex requires an Io at each call site. This critical
    // section is only a few integer operations, so the standard spin mutex is
    // the smallest cross-thread primitive that fits both HTTP and direct APIs.
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
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const requested = self.capacityUnits(units);
        if (requested > self.max_concurrent - self.active_units) {
            return error.QueueFull;
        }
        self.active_requests += 1;
        self.active_units += requested;
    }

    /// Grow an admitted request after bounded preflight reveals its real work.
    /// The resize is atomic so concurrent requests cannot overbook capacity.
    pub fn growUnits(self: *RequestQueue, current_units: usize, target_units: usize) !void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const current = self.capacityUnits(current_units);
        const target = self.capacityUnits(target_units);
        if (target <= current) return;
        const additional = target - current;
        if (additional > self.max_concurrent - self.active_units) return error.QueueFull;
        self.active_units += additional;
    }

    /// Release a slot after request completes.
    pub fn release(self: *RequestQueue) void {
        self.releaseUnits(1);
    }

    pub fn releaseUnits(self: *RequestQueue, units: usize) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const requested = self.capacityUnits(units);
        if (self.active_requests > 0) self.active_requests -= 1;
        if (self.active_units > requested) {
            self.active_units -= requested;
        } else {
            self.active_units = 0;
        }
    }

    pub fn capacityUnits(self: *const RequestQueue, units: usize) usize {
        // A zero limit is the documented escape hatch for unlimited
        // admission. Requests then consume zero accounting units.
        return @min(@max(units, 1), self.max_concurrent);
    }

    pub fn depth(self: *RequestQueue) usize {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.active_units;
    }

    pub fn requests(self: *RequestQueue) usize {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.active_requests;
    }

    pub fn available(self: *RequestQueue) usize {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.max_concurrent - self.active_units;
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

test "request queue grows weighted admission atomically" {
    var q = RequestQueue.init(8);
    try q.acquireUnits(2);
    try q.acquireUnits(3);

    try q.growUnits(2, 5);
    try std.testing.expectEqual(@as(usize, 8), q.depth());
    try std.testing.expectError(error.QueueFull, q.growUnits(3, 4));
    try std.testing.expectEqual(@as(usize, 8), q.depth());

    q.releaseUnits(5);
    q.releaseUnits(3);
    try std.testing.expectEqual(@as(usize, 0), q.depth());
}

test "request queue zero limit is unlimited" {
    var q = RequestQueue.init(0);

    try q.acquireUnits(std.math.maxInt(usize));
    try q.acquire();
    try std.testing.expectEqual(@as(usize, 2), q.requests());
    try std.testing.expectEqual(@as(usize, 0), q.depth());
    try std.testing.expectEqual(@as(usize, 0), q.available());

    q.releaseUnits(std.math.maxInt(usize));
    q.release();
    try std.testing.expectEqual(@as(usize, 0), q.requests());
}

test "request queue synchronizes embedded and HTTP callers" {
    const Worker = struct {
        fn run(q: *RequestQueue) void {
            for (0..2_000) |_| {
                q.acquire() catch unreachable;
                q.release();
            }
        }
    };

    var q = RequestQueue.init(4);
    var threads: [4]std.Thread = undefined;
    for (&threads) |*thread| thread.* = try std.Thread.spawn(.{}, Worker.run, .{&q});
    for (&threads) |*thread| thread.join();

    try std.testing.expectEqual(@as(usize, 0), q.requests());
    try std.testing.expectEqual(@as(usize, 0), q.depth());
}
