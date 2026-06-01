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
// httpx uses fiber-based concurrency on a single OS thread,
// so a simple counter suffices (no mutex needed).

const std = @import("std");

pub const RequestQueue = struct {
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
        const requested = @min(@max(units, 1), self.max_concurrent);
        if (self.active_units + requested > self.max_concurrent) {
            return error.QueueFull;
        }
        self.active_requests += 1;
        self.active_units += requested;
    }

    /// Release a slot after request completes.
    pub fn release(self: *RequestQueue) void {
        self.releaseUnits(1);
    }

    pub fn releaseUnits(self: *RequestQueue, units: usize) void {
        const requested = @min(@max(units, 1), self.max_concurrent);
        if (self.active_requests > 0) self.active_requests -= 1;
        if (self.active_units > requested) {
            self.active_units -= requested;
        } else {
            self.active_units = 0;
        }
    }

    pub fn depth(self: *const RequestQueue) usize {
        return self.active_units;
    }

    pub fn requests(self: *const RequestQueue) usize {
        return self.active_requests;
    }

    pub fn available(self: *const RequestQueue) usize {
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
}
