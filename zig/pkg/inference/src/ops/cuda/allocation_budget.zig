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

//! Per-owner physical DeviceBuffer accounting. Cached and deferred-free buffers
//! stay charged until cuMemFree succeeds. Driver/library-private memory is not
//! visible here and requires separate deployment headroom.
const std = @import("std");

pub const Budget = struct {
    mutex: std.atomic.Mutex = .unlocked,
    limit: usize = std.math.maxInt(usize),
    live: usize = 0,
    peak: usize = 0,

    pub const Snapshot = struct { limit: usize, live: usize, peak: usize };

    pub fn snapshot(self: *Budget) Snapshot {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        return .{ .limit = self.limit, .live = self.live, .peak = self.peak };
    }

    pub fn setLimit(self: *Budget, limit: usize) error{CudaMemoryLimitExceeded}!void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (limit == 0 or self.live > limit) return error.CudaMemoryLimitExceeded;
        self.limit = limit;
    }

    pub fn reserve(self: *Budget, bytes: usize) error{CudaMemoryLimitExceeded}!void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (bytes > self.limit - self.live) return error.CudaMemoryLimitExceeded;
        self.live += bytes;
        self.peak = @max(self.peak, self.live);
    }

    pub fn release(self: *Budget, bytes: usize) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        std.debug.assert(bytes <= self.live);
        self.live -= bytes;
    }
};

test "CUDA boundary physical allocation budget rejects overflow and preserves failed reservations" {
    var budget: Budget = .{};
    try budget.reserve(12);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, budget.setLimit(11));
    try budget.setLimit(32);
    try budget.reserve(20);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, budget.reserve(1));
    try std.testing.expectError(error.CudaMemoryLimitExceeded, budget.reserve(std.math.maxInt(usize)));
    try std.testing.expectEqual(@as(usize, 32), budget.snapshot().live);
    budget.release(20);
    try budget.reserve(20);
    budget.release(32);
    try std.testing.expectEqual(@as(usize, 0), budget.snapshot().live);
    try std.testing.expectEqual(@as(usize, 32), budget.snapshot().peak);
}
