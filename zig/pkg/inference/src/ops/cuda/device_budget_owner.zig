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

//! Node leases narrow the physical backstop for the lifetime of the runtime.
//! The tightest live owner wins; removing an owner never forgives allocations.
const std = @import("std");
const accounting = @import("allocation_budget.zig");

pub var process: Registry = .{ .budget = &accounting.process_device_allocations };

pub const Owner = struct {
    registry: *Registry,
    allocator: std.mem.Allocator,
    limit: usize,
    next: ?*Owner = null,

    pub fn deinit(self: *Owner) void {
        const registry = self.registry;
        registry.lock();
        var link = &registry.owners;
        while (link.*) |owner| {
            if (owner == self) {
                link.* = owner.next;
                break;
            }
            link = &owner.next;
        } else unreachable;
        var limit = registry.unowned_limit;
        var current = registry.owners;
        while (current) |owner| : (current = owner.next) limit = @min(limit, owner.limit);
        // Removing a constraint can only widen the limit. Outstanding and
        // quarantined allocations remain charged to the physical backstop.
        registry.budget.setLimit(limit) catch unreachable;
        registry.mutex.unlock();
        const allocator = self.allocator;
        allocator.destroy(self);
    }
};

pub const Registry = struct {
    budget: *accounting.Budget,
    mutex: std.atomic.Mutex = .unlocked,
    owners: ?*Owner = null,
    unowned_limit: usize = std.math.maxInt(usize),

    fn lock(self: *Registry) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn acquire(self: *Registry, allocator: std.mem.Allocator, requested: usize) !*Owner {
        const owner = try allocator.create(Owner);
        errdefer allocator.destroy(owner);
        const limit = if (requested == 0) std.math.maxInt(usize) else requested;
        self.lock();
        defer self.mutex.unlock();
        const before = self.budget.snapshot();
        if (self.owners == null) self.unowned_limit = before.limit;
        try self.budget.setLimit(@min(before.limit, limit));
        owner.* = .{ .registry = self, .allocator = allocator, .limit = limit, .next = self.owners };
        self.owners = owner;
        return owner;
    }
};

test "CUDA device budget owners preserve the tightest live cap and outstanding charges" {
    var budget = accounting.Budget{};
    var registry = Registry{ .budget = &budget };
    const a = try registry.acquire(std.testing.allocator, 64);
    const b = try registry.acquire(std.testing.allocator, 32);
    const unbounded = try registry.acquire(std.testing.allocator, 0);
    try std.testing.expectEqual(@as(usize, 32), budget.snapshot().limit);
    try budget.reserve(24);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, registry.acquire(std.testing.allocator, 16));
    try std.testing.expectEqual(@as(usize, 32), budget.snapshot().limit);
    b.deinit();
    try std.testing.expectEqual(@as(usize, 64), budget.snapshot().limit);
    a.deinit();
    try std.testing.expectEqual(std.math.maxInt(usize), budget.snapshot().limit);
    unbounded.deinit();
    try std.testing.expectEqual(@as(usize, 24), budget.snapshot().live);
    budget.release(24);
    const reloaded = try registry.acquire(std.testing.allocator, 16);
    reloaded.deinit();
}
