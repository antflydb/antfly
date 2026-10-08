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

const std = @import("std");
const resources = @import("../resource_manager.zig");

/// Reuse small writer batches without retaining an exceptional batch's peak.
pub const Scratch = struct {
    pub const max_retained_keys = 1024;
    keys: std.ArrayListUnmanaged([]const u8) = .empty,
    indexes: std.ArrayListUnmanaged(usize) = .empty,
    values: std.ArrayListUnmanaged(?[]const u8) = .empty,
    pub fn prepareKeys(self: *Scratch, a: std.mem.Allocator, n: usize) !void {
        try self.keys.ensureTotalCapacityPrecise(a, n);
        try self.indexes.ensureTotalCapacityPrecise(a, n);
        self.keys.items.len = n;
        self.indexes.items.len = n;
    }
    pub fn prepareValues(self: *Scratch, a: std.mem.Allocator, n: usize) !void {
        try self.values.ensureTotalCapacityPrecise(a, n);
        self.values.items.len = n;
    }
    pub fn prepare(self: *Scratch, a: std.mem.Allocator, n: usize) !void {
        try self.prepareKeys(a, n);
        try self.prepareValues(a, n);
    }
    pub fn deinit(self: *Scratch, a: std.mem.Allocator) void {
        self.keys.deinit(a);
        self.indexes.deinit(a);
        self.values.deinit(a);
        self.* = .{};
    }
};

/// Probe compaction uses separate arrays from the writer's input batch. This
/// storage owns metadata only; returned values keep their existing owners.
pub const ProbeScratch = struct {
    resolved: std.ArrayListUnmanaged(bool) = .empty,
    pending: Scratch = .{},
    planner: ?*Planner = null,

    /// Heap-stable allocator contexts allow the scratch owner to move. Retain
    /// at most 64 KiB of planning metadata, with its resource charge intact.
    pub const Planner = struct {
        pub const retained_bytes = 64 * 1024;
        budget: ?resources.BudgetedAllocator,
        arena: std.heap.ArenaAllocator,
        pub fn finish(self: *Planner) void {
            _ = self.arena.reset(.{ .retain_with_limit = retained_bytes });
            // Idle scratch must retain only its live charge, not the budget's
            // amortized spare credit (which can otherwise be 1 MiB per reader).
            if (self.budget) |*budget| _ = budget.releaseUnusedCredit();
        }
        pub fn denied(self: *Planner) bool {
            return if (self.budget) |*budget| budget.denied() else false;
        }
    };

    pub fn planning(self: *ProbeScratch, owner: std.mem.Allocator, backing: std.mem.Allocator, manager: ?*resources.ResourceManager) !*Planner {
        if (self.planner == null) {
            const planner = try owner.create(Planner);
            planner.budget = if (manager) |m| resources.BudgetedAllocator.init(m, .lsm_in_memory_state, backing, 1) else null;
            if (planner.budget) |*budget| budget.credit_quantum = 4096;
            planner.arena = std.heap.ArenaAllocator.init(if (planner.budget) |*budget| budget.allocator() else backing);
            self.planner = planner;
        }
        const planner = self.planner.?;
        if (planner.budget) |*budget| budget.budget_denied = false;
        return planner;
    }
    pub fn prepareResolved(self: *ProbeScratch, a: std.mem.Allocator, n: usize) ![]bool {
        try self.resolved.ensureTotalCapacityPrecise(a, n);
        self.resolved.items.len = n;
        @memset(self.resolved.items, false);
        return self.resolved.items;
    }
    pub fn deinit(self: *ProbeScratch, a: std.mem.Allocator) void {
        self.resolved.deinit(a);
        self.pending.deinit(a);
        if (self.planner) |planner| {
            planner.arena.deinit();
            if (planner.budget) |*budget| budget.deinit();
            a.destroy(planner);
        }
        self.* = .{};
    }
};

test "lsm writer batch scratch reuses allocations and unwinds failures" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var scratch: Scratch = .{};
            defer scratch.deinit(a);
            try scratch.prepare(a, 16);
            const keys = scratch.keys.items.ptr;
            const indexes = scratch.indexes.items.ptr;
            const values = scratch.values.items.ptr;
            try scratch.prepare(a, 16);
            try std.testing.expect(keys == scratch.keys.items.ptr);
            try std.testing.expect(indexes == scratch.indexes.items.ptr);
            try std.testing.expect(values == scratch.values.items.ptr);
            try scratch.prepare(a, 32);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "lsm probe batch scratch reuses all arrays and unwinds growth failures" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var scratch: ProbeScratch = .{};
            defer scratch.deinit(a);
            _ = try scratch.prepareResolved(a, 16);
            try scratch.pending.prepare(a, 16);
            const flags = scratch.resolved.items.ptr;
            const keys = scratch.pending.keys.items.ptr;
            const indexes = scratch.pending.indexes.items.ptr;
            const values = scratch.pending.values.items.ptr;
            @memset(scratch.resolved.items, true);
            const resolved = try scratch.prepareResolved(a, 8);
            try scratch.pending.prepare(a, 8);
            try std.testing.expect(flags == resolved.ptr);
            try std.testing.expect(keys == scratch.pending.keys.items.ptr);
            try std.testing.expect(indexes == scratch.pending.indexes.items.ptr);
            try std.testing.expect(values == scratch.pending.values.items.ptr);
            for (resolved) |flag| try std.testing.expect(!flag);
            _ = try scratch.prepareResolved(a, 32);
            try scratch.pending.prepare(a, 32);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "lsm probe planning scratch retains bounded charged memory and reuses allocations" {
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var backing = Budget{ .backing = std.testing.allocator };
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var scratch: ProbeScratch = .{};
    const planner = try scratch.planning(std.testing.allocator, backing.allocator(), &manager);
    _ = try planner.arena.allocator().alloc(u8, 4096);
    planner.finish();
    const calls = backing.alloc_calls;
    for (0..100) |_| {
        _ = try planner.arena.allocator().alloc(u8, 4096);
        planner.finish();
    }
    try std.testing.expectEqual(calls, backing.alloc_calls);
    try std.testing.expect(planner.budget.?.live_bytes != 0);
    try std.testing.expectEqual(planner.budget.?.live_bytes, manager.sliceStats(.lsm_in_memory_state).used_bytes);
    _ = try planner.arena.allocator().alloc(u8, 1024 * 1024);
    planner.finish();
    try std.testing.expect(backing.live <= ProbeScratch.Planner.retained_bytes + 128);
    try std.testing.expectEqual(planner.budget.?.live_bytes, manager.sliceStats(.lsm_in_memory_state).used_bytes);
    scratch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), backing.live);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_in_memory_state).used_bytes);
}

test "lsm probe planning scratch unwinds every allocation failure" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var scratch: ProbeScratch = .{};
            defer scratch.deinit(a);
            const planner = try scratch.planning(a, a, null);
            _ = try planner.arena.allocator().alloc(u8, 4096);
            _ = try planner.arena.allocator().alloc(u8, 128 * 1024);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
