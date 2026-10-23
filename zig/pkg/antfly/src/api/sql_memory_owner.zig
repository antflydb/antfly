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

//! Heap-owned shared host admission for materialized and streaming SQL. The
//! allocator survives Adapter/request stack frames until delivery is released.
const std = @import("std");
const local = @import("antfly_local_sources");
const resources = local.storage_resource_manager;
const runtime = local.sql_runtime;

const Owner = struct {
    control: std.mem.Allocator,
    budget: resources.BudgetedAllocator,

    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const control = self.control;
        std.debug.assert(self.budget.live_bytes == 0);
        self.budget.deinit();
        control.destroy(self);
    }
    fn available(raw: *anyopaque) usize {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return std.math.cast(usize, self.budget.reservation.manager.availableAdmissionBytes(.sql_execution_working_set)) orelse std.math.maxInt(usize);
    }
    fn exhausted(raw: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return if (self.budget.allocationFailureThreadSafe()) |failure| failure.cause == .admission else false;
    }
};

pub fn create(control: std.mem.Allocator, backing: std.mem.Allocator, manager: *resources.ResourceManager) !runtime.MemoryOwner {
    const self = try control.create(Owner);
    self.* = .{ .control = control, .budget = resources.BudgetedAllocator.initReclaiming(manager, .sql_execution_working_set, local.sql_memory_budget.reclaimingAllocator(backing), 1) };
    return .{ .allocator = self.budget.threadSafeAllocator(), .pressure = .{ .ptr = self, .exhausted = Owner.exhausted, .capacity = .{ .backing = self.budget.backing, .available = Owner.available } }, .ptr = self, .release = Owner.release };
}

test "SQL shared memory owners reclaim cache capacity and retain host admission until delivery ends" {
    const a = std.testing.allocator;
    const Cache = struct {
        retained: resources.Reservation,
        calls: usize = 0,
        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const bytes = @min(target, self.retained.bytes);
            self.retained.shrink(bytes);
            return bytes;
        }
    };
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 2 * 1024 * 1024 } });
    defer manager.deinit(a);
    var cache: Cache = .{ .retained = try manager.reserve(.lite_native_page_cache, 2 * 1024 * 1024) };
    defer cache.retained.release();
    const id = try manager.registerReclaimer(.lite_native_page_cache, &cache, Cache.reclaim);
    defer manager.unregisterReclaimer(id);
    {
        const second = try create(a, a, &manager);
        defer second.deinit();
        const right = try second.allocator.alloc(u8, 1024);
        defer second.allocator.free(right);
        {
            const first = try create(a, a, &manager);
            defer first.deinit();
            const left = try first.allocator.alloc(u8, 1024);
            defer first.allocator.free(left);
            try std.testing.expect(cache.calls > 0);
            try std.testing.expect(manager.sliceStats(.sql_execution_working_set).used_bytes >= 2048);
            try std.testing.expectError(error.OutOfMemory, second.allocator.alloc(u8, 3 * 1024 * 1024));
            try std.testing.expectEqual(error.SqlWorkingMemoryLimitExceeded, second.mapError(error.OutOfMemory));
        }
        // Owners independently release their live bytes and retained credit.
        try std.testing.expect(manager.sliceStats(.sql_execution_working_set).used_bytes > 0);
        var workspace: local.sql_memory_budget = .{ .backing = second.allocator, .limit = 2 * 1024 * 1024, .pressure = second.pressure };
        const headroom = local.sql_memory_budget.reusableHeadroom(workspace.allocator()).?;
        try std.testing.expect(headroom > 0);
        try std.testing.expectEqual(@min(@as(u64, workspace.limit), manager.availableAdmissionBytes(.sql_execution_working_set)), headroom);
        // Opaque caller quotas remain opaque even through shared admission.
        var unknown_backing = std.testing.FailingAllocator.init(a, .{});
        const third = try create(a, unknown_backing.allocator(), &manager);
        defer third.deinit();
        var unknown: local.sql_memory_budget = .{ .backing = third.allocator, .limit = 2 * 1024 * 1024, .pressure = third.pressure };
        try std.testing.expect(local.sql_memory_budget.reusableHeadroom(unknown.allocator()) == null);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.sql_execution_working_set).used_bytes);
}
