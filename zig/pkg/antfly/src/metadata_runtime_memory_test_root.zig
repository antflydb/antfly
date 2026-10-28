// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const local = @import("antfly_local_sources");
const resources = local.storage_resource_manager;
const Owner = @import("metadata/runtime_resources.zig").Owner;
pub const antfly_sources = @import("source_owner_physical.zig");

test "metadata runtime memory remains stable across owner moves and physical IO retirement" {
    var root = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 2048 << 20 } });
    defer root.deinit(std.testing.allocator);
    var first = try Owner.init(std.testing.allocator, .{ .parent = root.memoryParentPort() });
    var first_live = true;
    errdefer if (first_live) first.deinit();
    const manager = first.manager;
    var lane = try first.ptr().acquireControlLane();
    var lane_live = true;
    defer if (lane_live) lane.release();
    const io = lane.io();
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    var task = try io.concurrent(Work.run, .{});
    var moved = first;
    first = undefined;
    first_live = false;
    var live = true;
    defer if (live) {
        if (lane_live) {
            lane.release();
            lane_live = false;
        }
        moved.deinit();
    };
    try std.testing.expectEqual(manager, moved.manager);
    try std.testing.expectEqual(@as(u32, 42), task.await(io));
    try std.testing.expect(moved.ptr().runtime_memory.?.stackStats().used > 0);
    try std.testing.expect(root.snapshot().memory.used_bytes > 0);
    lane.release();
    lane_live = false;
    moved.deinit();
    live = false;
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "metadata runtime memory shares parent exhaustion and retains prepaid SQL control" {
    var root = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 4096 << 20 } });
    defer root.deinit(std.testing.allocator);
    var a = try Owner.init(std.testing.allocator, .{ .parent = root.memoryParentPort() });
    var a_live = true;
    defer if (a_live) a.deinit();
    var b = try Owner.init(std.testing.allocator, .{ .parent = root.memoryParentPort() });
    var b_live = true;
    defer if (b_live) b.deinit();
    var control_a = try a.ptr().acquireSharedSqlWorkers(.control, 1);
    var control_a_live = true;
    defer if (control_a_live) control_a.release();
    var control_b = try b.ptr().acquireSharedSqlWorkers(.control, 1);
    var control_b_live = true;
    defer if (control_b_live) control_b.release();
    const held = (4096 << 20) - root.snapshot().memory.used_bytes;
    try std.testing.expect(root.reserveExternal(held));
    var held_live = true;
    defer if (held_live) root.releaseExternal(held);
    try std.testing.expectError(error.ResourceBudgetExceeded, Owner.init(std.testing.allocator, .{ .parent = root.memoryParentPort() }));
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    var task_a = try control_a.io().concurrent(Work.run, .{});
    var task_b = try control_b.io().concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), task_a.await(control_a.io()));
    try std.testing.expectEqual(@as(u32, 42), task_b.await(control_b.io()));
    control_a.release();
    control_a_live = false;
    control_b.release();
    control_b_live = false;
    root.releaseExternal(held);
    held_live = false;
    a.deinit();
    a_live = false;
    try std.testing.expect(b.manager.snapshot().memory.used_bytes > 0);
    b.deinit();
    b_live = false;
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "metadata runtime memory construction failures release child and parent charges" {
    const Probe = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var root = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 2048 << 20 }, .identity_allocator = allocator });
            defer root.deinit(allocator);
            defer std.debug.assert(root.snapshot().memory.used_bytes == 0);
            var owner = Owner.init(allocator, .{ .parent = root.memoryParentPort() }) catch |err| {
                // std.Io reports allocation/spawn refusal as concurrency
                // unavailable. Normalize only an actually injected allocation
                // failure for the allocator harness; retain every other error.
                const failing: *std.testing.FailingAllocator = @ptrCast(@alignCast(allocator.ptr));
                if (err == error.ConcurrencyUnavailable and failing.has_induced_failure) return error.OutOfMemory;
                return err;
            };
            owner.deinit();
            try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "metadata runtime memory rejects a parent too small for construction before publication" {
    var root = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1 } });
    defer root.deinit(std.testing.allocator);
    try std.testing.expectError(error.ResourceBudgetExceeded, Owner.init(std.testing.allocator, .{ .parent = root.memoryParentPort() }));
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "metadata runtime memory helper without parent funds runtime and SQL from one stable envelope" {
    var original = try Owner.init(std.testing.allocator, .{ .limit_bytes = 2048 << 20 });
    var owner = original;
    original = undefined;
    defer owner.deinit();
    const parent = owner.ptr().memoryParent().?;
    try std.testing.expectEqual(@as(u64, 2048 << 20), parent.snapshot().hard_limit_bytes);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(owner.manager)), parent.ptr);
    var sql_control = try owner.ptr().acquireSharedSqlWorkers(.control, 1);
    defer sql_control.release();
    const used = owner.manager.snapshot().memory.used_bytes;
    try std.testing.expect(used > 0);
    const held = (2048 << 20) - used;
    try std.testing.expect(owner.manager.reserveExternal(held));
    defer owner.manager.releaseExternal(held);
    try std.testing.expect(!owner.manager.reserveExternal(1));
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    var work = try sql_control.io().concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), work.await(sql_control.io()));
}

test "metadata runtime memory helper refuses an unfundable explicit no-parent envelope" {
    try std.testing.expectError(error.ResourceBudgetExceeded, Owner.init(std.testing.allocator, .{ .limit_bytes = 1 }));
}
