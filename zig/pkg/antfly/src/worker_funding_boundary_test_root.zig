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

//! C allocator and executor contracts across independently compiled owners.
const std = @import("std");
const abi = @import("api/kernel_abi.zig");
const Lease = @import("api/sql_worker_lease.zig").Lease;
const Shared = @import("common/allocation_budget.zig").Shared;
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
extern fn antfly_worker_funding_fixture_create(*const abi.memory_abi.Allocator, *abi.SqlWorkerFactory, *?*anyopaque) callconv(.c) abi.Status;
extern fn antfly_worker_funding_fixture_reserved(*anyopaque) callconv(.c) usize;
extern fn antfly_worker_funding_fixture_destroy(*anyopaque) callconv(.c) void;

test "SQL worker funding archive boundary retains caller memory through physical executor join" {
    var node_backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var node_allocator = node_backing.allocator();
    const port = abi.memory_abi.Allocator.fromStd(&node_allocator);
    var factory: abi.SqlWorkerFactory = undefined;
    var context: ?*anyopaque = null;
    const status = antfly_worker_funding_fixture_create(&port, &factory, &context);
    if (!status.isOk()) return abi.errorFromStatus(status);
    defer antfly_worker_funding_fixture_destroy(context.?);
    node_backing.fail_index = node_backing.alloc_index;
    node_backing.resize_fail_index = node_backing.resize_index;
    defer {
        node_backing.fail_index = std.math.maxInt(usize);
        node_backing.resize_fail_index = std.math.maxInt(usize);
    }
    var memory: Shared = .{ .child = std.testing.allocator, .limit = 512 << 10 };
    var lease = try Lease.acquireImported(memory.allocator(), factory, 1);
    defer lease.release();
    try std.testing.expect(@as(*const anyopaque, lease.io().vtable) != lease.imported.?.io.vtable);
    const Work = struct {
        fn run(done: *std.atomic.Value(bool)) void {
            done.store(true, .release);
        }
    };
    var done: std.atomic.Value(bool) = .init(false);
    const io = lease.io();
    var future = try io.concurrent(Work.run, .{&done});
    future.await(io);
    try std.testing.expect(done.load(.acquire));
    try std.testing.expect(memory.stats().used_bytes != 0);
    try std.testing.expect(!node_backing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 1), antfly_worker_funding_fixture_reserved(context.?));
    lease.release();
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), antfly_worker_funding_fixture_reserved(context.?));
}
