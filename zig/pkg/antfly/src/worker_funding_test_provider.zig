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

//! A real worker producer compiled independently of its borrower and allocator.
const std = @import("std");
const abi = @import("api/kernel_abi.zig");
const workers = @import("api/sql_worker_lease.zig");
const background = @import("antfly_local_sources").storage_background_runtime;
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
const Node = struct {
    allocator: abi.memory_abi.Allocator,
    runtime: background.BackendRuntimeHandle,
};
const Dispatch = @FieldType(abi.native_abi.IoBorrow, "dispatch");
const P = @typeInfo(@typeInfo(Dispatch).pointer.child).@"fn".param_types;
var original: std.atomic.Value(?Dispatch) = .init(null);
fn forward(a: P[0].?, b: P[1].?, c: P[2].?, d: P[3].?, e: P[4].?, f: P[5].?) callconv(.c) void {
    original.load(.acquire).?(a, b, c, d, e, f);
}
fn acquire(raw: *anyopaque, role: c_int, capacity: usize, funding: ?*const abi.memory_abi.Allocator, out: *abi.SqlWorkerBorrow) callconv(.c) abi.Status {
    const node: *Node = @ptrCast(@alignCast(raw));
    const factory = workers.factory(node.runtime.ptr()).?;
    const status = factory.acquire(factory.context, role, capacity, funding, out);
    if (status.isOk()) {
        // Force the C dispatch path even if the linker coalesces identical
        // native helpers from the two independently compiled archives.
        original.store(out.io.dispatch, .release);
        out.io.dispatch = forward;
    }
    return status;
}
export fn antfly_worker_funding_fixture_create(allocator: *const abi.memory_abi.Allocator, out: *abi.SqlWorkerFactory, context: *?*anyopaque) callconv(.c) abi.Status {
    if (!allocator.valid()) return abi.statusFromError(error.UnsupportedVersion);
    const node = allocator.asStd().create(Node) catch return abi.statusFromError(error.OutOfMemory);
    node.allocator = allocator.*;
    node.runtime = background.BackendRuntimeHandle.init(node.allocator.asStd(), .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 16, .sql_worker_capacity = 16 } }) catch |err| {
        allocator.asStd().destroy(node);
        return abi.statusFromError(err);
    };
    out.* = workers.factory(node.runtime.ptr()).?;
    out.context = node;
    out.acquire = acquire;
    context.* = node;
    return .ok;
}
export fn antfly_worker_funding_fixture_reserved(context: *anyopaque) callconv(.c) usize {
    const node: *Node = @ptrCast(@alignCast(context));
    return node.runtime.ptr().laneStats().reserved_workers;
}
export fn antfly_worker_funding_fixture_destroy(context: *anyopaque) callconv(.c) void {
    const node: *Node = @ptrCast(@alignCast(context));
    const allocator = node.allocator;
    node.runtime.deinit();
    allocator.asStd().destroy(node);
}
