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

//! Independently linked consumer of the synchronous batch admission contract.
//! This exercises the archive boundary, not a physical DB implementation.
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const batch = @import("api/batch_execution_admission.zig");
const read = @import("common/read_admission.zig");
const wait = @import("common/execution_wait.zig");
const mutation = @import("storage/db/mutation_admission.zig");

const Wait = struct {
    context: *anyopaque,
    callback: *const fn (*anyopaque) callconv(.c) runtime.error_abi.Status,
    fn run(self: @This()) !void {
        const status = self.callback(self.context);
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
    }
};

export fn antfly_batch_visibility_archive_run(raw: *const anyopaque, context: *anyopaque, callback: *const fn (*anyopaque) callconv(.c) runtime.error_abi.Status, foreign_dispatches: *u32) callconv(.c) runtime.error_abi.Status {
    foreign_dispatches.* = 0;
    execute(raw, .{ .context = context, .callback = callback }, foreign_dispatches) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}

fn execute(raw: *const anyopaque, physical_wait: Wait, foreign_dispatches: *u32) !void {
    var borrow: batch.Borrow = .{};
    var controls = try borrow.bind(raw);
    if (controls.execution_wait != null) return error.InvalidRunnableTransition;
    const root = borrow.roots.root orelse return error.InvalidArgument;
    const parent = borrow.roots.retirement orelse return error.InvalidArgument;
    const boundary = borrow.roots.committed_visibility orelse return error.ExecutionWaitUnsupported;
    const original_payload: *const read.Payload = @ptrCast(@alignCast(borrow.original.payload));
    if (original_payload.dispatch == read.Boundary.local_dispatch or boundary.boundary_dispatch == wait.Boundary.local_dispatch)
        return error.UnsupportedOperation;
    foreign_dispatches.* = 2;
    const Check = struct {
        controls: batch.Controls,
        pub fn checkpoint(self: *@This()) !void {
            try self.controls.lifetime.check();
        }
    };
    var check: Check = .{ .controls = controls };
    var inner: ?runtime.mutation_barrier.MutationBarrier.SharedLease = try root.borrowControlled(&check);
    defer if (inner) |*lease| lease.release();
    controls.execution_wait = boundary;
    if (mutation.committedVisibilityBoundary(controls, .{ .root = root, .capture_barrier_present = true, .mutation_reader_active = true }) != null)
        return error.InvalidRunnableTransition;
    (mutation.MutationRetirement{ .current = &inner, .enclosing = parent }).retire();
    const eligible = mutation.committedVisibilityBoundary(controls, .{ .root = root, .capture_barrier_present = true, .mutation_reader_active = inner != null or parent.* != null }) orelse return error.InvalidRunnableTransition;
    try eligible.run(Wait.run, .{physical_wait});
    std.debug.assert(!root.held and root.borrowers.load(.acquire) == 0);
}
