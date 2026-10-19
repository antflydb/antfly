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

//! Trusted completion on a local ingress execution. Keep this bridge and its
//! executor at a stable address through the independent task's physical join.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;

pub const Bridge = struct {
    read: @import("runnable_local_read.zig").Read,
    io: std.Io,

    pub fn borrow(self: *Bridge) !abi.Borrowed {
        var value = (try self.read.team()).borrow;
        value.reserve_completion = reserve;
        value.activate_completion = activate;
        value.completion_remaining_ns = remaining;
        return value;
    }
    fn from(raw: *anyopaque) *Bridge {
        const read: *@import("runnable_local_read.zig").Read = @ptrCast(@alignCast(raw));
        return @fieldParentPtr("read", read);
    }
    fn reserve(raw: *anyopaque, options: *const abi.Completion) callconv(.c) runtime.error_abi.Status {
        reserveChecked(from(raw), options) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn reserveChecked(self: *Bridge, options: *const abi.Completion) !void {
        try options.validate();
        // This local bridge grants only its already-retained executor and the
        // platform monotonic clock. It cannot import a different cancellation
        // scope or extend authority using an arbitrary clock receiver.
        if (options.native_deadline != 1 or options.clock_io != null) return error.InvalidArgument;
        const local = runtime.native_abi.IoBorrow.init(&self.io);
        if (options.io.userdata != local.userdata or options.io.vtable != local.vtable or options.io.dispatch != local.dispatch or
            !options.io.contract.matches(local.contract)) return error.InvalidArgument;
        try self.read.execution.reserveCompletion(.{
            .io = self.io,
            .native_now_ns = @import("antfly_platform").time.monotonicNs,
            .deadline_ns = options.deadline_ns,
        });
    }
    fn activate(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        from(raw).read.execution.activateCompletion() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn remaining(raw: *anyopaque, out: *u64) callconv(.c) runtime.error_abi.Status {
        out.* = from(raw).read.execution.completionRemainingNs() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
};
