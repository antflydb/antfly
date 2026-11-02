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

//! Transport-independent ingress contract for HA administration and internal
//! replication operations. Public HTTP adapters and in-process runtimes use
//! the same request shape without manufacturing a legacy HTTP request.

const std = @import("std");

pub const Method = enum {
    get,
    post,
    put,
    delete,
};

pub const Request = struct {
    method: Method,
    target: []const u8,
    authorization: ?[]const u8 = null,
    content_type: ?[]const u8 = null,
    body: []const u8 = "",
    admission: ?@import("operation_admission.zig").Control = null,
};

pub const OwnedResponse = struct {
    owner_allocator: std.mem.Allocator,
    status: u16 = 200,
    content_type: []u8,
    body: []u8,

    pub fn deinit(self: *@This()) void {
        self.owner_allocator.free(self.content_type);
        if (self.body.len > 0) self.owner_allocator.free(self.body);
        self.* = undefined;
    }
};

pub const Executor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    const BoundaryAbi = @import("antfly_local_sources").runtime_callback_abi.Boundary(VTable);
    pub const VTable = struct {
        execute: *const fn (ptr: *anyopaque, request: Request) anyerror!OwnedResponse,
    };

    pub fn execute(self: Executor, request: Request) !OwnedResponse {
        return BoundaryAbi.call("execute", self.boundary_dispatch, self.vtable.execute, .{ self.ptr, request });
    }
};

test "storage.hot_standby typed HTTP operation response owns its body" {
    var response = OwnedResponse{
        .owner_allocator = std.testing.allocator,
        .content_type = try std.testing.allocator.dupe(u8, "text/plain"),
        .body = try std.testing.allocator.dupe(u8, "ok"),
    };
    response.deinit();
}

test "storage.ha HTTP executor preserves error identity across a foreign runtime callback" {
    const Fake = struct {
        fn execute(_: *anyopaque, _: Request) !OwnedResponse {
            return error.OutOfMemory;
        }
        fn dispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            return Executor.BoundaryAbi.local_dispatch(contract, callback, args, output);
        }
    };
    const executor: Executor = .{ .ptr = undefined, .vtable = &.{ .execute = Fake.execute }, .boundary_dispatch = Fake.dispatch };
    try std.testing.expectError(error.OutOfMemory, executor.execute(.{ .method = .get, .target = "/admin/v1/ha/primary/status" }));
}

test "HA original admission carries lifetime through typed foreign executor dispatch" {
    const runtime = @import("antfly_runtime_abi");
    const Fake = struct {
        checks: usize = 0,
        cause: ?anyerror = null,
        fn life(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            return if (self.cause) |cause| runtime.error_abi.statusFromError(cause) else .ok;
        }
        fn checkpoint(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            @panic("unexpected yielding checkpoint");
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            @panic("unexpected CPU transfer");
        }
        fn execute(raw: *anyopaque, request: Request) !OwnedResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const original = request.admission orelse return error.MissingAdmission;
            if (original.execution.context != @as(*anyopaque, self)) return error.WrongAdmissionOwner;
            var admission: @import("operation_admission.zig").Session = .{};
            try admission.bind(original);
            return error.OutOfMemory;
        }
        fn dispatch(contract: *const runtime.native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) runtime.error_abi.Status {
            return Executor.BoundaryAbi.local_dispatch(contract, callback, args, output);
        }
    };
    var state: Fake = .{};
    const io = std.testing.io;
    const request: Request = .{ .method = .post, .target = "/admin/v1/ha/primary/fence", .admission = .{
        .io = runtime.io_abi.Borrow.init(&io),
        .execution = .{ .context = &state, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.background), .checkpoint = Fake.checkpoint, .check_lifetime = Fake.life, .park = Fake.phase, .enter = Fake.phase },
    } };
    const executor: Executor = .{ .ptr = &state, .vtable = &.{ .execute = Fake.execute }, .boundary_dispatch = Fake.dispatch };
    try std.testing.expectError(error.OutOfMemory, executor.execute(request));
    try std.testing.expectEqual(@as(usize, 1), state.checks);
    state.cause = error.DeadlineExceeded;
    try std.testing.expectError(error.DeadlineExceeded, executor.execute(request));
    try std.testing.expectEqual(@as(usize, 2), state.checks);
}
