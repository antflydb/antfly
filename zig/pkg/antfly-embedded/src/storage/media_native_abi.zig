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

//! Same-compiler operation transport shared by media clients. No codec imports.
const std = @import("std");
pub const native = @import("antfly_runtime_abi").native_abi;
pub const work = @import("image_work_control");
pub const Failure = work.Failure;
pub fn Outcome(comptime T: type) type {
    return struct { value: T = undefined, failure: Failure = .{} };
}
pub const WorkBorrow = extern struct { context: ?*const anyopaque = null, check_fn: ?*const fn (?*const anyopaque) callconv(.c) Failure = null };
const Lease = struct {
    control: work.Control,
    fn check(context: ?*const anyopaque) callconv(.c) Failure {
        const self: *const Lease = @ptrCast(@alignCast(context.?));
        self.control.check() catch |err| return work.failure(err);
        return .{};
    }
    fn borrow(self: *const Lease) WorkBorrow {
        return if (self.control.check_fn == null) .{} else .{ .context = self, .check_fn = check };
    }
};
const Receiver = struct {
    borrowed: WorkBorrow,
    fn check(context: ?*const anyopaque) anyerror!void {
        const self: *const Receiver = @ptrCast(@alignCast(context.?));
        const identity = self.borrowed.check_fn.?(self.borrowed.context);
        if (identity.ptr == null) return;
        work.recordFailure(error.Canceled, identity);
        return error.Canceled;
    }
};
pub fn Protocol(comptime prefix: []const u8, comptime Methods: type, comptime extra_layouts: anytype) type {
    return struct {
        const Self = @This();
        pub fn Function(comptime name: []const u8) type {
            return @FieldType(Methods, name);
        }
        pub fn Args(comptime name: []const u8) type {
            return std.meta.ArgsTuple(@typeInfo(Function(name)).pointer.child);
        }
        pub fn Value(comptime name: []const u8) type {
            return @typeInfo(@typeInfo(@typeInfo(Function(name)).pointer.child).@"fn".return_type.?).error_union.payload;
        }
        pub fn Wire(comptime name: []const u8) type {
            return Outcome(Value(name));
        }
        pub fn signature(comptime name: []const u8) native.CallContract {
            @setEvalBranchQuota(100_000);
            return .of(prefix ++ name, Function(name), Args(name), Wire(name));
        }
        const layout_types = .{ Failure, WorkBorrow, std.mem.Allocator.VTable } ++ extra_layouts;
        pub const layouts = blk: {
            @setEvalBranchQuota(100_000);
            var out: [layout_types.len]native.TypeContract = undefined;
            for (layout_types, 0..) |T, i| out[i] = .of(T);
            break :blk out;
        };
        pub const Endpoint = extern struct {
            version: u32 = 2,
            contract: native.CallContract,
            layout_count: usize = layouts.len,
            layout: *const [layouts.len]native.TypeContract = &layouts,
            invoke: *const fn (*const anyopaque, *anyopaque, WorkBorrow) callconv(.c) void,
        };
        pub fn validate(comptime name: []const u8, endpoint: *const Endpoint) !void {
            if (endpoint.version != 2 or !endpoint.contract.matches(signature(name)) or endpoint.layout_count != layouts.len) return error.InvalidArgument;
            for (endpoint.layout.*, layouts) |actual, expected| if (!actual.matches(expected)) return error.InvalidArgument;
        }
        pub fn call(comptime name: []const u8, args: Args(name)) !Value(name) {
            const getter = @extern(*const fn () callconv(.c) *const Endpoint, .{ .name = prefix ++ name ++ "_v2" });
            const endpoint = getter();
            try validate(name, endpoint);
            const lease: Lease = .{ .control = work.current() };
            var out: Wire(name) = undefined;
            endpoint.invoke(&args, &out, lease.borrow());
            if (out.failure.localError()) |err| return err;
            if (out.failure.ptr) |ptr| return @import("engine_errors").fromName(ptr[0..out.failure.len]);
            return out.value;
        }
        fn Bridge(comptime Ops: type, comptime name: []const u8) type {
            return struct {
                fn invoke(raw: *const anyopaque, output: *anyopaque, borrowed: WorkBorrow) callconv(.c) void {
                    const receiver: Receiver = .{ .borrowed = borrowed };
                    const scope = work.Scope.enter(if (borrowed.check_fn != null) .{ .context = &receiver, .check_fn = Receiver.check } else .{});
                    defer scope.deinit();
                    const args: *const Args(name) = @ptrCast(@alignCast(raw));
                    const out: *Wire(name) = @ptrCast(@alignCast(output));
                    const value = @call(.auto, @field(Ops, name), args.*) catch |err| {
                        out.* = .{ .failure = work.failure(err) };
                        return;
                    };
                    out.* = .{ .value = value };
                }
                const endpoint: Endpoint = .{ .contract = signature(name), .invoke = invoke };
                fn get() callconv(.c) *const Endpoint {
                    return &endpoint;
                }
            };
        }
        pub fn exportOperations(comptime Ops: type) void {
            inline for (@typeInfo(Methods).@"struct".field_names) |name| @export(&Bridge(Ops, name).get, .{ .name = prefix ++ name ++ "_v2", .visibility = .hidden });
        }
    };
}
