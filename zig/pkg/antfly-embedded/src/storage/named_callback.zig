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

//! Private callbacks between compiler/layout-checked native storage owners.
//! Error names are produced in the callback's compilation unit. No foreign
//! numeric Zig error identity is interpreted in the receiving compilation unit.
const std = @import("std");
const native = @import("antfly_runtime_abi").native_abi;
const status = @import("antfly_runtime_abi").error_abi;
const Failure = struct { name: ?[*:0]const u8 = null };
fn Output(comptime T: type) type {
    return struct { value: T = undefined, failure: Failure = .{} };
}
pub const Dispatch = *const fn (*const native.CallContract, *const anyopaque, *const anyopaque, *anyopaque) callconv(.c) status.Status;
pub fn Boundary(comptime VTable: type) type {
    return struct {
        pub const local_dispatch: Dispatch = &dispatchLocal;
        comptime {
            native.assertUniqueMethodIds(VTable);
        }
        fn Function(comptime Pointer: type) type {
            return @typeInfo(Pointer).pointer.child;
        }
        fn Payload(comptime Pointer: type) type {
            const Return = @typeInfo(Function(Pointer)).@"fn".return_type.?;
            return @typeInfo(Return).error_union.payload;
        }
        pub fn call(comptime name: []const u8, dispatch: Dispatch, callback: anytype, args: anytype) anyerror!Payload(@TypeOf(callback)) {
            const Pointer = @TypeOf(callback);
            const Args = std.meta.ArgsTuple(Function(Pointer));
            const T = Payload(Pointer);
            var typed_args: Args = args;
            if (dispatch == local_dispatch) return @call(.auto, callback, typed_args);
            var output: Output(T) = undefined;
            const contract = native.CallContract.of(name, Pointer, Args, Output(T));
            const result = dispatch(&contract, @ptrCast(callback), @ptrCast(&typed_args), @ptrCast(&output));
            if (!result.isOk()) return status.errorFromStatus(result);
            if (output.failure.name) |value| return @import("storage_engine_errors").fromName(std.mem.span(value));
            return output.value;
        }
        fn dispatchLocal(contract: *const native.CallContract, callback: *const anyopaque, args: *const anyopaque, output: *anyopaque) callconv(.c) status.Status {
            @setEvalBranchQuota(100_000);
            if (contract.version != native.abi_version) return status.statusFromError(error.UnsupportedVersion);
            const info = @typeInfo(VTable).@"struct";
            inline for (info.field_names, info.field_types) |name, Field| {
                if (contract.method_id == native.stableId(name)) {
                    const Pointer = if (@typeInfo(Field) == .optional) @typeInfo(Field).optional.child else Field;
                    if (comptime @typeInfo(Pointer) != .pointer) continue;
                    if (comptime @typeInfo(@typeInfo(Pointer).pointer.child) != .@"fn") continue;
                    if (comptime @typeInfo(@typeInfo(Function(Pointer)).@"fn".return_type.?) != .error_union) continue;
                    const Args = std.meta.ArgsTuple(Function(Pointer));
                    const T = Payload(Pointer);
                    const expected = native.CallContract.of(name, Pointer, Args, Output(T));
                    if (!contract.matches(expected)) return status.statusFromError(error.InvalidArgument);
                    const method: Pointer = @ptrCast(@alignCast(callback));
                    const typed_args: *const Args = @ptrCast(@alignCast(args));
                    const typed_output: *Output(T) = @ptrCast(@alignCast(output));
                    typed_output.* = .{ .value = @call(.auto, method, typed_args.*) catch |err| {
                        typed_output.* = .{ .failure = .{ .name = @errorName(err).ptr } };
                        return .ok;
                    } };
                    return .ok;
                }
            }
            return status.statusFromError(error.InvalidArgument);
        }
    };
}
