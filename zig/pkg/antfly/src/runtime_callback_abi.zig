// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Checked native callback trampoline for hidden static runtime archives.
//!
//! The data plane deliberately remains zero-copy and native because all units
//! are compiled and statically linked by one Zig invocation. Every foreign call
//! carries a versioned method/signature/layout contract; the owning unit checks
//! it before interpreting a pointer. Errors use the language-neutral status
//! ABI, and allocator ownership uses runtime_memory_abi at explicit owner ABIs.

const std = @import("std");
const error_abi = @import("runtime_error_abi.zig");
const native_abi = @import("runtime_native_abi.zig");

pub const CallbackDispatch = *const fn (
    contract: *const native_abi.CallContract,
    callback: *const anyopaque,
    args: *const anyopaque,
    output: ?*anyopaque,
) callconv(.c) error_abi.Status;

pub fn Boundary(comptime VTable: type) type {
    return BoundaryImpl(VTable);
}

fn BoundaryImpl(comptime VTable: type) type {
    return struct {
        const Self = @This();

        pub const Dispatch = CallbackDispatch;

        pub const local_dispatch: Dispatch = &dispatchLocal;

        comptime {
            native_abi.assertUniqueMethodIds(VTable);
        }

        pub fn call(
            comptime field_name: []const u8,
            dispatch: Dispatch,
            callback: anytype,
            args: anytype,
        ) CallReturn(@TypeOf(callback)) {
            const Callback = @TypeOf(callback);
            const Function = functionType(Callback);
            const Args = std.meta.ArgsTuple(Function);
            const Payload = payloadType(Callback);
            _ = std.meta.fieldIndex(VTable, field_name) orelse
                @compileError("unknown boundary vtable field: " ++ field_name);
            var typed_args: Args = args;

            // A vtable created and consumed in this compilation unit retains
            // normal Zig error and return semantics. Only a dispatcher owned
            // by another runtime unit takes the C ABI path below.
            if (dispatch == local_dispatch)
                return @call(.auto, callback, typed_args);

            if (Payload == void) {
                const contract = native_abi.CallContract.of(field_name, Callback, Args, Payload);
                const status = dispatch(
                    &contract,
                    @ptrCast(callback),
                    @ptrCast(&typed_args),
                    null,
                );
                if (!status.isOk()) return error_abi.errorFromStatus(status);
                return;
            }

            var output: Payload = undefined;
            const contract = native_abi.CallContract.of(field_name, Callback, Args, Payload);
            const status = dispatch(
                &contract,
                @ptrCast(callback),
                @ptrCast(&typed_args),
                @ptrCast(&output),
            );
            if (!status.isOk()) return error_abi.errorFromStatus(status);
            return output;
        }

        fn CallReturn(comptime Callback: type) type {
            return anyerror!payloadType(Callback);
        }

        fn dispatchLocal(
            contract: *const native_abi.CallContract,
            callback: *const anyopaque,
            args: *const anyopaque,
            output: ?*anyopaque,
        ) callconv(.c) error_abi.Status {
            if (contract.version != native_abi.abi_version)
                return error_abi.statusFromError(error.UnsupportedVersion);
            inline for (std.meta.fields(VTable)) |field| {
                if (contract.method_id == native_abi.stableId(field.name)) {
                    if (comptime isCallbackField(field.type)) {
                        const Callback = callbackType(field.type);
                        const Function = functionType(Callback);
                        const Args = std.meta.ArgsTuple(Function);
                        const Payload = payloadType(Callback);
                        const expected = native_abi.CallContract.of(field.name, Callback, Args, Payload);
                        if (!contract.matches(expected))
                            return error_abi.statusFromError(error.InvalidArgument);
                        return invoke(field.type, callback, args, output);
                    }
                    return error_abi.statusFromError(error.InvalidArgument);
                }
            }
            return error_abi.statusFromError(error.InvalidArgument);
        }

        fn invoke(
            comptime Field: type,
            callback: *const anyopaque,
            args: *const anyopaque,
            output: ?*anyopaque,
        ) error_abi.Status {
            const Callback = callbackType(Field);
            const Function = functionType(Callback);
            const Args = std.meta.ArgsTuple(Function);
            const Return = @typeInfo(Function).@"fn".return_type orelse
                @compileError("boundary callback must have a return type");
            const Payload = payloadType(Callback);
            const typed_callback: Callback = @ptrCast(@alignCast(callback));
            const typed_args: *const Args = @ptrCast(@alignCast(args));

            if (Payload == void) {
                if (@typeInfo(Return) == .error_union) {
                    @call(.auto, typed_callback, typed_args.*) catch |err|
                        return error_abi.statusFromError(err);
                } else {
                    @call(.auto, typed_callback, typed_args.*);
                }
                return .ok;
            }

            const value = if (@typeInfo(Return) == .error_union)
                @call(.auto, typed_callback, typed_args.*) catch |err|
                    return error_abi.statusFromError(err)
            else
                @call(.auto, typed_callback, typed_args.*);
            const typed_output: *Payload = @ptrCast(@alignCast(output orelse
                return error_abi.statusFromError(error.InvalidArgument)));
            typed_output.* = value;
            return .ok;
        }

        fn callbackType(comptime Field: type) type {
            return switch (@typeInfo(Field)) {
                .optional => |optional| optional.child,
                else => Field,
            };
        }

        fn isCallbackField(comptime Field: type) bool {
            const Callback = switch (@typeInfo(Field)) {
                .optional => |optional| optional.child,
                else => Field,
            };
            return switch (@typeInfo(Callback)) {
                .pointer => |pointer| @typeInfo(pointer.child) == .@"fn",
                else => false,
            };
        }

        fn functionType(comptime Callback: type) type {
            return switch (@typeInfo(Callback)) {
                .pointer => |pointer| pointer.child,
                else => @compileError("boundary callback must be a function pointer"),
            };
        }

        fn payloadType(comptime Callback: type) type {
            const function = @typeInfo(functionType(Callback)).@"fn";
            const Return = function.return_type orelse
                @compileError("boundary callback must have a return type");
            return switch (@typeInfo(Return)) {
                .error_union => |error_union| error_union.payload,
                else => Return,
            };
        }
    };
}

test "boundary dispatcher preserves local calls and maps cross-unit calls" {
    const TestVTable = struct {
        value: *const fn (*u32, u32) anyerror!u32,
        fail: ?*const fn (*u32) anyerror!void = null,
        retryable_fail: ?*const fn (*u32) anyerror!void = null,
        durability_pending: ?*const fn (*u32) anyerror!void = null,
    };
    const TestBoundary = BoundaryImpl(TestVTable);
    const callbacks = struct {
        fn value(ptr: *u32, addend: u32) anyerror!u32 {
            return ptr.* + addend;
        }
        fn fail(_: *u32) anyerror!void {
            return error.Conflict;
        }

        fn privateFail(_: *u32) anyerror!void {
            return error.UnitPrivateError;
        }

        fn retryableFail(_: *u32) anyerror!void {
            return error.ProposalDropped;
        }

        fn durabilityPending(_: *u32) anyerror!void {
            return error.HASyncCommitWouldBlock;
        }

        fn foreignDispatch(
            contract: *const native_abi.CallContract,
            callback: *const anyopaque,
            args: *const anyopaque,
            output: ?*anyopaque,
        ) callconv(.c) error_abi.Status {
            return TestBoundary.local_dispatch(contract, callback, args, output);
        }

        fn rejectingDispatch(
            contract: *const native_abi.CallContract,
            callback: *const anyopaque,
            args: *const anyopaque,
            output: ?*anyopaque,
        ) callconv(.c) error_abi.Status {
            var incompatible = contract.*;
            incompatible.arguments.size += 1;
            return TestBoundary.local_dispatch(&incompatible, callback, args, output);
        }
    };

    var base: u32 = 40;
    try std.testing.expectEqual(
        @as(u32, 42),
        try TestBoundary.call("value", TestBoundary.local_dispatch, &callbacks.value, .{ &base, 2 }),
    );
    try std.testing.expectError(
        error.Conflict,
        TestBoundary.call("fail", TestBoundary.local_dispatch, &callbacks.fail, .{&base}),
    );
    try std.testing.expectError(
        error.UnitPrivateError,
        TestBoundary.call("fail", TestBoundary.local_dispatch, &callbacks.privateFail, .{&base}),
    );
    try std.testing.expectError(
        error.RuntimeBoundaryFailure,
        TestBoundary.call("fail", &callbacks.foreignDispatch, &callbacks.privateFail, .{&base}),
    );
    try std.testing.expectError(
        error.ProposalDropped,
        TestBoundary.call("retryable_fail", &callbacks.foreignDispatch, &callbacks.retryableFail, .{&base}),
    );
    try std.testing.expectError(
        error.HASyncCommitWouldBlock,
        TestBoundary.call("durability_pending", &callbacks.foreignDispatch, &callbacks.durabilityPending, .{&base}),
    );
    try std.testing.expectError(
        error.InvalidArgument,
        TestBoundary.call("value", &callbacks.rejectingDispatch, &callbacks.value, .{ &base, 2 }),
    );
}
