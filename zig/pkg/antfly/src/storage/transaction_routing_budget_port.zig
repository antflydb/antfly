// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Synchronous, checked callback for the original transaction routing budget.
//! The caller owns the payload and original clock/cancellation until return.
const std = @import("std");
const native = @import("antfly_runtime_abi").native_abi;
const admission = @import("antfly_local_sources").read_admission;
pub const VTable = admission.VTable;
pub const Boundary = admission.Boundary;
pub const Payload = admission.Payload;
pub const Port = admission.Port;

test "transaction routing control port rejects incompatible payloads before callbacks" {
    const State = struct {
        calls: usize = 0,
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
    };
    var state = State{};
    const payload = Payload{ .ptr = &state, .vtable = &.{ .checkpoint = State.checkpoint } };
    var port = Port.init(&payload);
    port.contract.layout_id +%= 1;
    try std.testing.expectError(error.UnsupportedVersion, port.checkpoint());
    try std.testing.expectEqual(@as(usize, 0), state.calls);
    port = Port.init(&payload);
    try port.checkpoint();
    try std.testing.expectEqual(@as(usize, 1), state.calls);
}

test "transaction routing control port preserves scoped admission errors through checked dispatch" {
    const State = struct {
        calls: usize = 0,
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.GenerationPublicationChanged;
        }
        fn foreign(contract: *const native.CallContract, function: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            return Boundary.local_dispatch(contract, function, args, output);
        }
    };
    var state = State{};
    const payload = Payload{ .ptr = &state, .vtable = &.{ .checkpoint = State.checkpoint }, .dispatch = State.foreign };
    const port = Port.init(&payload);
    try std.testing.expectError(error.GenerationPublicationChanged, port.checkpoint());
    try std.testing.expectEqual(@as(usize, 1), state.calls);
}

test "transaction routing control port wait requires a compatible vtable and a progress capability" {
    const State = struct {
        calls: usize = 0,
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
    };
    var state = State{};
    var payload = Payload{ .ptr = &state, .vtable = &.{ .checkpoint = State.checkpoint } };
    const port = Port.init(&payload);
    payload.vtable_contract.layout_id +%= 1;
    try std.testing.expectError(error.UnsupportedVersion, port.checkpoint());
    try std.testing.expectEqual(@as(usize, 0), state.calls);
    payload.vtable_contract = .of(VTable);
    try std.testing.expectError(error.ExecutionWaitUnsupported, port.waitForProgress());
    try std.testing.expectEqual(@as(usize, 1), state.calls);
}

test "transaction routing control port wait preserves exact failure through foreign progress dispatch" {
    const State = struct {
        waits: usize = 0,
        fn checkpoint(_: *anyopaque) !void {}
        fn wait(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.waits += 1;
            return error.GenerationPublicationChanged;
        }
        fn foreign(contract: *const native.CallContract, function: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            return Boundary.local_dispatch(contract, function, args, output);
        }
    };
    var state = State{};
    const payload = Payload{ .ptr = &state, .vtable = &.{ .checkpoint = State.checkpoint, .wait_for_progress = State.wait }, .dispatch = State.foreign };
    const port = Port.init(&payload);
    try std.testing.expectError(error.GenerationPublicationChanged, port.waitForProgress());
    try std.testing.expectEqual(@as(usize, 1), state.waits);
}
