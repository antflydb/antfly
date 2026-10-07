// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

comptime {
    _ = @import("common/execution_wait.zig");
}

const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const Boundary = @import("common/execution_wait.zig").Boundary;

extern fn antfly_execution_wait_fixture_open(*const runtime.native_abi.TypeContract, *Boundary, u32) callconv(.c) runtime.error_abi.Status;

test "execution wait independently compiled callbacks preserve error and cleanup ownership" {
    var boundary: Boundary = undefined;
    var contract = runtime.native_abi.TypeContract.of(Boundary);
    try std.testing.expect(antfly_execution_wait_fixture_open(&contract, &boundary, 0).isOk());
    try std.testing.expect(boundary.boundary_dispatch != Boundary.local_dispatch);
    try std.testing.expectError(error.SchedulerClosed, boundary.beginWait());
    try std.testing.expect(antfly_execution_wait_fixture_open(&contract, &boundary, 1).isOk());
    try boundary.beginWait();
    try std.testing.expectError(error.Forbidden, boundary.endWait());
    const Caller = struct {
        live: bool = false,
        fn open(self: *@This()) !*u8 {
            const value = try std.testing.allocator.create(u8);
            self.live = true;
            return value;
        }
        fn discard(value: *u8, self: *@This()) void {
            std.debug.assert(self.live);
            std.testing.allocator.destroy(value);
            self.live = false;
        }
    };
    var caller: Caller = .{};
    try std.testing.expectError(error.Forbidden, boundary.runOwned(Caller.open, .{&caller}, Caller.discard, .{&caller}));
    try std.testing.expect(!caller.live);
    try std.testing.expect(antfly_execution_wait_fixture_open(&contract, &boundary, 2).isOk());
    const value = try boundary.runOwned(Caller.open, .{&caller}, Caller.discard, .{&caller});
    Caller.discard(value, &caller);
    try std.testing.expect(!caller.live);
    // Reject a mismatched owner layout before publishing a callback object.
    contract.size += 1;
    try std.testing.expectEqual(error.InvalidArgument, runtime.error_abi.errorFromStatus(antfly_execution_wait_fixture_open(&contract, &boundary, 2)));
}
