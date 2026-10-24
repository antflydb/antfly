// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Independent compilation owner for checked execution-wait callback tests.
const runtime = @import("antfly_runtime_abi");
const Boundary = @import("common/execution_wait.zig").Boundary;

var mode: u32 = 0;

fn begin(_: *anyopaque) !void {
    if (mode == 0) return error.SchedulerClosed;
}

fn end(_: *anyopaque) !void {
    if (mode == 1) return error.Forbidden;
}

export fn antfly_execution_wait_fixture_open(contract: *const runtime.native_abi.TypeContract, output: *Boundary, requested_mode: u32) callconv(.c) runtime.error_abi.Status {
    if (!contract.matches(runtime.native_abi.TypeContract.of(Boundary))) return runtime.error_abi.statusFromError(error.InvalidArgument);
    if (requested_mode > 2) return runtime.error_abi.statusFromError(error.InvalidArgument);
    mode = requested_mode;
    // The default dispatcher is owned by THIS compilation unit. Consumers
    // must never invoke the compilation-local Zig error union directly.
    output.* = .{ .ptr = &mode, .begin = begin, .end = end };
    return .ok;
}
