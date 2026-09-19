// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const callback = @import("runtime_callback_abi.zig");
const dense = @import("storage/dense_execution.zig");
const VTable = struct {
    query: *const fn (u8) anyerror!void,
    routed_write: *const fn (u8) anyerror!?void,
};

export fn runtime_callback_test_dispatch() callback.CallbackDispatch {
    return callback.Boundary(VTable).local_dispatch;
}

export fn runtime_callback_test_query() *const anyopaque {
    return @ptrCast(&query);
}

export fn runtime_callback_test_routed_write() *const anyopaque {
    return @ptrCast(&routedWrite);
}

fn routedWrite(mode: u8) anyerror!?void {
    return switch (mode) {
        0 => {},
        1 => null,
        2 => error.MetadataSnapshotUnavailable,
        3 => error.GroupLeaderUnavailable,
        4 => error.LeaderUnavailable,
        5 => error.RaftBatchWriteOutcomeUnknown,
        6 => error.DeadlineExceeded,
        7 => error.Canceled,
        else => error.InvalidArgument,
    };
}

fn query(mode: u8) anyerror!void {
    // Exercise real dense ownership before crossing the foreign trampoline.
    // The remaining admission variants share this transport but originate in
    // other resource gates, so inject those semantic outcomes explicitly.
    switch (mode) {
        1 => return error.AdmissionQueueFull,
        2 => return error.AdmissionBytesExhausted,
        3 => return error.AdmissionRequestTooLarge,
        7 => return error.Canceled,
        else => {},
    }
    var threaded = std.Io.Threaded.init(std.heap.c_allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const waiting = mode == 4;
    const runtime = try dense.Runtime.create(std.heap.c_allocator, .{
        .max_runnable_tasks = 1,
        .max_outstanding_tasks = if (waiting) 2 else 1,
        .max_queued_tasks = if (waiting) 1 else 0,
        .max_wait_ms = if (waiting) 1 else 0,
    });
    defer runtime.destroy();
    if (mode == 5) runtime.scheduler.close();
    if (mode == 6) {
        var expired = try runtime.acquire(.{ .io = io, .deadline_ns = 0 });
        defer expired.release();
        return error.TestUnexpectedResult;
    }
    var lease = try runtime.acquire(.{ .io = io });
    defer lease.release();
    var excess = try runtime.acquire(.{ .io = io });
    defer excess.release();
    return error.TestUnexpectedResult;
}
