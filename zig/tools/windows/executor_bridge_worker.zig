// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const abi = @import("antfly_executor_abi");

/// Compile this in a separate archive from executor_bridge_host.zig.
export fn exerciseBorrowedExecutor(borrow: *const abi.Borrow) callconv(.c) bool {
    var receiver = borrow.receive() catch return false;
    const io = receiver.io();
    const Job = struct {
        fn run(count: *std.atomic.Value(usize)) void {
            _ = count.fetchAdd(1, .release);
        }
    };
    var count = std.atomic.Value(usize).init(0);
    for (0..64) |_| {
        // Let the owning archive's workers park before every dispatch. Busy
        // background tasks can conceal a foreign vtable's missed wakeups.
        io.sleep(.fromMilliseconds(100), .awake) catch return false;
        var future = io.concurrent(Job.run, .{&count}) catch return false;
        const previous = io.swapCancelProtection(.blocked);
        future.await(io);
        _ = io.swapCancelProtection(previous);
    }
    return count.load(.acquire) == 64;
}
