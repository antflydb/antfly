// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

/// Await mutation work without forwarding caller I/O cancellation to it.
/// The work still observes its borrowed request token at its own safe points.
pub fn wait(io: std.Io, future: *std.Io.Future(void)) void {
    const previous = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(previous);
    future.await(io);
}

test "caller cancellation does not interrupt protected child work" {
    var pool = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(8) });
    defer pool.deinit();
    const io = pool.io();
    const State = struct {
        started: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        canceled: bool = false,
        finished: bool = false,

        fn child(i: std.Io, self: *@This()) void {
            self.started.set(i);
            self.release.wait(i) catch {
                self.canceled = true;
                return;
            };
            self.finished = true;
        }
        fn parent(i: std.Io, self: *@This()) void {
            var future = i.concurrent(child, .{ i, self }) catch @panic("test executor unavailable");
            wait(i, &future);
        }
        fn releaseChild(i: std.Io, self: *@This()) void {
            i.sleep(.fromMilliseconds(50), .awake) catch {};
            self.release.set(i);
        }
    };
    var state = State{};
    var parent = try io.concurrent(State.parent, .{ io, &state });
    try state.started.wait(io);
    var release = try io.concurrent(State.releaseChild, .{ io, &state });
    parent.cancel(io);
    release.await(io);
    try std.testing.expect(!state.canceled);
    try std.testing.expect(state.finished);
}
