// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: MIT
const std = @import("std");
const connection = @import("protocol/h2_connection.zig");

test "H2 flow control contracts" {
    std.testing.refAllDecls(connection);
}

test "H2 flow disconnect wakes credit waiters even with no DATA event" {
    var h2 = connection.H2Connection.initServer(std.testing.allocator, std.testing.io);
    defer h2.deinit();
    const stream = try h2.stream_manager.getOrCreateStream(1);
    stream.state = .half_closed_remote;
    stream.completed = true;
    stream.end_stream_received = true;
    stream.send_window = 0;
    const generation = h2.send_window_generation.load(.acquire);
    h2.signalAllStreams(error.ConnectionClosed);
    try std.testing.expect(h2.send_window_generation.load(.acquire) != generation);
    try std.testing.expectEqual(error.ConnectionClosed, stream.stream_error.?);
    try std.testing.expectEqual(error.ConnectionClosed, stream.responseWriteError(true).?);
    // Notification before the physical wait cannot be lost.
    try std.testing.io.futexWaitTimeout(u32, &h2.send_window_generation.raw, generation, .{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } });
}
