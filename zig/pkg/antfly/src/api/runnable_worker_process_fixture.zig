// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Constrained subprocess fixture: actual production RPC, deterministic output.
const std = @import("std");
const rpc = @import("inference_worker_rpc");
fn handle(_: *anyopaque, request: *rpc.Request) !rpc.OwnedPayload {
    const payload = try request.endpoint.alloc.alloc(u8, 1024 * 1024);
    defer request.endpoint.alloc.free(payload);
    @memset(payload, 'x');
    for (0..64) |_| try request.eventAcknowledged(.{ .metadata = "chunk", .body = payload });
    // This branch must never execute after a denied output acknowledgment.
    return request.endpoint.copyPayload(.{ .body = "computed" });
}
fn closed(_: *anyopaque) void {
    std.process.exit(0);
}
pub fn main(init: std.process.Init) !void {
    var context: u8 = 0;
    var endpoint = rpc.Endpoint{ .alloc = init.gpa, .io = init.io, .input = .stdin(), .output = .stdout(), .context = &context, .handler = handle, .on_closed = closed, .next_id = 2 };
    try endpoint.start();
    defer endpoint.deinit();
    var lifetime: std.Io.Event = .unset;
    try lifetime.wait(init.io);
}
