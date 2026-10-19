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

const std = @import("std");
const rpc = @import("antfly_inference_worker_rpc");
const lifetime = @import("worker_lifetime.zig");
extern "c" fn atexit(handler: *const fn () callconv(.c) void) c_int;

fn wedgedTeardown() callconv(.c) void {
    const marker = "atexit-entered\n";
    _ = std.c.write(2, marker, marker.len);
    while (true) std.atomic.spinLoopHint();
}
fn reject(_: *anyopaque, _: *rpc.Request) !rpc.OwnedPayload {
    return error.UnsupportedOperation;
}
fn safetyLimit() void {
    // Bound the negative control even if the Python harness is interrupted.
    var delay = std.posix.timespec{ .sec = 6, .nsec = 0 };
    while (std.posix.errno(std.posix.system.nanosleep(&delay, &delay)) == .INTR) {}
    @import("antfly_platform").process.exitImmediately(255);
}
pub fn main(init: std.process.Init) !void {
    (try std.Thread.spawn(.{}, safetyLimit, .{})).detach();
    if (atexit(wedgedTeardown) != 0) return error.AtExitRegistrationFailed;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const mode = args.next() orelse return error.MissingMode;
    if (std.mem.eql(u8, mode, "libc-exit")) std.process.exit(0);
    if (std.mem.eql(u8, mode, "resource-failure")) lifetime.resourceOwnerLost();
    var context: u8 = 0;
    var endpoint = rpc.Endpoint{
        .alloc = init.gpa,
        .io = init.io,
        .input = .stdin(),
        .output = .stdout(),
        .context = &context,
        .handler = reject,
        .on_closed = lifetime.ownerLost,
        .next_id = 2,
    };
    try endpoint.start();
    var heap: @import("worker_heap.zig").Heap = undefined;
    if (std.mem.eql(u8, mode, "heap-invalid-reply")) {
        try heap.init(init.gpa, &endpoint);
        _ = try heap.allocator().alloc(u8, 1024);
        return error.InvalidHeapReplyWasAccepted;
    }
    if (std.mem.eql(u8, mode, "prompt-observation-denied")) {
        const bridge = @import("antfly_inference_bridge");
        const wire = @import("antfly_inference_worker_wire");
        const denial = bridge.statusFromError(error.ResourceTemporarilyUnavailable);
        var marker: [64]u8 = undefined;
        try std.Io.File.stderr().writeStreamingAll(init.io, try std.fmt.bufPrint(&marker, "denial {d} {d}\n", .{ denial.code, denial.detail }));
        var bytes: [rpc.max_resource_bytes]u8 = undefined;
        var writer = std.Io.Writer.fixed(&bytes);
        try std.json.Stringify.value(wire.ResourceRequest{ .operation = .prompt_cache, .data = "{\"key\":1,\"previous\":0,\"next\":64}" }, .{}, &writer);
        const reply = try endpoint.callResource(writer.buffered());
        if (reply.code != denial.code or reply.detail != denial.detail or reply.value != 0) return error.UnexpectedFixtureReply;
        _ = lifetime.cacheObservationReply(true, 0, 64, false);
        return error.DeniedObservationWasIgnored;
    }
    try std.Io.File.stderr().writeStreamingAll(init.io, "worker-ready\n");
    // Both the process entry thread and libc teardown are deliberately unable
    // to complete; only the independent RPC reader's production callback exits.
    var blocked: std.Io.Event = .unset;
    try blocked.wait(init.io);
}
