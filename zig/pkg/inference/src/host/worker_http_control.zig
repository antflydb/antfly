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

//! Semantic controls for one synchronous HTTP callback across a worker event.
//! Only the scalar deadline crosses a pipe; callback pointers stay in their
//! originating process until the event is acknowledged or the worker is fenced.
const std = @import("std");
const http = @import("antfly_runtime_abi").http_abi;
const rpc = @import("antfly_inference_worker_rpc");
const platform = @import("antfly_platform");

pub fn result(value: http.CallbackStatus) !void {
    return switch (value) {
        .ok => {},
        .canceled => error.Cancelled,
        .timeout => error.Timeout,
        else => error.InvalidWorkerHttpControl,
    };
}
pub fn status(err: anyerror) http.CallbackStatus {
    return switch (err) {
        error.Canceled, error.Cancelled => .canceled,
        error.Timeout, error.DeadlineExceeded => .timeout,
        else => .failed,
    };
}

pub const Child = struct {
    incoming: *const http.RequestControl,
    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try result((self.incoming.check orelse return error.InvalidWorkerHttpControl)(self.incoming.context));
    }
    pub fn view(self: *@This()) rpc.EventControl {
        return .{ .ptr = self, .check_fn = check };
    }
    pub fn deadline(self: *@This()) !?u64 {
        // Sample the destination clock before invoking the foreign clock.
        const target_now = platform.time.monotonicNs();
        try check(self);
        var timeout: http.OptionalTimeout = .{};
        try result((self.incoming.remaining orelse return error.InvalidWorkerHttpControl)(self.incoming.context, &timeout));
        if (timeout.present > 1) return error.InvalidWorkerHttpControl;
        if (timeout.present == 0) return null;
        if (timeout.milliseconds == 0) return error.Timeout;
        // remaining_ms rounds up. Tighten by at most one millisecond rather
        // than minting extra time in the destination clock domain.
        return target_now +| ((timeout.milliseconds - 1) *| std.time.ns_per_ms);
    }
};

pub const Parent = struct {
    original_ptr: ?*anyopaque,
    original_check: *const fn (?*anyopaque) anyerror!void,
    original_deadline_ns: ?u64,
    event_deadline_ns: ?u64,
    event: rpc.EventControl,

    pub fn poll(self: *const @This()) !void {
        try self.original_check(self.original_ptr);
        try self.event.check();
        const now = platform.time.monotonicNs();
        if (self.original_deadline_ns) |deadline| if (now >= deadline) return error.Timeout;
        if (self.event_deadline_ns) |deadline| if (now >= deadline) return error.Timeout;
    }
    fn check(raw: ?*anyopaque) callconv(.c) http.CallbackStatus {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.poll() catch |err| return status(err);
        return .ok;
    }
    fn remaining(raw: ?*anyopaque, out: *http.OptionalTimeout) callconv(.c) http.CallbackStatus {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.poll() catch |err| return status(err);
        const deadline: ?u64 = if (self.original_deadline_ns) |original| @min(original, self.event_deadline_ns orelse original) else self.event_deadline_ns;
        if (deadline) |fence| {
            const now = platform.time.monotonicNs();
            if (now >= fence) return .timeout;
            out.* = .{ .present = 1, .milliseconds = (fence - now - 1) / std.time.ns_per_ms + 1 };
        } else out.* = .{};
        return .ok;
    }
    pub fn active(self: @This()) bool {
        return self.event.check_fn != null or self.original_deadline_ns != null or self.event_deadline_ns != null;
    }
    pub fn view(self: *@This()) http.RequestControl {
        return .{ .context = self, .check = check, .remaining = remaining };
    }
};

test "inference worker HTTP control scalar projection never extends original native time" {
    const Check = struct {
        fn check(_: ?*anyopaque) !void {}
    };
    const original = platform.time.monotonicNs() + 200 * std.time.ns_per_ms;
    var parent: Parent = .{ .original_ptr = null, .original_check = Check.check, .original_deadline_ns = original, .event_deadline_ns = null, .event = .{} };
    const view_control = parent.view();
    var child: Child = .{ .incoming = &view_control };
    const projected = (try child.deadline()).?;
    try std.testing.expect(projected <= original);
    try std.testing.expect(projected > platform.time.monotonicNs());
    parent.original_deadline_ns = null;
    try std.testing.expect((try child.deadline()) == null);
    parent.event_deadline_ns = 0;
    try std.testing.expectError(error.Timeout, child.deadline());
    parent.event_deadline_ns = null;
    parent.original_deadline_ns = 0;
    try std.testing.expectError(error.Timeout, child.deadline());
}

test "inference worker HTTP control cancellation crosses pipes and stops a physical H2 credit wait before acknowledgment" {
    const httpx = @import("httpx");
    const transport = @import("antfly_runtime_abi").http_bridge;
    const io = std.testing.io;
    var pair: rpc.TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    var listener = try httpx.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
    defer listener.deinit();
    var socket = try httpx.Socket.connect(listener.getLocalAddress(), io);
    defer socket.close();
    socket.enableNativeTimeouts();
    var peer = try listener.accept();
    defer peer.socket.close();
    var h2 = httpx.H2Connection.initServer(std.testing.allocator, io);
    defer h2.deinit();
    const stream = try h2.stream_manager.getOrCreateStream(1);
    stream.state = .open;
    stream.send_window = 0;
    var request = try httpx.Request.init(std.testing.allocator, .GET, "/stream");
    defer request.deinit();
    var context = httpx.Context.init(std.testing.allocator, io, &request);
    defer context.deinit();
    context.h2 = &h2;
    context.h2_sock = &socket;
    context.h2_stream_id = 1;
    context.application_deadline_ns = @intCast(std.Io.Clock.awake.now(io).nanoseconds + 2000 * std.time.ns_per_ms);
    context.application_deadline_io = io;
    var outbound: transport.Outbound = .{ .context = &context };
    const sink = outbound.stream();
    try std.testing.expectEqual(.ok, sink.start.?(sink.context, 200, .init("text/plain"), .{}));
    const Owner = struct {
        pair: *rpc.TestPair,
        sink: http.StreamSink,
        entered: bool = false,
        fn check(_: ?*anyopaque) !void {}
        fn event(raw: ?*anyopaque, payload: rpc.Payload, event_control: rpc.EventControl) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.entered = true;
            var control: Parent = .{ .original_ptr = null, .original_check = check, .original_deadline_ns = null, .event_deadline_ns = null, .event = event_control };
            const view_control = control.view();
            self.pair.cancelled.store(true, .release);
            const outcome = self.sink.write_controlled.?(self.sink.context, .init(payload.body), &view_control);
            try std.testing.expectEqual(http.CallbackStatus.canceled, outcome);
            try std.testing.expect(!self.pair.phase_denied.isSet());
            return result(outcome);
        }
    };
    var owner = Owner{ .pair = &pair, .sink = sink };
    const started = platform.time.monotonicNs();
    try std.testing.expectError(error.Cancelled, pair.parent.call(.{ .body = "controlled-cancel" }, .{ .ptr = &owner, .event_controlled = Owner.event }));
    try std.testing.expect(platform.time.monotonicNs() - started < std.time.ns_per_s);
    try std.testing.expect(owner.entered);
    try pair.phase_denied.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
    try std.testing.expect(!pair.phase_resumed.load(.acquire));
    try std.testing.expect(outbound.writer.?.control.ptr == null);
    try std.testing.expect(outbound.writer.?.h2_writer.?.control.ptr == null);
    try std.testing.expect(socket.request_deadline_ms == null);
    const healthy = try pair.parent.call(.{ .body = "echo" }, .{});
    defer healthy.deinit();
    try std.testing.expectEqualStrings("echo", healthy.view().body);
}
