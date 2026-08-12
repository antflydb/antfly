// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Transport adapters for the layout-only runtime HTTP ABI. This module is
//! compiled independently on each side of a linked archive boundary; only the
//! extern values and callbacks in runtime_http_abi cross that boundary.

const httpx = @import("httpx");
const abi = @import("runtime_http_abi.zig");

pub const Outbound = struct {
    context: *httpx.Context,
    writer: ?httpx.Context.StreamWriter = null,
    started: bool = false,

    pub fn cancellation(self: *Outbound) abi.CancellationView {
        return .{ .context = self.context, .is_cancelled = isCancelled };
    }

    pub fn stream(self: *Outbound) abi.StreamSink {
        return .{
            .context = self,
            .start = start,
            .write = write,
            .close = close,
        };
    }

    fn isCancelled(raw: ?*const anyopaque) callconv(.c) u8 {
        const context: *const httpx.Context = @ptrCast(@alignCast(raw orelse return 0));
        return @intFromBool(context.isCancellationRequested());
    }

    fn start(raw: ?*anyopaque, status: u16) callconv(.c) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (self.started) return .failed;
        self.writer = self.context.streamResponse(status) catch return .failed;
        self.started = true;
        return .ok;
    }

    fn write(raw: ?*anyopaque, bytes: abi.Bytes) callconv(.c) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (self.context.isCancellationRequested()) return .canceled;
        const writer = &(self.writer orelse return .failed);
        writer.write(bytes.slice()) catch return .failed;
        return .ok;
    }

    fn close(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        const writer = &(self.writer orelse return .failed);
        writer.close() catch return .failed;
        return .ok;
    }
};

pub fn installInbound(
    context: *httpx.Context,
    cancellation: *const abi.CancellationView,
    stream: *const abi.StreamSink,
) void {
    if (cancellation.is_cancelled != null) {
        context.cancellation_probe = .{
            .ptr = cancellation,
            .is_cancelled = inboundIsCancelled,
        };
    }
    if (stream.start != null and stream.write != null and stream.close != null) {
        context.stream_delegate = .{
            .ptr = @constCast(stream),
            .start = inboundStart,
            .write = inboundWrite,
            .close = inboundClose,
        };
    }
}

fn inboundIsCancelled(raw: ?*const anyopaque) bool {
    const cancellation: *const abi.CancellationView = @ptrCast(@alignCast(raw orelse return false));
    return cancellation.requested();
}

fn inboundStart(raw: ?*anyopaque, status: u16) !void {
    const stream: *const abi.StreamSink = @ptrCast(@alignCast(raw orelse return error.StreamUnavailable));
    try callbackResult(stream.start.?(stream.context, status));
}

fn inboundWrite(raw: ?*anyopaque, bytes: []const u8) !void {
    const stream: *const abi.StreamSink = @ptrCast(@alignCast(raw orelse return error.StreamUnavailable));
    try callbackResult(stream.write.?(stream.context, abi.Bytes.init(bytes)));
}

fn inboundClose(raw: ?*anyopaque) !void {
    const stream: *const abi.StreamSink = @ptrCast(@alignCast(raw orelse return error.StreamUnavailable));
    try callbackResult(stream.close.?(stream.context));
}

fn callbackResult(status: abi.CallbackStatus) !void {
    return switch (status) {
        .ok => {},
        .failed => error.StreamWriteFailed,
        .canceled => error.Canceled,
    };
}

test "missing linked callbacks leave a normal buffered context" {
    const std = @import("std");
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    var request = try httpx.Request.init(std.testing.allocator, .GET, "/");
    defer request.deinit();
    var context = httpx.Context.init(std.testing.allocator, io_impl.io(), &request);
    defer context.deinit();
    const cancellation: abi.CancellationView = .{};
    const stream: abi.StreamSink = .{};
    installInbound(&context, &cancellation, &stream);
    try std.testing.expect(context.cancellation_probe == null);
    try std.testing.expect(context.stream_delegate == null);
}

test "linked callbacks preserve streaming and cancellation semantics" {
    const std = @import("std");
    const State = struct {
        canceled: bool = false,
        status: u16 = 0,
        bytes: [16]u8 = undefined,
        bytes_len: usize = 0,
        closed: bool = false,

        fn isCancelled(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw orelse return 0));
            return @intFromBool(self.canceled);
        }

        fn start(raw: ?*anyopaque, status: u16) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return .failed));
            self.status = status;
            return .ok;
        }

        fn write(raw: ?*anyopaque, bytes: abi.Bytes) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return .failed));
            const input = bytes.slice();
            if (input.len > self.bytes.len - self.bytes_len) return .failed;
            @memcpy(self.bytes[self.bytes_len..][0..input.len], input);
            self.bytes_len += input.len;
            return .ok;
        }

        fn close(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return .failed));
            self.closed = true;
            return .ok;
        }
    };

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    var request = try httpx.Request.init(std.testing.allocator, .POST, "/stream");
    defer request.deinit();
    var context = httpx.Context.init(std.testing.allocator, io_impl.io(), &request);
    defer context.deinit();
    var state = State{};
    const cancellation: abi.CancellationView = .{
        .context = &state,
        .is_cancelled = State.isCancelled,
    };
    const stream: abi.StreamSink = .{
        .context = &state,
        .start = State.start,
        .write = State.write,
        .close = State.close,
    };
    installInbound(&context, &cancellation, &stream);

    try std.testing.expect(!context.isCancellationRequested());
    state.canceled = true;
    try std.testing.expect(context.isCancellationRequested());
    state.canceled = false;

    var writer = try context.streamResponse(202);
    try writer.write("linked");
    try writer.close();
    try std.testing.expectEqual(@as(u16, 202), state.status);
    try std.testing.expectEqualStrings("linked", state.bytes[0..state.bytes_len]);
    try std.testing.expect(state.closed);
}
