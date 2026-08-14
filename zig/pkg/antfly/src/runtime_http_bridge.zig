// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Transport adapters for the layout-only runtime HTTP ABI. This module is
//! compiled independently on each side of a linked archive boundary; only the
//! extern values and callbacks in runtime_http_abi cross that boundary.

const httpx = @import("httpx");
const abi = @import("runtime_http_abi.zig");

fn callbackStatusFromError(err: anyerror) abi.CallbackStatus {
    return switch (err) {
        error.Canceled, error.Cancelled => .canceled,
        error.Timeout => .timeout,
        error.BodyTooLarge => .body_too_large,
        error.BodyCapacityExceeded => .body_capacity_exceeded,
        error.EndOfStream => .end_of_stream,
        else => .failed,
    };
}

fn callbackStatusAfterIo(context: *const httpx.Context, err: anyerror) abi.CallbackStatus {
    // Cancellation can arrive while a body read or response write is blocked.
    // Give the transport-owned lifetime signal precedence over the concrete
    // socket/stream error observed when that operation wakes.
    if (context.isCancellationRequested()) return .canceled;
    return callbackStatusFromError(err);
}

pub const Outbound = struct {
    context: *httpx.Context,
    writer: ?httpx.Context.StreamWriter = null,
    started: bool = false,

    pub fn cancellation(self: *Outbound) abi.CancellationView {
        return .{
            .context = self.context,
            .is_cancelled = isCancelled,
        };
    }

    pub fn bodySource(self: *Outbound) abi.RequestBodySource {
        return .{
            .context = self,
            .read_all = readAllBody,
            .streaming = @intFromBool(self.context.hasStreamingRequestBody()),
        };
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

    fn readAllBody(raw: ?*anyopaque, out: *abi.OptionalBytes) callconv(.c) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (self.context.isCancellationRequested()) return .canceled;
        const body = self.context.body() catch |err| return callbackStatusAfterIo(self.context, err);
        out.* = abi.OptionalBytes.init(body);
        return .ok;
    }

    fn start(raw: ?*anyopaque, status: u16) callconv(.c) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (self.context.isCancellationRequested()) return .canceled;
        if (self.started) return .failed;
        self.writer = self.context.streamResponse(status) catch |err| return callbackStatusAfterIo(self.context, err);
        self.started = true;
        return .ok;
    }

    fn write(raw: ?*anyopaque, bytes: abi.Bytes) callconv(.c) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (self.context.isCancellationRequested()) return .canceled;
        const writer = &(self.writer orelse return .failed);
        writer.write(bytes.slice()) catch |err| return callbackStatusAfterIo(self.context, err);
        return .ok;
    }

    fn close(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (self.context.isCancellationRequested()) return .canceled;
        const writer = &(self.writer orelse return .failed);
        writer.close() catch |err| return callbackStatusAfterIo(self.context, err);
        return .ok;
    }
};

pub fn installInbound(
    context: *httpx.Context,
    cancellation: *const abi.CancellationView,
    body_source: *const abi.RequestBodySource,
    stream: *const abi.StreamSink,
) void {
    context.cancellation = null;
    if (cancellation.is_cancelled != null) {
        context.cancellation_probe = .{
            .ptr = cancellation,
            .is_cancelled = inboundIsCancelled,
        };
    }
    if (body_source.read_all != null) {
        context.body_delegate = .{
            .ptr = @constCast(body_source),
            .read_all = inboundReadAllBody,
            .streaming = body_source.streaming != 0,
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

fn inboundReadAllBody(raw: ?*anyopaque) !?[]const u8 {
    const body_source: *const abi.RequestBodySource = @ptrCast(@alignCast(raw orelse return error.BodySourceUnavailable));
    var bytes: abi.OptionalBytes = .{};
    switch (body_source.read_all.?(body_source.context, &bytes)) {
        .ok => {},
        .failed => return error.BodyReadFailed,
        .canceled => return error.Canceled,
        .timeout => return error.Timeout,
        .body_too_large => return error.BodyTooLarge,
        .body_capacity_exceeded => return error.BodyCapacityExceeded,
        .end_of_stream => return error.EndOfStream,
    }
    return bytes.slice();
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
        .timeout => error.Timeout,
        .body_too_large => error.BodyTooLarge,
        .body_capacity_exceeded => error.BodyCapacityExceeded,
        .end_of_stream => error.EndOfStream,
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
    const body_source: abi.RequestBodySource = .{};
    const stream: abi.StreamSink = .{};
    installInbound(&context, &cancellation, &body_source, &stream);
    try std.testing.expect(context.cancellation_probe == null);
    try std.testing.expect(context.stream_delegate == null);
}

test "linked request bodies remain lazy and transport neutral" {
    const std = @import("std");
    const State = struct {
        calls: usize = 0,

        fn readAll(raw: ?*anyopaque, out: *abi.OptionalBytes) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return .failed));
            self.calls += 1;
            out.* = abi.OptionalBytes.init("linked body");
            return .ok;
        }
    };

    var request = try httpx.Request.init(std.testing.allocator, .POST, "/query");
    defer request.deinit();
    var context = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer context.deinit();
    var state = State{};
    const cancellation: abi.CancellationView = .{};
    const body_source: abi.RequestBodySource = .{
        .context = &state,
        .read_all = State.readAll,
        .streaming = 1,
    };
    const stream: abi.StreamSink = .{};
    installInbound(&context, &cancellation, &body_source, &stream);

    try std.testing.expect(context.hasStreamingRequestBody());
    try std.testing.expectEqual(@as(usize, 0), state.calls);
    try std.testing.expectEqualStrings("linked body", (try context.body()).?);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expect(!context.hasStreamingRequestBody());
    try std.testing.expectEqualStrings("linked body", (try context.body()).?);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
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
    const body_source: abi.RequestBodySource = .{};
    installInbound(&context, &cancellation, &body_source, &stream);

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

test "outbound stream callbacks preserve terminal status classes" {
    const std = @import("std");
    const Delegate = struct {
        fn startCanceled(_: ?*anyopaque, _: u16) anyerror!void {
            return error.Canceled;
        }

        fn startOk(_: ?*anyopaque, _: u16) anyerror!void {}

        fn writeTimeout(_: ?*anyopaque, _: []const u8) anyerror!void {
            return error.Timeout;
        }

        fn closeEndOfStream(_: ?*anyopaque) anyerror!void {
            return error.EndOfStream;
        }
    };

    var request = try httpx.Request.init(std.testing.allocator, .GET, "/");
    defer request.deinit();
    var context = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer context.deinit();
    var cancellation = std.atomic.Value(bool).init(false);
    context.cancellation = &cancellation;

    context.stream_delegate = .{
        .ptr = null,
        .start = Delegate.startCanceled,
        .write = Delegate.writeTimeout,
        .close = Delegate.closeEndOfStream,
    };
    var canceled = Outbound{ .context = &context };
    const canceled_sink = canceled.stream();
    try std.testing.expectEqual(.canceled, canceled_sink.start.?(canceled_sink.context, 200));

    context.stream_delegate.?.start = Delegate.startOk;
    var terminal = Outbound{ .context = &context };
    const terminal_sink = terminal.stream();
    try std.testing.expectEqual(.ok, terminal_sink.start.?(terminal_sink.context, 200));
    try std.testing.expectEqual(.timeout, terminal_sink.write.?(terminal_sink.context, abi.Bytes.init("x")));
    try std.testing.expectEqual(.end_of_stream, terminal_sink.close.?(terminal_sink.context));

    cancellation.store(true, .release);
    var pre_canceled = Outbound{ .context = &context };
    const pre_canceled_sink = pre_canceled.stream();
    try std.testing.expectEqual(.canceled, pre_canceled_sink.start.?(pre_canceled_sink.context, 200));
    try std.testing.expectEqual(.canceled, terminal_sink.close.?(terminal_sink.context));
}

test "outbound callbacks prefer cancellation that arrives during transport IO" {
    const std = @import("std");
    const State = struct {
        signal: *std.atomic.Value(bool),

        fn cancel(self: *@This()) void {
            self.signal.store(true, .release);
        }

        fn readAll(raw: ?*anyopaque) anyerror!?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return error.BodySourceUnavailable));
            self.cancel();
            return error.StreamReset;
        }

        fn startCanceled(raw: ?*anyopaque, _: u16) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return error.StreamUnavailable));
            self.cancel();
            return error.ConnectionClosed;
        }

        fn startOk(_: ?*anyopaque, _: u16) anyerror!void {}

        fn writeCanceled(raw: ?*anyopaque, _: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return error.StreamUnavailable));
            self.cancel();
            return error.StreamReset;
        }

        fn closeCanceled(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return error.StreamUnavailable));
            self.cancel();
            return error.ConnectionClosed;
        }
    };

    var request = try httpx.Request.init(std.testing.allocator, .POST, "/stream");
    defer request.deinit();
    var context = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer context.deinit();
    var cancellation = std.atomic.Value(bool).init(false);
    context.cancellation = &cancellation;
    var state = State{ .signal = &cancellation };

    context.body_delegate = .{
        .ptr = &state,
        .read_all = State.readAll,
        .streaming = true,
    };
    var body_outbound = Outbound{ .context = &context };
    const body_source = body_outbound.bodySource();
    var body: abi.OptionalBytes = .{};
    try std.testing.expectEqual(.canceled, body_source.read_all.?(body_source.context, &body));

    cancellation.store(false, .release);
    context.stream_delegate = .{
        .ptr = &state,
        .start = State.startCanceled,
        .write = State.writeCanceled,
        .close = State.closeCanceled,
    };
    var start_outbound = Outbound{ .context = &context };
    const start_sink = start_outbound.stream();
    try std.testing.expectEqual(.canceled, start_sink.start.?(start_sink.context, 200));

    cancellation.store(false, .release);
    context.stream_delegate.?.start = State.startOk;
    var stream_outbound = Outbound{ .context = &context };
    const stream_sink = stream_outbound.stream();
    try std.testing.expectEqual(.ok, stream_sink.start.?(stream_sink.context, 200));
    try std.testing.expectEqual(.canceled, stream_sink.write.?(stream_sink.context, abi.Bytes.init("x")));

    cancellation.store(false, .release);
    try std.testing.expectEqual(.canceled, stream_sink.close.?(stream_sink.context));
}
