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
            .read = readBodyChunk,
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

    fn readBodyChunk(raw: ?*anyopaque, dest: [*]u8, capacity: usize, count: *usize) callconv(.c) abi.CallbackStatus {
        count.* = 0;
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (self.context.isCancellationRequested()) return .canceled;
        var reader = self.context.requestBodyReader();
        const n = reader.read(dest[0..capacity]) catch |err| return callbackStatusAfterIo(self.context, err);
        if (n > capacity) return .failed;
        count.* = n;
        return .ok;
    }

    fn readAllBody(raw: ?*anyopaque, out: *abi.OptionalBytes) callconv(.c) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (self.context.isCancellationRequested()) return .canceled;
        const body = self.context.body() catch |err| return callbackStatusAfterIo(self.context, err);
        out.* = abi.OptionalBytes.init(body);
        return .ok;
    }

    fn start(raw: ?*anyopaque, status: u16, content_type: abi.Bytes, headers: abi.HeaderList) callconv(.c) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (self.context.isCancellationRequested()) return .canceled;
        if (self.started) return .failed;
        if (headers.ptr == null and headers.len != 0) return .failed;
        // Replace matching transport middleware fields, preserving repeated
        // application values such as Set-Cookie. The transport still owns
        // framing and will discard buffered length/transfer-encoding fields.
        for (headers.slice()) |header| self.context.response.headers.removeAll(header.name.slice());
        for (headers.slice()) |header| {
            self.context.response.headers.append(header.name.slice(), header.value.slice()) catch |err| return callbackStatusAfterIo(self.context, err);
        }
        self.writer = self.context.streamResponseWithContentType(status, content_type.slice()) catch |err| return callbackStatusAfterIo(self.context, err);
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
    if (body_source.read_all != null or body_source.read != null) {
        context.body_delegate = .{
            .ptr = @constCast(body_source),
            .read_all = inboundReadAllBody,
            .read = if (body_source.read != null) inboundReadBodyChunk else null,
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

fn inboundReadBodyChunk(raw: ?*anyopaque, dest: []u8) !usize {
    const source: *const abi.RequestBodySource = @ptrCast(@alignCast(raw orelse return error.BodySourceUnavailable));
    var count: usize = 0;
    switch (source.read.?(source.context, dest.ptr, dest.len, &count)) {
        .ok => {},
        .failed => return error.BodyReadFailed,
        .canceled => return error.Canceled,
        .timeout => return error.Timeout,
        .body_too_large => return error.BodyTooLarge,
        .body_capacity_exceeded => return error.BodyCapacityExceeded,
        .end_of_stream => return error.EndOfStream,
    }
    if (count > dest.len) return error.InvalidBodyReadCount;
    return count;
}

fn inboundReadAllBody(raw: ?*anyopaque) !?[]const u8 {
    const body_source: *const abi.RequestBodySource = @ptrCast(@alignCast(raw orelse return error.BodySourceUnavailable));
    var bytes: abi.OptionalBytes = .{};
    const read = body_source.read_all orelse return error.BodySourceUnavailable;
    switch (read(body_source.context, &bytes)) {
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

fn inboundStart(raw: ?*anyopaque, status: u16, content_type: []const u8, headers: *const httpx.Headers) !void {
    const stream: *const abi.StreamSink = @ptrCast(@alignCast(raw orelse return error.StreamUnavailable));
    var local: [16]abi.HeaderView = undefined;
    const views = if (headers.count() <= local.len) local[0..headers.count()] else try headers.allocator.alloc(abi.HeaderView, headers.count());
    defer if (views.len > local.len) headers.allocator.free(views);
    for (headers.iterator(), views) |header, *view| view.* = .{ .name = .init(header.name), .value = .init(header.value) };
    try callbackResult(stream.start.?(stream.context, status, abi.Bytes.init(content_type), .{ .ptr = views.ptr, .len = views.len }));
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

test "runtime HTTP streaming carries policy headers before commitment across both adapters" {
    const std = @import("std");
    const Capture = struct {
        started: bool = false,
        fn start(raw: ?*anyopaque, status: u16, content_type: []const u8, headers: *const httpx.Headers) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expectEqual(@as(u16, 200), status);
            try std.testing.expectEqualStrings("application/x-ndjson", content_type);
            try std.testing.expectEqualStrings("v1", headers.get("X-Antfly-Catalog-Route-Fence-Ack").?);
            try std.testing.expectEqualStrings("https://example.test", headers.get("Access-Control-Allow-Origin").?);
            try std.testing.expect(headers.get("Content-Length") == null);
            try std.testing.expect(headers.get("Transfer-Encoding") == null);
            self.started = true;
        }
        fn write(_: ?*anyopaque, _: []const u8) !void {}
        fn close(_: ?*anyopaque) !void {}
    };
    const Fixture = struct {
        fn check(alloc: @import("std").mem.Allocator, count: usize) !void {
            var request = try httpx.Request.init(alloc, .POST, "http://localhost/internal/v1/groups/1/tables/entities/documents");
            defer request.deinit();
            var outer = httpx.Context.init(alloc, @import("std").testing.io, &request);
            defer outer.deinit();
            var capture: Capture = .{};
            outer.stream_delegate = .{ .ptr = &capture, .start = Capture.start, .write = Capture.write, .close = Capture.close };
            var outbound = Outbound{ .context = &outer };
            var stream = outbound.stream();
            {
                var inner = httpx.Context.init(alloc, @import("std").testing.io, &request);
                defer inner.deinit();
                installInbound(&inner, &.{}, &.{}, &stream);
                try inner.setHeader("X-Antfly-Catalog-Route-Fence-Ack", "v1");
                try inner.setHeader("Access-Control-Allow-Origin", "https://example.test");
                try inner.setHeader("Content-Length", "999");
                try inner.setHeader("Transfer-Encoding", "identity");
                try inner.response.headers.append("Content-Length", "1000");
                try inner.response.headers.append("Transfer-Encoding", "chunked");
                for (0..count) |_| try inner.response.headers.append("Set-Cookie", "a=b; Secure");
                var writer = inner.streamResponseWithContentType(200, "application/x-ndjson") catch |err| {
                    // The stable C callback reports allocator failure as the
                    // generic stream class. Preserve OOM for the fault sweep.
                    if (err == error.StreamWriteFailed) return error.OutOfMemory;
                    return err;
                };
                try writer.close();
            }
            try @import("std").testing.expect(capture.started);
            // Incoming header storage is gone; the transport owns its copies.
            var cookies: usize = 0;
            for (outer.response.headers.iterator()) |header| {
                if (@import("std").ascii.eqlIgnoreCase(header.name, "Set-Cookie")) {
                    try @import("std").testing.expectEqualStrings("a=b; Secure", header.value);
                    cookies += 1;
                }
            }
            try @import("std").testing.expectEqual(count, cookies);
        }
    };
    for ([_]usize{ 2, 32 }) |count| try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{count});
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
        saw_ndjson: bool = false,
        bytes: [16]u8 = undefined,
        bytes_len: usize = 0,
        closed: bool = false,

        fn isCancelled(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw orelse return 0));
            return @intFromBool(self.canceled);
        }

        fn start(raw: ?*anyopaque, status: u16, content_type: abi.Bytes, _: abi.HeaderList) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return .failed));
            self.status = status;
            self.saw_ndjson = std.mem.eql(u8, content_type.slice(), "application/x-ndjson");
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

    var writer = try context.streamResponseWithContentType(202, "application/x-ndjson");
    try writer.write("linked");
    try writer.close();
    try std.testing.expectEqual(@as(u16, 202), state.status);
    try std.testing.expect(state.saw_ndjson);
    try std.testing.expectEqualStrings("linked", state.bytes[0..state.bytes_len]);
    try std.testing.expect(state.closed);
}

test "outbound stream callbacks preserve terminal status classes" {
    const std = @import("std");
    const Delegate = struct {
        fn startCanceled(_: ?*anyopaque, _: u16, _: []const u8, _: *const httpx.Headers) anyerror!void {
            return error.Canceled;
        }

        fn startOk(_: ?*anyopaque, _: u16, _: []const u8, _: *const httpx.Headers) anyerror!void {}

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
    try std.testing.expectEqual(.canceled, canceled_sink.start.?(canceled_sink.context, 200, abi.Bytes.init("text/event-stream; charset=utf-8"), .{}));

    context.stream_delegate.?.start = Delegate.startOk;
    var terminal = Outbound{ .context = &context };
    const terminal_sink = terminal.stream();
    try std.testing.expectEqual(.ok, terminal_sink.start.?(terminal_sink.context, 200, abi.Bytes.init("text/event-stream; charset=utf-8"), .{}));
    try std.testing.expectEqual(.timeout, terminal_sink.write.?(terminal_sink.context, abi.Bytes.init("x")));
    try std.testing.expectEqual(.end_of_stream, terminal_sink.close.?(terminal_sink.context));

    cancellation.store(true, .release);
    var pre_canceled = Outbound{ .context = &context };
    const pre_canceled_sink = pre_canceled.stream();
    try std.testing.expectEqual(.canceled, pre_canceled_sink.start.?(pre_canceled_sink.context, 200, abi.Bytes.init("text/event-stream; charset=utf-8"), .{}));
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

        fn startCanceled(raw: ?*anyopaque, _: u16, _: []const u8, _: *const httpx.Headers) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return error.StreamUnavailable));
            self.cancel();
            return error.ConnectionClosed;
        }

        fn startOk(_: ?*anyopaque, _: u16, _: []const u8, _: *const httpx.Headers) anyerror!void {}

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
    try std.testing.expectEqual(.canceled, start_sink.start.?(start_sink.context, 200, abi.Bytes.init("text/event-stream; charset=utf-8"), .{}));

    cancellation.store(false, .release);
    context.stream_delegate.?.start = State.startOk;
    var stream_outbound = Outbound{ .context = &context };
    const stream_sink = stream_outbound.stream();
    try std.testing.expectEqual(.ok, stream_sink.start.?(stream_sink.context, 200, abi.Bytes.init("text/event-stream; charset=utf-8"), .{}));
    try std.testing.expectEqual(.canceled, stream_sink.write.?(stream_sink.context, abi.Bytes.init("x")));

    cancellation.store(false, .release);
    try std.testing.expectEqual(.canceled, stream_sink.close.?(stream_sink.context));
}

test "linked incremental body reads cross both adapters without materializing the upload" {
    const std = @import("std");
    const State = struct {
        remaining: usize = 1024 * 1024,
        calls: usize = 0,
        failure: ?anyerror = null,
        canceled: bool = false,
        fn readAll(_: ?*anyopaque) !?[]const u8 {
            return error.UnexpectedWholeBodyRead;
        }
        fn read(raw: ?*anyopaque, bytes: []u8) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.failure) |err| return err;
            const n = @min(self.remaining, @min(bytes.len, 137));
            @memset(bytes[0..n], 'k');
            self.remaining -= n;
            return n;
        }
        fn isCanceled(raw: ?*const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return self.canceled;
        }
    };
    var request = try httpx.Request.init(std.testing.allocator, .POST, "/checkpoint");
    defer request.deinit();
    var outer = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer outer.deinit();
    var state: State = .{};
    outer.body_delegate = .{ .ptr = &state, .read_all = State.readAll, .read = State.read, .streaming = true };
    outer.cancellation_probe = .{ .ptr = &state, .is_cancelled = State.isCanceled };
    var outbound = Outbound{ .context = &outer };
    const source = outbound.bodySource();
    const cancellation = outbound.cancellation();
    var inner = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer inner.deinit();
    installInbound(&inner, &cancellation, &source, &.{});
    try std.testing.expect(inner.hasStreamingRequestBody());
    try std.testing.expectEqual(@as(usize, 0), state.calls);
    var reader = inner.requestBodyReader();
    var buffer: [512]u8 = undefined;
    var total: usize = 0;
    while (true) {
        const n = try reader.read(&buffer);
        if (n == 0) break;
        for (buffer[0..n]) |byte| try std.testing.expectEqual(@as(u8, 'k'), byte);
        total += n;
    }
    try std.testing.expectEqual(@as(usize, 1024 * 1024), total);
    try std.testing.expect(request.body == null);
    for ([_]anyerror{ error.Timeout, error.BodyTooLarge, error.BodyCapacityExceeded, error.EndOfStream, error.Canceled }) |err| {
        state.failure = err;
        try std.testing.expectError(err, reader.read(&buffer));
    }
    state.canceled = true;
    const calls = state.calls;
    try std.testing.expectError(error.Canceled, reader.read(&buffer));
    try std.testing.expectEqual(calls, state.calls);
}

test "linked incremental body callback rejects an oversized count" {
    const std = @import("std");
    const Broken = struct {
        fn read(_: ?*anyopaque, _: [*]u8, capacity: usize, count: *usize) callconv(.c) abi.CallbackStatus {
            count.* = capacity + 1;
            return .ok;
        }
    };
    var source: abi.RequestBodySource = .{ .read = Broken.read };
    var buffer: [8]u8 = undefined;
    try std.testing.expectError(error.InvalidBodyReadCount, inboundReadBodyChunk(&source, &buffer));
}

test "linked incremental-only source is installed without a whole-body fallback" {
    const std = @import("std");
    const Source = struct {
        remaining: usize = 13,
        fn read(raw: ?*anyopaque, destination: [*]u8, capacity: usize, count: *usize) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            count.* = @min(self.remaining, capacity, 3);
            @memset(destination[0..count.*], 'x');
            self.remaining -= count.*;
            return .ok;
        }
    };
    var source: Source = .{};
    const body: abi.RequestBodySource = .{ .context = &source, .read = Source.read, .streaming = 1 };
    var request = try httpx.Request.init(std.testing.allocator, .POST, "/ai/v1/generate/context/resume");
    defer request.deinit();
    var context = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer context.deinit();
    installInbound(&context, &.{}, &body, &.{});
    try std.testing.expect(context.hasStreamingRequestBody());
    var reader = context.requestBodyReader();
    var buffer: [5]u8 = undefined;
    var total: usize = 0;
    while (true) {
        const n = try reader.read(&buffer);
        if (n == 0) break;
        for (buffer[0..n]) |b| try std.testing.expectEqual(@as(u8, 'x'), b);
        total += n;
    }
    try std.testing.expectEqual(@as(usize, 13), total);
    try std.testing.expect(request.body == null);
    try std.testing.expectError(error.BodySourceUnavailable, context.body());
}

test "runtime HTTP body source preserves incremental reads without materializing" {
    const std = @import("std");
    const Source = struct {
        offset: usize = 0,
        fn all(_: ?*anyopaque) !?[]const u8 {
            return error.UnexpectedMaterialization;
        }
        fn read(raw: ?*anyopaque, dest: []u8) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            const bytes = "audio";
            const n = @min(dest.len, bytes.len - self.offset);
            @memcpy(dest[0..n], bytes[self.offset..][0..n]);
            self.offset += n;
            return n;
        }
    };
    const a = std.testing.allocator;
    var request = try httpx.Request.init(a, .POST, "http://localhost/stream");
    defer request.deinit();
    var outer = httpx.Context.init(a, std.testing.io, &request);
    defer outer.deinit();
    var source: Source = .{};
    outer.body_delegate = .{ .ptr = &source, .read_all = Source.all, .read = Source.read, .streaming = true };
    var transport = Outbound{ .context = &outer };
    const body_source = transport.bodySource();
    var inner_request = try httpx.Request.init(a, .POST, "http://localhost/stream");
    defer inner_request.deinit();
    var inner = httpx.Context.init(a, std.testing.io, &inner_request);
    defer inner.deinit();
    const cancellation: abi.CancellationView = .{};
    const stream: abi.StreamSink = .{};
    installInbound(&inner, &cancellation, &body_source, &stream);
    try std.testing.expect(inner.hasStreamingRequestBody());
    var reader = inner.requestBodyReader();
    var bytes: [3]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 3), try reader.read(&bytes));
    try std.testing.expectEqualStrings("aud", &bytes);
    try std.testing.expectEqual(@as(usize, 2), try reader.read(&bytes));
    try std.testing.expectEqualStrings("io", bytes[0..2]);
    try std.testing.expectEqual(@as(usize, 0), try reader.read(&bytes));
    try std.testing.expect(inner.request.body == null);
}
