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

const WriteControl = httpx.socket.WriteControl;

/// Each adapter is owned by one synchronous callback. Its view is never saved
/// in Outbound or a stream writer, so joined monitors cannot outlive the borrow.
const CallControl = struct {
    base: WriteControl,
    incoming: ?*const abi.RequestControl,

    fn result(status: abi.CallbackStatus) WriteControl.Error!void {
        return switch (status) {
            .ok => {},
            .timeout => error.Timeout,
            else => error.Canceled,
        };
    }
    fn check(raw: ?*anyopaque) WriteControl.Error!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try self.base.check();
        if (self.incoming) |incoming| {
            const call = incoming.check orelse return error.Canceled;
            try result(call(incoming.context));
        }
    }
    fn remaining(raw: ?*anyopaque) WriteControl.Error!?u64 {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        var limit = try self.base.timeout(null);
        if (self.incoming) |incoming| {
            const call = incoming.remaining orelse return error.Canceled;
            var timeout: abi.OptionalTimeout = .{};
            try result(call(incoming.context, &timeout));
            if (timeout.present > 1) return error.Canceled;
            if (timeout.present == 1) {
                if (timeout.milliseconds == 0) return error.Timeout;
                limit = @min(limit orelse timeout.milliseconds, timeout.milliseconds);
            }
        }
        return limit;
    }
    fn begin(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try self.base.wait_begin_fn.?(self.base.ptr);
    }
    fn end(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try self.base.wait_end_fn.?(self.base.ptr);
    }
    fn view(self: *@This()) WriteControl {
        if (self.incoming == null) return self.base;
        return .{ .ptr = self, .check_fn = check, .remaining_ms_fn = remaining, .wait_begin_fn = if (self.base.wait_begin_fn != null) begin else null, .wait_end_fn = if (self.base.wait_end_fn != null) end else null };
    }
};

const ExportControl = struct {
    control: WriteControl,
    fn check(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.control.check() catch |err| return callbackStatusFromError(err);
        return .ok;
    }
    fn remaining(raw: ?*anyopaque, out: *abi.OptionalTimeout) callconv(.c) abi.CallbackStatus {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        const ms = self.control.timeout(null) catch |err| return callbackStatusFromError(err);
        out.* = if (ms) |value| .{ .milliseconds = value, .present = 1 } else .{};
        return .ok;
    }
    fn view(self: *@This()) abi.RequestControl {
        return .{ .context = self, .check = check, .remaining = remaining };
    }
};

pub const Outbound = struct {
    context: *httpx.Context,
    writer: ?httpx.Context.StreamWriter = null,
    started: bool = false,
    write_control: httpx.socket.WriteControl = .{},
    execution_wait: abi.ExecutionWait = .{},

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
            .read_all_controlled = readAllBodyControlled,
            .streaming = @intFromBool(self.context.hasStreamingRequestBody()),
        };
    }

    pub fn stream(self: *Outbound) abi.StreamSink {
        return .{
            .context = self,
            .start = start,
            .write = write,
            .close = close,
            .set_execution_wait = setExecutionWait,
            .start_controlled = startControlled,
            .write_controlled = writeControlled,
            .close_controlled = closeControlled,
        };
    }

    fn setExecutionWait(raw: ?*anyopaque, view: ?*const abi.ExecutionWait) callconv(.c) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (view) |value| {
            if (value.begin == null or value.end == null) return .failed;
            self.execution_wait = value.*;
        } else self.execution_wait = .{};
        if (self.writer) |*writer| {
            writer.control = self.outputControl();
            if (writer.h2_writer) |*h2_writer| h2_writer.control = writer.control;
        }
        return .ok;
    }

    fn checkOutput(raw: ?*anyopaque) httpx.socket.WriteControl.Error!void {
        const self: *Outbound = @ptrCast(@alignCast(raw.?));
        try self.write_control.check();
    }
    fn remainingOutput(raw: ?*anyopaque) httpx.socket.WriteControl.Error!?u64 {
        const self: *Outbound = @ptrCast(@alignCast(raw.?));
        return self.write_control.timeout(null);
    }
    fn beginOutputWait(raw: ?*anyopaque) !void {
        const self: *Outbound = @ptrCast(@alignCast(raw.?));
        try callbackResult(self.execution_wait.begin.?(self.execution_wait.context));
    }
    fn endOutputWait(raw: ?*anyopaque) !void {
        const self: *Outbound = @ptrCast(@alignCast(raw.?));
        try callbackResult(self.execution_wait.end.?(self.execution_wait.context));
    }
    fn outputControl(self: *Outbound) httpx.socket.WriteControl {
        if (self.execution_wait.begin == null) return self.context.requestWriteControl(self.write_control);
        return self.context.requestWriteControl(.{ .ptr = self, .check_fn = checkOutput, .remaining_ms_fn = remainingOutput, .wait_begin_fn = beginOutputWait, .wait_end_fn = endOutputWait });
    }

    fn isCancelled(raw: ?*const anyopaque) callconv(.c) u8 {
        const context: *const httpx.Context = @ptrCast(@alignCast(raw orelse return 0));
        return @intFromBool(context.isCancellationRequested());
    }

    fn readAllBody(raw: ?*anyopaque, out: *abi.OptionalBytes) callconv(.c) abi.CallbackStatus {
        return readAllBodyWithControl(raw, out, null);
    }
    fn readAllBodyControlled(raw: ?*anyopaque, out: *abi.OptionalBytes, incoming: *const abi.RequestControl) callconv(.c) abi.CallbackStatus {
        return readAllBodyWithControl(raw, out, incoming);
    }
    fn readAllBodyWithControl(raw: ?*anyopaque, out: *abi.OptionalBytes, incoming: ?*const abi.RequestControl) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (self.context.isCancellationRequested()) return .canceled;
        var control: CallControl = .{ .base = self.outputControl(), .incoming = incoming };
        const body = self.context.bodyControlled(control.view()) catch |err| return callbackStatusAfterIo(self.context, err);
        out.* = abi.OptionalBytes.init(body);
        return .ok;
    }

    fn start(raw: ?*anyopaque, status: u16, content_type: abi.Bytes, headers: abi.HeaderList) callconv(.c) abi.CallbackStatus {
        return startWithControl(raw, status, content_type, headers, null);
    }
    fn startControlled(raw: ?*anyopaque, status: u16, content_type: abi.Bytes, headers: abi.HeaderList, incoming: *const abi.RequestControl) callconv(.c) abi.CallbackStatus {
        return startWithControl(raw, status, content_type, headers, incoming);
    }
    fn startWithControl(raw: ?*anyopaque, status: u16, content_type: abi.Bytes, headers: abi.HeaderList, incoming: ?*const abi.RequestControl) abi.CallbackStatus {
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
        var control: CallControl = .{ .base = self.outputControl(), .incoming = incoming };
        var writer = self.context.streamResponseWithContentTypeControlled(status, content_type.slice(), control.view()) catch |err| return callbackStatusAfterIo(self.context, err);
        // streamResponse copies its control. Drop the callback-local view before
        // publishing the persistent writer, including its nested H2 writer.
        writer.control = self.outputControl();
        if (writer.h2_writer) |*h2_writer| h2_writer.control = writer.control;
        self.writer = writer;
        self.started = true;
        return .ok;
    }

    fn write(raw: ?*anyopaque, bytes: abi.Bytes) callconv(.c) abi.CallbackStatus {
        return writeWithControl(raw, bytes, null);
    }
    fn writeControlled(raw: ?*anyopaque, bytes: abi.Bytes, incoming: *const abi.RequestControl) callconv(.c) abi.CallbackStatus {
        return writeWithControl(raw, bytes, incoming);
    }
    fn writeWithControl(raw: ?*anyopaque, bytes: abi.Bytes, incoming: ?*const abi.RequestControl) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (self.context.isCancellationRequested()) return .canceled;
        const writer = if (self.writer) |*writer| writer else return .failed;
        var control: CallControl = .{ .base = self.outputControl(), .incoming = incoming };
        writer.control = control.view();
        if (writer.h2_writer) |*h2_writer| h2_writer.control = writer.control;
        defer {
            writer.control = self.outputControl();
            if (writer.h2_writer) |*h2_writer| h2_writer.control = writer.control;
        }
        writer.write(bytes.slice()) catch |err| return callbackStatusAfterIo(self.context, err);
        return .ok;
    }

    fn close(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
        return closeWithControl(raw, null);
    }
    fn closeControlled(raw: ?*anyopaque, incoming: *const abi.RequestControl) callconv(.c) abi.CallbackStatus {
        return closeWithControl(raw, incoming);
    }
    fn closeWithControl(raw: ?*anyopaque, incoming: ?*const abi.RequestControl) abi.CallbackStatus {
        const self: *Outbound = @ptrCast(@alignCast(raw orelse return .failed));
        if (self.context.isCancellationRequested()) return .canceled;
        const writer = if (self.writer) |*writer| writer else return .failed;
        var control: CallControl = .{ .base = self.outputControl(), .incoming = incoming };
        writer.control = control.view();
        if (writer.h2_writer) |*h2_writer| h2_writer.control = writer.control;
        defer {
            writer.control = self.outputControl();
            if (writer.h2_writer) |*h2_writer| h2_writer.control = writer.control;
        }
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
            .read_all_controlled = inboundReadAllBodyControlled,
            .streaming = body_source.streaming != 0,
        };
    }
    if (stream.start != null and stream.write != null and stream.close != null) {
        context.stream_delegate = .{
            .ptr = @constCast(stream),
            .start = inboundStart,
            .write = inboundWrite,
            .close = inboundClose,
            .start_controlled = inboundStartControlled,
            .write_controlled = inboundWriteControlled,
            .close_controlled = inboundCloseControlled,
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

fn inboundReadAllBodyControlled(raw: ?*anyopaque, control: WriteControl) !?[]const u8 {
    if (!control.active()) return inboundReadAllBody(raw);
    const source: *const abi.RequestBodySource = @ptrCast(@alignCast(raw orelse return error.BodySourceUnavailable));
    const call = source.read_all_controlled orelse return error.RequestControlUnavailable;
    var adapter: ExportControl = .{ .control = control };
    const view = adapter.view();
    var bytes: abi.OptionalBytes = .{};
    const status = call(source.context, &bytes, &view);
    if (status == .failed) return error.BodyReadFailed;
    try callbackResult(status);
    try control.check();
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

fn inboundStartControlled(raw: ?*anyopaque, status: u16, content_type: []const u8, headers: *const httpx.Headers, control: WriteControl) !void {
    if (!control.active()) return inboundStart(raw, status, content_type, headers);
    const stream: *const abi.StreamSink = @ptrCast(@alignCast(raw orelse return error.StreamUnavailable));
    if (stream.write_controlled == null or stream.close_controlled == null) return error.RequestControlUnavailable;
    const call = stream.start_controlled orelse return error.RequestControlUnavailable;
    var local: [16]abi.HeaderView = undefined;
    const views = if (headers.count() <= local.len) local[0..headers.count()] else try headers.allocator.alloc(abi.HeaderView, headers.count());
    defer if (views.len > local.len) headers.allocator.free(views);
    for (headers.iterator(), views) |header, *view| view.* = .{ .name = .init(header.name), .value = .init(header.value) };
    var adapter: ExportControl = .{ .control = control };
    const view = adapter.view();
    try callbackResult(call(stream.context, status, .init(content_type), .{ .ptr = views.ptr, .len = views.len }, &view));
    try control.check();
}

fn inboundWriteControlled(raw: ?*anyopaque, bytes: []const u8, control: WriteControl) !void {
    if (!control.active()) return inboundWrite(raw, bytes);
    const stream: *const abi.StreamSink = @ptrCast(@alignCast(raw orelse return error.StreamUnavailable));
    const call = stream.write_controlled orelse return error.RequestControlUnavailable;
    var adapter: ExportControl = .{ .control = control };
    const view = adapter.view();
    try callbackResult(call(stream.context, .init(bytes), &view));
    try control.check();
}

fn inboundCloseControlled(raw: ?*anyopaque, control: WriteControl) !void {
    if (!control.active()) return inboundClose(raw);
    const stream: *const abi.StreamSink = @ptrCast(@alignCast(raw orelse return error.StreamUnavailable));
    const call = stream.close_controlled orelse return error.RequestControlUnavailable;
    var adapter: ExportControl = .{ .control = control };
    const view = adapter.view();
    try callbackResult(call(stream.context, &view));
    try control.check();
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

test "outbound original output fences survive linked stream callbacks" {
    const std = @import("std");
    const State = struct {
        failure: ?httpx.socket.WriteControl.Error = null,
        starts: usize = 0,
        writes: usize = 0,
        closes: usize = 0,
        fn check(raw: ?*anyopaque) httpx.socket.WriteControl.Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.failure) |err| return err;
        }
        fn startControlled(raw: ?*anyopaque, status: u16, content_type: []const u8, headers: *const httpx.Headers, control: WriteControl) !void {
            try control.check();
            return start(raw, status, content_type, headers);
        }
        fn writeControlled(raw: ?*anyopaque, bytes: []const u8, control: WriteControl) !void {
            try control.check();
            return write(raw, bytes);
        }
        fn closeControlled(raw: ?*anyopaque, control: WriteControl) !void {
            try control.check();
            return close(raw);
        }
        fn start(raw: ?*anyopaque, _: u16, _: []const u8, _: *const httpx.Headers) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.starts += 1;
        }
        fn write(raw: ?*anyopaque, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.writes += 1;
        }
        fn close(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.closes += 1;
        }
    };
    var request = try httpx.Request.init(std.testing.allocator, .POST, "/stream");
    defer request.deinit();
    var outer = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer outer.deinit();
    var state = State{ .failure = error.Timeout };
    outer.stream_delegate = .{ .ptr = &state, .start = State.start, .write = State.write, .close = State.close, .start_controlled = State.startControlled, .write_controlled = State.writeControlled, .close_controlled = State.closeControlled };
    var outbound = Outbound{ .context = &outer, .write_control = .{ .ptr = &state, .check_fn = State.check } };
    var sink = outbound.stream();
    try std.testing.expectEqual(.timeout, sink.start.?(sink.context, 200, abi.Bytes.init("application/x-ndjson"), .{}));
    try std.testing.expectEqual(@as(usize, 0), state.starts);
    // Each terminal case has its own stream: close retires a writer even when
    // the transport rejects it, so subsequent writes correctly report closed.
    for ([_]httpx.socket.WriteControl.Error{ error.Timeout, error.Canceled }) |failure| {
        state.failure = null;
        outbound = .{ .context = &outer, .write_control = .{ .ptr = &state, .check_fn = State.check } };
        sink = outbound.stream();
        var inner = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
        defer inner.deinit();
        installInbound(&inner, &.{}, &.{}, &sink);
        var writer = try inner.streamResponseWithContentType(200, "application/x-ndjson");
        try writer.write("first\n");
        const writes = state.writes;
        state.failure = failure;
        try std.testing.expectError(failure, writer.write("late\n"));
        try std.testing.expectError(failure, writer.close());
        try std.testing.expectEqual(writes, state.writes);
        try std.testing.expectEqual(@as(usize, 0), state.closes);
    }
    try std.testing.expectEqual(@as(usize, 2), state.starts);
    try std.testing.expectEqual(@as(usize, 2), state.writes);
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

test "outbound checked execution wait installation preserves fences and clears borrowed hooks" {
    const std = @import("std");
    const State = struct {
        parked: bool = false,
        canceled: bool = false,
        begins: usize = 0,
        ends: usize = 0,
        fn check(raw: ?*anyopaque) httpx.socket.WriteControl.Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.canceled) return error.Canceled;
        }
        fn begin(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.parked) return .failed;
            self.parked = true;
            self.begins += 1;
            return .ok;
        }
        fn end(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (!self.parked) return .failed;
            self.parked = false;
            self.ends += 1;
            return .ok;
        }
        fn operation(self: *@This()) !void {
            try std.testing.expect(self.parked);
        }
    };
    var request = try httpx.Request.init(std.testing.allocator, .POST, "/stream");
    defer request.deinit();
    var context = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer context.deinit();
    var state = State{};
    var outbound = Outbound{ .context = &context, .write_control = .{ .ptr = &state, .check_fn = State.check } };
    const sink = outbound.stream();
    try std.testing.expectEqual(.failed, sink.set_execution_wait.?(sink.context, &.{ .context = &state, .begin = State.begin }));
    try std.testing.expect(outbound.execution_wait.begin == null);
    try std.testing.expectEqual(.ok, sink.set_execution_wait.?(sink.context, &.{ .context = &state, .begin = State.begin, .end = State.end }));
    try outbound.outputControl().withWait(State.operation, .{&state});
    try std.testing.expect(!state.parked);
    try std.testing.expectEqual(@as(usize, 1), state.begins);
    try std.testing.expectEqual(@as(usize, 1), state.ends);
    state.canceled = true;
    try std.testing.expectError(error.Canceled, outbound.outputControl().withWait(State.operation, .{&state}));
    try std.testing.expectEqual(@as(usize, 1), state.begins);
    // Retirement must clear borrowed stack pointers despite cancellation.
    try std.testing.expectEqual(.ok, sink.set_execution_wait.?(sink.context, null));
    try std.testing.expect(outbound.outputControl().wait_begin_fn == null);
    try std.testing.expectError(error.Canceled, outbound.outputControl().check());
}

test "opaque HTTP controls interrupt physical lazy H1 reads and release the callback borrow" {
    const std = @import("std");
    const Body = struct {
        fn legacy(raw: ?*anyopaque) !?[]const u8 {
            const reader: *httpx.Context.H1StreamReader = @ptrCast(@alignCast(raw.?));
            return reader.readAllControlled(.{});
        }
        fn controlled(raw: ?*anyopaque, control: WriteControl) !?[]const u8 {
            const reader: *httpx.Context.H1StreamReader = @ptrCast(@alignCast(raw.?));
            return reader.readAllControlled(control);
        }
    };
    const io = std.testing.io;
    for ([_]bool{ false, true }) |native| {
        var listener = try httpx.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
        defer listener.deinit();
        var socket = try httpx.Socket.connect(listener.getLocalAddress(), io);
        defer socket.close();
        if (native) socket.enableNativeTimeouts();
        try socket.setRecvTimeout(2000);
        var peer = try listener.accept();
        defer peer.socket.close();
        var request = try httpx.Request.init(std.testing.allocator, .POST, "/upload");
        defer request.deinit();
        var outer = httpx.Context.init(std.testing.allocator, io, &request);
        defer outer.deinit();
        const started = std.Io.Clock.awake.now(io).nanoseconds;
        outer.application_deadline_ns = @intCast(started + 2000 * std.time.ns_per_ms);
        outer.application_deadline_io = io;
        var buffer: [8192]u8 = undefined;
        var leftover: usize = 0;
        var reader: httpx.Context.H1StreamReader = .{ .allocator = std.testing.allocator, .io = io, .sock = &socket, .buffer = &buffer, .leftover = &leftover, .remaining = 1, .deadline_ms = @intCast(@divTrunc(started, std.time.ns_per_ms) + 2000) };
        defer reader.deinit();
        outer.body_delegate = .{ .ptr = &reader, .read_all = Body.legacy, .read_all_controlled = Body.controlled, .streaming = true };
        var outbound: Outbound = .{ .context = &outer };
        const source = outbound.bodySource();
        var inner = httpx.Context.init(std.testing.allocator, io, &request);
        defer inner.deinit();
        installInbound(&inner, &.{}, &source, &.{});
        inner.application_deadline_ns = @intCast(started + 100 * std.time.ns_per_ms);
        inner.application_deadline_io = io;
        try std.testing.expectError(error.Timeout, inner.body());
        const elapsed = std.Io.Clock.awake.now(io).nanoseconds - started;
        try std.testing.expect(elapsed < 1000 * std.time.ns_per_ms);
        try std.testing.expect(!reader.control.active());
        try std.testing.expect(reader.owned_body == null);
        try std.testing.expectEqual(@as(u64, 1), reader.remaining);
    }
}

test "opaque HTTP controls bound H2 DATA credit waits without retaining per-call views" {
    const std = @import("std");
    const Waits = struct {
        parked: bool = false,
        begins: usize = 0,
        ends: usize = 0,
        fn begin(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.parked) return .failed;
            self.parked = true;
            self.begins += 1;
            return .ok;
        }
        fn end(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (!self.parked) return .failed;
            self.parked = false;
            self.ends += 1;
            return .ok;
        }
    };
    const io = std.testing.io;
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
    var outer = httpx.Context.init(std.testing.allocator, io, &request);
    defer outer.deinit();
    outer.h2 = &h2;
    outer.h2_sock = &socket;
    outer.h2_stream_id = 1;
    const started = std.Io.Clock.awake.now(io).nanoseconds;
    outer.application_deadline_ns = @intCast(started + 2000 * std.time.ns_per_ms);
    outer.application_deadline_io = io;
    var outbound: Outbound = .{ .context = &outer };
    const sink = outbound.stream();
    var waits: Waits = .{};
    try std.testing.expectEqual(.ok, sink.set_execution_wait.?(sink.context, &.{ .context = &waits, .begin = Waits.begin, .end = Waits.end }));
    var inner = httpx.Context.init(std.testing.allocator, io, &request);
    defer inner.deinit();
    installInbound(&inner, &.{}, &.{}, &sink);
    inner.application_deadline_ns = @intCast(started + 100 * std.time.ns_per_ms);
    inner.application_deadline_io = io;
    var writer = try inner.streamResponseWithContentType(200, "application/octet-stream");
    try std.testing.expect(outbound.writer.?.control.ptr == @as(?*anyopaque, @ptrCast(&outbound)));
    try std.testing.expect(outbound.writer.?.h2_writer.?.control.ptr == @as(?*anyopaque, @ptrCast(&outbound)));
    try std.testing.expectError(error.Timeout, writer.write("blocked"));
    try std.testing.expect(std.Io.Clock.awake.now(io).nanoseconds - started < 1000 * std.time.ns_per_ms);
    try std.testing.expect(outbound.writer.?.control.ptr == @as(?*anyopaque, @ptrCast(&outbound)));
    try std.testing.expect(outbound.writer.?.h2_writer.?.control.ptr == @as(?*anyopaque, @ptrCast(&outbound)));
    try std.testing.expect(waits.begins > 0 and waits.begins == waits.ends);
    try std.testing.expect(!waits.parked);
    try std.testing.expectEqual(.ok, sink.set_execution_wait.?(sink.context, null));
    try std.testing.expect(outbound.writer.?.control.ptr == null);
    try std.testing.expect(outbound.writer.?.h2_writer.?.control.ptr == null);
    // Retirement of the borrow must not turn a stream-local budget into a
    // socket deadline inherited by the next stream.
    try std.testing.expect(socket.request_deadline_ms == null);
    const next = try h2.stream_manager.getOrCreateStream(3);
    next.state = .open;
    outer.h2_stream_id = 3;
    var healthy = try outer.streamResponseWithContentType(200, "text/plain");
    try healthy.write("healthy");
    try healthy.close();
}

test "opaque HTTP controls reject a peer missing physical control callbacks before publication" {
    const std = @import("std");
    const State = struct {
        calls: usize = 0,
        fn start(raw: ?*anyopaque, _: u16, _: abi.Bytes, _: abi.HeaderList) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            return .ok;
        }
        fn startControlled(raw: ?*anyopaque, status: u16, content_type: abi.Bytes, headers: abi.HeaderList, _: *const abi.RequestControl) callconv(.c) abi.CallbackStatus {
            return start(raw, status, content_type, headers);
        }
        fn write(_: ?*anyopaque, _: abi.Bytes) callconv(.c) abi.CallbackStatus {
            return .ok;
        }
        fn close(_: ?*anyopaque) callconv(.c) abi.CallbackStatus {
            return .ok;
        }
        fn body(raw: ?*anyopaque, _: *abi.OptionalBytes) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            return .ok;
        }
    };
    var request = try httpx.Request.init(std.testing.allocator, .GET, "/");
    defer request.deinit();
    var context = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer context.deinit();
    var state: State = .{};
    const source: abi.RequestBodySource = .{ .context = &state, .read_all = State.body };
    const sink: abi.StreamSink = .{ .context = &state, .start = State.start, .write = State.write, .close = State.close };
    installInbound(&context, &.{}, &source, &sink);
    context.application_deadline_io = std.testing.io;
    context.application_deadline_ns = @intCast(std.Io.Clock.awake.now(std.testing.io).nanoseconds + std.time.ns_per_s);
    try std.testing.expectError(error.RequestControlUnavailable, context.body());
    try std.testing.expectError(error.RequestControlUnavailable, context.streamResponse(200));
    try std.testing.expectEqual(@as(usize, 0), state.calls);
    // A partial capability must not commit headers and discover the missing
    // controlled write/close only after the response is already visible.
    var partial_sink = sink;
    partial_sink.start_controlled = State.startControlled;
    installInbound(&context, &.{}, &source, &partial_sink);
    try std.testing.expectError(error.RequestControlUnavailable, context.streamResponse(200));
    try std.testing.expectEqual(@as(usize, 0), state.calls);
}

test "opaque HTTP controls interrupt check-only cancellation during native and Io body reads" {
    const std = @import("std");
    const State = struct {
        canceled: std.atomic.Value(bool) = .init(false),
        parked: bool = false,
        begins: usize = 0,
        ends: usize = 0,
        fn begin(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.parked) return .failed;
            self.parked = true;
            self.begins += 1;
            return .ok;
        }
        fn end(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (!self.parked) return .failed;
            self.parked = false;
            self.ends += 1;
            return .ok;
        }
        fn check(raw: ?*anyopaque) WriteControl.Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.canceled.load(.acquire)) return error.Canceled;
        }
        fn cancel(io: std.Io, self: *@This()) void {
            io.sleep(.fromMilliseconds(100), .awake) catch return;
            self.canceled.store(true, .release);
        }
        fn legacy(raw: ?*anyopaque) !?[]const u8 {
            const reader: *httpx.Context.H1StreamReader = @ptrCast(@alignCast(raw.?));
            return reader.readAllControlled(.{});
        }
        fn body(raw: ?*anyopaque, control: WriteControl) !?[]const u8 {
            const reader: *httpx.Context.H1StreamReader = @ptrCast(@alignCast(raw.?));
            return reader.readAllControlled(control);
        }
    };
    const io = std.testing.io;
    for ([_]bool{ false, true }) |native| {
        var listener = try httpx.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
        defer listener.deinit();
        var socket = try httpx.Socket.connect(listener.getLocalAddress(), io);
        defer socket.close();
        if (native) socket.enableNativeTimeouts();
        try socket.setRecvTimeout(2000);
        var peer = try listener.accept();
        defer peer.socket.close();
        var request = try httpx.Request.init(std.testing.allocator, .POST, "/upload");
        defer request.deinit();
        var outer = httpx.Context.init(std.testing.allocator, io, &request);
        defer outer.deinit();
        var buffer: [8192]u8 = undefined;
        var leftover: usize = 0;
        const started = std.Io.Clock.awake.now(io).nanoseconds;
        var reader: httpx.Context.H1StreamReader = .{ .allocator = std.testing.allocator, .io = io, .sock = &socket, .buffer = &buffer, .leftover = &leftover, .remaining = 1, .deadline_ms = @intCast(@divTrunc(started, std.time.ns_per_ms) + 2000) };
        defer reader.deinit();
        outer.body_delegate = .{ .ptr = &reader, .read_all = State.legacy, .read_all_controlled = State.body, .streaming = true };
        var outbound: Outbound = .{ .context = &outer };
        const source = outbound.bodySource();
        var inner = httpx.Context.init(std.testing.allocator, io, &request);
        defer inner.deinit();
        installInbound(&inner, &.{}, &source, &.{});
        var state: State = .{};
        const sink = outbound.stream();
        try std.testing.expectEqual(.ok, sink.set_execution_wait.?(sink.context, &.{ .context = &state, .begin = State.begin, .end = State.end }));
        var cancel = try io.concurrent(State.cancel, .{ io, &state });
        defer cancel.await(io);
        try std.testing.expectError(error.Canceled, inner.bodyControlled(.{ .ptr = &state, .check_fn = State.check }));
        try std.testing.expect(std.Io.Clock.awake.now(io).nanoseconds - started < 1000 * std.time.ns_per_ms);
        try std.testing.expect(!reader.control.active());
        try std.testing.expect(reader.owned_body == null);
        try std.testing.expect(!state.parked);
        try std.testing.expect(state.begins > 0 and state.begins == state.ends);
        try std.testing.expectEqual(.ok, sink.set_execution_wait.?(sink.context, null));
    }
}

test "opaque HTTP controls bound physical H1 backpressure and keep close state in the owning writer" {
    const std = @import("std");
    const io = std.testing.io;
    for ([_]bool{ false, true }) |native| {
        var listener = try httpx.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
        defer listener.deinit();
        var socket = try httpx.Socket.connect(listener.getLocalAddress(), io);
        defer socket.close();
        if (native) socket.enableNativeTimeouts();
        try socket.setSendTimeout(2000);
        const small_buffer: c_int = 4096;
        try std.posix.setsockopt(socket.handle, std.posix.SOL.SOCKET, std.posix.SO.SNDBUF, std.mem.asBytes(&small_buffer));
        var peer = try listener.accept();
        defer peer.socket.close();
        try std.posix.setsockopt(peer.socket.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVBUF, std.mem.asBytes(&small_buffer));
        var request = try httpx.Request.init(std.testing.allocator, .GET, "/stream");
        defer request.deinit();
        var outer = httpx.Context.init(std.testing.allocator, io, &request);
        defer outer.deinit();
        outer.h1_sock = &socket;
        const started = std.Io.Clock.awake.now(io).nanoseconds;
        outer.application_deadline_ns = @intCast(started + 2000 * std.time.ns_per_ms);
        outer.application_deadline_io = io;
        var outbound: Outbound = .{ .context = &outer };
        const sink = outbound.stream();
        var inner = httpx.Context.init(std.testing.allocator, io, &request);
        defer inner.deinit();
        installInbound(&inner, &.{}, &.{}, &sink);
        inner.application_deadline_ns = @intCast(started + 100 * std.time.ns_per_ms);
        inner.application_deadline_io = io;
        var writer = try inner.streamResponseWithContentType(200, "application/octet-stream");
        const payload = try std.testing.allocator.alloc(u8, 1024 * 1024);
        defer std.testing.allocator.free(payload);
        @memset(payload, 'x');
        try std.testing.expectError(error.Timeout, writer.write(payload));
        try std.testing.expect(std.Io.Clock.awake.now(io).nanoseconds - started < 1000 * std.time.ns_per_ms);
        try std.testing.expect(outbound.writer.?.control.ptr == null);
    }
    // A successful close updates the stored writer rather than an optional
    // payload temporary, so callbacks cannot write after the terminal frame.
    var listener = try httpx.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
    defer listener.deinit();
    var socket = try httpx.Socket.connect(listener.getLocalAddress(), io);
    defer socket.close();
    var peer = try listener.accept();
    defer peer.socket.close();
    var request = try httpx.Request.init(std.testing.allocator, .GET, "/stream");
    defer request.deinit();
    var outer = httpx.Context.init(std.testing.allocator, io, &request);
    defer outer.deinit();
    outer.h1_sock = &socket;
    var outbound: Outbound = .{ .context = &outer };
    const sink = outbound.stream();
    try std.testing.expectEqual(.ok, sink.start.?(sink.context, 200, .init("text/plain"), .{}));
    try std.testing.expectEqual(.ok, sink.close.?(sink.context));
    try std.testing.expect(outbound.writer.?.closed);
    try std.testing.expectEqual(.failed, sink.write.?(sink.context, .init("late")));
}

test "opaque HTTP controls bound lazy H2 mailbox waits before returning borrowed data" {
    const std = @import("std");
    const io = std.testing.io;
    var h2 = httpx.H2Connection.initServer(std.testing.allocator, io);
    defer h2.deinit();
    const stream = try h2.stream_manager.getOrCreateStream(1);
    stream.state = .open;
    var ready: std.Io.Event = .unset;
    const started = std.Io.Clock.awake.now(io).nanoseconds;
    var reader: httpx.Context.H2StreamReader = .{ .h2_stream = stream, .io = io, .data_event = &ready, .deadline_ms = @intCast(@divTrunc(started, std.time.ns_per_ms) + 2000) };
    var request = try httpx.Request.init(std.testing.allocator, .POST, "/upload");
    defer request.deinit();
    var outer = httpx.Context.init(std.testing.allocator, io, &request);
    defer outer.deinit();
    outer.h2_body_reader = &reader;
    outer.application_deadline_ns = @intCast(started + 2000 * std.time.ns_per_ms);
    outer.application_deadline_io = io;
    var outbound: Outbound = .{ .context = &outer };
    const source = outbound.bodySource();
    var inner = httpx.Context.init(std.testing.allocator, io, &request);
    defer inner.deinit();
    installInbound(&inner, &.{}, &source, &.{});
    inner.application_deadline_ns = @intCast(started + 100 * std.time.ns_per_ms);
    inner.application_deadline_io = io;
    try std.testing.expectError(error.Timeout, inner.body());
    try std.testing.expect(std.Io.Clock.awake.now(io).nanoseconds - started < 1000 * std.time.ns_per_ms);
    try std.testing.expect(!reader.control.active());
    try std.testing.expect(request.body == null);
}
