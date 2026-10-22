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

//! Incremental HTTP response adapter. Headers commit before the first body
//! byte, including error responses. Callers must disable transport retries.
const std = @import("std");
const httpx = @import("httpx");
const abi = @import("antfly_runtime_abi").http_abi;
const sse = @import("inference_sse.zig");

pub const Writer = struct {
    sink: abi.StreamSink,
    started: bool = false,

    fn check(status: abi.CallbackStatus) !void {
        return switch (status) {
            .ok => {},
            .canceled => error.Canceled,
            else => error.InferenceStreamFailed,
        };
    }
    pub fn startResponse(self: *Writer, response: httpx.Response) !void {
        if (self.started) return error.ResponseAlreadyCommitted;
        const start = self.sink.start orelse return error.InferenceStreamUnavailable;
        // Only forward public retry/request metadata; transport framing and
        // upstream cookies never cross the inference boundary.
        var headers: [3]abi.HeaderView = undefined;
        var count: usize = 0;
        inline for (.{ "retry-after", "x-request-id", "request-id" }) |name| {
            if (response.header(name)) |value| {
                headers[count] = .{ .name = .init(if (@import("std").mem.eql(u8, name, "x-request-id") or @import("std").mem.eql(u8, name, "request-id")) "x-antfly-upstream-request-id" else name), .value = .init(value) };
                count += 1;
            }
        }
        try check(start(self.sink.context, response.status.code, .init(response.contentType() orelse "application/json"), .{ .ptr = &headers, .len = count }));
        self.started = true;
    }
    pub fn writeAll(self: *Writer, bytes: []const u8) !void {
        if (!self.started) return error.InferenceStreamUnavailable;
        const write = self.sink.write orelse return error.InferenceStreamUnavailable;
        try check(write(self.sink.context, .init(bytes)));
    }
    pub fn close(self: *Writer) !void {
        const finish = self.sink.close orelse return error.InferenceStreamUnavailable;
        try check(finish(self.sink.context));
    }
};

/// A chain may discard transient headers only before the public sink commits.
/// All other responses, including refusals and authentication failures, commit
/// immediately and can never select another candidate.
pub const FallbackSink = struct {
    downstream: abi.StreamSink,
    failure: ?anyerror = null,
    committed: bool = false,

    pub fn sink(self: *FallbackSink) abi.StreamSink {
        return .{ .context = self, .start = start, .write = write, .close = close };
    }
    fn start(raw: ?*anyopaque, status: u16, content_type: abi.Bytes, headers: abi.HeaderList) callconv(.c) abi.CallbackStatus {
        const self: *FallbackSink = @ptrCast(@alignCast(raw.?));
        if (@import("inference_gateway.zig").transientStatus(status)) |failure| {
            self.failure = failure;
            return .failed;
        }
        // Mark committed before invoking the transport: a failed write may
        // already have delivered headers to the client.
        self.committed = true;
        return (self.downstream.start orelse return .failed)(self.downstream.context, status, content_type, headers);
    }
    fn write(raw: ?*anyopaque, bytes: abi.Bytes) callconv(.c) abi.CallbackStatus {
        const self: *FallbackSink = @ptrCast(@alignCast(raw.?));
        return (self.downstream.write orelse return .failed)(self.downstream.context, bytes);
    }
    fn close(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
        const self: *FallbackSink = @ptrCast(@alignCast(raw.?));
        return (self.downstream.close orelse return .failed)(self.downstream.context);
    }
};

/// Session handles are rewritten only after assembling a complete SSE event.
/// The one MiB bound covers all fields in an event, including metadata. CR,
/// LF and CRLF are normalized before parsing, independent of transport chunks.
pub const SessionSink = struct {
    allocator: std.mem.Allocator,
    public_id: []const u8,
    downstream: abi.StreamSink,
    framer: sse.Framer = .{ .limit = max_event_bytes },
    scratch: ?std.heap.ArenaAllocator = null,
    is_sse: bool = false,
    const max_event_bytes = 1 << 20;

    pub fn deinit(self: *SessionSink) void {
        self.framer.deinit(self.allocator);
        if (self.scratch) |*arena| arena.deinit();
    }
    pub fn sink(self: *SessionSink) abi.StreamSink {
        return .{ .context = self, .start = start, .write = write, .close = close };
    }
    fn start(raw: ?*anyopaque, status: u16, content_type: abi.Bytes, headers: abi.HeaderList) callconv(.c) abi.CallbackStatus {
        const self: *SessionSink = @ptrCast(@alignCast(raw.?));
        self.is_sse = sse.isEventStream(content_type.slice());
        return (self.downstream.start orelse return .failed)(self.downstream.context, status, content_type, headers);
    }
    fn event(self: *SessionSink, original: []const u8) !abi.CallbackStatus {
        const send = self.downstream.write orelse return .failed;
        // Retain and reuse parser storage even when the backing allocator is
        // a request arena, where per-event frees would not reclaim anything.
        if (self.scratch == null) self.scratch = std.heap.ArenaAllocator.init(self.allocator);
        const arena = &self.scratch.?;
        defer _ = arena.reset(.retain_capacity);
        const a = arena.allocator();
        const payload = (try sse.dataAlloc(a, original)) orelse return send(self.downstream.context, .init(original));
        if (payload.len == 0 or std.mem.eql(u8, payload, "[DONE]"))
            return send(self.downstream.context, .init(original));
        const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, payload, .{});
        if (parsed != .object or !parsed.object.contains("session_id"))
            return send(self.downstream.context, .init(original));
        var value = parsed;
        try value.object.put(a, "session_id", .{ .string = self.public_id });
        const encoded = try std.json.Stringify.valueAlloc(a, value, .{});
        var output: std.ArrayList(u8) = .empty;
        var lines = std.mem.splitScalar(u8, original, '\n');
        var replaced = false;
        // splitScalar yields a trailing empty token; original already has the
        // terminating blank line, so do not append an additional delimiter.
        var remaining = original.len;
        while (lines.next()) |line| {
            if (remaining == 0) break;
            remaining -= line.len + 1;
            if (sse.dataValue(line) != null) {
                if (replaced) continue;
                try output.appendSlice(a, "data: ");
                try output.appendSlice(a, encoded);
                replaced = true;
            } else try output.appendSlice(a, line);
            try output.append(a, '\n');
        }
        return send(self.downstream.context, .init(output.items));
    }
    fn write(raw: ?*anyopaque, bytes: abi.Bytes) callconv(.c) abi.CallbackStatus {
        const self: *SessionSink = @ptrCast(@alignCast(raw.?));
        if (!self.is_sse) return (self.downstream.write orelse return .failed)(self.downstream.context, bytes);
        for (bytes.slice()) |byte| {
            const complete = self.framer.push(self.allocator, byte) catch |err| return if (err == error.SSEEventTooLarge) .body_too_large else .failed;
            if (complete) |original| {
                const status = self.event(original) catch return .failed;
                if (status != .ok) return status;
            }
        }
        return .ok;
    }
    fn close(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
        const self: *SessionSink = @ptrCast(@alignCast(raw.?));
        // SSE dispatch requires a blank line. EOF discards an incomplete event,
        // rather than exposing an unvalidated backend handle to the caller.
        self.framer.finish();
        return (self.downstream.close orelse return .failed)(self.downstream.context);
    }
};

const SessionTestOutput = struct {
    bytes: std.ArrayList(u8) = .empty,
    closed: bool = false,
    fn sink(self: *@This()) abi.StreamSink {
        return .{ .context = self, .start = start, .write = write, .close = close };
    }
    fn start(_: ?*anyopaque, _: u16, _: abi.Bytes, _: abi.HeaderList) callconv(.c) abi.CallbackStatus {
        return .ok;
    }
    fn write(raw: ?*anyopaque, bytes: abi.Bytes) callconv(.c) abi.CallbackStatus {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.bytes.appendSlice(std.testing.allocator, bytes.slice()) catch return .failed;
        return .ok;
    }
    fn close(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.closed = true;
        return .ok;
    }
};

test "provider registry gateway session SSE framing preserves metadata across fragmented multiline events" {
    const a = std.testing.allocator;
    const input = "\xef\xbb\xbf: heartbeat\nid: cursor\nevent: session.open\nretry: 500\nx-extension: keep\ndata: {\"type\":\"session.open\",\ndata: \"session_id\":\"private\",\"extension\":true}\n\ndata\n\ndata: [DONE]\n\ndata: {\"session_id\":\"discard-private\"}\n";
    const expected = ": heartbeat\nid: cursor\nevent: session.open\nretry: 500\nx-extension: keep\ndata: {\"type\":\"session.open\",\"session_id\":\"public\",\"extension\":true}\n\ndata\n\ndata: [DONE]\n\n";
    for ([_][]const u8{ "\n", "\r\n", "\r" }) |newline| {
        const framed = try std.mem.replaceOwned(u8, a, input, "\n", newline);
        defer a.free(framed);
        for ([_]usize{ 1, 3, input.len * 2 }) |chunk_size| {
            var output: SessionTestOutput = .{};
            defer output.bytes.deinit(a);
            var session: SessionSink = .{ .allocator = a, .public_id = "public", .downstream = output.sink() };
            defer session.deinit();
            const sink = session.sink();
            try std.testing.expectEqual(abi.CallbackStatus.ok, sink.start.?(sink.context, 200, .init("Text/Event-Stream; charset=utf-8"), .{ .ptr = null, .len = 0 }));
            var offset: usize = 0;
            while (offset < framed.len) {
                const end = @min(offset + chunk_size, framed.len);
                try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init(framed[offset..end])));
                offset = end;
            }
            try std.testing.expectEqual(abi.CallbackStatus.ok, sink.close.?(sink.context));
            try std.testing.expect(output.closed);
            try std.testing.expectEqualStrings(expected, output.bytes.items);
        }
    }
}

test "provider registry gateway session SSE limits whole events and passes non-SSE errors through" {
    const a = std.testing.allocator;
    var output: SessionTestOutput = .{};
    defer output.bytes.deinit(a);
    var session: SessionSink = .{ .allocator = a, .public_id = "public", .downstream = output.sink() };
    defer session.deinit();
    const sink = session.sink();
    try std.testing.expectEqual(abi.CallbackStatus.ok, sink.start.?(sink.context, 500, .init("application/json"), .{ .ptr = null, .len = 0 }));
    try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init("{\"error\":\"backend failed\"}")));
    try std.testing.expectEqualStrings("{\"error\":\"backend failed\"}", output.bytes.items);
    output.bytes.clearRetainingCapacity();
    try std.testing.expectEqual(abi.CallbackStatus.ok, sink.start.?(sink.context, 200, .init("text/event-stream"), .{ .ptr = null, .len = 0 }));
    // Individually short lines must not bypass the event size bound.
    for (0..SessionSink.max_event_bytes / 8) |_| {
        try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init(": short\n")));
    }
    try std.testing.expectEqual(abi.CallbackStatus.body_too_large, sink.write.?(sink.context, .init("\n")));
    try std.testing.expectEqual(@as(usize, 0), output.bytes.items.len);
    var invalid: SessionSink = .{ .allocator = a, .public_id = "public", .downstream = output.sink() };
    defer invalid.deinit();
    const invalid_sink = invalid.sink();
    try std.testing.expectEqual(abi.CallbackStatus.ok, invalid_sink.start.?(invalid_sink.context, 200, .init("text/event-stream"), .{ .ptr = null, .len = 0 }));
    try std.testing.expectEqual(abi.CallbackStatus.failed, invalid_sink.write.?(invalid_sink.context, .init("data: {\"session_id\":\"private\",\ndata: invalid}\n\n")));
    try std.testing.expectEqual(@as(usize, 0), output.bytes.items.len);
}

test "provider registry gateway session SSE parser storage stays bounded on request arenas" {
    var backing = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer backing.deinit();
    var output: SessionTestOutput = .{};
    defer output.bytes.deinit(std.testing.allocator);
    var session: SessionSink = .{ .allocator = backing.allocator(), .public_id = "public", .downstream = output.sink() };
    defer session.deinit();
    const sink = session.sink();
    try std.testing.expectEqual(abi.CallbackStatus.ok, sink.start.?(sink.context, 200, .init("text/event-stream"), .{ .ptr = null, .len = 0 }));
    const event_bytes = "data: {\"session_id\":\"private\",\n" ++ "data: \"text\":\"hello\"}\n\n";
    for (0..10) |_| {
        try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init(event_bytes)));
        output.bytes.clearRetainingCapacity();
    }
    const warm_capacity = backing.queryCapacity();
    for (0..10000) |_| {
        try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init(event_bytes)));
        output.bytes.clearRetainingCapacity();
    }
    try std.testing.expectEqual(warm_capacity, backing.queryCapacity());
}
