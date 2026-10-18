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
const httpx = @import("httpx");
const abi = @import("antfly_runtime_abi").http_abi;

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

/// Session streams keep the public handle stable across a pinned transport.
/// One bounded SSE line is rewritten at a time; the stream is never buffered.
pub const SessionSink = struct {
    allocator: @import("std").mem.Allocator,
    public_id: []const u8,
    downstream: abi.StreamSink,
    pending: @import("std").ArrayList(u8) = .empty,
    pub fn deinit(self: *SessionSink) void {
        self.pending.deinit(self.allocator);
    }
    pub fn sink(self: *SessionSink) abi.StreamSink {
        return .{ .context = self, .start = start, .write = write, .close = close };
    }
    fn start(raw: ?*anyopaque, status: u16, content_type: abi.Bytes, headers: abi.HeaderList) callconv(.c) abi.CallbackStatus {
        const self: *SessionSink = @ptrCast(@alignCast(raw.?));
        return (self.downstream.start orelse return .failed)(self.downstream.context, status, content_type, headers);
    }
    fn line(self: *SessionSink) !abi.CallbackStatus {
        const std = @import("std");
        const original = self.pending.items;
        const send = self.downstream.write orelse return .failed;
        if (std.mem.startsWith(u8, original, "data:")) {
            const payload = std.mem.trim(u8, original[5..], " \r\n");
            if (!std.mem.eql(u8, payload, "[DONE]")) {
                var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, payload, .{});
                defer parsed.deinit();
                if (parsed.value == .object and parsed.value.object.contains("session_id")) {
                    try parsed.value.object.put(parsed.arena.allocator(), "session_id", .{ .string = self.public_id });
                    const encoded = try std.json.Stringify.valueAlloc(self.allocator, parsed.value, .{});
                    defer self.allocator.free(encoded);
                    const prefix = send(self.downstream.context, .init("data: "));
                    if (prefix != .ok) return prefix;
                    const data = send(self.downstream.context, .init(encoded));
                    if (data != .ok) return data;
                    return send(self.downstream.context, .init("\n"));
                }
            }
        }
        return send(self.downstream.context, .init(original));
    }
    fn write(raw: ?*anyopaque, bytes: abi.Bytes) callconv(.c) abi.CallbackStatus {
        const self: *SessionSink = @ptrCast(@alignCast(raw.?));
        for (bytes.slice()) |byte| {
            if (self.pending.items.len >= 1 << 20) return .body_too_large;
            self.pending.append(self.allocator, byte) catch return .failed;
            if (byte == '\n') {
                const status = self.line() catch return .failed;
                self.pending.clearRetainingCapacity();
                if (status != .ok) return status;
            }
        }
        return .ok;
    }
    fn close(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
        const self: *SessionSink = @ptrCast(@alignCast(raw.?));
        if (self.pending.items.len > 0) {
            const status = self.line() catch return .failed;
            if (status != .ok) return status;
        }
        return (self.downstream.close orelse return .failed)(self.downstream.context);
    }
};
