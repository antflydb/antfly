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

//! Per-attempt telemetry retains identity and actual provider usage only.
//! Prompts, tool arguments, media, credentials and endpoint URLs are excluded.
const std = @import("std");
const abi = @import("antfly_runtime_abi").http_abi;
const time = @import("antfly_platform").time;
const log = std.log.scoped(.inference_gateway);
const sse = @import("inference_sse.zig");

pub const Observation = struct {
    allocator: std.mem.Allocator,
    request_id: []const u8,
    requested: []const u8,
    resolved: []const u8,
    backend_model: ?[]const u8 = null,
    executor: ?[]const u8 = null,
    connection: ?[]const u8 = null,
    task: []const u8,
    attempt: u8,
    started_ns: u64,
    first_output_ns: ?u64 = null,
    status: ?u16 = null,
    failure: ?[]const u8 = null,
    input_tokens: ?u64 = null,
    output_tokens: ?u64 = null,
    total_tokens: ?u64 = null,
    downstream: abi.StreamSink = .{},
    framer: sse.Framer = .{ .limit = max_event_bytes, .overflow = .drop },
    is_sse: bool = true,
    // Reused even when allocator is a request arena, whose frees cannot be
    // relied on to reclaim a nested JSON parser's allocations per event.
    scratch: ?[]u8 = null,
    const max_event_bytes = 65536;
    const max_parse_bytes = 256 * 1024;

    pub fn deinit(self: *Observation) void {
        self.framer.deinit(self.allocator);
        if (self.scratch) |bytes| self.allocator.free(bytes);
        self.scratch = null;
    }

    pub fn finish(self: *Observation) void {
        defer self.deinit();
        log.info("invocation {f}", .{std.json.fmt(.{
            .request_id = self.request_id,
            .requested_alias = self.requested,
            .resolved_target = self.resolved,
            .backend_model = self.backend_model,
            .executor = self.executor,
            .connection = self.connection,
            .task = self.task,
            .attempt = self.attempt,
            .latency_ns = time.monotonicNs() -| self.started_ns,
            .time_to_first_output_ns = if (self.first_output_ns) |first| first -| self.started_ns else null,
            .status = self.status,
            .failure = self.failure,
            .input_tokens = self.input_tokens,
            .output_tokens = self.output_tokens,
            .total_tokens = self.total_tokens,
        }, .{})});
    }
    pub fn usage(self: *Observation, bytes: []const u8) void {
        if (bytes.len > max_event_bytes) return;
        if (self.scratch == null) self.scratch = self.allocator.alloc(u8, max_parse_bytes) catch return;
        var scratch = std.heap.FixedBufferAllocator.init(self.scratch.?);
        self.usageValue(scratch.allocator(), bytes);
    }
    fn usageValue(self: *Observation, a: std.mem.Allocator, bytes: []const u8) void {
        const parsed = std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{}) catch return;
        if (parsed != .object) return;
        const value = parsed.object.get("usage") orelse return;
        if (value != .object) return;
        self.input_tokens = count(value, "input_tokens") orelse count(value, "prompt_tokens") orelse self.input_tokens;
        self.output_tokens = count(value, "output_tokens") orelse count(value, "completion_tokens") orelse self.output_tokens;
        self.total_tokens = count(value, "total_tokens") orelse self.total_tokens;
    }
    fn count(value: std.json.Value, key: []const u8) ?u64 {
        const number = value.object.get(key) orelse return null;
        return if (number == .integer and number.integer >= 0) @intCast(number.integer) else null;
    }
    pub fn stream(self: *Observation, downstream: abi.StreamSink) abi.StreamSink {
        self.downstream = downstream;
        return .{ .context = self, .start = start, .write = write, .close = close };
    }
    fn start(raw: ?*anyopaque, status: u16, content_type: abi.Bytes, headers: abi.HeaderList) callconv(.c) abi.CallbackStatus {
        const self: *Observation = @ptrCast(@alignCast(raw.?));
        self.is_sse = sse.isEventStream(content_type.slice());
        self.status = status;
        return (self.downstream.start orelse return .failed)(self.downstream.context, status, content_type, headers);
    }
    fn write(raw: ?*anyopaque, bytes: abi.Bytes) callconv(.c) abi.CallbackStatus {
        const self: *Observation = @ptrCast(@alignCast(raw.?));
        if (bytes.len > 0 and self.first_output_ns == null) self.first_output_ns = time.monotonicNs();
        if (self.is_sse) for (bytes.slice()) |byte| {
            // Observation may drop an oversized or allocation-heavy event but
            // must forward the original transport bytes without interruption.
            if (self.framer.push(self.allocator, byte) catch null) |event| {
                if (self.scratch == null) self.scratch = self.allocator.alloc(u8, max_parse_bytes) catch continue;
                var scratch = std.heap.FixedBufferAllocator.init(self.scratch.?);
                const data = (sse.dataAlloc(scratch.allocator(), event) catch continue) orelse continue;
                self.usageValue(scratch.allocator(), data);
            }
        };
        return (self.downstream.write orelse return .failed)(self.downstream.context, bytes);
    }
    fn close(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
        const self: *Observation = @ptrCast(@alignCast(raw.?));
        self.framer.finish();
        return (self.downstream.close orelse return .failed)(self.downstream.context);
    }
};

test "provider registry gateway telemetry parses fragmented actual usage without estimating" {
    const a = std.testing.allocator;
    var observation = Observation{
        .allocator = a,
        .request_id = "request",
        .requested = "chains/example",
        .resolved = "generators/example",
        .task = "generate",
        .attempt = 1,
        .started_ns = time.monotonicNs(),
    };
    defer observation.deinit();
    const Discard = struct {
        fn write(_: ?*anyopaque, _: abi.Bytes) callconv(.c) abi.CallbackStatus {
            return .ok;
        }
    };
    const sink = observation.stream(.{ .write = Discard.write });
    try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")));
    try std.testing.expect(observation.first_output_ns != null);
    try std.testing.expectEqual(@as(?u64, null), observation.output_tokens);
    _ = sink.write.?(sink.context, .init("data: {\"usage\":{\"prompt_tokens\":2,"));
    try std.testing.expectEqual(@as(?u64, null), observation.input_tokens);
    _ = sink.write.?(sink.context, .init("\"completion_tokens\":1,\"total_tokens\":3}}\n\n"));
    try std.testing.expectEqual(@as(?u64, 2), observation.input_tokens);
    try std.testing.expectEqual(@as(?u64, 1), observation.output_tokens);
    try std.testing.expectEqual(@as(?u64, 3), observation.total_tokens);
    observation.usage("{\"usage\":{\"input_tokens\":-1},\"text\":\"long text cannot invent usage\"}");
    try std.testing.expectEqual(@as(?u64, 2), observation.input_tokens);
}

test "provider registry gateway telemetry storage stays bounded across long streams" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var observation = Observation{
        .allocator = arena.allocator(),
        .request_id = "request",
        .requested = "model",
        .resolved = "model",
        .task = "generate",
        .attempt = 1,
        .started_ns = time.monotonicNs(),
    };
    defer observation.deinit();
    const Capture = struct {
        bytes: usize = 0,
        fn write(raw: ?*anyopaque, bytes: abi.Bytes) callconv(.c) abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.bytes += bytes.len;
            return .ok;
        }
    };
    var capture: Capture = .{};
    const sink = observation.stream(.{ .context = &capture, .write = Capture.write });
    const event = "data: {\"usage\":{\"input_tokens\":2,\"total_tokens\":3}}\n\n";
    _ = sink.write.?(sink.context, .init(event));
    const warm_capacity = arena.queryCapacity();
    for (0..10000) |_| try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init(event)));
    try std.testing.expectEqual(warm_capacity, arena.queryCapacity());
    try std.testing.expectEqual(@as(usize, 10001 * event.len), capture.bytes);
    try std.testing.expectEqual(@as(?u64, 3), observation.total_tokens);
    // Valid JSON with many small values exceeds parser storage despite fitting
    // the event limit. Telemetry exhaustion must never break the stream.
    const heavy = try std.testing.allocator.alloc(u8, 60002);
    defer std.testing.allocator.free(heavy);
    heavy[0] = '[';
    for (0..30000) |i| {
        heavy[1 + i * 2] = '0';
        heavy[2 + i * 2] = ',';
    }
    heavy[heavy.len - 2] = ']';
    heavy[heavy.len - 1] = '\n';
    observation.usage(heavy);
    const before = capture.bytes;
    try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init(heavy)));
    try std.testing.expectEqual(before + heavy.len, capture.bytes);
    try std.testing.expectEqual(@as(?u64, 3), observation.total_tokens);
}

test "provider registry gateway telemetry shares SSE event framing across line endings" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "\n", "\r\n", "\r" }) |newline| {
        var observation: Observation = .{ .allocator = a, .request_id = "r", .requested = "m", .resolved = "m", .task = "generate", .attempt = 1, .started_ns = time.monotonicNs() };
        defer observation.deinit();
        const Capture = struct {
            output: std.ArrayList(u8) = .empty,
            fn write(raw: ?*anyopaque, bytes: abi.Bytes) callconv(.c) abi.CallbackStatus {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                self.output.appendSlice(std.testing.allocator, bytes.slice()) catch return .failed;
                return .ok;
            }
        };
        var capture: Capture = .{};
        defer capture.output.deinit(a);
        const sink = observation.stream(.{ .context = &capture, .write = Capture.write });
        const event = try std.mem.replaceOwned(u8, a, "\xef\xbb\xbf: heartbeat\nid: cursor\ndata: {\"choices\":[],\ndata: \"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":5}}\n\ndata: [DONE]\n\n", "\n", newline);
        defer a.free(event);
        for (event, 0..) |_, index| try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init(event[index .. index + 1])));
        try std.testing.expectEqualStrings(event, capture.output.items);
        try std.testing.expectEqual(@as(?u64, 2), observation.input_tokens);
        try std.testing.expectEqual(@as(?u64, 1), observation.output_tokens);
        try std.testing.expectEqual(@as(?u64, 5), observation.total_tokens);
    }
}

test "provider registry gateway telemetry drops whole oversized events and resumes after delimiter" {
    const a = std.testing.allocator;
    var observation: Observation = .{ .allocator = a, .request_id = "r", .requested = "m", .resolved = "m", .task = "generate", .attempt = 1, .started_ns = time.monotonicNs() };
    defer observation.deinit();
    const Discard = struct {
        fn write(_: ?*anyopaque, _: abi.Bytes) callconv(.c) abi.CallbackStatus {
            return .ok;
        }
        fn start(_: ?*anyopaque, _: u16, _: abi.Bytes, _: abi.HeaderList) callconv(.c) abi.CallbackStatus {
            return .ok;
        }
    };
    const sink = observation.stream(.{ .write = Discard.write, .start = Discard.start });
    for (0..Observation.max_event_bytes / 8 + 1) |_| try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init(": short\n")));
    const event = "data: {\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":3}}\n\n";
    try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init(event)));
    try std.testing.expectEqual(@as(?u64, null), observation.total_tokens);
    try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init(event)));
    try std.testing.expectEqual(@as(?u64, 3), observation.total_tokens);
    observation.total_tokens = null;
    try std.testing.expectEqual(abi.CallbackStatus.ok, sink.start.?(sink.context, 200, .init("audio/mpeg"), .{ .ptr = null, .len = 0 }));
    try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init(event)));
    try std.testing.expectEqual(@as(?u64, null), observation.total_tokens);
}
