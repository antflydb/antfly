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
const stream = @import("bedrock_stream.zig");
const inference = @import("antfly_inference_types");

fn frame(a: std.mem.Allocator, event: []const u8, payload: []const u8, kind: []const u8) ![]u8 {
    var headers: std.ArrayList(u8) = .empty;
    defer headers.deinit(a);
    for ([_][2][]const u8{ .{ ":message-type", kind }, .{ ":event-type", event } }) |pair| {
        try headers.append(a, @intCast(pair[0].len));
        try headers.appendSlice(a, pair[0]);
        try headers.append(a, 7);
        var length: [2]u8 = undefined;
        std.mem.writeInt(u16, &length, @intCast(pair[1].len), .big);
        try headers.appendSlice(a, &length);
        try headers.appendSlice(a, pair[1]);
    }
    const result = try a.alloc(u8, 16 + headers.items.len + payload.len);
    std.mem.writeInt(u32, result[0..4], @intCast(result.len), .big);
    std.mem.writeInt(u32, result[4..8], @intCast(headers.items.len), .big);
    std.mem.writeInt(u32, result[8..12], @import("antfly_hash").Crc32.hash(result[0..8]), .big);
    @memcpy(result[12..][0..headers.items.len], headers.items);
    @memcpy(result[12 + headers.items.len ..][0..payload.len], payload);
    std.mem.writeInt(u32, result[result.len - 4 ..][0..4], @import("antfly_hash").Crc32.hash(result[0 .. result.len - 4]), .big);
    return result;
}

const Sink = struct {
    output: std.ArrayList(u8) = .empty,
    starts: usize = 0,
    bytes: usize = 0,
    closed: bool = false,
    discard: bool = false,
    cancel: bool = false,

    fn view(self: *Sink) inference.ChatStream {
        return .{ .context = self, .start = start, .write = write, .close = close };
    }
    fn start(raw: *anyopaque, status: u16, content_type: []const u8, _: ?[]const u8, _: ?[]const u8) !void {
        const self: *Sink = @ptrCast(@alignCast(raw));
        try std.testing.expectEqual(@as(u16, 200), status);
        try std.testing.expectEqualStrings("text/event-stream", content_type);
        self.starts += 1;
    }
    fn write(raw: *anyopaque, bytes: []const u8) !void {
        const self: *Sink = @ptrCast(@alignCast(raw));
        if (self.cancel) return error.Canceled;
        self.bytes += bytes.len;
        if (!self.discard) try self.output.appendSlice(std.testing.allocator, bytes);
    }
    fn close(raw: *anyopaque) !void {
        const self: *Sink = @ptrCast(@alignCast(raw));
        self.closed = true;
    }
};

pub fn testDecoder() !void {
    const a = std.testing.allocator;
    const events = [_][2][]const u8{
        .{ "messageStart", "{\"role\":\"assistant\"}" },
        .{ "contentBlockDelta", "{\"contentBlockIndex\":0,\"delta\":{\"text\":\"sheets\"}}" },
        .{ "contentBlockStop", "{\"contentBlockIndex\":0}" },
        .{ "contentBlockStart", "{\"contentBlockIndex\":1,\"start\":{\"toolUse\":{\"toolUseId\":\"call-1\",\"name\":\"lookup\"}}}" },
        .{ "contentBlockDelta", "{\"contentBlockIndex\":1,\"delta\":{\"toolUse\":{\"input\":\"{\\\"x\\\":\"}}}" },
        .{ "contentBlockDelta", "{\"contentBlockIndex\":1,\"delta\":{\"toolUse\":{\"input\":\"7}\"}}}" },
        .{ "contentBlockStop", "{\"contentBlockIndex\":1}" },
        .{ "contentBlockDelta", "{\"contentBlockIndex\":2,\"delta\":{\"reasoningContent\":{\"text\":\"private\"}}}" },
        .{ "contentBlockDelta", "{\"contentBlockIndex\":2,\"delta\":{\"reasoningContent\":{\"signature\":\"signed\"}}}" },
        .{ "contentBlockStop", "{\"contentBlockIndex\":2}" },
        .{ "contentBlockDelta", "{\"contentBlockIndex\":3,\"delta\":{\"reasoningContent\":{\"redactedContent\":\"cmVkYWN0ZWQ=\"}}}" },
        .{ "contentBlockStop", "{\"contentBlockIndex\":3}" },
        .{ "messageStop", "{\"stopReason\":\"tool_use\"}" },
        .{ "metadata", "{\"usage\":{\"inputTokens\":10,\"outputTokens\":2,\"totalTokens\":12}}" },
    };
    var sink: Sink = .{};
    defer sink.output.deinit(a);
    var decoder = stream.Decoder{ .alloc = a, .sink = sink.view(), .model = "unlisted-vision-model", .include_usage = true };
    defer decoder.deinit();
    for (events) |event| {
        const bytes = try frame(a, event[0], event[1], "event");
        defer a.free(bytes);
        for (bytes) |byte| try decoder.push(&.{byte});
    }
    try decoder.finish();
    try std.testing.expectEqual(@as(usize, 1), sink.starts);
    try std.testing.expect(sink.closed);
    try std.testing.expect(std.mem.endsWith(u8, sink.output.items, "data: [DONE]\n\n"));
    var arguments: std.ArrayList(u8) = .empty;
    defer arguments.deinit(a);
    var chunks = std.mem.splitSequence(u8, sink.output.items, "\n\n");
    while (chunks.next()) |chunk| {
        if (chunk.len == 0 or std.mem.eql(u8, chunk, "data: [DONE]")) continue;
        var parsed = try std.json.parseFromSlice(std.json.Value, a, chunk["data: ".len..], .{});
        defer parsed.deinit();
        const choices = parsed.value.object.get("choices").?.array.items;
        if (choices.len == 0) {
            try std.testing.expectEqual(@as(i64, 10), parsed.value.object.get("usage").?.object.get("prompt_tokens").?.integer);
            continue;
        }
        if (choices[0].object.get("delta").?.object.get("tool_calls")) |calls| {
            const call = calls.array.items[0].object;
            try std.testing.expectEqual(@as(i64, 0), call.get("index").?.integer);
            if (call.get("id")) |id| try std.testing.expectEqualStrings("call-1", id.string);
            try arguments.appendSlice(a, call.get("function").?.object.get("arguments").?.string);
        }
    }
    try std.testing.expectEqualStrings("{\"x\":7}", arguments.items);
    try std.testing.expect(std.mem.indexOf(u8, sink.output.items, "\"signature\":\"signed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, sink.output.items, "\"redactedContent\":\"cmVkYWN0ZWQ=\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, sink.output.items, "\"event\":{\"contentBlockStop\":{\"contentBlockIndex\":3}}") != null);

    const start = try frame(a, events[0][0], events[0][1], "event");
    defer a.free(start);
    for (0..3) |mode| {
        var rejected: Sink = .{};
        defer rejected.output.deinit(a);
        var invalid = stream.Decoder{ .alloc = a, .sink = rejected.view(), .model = "m", .frame_limit = if (mode == 2) 16 else 4096 };
        defer invalid.deinit();
        if (mode == 0) {
            const corrupt = try a.dupe(u8, start);
            defer a.free(corrupt);
            corrupt[corrupt.len - 1] ^= 1;
            try std.testing.expectError(error.InvalidInferenceResponse, invalid.push(corrupt));
        } else if (mode == 1) {
            try invalid.push(start[0..11]);
            try std.testing.expectError(error.InvalidInferenceResponse, invalid.finish());
        } else try std.testing.expectError(error.InferenceResponseTooLarge, invalid.push(start[0..12]));
        try std.testing.expectEqual(@as(usize, 0), rejected.starts);
        try std.testing.expect(!rejected.closed);
    }
    // Only the documented transient exceptions permit provider fallback.
    for ([_]struct { tag: []const u8, failure: anyerror }{
        .{ .tag = "validationException", .failure = error.InvalidInferenceRequest },
        .{ .tag = "throttlingException", .failure = error.InferenceUpstreamRateLimited },
        .{ .tag = "internalServerException", .failure = error.InferenceUpstreamUnavailable },
        .{ .tag = "modelStreamErrorException", .failure = error.InferenceUpstreamUnavailable },
        .{ .tag = "serviceUnavailableException", .failure = error.InferenceUpstreamUnavailable },
        .{ .tag = "unknownFutureException", .failure = error.InvalidInferenceResponse },
    }) |case| {
        var rejected: Sink = .{};
        defer rejected.output.deinit(a);
        var invalid = stream.Decoder{ .alloc = a, .sink = rejected.view(), .model = "m" };
        defer invalid.deinit();
        const bytes = try frame(a, case.tag, "{\"message\":\"failed\"}", "exception");
        defer a.free(bytes);
        try std.testing.expectError(case.failure, invalid.push(bytes));
        try std.testing.expectEqual(@as(usize, 0), rejected.starts);
        try std.testing.expect(!rejected.closed);
    }
    var interrupted: Sink = .{ .cancel = true };
    defer interrupted.output.deinit(a);
    var canceled = stream.Decoder{ .alloc = a, .sink = interrupted.view(), .model = "m" };
    defer canceled.deinit();
    try std.testing.expectError(error.Canceled, canceled.push(start));
    try std.testing.expect(!interrupted.closed);

    var discarded: Sink = .{ .discard = true };
    defer discarded.output.deinit(a);
    var bounded = stream.Decoder{ .alloc = a, .sink = discarded.view(), .model = "m" };
    defer bounded.deinit();
    try bounded.push(start);
    const delta = try frame(a, events[1][0], events[1][1], "event");
    defer a.free(delta);
    for (0..1000) |_| try bounded.push(delta);
    try std.testing.expect(bounded.pending.capacity < 4096);
    try std.testing.expect(discarded.bytes > 100_000);
    try std.testing.expectError(error.InvalidInferenceResponse, bounded.finish());
    try std.testing.expect(!discarded.closed);
}
