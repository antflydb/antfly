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
const wire = @import("anthropic.zig");
const lib = @import("antfly_generating");
const Decoder = @import("anthropic_stream.zig").Decoder;

pub fn run() !void {
    const a = std.testing.allocator;
    var result = try wire.responseAlloc(a,
        \\{"role":"assistant","content":[{"type":"thinking","thinking":"private","signature":"signed-value"},{"type":"tool_use","id":"call-1","name":"lookup","input":{"id":7}}],"stop_reason":"tool_use","usage":{"input_tokens":2,"cache_read_input_tokens":5,"output_tokens":3}}
    );
    defer result.deinit();
    try std.testing.expectEqual(lib.GenerationFinishReason.tool_calls, result.finish_reason.?);
    try std.testing.expectEqual(@as(u64, 7), result.usage.?.input_tokens);
    const cfg = lib.GeneratorConfig{ .provider = .anthropic, .url = "", .model = "future-claude", .response_format_json = "{\"type\":\"json_schema\",\"json_schema\":{\"schema\":{\"type\":\"object\"}}}" };
    const messages = [_]lib.ChatMessage{
        .{ .role = .system, .content = .{ .text = "Use the image" } },
        .{ .role = .user, .content = .{ .parts = &.{.{ .image_url = .{ .url = "data:image/png;base64,aW1hZ2U=" } }} } },
        .{ .role = .assistant, .content = .{ .text = "" }, .tool_calls = result.tool_calls, .anthropic_content_json = result.anthropic_content_json },
        .{ .role = .tool, .tool_call_id = "call-1", .content = .{ .text = "found" } },
    };
    const body = try wire.requestJsonAlloc(a, cfg, cfg.model, &messages, false);
    defer a.free(body);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, body, .{});
    defer parsed.deinit();
    const turns = parsed.value.object.get("messages").?.array.items;
    const image = turns[0].object.get("content").?.array.items[0].object.get("source").?.object;
    try std.testing.expectEqualStrings("image/png", image.get("media_type").?.string);
    try std.testing.expectEqualStrings("aW1hZ2U=", image.get("data").?.string);
    try std.testing.expectEqualStrings("signed-value", turns[1].object.get("content").?.array.items[0].object.get("signature").?.string);
    try std.testing.expectEqualStrings("tool_result", turns[2].object.get("content").?.array.items[0].object.get("type").?.string);
    try std.testing.expectEqualStrings("json_schema", parsed.value.object.get("output_config").?.object.get("format").?.object.get("type").?.string);
    var changed = messages[2];
    changed.tool_calls = &.{};
    try std.testing.expectError(error.InvalidInferenceRequest, wire.requestJsonAlloc(a, cfg, cfg.model, &.{changed}, false));
    changed.anthropic_content_json = "[{\"type\":\"image\",\"source\":{\"type\":\"url\",\"url\":\"https://image\"}}]";
    try std.testing.expectError(error.InvalidInferenceRequest, wire.requestJsonAlloc(a, cfg, cfg.model, &.{changed}, false));

    var sink: Sink = .{};
    defer sink.output.deinit(a);
    var decoder = Decoder{ .alloc = a, .sink = sink.view(), .model = cfg.model, .id = "stream-test", .include_usage = true };
    defer decoder.deinit();
    const events = [_][]const u8{
        \\{"type":"message_start","message":{"role":"assistant","usage":{"input_tokens":2,"output_tokens":0,"cache_read_input_tokens":5}}}
        ,
        \\{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}
        ,
        \\{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signature"}}
        ,
        \\{"type":"content_block_stop","index":0}
        ,
        \\{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"call","name":"lookup","input":{}}}
        ,
        \\{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"id\":"}}
        ,
        \\{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"7}"}}
        ,
        \\{"type":"content_block_stop","index":1}
        ,
        \\{"type":"content_block_start","index":2,"content_block":{"type":"text","text":"visible"}}
        ,
        \\{"type":"content_block_stop","index":2}
        ,
        \\{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":3}}
        ,
        \\{"type":"message_stop"}
        ,
    };
    for (events) |event| {
        const framed = try std.fmt.allocPrint(a, "data: {s}\r\n\r\n", .{event});
        defer a.free(framed);
        for (framed) |byte| try decoder.push(&.{byte});
    }
    try decoder.finish();
    try std.testing.expect(sink.closed);
    try std.testing.expectEqual(@as(u64, 10), decoder.usage.?.total_tokens.?);
    try std.testing.expect(std.mem.indexOf(u8, sink.output.items, "signature_delta") != null);
    try std.testing.expect(std.mem.endsWith(u8, sink.output.items, "data: [DONE]\n\n"));
    try std.testing.expect(std.mem.indexOf(u8, sink.output.items, "\"content_block_index\":2") != null);
    try std.testing.expect(decoder.framer.pending.capacity < 4096);
    var rejected: Sink = .{};
    defer rejected.output.deinit(a);
    var invalid = Decoder{ .alloc = a, .sink = rejected.view(), .model = cfg.model, .id = "invalid" };
    defer invalid.deinit();
    try std.testing.expectError(error.InvalidInferenceRequest, invalid.push("data: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\"}}\n\n"));
    try std.testing.expect(!invalid.started);
    try std.testing.expectError(error.InvalidInferenceResponse, invalid.finish());
}
const Sink = struct {
    output: std.ArrayList(u8) = .empty,
    closed: bool = false,
    fn view(self: *Sink) lib.ChatStream {
        return .{ .context = self, .start = start, .write = write, .close = close };
    }
    fn start(_: *anyopaque, status: u16, content_type: []const u8, _: ?[]const u8, _: ?[]const u8) !void {
        try std.testing.expectEqual(@as(u16, 200), status);
        try std.testing.expectEqualStrings("text/event-stream", content_type);
    }
    fn write(raw: *anyopaque, bytes: []const u8) !void {
        const self: *Sink = @ptrCast(@alignCast(raw));
        try self.output.appendSlice(std.testing.allocator, bytes);
    }
    fn close(raw: *anyopaque) !void {
        const self: *Sink = @ptrCast(@alignCast(raw));
        self.closed = true;
    }
};
