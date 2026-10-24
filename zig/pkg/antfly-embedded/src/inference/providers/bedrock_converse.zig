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

//! Model-independent Bedrock Converse translation. Never infer vision support
//! from model IDs: the gateway's capability declaration owns that admission.
const std = @import("std");
const inference = @import("antfly_inference_types");
const Allocator = std.mem.Allocator;
const Value = std.json.Value;

pub const Options = struct {
    max_tokens: i64 = 256,
    temperature: ?f32 = null,
    top_p: ?f32 = null,
    tools_json: ?[]const u8 = null,
    tool_choice_json: ?[]const u8 = null,
    response_format_json: ?[]const u8 = null,
};

fn object() Value {
    return .{ .object = .empty };
}
fn array(a: Allocator) Value {
    return .{ .array = .init(a) };
}
fn str(value: []const u8) Value {
    return .{ .string = value };
}
fn single(a: Allocator, key: []const u8, value: Value) !Value {
    var result = object();
    try result.object.put(a, key, value);
    return result;
}
fn text(a: Allocator, value: []const u8) !Value {
    return single(a, "text", str(value));
}
fn field(value: Value, key: []const u8) !Value {
    if (value != .object) return error.InvalidBedrockResponse;
    return value.object.get(key) orelse error.InvalidBedrockResponse;
}
fn string(value: Value) ![]const u8 {
    if (value != .string) return error.InvalidBedrockResponse;
    return value.string;
}

fn requestField(value: Value, key: []const u8) !Value {
    return field(value, key) catch return error.InvalidGeneratorConfig;
}

pub fn requestJsonAlloc(alloc: Allocator, messages: []const inference.ChatMessage, options: Options) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var root = object();
    var system = array(a);
    var contents = array(a);
    for (messages) |message| {
        var parts = array(a);
        if (message.role == .tool) {
            const id = message.tool_call_id orelse return error.UnsupportedBedrockContentPart;
            const content = if (message.content) |value| switch (value) {
                .text => |value_text| value_text,
                else => return error.UnsupportedBedrockContentPart,
            } else "";
            var result = try single(a, "toolUseId", str(id));
            var results = array(a);
            try results.array.append(if (std.mem.trim(u8, content, " \t\r\n").len == 0) try single(a, "json", str(content)) else try text(a, content));
            try result.object.put(a, "content", results);
            try parts.array.append(try single(a, "toolResult", result));
        } else if (message.content) |content| switch (content) {
            .text => |value| if (std.mem.trim(u8, value, " \t\r\n").len > 0) try parts.array.append(try text(a, value)),
            .parts => |values| for (values) |part| {
                switch (part) {
                    .text => |value| if (std.mem.trim(u8, value, " \t\r\n").len > 0) try parts.array.append(try text(a, value)),
                    .image_url => |image| {
                        if (message.role != .user) return error.UnsupportedBedrockContentPart;
                        try parts.array.append(try imageDataUri(a, image.url));
                    },
                    .media => |media| {
                        if (message.role != .user) return error.UnsupportedBedrockContentPart;
                        try parts.array.append(if (media.url) |url| try imageDataUri(a, url) else try imagePart(a, media.mime_type, media.data));
                    },
                }
            },
        };
        if (message.role == .system) {
            if (message.tool_calls != null or message.tool_call_id != null) return error.UnsupportedBedrockContentPart;
            try system.array.appendSlice(parts.array.items);
            continue;
        }
        if (message.tool_calls) |calls| {
            if (message.role != .assistant) return error.UnsupportedBedrockContentPart;
            for (calls) |call| {
                const input = std.json.parseFromSliceLeaky(Value, a, call.arguments, .{}) catch return error.InvalidGeneratorConfig;
                if (input != .object) return error.InvalidGeneratorConfig;
                var use = try single(a, "toolUseId", str(call.id));
                try use.object.put(a, "name", str(call.name));
                try use.object.put(a, "input", input);
                try parts.array.append(try single(a, "toolUse", use));
            }
        }
        if (parts.array.items.len == 0) return error.UnsupportedBedrockContentPart;
        const role = if (message.role == .assistant) "assistant" else "user";
        // Consecutive tool results become one user turn, preserving order and
        // toolUseId rather than fabricating alternation or flattening to text.
        if (contents.array.items.len > 0) {
            const previous = &contents.array.items[contents.array.items.len - 1];
            if (std.mem.eql(u8, try string(try requestField(previous.*, "role")), role)) {
                try previous.object.getPtr("content").?.array.appendSlice(parts.array.items);
                continue;
            }
        }
        var entry = try single(a, "role", str(role));
        try entry.object.put(a, "content", parts);
        try contents.array.append(entry);
    }
    if (contents.array.items.len == 0) return error.UnsupportedBedrockContentPart;
    try root.object.put(a, "messages", contents);
    if (system.array.items.len > 0) try root.object.put(a, "system", system);
    var config = try single(a, "maxTokens", .{ .integer = options.max_tokens });
    if (options.temperature) |value| try config.object.put(a, "temperature", .{ .float = value });
    if (options.top_p) |value| try config.object.put(a, "topP", .{ .float = value });
    try root.object.put(a, "inferenceConfig", config);
    if (options.response_format_json) |raw| {
        const format = try std.json.parseFromSliceLeaky(Value, a, raw, .{});
        const kind = try requestField(format, "type");
        if (kind != .string or !std.mem.eql(u8, kind.string, "json_schema")) return error.UnsupportedBedrockGenerationOptions;
        const input = try requestField(format, "json_schema");
        const schema = try requestField(input, "schema");
        if (schema != .object) return error.InvalidGeneratorConfig;
        var definition = try single(a, "schema", str(try std.json.Stringify.valueAlloc(a, schema, .{})));
        inline for (.{ "name", "description" }) |key| if (input.object.get(key)) |value| {
            if (value != .string) return error.InvalidGeneratorConfig;
            try definition.object.put(a, key, value);
        };
        var text_format = try single(a, "type", str("json_schema"));
        try text_format.object.put(a, "structure", try single(a, "jsonSchema", definition));
        try root.object.put(a, "outputConfig", try single(a, "textFormat", text_format));
    }

    if (options.tools_json) |raw| {
        const input = try std.json.parseFromSliceLeaky(Value, a, raw, .{});
        if (input != .array or input.array.items.len == 0) return error.InvalidGeneratorConfig;
        var tools = array(a);
        for (input.array.items) |item| {
            const function = try requestField(item, "function");
            var spec = try single(a, "name", try requestField(function, "name"));
            if (function.object.get("description")) |description| try spec.object.put(a, "description", description);
            const schema = function.object.get("parameters") orelse return error.InvalidGeneratorConfig;
            try spec.object.put(a, "inputSchema", try single(a, "json", schema));
            try tools.array.append(try single(a, "toolSpec", spec));
        }
        var tool_config = try single(a, "tools", tools);
        if (options.tool_choice_json) |choice_raw| {
            const choice = try std.json.parseFromSliceLeaky(Value, a, choice_raw, .{});
            const translated = if (choice == .string) blk: {
                const key: []const u8 = if (std.mem.eql(u8, choice.string, "auto")) "auto" else if (std.mem.eql(u8, choice.string, "required")) "any" else return error.UnsupportedBedrockGenerationOptions;
                break :blk try single(a, key, object());
            } else try single(a, "tool", try single(a, "name", try requestField(try requestField(choice, "function"), "name")));
            try tool_config.object.put(a, "toolChoice", translated);
        }
        try root.object.put(a, "toolConfig", tool_config);
    } else if (options.tool_choice_json != null) return error.InvalidGeneratorConfig;
    return std.json.Stringify.valueAlloc(alloc, root, .{});
}

fn imageDataUri(a: Allocator, uri: []const u8) !Value {
    if (!std.mem.startsWith(u8, uri, "data:")) return error.UnsupportedBedrockContentPart;
    const separator = std.mem.indexOf(u8, uri, ";base64,") orelse return error.UnsupportedBedrockContentPart;
    return imagePart(a, uri[5..separator], uri[separator + 8 ..]);
}

fn imagePart(a: Allocator, mime: []const u8, bytes: []const u8) !Value {
    const format: []const u8 = if (std.ascii.eqlIgnoreCase(mime, "image/png")) "png" else if (std.ascii.eqlIgnoreCase(mime, "image/jpeg")) "jpeg" else if (std.ascii.eqlIgnoreCase(mime, "image/gif")) "gif" else if (std.ascii.eqlIgnoreCase(mime, "image/webp")) "webp" else return error.UnsupportedBedrockContentPart;
    if (bytes.len == 0) return error.UnsupportedBedrockContentPart;
    var image = try single(a, "format", str(format));
    try image.object.put(a, "source", try single(a, "bytes", str(bytes)));
    return single(a, "image", image);
}

pub fn finishReason(native: []const u8) !inference.GenerationFinishReason {
    if (std.mem.eql(u8, native, "tool_use")) return .tool_calls;
    if (std.mem.eql(u8, native, "max_tokens") or std.mem.eql(u8, native, "model_context_window_exceeded")) return .length;
    if (std.mem.eql(u8, native, "guardrail_intervened") or std.mem.eql(u8, native, "content_filtered")) return .content_filter;
    if (std.mem.eql(u8, native, "end_turn") or std.mem.eql(u8, native, "stop_sequence")) return .stop;
    return error.InvalidBedrockResponse;
}

pub fn responseAlloc(alloc: Allocator, body: []const u8) !inference.GenerateResult {
    var parsed = try std.json.parseFromSlice(Value, alloc, body, .{});
    defer parsed.deinit();
    const message = try field(try field(parsed.value, "output"), "message");
    if (!std.mem.eql(u8, try string(try field(message, "role")), "assistant")) return error.InvalidBedrockResponse;
    const parts = try field(message, "content");
    if (parts != .array or parts.array.items.len == 0) return error.InvalidBedrockResponse;
    var output: std.ArrayListUnmanaged(u8) = .empty;
    defer output.deinit(alloc);
    var calls: std.ArrayListUnmanaged(inference.ToolCall) = .empty;
    errdefer {
        for (calls.items) |*call| call.deinit(alloc);
        calls.deinit(alloc);
    }
    for (parts.array.items) |part| {
        if (part != .object) return error.InvalidBedrockResponse;
        if (part.object.get("text")) |value| {
            try output.appendSlice(alloc, try string(value));
        } else if (part.object.get("toolUse")) |use| {
            const id = try alloc.dupe(u8, try string(try field(use, "toolUseId")));
            errdefer alloc.free(id);
            const name = try alloc.dupe(u8, try string(try field(use, "name")));
            errdefer alloc.free(name);
            const arguments = try std.json.Stringify.valueAlloc(alloc, try field(use, "input"), .{});
            errdefer alloc.free(arguments);
            try calls.append(alloc, .{ .id = id, .name = name, .arguments = arguments });
        } else return error.InvalidBedrockResponse;
    }
    const content = try output.toOwnedSlice(alloc);
    errdefer alloc.free(content);
    return .{
        .content = content,
        .finish_reason = try finishReason(try string(try field(parsed.value, "stopReason"))),
        .tool_calls = try calls.toOwnedSlice(alloc),
        .usage = inference.GenerationUsage.fromValue(parsed.value.object.get("usage") orelse .null, "inputTokens", "outputTokens", "totalTokens"),
        .allocator = alloc,
    };
}

pub fn testConverseWire() !void {
    const a = std.testing.allocator;
    const messages = [_]inference.ChatMessage{
        .{ .role = .system, .content = .{ .text = "Use the sheets" } },
        .{ .role = .user, .content = .{ .parts = &.{ .{ .text = "Compare" }, .{ .image_url = .{ .url = "data:image/jpeg;base64,aW1hZ2U=" } }, .{ .media = .{ .mime_type = "image/png", .data = "c2hlZXQ=" } } } } },
        .{ .role = .assistant, .content = .{ .text = "" }, .tool_calls = &.{.{ .id = "call-1", .name = "lookup", .arguments = "{\"id\":7}" }} },
        .{ .role = .tool, .tool_call_id = "call-1", .content = .{ .text = "found" } },
    };
    const bytes = try requestJsonAlloc(a, &messages, .{});
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(Value, a, bytes, .{});
    defer parsed.deinit();
    const turns = parsed.value.object.get("messages").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), turns.len);
    try std.testing.expectEqual(@as(usize, 1), turns[1].object.get("content").?.array.items.len);
    const parts = turns[0].object.get("content").?.array.items;
    try std.testing.expectEqualStrings("jpeg", parts[1].object.get("image").?.object.get("format").?.string);
    try std.testing.expectEqualStrings("aW1hZ2U=", parts[1].object.get("image").?.object.get("source").?.object.get("bytes").?.string);
    try std.testing.expectEqualStrings("call-1", turns[2].object.get("content").?.array.items[0].object.get("toolResult").?.object.get("toolUseId").?.string);
    var result = try responseAlloc(a,
        \\{"output":{"message":{"role":"assistant","content":[{"text":"Compared"},{"toolUse":{"toolUseId":"call-2","name":"lookup","input":{"id":8}}}]}},"stopReason":"tool_use","usage":{"inputTokens":100,"outputTokens":5,"totalTokens":105}}
    );
    defer result.deinit();
    try std.testing.expectEqual(inference.GenerationFinishReason.tool_calls, result.finish_reason.?);
    try std.testing.expectEqualStrings("Compared", result.content);
    try std.testing.expectEqualStrings("call-2", result.tool_calls[0].id);
    try std.testing.expectEqual(@as(u64, 100), result.usage.?.input_tokens);
    // Completed and streaming adapters share the same stop-reason mapping.
    const cases = .{
        .{ "max_tokens", inference.GenerationFinishReason.length },
        .{ "model_context_window_exceeded", inference.GenerationFinishReason.length },
        .{ "end_turn", inference.GenerationFinishReason.stop },
        .{ "stop_sequence", inference.GenerationFinishReason.stop },
        .{ "tool_use", inference.GenerationFinishReason.tool_calls },
        .{ "guardrail_intervened", inference.GenerationFinishReason.content_filter },
        .{ "content_filtered", inference.GenerationFinishReason.content_filter },
    };
    inline for (cases) |case| {
        const body = try std.fmt.allocPrint(a, "{{\"output\":{{\"message\":{{\"role\":\"assistant\",\"content\":[{{\"text\":\"partial\"}}]}}}},\"stopReason\":\"{s}\"}}", .{case[0]});
        defer a.free(body);
        var finished = try responseAlloc(a, body);
        defer finished.deinit();
        try std.testing.expectEqual(case[1], finished.finish_reason.?);
    }
    try std.testing.expectError(error.InvalidBedrockResponse, finishReason("unknown_future_reason"));
}

test "Bedrock Converse preserves images text tool identities and usage" {
    try testConverseWire();
}
