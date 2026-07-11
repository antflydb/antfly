// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

// Provider-neutral types and interfaces for ML inference.

const std = @import("std");
const generating = @import("antfly_generating");

pub const Role = generating.Role;
pub const ImageURL = generating.ImageURL;
pub const MediaContent = generating.MediaContent;
pub const ContentPart = generating.ContentPart;
pub const ChatMessageContent = generating.ChatMessageContent;
pub const ToolCall = generating.ToolCall;
pub const ChatMessage = generating.ChatMessage;

pub const ChatWireFlavor = enum {
    openai_compatible,
    termite_native,
};

pub const ChatRequestOptions = struct {
    tools_json: ?[]const u8 = null,
    tool_choice_json: ?[]const u8 = null,
    max_tokens: ?i64 = null,
    temperature: ?f32 = null,
    top_p: ?f32 = null,
    top_k: ?i64 = null,
    frequency_penalty: ?f32 = null,
    presence_penalty: ?f32 = null,
};

pub fn chatRequestJsonAlloc(
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const ChatMessage,
    flavor: ChatWireFlavor,
) ![]u8 {
    return try chatRequestJsonWithOptionsAlloc(alloc, model, messages, flavor, .{});
}

pub fn chatRequestJsonWithOptionsAlloc(
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const ChatMessage,
    flavor: ChatWireFlavor,
    options: ChatRequestOptions,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"model\":");
    try appendJsonString(alloc, &out, model);
    try out.appendSlice(alloc, ",\"messages\":[");
    for (messages, 0..) |message, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendChatMessageJson(alloc, &out, message, flavor);
    }
    try out.append(alloc, ']');
    if (options.tools_json) |tools_json| {
        try out.appendSlice(alloc, ",\"tools\":");
        try out.appendSlice(alloc, tools_json);
    }
    if (options.tool_choice_json) |tool_choice_json| {
        try out.appendSlice(alloc, ",\"tool_choice\":");
        try out.appendSlice(alloc, tool_choice_json);
    }
    if (options.max_tokens) |max_tokens| {
        try appendJsonI64Field(alloc, &out, "max_tokens", max_tokens);
    }
    if (options.temperature) |temperature| try appendJsonFloatField(alloc, &out, "temperature", temperature);
    if (options.top_p) |top_p| try appendJsonFloatField(alloc, &out, "top_p", top_p);
    if (options.top_k) |top_k| try appendJsonI64Field(alloc, &out, "top_k", top_k);
    if (options.frequency_penalty) |frequency_penalty| try appendJsonFloatField(alloc, &out, "frequency_penalty", frequency_penalty);
    if (options.presence_penalty) |presence_penalty| try appendJsonFloatField(alloc, &out, "presence_penalty", presence_penalty);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendJsonI64Field(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), name: []const u8, value: i64) !void {
    const fragment = try std.fmt.allocPrint(alloc, ",\"{s}\":{d}", .{ name, value });
    defer alloc.free(fragment);
    try out.appendSlice(alloc, fragment);
}

fn appendJsonFloatField(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), name: []const u8, value: f32) !void {
    const fragment = try std.fmt.allocPrint(alloc, ",\"{s}\":{f}", .{ name, std.json.fmt(value, .{}) });
    defer alloc.free(fragment);
    try out.appendSlice(alloc, fragment);
}

fn appendChatMessageJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    message: ChatMessage,
    flavor: ChatWireFlavor,
) !void {
    try out.appendSlice(alloc, "{\"role\":");
    try appendJsonString(alloc, out, message.role.toSlice());
    if (message.content) |content| {
        try out.appendSlice(alloc, ",\"content\":");
        try appendChatMessageContentJson(alloc, out, content, flavor);
    }
    if (message.tool_calls) |tool_calls| {
        try out.appendSlice(alloc, ",\"tool_calls\":[");
        for (tool_calls, 0..) |tool_call, i| {
            if (i > 0) try out.append(alloc, ',');
            try appendToolCallJson(alloc, out, tool_call);
        }
        try out.append(alloc, ']');
    }
    if (message.tool_call_id) |tool_call_id| {
        try out.appendSlice(alloc, ",\"tool_call_id\":");
        try appendJsonString(alloc, out, tool_call_id);
    }
    try out.append(alloc, '}');
}

fn appendChatMessageContentJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    content: ChatMessageContent,
    flavor: ChatWireFlavor,
) !void {
    switch (content) {
        .text => |text| try appendJsonString(alloc, out, text),
        .parts => |parts| {
            try out.append(alloc, '[');
            for (parts, 0..) |part, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendContentPartJson(alloc, out, part, flavor);
            }
            try out.append(alloc, ']');
        },
    }
}

fn appendContentPartJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    part: ContentPart,
    flavor: ChatWireFlavor,
) !void {
    switch (part) {
        .text => |text| {
            try out.appendSlice(alloc, "{\"type\":\"text\",\"text\":");
            try appendJsonString(alloc, out, text);
            try out.append(alloc, '}');
        },
        .image_url => |image_url| {
            try out.appendSlice(alloc, "{\"type\":\"image_url\",\"image_url\":{\"url\":");
            try appendJsonString(alloc, out, image_url.url);
            try out.appendSlice(alloc, "}}");
        },
        .media => |media| switch (flavor) {
            .termite_native => {
                try appendMediaContentPartJson(alloc, out, media);
            },
            .openai_compatible => {
                try appendOpenAiMediaContentPartJson(alloc, out, media);
            },
        },
    }
}

fn appendOpenAiMediaContentPartJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    media: MediaContent,
) !void {
    if (!std.mem.startsWith(u8, media.mime_type, "image/")) return error.UnsupportedOpenAIContentPart;
    const image_url = if (media.url) |url|
        url
    else
        try std.fmt.allocPrint(alloc, "data:{s};base64,{s}", .{ media.mime_type, media.data });
    defer if (media.url == null) alloc.free(image_url);

    try out.appendSlice(alloc, "{\"type\":\"image_url\",\"image_url\":{\"url\":");
    try appendJsonString(alloc, out, image_url);
    try out.appendSlice(alloc, "}}");
}

fn appendMediaContentPartJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    media: MediaContent,
) !void {
    try out.appendSlice(alloc, "{\"type\":\"media\"");
    if (media.url) |url| {
        try out.appendSlice(alloc, ",\"url\":");
        try appendJsonString(alloc, out, url);
        if (media.mime_type.len > 0) {
            try out.appendSlice(alloc, ",\"mime_type\":");
            try appendJsonString(alloc, out, media.mime_type);
        }
    } else {
        try out.appendSlice(alloc, ",\"data\":");
        try appendJsonString(alloc, out, media.data);
        try out.appendSlice(alloc, ",\"mime_type\":");
        try appendJsonString(alloc, out, media.mime_type);
    }
    try out.append(alloc, '}');
}

fn appendToolCallJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    tool_call: ToolCall,
) !void {
    try out.appendSlice(alloc, "{\"id\":");
    try appendJsonString(alloc, out, tool_call.id);
    try out.appendSlice(alloc, ",\"type\":\"function\",\"function\":{\"name\":");
    try appendJsonString(alloc, out, tool_call.name);
    try out.appendSlice(alloc, ",\"arguments\":");
    try appendJsonString(alloc, out, tool_call.arguments);
    try out.appendSlice(alloc, "}}");
}

fn appendJsonString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: []const u8,
) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

pub const EmbedResult = struct {
    /// One f32 vector per input text.
    vectors: []const []const f32,
    dimension: usize,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *EmbedResult) void {
        for (self.vectors) |v| self.allocator.free(v);
        self.allocator.free(self.vectors);
        self.* = undefined;
    }
};

pub const SparseEmbedResult = struct {
    indices: []const []const i32,
    values: []const []const f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *SparseEmbedResult) void {
        for (self.indices) |idx| self.allocator.free(idx);
        self.allocator.free(self.indices);
        for (self.values) |val| self.allocator.free(val);
        self.allocator.free(self.values);
        self.* = undefined;
    }
};

pub const GenerateResult = struct {
    content: []const u8,
    tool_calls: []ToolCall = &.{},
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GenerateResult) void {
        self.allocator.free(self.content);
        for (self.tool_calls) |*tool_call| tool_call.deinit(self.allocator);
        if (self.tool_calls.len > 0) self.allocator.free(self.tool_calls);
        self.* = undefined;
    }
};

/// Some OpenAI-compatible providers and native model adapters emit a forced
/// function call as plain JSON content instead of a `tool_calls` entry. The
/// provider boundary normalizes that shape into the internal OpenAI-compatible
/// ToolCall representation. This is intentionally narrow: a single function
/// must be forced, that function must exist in `tools`, and the content must
/// parse as JSON.
pub fn synthesizeForcedToolCallFromContent(
    alloc: std.mem.Allocator,
    content_raw: []const u8,
    tools_json: ?[]const u8,
    tool_choice_json: ?[]const u8,
) ![]ToolCall {
    const content = jsonContentPayload(content_raw);
    if (content.len == 0) return &.{};

    const forced_name = (try forcedToolNameAlloc(alloc, tool_choice_json)) orelse return &.{};
    defer alloc.free(forced_name);
    if (!(try toolsContainFunction(alloc, tools_json, forced_name))) return &.{};

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, content, .{
        .allocate = .alloc_always,
    }) catch return &.{};
    defer parsed.deinit();
    switch (parsed.value) {
        .object, .array => {},
        else => return &.{},
    }

    const arguments = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(arguments);
    const calls = try alloc.alloc(ToolCall, 1);
    errdefer alloc.free(calls);
    calls[0] = .{
        .id = try alloc.dupe(u8, "call_forced_0"),
        .name = try alloc.dupe(u8, forced_name),
        .arguments = arguments,
    };
    return calls;
}

fn forcedToolNameAlloc(alloc: std.mem.Allocator, tool_choice_json: ?[]const u8) !?[]u8 {
    const raw = tool_choice_json orelse return null;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{
        .allocate = .alloc_always,
    }) catch return null;
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const function = parsed.value.object.get("function") orelse return null;
    if (function != .object) return null;
    const name = function.object.get("name") orelse return null;
    if (name != .string or name.string.len == 0) return null;
    return try alloc.dupe(u8, name.string);
}

fn toolsContainFunction(alloc: std.mem.Allocator, tools_json: ?[]const u8, name: []const u8) !bool {
    const raw = tools_json orelse return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{
        .allocate = .alloc_always,
    }) catch return false;
    defer parsed.deinit();
    if (parsed.value != .array) return false;
    for (parsed.value.array.items) |tool| {
        if (tool != .object) continue;
        const tool_type = tool.object.get("type") orelse continue;
        if (tool_type != .string or !std.mem.eql(u8, tool_type.string, "function")) continue;
        const function = tool.object.get("function") orelse continue;
        if (function != .object) continue;
        const function_name = function.object.get("name") orelse continue;
        if (function_name == .string and std.mem.eql(u8, function_name.string, name)) return true;
    }
    return false;
}

fn jsonContentPayload(raw: []const u8) []const u8 {
    var text = std.mem.trim(u8, raw, " \t\r\n");
    if (!std.mem.startsWith(u8, text, "```")) return text;
    const first_newline = std.mem.indexOfScalar(u8, text, '\n') orelse return text;
    text = std.mem.trim(u8, text[first_newline + 1 ..], " \t\r\n");
    if (std.mem.endsWith(u8, text, "```")) {
        text = std.mem.trim(u8, text[0 .. text.len - 3], " \t\r\n");
    }
    return text;
}

pub const RerankResult = struct {
    scores: []const f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *RerankResult) void {
        self.allocator.free(self.scores);
        self.* = undefined;
    }
};

/// Provider-neutral dense embedding interface.
pub const Embedder = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        embed: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, inputs: []const []const u8) anyerror!EmbedResult,
    };

    pub fn embed(self: Embedder, alloc: std.mem.Allocator, model: []const u8, inputs: []const []const u8) !EmbedResult {
        return self.vtable.embed(self.ptr, alloc, model, inputs);
    }
};

/// Provider-neutral text generation / chat interface.
pub const Generator = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        generate: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, messages: []const ChatMessage) anyerror!GenerateResult,
    };

    pub fn generate(self: Generator, alloc: std.mem.Allocator, model: []const u8, messages: []const ChatMessage) !GenerateResult {
        return self.vtable.generate(self.ptr, alloc, model, messages);
    }
};

/// Provider-neutral reranking interface.
pub const Reranker = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        rerank: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, query: []const u8, documents: []const []const u8) anyerror!RerankResult,
    };

    pub fn rerank(self: Reranker, alloc: std.mem.Allocator, model: []const u8, query: []const u8, documents: []const []const u8) !RerankResult {
        return self.vtable.rerank(self.ptr, alloc, model, query, documents);
    }
};

test "types compile" {
    _ = Embedder;
    _ = Generator;
    _ = Reranker;
    _ = EmbedResult;
    _ = SparseEmbedResult;
    _ = GenerateResult;
    _ = RerankResult;
    _ = ChatMessage;
    _ = Role;
}

test "openai compatible chat serialization rejects non-image media" {
    const alloc = std.testing.allocator;
    const messages = [_]ChatMessage{.{
        .role = .user,
        .content = .{ .parts = &.{
            .{ .text = "listen" },
            .{ .media = .{ .mime_type = "audio/wav", .data = "AA==" } },
            .{ .media = .{ .mime_type = "image/png", .data = "AQ==" } },
        } },
    }};
    try std.testing.expectError(error.UnsupportedOpenAIContentPart, chatRequestJsonAlloc(alloc, "gemma4", &messages, .openai_compatible));
}

test "openai compatible chat serialization converts image media urls" {
    const alloc = std.testing.allocator;
    const messages = [_]ChatMessage{.{
        .role = .user,
        .content = .{ .parts = &.{
            .{ .text = "look" },
            .{ .media = .{ .mime_type = "image/png", .url = "https://example.test/image.png" } },
        } },
    }};
    const body = try chatRequestJsonAlloc(alloc, "gemma4", &messages, .openai_compatible);
    defer alloc.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"type\":\"media\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"type\":\"image_url\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"url\":\"https://example.test/image.png\"") != null);
}

test "chat serialization includes max_tokens when configured" {
    const alloc = std.testing.allocator;
    const messages = [_]ChatMessage{.{ .role = .user, .content = .{ .text = "hello" } }};
    const body = try chatRequestJsonWithOptionsAlloc(alloc, "gemma4", &messages, .openai_compatible, .{ .max_tokens = 256 });
    defer alloc.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"max_tokens\":256") != null);
}

test "chat serialization includes sampling controls when configured" {
    const alloc = std.testing.allocator;
    const messages = [_]ChatMessage{.{ .role = .user, .content = .{ .text = "hello" } }};
    const body = try chatRequestJsonWithOptionsAlloc(alloc, "gemma4", &messages, .openai_compatible, .{
        .max_tokens = 256,
        .temperature = 0.25,
        .top_p = 0.9,
        .top_k = 40,
        .frequency_penalty = 0.1,
        .presence_penalty = 0.2,
    });
    defer alloc.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"temperature\":0.25") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"top_p\":0.9") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"top_k\":40") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"frequency_penalty\":0.1") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"presence_penalty\":0.2") != null);
}

test "forced tool call synthesis normalizes plain JSON content" {
    const alloc = std.testing.allocator;
    const tools =
        \\[{"type":"function","function":{"name":"emit_relations","parameters":{"type":"object"}}}]
    ;
    const tool_choice =
        \\{"type":"function","function":{"name":"emit_relations"}}
    ;

    const calls = try synthesizeForcedToolCallFromContent(alloc,
        \\{"relations":[{"type":"mentioned in","target":{"id":"Ada"}}]}
    , tools, tool_choice);
    defer {
        for (calls) |*call| call.deinit(alloc);
        if (calls.len > 0) alloc.free(calls);
    }

    try std.testing.expectEqual(@as(usize, 1), calls.len);
    try std.testing.expectEqualStrings("emit_relations", calls[0].name);
    try std.testing.expect(std.mem.indexOf(u8, calls[0].arguments, "\"relations\"") != null);
}

test "forced tool call synthesis accepts fenced JSON and rejects non-forced content" {
    const alloc = std.testing.allocator;
    const tools =
        \\[{"type":"function","function":{"name":"emit_relations","parameters":{"type":"object"}}}]
    ;
    const tool_choice =
        \\{"type":"function","function":{"name":"emit_relations"}}
    ;
    const fenced =
        \\```json
        \\{"relations":[]}
        \\```
    ;

    const calls = try synthesizeForcedToolCallFromContent(alloc, fenced, tools, tool_choice);
    defer {
        for (calls) |*call| call.deinit(alloc);
        if (calls.len > 0) alloc.free(calls);
    }
    try std.testing.expectEqual(@as(usize, 1), calls.len);
    try std.testing.expectEqualStrings("{\"relations\":[]}", calls[0].arguments);

    const skipped = try synthesizeForcedToolCallFromContent(alloc, "{\"relations\":[]}", tools, "\"auto\"");
    try std.testing.expectEqual(@as(usize, 0), skipped.len);
}
