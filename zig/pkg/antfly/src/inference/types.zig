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

pub fn chatRequestJsonAlloc(
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const ChatMessage,
    flavor: ChatWireFlavor,
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
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
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
                if (media.url == null and std.mem.startsWith(u8, media.mime_type, "image/")) {
                    const data_uri = try std.fmt.allocPrint(alloc, "data:{s};base64,{s}", .{ media.mime_type, media.data });
                    defer alloc.free(data_uri);
                    try out.appendSlice(alloc, "{\"type\":\"image_url\",\"image_url\":{\"url\":");
                    try appendJsonString(alloc, out, data_uri);
                    try out.appendSlice(alloc, "}}");
                } else {
                    try appendMediaContentPartJson(alloc, out, media);
                }
            },
        },
    }
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
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GenerateResult) void {
        self.allocator.free(self.content);
        self.* = undefined;
    }
};

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

test "openai compatible chat serialization keeps non-image media as media" {
    const alloc = std.testing.allocator;
    const messages = [_]ChatMessage{.{
        .role = .user,
        .content = .{ .parts = &.{
            .{ .text = "listen" },
            .{ .media = .{ .mime_type = "audio/wav", .data = "AA==" } },
            .{ .media = .{ .mime_type = "image/png", .data = "AQ==" } },
        } },
    }};
    const body = try chatRequestJsonAlloc(alloc, "gemma4", &messages, .openai_compatible);
    defer alloc.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"type\":\"media\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"mime_type\":\"audio/wav\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "data:image/png;base64,AQ==") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "data:audio/wav;base64") == null);
}

test "openai compatible chat serialization preserves media urls as media" {
    const alloc = std.testing.allocator;
    const messages = [_]ChatMessage{.{
        .role = .user,
        .content = .{ .parts = &.{
            .{ .text = "listen" },
            .{ .media = .{ .mime_type = "audio/wav", .url = "https://example.test/audio.wav" } },
        } },
    }};
    const body = try chatRequestJsonAlloc(alloc, "gemma4", &messages, .openai_compatible);
    defer alloc.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"type\":\"media\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"url\":\"https://example.test/audio.wav\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"type\":\"image_url\"") == null);
}
