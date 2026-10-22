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

// Provider-neutral types and interfaces for ML inference.

const std = @import("std");
const generating = @import("mod.zig");
const ChatMessage = generating.ChatMessage;
const ChatMessageContent = generating.ChatMessageContent;
const ContentPart = generating.ContentPart;
const MediaContent = generating.MediaContent;
const ToolCall = generating.ToolCall;

pub const ChatWireFlavor = enum {
    openai_compatible,
    termite_native,
};

pub const ChatRequestOptions = struct {
    response_format_json: ?[]const u8 = null,
    logprobs: bool = false,
    top_logprobs: u8 = 20,
    tools_json: ?[]const u8 = null,
    tool_choice_json: ?[]const u8 = null,
    max_tokens: ?i64 = null,
    temperature: ?f32 = null,
    top_p: ?f32 = null,
    top_k: ?i64 = null,
    frequency_penalty: ?f32 = null,
    presence_penalty: ?f32 = null,
    enable_thinking: ?bool = null,
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
    if (options.response_format_json) |format| {
        try out.appendSlice(alloc, ",\"response_format\":");
        try out.appendSlice(alloc, format);
    }
    if (options.logprobs) {
        try out.appendSlice(alloc, ",\"logprobs\":true");
        try appendJsonI64Field(alloc, &out, "top_logprobs", options.top_logprobs);
    }
    if (options.max_tokens) |max_tokens| {
        try appendJsonI64Field(alloc, &out, "max_tokens", max_tokens);
    }
    if (options.temperature) |temperature| try appendJsonFloatField(alloc, &out, "temperature", temperature);
    if (options.top_p) |top_p| try appendJsonFloatField(alloc, &out, "top_p", top_p);
    if (options.top_k) |top_k| try appendJsonI64Field(alloc, &out, "top_k", top_k);
    if (options.frequency_penalty) |frequency_penalty| try appendJsonFloatField(alloc, &out, "frequency_penalty", frequency_penalty);
    if (options.presence_penalty) |presence_penalty| try appendJsonFloatField(alloc, &out, "presence_penalty", presence_penalty);
    if (options.enable_thinking) |enabled| {
        try out.appendSlice(alloc, if (enabled) ",\"chat_template_kwargs\":{\"enable_thinking\":true}" else ",\"chat_template_kwargs\":{\"enable_thinking\":false}");
    }
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
            if (image_url.detail) |detail| {
                try out.appendSlice(alloc, ",\"detail\":");
                try appendJsonString(alloc, out, detail);
            }
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
    if (!std.ascii.startsWithIgnoreCase(media.mime_type, "image/")) return error.UnsupportedOpenAIContentPart;
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

test "shared chat wire preserves image detail and native media content" {
    const a = std.testing.allocator;
    const messages = [_]ChatMessage{.{ .role = .user, .content = .{ .parts = &.{
        .{ .image_url = .{ .url = "https://example.com/image.png", .detail = "high" } },
        .{ .media = .{ .mime_type = "image/png", .data = "iVBORw0KGgo=" } },
    } } }};
    for ([_]ChatWireFlavor{ .openai_compatible, .termite_native }) |flavor| {
        const body = try chatRequestJsonAlloc(a, "vision", &messages, flavor);
        defer a.free(body);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, body, .{});
        defer parsed.deinit();
        const parts = parsed.value.object.get("messages").?.array.items[0].object.get("content").?.array.items;
        try std.testing.expectEqualStrings("high", parts[0].object.get("image_url").?.object.get("detail").?.string);
        if (flavor == .termite_native) {
            try std.testing.expectEqualStrings("iVBORw0KGgo=", parts[1].object.get("data").?.string);
        } else {
            try std.testing.expectEqualStrings("data:image/png;base64,iVBORw0KGgo=", parts[1].object.get("image_url").?.object.get("url").?.string);
        }
    }
}
