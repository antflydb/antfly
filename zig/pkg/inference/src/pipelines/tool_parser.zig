// Copyright 2026 Antfly, Inc.
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
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");

var tool_call_counter: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

pub const FunctionDefinition = struct {
    name: []const u8,
    description: []const u8 = "",
    parameters: ?std.json.Value = null,
    strict: bool = false,
};

pub const ToolDefinition = struct {
    type: []const u8 = "function",
    function: FunctionDefinition,
};

pub const ToolCallFunction = struct {
    name: []const u8,
    arguments: []const u8,
};

pub const ToolCall = struct {
    id: []const u8,
    type: []const u8,
    function: ToolCallFunction,

    pub fn deinit(self: *ToolCall, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.function.name);
        allocator.free(self.function.arguments);
        self.* = undefined;
    }
};

pub const FeedUpdate = struct {
    ready_text: []const u8 = "",
    new_calls: []const ToolCall = &.{},
    call_start_index: usize = 0,
    active_tool_delta: ?ToolCallDeltaUpdate = null,
};

pub const ToolCallDeltaUpdate = struct {
    index: usize,
    id: ?[]const u8 = null,
    type: ?[]const u8 = null,
    name: ?[]const u8 = null,
    arguments: ?[]const u8 = null,
};

pub const ParsedToolChoice = union(enum) {
    auto,
    none,
    required,
    function: []const u8,
};

pub const FunctionGemmaTokens = struct {
    format: FunctionToolFormat = .functiongemma,
    start_function_decl: []const u8 = "<start_function_declaration>",
    end_function_decl: []const u8 = "<end_function_declaration>",
    start_function_call: []const u8 = "<start_function_call>",
    end_function_call: []const u8 = "<end_function_call>",
    escape: []const u8 = "<escape>",
};

const FunctionToolFormat = enum {
    functiongemma,
    gemma4,
};

const FunctionGemmaParser = struct {
    allocator: std.mem.Allocator,
    tokens: FunctionGemmaTokens,
    owns_tokens: bool = false,
    buffer: std.ArrayListUnmanaged(u8) = .empty,
    text_segments: std.ArrayListUnmanaged(u8) = .empty,
    tool_calls: std.ArrayListUnmanaged(ToolCall) = .empty,
    emitted_text_len: usize = 0,
    emitted_call_count: usize = 0,
    active_call: ?ActiveCall = null,
    pending_argument_delta: ?[]u8 = null,
    completed_tool_delta: ?ToolCallDeltaUpdate = null,

    fn deinit(self: *FunctionGemmaParser) void {
        if (self.owns_tokens) {
            self.allocator.free(self.tokens.start_function_decl);
            self.allocator.free(self.tokens.end_function_decl);
            self.allocator.free(self.tokens.start_function_call);
            self.allocator.free(self.tokens.end_function_call);
            self.allocator.free(self.tokens.escape);
        }
        self.buffer.deinit(self.allocator);
        self.text_segments.deinit(self.allocator);
        for (self.tool_calls.items) |*call| call.deinit(self.allocator);
        self.tool_calls.deinit(self.allocator);
        if (self.active_call) |*call| call.deinit(self.allocator);
        if (self.pending_argument_delta) |delta| self.allocator.free(delta);
        self.completed_tool_delta = null;
        self.* = undefined;
    }

    fn reset(self: *FunctionGemmaParser) void {
        self.buffer.clearRetainingCapacity();
        self.text_segments.clearRetainingCapacity();
        for (self.tool_calls.items) |*call| call.deinit(self.allocator);
        self.tool_calls.clearRetainingCapacity();
        self.emitted_text_len = 0;
        self.emitted_call_count = 0;
        if (self.active_call) |*call| {
            call.deinit(self.allocator);
            self.active_call = null;
        }
        if (self.pending_argument_delta) |delta| {
            self.allocator.free(delta);
            self.pending_argument_delta = null;
        }
        self.completed_tool_delta = null;
    }

    fn formatToolsPrompt(self: *FunctionGemmaParser, allocator: std.mem.Allocator, tools: []const ToolDefinition) ![]u8 {
        if (isGemma4ToolTokens(self.tokens)) {
            return self.formatGemma4ToolsPrompt(allocator, tools);
        }

        var buf = std.ArrayListUnmanaged(u8).empty;
        errdefer buf.deinit(allocator);

        try buf.appendSlice(allocator, "You are a model that can do function calling with the following functions.\n\n");
        for (tools) |tool| {
            try buf.appendSlice(allocator, self.tokens.start_function_decl);
            try buf.appendSlice(allocator, "declaration:");
            try buf.appendSlice(allocator, tool.function.name);
            try buf.appendSlice(allocator, "{description:");
            try buf.appendSlice(allocator, self.tokens.escape);
            try buf.appendSlice(allocator, tool.function.description);
            try buf.appendSlice(allocator, self.tokens.escape);
            try buf.appendSlice(allocator, ",parameters:");
            try self.formatParams(&buf, allocator, tool.function.parameters);
            try buf.appendSlice(allocator, "}");
            try buf.appendSlice(allocator, self.tokens.end_function_decl);
            try buf.append(allocator, '\n');
        }

        try buf.appendSlice(allocator, "\nWhen you want to call a function, output in this format:\n");
        try buf.appendSlice(allocator, self.tokens.start_function_call);
        try buf.appendSlice(allocator, "function_name{param1:");
        try buf.appendSlice(allocator, self.tokens.escape);
        try buf.appendSlice(allocator, "value1");
        try buf.appendSlice(allocator, self.tokens.escape);
        try buf.appendSlice(allocator, ",param2:");
        try buf.appendSlice(allocator, self.tokens.escape);
        try buf.appendSlice(allocator, "value2");
        try buf.appendSlice(allocator, self.tokens.escape);
        try buf.appendSlice(allocator, "}");
        try buf.appendSlice(allocator, self.tokens.end_function_call);
        try buf.append(allocator, '\n');

        return try buf.toOwnedSlice(allocator);
    }

    fn formatGemma4ToolsPrompt(self: *FunctionGemmaParser, allocator: std.mem.Allocator, tools: []const ToolDefinition) ![]u8 {
        var buf = std.ArrayListUnmanaged(u8).empty;
        errdefer buf.deinit(allocator);

        try buf.appendSlice(allocator, "You are a model that can call tools.\n");
        try buf.appendSlice(allocator, "Available tools:\n");
        for (tools) |tool| {
            try buf.appendSlice(allocator, self.tokens.start_function_decl);
            try buf.appendSlice(allocator, tool.function.name);
            try buf.appendSlice(allocator, "{description:");
            try buf.appendSlice(allocator, self.tokens.escape);
            try buf.appendSlice(allocator, tool.function.description);
            try buf.appendSlice(allocator, self.tokens.escape);
            try buf.appendSlice(allocator, ",parameters:");
            try self.formatParams(&buf, allocator, tool.function.parameters);
            try buf.append(allocator, '}');
            try buf.appendSlice(allocator, self.tokens.end_function_decl);
            try buf.append(allocator, '\n');
        }

        try buf.appendSlice(allocator, "When you call a tool, output only this format:\n");
        try buf.appendSlice(allocator, self.tokens.start_function_call);
        try buf.appendSlice(allocator, "call:function_name{param1:");
        try buf.appendSlice(allocator, self.tokens.escape);
        try buf.appendSlice(allocator, "value1");
        try buf.appendSlice(allocator, self.tokens.escape);
        try buf.append(allocator, '}');
        try buf.appendSlice(allocator, self.tokens.end_function_call);
        try buf.append(allocator, '\n');

        return try buf.toOwnedSlice(allocator);
    }

    fn formatParams(self: *FunctionGemmaParser, buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, parameters: ?std.json.Value) !void {
        if (parameters == null or parameters.? != .object) {
            try buf.appendSlice(allocator, "{}");
            return;
        }

        const params = parameters.?.object;
        const props_val = params.get("properties") orelse {
            try buf.appendSlice(allocator, "{}");
            return;
        };
        if (props_val != .object) {
            try buf.appendSlice(allocator, "{}");
            return;
        }

        try buf.append(allocator, '{');
        var first = true;
        var it = props_val.object.iterator();
        while (it.next()) |entry| {
            const prop_name = entry.key_ptr.*;
            const prop_val = entry.value_ptr.*;
            if (prop_val != .object) continue;
            if (!first) try buf.append(allocator, ',');
            first = false;

            try buf.appendSlice(allocator, prop_name);
            try buf.appendSlice(allocator, ":{type:");
            if (prop_val.object.get("type")) |type_val| {
                if (type_val == .string) try buf.appendSlice(allocator, type_val.string) else try buf.appendSlice(allocator, "any");
            } else {
                try buf.appendSlice(allocator, "any");
            }
            if (isRequired(params, prop_name)) {
                try buf.appendSlice(allocator, ",required:true");
            }
            if (prop_val.object.get("description")) |desc_val| {
                if (desc_val == .string) {
                    try buf.appendSlice(allocator, ",description:");
                    try buf.appendSlice(allocator, self.tokens.escape);
                    try buf.appendSlice(allocator, desc_val.string);
                    try buf.appendSlice(allocator, self.tokens.escape);
                }
            }
            try buf.append(allocator, '}');
        }
        try buf.append(allocator, '}');
    }

    fn feed(self: *FunctionGemmaParser, chunk: []const u8) !FeedUpdate {
        if (self.pending_argument_delta) |delta| {
            self.allocator.free(delta);
            self.pending_argument_delta = null;
        }
        self.completed_tool_delta = null;
        try self.buffer.appendSlice(self.allocator, chunk);
        _ = try self.parseBareGemma4CallMaybe();

        while (true) {
            const content = self.buffer.items;
            const start_idx = std.mem.indexOf(u8, content, self.tokens.start_function_call) orelse break;
            const end_search_start = start_idx + self.tokens.start_function_call.len;
            const end_idx = std.mem.indexOfPos(u8, content, end_search_start, self.tokens.end_function_call) orelse break;

            if (start_idx > 0) {
                try self.text_segments.appendSlice(self.allocator, content[0..start_idx]);
            }

            const call_content = content[end_search_start..end_idx];
            _ = try self.parseCall(call_content);

            const remaining_start = end_idx + self.tokens.end_function_call.len;
            compactBufferPrefix(&self.buffer, remaining_start);
        }

        const flush_len = if (isGemma4ToolTokens(self.tokens))
            safeGemma4FlushPrefixLen(self.buffer.items, self.tokens.start_function_call)
        else
            safeFlushPrefixLen(self.buffer.items, self.tokens.start_function_call);
        if (flush_len > 0) {
            try self.text_segments.appendSlice(self.allocator, self.buffer.items[0..flush_len]);
            compactBufferPrefix(&self.buffer, flush_len);
        }

        const update = FeedUpdate{
            .ready_text = self.text_segments.items[self.emitted_text_len..],
            .new_calls = self.tool_calls.items[self.emitted_call_count..],
            .call_start_index = self.emitted_call_count,
            .active_tool_delta = if (self.completed_tool_delta) |delta| delta else try self.activeToolDelta(),
        };
        self.emitted_text_len = self.text_segments.items.len;
        self.emitted_call_count = self.tool_calls.items.len;
        return update;
    }

    fn parseBareGemma4CallMaybe(self: *FunctionGemmaParser) !bool {
        if (!isGemma4ToolTokens(self.tokens)) return false;
        const trimmed = std.mem.trim(u8, self.buffer.items, &std.ascii.whitespace);
        if (!std.mem.startsWith(u8, trimmed, "call:")) return false;
        const brace_idx = std.mem.indexOfScalar(u8, trimmed, '{') orelse return false;
        const close_rel = std.mem.indexOfScalar(u8, trimmed[brace_idx + 1 ..], '}') orelse return false;
        const close_idx = brace_idx + 1 + close_rel;
        if (!try self.parseCall(trimmed[0 .. close_idx + 1])) return false;
        self.buffer.clearRetainingCapacity();
        return true;
    }

    fn finishText(self: *FunctionGemmaParser, allocator: std.mem.Allocator) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(allocator);
        try out.appendSlice(allocator, self.text_segments.items);
        try out.appendSlice(allocator, self.buffer.items);
        return try out.toOwnedSlice(allocator);
    }

    fn finishRemainingText(self: *FunctionGemmaParser, allocator: std.mem.Allocator) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(allocator);
        try out.appendSlice(allocator, self.text_segments.items[self.emitted_text_len..]);
        try out.appendSlice(allocator, self.buffer.items);
        self.emitted_text_len = self.text_segments.items.len;
        return try out.toOwnedSlice(allocator);
    }

    fn parseCall(self: *FunctionGemmaParser, content: []const u8) !bool {
        const brace_idx = std.mem.indexOfScalar(u8, content, '{') orelse return false;
        const func_name = parseFunctionGemmaCallName(content[0..brace_idx]) orelse return false;
        if (func_name.len == 0) return false;

        var params_text = content[brace_idx + 1 ..];
        if (params_text.len > 0 and params_text[params_text.len - 1] == '}') {
            params_text = params_text[0 .. params_text.len - 1];
        }

        var params = std.ArrayListUnmanaged(ParsedParam).empty;
        defer {
            for (params.items) |*param| param.deinit(self.allocator);
            params.deinit(self.allocator);
        }
        try self.splitParams(params_text, &params);
        const arguments = try buildArgumentsJson(self.allocator, params.items);
        errdefer self.allocator.free(arguments);

        var tool_call = ToolCall{
            .id = undefined,
            .type = "function",
            .function = .{
                .name = undefined,
                .arguments = arguments,
            },
        };
        if (self.active_call) |active| {
            if (std.mem.eql(u8, active.name, func_name)) {
                tool_call.id = active.id;
                tool_call.function.name = active.name;
                const suffix = if (arguments.len > active.emitted_args_len)
                    try self.allocator.dupe(u8, arguments[active.emitted_args_len..])
                else
                    null;
                if (suffix) |delta_args| {
                    self.pending_argument_delta = delta_args;
                    self.completed_tool_delta = .{
                        .index = self.tool_calls.items.len,
                        .arguments = delta_args,
                    };
                }
                self.active_call = null;
            } else {
                var stale = active;
                stale.deinit(self.allocator);
                self.active_call = null;
                tool_call.id = try generateCallId(self.allocator);
                tool_call.function.name = try self.allocator.dupe(u8, func_name);
                self.completed_tool_delta = .{
                    .index = self.tool_calls.items.len,
                    .id = tool_call.id,
                    .type = tool_call.type,
                    .name = tool_call.function.name,
                    .arguments = tool_call.function.arguments,
                };
            }
        } else {
            tool_call.id = try generateCallId(self.allocator);
            tool_call.function.name = try self.allocator.dupe(u8, func_name);
            self.completed_tool_delta = .{
                .index = self.tool_calls.items.len,
                .id = tool_call.id,
                .type = tool_call.type,
                .name = tool_call.function.name,
                .arguments = tool_call.function.arguments,
            };
        }
        errdefer self.allocator.free(tool_call.id);
        errdefer self.allocator.free(tool_call.function.name);

        try self.tool_calls.append(self.allocator, tool_call);
        return true;
    }

    fn splitParams(self: *FunctionGemmaParser, input: []const u8, params: *std.ArrayListUnmanaged(ParsedParam)) !void {
        if (input.len == 0) return;

        var current = std.ArrayListUnmanaged(u8).empty;
        defer current.deinit(self.allocator);

        var in_escape = false;
        var i: usize = 0;
        while (i < input.len) {
            if (std.mem.startsWith(u8, input[i..], self.tokens.escape)) {
                in_escape = !in_escape;
                i += self.tokens.escape.len;
                continue;
            }

            if (!in_escape and input[i] == ',') {
                try parseParam(self.allocator, current.items, params);
                current.clearRetainingCapacity();
                i += 1;
                continue;
            }

            try current.append(self.allocator, input[i]);
            i += 1;
        }

        if (current.items.len > 0) {
            try parseParam(self.allocator, current.items, params);
        }
    }

    fn activeToolDelta(self: *FunctionGemmaParser) !?ToolCallDeltaUpdate {
        if (!std.mem.startsWith(u8, self.buffer.items, self.tokens.start_function_call)) return null;
        const partial = self.buffer.items[self.tokens.start_function_call.len..];
        const brace_idx = std.mem.indexOfScalar(u8, partial, '{') orelse return null;
        const func_name = parseFunctionGemmaCallName(partial[0..brace_idx]) orelse return null;
        if (func_name.len == 0) return null;

        if (self.active_call == null) {
            self.active_call = .{
                .id = try generateCallId(self.allocator),
                .name = try self.allocator.dupe(u8, func_name),
            };
        }
        if (!std.mem.eql(u8, self.active_call.?.name, func_name)) return null;

        var delta = ToolCallDeltaUpdate{
            .index = self.tool_calls.items.len,
        };
        if (!self.active_call.?.emitted_name) {
            delta.id = self.active_call.?.id;
            delta.type = "function";
            delta.name = self.active_call.?.name;
            self.active_call.?.emitted_name = true;
        }

        const args_snapshot = try buildPartialArgumentsJson(self.allocator, partial[brace_idx + 1 ..], self.tokens.escape);
        defer self.allocator.free(args_snapshot);
        if (args_snapshot.len > self.active_call.?.emitted_args_len) {
            self.pending_argument_delta = try self.allocator.dupe(u8, args_snapshot[self.active_call.?.emitted_args_len..]);
            self.active_call.?.emitted_args_len = args_snapshot.len;
            delta.arguments = self.pending_argument_delta.?;
        }

        if (delta.id == null and delta.arguments == null and delta.name == null) return null;
        return delta;
    }
};

fn isGemma4ToolTokens(tokens: FunctionGemmaTokens) bool {
    return tokens.format == .gemma4;
}

fn safeGemma4FlushPrefixLen(buffer: []const u8, start_token: []const u8) usize {
    if (buffer.len == 0) return 0;

    var trimmed_start: usize = 0;
    while (trimmed_start < buffer.len and std.ascii.isWhitespace(buffer[trimmed_start])) : (trimmed_start += 1) {}
    const trimmed = buffer[trimmed_start..];
    const bare_call_prefix = "call:";
    if (std.mem.startsWith(u8, bare_call_prefix, trimmed) or
        std.mem.startsWith(u8, trimmed, bare_call_prefix))
    {
        return trimmed_start;
    }

    return safeFlushPrefixLen(buffer, start_token);
}

fn parseFunctionGemmaCallName(prefix: []const u8) ?[]const u8 {
    const trimmed = std.mem.trim(u8, prefix, &std.ascii.whitespace);
    if (trimmed.len == 0) return null;
    if (std.mem.startsWith(u8, trimmed, "call:")) {
        const named = std.mem.trim(u8, trimmed["call:".len..], &std.ascii.whitespace);
        if (named.len == 0) return null;
        return named;
    }
    return trimmed;
}

const JsonToolParser = struct {
    allocator: std.mem.Allocator,
    buffer: std.ArrayListUnmanaged(u8) = .empty,
    tool_calls: std.ArrayListUnmanaged(ToolCall) = .empty,
    emitted_call_count: usize = 0,

    fn deinit(self: *JsonToolParser) void {
        self.buffer.deinit(self.allocator);
        for (self.tool_calls.items) |*call| call.deinit(self.allocator);
        self.tool_calls.deinit(self.allocator);
        self.* = undefined;
    }

    fn reset(self: *JsonToolParser) void {
        self.buffer.clearRetainingCapacity();
        for (self.tool_calls.items) |*call| call.deinit(self.allocator);
        self.tool_calls.clearRetainingCapacity();
        self.emitted_call_count = 0;
    }

    fn formatToolsPrompt(self: *JsonToolParser, allocator: std.mem.Allocator, tools: []const ToolDefinition) ![]u8 {
        _ = self;
        var buf = std.ArrayListUnmanaged(u8).empty;
        errdefer buf.deinit(allocator);

        try buf.appendSlice(allocator, "You are a model that can call functions.\n");
        try buf.appendSlice(allocator, "When you need to call a function, respond with JSON only and no extra text.\n");
        try buf.appendSlice(allocator, "Allowed response formats:\n");
        try buf.appendSlice(allocator, "{\"name\":\"function_name\",\"arguments\":{...}}\n");
        try buf.appendSlice(allocator, "{\"tool_calls\":[{\"name\":\"function_name\",\"arguments\":{...}}]}\n");
        try buf.appendSlice(allocator, "Available functions:\n");

        for (tools) |tool| {
            try buf.appendSlice(allocator, "- ");
            try appendJsonString(&buf, allocator, tool.function.name);
            if (tool.function.description.len > 0) {
                try buf.appendSlice(allocator, " description=");
                try appendJsonString(&buf, allocator, tool.function.description);
            }
            try buf.appendSlice(allocator, " parameters=");
            if (tool.function.parameters) |parameters| {
                try appendJsonValueFromValue(&buf, allocator, parameters);
            } else {
                try buf.appendSlice(allocator, "{}");
            }
            try buf.append(allocator, '\n');
        }

        return try buf.toOwnedSlice(allocator);
    }

    fn feed(self: *JsonToolParser, chunk: []const u8) !FeedUpdate {
        try self.buffer.appendSlice(self.allocator, chunk);
        try self.parseBufferedCallsMaybe();

        const update = FeedUpdate{
            .new_calls = self.tool_calls.items[self.emitted_call_count..],
            .call_start_index = self.emitted_call_count,
        };
        self.emitted_call_count = self.tool_calls.items.len;
        return update;
    }

    fn finishText(self: *JsonToolParser, allocator: std.mem.Allocator) ![]u8 {
        return try allocator.dupe(u8, self.buffer.items);
    }

    fn finishRemainingText(self: *JsonToolParser, allocator: std.mem.Allocator) ![]u8 {
        return try self.finishText(allocator);
    }

    fn parseBufferedCallsMaybe(self: *JsonToolParser) !void {
        const trimmed = std.mem.trim(u8, self.buffer.items, &std.ascii.whitespace);
        if (trimmed.len == 0) return;

        const parsed = std.json.parseFromSlice(std.json.Value, self.allocator, trimmed, .{}) catch |err| switch (err) {
            error.SyntaxError, error.UnexpectedEndOfInput, error.InvalidNumber => return,
            else => return err,
        };
        defer parsed.deinit();

        var parsed_calls = std.ArrayListUnmanaged(ToolCall).empty;
        defer parsed_calls.deinit(self.allocator);
        errdefer {
            for (parsed_calls.items) |*call| call.deinit(self.allocator);
        }

        const found_call = try appendToolCallsFromJsonValue(self.allocator, parsed.value, &parsed_calls);
        if (!found_call or parsed_calls.items.len == 0) return;

        for (parsed_calls.items) |call| try self.tool_calls.append(self.allocator, call);
        parsed_calls.items.len = 0;
        self.buffer.clearRetainingCapacity();
    }
};

const ActiveCall = struct {
    id: []const u8,
    name: []const u8,
    emitted_name: bool = false,
    emitted_args_len: usize = 0,

    fn deinit(self: *ActiveCall, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.name);
        self.* = undefined;
    }
};

fn safeFlushPrefixLen(buffer: []const u8, start_token: []const u8) usize {
    if (buffer.len == 0) return 0;
    if (std.mem.indexOf(u8, buffer, start_token)) |start_idx| {
        return start_idx;
    }

    const max_keep = @min(buffer.len, start_token.len - 1);
    var keep: usize = 0;
    var candidate: usize = max_keep;
    while (candidate > 0) : (candidate -= 1) {
        if (std.mem.eql(u8, buffer[buffer.len - candidate ..], start_token[0..candidate])) {
            keep = candidate;
            break;
        }
    }
    return buffer.len - keep;
}

fn compactBufferPrefix(buffer: *std.ArrayListUnmanaged(u8), prefix_len: usize) void {
    if (prefix_len == 0) return;
    if (prefix_len >= buffer.items.len) {
        buffer.clearRetainingCapacity();
        return;
    }

    const remaining_len = buffer.items.len - prefix_len;
    std.mem.copyForwards(u8, buffer.items[0..remaining_len], buffer.items[prefix_len..]);
    buffer.items.len = remaining_len;
}

pub const Parser = union(enum) {
    functiongemma: FunctionGemmaParser,
    json: JsonToolParser,

    pub fn deinit(self: *Parser) void {
        switch (self.*) {
            .functiongemma => |*parser| parser.deinit(),
            .json => |*parser| parser.deinit(),
        }
    }

    pub fn reset(self: *Parser) void {
        switch (self.*) {
            .functiongemma => |*parser| parser.reset(),
            .json => |*parser| parser.reset(),
        }
    }

    pub fn name(self: *const Parser) []const u8 {
        return switch (self.*) {
            .functiongemma => "functiongemma",
            .json => "json",
        };
    }

    pub fn formatToolsPrompt(self: *Parser, allocator: std.mem.Allocator, tools: []const ToolDefinition) ![]u8 {
        return switch (self.*) {
            .functiongemma => |*parser| parser.formatToolsPrompt(allocator, tools),
            .json => |*parser| parser.formatToolsPrompt(allocator, tools),
        };
    }

    pub fn feed(self: *Parser, chunk: []const u8) !FeedUpdate {
        return switch (self.*) {
            .functiongemma => |*parser| parser.feed(chunk),
            .json => |*parser| parser.feed(chunk),
        };
    }

    pub fn finishText(self: *Parser, allocator: std.mem.Allocator) ![]u8 {
        return switch (self.*) {
            .functiongemma => |*parser| parser.finishText(allocator),
            .json => |*parser| parser.finishText(allocator),
        };
    }

    pub fn finishRemainingText(self: *Parser, allocator: std.mem.Allocator) ![]u8 {
        return switch (self.*) {
            .functiongemma => |*parser| parser.finishRemainingText(allocator),
            .json => |*parser| parser.finishRemainingText(allocator),
        };
    }

    pub fn toolCalls(self: *const Parser) []const ToolCall {
        return switch (self.*) {
            .functiongemma => |parser| parser.tool_calls.items,
            .json => |parser| parser.tool_calls.items,
        };
    }

    pub fn streamsIncrementalToolDeltas(self: *const Parser) bool {
        return switch (self.*) {
            .functiongemma => true,
            .json => false,
        };
    }
};

pub fn parseToolChoice(choice: ?std.json.Value) !ParsedToolChoice {
    if (choice == null) return .auto;
    switch (choice.?) {
        .string => |s| {
            if (std.mem.eql(u8, s, "auto")) return .auto;
            if (std.mem.eql(u8, s, "none")) return .none;
            if (std.mem.eql(u8, s, "required")) return .required;
            return error.InvalidToolChoice;
        },
        .object => |obj| {
            const type_val = obj.get("type") orelse return error.InvalidToolChoice;
            if (type_val != .string or !std.mem.eql(u8, type_val.string, "function")) {
                return error.InvalidToolChoice;
            }
            const fn_val = obj.get("function") orelse return error.InvalidToolChoice;
            if (fn_val != .object) return error.InvalidToolChoice;
            const name_val = fn_val.object.get("name") orelse return error.InvalidToolChoice;
            if (name_val != .string or name_val.string.len == 0) return error.InvalidToolChoice;
            return .{ .function = name_val.string };
        },
        else => return error.InvalidToolChoice,
    }
}

pub fn forcedFunctionName(choice: ParsedToolChoice) ?[]const u8 {
    return switch (choice) {
        .function => |name| name,
        else => null,
    };
}

pub fn toolCallsEnabled(choice: ParsedToolChoice) bool {
    return switch (choice) {
        .none => false,
        else => true,
    };
}

pub fn loadParser(allocator: std.mem.Allocator, model_dir: []const u8) !?Parser {
    if (try loadConfiguredToolCallFormat(allocator, model_dir)) |format| {
        defer allocator.free(format);
        if (std.mem.eql(u8, format, "functiongemma")) {
            return .{ .functiongemma = .{
                .allocator = allocator,
                .tokens = try loadFunctionGemmaTokens(allocator, model_dir),
                .owns_tokens = true,
            } };
        }
        if (std.mem.eql(u8, format, "json")) {
            return .{ .json = .{
                .allocator = allocator,
            } };
        }
        return error.UnknownToolCallFormat;
    }

    const tokens = loadFunctionGemmaTokens(allocator, model_dir) catch return null;
    return .{ .functiongemma = .{
        .allocator = allocator,
        .tokens = tokens,
        .owns_tokens = true,
    } };
}

fn appendToolCallsFromJsonValue(
    allocator: std.mem.Allocator,
    value: std.json.Value,
    out: *std.ArrayListUnmanaged(ToolCall),
) !bool {
    switch (value) {
        .object => |obj| {
            if (obj.get("tool_calls")) |tool_calls_val| {
                if (tool_calls_val != .array) return false;
                const start_len = out.items.len;
                for (tool_calls_val.array.items) |item| {
                    if (!try appendSingleToolCallFromJsonValue(allocator, item, out)) return false;
                }
                return out.items.len > start_len;
            }
            return try appendSingleToolCallFromJsonValue(allocator, value, out);
        },
        .array => |arr| {
            const start_len = out.items.len;
            for (arr.items) |item| {
                if (!try appendSingleToolCallFromJsonValue(allocator, item, out)) return false;
            }
            return out.items.len > start_len;
        },
        else => return false,
    }
}

fn appendSingleToolCallFromJsonValue(
    allocator: std.mem.Allocator,
    value: std.json.Value,
    out: *std.ArrayListUnmanaged(ToolCall),
) !bool {
    if (value != .object) return false;

    const obj = value.object;
    var name: ?[]const u8 = null;
    var call_id: ?[]const u8 = null;
    var call_type: []const u8 = "function";

    if (obj.get("id")) |id_val| {
        if (id_val != .string) return false;
        call_id = id_val.string;
    }
    if (obj.get("type")) |type_val| {
        if (type_val != .string) return false;
        call_type = type_val.string;
    }

    if (obj.get("function")) |function_val| {
        if (function_val != .object) return false;
        const fn_obj = function_val.object;
        const name_val = fn_obj.get("name") orelse return false;
        if (name_val != .string or name_val.string.len == 0) return false;
        name = name_val.string;
    } else {
        const name_val = obj.get("name") orelse return false;
        if (name_val != .string or name_val.string.len == 0) return false;
        name = name_val.string;
    }

    const call_name = name.?;
    const raw_arguments = if (obj.get("function")) |function_val| blk: {
        const fn_obj = function_val.object;
        if (fn_obj.get("arguments")) |arguments| break :blk try toolArgumentsToOwnedSlice(allocator, arguments);
        break :blk try allocator.dupe(u8, "{}");
    } else if (obj.get("arguments")) |arguments|
        try toolArgumentsToOwnedSlice(allocator, arguments)
    else
        try allocator.dupe(u8, "{}");
    errdefer allocator.free(raw_arguments);
    const owned_id = if (call_id) |id| try allocator.dupe(u8, id) else try generateCallId(allocator);
    errdefer allocator.free(owned_id);
    const owned_name = try allocator.dupe(u8, call_name);
    errdefer allocator.free(owned_name);

    try out.append(allocator, .{
        .id = owned_id,
        .type = call_type,
        .function = .{
            .name = owned_name,
            .arguments = raw_arguments,
        },
    });
    return true;
}

fn isRequired(params: std.json.ObjectMap, name: []const u8) bool {
    const required_val = params.get("required") orelse return false;
    if (required_val != .array) return false;
    for (required_val.array.items) |item| {
        if (item == .string and std.mem.eql(u8, item.string, name)) return true;
    }
    return false;
}

const ParsedParam = struct {
    key: []const u8,
    raw_value: []const u8,

    fn deinit(self: *ParsedParam, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        allocator.free(self.raw_value);
        self.* = undefined;
    }
};

fn parseParam(allocator: std.mem.Allocator, text: []const u8, params: *std.ArrayListUnmanaged(ParsedParam)) !void {
    const trimmed = std.mem.trim(u8, text, &std.ascii.whitespace);
    if (trimmed.len == 0) return;
    const colon_idx = std.mem.indexOfScalar(u8, trimmed, ':') orelse return;
    const key = std.mem.trim(u8, trimmed[0..colon_idx], &std.ascii.whitespace);
    const value = std.mem.trim(u8, trimmed[colon_idx + 1 ..], &std.ascii.whitespace);
    if (key.len == 0) return;

    try params.append(allocator, .{
        .key = try allocator.dupe(u8, key),
        .raw_value = try allocator.dupe(u8, value),
    });
}

fn buildArgumentsJson(allocator: std.mem.Allocator, params: []const ParsedParam) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(allocator);

    try buf.append(allocator, '{');
    for (params, 0..) |param, idx| {
        if (idx > 0) try buf.append(allocator, ',');
        try appendJsonString(&buf, allocator, param.key);
        try buf.append(allocator, ':');
        try appendJsonValue(&buf, allocator, param.raw_value);
    }
    try buf.append(allocator, '}');

    return try buf.toOwnedSlice(allocator);
}

fn jsonValueToOwnedSlice(allocator: std.mem.Allocator, value: std.json.Value) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(allocator);
    try appendJsonValueFromValue(&buf, allocator, value);
    return try buf.toOwnedSlice(allocator);
}

fn toolArgumentsToOwnedSlice(allocator: std.mem.Allocator, value: std.json.Value) ![]u8 {
    if (value == .string) {
        const parsed = std.json.parseFromSlice(std.json.Value, allocator, value.string, .{}) catch {
            return try jsonValueToOwnedSlice(allocator, value);
        };
        parsed.deinit();
        return try allocator.dupe(u8, value.string);
    }
    return try jsonValueToOwnedSlice(allocator, value);
}

fn appendJsonValueFromValue(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: std.json.Value) !void {
    switch (value) {
        .null => try buf.appendSlice(allocator, "null"),
        .bool => |b| try buf.appendSlice(allocator, if (b) "true" else "false"),
        .integer => |i| {
            const rendered = try std.fmt.allocPrint(allocator, "{d}", .{i});
            defer allocator.free(rendered);
            try buf.appendSlice(allocator, rendered);
        },
        .float => |f| {
            const rendered = try std.fmt.allocPrint(allocator, "{d}", .{f});
            defer allocator.free(rendered);
            try buf.appendSlice(allocator, rendered);
        },
        .number_string => |s| try buf.appendSlice(allocator, s),
        .string => |s| try appendJsonString(buf, allocator, s),
        .array => |arr| {
            try buf.append(allocator, '[');
            for (arr.items, 0..) |item, idx| {
                if (idx > 0) try buf.append(allocator, ',');
                try appendJsonValueFromValue(buf, allocator, item);
            }
            try buf.append(allocator, ']');
        },
        .object => |obj| {
            try buf.append(allocator, '{');
            var it = obj.iterator();
            var idx: usize = 0;
            while (it.next()) |entry| : (idx += 1) {
                if (idx > 0) try buf.append(allocator, ',');
                try appendJsonString(buf, allocator, entry.key_ptr.*);
                try buf.append(allocator, ':');
                try appendJsonValueFromValue(buf, allocator, entry.value_ptr.*);
            }
            try buf.append(allocator, '}');
        },
    }
}

fn buildPartialArgumentsJson(allocator: std.mem.Allocator, raw_params: []const u8, escape_token: []const u8) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(allocator);
    try buf.append(allocator, '{');

    var i: usize = 0;
    var first = true;
    while (i < raw_params.len) {
        while (i < raw_params.len and std.ascii.isWhitespace(raw_params[i])) : (i += 1) {}
        if (i >= raw_params.len or raw_params[i] == '}') break;

        const key_start = i;
        while (i < raw_params.len and raw_params[i] != ':') : (i += 1) {}
        if (i >= raw_params.len) break;
        const key = std.mem.trim(u8, raw_params[key_start..i], &std.ascii.whitespace);
        i += 1;
        while (i < raw_params.len and std.ascii.isWhitespace(raw_params[i])) : (i += 1) {}
        if (key.len == 0) break;

        var in_escape = false;
        var saw_escape = false;
        var value_buf = std.ArrayListUnmanaged(u8).empty;
        defer value_buf.deinit(allocator);
        while (i < raw_params.len) {
            if (std.mem.startsWith(u8, raw_params[i..], escape_token)) {
                in_escape = !in_escape;
                saw_escape = true;
                i += escape_token.len;
                continue;
            }
            if (!in_escape and (raw_params[i] == ',' or raw_params[i] == '}')) break;
            try value_buf.append(allocator, raw_params[i]);
            i += 1;
        }
        const value = std.mem.trim(u8, value_buf.items, &std.ascii.whitespace);
        if (!saw_escape and value.len == 0) break;
        if (saw_escape and value.len == 0) break;

        if (!first) try buf.append(allocator, ',');
        first = false;
        try appendJsonString(&buf, allocator, key);
        try buf.append(allocator, ':');
        try appendPartialJsonValue(&buf, allocator, value, saw_escape, in_escape);

        if (i < raw_params.len and raw_params[i] == ',') {
            i += 1;
            continue;
        }
        break;
    }

    return try buf.toOwnedSlice(allocator);
}

fn appendJsonValue(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, raw: []const u8) !void {
    const trimmed = std.mem.trim(u8, raw, &std.ascii.whitespace);
    if (trimmed.len == 0) {
        try buf.appendSlice(allocator, "null");
        return;
    }

    if (std.mem.eql(u8, trimmed, "true") or std.mem.eql(u8, trimmed, "false") or std.mem.eql(u8, trimmed, "null")) {
        try buf.appendSlice(allocator, trimmed);
        return;
    }
    if (looksNumeric(trimmed)) {
        try buf.appendSlice(allocator, trimmed);
        return;
    }
    if ((trimmed[0] == '{' and trimmed[trimmed.len - 1] == '}') or
        (trimmed[0] == '[' and trimmed[trimmed.len - 1] == ']') or
        (trimmed[0] == '"' and trimmed[trimmed.len - 1] == '"'))
    {
        if (std.json.parseFromSlice(std.json.Value, allocator, trimmed, .{})) |parsed| {
            defer parsed.deinit();
            try buf.appendSlice(allocator, trimmed);
            return;
        } else |_| {}
    }

    try appendJsonString(buf, allocator, trimmed);
}

fn appendPartialJsonValue(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    raw: []const u8,
    saw_escape: bool,
    in_escape: bool,
) !void {
    const trimmed = std.mem.trim(u8, raw, &std.ascii.whitespace);
    if (!saw_escape) {
        if (trimmed.len == 0) {
            try buf.appendSlice(allocator, "null");
            return;
        }
        try buf.appendSlice(allocator, trimmed);
        return;
    }

    try buf.append(allocator, '"');
    var i: usize = 0;
    while (i < trimmed.len) {
        const ch = trimmed[i];
        switch (ch) {
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => try buf.append(allocator, ch),
        }
        i += 1;
    }
    if (!in_escape) try buf.append(allocator, '"');
}

fn looksNumeric(text: []const u8) bool {
    if (text.len == 0) return false;
    _ = std.fmt.parseFloat(f64, text) catch return false;
    return true;
}

fn generateCallId(allocator: std.mem.Allocator) ![]u8 {
    const id = tool_call_counter.fetchAdd(1, .monotonic) + 1;
    return try std.fmt.allocPrint(allocator, "call_{x}", .{id});
}

fn loadConfiguredToolCallFormat(allocator: std.mem.Allocator, model_dir: []const u8) !?[]u8 {
    const bytes = c_file.readFileFromDir(allocator, model_dir, "genai_config.json") catch return null;
    defer allocator.free(bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const format_val = parsed.value.object.get("tool_call_format") orelse return null;
    if (format_val != .string or format_val.string.len == 0) return null;
    return try allocator.dupe(u8, format_val.string);
}

fn loadFunctionGemmaTokens(allocator: std.mem.Allocator, model_dir: []const u8) !FunctionGemmaTokens {
    return loadFunctionGemmaTokensFromFile(allocator, model_dir, "special_tokens_map.json") catch
        loadFunctionGemmaTokensFromFile(allocator, model_dir, "tokenizer_config.json") catch
        loadToolTokensFromGguf(allocator, model_dir);
}

fn loadFunctionGemmaTokensFromFile(allocator: std.mem.Allocator, model_dir: []const u8, file_name: []const u8) !FunctionGemmaTokens {
    const bytes = try c_file.readFileFromDir(allocator, model_dir, file_name);
    defer allocator.free(bytes);
    const saw_function_call_tokens =
        std.mem.indexOf(u8, bytes, "start_function_call") != null and
        std.mem.indexOf(u8, bytes, "end_function_call") != null;
    const saw_gemma4_tool_tokens =
        std.mem.indexOf(u8, bytes, "stc_token") != null and
        std.mem.indexOf(u8, bytes, "etc_token") != null and
        std.mem.indexOf(u8, bytes, "gemma4-tool-call") != null;

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.MissingFunctionGemmaTokens;

    var tokens = FunctionGemmaTokens{
        .start_function_decl = try allocator.dupe(u8, "<start_function_declaration>"),
        .end_function_decl = try allocator.dupe(u8, "<end_function_declaration>"),
        .start_function_call = try allocator.dupe(u8, "<start_function_call>"),
        .end_function_call = try allocator.dupe(u8, "<end_function_call>"),
        .escape = try allocator.dupe(u8, "<escape>"),
    };
    errdefer {
        allocator.free(tokens.start_function_decl);
        allocator.free(tokens.end_function_decl);
        allocator.free(tokens.start_function_call);
        allocator.free(tokens.end_function_call);
        allocator.free(tokens.escape);
    }
    tokens.start_function_decl = try replaceTokenMaybe(allocator, tokens.start_function_decl, parsed.value.object, "start_function_declaration");
    tokens.end_function_decl = try replaceTokenMaybe(allocator, tokens.end_function_decl, parsed.value.object, "end_function_declaration");
    tokens.start_function_call = try replaceTokenMaybe(allocator, tokens.start_function_call, parsed.value.object, "start_function_call");
    tokens.end_function_call = try replaceTokenMaybe(allocator, tokens.end_function_call, parsed.value.object, "end_function_call");
    tokens.escape = try replaceTokenMaybe(allocator, tokens.escape, parsed.value.object, "escape");

    if (saw_gemma4_tool_tokens) {
        tokens.format = .gemma4;
        tokens.start_function_decl = try replaceTokenMaybe(allocator, tokens.start_function_decl, parsed.value.object, "std_token");
        tokens.end_function_decl = try replaceTokenMaybe(allocator, tokens.end_function_decl, parsed.value.object, "etd_token");
        tokens.start_function_call = try replaceTokenMaybe(allocator, tokens.start_function_call, parsed.value.object, "stc_token");
        tokens.end_function_call = try replaceTokenMaybe(allocator, tokens.end_function_call, parsed.value.object, "etc_token");
        tokens.escape = try replaceTokenMaybe(allocator, tokens.escape, parsed.value.object, "escape_token");
    }

    if (parsed.value.object.get("added_tokens_decoder")) |decoder_val| {
        if (decoder_val == .object) {
            var it = decoder_val.object.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.* != .object) continue;
                const content_val = entry.value_ptr.object.get("content") orelse continue;
                if (content_val != .string) continue;
                const content = content_val.string;
                if (std.mem.indexOf(u8, content, "start_function_declaration") != null) tokens.start_function_decl = try replaceOwnedToken(allocator, tokens.start_function_decl, content);
                if (std.mem.indexOf(u8, content, "end_function_declaration") != null) tokens.end_function_decl = try replaceOwnedToken(allocator, tokens.end_function_decl, content);
                if (std.mem.indexOf(u8, content, "start_function_call") != null) tokens.start_function_call = try replaceOwnedToken(allocator, tokens.start_function_call, content);
                if (std.mem.indexOf(u8, content, "end_function_call") != null) tokens.end_function_call = try replaceOwnedToken(allocator, tokens.end_function_call, content);
                if (std.mem.indexOf(u8, content, "escape") != null) tokens.escape = try replaceOwnedToken(allocator, tokens.escape, content);
            }
        }
    }

    if (!saw_function_call_tokens and !saw_gemma4_tool_tokens) {
        return error.MissingFunctionGemmaTokens;
    }
    return tokens;
}

fn loadToolTokensFromGguf(allocator: std.mem.Allocator, model_dir: []const u8) !FunctionGemmaTokens {
    const gguf_path = try findPrimaryGgufPath(allocator, model_dir);
    defer allocator.free(gguf_path);

    var region = try c_file.MmapRegion.init(allocator, gguf_path);
    defer region.deinit();

    var parsed = try gguf_format.parse(allocator, region.data);
    defer parsed.deinit(allocator);

    return (try detectGgufToolTokens(allocator, &parsed)) orelse error.MissingFunctionGemmaTokens;
}

fn detectGgufToolTokens(allocator: std.mem.Allocator, parsed: *const gguf_format.File) !?FunctionGemmaTokens {
    if (ggufHasToolTokenConvention(parsed, .gemma4)) {
        return try defaultGgufToolTokens(allocator, .gemma4);
    }
    if (ggufHasToolTokenConvention(parsed, .functiongemma)) {
        return try defaultGgufToolTokens(allocator, .functiongemma);
    }
    return null;
}

fn ggufHasToolTokenConvention(parsed: *const gguf_format.File, format: FunctionToolFormat) bool {
    return switch (format) {
        .gemma4 => ggufHasTokenArrayEntries(parsed, &.{
            "<|tool>",
            "<tool|>",
            "<|tool_call>",
            "<tool_call|>",
        }) or ggufChatTemplateContainsAll(parsed, &.{
            "<|tool_call>",
            "<tool_call|>",
        }),
        .functiongemma => ggufHasTokenArrayEntries(parsed, &.{
            "<start_function_call>",
            "<end_function_call>",
        }) or ggufChatTemplateContainsAll(parsed, &.{
            "<start_function_call>",
            "<end_function_call>",
        }),
    };
}

fn ggufHasTokenArrayEntries(parsed: *const gguf_format.File, needles: []const []const u8) bool {
    for (needles) |needle| {
        if (!ggufMetadataStringArrayContains(parsed, "tokenizer.ggml.tokens", needle)) return false;
    }
    return true;
}

fn ggufChatTemplateContainsAll(parsed: *const gguf_format.File, needles: []const []const u8) bool {
    const entry = findGgufMetadataEntry(parsed, "tokenizer.chat_template") orelse return false;
    if (entry.value != .string) return false;
    for (needles) |needle| {
        if (std.mem.indexOf(u8, entry.value.string, needle) == null) return false;
    }
    return true;
}

fn defaultGgufToolTokens(allocator: std.mem.Allocator, format: FunctionToolFormat) !FunctionGemmaTokens {
    var tokens = FunctionGemmaTokens{
        .format = format,
        .start_function_decl = &.{},
        .end_function_decl = &.{},
        .start_function_call = &.{},
        .end_function_call = &.{},
        .escape = &.{},
    };
    errdefer {
        allocator.free(tokens.start_function_decl);
        allocator.free(tokens.end_function_decl);
        allocator.free(tokens.start_function_call);
        allocator.free(tokens.end_function_call);
        allocator.free(tokens.escape);
    }
    tokens.start_function_decl = try allocator.dupe(u8, switch (format) {
        .gemma4 => "<|tool>",
        .functiongemma => "<start_function_declaration>",
    });
    tokens.end_function_decl = try allocator.dupe(u8, switch (format) {
        .gemma4 => "<tool|>",
        .functiongemma => "<end_function_declaration>",
    });
    tokens.start_function_call = try allocator.dupe(u8, switch (format) {
        .gemma4 => "<|tool_call>",
        .functiongemma => "<start_function_call>",
    });
    tokens.end_function_call = try allocator.dupe(u8, switch (format) {
        .gemma4 => "<tool_call|>",
        .functiongemma => "<end_function_call>",
    });
    tokens.escape = try allocator.dupe(u8, switch (format) {
        .gemma4 => "<|\"|>",
        .functiongemma => "<escape>",
    });
    return tokens;
}

fn findPrimaryGgufPath(allocator: std.mem.Allocator, model_dir: []const u8) ![]u8 {
    if (std.mem.endsWith(u8, model_dir, ".gguf")) {
        return try allocator.dupe(u8, model_dir);
    }

    if (!c_file.link_libc) {
        var dir = std.Io.Dir.cwd().openDir(std.Options.debug_io, model_dir, .{ .iterate = true }) catch return error.MissingFunctionGemmaTokens;
        defer dir.close(std.Options.debug_io);
        var iter = dir.iterate();
        while (iter.next(std.Options.debug_io) catch null) |entry| {
            const name = entry.name;
            if (name.len == 0 or name[0] == '.') continue;
            if (!std.mem.endsWith(u8, name, ".gguf")) continue;
            if (isGgufProjectorFileName(name)) continue;
            return try std.fmt.allocPrint(allocator, "{s}/{s}", .{ model_dir, name });
        }
        return error.MissingFunctionGemmaTokens;
    }

    const model_dir_z = try allocator.dupeZ(u8, model_dir);
    defer allocator.free(model_dir_z);

    const dir = c_file.c.opendir(model_dir_z.ptr);
    if (dir == null) return error.MissingFunctionGemmaTokens;
    defer _ = c_file.c.closedir(dir);

    while (c_file.c.readdir(dir)) |entry| {
        const name_z: [*:0]const u8 = @ptrCast(&entry.*.d_name);
        const name = std.mem.span(name_z);
        if (name.len == 0 or name[0] == '.') continue;
        if (!std.mem.endsWith(u8, name, ".gguf")) continue;
        if (isGgufProjectorFileName(name)) continue;
        return try std.fmt.allocPrint(allocator, "{s}/{s}", .{ model_dir, name });
    }
    return error.MissingFunctionGemmaTokens;
}

fn isGgufProjectorFileName(name: []const u8) bool {
    return std.mem.eql(u8, name, "mmproj.gguf") or
        std.mem.startsWith(u8, name, "mmproj-") or
        std.mem.startsWith(u8, name, "mmproj_");
}

fn findGgufMetadataEntry(parsed: *const gguf_format.File, key: []const u8) ?*const gguf_format.MetadataEntry {
    for (parsed.metadata) |*entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry;
    }
    return null;
}

fn ggufMetadataStringArrayContains(parsed: *const gguf_format.File, key: []const u8, needle: []const u8) bool {
    const entry = findGgufMetadataEntry(parsed, key) orelse return false;
    if (entry.value != .array) return false;
    const array = entry.value.array;
    if (array.element_type != .string) return false;
    for (array.values) |value| {
        if (value == .string and std.mem.eql(u8, value.string, needle)) return true;
    }
    return false;
}

fn replaceTokenMaybe(allocator: std.mem.Allocator, current: []const u8, obj: std.json.ObjectMap, key: []const u8) ![]const u8 {
    if (obj.get(key)) |value| {
        switch (value) {
            .string => |s| if (s.len > 0) return try replaceOwnedToken(allocator, current, s),
            .object => |nested| {
                if (nested.get("content")) |content| {
                    if (content == .string and content.string.len > 0) return try replaceOwnedToken(allocator, current, content.string);
                }
            },
            else => {},
        }
    }
    return current;
}

fn replaceOwnedToken(allocator: std.mem.Allocator, current: []const u8, replacement: []const u8) ![]const u8 {
    const duped = try allocator.dupe(u8, replacement);
    allocator.free(current);
    return duped;
}

fn appendJsonString(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, text: []const u8) !void {
    try buf.append(allocator, '"');
    for (text) |ch| {
        switch (ch) {
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => try buf.append(allocator, ch),
        }
    }
    try buf.append(allocator, '"');
}

test "parse tool choice variants" {
    const allocator = std.testing.allocator;

    try std.testing.expectEqual(ParsedToolChoice.auto, try parseToolChoice(null));

    const required_choice = std.json.Value{ .string = "required" };
    try std.testing.expectEqual(ParsedToolChoice.required, try parseToolChoice(required_choice));

    const parsed_choice = try std.json.parseFromSlice(std.json.Value, allocator, "{\"type\":\"function\",\"function\":{\"name\":\"get_weather\"}}", .{});
    defer parsed_choice.deinit();
    const parsed = try parseToolChoice(parsed_choice.value);
    try std.testing.expectEqualStrings("get_weather", forcedFunctionName(parsed).?);
}

test "functiongemma parser formats prompt and parses tool call" {
    const allocator = std.testing.allocator;
    var parser = Parser{ .functiongemma = .{
        .allocator = allocator,
        .tokens = .{},
    } };
    defer parser.deinit();

    const params_json = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{"properties":{"location":{"type":"string"}},"required":["location"]}
    , .{});
    defer params_json.deinit();

    const tools = [_]ToolDefinition{
        .{
            .function = .{
                .name = "get_weather",
                .description = "Get the weather for a location",
                .parameters = params_json.value,
            },
        },
    };
    const prompt = try parser.formatToolsPrompt(allocator, &tools);
    defer allocator.free(prompt);
    try std.testing.expect(std.mem.indexOf(u8, prompt, "get_weather") != null);

    const chunk = "<start_function_call>get_weather{location:<escape>San Francisco<escape>,days:3,metric:true}<end_function_call>";
    const update = try parser.feed(chunk);
    try std.testing.expectEqualStrings("", update.ready_text);
    try std.testing.expectEqual(@as(usize, 1), update.new_calls.len);
    const remaining = try parser.finishText(allocator);
    defer allocator.free(remaining);
    try std.testing.expectEqualStrings("", remaining);

    const calls = parser.toolCalls();
    try std.testing.expectEqual(@as(usize, 1), calls.len);
    try std.testing.expectEqualStrings("get_weather", calls[0].function.name);
    const parsed_args = try std.json.parseFromSlice(std.json.Value, allocator, calls[0].function.arguments, .{});
    defer parsed_args.deinit();
    try std.testing.expectEqualStrings("San Francisco", parsed_args.value.object.get("location").?.string);
    try std.testing.expectEqual(@as(i64, 3), parsed_args.value.object.get("days").?.integer);
    try std.testing.expectEqual(true, parsed_args.value.object.get("metric").?.bool);
}

test "gemma4 tool tokens format prompt and parse tool call" {
    const allocator = std.testing.allocator;
    var parser = Parser{ .functiongemma = .{
        .allocator = allocator,
        .tokens = .{
            .format = .gemma4,
            .start_function_decl = "<|tool>",
            .end_function_decl = "<tool|>",
            .start_function_call = "<|tool_call>",
            .end_function_call = "<tool_call|>",
            .escape = "<|\"|>",
        },
    } };
    defer parser.deinit();

    const params_json = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{"properties":{"id":{"type":"integer"}},"required":["id"]}
    , .{});
    defer params_json.deinit();

    const tools = [_]ToolDefinition{
        .{
            .function = .{
                .name = "lookup",
                .description = "Look up an order by id",
                .parameters = params_json.value,
            },
        },
    };
    const prompt = try parser.formatToolsPrompt(allocator, &tools);
    defer allocator.free(prompt);
    try std.testing.expect(std.mem.indexOf(u8, prompt, "<|tool>lookup") != null);
    try std.testing.expect(std.mem.indexOf(u8, prompt, "<|tool_call>call:function_name") != null);

    const update = try parser.feed("<|tool_call>call:lookup{id:42}<tool_call|>");
    try std.testing.expectEqualStrings("", update.ready_text);
    try std.testing.expectEqual(@as(usize, 1), update.new_calls.len);

    const calls = parser.toolCalls();
    try std.testing.expectEqual(@as(usize, 1), calls.len);
    try std.testing.expectEqualStrings("lookup", calls[0].function.name);
    const parsed_args = try std.json.parseFromSlice(std.json.Value, allocator, calls[0].function.arguments, .{});
    defer parsed_args.deinit();
    try std.testing.expectEqual(@as(i64, 42), parsed_args.value.object.get("id").?.integer);

    parser.reset();
    const bare_first = try parser.feed("call:lookup{id:");
    try std.testing.expectEqualStrings("", bare_first.ready_text);
    try std.testing.expectEqual(@as(usize, 0), bare_first.new_calls.len);
    const bare_update = try parser.feed("42}");
    try std.testing.expectEqualStrings("", bare_update.ready_text);
    try std.testing.expectEqual(@as(usize, 1), bare_update.new_calls.len);
    const bare_remaining = try parser.finishText(allocator);
    defer allocator.free(bare_remaining);
    try std.testing.expectEqualStrings("", bare_remaining);

    parser.reset();
    const trailing_update = try parser.feed("call:lookup{id:42} extra text");
    try std.testing.expectEqualStrings("", trailing_update.ready_text);
    try std.testing.expectEqual(@as(usize, 1), trailing_update.new_calls.len);
}

test "loadParser detects tool token convention from GGUF tokenizer metadata" {
    const allocator = std.testing.allocator;
    const writer = @import("../gguf/writer.zig");

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var token_values = [_]gguf_format.MetadataValue{
        .{ .string = "<|tool>" },
        .{ .string = "<tool|>" },
        .{ .string = "<|tool_call>" },
        .{ .string = "<tool_call|>" },
        .{ .string = "<|\"|>" },
    };
    const metadata = [_]gguf_format.MetadataEntry{
        .{
            .key = "tokenizer.ggml.tokens",
            .value = .{ .array = .{
                .element_type = .string,
                .values = &token_values,
            } },
        },
        .{
            .key = "tokenizer.chat_template",
            .value = .{ .string = "{{ '<|tool_call>call:name{}<tool_call|>' }}" },
        },
    };
    var layout = try writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    var parser = (try loadParser(allocator, model_dir)) orelse return error.TestUnexpectedResult;
    defer parser.deinit();

    const params_json = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{"properties":{"id":{"type":"integer"}},"required":["id"]}
    , .{});
    defer params_json.deinit();

    const tools = [_]ToolDefinition{.{
        .function = .{
            .name = "lookup",
            .description = "Look up an order by id",
            .parameters = params_json.value,
        },
    }};
    const prompt = try parser.formatToolsPrompt(allocator, &tools);
    defer allocator.free(prompt);
    try std.testing.expect(std.mem.indexOf(u8, prompt, "<|tool>lookup") != null);

    const update = try parser.feed("<|tool_call>call:lookup{id:42}<tool_call|>");
    try std.testing.expectEqual(@as(usize, 1), update.new_calls.len);
    const calls = parser.toolCalls();
    try std.testing.expectEqualStrings("lookup", calls[0].function.name);
}

test "functiongemma parser streams plain text before completed tool call" {
    const allocator = std.testing.allocator;
    var parser = Parser{ .functiongemma = .{
        .allocator = allocator,
        .tokens = .{},
    } };
    defer parser.deinit();

    const first = try parser.feed("hello ");
    try std.testing.expectEqualStrings("hello ", first.ready_text);
    try std.testing.expectEqual(@as(usize, 0), first.new_calls.len);

    const second = try parser.feed("<start_function_call>do_it{x:1}<end_function_call> world");
    try std.testing.expectEqualStrings(" world", second.ready_text);
    try std.testing.expectEqual(@as(usize, 1), second.new_calls.len);

    const trailing = try parser.finishRemainingText(allocator);
    defer allocator.free(trailing);
    try std.testing.expectEqualStrings("", trailing);
}

test "functiongemma parser preserves partial start token across chunks" {
    const allocator = std.testing.allocator;
    var parser = Parser{ .functiongemma = .{
        .allocator = allocator,
        .tokens = .{},
    } };
    defer parser.deinit();

    const first = try parser.feed("hello <start_function_");
    try std.testing.expectEqualStrings("hello ", first.ready_text);
    try std.testing.expectEqual(@as(usize, 0), first.new_calls.len);

    const second = try parser.feed("call>do_it{x:1}<end_function_call>");
    try std.testing.expectEqualStrings("", second.ready_text);
    try std.testing.expectEqual(@as(usize, 1), second.new_calls.len);
}

test "functiongemma parser emits incremental argument deltas" {
    const allocator = std.testing.allocator;
    var parser = Parser{ .functiongemma = .{
        .allocator = allocator,
        .tokens = .{},
    } };
    defer parser.deinit();

    const first = try parser.feed("<start_function_call>get_weather{location:<escape>San");
    try std.testing.expect(first.active_tool_delta != null);
    try std.testing.expectEqualStrings("get_weather", first.active_tool_delta.?.name.?);
    try std.testing.expectEqualStrings("{\"location\":\"San", first.active_tool_delta.?.arguments.?);

    const second = try parser.feed(" Francisco<escape>,days:3");
    try std.testing.expect(second.active_tool_delta != null);
    try std.testing.expectEqualStrings(" Francisco\",\"days\":3", second.active_tool_delta.?.arguments.?);

    const third = try parser.feed("<end_function_call>");
    try std.testing.expectEqual(@as(usize, 1), third.new_calls.len);
    try std.testing.expect(third.active_tool_delta != null);
    try std.testing.expectEqualStrings("}", third.active_tool_delta.?.arguments.?);
}

test "functiongemma parser does not emit null argument placeholders" {
    const allocator = std.testing.allocator;
    var parser = Parser{ .functiongemma = .{
        .allocator = allocator,
        .tokens = .{},
    } };
    defer parser.deinit();

    const first = try parser.feed("<start_function_call>call:get_weather{location:");
    try std.testing.expect(first.active_tool_delta != null);
    try std.testing.expectEqualStrings("get_weather", first.active_tool_delta.?.name.?);
    if (first.active_tool_delta.?.arguments) |arguments| {
        try std.testing.expectEqualStrings("{", arguments);
        try std.testing.expect(std.mem.indexOf(u8, arguments, "null") == null);
    }

    const second = try parser.feed("<escape>San");
    try std.testing.expect(second.active_tool_delta != null);
    try std.testing.expectEqualStrings("\"location\":\"San", second.active_tool_delta.?.arguments.?);
}

test "json parser formats prompt and parses tool call objects" {
    const allocator = std.testing.allocator;
    var parser = Parser{ .json = .{
        .allocator = allocator,
    } };
    defer parser.deinit();

    const params_json = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{"type":"object","properties":{"location":{"type":"string"}},"required":["location"]}
    , .{});
    defer params_json.deinit();

    const tools = [_]ToolDefinition{
        .{
            .function = .{
                .name = "get_weather",
                .description = "Get the weather for a location",
                .parameters = params_json.value,
            },
        },
    };
    const prompt = try parser.formatToolsPrompt(allocator, &tools);
    defer allocator.free(prompt);
    try std.testing.expect(std.mem.indexOf(u8, prompt, "\"name\":\"function_name\"") != null);

    const update = try parser.feed("{\"tool_calls\":[{\"name\":\"get_weather\",\"arguments\":{\"location\":\"San Francisco\",\"days\":3}}]}");
    try std.testing.expectEqual(@as(usize, 1), update.new_calls.len);
    try std.testing.expectEqualStrings("", update.ready_text);

    const calls = parser.toolCalls();
    try std.testing.expectEqual(@as(usize, 1), calls.len);
    try std.testing.expectEqualStrings("get_weather", calls[0].function.name);
    const parsed_args = try std.json.parseFromSlice(std.json.Value, allocator, calls[0].function.arguments, .{});
    defer parsed_args.deinit();
    try std.testing.expectEqualStrings("San Francisco", parsed_args.value.object.get("location").?.string);
    try std.testing.expectEqual(@as(i64, 3), parsed_args.value.object.get("days").?.integer);
}

test "json parser accepts openai style function wrapper" {
    const allocator = std.testing.allocator;
    var parser = Parser{ .json = .{
        .allocator = allocator,
    } };
    defer parser.deinit();

    const update = try parser.feed(
        \\{"tool_calls":[{"id":"call_123","type":"function","function":{"name":"lookup","arguments":"{\"id\":42}"}}]}
    );
    try std.testing.expectEqual(@as(usize, 1), update.new_calls.len);
    const calls = parser.toolCalls();
    try std.testing.expectEqualStrings("call_123", calls[0].id);
    try std.testing.expectEqualStrings("lookup", calls[0].function.name);
    try std.testing.expectEqualStrings("{\"id\":42}", calls[0].function.arguments);
}
