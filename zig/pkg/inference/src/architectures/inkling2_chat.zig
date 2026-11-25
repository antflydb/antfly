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

//! Inkling 2 chat formatting and output publication shared by execution providers.
const std = @import("std");
const images = @import("../pipelines/image.zig");
const generation = @import("../pipelines/generation.zig");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const jsonschema = @import("antfly_jsonschema");

const tooling = @import("../pipelines/tool_parser.zig");

pub const ChatOptions = struct {
    effort: ?f64 = null,
    tools: []const tooling.ToolDefinition = &.{},
    choice: tooling.ParsedToolChoice = .auto,

    pub fn parseEffort(value: std.json.Value) !?f64 {
        const amount: f64 = switch (value) {
            .null => return null,
            .integer => |n| @floatFromInt(n),
            .float => |n| n,
            .number_string => |n| std.fmt.parseFloat(f64, n) catch return error.InvalidInkling2Effort,
            .string => |name| blk: {
                const efforts = .{ .none = 0.0, .minimal = 0.1, .low = 0.2, .medium = 0.7, .high = 0.9, .xhigh = 0.99, .max = 0.99 };
                inline for (@typeInfo(@TypeOf(efforts)).@"struct".field_names) |key| {
                    if (std.mem.eql(u8, name, key)) break :blk @field(efforts, key);
                }
                return error.InvalidInkling2Effort;
            },
            else => return error.InvalidInkling2Effort,
        };
        if (!std.math.isFinite(amount) or amount < 0 or amount > 0.99) return error.InvalidInkling2Effort;
        return amount;
    }
};

fn toolNameValid(name: []const u8) bool {
    if (name.len == 0 or name.len > 64) return false;
    for (name) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_' and ch != '-') return false;
    return true;
}

const model_marker = "<|message_model|>";
const text_marker = "<|content_text|>";
const reasoning_marker = "<|content_thinking|>";
const tool_marker = "<|content_invoke_tool_json|>";
const end_marker = "<|end_message|>";
const sampling_end_marker = "<|content_model_end_sampling|>";

pub const Prompt = struct {
    before_image: []i32,
    after_image: []i32,
    image: ?[]const u8,
    image_width: u32 = 0,
    image_height: u32 = 0,
    initial_channel: ?Channel,

    pub fn init(allocator: std.mem.Allocator, tok: Tokenizer, messages: []const generation.Message, options: ChatOptions, enable_thinking: ?bool) !Prompt {
        const level = if (enable_thinking == false) 0 else options.effort orelse (if (enable_thinking == true) @as(f64, 0.9) else 0);
        const effort = try std.fmt.allocPrint(allocator, "<|message_system|>" ++ text_marker ++ "Thinking effort level: {d}" ++ end_marker, .{level});
        defer allocator.free(effort);
        var text: std.ArrayList(u8) = .empty;
        defer text.deinit(allocator);
        if (options.tools.len != 0) try appendTools(&text, allocator, options);
        var effort_written = false;
        var image_data: ?[]const u8 = null;
        var image_offset: usize = 0;
        for (messages, 0..) |message, message_index| {
            var tool_role: ?[]u8 = null;
            defer if (tool_role) |role| allocator.free(role);
            const role = if (std.mem.eql(u8, message.role, "system"))
                "<|message_system|>"
            else if (std.mem.eql(u8, message.role, "user"))
                "<|message_user|>"
            else if (std.mem.eql(u8, message.role, "assistant"))
                model_marker
            else if (std.mem.eql(u8, message.role, "tool")) blk: {
                const name = toolResultName(messages[0..message_index], message.tool_call_id orelse return error.InvalidInkling2ToolHistory) orelse
                    return error.InvalidInkling2ToolHistory;
                tool_role = try std.fmt.allocPrint(allocator, "<|message_tool|>{s}", .{name});
                break :blk tool_role.?;
            } else return error.UnsupportedInkling2Request;
            if (message.audio_bytes != null or message.video_bytes != null)
                return error.UnsupportedInkling2Request;
            if (!effort_written and !std.mem.eql(u8, message.role, "system")) {
                try text.appendSlice(allocator, effort);
                effort_written = true;
            }
            if (message.content_parts) |parts| {
                for (parts) |part| switch (part) {
                    .text => |content| try appendText(&text, allocator, role, content),
                    .image => |index| {
                        const attached = message.image_bytes orelse return error.InvalidInkling2Image;
                        if (index >= attached.len or !std.mem.eql(u8, message.role, "user"))
                            return error.InvalidInkling2Image;
                        try appendImage(&text, allocator, role, attached[index], &image_data, &image_offset);
                    },
                    .audio => return error.UnsupportedInkling2Request,
                };
            } else {
                for (message.image_bytes orelse &.{}) |data| {
                    if (!std.mem.eql(u8, message.role, "user")) return error.InvalidInkling2Image;
                    try appendImage(&text, allocator, role, data, &image_data, &image_offset);
                }
                if (!message.content_is_null) try appendText(&text, allocator, role, message.content);
            }
            if (message.tool_calls) |calls| {
                if (!std.mem.eql(u8, message.role, "assistant")) return error.InvalidInkling2ToolHistory;
                for (calls) |call| try appendToolHistory(&text, allocator, call);
            }
            if (std.mem.eql(u8, message.role, "assistant"))
                try text.appendSlice(allocator, sampling_end_marker);
        }
        if (!effort_written) try text.appendSlice(allocator, effort);
        const initial_channel: ?Channel = if (level > 0) .reasoning else if (options.tools.len > 0) .header else null;
        if (initial_channel != .header) {
            try text.appendSlice(allocator, model_marker);
            try text.appendSlice(allocator, if (initial_channel == .reasoning) reasoning_marker else text_marker);
        }
        const before = try tok.encode(allocator, text.items[0..image_offset]);
        errdefer allocator.free(before);
        const after = try tok.encode(allocator, text.items[image_offset..]);
        errdefer allocator.free(after);
        if (after.len == 0) return error.InvalidInkling2Tokenizer;
        var prompt = Prompt{ .before_image = before, .after_image = after, .image = image_data, .initial_channel = initial_channel };
        if (image_data) |data| {
            const info = images.inspectEncodedForInference(data, 4096) catch return error.InvalidInkling2Image;
            prompt.image_width = info.width;
            prompt.image_height = info.height;
        }
        return prompt;
    }

    fn appendTools(text: *std.ArrayList(u8), allocator: std.mem.Allocator, options: ChatOptions) !void {
        const Spec = struct { description: []const u8, name: []const u8, parameters: std.json.Value, type: []const u8 };
        const specs = try allocator.alloc(Spec, options.tools.len);
        defer allocator.free(specs);
        for (options.tools, specs) |tool, *spec| {
            if (!toolNameValid(tool.function.name)) return error.InvalidInkling2ToolHistory;
            spec.* = .{
                .description = tool.function.description,
                .name = tool.function.name,
                .parameters = if (tool.function.parameters) |schema| .{ .object = schema.map } else .{ .object = .{} },
                .type = tool.type,
            };
        }
        const encoded = try std.json.Stringify.valueAlloc(allocator, specs, .{});
        defer allocator.free(encoded);
        try text.appendSlice(allocator, "<|message_system|>tool_declare<|content_xml|>");
        try text.appendSlice(allocator, encoded);
        try text.appendSlice(allocator, end_marker);
        switch (options.choice) {
            .required => try appendText(text, allocator, "<|message_system|>", "Call one of the declared tools in your next response."),
            .function => |name| {
                const instruction = try std.fmt.allocPrint(allocator, "Call the {s} tool in your next response.", .{name});
                defer allocator.free(instruction);
                try appendText(text, allocator, "<|message_system|>", instruction);
            },
            else => {},
        }
    }

    fn appendToolHistory(text: *std.ArrayList(u8), allocator: std.mem.Allocator, call: generation.Message.ToolCall) !void {
        if (!toolNameValid(call.name) or !std.mem.eql(u8, call.type, "function")) return error.InvalidInkling2ToolHistory;
        const parsed = std.json.parseFromSlice(std.json.Value, allocator, call.arguments, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidInkling2ToolHistory,
        };
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidInkling2ToolHistory;
        const call_json = try std.json.Stringify.valueAlloc(allocator, .{ .name = call.name, .args = parsed.value }, .{});
        defer allocator.free(call_json);
        for ([_][]const u8{ model_marker, call.name, tool_marker, call_json, end_marker }) |part| try text.appendSlice(allocator, part);
    }

    fn toolResultName(messages: []const generation.Message, id: []const u8) ?[]const u8 {
        for (messages) |message| {
            for (message.tool_calls orelse &.{}) |call| {
                if (std.mem.eql(u8, call.id, id) and toolNameValid(call.name)) return call.name;
            }
        }
        return null;
    }

    fn appendText(text: *std.ArrayList(u8), allocator: std.mem.Allocator, role: []const u8, content: []const u8) !void {
        try text.appendSlice(allocator, role);
        try text.appendSlice(allocator, "<|content_text|>");
        try text.appendSlice(allocator, content);
        try text.appendSlice(allocator, "<|end_message|>");
    }

    fn appendImage(text: *std.ArrayList(u8), allocator: std.mem.Allocator, role: []const u8, data: []const u8, image_data: *?[]const u8, offset: *usize) !void {
        if (image_data.* != null) return error.InvalidInkling2Image;
        try text.appendSlice(allocator, role);
        try text.appendSlice(allocator, "<|content_image|>");
        offset.* = text.items.len;
        image_data.* = data;
        try text.appendSlice(allocator, "<|end_message|>");
    }

    pub fn deinit(self: Prompt, allocator: std.mem.Allocator) void {
        allocator.free(self.before_image);
        allocator.free(self.after_image);
    }

    pub fn imageRows(self: Prompt) usize {
        return ((self.image_width + 39) / 40) * ((self.image_height + 39) / 40);
    }

    pub fn count(self: Prompt) usize {
        return self.before_image.len + self.after_image.len + self.imageRows();
    }
};

pub const Channel = enum { header, text, reasoning, tool };

pub fn turnEndToken(allocator: std.mem.Allocator, tokenizer: Tokenizer) !u32 {
    return specialToken(allocator, tokenizer, end_marker);
}

fn specialToken(allocator: std.mem.Allocator, tokenizer: Tokenizer, marker: []const u8) !u32 {
    const ids = try tokenizer.encode(allocator, marker);
    defer allocator.free(ids);
    if (ids.len != 1 or ids[0] < 0) return error.InvalidInkling2Tokenizer;
    return @intCast(ids[0]);
}

pub const ChannelTokens = struct {
    header: u32,
    end: u32,
    text: u32,
    reasoning: u32,
    tool: u32,

    pub fn init(allocator: std.mem.Allocator, tokenizer: Tokenizer, turn_end: u32) !ChannelTokens {
        return .{
            .header = try specialToken(allocator, tokenizer, model_marker),
            .end = turn_end,
            .text = try specialToken(allocator, tokenizer, text_marker),
            .reasoning = try specialToken(allocator, tokenizer, reasoning_marker),
            .tool = try specialToken(allocator, tokenizer, tool_marker),
        };
    }
};

pub const Output = struct {
    allocator: std.mem.Allocator,
    tokenizer: Tokenizer,
    tokens: std.ArrayList(i32) = .empty,
    text: Utf8Text = .{},
    reasoning: Utf8Text = .{},
    tool_tokens: std.ArrayList(i32) = .empty,
    calls: std.ArrayList(tooling.ToolCall) = .empty,
    ctx: ?*anyopaque,
    callback: ?generation.TokenCallback,
    reasoning_ctx: ?*anyopaque,
    reasoning_callback: ?generation.TokenCallback,
    channels: ?ChannelTokens,
    channel: Channel,
    tools: []const tooling.ToolDefinition,

    pub fn deinit(self: *Output) void {
        self.tokens.deinit(self.allocator);
        self.text.bytes.deinit(self.allocator);
        self.reasoning.bytes.deinit(self.allocator);
        self.tool_tokens.deinit(self.allocator);
        for (self.calls.items) |*call| call.deinit(self.allocator);
        self.calls.deinit(self.allocator);
    }

    pub fn append(self: *Output, token: u32) !void {
        const at_start = self.tokens.items.len == 0;
        try self.tokens.append(self.allocator, @intCast(token));
        if (self.channels) |channels| {
            const channel: ?Channel = if (token == channels.header or token == channels.end)
                .header
            else if (token == channels.text)
                .text
            else if (token == channels.reasoning)
                .reasoning
            else if (token == channels.tool)
                .tool
            else
                null;
            if (channel) |next| {
                if (self.channel == .tool) try self.finishTool();
                self.text.pending_len = 0;
                self.reasoning.pending_len = 0;
                self.channel = next;
                return;
            }
        } else if (self.callback == null) return;

        switch (self.channel) {
            .header => {},
            .tool => try self.tool_tokens.append(self.allocator, @intCast(token)),
            .text => try self.appendText(&self.text, self.ctx, self.callback, token, at_start),
            .reasoning => try self.appendText(&self.reasoning, self.reasoning_ctx, self.reasoning_callback, token, at_start),
        }
    }

    fn appendText(self: *Output, output: *Utf8Text, ctx: ?*anyopaque, callback: ?generation.TokenCallback, token: u32, at_start: bool) !void {
        const bytes = try self.tokenizer.decodeToken(self.allocator, @intCast(token), at_start);
        defer self.allocator.free(bytes);
        const start = output.bytes.items.len;
        for (bytes) |byte| try output.appendByte(self.allocator, byte);
        if (callback) |emit| {
            if (output.bytes.items.len > start and !emit(ctx.?, output.bytes.items[start..]))
                return error.Cancelled;
        }
    }

    pub fn finishTool(self: *Output) !void {
        const data = try self.tokenizer.decodeText(self.allocator, self.tool_tokens.items);
        defer self.allocator.free(data);
        const parsed = std.json.parseFromSlice(struct { name: []const u8, args: std.json.Value }, self.allocator, data, .{ .parse_numbers = false }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidInkling2ToolOutput,
        };
        defer parsed.deinit();
        if (parsed.value.args != .object) return error.InvalidInkling2ToolOutput;
        for (self.tools) |tool| {
            if (!std.mem.eql(u8, tool.function.name, parsed.value.name)) continue;
            if (tool.function.strict) {
                if (tool.function.parameters) |schema| {
                    jsonschema.validateJsonSchemaValue(self.allocator, .{ .object = schema.map }, parsed.value.args) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => return error.InvalidInkling2ToolOutput,
                    };
                }
            }
            const name = try self.allocator.dupe(u8, parsed.value.name);
            errdefer self.allocator.free(name);
            const arguments = try std.json.Stringify.valueAlloc(self.allocator, parsed.value.args, .{});
            errdefer self.allocator.free(arguments);
            const id = try tooling.generateCallId(self.allocator);
            errdefer self.allocator.free(id);
            try self.calls.append(self.allocator, .{ .id = id, .type = "function", .function = .{ .name = name, .arguments = arguments } });
            self.tool_tokens.clearRetainingCapacity();
            return;
        }
        return error.InvalidInkling2ToolOutput;
    }
};

const Utf8Text = struct {
    bytes: std.ArrayList(u8) = .empty,
    pending_utf8: [4]u8 = undefined,
    pending_len: usize = 0,

    fn appendByte(self: *Utf8Text, allocator: std.mem.Allocator, byte: u8) !void {
        if (self.pending_len != 0 and byte & 0xc0 != 0x80) self.pending_len = 0;
        if (self.pending_len == 0 and byte < 0x80) {
            try self.bytes.append(allocator, byte);
            return;
        }
        if (self.pending_len == 0 and byte & 0xc0 == 0x80) return;
        self.pending_utf8[self.pending_len] = byte;
        self.pending_len += 1;
        const len = std.unicode.utf8ByteSequenceLength(self.pending_utf8[0]) catch {
            self.pending_len = 0;
            return;
        };
        if (self.pending_len == len) {
            const sequence = self.pending_utf8[0..len];
            if (std.unicode.utf8ValidateSlice(sequence)) try self.bytes.appendSlice(allocator, sequence);
            self.pending_len = 0;
        }
    }
};

test "Inkling chat frontend builds prompts and publishes tokens without a device provider" {
    const allocator = std.testing.allocator;
    const hf_tokenizer = @import("inference_hf_tokenizer");
    var hf = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator,
        \\{"version":"1.0","added_tokens":[{"id":10,"content":"<|message_model|>"},{"id":11,"content":"<|content_text|>"},{"id":12,"content":"<|content_thinking|>"},{"id":13,"content":"<|content_invoke_tool_json|>"},{"id":14,"content":"<|end_message|>"}],"model":{"type":"BPE","unk_token":"<unk>","vocab":{"<unk>":0,"x":1,"<|message_model|>":10,"<|content_text|>":11,"<|content_thinking|>":12,"<|content_invoke_tool_json|>":13,"<|end_message|>":14},"merges":[]}}
    );
    defer hf.deinitSelf();
    const tokenizer = hf.tokenizer();
    const prompt = try Prompt.init(allocator, tokenizer, &.{.{ .role = "user", .content = "x" }}, .{}, true);
    defer prompt.deinit(allocator);
    try std.testing.expectEqual(Channel.reasoning, prompt.initial_channel.?);
    try std.testing.expect(prompt.count() > 0);
    try std.testing.expectEqual(@as(u32, 14), try turnEndToken(allocator, tokenizer));
    var output: Output = .{
        .allocator = allocator,
        .tokenizer = tokenizer,
        .ctx = null,
        .callback = null,
        .reasoning_ctx = null,
        .reasoning_callback = null,
        .channels = try .init(allocator, tokenizer, 14),
        .channel = .header,
        .tools = &.{},
    };
    defer output.deinit();
    try output.append(11);
    try output.append(1);
    try std.testing.expectEqualStrings("x", output.text.bytes.items);
}
