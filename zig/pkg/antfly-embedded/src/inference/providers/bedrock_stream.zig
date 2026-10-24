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

//! Bounded AWS EventStream -> Chat Completions SSE translation. Frames are
//! checked before publication; tool arguments stream without whole-result
//! buffering. EOF is successful only after an explicit native message stop.
const std = @import("std");
const inference = @import("antfly_inference_types");
const Json = std.json.Value;

pub const Decoder = struct {
    const Block = struct { tool_index: ?usize = null, closed: bool = false, has_reasoning: bool = false };
    alloc: std.mem.Allocator,
    sink: inference.ChatStream,
    model: []const u8,
    id: []const u8 = "chatcmpl-bedrock",
    created: u64 = 0,
    include_usage: bool = false,
    frame_limit: usize = 4 * 1024 * 1024,
    pending: std.ArrayList(u8) = .empty,
    blocks: std.AutoHashMapUnmanaged(u32, Block) = .empty,
    tool_count: usize = 0,
    role_seen: bool = false,
    stopped: bool = false,
    started: bool = false,
    request_id: ?[]const u8 = null,
    usage: ?inference.GenerationUsage = null,

    pub fn deinit(self: *Decoder) void {
        self.pending.deinit(self.alloc);
        self.blocks.deinit(self.alloc);
    }

    pub fn push(self: *Decoder, input: []const u8) !void {
        var bytes = input;
        while (bytes.len > 0) {
            var required: usize = 12;
            if (self.pending.items.len >= 12) {
                required = try self.frameLength();
            }
            const take = @min(bytes.len, required - self.pending.items.len);
            try self.pending.appendSlice(self.alloc, bytes[0..take]);
            bytes = bytes[take..];
            if (self.pending.items.len < 12) continue;
            required = try self.frameLength();
            if (self.pending.items.len != required) continue;
            const frame = self.pending.items;
            const expected = std.mem.readInt(u32, frame[frame.len - 4 ..][0..4], .big);
            if (@import("antfly_hash").Crc32.hash(frame[0 .. frame.len - 4]) != expected) return error.InvalidInferenceResponse;
            try self.event(frame);
            self.pending.clearRetainingCapacity();
        }
    }

    fn frameLength(self: *Decoder) !usize {
        const frame = self.pending.items;
        if (@import("antfly_hash").Crc32.hash(frame[0..8]) != std.mem.readInt(u32, frame[8..12], .big)) return error.InvalidInferenceResponse;
        const total = std.mem.readInt(u32, frame[0..4], .big);
        const headers = std.mem.readInt(u32, frame[4..8], .big);
        if (total < 16 or headers > total - 16) return error.InvalidInferenceResponse;
        if (total > self.frame_limit) return error.InferenceResponseTooLarge;
        return total;
    }

    fn event(self: *Decoder, frame: []const u8) !void {
        const header_length = std.mem.readInt(u32, frame[4..8], .big);
        const headers = frame[12..][0..header_length];
        var offset: usize = 0;
        var event_type: ?[]const u8 = null;
        var message_type: ?[]const u8 = null;
        while (offset < headers.len) {
            const name_length = headers[offset];
            offset += 1;
            if (name_length == 0 or name_length > headers.len - offset) return error.InvalidInferenceResponse;
            const name = headers[offset..][0..name_length];
            offset += name_length;
            if (offset == headers.len) return error.InvalidInferenceResponse;
            const kind = headers[offset];
            offset += 1;
            const length: usize = switch (kind) {
                0, 1 => 0,
                2 => 1,
                3 => 2,
                4 => 4,
                5, 8 => 8,
                9 => 16,
                6, 7 => blk: {
                    if (headers.len - offset < 2) return error.InvalidInferenceResponse;
                    const size = std.mem.readInt(u16, headers[offset..][0..2], .big);
                    offset += 2;
                    break :blk size;
                },
                else => return error.InvalidInferenceResponse,
            };
            if (length > headers.len - offset) return error.InvalidInferenceResponse;
            const value = headers[offset..][0..length];
            offset += length;
            if (std.mem.eql(u8, name, ":event-type") or std.mem.eql(u8, name, ":exception-type")) {
                if (kind != 7 or event_type != null) return error.InvalidInferenceResponse;
                event_type = value;
            } else if (std.mem.eql(u8, name, ":message-type")) {
                if (kind != 7 or message_type != null) return error.InvalidInferenceResponse;
                message_type = value;
            }
        }
        const tag = event_type orelse return error.InvalidInferenceResponse;
        if (!std.mem.eql(u8, message_type orelse return error.InvalidInferenceResponse, "event")) {
            if (!std.mem.eql(u8, message_type.?, "exception")) return error.InvalidInferenceResponse;
            if (std.mem.eql(u8, tag, "throttlingException")) return error.InferenceUpstreamRateLimited;
            if (std.mem.eql(u8, tag, "validationException")) return error.InvalidInferenceRequest;
            if (std.mem.eql(u8, tag, "internalServerException") or std.mem.eql(u8, tag, "serviceUnavailableException") or std.mem.eql(u8, tag, "modelStreamErrorException")) return error.InferenceUpstreamUnavailable;
            return error.InvalidInferenceResponse;
        }
        // A fresh arena per frame prevents a request arena from retaining every
        // streamed token or fragmented tool-argument buffer until request end.
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const parsed = std.json.parseFromSliceLeaky(Json, arena.allocator(), frame[12 + header_length .. frame.len - 4], .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidInferenceResponse;
        const value = if (parsed == .object) parsed.object.get(tag) orelse parsed else return error.InvalidInferenceResponse;
        if (std.mem.eql(u8, tag, "messageStart")) {
            if (self.role_seen or self.stopped or !std.mem.eql(u8, try string(try get(value, "role")), "assistant")) return error.InvalidInferenceResponse;
            self.role_seen = true;
            try self.delta(.{ .role = "assistant" });
        } else if (std.mem.eql(u8, tag, "contentBlockStart")) {
            if (!self.role_seen or self.stopped) return error.InvalidInferenceResponse;
            const index = try blockIndex(value);
            if (self.blocks.contains(index)) return error.InvalidInferenceResponse;
            const start = try get(value, "start");
            var block: Block = .{};
            if (start == .object) if (start.object.get("toolUse")) |tool| {
                block.tool_index = self.tool_count;
                self.tool_count += 1;
                try self.delta(.{ .tool_calls = &.{.{ .index = block.tool_index.?, .id = try string(try get(tool, "toolUseId")), .type = "function", .function = .{ .name = try string(try get(tool, "name")), .arguments = "" } }}, .extra_content = .{ .bedrock = .{ .content_block_index = index } } });
            };
            try self.addBlock(index, block);
        } else if (std.mem.eql(u8, tag, "contentBlockDelta")) {
            if (!self.role_seen or self.stopped) return error.InvalidInferenceResponse;
            const index = try blockIndex(value);
            const content = try get(value, "delta");
            if (content != .object) return error.InvalidInferenceResponse;
            if (self.blocks.get(index)) |block| if (block.closed) return error.InvalidInferenceResponse;
            if (content.object.get("text")) |part| {
                if (self.blocks.get(index)) |block| if (block.tool_index != null) return error.InvalidInferenceResponse;
                if (!self.blocks.contains(index)) try self.addBlock(index, .{});
                try self.delta(.{ .content = try string(part), .extra_content = .{ .bedrock = .{ .content_block_index = index } } });
            } else if (content.object.get("toolUse")) |tool| {
                const block = self.blocks.get(index) orelse return error.InvalidInferenceResponse;
                try self.delta(.{ .tool_calls = &.{.{ .index = block.tool_index orelse return error.InvalidInferenceResponse, .function = .{ .arguments = try string(try get(tool, "input")) } }} });
            } else if (content.object.get("reasoningContent")) |reasoning| {
                if (reasoning != .object) return error.InvalidInferenceResponse;
                if (!self.blocks.contains(index)) try self.addBlock(index, .{});
                self.blocks.getPtr(index).?.has_reasoning = true;
                try self.delta(.{ .extra_content = .{ .bedrock = .{ .content_block_index = index, .event = .{ .contentBlockDelta = value } } } });
            } else return error.InvalidInferenceResponse;
        } else if (std.mem.eql(u8, tag, "contentBlockStop")) {
            if (!self.role_seen or self.stopped) return error.InvalidInferenceResponse;
            const index = try blockIndex(value);
            if (!self.blocks.contains(index)) try self.addBlock(index, .{});
            const block = self.blocks.getPtr(index).?;
            if (block.closed) return error.InvalidInferenceResponse;
            block.closed = true;
            if (block.has_reasoning) try self.delta(.{ .extra_content = .{ .bedrock = .{ .event = .{ .contentBlockStop = value } } } });
        } else if (std.mem.eql(u8, tag, "messageStop")) {
            if (!self.role_seen or self.stopped) return error.InvalidInferenceResponse;
            var blocks = self.blocks.valueIterator();
            while (blocks.next()) |block| if (!block.closed) return error.InvalidInferenceResponse;
            const native = try string(try get(value, "stopReason"));
            const reason = @tagName(@import("bedrock_converse.zig").finishReason(native) catch return error.InvalidInferenceResponse);
            try self.emit(.{ .id = self.id, .object = "chat.completion.chunk", .created = self.created, .model = self.model, .choices = &.{.{ .index = @as(u32, 0), .delta = Json{ .object = .empty }, .finish_reason = reason }} });
            self.stopped = true;
        } else if (std.mem.eql(u8, tag, "metadata")) {
            if (!self.stopped or self.usage != null) return error.InvalidInferenceResponse;
            self.usage = inference.GenerationUsage.fromValue(try get(value, "usage"), "inputTokens", "outputTokens", "totalTokens") orelse return error.InvalidInferenceResponse;
            if (self.include_usage) try self.emit(.{ .id = self.id, .object = "chat.completion.chunk", .created = self.created, .model = self.model, .choices = [_]u32{}, .usage = .{ .prompt_tokens = self.usage.?.input_tokens, .completion_tokens = self.usage.?.output_tokens, .total_tokens = self.usage.?.total_tokens orelse (self.usage.?.input_tokens +| self.usage.?.output_tokens) }, .provider_usage = try get(value, "usage") });
        } else return error.InvalidInferenceResponse;
    }

    fn addBlock(self: *Decoder, index: u32, block: Block) !void {
        if (self.blocks.count() >= 256) return error.InferenceResponseTooLarge;
        try self.blocks.put(self.alloc, index, block);
    }
    fn delta(self: *Decoder, value: anytype) !void {
        try self.emit(.{ .id = self.id, .object = "chat.completion.chunk", .created = self.created, .model = self.model, .choices = &.{.{ .index = @as(u32, 0), .delta = value, .finish_reason = @as(?[]const u8, null) }} });
    }
    fn emit(self: *Decoder, value: anytype) !void {
        try inference.writeChatStreamChunk(self.alloc, self.sink, &self.started, self.request_id, value);
    }
    pub fn finish(self: *Decoder) !void {
        if (self.pending.items.len != 0 or !self.stopped or self.usage == null) return error.InvalidInferenceResponse;
        try self.sink.write(self.sink.context, "data: [DONE]\n\n");
        try self.sink.close(self.sink.context);
    }
};

fn get(value: Json, key: []const u8) !Json {
    if (value != .object) return error.InvalidInferenceResponse;
    return value.object.get(key) orelse error.InvalidInferenceResponse;
}
fn string(value: Json) ![]const u8 {
    if (value != .string) return error.InvalidInferenceResponse;
    return value.string;
}
fn blockIndex(value: Json) !u32 {
    const index = try get(value, "contentBlockIndex");
    if (index != .integer or index.integer < 0) return error.InvalidInferenceResponse;
    return std.math.cast(u32, index.integer) orelse error.InvalidInferenceResponse;
}
