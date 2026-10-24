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

//! Incremental Messages SSE -> Chat Completions translation. Native event and
//! block buffers are bounded; fragmented tool JSON is never accumulated here.
const std = @import("std");
const lib = @import("antfly_generating");
const wire = @import("anthropic.zig");
const sse = wire.streamSse;
const get = wire.streamGet;
const string = wire.streamString;
const Json = std.json.Value;

pub const Decoder = struct {
    const Block = struct { tool_index: ?usize = null, closed: bool = false, has_opaque: bool = false };
    alloc: std.mem.Allocator,
    sink: lib.ChatStream,
    model: []const u8,
    id: []const u8,
    created: u64 = 0,
    request_id: ?[]const u8 = null,
    include_usage: bool = false,
    framer: sse.Framer = .{ .limit = 4 * 1024 * 1024 },
    blocks: std.AutoHashMapUnmanaged(u32, Block) = .empty,
    tool_count: usize = 0,
    role_seen: bool = false,
    started: bool = false,
    stopped: bool = false,
    stop_reason: ?lib.GenerationFinishReason = null,
    counters: ?wire.UsageCounters = null,
    usage: ?lib.GenerationUsage = null,

    pub fn deinit(self: *Decoder) void {
        self.framer.deinit(self.alloc);
        self.blocks.deinit(self.alloc);
    }
    pub fn push(self: *Decoder, bytes: []const u8) !void {
        for (bytes) |byte| if (try self.framer.push(self.alloc, byte)) |record| try self.event(record);
    }
    fn event(self: *Decoder, event_: []const u8) !void {
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const data = try sse.dataAlloc(a, event_) orelse return;
        const value = std.json.parseFromSliceLeaky(Json, a, data, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidInferenceResponse;
        const tag = try string(try get(value, "type"));
        if (std.mem.eql(u8, tag, "ping")) return;
        if (std.mem.eql(u8, tag, "error")) {
            const kind = try string(try get(try get(value, "error"), "type"));
            if (std.mem.eql(u8, kind, "rate_limit_error")) return error.InferenceUpstreamRateLimited;
            if (std.mem.eql(u8, kind, "overloaded_error") or std.mem.eql(u8, kind, "api_error")) return error.InferenceUpstreamUnavailable;
            if (std.mem.eql(u8, kind, "invalid_request_error")) return error.InvalidInferenceRequest;
            return error.InvalidInferenceResponse;
        }
        if (self.stopped) return error.InvalidInferenceResponse;
        if (std.mem.eql(u8, tag, "message_start")) {
            if (self.role_seen) return error.InvalidInferenceResponse;
            const message = try get(value, "message");
            if (!std.mem.eql(u8, try string(try get(message, "role")), "assistant")) return error.InvalidInferenceResponse;
            self.counters = try wire.UsageCounters.init(try get(message, "usage"));
            self.role_seen = true;
            try self.delta(.{ .role = "assistant" });
        } else if (std.mem.eql(u8, tag, "content_block_start")) {
            if (!self.role_seen or self.stop_reason != null) return error.InvalidInferenceResponse;
            const index = try blockIndex(value);
            if (self.blocks.contains(index) or self.blocks.count() >= 256) return error.InvalidInferenceResponse;
            const content = try get(value, "content_block");
            const kind = try string(try get(content, "type"));
            var block: Block = .{};
            if (std.mem.eql(u8, kind, "tool_use")) {
                block.tool_index = self.tool_count;
                self.tool_count += 1;
                try self.delta(.{ .tool_calls = &.{.{ .index = block.tool_index.?, .id = try string(try get(content, "id")), .type = "function", .function = .{ .name = try string(try get(content, "name")), .arguments = "" } }}, .extra_content = .{ .anthropic = .{ .content_block_index = index } } });
            } else if (std.mem.eql(u8, kind, "text")) {
                const text = try string(try get(content, "text"));
                if (text.len > 0) try self.delta(.{ .content = text, .extra_content = .{ .anthropic = .{ .content_block_index = index } } });
            } else if (std.mem.eql(u8, kind, "thinking") or std.mem.eql(u8, kind, "redacted_thinking")) {
                block.has_opaque = true;
                try self.delta(.{ .extra_content = .{ .anthropic = .{ .event = value } } });
            } else return error.UnsupportedInferenceFeature;
            try self.blocks.put(self.alloc, index, block);
        } else if (std.mem.eql(u8, tag, "content_block_delta")) {
            const block = self.blocks.get(try blockIndex(value)) orelse return error.InvalidInferenceResponse;
            if (block.closed or self.stop_reason != null) return error.InvalidInferenceResponse;
            const content = try get(value, "delta");
            const kind = try string(try get(content, "type"));
            if (block.has_opaque) {
                try self.delta(.{ .extra_content = .{ .anthropic = .{ .event = value } } });
            } else if (std.mem.eql(u8, kind, "text_delta")) {
                if (block.tool_index != null) return error.InvalidInferenceResponse;
                try self.delta(.{ .content = try string(try get(content, "text")), .extra_content = .{ .anthropic = .{ .content_block_index = try blockIndex(value) } } });
            } else if (std.mem.eql(u8, kind, "input_json_delta")) {
                try self.delta(.{ .tool_calls = &.{.{ .index = block.tool_index orelse return error.InvalidInferenceResponse, .function = .{ .arguments = try string(try get(content, "partial_json")) } }} });
            } else return error.UnsupportedInferenceFeature;
        } else if (std.mem.eql(u8, tag, "content_block_stop")) {
            const block = self.blocks.getPtr(try blockIndex(value)) orelse return error.InvalidInferenceResponse;
            if (block.closed) return error.InvalidInferenceResponse;
            block.closed = true;
            if (block.has_opaque) try self.delta(.{ .extra_content = .{ .anthropic = .{ .event = value } } });
        } else if (std.mem.eql(u8, tag, "message_delta")) {
            if (!self.role_seen) return error.InvalidInferenceResponse;
            const delta_ = try get(value, "delta");
            if (delta_ != .object) return error.InvalidInferenceResponse;
            if (delta_.object.get("stop_reason")) |reason| if (reason != .null) {
                if (self.stop_reason != null) return error.InvalidInferenceResponse;
                var blocks = self.blocks.valueIterator();
                while (blocks.next()) |block| if (!block.closed) return error.InvalidInferenceResponse;
                self.stop_reason = try wire.finishReason(try string(reason));
            };
            if (value.object.get("usage")) |counts| {
                const counters = if (self.counters) |*current| current else return error.InvalidInferenceResponse;
                try counters.apply(counts);
                self.usage = counters.normalized();
            }
        } else if (std.mem.eql(u8, tag, "message_stop")) {
            const reason = self.stop_reason orelse return error.InvalidInferenceResponse;
            try self.emit(.{ .id = self.id, .object = "chat.completion.chunk", .created = self.created, .model = self.model, .choices = &.{.{ .index = @as(u32, 0), .delta = Json{ .object = .empty }, .finish_reason = @tagName(reason) }} });
            if (self.include_usage) if (self.usage) |counts| try self.emit(.{ .id = self.id, .object = "chat.completion.chunk", .created = self.created, .model = self.model, .choices = [_]u32{}, .usage = .{ .prompt_tokens = counts.input_tokens, .completion_tokens = counts.output_tokens, .total_tokens = counts.total_tokens.? } });
            self.stopped = true;
        }
        // Unknown top-level events are forward-compatible protocol additions;
        // they never substitute for the required message lifecycle.
    }
    fn delta(self: *Decoder, value: anytype) !void {
        try self.emit(.{ .id = self.id, .object = "chat.completion.chunk", .created = self.created, .model = self.model, .choices = &.{.{ .index = @as(u32, 0), .delta = value, .finish_reason = @as(?[]const u8, null) }} });
    }
    fn emit(self: *Decoder, value: anytype) !void {
        try lib.writeChatStreamChunk(self.alloc, self.sink, &self.started, self.request_id, value);
    }
    pub fn finish(self: *Decoder) !void {
        if (!self.stopped or (!self.framer.ready and self.framer.pending.items.len != 0)) return error.InvalidInferenceResponse;
        try self.sink.write(self.sink.context, "data: [DONE]\n\n");
        try self.sink.close(self.sink.context);
    }
};
fn blockIndex(value: Json) !u32 {
    const index = try get(value, "index");
    if (index != .integer or index.integer < 0) return error.InvalidInferenceResponse;
    return std.math.cast(u32, index.integer) orelse error.InvalidInferenceResponse;
}
