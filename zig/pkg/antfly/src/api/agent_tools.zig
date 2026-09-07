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

const std = @import("std");
const generating = @import("antfly_generating");

/// Request-arena-owned history shared by server-side agents. Never execute a
/// call before accepting the entire assistant turn: malformed/duplicate IDs
/// and oversized parallel batches must not cause partial side effects.
pub const Conversation = struct {
    alloc: std.mem.Allocator,
    messages: std.ArrayListUnmanaged(generating.ChatMessage) = .empty,
    ids: std.StringHashMapUnmanaged(void) = .empty,
    bytes: usize = 0,
    pub const max_bytes = 256 * 1024;
    pub const max_calls_per_turn = 8;

    pub fn append(self: *@This(), role: generating.Role, content: []const u8, id: ?[]const u8) !void {
        try self.reserve(content.len);
        try self.messages.append(self.alloc, .{
            .role = role,
            .content = .{ .text = try self.alloc.dupe(u8, content) },
            .tool_call_id = if (id) |value| try self.alloc.dupe(u8, value) else null,
        });
    }

    fn reserve(self: *@This(), count: usize) !void {
        if (count > max_bytes - self.bytes) return error.AgentContextLimitExceeded;
        self.bytes += count;
    }

    pub fn accept(self: *@This(), result: generating.GenerateResult, remaining: usize) ![]const generating.ToolCall {
        if (result.tool_calls.len > @min(remaining, max_calls_per_turn)) return error.AgentToolLimitExceeded;
        for (result.tool_calls) |call| {
            if (call.id.len == 0 or call.name.len == 0 or self.ids.contains(call.id)) return error.InvalidAgentToolCall;
            try self.ids.put(self.alloc, try self.alloc.dupe(u8, call.id), {});
            try self.reserve(call.id.len + call.name.len + call.arguments.len);
        }
        try self.reserve(result.content.len);
        const calls = try self.alloc.alloc(generating.ToolCall, result.tool_calls.len);
        for (calls, result.tool_calls) |*copy, call| copy.* = .{
            .id = try self.alloc.dupe(u8, call.id),
            .name = try self.alloc.dupe(u8, call.name),
            .arguments = try self.alloc.dupe(u8, call.arguments),
        };
        try self.messages.append(self.alloc, .{
            .role = .assistant,
            .content = if (result.content.len > 0) .{ .text = try self.alloc.dupe(u8, result.content) } else null,
            .tool_calls = if (calls.len > 0) calls else null,
        });
        return calls;
    }
};

pub fn withTools(alloc: std.mem.Allocator, chain: []const generating.ChainLink, schema: []const u8) ![]const generating.ChainLink {
    const copy = try alloc.dupe(generating.ChainLink, chain);
    for (copy) |*link| {
        // These providers preserve both function definitions and tool history.
        switch (link.generator.provider) {
            .antfly, .openai => {},
            else => return error.UnsupportedAgentToolProvider,
        }
        link.generator.tools_json = schema;
        link.generator.tool_choice_json = "\"auto\"";
    }
    return copy;
}

test "agent conversation rejects duplicate IDs and parallel budget overflow" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var history = Conversation{ .alloc = arena.allocator() };
    var calls = [_]generating.ToolCall{.{ .id = "call-1", .name = "search", .arguments = "{}" }};
    const result = generating.GenerateResult{ .allocator = arena.allocator(), .content = "", .tool_calls = &calls };
    try std.testing.expectError(error.AgentToolLimitExceeded, history.accept(result, 0));
    _ = try history.accept(result, 1);
    try history.append(.tool, "{\"hits\":[]}", "call-1");
    try std.testing.expectEqualStrings("call-1", history.messages.items[1].tool_call_id.?);
    try std.testing.expectError(error.InvalidAgentToolCall, history.accept(result, 1));
}
