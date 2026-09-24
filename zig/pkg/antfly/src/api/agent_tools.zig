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
    /// Nested agents may lend a smaller history ceiling than the default.
    limit_bytes: usize = max_bytes,
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
        if (count > self.limit_bytes -| self.bytes) return error.AgentContextLimitExceeded;
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
        if (result.google_parts_json) |parts| try self.reserve(parts.len);
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
            .google_parts_json = if (result.google_parts_json) |parts| try self.alloc.dupe(u8, parts) else null,
        });
        return calls;
    }
};

/// Execution ceilings for one model-directed agent loop. Public retrieval
/// requests use the defaults; composite agents such as research lend each
/// nested run a smaller slice so the parent's declared worst case holds.
pub const Budget = struct {
    /// Tool calls across every round, including delegated planning.
    max_tool_calls: i64 = default_max_tool_calls,
    /// Retained model history, see Conversation.limit_bytes.
    max_history_bytes: usize = Conversation.max_bytes,

    pub const default_max_tool_calls: i64 = 20;

    pub fn toolCalls(self: Budget) i64 {
        return std.math.clamp(self.max_tool_calls, 0, default_max_tool_calls);
    }
};

/// Conservative token estimate for budgeting text whose generator tokenizer
/// is not available locally. ASCII averages about four bytes per token for
/// BPE vocabularies; every non-ASCII code point (CJK, emoji, most non-Latin
/// scripts) is charged a full token so those budgets are not overrun 3-4x.
pub fn estimateTokens(text: []const u8) usize {
    var ascii: usize = 0;
    var other: usize = 0;
    var i: usize = 0;
    while (i < text.len) {
        const byte = text[i];
        if (byte < 0x80) {
            ascii += 1;
            i += 1;
            continue;
        }
        const len = std.unicode.utf8ByteSequenceLength(byte) catch 1;
        other += 1;
        i += @min(len, text.len - i);
    }
    return (ascii + 3) / 4 + other;
}

pub fn withTools(alloc: std.mem.Allocator, chain: []const generating.ChainLink, schema: []const u8) ![]const generating.ChainLink {
    const copy = try alloc.dupe(generating.ChainLink, chain);
    for (copy) |*link| {
        if (!link.generator.provider.supportsTools()) return error.UnsupportedAgentToolProvider;
        // A finished graph walk can leave no available tools. Omit both
        // fields for the final answer instead of sending an empty tool list
        // with a provider-dependent automatic-tool choice.
        const has_tools = !std.mem.eql(u8, schema, "[]");
        link.generator.tools_json = if (has_tools) schema else null;
        link.generator.tool_choice_json = if (has_tools) "\"auto\"" else null;
    }
    return copy;
}

test "token estimate charges non-ASCII code points individually" {
    try std.testing.expectEqual(@as(usize, 0), estimateTokens(""));
    try std.testing.expectEqual(@as(usize, 1), estimateTokens("abcd"));
    try std.testing.expectEqual(@as(usize, 2), estimateTokens("abcde"));
    // Three CJK code points are nine bytes but at least three tokens.
    try std.testing.expectEqual(@as(usize, 3), estimateTokens("\u{4e2d}\u{6587}\u{5b57}"));
    // Truncated sequences never read past the slice.
    try std.testing.expectEqual(@as(usize, 1), estimateTokens("\xe4"));
}

test "agent conversation honors a lent history ceiling" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var history = Conversation{ .alloc = arena.allocator(), .limit_bytes = 8 };
    try history.append(.user, "12345678", null);
    try std.testing.expectError(error.AgentContextLimitExceeded, history.append(.user, "x", null));
    try std.testing.expectEqual(@as(i64, 20), (Budget{ .max_tool_calls = 99 }).toolCalls());
}

test "agent tools accept OpenRouter generators" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const chain = try withTools(arena_impl.allocator(), &.{.{ .generator = .{
        .provider = .openrouter,
        .model = "test-model",
        .url = "https://openrouter.ai/api/v1",
    } }}, "[{\"type\":\"function\",\"function\":{\"name\":\"search\",\"parameters\":{\"type\":\"object\"}}}]");
    try std.testing.expectEqual(.openrouter, chain[0].generator.provider);
    try std.testing.expect(chain[0].generator.tools_json != null);
    try std.testing.expectEqualStrings("\"auto\"", chain[0].generator.tool_choice_json.?);
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

test "agent tools enable every real generator adapter and omit empty schemas" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    for (std.enums.values(generating.Provider)) |provider| {
        const chain = [_]generating.ChainLink{.{ .generator = .{ .provider = provider, .model = "m", .url = "" } }};
        if (provider == .mock) {
            try std.testing.expectError(error.UnsupportedAgentToolProvider, withTools(arena.allocator(), &chain, "[]"));
            continue;
        }
        const configured = try withTools(arena.allocator(), &chain, "[]");
        try std.testing.expect(configured[0].generator.tools_json == null);
        try std.testing.expect(configured[0].generator.tool_choice_json == null);
    }
}

test "agent conversation owns Google replay parts and charges the context budget" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var history = Conversation{ .alloc = arena.allocator() };
    var parts = [_]u8{ '[', ']' };
    const result = generating.GenerateResult{ .allocator = arena.allocator(), .content = "ok", .google_parts_json = &parts };
    _ = try history.accept(result, 1);
    parts[0] = 'x';
    try std.testing.expectEqualStrings("[]", history.messages.items[0].google_parts_json.?);
    try std.testing.expectEqual(@as(usize, 4), history.bytes);
    history.bytes = Conversation.max_bytes - 3;
    try std.testing.expectError(error.AgentContextLimitExceeded, history.accept(result, 1));
}
