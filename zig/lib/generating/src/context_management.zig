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

//! Adapter from public generator dispatch to shared context preparation.
const std = @import("std");
const g = @import("mod.zig");
pub const planner = @import("antfly_chat_context");
pub const preparation = planner.preparation;
pub const State = preparation.State;
pub const Config = struct {
    mode: preparation.Mode = .compact,
    context_window_tokens: usize,
    target_input_tokens: ?usize = null,
    reserved_output_tokens: usize = 4000,
    keep_recent_turns: usize = 2,
    summarizer_context_window_tokens: usize = 8192,
    summary_output_tokens: usize = 1024,
    max_summary_batches: usize = 8,
    summarizer: ?g.GeneratorConfig = null,
};
pub const Event = struct {
    diagnostics: preparation.Diagnostics,
    counting_method: []const u8 = "utf8_bytes_plus_framing",
    summary_provider: ?[]const u8 = null,
    summary_model: ?[]const u8 = null,
};
pub const Prepared = struct { messages: []const g.ChatMessage, next: State, event: Event };

/// Conservative byte accounting includes JSON-escaped content, opaque replay
/// blocks, tool schemas, response format, 32 framing tokens per message and
/// 512 template tokens. No remote tokenizer equivalence is claimed. A caller
/// with an exact destination tokenizer can replace this counter.
pub const Counter = struct {
    allocator: std.mem.Allocator,
    source: []const g.ChatMessage,
    chain: []const g.ChainLink,

    pub fn count(raw: *anyopaque, input: planner.CountInput) !usize {
        const self: *Counter = @ptrCast(@alignCast(raw));
        const messages = try effectiveMessages(self.allocator, self.source, input.keep, input.rendered_memory);
        const encoded = try std.json.Stringify.valueAlloc(self.allocator, messages, .{});
        var schemas: usize = 0;
        for (self.chain) |link| {
            const cfg = link.generator;
            schemas = @max(schemas, (if (cfg.tools_json) |s| s.len else @as(usize, 0)) +|
                (if (cfg.response_format_json) |s| s.len else @as(usize, 0)) +|
                (if (cfg.tool_choice_json) |s| s.len else @as(usize, 0)));
        }
        return std.math.add(usize, encoded.len +| schemas, 512 +| messages.len *| 32);
    }
};

pub fn effectiveMessages(a: std.mem.Allocator, source: []const g.ChatMessage, keep: []const bool, memory: ?[]const u8) ![]const g.ChatMessage {
    var out: std.ArrayListUnmanaged(g.ChatMessage) = .empty;
    var inserted = false;
    for (source, keep) |message, retained| {
        if (!retained) continue;
        var copy = message;
        if (!inserted and memory != null and message.role == .user) {
            const text = if (message.content) |c| if (c == .text) c.text else return error.UnsupportedContextMedia else "";
            copy.content = .{ .text = try std.mem.concat(a, u8, &.{ memory.?, "\n\n", text }) };
            inserted = true;
        }
        try out.append(a, copy);
    }
    if (memory != null and !inserted) return error.ContextMemoryRequiresUserMessage;
    return out.toOwnedSlice(a);
}

pub fn viewMessages(a: std.mem.Allocator, source: []const g.ChatMessage) ![]const planner.MessageView {
    if (source.len > 4096) return error.ContextMessageLimitExceeded;
    var total_calls: usize = 0;
    for (source) |message| total_calls +|= if (message.tool_calls) |calls| calls.len else 0;
    if (total_calls > 16384) return error.ContextToolCallLimitExceeded;
    const views = try a.alloc(planner.MessageView, source.len);
    for (source, views) |message, *view| {
        const text = if (message.content) |c| if (c == .text) c.text else return error.UnsupportedContextMedia else "";
        const calls = message.tool_calls orelse &.{};
        const call_views = try a.alloc(planner.ToolCallView, calls.len);
        for (calls, call_views) |call, *v| v.* = .{ .id = call.id, .name = call.name, .arguments = call.arguments };
        view.* = .{ .role = switch (message.role) {
            .system => .system,
            .user => .user,
            .assistant => .assistant,
            .tool => .tool,
        }, .content = text, .content_is_null = message.content == null, .tool_calls = call_views, .tool_call_id = message.tool_call_id };
    }
    return views;
}

pub const summary_schema =
    \\{"type":"object","additionalProperties":false,"required":["conversation","tool_evidence","unresolved_tool_calls"],"properties":{"conversation":{"type":"string"},"tool_evidence":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["tool_call_id","tool_name","facts"],"properties":{"tool_call_id":{"type":"string"},"tool_name":{"type":"string"},"facts":{"type":"string"}}}},"unresolved_tool_calls":{"type":"array","maxItems":0,"items":{"type":"object","additionalProperties":false,"required":["tool_call_id","tool_name"],"properties":{"tool_call_id":{"type":"string"},"tool_name":{"type":"string"}}}}}}
;

pub fn summaryConfig(a: std.mem.Allocator, base: g.GeneratorConfig, output: usize) !g.GeneratorConfig {
    var cfg = base;
    cfg.tools_json = null;
    cfg.tool_choice_json = null;
    cfg.max_tokens = @intCast(output);
    cfg.max_completion_tokens = if (base.max_completion_tokens != null) @intCast(output) else null;
    cfg.reasoning_effort = null;
    cfg.logprobs = false;
    // These adapters expose JSON-schema output. Other providers receive the
    // same JSON instructions and pass the identical strict parser/validator.
    cfg.response_format_json = switch (cfg.provider) {
        .antfly, .openai, .openrouter, .ollama, .anthropic, .bedrock => try std.fmt.allocPrint(a, "{{\"type\":\"json_schema\",\"json_schema\":{{\"name\":\"antfly_context_memory\",\"strict\":true,\"schema\":{s}}}}}", .{summary_schema}),
        else => null,
    };
    return cfg;
}

fn Adapter(comptime Runner: type) type {
    return struct {
        runner: Runner,
        cfg: g.GeneratorConfig,
        fn run(raw: *anyopaque, a: std.mem.Allocator, frame: []const u8, _: usize) !preparation.Summary {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const messages = [_]g.ChatMessage{
                .{ .role = .system, .content = .{ .text = planner.summary_instruction } },
                .{ .role = .user, .content = .{ .text = frame } },
            };
            var result = try self.runner.executeChain(a, &.{.{ .generator = self.cfg }}, &messages);
            defer result.deinit();
            if (result.finish_reason == .length) return error.ContextSummaryTruncated;
            if (result.tool_calls.len > 0 or result.finish_reason == .content_filter) return error.InvalidContextSummary;
            return .{ .text = try a.dupe(u8, result.content), .input_tokens = if (result.usage) |u| @intCast(u.input_tokens) else 0, .output_tokens = if (result.usage) |u| @intCast(u.output_tokens) else 0 };
        }
        fn count(raw: *anyopaque, input: planner.CountInput) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Count the actual summarizer framing/schema with its independent
            // capacity, rather than reusing the destination tokenizer/budget.
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const a = arena.allocator();
            const frame = try planner.buildCompactionFrame(a, input.messages, input.keep, input.rendered_memory);
            const messages = [_]g.ChatMessage{
                .{ .role = .system, .content = .{ .text = planner.summary_instruction } },
                .{ .role = .user, .content = .{ .text = frame } },
            };
            const views = try viewMessages(a, &messages);
            var c = Counter{ .allocator = a, .source = &messages, .chain = &.{.{ .generator = self.cfg }} };
            return Counter.count(&c, .{ .messages = views, .keep = &.{ true, true } });
        }
    };
}

/// Runner is the existing request-owned provider dispatch (including its
/// deadline, cancellation, admission and call budget). This call never invokes
/// agent preparation recursively and never creates a fallback chain.
pub fn prepare(a: std.mem.Allocator, runner: anytype, config: Config, chain: []const g.ChainLink, source: []const g.ChatMessage, previous: State, control: preparation.Control) !Prepared {
    if (chain.len == 0 or config.context_window_tokens <= config.reserved_output_tokens or
        config.summarizer_context_window_tokens <= config.summary_output_tokens or config.summary_output_tokens > 32768 or
        config.context_window_tokens > 16 * 1024 * 1024 or config.summarizer_context_window_tokens > 16 * 1024 * 1024) return error.InvalidContextRequest;
    const target = config.target_input_tokens orelse (config.context_window_tokens - config.reserved_output_tokens);
    if (target > config.context_window_tokens - config.reserved_output_tokens) return error.InvalidContextRequest;
    for (chain) |link| if (link.generator.outputTokenBudget() > config.reserved_output_tokens) return error.ContextOutputReservationTooSmall;
    const cfg = try summaryConfig(a, config.summarizer orelse chain[0].generator, config.summary_output_tokens);
    var adapter = Adapter(@TypeOf(runner)){ .runner = runner, .cfg = cfg };
    const views = try viewMessages(a, source);
    var counter = Counter{ .allocator = a, .source = source, .chain = chain };
    const result = try preparation.prepare(a, views, previous, .{
        .mode = config.mode,
        .target_input_tokens = target,
        .reserved_output_tokens = config.reserved_output_tokens,
        .keep_recent_turns = config.keep_recent_turns,
        .summary_input_tokens = config.summarizer_context_window_tokens - config.summary_output_tokens,
        .summary_output_tokens = config.summary_output_tokens,
        .max_batches = config.max_summary_batches,
    }, .{ .ptr = &counter, .count_fn = Counter.count }, .{ .ptr = &adapter, .count_fn = @TypeOf(adapter).count }, .{ .ptr = &adapter, .run_fn = @TypeOf(adapter).run }, control);
    return .{ .messages = try effectiveMessages(a, source, result.next.keep, result.rendered_memory), .next = result.next, .event = .{ .diagnostics = result.diagnostics, .summary_provider = if (result.diagnostics.batches > 0) @tagName(cfg.provider) else null, .summary_model = if (result.diagnostics.batches > 0) cfg.model else null } };
}

const FakeRunner = struct {
    calls: usize = 0,
    expected_provider: g.Provider,
    truncated: bool = false,
    pub fn executeChain(self: *@This(), a: std.mem.Allocator, chain: []const g.ChainLink, messages: []const g.ChatMessage) !g.GenerateResult {
        self.calls += 1;
        try std.testing.expectEqual(@as(usize, 1), chain.len);
        try std.testing.expect(chain[0].retry == null);
        try std.testing.expectEqual(self.expected_provider, chain[0].generator.provider);
        try std.testing.expect(chain[0].generator.tools_json == null);
        try std.testing.expectEqualStrings(planner.summary_instruction, messages[0].content.?.text);
        return .{ .allocator = a, .finish_reason = if (self.truncated) .length else .stop, .content = try a.dupe(u8, "{\"conversation\":\"doc:42 deadline Friday\",\"tool_evidence\":[{\"tool_call_id\":\"c1\",\"tool_name\":\"search\",\"facts\":\"doc:42 deadline Friday\"}],\"unresolved_tool_calls\":[]}"), .usage = .{ .input_tokens = 700, .output_tokens = 70 } };
    }
};

test "context adapter dispatches independent native schema and remote prompted JSON summarizers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const long = try a.alloc(u8, 5000);
    @memset(long, 'x');
    const source = [_]g.ChatMessage{
        .{ .role = .system, .content = .{ .text = "policy" } },
        .{ .role = .user, .content = .{ .text = "question" } },
        .{ .role = .assistant, .tool_calls = &.{.{ .id = "c1", .name = "search", .arguments = "{}" }} },
        .{ .role = .tool, .tool_call_id = "c1", .content = .{ .text = long } },
        .{ .role = .assistant, .content = .{ .text = "continue" } },
    };
    const destination = g.GeneratorConfig{ .provider = .openai, .model = "main", .url = "https://example.invalid", .max_tokens = 100 };
    inline for (.{ g.Provider.antfly, .anthropic, .gemini, .bedrock, .apple, .chatgpt, .vertex, .cloudflare, .ollama, .openrouter, .mock, .openai }) |provider| {
        var fake = FakeRunner{ .expected_provider = provider };
        const result = try prepare(a, &fake, .{ .context_window_tokens = 3000, .target_input_tokens = 2000, .reserved_output_tokens = 1000, .summarizer_context_window_tokens = 16000, .summarizer = .{ .provider = provider, .model = "summary", .url = "" } }, &.{.{ .generator = destination }}, &source, .{}, .{});
        try std.testing.expectEqual(@as(usize, 1), fake.calls);
        try std.testing.expectEqualStrings(@tagName(provider), result.event.summary_provider.?);
        try std.testing.expectEqualStrings("summary", result.event.summary_model.?);
        try std.testing.expectEqual(@as(usize, 700), result.event.diagnostics.summary_input_tokens);
        try std.testing.expect(result.event.diagnostics.effective_input_tokens <= 2000);
        try std.testing.expectEqual(@as(usize, 5000), source[3].content.?.text.len);
        fake.truncated = true;
        try std.testing.expectError(error.ContextSummaryTruncated, prepare(a, &fake, .{
            .context_window_tokens = 3000,
            .target_input_tokens = 2000,
            .reserved_output_tokens = 1000,
            .summarizer_context_window_tokens = 16000,
            .summarizer = .{ .provider = provider, .model = "summary", .url = "" },
        }, &.{.{ .generator = destination }}, &source, .{}, .{}));
    }
}

test "context adapter inherits generator and avoids all summaries for fitting prompts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fake = FakeRunner{ .expected_provider = .openai };
    const cfg = g.GeneratorConfig{ .provider = .openai, .model = "main", .url = "https://example.invalid", .max_tokens = 100 };
    const chain = [_]g.ChainLink{.{ .generator = cfg }};
    const source = [_]g.ChatMessage{.{ .role = .user, .content = .{ .text = "question" } }};
    const result = try prepare(a, &fake, .{ .context_window_tokens = 6000 }, &chain, &source, .{}, .{});
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try std.testing.expect(result.event.summary_provider == null);
    try std.testing.expectEqualStrings("question", result.messages[0].content.?.text);
    try std.testing.expectError(error.ContextOutputReservationTooSmall, prepare(a, &fake, .{ .context_window_tokens = 6000, .reserved_output_tokens = 1 }, &chain, &source, .{}, .{}));
    try std.testing.expectError(error.InvalidContextRequest, prepare(a, &fake, .{ .context_window_tokens = 6000, .target_input_tokens = 5000 }, &chain, &source, .{}, .{}));
}
