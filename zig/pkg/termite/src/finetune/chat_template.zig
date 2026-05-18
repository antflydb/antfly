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

pub const TemplateKind = enum {
    llama3,
    gemma,
    chatml,
    alpaca,
};

pub const Role = enum { system, user, assistant, tool };

pub const Message = struct {
    role: Role,
    content: []const u8,
    name: ?[]const u8 = null,
    tool_call_id: ?[]const u8 = null,
    tool_calls_json: ?[]const u8 = null,
};

pub const AssistantSpan = struct {
    start: usize,
    end: usize,
};

pub const RenderResult = struct {
    allocator: std.mem.Allocator,
    text: []u8,
    assistant_spans: []AssistantSpan,

    pub fn deinit(self: *RenderResult) void {
        self.allocator.free(self.text);
        self.allocator.free(self.assistant_spans);
        self.* = undefined;
    }
};

pub const RenderOptions = struct {
    add_generation_prompt: bool = false,
};

pub const RenderError = error{
    UnsupportedRoleForTemplate,
    OutOfMemory,
};

fn roleStr(role: Role) []const u8 {
    return switch (role) {
        .system => "system",
        .user => "user",
        .assistant => "assistant",
        .tool => "tool",
    };
}

pub fn render(
    allocator: std.mem.Allocator,
    kind: TemplateKind,
    messages: []const Message,
    options: RenderOptions,
) RenderError!RenderResult {
    return switch (kind) {
        .llama3 => renderLlama3(allocator, messages, options),
        .gemma => renderGemma(allocator, messages, options),
        .chatml => renderChatml(allocator, messages, options),
        .alpaca => renderAlpaca(allocator, messages, options),
    };
}

fn renderLlama3(
    allocator: std.mem.Allocator,
    messages: []const Message,
    options: RenderOptions,
) RenderError!RenderResult {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    var spans: std.ArrayList(AssistantSpan) = .empty;
    errdefer spans.deinit(allocator);

    if (messages.len > 0) {
        try buf.appendSlice(allocator, "<|begin_of_text|>");
    }

    for (messages) |msg| {
        const role_name = roleStr(msg.role);
        const span_start = buf.items.len;
        try buf.appendSlice(allocator, "<|start_header_id|>");
        try buf.appendSlice(allocator, role_name);
        try buf.appendSlice(allocator, "<|end_header_id|>\n\n");
        try buf.appendSlice(allocator, msg.content);
        try buf.appendSlice(allocator, "<|eot_id|>");
        if (msg.role == .assistant) {
            try spans.append(allocator, .{ .start = span_start, .end = buf.items.len });
        }
    }

    if (options.add_generation_prompt) {
        try buf.appendSlice(allocator, "<|start_header_id|>assistant<|end_header_id|>\n\n");
    }

    return RenderResult{
        .allocator = allocator,
        .text = try buf.toOwnedSlice(allocator),
        .assistant_spans = try spans.toOwnedSlice(allocator),
    };
}

fn renderGemma(
    allocator: std.mem.Allocator,
    messages: []const Message,
    options: RenderOptions,
) RenderError!RenderResult {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    var spans: std.ArrayList(AssistantSpan) = .empty;
    errdefer spans.deinit(allocator);

    // Gather system content to prepend onto the first user turn.
    var pending_system: std.ArrayList(u8) = .empty;
    defer pending_system.deinit(allocator);

    for (messages) |msg| {
        switch (msg.role) {
            .system => {
                if (pending_system.items.len > 0) {
                    try pending_system.append(allocator, '\n');
                }
                try pending_system.appendSlice(allocator, msg.content);
            },
            .user => {
                try buf.appendSlice(allocator, "<start_of_turn>user\n");
                if (pending_system.items.len > 0) {
                    try buf.appendSlice(allocator, pending_system.items);
                    try buf.appendSlice(allocator, "\n\n");
                    pending_system.clearRetainingCapacity();
                }
                try buf.appendSlice(allocator, msg.content);
                try buf.appendSlice(allocator, "<end_of_turn>\n");
            },
            .assistant => {
                const span_start = buf.items.len;
                try buf.appendSlice(allocator, "<start_of_turn>model\n");
                if (msg.content.len > 0) {
                    try buf.appendSlice(allocator, msg.content);
                }
                if (msg.tool_calls_json) |tool_calls_json| {
                    if (msg.content.len > 0) try buf.append(allocator, '\n');
                    try buf.appendSlice(allocator, "<tool_call>\n");
                    try buf.appendSlice(allocator, tool_calls_json);
                    try buf.appendSlice(allocator, "\n</tool_call>");
                }
                try buf.appendSlice(allocator, "<end_of_turn>\n");
                try spans.append(allocator, .{ .start = span_start, .end = buf.items.len });
            },
            .tool => {
                try buf.appendSlice(allocator, "<start_of_turn>user\n");
                try buf.appendSlice(allocator, "<tool_response");
                if (msg.name) |name| {
                    try buf.appendSlice(allocator, " name=\"");
                    try buf.appendSlice(allocator, name);
                    try buf.append(allocator, '"');
                }
                if (msg.tool_call_id) |tool_call_id| {
                    try buf.appendSlice(allocator, " tool_call_id=\"");
                    try buf.appendSlice(allocator, tool_call_id);
                    try buf.append(allocator, '"');
                }
                try buf.appendSlice(allocator, ">\n");
                try buf.appendSlice(allocator, msg.content);
                try buf.appendSlice(allocator, "\n</tool_response><end_of_turn>\n");
            },
        }
    }

    // If system content never landed on a user turn (no user messages),
    // emit it as a synthetic user turn so nothing is silently dropped.
    if (pending_system.items.len > 0) {
        try buf.appendSlice(allocator, "<start_of_turn>user\n");
        try buf.appendSlice(allocator, pending_system.items);
        try buf.appendSlice(allocator, "<end_of_turn>\n");
        pending_system.clearRetainingCapacity();
    }

    if (options.add_generation_prompt) {
        try buf.appendSlice(allocator, "<start_of_turn>model\n");
    }

    return RenderResult{
        .allocator = allocator,
        .text = try buf.toOwnedSlice(allocator),
        .assistant_spans = try spans.toOwnedSlice(allocator),
    };
}

fn renderChatml(
    allocator: std.mem.Allocator,
    messages: []const Message,
    options: RenderOptions,
) RenderError!RenderResult {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    var spans: std.ArrayList(AssistantSpan) = .empty;
    errdefer spans.deinit(allocator);

    for (messages) |msg| {
        const role_name = roleStr(msg.role);
        const span_start = buf.items.len;
        try buf.appendSlice(allocator, "<|im_start|>");
        try buf.appendSlice(allocator, role_name);
        try buf.append(allocator, '\n');
        try buf.appendSlice(allocator, msg.content);
        try buf.appendSlice(allocator, "<|im_end|>\n");
        if (msg.role == .assistant) {
            try spans.append(allocator, .{ .start = span_start, .end = buf.items.len });
        }
    }

    if (options.add_generation_prompt) {
        try buf.appendSlice(allocator, "<|im_start|>assistant\n");
    }

    return RenderResult{
        .allocator = allocator,
        .text = try buf.toOwnedSlice(allocator),
        .assistant_spans = try spans.toOwnedSlice(allocator),
    };
}

fn renderAlpaca(
    allocator: std.mem.Allocator,
    messages: []const Message,
    options: RenderOptions,
) RenderError!RenderResult {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    var spans: std.ArrayList(AssistantSpan) = .empty;
    errdefer spans.deinit(allocator);

    for (messages) |msg| {
        switch (msg.role) {
            .tool => return error.UnsupportedRoleForTemplate,
            .system => {
                try buf.appendSlice(allocator, msg.content);
                try buf.appendSlice(allocator, "\n\n");
            },
            .user => {
                try buf.appendSlice(allocator, "### Instruction:\n");
                try buf.appendSlice(allocator, msg.content);
                try buf.appendSlice(allocator, "\n\n");
            },
            .assistant => {
                const span_start = buf.items.len;
                try buf.appendSlice(allocator, "### Response:\n");
                try buf.appendSlice(allocator, msg.content);
                try buf.appendSlice(allocator, "\n\n");
                try spans.append(allocator, .{ .start = span_start, .end = buf.items.len });
            },
        }
    }

    if (options.add_generation_prompt) {
        try buf.appendSlice(allocator, "### Response:\n");
    }

    return RenderResult{
        .allocator = allocator,
        .text = try buf.toOwnedSlice(allocator),
        .assistant_spans = try spans.toOwnedSlice(allocator),
    };
}

pub fn makeCompletionLabels(
    allocator: std.mem.Allocator,
    input_ids: []const i32,
    token_byte_offsets: []const usize,
    assistant_spans: []const AssistantSpan,
    ignore_label: i32,
) ![]i32 {
    std.debug.assert(input_ids.len == token_byte_offsets.len);
    const labels = try allocator.alloc(i32, input_ids.len);
    errdefer allocator.free(labels);

    for (input_ids, token_byte_offsets, 0..) |id, off, i| {
        var in_span = false;
        for (assistant_spans) |s| {
            if (off >= s.start and off < s.end) {
                in_span = true;
                break;
            }
        }
        labels[i] = if (in_span) id else ignore_label;
    }
    return labels;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "llama3 render system+user+assistant" {
    const allocator = std.testing.allocator;
    const messages = [_]Message{
        .{ .role = .system, .content = "You are helpful." },
        .{ .role = .user, .content = "Hi" },
        .{ .role = .assistant, .content = "Hello!" },
    };
    var result = try render(allocator, .llama3, &messages, .{});
    defer result.deinit();

    const expected =
        "<|begin_of_text|>" ++
        "<|start_header_id|>system<|end_header_id|>\n\nYou are helpful.<|eot_id|>" ++
        "<|start_header_id|>user<|end_header_id|>\n\nHi<|eot_id|>" ++
        "<|start_header_id|>assistant<|end_header_id|>\n\nHello!<|eot_id|>";
    try std.testing.expectEqualStrings(expected, result.text);
    try std.testing.expectEqual(@as(usize, 1), result.assistant_spans.len);

    const span = result.assistant_spans[0];
    const expected_span_start =
        "<|begin_of_text|>".len +
        "<|start_header_id|>system<|end_header_id|>\n\nYou are helpful.<|eot_id|>".len +
        "<|start_header_id|>user<|end_header_id|>\n\nHi<|eot_id|>".len;
    try std.testing.expectEqual(expected_span_start, span.start);
    try std.testing.expectEqual(result.text.len, span.end);
    try std.testing.expectEqualStrings(
        "<|start_header_id|>assistant<|end_header_id|>\n\nHello!<|eot_id|>",
        result.text[span.start..span.end],
    );
}

test "gemma render merges system into first user turn; assistant is model" {
    const allocator = std.testing.allocator;
    const messages = [_]Message{
        .{ .role = .system, .content = "Be terse." },
        .{ .role = .user, .content = "2+2?" },
        .{ .role = .assistant, .content = "4" },
    };
    var result = try render(allocator, .gemma, &messages, .{});
    defer result.deinit();

    const expected =
        "<start_of_turn>user\nBe terse.\n\n2+2?<end_of_turn>\n" ++
        "<start_of_turn>model\n4<end_of_turn>\n";
    try std.testing.expectEqualStrings(expected, result.text);
    try std.testing.expectEqual(@as(usize, 1), result.assistant_spans.len);
    const span = result.assistant_spans[0];
    try std.testing.expectEqualStrings(
        "<start_of_turn>model\n4<end_of_turn>\n",
        result.text[span.start..span.end],
    );
}

test "gemma render supports tool turns and assistant tool calls" {
    const allocator = std.testing.allocator;
    const messages = [_]Message{
        .{ .role = .user, .content = "list files" },
        .{ .role = .assistant, .content = "Checking", .tool_calls_json = "{\"id\":\"call_1\",\"name\":\"shell\",\"arguments\":{\"cmd\":\"ls\"}}" },
        .{ .role = .tool, .name = "shell", .tool_call_id = "call_1", .content = "file.txt" },
        .{ .role = .assistant, .content = "Found file.txt" },
    };
    var result = try render(allocator, .gemma, &messages, .{});
    defer result.deinit();

    try std.testing.expect(std.mem.indexOf(u8, result.text, "<tool_call>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.text, "<tool_response name=\"shell\" tool_call_id=\"call_1\">") != null);
    try std.testing.expectEqual(@as(usize, 2), result.assistant_spans.len);
}

test "chatml render with tool role" {
    const allocator = std.testing.allocator;
    const messages = [_]Message{
        .{ .role = .system, .content = "sys" },
        .{ .role = .user, .content = "call weather" },
        .{ .role = .assistant, .content = "calling..." },
        .{ .role = .tool, .content = "{\"temp\":72}" },
        .{ .role = .assistant, .content = "It's 72." },
    };
    var result = try render(allocator, .chatml, &messages, .{});
    defer result.deinit();

    const expected =
        "<|im_start|>system\nsys<|im_end|>\n" ++
        "<|im_start|>user\ncall weather<|im_end|>\n" ++
        "<|im_start|>assistant\ncalling...<|im_end|>\n" ++
        "<|im_start|>tool\n{\"temp\":72}<|im_end|>\n" ++
        "<|im_start|>assistant\nIt's 72.<|im_end|>\n";
    try std.testing.expectEqualStrings(expected, result.text);
    try std.testing.expectEqual(@as(usize, 2), result.assistant_spans.len);
}

test "alpaca rejects tool role" {
    const allocator = std.testing.allocator;
    const messages = [_]Message{
        .{ .role = .user, .content = "do a thing" },
        .{ .role = .tool, .content = "result" },
    };
    const err = render(allocator, .alpaca, &messages, .{});
    try std.testing.expectError(error.UnsupportedRoleForTemplate, err);
}

test "two assistant turns produce two spans" {
    const allocator = std.testing.allocator;
    const messages = [_]Message{
        .{ .role = .user, .content = "q1" },
        .{ .role = .assistant, .content = "a1" },
        .{ .role = .user, .content = "q2" },
        .{ .role = .assistant, .content = "a2" },
    };
    var result = try render(allocator, .chatml, &messages, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.assistant_spans.len);
    try std.testing.expectEqualStrings(
        "<|im_start|>assistant\na1<|im_end|>\n",
        result.text[result.assistant_spans[0].start..result.assistant_spans[0].end],
    );
    try std.testing.expectEqualStrings(
        "<|im_start|>assistant\na2<|im_end|>\n",
        result.text[result.assistant_spans[1].start..result.assistant_spans[1].end],
    );
}

test "add_generation_prompt appends suffix without adding a span" {
    const allocator = std.testing.allocator;
    const messages = [_]Message{
        .{ .role = .user, .content = "hi" },
    };

    {
        var r = try render(allocator, .llama3, &messages, .{ .add_generation_prompt = true });
        defer r.deinit();
        try std.testing.expect(std.mem.endsWith(u8, r.text, "<|start_header_id|>assistant<|end_header_id|>\n\n"));
        try std.testing.expectEqual(@as(usize, 0), r.assistant_spans.len);
    }
    {
        var r = try render(allocator, .gemma, &messages, .{ .add_generation_prompt = true });
        defer r.deinit();
        try std.testing.expect(std.mem.endsWith(u8, r.text, "<start_of_turn>model\n"));
        try std.testing.expectEqual(@as(usize, 0), r.assistant_spans.len);
    }
    {
        var r = try render(allocator, .chatml, &messages, .{ .add_generation_prompt = true });
        defer r.deinit();
        try std.testing.expect(std.mem.endsWith(u8, r.text, "<|im_start|>assistant\n"));
        try std.testing.expectEqual(@as(usize, 0), r.assistant_spans.len);
    }
    {
        var r = try render(allocator, .alpaca, &messages, .{ .add_generation_prompt = true });
        defer r.deinit();
        try std.testing.expect(std.mem.endsWith(u8, r.text, "### Response:\n"));
        try std.testing.expectEqual(@as(usize, 0), r.assistant_spans.len);
    }
}

test "makeCompletionLabels masks tokens outside assistant spans" {
    const allocator = std.testing.allocator;
    const input_ids = [_]i32{ 10, 11, 12, 13, 14, 15 };
    //   offsets:      0    5    10   20   25   30
    //   span:         [20, 30)  -> indices 3, 4
    const offsets = [_]usize{ 0, 5, 10, 20, 25, 30 };
    const spans = [_]AssistantSpan{.{ .start = 20, .end = 30 }};

    const labels = try makeCompletionLabels(allocator, &input_ids, &offsets, &spans, -100);
    defer allocator.free(labels);

    try std.testing.expectEqual(@as(i32, -100), labels[0]);
    try std.testing.expectEqual(@as(i32, -100), labels[1]);
    try std.testing.expectEqual(@as(i32, -100), labels[2]);
    try std.testing.expectEqual(@as(i32, 13), labels[3]);
    try std.testing.expectEqual(@as(i32, 14), labels[4]);
    try std.testing.expectEqual(@as(i32, -100), labels[5]);
}

test "empty messages renders empty string and empty spans" {
    const allocator = std.testing.allocator;
    const messages = [_]Message{};
    inline for (.{ TemplateKind.llama3, .gemma, .chatml, .alpaca }) |k| {
        var r = try render(allocator, k, &messages, .{});
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, 0), r.text.len);
        try std.testing.expectEqual(@as(usize, 0), r.assistant_spans.len);
    }
}
