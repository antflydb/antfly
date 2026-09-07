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

// Jinja2 template engine for Zig.
//
// Implements the HuggingFace chat template subset of Jinja2:
// variable interpolation, for loops with loop vars, if/elif/else,
// set statements, dot/subscript access, string concat, filters,
// whitespace control, and escape sequences.
//
// Usage:
//     const jinja = @import("jinja");
//     var ctx = jinja.ValueMap{};
//     try ctx.put(arena, "name", jinja.Value.str("World"));
//     const result = try jinja.render(arena, "Hello {{ name }}!", &ctx);

const std = @import("std");
pub const ast = @import("ast.zig");
pub const Lexer = @import("lexer.zig").Lexer;
pub const Token = @import("lexer.zig").Token;
pub const TokenKind = @import("lexer.zig").TokenKind;
pub const Parser = @import("parser.zig").Parser;
pub const ParseError = @import("parser.zig").ParseError;
pub const Eval = @import("eval.zig").Eval;
pub const Value = @import("eval.zig").Value;
pub const ValueMap = @import("eval.zig").ValueMap;

/// A parsed Jinja2 template ready for rendering.
pub const Template = struct {
    nodes: []const *ast.Node,
    parse_arena: std.heap.ArenaAllocator,

    pub const Options = struct {
        trim_blocks: bool = false,
        lstrip_blocks: bool = false,
    };

    /// Parse a template string. The template owns the AST memory.
    pub fn init(backing_allocator: std.mem.Allocator, source: []const u8) !Template {
        return initWithOptions(backing_allocator, source, .{});
    }

    /// Match the Jinja environment used by Hugging Face chat templates.
    pub fn initHuggingFace(backing_allocator: std.mem.Allocator, source: []const u8) !Template {
        return initWithOptions(backing_allocator, source, .{
            .trim_blocks = true,
            .lstrip_blocks = true,
        });
    }

    pub fn initWithOptions(
        backing_allocator: std.mem.Allocator,
        source: []const u8,
        options: Options,
    ) !Template {
        var parse_arena = std.heap.ArenaAllocator.init(backing_allocator);
        errdefer parse_arena.deinit();

        const parse_source = if (options.trim_blocks or options.lstrip_blocks)
            try normalizeEnvironmentWhitespace(parse_arena.allocator(), source, options)
        else
            source;
        const nodes = try Parser.parse(parse_source, parse_arena.allocator());
        return .{
            .nodes = nodes,
            .parse_arena = parse_arena,
        };
    }

    pub fn deinit(self: *Template) void {
        self.parse_arena.deinit();
    }

    /// Render the template with the given context.
    /// The returned string is allocated from the provided arena.
    pub fn render(self: *const Template, arena: std.mem.Allocator, context: *ValueMap) ![]const u8 {
        var eval = Eval.init(arena, context);
        return eval.exec(self.nodes);
    }
};

fn normalizeEnvironmentWhitespace(
    allocator: std.mem.Allocator,
    source: []const u8,
    options: Template.Options,
) ![]const u8 {
    var result = std.ArrayListUnmanaged(u8).empty;
    var cursor: usize = 0;
    while (cursor < source.len) {
        const relative_open = std.mem.indexOfPos(u8, source, cursor, "{%") orelse {
            try result.appendSlice(allocator, source[cursor..]);
            break;
        };
        try result.appendSlice(allocator, source[cursor..relative_open]);

        if (options.lstrip_blocks) {
            // Determine line boundaries from the source. trim_blocks may have
            // consumed a newline without copying it to result, so walking the
            // output can incorrectly treat the preceding tag as part of this
            // source line and retain the next tag's indentation.
            var line_start = relative_open;
            while (line_start > 0 and source[line_start - 1] != '\n' and source[line_start - 1] != '\r') {
                line_start -= 1;
            }
            var whitespace_only = true;
            for (source[line_start..relative_open]) |byte| {
                if (byte != ' ' and byte != '\t') {
                    whitespace_only = false;
                    break;
                }
            }
            if (whitespace_only) {
                const copied_line_start = @max(line_start, cursor);
                result.items.len -= relative_open - copied_line_start;
            }
        }

        const relative_close = findStatementClose(source, relative_open + 2) orelse {
            try result.appendSlice(allocator, source[relative_open..]);
            break;
        };
        try result.appendSlice(allocator, source[relative_open .. relative_close + 2]);
        cursor = relative_close + 2;

        if (options.trim_blocks) {
            if (cursor < source.len and source[cursor] == '\n') {
                cursor += 1;
            } else if (cursor + 1 < source.len and source[cursor] == '\r' and source[cursor + 1] == '\n') {
                cursor += 2;
            }
        }
    }
    return result.toOwnedSlice(allocator);
}

fn findStatementClose(source: []const u8, start: usize) ?usize {
    var quote: ?u8 = null;
    var escaped = false;
    var cursor = start;
    while (cursor + 1 < source.len) : (cursor += 1) {
        const byte = source[cursor];
        if (quote) |delimiter| {
            if (escaped) {
                escaped = false;
            } else if (byte == '\\') {
                escaped = true;
            } else if (byte == delimiter) {
                quote = null;
            }
            continue;
        }
        if (byte == '\'' or byte == '"') {
            quote = byte;
        } else if (byte == '%' and source[cursor + 1] == '}') {
            return cursor;
        }
    }
    return null;
}

/// Parse and render a template in one call.
pub fn render(arena: std.mem.Allocator, source: []const u8, context: *ValueMap) ![]const u8 {
    const nodes = try Parser.parse(source, arena);
    var eval = Eval.init(arena, context);
    return eval.exec(nodes);
}

// --- Chat template helpers ---

/// Build a Jinja2 context for rendering HuggingFace chat templates.
/// Creates the standard variables: messages, bos_token, eos_token, etc.
pub fn chatTemplateContext(
    arena: std.mem.Allocator,
    messages: []const ChatMessage,
    options: ChatTemplateOptions,
) !ValueMap {
    // Convert messages to Value list
    const msg_values = try arena.alloc(Value, messages.len);
    for (messages, 0..) |msg, i| {
        var m = ValueMap{};
        try m.put(arena, "role", Value.str(msg.role));
        if (msg.parts) |parts| {
            const part_values = try arena.alloc(Value, parts.len);
            for (parts, 0..) |part, part_idx| {
                var part_map = ValueMap{};
                switch (part) {
                    .text => |text| {
                        try part_map.put(arena, "type", Value.str("text"));
                        try part_map.put(arena, "text", Value.str(text));
                    },
                    .image => {
                        try part_map.put(arena, "type", Value.str("image"));
                    },
                    .audio => {
                        try part_map.put(arena, "type", Value.str("audio"));
                    },
                }
                part_values[part_idx] = .{ .map = part_map };
            }
            try m.put(arena, "content", .{ .list = part_values });
        } else {
            try m.put(arena, "content", if (msg.content_is_null) .none else Value.str(msg.content));
        }
        if (msg.tool_calls) |calls| {
            const values = try arena.alloc(Value, calls.len);
            for (calls, 0..) |call, idx| {
                const arguments = try std.json.parseFromSliceLeaky(std.json.Value, arena, call.arguments, .{});
                if (arguments != .object) return error.InvalidToolArguments;
                var function = ValueMap{};
                try function.put(arena, "name", Value.str(call.name));
                try function.put(arena, "arguments", try jsonToTemplateValue(arena, arguments));
                var tool = ValueMap{};
                try tool.put(arena, "id", Value.str(call.id));
                try tool.put(arena, "type", Value.str(call.type));
                try tool.put(arena, "function", .{ .map = function });
                values[idx] = .{ .map = tool };
            }
            try m.put(arena, "tool_calls", .{ .list = values });
        }
        if (msg.tool_call_id) |id| try m.put(arena, "tool_call_id", Value.str(id));
        msg_values[i] = .{ .map = m };
    }

    var ctx = ValueMap{};
    try ctx.put(arena, "messages", .{ .list = msg_values });
    try ctx.put(arena, "add_generation_prompt", Value.bln(options.add_generation_prompt));
    try ctx.put(arena, "bos_token", Value.str(options.bos_token));
    try ctx.put(arena, "eos_token", Value.str(options.eos_token));
    try ctx.put(arena, "unk_token", Value.str(options.unk_token));
    try ctx.put(arena, "pad_token", Value.str(options.pad_token));
    if (options.enable_thinking) |enabled| {
        try ctx.put(arena, "enable_thinking", Value.bln(enabled));
    }

    return ctx;
}

pub const ChatMessage = struct {
    role: []const u8,
    content: []const u8,
    parts: ?[]const ChatContentPart = null,
    content_is_null: bool = false,
    tool_calls: ?[]const ChatToolCall = null,
    tool_call_id: ?[]const u8 = null,
};

/// Borrowed OpenAI function-call history. Arguments are JSON on the wire and
/// become an object in the Hugging Face template context (not a quoted string).
pub const ChatToolCall = struct {
    id: []const u8,
    type: []const u8 = "function",
    name: []const u8,
    arguments: []const u8,
};

fn jsonToTemplateValue(arena: std.mem.Allocator, value: std.json.Value) !Value {
    return switch (value) {
        .null => .none,
        .bool => |v| Value.bln(v),
        .integer => |v| Value.int(v),
        .float => |v| .{ .float = v },
        .number_string => |v| .{ .float = try std.fmt.parseFloat(f64, v) },
        .string => |v| Value.str(v),
        .array => |items| blk: {
            const values = try arena.alloc(Value, items.items.len);
            for (items.items, values) |item, *out| out.* = try jsonToTemplateValue(arena, item);
            break :blk .{ .list = values };
        },
        .object => |object| blk: {
            var map = ValueMap{};
            var iter = object.iterator();
            while (iter.next()) |entry| try map.put(arena, entry.key_ptr.*, try jsonToTemplateValue(arena, entry.value_ptr.*));
            break :blk .{ .map = map };
        },
    };
}

pub const ChatContentPart = union(enum) {
    text: []const u8,
    image,
    audio,
};

pub const ChatTemplateOptions = struct {
    add_generation_prompt: bool = true,
    bos_token: []const u8 = "",
    eos_token: []const u8 = "",
    unk_token: []const u8 = "",
    pad_token: []const u8 = "",
    /// Optional Hugging Face/llama.cpp-compatible template variable. Omit it
    /// to preserve each model template's own default reasoning behavior.
    enable_thinking: ?bool = null,
};

// --- Tests ---

test "tool history validates arguments and preserves JSON value types" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var messages = [_]ChatMessage{.{ .role = "assistant", .content = "", .tool_calls = &.{.{
        .id = "c1",
        .name = "search",
        .arguments = "{\"nested\":[true,null,2,1.5,{\"text\":\"a\\\"b\"}]}",
    }} }};
    const context = try chatTemplateContext(arena, &messages, .{});
    const args = context.get("messages").?.list[0].map.get("tool_calls").?.list[0].map.get("function").?.map.get("arguments").?.map;
    const nested = args.get("nested").?.list;
    try std.testing.expect(nested[0].boolean);
    try std.testing.expect(nested[1] == .none);
    try std.testing.expectEqual(@as(i64, 2), nested[2].integer);
    try std.testing.expectEqual(@as(f64, 1.5), nested[3].float);
    try std.testing.expectEqualStrings("a\"b", nested[4].map.get("text").?.string);
    messages[0].tool_calls = &.{.{ .id = "bad", .name = "search", .arguments = "[]" }};
    try std.testing.expectError(error.InvalidToolArguments, chatTemplateContext(arena, &messages, .{}));
}

test "render simple" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var ctx = ValueMap{};
    try ctx.put(arena, "name", Value.str("World"));
    const result = try render(arena, "Hello {{ name }}!", &ctx);
    try std.testing.expectEqualStrings("Hello World!", result);
}

test "Template struct" {
    var tpl = try Template.init(std.testing.allocator, "{{ greeting }}, {{ name }}!");
    defer tpl.deinit();

    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var ctx = ValueMap{};
    try ctx.put(arena, "greeting", Value.str("Hello"));
    try ctx.put(arena, "name", Value.str("World"));
    const result = try tpl.render(arena, &ctx);
    try std.testing.expectEqualStrings("Hello, World!", result);
}

test "Hugging Face template environment trims block lines" {
    var tpl = try Template.initHuggingFace(
        std.testing.allocator,
        "  {%- if enabled %}\n{{ 'assistant\\n' }}\n{%- endif %}\n",
    );
    defer tpl.deinit();

    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var ctx = ValueMap{};
    try ctx.put(arena, "enabled", Value.bln(true));
    const result = try tpl.render(arena, &ctx);
    try std.testing.expectEqualStrings("assistant\n", result);
}

test "Hugging Face lstrip uses source line after a trimmed newline" {
    var tpl = try Template.initHuggingFace(
        std.testing.allocator,
        "{% if enabled %}\n    {% set marker = 'ok' %}\n{{ marker }}\n{% endif %}\n",
    );
    defer tpl.deinit();

    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var ctx = ValueMap{};
    try ctx.put(arena, "enabled", Value.bln(true));
    const result = try tpl.render(arena, &ctx);
    try std.testing.expectEqualStrings("ok\n", result);
}

test "Hugging Face whitespace scan ignores quoted statement terminators" {
    var tpl = try Template.initHuggingFace(
        std.testing.allocator,
        "{% set marker = '%}' %}\n{{ marker }}",
    );
    defer tpl.deinit();

    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    var ctx = ValueMap{};
    const result = try tpl.render(arena_state.allocator(), &ctx);
    try std.testing.expectEqualStrings("%}", result);
}

test "render chat template pattern" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const messages = [_]ChatMessage{
        .{ .role = "user", .content = "Hello" },
    };
    var ctx = try chatTemplateContext(arena, &messages, .{
        .bos_token = "<bos>",
        .eos_token = "<eos>",
    });

    // Simple ChatML-like template
    const template =
        \\{{ bos_token }}{% for message in messages %}<|{{ message.role }}|>
        \\{{ message.content }}
        \\{% endfor %}{% if add_generation_prompt %}<|assistant|>
        \\{% endif %}
    ;

    const result = try render(arena, template, &ctx);
    try std.testing.expect(std.mem.indexOf(u8, result, "<bos>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Hello") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<|assistant|>") != null);
}

test "render with whitespace control" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{ Value.str("a"), Value.str("b") };
    var ctx = ValueMap{};
    try ctx.put(arena, "items", .{ .list = &items });

    const result = try render(arena, "{%- for x in items -%}{{ x }}{%- endfor -%}", &ctx);
    try std.testing.expectEqualStrings("ab", result);
}

test "gemma3 chat template single turn" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const messages = [_]ChatMessage{
        .{ .role = "user", .content = "The capital of France is" },
    };
    var ctx = try chatTemplateContext(arena, &messages, .{
        .bos_token = "<bos>",
        .add_generation_prompt = true,
    });

    // Simplified Gemma3 template (no system message handling)
    const template =
        "{{ bos_token }}" ++
        "{%- for message in messages -%}" ++
        "{{ '<start_of_turn>' + message['role'] + '\\n' + message['content'] + '<end_of_turn>\\n' }}" ++
        "{%- endfor -%}" ++
        "{%- if add_generation_prompt -%}" ++
        "{{ '<start_of_turn>model\\n' }}" ++
        "{%- endif -%}";

    const result = try render(arena, template, &ctx);
    try std.testing.expectEqualStrings(
        "<bos><start_of_turn>user\nThe capital of France is<end_of_turn>\n<start_of_turn>model\n",
        result,
    );
}

test "gemma3 chat template multi turn" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const messages = [_]ChatMessage{
        .{ .role = "user", .content = "Hello" },
        .{ .role = "assistant", .content = "Hi there!" },
        .{ .role = "user", .content = "What is 2+2?" },
    };
    var ctx = try chatTemplateContext(arena, &messages, .{
        .bos_token = "<bos>",
        .add_generation_prompt = true,
    });

    const template =
        "{{ bos_token }}" ++
        "{%- for message in messages -%}" ++
        "{{ '<start_of_turn>' + message['role'] + '\\n' + message['content'] + '<end_of_turn>\\n' }}" ++
        "{%- endfor -%}" ++
        "{%- if add_generation_prompt -%}" ++
        "{{ '<start_of_turn>model\\n' }}" ++
        "{%- endif -%}";

    const result = try render(arena, template, &ctx);
    try std.testing.expectEqualStrings(
        "<bos><start_of_turn>user\nHello<end_of_turn>\n<start_of_turn>assistant\nHi there!<end_of_turn>\n<start_of_turn>user\nWhat is 2+2?<end_of_turn>\n<start_of_turn>model\n",
        result,
    );
}

test "chatml chat template" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const messages = [_]ChatMessage{
        .{ .role = "system", .content = "You are helpful." },
        .{ .role = "user", .content = "Hi" },
    };
    var ctx = try chatTemplateContext(arena, &messages, .{
        .add_generation_prompt = true,
    });

    // Standard ChatML template (used by many models: Qwen, Yi, etc.)
    const template =
        "{%- for message in messages -%}" ++
        "{{ '<|im_start|>' + message['role'] + '\\n' + message['content'] + '<|im_end|>\\n' }}" ++
        "{%- endfor -%}" ++
        "{%- if add_generation_prompt -%}" ++
        "{{ '<|im_start|>assistant\\n' }}" ++
        "{%- endif -%}";

    const result = try render(arena, template, &ctx);
    try std.testing.expectEqualStrings(
        "<|im_start|>system\nYou are helpful.<|im_end|>\n<|im_start|>user\nHi<|im_end|>\n<|im_start|>assistant\n",
        result,
    );
}

test "llama3 chat template with loop.first" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const messages = [_]ChatMessage{
        .{ .role = "user", .content = "Hello" },
        .{ .role = "assistant", .content = "Hi!" },
        .{ .role = "user", .content = "Bye" },
    };
    var ctx = try chatTemplateContext(arena, &messages, .{
        .bos_token = "<|begin_of_text|>",
        .add_generation_prompt = true,
    });

    // Simplified Llama3-style template using loop.first for BOS
    const template =
        "{%- for message in messages -%}" ++
        "{%- if loop.first -%}{{ bos_token }}{%- endif -%}" ++
        "{{ '<|start_header_id|>' + message['role'] + '<|end_header_id|>\\n\\n' + message['content'] + '<|eot_id|>' }}" ++
        "{%- endfor -%}" ++
        "{%- if add_generation_prompt -%}" ++
        "{{ '<|start_header_id|>assistant<|end_header_id|>\\n\\n' }}" ++
        "{%- endif -%}";

    const result = try render(arena, template, &ctx);
    try std.testing.expectEqualStrings(
        "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n\nHello<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\nHi!<|eot_id|><|start_header_id|>user<|end_header_id|>\n\nBye<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n",
        result,
    );
}

test "template with set and system message handling" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const messages = [_]ChatMessage{
        .{ .role = "system", .content = "Be helpful." },
        .{ .role = "user", .content = "Hello" },
    };
    var ctx = try chatTemplateContext(arena, &messages, .{
        .bos_token = "<bos>",
        .add_generation_prompt = true,
    });

    // Template that extracts system message via set + if + slice
    const template =
        "{%- if messages[0]['role'] == 'system' -%}" ++
        "{%- set system_message = messages[0]['content'] -%}" ++
        "{%- set messages = messages[1:] -%}" ++
        "{%- else -%}" ++
        "{%- set system_message = '' -%}" ++
        "{%- endif -%}" ++
        "{{ bos_token }}" ++
        "{%- for message in messages -%}" ++
        "{%- if message['role'] == 'user' and loop.first and system_message != '' -%}" ++
        "{{ '<start_of_turn>user\\n' + system_message + '\\n\\n' + message['content'] + '<end_of_turn>\\n' }}" ++
        "{%- else -%}" ++
        "{{ '<start_of_turn>' + message['role'] + '\\n' + message['content'] + '<end_of_turn>\\n' }}" ++
        "{%- endif -%}" ++
        "{%- endfor -%}" ++
        "{%- if add_generation_prompt -%}" ++
        "{{ '<start_of_turn>model\\n' }}" ++
        "{%- endif -%}";

    const result = try render(arena, template, &ctx);
    try std.testing.expectEqualStrings(
        "<bos><start_of_turn>user\nBe helpful.\n\nHello<end_of_turn>\n<start_of_turn>model\n",
        result,
    );
}

test "gemma3 tokenizer_config chat template parses and renders" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const messages = [_]ChatMessage{
        .{ .role = "system", .content = "Be helpful." },
        .{ .role = "user", .content = "Hello" },
    };
    var ctx = try chatTemplateContext(arena, &messages, .{
        .bos_token = "<bos>",
        .add_generation_prompt = true,
    });

    const template =
        "{{ bos_token }}\n" ++
        "{%- if messages[0]['role'] == 'system' -%}\n" ++
        "    {%- if messages[0]['content'] is string -%}\n" ++
        "        {%- set first_user_prefix = messages[0]['content'] + '\\n\\n' -%}\n" ++
        "    {%- else -%}\n" ++
        "        {%- set first_user_prefix = messages[0]['content'][0]['text'] + '\\n\\n' -%}\n" ++
        "    {%- endif -%}\n" ++
        "    {%- set loop_messages = messages[1:] -%}\n" ++
        "{%- else -%}\n" ++
        "    {%- set first_user_prefix = \"\" -%}\n" ++
        "    {%- set loop_messages = messages -%}\n" ++
        "{%- endif -%}\n" ++
        "{%- for message in loop_messages -%}\n" ++
        "    {%- if (message['role'] == 'user') != (loop.index0 % 2 == 0) -%}\n" ++
        "        {{ raise_exception(\"Conversation roles must alternate user/assistant/user/assistant/...\") }}\n" ++
        "    {%- endif -%}\n" ++
        "    {%- if (message['role'] == 'assistant') -%}\n" ++
        "        {%- set role = \"model\" -%}\n" ++
        "    {%- else -%}\n" ++
        "        {%- set role = message['role'] -%}\n" ++
        "    {%- endif -%}\n" ++
        "    {{ '<start_of_turn>' + role + '\\n' + (first_user_prefix if loop.first else \"\") }}\n" ++
        "    {%- if message['content'] is string -%}\n" ++
        "        {{ message['content'] | trim }}\n" ++
        "    {%- elif message['content'] is iterable -%}\n" ++
        "        {%- for item in message['content'] -%}\n" ++
        "            {%- if item['type'] == 'image' -%}\n" ++
        "                {{ '<start_of_image>' }}\n" ++
        "            {%- elif item['type'] == 'text' -%}\n" ++
        "                {{ item['text'] | trim }}\n" ++
        "            {%- endif -%}\n" ++
        "        {%- endfor -%}\n" ++
        "    {%- else -%}\n" ++
        "        {{ raise_exception(\"Invalid content type\") }}\n" ++
        "    {%- endif -%}\n" ++
        "    {{ '<end_of_turn>\\n' }}\n" ++
        "{%- endfor -%}\n" ++
        "{%- if add_generation_prompt -%}\n" ++
        "    {{'<start_of_turn>model\\n'}}\n" ++
        "{%- endif -%}\n";

    const result = try render(arena, template, &ctx);
    try std.testing.expectEqualStrings(
        "<bos><start_of_turn>user\nBe helpful.\n\nHello<end_of_turn>\n<start_of_turn>model\n",
        result,
    );
}

test "gemma3 tokenizer_config chat template renders multimodal content parts" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const parts = [_]ChatContentPart{
        .image,
        .{ .text = "Describe this image." },
    };
    const messages = [_]ChatMessage{
        .{ .role = "user", .content = "", .parts = &parts },
    };
    var ctx = try chatTemplateContext(arena, &messages, .{
        .bos_token = "<bos>",
        .add_generation_prompt = true,
    });

    const template =
        "{{ bos_token }}" ++
        "{%- for message in messages -%}" ++
        "{{ '<start_of_turn>' + message['role'] + '\\n' }}" ++
        "{%- if message['content'] is iterable -%}" ++
        "{%- for item in message['content'] -%}" ++
        "{%- if item['type'] == 'image' -%}{{ '<start_of_image>' }}{%- endif -%}" ++
        "{%- if item['type'] == 'text' -%}{{ item['text'] }}{%- endif -%}" ++
        "{%- endfor -%}" ++
        "{%- endif -%}" ++
        "{{ '<end_of_turn>\\n' }}" ++
        "{%- endfor -%}" ++
        "{%- if add_generation_prompt -%}{{ '<start_of_turn>model\\n' }}{%- endif -%}";

    const result = try render(arena, template, &ctx);
    try std.testing.expectEqualStrings(
        "<bos><start_of_turn>user\n<start_of_image>Describe this image.<end_of_turn>\n<start_of_turn>model\n",
        result,
    );
}

test "gemma4 chat template single turn" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const messages = [_]ChatMessage{
        .{ .role = "user", .content = "What is the capital of France?" },
    };
    var ctx = try chatTemplateContext(arena, &messages, .{
        .bos_token = "<bos>",
        .add_generation_prompt = true,
    });

    // Gemma 4 template uses <|turn>/<turn|> instead of <start_of_turn>/<end_of_turn>
    const template =
        "{{ bos_token }}" ++
        "{%- if messages[0]['role'] == 'system' -%}" ++
        "{%- if messages[0]['content'] is string -%}" ++
        "{%- set first_user_prefix = messages[0]['content'] + '\\n\\n' -%}" ++
        "{%- else -%}" ++
        "{%- set first_user_prefix = messages[0]['content'][0]['text'] + '\\n\\n' -%}" ++
        "{%- endif -%}" ++
        "{%- set loop_messages = messages[1:] -%}" ++
        "{%- else -%}" ++
        "{%- set first_user_prefix = \"\" -%}" ++
        "{%- set loop_messages = messages -%}" ++
        "{%- endif -%}" ++
        "{%- for message in loop_messages -%}" ++
        "{%- if (message['role'] == 'assistant') -%}" ++
        "{%- set role = \"model\" -%}" ++
        "{%- else -%}" ++
        "{%- set role = message['role'] -%}" ++
        "{%- endif -%}" ++
        "{{ '<|turn>' + role + '\\n' + (first_user_prefix if loop.first else \"\") }}" ++
        "{%- if message['content'] is string -%}" ++
        "{{ message['content'] | trim }}" ++
        "{%- elif message['content'] is iterable -%}" ++
        "{%- for item in message['content'] -%}" ++
        "{%- if item['type'] == 'text' -%}" ++
        "{{ item['text'] | trim }}" ++
        "{%- endif -%}" ++
        "{%- endfor -%}" ++
        "{%- endif -%}" ++
        "{{ '<turn|>\\n' }}" ++
        "{%- endfor -%}" ++
        "{%- if add_generation_prompt -%}" ++
        "{{ '<|turn>model\\n' }}" ++
        "{%- endif -%}";

    const result = try render(arena, template, &ctx);
    try std.testing.expectEqualStrings(
        "<bos><|turn>user\nWhat is the capital of France?<turn|>\n<|turn>model\n",
        result,
    );
}

test "gemma4 chat template with system message" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const messages = [_]ChatMessage{
        .{ .role = "system", .content = "You are a helpful assistant." },
        .{ .role = "user", .content = "Hello" },
    };
    var ctx = try chatTemplateContext(arena, &messages, .{
        .bos_token = "<bos>",
        .add_generation_prompt = true,
    });

    const template =
        "{{ bos_token }}" ++
        "{%- if messages[0]['role'] == 'system' -%}" ++
        "{%- if messages[0]['content'] is string -%}" ++
        "{%- set first_user_prefix = messages[0]['content'] + '\\n\\n' -%}" ++
        "{%- else -%}" ++
        "{%- set first_user_prefix = messages[0]['content'][0]['text'] + '\\n\\n' -%}" ++
        "{%- endif -%}" ++
        "{%- set loop_messages = messages[1:] -%}" ++
        "{%- else -%}" ++
        "{%- set first_user_prefix = \"\" -%}" ++
        "{%- set loop_messages = messages -%}" ++
        "{%- endif -%}" ++
        "{%- for message in loop_messages -%}" ++
        "{%- if (message['role'] == 'assistant') -%}" ++
        "{%- set role = \"model\" -%}" ++
        "{%- else -%}" ++
        "{%- set role = message['role'] -%}" ++
        "{%- endif -%}" ++
        "{{ '<|turn>' + role + '\\n' + (first_user_prefix if loop.first else \"\") }}" ++
        "{%- if message['content'] is string -%}" ++
        "{{ message['content'] | trim }}" ++
        "{%- elif message['content'] is iterable -%}" ++
        "{%- for item in message['content'] -%}" ++
        "{%- if item['type'] == 'text' -%}" ++
        "{{ item['text'] | trim }}" ++
        "{%- endif -%}" ++
        "{%- endfor -%}" ++
        "{%- endif -%}" ++
        "{{ '<turn|>\\n' }}" ++
        "{%- endfor -%}" ++
        "{%- if add_generation_prompt -%}" ++
        "{{ '<|turn>model\\n' }}" ++
        "{%- endif -%}";

    const result = try render(arena, template, &ctx);
    try std.testing.expectEqualStrings(
        "<bos><|turn>user\nYou are a helpful assistant.\n\nHello<turn|>\n<|turn>model\n",
        result,
    );
}

test "gemma4 GGUF template basic chat" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const messages = [_]ChatMessage{
        .{ .role = "user", .content = "What is the capital of France?" },
    };
    var ctx = try chatTemplateContext(arena, &messages, .{
        .bos_token = "<bos>",
        .add_generation_prompt = true,
    });

    // The actual GGUF template from Gemma 4 E2B
    const template = @embedFile("testdata/gemma4_gguf_template.txt");

    const result = try render(arena, template, &ctx);
    try std.testing.expectEqualStrings(
        "<bos><|turn>user\n" ++
            "What is the capital of France?<turn|>\n" ++
            "<|turn>model\n",
        result,
    );
}

test "gemma4 GGUF template with system message" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const messages = [_]ChatMessage{
        .{ .role = "system", .content = "You are a helpful assistant." },
        .{ .role = "user", .content = "Hello" },
    };
    var ctx = try chatTemplateContext(arena, &messages, .{
        .bos_token = "<bos>",
        .add_generation_prompt = true,
    });

    const template = @embedFile("testdata/gemma4_gguf_template.txt");

    const result = try render(arena, template, &ctx);
    try std.testing.expectEqualStrings(
        "<bos><|turn>system\n" ++
            "You are a helpful assistant.<turn|>\n" ++
            "<|turn>user\n" ++
            "Hello<turn|>\n" ++
            "<|turn>model\n",
        result,
    );
}

test "capture blocks preserve scope and whitespace without leaking captured output" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var ctx = ValueMap{};
    const result = try render(
        arena,
        "{%- set ns = namespace(seen=false) -%}" ++
            "before  {%- set captured -%}  A{% set nested %}B{% endset %}{{ nested }}" ++
            "{% set ns.seen = true %}  {%- endset -%}  after|{{ captured }}|{{ ns.seen }}",
        &ctx,
    );
    try std.testing.expectEqualStrings("beforeafter|AB|True", result);
}

test "canonical Gemma4 template preserves thinking modes and tool history" {
    var template = try Template.initHuggingFace(std.testing.allocator, @embedFile("testdata/gemma4_canonical_template.txt"));
    defer template.deinit();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const messages = [_]ChatMessage{
        .{ .role = "user", .content = "Find the code." },
        .{ .role = "assistant", .content = "", .content_is_null = true, .tool_calls = &.{.{
            .id = "call_1",
            .name = "lookup",
            .arguments = "{\"query\":\"code\",\"limit\":2}",
        }} },
        .{ .role = "tool", .content = "AZURE-731", .tool_call_id = "call_1" },
    };
    for ([_]bool{ false, true }) |thinking| {
        var ctx = try chatTemplateContext(arena, messages[0..1], .{ .bos_token = "<bos>", .enable_thinking = thinking });
        const prompt = try template.render(arena, &ctx);
        try std.testing.expect(std.mem.endsWith(u8, prompt, "<|turn>model\n"));
        try std.testing.expectEqual(thinking, std.mem.indexOf(u8, prompt, "<|think|>") != null);
        ctx = try chatTemplateContext(arena, &messages, .{ .bos_token = "<bos>", .enable_thinking = thinking });
        const history = try template.render(arena, &ctx);
        try std.testing.expect(std.mem.indexOf(u8, history, "<|tool_call>call:lookup{") != null);
        try std.testing.expect(std.mem.indexOf(u8, history, "limit:2") != null);
        try std.testing.expect(std.mem.indexOf(u8, history, "query:<|\"|>code<|\"|>") != null);
        try std.testing.expect(std.mem.indexOf(u8, history, "<|tool_response>response:lookup{value:<|\"|>AZURE-731<|\"|>}<tool_response|>") != null);
        try std.testing.expect(std.mem.endsWith(u8, history, if (thinking) "<|channel>thought\n" else "<tool_response|>"));
    }
}

test "range and mapping get honor defaults and reject invalid bounds" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var ctx = ValueMap{};
    const result = try render(
        arena,
        "{% set obj = namespace(present=none, value=7) %}" ++
            "{{ obj.get('value', 9) }}|{{ obj.get('absent', 9) }}|{{ obj.get('present', 9) is none }}|" ++
            "{% for i in range(4, -1, -2) %}{{ i }}{% endfor %}|{% for i in range(2) %}{{ i }}{% endfor %}|" ++
            "{{ 'one\\n' 'two' }}",
        &ctx,
    );
    try std.testing.expectEqualStrings("7|9|True|420|01|one\ntwo", result);
    try std.testing.expectError(error.InvalidArguments, render(arena, "{{ range(0, 3, 0) }}", &ctx));
    try std.testing.expectError(error.RangeTooLarge, render(arena, "{{ range(100001) }}", &ctx));
    try std.testing.expectError(error.TemplateException, render(arena, "{{ raise_exception('bad ' 'input') }}", &ctx));
}

test {
    _ = @import("lexer.zig");
    _ = @import("parser.zig");
    _ = @import("eval.zig");
}
