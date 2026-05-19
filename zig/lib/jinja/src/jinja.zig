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

    /// Parse a template string. The template owns the AST memory.
    pub fn init(backing_allocator: std.mem.Allocator, source: []const u8) !Template {
        var parse_arena = std.heap.ArenaAllocator.init(backing_allocator);
        errdefer parse_arena.deinit();

        const nodes = try Parser.parse(source, parse_arena.allocator());
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
            try m.put(arena, "content", Value.str(msg.content));
        }
        msg_values[i] = .{ .map = m };
    }

    var ctx = ValueMap{};
    try ctx.put(arena, "messages", .{ .list = msg_values });
    try ctx.put(arena, "add_generation_prompt", Value.bln(options.add_generation_prompt));
    try ctx.put(arena, "bos_token", Value.str(options.bos_token));
    try ctx.put(arena, "eos_token", Value.str(options.eos_token));
    try ctx.put(arena, "unk_token", Value.str(options.unk_token));
    try ctx.put(arena, "pad_token", Value.str(options.pad_token));

    return ctx;
}

pub const ChatMessage = struct {
    role: []const u8,
    content: []const u8,
    parts: ?[]const ChatContentPart = null,
};

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
};

// --- Tests ---

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

test {
    _ = @import("lexer.zig");
    _ = @import("parser.zig");
    _ = @import("eval.zig");
}
