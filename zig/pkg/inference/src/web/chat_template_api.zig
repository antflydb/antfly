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
const generation = @import("../pipelines/generation.zig");
const web_runtime = @import("runtime_state.zig");

pub fn renderSingleTurn(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    tok_handle: u32,
    template_source: []const u8,
    system_prompt: []const u8,
    user_prompt: []const u8,
    add_generation_prompt: bool,
) ![]u8 {
    const hf_tok = try runtime.getTokenizer(tok_handle);
    const special = hf_tok.special;

    var chat_template = try generation.ChatTemplate.init(
        allocator,
        template_source,
        specialTokenString(hf_tok, special.cls_id),
        specialTokenString(hf_tok, special.sep_id),
        specialTokenString(hf_tok, special.unk_id),
        specialTokenString(hf_tok, special.pad_id),
    );
    defer chat_template.deinit();

    const message_count: usize = if (system_prompt.len > 0) 2 else 1;
    const messages = try allocator.alloc(generation.Message, message_count);
    defer allocator.free(messages);

    var index: usize = 0;
    if (system_prompt.len > 0) {
        messages[index] = .{
            .role = "system",
            .content = system_prompt,
        };
        index += 1;
    }
    messages[index] = .{
        .role = "user",
        .content = user_prompt,
    };

    return chat_template.apply(allocator, messages, add_generation_prompt);
}

fn specialTokenString(hf_tok: *const @import("termite_hf_tokenizer").HfTokenizer, token_id: i32) []const u8 {
    if (token_id < 0) return "";
    return hf_tok.id_to_token.get(token_id) orelse "";
}
