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

//! Text generation over the resident Metal Inkling decoder.
const std = @import("std");
const decoder = @import("inkling2_decoder_metal.zig");
const chat = @import("inkling2_chat.zig");
const generation = @import("../pipelines/generation.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Tokenizer = @import("inference_tokenizer").Tokenizer;

pub const TextPipeline = struct {
    allocator: std.mem.Allocator,
    tokenizer: Tokenizer,
    control: Control,
    prompt: chat.Prompt,
    options: chat.ChatOptions,

    pub const Callbacks = struct {
        text_ctx: ?*anyopaque = null,
        text: ?generation.TokenCallback = null,
        reasoning_ctx: ?*anyopaque = null,
        reasoning: ?generation.TokenCallback = null,
    };

    pub fn init(a: std.mem.Allocator, tokenizer: Tokenizer, control: Control, messages: []const generation.Message, config: generation.GenerationConfig, input_limit: ?usize, options: chat.ChatOptions) !TextPipeline {
        try control.check();
        const prompt = try chat.Prompt.init(a, tokenizer, messages, options, config.enable_thinking);
        errdefer prompt.deinit(a);
        if (prompt.image != null) return error.UnsupportedInkling2Request;
        if (input_limit) |limit| if (prompt.count() > limit) return error.InferenceInputTokensExceeded;
        return .{ .allocator = a, .tokenizer = tokenizer, .control = control, .prompt = prompt, .options = options };
    }

    pub fn deinit(self: *TextPipeline) void {
        self.prompt.deinit(self.allocator);
    }

    pub fn requestPlan(self: *const TextPipeline, model: *const decoder.Decoder, config: generation.GenerationConfig) !decoder.RequestPlan {
        if (config.max_tokens <= 0 or self.prompt.after_image.len == 0 or self.prompt.image != null or
            config.grammar != null or config.cache_dtype != null or config.kv_cache_mode != .exact_hybrid or
            config.cache_compaction_ratio != null or config.prompt_cache_enabled or config.draft_model != null or
            config.token_observer != null or (config.speculation_requested and config.speculation_policy == .force))
            return error.UnsupportedInkling2Request;
        const tokens = try std.math.add(usize, self.prompt.count(), @as(usize, @intCast(config.max_tokens)) - 1);
        return decoder.RequestPlan.init(model, std.math.cast(u32, tokens) orelse return error.InvalidInkling2Context);
    }

    /// The caller owns the runtime execution gate and memory admission for
    /// the complete request, including prompt prefill and streaming callbacks.
    pub fn generate(self: *const TextPipeline, model: *const decoder.Decoder, eos: u32, config: generation.GenerationConfig, callbacks: Callbacks) !generation.GenerationResult {
        try self.control.check();
        const plan = try self.requestPlan(model, config);
        var request: decoder.Request = undefined;
        try request.initWithPlan(self.allocator, model, plan);
        defer request.deinit();

        var history: std.ArrayList(i64) = .empty;
        defer history.deinit(self.allocator);
        const after = self.prompt.after_image;
        for ([_][]const i32{ self.prompt.before_image, after[0 .. after.len - 1] }) |prefix| {
            for (prefix) |id| {
                try self.control.check();
                const token = std.math.cast(u32, id) orelse return error.InvalidInkling2Token;
                try request.append(token);
                try history.append(self.allocator, id);
            }
        }

        const turn_end = try chat.turnEndToken(self.allocator, self.tokenizer);
        const channels: ?chat.ChannelTokens = if (self.prompt.initial_channel != null)
            try .init(self.allocator, self.tokenizer, turn_end)
        else
            null;
        var output: chat.Output = .{
            .allocator = self.allocator,
            .tokenizer = self.tokenizer,
            .ctx = callbacks.text_ctx,
            .callback = callbacks.text,
            .reasoning_ctx = callbacks.reasoning_ctx,
            .reasoning_callback = callbacks.reasoning,
            .channels = channels,
            .channel = self.prompt.initial_channel orelse .text,
            .tools = self.options.tools,
        };
        defer output.deinit();
        const limit: usize = @intCast(config.max_tokens);
        var token = std.math.cast(u32, after[after.len - 1]) orelse return error.InvalidInkling2Token;
        while (output.tokens.items.len < limit) {
            try self.control.check();
            const logits = try request.step(token);
            try history.append(self.allocator, token);
            token = @intCast(generation.sampleTokenFromLogits(self.allocator, logits, config, history.items));
            try self.control.check();
            if (!config.ignore_eos and (token == eos or (channels == null and token == turn_end))) break;
            try output.append(token);
        }
        return output.finalize(self.options, self.prompt.count(), limit);
    }
};
