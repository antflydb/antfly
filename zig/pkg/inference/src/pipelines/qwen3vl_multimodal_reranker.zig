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

//! Native pointwise multimodal scoring for Qwen3-VL-Reranker.
//!
//! This is intentionally separate from ColQwen late interaction: Qwen3-VL
//! projects images into one causal prompt and scores the final assistant row
//! with its yes-minus-no classifier head.

const std = @import("std");
const tokenizer_mod = @import("inference_tokenizer");
const ops = @import("../ops/ops.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const gpt_config_mod = @import("../models/gpt.zig");
const qwen3vl_projector = @import("../architectures/qwen3vl_projector.zig");
const qwen3vl_reranker = @import("../architectures/qwen3vl_reranker.zig");

pub const Config = struct {
    max_length: usize = qwen3vl_reranker.default_max_length,
    max_prompt_bytes: usize = qwen3vl_reranker.default_max_prompt_bytes,
    instruction: []const u8 = qwen3vl_reranker.default_instruction,
    max_images: usize = 8,
    /// The official reranker helper admits four merged tokens, unlike the
    /// generation processor's 64-token training-grid floor.
    min_merged_tokens_per_image: usize = 4,
    max_merged_tokens_per_image: usize = 576,
};

pub const ScoreResult = struct {
    score: f32,
    raw_logit: f32,
    prompt_tokens: usize,
    visual_tokens: usize,
};

/// Offline-only exact evidence. Production callers leave the pipeline pointer
/// null, so ordinary requests do not copy prompts or backend plans to host.
pub const QualificationTrace = struct {
    allocator: std.mem.Allocator,
    rendered_prompt: []u8 = &.{},
    placeholder_token_ids: []i32 = &.{},
    expanded_token_ids: []i64 = &.{},
    mrope_positions: []u32 = &.{},
    visual_token_mask: []bool = &.{},
    raw_logit: f32 = 0,
    score: f32 = 0,

    pub fn init(allocator: std.mem.Allocator) QualificationTrace {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *QualificationTrace) void {
        self.allocator.free(self.rendered_prompt);
        self.allocator.free(self.placeholder_token_ids);
        self.allocator.free(self.expanded_token_ids);
        self.allocator.free(self.mrope_positions);
        self.allocator.free(self.visual_token_mask);
        self.* = undefined;
    }

    fn capture(
        self: *QualificationTrace,
        prompt: []const u8,
        placeholder_ids: []const i32,
        prepared: *const qwen3vl_projector.PreparedPrompt,
        raw_logit: f32,
        score: f32,
    ) !void {
        const rendered_prompt = try self.allocator.dupe(u8, prompt);
        errdefer self.allocator.free(rendered_prompt);
        const placeholder_token_ids = try self.allocator.dupe(i32, placeholder_ids);
        errdefer self.allocator.free(placeholder_token_ids);
        const expanded_token_ids = try self.allocator.dupe(i64, prepared.token_ids);
        errdefer self.allocator.free(expanded_token_ids);
        const mrope_positions = try self.allocator.dupe(u32, prepared.plan.mrope_positions);
        errdefer self.allocator.free(mrope_positions);
        const visual_token_mask = try self.allocator.dupe(bool, prepared.plan.visual_token_mask);
        errdefer self.allocator.free(visual_token_mask);

        self.allocator.free(self.rendered_prompt);
        self.allocator.free(self.placeholder_token_ids);
        self.allocator.free(self.expanded_token_ids);
        self.allocator.free(self.mrope_positions);
        self.allocator.free(self.visual_token_mask);
        self.rendered_prompt = rendered_prompt;
        self.placeholder_token_ids = placeholder_token_ids;
        self.expanded_token_ids = expanded_token_ids;
        self.mrope_positions = mrope_positions;
        self.visual_token_mask = visual_token_mask;
        self.raw_logit = raw_logit;
        self.score = score;
    }
};

pub const Pipeline = struct {
    allocator: std.mem.Allocator,
    cb: *const ops.ComputeBackend,
    tokenizer: tokenizer_mod.Tokenizer,
    gpt_config: gpt_config_mod.Config,
    projector_path: []const u8,
    config: Config,
    qualification_trace: ?*QualificationTrace = null,

    pub fn init(
        allocator: std.mem.Allocator,
        cb: *const ops.ComputeBackend,
        tokenizer: tokenizer_mod.Tokenizer,
        gpt_config: gpt_config_mod.Config,
        projector_path: []const u8,
        config: Config,
    ) !Pipeline {
        if (gpt_config.family != .qwen3_vl or gpt_config.image_token_index < 0 or
            projector_path.len == 0 or config.max_images == 0 or config.max_images > 8 or
            config.max_length < qwen3vl_reranker.protected_assistant_suffix_tokens or
            config.max_prompt_bytes == 0 or config.max_merged_tokens_per_image < 64 or
            config.max_merged_tokens_per_image > 1024 or
            config.min_merged_tokens_per_image < 4 or
            config.min_merged_tokens_per_image > config.max_merged_tokens_per_image)
        {
            return error.InvalidRerankerConfiguration;
        }
        return .{
            .allocator = allocator,
            .cb = cb,
            .tokenizer = tokenizer,
            .gpt_config = gpt_config,
            .projector_path = projector_path,
            .config = config,
        };
    }

    pub fn scoreDocument(
        self: *Pipeline,
        query: []const u8,
        document_content: []const u8,
        images: []const []const u8,
    ) !ScoreResult {
        if (images.len == 0 or images.len > self.config.max_images) {
            return error.ImageLimitExceeded;
        }

        const prompt = try qwen3vl_reranker.renderMultimodalPromptAlloc(
            self.allocator,
            self.config.instruction,
            query,
            document_content,
            images.len,
            self.config.max_prompt_bytes,
        );
        defer self.allocator.free(prompt);

        const raw_ids = try self.tokenizer.encode(self.allocator, prompt);
        defer self.allocator.free(raw_ids);
        if (raw_ids.len == 0) return error.InvalidRerankerSequence;
        try validateTokenIdsAndPlaceholders(
            raw_ids,
            self.tokenizer.vocabSize(),
            self.gpt_config.image_token_index,
            images.len,
        );

        // Bound smart resize using the final expanded context, then use the
        // actual projected token counts to derive the exact text budget.
        const per_image_context_budget = self.config.max_length / images.len;
        if (per_image_context_budget < self.config.min_merged_tokens_per_image) {
            return error.InputTokenLimitExceeded;
        }
        const per_image_limit = @min(
            self.config.max_merged_tokens_per_image,
            per_image_context_budget,
        );
        var projected = try qwen3vl_projector.encodeProjectedImages(
            self.cb,
            self.allocator,
            self.projector_path,
            images,
            .{
                .max_images = self.config.max_images,
                .min_merged_tokens = self.config.min_merged_tokens_per_image,
                .max_merged_tokens = per_image_limit,
            },
        );
        defer projected.deinit();

        const visual_tokens = try sumVisualTokens(projected.tokens_per_image);
        const unexpanded_budget = try placeholderSequenceBudget(
            self.config.max_length,
            visual_tokens,
            images.len,
        );

        const unsigned_ids = try self.allocator.alloc(u32, raw_ids.len);
        defer self.allocator.free(unsigned_ids);
        for (raw_ids, unsigned_ids) |id, *out| out.* = @intCast(id);
        const special_ids = try self.tokenizer.allSpecialTokenIds(self.allocator);
        defer self.allocator.free(special_ids);
        const bounded_ids = try qwen3vl_reranker.truncateForScoring(
            self.allocator,
            unsigned_ids,
            unexpanded_budget,
            special_ids,
            .strict_bounded,
        );
        defer self.allocator.free(bounded_ids);

        const placeholder_ids = try self.allocator.alloc(i32, bounded_ids.len);
        defer self.allocator.free(placeholder_ids);
        for (bounded_ids, placeholder_ids) |id, *out| out.* = @intCast(id);
        try validateTokenIdsAndPlaceholders(
            placeholder_ids,
            self.tokenizer.vocabSize(),
            self.gpt_config.image_token_index,
            images.len,
        );

        var prepared = try qwen3vl_projector.prepareExpandedPromptEmbeddings(
            self.cb,
            self.allocator,
            self.gpt_config,
            placeholder_ids,
            projected,
            self.config.max_length,
        );
        defer prepared.deinit(self.cb);
        const prompt_tokens = prepared.token_ids.len;
        if (prompt_tokens == 0 or prepared.plan.tokenCount() != prompt_tokens) {
            return error.InvalidPreparedPrompt;
        }
        const input_embeddings = prepared.input_embeddings orelse
            return error.InvalidPreparedPrompt;
        prepared.input_embeddings = null;
        const raw_logit = try gpt_arch.qwen3VlRerankerLogitFromMultimodalEmbeddings(
            self.cb,
            self.allocator,
            self.gpt_config,
            input_embeddings,
            prompt_tokens,
            prepared.plan.mrope_positions,
            prepared.plan.visual_token_mask,
            prepared.deepstack_embeddings,
            prepared.deepstack_layer_count,
        );
        const score = try qwen3vl_reranker.stableSigmoid(raw_logit);
        if (self.qualification_trace) |trace| {
            try trace.capture(prompt, placeholder_ids, &prepared, raw_logit, score);
        }
        return .{
            .score = score,
            .raw_logit = raw_logit,
            .prompt_tokens = prompt_tokens,
            .visual_tokens = visual_tokens,
        };
    }
};

fn validateTokenIdsAndPlaceholders(
    ids: []const i32,
    vocab_size: usize,
    image_token_id: i32,
    expected_images: usize,
) !void {
    if (ids.len == 0 or vocab_size == 0 or image_token_id < 0 or expected_images == 0) {
        return error.InvalidRerankerSequence;
    }
    var placeholders: usize = 0;
    for (ids) |id| {
        if (id < 0 or @as(usize, @intCast(id)) >= vocab_size) {
            return error.InvalidRerankerTokenId;
        }
        if (id == image_token_id) placeholders += 1;
    }
    if (placeholders != expected_images) return error.ImagePlaceholderCountMismatch;
}

fn sumVisualTokens(counts: []const usize) !usize {
    if (counts.len == 0) return error.ImagePlaceholderCountMismatch;
    var total: usize = 0;
    for (counts) |count| {
        if (count == 0) return error.ImageProjectionSizeMismatch;
        total = std.math.add(usize, total, count) catch return error.RequestTooLarge;
    }
    return total;
}

/// Before expansion, one placeholder occupies one token. Reserve the extra
/// `(visual_tokens - placeholders)` rows while retaining the strict assistant
/// suffix and every special marker through the shared truncation policy.
fn placeholderSequenceBudget(
    max_length: usize,
    visual_tokens: usize,
    placeholders: usize,
) !usize {
    if (placeholders == 0 or visual_tokens < placeholders) {
        return error.ImageProjectionSizeMismatch;
    }
    const expansion = visual_tokens - placeholders;
    if (expansion >= max_length) return error.InputTokenLimitExceeded;
    const budget = max_length - expansion;
    const marker_tokens = std.math.mul(usize, placeholders, 3) catch
        return error.InputTokenLimitExceeded;
    const protected_tokens = std.math.add(
        usize,
        qwen3vl_reranker.protected_assistant_suffix_tokens,
        marker_tokens,
    ) catch return error.InputTokenLimitExceeded;
    if (budget < protected_tokens) {
        return error.InputTokenLimitExceeded;
    }
    return budget;
}

test "Qwen3-VL multimodal reranker reserves exact expanded token budget" {
    try std.testing.expectEqual(@as(usize, 4), (Config{}).min_merged_tokens_per_image);
    try std.testing.expectEqual(@as(usize, 705), try placeholderSequenceBudget(768, 64, 1));
    try std.testing.expectEqual(@as(usize, 642), try placeholderSequenceBudget(768, 128, 2));
    try std.testing.expectError(error.InputTokenLimitExceeded, placeholderSequenceBudget(64, 64, 1));
    try std.testing.expectError(error.ImageProjectionSizeMismatch, placeholderSequenceBudget(768, 1, 2));
}

test "Qwen3-VL multimodal reranker rejects placeholder drift and invalid ids" {
    try validateTokenIdsAndPlaceholders(&.{ 1, 7, 2, 7, 3 }, 10, 7, 2);
    try std.testing.expectError(
        error.ImagePlaceholderCountMismatch,
        validateTokenIdsAndPlaceholders(&.{ 1, 7, 2 }, 10, 7, 2),
    );
    try std.testing.expectError(
        error.InvalidRerankerTokenId,
        validateTokenIdsAndPlaceholders(&.{ 1, 10, 7 }, 10, 7, 1),
    );
}
