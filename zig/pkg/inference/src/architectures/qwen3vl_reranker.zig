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

//! Backend-independent Qwen3-VL-Reranker scoring and sequence contracts.
//!
//! The official checkpoint is a conditional-generation model whose pointwise
//! relevance score is `sigmoid((W_yes - W_no) dot final_hidden)`. It is not a
//! ColBERT/late-interaction reranker and must never be routed through that API.

const std = @import("std");

pub const yes_token_id: u32 = 9_693;
pub const no_token_id: u32 = 2_152;
/// The upstream Qwen helper deliberately defaults below the checkpoint's 32K
/// context window so reranking has a finite, operationally useful admission
/// boundary. Operators can raise this only through an explicit pipeline
/// configuration after qualifying the resulting residency envelope.
pub const default_max_length: usize = 8_192;
pub const protected_assistant_suffix_tokens: usize = 5;
pub const default_max_prompt_bytes: usize = 16 * 1024 * 1024;

pub const system_prompt =
    "Judge whether the Document meets the requirements based on the Query and the Instruct provided. " ++
    "Note that the answer can only be \"yes\" or \"no\".";
pub const default_instruction =
    "Given a search query, retrieve relevant candidates that answer the query.";
pub const instruct_prefix = "<Instruct>: ";
pub const query_prefix = "<Query>:";
pub const document_prefix = "\n<Document>:";
pub const image_marker = "<|vision_start|><|image_pad|><|vision_end|>";

const im_start = "<|im_start|>";
const im_end = "<|im_end|>";

/// Render the text-only form of the checkpoint's pinned chat template. Keeping
/// this small template explicit avoids making reranker correctness depend on
/// the general-purpose Jinja renderer and makes oracle drift testable.
fn renderPromptAlloc(
    allocator: std.mem.Allocator,
    instruction: []const u8,
    query: []const u8,
    document: []const u8,
    max_prompt_bytes: usize,
) ![]u8 {
    if (max_prompt_bytes == 0) return error.InvalidRerankerPromptLimit;
    const effective_instruction = if (instruction.len == 0) default_instruction else instruction;
    const fixed_bytes = std.math.add(usize, system_prompt.len, instruct_prefix.len) catch
        return error.RerankerPromptTooLarge;
    const variable_bytes = std.math.add(usize, effective_instruction.len, query.len) catch
        return error.RerankerPromptTooLarge;
    const with_document = std.math.add(usize, variable_bytes, document.len) catch
        return error.RerankerPromptTooLarge;
    // Chat markers, role labels, separators, and a conservative terminator
    // allowance. The exact allocation below remains authoritative.
    const estimated = std.math.add(usize, fixed_bytes, with_document) catch
        return error.RerankerPromptTooLarge;
    if (estimated > max_prompt_bytes or max_prompt_bytes - estimated < 128) {
        return error.RerankerPromptTooLarge;
    }

    const rendered = try std.fmt.allocPrint(
        allocator,
        "{s}system\n{s}{s}\n{s}user\n{s}{s}{s}{s}{s}{s}{s}\n{s}assistant\n",
        .{
            im_start,
            system_prompt,
            im_end,
            im_start,
            instruct_prefix,
            effective_instruction,
            query_prefix,
            query,
            document_prefix,
            document,
            im_end,
            im_start,
        },
    );
    errdefer allocator.free(rendered);
    if (rendered.len > max_prompt_bytes) return error.RerankerPromptTooLarge;
    return rendered;
}

pub fn renderTextPromptAlloc(
    allocator: std.mem.Allocator,
    instruction: []const u8,
    query: []const u8,
    document: []const u8,
    max_prompt_bytes: usize,
) ![]u8 {
    return renderPromptAlloc(allocator, instruction, query, document, max_prompt_bytes);
}

/// Render a document whose text and image markers are already in caller
/// order. Requiring the expected marker count makes accidental marker loss,
/// duplication, or user-supplied marker injection fail closed before vision
/// preprocessing or decoder execution.
pub fn renderMultimodalPromptAlloc(
    allocator: std.mem.Allocator,
    instruction: []const u8,
    query: []const u8,
    document_content: []const u8,
    expected_images: usize,
    max_prompt_bytes: usize,
) ![]u8 {
    if (expected_images == 0) return error.InvalidRerankerImageCount;
    var marker_count: usize = 0;
    var offset: usize = 0;
    while (std.mem.indexOfPos(u8, document_content, offset, image_marker)) |index| {
        marker_count = std.math.add(usize, marker_count, 1) catch
            return error.InvalidRerankerImageCount;
        offset = index + image_marker.len;
    }
    if (marker_count != expected_images) return error.ImagePlaceholderCountMismatch;
    return renderPromptAlloc(allocator, instruction, query, document_content, max_prompt_bytes);
}

pub const TruncationPolicy = enum {
    /// Reserve room for the final assistant suffix and guarantee that the
    /// result never exceeds `max_length`.
    strict_bounded,
    /// Reproduce the current upstream Python helper, which truncates the
    /// prefix to `max_length` before restoring five suffix tokens and can
    /// therefore return as many as `max_length + 5` tokens.
    upstream_compat,
};

pub fn stableSigmoid(value: f32) !f32 {
    if (!std.math.isFinite(value)) return error.NonFiniteRerankerScore;
    if (value >= 0) return 1.0 / (1.0 + @exp(-value));
    const exp_value = @exp(value);
    return exp_value / (1.0 + exp_value);
}

pub fn scoreYesNoLogits(yes_logit: f32, no_logit: f32) !f32 {
    if (!std.math.isFinite(yes_logit) or !std.math.isFinite(no_logit)) {
        return error.NonFiniteRerankerScore;
    }
    return stableSigmoid(yes_logit - no_logit);
}

pub fn scoreFinalHidden(final_hidden: []const f32, yes_weight: []const f32, no_weight: []const f32) !f32 {
    if (final_hidden.len == 0 or final_hidden.len != yes_weight.len or final_hidden.len != no_weight.len) {
        return error.InvalidRerankerScoreShape;
    }
    var difference_dot: f32 = 0;
    for (final_hidden, yes_weight, no_weight) |hidden, yes, no| {
        if (!std.math.isFinite(hidden) or !std.math.isFinite(yes) or !std.math.isFinite(no)) {
            return error.NonFiniteRerankerScore;
        }
        difference_dot += hidden * (yes - no);
    }
    return stableSigmoid(difference_dot);
}

pub fn scoreFinalHiddenBatch(
    final_hidden_rows: []const f32,
    row_count: usize,
    hidden_size: usize,
    yes_weight: []const f32,
    no_weight: []const f32,
    output: []f32,
) !void {
    const expected_values = std.math.mul(usize, row_count, hidden_size) catch return error.InvalidRerankerScoreShape;
    if (row_count == 0 or hidden_size == 0 or output.len != row_count or
        yes_weight.len != hidden_size or no_weight.len != hidden_size or
        final_hidden_rows.len != expected_values)
    {
        return error.InvalidRerankerScoreShape;
    }
    for (0..row_count) |row| {
        const offset = row * hidden_size;
        output[row] = try scoreFinalHidden(
            final_hidden_rows[offset..][0..hidden_size],
            yes_weight,
            no_weight,
        );
    }
}

fn isSpecialToken(token: u32, special_tokens: []const u32) bool {
    for (special_tokens) |special| {
        if (token == special) return true;
    }
    return false;
}

fn appendTruncatedPrefix(
    allocator: std.mem.Allocator,
    output: *std.ArrayListUnmanaged(u32),
    tokens: []const u32,
    max_length: usize,
    special_tokens: []const u32,
) !void {
    if (tokens.len <= max_length) {
        try output.appendSlice(allocator, tokens);
        return;
    }
    var special_count: usize = 0;
    for (tokens) |token| {
        if (isSpecialToken(token, special_tokens)) special_count += 1;
    }
    if (special_count > max_length) return error.SpecialTokenBudgetExceeded;
    const ordinary_budget = max_length - special_count;
    var ordinary_kept: usize = 0;
    for (tokens) |token| {
        if (isSpecialToken(token, special_tokens)) {
            try output.append(allocator, token);
        } else if (ordinary_kept < ordinary_budget) {
            try output.append(allocator, token);
            ordinary_kept += 1;
        }
    }
}

/// Truncate the prompt while preserving every multimodal/special marker and
/// the final assistant-generation suffix. The strict policy is the production
/// default; upstream compatibility exists solely for oracle comparisons.
pub fn truncateForScoring(
    allocator: std.mem.Allocator,
    input_ids: []const u32,
    max_length: usize,
    special_tokens: []const u32,
    policy: TruncationPolicy,
) ![]u32 {
    if (max_length == 0) return error.InvalidRerankerMaxLength;
    if (input_ids.len <= max_length) return allocator.dupe(u32, input_ids);
    if (input_ids.len < protected_assistant_suffix_tokens) return error.InvalidRerankerSequence;

    const suffix_start = input_ids.len - protected_assistant_suffix_tokens;
    const prefix_budget = switch (policy) {
        .strict_bounded => if (max_length < protected_assistant_suffix_tokens)
            return error.InvalidRerankerMaxLength
        else
            max_length - protected_assistant_suffix_tokens,
        .upstream_compat => max_length,
    };
    var output = std.ArrayListUnmanaged(u32).empty;
    errdefer output.deinit(allocator);
    try appendTruncatedPrefix(
        allocator,
        &output,
        input_ids[0..suffix_start],
        prefix_budget,
        special_tokens,
    );
    try output.appendSlice(allocator, input_ids[suffix_start..]);
    return output.toOwnedSlice(allocator);
}

test "Qwen3-VL reranker score is the stable yes-minus-no pointwise probability" {
    const hidden = [_]f32{ 0.5, -2.0, 1.5 };
    const yes = [_]f32{ 2.0, 1.0, -0.5 };
    const no = [_]f32{ 1.0, -1.0, 0.25 };
    const logit = 0.5 * (2.0 - 1.0) - 2.0 * (1.0 - -1.0) + 1.5 * (-0.5 - 0.25);
    try std.testing.expectApproxEqAbs(try stableSigmoid(logit), try scoreFinalHidden(&hidden, &yes, &no), 1e-6);

    var scores: [2]f32 = undefined;
    try scoreFinalHiddenBatch(&.{ 0.5, -2.0, 1.5, -0.5, 2.0, -1.5 }, 2, 3, &yes, &no, &scores);
    try std.testing.expect(scores[0] >= 0 and scores[0] <= 1);
    try std.testing.expect(scores[1] >= 0 and scores[1] <= 1);
    try std.testing.expectError(error.NonFiniteRerankerScore, stableSigmoid(std.math.inf(f32)));
    try std.testing.expectApproxEqAbs(
        try stableSigmoid(3.0),
        try scoreYesNoLogits(4.0, 1.0),
        1e-6,
    );
}

test "Qwen3-VL reranker text prompt exactly matches the pinned chat template" {
    const prompt = try renderTextPromptAlloc(
        std.testing.allocator,
        "Retrieve relevant text.",
        "red planet",
        "Mars is red.",
        4096,
    );
    defer std.testing.allocator.free(prompt);
    try std.testing.expectEqualStrings(
        "<|im_start|>system\n" ++ system_prompt ++ "<|im_end|>\n" ++
            "<|im_start|>user\n<Instruct>: Retrieve relevant text.<Query>:red planet\n<Document>:Mars is red.<|im_end|>\n" ++
            "<|im_start|>assistant\n",
        prompt,
    );
    try std.testing.expectError(
        error.RerankerPromptTooLarge,
        renderTextPromptAlloc(std.testing.allocator, "", "query", "document", 32),
    );
}

test "Qwen3-VL reranker multimodal prompt preserves interleaved image order" {
    const document = "before" ++ image_marker ++ "middle" ++ image_marker ++ "after";
    const prompt = try renderMultimodalPromptAlloc(
        std.testing.allocator,
        "Retrieve relevant content.",
        "invoice",
        document,
        2,
        4096,
    );
    defer std.testing.allocator.free(prompt);
    try std.testing.expect(std.mem.indexOf(u8, prompt, document) != null);
    try std.testing.expectError(
        error.ImagePlaceholderCountMismatch,
        renderMultimodalPromptAlloc(
            std.testing.allocator,
            "",
            "invoice",
            document,
            1,
            4096,
        ),
    );
    try std.testing.expectError(
        error.InvalidRerankerImageCount,
        renderMultimodalPromptAlloc(
            std.testing.allocator,
            "",
            "invoice",
            "text only",
            0,
            4096,
        ),
    );
}

test "Qwen3-VL reranker strict truncation preserves markers suffix and hard bound" {
    const ids = [_]u32{ 10, 101, 11, 12, 102, 13, 14, 15, 201, 202, 203, 204, 205 };
    const specials = [_]u32{ 101, 102, 201, 202, 203, 204, 205 };
    const strict = try truncateForScoring(std.testing.allocator, &ids, 10, &specials, .strict_bounded);
    defer std.testing.allocator.free(strict);
    try std.testing.expectEqualSlices(u32, &.{ 10, 101, 11, 12, 102, 201, 202, 203, 204, 205 }, strict);
    try std.testing.expect(strict.len <= 10);

    const compatible = try truncateForScoring(std.testing.allocator, &ids, 10, &specials, .upstream_compat);
    defer std.testing.allocator.free(compatible);
    try std.testing.expectEqualSlices(u32, &.{ 10, 101, 11, 12, 102, 13, 14, 15, 201, 202, 203, 204, 205 }, compatible);
    try std.testing.expect(compatible.len <= 10 + protected_assistant_suffix_tokens);
}
