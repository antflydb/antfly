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

//! CPU-only loaded-tokenizer/template materialization for sealed quality cases.
//! This module has no model session or backend dependency and cannot issue a
//! prefill, decode, fitting, selection, or publication operation.
const std = @import("std");
const generation = @import("pipelines/generation.zig");
const tokenizer_mod = @import("inference_tokenizer");
const context_policy = @import("runtime/context_policy.zig");
const teacher_forced = @import("backends/context_teacher_forced.zig");

pub const max_rendered_bytes: usize = 32 * 1024 * 1024;

pub const Result = struct {
    allocator: std.mem.Allocator,
    rendered_prompt: []u8,
    rendered_labels: []u32,
    prompt_tokens: []i64,
    prompt_token_offsets: [][2]u32,
    prompt_token_labels: []u32,
    combined_tokens: []i64,
    reference_tokens: []i64,
    reference_sequence_sha256: [32]u8,

    pub fn deinit(self: *Result) void {
        self.allocator.free(self.reference_tokens);
        self.allocator.free(self.combined_tokens);
        self.allocator.free(self.prompt_token_labels);
        self.allocator.free(self.prompt_token_offsets);
        self.allocator.free(self.prompt_tokens);
        self.allocator.free(self.rendered_labels);
        self.allocator.free(self.rendered_prompt);
        self.* = undefined;
    }
};

fn activeTokenCount(encoded: tokenizer_mod.EncodeResult) !usize {
    if (encoded.ids.len != encoded.attention_mask.len) return error.InvalidMaterializedPrompt;
    var count: usize = 0;
    while (count < encoded.attention_mask.len and encoded.attention_mask[count] != 0) : (count += 1) {
        if (encoded.ids[count] < 0) return error.InvalidMaterializedPrompt;
    }
    if (count == 0) return error.InvalidMaterializedPrompt;
    for (encoded.attention_mask[count..]) |mask| if (mask != 0) return error.InvalidMaterializedPrompt;
    return count;
}

fn copyTokens(allocator: std.mem.Allocator, ids: []const i32) ![]i64 {
    const result = try allocator.alloc(i64, ids.len);
    errdefer allocator.free(result);
    for (ids, result) |token, *out| {
        if (token < 0) return error.InvalidMaterializedPrompt;
        out.* = token;
    }
    return result;
}

pub fn splitStableContinuation(
    allocator: std.mem.Allocator,
    prompt: []const i32,
    combined: []const i32,
    reserved_output_tokens: u32,
) ![]i64 {
    if (prompt.len == 0 or combined.len <= prompt.len or reserved_output_tokens == 0 or
        !std.mem.eql(i32, prompt, combined[0..prompt.len]))
        return error.UnstableQualityReferenceBoundary;
    const count = combined.len - prompt.len;
    if (count < teacher_forced.min_reference_tokens or
        count > teacher_forced.max_reference_tokens or count > reserved_output_tokens)
        return error.InvalidQualityReferenceTokens;
    return copyTokens(allocator, combined[prompt.len..]);
}

fn validateOffsets(offsets: []const [2]u32, rendered_bytes: usize) !void {
    var previous_start: u32 = 0;
    var previous_end: u32 = 0;
    for (offsets) |offset| {
        const start, const end = offset;
        if (start == 0 and end == 0) continue;
        if (start > end or end > rendered_bytes or start < previous_start or end < previous_end)
            return error.InvalidMaterializedPromptOffsets;
        previous_start = start;
        previous_end = end;
    }
}

pub fn materialize(
    allocator: std.mem.Allocator,
    template: *const generation.ChatTemplate,
    tokenizer: tokenizer_mod.Tokenizer,
    messages: []const generation.Message,
    options: generation.ChatTemplate.ApplyOptions,
    policy: context_policy.Policy,
    prompt_token_limit: usize,
    reserved_output_tokens: u32,
    add_bos_token: bool,
    bos_token: []const u8,
    reference_utf8: []const u8,
) !Result {
    if (messages.len == 0 or prompt_token_limit == 0 or reserved_output_tokens == 0 or
        reference_utf8.len == 0)
        return error.InvalidQualityMaterialization;
    var prepared = try generation.prepareCompactionPayload(
        allocator,
        template,
        tokenizer,
        messages,
        options,
        policy,
        prompt_token_limit,
        add_bos_token,
    );
    defer prepared.deinit();
    if (prepared.rendered.text.len == 0 or prepared.rendered.text.len > max_rendered_bytes or
        prepared.rendered.labels.len != prepared.rendered.text.len)
        return error.InvalidMaterializedPrompt;
    const prompt_count = try activeTokenCount(prepared.encoded);
    const all_offsets = prepared.encoded.offsets orelse return error.InvalidMaterializedPromptOffsets;
    if (all_offsets.len < prompt_count) return error.InvalidMaterializedPromptOffsets;
    try validateOffsets(all_offsets[0..prompt_count], prepared.rendered.text.len);

    const combined_text_len = std.math.add(usize, prepared.rendered.text.len, reference_utf8.len) catch
        return error.InvalidQualityMaterialization;
    if (combined_text_len > max_rendered_bytes) return error.InvalidQualityMaterialization;
    const combined_text = try allocator.alloc(u8, combined_text_len);
    defer allocator.free(combined_text);
    @memcpy(combined_text[0..prepared.rendered.text.len], prepared.rendered.text);
    @memcpy(combined_text[prepared.rendered.text.len..], reference_utf8);
    const combined_limit = std.math.add(usize, prompt_count, reserved_output_tokens) catch
        return error.InvalidQualityMaterialization;
    var combined = try generation.encodeNativeGenerationPrompt(
        tokenizer,
        allocator,
        combined_text,
        combined_limit,
        add_bos_token,
        bos_token,
    );
    defer combined.deinit();
    const combined_count = try activeTokenCount(combined);
    const reference_tokens = try splitStableContinuation(
        allocator,
        prepared.encoded.ids[0..prompt_count],
        combined.ids[0..combined_count],
        reserved_output_tokens,
    );
    errdefer allocator.free(reference_tokens);
    const signed_reference = try allocator.alloc(i32, reference_tokens.len);
    defer allocator.free(signed_reference);
    for (reference_tokens, signed_reference) |token, *out|
        out.* = std.math.cast(i32, token) orelse return error.InvalidQualityReferenceTokens;
    const decoded = try tokenizer.decode(allocator, signed_reference);
    defer allocator.free(decoded);
    if (!std.mem.eql(u8, decoded, reference_utf8)) return error.QualityReferenceTokenizerMismatch;

    const rendered_prompt = try allocator.dupe(u8, prepared.rendered.text);
    errdefer allocator.free(rendered_prompt);
    const rendered_labels = try allocator.dupe(u32, prepared.rendered.labels);
    errdefer allocator.free(rendered_labels);
    const prompt_tokens = try copyTokens(allocator, prepared.encoded.ids[0..prompt_count]);
    errdefer allocator.free(prompt_tokens);
    const prompt_offsets = try allocator.dupe([2]u32, all_offsets[0..prompt_count]);
    errdefer allocator.free(prompt_offsets);
    if (prepared.eligible.labels.len != prompt_count) return error.InvalidMaterializedPrompt;
    const prompt_labels = try allocator.dupe(u32, prepared.eligible.labels);
    errdefer allocator.free(prompt_labels);
    const combined_tokens = try copyTokens(allocator, combined.ids[0..combined_count]);
    errdefer allocator.free(combined_tokens);
    return .{
        .allocator = allocator,
        .rendered_prompt = rendered_prompt,
        .rendered_labels = rendered_labels,
        .prompt_tokens = prompt_tokens,
        .prompt_token_offsets = prompt_offsets,
        .prompt_token_labels = prompt_labels,
        .combined_tokens = combined_tokens,
        .reference_tokens = reference_tokens,
        .reference_sequence_sha256 = try teacher_forced.referenceSequenceDigest(reference_tokens),
    };
}

test "context quality materializer rejects boundary merges and undersized references" {
    const allocator = std.testing.allocator;
    const prompt = [_]i32{ 1, 2, 3 };
    const combined = [_]i32{ 1, 2, 3, 4, 5 };
    const suffix = try splitStableContinuation(allocator, &prompt, &combined, 2);
    defer allocator.free(suffix);
    try std.testing.expectEqualSlices(i64, &.{ 4, 5 }, suffix);
    try std.testing.expectError(error.UnstableQualityReferenceBoundary, splitStableContinuation(allocator, &prompt, &.{ 1, 2, 9, 4, 5 }, 2));
    try std.testing.expectError(error.InvalidQualityReferenceTokens, splitStableContinuation(allocator, &prompt, &.{ 1, 2, 3, 4 }, 2));
    try std.testing.expectError(error.InvalidQualityReferenceTokens, splitStableContinuation(allocator, &prompt, &combined, 1));
}

test "context quality materializer accepts overlapping monotonic tokenizer offsets" {
    try validateOffsets(&.{ .{ 0, 0 }, .{ 1, 2 }, .{ 1, 2 }, .{ 2, 4 } }, 4);
    try std.testing.expectError(error.InvalidMaterializedPromptOffsets, validateOffsets(&.{ .{ 1, 3 }, .{ 0, 2 } }, 4));
    try std.testing.expectError(error.InvalidMaterializedPromptOffsets, validateOffsets(&.{.{ 1, 5 }}, 4));
}
