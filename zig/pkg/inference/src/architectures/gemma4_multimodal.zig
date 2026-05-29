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
const ops = @import("../ops/ops.zig");
const tokenizer_mod = @import("inference_tokenizer");
const gpt_mod = @import("../models/gpt.zig");
const gemma3_mm = @import("../pipelines/gemma3_multimodal.zig");
const gemma4_projector = @import("gemma4_projector.zig");

const ComputeBackend = ops.ComputeBackend;

const SpecialTokenIds = struct {
    boi: i32,
    image: i32,
    eoi: i32,
    boa: i32,
    audio: i32,
    eoa: i32,
};

pub fn expandPromptText(
    allocator: std.mem.Allocator,
    prompt: []const u8,
    tokens_per_image: []const usize,
    tokens_per_audio: []const usize,
) ![]u8 {
    const image_expanded = try expandMarker(
        allocator,
        prompt,
        "<|image|>",
        "<|image>",
        "<image|>",
        tokens_per_image,
        error.ImagePlaceholderCountMismatch,
    );
    defer allocator.free(image_expanded);

    return expandMarker(
        allocator,
        image_expanded,
        "<|audio|>",
        "<|audio>",
        "<audio|>",
        tokens_per_audio,
        error.AudioPlaceholderCountMismatch,
    );
}

fn expandMarker(
    allocator: std.mem.Allocator,
    prompt: []const u8,
    marker: []const u8,
    begin_marker: []const u8,
    end_marker: []const u8,
    token_counts: []const usize,
    mismatch_error: anyerror,
) ![]u8 {
    if (token_counts.len == 0) return try allocator.dupe(u8, prompt);

    var count: usize = 0;
    var cursor: usize = 0;
    while (std.mem.indexOfPos(u8, prompt, cursor, marker)) |idx| {
        count += 1;
        cursor = idx + marker.len;
    }
    if (count != token_counts.len) return mismatch_error;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    cursor = 0;
    var replacement_idx: usize = 0;
    while (std.mem.indexOfPos(u8, prompt, cursor, marker)) |idx| {
        try out.appendSlice(allocator, prompt[cursor..idx]);
        try out.appendSlice(allocator, begin_marker);
        for (0..token_counts[replacement_idx]) |_| {
            try out.appendSlice(allocator, marker);
        }
        try out.appendSlice(allocator, end_marker);
        replacement_idx += 1;
        cursor = idx + marker.len;
    }
    try out.appendSlice(allocator, prompt[cursor..]);
    return try out.toOwnedSlice(allocator);
}

pub fn prepareExpandedPromptEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    config: gpt_mod.Config,
    expanded_token_ids: []const i32,
    projected_images: ?*const gemma4_projector.ProjectedImages,
    projected_audio: ?*const gemma4_projector.ProjectedAudio,
) !gemma3_mm.PreparedPrompt {
    if (projected_images == null and projected_audio == null) return error.NoMultimodalInputs;
    if (projected_images) |projected| {
        if (projected.tokens_per_image.len == 0) return error.NoImages;
        if (projected.hidden_size != config.hidden_size) return error.InvalidTensorShape;
    }
    if (projected_audio) |projected| {
        if (projected.tokens_per_audio.len == 0) return error.NoAudio;
        if (projected.hidden_size != config.hidden_size) return error.InvalidTensorShape;
    }

    const special = try resolveSpecialTokenIds(allocator, tokenizer);
    var token_ids = try allocator.alloc(i64, expanded_token_ids.len);
    errdefer allocator.free(token_ids);
    const ple_token_ids: ?[]i64 = if (config.hasPle()) try allocator.alloc(i64, expanded_token_ids.len) else null;
    errdefer if (ple_token_ids) |ids| allocator.free(ids);
    const image_count = if (projected_images) |projected| projected.tokens_per_image.len else 0;
    const audio_count = if (projected_audio) |projected| projected.tokens_per_audio.len else 0;
    var image_offsets = try allocator.alloc(usize, image_count);
    defer allocator.free(image_offsets);
    var audio_offsets = try allocator.alloc(usize, audio_count);
    defer allocator.free(audio_offsets);
    const pad_token_id = if (config.pad_token_id >= 0) config.pad_token_id else tokenizer.specialTokens().pad_id;

    var image_idx: usize = 0;
    var audio_idx: usize = 0;
    var run_start: ?usize = null;
    var run_kind: enum { image, audio } = .image;
    var image_soft_token_count: usize = 0;
    var audio_soft_token_count: usize = 0;
    for (expanded_token_ids, 0..) |token_id, idx| {
        token_ids[idx] = token_id;
        if (ple_token_ids) |ids| ids[idx] = token_id;
        if (token_id == special.image or token_id == special.audio) {
            if (ple_token_ids) |ids| ids[idx] = pad_token_id;
            if (token_id == special.image) {
                image_soft_token_count += 1;
                if (run_start == null) {
                    run_start = idx;
                    run_kind = .image;
                } else if (run_kind != .image) return error.AudioPlaceholderCountMismatch;
            } else {
                audio_soft_token_count += 1;
                if (run_start == null) {
                    run_start = idx;
                    run_kind = .audio;
                } else if (run_kind != .audio) return error.AudioPlaceholderCountMismatch;
            }
            continue;
        }
        if (run_start) |start| {
            switch (run_kind) {
                .image => {
                    const projected = projected_images orelse return error.ImagePlaceholderCountMismatch;
                    if (image_idx >= projected.tokens_per_image.len) return error.ImagePlaceholderCountMismatch;
                    const expected = projected.tokens_per_image[image_idx];
                    if (idx - start != expected) return error.ImagePlaceholderCountMismatch;
                    if (start == 0 or expanded_token_ids[start - 1] != special.boi) return error.ImagePlaceholderCountMismatch;
                    if (token_id != special.eoi) return error.ImagePlaceholderCountMismatch;
                    image_offsets[image_idx] = start;
                    image_idx += 1;
                },
                .audio => {
                    const projected = projected_audio orelse return error.AudioPlaceholderCountMismatch;
                    if (audio_idx >= projected.tokens_per_audio.len) return error.AudioPlaceholderCountMismatch;
                    const expected = projected.tokens_per_audio[audio_idx];
                    if (idx - start != expected) return error.AudioPlaceholderCountMismatch;
                    if (start == 0 or expanded_token_ids[start - 1] != special.boa) return error.AudioPlaceholderCountMismatch;
                    if (token_id != special.eoa) return error.AudioPlaceholderCountMismatch;
                    audio_offsets[audio_idx] = start;
                    audio_idx += 1;
                },
            }
            run_start = null;
        }
    }
    if (run_start != null) return error.MultimodalPlaceholderCountMismatch;
    if (projected_images) |projected| {
        if (image_idx != projected.tokens_per_image.len) return error.ImagePlaceholderCountMismatch;
        var expected_soft_tokens: usize = 0;
        for (projected.tokens_per_image) |count| expected_soft_tokens += count;
        if (image_soft_token_count != expected_soft_tokens) return error.ImagePlaceholderCountMismatch;
    }
    if (projected_audio) |projected| {
        if (audio_idx != projected.tokens_per_audio.len) return error.AudioPlaceholderCountMismatch;
        var expected_soft_tokens: usize = 0;
        for (projected.tokens_per_audio) |count| expected_soft_tokens += count;
        if (audio_soft_token_count != expected_soft_tokens) return error.AudioPlaceholderCountMismatch;
    }

    const embed_w = try getEmbeddingWeight(cb, config);
    defer cb.free(embed_w);
    const base_embeddings = try cb.embeddingLookup(embed_w, token_ids, token_ids.len, config.hidden_size);
    defer cb.free(base_embeddings);

    const hidden_size: usize = config.hidden_size;
    const prompt_embeddings = try cb.toFloat32(base_embeddings, allocator);
    defer allocator.free(prompt_embeddings);

    const embedding_scale = config.tokenEmbeddingScale();
    if (!std.math.approxEqAbs(f32, embedding_scale, 1.0, 1e-6)) {
        for (prompt_embeddings) |*value| value.* *= embedding_scale;
    }

    if (projected_images) |projected| {
        var src_token_offset: usize = 0;
        for (image_offsets, 0..) |offset, idx| {
            const tokens = projected.tokens_per_image[idx];
            const dst = offset * hidden_size;
            const src = src_token_offset * hidden_size;
            @memcpy(
                prompt_embeddings[dst..][0 .. tokens * hidden_size],
                projected.embeddings[src..][0 .. tokens * hidden_size],
            );
            src_token_offset += tokens;
        }
    }
    if (projected_audio) |projected| {
        var src_token_offset: usize = 0;
        for (audio_offsets, 0..) |offset, idx| {
            const tokens = projected.tokens_per_audio[idx];
            const dst = offset * hidden_size;
            const src = src_token_offset * hidden_size;
            @memcpy(
                prompt_embeddings[dst..][0 .. tokens * hidden_size],
                projected.embeddings[src..][0 .. tokens * hidden_size],
            );
            src_token_offset += tokens;
        }
    }

    const embedding_shape = [_]i32{ @intCast(token_ids.len), @intCast(hidden_size) };
    const input_embeddings = try cb.fromFloat32Shape(prompt_embeddings, &embedding_shape);
    errdefer cb.free(input_embeddings);
    return .{
        .allocator = allocator,
        .token_ids = token_ids,
        .ple_token_ids = ple_token_ids,
        .input_embeddings = input_embeddings,
        .attn_or_mask = null,
    };
}

fn resolveSpecialTokenIds(allocator: std.mem.Allocator, tokenizer: tokenizer_mod.Tokenizer) !SpecialTokenIds {
    return .{
        .boi = try singleTokenId(allocator, tokenizer, "<|image>"),
        .image = try singleTokenId(allocator, tokenizer, "<|image|>"),
        .eoi = try singleTokenId(allocator, tokenizer, "<image|>"),
        .boa = try singleTokenId(allocator, tokenizer, "<|audio>"),
        .audio = try singleTokenId(allocator, tokenizer, "<|audio|>"),
        .eoa = try singleTokenId(allocator, tokenizer, "<audio|>"),
    };
}

fn singleTokenId(allocator: std.mem.Allocator, tokenizer: tokenizer_mod.Tokenizer, literal: []const u8) !i32 {
    const ids = try tokenizer.encode(allocator, literal);
    defer allocator.free(ids);
    if (ids.len != 1) return error.InvalidGemma4ImageToken;
    return ids[0];
}

fn getEmbeddingWeight(cb: *const ComputeBackend, config: gpt_mod.Config) !ops.CT {
    if (config.weight_prefix.len > 0) {
        var prefixed_buf: [256]u8 = undefined;
        const prefixed = std.fmt.bufPrint(&prefixed_buf, "{s}.embed_tokens.weight", .{config.weight_prefix}) catch return error.NameTooLong;
        if (cb.getWeight(prefixed)) |weight| return weight else |err| switch (err) {
            error.MissingWeight => {},
            else => return err,
        }
    }
    return switch (config.family) {
        .gpt2 => cb.getWeight("wte.weight"),
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .phi => cb.getWeight("model.embed_tokens.weight"),
        else => cb.getWeight("model.embed_tokens.weight") catch try cb.getWeight("wte.weight"),
    };
}

test "gemma4 expands dynamic image token sequence" {
    const allocator = std.testing.allocator;
    const expanded = try expandPromptText(allocator, "a <|image|> b", &.{3}, &.{});
    defer allocator.free(expanded);
    try std.testing.expectEqualStrings("a <|image><|image|><|image|><|image|><image|> b", expanded);
}

test "gemma4 expands dynamic audio token sequence" {
    const allocator = std.testing.allocator;
    const expanded = try expandPromptText(allocator, "a <|audio|> b", &.{}, &.{2});
    defer allocator.free(expanded);
    try std.testing.expectEqualStrings("a <|audio><|audio|><|audio|><audio|> b", expanded);
}
