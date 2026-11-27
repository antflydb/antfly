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

const std = @import("std");
const ops = @import("../ops/ops.zig");
const tokenizer_mod = @import("inference_tokenizer");
const gpt_mod = @import("../models/gpt.zig");
const gemma3_mm = @import("../pipelines/gemma3_multimodal.zig");
const gemma4_projector = @import("gemma4_projector.zig");
const prompt_rows = @import("../execution/decoder_prefill.zig");

const ComputeBackend = ops.ComputeBackend;

const SpecialTokenIds = struct {
    boi: i32,
    image: i32,
    eoi: i32,
    boa: i32,
    audio: i32,
    eoa: i32,
};

/// Gemma4 video uses timestamped, independently bounded vision spans. Internally
/// use the existing image placeholder for soft slots: it is replaced by projected
/// rows and masked from PLE, so no dynamic tokenizer vocabulary entry is needed.
/// This preserves the same per-frame vision mask as upstream video token slots.
pub fn expandVideoPromptText(allocator: std.mem.Allocator, prompt: []const u8, timestamps_us: []const i64) ![]u8 {
    const marker = "<|video|>";
    const offset = std.mem.indexOf(u8, prompt, marker) orelse return error.VideoPlaceholderCountMismatch;
    if (std.mem.indexOfPos(u8, prompt, offset + marker.len, marker) != null or timestamps_us.len == 0 or timestamps_us.len > @import("antfly_video").packet_sampling.gemma4_max_frames) return error.VideoPlaceholderCountMismatch;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, prompt[0..offset]);
    var last: i64 = -1;
    for (timestamps_us, 0..) |us, i| {
        if (us <= last) return error.InvalidVideoTimestamp;
        last = us;
        if (i != 0) try out.append(allocator, ' ');
        var buffer: [32]u8 = undefined;
        const seconds: u64 = @intCast(@divFloor(us, 1_000_000));
        try out.appendSlice(allocator, try std.fmt.bufPrint(&buffer, "{d:0>2}:{d:0>2} <|image|>", .{ @divFloor(seconds, 60), @mod(seconds, 60) }));
    }
    try out.appendSlice(allocator, prompt[offset + marker.len ..]);
    return out.toOwnedSlice(allocator);
}

test "gemma4 video prompt preserves timestamps and frame boundaries" {
    const a = std.testing.allocator;
    const result = try expandVideoPromptText(a, "User: <|video|> summarize", &.{ 0, 1_500_000, 59_000_000 });
    defer a.free(result);
    try std.testing.expectEqualStrings("User: 00:00 <|image|> 00:01 <|image|> 00:59 <|image|> summarize", result);
    const expanded = try expandPromptText(a, result, &.{ 2, 1, 2 }, &.{});
    defer a.free(expanded);
    try std.testing.expectEqualStrings("User: 00:00 <|image><|image|><|image|><image|> 00:01 <|image><|image|><image|> 00:59 <|image><|image|><|image|><image|> summarize", expanded);
    try std.testing.expectError(error.VideoPlaceholderCountMismatch, expandVideoPromptText(a, "<|video|><|video|>", &.{0}));
    try std.testing.expectError(error.InvalidVideoTimestamp, expandVideoPromptText(a, "<|video|>", &.{ 1_000_000, 0 }));
}

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
        "<start_of_image>",
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
        null,
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
    marker_alias: ?[]const u8,
    begin_marker: []const u8,
    end_marker: []const u8,
    token_counts: []const usize,
    mismatch_error: anyerror,
) ![]u8 {
    if (token_counts.len == 0) return try allocator.dupe(u8, prompt);

    var count: usize = 0;
    var cursor: usize = 0;
    while (nextMarker(prompt, cursor, marker, marker_alias)) |found| {
        count += 1;
        cursor = found.idx + found.len;
    }
    if (count != token_counts.len) return mismatch_error;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    cursor = 0;
    var replacement_idx: usize = 0;
    while (nextMarker(prompt, cursor, marker, marker_alias)) |found| {
        try out.appendSlice(allocator, prompt[cursor..found.idx]);
        try out.appendSlice(allocator, begin_marker);
        for (0..token_counts[replacement_idx]) |_| {
            try out.appendSlice(allocator, marker);
        }
        try out.appendSlice(allocator, end_marker);
        replacement_idx += 1;
        cursor = found.idx + found.len;
    }
    try out.appendSlice(allocator, prompt[cursor..]);
    return try out.toOwnedSlice(allocator);
}

const MarkerMatch = struct {
    idx: usize,
    len: usize,
};

fn nextMarker(prompt: []const u8, cursor: usize, marker: []const u8, marker_alias: ?[]const u8) ?MarkerMatch {
    const primary = std.mem.indexOfPos(u8, prompt, cursor, marker);
    if (marker_alias) |alias_marker| {
        const alias = std.mem.indexOfPos(u8, prompt, cursor, alias_marker);
        if (primary == null) {
            if (alias) |idx| return .{ .idx = idx, .len = alias_marker.len };
            return null;
        }
        if (alias) |idx| {
            if (idx < primary.?) return .{ .idx = idx, .len = alias_marker.len };
        }
        return .{ .idx = primary.?, .len = marker.len };
    }
    if (primary) |idx| return .{ .idx = idx, .len = marker.len };
    return null;
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
    return prepareExpandedPromptEmbeddingsFromRows(
        cb,
        allocator,
        tokenizer,
        config,
        expanded_token_ids,
        if (projected_images) |value| value.promptRows() else null,
        if (projected_audio) |value| value.promptRows() else null,
    );
}

fn copyMediaPromptRows(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    prompt: ops.CT,
    offsets: []const usize,
    projected: prompt_rows.ProjectedInputRows,
) !void {
    const counts = projected.rows_per_item;
    const width = projected.width;
    const resident = if (projected.source == .resident) projected.source.resident.rows else null;
    const source = resident orelse uploaded: {
        const host = projected.source.host;
        var rows: usize = 0;
        for (counts) |count| rows = try std.math.add(usize, rows, count);
        const shape = [_]i32{
            std.math.cast(i32, rows) orelse return error.InvalidTensorShape,
            std.math.cast(i32, width) orelse return error.InvalidTensorShape,
        };
        break :uploaded try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(host, &shape));
    };
    defer if (resident == null) cb.free(source);
    var source_row: usize = 0;
    for (offsets, counts) |offset, count| {
        try cb.checkExecutionControl();
        if (count != 0 and !try cb.copyRows2D(allocator, prompt, offset, source, source_row, count, width))
            return error.ResidentImageOperationUnavailable;
        source_row += count;
    }
}

pub fn prepareExpandedPromptEmbeddingsFromRows(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    config: gpt_mod.Config,
    expanded_token_ids: []const i32,
    projected_images: ?prompt_rows.ProjectedInputRows,
    projected_audio: ?prompt_rows.ProjectedInputRows,
) !gemma3_mm.PreparedPrompt {
    if (projected_images == null and projected_audio == null) return error.NoMultimodalInputs;
    const projections = [_]?prompt_rows.ProjectedInputRows{ projected_images, projected_audio };
    for (projections, 0..) |maybe, modality| {
        const projected = maybe orelse continue;
        if (projected.rows_per_item.len == 0) return if (modality == 0) error.NoImages else error.NoAudio;
        try projected.validateFor(cb, config.hidden_size);
        if (projected.source == .resident and !cb.supportsCopyRows2D())
            return error.ResidentImageOperationUnavailable;
    }

    const special = try resolveSpecialTokenIds(allocator, tokenizer);
    var token_ids = try allocator.alloc(i64, expanded_token_ids.len);
    errdefer allocator.free(token_ids);
    const ple_token_ids: ?[]i64 = if (config.hasPle()) try allocator.alloc(i64, expanded_token_ids.len) else null;
    errdefer if (ple_token_ids) |ids| allocator.free(ids);
    const image_count = if (projected_images) |projected| projected.rows_per_item.len else 0;
    const audio_count = if (projected_audio) |projected| projected.rows_per_item.len else 0;
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
                    if (image_idx >= projected.rows_per_item.len) return error.ImagePlaceholderCountMismatch;
                    const expected = projected.rows_per_item[image_idx];
                    if (idx - start != expected) return error.ImagePlaceholderCountMismatch;
                    if (start == 0 or expanded_token_ids[start - 1] != special.boi) return error.ImagePlaceholderCountMismatch;
                    if (token_id != special.eoi) return error.ImagePlaceholderCountMismatch;
                    image_offsets[image_idx] = start;
                    image_idx += 1;
                },
                .audio => {
                    const projected = projected_audio orelse return error.AudioPlaceholderCountMismatch;
                    if (audio_idx >= projected.rows_per_item.len) return error.AudioPlaceholderCountMismatch;
                    const expected = projected.rows_per_item[audio_idx];
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
        if (image_idx != projected.rows_per_item.len) return error.ImagePlaceholderCountMismatch;
        var expected_soft_tokens: usize = 0;
        for (projected.rows_per_item) |count| expected_soft_tokens += count;
        if (image_soft_token_count != expected_soft_tokens) return error.ImagePlaceholderCountMismatch;
    }
    if (projected_audio) |projected| {
        if (audio_idx != projected.rows_per_item.len) return error.AudioPlaceholderCountMismatch;
        var expected_soft_tokens: usize = 0;
        for (projected.rows_per_item) |count| expected_soft_tokens += count;
        if (audio_soft_token_count != expected_soft_tokens) return error.AudioPlaceholderCountMismatch;
    }

    const embed_w = try getEmbeddingWeight(cb, config);
    defer cb.free(embed_w);
    const base_embeddings = try cb.embeddingLookup(embed_w, token_ids, token_ids.len, config.hidden_size);
    defer cb.free(base_embeddings);

    const hidden_size: usize = config.hidden_size;
    const offsets = [_][]const usize{ image_offsets, audio_offsets };
    const has_resident = for (projections) |maybe| {
        if (maybe) |projected| if (projected.source == .resident) break true;
    } else false;
    const embedding_scale = config.tokenEmbeddingScale();
    const unit_scale = std.math.approxEqAbs(f32, embedding_scale, 1.0, 1e-6);
    const device_assembly = has_resident or (cb.kind() == .metal and cb.supportsCopyRows2D());
    const scaled = if (device_assembly)
        try cb.multiplyScalar(base_embeddings, if (!has_resident and unit_scale) 1.0 else embedding_scale)
    else
        null;
    if (scaled) |owned| {
        const input_embeddings = try cb.ensureDeviceResidentOwned(owned);
        errdefer cb.free(input_embeddings);
        for (projections, offsets) |maybe, starts| {
            if (maybe) |projected| try copyMediaPromptRows(cb, allocator, input_embeddings, starts, projected);
        }
        return .{ .allocator = allocator, .token_ids = token_ids, .ple_token_ids = ple_token_ids, .input_embeddings = input_embeddings, .attn_or_mask = null };
    }

    if (has_resident) return error.ResidentImageOperationUnavailable;

    const prompt_embeddings = try cb.toFloat32(base_embeddings, allocator);
    defer allocator.free(prompt_embeddings);

    if (!unit_scale) {
        for (prompt_embeddings) |*value| value.* *= embedding_scale;
    }

    for (projections, offsets) |maybe, starts| {
        const projected = maybe orelse continue;
        var source_offset: usize = 0;
        for (starts, projected.rows_per_item) |offset, rows| {
            const elements = rows * hidden_size;
            @memcpy(prompt_embeddings[offset * hidden_size ..][0..elements], projected.source.host[source_offset..][0..elements]);
            source_offset += elements;
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
        .llama, .mistral, .qwen2, .qwen3, .qwen3_vl, .qwen3_5, .gemma, .phi => cb.getWeight("model.embed_tokens.weight"),
        else => cb.getWeight("model.embed_tokens.weight") catch try cb.getWeight("wte.weight"),
    };
}

test "gemma4 expands dynamic image token sequence" {
    const allocator = std.testing.allocator;
    const expanded = try expandPromptText(allocator, "a <|image|> b", &.{3}, &.{});
    defer allocator.free(expanded);
    try std.testing.expectEqualStrings("a <|image><|image|><|image|><|image|><image|> b", expanded);
}

test "gemma4 expands fallback image marker alias" {
    const allocator = std.testing.allocator;
    const expanded = try expandPromptText(allocator, "a <start_of_image> b", &.{2}, &.{});
    defer allocator.free(expanded);
    try std.testing.expectEqualStrings("a <|image><|image|><|image|><image|> b", expanded);
}

test "gemma4 expands dynamic audio token sequence" {
    const allocator = std.testing.allocator;
    const expanded = try expandPromptText(allocator, "a <|audio|> b", &.{}, &.{2});
    defer allocator.free(expanded);
    try std.testing.expectEqualStrings("a <|audio><|audio|><|audio|><audio|> b", expanded);
}

test "Metal assembles host-projected Gemma media directly on device" {
    const helpers = @import("../ops/media_residency_test.zig");
    const Fixture = struct {
        fn weights(store: *@import("../ops/native_compute.zig").WeightStore) !void {
            try helpers.putWeight(store, "model.embed_tokens.weight", &.{ 8, 4 }, 0.25);
        }

        fn check(backend: ComputeBackend, reference: ComputeBackend) !void {
            const a = std.testing.allocator;
            const tokenizer_json =
                \\{"model":{"type":"BPE","unk_token":"[UNK]","vocab":{"[UNK]":0,"<|image>":1,"<|image|>":2,"<image|>":3,"<|audio>":4,"<|audio|>":5,"<audio|>":6,"text":7},"merges":[]},
                \\"added_tokens":[
                \\{"id":1,"content":"<|image>","special":true,"normalized":false},
                \\{"id":2,"content":"<|image|>","special":true,"normalized":false},
                \\{"id":3,"content":"<image|>","special":true,"normalized":false},
                \\{"id":4,"content":"<|audio>","special":true,"normalized":false},
                \\{"id":5,"content":"<|audio|>","special":true,"normalized":false},
                \\{"id":6,"content":"<audio|>","special":true,"normalized":false}
                \\]}
            ;
            const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_json);
            defer tokenizer.deinitSelf();
            const model: gpt_mod.Config = .{ .family = .gemma, .hidden_size = 4 };
            const tokens = [_]i32{ 1, 2, 3, 4, 5, 6, 7 };
            const image: prompt_rows.ProjectedInputRows = .{ .rows_per_item = &.{1}, .width = 4, .source = .{ .host = &.{ 1, 2, 3, 4 } } };
            const audio: prompt_rows.ProjectedInputRows = .{ .rows_per_item = &.{1}, .width = 4, .source = .{ .host = &.{ 5, 6, 7, 8 } } };
            var actual = try prepareExpandedPromptEmbeddingsFromRows(&backend, a, tokenizer.tokenizer(), model, &tokens, image, audio);
            defer actual.deinit(&backend);
            const actual_tensor = actual.input_embeddings orelse return error.MissingResidentPromptEmbedding;

            try std.testing.expect(@import("../ops/metal_compute.zig").MetalCompute.debugHasDeviceTensor(&backend, actual_tensor));

            var expected = try prepareExpandedPromptEmbeddingsFromRows(&reference, a, tokenizer.tokenizer(), model, &tokens, image, audio);
            defer expected.deinit(&reference);
            const have = try backend.toFloat32(actual_tensor, a);
            defer a.free(have);
            const want = try reference.toFloat32(expected.input_embeddings orelse return error.MissingReferencePromptEmbedding, a);
            defer a.free(want);
            try std.testing.expectEqualSlices(f32, want, have);
        }
    };
    try helpers.onMetal(Fixture.weights, Fixture.check);
}
