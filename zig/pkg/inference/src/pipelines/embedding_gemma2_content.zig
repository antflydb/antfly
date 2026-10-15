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

//! Ordered multimodal input preparation for EmbeddingGemma 2.
//!
//! The upstream processor concatenates content in caller order, expands each
//! media marker to a begin token, one placeholder per tower output token, and
//! an end token, then applies BOS/EOS once around the complete input. Text
//! embeddings are scaled by sqrt(512); projected media embeddings replace the
//! placeholder rows after that lookup and are deliberately left unscaled.

const std = @import("std");
const tokenizer_mod = @import("inference_tokenizer");
const ops = @import("../ops/ops.zig");
const gemma4_projector = @import("../architectures/gemma4_projector.zig");
const audio = @import("audio.zig");
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

pub const ComputeBackend = ops.ComputeBackend;
pub const Tokenizer = tokenizer_mod.Tokenizer;

pub const hidden_size: usize = 512;
pub const bos_token_id: i64 = 2;
pub const eos_token_id: i64 = 1;
pub const begin_image_token_id: i64 = 255_999;
pub const image_token_id: i64 = 258_880;
pub const end_image_token_id: i64 = 258_882;
pub const begin_audio_token_id: i64 = 256_000;
pub const audio_token_id: i64 = 258_881;
pub const end_audio_token_id: i64 = 258_883;

const begin_image_token = "<|image>";
const image_token = "<|image|>";
const end_image_token = "<image|>";
const begin_audio_token = "<|audio>";
const audio_token = "<|audio|>";
const end_audio_token = "<audio|>";

/// Kept independent from embedding.zig to avoid an import cycle. Callers may
/// translate EmbeddingContentPart directly; both unions preserve borrowed
/// encoded media bytes.
pub const Part = union(enum) {
    text: []const u8,
    image: []const u8,
    audio: struct {
        bytes: []const u8,
        decode_options: audio.DecodeOptions = .{},
    },
};

pub const Prepared = struct {
    allocator: std.mem.Allocator,
    input_ids: []i64,
    attention_mask: []i64,
    /// Row-major `[1, sequence, 512]` host input for the resident encoder.
    input_embeddings: []f32,
    sequence: usize,

    pub fn deinit(self: *Prepared) void {
        self.allocator.free(self.input_ids);
        self.allocator.free(self.attention_mask);
        self.allocator.free(self.input_embeddings);
        self.* = undefined;
    }
};

const Media = struct {
    kind: enum { image, audio },
    embeddings: []f32,
    tokens: usize,

    fn deinit(self: *Media, allocator: std.mem.Allocator) void {
        allocator.free(self.embeddings);
        self.* = undefined;
    }
};

fn appendRepeated(out: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: []const u8, count: usize) !void {
    const additional = std.math.mul(usize, value.len, count) catch return error.EmbeddingSequenceTooLong;
    try out.ensureUnusedCapacity(allocator, additional);
    for (0..count) |_| out.appendSliceAssumeCapacity(value);
}

fn appendMediaText(
    out: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    kind: @FieldType(Media, "kind"),
    count: usize,
) !void {
    if (count == 0) return error.EmptyMediaEmbedding;
    switch (kind) {
        .image => {
            try out.appendSlice(allocator, begin_image_token);
            try appendRepeated(out, allocator, image_token, count);
            try out.appendSlice(allocator, end_image_token);
        },
        .audio => {
            try out.appendSlice(allocator, begin_audio_token);
            try appendRepeated(out, allocator, audio_token, count);
            try out.appendSlice(allocator, end_audio_token);
        },
    }
}

fn checkControl(control: ?InferenceExecutionControl) !void {
    if (control) |value| try value.check();
}

fn roundToBfloat16(value: f32) f32 {
    const bits: u32 = @bitCast(value);
    // Round-to-nearest-even, matching CUDA/PyTorch's BF16 conversion.
    const rounded = bits +% 0x7fff + ((bits >> 16) & 1);
    return @bitCast(rounded & 0xffff0000);
}

fn containsManualMarker(parts: []const Part) bool {
    for (parts) |part| switch (part) {
        .text => |text| if (std.mem.indexOf(u8, text, image_token) != null or std.mem.indexOf(u8, text, audio_token) != null) return true,
        else => {},
    };
    return false;
}

/// Processor.validate_inputs rejects placeholders without their media. Text
/// batching bypasses the tower frontend, so enforce the same contract there.
pub fn validateTextOnly(text: []const u8) !void {
    if (std.mem.indexOf(u8, text, image_token) != null or
        std.mem.indexOf(u8, text, audio_token) != null or
        std.mem.indexOf(u8, text, "<|video|>") != null)
        return error.MediaPlaceholderMismatch;
}

fn nextMediaOfKind(media: []const Media, used: []bool, kind: @FieldType(Media, "kind")) ?usize {
    for (media, 0..) |item, index| {
        if (!used[index] and item.kind == kind) return index;
    }
    return null;
}

fn appendTextWithManualMarkers(
    rendered: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    text: []const u8,
    media: []const Media,
    used: []bool,
    media_order: *std.ArrayListUnmanaged(Media),
) !void {
    var remaining = text;
    while (remaining.len > 0) {
        const image_at = std.mem.indexOf(u8, remaining, image_token);
        const audio_at = std.mem.indexOf(u8, remaining, audio_token);
        const selected: ?struct { at: usize, kind: @FieldType(Media, "kind"), len: usize } = if (image_at) |at|
            if (audio_at) |audio_pos| (if (at <= audio_pos) .{ .at = at, .kind = .image, .len = image_token.len } else .{ .at = audio_pos, .kind = .audio, .len = audio_token.len }) else .{ .at = at, .kind = .image, .len = image_token.len }
        else if (audio_at) |at|
            .{ .at = at, .kind = .audio, .len = audio_token.len }
        else
            null;
        const marker = selected orelse {
            try rendered.appendSlice(allocator, remaining);
            break;
        };
        try rendered.appendSlice(allocator, remaining[0..marker.at]);
        const media_index = nextMediaOfKind(media, used, marker.kind) orelse return error.MediaPlaceholderMismatch;
        used[media_index] = true;
        const item = media[media_index];
        try appendMediaText(rendered, allocator, item.kind, item.tokens);
        try media_order.append(allocator, item);
        remaining = remaining[marker.at + marker.len ..];
    }
}

/// Prepare one ordered content group. The returned slices are owned by
/// `allocator`; media bytes and the tokenizer/backend remain borrowed.
pub fn prepare(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tok: Tokenizer,
    prefix: []const u8,
    parts: []const Part,
    max_tokens: usize,
    max_audio_decode_working_bytes: usize,
    control: ?InferenceExecutionControl,
) !Prepared {
    if (parts.len == 0) return error.EmptyEmbeddingInput;
    if (max_tokens == 0 or max_tokens > 8192) return error.InvalidEmbeddingTokenLimit;
    // Video is outside this serving lane, including manual placeholders.
    for (parts) |part| switch (part) {
        .text => |text| if (std.mem.indexOf(u8, text, "<|video|>") != null) return error.MediaPlaceholderMismatch,
        else => {},
    };

    var rendered: std.ArrayListUnmanaged(u8) = .empty;
    defer rendered.deinit(allocator);
    var media: std.ArrayListUnmanaged(Media) = .empty;
    defer {
        for (media.items) |*item| item.deinit(allocator);
        media.deinit(allocator);
    }
    // Towers run first because their dynamic token counts define marker
    // expansion. Their owned outputs remain in content-part order here.
    for (parts) |part| {
        try checkControl(control);
        switch (part) {
            .text => {},
            .image => |bytes| {
                if (bytes.len == 0) return error.InvalidImageInput;
                const encoded = try gemma4_projector.encodeEmbeddingGemma2Image(cb, allocator, bytes);
                errdefer allocator.free(encoded.embeddings);
                if (encoded.embeddings.len != std.math.mul(usize, encoded.tokens, hidden_size) catch return error.InvalidMediaEmbeddingShape)
                    return error.InvalidMediaEmbeddingShape;
                try media.append(allocator, .{ .kind = .image, .embeddings = encoded.embeddings, .tokens = encoded.tokens });
            },
            .audio => |clip| {
                if (clip.bytes.len == 0) return error.InvalidAudioInput;
                const encoded = try gemma4_projector.encodeEmbeddingGemma2AudioBounded(cb, allocator, clip.bytes, clip.decode_options, max_audio_decode_working_bytes);
                errdefer allocator.free(encoded.embeddings);
                if (encoded.embeddings.len != std.math.mul(usize, encoded.tokens, hidden_size) catch return error.InvalidMediaEmbeddingShape)
                    return error.InvalidMediaEmbeddingShape;
                try media.append(allocator, .{ .kind = .audio, .embeddings = encoded.embeddings, .tokens = encoded.tokens });
            },
        }
    }

    const manual_markers = containsManualMarker(parts);
    const used = try allocator.alloc(bool, media.items.len);
    defer allocator.free(used);
    @memset(used, false);
    var media_order: std.ArrayListUnmanaged(Media) = .empty;
    defer media_order.deinit(allocator);
    var prefixed = false;
    var automatic_media_index: usize = 0;
    for (parts) |part| switch (part) {
        .text => |text| {
            if (!prefixed) {
                try rendered.appendSlice(allocator, prefix);
                prefixed = true;
            }
            if (manual_markers)
                try appendTextWithManualMarkers(&rendered, allocator, text, media.items, used, &media_order)
            else
                try rendered.appendSlice(allocator, text);
        },
        .image, .audio => {
            const item = media.items[automatic_media_index];
            automatic_media_index += 1;
            if (!manual_markers) {
                used[automatic_media_index - 1] = true;
                try appendMediaText(&rendered, allocator, item.kind, item.tokens);
                try media_order.append(allocator, item);
            }
        },
    };
    for (used) |was_used| if (!was_used) return error.MediaPlaceholderMismatch;
    if (rendered.items.len == 0) return error.EmptyEmbeddingInput;

    // `encode` omits model wrappers, matching ProcessorMixin's final tokenizer
    // pass after marker expansion. BOS and EOS are applied once below.
    const inner = try tok.encode(allocator, rendered.items);
    defer allocator.free(inner);
    const sequence = std.math.add(usize, inner.len, 2) catch return error.EmbeddingSequenceTooLong;
    if (sequence > max_tokens) return error.EmbeddingSequenceTooLong;

    const input_ids = try allocator.alloc(i64, sequence);
    errdefer allocator.free(input_ids);
    input_ids[0] = bos_token_id;
    for (inner, 0..) |id, index| input_ids[index + 1] = id;
    input_ids[sequence - 1] = eos_token_id;
    try validateMediaLayout(input_ids, media_order.items);

    const attention_mask = try allocator.alloc(i64, sequence);
    errdefer allocator.free(attention_mask);
    @memset(attention_mask, 1);

    try checkControl(control);
    const embedding_weight = try cb.getWeight("language_model.embed_tokens.weight");
    defer cb.free(embedding_weight);
    const device_embeddings = try cb.embeddingLookup(embedding_weight, input_ids, sequence, hidden_size);
    defer cb.free(device_embeddings);
    const input_embeddings = try cb.toFloat32(device_embeddings, allocator);
    errdefer allocator.free(input_embeddings);
    if (input_embeddings.len != sequence * hidden_size) return error.InvalidTokenEmbeddingShape;

    // Upstream casts sqrt(512) to the embedding weight dtype before the
    // multiply: BF16 represents it as exactly 22.625, and the product is BF16.
    const token_scale_bf16: f32 = 22.625;
    for (input_embeddings) |*value| value.* = roundToBfloat16(value.* * token_scale_bf16);
    try replaceMediaEmbeddings(input_ids, input_embeddings, media_order.items);
    try checkControl(control);

    return .{
        .allocator = allocator,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .input_embeddings = input_embeddings,
        .sequence = sequence,
    };
}

fn mediaTokenId(kind: @FieldType(Media, "kind")) i64 {
    return switch (kind) {
        .image => image_token_id,
        .audio => audio_token_id,
    };
}

fn mediaWrapperIds(kind: @FieldType(Media, "kind")) struct { begin: i64, end: i64 } {
    return switch (kind) {
        .image => .{ .begin = begin_image_token_id, .end = end_image_token_id },
        .audio => .{ .begin = begin_audio_token_id, .end = end_audio_token_id },
    };
}

fn validateMediaLayout(ids: []const i64, media: []const Media) !void {
    var cursor: usize = 0;
    var media_index: usize = 0;
    while (cursor < ids.len) {
        const id = ids[cursor];
        if (id == begin_image_token_id or id == begin_audio_token_id) {
            if (media_index >= media.len) return error.MediaPlaceholderMismatch;
            const item = media[media_index];
            const wrappers = mediaWrapperIds(item.kind);
            if (id != wrappers.begin) return error.MediaPlaceholderMismatch;
            cursor += 1;
            if (cursor + item.tokens >= ids.len) return error.MediaPlaceholderMismatch;
            for (ids[cursor..][0..item.tokens]) |placeholder| {
                if (placeholder != mediaTokenId(item.kind)) return error.MediaPlaceholderMismatch;
            }
            cursor += item.tokens;
            if (ids[cursor] != wrappers.end) return error.MediaPlaceholderMismatch;
            cursor += 1;
            media_index += 1;
            continue;
        }
        if (id == image_token_id or id == audio_token_id or
            id == end_image_token_id or id == end_audio_token_id)
            return error.MediaPlaceholderMismatch;
        cursor += 1;
    }
    if (media_index != media.len) return error.MediaPlaceholderMismatch;
}

fn replaceMediaEmbeddings(ids: []const i64, embeddings: []f32, media: []const Media) !void {
    var media_index: usize = 0;
    var token_offset: usize = 0;
    for (ids, 0..) |id, sequence_index| {
        if (id != image_token_id and id != audio_token_id) continue;
        if (media_index >= media.len or id != mediaTokenId(media[media_index].kind))
            return error.MediaPlaceholderMismatch;
        const item = media[media_index];
        const destination = sequence_index * hidden_size;
        const source = token_offset * hidden_size;
        @memcpy(embeddings[destination..][0..hidden_size], item.embeddings[source..][0..hidden_size]);
        token_offset += 1;
        if (token_offset == item.tokens) {
            token_offset = 0;
            media_index += 1;
        }
    }
    if (media_index != media.len or token_offset != 0) return error.MediaPlaceholderMismatch;
}

test "media layout accepts ordered variable token runs" {
    const image_embeddings = try std.testing.allocator.alloc(f32, 2 * hidden_size);
    defer std.testing.allocator.free(image_embeddings);
    const audio_embeddings = try std.testing.allocator.alloc(f32, hidden_size);
    defer std.testing.allocator.free(audio_embeddings);
    const media = [_]Media{
        .{ .kind = .image, .embeddings = image_embeddings, .tokens = 2 },
        .{ .kind = .audio, .embeddings = audio_embeddings, .tokens = 1 },
    };
    try validateMediaLayout(&.{ bos_token_id, begin_image_token_id, image_token_id, image_token_id, end_image_token_id, 42, begin_audio_token_id, audio_token_id, end_audio_token_id, eos_token_id }, &media);
}

test "media layout rejects placeholder count and order mismatches" {
    const values = try std.testing.allocator.alloc(f32, 2 * hidden_size);
    defer std.testing.allocator.free(values);
    const media = [_]Media{.{ .kind = .image, .embeddings = values, .tokens = 2 }};
    try std.testing.expectError(error.MediaPlaceholderMismatch, validateMediaLayout(&.{ bos_token_id, begin_image_token_id, image_token_id, end_image_token_id, eos_token_id }, &media));
    try std.testing.expectError(error.MediaPlaceholderMismatch, validateMediaLayout(&.{ bos_token_id, begin_audio_token_id, audio_token_id, end_audio_token_id, eos_token_id }, &media));
}

test "media replacement preserves wrapper rows and does not scale tower values" {
    const embeddings = try std.testing.allocator.alloc(f32, 5 * hidden_size);
    defer std.testing.allocator.free(embeddings);
    @memset(embeddings, 0);
    const tower = try std.testing.allocator.alloc(f32, hidden_size);
    defer std.testing.allocator.free(tower);
    for (tower, 0..) |*value, index| value.* = @floatFromInt(index);
    const media = [_]Media{.{ .kind = .image, .embeddings = tower, .tokens = 1 }};
    try replaceMediaEmbeddings(&.{ bos_token_id, begin_image_token_id, image_token_id, end_image_token_id, eos_token_id }, embeddings, &media);
    try std.testing.expectEqualSlices(f32, tower, embeddings[2 * hidden_size ..][0..hidden_size]);
    try std.testing.expectEqual(@as(f32, 0), embeddings[hidden_size]);
}

test "text token scaling follows BF16 cast boundaries" {
    try std.testing.expectEqual(@as(f32, 22.625), roundToBfloat16(@as(f32, 1.0) * 22.625));
    try std.testing.expectEqual(@as(f32, 0.2265625), roundToBfloat16(@as(f32, 0.01) * 22.625));
}

test "manual markers consume supplied media in textual order" {
    const parts = [_]Part{
        .{ .text = "listen <|audio|> then inspect <|image|>" },
        .{ .image = "encoded-image" },
        .{ .audio = .{ .bytes = "encoded-audio" } },
    };
    try std.testing.expect(containsManualMarker(&parts));
    var no_embeddings: [0]f32 = .{};
    const media = [_]Media{
        .{ .kind = .image, .embeddings = &no_embeddings, .tokens = 2 },
        .{ .kind = .audio, .embeddings = &no_embeddings, .tokens = 1 },
    };
    var used = [_]bool{ false, false };
    var rendered: std.ArrayListUnmanaged(u8) = .empty;
    defer rendered.deinit(std.testing.allocator);
    var order: std.ArrayListUnmanaged(Media) = .empty;
    defer order.deinit(std.testing.allocator);
    try appendTextWithManualMarkers(&rendered, std.testing.allocator, parts[0].text, &media, &used, &order);
    try std.testing.expectEqualStrings(
        "listen <|audio><|audio|><audio|> then inspect <|image><|image|><|image|><image|>",
        rendered.items,
    );
    try std.testing.expectEqual(@as(usize, 2), order.items.len);
    try std.testing.expectEqual(.audio, order.items[0].kind);
    try std.testing.expectEqual(.image, order.items[1].kind);
    try std.testing.expect(used[0] and used[1]);
}

test "EmbeddingGemma2 text-only requests reject missing media and deferred video" {
    try validateTextOnly("Multilingual code: fn dot(a: []f32) f32 { return a[0]; } 世界 <bos>");
    try std.testing.expectError(error.MediaPlaceholderMismatch, validateTextOnly("describe <|image|>"));
    try std.testing.expectError(error.MediaPlaceholderMismatch, validateTextOnly("listen <|audio|>"));
    try std.testing.expectError(error.MediaPlaceholderMismatch, validateTextOnly("summarize <|video|>"));
}
