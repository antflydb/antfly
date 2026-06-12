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
const c_file = @import("../util/c_file.zig");
const image = @import("image.zig");
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;
const gpt_mod = @import("../models/gpt.zig");
const gemma3_vision = @import("../architectures/gemma3_vision.zig");
const gemma3_projector = @import("../architectures/gemma3_projector.zig");

pub const PreprocessorConfig = struct {
    image_size: u32 = 896,
    image_mean: [3]f32 = .{ 0.5, 0.5, 0.5 },
    image_std: [3]f32 = .{ 0.5, 0.5, 0.5 },
    rescale_factor: f32 = 1.0 / 255.0,
    image_seq_length: u32 = 256,
    do_resize: bool = true,
    do_rescale: bool = true,
    do_normalize: bool = true,
};

pub const ExpandedPrompt = struct {
    allocator: std.mem.Allocator,
    token_ids: []i64,
    image_offsets: []usize,

    pub fn deinit(self: *ExpandedPrompt) void {
        self.allocator.free(self.token_ids);
        self.allocator.free(self.image_offsets);
    }
};

pub const PreparedPrompt = struct {
    allocator: std.mem.Allocator,
    token_ids: []i64,
    ple_token_ids: ?[]i64 = null,
    input_embeddings: ?ops.CT,
    attn_or_mask: ?[]u8,

    pub fn deinit(self: *PreparedPrompt, cb: *const ComputeBackend) void {
        self.allocator.free(self.token_ids);
        if (self.ple_token_ids) |ids| self.allocator.free(ids);
        if (self.input_embeddings) |embeddings| cb.free(embeddings);
        if (self.attn_or_mask) |mask| self.allocator.free(mask);
    }
};

pub fn expandPromptText(
    allocator: std.mem.Allocator,
    prompt: []const u8,
    config: gpt_mod.Config,
    image_count: usize,
) ![]u8 {
    if (!config.isMultimodal()) return error.InvalidMultimodalConfig;
    if (image_count == 0) return try allocator.dupe(u8, prompt);

    const marker = "<start_of_image>";
    const replacement = try buildExpandedImageSequence(allocator, config);
    defer allocator.free(replacement);

    var count: usize = 0;
    var cursor: usize = 0;
    while (std.mem.indexOfPos(u8, prompt, cursor, marker)) |idx| {
        count += 1;
        cursor = idx + marker.len;
    }
    if (count != image_count) return error.ImagePlaceholderCountMismatch;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    cursor = 0;
    while (std.mem.indexOfPos(u8, prompt, cursor, marker)) |idx| {
        try out.appendSlice(allocator, prompt[cursor..idx]);
        try out.appendSlice(allocator, replacement);
        cursor = idx + marker.len;
    }
    try out.appendSlice(allocator, prompt[cursor..]);
    return try out.toOwnedSlice(allocator);
}

pub fn loadPreprocessorConfig(allocator: std.mem.Allocator, model_dir: []const u8) !PreprocessorConfig {
    const bytes = try c_file.readFileFromDir(allocator, model_dir, "preprocessor_config.json");
    defer allocator.free(bytes);
    return parsePreprocessorConfig(allocator, bytes);
}

pub fn parsePreprocessorConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !PreprocessorConfig {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidPreprocessorConfig;
    const obj = parsed.value.object;

    var config = PreprocessorConfig{};
    if (obj.get("image_seq_length")) |v| if (jsonU32(v)) |value| {
        config.image_seq_length = value;
    };
    if (obj.get("rescale_factor")) |v| if (jsonF32(v)) |value| {
        config.rescale_factor = value;
    };
    if (obj.get("do_resize")) |v| if (jsonBool(v)) |value| {
        config.do_resize = value;
    };
    if (obj.get("do_rescale")) |v| if (jsonBool(v)) |value| {
        config.do_rescale = value;
    };
    if (obj.get("do_normalize")) |v| if (jsonBool(v)) |value| {
        config.do_normalize = value;
    };
    if (obj.get("size")) |v| {
        if (v == .object) {
            if (v.object.get("height")) |height| if (jsonU32(height)) |value| {
                config.image_size = value;
            };
        }
    }
    if (obj.get("image_mean")) |v| if (parseRgbTriple(v)) |value| {
        config.image_mean = value;
    };
    if (obj.get("image_std")) |v| if (parseRgbTriple(v)) |value| {
        config.image_std = value;
    };
    return config;
}

pub fn preprocessImage(allocator: std.mem.Allocator, image_bytes: []const u8, config: PreprocessorConfig) ![]f32 {
    if (!config.do_resize or !config.do_rescale or !config.do_normalize) {
        return error.UnsupportedGemma3PreprocessorConfig;
    }
    if (!std.math.approxEqAbs(f32, config.rescale_factor, 1.0 / 255.0, 1e-9)) {
        return error.UnsupportedGemma3PreprocessorConfig;
    }
    return image.preprocess(allocator, image_bytes, config.image_size, config.image_mean, config.image_std);
}

pub fn expandPromptTokens(
    allocator: std.mem.Allocator,
    prompt_token_ids: []const i32,
    config: gpt_mod.Config,
    image_count: usize,
) !ExpandedPrompt {
    if (!config.isMultimodal()) return error.InvalidMultimodalConfig;
    if (image_count == 0) return error.NoImages;

    var token_ids = std.ArrayListUnmanaged(i64).empty;
    errdefer token_ids.deinit(allocator);
    var image_offsets = std.ArrayListUnmanaged(usize).empty;
    errdefer image_offsets.deinit(allocator);

    var placeholder_count: usize = 0;
    for (prompt_token_ids) |token_id| {
        if (token_id == config.boi_token_index) {
            if (placeholder_count >= image_count) return error.ImagePlaceholderCountMismatch;
            try token_ids.append(allocator, config.boi_token_index);
            try image_offsets.append(allocator, token_ids.items.len);
            for (0..config.mm_tokens_per_image) |_| {
                try token_ids.append(allocator, config.image_token_index);
            }
            try token_ids.append(allocator, config.eoi_token_index);
            placeholder_count += 1;
            continue;
        }
        try token_ids.append(allocator, token_id);
    }

    if (placeholder_count != image_count) return error.ImagePlaceholderCountMismatch;

    return .{
        .allocator = allocator,
        .token_ids = try token_ids.toOwnedSlice(allocator),
        .image_offsets = try image_offsets.toOwnedSlice(allocator),
    };
}

pub fn preparePromptEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    config: gpt_mod.Config,
    prompt_token_ids: []const i32,
    images: []const []const u8,
) !PreparedPrompt {
    const pre_cfg = try loadPreprocessorConfig(allocator, model_dir);
    if (pre_cfg.image_seq_length != config.mm_tokens_per_image) return error.ImageTokenLengthMismatch;

    var expanded = try expandPromptTokens(allocator, prompt_token_ids, config, images.len);
    errdefer expanded.deinit();

    const pixels_per_image = 3 * pre_cfg.image_size * pre_cfg.image_size;
    const pixel_values = try allocator.alloc(f32, images.len * pixels_per_image);
    defer allocator.free(pixel_values);

    for (images, 0..) |image_bytes, idx| {
        const processed = try preprocessImage(allocator, image_bytes, pre_cfg);
        defer allocator.free(processed);
        @memcpy(pixel_values[idx * pixels_per_image ..][0..pixels_per_image], processed);
    }

    const projected = try gemma3_vision.encodeProjectedImageTokens(cb, allocator, config, pixel_values, images.len);
    defer allocator.free(projected);

    const embed_w = try getEmbeddingWeight(cb, config);
    defer cb.free(embed_w);
    const base_embeddings = try cb.embeddingLookup(embed_w, expanded.token_ids, expanded.token_ids.len, config.hidden_size);
    defer cb.free(base_embeddings);

    const hidden_size: usize = config.hidden_size;
    const prompt_embeddings = try cb.toFloat32(base_embeddings, allocator);
    defer allocator.free(prompt_embeddings);

    const embedding_scale = config.tokenEmbeddingScale();
    if (!std.math.approxEqAbs(f32, embedding_scale, 1.0, 1e-6)) {
        for (prompt_embeddings) |*value| value.* *= embedding_scale;
    }
    const tokens_per_image: usize = config.mm_tokens_per_image;
    for (expanded.image_offsets, 0..) |offset, image_idx| {
        const dst = offset * hidden_size;
        const src = image_idx * tokens_per_image * hidden_size;
        @memcpy(
            prompt_embeddings[dst..][0 .. tokens_per_image * hidden_size],
            projected[src..][0 .. tokens_per_image * hidden_size],
        );
    }

    const embedding_shape = [_]i32{ @intCast(expanded.token_ids.len), @intCast(hidden_size) };
    const input_embeddings = try cb.fromFloat32Shape(prompt_embeddings, &embedding_shape);
    errdefer cb.free(input_embeddings);
    const attn_or_mask = try buildImageAttentionOrMask(allocator, expanded.token_ids.len, expanded.image_offsets, config.mm_tokens_per_image);
    return .{
        .allocator = allocator,
        .token_ids = expanded.token_ids,
        .input_embeddings = input_embeddings,
        .attn_or_mask = attn_or_mask,
    };
}

pub fn prepareExpandedPromptEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    config: gpt_mod.Config,
    expanded_token_ids: []const i32,
    image_count: usize,
    images: []const []const u8,
) !PreparedPrompt {
    if (image_count == 0 or images.len != image_count) return error.NoImages;

    const pre_cfg = try loadPreprocessorConfig(allocator, model_dir);
    if (pre_cfg.image_seq_length != config.mm_tokens_per_image) return error.ImageTokenLengthMismatch;

    var token_ids = try allocator.alloc(i64, expanded_token_ids.len);
    errdefer allocator.free(token_ids);
    var image_offsets = try allocator.alloc(usize, image_count);
    defer allocator.free(image_offsets);

    var soft_token_count: usize = 0;
    var image_idx: usize = 0;
    var run_start: ?usize = null;
    for (expanded_token_ids, 0..) |token_id, idx| {
        token_ids[idx] = token_id;
        if (token_id == config.image_token_index) {
            soft_token_count += 1;
            if (run_start == null) run_start = idx;
        } else if (run_start) |start| {
            if (idx - start != config.mm_tokens_per_image) return error.ImagePlaceholderCountMismatch;
            if (image_idx >= image_count) return error.ImagePlaceholderCountMismatch;
            image_offsets[image_idx] = start;
            image_idx += 1;
            run_start = null;
        }
    }
    if (run_start) |start| {
        if (expanded_token_ids.len - start != config.mm_tokens_per_image) return error.ImagePlaceholderCountMismatch;
        if (image_idx >= image_count) return error.ImagePlaceholderCountMismatch;
        image_offsets[image_idx] = start;
        image_idx += 1;
    }
    if (soft_token_count != image_count * config.mm_tokens_per_image or image_idx != image_count) {
        return error.ImagePlaceholderCountMismatch;
    }

    const pixels_per_image = 3 * pre_cfg.image_size * pre_cfg.image_size;
    const pixel_values = try allocator.alloc(f32, images.len * pixels_per_image);
    defer allocator.free(pixel_values);
    for (images, 0..) |image_bytes, idx| {
        const processed = try preprocessImage(allocator, image_bytes, pre_cfg);
        defer allocator.free(processed);
        @memcpy(pixel_values[idx * pixels_per_image ..][0..pixels_per_image], processed);
    }

    const projected = try gemma3_vision.encodeProjectedImageTokens(cb, allocator, config, pixel_values, images.len);
    defer allocator.free(projected);

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
    const tokens_per_image: usize = config.mm_tokens_per_image;
    for (image_offsets, 0..) |offset, idx| {
        const dst = offset * hidden_size;
        const src = idx * tokens_per_image * hidden_size;
        @memcpy(
            prompt_embeddings[dst..][0 .. tokens_per_image * hidden_size],
            projected[src..][0 .. tokens_per_image * hidden_size],
        );
    }

    const embedding_shape = [_]i32{ @intCast(token_ids.len), @intCast(hidden_size) };
    const input_embeddings = try cb.fromFloat32Shape(prompt_embeddings, &embedding_shape);
    errdefer cb.free(input_embeddings);
    const attn_or_mask = try buildImageAttentionOrMask(allocator, token_ids.len, image_offsets, config.mm_tokens_per_image);
    return .{
        .allocator = allocator,
        .token_ids = token_ids,
        .input_embeddings = input_embeddings,
        .attn_or_mask = attn_or_mask,
    };
}

pub fn prepareExpandedPromptEmbeddingsWithProjector(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    projector_path: []const u8,
    config: gpt_mod.Config,
    expanded_token_ids: []const i32,
    image_count: usize,
    images: []const []const u8,
) !PreparedPrompt {
    if (image_count == 0 or images.len != image_count) return error.NoImages;

    const pre_cfg = try loadPreprocessorConfig(allocator, model_dir);
    if (pre_cfg.image_seq_length != config.mm_tokens_per_image) return error.ImageTokenLengthMismatch;

    var token_ids = try allocator.alloc(i64, expanded_token_ids.len);
    errdefer allocator.free(token_ids);
    var image_offsets = try allocator.alloc(usize, image_count);
    defer allocator.free(image_offsets);

    var soft_token_count: usize = 0;
    var image_idx: usize = 0;
    var run_start: ?usize = null;
    for (expanded_token_ids, 0..) |token_id, idx| {
        token_ids[idx] = token_id;
        if (token_id == config.image_token_index) {
            soft_token_count += 1;
            if (run_start == null) run_start = idx;
        } else if (run_start) |start| {
            if (idx - start != config.mm_tokens_per_image) return error.ImagePlaceholderCountMismatch;
            if (image_idx >= image_count) return error.ImagePlaceholderCountMismatch;
            image_offsets[image_idx] = start;
            image_idx += 1;
            run_start = null;
        }
    }
    if (run_start) |start| {
        if (expanded_token_ids.len - start != config.mm_tokens_per_image) return error.ImagePlaceholderCountMismatch;
        if (image_idx >= image_count) return error.ImagePlaceholderCountMismatch;
        image_offsets[image_idx] = start;
        image_idx += 1;
    }
    if (soft_token_count != image_count * config.mm_tokens_per_image or image_idx != image_count) {
        return error.ImagePlaceholderCountMismatch;
    }

    const pixels_per_image = 3 * pre_cfg.image_size * pre_cfg.image_size;
    const pixel_values = try allocator.alloc(f32, images.len * pixels_per_image);
    defer allocator.free(pixel_values);
    for (images, 0..) |image_bytes, idx| {
        const processed = try preprocessImage(allocator, image_bytes, pre_cfg);
        defer allocator.free(processed);
        @memcpy(pixel_values[idx * pixels_per_image ..][0..pixels_per_image], processed);
    }

    const projected = try gemma3_projector.encodeProjectedImageTokens(cb, allocator, projector_path, config, pixel_values, images.len);
    defer allocator.free(projected);

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
    const tokens_per_image: usize = config.mm_tokens_per_image;
    for (image_offsets, 0..) |offset, idx| {
        const dst = offset * hidden_size;
        const src = idx * tokens_per_image * hidden_size;
        @memcpy(
            prompt_embeddings[dst..][0 .. tokens_per_image * hidden_size],
            projected[src..][0 .. tokens_per_image * hidden_size],
        );
    }

    const embedding_shape = [_]i32{ @intCast(token_ids.len), @intCast(hidden_size) };
    const input_embeddings = try cb.fromFloat32Shape(prompt_embeddings, &embedding_shape);
    errdefer cb.free(input_embeddings);
    const attn_or_mask = try buildImageAttentionOrMask(allocator, token_ids.len, image_offsets, config.mm_tokens_per_image);
    return .{
        .allocator = allocator,
        .token_ids = token_ids,
        .input_embeddings = input_embeddings,
        .attn_or_mask = attn_or_mask,
    };
}

fn buildExpandedImageSequence(allocator: std.mem.Allocator, config: gpt_mod.Config) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "\n\n<start_of_image>");
    for (0..config.mm_tokens_per_image) |_| {
        try out.appendSlice(allocator, "<image_soft_token>");
    }
    try out.appendSlice(allocator, "<end_of_image>\n\n");
    return try out.toOwnedSlice(allocator);
}

fn buildImageAttentionOrMask(
    allocator: std.mem.Allocator,
    token_count: usize,
    image_offsets: []const usize,
    tokens_per_image: usize,
) ![]u8 {
    const total = try std.math.mul(usize, token_count, token_count);
    const mask = try allocator.alloc(u8, total);
    @memset(mask, 0);
    for (image_offsets) |offset| {
        if (offset == 0) return error.ImagePlaceholderCountMismatch;
        const segment_start = offset - 1; // <start_of_image>
        const segment_len = tokens_per_image + 2; // boi + soft tokens + eoi
        if (segment_start + segment_len > token_count) return error.ImagePlaceholderCountMismatch;
        for (0..segment_len) |qi| {
            const query_index = segment_start + qi;
            for (0..segment_len) |ki| {
                const key_index = segment_start + ki;
                mask[query_index * token_count + key_index] = 1;
            }
        }
    }
    return mask;
}

pub fn buildImageAttentionOrMaskFromExpandedTokens(
    allocator: std.mem.Allocator,
    token_ids: []const i64,
    config: gpt_mod.Config,
) !?[]u8 {
    if (!config.isMultimodal()) return null;
    var image_offsets = std.ArrayListUnmanaged(usize).empty;
    defer image_offsets.deinit(allocator);

    var run_start: ?usize = null;
    for (token_ids, 0..) |token_id, idx| {
        if (token_id == config.image_token_index) {
            if (run_start == null) run_start = idx;
            continue;
        }
        if (run_start) |start| {
            if (idx - start != config.mm_tokens_per_image) return error.ImagePlaceholderCountMismatch;
            if (start == 0) return error.ImagePlaceholderCountMismatch;
            if (token_ids[start - 1] != config.boi_token_index) return error.ImagePlaceholderCountMismatch;
            if (token_id != config.eoi_token_index) return error.ImagePlaceholderCountMismatch;
            try image_offsets.append(allocator, start);
            run_start = null;
        }
    }
    if (run_start != null) return error.ImagePlaceholderCountMismatch;
    if (image_offsets.items.len == 0) return null;
    return try buildImageAttentionOrMask(allocator, token_ids.len, image_offsets.items, config.mm_tokens_per_image);
}

fn parseRgbTriple(value: std.json.Value) ?[3]f32 {
    if (value != .array or value.array.items.len != 3) return null;
    var out: [3]f32 = undefined;
    for (value.array.items, 0..) |item, i| {
        out[i] = jsonF32(item) orelse return null;
    }
    return out;
}

fn jsonU32(val: std.json.Value) ?u32 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

fn jsonF32(val: std.json.Value) ?f32 {
    return switch (val) {
        .float => |f| @floatCast(f),
        .integer => |i| @floatFromInt(i),
        else => null,
    };
}

fn jsonBool(val: std.json.Value) ?bool {
    return switch (val) {
        .bool => |b| b,
        else => null,
    };
}

fn getEmbeddingWeight(cb: *const ComputeBackend, config: gpt_mod.Config) !ops.CT {
    return switch (config.family) {
        .gpt2 => cb.getWeight("wte.weight"),
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .phi => cb.getWeight("model.embed_tokens.weight"),
        else => cb.getWeight("model.embed_tokens.weight") catch try cb.getWeight("wte.weight"),
    };
}

test "parse gemma3 preprocessor config" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "do_normalize": true,
        \\  "do_rescale": true,
        \\  "do_resize": true,
        \\  "image_mean": [0.5, 0.5, 0.5],
        \\  "image_std": [0.5, 0.5, 0.5],
        \\  "image_seq_length": 256,
        \\  "rescale_factor": 0.00392156862745098,
        \\  "size": { "height": 896, "width": 896 }
        \\}
    ;
    const config = try parsePreprocessorConfig(allocator, json);
    try std.testing.expectEqual(@as(u32, 896), config.image_size);
    try std.testing.expectEqual(@as(u32, 256), config.image_seq_length);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), config.image_mean[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0 / 255.0), config.rescale_factor, 1e-9);
}

test "gemma3 preprocess image returns expected tensor size" {
    const allocator = std.testing.allocator;
    const png_1x1 = [_]u8{
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
        0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
        0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41,
        0x54, 0x78, 0xda, 0x63, 0xfc, 0xff, 0x9f, 0xa1,
        0x1e, 0x00, 0x07, 0x82, 0x02, 0x7f, 0x3d, 0xc8,
        0x48, 0xef, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45,
        0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
    };
    const config = PreprocessorConfig{
        .image_size = 4,
        .image_mean = .{ 0.5, 0.5, 0.5 },
        .image_std = .{ 0.5, 0.5, 0.5 },
    };
    const tensor = try preprocessImage(allocator, &png_1x1, config);
    defer allocator.free(tensor);
    try std.testing.expectEqual(@as(usize, 3 * 4 * 4), tensor.len);
}

test "gemma3 preprocess image keeps normalized pixel range" {
    const allocator = std.testing.allocator;
    const png_1x1 = [_]u8{
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
        0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
        0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41,
        0x54, 0x78, 0xda, 0x63, 0xfc, 0xff, 0x9f, 0xa1,
        0x1e, 0x00, 0x07, 0x82, 0x02, 0x7f, 0x3d, 0xc8,
        0x48, 0xef, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45,
        0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
    };
    const config = PreprocessorConfig{
        .image_size = 1,
        .image_mean = .{ 0.5, 0.5, 0.5 },
        .image_std = .{ 0.5, 0.5, 0.5 },
        .rescale_factor = 1.0 / 255.0,
    };
    const tensor = try preprocessImage(allocator, &png_1x1, config);
    defer allocator.free(tensor);
    const expected = try image.preprocess(allocator, &png_1x1, config.image_size, config.image_mean, config.image_std);
    defer allocator.free(expected);
    try std.testing.expectEqual(expected.len, tensor.len);
    for (tensor, expected) |actual, want| {
        try std.testing.expectApproxEqAbs(want, actual, 1e-6);
    }
}

test "expand prompt tokens replaces image placeholder with boi soft tokens and eoi" {
    const allocator = std.testing.allocator;
    const cfg = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 8,
        .image_token_index = 42,
        .boi_token_index = 7,
        .eoi_token_index = 8,
        .mm_tokens_per_image = 4,
        .vision_image_size = 896,
    };
    const prompt_ids = [_]i32{ 1, 7, 2 };
    var expanded = try expandPromptTokens(allocator, &prompt_ids, cfg, 1);
    defer expanded.deinit();

    try std.testing.expectEqualSlices(i64, &.{ 1, 7, 42, 42, 42, 42, 8, 2 }, expanded.token_ids);
    try std.testing.expectEqual(@as(usize, 1), expanded.image_offsets.len);
    try std.testing.expectEqual(@as(usize, 2), expanded.image_offsets[0]);
}

test "expand prompt text matches hf-style image sequence framing" {
    const allocator = std.testing.allocator;
    const cfg = gpt_mod.Config{
        .family = .gemma,
        .image_token_index = 42,
        .boi_token_index = 7,
        .eoi_token_index = 8,
        .mm_tokens_per_image = 3,
        .vision_image_size = 896,
    };
    const expanded = try expandPromptText(allocator, "<bos><start_of_turn>user\n<start_of_image>Describe this image.<end_of_turn>\n", cfg, 1);
    defer allocator.free(expanded);
    try std.testing.expectEqualStrings(
        "<bos><start_of_turn>user\n\n\n<start_of_image><image_soft_token><image_soft_token><image_soft_token><end_of_image>\n\nDescribe this image.<end_of_turn>\n",
        expanded,
    );
}

test "build image attention mask enables whole image segment block" {
    const allocator = std.testing.allocator;
    const mask = try buildImageAttentionOrMask(allocator, 8, &.{2}, 3);
    defer allocator.free(mask);
    try std.testing.expectEqual(@as(u8, 1), mask[2 * 8 + 4]);
    try std.testing.expectEqual(@as(u8, 1), mask[4 * 8 + 2]);
    try std.testing.expectEqual(@as(u8, 1), mask[1 * 8 + 5]);
    try std.testing.expectEqual(@as(u8, 0), mask[0 * 8 + 2]);
    try std.testing.expectEqual(@as(u8, 0), mask[2 * 8 + 6]);
}
