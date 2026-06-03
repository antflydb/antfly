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
const audio = @import("../pipelines/audio.zig");
const image = @import("../pipelines/image.zig");
const ops = @import("../ops/ops.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_format = @import("../gguf/format.zig");
const tensor_store_mod = @import("../models/tensor_store.zig");
const weight_source_mod = @import("../models/weight_source.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const activations = @import("../backends/activations.zig");
const projector_format_mod = @import("projector_format.zig");

const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;

const default_spatial_merge_size: usize = 3;
const default_max_image_tokens: usize = 280;

pub const ProjectedImages = struct {
    allocator: std.mem.Allocator,
    embeddings: []f32,
    tokens_per_image: []usize,
    hidden_size: usize,

    pub fn deinit(self: *ProjectedImages) void {
        self.allocator.free(self.embeddings);
        self.allocator.free(self.tokens_per_image);
    }
};

pub const ProjectedAudio = struct {
    allocator: std.mem.Allocator,
    embeddings: []f32,
    tokens_per_audio: []usize,
    hidden_size: usize,

    pub fn deinit(self: *ProjectedAudio) void {
        self.allocator.free(self.embeddings);
        self.allocator.free(self.tokens_per_audio);
    }
};

const Config = struct {
    text_hidden: usize,
    vision_hidden: usize,
    intermediate_size: usize,
    block_count: usize,
    head_count: usize,
    image_size: usize,
    patch_size: usize,
    layer_norm_eps: f32,
    rope_theta: f32 = 100.0,
    image_mean: [3]f32,
    image_std: [3]f32,
    spatial_merge_size: usize = default_spatial_merge_size,
    max_image_tokens: usize = default_max_image_tokens,

    fn maxPatchCount(self: Config) usize {
        return self.max_image_tokens * self.spatial_merge_size * self.spatial_merge_size;
    }
};

const AudioConfig = struct {
    text_hidden: usize,
    audio_hidden: usize,
    output_hidden: usize,
    intermediate_size: usize,
    block_count: usize,
    head_count: usize,
    mel_bins: usize,
    layer_norm_eps: f32,
    conv_channels0: usize = 128,
    conv_channels1: usize = 32,
    conv_kernel_size: usize = 5,
    residual_weight: f32 = 0.5,
    attention_chunk_size: usize = 12,
    attention_context_left: usize = 13,
    attention_context_right: usize = 0,
    attention_logit_cap: f32 = 50.0,
    attention_invalid_logits_value: f32 = -1.0e9,
    gradient_clipping: f32 = 1.0e10,

    fn headDim(self: AudioConfig) usize {
        return self.audio_hidden / self.head_count;
    }

    fn attentionContextSize(self: AudioConfig) usize {
        return self.attention_chunk_size + self.attention_context_left - 1 + self.attention_context_right;
    }
};

const Geometry = struct {
    width: usize,
    height: usize,
    grid_x: usize,
    grid_y: usize,
    pooled_x: usize,
    pooled_y: usize,

    fn tokenCount(self: Geometry) usize {
        return self.pooled_x * self.pooled_y;
    }
};

const LoadedF32 = struct {
    weight: weight_source_mod.LoadedWeight,
    converted: ?Tensor = null,
    data: []const f32,
    shape: []const i64,

    fn deinit(self: *LoadedF32) void {
        if (self.converted) |*converted| converted.deinit();
        self.weight.deinit();
    }
};

pub fn isSupportedImageProjectorPath(allocator: std.mem.Allocator, projector_path: []const u8) !bool {
    return switch (try projector_format_mod.detectPath(allocator, projector_path)) {
        .clip_gemma4_image, .clip_gemma4_image_audio => true,
        else => false,
    };
}

pub fn isSupportedImageProjectorFile(file: *const gguf_format.File) bool {
    return switch (projector_format_mod.detectFile(file)) {
        .clip_gemma4_image, .clip_gemma4_image_audio => true,
        else => false,
    };
}

/// Run Gemma 4's external GGUF vision projector.
///
/// This supports the `gemma4v` image projector path. Gemma 4 audio projectors use
/// separate `a.*` tensors and are intentionally not routed through this function.
pub fn encodeProjectedImages(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    projector_path: []const u8,
    images: []const []const u8,
) !ProjectedImages {
    var store = try tensor_store_mod.GgufStore.initAbsolute(allocator, projector_path);
    defer store.tensorStore().deinit();

    return encodeProjectedImagesFromStore(cb, allocator, store, images);
}

pub fn encodeProjectedImagesFromStore(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    images: []const []const u8,
) !ProjectedImages {
    const cfg = try parseConfig(&store.parsed);

    var all_embeddings = std.ArrayListUnmanaged(f32).empty;
    errdefer all_embeddings.deinit(allocator);
    var tokens_per_image = std.ArrayListUnmanaged(usize).empty;
    errdefer tokens_per_image.deinit(allocator);

    for (images) |image_bytes| {
        const encoded = try encodeSingleImage(cb, allocator, store, cfg, image_bytes);
        defer allocator.free(encoded.embeddings);
        try all_embeddings.appendSlice(allocator, encoded.embeddings);
        try tokens_per_image.append(allocator, encoded.tokens);
    }

    return .{
        .allocator = allocator,
        .embeddings = try all_embeddings.toOwnedSlice(allocator),
        .tokens_per_image = try tokens_per_image.toOwnedSlice(allocator),
        .hidden_size = cfg.text_hidden,
    };
}

pub fn encodeProjectedAudio(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    projector_path: []const u8,
    audio_clips: []const []const u8,
) !ProjectedAudio {
    var store = try tensor_store_mod.GgufStore.initAbsolute(allocator, projector_path);
    defer store.tensorStore().deinit();

    return encodeProjectedAudioFromStore(cb, allocator, store, audio_clips);
}

pub fn encodeProjectedAudioFromStore(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    audio_clips: []const []const u8,
) !ProjectedAudio {
    const cfg = try parseAudioConfig(&store.parsed);

    var all_embeddings = std.ArrayListUnmanaged(f32).empty;
    errdefer all_embeddings.deinit(allocator);
    var tokens_per_audio = std.ArrayListUnmanaged(usize).empty;
    errdefer tokens_per_audio.deinit(allocator);

    for (audio_clips) |audio_bytes| {
        const encoded = try encodeSingleAudio(cb, allocator, store, cfg, audio_bytes);
        defer allocator.free(encoded.embeddings);
        try all_embeddings.appendSlice(allocator, encoded.embeddings);
        try tokens_per_audio.append(allocator, encoded.tokens);
    }

    return .{
        .allocator = allocator,
        .embeddings = try all_embeddings.toOwnedSlice(allocator),
        .tokens_per_audio = try tokens_per_audio.toOwnedSlice(allocator),
        .hidden_size = cfg.text_hidden,
    };
}

const EncodedImage = struct {
    embeddings: []f32,
    tokens: usize,
};

const EncodedAudio = struct {
    embeddings: []f32,
    tokens: usize,
};

fn encodeSingleAudio(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    cfg: AudioConfig,
    audio_bytes: []const u8,
) !EncodedAudio {
    var features = try prepareGemma4AudioFeatures(allocator, audio_bytes, cfg.mel_bins);
    defer features.deinit();

    var subsampled = try audioSubsample(cb, allocator, store, cfg, &features);
    defer subsampled.deinit(allocator);

    var hidden = subsampled.hidden;
    errdefer cb.free(hidden);
    const positions = try audioRelativePositionEmbeddings(allocator, cfg);
    defer allocator.free(positions);

    for (0..cfg.block_count) |layer| {
        const next = try audioLayer(cb, allocator, store, cfg, hidden, subsampled.valid_mask, positions, layer);
        cb.free(hidden);
        hidden = next;
    }

    const output = try audioLinearWithBias(cb, allocator, store, hidden, "a.pre_encode.out", subsampled.seq_len, cfg.audio_hidden, cfg.output_hidden);
    cb.free(hidden);
    defer cb.free(output);

    const normed = try rmsNormNoScaleCt(cb, allocator, output, subsampled.seq_len, cfg.output_hidden, cfg.layer_norm_eps);
    defer cb.free(normed);
    const projection_w = try loadLinearWeightCt(cb, allocator, store, "mm.a.input_projection.weight", cfg.output_hidden, cfg.text_hidden);
    defer cb.free(projection_w);
    const projected = try cb.linearNoBias(normed, projection_w, subsampled.seq_len, cfg.output_hidden, cfg.text_hidden);
    defer cb.free(projected);

    const projected_data = try cb.toFloat32(projected, allocator);
    defer allocator.free(projected_data);
    const valid_count = countTrue(subsampled.valid_mask);
    const embeddings = try allocator.alloc(f32, valid_count * cfg.text_hidden);
    var dst_token: usize = 0;
    for (subsampled.valid_mask, 0..) |valid, token| {
        if (!valid) continue;
        @memcpy(
            embeddings[dst_token * cfg.text_hidden ..][0..cfg.text_hidden],
            projected_data[token * cfg.text_hidden ..][0..cfg.text_hidden],
        );
        dst_token += 1;
    }

    return .{
        .embeddings = embeddings,
        .tokens = valid_count,
    };
}

const AudioFeatures = struct {
    allocator: std.mem.Allocator,
    data: []f32,
    mask: []bool,
    frames: usize,
    mel_bins: usize,

    fn deinit(self: *AudioFeatures) void {
        self.allocator.free(self.data);
        self.allocator.free(self.mask);
    }
};

const SubsampledAudio = struct {
    hidden: CT,
    valid_mask: []bool,
    seq_len: usize,

    fn deinit(self: *SubsampledAudio, allocator: std.mem.Allocator) void {
        allocator.free(self.valid_mask);
    }
};

fn prepareGemma4AudioFeatures(
    allocator: std.mem.Allocator,
    audio_bytes: []const u8,
    mel_bins: usize,
) !AudioFeatures {
    const target_rate: u32 = 16_000;
    const frame_length: usize = 320;
    const hop_length: usize = 160;
    const fft_length: usize = 512;
    const mel_floor: f32 = 1e-3;
    const max_samples: usize = 480_000;
    const pad_multiple: usize = 128;

    var decoded = try audio.decode(allocator, audio_bytes, .{});
    defer decoded.deinit();
    const resampled = try audio.copyOrResample(allocator, decoded.samples, decoded.sample_rate, target_rate);
    defer allocator.free(resampled);

    const real_samples: usize = @min(resampled.len, max_samples);
    var padded_samples_len: usize = real_samples;
    if (padded_samples_len % pad_multiple != 0) {
        padded_samples_len += pad_multiple - (padded_samples_len % pad_multiple);
    }
    const pad_left = frame_length / 2;
    const total_samples = padded_samples_len + pad_left;
    const frame_size_for_unfold = frame_length + 1;
    const frames = if (total_samples >= frame_size_for_unfold)
        (total_samples - frame_size_for_unfold) / hop_length + 1
    else
        0;

    const out = try allocator.alloc(f32, frames * mel_bins);
    errdefer allocator.free(out);
    const mask = try allocator.alloc(bool, frames);
    errdefer allocator.free(mask);
    if (frames == 0) {
        return .{ .allocator = allocator, .data = out, .mask = mask, .frames = frames, .mel_bins = mel_bins };
    }

    const samples = try allocator.alloc(f32, total_samples);
    defer allocator.free(samples);
    @memset(samples, 0.0);
    @memcpy(samples[pad_left..][0..real_samples], resampled[0..real_samples]);

    const filters = try gemma4MelFilterbank(allocator, mel_bins, fft_length, target_rate);
    defer allocator.free(filters);
    const window = try allocator.alloc(f32, frame_length);
    defer allocator.free(window);
    for (window, 0..) |*value, i| {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(frame_length));
        value.* = 0.5 - 0.5 * @cos(2.0 * std.math.pi * t);
    }
    const magnitudes = try allocator.alloc(f32, fft_length / 2 + 1);
    defer allocator.free(magnitudes);

    for (0..frames) |frame| {
        const start = frame * hop_length;
        const frame_end = start + frame_size_for_unfold - 1;
        mask[frame] = frame_end >= pad_left and frame_end < pad_left + real_samples;
        try rfftMagnitudeNaive(samples[start..][0..frame_length], window, magnitudes, fft_length);
        for (0..mel_bins) |m| {
            var sum: f32 = 0.0;
            const filter = filters[m * magnitudes.len ..][0..magnitudes.len];
            for (magnitudes, 0..) |mag, k| sum += mag * filter[k];
            out[frame * mel_bins + m] = if (mask[frame]) @log(sum + mel_floor) else 0.0;
        }
    }

    return .{ .allocator = allocator, .data = out, .mask = mask, .frames = frames, .mel_bins = mel_bins };
}

fn audioSubsample(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    cfg: AudioConfig,
    features: *const AudioFeatures,
) !SubsampledAudio {
    if (features.mel_bins != cfg.mel_bins) return error.InvalidTensorShape;

    var masked_features = try allocator.dupe(f32, features.data);
    defer allocator.free(masked_features);
    for (features.mask, 0..) |valid, frame| {
        if (valid) continue;
        @memset(masked_features[frame * features.mel_bins ..][0..features.mel_bins], 0.0);
    }

    var shape = [_]i32{ 1, 1, @intCast(features.frames), @intCast(features.mel_bins) };
    var hidden = try cb.fromFloat32Shape(masked_features, &shape);
    errdefer cb.free(hidden);
    var mask = try allocator.dupe(bool, features.mask);
    errdefer allocator.free(mask);
    var height = features.frames;
    var width = features.mel_bins;
    var channels: usize = 1;

    const layer_channels = [_]usize{ cfg.conv_channels0, cfg.conv_channels1 };
    for (layer_channels, 0..) |out_channels, layer| {
        var weight_buf: [128]u8 = undefined;
        var norm_buf: [128]u8 = undefined;
        const weight = try loadConv2dWeightCt(cb, allocator, store, try fmt(&weight_buf, "a.conv1d.{d}.weight", .{layer}), 3, 3, channels, out_channels);
        defer cb.free(weight);
        const zero_bias = try allocator.alloc(f32, out_channels);
        defer allocator.free(zero_bias);
        @memset(zero_bias, 0.0);
        const bias_shape = [_]i32{@intCast(out_channels)};
        const bias = try cb.fromFloat32Shape(zero_bias, &bias_shape);
        defer cb.free(bias);

        const conv = try cb.conv2d(hidden, weight, bias, 1, channels, out_channels, height, width, 3, 3, 2, 2, 1, 1, 1);
        cb.free(hidden);
        hidden = conv;
        const out_h = (height + 2 - 3) / 2 + 1;
        const out_w = (width + 2 - 3) / 2 + 1;

        const conv_data = try cb.toFloat32(hidden, allocator);
        cb.free(hidden);
        defer allocator.free(conv_data);
        var norm = try loadTensorF32(store, try fmt(&norm_buf, "a.conv1d.{d}.norm.weight", .{layer}));
        defer norm.deinit();
        try layerNormChannelsRelu(conv_data, 1, out_channels, out_h, out_w, norm.data, cfg.layer_norm_eps);
        shape = [_]i32{ 1, @intCast(out_channels), @intCast(out_h), @intCast(out_w) };
        hidden = try cb.fromFloat32Shape(conv_data, &shape);
        errdefer cb.free(hidden);

        const next_mask = try subsampleMaskEveryOther(allocator, mask, out_h);
        allocator.free(mask);
        mask = next_mask;
        height = out_h;
        width = out_w;
        channels = out_channels;
    }

    const flattened = try flattenAudioConvOutput(cb, allocator, hidden, height, width, channels);
    cb.free(hidden);
    hidden = flattened;

    const projected = try linearNoBiasMaybeClipped(cb, allocator, store, hidden, "a.input_projection", height, width * channels, cfg.audio_hidden);
    cb.free(hidden);

    return .{
        .hidden = projected,
        .valid_mask = mask,
        .seq_len = height,
    };
}

fn flattenAudioConvOutput(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: CT,
    time_steps: usize,
    freq_bins: usize,
    channels: usize,
) !CT {
    const data = try cb.toFloat32(hidden, allocator);
    defer allocator.free(data);
    const flattened = try allocator.alloc(f32, time_steps * freq_bins * channels);
    defer allocator.free(flattened);
    for (0..time_steps) |t| {
        for (0..freq_bins) |f| {
            for (0..channels) |c| {
                flattened[(t * freq_bins + f) * channels + c] = data[(c * time_steps + t) * freq_bins + f];
            }
        }
    }
    const shape = [_]i32{ @intCast(time_steps), @intCast(freq_bins * channels) };
    return cb.fromFloat32Shape(flattened, &shape);
}

fn encodeSingleImage(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    cfg: Config,
    image_bytes: []const u8,
) !EncodedImage {
    const decoded = try image.decode(allocator, image_bytes);
    defer decoded.deinit(allocator);

    const geometry = targetGeometry(cfg, decoded.width, decoded.height);
    const pixel_values = try image.preprocessDecodedRectScaledWithResample(
        allocator,
        decoded,
        @intCast(geometry.width),
        @intCast(geometry.height),
        cfg.image_mean,
        cfg.image_std,
        1.0 / 255.0,
        .bilinear,
    );
    defer allocator.free(pixel_values);

    const patches = try patchEmbed(cb, allocator, store, cfg, pixel_values, geometry);
    defer allocator.free(patches);

    const positioned = try addPositionEmbeddings(allocator, store, cfg, patches, geometry);
    defer allocator.free(positioned);

    const hidden_shape = [_]i32{ @intCast(geometry.grid_x * geometry.grid_y), @intCast(cfg.vision_hidden) };
    var hidden = try cb.fromFloat32Shape(positioned, &hidden_shape);
    errdefer cb.free(hidden);

    for (0..cfg.block_count) |layer| {
        const next = try encoderBlock(cb, allocator, store, cfg, hidden, geometry, layer);
        cb.free(hidden);
        hidden = next;
    }

    const hidden_data = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    defer allocator.free(hidden_data);

    const pooled = try averagePoolSpatial(allocator, hidden_data, cfg, geometry);
    defer allocator.free(pooled);
    try applyOptionalStandardization(allocator, store, pooled, cfg);

    const pooled_shape = [_]i32{ @intCast(geometry.tokenCount()), @intCast(cfg.vision_hidden) };
    const pooled_ct = try cb.fromFloat32Shape(pooled, &pooled_shape);
    defer cb.free(pooled_ct);
    const normed_pooled = try rmsNormNoScaleCt(cb, allocator, pooled_ct, geometry.tokenCount(), cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(normed_pooled);

    const projection_w = try loadLinearWeightCt(cb, allocator, store, "mm.input_projection.weight", cfg.vision_hidden, cfg.text_hidden);
    defer cb.free(projection_w);
    const projected = try cb.linearNoBias(normed_pooled, projection_w, geometry.tokenCount(), cfg.vision_hidden, cfg.text_hidden);
    defer cb.free(projected);

    return .{
        .embeddings = try cb.toFloat32(projected, allocator),
        .tokens = geometry.tokenCount(),
    };
}

fn parseConfig(file: *const gguf_format.File) !Config {
    const view = gguf_metadata.View.init(file);
    const arch = view.getString("general.architecture") orelse return error.InvalidGgufProjector;
    if (!std.mem.eql(u8, arch, "clip")) return error.InvalidGgufProjector;
    const projector_type = view.getString("clip.vision.projector_type") orelse return error.InvalidGgufProjector;
    if (!std.mem.eql(u8, projector_type, "gemma4v")) return error.UnsupportedGgufProjector;

    var image_mean = [3]f32{ 0.0, 0.0, 0.0 };
    var image_std = [3]f32{ 1.0, 1.0, 1.0 };
    if (metadataF32Triple(view, "clip.vision.image_mean")) |mean| {
        if (metadataF32Triple(view, "clip.vision.image_std")) |std_dev| {
            image_mean = mean;
            image_std = std_dev;
        }
    }

    return .{
        .text_hidden = @intCast(view.getU64("clip.vision.projection_dim") orelse return error.InvalidGgufProjector),
        .vision_hidden = @intCast(view.getU64("clip.vision.embedding_length") orelse return error.InvalidGgufProjector),
        .intermediate_size = @intCast(view.getU64("clip.vision.feed_forward_length") orelse return error.InvalidGgufProjector),
        .block_count = @intCast(view.getU64("clip.vision.block_count") orelse return error.InvalidGgufProjector),
        .head_count = @intCast(view.getU64("clip.vision.attention.head_count") orelse return error.InvalidGgufProjector),
        .image_size = @intCast(view.getU64("clip.vision.image_size") orelse return error.InvalidGgufProjector),
        .patch_size = @intCast(view.getU64("clip.vision.patch_size") orelse return error.InvalidGgufProjector),
        .layer_norm_eps = view.getF32("clip.vision.attention.layer_norm_epsilon") orelse 1e-6,
        .image_mean = image_mean,
        .image_std = image_std,
    };
}

fn audioLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    cfg: AudioConfig,
    input: CT,
    valid_mask: []const bool,
    position_embeddings: []const f32,
    layer: usize,
) !CT {
    const ff1 = try audioFeedForward(cb, allocator, store, cfg, input, valid_mask.len, layer, false);
    defer cb.free(ff1);

    var buf: [128]u8 = undefined;
    const attn_pre = try loadWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.attn_pre_norm.weight", .{layer}));
    defer cb.free(attn_pre);
    const normed = try cb.rmsNorm(ff1, attn_pre, cfg.audio_hidden, cfg.layer_norm_eps);
    defer cb.free(normed);
    const attn = try audioSelfAttention(cb, allocator, store, cfg, normed, valid_mask, position_embeddings, layer);
    defer cb.free(attn);
    const attn_post = try loadWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.attn_post_norm.weight", .{layer}));
    defer cb.free(attn_post);
    const attn_normed = try cb.rmsNorm(attn, attn_post, cfg.audio_hidden, cfg.layer_norm_eps);
    defer cb.free(attn_normed);
    const res_attn = try cb.add(ff1, attn_normed);
    defer cb.free(res_attn);

    const lconv = try audioLightConv(cb, allocator, store, cfg, res_attn, valid_mask.len, layer);
    defer cb.free(lconv);
    const ff2 = try audioFeedForward(cb, allocator, store, cfg, lconv, valid_mask.len, layer, true);
    defer cb.free(ff2);
    const out_norm = try loadWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.ln2.weight", .{layer}));
    defer cb.free(out_norm);
    return cb.rmsNorm(ff2, out_norm, cfg.audio_hidden, cfg.layer_norm_eps);
}

fn audioFeedForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    cfg: AudioConfig,
    input: CT,
    rows: usize,
    layer: usize,
    second: bool,
) !CT {
    var buf: [128]u8 = undefined;
    const suffix = if (second) "_1" else "";
    const norm_w = try loadWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.ffn_norm{s}.weight", .{ layer, suffix }));
    defer cb.free(norm_w);
    const normed = try cb.rmsNorm(input, norm_w, cfg.audio_hidden, cfg.layer_norm_eps);
    defer cb.free(normed);
    const up = try linearNoBiasMaybeClipped(cb, allocator, store, normed, try fmt(&buf, "a.blk.{d}.ffn_up{s}", .{ layer, suffix }), rows, cfg.audio_hidden, cfg.intermediate_size);
    defer cb.free(up);
    const activated = try cb.silu(up);
    defer cb.free(activated);
    const down = try linearNoBiasMaybeClipped(cb, allocator, store, activated, try fmt(&buf, "a.blk.{d}.ffn_down{s}", .{ layer, suffix }), rows, cfg.intermediate_size, cfg.audio_hidden);
    defer cb.free(down);
    const post_w = try loadWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.ffn_post_norm{s}.weight", .{ layer, suffix }));
    defer cb.free(post_w);
    const post = try cb.rmsNorm(down, post_w, cfg.audio_hidden, cfg.layer_norm_eps);
    defer cb.free(post);
    const post_data = try cb.toFloat32(post, allocator);
    defer allocator.free(post_data);
    for (post_data) |*value| value.* *= cfg.residual_weight;
    const shape = [_]i32{ @intCast(rows), @intCast(cfg.audio_hidden) };
    const scaled = try cb.fromFloat32Shape(post_data, &shape);
    defer cb.free(scaled);
    return cb.add(input, scaled);
}

fn audioLightConv(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    cfg: AudioConfig,
    input: CT,
    rows: usize,
    layer: usize,
) !CT {
    var buf: [128]u8 = undefined;
    const pre_w = try loadWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.norm_conv.weight", .{layer}));
    defer cb.free(pre_w);
    const normed = try cb.rmsNorm(input, pre_w, cfg.audio_hidden, cfg.layer_norm_eps);
    defer cb.free(normed);
    const pw1 = try linearNoBiasMaybeClipped(cb, allocator, store, normed, try fmt(&buf, "a.blk.{d}.conv_pw1", .{layer}), rows, cfg.audio_hidden, cfg.audio_hidden * 2);
    defer cb.free(pw1);
    const glu = try applyGlu(cb, allocator, pw1, rows, cfg.audio_hidden);
    defer cb.free(glu);
    const conv = try depthwiseCausalConv1d(cb, allocator, store, glu, rows, cfg.audio_hidden, cfg.conv_kernel_size, try fmt(&buf, "a.blk.{d}.conv_dw.weight", .{layer}));
    defer cb.free(conv);
    const conv_norm_w = try loadWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.conv_norm.weight", .{layer}));
    defer cb.free(conv_norm_w);
    const conv_normed = try cb.rmsNorm(conv, conv_norm_w, cfg.audio_hidden, cfg.layer_norm_eps);
    defer cb.free(conv_normed);
    const activated = try cb.silu(conv_normed);
    defer cb.free(activated);
    const pw2 = try linearNoBiasMaybeClipped(cb, allocator, store, activated, try fmt(&buf, "a.blk.{d}.conv_pw2", .{layer}), rows, cfg.audio_hidden, cfg.audio_hidden);
    defer cb.free(pw2);
    return cb.add(input, pw2);
}

fn audioSelfAttention(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    cfg: AudioConfig,
    input: CT,
    valid_mask: []const bool,
    position_embeddings: []const f32,
    layer: usize,
) !CT {
    const rows = valid_mask.len;
    var buf: [128]u8 = undefined;
    const q = try linearNoBiasMaybeClipped(cb, allocator, store, input, try fmt(&buf, "a.blk.{d}.attn_q", .{layer}), rows, cfg.audio_hidden, cfg.audio_hidden);
    defer cb.free(q);
    const k = try linearNoBiasMaybeClipped(cb, allocator, store, input, try fmt(&buf, "a.blk.{d}.attn_k", .{layer}), rows, cfg.audio_hidden, cfg.audio_hidden);
    defer cb.free(k);
    const v = try linearNoBiasMaybeClipped(cb, allocator, store, input, try fmt(&buf, "a.blk.{d}.attn_v", .{layer}), rows, cfg.audio_hidden, cfg.audio_hidden);
    defer cb.free(v);

    const rel_shape = [_]i32{ @intCast(cfg.attention_context_left), @intCast(cfg.audio_hidden) };
    const rel_in = try cb.fromFloat32Shape(position_embeddings, &rel_shape);
    defer cb.free(rel_in);
    const rel = try linearNoBiasMaybeClipped(cb, allocator, store, rel_in, try fmt(&buf, "a.blk.{d}.attn_k_rel", .{layer}), cfg.attention_context_left, cfg.audio_hidden, cfg.audio_hidden);
    defer cb.free(rel);

    const q_data = try cb.toFloat32(q, allocator);
    defer allocator.free(q_data);
    const k_data = try cb.toFloat32(k, allocator);
    defer allocator.free(k_data);
    const v_data = try cb.toFloat32(v, allocator);
    defer allocator.free(v_data);
    const rel_data = try cb.toFloat32(rel, allocator);
    defer allocator.free(rel_data);
    var per_dim = try loadTensorF32(store, try fmt(&buf, "a.blk.{d}.per_dim_scale.weight", .{layer}));
    defer per_dim.deinit();

    const out = try audioLocalAttentionCpu(allocator, cfg, q_data, k_data, v_data, rel_data, per_dim.data, valid_mask);
    defer allocator.free(out);
    const out_shape = [_]i32{ @intCast(rows), @intCast(cfg.audio_hidden) };
    const out_ct = try cb.fromFloat32Shape(out, &out_shape);
    defer cb.free(out_ct);
    return linearNoBiasMaybeClipped(cb, allocator, store, out_ct, try fmt(&buf, "a.blk.{d}.attn_out", .{layer}), rows, cfg.audio_hidden, cfg.audio_hidden);
}

fn rfftMagnitudeNaive(frame: []const f32, window: []const f32, out: []f32, fft_len: usize) !void {
    if (frame.len != window.len or out.len != fft_len / 2 + 1) return error.InvalidTensorShape;
    const fft_len_f: f32 = @floatFromInt(fft_len);
    for (0..out.len) |k| {
        const k_f: f32 = @floatFromInt(k);
        var re: f32 = 0.0;
        var im: f32 = 0.0;
        for (frame, 0..) |sample, n| {
            const angle = -2.0 * std.math.pi * k_f * @as(f32, @floatFromInt(n)) / fft_len_f;
            const value = sample * window[n];
            re += value * @cos(angle);
            im += value * @sin(angle);
        }
        out[k] = @sqrt(re * re + im * im);
    }
}

fn gemma4MelFilterbank(allocator: std.mem.Allocator, n_mels: usize, n_fft: usize, sample_rate: u32) ![]f32 {
    const n_freq = n_fft / 2 + 1;
    const filters = try allocator.alloc(f32, n_mels * n_freq);
    @memset(filters, 0.0);

    const sr_f: f32 = @floatFromInt(sample_rate);
    const fft_f: f32 = @floatFromInt(n_fft);
    const mel_low = htkHzToMel(0.0);
    const mel_high = htkHzToMel(sr_f / 2.0);
    const points = n_mels + 2;
    const hz_points = try allocator.alloc(f32, points);
    defer allocator.free(hz_points);
    for (0..points) |i| {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(points - 1));
        hz_points[i] = htkMelToHz(mel_low + t * (mel_high - mel_low));
    }

    for (0..n_mels) |m| {
        const left = hz_points[m] * fft_f / sr_f;
        const center = hz_points[m + 1] * fft_f / sr_f;
        const right = hz_points[m + 2] * fft_f / sr_f;
        for (0..n_freq) |k| {
            const bin: f32 = @floatFromInt(k);
            filters[m * n_freq + k] = if (bin >= left and bin < center and center > left)
                (bin - left) / (center - left)
            else if (bin >= center and bin < right and right > center)
                (right - bin) / (right - center)
            else
                0.0;
        }
    }
    return filters;
}

fn htkHzToMel(hz: f32) f32 {
    return 2595.0 * std.math.log10(1.0 + hz / 700.0);
}

fn htkMelToHz(mel: f32) f32 {
    return 700.0 * (std.math.pow(f32, 10.0, mel / 2595.0) - 1.0);
}

fn loadConv2dWeightCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    name: []const u8,
    kernel_h: usize,
    kernel_w: usize,
    in_channels: usize,
    out_channels: usize,
) !CT {
    var tensor = try loadTensorF32(store, name);
    defer tensor.deinit();
    if (tensor.shape.len != 4) return error.InvalidTensorShape;
    const d0: usize = @intCast(tensor.shape[0]);
    const d1: usize = @intCast(tensor.shape[1]);
    const d2: usize = @intCast(tensor.shape[2]);
    const d3: usize = @intCast(tensor.shape[3]);
    const out_shape = [_]i32{ @intCast(out_channels), @intCast(in_channels), @intCast(kernel_h), @intCast(kernel_w) };
    if (d0 == out_channels and d1 == in_channels and d2 == kernel_h and d3 == kernel_w) {
        return cb.fromFloat32Shape(tensor.data, &out_shape);
    }
    if (d0 == kernel_h and d1 == kernel_w and d2 == in_channels and d3 == out_channels) {
        const transposed = try allocator.alloc(f32, tensor.data.len);
        defer allocator.free(transposed);
        for (0..out_channels) |oc| {
            for (0..in_channels) |ic| {
                for (0..kernel_h) |ky| {
                    for (0..kernel_w) |kx| {
                        const src = (((ky * kernel_w + kx) * in_channels + ic) * out_channels) + oc;
                        const dst = (((oc * in_channels + ic) * kernel_h + ky) * kernel_w) + kx;
                        transposed[dst] = tensor.data[src];
                    }
                }
            }
        }
        return cb.fromFloat32Shape(transposed, &out_shape);
    }
    return error.InvalidTensorShape;
}

fn layerNormChannelsRelu(
    data: []f32,
    batch: usize,
    channels: usize,
    height: usize,
    width: usize,
    weight: []const f32,
    eps: f32,
) !void {
    if (data.len != batch * channels * height * width or weight.len != channels) return error.InvalidTensorShape;
    for (0..batch) |b| {
        for (0..height) |y| {
            for (0..width) |x| {
                var mean: f32 = 0.0;
                for (0..channels) |c| mean += data[((b * channels + c) * height + y) * width + x];
                mean /= @floatFromInt(channels);
                var variance: f32 = 0.0;
                for (0..channels) |c| {
                    const delta = data[((b * channels + c) * height + y) * width + x] - mean;
                    variance += delta * delta;
                }
                variance /= @floatFromInt(channels);
                const scale = 1.0 / @sqrt(variance + eps);
                for (0..channels) |c| {
                    const idx = ((b * channels + c) * height + y) * width + x;
                    data[idx] = @max((data[idx] - mean) * scale * weight[c], 0.0);
                }
            }
        }
    }
}

fn subsampleMaskEveryOther(allocator: std.mem.Allocator, mask: []const bool, out_len: usize) ![]bool {
    const out = try allocator.alloc(bool, out_len);
    for (out, 0..) |*valid, i| {
        const src = i * 2;
        valid.* = src < mask.len and mask[src];
    }
    return out;
}

fn audioRelativePositionEmbeddings(allocator: std.mem.Allocator, cfg: AudioConfig) ![]f32 {
    const num_timescales = cfg.audio_hidden / 2;
    if (num_timescales == 0) return error.InvalidTensorShape;
    const out = try allocator.alloc(f32, cfg.attention_context_left * cfg.audio_hidden);
    const denom = @max(num_timescales - 1, 1);
    const log_increment = @log(10000.0) / @as(f32, @floatFromInt(denom));
    for (0..cfg.attention_context_left) |pos_idx| {
        const position: f32 = @floatFromInt(cfg.attention_context_left - 1 - pos_idx);
        const base = pos_idx * cfg.audio_hidden;
        for (0..num_timescales) |i| {
            const inv_timescale = @exp(@as(f32, @floatFromInt(i)) * -log_increment);
            const scaled = position * inv_timescale;
            out[base + i] = @sin(scaled);
            out[base + num_timescales + i] = @cos(scaled);
        }
    }
    return out;
}

fn audioLinearWithBias(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    input: CT,
    prefix: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !CT {
    const linear = try linearNoBiasMaybeClipped(cb, allocator, store, input, prefix, rows, in_dim, out_dim);
    defer cb.free(linear);
    const data = try cb.toFloat32(linear, allocator);
    defer allocator.free(data);

    const bias_name = try std.fmt.allocPrint(allocator, "{s}.bias", .{prefix});
    defer allocator.free(bias_name);
    var bias = try loadTensorF32(store, bias_name);
    defer bias.deinit();
    if (bias.data.len != out_dim) return error.InvalidTensorShape;
    for (0..rows) |row| {
        for (0..out_dim) |col| data[row * out_dim + col] += bias.data[col];
    }
    const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
    return cb.fromFloat32Shape(data, &shape);
}

fn countTrue(mask: []const bool) usize {
    var count: usize = 0;
    for (mask) |valid| {
        if (valid) count += 1;
    }
    return count;
}

fn applyGlu(cb: *const ComputeBackend, allocator: std.mem.Allocator, input: CT, rows: usize, hidden: usize) !CT {
    const data = try cb.toFloat32(input, allocator);
    defer allocator.free(data);
    if (data.len != rows * hidden * 2) return error.InvalidTensorShape;
    const out = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(out);
    for (0..rows) |row| {
        const base = row * hidden * 2;
        for (0..hidden) |h| {
            out[row * hidden + h] = data[base + h] * sigmoid(data[base + hidden + h]);
        }
    }
    const shape = [_]i32{ @intCast(rows), @intCast(hidden) };
    return cb.fromFloat32Shape(out, &shape);
}

fn depthwiseCausalConv1d(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    input: CT,
    rows: usize,
    hidden: usize,
    kernel_size: usize,
    weight_name: []const u8,
) !CT {
    const input_data = try cb.toFloat32(input, allocator);
    defer allocator.free(input_data);
    if (input_data.len != rows * hidden) return error.InvalidTensorShape;
    var weight = try loadTensorF32(store, weight_name);
    defer weight.deinit();
    if (weight.data.len != kernel_size * hidden or weight.shape.len < 2) return error.InvalidTensorShape;
    const d0: usize = @intCast(weight.shape[0]);
    const d1: usize = @intCast(weight.shape[1]);
    const kernel_first = d0 == kernel_size and d1 == hidden;
    const hidden_first = d0 == hidden and d1 == kernel_size;
    if (!kernel_first and !hidden_first) return error.InvalidTensorShape;

    const out = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(out);
    const left_pad = kernel_size - 1;
    for (0..rows) |t| {
        for (0..hidden) |h| {
            var sum: f32 = 0.0;
            for (0..kernel_size) |k| {
                if (t + k < left_pad) continue;
                const src_t = t + k - left_pad;
                if (src_t >= rows) continue;
                const w = if (kernel_first) weight.data[k * hidden + h] else weight.data[h * kernel_size + k];
                sum += input_data[src_t * hidden + h] * w;
            }
            out[t * hidden + h] = sum;
        }
    }
    const shape = [_]i32{ @intCast(rows), @intCast(hidden) };
    return cb.fromFloat32Shape(out, &shape);
}

fn audioLocalAttentionCpu(
    allocator: std.mem.Allocator,
    cfg: AudioConfig,
    q_data: []const f32,
    k_data: []const f32,
    v_data: []const f32,
    rel_data: []const f32,
    per_dim_scale: []const f32,
    valid_mask: []const bool,
) ![]f32 {
    const rows = valid_mask.len;
    const hidden = cfg.audio_hidden;
    const head_dim = cfg.headDim();
    const heads = cfg.head_count;
    const context = cfg.attentionContextSize();
    const past = cfg.attention_context_left - 1;
    if (q_data.len != rows * hidden or k_data.len != rows * hidden or v_data.len != rows * hidden) return error.InvalidTensorShape;
    if (rel_data.len != cfg.attention_context_left * hidden or per_dim_scale.len != head_dim) return error.InvalidTensorShape;

    const out = try allocator.alloc(f32, rows * hidden);
    @memset(out, 0.0);
    errdefer allocator.free(out);
    const scores = try allocator.alloc(f32, context);
    defer allocator.free(scores);
    const q_dim_scales = try allocator.alloc(f32, head_dim);
    defer allocator.free(q_dim_scales);

    const q_scale = @as(f32, @floatFromInt(1)) / @sqrt(@as(f32, @floatFromInt(head_dim))) / @log(@as(f32, 2.0));
    const k_scale = @log(@as(f32, 1.0) + std.math.e) / @log(@as(f32, 2.0));
    for (q_dim_scales, 0..) |*scale, i| scale.* = q_scale * softplus(per_dim_scale[i]);

    for (0..rows) |q_idx| {
        if (!valid_mask[q_idx]) continue;
        const block_start = (q_idx / cfg.attention_chunk_size) * cfg.attention_chunk_size;
        const q_off = q_idx - block_start;
        for (0..heads) |head| {
            const head_base = head * head_dim;
            var max_score = -std.math.inf(f32);
            var valid_score_count: usize = 0;
            for (0..context) |c| {
                const rel_idx_signed: isize = @as(isize, @intCast(c)) - @as(isize, @intCast(q_off));
                const k_idx_signed: isize = @as(isize, @intCast(block_start + c)) - @as(isize, @intCast(past));
                if (rel_idx_signed < 0 or rel_idx_signed >= @as(isize, @intCast(cfg.attention_context_left)) or k_idx_signed < 0 or k_idx_signed >= @as(isize, @intCast(rows))) {
                    scores[c] = cfg.attention_invalid_logits_value;
                    continue;
                }
                const k_idx: usize = @intCast(k_idx_signed);
                if (!valid_mask[k_idx] or k_idx > q_idx or q_idx - k_idx > past) {
                    scores[c] = cfg.attention_invalid_logits_value;
                    continue;
                }
                const rel_idx: usize = @intCast(rel_idx_signed);
                var score: f32 = 0.0;
                const q_base = q_idx * hidden + head_base;
                const k_base = k_idx * hidden + head_base;
                const rel_base = rel_idx * hidden + head_base;
                for (0..head_dim) |d| {
                    const q = q_data[q_base + d] * q_dim_scales[d];
                    score += q * (k_data[k_base + d] * k_scale);
                    score += q * rel_data[rel_base + d];
                }
                score = std.math.tanh(score / cfg.attention_logit_cap) * cfg.attention_logit_cap;
                scores[c] = score;
                max_score = @max(max_score, score);
                valid_score_count += 1;
            }
            if (valid_score_count == 0) continue;

            var sum_exp: f32 = 0.0;
            for (scores) |*score| {
                score.* = @exp(score.* - max_score);
                sum_exp += score.*;
            }
            if (sum_exp == 0.0) continue;

            const out_base = q_idx * hidden + head_base;
            for (0..context) |c| {
                const prob = scores[c] / sum_exp;
                if (prob == 0.0) continue;
                const k_idx_signed: isize = @as(isize, @intCast(block_start + c)) - @as(isize, @intCast(past));
                if (k_idx_signed < 0 or k_idx_signed >= @as(isize, @intCast(rows))) continue;
                const k_idx: usize = @intCast(k_idx_signed);
                if (!valid_mask[k_idx] or k_idx > q_idx or q_idx - k_idx > past) continue;
                const v_base = k_idx * hidden + head_base;
                for (0..head_dim) |d| out[out_base + d] += prob * v_data[v_base + d];
            }
        }
    }

    return out;
}

fn sigmoid(x: f32) f32 {
    if (x >= 0.0) {
        const z = @exp(-x);
        return 1.0 / (1.0 + z);
    }
    const z = @exp(x);
    return z / (1.0 + z);
}

fn softplus(x: f32) f32 {
    if (x > 20.0) return x;
    if (x < -20.0) return @exp(x);
    return @log(1.0 + @exp(x));
}

fn parseAudioConfig(file: *const gguf_format.File) !AudioConfig {
    const view = gguf_metadata.View.init(file);
    const arch = view.getString("general.architecture") orelse return error.InvalidGgufProjector;
    if (!std.mem.eql(u8, arch, "clip")) return error.InvalidGgufProjector;
    const projector_type = view.getString("clip.audio.projector_type") orelse return error.AudioProjectorNotFound;
    if (!std.mem.eql(u8, projector_type, "gemma4a")) return error.UnsupportedGgufProjector;

    return .{
        .text_hidden = @intCast(view.getU64("clip.audio.projection_dim") orelse return error.InvalidGgufProjector),
        .audio_hidden = @intCast(view.getU64("clip.audio.embedding_length") orelse return error.InvalidGgufProjector),
        .output_hidden = @intCast(view.getU64("clip.audio.projection_dim") orelse return error.InvalidGgufProjector),
        .intermediate_size = @intCast(view.getU64("clip.audio.feed_forward_length") orelse return error.InvalidGgufProjector),
        .block_count = @intCast(view.getU64("clip.audio.block_count") orelse return error.InvalidGgufProjector),
        .head_count = @intCast(view.getU64("clip.audio.attention.head_count") orelse return error.InvalidGgufProjector),
        .mel_bins = @intCast(view.getU64("clip.audio.num_mel_bins") orelse 128),
        .layer_norm_eps = view.getF32("clip.audio.attention.layer_norm_epsilon") orelse 1e-5,
    };
}

fn metadataF32Triple(view: gguf_metadata.View, key: []const u8) ?[3]f32 {
    const entry = view.find(key) orelse return null;
    if (entry.value != .array or entry.value.array.values.len != 3) return null;
    var out: [3]f32 = undefined;
    for (entry.value.array.values, 0..) |value, i| {
        out[i] = switch (value) {
            .f32 => |v| v,
            .f64 => |v| @floatCast(v),
            .u8 => |v| @floatFromInt(v),
            .u16 => |v| @floatFromInt(v),
            .u32 => |v| @floatFromInt(v),
            .u64 => |v| @floatFromInt(v),
            .i8 => |v| @floatFromInt(v),
            .i16 => |v| @floatFromInt(v),
            .i32 => |v| @floatFromInt(v),
            .i64 => |v| @floatFromInt(v),
            else => return null,
        };
    }
    return out;
}

fn targetGeometry(cfg: Config, width_u32: u32, height_u32: u32) Geometry {
    const width = @max(@as(usize, @intCast(width_u32)), 1);
    const height = @max(@as(usize, @intCast(height_u32)), 1);
    const block = cfg.patch_size * cfg.spatial_merge_size;
    const max_patches = cfg.maxPatchCount();
    const target_pixels: f64 = @floatFromInt(max_patches * cfg.patch_size * cfg.patch_size);
    const src_pixels: f64 = @floatFromInt(width * height);
    const factor = @sqrt(target_pixels / src_pixels);
    const ideal_w = factor * @as(f64, @floatFromInt(width));
    const ideal_h = factor * @as(f64, @floatFromInt(height));

    var target_w = floorToMultiple(@intFromFloat(@floor(ideal_w)), block);
    var target_h = floorToMultiple(@intFromFloat(@floor(ideal_h)), block);
    const max_side = (max_patches / (cfg.spatial_merge_size * cfg.spatial_merge_size)) * block;
    if (target_w == 0 and target_h == 0) {
        target_w = block;
        target_h = block;
    } else if (target_w == 0) {
        target_w = block;
        target_h = @min(floorToMultiple(height / width, 1) * block, max_side);
    } else if (target_h == 0) {
        target_h = block;
        target_w = @min(floorToMultiple(width / height, 1) * block, max_side);
    }
    target_w = @max(target_w, block);
    target_h = @max(target_h, block);
    while ((target_w / cfg.patch_size) * (target_h / cfg.patch_size) > max_patches) {
        if (target_w >= target_h and target_w > block) {
            target_w -= block;
        } else if (target_h > block) {
            target_h -= block;
        } else {
            break;
        }
    }
    const grid_x = target_w / cfg.patch_size;
    const grid_y = target_h / cfg.patch_size;
    return .{
        .width = target_w,
        .height = target_h,
        .grid_x = grid_x,
        .grid_y = grid_y,
        .pooled_x = grid_x / cfg.spatial_merge_size,
        .pooled_y = grid_y / cfg.spatial_merge_size,
    };
}

fn floorToMultiple(value: usize, multiple: usize) usize {
    if (multiple == 0) return value;
    return (value / multiple) * multiple;
}

fn patchEmbed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    cfg: Config,
    pixel_values: []const f32,
    geometry: Geometry,
) ![]f32 {
    const scaled_pixels = try allocator.dupe(f32, pixel_values);
    defer allocator.free(scaled_pixels);
    for (scaled_pixels) |*value| value.* = 2.0 * (value.* - 0.5);

    const patch_w = try loadWeightCt(cb, allocator, store, "v.patch_embd.weight");
    defer cb.free(patch_w);
    const zero_bias = try allocator.alloc(f32, cfg.vision_hidden);
    defer allocator.free(zero_bias);
    @memset(zero_bias, 0.0);
    const bias_shape = [_]i32{@intCast(cfg.vision_hidden)};
    const bias_ct = try cb.fromFloat32Shape(zero_bias, &bias_shape);
    defer cb.free(bias_ct);

    const pixel_shape = [_]i32{ 1, 3, @intCast(geometry.height), @intCast(geometry.width) };
    const pixels_ct = try cb.fromFloat32Shape(scaled_pixels, &pixel_shape);
    defer cb.free(pixels_ct);

    const conv = try cb.conv2d(
        pixels_ct,
        patch_w,
        bias_ct,
        1,
        3,
        cfg.vision_hidden,
        geometry.height,
        geometry.width,
        cfg.patch_size,
        cfg.patch_size,
        cfg.patch_size,
        cfg.patch_size,
        0,
        0,
        1,
    );
    defer cb.free(conv);
    const conv_data = try cb.toFloat32(conv, allocator);
    defer allocator.free(conv_data);

    const patch_count = geometry.grid_x * geometry.grid_y;
    if (conv_data.len != cfg.vision_hidden * patch_count) return error.InvalidPatchEmbeddingShape;
    const embedded = try allocator.alloc(f32, patch_count * cfg.vision_hidden);
    for (0..cfg.vision_hidden) |channel| {
        const src_base = channel * patch_count;
        for (0..patch_count) |patch_idx| {
            embedded[patch_idx * cfg.vision_hidden + channel] = conv_data[src_base + patch_idx];
        }
    }
    return embedded;
}

fn addPositionEmbeddings(
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    cfg: Config,
    patch_embeddings: []const f32,
    geometry: Geometry,
) ![]f32 {
    var pos = try loadTensorF32(store, "v.position_embd.weight");
    defer pos.deinit();
    if (pos.shape.len != 3 or pos.shape[0] != 2 or pos.shape[2] != @as(i64, @intCast(cfg.vision_hidden))) {
        return error.InvalidPositionEmbeddingShape;
    }
    const positions_per_axis: usize = @intCast(pos.shape[1]);
    if (geometry.grid_x > positions_per_axis or geometry.grid_y > positions_per_axis) {
        return error.InvalidPositionEmbeddingShape;
    }

    const patch_count = geometry.grid_x * geometry.grid_y;
    const out = try allocator.alloc(f32, patch_count * cfg.vision_hidden);
    for (0..geometry.grid_y) |y| {
        for (0..geometry.grid_x) |x| {
            const patch_idx = y * geometry.grid_x + x;
            const dst = patch_idx * cfg.vision_hidden;
            const y_pos = (0 * positions_per_axis + y) * cfg.vision_hidden;
            const x_pos = (1 * positions_per_axis + x) * cfg.vision_hidden;
            for (0..cfg.vision_hidden) |h| {
                out[dst + h] = patch_embeddings[dst + h] + pos.data[y_pos + h] + pos.data[x_pos + h];
            }
        }
    }
    return out;
}

fn encoderBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    cfg: Config,
    input: CT,
    geometry: Geometry,
    layer: usize,
) !CT {
    const total = geometry.grid_x * geometry.grid_y;
    var buf: [128]u8 = undefined;

    const ln1 = try loadWeightCt(cb, allocator, store, try fmt(&buf, "v.blk.{d}.ln1.weight", .{layer}));
    defer cb.free(ln1);
    const normed1 = try cb.rmsNorm(input, ln1, cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(normed1);

    const attn = try selfAttention(cb, allocator, store, cfg, normed1, geometry, layer, &buf);
    defer cb.free(attn);
    const attn_post = try loadWeightCt(cb, allocator, store, try fmt(&buf, "v.blk.{d}.attn_post_norm.weight", .{layer}));
    defer cb.free(attn_post);
    const attn_normed = try cb.rmsNorm(attn, attn_post, cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(attn_normed);
    const res1 = try cb.add(input, attn_normed);
    errdefer cb.free(res1);

    const ln2 = try loadWeightCt(cb, allocator, store, try fmt(&buf, "v.blk.{d}.ln2.weight", .{layer}));
    defer cb.free(ln2);
    const normed2 = try cb.rmsNorm(res1, ln2, cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(normed2);

    const ffn = try feedForward(cb, allocator, store, cfg, normed2, total, layer, &buf);
    defer cb.free(ffn);
    const ffn_post = try loadWeightCt(cb, allocator, store, try fmt(&buf, "v.blk.{d}.ffn_post_norm.weight", .{layer}));
    defer cb.free(ffn_post);
    const ffn_normed = try cb.rmsNorm(ffn, ffn_post, cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(ffn_normed);

    const res2 = try cb.add(res1, ffn_normed);
    cb.free(res1);
    return res2;
}

fn selfAttention(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    cfg: Config,
    input: CT,
    geometry: Geometry,
    layer: usize,
    buf: *[128]u8,
) !CT {
    const total = geometry.grid_x * geometry.grid_y;
    const head_dim = cfg.vision_hidden / cfg.head_count;

    var q = try linearNoBiasMaybeClipped(cb, allocator, store, input, try fmt(buf, "v.blk.{d}.attn_q", .{layer}), total, cfg.vision_hidden, cfg.vision_hidden);
    errdefer cb.free(q);
    {
        const q_norm_w = try loadWeightCt(cb, allocator, store, try fmt(buf, "v.blk.{d}.attn_q_norm.weight", .{layer}));
        defer cb.free(q_norm_w);
        const normed = try rmsNormHeadChunksAnd2dRope(cb, allocator, q, q_norm_w, total, cfg.vision_hidden, head_dim, geometry, cfg.layer_norm_eps, cfg.rope_theta, true);
        cb.free(q);
        q = normed;
    }
    defer cb.free(q);

    var k = try linearNoBiasMaybeClipped(cb, allocator, store, input, try fmt(buf, "v.blk.{d}.attn_k", .{layer}), total, cfg.vision_hidden, cfg.vision_hidden);
    errdefer cb.free(k);
    {
        const k_norm_w = try loadWeightCt(cb, allocator, store, try fmt(buf, "v.blk.{d}.attn_k_norm.weight", .{layer}));
        defer cb.free(k_norm_w);
        const normed = try rmsNormHeadChunksAnd2dRope(cb, allocator, k, k_norm_w, total, cfg.vision_hidden, head_dim, geometry, cfg.layer_norm_eps, cfg.rope_theta, false);
        cb.free(k);
        k = normed;
    }
    defer cb.free(k);

    var v = try linearNoBiasMaybeClipped(cb, allocator, store, input, try fmt(buf, "v.blk.{d}.attn_v", .{layer}), total, cfg.vision_hidden, cfg.vision_hidden);
    errdefer cb.free(v);
    {
        const normed = try rmsNormHeadChunksNoScale(cb, allocator, v, total, cfg.vision_hidden, head_dim, cfg.layer_norm_eps);
        cb.free(v);
        v = normed;
    }
    defer cb.free(v);

    const mask = try allocator.alloc(i64, total);
    defer allocator.free(mask);
    @memset(mask, 1);
    const attn = try cb.scaledDotProductAttention(q, k, v, mask, null, 1, total, cfg.head_count, head_dim);
    defer cb.free(attn);

    return linearNoBiasMaybeClipped(cb, allocator, store, attn, try fmt(buf, "v.blk.{d}.attn_out", .{layer}), total, cfg.vision_hidden, cfg.vision_hidden);
}

fn rmsNormHeadChunksAnd2dRope(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    weight: CT,
    rows: usize,
    hidden: usize,
    head_dim: usize,
    geometry: Geometry,
    eps: f32,
    rope_theta: f32,
    compensate_sdpa_scale: bool,
) !CT {
    const input_data = try cb.toFloat32(input, allocator);
    defer allocator.free(input_data);
    const weight_data = try cb.toFloat32(weight, allocator);
    defer allocator.free(weight_data);
    if (input_data.len != rows * hidden or weight_data.len != head_dim) return error.InvalidTensorShape;
    activations.rmsNorm(input_data, weight_data, head_dim, eps);
    apply2dRope(input_data, rows, hidden, head_dim, geometry, rope_theta, compensate_sdpa_scale);
    const shape = [_]i32{ @intCast(rows), @intCast(hidden) };
    return cb.fromFloat32Shape(input_data, &shape);
}

fn rmsNormHeadChunksNoScale(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    rows: usize,
    hidden: usize,
    head_dim: usize,
    eps: f32,
) !CT {
    const input_data = try cb.toFloat32(input, allocator);
    defer allocator.free(input_data);
    if (input_data.len != rows * hidden) return error.InvalidTensorShape;
    const ones = try allocator.alloc(f32, head_dim);
    defer allocator.free(ones);
    @memset(ones, 1.0);
    activations.rmsNorm(input_data, ones, head_dim, eps);
    const shape = [_]i32{ @intCast(rows), @intCast(hidden) };
    return cb.fromFloat32Shape(input_data, &shape);
}

fn apply2dRope(
    data: []f32,
    rows: usize,
    hidden: usize,
    head_dim: usize,
    geometry: Geometry,
    rope_theta: f32,
    compensate_sdpa_scale: bool,
) void {
    const ndim: usize = 2;
    const channels_per_dim = 2 * (head_dim / (2 * ndim));
    if (channels_per_dim == 0) return;
    const half = channels_per_dim / 2;
    const spatial_dim = head_dim / 2;
    const heads = hidden / head_dim;
    const q_scale: f32 = if (compensate_sdpa_scale) @sqrt(@as(f32, @floatFromInt(head_dim))) else 1.0;

    for (0..rows) |token| {
        const x: f32 = @floatFromInt(token % geometry.grid_x);
        const y: f32 = @floatFromInt(token / geometry.grid_x);
        const positions = [2]f32{ x, y };
        for (0..heads) |head| {
            const base = token * hidden + head * head_dim;
            for (0..ndim) |axis| {
                const part = axis * channels_per_dim;
                var j: usize = 0;
                while (j < half) : (j += 1) {
                    const exponent = @as(f32, @floatFromInt(j * 2)) / @as(f32, @floatFromInt(spatial_dim));
                    const inv_freq = 1.0 / std.math.pow(f32, rope_theta, exponent);
                    const angle = positions[axis] * inv_freq;
                    const cos_v = @cos(angle);
                    const sin_v = @sin(angle);
                    const a_idx = base + part + j;
                    const b_idx = base + part + half + j;
                    const a = data[a_idx];
                    const b = data[b_idx];
                    data[a_idx] = (a * cos_v - b * sin_v) * q_scale;
                    data[b_idx] = (b * cos_v + a * sin_v) * q_scale;
                }
            }
        }
    }
}

fn feedForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    cfg: Config,
    input: CT,
    total: usize,
    layer: usize,
    buf: *[128]u8,
) !CT {
    const gate = try linearNoBiasMaybeClipped(cb, allocator, store, input, try fmt(buf, "v.blk.{d}.ffn_gate", .{layer}), total, cfg.vision_hidden, cfg.intermediate_size);
    defer cb.free(gate);

    const up = try linearNoBiasMaybeClipped(cb, allocator, store, input, try fmt(buf, "v.blk.{d}.ffn_up", .{layer}), total, cfg.vision_hidden, cfg.intermediate_size);
    defer cb.free(up);

    const activated = try cb.gelu(gate);
    defer cb.free(activated);
    const gated = try cb.multiply(activated, up);
    defer cb.free(gated);

    return linearNoBiasMaybeClipped(cb, allocator, store, gated, try fmt(buf, "v.blk.{d}.ffn_down", .{layer}), total, cfg.intermediate_size, cfg.vision_hidden);
}

fn averagePoolSpatial(
    allocator: std.mem.Allocator,
    hidden: []const f32,
    cfg: Config,
    geometry: Geometry,
) ![]f32 {
    if (hidden.len != geometry.grid_x * geometry.grid_y * cfg.vision_hidden) return error.InvalidPatchEmbeddingShape;
    const merge = cfg.spatial_merge_size;
    const pooled = try allocator.alloc(f32, geometry.tokenCount() * cfg.vision_hidden);
    for (0..geometry.pooled_y) |py| {
        for (0..geometry.pooled_x) |px| {
            const dst_token = py * geometry.pooled_x + px;
            const dst = dst_token * cfg.vision_hidden;
            @memset(pooled[dst..][0..cfg.vision_hidden], 0.0);
            for (0..merge) |dy| {
                for (0..merge) |dx| {
                    const src_token = (py * merge + dy) * geometry.grid_x + (px * merge + dx);
                    const src = src_token * cfg.vision_hidden;
                    for (0..cfg.vision_hidden) |h| pooled[dst + h] += hidden[src + h];
                }
            }
            const denom: f32 = @floatFromInt(merge * merge);
            const scale = @sqrt(@as(f32, @floatFromInt(cfg.vision_hidden)));
            for (0..cfg.vision_hidden) |h| pooled[dst + h] = (pooled[dst + h] / denom) * scale;
        }
    }
    return pooled;
}

fn rmsNormNoScaleCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    rows: usize,
    dim: usize,
    eps: f32,
) !CT {
    _ = rows;
    const ones = try allocator.alloc(f32, dim);
    defer allocator.free(ones);
    @memset(ones, 1.0);
    const shape = [_]i32{@intCast(dim)};
    const weight = try cb.fromFloat32Shape(ones, &shape);
    defer cb.free(weight);
    return cb.rmsNorm(input, weight, dim, eps);
}

fn applyOptionalStandardization(
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    pooled: []f32,
    cfg: Config,
) !void {
    var scale = loadTensorF32(store, "v.std_scale") catch |err| switch (err) {
        error.TensorNotFound => null,
        else => return err,
    };
    var bias = loadTensorF32(store, "v.std_bias") catch |err| switch (err) {
        error.TensorNotFound => null,
        else => return err,
    };
    _ = allocator;
    defer if (scale) |*s| s.deinit();
    defer if (bias) |*b| b.deinit();
    if (scale == null and bias == null) return;
    if (scale == null or bias == null) return error.InvalidStandardizationTensorShape;
    if (scale.?.data.len != cfg.vision_hidden or bias.?.data.len != cfg.vision_hidden) {
        return error.InvalidStandardizationTensorShape;
    }
    const rows = pooled.len / cfg.vision_hidden;
    for (0..rows) |row| {
        const base = row * cfg.vision_hidden;
        for (0..cfg.vision_hidden) |h| {
            pooled[base + h] = (pooled[base + h] - bias.?.data[h]) * scale.?.data[h];
        }
    }
}

fn loadWeightCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    name: []const u8,
) !CT {
    var tensor = try loadTensorF32(store, name);
    defer tensor.deinit();
    const shape = try shapeI32(allocator, tensor.shape);
    defer allocator.free(shape);
    return cb.fromFloat32Shape(tensor.data, shape);
}

fn linearNoBiasMaybeClipped(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    input: CT,
    prefix: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !CT {
    const weight_name = try std.fmt.allocPrint(allocator, "{s}.weight", .{prefix});
    defer allocator.free(weight_name);
    const input_min_name = try std.fmt.allocPrint(allocator, "{s}.input_min", .{prefix});
    defer allocator.free(input_min_name);
    const input_max_name = try std.fmt.allocPrint(allocator, "{s}.input_max", .{prefix});
    defer allocator.free(input_max_name);
    const output_min_name = try std.fmt.allocPrint(allocator, "{s}.output_min", .{prefix});
    defer allocator.free(output_min_name);
    const output_max_name = try std.fmt.allocPrint(allocator, "{s}.output_max", .{prefix});
    defer allocator.free(output_max_name);

    var input_min = try loadOptionalTensorF32(store, input_min_name);
    defer if (input_min) |*tensor| tensor.deinit();
    var input_max = try loadOptionalTensorF32(store, input_max_name);
    defer if (input_max) |*tensor| tensor.deinit();
    var output_min = try loadOptionalTensorF32(store, output_min_name);
    defer if (output_min) |*tensor| tensor.deinit();
    var output_max = try loadOptionalTensorF32(store, output_max_name);
    defer if (output_max) |*tensor| tensor.deinit();

    var linear_input = input;
    var free_linear_input = false;
    if (input_min != null or input_max != null) {
        const input_data = try cb.toFloat32(input, allocator);
        defer allocator.free(input_data);
        try applyClamp(input_data, rows, in_dim, if (input_min) |*t| t.data else null, if (input_max) |*t| t.data else null);
        const input_shape = [_]i32{ @intCast(rows), @intCast(in_dim) };
        linear_input = try cb.fromFloat32Shape(input_data, &input_shape);
        free_linear_input = true;
    }
    defer if (free_linear_input) cb.free(linear_input);

    const weight = try loadLinearWeightCt(cb, allocator, store, weight_name, in_dim, out_dim);
    defer cb.free(weight);
    var output = try cb.linearNoBias(linear_input, weight, rows, in_dim, out_dim);
    errdefer cb.free(output);

    if (output_min != null or output_max != null) {
        const output_data = try cb.toFloat32(output, allocator);
        defer allocator.free(output_data);
        try applyClamp(output_data, rows, out_dim, if (output_min) |*t| t.data else null, if (output_max) |*t| t.data else null);
        const output_shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
        const clipped_output = try cb.fromFloat32Shape(output_data, &output_shape);
        cb.free(output);
        output = clipped_output;
    }

    return output;
}

fn applyClamp(data: []f32, rows: usize, dim: usize, maybe_min: ?[]const f32, maybe_max: ?[]const f32) !void {
    if (data.len != rows * dim) return error.InvalidTensorShape;
    if (maybe_min) |min_data| try validateClampLen(min_data.len, rows, dim);
    if (maybe_max) |max_data| try validateClampLen(max_data.len, rows, dim);
    for (0..rows) |row| {
        for (0..dim) |col| {
            const idx = row * dim + col;
            if (maybe_min) |min_data| data[idx] = @max(data[idx], clampValue(min_data, row, col, dim));
            if (maybe_max) |max_data| data[idx] = @min(data[idx], clampValue(max_data, row, col, dim));
        }
    }
}

fn validateClampLen(len: usize, rows: usize, dim: usize) !void {
    if (len == 1 or len == dim or len == rows * dim) return;
    return error.InvalidTensorShape;
}

fn clampValue(values: []const f32, row: usize, col: usize, dim: usize) f32 {
    if (values.len == 1) return values[0];
    if (values.len == dim) return values[col];
    return values[row * dim + col];
}

fn loadLinearWeightCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    name: []const u8,
    in_dim: usize,
    out_dim: usize,
) !CT {
    var tensor = try loadTensorF32(store, name);
    defer tensor.deinit();
    if (tensor.shape.len != 2) return error.InvalidTensorShape;
    const rows: usize = @intCast(tensor.shape[0]);
    const cols: usize = @intCast(tensor.shape[1]);
    if (rows == out_dim and cols == in_dim) {
        const shape = [_]i32{ @intCast(out_dim), @intCast(in_dim) };
        return cb.fromFloat32Shape(tensor.data, &shape);
    }
    if (rows == in_dim and cols == out_dim) {
        const transposed = try transposeMatrix(allocator, tensor.data, in_dim, out_dim);
        defer allocator.free(transposed);
        const shape = [_]i32{ @intCast(out_dim), @intCast(in_dim) };
        return cb.fromFloat32Shape(transposed, &shape);
    }
    return error.InvalidTensorShape;
}

fn loadOptionalTensorF32(store: *tensor_store_mod.GgufStore, name: []const u8) !?LoadedF32 {
    return loadTensorF32(store, name) catch |err| switch (err) {
        error.TensorNotFound => null,
        else => return err,
    };
}

fn loadTensorF32(store: *tensor_store_mod.GgufStore, name: []const u8) !LoadedF32 {
    var tensor_ref = try store.tensorStore().describeTensor(store.allocator, name);
    defer tensor_ref.deinit(store.allocator);
    var loaded = try store.tensorStore().loadTensorRef(&tensor_ref);
    errdefer loaded.deinit();

    if (loaded.tensor.dtype == .f32) {
        return .{
            .weight = loaded,
            .data = loaded.tensor.asFloat32(),
            .shape = loaded.tensor.shape,
        };
    }

    if (loaded.tensor.dtype == .f16 or loaded.tensor.dtype == .bf16) {
        const converted = try weight_source_mod.convertToF32(store.allocator, &loaded.tensor);
        errdefer converted.deinit();
        return .{
            .weight = loaded,
            .converted = converted,
            .data = converted.asFloat32(),
            .shape = converted.shape,
        };
    }

    return error.UnsupportedTensorType;
}

fn shapeI32(allocator: std.mem.Allocator, shape: []const i64) ![]i32 {
    const out = try allocator.alloc(i32, shape.len);
    for (shape, 0..) |dim, i| out[i] = @intCast(dim);
    return out;
}

fn transposeMatrix(allocator: std.mem.Allocator, input: []const f32, rows: usize, cols: usize) ![]f32 {
    if (input.len != rows * cols) return error.InvalidTensorShape;
    const transposed = try allocator.alloc(f32, input.len);
    for (0..rows) |row| {
        for (0..cols) |col| {
            transposed[col * rows + row] = input[row * cols + col];
        }
    }
    return transposed;
}

fn fmt(buf: *[128]u8, comptime format: []const u8, args: anytype) ![]const u8 {
    return std.fmt.bufPrint(buf, format, args) catch return error.WeightNameTooLong;
}
